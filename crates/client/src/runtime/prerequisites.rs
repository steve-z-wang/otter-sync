//! The prerequisite scheduler: Rust decides which task runs and when, and
//! records what it came to; the host only runs the application's handler as
//! an effect ([#134](https://github.com/zanminwang/axton/issues/134),
//! [#185](https://github.com/zanminwang/axton/issues/185)).
//!
//! The handler names are fixed at open. The scheduler turns dirty when a
//! task may have become runnable - at open, after every commit, after a
//! handler's outcome, a readiness change, a backoff timer or a rebuild - and a
//! dirty scheduler with no handler running takes one lane turn: it reads the
//! pending tasks and issues one `prerequisite` effect for the first one, in
//! key order, that has a handler and is not backing off. One handler runs at
//! a time, and no transaction is held while it runs.
//!
//! A success resolves the task and a plain failure fails it with its reason,
//! each in its own unit. A failure the handler declared transient
//! (`error.retry`) writes nothing: the task stays pending and waits
//! [`crate::load_backoff`] of its consecutive transient failures, counted in
//! memory, so a reopen runs it at once. One `timer` effect waits for the
//! earliest retry. A readiness change forgets a task's backoff.
use super::effects::{EffectKind, Ready};
use super::*;
use crate::ClientStore;

/// The scheduler's state for one runtime.
#[derive(Default)]
pub(super) struct Prerequisites {
    /// The prerequisite names the application registered handlers for.
    handlers: BTreeSet<String>,
    /// A task may have become runnable since the last turn.
    dirty: bool,
    /// The handler run in flight: its effect and its task's key.
    running: Option<(String, String)>,
    /// Tasks backing off after a transient failure, by key.
    backoff: BTreeMap<String, Backoff>,
    /// The timer for the earliest retry, and the time it is due.
    timer: Option<(String, u64)>,
}

struct Backoff {
    /// Consecutive transient failures.
    attempts: u64,
    /// When the task may run again.
    due: u64,
}

impl Prerequisites {
    /// Whether a lane turn has something to decide.
    pub(super) fn ready(&self) -> bool {
        self.dirty && self.running.is_none() && !self.handlers.is_empty()
    }
    /// Something committed or changed: look for a runnable task.
    pub(super) fn wake(&mut self) {
        self.dirty = true;
    }
}

impl<S: ClientStore + 'static> ClientRuntime<S> {
    /// Register the prerequisite names the application has handlers for.
    /// Each must be declared by the schema this client asked for; the names
    /// are fixed for the runtime's lifetime, and pending tasks run from the
    /// first step.
    pub fn register_prerequisite_handlers(mut self, names: Vec<String>) -> Result<Self> {
        if self.admitted != 0 || self.lifecycle != Lifecycle::Open || self.transaction.is_some() {
            return Err(crate::invalid(
                "prerequisite handlers are fixed at runtime open",
            ));
        }
        let declared = self.client.target_prerequisites();
        let mut handlers = BTreeSet::new();
        for name in names {
            if !declared.contains(&name) || !handlers.insert(name.clone()) {
                return Err(crate::invalid(format!(
                    "invalid prerequisite handler {name}"
                )));
            }
        }
        self.prerequisites = Prerequisites {
            dirty: true,
            handlers,
            ..Prerequisites::default()
        };
        Ok(self)
    }
    /// One turn: run the first runnable task, and keep the timer for the
    /// earliest one backing off.
    pub(super) fn prerequisite_turn(&mut self, now: u64) {
        self.prerequisites.dirty = false;
        let tasks = match self.client.pending_tasks() {
            Ok(tasks) => tasks,
            Err(e) => return self.error(format!("prerequisite tasks: {e}")),
        };
        let state = &mut self.prerequisites;
        let pending = |task: &&Value| task["state"] == "pending";
        // A task that is no longer pending forgets its backoff.
        state.backoff.retain(|key, _| {
            tasks
                .iter()
                .filter(pending)
                .any(|task| task["key"].as_str() == Some(key))
        });
        let next = tasks.iter().filter(pending).find(|task| {
            let handled = task["name"]
                .as_str()
                .is_some_and(|name| state.handlers.contains(name));
            let waiting = task["key"]
                .as_str()
                .and_then(|key| state.backoff.get(key))
                .is_some_and(|backoff| backoff.due > now);
            handled && !waiting
        });
        if let Some(task) = next {
            let key = task["key"].as_str().unwrap_or_default().to_string();
            let operation = Operation::Prerequisite {
                key: key.clone(),
                name: task["name"].as_str().unwrap_or_default().to_string(),
                arguments: task.get("arguments").cloned().unwrap_or(Value::Null),
            };
            if let Some(effect) =
                self.issue_effect(EffectKind::Prerequisite { key: key.clone() }, operation)
            {
                self.prerequisites.running = Some((effect, key));
            }
        }
        self.arm_prerequisite_timer(now);
    }
    /// The handler settled. A success or a terminal failure is recorded by
    /// the next unit; a transient one backs the task off in memory.
    pub(super) fn prerequisite_result(
        &mut self,
        key: String,
        outcome: EffectOutcome,
        now: u64,
        entropy: u64,
    ) {
        self.prerequisites.running = None;
        self.prerequisites.dirty = true;
        match (outcome.ok, outcome.error) {
            (true, _) => self
                .ready
                .push_back(Ready::PrerequisiteOutcome { key, error: None }),
            (false, Some(error)) if error.retry => {
                let backoff = self.prerequisites.backoff.entry(key).or_insert(Backoff {
                    attempts: 0,
                    due: now,
                });
                backoff.attempts = backoff.attempts.saturating_add(1);
                backoff.due = now.saturating_add(crate::load_backoff(backoff.attempts, entropy));
            }
            (false, error) => self.ready.push_back(Ready::PrerequisiteOutcome {
                key,
                error: Some(error.map_or_else(|| "prerequisite failed".into(), |e| e.message)),
            }),
        }
    }
    /// Record what the handler came to: `None` resolves the task, a reason
    /// fails it and is kept.
    pub(super) fn prerequisite_outcome(&mut self, key: String, error: Option<String>) {
        self.prerequisites.backoff.remove(&key);
        self.prerequisites.dirty = true;
        if let Err(e) = self.client.outcome(&key, error.as_deref()) {
            self.error(format!("prerequisite outcome: {e}"));
        }
    }
    /// The application changed a task's readiness: its backoff is over.
    pub(super) fn prerequisite_readiness(&mut self, key: &str) {
        self.prerequisites.backoff.remove(key);
        self.prerequisites.dirty = true;
    }
    /// The backoff timer fired: tasks whose delay passed may run.
    pub(super) fn prerequisite_timer_fired(&mut self, effect_id: &str) {
        if self
            .prerequisites
            .timer
            .as_ref()
            .is_some_and(|(timer, _)| timer == effect_id)
        {
            self.prerequisites.timer = None;
            self.prerequisites.dirty = true;
        }
    }
    /// Keep one timer, for the earliest task that still waits.
    fn arm_prerequisite_timer(&mut self, now: u64) {
        let due = self
            .prerequisites
            .backoff
            .values()
            .map(|backoff| backoff.due)
            .filter(|due| *due > now)
            .min();
        if self.prerequisites.timer.as_ref().map(|(_, at)| *at) == due {
            return;
        }
        if let Some((timer, _)) = self.prerequisites.timer.take() {
            self.cancel_effect(&timer);
        }
        if let Some(due) = due
            && let Some(timer) = self.issue_effect(
                EffectKind::PrerequisiteTimer,
                Operation::Timer { millis: due - now },
            )
        {
            self.prerequisites.timer = Some((timer, due));
        }
    }
    /// A rebuild replaced the replica: the handler in flight and every
    /// backoff belonged to the old one. The new replica is scanned afresh.
    pub(super) fn rebuilt_prerequisites(&mut self) {
        self.ready
            .retain(|ready| !matches!(ready, Ready::PrerequisiteOutcome { .. }));
        let state = &mut self.prerequisites;
        let running = state.running.take().map(|(effect, _)| effect);
        let timer = state.timer.take().map(|(timer, _)| timer);
        state.backoff.clear();
        state.dirty = true;
        for effect in running.into_iter().chain(timer) {
            self.cancel_effect(&effect);
        }
    }
    /// Close cancelled every effect; nothing is left to run.
    pub(super) fn close_prerequisites(&mut self) {
        let handlers = std::mem::take(&mut self.prerequisites.handlers);
        self.prerequisites = Prerequisites {
            handlers,
            ..Prerequisites::default()
        };
    }
}
