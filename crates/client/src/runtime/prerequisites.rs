//! The prerequisite loop: Rust picks each task and records its outcome; the
//! host only runs the application's handler as an effect
//! ([#134](https://github.com/zanminwang/axton/issues/134)).
//!
//! `runPrerequisites {handlers}` asks [`Client::next_task`] for a task (one
//! local transaction), issues one `prerequisite` effect for it and parks; the
//! handler's result becomes a continuation that records it with
//! [`Client::outcome`] (another), which wakes the push lane; the next turn
//! asks again. The task completes with `null` when no task is left. No
//! transaction is held while a handler runs.
use super::effects::{EffectKind, Ready};
use super::*;
use crate::ClientStore;

pub(super) const ALREADY_RUNNING: &str = "prerequisites already running";

/// The one `runPrerequisites` task in progress.
pub(super) struct Loop {
    request_id: String,
    handlers: Vec<String>,
    /// The handler run in flight.
    effect: Option<String>,
}

impl<S: ClientStore + 'static> ClientRuntime<S> {
    /// `runPrerequisites {handlers}`; `None` while the loop runs.
    pub(super) fn run_prerequisites(
        &mut self,
        request_id: &str,
        handlers: Vec<String>,
    ) -> Option<std::result::Result<Value, String>> {
        if self.prerequisites.is_some() {
            return Some(Err(ALREADY_RUNNING.into()));
        }
        self.prerequisites = Some(Loop {
            request_id: request_id.to_string(),
            handlers,
            effect: None,
        });
        self.next_prerequisite();
        None
    }
    /// One turn: pick the next task and ask the host to run its handler, or
    /// finish the loop.
    pub(super) fn next_prerequisite(&mut self) {
        let Some(active) = &self.prerequisites else {
            return;
        };
        let request_id = active.request_id.clone();
        match self.client.next_task(&active.handlers) {
            Ok(Some(task)) => {
                let key = task["key"].as_str().unwrap_or_default().to_string();
                let name = task["name"].as_str().unwrap_or_default().to_string();
                let arguments = task.get("arguments").cloned().unwrap_or(Value::Null);
                let effect = self.issue_effect(
                    EffectKind::Prerequisite {
                        request_id: request_id.clone(),
                        key: key.clone(),
                    },
                    Operation::Prerequisite {
                        key,
                        name,
                        arguments,
                    },
                );
                match (effect, &mut self.prerequisites) {
                    (Some(effect), Some(active)) => active.effect = Some(effect),
                    _ => self.finish_prerequisites(Err("runtime identifiers exhausted".into())),
                }
            }
            Ok(None) => self.finish_prerequisites(Ok(Value::Null)),
            Err(e) => self.finish_prerequisites(Err(e.to_string())),
        }
    }
    /// The handler settled: its outcome is recorded by the next unit.
    pub(super) fn prerequisite_result(
        &mut self,
        request_id: String,
        key: String,
        outcome: EffectOutcome,
    ) {
        let Some(active) = &mut self.prerequisites else {
            return;
        };
        if active.request_id != request_id {
            return;
        }
        active.effect = None;
        let error = (!outcome.ok).then(|| {
            outcome
                .error
                .map(|e| e.message)
                .unwrap_or_else(|| "prerequisite failed".into())
        });
        self.ready
            .push_back(Ready::PrerequisiteOutcome { key, error });
    }
    /// Record what the handler came to - a failure keeps its reason - and
    /// take the next turn.
    pub(super) fn prerequisite_outcome(&mut self, key: String, error: Option<String>) {
        if self.prerequisites.is_none() {
            return;
        }
        match self.client.outcome(&key, error.as_deref()) {
            Ok(()) => self.ready.push_back(Ready::PrerequisiteNext),
            Err(e) => self.finish_prerequisites(Err(e.to_string())),
        }
    }
    pub(super) fn finish_prerequisites(&mut self, outcome: std::result::Result<Value, String>) {
        if let Some(active) = self.prerequisites.take() {
            if let Some(effect) = active.effect {
                self.cancel_effect(&effect);
            }
            self.complete(active.request_id, outcome);
        }
    }
    /// A rebuild replaced the replica: the handler in flight worked for a
    /// task of the old one. The loop goes on against the new replica.
    pub(super) fn rebuilt_prerequisites(&mut self) {
        self.ready.retain(|ready| {
            !matches!(
                ready,
                Ready::PrerequisiteOutcome { .. } | Ready::PrerequisiteNext
            )
        });
        let Some(active) = &mut self.prerequisites else {
            return;
        };
        if let Some(effect) = active.effect.take() {
            self.cancel_effect(&effect);
        }
        self.ready.push_back(Ready::PrerequisiteNext);
    }
}
