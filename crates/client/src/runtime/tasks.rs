//! The task table: request correlation, the ordinary FIFO, scheduling,
//! rebuild fencing and close
//! ([#134](https://github.com/zanminwang/axton/issues/134)).
use super::effects::Ready;
use super::transactions::{Continuation, TransactionOwner};
use super::*;
use crate::ClientStore;
use std::collections::{BTreeSet, VecDeque};

// Every request id still routed - queued, running, parked behind a callback
// or waiting on the continuation lane - and the ordinary FIFO.
#[derive(Default)]
pub(super) struct Tasks {
    routed: BTreeSet<String>,
    queue: VecDeque<Queued>,
    initial_reads: VecDeque<Queued>,
}
pub(super) struct Queued {
    pub(super) request_id: String,
    pub(super) command: Command,
    // Its admission number, which orders it against lane work.
    seq: u64,
}
impl Tasks {
    // Route `request_id`; false when it is already routed, which the SDK's
    // never-reused counter rules out unless it violates the contract.
    fn admit(&mut self, request_id: &str) -> bool {
        self.routed.insert(request_id.to_string())
    }
    pub(super) fn release(&mut self, request_id: &str) {
        self.routed.remove(request_id);
    }
}

impl<S: ClientStore + 'static> ClientRuntime<S> {
    // Admit one input: queue, correlate or record it. Runs no database work,
    // so control and callback answers are serviceable while a callback holds
    // the transaction.
    pub fn receive(
        &mut self,
        input: Input,
        now: u64,
        entropy: u64,
    ) -> std::result::Result<(), BridgeError> {
        if self.lifecycle != Lifecycle::Open {
            return Err(BridgeError::Closed);
        }
        match input {
            Input::Task {
                request_id,
                command,
            } => {
                if self.admit(&request_id) {
                    let seq = self.admission();
                    self.tasks.queue.push_back(Queued {
                        request_id,
                        command,
                        seq,
                    });
                }
            }
            Input::TransactionCommand {
                request_id,
                transaction_id,
                scope,
                companion_id,
                command,
            } => {
                if self.admit(&request_id) {
                    self.continue_transaction(Continuation {
                        request_id,
                        transaction_id,
                        scope,
                        companion_id,
                        command,
                    });
                }
            }
            Input::CallbackResult {
                effect_id,
                transaction_id,
                companion_id,
                ok,
                error,
                input,
            } => self.callback_result(
                &effect_id,
                &transaction_id,
                companion_id.as_deref(),
                ok,
                error,
                input,
            ),
            Input::EffectResult { effect_id, outcome } => {
                self.effect_result(effect_id, outcome, now, entropy);
                // Inbound work takes its place in the arrival order now, so an
                // ordinary task admitted after it waits its turn.
                if self.lane_ready() && self.lane_since.is_none() {
                    self.lane_since = Some(self.admitted);
                }
                // A session that ended or a catch-up that answered is a
                // transport fact the statuses show at once.
                self.publish_statuses();
            }
            Input::Close => self.lifecycle = Lifecycle::Closing,
        }
        Ok(())
    }
    // Run at most one local unit and say whether anything happened. Close
    // goes first; an open transaction's lane and result come before
    // anything else, which waits while a callback owns the writer. Otherwise
    // ordinary tasks and lane units run in admission order. The observers
    // publish what the unit changed before it ends.
    pub fn step(&mut self, now: u64, entropy: u64) -> bool {
        match self.lifecycle {
            Lifecycle::Closed => return false,
            Lifecycle::Closing => {
                self.close();
                return true;
            }
            Lifecycle::Open => {}
        }
        let ran = self.unit(now, entropy);
        if ran {
            self.publish_unsent();
            self.publish();
        }
        ran
    }
    fn unit(&mut self, now: u64, entropy: u64) -> bool {
        if self.transaction.is_some() {
            return self.step_transaction(now, entropy);
        }
        let head = self.tasks.queue.front().map(|task| task.seq);
        let lane_since = if self.lane_ready() {
            Some(*self.lane_since.get_or_insert(self.admitted))
        } else {
            self.lane_since = None;
            None
        };
        // Admission order decides: lane work runs when it was ready before
        // the oldest ordinary task arrived, otherwise that task goes first.
        // After a lane unit the lane re-enters the order behind everything
        // admitted so far.
        match (head, lane_since) {
            (None, None) => return false,
            (Some(_), None) => self.ordinary_unit(now, entropy),
            (Some(head), Some(since)) if head <= since => self.ordinary_unit(now, entropy),
            (_, Some(_)) => {
                self.lane_since = None;
                self.lane_unit(now, entropy);
            }
        }
        true
    }
    // Number one admission. Exhaustion is not reachable in practice; saturate
    // rather than wrap so the order never inverts.
    pub(super) fn admission(&mut self) -> u64 {
        self.admitted = self.admitted.saturating_add(1);
        self.admitted
    }
    fn lane_ready(&self) -> bool {
        !self.ready.is_empty() || self.prerequisites.ready()
    }
    // One lane unit: a ready continuation, else a prerequisite turn, a Load
    // unit, a Downlink pump or a push-lane turn. A prerequisite turn is one
    // read that runs only when a commit or an outcome made it dirty, so it
    // cannot starve the others. The Load lane alternates with the other two: when
    // both have work, the one that did not have the last turn goes. A
    // continuation that committed wakes both lanes.
    fn lane_unit(&mut self, now: u64, entropy: u64) {
        if let Some(ready) = self.ready.pop_front() {
            let generation = self.client.generation();
            match ready {
                Ready::ApplyDirect {
                    request_id,
                    response,
                } => self.apply_direct(request_id, response, now, entropy),
                Ready::PrerequisiteOutcome { key, error } => self.prerequisite_outcome(key, error),
            };
            if self.client.generation() != generation {
                self.wake_lanes(now, entropy)
            };
            return;
        }
        if self.prerequisites.ready() {
            self.prerequisite_turn(now)
        }
    }
    pub(super) fn release_initial_reads05(&mut self, initialized: bool) {
        if initialized {
            while let Some(task) = self.tasks.initial_reads.pop_back() {
                self.tasks.queue.push_front(task)
            }
        } else {
            let tasks = std::mem::take(&mut self.tasks.initial_reads);
            for task in tasks {
                if matches!(task.command, Command::Fetch { .. }) {
                    self.fail(
                        task.request_id,
                        direct::FETCH_UNAVAILABLE,
                        direct::code(direct::FETCH_UNAVAILABLE),
                    );
                } else {
                    self.complete(task.request_id, Err(direct::UNAVAILABLE.into()));
                }
            }
        }
    }
    // One ordinary task. The runtime-owned lifecycles - the transaction, the
    // connection, direct calls, readiness, rebuild and the observers -
    // are decided here; everything else is a command against the client. A
    // task that committed wakes both lanes.
    fn ordinary_unit(&mut self, now: u64, entropy: u64) {
        let Some(Queued {
            request_id,
            command,
            ..
        }) = self.tasks.queue.pop_front()
        else {
            return;
        };
        if self.connection.is_some()
            && matches!(&command, Command::Invoke { .. } | Command::Fetch { .. })
        {
            let schema_pending = self.client.pending_schema05().is_ok_and(|s| s.is_some());
            let storing = matches!(&command, Command::Invoke {store,..} | Command::Fetch {store,..} if store.as_ref().is_none_or(|v|v==&Value::Bool(true)));
            let initial_pending = storing
                && self
                    .client
                    .store_status05()
                    .is_ok_and(|s| s.start_cursor.is_none());
            if schema_pending || initial_pending {
                self.tasks.initial_reads.push_back(Queued {
                    request_id,
                    command,
                    seq: self.admitted,
                });
                return;
            }
        }
        let generation = self.client.generation();
        let outcome = match &command {
            Command::Transaction => return self.open_transaction(request_id),
            Command::Connect {
                direct_timeout_ms,
                refresh_auth,
            } => Some(self.connect(
                *direct_timeout_ms,
                refresh_auth.unwrap_or(false),
                now,
                entropy,
            )),
            Command::Connection { event } => Some(self.control(*event, now, entropy)),
            Command::Invoke {
                name,
                version,
                args,
                store,
            } => self.invoke(
                &request_id,
                direct::Invocation {
                    name,
                    version: *version,
                    args,
                    store,
                },
            ),
            Command::Fetch {
                model,
                version,
                identity,
                store,
            } => {
                self.fetch(&request_id, model, *version, identity, store);
                None
            }
            Command::Readiness { key, .. } => {
                self.prerequisite_readiness(key);
                Some(commands::execute(&mut self.client, &command).map_err(|e| e.to_string()))
            }
            // A retry that did not commit leaves the backoff alone.
            Command::RetryTasks { keys } => {
                let outcome =
                    commands::execute(&mut self.client, &command).map_err(|e| e.to_string());
                if outcome.is_ok() {
                    for key in keys {
                        self.prerequisite_readiness(key);
                    }
                }
                Some(outcome)
            }
            Command::UnsentWatch { view } => Some(self.unsent_watch(*view)),
            Command::ResetStore { discard_pending } => {
                Some(self.reset_store(discard_pending.unwrap_or(false), now, entropy))
            }
            Command::StreamSubscribe { stream } => Some(self.subscribe_stream(stream)),
            Command::StreamBootstrap { .. } => self.bootstrap_stream(&request_id, &command),
            Command::Watch { model, spec } => Some(self.watch(model, spec.as_ref())),
            Command::WatchSql { sql, parameters } => Some(self.watch_sql(sql, parameters)),
            Command::Unwatch { observer_id } => {
                self.unwatch_unsent(observer_id);
                Some(self.unwatch(observer_id))
            }
            _ => Some(commands::execute(&mut self.client, &command).map_err(|e| e.to_string())),
        };
        self.committed_since(generation);
        if let Some(Ok(value)) = &outcome {
            // The protocol seams settle calls too; every final outcome
            // travels as `callCompleted`, after the commit that decided it.
            if matches!(command, Command::Drop { .. } | Command::Discard { .. }) {
                self.seam_completions(value);
            }
        }
        if self.client.generation() != generation {
            self.wake_lanes(now, entropy);
        }
        if let Some(outcome) = outcome {
            self.complete(request_id, outcome);
        }
    }
    // Reset the bound Store and cancel work belonging to its old incarnation.
    fn reset_store(
        &mut self,
        discard_pending: bool,
        _now: u64,
        _entropy: u64,
    ) -> std::result::Result<Value, String> {
        let report = self
            .client
            .reset_store05(discard_pending)
            .map_err(|e| e.to_string())?;
        let value = serde_json::to_value(&report).map_err(|e| e.to_string())?;
        let abandoned_calls = report.abandoned_calls;
        self.fence_directs();
        self.rebuilt_prerequisites();
        self.observers.stale = true;
        self.rebuilt_observers();
        self.release_initial_reads05(false);
        self.sync05_live = false;
        self.sync05_catching_up = false;
        for abandoned in &abandoned_calls {
            self.events.push(Event::CallCompleted { call_id:abandoned.call_id.clone(), outcome:json!({"status":"failed","code":"abandoned","execution":if abandoned.frozen {"unknown"}else{"rejected"}}) });
        }
        Ok(value)
    }

    // The `completions` of an `ack`, `pull` or `drop` answer, announced.
    pub(super) fn seam_completions(&mut self, value: &Value) {
        for completion in value["completions"].as_array().into_iter().flatten() {
            self.events.push(Event::CallCompleted {
                call_id: completion["callId"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
                outcome: completion["outcome"].clone(),
            });
        }
    }
    fn admit(&mut self, request_id: &str) -> bool {
        if self.tasks.admit(request_id) {
            return true;
        }
        self.report(Diagnostic::Protocol {
            message: format!("duplicate request id {request_id}"),
        });
        false
    }
    // Priority close: roll back the open session, cancel every outstanding
    // effect (a local callback's too), turn the provisional calls
    // `rolledBack`, fail the parked parent, a submission waiting on its
    // local callback, its queued commands and every queued task with
    // `client_closed`, fail direct calls as unavailable, forget the
    // prerequisite handler run, end the lanes and the observers, then
    // announce the end. Nothing is applied after it.
    fn close(&mut self) {
        let mut transaction = self.transaction.take();
        if transaction.is_some()
            && let Err(e) = self.client.rollback_session()
        {
            self.error(format!("rollback at close failed: {e}"));
        }
        for effect_id in std::mem::take(&mut self.effects).into_keys() {
            self.events.push(Event::CancelEffect { effect_id });
        }
        if let Some(transaction) = &mut transaction {
            let calls = std::mem::take(&mut transaction.calls);
            self.call_transitions(calls, CallTransition::RolledBack);
        }
        if let Some(transaction) = transaction {
            let local = transaction.local;
            match transaction.owner {
                TransactionOwner::Application { request_id } => {
                    self.complete(request_id, Err("client_closed".into()))
                }
            }
            if let Some(local) = local {
                self.complete(local.request_id, Err("client_closed".into()));
            }
            for command in transaction.lane {
                self.complete(command.request_id, Err("client_closed".into()));
            }
        }
        self.tasks.queue.append(&mut self.tasks.initial_reads);
        for task in std::mem::take(&mut self.tasks.queue) {
            self.complete(task.request_id, Err("client_closed".into()));
        }
        self.fail_directs(direct::Failure::Unavailable);
        self.close_prerequisites();
        self.ready.clear();
        self.close_lanes();
        self.close_observers();
        self.close_unsent();
        self.lifecycle = Lifecycle::Closed;
        self.events.push(Event::RuntimeClosed);
    }
}
