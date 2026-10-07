//! The task table: request correlation, the ordinary FIFO, scheduling,
//! rebuild fencing and close
//! ([#134](https://github.com/zanminwang/axton/issues/134)).
use super::effects::Ready;
use super::transactions::{Continuation, StoreContinuation, TransactionOwner};
use super::*;
use crate::ClientStore;
use std::collections::{BTreeSet, VecDeque};

/// Every request id still routed - queued, running, parked behind a callback
/// or waiting on the continuation lane - and the ordinary FIFO.
#[derive(Default)]
pub(super) struct Tasks {
    routed: BTreeSet<String>,
    queue: VecDeque<Queued>,
    initial_reads: VecDeque<Queued>,
}
pub(super) struct Queued {
    pub(super) request_id: String,
    pub(super) command: Command,
    /// Its admission number, which orders it against lane work.
    seq: u64,
}
impl Tasks {
    /// Route `request_id`; false when it is already routed, which the SDK's
    /// never-reused counter rules out unless it violates the contract.
    fn admit(&mut self, request_id: &str) -> bool {
        self.routed.insert(request_id.to_string())
    }
    pub(super) fn release(&mut self, request_id: &str) {
        self.routed.remove(request_id);
    }
}

impl<S: ClientStore + 'static> ClientRuntime<S> {
    /// Admit one input: queue, correlate or record it. Runs no database work,
    /// so control and callback answers are serviceable while a callback holds
    /// the transaction.
    pub fn receive(
        &mut self,
        input: Input,
        now: u64,
        entropy: u64,
    ) -> std::result::Result<(), BridgeError> {
        if self.lifecycle != Lifecycle::Open {
            return Err(BridgeError::Closed);
        }
        self.loads.clock = self.loads.clock.max(now);
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
    /// Run at most one local unit and say whether anything happened. Close
    /// goes first; an open transaction's lane and result come before
    /// anything else, which waits while a callback owns the writer. Otherwise
    /// ordinary tasks and lane units run in admission order. The observers
    /// publish what the unit changed before it ends.
    pub fn step(&mut self, now: u64, entropy: u64) -> bool {
        match self.lifecycle {
            Lifecycle::Closed => return false,
            Lifecycle::Closing => {
                self.close();
                return true;
            }
            Lifecycle::Open => {}
        }
        self.loads.clock = self.loads.clock.max(now);
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
    /// Number one admission. Exhaustion is not reachable in practice; saturate
    /// rather than wrap so the order never inverts.
    pub(super) fn admission(&mut self) -> u64 {
        self.admitted = self.admitted.saturating_add(1);
        self.admitted
    }
    fn lane_ready(&self) -> bool {
        !self.ready.is_empty()
            || self
                .connection
                .as_ref()
                .is_some_and(|c| c.downlink.dirty || c.push.dirty)
            || self.prerequisites.ready()
            || self.load_ready()
    }
    /// One lane unit: a ready continuation, else a prerequisite turn, a Load
    /// unit, a Downlink pump or a push-lane turn. A prerequisite turn is one
    /// read that runs only when a commit or an outcome made it dirty, so it
    /// cannot starve the others. The Load lane alternates with the other two: when
    /// both have work, the one that did not have the last turn goes. A
    /// continuation that committed wakes both lanes.
    fn lane_unit(&mut self, now: u64, entropy: u64) {
        if let Some(ready) = self.ready.pop_front() {
            let generation = self.client.generation();
            match ready {
                Ready::PushReceipt { body } => self.push_receipt(body, now, entropy),
                Ready::ApplyDirect {
                    request_id,
                    response,
                } => self.apply_direct(request_id, response, now, entropy),
                Ready::PrerequisiteOutcome { key, error } => self.prerequisite_outcome(key, error),
            }
            if self.client.generation() != generation {
                self.wake_lanes(now, entropy);
            }
            return;
        }
        if self.prerequisites.ready() {
            self.prerequisite_turn(now);
            return;
        }
        let (downlink, push) = self
            .connection
            .as_ref()
            .map_or((false, false), |c| (c.downlink.dirty, c.push.dirty));
        if self.load_ready() && (!self.loads.served || !(downlink || push)) {
            self.loads.served = true;
            self.load_turn(now, entropy);
            return;
        }
        self.loads.served = false;
        if downlink {
            self.downlink_turn(now, entropy);
        } else if push {
            self.push_turn(now, entropy);
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
                self.complete(task.request_id, Err(direct::UNAVAILABLE.into()));
            }
        }
    }
    /// One ordinary task. The runtime-owned lifecycles - the transaction, the
    /// connection, direct calls, readiness, rebuild and the observers -
    /// are decided here; everything else is a command against the client. A
    /// task that committed wakes both lanes.
    fn ordinary_unit(&mut self, now: u64, entropy: u64) {
        let Some(Queued {
            request_id,
            command,
            ..
        }) = self.tasks.queue.pop_front()
        else {
            return;
        };
        if self.protocol05
            && self.connection.is_some()
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
            Command::Ack { sequence, receipt }
                if self.has_store_hook_candidate(
                    receipt["records"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(|record| record["model"].as_str().map(str::to_string)),
                ) =>
            {
                let bytes = match serde_json::to_vec(receipt) {
                    Ok(bytes) => bytes,
                    Err(e) => return self.complete(request_id, Err(e.to_string())),
                };
                let decoded = if receipt.get("completions").is_some() {
                    crate::PushReceipt::decode_action_envelope(&bytes)
                } else {
                    crate::PushReceipt::decode(&bytes)
                };
                match decoded {
                    Ok(receipt) => self.open_store(
                        crate::StoreDelivery::Receipt {
                            sequence: *sequence,
                            receipt,
                        },
                        StoreContinuation::Ack { request_id },
                        now,
                        entropy,
                    ),
                    Err(e) => self.complete(request_id, Err(e.to_string())),
                }
                return;
            }
            Command::Pull { page }
                if self.has_store_hook_candidate(
                    page["changes"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter(|record| record["kind"] == "upsert")
                        .filter_map(|record| record["model"].as_str().map(str::to_string)),
                ) =>
            {
                match serde_json::to_vec(page)
                    .map_err(|e| crate::invalid(e.to_string()))
                    .and_then(|bytes| crate::StreamPullPage::decode(&bytes))
                {
                    Ok(page) => self.open_store(
                        crate::StoreDelivery::StreamPage(page),
                        StoreContinuation::Pull { request_id },
                        now,
                        entropy,
                    ),
                    Err(e) => self.complete(request_id, Err(e.to_string())),
                }
                return;
            }
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
                once,
                refresh,
            } if self.protocol05 && (once.is_some() || refresh.is_some()) => {
                Some(Err("Query accepts only store".into()))
            }
            Command::Invoke {
                name,
                version,
                args,
                store,
                once,
                refresh,
            } => self.invoke(
                &request_id,
                direct::Invocation {
                    name,
                    version: *version,
                    args,
                    store,
                    once: once.unwrap_or(false),
                    refresh: refresh.unwrap_or(false),
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
            Command::Rebuild { discard_pending } => {
                Some(self.rebuild(discard_pending.unwrap_or(false), now, entropy))
            }
            Command::StreamSubscribe { stream } => Some(self.subscribe_stream(stream)),
            Command::StreamBootstrap { .. } => self.bootstrap_stream(&request_id, &command),
            Command::Watch { model, spec } => Some(self.watch(model, spec.as_ref())),
            Command::WatchSql { sql, parameters } => Some(self.watch_sql(sql, parameters)),
            Command::Unwatch { observer_id } => {
                self.unwatch_unsent(observer_id);
                Some(self.unwatch(observer_id))
            }
            Command::LoadStart { .. }
            | Command::LoadGet { .. }
            | Command::LoadStatus { .. }
            | Command::LoadList { .. }
            | Command::LoadWait { .. }
            | Command::LoadCancel { .. }
            | Command::LoadRetry { .. }
            | Command::LoadForget { .. }
            | Command::LoadInvalidate { .. }
            | Command::LoadDispose { .. } => {
                if self.protocol05 || self.client.request_context().is_ok() {
                    Some(Err("Load is retired in protocol 4".into()))
                } else {
                    self.load_task(&request_id, &command)
                }
            }
            _ => Some(commands::execute(&mut self.client, &command).map_err(|e| e.to_string())),
        };
        self.committed_since(generation);
        if let Some(Ok(value)) = &outcome {
            self.removed(&command, value);
            // The protocol seams settle calls too; every final outcome
            // travels as `callCompleted`, after the commit that decided it.
            if matches!(
                command,
                Command::Ack { .. }
                    | Command::Pull { .. }
                    | Command::Drop { .. }
                    | Command::Discard { .. }
            ) {
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
    /// `rebuild {discardPending?}`: the report the client answers, plus the
    /// fence - everything in flight belongs to the replaced replica. Lane
    /// effects are cancelled and the lanes start over in the same intent,
    /// direct calls fail with an unknown execution (Fetches with
    /// `fetch.schema_changed`, their flights fenced), the prerequisite handler
    /// in flight is cancelled and the new replica scanned, every observer of the old replica ends and every abandoned
    /// durable call is completed. A refused rebuild changes nothing.
    fn reset_store(
        &mut self,
        discard_pending: bool,
        now: u64,
        entropy: u64,
    ) -> std::result::Result<Value, String> {
        let report = self
            .client
            .reset_store04(discard_pending)
            .map_err(|error| error.to_string())?;
        self.lanes.cycle = crate::SyncCycle::default();
        self.lanes.downlink.reset_for_rebuild();
        self.fence_directs();
        self.rebuilt_prerequisites();
        self.rebuilt_loads(&[]);
        self.rebuilt_lanes(now, entropy);
        self.observers.stale = true;
        self.rebuilt_observers();
        for abandoned in &report.abandoned_calls {
            self.events.push(Event::CallCompleted { call_id:abandoned.call_id.clone(), outcome:json!({"status":"failed","code":"abandoned","execution":if abandoned.frozen {"unknown"}else{"rejected"}}) });
        }
        serde_json::to_value(report).map_err(|error| error.to_string())
    }
    fn rebuild(
        &mut self,
        discard_pending: bool,
        now: u64,
        entropy: u64,
    ) -> std::result::Result<Value, String> {
        let report = self
            .client
            .rebuild(discard_pending)
            .map_err(|e| e.to_string())?;
        // The push cycle starts over with the fresh replica; the worker
        // forgets the old one but keeps its intent and its identifier
        // allocators, so no answer to old I/O can match new I/O (#162).
        self.lanes.cycle = crate::SyncCycle::default();
        self.lanes.downlink.reset_for_rebuild();
        self.fence_directs();
        self.rebuilt_prerequisites();
        self.rebuilt_loads(&report.abandoned_loads);
        self.rebuilt_lanes(now, entropy);
        self.observers.stale = true;
        self.rebuilt_observers();
        for abandoned in &report.abandoned_calls {
            self.events.push(Event::CallCompleted {
                call_id: abandoned.call_id.clone(),
                outcome: json!({
                    "status": "failed",
                    "code": "abandoned",
                    "execution": if abandoned.frozen { "unknown" } else { "rejected" },
                }),
            });
        }
        Ok(commands::rebuild_json(&report))
    }
    /// The `completions` of an `ack`, `pull` or `drop` answer, announced.
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
    /// Priority close: roll back the open session, cancel every outstanding
    /// effect (a local callback's too), turn the provisional calls
    /// `rolledBack`, fail the parked parent, a submission waiting on its
    /// local callback, its queued commands and every queued task with
    /// `client_closed`, fail direct calls as unavailable, forget the
    /// prerequisite handler run, end the lanes and the observers, then
    /// announce the end. Nothing is applied after it.
    fn close(&mut self) {
        let mut transaction = self.transaction.take();
        match transaction.as_ref().map(|transaction| &transaction.owner) {
            Some(TransactionOwner::Authority { .. }) => self.abort_authority_session(),
            Some(TransactionOwner::Application { .. }) => {
                if let Err(e) = self.client.rollback_session() {
                    self.error(format!("rollback at close failed: {e}"));
                }
            }
            None => {}
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
                TransactionOwner::Authority { continuation, .. } => match continuation {
                    // The direct flight still owns its caller (and any joined
                    // once callers). The common direct-close path below settles
                    // them as unavailable after this session is rolled back.
                    StoreContinuation::Direct { .. } | StoreContinuation::Fetch { .. } => {}
                    continuation => {
                        continuation.fail(self, "client_closed".into(), None, None, &[], 0, 0)
                    }
                },
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
        self.close_loads();
        self.close_observers();
        self.close_unsent();
        self.lifecycle = Lifecycle::Closed;
        self.events.push(Event::RuntimeClosed);
    }
}
