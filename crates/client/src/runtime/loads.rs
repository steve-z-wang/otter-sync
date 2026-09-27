//! Native Loads as runtime work ([#173](https://github.com/zanminwang/axton/issues/173)):
//! the Load commands, the [`LoadWorker`](crate::LoadWorker)'s decisions
//! executed as effects, page application through the owned store session,
//! and the status observers and waiters of Load handles.
//!
//! **Lane units.** The worker's work runs as lane units in the runtime's
//! admission order, alternating with the Downlink and push lanes: one unit
//! applies one received page (or records one failure), or dispatches one
//! batch. A batch waits on its `http` effect (`load` route) and a per-attempt
//! deadline `timer` without holding the writer; its answer is decoded and
//! queued as it is admitted, without database work. A page is stored through
//! [`ClientRuntime::open_store`] under the same fence, preparation, `onStore`
//! and commit rules as every other delivery, and nothing is published before
//! that commit.
//!
//! **Failures.** A transport failure, a deadline, a 429 or server failure, a
//! response envelope that does not correlate, and a retryable item keep the
//! frozen call ID and back off from the persisted attempt count. A 401 joins
//! the connection's one credential refresh and resends the same body once; a
//! refresh refused with HTTP status 401 or 403 fails the batch's jobs
//! `load.unauthorized`, any other refresh failure backs off. Pause and stop
//! abandon unanswered batches without a failure; an answer in hand is still
//! applied.
//!
//! **Handles.** `loadStart` and `loadGet` issue an observer per handle that
//! publishes the job's status - its stored phase projected with what the
//! runtime knows: a page in flight is `loading`; offline, paused, backing
//! off or parked behind a pending rebuild is `waiting`. `loadWait` parks on
//! the job's current run: it completes after the final page committed or
//! fails with the run's stored error, and settles only from that run. Dispose
//! releases one observer; close fails waiters
//! `client_closed` and a rebuild fails them `load.schema_changed`, ending
//! every observer, without cancelling durable jobs.
use super::effects::{EffectKind, Waiter};
use super::transactions::StoreContinuation;
use super::*;
use crate::loads::{INVALID_OPTIONS, NOT_FOUND};
use crate::{
    ApplyReport, ClientStore, LoadAnswer, LoadApply, LoadFailure, LoadJob, LoadJobError,
    LoadOptions, LoadPageStep, LoadPhase, LoadReceived, LoadSent, LoadStartKind, LoadStatus,
    LoadWorker,
};

/// A refresh that was refused (not merely failed) leaves the batch's jobs
/// without credentials.
pub(super) const UNAUTHORIZED: &str = "load.unauthorized";
/// A rebuild replaced the replica the job belonged to.
pub(super) const SCHEMA_CHANGED: &str = "load.schema_changed";
const CLIENT_CLOSED: &str = "client_closed";
const TIMED_OUT: &str = "load request timed out";
const DEFAULT_LIST: u64 = 50;

/// The runtime's Load state: the worker and what executes its decisions.
#[derive(Default)]
pub(super) struct Loads {
    pub(super) worker: LoadWorker,
    /// The effects of every unanswered batch.
    flights: BTreeMap<u64, Flight>,
    /// The backoff timer and the time it fires at.
    timer: Option<(String, u64)>,
    /// Jobs some handle observes or waits on, by ID.
    tracked: BTreeMap<String, Tracked>,
    /// Observer IDs to the job each one observes.
    observers: BTreeMap<String, String>,
    /// The Load lane had the last lane turn: Downlink or push goes next.
    pub(super) served: bool,
    /// The latest clock the runtime was driven with, for the projection.
    pub(super) clock: u64,
}
struct Flight {
    http: Option<String>,
    deadline: Option<String>,
    /// The one resend after a credential refresh was used.
    refreshed: bool,
}
struct Tracked {
    /// The committed status this runtime last saw.
    status: LoadStatus,
    run: u64,
    /// Each observer and the snapshot it last published.
    observers: BTreeMap<String, Option<Value>>,
    /// Parked `loadWait` tasks and the run each attached to.
    waiters: Vec<(String, u64)>,
}

/// A management refusal: `{code}` when the engine coded it.
fn refusal(error: &str) -> (String, Option<Value>) {
    let coded = error
        .split_once(": ")
        .filter(|(code, _)| code.starts_with("load.") && !code.contains(' '));
    match coded {
        Some((code, message)) => (
            error.to_string(),
            Some(json!({"code": code, "message": message})),
        ),
        None => (error.to_string(), None),
    }
}
/// An option that is absent, `null` or a boolean.
fn flag(value: &Option<Value>) -> std::result::Result<bool, ()> {
    match value {
        None | Some(Value::Null) => Ok(false),
        Some(Value::Bool(flag)) => Ok(*flag),
        Some(_) => Err(()),
    }
}
fn identity_of(change: &crate::StoreChange) -> Value {
    match change {
        crate::StoreChange::Upsert { identity, .. } | crate::StoreChange::Delete { identity } => {
            identity.clone()
        }
    }
}
/// The identities one Model's hook was handed, for a hook failure's
/// diagnostics.
pub(super) fn hook_identities(prepared: &crate::PreparedStore, model: Option<&str>) -> Vec<Value> {
    model
        .and_then(|model| prepared.changes().get(model))
        .map(|changes| changes.iter().map(identity_of).collect())
        .unwrap_or_default()
}

impl<S: ClientStore + 'static> ClientRuntime<S> {
    // --- Readiness and lane units ------------------------------------------

    /// Whether pages may go out: a running connection that is not paused and
    /// no pending incompatible rebuild.
    pub(super) fn load_online(&self) -> bool {
        self.connection.as_ref().is_some_and(|c| !c.paused)
            && self.client.schema_state().pending.is_none()
    }
    /// Whether the Load lane has a unit to run.
    pub(super) fn load_ready(&self) -> bool {
        self.loads.worker.has_outcome()
            || (self.loads.worker.wants_dispatch() && self.load_online())
    }
    /// One Load lane unit: apply (or record) one received page, else send
    /// one batch.
    pub(super) fn load_turn(&mut self, now: u64, entropy: u64) {
        if let Some(received) = self.loads.worker.next_outcome() {
            self.apply_load(received, now, entropy);
        } else if self.loads.worker.wants_dispatch() && self.load_online() {
            self.dispatch_loads(now, entropy);
        }
    }
    fn dispatch_loads(&mut self, now: u64, entropy: u64) {
        match self.loads.worker.dispatch(&mut self.client, now, entropy) {
            Ok(step) => {
                for issue in step.issues {
                    self.error(format!("load ledger {}: {}", issue.load_id, issue.detail));
                }
                if let Some(dispatch) = step.dispatch {
                    self.loads.flights.insert(
                        dispatch.batch,
                        Flight {
                            http: None,
                            deadline: None,
                            refreshed: false,
                        },
                    );
                    self.send_loads(dispatch.batch);
                }
            }
            Err(error) => {
                // The scheduler read failed: look again after a bounded delay
                // the worker keeps until a dispatch runs.
                self.error(format!("load scheduling failed: {error}"));
                self.loads.worker.scan_failed(now, entropy);
            }
        }
        self.arm_load_timer(now);
    }
    /// Send (or resend) the exact body of `batch` with a fresh attempt
    /// deadline.
    fn send_loads(&mut self, batch: u64) {
        let (Some(body), Some(timeout)) = (
            self.loads.worker.unanswered(batch).map(str::to_string),
            self.connection.as_ref().map(|c| c.timeout),
        ) else {
            self.abandon_load_batch(batch);
            return;
        };
        if let Some(old) = self
            .loads
            .flights
            .get_mut(&batch)
            .and_then(|f| f.deadline.take())
        {
            self.cancel_effect(&old);
        }
        let http = self.issue_effect(
            EffectKind::LoadHttp { batch },
            Operation::Http {
                route: HttpRoute::Load,
                body,
            },
        );
        let deadline = self.issue_effect(
            EffectKind::LoadDeadline { batch },
            Operation::Timer { millis: timeout },
        );
        match (http, self.loads.flights.get_mut(&batch)) {
            (Some(http), Some(flight)) => {
                flight.http = Some(http);
                flight.deadline = deadline;
            }
            _ => self.fail_load_batch(
                batch,
                LoadFailure::transport("runtime identifiers exhausted"),
            ),
        }
    }

    // --- Effect results (admission: no database work) ----------------------

    /// The answer of a batch request: queued for the writer, or a failure -
    /// after one shared refresh on 401.
    pub(super) fn load_result(&mut self, batch: u64, outcome: EffectOutcome) {
        let Some(flight) = self.loads.flights.get_mut(&batch) else {
            return;
        };
        flight.http = None;
        let refreshed = flight.refreshed;
        match effects::http_body(outcome) {
            Ok(body) => {
                if let Some(flight) = self.loads.flights.remove(&batch)
                    && let Some(deadline) = flight.deadline
                {
                    self.cancel_effect(&deadline);
                }
                if let Err(message) = self.loads.worker.answered(batch, body.as_bytes()) {
                    self.error(message);
                }
            }
            Err(error) => {
                self.error_status(error.message.clone(), error.status);
                let refresh = error.status == Some(401)
                    && !refreshed
                    && self.connection.as_ref().is_some_and(|c| c.refresh);
                let refused = matches!(error.status, Some(400..=499))
                    && !matches!(error.status, Some(401 | 408 | 429));
                if refresh {
                    if let Some(flight) = self.loads.flights.get_mut(&batch) {
                        flight.refreshed = true;
                    }
                    self.join_refresh(Waiter::Load { batch });
                } else if refused {
                    // The backend refused the request whole: its pages go
                    // alone, and a page refused alone fails its job.
                    self.retire_load_flight(batch);
                    self.loads.worker.rejected(batch, &error.message);
                } else {
                    self.fail_load_batch(batch, LoadFailure::transport(error.message));
                }
            }
        }
    }
    /// The attempt deadline passed first: the request is abandoned and every
    /// page backs off under its frozen call ID.
    pub(super) fn load_deadline(&mut self, batch: u64) {
        if let Some(flight) = self.loads.flights.get_mut(&batch) {
            flight.deadline = None;
            self.fail_load_batch(batch, LoadFailure::transport(TIMED_OUT));
        }
    }
    /// The shared refresh settled for a batch that met a 401.
    pub(super) fn load_refreshed(&mut self, batch: u64, refused: Option<&EffectError>) {
        if !self.loads.flights.contains_key(&batch) {
            return;
        }
        match refused {
            None => self.send_loads(batch),
            Some(error) if matches!(error.status, Some(401 | 403)) => self.fail_load_batch(
                batch,
                LoadFailure::Local(LoadJobError::new(UNAUTHORIZED, &error.message, vec![])),
            ),
            Some(error) => self.fail_load_batch(batch, LoadFailure::transport(&error.message)),
        }
    }
    /// The backoff timer fired: jobs whose delay passed are ready again.
    pub(super) fn load_timer_fired(&mut self, effect_id: &str) {
        if self
            .loads
            .timer
            .as_ref()
            .is_some_and(|(timer, _)| timer == effect_id)
        {
            self.loads.timer = None;
            self.loads.worker.wake();
        }
    }
    /// Every page of `batch` records `failure`; its effects are gone.
    fn fail_load_batch(&mut self, batch: u64, failure: LoadFailure) {
        self.retire_load_flight(batch);
        self.loads.worker.failed(batch, failure);
    }
    /// Pause, stop: `batch` goes without an answer and without a failure.
    fn abandon_load_batch(&mut self, batch: u64) {
        self.retire_load_flight(batch);
        self.loads.worker.abandon(batch);
    }
    fn retire_load_flight(&mut self, batch: u64) {
        if let Some(flight) = self.loads.flights.remove(&batch) {
            for effect in [flight.http, flight.deadline].into_iter().flatten() {
                self.cancel_effect(&effect);
            }
        }
        if let Some(connection) = &mut self.connection {
            connection
                .waiters
                .retain(|w| !matches!(w, Waiter::Load { batch: b } if *b == batch));
        }
    }
    /// Keep one backoff timer, for the earliest job that waits.
    fn arm_load_timer(&mut self, now: u64) {
        match self.loads.worker.next_due(now) {
            Some(due) => self.arm_load_timer_at(due, now),
            None => {
                if let Some((timer, _)) = self.loads.timer.take() {
                    self.cancel_effect(&timer);
                }
            }
        }
    }
    fn arm_load_timer_at(&mut self, due: u64, now: u64) {
        if self.loads.timer.as_ref().is_some_and(|(_, at)| *at == due) || !self.load_online() {
            return;
        }
        if let Some((timer, _)) = self.loads.timer.take() {
            self.cancel_effect(&timer);
        }
        let millis = due.saturating_sub(now).max(1);
        if let Some(timer) = self.issue_effect(EffectKind::LoadTimer, Operation::Timer { millis }) {
            self.loads.timer = Some((timer, due));
        }
    }

    // --- Applying what came back -------------------------------------------

    /// One received page: stale, a failure to record, or a page to store
    /// through the owned session.
    fn apply_load(&mut self, received: LoadReceived, now: u64, entropy: u64) {
        let LoadReceived {
            batch,
            sent,
            answer,
        } = received;
        match answer {
            LoadAnswer::Failure(failure) => self.record_load(batch, sent, failure, now, entropy),
            LoadAnswer::Reply(reply) => match self.client.load_page_step(&sent.fence, reply) {
                Ok(LoadPageStep::Stale) => self.load_consumed(batch, &sent),
                Ok(LoadPageStep::Record(failure)) => {
                    self.record_load(batch, sent, failure, now, entropy)
                }
                Ok(LoadPageStep::Store(delivery)) => self.open_store(
                    delivery,
                    StoreContinuation::Load { batch, sent },
                    now,
                    entropy,
                ),
                Err(error) => self.record_load(
                    batch,
                    sent,
                    LoadFailure::local_retry(error.to_string()),
                    now,
                    entropy,
                ),
            },
        }
    }
    /// A failure found while storing: recorded by the next Load unit, after
    /// this one's rollback.
    pub(super) fn requeue_load(&mut self, batch: u64, sent: LoadSent, failure: LoadFailure) {
        self.loads.worker.requeue(LoadReceived {
            batch,
            sent,
            answer: LoadAnswer::Failure(failure),
        });
    }
    /// Record one failure of the page `sent` names in its own short
    /// transaction. A retryable one backs off from the persisted attempt
    /// count; one that cannot be recorded backs off in memory, its frozen
    /// call unchanged.
    fn record_load(
        &mut self,
        batch: u64,
        sent: LoadSent,
        failure: LoadFailure,
        now: u64,
        entropy: u64,
    ) {
        let generation = self.client.generation();
        let recorded = self.client.record_load_failure(&sent.fence, &failure);
        self.committed_since(generation);
        let id = sent.fence.load_id.clone();
        match recorded {
            Ok(Some(job)) => {
                match (job.phase, &job.call_id) {
                    (LoadPhase::Pending, Some(call)) => {
                        self.loads
                            .worker
                            .back_off(&id, call, job.attempts, now, entropy)
                    }
                    _ => self.loads.worker.settled(&id),
                }
                self.load_changed(&job);
            }
            Ok(None) => {}
            Err(error) => {
                self.error(format!("load {id}: {error}"));
                self.loads
                    .worker
                    .back_off(&id, &sent.fence.call_id, sent.attempts, now, entropy);
            }
        }
        self.load_consumed(batch, &sent);
        self.arm_load_timer(now);
    }
    fn load_consumed(&mut self, batch: u64, sent: &LoadSent) {
        self.loads.worker.consumed(batch, &sent.fence.load_id);
    }
    /// A page's session ended with its commit (or found the job moved on).
    pub(super) fn load_stored(&mut self, batch: u64, sent: LoadSent, apply: LoadApply) {
        match apply {
            LoadApply::Applied { job, report } => {
                self.load_reports(&report);
                self.loads.worker.settled(&sent.fence.load_id);
                self.load_consumed(batch, &sent);
                self.load_changed(&job);
                // Its next page queues behind ready peers.
                self.loads.worker.wake();
            }
            LoadApply::Stale => self.load_consumed(batch, &sent),
            LoadApply::Refused(failure) => self.requeue_load(batch, sent, failure),
        }
    }
    /// What a committed page could not replay: diverged pending edits.
    fn load_reports(&mut self, report: &ApplyReport) {
        if !report.reports.is_empty() {
            self.report(Diagnostic::Records {
                reports: report.reports.clone(),
            });
        }
    }

    // --- Commands ------------------------------------------------------------

    /// One Load task. `None` when it parked (`loadWait`) or already settled
    /// with a coded refusal.
    pub(super) fn load_task(
        &mut self,
        request_id: &str,
        command: &Command,
    ) -> Option<std::result::Result<Value, String>> {
        let outcome = match command {
            Command::LoadStart {
                name,
                version,
                args,
                once,
                refresh,
            } => self.start_load(name, *version, args, once, refresh),
            Command::LoadGet { load_id } => self.get_load(load_id),
            Command::LoadStatus { load_id } => self
                .client
                .get_load(load_id)
                .map_err(|e| e.to_string())
                .and_then(|job| {
                    let job = job.ok_or_else(|| format!("{NOT_FOUND}: Load {load_id}"))?;
                    self.load_changed(&job);
                    Ok(self.load_status_json(job.status()))
                }),
            Command::LoadList { limit } => self.list_loads(limit),
            Command::LoadWait { load_id } => return self.wait_load(request_id, load_id),
            Command::LoadCancel { load_id } => self
                .client
                .cancel_load(load_id)
                .map_err(|e| e.to_string())
                .map(|job| {
                    self.loads.worker.settled(&job.id);
                    self.load_changed(&job);
                    self.load_status_json(job.status())
                }),
            Command::LoadRetry { load_id } => self
                .client
                .retry_load(load_id)
                .map_err(|e| e.to_string())
                .map(|job| {
                    // Retrying active work changes nothing, its backoff
                    // included; a failed job starts clean under a new call.
                    self.loads.worker.retried(&job.id, job.call_id.as_deref());
                    self.loads.worker.wake();
                    self.load_changed(&job);
                    self.load_status_json(job.status())
                }),
            Command::LoadForget { load_id } => self
                .client
                .forget_load(load_id)
                .map_err(|e| e.to_string())
                .map(|()| {
                    if let Ok(id) = uuid::Uuid::parse_str(load_id) {
                        self.loads.worker.settled(&id.to_string());
                    }
                    Value::Null
                }),
            Command::LoadInvalidate { name, args } => self
                .client
                .invalidate_load(name, args)
                .map_err(|e| e.to_string())
                .map(|removed| json!({ "removed": removed })),
            Command::LoadDispose { observer_id } => {
                self.dispose_load(observer_id);
                Ok(Value::Null)
            }
            _ => Err("not a Load command".into()),
        };
        match outcome {
            Ok(value) => Some(Ok(value)),
            Err(error) => {
                match refusal(&error) {
                    (error, Some(details)) => self.fail(request_id.to_string(), error, details),
                    (error, None) => self.complete(request_id.to_string(), Err(error)),
                }
                None
            }
        }
    }
    fn start_load(
        &mut self,
        name: &str,
        version: u64,
        args: &Value,
        once: &Option<Value>,
        refresh: &Option<Value>,
    ) -> std::result::Result<Value, String> {
        let (Ok(once), Ok(refresh)) = (flag(once), flag(refresh)) else {
            return Err(format!(
                "{INVALID_OPTIONS}: once and refresh must be booleans"
            ));
        };
        let started = self
            .client
            .start_load(name, version, args, LoadOptions { once, refresh })
            .map_err(|e| e.to_string())?;
        let start = match started.kind {
            LoadStartKind::Created => {
                self.loads.worker.wake();
                "created"
            }
            LoadStartKind::Joined => "joined",
            LoadStartKind::Reused => "reused",
        };
        let mut answer = self.observe_load(&started.job)?;
        answer["start"] = json!(start);
        Ok(answer)
    }
    fn get_load(&mut self, load_id: &str) -> std::result::Result<Value, String> {
        match self.client.get_load(load_id).map_err(|e| e.to_string())? {
            None => Ok(Value::Null),
            Some(job) => self.observe_load(&job),
        }
    }
    fn list_loads(&mut self, limit: &Option<Value>) -> std::result::Result<Value, String> {
        let limit = match limit {
            None | Some(Value::Null) => DEFAULT_LIST,
            Some(limit) => limit
                .as_u64()
                .ok_or_else(|| format!("{INVALID_OPTIONS}: list limit must be 1..100"))?,
        };
        let statuses = self
            .client
            .list_loads(usize::try_from(limit).unwrap_or(usize::MAX))
            .map_err(|e| e.to_string())?;
        Ok(Value::Array(
            statuses
                .into_iter()
                .map(|status| self.load_status_json(status))
                .collect(),
        ))
    }
    /// Attach to the job's current run. A terminal run answers now.
    fn wait_load(
        &mut self,
        request_id: &str,
        load_id: &str,
    ) -> Option<std::result::Result<Value, String>> {
        let job = match self.client.get_load(load_id) {
            Ok(Some(job)) => job,
            Ok(None) => {
                let error = format!("{NOT_FOUND}: Load {load_id}");
                self.fail(
                    request_id.to_string(),
                    error,
                    json!({"code": NOT_FOUND, "message": format!("Load {load_id}")}),
                );
                return None;
            }
            Err(error) => return Some(Err(error.to_string())),
        };
        let tracked = self.track(&job);
        tracked.waiters.push((request_id.to_string(), job.run));
        self.load_changed(&job);
        None
    }
    /// A handle: a fresh observer of `job`, published after the answer.
    fn observe_load(&mut self, job: &LoadJob) -> std::result::Result<Value, String> {
        let observer = self.issue()?.to_string();
        self.track(job).observers.insert(observer.clone(), None);
        self.loads
            .observers
            .insert(observer.clone(), job.id.clone());
        Ok(json!({
            "loadId": job.id,
            "observerId": observer,
            "status": self.load_status_json(job.status()),
        }))
    }
    fn track(&mut self, job: &LoadJob) -> &mut Tracked {
        self.loads
            .tracked
            .entry(job.id.clone())
            .or_insert_with(|| Tracked {
                status: job.status(),
                run: job.run,
                observers: BTreeMap::new(),
                waiters: vec![],
            })
    }
    fn dispose_load(&mut self, observer_id: &str) {
        let Some(id) = self.loads.observers.remove(observer_id) else {
            return;
        };
        if let Some(tracked) = self.loads.tracked.get_mut(&id) {
            tracked.observers.remove(observer_id);
            if tracked.observers.is_empty() && tracked.waiters.is_empty() {
                self.loads.tracked.remove(&id);
            }
        }
    }

    // --- Status and waiters --------------------------------------------------

    /// A committed state of `job`: its observers publish it at the end of
    /// the unit and waiters of its run settle when it is terminal.
    ///
    /// A waiter settles only from the run it attached to, so it never
    /// resolves from another attempt. No waiter can see its run replaced:
    /// `loadWait` attaches only to an active run (a terminal one answers at
    /// once), and a run changes only when `loadRetry` restarts a *failed*
    /// run, whose failure settled every waiter of it in the unit that
    /// recorded it. The run fence therefore needs no separate superseded
    /// outcome.
    pub(super) fn load_changed(&mut self, job: &LoadJob) {
        let Some(tracked) = self.loads.tracked.get_mut(&job.id) else {
            return;
        };
        tracked.status = job.status();
        tracked.run = job.run;
        let terminal = !job.phase.active();
        debug_assert!(
            tracked.waiters.iter().all(|(_, run)| *run >= job.run),
            "a Load waiter outlived its run"
        );
        let mut settled = vec![];
        tracked.waiters.retain(|(request_id, run)| {
            if *run == job.run && terminal {
                settled.push(request_id.clone());
                false
            } else {
                true
            }
        });
        if tracked.observers.is_empty() && tracked.waiters.is_empty() {
            self.loads.tracked.remove(&job.id);
        }
        for request_id in settled {
            match (&job.phase, &job.error) {
                (LoadPhase::Complete, _) => self.complete(request_id, Ok(Value::Null)),
                (_, Some(error)) => self.fail(
                    request_id,
                    error.message.clone(),
                    json!({"code": error.code, "message": error.message}),
                ),
                (_, None) => self.fail(
                    request_id,
                    "load failed",
                    json!({"code": "load.failed", "message": "load failed"}),
                ),
            }
        }
    }
    /// A stored status as a handle sees it: a pending job is `loading` while
    /// its page is in flight and `waiting` while it cannot go out.
    fn project(&self, mut status: LoadStatus) -> LoadStatus {
        if status.phase == LoadPhase::Pending {
            status.phase = if self.loads.worker.in_flight(&status.id) {
                LoadPhase::Loading
            } else if !self.load_online()
                || self.loads.worker.backing_off(&status.id, self.loads.clock)
            {
                LoadPhase::Waiting
            } else {
                LoadPhase::Pending
            };
        }
        status
    }
    fn load_status_json(&self, status: LoadStatus) -> Value {
        serde_json::to_value(self.project(status)).unwrap_or(Value::Null)
    }
    /// Publish every handle's snapshot that differs from its last one.
    /// Memory only.
    pub(super) fn publish_loads(&mut self) {
        let mut changed = vec![];
        for (id, tracked) in &self.loads.tracked {
            let snapshot =
                json!({"kind": "load", "status": self.load_status_json(tracked.status.clone())});
            for (observer, published) in &tracked.observers {
                if published.as_ref() != Some(&snapshot) {
                    changed.push((id.clone(), observer.clone(), snapshot.clone()));
                }
            }
        }
        for (id, observer_id, snapshot) in changed {
            if let Some(published) = self
                .loads
                .tracked
                .get_mut(&id)
                .and_then(|t| t.observers.get_mut(&observer_id))
            {
                *published = Some(snapshot.clone());
            }
            self.events.push(Event::ObserverChanged {
                observer_id,
                snapshot,
            });
        }
    }
    /// End every handle and waiter: waiters fail with `code`, observers
    /// publish a terminal snapshot carrying it. Durable jobs stay.
    fn end_load_handles(&mut self, code: &str) {
        for (_, tracked) in std::mem::take(&mut self.loads.tracked) {
            for (request_id, _) in tracked.waiters {
                self.fail(request_id, code, json!({ "code": code }));
            }
            let status = self.load_status_json(tracked.status.clone());
            for observer_id in tracked.observers.into_keys() {
                self.events.push(Event::ObserverChanged {
                    observer_id,
                    snapshot: json!({"kind": "load", "status": status, "closed": true, "code": code}),
                });
            }
        }
        self.loads.observers.clear();
    }

    // --- Connection controls, rebuild and close ------------------------------

    /// Pause or stop: every unanswered batch is abandoned without a failure
    /// and the backoff timer is dropped. Answers already received still
    /// apply.
    pub(super) fn abandon_loads(&mut self) {
        for batch in self.loads.worker.unanswered_batches() {
            self.abandon_load_batch(batch);
        }
        if let Some((timer, _)) = self.loads.timer.take() {
            self.cancel_effect(&timer);
        }
    }
    /// The replica was replaced: nothing in flight or observed belongs to
    /// the fresh ledger.
    pub(super) fn rebuilt_loads(&mut self) {
        let flights: Vec<u64> = self.loads.flights.keys().copied().collect();
        for batch in flights {
            self.retire_load_flight(batch);
        }
        if let Some((timer, _)) = self.loads.timer.take() {
            self.cancel_effect(&timer);
        }
        self.loads.worker.reset();
        self.end_load_handles(SCHEMA_CHANGED);
    }
    /// Close: effects were already cancelled; waiters and observers end.
    pub(super) fn close_loads(&mut self) {
        self.loads.flights.clear();
        self.loads.timer = None;
        self.loads.worker.reset();
        self.end_load_handles(CLIENT_CLOSED);
    }
}
