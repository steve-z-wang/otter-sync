//! The Downlink worker: the long-lived owner of inbound delivery. It holds the
//! bounded inbound queue, the durable-cursor policy, the catch-up requests and
//! the lane's schedule; the host only carries sockets, HTTP, timers and
//! credential refresh. A callback enqueues what arrived and wakes the loop; the
//! loop pumps, and only a pump commits
//! ([Downlink worker](../../../docs/engineering/architecture/client/connection/controller/downlink-worker.md)).
use crate::bootstrap_ledger::LedgerIssue;
use crate::*;
use std::collections::{BTreeSet, VecDeque};

/// What the host tells the worker. `now` and `entropy` travel beside the event
/// ([`DownlinkWorker::handle`]). `epoch` names the socket session an event
/// belongs to and `request` the catch-up it answers, so whatever an abandoned
/// socket or request still delivers is ignored.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(tag = "event", rename_all = "camelCase")]
pub enum DownlinkEvent {
    /// The lane starts; the first session begins on the next `next`.
    Start,
    /// The lane stops for good: the session ends and nothing is scheduled.
    Stop,
    /// The session ends without backoff; nothing runs until `resume`.
    Pause,
    Resume,
    /// Something changed that may need work: a subscription or local commit.
    Wake,
    /// Pump: consume what is queued, commit at most one page, and answer with
    /// what to do next. The only event that touches the database.
    Next,
    /// A frame arrived on the socket of this epoch: the acknowledgement or a page.
    Message {
        epoch: u64,
        body: String,
    },
    /// The socket of this epoch closed. The host has already reported the error
    /// and refreshed credentials if it chose to.
    Closed {
        epoch: u64,
    },
    /// The host's own frame buffer for this epoch overflowed and frames were
    /// dropped before they reached the worker.
    Overflow {
        epoch: u64,
    },
    /// The answer to the `request` action this id names.
    Response {
        request: u64,
        body: String,
    },
    /// The request this id names failed; `reason` is what the host reported and
    /// `status` the HTTP status it carried, when it had one. A status in the
    /// 4xx range is a refusal the server decided, not a transport failure.
    Failed {
        request: u64,
        #[serde(default)]
        reason: Option<String>,
        #[serde(default)]
        status: Option<u16>,
    },
}

/// What the host does next, in order.
#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum DownlinkAction {
    /// Open the socket and send `subscribe` once it is open. Frames it delivers
    /// are `message` events of this epoch; its end is `closed`.
    Open {
        epoch: u64,
        subscribe: String,
    },
    /// Close the socket of this epoch and abandon its request, if any. A
    /// `reason` is a protocol violation the host reports as an error.
    Close {
        epoch: u64,
        reason: Option<String>,
    },
    /// `POST /sync/pull` with `body`; its answer is a `response` event of this
    /// id, a failure is `failed`. An ordinary catch-up carries every subscribed
    /// channel and belongs to the open session, so the session's cancellation
    /// abandons it and its failure ends the session. A `bootstrap` request is
    /// one Scope's historical page on the same route: it belongs to the lane,
    /// not to a socket, so it outlives the session, its failure ends none, and
    /// only `pause`, `reset` and `close` abandon it.
    Request {
        request: u64,
        body: String,
        bootstrap: bool,
    },
    /// A bootstrap run changed and the change is committed: the phase, the
    /// historical progress, the fixed barrier and the stored failure of one
    /// registration. The SDKs publish it as the subscription's load status; no
    /// delivery decision depends on it
    /// ([#151](https://github.com/zanminwang/axton/issues/151)).
    Bootstrap(BootstrapState),
    Reconciliation(BootstrapState),
    /// A page applied and may have settled a batch: wake the push lane.
    Wake {
        lane: &'static str,
    },
    /// What the last page could not apply; the host hands it to the application.
    Report {
        reports: Vec<Report>,
    },
    /// A commit landed for these Scopes: their cursors moved.
    Changed {
        scopes: Vec<String>,
    },
    /// The handshake of the open session covered these Scopes: delivery for
    /// them is established, whether or not anything was committed for them.
    /// The SDKs turn it into the `live` connection status; no sync decision
    /// depends on it.
    Acknowledged {
        scopes: Vec<String>,
    },
    /// A stored Bootstrap row of `channel` cannot be decoded, so the schedule
    /// and the barrier settlement skip it: the host hands `message`, a bounded
    /// reason, to the application's error handler. The row is kept as stored
    /// and a named read of it still fails. Announced once per defect for the
    /// worker's lifetime - a changed defect, or one that returns after a
    /// repair or a removal, is announced again - and it is neither a committed
    /// `bootstrap` transition nor a record `report`
    /// ([#163](https://github.com/zanminwang/axton/issues/163)).
    LedgerIssue {
        channel: String,
        message: String,
    },
    /// Nothing to do for `millis`; then pump again.
    Wait {
        millis: u64,
    },
    /// The replica under the lane was rebuilt: abandon the socket and every
    /// request - ordinary or historical - the host still holds for it, and
    /// forget their state, without reporting anything. Always first in the
    /// answer of the first pump after the rebuild, and announced once. It
    /// replaces a `close` for the old session, which could otherwise reach a
    /// socket opened after it; what the abandoned I/O still delivers names an
    /// epoch or request id the worker never issues again, so it is ignored
    /// ([#162](https://github.com/zanminwang/axton/issues/162)).
    Reset,
}

/// Inbound work that a full page queue may never drop: the handshake, an
/// overflow to recover from, and the answer to the request in flight. Each one
/// needs the database, so the pump consumes it, never the enqueue.
#[derive(Clone)]
enum Control {
    /// The acknowledgement, already confirmed against the subscribe frame.
    Acknowledged(SubscriptionAck),
    /// Frames were lost: recover every channel from its durable cursor.
    Overflow,
    /// The body the request in flight answered with.
    Response(String),
}

/// One catch-up in flight: the id the host correlates its answer by and the
/// request that answer must match.
#[derive(Clone)]
struct Pending {
    id: u64,
    request: PullRequest,
}

/// The one historical page request in flight, across every Scope: the id the
/// host correlates its answer by, the registration and run it belongs to, and
/// the request that answer must be a page of. It names no socket epoch - a load
/// belongs to the client, not to a session - so replacing the socket neither
/// cancels nor restarts it, and the answer is validated against what is
/// committed when it arrives.
#[derive(Clone)]
struct PendingBootstrap {
    reconciliation: bool,
    id: u64,
    subscription_id: u64,
    run: u64,
    request: BootstrapRequest,
}

/// An admitted page is owned independently of whatever socket/request is
/// current when its application callback completes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct StoreToken {
    serial: u64,
    generation: u64,
    request: Option<u64>,
    epoch: Option<u64>,
    path: &'static str,
}
impl StoreToken {
    pub(crate) fn path(self) -> &'static str {
        self.path
    }
}
enum StoreSource {
    Live(ChannelPullPage),
    Catchup { request: u64, continues: bool },
    Bootstrap(PendingBootstrap),
}
struct PendingStore {
    token: StoreToken,
    source: StoreSource,
    delivery: StoreDelivery,
}
pub(crate) struct RuntimeDownlinkPump {
    pub(crate) actions: Vec<DownlinkAction>,
    pub(crate) store: Option<(StoreToken, StoreDelivery)>,
}
struct PendingBootstrapFailure {
    pending: PendingBootstrap,
    error: BootstrapError,
}

/// What the host reported about the historical request in flight, waiting for
/// the pump that may commit it. It is held apart from the session's control
/// queue, which an ended session discards: an answer must survive the socket.
#[derive(Clone)]
enum Loaded {
    /// The body the request answered with.
    Page(String),
    /// The request did not answer. `status` is the HTTP status the host had.
    Failed {
        status: Option<u16>,
        reason: Option<String>,
    },
}

/// The historical schedule: whose turn it is and when the next page may be
/// asked for. It is the lane's own retry policy applied to one work class, so
/// no page is ever in flight twice and no failure is retried in a tight loop.
#[derive(Default)]
struct Loading {
    /// Whether anything may have become schedulable since the last enumeration:
    /// the lane started, a wake arrived - every commit wakes it - or a page
    /// committed. Without it every pump would query the ledger for nothing.
    dirty: bool,
    /// The Scope whose page was asked for last: the rotation's position.
    rotation: Option<String>,
    /// Transport failures in a row, and the time before which none is retried.
    /// There is no overall timeout: waiting for connectivity is not a failure.
    attempt: u32,
    due: u64,
}
impl Loading {
    /// A commit may have made work schedulable.
    fn wake(&mut self) {
        self.dirty = true;
    }
    /// The lane started or resumed: nothing is deferred any more.
    fn restart(&mut self, now: u64) {
        *self = Self {
            dirty: true,
            rotation: self.rotation.take(),
            attempt: 0,
            due: now,
        };
    }
    /// A transport failure: hold the next attempt back, keeping the run.
    fn defer(&mut self, now: u64, entropy: u64) {
        let delay = ConnectionDriver::backoff(self.attempt, entropy);
        self.attempt = self.attempt.saturating_add(1);
        self.due = now.saturating_add(delay);
        self.dirty = true;
    }
    /// The request answered: the transport works, and the ledger may have more.
    fn answered(&mut self, now: u64) {
        self.attempt = 0;
        self.due = now;
        self.dirty = true;
    }
}

/// Which rows a ledger scan read: every active one, or only the candidates it
/// was asked about.
#[derive(Clone, Copy, PartialEq)]
enum Scan {
    Complete,
    Candidates,
}

/// Streamed page frames held in the queue. Beyond this the queue is discarded
/// whole and every channel recovers from the durable cursor: the server log is
/// the durable queue, the cursor the pointer into it. Control work is queued
/// apart from it and is never dropped for this bound.
pub const QUEUED_FRAMES: usize = 64;

/// The Downlink worker: one lane, one socket session at a time.
#[derive(Default)]
pub struct DownlinkWorker {
    /// The lane's schedule: when to open a session, when to retry, pause, stop.
    driver: ConnectionDriver,
    /// The socket session it directs; it owns no queue and no client.
    session: LiveSession,
    /// Control work in arrival order, never dropped for the page bound.
    control: VecDeque<Control>,
    /// Streamed pages not yet applied, in arrival order. The front is applied
    /// when every channel it names connects to its cursor; a page with a gap
    /// stays until a pull connects it or covers it.
    pages: VecDeque<ChannelPullPage>,
    /// The one ordinary catch-up in flight.
    active: Option<Pending>,
    /// The one historical page request in flight, across every Scope. It is a
    /// second slot, not a second queue: the ordinary cursor path never sees it.
    bootstrap: Option<PendingBootstrap>,
    /// What the host reported about that request, for the next pump to apply.
    loaded: Option<Loaded>,
    /// Whose turn the next historical page is, and when it may be asked for.
    loading: Loading,
    /// The lane started and has not re-evaluated the persisted barriers yet.
    reopened: bool,
    /// Another pull is needed once the one in flight ends (an overflow while
    /// pulling: the lost frames may lie beyond the answer).
    again: bool,
    /// Allocates the ids the host correlates catch-up answers by. Like the
    /// session's epoch it only grows for the worker's lifetime, a rebuild
    /// included, so an answer to an abandoned request never matches a new one.
    requests: u64,
    /// A session an enqueued event ended; the next pump tells the host to close
    /// its socket.
    closing: Option<(u64, Option<String>)>,
    /// The subscription identity of every Scope the open session subscribed,
    /// snapshotted with the generation when it began. It fences the boundaries
    /// its acknowledgement establishes: a Scope that has been unsubscribed, or
    /// recreated, since is another subscription and takes nothing from it.
    expected: BTreeMap<String, u64>,
    /// The replica was rebuilt since the last pump: the next one tells the
    /// host to abandon the old replica's I/O before anything else.
    reset: bool,
    /// The ledger issue last announced for each channel, by fingerprint: a
    /// scan that finds the same defect again announces nothing
    /// ([`DownlinkWorker::ledger`]).
    reported: BTreeMap<String, String>,
    /// Actions decided by a pump that later failed. The host saw none of them,
    /// but the worker (and possibly SQLite) already advanced. Return them
    /// before making another fallible decision.
    pending: Vec<DownlinkAction>,
    /// Scopes whose delivery committed but whose barrier scan failed. Their
    /// Changed action is already in the outbox; settlement still needs work.
    barrier_retry: BTreeSet<String>,
    store_serial: u64,
    pending_store: Option<PendingStore>,
    yielded_store: Option<(StoreToken, StoreDelivery)>,
    bootstrap_failure: Option<PendingBootstrapFailure>,
    reconciliation_failed: bool,
}

/// Whether an HTTP status is a refusal the server decided, which no retry can
/// clear: a deterministic request or read-contract failure. Authentication
/// (401), a timeout (408) and rate limiting (429) are transport conditions the
/// lane retries instead, as is every server error.
fn refused(status: u16) -> bool {
    (400..500).contains(&status) && !matches!(status, 401 | 408 | 429)
}

/// Collect one page's outcome into the actions: the push lane wakes and the
/// Scopes it moved are announced when it applied, its reports when it has any.
fn settle(progress: &DownlinkProgress, actions: &mut Vec<DownlinkAction>) {
    if progress.disposition == "applied" {
        actions.push(DownlinkAction::Wake { lane: "push" });
        actions.push(DownlinkAction::Changed {
            scopes: progress.report.cursors.keys().cloned().collect(),
        });
    }
    if !progress.report.reports.is_empty() {
        actions.push(DownlinkAction::Report {
            reports: progress.report.reports.clone(),
        });
    }
}

impl DownlinkWorker {
    pub(crate) fn store_committed(&mut self, token: StoreToken, result: StoreResult) {
        let Some(pending) = self.pending_store.take() else {
            return;
        };
        if pending.token != token {
            self.pending_store = Some(pending);
            return;
        }
        match (pending.source, result) {
            (StoreSource::Live(page), StoreResult::Page(report)) => {
                if self.pages.front() == Some(&page) {
                    self.pages.pop_front();
                }
                self.barrier_retry.extend(report.cursors.keys().cloned());
                settle(
                    &DownlinkProgress {
                        disposition: "applied",
                        gaps: vec![],
                        continues: vec![],
                        report,
                    },
                    &mut self.pending,
                );
            }
            (StoreSource::Catchup { request, continues }, StoreResult::Page(report)) => {
                if self
                    .active
                    .as_ref()
                    .is_some_and(|active| active.id == request)
                {
                    self.active = None;
                    if continues || self.again {
                        self.again = true;
                    }
                }
                self.barrier_retry.extend(report.cursors.keys().cloned());
                settle(
                    &DownlinkProgress {
                        disposition: "applied",
                        gaps: vec![],
                        continues: vec![],
                        report,
                    },
                    &mut self.pending,
                );
            }
            (StoreSource::Bootstrap(pending), StoreResult::Bootstrap(applied)) => {
                if self
                    .bootstrap
                    .as_ref()
                    .is_some_and(|current| current.id == pending.id)
                {
                    self.bootstrap = None;
                    self.loaded = None;
                }
                if let Some(report) = applied.report().filter(|r| !r.reports.is_empty()) {
                    self.pending.push(DownlinkAction::Report {
                        reports: report.reports.clone(),
                    });
                }
                if let Some(state) = applied.state() {
                    self.reconciliation_failed |=
                        pending.reconciliation && state.state == BootstrapPhase::Failed;
                    self.pending.push(if pending.reconciliation {
                        DownlinkAction::Reconciliation(state.clone())
                    } else {
                        DownlinkAction::Bootstrap(state.clone())
                    });
                }
            }
            _ => unreachable!("downlink store owner/result mismatch"),
        }
    }

    pub(crate) fn store_failed<S: ClientStore>(
        &mut self,
        client: &mut Client<S>,
        token: StoreToken,
        error: String,
        hook_failed: bool,
        now: u64,
        entropy: u64,
    ) -> Result<()> {
        let Some(pending) = self.pending_store.take() else {
            return Ok(());
        };
        if pending.token != token {
            self.pending_store = Some(pending);
            return Ok(());
        }
        let pending_store_delivery = pending.delivery;
        match pending.source {
            StoreSource::Live(_) | StoreSource::Catchup { .. } => {
                self.fail(client, None, now, entropy);
                Ok(())
            }
            StoreSource::Bootstrap(pending) => {
                if !hook_failed {
                    let (StoreDelivery::ChannelBootstrap { page, .. }
                    | StoreDelivery::ChannelReconciliation { page, .. }) = pending_store_delivery
                    else {
                        unreachable!()
                    };
                    let body = String::from_utf8(page.encode()?)
                        .map_err(|_| invalid("saved bootstrap page is not UTF-8"))?;
                    self.bootstrap = Some(pending);
                    self.loaded = Some(Loaded::Page(body));
                    self.loading.defer(now, entropy);
                    return Err(invalid(error));
                }
                self.bootstrap_failure = Some(PendingBootstrapFailure {
                    pending,
                    error: BootstrapError::new("store_hook_failed", error, vec![]),
                });
                self.persist_bootstrap_failure(client)
            }
        }
    }

    fn persist_bootstrap_failure<S: ClientStore>(&mut self, client: &mut Client<S>) -> Result<()> {
        let Some(failure) = &self.bootstrap_failure else {
            return Ok(());
        };
        let scope = &failure.pending.request.channel;
        let state = client.fail_history_state(
            failure.pending.reconciliation,
            scope,
            failure.pending.subscription_id,
            failure.pending.run,
            failure.error.clone(),
        )?;
        let reconciliation = failure.pending.reconciliation;
        self.bootstrap_failure = None;
        if let Some(state) = state {
            self.reconciliation_failed |= reconciliation;
            self.pending.push(if reconciliation {
                DownlinkAction::Reconciliation(state)
            } else {
                DownlinkAction::Bootstrap(state)
            });
        }
        Ok(())
    }

    pub(crate) fn next_runtime<S: ClientStore>(
        &mut self,
        client: &mut Client<S>,
        now: u64,
        entropy: u64,
        hooks: &BTreeSet<String>,
        hooks_active: bool,
    ) -> Result<RuntimeDownlinkPump> {
        let actions = self.pump_with_hooks(client, now, entropy, hooks_active.then_some(hooks))?;
        Ok(RuntimeDownlinkPump {
            actions,
            store: self.yielded_store.take(),
        })
    }

    fn wants_channel_hook(changes: &[ChannelChange], hooks: Option<&BTreeSet<String>>) -> bool {
        hooks.is_some_and(|hooks| {
            changes
                .iter()
                .filter_map(ChannelChange::record)
                .any(|record| hooks.contains(&record.model))
        })
    }
    fn yield_store<S: ClientStore>(
        &mut self,
        client: &Client<S>,
        delivery: StoreDelivery,
        source: StoreSource,
    ) -> Result<()> {
        self.store_serial = allocate(self.store_serial, "downlink store token")?;
        let token = StoreToken {
            serial: self.store_serial,
            generation: client.subscription_generation(),
            request: match &source {
                StoreSource::Catchup { request, .. } => Some(*request),
                StoreSource::Bootstrap(pending) => Some(pending.id),
                StoreSource::Live(_) => None,
            },
            epoch: self.session.active_epoch(),
            path: match &source {
                StoreSource::Live(_) => "live",
                StoreSource::Catchup { .. } => "catchUp",
                StoreSource::Bootstrap(_) => "bootstrap",
            },
        };
        self.pending_store = Some(PendingStore {
            token,
            source,
            delivery: delivery.clone(),
        });
        self.yielded_store = Some((token, delivery));
        Ok(())
    }
    /// One event. Everything but [`DownlinkEvent::Next`] is enqueued into typed
    /// state and answers with no actions; `next` is the pump.
    pub fn handle<S: ClientStore>(
        &mut self,
        client: &mut Client<S>,
        event: DownlinkEvent,
        now: u64,
        entropy: u64,
    ) -> Result<Vec<DownlinkAction>> {
        match event {
            DownlinkEvent::Next => self.pump_with_hooks(client, now, entropy, None),
            other => {
                self.enqueue(client, other, now, entropy);
                Ok(vec![])
            }
        }
    }

    /// Take one event: lane controls reach the schedule at once, I/O reaches the
    /// queues. No database work, no page application, and nothing the pump must
    /// see is dropped; the client is read only for the in-memory subscription
    /// generation, which decides whether a failure deserves backoff.
    fn enqueue<S: ClientStore>(
        &mut self,
        client: &mut Client<S>,
        event: DownlinkEvent,
        now: u64,
        entropy: u64,
    ) {
        match event {
            // Handled by `handle`; a pump is not queued.
            DownlinkEvent::Next => {}
            DownlinkEvent::Start => {
                self.discard_pending_io(false);
                self.pending
                    .retain(|action| !matches!(action, DownlinkAction::LedgerIssue { .. }));
                // A lane that replaced a closed one starts clean: the old
                // host abandoned its socket already, so nothing of a leftover
                // session is queued or announced to this one.
                self.end(None);
                self.closing = None;
                self.driver.start(now);
                // Persisted loads resume without another call from the
                // frontend, and every barrier is re-evaluated before any I/O.
                self.loading.restart(now);
                self.reopened = true;
                // A new connection has its own error callback: to the
                // application it is a reopen, so an unchanged defect is
                // announced to it again. Only an explicit connect starts a
                // lane, so this cannot flood.
                self.reported.clear();
            }
            DownlinkEvent::Stop => {
                self.discard_pending_io(true);
                self.end(None);
                self.driver.stop();
                // The lane is gone for good: its request is abandoned and its
                // answer belongs to nobody. The durable task is untouched and
                // the next start picks it up.
                self.bootstrap = None;
                self.loaded = None;
                self.loading = Loading::default();
                self.reopened = false;
            }
            DownlinkEvent::Pause => {
                self.discard_pending_io(true);
                if self.session.open() {
                    self.end(None);
                    self.driver.complete(true, now, 0);
                }
                self.driver.pause();
            }
            DownlinkEvent::Resume => {
                self.driver.resume(now);
                self.loading.restart(now);
            }
            DownlinkEvent::Wake => {
                self.driver.wake();
                self.loading.wake();
            }
            DownlinkEvent::Message { epoch, body } => {
                if self.session.current(epoch) {
                    self.frame(client, &body, now, entropy);
                }
            }
            DownlinkEvent::Closed { epoch } => {
                if self.session.current(epoch) {
                    self.fail(client, None, now, entropy);
                }
            }
            DownlinkEvent::Overflow { epoch } => {
                // Before the handshake there is no stream to have lost frames of.
                if self.session.current(epoch) && self.session.acknowledged() {
                    self.overflowed();
                }
            }
            DownlinkEvent::Response { request, body } => {
                if self.loads(request) {
                    self.loaded = Some(Loaded::Page(body));
                } else if self.answers(request) {
                    self.control.push_back(Control::Response(body));
                }
            }
            DownlinkEvent::Failed {
                request,
                reason,
                status,
            } => {
                // A historical page that failed ends no session: it is retried
                // on its own schedule, or refused if the server decided so.
                if self.loads(request) {
                    self.loaded = Some(Loaded::Failed { status, reason });
                } else if self.answers(request) {
                    // The host has reported the failure already; the session
                    // ends and the lane retries with backoff.
                    self.fail(client, None, now, entropy);
                }
            }
        }
    }

    /// The streamed page frames held for the pump, never more than
    /// [`QUEUED_FRAMES`].
    pub(crate) fn stream_acknowledged(&self) -> bool {
        self.session.acknowledged()
    }

    pub fn queued_frames(&self) -> usize {
        self.pages.len()
    }

    /// The replica under the lane was rebuilt in place
    /// ([`Client::rebuild`]): everything this worker held was measured against
    /// the old file, so every session, queue, request slot and retry is
    /// dropped, and the host is told to abandon its I/O by the first pump
    /// ([`DownlinkAction::Reset`]). The application's intent survives - a
    /// running lane opens a session for the carried Scopes on that pump with
    /// no further `start`, a paused one waits for `resume`, a stopped one for
    /// `start` - and so do the epoch and request id allocators, the fence
    /// against whatever the abandoned socket and requests still deliver. The
    /// fresh replica's subscription generation cannot fence them: it restarts
    /// and may equal the old one. No database work: the binding calls this
    /// right after a successful rebuild, and never after a failed one
    /// ([#162](https://github.com/zanminwang/axton/issues/162)).
    pub fn reset_for_rebuild(&mut self) {
        self.discard_pending_io(false);
        // Old replica states and statuses cannot be replayed against a fresh
        // file: subscription identity and run counters may be reused there.
        self.pending.clear();
        self.barrier_retry.clear();
        self.pending_store = None;
        self.yielded_store = None;
        self.bootstrap_failure = None;
        // Closing without a `close` action: the host's reset abandons it.
        self.session.close();
        self.control.clear();
        self.pages.clear();
        self.active = None;
        self.bootstrap = None;
        self.loaded = None;
        self.again = false;
        self.closing = None;
        self.expected.clear();
        self.driver.restart();
        let running = self.driver.running();
        // A started lane re-evaluates the persisted barriers and the
        // historical schedule before any I/O, as `start` does; the rotation
        // and the backoff belonged to the old ledger.
        self.loading = Loading {
            dirty: running,
            ..Loading::default()
        };
        // Paused included: its first pump then re-evaluates the barriers - a
        // local read and at most a commit, never I/O - before `resume`.
        self.reopened = running;
        self.reset = true;
        // The fresh replica carries no row this map describes.
        self.reported.clear();
    }

    /// The next catch-up or historical request id: one id space, never reused.
    fn next_request(&mut self) -> Result<u64> {
        self.requests = allocate(self.requests, "downlink request id")?;
        Ok(self.requests)
    }

    /// Whether `request` is the catch-up in flight. An answer or failure of any
    /// other belongs to a session that is gone.
    fn answers(&self, request: u64) -> bool {
        self.active.as_ref().is_some_and(|p| p.id == request)
    }
    /// Whether `request` is the historical page in flight. The two slots share
    /// one id space, so one test decides which work an answer belongs to.
    fn loads(&self, request: u64) -> bool {
        self.bootstrap.as_ref().is_some_and(|p| p.id == request)
    }

    /// One frame of the current socket: the handshake is validated for order
    /// here and queued for the pump; a page joins the bounded queue.
    fn frame<S: ClientStore>(
        &mut self,
        client: &mut Client<S>,
        body: &str,
        now: u64,
        entropy: u64,
    ) {
        let decoded = match ChannelLiveMessage::decode(body.as_bytes()) {
            Ok(decoded) => decoded,
            Err(e) => return self.fail(client, Some(e.to_string()), now, entropy),
        };
        match decoded {
            ChannelLiveMessage::Acknowledged(ack) => {
                if let Err(e) = self.session.acknowledge(&ack) {
                    return self.fail(client, Some(e.to_string()), now, entropy);
                }
                self.control.push_back(Control::Acknowledged(ack));
            }
            ChannelLiveMessage::Page(page) => {
                if let Err(e) = self.session.streamed() {
                    return self.fail(client, Some(e.to_string()), now, entropy);
                }
                if self.pages.len() >= QUEUED_FRAMES {
                    return self.overflowed();
                }
                self.pages.push_back(page);
            }
        }
    }

    /// Frames were lost - the host's buffer or this queue overflowed - and which
    /// channels they belonged to is unknown: the queue is discarded and every
    /// channel recovers from its durable cursor. Redundant overflows coalesce
    /// into the one recovery still to run.
    fn overflowed(&mut self) {
        self.pages.clear();
        if !self.control.iter().any(|c| matches!(c, Control::Overflow)) {
            self.control.push_back(Control::Overflow);
        }
    }

    /// End the session: the host closes its socket and abandons its request, and
    /// whatever it queued is dropped, since the next session recovers from the
    /// durable cursors.
    fn end(&mut self, reason: Option<String>) {
        // A failed pump may have held actions for this session that the host
        // never saw. Its close supersedes those session-bound effects and
        // status, while committed notifications and historical work survive.
        self.pending.retain(|action| {
            !matches!(
                action,
                DownlinkAction::Open { .. }
                    | DownlinkAction::Request {
                        bootstrap: false,
                        ..
                    }
                    | DownlinkAction::Acknowledged { .. }
                    | DownlinkAction::Wait { .. }
            )
        });
        if let Some(epoch) = self.session.close() {
            self.closing = Some((epoch, reason));
        }
        self.control.clear();
        self.pages.clear();
        self.active = None;
        self.again = false;
        self.expected.clear();
    }

    /// A protocol violation or a transport failure: the session ends and the
    /// lane retries with backoff - unless a committed subscription change had
    /// already invalidated it, in which case the change, not the transport,
    /// ended it: the lane opens the next session at once and counts no failed
    /// attempt. The server or the network can report the socket closed before
    /// the wake of the commit that invalidated it is pumped, so the generation,
    /// not the arrival order, decides: a session whose subscribed set a commit
    /// has replaced reconnects without backoff however its socket ended.
    fn fail<S: ClientStore>(
        &mut self,
        client: &mut Client<S>,
        reason: Option<String>,
        now: u64,
        entropy: u64,
    ) {
        if self.stale(client) {
            return self.invalidate(now);
        }
        self.end(reason);
        self.driver.complete(false, now, entropy);
    }

    /// Whether the open session subscribed under a subscription set that a
    /// commit has since replaced.
    fn stale<S: ClientStore>(&self, client: &mut Client<S>) -> bool {
        self.session
            .generation()
            .is_some_and(|generation| generation != client.subscription_generation())
    }

    /// The subscribed set the session negotiated is gone: end it and open the
    /// next one without backoff.
    fn invalidate(&mut self, now: u64) {
        self.end(None);
        self.driver.complete(true, now, 0);
        self.driver.wake();
    }

    /// Tell the host to close a session the worker ended, in order.
    fn flush(&mut self, actions: &mut Vec<DownlinkAction>) {
        if let Some((epoch, reason)) = self.closing.take() {
            actions.push(DownlinkAction::Close { epoch, reason });
        }
    }

    /// A control may supersede an I/O decision the host has not received.
    /// Keep committed notifications, which describe work already in SQLite.
    /// Stop and pause still owe a close for a socket the host did receive.
    fn discard_pending_io(&mut self, keep_close: bool) {
        let unopened: Vec<u64> = self
            .pending
            .iter()
            .filter_map(|action| match action {
                DownlinkAction::Open { epoch, .. } => Some(*epoch),
                _ => None,
            })
            .collect();
        let unsent: Vec<u64> = self
            .pending
            .iter()
            .filter_map(|action| match action {
                DownlinkAction::Request { request, .. } => Some(*request),
                _ => None,
            })
            .collect();
        if self.active.as_ref().is_some_and(|p| unsent.contains(&p.id)) {
            self.active = None;
            self.again = false;
        }
        if self
            .bootstrap
            .as_ref()
            .is_some_and(|p| unsent.contains(&p.id))
        {
            self.bootstrap = None;
            self.loaded = None;
            self.loading.wake();
        }
        if self
            .closing
            .as_ref()
            .is_some_and(|(epoch, _)| unopened.contains(epoch))
        {
            self.closing = None;
        }
        self.pending.retain(|action| match action {
            DownlinkAction::Bootstrap(_)
            | DownlinkAction::Reconciliation(_)
            | DownlinkAction::Wake { .. }
            | DownlinkAction::Report { .. }
            | DownlinkAction::Changed { .. }
            | DownlinkAction::LedgerIssue { .. }
            | DownlinkAction::Reset => true,
            DownlinkAction::Close { epoch, .. } => keep_close && !unopened.contains(epoch),
            _ => false,
        });
    }

    /// One bounded pump: control work first, then at most one page application,
    /// then the lane's next decision. One commit per call, so foreground work
    /// interleaves; the host pumps again while actions come back.
    fn pump_with_hooks<S: ClientStore>(
        &mut self,
        client: &mut Client<S>,
        now: u64,
        entropy: u64,
        hooks: Option<&BTreeSet<String>>,
    ) -> Result<Vec<DownlinkAction>> {
        self.persist_bootstrap_failure(client)?;
        if std::mem::take(&mut self.reconciliation_failed) {
            self.loading.defer(now, entropy);
        }
        if self.pending_store.is_some() {
            return Ok(vec![]);
        }
        if !self.pending.is_empty() {
            // A subscription may have changed since the failed pump. Fence
            // undelivered session I/O before the outbox reaches the host;
            // committed notifications and historical work still survive.
            if self.stale(client) {
                self.invalidate(now);
            }
            let mut actions = vec![];
            if std::mem::take(&mut self.reset) {
                actions.push(DownlinkAction::Reset);
            }
            self.flush(&mut actions);
            actions.append(&mut self.pending);
            return Ok(actions);
        }
        let mut actions = vec![];
        // The host abandons the old replica's I/O before it opens or requests
        // anything for the new one. A failed pump retains this action with
        // every other decision and returns them before another fallible step.
        let reset = std::mem::take(&mut self.reset);
        if reset {
            actions.push(DownlinkAction::Reset);
        }
        match self.advance(client, now, entropy, hooks, &mut actions) {
            Ok(()) => Ok(actions),
            Err(error) => {
                // No action reached the host. Keep every decided action and
                // the progressed worker state; do not replay a database commit
                // merely to recreate its notification.
                self.barrier_retry.extend(
                    actions
                        .iter()
                        .filter_map(|action| match action {
                            DownlinkAction::Changed { scopes } => Some(scopes.iter().cloned()),
                            _ => None,
                        })
                        .flatten(),
                );
                self.pending = actions;
                Err(error)
            }
        }
    }

    /// The pump's body, after the reset: everything it decides is collected
    /// into `actions`.
    fn advance<S: ClientStore>(
        &mut self,
        client: &mut Client<S>,
        now: u64,
        entropy: u64,
        hooks: Option<&BTreeSet<String>>,
        actions: &mut Vec<DownlinkAction>,
    ) -> Result<()> {
        self.flush(actions);
        // A committed subscribe or unsubscribe invalidates the session: the
        // lane starts over with the new channel set, without backoff.
        if self.stale(client) {
            self.invalidate(now);
            self.flush(actions);
        }
        // A lane that just started re-evaluates every persisted barrier before
        // it issues anything: a run whose delivery reached its barrier while
        // the client was closed completes without another request.
        if std::mem::take(&mut self.reopened) {
            match self.resume(client, actions) {
                Ok(true) => return Ok(()),
                Ok(false) => {}
                Err(error) => {
                    self.reopened = true;
                    return Err(error);
                }
            }
        }
        let committed = self.process(client, now, entropy, hooks, actions)?;
        if self.pending_store.is_some() {
            return Ok(());
        }
        self.flush(actions);
        self.barriers(client, actions)?;
        self.historical(client, now, entropy, committed, hooks, actions)?;
        if self.pending_store.is_some() {
            return Ok(());
        }
        // The two schedules are read together and answered with one sleep: the
        // load's next attempt is its own, so the socket's backoff must never
        // hold a page back ([#151](https://github.com/zanminwang/axton/issues/151)).
        let load = self.load_due(now);
        let mut socket = None;
        if !self.session.open() {
            match self.driver.next(now) {
                ConnectionAction::Sync => {
                    if let Err(error) = self.begin(client, now, actions) {
                        // `next` reserved the attempt before `begin` read the
                        // subscriptions. Release it for a bounded retry.
                        self.driver.complete(false, now, entropy);
                        return Err(error);
                    }
                }
                ConnectionAction::Wait { millis } => socket = Some(millis),
                ConnectionAction::Idle => {}
            }
        }
        self.rest(load, socket, actions);
        Ok(())
    }

    /// When the historical schedule next wants a pump, in millis from `now`:
    /// `None` when it wants none - a page is in flight, nothing became
    /// schedulable, or the lane is paused or stopped - and zero when it is due
    /// already, which is the one case that asks for no sleep at all.
    fn load_due(&self, now: u64) -> Option<u64> {
        if !self.driver.active() || self.bootstrap.is_some() || !self.loading.dirty {
            return None;
        }
        Some(self.loading.due.saturating_sub(now))
    }

    /// The lane's one sleep: the earlier of the two schedules, so neither work
    /// class waits on the other's. The socket's is answered whatever else this
    /// pump found, because with no session open nothing else will wake it; the
    /// load's alone is answered only when the pump gave the host nothing else
    /// to do, so a deferred page never delays work already queued. A schedule
    /// that is due now asks for no sleep: the host pumps again as soon as the
    /// actions come back.
    fn rest(&self, load: Option<u64>, socket: Option<u64>, actions: &mut Vec<DownlinkAction>) {
        let millis = match (load, socket) {
            (Some(load), Some(socket)) => load.min(socket),
            (None, Some(socket)) => socket,
            (Some(load), None) if actions.is_empty() => load,
            _ => return,
        };
        if millis > 0 || actions.is_empty() {
            actions.push(DownlinkAction::Wait { millis });
        }
    }

    /// Consume the queues: every control event, which a page that cannot apply
    /// yet never holds back, then streamed pages from the front until one
    /// commits. A page leaves the queue only by being applied or covered.
    /// `true` when a commit landed, so this pump holds no other.
    fn process<S: ClientStore>(
        &mut self,
        client: &mut Client<S>,
        now: u64,
        entropy: u64,
        hooks: Option<&BTreeSet<String>>,
        actions: &mut Vec<DownlinkAction>,
    ) -> Result<bool> {
        while self.session.open() {
            let Some(control) = self.control.pop_front() else {
                break;
            };
            let retry = control.clone();
            let result = match control {
                Control::Acknowledged(ack) => {
                    self.acknowledged(client, &ack, now, entropy, actions)
                }
                Control::Overflow => self.recover(client, actions).map(|()| false),
                Control::Response(body) => {
                    self.response(client, &body, now, entropy, hooks, actions)
                }
            };
            let committed = match result {
                Ok(committed) => committed,
                Err(error) => {
                    // An acknowledgement can be reconsidered from committed
                    // rows, and a failed page still holds its request slot.
                    // A response that committed before a later pull failure
                    // has no slot: its continuation is `again`, not replay.
                    if !matches!(retry, Control::Response(_)) || self.active.is_some() {
                        self.control.push_front(retry);
                    }
                    return Err(error);
                }
            };
            if committed {
                return Ok(true);
            }
        }
        if self.again && self.active.is_none() {
            self.pull(client, actions)?;
            self.again = false;
        }
        // Pages wait while a pull is in flight: its answer moves the cursors
        // they are measured against. A historical page is in neither queue and
        // holds nothing back.
        while self.session.open() && self.active.is_none() {
            let Some(front) = self.pages.front().cloned() else {
                break;
            };
            if Self::wants_channel_hook(&front.changes, hooks) {
                let admission = client.admit_channel_downlink(&front, None)?;
                if admission.disposition == "applied" {
                    self.yield_store(
                        client,
                        StoreDelivery::ChannelPage(front.clone()),
                        StoreSource::Live(front),
                    )?;
                    return Ok(true);
                }
            }
            let progress = client.receive_channel_downlink(front, None)?;
            settle(&progress, actions);
            if progress.disposition == "recover" {
                // The gap stays at the front until a pull connects or covers it.
                self.pull(client, actions)?;
                return Ok(false);
            }
            self.pages.pop_front();
            if progress.disposition == "applied" {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The lane started: complete every persisted run whose fixed barrier
    /// delivery has already reached. `true` when it committed, so the pump
    /// holds that one commit and the host pumps again for the session.
    fn resume<S: ClientStore>(
        &mut self,
        client: &mut Client<S>,
        actions: &mut Vec<DownlinkAction>,
    ) -> Result<bool> {
        let (waiting, issues) = client.bootstrap_barriers_scan()?;
        self.ledger(issues, Scan::Complete, actions);
        let (settled, issues) = client.settle_history_barriers_scan(&waiting)?;
        self.ledger(issues, Scan::Candidates, actions);
        let committed = !settled.is_empty();
        for (state, reconciliation) in settled {
            actions.push(if reconciliation {
                DownlinkAction::Reconciliation(state)
            } else {
                DownlinkAction::Bootstrap(state)
            });
        }
        Ok(committed)
    }

    /// Committed delivery progress may have reached a fixed barrier: complete
    /// every run of a Scope this pump moved. The Scopes are the ones the commit
    /// announced, so a barrier is settled by the transaction that reached it
    /// and by nothing else; a run still short of its barrier writes nothing
    /// ([`Client::settle_bootstrap_barriers`]).
    fn barriers<S: ClientStore>(
        &mut self,
        client: &mut Client<S>,
        actions: &mut Vec<DownlinkAction>,
    ) -> Result<()> {
        let moved: BTreeSet<String> = actions
            .iter()
            .filter_map(|action| match action {
                DownlinkAction::Changed { scopes } => Some(scopes.clone()),
                _ => None,
            })
            .flatten()
            .chain(self.barrier_retry.iter().cloned())
            .collect();
        if moved.is_empty() {
            return Ok(());
        }
        let moved: Vec<String> = moved.into_iter().collect();
        let (settled, issues) = match client.settle_history_barriers_scan(&moved) {
            Ok(result) => result,
            Err(error) => {
                self.barrier_retry.extend(moved);
                return Err(error);
            }
        };
        self.barrier_retry.clear();
        self.ledger(issues, Scan::Candidates, actions);
        for (state, reconciliation) in settled {
            actions.push(if reconciliation {
                DownlinkAction::Reconciliation(state)
            } else {
                DownlinkAction::Bootstrap(state)
            });
        }
        Ok(())
    }

    /// Announce the rows a ledger scan skipped because they cannot be decoded,
    /// each defect once: an issue whose fingerprint is the one last announced
    /// for its channel says nothing new, so no pump, wake or delivery that
    /// leaves the defect as it was repeats it. A complete scan saw every active
    /// row, so a channel it found healthy or absent is forgotten, and the same
    /// defect coming back after a repair or a removal is announced again. A
    /// candidate scan saw only the rows it was asked about and forgets nothing
    /// ([#163](https://github.com/zanminwang/axton/issues/163)).
    fn ledger(&mut self, issues: Vec<LedgerIssue>, scan: Scan, actions: &mut Vec<DownlinkAction>) {
        if scan == Scan::Complete {
            self.reported
                .retain(|channel, _| issues.iter().any(|issue| &issue.channel == channel));
        }
        for issue in issues {
            if self.reported.get(&issue.channel) == Some(&issue.fingerprint) {
                continue;
            }
            actions.push(DownlinkAction::LedgerIssue {
                channel: issue.channel.clone(),
                message: issue.detail,
            });
            self.reported.insert(issue.channel, issue.fingerprint);
        }
    }

    /// The historical work class: apply what the request in flight answered,
    /// then ask for the next page. Neither step touches the socket, the live
    /// cursors or the ordinary catch-up slot, and neither runs when this pump
    /// has already committed - one commit per pump, so foreground work and
    /// delivery interleave with a load.
    fn historical<S: ClientStore>(
        &mut self,
        client: &mut Client<S>,
        now: u64,
        entropy: u64,
        committed: bool,
        hooks: Option<&BTreeSet<String>>,
        actions: &mut Vec<DownlinkAction>,
    ) -> Result<()> {
        if committed || self.answered(client, now, entropy, hooks, actions)? {
            return Ok(());
        }
        self.schedule(client, now, actions)
    }

    /// Apply what the host reported about the historical request in flight;
    /// `true` when it committed. A page is refused unless it answers the
    /// request that asked for it, and every test the ledger makes is against
    /// what is committed now, so an answer that outlived its run writes
    /// nothing. A transport failure keeps the run and defers the same page; a
    /// refusal the server decided fails the run until an explicit retry.
    fn answered<S: ClientStore>(
        &mut self,
        client: &mut Client<S>,
        now: u64,
        entropy: u64,
        hooks: Option<&BTreeSet<String>>,
        actions: &mut Vec<DownlinkAction>,
    ) -> Result<bool> {
        // The slot is emptied only by an answer to the request it holds: taking
        // both at once would clear it on every pump.
        let Some(loaded) = self.loaded.take() else {
            return Ok(false);
        };
        let Some(pending) = self.bootstrap.take() else {
            return Ok(false);
        };
        let retry_loaded = loaded.clone();
        let retry_pending = pending.clone();
        let result = self.finish_answer(client, (now, entropy), hooks, actions, loaded, pending);
        if result.is_err() {
            // The store rejected this application, so the answer and the
            // request it names are still the next unit of local work.
            self.loaded = Some(retry_loaded);
            self.bootstrap = Some(retry_pending);
        }
        result
    }

    fn finish_answer<S: ClientStore>(
        &mut self,
        client: &mut Client<S>,
        timing: (u64, u64),
        hooks: Option<&BTreeSet<String>>,
        actions: &mut Vec<DownlinkAction>,
        loaded: Loaded,
        pending: PendingBootstrap,
    ) -> Result<bool> {
        let (now, entropy) = timing;
        let scope = pending.request.channel.clone();
        let body = match loaded {
            Loaded::Failed { status, reason } => {
                if !status.is_some_and(refused) {
                    // Offline or interrupted: the run is untouched and the same
                    // page is asked for again once the backoff has passed
                    // ([`DownlinkWorker::load_due`] carries it to the sleep).
                    self.loading.defer(now, entropy);
                    return Ok(false);
                }
                self.loading.answered(now);
                let detail = reason.map(|r| format!(": {r}")).unwrap_or_default();
                return self.refuse(
                    client,
                    &pending,
                    BootstrapError::new(
                        REQUEST_REJECTED,
                        format!(
                            "the bootstrap request for {scope} was refused with HTTP {}{detail}",
                            status.unwrap_or_default()
                        ),
                        vec![],
                    ),
                    actions,
                );
            }
            Loaded::Page(body) => body,
        };
        self.loading.answered(now);
        let page = match ChannelBootstrapPage::decode(body.as_bytes()) {
            Ok(page) if page.answers(&pending.request) => page,
            Ok(page) => {
                return self.refuse(
                    client,
                    &pending,
                    BootstrapError::new(
                        PROTOCOL_INVALID,
                        format!(
                            "a bootstrap page ({}, {}] of {} does not answer the request ({}, {}] of {scope}",
                            page.from, page.to, page.channel, pending.request.after,
                            pending.request.until
                        ),
                        vec![],
                    ),
                    actions,
                );
            }
            Err(e) => {
                return self.refuse(
                    client,
                    &pending,
                    BootstrapError::new(
                        PROTOCOL_INVALID,
                        format!("invalid bootstrap page for {scope}: {e}"),
                        vec![],
                    ),
                    actions,
                );
            }
        };
        if Self::wants_channel_hook(&page.changes, hooks) {
            self.yield_store(
                client,
                if pending.reconciliation {
                    StoreDelivery::ChannelReconciliation {
                        scope: scope.clone(),
                        subscription_id: pending.subscription_id,
                        run: pending.run,
                        expected_after: pending.request.after,
                        page,
                    }
                } else {
                    StoreDelivery::ChannelBootstrap {
                        scope: scope.clone(),
                        subscription_id: pending.subscription_id,
                        run: pending.run,
                        expected_after: pending.request.after,
                        page,
                    }
                },
                StoreSource::Bootstrap(pending),
            )?;
            return Ok(true);
        }
        let applied = client.apply_channel_history_page(
            pending.reconciliation,
            &scope,
            pending.subscription_id,
            pending.run,
            pending.request.after,
            &page,
        )?;
        // What the page could not apply is the application's, whether or not
        // the run survived it.
        if let Some(report) = applied.report().filter(|r| !r.reports.is_empty()) {
            actions.push(DownlinkAction::Report {
                reports: report.reports.clone(),
            });
        }
        let Some(state) = applied.state() else {
            return Ok(false);
        };
        self.reconciliation_failed |=
            pending.reconciliation && state.state == BootstrapPhase::Failed;
        actions.push(if pending.reconciliation {
            DownlinkAction::Reconciliation(state.clone())
        } else {
            DownlinkAction::Bootstrap(state.clone())
        });
        Ok(true)
    }

    /// Store a failure the ledger cannot see for itself and announce it;
    /// `false` when the answer named no run this client still holds, in which
    /// case nothing was written and nothing is announced.
    ///
    /// The registration is checked on the committed reader first, because
    /// naming one that is gone - unsubscribed, or replaced by another identity
    /// while the page was in flight - is how the ledger reports a closed
    /// subscription: with an error. An error here would leave the pump, discard
    /// the actions it had gathered and end the live session, and a stale answer
    /// must do none of that.
    fn refuse<S: ClientStore>(
        &mut self,
        client: &mut Client<S>,
        pending: &PendingBootstrap,
        error: BootstrapError,
        actions: &mut Vec<DownlinkAction>,
    ) -> Result<bool> {
        let scope = &pending.request.channel;
        let held = client
            .subscription_state(scope)?
            .is_some_and(|state| state.subscription_id == pending.subscription_id);
        if !held {
            return Ok(false);
        }
        let Some(state) = client.fail_history_state(
            pending.reconciliation,
            scope,
            pending.subscription_id,
            pending.run,
            error,
        )?
        else {
            return Ok(false);
        };
        self.reconciliation_failed |= pending.reconciliation;
        actions.push(if pending.reconciliation {
            DownlinkAction::Reconciliation(state)
        } else {
            DownlinkAction::Bootstrap(state)
        });
        Ok(true)
    }

    /// Ask for one historical page when none is in flight: the next run in the
    /// rotation, from the progress it committed, bounded by its own origin. The
    /// read that picks it is closed before the action leaves, so no transaction
    /// and no pump waits on the network.
    fn schedule<S: ClientStore>(
        &mut self,
        client: &mut Client<S>,
        now: u64,
        actions: &mut Vec<DownlinkAction>,
    ) -> Result<()> {
        // With no network configuration - a lane that never started, or a
        // paused one - there is nothing to issue a request to.
        if self.bootstrap.is_some() || !self.driver.active() || !self.loading.dirty {
            return Ok(());
        }
        // A deferred page stays deferred; the pump answers for the sleep.
        if self.loading.due > now {
            return Ok(());
        }
        client.retry_reconciliation_failures()?;
        let (task, issues) = client.bootstrap_schedule_scan(self.loading.rotation.as_deref())?;
        self.ledger(issues, Scan::Complete, actions);
        let Some(task) = task else {
            // Nothing is schedulable: a wake says when to look again.
            self.loading.dirty = false;
            return Ok(());
        };
        let request = task.request(client.declared_models());
        let body = String::from_utf8(task.encode_request(client.declared_models())?)
            .map_err(|_| invalid("a bootstrap request must be UTF-8"))?;
        let id = self.next_request()?;
        self.loading.rotation = Some(task.state.scope.clone());
        self.bootstrap = Some(PendingBootstrap {
            reconciliation: task.reconciliation,
            id,
            subscription_id: task.state.subscription_id,
            run: task.state.run,
            request,
        });
        actions.push(DownlinkAction::Request {
            request: id,
            body,
            bootstrap: true,
        });
        Ok(())
    }

    /// Snapshot the desired Scopes, their subscription identities and the
    /// generation; with no Scope the session ends successfully and the lane
    /// stays idle until a subscribe wakes it. A Scope still waiting for its
    /// first boundary belongs to the set the socket asks for.
    fn begin<S: ClientStore>(
        &mut self,
        client: &mut Client<S>,
        now: u64,
        actions: &mut Vec<DownlinkAction>,
    ) -> Result<()> {
        let desired = client.subscription_states()?;
        if desired.is_empty() {
            self.driver.complete(true, now, 0);
            return Ok(());
        }
        self.expected = desired
            .iter()
            .map(|s| (s.scope.clone(), s.subscription_id))
            .collect();
        let (epoch, subscribe) = self.session.begin(
            self.expected.keys().cloned().collect(),
            client.declared_models(),
            client.subscription_generation(),
        )?;
        actions.push(DownlinkAction::Open { epoch, subscribe });
        Ok(())
    }

    /// The handshake landed: one transaction commits the first delivery
    /// boundary of every subscription still waiting for one and leaves every
    /// initialized cursor where it is
    /// ([`Client::initialize_subscriptions`]). Status follows that commit and
    /// no queued page is applied before it, so the answer is `true` whenever a
    /// boundary landed and one pump still holds one commit. A channel behind
    /// its acknowledged head catches up over HTTP first; a head below a
    /// committed cursor is a server-state fault that ends the session.
    fn acknowledged<S: ClientStore>(
        &mut self,
        client: &mut Client<S>,
        ack: &SubscriptionAck,
        now: u64,
        entropy: u64,
        actions: &mut Vec<DownlinkAction>,
    ) -> Result<bool> {
        let initialization = client.initialize_subscriptions(&self.expected, &ack.cursors)?;
        if let Some(reason) = initialization.fault {
            self.fail(client, Some(reason), now, entropy);
            return Ok(false);
        }
        self.loading.wake();
        let committed = !initialization.initialized.is_empty();
        if committed {
            // A load registered before its subscription had an origin has
            // nothing to bound its interval, so the schedule passed it over.
            // The boundary this transaction committed is that bound: without
            // this wake the run would wait for some unrelated commit
            // ([#151](https://github.com/zanminwang/axton/issues/151)).
            self.loading.wake();
            actions.push(DownlinkAction::Changed {
                scopes: initialization.initialized,
            });
        }
        if !initialization.catch_up.is_empty() {
            self.pull(client, actions)?;
        }
        // Delivery is established for the acknowledged set, whether or not a
        // boundary was committed for any of it: the SDKs read it as `live`.
        let mut scopes = vec![];
        for scope in ack.cursors.keys() {
            if !client.reconciliation_pending(scope)? {
                scopes.push(scope.clone());
            }
        }
        if !scopes.is_empty() {
            actions.push(DownlinkAction::Acknowledged { scopes });
        }
        Ok(committed)
    }

    /// Recover every channel from its durable cursor after lost frames. A pull
    /// in flight keeps its progress and another follows it.
    fn recover<S: ClientStore>(
        &mut self,
        client: &mut Client<S>,
        actions: &mut Vec<DownlinkAction>,
    ) -> Result<()> {
        self.pages.clear();
        self.pull(client, actions)
    }

    /// Issue one pull for every initialized subscription when none is in
    /// flight; otherwise remember that another is needed.
    fn pull<S: ClientStore>(
        &mut self,
        client: &mut Client<S>,
        actions: &mut Vec<DownlinkAction>,
    ) -> Result<()> {
        if self.active.is_some() {
            self.again = true;
            return Ok(());
        }
        let Some(body) = client.downlink_request()? else {
            return Ok(());
        };
        let request = PullRequest::decode(body.as_bytes())?;
        let id = self.next_request()?;
        self.active = Some(Pending { id, request });
        actions.push(DownlinkAction::Request {
            request: id,
            body,
            bootstrap: false,
        });
        Ok(())
    }

    /// Apply the answer to the request in flight; `true` when it committed. The
    /// round goes on from the durable cursors while any channel continues.
    fn response<S: ClientStore>(
        &mut self,
        client: &mut Client<S>,
        body: &str,
        now: u64,
        entropy: u64,
        hooks: Option<&BTreeSet<String>>,
        actions: &mut Vec<DownlinkAction>,
    ) -> Result<bool> {
        if let Some(pending) = &self.active
            && let Ok(page) = ChannelPullPage::decode(body.as_bytes())
            && Self::wants_channel_hook(&page.changes, hooks)
        {
            let pending = pending.clone();
            let admission = match client.admit_channel_downlink(&page, Some(&pending.request)) {
                Ok(admission) => admission,
                Err(e) if e.to_string() == "response does not match pull request" => {
                    self.fail(client, Some(e.to_string()), now, entropy);
                    return Ok(false);
                }
                Err(e) => return Err(e),
            };
            if admission.disposition == "applied" {
                let continues = !admission.continues.is_empty();
                self.yield_store(
                    client,
                    StoreDelivery::ChannelPage(page),
                    StoreSource::Catchup {
                        request: pending.id,
                        continues,
                    },
                )?;
                return Ok(true);
            }
        }
        // The id was matched when the answer was enqueued; a second answer to
        // the same request finds nothing in flight and is ignored.
        let Some(pending) = self.active.take() else {
            return Ok(false);
        };
        let page = match ChannelPullPage::decode(body.as_bytes()) {
            Ok(page) => page,
            Err(e) => {
                self.fail(
                    client,
                    Some(format!("invalid pull response: {e}")),
                    now,
                    entropy,
                );
                return Ok(false);
            }
        };
        let progress = match client.receive_channel_downlink(page, Some(pending.request.clone())) {
            Ok(progress) => progress,
            Err(e) if e.to_string() == "response does not match pull request" => {
                self.fail(client, Some(e.to_string()), now, entropy);
                return Ok(false);
            }
            Err(e) => {
                self.active = Some(pending);
                return Err(e);
            }
        };
        self.loading.wake();
        settle(&progress, actions);
        if !progress.continues.is_empty() || self.again {
            self.again = true;
            self.pull(client, actions)?;
            self.again = false;
        }
        Ok(progress.disposition == "applied")
    }
}
