//! The connection lanes as runtime work: the connection intent, its controls,
//! the push lane and the Downlink worker
//! ([#134](https://github.com/zanminwang/axton/issues/134);
//! [Controller](../../../../docs/engineering/architecture/client/connection/controller/README.md)).
//!
//! Three engines decide: `ConnectionDriver` when the push lane syncs and how
//! it backs off, `SyncCycle` freezing and settling one batch at a time, and
//! `DownlinkWorker` delivery, its sessions, its catch-up and Bootstrap
//! requests. Everything around them - executing their actions, waiting,
//! reporting, refreshing credentials on 401, aborting on pause - is done
//! here; the host only executes the effects.
use super::effects::{EffectKind, Waiter};
use super::*;
use crate::{
    ClientStore, ConnectionAction, ConnectionDriver, DownlinkAction, DownlinkEvent, DownlinkWorker,
    SyncCycle,
};

/// The engines behind the lanes.
#[derive(Default)]
pub(super) struct Lanes {
    pub(super) cycle: SyncCycle,
    pub(super) connection: ConnectionDriver,
    pub(super) downlink: DownlinkWorker,
}

/// The deadline of a direct call when `connect` names none.
const DIRECT_TIMEOUT_MS: u64 = 30_000;
const DIRECT_TIMEOUT_MAX: u64 = 2_147_483_647;
const ALREADY_ACTIVE: &str = "connection already active";

/// A runtime-owned connection: what `connect` asked for and the lanes' host
/// work in flight.
pub(super) struct Connection {
    /// The deadline of one direct call attempt, retries included.
    pub(super) timeout: u64,
    /// Whether a 401 may ask the application for a credential refresh.
    pub(super) refresh: bool,
    pub(super) paused: bool,
    pub(super) push: PushLane,
    pub(super) downlink: DownlinkLane,
    /// The credential refresh in flight, shared by everything that hit a 401.
    pub(super) refreshing: Option<String>,
    pub(super) waiters: Vec<Waiter>,
}
impl Connection {
    /// The epoch of the open socket session, if one is open.
    pub(super) fn session(&self) -> Option<u64> {
        self.downlink.socket.as_ref().map(|(_, epoch)| *epoch)
    }
    /// Whether a catch-up request of the open session is out.
    pub(super) fn catching_up(&self) -> bool {
        self.downlink.outstanding > 0
    }
}
#[derive(Default)]
pub(super) struct PushLane {
    /// Ask the driver (or the cycle) what to do on the next lane unit.
    pub(super) dirty: bool,
    timer: Option<String>,
    effect: Option<String>,
    /// A cycle the driver started is running: the next turn asks the cycle
    /// for the next batch instead of the driver.
    cycling: bool,
    /// The cycle's batch is out - sent, waiting for a refresh, or answered
    /// and waiting for its receipt to settle: one frozen batch in flight.
    waiting: bool,
}
#[derive(Default)]
pub(super) struct DownlinkLane {
    /// Pump the worker on the next lane unit.
    pub(super) dirty: bool,
    timer: Option<String>,
    /// The socket effect of the worker's open session and its epoch.
    socket: Option<(String, u64)>,
    /// Catch-up requests of that session not answered yet.
    outstanding: u64,
}

impl<S: ClientStore + 'static> ClientRuntime<S> {
    /// `connect {directTimeoutMs?, refreshAuth?}`: record the intent and start
    /// both lanes. Refused while a connection is active.
    pub(super) fn connect(
        &mut self,
        direct_timeout_ms: Option<u64>,
        refresh: bool,
        now: u64,
        entropy: u64,
    ) -> std::result::Result<Value, String> {
        if self.connection.is_some() {
            return Err(ALREADY_ACTIVE.into());
        }
        let timeout = match direct_timeout_ms {
            None => DIRECT_TIMEOUT_MS,
            Some(timeout) if (1..=DIRECT_TIMEOUT_MAX).contains(&timeout) => timeout,
            Some(_) => return Err("directTimeoutMs must be an integer from 1 to 2147483647".into()),
        };
        self.connection = Some(Connection {
            timeout,
            refresh,
            paused: false,
            push: PushLane {
                dirty: true,
                ..Default::default()
            },
            downlink: DownlinkLane {
                dirty: true,
                ..Default::default()
            },
            refreshing: None,
            waiters: vec![],
        });
        self.lanes.connection.start(now);
        self.enqueue_downlink(DownlinkEvent::Start, now, entropy);
        Ok(Value::Null)
    }

    /// `connection {event}`: the four controls of the runtime-owned
    /// connection. Without one they change nothing.
    pub(super) fn control(
        &mut self,
        event: ConnectionEvent,
        now: u64,
        entropy: u64,
    ) -> std::result::Result<Value, String> {
        if self.connection.is_none() {
            return Ok(Value::Null);
        }
        match event {
            ConnectionEvent::Pause => self.pause_lanes(now, entropy),
            ConnectionEvent::Resume => {
                self.lanes.connection.resume(now);
                self.enqueue_downlink(DownlinkEvent::Resume, now, entropy);
                if let Some(connection) = &mut self.connection {
                    connection.paused = false;
                }
                self.wake_push();
            }
            ConnectionEvent::Wake => self.wake_lanes(now, entropy),
            ConnectionEvent::Stop => self.stop_lanes(),
        }
        Ok(Value::Null)
    }

    /// Pause: abandon the session, its catch-up, the Bootstrap page and the
    /// push in flight - the lane's own abandonment, so nothing is reported
    /// and no backoff follows - and schedule nothing until `resume`.
    fn pause_lanes(&mut self, now: u64, entropy: u64) {
        if let Some((_, epoch)) = self
            .connection
            .as_ref()
            .and_then(|c| c.downlink.socket.clone())
        {
            self.abandon_session(epoch);
        }
        // The worker still hears about its Bootstrap page, so it clears the
        // slot and asks again on `resume`; its status says no server decided
        // anything.
        let loads: Vec<(String, u64)> = self
            .effects
            .iter()
            .filter_map(|(id, kind)| match kind {
                EffectKind::Pull {
                    request,
                    bootstrap: true,
                    ..
                } => Some((id.clone(), *request)),
                _ => None,
            })
            .collect();
        for (effect_id, request) in loads {
            self.cancel_effect(&effect_id);
            self.enqueue_downlink(
                DownlinkEvent::Failed {
                    request,
                    reason: None,
                    status: None,
                },
                now,
                entropy,
            );
        }
        self.cancel_lane_effects(false);
        let Some(connection) = &mut self.connection else {
            return;
        };
        connection.paused = true;
        // A push in flight - sent, or failed and waiting for a refresh - is
        // abandoned; an aborted push is not a failure, so no backoff follows.
        let sent = connection.push.effect.take();
        let mut refreshing = false;
        let mut loads = vec![];
        connection.waiters.retain(|waiter| match waiter {
            Waiter::Push => {
                refreshing = true;
                false
            }
            Waiter::Socket { .. } => false,
            Waiter::Pull {
                request, bootstrap, ..
            } => {
                if *bootstrap {
                    loads.push(*request);
                }
                false
            }
            Waiter::Direct { .. } => true,
        });
        let abandoned = sent.is_some() || refreshing;
        if abandoned {
            connection.push.cycling = false;
            connection.push.waiting = false;
        }
        for request in loads {
            self.enqueue_downlink(
                DownlinkEvent::Failed {
                    request,
                    reason: None,
                    status: None,
                },
                now,
                entropy,
            );
        }
        if let Some(effect_id) = &sent {
            self.cancel_effect(effect_id);
        }
        if abandoned {
            self.lanes.connection.complete(true, now, 0);
        }
        self.lanes.connection.pause();
        self.enqueue_downlink(DownlinkEvent::Pause, now, entropy);
    }

    /// Stop: everything `pause` abandons, the lanes stop for good, direct
    /// calls in flight fail as unavailable and the intent is cleared.
    fn stop_lanes(&mut self) {
        if let Some((_, epoch)) = self
            .connection
            .as_ref()
            .and_then(|c| c.downlink.socket.clone())
        {
            self.abandon_session(epoch);
        }
        self.cancel_lane_effects(true);
        if let Some(refresh) = self.connection.as_ref().and_then(|c| c.refreshing.clone()) {
            self.cancel_effect(&refresh);
        }
        self.fail_directs_in_flight(direct::UNAVAILABLE);
        self.connection = None;
        self.lanes.connection.stop();
        // Enqueue work only: no database access, no actions.
        let _ = self
            .lanes
            .downlink
            .handle(&mut self.client, DownlinkEvent::Stop, 0, 0);
    }

    /// Cancel every lane effect: worker requests, timers and - with `push` -
    /// the push in flight.
    fn cancel_lane_effects(&mut self, push: bool) {
        let lane: Vec<String> = self
            .effects
            .iter()
            .filter(|(_, kind)| match kind {
                EffectKind::Pull { .. }
                | EffectKind::Socket { .. }
                | EffectKind::PushTimer
                | EffectKind::DownlinkTimer => true,
                EffectKind::Push => push,
                _ => false,
            })
            .map(|(id, _)| id.clone())
            .collect();
        for effect_id in lane {
            self.cancel_effect(&effect_id);
        }
        if let Some(connection) = &mut self.connection {
            connection.push.timer = None;
            connection.downlink.timer = None;
            if push {
                connection.push.effect = None;
            }
        }
    }

    /// Something committed or asked for work: both lanes look again, and a
    /// lane sleeping on a timer drops it.
    pub(super) fn wake_lanes(&mut self, now: u64, entropy: u64) {
        if self.connection.is_none() {
            return;
        }
        self.enqueue_downlink(DownlinkEvent::Wake, now, entropy);
        self.wake_push();
    }
    fn wake_push(&mut self) {
        let Some(connection) = &mut self.connection else {
            return;
        };
        self.lanes.connection.wake();
        connection.push.dirty = true;
        if let Some(timer) = connection.push.timer.take() {
            self.cancel_effect(&timer);
        }
    }
    /// Hand the worker one event as it is admitted, with the facts of this
    /// call, and wake the lane for its next pump. The worker only queues it -
    /// no database work, no actions - so frames never pile up outside its
    /// bounded queue, even while a callback holds the writer.
    pub(super) fn enqueue_downlink(&mut self, event: DownlinkEvent, now: u64, entropy: u64) {
        if self.connection.is_none() {
            return;
        }
        // Enqueue work only: it answers no actions and cannot fail.
        let _ = self
            .lanes
            .downlink
            .handle(&mut self.client, event, now, entropy);
        let Some(connection) = &mut self.connection else {
            return;
        };
        connection.downlink.dirty = true;
        if let Some(timer) = connection.downlink.timer.take() {
            self.cancel_effect(&timer);
        }
    }

    // --- Push lane ---------------------------------------------------------

    /// One push-lane turn: ask the driver whether to sync, or the running
    /// cycle for its next batch (a freeze: one local transaction).
    pub(super) fn push_turn(&mut self, now: u64, entropy: u64) {
        let Some(connection) = &mut self.connection else {
            return;
        };
        connection.push.dirty = false;
        if connection.paused {
            if std::mem::take(&mut connection.push.cycling) {
                self.lanes.connection.complete(true, now, 0);
            }
            return;
        }
        if connection.push.waiting {
            return;
        }
        if !connection.push.cycling {
            match self.lanes.connection.next(now) {
                ConnectionAction::Idle => return,
                ConnectionAction::Wait { millis } => {
                    let timer = connection.push.timer.take();
                    if let Some(timer) = timer {
                        self.cancel_effect(&timer);
                    }
                    let timer =
                        self.issue_effect(EffectKind::PushTimer, Operation::Timer { millis });
                    if let Some(connection) = &mut self.connection {
                        connection.push.timer = timer;
                    }
                    return;
                }
                ConnectionAction::Sync => {
                    self.lanes.cycle.restart_push_only();
                    connection.push.cycling = true;
                }
            }
        }
        let generation = self.client.generation();
        let next = self.lanes.cycle.next(&mut self.client);
        self.committed_since(generation);
        match next {
            Ok(Some(action)) => {
                let effect = self.issue_effect(
                    EffectKind::Push,
                    Operation::Http {
                        route: HttpRoute::Push,
                        body: action.body,
                    },
                );
                match effect {
                    Some(effect) => {
                        if let Some(connection) = &mut self.connection {
                            connection.push.effect = Some(effect);
                            connection.push.waiting = true;
                        }
                    }
                    None => self.push_failed(now, entropy),
                }
            }
            Ok(None) => {
                if let Some(connection) = &mut self.connection {
                    connection.push.cycling = false;
                    connection.push.dirty = true;
                }
                self.lanes.connection.complete(true, now, 0);
            }
            Err(e) => {
                self.error(e.to_string());
                self.push_failed(now, entropy);
            }
        }
    }
    /// The push effect answered: a receipt to settle, or a failure that
    /// fails the cycle - after one shared refresh on 401.
    pub(super) fn push_result(&mut self, outcome: EffectOutcome, now: u64, entropy: u64) {
        if let Some(connection) = &mut self.connection {
            connection.push.effect = None;
        }
        match effects::http_body(outcome) {
            Ok(body) => self.ready.push_back(effects::Ready::PushReceipt { body }),
            Err(error) => {
                self.error_status(error.message, error.status);
                self.after_refresh(error.status, Waiter::Push, now, entropy);
            }
        }
    }
    /// The cycle failed: the frozen batch stays in flight for the next cycle
    /// and the driver backs off.
    pub(super) fn push_failed(&mut self, now: u64, entropy: u64) {
        if let Some(connection) = &mut self.connection {
            connection.push.cycling = false;
            connection.push.waiting = false;
            connection.push.dirty = true;
        }
        self.lanes.connection.complete(false, now, entropy);
    }
    /// Settle a receipt in one transaction: completions after the commit,
    /// then the cycle goes on with the next batch.
    pub(super) fn push_receipt(&mut self, body: String, now: u64, entropy: u64) {
        let generation = self.client.generation();
        let applied = self.lanes.cycle.complete(&mut self.client, body.as_bytes());
        self.committed_since(generation);
        match applied {
            Ok(report) => {
                self.settled(&report);
                if let Some(connection) = &mut self.connection {
                    connection.push.waiting = false;
                    connection.push.dirty = true;
                }
            }
            Err(e) => {
                self.error(e.to_string());
                self.push_failed(now, entropy);
            }
        }
    }
    /// What a commit settled: every call's final outcome, then what it could
    /// not apply.
    pub(super) fn settled(&mut self, report: &crate::ApplyReport) {
        for completion in &report.completions {
            self.events.push(Event::CallCompleted {
                call_id: completion.call_id.clone(),
                outcome: serde_json::to_value(&completion.outcome).unwrap_or(Value::Null),
            });
        }
        if !report.reports.is_empty() {
            self.report(Diagnostic::Records {
                reports: report.reports.clone(),
            });
        }
    }
    pub(super) fn push_timer_fired(&mut self, effect_id: &str) {
        if let Some(connection) = &mut self.connection
            && connection.push.timer.as_deref() == Some(effect_id)
        {
            connection.push.timer = None;
            connection.push.dirty = true;
        }
    }

    // --- Downlink lane -----------------------------------------------------

    /// One pump: the worker consumes what it holds, commits at most one page
    /// and decides; its actions are executed as effects.
    pub(super) fn downlink_turn(&mut self, now: u64, entropy: u64) {
        let Some(connection) = &mut self.connection else {
            return;
        };
        connection.downlink.dirty = false;
        let generation = self.client.generation();
        let pumped =
            self.lanes
                .downlink
                .handle(&mut self.client, DownlinkEvent::Next, now, entropy);
        self.committed_since(generation);
        match pumped {
            Ok(actions) => {
                let waits = actions
                    .iter()
                    .any(|action| matches!(action, DownlinkAction::Wait { .. }));
                let progressed = !actions.is_empty();
                for action in actions {
                    self.downlink_action(action);
                }
                // Progress without a sleep: pump again on a later unit.
                if progressed
                    && !waits
                    && let Some(connection) = &mut self.connection
                {
                    connection.downlink.dirty = true;
                }
            }
            Err(e) => {
                // A pump that failed on the open session ends it; with none
                // open there is nothing to retry until something arrives.
                self.error(e.to_string());
                let socket = self
                    .connection
                    .as_ref()
                    .and_then(|c| c.downlink.socket.clone());
                if let Some((_, epoch)) = socket {
                    self.abandon_session(epoch);
                    self.enqueue_downlink(DownlinkEvent::Closed { epoch }, now, entropy);
                }
            }
        }
    }
    fn downlink_action(&mut self, action: DownlinkAction) {
        match action {
            DownlinkAction::Open { epoch, subscribe } => {
                if let Some((_, old)) = self
                    .connection
                    .as_ref()
                    .and_then(|c| c.downlink.socket.clone())
                {
                    self.abandon_session(old);
                }
                let effect = self.issue_effect(
                    EffectKind::Socket { epoch },
                    Operation::Socket { subscribe },
                );
                if let Some(connection) = &mut self.connection {
                    connection.downlink.socket = effect.map(|id| (id, epoch));
                    connection.downlink.outstanding = 0;
                }
            }
            DownlinkAction::Close { epoch, reason } => {
                self.abandon_session(epoch);
                if let Some(reason) = reason {
                    self.error(reason);
                }
            }
            DownlinkAction::Request {
                request,
                body,
                bootstrap,
            } => {
                let epoch = if bootstrap {
                    0
                } else {
                    // A catch-up belongs to the open session; with none the
                    // worker ends it on the close it is about to hear.
                    match self
                        .connection
                        .as_ref()
                        .and_then(|c| c.downlink.socket.clone())
                    {
                        Some((_, epoch)) => epoch,
                        None => return,
                    }
                };
                self.issue_effect(
                    EffectKind::Pull {
                        request,
                        bootstrap,
                        epoch,
                    },
                    Operation::Http {
                        route: HttpRoute::Pull,
                        body,
                    },
                );
                if !bootstrap && let Some(connection) = &mut self.connection {
                    connection.downlink.outstanding += 1;
                }
            }
            DownlinkAction::Bootstrap(state) => self.observe_run(state),
            // The replica was rebuilt: the runtime already cancelled the old
            // replica's lane effects when it reset the worker; whatever is
            // still held for it goes now, before the worker opens or requests
            // anything for the new one (#162).
            DownlinkAction::Reset => {
                if let Some((_, epoch)) = self
                    .connection
                    .as_ref()
                    .and_then(|c| c.downlink.socket.clone())
                {
                    self.abandon_session(epoch);
                }
                self.cancel_lane_effects(false);
            }
            // A stored Bootstrap row the ledger cannot decode: the application
            // hears about the contained registration once per unchanged
            // defect; no status transition accompanies it (#163).
            DownlinkAction::LedgerIssue { channel, message } => {
                self.error(format!("bootstrap ledger {channel}: {message}"));
            }
            DownlinkAction::Wake { .. } => self.wake_push(),
            DownlinkAction::Report { reports } => self.report(Diagnostic::Records { reports }),
            DownlinkAction::Changed { scopes } => self.scopes_changed(&scopes),
            DownlinkAction::Acknowledged { scopes } => self.acknowledged(scopes),
            DownlinkAction::Wait { millis } => {
                let Some(connection) = &mut self.connection else {
                    return;
                };
                if let Some(timer) = connection.downlink.timer.take() {
                    self.cancel_effect(&timer);
                }
                if millis == 0 {
                    if let Some(connection) = &mut self.connection {
                        connection.downlink.dirty = true;
                    }
                    return;
                }
                let timer =
                    self.issue_effect(EffectKind::DownlinkTimer, Operation::Timer { millis });
                if let Some(connection) = &mut self.connection {
                    connection.downlink.timer = timer;
                }
            }
        }
    }
    /// The session of `epoch` is over: its socket (if it is still the one
    /// open) and its catch-up requests are abandoned. A session already gone
    /// is left alone.
    pub(super) fn abandon_session(&mut self, epoch: u64) {
        let pulls: Vec<String> = self
            .effects
            .iter()
            .filter(|(_, kind)| {
                matches!(kind, EffectKind::Pull { bootstrap: false, epoch: e, .. } if *e == epoch)
            })
            .map(|(id, _)| id.clone())
            .collect();
        for effect_id in pulls {
            self.cancel_effect(&effect_id);
        }
        let Some(connection) = &mut self.connection else {
            return;
        };
        let Some((effect_id, _)) = connection
            .downlink
            .socket
            .take_if(|(_, open)| *open == epoch)
        else {
            return;
        };
        connection.downlink.outstanding = 0;
        self.cancel_effect(&effect_id);
    }
    /// A catch-up of the session of `epoch` answered or failed.
    pub(super) fn settle_catch_up(&mut self, epoch: u64) {
        let Some(connection) = &mut self.connection else {
            return;
        };
        if connection.downlink.socket.as_ref().map(|(_, e)| *e) != Some(epoch) {
            return;
        }
        connection.downlink.outstanding = connection.downlink.outstanding.saturating_sub(1);
    }
    pub(super) fn downlink_timer_fired(&mut self, effect_id: &str) {
        if let Some(connection) = &mut self.connection
            && connection.downlink.timer.as_deref() == Some(effect_id)
        {
            connection.downlink.timer = None;
            connection.downlink.dirty = true;
        }
    }

    /// A rebuild replaced the replica: every lane effect belongs to the old
    /// one. The worker has already been reset in the same intent; the push
    /// driver keeps its intent and its cycle starts over.
    pub(super) fn rebuilt_lanes(&mut self, now: u64, entropy: u64) {
        self.ready
            .retain(|ready| !matches!(ready, effects::Ready::PushReceipt { .. }));
        let socket = self
            .connection
            .as_ref()
            .and_then(|c| c.downlink.socket.clone());
        if let Some((_, epoch)) = socket {
            self.abandon_session(epoch);
        }
        self.cancel_lane_effects(true);
        let Some(connection) = &mut self.connection else {
            return;
        };
        connection
            .waiters
            .retain(|waiter| matches!(waiter, Waiter::Direct { .. }));
        connection.push.cycling = false;
        connection.push.waiting = false;
        connection.downlink.outstanding = 0;
        // Whatever was in flight on the push lane is gone with the replica.
        self.lanes.connection.complete(true, now, 0);
        self.wake_lanes(now, entropy);
    }

    /// Close: the lanes stop; their effects were already cancelled.
    pub(super) fn close_lanes(&mut self) {
        self.connection = None;
    }
}
