//! The effect table: what every outstanding effect was issued for, how its
//! result is admitted, the continuations results become, and the one
//! credential refresh the lanes and direct calls share
//! ([#134](https://github.com/zanminwang/axton/issues/134)).
//!
//! A result is admitted by [`ClientRuntime::receive`] without database work:
//! it is correlated by id - an id that is not outstanding was cancelled,
//! already answered or never issued, and its result is ignored - and handed
//! to the Downlink worker's queues at once, turned into a lane flag, or into
//! a [`Ready`] continuation that a later step runs as one local unit.
use super::*;
use crate::{ClientStore, DownlinkEvent};

/// What an outstanding effect was issued for.
pub(super) enum EffectKind {
    /// The application callback of the transaction this id names; answered
    /// by [`Input::CallbackResult`], never by an effect result.
    Callback,
    /// The frozen batch of the push lane's cycle.
    Push,
    /// A Downlink worker request: an ordinary catch-up of the session of
    /// `epoch`, or a Bootstrap page, which belongs to no session.
    Pull {
        request: u64,
        bootstrap: bool,
        epoch: u64,
    },
    /// The socket of the worker's session `epoch`: a stream of results.
    Socket {
        epoch: u64,
    },
    PushTimer,
    DownlinkTimer,
    RefreshAuth,
    /// The HTTP request of the direct call routed by `request_id`.
    DirectHttp {
        request_id: String,
    },
    /// The deadline of that call.
    DirectTimer {
        request_id: String,
    },
    /// One prerequisite handler run of the `runPrerequisites` task.
    Prerequisite {
        request_id: String,
        key: String,
    },
}

/// An effect result that needs local work, run as one unit by `step`.
pub(super) enum Ready {
    /// The receipt of the push in flight: settle it in one transaction.
    PushReceipt { body: String },
    /// The response of a direct call: apply it in one transaction.
    ApplyDirect {
        request_id: String,
        response: String,
    },
    /// A prerequisite handler settled: record it in one transaction.
    PrerequisiteOutcome { key: String, error: Option<String> },
    /// The prerequisite loop's next turn: pick the next task.
    PrerequisiteNext,
}

/// Work waiting for the credential refresh in flight, resumed when it settles.
pub(super) enum Waiter {
    /// The push that failed with 401: the cycle then fails with backoff.
    Push,
    /// The socket that failed with 401: the worker then hears it closed.
    Socket { epoch: u64 },
    /// The worker request that failed with 401: the worker then hears it failed.
    Pull {
        request: u64,
        reason: Option<String>,
        status: Option<u16>,
        bootstrap: bool,
    },
    /// The direct call that failed with 401: sent again once, or failed.
    Direct { request_id: String },
}

/// The HTTP answer's body, or why the request failed. A success value is
/// `{"status", "body"}` (a non-2xx status is a failure with that status) or
/// a bare body string.
pub(super) fn http_body(outcome: EffectOutcome) -> std::result::Result<String, EffectError> {
    if !outcome.ok {
        return Err(outcome.error.unwrap_or_else(|| EffectError {
            message: "request failed".into(),
            status: None,
        }));
    }
    match outcome.value {
        Some(Value::String(body)) => Ok(body),
        Some(Value::Object(mut answer)) => {
            let status = answer
                .get("status")
                .and_then(Value::as_u64)
                .and_then(|s| u16::try_from(s).ok());
            if let Some(status) = status.filter(|s| !(200..300).contains(s)) {
                return Err(EffectError {
                    message: format!("HTTP {status}"),
                    status: Some(status),
                });
            }
            match answer.remove("body") {
                Some(Value::String(body)) => Ok(body),
                _ => Err(EffectError {
                    message: "invalid HTTP result: body must be a string".into(),
                    status,
                }),
            }
        }
        _ => Err(EffectError {
            message: "invalid HTTP result".into(),
            status: None,
        }),
    }
}

impl<S: ClientStore + 'static> ClientRuntime<S> {
    /// Ask the host for one effect and remember what it is for. `None` only
    /// when the identifiers are exhausted, which is reported.
    pub(super) fn issue_effect(
        &mut self,
        kind: EffectKind,
        operation: Operation,
    ) -> Option<String> {
        match self.issue() {
            Ok(id) => {
                let effect_id = id.to_string();
                self.effects.insert(effect_id.clone(), kind);
                self.events.push(Event::Effect {
                    effect_id: effect_id.clone(),
                    operation,
                });
                Some(effect_id)
            }
            Err(message) => {
                self.error(message);
                None
            }
        }
    }
    /// Retire an outstanding effect and tell the host to abort it. A late
    /// result is fenced either way. False when it was not outstanding.
    pub(super) fn cancel_effect(&mut self, effect_id: &str) -> bool {
        if self.effects.remove(effect_id).is_none() {
            return false;
        }
        self.events.push(Event::CancelEffect {
            effect_id: effect_id.to_string(),
        });
        true
    }

    /// Admit one effect result: correlate, fence, and turn it into queued
    /// work. No database work.
    pub(super) fn effect_result(
        &mut self,
        effect_id: String,
        outcome: EffectOutcome,
        now: u64,
        entropy: u64,
    ) {
        match self.effects.get(&effect_id) {
            None | Some(EffectKind::Callback) => {}
            Some(EffectKind::Socket { epoch }) => {
                let epoch = *epoch;
                self.socket_result(effect_id, epoch, outcome, now, entropy);
            }
            Some(_) => {
                let Some(kind) = self.effects.remove(&effect_id) else {
                    return;
                };
                match kind {
                    EffectKind::Push => self.push_result(outcome, now, entropy),
                    EffectKind::Pull {
                        request,
                        bootstrap,
                        epoch,
                    } => self.pull_result(request, bootstrap, epoch, outcome, now, entropy),
                    EffectKind::PushTimer => self.push_timer_fired(&effect_id),
                    EffectKind::DownlinkTimer => self.downlink_timer_fired(&effect_id),
                    EffectKind::RefreshAuth => self.refreshed(&effect_id, outcome, now, entropy),
                    EffectKind::DirectHttp { request_id } => {
                        self.direct_result(request_id, outcome)
                    }
                    EffectKind::DirectTimer { request_id } => self.direct_timeout(request_id),
                    EffectKind::Prerequisite { request_id, key } => {
                        self.prerequisite_result(request_id, key, outcome)
                    }
                    EffectKind::Callback | EffectKind::Socket { .. } => {}
                }
            }
        }
    }

    /// One result of a socket stream. A frame or an overflow goes to the
    /// worker; the end of the stream - `closed` or a failure - is reported,
    /// refreshes credentials on 401, and then tells the worker it closed.
    fn socket_result(
        &mut self,
        effect_id: String,
        epoch: u64,
        outcome: EffectOutcome,
        now: u64,
        entropy: u64,
    ) {
        if !outcome.ok {
            let error = outcome.error.unwrap_or_else(|| EffectError {
                message: "socket failed".into(),
                status: None,
            });
            return self.socket_ended(&effect_id, epoch, error, now, entropy);
        }
        let event = serde_json::from_value::<SocketEvent>(outcome.value.unwrap_or(Value::Null));
        match event {
            Ok(SocketEvent::Opened) => {}
            Ok(SocketEvent::Message { body }) => {
                self.enqueue_downlink(DownlinkEvent::Message { epoch, body }, now, entropy);
            }
            Ok(SocketEvent::Overflow) => {
                self.enqueue_downlink(DownlinkEvent::Overflow { epoch }, now, entropy)
            }
            Ok(SocketEvent::Closed) => {
                let error = EffectError {
                    message: "socket closed".into(),
                    status: None,
                };
                self.socket_ended(&effect_id, epoch, error, now, entropy);
            }
            Err(e) => self.report(Diagnostic::Protocol {
                message: format!("invalid socket event: {e}"),
            }),
        }
    }
    /// The socket ended on its own: the session is abandoned (its catch-up
    /// with it), the application hears why, and the worker hears it closed -
    /// after one shared refresh when the server asked for credentials.
    fn socket_ended(
        &mut self,
        effect_id: &str,
        epoch: u64,
        error: EffectError,
        now: u64,
        entropy: u64,
    ) {
        self.effects.remove(effect_id);
        self.abandon_session(epoch);
        self.error_status(error.message, error.status);
        self.after_refresh(error.status, Waiter::Socket { epoch }, now, entropy);
    }
    /// The answer to a worker request, or its failure. An ordinary catch-up's
    /// failure ends its session in the worker; a Bootstrap page's failure is
    /// retried or refused by the worker on its own schedule.
    fn pull_result(
        &mut self,
        request: u64,
        bootstrap: bool,
        epoch: u64,
        outcome: EffectOutcome,
        now: u64,
        entropy: u64,
    ) {
        if !bootstrap {
            self.settle_catch_up(epoch);
        }
        match http_body(outcome) {
            Ok(body) => {
                self.enqueue_downlink(DownlinkEvent::Response { request, body }, now, entropy)
            }
            Err(error) => {
                self.error_status(error.message.clone(), error.status);
                let waiter = Waiter::Pull {
                    request,
                    reason: Some(error.message),
                    status: error.status,
                    bootstrap,
                };
                self.after_refresh(error.status, waiter, now, entropy);
            }
        }
    }

    /// Resume `waiter` now, or after the shared credential refresh when the
    /// failure was a 401 and the connection may refresh.
    pub(super) fn after_refresh(
        &mut self,
        status: Option<u16>,
        waiter: Waiter,
        now: u64,
        entropy: u64,
    ) {
        let refresh = status == Some(401)
            && self
                .connection
                .as_ref()
                .is_some_and(|connection| connection.refresh);
        if refresh {
            self.join_refresh(waiter);
        } else {
            self.resume_waiter(waiter, None, now, entropy);
        }
    }
    /// Wait for the refresh in flight, starting one when none is: however
    /// many requests failed with 401 together, the application's
    /// `refreshAuth` runs once for them.
    pub(super) fn join_refresh(&mut self, waiter: Waiter) {
        let Some(connection) = &mut self.connection else {
            return;
        };
        connection.waiters.push(waiter);
        if connection.refreshing.is_some() {
            return;
        }
        let effect = self.issue_effect(EffectKind::RefreshAuth, Operation::RefreshAuth);
        if let Some(connection) = &mut self.connection {
            connection.refreshing = effect;
        }
    }
    /// The refresh settled: every waiter resumes what it was doing. A failed
    /// refresh is the application's error as well.
    fn refreshed(&mut self, effect_id: &str, outcome: EffectOutcome, now: u64, entropy: u64) {
        let waiters = match &mut self.connection {
            Some(connection) if connection.refreshing.as_deref() == Some(effect_id) => {
                connection.refreshing = None;
                std::mem::take(&mut connection.waiters)
            }
            _ => return,
        };
        let refused = (!outcome.ok).then(|| {
            outcome.error.unwrap_or_else(|| EffectError {
                message: "refreshAuth failed".into(),
                status: None,
            })
        });
        if let Some(refused) = &refused {
            self.error_status(refused.message.clone(), refused.status);
        }
        for waiter in waiters {
            self.resume_waiter(waiter, refused.as_ref(), now, entropy);
        }
    }
    /// Continue what a failure interrupted: the push cycle fails with
    /// backoff, the worker hears its socket closed or its request failed, a
    /// direct call is sent again once - or fails with the refresh's refusal
    /// as its cause when the refresh was `refused`.
    fn resume_waiter(
        &mut self,
        waiter: Waiter,
        refused: Option<&EffectError>,
        now: u64,
        entropy: u64,
    ) {
        match waiter {
            Waiter::Push => self.push_failed(now, entropy),
            Waiter::Socket { epoch } => {
                self.enqueue_downlink(DownlinkEvent::Closed { epoch }, now, entropy)
            }
            Waiter::Pull {
                request,
                reason,
                status,
                ..
            } => self.enqueue_downlink(
                DownlinkEvent::Failed {
                    request,
                    reason,
                    status,
                },
                now,
                entropy,
            ),
            Waiter::Direct { request_id } => match refused {
                None => self.resend_direct(&request_id),
                Some(refused) => self.fail_direct(
                    &request_id,
                    direct::EXECUTION_UNKNOWN,
                    direct::transport_failure(refused),
                ),
            },
        }
    }
}
