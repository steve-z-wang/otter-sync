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
use crate::ClientStore;

/// What an outstanding effect was issued for.
pub(super) enum EffectKind {
    Callback,
    RefreshAuth,
    DirectHttp { request_id: String },
    DirectTimer { request_id: String },
    Prerequisite { key: String },
    PrerequisiteTimer,
}
pub(super) enum Ready {
    ApplyDirect {
        request_id: String,
        response: String,
    },
    PrerequisiteOutcome {
        key: String,
        error: Option<String>,
    },
}
pub(super) enum Waiter {
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
            refusal: None,
            retry: false,
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
                    refusal: None,
                    retry: false,
                });
            }
            match answer.remove("body") {
                Some(Value::String(body)) => Ok(body),
                _ => Err(EffectError {
                    message: "invalid HTTP result: body must be a string".into(),
                    status,
                    refusal: None,
                    retry: false,
                }),
            }
        }
        _ => Err(EffectError {
            message: "invalid HTTP result".into(),
            status: None,
            refusal: None,
            retry: false,
        }),
    }
}

/// The admission refusal a failed effect carries: a `refusal` body together
/// with the status it was answered with. Either alone is an ordinary failure.
fn admission_refusal(outcome: &EffectOutcome) -> Option<EffectError> {
    outcome
        .error
        .as_ref()
        .filter(|error| !outcome.ok && error.status.is_some() && error.refusal.is_some())
        .cloned()
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
        if let Some(error) = admission_refusal(&outcome)
            && matches!(
                self.effects.get(&effect_id),
                Some(EffectKind::DirectHttp { .. })
            )
        {
            return self.refused(&effect_id, error);
        }
        if matches!(
            self.effects.get(&effect_id),
            None | Some(EffectKind::Callback)
        ) {
            return;
        }
        let Some(kind) = self.effects.remove(&effect_id) else {
            return;
        };
        match kind {
            EffectKind::RefreshAuth => self.refreshed(&effect_id, outcome, now, entropy),
            EffectKind::DirectHttp { request_id } => self.direct_result(request_id, outcome),
            EffectKind::DirectTimer { request_id } => self.direct_timeout(request_id),
            EffectKind::Prerequisite { key } => {
                self.prerequisite_result(key, outcome, now, entropy)
            }
            EffectKind::PrerequisiteTimer => self.prerequisite_timer_fired(&effect_id),
            EffectKind::Callback => {}
        }
    }

    /// The server refused this client's admission on one of the connection's
    /// requests. That answers the client, not the request: it is reported
    /// once and the connection stops as `stop` stops it - no credential
    /// refresh, no backoff, no reconnect. The direct call whose request was
    /// refused fails with the refusal as its cause, every other call still
    /// out fails unavailable, and the frozen batch, Load pages and
    /// subscriptions stay for a later `connect`. Results of the requests the
    /// stop abandoned are fenced like any cancelled effect's.
    fn refused(&mut self, effect_id: &str, error: EffectError) {
        let Some(kind) = self.effects.remove(effect_id) else {
            return;
        };
        if let EffectKind::DirectHttp { request_id } = kind {
            self.fail_call(&request_id, direct::Failure::Refused(error.clone()));
        }
        if self.connection.is_none() {
            return;
        }
        let (Some(status), Some(body)) = (error.status, error.refusal) else {
            return;
        };
        let body = serde_json::from_str(&body).unwrap_or(Value::String(body));
        self.report(Diagnostic::Refused {
            message: error.message,
            status,
            body,
        });
        self.stop_lanes();
    }

    /// Share one credential refresh across direct callers.
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
                refusal: None,
                retry: false,
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
        _now: u64,
        _entropy: u64,
    ) {
        let Waiter::Direct { request_id } = waiter;
        if let Some(error) = refused {
            self.fail_call(&request_id, direct::Failure::Transport(error.clone()))
        } else {
            self.resend_direct(&request_id)
        }
    }
}
