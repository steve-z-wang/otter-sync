//! Direct Query and Mutation calls and Query once flights as runtime tasks
//! ([#134](https://github.com/zanminwang/axton/issues/134),
//! [#158](https://github.com/zanminwang/axton/issues/158)).
//!
//! An `invoke` task prepares the exact request without touching the database
//! (or asks the Query once cache first), asks the host for one `http`
//! (`action` route) effect and one deadline timer, and parks: nothing holds
//! the writer while the request is out. The response is applied by one later
//! unit in one local transaction, and the task succeeds only after that
//! commit, with the response's own outcome - the invocation's snapshot, never
//! a reread of the Model. A 401 refreshes credentials once, shared with the
//! lanes, and sends the same body once more under the same deadline; every
//! other failure, the deadline, and a rebuild leave the execution unknown.
//!
//! Every such failure carries `details` with its `code` and, when a cause is
//! known, that cause's `message` and HTTP `status`, so an SDK can attach it:
//! the transport failure, the refused refresh, the failed apply, or
//! `"direct call timed out"` for the deadline ([`transport_failure`]).
//!
//! Stopping the connection fails a call still waiting on the network with
//! `action.unavailable`, but a call whose response is already in hand is
//! known to have executed: it is still applied once the writer is free and
//! completes with its own outcome. Closing the client fails every call with
//! `action.unavailable`, a response in hand included, and nothing is applied
//! after [`Event::RuntimeClosed`].
use super::effects::{EffectKind, Ready, Waiter};
use super::*;
use crate::{ActionCallOptions, ClientStore, QueryOnce, QueryOnceOptions};

pub(super) const UNAVAILABLE: &str = "action.unavailable";
pub(super) const EXECUTION_UNKNOWN: &str = "action.execution_unknown";
pub(super) const OBSERVATION_FAILED: &str = "action.observation_failed";
pub(super) const INVALID_OPTIONS: &str = "action.invalid_options";
const TIMED_OUT: &str = "direct call timed out";

/// The `details` of a direct failure with no known cause: `{"code"}`.
pub(super) fn code(error: &str) -> Value {
    json!({ "code": error })
}
/// The `details` of an unknown execution caused by `cause`:
/// `{"code":"action.execution_unknown","message","status"?}`.
pub(super) fn transport_failure(cause: &EffectError) -> Value {
    let mut details = json!({ "code": EXECUTION_UNKNOWN, "message": cause.message });
    if let Some(status) = cause.status {
        details["status"] = json!(status);
    }
    details
}

/// One direct call in flight, by the request id of its task.
pub(super) struct Call {
    /// The call id the response's completion must carry.
    call_id: String,
    /// The exact request body: sent, re-sent once after a refresh, and what
    /// the response is validated against.
    body: String,
    http: Option<String>,
    timer: Option<String>,
    /// The one re-send after a credential refresh was used.
    retried: bool,
    /// The response arrived and waits for its apply unit.
    applying: bool,
    /// The Query once flight this call fetches for, if any.
    flight: Option<String>,
}

#[derive(Default)]
pub(super) struct Directs {
    calls: BTreeMap<String, Call>,
    /// The requests joined to a flight another call fetches, by flight id.
    joined: BTreeMap<String, Vec<String>>,
}

/// One `invoke` as the task named it.
pub(super) struct Invocation<'a> {
    pub(super) name: &'a str,
    pub(super) version: u64,
    pub(super) args: &'a Value,
    pub(super) store: &'a Option<Value>,
    pub(super) once: bool,
    pub(super) refresh: bool,
}

impl<S: ClientStore + 'static> ClientRuntime<S> {
    /// `invoke {name, version, args, store?, once?, refresh?}`. `None` while
    /// the task waits for its request.
    pub(super) fn invoke(
        &mut self,
        request_id: &str,
        invocation: Invocation<'_>,
    ) -> Option<std::result::Result<Value, String>> {
        match self.begin_invoke(request_id, invocation) {
            Ok(Some(value)) => Some(Ok(value)),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        }
    }
    fn begin_invoke(
        &mut self,
        request_id: &str,
        invocation: Invocation<'_>,
    ) -> std::result::Result<Option<Value>, String> {
        let Invocation {
            name,
            version,
            args,
            store,
            once,
            refresh,
        } = invocation;
        let store = commands::options(store).map_err(|e| e.to_string())?.store;
        if refresh && !once {
            return Err(INVALID_OPTIONS.into());
        }
        if !once {
            if self.connection.is_none() {
                return Err(UNAVAILABLE.into());
            }
            let prepared = self
                .client
                .prepare_action_with_options(
                    name,
                    version,
                    args.clone(),
                    ActionCallOptions { store },
                )
                .map_err(|e| e.to_string())?;
            let body = prepared.encode().map_err(|e| e.to_string())?;
            let body = String::from_utf8(body).map_err(|_| "utf8".to_string())?;
            self.send_direct(request_id, prepared.call.call_id, body, None)?;
            return Ok(None);
        }
        let decision = self
            .client
            .begin_query_once(name, version, args, &QueryOnceOptions { store, refresh })
            .map_err(|e| e.to_string())?;
        match decision {
            QueryOnce::Cached { result } => Ok(Some(
                json!({"outcome":{"status":"succeeded","result":result}}),
            )),
            QueryOnce::Join { flight_id } => {
                let fetching = self
                    .directs
                    .calls
                    .values()
                    .any(|call| call.flight.as_deref() == Some(flight_id.as_str()));
                if !fetching {
                    // A flight this runtime is not fetching belongs to a
                    // replaced replica.
                    return Err(EXECUTION_UNKNOWN.into());
                }
                self.directs
                    .joined
                    .entry(flight_id)
                    .or_default()
                    .push(request_id.to_string());
                Ok(None)
            }
            QueryOnce::Fetch { flight_id, request } => {
                if self.connection.is_none() {
                    self.client.fail_query_once(&flight_id);
                    return Err(UNAVAILABLE.into());
                }
                let body = request.encode().map_err(|e| e.to_string())?;
                let body = String::from_utf8(body).map_err(|_| "utf8".to_string())?;
                if let Err(error) = self.send_direct(
                    request_id,
                    request.call.call_id,
                    body,
                    Some(flight_id.clone()),
                ) {
                    self.client.fail_query_once(&flight_id);
                    return Err(error);
                }
                Ok(None)
            }
        }
    }
    /// Ask for the request and its deadline; the task parks.
    fn send_direct(
        &mut self,
        request_id: &str,
        call_id: String,
        body: String,
        flight: Option<String>,
    ) -> std::result::Result<(), String> {
        let timeout = self
            .connection
            .as_ref()
            .map(|connection| connection.timeout)
            .ok_or(UNAVAILABLE)?;
        let http = self
            .issue_effect(
                EffectKind::DirectHttp {
                    request_id: request_id.to_string(),
                },
                Operation::Http {
                    route: HttpRoute::Action,
                    body: body.clone(),
                },
            )
            .ok_or(EXECUTION_UNKNOWN)?;
        let timer = self.issue_effect(
            EffectKind::DirectTimer {
                request_id: request_id.to_string(),
            },
            Operation::Timer { millis: timeout },
        );
        self.directs.calls.insert(
            request_id.to_string(),
            Call {
                call_id,
                body,
                http: Some(http),
                timer,
                retried: false,
                applying: false,
                flight,
            },
        );
        Ok(())
    }
    /// The request answered: the response waits for its apply unit and the
    /// deadline no longer applies. A 401 asks for one shared refresh and one
    /// more attempt; anything else leaves the execution unknown.
    pub(super) fn direct_result(&mut self, request_id: String, outcome: EffectOutcome) {
        let Some(call) = self.directs.calls.get_mut(&request_id) else {
            return;
        };
        call.http = None;
        match effects::http_body(outcome) {
            Ok(response) => {
                call.applying = true;
                if let Some(timer) = call.timer.take() {
                    self.cancel_effect(&timer);
                }
                self.ready.push_back(Ready::ApplyDirect {
                    request_id,
                    response,
                });
            }
            Err(error) => {
                let refresh = error.status == Some(401)
                    && !call.retried
                    && self
                        .connection
                        .as_ref()
                        .is_some_and(|connection| connection.refresh);
                if refresh {
                    call.retried = true;
                    self.join_refresh(Waiter::Direct { request_id });
                } else {
                    self.fail_direct(&request_id, EXECUTION_UNKNOWN, transport_failure(&error));
                }
            }
        }
    }
    /// The refresh succeeded: send the same body once more.
    pub(super) fn resend_direct(&mut self, request_id: &str) {
        let Some(body) = self.directs.calls.get(request_id).map(|c| c.body.clone()) else {
            return;
        };
        let http = self.issue_effect(
            EffectKind::DirectHttp {
                request_id: request_id.to_string(),
            },
            Operation::Http {
                route: HttpRoute::Action,
                body,
            },
        );
        match (http, self.directs.calls.get_mut(request_id)) {
            (Some(http), Some(call)) => call.http = Some(http),
            _ => self.fail_direct(request_id, EXECUTION_UNKNOWN, code(EXECUTION_UNKNOWN)),
        }
    }
    /// The deadline passed first: the request is abandoned and its execution
    /// is unknown. A late answer is fenced.
    pub(super) fn direct_timeout(&mut self, request_id: String) {
        if let Some(call) = self.directs.calls.get_mut(&request_id) {
            call.timer = None;
            let details = transport_failure(&EffectError {
                message: TIMED_OUT.into(),
                status: None,
            });
            self.fail_direct(&request_id, EXECUTION_UNKNOWN, details);
        }
    }
    /// Fail one call - and every caller joined to its flight - with `error`
    /// and its `details`, abandoning its effects and releasing its flight.
    pub(super) fn fail_direct(&mut self, request_id: &str, error: &str, details: Value) {
        let Some(call) = self.directs.calls.remove(request_id) else {
            return;
        };
        for effect_id in [call.http, call.timer].into_iter().flatten() {
            self.cancel_effect(&effect_id);
        }
        if let Some(connection) = &mut self.connection {
            connection
                .waiters
                .retain(|w| !matches!(w, Waiter::Direct { request_id: r } if r == request_id));
        }
        self.ready
            .retain(|r| !matches!(r, Ready::ApplyDirect { request_id: r, .. } if r == request_id));
        let joined = match &call.flight {
            Some(flight) => {
                self.client.fail_query_once(flight);
                self.directs.joined.remove(flight).unwrap_or_default()
            }
            None => vec![],
        };
        self.fail(request_id.to_string(), error, details.clone());
        for joined in joined {
            self.fail(joined, error, details.clone());
        }
    }
    /// Fail every call still waiting on the network (`stop`); a response that
    /// already arrived is local work and still applies.
    pub(super) fn fail_directs_in_flight(&mut self, error: &str) {
        let waiting: Vec<String> = self
            .directs
            .calls
            .iter()
            .filter(|(_, call)| !call.applying)
            .map(|(id, _)| id.clone())
            .collect();
        for request_id in waiting {
            self.fail_direct(&request_id, error, code(error));
        }
    }
    /// Fail every call and joined caller (close, rebuild).
    pub(super) fn fail_directs(&mut self, error: &str) {
        let all: Vec<String> = self.directs.calls.keys().cloned().collect();
        for request_id in all {
            self.fail_direct(&request_id, error, code(error));
        }
        // Joined callers whose fetcher is already gone.
        for (_, joined) in std::mem::take(&mut self.directs.joined) {
            for request_id in joined {
                self.fail(request_id, error, code(error));
            }
        }
    }
    /// Apply one response in one local transaction, then settle: every
    /// completion is announced after the commit, and the task - with every
    /// caller joined to its flight - completes with this call's own outcome.
    pub(super) fn apply_direct(&mut self, request_id: String, response: String) {
        let Some(call) = self.directs.calls.remove(&request_id) else {
            return;
        };
        let joined = call
            .flight
            .as_ref()
            .and_then(|flight| self.directs.joined.remove(flight))
            .unwrap_or_default();
        let generation = self.client.generation();
        let applied = match &call.flight {
            Some(flight) => self.client.finish_query_once(flight, response.as_bytes()),
            None => self
                .client
                .apply_action_response_bytes(call.body.as_bytes(), response.as_bytes()),
        };
        self.committed_since(generation);
        let outcome = match applied {
            Ok(report) => {
                self.settled(&report);
                report
                    .completions
                    .iter()
                    .find(|completion| completion.call_id == call.call_id)
                    .map(|completion| {
                        json!({"outcome": serde_json::to_value(&completion.outcome).unwrap_or(Value::Null)})
                    })
                    .ok_or((OBSERVATION_FAILED, code(OBSERVATION_FAILED)))
            }
            // Nothing was committed: the response cannot be observed, and
            // the apply's failure is its cause.
            Err(e) => Err((
                EXECUTION_UNKNOWN,
                transport_failure(&EffectError {
                    message: e.to_string(),
                    status: None,
                }),
            )),
        };
        for request_id in std::iter::once(request_id).chain(joined) {
            match &outcome {
                Ok(value) => self.complete(request_id, Ok(value.clone())),
                Err((error, details)) => self.fail(request_id, *error, details.clone()),
            }
        }
    }
}
