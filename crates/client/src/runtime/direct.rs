//! Direct Query and Mutation calls, Query once flights and Model Fetches as
//! runtime tasks ([#134](https://github.com/zanminwang/axton/issues/134),
//! [#158](https://github.com/zanminwang/axton/issues/158),
//! [#153](https://github.com/zanminwang/axton/issues/153)).
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
//!
//! A `fetch` task shares that lifecycle - frozen body, `fetch` route effect,
//! deadline, one shared refresh, response admission, stop and close - under
//! its own codes (`fetch.*`, [`Failure`]). Identical overlapping Fetches join
//! one flight, keyed in memory by replica, Model, read version, canonical
//! identity and store policy; the flight and every joined caller are released
//! on every terminal path, so a later call reads again. A read failure claims
//! nothing about side effects. A stored response is validated first, then
//! applied as one single-record delivery - through the onStore transaction
//! when its Model has a hook - and every joined caller answers after that
//! commit. A draining replica refuses Fetch (`fetch.schema_pending`) and a
//! rebuild fails the old replica's Fetches (`fetch.schema_changed`).
use super::effects::{EffectKind, Ready, Waiter};
use super::transactions::StoreContinuation;
use super::*;
use crate::{
    ActionCallOptions, ApplyReport, ClientStore, QueryOnce, QueryOnceOptions, StoreDelivery,
};
use std::fmt::Display;

pub(super) const UNAVAILABLE: &str = "action.unavailable";
pub(super) const EXECUTION_UNKNOWN: &str = "action.execution_unknown";
pub(super) const OBSERVATION_FAILED: &str = "action.observation_failed";
pub(super) const INVALID_OPTIONS: &str = "action.invalid_options";
const TIMED_OUT: &str = "direct call timed out";

const FETCH_INVALID_OPTIONS: &str = "fetch.invalid_options";
const FETCH_UNAVAILABLE: &str = "fetch.unavailable";
const FETCH_TIMEOUT: &str = "fetch.timeout";
const FETCH_TRANSPORT_FAILED: &str = "fetch.transport_failed";
const FETCH_INVALID_RESPONSE: &str = "fetch.invalid_response";
pub(super) const FETCH_STORE_FAILED: &str = "fetch.store_failed";
const FETCH_SCHEMA_PENDING: &str = "fetch.schema_pending";
const FETCH_SCHEMA_CHANGED: &str = "fetch.schema_changed";

/// The `details` of a direct failure with no known cause: `{"code"}`.
pub(super) fn code(error: &str) -> Value {
    json!({ "code": error })
}
/// The `details` of an unknown execution caused by `cause`:
/// `{"code":"action.execution_unknown","message","status"?}`.
pub(super) fn transport_failure(cause: &EffectError) -> Value {
    with_cause(EXECUTION_UNKNOWN, cause)
}

/// The `details` of `error` caused by `cause`: `{"code","message","status"?}`.
fn with_cause(error: &str, cause: &EffectError) -> Value {
    let mut details = caused(error, &cause.message);
    if let Some(status) = cause.status {
        details["status"] = json!(status);
    }
    details
}

/// The `details` of a failure with a known cause message: `{"code","message"}`.
fn caused(error: &str, cause: impl Display) -> Value {
    json!({ "code": error, "message": cause.to_string() })
}

/// Why a direct call ended before its response applied. An Action call and a
/// Fetch name the same lifecycle facts with their own codes.
#[derive(Clone)]
pub(super) enum Failure {
    /// No connection, `stop` before the response, or close.
    Unavailable,
    /// The deadline passed first.
    Timeout,
    /// The request failed, or the credential refresh it waited for did.
    Transport(EffectError),
    /// The runtime could not issue the request's effect.
    Lost,
    /// A rebuild replaced the replica the call belongs to.
    Replaced,
    /// The server refused this client's admission on the call's own request:
    /// unavailable, with the refusal as its cause.
    Refused(EffectError),
}
impl Failure {
    /// Action calls: unavailable, otherwise an unknown execution.
    fn action(&self) -> (&'static str, Value) {
        match self {
            Self::Unavailable => (UNAVAILABLE, code(UNAVAILABLE)),
            Self::Timeout => (
                EXECUTION_UNKNOWN,
                transport_failure(&EffectError {
                    message: TIMED_OUT.into(),
                    status: None,
                    refusal: None,
                    retry: false,
                }),
            ),
            Self::Transport(cause) => (EXECUTION_UNKNOWN, transport_failure(cause)),
            Self::Lost | Self::Replaced => (EXECUTION_UNKNOWN, code(EXECUTION_UNKNOWN)),
            Self::Refused(cause) => (UNAVAILABLE, with_cause(UNAVAILABLE, cause)),
        }
    }
    /// Fetches: a read claims nothing about side effects.
    fn fetch(&self) -> (&'static str, Value) {
        match self {
            Self::Unavailable => (FETCH_UNAVAILABLE, code(FETCH_UNAVAILABLE)),
            Self::Timeout => (FETCH_TIMEOUT, caused(FETCH_TIMEOUT, TIMED_OUT)),
            Self::Transport(cause) => (
                FETCH_TRANSPORT_FAILED,
                with_cause(FETCH_TRANSPORT_FAILED, cause),
            ),
            Self::Lost => (FETCH_TRANSPORT_FAILED, code(FETCH_TRANSPORT_FAILED)),
            Self::Replaced => (FETCH_SCHEMA_CHANGED, code(FETCH_SCHEMA_CHANGED)),
            Self::Refused(cause) => (FETCH_UNAVAILABLE, with_cause(FETCH_UNAVAILABLE, cause)),
        }
    }
}

/// What joins one Fetch flight: the same replica, Model, read version,
/// canonical identity and normalized store policy.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct FetchKey {
    replica: u64,
    model: String,
    version: u64,
    identity: String,
    store: bool,
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
    /// The Fetch flight this call is, if it is a Fetch.
    fetch: Option<FetchKey>,
}

#[derive(Default)]
pub(super) struct Directs {
    calls: BTreeMap<String, Call>,
    /// The requests joined to a flight another call fetches, by flight id.
    joined: BTreeMap<String, Vec<String>>,
    /// Fetch flights: each key to the request id of the call that owns it.
    fetches: BTreeMap<FetchKey, String>,
    /// The requests joined to a Fetch flight, by its owner's request id.
    fetch_joined: BTreeMap<String, Vec<String>>,
    /// The replica generation: every rebuild starts a new one, so no Fetch
    /// flight of a replaced replica can be joined.
    replica: u64,
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
        if self.protocol05 {
            let outcome = self.begin_read05(
                request_id,
                crate::v05::ReadInvocation::Query {
                    name: invocation.name.into(),
                    version: invocation.version,
                    args: invocation.args.clone(),
                },
                invocation.store,
            );
            return outcome.err().map(Err);
        }
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
                    ActionCallOptions {
                        store: store.clone(),
                    },
                )
                .map_err(|e| e.to_string())?;
            let body = if self.client.request_context().is_ok() {
                let request = self
                    .client
                    .freeze_query04(&prepared, &store)
                    .map_err(|e| e.to_string())?;
                axton_core::v04::encode(&request).map_err(|e| e.to_string())?
            } else {
                prepared.encode().map_err(|e| e.to_string())?
            };
            let body = String::from_utf8(body).map_err(|_| "utf8".to_string())?;
            self.send_direct(request_id, prepared.call.call_id, body, None, None)?;
            return Ok(None);
        }
        let decision = self
            .client
            .begin_query_once(
                name,
                version,
                args,
                &QueryOnceOptions {
                    store: store.clone(),
                    refresh,
                },
            )
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
                // Until dispatch succeeds, this scope owns the flight and its
                // transient request token, including fallible durable freezing.
                let dispatched = (|| {
                    if self.connection.is_none() {
                        return Err(UNAVAILABLE.into());
                    }
                    let body = if self.client.request_context().is_ok() {
                        let read = self
                            .client
                            .freeze_query04(&request, &store)
                            .map_err(|e| e.to_string())?;
                        axton_core::v04::encode(&read).map_err(|e| e.to_string())?
                    } else {
                        request.encode().map_err(|e| e.to_string())?
                    };
                    let body = String::from_utf8(body).map_err(|_| "utf8".to_string())?;
                    self.send_direct(
                        request_id,
                        request.call.call_id,
                        body,
                        Some(flight_id.clone()),
                        None,
                    )
                })();
                if let Err(error) = dispatched {
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
        fetch: Option<FetchKey>,
    ) -> std::result::Result<(), String> {
        let Some(timeout) = self
            .connection
            .as_ref()
            .map(|connection| connection.timeout)
        else {
            self.client.retire_request(&call_id);
            return Err(UNAVAILABLE.into());
        };
        let http = self.issue_effect(
            EffectKind::DirectHttp {
                request_id: request_id.to_string(),
            },
            Operation::Http {
                route: route(&fetch),
                body: body.clone(),
            },
        );
        let Some(http) = http else {
            self.client.retire_request(&call_id);
            return Err(EXECUTION_UNKNOWN.into());
        };
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
                fetch,
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
                    self.fail_call(&request_id, Failure::Transport(error));
                }
            }
        }
    }
    /// The refresh succeeded: send the same body once more.
    pub(super) fn resend_direct(&mut self, request_id: &str) {
        let Some((body, route)) = self
            .directs
            .calls
            .get(request_id)
            .map(|c| (c.body.clone(), route(&c.fetch)))
        else {
            return;
        };
        let http = self.issue_effect(
            EffectKind::DirectHttp {
                request_id: request_id.to_string(),
            },
            Operation::Http { route, body },
        );
        match (http, self.directs.calls.get_mut(request_id)) {
            (Some(http), Some(call)) => call.http = Some(http),
            _ => self.fail_call(request_id, Failure::Lost),
        }
    }
    /// The deadline passed first: the request is abandoned and its execution
    /// is unknown. A late answer is fenced.
    pub(super) fn direct_timeout(&mut self, request_id: String) {
        if let Some(call) = self.directs.calls.get_mut(&request_id) {
            call.timer = None;
            self.fail_call(&request_id, Failure::Timeout);
        }
    }
    /// Fail one call - and every caller joined to it - for a lifecycle
    /// `failure`, under the codes of its kind.
    pub(super) fn fail_call(&mut self, request_id: &str, failure: Failure) {
        let Some(call) = self.directs.calls.get(request_id) else {
            return;
        };
        let (error, details) = match call.fetch {
            Some(_) => failure.fetch(),
            None => failure.action(),
        };
        self.fail_direct(request_id, error, details);
    }
    /// Fail one call - and every caller joined to its flight - with `error`
    /// and its `details`, abandoning its effects and releasing its flight.
    pub(super) fn fail_direct(&mut self, request_id: &str, error: &str, details: Value) {
        let Some(call) = self.directs.calls.remove(request_id) else {
            return;
        };
        self.client.retire_request(&call.call_id);
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
        let joined = match (&call.flight, &call.fetch) {
            (Some(flight), _) => {
                self.client.fail_query_once(flight);
                self.directs.joined.remove(flight).unwrap_or_default()
            }
            (None, Some(key)) => self.release_fetch(key, request_id),
            (None, None) => vec![],
        };
        self.fail(request_id.to_string(), error, details.clone());
        for joined in joined {
            self.fail(joined, error, details.clone());
        }
    }
    /// Fail every call still waiting on the network (`stop`); a response that
    /// already arrived is local work and still applies.
    pub(super) fn fail_directs_in_flight(&mut self, failure: Failure) {
        let waiting: Vec<String> = self
            .directs
            .calls
            .iter()
            .filter(|(_, call)| !call.applying)
            .map(|(id, _)| id.clone())
            .collect();
        for request_id in waiting {
            self.fail_call(&request_id, failure.clone());
        }
    }
    /// Fail every call and joined caller (close, rebuild).
    pub(super) fn fail_directs(&mut self, failure: Failure) {
        let all: Vec<String> = self.directs.calls.keys().cloned().collect();
        for request_id in all {
            self.fail_call(&request_id, failure.clone());
        }
        // Joined callers whose fetcher is already gone.
        let (error, details) = failure.action();
        for (_, joined) in std::mem::take(&mut self.directs.joined) {
            for request_id in joined {
                self.fail(request_id, error, details.clone());
            }
        }
        let (error, details) = failure.fetch();
        self.directs.fetches.clear();
        for (_, joined) in std::mem::take(&mut self.directs.fetch_joined) {
            for request_id in joined {
                self.fail(request_id, error, details.clone());
            }
        }
    }
    /// A rebuild replaced the replica: every call of the old one fails, and
    /// no later Fetch can join an old flight.
    pub(super) fn fence_directs(&mut self) {
        self.directs.replica += 1;
        self.fail_directs(Failure::Replaced);
    }
    /// Apply one response in one local transaction, then settle: every
    /// completion is announced after the commit, and the task - with every
    /// caller joined to its flight - completes with this call's own outcome.
    pub(super) fn apply_direct(
        &mut self,
        request_id: String,
        response: String,
        now: u64,
        entropy: u64,
    ) {
        if self.protocol05 {
            return self.apply_read05(request_id, response);
        }
        if self
            .directs
            .calls
            .get(&request_id)
            .is_some_and(|call| call.fetch.is_some())
        {
            return self.apply_fetch(request_id, response, now, entropy);
        }
        let candidate = self.client.request_context().is_err()
            && serde_json::from_str::<Value>(&response)
                .ok()
                .and_then(|raw| raw["records"].as_array().cloned())
                .is_some_and(|records| {
                    self.has_store_hook_candidate(
                        records
                            .into_iter()
                            .filter_map(|record| record["model"].as_str().map(str::to_string)),
                    )
                });
        if candidate {
            let Some(call) = self.directs.calls.get(&request_id) else {
                return;
            };
            let delivery = match &call.flight {
                Some(flight) => self
                    .client
                    .decode_query_once_store(flight, response.as_bytes()),
                None => self
                    .client
                    .decode_direct_store(call.body.as_bytes(), response.as_bytes()),
            };
            match delivery {
                Ok(delivery @ StoreDelivery::Direct { .. }) => self.open_store(
                    delivery,
                    StoreContinuation::Direct { request_id },
                    now,
                    entropy,
                ),
                Err(error) => self.fail_direct(
                    &request_id,
                    EXECUTION_UNKNOWN,
                    transport_failure(&EffectError {
                        message: error.to_string(),
                        status: None,
                        refusal: None,
                        retry: false,
                    }),
                ),
                _ => unreachable!(),
            }
            return;
        }
        let Some(call) = self.directs.calls.remove(&request_id) else {
            return;
        };
        let joined = call
            .flight
            .as_ref()
            .and_then(|flight| self.directs.joined.remove(flight))
            .unwrap_or_default();
        let generation = self.client.generation();
        let applied = if self.client.request_context().is_ok() {
            axton_core::v04::decode::<axton_core::v04::ReadIntent>(call.body.as_bytes()).and_then(
                |request| {
                    let response = axton_core::v04::decode::<axton_core::v04::ReadResponse>(
                        response.as_bytes(),
                    )?;
                    match &call.flight {
                        Some(flight) => {
                            self.client.finish_query_once04(flight, &request, &response)
                        }
                        None => self.client.apply_query04(&request, &response, None),
                    }
                },
            )
        } else {
            match &call.flight {
                Some(flight) => self.client.finish_query_once(flight, response.as_bytes()),
                None => self
                    .client
                    .apply_action_response_bytes(call.body.as_bytes(), response.as_bytes()),
            }
        };
        self.client.retire_request(&call.call_id);
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
                    refusal: None,
                    retry: false,
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
    /// Settle the still-owned call and every joined once caller after the
    /// authority and cache snapshot have committed.
    pub(super) fn finish_direct_store(&mut self, request_id: String, report: ApplyReport) {
        let Some(call) = self.directs.calls.remove(&request_id) else {
            return;
        };
        self.client.retire_request(&call.call_id);
        let joined = call
            .flight
            .as_ref()
            .and_then(|flight| {
                self.client.fail_query_once(flight);
                self.directs.joined.remove(flight)
            })
            .unwrap_or_default();
        self.settled(&report);
        let outcome = report.completions.iter()
            .find(|completion| completion.call_id == call.call_id)
            .map(|completion| json!({"outcome": serde_json::to_value(&completion.outcome).unwrap_or(Value::Null)}))
            .ok_or((OBSERVATION_FAILED, code(OBSERVATION_FAILED)));
        for id in std::iter::once(request_id).chain(joined) {
            match &outcome {
                Ok(value) => self.complete(id, Ok(value.clone())),
                Err((error, details)) => self.fail(id, *error, details.clone()),
            }
        }
    }

    // --- Model Fetch --------------------------------------------------------

    /// `fetch {model, version, identity, store?}`: refuse invalid options
    /// before any I/O, join an identical flight, or send a new request. The
    /// task parks unless it was refused.
    pub(super) fn fetch(
        &mut self,
        request_id: &str,
        model: &str,
        version: u64,
        identity: &Value,
        store: &Option<Value>,
    ) {
        if self.protocol05 {
            if let Err(error) = self.begin_read05(
                request_id,
                crate::v05::ReadInvocation::Fetch {
                    key: crate::v05::RecordKey {
                        model: model.into(),
                        identity: identity.clone(),
                    },
                    version,
                },
                store,
            ) {
                self.complete(request_id.into(), Err(error));
            }
            return;
        }
        let refuse = |error: &str, cause: &dyn Display| (error.to_string(), caused(error, cause));
        let started = (|| {
            let store = match store {
                None => true,
                Some(Value::Bool(store)) => *store,
                Some(_) => {
                    return Err(refuse(
                        FETCH_INVALID_OPTIONS,
                        &"Fetch store must be a boolean",
                    ));
                }
            };
            // A replica draining for an incompatible schema neither stores
            // nor reads the application's schema.
            if !self.client.store_hooks_active() {
                return Err((FETCH_SCHEMA_PENDING.into(), code(FETCH_SCHEMA_PENDING)));
            }
            let (call_id, identity, body) = if self.client.request_context().is_ok() {
                let request = self
                    .client
                    .prepare_fetch04(model, version, identity, store)
                    .map_err(|e| refuse(FETCH_INVALID_OPTIONS, &e))?;
                let body = axton_core::v04::encode(&request)
                    .map_err(|e| refuse(FETCH_INVALID_OPTIONS, &e))?;
                (request.call_id, request.identity, body)
            } else {
                let request = self
                    .client
                    .prepare_fetch(model, version, identity, store)
                    .map_err(|e| refuse(FETCH_INVALID_OPTIONS, &e))?;
                let body = request
                    .encode()
                    .map_err(|e| refuse(FETCH_INVALID_OPTIONS, &e))?;
                (request.call_id, request.identity, body)
            };
            let key = FetchKey {
                replica: self.directs.replica,
                model: model.into(),
                version,
                identity: crate::canonical_json(&identity)
                    .map_err(|e| refuse(FETCH_INVALID_OPTIONS, &e))?,
                store,
            };
            if let Some(owner) = self.directs.fetches.get(&key).cloned() {
                // The join uses the owning flight's original token.
                self.client.retire_request(&call_id);
                self.directs
                    .fetch_joined
                    .entry(owner)
                    .or_default()
                    .push(request_id.to_string());
                return Ok(());
            }
            if self.connection.is_none() {
                self.client.retire_request(&call_id);
                return Err((FETCH_UNAVAILABLE.into(), code(FETCH_UNAVAILABLE)));
            }
            let body = String::from_utf8(body).map_err(|e| refuse(FETCH_INVALID_OPTIONS, &e))?;
            self.send_direct(request_id, call_id, body, None, Some(key.clone()))
                .map_err(|_| (FETCH_UNAVAILABLE.to_string(), code(FETCH_UNAVAILABLE)))?;
            self.directs.fetches.insert(key, request_id.to_string());
            Ok(())
        })();
        if let Err((error, details)) = started {
            self.fail(request_id.to_string(), error, details);
        }
    }
    fn begin_read05(
        &mut self,
        request_id: &str,
        invocation: crate::v05::ReadInvocation,
        store: &Option<Value>,
    ) -> std::result::Result<(), String> {
        if self.connection.is_none() {
            return Err(UNAVAILABLE.into());
        }
        if self
            .client
            .pending_schema05()
            .map_err(|e| e.to_string())?
            .is_some()
        {
            return Err("read.schema_pending".into());
        }
        let store = match store {
            None => true,
            Some(Value::Bool(value)) => *value,
            _ => return Err(INVALID_OPTIONS.into()),
        };
        match &invocation {
            crate::v05::ReadInvocation::Query { name, version, .. } => {
                if self
                    .client
                    .schema
                    .action(name, *version)
                    .map_err(|e| e.to_string())?
                    .kind
                    != crate::CallKind::Query
                {
                    return Err("invoke requires Query".into());
                }
            }
            crate::v05::ReadInvocation::Fetch { key, .. } => {
                self.client
                    .schema
                    .record_key(&key.model, &key.identity)
                    .map_err(|e| e.to_string())?;
            }
        }
        let request = crate::v05::ReadRequest {
            context: self.client.request_context05().map_err(|e| e.to_string())?,
            request_id: self.capability_token("read", self.issued + 1),
            store,
            invocation,
        };
        let body = String::from_utf8(crate::v05::encode(&request).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
        let fetch = match &request.invocation {
            crate::v05::ReadInvocation::Fetch { key, version } => Some(FetchKey {
                replica: self.directs.replica,
                model: key.model.clone(),
                version: *version,
                identity: crate::canonical_json(&key.identity).map_err(|e| e.to_string())?,
                store,
            }),
            _ => None,
        };
        self.send_direct(request_id, request.request_id, body, None, fetch)
    }
    fn apply_read05(&mut self, request_id: String, response: String) {
        let Some(call) = self.directs.calls.remove(&request_id) else {
            return;
        };
        let generation = self.client.generation();
        let outcome = (|| {
            let request = crate::v05::decode::<crate::v05::ReadRequest>(call.body.as_bytes())?;
            let response = crate::v05::decode::<crate::v05::ReadResponse>(response.as_bytes())?;
            response.admit(&request, &self.client.request_context05()?)?;
            if request.store {
                self.client.install_cache05(&response.records, true)?;
            }
            Ok::<_, crate::Error>(match response.outcome {
                crate::v05::ReadOutcome::Succeeded { result } => {
                    json!({"outcome":{"kind":"succeeded","result":result}})
                }
                crate::v05::ReadOutcome::Failed { code, message } => {
                    json!({"outcome":{"kind":"failed","code":code,"message":message}})
                }
            })
        })();
        self.committed_since(generation);
        self.complete(request_id, outcome.map_err(|e| e.to_string()));
    }
    /// Forget one Fetch flight and answer the callers joined to it.
    fn release_fetch(&mut self, key: &FetchKey, owner: &str) -> Vec<String> {
        if self.directs.fetches.get(key).map(String::as_str) == Some(owner) {
            self.directs.fetches.remove(key);
        }
        self.directs.fetch_joined.remove(owner).unwrap_or_default()
    }
    /// Validate one Fetch response against its frozen request, then store its
    /// authority as one delivery - through the onStore transaction when its
    /// Model has a hook - before any caller hears the outcome. A response
    /// without authority (`store: false`, or the backend's refusal) stores
    /// nothing.
    fn apply_fetch(&mut self, request_id: String, response: String, now: u64, entropy: u64) {
        let Some(call) = self.directs.calls.get(&request_id) else {
            return;
        };
        if !self.client.store_hooks_active() {
            return self.fail_direct(
                &request_id,
                FETCH_SCHEMA_PENDING,
                code(FETCH_SCHEMA_PENDING),
            );
        }
        if self.client.request_context().is_ok() {
            let decoded =
                axton_core::v04::decode::<axton_core::v04::FetchIntent>(call.body.as_bytes())
                    .and_then(|request| {
                        axton_core::v04::decode::<axton_core::v04::ReadResponse>(
                            response.as_bytes(),
                        )
                        .map(|response| (request, response))
                    });
            let (request, response) = match decoded {
                Ok(pair) => pair,
                Err(error) => {
                    return self.fail_direct(
                        &request_id,
                        FETCH_INVALID_RESPONSE,
                        caused(FETCH_INVALID_RESPONSE, error),
                    );
                }
            };
            let generation = self.client.generation();
            let applied = self.client.apply_fetch04(&request, &response);
            self.committed_since(generation);
            return match applied {
                Ok(report) => self.finish_fetch(request_id, report),
                Err(error) => self.fail_direct(
                    &request_id,
                    FETCH_STORE_FAILED,
                    caused(FETCH_STORE_FAILED, error),
                ),
            };
        }
        let (request, response) = match self
            .client
            .decode_fetch(call.body.as_bytes(), response.as_bytes())
        {
            Ok(decoded) => decoded,
            Err(error) => {
                return self.fail_direct(
                    &request_id,
                    FETCH_INVALID_RESPONSE,
                    caused(FETCH_INVALID_RESPONSE, error),
                );
            }
        };
        if !response.records.is_empty()
            && self.has_store_hook_candidate(std::iter::once(request.model))
        {
            return self.open_store(
                StoreDelivery::Fetch { response },
                StoreContinuation::Fetch { request_id },
                now,
                entropy,
            );
        }
        let generation = self.client.generation();
        let applied = self.client.apply_fetch_response(&response);
        self.committed_since(generation);
        match applied {
            Ok(report) => self.finish_fetch(request_id, report),
            Err(error) => self.fail_direct(
                &request_id,
                FETCH_STORE_FAILED,
                caused(FETCH_STORE_FAILED, error),
            ),
        }
    }
    /// The response's authority committed (or there was none): release the
    /// flight and answer the owner and every joined caller with the
    /// invocation's own outcome, never a reread of the local row. A Fetch is
    /// no durable call, so no `callCompleted` is announced.
    pub(super) fn finish_fetch(&mut self, request_id: String, mut report: ApplyReport) {
        let Some(call) = self.directs.calls.remove(&request_id) else {
            return;
        };
        self.client.retire_request(&call.call_id);
        let joined = match &call.fetch {
            Some(key) => self.release_fetch(key, &request_id),
            None => vec![],
        };
        let completions = std::mem::take(&mut report.completions);
        self.settled(&report);
        let outcome = completions
            .iter()
            .find(|completion| completion.call_id == call.call_id)
            .map(|completion| json!({ "outcome": completion.outcome }));
        for id in std::iter::once(request_id).chain(joined) {
            match &outcome {
                Some(value) => self.complete(id, Ok(value.clone())),
                None => self.fail(id, FETCH_INVALID_RESPONSE, code(FETCH_INVALID_RESPONSE)),
            }
        }
    }
}

/// The route of a direct call's request.
fn route(fetch: &Option<FetchKey>) -> HttpRoute {
    match fetch {
        Some(_) => HttpRoute::Fetch,
        None => HttpRoute::Action,
    }
}
