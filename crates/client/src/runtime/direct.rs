//! Independent protocol5 Query and Fetch requests with deadlines, shared auth
//! refresh, exact correlation, and transactional guarded cache installation.
use super::effects::{EffectKind, Ready, Waiter};
use super::*;
use crate::ClientStore;
use std::fmt::Display;

pub(super) const UNAVAILABLE: &str = "action.unavailable";
pub(super) const EXECUTION_UNKNOWN: &str = "action.execution_unknown";
pub(super) const OBSERVATION_FAILED: &str = "action.observation_failed";
pub(super) const INVALID_OPTIONS: &str = "action.invalid_options";
const TIMED_OUT: &str = "direct call timed out";

const FETCH_INVALID_OPTIONS: &str = "fetch.invalid_options";
pub(super) const FETCH_UNAVAILABLE: &str = "fetch.unavailable";
const FETCH_TIMEOUT: &str = "fetch.timeout";
const FETCH_TRANSPORT_FAILED: &str = "fetch.transport_failed";
const FETCH_INVALID_RESPONSE: &str = "fetch.invalid_response";
pub(super) const FETCH_STORE_FAILED: &str = "fetch.store_failed";

const FETCH_SCHEMA_CHANGED: &str = "fetch.schema_changed";

// The `details` of a direct failure with no known cause: `{"code"}`.
pub(super) fn code(error: &str) -> Value {
    json!({ "code": error })
}
// The `details` of an unknown execution caused by `cause`:
// `{"code":"action.execution_unknown","message","status"?}`.
pub(super) fn transport_failure(cause: &EffectError) -> Value {
    with_cause(EXECUTION_UNKNOWN, cause)
}

// The `details` of `error` caused by `cause`: `{"code","message","status"?}`.
fn with_cause(error: &str, cause: &EffectError) -> Value {
    let mut details = caused(error, &cause.message);
    if let Some(status) = cause.status {
        details["status"] = json!(status);
    }
    details
}

// The `details` of a failure with a known cause message: `{"code","message"}`.
fn caused(error: &str, cause: impl Display) -> Value {
    json!({ "code": error, "message": cause.to_string() })
}

// Why a direct call ended before its response applied. An Action call and a
// Fetch name the same lifecycle facts with their own codes.
#[derive(Clone)]
pub(super) enum Failure {
    // No connection, `stop` before the response, or close.
    Unavailable,
    // The deadline passed first.
    Timeout,
    // The request failed, or the credential refresh it waited for did.
    Transport(EffectError),
    // The runtime could not issue the request's effect.
    Lost,
    // A rebuild replaced the replica the call belongs to.
    Replaced,
    // The server refused this client's admission on the call's own request:
    // unavailable, with the refusal as its cause.
    Refused(EffectError),
}
impl Failure {
    // Action calls: unavailable, otherwise an unknown execution.
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
    // Fetches: a read claims nothing about side effects.
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

// What joins one Fetch flight: the same replica, Model, read version,
// canonical identity and normalized store policy.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct FetchKey {
    replica: u64,
    model: String,
    version: u64,
    identity: String,
    store: bool,
}

// One direct call in flight, by the request id of its task.
pub(super) struct Call {
    // The call id the response's completion must carry.
    call_id: String,
    // The exact request body: sent, re-sent once after a refresh, and what
    // the response is validated against.
    body: String,
    http: Option<String>,
    timer: Option<String>,
    // The one re-send after a credential refresh was used.
    retried: bool,
    // The response arrived and waits for its apply unit.
    applying: bool,
    // The Query once flight this call fetches for, if any.
    // The Fetch flight this call is, if it is a Fetch.
    fetch: Option<FetchKey>,
}

#[derive(Default)]
pub(super) struct Directs {
    calls: BTreeMap<String, Call>,
    // The requests joined to a flight another call fetches, by flight id.
    // Fetch flights: each key to the request id of the call that owns it.
    fetches: BTreeMap<FetchKey, String>,
    // The requests joined to a Fetch flight, by its owner's request id.
    fetch_joined: BTreeMap<String, Vec<String>>,
    // The replica generation: every rebuild starts a new one, so no Fetch
    // flight of a replaced replica can be joined.
    replica: u64,
}

// One `invoke` as the task named it.
pub(super) struct Invocation<'a> {
    pub(super) name: &'a str,
    pub(super) version: u64,
    pub(super) args: &'a Value,
    pub(super) store: &'a Option<Value>,
}

impl<S: ClientStore + 'static> ClientRuntime<S> {
    // `invoke {name, version, args, store?, once?, refresh?}`. `None` while
    // the task waits for its request.
    pub(super) fn invoke(
        &mut self,
        request_id: &str,
        invocation: Invocation<'_>,
    ) -> Option<std::result::Result<Value, String>> {
        self.begin_read05(
            request_id,
            crate::v05::ReadInvocation::Query {
                name: invocation.name.into(),
                version: invocation.version,
                args: invocation.args.clone(),
            },
            invocation.store,
        )
        .err()
        .map(Err)
    }

    // Ask for the request and its deadline; the task parks.
    fn send_direct(
        &mut self,
        request_id: &str,
        call_id: String,
        body: String,
        fetch: Option<FetchKey>,
    ) -> std::result::Result<(), String> {
        let Some(timeout) = self
            .connection
            .as_ref()
            .map(|connection| connection.timeout)
        else {
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
                fetch,
            },
        );
        Ok(())
    }
    // The request answered: the response waits for its apply unit and the
    // deadline no longer applies. A 401 asks for one shared refresh and one
    // more attempt; anything else leaves the execution unknown.
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
    // The refresh succeeded: send the same body once more.
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
    // The deadline passed first: the request is abandoned and its execution
    // is unknown. A late answer is fenced.
    pub(super) fn direct_timeout(&mut self, request_id: String) {
        if let Some(call) = self.directs.calls.get_mut(&request_id) {
            call.timer = None;
            self.fail_call(&request_id, Failure::Timeout);
        }
    }
    // Fail one call - and every caller joined to it - for a lifecycle
    // `failure`, under the codes of its kind.
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
    // Fail one call - and every caller joined to its flight - with `error`
    // and its `details`, abandoning its effects and releasing its flight.
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
        let joined = call
            .fetch
            .as_ref()
            .map_or_else(Vec::new, |key| self.release_fetch(key, request_id));
        self.fail(request_id.to_string(), error, details.clone());
        for joined in joined {
            self.fail(joined, error, details.clone());
        }
    }
    // Fail every call still waiting on the network (`stop`); a response that
    // already arrived is local work and still applies.
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
    // Fail every call and joined caller (close, rebuild).
    pub(super) fn fail_directs(&mut self, failure: Failure) {
        let all: Vec<String> = self.directs.calls.keys().cloned().collect();
        for request_id in all {
            self.fail_call(&request_id, failure.clone());
        }
        let (error, details) = failure.fetch();
        self.directs.fetches.clear();
        for (_, joined) in std::mem::take(&mut self.directs.fetch_joined) {
            for request_id in joined {
                self.fail(request_id, error, details.clone());
            }
        }
    }
    // A rebuild replaced the replica: every call of the old one fails, and
    // no later Fetch can join an old flight.
    pub(super) fn fence_directs(&mut self) {
        self.directs.replica += 1;
        self.fail_directs(Failure::Replaced);
    }
    // Apply one response in one local transaction, then settle: every
    // completion is announced after the commit, and the task - with every
    // caller joined to its flight - completes with this call's own outcome.
    pub(super) fn apply_direct(
        &mut self,
        request_id: String,
        response: String,
        _now: u64,
        _entropy: u64,
    ) {
        self.apply_read05(request_id, response)
    }

    // --- Model Fetch --------------------------------------------------------

    // `fetch {model, version, identity, store?}`: refuse invalid options
    // before any I/O, join an identical flight, or send a new request. The
    // task parks unless it was refused.
    pub(super) fn fetch(
        &mut self,
        request_id: &str,
        model: &str,
        version: u64,
        identity: &Value,
        store: &Option<Value>,
    ) {
        let invocation = crate::v05::ReadInvocation::Fetch {
            key: crate::v05::RecordKey {
                model: model.into(),
                identity: identity.clone(),
            },
            version,
        };
        if let Err(error) = self.begin_read05(request_id, invocation, store) {
            let kind = match error.as_str() {
                UNAVAILABLE => FETCH_UNAVAILABLE,
                "read.schema_pending" => "fetch.schema_pending",
                _ => FETCH_INVALID_OPTIONS,
            };
            self.fail(request_id.into(), kind, caused(kind, error));
        }
    }
    fn begin_read05(
        &mut self,
        request_id: &str,
        invocation: crate::v05::ReadInvocation,
        store: &Option<Value>,
    ) -> std::result::Result<(), String> {
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
            crate::v05::ReadInvocation::Fetch { key, version } => {
                if self
                    .client
                    .schema
                    .result_model(&key.model, *version)
                    .is_err()
                    && !self
                        .client
                        .schema
                        .models
                        .iter()
                        .any(|m| m.name == key.model && m.version == *version)
                {
                    return Err(format!(
                        "unsupported Fetch Model {} v{}",
                        key.model, version
                    ));
                }
                self.client
                    .schema
                    .record_key(&key.model, &key.identity)
                    .map_err(|e| e.to_string())?;
            }
        }
        if self.connection.is_none() {
            return Err(UNAVAILABLE.into());
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
        self.send_direct(request_id, request.request_id, body, fetch)
    }
    fn apply_read05(&mut self, request_id: String, response: String) {
        let Some(call) = self.directs.calls.remove(&request_id) else {
            return;
        };
        let fetch = call.fetch.is_some();
        let mut snapshot_mismatch = false;
        let decoded = (|| {
            let request = crate::v05::decode::<crate::v05::ReadRequest>(call.body.as_bytes())?;
            let mut response = crate::v05::decode::<crate::v05::ReadResponse>(response.as_bytes())?;
            let active = self.client.request_context05()?;
            // Correlation and snapshot agreement use the caller's retained read
            // contract. The cache record remains its separate local projection.
            response.admit_correlation(&request, &active)?;
            let mut admitted = response.clone();
            if let crate::v05::ReadInvocation::Fetch { key, version } = &request.invocation
                && let crate::v05::ReadOutcome::Succeeded { result } = &mut response.outcome
            {
                *result = axton_core::normalize_read_snapshot(
                    &self.client.schema,
                    &key.model,
                    *version,
                    result,
                )?;
                if !result.is_null()
                    && key
                        .identity
                        .as_object()
                        .unwrap()
                        .iter()
                        .any(|(field, value)| result.get(field) != Some(value))
                {
                    return Err(crate::invalid("Fetch result identity mismatch"));
                }
                if self.client.schema.model(&key.model)?.version == *version
                    && let [record] = admitted.records.as_mut_slice()
                    && record.key == *key
                    && !record.state.as_object().is_some_and(|state| {
                        key.identity
                            .as_object()
                            .unwrap()
                            .keys()
                            .any(|field| state.contains_key(field))
                    })
                {
                    let full = if record.state.is_null() {
                        Value::Null
                    } else {
                        crate::rows::merge_identity(&key.identity, &record.state)
                    };
                    let expected = axton_core::normalize_read_snapshot(
                        &self.client.schema,
                        &key.model,
                        *version,
                        &full,
                    )?;
                    snapshot_mismatch = *result != expected;
                    record.state = if expected.is_null() {
                        Value::Null
                    } else {
                        Value::Object(
                            expected
                                .as_object()
                                .unwrap()
                                .iter()
                                .filter(|(field, _)| {
                                    !key.identity.as_object().unwrap().contains_key(*field)
                                })
                                .map(|(field, value)| (field.clone(), value.clone()))
                                .collect(),
                        )
                    };
                }
                admitted.outcome = crate::v05::ReadOutcome::Succeeded {
                    result: result.clone(),
                };
            }
            if let crate::v05::ReadInvocation::Fetch { key, version } = &request.invocation
                && self.client.schema.model(&key.model)?.version == *version
            {
                admitted.admit(&request, &active)?;
            }
            if request.store {
                for record in &response.records {
                    self.client
                        .schema
                        .record_key(&record.key.model, &record.key.identity)?;
                    if !record.state.is_null() {
                        self.client
                            .schema
                            .validate_state(&record.key.model, &record.state)?;
                    }
                }
            }
            Ok::<_, crate::Error>((request, response))
        })();
        let (request, response) = match decoded {
            Ok(value) => value,
            Err(error) => {
                let kind = if fetch && snapshot_mismatch {
                    FETCH_STORE_FAILED
                } else if fetch {
                    FETCH_INVALID_RESPONSE
                } else {
                    EXECUTION_UNKNOWN
                };
                self.fail(request_id, kind, caused(kind, error));
                return;
            }
        };
        let generation = self.client.generation();
        if request.store
            && let Err(error) = self.client.install_cache05(&response.records, true)
        {
            let kind = if fetch {
                FETCH_STORE_FAILED
            } else {
                OBSERVATION_FAILED
            };
            self.fail(request_id, kind, caused(kind, error));
            return;
        }
        self.committed_since(generation);
        let outcome = match response.outcome {
            crate::v05::ReadOutcome::Succeeded { result } => {
                json!({"status":"succeeded","result":result})
            }
            crate::v05::ReadOutcome::Failed { code, message } => {
                json!({"status":"failed","code":code,"message":message,"execution":"rejected"})
            }
        };
        if !fetch {
            self.events.push(Event::CallCompleted {
                call_id: call.call_id,
                outcome: outcome.clone(),
            });
        }
        self.complete(request_id, Ok(json!({"outcome":outcome})));
    }
    // Forget one Fetch flight and answer the callers joined to it.
    fn release_fetch(&mut self, key: &FetchKey, owner: &str) -> Vec<String> {
        if self.directs.fetches.get(key).map(String::as_str) == Some(owner) {
            self.directs.fetches.remove(key);
        }
        self.directs.fetch_joined.remove(owner).unwrap_or_default()
    }
}

// The route of a direct call's request.
fn route(fetch: &Option<FetchKey>) -> HttpRoute {
    match fetch {
        Some(_) => HttpRoute::Fetch,
        None => HttpRoute::Action,
    }
}
