use axton_server::live::Subscriptions;
use napi::{bindgen_prelude::*, threadsafe_function::ThreadsafeFunction};
use napi_derive::napi;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{Mutex, MutexGuard, OnceLock},
};
struct CallbackHost(ThreadsafeFunction<String, Promise<String>, String, Status, false>);
impl axton_server::Host for CallbackHost {
    fn call(
        &self,
        request: Value,
    ) -> Pin<Box<dyn Future<Output = axton_server::HostResult<Value>> + Send + '_>> {
        Box::pin(async move {
            let returned = self
                .0
                .call_async_catch(request.to_string())
                .await
                .map_err(|e| e.to_string())?
                .await
                .map_err(|e| e.to_string())?;
            serde_json::from_str(&returned).map_err(|e| e.to_string())
        })
    }
}
/// The engine error crosses N-API as its JSON encoding in the reason string;
/// the server SDK decodes it back into `{code, message, details}`.
fn reason(error: axton_server::Error) -> Error {
    Error::from_reason(serde_json::to_string(&error).unwrap_or_else(|_| error.to_string()))
}
fn internal(error: impl std::fmt::Display) -> Error {
    reason(axton_server::Error::new(
        axton_server::code::INTERNAL,
        error.to_string(),
    ))
}
fn config(raw: &str) -> Result<axton_server::Config> {
    axton_server::Config::decode(serde_json::from_str(raw).map_err(|e| {
        reason(axton_server::Error::new(
            axton_server::code::CONFIG_INVALID,
            e.to_string(),
        ))
    })?)
    .map_err(reason)
}
#[napi]
pub fn validate_config(config_json: String) -> Result<()> {
    config(&config_json).map(|_| ())
}
#[napi]
pub async fn process_push(
    config_json: String,
    owner: String,
    request_json: String,
    callback: ThreadsafeFunction<String, Promise<String>, String, Status, false>,
) -> Result<String> {
    let decoded = config(&config_json)?;
    let action_batch = serde_json::from_str::<Value>(&request_json).ok()
        .and_then(|request| request.get("mutations")?.as_array().cloned())
        .is_some_and(|calls| calls.iter().any(|call| call.get("callId").is_some()));
    if action_batch {
        axton_server::process_action_push(&decoded, &owner, request_json.as_bytes(), &CallbackHost(callback)).await.map_err(reason)
    } else {
        axton_server::process_push(&decoded, &owner, request_json.as_bytes(), &CallbackHost(callback)).await.map_err(reason)
    }
}
#[napi]
pub async fn process_action(
    config_json: String,
    owner: String,
    request_json: String,
    callback: ThreadsafeFunction<String, Promise<String>, String, Status, false>,
) -> Result<String> {
    axton_server::process_action(&config(&config_json)?, &owner, request_json.as_bytes(), &CallbackHost(callback)).await.map_err(reason)
}
/// Structural validation of one `POST /sync/loads` envelope; answers each
/// item's canonical JSON in request order. The carrier runs each item through
/// `process_load` in its own transaction and never accepts a looser envelope.
#[napi]
pub fn validate_load_batch(request_json: String) -> Result<Vec<String>> {
    axton_server::validate_load_batch(request_json.as_bytes()).map_err(reason)
}
/// What escaped one Load item's transaction: `kind` is `engine` (with the
/// engine error's `code` and `message`), `conflict` or `unavailable`.
#[napi(object)]
pub struct LoadFault {
    pub kind: String,
    pub code: Option<String>,
    pub message: Option<String>,
}
/// One Load item as its transaction ended: exactly one of the page
/// `processLoad` answered, or the fault that escaped the transaction.
#[napi(object)]
pub struct LoadItemAnswer {
    pub page: Option<String>,
    pub fault: Option<LoadFault>,
}
/// The one bounded `{"loads":[…]}` response to a batch: the canonical items
/// `validateLoadBatch` answered and each item's answer, in the same order.
/// The engine classifies every fault (`retryable` or unsaved `failed`) and
/// checks every page against its item.
#[napi]
pub fn encode_load_batch(items: Vec<String>, answers: Vec<LoadItemAnswer>) -> Result<String> {
    let answers = answers
        .into_iter()
        .map(|answer| match answer {
            LoadItemAnswer { page: Some(page), fault: None } => {
                Ok(axton_server::LoadItemAnswer::Page(page))
            }
            LoadItemAnswer { page: None, fault: Some(fault) } => Ok(axton_server::LoadItemAnswer::Fault(
                match (fault.kind.as_str(), fault.code, fault.message) {
                    ("engine", Some(code), Some(message)) => {
                        axton_server::LoadFault::Engine { code, message }
                    }
                    ("conflict", None, None) => axton_server::LoadFault::Conflict,
                    ("unavailable", None, None) => axton_server::LoadFault::Unavailable,
                    (kind, _, _) => return Err(internal(format!("invalid Load fault {kind}"))),
                },
            )),
            _ => Err(internal("a Load answer is exactly one page or fault")),
        })
        .collect::<Result<Vec<_>>>()?;
    axton_server::encode_load_batch(&items, answers).map_err(reason)
}
/// Executes or replays one Load page (one validated batch item) in the
/// host's transaction and answers its page JSON.
#[napi]
pub async fn process_load(
    config_json: String,
    owner: String,
    item_json: String,
    callback: ThreadsafeFunction<String, Promise<String>, String, Status, false>,
) -> Result<String> {
    axton_server::process_load(
        &config(&config_json)?,
        &owner,
        item_json.as_bytes(),
        &CallbackHost(callback),
    )
    .await
    .map_err(reason)
}
/// One Model Fetch (`POST /sync/fetch`) in the caller's application transaction.
#[napi]
pub async fn process_fetch(
    config_json: String,
    owner: String,
    request_json: String,
    callback: ThreadsafeFunction<String, Promise<String>, String, Status, false>,
) -> Result<String> {
    axton_server::process_fetch(
        &config(&config_json)?,
        &owner,
        request_json.as_bytes(),
        &CallbackHost(callback),
    )
    .await
    .map_err(reason)
}
#[napi]
pub async fn process_pull(
    config_json: String,
    owner: String,
    request_json: String,
    callback: ThreadsafeFunction<String, Promise<String>, String, Status, false>,
) -> Result<String> {
    axton_server::process_stream_pull(
        &config(&config_json)?,
        &owner,
        request_json.as_bytes(),
        &CallbackHost(callback),
    )
    .await
    .map_err(reason)
}
#[napi]
pub async fn settle_external(
    config_json: String,
    settlement_json: String,
    callback: ThreadsafeFunction<String, Promise<String>, String, Status, false>,
) -> Result<String> {
    let settlement = serde_json::from_str(&settlement_json).map_err(|e: serde_json::Error| {
        reason(axton_server::Error::new(
            axton_server::code::PUBLISH_INVALID,
            e.to_string(),
        ))
    })?;
    axton_server::settle_external(&config(&config_json)?, &settlement, &CallbackHost(callback))
        .await
        .map(|v| v.to_string())
        .map_err(reason)
}
/// The open live sessions of this process, one `Subscriptions` per socket
/// under a handle the host carries between native calls (like the client
/// `RuntimeHost`). The controller is pure state, so the lock is held only for
/// the transition itself.
#[derive(Default)]
struct LiveSessions {
    next: u64,
    open: BTreeMap<u64, Subscriptions>,
}
static LIVE: OnceLock<Mutex<LiveSessions>> = OnceLock::new();
fn live_sessions() -> Result<MutexGuard<'static, LiveSessions>> {
    LIVE.get_or_init(Mutex::default)
        .lock()
        .map_err(|_| internal("live sessions poisoned"))
}
fn live_invalid(message: &str) -> Error {
    reason(axton_server::Error::new(
        axton_server::code::LIVE_INVALID_EVENT,
        message,
    ))
}
/// Negotiates the subscribe frame inside the host's transaction, opens the
/// socket's `Subscriptions`, and answers `{handle, actions}`: the handle names
/// the session for `live_event` and `live_close`, and the actions are the
/// session's first (listen, send the acknowledgement, pull each stream).
#[napi]
pub async fn negotiate_live(
    config_json: String,
    owner: String,
    request_json: String,
    callback: ThreadsafeFunction<String, Promise<String>, String, Status, false>,
) -> Result<String> {
    let negotiation = axton_server::live::negotiate(
        &config(&config_json)?,
        &owner,
        request_json.as_bytes(),
        &CallbackHost(callback),
    )
    .await
    .map_err(reason)?;
    let (subscriptions, actions) = Subscriptions::open(negotiation);
    let handle = {
        let mut sessions = live_sessions()?;
        sessions.next += 1;
        let handle = sessions.next;
        sessions.open.insert(handle, subscriptions);
        handle
    };
    serde_json::to_string(&serde_json::json!({"handle": handle, "actions": actions}))
        .map_err(internal)
}
/// Applies one `LiveEvent` (JSON) to the session and answers its `LiveAction`s
/// (JSON array). Synchronous: no host call is involved. An unknown handle is
/// `live.invalid_event`, like any other event the session cannot accept.
#[napi]
pub fn live_event(handle: i64, event_json: String) -> Result<String> {
    let event = serde_json::from_str(&event_json).map_err(|e| live_invalid(&e.to_string()))?;
    let actions = {
        let mut sessions = live_sessions()?;
        let subscriptions = u64::try_from(handle)
            .ok()
            .and_then(|handle| sessions.open.get_mut(&handle))
            .ok_or_else(|| live_invalid("live session handle is not open"))?;
        subscriptions.handle_stream(event).map_err(reason)?
    };
    serde_json::to_string(&actions).map_err(internal)
}
/// Forgets the session; idempotent. The host calls it once the socket is
/// closed and `closed` has been dispatched.
#[napi]
pub fn live_close(handle: i64) -> Result<()> {
    if let Ok(handle) = u64::try_from(handle) {
        live_sessions()?.open.remove(&handle);
    }
    Ok(())
}
#[napi]
pub async fn pull_live(
    config_json: String,
    owner: String,
    cursors_json: String,
    models_json: String,
    callback: ThreadsafeFunction<String, Promise<String>, String, Status, false>,
) -> Result<String> {
    // The engine validates the declaration and the cursors again when it
    // decodes the pull.
    let models: std::collections::BTreeMap<String, u64> = serde_json::from_str(&models_json)
        .map_err(|_| {
            reason(axton_server::Error::new(
                axton_server::code::REQUEST_INVALID,
                "invalid live models",
            ))
        })?;
    let cursors: std::collections::BTreeMap<String, u64> = serde_json::from_str(&cursors_json)
        .map_err(|_| {
            reason(axton_server::Error::new(
                axton_server::code::REQUEST_INVALID,
                "invalid live cursors",
            ))
        })?;
    let result = axton_server::live::stream_pull(
        &config(&config_json)?,
        &owner,
        &cursors,
        &models,
        &CallbackHost(callback),
    )
    .await
    .map_err(reason)?;
    serde_json::to_string(&result).map_err(internal)
}

/// Same Model-only context derivation used by the offline client runtime.
#[napi]
pub fn server_materialization_id(config_json:String,projection_generation:String)->Result<String>{
    axton_core_materialization(&config(&config_json)?,&projection_generation)
}
fn axton_core_materialization(config:&axton_server::Config,generation:&str)->Result<String>{
    axton_server::materialization_id(config,generation).map_err(reason)
}

#[napi]
pub fn validate_mutation_batch(config_json:String,request_json:String)->Result<String> {
 axton_server::validate_mutation_batch(&config(&config_json)?,request_json.as_bytes()).map_err(reason)
}
#[napi]
pub async fn process_batch_member(config_json:String,owner:String,request_json:String,ordinal:u32,callback:ThreadsafeFunction<String,Promise<String>,String,Status,false>)->Result<String> {
 axton_server::process_batch_member(&config(&config_json)?,&owner,request_json.as_bytes(),ordinal as u64,&CallbackHost(callback)).await.map_err(reason)
}
#[napi]
pub fn encode_batch_acknowledgement(request_json:String,results:Vec<String>)->Result<String> {
 axton_server::encode_batch_acknowledgement(request_json.as_bytes(),&results).map_err(reason)
}
#[napi]
pub async fn settle_external05(config_json:String,settlement_json:String,callback:ThreadsafeFunction<String,Promise<String>,String,Status,false>)->Result<String> {
 let value=serde_json::from_str(&settlement_json).map_err(internal)?;
 let result=axton_server::settle_external05(&config(&config_json)?,&value,&CallbackHost(callback)).await.map_err(reason)?;
 Ok(result.to_string())
}

#[napi]
pub fn server_materialization_id05(config_json:String,projection_generation:String)->Result<String> {
 axton_server::materialization_id05(&config(&config_json)?,&projection_generation).map_err(reason)
}
