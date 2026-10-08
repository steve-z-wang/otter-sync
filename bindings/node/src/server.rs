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
    let cfg=config(&config_json)?;
    let (subscriptions,actions)=axton_server::live::negotiate05(&cfg,&owner,request_json.as_bytes(),&CallbackHost(callback)).await.map_err(reason)?;
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

#[napi]
pub async fn process_delivery05(config_json:String,owner:String,request_json:String,callback:ThreadsafeFunction<String,Promise<String>,String,Status,false>)->Result<String> {
 axton_server::process_delivery05(&config(&config_json)?,&owner,request_json.as_bytes(),&CallbackHost(callback)).await.map_err(reason)
}

#[napi]
pub async fn process_materialization05(config_json:String,owner:String,request_json:String,callback:ThreadsafeFunction<String,Promise<String>,String,Status,false>)->Result<String> {
 axton_server::process_materialization05(&config(&config_json)?,&owner,request_json.as_bytes(),&CallbackHost(callback)).await.map_err(reason)
}

#[napi]
pub async fn process_read05(config_json:String,owner:String,request_json:String,callback:ThreadsafeFunction<String,Promise<String>,String,Status,false>)->Result<String> {
 axton_server::process_read05(&config(&config_json)?,&owner,request_json.as_bytes(),&CallbackHost(callback)).await.map_err(reason)
}

#[napi]
pub async fn handshake05(config_json:String,owner:String,request_json:String,callback:ThreadsafeFunction<String,Promise<String>,String,Status,false>)->Result<String> {
 axton_server::handshake05(&config(&config_json)?,&owner,request_json.as_bytes(),&CallbackHost(callback)).await.map_err(reason)
}

#[napi]
pub async fn process_live05(config_json:String,owner:String,request_json:String,callback:ThreadsafeFunction<String,Promise<String>,String,Status,false>)->Result<String>{
 axton_server::process_live05(&config(&config_json)?,&owner,request_json.as_bytes(),&CallbackHost(callback)).await.map_err(reason)
}
