//! The Rust-owned client runtime (#134): admission, drain and a wake that only
//! schedules the SDK's drain. No call here waits for a task, and none holds a
//! process-wide lock during database work: each client runs on its own actor.
//!
//! A runtime belongs to the env that opened it. The env's cleanup hook -
//! installed by its first `runtimeOpen` - detaches every runtime it has not
//! detached itself, so `worker.terminate()` or the end of a process closes
//! them instead of leaving their SQLite transaction and thread alive.
use axton_binding::actor::{self, Owners};
use napi::bindgen_prelude::{Status, Unknown};
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi::{Env, Error, Result};
use napi_derive::napi;

/// The runtimes each live env opened, by the env's address.
static OWNERS: Owners = Owners::new();

/// `(runtimeId) => void`, weak so a pending wake never keeps the event loop
/// alive, and non-blocking so the actor never waits on JavaScript.
type Wake = ThreadsafeFunction<String, Unknown<'static>, String, Status, false, true>;

fn runtime_id(runtime_id: &str) -> Result<u64> {
    runtime_id
        .parse()
        .map_err(|_| Error::from_reason("client_closed"))
}

/// Open a runtime; answer its id as a decimal string. The open's outcome is
/// the `taskCompleted` of the request's `requestId`, delivered through `wake`
/// and `runtimeDrain`. Throws only when the request cannot be admitted.
#[napi]
pub fn runtime_open(env: Env, request: String, wake: Wake) -> Result<String> {
    let request = serde_json::from_str(&request).map_err(|e| Error::from_reason(e.to_string()))?;
    let owner = env.raw() as usize;
    OWNERS.watch(owner, || {
        env.add_env_cleanup_hook(owner, |owner| {
            OWNERS.lost(owner);
        })
        .map(drop)
    })?;
    let id = actor::open(
        request,
        Box::new(move |runtime| {
            wake.call(runtime.to_string(), ThreadsafeFunctionCallMode::NonBlocking);
        }),
    )
    .map_err(Error::from_reason)?;
    OWNERS.opened(owner, id);
    Ok(id.to_string())
}

/// Admit one envelope; throws `client_closed` for an unknown, detached or
/// closed runtime. Admission is not completion.
#[napi]
pub fn runtime_submit(runtime_id_text: String, message: String) -> Result<()> {
    let runtime = runtime_id(&runtime_id_text)?;
    let message = serde_json::from_str(&message).map_err(|e| Error::from_reason(e.to_string()))?;
    actor::submit(runtime, message).map_err(Error::from_reason)
}

/// The events published so far, as JSON array text.
#[napi]
pub fn runtime_drain(runtime_id_text: String) -> Result<String> {
    let runtime = runtime_id(&runtime_id_text)?;
    Ok(serde_json::Value::Array(actor::drain(runtime)).to_string())
}

/// Stop wakes and forget the runtime; the wake function is released when this
/// returns and never called again.
#[napi]
pub fn runtime_detach(runtime_id_text: String) -> Result<()> {
    let runtime = runtime_id(&runtime_id_text)?;
    actor::detach(runtime);
    OWNERS.detached(runtime);
    Ok(())
}
