//! The direct call ledger protocol that Actions and Fetch share: claim a call
//! under its canonical request identity, replay what it committed, or run it
//! once under a savepoint and save its outcome. Each caller keeps its own
//! request identity, response type and response encoding.
use crate::actions::call_error;
use crate::host::{Acknowledged, ClaimedCall, HostExt, HostRequest};
use crate::{Error, Host, Result, storage_invalid};
use axton_core::CallCompletion;
use serde::Deserialize;

/// What claiming a call ID decided.
pub(crate) enum Claim {
    /// The owner already claimed this call ID under another request identity.
    /// Nothing is saved, and the saved bytes are never read.
    Conflict,
    /// The call committed this response, whose completion names the call.
    Replay(String),
    /// The call is new, and this transaction now holds its claim.
    Fresh,
}

/// The part of a saved response every replay checks.
#[derive(Deserialize)]
struct Saved {
    completion: CallCompletion,
}

/// Claim `call_id` for `owner` under the canonical `request` identity. The
/// identities are compared before any saved bytes are read, so a response
/// saved by another call kind is never decoded.
pub(crate) async fn claim(
    owner: &str,
    call_id: &str,
    request: &str,
    host: &impl Host,
) -> Result<Claim> {
    let claimed: ClaimedCall = host
        .call_typed(HostRequest::ClaimCall {
            owner: owner.into(),
            call_id: call_id.into(),
            request: request.into(),
        })
        .await?;
    if claimed.request != request {
        return Ok(Claim::Conflict);
    }
    if !claimed.fresh {
        let saved = claimed
            .response
            .ok_or_else(|| storage_invalid("committed call has no response"))?;
        let Saved { completion } = serde_json::from_str(&saved).map_err(storage_invalid)?;
        if completion.call_id != call_id {
            return Err(storage_invalid("saved call ID mismatch"));
        }
        return Ok(Claim::Replay(saved));
    }
    if claimed.response.is_some() {
        return Err(storage_invalid("fresh call already completed"));
    }
    Ok(Claim::Fresh)
}

/// Run a freshly claimed call under savepoint `ordinal` and save its outcome.
/// `run` is polled only after the savepoint opens. A call's own terminal
/// error rolls the savepoint back and becomes the `rejected` response; any
/// other error propagates unsaved, so the caller's transaction, claim
/// included, rolls back. Returns the response and its saved text.
pub(crate) async fn complete<T>(
    owner: &str,
    call_id: &str,
    ordinal: u64,
    host: &impl Host,
    run: impl Future<Output = Result<T>>,
    rejected: impl FnOnce(&Error) -> T,
    encode: impl FnOnce(&T) -> Result<String>,
) -> Result<(T, String)> {
    let Acknowledged = host.call_typed(HostRequest::Savepoint { ordinal }).await?;
    let response = match run.await {
        Ok(response) => {
            let Acknowledged = host.call_typed(HostRequest::Release { ordinal }).await?;
            response
        }
        Err(error) if call_error(&error) => {
            let Acknowledged = host.call_typed(HostRequest::Rollback { ordinal }).await?;
            let Acknowledged = host.call_typed(HostRequest::Release { ordinal }).await?;
            rejected(&error)
        }
        Err(error) => return Err(error),
    };
    let text = encode(&response)?;
    let Acknowledged = host
        .call_typed(HostRequest::SaveCall {
            owner: owner.into(),
            call_id: call_id.into(),
            response: text.clone(),
        })
        .await?;
    Ok((response, text))
}
