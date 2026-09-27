//! Model Fetch: one authorized Loader read of one identity at a retained
//! Model read version, inside the application's transaction
//! ([#153](https://github.com/zanminwang/axton/issues/153)). Fetch declares no
//! Action and runs no Handler. It shares the direct call ledger with Actions
//! under a Fetch-tagged request identity, and the single-record read with
//! Action results. It never touches, publishes or changes a membership.
use crate::action_results::{ensure_stamp, load_one_state};
use crate::actions::call_error;
use crate::host::{Acknowledged, ClaimedCall, HostExt, HostRequest};
use crate::settlement::unregistered;
use crate::{
    Config, Error, Host, Result, code, internal, principal, request_invalid, storage_invalid,
};
use axton_core::{
    ActionOutcome, AuthorityRecord, CallCompletion, ExecutionState, FetchRequest, FetchResponse,
    RecordKey, canonical_json,
};
use serde::Deserialize;
use serde_json::{Value, json};

/// The one savepoint a Fetch opens, as a direct Action opens its own.
const ORDINAL: u64 = 1;

fn failed(call_id: &str, code: &str) -> FetchResponse {
    FetchResponse {
        completion: CallCompletion {
            call_id: call_id.into(),
            outcome: ActionOutcome::Failed {
                code: code.into(),
                execution: ExecutionState::Rejected,
            },
        },
        records: vec![],
    }
}

fn encode(response: &FetchResponse) -> Result<String> {
    String::from_utf8(response.encode().map_err(internal)?).map_err(internal)
}

/// The canonical call identity. The explicit `fetch` kind keeps it distinct
/// from every Action identity (which has no `kind`) and from any other tagged
/// kind, so reusing a call ID across kinds conflicts rather than replays.
fn canonical_intent(request: &FetchRequest) -> Result<String> {
    canonical_json(&json!({
        "kind": "fetch",
        "callId": request.call_id,
        "model": request.model,
        "version": request.version,
        "identity": request.identity,
        "store": request.store,
    }))
    .map_err(internal)
}

/// The part of a saved response replay checks before returning its bytes.
#[derive(Deserialize)]
struct Saved {
    completion: CallCompletion,
}

/// Serve one `POST /sync/fetch` in the caller's application transaction. The
/// caller commits before replying and must never commit after an error.
///
/// A malformed envelope, or an identity the requested served read contract
/// does not accept, is `request.invalid` before the call is claimed. An
/// unserved Model or version, a missing Loader and every Loader refusal,
/// failure or invalid row are the claimed call's saved rejection. Host and
/// storage failures propagate so the transaction, claim included, rolls back.
pub async fn process_fetch(
    config: &Config,
    owner: &str,
    bytes: &[u8],
    host: &impl Host,
) -> Result<String> {
    principal(owner)?;
    let mut request = FetchRequest::decode_envelope(bytes).map_err(request_invalid)?;
    // Normalized by the requested read contract when this backend serves it,
    // so key order and numeric spelling never split one call identity.
    let key = match config.contract(&request.model, request.version) {
        Some(contract) => {
            let key = contract
                .record_key(&request.model, &request.identity)
                .map_err(request_invalid)?;
            request.identity = key.identity.clone();
            Some(key)
        }
        None => None,
    };
    let intent = canonical_intent(&request)?;
    let claimed: ClaimedCall = host
        .call_typed(HostRequest::ClaimCall {
            owner: owner.into(),
            call_id: request.call_id.clone(),
            request: intent.clone(),
        })
        .await?;
    // Compared before any saved bytes are read: another kind's response is
    // never decoded as a Fetch.
    if claimed.request != intent {
        return encode(&failed(&request.call_id, "call.identity_conflict"));
    }
    if !claimed.fresh {
        let saved = claimed
            .response
            .ok_or_else(|| storage_invalid("committed call has no response"))?;
        let Saved { completion } = serde_json::from_str(&saved).map_err(storage_invalid)?;
        if completion.call_id != request.call_id {
            return Err(storage_invalid("saved call ID mismatch"));
        }
        return Ok(saved);
    }
    if claimed.response.is_some() {
        return Err(storage_invalid("fresh call already completed"));
    }
    let Acknowledged = host
        .call_typed(HostRequest::Savepoint { ordinal: ORDINAL })
        .await?;
    let response = match read(config, owner, &request, key, host).await {
        Ok(response) => {
            let Acknowledged = host
                .call_typed(HostRequest::Release { ordinal: ORDINAL })
                .await?;
            response
        }
        Err(error) if call_error(&error) => {
            let Acknowledged = host
                .call_typed(HostRequest::Rollback { ordinal: ORDINAL })
                .await?;
            let Acknowledged = host
                .call_typed(HostRequest::Release { ordinal: ORDINAL })
                .await?;
            failed(&request.call_id, &error.code)
        }
        Err(error) => return Err(error),
    };
    let text = encode(&response)?;
    let Acknowledged = host
        .call_typed(HostRequest::SaveCall {
            owner: owner.into(),
            call_id: request.call_id.clone(),
            response: text.clone(),
        })
        .await?;
    Ok(text)
}

/// The fresh read: stamp evidence first when storing, then one Loader read
/// whose normalized state is both the result and the authority.
async fn read(
    config: &Config,
    owner: &str,
    request: &FetchRequest,
    key: Option<RecordKey>,
    host: &impl Host,
) -> Result<FetchResponse> {
    let key = key.ok_or_else(|| Error::code(code::MODEL_VERSION_UNSUPPORTED))?;
    if !config.loaders.contains(&key.model) {
        return Err(unregistered());
    }
    let stamp = match request.store {
        true => Some(ensure_stamp(&key, host).await?),
        false => None,
    };
    let state = load_one_state(config, owner, &key, request.version, host).await?;
    let result = match &state {
        Value::Object(fields) => {
            let mut result = key.identity.as_object().cloned().unwrap_or_default();
            result.extend(fields.clone());
            Value::Object(result)
        }
        _ => Value::Null,
    };
    Ok(FetchResponse {
        completion: CallCompletion {
            call_id: request.call_id.clone(),
            outcome: ActionOutcome::Succeeded { result },
        },
        records: stamp
            .map(|stamp| AuthorityRecord {
                model: key.model.clone(),
                identity: key.identity.clone(),
                stamp,
                state,
                error: None,
            })
            .into_iter()
            .collect(),
    })
}
