//! Native Load pages. One page is one durable call: it is claimed under its
//! owner and call ID, executed at most once inside the caller's application
//! transaction, and its outcome (identity lists, next continuation and
//! authority records, or a terminal rejection) is saved in that same
//! transaction. A repeated call ID answers the saved outcome without running
//! the handler or any Loader.
//!
//! An HTTP batch is transport grouping only: the host validates the envelope
//! once with [`validate_load_batch`] and runs [`process_load`] for each item
//! in its own transaction, so no two items share a transaction, a savepoint
//! or a push sequence.
use crate::actions::{call_error, current_authority};
use crate::host::{Acknowledged, ClaimedCall, HandledLoad, HostExt, HostRequest, Loaded, Stamps};
use crate::{
    Config, Error, Host, Result, code, internal, principal, request_invalid, storage_invalid,
};
use axton_core::{
    AuthorityRecord, Continuation, LoadBatchRequest, LoadError, LoadIntent, LoadItemErrorKind,
    LoadOutcome, LoadPageResponse, RecordKey, canonical_json, limits, normalize_load_args,
    validate_load_data,
};
use serde_json::{Value, json};
use std::collections::BTreeMap;

/// The one savepoint of a page's own transaction.
const ORDINAL: u64 = 1;

/// Structural ingress of one `{"loads":[…]}` batch: bytes, item count,
/// canonical and unique load and call IDs, names, versions and declared read
/// contracts. Answers each item's canonical JSON, in request order, for
/// [`process_load`]. Unknown operations and invalid arguments or continuation
/// state are not refused here; they fail only their own item.
pub fn validate_load_batch(bytes: &[u8]) -> Result<Vec<String>> {
    LoadBatchRequest::decode_envelope(bytes)
        .map_err(request_invalid)?
        .loads
        .iter()
        .map(|item| {
            canonical_json(&serde_json::to_value(item).map_err(internal)?).map_err(internal)
        })
        .collect()
}

/// Execute or replay one Load page inside the caller's application
/// transaction and answer its `LoadPageResponse` JSON. `Ok` is a committed
/// outcome: `succeeded`, a saved terminal `failed`, or an unsaved
/// `call.identity_conflict`. `Err` means nothing may commit: the host rolls
/// back and classifies it (a host failure is retryable; `storage.invalid`,
/// `host.invalid`, `request.invalid` and `internal` are deterministic).
pub async fn process_load(
    config: &Config,
    owner: &str,
    item: &[u8],
    host: &impl Host,
) -> Result<String> {
    principal(owner)?;
    let intent = decode_item(item)?;
    let request = fingerprint(&intent)?;
    let claimed: ClaimedCall = host
        .call_typed(HostRequest::ClaimCall {
            owner: owner.into(),
            call_id: intent.call_id.clone(),
            request: request.clone(),
        })
        .await?;
    if claimed.request != request {
        return encode(&failed(&intent, &Error::code(code::CALL_IDENTITY_CONFLICT)));
    }
    if !claimed.fresh {
        let saved = claimed
            .response
            .ok_or_else(|| storage_invalid("committed call has no response"))?;
        let page: LoadPageResponse = serde_json::from_str(&saved).map_err(storage_invalid)?;
        if !page.answers(&intent) {
            return Err(storage_invalid("saved Load page answers another request"));
        }
        return encode(&current(config, &intent, page)?);
    }
    if claimed.response.is_some() {
        return Err(storage_invalid("fresh call already completed"));
    }
    let Acknowledged = host
        .call_typed(HostRequest::Savepoint { ordinal: ORDINAL })
        .await?;
    let page = match execute_fresh(config, owner, &intent, host).await {
        Ok(page) => {
            let Acknowledged = host
                .call_typed(HostRequest::Release { ordinal: ORDINAL })
                .await?;
            page
        }
        // A rejected page keeps nothing it did: stamps it initialized and any
        // write its handler made roll back before the rejection is saved.
        Err(error) if call_error(&error) => {
            let Acknowledged = host
                .call_typed(HostRequest::Rollback { ordinal: ORDINAL })
                .await?;
            let Acknowledged = host
                .call_typed(HostRequest::Release { ordinal: ORDINAL })
                .await?;
            failed(&intent, &error)
        }
        Err(error) => return Err(error),
    };
    let Acknowledged = host
        .call_typed(HostRequest::SaveCall {
            owner: owner.into(),
            call_id: intent.call_id.clone(),
            response: encode(&page)?,
        })
        .await?;
    encode(&current(config, &intent, page)?)
}

/// One item under the batch's own structural rules.
fn decode_item(item: &[u8]) -> Result<LoadIntent> {
    let item: Value = serde_json::from_slice(item).map_err(request_invalid)?;
    let envelope = serde_json::to_vec(&json!({ "loads": [item] })).map_err(internal)?;
    let mut request = LoadBatchRequest::decode_envelope(&envelope).map_err(request_invalid)?;
    Ok(request.loads.remove(0))
}

/// The claimed page identity. `kind` keeps a Load page from ever matching an
/// Action's saved request under the same call ID. The continuation state is
/// normalized when it can be (the rule depends on no schema); arguments stay
/// as sent, like an Action's, so replay never depends on the retained schema.
fn fingerprint(intent: &LoadIntent) -> Result<String> {
    let continuation = intent
        .continuation
        .as_ref()
        .map(|next| next.normalized().unwrap_or_else(|_| next.clone()));
    canonical_json(&json!({
        "kind": "load",
        "loadId": intent.load_id,
        "callId": intent.call_id,
        "name": intent.name,
        "version": intent.version,
        "args": intent.args,
        "continuation": continuation,
        "models": intent.models,
    }))
    .map_err(internal)
}

async fn execute_fresh(
    config: &Config,
    owner: &str,
    intent: &LoadIntent,
    host: &impl Host,
) -> Result<LoadPageResponse> {
    let load = config
        .schema
        .load(&intent.name, intent.version)
        .map_err(|_| Error::code(code::LOAD_VERSION_UNSUPPORTED))?;
    let args = normalize_load_args(&config.schema, load, &intent.args)
        .map_err(|_| Error::code(code::LOAD_INVALID))?;
    let continuation = intent
        .continuation
        .as_ref()
        .map(Continuation::normalized)
        .transpose()
        .map_err(|error| Error::new(code::LOAD_INVALID_CONTINUATION, error.to_string()))?;
    // Every output Model's authority is served at a declared, retained read
    // contract; nothing is inferred for an undeclared one.
    config.check_declared(&intent.models)?;
    for output in &load.outputs {
        let model = output
            .model
            .as_deref()
            .ok_or_else(|| internal("Load output without a Model"))?;
        if !intent.models.contains_key(model) {
            return Err(Error::new(
                code::MODEL_VERSION_UNSUPPORTED,
                format!("model {model} is not declared by the client"),
            ));
        }
    }
    let handled: HandledLoad = host
        .call_typed(HostRequest::HandleLoad {
            name: intent.name.clone(),
            version: intent.version,
            arguments: args,
            continuation,
            owner: owner.into(),
            call_id: intent.call_id.clone(),
            load_id: intent.load_id.clone(),
        })
        .await?;
    let (data, next) = match handled {
        HandledLoad::Rejected { rejection } => return Err(Error::code(rejection)),
        HandledLoad::Failed { .. } => return Err(Error::code(code::HANDLER_FAILED)),
        HandledLoad::Settled { data, next } => (data, next),
    };
    // Rechecked here whatever the host bridge already did: the page's next
    // request is only ever a bounded portable state.
    let next = next
        .as_ref()
        .map(Continuation::normalized)
        .transpose()
        .map_err(|error| Error::new(code::LOAD_INVALID_CONTINUATION, error.to_string()))?;
    let entries: usize = load
        .outputs
        .iter()
        .map(|output| data[&output.name].as_array().map_or(0, Vec::len))
        .sum();
    if entries > limits::LOAD_PAGE_IDENTITIES {
        return Err(Error::new(
            code::LOAD_PAGE_TOO_LARGE,
            format!(
                "Load page enumerates {entries} identities; at most {} are allowed",
                limits::LOAD_PAGE_IDENTITIES
            ),
        ));
    }
    let data = validate_load_data(&config.schema, load, &data)
        .map_err(|error| Error::new(code::HANDLER_INVALID, error.to_string()))?;
    // Distinct identities per Model, whichever outputs repeat them.
    let mut groups: BTreeMap<String, BTreeMap<String, RecordKey>> = BTreeMap::new();
    for output in &load.outputs {
        let model = output
            .model
            .as_deref()
            .ok_or_else(|| internal("Load output without a Model"))?;
        for identity in data[&output.name].as_array().into_iter().flatten() {
            let key = config
                .schema
                .record_key(model, identity)
                .map_err(|error| Error::new(code::HANDLER_INVALID, error.to_string()))?;
            groups
                .entry(model.into())
                .or_default()
                .insert(key.encoded_identity().map_err(internal)?, key);
        }
    }
    let mut records = vec![];
    for (model, keys) in groups {
        records.extend(resolve(config, owner, intent, &model, keys, host).await?);
    }
    let page = LoadPageResponse {
        load_id: intent.load_id.clone(),
        call_id: intent.call_id.clone(),
        outcome: LoadOutcome::Succeeded { data, next },
        records,
    };
    let bytes = encode(&page)?.len();
    if bytes > limits::LOAD_PAGE_BYTES {
        return Err(Error::new(
            code::LOAD_PAGE_TOO_LARGE,
            format!(
                "Load page encodes to {bytes} bytes; at most {} are allowed",
                limits::LOAD_PAGE_BYTES
            ),
        ));
    }
    // The batch encoder checks no succeeded page's shape or size, so the
    // server holds itself to the client's own per-item rule before saving:
    // a success the client would refuse is never committed.
    page.clone()
        .normalize(&config.schema, intent)
        .map_err(|error| match error.kind {
            LoadItemErrorKind::PageTooLarge => Error::new(code::LOAD_PAGE_TOO_LARGE, error.message),
            _ => internal(format!(
                "assembled Load page is malformed: {}",
                error.message
            )),
        })?;
    Ok(page)
}

/// The authority of one Model's distinct identities: one batched stamp read
/// that initializes only missing stamps, then one batched Loader read at the
/// declared read contract, in the same snapshot. Every identity must have a
/// row; an absent one fails the page rather than reading as a deletion.
async fn resolve(
    config: &Config,
    owner: &str,
    intent: &LoadIntent,
    model: &str,
    keys: BTreeMap<String, RecordKey>,
    host: &impl Host,
) -> Result<Vec<AuthorityRecord>> {
    if !config.loaders.iter().any(|loader| loader == model) {
        return Err(Error::new(code::LOADER_UNREGISTERED, "unregistered loader"));
    }
    let version = intent.models[model];
    let contract = config
        .contract(model, version)
        .ok_or_else(|| Error::code(code::MODEL_VERSION_UNSUPPORTED))?;
    let stamps: Stamps = host
        .call_typed(HostRequest::ReadStamps {
            model: model.into(),
            identity_keys: keys.keys().cloned().collect(),
        })
        .await?;
    if stamps.len() != keys.len() {
        return Err(Error::new(
            code::HOST_INVALID,
            format!(
                "readStamps response invalid: {} stamps for {} records",
                stamps.len(),
                keys.len()
            ),
        ));
    }
    let loaded: Loaded = host
        .call_typed(HostRequest::Load {
            model: model.into(),
            version,
            identities: keys.values().map(|key| key.identity.clone()).collect(),
            owner: owner.into(),
        })
        .await?;
    let rows = match loaded {
        Loaded::Rows(rows) if rows.len() == keys.len() => rows,
        Loaded::Rows(_) => return Err(Error::code(code::LOADER_INVALID)),
        Loaded::Refused { rejection } => return Err(Error::code(rejection)),
        Loaded::Failed { .. } => return Err(Error::code(code::LOADER_FAILED)),
    };
    keys.into_values()
        .zip(stamps)
        .zip(rows)
        .map(|((key, stamp), row)| {
            let row = row.ok_or_else(|| {
                Error::new(
                    code::LOAD_RECORD_UNAVAILABLE,
                    format!("{model} {} is unavailable", key.identity),
                )
            })?;
            Ok(AuthorityRecord {
                state: contract
                    .normalize_state(model, &row)
                    .map_err(|_| Error::code(code::LOADER_INVALID))?,
                model: key.model,
                identity: key.identity,
                stamp: stamp.0,
                error: None,
            })
        })
        .collect()
}

/// A terminal page outcome carrying the error's code and bounded message.
fn failed(intent: &LoadIntent, error: &Error) -> LoadPageResponse {
    LoadPageResponse {
        load_id: intent.load_id.clone(),
        call_id: intent.call_id.clone(),
        outcome: LoadOutcome::Failed {
            error: LoadError::bounded(error.code.clone(), error.message.clone()),
        },
        records: vec![],
    }
}

/// The page with its authority normalized for the current read contracts,
/// as a replayed Action receipt is: a compatible contract change since the
/// page was saved reads back in its current shape.
fn current(
    config: &Config,
    intent: &LoadIntent,
    page: LoadPageResponse,
) -> Result<LoadPageResponse> {
    Ok(LoadPageResponse {
        records: page
            .records
            .into_iter()
            .map(|record| current_authority(config, &intent.models, record))
            .collect::<Result<_>>()?,
        ..page
    })
}

fn encode(page: &LoadPageResponse) -> Result<String> {
    canonical_json(&serde_json::to_value(page).map_err(internal)?).map_err(internal)
}
