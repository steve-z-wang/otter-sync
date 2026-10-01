//! Native Load pages. One page is one durable call: it is claimed under its
//! owner and call ID, executed at most once inside the caller's application
//! transaction, and its outcome (identity lists, next continuation and
//! authority records, or a terminal rejection) is saved in that same
//! transaction. A repeated call ID answers the saved outcome without running
//! the handler or any Loader.
//!
//! A fresh page may also enroll records it loaded into Scopes, as the
//! add-only handles of its handler declared. The enrollment is judged against
//! the validated page before any read and settled by shared settlement once
//! the page is final, inside the page's savepoint: it commits or rolls back
//! with the page, and a replay never repeats it.
//!
//! An HTTP batch is transport grouping only: the host validates the envelope
//! once with [`validate_load_batch`] and runs [`process_load`] for each item
//! in its own transaction, so no two items share a transaction, a savepoint
//! or a push sequence. It then hands every item's page, or the [`LoadFault`]
//! that escaped its transaction, to [`encode_load_batch`], which classifies
//! the faults and writes the one bounded response.
use crate::actions::{call_error, current_authority};
use crate::host::{
    Acknowledged, ClaimedCall, HandledLoad, HostExt, HostRequest, Loaded, RecordRef, ScopeIntent,
    Stamps,
};
use crate::scope_members::declared_tags;
use crate::settlement::{Changes, invalid_tags, lock_scopes, settle_locked};
use crate::{
    Config, Error, Host, Result, code, internal, principal, request_invalid, storage_invalid,
};
use axton_core::{
    AuthorityRecord, Continuation, LoadBatchRequest, LoadBatchResponse, LoadError, LoadIntent,
    LoadItemErrorKind, LoadNext, LoadOutcome, LoadPageResponse, RecordKey, canonical_json, limits,
    normalize_load_args, validate_load_data,
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, btree_map::Entry};

/// The one savepoint of a page's own transaction.
const ORDINAL: u64 = 1;

/// Structural ingress of one `{"loads":[…]}` batch: bytes, item count,
/// canonical and unique load and call IDs, names, versions and declared read
/// contracts. Answers each item's canonical JSON, in request order, for
/// [`process_load`]. Unknown operations and invalid arguments or continuation
/// state are not refused here; they fail only their own item.
pub fn validate_load_batch(bytes: &[u8]) -> Result<Vec<String>> {
    crate::admit_protocol(bytes)?;
    LoadBatchRequest::decode_envelope(bytes)
        .map_err(request_invalid)?
        .loads
        .iter()
        .map(|item| {
            let encoded = serde_json::to_vec(item).map_err(internal)?;
            let capable =
                axton_core::with_capabilities(&encoded, &[axton_core::SCOPE_MEMBERSHIP_CAPABILITY])
                    .map_err(internal)?;
            String::from_utf8(capable).map_err(internal)
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
    crate::admit_protocol(item)?;
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
    if !crate::calls::same_logical_request(&claimed.request, &request)? {
        return encode(&failed(&intent, &Error::code(code::CALL_IDENTITY_CONFLICT)));
    }
    if !claimed.fresh {
        let saved = claimed
            .response
            .ok_or_else(|| storage_invalid("committed call has no response"))?;
        let page = decode_saved_page(&saved)?;
        if !page.answers(&intent) {
            return Err(storage_invalid("saved Load page answers another request"));
        }
        return replayed(config, &intent, current(config, &intent, page)?);
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
    let item = axton_core::logical_request(&item).map_err(request_invalid)?;
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
    let (data, next, memberships) = match handled {
        HandledLoad::Rejected { rejection } => return Err(Error::code(rejection)),
        HandledLoad::Failed { .. } => return Err(Error::code(code::HANDLER_FAILED)),
        HandledLoad::Settled {
            data,
            next,
            memberships,
        } => (data, next, memberships),
    };
    // Judged here whatever the host bridge already did, before the data: a
    // missing or malformed `next` wrapper and a state past the portable
    // bounds are all the page's `load.invalid_continuation`.
    let next = returned_next(next)?;
    if !data.is_object() {
        return Err(Error::new(
            code::HANDLER_INVALID,
            "Load handler data must be an object",
        ));
    }
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
    // Distinct identities per Model, whichever outputs repeat them, and the
    // canonical keys of every record the page names.
    let mut groups: BTreeMap<String, BTreeMap<String, RecordKey>> = BTreeMap::new();
    let mut data_keys = BTreeSet::new();
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
            data_keys.insert(key.encoded().map_err(internal)?);
            groups
                .entry(model.into())
                .or_default()
                .insert(key.encoded_identity().map_err(internal)?, key);
        }
    }
    // Judged before any read: an enrollment the page may not declare costs
    // no stamp or Loader work.
    let memberships = validate_enrollment(config, &data_keys, memberships)?;
    // Every membership writer locks its Scopes before any record row:
    // `readStamps` below may insert a record's metadata row.
    let scopes: BTreeSet<String> = memberships
        .iter()
        .map(|intent| intent.scope().to_string())
        .collect();
    lock_scopes(&scopes, host).await?;
    let mut records = vec![];
    for (model, keys) in groups {
        records.extend(resolve(config, owner, intent, &model, keys, host).await?);
    }
    let mut page = LoadPageResponse {
        load_id: intent.load_id.clone(),
        call_id: intent.call_id.clone(),
        outcome: LoadOutcome::Succeeded { data, next },
        records,
        memberships: Vec::new(),
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
    // The page is final. Its enrollment settles as an external transaction's
    // unchanged records do: a new member keeps the stamp `resolve` read (and
    // initialized) for this page and gains one position at it; an existing
    // one publishes nothing. No loaded record is touched. Its Scopes are
    // already locked, before the page's reads. A host fault here escapes the
    // page transaction like any other.
    let settled = settle_locked(config, &Changes::new(), &memberships, &scopes, host).await?;
    page.memberships = settled.claims(config, &memberships, &page.records)?;
    page.clone()
        .normalize(&config.schema, intent)
        .map_err(|e| Error::new(code::LOAD_PAGE_TOO_LARGE, e.message))?;
    Ok(page)
}

/// The page's enrollment as canonical additions, preserving each declaration.
/// Each intent must `add` to a named Scope a record of a loaded Model,
/// under a valid identity, that the page's validated outputs name:
/// `data_keys` holds their canonical keys. Repeated pairs count once toward
/// [`limits::LOAD_ENROLLMENT_PAIRS`] and [`limits::LOAD_ENROLLMENT_BYTES`],
/// a pair measuring its canonical add intent with its tags, and validation
/// stops at the first pair past either bound. A repeated pair unions its tags
/// for accounting, in declaration order, and is measured again. Settlement
/// receives the validated declarations so that an accumulated union is never
/// mistaken for one add. Tags follow
/// the add rules ([`declared_tags`]). A removal or tag selector is refused.
fn validate_enrollment(
    config: &Config,
    data_keys: &BTreeSet<String>,
    memberships: Vec<ScopeIntent>,
) -> Result<Vec<ScopeIntent>> {
    let invalid = |message: String| Error::new(code::HANDLER_INVALID, message);
    let too_large = |message: String| Error::new(code::LOAD_PAGE_TOO_LARGE, message);
    let mut pairs: BTreeMap<(String, String), ScopeIntent> = BTreeMap::new();
    let mut bytes = 0;
    let mut declarations = vec![];
    for intent in memberships {
        let (scope, record, tags, label_only) = match intent {
            ScopeIntent::Add {
                scope,
                record,
                tags,
            } => (scope, record, tags, false),
            ScopeIntent::TagAdd {
                scope,
                record,
                tags,
            } => (scope, record, tags, true),
            ScopeIntent::Remove { scope, record } => {
                return Err(invalid(format!(
                    "a Load only adds records to Scopes; it removes {} from {scope}",
                    record.model
                )));
            }
            ScopeIntent::TagRemove { .. }
            | ScopeIntent::DetachTags { .. }
            | ScopeIntent::Select { .. } => {
                return Err(invalid(
                    "a Load only adds membership or labels for returned records".into(),
                ));
            }
        };
        if axton_core::check_scope(&scope).is_err() {
            return Err(invalid("Load enrollment names a blank Scope".into()));
        }
        let key = config
            .schema
            .record_key(&record.model, &record.identity)
            .map_err(|error| invalid(error.to_string()))?;
        if !config.loaders.contains(&key.model) {
            return Err(crate::settlement::unregistered(&key.model));
        }
        let encoded = key.encoded().map_err(internal)?;
        if !data_keys.contains(&encoded) {
            return Err(invalid(format!(
                "Load enrolls {} {} that its page does not return",
                key.model, key.identity
            )));
        }
        if label_only && tags.is_empty() {
            return Err(invalid("label operation must name at least one tag".into()));
        }
        declared_tags(&tags).map_err(|reason| invalid_tags(&scope, reason))?;
        // Distinct, in first-declaration order, as the collector measures them.
        let mut distinct: Vec<String> = vec![];
        for tag in tags {
            if !distinct.contains(&tag) {
                distinct.push(tag);
            }
        }
        // Settlement must see validated declarations, never their larger union.
        let record = RecordRef {
            model: key.model.clone(),
            identity: key.identity.clone(),
        };
        declarations.push(if label_only {
            ScopeIntent::TagAdd {
                scope: scope.clone(),
                record,
                tags: distinct.clone(),
            }
        } else {
            ScopeIntent::Add {
                scope: scope.clone(),
                record,
                tags: distinct.clone(),
            }
        });
        let measure = |intent: &ScopeIntent| -> Result<usize> {
            Ok(
                canonical_json(&serde_json::to_value(intent).map_err(internal)?)
                    .map_err(internal)?
                    .len(),
            )
        };
        match pairs.entry((scope.clone(), encoded)) {
            Entry::Vacant(pair) => {
                let canonical = ScopeIntent::Add {
                    scope,
                    record: RecordRef {
                        model: key.model,
                        identity: key.identity,
                    },
                    tags: distinct,
                };
                bytes += measure(&canonical)?;
                pair.insert(canonical);
            }
            Entry::Occupied(mut pair) => {
                let before = measure(pair.get())?;
                if let ScopeIntent::Add { tags: held, .. } = pair.get_mut() {
                    for tag in distinct {
                        if !held.contains(&tag) {
                            held.push(tag);
                        }
                    }
                }
                bytes = bytes - before + measure(pair.get())?;
            }
        }
        if pairs.len() > limits::LOAD_ENROLLMENT_PAIRS {
            return Err(too_large(format!(
                "Load page enrolls more than {} Scope/record pairs",
                limits::LOAD_ENROLLMENT_PAIRS
            )));
        }
        if bytes > limits::LOAD_ENROLLMENT_BYTES {
            return Err(too_large(format!(
                "Load page enrollment encodes to more than {} bytes",
                limits::LOAD_ENROLLMENT_BYTES
            )));
        }
    }
    Ok(declarations)
}

/// The handler's `next` member as a continuation: `null` or exactly
/// `{state}`, with the state normalized within the portable bounds.
fn returned_next(next: Option<Value>) -> Result<LoadNext> {
    let invalid = |message: String| Error::new(code::LOAD_INVALID_CONTINUATION, message);
    let next = next.ok_or_else(|| invalid("Load handler answered no next continuation".into()))?;
    serde_json::from_value::<LoadNext>(next)
        .map_err(|error| invalid(format!("invalid Load continuation: {error}")))?
        .as_ref()
        .map(Continuation::normalized)
        .transpose()
        .map_err(|error| invalid(error.to_string()))
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
        return Err(crate::settlement::unregistered(model));
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
        memberships: Vec::new(),
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
        memberships: crate::settlement::current_claims(config, page.memberships)?,
        records: page
            .records
            .into_iter()
            .map(|record| current_authority(config, &intent.models, record))
            .collect::<Result<_>>()?,
        ..page
    })
}

/// A replayed page as answered now. Renormalized authority can outgrow the
/// page bound the saved page met (a compatible contract change adds a
/// member to every record), so a succeeded page is held to the client's
/// per-item rule again: one it would refuse as too large answers an unsaved
/// `load.page_too_large` instead, and the saved page stays as it was.
fn replayed(config: &Config, intent: &LoadIntent, page: LoadPageResponse) -> Result<String> {
    if matches!(page.outcome, LoadOutcome::Succeeded { .. })
        && let Err(error) = page.clone().normalize(&config.schema, intent)
    {
        return match error.kind {
            LoadItemErrorKind::PageTooLarge => encode(&failed(
                intent,
                &Error::new(code::LOAD_PAGE_TOO_LARGE, error.message),
            )),
            _ => Err(storage_invalid(format!(
                "saved Load page no longer reads back: {}",
                error.message
            ))),
        };
    }
    encode(&page)
}

fn encode(page: &LoadPageResponse) -> Result<String> {
    canonical_json(&serde_json::to_value(page).map_err(internal)?).map_err(internal)
}

/// What escaped one Load item's transaction, as its carrier observed it.
/// Nothing of the item committed, or its commit result is unknown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoadFault {
    /// [`process_load`] answered this error.
    Engine { code: String, message: String },
    /// The database kept refusing the transaction with a serialization
    /// failure or deadlock after the carrier's own retries.
    Conflict,
    /// Anything else escaped the transaction boundary: a commit whose result
    /// is unknown, a pool, driver or connection failure.
    Unavailable,
}

/// One batch item as its transaction ended: the page [`process_load`]
/// answered (committed), or the fault that escaped it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoadItemAnswer {
    Page(String),
    Fault(LoadFault),
}

/// The unsaved outcome of a fault. A host failure, a conflict and an
/// unknown commit are `retryable`: the client resends the same call ID and
/// the saved claim decides what committed. Every other engine error is a
/// deterministic defect (`host.invalid`, `storage.invalid`, `internal`,
/// `request.invalid`, `principal.invalid`, `config.invalid`) that a resend
/// would reproduce, so it is `failed` and the job stops. No database text
/// reaches the client for a retryable fault.
pub fn load_fault_outcome(fault: &LoadFault) -> LoadOutcome {
    let retryable = |code: &str, message: &str| LoadOutcome::Retryable {
        error: LoadError::bounded(code, message),
    };
    match fault {
        LoadFault::Engine { code, .. } if code == code::HOST => retryable(
            code::SERVER_UNAVAILABLE,
            "the page transaction did not complete; resend the same call ID",
        ),
        // Settlement found its Scope locks outdated and the carrier's
        // retries ran out: a conflict like any other.
        LoadFault::Engine { code, .. } if code == code::TRANSACTION_CONFLICT => retryable(
            code::TRANSACTION_CONFLICT,
            "the page transaction kept conflicting; resend it",
        ),
        LoadFault::Engine { code, message } => LoadOutcome::Failed {
            error: LoadError::bounded(code.clone(), message.clone()),
        },
        LoadFault::Conflict => retryable(
            code::TRANSACTION_CONFLICT,
            "the page transaction kept conflicting; resend it",
        ),
        LoadFault::Unavailable => retryable(
            code::SERVER_UNAVAILABLE,
            "the page transaction did not complete; resend the same call ID",
        ),
    }
}

/// The one `{"loads":[…]}` response to a batch, from the canonical items
/// [`validate_load_batch`] answered and each item's [`LoadItemAnswer`], in
/// the same order. Every item answers its own request: a fault becomes its
/// [`load_fault_outcome`]; a page is decoded under the client's item rules
/// and must answer its item, and one that does not, or that is succeeded
/// past [`limits::LOAD_PAGE_BYTES`], becomes an unsaved `failed` item
/// (`internal` or `load.page_too_large`) without costing its siblings. The
/// response is written by [`LoadBatchResponse::encode`], so it holds 1..=8
/// canonical, unique correlations within [`limits::LOAD_RESPONSE_BYTES`].
pub fn encode_load_batch(items: &[String], answers: Vec<LoadItemAnswer>) -> Result<String> {
    if items.len() != answers.len() {
        return Err(internal(format!(
            "{} Load answers for {} items",
            answers.len(),
            items.len()
        )));
    }
    let mut loads = Vec::with_capacity(items.len());
    for (item, answer) in items.iter().zip(answers) {
        let intent = decode_item(item.as_bytes())?;
        let unsaved = |outcome| LoadPageResponse {
            load_id: intent.load_id.clone(),
            call_id: intent.call_id.clone(),
            outcome,
            records: vec![],
            memberships: Vec::new(),
        };
        loads.push(match answer {
            LoadItemAnswer::Fault(fault) => unsaved(load_fault_outcome(&fault)),
            LoadItemAnswer::Page(page) => match answered_page(&intent, &page) {
                Ok(page) => page,
                Err(error) => unsaved(LoadOutcome::Failed {
                    error: LoadError::bounded(error.code, error.message),
                }),
            },
        });
    }
    let bytes = LoadBatchResponse { loads }
        .encode()
        .map_err(|error| internal(format!("Load response: {error}")))?;
    String::from_utf8(bytes).map_err(internal)
}

/// One engine page, checked before it joins the response.
fn answered_page(intent: &LoadIntent, page: &str) -> Result<LoadPageResponse> {
    let value: Value = serde_json::from_str(page).map_err(internal)?;
    let page = LoadPageResponse::decode_item(&value)
        .map_err(|error| internal(format!("malformed Load page: {}", error.message)))?;
    if !page.answers(intent) {
        return Err(internal("Load page answers another item"));
    }
    if matches!(page.outcome, LoadOutcome::Succeeded { .. }) {
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
    }
    Ok(page)
}

/// Only the durable ledger can contain a pre-capability response. Its absent
/// claims remain absent; replay never executes enrollment to fill them in.
fn decode_saved_page(saved: &str) -> Result<LoadPageResponse> {
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct LegacyPage {
        load_id: String,
        call_id: String,
        outcome: LoadOutcome,
        records: Vec<AuthorityRecord>,
    }
    let raw: Value = serde_json::from_str(saved).map_err(storage_invalid)?;
    if raw.get("memberships").is_some() {
        return serde_json::from_value(raw).map_err(storage_invalid);
    }
    let legacy: LegacyPage = serde_json::from_value(raw).map_err(storage_invalid)?;
    Ok(LoadPageResponse {
        load_id: legacy.load_id,
        call_id: legacy.call_id,
        outcome: legacy.outcome,
        records: legacy.records,
        memberships: vec![],
    })
}
