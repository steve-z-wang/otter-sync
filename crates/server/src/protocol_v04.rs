//! Actual protocol4 dispatch. Host/persistence owns the application transaction.
use crate::{
    Config, Error, Host, Result,
    calls::{self, Claim},
    code,
    host::{HostExt, HostRequest},
    internal, request_invalid,
};
use axton_core::{
    ActionOutcome, CallCompletion, ExecutionState,
    v04::{self, FetchIntent, NullCursor, ReadRecord, ReadResponse, RequestContext, Validate},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RetainedMaterialization {
    pub schema: axton_core::Schema,
    #[serde(default = "default_projection")]
    pub projection_generation: String,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProtocolConfig {
    pub backend_id: String,
    pub contract_id: String,
    #[serde(default = "default_unit_bytes")]
    pub max_unit_bytes: u64,
    #[serde(default)]
    pub materialization_id: String,
    #[serde(default = "default_projection")]
    pub projection_generation: String,
    #[serde(default)]
    pub materializations: std::collections::BTreeMap<String, RetainedMaterialization>,
}
fn default_unit_bytes() -> u64 {
    1024 * 1024
}
fn default_projection() -> String {
    "1".into()
}
impl ProtocolConfig {
    pub(crate) fn models(
        &self,
        config: &Config,
        context: &RequestContext,
    ) -> Result<std::collections::BTreeMap<String, u64>> {
        model_versions(
            config,
            &context.materialization,
            &self.materialization_id,
            &self.materializations,
            v04::materialization_id,
        )
    }
}
pub(crate) fn model_versions(
    config: &Config,
    materialization: &str,
    active: &str,
    retained_contexts: &std::collections::BTreeMap<String, RetainedMaterialization>,
    identity: fn(&axton_core::Schema, &str) -> axton_core::Result<String>,
) -> Result<std::collections::BTreeMap<String, u64>> {
    if materialization == active {
        return Ok(config
            .schema
            .models
            .iter()
            .map(|m| (m.name.clone(), m.version))
            .collect());
    }
    let retained = retained_contexts
        .get(materialization)
        .ok_or_else(|| Error::code("context_mismatch"))?;
    if identity(&retained.schema, &retained.projection_generation).map_err(request_invalid)?
        != materialization
    {
        return Err(Error::code("context_mismatch"));
    }
    let mut models = std::collections::BTreeMap::new();
    for model in &retained.schema.models {
        let served = config
            .contract(&model.name, model.version)
            .ok_or_else(|| Error::code("context_mismatch"))?;
        let mut expected = retained.schema.clone();
        expected.models = vec![model.clone()];
        let mut actual = served.clone();
        actual.models[0].bootstrap = model.bootstrap;
        if identity(&expected, "read-contract-check").map_err(request_invalid)?
            != identity(&actual, "read-contract-check").map_err(request_invalid)?
        {
            return Err(Error::code("context_mismatch"));
        }
        models.insert(model.name.clone(), model.version);
    }
    Ok(models)
}
pub(crate) fn models(
    config: &Config,
    context: &RequestContext,
) -> Result<std::collections::BTreeMap<String, u64>> {
    config
        .protocol4
        .as_ref()
        .ok_or_else(|| Error::code("protocol.unsupported"))?
        .models(config, context)
}
pub(crate) fn is_request(bytes: &[u8]) -> bool {
    serde_json::from_slice::<Value>(bytes)
        .ok()
        .and_then(|v| v.get("context")?.get("protocol").cloned())
        .is_some_and(|v| v == 4)
}
pub(crate) async fn admit(
    config: &Config,
    owner: &str,
    context: &RequestContext,
    durable: bool,
    host: &impl Host,
) -> Result<()> {
    context.validate().map_err(request_invalid)?;
    let served = config
        .protocol4
        .as_ref()
        .ok_or_else(|| Error::code("protocol.unsupported"))?;
    if context.binding.viewer != owner
        || context.binding.backend != served.backend_id
        || context.binding.contract != served.contract_id
        || (!durable && context.materialization != served.materialization_id)
    {
        return Err(Error::code("context_mismatch"));
    }
    models(config, context)?;
    let allowed: bool = host
        .call_typed(HostRequest::AdmitContext {
            owner: owner.into(),
            context: context.clone(),
            durable,
        })
        .await?;
    if !allowed {
        return Err(Error::code("stream.forbidden"));
    }
    Ok(())
}
fn failed(context: RequestContext, id: String, code: String) -> ReadResponse {
    ReadResponse {
        context,
        completion: CallCompletion {
            call_id: id,
            outcome: ActionOutcome::Failed {
                code,
                execution: ExecutionState::Rejected,
            },
        },
        records: vec![],
    }
}
fn encode<T: Serialize + Validate>(v: &T) -> Result<String> {
    String::from_utf8(v04::encode(v).map_err(internal)?).map_err(internal)
}
pub(crate) async fn fetch(
    config: &Config,
    owner: &str,
    bytes: &[u8],
    host: &impl Host,
) -> Result<String> {
    let mut intent: FetchIntent = v04::decode(bytes).map_err(request_invalid)?;
    admit(config, owner, &intent.context, false, host).await?;
    if models(config, &intent.context)?.get(&intent.model) != Some(&intent.version) {
        return Err(Error::code(code::MODEL_VERSION_UNSUPPORTED));
    }
    let key = config
        .contract(&intent.model, intent.version)
        .map(|schema| schema.record_key(&intent.model, &intent.identity))
        .transpose()
        .map_err(request_invalid)?;
    if let Some(key) = &key {
        intent.identity = key.identity.clone();
    }
    let request =
        axton_core::canonical_json(&json!({"kind":"fetch","intent":intent})).map_err(internal)?;
    match calls::claim(owner, &intent.call_id, &request, host).await? {
        Claim::Replay(saved) => {
            let _: ReadResponse = v04::decode(saved.as_bytes()).map_err(crate::storage_invalid)?;
            return Ok(saved);
        }
        Claim::Conflict => {
            return encode(&failed(
                intent.context.clone(),
                intent.call_id.clone(),
                "call.identity_conflict".into(),
            ));
        }
        Claim::Fresh => {}
    }
    let read = async {
        let key = key.ok_or_else(|| Error::code(code::MODEL_VERSION_UNSUPPORTED))?;
        if !config.loaders.contains(&key.model) {
            return Err(crate::settlement::unregistered(&key.model));
        }
        let state =
            crate::action_results::load_one_state(config, owner, &key, intent.version, host)
                .await?;
        let result = if state.is_null() {
            Value::Null
        } else {
            let mut full = key.identity.as_object().cloned().unwrap_or_default();
            full.extend(state.as_object().cloned().unwrap_or_default());
            Value::Object(full)
        };
        Ok(ReadResponse {
            context: intent.context.clone(),
            completion: CallCompletion {
                call_id: intent.call_id.clone(),
                outcome: ActionOutcome::Succeeded { result },
            },
            records: vec![ReadRecord {
                key,
                cursor: NullCursor,
                state,
            }],
        })
    };
    let (_, text) = calls::complete(
        owner,
        &intent.call_id,
        1,
        host,
        read,
        |e| {
            failed(
                intent.context.clone(),
                intent.call_id.clone(),
                e.code.clone(),
            )
        },
        encode,
    )
    .await?;
    Ok(text)
}

pub(crate) async fn query(
    config: &Config,
    owner: &str,
    bytes: &[u8],
    host: &impl Host,
) -> Result<String> {
    use crate::host::{Acknowledged, HandledAction, StreamIntent};
    let intent: v04::ReadIntent = v04::decode(bytes).map_err(request_invalid)?;
    admit(config, owner, &intent.context, false, host).await?;
    let request =
        axton_core::canonical_json(&json!({"kind":"query","intent":intent})).map_err(internal)?;
    match calls::claim(owner, &intent.call_id, &request, host).await? {
        Claim::Replay(saved) => {
            let _: ReadResponse = v04::decode(saved.as_bytes()).map_err(crate::storage_invalid)?;
            return Ok(saved);
        }
        Claim::Conflict => {
            return encode(&failed(
                intent.context.clone(),
                intent.call_id.clone(),
                "call.identity_conflict".into(),
            ));
        }
        Claim::Fresh => {}
    }
    let read = async {
        let action = config
            .schema
            .action(&intent.name, intent.version)
            .map_err(|_| Error::code("action_version_unsupported"))?;
        if action.kind != axton_core::CallKind::Query {
            return Err(Error::code("action.invalid"));
        }
        let args = axton_core::normalize_action_args(&config.schema, action, &intent.args)
            .map_err(|_| Error::code("action.invalid"))?;
        let handled: HandledAction = host
            .call_typed(HostRequest::HandleAction {
                name: intent.name.clone(),
                version: intent.version,
                arguments: args.clone(),
                owner: owner.into(),
                call_id: intent.call_id.clone(),
                ordinal: 1,
                context: Some(intent.context.clone()),
            })
            .await?;
        let (outputs, changes, declarations) = match handled {
            HandledAction::Rejected { rejection } => return Err(Error::code(rejection)),
            HandledAction::Failed { .. } => return Err(Error::code(code::HANDLER_FAILED)),
            HandledAction::Settled {
                outputs,
                changes,
                declarations,
            } => (outputs, changes, declarations),
        };
        if !changes.is_empty()
            || declarations
                .iter()
                .any(|d| matches!(d, StreamIntent::Invalidate { .. }))
        {
            return Err(Error::code(code::QUERY_EFFECTS_FORBIDDEN));
        }
        if !declarations.is_empty() {
            let _: Acknowledged = host.call_typed(HostRequest::PublicationFence {}).await?;
            crate::settlement::settle_changes(config, &Default::default(), &declarations, host)
                .await?;
        }
        let (result, records) = crate::action_results::assemble_snapshots(
            config,
            owner,
            action,
            &args,
            &outputs,
            crate::action_results::SnapshotPolicy {
                models: &models(config, &intent.context)?,
                canonical: false,
            },
            host,
        )
        .await?;
        let result = axton_core::validate_action_result(&config.schema, action, &result)
            .map_err(|_| Error::code(code::HANDLER_INVALID))?;
        Ok(ReadResponse {
            context: intent.context.clone(),
            completion: CallCompletion {
                call_id: intent.call_id.clone(),
                outcome: ActionOutcome::Succeeded { result },
            },
            records,
        })
    };
    let (_, text) = calls::complete(
        owner,
        &intent.call_id,
        1,
        host,
        read,
        |e| {
            failed(
                intent.context.clone(),
                intent.call_id.clone(),
                e.code.clone(),
            )
        },
        encode,
    )
    .await?;
    Ok(text)
}

async fn claim_plan(
    config: &Config,
    owner: &str,
    context: &RequestContext,
    id: &str,
    request: &str,
    host: &impl Host,
) -> Result<Option<String>> {
    admit(config, owner, context, false, host).await?;
    let claimed: crate::host::ClaimedCall = host
        .call_typed(HostRequest::ClaimCall {
            owner: owner.into(),
            call_id: id.into(),
            request: request.into(),
        })
        .await?;
    if claimed.request != request {
        return Err(Error::code("call.identity_conflict"));
    }
    if claimed.fresh {
        if claimed.response.is_some() {
            return Err(crate::storage_invalid("fresh plan already saved"));
        }
        Ok(None)
    } else {
        Ok(Some(claimed.response.ok_or_else(|| {
            crate::storage_invalid("committed plan incomplete")
        })?))
    }
}
async fn save_plan(owner: &str, id: &str, response: String, host: &impl Host) -> Result<String> {
    let _: crate::host::Acknowledged = host
        .call_typed(HostRequest::SaveCall {
            owner: owner.into(),
            call_id: id.into(),
            response: response.clone(),
        })
        .await?;
    Ok(response)
}
pub(crate) async fn delta(
    config: &Config,
    owner: &str,
    bytes: &[u8],
    host: &impl Host,
) -> Result<String> {
    use crate::host::{Acknowledged, MemberKey, PublicationGroup};
    use axton_core::v04::{CommitUnit, DeltaIntent, DeltaPage};
    let intent: DeltaIntent = v04::decode(bytes).map_err(request_invalid)?;
    if models(config, &intent.context)? != intent.models {
        return Err(Error::code("context_mismatch"));
    }
    if intent.limit > 1000 {
        return Err(Error::code("page.capacity"));
    }
    let request =
        axton_core::canonical_json(&json!({"kind":"delta","intent":intent})).map_err(internal)?;
    if let Some(saved) = claim_plan(
        config,
        owner,
        &intent.context,
        &intent.call_id,
        &request,
        host,
    )
    .await?
    {
        let _: DeltaPage = v04::decode(saved.as_bytes()).map_err(crate::storage_invalid)?;
        return Ok(saved);
    }
    let _: Acknowledged = host.call_typed(HostRequest::PublicationFence {}).await?;
    let mut prepared = std::collections::BTreeSet::new();
    let (head, groups) = loop {
        let head = crate::head(host, &intent.context.binding.stream).await?;
        if intent.after > head {
            return Err(request_invalid("cursor ahead of head"));
        }
        let groups: Vec<PublicationGroup> = host
            .call_typed(HostRequest::ReadPublicationGroups {
                stream: intent.context.binding.stream.clone(),
                after: intent.after,
                limit: intent.limit,
            })
            .await?;
        let keys = groups
            .iter()
            .flat_map(|g| g.keys.iter().cloned())
            .collect::<Vec<_>>();
        if !prepare_stream_keys(
            config,
            owner,
            &intent.context,
            &intent.models,
            keys,
            &mut prepared,
            host,
        )
        .await?
        {
            break (head, groups);
        }
    };
    let mut through = intent.after;
    let mut seen = std::collections::BTreeSet::new();
    let mut units = vec![];
    for group in groups {
        if group.through <= through
            || group.through > head
            || group.from > through
            || group.keys.len() > 10000
        {
            return Err(crate::storage_invalid("publication group prefix invalid"));
        }
        let keys = group
            .keys
            .into_iter()
            .filter(|k| !seen.contains(&(k.model.clone(), k.identity_key.clone())))
            .collect::<Vec<MemberKey>>();
        let positions = read_positions_checked(&intent.context, &keys, host).await?;
        for position in &positions {
            if position.cursor <= through || position.cursor > head {
                return Err(crate::storage_invalid(
                    "publication group current position invalid",
                ));
            }
        }
        for key in keys {
            seen.insert((key.model, key.identity_key));
        }
        let changes = materialize_authority(config, owner, &intent.models, positions, host).await?;
        bounded_unit(config, &changes)?;
        through = group.through;
        units.push(CommitUnit { through, changes });
    }
    if through == intent.after && intent.after < head {
        return Err(crate::storage_invalid(
            "publication prefix evidence missing",
        ));
    }
    let page = DeltaPage {
        context: intent.context,
        page_id: intent.call_id.clone(),
        from: intent.after,
        to: through,
        head,
        units,
    };
    save_plan(owner, &intent.call_id, encode(&page)?, host).await
}

/// Membership Remove is complete without a Loader or a preparation hook.
async fn prepare_stream_keys(
    config: &Config,
    owner: &str,
    context: &RequestContext,
    models: &std::collections::BTreeMap<String, u64>,
    keys: Vec<crate::host::MemberKey>,
    prepared: &mut std::collections::BTreeSet<(String, String)>,
    host: &impl Host,
) -> Result<bool> {
    let distinct = keys
        .into_iter()
        .map(|k| ((k.model.clone(), k.identity_key.clone()), k))
        .collect::<std::collections::BTreeMap<_, _>>()
        .into_values()
        .collect::<Vec<_>>();
    let positions: crate::host::Positions = host
        .call_typed(HostRequest::ReadPositions {
            stream: context.binding.stream.clone(),
            records: distinct.clone(),
        })
        .await?;
    if positions.len() != distinct.len() {
        return Err(crate::storage_invalid(
            "preparation position count mismatch",
        ));
    }
    let mut live = vec![];
    for (key, position) in distinct.into_iter().zip(positions) {
        if position.key != key.key() || position.stream != context.binding.stream {
            return Err(crate::storage_invalid(
                "preparation position identity mismatch",
            ));
        }
        if position.kind == crate::stream_members::PositionKind::Upsert {
            live.push(key);
        }
    }
    prepare_keys(config, owner, models, live, prepared, host).await
}

/// Preparation may publish additional members. Callers re-plan until every
/// identity in the bounded final closure has been prepared, then read positions
/// and content without running hooks again.
async fn prepare_keys(
    config: &Config,
    owner: &str,
    models: &std::collections::BTreeMap<String, u64>,
    keys: Vec<crate::host::MemberKey>,
    prepared: &mut std::collections::BTreeSet<(String, String)>,
    host: &impl Host,
) -> Result<bool> {
    let mut changed = false;
    for key in keys {
        if !prepared.insert((key.model.clone(), key.identity_key.clone())) {
            continue;
        }
        if prepared.len() > 10000 {
            return Err(Error::code("publication.capacity"));
        }
        let version = *models
            .get(&key.model)
            .ok_or_else(|| Error::code(code::MODEL_VERSION_UNSUPPORTED))?;
        config
            .contract(&key.model, version)
            .ok_or_else(|| Error::code(code::MODEL_VERSION_UNSUPPORTED))?
            .record_key(&key.model, &key.key().identity)
            .map_err(|_| request_invalid("invalid preparation identity"))?;
        let loaded: crate::host::Loaded = host
            .call_typed(HostRequest::Load {
                mode: Some(crate::host::LoaderMode::Prepare),
                model: key.model.clone(),
                version,
                identities: vec![key.key().identity],
                owner: owner.into(),
            })
            .await?;
        match loaded {
            crate::host::Loaded::Rows(rows) if rows.is_empty() => {}
            crate::host::Loaded::Refused { rejection } => return Err(Error::code(rejection)),
            crate::host::Loaded::Failed { .. } => return Err(Error::code(code::LOADER_FAILED)),
            _ => return Err(Error::code(code::LOADER_INVALID)),
        }
        changed = true;
    }
    Ok(changed)
}

async fn current_changes(
    config: &Config,
    owner: &str,
    context: &RequestContext,
    models: &std::collections::BTreeMap<String, u64>,
    keys: Vec<crate::host::MemberKey>,
    host: &impl Host,
) -> Result<Vec<v04::StreamChange>> {
    normalize_manifest_keys(
        config,
        context,
        &keys.iter().map(|key| key.key()).collect::<Vec<_>>(),
    )?;
    let positions = read_positions_checked(context, &keys, host).await?;
    materialize_authority(config, owner, models, positions, host).await
}

/// Final authority must match every requested pair before canonical loading.
async fn read_positions_checked(
    context: &RequestContext,
    keys: &[crate::host::MemberKey],
    host: &impl Host,
) -> Result<crate::host::Positions> {
    let positions: crate::host::Positions = host
        .call_typed(HostRequest::ReadPositions {
            stream: context.binding.stream.clone(),
            records: keys.to_vec(),
        })
        .await?;
    if positions.len() != keys.len() {
        return Err(crate::storage_invalid("position count mismatch"));
    }
    for (key, position) in keys.iter().zip(&positions) {
        if position.stream != context.binding.stream || position.key != key.key() {
            return Err(crate::storage_invalid("position identity mismatch"));
        }
    }
    Ok(positions)
}

/// Delta and Manifest carry the same final Stream authority. Coverage/range
/// validation stays with their callers; Remove never invokes a Loader.
async fn materialize_authority(
    config: &Config,
    owner: &str,
    models: &std::collections::BTreeMap<String, u64>,
    positions: crate::host::Positions,
    host: &impl Host,
) -> Result<Vec<v04::StreamChange>> {
    let mut changes = vec![];
    for position in positions {
        if position.kind == crate::stream_members::PositionKind::Remove {
            changes.push(v04::StreamChange::Remove {
                key: position.key,
                cursor: position.cursor,
            });
        } else {
            let version = *models
                .get(&position.key.model)
                .ok_or_else(|| Error::code(code::MODEL_VERSION_UNSUPPORTED))?;
            let state = crate::action_results::load_state(
                config,
                owner,
                &position.key,
                version,
                true,
                host,
            )
            .await?;
            changes.push(v04::StreamChange::Upsert {
                record: v04::StreamRecord {
                    key: position.key,
                    cursor: position.cursor,
                    state,
                },
            });
        }
    }
    Ok(changes)
}
pub(crate) async fn bootstrap(
    config: &Config,
    owner: &str,
    bytes: &[u8],
    host: &impl Host,
) -> Result<String> {
    use crate::host::{Acknowledged, BootstrapEffects, ManifestSlice};
    let intent: v04::BootstrapIntent = v04::decode(bytes).map_err(request_invalid)?;
    let request = axton_core::canonical_json(&json!({"kind":"bootstrap","intent":intent}))
        .map_err(internal)?;
    if let Some(saved) = claim_plan(
        config,
        owner,
        intent.context(),
        intent.call_id(),
        &request,
        host,
    )
    .await?
    {
        match &intent {
            v04::BootstrapIntent::Start { .. } | v04::BootstrapIntent::Materialize { .. } => {
                let _: v04::BootstrapStarted = v04::decode(saved.as_bytes()).map_err(internal)?;
            }
            v04::BootstrapIntent::Page { .. } => {
                let _: v04::ManifestPage = v04::decode(saved.as_bytes()).map_err(internal)?;
            }
            v04::BootstrapIntent::Tail { .. } => {
                let _: v04::BootstrapTail = v04::decode(saved.as_bytes()).map_err(internal)?;
            }
        }
        return Ok(saved);
    }
    let _: Acknowledged = host.call_typed(HostRequest::PublicationFence {}).await?;
    let text = match &intent {
        v04::BootstrapIntent::Start {
            context,
            call_id,
            models: declared,
            budget,
            held_keys,
        } => {
            if &models(config, context)? != declared {
                return Err(Error::code("context_mismatch"));
            }
            if *budget > 100000 {
                return Err(Error::code("manifest.capacity"));
            }
            let effects: BootstrapEffects = host
                .call_typed(HostRequest::HandleBootstrap {
                    owner: owner.into(),
                    call_id: call_id.clone(),
                    context: context.clone(),
                })
                .await?;
            crate::settlement::settle_changes(
                config,
                &Default::default(),
                &effects
                    .declarations
                    .into_iter()
                    .map(Into::into)
                    .collect::<Vec<_>>(),
                host,
            )
            .await?;
            let start = crate::head(host, &context.binding.stream).await?;
            let manifest: ManifestSlice = host
                .call_typed(HostRequest::CreateManifest {
                    owner: owner.into(),
                    manifest_id: call_id.clone(),
                    context: context.clone(),
                    start,
                    models: declared.clone(),
                    selected: config
                        .schema
                        .models
                        .iter()
                        .filter(|m| m.bootstrap)
                        .map(|m| m.name.clone())
                        .collect(),
                    held: normalize_manifest_keys(config, context, held_keys)?,
                    budget: *budget,
                })
                .await?;
            encode(&v04::BootstrapStarted {
                context: context.clone(),
                manifest_id: call_id.clone(),
                start,
                total: manifest.total,
            })?
        }
        v04::BootstrapIntent::Materialize {
            context,
            call_id,
            receipt_targets,
            budget,
        } => {
            if *budget > 100000 || receipt_targets.keys.len() as u64 > *budget {
                return Err(Error::code("manifest.capacity"));
            }
            let saved: Option<String> = host
                .call_typed(HostRequest::ReadCall {
                    owner: owner.into(),
                    call_id: receipt_targets.call_id.clone(),
                })
                .await?;
            let receipt: v04::MutationReceipt = v04::decode(
                saved
                    .ok_or_else(|| Error::code("receipt.invalid"))?
                    .as_bytes(),
            )
            .map_err(|_| Error::code("receipt.invalid"))?;
            if receipt.context.binding != context.binding
                || receipt.context.incarnation != context.incarnation
                || receipt.completion.call_id != receipt_targets.call_id
                || !matches!(receipt.completion.outcome, ActionOutcome::Succeeded { .. })
            {
                return Err(Error::code("receipt.invalid"));
            }
            // Saved durable outcome may name a supported older materialization.
            // Only current-context authority is returned; the original descriptor
            // remains verified independently of today's Model set/bootstrap flags.
            models(config, &receipt.context)?;
            normalize_manifest_keys(config, &receipt.context, &receipt_targets.keys)?;
            for key in &receipt_targets.keys {
                if !receipt.targets.iter().any(|target| matches!(target,v04::SettlementTarget::Stream{key:target_key,..} if target_key==key)) { return Err(Error::code("receipt.invalid")); }
            }
            let start = crate::head(host, &context.binding.stream).await?;
            let manifest: ManifestSlice = host
                .call_typed(HostRequest::CreateManifest {
                    owner: owner.into(),
                    manifest_id: call_id.clone(),
                    context: context.clone(),
                    start,
                    models: models(config, context)?,
                    selected: vec![],
                    held: normalize_manifest_keys(config, context, &receipt_targets.keys)?,
                    budget: *budget,
                })
                .await?;
            encode(&v04::BootstrapStarted {
                context: context.clone(),
                manifest_id: call_id.clone(),
                start,
                total: manifest.total,
            })?
        }
        v04::BootstrapIntent::Page {
            context,
            manifest_id,
            from,
            limit,
            ..
        } => {
            if *limit > 1000 {
                return Err(Error::code("page.capacity"));
            }
            let mut prepared = std::collections::BTreeSet::new();
            let manifest: ManifestSlice = loop {
                let manifest: ManifestSlice = host
                    .call_typed(HostRequest::ReadManifest {
                        owner: owner.into(),
                        manifest_id: manifest_id.clone(),
                        context: context.clone(),
                        from: *from,
                        limit: *limit,
                        unique_models: config
                            .schema
                            .models
                            .iter()
                            .filter(|m| {
                                m.unique.iter().any(|fields| {
                                    !m.identity.iter().all(|identity| fields.contains(identity))
                                })
                            })
                            .map(|m| m.name.clone())
                            .collect(),
                    })
                    .await?;
                let keys = manifest
                    .keys
                    .iter()
                    .chain(&manifest.companions)
                    .cloned()
                    .collect();
                if !prepare_stream_keys(
                    config,
                    owner,
                    context,
                    &manifest.models,
                    keys,
                    &mut prepared,
                    host,
                )
                .await?
                {
                    break manifest;
                }
            };
            if manifest.models != models(config, context)?
                || manifest.keys.len() + manifest.companions.len() > 10000
                || manifest.from != *from
                || manifest.to < manifest.from
                || manifest.to > manifest.total
                || manifest.keys.len() as u64 != manifest.to - manifest.from
            {
                return Err(crate::storage_invalid("manifest ordinal coverage invalid"));
            }
            let changes = current_changes(
                config,
                owner,
                context,
                &manifest.models,
                manifest.keys,
                host,
            )
            .await?;
            let companions = current_changes(
                config,
                owner,
                context,
                &manifest.models,
                manifest.companions,
                host,
            )
            .await?;
            bounded_unit(
                config,
                &changes.iter().chain(&companions).collect::<Vec<_>>(),
            )?;
            encode(&v04::ManifestPage {
                context: context.clone(),
                manifest_id: manifest_id.clone(),
                total: manifest.total,
                from: *from,
                to: manifest.to,
                companions,
                items: changes
                    .into_iter()
                    .enumerate()
                    .map(|(index, change)| v04::ManifestItem {
                        ordinal: *from + index as u64,
                        change,
                    })
                    .collect(),
            })?
        }
        v04::BootstrapIntent::Tail {
            context,
            manifest_id,
            ..
        } => {
            let head = crate::head(host, &context.binding.stream).await?;
            let tail: u64 = host
                .call_typed(HostRequest::CaptureTail {
                    owner: owner.into(),
                    manifest_id: manifest_id.clone(),
                    context: context.clone(),
                    head,
                })
                .await?;
            encode(&v04::BootstrapTail {
                context: context.clone(),
                manifest_id: manifest_id.clone(),
                head: tail,
            })?
        }
    };
    save_plan(owner, intent.call_id(), text, host).await
}

fn normalize_manifest_keys(
    config: &Config,
    context: &RequestContext,
    keys: &[axton_core::RecordKey],
) -> Result<Vec<crate::host::MemberKey>> {
    let served = models(config, context)?;
    keys.iter()
        .map(|key| {
            if !config.loaders.contains(&key.model) {
                return Err(Error::code("manifest.identity_invalid"));
            }
            let version = *served
                .get(&key.model)
                .ok_or_else(|| Error::code("manifest.identity_invalid"))?;
            let schema = config
                .contract(&key.model, version)
                .ok_or_else(|| Error::code("manifest.identity_invalid"))?;
            let normalized = schema
                .record_key(&key.model, &key.identity)
                .map_err(request_invalid)?;
            if &normalized != key {
                return Err(Error::code("manifest.identity_invalid"));
            }
            Ok(crate::host::MemberKey::from_key(key))
        })
        .collect()
}

/// Durable named Mutation: canonical outcome plus exact optimistic targets.
/// Device-only companion writes are never named by the server receipt.
pub(crate) async fn mutation(
    config: &Config,
    owner: &str,
    bytes: &[u8],
    host: &impl Host,
) -> Result<String> {
    use crate::host::{Acknowledged, HandledAction, MemberKey, Positions, Tracking};
    let intent: v04::MutationIntent = v04::decode(bytes).map_err(request_invalid)?;
    admit(config, owner, &intent.context, true, host).await?;
    if models(config, &intent.context)? != intent.models {
        return Err(Error::code("context_mismatch"));
    }
    let request = axton_core::canonical_json(&json!({"kind":"mutation","intent":intent}))
        .map_err(internal)?;
    let intent_digest = intent.digest().map_err(internal)?;
    let rejected = |error: &Error| v04::MutationReceipt {
        context: intent.context.clone(),
        intent_digest: intent_digest.clone(),
        completion: CallCompletion {
            call_id: intent.call_id.clone(),
            outcome: ActionOutcome::Failed {
                code: error.code.clone(),
                execution: ExecutionState::Rejected,
            },
        },
        targets: vec![],
    };
    match calls::claim(owner, &intent.call_id, &request, host).await? {
        Claim::Replay(saved) => {
            let receipt: v04::MutationReceipt =
                v04::decode(saved.as_bytes()).map_err(crate::storage_invalid)?;
            receipt
                .admit(&intent, &intent.context)
                .map_err(crate::storage_invalid)?;
            return Ok(saved);
        }
        Claim::Conflict => return encode(&rejected(&Error::code("call.identity_conflict"))),
        Claim::Fresh => {}
    }
    let run = async {
        let _: Acknowledged = host.call_typed(HostRequest::PublicationFence {}).await?;
        let action = config
            .schema
            .action(&intent.name, intent.version)
            .map_err(|_| Error::code("action_version_unsupported"))?;
        if action.kind != axton_core::CallKind::Mutation {
            return Err(Error::code("action.invalid"));
        }
        let args = axton_core::normalize_action_args(&config.schema, action, &intent.args)
            .map_err(|_| Error::code("action.invalid"))?;
        let mut input_targets = crate::settlement::Changes::new();
        for input in &action.inputs {
            if let axton_core::ActionInputDescriptor::Model { name, model, .. } = input {
                for identity in
                    crate::actions::input_identities(&config.schema, model, &args[name], input)?
                {
                    let key = config
                        .schema
                        .record_key(model, &identity)
                        .map_err(request_invalid)?;
                    crate::settlement::insert(&mut input_targets, key)?;
                }
            }
        }
        normalize_manifest_keys(
            config,
            &intent.context,
            &input_targets.values().cloned().collect::<Vec<_>>(),
        )?;
        let handled: HandledAction = host
            .call_typed(HostRequest::HandleAction {
                name: intent.name.clone(),
                version: intent.version,
                arguments: args.clone(),
                owner: owner.into(),
                call_id: intent.call_id.clone(),
                ordinal: 1,
                context: Some(intent.context.clone()),
            })
            .await?;
        let (outputs, extra, declarations) = match handled {
            HandledAction::Rejected { rejection } => return Err(Error::code(rejection)),
            HandledAction::Failed { .. } => return Err(Error::code(code::HANDLER_FAILED)),
            HandledAction::Settled {
                outputs,
                changes,
                declarations,
            } => (outputs, changes, declarations),
        };
        // Inputs are settlement obligations, not implicit publications. A no-op
        // preserves an existing pair cursor and an untracked target stays private.
        let mut changed = crate::settlement::Changes::new();
        for record in &extra {
            crate::settlement::insert(&mut changed, crate::settlement::resolve(config, record)?)?;
        }
        crate::settlement::settle_changes(config, &changed, &declarations, host).await?;
        // All output and input preparation precedes both target evidence and
        // invocation snapshots. Canonical reads below cannot republish afterward.
        let mut keys = crate::action_results::snapshot_keys(config, action, &args, &outputs)?;
        keys.extend(input_targets.values().cloned());
        let mut prepared = std::collections::BTreeSet::new();
        prepare_keys(
            config,
            owner,
            &intent.models,
            keys.iter().map(MemberKey::from_key).collect(),
            &mut prepared,
            host,
        )
        .await?;
        let tracking: Tracking = host
            .call_typed(HostRequest::ReadTracking {
                records: input_targets.values().map(MemberKey::from_key).collect(),
                pairs: vec![],
            })
            .await?;
        let live = tracking
            .iter()
            .filter(|p| p.stream == intent.context.binding.stream)
            .map(|p| MemberKey {
                model: p.model.clone(),
                identity_key: p.identity_key.clone(),
            })
            .collect::<Vec<_>>();
        let positions: Positions = host
            .call_typed(HostRequest::ReadPositions {
                stream: intent.context.binding.stream.clone(),
                records: live.clone(),
            })
            .await?;
        if positions.len() != live.len() {
            return Err(crate::storage_invalid("settlement position count mismatch"));
        }
        let mut authority = std::collections::BTreeMap::new();
        for (key, position) in live.into_iter().zip(positions) {
            if position.stream != intent.context.binding.stream
                || position.key != key.key()
                || position.kind != crate::stream_members::PositionKind::Upsert
            {
                return Err(crate::storage_invalid("live settlement position mismatch"));
            }
            authority.insert(position.key.encoded().map_err(internal)?, position.cursor);
        }
        let mut targets = vec![];
        for (encoded, key) in &input_targets {
            let state = crate::action_results::load_state(
                config,
                owner,
                key,
                intent.models[&key.model],
                true,
                host,
            )
            .await?;
            let record = ReadRecord {
                key: key.clone(),
                cursor: NullCursor,
                state,
            };
            targets.push(match authority.get(encoded) {
                Some(cursor) => v04::SettlementTarget::Stream {
                    key: key.clone(),
                    cursor: *cursor,
                    fallback: record,
                },
                None => v04::SettlementTarget::Private { record },
            });
        }
        let (result, _) = crate::action_results::assemble_snapshots(
            config,
            owner,
            action,
            &args,
            &outputs,
            crate::action_results::SnapshotPolicy {
                models: &intent.models,
                canonical: true,
            },
            host,
        )
        .await?;
        let result = axton_core::validate_action_result(&config.schema, action, &result)
            .map_err(|_| Error::code(code::HANDLER_INVALID))?;
        let receipt = v04::MutationReceipt {
            context: intent.context.clone(),
            intent_digest: intent_digest.clone(),
            completion: CallCompletion {
                call_id: intent.call_id.clone(),
                outcome: ActionOutcome::Succeeded { result },
            },
            targets,
        };
        receipt
            .validate_targets(&input_targets.values().cloned().collect::<Vec<_>>())
            .map_err(internal)?;
        Ok(receipt)
    };
    let (_, text) = calls::complete(owner, &intent.call_id, 1, host, run, rejected, encode).await?;
    Ok(text)
}

fn bounded_unit(config: &Config, value: &impl Serialize) -> Result<()> {
    let limit = config
        .protocol4
        .as_ref()
        .ok_or_else(|| Error::code("protocol.unsupported"))?
        .max_unit_bytes;
    if limit == 0 || serde_json::to_vec(value).map_err(internal)?.len() as u64 > limit {
        return Err(Error::code("constraint_group_capacity"));
    }
    Ok(())
}
