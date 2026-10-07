//! Finite fenced authority, frozen once; continuation never invokes a Loader.
use crate::{
    Config, Error, Host, Result,
    host::{Acknowledged, HostExt, HostRequest, Loaded, LoaderMode, MemberKey},
    internal,
    protocol_v05::{Publication05, call},
    request_invalid, storage_invalid,
};
use axton_core::{
    canonical_json,
    v05::{self, Validate},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::BTreeMap;
fn encode<T: serde::Serialize + Validate>(v: &T) -> Result<String> {
    String::from_utf8(v05::encode(v).map_err(internal)?).map_err(internal)
}
#[derive(Deserialize)]
struct Binding {
    principal: String,
    stream: String,
}
async fn bind(owner: &str, context: &v05::RequestContext, host: &impl Host) -> Result<()> {
    crate::principal(owner)?;
    let s:Binding=call(host,json!({"op":"claimStore","storeId":context.store_id,"principal":owner,"stream":context.stream})).await?;
    if s.principal != owner || s.stream != context.stream {
        return Err(Error::code("store.binding"));
    }
    let allowed: bool = call(host, json!({"op":"admit","owner":owner,"context":context})).await?;
    if !allowed {
        return Err(Error::code("stream.forbidden"));
    }
    Ok(())
}
async fn fence(host: &impl Host) -> Result<()> {
    let _: Acknowledged = host.call_typed(HostRequest::PublicationFence {}).await?;
    Ok(())
}
async fn prepare(
    config: &Config,
    owner: &str,
    context: &v05::RequestContext,
    host: &impl Host,
) -> Result<()> {
    let done: bool = call(
        host,
        json!({"op":"bootstrapState","storeId":context.store_id}),
    )
    .await?;
    if !done {
        let effects:crate::host::BootstrapEffects=call(host,json!({"op":"handleBootstrap05","owner":owner,"storeId":context.store_id,"stream":context.stream})).await?;
        let declarations = effects
            .declarations
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>();
        crate::settlement::settle_changes(config, &Default::default(), &declarations, host).await?;
        let _: Acknowledged = call(
            host,
            json!({"op":"finishBootstrap","storeId":context.store_id}),
        )
        .await?;
    }
    Ok(())
}
/// Handshake does not carry a client cursor. Preparation and initial head commit together.
pub async fn handshake05(
    config: &Config,
    owner: &str,
    bytes: &[u8],
    raw: &impl Host,
) -> Result<String> {
    let r: v05::HandshakeRequest = v05::decode(bytes).map_err(request_invalid)?;
    let context = v05::RequestContext {
        protocol: 5,
        store_id: r.store_id.clone(),
        stream: r.stream.clone(),
        materialization: v05::materialization_id(
            &config.schema,
            config
                .protocol5
                .as_ref()
                .map(|p| p.projection_generation.as_str())
                .unwrap_or("1"),
        )
        .map_err(internal)?,
    };
    bind(owner, &context, raw).await?;
    let host = Publication05::new(raw);
    fence(&host).await?;
    prepare(config, owner, &context, &host).await?;
    let head: u64 = call(&host, json!({"op":"deliveryHead","stream":r.stream})).await?;
    encode(&v05::HandshakeResponse {
        protocol: 5,
        store_id: r.store_id,
        stream: r.stream,
        head,
    })
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Position {
    model: String,
    identity_key: String,
    cursor: u64,
    kind: String,
}
fn context_schema<'a>(config: &'a Config, context: &v05::RequestContext) -> &'a axton_core::Schema {
    config
        .protocol5
        .as_ref()
        .and_then(|p| p.materializations.get(&context.materialization))
        .map(|m| &m.schema)
        .unwrap_or(&config.schema)
}
fn bootstrap_model(
    config: &Config,
    context: &v05::RequestContext,
    name: &str,
    version: u64,
) -> bool {
    let schema = context_schema(config, context);
    schema
        .models
        .iter()
        .any(|m| m.name == name && m.version == version && m.bootstrap)
}
fn selection(
    config: &Config,
    context: &v05::RequestContext,
    bootstrap: bool,
) -> Result<Option<Vec<String>>> {
    let models = crate::mutation_batch::models(config, context)?;
    Ok(Some({
        models
            .iter()
            .filter(|(m, v)| !bootstrap || bootstrap_model(config, context, m, **v))
            .map(|(m, _)| m.clone())
            .collect()
    }))
}
async fn prepared_candidates(
    config: &Config,
    owner: &str,
    context: &v05::RequestContext,
    after: u64,
    models: Option<Vec<String>>,
    keys: Option<Vec<MemberKey>>,
    host: &impl Host,
) -> Result<Vec<Position>> {
    let versions = crate::mutation_batch::models(config, context)?;
    let mut prepared = std::collections::BTreeSet::new();
    loop {
        let positions:Option<Vec<Position>>=call(host,json!({"op":"deliveryCandidates","stream":context.stream,"after":after,"models":models,"keys":keys,"capacity":100000})).await?;
        let positions = positions.ok_or_else(|| Error::code("delivery.capacity"))?;
        let fresh = positions
            .iter()
            .filter(|p| {
                p.kind == "upsert" && !prepared.contains(&(p.model.clone(), p.identity_key.clone()))
            })
            .collect::<Vec<_>>();
        if fresh.is_empty() {
            return Ok(positions);
        }
        let mut groups = BTreeMap::<String, Vec<Value>>::new();
        for p in fresh {
            prepared.insert((p.model.clone(), p.identity_key.clone()));
            groups
                .entry(p.model.clone())
                .or_default()
                .push(serde_json::from_str(&p.identity_key).map_err(storage_invalid)?);
        }
        if prepared.len() > 100000 {
            return Err(Error::code("delivery.capacity"));
        }
        for (model, identities) in groups {
            let version = *versions
                .get(&model)
                .ok_or_else(|| Error::code("context_mismatch"))?;
            for ids in identities.chunks(500) {
                let loaded: Loaded = host
                    .call_typed(HostRequest::Load {
                        mode: Some(LoaderMode::Prepare),
                        model: model.clone(),
                        version,
                        identities: ids.to_vec(),
                        owner: owner.into(),
                    })
                    .await?;
                match loaded {
                    Loaded::Rows(rows) if rows.is_empty() => {}
                    Loaded::Refused { rejection } => return Err(Error::code(rejection)),
                    _ => return Err(Error::code("loader.failed")),
                }
            }
        }
    }
}
async fn materialize(
    config: &Config,
    owner: &str,
    context: &v05::RequestContext,
    positions: Vec<Position>,
    host: &impl Host,
) -> Result<Vec<v05::AuthorityChange>> {
    let versions = crate::mutation_batch::models(config, context)?;
    let mut changes = Vec::new();
    let mut staged_bytes = 0usize;
    let mut by_model = BTreeMap::<String, Vec<Position>>::new();
    for p in positions {
        if p.kind == "remove" {
            changes.push(v05::AuthorityChange::Remove {
                key: v05::RecordKey {
                    model: p.model,
                    identity: serde_json::from_str(&p.identity_key).map_err(storage_invalid)?,
                },
                cursor: p.cursor,
            });
        } else {
            by_model.entry(p.model.clone()).or_default().push(p);
        }
    }
    for (model, group) in by_model {
        let version = *versions
            .get(&model)
            .ok_or_else(|| Error::code("context_mismatch"))?;
        for group in group.chunks(500) {
            let keys = group
                .iter()
                .map(|p| serde_json::from_str(&p.identity_key).map_err(storage_invalid))
                .collect::<Result<Vec<Value>>>()?;
            let loaded: Loaded = host
                .call_typed(HostRequest::Load {
                    mode: Some(LoaderMode::Canonical),
                    model: model.clone(),
                    version,
                    identities: keys.clone(),
                    owner: owner.into(),
                })
                .await?;
            let rows = match loaded {
                Loaded::Rows(rows) => rows,
                Loaded::Refused { rejection } => return Err(Error::code(rejection)),
                Loaded::Failed { .. } => return Err(Error::code("loader.failed")),
            };
            let schema = config
                .contract(&model, version)
                .ok_or_else(|| Error::code("model_version_unsupported"))?;
            if rows.len() != group.len() {
                return Err(Error::code("loader.invalid"));
            }
            for ((p, identity), row) in group.iter().zip(keys).zip(rows) {
                let state = match row {
                    Some(row) => schema
                        .normalize_state(&model, &row)
                        .map_err(|_| Error::code("loader.invalid"))?,
                    None => Value::Null,
                };
                let change = v05::AuthorityChange::Record {
                    key: v05::RecordKey {
                        model: model.clone(),
                        identity,
                    },
                    cursor: p.cursor,
                    state,
                };
                staged_bytes = staged_bytes
                    .checked_add(serde_json::to_vec(&change).map_err(internal)?.len())
                    .ok_or_else(|| Error::code("delivery.capacity"))?;
                if staged_bytes > 256 * 1024 * 1024 {
                    return Err(Error::code("delivery.capacity"));
                }
                changes.push(change);
            }
        }
    }
    Ok(changes)
}
fn coalesce(units: Vec<v05::DeliveryUnit>, target: usize) -> Vec<v05::DeliveryUnit> {
    let mut packed: Vec<v05::DeliveryUnit> = Vec::new();
    for mut unit in units {
        if let Some(last) = packed
            .last_mut()
            .filter(|last| last.changes.len() + unit.changes.len() <= target)
        {
            last.changes.append(&mut unit.changes);
            if unit.through.is_some() {
                last.through = unit.through;
            }
        } else {
            unit.index = packed.len() as u64;
            packed.push(unit);
        }
    }
    packed
}
fn units(
    schema: &axton_core::Schema,
    changes: &[v05::AuthorityChange],
    after: u64,
    through: u64,
    head: u64,
) -> Result<Vec<v05::DeliveryUnit>> {
    let unique = schema
        .models
        .iter()
        .filter(|m| !m.unique.is_empty())
        .map(|m| m.name.clone())
        .collect();
    // Conservative cascade closure: navigation-only relations impose no existence constraint.
    let mut representatives = BTreeMap::new();
    let mut dependencies = Vec::new();
    for c in changes {
        representatives
            .entry(c.key().model.clone())
            .or_insert_with(|| c.key().clone());
    }
    for model in &schema.models {
        for relation in &model.relations {
            if relation.on_delete == "delete"
                && let (Some(a), Some(b)) = (
                    representatives.get(&model.name),
                    representatives.get(&relation.target),
                )
            {
                for c in changes
                    .iter()
                    .filter(|c| c.key().model == model.name || c.key().model == relation.target)
                {
                    dependencies.push((a.clone(), c.key().clone()));
                }
                dependencies.push((a.clone(), b.clone()));
            }
        }
    }
    v05::plan_units(changes, &unique, &dependencies, after, through, head)
        .map(|units| coalesce(units, 500))
        .map_err(internal)
}
/// An immutable plan was constructed and fully validated before persistence.
/// Continuation verifies only the indexed fragment, never rescans the manifest.
pub(crate) fn verify_indexed_part(
    header: &v05::DeliveryHeader,
    part: &v05::DeliveryPart,
) -> Result<()> {
    part.validate().map_err(storage_invalid)?;
    if part.plan_id != header.plan_id
        || part.plan_digest != header.digest
        || header
            .units
            .get(part.unit as usize)
            .and_then(|u| u.parts.get(part.part as usize))
            != Some(&v05::part_digest(part).map_err(storage_invalid)?)
        || part.changes.iter().any(|c| {
            c.cursor() > header.observed_head
                || header.after.is_some_and(|after| c.cursor() <= after)
        })
    {
        return Err(storage_invalid("indexed part mismatch"));
    }
    Ok(())
}
fn verify_continuation(
    d: &v05::DeliveryResponse,
    c: &v05::Continuation,
    context: &v05::RequestContext,
) -> Result<()> {
    if d.header.context != *context
        || d.header.plan_id != c.plan_id
        || d.header.digest != c.digest
        || d.parts.len() != 1
        || d.parts[0].unit != c.unit
        || d.parts[0].part != c.part
    {
        return Err(storage_invalid("continuation correlation mismatch"));
    }
    verify_indexed_part(&d.header, &d.parts[0])
}
fn encode_persisted<T: serde::Serialize>(v: &T) -> Result<String> {
    serde_json::to_string(v).map_err(internal)
}
fn intent<T: serde::Serialize>(r: &T) -> Result<String> {
    let mut v = serde_json::to_value(r).map_err(internal)?;
    v.as_object_mut().unwrap().remove("continuation");
    canonical_json(&v).map_err(internal)
}
async fn persist(
    owner: &str,
    intent: &str,
    frozen: v05::FrozenDelivery,
    host: &impl Host,
) -> Result<v05::DeliveryResponse> {
    let saved:bool=call(host,json!({"op":"saveDelivery","owner":owner,"intent":intent,"header":frozen.header,"parts":frozen.parts})).await?;
    if !saved {
        return Err(Error::code("delivery.capacity"));
    }
    Ok(v05::DeliveryResponse {
        header: frozen.header,
        parts: vec![frozen.parts[0].clone()],
    })
}
pub async fn process_delivery05(
    config: &Config,
    owner: &str,
    bytes: &[u8],
    raw: &impl Host,
) -> Result<String> {
    let r: v05::DeltaRequest = v05::decode(bytes).map_err(request_invalid)?;
    crate::mutation_batch::models(config, &r.context)?;
    bind(owner, &r.context, raw).await?;
    let saved_intent = intent(&r)?;
    if let Some(c) = &r.continuation {
        let d:Option<v05::DeliveryResponse>=call(raw,json!({"op":"readDelivery","owner":owner,"context":r.context,"intent":saved_intent,"continuation":c})).await?;
        let d = d.ok_or_else(|| Error::code("delivery.expired"))?;
        verify_continuation(&d, c, &r.context)?;
        if d.header.after != Some(r.after)
            || d.header.through != Some(r.through)
            || d.header.bootstrap != r.bootstrap
            || d.header.owner.is_some()
        {
            return Err(storage_invalid("delta range mismatch"));
        }
        return encode_persisted(&d);
    }
    let host = Publication05::new(raw);
    fence(&host).await?;
    prepare(config, owner, &r.context, &host).await?;
    let positions = prepared_candidates(
        config,
        owner,
        &r.context,
        r.after,
        selection(config, &r.context, r.bootstrap)?,
        None,
        &host,
    )
    .await?;
    let head: u64 = call(
        &host,
        json!({"op":"deliveryHead","stream":r.context.stream}),
    )
    .await?;
    if r.through > head {
        return Err(request_invalid("through ahead of head"));
    }
    let changes = materialize(config, owner, &r.context, positions, &host).await?;
    let units = units(
        context_schema(config, &r.context),
        &changes,
        r.after,
        r.through,
        head,
    )?;
    let now: u64 = call(&host, json!({"op":"deliveryNow"})).await?;
    let frozen = v05::freeze_delivery(
        r.context.clone(),
        uuid::Uuid::new_v4().to_string(),
        if r.bootstrap {
            v05::DeliveryPurpose::Bootstrap
        } else {
            v05::DeliveryPurpose::Sync
        },
        r.after,
        r.through,
        head,
        now + 300000,
        units,
        500,
    )
    .map_err(internal)?;
    encode(&persist(owner, &saved_intent, frozen, &host).await?)
}
pub async fn process_materialization05(
    config: &Config,
    owner: &str,
    bytes: &[u8],
    raw: &impl Host,
) -> Result<String> {
    let r: v05::MaterializationRequest = v05::decode(bytes).map_err(request_invalid)?;
    let versions = crate::mutation_batch::models(config, &r.context)?;
    bind(owner, &r.context, raw).await?;
    if let v05::MaterializationOwner::Schema {
        previous_materialization,
    } = &r.owner
    {
        let previous = v05::RequestContext {
            materialization: previous_materialization.clone(),
            ..r.context.clone()
        };
        crate::mutation_batch::models(config, &previous)?;
    }
    let saved_intent = intent(&r)?;
    if let Some(c) = &r.continuation {
        let delivery:Option<v05::DeliveryResponse>=call(raw,json!({"op":"readDelivery","owner":owner,"context":r.context,"intent":saved_intent,"continuation":c})).await?;
        let delivery = delivery.ok_or_else(|| Error::code("delivery.expired"))?;
        let response = v05::MaterializationResponse {
            request_id: r.request_id.clone(),
            delivery,
        };
        verify_continuation(&response.delivery, c, &r.context)?;
        if response.delivery.header.owner.as_ref() != Some(&r.owner)
            || response.delivery.header.after.is_some()
            || response.delivery.header.through.is_some()
        {
            return Err(storage_invalid("materialization owner mismatch"));
        }
        return encode_persisted(&response);
    }
    if let v05::MaterializationOwner::Settlement {
        batch_id,
        mutation_id,
    } = r.owner
    {
        let results: Vec<v05::MutationResult> = call(
            raw,
            json!({"op":"readResults","storeId":r.context.store_id,"batchId":batch_id}),
        )
        .await?;
        let result = results
            .iter()
            .find(|v| v.mutation_id == mutation_id)
            .ok_or_else(|| request_invalid("unknown settlement owner"))?;
        let targets = match &result.outcome {
            v05::MutationOutcome::Accepted { targets, .. } => targets,
            _ => return Err(request_invalid("rejected settlement owner")),
        };
        if r.keys.iter().any(|k| {
            !targets
                .iter()
                .any(|t| matches!(t,v05::SettlementTarget::Stream{key,..} if key==k))
        }) {
            return Err(request_invalid("unowned settlement target"));
        }
    }
    for (model, version) in &r.models {
        if versions.get(model) != Some(version)
            || !bootstrap_model(config, &r.context, model, *version)
        {
            return Err(request_invalid("unselected schema Model"));
        }
    }
    let host = Publication05::new(raw);
    fence(&host).await?;
    let positions = prepared_candidates(
        config,
        owner,
        &r.context,
        0,
        Some(r.models.keys().cloned().collect()),
        Some(
            r.keys
                .iter()
                .map(|k| {
                    MemberKey::from_key(&axton_core::RecordKey {
                        model: k.model.clone(),
                        identity: k.identity.clone(),
                    })
                })
                .collect(),
        ),
        &host,
    )
    .await?;
    let head: u64 = call(
        &host,
        json!({"op":"deliveryHead","stream":r.context.stream}),
    )
    .await?;
    let changes = materialize(config, owner, &r.context, positions, &host).await?;
    let mut units = units(context_schema(config, &r.context), &changes, 0, head, head)?;
    for u in &mut units {
        u.through = None;
    }
    let now: u64 = call(&host, json!({"op":"deliveryNow"})).await?;
    let frozen = v05::freeze_materialization(
        r.context.clone(),
        uuid::Uuid::new_v4().to_string(),
        r.owner.clone(),
        head,
        now + 300000,
        units,
        500,
    )
    .map_err(internal)?;
    encode(&v05::MaterializationResponse {
        request_id: r.request_id,
        delivery: persist(owner, &saved_intent, frozen, &host).await?,
    })
}
/// Query/Fetch reuse retained normalization and snapshot assembly; snapshots
/// carry null cursors; only explicit authenticated Query tracking enrolls.
pub async fn process_read05(
    config: &Config,
    owner: &str,
    bytes: &[u8],
    raw: &impl Host,
) -> Result<String> {
    let r: v05::ReadRequest = v05::decode(bytes).map_err(request_invalid)?;
    let versions = crate::mutation_batch::models(config, &r.context)?;
    bind(owner, &r.context, raw).await?;
    let host = Publication05::new(raw);
    fence(&host).await?;
    let _: Acknowledged = host
        .call_typed(HostRequest::Savepoint { ordinal: 1 })
        .await?;
    let read = async {
        match &r.invocation {
            v05::ReadInvocation::Fetch { key, version } => {
                if versions.get(&key.model) != Some(version) {
                    return Err(Error::code("model_version_unsupported"));
                }
                let k = config
                    .contract(&key.model, *version)
                    .ok_or_else(|| Error::code("model_version_unsupported"))?
                    .record_key(&key.model, &key.identity)
                    .map_err(request_invalid)?;
                let state = crate::action_results::load_one_canonical_state(
                    config, owner, &k, *version, &host,
                )
                .await?;
                let result = if state.is_null() {
                    Value::Null
                } else {
                    let mut f = k.identity.as_object().unwrap().clone();
                    f.extend(state.as_object().unwrap().clone());
                    Value::Object(f)
                };
                Ok((
                    result,
                    vec![v05::ReadRecord {
                        key: v05::RecordKey {
                            model: k.model,
                            identity: k.identity,
                        },
                        cursor: (),
                        state,
                    }],
                ))
            }
            v05::ReadInvocation::Query {
                name,
                version,
                args,
            } => {
                let action = config
                    .schema
                    .action(name, *version)
                    .map_err(|_| Error::code("action_version_unsupported"))?;
                if action.kind != axton_core::CallKind::Query {
                    return Err(request_invalid("expected Query"));
                }
                let args = axton_core::normalize_action_args(&config.schema, action, args)
                    .map_err(request_invalid)?;
                let handled: crate::host::HandledAction = host
                    .call_typed(HostRequest::HandleAction {
                        name: name.clone(),
                        version: *version,
                        arguments: args.clone(),
                        owner: owner.into(),
                        call_id: r.request_id.clone(),
                        ordinal: 1,
                        // Existing host carrier: only scopedStreams reads binding.stream.
                        // These internal fields never select protocol-4 authority or reach the wire.
                        context: Some(axton_core::v04::RequestContext {
                            protocol: 4,
                            binding: axton_core::v04::StoreBinding {
                                backend: "protocol5".into(),
                                viewer: owner.into(),
                                stream: r.context.stream.clone(),
                                contract: r.context.materialization.clone(),
                            },
                            materialization: r.context.materialization.clone(),
                            incarnation: r.context.store_id.clone(),
                        }),
                    })
                    .await?;
                let outputs = match handled {
                    crate::host::HandledAction::Settled {
                        outputs,
                        changes,
                        declarations,
                    } if changes.is_empty()
                        && !declarations
                            .iter()
                            .any(|d| matches!(d, crate::host::StreamIntent::Invalidate { .. })) =>
                    {
                        crate::settlement::settle_changes(
                            config,
                            &Default::default(),
                            &declarations,
                            &host,
                        )
                        .await?;
                        outputs
                    }
                    crate::host::HandledAction::Settled { .. } => {
                        return Err(Error::code("query.effects_forbidden"));
                    }
                    crate::host::HandledAction::Rejected { rejection } => {
                        return Err(Error::code(rejection));
                    }
                    crate::host::HandledAction::Failed { .. } => {
                        return Err(Error::code("handler.failed"));
                    }
                };
                let (result, snapshots) = crate::action_results::assemble_snapshots(
                    config,
                    owner,
                    action,
                    &args,
                    &outputs,
                    crate::action_results::SnapshotPolicy {
                        models: &versions,
                        canonical: true,
                    },
                    &host,
                )
                .await?;
                let result = axton_core::validate_action_result(&config.schema, action, &result)
                    .map_err(|_| Error::code("handler.invalid"))?;
                Ok((
                    result,
                    snapshots
                        .into_iter()
                        .map(|s| v05::ReadRecord {
                            key: v05::RecordKey {
                                model: s.key.model,
                                identity: s.key.identity,
                            },
                            cursor: (),
                            state: s.state,
                        })
                        .collect(),
                ))
            }
        }
    };
    let answer = read.await;
    if answer.is_err() {
        let _: Acknowledged = host
            .call_typed(HostRequest::Rollback { ordinal: 1 })
            .await?;
    }
    let _: Acknowledged = host.call_typed(HostRequest::Release { ordinal: 1 }).await?;
    let (outcome, records) = match answer {
        Ok((result, records)) => (v05::ReadOutcome::Succeeded { result }, records),
        Err(e)
            if !matches!(
                e.code.as_str(),
                "host"
                    | "host.invalid"
                    | "storage.invalid"
                    | "internal"
                    | "loader.failed"
                    | "handler.failed"
                    | "loader.invalid"
                    | "handler.invalid"
            ) =>
        {
            (
                v05::ReadOutcome::Failed {
                    code: e.code,
                    message: Some(e.message),
                },
                vec![],
            )
        }
        Err(e) => return Err(e),
    };
    encode(&v05::ReadResponse {
        context: r.context,
        request_id: r.request_id,
        outcome,
        records,
    })
}

/// A fresh live offer observes its finite through under the same transaction.
/// Subsequent fragments keep that exact bound; no delayed notification cursor
/// is attached to content loaded at a later head.
pub async fn process_live05(
    config: &Config,
    owner: &str,
    bytes: &[u8],
    host: &impl Host,
) -> Result<String> {
    let mut r: v05::DeltaRequest = v05::decode(bytes).map_err(request_invalid)?;
    if r.continuation.is_none() {
        bind(owner, &r.context, host).await?;
        fence(host).await?;
        r.through = call(host, json!({"op":"deliveryHead","stream":r.context.stream})).await?;
    }
    process_delivery05(config, owner, &v05::encode(&r).map_err(internal)?, host).await
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn indexed_continuation_rejects_changed_payload_without_scanning_other_units() {
        let context = v05::RequestContext {
            protocol: 5,
            store_id: "s".into(),
            stream: "User:a".into(),
            materialization: "m".into(),
        };
        let frozen = v05::freeze_delivery(
            context,
            "p".into(),
            v05::DeliveryPurpose::Sync,
            0,
            1,
            1,
            1000,
            vec![v05::DeliveryUnit {
                index: 0,
                through: Some(1),
                changes: vec![],
            }],
            500,
        )
        .unwrap();
        assert!(verify_indexed_part(&frozen.header, &frozen.parts[0]).is_ok());
        let mut part = frozen.parts[0].clone();
        part.changes.push(v05::AuthorityChange::Remove {
            key: v05::RecordKey {
                model: "Entry".into(),
                identity: json!({"id":"x"}),
            },
            cursor: 1,
        });
        assert!(verify_indexed_part(&frozen.header, &part).is_err());
    }
    #[test]
    fn coalesces_independent_components_without_splitting_atomic_units() {
        let unit = |index, rows| v05::DeliveryUnit {
            index,
            through: Some(index + 1),
            changes: (0..rows)
                .map(|i| v05::AuthorityChange::Record {
                    key: v05::RecordKey {
                        model: "Entry".into(),
                        identity: json!({"id":format!("{index}-{i}")}),
                    },
                    cursor: index + 1,
                    state: json!({}),
                })
                .collect(),
        };
        let packed = coalesce(
            vec![unit(0, 200), unit(1, 200), unit(2, 800), unit(3, 1)],
            500,
        );
        assert_eq!(packed.len(), 3);
        assert_eq!(packed[0].changes.len(), 400);
        assert_eq!(packed[0].through, Some(2));
        assert_eq!(packed[1].changes.len(), 800);
        assert_eq!(packed[2].index, 2);
    }
}
