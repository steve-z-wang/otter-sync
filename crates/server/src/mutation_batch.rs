//! One member transaction. Store lock, business savepoint and saved progress
//! are deliberately independent of the transport's complete Batch loop.
use crate::{
    Config, Error, Host, Result,
    host::{Acknowledged, HandledAction, HostExt, HostRequest, Loaded, LoaderMode, MemberKey},
    internal,
    protocol_v05::{self, Publication05},
    request_invalid, storage_invalid,
};
use axton_core::{ActionInputDescriptor, RecordKey};
use axton_protocols::sync as v05;
use serde::Deserialize;
use serde_json::json;
use std::collections::BTreeMap;
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Store {
    principal: String,
    stream: String,
    last_processed_batch_id: u64,
    progress: u64,
    current_digest: Option<String>,
    current_count: Option<u64>,
    last_digest: Option<String>,
    last_count: Option<u64>,
}
pub(crate) async fn execute(
    config: &Config,
    owner: &str,
    request: &v05::MutationRequest,
    ordinal: u64,
    raw_host: &impl Host,
) -> Result<v05::MutationResult> {
    if owner.trim().is_empty() {
        return Err(Error::code(crate::code::PRINCIPAL_INVALID));
    }
    let member = request
        .mutations
        .get(usize::try_from(ordinal).map_err(request_invalid)?)
        .ok_or_else(|| request_invalid("member ordinal out of range"))?;
    let store:Store=protocol_v05::call(raw_host,json!({"op":"claimStore","storeId":request.context.store_id,"principal":owner,"stream":request.context.stream})).await?;
    if store.principal != owner || store.stream != request.context.stream {
        return Err(Error::code("store.binding"));
    }
    models(config, &request.context)?;
    let _: Acknowledged = raw_host
        .call_typed(HostRequest::PublicationFence {})
        .await?;
    let allowed: bool = protocol_v05::call(
        raw_host,
        json!({"op":"admit","owner":owner,"context":request.context}),
    )
    .await?;
    if !allowed {
        return Err(Error::code("stream.forbidden"));
    }
    let count = request.mutations.len() as u64;
    let completed = request.batch_id == store.last_processed_batch_id;
    let current = request.batch_id
        == store
            .last_processed_batch_id
            .checked_add(1)
            .ok_or_else(|| Error::code("batch.sequence"))?;
    if !completed && !current {
        return Err(Error::code("batch.sequence"));
    }
    let (digest, size) = if completed {
        (&store.last_digest, store.last_count)
    } else {
        (&store.current_digest, store.current_count)
    };
    if (completed || digest.is_some())
        && (digest.as_deref() != Some(request.digest.as_str()) || size != Some(count))
    {
        return Err(Error::code("batch.conflict"));
    }
    if completed || ordinal < store.progress {
        let saved:Option<v05::MutationResult>=protocol_v05::call(raw_host,json!({"op":"readResult","storeId":request.context.store_id,"batchId":request.batch_id,"ordinal":ordinal})).await?;
        let saved = saved.ok_or_else(|| storage_invalid("committed member result missing"))?;
        if saved.mutation_id != member.id {
            return Err(storage_invalid("saved member mismatch"));
        }
        return Ok(saved);
    }
    if ordinal != store.progress {
        return Err(Error::code("batch.progress"));
    }
    if store.current_digest.is_none() {
        let _:Acknowledged=protocol_v05::call(raw_host,json!({"op":"beginBatch","storeId":request.context.store_id,"batchId":request.batch_id,"digest":request.digest,"count":count})).await?;
    }
    let args = match protocol_v05::validate_member(config, member)? {
        protocol_v05::MutationInput::Supported(args) => args,
        protocol_v05::MutationInput::Unsupported => {
            let result = v05::MutationResult {
                mutation_id: member.id,
                outcome: v05::MutationOutcome::Rejected {
                    code: crate::code::MUTATION_VERSION_UNSUPPORTED.into(),
                    message: None,
                },
            };
            let _: Acknowledged = protocol_v05::call(
                raw_host,
                json!({
                    "op":"saveResult", "storeId":request.context.store_id,
                    "batchId":request.batch_id, "ordinal":ordinal, "count":count,
                    "result":result,
                }),
            )
            .await?;
            return Ok(result);
        }
    };
    let host = Publication05::new(raw_host);
    let _: Acknowledged = host.call_typed(HostRequest::PublicationFence {}).await?;
    let _: Acknowledged = host
        .call_typed(HostRequest::Savepoint {
            ordinal: ordinal + 1,
        })
        .await?;
    let handled: HandledAction = host
        .call_typed(HostRequest::HandleAction {
            name: member.name.clone(),
            version: member.version,
            arguments: args.clone(),
            owner: owner.into(),
            call_id: format!(
                "{}:{}:{}",
                request.context.store_id, request.batch_id, member.id
            ),
            ordinal: ordinal + 1,
            context: Some(protocol_v05::handler_context(owner, &request.context)),
        })
        .await?;
    let outcome = match handled {
        HandledAction::Rejected { rejection } => {
            let _: Acknowledged = host
                .call_typed(HostRequest::Rollback {
                    ordinal: ordinal + 1,
                })
                .await?;
            v05::MutationOutcome::Rejected {
                code: rejection,
                message: None,
            }
        }
        HandledAction::Failed { error } => {
            return Err(Error::new(crate::code::HANDLER_FAILED, error));
        }
        HandledAction::Settled {
            outputs,
            changes,
            declarations,
        } => {
            let accepted:Result<v05::MutationOutcome>=async {
            let mut changed = crate::settlement::Changes::new();
            for r in &changes {
                crate::settlement::insert(&mut changed, crate::settlement::resolve(config, r)?)?;
            }
            crate::settlement::settle_changes(config, &changed, &declarations, &host).await?;
            let action = config
                .schema
                .action(&member.name, member.version)
                .map_err(request_invalid)?;
            let models = models(config, &request.context)?;
            let mut inputs = crate::settlement::Changes::new();
            for input in &action.inputs {
                if let ActionInputDescriptor::Model { name, model, .. } = input {
                    for identity in
                        crate::actions::input_identities(&config.schema, model, &args[name], input)?
                    {
                        crate::settlement::insert(
                            &mut inputs,
                            config
                                .schema
                                .record_key(model, &identity)
                                .map_err(request_invalid)?,
                        )?;
                    }
                }
            }
            let mut keys = crate::action_results::snapshot_keys(config, action, &args, &outputs)?;
            keys.extend(inputs.values().cloned());
            prepare(config, owner, &models, keys, &host).await?;
            let positions:Vec<Option<crate::stream_members::MemberPosition>>=protocol_v05::call(raw_host,json!({"op":"targetPositions","stream":request.context.stream,"records":inputs.values().map(MemberKey::from_key).collect::<Vec<_>>()})).await?;
            if positions.len() != inputs.len() {
                return Err(storage_invalid("target position count mismatch"));
            }
            let mut targets = vec![];
            for (key, position) in inputs.values().zip(positions) {
                let state = crate::action_results::load_state(
                    config,
                    owner,
                    key,
                    *models
                        .get(&key.model)
                        .ok_or_else(|| request_invalid("Model outside descriptor"))?,
                    true,
                    &host,
                )
                .await?;
                let key05 = v05::RecordKey {
                    model: key.model.clone(),
                    identity: key.identity.clone(),
                };
                let record = v05::ReadRecord {
                    key: key05.clone(),
                    cursor: (),
                    state,
                };
                targets.push(match position {
                    Some(p) if p.kind == crate::stream_members::PositionKind::Upsert => {
                        if p.key != *key || p.stream != request.context.stream {
                            return Err(storage_invalid("target position mismatch"));
                        }
                        v05::SettlementTarget::Stream {
                            key: key05,
                            cursor: p.cursor,
                            fallback: record,
                        }
                    }
                    _ => v05::SettlementTarget::Private { record },
                });
            }
            let (result, _) = crate::action_results::assemble_snapshots(
                config,
                owner,
                action,
                &args,
                &outputs,
                crate::action_results::SnapshotPolicy {
                    models: &models,
                    canonical: true,
                    cache: true,
                },
                &host,
            )
            .await?;
            let result = axton_core::validate_action_result(&config.schema, action, &result)
                .map_err(internal)?;
            let sync_cursor = crate::head(&host, &request.context.stream).await?;
            Ok(v05::MutationOutcome::Accepted {sync_cursor,result,targets})
            }.await;
            match accepted {
                Ok(outcome) => outcome,
                Err(error) => {
                    let Some(code) = host.refusal() else {
                        return Err(error);
                    };
                    let _: Acknowledged = host
                        .call_typed(HostRequest::Rollback {
                            ordinal: ordinal + 1,
                        })
                        .await?;
                    v05::MutationOutcome::Rejected {
                        code,
                        message: None,
                    }
                }
            }
        }
    };
    let _: Acknowledged = host
        .call_typed(HostRequest::Release {
            ordinal: ordinal + 1,
        })
        .await?;
    let result = v05::MutationResult {
        mutation_id: member.id,
        outcome,
    };
    let _:Acknowledged=protocol_v05::call(raw_host,json!({"op":"saveResult","storeId":request.context.store_id,"batchId":request.batch_id,"ordinal":ordinal,"count":count,"result":result})).await?;
    Ok(result)
}
pub(crate) fn models(
    config: &Config,
    context: &v05::RequestContext,
) -> Result<BTreeMap<String, u64>> {
    let generation = config
        .protocol5
        .as_ref()
        .map(|p| p.projection_generation.as_str())
        .unwrap_or("1");
    let active = v05::materialization_id(&config.schema, generation).map_err(internal)?;
    crate::materialization::model_versions(
        config,
        &context.materialization,
        &active,
        &config
            .protocol5
            .as_ref()
            .map(|p| p.materializations.clone())
            .unwrap_or_default(),
        v05::materialization_id,
    )
}

async fn prepare(
    _config: &Config,
    owner: &str,
    models: &BTreeMap<String, u64>,
    keys: Vec<RecordKey>,
    host: &impl Host,
) -> Result<()> {
    let keys: BTreeMap<String, RecordKey> = keys
        .into_iter()
        .map(|k| Ok((k.encoded().map_err(internal)?, k)))
        .collect::<Result<_>>()?;
    if keys.len() > 10000 {
        return Err(Error::code("publication.capacity"));
    }
    for key in keys.values() {
        let loaded: Loaded = host
            .call_typed(HostRequest::Load {
                mode: Some(LoaderMode::Prepare),
                model: key.model.clone(),
                version: *models
                    .get(&key.model)
                    .ok_or_else(|| request_invalid("Model outside descriptor"))?,
                identities: vec![key.identity.clone()],
                owner: owner.into(),
            })
            .await?;
        if !matches!(loaded,Loaded::Rows(ref rows) if rows.is_empty()) {
            return Err(Error::code(crate::code::LOADER_FAILED));
        }
    }
    Ok(())
}
