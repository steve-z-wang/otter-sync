//! Shared transactional Action executor. A stored call response is independent
//! of the batch receipt, so replay retains its own result and authority.
use crate::host::{Acknowledged, Claimed, ClaimedCall, HandledAction, HostExt, HostRequest};
use crate::readback::{self, Outcome};
use crate::settlement::{self, Changes};
use crate::{
    Config, Error, Host, Result, code, internal, principal, request_invalid, storage_invalid,
};
use axton_core::{
    ActionInputDescriptor, ActionIntent, ActionOutcome, AuthorityRecord, CallCompletion, CallKind,
    DirectActionRequest, DirectActionResponse, ExecutionState, PushReceipt, PushRequest, Rejection,
    canonical_json, normalize_action_args, normalize_call_id, validate_action_result,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActionResponse {
    pub completion: CallCompletion,
    pub records: Vec<AuthorityRecord>,
}

fn rejected(call_id: &str, code: &str) -> ActionResponse {
    ActionResponse {
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

fn call_error(error: &Error) -> bool {
    !matches!(
        error.code.as_str(),
        code::HOST
            | code::HOST_INVALID
            | code::STORAGE_INVALID
            | code::INTERNAL
            | code::REQUEST_INVALID
    )
}

/// The canonical call identity. The canonical store policy joins it only
/// when it is not the default, so default requests keep their existing
/// fingerprint and explicit `true` entries do not change the identity.
/// Key validation still sees the explicit policy in `execute_fresh`.
fn canonical_intent(call: &ActionIntent, models: &BTreeMap<String, u64>) -> Result<String> {
    let mut identity = json!({"callId":call.call_id,"name":call.name,"version":call.version,"args":call.args,"models":models});
    if let Some(store) = call.store.clone().canonical().wire() {
        identity["store"] = store;
    }
    canonical_json(&identity).map_err(internal)
}

fn current_authority(
    config: &Config,
    models: &BTreeMap<String, u64>,
    mut record: AuthorityRecord,
) -> Result<AuthorityRecord> {
    if record.error.is_some() {
        return Err(storage_invalid("saved Action authority carries an error"));
    }
    let version = *models
        .get(&record.model)
        .ok_or_else(|| storage_invalid("authority model not declared"))?;
    let contract = config
        .contract(&record.model, version)
        .ok_or_else(|| storage_invalid("authority read contract not retained"))?;
    if !record.state.is_null() {
        let mut state = record
            .state
            .as_object()
            .ok_or_else(|| storage_invalid("saved authority state invalid"))?
            .clone();
        let model = contract.model(&record.model).map_err(storage_invalid)?;
        for field in &model.fields {
            if !model.identity.contains(&field.name)
                && !state.contains_key(&field.name)
                && let Some(default) = &field.default
            {
                state.insert(field.name.clone(), default.clone());
            }
        }
        record.state = contract
            .normalize_state(&record.model, &Value::Object(state))
            .map_err(storage_invalid)?;
    }
    Ok(record)
}

/// Execute one Action inside the caller's database transaction. The caller
/// owns commit, and must never commit after this function returns an error.
pub async fn execute_action(
    config: &Config,
    owner: &str,
    call: &ActionIntent,
    models: &BTreeMap<String, u64>,
    ordinal: u64,
    host: &impl Host,
) -> Result<ActionResponse> {
    principal(owner)?;
    let request = canonical_intent(call, models)?;
    let claimed: ClaimedCall = host
        .call_typed(HostRequest::ClaimCall {
            owner: owner.into(),
            call_id: call.call_id.clone(),
            request: request.clone(),
        })
        .await?;
    if claimed.request != request {
        return Ok(rejected(&call.call_id, "call.identity_conflict"));
    }
    if !claimed.fresh {
        let saved = claimed
            .response
            .ok_or_else(|| storage_invalid("committed call has no response"))?;
        let response: ActionResponse = serde_json::from_str(&saved).map_err(storage_invalid)?;
        if response.completion.call_id != call.call_id {
            return Err(storage_invalid("saved call ID mismatch"));
        }
        return Ok(response);
    }
    if claimed.response.is_some() {
        return Err(storage_invalid("fresh call already completed"));
    }
    let Acknowledged = host.call_typed(HostRequest::Savepoint { ordinal }).await?;
    let result = execute_fresh(config, owner, call, models, ordinal, host).await;
    let response = match result {
        Ok(response) => {
            let Acknowledged = host.call_typed(HostRequest::Release { ordinal }).await?;
            response
        }
        Err(error) if call_error(&error) => {
            let Acknowledged = host.call_typed(HostRequest::Rollback { ordinal }).await?;
            let Acknowledged = host.call_typed(HostRequest::Release { ordinal }).await?;
            rejected(&call.call_id, &error.code)
        }
        Err(error) => return Err(error),
    };
    let text =
        canonical_json(&serde_json::to_value(&response).map_err(internal)?).map_err(internal)?;
    let Acknowledged = host
        .call_typed(HostRequest::SaveCall {
            owner: owner.into(),
            call_id: call.call_id.clone(),
            response: text,
        })
        .await?;
    Ok(response)
}

/// Execute a single direct invocation in the caller's application transaction.
/// No durable client sequence is claimed; the host must commit before replying.
pub async fn process_action(
    config: &Config,
    owner: &str,
    bytes: &[u8],
    host: &impl Host,
) -> Result<String> {
    principal(owner)?;
    let request = DirectActionRequest::decode_envelope(bytes).map_err(request_invalid)?;
    let response = execute_action(config, owner, &request.call, &request.models, 1, host).await?;
    let response = DirectActionResponse {
        completion: response.completion,
        records: response
            .records
            .into_iter()
            .map(|record| current_authority(config, &request.models, record))
            .collect::<Result<Vec<_>>>()?,
    };
    String::from_utf8(response.encode().map_err(internal)?).map_err(internal)
}

async fn execute_fresh(
    config: &Config,
    owner: &str,
    call: &ActionIntent,
    models: &BTreeMap<String, u64>,
    ordinal: u64,
    host: &impl Host,
) -> Result<ActionResponse> {
    let action = config
        .schema
        .action(&call.name, call.version)
        .map_err(|_| Error::code("action_version_unsupported"))?;
    let args = normalize_action_args(&config.schema, action, &call.args)
        .map_err(|_| Error::code("action.invalid"))?;
    call.store
        .validate(action)
        .map_err(|_| Error::code("action.invalid"))?;
    let store = call.store.clone().canonical();
    // Input targets are mandatory caller authority: each is reconciled
    // whatever the outputs, `store` policy or Channels say.
    let mut input_targets = Changes::new();
    for input in &action.inputs {
        if let ActionInputDescriptor::Model { name, model, .. } = input {
            for identity in input_identities(&config.schema, model, &args[name], input)? {
                settlement::insert(
                    &mut input_targets,
                    config
                        .schema
                        .record_key(model, &identity)
                        .map_err(|_| Error::code("action.invalid"))?,
                )?;
            }
        }
    }
    for key in input_targets.values() {
        if !models.contains_key(&key.model)
            || config.contract(&key.model, models[&key.model]).is_none()
        {
            return Err(Error::code(code::MODEL_VERSION_UNSUPPORTED));
        }
    }
    let settled: HandledAction = host
        .call_typed(HostRequest::HandleAction {
            name: call.name.clone(),
            version: call.version,
            arguments: args.clone(),
            owner: owner.into(),
            call_id: call.call_id.clone(),
            ordinal,
        })
        .await?;
    let (outputs, extra, memberships) = match settled {
        HandledAction::Rejected { rejection } => return Err(Error::code(rejection)),
        HandledAction::Failed { .. } => return Err(Error::code(code::HANDLER_FAILED)),
        HandledAction::Settled {
            outputs,
            changes,
            memberships,
        } => (outputs, changes, memberships),
    };
    // A Query's contract has no business effects. A settlement that reports
    // any is refused before the framework stamps, reads back or publishes it,
    // whatever host produced it; the caller rolls back its savepoint.
    if action.kind == CallKind::Query && (!extra.is_empty() || !memberships.is_empty()) {
        return Err(Error::code(code::QUERY_EFFECTS_FORBIDDEN));
    }
    // Changed records are the input targets plus extra touches. An extra
    // touch is distributed but never becomes caller authority by itself.
    let mut changed = input_targets.clone();
    for record in &extra {
        settlement::insert(&mut changed, settlement::resolve(config, record)?)?;
    }
    let stamps = settlement::settle_changes(config, &changed, &memberships, host).await?;
    let mut records =
        match readback::read_back(config, models, owner, &input_targets, &stamps, host).await? {
            Outcome::Refused(code) => return Err(Error::code(code)),
            Outcome::Records(records) => records,
        };
    let (result, additional) = crate::action_results::assemble_result(
        config,
        owner,
        action,
        &args,
        &outputs,
        crate::action_results::ResultReadback {
            records: &records,
            stamps: &stamps,
            models,
            store: &store,
        },
        host,
    )
    .await?;
    let result = validate_action_result(&config.schema, action, &result)
        .map_err(|_| Error::code(code::HANDLER_INVALID))?;
    records.extend(additional);
    Ok(ActionResponse {
        completion: CallCompletion {
            call_id: call.call_id.clone(),
            outcome: ActionOutcome::Succeeded { result },
        },
        records,
    })
}

pub(crate) fn input_identities(
    schema: &axton_core::Schema,
    model: &str,
    value: &Value,
    input: &ActionInputDescriptor,
) -> Result<Vec<Value>> {
    let (operation, cardinality) = match input {
        ActionInputDescriptor::Model {
            operation,
            cardinality,
            ..
        } => (operation.as_str(), cardinality.as_str()),
        _ => return Ok(vec![]),
    };
    let entries: Vec<&Value> = match cardinality {
        "list" => value
            .as_array()
            .ok_or_else(|| Error::code("action.invalid"))?
            .iter()
            .collect(),
        "optional" if value.is_null() => vec![],
        _ => vec![value],
    };
    entries
        .into_iter()
        .map(|entry| {
            let _ = operation;
            let model_desc = schema
                .model(model)
                .map_err(|_| Error::code("action.invalid"))?;
            let identity = Value::Object(
                model_desc
                    .identity
                    .iter()
                    .filter_map(|field| {
                        entry.get(field).map(|value| (field.clone(), value.clone()))
                    })
                    .collect(),
            );
            schema
                .record_key(model, &identity)
                .map(|key| key.identity)
                .map_err(|_| Error::code("action.invalid"))
        })
        .collect()
}

/// Process a durable Action batch while the application owns the outer transaction.
pub async fn process_action_push(
    config: &Config,
    owner: &str,
    bytes: &[u8],
    host: &impl Host,
) -> Result<String> {
    principal(owner)?;
    let request = PushRequest::decode_action_envelope(bytes).map_err(request_invalid)?;
    let locked: Claimed = host
        .call_typed(HostRequest::Claim {
            owner: owner.into(),
            client_id: request.client_id.clone(),
        })
        .await?;
    if locked.client_id != request.client_id {
        return Err(storage_invalid("storage client mismatch"));
    }
    if locked.owner != owner {
        return Err(Error::code(code::OWNER_MISMATCH));
    }
    if request.batch_sequence == locked.sequence {
        return locked
            .receipt
            .ok_or_else(|| storage_invalid("receipt missing"));
    }
    if request.batch_sequence < locked.sequence {
        return Err(Error::code(code::OVERLAP));
    }
    if request.batch_sequence != locked.sequence + 1 {
        return Err(Error::code(code::GAP));
    }
    let mut rejections = vec![];
    let mut completions = vec![];
    let mut authority: BTreeMap<String, AuthorityRecord> = BTreeMap::new();
    for mutation in &request.mutations {
        let mut call: ActionIntent =
            serde_json::from_value(mutation.raw.clone()).map_err(request_invalid)?;
        call.call_id = normalize_call_id(&call.call_id).map_err(request_invalid)?;
        let response = execute_action(
            config,
            owner,
            &call,
            &request.models,
            mutation.ordinal,
            host,
        )
        .await?;
        if let ActionOutcome::Failed { code, .. } = &response.completion.outcome {
            rejections.push(Rejection {
                ordinal: mutation.ordinal,
                code: code.clone(),
            });
        }
        completions.push(response.completion);
        for record in response.records {
            let record = current_authority(config, &request.models, record)?;
            let key = config
                .schema
                .record_key(&record.model, &record.identity)
                .map_err(storage_invalid)?
                .encoded()
                .map_err(internal)?;
            match authority.get(&key) {
                Some(previous) if previous.stamp > record.stamp => {}
                Some(previous) if previous.stamp == record.stamp && previous != &record => {
                    return Err(storage_invalid("conflicting content at equal stamp"));
                }
                _ => {
                    authority.insert(key, record);
                }
            }
        }
    }
    let receipt = PushReceipt {
        client_id: request.client_id.clone(),
        batch_sequence: request.batch_sequence,
        rejections,
        completions,
        records: authority.into_values().collect(),
    };
    let text = String::from_utf8(receipt.encode().map_err(internal)?).map_err(internal)?;
    let Acknowledged = host
        .call_typed(HostRequest::SaveReceipt {
            owner: owner.into(),
            client_id: request.client_id,
            sequence: request.batch_sequence,
            receipt: text.clone(),
        })
        .await?;
    Ok(text)
}
