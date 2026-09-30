//! Durable Action submission. The intent is persisted independently of its
//! inferred optimistic Model operations.
use crate::engine::Engine;
use crate::query_cache::QueryCacheKey;
use crate::{ApplyReport, Client, ClientStore, Mutation, Operation, OperationKind};
use axton_core::{
    ActionDescriptor, ActionInputDescriptor, ActionIntent, ActionOutcome, ActionStore,
    DirectActionRequest, DirectActionResponse, Result, Schema, invalid, normalize_action_args,
};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubmittedCall {
    pub call_id: String,
    pub ordinal: u64,
}

/// Invocation options kept apart from business args and never passed to
/// the Handler.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ActionCallOptions {
    /// Which explicit Model outputs contribute additional local authority.
    pub store: ActionStore,
}

impl<S: ClientStore> Client<S> {
    /// Prepare a direct invocation without touching local persistence.
    pub fn prepare_action(
        &self,
        name: &str,
        version: u64,
        args: Value,
    ) -> Result<DirectActionRequest> {
        self.prepare_action_with_options(name, version, args, ActionCallOptions::default())
    }
    /// [`Self::prepare_action`] with invocation options, validated before
    /// the request can be dispatched.
    pub fn prepare_action_with_options(
        &self,
        name: &str,
        version: u64,
        args: Value,
        options: ActionCallOptions,
    ) -> Result<DirectActionRequest> {
        let action = self.schema.action(name, version)?;
        let (args, store) = fresh_args(&self.schema, action, args, options)?;
        Ok(DirectActionRequest {
            call: ActionIntent {
                call_id: uuid::Uuid::new_v4().to_string(),
                name: name.into(),
                version,
                args,
                store,
            },
            models: self.declared_models(),
        })
    }

    /// Apply authoritative direct results in one short local transaction.
    /// The transient completion is exposed only after that transaction commits.
    pub fn apply_action_response(
        &mut self,
        request: &DirectActionRequest,
        bytes: &[u8],
    ) -> Result<ApplyReport> {
        let response = DirectActionResponse::decode(bytes, request, &self.schema)?;
        self.apply_direct_response(response, None)
    }
    /// Apply a validated direct response: its authority under the stamp
    /// rules and, for a Query once call that succeeded, its result snapshot
    /// fenced by the generation the request saw, all in one local
    /// transaction. A response with nothing to write opens none.
    pub(crate) fn apply_direct_response(
        &mut self,
        response: DirectActionResponse,
        snapshot: Option<(&QueryCacheKey, Option<&str>)>,
    ) -> Result<ApplyReport> {
        let result = match (&response.completion.outcome, snapshot) {
            (ActionOutcome::Succeeded { result }, Some(snapshot)) => Some((result, snapshot)),
            _ => None,
        };
        if response.records.is_empty() && result.is_none() {
            let mut report = ApplyReport::default();
            report.completions.push(response.completion.clone());
            return Ok(report);
        }
        self.write(|engine| engine.apply_direct_response_body(&response, snapshot))
    }
    pub fn apply_action_response_bytes(
        &mut self,
        request: &[u8],
        response: &[u8],
    ) -> Result<ApplyReport> {
        let request = DirectActionRequest::decode(request, &self.schema)?;
        self.apply_action_response(&request, response)
    }
    /// Decode a received direct response while leaving its runtime call
    /// owner in place until the authority transaction commits.
    pub(crate) fn decode_direct_store(
        &self,
        request: &[u8],
        response: &[u8],
    ) -> Result<crate::StoreDelivery> {
        let request = DirectActionRequest::decode(request, &self.schema)?;
        let response = DirectActionResponse::decode(response, &request, &self.schema)?;
        Ok(crate::StoreDelivery::Direct {
            response,
            snapshot: None,
        })
    }
    pub fn submit_action(
        &mut self,
        name: &str,
        version: u64,
        args: Value,
    ) -> Result<SubmittedCall> {
        self.submit_action_with_options(name, version, args, ActionCallOptions::default())
    }
    /// [`Self::submit_action`] with invocation options. The store policy is
    /// validated before any local write and persisted with the call ID,
    /// args and optimism in one transaction. This standalone entry also
    /// queues a Query; a transaction submits Mutations only
    /// ([`crate::ClientTransaction::submit_mutation`]).
    pub fn submit_action_with_options(
        &mut self,
        name: &str,
        version: u64,
        args: Value,
        options: ActionCallOptions,
    ) -> Result<SubmittedCall> {
        let action = self.schema.action(name, version)?;
        let mutation = fresh_call(&self.schema, action, args, options)?;
        let call_id = mutation.call_id.clone().unwrap_or_default();
        let ordinal = self.transaction(|tx| tx.enqueue(mutation))?;
        Ok(SubmittedCall { call_id, ordinal })
    }
}

/// Fresh business arguments made canonical: generated values are fixed here,
/// once, then the args are normalized and their bindings and the store
/// policy validated, all before any local write. Returns the args with the
/// canonical store policy.
fn fresh_args(
    schema: &Schema,
    action: &ActionDescriptor,
    mut args: Value,
    options: ActionCallOptions,
) -> Result<(Value, ActionStore)> {
    crate::defaults::fill_action_args(schema, action, &mut args);
    let args = normalize_action_args(schema, action, &args)?;
    validate_bindings(schema, action, &args)?;
    options.store.validate(action)?;
    Ok((args, options.store.canonical()))
}

/// A fresh queued call for `action`: canonical args ([`fresh_args`]), the
/// wire operations derived from them and a new call ID. Its queue row is
/// written in the caller's transaction, which also validates it again as a
/// canonical intent.
pub(crate) fn fresh_call(
    schema: &Schema,
    action: &ActionDescriptor,
    args: Value,
    options: ActionCallOptions,
) -> Result<Mutation> {
    let (args, store) = fresh_args(schema, action, args, options)?;
    let mut mutation = Mutation::new(&action.name, derive_operations(schema, action, &args)?);
    mutation.version = action.version;
    mutation.call_id = Some(uuid::Uuid::new_v4().to_string());
    mutation.args = Some(args);
    mutation.store = store;
    Ok(mutation)
}

impl<S: ClientStore> Engine<'_, S> {
    pub(crate) fn apply_direct_response_body(
        &mut self,
        response: &DirectActionResponse,
        snapshot: Option<(&QueryCacheKey, Option<&str>)>,
    ) -> Result<ApplyReport> {
        let mut report = self.apply_enrolled_records(&response.records, &response.memberships)?;
        if let (ActionOutcome::Succeeded { result }, Some((key, generation))) =
            (&response.completion.outcome, snapshot)
        {
            self.save_query_result(key, generation, result)?;
        }
        report.completions.push(response.completion.clone());
        Ok(report)
    }
}

pub(crate) fn validate_bindings(
    schema: &axton_core::Schema,
    action: &axton_core::ActionDescriptor,
    args: &Value,
) -> Result<()> {
    axton_core::validate_action_bindings(schema, action, args)
}

pub(crate) fn derive_operations(
    schema: &axton_core::Schema,
    action: &axton_core::ActionDescriptor,
    args: &Value,
) -> Result<Vec<Operation>> {
    let mut operations = Vec::new();
    for input in &action.inputs {
        let ActionInputDescriptor::Model {
            name,
            model,
            operation,
            cardinality,
            ..
        } = input
        else {
            continue;
        };
        let value = &args[name];
        let values: Vec<&Value> = match cardinality.as_str() {
            "optional" if value.is_null() => vec![],
            "list" => value
                .as_array()
                .ok_or_else(|| invalid("invalid Action list"))?
                .iter()
                .collect(),
            "single" | "optional" => vec![value],
            _ => return Err(invalid("invalid Action cardinality")),
        };
        for value in values {
            let (kind, identity, values) = match operation.as_str() {
                "create" => {
                    let mut state = value
                        .as_object()
                        .ok_or_else(|| invalid("invalid create input"))?
                        .clone();
                    let model_descriptor = action
                        .input
                        .as_ref()
                        .and_then(|s| s.models.iter().find(|m| m.name == *model))
                        .or_else(|| schema.models.iter().find(|m| m.name == *model));
                    let identity_fields = model_descriptor
                        .ok_or_else(|| invalid("Action input Model missing"))?
                        .identity
                        .clone();
                    let mut identity = serde_json::Map::new();
                    for field in identity_fields {
                        identity.insert(
                            field.clone(),
                            state
                                .remove(&field)
                                .ok_or_else(|| invalid("create identity missing"))?,
                        );
                    }
                    (
                        OperationKind::Create,
                        Value::Object(identity),
                        Some(Value::Object(state)),
                    )
                }
                "update" | "delete" => {
                    let model_descriptor = action
                        .input
                        .as_ref()
                        .and_then(|snapshot| {
                            snapshot.models.iter().find(|entry| entry.name == *model)
                        })
                        .or_else(|| schema.models.iter().find(|entry| entry.name == *model))
                        .ok_or_else(|| invalid("Action input Model missing"))?;
                    let mut fields = value
                        .as_object()
                        .ok_or_else(|| invalid("invalid flat Model input"))?
                        .clone();
                    let mut identity = serde_json::Map::new();
                    for field in &model_descriptor.identity {
                        identity.insert(
                            field.clone(),
                            fields
                                .remove(field)
                                .ok_or_else(|| invalid("Action identity missing"))?,
                        );
                    }
                    if operation == "update" {
                        (
                            OperationKind::Update,
                            Value::Object(identity),
                            Some(Value::Object(fields)),
                        )
                    } else {
                        (OperationKind::Delete, Value::Object(identity), None)
                    }
                }
                _ => return Err(invalid("invalid Action operation")),
            };
            operations.push(Operation {
                model: model.clone(),
                op: kind,
                identity,
                values,
            });
        }
    }
    Ok(operations)
}
