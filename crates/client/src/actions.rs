//! Durable Action submission. The intent is persisted independently of its
//! inferred optimistic Model operations.
use crate::engine::Engine;
use crate::query_cache::QueryCacheKey;
use crate::{ApplyReport, Client, ClientStore, Mutation, Operation, OperationKind};
use axton_core::{
    ActionInputDescriptor, ActionIntent, ActionOutcome, ActionStore, DirectActionRequest,
    DirectActionResponse, Result, invalid, normalize_action_args,
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
        // Fresh arguments only: generated values are fixed here, once.
        let mut args = args;
        crate::defaults::fill_action_args(&self.schema, action, &mut args);
        let args = normalize_action_args(&self.schema, action, &args)?;
        validate_bindings(&self.schema, action, &args)?;
        options.store.validate(action)?;
        Ok(DirectActionRequest {
            call: ActionIntent {
                call_id: uuid::Uuid::new_v4().to_string(),
                name: name.into(),
                version,
                args,
                store: options.store.canonical(),
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
    /// args and optimism in one transaction.
    pub fn submit_action_with_options(
        &mut self,
        name: &str,
        version: u64,
        args: Value,
        options: ActionCallOptions,
    ) -> Result<SubmittedCall> {
        let action = self.schema.action(name, version)?;
        // Fresh arguments only: generated values are fixed here, once.
        let mut args = args;
        crate::defaults::fill_action_args(&self.schema, action, &mut args);
        let args = normalize_action_args(&self.schema, action, &args)?;
        validate_bindings(&self.schema, action, &args)?;
        options.store.validate(action)?;
        let operations = derive_operations(&self.schema, action, &args)?;
        let call_id = uuid::Uuid::new_v4().to_string();
        let mut mutation = Mutation::new(name, operations);
        mutation.version = version;
        mutation.call_id = Some(call_id.clone());
        mutation.args = Some(args);
        mutation.store = options.store.canonical();
        let ordinal = self.transaction(|tx| tx.enqueue(mutation))?;
        Ok(SubmittedCall { call_id, ordinal })
    }
}

impl<S: ClientStore> Engine<'_, S> {
    pub(crate) fn apply_direct_response_body(
        &mut self,
        response: &DirectActionResponse,
        snapshot: Option<(&QueryCacheKey, Option<&str>)>,
    ) -> Result<ApplyReport> {
        let mut report = if response.records.is_empty() {
            ApplyReport::default()
        } else {
            self.apply_records(&response.records)?
        };
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
