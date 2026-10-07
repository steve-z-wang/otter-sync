//! Named Mutation operation derivation; lifecycle belongs to the canonical queue.
use crate::{Operation, OperationKind};
use axton_core::{ActionInputDescriptor, Result, invalid};
use serde_json::Value;
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubmittedCall {
    pub call_id: String,
    pub ordinal: u64,
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
