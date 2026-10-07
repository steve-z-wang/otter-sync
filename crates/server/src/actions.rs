//! Retained Model slot identity normalization.
use crate::{Error, Result};
use axton_core::ActionInputDescriptor;
use serde_json::Value;
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
