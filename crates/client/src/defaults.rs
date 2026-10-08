//! Create-only Model defaults ([#27](https://github.com/zanminwang/axton/issues/27)).
//!
//! Source `@default` fills a field only when a fresh create omits it. This is
//! the one place that evaluates `uuid()` and `now()`: the client prepares a
//! fresh local create, low-level enqueue or Mutation call here, before strict
//! normalization, persistence or dispatch. Everything downstream (optimism,
//! persisted intent, frozen bytes, the direct request, retries, reopen,
//! replay and settlement) consumes the concrete values produced once here.
//! Updates, deletes, reads, Loader records, incoming authority and migration
//! never come through this module.
use crate::{Operation, OperationKind};
use axton_core::{ActionDescriptor, ActionInputDescriptor, CreateDefault, ModelDescriptor, Schema};
use chrono::SecondsFormat;
use serde_json::{Map, Value};

/// A concrete value for one omitted field.
fn materialize(default: &CreateDefault) -> Value {
    match default {
        CreateDefault::Literal { value } => value.clone(),
        CreateDefault::Uuid => Value::String(uuid::Uuid::new_v4().hyphenated().to_string()),
        CreateDefault::Now => {
            Value::String(chrono::Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true))
        }
    }
}

/// Fill every defaulted field of `policy` that `admits` and that no part of
/// the create already names; `insert` places each filled value.
fn fill(
    policy: &ModelDescriptor,
    admits: impl Fn(&str) -> bool,
    present: impl Fn(&str) -> bool,
    mut insert: impl FnMut(&str, Value),
) {
    for field in &policy.fields {
        let Some(default) = &field.create_default else {
            continue;
        };
        if admits(&field.name) && !present(&field.name) {
            insert(&field.name, materialize(default));
        }
    }
}

/// A fresh create operation split into identity and values: an omitted
/// identity field is filled into the identity, an omitted state field into
/// the values. A field named on either side counts as supplied, so a
/// misplaced identity key is left for normalization to refuse rather than
/// replaced by a generated one. Anything that is not an object is left for
/// normalization to refuse as well.
pub(crate) fn fill_operation(schema: &Schema, op: &mut Operation) {
    if op.op != OperationKind::Create {
        return;
    }
    let Ok(policy) = schema.model(&op.model) else {
        return;
    };
    let (Some(identity), Some(Some(values))) = (
        op.identity.as_object(),
        op.values.as_ref().map(Value::as_object),
    ) else {
        return;
    };
    let mut additions: Vec<(String, Value)> = vec![];
    fill(
        policy,
        |_| true,
        |name| identity.contains_key(name) || values.contains_key(name),
        |name, value| additions.push((name.to_string(), value)),
    );
    for (name, value) in additions {
        let side = if policy.identity.contains(&name) {
            &mut op.identity
        } else {
            op.values.as_mut().unwrap()
        };
        side.as_object_mut().unwrap().insert(name, value);
    }
}

/// Fill the `Model.create` operands of fresh Mutation arguments. The current
/// Model descriptor supplies the policy; the operation's retained input
/// contract limits it to fields that contract declares with the same type,
/// so a default is never injected into a version that predates or reshaped
/// its field. Absent optional
/// operands stay absent and every list item gets its own values.
pub(crate) fn fill_action_args(schema: &Schema, action: &ActionDescriptor, args: &mut Value) {
    let Some(args) = args.as_object_mut() else {
        return;
    };
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
        if operation != "create" {
            continue;
        }
        let Ok(policy) = schema.model(model) else {
            continue;
        };
        let contract = action
            .input
            .as_ref()
            .and_then(|input| input.models.iter().find(|m| m.name == *model))
            .unwrap_or(policy);
        let Some(value) = args.get_mut(name) else {
            continue;
        };
        let items: Vec<&mut Value> = match (cardinality.as_str(), value) {
            ("list", Value::Array(items)) => items.iter_mut().collect(),
            (_, value) => vec![value],
        };
        for item in items {
            if let Value::Object(record) = item {
                fill_record(policy, contract, record);
            }
        }
    }
}

fn fill_record(
    policy: &ModelDescriptor,
    contract: &ModelDescriptor,
    record: &mut Map<String, Value>,
) {
    let mut additions: Vec<(String, Value)> = vec![];
    fill(
        policy,
        |name| {
            // The retained contract must declare the field with the same
            // type; a same-named field of another shape is not this policy's.
            let current = policy.fields.iter().find(|f| f.name == name);
            contract.fields.iter().any(|f| {
                f.name == name
                    && current.is_some_and(|c| {
                        serde_json::to_value(&c.value_type).ok()
                            == serde_json::to_value(&f.value_type).ok()
                    })
            })
        },
        |name| record.contains_key(name),
        |name, value| additions.push((name.to_string(), value)),
    );
    record.extend(additions);
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn retained_input_contract_only_receives_defaults_for_its_own_field_types() {
        let schema = Schema::from_value(json!({"enums":[],"models":[{"name":"Todo","identity":["id"],"fields":[
            {"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"},"createDefault":{"kind":"uuid"}},
            {"name":"title","nullable":false,"type":{"kind":"scalar","name":"string"},"createDefault":{"kind":"literal","value":""}},
            {"name":"priority","nullable":false,"type":{"kind":"scalar","name":"int"},"createDefault":{"kind":"literal","value":0}},
            {"name":"note","nullable":true,"type":{"kind":"scalar","name":"string"},"createDefault":{"kind":"literal","value":"new"}}
        ]}],"actions":[{"name":"AddOld","version":1,"inputs":[{"kind":"model","name":"todo","model":"Todo","operation":"create","cardinality":"single"}],"outputs":[],"input":{"enums":[],"models":[{"name":"Todo","version":1,"identity":["id"],"fields":[
            {"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"}},
            {"name":"title","nullable":false,"type":{"kind":"scalar","name":"string"}},
            {"name":"priority","nullable":true,"type":{"kind":"scalar","name":"string"}}
        ]}]}}]})).unwrap();
        let action = schema.action("AddOld", 1).unwrap();
        let mut args = json!({"todo":{}});
        fill_action_args(&schema, action, &mut args);
        let args = axton_core::normalize_action_args(&schema, action, &args).unwrap();
        let todo = &args["todo"];
        assert_eq!(
            uuid::Uuid::parse_str(todo["id"].as_str().unwrap())
                .unwrap()
                .get_version_num(),
            4
        );
        assert_eq!(todo["title"], "");
        assert_eq!(todo["priority"], Value::Null);
        assert!(todo.get("note").is_none());
    }
}
