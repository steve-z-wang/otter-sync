//! Resolve schema-declared dependencies from data, without replaying application callbacks.
use crate::engine::Engine;
use crate::store::ClientStore;
use crate::{Mutation, OperationKind};
use axton_core::{RecordKey, Result, Schema, canonical_json, invalid};
use serde_json::{Map, Value, json};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) fn derive<S: ClientStore>(
    engine: &mut Engine<'_, S>,
    mutation: &mut Mutation,
) -> Result<()> {
    let queue = engine.queued()?;
    let schema = engine.schema;
    let requirements: Vec<axton_core::RequirementDescriptor> = if mutation.call_id.is_some() {
        let action = schema.action(&mutation.name, mutation.version)?;
        match action.policy.get("requirements") {
            Some(value) => serde_json::from_value(value.clone())?,
            None => schema.requirements.clone(),
        }
    } else {
        schema.requirements.clone()
    };
    let mut lifecycle: BTreeSet<_> = mutation.lifecycle_dependencies.iter().copied().collect();
    for op in &mutation.operations {
        let key = schema.record_key(&op.model, &op.identity)?;
        let mut references = vec![key.clone()];
        for relation in &schema.model(&key.model)?.relations {
            if let Some(target) = reference(engine, &key, &relation.name, &Map::new())? {
                references.push(target);
            }
        }
        for prior in &queue {
            for previous in &prior.mutation.operations {
                let previous_key = schema.record_key(&previous.model, &previous.identity)?;
                if (previous.op == OperationKind::Create && references.contains(&previous_key))
                    || (previous.op == OperationKind::Delete
                        && op.op == OperationKind::Create
                        && previous_key == key)
                {
                    lifecycle.insert(prior.ordinal);
                }
            }
        }
        for requirement in requirements.iter().filter(|r| r.model == op.model) {
            let Some(value) = op
                .values
                .as_ref()
                .and_then(|v| v.get(&requirement.field))
                .filter(|v| !v.is_null())
            else {
                continue;
            };
            let arguments: serde_json::Map<_, _> = requirement
                .arguments
                .keys()
                .map(|k| (k.clone(), value.clone()))
                .collect();
            let invocation = json!({"name":requirement.name,"arguments":arguments});
            mutation.prerequisites.push(canonical_json(&invocation)?);
        }
    }
    mutation.lifecycle_dependencies = lifecycle.into_iter().collect();
    mutation.prerequisites.sort();
    mutation.prerequisites.dedup();
    let mut sequences: BTreeSet<_> = mutation.sequence_dependencies.iter().copied().collect();
    if let Some(policy) = policy_fn(schema, mutation) {
        let current = slots(schema, mutation, &policy)?;
        if let Some(after) = policy["sequence"]["after"].as_array() {
            for reference_spec in after {
                let name = reference_spec["name"]
                    .as_str()
                    .ok_or_else(|| invalid("invalid sequence descriptor"))?;
                let arguments = reference_spec["arguments"]
                    .as_object()
                    .ok_or_else(|| invalid("invalid sequence arguments"))?;
                for prior in queue.iter().filter(|q| q.mutation.name == name) {
                    let Some(prior_policy) = policy_fn(schema, &prior.mutation) else {
                        continue;
                    };
                    let targets = slots(schema, &prior.mutation, &prior_policy)?;
                    let mut matches = true;
                    for (target, path) in arguments {
                        let path = path
                            .as_str()
                            .ok_or_else(|| invalid("invalid sequence path"))?;
                        // Both sides are `slot.relations…`; a list slot
                        // contributes every element, and the two must meet.
                        let sources = resolve(engine, mutation, &current, path)?;
                        let reached = resolve(engine, &prior.mutation, &targets, target)?;
                        if !sources.iter().any(|key| reached.contains(key)) {
                            matches = false;
                            break;
                        }
                    }
                    if matches {
                        sequences.insert(prior.ordinal);
                    }
                }
            }
        }
    }
    mutation.sequence_dependencies = sequences.into_iter().collect();
    Ok(())
}
fn policy_fn(schema: &Schema, mutation: &Mutation) -> Option<Value> {
    if mutation.call_id.is_some() {
        let action = schema.action(&mutation.name, mutation.version).ok()?;
        let slots: Vec<Value> = action.inputs.iter().filter_map(|input| match input {
            axton_core::ActionInputDescriptor::Model { name, model, operation, cardinality, .. } => Some(json!({"name":name,"model":model,"operation":operation,"cardinality":cardinality})),
            _ => None,
        }).collect();
        return Some(
            json!({"slots":slots,"sequence":action.policy.get("sequence").cloned().unwrap_or(Value::Null)}),
        );
    }
    schema
        .client_policies
        .iter()
        .find(|p| p["name"] == mutation.name && p["version"].as_u64() == Some(mutation.version))
        .cloned()
}
fn slots(
    schema: &Schema,
    mutation: &Mutation,
    policy: &Value,
) -> Result<BTreeMap<String, Vec<RecordKey>>> {
    let mut result = BTreeMap::new();
    if let Some(args) = &mutation.args {
        let action = schema.action(&mutation.name, mutation.version)?;
        for input in &action.inputs {
            let axton_core::ActionInputDescriptor::Model {
                name,
                model,
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
                _ => vec![value],
            };
            let mut keys = Vec::new();
            for value in values {
                let mut fields = Map::new();
                for field in &schema.model(model)?.identity {
                    fields.insert(
                        field.clone(),
                        value
                            .get(field)
                            .ok_or_else(|| invalid("Action input identity missing"))?
                            .clone(),
                    );
                }
                keys.push(schema.record_key(model, &Value::Object(fields))?);
            }
            result.insert(name.clone(), keys);
        }
        return Ok(result);
    }
    let mut at = 0;
    for slot in policy["slots"]
        .as_array()
        .ok_or_else(|| invalid("client policy slots missing"))?
    {
        let name = slot["name"]
            .as_str()
            .ok_or_else(|| invalid("slot name missing"))?;
        let mut keys = vec![];
        while let Some(op) = mutation.operations.get(at) {
            if slot["model"] != op.model || slot["operation"] != serde_json::to_value(op.op)? {
                break;
            }
            keys.push(schema.record_key(&op.model, &op.identity)?);
            at += 1;
            if slot["cardinality"] != "list" {
                break;
            }
        }
        result.insert(name.into(), keys);
    }
    Ok(result)
}
/// The records at `path`, a slot of `mutation` followed by relation names:
/// one per record the slot holds whose relations resolve.
fn resolve<S: ClientStore>(
    engine: &mut Engine<'_, S>,
    mutation: &Mutation,
    slots: &BTreeMap<String, Vec<RecordKey>>,
    path: &str,
) -> Result<Vec<RecordKey>> {
    let mut parts = path.split('.');
    let first = parts.next().ok_or_else(|| invalid("empty path"))?;
    let relations: Vec<&str> = parts.collect();
    let mut reached = vec![];
    for key in slots.get(first).into_iter().flatten() {
        if let Some(key) = follow(engine, mutation, key.clone(), &relations)? {
            reached.push(key);
        }
    }
    Ok(reached)
}
/// Follow `relations` from `key`, a record `owner` targets. The first step
/// reads what the act itself carries, its identity and a create's or update's
/// values, before the stored record; so a delete resolves from its identity
/// even when neither a local row nor a held base remains. Later steps read
/// stored records.
fn follow<S: ClientStore>(
    engine: &mut Engine<'_, S>,
    owner: &Mutation,
    mut key: RecordKey,
    relations: &[&str],
) -> Result<Option<RecordKey>> {
    for (step, name) in relations.iter().enumerate() {
        let mut carried = Map::new();
        if step == 0 {
            if let Some(identity) = key.identity.as_object() {
                carried.extend(identity.clone());
            }
            for op in &owner.operations {
                if engine.schema.record_key(&op.model, &op.identity)? == key
                    && let Some(values) = op.values.as_ref().and_then(Value::as_object)
                {
                    carried.extend(values.clone());
                }
            }
        }
        let Some(next) = reference(engine, &key, name, &carried)? else {
            return Ok(None);
        };
        key = next;
    }
    Ok(Some(key))
}
/// The target of relation `name` of `key`: its fields from `carried`, else from
/// the local row, else from the held authoritative base.
fn reference<S: ClientStore>(
    engine: &mut Engine<'_, S>,
    key: &RecordKey,
    name: &str,
    carried: &Map<String, Value>,
) -> Result<Option<RecordKey>> {
    let relation = engine
        .schema
        .model(&key.model)?
        .relations
        .iter()
        .find(|r| r.name == name)
        .ok_or_else(|| invalid("unknown relation in dependency"))?
        .clone();
    let mut row = Map::new();
    if !relation.fields.iter().all(|f| carried.contains_key(f)) {
        let stored = match engine.read_row(key)? {
            Some(row) => Some(row),
            None => engine.truth(key)?,
        };
        if let Some(Value::Object(stored)) = stored {
            row = stored;
        }
    }
    row.extend(carried.clone());
    let mut identity = Map::new();
    for (local, target) in relation.fields.iter().zip(&relation.target_fields) {
        let Some(value) = row.get(local).filter(|v| !v.is_null()) else {
            return Ok(None);
        };
        identity.insert(target.clone(), value.clone());
    }
    Ok(Some(
        engine
            .schema
            .record_key(&relation.target, &Value::Object(identity))?,
    ))
}
