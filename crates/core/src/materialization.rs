//! Canonical read contracts shared by versioned hash wrappers.
use crate::{Result, Schema, invalid};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
pub fn contract(
    schema: &Schema,
    models: &BTreeMap<String, u64>,
    projection_generation: &str,
) -> Result<Value> {
    if projection_generation.trim().is_empty() {
        return Err(invalid("blank identifier"));
    }
    if models.len() != schema.models.len() {
        return Err(invalid("materialization needs every Model"));
    }
    fn enums(ty: &crate::ValueType, names: &mut BTreeSet<String>) {
        match ty {
            crate::ValueType::Enum { name } => {
                names.insert(name.clone());
            }
            crate::ValueType::List { element } => enums(element, names),
            _ => {}
        }
    }
    let mut contracts = Vec::new();
    for (name, version) in models {
        crate::counter(*version)?;
        if *version == 0 {
            return Err(invalid("positive counter required"));
        }
        let current = schema.model(name)?;
        let (fields, identity, available) = if current.version == *version {
            (&current.fields, &current.identity, &schema.enums)
        } else {
            let old = schema
                .result_models
                .iter()
                .find(|m| m.name == *name && m.version == *version)
                .ok_or_else(|| invalid("retained materialization contract missing"))?;
            (&old.fields, &old.identity, &old.enums)
        };
        let mut reachable = BTreeSet::new();
        let mut normalized = BTreeMap::new();
        for field in fields {
            enums(&field.value_type, &mut reachable);
            if normalized.insert(field.name.clone(),serde_json::json!({"name":field.name,"type":field.value_type,"nullable":field.nullable})).is_some(){return Err(invalid("duplicate materialization field"));}
        }
        let mut enum_contracts = BTreeMap::new();
        for name in reachable {
            let descriptor = available
                .iter()
                .find(|e| e.name == name)
                .ok_or_else(|| invalid("materialization enum missing"))?;
            let mut values = descriptor.values.clone();
            values.sort();
            enum_contracts.insert(
                name.clone(),
                serde_json::json!({"name":name,"values":values}),
            );
        }
        let mut identity = identity.clone();
        identity.sort();
        contracts.push(serde_json::json!({"name":name,"version":version,"bootstrap":current.bootstrap,"identity":identity,"fields":normalized.into_values().collect::<Vec<_>>(),"enums":enum_contracts.into_values().collect::<Vec<_>>()}));
    }
    Ok(serde_json::json!({"projectionGeneration":projection_generation,"models":contracts}))
}
