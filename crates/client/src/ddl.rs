//! The tables are the schema record. Reconciliation makes them match the compiled schema or fails.
use crate::store::ClientStore;
use axton_core::{
    FieldDescriptor, ModelDescriptor, Result, ScalarType, Schema, ValueType, invalid,
};
use serde_json::Value;
use std::collections::BTreeMap;

pub fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub fn before_table(model: &str) -> String {
    format!("axton_before_{model}")
}

pub fn storage_type(value_type: &ValueType) -> &'static str {
    match value_type {
        ValueType::Scalar {
            name: ScalarType::Boolean | ScalarType::Int,
        } => "INTEGER",
        ValueType::Scalar {
            name: ScalarType::Float,
        } => "REAL",
        _ => "TEXT",
    }
}

fn literal(field: &FieldDescriptor) -> Result<String> {
    let value = field.default.as_ref().ok_or_else(|| {
        invalid(format!(
            "column {} is not nullable and has no default",
            field.name
        ))
    })?;
    Ok(match value {
        Value::Bool(b) => i64::from(*b).to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Null => {
            return Err(invalid(format!(
                "column {} default cannot be null",
                field.name
            )));
        }
        other => format!("'{}'", serde_json::to_string(other)?.replace('\'', "''")),
    })
}

fn column(field: &FieldDescriptor) -> String {
    let null = if field.nullable { "" } else { " NOT NULL" };
    format!(
        "{} {}{null}",
        quote(&field.name),
        storage_type(&field.value_type)
    )
}

fn table_ddl(table: &str, model: &ModelDescriptor) -> String {
    let columns = model
        .fields
        .iter()
        .map(column)
        .collect::<Vec<_>>()
        .join(", ");
    let key = model
        .identity
        .iter()
        .map(|f| quote(f))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "CREATE TABLE IF NOT EXISTS {} ({columns}, PRIMARY KEY ({key}))",
        quote(table)
    )
}

pub fn model_ddl(model: &ModelDescriptor) -> Vec<String> {
    let mut statements = vec![
        table_ddl(&model.name, model),
        table_ddl(&before_table(&model.name), model),
    ];
    for fields in &model.unique {
        let name = format!("{}_{}_unique", model.name, fields.join("_"));
        let columns = fields
            .iter()
            .map(|f| quote(f))
            .collect::<Vec<_>>()
            .join(", ");
        statements.push(format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS {} ON {} ({columns})",
            quote(&name),
            quote(&model.name)
        ));
    }
    statements
}

struct Existing {
    columns: BTreeMap<String, String>, // name -> declared type
    identity: Vec<String>,             // pk columns in key order
}

fn existing<S: ClientStore>(store: &mut S, table: &str) -> Result<Option<Existing>> {
    let rows = store.query(&format!("PRAGMA table_info({})", quote(table)), &[])?;
    if rows.rows.is_empty() {
        return Ok(None);
    }
    let mut columns = BTreeMap::new();
    let mut keyed = vec![];
    for row in rows.rows {
        let name = row[1]
            .as_str()
            .ok_or_else(|| invalid("table_info name"))?
            .to_string();
        let ty = row[2].as_str().unwrap_or("").to_ascii_uppercase();
        let pk = row[5].as_i64().unwrap_or(0);
        if pk > 0 {
            keyed.push((pk, name.clone()));
        }
        columns.insert(name, ty);
    }
    keyed.sort();
    Ok(Some(Existing {
        columns,
        identity: keyed.into_iter().map(|(_, n)| n).collect(),
    }))
}

/// Why the tables in `store` cannot be reconciled with `schema`, if they
/// cannot: the same refusals [`reconcile`] makes, found by reading only. A
/// storage failure is an error, never a reason, so a caller can tell an
/// incompatible layout from a database that merely failed to answer.
pub fn incompatibility<S: ClientStore>(store: &mut S, schema: &Schema) -> Result<Option<String>> {
    for model in &schema.models {
        let Some(current) = existing(store, &model.name)? else {
            continue;
        };
        if current.identity != model.identity {
            return Ok(Some(format!("identity columns of {} changed", model.name)));
        }
        for field in &model.fields {
            match current.columns.get(&field.name) {
                Some(ty) if ty == storage_type(&field.value_type) => {}
                Some(ty) => {
                    return Ok(Some(format!(
                        "column {}.{} is {ty} in the database but {} in the schema",
                        model.name,
                        field.name,
                        storage_type(&field.value_type)
                    )));
                }
                None if !field.nullable && field.default.is_none() => {
                    return Ok(Some(format!(
                        "column {}.{} is not nullable and has no default",
                        model.name, field.name
                    )));
                }
                None => {}
            }
        }
    }
    Ok(None)
}

pub fn reconcile<S: ClientStore>(store: &mut S, schema: &Schema) -> Result<()> {
    for model in &schema.models {
        let Some(current) = existing(store, &model.name)? else {
            for statement in model_ddl(model) {
                store.execute(&statement, &[])?;
            }
            continue;
        };
        if current.identity != model.identity {
            return Err(invalid(format!(
                "identity columns of {} changed; cannot open",
                model.name
            )));
        }
        for field in &model.fields {
            match current.columns.get(&field.name) {
                Some(ty) if ty == storage_type(&field.value_type) => {}
                Some(ty) => {
                    return Err(invalid(format!(
                        "column {}.{} is {ty} in the database but {} in the schema",
                        model.name,
                        field.name,
                        storage_type(&field.value_type)
                    )));
                }
                None => {
                    let mut definition = column(field);
                    if !field.nullable {
                        definition.push_str(&format!(" DEFAULT {}", literal(field)?));
                    }
                    for table in [model.name.clone(), before_table(&model.name)] {
                        store.execute(
                            &format!("ALTER TABLE {} ADD COLUMN {definition}", quote(&table)),
                            &[],
                        )?;
                    }
                }
            }
        }
        for statement in model_ddl(model).into_iter().skip(2) {
            store.execute(&statement, &[])?;
        }
    }
    Ok(())
}
