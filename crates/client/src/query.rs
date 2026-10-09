//! Filters run in SQL; ordering keeps the reference comparison rules (nulls first, UTF-16 order).
use crate::engine::Engine;
use crate::store::{ClientStore, SqlRows};
use axton_core::{RecordKey, Result, ValueType, invalid};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::BTreeSet;

pub use axton_protocols::client_bridge::query::{Direction, QueryOrder, QuerySpec};
fn compare(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::String(a), Value::String(b)) => a.encode_utf16().cmp(b.encode_utf16()),
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Number(a), Value::Number(b)) => a.as_f64().unwrap().total_cmp(&b.as_f64().unwrap()),
        _ => Ordering::Equal,
    }
}
pub(crate) fn rows_to_objects(rows: SqlRows) -> Result<Vec<Value>> {
    if rows.columns.iter().collect::<BTreeSet<_>>().len() != rows.columns.len() {
        return Err(invalid(
            "SQL result column names must be unique; use aliases",
        ));
    }
    Ok(rows
        .rows
        .into_iter()
        .map(|row| Value::Object(rows.columns.iter().cloned().zip(row).collect()))
        .collect())
}
pub fn evaluate<S: ClientStore>(
    engine: &mut Engine<'_, S>,
    model: &str,
    spec: &QuerySpec,
) -> Result<Vec<Value>> {
    let schema = engine.schema;
    let model = schema.model(model)?.clone();
    let field = |name: &str| {
        model
            .fields
            .iter()
            .find(|f| f.name == name)
            .ok_or_else(|| invalid(format!("unknown query field {name}")))
    };
    let mut filter = vec![];
    for (name, value) in &spec.filter {
        let field = field(name)?;
        if matches!(field.value_type, ValueType::List { .. }) {
            return Err(invalid("list predicates unsupported"));
        }
        filter.push((name.clone(), schema.normalize_value(field, value)?));
    }
    for order in &spec.order_by {
        if !matches!(field(&order.field)?.value_type, ValueType::Scalar { .. }) {
            return Err(invalid("ordering requires scalar field"));
        }
    }
    let mut rows = engine.rows_where(&model.name, &model, &filter)?;
    rows.sort_by(|a, b| {
        for order in &spec.order_by {
            let cmp = compare(&a[&order.field], &b[&order.field]);
            let cmp = match order.direction {
                Direction::Ascending => cmp,
                Direction::Descending => cmp.reverse(),
            };
            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        for field in &model.identity {
            let cmp = compare(&a[field], &b[field]);
            if cmp != Ordering::Equal {
                return cmp;
            }
        }
        Ordering::Equal
    });
    if let Some(limit) = spec.limit {
        rows.truncate(limit);
    }
    Ok(rows)
}
pub fn related<S: ClientStore>(
    engine: &mut Engine<'_, S>,
    key: &RecordKey,
    name: &str,
) -> Result<Option<Value>> {
    let schema = engine.schema;
    let key = schema.record_key(&key.model, &key.identity)?;
    let relation = schema
        .model(&key.model)?
        .relations
        .iter()
        .find(|r| r.name == name)
        .ok_or_else(|| invalid("unknown relation"))?
        .clone();
    let Some(row) = engine.read_row(&key)? else {
        return Ok(None);
    };
    let mut identity = serde_json::Map::new();
    for (local, target) in relation.fields.iter().zip(&relation.target_fields) {
        if row[local].is_null() {
            return Ok(None);
        }
        identity.insert(target.clone(), row[local].clone());
    }
    engine.read_row(&schema.record_key(&relation.target, &Value::Object(identity))?)
}
pub fn referencing<S: ClientStore>(
    engine: &mut Engine<'_, S>,
    key: &RecordKey,
    source: &str,
    name: &str,
) -> Result<Vec<Value>> {
    let schema = engine.schema;
    let key = schema.record_key(&key.model, &key.identity)?;
    let relation = schema
        .model(source)?
        .relations
        .iter()
        .find(|r| r.name == name && r.target == key.model)
        .ok_or_else(|| invalid("unknown inverse relation"))?
        .clone();
    let filter = relation
        .fields
        .iter()
        .zip(&relation.target_fields)
        .map(|(local, target)| (local.clone(), key.identity[target].clone()))
        .collect();
    evaluate(
        engine,
        source,
        &QuerySpec {
            filter,
            ..Default::default()
        },
    )
}
