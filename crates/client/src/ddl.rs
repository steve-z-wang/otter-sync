//! The tables are the schema record. Reconciliation makes them match the compiled schema or fails.
use crate::store::ClientStore;
use axton_core::{
    FieldDescriptor, ModelDescriptor, Result, ScalarType, Schema, ValueType, invalid,
};
use serde_json::Value;
use std::collections::BTreeMap;

pub const FRAMEWORK_TABLES: &[&str] = &[
    "axton_schema",
    "axton_client",
    "axton_record",
    "axton_scope_member",
    "axton_local_replica_layer",
    "axton_subscription",
    "axton_mutation",
    "axton_mutation_operation",
    "axton_mutation_dependency",
    "axton_mutation_prerequisite",
    "axton_local_write",
    "axton_rejection",
    "axton_query_cache",
    "axton_load",
    "axton_load_once",
];

/// Framework tables an earlier layout kept and this one cannot open in place:
/// scope claims owned records and push checkpoints settled batches, both
/// replaced by receipt completion ([#55](https://github.com/zanminwang/axton/issues/55)).
pub const LEGACY_TABLES: &[&str] = &["axton_claim", "axton_push_checkpoint"];

/// `axton_client` columns this layout requires beyond the original ones. A
/// database created before they existed holds pending work under the old
/// contract; it is rebuilt beside, never converted or wiped.
const CLIENT_COLUMNS: &[&str] = &["last_completed_push", "push_models", "next_subscription"];
/// Framework columns added after a layout shipped, with their definitions.
/// A database without one gets it in place: its queue stays sendable, and its
/// subscriptions keep their identities and delivery boundaries.
/// `diverged` marks a queued mutation whose replay failed over new authority
/// ([#122](https://github.com/zanminwang/axton/issues/122)); the `bootstrap_`
/// columns carry a Scope's historical load beside its subscription
/// ([#151](https://github.com/zanminwang/axton/issues/151)), whose defaults are
/// a load that was never requested.
const ADDED_COLUMNS: &[(&str, &str, &str)] = &[
    (
        "axton_client",
        "scope_membership_version",
        "INTEGER NOT NULL DEFAULT 0",
    ),
    (
        "axton_subscription",
        "reconcile_state",
        "TEXT NOT NULL DEFAULT 'not_requested'",
    ),
    (
        "axton_subscription",
        "reconcile_run",
        "INTEGER NOT NULL DEFAULT 0",
    ),
    (
        "axton_subscription",
        "reconcile_cursor",
        "INTEGER NOT NULL DEFAULT 0",
    ),
    ("axton_subscription", "reconcile_bound", "INTEGER"),
    ("axton_subscription", "reconcile_barrier", "INTEGER"),
    ("axton_subscription", "reconcile_error", "TEXT"),
    ("axton_client", "store_epoch", "INTEGER NOT NULL DEFAULT 0"),
    (
        "axton_mutation",
        "store_epoch",
        "INTEGER NOT NULL DEFAULT 0",
    ),
    ("axton_load", "store_epoch", "INTEGER NOT NULL DEFAULT 0"),
    (
        "axton_record",
        "base_state",
        "TEXT NOT NULL DEFAULT 'legacy'",
    ),
    ("axton_record", "evicted_at", "INTEGER NOT NULL DEFAULT 0"),
    ("axton_mutation", "diverged", "INTEGER NOT NULL DEFAULT 0"),
    ("axton_mutation", "call_id", "TEXT"),
    ("axton_mutation", "args", "TEXT"),
    ("axton_mutation", "store", "TEXT"),
    ("axton_client", "push_results", "TEXT"),
    (
        "axton_subscription",
        "bootstrap_state",
        "TEXT NOT NULL DEFAULT 'not_requested'",
    ),
    (
        "axton_subscription",
        "bootstrap_run",
        "INTEGER NOT NULL DEFAULT 0",
    ),
    (
        "axton_subscription",
        "bootstrap_cursor",
        "INTEGER NOT NULL DEFAULT 0",
    ),
    ("axton_subscription", "bootstrap_barrier", "INTEGER"),
    ("axton_subscription", "bootstrap_error", "TEXT"),
];

/// Add every framework column in [`ADDED_COLUMNS`] a table still lacks.
pub fn add_framework_columns<S: ClientStore>(store: &mut S) -> Result<()> {
    store.begin()?;
    let result = (|| {
        for (table, column, definition) in ADDED_COLUMNS {
            let columns = store.query_committed(&format!("PRAGMA table_info({table})"), &[])?;
            if !columns.rows.iter().any(|r| r[1].as_str() == Some(column)) {
                store.execute_batch(&format!(
                    "ALTER TABLE {table} ADD COLUMN {column} {definition}"
                ))?;
            }
        }
        store.execute_batch(
            "UPDATE axton_subscription SET reconcile_state='requested', reconcile_run=reconcile_run+1
             WHERE EXISTS (SELECT 1 FROM axton_client WHERE scope_membership_version=0);
             UPDATE axton_client SET scope_membership_version=1;"
        )?;
        store.execute_batch(
            "CREATE UNIQUE INDEX IF NOT EXISTS axton_mutation_call_id ON axton_mutation(call_id)",
        )?;
        Ok(())
    })();
    match result {
        Ok(()) => match store.commit() {
            Ok(()) => Ok(()),
            Err(error) => {
                let _ = store.rollback();
                Err(error)
            }
        },
        Err(error) => {
            store.rollback()?;
            Err(error)
        }
    }
}

pub const FRAMEWORK_DDL: &str = "
CREATE TABLE IF NOT EXISTS axton_schema (
  descriptor TEXT NOT NULL, created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS axton_client (
  client_id    TEXT PRIMARY KEY,
  next_ordinal INTEGER NOT NULL,
  next_push    INTEGER NOT NULL,
  generation   INTEGER NOT NULL,
  last_completed_push INTEGER NOT NULL DEFAULT 0,
  push_models  TEXT,
  push_results TEXT,
  scope_membership_version INTEGER NOT NULL DEFAULT 1,
  store_epoch INTEGER NOT NULL DEFAULT 0,
  next_subscription INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS axton_record (
  model TEXT NOT NULL, identity TEXT NOT NULL, stamp INTEGER NOT NULL,
  base_state TEXT NOT NULL DEFAULT 'materialized',
  evicted_at INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (model, identity)
);
CREATE TABLE IF NOT EXISTS axton_scope_member (
  scope TEXT NOT NULL, model TEXT NOT NULL, identity TEXT NOT NULL,
  cursor INTEGER NOT NULL CHECK(cursor > 0), present INTEGER NOT NULL CHECK(present IN (0,1)),
  PRIMARY KEY(scope, model, identity)
);
CREATE INDEX IF NOT EXISTS axton_scope_member_record ON axton_scope_member(model, identity, present);
CREATE TABLE IF NOT EXISTS axton_local_replica_layer (
  model TEXT NOT NULL, identity TEXT NOT NULL, operations TEXT NOT NULL,
  PRIMARY KEY(model, identity)
);
CREATE TABLE IF NOT EXISTS axton_subscription (
  scope          TEXT PRIMARY KEY,
  subscription_id  INTEGER NOT NULL UNIQUE,
  starting_cursor  INTEGER,
  cursor           INTEGER,
  bootstrap_state   TEXT NOT NULL DEFAULT 'not_requested',
  bootstrap_run     INTEGER NOT NULL DEFAULT 0,
  bootstrap_cursor  INTEGER NOT NULL DEFAULT 0,
  bootstrap_barrier INTEGER,
  bootstrap_error   TEXT,
  reconcile_state TEXT NOT NULL DEFAULT 'not_requested',
  reconcile_run INTEGER NOT NULL DEFAULT 0,
  reconcile_cursor INTEGER NOT NULL DEFAULT 0,
  reconcile_bound INTEGER,
  reconcile_barrier INTEGER,
  reconcile_error TEXT,
  CHECK ((starting_cursor IS NULL AND cursor IS NULL) OR
         (starting_cursor IS NOT NULL AND cursor IS NOT NULL AND
          starting_cursor >= 0 AND cursor >= starting_cursor))
);
CREATE TABLE IF NOT EXISTS axton_mutation (
  ordinal INTEGER PRIMARY KEY, name TEXT NOT NULL, version INTEGER NOT NULL, push INTEGER,
  store_epoch INTEGER NOT NULL DEFAULT 0,
  diverged INTEGER NOT NULL DEFAULT 0, call_id TEXT UNIQUE, args TEXT, store TEXT,
  CHECK ((call_id IS NULL AND args IS NULL) OR (call_id IS NOT NULL AND args IS NOT NULL))
);
CREATE TABLE IF NOT EXISTS axton_mutation_operation (
  ordinal INTEGER NOT NULL REFERENCES axton_mutation(ordinal) ON DELETE CASCADE,
  position INTEGER NOT NULL,
  kind TEXT NOT NULL CHECK (kind IN ('wire','companion','effect')),
  model TEXT NOT NULL, identity TEXT NOT NULL,
  op TEXT NOT NULL CHECK (op IN ('create','update','delete')),
  \"values\" TEXT,
  PRIMARY KEY (ordinal, position)
);
CREATE INDEX IF NOT EXISTS axton_mutation_operation_record ON axton_mutation_operation (model, identity, ordinal, position);
CREATE TABLE IF NOT EXISTS axton_mutation_dependency (
  ordinal INTEGER NOT NULL REFERENCES axton_mutation(ordinal) ON DELETE CASCADE,
  depends_on INTEGER NOT NULL REFERENCES axton_mutation(ordinal) ON DELETE CASCADE,
  kind TEXT NOT NULL CHECK (kind IN ('lifecycle','sequence')),
  PRIMARY KEY (ordinal, depends_on),
  CHECK (depends_on < ordinal)
);
CREATE TABLE IF NOT EXISTS axton_mutation_prerequisite (
  ordinal INTEGER NOT NULL REFERENCES axton_mutation(ordinal) ON DELETE CASCADE,
  key TEXT NOT NULL, error TEXT,
  PRIMARY KEY (ordinal, key)
);
CREATE TABLE IF NOT EXISTS axton_local_write (
  sequence INTEGER PRIMARY KEY,
  ordinal INTEGER NOT NULL,
  position INTEGER,
  disposition TEXT NOT NULL CHECK (disposition IN ('independent','accepted')),
  model TEXT NOT NULL, identity TEXT NOT NULL,
  op TEXT NOT NULL CHECK (op IN ('create','update','delete')),
  \"values\" TEXT,
  CHECK ((disposition = 'independent') = (position IS NULL))
);
CREATE INDEX IF NOT EXISTS axton_local_write_record ON axton_local_write (model, identity, sequence);
CREATE TABLE IF NOT EXISTS axton_rejection (
  ordinal INTEGER PRIMARY KEY, name TEXT NOT NULL, code TEXT NOT NULL, detail TEXT
);
CREATE TABLE IF NOT EXISTS axton_query_cache (
  key TEXT PRIMARY KEY, contract TEXT NOT NULL, name TEXT NOT NULL,
  version INTEGER NOT NULL, args TEXT NOT NULL, store TEXT NOT NULL,
  generation TEXT NOT NULL, result TEXT
);
CREATE INDEX IF NOT EXISTS axton_query_cache_arguments ON axton_query_cache (contract, name, version, args);
CREATE TABLE IF NOT EXISTS axton_load (
  store_epoch INTEGER NOT NULL DEFAULT 0,
  load_id TEXT PRIMARY KEY, seq INTEGER NOT NULL UNIQUE, ready INTEGER NOT NULL,
  name TEXT NOT NULL, version INTEGER NOT NULL, args TEXT NOT NULL, models TEXT NOT NULL,
  continuation TEXT, run INTEGER NOT NULL,
  phase TEXT NOT NULL CHECK (phase IN ('pending','complete','failed','cancelled')),
  pages INTEGER NOT NULL DEFAULT 0,
  call_id TEXT UNIQUE, intent TEXT,
  retry TEXT CHECK (retry IN ('transport','backend','local')),
  attempts INTEGER NOT NULL DEFAULT 0, error TEXT,
  CHECK ((call_id IS NULL AND intent IS NULL) OR (call_id IS NOT NULL AND intent IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS axton_load_ready ON axton_load (phase, ready);
CREATE TABLE IF NOT EXISTS axton_load_once (
  key TEXT PRIMARY KEY, name TEXT NOT NULL, version INTEGER NOT NULL,
  args TEXT NOT NULL, models TEXT NOT NULL, load_id TEXT NOT NULL UNIQUE
);
CREATE INDEX IF NOT EXISTS axton_load_once_arguments ON axton_load_once (name, version, args);
";

/// What an existing file was laid out by, decided before anything is written.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Layout {
    /// No framework tables: a fresh file.
    Fresh,
    /// This runtime's layout.
    Current,
    /// An earlier runtime's layout (scope claims, push checkpoints, or an
    /// `axton_client` without this layout's columns): only a rebuild can use
    /// the file ([Reconciliation](../../../docs/engineering/architecture/client/storage/reconciliation.md)).
    Legacy(String),
}

/// Classify the file's layout. Reads only: an incompatible file is left
/// exactly as found, pending work included.
pub fn check_layout<S: ClientStore>(store: &mut S) -> Result<Layout> {
    let tables = store.query_committed(
        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'axton\\_%' ESCAPE '\\'",
        &[],
    )?;
    let names: Vec<String> = tables
        .rows
        .iter()
        .filter_map(|r| r[0].as_str().map(str::to_owned))
        .collect();
    for table in LEGACY_TABLES {
        if names.iter().any(|n| n == table) {
            return Ok(Layout::Legacy(format!("table {table}")));
        }
    }
    if !names.iter().any(|n| n == "axton_client") {
        return Ok(Layout::Fresh);
    }
    let columns = store.query_committed("PRAGMA table_info(axton_client)", &[])?;
    for column in CLIENT_COLUMNS {
        if !columns.rows.iter().any(|r| r[1].as_str() == Some(column)) {
            return Ok(Layout::Legacy(format!("axton_client lacks {column}")));
        }
    }
    Ok(Layout::Current)
}

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
