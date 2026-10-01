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
/// columns carry a Stream's historical load beside its subscription
/// ([#151](https://github.com/zanminwang/axton/issues/151)), whose defaults are
/// a load that was never requested.
const ADDED_COLUMNS: &[(&str, &str, &str)] = &[
    (
        "axton_client",
        "stream_membership_version",
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
    let result = add_framework_columns_in_transaction(store);
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

fn add_framework_columns_in_transaction<S: ClientStore>(store: &mut S) -> Result<()> {
    for (table, column, definition) in ADDED_COLUMNS {
        let columns = store.query(&format!("PRAGMA table_info({table})"), &[])?;
        if !columns.rows.iter().any(|r| r[1].as_str() == Some(column)) {
            store.execute_batch(&format!(
                "ALTER TABLE {table} ADD COLUMN {column} {definition}"
            ))?;
        }
    }
    store.execute_batch(
        "CREATE UNIQUE INDEX IF NOT EXISTS axton_mutation_call_id ON axton_mutation(call_id)",
    )?;
    Ok(())
}

/// Upgrade framework ownership names before fresh DDL or layout reconciliation.
/// Durable business JSON and the membership reconciliation marker are untouched.
pub fn migrate_stream_layout<S: ClientStore>(store: &mut S) -> Result<()> {
    store.begin()?;
    let result = (|| {
        if !validate_authority_layout(store)? {
            return Ok(());
        }
        migrate_channel_layout(store)?;
        let rows = store.query("SELECT name FROM sqlite_master WHERE type='table'", &[])?;
        let tables: Vec<&str> = rows.rows.iter().filter_map(|r| r[0].as_str()).collect();
        let columns = |store: &mut S, table: &str| -> Result<Vec<String>> {
            Ok(store
                .query(&format!("PRAGMA table_info({table})"), &[])?
                .rows
                .iter()
                .filter_map(|r| r[1].as_str().map(str::to_owned))
                .collect())
        };
        let client = columns(store, "axton_client")?;
        let subscription = columns(store, "axton_subscription")?;
        let old = tables.contains(&"axton_scope_member")
            || subscription.iter().any(|c| c == "scope")
            || client.iter().any(|c| c == "scope_membership_version");
        if !old {
            return migrate_local_authority(store);
        }
        // Older incompatible replicas are still rebuilt beside by opening;
        // do not turn their pre-membership Scope vocabulary into a refusal.
        if LEGACY_TABLES.iter().any(|table| tables.contains(table))
            || (tables.contains(&"axton_client")
                && CLIENT_COLUMNS
                    .iter()
                    .any(|required| !client.iter().any(|column| column == required)))
        {
            return Ok(());
        }
        if tables.contains(&"axton_stream_member")
            || subscription.iter().any(|c| c == "stream")
            || client.iter().any(|c| c == "stream_membership_version")
        {
            return Err(invalid("conflicting Scope and Stream framework layouts"));
        }
        if !tables.contains(&"axton_client")
            || !tables.contains(&"axton_subscription")
            || !subscription.iter().any(|c| c == "scope")
        {
            return Err(invalid("incomplete Scope framework layout"));
        }
        if client.iter().any(|c| c == "scope_membership_version")
            && !tables.contains(&"axton_scope_member")
        {
            return Err(invalid("Scope membership layout lacks axton_scope_member"));
        }
        if tables.contains(&"axton_scope_member") {
            let member = columns(store, "axton_scope_member")?;
            if ["scope", "model", "identity", "cursor", "present"]
                .iter()
                .any(|required| !member.iter().any(|c| c == required))
                || member.iter().any(|c| c == "stream")
            {
                return Err(invalid(
                    "conflicting or incomplete Scope membership columns",
                ));
            }
            store.execute_batch("ALTER TABLE axton_scope_member RENAME TO axton_stream_member;
                ALTER TABLE axton_stream_member RENAME COLUMN scope TO stream;
                DROP INDEX IF EXISTS axton_scope_member_record;
                CREATE INDEX axton_stream_member_record ON axton_stream_member(model, identity, present);")?;
        }
        store.execute_batch("ALTER TABLE axton_subscription RENAME COLUMN scope TO stream")?;
        if client.iter().any(|c| c == "scope_membership_version") {
            store.execute_batch("ALTER TABLE axton_client RENAME COLUMN scope_membership_version TO stream_membership_version")?;
        }
        migrate_local_authority(store)
    })();
    match result {
        Ok(()) => match store.commit() {
            Ok(()) => Ok(()),
            Err(e) => {
                let _ = store.rollback();
                Err(e)
            }
        },
        Err(e) => {
            store.rollback()?;
            Err(e)
        }
    }
}

/// Validate the original vocabulary before any rename or destructive migration.
/// Unsupported checkpoint layouts retain the existing rebuild-beside policy.
fn validate_authority_layout<S: ClientStore>(store: &mut S) -> Result<bool> {
    let catalog = store.query("SELECT type,name,tbl_name FROM sqlite_master", &[])?;
    let has_table = |name: &str| catalog.rows.iter().any(|r| r[0] == "table" && r[1] == name);
    let columns = |store: &mut S, name: &str| -> Result<Vec<String>> {
        Ok(store
            .query(&format!("PRAGMA table_info({name})"), &[])?
            .rows
            .iter()
            .filter_map(|r| r[1].as_str().map(str::to_owned))
            .collect())
    };
    let client = columns(store, "axton_client")?;
    if LEGACY_TABLES.iter().any(|t| has_table(t)) {
        return Ok(false);
    }
    if has_table("axton_client")
        && CLIENT_COLUMNS
            .iter()
            .any(|c| !client.iter().any(|v| v == c))
    {
        if client.iter().any(|c| {
            [
                "local_authority_version",
                "channel_membership_version",
                "scope_membership_version",
                "stream_membership_version",
            ]
            .contains(&c.as_str())
        }) {
            return Err(invalid("incomplete modern client columns"));
        }
        return Ok(false);
    }
    let subscription = columns(store, "axton_subscription")?;
    let vocabularies: Vec<_> = ["channel", "scope", "stream"]
        .into_iter()
        .filter(|v| {
            has_table(&format!("axton_{v}_member"))
                || subscription.iter().any(|c| c == v)
                || client
                    .iter()
                    .any(|c| c == &format!("{v}_membership_version"))
        })
        .collect();
    if !has_table("axton_client") {
        if !vocabularies.is_empty() {
            return Err(invalid("incomplete authority framework layout"));
        }
        return Ok(false);
    }
    if vocabularies.len() != 1 {
        return Err(invalid("conflicting framework vocabularies"));
    }
    let vocabulary = vocabularies[0];
    if [vocabulary, "subscription_id", "starting_cursor", "cursor"]
        .iter()
        .any(|required| !subscription.iter().any(|c| c == required))
    {
        return Err(invalid("incomplete subscription columns"));
    }
    let modern = has_table(&format!("axton_{vocabulary}_member"))
        || client.iter().any(|c| {
            c == "local_authority_version" || c == &format!("{vocabulary}_membership_version")
        });
    for table in FRAMEWORK_TABLES {
        // These were additive framework tables before the membership era.
        let additive = [
            "axton_local_replica_layer",
            "axton_local_write",
            "axton_query_cache",
            "axton_load",
            "axton_load_once",
        ]
        .contains(table);
        if !has_table(table) && (modern || !additive) {
            return Err(invalid(format!(
                "incomplete authority framework layout: {table}"
            )));
        }
    }
    // Names alone cannot prove a supported layout: validate durable work's
    // original fields before dropping the old ledger. Only pre-membership
    // layouts may receive additive defaults after this migration.
    for (table, required) in [
        ("axton_schema", &["descriptor", "created_at"][..]),
        (
            "axton_client",
            &[
                "client_id",
                "next_ordinal",
                "next_push",
                "generation",
                "last_completed_push",
                "push_models",
                "next_subscription",
            ][..],
        ),
        ("axton_record", &["model", "identity", "stamp"][..]),
        (
            "axton_local_replica_layer",
            &["model", "identity", "operations"][..],
        ),
        (
            "axton_mutation",
            &["ordinal", "name", "version", "push"][..],
        ),
        (
            "axton_mutation_operation",
            &[
                "ordinal", "position", "kind", "model", "identity", "op", "values",
            ][..],
        ),
        (
            "axton_mutation_dependency",
            &["ordinal", "depends_on", "kind"][..],
        ),
        (
            "axton_mutation_prerequisite",
            &["ordinal", "key", "error"][..],
        ),
        (
            "axton_local_write",
            &[
                "sequence",
                "ordinal",
                "position",
                "disposition",
                "model",
                "identity",
                "op",
                "values",
            ][..],
        ),
        (
            "axton_rejection",
            &["ordinal", "name", "code", "detail"][..],
        ),
        (
            "axton_query_cache",
            &[
                "key",
                "contract",
                "name",
                "version",
                "args",
                "store",
                "generation",
                "result",
            ][..],
        ),
        (
            "axton_load",
            &[
                "load_id",
                "seq",
                "ready",
                "name",
                "version",
                "args",
                "models",
                "continuation",
                "run",
                "phase",
                "pages",
                "call_id",
                "intent",
                "retry",
                "attempts",
                "error",
            ][..],
        ),
        (
            "axton_load_once",
            &["key", "name", "version", "args", "models", "load_id"][..],
        ),
    ] {
        if !has_table(table) {
            continue;
        }
        let existing = columns(store, table)?;
        if required
            .iter()
            .any(|required| !existing.iter().any(|c| c == required))
        {
            return Err(invalid(format!("incomplete framework columns: {table}")));
        }
    }
    let marker = if client.iter().any(|c| c == "local_authority_version") {
        let values = store.query("SELECT local_authority_version FROM axton_client", &[])?;
        if values.rows.iter().any(|r| r[0] != 0 && r[0] != 1) {
            return Err(invalid("invalid local authority marker"));
        }
        if values.rows.iter().any(|r| r[0] == 0) && values.rows.iter().any(|r| r[0] == 1) {
            return Err(invalid("conflicting local authority markers"));
        }
        values.rows.first().map(|r| r[0] == 1).unwrap_or(true)
    } else {
        false
    };
    let member_table = format!("axton_{vocabulary}_member");
    if modern {
        // Known membership-era and completed layouts already have these fields.
        // Default repair would replace durable work or legacy eviction fences.
        // Only genuinely pre-membership additive layouts may lack them.
        for (table, field, _) in ADDED_COLUMNS {
            let required = if *field == "stream_membership_version" {
                format!("{vocabulary}_membership_version")
            } else {
                (*field).to_owned()
            };
            if !columns(store, table)?.iter().any(|c| c == &required) {
                return Err(invalid(format!(
                    "incomplete modern authority columns: {table}.{required}"
                )));
            }
        }
    }
    if marker {
        if vocabulary != "stream"
            || has_table(&member_table)
            || catalog.rows.iter().any(|r| {
                [
                    "axton_channel_member_record",
                    "axton_scope_member_record",
                    "axton_stream_member_record",
                ]
                .iter()
                .any(|name| r[1] == *name)
            })
        {
            return Err(invalid(
                "completed authority layout still contains holdings",
            ));
        }
        return Ok(true);
    }
    let membership_marker = format!("{vocabulary}_membership_version");
    if client.iter().any(|c| c == &membership_marker) && !has_table(&member_table) {
        return Err(invalid("membership layout lacks holding table"));
    }
    if has_table(&member_table) {
        let member = columns(store, &member_table)?;
        if [vocabulary, "model", "identity", "cursor", "present"]
            .iter()
            .any(|required| !member.iter().any(|c| c == required))
        {
            return Err(invalid("incomplete membership columns"));
        }
    }
    for vocabulary in ["channel", "scope", "stream"] {
        let index = format!("axton_{vocabulary}_member_record");
        if catalog.rows.iter().any(|r| {
            r[1] == index && (r[0] != "index" || r[2] != format!("axton_{vocabulary}_member"))
        }) {
            return Err(invalid("holding index has wrong owner"));
        }
    }
    Ok(true)
}

fn migrate_local_authority<S: ClientStore>(store: &mut S) -> Result<()> {
    let client = store.query("PRAGMA table_info(axton_client)", &[])?;
    if client
        .rows
        .iter()
        .any(|r| r[1] == "local_authority_version")
    {
        let marker = store.query("SELECT local_authority_version FROM axton_client", &[])?;
        if marker.rows.iter().all(|r| r[0] == 1) {
            return Ok(());
        }
    } else {
        store.execute_batch("ALTER TABLE axton_client ADD COLUMN local_authority_version INTEGER NOT NULL DEFAULT 0")?;
    }
    // Completion describes a complete layout, including genuinely older
    // additive tables/fields. open_at may validate it again before Client::open.
    store.execute_batch(FRAMEWORK_DDL)?;
    add_framework_columns_in_transaction(store)?;
    store.execute_batch(
        "DROP INDEX IF EXISTS axton_stream_member_record;
        DROP TABLE IF EXISTS axton_stream_member;
        UPDATE axton_client SET local_authority_version=1;",
    )?;
    Ok(())
}

fn migrate_channel_layout<S: ClientStore>(store: &mut S) -> Result<()> {
    let rows = store.query("SELECT name FROM sqlite_master WHERE type='table'", &[])?;
    let tables: Vec<&str> = rows.rows.iter().filter_map(|r| r[0].as_str()).collect();
    let columns = |store: &mut S, table: &str| -> Result<Vec<String>> {
        Ok(store
            .query(&format!("PRAGMA table_info({table})"), &[])?
            .rows
            .iter()
            .filter_map(|r| r[1].as_str().map(str::to_owned))
            .collect())
    };
    let client = columns(store, "axton_client")?;
    let subscription = columns(store, "axton_subscription")?;
    let old_member = tables.contains(&"axton_channel_member");
    let old = old_member
        || subscription.iter().any(|c| c == "channel")
        || client.iter().any(|c| c == "channel_membership_version");
    if !old {
        return Ok(());
    }
    if tables.contains(&"axton_scope_member")
        || subscription.iter().any(|c| c == "scope")
        || client.iter().any(|c| c == "scope_membership_version")
    {
        return Err(invalid("conflicting old and Scope framework layouts"));
    }
    if !tables.contains(&"axton_client")
        || !tables.contains(&"axton_subscription")
        || !subscription.iter().any(|c| c == "channel")
    {
        return Err(invalid("incomplete old framework layout"));
    }
    if client.iter().any(|c| c == "channel_membership_version") && !old_member {
        return Err(invalid("old membership layout lacks axton_channel_member"));
    }
    if old_member {
        let member = columns(store, "axton_channel_member")?;
        if !member.iter().any(|c| c == "channel") || member.iter().any(|c| c == "scope") {
            return Err(invalid("conflicting or incomplete old membership columns"));
        }
        store.execute_batch("ALTER TABLE axton_channel_member RENAME TO axton_scope_member;
                ALTER TABLE axton_scope_member RENAME COLUMN channel TO scope;
                DROP INDEX IF EXISTS axton_channel_member_record;
                CREATE INDEX axton_scope_member_record ON axton_scope_member(model, identity, present);")?;
    }
    store.execute_batch("ALTER TABLE axton_subscription RENAME COLUMN channel TO scope")?;
    if client.iter().any(|c| c == "channel_membership_version") {
        store.execute_batch("ALTER TABLE axton_client RENAME COLUMN channel_membership_version TO scope_membership_version")?;
    }
    Ok(())
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
  stream_membership_version INTEGER NOT NULL DEFAULT 1,
  local_authority_version INTEGER NOT NULL DEFAULT 1,
  store_epoch INTEGER NOT NULL DEFAULT 0,
  next_subscription INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS axton_record (
  model TEXT NOT NULL, identity TEXT NOT NULL, stamp INTEGER NOT NULL,
  base_state TEXT NOT NULL DEFAULT 'materialized',
  evicted_at INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (model, identity)
);
CREATE TABLE IF NOT EXISTS axton_local_replica_layer (
  model TEXT NOT NULL, identity TEXT NOT NULL, operations TEXT NOT NULL,
  PRIMARY KEY(model, identity)
);
CREATE TABLE IF NOT EXISTS axton_subscription (
  stream          TEXT PRIMARY KEY,
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
