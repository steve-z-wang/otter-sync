//! SQLite implements the client's storage contract with one writer and one reader connection.
use axton_client::{ClientStore, SqlRows};
use axton_core::{Result, invalid};
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use rusqlite::types::{Value as SqlValue, ValueRef};
use rusqlite::{Connection, params_from_iter};
use serde_json::Value;
use std::collections::BTreeSet;
use std::path::Path;
use std::sync::{Arc, Mutex};

pub struct SqliteStore {
    writer: Connection,
    reader: Connection,
    // The actual database inode remains exclusively owned for the Store lifetime.
    _file_lock: Option<StoreOwnership>,
}

struct StoreOwnership {
    _identity: std::fs::File,
    lock: std::fs::File,
}

impl Drop for StoreOwnership {
    fn drop(&mut self) {
        // This final Store field drops after both SQLite connections. Closing
        // alone may retain the lock in a pre-exec child's inherited descriptor.
        let _ = self.lock.unlock();
    }
}

fn db(e: rusqlite::Error) -> axton_core::Error {
    invalid(format!("sqlite: {e}"))
}

fn parameter(value: &Value) -> Result<SqlValue> {
    Ok(match value {
        Value::Null => SqlValue::Null,
        Value::Bool(v) => SqlValue::Integer(i64::from(*v)),
        Value::Number(v) => {
            if let Some(v) = v.as_i64() {
                SqlValue::Integer(v)
            } else {
                SqlValue::Real(v.as_f64().ok_or_else(|| invalid("invalid SQL number"))?)
            }
        }
        Value::String(v) => SqlValue::Text(v.clone()),
        Value::Array(_) | Value::Object(_) => SqlValue::Text(serde_json::to_string(value)?),
    })
}

fn rows(connection: &Connection, sql: &str, parameters: &[Value]) -> Result<SqlRows> {
    let mut statement = connection.prepare(sql).map_err(db)?;
    if !statement.readonly() || statement.column_count() == 0 {
        return Err(invalid("SQL write statements are forbidden"));
    }
    let columns = statement
        .column_names()
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let values = parameters
        .iter()
        .map(parameter)
        .collect::<Result<Vec<_>>>()?;
    let mut cursor = statement.query(params_from_iter(values)).map_err(db)?;
    let mut output = vec![];
    while let Some(row) = cursor.next().map_err(db)? {
        let mut record = Vec::with_capacity(columns.len());
        for index in 0..columns.len() {
            record.push(match row.get_ref(index).map_err(db)? {
                ValueRef::Null => Value::Null,
                ValueRef::Integer(v) => Value::from(v),
                ValueRef::Real(v) => Value::from(v),
                ValueRef::Text(v) => Value::from(
                    std::str::from_utf8(v).map_err(|_| invalid("SQL text must be UTF8"))?,
                ),
                ValueRef::Blob(_) => {
                    return Err(invalid("SQL blobs cannot cross the JSON boundary"));
                }
            });
        }
        output.push(record);
    }
    Ok(SqlRows {
        columns,
        rows: output,
    })
}

/// What SQLite's authorizer saw while one statement was prepared: the tables
/// it reads, and the first action that is not part of a plain `SELECT`.
#[derive(Default)]
struct Seen {
    tables: BTreeSet<String>,
    other: Option<String>,
}

/// The tables `sql` reads, found by SQLite while it prepares the statement
/// on `connection` under an authorizer that lives for that prepare only. The
/// authorizer names a table for every column read, and once with an empty
/// column for a table read without columns (`count(*)`), under the name as
/// written: each is resolved to the stored table name. A CTE is not a table.
fn read_tables(connection: &Connection, sql: &str) -> Result<BTreeSet<String>> {
    let seen = Arc::new(Mutex::new(Seen::default()));
    let record = seen.clone();
    connection
        .authorizer(Some(move |context: AuthContext<'_>| {
            let mut seen = record.lock().unwrap_or_else(|e| e.into_inner());
            match context.action {
                AuthAction::Select | AuthAction::Function { .. } | AuthAction::Recursive => {}
                AuthAction::Read { table_name, .. } => {
                    seen.tables.insert(table_name.to_string());
                }
                other => {
                    seen.other.get_or_insert_with(|| format!("{other:?}"));
                }
            }
            Authorization::Allow
        }))
        .map_err(db)?;
    let prepared = connection.prepare(sql).map(|statement| {
        (
            statement.readonly(),
            statement.column_count(),
            statement.is_explain(),
        )
    });
    connection
        .authorizer(None::<fn(AuthContext<'_>) -> Authorization>)
        .map_err(db)?;
    let (readonly, columns, explain) = prepared.map_err(db)?;
    let seen = std::mem::take(&mut *seen.lock().unwrap_or_else(|e| e.into_inner()));
    if !readonly || columns == 0 {
        return Err(invalid("SQL write statements are forbidden"));
    }
    if explain != 0 || seen.other.is_some() {
        return Err(invalid(format!(
            "only a SELECT can be watched{}",
            seen.other
                .map_or(String::new(), |other| format!(": {other}"))
        )));
    }
    let mut tables = BTreeSet::new();
    for table in seen.tables {
        let stored = connection
            .query_row(
                "SELECT name FROM sqlite_schema WHERE type IN ('table','view') AND name = ?1 COLLATE NOCASE",
                [&table],
                |row| row.get::<_, String>(0),
            )
            .unwrap_or(table);
        if stored.to_ascii_lowercase().starts_with("axton_") {
            return Err(invalid(format!(
                "{stored} is an engine table: watched SQL reads Model tables only"
            )));
        }
        tables.insert(stored);
    }
    Ok(tables)
}

fn name_ok(name: &str) -> Result<()> {
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(invalid("invalid savepoint name"));
    }
    Ok(())
}

static APPLICATION_DATA: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();

fn application_data_directory() -> Result<std::path::PathBuf> {
    if let Some(path) = APPLICATION_DATA.get() {
        return Ok(path.clone());
    }
    let home = std::env::var_os("HOME").map(std::path::PathBuf::from);
    let path = if cfg!(any(target_os = "macos", target_os = "ios")) {
        home.map(|home| home.join("Library/Application Support/AXTON"))
    } else if cfg!(target_os = "linux") {
        std::env::var_os("XDG_DATA_HOME")
            .map(std::path::PathBuf::from)
            .or_else(|| home.map(|home| home.join(".local/share")))
            .map(|root| root.join("axton"))
    } else {
        None
    }
    .filter(|path| path.is_absolute())
    .ok_or_else(|| invalid("stable application data directory required for Store ownership"))?;
    let _ = APPLICATION_DATA.set(path.clone());
    Ok(APPLICATION_DATA.get().cloned().unwrap_or(path))
}

// Whole-file locking the database conflicts with SQLite's own byte locks on
// macOS. Keep ownership on a separate OS-locked file named by physical inode.
// Never unlink these files on unlock: that would split concurrent lock owners.
#[cfg(unix)]
fn ownership_lock_path(file: &std::fs::File) -> Result<std::path::PathBuf> {
    use std::os::unix::fs::MetadataExt;
    let identity = file
        .metadata()
        .map_err(|error| invalid(format!("store identity: {error}")))?;
    let root = application_data_directory()?;
    Ok(root
        .join("axton-store-locks")
        .join(format!("{}-{}.lock", identity.dev(), identity.ino())))
}
#[cfg(not(unix))]
fn ownership_lock_path(_file: &std::fs::File) -> Result<std::path::PathBuf> {
    Err(invalid(
        "physical Store ownership is unsupported on this target",
    ))
}

impl SqliteStore {
    /// Mobile/native host initialization, once for the application's lifetime.
    /// This is not a per-client option. Hosts must consistently resolve their
    /// durable sandbox application-data directory before opening any Store.
    pub fn set_application_data_directory(path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        if !path.is_absolute() {
            return Err(invalid("application data directory must be absolute"));
        }
        if let Some(saved) = APPLICATION_DATA.get() {
            if saved != path {
                return Err(invalid("application data directory already fixed"));
            }
            return Ok(());
        }
        APPLICATION_DATA
            .set(path.to_path_buf())
            .map_err(|_| invalid("application data directory already fixed"))
    }

    /// Protocol-4 file ownership. Acquire before SQLite can change journal or
    /// schema state. OS locking follows the inode through aliases/hard links.
    pub fn open_exclusive(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|error| invalid(format!("sqlite file: {error}")))?;
        let lock_path = ownership_lock_path(&file)?;
        std::fs::create_dir_all(lock_path.parent().unwrap())
            .map_err(|error| invalid(format!("store lock directory: {error}")))?;
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)
            .map_err(|error| invalid(format!("store lock file: {error}")))?;
        lock.try_lock()
            .map_err(|error| invalid(format!("store_in_use: {error}")))?;
        let ownership = StoreOwnership {
            _identity: file,
            lock,
        };
        let mut store = Self::open(path)?;
        // Keep database identity open too, preventing inode reuse while owned.
        store._file_lock = Some(ownership);
        Ok(store)
    }
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let writer = Connection::open(&path).map_err(db)?;
        writer
            .execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=1000;",
            )
            .map_err(db)?;
        let reader = Connection::open(&path).map_err(db)?;
        reader
            .execute_batch(
                "PRAGMA query_only=ON; PRAGMA foreign_keys=ON; PRAGMA busy_timeout=1000;",
            )
            .map_err(db)?;
        // A double-quoted name that is not a column must be an error, never a
        // string literal: with the legacy fallback a statement prepared
        // against a stale schema silently returns the column's name as text.
        for connection in [&writer, &reader] {
            connection
                .set_db_config(rusqlite::config::DbConfig::SQLITE_DBCONFIG_DQS_DML, false)
                .map_err(db)?;
            connection
                .set_db_config(rusqlite::config::DbConfig::SQLITE_DBCONFIG_DQS_DDL, false)
                .map_err(db)?;
        }
        Ok(Self {
            writer,
            reader,
            _file_lock: None,
        })
    }
}

impl ClientStore for SqliteStore {
    fn begin(&mut self) -> Result<()> {
        self.writer.execute_batch("BEGIN IMMEDIATE").map_err(db)
    }
    fn commit(&mut self) -> Result<()> {
        self.writer.execute_batch("COMMIT").map_err(db)
    }
    fn rollback(&mut self) -> Result<()> {
        self.writer.execute_batch("ROLLBACK").map_err(db)
    }
    fn savepoint(&mut self, name: &str) -> Result<()> {
        name_ok(name)?;
        self.writer
            .execute_batch(&format!("SAVEPOINT {name}"))
            .map_err(db)
    }
    fn release(&mut self, name: &str) -> Result<()> {
        name_ok(name)?;
        self.writer
            .execute_batch(&format!("RELEASE {name}"))
            .map_err(db)
    }
    fn rollback_to(&mut self, name: &str) -> Result<()> {
        name_ok(name)?;
        self.writer
            .execute_batch(&format!("ROLLBACK TO {name}; RELEASE {name}"))
            .map_err(db)
    }
    fn execute(&mut self, sql: &str, parameters: &[Value]) -> Result<usize> {
        let values = parameters
            .iter()
            .map(parameter)
            .collect::<Result<Vec<_>>>()?;
        self.writer
            .execute(sql, params_from_iter(values))
            .map_err(db)
    }
    fn execute_batch(&mut self, sql: &str) -> Result<()> {
        self.writer.execute_batch(sql).map_err(db)
    }
    fn query(&mut self, sql: &str, parameters: &[Value]) -> Result<SqlRows> {
        rows(&self.writer, sql, parameters)
    }
    fn query_committed(&mut self, sql: &str, parameters: &[Value]) -> Result<SqlRows> {
        rows(&self.reader, sql, parameters)
    }
    fn read_tables(&mut self, sql: &str) -> Result<BTreeSet<String>> {
        read_tables(&self.reader, sql)
    }
}
