//! One handle per open transaction. Every module adds methods to it.
use crate::store::{ClientStore, SqlRows};
use axton_core::{Result, Schema, invalid};
use serde_json::Value;
use std::collections::BTreeSet;

pub struct Engine<'a, S: ClientStore> {
    pub store: &'a mut S,
    pub schema: &'a Schema,
    pub changed: &'a mut BTreeSet<String>,
    pub committed: bool,
}

impl<'a, S: ClientStore> Engine<'a, S> {
    pub fn new(
        store: &'a mut S,
        schema: &'a Schema,
        changed: &'a mut BTreeSet<String>,
        committed: bool,
    ) -> Self {
        Self {
            store,
            schema,
            changed,
            committed,
        }
    }
    pub fn rows(&mut self, sql: &str, parameters: &[Value]) -> Result<SqlRows> {
        if self.committed {
            self.store.query_committed(sql, parameters)
        } else {
            self.store.query(sql, parameters)
        }
    }
    pub fn exec(&mut self, table: &str, sql: &str, parameters: &[Value]) -> Result<usize> {
        self.changed.insert(table.to_string());
        self.store.execute(sql, parameters)
    }
    pub fn scalar(&mut self, sql: &str, parameters: &[Value]) -> Result<Option<Value>> {
        Ok(self
            .rows(sql, parameters)?
            .rows
            .into_iter()
            .next()
            .and_then(|r| r.into_iter().next()))
    }
}

pub(crate) fn as_u64(value: &Value) -> Result<u64> {
    value
        .as_u64()
        .or_else(|| value.as_i64().and_then(|v| u64::try_from(v).ok()))
        .ok_or_else(|| invalid("expected an unsigned integer"))
}
