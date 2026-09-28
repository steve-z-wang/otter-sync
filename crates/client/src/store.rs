//! Storage contract: a SQL executor with transactions. The engine owns every statement.
use axton_core::{Result, invalid};
use serde_json::Value;
use std::collections::BTreeSet;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct SqlRows {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

pub trait ClientStore {
    fn begin(&mut self) -> Result<()>;
    fn commit(&mut self) -> Result<()>;
    fn rollback(&mut self) -> Result<()>;
    fn savepoint(&mut self, name: &str) -> Result<()>;
    fn release(&mut self, name: &str) -> Result<()>;
    fn rollback_to(&mut self, name: &str) -> Result<()>;
    fn execute(&mut self, sql: &str, parameters: &[Value]) -> Result<usize>;
    fn execute_batch(&mut self, sql: &str) -> Result<()>;
    fn query(&mut self, sql: &str, parameters: &[Value]) -> Result<SqlRows>;
    fn query_committed(&mut self, sql: &str, parameters: &[Value]) -> Result<SqlRows>;
    /// The tables `sql` reads, by their stored names, found by preparing it on
    /// the committed reader without running it
    /// ([#184](https://github.com/zanminwang/axton/issues/184)). Refused
    /// unless `sql` is one read-only `SELECT` (or `WITH … SELECT`) that
    /// returns columns and reads no engine table (`axton_*`). A store that
    /// cannot tell refuses, so it offers no reactive SQL.
    fn read_tables(&mut self, sql: &str) -> Result<BTreeSet<String>> {
        let _ = sql;
        Err(invalid("this store cannot watch SQL"))
    }
}
