//! Watched read-only SQL over several Models
//! ([#184](https://github.com/zanminwang/axton/issues/184)).
//!
//! `watchSql {sql, parameters}` asks the store which tables the statement
//! reads - SQLite's answer, found while it prepares the statement, never the
//! application's - and refuses anything but one read-only `SELECT` (or
//! `WITH … SELECT`) that reads no engine table. It then runs the statement on
//! the committed reader, answers its observer id and publishes the rows after
//! the task's completion, as a `watch` does.
//!
//! The engine signals the watch through [`Client::watch`] after every commit
//! that wrote one of its tables, whatever the path: a local write, a
//! settlement or rejection rollback, an optimistic replay, a Channel page, a
//! Load page, a Fetch or a rebuild. At the end of a unit with no callback
//! transaction open, only a signalled watch re-runs, and it publishes only a
//! result that differs from the last one. A commit that writes none of its
//! tables does not re-run it. A re-run that fails is reported and the watch
//! stays; `unwatch` ends it with no snapshot, and close with a terminal one.
//! Both also remove its engine watcher. A statement that reads no table a
//! commit writes (`SELECT 1`, SQLite's own `sqlite_*` tables) registers
//! none: no commit signals it.
use super::*;
use std::sync::mpsc::Receiver;

#[derive(Default)]
pub(super) struct SqlWatches {
    /// By the number behind their observer id: registration order.
    watches: BTreeMap<u64, SqlWatch>,
}

struct SqlWatch {
    sql: String,
    parameters: Vec<Value>,
    /// The engine watcher, by its id, signalled by every commit that wrote a
    /// table the statement reads; none when it reads no table.
    changes: Option<(u64, Receiver<()>)>,
    rows: Value,
    /// `rows` were published.
    published: bool,
}

impl<S: ClientStore + 'static> ClientRuntime<S> {
    /// `unwatch` of one of these: forget it and its engine watcher.
    pub(super) fn unwatch_sql(&mut self, id: u64) {
        if let Some(watch) = self.observers.sql.watches.remove(&id)
            && let Some((watcher, _)) = watch.changes
        {
            self.client.unwatch(watcher);
        }
    }
    /// `watchSql {sql, parameters}`: find the tables, run the statement now
    /// and publish its rows after the task's completion.
    pub(super) fn watch_sql(
        &mut self,
        sql: &str,
        parameters: &[Value],
    ) -> std::result::Result<Value, String> {
        let tables = self.client.sql_tables(sql).map_err(|e| e.to_string())?;
        let rows = self
            .client
            .read_sql(sql, parameters)
            .map_err(|e| e.to_string())?;
        let id = self.issue()?;
        // SQLite's own tables (`sqlite_schema`, `dbstat`'s source) are in no
        // commit's written set: only the others can signal the watch.
        let tables: BTreeSet<String> = tables
            .into_iter()
            .filter(|table| !table.to_ascii_lowercase().starts_with("sqlite_"))
            .collect();
        let changes = (!tables.is_empty()).then(|| self.client.watch_keyed(tables));
        self.observers.sql.watches.insert(
            id,
            SqlWatch {
                sql: sql.to_string(),
                parameters: parameters.to_vec(),
                changes,
                rows: Value::Array(rows),
                published: false,
            },
        );
        Ok(json!({"observerId": id.to_string()}))
    }

    /// Re-run every watch a commit signalled since it last ran. Never while a
    /// callback transaction is open: its writes are not committed, and the
    /// signal of an earlier commit waits for the next unit.
    pub(super) fn rerun_sql_watches(&mut self) {
        if self.transaction.is_some() {
            return;
        }
        let ids: Vec<u64> = self.observers.sql.watches.keys().copied().collect();
        for id in ids {
            let Some(watch) = self.observers.sql.watches.get(&id) else {
                continue;
            };
            let signalled = watch
                .changes
                .as_ref()
                .is_some_and(|(_, changes)| changes.try_iter().count() > 0);
            if !signalled {
                continue;
            }
            let (sql, parameters) = (watch.sql.clone(), watch.parameters.clone());
            match self.client.read_sql(&sql, &parameters) {
                Ok(rows) => {
                    let rows = Value::Array(rows);
                    if let Some(watch) = self.observers.sql.watches.get_mut(&id)
                        && watch.rows != rows
                    {
                        watch.rows = rows;
                        watch.published = false;
                    }
                }
                Err(error) => self.error(error.to_string()),
            }
        }
    }

    /// Publish every result not published yet.
    pub(super) fn publish_sql_watches(&mut self) {
        let mut snapshots = vec![];
        for (id, watch) in &mut self.observers.sql.watches {
            if !watch.published {
                watch.published = true;
                snapshots.push(Event::ObserverChanged {
                    observer_id: id.to_string(),
                    snapshot: json!({"kind": "watch", "rows": watch.rows}),
                });
            }
        }
        self.events.extend(snapshots);
    }

    /// Close: every watch ends with its terminal snapshot, carrying its last
    /// rows.
    pub(super) fn close_sql_watches(&mut self) {
        for (id, watch) in std::mem::take(&mut self.observers.sql.watches) {
            if let Some((watcher, _)) = watch.changes {
                self.client.unwatch(watcher);
            }
            self.events.push(Event::ObserverChanged {
                observer_id: id.to_string(),
                snapshot: json!({"kind": "watch", "rows": watch.rows, "closed": true}),
            });
        }
    }
}
