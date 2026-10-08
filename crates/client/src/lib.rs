//! Client engine over per-model SQLite tables. No state lives in memory between calls.
pub mod actions;
pub mod authority;
pub mod bootstrap;
pub mod ddl;
mod defaults;
pub mod engine;
mod mutate;
pub mod mutation_queue;
mod policies;
pub mod query;
pub mod queue;
pub mod rows;
pub mod runtime;
pub mod settlement05;
pub mod store;
pub mod store05;
pub mod subscriptions;
pub mod sync05;
pub mod unsent;

pub use actions::SubmittedCall;
pub use axton_core::*;
pub use bootstrap::{
    BootstrapError, BootstrapPhase, BootstrapRecordFailure, BootstrapState, SUBSCRIPTION_CLOSED,
};
pub use query::{Direction, QueryOrder, QuerySpec};
pub use store::*;
pub use subscriptions::SubscriptionState;
pub use unsent::{FailedAct, FailedTask, RefusedAct, SubmittedAct};

use engine::Engine;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::mpsc::{self, Receiver, Sender};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum OperationKind {
    Create,
    Update,
    Delete,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Operation {
    pub model: String,
    pub op: OperationKind,
    pub identity: Value,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub values: Option<Value>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Mutation {
    pub name: String,
    #[serde(default = "one")]
    pub version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub call_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args: Option<Value>,
    // The Action call's store policy; only meaningful with `call_id`.
    pub operations: Vec<Operation>,
    #[serde(default)]
    pub companion: Vec<Operation>,
    #[serde(default)]
    pub effects: Vec<Operation>,
    #[serde(default)]
    pub prerequisites: Vec<String>,
    #[serde(default)]
    pub lifecycle_dependencies: Vec<u64>,
    #[serde(default)]
    pub sequence_dependencies: Vec<u64>,
}
fn one() -> u64 {
    1
}
// The read contracts a client of `schema` expects: every model with the
// version its generated types read. Declared on every push, pull and
// subscribe so receipts, HTTP catch-up and the live stream are served alike
// ([#91](https://github.com/zanminwang/axton/issues/91)).
pub fn declared_models(schema: &Schema) -> BTreeMap<String, u64> {
    schema
        .models
        .iter()
        .map(|m| (m.name.clone(), m.version))
        .collect()
}
impl Mutation {
    pub fn new(name: impl Into<String>, operations: Vec<Operation>) -> Self {
        Self {
            name: name.into(),
            version: 1,
            call_id: None,
            args: None,
            operations,
            companion: vec![],
            effects: vec![],
            prerequisites: vec![],
            lifecycle_dependencies: vec![],
            sequence_dependencies: vec![],
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Readiness {
    Pending,
    Ready,
    Failed,
}
// Why one record or one queued mutation could not be applied as delivered.
// Every kind leaves the client consistent; the report is for the application
// ([#51](https://github.com/zanminwang/axton/issues/51),
// [#122](https://github.com/zanminwang/axton/issues/122)).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ReportKind {
    // The server could not read the record: the change carried `error`
    // instead of a state. Local content and stamp are kept.
    ReadFailed,
    // The delivered state does not fit this client's schema. Nothing written.
    Skipped,
    // The same stamp with different content. Nothing written.
    Conflict,
    // A queued operation no longer replays over the new base: the base is
    // visible and the mutation is still sent (`ordinal`).
    Diverged,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Report {
    pub kind: ReportKind,
    pub model: String,
    pub identity: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ordinal: Option<u64>,
    #[serde(default)]
    pub detail: Value,
}
impl Report {
    pub(crate) fn new(kind: ReportKind, model: &str, identity: &Value) -> Self {
        Self {
            kind,
            model: model.to_string(),
            identity: identity.clone(),
            code: None,
            ordinal: None,
            detail: Value::Null,
        }
    }
}
// What applying a receipt or a page came to. `cursors` are the stream
// cursors the page moved, at their new values.
#[derive(Debug, Default, Serialize)]
pub struct ApplyReport {
    pub applied: usize,
    pub stale: bool,
    pub cursors: BTreeMap<String, u64>,
    pub reports: Vec<Report>,
    // Invocation outcomes are emitted after settlement and never stored locally.
    pub completions: Vec<CallCompletion>,
}
impl ApplyReport {
    pub fn count(&self, kind: ReportKind) -> usize {
        self.reports.iter().filter(|r| r.kind == kind).count()
    }
    pub fn skipped(&self) -> usize {
        self.count(ReportKind::Skipped)
    }
    pub fn conflicts(&self) -> usize {
        self.count(ReportKind::Conflict)
    }
    pub fn diverged(&self) -> usize {
        self.count(ReportKind::Diverged)
    }
    pub fn read_failed(&self) -> usize {
        self.count(ReportKind::ReadFailed)
    }
}

// A transaction the host holds open across calls, with its own savepoint stack.
struct Session {
    changed: BTreeSet<String>,
    savepoints: Vec<SessionSavepoint>,
    counter: u64,
    // Ordinals of the calls submitted in this transaction.
    submitted: BTreeSet<u64>,
    // The session delivers incoming authority (`prepare_store`): its store
    // hooks are local-only and submit no Mutation.
}

struct SessionSavepoint {
    name: String,
    changed: BTreeSet<String>,
    submitted: BTreeSet<u64>,
}

pub struct Client<S: ClientStore> {
    store: S,
    context05: Option<v05::RequestContext>,
    schema: Schema,
    client_id: String,
    generation: u64,
    // Table watchers by the id [`Client::watch_keyed`] answered.
    watchers: Vec<(u64, BTreeSet<String>, Sender<()>)>,
    // The last watcher id issued.
    watcher_ids: u64,
    session: Option<Session>,
    last_changed: BTreeSet<String>,
    last_bootstrap: BTreeSet<String>,
}

// Call completions canceled by an explicit Store05 reset.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AbandonedCall {
    pub call_id: String,
    pub frozen: bool,
}

// Marker a transaction leaves in its changed set when it subscribes or
// unsubscribes a stream; stripped before the set reaches watchers.

// Marker a transaction leaves in its changed set when it changes a Stream's
// bootstrap state ([#151](https://github.com/zanminwang/axton/issues/151)).
// A load request changes no membership, so - unlike [`SUBSCRIPTION_MARK`] - it
// bumps neither the subscription generation nor a stream epoch: registering a
// load must not make the open live session stale or a pull in flight. It is
// stripped like the other mark, and the Streams it named are read back through
// [`Client::last_bootstrap_streams`].
pub(crate) const BOOTSTRAP_MARK: &str = "axton_bootstrap:";

// Take every mark with `prefix` out of `changed` and answer with the names
// they carried. A mark is a signal about a transaction, never a table, so it
// never reaches a watcher or a host's changed-table list.
fn strip_marks(changed: &mut BTreeSet<String>, prefix: &str) -> BTreeSet<String> {
    let marks: Vec<String> = changed
        .iter()
        .filter(|t| t.starts_with(prefix))
        .cloned()
        .collect();
    marks
        .into_iter()
        .map(|mark| {
            changed.remove(&mark);
            mark[prefix.len()..].to_string()
        })
        .collect()
}

impl<S: ClientStore> Client<S> {
    pub fn client_id(&self) -> &str {
        &self.client_id
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn last_changed(&self) -> &BTreeSet<String> {
        &self.last_changed
    }
    // The Streams whose bootstrap state the last committed transaction changed.
    // The scheduler reads its work from [`Client::bootstrap_tasks`]; this says
    // whether a commit touched any of it at all.
    pub fn last_bootstrap_streams(&self) -> &BTreeSet<String> {
        &self.last_bootstrap
    }
    pub fn session_active(&self) -> bool {
        self.session.is_some()
    }
    pub fn watch(&mut self, tables: BTreeSet<String>) -> Receiver<()> {
        self.watch_keyed(tables).1
    }
    // [`Client::watch`], with the id [`Client::unwatch`] removes it by. A
    // watcher whose receiver is dropped is otherwise kept until a commit
    // to one of its tables finds it gone.
    pub fn watch_keyed(&mut self, tables: BTreeSet<String>) -> (u64, Receiver<()>) {
        let (tx, rx) = mpsc::channel();
        self.watcher_ids += 1;
        self.watchers.push((self.watcher_ids, tables, tx));
        (self.watcher_ids, rx)
    }
    // Forget the watcher `id`; an unknown id changes nothing.
    pub fn unwatch(&mut self, id: u64) {
        self.watchers.retain(|(watcher, _, _)| *watcher != id);
    }
    // The table watchers registered.
    pub fn watcher_count(&self) -> usize {
        self.watchers.len()
    }
    fn notify(&mut self, mut changed: BTreeSet<String>) {
        self.last_bootstrap = strip_marks(&mut changed, BOOTSTRAP_MARK);
        self.watchers.retain(|(_, tables, sender)| {
            if tables.iter().any(|t| changed.contains(t)) {
                sender.send(()).is_ok()
            } else {
                true
            }
        });
        self.last_changed = changed;
    }
    // Bump the generation inside the open transaction; a stale writer fails here.
    fn fence(&mut self) -> Result<()> {
        let affected = self.store.execute(
            "UPDATE axton_store SET generation=generation+1 WHERE generation=?",
            &[Value::from(self.generation)],
        )?;
        if affected != 1 {
            return Err(invalid("stale client writer; reopen runtime"));
        }
        Ok(())
    }
    pub(crate) fn write<T>(
        &mut self,
        body: impl FnOnce(&mut Engine<'_, S>) -> Result<T>,
    ) -> Result<T> {
        if self.session.is_some() {
            return Err(invalid("client transaction active"));
        }
        self.store.begin()?;
        let mut changed = BTreeSet::new();
        let applied = body(&mut Engine::new(
            &mut self.store,
            &self.schema,
            &mut changed,
            false,
        ));
        match applied.and_then(|value| self.fence().map(|()| value)) {
            Ok(value) => {
                // A failed COMMIT leaves the transaction open; without this rollback
                // every later `begin` would fail. The commit error is what we report.
                if let Err(e) = self.store.commit() {
                    let _ = self.store.rollback();
                    return Err(e);
                }
                self.generation += 1;
                changed.insert("axton_store".into());
                self.notify(changed);
                Ok(value)
            }
            Err(e) => {
                self.store.rollback()?;
                Err(e)
            }
        }
    }
    pub(crate) fn view<T>(
        &mut self,
        body: impl FnOnce(&mut Engine<'_, S>) -> Result<T>,
    ) -> Result<T> {
        let mut changed = BTreeSet::new();
        body(&mut Engine::new(
            &mut self.store,
            &self.schema,
            &mut changed,
            true,
        ))
    }
    pub fn transaction<T>(
        &mut self,
        body: impl FnOnce(&mut ClientTransaction<'_, S>) -> Result<T>,
    ) -> Result<T> {
        self.write(|engine| {
            let mut submitted = BTreeSet::new();
            let mut tx = ClientTransaction {
                engine: Engine::new(
                    &mut *engine.store,
                    engine.schema,
                    &mut *engine.changed,
                    false,
                ),
                depth: 0,
                submitted: &mut submitted,
            };
            body(&mut tx)
        })
    }
    pub fn begin_session(&mut self) -> Result<()> {
        if self.session.is_some() {
            return Err(invalid("transaction already active"));
        }
        self.store.begin()?;
        self.session = Some(Session {
            changed: BTreeSet::new(),
            savepoints: vec![],
            counter: 0,
            submitted: BTreeSet::new(),
        });
        Ok(())
    }
    pub fn session<T>(
        &mut self,
        body: impl FnOnce(&mut ClientTransaction<'_, S>) -> Result<T>,
    ) -> Result<T> {
        let Self {
            store,
            schema,
            session,
            ..
        } = self;
        let session = session
            .as_mut()
            .ok_or_else(|| invalid("no active transaction"))?;
        let mut tx = ClientTransaction {
            engine: Engine::new(store, schema, &mut session.changed, false),
            depth: 0,
            submitted: &mut session.submitted,
        };
        body(&mut tx)
    }
    pub fn commit_session(&mut self) -> Result<()> {
        let session = self
            .session
            .take()
            .ok_or_else(|| invalid("no active transaction"))?;
        if !session.savepoints.is_empty() {
            let _ = self.physical_rollback();
            return Err(invalid("unclosed savepoint"));
        }
        if let Err(e) = self.fence() {
            let _ = self.physical_rollback();
            return Err(e);
        }
        if let Err(e) = self.store.commit() {
            let _ = self.physical_rollback();
            return Err(e);
        }
        self.generation += 1;

        let mut changed = session.changed;
        changed.insert("axton_store".into());
        self.notify(changed);
        Ok(())
    }
    pub fn rollback_session(&mut self) -> Result<()> {
        self.session
            .take()
            .ok_or_else(|| invalid("no active transaction"))?;
        self.physical_rollback()
    }
    fn physical_rollback(&mut self) -> Result<()> {
        self.store.rollback()
    }

    pub fn session_savepoint(&mut self) -> Result<()> {
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| invalid("no active transaction"))?;
        session.counter += 1;
        let name = format!("session_{}", session.counter);
        self.store.savepoint(&name)?;
        session.savepoints.push(SessionSavepoint {
            name,
            changed: session.changed.clone(),
            submitted: session.submitted.clone(),
        });
        Ok(())
    }
    pub fn session_release(&mut self) -> Result<()> {
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| invalid("no active transaction"))?;
        let savepoint = session
            .savepoints
            .pop()
            .ok_or_else(|| invalid("no savepoint"))?;
        self.store.release(&savepoint.name)
    }
    pub fn session_rollback_savepoint(&mut self) -> Result<()> {
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| invalid("no active transaction"))?;
        let savepoint = session
            .savepoints
            .pop()
            .ok_or_else(|| invalid("no savepoint"))?;
        // The scope's calls are discarded even when the physical rollback
        // fails: nothing may attach a companion to them afterwards.
        session.submitted = savepoint.submitted;
        self.store.rollback_to(&savepoint.name)?;
        session.changed = savepoint.changed;
        Ok(())
    }
    pub fn read(&mut self, key: &RecordKey) -> Result<Option<Value>> {
        let key = self.schema.record_key(&key.model, &key.identity)?;
        self.view(|e| e.read_row(&key))
    }
    pub fn query(&mut self, model: &str, filter: &Value) -> Result<Vec<Value>> {
        let filter: std::collections::BTreeMap<String, Value> =
            serde_json::from_value(filter.clone())?;
        self.view(|e| {
            query::evaluate(
                e,
                model,
                &QuerySpec {
                    filter,
                    ..Default::default()
                },
            )
        })
    }
    pub fn query_spec(&mut self, model: &str, spec: &QuerySpec) -> Result<Vec<Value>> {
        self.view(|e| query::evaluate(e, model, spec))
    }
    pub fn related(&mut self, key: &RecordKey, name: &str) -> Result<Option<Value>> {
        self.view(|e| query::related(e, key, name))
    }
    pub fn referencing(&mut self, key: &RecordKey, source: &str, name: &str) -> Result<Vec<Value>> {
        self.view(|e| query::referencing(e, key, source, name))
    }
    pub fn read_sql(&mut self, sql: &str, parameters: &[Value]) -> Result<Vec<Value>> {
        let rows = self.store.query_committed(sql, parameters)?;
        query::rows_to_objects(rows)
    }
    // The tables a watched read-only statement reads: SQLite's answer,
    // never the application's ([`ClientStore::read_tables`]).
    pub fn sql_tables(&mut self, sql: &str) -> Result<BTreeSet<String>> {
        self.store.read_tables(sql)
    }
    pub fn session_sql(&mut self, sql: &str, parameters: &[Value]) -> Result<Vec<Value>> {
        if self.session.is_none() {
            return Err(invalid("no active transaction"));
        }
        let rows = self.store.query(sql, parameters)?;
        query::rows_to_objects(rows)
    }
    pub fn pending_count(&mut self) -> Result<usize> {
        self.view(|e| {
            let count = e.scalar("SELECT count(*) FROM axton_mutation_queue WHERE reconciled=0 AND rejection_code IS NULL", &[])?
                .ok_or_else(|| invalid("queue count missing"))?;
            Ok(engine::as_u64(&count)? as usize)
        })
    }
    pub fn before_image_count(&mut self) -> Result<usize> {
        let tables: Vec<String> = self
            .schema
            .models
            .iter()
            .map(|m| ddl::before_table(&m.name))
            .collect();
        self.view(|e| {
            let mut total = 0;
            for table in &tables {
                total += e.count(table)? as usize;
            }
            Ok(total)
        })
    }
    // The record's stamp evidence: the last authoritative version this client
    // applied, retained across deletion and unsubscription; 0 when none.

    // The sequence of the last push a receipt completed.

    // The read contracts this client expects; see [`declared_models`].
    pub fn declared_models(&self) -> std::collections::BTreeMap<String, u64> {
        declared_models(&self.schema)
    }
    // Hook names belong to the requested schema. A pending rebuild may be
    // draining a replica whose stored schema differs from that request.

    // The prerequisite names the requested schema declares: the target
    // schema while an incompatible old replica drains.
    pub(crate) fn target_prerequisites(&self) -> BTreeSet<String> {
        self.schema
            .prerequisites
            .iter()
            .filter_map(|p| p["name"].as_str().map(str::to_string))
            .collect()
    }

    pub fn set_readiness(&mut self, key: &str, value: Readiness) -> Result<()> {
        self.write(|e| {
            match value {
                Readiness::Ready => e.resolve_prerequisite(key)?,
                Readiness::Failed => e.fail_prerequisite(key, "failed")?,
                Readiness::Pending => e.reset_prerequisite(key)?,
            };
            Ok(())
        })
    }
    pub fn pending_tasks(&mut self) -> Result<Vec<Value>> {
        self.view(|e| {
            Ok(e.prerequisite_keys()?
                .into_iter()
                .map(|(key, error)| task(&key, error.as_deref()))
                .collect())
        })
    }
    // What running a task came to: `None` resolves it, `Some(reason)` fails
    // it and keeps the reason for `pending_tasks` and `record_status`.
    pub fn outcome(&mut self, key: &str, error: Option<&str>) -> Result<()> {
        self.write(|e| {
            match error {
                None => e.resolve_prerequisite(key)?,
                Some(reason) => e.fail_prerequisite(key, reason)?,
            };
            Ok(())
        })
    }

    // Every retained refusal with the act as submitted, oldest first
    // ([#186](https://github.com/zanminwang/axton/issues/186)).

    // One retained refusal, or `None`.

    // The unsent acts blocked on a terminally failed task.
    pub fn failed_acts(&mut self) -> Result<Vec<FailedAct>> {
        self.view(|e| e.failed_acts())
    }
    // Make the tasks pending again, in one local transaction.
    pub fn retry_tasks(&mut self, keys: &[String]) -> Result<()> {
        self.write(|e| e.retry_tasks(keys))
    }
    // Remove unsent work and its optimism without recording a refusal for
    // it; lifecycle dependents are refused. Answers the removed Calls'
    // completions.

    pub fn record_status(&mut self, key: &RecordKey) -> Result<Value> {
        let key = self.schema.record_key(&key.model, &key.identity)?;
        self.view(|e| {
            let prerequisites: BTreeMap<String, Option<String>> =
                e.prerequisite_keys()?.into_iter().collect();
            let mut pending = vec![];
            for q in e.queued()? {
                let touches = q
                    .mutation
                    .operations
                    .iter()
                    .chain(&q.mutation.companion)
                    .chain(&q.mutation.effects)
                    .any(|op| op.model == key.model && op.identity == key.identity);
                if !touches {
                    continue;
                }
                let phase = match q.push {
                    None => "queued",
                    Some(_) => "frozen",
                };
                let prerequisites: Vec<Value> = q
                    .mutation
                    .prerequisites
                    .iter()
                    .map(|k| {
                        match prerequisites.get(k) {
                            None => json!({"key":k,"state":"ready"}),
                            Some(Some(error)) => json!({"key":k,"state":"failed","error":error}),
                            Some(None) => json!({"key":k,"state":"pending"}),
                        }
                    })
                    .collect();
                pending.push(json!({"ordinal":q.ordinal,"name":q.mutation.name,"phase":phase,"diverged":q.diverged,"prerequisites":prerequisites}));
            }
            let rejections: Vec<Value> = e
                .rejection_details()?
                .into_iter()
                .filter(|d| {
                    d["records"].as_array().is_some_and(|r| {
                        r.iter()
                            .any(|x| x["model"] == key.model && x["identity"] == key.identity)
                    })
                })
                .collect();
            Ok(json!({"pending":pending,"rejections":rejections}))
        })
    }
}

pub struct ClientTransaction<'a, S: ClientStore> {
    pub(crate) engine: Engine<'a, S>,
    depth: u64,
    // Ordinals of the calls [`Self::submit_mutation`] queued in this
    // transaction and not rolled back: the only calls that take companions.
    submitted: &'a mut BTreeSet<u64>,
    // A store hook's transaction: local reads and writes only.
}
impl<S: ClientStore> ClientTransaction<'_, S> {
    pub fn read(&mut self, key: &RecordKey) -> Result<Option<Value>> {
        let key = self.engine.schema.record_key(&key.model, &key.identity)?;
        self.engine.read_row(&key)
    }
    pub fn savepoint<T>(&mut self, body: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        self.depth += 1;
        let name = format!("tx_{}", self.depth);
        self.engine.store.savepoint(&name)?;
        let submitted = self.submitted.clone();
        let result = body(self);
        self.depth -= 1;
        match result {
            Ok(v) => {
                self.engine.store.release(&name)?;
                Ok(v)
            }
            Err(e) => {
                // Calls made in the discarded scope are gone with it, even
                // when the physical rollback fails and fails the rest.
                *self.submitted = submitted;
                self.engine.store.rollback_to(&name)?;
                Err(e)
            }
        }
    }

    // Submit a named Mutation as part of this transaction. Its args are
    // made canonical once (generated values filled, normalized, bindings
    // and store policy validated) and its call ID, args and inferred
    // optimism are written here, so they commit or roll back with the rest
    // of the transaction; nothing is sendable before commit. Queries are
    // refused: only the standalone entry queues them.

    pub(crate) fn preview_callback(&mut self, operation: Operation) -> Result<Vec<Operation>> {
        self.savepoint(|tx| tx.engine.preview_callback(operation))
    }

    // Record `operation` as a local companion of call `ordinal`: applied
    // now, stored with the call and settled with its outcome, never sent.
    // The call must be the latest this transaction submitted, and no
    // independent write may have followed it, so the companion keeps its
    // place in local order. Reserved for the runtime's companion
    // capability; it is not an application API.
    #[doc(hidden)]
    pub fn append_companion(&mut self, ordinal: u64, operation: Operation) -> Result<()> {
        if !self.submitted.contains(&ordinal) {
            return Err(invalid(
                "a companion belongs to a Mutation submitted in this transaction",
            ));
        }
        self.savepoint(|tx| tx.engine.append_companion(ordinal, operation))
    }
    pub fn query(&mut self, model: &str, filter: &Value) -> Result<Vec<Value>> {
        let filter: BTreeMap<String, Value> = serde_json::from_value(filter.clone())?;
        query::evaluate(
            &mut self.engine,
            model,
            &QuerySpec {
                filter,
                ..Default::default()
            },
        )
    }
    pub fn query_spec(&mut self, model: &str, spec: &QuerySpec) -> Result<Vec<Value>> {
        query::evaluate(&mut self.engine, model, spec)
    }
    pub fn related(&mut self, key: &RecordKey, name: &str) -> Result<Option<Value>> {
        query::related(&mut self.engine, key, name)
    }
    pub fn referencing(&mut self, key: &RecordKey, source: &str, name: &str) -> Result<Vec<Value>> {
        query::referencing(&mut self.engine, key, source, name)
    }
    pub fn direct(&mut self, operation: Operation) -> Result<()> {
        self.savepoint(|tx| tx.engine.direct(operation))
    }
    // Dismiss a retained refusal as part of this transaction
    // ([#205](https://github.com/zanminwang/axton/issues/205)).

    // Make the tasks pending again as part of this transaction; nothing runs
    // them before it commits.
    pub fn retry_tasks(&mut self, keys: &[String]) -> Result<()> {
        self.resolution()?;
        self.savepoint(|tx| tx.engine.retry_tasks(keys))
    }
    // Discard unsent act `ordinal` as part of this transaction: its
    // optimism is gone for every later read and submission here, and a
    // later submission is neither planned over it nor sequenced after it.
    // Answers the removed Calls' completions, final only once the
    // transaction commits.

    // A store hook's transaction resolves no unsent work.
    fn resolution(&self) -> Result<()> {
        Ok(())
    }
}

// A task as the host sees it: the fields of a schema-derived key (a canonical
// JSON invocation; an opaque key carries none), its key, its state and, when
// failed, the reason.
fn task(key: &str, error: Option<&str>) -> Value {
    let mut value = serde_json::from_str::<Value>(key)
        .ok()
        .filter(Value::is_object)
        .unwrap_or_else(|| json!({}));
    value["key"] = json!(key);
    match error {
        None => value["state"] = json!("pending"),
        Some(reason) => {
            value["state"] = json!("failed");
            value["error"] = json!(reason);
        }
    }
    value
}
