//! Client engine over per-model SQLite tables. No state lives in memory between calls.
pub mod actions;
pub mod authority;
pub mod bootstrap;
mod bootstrap_ledger;
pub mod connection;
pub mod ddl;
mod defaults;
mod downlink;
pub mod downlink_worker;
pub mod engine;
pub mod ledger;
pub mod live;
mod mutate;
mod policies;
mod push;
pub mod query;
pub mod query_cache;
pub mod queue;
pub mod rows;
pub mod runtime;
pub mod schema_store;
pub mod store;
pub mod subscriptions;
pub mod transport;

pub use actions::{ActionCallOptions, SubmittedCall};
pub use axton_core::*;
pub use bootstrap::{
    BootstrapApply, BootstrapError, BootstrapPhase, BootstrapRecordFailure, BootstrapState,
    BootstrapTask, PROTOCOL_INVALID, RECORDS_FAILED, REQUEST_REJECTED, SUBSCRIPTION_CLOSED,
};
pub use connection::*;
pub use downlink_worker::*;
pub use live::*;
pub use query::{Direction, QueryOrder, QuerySpec};
pub use query_cache::{QueryCacheEntry, QueryCacheKey, QueryOnce, QueryOnceOptions};
pub use store::*;
pub use subscriptions::{Initialization, SubscriptionState};
pub use transport::*;

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
    /// The Action call's store policy; only meaningful with `call_id`.
    #[serde(default, skip_serializing_if = "ActionStore::is_all")]
    pub store: ActionStore,
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
/// The read contracts a client of `schema` expects: every model with the
/// version its generated types read. Declared on every push, pull and
/// subscribe so receipts, HTTP catch-up and the live stream are served alike
/// ([#91](https://github.com/zanminwang/axton/issues/91)).
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
            store: ActionStore::All,
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
/// Why one record or one queued mutation could not be applied as delivered.
/// Every kind leaves the client consistent; the report is for the application
/// ([#51](https://github.com/zanminwang/axton/issues/51),
/// [#122](https://github.com/zanminwang/axton/issues/122)).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ReportKind {
    /// The server could not read the record: the change carried `error`
    /// instead of a state. Local content and stamp are kept.
    ReadFailed,
    /// The delivered state does not fit this client's schema. Nothing written.
    Skipped,
    /// The same stamp with different content. Nothing written.
    Conflict,
    /// A queued operation no longer replays over the new base: the base is
    /// visible and the mutation is still sent (`ordinal`).
    Diverged,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Report {
    pub kind: ReportKind,
    pub model: String,
    pub identity: Value,
    pub stamp: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ordinal: Option<u64>,
    #[serde(default)]
    pub detail: Value,
}
impl Report {
    pub(crate) fn new(kind: ReportKind, model: &str, identity: &Value, stamp: u64) -> Self {
        Self {
            kind,
            model: model.to_string(),
            identity: identity.clone(),
            stamp,
            code: None,
            ordinal: None,
            detail: Value::Null,
        }
    }
}
/// What applying a receipt or a page came to. `cursors` are the channel
/// cursors the page moved, at their new values.
#[derive(Debug, Default, Serialize)]
pub struct ApplyReport {
    pub applied: usize,
    pub stale: bool,
    pub cursors: BTreeMap<String, u64>,
    pub reports: Vec<Report>,
    /// Invocation outcomes are emitted after settlement and never stored locally.
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

/// A transaction the host holds open across calls, with its own savepoint stack.
struct Session {
    changed: BTreeSet<String>,
    savepoints: Vec<String>,
    counter: u64,
}

pub struct Client<S: ClientStore> {
    store: S,
    schema: Schema,
    client_id: String,
    generation: u64,
    watchers: Vec<(BTreeSet<String>, Sender<()>)>,
    session: Option<Session>,
    last_changed: BTreeSet<String>,
    last_bootstrap: BTreeSet<String>,
    pulls: PullLedger,
    schema_state: SchemaState,
    origin: Option<Origin<S>>,
    /// Fingerprint of `schema` partitioning saved Query results.
    query_contract: String,
    /// Active Query once requests of this runtime; memory-only.
    query_flights: query_cache::QueryFlights,
}

/// Where a client opened through [`Client::open_at`] came from: the path the
/// application named, how to open a store at a file, and the schema it asked
/// for (which may differ from the one in use while an old file is pending).
struct Origin<S> {
    path: std::path::PathBuf,
    factory: StoreFactory<S>,
    target: Schema,
}

/// Opens a store at a file; how [`Client::open_at`] reaches storage.
pub type StoreFactory<S> = Box<dyn Fn(&std::path::Path) -> Result<S> + Send + Sync>;

/// What the schema check found at open ([Reconciliation](../../docs/engineering/architecture/client/storage/reconciliation.md)).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SchemaState {
    /// This open created a fresh file beside an incompatible one.
    pub rebuilt: bool,
    /// The incompatible file is still in use because it holds unsent work.
    pub pending: Option<PendingRebuild>,
    /// What the last rebuild left behind.
    pub last_rebuild: Option<RebuildReport>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingRebuild {
    pub old_file: String,
    pub reason: String,
    pub pending: usize,
    pub direct: usize,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RebuildReport {
    pub old_file: String,
    pub new_file: String,
    pub reason: String,
    pub left_pending: usize,
    pub left_direct: usize,
    /// Live observers can terminate calls left in the prior file. A frozen
    /// call may have executed remotely; its outcome is unknown.
    pub abandoned_calls: Vec<AbandonedCall>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AbandonedCall {
    pub call_id: String,
    pub frozen: bool,
}

/// Marker a transaction leaves in its changed set when it subscribes or
/// unsubscribes a channel; stripped before the set reaches watchers.
pub(crate) const SUBSCRIPTION_MARK: &str = "axton_subscription:";
/// Marker a transaction leaves in its changed set when it changes a Scope's
/// bootstrap state ([#151](https://github.com/zanminwang/axton/issues/151)).
/// A load request changes no membership, so - unlike [`SUBSCRIPTION_MARK`] - it
/// bumps neither the subscription generation nor a channel epoch: registering a
/// load must not make the open live session stale or a pull in flight. It is
/// stripped like the other mark, and the Scopes it named are read back through
/// [`Client::last_bootstrap_scopes`].
pub(crate) const BOOTSTRAP_MARK: &str = "axton_bootstrap:";

/// Take every mark with `prefix` out of `changed` and answer with the names
/// they carried. A mark is a signal about a transaction, never a table, so it
/// never reaches a watcher or a host's changed-table list.
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

/// In-memory memory of the pulls this client issued and of how many times each
/// channel's subscription changed since open. A page whose request predates the
/// current subscription of any channel it names is stale, not a gap: the
/// resubscribe reset the cursor, and the next pull from that cursor delivers
/// everything. Nothing here is durable; a process restart cannot have a
/// request in flight.
#[derive(Default)]
struct PullLedger {
    epochs: BTreeMap<String, u64>,
    issued: std::collections::VecDeque<IssuedPull>,
    /// Incremented by every committed subscribe or unsubscribe; the live
    /// session compares it with the value it started under.
    generation: u64,
}
/// One request: the cursor it asked from on every channel, and the epoch each
/// channel's subscription was at.
struct IssuedPull {
    cursors: BTreeMap<String, u64>,
    epochs: BTreeMap<String, u64>,
}
impl PullLedger {
    const CAPACITY: usize = 1024;
    fn epoch(&self, channel: &str) -> u64 {
        self.epochs.get(channel).copied().unwrap_or(0)
    }
    /// Apply the subscription changes a committed transaction recorded.
    fn absorb(&mut self, changed: &mut BTreeSet<String>) {
        for channel in strip_marks(changed, SUBSCRIPTION_MARK) {
            *self.epochs.entry(channel).or_insert(0) += 1;
            self.generation += 1;
        }
    }
    fn issue(&mut self, cursors: &BTreeMap<String, u64>) {
        if self.issued.len() == Self::CAPACITY {
            self.issued.pop_front();
        }
        let epochs = cursors.keys().map(|c| (c.clone(), self.epoch(c))).collect();
        self.issued.push_back(IssuedPull {
            cursors: cursors.clone(),
            epochs,
        });
    }
    fn current_epochs(&self, cursors: &BTreeMap<String, u64>) -> BTreeMap<String, u64> {
        cursors.keys().map(|c| (c.clone(), self.epoch(c))).collect()
    }
    /// Whether the page answering a request from `cursors` was requested under
    /// an earlier subscription of one of its channels. Consumes the matching
    /// request. A page this client never requested is not judged here.
    fn stale(&mut self, cursors: &BTreeMap<String, u64>) -> bool {
        let current = self.current_epochs(cursors);
        let matches = |p: &IssuedPull| p.cursors == *cursors;
        if let Some(i) = self
            .issued
            .iter()
            .position(|p| matches(p) && p.epochs == current)
        {
            self.issued.remove(i);
            // The wire identifies requests only by channels and cursors. If old
            // and current subscriptions issued the same request, this response
            // could belong to either one. Let every indistinguishable answer use
            // the cursor gate; otherwise the fresh answer can be dropped as stale
            // when the old answer arrives first. Retain the entries so another
            // subscription change can still make the outstanding answers stale.
            for pull in self.issued.iter_mut().filter(|p| matches(p)) {
                pull.epochs = current.clone();
            }
            return false;
        }
        if let Some(i) = self.issued.iter().position(matches) {
            self.issued.remove(i);
            return true;
        }
        false
    }
}

impl<S: ClientStore> Client<S> {
    /// Open `store` for `schema`. An earlier framework layout is refused: file
    /// selection and rebuilding belong to [`Client::open_at`]. The schema the
    /// store is built for is recorded (or replaced) once reconciliation succeeds.
    pub fn open(mut store: S, schema: Schema) -> Result<Self> {
        schema.validate()?;
        if let ddl::Layout::Legacy(what) = ddl::check_layout(&mut store)? {
            return Err(invalid(format!(
                "this database was created by an earlier AXTON runtime ({what}); open it through a path so it can be rebuilt beside"
            )));
        }
        store.execute_batch(ddl::FRAMEWORK_DDL)?;
        ddl::add_framework_columns(&mut store)?;
        let query_contract = query_cache::contract_fingerprint(&schema)?;
        store.begin()?;
        let opened = (|| {
            ddl::reconcile(&mut store, &schema)?;
            schema_store::write_descriptor(&mut store, &schema)?;
            query_cache::prune(&mut store, &query_contract)?;
            let row = store.query("SELECT client_id, generation FROM axton_client", &[])?;
            let (client_id, generation) = match row.rows.first() {
                Some(r) => (
                    r[0].as_str().unwrap_or("").to_string(),
                    engine::as_u64(&r[1])?,
                ),
                None => {
                    let id = uuid::Uuid::new_v4().to_string();
                    store.execute("INSERT INTO axton_client (client_id, next_ordinal, next_push, generation, next_subscription) VALUES (?,1,1,1,1)", &[Value::from(id.clone())])?;
                    (id, 1)
                }
            };
            Ok::<_, Error>((client_id, generation))
        })();
        let (client_id, generation) = match opened {
            Ok(v) => v,
            Err(e) => {
                store.rollback()?;
                return Err(e);
            }
        };
        store.commit()?;
        // Reconciliation may have altered tables on the writing connection;
        // a committed read makes every connection load the new schema before
        // the first statement is prepared against it.
        for model in &schema.models {
            store.query_committed(
                &format!("SELECT 1 FROM {} LIMIT 0", ddl::quote(&model.name)),
                &[],
            )?;
        }
        Ok(Self {
            store,
            schema,
            client_id,
            generation,
            watchers: vec![],
            session: None,
            last_changed: BTreeSet::new(),
            last_bootstrap: BTreeSet::new(),
            pulls: PullLedger::default(),
            schema_state: SchemaState::default(),
            origin: None,
            query_contract,
            query_flights: Default::default(),
        })
    }
    /// Open the database the application names by `path`, choosing the file
    /// through the sidecar and the schema check: identical or compatible →
    /// the current file; incompatible or an earlier layout → a fresh file
    /// beside it, unless the old file holds unsent work and `discard_pending`
    /// is false, in which case the old file opens with its own schema so the
    /// work can be sent first ([`SchemaState::pending`]).
    pub fn open_at(
        path: impl AsRef<std::path::Path>,
        schema: Schema,
        factory: StoreFactory<S>,
        discard_pending: bool,
    ) -> Result<Self>
    where
        S: 'static,
    {
        schema.validate()?;
        let path = path.as_ref().to_path_buf();
        let file = schema_store::current_file(&path);
        let mut store = factory(&file)?;
        let mut client = match ddl::check_layout(&mut store)? {
            ddl::Layout::Fresh => Self::open(store, schema.clone())?,
            ddl::Layout::Legacy(what) => {
                let pending = count_rows(&mut store, "axton_mutation").unwrap_or(0);
                drop(store);
                Self::rebuild_beside(&path, &factory, &file, &schema, &what, pending, 0)?
            }
            ddl::Layout::Current => {
                store.execute_batch(ddl::FRAMEWORK_DDL)?;
                match schema_store::read_descriptor(&mut store)? {
                    // A database from before descriptors were stored: only its
                    // tables can say whether it fits. Any other open failure is
                    // an error, never a reason to switch files.
                    None => match ddl::incompatibility(&mut store, &schema)? {
                        None => Self::open(store, schema.clone())?,
                        Some(reason) => {
                            let pending = count_rows(&mut store, "axton_mutation")?;
                            drop(store);
                            Self::rebuild_beside(
                                &path, &factory, &file, &schema, &reason, pending, 0,
                            )?
                        }
                    },
                    Some(stored) => match Schema::compatibility(&stored, &schema) {
                        Compatibility::Identical | Compatibility::Additive(_) => {
                            Self::open(store, schema.clone())?
                        }
                        Compatibility::Incompatible(reason) => {
                            let pending = count_rows(&mut store, "axton_mutation")?;
                            let direct = count_direct(&mut store, &stored)?;
                            if pending > 0 && !discard_pending {
                                let mut client = Self::open(store, stored)?;
                                client.schema_state.pending = Some(PendingRebuild {
                                    old_file: file.to_string_lossy().into_owned(),
                                    reason,
                                    pending,
                                    direct,
                                });
                                client
                            } else {
                                drop(store);
                                Self::rebuild_beside(
                                    &path, &factory, &file, &schema, &reason, pending, direct,
                                )?
                            }
                        }
                    },
                }
            }
        };
        client.origin = Some(Origin {
            path,
            factory,
            target: schema,
        });
        Ok(client)
    }
    /// Create `<path>.<n>`, initialise it for `schema`, carry the old file's
    /// subscribed Scopes over as fresh uninitialized subscriptions, and point
    /// the sidecar at it. The replacement resets every delivery boundary and
    /// allocates identities above the replaced file's counter, so no handle,
    /// acknowledgement or request of the old replica matches one of them.
    /// Files are numbered upward: a numbered file above the one in use was
    /// never pointed at (an interrupted rebuild) and is removed; the file in
    /// use and every earlier generation are kept.
    fn rebuild_beside(
        path: &std::path::Path,
        factory: &dyn Fn(&std::path::Path) -> Result<S>,
        old_file: &std::path::Path,
        schema: &Schema,
        reason: &str,
        left_pending: usize,
        left_direct: usize,
    ) -> Result<Self> {
        let in_use = schema_store::file_number(path, old_file);
        for stray in schema_store::numbered_files(path) {
            if schema_store::file_number(path, &stray) > in_use {
                schema_store::remove_database_files(&stray);
            }
        }
        let new_file = schema_store::next_free_file(path);
        let mut old = factory(old_file)?;
        let channels: Vec<String> = old
            .query_committed(
                "SELECT channel FROM axton_subscription ORDER BY channel",
                &[],
            )
            .map(|rows| {
                rows.rows
                    .iter()
                    .filter_map(|r| r[0].as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default();
        // Absent in an earlier layout, which allocated no subscription
        // identities: this replica starts its own at one.
        let next_subscription = old
            .query_committed("SELECT next_subscription FROM axton_client", &[])
            .ok()
            .and_then(|rows| rows.rows.first().and_then(|r| r[0].as_u64()));
        let columns = old.query_committed("PRAGMA table_info(axton_mutation)", &[])?;
        let has = |name: &str| columns.rows.iter().any(|r| r[1].as_str() == Some(name));
        let abandoned_calls = if has("call_id") && has("push") {
            old.query_committed("SELECT call_id, push FROM axton_mutation WHERE call_id IS NOT NULL ORDER BY ordinal", &[])?
                .rows.iter().map(|r| Ok(AbandonedCall {
                    call_id: r[0].as_str().ok_or_else(|| invalid("stored Action call ID is not text"))?.to_owned(),
                    frozen: !r[1].is_null(),
                })).collect::<Result<Vec<_>>>()?
        } else {
            // An older framework table predates Action identity columns.
            vec![]
        };
        let mut client = Self::open(factory(&new_file)?, schema.clone())?;
        if !channels.is_empty() || next_subscription.is_some() {
            client.write(|e| {
                if let Some(next) = next_subscription {
                    e.carry_subscription_allocator(next)?;
                }
                for channel in &channels {
                    e.ensure_subscription(channel)?;
                }
                Ok(())
            })?;
        }
        schema_store::set_current_file(path, &new_file)?;
        client.schema_state.rebuilt = true;
        client.schema_state.last_rebuild = Some(RebuildReport {
            old_file: old_file.to_string_lossy().into_owned(),
            new_file: new_file.to_string_lossy().into_owned(),
            reason: reason.to_string(),
            left_pending,
            left_direct,
            abandoned_calls,
        });
        Ok(client)
    }
    /// The schema check's outcome for this client.
    pub fn schema_state(&self) -> &SchemaState {
        &self.schema_state
    }
    /// Rebuild now: leave the incompatible file behind and open a fresh one
    /// for the schema the application asked for. Refused while unsent work
    /// remains unless `discard_pending`; the report says what was left.
    pub fn rebuild(&mut self, discard_pending: bool) -> Result<RebuildReport>
    where
        S: 'static,
    {
        let Some(pending) = self.schema_state.pending.clone() else {
            return Err(invalid("no rebuild is pending"));
        };
        if self.session.is_some() {
            return Err(invalid("client transaction active"));
        }
        let remaining = self.pending_count()?;
        if remaining > 0 && !discard_pending {
            return Err(invalid(format!(
                "{remaining} unsent mutations remain in {}; send them or rebuild with discardPending",
                pending.old_file
            )));
        }
        let origin = self
            .origin
            .take()
            .ok_or_else(|| invalid("client was not opened through a path"))?;
        let mut fresh = Self::open_at(&origin.path, origin.target.clone(), origin.factory, true)?;
        let report = fresh
            .schema_state
            .last_rebuild
            .clone()
            .ok_or_else(|| invalid("rebuild produced no report"))?;
        fresh.watchers = std::mem::take(&mut self.watchers);
        *self = fresh;
        let tables: BTreeSet<String> = self.schema.models.iter().map(|m| m.name.clone()).collect();
        self.notify(tables);
        Ok(report)
    }
    pub fn client_id(&self) -> &str {
        &self.client_id
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn last_changed(&self) -> &BTreeSet<String> {
        &self.last_changed
    }
    /// The Scopes whose bootstrap state the last committed transaction changed.
    /// The scheduler reads its work from [`Client::bootstrap_tasks`]; this says
    /// whether a commit touched any of it at all.
    pub fn last_bootstrap_scopes(&self) -> &BTreeSet<String> {
        &self.last_bootstrap
    }
    pub fn session_active(&self) -> bool {
        self.session.is_some()
    }
    pub fn watch(&mut self, tables: BTreeSet<String>) -> Receiver<()> {
        let (tx, rx) = mpsc::channel();
        self.watchers.push((tables, tx));
        rx
    }
    fn notify(&mut self, mut changed: BTreeSet<String>) {
        self.last_bootstrap = strip_marks(&mut changed, BOOTSTRAP_MARK);
        self.pulls.absorb(&mut changed);
        self.watchers.retain(|(tables, sender)| {
            if tables.iter().any(|t| changed.contains(t)) {
                sender.send(()).is_ok()
            } else {
                true
            }
        });
        self.last_changed = changed;
    }
    /// Bump the generation inside the open transaction; a stale writer fails here.
    fn fence(&mut self) -> Result<()> {
        let affected = self.store.execute(
            "UPDATE axton_client SET generation = generation + 1 WHERE generation = ?",
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
                changed.insert("axton_client".into());
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
            let mut tx = ClientTransaction {
                engine: Engine::new(
                    &mut *engine.store,
                    engine.schema,
                    &mut *engine.changed,
                    false,
                ),
                depth: 0,
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
        };
        body(&mut tx)
    }
    pub fn commit_session(&mut self) -> Result<()> {
        let session = self
            .session
            .take()
            .ok_or_else(|| invalid("no active transaction"))?;
        if !session.savepoints.is_empty() {
            self.store.rollback()?;
            return Err(invalid("unclosed savepoint"));
        }
        if let Err(e) = self.fence() {
            self.store.rollback()?;
            return Err(e);
        }
        // The session is already taken; a failed COMMIT must also close the
        // transaction, or every later `begin` would fail. Report the commit error.
        if let Err(e) = self.store.commit() {
            let _ = self.store.rollback();
            return Err(e);
        }
        self.generation += 1;
        let mut changed = session.changed;
        changed.insert("axton_client".into());
        self.notify(changed);
        Ok(())
    }
    pub fn rollback_session(&mut self) -> Result<()> {
        self.session
            .take()
            .ok_or_else(|| invalid("no active transaction"))?;
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
        session.savepoints.push(name);
        Ok(())
    }
    pub fn session_release(&mut self) -> Result<()> {
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| invalid("no active transaction"))?;
        let name = session
            .savepoints
            .pop()
            .ok_or_else(|| invalid("no savepoint"))?;
        self.store.release(&name)
    }
    pub fn session_rollback_savepoint(&mut self) -> Result<()> {
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| invalid("no active transaction"))?;
        let name = session
            .savepoints
            .pop()
            .ok_or_else(|| invalid("no savepoint"))?;
        self.store.rollback_to(&name)
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
    pub fn session_sql(&mut self, sql: &str, parameters: &[Value]) -> Result<Vec<Value>> {
        if self.session.is_none() {
            return Err(invalid("no active transaction"));
        }
        let rows = self.store.query(sql, parameters)?;
        query::rows_to_objects(rows)
    }
    pub fn pending_count(&mut self) -> Result<usize> {
        self.view(|e| Ok(e.count("axton_mutation")? as usize))
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
    /// The record's stamp evidence: the last authoritative version this client
    /// applied, retained across deletion and unsubscription; 0 when none.
    pub fn record_stamp(&mut self, key: &RecordKey) -> Result<u64> {
        let key = self.schema.record_key(&key.model, &key.identity)?;
        self.view(|e| e.record_stamp(&key))
    }
    /// The sequence of the last push a receipt completed.
    pub fn last_completed_push(&mut self) -> Result<u64> {
        self.view(|e| e.last_completed_push())
    }
    /// The read contracts this client expects; see [`declared_models`].
    pub fn declared_models(&self) -> std::collections::BTreeMap<String, u64> {
        declared_models(&self.schema)
    }
    pub fn subscription_generation(&self) -> u64 {
        self.pulls.generation
    }
    pub fn drop_mutation(&mut self, ordinal: u64) -> Result<()> {
        self.write(|e| {
            match e.queued_one(ordinal)? {
                None => return Ok(()),
                Some(q) if q.push.is_some() => {
                    return Err(invalid(
                        "cannot drop a sent mutation with unknown/accepted outcome",
                    ));
                }
                Some(_) => {}
            }
            let (affected, completions) = e.mark_rejected_with_completions(&[Rejection {
                ordinal,
                code: "dropped".into(),
            }])?;
            if !completions.is_empty() {
                return Err(invalid(
                    "Action discard requires completion-returning drop_action",
                ));
            }
            e.rebuild_held(&affected)?;
            Ok(())
        })
    }
    /// Explicitly discard unsent work and return terminal events for any
    /// Action calls removed through its lifecycle dependency chain.
    pub fn drop_action(&mut self, ordinal: u64) -> Result<Vec<CallCompletion>> {
        self.write(|e| {
            match e.queued_one(ordinal)? {
                None => return Ok(vec![]),
                Some(q) if q.push.is_some() => {
                    return Err(invalid(
                        "cannot drop a sent mutation with unknown/accepted outcome",
                    ));
                }
                Some(_) => {}
            }
            let (affected, completions) = e.mark_rejected_with_completions(&[Rejection {
                ordinal,
                code: "dropped".into(),
            }])?;
            e.rebuild_held(&affected)?;
            Ok(completions)
        })
    }
    pub fn freeze(&mut self) -> Result<Option<Vec<u8>>> {
        self.freeze_with_limit(limits::PUSH_BYTES)
    }
    pub fn freeze_with_limit(&mut self, max_bytes: usize) -> Result<Option<Vec<u8>>> {
        self.write(|e| e.freeze(max_bytes))
    }
    /// Complete the push in flight from its receipt: the returned authority
    /// lands, the completed operations leave the queue and what remains
    /// replays, in one transaction. Nothing waits for a channel.
    pub fn acknowledge(&mut self, sequence: u64, receipt: PushReceipt) -> Result<ApplyReport> {
        self.write(|e| e.acknowledge(sequence, &receipt))
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
    /// The next task the host can run, given the names it has handlers for.
    /// A pending task no handler covers fails with that reason and the walk
    /// goes on, so the host only calls handlers; which task, whether one is
    /// runnable and when the run ends are decided here.
    pub fn next_task(&mut self, handlers: &[String]) -> Result<Option<Value>> {
        self.write(|e| {
            for (key, error) in e.prerequisite_keys()? {
                if error.is_some() {
                    continue;
                }
                let task = task(&key, None);
                let handled = task["name"]
                    .as_str()
                    .is_some_and(|name| handlers.iter().any(|h| h == name));
                if handled {
                    return Ok(Some(task));
                }
                e.fail_prerequisite(&key, "missing prerequisite handler")?;
            }
            Ok(None)
        })
    }
    /// What running a task came to: `None` resolves it, `Some(reason)` fails
    /// it and keeps the reason for `pending_tasks` and `record_status`.
    pub fn outcome(&mut self, key: &str, error: Option<&str>) -> Result<()> {
        self.write(|e| {
            match error {
                None => e.resolve_prerequisite(key)?,
                Some(reason) => e.fail_prerequisite(key, reason)?,
            };
            Ok(())
        })
    }
    pub fn dismiss_rejection(&mut self, ordinal: u64) -> Result<()> {
        self.write(|e| e.delete_rejection(ordinal))
    }
    pub fn rejections(&mut self) -> Result<Vec<Rejection>> {
        self.view(|e| e.rejections())
    }
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

fn count_rows<S: ClientStore>(store: &mut S, table: &str) -> Result<usize> {
    let rows = store.query_committed(&format!("SELECT COUNT(*) FROM {table}"), &[])?;
    rows.rows
        .first()
        .and_then(|r| r[0].as_u64())
        .map(|n| n as usize)
        .ok_or_else(|| invalid("count failed"))
}

/// Rows the server never confirmed: visible rows without stamp evidence.
/// They exist only in this file and are not carried into a rebuilt one.
fn count_direct<S: ClientStore>(store: &mut S, schema: &Schema) -> Result<usize> {
    let mut total = 0;
    for model in &schema.models {
        let mut keys = model.identity.clone();
        keys.sort();
        let pairs: Vec<String> = keys
            .iter()
            .map(|k| format!("'{}', m.{}", k.replace('\'', "''"), ddl::quote(k)))
            .collect();
        // A row with no stamp and no pending operation reached this file only
        // through a direct write: nothing will ever send it.
        let identity = format!("json_object({})", pairs.join(", "));
        let sql = format!(
            "SELECT COUNT(*) FROM {} m WHERE NOT EXISTS (SELECT 1 FROM axton_record r WHERE r.model = ? AND r.identity = {identity}) AND NOT EXISTS (SELECT 1 FROM axton_mutation_operation o WHERE o.model = ? AND o.identity = {identity})",
            ddl::quote(&model.name),
        );
        let rows = store.query_committed(&sql, &[json!(model.name), json!(model.name)])?;
        total += rows.rows.first().and_then(|r| r[0].as_u64()).unwrap_or(0) as usize;
    }
    Ok(total)
}

pub struct ClientTransaction<'a, S: ClientStore> {
    pub(crate) engine: Engine<'a, S>,
    depth: u64,
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
        let result = body(self);
        self.depth -= 1;
        match result {
            Ok(v) => {
                self.engine.store.release(&name)?;
                Ok(v)
            }
            Err(e) => {
                self.engine.store.rollback_to(&name)?;
                Err(e)
            }
        }
    }
    /// Subscribe or unsubscribe `channel` in this transaction. Subscribing
    /// registers durable intent with no delivery position; subscribing an
    /// already subscribed channel is not a membership change: it touches no
    /// cursor and leaves the subscription generation alone.
    pub fn set_channel(&mut self, channel: String, subscribed: bool) -> Result<()> {
        if subscribed {
            // Registration is intent only: the first delivery boundary is the
            // head the Downlink worker's next handshake acknowledges, not zero
            // ([#150](https://github.com/zanminwang/axton/issues/150)).
            let (_, created) = self.engine.ensure_subscription(&channel)?;
            if created {
                self.engine.mark_subscription(&channel);
            }
            Ok(())
        } else {
            if self.engine.unsubscribe(&channel)? {
                self.engine.mark_subscription(&channel);
            }
            Ok(())
        }
    }
    pub fn enqueue(&mut self, mutation: Mutation) -> Result<u64> {
        self.savepoint(|tx| tx.engine.enqueue(mutation))
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
}

/// A task as the host sees it: the fields of a schema-derived key (a canonical
/// JSON invocation; an opaque key carries none), its key, its state and, when
/// failed, the reason.
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
