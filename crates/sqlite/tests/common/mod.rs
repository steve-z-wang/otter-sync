#![allow(dead_code)]
//! Helpers shared by every client-facing integration test in this crate.
mod lane;
#[allow(unused_imports)]
pub use lane::*;

use axton_client::*;
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};
use std::collections::BTreeMap;

pub fn schema() -> Schema {
    Schema::from_value(
        serde_json::from_str(include_str!("../../../../fixtures/schemas/entry.json")).unwrap(),
    )
    .unwrap()
}
pub fn open(path: &std::path::Path) -> Client<SqliteStore> {
    Client::open(SqliteStore::open(path).unwrap(), schema()).unwrap()
}
pub fn key() -> RecordKey {
    schema().record_key("Entry", &json!({"id":"e"})).unwrap()
}
pub fn update(text: &str) -> Operation {
    Operation {
        model: "Entry".into(),
        op: OperationKind::Update,
        identity: json!({"id":"e"}),
        values: Some(json!({ "text": text })),
    }
}
pub fn mutation(text: &str) -> Mutation {
    Mutation::new("Edit", vec![update(text)])
}
/// A one-scope page moving `scope` from `from` to `to` (its head) with
/// `Entry e` at stamp `to`.
pub fn page(scope: &str, from: u64, to: u64, text: Option<&str>) -> PullPage {
    PullPage {
        cursors: BTreeMap::from([(scope.to_string(), CursorRange { from, to, head: to })]),
        changes: vec![authority(text, to)],
    }
}
/// A page for several scopes at once, each `(scope, from, to, head)`, with `changes`.
pub fn multi(scopes: &[(&str, u64, u64, u64)], changes: Vec<AuthorityRecord>) -> PullPage {
    PullPage {
        cursors: scopes
            .iter()
            .map(|(c, from, to, head)| {
                (
                    c.to_string(),
                    CursorRange {
                        from: *from,
                        to: *to,
                        head: *head,
                    },
                )
            })
            .collect(),
        changes,
    }
}
/// The authority of `Entry e` at `stamp`: a state, or `None` for a deletion.
pub fn authority(text: Option<&str>, stamp: u64) -> AuthorityRecord {
    AuthorityRecord {
        model: "Entry".into(),
        identity: json!({"id":"e"}),
        stamp,
        state: text
            .map(|t| json!({"text":t,"note":null}))
            .unwrap_or(Value::Null),
        error: None,
    }
}
/// The authority of any `Entry` at `stamp`.
pub fn authority_of(id: &str, text: Option<&str>, stamp: u64) -> AuthorityRecord {
    let mut record = authority(text, stamp);
    record.identity = json!({ "id": id });
    record
}
/// A receipt answering this client's batch `sequence` with `records` and no rejections.
pub fn receipt(
    c: &mut Client<SqliteStore>,
    sequence: u64,
    records: Vec<AuthorityRecord>,
) -> PushReceipt {
    PushReceipt {
        client_id: c.client_id().to_string(),
        batch_sequence: sequence,
        rejections: vec![],
        completions: vec![],
        records,
        memberships: Vec::new(),
    }
}
/// A receipt rejecting `ordinals` with `code` and returning `records` for the rest.
pub fn rejecting(
    c: &mut Client<SqliteStore>,
    sequence: u64,
    ordinals: &[u64],
    code: &str,
    records: Vec<AuthorityRecord>,
) -> PushReceipt {
    let mut r = receipt(c, sequence, records);
    r.rejections = ordinals
        .iter()
        .map(|o| Rejection {
            ordinal: *o,
            code: code.into(),
        })
        .collect();
    r
}
/// Commit first delivery boundaries the way a session's acknowledgement at
/// these heads does: the expected identities are the stored ones, so nothing
/// here is stale ([#150](https://github.com/zanminwang/axton/issues/150)).
pub fn acknowledge(c: &mut Client<SqliteStore>, heads: &[(&str, u64)]) -> Initialization {
    let mut expected = BTreeMap::new();
    for (scope, _) in heads {
        let state = c
            .subscription_state(scope)
            .unwrap()
            .expect("a registered subscription");
        expected.insert(state.stream, state.subscription_id);
    }
    let heads = heads.iter().map(|(c, h)| (c.to_string(), *h)).collect();
    c.initialize_subscriptions(&expected, &heads).unwrap()
}
/// Only a subscribed scope may be pulled: `apply_page` drops a page for any
/// other. Registration alone has no delivery position, so this fixture also
/// commits the boundary a first acknowledgement at head zero establishes -
/// where these tests measure their pages from.
pub fn subscribe(c: &mut Client<SqliteStore>, scope: &str) {
    c.transaction(|tx| tx.set_stream(scope.into(), true))
        .unwrap();
    acknowledge(c, &[(scope, 0)]);
}
/// Unsubscribe and subscribe again: a new identity, initialized at zero as its
/// own first acknowledgement would leave it.
pub fn resubscribe(c: &mut Client<SqliteStore>, scope: &str) {
    c.transaction(|tx| tx.set_stream(scope.into(), false))
        .unwrap();
    subscribe(c, scope);
}
pub fn seed(c: &mut Client<SqliteStore>, text: &str) {
    c.transaction(|tx| {
        tx.direct(Operation {
            model: "Entry".into(),
            op: OperationKind::Create,
            identity: json!({"id":"e"}),
            values: Some(json!({"text":text,"note":null})),
        })
    })
    .unwrap();
}
pub fn family_schema() -> Schema {
    Schema::from_value(json!({"enums":[],"models":[
 {"name":"Book","identity":["id"],"fields":[{"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"}},{"name":"title","nullable":false,"type":{"kind":"scalar","name":"string"}}]},
 {"name":"Comment","identity":["id"],"fields":[{"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"}},{"name":"bookId","nullable":false,"type":{"kind":"scalar","name":"string"}},{"name":"text","nullable":false,"type":{"kind":"scalar","name":"string"}}],"relations":[{"name":"book","target":"Book","fields":["bookId"],"targetFields":["id"],"onDelete":"delete"}],"unique":[["bookId","text"]]}
]})).unwrap()
}
pub fn create(model: &str, id: &str, values: Value) -> Operation {
    Operation {
        model: model.into(),
        op: OperationKind::Create,
        identity: json!({ "id": id }),
        values: Some(values),
    }
}
pub fn table_count(c: &mut Client<SqliteStore>, table: &str) -> u64 {
    c.read_sql(&format!("SELECT COUNT(*) AS n FROM \"{table}\""), &[])
        .unwrap()[0]["n"]
        .as_u64()
        .unwrap()
}
/// The Entry schema with native Loads ([#173](https://github.com/zanminwang/axton/issues/173)):
/// `Entries` v1 and v2 (a `projectId` UUID and a nullable `since` date-time),
/// the no-argument `Recent` and `Tagged` (a string list), each filling the
/// `entries` list of Entry identities at read contract 1.
pub fn load_schema_value() -> Value {
    let fields = json!([
        {"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"}},
        {"name":"text","nullable":false,"type":{"kind":"scalar","name":"string"}},
        {"name":"note","nullable":true,"type":{"kind":"scalar","name":"string"}}
    ]);
    let input = |name: &str, scalar: &str, nullable: bool, list: bool| {
        json!({"kind":"value","name":name,"type":{"kind":"scalar","name":scalar},
            "nullable":nullable,"list":list,"required":true,
            "cardinality": if list { "list" } else { "single" }})
    };
    let load = |name: &str, version: u64, inputs: Value| {
        json!({"name":name,"version":version,"inputs":inputs,
            "outputs":[{"name":"entries","kind":"model","cardinality":"list","source":"handlerIdentity",
                "model":"Entry","modelReadVersion":1,
                "handlerType":{"kind":"identity","model":"Entry","fields":[
                    {"name":"id","type":{"kind":"scalar","name":"string"}}]}}],
            "input":{"models":[],"enums":[]},"outputEnums":[]})
    };
    let project = json!([
        input("projectId", "uuid", false, false),
        input("since", "dateTime", true, false)
    ]);
    json!({
        "enums":[],
        "models":[{"name":"Entry","version":1,"identity":["id"],"fields":fields}],
        "resultModels":[{"name":"Entry","version":1,"identity":["id"],"fields":fields,"enums":[]}],
        "loads":[
            load("Entries", 1, project.clone()),
            load("Entries", 2, project),
            load("Recent", 1, json!([])),
            load("Tagged", 1, json!([input("tags", "string", false, true)]))
        ]
    })
}
pub fn load_schema() -> Schema {
    Schema::from_value(load_schema_value()).unwrap()
}
/// A successful page answering `fence`: the Entry identities `entries` as
/// `(id, text, stamp)` with their records, and `next` as the continuation
/// state (`None` completes the job).
pub fn load_page(
    fence: &LoadFence,
    entries: &[(&str, &str, u64)],
    next: Option<Value>,
) -> LoadPageResponse {
    LoadPageResponse {
        load_id: fence.load_id.clone(),
        call_id: fence.call_id.clone(),
        outcome: LoadOutcome::Succeeded {
            data: json!({"entries": entries.iter().map(|(id, _, _)| json!({"id": id})).collect::<Vec<_>>()}),
            next: next.map(|state| Continuation { state }),
        },
        records: entries
            .iter()
            .map(|(id, text, stamp)| authority_of(id, Some(text), *stamp))
            .collect(),
        memberships: Vec::new(),
    }
}
/// The correlated reply a decoded response carries for a well-formed `page`.
pub fn reply(page: LoadPageResponse) -> LoadPageReply {
    LoadPageReply {
        load_id: page.load_id.clone(),
        call_id: page.call_id.clone(),
        page: Ok(page),
    }
}
/// A SQLite store whose next commit fails once while `fail_commit` is set:
/// nothing of that transaction is kept.
pub struct CommitFaultStore {
    pub inner: SqliteStore,
    pub fail_commit: std::sync::Arc<std::sync::atomic::AtomicBool>,
}
impl CommitFaultStore {
    pub fn open(path: &std::path::Path) -> (Self, std::sync::Arc<std::sync::atomic::AtomicBool>) {
        let fail = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        (
            Self {
                inner: SqliteStore::open(path).unwrap(),
                fail_commit: fail.clone(),
            },
            fail,
        )
    }
}
impl ClientStore for CommitFaultStore {
    fn begin(&mut self) -> Result<()> {
        self.inner.begin()
    }
    fn commit(&mut self) -> Result<()> {
        if self
            .fail_commit
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(axton_core::invalid("injected commit failure"));
        }
        self.inner.commit()
    }
    fn rollback(&mut self) -> Result<()> {
        self.inner.rollback()
    }
    fn savepoint(&mut self, name: &str) -> Result<()> {
        self.inner.savepoint(name)
    }
    fn release(&mut self, name: &str) -> Result<()> {
        self.inner.release(name)
    }
    fn rollback_to(&mut self, name: &str) -> Result<()> {
        self.inner.rollback_to(name)
    }
    fn execute(&mut self, sql: &str, parameters: &[Value]) -> Result<usize> {
        self.inner.execute(sql, parameters)
    }
    fn execute_batch(&mut self, sql: &str) -> Result<()> {
        self.inner.execute_batch(sql)
    }
    fn query(&mut self, sql: &str, parameters: &[Value]) -> Result<SqlRows> {
        self.inner.query(sql, parameters)
    }
    fn query_committed(&mut self, sql: &str, parameters: &[Value]) -> Result<SqlRows> {
        self.inner.query_committed(sql, parameters)
    }
}
/// A `Tagged` job whose first page fits the 1 MiB request bound and whose
/// committed first page returns a 60 KB state: its next frozen page request
/// no longer fits, even alone. Answers the job ID.
pub fn oversized_next_page<S: ClientStore>(c: &mut Client<S>) -> String {
    let tag = "x".repeat(limits::LOAD_REQUEST_BYTES - 30_000);
    let job = c
        .start_load(
            "Tagged",
            1,
            &json!({ "tags": [tag] }),
            LoadOptions::default(),
        )
        .unwrap()
        .job;
    let fence = LoadFence {
        replica: c.replica_generation(),
        load_id: job.id.clone(),
        run: job.run,
        call_id: job.call_id.unwrap(),
    };
    let page = load_page(&fence, &[], Some(json!("s".repeat(60_000))));
    assert!(matches!(
        c.store_load_page(&fence, reply(page)).unwrap(),
        LoadStored::Applied { .. }
    ));
    let next = c.get_load(&job.id).unwrap().unwrap().intent.unwrap();
    assert!(
        LoadBatchRequest { loads: vec![next] }.encode().is_err(),
        "the next frozen page exceeds the request bound"
    );
    job.id
}

/// Upgrade the old authority-only fixture vocabulary at the test host boundary.
/// Production decoders stay strict; these fixtures now state Stream provenance.
pub fn scope_fixture(mut value: Value) -> Value {
    if value["mode"] == "bootstrap"
        && let Some(scope) = value
            .as_object_mut()
            .and_then(|object| object.remove("scope"))
    {
        value["stream"] = scope;
    }
    let ranges = value.get("cursors").and_then(Value::as_object).cloned();
    if let Some(ranges) = ranges {
        if let Some(records) = value["changes"].as_array().cloned() {
            if records.iter().any(|r| r.get("kind").is_some()) {
                return value;
            }
            let mut changes = vec![];
            for (scope, range) in ranges {
                let from = range["from"].as_u64().unwrap_or(0);
                let to = range["to"].as_u64().unwrap_or(0);
                if to <= from {
                    continue;
                }
                for (i, record) in records.iter().enumerate() {
                    let mut record = record.clone();
                    record["kind"] = json!("upsert");
                    record["stream"] = json!(scope);
                    record["cursor"] =
                        json!(to.saturating_sub(records.len().saturating_sub(i + 1) as u64));
                    changes.push(record);
                }
            }
            value["changes"] = json!(changes);
        }
    } else if value["mode"] == "bootstrap"
        && let Some(records) = value["records"].as_array().cloned()
    {
        let to = value["to"].as_u64().unwrap();
        let changes: Vec<Value> = records
            .iter()
            .enumerate()
            .map(|(i, record)| {
                let mut record = record.clone();
                record["kind"] = json!("upsert");
                record["stream"] = value["stream"].clone();
                record["cursor"] =
                    json!(to.saturating_sub(records.len().saturating_sub(i + 1) as u64));
                record
            })
            .collect();
        value.as_object_mut().unwrap().remove("records");
        value["changes"] = json!(changes);
    }
    value
}
