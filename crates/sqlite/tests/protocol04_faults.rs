use axton_client::{
    Client, ClientStore, RecordKey, Schema, SqlRows,
    runtime::{ClientRuntime, Input},
    v04,
};
use axton_core::{Result, invalid};
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, AtomicU8, Ordering},
};
const REQUEST_INSERT_FAULT: u8 = 1;
const REQUEST_COMMIT_FAULT: u8 = 2;

struct FaultStore {
    inner: SqliteStore,
    fail: Arc<AtomicBool>,
    request_fault: Arc<AtomicU8>,
    inserted_request: bool,
}
impl ClientStore for FaultStore {
    fn begin(&mut self) -> Result<()> {
        self.inserted_request = false;
        self.inner.begin()
    }
    fn commit(&mut self) -> Result<()> {
        if self.inserted_request
            && self
                .request_fault
                .compare_exchange(REQUEST_COMMIT_FAULT, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            return Err(invalid("injected read-intent commit failure"));
        }
        if self.fail.swap(false, Ordering::SeqCst) {
            Err(invalid("injected SQLite commit refusal"))
        } else {
            self.inner.commit()
        }
    }
    fn rollback(&mut self) -> Result<()> {
        self.inner.rollback()
    }
    fn savepoint(&mut self, n: &str) -> Result<()> {
        self.inner.savepoint(n)
    }
    fn release(&mut self, n: &str) -> Result<()> {
        self.inner.release(n)
    }
    fn rollback_to(&mut self, n: &str) -> Result<()> {
        self.inner.rollback_to(n)
    }
    fn execute(&mut self, s: &str, p: &[Value]) -> Result<usize> {
        if s.starts_with("INSERT INTO axton_v04_request") {
            if self
                .request_fault
                .compare_exchange(REQUEST_INSERT_FAULT, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return Err(invalid("injected read-intent insert failure"));
            }
            self.inserted_request = true;
        }
        self.inner.execute(s, p)
    }
    fn execute_batch(&mut self, s: &str) -> Result<()> {
        self.inner.execute_batch(s)
    }
    fn query(&mut self, s: &str, p: &[Value]) -> Result<SqlRows> {
        self.inner.query(s, p)
    }
    fn query_committed(&mut self, s: &str, p: &[Value]) -> Result<SqlRows> {
        self.inner.query_committed(s, p)
    }
}
fn schema() -> Schema {
    let mut raw: Value =
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap();
    raw["actions"] = json!([{"name":"Rename","version":1,"inputs":[{"kind":"model","name":"entry","model":"Entry","operation":"update","cardinality":"single"}],"outputs":[]}]);
    Schema::from_value(raw).unwrap()
}
fn fault_store_registry() {
    static REGISTRY: OnceLock<tempfile::TempDir> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        let dir = tempfile::tempdir().unwrap();
        SqliteStore::set_application_data_directory(dir.path()).unwrap();
        dir
    });
}
fn open(p: &std::path::Path, fail: Arc<AtomicBool>) -> Client<FaultStore> {
    fault_store_registry();
    Client::open_bound(
        FaultStore {
            inner: SqliteStore::open_exclusive(p).unwrap(),
            fail,
            request_fault: Arc::new(AtomicU8::new(0)),
            inserted_request: false,
        },
        schema(),
        v04::StoreBinding {
            backend: "b".into(),
            viewer: "a".into(),
            stream: "User:a".into(),
            contract: "app".into(),
        },
    )
    .unwrap()
}
fn key(id: &str) -> RecordKey {
    RecordKey {
        model: "Entry".into(),
        identity: json!({"id":id}),
    }
}
fn change(id: &str, cursor: u64) -> v04::StreamChange {
    v04::StreamChange::Upsert {
        record: v04::StreamRecord {
            key: key(id),
            cursor,
            state: json!({"text":id,"note":null}),
        },
    }
}
#[test]
fn commit_fault_keeps_unit_rows_evidence_cursor_and_plan_at_same_proven_prefix_across_reopen() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let fail = Arc::new(AtomicBool::new(false));
    let mut c = open(&p, fail.clone());
    let page = v04::DeltaPage {
        context: c.request_context().unwrap().clone(),
        page_id: "fault-page".into(),
        from: 0,
        to: 2,
        head: 2,
        units: vec![
            v04::CommitUnit {
                through: 1,
                changes: vec![change("a", 1)],
            },
            v04::CommitUnit {
                through: 2,
                changes: vec![change("b", 2)],
            },
        ],
    };
    c.begin_delta04(&page).unwrap();
    fail.store(true, Ordering::SeqCst);
    assert!(c.apply_delta_unit04(&page).is_err());
    assert!(c.read(&key("a")).unwrap().is_none());
    assert_eq!(
        c.record_evidence04(&key("a")).unwrap(),
        v04::RecordEvidence::default()
    );
    assert_eq!(c.stream_cursor04().unwrap(), 0);
    assert_eq!(c.delta_progress04().unwrap().unwrap().next_unit, 0);
    c.apply_delta_unit04(&page).unwrap();
    fail.store(true, Ordering::SeqCst);
    assert!(c.apply_delta_unit04(&page).is_err());
    assert!(c.read(&key("b")).unwrap().is_none());
    assert_eq!(
        c.record_evidence04(&key("b")).unwrap(),
        v04::RecordEvidence::default()
    );
    assert_eq!(c.stream_cursor04().unwrap(), 1);
    assert_eq!(c.delta_progress04().unwrap().unwrap().next_unit, 1);
    drop(c);
    let mut c = open(&p, fail.clone());
    assert_eq!(c.stream_cursor04().unwrap(), 1);
    assert_eq!(c.read(&key("a")).unwrap().unwrap()["text"], "a");
    assert_eq!(
        c.record_evidence04(&key("a"))
            .unwrap()
            .current
            .unwrap()
            .cursor,
        1
    );
    c.apply_delta_unit04(&page).unwrap();
    assert_eq!(c.stream_cursor04().unwrap(), 2);
    assert_eq!(c.read(&key("b")).unwrap().unwrap()["text"], "b");
}
#[test]
fn failed_reset_keeps_old_incarnation_rows_and_authority_after_actual_reopen() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let fail = Arc::new(AtomicBool::new(false));
    let mut c = open(&p, fail.clone());
    let old = c.request_context().unwrap().clone();
    let page = v04::DeltaPage {
        context: old.clone(),
        page_id: "before-reset".into(),
        from: 0,
        to: 57,
        head: 57,
        units: vec![v04::CommitUnit {
            through: 57,
            changes: vec![change("a", 57)],
        }],
    };
    c.apply_delta04(&page).unwrap();
    fail.store(true, Ordering::SeqCst);
    assert!(c.reset_store04(true).is_err());
    assert_eq!(c.request_context().unwrap(), &old);
    assert_eq!(c.read(&key("a")).unwrap().unwrap()["text"], "a");
    assert_eq!(c.stream_cursor04().unwrap(), 57);
    drop(c);
    let mut c = open(&p, fail);
    assert_eq!(c.request_context().unwrap(), &old);
    assert_eq!(c.stream_cursor04().unwrap(), 57);
    assert_eq!(
        c.record_evidence04(&key("a"))
            .unwrap()
            .current
            .unwrap()
            .cursor,
        57
    );
}

#[test]
fn locally_failed_settlement_keeps_accepted_receipt_pending_work_and_completion_atomic() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let fail = Arc::new(AtomicBool::new(false));
    let mut c = open(&p, fail.clone());
    let context = c.request_context().unwrap().clone();
    c.apply_cache04(
        &context,
        &[v04::ReadRecord {
            key: key("a"),
            cursor: v04::NullCursor,
            state: json!({"text":"base","note":null}),
        }],
        true,
    )
    .unwrap();
    let call = c
        .submit_action("Rename", 1, json!({"entry":{"id":"a","text":"pending"}}))
        .unwrap();
    let intent = c.mutation_intent04(&call.call_id).unwrap().unwrap();
    let receipt = v04::MutationReceipt {
        context: context.clone(),
        intent_digest: intent.digest().unwrap(),
        completion: axton_client::CallCompletion {
            call_id: call.call_id.clone(),
            outcome: axton_client::ActionOutcome::Succeeded {
                result: Value::Null,
            },
        },
        targets: vec![v04::SettlementTarget::Private {
            record: v04::ReadRecord {
                key: key("a"),
                cursor: v04::NullCursor,
                state: json!({"text":"canonical","note":null}),
            },
        }],
    };
    c.save_receipt04(&receipt).unwrap();
    fail.store(true, Ordering::SeqCst);
    assert!(c.settle_receipts04().is_err());
    assert!(c.accepted_awaiting04(&call.call_id).unwrap());
    assert_eq!(c.pending_count().unwrap(), 1);
    assert_eq!(c.read(&key("a")).unwrap().unwrap()["text"], "pending");
    assert!(c.call_completion04(&call.call_id).unwrap().is_none());
    assert_eq!(
        c.record_evidence04(&key("a")).unwrap(),
        v04::RecordEvidence::default()
    );
    assert_eq!(c.stream_cursor04().unwrap(), 0);
    drop(c);
    let mut c = open(&p, fail);
    assert!(c.accepted_awaiting04(&call.call_id).unwrap());
    assert_eq!(c.read(&key("a")).unwrap().unwrap()["text"], "pending");
    let report = c.settle_receipts04().unwrap();
    assert_eq!(report.completions, vec![receipt.completion.clone()]);
    assert_eq!(
        c.call_completion04(&call.call_id).unwrap(),
        Some(receipt.completion)
    );
    assert_eq!(c.pending_count().unwrap(), 0);
    assert_eq!(c.read(&key("a")).unwrap().unwrap()["text"], "canonical");
    assert_eq!(c.stream_cursor04().unwrap(), 0);
    assert_eq!(
        c.record_evidence04(&key("a")).unwrap(),
        v04::RecordEvidence::default()
    );
}

fn runtime_run(runtime: &mut ClientRuntime<FaultStore>) -> Vec<Value> {
    while runtime.step(1, 7) {}
    runtime
        .take_events()
        .into_iter()
        .map(|e| serde_json::to_value(e).unwrap())
        .collect()
}
fn runtime_task(runtime: &mut ClientRuntime<FaultStore>, id: &str, command: Value) {
    runtime
        .receive(
            serde_json::from_value::<Input>(
                json!({"type":"task","requestId":id,"command":command}),
            )
            .unwrap(),
            1,
            7,
        )
        .unwrap();
}
fn answer_query(runtime: &mut ClientRuntime<FaultStore>, effect: &Value) -> Vec<Value> {
    let request: v04::ReadIntent =
        v04::decode(effect["operation"]["body"].as_str().unwrap().as_bytes()).unwrap();
    let response = json!({"context":request.context,"completion":{"callId":request.call_id,"outcome":{"status":"succeeded","result":null}},"records":[]});
    runtime.receive(serde_json::from_value(json!({"type":"effectResult","effectId":effect["effectId"],"outcome":{"ok":true,"value":{"status":200,"body":response.to_string()}}})).unwrap(), 1, 7).unwrap();
    runtime_run(runtime)
}
fn query_once_prepare_fault(fault: u8, refresh: bool) {
    fault_store_registry();
    let dir = tempfile::tempdir().unwrap();
    let request_fault = Arc::new(AtomicU8::new(0));
    let mut raw = serde_json::to_value(schema()).unwrap();
    raw["actions"] = json!([{"name":"Ping","kind":"query","version":1,"inputs":[],"outputs":[]}]);
    let mut client = Client::open_bound(
        FaultStore {
            inner: SqliteStore::open_exclusive(dir.path().join("db")).unwrap(),
            fail: Arc::new(AtomicBool::new(false)),
            request_fault: request_fault.clone(),
            inserted_request: false,
        },
        Schema::from_value(raw).unwrap(),
        v04::StoreBinding {
            backend: "b".into(),
            viewer: "a".into(),
            stream: "User:a".into(),
            contract: "app".into(),
        },
    )
    .unwrap();
    client
        .apply_delta04(&v04::DeltaPage {
            context: client.request_context().unwrap().clone(),
            page_id: "seed".into(),
            from: 0,
            to: 1,
            head: 1,
            units: vec![v04::CommitUnit {
                through: 1,
                changes: vec![change("a", 1)],
            }],
        })
        .unwrap();
    let mut runtime = ClientRuntime::new(client);
    runtime_task(&mut runtime, "connect", json!({"kind":"connect"}));
    runtime_run(&mut runtime);
    let query = |refresh| json!({"kind":"invoke","name":"Ping","version":1,"args":{},"store":true,"once":true,"refresh":refresh});
    if refresh {
        runtime_task(&mut runtime, "initial", query(false));
        let events = runtime_run(&mut runtime);
        let effect = events
            .iter()
            .find(|e| e["operation"]["route"] == "action")
            .unwrap();
        let events = answer_query(&mut runtime, effect);
        assert!(
            events
                .iter()
                .any(|e| e["requestId"] == "initial" && e["ok"] == true),
            "{events:?}"
        );
    }
    let before_row = runtime.client().read(&key("a")).unwrap();
    let before_evidence = runtime.client().record_evidence04(&key("a")).unwrap();
    let before_generation = runtime.client().generation();
    request_fault.store(fault, Ordering::SeqCst);
    runtime_task(&mut runtime, "failed", query(refresh));
    let events = runtime_run(&mut runtime);
    let failed = events.iter().find(|e| e["requestId"] == "failed").unwrap();
    assert_eq!(failed["ok"], false, "{events:?}");
    assert!(
        failed["error"]
            .as_str()
            .unwrap()
            .contains("injected read-intent"),
        "{events:?}"
    );
    assert!(
        !events.iter().any(|e| e["operation"]["route"] == "action"),
        "prepare must not dispatch: {events:?}"
    );
    assert_eq!(
        request_fault.load(Ordering::SeqCst),
        0,
        "the fault reached request persistence"
    );
    assert_eq!(runtime.client().generation(), before_generation);
    assert_eq!(runtime.client().read(&key("a")).unwrap(), before_row);
    assert_eq!(
        runtime.client().record_evidence04(&key("a")).unwrap(),
        before_evidence
    );
    assert_eq!(runtime.client().stream_cursor04().unwrap(), 1);
    if refresh {
        runtime_task(&mut runtime, "cached", query(false));
        let events = runtime_run(&mut runtime);
        assert!(
            events
                .iter()
                .any(|e| e["requestId"] == "cached" && e["ok"] == true),
            "prior cache survives: {events:?}"
        );
        assert!(!events.iter().any(|e| e["operation"]["route"] == "action"));
    }
    runtime_task(&mut runtime, "retry", query(refresh));
    runtime_task(&mut runtime, "joined", query(refresh));
    let events = runtime_run(&mut runtime);
    let effects: Vec<_> = events
        .iter()
        .filter(|e| e["operation"]["route"] == "action")
        .collect();
    assert_eq!(
        effects.len(),
        1,
        "retry must dispatch one new request and concurrent caller joins: {events:?}"
    );
    let events = answer_query(&mut runtime, effects[0]);
    for id in ["retry", "joined"] {
        assert!(
            events
                .iter()
                .any(|e| e["requestId"] == id && e["ok"] == true),
            "{events:?}"
        );
    }
}
#[test]
fn query_once_insert_failure_releases_flight_for_identical_retry() {
    query_once_prepare_fault(REQUEST_INSERT_FAULT, false);
}
#[test]
fn query_once_commit_failure_releases_flight_for_identical_retry() {
    query_once_prepare_fault(REQUEST_COMMIT_FAULT, false);
}
#[test]
fn query_once_refresh_insert_failure_preserves_cache_and_releases_flight() {
    query_once_prepare_fault(REQUEST_INSERT_FAULT, true);
}
#[test]
fn query_once_refresh_commit_failure_preserves_cache_and_releases_flight() {
    query_once_prepare_fault(REQUEST_COMMIT_FAULT, true);
}
