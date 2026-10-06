use axton_client::{Client, ClientStore, RecordKey, Schema, SqlRows, v04};
use axton_core::{Result, invalid};
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
struct FaultStore {
    inner: SqliteStore,
    fail: Arc<AtomicBool>,
}
impl ClientStore for FaultStore {
    fn begin(&mut self) -> Result<()> {
        self.inner.begin()
    }
    fn commit(&mut self) -> Result<()> {
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
fn open(p: &std::path::Path, fail: Arc<AtomicBool>) -> Client<FaultStore> {
    Client::open_bound(
        FaultStore {
            inner: SqliteStore::open_exclusive(p).unwrap(),
            fail,
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
