mod common05;
use axton_client::{Client, Operation, OperationKind, v05};
use axton_sqlite::SqliteStore;
use common05::{key, open};
use serde_json::{Value, json};
fn state(text: Option<&str>) -> Value {
    text.map(|t| json!({"text":t,"note":null}))
        .unwrap_or(Value::Null)
}
fn op(kind: OperationKind, text: Option<&str>) -> Operation {
    Operation {
        model: "Entry".into(),
        identity: json!({"id":"e"}),
        op: kind,
        values: text.map(|t| {
            if kind == OperationKind::Create {
                state(Some(t))
            } else {
                json!({"text":t})
            }
        }),
    }
}
fn stream(c: &mut Client<SqliteStore>, cursor: u64, text: Option<&str>) {
    let context = c.request_context05().unwrap();
    c.install_authority05(
        &context,
        &[v05::AuthorityChange::Record {
            key: v05::RecordKey {
                model: "Entry".into(),
                identity: json!({"id":"e"}),
            },
            cursor,
            state: state(text),
        }],
        None,
    )
    .unwrap();
}
fn cache(c: &mut Client<SqliteStore>, text: Option<&str>, store: bool) {
    c.install_cache05(
        &[v05::ReadRecord {
            key: v05::RecordKey {
                model: "Entry".into(),
                identity: json!({"id":"e"}),
            },
            cursor: (),
            state: state(text),
        }],
        store,
    )
    .unwrap();
}
#[test]
fn direct_content_has_null_cursor_but_stream_history_survives_and_tombstone_guards_reads() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut c = open(&p);
    let context = c.request_context05().unwrap().clone();
    stream(&mut c, 57, Some("A"));
    c.transaction(|tx| tx.direct(op(OperationKind::Update, Some("B"))))
        .unwrap();
    let e = c.record_evidence05(&key()).unwrap();
    assert!(e.current.is_none());
    assert_eq!(e.history[&context.materialization], 57);
    stream(&mut c, 56, Some("old"));
    stream(&mut c, 57, Some("equal but different"));
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "B");
    stream(&mut c, 58, Some("new"));
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "new");
    c.transaction(|tx| tx.direct(op(OperationKind::Delete, None)))
        .unwrap();
    cache(&mut c, Some("refill"), true);
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "refill");
    stream(&mut c, 59, None);
    cache(&mut c, Some("late"), true);
    assert!(c.read(&key()).unwrap().is_none());
    assert!(
        c.record_evidence05(&key())
            .unwrap()
            .current
            .unwrap()
            .deleted
    );
    drop(c);
    let mut c = open(&p);
    cache(&mut c, Some("after-reopen"), true);
    assert!(c.read(&key()).unwrap().is_none());
    assert_eq!(
        c.record_evidence05(&key()).unwrap().history[&context.materialization],
        59
    );
}
#[test]
fn pending_optimism_does_not_release_base_protection_and_null_reads_never_delete() {
    let d = tempfile::tempdir().unwrap();
    let mut c = open(&d.path().join("db"));
    stream(&mut c, 57, Some("A"));
    c.transaction(|tx| {
        tx.submit_mutation05("Edit", 1, json!({"entry":{"id":"e","text":"P"}}), vec![])
    })
    .unwrap();
    assert_eq!(
        c.record_evidence05(&key()).unwrap().current.unwrap().cursor,
        57
    );
    cache(&mut c, Some("ordinary"), true);
    cache(&mut c, None, true);
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "P");
    stream(&mut c, 58, Some("new"));
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "P");
    assert_eq!(
        c.record_evidence05(&key()).unwrap().current.unwrap().cursor,
        58
    );
}
#[test]
fn cache_mode_false_changes_no_rows_or_evidence_and_true_is_best_effort_without_g() {
    let d = tempfile::tempdir().unwrap();
    let mut c = open(&d.path().join("db"));
    cache(&mut c, Some("snapshot"), false);
    assert!(c.read(&key()).unwrap().is_none());
    assert_eq!(
        c.record_evidence05(&key()).unwrap(),
        axton_core::authority::RecordEvidence::default()
    );
    cache(&mut c, Some("newer request finished first"), true);
    cache(&mut c, Some("older request finished last"), true);
    assert_eq!(
        c.read(&key()).unwrap().unwrap()["text"],
        "older request finished last"
    );
    assert_eq!(
        c.record_evidence05(&key()).unwrap(),
        axton_core::authority::RecordEvidence::default()
    );
    cache(&mut c, None, true);
    assert!(c.read(&key()).unwrap().is_some());
}
