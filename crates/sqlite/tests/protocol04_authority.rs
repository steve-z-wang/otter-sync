use axton_client::{Client, Mutation, Operation, OperationKind, RecordKey, Schema, v04};
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};

fn schema() -> Schema {
    Schema::from_value(
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap(),
    )
    .unwrap()
}
fn open(path: &std::path::Path) -> Client<SqliteStore> {
    Client::open_bound(
        SqliteStore::open_exclusive(path).unwrap(),
        schema(),
        v04::StoreBinding {
            backend: "backend".into(),
            viewer: "alice".into(),
            stream: "User:alice".into(),
            contract: "app".into(),
        },
    )
    .unwrap()
}
fn key() -> RecordKey {
    schema().record_key("Entry", &json!({"id":"e"})).unwrap()
}
fn state(text: Option<&str>) -> Value {
    text.map(|text| json!({"text":text,"note":null}))
        .unwrap_or(Value::Null)
}
fn op(kind: OperationKind, text: Option<&str>) -> Operation {
    Operation {
        model: "Entry".into(),
        identity: json!({"id":"e"}),
        op: kind,
        values: text.map(|text| {
            if kind == OperationKind::Create {
                state(Some(text))
            } else {
                json!({"text":text})
            }
        }),
    }
}
fn stream(c: &mut Client<SqliteStore>, cursor: u64, text: Option<&str>) {
    let context = c.request_context().unwrap().clone();
    c.install_stream04(
        &context,
        &v04::StreamRecord {
            key: key(),
            cursor,
            state: state(text),
        },
    )
    .unwrap();
}
fn cache(c: &mut Client<SqliteStore>, text: Option<&str>, store: bool) {
    let context = c.request_context().unwrap().clone();
    c.apply_cache04(
        &context,
        &[v04::ReadRecord {
            key: key(),
            cursor: v04::NullCursor,
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
    let context = c.request_context().unwrap().clone();
    stream(&mut c, 57, Some("A"));
    c.transaction(|tx| tx.direct(op(OperationKind::Update, Some("B"))))
        .unwrap();
    let e = c.record_evidence04(&key()).unwrap();
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
        c.record_evidence04(&key())
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
        c.record_evidence04(&key()).unwrap().history[&context.materialization],
        59
    );
}
#[test]
fn pending_optimism_does_not_release_base_protection_and_null_reads_never_delete() {
    let d = tempfile::tempdir().unwrap();
    let mut c = open(&d.path().join("db"));
    stream(&mut c, 57, Some("A"));
    c.transaction(|tx| {
        tx.enqueue(Mutation::new(
            "Edit",
            vec![op(OperationKind::Update, Some("P"))],
        ))
    })
    .unwrap();
    assert_eq!(
        c.record_evidence04(&key()).unwrap().current.unwrap().cursor,
        57
    );
    cache(&mut c, Some("ordinary"), true);
    cache(&mut c, None, true);
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "P");
    stream(&mut c, 58, Some("new"));
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "P");
    assert_eq!(
        c.record_evidence04(&key()).unwrap().current.unwrap().cursor,
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
        c.record_evidence04(&key()).unwrap(),
        v04::RecordEvidence::default()
    );
    cache(&mut c, Some("newer request finished first"), true);
    cache(&mut c, Some("older request finished last"), true);
    assert_eq!(
        c.read(&key()).unwrap().unwrap()["text"],
        "older request finished last"
    );
    assert_eq!(
        c.record_evidence04(&key()).unwrap(),
        v04::RecordEvidence::default()
    );
    cache(&mut c, None, true);
    assert!(c.read(&key()).unwrap().is_some());
}
#[test]
fn rematerialization_keeps_direct_patches_and_pending_without_overwriting_new_fields() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut c = open(&p);
    stream(&mut c, 57, Some("A"));
    c.transaction(|tx| tx.direct(op(OperationKind::Update, Some("D"))))
        .unwrap();
    c.transaction(|tx| {
        tx.enqueue(Mutation::new(
            "Edit",
            vec![op(OperationKind::Update, Some("P"))],
        ))
    })
    .unwrap();
    let old = c.request_context().unwrap().clone();
    drop(c);
    let mut changed = serde_json::to_value(schema()).unwrap();
    changed["models"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"label","nullable":true,"type":{"kind":"scalar","name":"string"}}));
    let mut c = Client::open_bound(
        SqliteStore::open_exclusive(&p).unwrap(),
        Schema::from_value(changed).unwrap(),
        old.binding.clone(),
    )
    .unwrap();
    let current = c.request_context().unwrap().clone();
    assert_ne!(old.materialization, current.materialization);
    c.install_stream04(
        &current,
        &v04::StreamRecord {
            key: key(),
            cursor: 57,
            state: json!({"text":"A","note":null,"label":"fresh"}),
        },
    )
    .unwrap();
    assert_eq!(
        c.read(&key()).unwrap().unwrap(),
        json!({"id":"e","text":"P","note":null,"label":"fresh"})
    );
    let evidence = c.record_evidence04(&key()).unwrap();
    assert!(evidence.current.is_none());
    assert_eq!(evidence.history[&old.materialization], 57);
    assert_eq!(evidence.history[&current.materialization], 57);
    assert!(c.apply_cache04(&old, &[], true).is_err());
}

#[test]
fn same_position_rematerialized_absence_keeps_later_direct_recreation_and_pending_patch() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut c = open(&p);
    stream(&mut c, 57, None);
    c.transaction(|tx| tx.direct(op(OperationKind::Create, Some("direct"))))
        .unwrap();
    c.transaction(|tx| {
        tx.enqueue(Mutation::new(
            "Edit",
            vec![op(OperationKind::Update, Some("pending"))],
        ))
    })
    .unwrap();
    let old = c.request_context().unwrap().clone();
    drop(c);
    let mut changed = serde_json::to_value(schema()).unwrap();
    changed["models"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"label","nullable":true,"type":{"kind":"scalar","name":"string"}}));
    let mut c = Client::open_bound(
        SqliteStore::open_exclusive(&p).unwrap(),
        Schema::from_value(changed).unwrap(),
        old.binding.clone(),
    )
    .unwrap();
    let active = c.request_context().unwrap().clone();
    c.install_stream04(
        &active,
        &v04::StreamRecord {
            key: key(),
            cursor: 57,
            state: Value::Null,
        },
    )
    .unwrap();
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "pending");
    let evidence = c.record_evidence04(&key()).unwrap();
    assert_eq!(evidence.history[&old.materialization], 57);
    assert_eq!(evidence.history[&active.materialization], 57);
    assert!(evidence.current.unwrap().deleted);
    cache(&mut c, Some("late cache"), true);
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "pending");
    assert_eq!(c.pending_count().unwrap(), 1);
}
