mod common;
use axton_client::{Client, RecordKey, Schema, v04};
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};
fn key(model: &str, id: &str) -> RecordKey {
    RecordKey {
        model: model.into(),
        identity: json!({"id":id}),
    }
}
fn open(p: &std::path::Path, schema: Schema) -> Client<SqliteStore> {
    Client::open_bound(
        SqliteStore::open_exclusive(p).unwrap(),
        schema,
        v04::StoreBinding {
            backend: "b".into(),
            viewer: "a".into(),
            stream: "User:a".into(),
            contract: "app".into(),
        },
    )
    .unwrap()
}
fn cache(c: &mut Client<SqliteStore>, model: &str, id: &str, state: Value) {
    c.apply_cache04(
        &c.request_context().unwrap().clone(),
        &[v04::ReadRecord {
            key: key(model, id),
            cursor: v04::NullCursor,
            state,
        }],
        true,
    )
    .unwrap();
}
#[test]
fn ordinary_child_cache_only_checks_known_current_declared_parent_stream_tombstone() {
    let d = tempfile::tempdir().unwrap();
    let mut c = open(&d.path().join("db"), common::family_schema());
    cache(
        &mut c,
        "Comment",
        "c",
        json!({"bookId":"missing","text":"allowed without parent"}),
    );
    assert!(c.read(&key("Comment", "c")).unwrap().is_some());
    c.install_stream04(
        &c.request_context().unwrap().clone(),
        &v04::StreamRecord {
            key: key("Book", "missing"),
            cursor: 57,
            state: Value::Null,
        },
    )
    .unwrap();
    assert!(c.read(&key("Comment", "c")).unwrap().is_none());
    assert_eq!(
        c.record_evidence04(&key("Comment", "c")).unwrap(),
        v04::RecordEvidence::default()
    );
    cache(
        &mut c,
        "Comment",
        "c",
        json!({"bookId":"missing","text":"blocked"}),
    );
    assert!(c.read(&key("Comment", "c")).unwrap().is_none());
    c.transaction(|tx| {
        tx.direct(common::create(
            "Book",
            "missing",
            json!({"title":"direct parent"}),
        ))
    })
    .unwrap();
    cache(
        &mut c,
        "Comment",
        "c",
        json!({"bookId":"missing","text":"refill"}),
    );
    assert!(c.read(&key("Comment", "c")).unwrap().is_none());
    c.install_stream04(
        &c.request_context().unwrap().clone(),
        &v04::StreamRecord {
            key: key("Book", "missing"),
            cursor: 58,
            state: json!({"title":"current parent"}),
        },
    )
    .unwrap();
    cache(
        &mut c,
        "Comment",
        "c",
        json!({"bookId":"missing","text":"refill"}),
    );
    assert_eq!(
        c.read(&key("Comment", "c")).unwrap().unwrap()["text"],
        "refill"
    );
}
#[test]
fn same_position_parent_absence_rematerialization_preserves_later_direct_child_work() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut c = open(&p, common::family_schema());
    let old = c.request_context().unwrap().clone();
    c.install_stream04(
        &old,
        &v04::StreamRecord {
            key: key("Book", "b"),
            cursor: 57,
            state: Value::Null,
        },
    )
    .unwrap();
    c.transaction(|tx| {
        tx.direct(common::create(
            "Book",
            "b",
            json!({"title":"direct parent"}),
        ))?;
        tx.direct(common::create(
            "Comment",
            "c",
            json!({"bookId":"b","text":"later direct child"}),
        ))
    })
    .unwrap();
    drop(c);
    let mut raw = serde_json::to_value(common::family_schema()).unwrap();
    raw["models"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"label","nullable":true,"type":{"kind":"scalar","name":"string"}}));
    let mut c = open(&p, Schema::from_value(raw).unwrap());
    let active = c.request_context().unwrap().clone();
    c.install_stream04(
        &active,
        &v04::StreamRecord {
            key: key("Book", "b"),
            cursor: 57,
            state: Value::Null,
        },
    )
    .unwrap();
    assert_eq!(
        c.read(&key("Book", "b")).unwrap().unwrap()["title"],
        "direct parent"
    );
    assert_eq!(
        c.read(&key("Comment", "c")).unwrap().unwrap()["text"],
        "later direct child"
    );
    assert!(
        c.record_evidence04(&key("Book", "b"))
            .unwrap()
            .current
            .unwrap()
            .deleted
    );
    cache(
        &mut c,
        "Comment",
        "c",
        json!({"bookId":"b","text":"late cache"}),
    );
    assert_eq!(
        c.read(&key("Comment", "c")).unwrap().unwrap()["text"],
        "later direct child"
    );
}

#[test]
fn older_parent_absence_cannot_erase_already_installed_later_child_authority() {
    let d = tempfile::tempdir().unwrap();
    let mut c = open(&d.path().join("db"), common::family_schema());
    let context = c.request_context().unwrap().clone();
    c.install_stream04(
        &context,
        &v04::StreamRecord {
            key: key("Comment", "c"),
            cursor: 58,
            state: json!({"bookId":"b","text":"later child authority"}),
        },
    )
    .unwrap();
    c.install_stream04(
        &context,
        &v04::StreamRecord {
            key: key("Book", "b"),
            cursor: 57,
            state: Value::Null,
        },
    )
    .unwrap();
    assert_eq!(
        c.read(&key("Comment", "c")).unwrap().unwrap()["text"],
        "later child authority"
    );
    assert_eq!(
        c.record_evidence04(&key("Comment", "c"))
            .unwrap()
            .current
            .unwrap()
            .cursor,
        58
    );
}

#[test]
fn group_cascade_uses_positions_not_transport_array_order() {
    for reverse in [false, true] {
        let d = tempfile::tempdir().unwrap();
        let mut c = open(&d.path().join("db"), common::family_schema());
        cache(
            &mut c,
            "Comment",
            "c",
            json!({"bookId":"b","text":"cached"}),
        );
        let mut changes = vec![
            v04::StreamChange::Upsert {
                record: v04::StreamRecord {
                    key: key("Book", "b"),
                    cursor: 57,
                    state: Value::Null,
                },
            },
            v04::StreamChange::Upsert {
                record: v04::StreamRecord {
                    key: key("Comment", "c"),
                    cursor: 56,
                    state: json!({"bookId":"b","text":"older child"}),
                },
            },
        ];
        if reverse {
            changes.reverse();
        }
        c.apply_delta04(&v04::DeltaPage {
            context: c.request_context().unwrap().clone(),
            page_id: "causal".into(),
            from: 0,
            to: 57,
            head: 57,
            units: vec![v04::CommitUnit {
                through: 57,
                changes,
            }],
        })
        .unwrap();
        assert!(c.read(&key("Comment", "c")).unwrap().is_none());
        assert_eq!(
            c.record_evidence04(&key("Comment", "c"))
                .unwrap()
                .history
                .values()
                .copied()
                .max(),
            Some(56)
        );
    }
}
