mod common05;
use axton_client::{Client, RecordKey, v05};
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};
fn key(model: &str) -> RecordKey {
    RecordKey {
        model: model.into(),
        identity: json!({"id":if model=="Book"{"b"}else{"c"}}),
    }
}
fn change(model: &str, cursor: u64, state: Value) -> v05::AuthorityChange {
    v05::AuthorityChange::Record {
        key: v05::RecordKey {
            model: model.into(),
            identity: key(model).identity,
        },
        cursor,
        state,
    }
}
#[test]
fn older_parent_absence_cannot_erase_already_installed_later_child_authority() {
    let d = tempfile::tempdir().unwrap();
    let mut c = Client::open05(
        SqliteStore::open(d.path().join("db")).unwrap(),
        common05::family_schema(),
        "User:u",
    )
    .unwrap();
    let context = c.request_context05().unwrap();
    c.install_authority05(
        &context,
        &[change(
            "Comment",
            58,
            json!({"bookId":"b","text":"later child authority"}),
        )],
        None,
    )
    .unwrap();
    c.install_authority05(&context, &[change("Book", 57, Value::Null)], None)
        .unwrap();
    assert_eq!(
        c.read(&key("Comment")).unwrap().unwrap()["text"],
        "later child authority"
    );
    assert_eq!(
        c.record_evidence05(&key("Comment"))
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
        let mut c = Client::open05(
            SqliteStore::open(d.path().join("db")).unwrap(),
            common05::family_schema(),
            "User:u",
        )
        .unwrap();
        let context = c.request_context05().unwrap();
        c.install_cache05(
            &[v05::ReadRecord {
                key: v05::RecordKey {
                    model: "Comment".into(),
                    identity: json!({"id":"c"}),
                },
                cursor: (),
                state: json!({"bookId":"b","text":"cached"}),
            }],
            true,
        )
        .unwrap();
        let mut changes = vec![
            change("Book", 57, Value::Null),
            change("Comment", 56, json!({"bookId":"b","text":"older child"})),
        ];
        if reverse {
            changes.reverse()
        };
        c.install_authority05(&context, &changes, None).unwrap();
        assert!(c.read(&key("Comment")).unwrap().is_none());
        assert_eq!(
            c.record_evidence05(&key("Comment"))
                .unwrap()
                .history
                .values()
                .copied()
                .max(),
            Some(56)
        );
    }
}

#[test]
fn same_position_parent_absence_rematerialization_preserves_later_direct_child_work() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut c = Client::open05(
        SqliteStore::open(&p).unwrap(),
        common05::family_schema(),
        "User:u",
    )
    .unwrap();
    let old = c.request_context05().unwrap();
    c.install_authority05(&old, &[change("Book", 57, Value::Null)], None)
        .unwrap();
    c.transaction(|tx| {
        tx.direct(common05::create(
            "Book",
            "b",
            json!({"title":"direct parent"}),
        ))?;
        tx.direct(common05::create(
            "Comment",
            "c",
            json!({"bookId":"b","text":"later direct child"}),
        ))
    })
    .unwrap();
    drop(c);
    let mut raw = serde_json::to_value(common05::family_schema()).unwrap();
    raw["models"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"label","nullable":true,"type":{"kind":"scalar","name":"string"}}));
    let mut c = Client::open05(
        SqliteStore::open(&p).unwrap(),
        axton_client::Schema::from_value(raw).unwrap(),
        "User:u",
    )
    .unwrap();
    let pending = c.pending_schema05().unwrap().unwrap();
    c.transaction(|tx| tx.enable_schema05(&pending.previous_context, &pending.desired_context))
        .unwrap();
    let active = c.request_context05().unwrap();
    c.install_authority05(&active, &[change("Book", 57, Value::Null)], None)
        .unwrap();
    assert_eq!(
        c.read(&key("Book")).unwrap().unwrap()["title"],
        "direct parent"
    );
    assert_eq!(
        c.read(&key("Comment")).unwrap().unwrap()["text"],
        "later direct child"
    );
    assert!(
        c.record_evidence05(&key("Book"))
            .unwrap()
            .current
            .unwrap()
            .deleted
    );
    c.install_cache05(
        &[v05::ReadRecord {
            key: v05::RecordKey {
                model: "Comment".into(),
                identity: key("Comment").identity,
            },
            cursor: (),
            state: json!({"bookId":"b","text":"late cache"}),
        }],
        true,
    )
    .unwrap();
    assert_eq!(
        c.read(&key("Comment")).unwrap().unwrap()["text"],
        "later direct child"
    );
}
