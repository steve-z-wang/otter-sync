mod common05;
use axton_client::{Client, Operation, OperationKind, Readiness, RecordKey, Schema, v05};
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};
fn key(model: &str, id: &str) -> RecordKey {
    RecordKey {
        model: model.into(),
        identity: json!({"id":id}),
    }
}
fn delete_book() -> Operation {
    Operation {
        model: "Book".into(),
        op: OperationKind::Delete,
        identity: json!({"id":"b"}),
        values: None,
    }
}
fn schema() -> Schema {
    let mut s = serde_json::to_value(common05::family_schema()).unwrap();
    s["prerequisites"] = json!([{"name":"Ready","fields":[{"name":"key","type":"String"}]}]);
    s["requirements"] =
        json!([{"model":"Book","field":"title","name":"Ready","arguments":{"key":"self"}}]);
    s["actions"] = json!([
 {"name":"Hold","version":1,"inputs":[{"kind":"model","name":"book","model":"Book","operation":"update","cardinality":"single","fields":["title"]}],"outputs":[]},
 {"name":"EditComment","version":1,"inputs":[{"kind":"model","name":"comment","model":"Comment","operation":"update","cardinality":"single","fields":["text"]}],"outputs":[]},
 {"name":"Ping","version":1,"inputs":[],"outputs":[]}]);
    Schema::from_value(s).unwrap()
}
fn start(path: &std::path::Path) -> Client<SqliteStore> {
    let mut c = Client::open05(SqliteStore::open(path).unwrap(), schema(), "User:u").unwrap();
    c.initialize_stream05(0).unwrap();
    c.transaction(|tx| {
        tx.direct(common05::create("Book", "b", json!({"title":"B"})))?;
        tx.direct(common05::create("Book", "other", json!({"title":"O"})))?;
        tx.direct(common05::create(
            "Comment",
            "c",
            json!({"bookId":"b","text":"C"}),
        ))
    })
    .unwrap();
    c
}
fn settle(c: &mut Client<SqliteStore>, accepted: bool, owner: bool) {
    let b = c.freeze_batch05().unwrap().unwrap();
    assert_eq!(b.mutations.len(), 1);
    let outcome = if accepted {
        v05::MutationOutcome::Accepted {
            sync_cursor: 0,
            result: Value::Null,
            targets: if owner {
                vec![v05::SettlementTarget::Private {
                    record: v05::ReadRecord {
                        key: v05::RecordKey {
                            model: "Book".into(),
                            identity: json!({"id":"other"}),
                        },
                        cursor: (),
                        state: json!({"title":"O2"}),
                    },
                }]
            } else {
                vec![]
            },
        }
    } else {
        v05::MutationOutcome::Rejected {
            code: "act.denied".into(),
            message: None,
        }
    };
    let results = vec![v05::MutationResult {
        mutation_id: b.mutations[0].id,
        outcome,
    }];
    c.acknowledge_batch05(&v05::BatchAcknowledgement {
        context: b.context,
        batch_id: b.batch_id,
        digest: b.digest,
        results,
    })
    .unwrap();
    c.settle_ready05().unwrap();
}
fn release(c: &mut Client<SqliteStore>) {
    c.set_readiness(
        &json!({"name":"Ready","arguments":{"key":"hold"}}).to_string(),
        Readiness::Ready,
    )
    .unwrap();
}
#[test]
fn a_companion_cascade_follows_its_call_and_spares_later_children() {
    for accepted in [false, true] {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("db");
        let mut c = start(&path);
        c.transaction(|tx| {
            tx.submit_mutation05(
                "Hold",
                1,
                json!({"book":{"id":"other","title":"hold"}}),
                vec![delete_book()],
            )
        })
        .unwrap();
        assert!(c.read(&key("Comment", "c")).unwrap().is_none());
        c.transaction(|tx| {
            tx.submit_mutation05("Ping", 1, json!({}), vec![])?;
            tx.direct(common05::create("Book", "b", json!({"title":"B2"})))?;
            tx.direct(common05::create(
                "Comment",
                "c2",
                json!({"bookId":"b","text":"C2"}),
            ))
        })
        .unwrap();
        settle(&mut c, true, false);
        assert!(c.read(&key("Comment", "c")).unwrap().is_none());
        assert_eq!(c.read(&key("Book", "b")).unwrap().unwrap()["title"], "B2");
        assert!(c.read(&key("Comment", "c2")).unwrap().is_some());
        drop(c);
        let mut c = Client::open05(SqliteStore::open(&path).unwrap(), schema(), "User:u").unwrap();
        release(&mut c);
        settle(&mut c, accepted, true);
        assert_eq!(c.read(&key("Book", "b")).unwrap().unwrap()["title"], "B2");
        assert!(c.read(&key("Comment", "c2")).unwrap().is_some());
        assert_eq!(c.read(&key("Comment", "c")).unwrap().is_some(), !accepted);
        assert_eq!(c.pending_count().unwrap(), 0);
        assert_eq!(c.before_image_count().unwrap(), 0);
    }
}
#[test]
fn a_companion_cascade_keeps_its_place_before_later_companions_of_the_same_call() {
    for accepted in [false, true] {
        let d = tempfile::tempdir().unwrap();
        let mut c = start(&d.path().join("db"));
        c.transaction(|tx| {
            tx.submit_mutation05(
                "EditComment",
                1,
                json!({"comment":{"id":"c","text":"earlier"}}),
                vec![],
            )
        })
        .unwrap();
        c.transaction(|tx| {
            tx.submit_mutation05(
                "Hold",
                1,
                json!({"book":{"id":"other","title":"hold"}}),
                vec![
                    delete_book(),
                    common05::create("Book", "b", json!({"title":"B2"})),
                    common05::create("Comment", "c", json!({"bookId":"b","text":"C2"})),
                ],
            )
        })
        .unwrap();
        settle(&mut c, false, false);
        assert_eq!(c.read(&key("Comment", "c")).unwrap().unwrap()["text"], "C2");
        assert_eq!(c.read(&key("Book", "b")).unwrap().unwrap()["title"], "B2");
        release(&mut c);
        settle(&mut c, accepted, true);
        assert_eq!(
            c.read(&key("Book", "b")).unwrap().unwrap()["title"],
            if accepted { "B2" } else { "B" }
        );
        assert_eq!(
            c.read(&key("Comment", "c")).unwrap().unwrap()["text"],
            if accepted { "C2" } else { "C" }
        );
        assert_eq!(c.pending_count().unwrap(), 0);
        assert_eq!(c.before_image_count().unwrap(), 0);
    }
}
