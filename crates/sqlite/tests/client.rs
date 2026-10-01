mod common;
use axton_client::*;
use axton_sqlite::SqliteStore;
use common::*;
use serde_json::json;
use std::collections::BTreeSet;

#[test]
fn open_creates_tables_persists_identity_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open(&path);
    let id = c.client_id().to_string();
    seed(&mut c, "A");
    assert_eq!(
        c.read(&key()).unwrap().unwrap(),
        json!({"id":"e","text":"A","note":null})
    );
    drop(c);
    let mut c = open(&path);
    assert_eq!(c.client_id(), id);
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "A");
    assert_eq!(table_count(&mut c, "axton_before_Entry"), 0);
}

#[test]
fn optimistic_edit_holds_truth_once_and_rejection_rebuilds_from_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    seed(&mut c, "A");
    c.transaction(|tx| {
        tx.enqueue(Mutation::new("Composite", vec![update("B"), update("C")]))?;
        Ok(())
    })
    .unwrap();
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "C");
    assert_eq!(c.before_image_count().unwrap(), 1);
    assert_eq!(
        c.read_sql("SELECT text FROM axton_before_Entry", &[])
            .unwrap(),
        vec![json!({"text":"A"})]
    );
    c.transaction(|tx| {
        tx.enqueue(mutation("D"))?;
        Ok(())
    })
    .unwrap();
    assert_eq!(
        c.before_image_count().unwrap(),
        1,
        "second edit does not copy again"
    );
    c.drop_mutation(2).unwrap();
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "C");
    c.drop_mutation(1).unwrap();
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "A");
    assert_eq!(c.before_image_count().unwrap(), 0);
}

#[test]
fn local_transaction_and_mutation_savepoint_have_independent_fate() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    seed(&mut c, "A");
    let events = c.watch(BTreeSet::from(["Entry".to_string()]));
    let result: Result<()> = c.transaction(|tx| {
        tx.enqueue(mutation("B"))?;
        Err(invalid("rollback"))
    });
    assert!(result.is_err());
    assert_eq!(c.pending_count().unwrap(), 0);
    assert!(events.try_recv().is_err());
    c.transaction(|tx| {
        tx.direct(update("LOCAL"))?;
        let failed: Result<()> = tx.savepoint(|tx| {
            tx.enqueue(mutation("bad"))?;
            Err(invalid("refuse"))
        });
        assert!(failed.is_err());
        Ok(())
    })
    .unwrap();
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "LOCAL");
    assert_eq!(c.pending_count().unwrap(), 0);
    assert!(events.try_recv().is_ok());
    assert!(events.try_recv().is_err());
}

#[test]
fn watch_fires_only_for_declared_tables() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    let entry = c.watch(BTreeSet::from(["Entry".to_string()]));
    let queue = c.watch(BTreeSet::from(["axton_mutation".to_string()]));
    seed(&mut c, "A");
    assert!(entry.try_recv().is_ok());
    assert!(queue.try_recv().is_err());
    c.transaction(|tx| tx.set_stream("book".into(), true))
        .unwrap();
    assert!(entry.try_recv().is_err());
    assert_eq!(
        c.last_changed(),
        &BTreeSet::from(["axton_client".to_string(), "axton_subscription".to_string()])
    );
}

#[test]
fn session_reads_own_writes_without_notifying_until_commit_and_blocks_other_writes() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "book");
    seed(&mut c, "A");
    let events = c.watch(BTreeSet::from(["Entry".to_string()]));
    c.begin_session().unwrap();
    c.session(|tx| {
        tx.enqueue(mutation("B"))?;
        Ok(())
    })
    .unwrap();
    assert_eq!(
        c.session(|tx| tx.read(&key())).unwrap().unwrap()["text"],
        "B"
    );
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "A");
    assert!(events.try_recv().is_err());
    assert!(
        c.apply_page(page("book", 0, 1, Some("X"))).is_err(),
        "no second transaction during a session"
    );
    c.session_savepoint().unwrap();
    c.session(|tx| tx.direct(update("C"))).unwrap();
    c.session_rollback_savepoint().unwrap();
    assert_eq!(
        c.session(|tx| tx.read(&key())).unwrap().unwrap()["text"],
        "B"
    );
    c.commit_session().unwrap();
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "B");
    assert!(events.try_recv().is_ok());
    c.begin_session().unwrap();
    c.session(|tx| tx.direct(update("Z"))).unwrap();
    c.rollback_session().unwrap();
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "B");
}

#[test]
fn stale_writer_cannot_overwrite_committed_database() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut a = open(&path);
    let mut b = open(&path);
    seed(&mut a, "A");
    let error = b
        .transaction(|tx| tx.direct(update("B")))
        .expect_err("the stale handle must be fenced out");
    assert!(
        error.to_string().contains("stale client writer"),
        "the write itself would succeed; only the fence refuses it: {error}"
    );
    assert_eq!(open(&path).read(&key()).unwrap().unwrap()["text"], "A");
}

#[test]
fn schema_cascade_is_optimistic_same_fate_and_not_extra_wire_operations() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = Client::open(
        SqliteStore::open(dir.path().join("db")).unwrap(),
        family_schema(),
    )
    .unwrap();
    c.transaction(|tx| {
        tx.direct(create("Book", "b", json!({"title":"Book"})))?;
        tx.direct(create("Comment", "c", json!({"bookId":"b","text":"hello"})))
    })
    .unwrap();
    c.transaction(|tx| {
        tx.enqueue(Mutation::new(
            "DeleteBook",
            vec![Operation {
                model: "Book".into(),
                op: OperationKind::Delete,
                identity: json!({"id":"b"}),
                values: None,
            }],
        ))?;
        Ok(())
    })
    .unwrap();
    assert!(c.query("Comment", &json!({})).unwrap().is_empty());
    assert_eq!(table_count(&mut c, "axton_before_Comment"), 1);
    let request = PushRequest::decode(&c.freeze().unwrap().unwrap()).unwrap();
    assert_eq!(
        request.raw["mutations"][0]["operations"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let ack = rejecting(&mut c, 1, &[1], "denied", vec![]);
    c.acknowledge(1, ack).unwrap();
    assert_eq!(c.query("Comment", &json!({})).unwrap().len(), 1);
    assert_eq!(table_count(&mut c, "axton_before_Comment"), 0);
}

#[test]
fn declared_unique_constraint_is_atomic() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = Client::open(
        SqliteStore::open(dir.path().join("db")).unwrap(),
        family_schema(),
    )
    .unwrap();
    let result = c.transaction(|tx| {
        tx.direct(create("Comment", "c1", json!({"bookId":"b","text":"same"})))?;
        tx.direct(create("Comment", "c2", json!({"bookId":"b","text":"same"})))
    });
    assert!(result.is_err());
    assert!(c.query("Comment", &json!({})).unwrap().is_empty());
}

#[test]
fn direct_cascade_handles_cyclic_relationships_once() {
    let dir = tempfile::tempdir().unwrap();
    let mut value = serde_json::to_value(family_schema()).unwrap();
    value["models"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"commentId","nullable":true,"type":{"kind":"scalar","name":"string"}}));
    value["models"][0]["relations"] = json!([{"name":"comment","target":"Comment","fields":["commentId"],"targetFields":["id"],"onDelete":"delete"}]);
    let mut c = Client::open(
        SqliteStore::open(dir.path().join("db")).unwrap(),
        Schema::from_value(value).unwrap(),
    )
    .unwrap();
    c.transaction(|tx| {
        tx.direct(create("Book", "b", json!({"title":"B","commentId":"c"})))?;
        tx.direct(create("Comment", "c", json!({"bookId":"b","text":"C"})))?;
        tx.direct(Operation {
            model: "Book".into(),
            op: OperationKind::Delete,
            identity: json!({"id":"b"}),
            values: None,
        })
    })
    .unwrap();
    assert!(c.query("Book", &json!({})).unwrap().is_empty());
    assert!(c.query("Comment", &json!({})).unwrap().is_empty());
}

#[test]
fn creating_then_editing_a_record_automatically_has_lifecycle_dependency() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    c.transaction(|tx| {
        tx.enqueue(Mutation::new(
            "Create",
            vec![create("Entry", "e", json!({"text":"A"}))],
        ))?;
        tx.enqueue(mutation("B"))?;
        Ok(())
    })
    .unwrap();
    let batch = PushRequest::decode(&c.freeze().unwrap().unwrap()).unwrap();
    assert_eq!(batch.mutations.len(), 1);
}

/// Unsubscribing restarts the scope's cursor and keeps every record it
/// delivered, whether or not another scope also provides it.
#[test]
fn unsubscribe_retains_records_and_restarts_from_zero() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    subscribe(&mut c, "b");
    c.apply_page(page("a", 0, 1, Some("A"))).unwrap();
    let mut newer = page("b", 0, 1, Some("B"));
    newer.changes[0].stamp = 2;
    c.apply_page(newer).unwrap();
    let mut other = page("a", 1, 2, Some("O"));
    other.changes[0].identity = json!({"id":"only-a"});
    c.apply_page(other).unwrap();
    c.transaction(|tx| tx.set_stream("a".into(), false))
        .unwrap();
    assert_eq!(
        c.cursor("a").unwrap(),
        None,
        "an unsubscribed scope has no delivery position"
    );
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "B");
    let only_a = schema()
        .record_key("Entry", &json!({"id":"only-a"}))
        .unwrap();
    assert_eq!(
        c.read(&only_a).unwrap().unwrap()["text"],
        "O",
        "a record only the unsubscribed scope delivered is retained"
    );
    assert_eq!(table_count(&mut c, "axton_record"), 2);
    assert_eq!(c.record_stamp(&only_a).unwrap(), 2);
    subscribe(&mut c, "a");
    assert_eq!(c.cursor("a").unwrap(), Some(0), "resubscribing starts over");
}

/// L3: a host transaction cannot commit with a savepoint still open; the refusal
/// rolls the whole transaction back and the client stays usable.
#[test]
fn committing_with_an_unclosed_savepoint_is_refused_and_rolls_back() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    c.begin_session().unwrap();
    c.session(|tx| tx.direct(create("Entry", "e", json!({"text":"inside","note":null}))))
        .unwrap();
    c.session_savepoint().unwrap();
    c.session(|tx| tx.enqueue(mutation("edited")).map(|_| ()))
        .unwrap();
    let err = c.commit_session().unwrap_err();
    assert!(err.to_string().contains("unclosed savepoint"), "{err}");
    assert!(!c.session_active(), "the refused commit closes the session");
    assert!(
        c.read(&key()).unwrap().is_none(),
        "nothing from the session committed"
    );
    assert_eq!(c.pending_count().unwrap(), 0);
    c.begin_session().unwrap();
    c.session(|tx| tx.direct(create("Entry", "e", json!({"text":"again","note":null}))))
        .unwrap();
    c.session_savepoint().unwrap();
    c.session_release().unwrap();
    c.commit_session().unwrap();
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "again");
}
