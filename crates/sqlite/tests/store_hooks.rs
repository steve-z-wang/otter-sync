mod common;
use axton_client::*;
use axton_client::{StoreChange, StoreDelivery};
use axton_sqlite::SqliteStore;
use common::*;
use serde_json::json;

#[test]
fn prepares_newer_authority_under_optimism_without_changing_visible_row() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    subscribe(&mut client, "a");
    client.apply_page(page("a", 0, 1, Some("base"))).unwrap();
    client
        .transaction(|tx| tx.enqueue(mutation("optimistic")))
        .unwrap();
    client.begin_session().unwrap();
    let prepared = client
        .prepare_store(StoreDelivery::Page(page("a", 1, 2, Some("server"))))
        .unwrap();
    assert_eq!(prepared.accepted(), &[0]);
    assert_eq!(
        prepared.changes()["Entry"],
        vec![StoreChange::Upsert {
            identity: json!({"id":"e"}),
            row: json!({"id":"e","text":"server","note":null}),
        }]
    );
    assert_eq!(
        client.session(|tx| tx.read(&key())).unwrap().unwrap()["text"],
        "optimistic"
    );
    assert_eq!(client.cursor("a").unwrap(), Some(1));
    let applied = client.apply_prepared_store(prepared).unwrap();
    assert_eq!(applied.as_page().unwrap().applied, 1);
    client.commit_session().unwrap();
    assert_eq!(client.cursor("a").unwrap(), Some(2));
    assert_eq!(client.read(&key()).unwrap().unwrap()["text"], "optimistic");
}

#[test]
fn repeated_identities_select_occurrences_and_preserve_known_failures() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    subscribe(&mut client, "a");
    client.apply_page(page("a", 0, 1, Some("base"))).unwrap();
    let mut malformed = authority_of("bad", Some("invalid"), 2);
    malformed.state = json!({"text": 99});
    let incoming = DirectActionResponse {
        completion: CallCompletion {
            call_id: "local-direct".into(),
            outcome: ActionOutcome::Succeeded {
                result: json!(null),
            },
        },
        records: vec![
            authority(Some("two"), 2),
            authority(Some("three"), 3),
            authority(Some("three"), 3),
            authority(Some("conflict"), 3),
            authority(Some("older"), 2),
            malformed,
            authority_of("valid", Some("good"), 2),
            authority(None, 4),
        ],
    };
    client.begin_session().unwrap();
    let prepared = client
        .prepare_store(StoreDelivery::Direct {
            response: incoming,
            snapshot: None,
        })
        .unwrap();
    assert_eq!(prepared.accepted(), &[0, 1, 6, 7]);
    assert_eq!(prepared.changes()["Entry"].len(), 4);
    assert_eq!(
        prepared.changes()["Entry"][3],
        StoreChange::Delete {
            identity: json!({"id":"e"})
        }
    );
    assert_eq!(
        client.session(|tx| tx.read(&key())).unwrap().unwrap()["text"],
        "base"
    );
    let report = client.apply_prepared_store(prepared).unwrap();
    let StoreResult::Direct(report) = report else {
        panic!("direct")
    };
    assert_eq!(report.applied, 4);
    assert_eq!(report.conflicts(), 1);
    assert_eq!(report.skipped(), 1);
    client.commit_session().unwrap();
    assert!(client.read(&key()).unwrap().is_none());
    assert_eq!(
        client
            .read(
                &schema()
                    .record_key("Entry", &json!({"id":"valid"}))
                    .unwrap()
            )
            .unwrap()
            .unwrap()["text"],
        "good"
    );
}

#[test]
fn receipt_preflight_leaves_queue_and_view_unchanged_until_replay() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    subscribe(&mut client, "a");
    client.apply_page(page("a", 0, 1, Some("base"))).unwrap();
    client
        .transaction(|tx| tx.enqueue(mutation("optimistic")))
        .unwrap();
    client.freeze().unwrap();
    let incoming = receipt(&mut client, 1, vec![authority(Some("server"), 2)]);
    client.begin_session().unwrap();
    let prepared = client
        .prepare_store(StoreDelivery::Receipt {
            sequence: 1,
            receipt: incoming,
        })
        .unwrap();
    assert_eq!(prepared.accepted(), &[0]);
    assert_eq!(client.pending_count().unwrap(), 1);
    assert_eq!(
        client.session(|tx| tx.read(&key())).unwrap().unwrap()["text"],
        "optimistic"
    );
    let result = client.apply_prepared_store(prepared).unwrap();
    match result {
        axton_client::StoreResult::Receipt(report) => assert_eq!(report.applied, 1),
        _ => panic!("receipt"),
    }
    client.commit_session().unwrap();
    assert_eq!(client.pending_count().unwrap(), 0);
    assert_eq!(client.read(&key()).unwrap().unwrap()["text"], "server");
}

#[test]
fn admitted_page_applies_after_unsubscribe_without_restoring_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    subscribe(&mut client, "a");
    client.begin_session().unwrap();
    let prepared = client
        .prepare_store(StoreDelivery::Page(page("a", 0, 1, Some("server"))))
        .unwrap();
    client
        .session(|tx| tx.set_channel("a".into(), false))
        .unwrap();
    let report = client.apply_prepared_store(prepared).unwrap();
    assert_eq!(report.as_page().unwrap().applied, 1);
    assert!(report.as_page().unwrap().cursors.is_empty());
    client.commit_session().unwrap();
    assert!(client.subscription_state("a").unwrap().is_none());
    assert_eq!(client.read(&key()).unwrap().unwrap()["text"], "server");
}

#[test]
fn callback_constraint_failure_aborts_entire_prepared_unit() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = Client::open(
        SqliteStore::open(dir.path().join("db")).unwrap(),
        family_schema(),
    )
    .unwrap();
    subscribe(&mut client, "a");
    client.begin_session().unwrap();
    let incoming = AuthorityRecord {
        model: "Comment".into(),
        identity: json!({"id":"server"}),
        stamp: 1,
        state: json!({"bookId":"b","text":"same"}),
        error: None,
    };
    let prepared = client
        .prepare_store(StoreDelivery::Page(multi(
            &[("a", 0, 1, 1)],
            vec![incoming],
        )))
        .unwrap();
    assert_eq!(prepared.accepted(), &[0]);
    client
        .session(|tx| {
            tx.direct(create(
                "Comment",
                "local",
                json!({"bookId":"b","text":"same"}),
            ))
        })
        .unwrap();
    assert!(client.apply_prepared_store(prepared).is_err());
    assert!(!client.session_active());
    assert!(client.query("Comment", &json!({})).unwrap().is_empty());
    assert_eq!(client.cursor("a").unwrap(), Some(0));
}

#[test]
fn parent_cascade_is_not_an_incoming_child_change() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = Client::open(
        SqliteStore::open(dir.path().join("db")).unwrap(),
        family_schema(),
    )
    .unwrap();
    subscribe(&mut client, "a");
    client
        .transaction(|tx| {
            tx.direct(create("Book", "b", json!({"title":"book"})))?;
            tx.direct(create("Comment", "c", json!({"bookId":"b","text":"child"})))
        })
        .unwrap();
    client.begin_session().unwrap();
    let parent = AuthorityRecord {
        model: "Book".into(),
        identity: json!({"id":"b"}),
        stamp: 1,
        state: serde_json::Value::Null,
        error: None,
    };
    let prepared = client
        .prepare_store(StoreDelivery::Page(multi(&[("a", 0, 1, 1)], vec![parent])))
        .unwrap();
    assert_eq!(
        prepared
            .changes()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        vec!["Book"]
    );
    assert_eq!(
        client
            .session(|tx| tx.query("Comment", &json!({})))
            .unwrap()
            .len(),
        1
    );
    client.apply_prepared_store(prepared).unwrap();
    client.commit_session().unwrap();
    assert!(client.query("Comment", &json!({})).unwrap().is_empty());
}

#[test]
fn preflight_rejected_row_is_not_newly_admitted_after_callback_write() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = Client::open(
        SqliteStore::open(dir.path().join("db")).unwrap(),
        family_schema(),
    )
    .unwrap();
    subscribe(&mut client, "a");
    client
        .transaction(|tx| {
            tx.direct(create(
                "Comment",
                "existing",
                json!({"bookId":"b","text":"same"}),
            ))
        })
        .unwrap();
    let incoming = AuthorityRecord {
        model: "Comment".into(),
        identity: json!({"id":"server"}),
        stamp: 1,
        state: json!({"bookId":"b","text":"same"}),
        error: None,
    };
    client.begin_session().unwrap();
    let prepared = client
        .prepare_store(StoreDelivery::Page(multi(
            &[("a", 0, 1, 1)],
            vec![incoming],
        )))
        .unwrap();
    assert!(prepared.accepted().is_empty());
    client
        .session(|tx| {
            tx.direct(Operation {
                model: "Comment".into(),
                op: OperationKind::Delete,
                identity: json!({"id":"existing"}),
                values: None,
            })
        })
        .unwrap();
    let result = client.apply_prepared_store(prepared).unwrap();
    assert_eq!(result.as_page().unwrap().skipped(), 1);
    client.commit_session().unwrap();
    assert!(client.query("Comment", &json!({})).unwrap().is_empty());
    assert_eq!(client.cursor("a").unwrap(), Some(1));
}

#[test]
fn bootstrap_preflight_retains_partial_failure_rules() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    let registration = client.ensure_subscription("a").unwrap();
    acknowledge(&mut client, &[("a", 2)]);
    let state = client
        .request_bootstrap("a", registration.subscription_id)
        .unwrap();
    let mut bad = authority_of("bad", Some("bad"), 1);
    bad.state = json!({"text": 42});
    let page = BootstrapPage {
        channel: "a".into(),
        from: 0,
        to: 1,
        until: 2,
        head: 2,
        records: vec![authority_of("good", Some("good"), 1), bad],
    };
    client.begin_session().unwrap();
    let prepared = client
        .prepare_store(StoreDelivery::Bootstrap {
            scope: "a".into(),
            subscription_id: registration.subscription_id,
            run: state.run,
            expected_after: 0,
            page,
        })
        .unwrap();
    assert_eq!(prepared.accepted(), &[0]);
    assert!(
        client
            .session(|tx| tx.query("Entry", &json!({})))
            .unwrap()
            .is_empty()
    );
    let StoreResult::Bootstrap(BootstrapApply::Failed { state, report }) =
        client.apply_prepared_store(prepared).unwrap()
    else {
        panic!("failed bootstrap")
    };
    assert_eq!(state.cursor, 0);
    assert_eq!(report.applied, 1);
    assert_eq!(report.skipped(), 1);
    client.commit_session().unwrap();
    assert_eq!(
        client
            .bootstrap_state("a", registration.subscription_id)
            .unwrap()
            .state,
        BootstrapPhase::Failed
    );
    assert_eq!(client.query("Entry", &json!({})).unwrap().len(), 1);
}

#[test]
fn bootstrap_authority_survives_unsubscribe_without_recreating_run() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    let registration = client.ensure_subscription("a").unwrap();
    acknowledge(&mut client, &[("a", 1)]);
    let state = client
        .request_bootstrap("a", registration.subscription_id)
        .unwrap();
    let page = BootstrapPage {
        channel: "a".into(),
        from: 0,
        to: 1,
        until: 1,
        head: 1,
        records: vec![authority(Some("server"), 1)],
    };
    client.begin_session().unwrap();
    let prepared = client
        .prepare_store(StoreDelivery::Bootstrap {
            scope: "a".into(),
            subscription_id: registration.subscription_id,
            run: state.run,
            expected_after: 0,
            page,
        })
        .unwrap();
    client
        .session(|tx| tx.set_channel("a".into(), false))
        .unwrap();
    client.apply_prepared_store(prepared).unwrap();
    client.commit_session().unwrap();
    assert!(client.subscription_state("a").unwrap().is_none());
    assert_eq!(client.read(&key()).unwrap().unwrap()["text"], "server");
}

#[test]
fn duplicate_wire_page_is_rejected_before_preparation_changes_anything() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    subscribe(&mut client, "a");
    client.begin_session().unwrap();
    let incoming = multi(
        &[("a", 0, 1, 1)],
        vec![authority(Some("first"), 1), authority(Some("second"), 2)],
    );
    assert!(client.prepare_store(StoreDelivery::Page(incoming)).is_err());
    assert!(client.session(|tx| tx.read(&key())).unwrap().is_none());
    client.rollback_session().unwrap();
    assert_eq!(client.cursor("a").unwrap(), Some(0));
}

#[test]
fn preflight_does_not_consume_outstanding_pull_identity() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    subscribe(&mut client, "a");
    client.downlink_request().unwrap().expect("issued pull");
    client.begin_session().unwrap();
    client
        .prepare_store(StoreDelivery::Page(page("a", 0, 1, Some("old-response"))))
        .unwrap();
    client.rollback_session().unwrap();
    client
        .transaction(|tx| tx.set_channel("a".into(), false))
        .unwrap();
    subscribe(&mut client, "a");
    let report = client
        .apply_page(page("a", 0, 1, Some("old-response")))
        .unwrap();
    assert!(report.stale);
    assert!(client.read(&key()).unwrap().is_none());
}
