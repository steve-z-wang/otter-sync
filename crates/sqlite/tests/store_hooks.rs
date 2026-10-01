mod common;
use axton_client::*;
use axton_client::{StoreChange, StoreDelivery};
use axton_sqlite::SqliteStore;
use common::*;
use serde_json::Value;
use serde_json::json;
use std::cell::Cell;
use std::rc::Rc;

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
        memberships: Vec::new(),
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
        .session(|tx| tx.set_stream("a".into(), false))
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
        stream: "a".into(),
        from: 0,
        to: 1,
        until: 2,
        head: 2,
        records: vec![authority_of("good", Some("good"), 1), bad],
    };
    client.begin_session().unwrap();
    let prepared = client
        .prepare_store(StoreDelivery::Bootstrap {
            stream: "a".into(),
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
        stream: "a".into(),
        from: 0,
        to: 1,
        until: 1,
        head: 1,
        records: vec![authority(Some("server"), 1)],
    };
    client.begin_session().unwrap();
    let prepared = client
        .prepare_store(StoreDelivery::Bootstrap {
            stream: "a".into(),
            subscription_id: registration.subscription_id,
            run: state.run,
            expected_after: 0,
            page,
        })
        .unwrap();
    client
        .session(|tx| tx.set_stream("a".into(), false))
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
        .transaction(|tx| tx.set_stream("a".into(), false))
        .unwrap();
    subscribe(&mut client, "a");
    let report = client
        .apply_page(page("a", 0, 1, Some("old-response")))
        .unwrap();
    assert!(report.stale);
    assert!(client.read(&key()).unwrap().is_none());
}

#[test]
fn rolled_back_caller_savepoint_does_not_consume_prepared_pull() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    subscribe(&mut client, "a");
    client.downlink_request().unwrap().expect("issued pull");
    client.begin_session().unwrap();
    client.session_savepoint().unwrap();
    let prepared = client
        .prepare_store(StoreDelivery::Page(page("a", 0, 1, Some("old-response"))))
        .unwrap();
    assert_eq!(
        client
            .apply_prepared_store(prepared)
            .unwrap()
            .as_page()
            .unwrap()
            .applied,
        1
    );
    client.session_rollback_savepoint().unwrap();
    client.commit_session().unwrap();
    assert!(client.read(&key()).unwrap().is_none());
    assert_eq!(client.cursor("a").unwrap(), Some(0));
    assert!(!client.last_changed().contains("Entry"));
    client
        .transaction(|tx| tx.set_stream("a".into(), false))
        .unwrap();
    subscribe(&mut client, "a");
    let report = client
        .apply_page(page("a", 0, 1, Some("old-response")))
        .unwrap();
    assert!(
        report.stale,
        "rolled-back response must retain its old pull epoch"
    );
    assert!(client.read(&key()).unwrap().is_none());
}

#[test]
fn released_caller_savepoint_keeps_prepared_pull_for_commit() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    subscribe(&mut client, "a");
    client.downlink_request().unwrap().expect("issued pull");
    client.begin_session().unwrap();
    client.session_savepoint().unwrap();
    let prepared = client
        .prepare_store(StoreDelivery::Page(page("a", 0, 1, Some("first"))))
        .unwrap();
    client.apply_prepared_store(prepared).unwrap();
    client.session_release().unwrap();
    client.commit_session().unwrap();
    assert_eq!(client.read(&key()).unwrap().unwrap()["text"], "first");
    client
        .transaction(|tx| tx.set_stream("a".into(), false))
        .unwrap();
    subscribe(&mut client, "a");
    let mut second = page("a", 0, 1, Some("second"));
    second.changes[0].stamp = 2;
    let report = client.apply_page(second).unwrap();
    assert!(
        !report.stale,
        "released page must consume the old pull at commit"
    );
    assert_eq!(client.read(&key()).unwrap().unwrap()["text"], "second");
}

struct FaultStore {
    sqlite: SqliteStore,
    enabled: Rc<Cell<bool>>,
    failed: Rc<Cell<bool>>,
    post_fault_held_reads: Rc<Cell<usize>>,
}

impl ClientStore for FaultStore {
    fn begin(&mut self) -> Result<()> {
        self.sqlite.begin()
    }
    fn commit(&mut self) -> Result<()> {
        self.sqlite.commit()
    }
    fn rollback(&mut self) -> Result<()> {
        self.sqlite.rollback()
    }
    fn savepoint(&mut self, name: &str) -> Result<()> {
        self.sqlite.savepoint(name)
    }
    fn release(&mut self, name: &str) -> Result<()> {
        self.sqlite.release(name)
    }
    fn rollback_to(&mut self, name: &str) -> Result<()> {
        self.sqlite.rollback_to(name)
    }
    fn execute(&mut self, sql: &str, parameters: &[Value]) -> Result<usize> {
        // The dirty Entry has already been staged into its before image and
        // inserted into Held when storing its stamp reaches this boundary.
        if self.enabled.get()
            && !self.failed.get()
            && sql.starts_with("INSERT INTO axton_record")
            && parameters.get(2) == Some(&json!(2))
        {
            self.failed.set(true);
            return Err(invalid("injected stamp write failure after Held insert"));
        }
        self.sqlite.execute(sql, parameters)
    }
    fn execute_batch(&mut self, sql: &str) -> Result<()> {
        self.sqlite.execute_batch(sql)
    }
    fn query(&mut self, sql: &str, parameters: &[Value]) -> Result<SqlRows> {
        if self.failed.get()
            && sql.contains("FROM \"axton_before_Entry\"")
            && parameters == [json!("e")]
        {
            self.post_fault_held_reads
                .set(self.post_fault_held_reads.get() + 1);
        }
        self.sqlite.query(sql, parameters)
    }
    fn query_committed(&mut self, sql: &str, parameters: &[Value]) -> Result<SqlRows> {
        self.sqlite.query_committed(sql, parameters)
    }
}

#[test]
fn failed_record_savepoint_discards_held_key_before_later_record() {
    let dir = tempfile::tempdir().unwrap();
    let enabled = Rc::new(Cell::new(false));
    let failed = Rc::new(Cell::new(false));
    let post_fault_held_reads = Rc::new(Cell::new(0));
    let store = FaultStore {
        sqlite: SqliteStore::open(dir.path().join("db")).unwrap(),
        enabled: enabled.clone(),
        failed: failed.clone(),
        post_fault_held_reads: post_fault_held_reads.clone(),
    };
    let mut client = Client::open(store, schema()).unwrap();
    client
        .transaction(|tx| {
            tx.direct(create("Entry", "e", json!({"text":"base","note":null})))?;
            tx.enqueue(mutation("optimistic"))?;
            Ok(())
        })
        .unwrap();
    enabled.set(true);
    let response = DirectActionResponse {
        completion: CallCompletion {
            call_id: "fault-case".into(),
            outcome: ActionOutcome::Succeeded {
                result: Value::Null,
            },
        },
        records: vec![
            authority(Some("server"), 2),
            authority_of("valid", Some("good"), 3),
        ],
        memberships: Vec::new(),
    };
    client.begin_session().unwrap();
    let prepared = client
        .prepare_store(StoreDelivery::Direct {
            response,
            snapshot: None,
        })
        .unwrap();
    assert!(
        failed.get(),
        "the injected fault must fire after Held insertion"
    );
    assert_eq!(prepared.accepted(), &[1]);
    assert_eq!(
        post_fault_held_reads.get(),
        0,
        "failed key must not be replayed from Held"
    );
    assert_eq!(
        client.session(|tx| tx.read(&key())).unwrap().unwrap()["text"],
        "optimistic"
    );
    let StoreResult::Direct(report) = client.apply_prepared_store(prepared).unwrap() else {
        panic!("direct")
    };
    assert_eq!(report.skipped(), 1);
    assert_eq!(report.applied, 1);
    client.commit_session().unwrap();
    assert_eq!(client.read(&key()).unwrap().unwrap()["text"], "optimistic");
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

/// A stored Fetch response of `Entry e`: its snapshot and matching authority.
fn fetch_response(text: Option<&str>, stamp: u64) -> FetchResponse {
    FetchResponse {
        completion: CallCompletion {
            call_id: "123e4567-e89b-42d3-a456-426614174000".into(),
            outcome: ActionOutcome::Succeeded {
                result: text.map_or(Value::Null, |t| json!({"id":"e","text":t,"note":null})),
            },
        },
        records: vec![authority(text, stamp)],
    }
}

/// A Fetch is one single-record delivery: preflight selects its one change
/// for the callback, an older stamp selects none, and an equal-stamp
/// conflict refuses the whole delivery before any callback could open,
/// leaving the session untouched.
#[test]
fn fetch_delivery_prepares_its_one_change_and_refuses_a_conflict_before_the_hook() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    client
        .apply_fetch_response(&fetch_response(Some("base"), 2))
        .unwrap();
    client.begin_session().unwrap();
    let error = client
        .prepare_store(StoreDelivery::Fetch {
            response: fetch_response(Some("other"), 2),
        })
        .err()
        .expect("an equal-stamp conflict refuses the delivery");
    assert!(error.to_string().contains("conflicts"), "{error}");
    let older = client
        .prepare_store(StoreDelivery::Fetch {
            response: fetch_response(Some("old"), 1),
        })
        .unwrap();
    assert!(older.changes().is_empty());
    let prepared = client
        .prepare_store(StoreDelivery::Fetch {
            response: fetch_response(Some("server"), 3),
        })
        .unwrap();
    assert_eq!(
        prepared.changes()["Entry"],
        vec![StoreChange::Upsert {
            identity: json!({"id":"e"}),
            row: json!({"id":"e","text":"server","note":null}),
        }]
    );
    assert_eq!(
        client.session(|tx| tx.read(&key())).unwrap().unwrap()["text"],
        "base"
    );
    let StoreResult::Fetch(report) = client.apply_prepared_store(prepared).unwrap() else {
        panic!("fetch")
    };
    assert_eq!(report.applied, 1);
    assert_eq!(report.completions.len(), 1);
    client.commit_session().unwrap();
    assert_eq!(client.read(&key()).unwrap().unwrap()["text"], "server");
    assert_eq!(client.record_stamp(&key()).unwrap(), 3);
    // Stamped absence prepares a deletion.
    client.begin_session().unwrap();
    let absent = client
        .prepare_store(StoreDelivery::Fetch {
            response: fetch_response(None, 4),
        })
        .unwrap();
    assert_eq!(
        absent.changes()["Entry"],
        vec![StoreChange::Delete {
            identity: json!({"id":"e"})
        }]
    );
    client.rollback_session().unwrap();
}

/// A record the local store cannot write is reported and skipped in a
/// scope delivery, but a Fetch rejects instead of succeeding from the
/// server envelope alone, and nothing it staged remains.
#[test]
fn fetch_refuses_a_record_the_store_cannot_write_and_keeps_the_row() {
    let dir = tempfile::tempdir().unwrap();
    let enabled = Rc::new(Cell::new(false));
    let failed = Rc::new(Cell::new(false));
    let store = FaultStore {
        sqlite: SqliteStore::open(dir.path().join("db")).unwrap(),
        enabled: enabled.clone(),
        failed: failed.clone(),
        post_fault_held_reads: Rc::new(Cell::new(0)),
    };
    let mut client = Client::open(store, schema()).unwrap();
    client
        .transaction(|tx| tx.direct(create("Entry", "e", json!({"text":"base","note":null}))))
        .unwrap();
    enabled.set(true);
    let error = client
        .apply_fetch_response(&fetch_response(Some("server"), 2))
        .expect_err("the skipped record refuses the Fetch");
    assert!(failed.get(), "the injected stamp write fault fired");
    assert!(error.to_string().contains("could not be stored"), "{error}");
    assert_eq!(client.read(&key()).unwrap().unwrap()["text"], "base");
    assert_eq!(client.record_stamp(&key()).unwrap(), 0);
    // Preflight refuses it the same way, before any callback.
    failed.set(false);
    client.begin_session().unwrap();
    assert!(
        client
            .prepare_store(StoreDelivery::Fetch {
                response: fetch_response(Some("server"), 2),
            })
            .is_err()
    );
    client.rollback_session().unwrap();
    assert_eq!(client.read(&key()).unwrap().unwrap()["text"], "base");
}

/// A store session is incoming authority's: whatever handle its hooks write
/// through, it submits no Mutation and records no companion, while its local
/// writes and the delivery still commit. An application session keeps both.
#[test]
fn a_store_session_submits_no_mutation_and_records_no_companion() {
    let dir = tempfile::tempdir().unwrap();
    let mut raw = serde_json::to_value(schema()).unwrap();
    raw["actions"] = json!([{"name":"Ping","version":1,"inputs":[],"outputs":[]}]);
    let mut client = Client::open(
        SqliteStore::open(dir.path().join("db")).unwrap(),
        Schema::from_value(raw).unwrap(),
    )
    .unwrap();
    subscribe(&mut client, "a");
    client.begin_session().unwrap();
    let submitted = client
        .session(|tx| tx.submit_mutation("Ping", 1, json!({}), ActionCallOptions::default()))
        .unwrap();
    client.rollback_session().unwrap();
    client.begin_session().unwrap();
    let prepared = client
        .prepare_store(StoreDelivery::Page(page("a", 0, 1, Some("server"))))
        .unwrap();
    let refused = client
        .session(|tx| tx.submit_mutation("Ping", 1, json!({}), ActionCallOptions::default()))
        .unwrap_err();
    assert_eq!(refused.to_string(), "store hook cannot submit a Mutation");
    let refused = client
        .session(|tx| tx.append_companion(submitted.ordinal, update("x")))
        .unwrap_err();
    assert_eq!(refused.to_string(), "store hook cannot submit a Mutation");
    client
        .session(|tx| {
            tx.direct(Operation {
                model: "Entry".into(),
                op: OperationKind::Create,
                identity: json!({"id":"local"}),
                values: Some(json!({"text":"hook","note":null})),
            })
        })
        .unwrap();
    client.apply_prepared_store(prepared).unwrap();
    client.commit_session().unwrap();
    assert_eq!(client.pending_count().unwrap(), 0);
    assert_eq!(client.read(&key()).unwrap().unwrap()["text"], "server");
    let local = RecordKey {
        model: "Entry".into(),
        identity: json!({"id":"local"}),
    };
    assert_eq!(client.read(&local).unwrap().unwrap()["text"], "hook");
    assert_eq!(client.cursor("a").unwrap(), Some(1));
}

#[test]
fn scope_release_notifies_local_observers_only_after_commit() {
    use std::collections::{BTreeMap, BTreeSet};
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    subscribe(&mut client, "a");
    client
        .apply_stream_page(StreamPullPage {
            cursors: BTreeMap::from([(
                "a".into(),
                CursorRange {
                    from: 0,
                    to: 1,
                    head: 1,
                },
            )]),
            changes: vec![StreamChange::Upsert {
                stream: "a".into(),
                cursor: 1,
                record: authority(Some("base"), 7),
            }],
        })
        .unwrap();
    let observer = client.watch(BTreeSet::from(["Entry".into()]));
    client.begin_session().unwrap();
    let prepared = client
        .prepare_store(StoreDelivery::StreamPage(StreamPullPage {
            cursors: BTreeMap::from([(
                "a".into(),
                CursorRange {
                    from: 1,
                    to: 2,
                    head: 2,
                },
            )]),
            changes: vec![StreamChange::Remove {
                stream: "a".into(),
                cursor: 2,
                key: key(),
            }],
        }))
        .unwrap();
    assert!(prepared.accepted().is_empty());
    assert!(prepared.changes().is_empty());
    assert!(observer.try_recv().is_err());
    client.apply_prepared_store(prepared).unwrap();
    assert!(observer.try_recv().is_err());
    assert!(client.read(&key()).unwrap().is_some());
    client.commit_session().unwrap();
    assert!(observer.try_recv().is_ok());
    assert!(client.read(&key()).unwrap().is_none());
}

#[test]
fn prepared_scope_bootstrap_detaches_after_hook_replaces_registration() {
    for removal_only in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut c = open(&dir.path().join("db"));
        c.transaction(|tx| tx.set_stream("a".into(), true)).unwrap();
        acknowledge(&mut c, &[("a", 1)]);
        let old = c.subscription_state("a").unwrap().unwrap().subscription_id;
        let run = c.request_bootstrap("a", old).unwrap().run;
        if removal_only {
            c.apply_page(page("a", 1, 2, Some("cached"))).unwrap();
        }
        c.begin_session().unwrap();
        let change = if removal_only {
            StreamChange::Remove {
                stream: "a".into(),
                cursor: 1,
                key: key(),
            }
        } else {
            StreamChange::Upsert {
                stream: "a".into(),
                cursor: 1,
                record: authority(Some("admitted"), 7),
            }
        };
        let prepared = c
            .prepare_store(StoreDelivery::StreamBootstrap {
                stream: "a".into(),
                subscription_id: old,
                run,
                expected_after: 0,
                page: StreamBootstrapPage {
                    stream: "a".into(),
                    from: 0,
                    to: 1,
                    until: 1,
                    head: 2,
                    changes: vec![change],
                },
            })
            .unwrap();
        assert_eq!(prepared.accepted().len(), usize::from(!removal_only));
        c.session(|tx| {
            tx.set_stream("a".into(), false)?;
            tx.set_stream("a".into(), true)?;
            tx.direct(create(
                "Entry",
                "hook",
                json!({"text":"hook committed","note":null}),
            ))
        })
        .unwrap();
        let result = c.apply_prepared_store(prepared).unwrap();
        c.commit_session().unwrap();
        assert!(matches!(
            result,
            StoreResult::Bootstrap(BootstrapApply::Detached { .. })
        ));
        let new = c.subscription_state("a").unwrap().unwrap();
        assert_ne!(new.subscription_id, old);
        assert_eq!(new.cursor, None);
        assert_eq!(new.starting_cursor, None);
        let state = c.bootstrap_state("a", new.subscription_id).unwrap();
        assert_eq!(state.cursor, 0);
        assert_eq!(state.barrier, None);
        assert_eq!(state.state, BootstrapPhase::NotRequested);
        assert!(c.bootstrap_state("a", old).is_err());
        let member = c
            .read_sql(
                "SELECT cursor,present FROM axton_stream_member WHERE stream='a'",
                &[],
            )
            .unwrap();
        assert_eq!(member[0]["cursor"], 1);
        assert_eq!(member[0]["present"], u8::from(!removal_only));
        assert!(
            c.read(&schema().record_key("Entry", &json!({"id":"hook"})).unwrap())
                .unwrap()
                .is_some()
        );
        if removal_only {
            assert!(c.read(&key()).unwrap().is_none());
        } else {
            assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "admitted");
        }
    }
}
#[test]
fn prepared_scope_page_keeps_admitted_authority_after_hook_replaces_registration() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    let old = c.subscription_state("a").unwrap().unwrap().subscription_id;
    c.begin_session().unwrap();
    let prepared = c
        .prepare_store(StoreDelivery::StreamPage(StreamPullPage {
            cursors: std::collections::BTreeMap::from([(
                "a".into(),
                CursorRange {
                    from: 0,
                    to: 1,
                    head: 1,
                },
            )]),
            changes: vec![StreamChange::Upsert {
                stream: "a".into(),
                cursor: 1,
                record: authority(Some("admitted"), 7),
            }],
        }))
        .unwrap();
    c.session(|tx| {
        tx.set_stream("a".into(), false)?;
        tx.set_stream("a".into(), true)
    })
    .unwrap();
    let result = c.apply_prepared_store(prepared).unwrap();
    c.commit_session().unwrap();
    assert!(result.as_page().unwrap().cursors.is_empty());
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "admitted");
    let new = c.subscription_state("a").unwrap().unwrap();
    assert_ne!(new.subscription_id, old);
    assert_eq!(new.cursor, None);
    let member = c
        .read_sql(
            "SELECT cursor,present FROM axton_stream_member WHERE stream='a'",
            &[],
        )
        .unwrap();
    assert_eq!(member[0]["cursor"], 1);
    assert_eq!(member[0]["present"], 1);
}
