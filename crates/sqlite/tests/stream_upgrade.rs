//! Production Stream negotiation and additive retained-replica upgrade.
mod common;
use axton_client::*;
use common::*;
use serde_json::{Value, json};
use std::collections::BTreeMap;

fn capable(body: &str) {
    let value: Value = serde_json::from_str(body).unwrap();
    assert!(
        read_capabilities(&value)
            .unwrap()
            .contains(STREAM_AUTHORITY_CAPABILITY),
        "missing capability in {body}"
    );
}

#[test]
fn production_pull_and_live_advertise_stream_membership_after_encoding() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    let state = client.ensure_subscription("a").unwrap();
    client
        .initialize_subscriptions(
            &BTreeMap::from([("a".into(), state.subscription_id)]),
            &BTreeMap::from([("a".into(), 0)]),
        )
        .unwrap();
    capable(&client.downlink_request().unwrap().unwrap());
    let mut live = LiveSession::default();
    capable(
        &live
            .begin(vec!["a".into()], BTreeMap::from([("Entry".into(), 1)]), 0)
            .unwrap()
            .1,
    );
}

#[test]
fn production_http_cycle_consumes_membership_removals() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    let state = client.ensure_subscription("a").unwrap();
    client
        .initialize_subscriptions(
            &BTreeMap::from([("a".into(), state.subscription_id)]),
            &BTreeMap::from([("a".into(), 0)]),
        )
        .unwrap();
    let mut cycle = SyncCycle::default();
    cycle.next(&mut client).unwrap().unwrap();
    let body = json!({"cursors":{"a":{"from":0,"to":1,"head":1}},"changes":[{"kind":"remove","stream":"a","cursor":1,"model":"Entry","identity":{"id":"e"}}]});
    cycle
        .complete(&mut client, body.to_string().as_bytes())
        .unwrap();
    assert_eq!(client.cursor("a").unwrap(), Some(1));
}

#[test]
fn production_live_worker_consumes_membership_removals() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    let (epoch, _) = lane.streaming("a", 0);
    let body = json!({"cursors":{"a":{"from":0,"to":1,"head":1}},"changes":[{"kind":"remove","stream":"a","cursor":1,"model":"Entry","identity":{"id":"e"}}]});
    lane.message(epoch, body.to_string());
    assert_eq!(lane.client.cursor("a").unwrap(), Some(1));
}

#[test]
fn low_level_frozen_push_exports_capable_stable_wire_copy() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut client = open(&path);
    client
        .transaction(|tx| {
            tx.direct(Operation {
                model: "Entry".into(),
                op: OperationKind::Create,
                identity: json!({"id":"e"}),
                values: Some(json!({"text":"base","note":null})),
            })
        })
        .unwrap();
    client
        .transaction(|tx| tx.enqueue(mutation("pending")))
        .unwrap();
    let first = client.freeze().unwrap().unwrap();
    capable(std::str::from_utf8(&first).unwrap());
    drop(client);
    let mut client = open(&path);
    assert_eq!(first, client.freeze().unwrap().unwrap());
    assert_eq!(
        client
            .read_sql("SELECT store_epoch FROM axton_mutation", &[])
            .unwrap()[0]["store_epoch"],
        0
    );
}

#[test]
fn resumed_holding_evidence_schedules_own_fixed_history_bound() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open(&path);
    let s = c.ensure_subscription("a").unwrap();
    c.initialize_subscriptions(
        &BTreeMap::from([("a".into(), s.subscription_id)]),
        &BTreeMap::from([("a".into(), 0)]),
    )
    .unwrap();
    let p = json!({"cursors":{"a":{"from":0,"to":1,"head":1}},"changes":[{"kind":"upsert","stream":"a","cursor":1,"model":"Entry","identity":{"id":"e"},"stamp":1,"state":{"text":"held","note":null}}]});
    c.apply_stream_page(StreamPullPage::decode(p.to_string().as_bytes()).unwrap())
        .unwrap();
    c.remove_subscription("a", s.subscription_id).unwrap();
    assert!(c.read(&key()).unwrap().is_some());
    let resumed = c.ensure_subscription("a").unwrap();
    c.initialize_subscriptions(
        &BTreeMap::from([("a".into(), resumed.subscription_id)]),
        &BTreeMap::from([("a".into(), 20)]),
    )
    .unwrap();
    let task = c
        .bootstrap_schedule(None)
        .unwrap()
        .expect("retained membership requires reconciliation");
    assert_eq!((task.origin, task.state.cursor), (20, 0));
    drop(c);
    let mut c = open(&path);
    c.initialize_subscriptions(
        &BTreeMap::from([("a".into(), resumed.subscription_id)]),
        &BTreeMap::from([("a".into(), 30)]),
    )
    .unwrap();
    assert_eq!(
        c.bootstrap_schedule(None).unwrap().unwrap().origin,
        20,
        "reopen must retain observed bound"
    );
    assert_eq!(c.cursor("a").unwrap(), Some(20));
    let task = c.bootstrap_schedule(None).unwrap().unwrap();
    let response = StreamBootstrapPage::decode(json!({"mode":"bootstrap","stream":"a","from":0,"to":20,"until":20,"head":20,"changes":[{"kind":"remove","stream":"a","cursor":2,"model":"Entry","identity":{"id":"e"}}]}).to_string().as_bytes()).unwrap();
    let applied = c.apply_stream_bootstrap_task(task, &response).unwrap();
    assert!(matches!(applied, BootstrapApply::Applied { .. }));
    assert!(c.read(&key()).unwrap().is_none());
    assert!(c.bootstrap_schedule(None).unwrap().is_none());
    let row = &c
        .read_sql(
            "SELECT reconcile_state, bootstrap_state, cursor FROM axton_subscription",
            &[],
        )
        .unwrap()[0];
    assert_eq!(row["reconcile_state"], "complete");
    assert_eq!(row["bootstrap_state"], "not_requested");
    assert_eq!(row["cursor"], 20);
}

#[test]
fn new_push_batch_budget_counts_negotiation_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    for id in ["e", "f"] {
        c.transaction(|tx| {
            tx.enqueue(Mutation::new(
                "Edit",
                vec![Operation {
                    model: "Entry".into(),
                    op: OperationKind::Create,
                    identity: json!({"id":id}),
                    values: Some(json!({"text":"small","note":null})),
                }],
            ))
        })
        .unwrap();
    }
    let logical = json!({"clientId":c.client_id(),"batchSequence":1,"models":{"Entry":1},"mutations":[{"name":"Edit","version":1,"ordinal":1,"operations":[{"model":"Entry","op":"create","identity":{"id":"e"},"values":{"text":"small","note":null}}]},{"name":"Edit","version":1,"ordinal":2,"operations":[{"model":"Entry","op":"create","identity":{"id":"f"},"values":{"text":"small","note":null}}]}]});
    let max = canonical_json(&logical).unwrap().len();
    let bytes = c.freeze_with_limit(max).unwrap().unwrap();
    assert!(
        bytes.len() <= max,
        "final wire copy exceeds requested budget"
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&bytes).unwrap()["mutations"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn legacy_upgrade_preserves_queue_load_and_ordinary_cursor_without_fabricating_holds() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = Client::open(
        axton_sqlite::SqliteStore::open(&path).unwrap(),
        load_schema(),
    )
    .unwrap();
    let s = c.ensure_subscription("a").unwrap();
    c.initialize_subscriptions(
        &BTreeMap::from([("a".into(), s.subscription_id)]),
        &BTreeMap::from([("a".into(), 5)]),
    )
    .unwrap();
    c.apply_page(page("a", 5, 12, Some("legacy"))).unwrap();
    c.transaction(|tx| tx.enqueue(mutation("pending"))).unwrap();
    let frozen = c.freeze().unwrap().unwrap();
    let load = c
        .start_load("Recent", 1, &json!({}), LoadOptions::default())
        .unwrap()
        .job;
    drop(c);
    let mut raw = axton_sqlite::SqliteStore::open(&path).unwrap();
    for column in [
        "reconcile_state",
        "reconcile_run",
        "reconcile_cursor",
        "reconcile_bound",
        "reconcile_barrier",
        "reconcile_error",
    ] {
        raw.execute_batch(&format!(
            "ALTER TABLE axton_subscription DROP COLUMN {column}"
        ))
        .unwrap();
    }
    raw.execute_batch("ALTER TABLE axton_client DROP COLUMN stream_membership_version")
        .unwrap();
    drop(raw);
    let mut c = Client::open(
        axton_sqlite::SqliteStore::open(&path).unwrap(),
        load_schema(),
    )
    .unwrap();
    assert_eq!(c.freeze().unwrap().unwrap(), frozen);
    assert_eq!(c.cursor("a").unwrap(), Some(12));
    assert_eq!(
        c.subscription_state("a").unwrap().unwrap().starting_cursor,
        Some(5)
    );
    assert_eq!(
        c.read_sql("SELECT count(*) AS n FROM axton_stream_member", &[])
            .unwrap()[0]["n"],
        0
    );
    assert_eq!(
        c.read_sql(
            "SELECT call_id FROM axton_load WHERE load_id=?",
            &[json!(load.id)]
        )
        .unwrap()[0]["call_id"],
        json!(load.call_id)
    );
    c.initialize_subscriptions(
        &BTreeMap::from([("a".into(), s.subscription_id)]),
        &BTreeMap::from([("a".into(), 30)]),
    )
    .unwrap();
    let task = c.bootstrap_schedule(None).unwrap().unwrap();
    assert_eq!((task.origin, task.state.cursor), (30, 0));
    assert_eq!(c.cursor("a").unwrap(), Some(12));
}

#[test]
fn http_only_upgrade_reconciles_before_cycle_completion_without_an_ack() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open(&path);
    let s = c.ensure_subscription("a").unwrap();
    c.initialize_subscriptions(
        &BTreeMap::from([("a".into(), s.subscription_id)]),
        &BTreeMap::from([("a".into(), 5)]),
    )
    .unwrap();
    c.apply_page(page("a", 5, 12, Some("legacy"))).unwrap();
    drop(c);
    let mut raw = axton_sqlite::SqliteStore::open(&path).unwrap();
    raw.execute_batch("UPDATE axton_client SET stream_membership_version=0")
        .unwrap();
    drop(raw);
    let mut c = open(&path);
    let mut cycle = SyncCycle::default();
    let request = cycle.next(&mut c).unwrap().unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&request.body).unwrap()["cursors"]["a"],
        12
    );
    cycle
        .complete(
            &mut c,
            json!({"cursors":{"a":{"from":12,"to":12,"head":12}},"changes":[]})
                .to_string()
                .as_bytes(),
        )
        .unwrap();
    let request = cycle
        .next(&mut c)
        .unwrap()
        .expect("history pending before completion");
    let request: Value = serde_json::from_str(&request.body).unwrap();
    assert_eq!(
        (request["after"].clone(), request["until"].clone()),
        (json!(0), json!(12))
    );
    cycle.complete(&mut c,json!({"mode":"bootstrap","stream":"a","from":0,"to":12,"until":12,"head":20,"changes":[{"kind":"remove","stream":"a","cursor":9,"model":"Entry","identity":{"id":"e"}}]}).to_string().as_bytes()).unwrap();
    assert_eq!(c.cursor("a").unwrap(), Some(12));
    assert!(c.read(&key()).unwrap().is_none());
    drop(c);
    let mut c = open(&path);
    let mut cycle = SyncCycle::default();
    let request = cycle.next(&mut c).unwrap().unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&request.body).unwrap()["cursors"]["a"],
        12
    );
    cycle
        .complete(
            &mut c,
            json!({"cursors":{"a":{"from":12,"to":20,"head":20}},"changes":[]})
                .to_string()
                .as_bytes(),
        )
        .unwrap();
    assert!(cycle.next(&mut c).unwrap().is_none());
    assert_eq!(
        c.read_sql("SELECT reconcile_state FROM axton_subscription", &[])
            .unwrap()[0]["reconcile_state"],
        "complete"
    );
}

#[test]
fn hidden_reconciliation_failure_retries_on_bounded_timer_with_same_fixed_bound() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open(&path);
    let s = c.ensure_subscription("a").unwrap();
    c.initialize_subscriptions(
        &BTreeMap::from([("a".into(), s.subscription_id)]),
        &BTreeMap::from([("a".into(), 1)]),
    )
    .unwrap();
    drop(c);
    let mut raw = axton_sqlite::SqliteStore::open(&path).unwrap();
    raw.execute_batch("UPDATE axton_client SET stream_membership_version=0")
        .unwrap();
    drop(raw);
    let mut lane = Lane::of(open(&path));
    let (_, actions) = lane.streaming("a", 4);
    let (history, body) = actions
        .iter()
        .find_map(|a| match a {
            DownlinkAction::Request {
                request,
                body,
                bootstrap: true,
            } => Some((*request, body.clone())),
            _ => None,
        })
        .unwrap();
    assert_eq!(serde_json::from_str::<Value>(&body).unwrap()["until"], 4);
    let actions=lane.send(DownlinkEvent::Response { request:history,body:json!({"mode":"bootstrap","stream":"a","from":0,"to":2,"until":4,"head":4,"changes":[{"kind":"upsert","stream":"a","cursor":2,"model":"Entry","identity":{"id":"e"},"stamp":2,"state":{"text":99,"note":null}}]}).to_string() });
    assert!(
        actions.iter().any(
            |a| matches!(a,DownlinkAction::Reconciliation(s) if s.state==BootstrapPhase::Failed)
        )
    );
    assert!(!lane.drain().iter().any(|a| matches!(
        a,
        DownlinkAction::Request {
            bootstrap: true,
            ..
        }
    )));
    lane.now += 31_000;
    let actions = lane.pump();
    let (_, body) = actions
        .iter()
        .find_map(|a| match a {
            DownlinkAction::Request {
                request,
                body,
                bootstrap: true,
            } => Some((*request, body)),
            _ => None,
        })
        .expect("hidden failed run must retry without a public bootstrap call");
    let body: Value = serde_json::from_str(body).unwrap();
    assert_eq!(
        (body["until"].clone(), body["after"].clone()),
        (json!(4), json!(0))
    );
    assert_eq!(
        lane.client
            .bootstrap_state("a", s.subscription_id)
            .unwrap()
            .state,
        BootstrapPhase::NotRequested
    );
    assert_eq!(
        lane.client
            .read_sql("SELECT reconcile_run FROM axton_subscription", &[])
            .unwrap()[0]["reconcile_run"],
        2
    );
}

#[test]
fn prepared_reconciliation_replacement_keeps_authority_without_completing_new_registration() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open(&path);
    let s = c.ensure_subscription("a").unwrap();
    c.initialize_subscriptions(
        &BTreeMap::from([("a".into(), s.subscription_id)]),
        &BTreeMap::from([("a".into(), 0)]),
    )
    .unwrap();
    let up = json!({"cursors":{"a":{"from":0,"to":1,"head":1}},"changes":[{"kind":"upsert","stream":"a","cursor":1,"model":"Entry","identity":{"id":"e"},"stamp":1,"state":{"text":"held","note":null}}]});
    c.apply_stream_page(StreamPullPage::decode(up.to_string().as_bytes()).unwrap())
        .unwrap();
    drop(c);
    let mut raw = axton_sqlite::SqliteStore::open(&path).unwrap();
    raw.execute_batch("UPDATE axton_client SET stream_membership_version=0")
        .unwrap();
    drop(raw);
    let mut c = open(&path);
    c.initialize_subscriptions(
        &BTreeMap::from([("a".into(), s.subscription_id)]),
        &BTreeMap::from([("a".into(), 4)]),
    )
    .unwrap();
    let task = c.bootstrap_schedule(None).unwrap().unwrap();
    let page=StreamBootstrapPage::decode(json!({"mode":"bootstrap","stream":"a","from":0,"to":4,"until":4,"head":4,"changes":[{"kind":"upsert","stream":"a","cursor":3,"model":"Entry","identity":{"id":"e"},"stamp":3,"state":{"text":"new","note":null}}]}).to_string().as_bytes()).unwrap();
    c.begin_session().unwrap();
    let prepared = c
        .prepare_store(StoreDelivery::StreamReconciliation {
            stream: "a".into(),
            subscription_id: s.subscription_id,
            run: task.state.run,
            expected_after: 0,
            page,
        })
        .unwrap();
    c.session(|tx| {
        tx.set_stream("a".into(), false)?;
        tx.set_stream("a".into(), true)
    })
    .unwrap();
    let StoreResult::Bootstrap(result) = c.apply_prepared_store(prepared).unwrap() else {
        panic!("bootstrap result");
    };
    assert!(matches!(result, BootstrapApply::Detached { .. }));
    c.commit_session().unwrap();
    let current = c.subscription_state("a").unwrap().unwrap();
    assert_ne!(current.subscription_id, s.subscription_id);
    assert_eq!(current.cursor, None);
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "new");
    let row = &c
        .read_sql(
            "SELECT reconcile_state,reconcile_cursor,reconcile_bound FROM axton_subscription",
            &[],
        )
        .unwrap()[0];
    assert_eq!(row["reconcile_state"], "requested");
    assert_eq!(row["reconcile_cursor"], 0);
    assert!(row["reconcile_bound"].is_null());
}

#[test]
fn stale_replaced_http_response_cannot_choose_new_reconciliation_bound() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open(&path);
    let s = c.ensure_subscription("a").unwrap();
    c.initialize_subscriptions(
        &BTreeMap::from([("a".into(), s.subscription_id)]),
        &BTreeMap::from([("a".into(), 0)]),
    )
    .unwrap();
    c.apply_stream_page(StreamPullPage::decode(json!({"cursors":{"a":{"from":0,"to":1,"head":1}},"changes":[{"kind":"upsert","stream":"a","cursor":1,"model":"Entry","identity":{"id":"e"},"stamp":1,"state":{"text":"held","note":null}}]}).to_string().as_bytes()).unwrap()).unwrap();
    let mut cycle = SyncCycle::default();
    cycle.next(&mut c).unwrap().unwrap();
    c.remove_subscription("a", s.subscription_id).unwrap();
    let renewed = c.ensure_subscription("a").unwrap();
    let mut raw = axton_sqlite::SqliteStore::open(&path).unwrap();
    raw.execute_batch("UPDATE axton_subscription SET starting_cursor=1,cursor=1")
        .unwrap();
    drop(raw);
    let report = cycle
        .complete(
            &mut c,
            json!({"cursors":{"a":{"from":1,"to":2,"head":2}},"changes":[]})
                .to_string()
                .as_bytes(),
        )
        .unwrap();
    assert!(report.stale);
    assert_eq!(
        c.subscription_state("a").unwrap().unwrap().subscription_id,
        renewed.subscription_id
    );
    assert!(
        c.read_sql("SELECT reconcile_bound FROM axton_subscription", &[])
            .unwrap()[0]["reconcile_bound"]
            .is_null()
    );
    let next = cycle.next(&mut c).unwrap().unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&next.body).unwrap()["cursors"]["a"],
        1
    );
}

#[test]
fn http_cycle_requires_restart_to_retry_failed_hidden_run_at_same_bound() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open(&path);
    let s = c.ensure_subscription("a").unwrap();
    c.initialize_subscriptions(
        &BTreeMap::from([("a".into(), s.subscription_id)]),
        &BTreeMap::from([("a".into(), 12)]),
    )
    .unwrap();
    drop(c);
    let mut raw = axton_sqlite::SqliteStore::open(&path).unwrap();
    raw.execute_batch("UPDATE axton_client SET stream_membership_version=0")
        .unwrap();
    drop(raw);
    let mut c = open(&path);
    let mut cycle = SyncCycle::default();
    cycle.next(&mut c).unwrap().unwrap();
    cycle
        .complete(
            &mut c,
            json!({"cursors":{"a":{"from":12,"to":12,"head":12}},"changes":[]})
                .to_string()
                .as_bytes(),
        )
        .unwrap();
    cycle.next(&mut c).unwrap().unwrap();
    assert!(cycle.complete(&mut c,json!({"mode":"bootstrap","stream":"a","from":0,"to":12,"until":12,"head":12,"changes":[{"kind":"upsert","stream":"a","cursor":2,"model":"Entry","identity":{"id":"e"},"stamp":2,"state":{"text":99,"note":null}}]}).to_string().as_bytes()).is_err());
    assert!(
        cycle.next(&mut c).is_err(),
        "failed run cannot spin silently"
    );
    cycle.restart();
    let request = cycle.next(&mut c).unwrap().unwrap();
    let request: Value = serde_json::from_str(&request.body).unwrap();
    assert_eq!(
        (request["after"].clone(), request["until"].clone()),
        (json!(0), json!(12))
    );
    cycle.complete(&mut c,json!({"mode":"bootstrap","stream":"a","from":0,"to":12,"until":12,"head":12,"changes":[]}).to_string().as_bytes()).unwrap();
    assert_eq!(
        c.read_sql(
            "SELECT reconcile_run,reconcile_state,bootstrap_state FROM axton_subscription",
            &[]
        )
        .unwrap()[0],
        json!({"reconcile_run":2,"reconcile_state":"complete","bootstrap_state":"not_requested"})
    );
}

#[test]
fn original_v02_layout_reopens_without_parallel_empty_holds_or_queue_loss() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let schema =
        Schema::from_value(serde_json::from_str(include_str!("fixtures/schema.json")).unwrap())
            .unwrap();
    let mut raw = axton_sqlite::SqliteStore::open(&path).unwrap();
    raw.execute_batch(include_str!("fixtures/v02-framework.sql"))
        .unwrap();
    raw.execute_batch(include_str!("fixtures/sqlite-state.sql"))
        .unwrap();
    axton_client::schema_store::write_descriptor(&mut raw, &schema).unwrap();
    drop(raw);
    let mut c = Client::open(
        axton_sqlite::SqliteStore::open(&path).unwrap(),
        schema.clone(),
    )
    .unwrap();
    assert_eq!(
        c.read_sql("SELECT count(*) AS n FROM axton_stream_member", &[])
            .unwrap()[0]["n"],
        4
    );
    assert_eq!(c.cursor("Channel:business-scope").unwrap(), Some(11));
    assert_eq!(
        c.read_sql(
            "SELECT count(*) AS n FROM sqlite_master WHERE name LIKE 'axton_channel%'",
            &[]
        )
        .unwrap()[0]["n"],
        0
    );
    let first = c.freeze().unwrap().unwrap();
    let mut logical: Value = serde_json::from_slice(&first).unwrap();
    logical.as_object_mut().unwrap().remove("capabilities");
    let expected: Value =
        serde_json::from_str(include_str!("fixtures/frozen-push-logical.json")).unwrap();
    assert_eq!(logical, expected);
    assert_eq!(
        c.read_sql("SELECT channel FROM Todo", &[]).unwrap()[0]["channel"],
        "second queued Channel"
    );
    drop(c);
    let mut c = Client::open(axton_sqlite::SqliteStore::open(&path).unwrap(), schema).unwrap();
    assert_eq!(c.freeze().unwrap().unwrap(), first);
}

fn original_store(path: &std::path::Path, marker: u8) -> (axton_sqlite::SqliteStore, Schema) {
    let schema =
        Schema::from_value(serde_json::from_str(include_str!("fixtures/schema.json")).unwrap())
            .unwrap();
    let mut raw = axton_sqlite::SqliteStore::open(path).unwrap();
    raw.execute_batch(include_str!("fixtures/v02-framework.sql"))
        .unwrap();
    raw.execute_batch(include_str!("fixtures/sqlite-state.sql"))
        .unwrap();
    raw.execute_batch(&format!(
        "UPDATE axton_client SET channel_membership_version={marker}"
    ))
    .unwrap();
    axton_client::schema_store::write_descriptor(&mut raw, &schema).unwrap();
    (raw, schema)
}

#[test]
fn original_zero_marker_reconciles_once_and_pending_batch_settles_independently() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (raw, schema) = original_store(&path, 0);
    drop(raw);
    let mut c = Client::open(
        axton_sqlite::SqliteStore::open(&path).unwrap(),
        schema.clone(),
    )
    .unwrap();
    assert_eq!(
        c.read_sql(
            "SELECT reconcile_run FROM axton_subscription WHERE stream='Channel:business-scope'",
            &[]
        )
        .unwrap()[0]["reconcile_run"],
        4
    );
    let first = c.freeze().unwrap().unwrap();
    drop(c);
    let mut c = Client::open(axton_sqlite::SqliteStore::open(&path).unwrap(), schema).unwrap();
    assert_eq!(c.freeze().unwrap().unwrap(), first);
    assert_eq!(
        c.read_sql(
            "SELECT reconcile_run FROM axton_subscription WHERE stream='Channel:business-scope'",
            &[]
        )
        .unwrap()[0]["reconcile_run"],
        4
    );
    let receipt=PushReceipt::decode_action_envelope(&serde_json::to_vec(&json!({"clientId":"fixture-client","batchSequence":1,"rejections":[],"completions":[{"callId":"01890f47-1234-7123-8123-000000000001","outcome":{"status":"succeeded","result":null}}],"records":[{"model":"Todo","identity":{"id":"live"},"stamp":8,"state":{"title":"saved snapshot","channel":"queued Channel"}}]})).unwrap()).unwrap();
    c.acknowledge(1, receipt).unwrap();
    assert_eq!(c.pending_count().unwrap(), 1);
    let second: Value = serde_json::from_slice(&c.freeze().unwrap().unwrap()).unwrap();
    assert_eq!(second["batchSequence"], 2);
    assert_eq!(
        second["mutations"][0]["callId"],
        "01890f47-1234-7123-8123-000000000004"
    );
    assert_eq!(
        c.read_sql("SELECT channel FROM Todo", &[]).unwrap()[0]["channel"],
        "second queued Channel"
    );
}

#[test]
fn original_layout_conflict_and_late_failure_roll_back_without_rewriting_saved_work() {
    for extra in [
        "CREATE TABLE axton_stream_member(scope TEXT)",
        "ALTER TABLE axton_subscription ADD COLUMN scope TEXT",
        "CREATE INDEX axton_stream_member_record ON Todo(channel)",
        "DROP TABLE axton_channel_member",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (mut raw, schema) = original_store(&path, 1);
        raw.execute_batch(extra).unwrap();
        let catalog = raw
            .query(
                "SELECT type,name,sql FROM sqlite_master ORDER BY type,name",
                &[],
            )
            .unwrap()
            .rows;
        let work = raw
            .query(
                "SELECT args,store_epoch FROM axton_mutation ORDER BY ordinal",
                &[],
            )
            .unwrap()
            .rows;
        let load = raw
            .query("SELECT intent,continuation FROM axton_load", &[])
            .unwrap()
            .rows;
        drop(raw);
        assert!(
            Client::open(axton_sqlite::SqliteStore::open(&path).unwrap(), schema).is_err(),
            "{extra}"
        );
        let mut raw = axton_sqlite::SqliteStore::open(&path).unwrap();
        assert_eq!(
            raw.query(
                "SELECT type,name,sql FROM sqlite_master ORDER BY type,name",
                &[]
            )
            .unwrap()
            .rows,
            catalog,
            "{extra}"
        );
        assert_eq!(
            raw.query(
                "SELECT args,store_epoch FROM axton_mutation ORDER BY ordinal",
                &[]
            )
            .unwrap()
            .rows,
            work
        );
        assert_eq!(
            raw.query("SELECT intent,continuation FROM axton_load", &[])
                .unwrap()
                .rows,
            load
        );
    }
}

#[test]
fn original_layout_preserves_raw_work_bytes_and_all_subscription_state() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut raw, schema) = original_store(&path, 1);
    let queries = [
        "SELECT hex(CAST(args AS BLOB)),hex(CAST(store AS BLOB)),store_epoch,call_id,push,ordinal FROM axton_mutation ORDER BY ordinal",
        "SELECT hex(CAST(intent AS BLOB)),hex(CAST(continuation AS BLOB)),hex(CAST(args AS BLOB)),phase,run,pages,attempts,retry,call_id,load_id,store_epoch FROM axton_load",
        "SELECT hex(CAST(push_models AS BLOB)),hex(CAST(push_results AS BLOB)),client_id,next_push,next_ordinal,generation,store_epoch,last_completed_push FROM axton_client",
        "SELECT hex(CAST(channel AS BLOB)),title,id FROM Todo",
        "SELECT hex(CAST(channel AS BLOB)),title,id FROM axton_before_Todo",
    ];
    let before: Vec<_> = queries
        .iter()
        .map(|q| raw.query(q, &[]).unwrap().rows)
        .collect();
    let states=raw.query("SELECT subscription_id,starting_cursor,cursor,bootstrap_state,bootstrap_run,bootstrap_cursor,bootstrap_barrier,reconcile_state,reconcile_run,reconcile_cursor,reconcile_bound,reconcile_barrier FROM axton_subscription ORDER BY subscription_id",&[]).unwrap().rows;
    drop(raw);
    let c = Client::open(axton_sqlite::SqliteStore::open(&path).unwrap(), schema).unwrap();
    drop(c);
    let mut raw = axton_sqlite::SqliteStore::open(&path).unwrap();
    for (q, b) in queries.iter().zip(before) {
        assert_eq!(raw.query(q, &[]).unwrap().rows, b, "{q}");
    }
    assert_eq!(raw.query("SELECT subscription_id,starting_cursor,cursor,bootstrap_state,bootstrap_run,bootstrap_cursor,bootstrap_barrier,reconcile_state,reconcile_run,reconcile_cursor,reconcile_bound,reconcile_barrier FROM axton_subscription ORDER BY subscription_id",&[]).unwrap().rows,states);
}

#[test]
fn scope_file_upgrades_in_place_preserving_every_layer_and_frozen_call() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (mut raw, schema) = original_store(&path, 1);
    let mut descriptor = serde_json::to_value(&schema).unwrap();
    let mut composition = descriptor["models"][0].clone();
    composition["name"] = json!("Composition");
    descriptor["models"]
        .as_array_mut()
        .unwrap()
        .push(composition);
    let schema = Schema::from_value(descriptor).unwrap();
    // Composition has no published read contract or handler: device-only work.
    axton_client::schema_store::write_descriptor(&mut raw, &schema).unwrap();
    raw.execute_batch("CREATE TABLE Composition(id TEXT PRIMARY KEY,title TEXT NOT NULL,channel TEXT NOT NULL);
        CREATE TABLE axton_before_Composition(id TEXT PRIMARY KEY,title TEXT NOT NULL,channel TEXT NOT NULL);
        INSERT INTO Composition VALUES('draft','frozen device words','scope:opaque');").unwrap();
    // An actual shipped Scope layout, including business JSON that spells scope.
    raw.execute_batch("ALTER TABLE axton_channel_member RENAME TO axton_scope_member;
        ALTER TABLE axton_scope_member RENAME COLUMN channel TO scope;
        DROP INDEX axton_channel_member_record;
        CREATE INDEX axton_scope_member_record ON axton_scope_member(model,identity,present);
        ALTER TABLE axton_subscription RENAME COLUMN channel TO scope;
        ALTER TABLE axton_client RENAME COLUMN channel_membership_version TO scope_membership_version;
        UPDATE axton_scope_member SET present=1 WHERE scope='Channel:business-scope' AND identity='{\"id\":\"live\"}';
        INSERT INTO axton_rejection VALUES(99,'Edit','edit.refused','{\"scope\":\"business refusal\"}');
        INSERT INTO axton_mutation_operation VALUES(2,1,'companion','Todo','{\"id\":\"companion\"}','create','{\"title\":\"local words\",\"channel\":\"scope:opaque\"}');
        INSERT INTO Todo VALUES('companion','local words','scope:opaque');
        INSERT INTO axton_local_write VALUES(1,3,NULL,'independent','Todo','{\"id\":\"local\"}','create','{\"title\":\"device words\",\"channel\":\"scope:opaque\"}');
        INSERT INTO Todo VALUES('local','device words','scope:opaque');").unwrap();
    let tables = [
        "axton_client",
        "axton_record",
        "axton_subscription",
        "axton_scope_member",
        "axton_mutation",
        "axton_mutation_operation",
        "axton_mutation_dependency",
        "axton_rejection",
        "axton_local_write",
        "axton_load",
        "axton_load_once",
        "Todo",
        "axton_before_Todo",
        "Composition",
        "axton_before_Composition",
    ];
    let before: Vec<_> = tables
        .iter()
        .map(|t| {
            raw.query(&format!("SELECT * FROM {t} ORDER BY rowid"), &[])
                .unwrap()
                .rows
        })
        .collect();
    drop(raw);
    let mut c = Client::open_at(
        &path,
        schema.clone(),
        Box::new(|p| axton_sqlite::SqliteStore::open(p)),
        false,
    )
    .unwrap();
    assert_eq!(axton_client::schema_store::current_file(&path), path);
    assert!(!axton_client::schema_store::sidecar_of(&path).exists());
    for (t, expected) in tables.iter().zip(before) {
        let t = if *t == "axton_scope_member" {
            "axton_stream_member"
        } else {
            t
        };
        let values = c
            .read_sql(&format!("SELECT * FROM {t} ORDER BY rowid"), &[])
            .unwrap();
        // read_sql exposes columns by name, while raw snapshots expose ordinal values.
        let mut reopened = axton_sqlite::SqliteStore::open(&path).unwrap();
        assert_eq!(
            reopened
                .query(&format!("SELECT * FROM {t} ORDER BY rowid"), &[])
                .unwrap()
                .rows,
            expected,
            "{t}"
        );
        assert_eq!(values.len(), expected.len());
    }
    let frozen = c.freeze().unwrap().unwrap();
    let job = c
        .get_load("01890f47-1234-7123-8123-000000000002")
        .unwrap()
        .unwrap();
    let fence = LoadFence {
        replica: c.replica_generation(),
        load_id: job.id.clone(),
        run: job.run,
        call_id: job.call_id.clone().unwrap(),
    };
    let delayed = LoadPageResponse {
        load_id: fence.load_id.clone(),
        call_id: fence.call_id.clone(),
        outcome: LoadOutcome::Succeeded {
            data: json!({"todos":[{"id":"live"}]}),
            next: None,
        },
        records: vec![AuthorityRecord {
            model: "Todo".into(),
            identity: json!({"id":"live"}),
            stamp: 9,
            state: json!({"title":"stale page","channel":"scope:opaque"}),
            error: None,
        }],
        memberships: vec![],
    };
    let remove = |name: &str, from: u64, to: u64| {
        StreamPullPage::decode(json!({"cursors":{name:{"from":from,"to":to,"head":to}},"changes":[{"kind":"remove","stream":name,"cursor":to,"model":"Todo","identity":{"id":"live"}}]}).to_string().as_bytes()).unwrap()
    };
    c.apply_stream_page(remove("Channel:business-scope", 11, 12))
        .unwrap();
    assert!(
        c.read(&schema.record_key("Todo", &json!({"id":"live"})).unwrap())
            .unwrap()
            .is_some(),
        "other holding retains base"
    );
    c.apply_stream_page(remove("Other", 3, 4)).unwrap();
    assert_eq!(c.read_sql("SELECT base_state,stamp,evicted_at FROM axton_record WHERE identity='{\"id\":\"live\"}'", &[]).unwrap()[0]["base_state"], "evicted");
    let LoadStored::Applied { .. } = c
        .store_load_page(
            &fence,
            LoadPageReply {
                load_id: fence.load_id.clone(),
                call_id: fence.call_id.clone(),
                page: Ok(delayed),
            },
        )
        .unwrap()
    else {
        panic!("stale authority can finish the Load without readmitting the base");
    };
    assert_eq!(
        c.read_sql(
            "SELECT base_state FROM axton_record WHERE identity='{\"id\":\"live\"}'",
            &[]
        )
        .unwrap()[0]["base_state"],
        "evicted"
    );
    assert_eq!(
        c.read_sql("SELECT title FROM Composition WHERE id='draft'", &[])
            .unwrap()[0]["title"],
        "frozen device words"
    );
    assert_eq!(c.pending_count().unwrap(), 2);
    assert_eq!(c.freeze().unwrap().unwrap(), frozen);
    assert_eq!(
        c.read_sql("SELECT title FROM Todo WHERE id='companion'", &[])
            .unwrap()[0]["title"],
        "local words"
    );
    assert_eq!(
        c.read_sql("SELECT title FROM Todo WHERE id='local'", &[])
            .unwrap()[0]["title"],
        "device words"
    );
    assert_eq!(
        c.read_sql("SELECT count(*) AS n FROM axton_rejection", &[])
            .unwrap()[0]["n"],
        1
    );
}

#[test]
fn incomplete_or_conflicting_scope_layout_rolls_back_without_mutating_work() {
    for damage in [
        "CREATE TABLE axton_stream_member(stream TEXT)",
        "ALTER TABLE axton_subscription ADD COLUMN stream TEXT",
        "ALTER TABLE axton_client ADD COLUMN stream_membership_version INTEGER",
        "ALTER TABLE axton_scope_member DROP COLUMN present",
        "DROP TABLE axton_load_once",
        "DROP TABLE axton_client",
        "CREATE INDEX axton_stream_member_record ON Todo(channel)",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (mut raw, schema) = original_store(&path, 1);
        raw.execute_batch("ALTER TABLE axton_channel_member RENAME TO axton_scope_member;
            ALTER TABLE axton_scope_member RENAME COLUMN channel TO scope;
            DROP INDEX axton_channel_member_record;
            ALTER TABLE axton_subscription RENAME COLUMN channel TO scope;
            ALTER TABLE axton_client RENAME COLUMN channel_membership_version TO scope_membership_version;").unwrap();
        raw.execute_batch(damage).unwrap();
        let catalog = raw
            .query(
                "SELECT type,name,sql FROM sqlite_master ORDER BY type,name",
                &[],
            )
            .unwrap()
            .rows;
        let work = raw
            .query(
                "SELECT args,store_epoch,call_id FROM axton_mutation ORDER BY ordinal",
                &[],
            )
            .unwrap()
            .rows;
        let load = raw
            .query("SELECT intent,continuation FROM axton_load", &[])
            .unwrap()
            .rows;
        let members = raw
            .query("SELECT * FROM axton_scope_member ORDER BY rowid", &[])
            .unwrap()
            .rows;
        let subscriptions = raw
            .query("SELECT * FROM axton_subscription ORDER BY rowid", &[])
            .unwrap()
            .rows;
        drop(raw);
        assert!(
            Client::open(axton_sqlite::SqliteStore::open(&path).unwrap(), schema).is_err(),
            "{damage}"
        );
        let mut raw = axton_sqlite::SqliteStore::open(&path).unwrap();
        assert_eq!(
            raw.query(
                "SELECT type,name,sql FROM sqlite_master ORDER BY type,name",
                &[]
            )
            .unwrap()
            .rows,
            catalog,
            "{damage}"
        );
        assert_eq!(
            raw.query(
                "SELECT args,store_epoch,call_id FROM axton_mutation ORDER BY ordinal",
                &[]
            )
            .unwrap()
            .rows,
            work,
            "{damage}"
        );
        assert_eq!(
            raw.query("SELECT intent,continuation FROM axton_load", &[])
                .unwrap()
                .rows,
            load,
            "{damage}"
        );
        assert_eq!(
            raw.query("SELECT * FROM axton_scope_member ORDER BY rowid", &[])
                .unwrap()
                .rows,
            members,
            "{damage}"
        );
        assert_eq!(
            raw.query("SELECT * FROM axton_subscription ORDER BY rowid", &[])
                .unwrap()
                .rows,
            subscriptions,
            "{damage}"
        );
    }
}
