//! Production channel negotiation and additive retained-replica upgrade.
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
            .contains(CHANNEL_MEMBERSHIP_CAPABILITY),
        "missing capability in {body}"
    );
}

#[test]
fn production_pull_and_live_advertise_channel_membership_after_encoding() {
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
    let body = json!({"cursors":{"a":{"from":0,"to":1,"head":1}},"changes":[{"kind":"remove","channel":"a","cursor":1,"model":"Entry","identity":{"id":"e"}}]});
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
    let body = json!({"cursors":{"a":{"from":0,"to":1,"head":1}},"changes":[{"kind":"remove","channel":"a","cursor":1,"model":"Entry","identity":{"id":"e"}}]});
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
    let p = json!({"cursors":{"a":{"from":0,"to":1,"head":1}},"changes":[{"kind":"upsert","channel":"a","cursor":1,"model":"Entry","identity":{"id":"e"},"stamp":1,"state":{"text":"held","note":null}}]});
    c.apply_channel_page(ChannelPullPage::decode(p.to_string().as_bytes()).unwrap())
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
    let response = ChannelBootstrapPage::decode(json!({"mode":"bootstrap","channel":"a","from":0,"to":20,"until":20,"head":20,"changes":[{"kind":"remove","channel":"a","cursor":2,"model":"Entry","identity":{"id":"e"}}]}).to_string().as_bytes()).unwrap();
    let applied = c.apply_channel_bootstrap_task(task, &response).unwrap();
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
    raw.execute_batch("ALTER TABLE axton_client DROP COLUMN channel_membership_version")
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
        c.read_sql("SELECT count(*) AS n FROM axton_channel_member", &[])
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
    raw.execute_batch("UPDATE axton_client SET channel_membership_version=0")
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
    cycle.complete(&mut c,json!({"mode":"bootstrap","channel":"a","from":0,"to":12,"until":12,"head":20,"changes":[{"kind":"remove","channel":"a","cursor":9,"model":"Entry","identity":{"id":"e"}}]}).to_string().as_bytes()).unwrap();
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
    raw.execute_batch("UPDATE axton_client SET channel_membership_version=0")
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
    let actions=lane.send(DownlinkEvent::Response { request:history,body:json!({"mode":"bootstrap","channel":"a","from":0,"to":2,"until":4,"head":4,"changes":[{"kind":"upsert","channel":"a","cursor":2,"model":"Entry","identity":{"id":"e"},"stamp":2,"state":{"text":99,"note":null}}]}).to_string() });
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
    let up = json!({"cursors":{"a":{"from":0,"to":1,"head":1}},"changes":[{"kind":"upsert","channel":"a","cursor":1,"model":"Entry","identity":{"id":"e"},"stamp":1,"state":{"text":"held","note":null}}]});
    c.apply_channel_page(ChannelPullPage::decode(up.to_string().as_bytes()).unwrap())
        .unwrap();
    drop(c);
    let mut raw = axton_sqlite::SqliteStore::open(&path).unwrap();
    raw.execute_batch("UPDATE axton_client SET channel_membership_version=0")
        .unwrap();
    drop(raw);
    let mut c = open(&path);
    c.initialize_subscriptions(
        &BTreeMap::from([("a".into(), s.subscription_id)]),
        &BTreeMap::from([("a".into(), 4)]),
    )
    .unwrap();
    let task = c.bootstrap_schedule(None).unwrap().unwrap();
    let page=ChannelBootstrapPage::decode(json!({"mode":"bootstrap","channel":"a","from":0,"to":4,"until":4,"head":4,"changes":[{"kind":"upsert","channel":"a","cursor":3,"model":"Entry","identity":{"id":"e"},"stamp":3,"state":{"text":"new","note":null}}]}).to_string().as_bytes()).unwrap();
    c.begin_session().unwrap();
    let prepared = c
        .prepare_store(StoreDelivery::ChannelReconciliation {
            scope: "a".into(),
            subscription_id: s.subscription_id,
            run: task.state.run,
            expected_after: 0,
            page,
        })
        .unwrap();
    c.session(|tx| {
        tx.set_channel("a".into(), false)?;
        tx.set_channel("a".into(), true)
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
    c.apply_channel_page(ChannelPullPage::decode(json!({"cursors":{"a":{"from":0,"to":1,"head":1}},"changes":[{"kind":"upsert","channel":"a","cursor":1,"model":"Entry","identity":{"id":"e"},"stamp":1,"state":{"text":"held","note":null}}]}).to_string().as_bytes()).unwrap()).unwrap();
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
    raw.execute_batch("UPDATE axton_client SET channel_membership_version=0")
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
    assert!(cycle.complete(&mut c,json!({"mode":"bootstrap","channel":"a","from":0,"to":12,"until":12,"head":12,"changes":[{"kind":"upsert","channel":"a","cursor":2,"model":"Entry","identity":{"id":"e"},"stamp":2,"state":{"text":99,"note":null}}]}).to_string().as_bytes()).is_err());
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
    cycle.complete(&mut c,json!({"mode":"bootstrap","channel":"a","from":0,"to":12,"until":12,"head":12,"changes":[]}).to_string().as_bytes()).unwrap();
    assert_eq!(
        c.read_sql(
            "SELECT reconcile_run,reconcile_state,bootstrap_state FROM axton_subscription",
            &[]
        )
        .unwrap()[0],
        json!({"reconcile_run":2,"reconcile_state":"complete","bootstrap_state":"not_requested"})
    );
}
