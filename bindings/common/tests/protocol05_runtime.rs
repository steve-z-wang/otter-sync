use axton_binding::actor;
use serde_json::{Value, json};
use std::time::{Duration, Instant};
fn wait(id: u64) -> Vec<Value> {
    let start = Instant::now();
    loop {
        let events = actor::drain(id);
        if !events.is_empty() {
            return events;
        }
        assert!(start.elapsed() < Duration::from_secs(5));
        std::thread::sleep(Duration::from_millis(2));
    }
}
fn open(path: &std::path::Path) -> u64 {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        axton_sqlite::SqliteStore::set_application_data_directory("/private/tmp/axton-task5-locks")
            .unwrap()
    });
    actor::open(json!({"type":"open","requestId":"o","protocol":5,"path":path,"stream":"User:u","schema":schema()}),Box::new(|_|{})).unwrap()
}
#[test]
fn protocol05_open_derives_durable_context_and_keeps_routing_id() {
    let d = tempfile::tempdir().unwrap();
    let id = open(&d.path().join("db"));
    let events = wait(id);
    assert_eq!(events[0]["ok"], true, "{events:?}");
    assert_eq!(events[0]["value"]["context"]["protocol"], 5);
    assert!(events[0]["value"]["clientId"].is_string());
    actor::detach(id);
    assert!(actor::wait_closed(id, Duration::from_secs(5)));
}
#[test]
fn identical_queries_execute_independent_protocol05_reads() {
    let d = tempfile::tempdir().unwrap();
    let id = open(&d.path().join("db"));
    assert_eq!(wait(id)[0]["ok"], true);
    actor::submit(
        id,
        json!({"type":"task","requestId":"connect","command":{"kind":"connect"}}),
    )
    .unwrap();
    wait(id);
    for request in ["q1", "q2"] {
        actor::submit(id,json!({"type":"task","requestId":request,"command":{"kind":"invoke","name":"Lookup","version":1,"args":{},"store":false}})).unwrap();
    }
    let start = Instant::now();
    let mut reads = Vec::new();
    while reads.len() < 2 {
        for e in actor::drain(id) {
            if e["operation"]["route"] == "action" {
                let body: Value =
                    serde_json::from_str(e["operation"]["body"].as_str().unwrap()).unwrap();
                reads.push((e, body));
            }
        }
        assert!(start.elapsed() < Duration::from_secs(3), "{reads:?}");
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_ne!(reads[0].1["requestId"], reads[1].1["requestId"]);
    for (e, b) in reads {
        assert_eq!(b["protocol"], 5);
        actor::submit(id,json!({"type":"effectResult","effectId":e["effectId"],"outcome":{"ok":true,"value":serde_json::to_string(&json!({"protocol":5,"storeId":b["storeId"],"stream":b["stream"],"materialization":b["materialization"],"requestId":b["requestId"],"outcome":{"kind":"succeeded","result":[]},"records":[]})).unwrap()}})).unwrap();
    }
    actor::detach(id);
    assert!(actor::wait_closed(id, Duration::from_secs(5)));
}

fn schema() -> Value {
    let mut s: Value =
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap();
    s["actions"] = json!([{ "name":"Lookup", "version":1,"kind":"query","inputs":[],"outputs":[]}, {"name":"Write","version":1,"inputs":[{"kind":"model","name":"entry","model":"Entry","operation":"create","cardinality":"single"}],"outputs":[]}]);
    s
}
#[test]
fn handshake_reception_progresses_while_application_transaction_owns_worker() {
    let d = tempfile::tempdir().unwrap();
    let id = open(&d.path().join("db"));
    assert_eq!(wait(id)[0]["ok"], true);
    actor::submit(
        id,
        json!({"type":"task","requestId":"connect","command":{"kind":"connect"}}),
    )
    .unwrap();
    let mut handshake = None;
    let start = Instant::now();
    while handshake.is_none() {
        for e in actor::drain(id) {
            if e["operation"]["route"] == "handshake" {
                handshake = Some(e)
            }
        }
        assert!(start.elapsed() < Duration::from_secs(2));
        std::thread::sleep(Duration::from_millis(2));
    }
    actor::submit(
        id,
        json!({"type":"task","requestId":"tx","command":{"kind":"transaction"}}),
    )
    .unwrap();
    let callback = wait(id)
        .into_iter()
        .find(|e| e["operation"]["kind"] == "callback")
        .unwrap();
    let h = handshake.unwrap();
    let body: Value = serde_json::from_str(h["operation"]["body"].as_str().unwrap()).unwrap();
    actor::submit(id,json!({"type":"effectResult","effectId":h["effectId"],"outcome":{"ok":true,"value":serde_json::to_string(&json!({"protocol":5,"storeId":body["storeId"],"stream":body["stream"],"head":0})).unwrap()}})).unwrap();
    // A socket effect is emitted by control before SQLite initialization can run.
    let events = wait(id);
    assert!(
        events.iter().any(|e| e["operation"]["kind"] == "socket"),
        "{events:?}"
    );
    actor::submit(id,json!({"type":"callbackResult","effectId":callback["effectId"],"transactionId":callback["operation"]["transactionId"],"ok":true})).unwrap();
    actor::detach(id);
    assert!(actor::wait_closed(id, Duration::from_secs(5)));
}
#[test]
fn no_store_returns_own_snapshot_and_duplicate_effect_completes_once() {
    let d = tempfile::tempdir().unwrap();
    let id = open(&d.path().join("db"));
    assert_eq!(wait(id)[0]["ok"], true);
    actor::submit(
        id,
        json!({"type":"task","requestId":"connect","command":{"kind":"connect"}}),
    )
    .unwrap();
    wait(id);
    actor::submit(id,json!({"type":"task","requestId":"q","command":{"kind":"invoke","name":"Lookup","version":1,"args":{},"store":false}})).unwrap();
    let e = wait(id)
        .into_iter()
        .find(|e| e["operation"]["route"] == "action")
        .unwrap();
    let b: Value = serde_json::from_str(e["operation"]["body"].as_str().unwrap()).unwrap();
    let response = json!({"protocol":5,"storeId":b["storeId"],"stream":b["stream"],"materialization":b["materialization"],"requestId":b["requestId"],"outcome":{"kind":"succeeded","result":[{"id":"e","text":"snapshot","note":null}]},"records":[{"key":{"model":"Entry","identity":{"id":"e"}},"cursor":null,"state":{"text":"snapshot","note":null}}]});
    let answer = json!({"type":"effectResult","effectId":e["effectId"],"outcome":{"ok":true,"value":response.to_string()}});
    actor::submit(id, answer.clone()).unwrap();
    actor::submit(id, answer).unwrap();
    let events = wait(id);
    let completed: Vec<_> = events.iter().filter(|e| e["requestId"] == "q").collect();
    assert_eq!(completed.len(), 1, "{events:?}");
    assert_eq!(
        completed[0]["value"]["outcome"]["result"][0]["text"],
        "snapshot"
    );
    actor::submit(id,json!({"type":"task","requestId":"read","command":{"kind":"read","key":{"model":"Entry","identity":{"id":"e"}}}})).unwrap();
    let events = wait(id);
    assert_eq!(
        events.iter().find(|e| e["requestId"] == "read").unwrap()["value"],
        Value::Null
    );
    actor::detach(id);
    assert!(actor::wait_closed(id, Duration::from_secs(5)));
}
#[test]
fn real_slow_sql_keeps_fragment_reception_and_close_nonblocking() {
    use axton_client::{ClientStore, v05};
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("db");
    let id = open(&path);
    let opened = wait(id);
    assert_eq!(opened[0]["ok"], true);
    let context: v05::RequestContext =
        serde_json::from_value(opened[0]["value"]["context"].clone()).unwrap();
    let mut blocker = axton_sqlite::SqliteStore::open(&path).unwrap();
    blocker.begin().unwrap();
    blocker
        .execute("UPDATE axton_store SET start_cursor=start_cursor", &[])
        .unwrap();
    actor::submit(
        id,
        json!({"type":"task","requestId":"connect","command":{"kind":"connect"}}),
    )
    .unwrap();
    let start = Instant::now();
    let h = loop {
        let events = actor::drain(id);
        if let Some(h) = events
            .into_iter()
            .find(|e| e["operation"]["route"] == "handshake")
        {
            break h;
        }
        assert!(start.elapsed() < Duration::from_secs(2));
        std::thread::sleep(Duration::from_millis(2));
    };
    let body: Value = serde_json::from_str(h["operation"]["body"].as_str().unwrap()).unwrap();
    actor::submit(id,json!({"type":"effectResult","effectId":h["effectId"],"outcome":{"ok":true,"value":json!({"protocol":5,"storeId":body["storeId"],"stream":"User:u","head":0}).to_string()}})).unwrap();
    let socket = wait(id)
        .into_iter()
        .find(|e| e["operation"]["kind"] == "socket")
        .unwrap();
    let changes = ["a", "b"]
        .into_iter()
        .map(|id| v05::AuthorityChange::Record {
            key: v05::RecordKey {
                model: "Entry".into(),
                identity: json!({"id":id}),
            },
            cursor: 1,
            state: json!({"text":id,"note":null}),
        })
        .collect();
    let plan = v05::freeze_delivery(
        context,
        "socket-plan".into(),
        v05::DeliveryPurpose::Sync,
        0,
        1,
        1,
        8_000_000_000_000_000,
        vec![v05::DeliveryUnit {
            index: 0,
            through: Some(1),
            changes,
        }],
        1,
    )
    .unwrap();
    actor::submit(id,json!({"type":"effectResult","effectId":socket["effectId"],"outcome":{"ok":true,"value":{"event":"message","body":json!({"header":plan.header,"parts":[plan.parts[0].clone()]}).to_string()}}})).unwrap();
    let events = wait(id);
    assert!(
        events.iter().any(|e| e["operation"]["route"] == "pull"),
        "control did not request missing part while SQL writer was blocked: {events:?}"
    );
    assert_eq!(
        blocker
            .query("SELECT start_cursor FROM axton_store", &[])
            .unwrap()
            .rows[0][0],
        Value::Null
    );
    let close = Instant::now();
    actor::detach(id);
    assert!(close.elapsed() < Duration::from_millis(200));
    blocker.rollback().unwrap();
    drop(blocker);
    assert!(actor::wait_closed(id, Duration::from_secs(5)));
}
#[test]
fn mutation_local_acceptance_precedes_backend_settlement_and_survives_reopen() {
    use axton_client::v05;
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("db");
    let id = open(&path);
    let opened = wait(id);
    assert_eq!(opened[0]["ok"], true);
    actor::submit(id,json!({"type":"task","requestId":"submit","command":{"kind":"submitAction","name":"Write","version":1,"args":{"entry":{"id":"e","text":"local","note":null}}}})).unwrap();
    let local = wait(id);
    let completion = local.iter().find(|e| e["requestId"] == "submit").unwrap();
    assert_eq!(completion["ok"], true, "{local:?}");
    let call = completion["value"]["callId"].clone();
    assert!(local.iter().all(|e| e["type"] != "callCompleted"));
    actor::submit(
        id,
        json!({"type":"task","requestId":"connect","command":{"kind":"connect"}}),
    )
    .unwrap();
    let mut completed = Vec::new();
    let start = Instant::now();
    while completed.is_empty() {
        for e in actor::drain(id) {
            if e["type"] == "callCompleted" {
                completed.push(e);
                continue;
            }
            let Some(body) = e["operation"]["body"].as_str() else {
                continue;
            };
            let b: Value = serde_json::from_str(body).unwrap();
            let response = match e["operation"]["route"].as_str() {
                Some("handshake") => {
                    json!({"protocol":5,"storeId":b["storeId"],"stream":b["stream"],"head":0})
                }
                Some("push") => {
                    let request: v05::MutationRequest = serde_json::from_value(b).unwrap();
                    json!(v05::BatchAcknowledgement {
                        context: request.context,
                        batch_id: request.batch_id,
                        digest: request.digest,
                        results: vec![v05::MutationResult {
                            mutation_id: request.mutations[0].id,
                            outcome: v05::MutationOutcome::Accepted {
                                sync_cursor: 0,
                                result: Value::Null,
                                targets: vec![v05::SettlementTarget::Private {
                                    record: v05::ReadRecord {
                                        key: v05::RecordKey {
                                            model: "Entry".into(),
                                            identity: json!({"id":"e"})
                                        },
                                        cursor: (),
                                        state: json!({"text":"server","note":null})
                                    }
                                }]
                            }
                        }]
                    })
                }
                Some("pull") => {
                    let request: v05::DeltaRequest = serde_json::from_value(b).unwrap();
                    let p = v05::freeze_delivery(
                        request.context,
                        "empty".into(),
                        v05::DeliveryPurpose::Bootstrap,
                        0,
                        0,
                        0,
                        8_000_000_000_000_000,
                        vec![v05::DeliveryUnit {
                            index: 0,
                            through: Some(0),
                            changes: vec![],
                        }],
                        1,
                    )
                    .unwrap();
                    json!({"header":p.header,"parts":p.parts})
                }
                _ => continue,
            };
            actor::submit(id,json!({"type":"effectResult","effectId":e["effectId"],"outcome":{"ok":true,"value":response.to_string()}})).unwrap();
        }
        assert!(start.elapsed() < Duration::from_secs(5), "no settled Call");
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0]["callId"], call);
    actor::detach(id);
    assert!(actor::wait_closed(id, Duration::from_secs(5)));
    let id = open(&path);
    assert_eq!(wait(id)[0]["ok"], true);
    actor::submit(id,json!({"type":"task","requestId":"outcome","command":{"kind":"callCompletion","callId":call}})).unwrap();
    let persisted = wait(id);
    assert_eq!(
        persisted
            .iter()
            .find(|e| e["requestId"] == "outcome")
            .unwrap()["ok"],
        true
    );
    actor::detach(id);
    assert!(actor::wait_closed(id, Duration::from_secs(5)));
}
#[test]
fn sync_and_direct_401_share_one_refresh_and_retry_original_requests() {
    let d = tempfile::tempdir().unwrap();
    let id = open(&d.path().join("db"));
    assert_eq!(wait(id)[0]["ok"], true);
    actor::submit(id,json!({"type":"task","requestId":"connect","command":{"kind":"connect","refreshAuth":true}})).unwrap();
    let start = Instant::now();
    let handshake = loop {
        if let Some(e) = actor::drain(id)
            .into_iter()
            .find(|e| e["operation"]["route"] == "handshake")
        {
            break e;
        }
        assert!(start.elapsed() < Duration::from_secs(3));
        std::thread::sleep(Duration::from_millis(2));
    };
    actor::submit(id,json!({"type":"task","requestId":"q","command":{"kind":"invoke","name":"Lookup","version":1,"args":{},"store":false}})).unwrap();
    let direct = wait(id)
        .into_iter()
        .find(|e| e["operation"]["route"] == "action")
        .unwrap();
    for e in [&handshake, &direct] {
        actor::submit(id,json!({"type":"effectResult","effectId":e["effectId"],"outcome":{"ok":false,"error":{"message":"expired credential","status":401}}})).unwrap();
    }
    let mut refreshes = Vec::new();
    for _ in 0..30 {
        refreshes.extend(
            actor::drain(id)
                .into_iter()
                .filter(|e| e["operation"]["kind"] == "refreshAuth"),
        );
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(refreshes.len(), 1);
    actor::submit(id,json!({"type":"effectResult","effectId":refreshes[0]["effectId"],"outcome":{"ok":true,"value":null}})).unwrap();
    let start = Instant::now();
    let mut retried = Vec::new();
    while retried.len() < 2 {
        retried.extend(actor::drain(id).into_iter().filter(|e| {
            matches!(
                e["operation"]["route"].as_str(),
                Some("handshake" | "action")
            )
        }));
        assert!(start.elapsed() < Duration::from_secs(3), "{retried:?}");
        std::thread::sleep(Duration::from_millis(2));
    }
    for original in [handshake, direct] {
        let route = original["operation"]["route"].clone();
        assert_eq!(
            retried
                .iter()
                .find(|e| e["operation"]["route"] == route)
                .unwrap()["operation"]["body"],
            original["operation"]["body"]
        );
    }
    actor::detach(id);
    assert!(actor::wait_closed(id, Duration::from_secs(5)));
}
#[test]
fn storing_reads_wait_for_initial_s_commit_but_no_store_does_not() {
    let d = tempfile::tempdir().unwrap();
    let id = open(&d.path().join("db"));
    assert_eq!(wait(id)[0]["ok"], true);
    actor::submit(
        id,
        json!({"type":"task","requestId":"connect","command":{"kind":"connect"}}),
    )
    .unwrap();
    let start = Instant::now();
    let handshake = loop {
        if let Some(e) = actor::drain(id)
            .into_iter()
            .find(|e| e["operation"]["route"] == "handshake")
        {
            break e;
        }
        assert!(start.elapsed() < Duration::from_secs(3));
        std::thread::sleep(Duration::from_millis(2));
    };
    for (request, store) in [("stored", true), ("unstored", false)] {
        actor::submit(id,json!({"type":"task","requestId":request,"command":{"kind":"invoke","name":"Lookup","version":1,"args":{},"store":store}})).unwrap();
    }
    let mut reads = Vec::new();
    for _ in 0..30 {
        reads.extend(
            actor::drain(id)
                .into_iter()
                .filter(|e| e["operation"]["route"] == "action"),
        );
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(reads.len(), 1, "storetrue escaped before S commit");
    let b: Value = serde_json::from_str(reads[0]["operation"]["body"].as_str().unwrap()).unwrap();
    assert_eq!(b["store"], false);
    let b: Value = serde_json::from_str(handshake["operation"]["body"].as_str().unwrap()).unwrap();
    actor::submit(id,json!({"type":"effectResult","effectId":handshake["effectId"],"outcome":{"ok":true,"value":json!({"protocol":5,"storeId":b["storeId"],"stream":b["stream"],"head":7}).to_string()}})).unwrap();
    let start = Instant::now();
    let read = loop {
        if let Some(e) = actor::drain(id)
            .into_iter()
            .find(|e| e["operation"]["route"] == "action")
        {
            break e;
        }
        assert!(start.elapsed() < Duration::from_secs(3));
        std::thread::sleep(Duration::from_millis(2));
    };
    let b: Value = serde_json::from_str(read["operation"]["body"].as_str().unwrap()).unwrap();
    assert_eq!(b["store"], true);
    actor::detach(id);
    assert!(actor::wait_closed(id, Duration::from_secs(5)));
}
#[test]
fn provisional_mutation_rolls_back_and_watch_never_publishes_optimism_before_commit() {
    let d = tempfile::tempdir().unwrap();
    let id = open(&d.path().join("db"));
    assert_eq!(wait(id)[0]["ok"], true);
    actor::submit(
        id,
        json!({"type":"task","requestId":"watch","command":{"kind":"watch","model":"Entry"}}),
    )
    .unwrap();
    let _ = wait(id);
    actor::submit(
        id,
        json!({"type":"task","requestId":"tx","command":{"kind":"transaction"}}),
    )
    .unwrap();
    let callback = wait(id)
        .into_iter()
        .find(|e| e["operation"]["kind"] == "callback")
        .unwrap();
    actor::submit(id,json!({"type":"transactionCommand","requestId":"local","transactionId":callback["operation"]["transactionId"],"command":{"kind":"submitMutation","name":"Write","version":1,"args":{"entry":{"id":"e","text":"provisional","note":null}}}})).unwrap();
    let local = wait(id);
    assert!(local.iter().all(|e| e["type"] != "observerChanged"));
    let call = local.iter().find(|e| e["requestId"] == "local").unwrap()["value"]["callId"].clone();
    assert!(call.is_string());
    actor::submit(id,json!({"type":"callbackResult","effectId":callback["effectId"],"transactionId":callback["operation"]["transactionId"],"ok":false,"error":"application rollback"})).unwrap();
    let events = wait(id);
    assert!(
        events.iter().any(|e| e["type"] == "transactionCallState"
            && e["callId"] == call
            && e["state"] == "rolledBack"),
        "{events:?}"
    );
    assert!(events.iter().all(|e| e["type"] != "observerChanged"));
    actor::submit(
        id,
        json!({"type":"task","requestId":"status","command":{"kind":"status"}}),
    )
    .unwrap();
    let events = wait(id);
    let status = events.iter().find(|e| e["requestId"] == "status").unwrap();
    assert_eq!(status["ok"], true, "{events:?}");
    assert_eq!(status["value"]["pending"], 0);
    actor::detach(id);
    assert!(actor::wait_closed(id, Duration::from_secs(5)));
}

#[test]
fn pending_schema_parks_desired_reads_until_owned_rollover_commits() {
    use axton_client::v05;
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("db");
    let id = open(&path);
    assert_eq!(wait(id)[0]["ok"], true);
    actor::detach(id);
    assert!(actor::wait_closed(id, Duration::from_secs(5)));
    let id = actor::open(json!({"type":"open","requestId":"o","protocol":5,"path":path,"stream":"User:u","projectionGeneration":"2","schema":schema()}),Box::new(|_|{})).unwrap();
    let opened = wait(id);
    assert_eq!(opened[0]["ok"], true);
    let old: v05::RequestContext =
        serde_json::from_value(opened[0]["value"]["context"].clone()).unwrap();
    actor::submit(
        id,
        json!({"type":"task","requestId":"connect","command":{"kind":"connect"}}),
    )
    .unwrap();
    let events = wait(id);
    let handshake = events
        .iter()
        .find(|e| e["operation"]["route"] == "handshake")
        .unwrap();
    actor::submit(id,json!({"type":"task","requestId":"query","command":{"kind":"invoke","name":"Lookup","version":1,"args":{},"store":false}})).unwrap();
    std::thread::sleep(Duration::from_millis(30));
    assert!(
        actor::drain(id)
            .iter()
            .all(|e| e["operation"]["route"] != "action")
    );
    actor::submit(id,json!({"type":"effectResult","effectId":handshake["effectId"],"outcome":{"ok":true,"value":serde_json::to_string(&v05::HandshakeResponse{protocol:5,store_id:old.store_id.clone(),stream:old.stream.clone(),head:0}).unwrap()}})).unwrap();
    let start = Instant::now();
    let owned = loop {
        let events = actor::drain(id);
        assert!(
            events.iter().all(|e| e["operation"]["route"] != "action"),
            "{events:?}"
        );
        if let Some(e) = events
            .into_iter()
            .find(|e| e["operation"]["route"] == "materialize")
        {
            break e;
        }
        assert!(start.elapsed() < Duration::from_secs(5));
        std::thread::sleep(Duration::from_millis(2));
    };
    let request: v05::MaterializationRequest =
        serde_json::from_str(owned["operation"]["body"].as_str().unwrap()).unwrap();
    assert_ne!(request.context.materialization, old.materialization);
    let f = v05::freeze_materialization(
        request.context.clone(),
        "schema-rollover".into(),
        request.owner.clone(),
        0,
        8_000_000_000_000_000,
        vec![v05::DeliveryUnit {
            index: 0,
            through: None,
            changes: vec![],
        }],
        1,
    )
    .unwrap();
    let response = v05::MaterializationResponse {
        request_id: request.request_id,
        delivery: v05::DeliveryResponse {
            header: f.header,
            parts: f.parts,
        },
    };
    actor::submit(id,json!({"type":"effectResult","effectId":owned["effectId"],"outcome":{"ok":true,"value":serde_json::to_string(&response).unwrap()}})).unwrap();
    let start = Instant::now();
    let read = loop {
        if let Some(e) = actor::drain(id)
            .into_iter()
            .find(|e| e["operation"]["route"] == "action")
        {
            break e;
        }
        assert!(start.elapsed() < Duration::from_secs(5));
        std::thread::sleep(Duration::from_millis(2));
    };
    let body: Value = serde_json::from_str(read["operation"]["body"].as_str().unwrap()).unwrap();
    assert_eq!(body["materialization"], request.context.materialization);
    assert_eq!(body["invocation"]["name"], "Lookup");
    actor::detach(id);
    assert!(actor::wait_closed(id, Duration::from_secs(5)));
}

#[test]
fn runtime_closed_event_releases_exclusive_store_before_immediate_reopen() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("db");
    // Establish the process's test lock directory before the custom wake sink.
    let first = open(&path);
    assert_eq!(wait(first)[0]["ok"], true);
    actor::detach(first);
    assert!(actor::wait_closed(first, Duration::from_secs(5)));
    for _ in 0..32 {
        let (sent, received) = std::sync::mpsc::channel();
        let reopen = path.clone();
        let id=actor::open(json!({"type":"open","requestId":"o","protocol":5,"path":path,"stream":"User:u","schema":schema()}),Box::new(move |id| {
            for event in actor::drain(id) {
                if event["type"]=="runtimeClosed" {
                    let result=axton_sqlite::SqliteStore::open_exclusive05(&reopen,"User:u").map(|_|()).map_err(|e|e.to_string());
                    sent.send(result).unwrap();
                }
            }
        })).unwrap();
        actor::submit(id, json!({"type":"close"})).unwrap();
        assert_eq!(
            received.recv_timeout(Duration::from_secs(5)).unwrap(),
            Ok(())
        );
        actor::detach(id);
        assert!(actor::wait_closed(id, Duration::from_secs(5)));
    }
}
