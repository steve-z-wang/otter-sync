use axton_client::{Client, DownlinkAction, DownlinkEvent, DownlinkWorker, RecordKey, Schema, v04};
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};
fn next(worker: &mut DownlinkWorker, c: &mut Client<SqliteStore>) -> Vec<DownlinkAction> {
    worker.handle(c, DownlinkEvent::Next, 1, 7).unwrap()
}
fn gather(worker: &mut DownlinkWorker, c: &mut Client<SqliteStore>) -> Vec<DownlinkAction> {
    let mut out = Vec::new();
    for _ in 0..20 {
        let actions = next(worker, c);
        if actions.is_empty() {
            break;
        }
        let waits = actions
            .iter()
            .any(|a| matches!(a, DownlinkAction::Wait { .. }));
        out.extend(actions);
        if waits {
            break;
        }
    }
    out
}
fn request(actions: &[DownlinkAction], kind: &str) -> (u64, Value) {
    actions
        .iter()
        .find_map(|action| {
            if let DownlinkAction::Request { request, body, .. } = action {
                let body: Value = serde_json::from_str(body).unwrap();
                if body["kind"] == kind {
                    return Some((*request, body));
                }
                None
            } else {
                None
            }
        })
        .unwrap_or_else(|| panic!("missing {kind}: {actions:?}"))
}
fn answer(
    worker: &mut DownlinkWorker,
    c: &mut Client<SqliteStore>,
    request: u64,
    body: Value,
) -> Vec<DownlinkAction> {
    worker
        .handle(
            c,
            DownlinkEvent::Response {
                request,
                body: body.to_string(),
            },
            1,
            7,
        )
        .unwrap();
    gather(worker, c)
}
#[test]
fn existing_downlink_worker_bootstraps_bound_stream_and_commits_live_v04_prefix() {
    let d = tempfile::tempdir().unwrap();
    let mut raw: Value =
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap();
    raw["models"][0]["bootstrap"] = json!(true);
    let mut c = Client::open_bound(
        SqliteStore::open_exclusive(d.path().join("db")).unwrap(),
        Schema::from_value(raw).unwrap(),
        v04::StoreBinding {
            backend: "b".into(),
            viewer: "a".into(),
            stream: "User:a".into(),
            contract: "app".into(),
        },
    )
    .unwrap();
    let ctx = c.request_context().unwrap().clone();
    let mut worker = DownlinkWorker::default();
    worker.handle(&mut c, DownlinkEvent::Start, 1, 7).unwrap();
    let actions = gather(&mut worker, &mut c);
    let (start, _) = request(&actions, "start");
    let actions = answer(
        &mut worker,
        &mut c,
        start,
        json!({"context":ctx,"manifestId":"m","start":100,"total":1}),
    );
    let epoch = actions
        .iter()
        .find_map(|a| {
            if let DownlinkAction::Open { epoch, subscribe } = a {
                let sub: v04::SubscribeIntent = v04::decode(subscribe.as_bytes()).unwrap();
                assert_eq!(sub.cursor, 100);
                Some(*epoch)
            } else {
                None
            }
        })
        .unwrap();
    let (page, _) = request(&actions, "page");
    worker
        .handle(
            &mut c,
            DownlinkEvent::Message {
                epoch,
                body: json!({"context":ctx,"cursor":100,"head":100}).to_string(),
            },
            1,
            7,
        )
        .unwrap();
    gather(&mut worker, &mut c);
    let actions = answer(
        &mut worker,
        &mut c,
        page,
        json!({"context":ctx,"manifestId":"m","total":1,"from":0,"to":1,"items":[{"ordinal":0,"change":{"kind":"upsert","record":{"model":"Entry","identity":{"id":"e"},"cursor":57,"state":{"text":"historical","note":null}}}}]}),
    );
    let (tail, _) = request(&actions, "tail");
    let tail_actions = answer(
        &mut worker,
        &mut c,
        tail,
        json!({"context":ctx,"manifestId":"m","head":150}),
    );
    assert!(
        tail_actions.iter().any(|action| matches!(
            action,
            DownlinkAction::Request {
                bootstrap: false,
                ..
            }
        )),
        "fixed tail beyond ACK head needs independent Delta catch-up"
    );
    assert_eq!(c.stream_cursor04().unwrap(), 100);
    assert!(!c.bootstrap_complete04().unwrap());
    let subscription = c.subscription_state("User:a").unwrap().unwrap();
    assert_eq!(
        c.bootstrap_state("User:a", subscription.subscription_id)
            .unwrap()
            .state,
        axton_client::BootstrapPhase::CatchingUp
    );
    let page = v04::DeltaPage {
        context: ctx,
        page_id: "live".into(),
        from: 100,
        to: 150,
        head: 150,
        units: vec![v04::CommitUnit {
            through: 150,
            changes: vec![v04::StreamChange::Upsert {
                record: v04::StreamRecord {
                    key: RecordKey {
                        model: "Entry".into(),
                        identity: json!({"id":"e"}),
                    },
                    cursor: 150,
                    state: json!({"text":"live","note":null}),
                },
            }],
        }],
    };
    worker
        .handle(
            &mut c,
            DownlinkEvent::Message {
                epoch,
                body: String::from_utf8(v04::encode(&page).unwrap()).unwrap(),
            },
            1,
            7,
        )
        .unwrap();
    gather(&mut worker, &mut c);
    assert_eq!(c.stream_cursor04().unwrap(), 150);
    assert!(c.bootstrap_complete04().unwrap());
    assert_eq!(
        c.bootstrap_state("User:a", subscription.subscription_id)
            .unwrap()
            .state,
        axton_client::BootstrapPhase::Complete
    );
    assert_eq!(
        c.read(&RecordKey {
            model: "Entry".into(),
            identity: json!({"id":"e"})
        })
        .unwrap()
        .unwrap()["text"],
        "live"
    );
}

#[test]
fn ack_head_requests_delta_and_loader_failure_reduces_batch_without_advancing_cursor() {
    let d = tempfile::tempdir().unwrap();
    let mut c = Client::open_bound(
        SqliteStore::open_exclusive(d.path().join("db")).unwrap(),
        Schema::from_value(
            serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap(),
        )
        .unwrap(),
        v04::StoreBinding {
            backend: "b".into(),
            viewer: "a".into(),
            stream: "User:a".into(),
            contract: "app".into(),
        },
    )
    .unwrap();
    let ctx = c.request_context().unwrap().clone();
    c.start_bootstrap04(&v04::BootstrapStarted {
        context: ctx.clone(),
        manifest_id: "ready".into(),
        start: 0,
        total: 0,
    })
    .unwrap();
    c.capture_bootstrap_tail04(&v04::BootstrapTail {
        context: ctx.clone(),
        manifest_id: "ready".into(),
        head: 2,
    })
    .unwrap();
    let mut worker = DownlinkWorker::default();
    worker.handle(&mut c, DownlinkEvent::Start, 1, 7).unwrap();
    let open = gather(&mut worker, &mut c);
    let epoch = open
        .iter()
        .find_map(|a| {
            if let DownlinkAction::Open { epoch, .. } = a {
                Some(*epoch)
            } else {
                None
            }
        })
        .unwrap();
    worker
        .handle(
            &mut c,
            DownlinkEvent::Message {
                epoch,
                body: json!({"context":ctx,"cursor":0,"head":2}).to_string(),
            },
            1,
            7,
        )
        .unwrap();
    let actions = gather(&mut worker, &mut c);
    let (request, first) = actions
        .iter()
        .find_map(|a| {
            if let DownlinkAction::Request {
                request,
                body,
                bootstrap: false,
            } = a
            {
                Some((*request, serde_json::from_str::<Value>(body).unwrap()))
            } else {
                None
            }
        })
        .expect("ACK head requires catch-up even if live publication fails");
    assert_eq!(first["after"], 0);
    assert_eq!(first["limit"], 128);
    worker
        .handle(
            &mut c,
            DownlinkEvent::Failed {
                request,
                reason: Some("loader.failed".into()),
                status: Some(500),
            },
            1,
            7,
        )
        .unwrap();
    gather(&mut worker, &mut c); // Admit the failure before advancing the retry clock.
    let mut actions = Vec::new();
    for _ in 0..4 {
        actions.extend(
            worker
                .handle(&mut c, DownlinkEvent::Next, 100000, 7)
                .unwrap(),
        );
    }
    let epoch = actions
        .iter()
        .find_map(|a| {
            if let DownlinkAction::Open { epoch, .. } = a {
                Some(*epoch)
            } else {
                None
            }
        })
        .unwrap();
    worker
        .handle(
            &mut c,
            DownlinkEvent::Message {
                epoch,
                body: json!({"context":ctx,"cursor":0,"head":2}).to_string(),
            },
            100000,
            7,
        )
        .unwrap();
    let mut actions = Vec::new();
    for _ in 0..4 {
        actions.extend(
            worker
                .handle(&mut c, DownlinkEvent::Next, 100000, 7)
                .unwrap(),
        );
    }
    let retry = actions
        .iter()
        .find_map(|a| {
            if let DownlinkAction::Request {
                body,
                bootstrap: false,
                ..
            } = a
            {
                Some(serde_json::from_str::<Value>(body).unwrap())
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(retry["after"], 0);
    assert_eq!(retry["limit"], 64);
    assert_ne!(retry["callId"], first["callId"]);
    assert_eq!(c.stream_cursor04().unwrap(), 0);
}

#[test]
fn local_manifest_constraint_failure_reduces_ordinal_batch_without_skipping_coverage() {
    let d = tempfile::tempdir().unwrap();
    let mut raw: Value =
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap();
    raw["models"][0]["bootstrap"] = json!(true);
    raw["models"][0]["unique"] = json!([["text"]]);
    let mut c = Client::open_bound(
        SqliteStore::open_exclusive(d.path().join("db")).unwrap(),
        Schema::from_value(raw).unwrap(),
        v04::StoreBinding {
            backend: "b".into(),
            viewer: "a".into(),
            stream: "User:a".into(),
            contract: "app".into(),
        },
    )
    .unwrap();
    let ctx = c.request_context().unwrap().clone();
    c.apply_cache04(
        &ctx,
        &[v04::ReadRecord {
            key: RecordKey {
                model: "Entry".into(),
                identity: json!({"id":"untracked"}),
            },
            cursor: v04::NullCursor,
            state: json!({"text":"occupied","note":null}),
        }],
        true,
    )
    .unwrap();
    let mut w = DownlinkWorker::default();
    w.handle(&mut c, DownlinkEvent::Start, 1, 7).unwrap();
    let actions = gather(&mut w, &mut c);
    let (start, _) = request(&actions, "start");
    let actions = answer(
        &mut w,
        &mut c,
        start,
        json!({"context":ctx,"manifestId":"m","start":40,"total":2}),
    );
    let (page, initial) = request(&actions, "page");
    let item = |ordinal, id, text| json!({"ordinal":ordinal,"change":{"kind":"upsert","record":{"model":"Entry","identity":{"id":id},"cursor":40,"state":{"text":text,"note":null}}}});
    w.handle(&mut c,DownlinkEvent::Response{request:page,body:json!({"context":ctx,"manifestId":"m","total":2,"from":0,"to":2,"items":[item(0,"a","free"),item(1,"b","occupied")]}).to_string()},1,7).unwrap();
    assert!(w.handle(&mut c, DownlinkEvent::Next, 1, 7).is_err());
    assert_eq!(c.bootstrap_coverage04().unwrap().unwrap().covered, 0);
    let mut actions = Vec::new();
    for _ in 0..5 {
        actions.extend(w.handle(&mut c, DownlinkEvent::Next, 100000, 7).unwrap());
    }
    let (retry, intent) = request(&actions, "page");
    assert_eq!(intent["from"], 0);
    assert_eq!(intent["limit"], 64);
    assert_ne!(intent["callId"], initial["callId"]);
    answer(
        &mut w,
        &mut c,
        retry,
        json!({"context":ctx,"manifestId":"m","total":2,"from":0,"to":1,"items":[item(0,"a","free")]}),
    );
    assert_eq!(c.bootstrap_coverage04().unwrap().unwrap().covered, 1);
    assert_eq!(c.stream_cursor04().unwrap(), 40);
}
