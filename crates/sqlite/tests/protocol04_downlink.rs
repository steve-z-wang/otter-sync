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

fn ready_delta_fixture() -> (
    tempfile::TempDir,
    Client<SqliteStore>,
    DownlinkWorker,
    u64,
    u64,
    v04::DeltaIntent,
) {
    ready_delta_fixture_with_unique(false)
}

fn ready_delta_fixture_with_unique(
    unique: bool,
) -> (
    tempfile::TempDir,
    Client<SqliteStore>,
    DownlinkWorker,
    u64,
    u64,
    v04::DeltaIntent,
) {
    ready_delta_fixture_options(unique, false)
}

fn ready_delta_fixture_options(
    unique: bool,
    float_note: bool,
) -> (
    tempfile::TempDir,
    Client<SqliteStore>,
    DownlinkWorker,
    u64,
    u64,
    v04::DeltaIntent,
) {
    let dir = tempfile::tempdir().unwrap();
    let mut raw: Value =
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap();
    if unique {
        raw["models"][0]["unique"] = json!([["text"]]);
    }
    if float_note {
        raw["models"][0]["fields"][2]["type"]["name"] = json!("float");
    }
    let mut client = Client::open_bound(
        SqliteStore::open_exclusive(dir.path().join("db")).unwrap(),
        Schema::from_value(raw).unwrap(),
        v04::StoreBinding {
            backend: "b".into(),
            viewer: "a".into(),
            stream: "User:a".into(),
            contract: "app".into(),
        },
    )
    .unwrap();
    let context = client.request_context().unwrap().clone();
    client
        .start_bootstrap04(&v04::BootstrapStarted {
            context: context.clone(),
            manifest_id: "ready".into(),
            start: 0,
            total: 0,
        })
        .unwrap();
    client
        .capture_bootstrap_tail04(&v04::BootstrapTail {
            context: context.clone(),
            manifest_id: "ready".into(),
            head: 3,
        })
        .unwrap();
    let mut worker = DownlinkWorker::default();
    worker
        .handle(&mut client, DownlinkEvent::Start, 1, 7)
        .unwrap();
    let opened = gather(&mut worker, &mut client);
    let epoch = opened
        .iter()
        .find_map(|action| match action {
            DownlinkAction::Open { epoch, .. } => Some(*epoch),
            _ => None,
        })
        .unwrap();
    worker
        .handle(
            &mut client,
            DownlinkEvent::Message {
                epoch,
                body: json!({"context":context,"cursor":0,"head":3}).to_string(),
            },
            1,
            7,
        )
        .unwrap();
    let actions = gather(&mut worker, &mut client);
    let (request, intent) = delta_request(&actions);
    (dir, client, worker, epoch, request, intent)
}

fn delta_request(actions: &[DownlinkAction]) -> (u64, v04::DeltaIntent) {
    actions
        .iter()
        .find_map(|action| match action {
            DownlinkAction::Request {
                request,
                body,
                bootstrap: false,
            } => Some((*request, v04::decode(body.as_bytes()).unwrap())),
            _ => None,
        })
        .unwrap_or_else(|| panic!("missing Delta request: {actions:?}"))
}

fn delta_page(
    context: &v04::RequestContext,
    id: &str,
    from: u64,
    to: u64,
    prefix: &str,
) -> v04::DeltaPage {
    v04::DeltaPage {
        context: context.clone(),
        page_id: id.into(),
        from,
        to,
        head: to.max(3),
        units: ((from + 1)..=to)
            .map(|through| v04::CommitUnit {
                through,
                changes: vec![v04::StreamChange::Upsert {
                    record: v04::StreamRecord {
                        key: RecordKey {
                            model: "Entry".into(),
                            identity: json!({"id":format!("e{through}")}),
                        },
                        cursor: through,
                        state: json!({"text":format!("{prefix}{through}"),"note":null}),
                    },
                }],
            })
            .collect(),
    }
}

fn page_text(client: &mut Client<SqliteStore>, id: u64) -> Option<Value> {
    client
        .read(&RecordKey {
            model: "Entry".into(),
            identity: json!({"id":format!("e{id}")}),
        })
        .unwrap()
        .map(|row| row["text"].clone())
}

#[test]
fn longer_late_http_page_cannot_preempt_partly_committed_live_plan_and_suffix_is_repulled() {
    let (_dir, mut client, mut worker, epoch, request, intent) = ready_delta_fixture();
    let context = client.request_context().unwrap().clone();
    let live = delta_page(&context, "live-plan", 0, 2, "canonical");
    worker
        .handle(
            &mut client,
            DownlinkEvent::Message {
                epoch,
                body: String::from_utf8(v04::encode(&live).unwrap()).unwrap(),
            },
            1,
            7,
        )
        .unwrap();
    next(&mut worker, &mut client);
    assert_eq!(client.stream_cursor04().unwrap(), 1);
    assert_eq!(
        client.delta_progress04().unwrap().unwrap().page_id,
        "live-plan"
    );
    assert!(!client.bootstrap_complete04().unwrap());
    let http = delta_page(&context, &intent.call_id, 0, 3, "canonical");
    let mut actions = answer(
        &mut worker,
        &mut client,
        request,
        serde_json::to_value(http).unwrap(),
    );
    actions.extend(gather(&mut worker, &mut client));
    assert_eq!(client.stream_cursor04().unwrap(), 2);
    assert_eq!(page_text(&mut client, 1), Some(json!("canonical1")));
    assert_eq!(page_text(&mut client, 2), Some(json!("canonical2")));
    assert_eq!(page_text(&mut client, 3), None);
    let (request, suffix) = delta_request(&actions);
    assert_eq!(suffix.after, 2);
    assert_ne!(suffix.call_id, intent.call_id);
    let fresh = delta_page(&context, &suffix.call_id, 2, 3, "canonical");
    answer(
        &mut worker,
        &mut client,
        request,
        serde_json::to_value(fresh).unwrap(),
    );
    assert_eq!(client.stream_cursor04().unwrap(), 3);
    assert_eq!(page_text(&mut client, 3), Some(json!("canonical3")));
    assert!(client.bootstrap_complete04().unwrap());
}

#[test]
fn longer_live_page_after_short_http_prefix_repulls_unseen_suffix_without_replaying_prefix() {
    let (_dir, mut client, mut worker, epoch, request, intent) = ready_delta_fixture();
    let context = client.request_context().unwrap().clone();
    let http = delta_page(&context, &intent.call_id, 0, 1, "canonical");
    let actions = answer(
        &mut worker,
        &mut client,
        request,
        serde_json::to_value(http).unwrap(),
    );
    assert_eq!(client.stream_cursor04().unwrap(), 1);
    let (request, suffix) = delta_request(&actions);
    assert_eq!(suffix.after, 1);
    let live = delta_page(&context, "long-live-plan", 0, 3, "canonical");
    worker
        .handle(
            &mut client,
            DownlinkEvent::Message {
                epoch,
                body: String::from_utf8(v04::encode(&live).unwrap()).unwrap(),
            },
            1,
            7,
        )
        .unwrap();
    gather(&mut worker, &mut client);
    assert_eq!(client.stream_cursor04().unwrap(), 1);
    assert_eq!(page_text(&mut client, 1), Some(json!("canonical1")));
    assert_eq!(page_text(&mut client, 2), None);
    let fresh = delta_page(&context, &suffix.call_id, 1, 3, "canonical");
    answer(
        &mut worker,
        &mut client,
        request,
        serde_json::to_value(fresh).unwrap(),
    );
    assert_eq!(client.stream_cursor04().unwrap(), 3);
    assert_eq!(page_text(&mut client, 1), Some(json!("canonical1")));
    assert_eq!(page_text(&mut client, 2), Some(json!("canonical2")));
    assert_eq!(page_text(&mut client, 3), Some(json!("canonical3")));
    assert!(client.bootstrap_complete04().unwrap());
}

fn start_live_plan(
    client: &mut Client<SqliteStore>,
    worker: &mut DownlinkWorker,
    epoch: u64,
    page: &v04::DeltaPage,
) {
    worker
        .handle(
            client,
            DownlinkEvent::Message {
                epoch,
                body: serde_json::to_string(page).unwrap(),
            },
            1,
            7,
        )
        .unwrap();
    next(worker, client);
    assert_eq!(client.stream_cursor04().unwrap(), 1);
}

#[test]
fn admitted_overlap_head_drives_fresh_suffix_without_wake_and_does_not_expand_bootstrap_tail() {
    let (_dir, mut c, mut worker, epoch, request, intent) = ready_delta_fixture();
    let ctx = c.request_context().unwrap().clone();
    start_live_plan(
        &mut c,
        &mut worker,
        epoch,
        &delta_page(&ctx, "live", 0, 2, "canonical"),
    );
    let mut actions = answer(
        &mut worker,
        &mut c,
        request,
        serde_json::to_value(delta_page(&ctx, &intent.call_id, 0, 4, "canonical")).unwrap(),
    );
    actions.extend(gather(&mut worker, &mut c));
    let (request, suffix) = delta_request(&actions);
    assert_eq!(suffix.after, 2);
    assert_eq!(page_text(&mut c, 3), None);
    let mut fresh = delta_page(&ctx, &suffix.call_id, 2, 3, "canonical");
    fresh.head = 4;
    let actions = answer(
        &mut worker,
        &mut c,
        request,
        serde_json::to_value(fresh).unwrap(),
    );
    assert_eq!(c.stream_cursor04().unwrap(), 3);
    assert!(
        c.bootstrap_complete04().unwrap(),
        "captured tail stays 3, despite observed head 4"
    );
    let (request, suffix) = delta_request(&actions);
    assert_eq!(suffix.after, 3);
    answer(
        &mut worker,
        &mut c,
        request,
        serde_json::to_value(delta_page(&ctx, &suffix.call_id, 3, 4, "canonical")).unwrap(),
    );
    assert_eq!(page_text(&mut c, 4), Some(json!("canonical4")));
    assert_eq!(c.stream_cursor04().unwrap(), 4);
}

#[test]
fn same_plan_tamper_is_visible_without_preempting_verified_active_suffix() {
    let (_dir, mut c, mut worker, epoch, _, _) = ready_delta_fixture();
    let ctx = c.request_context().unwrap().clone();
    let page = delta_page(&ctx, "live", 0, 2, "canonical");
    start_live_plan(&mut c, &mut worker, epoch, &page);
    let mut tampered = page.clone();
    tampered.units[1].changes = delta_page(&ctx, "unused", 1, 2, "tampered")
        .units
        .remove(0)
        .changes;
    worker
        .handle(
            &mut c,
            DownlinkEvent::Message {
                epoch,
                body: serde_json::to_string(&tampered).unwrap(),
            },
            1,
            7,
        )
        .unwrap();
    let error = worker
        .handle(&mut c, DownlinkEvent::Next, 1, 7)
        .unwrap_err();
    assert!(
        error.to_string().contains("page resume mismatch"),
        "{error}"
    );
    assert_eq!(c.stream_cursor04().unwrap(), 1);
    assert_eq!(page_text(&mut c, 2), None);
    next(&mut worker, &mut c);
    assert_eq!(page_text(&mut c, 2), Some(json!("canonical2")));
}

#[test]
fn same_plan_replay_resumes_original_frozen_units() {
    let (_dir, mut c, mut worker, epoch, _, _) = ready_delta_fixture();
    let ctx = c.request_context().unwrap().clone();
    let page = delta_page(&ctx, "live", 0, 2, "canonical");
    start_live_plan(&mut c, &mut worker, epoch, &page);
    worker
        .handle(
            &mut c,
            DownlinkEvent::Message {
                epoch,
                body: serde_json::to_string(&page).unwrap(),
            },
            1,
            7,
        )
        .unwrap();
    gather(&mut worker, &mut c);
    assert_eq!(c.stream_cursor04().unwrap(), 2);
    assert_eq!(page_text(&mut c, 1), Some(json!("canonical1")));
    assert_eq!(page_text(&mut c, 2), Some(json!("canonical2")));
}

#[test]
fn transport_failure_and_lane_controls_retain_active_plan() {
    for control in ["failed", "closed", "overflow", "pause", "stop"] {
        let (_dir, mut c, mut worker, epoch, request, _) = ready_delta_fixture();
        let ctx = c.request_context().unwrap().clone();
        start_live_plan(
            &mut c,
            &mut worker,
            epoch,
            &delta_page(&ctx, "live", 0, 2, "canonical"),
        );
        let event = match control {
            "failed" => DownlinkEvent::Failed {
                request,
                reason: Some("transport".into()),
                status: Some(500),
            },
            "closed" => DownlinkEvent::Closed { epoch },
            "overflow" => DownlinkEvent::Overflow { epoch },
            "pause" => DownlinkEvent::Pause,
            _ => DownlinkEvent::Stop,
        };
        worker.handle(&mut c, event, 1, 7).unwrap();
        next(&mut worker, &mut c);
        assert_eq!(c.stream_cursor04().unwrap(), 1, "{control}");
        worker
            .handle(
                &mut c,
                if control == "stop" {
                    DownlinkEvent::Start
                } else {
                    DownlinkEvent::Resume
                },
                100000,
                7,
            )
            .unwrap();
        for _ in 0..4 {
            worker
                .handle(&mut c, DownlinkEvent::Next, 100000, 7)
                .unwrap();
        }
        assert_eq!(c.stream_cursor04().unwrap(), 2, "{control}");
        assert_eq!(page_text(&mut c, 2), Some(json!("canonical2")), "{control}");
    }
}

#[test]
fn actual_reopen_restores_active_frozen_plan_before_new_carriers() {
    let (dir, mut c, mut worker, epoch, _, _) = ready_delta_fixture();
    let ctx = c.request_context().unwrap().clone();
    start_live_plan(
        &mut c,
        &mut worker,
        epoch,
        &delta_page(&ctx, "live", 0, 2, "canonical"),
    );
    drop(worker);
    drop(c);
    let mut c = Client::open_bound(
        SqliteStore::open_exclusive(dir.path().join("db")).unwrap(),
        Schema::from_value(
            serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap(),
        )
        .unwrap(),
        ctx.binding,
    )
    .unwrap();
    let mut worker = DownlinkWorker::default();
    worker.handle(&mut c, DownlinkEvent::Start, 1, 7).unwrap();
    gather(&mut worker, &mut c);
    assert_eq!(c.stream_cursor04().unwrap(), 2);
    assert_eq!(page_text(&mut c, 1), Some(json!("canonical1")));
    assert_eq!(page_text(&mut c, 2), Some(json!("canonical2")));
}

#[test]
fn invalid_page_or_context_cannot_raise_catchup_demand() {
    for wrong_context in [false, true] {
        let (_dir, mut c, mut worker, epoch, request, intent) = ready_delta_fixture();
        let ctx = c.request_context().unwrap().clone();
        let mut invalid = delta_page(&ctx, "invalid", 0, 4, "canonical");
        if wrong_context {
            invalid.context.binding.viewer = "other".into();
        } else {
            invalid.units.pop();
        }
        worker
            .handle(
                &mut c,
                DownlinkEvent::Message {
                    epoch,
                    body: serde_json::to_string(&invalid).unwrap(),
                },
                1,
                7,
            )
            .unwrap();
        assert!(worker.handle(&mut c, DownlinkEvent::Next, 1, 7).is_err());
        assert_eq!(c.stream_cursor04().unwrap(), 0);
        let actions = answer(
            &mut worker,
            &mut c,
            request,
            serde_json::to_value(delta_page(&ctx, &intent.call_id, 0, 3, "canonical")).unwrap(),
        );
        assert_eq!(c.stream_cursor04().unwrap(), 3);
        assert!(
            !actions.iter().any(|action| matches!(
                action,
                DownlinkAction::Request {
                    bootstrap: false,
                    ..
                }
            )),
            "invalid head cannot request suffix: {actions:?}"
        );
    }
}

#[test]
fn failed_atomic_unit_preserves_prefix_and_allows_fresh_smaller_independent_recovery() {
    let (_dir, mut c, mut worker, epoch, _, _) = ready_delta_fixture_with_unique(true);
    let ctx = c.request_context().unwrap().clone();
    let mut page = delta_page(&ctx, "failing", 0, 3, "canonical");
    // Both independent changes share one atomic unit. Its local failure
    // must roll back the first too, without retaining a carrier deadlock.
    let last = page.units.pop().unwrap();
    page.units[1].through = 3;
    page.units[1].changes.extend(last.changes);
    start_live_plan(&mut c, &mut worker, epoch, &page);
    // Current device-only state causes a genuine UNIQUE failure. All
    // server canonical payloads remain identical across retry pages.
    c.transaction(|tx| {
        tx.direct(axton_client::Operation {
            model: "Entry".into(),
            identity: json!({"id":"local-conflict"}),
            op: axton_client::OperationKind::Create,
            values: Some(json!({"text":"canonical3","note":null})),
        })
    })
    .unwrap();
    let error = worker
        .handle(&mut c, DownlinkEvent::Next, 1, 7)
        .unwrap_err();
    assert!(error.to_string().contains("UNIQUE"), "{error}");
    assert_eq!(c.stream_cursor04().unwrap(), 1);
    assert_eq!(page_text(&mut c, 1), Some(json!("canonical1")));
    assert_eq!(page_text(&mut c, 2), None);
    assert_eq!(page_text(&mut c, 3), None);
    c.transaction(|tx| {
        tx.direct(axton_client::Operation {
            model: "Entry".into(),
            identity: json!({"id":"local-conflict"}),
            op: axton_client::OperationKind::Delete,
            values: None,
        })
    })
    .unwrap();
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
        .find_map(|action| match action {
            DownlinkAction::Open { epoch, .. } => Some(*epoch),
            _ => None,
        })
        .unwrap();
    worker
        .handle(
            &mut c,
            DownlinkEvent::Message {
                epoch,
                body: json!({"context":ctx,"cursor":1,"head":3}).to_string(),
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
    let (request, intent) = delta_request(&actions);
    assert_eq!(intent.after, 1);
    assert_eq!(intent.limit, 64);
    let actions = answer_after_retry(
        &mut worker,
        &mut c,
        request,
        serde_json::to_value(delta_page(&ctx, &intent.call_id, 1, 2, "canonical")).unwrap(),
    );
    let (request, intent) = delta_request(&actions);
    assert_eq!(intent.after, 2);
    answer_after_retry(
        &mut worker,
        &mut c,
        request,
        serde_json::to_value(delta_page(&ctx, &intent.call_id, 2, 3, "canonical")).unwrap(),
    );
    assert_eq!(c.stream_cursor04().unwrap(), 3);
    assert_eq!(page_text(&mut c, 2), Some(json!("canonical2")));
    assert_eq!(page_text(&mut c, 3), Some(json!("canonical3")));
}

fn answer_after_retry(
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
            100000,
            7,
        )
        .unwrap();
    let mut out = Vec::new();
    for _ in 0..20 {
        let actions = worker.handle(c, DownlinkEvent::Next, 100000, 7).unwrap();
        let stop = actions.is_empty()
            || actions
                .iter()
                .any(|a| matches!(a, DownlinkAction::Wait { .. }));
        out.extend(actions);
        if stop {
            break;
        }
    }
    out
}

#[test]
fn ahead_page_does_not_starve_prefix_http_response() {
    let (_dir, mut c, mut worker, epoch, request, intent) = ready_delta_fixture();
    let ctx = c.request_context().unwrap().clone();
    worker
        .handle(
            &mut c,
            DownlinkEvent::Message {
                epoch,
                body: serde_json::to_string(&delta_page(&ctx, "ahead", 2, 3, "canonical")).unwrap(),
            },
            1,
            7,
        )
        .unwrap();
    gather(&mut worker, &mut c);
    assert_eq!(c.stream_cursor04().unwrap(), 0);
    let actions = answer(
        &mut worker,
        &mut c,
        request,
        serde_json::to_value(delta_page(&ctx, &intent.call_id, 0, 1, "canonical")).unwrap(),
    );
    let (request, intent) = delta_request(&actions);
    assert_eq!(intent.after, 1);
    answer(
        &mut worker,
        &mut c,
        request,
        serde_json::to_value(delta_page(&ctx, &intent.call_id, 1, 3, "canonical")).unwrap(),
    );
    assert_eq!(c.stream_cursor04().unwrap(), 3);
    assert_eq!(page_text(&mut c, 3), Some(json!("canonical3")));
}

#[test]
fn reset_discards_active_old_incarnation_plan() {
    let (_dir, mut c, mut worker, epoch, _, _) = ready_delta_fixture();
    let old = c.request_context().unwrap().clone();
    start_live_plan(
        &mut c,
        &mut worker,
        epoch,
        &delta_page(&old, "live", 0, 2, "canonical"),
    );
    c.reset_store04(true).unwrap();
    let actions = next(&mut worker, &mut c);
    assert!(
        actions
            .iter()
            .any(|action| matches!(action, DownlinkAction::Reset))
    );
    assert_eq!(c.stream_cursor04().unwrap(), 0);
    worker
        .handle(
            &mut c,
            DownlinkEvent::Message {
                epoch,
                body: serde_json::to_string(&delta_page(&old, "live", 0, 2, "canonical")).unwrap(),
            },
            1,
            7,
        )
        .unwrap();
    gather(&mut worker, &mut c);
    assert_eq!(c.stream_cursor04().unwrap(), 0);
    assert_eq!(page_text(&mut c, 1), None);
    assert_eq!(page_text(&mut c, 2), None);
}

#[test]
fn completed_plan_tamper_cannot_create_new_head_demand() {
    let (_dir, mut c, mut worker, epoch, request, intent) = ready_delta_fixture();
    let ctx = c.request_context().unwrap().clone();
    let page = delta_page(&ctx, &intent.call_id, 0, 3, "canonical");
    answer(
        &mut worker,
        &mut c,
        request,
        serde_json::to_value(&page).unwrap(),
    );
    assert_eq!(c.stream_cursor04().unwrap(), 3);
    let mut changed = page;
    changed.head = 4;
    worker
        .handle(
            &mut c,
            DownlinkEvent::Message {
                epoch,
                body: serde_json::to_string(&changed).unwrap(),
            },
            1,
            7,
        )
        .unwrap();
    let error = worker
        .handle(&mut c, DownlinkEvent::Next, 1, 7)
        .unwrap_err();
    assert!(error.to_string().contains("page resume mismatch"));
    let actions = gather(&mut worker, &mut c);
    assert!(!actions.iter().any(|a| matches!(
        a,
        DownlinkAction::Request {
            bootstrap: false,
            ..
        }
    )));
    assert_eq!(c.stream_cursor04().unwrap(), 3);
}

#[test]
fn same_plan_replay_uses_canonical_digest_for_equivalent_numeric_representation() {
    let (_dir, mut c, mut worker, epoch, _, _) = ready_delta_fixture_options(false, true);
    let ctx = c.request_context().unwrap().clone();
    let mut page = delta_page(&ctx, "numeric", 0, 2, "canonical");
    if let v04::StreamChange::Upsert { record } = &mut page.units[1].changes[0] {
        record.state["note"] = json!(1);
    }
    start_live_plan(&mut c, &mut worker, epoch, &page);
    let frozen_digest = c.delta_progress04().unwrap().unwrap().plan;
    if let v04::StreamChange::Upsert { record } = &mut page.units[1].changes[0] {
        record.state["note"] = json!(1.0);
    }
    assert_eq!(v04::PageProgress::new(&page).unwrap().plan, frozen_digest);
    worker
        .handle(
            &mut c,
            DownlinkEvent::Message {
                epoch,
                body: serde_json::to_string(&page).unwrap(),
            },
            1,
            7,
        )
        .unwrap();
    gather(&mut worker, &mut c);
    assert_eq!(c.stream_cursor04().unwrap(), 2);
    let row = c
        .read(&RecordKey {
            model: "Entry".into(),
            identity: json!({"id":"e2"}),
        })
        .unwrap()
        .unwrap();
    assert_eq!(row["note"].as_f64(), Some(1.0));
}
