use axton_client::{
    Client, Schema,
    runtime::{ClientRuntime, Input},
    v04,
};
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};
fn run(runtime: &mut ClientRuntime<SqliteStore>) -> Vec<Value> {
    while runtime.step(1, 7) {}
    runtime
        .take_events()
        .into_iter()
        .map(|e| serde_json::to_value(e).unwrap())
        .collect()
}
fn task(runtime: &mut ClientRuntime<SqliteStore>, id: &str, command: Value) {
    runtime
        .receive(
            serde_json::from_value::<Input>(
                json!({"type":"task","requestId":id,"command":command}),
            )
            .unwrap(),
            1,
            7,
        )
        .unwrap();
}
#[test]
fn bound_runtime_fetch_uses_v04_context_and_commits_cache_before_completion() {
    let dir = tempfile::tempdir().unwrap();
    let schema = Schema::from_value(
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap(),
    )
    .unwrap();
    let client = Client::open_bound(
        SqliteStore::open_exclusive(dir.path().join("db")).unwrap(),
        schema,
        v04::StoreBinding {
            backend: "b".into(),
            viewer: "a".into(),
            stream: "User:a".into(),
            contract: "app".into(),
        },
    )
    .unwrap();
    let context = client.request_context().unwrap().clone();
    let mut runtime = ClientRuntime::new(client);
    task(&mut runtime, "connect", json!({"kind":"connect"}));
    run(&mut runtime);
    task(
        &mut runtime,
        "fetch",
        json!({"kind":"fetch","model":"Entry","version":1,"identity":{"id":"e"},"store":true}),
    );
    let events = run(&mut runtime);
    let effect = events
        .iter()
        .find(|e| e["type"] == "effect" && e["operation"]["kind"] == "http")
        .unwrap();
    let request: v04::FetchIntent =
        v04::decode(effect["operation"]["body"].as_str().unwrap().as_bytes()).unwrap();
    assert_eq!(request.context, context);
    let response = json!({"context":context,"completion":{"callId":request.call_id,"outcome":{"status":"succeeded","result":{"id":"e","text":"server","note":null}}},"records":[{"model":"Entry","identity":{"id":"e"},"cursor":null,"state":{"text":"server","note":null}}]});
    runtime.receive(serde_json::from_value(json!({"type":"effectResult","effectId":effect["effectId"],"outcome":{"ok":true,"value":{"status":200,"body":response.to_string()}}})).unwrap(),1,7).unwrap();
    let events = run(&mut runtime);
    assert!(
        events
            .iter()
            .any(|e| e["type"] == "taskCompleted" && e["requestId"] == "fetch" && e["ok"] == true),
        "{events:?}"
    );
    assert_eq!(
        runtime
            .client()
            .read(&axton_client::RecordKey {
                model: "Entry".into(),
                identity: json!({"id":"e"})
            })
            .unwrap()
            .unwrap()["text"],
        "server"
    );
}
#[test]
fn bound_runtime_query_once_keeps_modes_separate_and_reuses_persisted_result() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("db");
    let mut raw: Value =
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap();
    raw["actions"] = json!([{"name":"Ping","kind":"query","version":1,"inputs":[],"outputs":[]}]);
    let schema = Schema::from_value(raw).unwrap();
    let binding = v04::StoreBinding {
        backend: "b".into(),
        viewer: "a".into(),
        stream: "User:a".into(),
        contract: "app".into(),
    };
    let mut runtime = ClientRuntime::new(
        Client::open_bound(
            SqliteStore::open_exclusive(&p).unwrap(),
            schema.clone(),
            binding.clone(),
        )
        .unwrap(),
    );
    task(&mut runtime, "connect", json!({"kind":"connect"}));
    run(&mut runtime);
    for (id, store) in [("false", false), ("true", true)] {
        task(
            &mut runtime,
            id,
            json!({"kind":"invoke","name":"Ping","version":1,"args":{},"once":true,"store":store}),
        );
        let events = run(&mut runtime);
        let effect = events
            .iter()
            .find(|e| e["type"] == "effect" && e["operation"]["kind"] == "http")
            .unwrap();
        let request: v04::ReadIntent =
            v04::decode(effect["operation"]["body"].as_str().unwrap().as_bytes()).unwrap();
        assert_eq!(request.store, store);
        let response = json!({"context":request.context,"completion":{"callId":request.call_id,"outcome":{"status":"succeeded","result":null}},"records":[]});
        runtime.receive(serde_json::from_value(json!({"type":"effectResult","effectId":effect["effectId"],"outcome":{"ok":true,"value":{"status":200,"body":response.to_string()}}})).unwrap(),1,7).unwrap();
        assert!(
            run(&mut runtime)
                .iter()
                .any(|e| e["type"] == "taskCompleted" && e["requestId"] == id && e["ok"] == true)
        );
    }
    drop(runtime);
    let mut runtime = ClientRuntime::new(
        Client::open_bound(SqliteStore::open_exclusive(&p).unwrap(), schema, binding).unwrap(),
    );
    task(
        &mut runtime,
        "hit",
        json!({"kind":"invoke","name":"Ping","version":1,"args":{},"once":true,"store":false}),
    );
    let events = run(&mut runtime);
    assert!(
        events
            .iter()
            .any(|e| e["type"] == "taskCompleted" && e["requestId"] == "hit" && e["ok"] == true),
        "{events:?}"
    );
    assert!(!events.iter().any(|e| e["operation"]["kind"] == "http"));
}
#[test]
fn bound_runtime_durable_mutation_uses_action_route_and_settles_private_target() {
    let d = tempfile::tempdir().unwrap();
    let mut raw: Value =
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap();
    raw["actions"] = json!([{"name":"Rename","version":1,"inputs":[{"kind":"model","name":"entry","model":"Entry","operation":"update","cardinality":"single"}],"outputs":[]}]);
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
    c.transaction(|tx| {
        tx.direct(axton_client::Operation {
            model: "Entry".into(),
            identity: json!({"id":"e"}),
            op: axton_client::OperationKind::Create,
            values: Some(json!({"text":"old","note":null})),
        })
    })
    .unwrap();
    let call = c
        .submit_action("Rename", 1, json!({"entry":{"id":"e","text":"optimistic"}}))
        .unwrap();
    let mut runtime = ClientRuntime::new(c);
    task(&mut runtime, "connect", json!({"kind":"connect"}));
    let events = run(&mut runtime);
    let effect = events
        .iter()
        .find(|e| {
            e["type"] == "effect"
                && e["operation"]["kind"] == "http"
                && e["operation"]["route"] == "action"
        })
        .unwrap();
    assert_eq!(effect["operation"]["route"], "action");
    let intent: v04::MutationIntent =
        v04::decode(effect["operation"]["body"].as_str().unwrap().as_bytes()).unwrap();
    assert_eq!(intent.call_id, call.call_id);
    let receipt = v04::MutationReceipt {
        context: intent.context.clone(),
        intent_digest: intent.digest().unwrap(),
        completion: axton_client::CallCompletion {
            call_id: call.call_id,
            outcome: axton_client::ActionOutcome::Succeeded {
                result: Value::Null,
            },
        },
        targets: vec![v04::SettlementTarget::Private {
            record: v04::ReadRecord {
                key: axton_client::RecordKey {
                    model: "Entry".into(),
                    identity: json!({"id":"e"}),
                },
                cursor: v04::NullCursor,
                state: json!({"text":"accepted","note":null}),
            },
        }],
    };
    runtime.receive(serde_json::from_value(json!({"type":"effectResult","effectId":effect["effectId"],"outcome":{"ok":true,"value":{"status":200,"body":String::from_utf8(v04::encode(&receipt).unwrap()).unwrap()}}})).unwrap(),1,7).unwrap();
    run(&mut runtime);
    assert_eq!(runtime.client().pending_count().unwrap(), 0);
    assert_eq!(
        runtime
            .client()
            .read(&axton_client::RecordKey {
                model: "Entry".into(),
                identity: json!({"id":"e"})
            })
            .unwrap()
            .unwrap()["text"],
        "accepted"
    );
}
fn effect_answer(runtime: &mut ClientRuntime<SqliteStore>, id: &Value, value: Value) -> Vec<Value> {
    runtime
        .receive(
            serde_json::from_value(
                json!({"type":"effectResult","effectId":id,"outcome":{"ok":true,"value":value}}),
            )
            .unwrap(),
            1,
            7,
        )
        .unwrap();
    run(runtime)
}
#[test]
fn actual_runtime_bootstrap_ack_tail_and_live_delta_keep_completion_behind_committed_prefix() {
    let d = tempfile::tempdir().unwrap();
    let c = Client::open_bound(
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
    let mut runtime = ClientRuntime::new(c);
    task(&mut runtime, "connect", json!({"kind":"connect"}));
    let events = run(&mut runtime);
    let start = events
        .iter()
        .find(|e| {
            e["operation"]["kind"] == "http"
                && serde_json::from_str::<Value>(e["operation"]["body"].as_str().unwrap()).unwrap()
                    ["kind"]
                    == "start"
        })
        .unwrap();
    let events = effect_answer(
        &mut runtime,
        &start["effectId"],
        json!({"status":200,"body":json!({"context":ctx,"manifestId":"m","start":100,"total":0}).to_string()}),
    );
    let socket = events
        .iter()
        .find(|e| e["operation"]["kind"] == "socket")
        .unwrap();
    let intent: v04::SubscribeIntent = v04::decode(
        socket["operation"]["subscribe"]
            .as_str()
            .unwrap()
            .as_bytes(),
    )
    .unwrap();
    assert_eq!(intent.cursor, 100);
    let tail = events
        .iter()
        .find(|e| {
            e["operation"]["kind"] == "http"
                && serde_json::from_str::<Value>(e["operation"]["body"].as_str().unwrap()).unwrap()
                    ["kind"]
                    == "tail"
        })
        .unwrap();
    effect_answer(
        &mut runtime,
        &socket["effectId"],
        json!({"event":"message","body":json!({"context":ctx,"cursor":100,"head":150}).to_string()}),
    );
    effect_answer(
        &mut runtime,
        &tail["effectId"],
        json!({"status":200,"body":json!({"context":ctx,"manifestId":"m","head":150}).to_string()}),
    );
    assert_eq!(runtime.client().stream_cursor04().unwrap(), 100);
    assert!(!runtime.client().bootstrap_complete04().unwrap());
    let delta = v04::DeltaPage {
        context: ctx,
        page_id: "live".into(),
        from: 100,
        to: 150,
        head: 150,
        units: vec![v04::CommitUnit {
            through: 150,
            changes: vec![v04::StreamChange::Upsert {
                record: v04::StreamRecord {
                    key: axton_client::RecordKey {
                        model: "Entry".into(),
                        identity: json!({"id":"e"}),
                    },
                    cursor: 150,
                    state: json!({"text":"live","note":null}),
                },
            }],
        }],
    };
    effect_answer(
        &mut runtime,
        &socket["effectId"],
        json!({"event":"message","body":String::from_utf8(v04::encode(&delta).unwrap()).unwrap()}),
    );
    assert_eq!(runtime.client().stream_cursor04().unwrap(), 150);
    assert!(runtime.client().bootstrap_complete04().unwrap());
    assert_eq!(
        runtime
            .client()
            .read(&axton_client::RecordKey {
                model: "Entry".into(),
                identity: json!({"id":"e"})
            })
            .unwrap()
            .unwrap()["text"],
        "live"
    );
}
fn http_kind(events: &[Value], kind: &str) -> Value {
    events
        .iter()
        .find(|e| {
            e["operation"]["kind"] == "http"
                && serde_json::from_str::<Value>(e["operation"]["body"].as_str().unwrap()).unwrap()
                    ["kind"]
                    == kind
        })
        .unwrap_or_else(|| panic!("missing {kind}: {events:?}"))
        .clone()
}
#[test]
fn historical_unmarked_receipt_target_materializes_while_public_bootstrap_page_waits() {
    let d = tempfile::tempdir().unwrap();
    let mut raw: Value =
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap();
    let mut boot = raw["models"][0].clone();
    boot["name"] = json!("Boot");
    boot["bootstrap"] = json!(true);
    raw["models"].as_array_mut().unwrap().push(boot);
    raw["actions"] = json!([{"name":"Rename","version":1,"inputs":[{"kind":"model","name":"entry","model":"Entry","operation":"update","cardinality":"single"}],"outputs":[]}]);
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
    c.transaction(|tx| {
        tx.direct(axton_client::Operation {
            model: "Entry".into(),
            identity: json!({"id":"e"}),
            op: axton_client::OperationKind::Create,
            values: Some(json!({"text":"cached","note":null})),
        })
    })
    .unwrap();
    let call = c
        .submit_action("Rename", 1, json!({"entry":{"id":"e","text":"optimistic"}}))
        .unwrap();
    let intent = c.mutation_intent04(&call.call_id).unwrap().unwrap();
    let mut runtime = ClientRuntime::new(c);
    task(&mut runtime, "connect", json!({"kind":"connect"}));
    let events = run(&mut runtime);
    let start = http_kind(&events, "start");
    let mutation = events
        .iter()
        .find(|e| e["operation"]["kind"] == "http" && e["operation"]["route"] == "action")
        .unwrap();
    let started = effect_answer(
        &mut runtime,
        &start["effectId"],
        json!({"status":200,"body":json!({"context":ctx,"manifestId":"public","start":80,"total":1}).to_string()}),
    );
    let _public_page = http_kind(&started, "page");
    assert_eq!(runtime.client().stream_cursor04().unwrap(), 80);
    let receipt = v04::MutationReceipt {
        context: ctx.clone(),
        intent_digest: intent.digest().unwrap(),
        completion: axton_client::CallCompletion {
            call_id: call.call_id.clone(),
            outcome: axton_client::ActionOutcome::Succeeded {
                result: Value::Null,
            },
        },
        targets: vec![v04::SettlementTarget::Stream {
            key: axton_client::RecordKey {
                model: "Entry".into(),
                identity: json!({"id":"e"}),
            },
            cursor: 57,
            fallback: v04::ReadRecord {
                key: axton_client::RecordKey {
                    model: "Entry".into(),
                    identity: json!({"id":"e"}),
                },
                cursor: v04::NullCursor,
                state: json!({"text":"canonical","note":null}),
            },
        }],
    };
    let events = effect_answer(
        &mut runtime,
        &mutation["effectId"],
        json!({"status":200,"body":String::from_utf8(v04::encode(&receipt).unwrap()).unwrap()}),
    );
    let materialize = http_kind(&events, "materialize");
    assert!(runtime.client().accepted_awaiting04(&call.call_id).unwrap());
    let events = effect_answer(
        &mut runtime,
        &materialize["effectId"],
        json!({"status":200,"body":json!({"context":ctx,"manifestId":"receipt","start":90,"total":1}).to_string()}),
    );
    let page = http_kind(&events, "page");
    let manifest = v04::ManifestPage {
        context: ctx,
        manifest_id: "receipt".into(),
        total: 1,
        from: 0,
        to: 1,
        items: vec![v04::ManifestItem {
            ordinal: 0,
            change: v04::StreamChange::Upsert {
                record: v04::StreamRecord {
                    key: axton_client::RecordKey {
                        model: "Entry".into(),
                        identity: json!({"id":"e"}),
                    },
                    cursor: 57,
                    state: json!({"text":"canonical","note":null}),
                },
            },
        }],
        companions: vec![],
    };
    let events = effect_answer(
        &mut runtime,
        &page["effectId"],
        json!({"status":200,"body":String::from_utf8(v04::encode(&manifest).unwrap()).unwrap()}),
    );
    assert!(
        events
            .iter()
            .any(|e| e["type"] == "callCompleted" && e["callId"] == call.call_id),
        "{events:?}"
    );
    assert_eq!(runtime.client().pending_count().unwrap(), 0);
    assert_eq!(runtime.client().stream_cursor04().unwrap(), 80);
    let coverage = runtime.client().bootstrap_coverage04().unwrap().unwrap();
    assert_eq!(coverage.manifest_id, "public");
    assert_eq!(coverage.covered, 0);
    assert_eq!(
        runtime
            .client()
            .read(&axton_client::RecordKey {
                model: "Entry".into(),
                identity: json!({"id":"e"})
            })
            .unwrap()
            .unwrap()["text"],
        "canonical"
    );
}

#[test]
fn bound_runtime_refuses_legacy_protocol_seams_and_custom_store_hooks() {
    let d = tempfile::tempdir().unwrap();
    let client = Client::open_bound(
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
    let mut runtime = ClientRuntime::new(client);
    for (id, command) in [
        ("freeze", json!({"kind":"freeze"})),
        ("pull", json!({"kind":"pull","page":{}})),
        (
            "enqueue",
            json!({"kind":"enqueue","mutation":{"name":"Edit","version":1,"operations":[{"model":"Entry","identity":{"id":"e"},"op":"create","values":{"text":"bad","note":null}}]}}),
        ),
    ] {
        task(&mut runtime, id, command);
        let events = run(&mut runtime);
        let outcome = events.iter().find(|e| e["requestId"] == id).unwrap();
        assert_eq!(outcome["ok"], false);
        assert!(
            outcome["error"]
                .as_str()
                .unwrap()
                .contains("legacy protocol seam"),
            "{outcome}"
        );
    }
    assert_eq!(runtime.client().pending_count().unwrap(), 0);
    assert_eq!(runtime.client().stream_cursor04().unwrap(), 0);
    assert!(runtime.register_store_hooks(vec!["Entry".into()]).is_err());
}

#[test]
fn native_call_completion_lookup_returns_persisted_terminal_result() {
    let d = tempfile::tempdir().unwrap();
    let mut raw: Value =
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap();
    raw["actions"] = json!([{"name":"Rename","version":1,"inputs":[{"kind":"model","name":"entry","model":"Entry","operation":"update","cardinality":"single"}],"outputs":[]}]);
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
    c.apply_cache04(
        &c.request_context().unwrap().clone(),
        &[v04::ReadRecord {
            key: axton_client::RecordKey {
                model: "Entry".into(),
                identity: json!({"id":"e"}),
            },
            cursor: v04::NullCursor,
            state: json!({"text":"old","note":null}),
        }],
        true,
    )
    .unwrap();
    let call = c
        .submit_action("Rename", 1, json!({"entry":{"id":"e","text":"new"}}))
        .unwrap();
    let completion = c.discard(call.ordinal).unwrap().remove(0);
    let mut r = ClientRuntime::new(c);
    assert!(serde_json::from_value::<Input>(json!({"type":"task","requestId":"lookup","command":{"kind":"callCompletion","callId":call.call_id}})).is_ok());
    task(
        &mut r,
        "lookup",
        json!({"kind":"callCompletion","callId":call.call_id}),
    );
    let events = run(&mut r);
    assert!(
        events.iter().any(|e| e["type"] == "taskCompleted"
            && e["requestId"] == "lookup"
            && e["value"] == serde_json::to_value(&completion).unwrap()),
        "{events:?}"
    );
}
