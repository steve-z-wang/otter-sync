//! Actual v04 server entrypoints and host ordering; PostgreSQL races are separate.
use axton_core::v04::{self, FetchIntent, RequestContext, StoreBinding};
use axton_server::{Config, Host, HostResult, process_fetch};
use serde_json::{Value, json};
use std::{
    future::Future,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll, Waker},
};
fn run<T>(future: impl Future<Output = T>) -> T {
    let mut f = std::pin::pin!(future);
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(v) = f.as_mut().poll(&mut cx) {
            return v;
        }
    }
}
fn context() -> RequestContext {
    RequestContext {
        protocol: 4,
        binding: StoreBinding {
            backend: "api".into(),
            viewer: "alice".into(),
            stream: "User:alice".into(),
            contract: "app".into(),
        },
        materialization: config().protocol4.unwrap().materialization_id,
        incarnation: "store1".into(),
    }
}
fn config() -> Config {
    Config::decode(json!({"protocol4":{"backendId":"api","contractId":"app"},"schema":{"enums":[],"actions":[],"models":[{"name":"Todo","version":1,"identity":["id"],"fields":[{"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"}},{"name":"title","nullable":false,"type":{"kind":"scalar","name":"string"}}]}]},"loaders":["Todo"],"mutations":[]})).unwrap()
}
#[derive(Default)]
struct Recording(Mutex<Vec<Value>>);
impl Host for Recording {
    fn call(&self, r: Value) -> Pin<Box<dyn Future<Output = HostResult<Value>> + Send + '_>> {
        Box::pin(async move {
            self.0.lock().unwrap().push(r.clone());
            Ok(match r["op"].as_str().unwrap() {
                "claimCall" => json!({"fresh":true,"request":r["request"],"response":null}),
                "load" if r["mode"] == "prepare" => json!([]),
                "load" => {
                    if r["version"] == 2 {
                        json!([{"id":"t1","title":"A","done":true}])
                    } else {
                        json!([{"id":"t1","title":"A"}])
                    }
                }
                "admitContext" => json!(true),
                "handleAction" if r["name"] == "Get" => {
                    json!({"outputs":{"todo":{"id":"t1"}},"changes":[],"declarations":[]})
                }
                "handleAction" => {
                    json!({"outputs":{"message":"ok"},"changes":[],"declarations":[{"kind":"track","stream":"User:alice","record":{"model":"Todo","identity":{"id":"t1"}}}]})
                }
                "head" => json!(3),
                "readPublicationGroups" => {
                    json!([{ "from":0,"through":1,"keys":[{"model":"Todo","identityKey":"{\"id\":\"t1\"}"}]}])
                }
                "readPositions" => {
                    json!([{ "stream":"User:alice","model":"Todo","identityKey":"{\"id\":\"t1\"}","cursor":3,"kind":"upsert"}])
                }
                "readTracking" => json!([]),
                "guardRecords" => json!([7]),
                "applyStreamMembers" => {
                    json!([{"stream":"User:alice","model":"Todo","identityKey":"{\"id\":\"t1\"}","cursor":1,"kind":"upsert"}])
                }
                _ => Value::Null,
            })
        })
    }
}
#[test]
fn actual_fetch_returns_null_cursor_in_both_modes_without_stream_or_stamp_reads() {
    for store in [true, false] {
        let host = Recording::default();
        let intent = FetchIntent {
            context: context(),
            call_id: "01890f47-1234-7123-8123-123456789ab1".into(),
            model: "Todo".into(),
            version: 1,
            identity: json!({"id":"t1"}),
            store,
        };
        let raw = run(process_fetch(
            &config(),
            "alice",
            &v04::encode(&intent).unwrap(),
            &host,
        ))
        .unwrap();
        let response = v04::decode(raw.as_bytes()).unwrap();
        intent.admit_response(&response, &context()).unwrap();
        let ops = host.0.lock().unwrap();
        assert_eq!(ops.iter().filter(|v| v["op"] == "load").count(), 1);
        assert!(!ops.iter().any(|v| {
            [
                "ensureStamp",
                "readStamps",
                "head",
                "scan",
                "applyStreamMembers",
            ]
            .contains(&v["op"].as_str().unwrap())
        }));
    }
}
#[test]
fn actual_fetch_refuses_detached_viewer_before_claim() {
    let host = Recording::default();
    let intent = FetchIntent {
        context: context(),
        call_id: "01890f47-1234-7123-8123-123456789ab1".into(),
        model: "Todo".into(),
        version: 1,
        identity: json!({"id":"t1"}),
        store: true,
    };
    assert!(
        run(process_fetch(
            &config(),
            "bob",
            &v04::encode(&intent).unwrap(),
            &host
        ))
        .is_err()
    );
    assert!(
        !host
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|v| v["op"] == "claimCall")
    );
}

#[test]
fn actual_query_permits_track_and_fences_before_publication() {
    let mut cfg = serde_json::to_value(config()).unwrap();
    cfg["schema"]["actions"] = json!([{"name":"Find","version":1,"kind":"query","inputs":[],"outputs":[{"name":"message","kind":"value","source":"handlerValue","cardinality":"single","type":{"kind":"scalar","name":"string"}}]}]);
    let config = Config::decode(cfg).unwrap();
    let request = v04::ReadIntent {
        context: context(),
        call_id: "01890f47-1234-7123-8123-123456789ab2".into(),
        name: "Find".into(),
        version: 1,
        args: json!({}),
        store: false,
    };
    let host = Recording::default();
    let raw = run(axton_server::process_action(
        &config,
        "alice",
        &v04::encode(&request).unwrap(),
        &host,
    ))
    .unwrap();
    let response: v04::ReadResponse = v04::decode(raw.as_bytes()).unwrap();
    assert_eq!(
        response.completion.outcome,
        axton_core::ActionOutcome::Succeeded {
            result: json!({"message":"ok"})
        }
    );
    let ops = host.0.lock().unwrap();
    let at = |op| ops.iter().position(|v| v["op"] == op).unwrap();
    assert!(at("publicationFence") < at("applyStreamMembers"));
}

#[test]
fn retained_query_result_and_current_materialization_cache_use_distinct_versions() {
    let old = serde_json::to_value(config()).unwrap();
    let fields = old["schema"]["models"][0]["fields"].clone();
    let mut value = old.clone();
    value["schema"]["models"][0]["version"] = json!(2);
    value["schema"]["models"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"done","nullable":true,"type":{"kind":"scalar","name":"boolean"}}));
    value["models"] = json!([{"name":"Todo","version":1,"identity":["id"],"fields":fields,"enums":[]},{"name":"Todo","version":2,"identity":["id"],"fields":value["schema"]["models"][0]["fields"],"enums":[]}]);
    value["schema"]["resultModels"] =
        json!([{"name":"Todo","version":1,"identity":["id"],"fields":fields,"enums":[]}]);
    value["schema"]["actions"] = json!([{"name":"Get","version":1,"kind":"query","inputs":[],"outputs":[{"name":"todo","kind":"model","cardinality":"single","source":"handlerIdentity","model":"Todo","modelReadVersion":1,"handlerType":{"kind":"identity","model":"Todo","fields":[{"name":"id","type":{"kind":"scalar","name":"string"}}]}}]}]);
    value["protocol4"]
        .as_object_mut()
        .unwrap()
        .remove("materializationId");
    let cfg = Config::decode(value).unwrap();
    let mut active = context();
    active.materialization = cfg.protocol4.as_ref().unwrap().materialization_id.clone();
    let host = Recording::default();
    let req = v04::ReadIntent {
        context: active,
        call_id: "01890f47-1234-7123-8123-123456789ab3".into(),
        name: "Get".into(),
        version: 1,
        args: json!({}),
        store: true,
    };
    let raw = run(axton_server::process_action(
        &cfg,
        "alice",
        &v04::encode(&req).unwrap(),
        &host,
    ))
    .unwrap();
    let reply: v04::ReadResponse = v04::decode(raw.as_bytes()).unwrap();
    assert_eq!(
        reply.completion.outcome,
        axton_core::ActionOutcome::Succeeded {
            result: json!({"todo":{"id":"t1","title":"A"}})
        }
    );
    assert_eq!(reply.records[0].state, json!({"title":"A","done":true}));
}

#[test]
fn actual_delta_resolves_retained_group_identity_at_its_current_position() {
    let request = v04::DeltaIntent {
        context: context(),
        call_id: "01890f47-1234-7123-8123-123456789ab4".into(),
        after: 0,
        models: std::collections::BTreeMap::from([("Todo".into(), 1)]),
        limit: 1,
    };
    let host = Recording::default();
    let raw = run(axton_server::process_stream_pull(
        &config(),
        "alice",
        &v04::encode(&request).unwrap(),
        &host,
    ))
    .unwrap();
    let page: v04::DeltaPage = v04::decode(raw.as_bytes()).unwrap();
    assert_eq!(page.units.len(), 1);
    assert_eq!(page.units[0].through, 1);
    assert_eq!(page.units[0].changes[0].cursor(), 3);
    assert_eq!(page.to, 1);
    assert_eq!(page.head, 3);
    let ops = host.0.lock().unwrap();
    assert!(
        ops.iter()
            .position(|v| v["op"] == "publicationFence")
            .unwrap()
            < ops
                .iter()
                .position(|v| v["op"] == "readPublicationGroups")
                .unwrap()
    );
}

#[test]
fn active_compatible_authority_can_complete_retained_receipt_without_old_g() {
    let old = context();
    let mut active = old.clone();
    active.materialization = "schema-new".into();
    let intent = v04::MutationIntent {
        context: old.clone(),
        call_id: "01890f47-1234-7123-8123-123456789ab9".into(),
        name: "Edit".into(),
        version: 1,
        args: json!({}),
        models: std::collections::BTreeMap::from([("Todo".into(), 1)]),
    };
    let target = v04::SettlementTarget::Stream {
        key: axton_core::RecordKey {
            model: "Todo".into(),
            identity: json!({"id":"t1"}),
        },
        cursor: 58,
        fallback: v04::ReadRecord {
            key: axton_core::RecordKey {
                model: "Todo".into(),
                identity: json!({"id":"t1"}),
            },
            cursor: v04::NullCursor,
            state: json!({"title":"old"}),
        },
    };
    let receipt = v04::MutationReceipt {
        context: old.clone(),
        intent_digest: intent.digest().unwrap(),
        completion: axton_core::CallCompletion {
            call_id: intent.call_id.clone(),
            outcome: axton_core::ActionOutcome::Succeeded { result: json!({}) },
        },
        targets: vec![target.clone()],
    };
    receipt.admit(&intent, &active).unwrap();
    let mut evidence = v04::RecordEvidence::default();
    evidence.install(&old.materialization, 57, false).unwrap();
    evidence
        .install(&active.materialization, 58, false)
        .unwrap();
    assert_eq!(
        target.disposition(&old.materialization, &evidence).unwrap(),
        v04::SettlementDisposition::AwaitStream
    );
    assert_eq!(
        target
            .disposition(&active.materialization, &evidence)
            .unwrap(),
        v04::SettlementDisposition::InstalledStream
    );
}
