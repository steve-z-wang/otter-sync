//! Per-invocation `store` policy: which explicit Model outputs contribute
//! additional authority, on both routes, with saved replay and identity.
use axton_server::{Config, Host, HostResult, process_action, process_action_push};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll, Waker},
};

fn run<T>(future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut context) {
            return value;
        }
    }
}

const A: &str = "01890f47-1234-7123-8123-00000000000a";
const B: &str = "01890f47-1234-7123-8123-00000000000b";
const C: &str = "01890f47-1234-7123-8123-00000000000c";
const CALL: &str = "01890f47-1234-7123-8123-123456789ab1";
const CALL2: &str = "01890f47-1234-7123-8123-123456789ab2";

/// A backend with one Todo table, per-record stamps and saved calls.
#[derive(Default)]
struct State {
    rows: BTreeMap<String, String>,
    stamps: BTreeMap<String, u64>,
    next_stamp: u64,
    outputs: Value,
    extra: Vec<Value>,
    calls: BTreeMap<String, (String, Option<String>)>,
    sequence: u64,
    receipt: Option<String>,
    savepoint: Option<BTreeMap<String, String>>,
    log: Vec<Value>,
}
struct StoreHost(Mutex<State>);
impl StoreHost {
    fn new(outputs: Value) -> Self {
        let mut state = State {
            outputs,
            next_stamp: 10,
            ..State::default()
        };
        for (id, title) in [(A, "A"), (B, "B"), (C, "C")] {
            state.rows.insert(id.into(), title.into());
        }
        Self(Mutex::new(state))
    }
    fn ops(&self, op: &str) -> Vec<Value> {
        let state = self.0.lock().unwrap();
        state
            .log
            .iter()
            .filter(|r| r["op"] == op)
            .cloned()
            .collect()
    }
    fn clear_log(&self) {
        self.0.lock().unwrap().log.clear();
    }
    fn stamped(&self) -> Vec<String> {
        self.ops("ensureStamp")
            .iter()
            .map(|r| r["identityKey"].as_str().unwrap().to_string())
            .collect()
    }
    fn loads(&self) -> Vec<(u64, Vec<String>)> {
        self.ops("load")
            .iter()
            .map(|r| {
                (
                    r["version"].as_u64().unwrap(),
                    r["identities"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|i| i["id"].as_str().unwrap().to_string())
                        .collect(),
                )
            })
            .collect()
    }
}
impl Host for StoreHost {
    fn call(&self, request: Value) -> Pin<Box<dyn Future<Output = HostResult<Value>> + Send + '_>> {
        Box::pin(async move {
            let mut state = self.0.lock().unwrap();
            state.log.push(request.clone());
            let answer = match request["op"].as_str().unwrap() {
                "claim" => json!({"clientId":request["clientId"],"owner":"alice",
                    "sequence":state.sequence,"receipt":state.receipt}),
                "saveReceipt" => {
                    state.sequence = request["sequence"].as_u64().unwrap();
                    state.receipt = Some(request["receipt"].as_str().unwrap().into());
                    Value::Null
                }
                "claimCall" => {
                    let id = request["callId"].as_str().unwrap();
                    if let Some((stored, response)) = state.calls.get(id) {
                        json!({"fresh":false,"request":stored,"response":response})
                    } else {
                        let incoming = request["request"].as_str().unwrap().to_string();
                        state.calls.insert(id.into(), (incoming.clone(), None));
                        json!({"fresh":true,"request":incoming,"response":null})
                    }
                }
                "saveCall" => {
                    let id = request["callId"].as_str().unwrap();
                    state.calls.get_mut(id).unwrap().1 =
                        Some(request["response"].as_str().unwrap().into());
                    Value::Null
                }
                "savepoint" => {
                    state.savepoint = Some(state.rows.clone());
                    Value::Null
                }
                "rollback" => {
                    state.rows = state.savepoint.clone().unwrap();
                    Value::Null
                }
                "release" => {
                    state.savepoint = None;
                    Value::Null
                }
                "handleAction" => {
                    if let Some(todo) = request["arguments"]["todo"].as_object() {
                        state.rows.insert(
                            todo["id"].as_str().unwrap().into(),
                            todo["title"].as_str().unwrap().into(),
                        );
                    }
                    json!({"outputs":state.outputs,"changes":state.extra,"memberships":[]})
                }
                "advanceStamp" => {
                    state.next_stamp += 1;
                    let stamp = state.next_stamp;
                    let key = request["identityKey"].as_str().unwrap().to_string();
                    state.stamps.insert(key, stamp);
                    json!(stamp)
                }
                "ensureStamp" => {
                    let key = request["identityKey"].as_str().unwrap().to_string();
                    let existing = state.stamps.get(&key).copied();
                    let stamp = existing.unwrap_or_else(|| {
                        state.next_stamp += 1;
                        state.next_stamp
                    });
                    state.stamps.insert(key, stamp);
                    json!(stamp)
                }
                // No Todo belongs to a Channel here.
                "memberships" => json!([]),
                "load" => {
                    let version = request["version"].as_u64().unwrap();
                    let rows: Vec<Value> = request["identities"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|identity| {
                            let id = identity["id"].as_str().unwrap();
                            match state.rows.get(id) {
                                Some(title) if version == 1 => json!({"id":id,"title":title}),
                                Some(title) => json!({"id":id,"title":title,"done":false}),
                                None => Value::Null,
                            }
                        })
                        .collect();
                    json!(rows)
                }
                other => return Err(format!("unexpected host operation {other}")),
            };
            Ok(answer)
        })
    }
}

/// Local Todo v2 authority; retained Todo v1 result reads.
fn config() -> Config {
    let v1 = json!([{"name":"id","type":{"kind":"scalar","name":"uuid"},"nullable":false},
                    {"name":"title","type":{"kind":"scalar","name":"string"},"nullable":false}]);
    let mut v2 = v1.clone();
    v2.as_array_mut()
        .unwrap()
        .push(json!({"name":"done","type":{"kind":"scalar","name":"boolean"},"nullable":false}));
    let handler = json!({"kind":"identity","model":"Todo","fields":[{"name":"id","type":{"kind":"scalar","name":"uuid"}}]});
    Config::decode(json!({
        "schema":{"enums":[],
            "models":[{"name":"Todo","version":2,"identity":["id"],"fields":v2}],
            "resultModels":[{"name":"Todo","version":1,"identity":["id"],"fields":v1,"enums":[]}],
            "actions":[{"name":"Open","version":1,"inputs":[
                {"kind":"value","name":"store","type":{"kind":"scalar","name":"string"},"nullable":true,"list":false},
                {"kind":"model","name":"todo","model":"Todo","operation":"update","cardinality":"optional"}],
             "outputs":[
                {"name":"mainTodo","kind":"model","model":"Todo","modelReadVersion":1,"cardinality":"optional","source":"handlerIdentity","handlerType":handler},
                {"name":"suggestions","kind":"model","model":"Todo","modelReadVersion":1,"cardinality":"list","source":"handlerIdentity","handlerType":handler}]}]},
        "mutations":[],"loaders":["Todo"],
        "models":[{"name":"Todo","version":1,"identity":["id"],"fields":v1,"enums":[]},
                  {"name":"Todo","version":2,"identity":["id"],"fields":v2,"enums":[]}]
    }))
    .unwrap()
}

fn outputs(main: Option<&str>, suggestions: &[&str]) -> Value {
    json!({"mainTodo":main.map(|id| json!({"id":id})),
           "suggestions":suggestions.iter().map(|id| json!({"id":id})).collect::<Vec<_>>()})
}

/// `args` gets the nullable business input named `store` when absent.
fn intent(id: &str, ordinal: u64, mut args: Value, store: Option<Value>) -> Value {
    if args.get("store").is_none() {
        args["store"] = Value::Null;
    }
    let mut call = json!({"ordinal":ordinal,"callId":id,"name":"Open","version":1,"args":args});
    if let Some(store) = store {
        call["store"] = store;
    }
    call
}

fn push(host: &StoreHost, sequence: u64, calls: Vec<Value>) -> Value {
    let request =
        json!({"clientId":"device","batchSequence":sequence,"models":{"Todo":2},"mutations":calls});
    serde_json::from_str(
        &run(process_action_push(
            &config(),
            "alice",
            request.to_string().as_bytes(),
            host,
        ))
        .unwrap(),
    )
    .unwrap()
}

fn record_ids(receipt: &Value) -> Vec<String> {
    receipt["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["identity"]["id"].as_str().unwrap().to_string())
        .collect()
}

fn key(id: &str) -> String {
    format!("{{\"id\":\"{id}\"}}")
}

#[test]
fn store_false_returns_loader_snapshots_without_output_authority_or_stamps() {
    let host = StoreHost::new(outputs(Some(A), &[B, A, B]));
    let receipt = push(
        &host,
        1,
        vec![intent(CALL, 1, json!({"store":"x"}), Some(json!(false)))],
    );
    assert_eq!(
        receipt["completions"][0]["outcome"]["result"],
        json!({"mainTodo":{"id":A,"title":"A"},
               "suggestions":[{"id":B,"title":"B"},{"id":A,"title":"A"},{"id":B,"title":"B"}]})
    );
    assert_eq!(receipt["records"], json!([]));
    assert!(host.stamped().is_empty(), "no output-only stamp allocation");
    // Each identity is read once at the result read version; no authority read.
    assert_eq!(host.loads(), vec![(1, vec![A.into()]), (1, vec![B.into()])]);
}

#[test]
fn default_store_applies_each_output_identity_once_at_the_declared_version() {
    let host = StoreHost::new(outputs(Some(A), &[B, A]));
    let receipt = push(&host, 1, vec![intent(CALL, 1, json!({}), None)]);
    assert_eq!(record_ids(&receipt), vec![A, B]);
    assert_eq!(
        receipt["records"][0]["state"],
        json!({"title":"A","done":false})
    );
    assert_eq!(host.stamped(), vec![key(A), key(B)]);
    let log = host.0.lock().unwrap().log.clone();
    let first_stamp = log.iter().position(|r| r["op"] == "ensureStamp").unwrap();
    let first_load = log.iter().position(|r| r["op"] == "load").unwrap();
    assert!(first_stamp < first_load, "stamp evidence precedes content");
}

#[test]
fn per_output_map_is_a_positive_union_in_either_declaration_order() {
    // Disabled output first (mainTodo), enabled output later shares A.
    let host = StoreHost::new(outputs(Some(A), &[B, A]));
    let receipt = push(
        &host,
        1,
        vec![intent(CALL, 1, json!({}), Some(json!({"mainTodo":false})))],
    );
    assert_eq!(record_ids(&receipt), vec![A, B]);
    assert_eq!(host.stamped(), vec![key(B), key(A)]);
    // A's result read is reused; its authority is read after its stamp.
    assert_eq!(
        host.loads(),
        vec![
            (1, vec![A.into()]),
            (1, vec![B.into()]),
            (2, vec![B.into()]),
            (2, vec![A.into()])
        ]
    );
    // Enabled output first (mainTodo), disabled output later shares A; C is
    // selected only by the disabled output.
    let host = StoreHost::new(outputs(Some(A), &[C, A]));
    let receipt = push(
        &host,
        1,
        vec![intent(
            CALL,
            1,
            json!({}),
            Some(json!({"suggestions":false})),
        )],
    );
    assert_eq!(record_ids(&receipt), vec![A]);
    assert_eq!(host.stamped(), vec![key(A)]);
    assert_eq!(
        receipt["completions"][0]["outcome"]["result"]["suggestions"],
        json!([{"id":C,"title":"C"},{"id":A,"title":"A"}])
    );
}

#[test]
fn store_false_keeps_mandatory_input_authority_and_extra_touches_never_force_storage() {
    let host = StoreHost::new(outputs(Some(A), &[B, C]));
    host.0.lock().unwrap().extra = vec![json!({"model":"Todo","identity":{"id":C}})];
    let receipt = push(
        &host,
        1,
        vec![intent(
            CALL,
            1,
            json!({"todo":{"id":A,"title":"X"}}),
            Some(json!(false)),
        )],
    );
    // A (mutation input) stays; C (extra touch) and B (output-only) do not.
    assert_eq!(record_ids(&receipt), vec![A]);
    assert_eq!(
        receipt["records"][0]["state"],
        json!({"title":"X","done":false})
    );
    assert!(host.stamped().is_empty());
    assert_eq!(
        host.ops("advanceStamp").len(),
        2,
        "A and C each advance once"
    );
    assert_eq!(
        receipt["completions"][0]["outcome"]["result"]["mainTodo"],
        json!({"id":A,"title":"X"})
    );
    // Under the default policy a touched record is authority only when an
    // enabled output selects it, at the stamp settlement allocated; a pure
    // touch (B) is not.
    let host = StoreHost::new(outputs(Some(A), &[C]));
    host.0.lock().unwrap().extra = vec![
        json!({"model":"Todo","identity":{"id":B}}),
        json!({"model":"Todo","identity":{"id":C}}),
    ];
    let receipt = push(
        &host,
        1,
        vec![intent(CALL, 1, json!({"todo":{"id":A,"title":"X"}}), None)],
    );
    assert_eq!(record_ids(&receipt), vec![A, C]);
    let stamps: Vec<u64> = receipt["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|record| record["stamp"].as_u64().unwrap())
        .collect();
    assert_eq!(stamps, [11, 13], "A, B and C advanced in key order");
    assert!(host.stamped().is_empty(), "no second stamp for touched C");
    assert_eq!(host.ops("advanceStamp").len(), 3);
}

#[test]
fn saved_replay_returns_original_result_and_changed_policy_conflicts() {
    let host = StoreHost::new(outputs(Some(A), &[B]));
    let first = push(
        &host,
        1,
        vec![intent(
            CALL,
            1,
            json!({}),
            Some(json!({"suggestions":false,"mainTodo":true})),
        )],
    );
    host.0
        .lock()
        .unwrap()
        .rows
        .insert(A.into(), "changed".into());
    host.clear_log();
    // Same policy spelled with reordered keys replays without Handler/Loader.
    let raw = format!(
        r#"{{"clientId":"device","batchSequence":2,"models":{{"Todo":2}},"mutations":[{{"ordinal":1,"callId":"{CALL}","name":"Open","version":1,"args":{{"store":null}},"store":{{"suggestions":false,"mainTodo":true}}}}]}}"#
    );
    let replay: Value = serde_json::from_str(
        &run(process_action_push(
            &config(),
            "alice",
            raw.as_bytes(),
            &host,
        ))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(replay["completions"], first["completions"]);
    assert_eq!(replay["records"], first["records"]);
    assert!(host.ops("handleAction").is_empty());
    assert!(host.ops("load").is_empty());
    assert!(host.ops("ensureStamp").is_empty());
    // A changed policy on the same call ID is an identity conflict.
    for (sequence, store) in [
        (3, Some(json!(false))),
        (4, None),
        (5, Some(json!({"mainTodo":false,"suggestions":false}))),
    ] {
        let conflict = push(&host, sequence, vec![intent(CALL, 1, json!({}), store)]);
        assert_eq!(
            conflict["completions"][0]["outcome"],
            json!({"status":"failed","code":"call.identity_conflict","execution":"rejected"})
        );
    }
    assert!(host.ops("handleAction").is_empty());
    assert!(host.ops("load").is_empty());
}

#[test]
fn omitted_and_true_policy_share_one_call_identity() {
    let host = StoreHost::new(outputs(Some(A), &[]));
    let first = push(&host, 1, vec![intent(CALL, 1, json!({}), None)]);
    host.clear_log();
    let replay = push(
        &host,
        2,
        vec![intent(CALL, 1, json!({}), Some(json!(true)))],
    );
    assert_eq!(replay["completions"], first["completions"]);
    assert!(host.ops("handleAction").is_empty());
    let stored: Value =
        serde_json::from_str(&host.0.lock().unwrap().calls[CALL].0.clone()).unwrap();
    assert!(stored.get("store").is_none(), "{stored}");
}

#[test]
fn invalid_store_key_rejects_only_its_own_call_before_the_handler() {
    let host = StoreHost::new(outputs(Some(A), &[]));
    let receipt = push(
        &host,
        1,
        vec![
            intent(CALL, 1, json!({}), Some(json!({"store":false}))),
            intent(CALL2, 2, json!({}), Some(json!({"mainTodo":false}))),
        ],
    );
    assert_eq!(
        receipt["rejections"],
        json!([{"ordinal":1,"code":"action.invalid"}])
    );
    assert_eq!(
        receipt["completions"][1]["outcome"]["result"]["mainTodo"],
        json!({"id":A,"title":"A"})
    );
    assert_eq!(host.ops("handleAction").len(), 1);
    // A structurally invalid policy value refuses the envelope.
    let request = json!({"clientId":"device","batchSequence":2,"models":{"Todo":2},
        "mutations":[intent(CALL2, 1, json!({}), Some(json!("no")))]});
    assert!(
        run(process_action_push(
            &config(),
            "alice",
            request.to_string().as_bytes(),
            &host
        ))
        .is_err()
    );
}

#[test]
fn direct_route_honours_store_and_replays_saved_authority() {
    let host = StoreHost::new(outputs(Some(A), &[B]));
    let direct = |store: Option<Value>| {
        let mut call = intent(CALL, 1, json!({}), store);
        call.as_object_mut().unwrap().remove("ordinal");
        json!({"call":call,"models":{"Todo":2}}).to_string()
    };
    let response: Value = serde_json::from_str(
        &run(process_action(
            &config(),
            "alice",
            direct(Some(json!({"mainTodo":false}))).as_bytes(),
            &host,
        ))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        response["completion"]["outcome"]["result"],
        json!({"mainTodo":{"id":A,"title":"A"},"suggestions":[{"id":B,"title":"B"}]})
    );
    assert_eq!(record_ids(&response), vec![B]);
    host.clear_log();
    let again: Value = serde_json::from_str(
        &run(process_action(
            &config(),
            "alice",
            direct(Some(json!({"mainTodo":false}))).as_bytes(),
            &host,
        ))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(again, response);
    assert!(host.ops("load").is_empty());
    let conflict: Value = serde_json::from_str(
        &run(process_action(
            &config(),
            "alice",
            direct(None).as_bytes(),
            &host,
        ))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        conflict["completion"]["outcome"]["code"],
        "call.identity_conflict"
    );
}

#[test]
fn store_false_keeps_absence_semantics_without_tombstones() {
    const GONE: &str = "01890f47-1234-7123-8123-0000000000ff";
    for store in [Some(json!(false)), None] {
        // A nullable output whose identity is authoritatively absent is null.
        let host = StoreHost::new(outputs(Some(GONE), &[]));
        let receipt = push(&host, 1, vec![intent(CALL, 1, json!({}), store.clone())]);
        assert_eq!(
            receipt["completions"][0]["outcome"]["result"]["mainTodo"],
            Value::Null
        );
        // Only enabled storage with stamp evidence carries the tombstone.
        let tombstones = receipt["records"].as_array().unwrap().len();
        assert_eq!(tombstones, usize::from(store.is_none()));
        // A missing list element is a resolution error, never compacted.
        let host = StoreHost::new(outputs(None, &[A, GONE]));
        let receipt = push(&host, 1, vec![intent(CALL, 1, json!({}), store.clone())]);
        assert_eq!(
            receipt["rejections"],
            json!([{"ordinal":1,"code":"loader.invalid"}])
        );
        // An empty selection deletes nothing.
        let host = StoreHost::new(outputs(None, &[]));
        let receipt = push(&host, 1, vec![intent(CALL, 1, json!({}), store)]);
        assert_eq!(receipt["records"], json!([]));
    }
}

#[test]
fn explicit_true_entries_share_the_default_call_identity() {
    let host = StoreHost::new(outputs(Some(A), &[B]));
    let first = push(
        &host,
        1,
        vec![intent(CALL, 1, json!({}), Some(json!({"mainTodo":true})))],
    );
    let stored: Value =
        serde_json::from_str(&host.0.lock().unwrap().calls[CALL].0.clone()).unwrap();
    assert!(stored.get("store").is_none(), "{stored}");
    host.clear_log();
    let replay = push(&host, 2, vec![intent(CALL, 1, json!({}), None)]);
    assert_eq!(replay["completions"], first["completions"]);
    assert!(host.ops("handleAction").is_empty());
    // {a:false, b:true} and {a:false} are one identity.
    push(
        &host,
        3,
        vec![intent(
            CALL2,
            1,
            json!({}),
            Some(json!({"suggestions":false,"mainTodo":true})),
        )],
    );
    host.clear_log();
    let again = push(
        &host,
        4,
        vec![intent(
            CALL2,
            1,
            json!({}),
            Some(json!({"suggestions":false})),
        )],
    );
    assert_eq!(again["rejections"], json!([]));
    assert!(host.ops("handleAction").is_empty());
}

#[test]
fn unknown_key_is_rejected_even_when_true() {
    let host = StoreHost::new(outputs(Some(A), &[]));
    let receipt = push(
        &host,
        1,
        vec![intent(CALL, 1, json!({}), Some(json!({"missing":true})))],
    );
    assert_eq!(
        receipt["rejections"],
        json!([{"ordinal":1,"code":"action.invalid"}])
    );
    assert!(host.ops("handleAction").is_empty());
}
