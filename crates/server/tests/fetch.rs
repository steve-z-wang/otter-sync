//! Model Fetch on the server: one authorized Loader read of one identity at
//! a retained read version, claimed and saved in the call ledger, with no
//! Handler, touch, membership or publication.
mod capability;
use axton_core::{FetchRequest, FetchResponse};
use axton_server::{Config, Host, HostResult, process_action, process_fetch};
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

const CALL: &str = "01890f47-1234-7123-8123-123456789ab1";
const CALL2: &str = "01890f47-1234-7123-8123-123456789ab2";

/// Operations a Fetch must never issue: it runs no Handler and changes no
/// stamp, membership, Scope or publication.
const FORBIDDEN: [&str; 10] = [
    "handle",
    "handleAction",
    "advanceStamp",
    "lockRecord",
    "readTracking",
    "guardRecords",
    "lockStreams",
    "applyStreamMembers",
    "head",
    "scan",
];

/// A recording backend with Todo rows, per-record stamps and a call ledger
/// keyed by `(owner, callId)`.
#[derive(Default)]
struct State {
    ops: Vec<Value>,
    rows: BTreeMap<String, Value>,
    stamps: BTreeMap<String, u64>,
    calls: BTreeMap<(String, String), (String, Option<String>)>,
    /// A fixed Loader answer instead of the stored rows.
    answer: Option<Value>,
    /// An operation whose host call throws (a storage or transaction fault).
    fault: Option<&'static str>,
}
struct FetchHost(Mutex<State>);
impl FetchHost {
    fn new() -> Self {
        let host = Self(Mutex::new(State::default()));
        host.row("t1", json!({"id":"t1","title":"A"}));
        host
    }
    fn row(&self, id: &str, row: Value) {
        self.0.lock().unwrap().rows.insert(id.into(), row);
    }
    fn ops(&self) -> Vec<Value> {
        self.0.lock().unwrap().ops.clone()
    }
    fn names(&self) -> Vec<String> {
        self.ops()
            .iter()
            .map(|op| op["op"].as_str().unwrap().to_string())
            .collect()
    }
    fn count(&self, name: &str) -> usize {
        self.names().iter().filter(|op| *op == name).count()
    }
    fn clear(&self) {
        self.0.lock().unwrap().ops.clear();
    }
}
impl Host for FetchHost {
    fn call(&self, request: Value) -> Pin<Box<dyn Future<Output = HostResult<Value>> + Send + '_>> {
        Box::pin(async move {
            let mut state = self.0.lock().unwrap();
            state.ops.push(request.clone());
            let op = request["op"].as_str().unwrap();
            if state.fault == Some(op) {
                return Err(format!("forced {op} fault"));
            }
            let text = |name: &str| request[name].as_str().unwrap().to_string();
            Ok(match op {
                "claimCall" => {
                    let key = (text("owner"), text("callId"));
                    match state.calls.get(&key) {
                        Some((saved, response)) => {
                            json!({"fresh":false,"request":saved,"response":response})
                        }
                        None => {
                            state.calls.insert(key, (text("request"), None));
                            json!({"fresh":true,"request":request["request"],"response":null})
                        }
                    }
                }
                "saveCall" => {
                    let key = (text("owner"), text("callId"));
                    state.calls.get_mut(&key).unwrap().1 = Some(text("response"));
                    Value::Null
                }
                "ensureStamp" => {
                    let stamp = *state.stamps.entry(text("identityKey")).or_insert(7);
                    json!(stamp)
                }
                "load" => match &state.answer {
                    Some(answer) => answer.clone(),
                    None => Value::Array(
                        request["identities"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|identity| {
                                state
                                    .rows
                                    .get(identity["id"].as_str().unwrap())
                                    .cloned()
                                    .unwrap_or(Value::Null)
                            })
                            .collect(),
                    ),
                },
                "handleAction" => {
                    json!({"outputs":{"message":"ok"},"changes":[],"declarations":[]})
                }
                _ => Value::Null,
            })
        })
    }
}

fn field(name: &str, kind: &str, nullable: bool) -> Value {
    json!({"name":name,"type":{"kind":"scalar","name":kind},"nullable":nullable})
}
fn v1() -> Vec<Value> {
    vec![
        field("id", "string", false),
        field("title", "string", false),
    ]
}
fn v2() -> Vec<Value> {
    vec![
        field("id", "string", false),
        field("title", "string", false),
        field("done", "boolean", true),
    ]
}
fn member() -> Vec<Value> {
    vec![
        field("team", "string", false),
        field("seat", "int", false),
        field("name", "string", false),
    ]
}
/// Todo is current at v2 with v1 still retained; Member has a composite
/// identity; Draft is a Model without a registered Loader. No Actions.
fn config() -> Config {
    Config::decode(json!({
        "schema":{"enums":[],"actions":[],
            "models":[
                {"name":"Todo","version":2,"identity":["id"],"fields":v2()},
                {"name":"Member","version":1,"identity":["team","seat"],"fields":member()},
                {"name":"Draft","version":1,"identity":["id"],"fields":v1()}
            ],
            "resultModels":[
                {"name":"Todo","version":1,"identity":["id"],"fields":v1(),"enums":[]},
                {"name":"Todo","version":2,"identity":["id"],"fields":v2(),"enums":[]},
                {"name":"Member","version":1,"identity":["team","seat"],"fields":member(),"enums":[]}
            ]},
        "mutations":[],"loaders":["Todo","Member"],
        "models":[
            {"name":"Todo","version":1,"identity":["id"],"fields":v1(),"enums":[]},
            {"name":"Todo","version":2,"identity":["id"],"fields":v2(),"enums":[]},
            {"name":"Member","version":1,"identity":["team","seat"],"fields":member(),"enums":[]},
            {"name":"Draft","version":1,"identity":["id"],"fields":v1(),"enums":[]}
        ]
    }))
    .unwrap()
}

fn request(
    call_id: &str,
    model: &str,
    version: u64,
    identity: Value,
    store: Option<bool>,
) -> Vec<u8> {
    let mut wire = json!({"callId":call_id,"model":model,"version":version,"identity":identity});
    if let Some(store) = store {
        wire["store"] = json!(store);
    }
    wire.to_string().into_bytes()
}
fn todo(call_id: &str, id: &str, store: Option<bool>) -> Vec<u8> {
    request(call_id, "Todo", 1, json!({"id":id}), store)
}

/// Fetch, then check the produced bytes with the client's own decoder.
fn fetch(config: &Config, owner: &str, bytes: &[u8], host: &FetchHost) -> Value {
    let text = run(process_fetch(
        config,
        owner,
        &crate::capability::request(bytes),
        host,
    ))
    .unwrap();
    let request = FetchRequest::decode(bytes, &config.schema).unwrap();
    FetchResponse::decode(text.as_bytes(), &request, &config.schema)
        .unwrap_or_else(|error| panic!("{error}: {text}"));
    serde_json::from_str(&text).unwrap()
}
fn outcome(response: &Value) -> Value {
    response["completion"]["outcome"].clone()
}
fn failed(code: &str) -> Value {
    json!({"status":"failed","code":code,"execution":"rejected"})
}
fn assert_no_effects(host: &FetchHost) {
    for op in host.names() {
        assert!(!FORBIDDEN.contains(&op.as_str()), "Fetch issued {op}");
    }
}

#[test]
fn stored_fetch_takes_stamp_evidence_then_reads_once_and_saves_one_snapshot() {
    let config = config();
    let host = FetchHost::new();
    let response = fetch(&config, "alice", &todo(CALL, "t1", None), &host);
    assert_eq!(
        response,
        json!({
            "completion":{"callId":CALL,"outcome":{"status":"succeeded","result":{"id":"t1","title":"A"}}},
            "records":[{"model":"Todo","identity":{"id":"t1"},"stamp":7,"state":{"title":"A"}}]
        })
    );
    assert_eq!(
        host.names(),
        [
            "claimCall",
            "savepoint",
            "ensureStamp",
            "load",
            "release",
            "saveCall"
        ]
    );
    let ops = host.ops();
    assert_eq!(
        ops[3],
        json!({"op":"load","model":"Todo","version":1,"identities":[{"id":"t1"}],"owner":"alice"})
    );
    assert_eq!(ops[2]["identityKey"], json!("{\"id\":\"t1\"}"));
    let fingerprint: Value = serde_json::from_str(ops[0]["request"].as_str().unwrap()).unwrap();
    assert_eq!(
        fingerprint,
        json!({"kind":"fetch","callId":CALL,"model":"Todo","version":1,"identity":{"id":"t1"},"store":true})
    );
    assert_eq!(
        serde_json::from_str::<Value>(ops[5]["response"].as_str().unwrap()).unwrap(),
        response,
        "the returned bytes are the saved outcome"
    );
    assert_no_effects(&host);
}

#[test]
fn store_false_reads_without_stamp_allocation_or_authority() {
    let config = config();
    let host = FetchHost::new();
    let response = fetch(&config, "alice", &todo(CALL, "t1", Some(false)), &host);
    assert_eq!(
        outcome(&response),
        json!({"status":"succeeded","result":{"id":"t1","title":"A"}})
    );
    assert_eq!(response["records"], json!([]));
    assert_eq!(
        host.names(),
        ["claimCall", "savepoint", "load", "release", "saveCall"]
    );
    let fingerprint: Value =
        serde_json::from_str(host.ops()[0]["request"].as_str().unwrap()).unwrap();
    assert_eq!(fingerprint["store"], json!(false));
}

#[test]
fn absence_is_null_with_stamped_null_authority_only_when_storing() {
    let config = config();
    let host = FetchHost::new();
    let stored = fetch(&config, "alice", &todo(CALL, "gone", None), &host);
    assert_eq!(
        outcome(&stored),
        json!({"status":"succeeded","result":null})
    );
    assert_eq!(
        stored["records"],
        json!([{"model":"Todo","identity":{"id":"gone"},"stamp":7,"state":null}])
    );
    let preview = fetch(&config, "alice", &todo(CALL2, "gone", Some(false)), &host);
    assert_eq!(
        outcome(&preview),
        json!({"status":"succeeded","result":null})
    );
    assert_eq!(preview["records"], json!([]));
    assert_no_effects(&host);
}

#[test]
fn the_requested_read_version_selects_the_loader_and_normalizes_its_row() {
    let config = config();
    let host = FetchHost::new();
    // The v2 Loader omits the nullable `done`; the retained contract fills it.
    let response = fetch(
        &config,
        "alice",
        &request(CALL, "Todo", 2, json!({"id":"t1"}), None),
        &host,
    );
    assert_eq!(
        outcome(&response),
        json!({"status":"succeeded","result":{"id":"t1","title":"A","done":null}})
    );
    assert_eq!(
        response["records"][0]["state"],
        json!({"title":"A","done":null})
    );
    let loads: Vec<Value> = host
        .ops()
        .into_iter()
        .filter(|op| op["op"] == "load")
        .collect();
    assert_eq!(loads.len(), 1);
    assert_eq!(loads[0]["version"], json!(2));
}

#[test]
fn a_composite_identity_is_normalized_before_it_joins_the_call_identity() {
    let config = config();
    let host = FetchHost::new();
    host.0.lock().unwrap().answer = Some(json!([{"team":"red","seat":3,"name":"Ann"}]));
    let first = fetch(
        &config,
        "alice",
        &request(CALL, "Member", 1, json!({"team":"red","seat":3}), None),
        &host,
    );
    assert_eq!(
        outcome(&first),
        json!({"status":"succeeded","result":{"team":"red","seat":3,"name":"Ann"}})
    );
    assert_eq!(
        host.ops()[3]["identities"],
        json!([{"seat":3,"team":"red"}])
    );
    // Another member order and an integral float spell the same identity.
    let again = run(process_fetch(
        &config,
        "alice",
        &crate::capability::request(br#"{"identity":{"seat":3.0,"team":"red"},"version":1,"model":"Member","callId":"01890f47-1234-7123-8123-123456789ab1"}"#),
        &host,
    ))
    .unwrap();
    assert_eq!(serde_json::from_str::<Value>(&again).unwrap(), first);
    assert_eq!(host.count("load"), 1);
}

#[test]
fn a_repeated_call_id_replays_the_saved_snapshot_after_the_row_changes() {
    let config = config();
    let host = FetchHost::new();
    let bytes = todo(CALL, "t1", None);
    let first = run(process_fetch(
        &config,
        "alice",
        &crate::capability::request(&bytes),
        &host,
    ))
    .unwrap();
    host.row("t1", json!({"id":"t1","title":"changed"}));
    host.clear();
    let replay = run(process_fetch(
        &config,
        "alice",
        &crate::capability::request(&bytes),
        &host,
    ))
    .unwrap();
    assert_eq!(replay, first, "the saved bytes answer the retry");
    assert_eq!(
        host.names(),
        ["claimCall"],
        "no Loader, stamp or save on replay"
    );
    // An uppercase spelling of the same call ID is the same call.
    let upper = todo(&CALL.to_uppercase(), "t1", None);
    assert_eq!(
        run(process_fetch(
            &config,
            "alice",
            &crate::capability::request(&upper),
            &host
        ))
        .unwrap(),
        first
    );
    assert_eq!(host.count("load"), 0);
}

#[test]
fn a_call_id_is_owned_per_principal() {
    let config = config();
    let host = FetchHost::new();
    let bytes = todo(CALL, "t1", None);
    fetch(&config, "alice", &bytes, &host);
    host.row("t1", json!({"id":"t1","title":"B"}));
    let bob = fetch(&config, "bob", &bytes, &host);
    assert_eq!(
        outcome(&bob),
        json!({"status":"succeeded","result":{"id":"t1","title":"B"}}),
        "another owner's saved outcome is never shared"
    );
    let loads: Vec<Value> = host
        .ops()
        .into_iter()
        .filter(|op| op["op"] == "load")
        .collect();
    assert_eq!(loads.len(), 2);
    assert_eq!(loads[1]["owner"], json!("bob"));
}

#[test]
fn reusing_a_call_id_for_another_fetch_intent_conflicts_without_reading() {
    let config = config();
    let host = FetchHost::new();
    host.row("t2", json!({"id":"t2","title":"B"}));
    fetch(&config, "alice", &todo(CALL, "t1", None), &host);
    for changed in [
        todo(CALL, "t2", None),
        todo(CALL, "t1", Some(false)),
        request(CALL, "Todo", 2, json!({"id":"t1"}), None),
        request(CALL, "Member", 1, json!({"team":"red","seat":1}), None),
    ] {
        host.clear();
        let response = fetch(&config, "alice", &changed, &host);
        assert_eq!(outcome(&response), failed("call.identity_conflict"));
        assert_eq!(response["records"], json!([]));
        assert_eq!(
            host.names(),
            ["claimCall"],
            "a conflict is neither read nor saved"
        );
    }
    // Omitted and explicit `true` are one storage policy.
    host.clear();
    let same = fetch(&config, "alice", &todo(CALL, "t1", Some(true)), &host);
    assert_eq!(outcome(&same)["status"], json!("succeeded"));
    assert_eq!(host.names(), ["claimCall"]);
}

#[test]
fn actions_and_fetches_never_replay_each_others_saved_responses() {
    let mut config = config();
    config.schema.actions = serde_json::from_value(json!([{"name":"Send","version":1,"inputs":[],"outputs":[{"name":"message","kind":"value","type":{"kind":"scalar","name":"string"},"cardinality":"single","source":"handlerValue"}]}])).unwrap();
    let host = FetchHost::new();
    // An Action saved first: a Fetch reusing its call ID conflicts.
    let action = json!({"call":{"callId":CALL,"name":"Send","version":1,"args":{}},"models":{}});
    let sent: Value = serde_json::from_str(
        &run(process_action(
            &config,
            "alice",
            &crate::capability::request(action.to_string().as_bytes()),
            &host,
        ))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        sent["completion"]["outcome"]["result"],
        json!({"message":"ok"})
    );
    host.clear();
    let response = fetch(&config, "alice", &todo(CALL, "t1", None), &host);
    assert_eq!(outcome(&response), failed("call.identity_conflict"));
    assert_eq!(host.names(), ["claimCall"]);
    // A Fetch saved first: the Action reusing its call ID conflicts, and its
    // Handler never runs.
    fetch(&config, "alice", &todo(CALL2, "t1", None), &host);
    host.clear();
    let action = json!({"call":{"callId":CALL2,"name":"Send","version":1,"args":{}},"models":{}});
    let refused: Value = serde_json::from_str(
        &run(process_action(
            &config,
            "alice",
            &crate::capability::request(action.to_string().as_bytes()),
            &host,
        ))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        refused["completion"]["outcome"],
        failed("call.identity_conflict")
    );
    assert_eq!(host.names(), ["claimCall"]);
    // Another kind's saved bytes are never decoded, even when unreadable.
    let foreign = "01890f47-1234-7123-8123-123456789ab3";
    host.0.lock().unwrap().calls.insert(
        ("alice".into(), foreign.into()),
        (r#"{"kind":"load"}"#.into(), Some("not json".into())),
    );
    let response = fetch(&config, "alice", &todo(foreign, "t1", None), &host);
    assert_eq!(outcome(&response), failed("call.identity_conflict"));
}

#[test]
fn loader_refusals_failures_and_invalid_rows_are_saved_terminal_rejections() {
    let config = config();
    for (index, (answer, code)) in [
        (json!({"rejection":"todo.hidden"}), "todo.hidden"),
        (json!({"error":"boom"}), "loader.failed"),
        (json!([]), "loader.invalid"),
        (json!([null, null]), "loader.invalid"),
        (json!([{"id":"t1","title":5}]), "loader.invalid"),
        (json!([{"id":"t1"}]), "loader.invalid"),
        (json!([{"id":"t1","title":"A","extra":1}]), "loader.invalid"),
    ]
    .into_iter()
    .enumerate()
    {
        for store in [None, Some(false)] {
            let host = FetchHost::new();
            host.0.lock().unwrap().answer = Some(answer.clone());
            let call = format!("01890f47-1234-7123-8123-1234567891{index}0");
            let bytes = todo(&call, "t1", store);
            let response = fetch(&config, "alice", &bytes, &host);
            assert_eq!(outcome(&response), failed(code), "{answer}");
            assert_eq!(response["records"], json!([]), "{answer}");
            let mut expected = vec!["claimCall", "savepoint"];
            if store.is_none() {
                expected.push("ensureStamp");
            }
            expected.extend(["load", "rollback", "release", "saveCall"]);
            assert_eq!(host.names(), expected, "{answer}: rolled back, then saved");
            // The rejection is the call's outcome: a retry replays it.
            host.clear();
            host.0.lock().unwrap().answer = None;
            let replay = fetch(&config, "alice", &bytes, &host);
            assert_eq!(replay, response);
            assert_eq!(host.names(), ["claimCall"]);
        }
    }
}

#[test]
fn an_unserved_read_contract_or_missing_loader_is_a_saved_terminal_rejection() {
    let config = config();
    for (index, (model, version, code)) in [
        ("Todo", 3, "model_version_unsupported"),
        ("Ghost", 1, "model_version_unsupported"),
        ("Draft", 1, "loader.unregistered"),
    ]
    .into_iter()
    .enumerate()
    {
        let host = FetchHost::new();
        let call = format!("01890f47-1234-7123-8123-1234567892{index}0");
        let bytes = request(&call, model, version, json!({"id":"t1"}), None);
        let text = run(process_fetch(
            &config,
            "alice",
            &crate::capability::request(&bytes),
            &host,
        ))
        .unwrap();
        let response: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(
            response,
            json!({"completion":{"callId":call,"outcome":failed(code)},"records":[]}),
            "{model} v{version}"
        );
        assert_eq!(
            host.names(),
            ["claimCall", "savepoint", "rollback", "release", "saveCall"],
            "{model} v{version}: nothing is read"
        );
        // The client decodes the rejection whenever it knows the contract.
        if let Ok(request) = FetchRequest::decode(&bytes, &config.schema) {
            FetchResponse::decode(text.as_bytes(), &request, &config.schema).unwrap();
        }
    }
}

#[test]
fn malformed_requests_and_identities_fail_before_the_call_is_claimed() {
    let config = config();
    for bytes in [
        br#"{"callId":"nope","model":"Todo","version":1,"identity":{"id":"t1"}}"#.to_vec(),
        br#"{"callId":"01890f47-1234-7123-8123-123456789ab1","model":"Todo","version":1,"identity":{"id":"t1"},"store":"yes"}"#.to_vec(),
        br#"{"callId":"01890f47-1234-7123-8123-123456789ab1","model":"Todo","version":1,"identity":{"id":"t1"},"extra":1}"#.to_vec(),
        br#"{"callId":"01890f47-1234-7123-8123-123456789ab1","model":"Todo","version":0,"identity":{"id":"t1"}}"#.to_vec(),
        b"[]".to_vec(),
        todo(CALL, "t1", None)[..20].to_vec(),
        request(CALL, "Todo", 1, json!({}), None),
        request(CALL, "Todo", 1, json!({"id":"t1","title":"A"}), None),
        request(CALL, "Todo", 1, json!({"id":5}), None),
        request(CALL, "Member", 1, json!({"team":"red","seat":1.5}), None),
    ] {
        let host = FetchHost::new();
        let error = run(process_fetch(&config, "alice",
        &crate::capability::request(&bytes), &host)).unwrap_err();
        assert_eq!(error.code, "request.invalid", "{}", String::from_utf8_lossy(&bytes));
        assert!(host.ops().is_empty());
    }
    let host = FetchHost::new();
    let error = run(process_fetch(
        &config,
        " ",
        &crate::capability::request(&todo(CALL, "t1", None)),
        &host,
    ))
    .unwrap_err();
    assert_eq!(error.code, "principal.invalid");
    assert!(host.ops().is_empty());
}

#[test]
fn storage_faults_propagate_without_saving_an_outcome() {
    let config = config();
    for (fault, store) in [
        ("claimCall", None),
        ("savepoint", None),
        ("ensureStamp", None),
        ("load", None),
        ("load", Some(false)),
        ("rollback", None),
        ("release", None),
        ("saveCall", None),
    ] {
        let host = FetchHost::new();
        let mut state = host.0.lock().unwrap();
        state.fault = Some(fault);
        if fault == "rollback" {
            state.answer = Some(json!({"rejection":"todo.hidden"}));
        }
        drop(state);
        let error = run(process_fetch(
            &config,
            "alice",
            &crate::capability::request(&todo(CALL, "t1", store)),
            &host,
        ))
        .unwrap_err();
        assert_eq!(error.code, "host", "{fault}");
        assert!(error.message.contains(fault), "{fault}: {error}");
        if fault != "saveCall" {
            assert_eq!(host.count("saveCall"), 0, "{fault}: no outcome is saved");
        }
    }
    // Inconsistent ledger answers are storage faults, not outcomes.
    for (claimed, expected) in [
        (json!({"fresh":true,"response":"{}"}), "storage.invalid"),
        (json!({"fresh":false,"response":null}), "storage.invalid"),
        (
            json!({"fresh":false,"response":"not json"}),
            "storage.invalid",
        ),
    ] {
        struct Ledger(Value, Mutex<Vec<Value>>);
        impl Host for Ledger {
            fn call(
                &self,
                request: Value,
            ) -> Pin<Box<dyn Future<Output = HostResult<Value>> + Send + '_>> {
                Box::pin(async move {
                    self.1.lock().unwrap().push(request.clone());
                    let mut claimed = self.0.clone();
                    claimed["request"] = request["request"].clone();
                    Ok(claimed)
                })
            }
        }
        let host = Ledger(claimed.clone(), Mutex::new(vec![]));
        let error = run(process_fetch(
            &config,
            "alice",
            &crate::capability::request(&todo(CALL, "t1", None)),
            &host,
        ))
        .unwrap_err();
        assert_eq!(error.code, expected, "{claimed}");
        assert_eq!(host.1.lock().unwrap().len(), 1);
    }
}

#[test]
fn a_replayed_response_must_answer_its_own_call() {
    let config = config();
    let host = FetchHost::new();
    let bytes = todo(CALL, "t1", None);
    run(process_fetch(
        &config,
        "alice",
        &crate::capability::request(&bytes),
        &host,
    ))
    .unwrap();
    let key = ("alice".to_string(), CALL.to_string());
    let mut state = host.0.lock().unwrap();
    let saved = state.calls[&key].1.clone().unwrap();
    state.calls.get_mut(&key).unwrap().1 = Some(saved.replace(CALL, CALL2));
    drop(state);
    let error = run(process_fetch(
        &config,
        "alice",
        &crate::capability::request(&bytes),
        &host,
    ))
    .unwrap_err();
    assert_eq!(error.code, "storage.invalid");
}
