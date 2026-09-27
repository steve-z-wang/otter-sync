use axton_server::{Config, Host, HostResult, process_action_push};
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

struct HostState(Mutex<Vec<Value>>);
impl Host for HostState {
    fn call(&self, request: Value) -> Pin<Box<dyn Future<Output = HostResult<Value>> + Send + '_>> {
        Box::pin(async move {
            self.0.lock().unwrap().push(request.clone());
            Ok(match request["op"].as_str().unwrap() {
                "claim" => json!({"clientId":"device","owner":"alice","sequence":0,"receipt":null}),
                "claimCall" => json!({"fresh":true,"request":request["request"],"response":null}),
                "handleAction" => {
                    let outputs = match request["name"].as_str().unwrap() {
                        "Void" | "Broken" => json!({}),
                        "UnexpectedVoid" => json!({"unexpected":1}),
                        "Values" => json!({"maybe":null,"items":["a","b"]}),
                        _ => json!({"message":"ok"}),
                    };
                    json!({"outputs":outputs,"changes":[],"memberships":[]})
                }
                _ => Value::Null,
            })
        })
    }
}

#[test]
fn ordinary_action_returns_its_handler_result_and_saves_it() {
    let config = Config::decode(json!({"schema":{"enums":[],"models":[],"actions":[{"name":"Send","version":1,"inputs":[],"outputs":[{"name":"message","kind":"value","type":{"kind":"scalar","name":"string"},"cardinality":"single","source":"handlerValue"}]}]},"mutations":[],"loaders":[]})).unwrap();
    let host = HostState(Mutex::new(vec![]));
    let request = json!({"clientId":"device","batchSequence":1,"models":{},"mutations":[{"ordinal":1,"callId":"01890f47-1234-7123-8123-123456789abc","name":"Send","version":1,"args":{}}]});
    let receipt = run(process_action_push(
        &config,
        "alice",
        request.to_string().as_bytes(),
        &host,
    ))
    .unwrap();
    let receipt: Value = serde_json::from_str(&receipt).unwrap();
    assert_eq!(
        receipt["completions"][0]["outcome"]["result"],
        json!({"message":"ok"})
    );
    assert!(
        host.0
            .lock()
            .unwrap()
            .iter()
            .any(|request| request["op"] == "saveCall")
    );
}

#[test]
fn ordinary_list_nullable_void_and_missing_explicit_outputs_are_independent() {
    let scalar = json!({"kind":"scalar","name":"string"});
    let config = Config::decode(json!({"schema":{"enums":[],"models":[],"actions":[
        {"name":"Values","version":1,"inputs":[],"outputs":[{"name":"maybe","kind":"value","type":scalar,"cardinality":"optional","source":"handlerValue"},{"name":"items","kind":"value","type":scalar,"cardinality":"list","source":"handlerValue"}]},
        {"name":"Broken","version":1,"inputs":[],"outputs":[{"name":"message","kind":"value","type":scalar,"cardinality":"single","source":"handlerValue"}]},
        {"name":"Void","version":1,"inputs":[],"outputs":[]}
    ]},"mutations":[],"loaders":[]})).unwrap();
    let host = HostState(Mutex::new(vec![]));
    let calls = ["Values","Broken","Void"].into_iter().enumerate().map(|(index,name)| json!({"ordinal":index+1,"callId":format!("01890f47-1234-7123-8123-123456789ab{}", index+1),"name":name,"version":1,"args":{}})).collect::<Vec<_>>();
    let request = json!({"clientId":"device","batchSequence":1,"models":{},"mutations":calls});
    let receipt: Value = serde_json::from_str(
        &run(process_action_push(
            &config,
            "alice",
            request.to_string().as_bytes(),
            &host,
        ))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        receipt["completions"][0]["outcome"]["result"],
        json!({"maybe":null,"items":["a","b"]})
    );
    assert_eq!(
        receipt["rejections"],
        json!([{"ordinal":2,"code":"handler.invalid"}])
    );
    assert_eq!(receipt["completions"][2]["outcome"]["result"], Value::Null);
    let ops = host.0.lock().unwrap();
    assert_eq!(ops.iter().filter(|op| op["op"] == "rollback").count(), 1);
    assert_eq!(ops.iter().filter(|op| op["op"] == "saveCall").count(), 3);
}

#[test]
fn void_action_rejects_undeclared_handler_output_without_rejecting_next_call() {
    let config = Config::decode(json!({"schema":{"enums":[],"models":[],"actions":[
        {"name":"UnexpectedVoid","version":1,"inputs":[],"outputs":[]},
        {"name":"Void","version":1,"inputs":[],"outputs":[]}
    ]},"mutations":[],"loaders":[]}))
    .unwrap();
    let host = HostState(Mutex::new(vec![]));
    let calls = ["UnexpectedVoid", "Void"]
        .into_iter()
        .enumerate()
        .map(|(index, name)| {
            json!({
                "ordinal":index+1,
                "callId":format!("01890f47-1234-7123-8123-123456789ab{}",index+1),
                "name":name,"version":1,"args":{}
            })
        })
        .collect::<Vec<_>>();
    let request = json!({"clientId":"device","batchSequence":1,"models":{},"mutations":calls});
    let receipt: Value = serde_json::from_str(
        &run(process_action_push(
            &config,
            "alice",
            request.to_string().as_bytes(),
            &host,
        ))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        receipt["rejections"],
        json!([{"ordinal":1,"code":"handler.invalid"}])
    );
    assert_eq!(receipt["completions"][1]["outcome"]["result"], Value::Null);
    let ops = host.0.lock().unwrap();
    assert_eq!(ops.iter().filter(|op| op["op"] == "rollback").count(), 1);
    assert_eq!(ops.iter().filter(|op| op["op"] == "saveCall").count(), 2);
}

struct ModelHost(Mutex<Vec<Value>>);
impl Host for ModelHost {
    fn call(&self, request: Value) -> Pin<Box<dyn Future<Output = HostResult<Value>> + Send + '_>> {
        Box::pin(async move {
            self.0.lock().unwrap().push(request.clone());
            Ok(match request["op"].as_str().unwrap() {
                "claim" => {
                    json!({"clientId":"device-1","owner":"alice","sequence":3,"receipt":null})
                }
                "claimCall" => json!({"fresh":true,"request":request["request"],"response":null}),
                "handleAction" => {
                    json!({"outputs":{"todo":{"id":"01890f47-1234-7123-8123-123456789abc"}},"changes":[],"memberships":[]})
                }
                "ensureStamp" => json!(1),
                "load" if request["version"] == 1 => {
                    json!([{"id":"01890f47-1234-7123-8123-123456789abc","title":"A"}])
                }
                "load" => {
                    json!([{"id":"01890f47-1234-7123-8123-123456789abc","title":"A","done":false}])
                }
                _ => Value::Null,
            })
        })
    }
}

#[test]
fn explicit_model_output_uses_old_result_loader_and_current_authority_loader() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/protocol/action-results.json"
    ))
    .unwrap();
    let config = Config::decode(json!({"schema":fixture["schema"],"mutations":[],"loaders":["Todo"],"models":[
        {"name":"Todo","version":1,"identity":["id"],"fields":fixture["schema"]["resultModels"][0]["fields"],"enums":[]},
        {"name":"Todo","version":2,"identity":["id"],"fields":fixture["schema"]["models"][0]["fields"],"enums":[]}
    ]})).unwrap();
    let host = ModelHost(Mutex::new(vec![]));
    let mut request = fixture["request"].clone();
    request["mutations"].as_array_mut().unwrap().truncate(1);
    let receipt = run(process_action_push(
        &config,
        "alice",
        request.to_string().as_bytes(),
        &host,
    ))
    .unwrap();
    let receipt: Value = serde_json::from_str(&receipt).unwrap();
    assert_eq!(
        receipt["completions"][0]["outcome"]["result"]["todo"],
        json!({"id":"01890f47-1234-7123-8123-123456789abc","title":"A"})
    );
    assert_eq!(
        receipt["records"][0]["state"],
        json!({"title":"A","done":false})
    );
    let ops = host.0.lock().unwrap();
    let versions: Vec<u64> = ops
        .iter()
        .filter(|op| op["op"] == "load")
        .map(|op| op["version"].as_u64().unwrap())
        .collect();
    assert_eq!(versions, vec![1, 2]);
    assert!(
        ops.iter().position(|op| op["op"] == "ensureStamp")
            < ops.iter().position(|op| op["op"] == "load")
    );
}

#[derive(Default)]
struct Stateful {
    row: Option<String>,
    stamp: u64,
    savepoint: Option<(Option<String>, u64)>,
    calls: BTreeMap<String, (String, Option<String>)>,
    receipts: BTreeMap<String, (u64, String)>,
    handlers: usize,
    loaders: usize,
    delete_on_handle: bool,
    log: Vec<Value>,
}
struct StatefulHost(Mutex<Stateful>);
impl StatefulHost {
    fn new() -> Self {
        Self(Mutex::new(Stateful::default()))
    }
}
impl Host for StatefulHost {
    fn call(&self, request: Value) -> Pin<Box<dyn Future<Output = HostResult<Value>> + Send + '_>> {
        Box::pin(async move {
            let mut state = self.0.lock().unwrap();
            state.log.push(request.clone());
            let answer = match request["op"].as_str().unwrap() {
                "claim" => {
                    let (sequence, receipt) = state
                        .receipts
                        .get(request["clientId"].as_str().unwrap())
                        .cloned()
                        .unwrap_or((0, String::new()));
                    json!({"clientId":request["clientId"],"owner":"alice","sequence":sequence,"receipt":if receipt.is_empty() {Value::Null} else {json!(receipt)}})
                }
                "saveReceipt" => {
                    state.receipts.insert(
                        request["clientId"].as_str().unwrap().into(),
                        (
                            request["sequence"].as_u64().unwrap(),
                            request["receipt"].as_str().unwrap().into(),
                        ),
                    );
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
                    state.savepoint = Some((state.row.clone(), state.stamp));
                    Value::Null
                }
                "rollback" => {
                    let (row, stamp) = state.savepoint.clone().unwrap();
                    state.row = row;
                    state.stamp = stamp;
                    Value::Null
                }
                "release" => {
                    state.savepoint = None;
                    Value::Null
                }
                "handleAction" => {
                    state.handlers += 1;
                    if request["name"] == "Read" {
                        return Ok(
                            json!({"outputs":{"todo":{"id":"t"}},"changes":[],"memberships":[]}),
                        );
                    }
                    if request["name"] == "Delete" {
                        if state.delete_on_handle {
                            state.row = None;
                        }
                        return Ok(json!({"outputs":{},"changes":[],"memberships":[]}));
                    }
                    if request["arguments"]["todo"].is_null() {
                        return Ok(json!({"outputs":{},"changes":[],"memberships":[]}));
                    }
                    let title = request["arguments"]["todo"]["title"].as_str().unwrap();
                    match title {
                        "refuse" => json!({"rejection":"todo.refused"}),
                        "crash" => json!({"error":"application fault"}),
                        "missing" => json!({"outputs":{},"changes":[],"memberships":[]}),
                        _ => {
                            state.row = Some(title.into());
                            json!({"outputs":{},"changes":[],"memberships":[]})
                        }
                    }
                }
                "advanceStamp" => {
                    state.stamp += 1;
                    json!(state.stamp)
                }
                // The one Todo belongs to no Channel.
                "memberships" => json!([]),
                "ensureStamp" => {
                    if state.stamp == 0 {
                        state.stamp = 1;
                    }
                    json!(state.stamp)
                }
                "load" => {
                    state.loaders += 1;
                    match &state.row {
                        Some(title) => json!([{"id":"t","title":title}]),
                        None => json!([null]),
                    }
                }
                _ => return Err(format!("unexpected host operation {}", request["op"])),
            };
            Ok(answer)
        })
    }
}

/// A retained historical descriptor: `Edit`'s `todo` output is bound to its
/// input (`inputIdentity`), which newly compiled source no longer emits. These
/// tests keep the decoder's bounded compatibility for saved calls and retained
/// versions; the explicit-output contract is covered by the #140 tests below.
fn stateful_config() -> Config {
    stateful_config_with_note(false)
}
fn stateful_config_with_note(note: bool) -> Config {
    let mut fields = json!([{"name":"id","type":{"kind":"scalar","name":"string"},"nullable":false},{"name":"title","type":{"kind":"scalar","name":"string"},"nullable":false}]);
    if note {
        fields
            .as_array_mut()
            .unwrap()
            .push(json!({"name":"note","type":{"kind":"scalar","name":"string"},"nullable":true}));
    }
    Config::decode(json!({"schema":{"enums":[],"models":[{"name":"Todo","version":1,"identity":["id"],"fields":fields}],"resultModels":[{"name":"Todo","version":1,"identity":["id"],"fields":fields,"enums":[]}],"actions":[{"name":"Edit","version":1,"inputs":[{"kind":"model","name":"todo","model":"Todo","operation":"update","cardinality":"single","allowedPatchFields":["title"]}],"outputs":[{"name":"todo","kind":"model","modelReadVersion":1,"model":"Todo","cardinality":"single","source":{"inputIdentity":"todo"}}]},{"name":"Read","version":1,"inputs":[],"outputs":[{"name":"todo","kind":"model","modelReadVersion":1,"model":"Todo","cardinality":"single","source":"handlerIdentity","handlerType":{"kind":"identity","model":"Todo","fields":[{"name":"id","type":{"kind":"scalar","name":"string"}}]}}]}]},"mutations":[],"loaders":["Todo"]})).unwrap()
}

#[test]
fn supplied_optional_implicit_model_requires_a_row_but_explicit_nullable_identity_may_be_missing() {
    let mut raw = serde_json::to_value(stateful_config()).unwrap();
    raw["schema"]["actions"][0]["inputs"][0]["cardinality"] = json!("optional");
    raw["schema"]["actions"][0]["outputs"][0]["cardinality"] = json!("optional");
    raw["schema"]["actions"][1]["outputs"][0]["cardinality"] = json!("optional");
    let config = Config::decode(raw).unwrap();
    let host = StatefulHost::new();
    let present = stateful_push(
        &config,
        &host,
        1,
        vec![call(1, "01890f47-1234-7123-8123-123456789abc", "missing")],
    );
    assert_eq!(
        present["rejections"],
        json!([{"ordinal":1,"code":"loader.invalid"}])
    );
    let absent = json!({"ordinal":1,"callId":"01890f47-1234-7123-8123-123456789abd","name":"Edit","version":1,"args":{}});
    let selected = json!({"ordinal":2,"callId":"01890f47-1234-7123-8123-123456789abe","name":"Read","version":1,"args":{}});
    let next = stateful_push(&config, &host, 2, vec![absent, selected]);
    assert_eq!(
        next["completions"][0]["outcome"]["result"],
        json!({"todo":null})
    );
    assert_eq!(
        next["completions"][1]["outcome"]["result"],
        json!({"todo":null})
    );
    assert_eq!(next["records"][0]["state"], Value::Null);
}

#[test]
fn delete_confirmation_requires_loader_absence_and_rolls_back_a_forgotten_delete() {
    let mut raw = serde_json::to_value(stateful_config()).unwrap();
    raw["schema"]["actions"][0]["name"] = json!("Delete");
    raw["schema"]["actions"][0]["inputs"][0]["operation"] = json!("delete");
    raw["schema"]["actions"][0]["inputs"][0]
        .as_object_mut()
        .unwrap()
        .remove("allowedPatchFields");
    raw["schema"]["actions"][0]["outputs"][0]["kind"] = json!("deleteIdentity");
    raw["schema"]["actions"][0]["outputs"][0]
        .as_object_mut()
        .unwrap()
        .remove("modelReadVersion");
    let config = Config::decode(raw).unwrap();
    let host = StatefulHost::new();
    host.0.lock().unwrap().row = Some("A".into());
    let invoke = |ordinal, id| json!({"ordinal":ordinal,"callId":id,"name":"Delete","version":1,"args":{"todo":{"id":"t"}}});
    let failed = stateful_push(
        &config,
        &host,
        1,
        vec![invoke(1, "01890f47-1234-7123-8123-123456789abc")],
    );
    assert_eq!(
        failed["rejections"],
        json!([{"ordinal":1,"code":"loader.invalid"}])
    );
    assert_eq!(host.0.lock().unwrap().row.as_deref(), Some("A"));
    host.0.lock().unwrap().delete_on_handle = true;
    let succeeded = stateful_push(
        &config,
        &host,
        2,
        vec![invoke(1, "01890f47-1234-7123-8123-123456789abd")],
    );
    assert_eq!(
        succeeded["completions"][0]["outcome"]["result"],
        json!({"todo":{"id":"t"}})
    );
    assert_eq!(succeeded["records"][0]["state"], Value::Null);
}

#[test]
fn mismatched_model_binding_rejects_only_its_action_before_handler() {
    let mut raw = serde_json::to_value(stateful_config()).unwrap();
    raw["schema"]["models"].as_array_mut().unwrap().extend([
        json!({"name":"Parent","version":1,"identity":["id"],"fields":[{"name":"id","type":{"kind":"scalar","name":"string"},"nullable":false}]}),
        json!({"name":"Child","version":1,"identity":["id"],"fields":[{"name":"id","type":{"kind":"scalar","name":"string"},"nullable":false},{"name":"parentId","type":{"kind":"scalar","name":"string"},"nullable":false}],"relations":[{"name":"parent","target":"Parent","fields":["parentId"],"targetFields":["id"],"onDelete":"none"}]}),
    ]);
    raw["schema"]["actions"].as_array_mut().unwrap().push(json!({"name":"AddPair","version":1,"inputs":[
        {"kind":"model","name":"parent","model":"Parent","operation":"create","cardinality":"single"},
        {"kind":"model","name":"child","model":"Child","operation":"create","cardinality":"single","bindings":[{"slot":"parent","fields":["parentId"]}]}
    ],"outputs":[]}));
    raw["loaders"] = json!(["Todo", "Parent", "Child"]);
    raw["models"] = json!([]);
    let config = Config::decode(raw).unwrap();
    let host = StatefulHost::new();
    let invalid = json!({"ordinal":1,"callId":"01890f47-1234-7123-8123-123456789abc","name":"AddPair","version":1,"args":{"parent":{"id":"p"},"child":{"id":"c","parentId":"other"}}});
    let receipt = stateful_push(
        &config,
        &host,
        1,
        vec![
            invalid,
            call(2, "01890f47-1234-7123-8123-123456789abd", "B"),
        ],
    );
    assert_eq!(
        receipt["rejections"],
        json!([{"ordinal":1,"code":"action.invalid"}])
    );
    assert_eq!(
        receipt["completions"][1]["outcome"]["result"]["todo"]["title"],
        "B"
    );
    assert_eq!(host.0.lock().unwrap().handlers, 1);
}

#[test]
fn compatible_added_nullable_field_does_not_conflict_with_old_saved_stamp() {
    let host = StatefulHost::new();
    let old = "01890f47-1234-7123-8123-123456789abc";
    let reader = "01890f47-1234-7123-8123-123456789abd";
    stateful_push(&stateful_config(), &host, 1, vec![call(1, old, "A")]);
    let current = stateful_config_with_note(true);
    let read = json!({"ordinal":1,"callId":reader,"name":"Read","version":1,"args":{}});
    let receipt = stateful_push(&current, &host, 2, vec![read, call(2, old, "A")]);
    assert_eq!(
        receipt["completions"][1]["outcome"]["result"]["todo"],
        json!({"id":"t","title":"A"})
    );
    assert_eq!(
        receipt["records"][0]["state"],
        json!({"title":"A","note":null})
    );
    assert_eq!(receipt["records"][0]["stamp"], 1);
}

#[test]
fn unequal_content_at_equal_stamp_is_a_storage_fault() {
    let config = stateful_config();
    let host = StatefulHost::new();
    let old = "01890f47-1234-7123-8123-123456789abc";
    let reader = "01890f47-1234-7123-8123-123456789abd";
    stateful_push(&config, &host, 1, vec![call(1, old, "A")]);
    {
        let mut state = host.0.lock().unwrap();
        let response = &mut state.calls.get_mut(old).unwrap().1;
        let mut saved: Value = serde_json::from_str(response.as_ref().unwrap()).unwrap();
        saved["records"][0]["state"]["title"] = json!("OTHER");
        *response = Some(saved.to_string());
    }
    let read = json!({"ordinal":1,"callId":reader,"name":"Read","version":1,"args":{}});
    let body = json!({"clientId":"device","batchSequence":2,"models":{"Todo":1},"mutations":[read,call(2,old,"A")]});
    let error = run(process_action_push(
        &config,
        "alice",
        body.to_string().as_bytes(),
        &host,
    ))
    .unwrap_err();
    assert_eq!(error.code, "storage.invalid");
    assert!(
        host.0
            .lock()
            .unwrap()
            .receipts
            .get("device")
            .is_some_and(|(sequence, _)| *sequence == 1)
    );
}
fn call(ordinal: u64, id: &str, title: &str) -> Value {
    json!({"ordinal":ordinal,"callId":id,"name":"Edit","version":1,"args":{"todo":{"id":"t","title":title}}})
}
fn stateful_push(config: &Config, host: &StatefulHost, sequence: u64, calls: Vec<Value>) -> Value {
    let body =
        json!({"clientId":"device","batchSequence":sequence,"models":{"Todo":1},"mutations":calls});
    serde_json::from_str(
        &run(process_action_push(
            config,
            "alice",
            body.to_string().as_bytes(),
            host,
        ))
        .unwrap(),
    )
    .unwrap()
}

#[test]
fn saved_old_result_after_fresh_new_result_keeps_newest_authority() {
    let config = stateful_config();
    let host = StatefulHost::new();
    let old = "01890f47-1234-7123-8123-123456789abc";
    let new = "01890f47-1234-7123-8123-123456789abd";
    let first = stateful_push(&config, &host, 1, vec![call(1, old, "A")]);
    let second = stateful_push(
        &config,
        &host,
        2,
        vec![call(1, new, "B"), call(2, old, "A")],
    );
    assert_eq!(
        first["completions"][0]["outcome"]["result"]["todo"]["title"],
        "A"
    );
    assert_eq!(
        second["completions"][0]["outcome"]["result"]["todo"]["title"],
        "B"
    );
    assert_eq!(
        second["completions"][1]["outcome"]["result"]["todo"]["title"],
        "A"
    );
    assert_eq!(second["records"][0]["stamp"], 2);
    assert_eq!(second["records"][0]["state"]["title"], "B");
    let state = host.0.lock().unwrap();
    assert_eq!((state.handlers, state.loaders), (2, 2));
}

#[test]
fn invalid_and_refused_calls_do_not_replace_a_successful_call_or_original_claim() {
    let config = stateful_config();
    let host = StatefulHost::new();
    let old = "01890f47-1234-7123-8123-123456789abc";
    let fresh = "01890f47-1234-7123-8123-123456789abd";
    let invalid = "01890f47-1234-7123-8123-123456789abe";
    let refused = "01890f47-1234-7123-8123-123456789abf";
    stateful_push(&config, &host, 1, vec![call(1, old, "A")]);
    let mut unsupported = call(2, invalid, "Z");
    unsupported["version"] = json!(2);
    let receipt = stateful_push(
        &config,
        &host,
        2,
        vec![
            call(1, old, "DIFFERENT"),
            unsupported,
            call(3, refused, "refuse"),
            call(4, fresh, "B"),
        ],
    );
    assert_eq!(
        receipt["rejections"],
        json!([{"ordinal":1,"code":"call.identity_conflict"},{"ordinal":2,"code":"action_version_unsupported"},{"ordinal":3,"code":"todo.refused"}])
    );
    assert_eq!(
        receipt["completions"][3]["outcome"]["result"]["todo"]["title"],
        "B"
    );
    let state = host.0.lock().unwrap();
    assert_eq!(state.row.as_deref(), Some("B"));
    assert_eq!(state.handlers, 3);
    assert!(
        state
            .calls
            .get(old)
            .unwrap()
            .1
            .as_ref()
            .unwrap()
            .contains("\"A\"")
    );
}

/// A host whose Query handlers forge business effects the Query context
/// cannot express, as a non-TypeScript host or a defect could.
struct ForgedQueryHost(Mutex<Vec<Value>>);
impl Host for ForgedQueryHost {
    fn call(&self, request: Value) -> Pin<Box<dyn Future<Output = HostResult<Value>> + Send + '_>> {
        Box::pin(async move {
            self.0.lock().unwrap().push(request.clone());
            let todo = json!({"model":"Todo","identity":{"id":"t1"}});
            let membership =
                json!({"channel":"c","model":"Todo","identity":{"id":"t1"},"present":true});
            Ok(match request["op"].as_str().unwrap() {
                "claim" => json!({"clientId":"device","owner":"alice","sequence":0,"receipt":null}),
                "claimCall" => json!({"fresh":true,"request":request["request"],"response":null}),
                "handleAction" => match request["name"].as_str().unwrap() {
                    "Changes" => {
                        json!({"outputs":{"message":"x"},"changes":[todo],"memberships":[]})
                    }
                    "Enrolls" => {
                        json!({"outputs":{"message":"x"},"changes":[],"memberships":[membership]})
                    }
                    "Both" => {
                        json!({"outputs":{"message":"x"},"changes":[todo],"memberships":[membership]})
                    }
                    _ => json!({"outputs":{"message":"ok"},"changes":[],"memberships":[]}),
                },
                "ensureStamp" | "advanceStamp" => json!(1),
                "load" => json!([{"id":"t1"}]),
                _ => Value::Null,
            })
        })
    }
}

fn forged_config() -> Config {
    let message = json!([{"name":"message","kind":"value","type":{"kind":"scalar","name":"string"},"cardinality":"single","source":"handlerValue"}]);
    let actions: Vec<Value> = [
        ("Changes", "query"),
        ("Enrolls", "query"),
        ("Both", "query"),
        ("Read", "query"),
        ("Save", "mutation"),
    ]
    .into_iter()
    .map(|(name, kind)| json!({"name":name,"version":1,"kind":kind,"inputs":[],"outputs":message}))
    .collect();
    Config::decode(json!({"schema":{"enums":[],"models":[{"name":"Todo","identity":["id"],"fields":[{"name":"id","type":{"kind":"scalar","name":"string"},"nullable":false}]}],"actions":actions},"mutations":[],"loaders":["Todo"]})).unwrap()
}

#[test]
fn forged_query_effects_reject_only_that_call_before_framework_handling() {
    let config = forged_config();
    let host = ForgedQueryHost(Mutex::new(vec![]));
    let calls = ["Changes", "Save", "Enrolls", "Both", "Read"]
        .into_iter()
        .enumerate()
        .map(|(index, name)| {
            json!({"ordinal":index+1,"callId":format!("01890f47-1234-7123-8123-123456789ab{}",index+1),"name":name,"version":1,"args":{}})
        })
        .collect::<Vec<_>>();
    let request =
        json!({"clientId":"device","batchSequence":1,"models":{"Todo":1},"mutations":calls});
    let receipt: Value = serde_json::from_str(
        &run(process_action_push(
            &config,
            "alice",
            request.to_string().as_bytes(),
            &host,
        ))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        receipt["rejections"],
        json!([
            {"ordinal":1,"code":"query.effects_forbidden"},
            {"ordinal":3,"code":"query.effects_forbidden"},
            {"ordinal":4,"code":"query.effects_forbidden"}
        ])
    );
    for index in [0, 2, 3] {
        assert_eq!(
            receipt["completions"][index]["outcome"],
            json!({"status":"failed","code":"query.effects_forbidden","execution":"rejected"})
        );
    }
    assert_eq!(
        receipt["completions"][1]["outcome"]["result"],
        json!({"message":"ok"})
    );
    assert_eq!(
        receipt["completions"][4]["outcome"]["result"],
        json!({"message":"ok"})
    );
    assert_eq!(receipt["records"], json!([]));
    let ops = host.0.lock().unwrap();
    // No guard, stamping, readback, membership or publication happened.
    for op in [
        "ensureStamp",
        "advanceStamp",
        "lockRecord",
        "memberships",
        "setMembership",
        "load",
        "publish",
    ] {
        assert!(!ops.iter().any(|request| request["op"] == op), "{op}");
    }
    let rolled: Vec<_> = ops
        .iter()
        .filter(|request| request["op"] == "rollback")
        .map(|request| request["ordinal"].clone())
        .collect();
    assert_eq!(rolled, [json!(1), json!(3), json!(4)]);
    // Each forbidden outcome is saved, so a retry replays the rejection.
    assert_eq!(ops.iter().filter(|op| op["op"] == "saveCall").count(), 5);
}

#[test]
fn forged_query_effects_are_rejected_on_the_direct_path_too() {
    let config = forged_config();
    let host = ForgedQueryHost(Mutex::new(vec![]));
    let request = json!({"call":{"callId":"01890f47-1234-7123-8123-123456789abc","name":"Both","version":1,"args":{}},"models":{"Todo":1}});
    let response: Value = serde_json::from_str(
        &run(axton_server::process_action(
            &config,
            "alice",
            request.to_string().as_bytes(),
            &host,
        ))
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        response["completion"]["outcome"],
        json!({"status":"failed","code":"query.effects_forbidden","execution":"rejected"})
    );
    assert_eq!(response["records"], json!([]));
    let ops = host.0.lock().unwrap();
    assert!(ops.iter().any(|op| op["op"] == "rollback"));
    assert!(
        !ops.iter()
            .any(|op| op["op"] == "ensureStamp" || op["op"] == "load" || op["op"] == "publish")
    );
}

#[test]
fn backend_config_refuses_query_descriptors_with_model_operands() {
    let error = Config::decode(json!({"schema":{"enums":[],"models":[{"name":"Todo","identity":["id"],"fields":[{"name":"id","type":{"kind":"scalar","name":"string"},"nullable":false}]}],"actions":[
        {"name":"Edit","version":1,"kind":"query","inputs":[{"kind":"model","name":"todo","model":"Todo","operation":"delete","cardinality":"single"}],"outputs":[{"name":"todo","kind":"deleteIdentity","model":"Todo","cardinality":"single","source":{"inputIdentity":"todo"}}]}
    ]},"mutations":[],"loaders":["Todo"]}))
    .err()
    .expect("a Query with a Model operand is not a valid backend config");
    assert!(error.to_string().contains("Model operand"), "{error}");
}

// Caller authority versus changed records (#140). Inputs are mandatory caller
// authority; explicit outputs are independent Loader reads; extra touches are
// changed records, never caller authority.
mod support;
use support::{Backend, authority, edit, reference};

fn todo(id: &str, title: &str) -> Value {
    json!({"id":id,"title":title})
}

/// Input Todo A is modified and same-name output `todo` selects Todo B: the
/// receipt confirms A and `result.todo` is B's snapshot. Omitting the output
/// fails the call; it is never filled from the input.
#[test]
fn an_input_and_a_same_name_output_are_independent_and_a_missing_output_never_falls_back() {
    let backend = Backend::new();
    backend.seed("Todo", "a", todo("a", "old"), None);
    backend.seed("Todo", "b", todo("b", "other"), None);
    backend.script(
        "EditAndRead",
        json!({"outputs":{"todo":{"id":"b"}},"changes":[],"memberships":[]}),
    );
    let receipt = support::push(
        &backend,
        1,
        json!({"Todo":1}),
        vec![edit(1, 1, "EditAndRead", "a", "typed")],
    );
    assert_eq!(receipt["rejections"], json!([]));
    assert_eq!(
        receipt["completions"][0]["outcome"]["result"],
        json!({"todo":{"id":"b","title":"other"}})
    );
    let records = authority(&receipt);
    assert!(
        records.contains(&("Todo".into(), "a".into(), 1)),
        "{records:?}"
    );
    let a = receipt["records"]
        .as_array()
        .unwrap()
        .iter()
        .find(|record| record["identity"]["id"] == "a")
        .unwrap();
    assert_eq!(a["state"], json!({"title":"typed"}));
    // The handler omits its declared output: the call fails and rolls back.
    backend.script(
        "EditAndRead",
        json!({"outputs":{},"changes":[],"memberships":[]}),
    );
    let receipt = support::push(
        &backend,
        2,
        json!({"Todo":1}),
        vec![edit(1, 2, "EditAndRead", "a", "lost")],
    );
    assert_eq!(
        receipt["rejections"],
        json!([{"ordinal":1,"code":"handler.invalid"}])
    );
    assert_eq!(receipt["records"], json!([]));
    assert_eq!(backend.row("Todo", "a"), Some(todo("a", "typed")));
    assert_eq!(
        backend.stamp("Todo", "a"),
        Some(1),
        "the failed call's stamp rolled back"
    );
}

/// An operation without outputs answers a null result on the wire (the
/// generated SDK's `void`) and still carries its input authority.
#[test]
fn no_declared_outputs_answer_null_and_keep_input_authority() {
    let backend = Backend::new();
    backend.seed("Todo", "a", todo("a", "old"), Some(4));
    let receipt = support::push(
        &backend,
        1,
        json!({"Todo":1}),
        vec![edit(1, 1, "Edit", "a", "typed")],
    );
    assert_eq!(receipt["completions"][0]["outcome"]["result"], Value::Null);
    assert_eq!(authority(&receipt), [("Todo".into(), "a".into(), 5)]);
    assert_eq!(receipt["records"][0]["state"], json!({"title":"typed"}));
}

/// An extra touch of a Model the caller never declared succeeds: the Project
/// advances and fans out to its Channel, its Loader is not invoked for the
/// caller, and it is absent from the caller's authority.
#[test]
fn an_extra_touch_of_an_undeclared_model_fans_out_without_caller_authority() {
    let backend = Backend::new();
    backend.seed("Todo", "a", todo("a", "old"), None);
    backend.seed("Project", "p", json!({"id":"p","name":"P"}), Some(2));
    backend.enroll("project:p", "Project", "p", 7);
    backend.script(
        "Edit",
        json!({"outputs":{},"changes":[reference("Project","p")],"memberships":[]}),
    );
    let receipt = support::push(
        &backend,
        1,
        json!({"Todo":1}),
        vec![edit(1, 1, "Edit", "a", "typed")],
    );
    assert_eq!(receipt["rejections"], json!([]));
    assert_eq!(authority(&receipt), [("Todo".into(), "a".into(), 1)]);
    assert_eq!(backend.stamp("Project", "p"), Some(3));
    assert_eq!(backend.head("project:p"), 8);
    assert_eq!(
        backend.invalidation("project:p", "Project", "p"),
        Some((8, 3))
    );
    assert_eq!(
        backend.loaded_models(),
        ["Todo"],
        "no Project read for the caller"
    );
}

/// A touched Project the caller also selects as an explicit output is read
/// as an actual output: at the one stamp settlement allocated, with the
/// caller's declared version and the Loader's authorization. An undeclared
/// version or a refusal rejects the call and rolls back the whole mutation.
#[test]
fn a_touched_output_is_an_actual_read_and_its_failure_rolls_back_the_mutation() {
    let prepare = || {
        let backend = Backend::new();
        backend.seed("Todo", "a", todo("a", "old"), Some(1));
        backend.seed("Project", "p", json!({"id":"p","name":"P"}), Some(2));
        backend.enroll("project:p", "Project", "p", 7);
        backend.script(
            "EditAndReadProject",
            json!({"outputs":{"project":{"id":"p"}},"changes":[reference("Project","p")],"memberships":[]}),
        );
        backend
    };
    let backend = prepare();
    let receipt = support::push(
        &backend,
        1,
        json!({"Todo":1,"Project":1}),
        vec![edit(1, 1, "EditAndReadProject", "a", "typed")],
    );
    assert_eq!(receipt["rejections"], json!([]));
    assert_eq!(
        receipt["completions"][0]["outcome"]["result"],
        json!({"project":{"id":"p","name":"P"}})
    );
    assert_eq!(
        authority(&receipt),
        [
            ("Project".into(), "p".into(), 3),
            ("Todo".into(), "a".into(), 2)
        ]
    );
    assert_eq!(backend.count("advanceStamp"), 2);
    assert_eq!(
        backend.count("ensureStamp"),
        0,
        "the settled stamp is reused"
    );
    for (label, models, refuse, code) in [
        (
            "undeclared",
            json!({"Todo":1}),
            false,
            "model_version_unsupported",
        ),
        (
            "refused",
            json!({"Todo":1,"Project":1}),
            true,
            "project.forbidden",
        ),
    ] {
        let backend = prepare();
        if refuse {
            backend.refuse_load("Project", "p");
        }
        let before = backend.tables();
        let receipt = support::push(
            &backend,
            1,
            models,
            vec![edit(1, 1, "EditAndReadProject", "a", "typed")],
        );
        assert_eq!(
            receipt["rejections"],
            json!([{"ordinal":1,"code":code}]),
            "{label}"
        );
        assert_eq!(receipt["records"], json!([]), "{label}");
        let after = backend.tables();
        assert_eq!(
            after.rows, before.rows,
            "{label}: business write rolled back"
        );
        assert_eq!(after.stamps, before.stamps, "{label}: stamps rolled back");
        assert_eq!(after.heads, before.heads, "{label}: positions rolled back");
        assert_eq!(after.invalidations, before.invalidations, "{label}");
    }
}

/// A touch of the input target itself and a repeated touch of another record
/// deduplicate: one stamp per changed record, never two.
#[test]
fn a_duplicate_or_inferred_touch_allocates_one_stamp() {
    let backend = Backend::new();
    backend.seed("Todo", "a", todo("a", "old"), Some(1));
    backend.seed("Project", "p", json!({"id":"p","name":"P"}), Some(6));
    backend.script(
        "Edit",
        json!({"outputs":{},"changes":[
            reference("Todo","a"),reference("Project","p"),reference("Project","p")],"memberships":[]}),
    );
    let receipt = support::push(
        &backend,
        1,
        json!({"Todo":1}),
        vec![edit(1, 1, "Edit", "a", "typed")],
    );
    assert_eq!(receipt["rejections"], json!([]));
    assert_eq!(backend.count("advanceStamp"), 2);
    assert_eq!(backend.stamp("Todo", "a"), Some(2));
    assert_eq!(backend.stamp("Project", "p"), Some(7));
    assert_eq!(authority(&receipt), [("Todo".into(), "a".into(), 2)]);
}
