//! Native Load contracts: descriptors, portable continuation state and the
//! batched page wire. Envelope checks are structural; page content is judged
//! per item so one malformed page fails only its own job.
use axton_core::*;
use serde_json::{Value, json};

fn id(n: u64) -> String {
    format!("01890f47-1234-7123-8123-{n:012x}")
}

fn model(name: &str, extra: Value) -> Value {
    let mut fields = vec![
        json!({"name":"id","type":{"kind":"scalar","name":"uuid"},"nullable":false}),
        json!({"name":"title","type":{"kind":"scalar","name":"string"},"nullable":false}),
    ];
    if let Value::Array(more) = extra {
        fields.extend(more);
    }
    json!({"name":name,"version":1,"identity":["id"],"fields":fields})
}

fn output(name: &str, model: &str) -> Value {
    json!({
        "name":name,"kind":"model","cardinality":"list","source":"handlerIdentity",
        "model":model,"modelReadVersion":1,
        "handlerType":{"kind":"identity","model":model,"fields":[
            {"name":"id","type":{"kind":"scalar","name":"uuid"}}
        ]}
    })
}

fn load_descriptor() -> Value {
    json!({
        "name":"ProjectTodos","version":1,
        "inputs":[
            {"kind":"value","name":"projectId","type":{"kind":"scalar","name":"uuid"},"nullable":false,"list":false,"required":true,"cardinality":"single"},
            {"kind":"value","name":"status","type":{"kind":"enum","name":"Status"},"nullable":true,"list":false,"required":true,"cardinality":"single"},
            {"kind":"value","name":"tags","type":{"kind":"scalar","name":"string"},"nullable":false,"list":true,"required":true,"cardinality":"list"}
        ],
        "outputs":[output("todos","Todo"), output("notes","Note")],
        "input":{"models":[],"enums":[{"name":"Status","values":["open","done"]}]},
        "outputEnums":[]
    })
}

fn raw_schema() -> Value {
    let read = |name: &str| {
        let mut m = model(name, json!([]));
        m["enums"] = json!([]);
        m
    };
    json!({
        "enums":[{"name":"Status","values":["open","done"]}],
        "models":[model("Todo", json!([])), model("Note", json!([]))],
        "resultModels":[read("Todo"), read("Note")],
        "actions":[{"name":"Send","version":1,"kind":"mutation","inputs":[],"outputs":[]}],
        "loads":[load_descriptor()]
    })
}

fn schema() -> Schema {
    Schema::from_value(raw_schema()).unwrap()
}

fn intent(n: u64, continuation: Value) -> Value {
    json!({
        "loadId": id(n), "callId": id(100 + n),
        "name":"ProjectTodos","version":1,
        "args":{"projectId":id(7),"status":null,"tags":["a"]},
        "continuation": continuation, "models":{"Todo":1,"Note":1}
    })
}

fn request(items: Vec<Value>) -> LoadBatchRequest {
    LoadBatchRequest::decode_envelope(json!({ "loads": items }).to_string().as_bytes()).unwrap()
}

fn succeeded(n: u64, data: Value, next: Value, records: Value) -> Value {
    json!({
        "loadId": id(n), "callId": id(100 + n),
        "outcome":{"status":"succeeded","data":data,"next":next},
        "records":records
    })
}

fn record(model: &str, n: u64) -> Value {
    json!({"model":model,"identity":{"id":id(n)},"stamp":3,"state":{"title":"t"}})
}

fn response(items: Vec<Value>) -> Vec<u8> {
    json!({ "loads": items }).to_string().into_bytes()
}

#[test]
fn limits_are_the_specified_initial_values() {
    assert_eq!(limits::LOAD_BATCH_ITEMS, 8);
    assert_eq!(limits::LOAD_REQUEST_BYTES, 1024 * 1024);
    assert_eq!(limits::LOAD_PAGE_BYTES, 1024 * 1024);
    assert_eq!(limits::LOAD_RESPONSE_BYTES, 8 * 1024 * 1024);
    assert_eq!(limits::LOAD_PAGE_IDENTITIES, 1000);
    assert_eq!(limits::LOAD_STATE_BYTES, 64 * 1024);
    assert_eq!(limits::LOAD_STATE_DEPTH, 64);
}

#[test]
fn first_and_end_null_stays_distinct_from_a_null_state() {
    let first: LoadIntent = serde_json::from_value(intent(1, Value::Null)).unwrap();
    assert_eq!(first.continuation, None);
    let null_state: LoadIntent = serde_json::from_value(intent(1, json!({"state":null}))).unwrap();
    assert_eq!(
        null_state.continuation,
        Some(Continuation { state: Value::Null })
    );
    let batch = request(vec![
        intent(1, Value::Null),
        intent(2, json!({"state":null})),
    ]);
    let bytes = batch.encode().unwrap();
    let text = String::from_utf8(bytes.clone()).unwrap();
    assert!(text.contains(r#""continuation":null"#), "{text}");
    assert!(text.contains(r#""continuation":{"state":null}"#), "{text}");
    let decoded = LoadBatchRequest::decode_envelope(&bytes).unwrap();
    assert_eq!(decoded.loads[0].continuation, None);
    assert_eq!(
        decoded.loads[1].continuation,
        Some(Continuation { state: Value::Null })
    );
    // The wrapper is exact: no missing state, no extra member, no missing key.
    for bad in [
        json!({}),
        json!({"state":1,"cursor":2}),
        json!(1),
        json!([]),
    ] {
        assert!(
            serde_json::from_value::<LoadIntent>(intent(1, bad.clone())).is_err(),
            "{bad}"
        );
    }
    let mut missing = intent(1, Value::Null);
    missing.as_object_mut().unwrap().remove("continuation");
    assert!(serde_json::from_value::<LoadIntent>(missing).is_err());
}

#[test]
fn nested_states_round_trip_as_normalized_portable_json() {
    let state = json!({
        "cursor":{"after":[1, 2.5, "x", null, true, {"deep":[[]]}]},
        "offset": 1.0, "negativeZero": -0.0, "unicode":"\u{e9}\u{1F600}", "empty":{}
    });
    let normalized = Continuation::new(state).unwrap();
    assert_eq!(
        normalized.state,
        json!({
            "cursor":{"after":[1, 2.5, "x", null, true, {"deep":[[]]}]},
            "offset": 1, "negativeZero": 0, "unicode":"\u{e9}\u{1F600}", "empty":{}
        })
    );
    for state in [
        Value::Null,
        json!(0),
        json!("token"),
        json!([null]),
        json!(false),
        normalized.state.clone(),
    ] {
        let batch = request(vec![intent(1, json!({ "state": state }))]);
        let decoded = LoadBatchRequest::decode_envelope(&batch.encode().unwrap()).unwrap();
        let item = decoded.loads[0].clone().normalize(&schema()).unwrap();
        assert_eq!(item.continuation, Some(Continuation::new(state).unwrap()));
    }
}

#[test]
fn continuation_state_is_bounded_portable_json() {
    let nest = |depth: usize| {
        let mut value = Value::Null;
        for _ in 0..depth {
            value = json!([value]);
        }
        value
    };
    assert!(Continuation::new(nest(limits::LOAD_STATE_DEPTH)).is_ok());
    assert!(Continuation::new(nest(limits::LOAD_STATE_DEPTH + 1)).is_err());
    // Canonical bytes of the state itself: `"` + text + `"`.
    let fits = "a".repeat(limits::LOAD_STATE_BYTES - 2);
    assert!(Continuation::new(json!(fits)).is_ok());
    assert!(Continuation::new(json!(format!("{fits}a"))).is_err());
    for safe in [
        json!(9_007_199_254_740_991_u64),
        json!(-9_007_199_254_740_991_i64),
    ] {
        assert_eq!(Continuation::new(safe.clone()).unwrap().state, safe);
    }
    // Integers beyond the JavaScript safe range must be application strings;
    // an integral double beyond it is the same unsafe integer.
    for unsafe_number in [
        json!(9_007_199_254_740_992_u64),
        json!(-9_007_199_254_740_992_i64),
        json!(u64::MAX),
        json!(1e300),
        json!({"nested":[9_007_199_254_740_993_u64]}),
    ] {
        assert!(
            Continuation::new(unsafe_number.clone()).is_err(),
            "{unsafe_number}"
        );
    }
    assert!(Continuation::new(json!(1e-300)).is_ok());
    // A deserialized wrapper keeps its bytes until normalized per item.
    let raw: Continuation = serde_json::from_value(json!({"state":1e300})).unwrap();
    assert!(raw.normalized().is_err());
}

#[test]
fn intents_normalize_ids_args_and_continuation_against_the_load() {
    let schema = schema();
    let mut raw = intent(1, json!({"state":{"n":2.0}}));
    raw["loadId"] = json!(id(1).to_uppercase());
    raw["callId"] = json!(id(101).to_uppercase());
    raw["args"]["projectId"] = json!(id(7).to_uppercase());
    raw["args"]["status"] = json!("open");
    let item: LoadIntent = serde_json::from_value(raw).unwrap();
    let item = item.normalize(&schema).unwrap();
    assert_eq!(item.load_id, id(1));
    assert_eq!(item.call_id, id(101));
    assert_eq!(
        item.args,
        json!({"projectId":id(7),"status":"open","tags":["a"]})
    );
    assert_eq!(
        item.continuation,
        Some(Continuation {
            state: json!({"n":2})
        })
    );
    let load = schema.load("ProjectTodos", 1).unwrap();
    // Ordinary nullable inputs still name their argument; lists stay ordered.
    for bad in [
        json!({"projectId":id(7),"tags":[]}),
        json!({"projectId":id(7),"status":"closed","tags":[]}),
        json!({"projectId":id(7),"status":null,"tags":null}),
        json!({"projectId":id(7),"status":null,"tags":[],"once":true}),
        json!([]),
    ] {
        assert!(normalize_load_args(&schema, load, &bad).is_err(), "{bad}");
    }
    assert_eq!(
        normalize_load_args(
            &schema,
            load,
            &json!({"projectId":id(7),"status":null,"tags":["b","a"]})
        )
        .unwrap(),
        json!({"projectId":id(7),"status":null,"tags":["b","a"]})
    );
    let mut unknown: LoadIntent = serde_json::from_value(intent(1, Value::Null)).unwrap();
    unknown.version = 2;
    assert!(unknown.normalize(&schema).is_err());
    let invalid_state: LoadIntent =
        serde_json::from_value(intent(1, json!({"state":1e300}))).unwrap();
    assert!(invalid_state.normalize(&schema).is_err());
}

#[test]
fn load_requests_declare_every_output_read_contract() {
    let schema = schema();
    let load = schema.load("ProjectTodos", 1).unwrap();
    let models = |value: Value| serde_json::from_value(value).unwrap();
    assert!(validate_load_models(&schema, load, &models(json!({"Todo":1,"Note":1}))).is_ok());
    assert!(validate_load_models(&schema, load, &models(json!({"Todo":1}))).is_err());
    assert!(validate_load_models(&schema, load, &models(json!({"Todo":2,"Note":1}))).is_err());
}

#[test]
fn request_envelopes_are_structural_and_bounded() {
    let full: Vec<Value> = (1..=8).map(|n| intent(n, Value::Null)).collect();
    assert_eq!(request(full.clone()).loads.len(), 8);
    let decode = |items: Vec<Value>| {
        LoadBatchRequest::decode_envelope(json!({ "loads": items }).to_string().as_bytes())
    };
    let mut nine = full.clone();
    nine.push(intent(9, Value::Null));
    assert!(decode(nine).is_err());
    assert!(decode(vec![]).is_err());
    let mut same_load = intent(2, Value::Null);
    same_load["loadId"] = json!(id(1));
    assert!(decode(vec![intent(1, Value::Null), same_load]).is_err());
    let mut same_call = intent(2, Value::Null);
    same_call["callId"] = json!(id(101).to_uppercase());
    assert!(decode(vec![intent(1, Value::Null), same_call]).is_err());
    let mut bad_id = intent(1, Value::Null);
    bad_id["loadId"] = json!("not-a-uuid");
    assert!(decode(vec![bad_id]).is_err());
    let mut extra = json!({"loads":[intent(1, Value::Null)]});
    extra["mode"] = json!("x");
    assert!(LoadBatchRequest::decode_envelope(extra.to_string().as_bytes()).is_err());
    let mut member = intent(1, Value::Null);
    member["once"] = json!(true);
    assert!(decode(vec![member]).is_err());
    let mut huge = intent(1, Value::Null);
    huge["args"]["projectId"] = json!("x".repeat(limits::LOAD_REQUEST_BYTES));
    assert!(decode(vec![huge]).is_err());
    // Unknown names, versions and args are item rejections, not envelope failures.
    let mut unknown = intent(2, Value::Null);
    unknown["name"] = json!("Missing");
    let mut invalid_args = intent(3, Value::Null);
    invalid_args["args"] = json!({"projectId":7});
    let batch = decode(vec![intent(1, Value::Null), unknown, invalid_args]).unwrap();
    let schema = schema();
    let results: Vec<bool> = batch
        .loads
        .into_iter()
        .map(|item| item.normalize(&schema).is_ok())
        .collect();
    assert_eq!(results, vec![true, false, false]);
}

#[test]
fn responses_correlate_exactly_before_any_item_is_used() {
    let batch = request(vec![intent(1, Value::Null), intent(2, Value::Null)]);
    let ok = |n| succeeded(n, json!({"todos":[],"notes":[]}), Value::Null, json!([]));
    // Order is transport grouping only.
    let decoded = LoadBatchResponse::decode(&response(vec![ok(2), ok(1)]), &batch).unwrap();
    assert_eq!(decoded.loads.len(), 2);
    assert_eq!(decoded.loads[0].load_id, id(2));
    let mut swapped = ok(1);
    swapped["callId"] = json!(id(102));
    let mut unrequested = ok(2);
    unrequested["loadId"] = json!(id(9));
    for items in [
        vec![ok(1)],
        vec![ok(1), ok(2), ok(1)],
        vec![ok(1), ok(1)],
        vec![swapped, ok(2)],
        vec![ok(1), unrequested],
    ] {
        assert!(
            LoadBatchResponse::decode(&response(items.clone()), &batch).is_err(),
            "{items:?}"
        );
    }
    let failed = json!({"loadId":id(2),"callId":id(102),"outcome":{"status":"failed","error":{"code":"load.invalid_continuation","message":"bad state"}},"records":[]});
    let retryable = json!({"loadId":id(2),"callId":id(102),"outcome":{"status":"retryable","error":{"code":"storage.unavailable","message":""}},"records":[]});
    for sibling in [failed.clone(), retryable] {
        let decoded = LoadBatchResponse::decode(&response(vec![ok(1), sibling]), &batch).unwrap();
        assert!(matches!(
            decoded.loads[1].outcome,
            LoadOutcome::Failed { .. } | LoadOutcome::Retryable { .. }
        ));
    }
    // Outcome shape is envelope-level.
    let mut with_records = failed.clone();
    with_records["records"] = json!([record("Todo", 1)]);
    let mut bad_code = failed.clone();
    bad_code["outcome"]["error"]["code"] = json!("Not A Code");
    let mut long_message = failed.clone();
    long_message["outcome"]["error"]["message"] = json!("m".repeat(1025));
    let mut unknown_status = failed.clone();
    unknown_status["outcome"]["status"] = json!("pending");
    let mut no_next = ok(2);
    no_next["outcome"].as_object_mut().unwrap().remove("next");
    let mut data_list = ok(2);
    data_list["outcome"]["data"] = json!([]);
    let mut bad_stamp = ok(2);
    bad_stamp["records"] =
        json!([{"model":"Todo","identity":{"id":id(1)},"stamp":0,"state":{"title":"t"}}]);
    let mut extra_member = ok(2);
    extra_member["cursor"] = json!(1);
    for bad in [
        with_records,
        bad_code,
        long_message,
        unknown_status,
        no_next,
        data_list,
        bad_stamp,
        extra_member,
    ] {
        assert!(
            LoadBatchResponse::decode(&response(vec![ok(1), bad.clone()]), &batch).is_err(),
            "{bad}"
        );
    }
    let mut padded = ok(2);
    padded["outcome"]["next"] = json!({"state":"x".repeat(limits::LOAD_RESPONSE_BYTES)});
    assert!(LoadBatchResponse::decode(&response(vec![ok(1), padded]), &batch).is_err());
}

#[test]
fn pages_carry_declared_identity_lists_and_matching_authority() {
    let schema = schema();
    let batch = request(vec![intent(1, Value::Null)]);
    let item = batch.loads[0].clone().normalize(&schema).unwrap();
    let page = |data: Value, next: Value, records: Value| {
        let bytes = response(vec![succeeded(1, data, next, records)]);
        LoadBatchResponse::decode(&bytes, &batch)
            .unwrap()
            .loads
            .remove(0)
    };
    // Identities are normalized; a repeated identity needs one record.
    let upper = json!({"id":id(1).to_uppercase()});
    let good = page(
        json!({"todos":[upper.clone(), upper],"notes":[{"id":id(2)}]}),
        json!({"state":{"after":2}}),
        json!([record("Todo", 1), record("Note", 2)]),
    )
    .normalize(&schema, &item)
    .unwrap();
    let LoadOutcome::Succeeded { data, next } = &good.outcome else {
        panic!("expected success");
    };
    assert_eq!(
        data,
        &json!({"todos":[{"id":id(1)},{"id":id(1)}],"notes":[{"id":id(2)}]})
    );
    assert_eq!(
        next,
        &Some(Continuation {
            state: json!({"after":2})
        })
    );
    // An empty non-final page is valid progress, not completion.
    let empty = page(
        json!({"todos":[],"notes":[]}),
        json!({"state":null}),
        json!([]),
    )
    .normalize(&schema, &item)
    .unwrap();
    assert!(matches!(
        empty.outcome,
        LoadOutcome::Succeeded {
            next: Some(Continuation { state: Value::Null }),
            ..
        }
    ));
    for (data, records) in [
        (json!({"todos":[]}), json!([])),
        (json!({"todos":[],"notes":[],"extra":[]}), json!([])),
        (json!({"todos":{"id":id(1)},"notes":[]}), json!([])),
        (
            json!({"todos":[{"id":id(1),"title":"t"}],"notes":[]}),
            json!([record("Todo", 1)]),
        ),
        (json!({"todos":[{"id":"x"}],"notes":[]}), json!([])),
        (json!({"todos":[{"id":id(1)}],"notes":[]}), json!([])),
        (json!({"todos":[],"notes":[]}), json!([record("Todo", 1)])),
        (
            json!({"todos":[{"id":id(1)}],"notes":[]}),
            json!([record("Note", 1)]),
        ),
        (
            json!({"todos":[{"id":id(1)}],"notes":[]}),
            json!([record("Todo", 1), record("Todo", 1)]),
        ),
        (
            json!({"todos":[{"id":id(1)}],"notes":[]}),
            json!([{"model":"Todo","identity":{"id":id(1)},"stamp":3,"state":null}]),
        ),
        (
            json!({"todos":[{"id":id(1)}],"notes":[]}),
            json!([{"model":"Todo","identity":{"id":id(1)},"stamp":3,"error":"loader.failed"}]),
        ),
    ] {
        let malformed = page(data.clone(), Value::Null, records.clone());
        assert!(
            malformed.normalize(&schema, &item).is_err(),
            "{data} {records}"
        );
    }
    let bad_next = page(
        json!({"todos":[],"notes":[]}),
        json!({"state":9_007_199_254_740_992_u64}),
        json!([]),
    );
    assert!(bad_next.normalize(&schema, &item).is_err());
    // At most 1,000 identity entries across every declared list.
    let identities = |n: u64| (0..n).map(|i| json!({"id":id(i)})).collect::<Vec<_>>();
    let records = |n: u64| (0..n).map(|i| record("Todo", i)).collect::<Vec<_>>();
    let at_bound = page(
        json!({"todos":identities(999),"notes":[{"id":id(5000)}]}),
        Value::Null,
        json!(
            records(999)
                .into_iter()
                .chain([record("Note", 5000)])
                .collect::<Vec<_>>()
        ),
    );
    assert!(at_bound.normalize(&schema, &item).is_ok());
    let over = page(
        json!({"todos":identities(1001),"notes":[]}),
        Value::Null,
        json!(records(1001)),
    );
    assert!(over.normalize(&schema, &item).is_err());
    // An oversized page fails its own item, not the envelope.
    let mut big = record("Todo", 1);
    big["state"]["title"] = json!("x".repeat(limits::LOAD_PAGE_BYTES));
    let oversized = page(
        json!({"todos":[{"id":id(1)}],"notes":[]}),
        Value::Null,
        json!([big]),
    );
    assert!(oversized.normalize(&schema, &item).is_err());
    // A page answers only its own frozen intent.
    let other: LoadIntent = serde_json::from_value(intent(2, Value::Null)).unwrap();
    assert!(!good.answers(&other));
    assert!(good.answers(&item));
    assert!(good.normalize(&schema, &other).is_err());
}

#[test]
fn response_encoding_round_trips_normalized_pages() {
    let schema = schema();
    let batch = request(vec![intent(1, Value::Null), intent(2, Value::Null)]);
    let item = batch.loads[0].clone().normalize(&schema).unwrap();
    let bytes = response(vec![
        succeeded(
            1,
            json!({"todos":[{"id":id(1)}],"notes":[]}),
            json!({"state":{"k":[1.0, null]}}),
            json!([record("Todo", 1)]),
        ),
        json!({"loadId":id(2),"callId":id(102),"outcome":{"status":"failed","error":{"code":"handler.failed","message":"no"}},"records":[]}),
    ]);
    let mut decoded = LoadBatchResponse::decode(&bytes, &batch).unwrap();
    decoded.loads[0] = decoded.loads[0].clone().normalize(&schema, &item).unwrap();
    let encoded = decoded.encode().unwrap();
    let again = LoadBatchResponse::decode(&encoded, &batch).unwrap();
    assert_eq!(again, decoded);
    assert!(matches!(
        &again.loads[0].outcome,
        LoadOutcome::Succeeded { next: Some(Continuation { state }), .. } if state == &json!({"k":[1, null]})
    ));
}

#[test]
fn loads_never_route_as_actions() {
    let schema = schema();
    assert!(schema.load("ProjectTodos", 1).is_ok());
    assert!(schema.action("ProjectTodos", 1).is_err());
    let call: ActionIntent = serde_json::from_value(json!({
        "callId":id(1),"name":"ProjectTodos","version":1,
        "args":{"projectId":id(7),"status":null,"tags":[]}
    }))
    .unwrap();
    assert!(call.clone().normalize(&schema).is_err());
    let direct = json!({"call":call,"models":{"Todo":1,"Note":1}}).to_string();
    assert!(DirectActionRequest::decode(direct.as_bytes(), &schema).is_err());
    assert!(schema.load("Send", 1).is_err());
}

/// One change to the fixture Load descriptor.
type DescriptorEdit = fn(&mut Value);

#[test]
fn malformed_load_descriptors_are_refused() {
    let edit = |change: &dyn Fn(&mut Value)| {
        let mut raw = raw_schema();
        change(&mut raw["loads"][0]);
        Schema::from_value(raw)
    };
    assert!(edit(&|_| {}).is_ok());
    let cases: Vec<(&str, DescriptorEdit)> = vec![
        ("kind member", |l| l["kind"] = json!("query")),
        ("sequence member", |l| l["sequence"] = json!(null)),
        ("once member", |l| l["once"] = json!(true)),
        ("version zero", |l| l["version"] = json!(0)),
        ("empty name", |l| l["name"] = json!("")),
        ("no outputs", |l| l["outputs"] = json!([])),
        ("model operand", |l| {
            l["inputs"]
                    .as_array_mut()
                    .unwrap()
                    .push(json!({"kind":"model","name":"todo","model":"Todo","operation":"create","cardinality":"single"}))
        }),
        (
            "scalar output",
            |l| l["outputs"][0] = json!({"name":"count","kind":"value","type":{"kind":"scalar","name":"int"},"cardinality":"list","source":"handlerValue"}),
        ),
        ("single output", |l| {
            l["outputs"][0]["cardinality"] = json!("single")
        }),
        ("optional output", |l| {
            l["outputs"][0]["cardinality"] = json!("optional")
        }),
        ("input identity output", |l| {
            l["outputs"][0]["source"] = json!({"inputIdentity":"todo"})
        }),
        ("unknown read contract", |l| {
            l["outputs"][0]["modelReadVersion"] = json!(2)
        }),
        ("missing read contract", |l| {
            l["outputs"][0]
                .as_object_mut()
                .unwrap()
                .remove("modelReadVersion");
        }),
        ("wrong handler type", |l| {
            l["outputs"][0]["handlerType"]["model"] = json!("Note")
        }),
        ("duplicate output", |l| {
            l["outputs"][1]["name"] = json!("todos")
        }),
        ("duplicate input", |l| {
            l["inputs"][1]["name"] = json!("projectId")
        }),
        ("nullable list input", |l| {
            l["inputs"][2]["nullable"] = json!(true)
        }),
        ("enum outside snapshot", |l| l["input"]["enums"] = json!([])),
        ("snapshot operand model", |l| {
            l["input"]["models"] = json!([model("Todo", json!([]))])
        }),
        ("reserved get", |l| l["name"] = json!("Get")),
        ("reserved list", |l| l["name"] = json!("list")),
        ("reserved invalidate", |l| l["name"] = json!("Invalidate")),
        ("action name", |l| l["name"] = json!("Send")),
        ("normalized action name", |l| l["name"] = json!("send")),
    ];
    for (label, change) in cases {
        assert!(edit(&change).is_err(), "{label}");
    }
    let mut duplicate = raw_schema();
    duplicate["loads"]
        .as_array_mut()
        .unwrap()
        .push(load_descriptor());
    assert!(Schema::from_value(duplicate).is_err());
    let mut spelled = raw_schema();
    let mut other = load_descriptor();
    other["name"] = json!("projectTodos");
    spelled["loads"].as_array_mut().unwrap().push(other);
    assert!(Schema::from_value(spelled).is_err());
    let mut versions = raw_schema();
    let mut v2 = load_descriptor();
    v2["version"] = json!(2);
    versions["loads"].as_array_mut().unwrap().push(v2);
    assert!(
        Schema::from_value(versions)
            .unwrap()
            .load("ProjectTodos", 2)
            .is_ok()
    );
}

#[test]
fn schemas_without_loads_serialize_unchanged() {
    let mut raw = raw_schema();
    raw.as_object_mut().unwrap().remove("loads");
    let without = Schema::from_value(raw).unwrap();
    assert!(without.loads.is_empty());
    assert!(
        serde_json::to_value(&without)
            .unwrap()
            .get("loads")
            .is_none()
    );
    let with = serde_json::to_value(schema()).unwrap();
    assert_eq!(with["loads"][0]["name"], "ProjectTodos");
}
