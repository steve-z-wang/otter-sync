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

/// An error whose message names `fragment`.
fn fails<T: std::fmt::Debug, E: std::fmt::Display>(
    result: std::result::Result<T, E>,
    fragment: &str,
) {
    let error = result.unwrap_err().to_string();
    assert!(
        error.contains(fragment),
        "expected `{fragment}`, got `{error}`"
    );
}
/// A per-item refusal of `kind` whose message names `fragment`.
fn item_fails<T: std::fmt::Debug>(
    result: std::result::Result<T, LoadItemError>,
    kind: LoadItemErrorKind,
    fragment: &str,
) {
    let error = result.unwrap_err();
    assert_eq!(error.kind, kind, "{error}");
    assert!(
        error.message.contains(fragment),
        "expected `{fragment}`, got `{error}`"
    );
}
fn only_page(bytes: &[u8], batch: &LoadBatchRequest) -> LoadPageResponse {
    LoadBatchResponse::decode(bytes, batch)
        .unwrap()
        .remove(0)
        .page
        .unwrap()
}

#[test]
fn limits_are_the_specified_initial_values() {
    assert_eq!(limits::LOAD_BATCH_ITEMS, 8);
    assert_eq!(limits::LOAD_REQUEST_BYTES, 1024 * 1024);
    assert_eq!(limits::LOAD_PAGE_BYTES, 1024 * 1024);
    // Ruling R3: eight maximal pages plus a 64 KiB envelope allowance.
    assert_eq!(limits::LOAD_RESPONSE_BYTES, 8 * 1024 * 1024 + 64 * 1024);
    assert_eq!(limits::LOAD_PAGE_IDENTITIES, 1000);
    assert_eq!(limits::LOAD_STATE_BYTES, 64 * 1024);
    assert_eq!(limits::LOAD_STATE_DEPTH, 64);
    assert_eq!(limits::LOAD_ERROR_MESSAGE_BYTES, 1024);
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
    // The wrapper is exact: no missing state, no extra member, no other type.
    for (bad, fragment) in [
        (json!({}), "continuation state missing"),
        (json!({"state":1,"cursor":2}), "unknown continuation member"),
        (json!(1), "invalid type"),
        (json!([]), "invalid type"),
    ] {
        fails(
            serde_json::from_value::<LoadIntent>(intent(1, bad)),
            fragment,
        );
    }
    let mut missing = intent(1, Value::Null);
    missing.as_object_mut().unwrap().remove("continuation");
    fails(
        serde_json::from_value::<LoadIntent>(missing),
        "missing field `continuation`",
    );
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
    fails(
        Continuation::new(nest(limits::LOAD_STATE_DEPTH + 1)),
        "exceeds nesting depth",
    );
    // Canonical bytes of the state itself: `"` + text + `"`.
    let fits = "a".repeat(limits::LOAD_STATE_BYTES - 2);
    assert!(Continuation::new(json!(fits)).is_ok());
    fails(
        Continuation::new(json!(format!("{fits}a"))),
        "exceeds byte limit",
    );
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
        fails(
            Continuation::new(unsafe_number),
            "integer outside safe range",
        );
    }
    assert!(Continuation::new(json!(1e-300)).is_ok());
    // A deserialized wrapper keeps its bytes until normalized per item.
    let raw: Continuation = serde_json::from_value(json!({"state":1e300})).unwrap();
    fails(raw.normalized(), "integer outside safe range");
}

#[test]
fn intents_normalize_ids_args_and_continuation_against_the_load() {
    use LoadItemErrorKind::*;
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
    for (bad, fragment) in [
        (
            json!({"projectId":id(7),"tags":[]}),
            "required input missing",
        ),
        (
            json!({"projectId":id(7),"status":"closed","tags":[]}),
            "invalid enum value",
        ),
        (
            json!({"projectId":id(7),"status":null,"tags":null}),
            "tags is not nullable",
        ),
        (
            json!({"projectId":id(7),"status":null,"tags":[],"once":true}),
            "undeclared input",
        ),
        (json!([]), "Load args must be an object"),
    ] {
        fails(normalize_load_args(&schema, load, &bad), fragment);
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
    // Each step is classified, so a caller never matches message text.
    let with = |edit: &dyn Fn(&mut Value)| {
        let mut raw = intent(1, Value::Null);
        edit(&mut raw);
        serde_json::from_value::<LoadIntent>(raw)
            .unwrap()
            .normalize(&schema)
    };
    item_fails(with(&|r| r["loadId"] = json!("x")), InvalidId, "loadId");
    item_fails(with(&|r| r["callId"] = json!("x")), InvalidId, "callId");
    item_fails(
        with(&|r| r["version"] = json!(2)),
        UnsupportedLoad,
        "unknown Load ProjectTodos v2",
    );
    item_fails(
        with(&|r| r["name"] = json!("Send")),
        UnsupportedLoad,
        "unknown Load Send",
    );
    item_fails(
        with(&|r| r["args"] = json!({"projectId":7})),
        InvalidArgs,
        "expected UUID",
    );
    item_fails(
        with(&|r| r["continuation"] = json!({"state":1e300})),
        InvalidContinuation,
        "integer outside safe range",
    );
}

#[test]
fn load_requests_declare_every_output_read_contract() {
    let schema = schema();
    let load = schema.load("ProjectTodos", 1).unwrap();
    let models = |value: Value| serde_json::from_value(value).unwrap();
    assert!(validate_load_models(&schema, load, &models(json!({"Todo":1,"Note":1}))).is_ok());
    fails(
        validate_load_models(&schema, load, &models(json!({"Todo":1}))),
        "Load needs local Model read contract Note",
    );
    fails(
        validate_load_models(&schema, load, &models(json!({"Todo":2,"Note":1}))),
        "unsupported local Model read version",
    );
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
    fails(decode(nine), "Load batch must contain 1..8 items");
    fails(decode(vec![]), "Load batch must contain 1..8 items");
    let mut same_load = intent(2, Value::Null);
    same_load["loadId"] = json!(id(1));
    fails(
        decode(vec![intent(1, Value::Null), same_load]),
        "duplicate Load loadId or callId",
    );
    let mut same_call = intent(2, Value::Null);
    same_call["callId"] = json!(id(101).to_uppercase());
    fails(
        decode(vec![intent(1, Value::Null), same_call]),
        "duplicate Load loadId or callId",
    );
    let mut bad_id = intent(1, Value::Null);
    bad_id["loadId"] = json!("not-a-uuid");
    fails(decode(vec![bad_id]), "invalid loadId UUID");
    let mut extra = json!({"loads":[intent(1, Value::Null)]});
    extra["mode"] = json!("x");
    fails(
        LoadBatchRequest::decode_envelope(extra.to_string().as_bytes()),
        "must be exactly",
    );
    let mut member = intent(1, Value::Null);
    member["once"] = json!(true);
    fails(decode(vec![member]), "unknown field `once`");
    let mut huge = intent(1, Value::Null);
    huge["args"]["projectId"] = json!("x".repeat(limits::LOAD_REQUEST_BYTES));
    fails(decode(vec![huge]), "Load request exceeds byte limit");
    // Unknown names, versions and args are item rejections, not envelope failures.
    let mut unknown = intent(2, Value::Null);
    unknown["name"] = json!("Missing");
    let mut invalid_args = intent(3, Value::Null);
    invalid_args["args"] = json!({"projectId":7});
    let batch = decode(vec![intent(1, Value::Null), unknown, invalid_args]).unwrap();
    let schema = schema();
    let kinds: Vec<Option<LoadItemErrorKind>> = batch
        .loads
        .into_iter()
        .map(|item| item.normalize(&schema).err().map(|e| e.kind))
        .collect();
    assert_eq!(
        kinds,
        vec![
            None,
            Some(LoadItemErrorKind::UnsupportedLoad),
            Some(LoadItemErrorKind::InvalidArgs)
        ]
    );
}

#[test]
fn only_correlation_structure_rejects_a_response_envelope() {
    let batch = request(vec![intent(1, Value::Null), intent(2, Value::Null)]);
    let ok = |n| succeeded(n, json!({"todos":[],"notes":[]}), Value::Null, json!([]));
    // Order is transport grouping only.
    let replies = LoadBatchResponse::decode(&response(vec![ok(2), ok(1)]), &batch).unwrap();
    assert_eq!(replies.len(), 2);
    assert_eq!(replies[0].load_id, id(2));
    assert!(replies.iter().all(|r| r.page.is_ok()));
    let mut swapped = ok(1);
    swapped["callId"] = json!(id(102));
    let mut unrequested = ok(2);
    unrequested["loadId"] = json!(id(9));
    let mut bad_id = ok(2);
    bad_id["callId"] = json!("x");
    let mut no_outcome = ok(2);
    no_outcome.as_object_mut().unwrap().remove("outcome");
    let mut records_object = ok(2);
    records_object["records"] = json!({});
    let mut outcome_list = ok(2);
    outcome_list["outcome"] = json!([]);
    for (items, fragment) in [
        (vec![ok(1)], "does not answer every requested page"),
        (vec![ok(1), ok(2), ok(1)], "duplicate Load page correlation"),
        (vec![ok(1), ok(1)], "duplicate Load page correlation"),
        (vec![swapped, ok(2)], "unrequested page"),
        (vec![ok(1), unrequested], "unrequested page"),
        (vec![ok(1), bad_id], "invalid callId UUID"),
        (
            vec![ok(1), no_outcome],
            "object outcome and a records array",
        ),
        (
            vec![ok(1), records_object],
            "object outcome and a records array",
        ),
        (
            vec![ok(1), outcome_list],
            "object outcome and a records array",
        ),
        (vec![ok(1), json!([])], "items must be objects"),
    ] {
        fails(
            LoadBatchResponse::decode(&response(items), &batch),
            fragment,
        );
    }
    let mut top = json!({"loads":[ok(1), ok(2)]});
    top["cursor"] = json!(1);
    fails(
        LoadBatchResponse::decode(top.to_string().as_bytes(), &batch),
        "must be exactly",
    );
    let mut padded = ok(2);
    padded["outcome"]["next"] = json!({"state":"x".repeat(limits::LOAD_RESPONSE_BYTES)});
    fails(
        LoadBatchResponse::decode(&response(vec![ok(1), padded]), &batch),
        "Load response exceeds byte limit",
    );
}

#[test]
fn a_malformed_page_shape_fails_only_its_own_item() {
    let batch = request(vec![intent(1, Value::Null), intent(2, Value::Null)]);
    let ok = |n| succeeded(n, json!({"todos":[],"notes":[]}), Value::Null, json!([]));
    let failed = json!({"loadId":id(2),"callId":id(102),"outcome":{"status":"failed","error":{"code":"load.invalid_continuation","message":"bad state"}},"records":[]});
    let retryable = json!({"loadId":id(2),"callId":id(102),"outcome":{"status":"retryable","error":{"code":"storage.unavailable","message":""}},"records":[]});
    for sibling in [failed.clone(), retryable] {
        let replies = LoadBatchResponse::decode(&response(vec![ok(1), sibling]), &batch).unwrap();
        assert!(matches!(
            replies[1].page.as_ref().unwrap().outcome,
            LoadOutcome::Failed { .. } | LoadOutcome::Retryable { .. }
        ));
    }
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
    for (bad, fragment) in [
        (with_records, "an unsuccessful Load page carries no records"),
        (bad_code, "Load error code is not a machine code"),
        (long_message, "Load error message exceeds byte limit"),
        (unknown_status, "unknown variant `pending`"),
        (no_next, "missing field `next`"),
        (data_list, "Load page data must be an object"),
        (bad_stamp, "record stamp must be a positive counter"),
        (extra_member, "unknown field `cursor`"),
    ] {
        let replies =
            LoadBatchResponse::decode(&response(vec![ok(1), bad.clone()]), &batch).unwrap();
        let sibling = replies.iter().find(|r| r.load_id == id(1)).unwrap();
        assert!(sibling.page.is_ok(), "{bad}");
        let malformed = replies.iter().find(|r| r.load_id == id(2)).unwrap();
        assert_eq!(malformed.call_id, id(102));
        item_fails(
            malformed.page.clone(),
            LoadItemErrorKind::MalformedPage,
            fragment,
        );
    }
}

#[test]
fn pages_carry_declared_identity_lists_and_matching_authority() {
    use LoadItemErrorKind::*;
    let schema = schema();
    let batch = request(vec![intent(1, Value::Null)]);
    let item = batch.loads[0].clone().normalize(&schema).unwrap();
    let page = |data: Value, next: Value, records: Value| {
        only_page(&response(vec![succeeded(1, data, next, records)]), &batch)
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
    let todo = |n| json!({"todos":[{"id":id(n)}],"notes":[]});
    for (data, records, fragment) in [
        (json!({"todos":[]}), json!([]), "exactly declared outputs"),
        (
            json!({"todos":[],"notes":[],"extra":[]}),
            json!([]),
            "exactly declared outputs",
        ),
        (
            json!({"todos":{"id":id(1)},"notes":[]}),
            json!([]),
            "Load output todos must be a list",
        ),
        (
            json!({"todos":[{"id":id(1),"title":"t"}],"notes":[]}),
            json!([record("Todo", 1)]),
            "exactly identity fields",
        ),
        (
            json!({"todos":[{"id":"x"}],"notes":[]}),
            json!([]),
            "invalid UUID",
        ),
        (todo(1), json!([]), "identity has no record"),
        (
            json!({"todos":[],"notes":[]}),
            json!([record("Todo", 1)]),
            "does not match one enumerated identity",
        ),
        (
            todo(1),
            json!([record("Note", 1)]),
            "does not match one enumerated identity",
        ),
        (
            todo(1),
            json!([record("Todo", 1), record("Todo", 1)]),
            "does not match one enumerated identity",
        ),
        (
            todo(1),
            json!([{"model":"Todo","identity":{"id":id(1)},"stamp":3,"state":null}]),
            "must carry record state",
        ),
        (
            todo(1),
            json!([{"model":"Todo","identity":{"id":id(1)},"stamp":3,"error":"loader.failed"}]),
            "must carry record state",
        ),
    ] {
        let malformed = page(data.clone(), Value::Null, records.clone());
        item_fails(malformed.normalize(&schema, &item), MalformedPage, fragment);
    }
    let bad_next = page(
        json!({"todos":[],"notes":[]}),
        json!({"state":9_007_199_254_740_992_u64}),
        json!([]),
    );
    item_fails(
        bad_next.normalize(&schema, &item),
        InvalidContinuation,
        "integer outside safe range",
    );
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
    item_fails(
        over.normalize(&schema, &item),
        PageTooLarge,
        "exceeds 1000 identities",
    );
    // An oversized page fails its own item, not the envelope.
    let mut big = record("Todo", 1);
    big["state"]["title"] = json!("x".repeat(limits::LOAD_PAGE_BYTES));
    let oversized = page(todo(1), Value::Null, json!([big]));
    item_fails(
        oversized.normalize(&schema, &item),
        PageTooLarge,
        "Load page exceeds byte limit",
    );
    // A page answers only its own frozen intent.
    let other: LoadIntent = serde_json::from_value(intent(2, Value::Null)).unwrap();
    assert!(!good.answers(&other));
    assert!(good.answers(&item));
    item_fails(
        good.normalize(&schema, &other),
        MalformedPage,
        "answers another request",
    );
}

/// A succeeded page for item `n` whose canonical encoding is exactly `bytes`.
fn page_of_size(n: u64, bytes: usize) -> Value {
    let mut page = succeeded(
        n,
        json!({"todos":[{"id":id(n)}],"notes":[]}),
        Value::Null,
        json!([record("Todo", n)]),
    );
    let base = canonical_json(&page).unwrap().len();
    page["records"][0]["state"]["title"] = json!("x".repeat(bytes - base + 1));
    page
}

#[test]
fn eight_maximal_pages_fit_one_response() {
    let schema = schema();
    let batch = request((1..=8).map(|n| intent(n, Value::Null)).collect());
    let pages: Vec<LoadPageResponse> = (1..=8)
        .map(|n| LoadPageResponse::decode_item(&page_of_size(n, limits::LOAD_PAGE_BYTES)).unwrap())
        .collect();
    let bytes = LoadBatchResponse { loads: pages }.encode().unwrap();
    assert!(bytes.len() > 8 * limits::LOAD_PAGE_BYTES);
    assert!(bytes.len() <= limits::LOAD_RESPONSE_BYTES);
    for reply in LoadBatchResponse::decode(&bytes, &batch).unwrap() {
        let intent = batch
            .loads
            .iter()
            .find(|i| i.load_id == reply.load_id)
            .unwrap();
        let page = reply.page.unwrap();
        let encoded = canonical_json(&serde_json::to_value(&page).unwrap()).unwrap();
        assert_eq!(encoded.len(), limits::LOAD_PAGE_BYTES);
        let item = intent.clone().normalize(&schema).unwrap();
        assert!(page.normalize(&schema, &item).is_ok());
    }
    // One byte more is a page failure for that item alone.
    let item = batch.loads[0].clone().normalize(&schema).unwrap();
    let over =
        LoadPageResponse::decode_item(&page_of_size(1, limits::LOAD_PAGE_BYTES + 1)).unwrap();
    item_fails(
        over.normalize(&schema, &item),
        LoadItemErrorKind::PageTooLarge,
        "Load page exceeds byte limit",
    );
}

#[test]
fn server_encoding_bounds_item_errors_instead_of_refusing_the_batch() {
    let batch = request(vec![intent(1, Value::Null), intent(2, Value::Null)]);
    let failed = |n: u64, code: &str, message: String| LoadPageResponse {
        load_id: id(n),
        call_id: id(100 + n),
        outcome: LoadOutcome::Failed {
            error: LoadError {
                code: code.into(),
                message,
            },
        },
        records: vec![],
    };
    // A 1,023-byte prefix plus a 2-byte character: cut on the boundary.
    let message = format!("{}\u{e9}tail", "m".repeat(1023));
    let mut noisy = failed(1, "Handler Blew Up", message);
    noisy.records = vec![serde_json::from_value(record("Todo", 1)).unwrap()];
    let response = LoadBatchResponse {
        loads: vec![noisy, failed(2, "handler.failed", "fine".into())],
    };
    let bytes = response.encode().unwrap();
    let replies = LoadBatchResponse::decode(&bytes, &batch).unwrap();
    let page = replies[0].page.clone().unwrap();
    let LoadOutcome::Failed { error } = &page.outcome else {
        panic!("expected failure");
    };
    assert_eq!(error.code, "internal");
    assert_eq!(error.message, "m".repeat(1023));
    assert!(page.records.is_empty());
    let LoadOutcome::Failed { error } = &replies[1].page.as_ref().unwrap().outcome else {
        panic!("expected failure");
    };
    assert_eq!(error, &LoadError::bounded("handler.failed", "fine"));
    // Correlation structure still refuses the batch.
    let duplicate = LoadBatchResponse {
        loads: vec![failed(1, "a", String::new()), failed(1, "a", String::new())],
    };
    fails(duplicate.encode(), "duplicate Load page correlation");
    let mut upper = failed(1, "a", String::new());
    upper.load_id = upper.load_id.to_uppercase();
    fails(
        LoadBatchResponse { loads: vec![upper] }.encode(),
        "loadId must be canonical",
    );
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
    let replies = LoadBatchResponse::decode(&bytes, &batch).unwrap();
    let pages = vec![
        replies[0]
            .page
            .clone()
            .unwrap()
            .normalize(&schema, &item)
            .unwrap(),
        replies[1].page.clone().unwrap(),
    ];
    let encoded = LoadBatchResponse {
        loads: pages.clone(),
    }
    .encode()
    .unwrap();
    let again: Vec<LoadPageResponse> = LoadBatchResponse::decode(&encoded, &batch)
        .unwrap()
        .into_iter()
        .map(|reply| reply.page.unwrap())
        .collect();
    assert_eq!(again, pages);
    assert!(matches!(
        &again[0].outcome,
        LoadOutcome::Succeeded { next: Some(Continuation { state }), .. } if state == &json!({"k":[1, null]})
    ));
}

#[test]
fn loads_never_route_as_actions() {
    let schema = schema();
    assert!(schema.load("ProjectTodos", 1).is_ok());
    fails(
        schema.action("ProjectTodos", 1),
        "unknown Action ProjectTodos v1",
    );
    let call: ActionIntent = serde_json::from_value(json!({
        "callId":id(1),"name":"ProjectTodos","version":1,
        "args":{"projectId":id(7),"status":null,"tags":[]}
    }))
    .unwrap();
    fails(
        call.clone().normalize(&schema),
        "unknown Action ProjectTodos v1",
    );
    let direct = json!({"call":call,"models":{"Todo":1,"Note":1}}).to_string();
    fails(
        DirectActionRequest::decode(direct.as_bytes(), &schema),
        "unknown Action ProjectTodos v1",
    );
    fails(schema.load("Send", 1), "unknown Load Send v1");
}

/// One change to the fixture Load descriptor, and the refusal it must cause.
type DescriptorEdit = (&'static str, fn(&mut Value), &'static str);

#[test]
fn malformed_load_descriptors_are_refused() {
    let edit = |change: &dyn Fn(&mut Value)| {
        let mut raw = raw_schema();
        change(&mut raw["loads"][0]);
        Schema::from_value(raw)
    };
    assert!(edit(&|_| {}).is_ok());
    let cases: Vec<DescriptorEdit> = vec![
        (
            "kind member",
            |l| l["kind"] = json!("query"),
            "unknown field `kind`",
        ),
        (
            "sequence member",
            |l| l["sequence"] = json!(null),
            "unknown field `sequence`",
        ),
        (
            "once member",
            |l| l["once"] = json!(true),
            "unknown field `once`",
        ),
        (
            "version zero",
            |l| l["version"] = json!(0),
            "invalid or duplicate Load descriptor",
        ),
        (
            "empty name",
            |l| l["name"] = json!(""),
            "invalid or duplicate Load descriptor",
        ),
        (
            "no outputs",
            |l| l["outputs"] = json!([]),
            "declares no output",
        ),
        (
            "model operand",
            |l| {
                l["inputs"].as_array_mut().unwrap().push(
                    json!({"kind":"model","name":"todo","model":"Todo","operation":"create","cardinality":"single"}),
                )
            },
            "cannot take a Model operand",
        ),
        (
            "scalar output",
            |l| l["outputs"][0] = json!({"name":"count","kind":"value","type":{"kind":"scalar","name":"int"},"cardinality":"list","source":"handlerValue"}),
            "output count must be a list of Model identities",
        ),
        (
            "single output",
            |l| l["outputs"][0]["cardinality"] = json!("single"),
            "output todos must be a list of Model identities",
        ),
        (
            "optional output",
            |l| l["outputs"][0]["cardinality"] = json!("optional"),
            "output todos must be a list of Model identities",
        ),
        (
            "input identity output",
            |l| l["outputs"][0]["source"] = json!({"inputIdentity":"todo"}),
            "output todos must be a list of Model identities",
        ),
        (
            "unknown read contract",
            |l| l["outputs"][0]["modelReadVersion"] = json!(2),
            "unknown result Model Todo v2",
        ),
        (
            "missing read contract",
            |l| {
                l["outputs"][0]
                    .as_object_mut()
                    .unwrap()
                    .remove("modelReadVersion");
            },
            "missing Model read version",
        ),
        (
            "wrong handler type",
            |l| l["outputs"][0]["handlerType"]["model"] = json!("Note"),
            "invalid Action identity handler type",
        ),
        (
            "one Model at two read versions",
            |l| {
                let mut again = l["outputs"][0].clone();
                again["name"] = json!("older");
                again["modelReadVersion"] = json!(2);
                l["outputs"].as_array_mut().unwrap().push(again);
            },
            "reads Model Todo at two contract versions",
        ),
        (
            "duplicate output",
            |l| l["outputs"][1]["name"] = json!("todos"),
            "invalid Load output",
        ),
        (
            "duplicate input",
            |l| l["inputs"][1]["name"] = json!("projectId"),
            "invalid Load input name",
        ),
        (
            "nullable list input",
            |l| l["inputs"][2]["nullable"] = json!(true),
            "Load lists cannot be nullable",
        ),
        (
            "enum outside snapshot",
            |l| l["input"]["enums"] = json!([]),
            "unknown Action enum",
        ),
        (
            "snapshot operand model",
            |l| l["input"]["models"] = json!([model("Todo", json!([]))]),
            "cannot retain Model operands",
        ),
        (
            "reserved get",
            |l| l["name"] = json!("Get"),
            "Load name Get is reserved",
        ),
        (
            "reserved list",
            |l| l["name"] = json!("list"),
            "Load name list is reserved",
        ),
        (
            "reserved invalidate",
            |l| l["name"] = json!("Invalidate"),
            "Load name Invalidate is reserved",
        ),
        (
            "action name",
            |l| l["name"] = json!("Send"),
            "shares the operation name of another operation",
        ),
        (
            "normalized action name",
            |l| l["name"] = json!("send"),
            "shares the operation name of another operation",
        ),
    ];
    for (label, change, fragment) in cases {
        let error = edit(&change).unwrap_err().to_string();
        assert!(error.contains(fragment), "{label}: {error}");
    }
    let mut duplicate = raw_schema();
    duplicate["loads"]
        .as_array_mut()
        .unwrap()
        .push(load_descriptor());
    fails(
        Schema::from_value(duplicate),
        "invalid or duplicate Load descriptor",
    );
    let mut spelled = raw_schema();
    let mut other = load_descriptor();
    other["name"] = json!("projectTodos");
    spelled["loads"].as_array_mut().unwrap().push(other);
    fails(
        Schema::from_value(spelled),
        "shares the operation name of another operation",
    );
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
