use axton_core::*;
use serde_json::{Value, json};

fn schema() -> Schema {
    Schema::from_value(
        json!({"enums": [{"name":"Mood","values":["calm","busy"]}], "models":[{
            "name":"Entry", "identity":["id"], "fields":[
                {"name":"id","type":{"kind":"scalar","name":"uuid"},"nullable":false},
                {"name":"text","type":{"kind":"scalar","name":"string"},"nullable":false},
                {"name":"note","type":{"kind":"scalar","name":"string"},"nullable":true},
                {"name":"count","type":{"kind":"scalar","name":"int"},"nullable":false}
            ]
        }]}),
    )
    .unwrap()
}
const ID: &str = "01890F47-1234-7123-8123-123456789ABC";

#[test]
fn fresh_model_result_cannot_omit_declared_nullable_field() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/protocol/action-results.json"
    ))
    .unwrap();
    let mut raw = fixture["schema"].clone();
    raw["resultModels"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"note","type":{"kind":"scalar","name":"string"},"nullable":true}));
    let schema = Schema::from_value(raw).unwrap();
    let action = schema.action("Find", 1).unwrap();
    let incomplete = json!({"todo":{"id":ID.to_lowercase(),"title":"A"}});
    assert!(validate_action_result(&schema, action, &incomplete).is_err());
}

fn action_schema() -> Schema {
    Schema::from_value(json!({
        "enums":[], "models":[],
        "actions":[{"name":"Send","version":1,"inputs":[
            {"kind":"value","name":"to","type":{"kind":"scalar","name":"string"},"nullable":false,"list":false,"required":true,"cardinality":"single"}
        ],"outputs":[
            {"name":"messageId","kind":"value","type":{"kind":"scalar","name":"string"},"cardinality":"single","source":"handlerValue"},
            {"name":"note","kind":"value","type":{"kind":"scalar","name":"string"},"cardinality":"optional","source":"handlerValue"}
        ]},{"name":"Void","version":1,"inputs":[],"outputs":[]}]
    })).unwrap()
}

#[test]
fn ordinary_only_actions_normalize_args_and_distinguish_named_null_from_void() {
    let schema = action_schema();
    let action = schema.action("Send", 1).unwrap();
    assert_eq!(
        normalize_action_args(&schema, action, &json!({"to":"a"})).unwrap(),
        json!({"to":"a"})
    );
    assert!(normalize_action_args(&schema, action, &json!({"to":7})).is_err());
    assert!(normalize_action_args(&schema, action, &json!({"to":"a","extra":true})).is_err());
    assert_eq!(
        validate_action_result(&schema, action, &json!({"messageId":"m","note":null})).unwrap(),
        json!({"messageId":"m","note":null})
    );
    assert!(validate_action_result(&schema, action, &Value::Null).is_err());
    assert!(validate_action_result(&schema, action, &json!({"messageId":"m"})).is_err());
    let void = schema.action("Void", 1).unwrap();
    assert_eq!(
        validate_action_result(&schema, void, &Value::Null).unwrap(),
        Value::Null
    );
    assert!(validate_action_result(&schema, void, &json!({})).is_err());
}

#[test]
fn action_receipt_keeps_each_result_when_authority_collapses() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/protocol/action-results.json"
    ))
    .unwrap();
    let schema = Schema::from_value(fixture["schema"].clone()).unwrap();
    let calls: Vec<ActionIntent> = serde_json::from_value(fixture["calls"].clone()).unwrap();
    let request =
        PushRequest::decode_actions(fixture["request"].to_string().as_bytes(), &schema).unwrap();
    let receipt =
        PushReceipt::decode_actions(fixture["receipt"].to_string().as_bytes(), &request, &schema)
            .unwrap();
    assert!(
        PushReceipt::decode(fixture["receipt"].to_string().as_bytes()).is_err(),
        "Action results require request-aware correlation"
    );
    assert_eq!(receipt.completions.len(), 2);
    assert_eq!(receipt.completions[0].call_id, calls[0].call_id);
    assert_eq!(
        success_result(&receipt.completions[0])["todo"]["title"],
        "A"
    );
    assert_eq!(
        success_result(&receipt.completions[1])["todo"]["title"],
        "B"
    );
    assert_eq!(receipt.records.len(), 1);
    assert_eq!(receipt.records[0].state["title"], "B");
    for bad in fixture["invalidReceipts"].as_array().unwrap() {
        assert!(
            PushReceipt::decode_actions(bad.to_string().as_bytes(), &request, &schema).is_err(),
            "{bad}"
        );
    }
}

#[test]
fn action_receipt_rejection_matches_the_frozen_ordinal_and_failure_code() {
    let schema = action_schema();
    let request = PushRequest::decode_actions(json!({"clientId":"device","batchSequence":3,"models":{},"mutations":[{"ordinal":7,"callId":ID,"name":"Send","version":1,"args":{"to":"a"}}]}).to_string().as_bytes(), &schema).unwrap();
    let failure = json!({"callId":ID.to_lowercase(),"outcome":{"status":"failed","code":"handler.failed","execution":"rejected"}});
    let base = json!({"clientId":"device","batchSequence":3,"rejections":[{"ordinal":7,"code":"handler.failed"}],"completions":[failure],"records":[]});
    assert!(PushReceipt::decode_actions(base.to_string().as_bytes(), &request, &schema).is_ok());
    for bad in [
        json!({"clientId":"device","batchSequence":3,"rejections":[],"completions":[failure],"records":[]}),
        json!({"clientId":"device","batchSequence":3,"rejections":[{"ordinal":1,"code":"handler.failed"}],"completions":[failure],"records":[]}),
        json!({"clientId":"device","batchSequence":3,"rejections":[{"ordinal":7,"code":"other.failed"}],"completions":[failure],"records":[]}),
    ] {
        assert!(
            PushReceipt::decode_actions(bad.to_string().as_bytes(), &request, &schema).is_err(),
            "{bad}"
        );
    }
}

#[test]
fn action_receipt_returns_normalized_model_identity_in_completion() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/protocol/action-results.json"
    ))
    .unwrap();
    let schema = Schema::from_value(fixture["schema"].clone()).unwrap();
    let request =
        PushRequest::decode_actions(fixture["request"].to_string().as_bytes(), &schema).unwrap();
    let mut response = fixture["receipt"].clone();
    response["completions"][0]["outcome"]["result"]["todo"]["id"] = json!(ID);
    let decoded =
        PushReceipt::decode_actions(response.to_string().as_bytes(), &request, &schema).unwrap();
    assert_eq!(
        success_result(&decoded.completions[0])["todo"]["id"],
        ID.to_lowercase()
    );
}

fn success_result(completion: &CallCompletion) -> &Value {
    match &completion.outcome {
        ActionOutcome::Succeeded { result } => result,
        ActionOutcome::Failed { .. } => panic!("expected success"),
    }
}

#[test]
fn direct_action_request_accepts_empty_models_only_for_scalar_contracts() {
    let schema = action_schema();
    let wire = br#"{"call":{"callId":"01890F47-1234-7123-8123-123456789ABC","name":"Send","version":1,"args":{"to":"a"}},"models":{}}"#;
    let request = DirectActionRequest::decode(wire, &schema).unwrap();
    assert_eq!(request.call.call_id, ID.to_lowercase());
    assert_eq!(
        DirectActionRequest::decode(&request.encode().unwrap(), &schema)
            .unwrap()
            .call
            .call_id,
        ID.to_lowercase()
    );
    for bad in [
        json!({"call":{"callId":ID,"name":"Unknown","version":1,"args":{"to":"a"}},"models":{}}),
        json!({"call":{"callId":ID,"name":"Send","version":2,"args":{"to":"a"}},"models":{}}),
        json!({"call":{"callId":ID,"name":"Send","version":1,"args":{"to":3}},"models":{}}),
        json!({"call":{"callId":"bad","name":"Send","version":1,"args":{"to":"a"}},"models":{}}),
    ] {
        assert!(
            DirectActionRequest::decode(bad.to_string().as_bytes(), &schema).is_err(),
            "{bad}"
        );
    }
}

#[test]
fn action_push_envelope_rejects_duplicate_call_ids_and_oversize_bytes() {
    let schema = action_schema();
    let call = json!({"callId":ID,"name":"Send","version":1,"args":{"to":"a"},"ordinal":1});
    let duplicate = json!({"clientId":"device","batchSequence":1,"models":{},"mutations":[call,{"callId":ID,"name":"Send","version":1,"args":{"to":"b"},"ordinal":2}]});
    assert!(PushRequest::decode_actions(duplicate.to_string().as_bytes(), &schema).is_err());
    let good = json!({"clientId":"device","batchSequence":1,"models":{},"mutations":[{"callId":ID,"name":"Send","version":1,"args":{"to":"a"},"ordinal":1}]});
    assert_eq!(
        PushRequest::decode_actions(good.to_string().as_bytes(), &schema)
            .unwrap()
            .mutations
            .len(),
        1
    );
    let too_large = json!({"clientId":"device","batchSequence":1,"models":{},"mutations":[{"callId":ID,"name":"Send","version":1,"args":{"to":"x".repeat(limits::PUSH_BYTES)},"ordinal":1}]});
    assert!(PushRequest::decode_actions(too_large.to_string().as_bytes(), &schema).is_err());
}

#[test]
fn structural_action_envelope_keeps_unsupported_version_for_per_call_rejection() {
    let schema = action_schema();
    let request = json!({"clientId":"device","batchSequence":1,"models":{},"mutations":[{"callId":ID,"name":"Send","version":2,"args":{"to":"a"},"ordinal":7}]});
    assert!(PushRequest::decode_action_envelope(request.to_string().as_bytes()).is_ok());
    assert!(PushRequest::decode_actions(request.to_string().as_bytes(), &schema).is_err());
}

#[test]
fn action_schema_rejects_model_reads_without_local_model_and_bad_output_source() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/protocol/action-results.json"
    ))
    .unwrap();
    let mut no_local = fixture["schema"].clone();
    no_local["models"] = json!([]);
    assert!(Schema::from_value(no_local).is_err());
    let mut wrong_source = fixture["schema"].clone();
    wrong_source["actions"][0]["outputs"][0]["source"] = json!({"inputIdentity":"missing"});
    assert!(Schema::from_value(wrong_source).is_err());
    let mut missing_history = fixture["schema"].clone();
    missing_history["resultModels"] = json!([]);
    assert!(Schema::from_value(missing_history).is_err());
}

#[test]
fn retained_action_metadata_survives_schema_round_trip_and_optional_model_defaults_null() {
    let mut raw: Value = serde_json::from_str(include_str!(
        "../../../fixtures/protocol/action-results.json"
    ))
    .unwrap();
    let schema = &mut raw["schema"];
    schema["actions"][0]["inputs"].as_array_mut().unwrap().push(json!({"kind":"model","name":"maybe","model":"Todo","operation":"update","cardinality":"optional","bindings":[{"slot":"prior","fields":["id"]}]}));
    schema["actions"][0]["requirements"] =
        json!([{"model":"Todo","field":"title","name":"Ready","arguments":{}}]);
    schema["actions"][0]["prerequisites"] = json!([{"name":"Ready","fields":[]}]);
    schema["actions"][0]["sequence"] = json!({"after":[{"name":"Earlier","arguments":{}}]});
    let parsed = Schema::from_value(schema.clone()).unwrap();
    let serialized = serde_json::to_value(&parsed).unwrap();
    assert_eq!(
        serialized["actions"][0]["inputs"][1]["bindings"],
        schema["actions"][0]["inputs"][1]["bindings"]
    );
    assert_eq!(
        serialized["actions"][0]["requirements"],
        schema["actions"][0]["requirements"]
    );
    assert_eq!(
        serialized["actions"][0]["prerequisites"],
        schema["actions"][0]["prerequisites"]
    );
    assert_eq!(
        serialized["actions"][0]["sequence"],
        schema["actions"][0]["sequence"]
    );
    assert_eq!(
        normalize_action_args(
            &parsed,
            parsed.action("Find", 1).unwrap(),
            &json!({"query":"a"})
        )
        .unwrap(),
        json!({"query":"a","maybe":null})
    );
    assert!(
        normalize_action_args(
            &parsed,
            parsed.action("Find", 1).unwrap(),
            &json!({"maybe":null})
        )
        .is_err()
    );
}

#[test]
fn action_model_operands_and_delete_results_use_exact_identity_objects() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/protocol/action-results.json"
    ))
    .unwrap();
    let mut raw = fixture["schema"].clone();
    raw["actions"][0]["inputs"] = json!([{"kind":"model","name":"todo","model":"Todo","operation":"update","cardinality":"single","allowedPatchFields":["title"]}]);
    raw["actions"][0]["outputs"] = json!([{"name":"todo","kind":"deleteIdentity","model":"Todo","cardinality":"single","source":{"inputIdentity":"todo"}}]);
    let schema = Schema::from_value(raw).unwrap();
    let action = schema.action("Find", 1).unwrap();
    let normalized =
        normalize_action_args(&schema, action, &json!({"todo":{"id":ID,"title":"B"}})).unwrap();
    assert_eq!(normalized["todo"]["id"], ID.to_lowercase());
    assert!(normalize_action_args(&schema, action, &json!({"todo":{"id":7,"title":"B"}})).is_err());
    assert!(
        normalize_action_args(&schema, action, &json!({"todo":{"id":ID,"done":true}})).is_err()
    );
    assert_eq!(
        validate_action_result(&schema, action, &json!({"todo":{"id":ID}})).unwrap()["todo"]["id"],
        ID.to_lowercase()
    );
    assert!(validate_action_result(&schema, action, &json!({"todo":ID})).is_err());
}

#[test]
fn flat_update_and_delete_keep_model_fields_named_identity_or_patch() {
    let mut raw: serde_json::Value = serde_json::from_str(include_str!(
        "../../../fixtures/protocol/action-results.json"
    ))
    .unwrap();
    let schema = &mut raw["schema"];
    schema["models"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"identity","type":{"kind":"scalar","name":"string"},"nullable":true}));
    schema["models"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"patch","type":{"kind":"scalar","name":"string"},"nullable":true}));
    schema["actions"][0]["inputs"] = json!([{"kind":"model","name":"todo","model":"Todo","operation":"update","cardinality":"single","allowedPatchFields":["identity","patch"]}]);
    schema["actions"][0]["outputs"] = json!([]);
    let schema: Schema = serde_json::from_value(schema.clone()).unwrap();
    schema.validate().unwrap();
    let action = schema.action("Find", 1).unwrap();
    assert_eq!(
        normalize_action_args(&schema, action, &json!({"todo":{"id":ID,"identity":"x"}})).unwrap(),
        json!({"todo":{"id":ID.to_lowercase(),"identity":"x"}})
    );
    assert!(
        normalize_action_args(&schema, action, &json!({"todo":{"id":ID,"done":true}})).is_err()
    );
}

#[test]
fn flat_composite_delete_accepts_only_the_identity_fields() {
    let schema = Schema::from_value(json!({"enums":[],"models":[{"name":"Book","identity":["slug","edition"],"fields":[{"name":"slug","type":{"kind":"scalar","name":"string"},"nullable":false},{"name":"edition","type":{"kind":"scalar","name":"int"},"nullable":false},{"name":"title","type":{"kind":"scalar","name":"string"},"nullable":false}]}],"actions":[{"name":"Delete","version":1,"inputs":[{"kind":"model","name":"book","model":"Book","operation":"delete","cardinality":"single"}],"outputs":[]}]})).unwrap();
    let action = schema.action("Delete", 1).unwrap();
    assert_eq!(
        normalize_action_args(
            &schema,
            action,
            &json!({"book":{"edition":2,"slug":"edition-2"}})
        )
        .unwrap(),
        json!({"book":{"slug":"edition-2","edition":2}})
    );
    assert!(
        normalize_action_args(
            &schema,
            action,
            &json!({"book":{"slug":"edition-2","edition":2,"title":"extra"}})
        )
        .is_err()
    );
    assert!(normalize_action_args(&schema, action, &json!({"book":{"slug":"edition-2"}})).is_err());
}

#[test]
fn model_action_requires_local_read_version_independent_of_result_snapshot() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/protocol/action-results.json"
    ))
    .unwrap();
    let schema = Schema::from_value(fixture["schema"].clone()).unwrap();
    let request = &fixture["request"];
    assert!(PushRequest::decode_actions(request.to_string().as_bytes(), &schema).is_ok());
    for models in [json!({}), json!({"Todo":1}), json!({"Todo":3})] {
        let mut invalid = request.clone();
        invalid["models"] = models;
        assert!(PushRequest::decode_actions(invalid.to_string().as_bytes(), &schema).is_err());
    }
}

#[test]
fn direct_action_response_correlates_and_validates_before_application() {
    let schema = action_schema();
    let request = DirectActionRequest::decode(
        json!({"call":{"callId":ID,"name":"Send","version":1,"args":{"to":"a"}},"models":{}})
            .to_string()
            .as_bytes(),
        &schema,
    )
    .unwrap();
    let success = json!({"completion":{"callId":ID.to_lowercase(),"outcome":{"status":"succeeded","result":{"messageId":"m","note":null}}},"records":[]});
    let response =
        DirectActionResponse::decode(success.to_string().as_bytes(), &request, &schema).unwrap();
    assert_eq!(success_result(&response.completion)["messageId"], "m");
    assert!(DirectActionResponse::decode(&response.encode().unwrap(), &request, &schema).is_ok());
    for bad in [
        json!({"completion":{"callId":"01890f47-1234-7123-8123-123456789abd","outcome":{"status":"succeeded","result":{"messageId":"m","note":null}}},"records":[]}),
        json!({"completion":{"callId":ID.to_lowercase(),"outcome":{"status":"succeeded","result":{"messageId":"m"}}},"records":[]}),
        json!({"completion":{"callId":ID.to_lowercase(),"outcome":{"status":"failed","code":"handler.failed","execution":"unknown"}},"records":[]}),
    ] {
        assert!(
            DirectActionResponse::decode(bad.to_string().as_bytes(), &request, &schema).is_err(),
            "{bad}"
        );
    }
}

#[test]
fn retained_result_materialization_joins_identity_to_v1_state() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/protocol/action-results.json"
    ))
    .unwrap();
    let schema = Schema::from_value(fixture["schema"].clone()).unwrap();
    let identity = &fixture["receipt"]["records"][0]["identity"];
    assert_eq!(
        materialize_action_model(&schema, "Todo", 1, identity, &json!({"title":"A"})).unwrap(),
        json!({"id":identity["id"],"title":"A"})
    );
    assert!(
        materialize_action_model(
            &schema,
            "Todo",
            1,
            identity,
            &json!({"title":"A","done":false})
        )
        .is_err()
    );
}

#[test]
fn action_only_schema_still_checks_value_type_rules() {
    let mut raw = serde_json::to_value(action_schema()).unwrap();
    raw["actions"][0]["inputs"][0]["list"] = json!(true);
    raw["actions"][0]["inputs"][0]["nullable"] = json!(true);
    assert!(Schema::from_value(raw).is_err());
}

#[test]
fn identities_are_exact_normalized_and_independent_of_channels() {
    let schema = schema();
    let key = schema.record_key("Entry", &json!({"id":ID})).unwrap();
    assert_eq!(key.identity, json!({"id":ID.to_lowercase()}));
    assert_eq!(
        key.encoded_identity().unwrap(),
        format!("{{\"id\":\"{}\"}}", ID.to_lowercase())
    );
    assert!(
        schema
            .record_key("Entry", &json!({"id":ID,"channel":"book"}))
            .is_err()
    );
    assert!(schema.record_key("Entry", &json!({"id":"bad"})).is_err());
}

#[test]
fn state_is_complete_but_patch_preserves_absent_and_null() {
    let s = schema();
    assert_eq!(
        s.normalize_state("Entry", &json!({"id":ID,"text":"a","count":0}))
            .unwrap(),
        json!({"text":"a","count":0,"note":null})
    );
    assert!(s.validate_state("Entry", &json!({"count":0})).is_err());
    assert_eq!(
        s.validate_patch("Entry", &json!({"note":null})).unwrap(),
        json!({"note":null})
    );
    assert_eq!(s.validate_patch("Entry", &json!({})).unwrap(), json!({}));
    assert!(s.validate_patch("Entry", &json!({"text":null})).is_err());
    assert!(s.validate_patch("Entry", &json!({"id":ID})).is_err());
    assert!(
        s.validate_patch("Entry", &json!({"count":9007199254740992u64}))
            .is_err()
    );
}

#[test]
fn independent_schemas_load_without_business_rust_types() {
    let second=Schema::from_value(json!({"enums":[],"models":[{"name":"Book","identity":["slug","edition"],"fields":[{"name":"slug","type":{"kind":"scalar","name":"string"},"nullable":false},{"name":"edition","type":{"kind":"scalar","name":"int"},"nullable":false}]}]})).unwrap();
    assert!(
        second
            .record_key("Book", &json!({"slug":"x","edition":1}))
            .is_ok()
    );
    assert!(
        schema()
            .record_key("Book", &json!({"slug":"x","edition":1}))
            .is_err()
    );
}

#[test]
fn a_page_names_its_channels_and_keeps_unknown_fields_out_of_the_records() {
    let page=PullPage::decode(br#"{"cursors":{"book:1":{"from":0,"to":2,"head":2}},"changes":[{"model":"Entry","identity":{"id":"x"},"stamp":2,"state":null}],"future":true}"#).unwrap();
    assert_eq!(page.channels().collect::<Vec<_>>(), ["book:1"]);
    assert_eq!(page.cursors["book:1"].to, 2);
    let wire: Value = serde_json::from_slice(&page.encode().unwrap()).unwrap();
    assert!(wire.get("scope").is_none());
    assert!(wire["changes"][0].get("syncId").is_none());
    assert!(
        wire["changes"][0].get("error").is_none(),
        "no error is no field"
    );
    assert!(
        PullPage::decode(br#"{"cursors":{"a":{"from":0,"to":9007199254740992,"head":9007199254740992}},"changes":[]}"#)
            .is_err()
    );
    assert!(
        PullPage::decode(br#"{"cursors":{"a":{"from":2,"to":1,"head":1}},"changes":[]}"#).is_err()
    );
}

#[test]
fn batch_envelope_keeps_unknown_data_in_canonical_bytes() {
    let a=PushRequest::decode(br#"{"clientId":"c","batchSequence":1,"models":{"Entry":1},"mutations":[{"ordinal":4,"name":"Edit","args":{}}],"future":1}"#).unwrap();
    let b=PushRequest::decode(br#"{"future":1,"mutations":[{"args":{},"name":"Edit","ordinal":4}],"batchSequence":1,"models":{"Entry":1},"clientId":"c"}"#).unwrap();
    assert_eq!(a.encode().unwrap(), b.encode().unwrap());
    assert_eq!(
        a.encode().unwrap(),
        br#"{"batchSequence":1,"clientId":"c","future":1,"models":{"Entry":1},"mutations":[{"args":{},"name":"Edit","ordinal":4}]}"#
    );
    assert_eq!(a.models.get("Entry"), Some(&1));
    let c=PushRequest::decode(br#"{"clientId":"c","batchSequence":1,"models":{"Entry":1},"mutations":[{"ordinal":4,"name":"Edit","args":{}}]}"#).unwrap();
    assert_ne!(a.encode().unwrap(), c.encode().unwrap());
    assert!(
        PushRequest::decode(
            br#"{"clientId":"c","batchSequence":1,"models":{"Entry":1},"mutations":[{"ordinal":1},{"ordinal":1}]}"#
        )
        .is_err()
    );
    assert!(
        PushRequest::decode(
            br#"{"clientId":"c","batchSequence":1,"mutations":[{"ordinal":1,"name":"Edit"}]}"#
        )
        .is_err(),
        "the read contracts the receipt is served at are required"
    );
}

#[test]
fn canonical_numbers_match_javascript_and_utf16_key_order() {
    assert_eq!(
        canonical_json(&json!({"z":1.0,"a":-0.0})).unwrap(),
        "{\"a\":0,\"z\":1}"
    );
    assert_eq!(
        canonical_json(&json!({"\u{e000}":1,"\u{1f600}":2})).unwrap(),
        "{\"😀\":2,\"\":1}"
    );
}

#[test]
fn receipt_wire_round_trips_and_carries_authority_without_a_cursor() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/protocol/receipt-authority.json"
    ))
    .unwrap();
    let canonical = &fixture["canonical"];
    let receipt = PushReceipt::decode(canonical["wire"].as_str().unwrap().as_bytes()).unwrap();
    assert_eq!(receipt.client_id, canonical["clientId"]);
    assert_eq!(
        receipt.batch_sequence,
        canonical["batchSequence"].as_u64().unwrap()
    );
    assert_eq!(
        receipt.records[0].stamp,
        canonical["stamp"].as_u64().unwrap()
    );
    assert!(receipt.rejections.is_empty());
    assert_eq!(
        PushReceipt::decode(&receipt.encode().unwrap()).unwrap(),
        receipt
    );
    assert_eq!(
        String::from_utf8(receipt.encode().unwrap()).unwrap(),
        canonical["wire"].as_str().unwrap(),
        "the canonical bytes are stable"
    );
    assert!(receipt.answers("device-1", 4));
    assert!(!receipt.answers("device-1", 5));
    assert!(!receipt.answers("device-2", 4));
    // A page change is the same type: a page's record decodes as a receipt's.
    let page = PullPage::decode(br#"{"cursors":{"a":{"from":8,"to":9,"head":9}},"changes":[{"model":"Entry","identity":{"id":"e"},"stamp":12,"state":{"text":"Hello","note":null}}]}"#).unwrap();
    assert_eq!(page.changes[0], receipt.records[0]);
    let wire: Value = serde_json::from_slice(&receipt.encode().unwrap()).unwrap();
    assert!(wire["records"][0].get("syncId").is_none());
    assert!(wire.get("requiredCheckpoints").is_none());
    // A receipt never carries a read failure.
    assert!(PushReceipt::decode(br#"{"clientId":"d","batchSequence":1,"rejections":[],"records":[{"model":"Entry","identity":{"id":"e"},"stamp":1,"error":"loader.failed"}]}"#).is_err());
}

#[test]
fn receipt_fixture_cases_decode_as_declared() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/protocol/receipt-authority.json"
    ))
    .unwrap();
    for case in fixture["receipt"].as_array().unwrap() {
        let wire = case["wire"].as_str().unwrap().as_bytes();
        let decoded = PushReceipt::decode(wire);
        assert_eq!(
            decoded.is_ok(),
            case["valid"].as_bool().unwrap(),
            "{}: {decoded:?}",
            case["name"]
        );
        if let Ok(receipt) = decoded {
            assert_eq!(
                PushReceipt::decode(&receipt.encode().unwrap()).unwrap(),
                receipt,
                "{}",
                case["name"]
            );
        }
    }
}

#[test]
fn received_state_supports_additive_schema_evolution() {
    let s = schema();
    assert_eq!(
        s.validate_state("Entry", &json!({"text":"a","count":0,"newField":42}))
            .unwrap(),
        json!({"text":"a","count":0,"note":null})
    );
    assert!(
        s.validate_state("Entry", &json!({"id":ID,"text":"a","count":0}))
            .is_err()
    );
}
#[test]
fn server_pull_request_accepts_js_integer_number_spellings() {
    for number in ["0.0", "1e0", "-0"] {
        let wire = format!("{{\"cursors\":{{\"s\":{number}}},\"models\":{{\"Entry\":1}}}}");
        assert!(PullRequest::decode(wire.as_bytes()).is_ok(), "{number}");
    }
}

#[test]
fn pull_page_fixture_cases_decode_as_declared() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/protocol/pull-page.json")).unwrap();
    let canonical = &fixture["canonical"];
    let page = PullPage::decode(canonical["wire"].as_str().unwrap().as_bytes()).unwrap();
    assert_eq!(
        page.channels().collect::<Vec<_>>(),
        canonical["channels"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c.as_str().unwrap())
            .collect::<Vec<_>>()
    );
    assert_eq!(
        page.changes.len(),
        canonical["changes"].as_u64().unwrap() as usize
    );
    assert_eq!(page.changes[2].error.as_deref(), Some("loader.failed"));
    assert!(!page.cursors["book:demo"].continues(), "at head");
    assert!(
        PullPage::decode(br#"{"cursors":{"a":{"from":0,"to":50,"head":80}},"changes":[]}"#)
            .unwrap()
            .cursors["a"]
            .continues()
    );
    assert!(page.changes[2].is_error() && page.changes[2].state.is_null());
    assert_eq!(
        String::from_utf8(page.encode().unwrap()).unwrap(),
        canonical["wire"].as_str().unwrap(),
        "the canonical bytes are stable"
    );
    for case in fixture["page"].as_array().unwrap() {
        let decoded = PullPage::decode(case["wire"].as_str().unwrap().as_bytes());
        assert_eq!(
            decoded.is_ok(),
            case["valid"].as_bool().unwrap(),
            "{}: {decoded:?}",
            case["name"]
        );
        if let Ok(page) = decoded {
            assert_eq!(
                PullPage::decode(&page.encode().unwrap()).unwrap(),
                page,
                "{}",
                case["name"]
            );
        }
    }
    for case in fixture["request"].as_array().unwrap() {
        let decoded = PullRequest::decode(case["wire"].as_str().unwrap().as_bytes());
        assert_eq!(
            decoded.is_ok(),
            case["valid"].as_bool().unwrap(),
            "{}: {decoded:?}",
            case["name"]
        );
    }
}

#[test]
fn shared_wire_fixtures_preserve_counter_boundaries() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/protocol/counter-boundaries.json"
    ))
    .unwrap();
    for case in fixture["pull"].as_array().unwrap() {
        let wire = case["wire"].as_str().unwrap().as_bytes();
        let valid = PullPage::decode(wire).is_ok();
        assert_eq!(valid, case["valid"].as_bool().unwrap(), "{}", case["name"]);
    }
}

#[test]
fn field_default_and_record_stamp_round_trip_and_axton_prefix_is_rejected() {
    let field: FieldDescriptor = serde_json::from_value(
        json!({"name":"rank","nullable":false,"type":{"kind":"scalar","name":"int"},"default":0}),
    )
    .unwrap();
    assert_eq!(field.default, Some(json!(0)));
    let plain: FieldDescriptor = serde_json::from_value(
        json!({"name":"t","nullable":true,"type":{"kind":"scalar","name":"string"}}),
    )
    .unwrap();
    assert_eq!(plain.default, None);
    assert!(!serde_json::to_string(&plain).unwrap().contains("default"));
    let page = PullPage::decode(
        br#"{"cursors":{"c":{"from":0,"to":1,"head":1}},"changes":[{"model":"E","identity":{"id":"e"},"stamp":7,"state":null}]}"#,
    )
    .unwrap();
    assert_eq!(page.changes[0].stamp, 7);
    assert!(
        String::from_utf8(page.encode().unwrap())
            .unwrap()
            .contains(r#""stamp":7"#)
    );
    let unstamped = PullPage::decode(
        br#"{"cursors":{"c":{"from":0,"to":1,"head":1}},"changes":[{"model":"E","identity":{"id":"e"},"state":null}]}"#,
    );
    assert!(unstamped.unwrap_err().to_string().contains("stamp"));
    let bad = Schema::from_value(
        json!({"enums":[],"models":[{"name":"axton_x","identity":["id"],"fields":[{"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"}}]}]}),
    );
    assert!(bad.is_err());
    for name in ["sqlite_x", "SQLITE_x", "AXTON_x"] {
        let reserved = Schema::from_value(
            json!({"enums":[],"models":[{"name":name,"identity":["id"],"fields":[{"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"}}]}]}),
        );
        assert!(
            reserved.unwrap_err().to_string().contains("reserved"),
            "{name} must be refused as reserved"
        );
    }
    for name in ["sqlitex", "Sqlite", "axtonx"] {
        assert!(
            Schema::from_value(
                json!({"enums":[],"models":[{"name":name,"identity":["id"],"fields":[{"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"}}]}]}),
            )
            .is_ok(),
            "{name} must stay valid"
        );
    }
}

#[test]
fn scalar_and_enum_values_normalize_or_are_refused() {
    let s = Schema::from_value(json!({"enums":[{"name":"Mood","values":["calm","busy"]}],"models":[{
        "name":"E","identity":["id"],"fields":[
            {"name":"id","type":{"kind":"scalar","name":"string"},"nullable":false},
            {"name":"at","type":{"kind":"scalar","name":"dateTime"},"nullable":false},
            {"name":"ratio","type":{"kind":"scalar","name":"float"},"nullable":true},
            {"name":"mood","type":{"kind":"enum","name":"Mood"},"nullable":false},
            {"name":"tags","type":{"kind":"list","element":{"kind":"scalar","name":"string"}},"nullable":false}
        ]}]}))
    .unwrap();
    let patch = |v: Value| s.validate_patch("E", &v);
    // dateTime re-encodes to UTC milliseconds; a date, a space separator or a number is refused.
    assert_eq!(
        patch(json!({"at":"2024-01-02T03:04:05+01:00"})).unwrap(),
        json!({"at":"2024-01-02T02:04:05.000Z"})
    );
    assert_eq!(
        patch(json!({"at":"2024-01-02T03:04:05.25Z"})).unwrap()["at"],
        "2024-01-02T03:04:05.250Z"
    );
    for bad in [
        json!("2024-01-02"),
        json!("2024-01-02 03:04:05Z"),
        json!(1704164645),
    ] {
        assert!(patch(json!({"at":bad})).is_err(), "{bad} must be refused");
    }
    // float must be finite; -0 becomes 0; null is allowed only because ratio is nullable.
    assert_eq!(patch(json!({"ratio":-0.0})).unwrap()["ratio"], json!(0.0));
    assert_eq!(patch(json!({"ratio":1.5})).unwrap()["ratio"], json!(1.5));
    assert_eq!(patch(json!({"ratio":null})).unwrap()["ratio"], Value::Null);
    assert!(patch(json!({"ratio":"1.5"})).is_err());
    // JSON cannot carry NaN or infinity: `Value::from(f64::NAN)` is already null,
    // so the only non-finite inputs a wire can produce are refused as non-numbers.
    assert!(patch(json!({"ratio":"NaN"})).is_err());
    assert!(patch(json!({"ratio":"Infinity"})).is_err());
    // enum values must be declared and be strings.
    assert_eq!(patch(json!({"mood":"busy"})).unwrap()["mood"], "busy");
    assert!(patch(json!({"mood":"angry"})).is_err());
    assert!(patch(json!({"mood":1})).is_err());
    assert!(patch(json!({"mood":null})).is_err(), "mood is not nullable");
    // lists normalize each element and refuse non-lists and bad elements.
    assert_eq!(
        patch(json!({"tags":["a","b"]})).unwrap()["tags"],
        json!(["a", "b"])
    );
    assert!(patch(json!({"tags":"a"})).is_err());
    assert!(patch(json!({"tags":["a",1]})).is_err());
    assert!(patch(json!({"tags":null})).is_err(), "lists cannot be null");
}

#[test]
fn list_descriptors_must_hold_scalars_and_cannot_be_nullable() {
    let model = |field: Value| {
        Schema::from_value(
            json!({"enums":[{"name":"Mood","values":["calm"]}],"models":[{
            "name":"E","identity":["id"],"fields":[
                {"name":"id","type":{"kind":"scalar","name":"string"},"nullable":false},
                field
            ]}]}),
        )
    };
    assert!(model(json!({"name":"tags","type":{"kind":"list","element":{"kind":"scalar","name":"string"}},"nullable":false})).is_ok());
    let nullable_list = model(
        json!({"name":"tags","type":{"kind":"list","element":{"kind":"scalar","name":"string"}},"nullable":true}),
    );
    assert!(
        nullable_list
            .unwrap_err()
            .to_string()
            .contains("lists cannot be nullable")
    );
    let enum_list = model(
        json!({"name":"moods","type":{"kind":"list","element":{"kind":"enum","name":"Mood"}},"nullable":false}),
    );
    assert!(
        enum_list
            .unwrap_err()
            .to_string()
            .contains("list elements must be scalar")
    );
    let nested = model(
        json!({"name":"grid","type":{"kind":"list","element":{"kind":"list","element":{"kind":"scalar","name":"int"}}},"nullable":false}),
    );
    assert!(nested.is_err());
    assert!(
        model(json!({"name":"mood","type":{"kind":"enum","name":"Unknown"},"nullable":false}))
            .is_err()
    );
}

#[test]
fn push_batches_hold_one_to_twenty_mutations_with_distinct_ordinals() {
    let batch = |count: usize| {
        let mutations: Vec<Value> = (1..=count)
            .map(|i| json!({"ordinal":i,"name":"edit","operations":[]}))
            .collect();
        json!({"clientId":"c","batchSequence":1,"models":{"Entry":1},"mutations":mutations})
            .to_string()
    };
    assert!(PushRequest::decode(batch(0).as_bytes()).is_err());
    assert_eq!(
        PushRequest::decode(batch(1).as_bytes())
            .unwrap()
            .mutations
            .len(),
        1
    );
    assert_eq!(
        PushRequest::decode(batch(20).as_bytes())
            .unwrap()
            .mutations
            .len(),
        20
    );
    let err = PushRequest::decode(batch(21).as_bytes()).unwrap_err();
    assert!(err.to_string().contains("1..20"), "{err}");
    let duplicate = json!({"clientId":"c","batchSequence":1,"models":{"Entry":1},"mutations":[
        {"ordinal":1,"name":"edit","operations":[]},{"ordinal":1,"name":"edit","operations":[]}
    ]})
    .to_string();
    assert!(PushRequest::decode(duplicate.as_bytes()).is_err());
    let zero = json!({"clientId":"c","batchSequence":1,"mutations":[{"ordinal":0,"name":"edit","operations":[]}]}).to_string();
    assert!(PushRequest::decode(zero.as_bytes()).is_err());
}

#[test]
fn push_requests_refuse_a_blank_client_id_and_pulls_carry_none() {
    let mutations = json!([{"ordinal":1,"name":"edit","operations":[]}]);
    for blank in ["", "   "] {
        let push =
            json!({"clientId":blank,"batchSequence":1,"models":{"Entry":1},"mutations":mutations})
                .to_string();
        assert!(
            PushRequest::decode(push.as_bytes()).is_err(),
            "push {blank:?}"
        );
    }
    let push = json!({"clientId":"c","batchSequence":1,"models":{"Entry":1},"mutations":mutations})
        .to_string();
    assert_eq!(PushRequest::decode(push.as_bytes()).unwrap().client_id, "c");
    let missing = json!({"batchSequence":1,"mutations":mutations}).to_string();
    assert!(
        PushRequest::decode(missing.as_bytes()).is_err(),
        "missing clientId"
    );
    let pull = json!({"cursors":{"a":0},"models":{"Entry":1}}).to_string();
    let request = PullRequest::decode(pull.as_bytes()).unwrap();
    assert_eq!(
        String::from_utf8(request.encode().unwrap()).unwrap(),
        r#"{"cursors":{"a":0},"models":{"Entry":1}}"#,
        "a pull identifies no client"
    );
}

#[test]
fn shared_limits_are_defined_once_and_apply_per_channel() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/protocol/live-messages.json"
    ))
    .unwrap();
    assert_eq!(fixture["limits"]["pushMutations"], limits::PUSH_MUTATIONS);
    assert_eq!(fixture["limits"]["pushBytes"], limits::PUSH_BYTES);
    assert_eq!(fixture["limits"]["pullChanges"], limits::PULL_CHANGES);
    let page = |channels: usize, count: usize| {
        let changes: Vec<Value> = (1..=count)
            .map(
                |i| json!({"model":"Entry","identity":{"id":i.to_string()},"stamp":i,"state":null}),
            )
            .collect();
        let cursors: serde_json::Map<String, Value> = (0..channels)
            .map(|c| {
                (
                    format!("c{c}"),
                    json!({"from":0,"to":count.max(1),"head":count.max(1)}),
                )
            })
            .collect();
        json!({"cursors":cursors,"changes":changes}).to_string()
    };
    assert!(PullPage::decode(page(1, limits::PULL_CHANGES).as_bytes()).is_ok());
    let err = PullPage::decode(page(1, limits::PULL_CHANGES + 1).as_bytes()).unwrap_err();
    assert!(err.to_string().contains("exceeds 50"), "{err}");
    assert!(
        PullPage::decode(page(2, limits::PULL_CHANGES * 2).as_bytes()).is_ok(),
        "the cap is per channel"
    );
    assert!(PullPage::decode(page(2, limits::PULL_CHANGES * 2 + 1).as_bytes()).is_err());
}

#[test]
fn live_frames_decode_as_acknowledgement_or_page_and_channels_normalize() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/protocol/live-messages.json"
    ))
    .unwrap();
    for case in fixture["subscribe"].as_array().unwrap() {
        let wire = case["wire"].as_str().unwrap().as_bytes();
        match SubscribeRequest::decode(wire) {
            Ok(request) => {
                assert_eq!(case["valid"], true, "{}", case["name"]);
                assert_eq!(
                    json!(request.channels),
                    case["channels"],
                    "{}",
                    case["name"]
                );
                assert_eq!(json!(request.models), case["models"], "{}", case["name"]);
                let again = SubscribeRequest::decode(&request.encode().unwrap()).unwrap();
                assert_eq!(again, request, "encoding is canonical: {}", case["name"]);
            }
            Err(_) => assert_eq!(case["valid"], false, "{}", case["name"]),
        }
    }
    for case in fixture["acknowledgement"].as_array().unwrap() {
        let wire = case["wire"].as_str().unwrap().as_bytes();
        match SubscriptionAck::decode(wire) {
            Ok(ack) => {
                assert_eq!(case["valid"], true, "{}", case["name"]);
                assert_eq!(json!(ack.cursors), case["cursors"], "{}", case["name"]);
                assert_eq!(
                    SubscriptionAck::decode(&ack.encode().unwrap()).unwrap(),
                    ack
                );
            }
            Err(_) => assert_eq!(case["valid"], false, "{}", case["name"]),
        }
    }
    for case in fixture["frame"].as_array().unwrap() {
        let wire = case["wire"].as_str().unwrap().as_bytes();
        let kind = match LiveMessage::decode(wire) {
            Ok(LiveMessage::Acknowledged(_)) => "acknowledged",
            Ok(LiveMessage::Page(_)) => "page",
            Err(_) => "invalid",
        };
        assert_eq!(kind, case["kind"], "{}", case["name"]);
    }
    let models = std::collections::BTreeMap::from([("Task".to_string(), 1)]);
    let request = SubscribeRequest::new(vec!["b".into(), "a".into()], models.clone()).unwrap();
    let heads = |pairs: &[(&str, u64)]| {
        SubscriptionAck::new(pairs.iter().map(|(c, h)| (c.to_string(), *h)).collect()).unwrap()
    };
    assert!(heads(&[("a", 4), ("b", 0)]).confirms(&request));
    assert!(!heads(&[("a", 4)]).confirms(&request));
    assert!(!heads(&[("a", 4), ("b", 0), ("c", 1)]).confirms(&request));
    // The server's frame is the acknowledgement the client decodes, byte for byte.
    assert_eq!(
        String::from_utf8(heads(&[("b", 0), ("a", 4)]).encode().unwrap()).unwrap(),
        r#"{"cursors":{"a":4,"b":0},"type":"subscribed"}"#
    );
}

#[test]
fn pull_and_subscribe_declare_the_read_contracts_and_refuse_a_missing_or_bad_declaration() {
    // The declaration is the same object on both paths: one positive version per model.
    let good = json!({"cursors":{"a":0},"models":{"Task":2,"Note":1}});
    let request = PullRequest::decode(good.to_string().as_bytes()).unwrap();
    assert_eq!(request.models.get("Task"), Some(&2));
    assert_eq!(request.models.get("Note"), Some(&1));
    assert_eq!(
        String::from_utf8(request.encode().unwrap()).unwrap(),
        r#"{"cursors":{"a":0},"models":{"Note":1,"Task":2}}"#,
        "canonical: models sorted by name"
    );
    for (name, models) in [
        ("missing", Value::Null),
        ("not an object", json!(["Task"])),
        ("empty", json!({})),
        ("zero version", json!({"Task":0})),
        ("negative version", json!({"Task":-1})),
        ("fractional version", json!({"Task":1.5})),
        ("string version", json!({"Task":"1"})),
        ("empty model name", json!({"":1})),
    ] {
        let mut pull = good.clone();
        if models.is_null() {
            pull.as_object_mut().unwrap().remove("models");
        } else {
            pull["models"] = models.clone();
        }
        assert!(
            PullRequest::decode(pull.to_string().as_bytes()).is_err(),
            "pull {name}"
        );
        let mut subscribe = json!({"type":"subscribe","channels":["a"],"models":{"Task":1}});
        if models.is_null() {
            subscribe.as_object_mut().unwrap().remove("models");
        } else {
            subscribe["models"] = models;
        }
        assert!(
            SubscribeRequest::decode(subscribe.to_string().as_bytes()).is_err(),
            "subscribe {name}"
        );
    }
    let empty = std::collections::BTreeMap::new();
    assert!(SubscribeRequest::new(vec!["a".into()], empty).is_err());
}

/// An Action with a business input named `store`, two explicit Model outputs,
/// a scalar output, an input-bound output and a Delete confirmation.
fn store_schema() -> Schema {
    let identity = json!({"kind":"identity","model":"Todo","fields":[{"name":"id","type":{"kind":"scalar","name":"uuid"}}]});
    Schema::from_value(json!({
        "enums":[],
        "models":[{"name":"Todo","version":1,"identity":["id"],"fields":[
            {"name":"id","type":{"kind":"scalar","name":"uuid"},"nullable":false},
            {"name":"title","type":{"kind":"scalar","name":"string"},"nullable":false}]}],
        "resultModels":[{"name":"Todo","version":1,"identity":["id"],"fields":[
            {"name":"id","type":{"kind":"scalar","name":"uuid"},"nullable":false},
            {"name":"title","type":{"kind":"scalar","name":"string"},"nullable":false}],"enums":[]}],
        "actions":[
            {"name":"Open","version":1,"inputs":[
                {"kind":"value","name":"store","type":{"kind":"scalar","name":"string"},"nullable":false,"list":false},
                {"kind":"model","name":"todo","model":"Todo","operation":"update","cardinality":"single"},
                {"kind":"model","name":"gone","model":"Todo","operation":"delete","cardinality":"optional"}],
             "outputs":[
                {"name":"mainTodo","kind":"model","model":"Todo","modelReadVersion":1,"cardinality":"single","source":"handlerIdentity","handlerType":identity},
                {"name":"suggestions","kind":"model","model":"Todo","modelReadVersion":1,"cardinality":"list","source":"handlerIdentity","handlerType":identity},
                {"name":"note","kind":"value","type":{"kind":"scalar","name":"string"},"cardinality":"single","source":"handlerValue"},
                {"name":"todo","kind":"model","model":"Todo","modelReadVersion":1,"cardinality":"single","source":{"inputIdentity":"todo"}},
                {"name":"gone","kind":"deleteIdentity","model":"Todo","cardinality":"optional","source":{"inputIdentity":"gone"}}]},
            {"name":"Send","version":1,"inputs":[],"outputs":[]}
        ]
    }))
    .unwrap()
}

fn store_intent(store: Option<Value>) -> Value {
    let mut call = json!({"callId":ID,"name":"Open","version":1,
        "args":{"store":"business","todo":{"id":ID,"title":"B"}}});
    if let Some(store) = store {
        call["store"] = store;
    }
    call
}

#[test]
fn action_store_policy_decodes_bool_or_output_map_and_serializes_canonically() {
    let decode = |store: Option<Value>| -> ActionIntent {
        serde_json::from_value(store_intent(store)).unwrap()
    };
    let omitted = decode(None);
    assert_eq!(omitted.store, ActionStore::All);
    assert!(
        serde_json::to_value(&omitted)
            .unwrap()
            .get("store")
            .is_none()
    );
    let enabled = decode(Some(json!(true)));
    assert_eq!(enabled.store, ActionStore::All);
    assert!(
        serde_json::to_value(&enabled)
            .unwrap()
            .get("store")
            .is_none()
    );
    assert_eq!(decode(Some(json!({}))).store, ActionStore::All);
    let disabled = decode(Some(json!(false)));
    assert_eq!(disabled.store, ActionStore::None);
    assert_eq!(
        serde_json::to_value(&disabled).unwrap()["store"],
        json!(false)
    );
    let map = decode(Some(json!({"suggestions":false,"mainTodo":true})));
    assert_eq!(
        canonical_json(&serde_json::to_value(&map).unwrap()["store"]).unwrap(),
        r#"{"mainTodo":true,"suggestions":false}"#
    );
    assert!(map.store.selects("mainTodo"));
    assert!(!map.store.selects("suggestions"));
    assert!(
        decode(Some(json!({"suggestions":false})))
            .store
            .selects("mainTodo")
    );
    assert!(!disabled.store.selects("mainTodo"));
    assert!(omitted.store.selects("mainTodo"));
    // The policy stays outside business args, including an input named store.
    assert_eq!(map.args["store"], "business");
    for bad in [
        json!(null),
        json!("false"),
        json!(0),
        json!([]),
        json!({"mainTodo":"no"}),
        json!({"mainTodo":null}),
    ] {
        assert!(
            serde_json::from_value::<ActionIntent>(store_intent(Some(bad.clone()))).is_err(),
            "{bad}"
        );
        let mut mutation = store_intent(Some(bad.clone()));
        mutation["ordinal"] = json!(1);
        let push =
            json!({"clientId":"device","batchSequence":1,"models":{},"mutations":[mutation]});
        assert!(
            PushRequest::decode_action_envelope(push.to_string().as_bytes()).is_err(),
            "{bad}"
        );
    }
}

#[test]
fn action_store_keys_name_only_explicit_model_outputs() {
    let schema = store_schema();
    let open = schema.action("Open", 1).unwrap();
    for good in [
        json!(true),
        json!(false),
        json!({"suggestions":false}),
        json!({"mainTodo":true,"suggestions":false}),
    ] {
        let intent: ActionIntent =
            serde_json::from_value(store_intent(Some(good.clone()))).unwrap();
        intent
            .store
            .validate(open)
            .unwrap_or_else(|e| panic!("{good}: {e}"));
        let normalized = intent.normalize(&schema).unwrap();
        assert_eq!(normalized.args["store"], "business");
    }
    // Unknown, scalar, input-bound and Delete-confirmation keys are refused,
    // even when their value is true.
    for key in ["missing", "note", "todo", "gone", "store"] {
        for value in [true, false] {
            let intent: ActionIntent =
                serde_json::from_value(store_intent(Some(json!({key: value})))).unwrap();
            assert!(intent.store.validate(open).is_err(), "{key}");
            assert!(intent.normalize(&schema).is_err(), "{key}");
        }
    }
    // Boolean policy is accepted on an Action without eligible outputs.
    let send = schema.action("Send", 1).unwrap();
    ActionStore::None.validate(send).unwrap();
    assert!(
        ActionStore::Outputs([("x".to_string(), false)].into())
            .validate(send)
            .is_err()
    );
    let eligible: Vec<&str> = open
        .outputs
        .iter()
        .filter(|output| store_eligible(output))
        .map(|output| output.name.as_str())
        .collect();
    assert_eq!(eligible, ["mainTodo", "suggestions"]);
}

#[test]
fn direct_action_request_carries_store_outside_args_and_response_decodes() {
    let schema = store_schema();
    let wire = json!({"call":store_intent(Some(json!({"suggestions":false}))),"models":{"Todo":1}});
    let request = DirectActionRequest::decode(wire.to_string().as_bytes(), &schema).unwrap();
    assert_eq!(
        request.call.store,
        ActionStore::Outputs([("suggestions".to_string(), false)].into())
    );
    let encoded: Value = serde_json::from_slice(&request.encode().unwrap()).unwrap();
    assert_eq!(encoded["call"]["store"], json!({"suggestions":false}));
    assert_eq!(encoded["call"]["args"]["store"], "business");
    let reopened = DirectActionRequest::decode(&request.encode().unwrap(), &schema).unwrap();
    assert_eq!(reopened.call.store, request.call.store);
    let bad = json!({"call":store_intent(Some(json!({"note":false}))),"models":{"Todo":1}});
    assert!(DirectActionRequest::decode(bad.to_string().as_bytes(), &schema).is_err());
    // Structural ingress keeps a semantically invalid key for per-call rejection.
    assert!(DirectActionRequest::decode_envelope(bad.to_string().as_bytes()).is_ok());
    let id = ID.to_lowercase();
    let todo = json!({"id":id,"title":"A"});
    let response = json!({"completion":{"callId":id,"outcome":{"status":"succeeded","result":{
        "mainTodo":todo,"suggestions":[todo],"note":"n","todo":todo,"gone":null}}},"records":[]});
    let decoded =
        DirectActionResponse::decode(response.to_string().as_bytes(), &request, &schema).unwrap();
    assert_eq!(decoded.completion.call_id, id);
}

#[test]
fn action_store_canonical_form_drops_explicit_true_after_validation() {
    let schema = store_schema();
    let open = schema.action("Open", 1).unwrap();
    let outputs = |pairs: &[(&str, bool)]| {
        ActionStore::Outputs(pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect())
    };
    assert_eq!(outputs(&[("mainTodo", true)]).canonical(), ActionStore::All);
    assert_eq!(
        outputs(&[("mainTodo", true), ("suggestions", false)]).canonical(),
        outputs(&[("suggestions", false)])
    );
    assert_eq!(ActionStore::None.canonical(), ActionStore::None);
    // Validation sees the explicit map, so an unknown true key is refused.
    assert!(outputs(&[("missing", true)]).validate(open).is_err());
    let intent: ActionIntent = serde_json::from_value(store_intent(Some(
        json!({"mainTodo":true,"suggestions":false}),
    )))
    .unwrap();
    let normalized = intent.normalize(&schema).unwrap();
    assert_eq!(
        serde_json::to_value(&normalized).unwrap()["store"],
        json!({"suggestions":false})
    );
    let all_true: ActionIntent =
        serde_json::from_value(store_intent(Some(json!({"mainTodo":true})))).unwrap();
    let normalized = all_true.normalize(&schema).unwrap();
    assert!(
        serde_json::to_value(&normalized)
            .unwrap()
            .get("store")
            .is_none()
    );
    let unknown: ActionIntent =
        serde_json::from_value(store_intent(Some(json!({"missing":true})))).unwrap();
    assert!(unknown.normalize(&schema).is_err());
}

#[test]
fn bootstrap_request_fixture_cases_decode_as_declared() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/protocol/bootstrap-request.json"
    ))
    .unwrap();
    let canonical = &fixture["canonical"];
    let request = BootstrapRequest::decode(canonical["wire"].as_str().unwrap().as_bytes()).unwrap();
    assert_eq!(request.channel, canonical["channel"].as_str().unwrap());
    assert_eq!(request.after, canonical["after"].as_u64().unwrap());
    assert_eq!(request.until, canonical["until"].as_u64().unwrap());
    assert_eq!(
        request.models,
        [("Comment".into(), 1), ("Entry".into(), 2)].into()
    );
    assert_eq!(
        String::from_utf8(request.encode().unwrap()).unwrap(),
        canonical["wire"].as_str().unwrap(),
        "the canonical bytes are stable"
    );
    for case in fixture["request"].as_array().unwrap() {
        let decoded = BootstrapRequest::decode(case["wire"].as_str().unwrap().as_bytes());
        assert_eq!(
            decoded.is_ok(),
            case["valid"].as_bool().unwrap(),
            "{}: {decoded:?}",
            case["name"]
        );
        if let Ok(request) = decoded {
            assert_eq!(
                BootstrapRequest::decode(&request.encode().unwrap()).unwrap(),
                request,
                "{}",
                case["name"]
            );
        }
    }
    // A bootstrap request is not an ordinary pull request and the reverse.
    let ordinary = br#"{"models":{"Entry":1},"cursors":{"a":0}}"#;
    assert!(BootstrapRequest::decode(ordinary).is_err());
    assert!(PullRequest::decode(canonical["wire"].as_str().unwrap().as_bytes()).is_err());
}

#[test]
fn bootstrap_page_fixture_cases_decode_as_declared() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/protocol/bootstrap-page.json"
    ))
    .unwrap();
    let canonical = &fixture["canonical"];
    let page = BootstrapPage::decode(canonical["wire"].as_str().unwrap().as_bytes()).unwrap();
    assert_eq!(page.channel, canonical["channel"].as_str().unwrap());
    assert_eq!(page.from, canonical["from"].as_u64().unwrap());
    assert_eq!(page.to, canonical["to"].as_u64().unwrap());
    assert_eq!(page.until, canonical["until"].as_u64().unwrap());
    assert_eq!(page.head, canonical["head"].as_u64().unwrap());
    assert_eq!(
        page.records.len(),
        canonical["records"].as_u64().unwrap() as usize
    );
    assert_eq!(page.records[2].error.as_deref(), Some("loader.failed"));
    assert!(page.records[2].is_error() && page.records[2].state.is_null());
    assert_eq!(page.terminal(), canonical["terminal"].as_bool().unwrap());
    assert_eq!(
        String::from_utf8(page.encode().unwrap()).unwrap(),
        canonical["wire"].as_str().unwrap(),
        "the canonical bytes are stable"
    );
    for case in fixture["page"].as_array().unwrap() {
        let decoded = BootstrapPage::decode(case["wire"].as_str().unwrap().as_bytes());
        assert_eq!(
            decoded.is_ok(),
            case["valid"].as_bool().unwrap(),
            "{}: {decoded:?}",
            case["name"]
        );
        if let Ok(page) = decoded {
            assert_eq!(
                BootstrapPage::decode(&page.encode().unwrap()).unwrap(),
                page,
                "{}",
                case["name"]
            );
            assert_eq!(
                page.terminal(),
                case["terminal"].as_bool().unwrap(),
                "{}",
                case["name"]
            );
        }
    }
    // One page carries at most the shared per-scan limit.
    let record =
        |i: usize| json!({"model":"Entry","identity":{"id":i.to_string()},"stamp":1,"state":null});
    let over = fixture["overLimit"]["records"].as_u64().unwrap() as usize;
    assert_eq!(over, limits::PULL_CHANGES + 1);
    let wire = |count: usize| {
        json!({"mode":"bootstrap","channel":"a","from":0,"to":50,"until":100,"head":100,
               "records":(0..count).map(record).collect::<Vec<_>>()})
        .to_string()
    };
    assert!(BootstrapPage::decode(wire(limits::PULL_CHANGES).as_bytes()).is_ok());
    assert!(BootstrapPage::decode(wire(over).as_bytes()).is_err());
    // An ordinary pull page and a bootstrap page never decode as each other.
    assert!(PullPage::decode(canonical["wire"].as_str().unwrap().as_bytes()).is_err());
}

#[test]
fn a_bootstrap_page_answers_only_the_request_it_continues() {
    let request = |after: u64, until: u64| BootstrapRequest {
        channel: "a".into(),
        models: [("Entry".to_string(), 1)].into(),
        after,
        until,
    };
    let page = |channel: &str, from: u64, to: u64, until: u64| BootstrapPage {
        channel: channel.into(),
        from,
        to,
        until,
        head: 200,
        records: vec![],
    };
    let asked = request(40, 100);
    // A terminal page and a nonterminal page that advanced both answer it.
    let terminal = page("a", 40, 100, 100);
    assert!(terminal.answers(&asked) && terminal.terminal());
    let nonterminal = page("a", 40, 60, 100);
    assert!(nonterminal.answers(&asked) && !nonterminal.terminal());
    // Another channel, another origin, or another starting point is not an
    // answer to this request.
    assert!(!page("b", 40, 100, 100).answers(&asked), "another channel");
    assert!(!page("a", 40, 100, 120).answers(&asked), "another origin");
    assert!(!page("a", 0, 100, 100).answers(&asked), "another `from`");
    // A nonterminal page must advance: repeating `from` would loop the client.
    let stalled = page("a", 40, 40, 100);
    assert!(
        !stalled.terminal() && !stalled.answers(&asked),
        "no progress"
    );
    // `to == from` is fine when that finishes the interval.
    let exhausted = request(100, 100);
    let empty = page("a", 100, 100, 100);
    assert!(empty.answers(&exhausted) && empty.terminal());
    // Backwards progress is refused by `validate` and by `answers`.
    let backwards = BootstrapPage {
        to: 30,
        ..page("a", 40, 30, 100)
    };
    assert!(!backwards.answers(&asked));
    assert!(backwards.validate().is_err());
}

fn kind_schema(action: Value) -> Result<Schema> {
    Schema::from_value(json!({
        "enums":[],
        "models":[{"name":"Todo","identity":["id"],"fields":[{"name":"id","type":{"kind":"scalar","name":"string"},"nullable":false}]}],
        "resultModels":[{"name":"Todo","version":1,"identity":["id"],"fields":[{"name":"id","type":{"kind":"scalar","name":"string"},"nullable":false}],"enums":[]}],
        "actions":[action],
    }))
}

#[test]
fn operation_kind_defaults_to_mutation_and_round_trips_explicitly() {
    let legacy = kind_schema(json!({"name":"Ping","version":1,"inputs":[],"outputs":[]})).unwrap();
    assert_eq!(legacy.action("Ping", 1).unwrap().kind, CallKind::Mutation);
    let explicit =
        kind_schema(json!({"name":"Ping","version":1,"kind":"mutation","inputs":[],"outputs":[]}))
            .unwrap();
    assert_eq!(explicit.action("Ping", 1).unwrap().kind, CallKind::Mutation);
    let query = kind_schema(json!({"name":"Find","version":1,"kind":"query","inputs":[
        {"kind":"value","name":"text","type":{"kind":"scalar","name":"string"},"nullable":true,"list":false}
    ],"outputs":[
        {"name":"todos","kind":"model","model":"Todo","modelReadVersion":1,"cardinality":"list","source":"handlerIdentity","handlerType":{"kind":"identity","model":"Todo","fields":[{"name":"id","type":{"kind":"scalar","name":"string"}}]}}
    ]}))
    .unwrap();
    assert_eq!(query.action("Find", 1).unwrap().kind, CallKind::Query);
    let written = serde_json::to_value(&query).unwrap();
    assert_eq!(written["actions"][0]["kind"], "query");
    // The kind is a typed field, never a leftover policy entry.
    assert!(!query.action("Find", 1).unwrap().policy.contains_key("kind"));
    let reopened: Schema = serde_json::from_value(written).unwrap();
    reopened.validate().unwrap();
    assert_eq!(reopened.action("Find", 1).unwrap().kind, CallKind::Query);
    assert_eq!(
        serde_json::to_value(&legacy).unwrap()["actions"][0]["kind"],
        "mutation"
    );
}

#[test]
fn unknown_operation_kinds_are_rejected() {
    for kind in [json!("action"), json!("Query"), json!(true), Value::Null] {
        assert!(
            kind_schema(json!({"name":"Ping","version":1,"kind":kind,"inputs":[],"outputs":[]}))
                .is_err(),
            "{kind}"
        );
    }
}

#[test]
fn hand_written_query_descriptors_cannot_declare_business_effects() {
    for operation in ["create", "update", "delete"] {
        let mut input = json!({"kind":"model","name":"todo","model":"Todo","operation":operation,"cardinality":"single"});
        if operation == "update" {
            input["allowedPatchFields"] = json!([]);
        }
        let output_kind = if operation == "delete" {
            "deleteIdentity"
        } else {
            "model"
        };
        let mut output = json!({"name":"todo","kind":output_kind,"model":"Todo","cardinality":"single","source":{"inputIdentity":"todo"}});
        if operation != "delete" {
            output["modelReadVersion"] = json!(1);
        }
        let action =
            json!({"name":"Edit","version":1,"kind":"query","inputs":[input],"outputs":[output]});
        let error = kind_schema(action.clone()).unwrap_err().to_string();
        assert!(error.contains("Model operand"), "{operation}: {error}");
        let mut mutation = action;
        mutation["kind"] = json!("mutation");
        kind_schema(mutation).unwrap();
    }
    let sequenced = json!({"name":"Find","version":1,"kind":"query","inputs":[],"outputs":[],
        "sequence":{"after":[{"name":"Find","arguments":{}}]}});
    let error = kind_schema(sequenced).unwrap_err().to_string();
    assert!(error.contains("sequence"), "{error}");
    kind_schema(
        json!({"name":"Find","version":1,"kind":"query","inputs":[],"outputs":[],"sequence":null}),
    )
    .unwrap();
}

fn with_default(ty: Value, nullable: bool, create_default: Value) -> Result<Schema> {
    Schema::from_value(
        json!({"enums":[{"name":"Mood","values":["calm","busy"]}],"models":[{
            "name":"Entry","identity":["id"],"fields":[
                {"name":"id","type":{"kind":"scalar","name":"uuid"},"nullable":false},
                {"name":"value","type":ty,"nullable":nullable,"createDefault":create_default}
            ]
        }]}),
    )
}

#[test]
fn create_default_descriptors_are_validated_by_kind_and_field_type() {
    let scalar = |name: &str| json!({"kind":"scalar","name":name});
    let accepted = [
        (scalar("uuid"), json!({"kind":"uuid"})),
        (scalar("string"), json!({"kind":"uuid"})),
        (scalar("dateTime"), json!({"kind":"now"})),
        (scalar("string"), json!({"kind":"literal","value":""})),
        (scalar("int"), json!({"kind":"literal","value":0})),
        (scalar("float"), json!({"kind":"literal","value":1.5})),
        (scalar("boolean"), json!({"kind":"literal","value":true})),
        (
            scalar("dateTime"),
            json!({"kind":"literal","value":"2026-01-02T03:04:05.000Z"}),
        ),
        (
            json!({"kind":"enum","name":"Mood"}),
            json!({"kind":"literal","value":"busy"}),
        ),
    ];
    for (ty, create_default) in accepted {
        let schema = with_default(ty.clone(), false, create_default.clone())
            .unwrap_or_else(|e| panic!("{ty} {create_default}: {e}"));
        // The metadata round-trips through the stored descriptor.
        let field = &serde_json::to_value(&schema).unwrap()["models"][0]["fields"][1];
        assert_eq!(field["createDefault"], create_default);
        assert!(field.get("default").is_none());
    }
    let rejected = [
        (scalar("int"), json!({"kind":"uuid"})),
        (scalar("dateTime"), json!({"kind":"uuid"})),
        (scalar("string"), json!({"kind":"now"})),
        (scalar("int"), json!({"kind":"literal","value":"1"})),
        (scalar("int"), json!({"kind":"literal","value":1.5})),
        (scalar("uuid"), json!({"kind":"literal","value":"nope"})),
        (
            json!({"kind":"enum","name":"Mood"}),
            json!({"kind":"literal","value":"sad"}),
        ),
        (scalar("string"), json!({"kind":"literal","value":null})),
        (scalar("string"), json!({"kind":"literal"})),
        (scalar("string"), json!({"kind":"cuid"})),
        (scalar("string"), json!({"kind":"uuid","value":"x"})),
        (scalar("string"), json!({"value":"x"})),
        (
            json!({"kind":"list","element":{"kind":"scalar","name":"string"}}),
            json!({"kind":"literal","value":["a"]}),
        ),
    ];
    for (ty, create_default) in rejected {
        assert!(
            with_default(ty.clone(), false, create_default.clone()).is_err(),
            "{ty} {create_default} must be rejected"
        );
    }
    // A nullable field may carry a non-null default.
    with_default(
        scalar("string"),
        true,
        json!({"kind":"literal","value":"x"}),
    )
    .unwrap();
    // Absent metadata stays compatible.
    schema();
}

const FETCH_CALL: &str = "123e4567-e89b-42d3-a456-426614174000";

fn fetch_request(schema: &Schema, raw: Value) -> Result<FetchRequest> {
    FetchRequest::decode(raw.to_string().as_bytes(), schema)
}

fn entry_fetch(store: Option<bool>) -> FetchRequest {
    let mut raw = json!({"callId":FETCH_CALL,"model":"Entry","version":1,"identity":{"id":ID}});
    if let Some(store) = store {
        raw["store"] = json!(store);
    }
    fetch_request(&schema(), raw).unwrap()
}

fn fetch_response(request: &FetchRequest, schema: &Schema, raw: Value) -> Result<FetchResponse> {
    FetchResponse::decode(raw.to_string().as_bytes(), request, schema)
}

fn fetch_success(result: Value, records: Value) -> Value {
    json!({"completion":{"callId":FETCH_CALL,"outcome":{"status":"succeeded","result":result}},"records":records})
}

fn entry_row() -> Value {
    json!({"id":ID,"text":"a","note":null,"count":1})
}

fn entry_record(stamp: u64, state: Value) -> Value {
    json!({"model":"Entry","identity":{"id":ID.to_lowercase()},"stamp":stamp,"state":state})
}

#[test]
fn fetch_defaults_to_storage_without_an_action() {
    let raw = serde_json::json!({
        "callId": "123e4567-e89b-42d3-a456-426614174000",
        "model": "Entry", "version": 1, "identity": {"id": ID}
    });
    let request = FetchRequest::decode(raw.to_string().as_bytes(), &schema()).unwrap();
    assert!(request.store);
    assert_eq!(
        request.identity,
        serde_json::json!({"id": ID.to_lowercase()})
    );
    let encoded: serde_json::Value = serde_json::from_slice(&request.encode().unwrap()).unwrap();
    assert!(encoded.get("store").is_none());
}

#[test]
fn fetch_request_carries_only_a_boolean_storage_policy() {
    let default = entry_fetch(None);
    assert_eq!(default, entry_fetch(Some(true)));
    assert_eq!(
        default.encode().unwrap(),
        entry_fetch(Some(true)).encode().unwrap()
    );
    let preview = entry_fetch(Some(false));
    assert!(!preview.store);
    assert_eq!(preview.call_id, FETCH_CALL);
    assert_eq!((preview.model.as_str(), preview.version), ("Entry", 1));
    let wire: Value = serde_json::from_slice(&preview.encode().unwrap()).unwrap();
    assert_eq!(
        wire,
        json!({"callId":FETCH_CALL,"model":"Entry","version":1,"identity":{"id":ID.to_lowercase()},"store":false})
    );
    assert_eq!(
        FetchRequest::decode(&preview.encode().unwrap(), &schema()).unwrap(),
        preview
    );
    assert_ne!(default.encode().unwrap(), preview.encode().unwrap());
}

#[test]
fn fetch_request_refuses_invalid_envelopes_identities_and_options() {
    let schema = schema();
    let base = json!({"callId":FETCH_CALL,"model":"Entry","version":1,"identity":{"id":ID}});
    assert!(fetch_request(&schema, base.clone()).is_ok());
    let with = |field: &str, value: Value| {
        let mut raw = base.clone();
        raw[field] = value;
        raw
    };
    let without = |field: &str| {
        let mut raw = base.clone();
        raw.as_object_mut().unwrap().remove(field);
        raw
    };
    let cases = [
        ("missing identity", without("identity")),
        ("missing identity field", with("identity", json!({}))),
        (
            "extra identity field",
            with("identity", json!({"id":ID,"text":"a"})),
        ),
        (
            "unknown identity field",
            with("identity", json!({"key":ID})),
        ),
        ("wrong identity type", with("identity", json!({"id":7}))),
        (
            "invalid identity UUID",
            with("identity", json!({"id":"bad"})),
        ),
        ("null identity field", with("identity", json!({"id":null}))),
        ("identity not an object", with("identity", json!(ID))),
        ("missing callId", without("callId")),
        ("invalid callId", with("callId", json!("not-a-uuid"))),
        ("numeric callId", with("callId", json!(7))),
        ("missing model", without("model")),
        ("empty model", with("model", json!(""))),
        ("unknown Model", with("model", json!("Missing"))),
        ("missing version", without("version")),
        ("zero version", with("version", json!(0))),
        ("fractional version", with("version", json!(1.5))),
        ("string version", with("version", json!("1"))),
        ("unsupported version", with("version", json!(2))),
        ("object store", with("store", json!({"Entry":false}))),
        ("empty object store", with("store", json!({}))),
        ("null store", with("store", Value::Null)),
        ("string store", with("store", json!("false"))),
        ("unknown member", with("once", json!(true))),
        ("not an object", json!([base.clone()])),
    ];
    for (name, raw) in cases {
        assert!(
            fetch_request(&schema, raw.clone()).is_err(),
            "{name}: {raw}"
        );
    }
    let mut oversized = base.clone();
    oversized["identity"]["id"] = json!("x".repeat(limits::PUSH_BYTES));
    assert!(
        FetchRequest::decode(oversized.to_string().as_bytes(), &schema)
            .unwrap_err()
            .to_string()
            .contains("byte limit")
    );
}

#[test]
fn fetch_composite_identities_normalize_to_one_key() {
    let schema = Schema::from_value(json!({"enums":[],"models":[{"name":"Book","version":3,"identity":["slug","edition"],"fields":[
        {"name":"slug","type":{"kind":"scalar","name":"string"},"nullable":false},
        {"name":"edition","type":{"kind":"scalar","name":"int"},"nullable":false},
        {"name":"title","type":{"kind":"scalar","name":"string"},"nullable":false}
    ]}]}))
    .unwrap();
    let request = |identity: &str| {
        let raw = format!(
            r#"{{"identity":{identity},"version":3,"model":"Book","callId":"{FETCH_CALL}"}}"#
        );
        FetchRequest::decode(raw.as_bytes(), &schema)
    };
    let first = request(r#"{"slug":"x","edition":1}"#).unwrap();
    let second = request(r#"{"edition":1.0,"slug":"x"}"#).unwrap();
    assert_eq!(first.identity, json!({"slug":"x","edition":1}));
    assert_eq!(
        canonical_json(&first.identity).unwrap(),
        canonical_json(&second.identity).unwrap()
    );
    assert_eq!(first.encode().unwrap(), second.encode().unwrap());
    assert_ne!(
        first.identity,
        request(r#"{"slug":"x","edition":2}"#).unwrap().identity
    );
    for bad in [
        r#"{"slug":"x"}"#,
        r#"{"slug":"x","edition":1,"title":"t"}"#,
        r#"{"slug":"x","edition":"1"}"#,
    ] {
        assert!(request(bad).is_err(), "{bad}");
    }
}

#[test]
fn fetch_response_stores_exactly_the_requested_complete_row() {
    let schema = schema();
    let request = entry_fetch(None);
    let raw = fetch_success(
        json!({"id":ID,"text":"a","note":null,"count":1.0}),
        json!([entry_record(4, json!({"text":"a","note":null,"count":1}))]),
    );
    let response = fetch_response(&request, &schema, raw).unwrap();
    assert_eq!(response.completion.call_id, FETCH_CALL);
    assert_eq!(
        success_result(&response.completion),
        &json!({"id":ID.to_lowercase(),"text":"a","note":null,"count":1})
    );
    assert_eq!(response.records.len(), 1);
    assert_eq!(response.records[0].stamp, 4);
    assert_eq!(response.records[0].identity, request.identity);
    assert_eq!(
        response.records[0].state,
        json!({"text":"a","note":null,"count":1})
    );
    let reopened = FetchResponse::decode(&response.encode().unwrap(), &request, &schema).unwrap();
    assert_eq!(reopened.completion, response.completion);
    assert_eq!(reopened.records, response.records);
}

#[test]
fn fetch_response_absence_is_null_with_stamped_null_authority_only_when_storing() {
    let schema = schema();
    let stored = entry_fetch(None);
    let response = fetch_response(
        &stored,
        &schema,
        fetch_success(Value::Null, json!([entry_record(9, Value::Null)])),
    )
    .unwrap();
    assert_eq!(success_result(&response.completion), &Value::Null);
    assert_eq!(response.records.len(), 1);
    assert_eq!(response.records[0].stamp, 9);
    assert!(response.records[0].state.is_null());
    assert!(fetch_response(&stored, &schema, fetch_success(Value::Null, json!([]))).is_err());

    let preview = entry_fetch(Some(false));
    let response =
        fetch_response(&preview, &schema, fetch_success(Value::Null, json!([]))).unwrap();
    assert_eq!(success_result(&response.completion), &Value::Null);
    assert!(response.records.is_empty());
    assert!(fetch_response(&preview, &schema, fetch_success(entry_row(), json!([]))).is_ok());
    for records in [
        json!([entry_record(9, Value::Null)]),
        json!([entry_record(9, json!({"text":"a","note":null,"count":1}))]),
    ] {
        assert!(
            fetch_response(
                &preview,
                &schema,
                fetch_success(entry_row(), records.clone())
            )
            .is_err(),
            "{records}"
        );
    }
}

#[test]
fn fetch_refusal_has_no_records_and_keeps_its_code() {
    let schema = schema();
    for store in [true, false] {
        let request = entry_fetch(Some(store));
        let failed = |code: &str, execution: &str, records: Value| json!({"completion":{"callId":FETCH_CALL,"outcome":{"status":"failed","code":code,"execution":execution}},"records":records});
        let response = fetch_response(
            &request,
            &schema,
            failed("todo.forbidden", "rejected", json!([])),
        )
        .unwrap();
        assert_eq!(
            response.completion.outcome,
            ActionOutcome::Failed {
                code: "todo.forbidden".into(),
                execution: ExecutionState::Rejected
            }
        );
        assert!(response.records.is_empty());
        for bad in [
            failed(
                "todo.forbidden",
                "rejected",
                json!([entry_record(2, Value::Null)]),
            ),
            failed("todo.forbidden", "unknown", json!([])),
            failed("Not A Code", "rejected", json!([])),
        ] {
            assert!(
                fetch_response(&request, &schema, bad.clone()).is_err(),
                "{bad}"
            );
        }
    }
}

#[test]
fn fetch_response_refuses_foreign_or_disagreeing_authority_before_exposure() {
    let schema = schema();
    let request = entry_fetch(None);
    let state = json!({"text":"a","note":null,"count":1});
    let other = "01890f47-1234-7123-8123-123456789abd";
    let with_call = |call: &str| {
        let mut raw = fetch_success(entry_row(), json!([entry_record(4, state.clone())]));
        raw["completion"]["callId"] = json!(call);
        raw
    };
    assert!(fetch_response(&request, &schema, with_call(FETCH_CALL)).is_ok());
    let mut other_model = entry_record(4, state.clone());
    other_model["model"] = json!("Other");
    let mut other_identity = entry_record(4, state.clone());
    other_identity["identity"] = json!({"id":other});
    let mut read_failure = entry_record(4, Value::Null);
    read_failure["error"] = json!("loader.failed");
    let cases = [
        ("mismatched call ID", with_call(other)),
        (
            "noncanonical call ID",
            with_call(&FETCH_CALL.to_uppercase()),
        ),
        ("missing authority", fetch_success(entry_row(), json!([]))),
        (
            "duplicate authority",
            fetch_success(
                entry_row(),
                json!([
                    entry_record(4, state.clone()),
                    entry_record(4, state.clone())
                ]),
            ),
        ),
        (
            "additional authority",
            fetch_success(
                entry_row(),
                json!([entry_record(4, state.clone()), {"model":"Entry","identity":{"id":other},"stamp":4,"state":state}]),
            ),
        ),
        (
            "wrong Model authority",
            fetch_success(entry_row(), json!([other_model])),
        ),
        (
            "wrong identity authority",
            fetch_success(entry_row(), json!([other_identity])),
        ),
        (
            "record error",
            fetch_success(entry_row(), json!([read_failure])),
        ),
        (
            "zero stamp",
            fetch_success(entry_row(), json!([entry_record(0, state.clone())])),
        ),
        (
            "content disagreement",
            fetch_success(
                entry_row(),
                json!([entry_record(4, json!({"text":"b","note":null,"count":1}))]),
            ),
        ),
        (
            "null result with content authority",
            fetch_success(Value::Null, json!([entry_record(4, state.clone())])),
        ),
        (
            "row result with null authority",
            fetch_success(entry_row(), json!([entry_record(4, Value::Null)])),
        ),
        (
            "identity inside authority state",
            fetch_success(
                entry_row(),
                json!([entry_record(
                    4,
                    json!({"id":ID,"text":"a","note":null,"count":1})
                )]),
            ),
        ),
        (
            "result for another identity",
            fetch_success(
                json!({"id":other,"text":"a","note":null,"count":1}),
                json!([entry_record(4, state.clone())]),
            ),
        ),
        (
            "invalid result value",
            fetch_success(
                json!({"id":ID,"text":7,"note":null,"count":1}),
                json!([entry_record(4, state.clone())]),
            ),
        ),
        (
            "result missing a required field",
            fetch_success(
                json!({"id":ID,"note":null,"count":1}),
                json!([entry_record(4, json!({"note":null,"count":1}))]),
            ),
        ),
        (
            "named output object",
            fetch_success(
                json!({"entry":entry_row()}),
                json!([entry_record(4, state.clone())]),
            ),
        ),
        (
            "missing records",
            json!({"completion":with_call(FETCH_CALL)["completion"]}),
        ),
        ("missing completion", json!({"records":[]})),
    ];
    for (name, raw) in cases {
        assert!(
            fetch_response(&request, &schema, raw.clone()).is_err(),
            "{name}: {raw}"
        );
    }
    let mut oversized = with_call(FETCH_CALL);
    oversized["padding"] = json!("x".repeat(limits::PUSH_BYTES));
    assert!(
        FetchResponse::decode(oversized.to_string().as_bytes(), &request, &schema)
            .unwrap_err()
            .to_string()
            .contains("byte limit")
    );
}

fn result_fixture_schema() -> Value {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/protocol/action-results.json"
    ))
    .unwrap();
    fixture["schema"].clone()
}

#[test]
fn fetch_uses_the_requested_retained_read_version() {
    let schema = Schema::from_value(result_fixture_schema()).unwrap();
    let id = "01890f47-1234-7123-8123-123456789abc";
    let request = |version: u64| {
        fetch_request(
            &schema,
            json!({"callId":FETCH_CALL,"model":"Todo","version":version,"identity":{"id":id}}),
        )
    };
    for unsupported in [3, 99] {
        assert!(request(unsupported).is_err(), "v{unsupported}");
    }
    // v1 is retained with {id,title}; v2 is the local Model with {id,title,done}.
    let retained = request(1).unwrap();
    let local = request(2).unwrap();
    let v1_record = json!([{"model":"Todo","identity":{"id":id},"stamp":2,"state":{"title":"A"}}]);
    let v2_record =
        json!([{"model":"Todo","identity":{"id":id},"stamp":2,"state":{"title":"A","done":false}}]);
    let decoded = fetch_response(
        &retained,
        &schema,
        fetch_success(json!({"id":id,"title":"A"}), v1_record.clone()),
    )
    .unwrap();
    assert_eq!(
        success_result(&decoded.completion),
        &json!({"id":id,"title":"A"})
    );
    let decoded = fetch_response(
        &local,
        &schema,
        fetch_success(json!({"id":id,"title":"A","done":false}), v2_record.clone()),
    )
    .unwrap();
    assert_eq!(
        success_result(&decoded.completion),
        &json!({"id":id,"title":"A","done":false})
    );
    assert!(
        fetch_response(
            &local,
            &schema,
            fetch_success(json!({"id":id,"title":"A"}), v1_record)
        )
        .is_err(),
        "a v1 snapshot cannot satisfy a v2 read"
    );
}

#[test]
fn fetch_snapshot_follows_same_version_compatible_field_rules() {
    let mut raw = result_fixture_schema();
    let fields = raw["resultModels"][0]["fields"].as_array_mut().unwrap();
    fields.push(json!({"name":"note","type":{"kind":"scalar","name":"string"},"nullable":true}));
    fields.push(json!({"name":"flag","type":{"kind":"scalar","name":"boolean"},"nullable":false,"default":true}));
    let schema = Schema::from_value(raw).unwrap();
    let id = "01890f47-1234-7123-8123-123456789abc";
    let request = fetch_request(
        &schema,
        json!({"callId":FETCH_CALL,"model":"Todo","version":1,"identity":{"id":id}}),
    )
    .unwrap();
    let record =
        |state: Value| json!([{"model":"Todo","identity":{"id":id},"stamp":3,"state":state}]);
    // An older same-version server omits the added fields; a newer one adds
    // a field this contract does not know. Both describe one snapshot.
    let complete = json!({"id":id,"title":"A","note":null,"flag":true});
    for (result, state) in [
        (json!({"id":id,"title":"A"}), json!({"title":"A"})),
        (
            json!({"id":id,"title":"A","extra":1}),
            json!({"title":"A","extra":1}),
        ),
        (
            json!({"id":id,"title":"A"}),
            json!({"title":"A","note":null,"flag":true}),
        ),
    ] {
        let decoded = fetch_response(
            &request,
            &schema,
            fetch_success(result.clone(), record(state.clone())),
        )
        .unwrap();
        assert_eq!(success_result(&decoded.completion), &complete, "{result}");
        assert_eq!(
            decoded.records[0].state,
            json!({"title":"A","note":null,"flag":true}),
            "{state}"
        );
    }
    let explicit = fetch_response(
        &request,
        &schema,
        fetch_success(
            json!({"id":id,"title":"A","note":"n","flag":false}),
            record(json!({"title":"A","note":"n","flag":false})),
        ),
    )
    .unwrap();
    assert_eq!(
        success_result(&explicit.completion),
        &json!({"id":id,"title":"A","note":"n","flag":false})
    );
    // A compatible default never hides disagreement or a missing required field.
    for (result, state) in [
        (
            json!({"id":id,"title":"A"}),
            json!({"title":"A","flag":false}),
        ),
        (json!({"id":id}), json!({})),
        (
            json!({"id":id,"title":"A","note":7}),
            json!({"title":"A","note":7}),
        ),
    ] {
        assert!(
            fetch_response(
                &request,
                &schema,
                fetch_success(result.clone(), record(state.clone()))
            )
            .is_err(),
            "{result} {state}"
        );
    }
}

fn channel_fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../../fixtures/protocol/channel-membership.json"
    ))
    .unwrap()
}

#[test]
fn channel_membership_pages_decode_as_declared_and_never_as_record_only_pages() {
    let fixture = channel_fixture();
    assert_eq!(fixture["capability"], CHANNEL_MEMBERSHIP_CAPABILITY);
    let canonical = &fixture["canonical"];
    let page = ChannelPullPage::decode(canonical["wire"].as_str().unwrap().as_bytes()).unwrap();
    assert_eq!(
        json!(page.channels().collect::<Vec<_>>()),
        canonical["channels"]
    );
    assert_eq!(
        json!(
            page.changes
                .iter()
                .map(ChannelChange::kind)
                .collect::<Vec<_>>()
        ),
        canonical["kinds"]
    );
    match &page.changes[0] {
        ChannelChange::Upsert {
            channel,
            cursor,
            record,
        } => {
            assert_eq!((channel.as_str(), *cursor), ("U", 2));
            assert_eq!(
                record,
                &AuthorityRecord {
                    model: "Entry".into(),
                    identity: json!({"id":"a"}),
                    stamp: 1,
                    state: json!({"text":"A"}),
                    error: None,
                }
            );
        }
        other => panic!("expected an upsert: {other:?}"),
    }
    assert_eq!(
        page.changes[1],
        ChannelChange::Remove {
            channel: "U".into(),
            cursor: 3,
            key: RecordKey {
                model: "Entry".into(),
                identity: json!({"id":"b"}),
            },
        }
    );
    assert_eq!(
        String::from_utf8(page.encode().unwrap()).unwrap(),
        canonical["wire"].as_str().unwrap(),
        "the canonical bytes are stable"
    );
    let cases = fixture["page"].as_array().unwrap();
    // The first case is the canonical example as the server may send it.
    assert_eq!(
        ChannelPullPage::decode(cases[0]["wire"].as_str().unwrap().as_bytes()).unwrap(),
        page
    );
    for case in cases {
        let wire = case["wire"].as_str().unwrap().as_bytes();
        let decoded = ChannelPullPage::decode(wire);
        assert_eq!(
            decoded.is_ok(),
            case["valid"].as_bool().unwrap(),
            "{}: {decoded:?}",
            case["name"]
        );
        if let Ok(page) = decoded {
            assert_eq!(
                ChannelPullPage::decode(&page.encode().unwrap()).unwrap(),
                page,
                "{}",
                case["name"]
            );
            if !page.changes.is_empty() {
                assert!(
                    PullPage::decode(wire).is_err(),
                    "a channel page never passes the record-only decoder: {}",
                    case["name"]
                );
            }
        }
    }
}

#[test]
fn channel_changes_flatten_authority_and_removals_carry_only_identity() {
    let upsert = ChannelChange::Upsert {
        channel: "U".into(),
        cursor: 2,
        record: AuthorityRecord {
            model: "Entry".into(),
            identity: json!({"id":"a"}),
            stamp: 5,
            state: Value::Null,
            error: Some("loader.failed".into()),
        },
    };
    assert_eq!(
        serde_json::to_value(&upsert).unwrap(),
        json!({"channel":"U","cursor":2,"kind":"upsert","model":"Entry","identity":{"id":"a"},"stamp":5,"state":null,"error":"loader.failed"})
    );
    let remove = ChannelChange::Remove {
        channel: "U".into(),
        cursor: 3,
        key: RecordKey {
            model: "Entry".into(),
            identity: json!({"id":"b"}),
        },
    };
    assert_eq!(
        serde_json::to_value(&remove).unwrap(),
        json!({"channel":"U","cursor":3,"kind":"remove","model":"Entry","identity":{"id":"b"}})
    );
    for change in [&upsert, &remove] {
        let wire = serde_json::to_value(change).unwrap();
        assert_eq!(&ChannelChange::decode(&wire).unwrap(), change);
        assert_eq!(
            &serde_json::from_value::<ChannelChange>(wire).unwrap(),
            change,
            "serde decoding validates like decode"
        );
    }
    assert_eq!((upsert.channel(), upsert.cursor()), ("U", 2));
    assert_eq!(upsert.key().identity, json!({"id":"a"}));
    assert!(upsert.record().is_some() && remove.record().is_none());
    assert_eq!(remove.key().model, "Entry");
    // Serde decoding refuses what `decode` refuses.
    assert!(
        serde_json::from_value::<ChannelChange>(
            json!({"channel":"U","cursor":3,"kind":"remove","model":"Entry","identity":{"id":"b"},"stamp":1})
        )
        .is_err()
    );
    // A non-channel receipt still carries a plain authority record.
    assert!(
        PushReceipt::decode(
            br#"{"clientId":"c","batchSequence":1,"rejections":[],"records":[{"model":"Entry","identity":{"id":"a"},"stamp":1,"state":null}]}"#
        )
        .is_ok()
    );
    assert!(
        PushReceipt::decode(
            br#"{"clientId":"c","batchSequence":1,"rejections":[],"records":[{"channel":"U","cursor":1,"kind":"upsert","model":"Entry","identity":{"id":"a"},"stamp":1,"state":null}]}"#
        )
        .is_err(),
        "authority records name no channel"
    );
}

#[test]
fn channel_pages_hold_at_most_fifty_changes_per_channel() {
    let change = |channel: &str, i: usize| json!({"channel":channel,"cursor":i,"kind":"remove","model":"Entry","identity":{"id":i.to_string()}});
    let page = |counts: &[usize]| {
        let mut cursors = serde_json::Map::new();
        let mut changes = vec![];
        for (c, count) in counts.iter().enumerate() {
            let channel = format!("c{c}");
            cursors.insert(channel.clone(), json!({"from":0,"to":100,"head":100}));
            changes.extend((1..=*count).map(|i| change(&channel, i)));
        }
        json!({"cursors":cursors,"changes":changes}).to_string()
    };
    assert!(ChannelPullPage::decode(page(&[limits::PULL_CHANGES]).as_bytes()).is_ok());
    let err = ChannelPullPage::decode(page(&[limits::PULL_CHANGES + 1]).as_bytes()).unwrap_err();
    assert!(err.to_string().contains("exceeds 50"), "{err}");
    assert!(
        ChannelPullPage::decode(page(&[limits::PULL_CHANGES, limits::PULL_CHANGES]).as_bytes())
            .is_ok(),
        "the cap is per channel"
    );
    assert!(
        ChannelPullPage::decode(page(&[limits::PULL_CHANGES + 1, 0]).as_bytes()).is_err(),
        "an idle channel lends no capacity to another"
    );
    let bootstrap = |count: usize| {
        json!({"mode":"bootstrap","channel":"c0","from":0,"to":100,"until":100,"head":100,
               "changes":(1..=count).map(|i| change("c0", i)).collect::<Vec<_>>()})
        .to_string()
    };
    assert!(ChannelBootstrapPage::decode(bootstrap(limits::PULL_CHANGES).as_bytes()).is_ok());
    assert!(ChannelBootstrapPage::decode(bootstrap(limits::PULL_CHANGES + 1).as_bytes()).is_err());
}

#[test]
fn channel_bootstrap_page_fixture_cases_decode_as_declared() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/protocol/bootstrap-page.json"
    ))
    .unwrap();
    for case in fixture["channelPage"].as_array().unwrap() {
        let wire = case["wire"].as_str().unwrap().as_bytes();
        let decoded = ChannelBootstrapPage::decode(wire);
        assert_eq!(
            decoded.is_ok(),
            case["valid"].as_bool().unwrap(),
            "{}: {decoded:?}",
            case["name"]
        );
        if let Ok(page) = decoded {
            assert_eq!(
                ChannelBootstrapPage::decode(&page.encode().unwrap()).unwrap(),
                page,
                "{}",
                case["name"]
            );
            assert_eq!(page.terminal(), case["terminal"].as_bool().unwrap());
            assert!(
                BootstrapPage::decode(wire).is_err(),
                "never a record-only page: {}",
                case["name"]
            );
        }
    }
    // The page answers the same bounded request a record-only page answers.
    let first = &fixture["channelPage"][0]["wire"];
    let page = ChannelBootstrapPage::decode(first.as_str().unwrap().as_bytes()).unwrap();
    let request = |after: u64, until: u64| BootstrapRequest {
        channel: "project:123".into(),
        models: [("Entry".to_string(), 1)].into(),
        after,
        until,
    };
    assert!(page.answers(&request(40, 100)));
    assert!(!page.answers(&request(0, 100)), "another `from`");
    assert!(!page.answers(&request(40, 120)), "another origin");
    assert_eq!(page.changes.len(), 4);
    assert!(matches!(
        &page.changes[3],
        ChannelChange::Remove { cursor: 99, .. }
    ));
}

#[test]
fn channel_live_frames_decode_as_acknowledgement_or_channel_page() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/protocol/live-messages.json"
    ))
    .unwrap();
    for case in fixture["channelFrame"].as_array().unwrap() {
        let kind = match ChannelLiveMessage::decode(case["wire"].as_str().unwrap().as_bytes()) {
            Ok(ChannelLiveMessage::Acknowledged(_)) => "acknowledged",
            Ok(ChannelLiveMessage::Page(_)) => "page",
            Err(_) => "invalid",
        };
        assert_eq!(kind, case["kind"], "{}", case["name"]);
    }
}

#[test]
fn membership_claims_are_unique_pairs_tied_to_returned_records() {
    let fixture = channel_fixture();
    for case in fixture["claims"].as_array().unwrap() {
        let response = &case["response"];
        let records: Vec<AuthorityRecord> =
            serde_json::from_value(response["records"].clone()).unwrap();
        let decoded = read_memberships(response, &records);
        assert_eq!(
            decoded.is_ok(),
            case["valid"].as_bool().unwrap(),
            "{}: {decoded:?}",
            case["name"]
        );
        if let Ok(claims) = decoded {
            let expected = response
                .get("memberships")
                .map_or(0, |m| m.as_array().unwrap().len());
            assert_eq!(claims.len(), expected, "{}", case["name"]);
            validate_memberships(&claims, &records).unwrap();
            for claim in &claims {
                let wire = serde_json::to_value(claim).unwrap();
                assert_eq!(
                    &serde_json::from_value::<MembershipClaim>(wire).unwrap(),
                    claim
                );
            }
        }
    }
    let claim = MembershipClaim {
        channel: "project:p1".into(),
        cursor: 10,
        model: "Todo".into(),
        identity: json!({"id":"t1"}),
    };
    assert_eq!(
        canonical_json(&serde_json::to_value(&claim).unwrap()).unwrap(),
        r#"{"channel":"project:p1","cursor":10,"identity":{"id":"t1"},"model":"Todo"}"#
    );
    assert_eq!(
        claim.key(),
        RecordKey {
            model: "Todo".into(),
            identity: json!({"id":"t1"}),
        }
    );
    assert!(
        validate_memberships(std::slice::from_ref(&claim), &[]).is_err(),
        "a claim needs its returned record"
    );
}

#[test]
fn capability_negotiation_refuses_with_stable_codes() {
    assert_eq!(CHANNEL_MEMBERSHIP_CAPABILITY, "channel-membership-v1");
    assert_eq!(PROTOCOL_UNSUPPORTED, "protocol.unsupported");
    let fixture = channel_fixture();
    for case in fixture["negotiation"].as_array().unwrap() {
        let wire = case["wire"].as_str().unwrap().as_bytes();
        let outcome = match require_capability(wire, CHANNEL_MEMBERSHIP_CAPABILITY) {
            Ok(()) => "supported",
            Err(refusal) => refusal.code(),
        };
        assert_eq!(outcome, case["outcome"], "{}", case["name"]);
        // Each route's decoder agrees on what is malformed; the capability
        // gate, not the decoder, refuses an absent or unsupported one.
        let Ok(envelope) = serde_json::from_slice::<Value>(wire) else {
            continue;
        };
        let decodes = if envelope.get("type").is_some() {
            SubscribeRequest::decode(wire).is_ok()
        } else if envelope.get("mode").is_some() {
            BootstrapRequest::decode(wire).is_ok()
        } else if envelope.get("cursors").is_some() {
            PullRequest::decode(wire).is_ok()
        } else {
            continue;
        };
        assert_eq!(decodes, outcome != "request.invalid", "{}", case["name"]);
    }
    let refusal =
        require_capability(br#"{"models":{"Entry":1},"cursors":{"a":0}}"#, "x").unwrap_err();
    assert!(matches!(refusal, NegotiationRefusal::Unsupported(_)));
    assert!(
        read_capabilities(&json!({"capabilities":["b","a"]}))
            .unwrap()
            .into_iter()
            .eq(["a", "b"])
    );
    assert!(read_capabilities(&json!({})).unwrap().is_empty());
}

#[test]
fn negotiation_metadata_is_not_part_of_the_logical_request() {
    let caps = [CHANNEL_MEMBERSHIP_CAPABILITY];
    let upgrade = |plain: &[u8]| {
        let upgraded = with_capabilities(plain, &caps).unwrap();
        assert!(require_capability(&upgraded, CHANNEL_MEMBERSHIP_CAPABILITY).is_ok());
        let (plain_value, upgraded_value): (Value, Value) = (
            serde_json::from_slice(plain).unwrap(),
            serde_json::from_slice(&upgraded).unwrap(),
        );
        assert_ne!(plain_value, upgraded_value);
        assert_eq!(
            logical_request(&upgraded_value).unwrap(),
            logical_request(&plain_value).unwrap()
        );
        upgraded
    };
    // The advertised form is canonical envelope bytes.
    let pull = br#"{"models":{"Entry":1},"cursors":{"a":0}}"#;
    assert_eq!(
        String::from_utf8(with_capabilities(pull, &caps).unwrap()).unwrap(),
        r#"{"capabilities":["channel-membership-v1"],"cursors":{"a":0},"models":{"Entry":1}}"#
    );
    assert_eq!(
        PullRequest::decode(&upgrade(pull)).unwrap(),
        PullRequest::decode(pull).unwrap()
    );
    let subscribe = SubscribeRequest::new(vec!["a".into()], [("Task".to_string(), 1)].into())
        .unwrap()
        .encode()
        .unwrap();
    assert_eq!(
        SubscribeRequest::decode(&upgrade(&subscribe)).unwrap(),
        SubscribeRequest::decode(&subscribe).unwrap()
    );
    let bootstrap =
        br#"{"mode":"bootstrap","channel":"a","models":{"Entry":1},"after":0,"until":3}"#;
    assert_eq!(
        BootstrapRequest::decode(&upgrade(bootstrap)).unwrap(),
        BootstrapRequest::decode(bootstrap).unwrap()
    );
    // Saved calls: a retry that now advertises the capability decodes to the
    // same logical request the server fingerprints and stored.
    let load = json!({"loads":[{"loadId":ID,"callId":FETCH_CALL,"name":"ProjectTodos","version":1,
        "args":{},"continuation":null,"models":{"Todo":1}}]})
    .to_string();
    assert_eq!(
        LoadBatchRequest::decode_envelope(&upgrade(load.as_bytes())).unwrap(),
        LoadBatchRequest::decode_envelope(load.as_bytes()).unwrap()
    );
    let fetch =
        json!({"callId":FETCH_CALL,"model":"Entry","version":1,"identity":{"id":ID}}).to_string();
    assert_eq!(
        FetchRequest::decode_envelope(&upgrade(fetch.as_bytes())).unwrap(),
        FetchRequest::decode_envelope(fetch.as_bytes()).unwrap()
    );
    let direct =
        json!({"call":{"callId":ID,"name":"Send","version":1,"args":{"to":"a"}},"models":{}})
            .to_string();
    assert_eq!(
        DirectActionRequest::decode_envelope(&upgrade(direct.as_bytes()))
            .unwrap()
            .encode()
            .unwrap(),
        DirectActionRequest::decode_envelope(direct.as_bytes())
            .unwrap()
            .encode()
            .unwrap()
    );
    let push = json!({"clientId":"device","batchSequence":1,"models":{},
        "mutations":[{"callId":ID,"name":"Send","version":1,"args":{"to":"a"},"ordinal":1}]})
    .to_string();
    assert_eq!(
        PushRequest::decode_action_envelope(&upgrade(push.as_bytes()))
            .unwrap()
            .encode()
            .unwrap(),
        PushRequest::decode_action_envelope(push.as_bytes())
            .unwrap()
            .encode()
            .unwrap(),
        "the frozen batch bytes exclude negotiation"
    );
    // Every request decoder refuses a malformed capabilities member.
    let malformed = |wire: &str| {
        let mut value: Value = serde_json::from_str(wire).unwrap();
        value["capabilities"] = json!("channel-membership-v1");
        value.to_string().into_bytes()
    };
    assert!(PullRequest::decode(&malformed(std::str::from_utf8(pull).unwrap())).is_err());
    assert!(
        SubscribeRequest::decode(&malformed(std::str::from_utf8(&subscribe).unwrap())).is_err()
    );
    assert!(BootstrapRequest::decode(&malformed(std::str::from_utf8(bootstrap).unwrap())).is_err());
    assert!(LoadBatchRequest::decode_envelope(&malformed(&load)).is_err());
    assert!(FetchRequest::decode_envelope(&malformed(&fetch)).is_err());
    assert!(DirectActionRequest::decode_envelope(&malformed(&direct)).is_err());
    assert!(PushRequest::decode_action_envelope(&malformed(&push)).is_err());
    assert!(PushRequest::decode(&malformed(&push)).is_err());
    assert!(logical_request(&json!({"capabilities":7})).is_err());
    assert!(with_capabilities(b"[]", &caps).is_err());
}

#[test]
fn enrollment_responses_carry_memberships_beside_their_records() {
    let fixture = channel_fixture();
    let envelopes = &fixture["envelopes"];
    // The member sits at the envelope's top level and is omitted when empty.
    let frozen = |name: &str, response: &Value, encoded: Value| {
        assert_eq!(
            encoded.get("memberships"),
            response
                .get("memberships")
                .filter(|m| !m.as_array().unwrap().is_empty()),
            "{name}"
        );
    };
    for case in envelopes["loadPage"].as_array().unwrap() {
        let (name, response) = (case["name"].as_str().unwrap(), &case["response"]);
        let decoded = LoadPageResponse::decode_item(response);
        assert_eq!(decoded.is_ok(), case["valid"], "{name}: {decoded:?}");
        if let Ok(page) = decoded {
            let expected = response
                .get("memberships")
                .map_or(0, |m| m.as_array().unwrap().len());
            assert_eq!(page.memberships.len(), expected, "{name}");
            frozen(name, response, serde_json::to_value(&page).unwrap());
        }
    }
    for case in envelopes["pushReceipt"].as_array().unwrap() {
        let (name, response) = (case["name"].as_str().unwrap(), &case["response"]);
        let decoded = PushReceipt::decode(response.to_string().as_bytes());
        assert_eq!(decoded.is_ok(), case["valid"], "{name}: {decoded:?}");
        if let Ok(receipt) = decoded {
            let encoded: Value = serde_json::from_slice(&receipt.encode().unwrap()).unwrap();
            frozen(name, response, encoded);
            assert_eq!(
                PushReceipt::decode(&receipt.encode().unwrap()).unwrap(),
                receipt
            );
        }
    }
    let schema = action_schema();
    let request = DirectActionRequest::decode(
        envelopes["directActionRequest"].to_string().as_bytes(),
        &schema,
    )
    .unwrap();
    for case in envelopes["directAction"].as_array().unwrap() {
        let (name, response) = (case["name"].as_str().unwrap(), &case["response"]);
        let decoded =
            DirectActionResponse::decode(response.to_string().as_bytes(), &request, &schema);
        assert_eq!(decoded.is_ok(), case["valid"], "{name}: {decoded:?}");
        if let Ok(direct) = decoded {
            let encoded: Value = serde_json::from_slice(&direct.encode().unwrap()).unwrap();
            frozen(name, response, encoded);
            assert!(
                DirectActionResponse::decode(&direct.encode().unwrap(), &request, &schema).is_ok(),
                "{name}"
            );
        }
    }
}

fn at_request_limit(mut request: Value, pointer: &str, limit: usize) -> Vec<u8> {
    *request.pointer_mut(pointer).unwrap() = json!("");
    let base = canonical_json(&request).unwrap().len();
    *request.pointer_mut(pointer).unwrap() = json!("x".repeat(limit - base));
    let bytes = canonical_json(&request).unwrap().into_bytes();
    assert_eq!(bytes.len(), limit);
    bytes
}

#[test]
fn frozen_requests_at_payload_limit_accept_required_negotiation() {
    type RequestCase = (Value, &'static str, usize, fn(&[u8]) -> bool);
    let cases: Vec<RequestCase> = vec![
        (
            json!({"clientId":"device","batchSequence":1,"models":{},"mutations":[{"callId":ID,"name":"Send","version":1,"args":{"to":""},"ordinal":1}]}),
            "/mutations/0/args/to",
            limits::PUSH_BYTES,
            |b| PushRequest::decode_action_envelope(b).is_ok(),
        ),
        (
            json!({"call":{"callId":ID,"name":"Send","version":1,"args":{"to":""}},"models":{}}),
            "/call/args/to",
            limits::PUSH_BYTES,
            |b| DirectActionRequest::decode_envelope(b).is_ok(),
        ),
        (
            json!({"callId":FETCH_CALL,"model":"Entry","version":1,"identity":{"id":""}}),
            "/identity/id",
            limits::PUSH_BYTES,
            |b| FetchRequest::decode_envelope(b).is_ok(),
        ),
        (
            json!({"loads":[{"loadId":ID,"callId":FETCH_CALL,"name":"ProjectTodos","version":1,"args":{"to":""},"continuation":null,"models":{"Todo":1}}]}),
            "/loads/0/args/to",
            limits::LOAD_REQUEST_BYTES,
            |b| LoadBatchRequest::decode_envelope(b).is_ok(),
        ),
    ];
    for (request, pointer, limit, decode) in cases {
        let frozen = at_request_limit(request, pointer, limit);
        assert!(
            decode(&frozen),
            "legacy request decodes at its original limit"
        );
        let upgraded = with_capabilities(&frozen, &[CHANNEL_MEMBERSHIP_CAPABILITY]).unwrap();
        assert!(
            decode(&upgraded),
            "required negotiation must not strand a frozen request"
        );
    }
}

#[test]
fn negotiation_headroom_does_not_expand_logical_payload_or_accept_unbounded_metadata() {
    let original = canonical_json(&json!({"body":"x".repeat(100)}))
        .unwrap()
        .into_bytes();
    let limit = original.len();
    let upgraded = with_capabilities(&original, &[CHANNEL_MEMBERSHIP_CAPABILITY]).unwrap();
    assert_eq!(upgraded.len() - limit, 41);
    assert!(check_request_size(&upgraded, limit).is_ok());
    // Even one extra semantic byte cannot borrow the metadata allowance.
    let mut oversized: Value = serde_json::from_slice(&upgraded).unwrap();
    oversized["body"] = json!("x".repeat(101));
    assert!(check_request_size(canonical_json(&oversized).unwrap().as_bytes(), limit).is_err());
    assert!(check_request_size(&original, limit - 1).is_err());
    // A shorter logical payload cannot grant arbitrary negotiation headroom.
    let excessive = with_capabilities(
        &original,
        &[CHANNEL_MEMBERSHIP_CAPABILITY, &"z".repeat(1000)],
    )
    .unwrap();
    assert!(check_request_size(&excessive, limit).is_err());
    for capabilities in [
        json!(null),
        json!(CHANNEL_MEMBERSHIP_CAPABILITY),
        json!([CHANNEL_MEMBERSHIP_CAPABILITY, CHANNEL_MEMBERSHIP_CAPABILITY]),
        json!(["unrecognized"]),
    ] {
        let malformed = json!({"body":"x".repeat(100),"capabilities":capabilities});
        assert!(check_request_size(canonical_json(&malformed).unwrap().as_bytes(), limit).is_err());
    }
    // Every semantic extension remains part of the logical body.
    let extension =
        json!({"body":"x".repeat(100),"extra":true,"capabilities":[CHANNEL_MEMBERSHIP_CAPABILITY]});
    assert!(check_request_size(canonical_json(&extension).unwrap().as_bytes(), limit).is_err());
    let mut whitespace = upgraded;
    whitespace.push(b' ');
    assert!(check_request_size(&whitespace, limit).is_err());
    // This helper keeps the legacy raw-size path; ingress decoders validate shape.
    assert!(check_request_size(b"legacy", 6).is_ok());
}
