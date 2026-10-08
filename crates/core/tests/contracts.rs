use axton_core::*;
use serde_json::{Value, json};
const ID: &str = "01890F47-1234-7123-8123-123456789ABC";
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

fn kind_schema(action: Value) -> Result<Schema> {
    Schema::from_value(json!({
        "enums":[],
        "models":[{"name":"Todo","identity":["id"],"fields":[{"name":"id","type":{"kind":"scalar","name":"string"},"nullable":false}]}],
        "resultModels":[{"name":"Todo","version":1,"identity":["id"],"fields":[{"name":"id","type":{"kind":"scalar","name":"string"},"nullable":false}],"enums":[]}],
        "actions":[action],
    }))
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
fn identities_are_exact_normalized_and_independent_of_streams() {
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

#[test]
fn field_defaults_round_trip_and_axton_prefix_is_rejected() {
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
#[test]
fn action_store_policy_decodes_bool_or_output_map_and_serializes_canonically() {
    let decode = |v: Value| serde_json::from_value::<ActionStore>(v).unwrap();
    assert_eq!(ActionStore::default(), ActionStore::All);
    assert_eq!(decode(json!(true)), ActionStore::All);
    assert_eq!(decode(json!({})), ActionStore::All);
    let disabled = decode(json!(false));
    assert_eq!(disabled, ActionStore::None);
    assert_eq!(serde_json::to_value(&disabled).unwrap(), json!(false));
    let map = decode(json!({"suggestions":false,"mainTodo":true}));
    assert_eq!(
        canonical_json(&serde_json::to_value(&map).unwrap()).unwrap(),
        r#"{"mainTodo":true,"suggestions":false}"#
    );
    assert!(map.selects("mainTodo"));
    assert!(!map.selects("suggestions"));
    assert!(decode(json!({"suggestions":false})).selects("mainTodo"));
    assert!(!disabled.selects("mainTodo"));
    assert!(ActionStore::default().selects("mainTodo"));
    for bad in [
        json!(null),
        json!("false"),
        json!(0),
        json!([]),
        json!({"mainTodo":"no"}),
        json!({"mainTodo":null}),
    ] {
        assert!(serde_json::from_value::<ActionStore>(bad).is_err());
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
        serde_json::from_value::<ActionStore>(good)
            .unwrap()
            .validate(open)
            .unwrap();
    }
    for bad in ["missing", "store", "todo", "deleted"] {
        assert!(
            ActionStore::Outputs([(bad.to_string(), false)].into())
                .validate(open)
                .is_err(),
            "{bad}"
        );
    }
    let eligible: Vec<_> = open
        .outputs
        .iter()
        .filter(|o| store_eligible(o))
        .map(|o| o.name.as_str())
        .collect();
    assert_eq!(eligible, ["mainTodo", "suggestions"]);
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
    assert!(outputs(&[("missing", true)]).validate(open).is_err());
    assert_eq!(
        serde_json::to_value(outputs(&[("mainTodo", true), ("suggestions", false)]).canonical())
            .unwrap(),
        json!({"suggestions":false})
    );
    assert!(outputs(&[("mainTodo", true)]).canonical().wire().is_none());
}
