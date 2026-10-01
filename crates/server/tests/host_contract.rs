//! The host operation contract: the shared fixture round-trips through the
//! Rust types, and a malformed request or response is refused per operation.
use axton_server::host::{
    Acknowledged, Claimed, ClaimedCall, Handled, HandledAction, HandledLoad, Head, HostRequest,
    Invalidation, Loaded, Locked, Memberships, OPERATIONS, Positions, RecordRef, Scanned,
    ScopeIntent, ScopeMembers, Stamped, Stamps,
};
use axton_server::scope_members::{MemberDelta, MemberPosition, MemberState, PositionKind};
use serde_json::{Value, json};

fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../../fixtures/protocol/host-operations.json"
    ))
    .expect("fixture is valid JSON")
}

/// Decode one response into the type its operation promises and re-encode it.
fn round_trip_response(op: &str, value: &Value) -> Result<Value, String> {
    macro_rules! round {
        ($type:ty) => {
            serde_json::from_value::<$type>(value.clone())
                .map(|decoded| serde_json::to_value(decoded).expect("re-encodes"))
                .map_err(|error| error.to_string())
        };
    }
    match op {
        "claim" => round!(Claimed),
        "claimCall" => round!(ClaimedCall),
        "saveReceipt" | "saveCall" | "savepoint" | "rollback" | "release" | "lockScopes" => {
            round!(Acknowledged)
        }
        "head" => round!(Head),
        "scan" => round!(Scanned),
        "handle" => round!(Handled),
        "handleAction" => round!(HandledAction),
        "handleLoad" => round!(HandledLoad),
        "load" => round!(Loaded),
        "advanceStamp" | "ensureStamp" => round!(Stamped),
        "readStamps" => round!(Stamps),
        "lockRecord" => round!(Locked),
        "memberships" => round!(Memberships),
        "readScopeMembers" => round!(ScopeMembers),
        "applyScopeMembers" => round!(Positions),
        other => panic!("no response type is wired for {other}"),
    }
}

/// The variants serde itself knows about, read out of its own refusal. Keeps
/// [`OPERATIONS`] honest when a variant is added.
fn variants_serde_accepts() -> Vec<String> {
    let error = serde_json::from_value::<HostRequest>(json!({"op": "\u{0}unknown"}))
        .expect_err("an unknown op is refused")
        .to_string();
    let (_, listed) = error
        .split_once("expected one of ")
        .expect("lists variants");
    listed
        .split(", ")
        .map(|name| name.trim_matches(|c| c == '`' || c == ' ').to_string())
        .filter(|name| !name.is_empty())
        .collect()
}

#[test]
fn the_operation_list_matches_the_request_enum() {
    assert_eq!(variants_serde_accepts(), OPERATIONS);
}

#[test]
fn the_fixture_covers_every_operation_exactly_once() {
    let fixture = fixture();
    let covered: Vec<String> = fixture["operations"]
        .as_array()
        .expect("operations is an array")
        .iter()
        .map(|entry| entry["op"].as_str().expect("op is a string").to_string())
        .collect();
    let mut sorted = covered.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), covered.len(), "an operation appears twice");
    let mut expected: Vec<String> = OPERATIONS.iter().map(|op| op.to_string()).collect();
    expected.sort();
    assert_eq!(sorted, expected);
}

#[test]
fn every_fixture_request_and_response_round_trips() {
    let fixture = fixture();
    for entry in fixture["operations"].as_array().unwrap() {
        let op = entry["op"].as_str().unwrap();
        let request = &entry["request"];
        let decoded: HostRequest = serde_json::from_value(request.clone())
            .unwrap_or_else(|error| panic!("{op} request: {error}"));
        assert_eq!(
            serde_json::to_value(&decoded).unwrap(),
            *request,
            "{op} request does not re-encode to the fixture"
        );
        let responses = entry["responses"].as_array().unwrap();
        assert!(!responses.is_empty(), "{op} has no response example");
        for response in responses {
            let variant = response["variant"].as_str().unwrap();
            let value = &response["value"];
            let encoded = round_trip_response(op, value)
                .unwrap_or_else(|error| panic!("{op}/{variant} response: {error}"));
            assert_eq!(
                encoded, *value,
                "{op}/{variant} response does not re-encode to the fixture"
            );
        }
    }
}

#[test]
fn a_request_missing_a_field_or_carrying_an_unknown_one_is_refused() {
    let fixture = fixture();
    for entry in fixture["operations"].as_array().unwrap() {
        let op = entry["op"].as_str().unwrap();
        let request = entry["request"].as_object().unwrap();
        let mut unknown = request.clone();
        unknown.insert("surprise".into(), json!(1));
        assert!(
            serde_json::from_value::<HostRequest>(Value::Object(unknown)).is_err(),
            "{op} accepted an unknown request field"
        );
        for field in request.keys().filter(|key| *key != "op") {
            let mut missing = request.clone();
            missing.remove(field);
            assert!(
                serde_json::from_value::<HostRequest>(Value::Object(missing)).is_err(),
                "{op} accepted a request without {field}"
            );
            // `arguments`, `identity` and `identities` carry schema-shaped
            // payloads verbatim; the contract constrains every other field.
            if ["arguments", "identity", "identities"].contains(&field.as_str()) {
                continue;
            }
            // `present` is the one boolean field; every other field is not.
            let wrong_value = if field == "present" {
                json!("true")
            } else {
                json!(true)
            };
            let mut wrong = request.clone();
            wrong.insert(field.clone(), wrong_value.clone());
            assert!(
                serde_json::from_value::<HostRequest>(Value::Object(wrong)).is_err(),
                "{op} accepted {wrong_value} as {field}"
            );
        }
    }
    assert!(serde_json::from_value::<HostRequest>(json!({"op": "vacuum"})).is_err());
}

#[test]
fn a_response_carrying_an_unknown_field_is_refused() {
    let fixture = fixture();
    for entry in fixture["operations"].as_array().unwrap() {
        let op = entry["op"].as_str().unwrap();
        for response in entry["responses"].as_array().unwrap() {
            let value = &response["value"];
            let surprised = match value {
                Value::Object(fields) => {
                    let mut fields = fields.clone();
                    fields.insert("surprise".into(), json!(1));
                    Value::Object(fields)
                }
                Value::Array(rows) if !rows.is_empty() => {
                    // `load` entries are opaque record state; `scan`, member
                    // and position rows are not.
                    if !["scan", "readScopeMembers", "applyScopeMembers"].contains(&op) {
                        continue;
                    }
                    let mut rows = rows.clone();
                    let mut first = rows[0].as_object().unwrap().clone();
                    first.insert("surprise".into(), json!(1));
                    rows[0] = Value::Object(first);
                    Value::Array(rows)
                }
                _ => continue,
            };
            assert!(
                round_trip_response(op, &surprised).is_err(),
                "{op} accepted an unknown response field"
            );
        }
    }
}

#[test]
fn a_claimed_call_always_names_its_response_even_when_uncompleted() {
    assert!(
        serde_json::from_value::<ClaimedCall>(json!({"fresh": true, "request": "{}"})).is_err()
    );
    assert_eq!(
        serde_json::from_value::<ClaimedCall>(
            json!({"fresh": true, "request": "{}", "response": null})
        )
        .unwrap()
        .response,
        None
    );
}

#[test]
fn a_response_of_the_wrong_type_is_refused_per_operation() {
    // One clearly wrong answer per operation, in the shape a host might drift into.
    let wrong: [(&str, Value); 20] = [
        ("claim", json!({"clientId":"c","owner":"o","sequence":-1})),
        ("saveReceipt", json!({"saved": true})),
        (
            "claimCall",
            json!({"fresh": true, "request": "{}", "response": 1}),
        ),
        ("saveCall", json!({"saved": true})),
        ("head", json!("7")),
        ("scan", json!({"rows": []})),
        ("savepoint", json!({})),
        ("rollback", json!({})),
        ("release", json!({})),
        ("handle", json!({"scope": "shared"})),
        ("handleLoad", json!({"next": null})),
        ("load", json!({"0": null})),
        ("advanceStamp", json!("4")),
        ("ensureStamp", json!(0)),
        ("readStamps", json!([1, 0])),
        ("lockRecord", json!(0)),
        ("memberships", json!(["shared", "shared"])),
        ("lockScopes", json!(["shared"])),
        (
            "readScopeMembers",
            json!([{"model": "Task", "identityKey": "{\"id\":\"t-1\"}"}]),
        ),
        (
            "applyScopeMembers",
            json!([{"scope": "shared", "model": "Task", "identityKey": "{\"id\":\"t-1\"}",
                "cursor": 0, "kind": "upsert"}]),
        ),
    ];
    for (op, value) in wrong {
        assert!(
            round_trip_response(op, &value).is_err(),
            "{op} accepted {value}"
        );
    }
}

#[test]
fn a_handle_response_carries_changes_and_memberships_or_a_rejection_and_never_both() {
    let task = |id: &str| RecordRef {
        model: "Task".into(),
        identity: json!({"id": id}),
    };
    let add = |scope: &str| ScopeIntent::Add {
        scope: scope.into(),
        record: task("t-2"),
        tags: vec![],
    };
    let remove = |scope: &str| ScopeIntent::Remove {
        scope: scope.into(),
        record: task("t-2"),
    };
    assert_eq!(
        serde_json::from_value::<Handled>(json!({
            "changes": [{"model":"Task","identity":{"id":"t-2"}}],
            "memberships": [
                {"kind":"add","scope":"shared","record":{"model":"Task","identity":{"id":"t-2"}},"tags":[]},
                {"kind":"remove","scope":"other","record":{"model":"Task","identity":{"id":"t-2"}}}
            ]
        }))
        .unwrap(),
        Handled::Settled {
            changes: vec![task("t-2")],
            memberships: vec![add("shared"), remove("other")]
        }
    );
    assert_eq!(
        serde_json::from_value::<Handled>(json!({"changes": [], "memberships": []})).unwrap(),
        Handled::Settled {
            changes: vec![],
            memberships: vec![]
        },
        "a handler that changed nothing beyond its operations and enrolled nothing"
    );
    assert_eq!(
        serde_json::from_value::<Handled>(json!({"rejection": "task.refused"})).unwrap(),
        Handled::Rejected {
            rejection: "task.refused".into()
        }
    );
    let both = serde_json::from_value::<Handled>(
        json!({"changes": [], "memberships": [], "rejection": "task.refused"}),
    )
    .unwrap_err()
    .to_string();
    assert!(both.contains("not several"), "{both}");
    assert_eq!(
        serde_json::from_value::<Handled>(json!({"error": "boom"})).unwrap(),
        Handled::Failed {
            error: "boom".into()
        }
    );
    for refused in [
        json!({"error": 1}),
        json!({"error": "boom", "rejection": "x"}),
        json!({"error": "boom", "changes": [], "memberships": []}),
    ] {
        assert!(
            serde_json::from_value::<Handled>(refused.clone()).is_err(),
            "accepted {refused}"
        );
    }
    let none = serde_json::from_value::<Handled>(json!({}))
        .unwrap_err()
        .to_string();
    assert!(none.contains("invalid handler settlement"), "{none}");
    let member = |fields: Value| {
        let mut intent = json!({"kind":"add","scope":"shared","record":{"model":"Task","identity":{"id":"t"}},"tags":[]});
        for (name, value) in fields.as_object().unwrap() {
            if value.is_null() {
                intent.as_object_mut().unwrap().remove(name);
            } else {
                intent[name] = value.clone();
            }
        }
        json!({"changes": [], "memberships": [intent]})
    };
    for refused in [
        json!({}),
        json!({"changes": []}),
        json!({"memberships": []}),
        json!({"scope": "shared"}),
        json!({"changes": null, "memberships": []}),
        json!({"changes": [], "memberships": null}),
        json!({"changes": [{"model":"","identity":{}}], "memberships": []}),
        json!({"changes": [{"model":"Task","identity":"t"}], "memberships": []}),
        // The retired publication shape: no implicit publish-all remains.
        json!({"changes": [], "publications": []}),
        json!({"changes": [], "memberships": [], "publications": [{"scope":"shared"}]}),
        member(json!({"scope": ""})),
        member(json!({"scope": null})),
        member(json!({"record": {"model":"","identity":{"id":"t"}}})),
        member(json!({"record": {"identity":{"id":"t"}}})),
        member(json!({"record": {"model":"Task","identity":"t"}})),
        member(json!({"record": {"model":"Task"}})),
        member(json!({"record": {"model":"Task","identity":{"id":"t"},"extra":1}})),
        member(json!({"record": null})),
        member(json!({"kind": null})),
        member(json!({"kind": "Add"})),
        member(json!({"kind": "present"})),
        member(json!({"tags": null})),
        member(json!({"tags": "X"})),
        member(json!({"tags": [1]})),
        // An add names tags, not a selector, and nothing of the retired shape.
        member(json!({"tag": "X"})),
        member(json!({"present": true})),
        member(json!({"records": []})),
        // A removal carries no tags; a tag selector names one tag and no record.
        member(json!({"kind": "remove", "tags": []})),
        member(json!({"kind": "removeTag", "record": null, "tags": null})),
        member(json!({"kind": "removeTag", "record": null, "tags": null, "tag": 1})),
        member(json!({"kind": "removeTag", "tags": null, "tag": "X"})),
        member(json!({"kind": "removeTag", "record": null, "tags": null, "tag": "X", "scope": ""})),
        // The retired boolean shape.
        json!({"changes": [], "memberships": [{"scope":"shared","model":"Task","identity":{"id":"t"},"present":true}]}),
        json!({"rejection": null}),
        json!({"rejection": "Not A Code"}),
        json!({"settled": "shared"}),
    ] {
        assert!(
            serde_json::from_value::<Handled>(refused.clone()).is_err(),
            "accepted {refused}"
        );
        if refused.get("changes").is_some() {
            let mut action = refused.clone();
            action["outputs"] = json!({});
            assert!(
                serde_json::from_value::<HandledAction>(action.clone()).is_err(),
                "an Action handler answer accepted {action}"
            );
        }
    }
}

#[test]
fn a_load_response_is_rows_or_a_refusal_code() {
    assert_eq!(
        serde_json::from_value::<Loaded>(json!([{"id":"t-1"}, null])).unwrap(),
        Loaded::Rows(vec![Some(json!({"id":"t-1"})), None])
    );
    assert_eq!(
        serde_json::from_value::<Loaded>(json!({"rejection":"task.forbidden"})).unwrap(),
        Loaded::Refused {
            rejection: "task.forbidden".into()
        }
    );
    assert_eq!(
        serde_json::from_value::<Loaded>(json!({"error": "boom"})).unwrap(),
        Loaded::Failed {
            error: "boom".into()
        }
    );
    for refused in [
        json!({}),
        json!({"rejection": ""}),
        json!({"rejection": "Not A Code"}),
        json!({"rows": []}),
        json!(null),
        json!({"error": 1}),
        json!({"error": "boom", "rejection": "x"}),
    ] {
        assert!(
            serde_json::from_value::<Loaded>(refused.clone()).is_err(),
            "accepted {refused}"
        );
    }
}

#[test]
fn counters_keep_their_range_and_name_themselves() {
    let row = |stamp: Value| {
        let mut row = json!({"scope":"a","cursor":1,"model":"Entry","identity":{"id":"e"},"identityKey":"{\"id\":\"e\"}"});
        row["stamp"] = stamp;
        serde_json::from_value::<Invalidation>(row).map_err(|error| error.to_string())
    };
    assert_eq!(row(json!(7)).unwrap().stamp, 7);
    // `read_counter`'s tolerance for integral JSON numbers is preserved.
    assert_eq!(row(json!(7.0)).unwrap().stamp, 7);
    for bad in [json!(0), json!(-1), json!(1.5), json!(9007199254740992u64)] {
        let error = row(bad.clone()).unwrap_err();
        assert!(error.contains("stamp"), "{bad}: {error}");
    }
    assert_eq!(
        serde_json::from_value::<Head>(json!(0)).unwrap(),
        Head(0),
        "a head of zero is a real answer"
    );
    assert!(serde_json::from_value::<Head>(json!(-1)).is_err());
    assert_eq!(
        serde_json::from_value::<Claimed>(
            json!({"clientId":"c","owner":"o","sequence":0,"receipt":null})
        )
        .unwrap()
        .receipt,
        None
    );
    assert_eq!(
        serde_json::from_value::<Claimed>(json!({"clientId":"c","owner":"o","sequence":0}))
            .unwrap()
            .receipt,
        None,
        "an absent receipt has always meant the same as a null one"
    );
}

#[test]
fn an_unusable_response_names_its_operation_and_ordinal() {
    let handle = HostRequest::Handle {
        name: "edit".into(),
        version: 1,
        arguments: json!({}),
        owner: "alice".into(),
        ordinal: 3,
    };
    let error = handle.invalid_response("invalid handler settlement");
    assert_eq!(error.code, axton_server::code::HANDLER_INVALID);
    assert_eq!(
        error.message,
        "handle(ordinal 3) response invalid: invalid handler settlement"
    );
    let load = HostRequest::Load {
        model: "Task".into(),
        version: 1,
        identities: vec![],
        owner: "alice".into(),
    };
    assert_eq!(
        load.invalid_response("x").code,
        axton_server::code::LOADER_INVALID
    );
    assert_eq!(
        HostRequest::Claim {
            owner: "alice".into(),
            client_id: "c".into()
        }
        .invalid_response("x")
        .code,
        axton_server::code::STORAGE_INVALID
    );
    assert_eq!(
        HostRequest::Head {
            scope: "shared".into()
        }
        .invalid_response("x")
        .code,
        axton_server::code::HOST_INVALID
    );
    assert_eq!(
        HostRequest::AdvanceStamp {
            model: "Task".into(),
            identity_key: "{\"id\":\"t-1\"}".into()
        }
        .invalid_response("x")
        .code,
        axton_server::code::HOST_INVALID
    );
    let record = || ("Task".to_string(), "{\"id\":\"t-1\"}".to_string());
    for request in [
        HostRequest::LockRecord {
            model: record().0,
            identity_key: record().1,
        },
        HostRequest::Memberships {
            model: record().0,
            identity_key: record().1,
        },
        HostRequest::LockScopes {
            scopes: vec!["shared".into()],
        },
        HostRequest::ReadScopeMembers {
            scope: "shared".into(),
            explicit_keys: vec![],
            all: false,
            tags: vec![],
        },
        HostRequest::ApplyScopeMembers { deltas: vec![] },
    ] {
        let error = request.invalid_response("x");
        assert_eq!(error.code, axton_server::code::HOST_INVALID);
        assert!(
            error.message.starts_with(&request.label()),
            "{}",
            error.message
        );
    }
}

/// Every intent kind decodes in declaration order, tags as spelled, and
/// encodes back to the same wire.
#[test]
fn scope_intents_decode_every_kind_in_declaration_order() {
    let wire = json!({"changes": [], "memberships": [
        {"kind":"add","scope":"U","record":{"model":"Task","identity":{"id":"a"}},"tags":["X"," Y"]},
        {"kind":"select","scope":"U","predicate":{"tags":{"any":["X"]}},"action":{"kind":"remove"}},
        {"kind":"add","scope":"U","record":{"model":"Task","identity":{"id":"b"}},"tags":[]},
        {"kind":"remove","scope":"V","record":{"model":"Task","identity":{"id":"a"}}}
    ]});
    let task = |id: &str| RecordRef {
        model: "Task".into(),
        identity: json!({"id": id}),
    };
    let decoded = serde_json::from_value::<Handled>(wire.clone()).unwrap();
    assert_eq!(
        decoded,
        Handled::Settled {
            changes: vec![],
            memberships: vec![
                ScopeIntent::Add {
                    scope: "U".into(),
                    record: task("a"),
                    tags: vec!["X".into(), " Y".into()],
                },
                ScopeIntent::Select {
                    scope: "U".into(),
                    model: None,
                    predicate: serde_json::from_value(json!({"tags":{"any":["X"]}})).unwrap(),
                    action: axton_server::scope_members::SelectionAction::Remove
                },
                ScopeIntent::Add {
                    scope: "U".into(),
                    record: task("b"),
                    tags: vec![],
                },
                ScopeIntent::Remove {
                    scope: "V".into(),
                    record: task("a"),
                },
            ],
        }
    );
    assert_eq!(serde_json::to_value(&decoded).unwrap(), wire);
}

/// Scope requests are canonical: `lockScopes` names distinct valid
/// Scopes in byte order, the one lock order; `readScopeMembers` names a
/// valid Scope, canonical record keys and distinct valid tags in byte order.
#[test]
fn scope_requests_are_canonical() {
    let decode = |request: Value| {
        serde_json::from_value::<HostRequest>(request).map_err(|error| error.to_string())
    };
    assert_eq!(
        decode(json!({"op": "lockScopes", "scopes": ["Other", "a b", "shared"]})).unwrap(),
        HostRequest::LockScopes {
            scopes: vec!["Other".into(), "a b".into(), "shared".into()]
        }
    );
    for (scopes, detail) in [
        (json!([]), "no Scope"),
        (json!(["shared", "other"]), "canonical byte order"),
        (json!(["shared", "shared"]), "distinct"),
        (json!(["shared", " "]), "scope"),
        (json!("shared"), "invalid type"),
    ] {
        let error = decode(json!({"op": "lockScopes", "scopes": scopes})).unwrap_err();
        assert!(error.contains(detail), "{scopes}: {error}");
    }
    let read = |fields: Value| {
        let mut request = json!({"op": "readScopeMembers", "scope": "shared",
            "explicitKeys": [{"model": "Task", "identityKey": "{\"id\":\"t-1\"}"}], "tags": ["X", "Y"]});
        for (name, value) in fields.as_object().unwrap() {
            request[name] = value.clone();
        }
        decode(request)
    };
    assert!(read(json!({})).is_ok());
    assert!(read(json!({"explicitKeys": [], "tags": []})).is_ok());
    for (fields, detail) in [
        (json!({"scope": "  "}), "scope"),
        (json!({"tags": ["Y", "X"]}), "canonical byte order"),
        (json!({"tags": ["X", "X"]}), "distinct"),
        (json!({"tags": ["\u{feff}"]}), "blank"),
        (json!({"tags": ["x".repeat(257)]}), "256"),
        (
            json!({"explicitKeys": [{"model": "Task", "identityKey": "{ \"id\": \"t-1\" }"}]}),
            "not canonical",
        ),
        (
            json!({"explicitKeys": [{"model": "Task", "identityKey": "\"t-1\""}]}),
            "not an object",
        ),
        (
            json!({"explicitKeys": [{"model": "", "identityKey": "{\"id\":\"t-1\"}"}]}),
            "no Model",
        ),
        (
            json!({"explicitKeys": [{"model": "Task", "identity": {"id": "t-1"}}]}),
            "unknown field",
        ),
    ] {
        let error = read(fields.clone()).unwrap_err();
        assert!(error.contains(detail), "{fields}: {error}");
    }
    for op in ["lockRecord", "memberships"] {
        assert!(
            serde_json::from_value::<HostRequest>(
                json!({"op": op, "model": "Task", "identityKey": "{}", "scope": "shared"})
            )
            .is_err(),
            "{op} names a record, never a scope"
        );
    }
}

/// A member answers its complete tags, as a set; a delta is a final state
/// whose removal carries no tags and always publishes; a position names its
/// pair, a positive safe cursor and its kind. Every record key is canonical.
#[test]
fn members_deltas_and_positions_decode_only_whole_and_canonical() {
    let key = || json!({"model": "Task", "identityKey": "{\"id\":\"t-1\"}"});
    let with = |base: Value, fields: Value| {
        let mut value = base;
        for (name, field) in fields.as_object().unwrap() {
            if field.is_null() {
                value.as_object_mut().unwrap().remove(name);
            } else {
                value[name] = field.clone();
            }
        }
        value
    };
    let member = |fields: Value| {
        serde_json::from_value::<MemberState>(with(
            with(key(), json!({"tags": ["Y", "X"]})),
            fields,
        ))
        .map_err(|error| error.to_string())
    };
    let decoded = member(json!({})).unwrap();
    assert_eq!(
        decoded.tags.iter().map(String::as_str).collect::<Vec<_>>(),
        ["X", "Y"],
        "tags are a set in any answered order, held in byte order"
    );
    assert_eq!(decoded.key.identity, json!({"id": "t-1"}));
    for (fields, detail) in [
        (json!({"tags": null}), "missing field `tags`"),
        (json!({"tags": ["X", "X"]}), "duplicate tag"),
        (json!({"tags": [" "]}), "blank"),
        (json!({"identityKey": "{\"id\": \"t-1\"}"}), "not canonical"),
        (json!({"model": null}), "missing field `model`"),
        (json!({"stamp": 1}), "unknown field"),
    ] {
        let error = member(fields.clone()).unwrap_err();
        assert!(error.contains(detail), "{fields}: {error}");
    }
    let delta = |fields: Value| {
        let base = json!({"scope": "shared", "model": "Task", "identity": {"id": "t-1"},
            "identityKey": "{\"id\":\"t-1\"}", "present": true, "tags": ["X"], "publish": false});
        serde_json::from_value::<MemberDelta>(with(base, fields)).map_err(|error| error.to_string())
    };
    assert!(delta(json!({})).is_ok());
    assert!(delta(json!({"present": false, "tags": [], "publish": true})).is_ok());
    for (fields, detail) in [
        (json!({"present": false, "publish": true}), "no tags"),
        (json!({"present": false, "tags": []}), "always publishes"),
        (json!({"identity": {"id": "t-2"}}), "different records"),
        (json!({"scope": ""}), "scope"),
        (json!({"publish": null}), "missing field `publish`"),
    ] {
        let error = delta(fields.clone()).unwrap_err();
        assert!(error.contains(detail), "{fields}: {error}");
    }
    let position = |fields: Value| {
        let base = json!({"scope": "shared", "model": "Task",
            "identityKey": "{\"id\":\"t-1\"}", "cursor": 5, "kind": "remove"});
        serde_json::from_value::<MemberPosition>(with(base, fields))
            .map_err(|error| error.to_string())
    };
    assert_eq!(position(json!({})).unwrap().kind, PositionKind::Remove);
    for (fields, detail) in [
        (json!({"kind": "delete"}), "unknown variant"),
        (json!({"kind": null}), "missing field `kind`"),
        (json!({"cursor": 0}), "cursor"),
        (json!({"cursor": 9007199254740992u64}), "cursor"),
        (json!({"scope": " "}), "scope"),
        (json!({"identity": {"id": "t-1"}}), "unknown field"),
    ] {
        let error = position(fields.clone()).unwrap_err();
        assert!(error.contains(detail), "{fields}: {error}");
    }
}

#[test]
fn a_lock_answers_a_safe_positive_stamp_or_null_for_an_absent_record() {
    let lock =
        |value: Value| serde_json::from_value::<Locked>(value).map_err(|error| error.to_string());
    assert_eq!(lock(json!(4)).unwrap(), Some(Stamped(4)));
    assert_eq!(
        lock(json!(null)).unwrap(),
        None,
        "a lock never creates a row"
    );
    assert_eq!(
        lock(json!(9007199254740991u64)).unwrap(),
        Some(Stamped(9007199254740991))
    );
    for bad in [
        json!(0),
        json!(-1),
        json!(1.5),
        json!(9007199254740992u64),
        json!("4"),
        json!({"stamp": 4}),
    ] {
        let error = lock(bad.clone()).unwrap_err();
        assert!(
            error.contains("stamp") || error.contains("invalid type"),
            "{bad}: {error}"
        );
    }
}

#[test]
fn memberships_are_unique_valid_scopes_held_in_canonical_order() {
    let decode = |value: Value| {
        serde_json::from_value::<Memberships>(value).map_err(|error| error.to_string())
    };
    assert!(decode(json!([])).unwrap().0.is_empty());
    // A database collation may order differently from bytes; the answer is a
    // set, so the Rust side holds it in canonical byte order either way.
    let members = decode(json!(["shared", "Other", "a b"])).unwrap();
    assert_eq!(
        members.0.iter().map(String::as_str).collect::<Vec<_>>(),
        ["Other", "a b", "shared"]
    );
    assert_eq!(
        serde_json::to_value(&members).unwrap(),
        json!(["Other", "a b", "shared"])
    );
    for (bad, detail) in [
        (json!(["shared", "shared"]), "duplicate"),
        (json!([""]), "scope"),
        (json!([" "]), "scope"),
        (json!([1]), "invalid type"),
        (json!(null), "invalid type"),
        (json!("shared"), "invalid type"),
    ] {
        let error = decode(bad.clone()).unwrap_err();
        assert!(error.contains(detail), "{bad}: {error}");
    }
}

#[test]
fn a_load_handler_answers_identity_data_and_a_continuation_and_never_changes() {
    let decode = |value: Value| serde_json::from_value::<HandledLoad>(value);
    assert_eq!(
        decode(json!({"data": {"tasks": []}, "next": {"state": null}})).unwrap(),
        HandledLoad::Settled {
            data: json!({"tasks": []}),
            next: Some(json!({"state": null})),
            memberships: vec![],
        },
        "a null state is a continuation, not the end"
    );
    // `data` and `next` are carried as answered: the engine judges them, so
    // a missing or malformed wrapper is the page's `load.invalid_continuation`
    // and malformed data its `handler.invalid`, whatever the host bridge.
    for (answer, next) in [
        (json!({"data": {}}), None),
        (json!({"data": {}, "next": {}}), Some(json!({}))),
        (json!({"data": {}, "next": 1}), Some(json!(1))),
        (json!({"data": [], "next": null}), Some(Value::Null)),
    ] {
        let decoded = decode(answer.clone()).unwrap();
        assert_eq!(
            decoded,
            HandledLoad::Settled {
                data: answer["data"].clone(),
                next,
                memberships: vec![],
            }
        );
    }
    for refused in [
        json!({"data": {}, "next": null, "changes": []}),
        json!({"data": {}, "next": null, "changes": [], "memberships": []}),
        json!({"data": {}, "next": null, "outputs": {}}),
        json!({"next": null}),
        json!({"data": {}, "next": null, "rejection": "tasks.refused"}),
        json!({"rejection": "Not A Code"}),
        json!({"error": 7}),
        json!({}),
    ] {
        assert!(decode(refused.clone()).is_err(), "accepted {refused}");
    }
    let request: HostRequest =
        serde_json::from_value(json!({"op":"handleLoad","name":"Tasks","version":1,
        "arguments":{},"continuation":null,"owner":"alice","callId":"c","loadId":"l"}))
        .unwrap();
    assert!(matches!(
        request,
        HostRequest::HandleLoad {
            continuation: None,
            ..
        }
    ));
    let error = serde_json::from_value::<HandledLoad>(json!({"next": null}))
        .map_err(|error| request.invalid_response(error))
        .unwrap_err();
    assert_eq!(error.code, axton_server::code::HANDLER_INVALID);
    let stamps = HostRequest::ReadStamps {
        model: "Task".into(),
        identity_keys: vec![],
    };
    assert_eq!(
        stamps.invalid_response("x").code,
        axton_server::code::HOST_INVALID
    );
}

#[test]
fn a_load_handler_answer_may_carry_membership_intents_and_nothing_else() {
    let decode = |value: Value| serde_json::from_value::<HandledLoad>(value);
    let intent = json!({"kind": "add", "scope": "shared", "record": {"model": "Task", "identity": {"id": "t-1"}}, "tags": []});
    assert_eq!(
        decode(json!({"data": {"tasks": [{"id": "t-1"}]}, "next": null, "memberships": [intent]}))
            .unwrap(),
        HandledLoad::Settled {
            data: json!({"tasks": [{"id": "t-1"}]}),
            next: Some(Value::Null),
            memberships: vec![ScopeIntent::Add {
                scope: "shared".into(),
                record: RecordRef {
                    model: "Task".into(),
                    identity: json!({"id": "t-1"}),
                },
                tags: vec![],
            }],
        }
    );
    // An older host answers no member at all; an explicit empty list is the
    // same answer. Both encode without the member, as older hosts wrote it.
    for answer in [
        json!({"data": {}, "next": null}),
        json!({"data": {}, "next": null, "memberships": []}),
    ] {
        let decoded = decode(answer.clone()).unwrap();
        assert_eq!(
            decoded,
            HandledLoad::Settled {
                data: json!({}),
                next: Some(Value::Null),
                memberships: vec![],
            },
            "{answer}"
        );
        assert_eq!(
            serde_json::to_value(decoded).unwrap(),
            json!({"data": {}, "next": null})
        );
    }
    // A removal and a tag selector decode: the engine, not the wire, refuses
    // them for a Load, so every host's answer fails its page the same way.
    let removal = json!({"kind": "remove", "scope": "shared", "record": {"model": "Task", "identity": {"id": "t-1"}}});
    let selector = json!({"kind":"select","scope":"shared","predicate":{"tags":{"any":["X"]}},"action":{"kind":"remove"}});
    assert!(decode(json!({"data": {}, "next": null, "memberships": [removal, selector]})).is_ok());
    // Nor does the wire check coverage (`t-1` is not in this page's data), and
    // memberships leave the continuation as answered: a missing or malformed
    // `next` is still the engine's `load.invalid_continuation` to judge.
    for (answer, next) in [
        (json!({"data": {}, "memberships": [intent]}), None),
        (
            json!({"data": {}, "next": 1, "memberships": [intent]}),
            Some(json!(1)),
        ),
    ] {
        match decode(answer.clone()).unwrap() {
            HandledLoad::Settled {
                next: decoded,
                memberships,
                ..
            } => {
                assert_eq!(decoded, next, "{answer}");
                assert_eq!(memberships.len(), 1, "{answer}");
            }
            other => panic!("{answer} decoded as {other:?}"),
        }
    }
    for refused in [
        json!({"data": {}, "next": null, "memberships": null}),
        json!({"data": {}, "next": null, "memberships": {}}),
        json!({"data": {}, "next": null, "memberships": "shared"}),
        json!({"data": {}, "next": null, "memberships": [1]}),
        json!({"data": {}, "next": null, "memberships": [{"scope": "shared", "model": "Task", "identity": {"id": "t-1"}}]}),
        json!({"data": {}, "next": null, "memberships": [{"scope": "shared", "model": "Task", "identity": {"id": "t-1"}, "present": true}]}),
        json!({"data": {}, "next": null, "memberships": [{"kind": "add", "scope": "shared", "record": {"model": "Task", "identity": {"id": "t-1"}}}]}),
        json!({"data": {}, "next": null, "memberships": [{"kind": "add", "scope": "shared", "record": {"model": "Task", "identity": {"id": "t-1"}}, "tags": [], "extra": 1}]}),
        json!({"data": {}, "next": null, "memberships": [{"kind": "add", "scope": "", "record": {"model": "Task", "identity": {"id": "t-1"}}, "tags": []}]}),
        json!({"data": {}, "next": null, "memberships": [{"kind": "add", "scope": "shared", "record": {"model": "", "identity": {"id": "t-1"}}, "tags": []}]}),
        json!({"data": {}, "next": null, "memberships": [{"kind": "add", "scope": "shared", "record": {"model": "Task", "identity": "t-1"}, "tags": []}]}),
        json!({"data": {}, "next": null, "memberships": [intent], "changes": []}),
        json!({"data": {}, "next": null, "memberships": [intent], "surprise": 1}),
        json!({"memberships": [intent], "rejection": "tasks.refused"}),
        json!({"memberships": [intent], "error": "boom"}),
        json!({"memberships": [], "rejection": "tasks.refused"}),
        json!({"memberships": [], "error": "boom"}),
        json!({"data": {}, "next": null, "memberships": [intent], "rejection": "tasks.refused"}),
        json!({"memberships": [intent]}),
    ] {
        assert!(decode(refused.clone()).is_err(), "accepted {refused}");
    }
}

#[test]
fn a_legacy_scan_row_without_kind_decodes_as_upsert() {
    let row: axton_server::host::Invalidation = serde_json::from_value(json!({"scope":"c","cursor":1,"model":"Task","identity":{"id":"t"},"identityKey":"{\"id\":\"t\"}","stamp":1})).unwrap();
    assert_eq!(row.kind, axton_server::scope_members::PositionKind::Upsert);
}

#[test]
fn scope_intents_and_all_candidate_mode_round_trip_strictly() {
    use axton_server::host::ScopeIntent;
    for value in [
        json!({"kind":"tagAdd","scope":"U","record":{"model":"Task","identity":{"id":"A"}},"tags":["X"]}),
        json!({"kind":"tagRemove","scope":"U","record":{"model":"Task","identity":{"id":"A"}},"tags":["X"]}),
        json!({"kind":"detachTags","scope":"U","tags":["X"]}),
        json!({"kind":"select","scope":"U","model":"Task","predicate":{"tags":{"only":[]}},"action":{"kind":"remove"}}),
    ] {
        let intent: ScopeIntent = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(intent).unwrap(), value);
    }
    let baseline = json!({"op":"readScopeMembers","scope":"U","explicitKeys":[],"tags":[]});
    let request: HostRequest = serde_json::from_value(baseline.clone()).unwrap();
    assert_eq!(serde_json::to_value(request).unwrap(), baseline);
    let mut all = baseline.clone();
    all["all"] = json!(true);
    assert_eq!(
        serde_json::to_value(serde_json::from_value::<HostRequest>(all.clone()).unwrap()).unwrap(),
        all
    );
    for value in [json!(null), json!("true"), json!(1)] {
        let mut bad = baseline.clone();
        bad["all"] = value;
        assert!(serde_json::from_value::<HostRequest>(bad).is_err());
    }
    for predicate in [
        json!({"tags":null}),
        json!({"and":null}),
        json!({"or":null}),
        json!({"not":null}),
        json!({"tags":{"all":null}}),
        json!({"unknown":true}),
    ] {
        assert!(serde_json::from_value::<ScopeIntent>(json!({"kind":"select","scope":"U","predicate":predicate,"action":{"kind":"remove"}})).is_err());
    }
}
