//! The host operation contract: the shared fixture round-trips through the
//! Rust types, and a malformed request or response is refused per operation.
use axton_server::host::{
    Acknowledged, Claimed, ClaimedCall, Guards, Handled, HandledAction, HandledLoad, Head,
    HostRequest, Invalidation, Loaded, Locked, OPERATIONS, Positions, Scanned, Stamped, Stamps,
    StreamIntent, Tracking,
};
use axton_server::stream_members::{MemberDelta, MemberPosition, PositionKind};
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
        "saveReceipt" | "saveCall" | "savepoint" | "rollback" | "release" | "lockStreams" => {
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
        "readTracking" => round!(Tracking),
        "guardRecords" => round!(Guards),
        "applyStreamMembers" => round!(Positions),
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
                    if !["scan", "readTracking", "applyStreamMembers"].contains(&op) {
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
fn bulk_tracking_and_guard_requests_decode_exactly() {
    for request in [
        json!({"op":"readTracking","records":[{"model":"Todo","identityKey":"{\"id\":\"t\"}"}],"pairs":[]}),
        json!({"op":"guardRecords","records":[{"model":"Todo","identityKey":"{\"id\":\"t\"}","mode":"advance"}]}),
    ] {
        let decoded: HostRequest = serde_json::from_value(request.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), request);
    }
}
#[test]
fn guard_operands_are_exact_canonical_distinct_and_ordered() {
    let row = json!({"model":"Todo","identityKey":"{\"id\":\"t\"}","mode":"advance"});
    for rows in [
        json!([row.clone(), row.clone()]),
        json!([{"model":"Z","identityKey":"{}","mode":"lock"},{"model":"A","identityKey":"{}","mode":"ensure"}]),
        json!([{"model":"Todo","identityKey":"{}","mode":"touch"}]),
        json!([{"model":"Todo","identityKey":"{ \"id\":\"t\"}","mode":"advance"}]),
        json!([{"model":"Todo","identityKey":"[]","mode":"advance"}]),
        json!([{"model":"Todo","identityKey":"{}","mode":"advance","tags":[]}]),
    ] {
        assert!(
            serde_json::from_value::<HostRequest>(json!({"op":"guardRecords","records":rows}))
                .is_err()
        );
    }
    for streams in [
        json!(["B", "A"]),
        json!(["A", "A"]),
        json!([" "]),
        json!([]),
    ] {
        assert!(
            serde_json::from_value::<HostRequest>(json!({"op":"lockStreams","streams":streams}))
                .is_err()
        );
    }
}
#[test]
fn tracking_response_has_only_requested_pairs_without_duplicates() {
    let request:HostRequest=serde_json::from_value(json!({"op":"readTracking","records":[{"model":"Todo","identityKey":"{\"id\":\"t\"}"}],"pairs":[{"stream":"C","model":"Todo","identityKey":"{}"}]})).unwrap();
    let pair = json!({"stream":"A","model":"Todo","identityKey":"{\"id\":\"t\"}"});
    request.validate_response(&json!([pair.clone(),{"stream":"B","model":"Todo","identityKey":"{\"id\":\"t\"}"},{"stream":"C","model":"Todo","identityKey":"{}"}])).unwrap();
    // All-holder completeness is trusted, including an empty answer.
    request.validate_response(&json!([])).unwrap();
    for response in [
        json!([pair.clone(), pair.clone()]),
        json!([{"stream":"D","model":"Todo","identityKey":"{}"}]),
        json!([{"stream":"A","model":"Other","identityKey":"{}"}]),
        json!([{"stream":"A","model":"Todo","identityKey":"{}","tags":[]}]),
    ] {
        assert_eq!(
            request.validate_response(&response).unwrap_err().code,
            "host.invalid"
        );
    }
}
#[test]
fn guard_results_are_aligned_safe_and_mode_specific() {
    let request:HostRequest=serde_json::from_value(json!({"op":"guardRecords","records":[{"model":"A","identityKey":"{}","mode":"advance"},{"model":"B","identityKey":"{}","mode":"ensure"},{"model":"C","identityKey":"{}","mode":"lock"}]})).unwrap();
    request.validate_response(&json!([1, 2, null])).unwrap();
    request.validate_response(&json!([1, 2, 3])).unwrap();
    for response in [
        json!([1, 2]),
        json!([null, 2, null]),
        json!([1, null, null]),
        json!([0, 2, null]),
        json!([9007199254740992u64, 2, null]),
        json!([1, 2, 0]),
        json!([1, 2, -1]),
    ] {
        assert!(request.validate_response(&response).is_err(), "{response}");
    }
}
#[test]
fn declarations_reject_retired_shapes_and_preserve_business_identity() {
    let record = json!({"model":"Scope","identity":{"scope":"unchanged"}});
    let value = json!({"changes":[],"declarations":[{"kind":"track","stream":"User:alice","record":record.clone()},{"kind":"invalidate","streams":null,"record":record.clone()}]});
    let decoded: Handled = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), value);
    for value in [
        json!({"changes":[],"memberships":[]}),
        json!({"changes":[],"declarations":[],"rejection":"not.allowed"}),
        json!({"changes":[],"declarations":[{"kind":"add","scope":"A","record":record,"tags":[]}]}),
        json!({"error":"failure","declarations":[]}),
    ] {
        assert!(serde_json::from_value::<Handled>(value).is_err());
    }
    for intent in [
        json!({"kind":"remove","stream":"A","record":record}),
        json!({"kind":"track","stream":"A","record":record,"tags":[]}),
        json!({"kind":"invalidate","streams":true,"record":record}),
    ] {
        assert!(serde_json::from_value::<StreamIntent>(intent).is_err());
    }
}
#[test]
fn load_answers_tracking_only_beside_data_and_scans_retain_removals() {
    let intent =
        json!({"kind":"track","stream":"A","record":{"model":"Todo","identity":{"id":"t"}}});
    let value = json!({"data":{"todos":[{"id":"t"}]},"next":null,"tracking":[intent]});
    assert_eq!(
        serde_json::to_value(serde_json::from_value::<HandledLoad>(value.clone()).unwrap())
            .unwrap(),
        value
    );
    for value in [
        json!({"data":{},"next":null,"changes":[]}),
        json!({"data":{},"next":null,"memberships":[]}),
        json!({"rejection":"load.no","tracking":[]}),
        json!({"error":"failure","tracking":[]}),
        json!({"data":{},"next":null,"tracking":null}),
    ] {
        assert!(serde_json::from_value::<HandledLoad>(value).is_err());
    }
    let saved = json!({"kind":"remove","stream":"A","cursor":4,"model":"Todo","identity":{"id":"t"},"identityKey":"{\"id\":\"t\"}"});
    assert_eq!(
        serde_json::to_value(serde_json::from_value::<Invalidation>(saved.clone()).unwrap())
            .unwrap(),
        saved
    );
    let position:MemberPosition=serde_json::from_value(json!({"kind":"remove","stream":"A","cursor":4,"model":"Todo","identityKey":"{\"id\":\"t\"}"})).unwrap();
    assert_eq!(position.kind, PositionKind::Remove);
}
#[test]
fn fresh_deltas_are_whole_exact_and_canonical_upserts_only() {
    let valid = json!({"stream":"A","model":"Todo","identity":{"id":"t"},"identityKey":"{\"id\":\"t\"}","publish":true});
    let _: MemberDelta = serde_json::from_value(valid.clone()).unwrap();
    for (field, value) in [
        ("identity", json!({"id":"other"})),
        ("identityKey", json!("[]")),
        ("stream", json!(" ")),
        ("present", json!(false)),
        ("tags", json!([])),
    ] {
        let mut invalid = valid.clone();
        invalid[field] = value;
        assert!(serde_json::from_value::<MemberDelta>(invalid).is_err());
    }
}
#[test]
fn loader_failure_is_distinct_from_absence_and_unknown_fields() {
    assert_eq!(
        serde_json::from_value::<Loaded>(json!([null])).unwrap(),
        Loaded::Rows(vec![None])
    );
    assert!(matches!(
        serde_json::from_value::<Loaded>(json!({"error":"failed"})).unwrap(),
        Loaded::Failed { .. }
    ));
    for value in [
        json!({"error":"failed","rejection":"load.no"}),
        json!({}),
        json!({"rejection":""}),
        json!({"error":"failed","extra":1}),
    ] {
        assert!(serde_json::from_value::<Loaded>(value).is_err());
    }
    for value in [json!(0), json!(-1), json!(9007199254740992u64)] {
        assert!(serde_json::from_value::<Stamped>(value).is_err());
    }
}
#[test]
fn loads_refuse_invalidation_and_global_selection_requires_explicit_null() {
    let record = json!({"model":"Todo","identity":{"id":"t"}});
    assert!(
        serde_json::from_value::<StreamIntent>(json!({"kind":"invalidate","record":record}))
            .is_err()
    );
    assert!(serde_json::from_value::<HandledLoad>(json!({"data":{},"next":null,"tracking":[{"kind":"invalidate","streams":null,"record":record}]})).is_err());
}
