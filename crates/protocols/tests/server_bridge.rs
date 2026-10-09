use axton_protocols::server_bridge::{GuardMode, HandledAction, HostRequest, Loaded, OPERATIONS};
use serde_json::json;
#[test]
fn guard_presence_is_stamp_free_and_ordered() {
    let r:HostRequest=serde_json::from_value(json!({"op":"guardRecords","records":[{"model":"A","identityKey":"{}","mode":"ensure"},{"model":"B","identityKey":"{}","mode":"lock"}]})).unwrap();
    assert!(r.validate_response(&json!([true, false])).is_ok());
    for answer in [
        json!([false, false]),
        json!([1, null]),
        json!([true]),
        json!([true, null]),
    ] {
        assert!(r.validate_response(&answer).is_err());
    }
    let HostRequest::GuardRecords { records } = r else {
        unreachable!()
    };
    assert_eq!(records[0].mode, GuardMode::Ensure);
    for records in [
        json!([{"model":"A","identityKey":"{}","mode":"advance"}]),
        json!([{"model":"B","identityKey":"{}","mode":"lock"},{"model":"A","identityKey":"{}","mode":"ensure"}]),
        json!([{"model":"A","identityKey":"{}","mode":"ensure"},{"model":"A","identityKey":"{}","mode":"lock"}]),
    ] {
        assert!(
            serde_json::from_value::<HostRequest>(json!({"op":"guardRecords","records":records}))
                .is_err()
        );
    }
}
#[test]
fn handler_context_is_authenticated_server_local_carrier() {
    let r:HostRequest=serde_json::from_value(json!({"op":"handleAction","name":"Write","version":1,"arguments":{},"owner":"alice","callId":"s:1:1","ordinal":1,"context":{"owner":"alice","stream":"User:alice","storeId":"s","materialization":"m"}})).unwrap();
    assert_eq!(
        serde_json::to_value(r).unwrap()["context"],
        json!({"owner":"alice","stream":"User:alice","storeId":"s","materialization":"m"})
    );
    assert!(!OPERATIONS.iter().any(|op| op.contains("Stamp")
        || op.contains("Manifest")
        || *op == "claimCall"
        || *op == "handleLoad"));
}
#[test]
fn unrelated_and_duplicate_tracking_answers_are_refused() {
    let r:HostRequest=serde_json::from_value(json!({"op":"readTracking","records":[{"model":"Entry","identityKey":"{\"id\":\"a\"}"}],"pairs":[]})).unwrap();
    let pair = json!({"stream":"User:a","model":"Entry","identityKey":"{\"id\":\"a\"}"});
    assert!(r.validate_response(&json!([pair.clone()])).is_ok());
    assert!(r.validate_response(&json!([pair.clone(), pair])).is_err());
    assert!(
        r.validate_response(
            &json!([{"stream":"User:a","model":"Entry","identityKey":"{\"id\":\"b\"}"}])
        )
        .is_err()
    );
}
#[test]
fn handlers_and_loaders_reject_ambiguous_outcomes_and_invalid_words() {
    for answer in [
        json!({"outputs":{},"changes":[],"declarations":[],"rejection":"write.no"}),
        json!({"rejection":"write.no","error":"failed"}),
        json!({"rejection":"Bad Code"}),
    ] {
        assert!(serde_json::from_value::<HandledAction>(answer).is_err());
    }
    assert!(
        serde_json::from_value::<HandledAction>(
            json!({"outputs":{},"changes":[],"declarations":[]})
        )
        .is_ok()
    );
    assert!(serde_json::from_value::<Loaded>(json!({"rejection":"Bad Code"})).is_err());
}
fn fixture() -> serde_json::Value {
    serde_json::from_str(include_str!(
        "../../../fixtures/protocol/host-operations.json"
    ))
    .unwrap()
}
fn round_trip_response(op: &str, value: &serde_json::Value) -> serde_json::Value {
    macro_rules! round {
        ($ty:ty) => {
            serde_json::to_value(serde_json::from_value::<$ty>(value.clone()).unwrap()).unwrap()
        };
    }
    use axton_protocols::server_bridge::{Acknowledged, Guards, Head, Positions, Tracking};
    match op {
        "protocol05" => value.clone(),
        "publicationFence" | "savepoint" | "rollback" | "release" | "lockStreams" => {
            round!(Acknowledged)
        }
        "head" => round!(Head),
        "handleAction" => round!(HandledAction),
        "load" => round!(Loaded),
        "readTracking" => round!(Tracking),
        "guardRecords" => round!(Guards),
        "applyStreamMembers" => round!(Positions),
        other => panic!("missing response type {other}"),
    }
}
#[test]
fn fixture_covers_each_retained_operation_and_all_response_variants() {
    let f = fixture();
    let entries = f["operations"].as_array().unwrap();
    let mut actual = entries
        .iter()
        .map(|e| e["op"].as_str().unwrap())
        .collect::<Vec<_>>();
    actual.sort();
    let mut expected = OPERATIONS.to_vec();
    expected.sort();
    assert_eq!(actual, expected);
    for entry in entries {
        let request: HostRequest = serde_json::from_value(entry["request"].clone()).unwrap();
        assert_eq!(serde_json::to_value(&request).unwrap(), entry["request"]);
        let responses = entry["responses"].as_array().unwrap();
        assert!(!responses.is_empty());
        for r in responses {
            assert_eq!(
                round_trip_response(entry["op"].as_str().unwrap(), &r["value"]),
                r["value"]
            );
            request.validate_response(&r["value"]).unwrap();
        }
    }
}
#[test]
fn operation_list_matches_actual_serde_variants() {
    let error = serde_json::from_value::<HostRequest>(json!({"op":"unknown"}))
        .unwrap_err()
        .to_string();
    let (_, list) = error.split_once("expected one of ").unwrap();
    let names = list
        .split(", ")
        .map(|name| name.trim_matches(|c| c == '`' || c == ' ').to_owned())
        .collect::<Vec<_>>();
    assert_eq!(names, OPERATIONS);
}
