use serde_json::{Value, json};
fn config() -> Value {
    json!({"schema":{"enums":[],"models":[{"name":"Task","identity":["id"],"fields":[{"name":"id","type":{"kind":"scalar","name":"string"},"nullable":false},{"name":"title","type":{"kind":"scalar","name":"string"},"nullable":false},{"name":"note","type":{"kind":"scalar","name":"string"},"nullable":true}]}]},"loaders":["Task"],"mutations":[{"name":"edit","version":1,"slots":[{"name":"task","model":"Task","operation":"update","cardinality":"single","allowedPatchFields":["title"]}]}]})
}
#[test]
fn ordered_slot_decodes_known_fields_and_ignores_new_fields() {
    let args=axton_server::decode_arguments(&config(),&json!({"name":"edit","operations":[{"model":"Task","op":"update","identity":{"id":"a","future":1},"values":{"title":"hi","future":true}}]})).unwrap();
    assert_eq!(
        args,
        json!({"task":{"identity":{"id":"a"},"patch":{"title":"hi"}}})
    );
}
#[test]
fn known_disallowed_patch_is_explicit_refusal() {
    let err=axton_server::decode_arguments(&config(),&json!({"name":"edit","operations":[{"model":"Task","op":"update","identity":{"id":"a"},"values":{"note":"x"}}]})).unwrap_err();
    assert_eq!(err.code, "edit.not_allowed");
}
#[test]
fn undeclared_operation_is_invalid() {
    assert_eq!(axton_server::decode_arguments(&config(),&json!({"name":"edit","operations":[{"model":"Task","op":"delete","identity":{"id":"a"}}]})).unwrap_err().code,"mutation.invalid");
}
#[test]
fn create_binding_mismatch_refuses_the_whole_act() {
    let mut c = config();
    c["mutations"] = json!([{"name":"createPair","version":1,"slots":[{"name":"parent","model":"Task","operation":"delete","cardinality":"single"},{"name":"child","model":"Task","operation":"create","cardinality":"single","bindings":[{"relation":"parent","fields":["note"],"slot":"parent"}]}]}]);
    let body = json!({"name":"createPair","operations":[{"model":"Task","op":"delete","identity":{"id":"a"}},{"model":"Task","op":"create","identity":{"id":"b"},"values":{"title":"child","note":"other"}}]});
    assert_eq!(
        axton_server::decode_arguments(&c, &body).unwrap_err().code,
        "create_pair.invalid"
    );
}
#[test]
fn historical_known_field_outside_capability_is_refused() {
    let mut c = config();
    let mut input = c["schema"].clone();
    input["models"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .retain(|f| f["name"] != "note");
    c["mutations"][0]["input"] = input;
    c["mutations"][0]["knownFields"] = json!({"Task":["id","title","note"]});
    let result = axton_server::decode_arguments(
        &c,
        &json!({"name":"edit","operations":[{"model":"Task","op":"update","identity":{"id":"a"},"values":{"title":"valid","note":"disallowed"}}]}),
    );
    assert_eq!(result.unwrap_err().code, "edit.not_allowed");
}

#[test]
fn live_subscribe_requires_one_subscribe_frame_and_normalizes_channels() {
    let decoded = axton_server::live::decode_subscribe(
        br#"{"type":"subscribe","channels":["shared","alice","shared"],"models":{"Task":1}}"#,
    )
    .unwrap();
    assert_eq!(decoded.channels, vec!["alice", "shared"]);
    assert_eq!(decoded.models.get("Task"), Some(&1));
    assert!(
        axton_server::live::decode_subscribe(
            br#"{"type":"other","channels":["a"],"models":{"Task":1}}"#
        )
        .is_err()
    );
    assert!(
        axton_server::live::decode_subscribe(
            br#"{"type":"subscribe","channels":[],"models":{"Task":1}}"#
        )
        .is_err()
    );
    assert!(
        axton_server::live::decode_subscribe(br#"{"type":"subscribe","channels":["a"]}"#).is_err(),
        "models are required"
    );
}

#[test]
fn live_page_progression_checks_every_channel_it_asked_for() {
    let full = json!({
        "cursors": {"shared": {"from":7, "to":57, "head":90}},
        "changes": (8..=57).map(|i| json!({
            "model":"Task","identity":{"id":i},"stamp":i,"state":null
        })).collect::<Vec<_>>()
    });
    let asked = std::collections::BTreeMap::from([("shared".to_string(), 7)]);
    let progress = axton_server::live::page_progress(&full.to_string(), &asked).unwrap();
    assert_eq!(progress.cursors["shared"].to, 57);
    assert!(progress.cursors["shared"].continues());
    let tail = json!({"cursors":{"shared":{"from":57,"to":60,"head":60}},"changes":[]});
    let at_57 = std::collections::BTreeMap::from([("shared".to_string(), 57)]);
    let progress = axton_server::live::page_progress(&tail.to_string(), &at_57).unwrap();
    assert!(!progress.cursors["shared"].continues());
    let wrong = json!({"cursors":{"shared":{"from":57,"to":60,"head":60}},"changes":[]});
    let asked = std::collections::BTreeMap::from([("shared".to_string(), 7)]);
    assert_eq!(
        axton_server::live::page_progress(&wrong.to_string(), &asked)
            .unwrap_err()
            .code,
        axton_server::code::LIVE_INVALID_PAGE
    );
    let other = json!({"cursors":{"other":{"from":7,"to":7,"head":7}},"changes":[]});
    assert_eq!(
        axton_server::live::page_progress(&other.to_string(), &asked)
            .unwrap_err()
            .code,
        axton_server::code::LIVE_INVALID_PAGE
    );
}
#[test]
fn startup_rejects_invalid_patch_capabilities() {
    for fields in [json!(["id"]), json!(["missing"]), json!(["title", "title"])] {
        let mut c = config();
        c["mutations"][0]["slots"][0]["allowedPatchFields"] = fields;
        assert!(axton_server::Config::decode(c).is_err());
    }
    let mut c = config();
    c["mutations"][0]["slots"][0]["operation"] = json!("delete");
    assert!(axton_server::Config::decode(c).is_err());
}

#[test]
fn startup_validates_the_retained_model_contracts() {
    let contract = |version: u64, fields: Value| json!({"name":"Task","version":version,"identity":["id"],"fields":fields,"enums":[]});
    let id = json!({"name":"id","type":{"kind":"scalar","name":"string"},"nullable":false});
    let title = json!({"name":"title","type":{"kind":"scalar","name":"string"},"nullable":false});
    let note = json!({"name":"note","type":{"kind":"scalar","name":"string"},"nullable":true});
    // Without `models`, every model is retained at the schema's own version.
    let derived = axton_server::Config::decode(config()).unwrap();
    assert_eq!(
        derived
            .models
            .iter()
            .map(|m| (m.name.as_str(), m.version))
            .collect::<Vec<_>>(),
        [("Task", 1)]
    );
    let mut c = config();
    c["schema"]["models"][0]["version"] = json!(2);
    c["models"] = json!([
        contract(1, json!([id, title])),
        contract(2, json!([id, title, note]))
    ]);
    let decoded = axton_server::Config::decode(c.clone()).unwrap();
    assert_eq!(decoded.models.len(), 2);
    assert_eq!(
        decoded
            .contract("Task", 1)
            .unwrap()
            .model("Task")
            .unwrap()
            .fields
            .len(),
        2
    );
    assert!(
        decoded.contract("Task", 3).is_none(),
        "an unretained version is not served"
    );
    // The schema's current version must be retained; identities must agree;
    // a contract must be a valid schema; versions are unique per model.
    let mut missing_current = c.clone();
    missing_current["models"] = json!([contract(1, json!([id, title]))]);
    assert!(axton_server::Config::decode(missing_current).is_err());
    let mut other_identity = c.clone();
    other_identity["models"][0]["identity"] = json!(["title"]);
    assert!(axton_server::Config::decode(other_identity).is_err());
    let mut unknown_enum = c.clone();
    unknown_enum["models"][0]["fields"] =
        json!([id, {"name":"kind","type":{"kind":"enum","name":"Kind"},"nullable":false}]);
    assert!(axton_server::Config::decode(unknown_enum).is_err());
    let mut duplicate = c.clone();
    duplicate["models"] = json!([
        contract(2, json!([id, title])),
        contract(2, json!([id, title, note]))
    ]);
    assert!(axton_server::Config::decode(duplicate).is_err());
    let mut unknown_model = c.clone();
    unknown_model["models"][0]["name"] = json!("Other");
    assert!(axton_server::Config::decode(unknown_model).is_err());
}

/// A host whose `claim` answers with the given owner and last sequence and
/// which records every `handle` call, for asserting protocol refusals in
/// process without a database.
mod refusals {
    use axton_server::{
        Host, HostResult, code,
        host::{self, Acknowledged, Handled, Head, HostRequest, Loaded, Memberships, Stamped},
    };
    use serde_json::{Value, json};
    use std::{
        future::Future,
        pin::Pin,
        sync::Mutex,
        task::{Context, Poll, Waker},
    };

    fn run<T>(future: impl Future<Output = T>) -> T {
        let mut f = std::pin::pin!(future);
        let mut cx = Context::from_waker(Waker::noop());
        loop {
            if let Poll::Ready(result) = f.as_mut().poll(&mut cx) {
                return result;
            }
        }
    }
    struct Claimed {
        owner: &'static str,
        sequence: u64,
        handled: Mutex<Vec<HostRequest>>,
    }
    impl Host for Claimed {
        fn call(&self, r: Value) -> Pin<Box<dyn Future<Output = HostResult<Value>> + Send + '_>> {
            Box::pin(async move {
                let request: HostRequest = serde_json::from_value(r)
                    .map_err(|error| format!("unsupported host request: {error}"))?;
                Ok(match &request {
                    HostRequest::Claim { client_id, .. } => serde_json::to_value(host::Claimed {
                        client_id: client_id.clone(),
                        owner: self.owner.into(),
                        sequence: self.sequence,
                        receipt: Some("{}".into()),
                    })
                    .unwrap(),
                    HostRequest::Handle { .. } => {
                        self.handled.lock().unwrap().push(request.clone());
                        serde_json::to_value(Handled::Settled {
                            changes: vec![],
                            memberships: vec![],
                        })
                        .unwrap()
                    }
                    HostRequest::AdvanceStamp { .. } => serde_json::to_value(Stamped(1)).unwrap(),
                    HostRequest::Memberships { .. } => {
                        serde_json::to_value(Memberships::default()).unwrap()
                    }
                    HostRequest::Load { identities, .. } => serde_json::to_value(Loaded::Rows(
                        identities
                            .iter()
                            .map(|_| Some(json!({"id":"a","title":"t","note":null})))
                            .collect(),
                    ))
                    .unwrap(),
                    HostRequest::Head { .. } => serde_json::to_value(Head(0)).unwrap(),
                    HostRequest::Savepoint { .. }
                    | HostRequest::Rollback { .. }
                    | HostRequest::Release { .. }
                    | HostRequest::SaveReceipt { .. } => {
                        serde_json::to_value(Acknowledged).unwrap()
                    }
                    other => return Err(format!("unsupported {}", other.label())),
                })
            })
        }
    }
    fn claimed(owner: &'static str, sequence: u64) -> Claimed {
        Claimed {
            owner,
            sequence,
            handled: Mutex::new(vec![]),
        }
    }
    fn push(sequence: u64, version: Option<u64>) -> Vec<u8> {
        let mut mutation = json!({"ordinal":1,"name":"edit","operations":[{"model":"Task","op":"update","identity":{"id":"a"},"values":{"title":"t"}}]});
        if let Some(v) = version {
            mutation["version"] = json!(v);
        }
        json!({"clientId":"c","batchSequence":sequence,"models":{"Task":1},"mutations":[mutation]})
            .to_string()
            .into_bytes()
    }
    fn config() -> axton_server::Config {
        axton_server::Config::decode(super::config()).unwrap()
    }

    #[test]
    fn push_refusals_carry_stable_codes_and_run_no_handler() {
        // (claimed owner, last accepted sequence, batch sequence, version, expected code)
        let cases: [(&str, u64, u64, Option<u64>, &str); 3] = [
            ("alice", 1, 3, None, code::GAP),
            ("alice", 2, 1, None, code::OVERLAP),
            ("bob", 0, 1, None, code::OWNER_MISMATCH),
        ];
        for (owner, last, batch, version, expected) in cases {
            let host = claimed(owner, last);
            let err = run(axton_server::process_push(
                &config(),
                "alice",
                &push(batch, version),
                &host,
            ))
            .unwrap_err();
            assert_eq!(err.code, expected, "{err}");
            assert!(
                host.handled.lock().unwrap().is_empty(),
                "{expected} ran a handler"
            );
        }
        let host = claimed("alice", 0);
        let result = run(axton_server::process_push(
            &config(),
            "alice",
            &push(1, Some(2)),
            &host,
        ))
        .unwrap();
        let receipt = axton_core::PushReceipt::decode(result.as_bytes()).unwrap();
        assert_eq!(
            receipt.rejections,
            vec![axton_core::Rejection {
                ordinal: 1,
                code: code::MUTATION_VERSION_UNSUPPORTED.into()
            }]
        );
        assert!(
            host.handled.lock().unwrap().is_empty(),
            "an unsupported version never runs the handler"
        );
        let host = claimed("alice", 0);
        let accepted = run(axton_server::process_push(
            &config(),
            "alice",
            &push(1, None),
            &host,
        ))
        .unwrap();
        let receipt = axton_core::PushReceipt::decode(accepted.as_bytes()).unwrap();
        assert!(receipt.answers("c", 1));
        assert_eq!(receipt.records.len(), 1, "the changed record is read back");
        assert_eq!(receipt.records[0].stamp, 1);
        assert_eq!(receipt.records[0].state, json!({"title":"t","note":null}));
        assert_eq!(host.handled.lock().unwrap().len(), 1);
    }

    #[test]
    fn malformed_requests_and_blank_owners_are_refused_with_codes() {
        let host = claimed("alice", 0);
        let err = run(axton_server::process_push(&config(), "alice", b"{", &host)).unwrap_err();
        assert_eq!(err.code, code::REQUEST_INVALID);
        let err = run(axton_server::process_pull(&config(), "alice", b"[]", &host)).unwrap_err();
        assert_eq!(err.code, code::REQUEST_INVALID);
        let ahead = json!({"cursors":{"a":7},"models":{"Task":1}}).to_string();
        let err = run(axton_server::process_pull(
            &config(),
            "alice",
            ahead.as_bytes(),
            &host,
        ))
        .unwrap_err();
        assert_eq!(err.code, code::REQUEST_INVALID);
        assert!(err.message.contains("ahead of head"));
        let err = run(axton_server::process_push(
            &config(),
            " ",
            &push(1, None),
            &host,
        ))
        .unwrap_err();
        assert_eq!(err.code, code::PRINCIPAL_INVALID);
        let err = axton_server::live::decode_subscribe(br#"{"type":"other","channels":["a"]}"#)
            .unwrap_err();
        assert_eq!(err.code, code::REQUEST_INVALID);
    }

    #[test]
    fn host_failures_keep_their_message_under_the_host_code() {
        struct Failing;
        impl Host for Failing {
            fn call(
                &self,
                _: Value,
            ) -> Pin<Box<dyn Future<Output = HostResult<Value>> + Send + '_>> {
                Box::pin(async { Err("connection reset by peer".to_string()) })
            }
        }
        let err = run(axton_server::process_push(
            &config(),
            "alice",
            &push(1, None),
            &Failing,
        ))
        .unwrap_err();
        assert_eq!(err.code, code::HOST);
        assert_eq!(err.message, "connection reset by peer");
        assert_eq!(err.to_string(), "host: connection reset by peer");
        let round_trip: axton_server::Error =
            serde_json::from_str(&serde_json::to_string(&err).unwrap()).unwrap();
        assert_eq!(round_trip, err);
        assert!(!serde_json::to_string(&err).unwrap().contains("details"));
    }
}
#[test]
fn empty_patch_decodes_as_a_no_op_update() {
    let args = axton_server::decode_arguments(
        &config(),
        &json!({"name":"edit","operations":[{"model":"Task","op":"update","identity":{"id":"a"},"values":{}}]}),
    )
    .unwrap();
    assert_eq!(args, json!({"task":{"identity":{"id":"a"},"patch":{}}}));
}
#[test]
fn a_patch_of_only_unknown_fields_decodes_as_a_no_op_update() {
    let args = axton_server::decode_arguments(
        &config(),
        &json!({"name":"edit","operations":[{"model":"Task","op":"update","identity":{"id":"a"},"values":{"future":true}}]}),
    )
    .unwrap();
    assert_eq!(args, json!({"task":{"identity":{"id":"a"},"patch":{}}}));
}
#[test]
fn update_values_must_still_be_an_object() {
    let err = axton_server::decode_arguments(
        &config(),
        &json!({"name":"edit","operations":[{"model":"Task","op":"update","identity":{"id":"a"},"values":null}]}),
    )
    .unwrap_err();
    assert_eq!(err.code, "mutation.invalid");
}
