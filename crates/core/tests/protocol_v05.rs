use axton_core::v05::*;
use serde_json::{Value, json};

fn ctx() -> RequestContext {
    RequestContext {
        protocol: 5,
        store_id: "store".into(),
        stream: "User:a".into(),
        materialization: "schema1".into(),
    }
}
fn request() -> MutationRequest {
    let mut request: MutationRequest = serde_json::from_value(wire(json!({
        "context":ctx(), "batchId":1,"digest":"", "mutations":[{
            "id":1,"name":"Write","version":1,"descriptor":"retained1","operations":[
                {"step":0,"inputPath":"text","operation":"argument","model":null,"identity":null,"value":"hello"},
                {"step":1,"inputPath":"items","operation":"argument","model":null,"identity":null,"value":[]},
                {"step":2,"inputPath":"optional","operation":"argument","model":null,"identity":null,"value":null}
            ]}]}))).unwrap();
    request.digest = batch_digest(&request).unwrap();
    request
}
fn ack(request: &MutationRequest) -> BatchAcknowledgement {
    serde_json::from_value(wire(json!({"context":request.context,"batchId":1,"digest":request.digest,"results":[
        {"mutationId":1,"outcome":{"kind":"accepted","syncCursor":0,"result":{"appOpaque":[1,null]},"targets":[]}}
    ]}))).unwrap()
}
fn wire(mut value: Value) -> Value {
    if let Some(context) = value.as_object_mut().unwrap().remove("context") {
        value
            .as_object_mut()
            .unwrap()
            .extend(context.as_object().unwrap().clone());
    }
    value
}
fn rejects<T: serde::de::DeserializeOwned + Validate>(value: Value) {
    assert!(
        decode::<T>(&serde_json::to_vec(&value).unwrap()).is_err(),
        "accepted {value}"
    );
}

#[test]
fn a1_canonical_batch_binds_exact_order_context_and_descriptor() {
    let request = request();
    validate_batch(&request).unwrap();
    let bytes = encode(&request).unwrap();
    assert_eq!(decode::<MutationRequest>(&bytes).unwrap(), request);
    let mut changed = request.clone();
    changed.mutations[0].operations[0].value = json!("changed");
    assert!(validate_batch(&changed).is_err());
    changed.digest = batch_digest(&changed).unwrap();
    assert_ne!(changed.digest, request.digest);
    for modify in [0, 1, 2, 3] {
        let mut changed = request.clone();
        match modify {
            0 => changed.context.stream = "User:b".into(),
            1 => changed.context.store_id = "new".into(),
            2 => changed.context.materialization = "schema2".into(),
            _ => changed.mutations[0].descriptor = "retained2".into(),
        }
        assert_ne!(batch_digest(&changed).unwrap(), request.digest);
        assert!(validate_batch(&changed).is_err());
    }
}
#[test]
fn strict_envelopes_counters_and_duplicates() {
    let request = request();
    let value = serde_json::to_value(&request).unwrap();
    for path in ["top", "context", "mutation", "operation"] {
        let mut bad = value.clone();
        match path {
            "top" => bad["extra"] = json!(1),
            "context" => bad["contextExtra"] = json!(1),
            "mutation" => bad["mutations"][0]["extra"] = json!(1),
            _ => bad["mutations"][0]["operations"][0]["extra"] = json!(1),
        }
        rejects::<MutationRequest>(bad);
    }
    let mut bad = value.clone();
    bad["protocol"] = json!(4);
    rejects::<MutationRequest>(bad);
    let mut bad = value.clone();
    bad["batchId"] = json!(9_007_199_254_740_992u64);
    rejects::<MutationRequest>(bad);
    let mut duplicate = request.clone();
    duplicate.mutations.push(duplicate.mutations[0].clone());
    assert!(batch_digest(&duplicate).is_err());
    let mut duplicate = request.clone();
    let op = duplicate.mutations[0].operations[0].clone();
    duplicate.mutations[0].operations.push(op);
    assert!(batch_digest(&duplicate).is_err());
    let mut bad = value;
    bad["mutations"][0]["operations"][0]["model"] = json!("Entry");
    rejects::<MutationRequest>(bad);
}
#[test]
fn exact_ack_and_owned_target_envelopes() {
    let request = request();
    let acknowledgement = ack(&request);
    validate_acknowledgement(&request, &acknowledgement).unwrap();
    for variant in 0..7 {
        let mut bad = acknowledgement.clone();
        match variant {
            0 => bad.results.clear(),
            1 => bad.results.push(bad.results[0].clone()),
            2 => bad.results[0].mutation_id = 2,
            3 => bad.context.stream = "User:b".into(),
            4 => bad.context.store_id = "new".into(),
            5 => bad.context.materialization = "new".into(),
            _ => bad.digest = "0".repeat(64),
        }
        assert!(validate_acknowledgement(&request, &bad).is_err());
    }
    let base = json!({"kind":"stream","key":{"model":"Entry","identity":{"id":"e"}},"cursor":2,"fallback":{"key":{"model":"Entry","identity":{"id":"e"}},"cursor":null,"state":{"text":"a"}}});
    let target = decode::<SettlementTarget>(&serde_json::to_vec(&base).unwrap()).unwrap();
    assert_eq!(serde_json::to_value(target).unwrap(), base);
    for variant in 0..5 {
        let mut bad = base.clone();
        match variant {
            0 => bad["key"]["extra"] = json!(true),
            1 => bad["fallback"]["cursor"] = json!(1),
            2 => bad["fallback"]
                .as_object_mut()
                .unwrap()
                .remove("cursor")
                .map(|_| ())
                .unwrap(),
            3 => bad["fallback"]["key"]["identity"] = json!({"id":"other"}),
            _ => bad["cursor"] = json!(0),
        };
        rejects::<SettlementTarget>(bad);
    }
    rejects::<BatchAcknowledgement>(wire(
        json!({"context":ctx(),"batchId":1,"digest":request.digest,"results":[{"mutationId":1,"outcome":{"kind":"accepted","syncCursor":0,"result":null,"targets":[],"extra":true}}]}),
    ));
}
#[test]
fn a14_operations_preserve_null_empty_omitted_order_and_retained_descriptor() {
    let request = request();
    let input = reconstruct_input(&request.mutations[0].operations).unwrap();
    assert_eq!(input, json!({"text":"hello","items":[],"optional":null}));
    assert!(input.get("omitted").is_none());
    let operations:Vec<MutationOperation>=serde_json::from_value(json!([
        {"step":0,"inputPath":"items[0]","operation":"create","model":"Entry","identity":{"id":"a"},"value":{"text":"A"}},
        {"step":1,"inputPath":"items[1]","operation":"delete","model":"Entry","identity":{"id":"b"},"value":null}
    ])).unwrap();
    assert_eq!(
        reconstruct_input(&operations).unwrap(),
        json!({"items":[{"id":"a","text":"A"},{"id":"b"}]})
    );
    let mut gap = operations.clone();
    gap[1].input_path = "items[2]".into();
    assert!(reconstruct_input(&gap).is_err());
    let mut overlap = operations;
    overlap[1].input_path = "items[0]".into();
    assert!(reconstruct_input(&overlap).is_err());
}
#[test]
fn strict_read_and_delta_context() {
    let read = wire(
        json!({"context":ctx(),"requestId":"read1","outcome":{"kind":"succeeded","result":{"arbitrary":true}},"records":[{"key":{"model":"Entry","identity":{"id":"e"}},"cursor":null,"state":null}]}),
    );
    let response = decode::<ReadResponse>(&serde_json::to_vec(&read).unwrap()).unwrap();
    response.context.admit(&ctx()).unwrap();
    let mut bad = read.clone();
    bad["records"][0]["state"] = json!("bad");
    rejects::<ReadResponse>(bad);
    let mut bad = read;
    bad["records"][0]["key"]["unknown"] = json!(true);
    rejects::<ReadResponse>(bad);
    let value =
        wire(json!({"context":ctx(),"after":0,"through":40,"bootstrap":true,"continuation":null}));
    let delta = decode::<DeltaRequest>(&serde_json::to_vec(&value).unwrap()).unwrap();
    assert_eq!(delta.after, 0);
    let mut bad = value;
    bad["through"] = json!(9_007_199_254_740_992u64);
    rejects::<DeltaRequest>(bad);
}
fn frozen() -> FrozenDelivery {
    let changes = vec![
        AuthorityChange::Record {
            key: RecordKey {
                model: "Entry".into(),
                identity: json!({"id":"a"}),
            },
            cursor: 4,
            state: json!({"text":"a"}),
        },
        AuthorityChange::Record {
            key: RecordKey {
                model: "Entry".into(),
                identity: json!({"id":"b"}),
            },
            cursor: 4,
            state: json!({"text":"b"}),
        },
    ];
    let units = plan_units(&changes, &Default::default(), &[], 0, 4, 4).unwrap();
    freeze_delivery(
        ctx(),
        "plan".into(),
        DeliveryPurpose::Sync,
        0,
        4,
        4,
        100,
        units,
        1,
    )
    .unwrap()
}
#[test]
fn delivery_manifest_binds_coverage_payload_parts_and_complete_units() {
    let frozen = frozen();
    let mut p = DeliveryProgress::new(&frozen.header, &ctx(), 0).unwrap();
    assert_eq!(frozen.parts.len(), 2);
    let before = p.clone();
    assert!(p.stage_with_limit(&frozen.parts[0], &ctx(), 1, 1).is_err());
    assert_eq!(p, before);
    assert!(
        p.stage_with_limit(&frozen.parts[1], &ctx(), 1, 1024 * 1024)
            .unwrap()
            .is_none()
    );
    let partial = p.clone();
    let forged = DeliveryUnit {
        index: 0,
        through: Some(4),
        changes: frozen.parts[1].changes.clone(),
    };
    assert!(p.commit(&forged, &ctx(), 1).is_err());
    assert_eq!(p, partial);
    let mut bad = frozen.parts[0].clone();
    bad.changes[0] = AuthorityChange::Remove {
        key: RecordKey {
            model: "Entry".into(),
            identity: json!({"id":"a"}),
        },
        cursor: 4,
    };
    assert!(p.stage_with_limit(&bad, &ctx(), 1, 1024 * 1024).is_err());
    assert_eq!(p, partial);
    let complete = p
        .stage_with_limit(&frozen.parts[0], &ctx(), 1, 1024 * 1024)
        .unwrap()
        .unwrap();
    validate_delivery(&frozen.header, std::slice::from_ref(&complete)).unwrap();
    assert_eq!(
        p.stage_with_limit(&frozen.parts[0], &ctx(), 1, 1024 * 1024)
            .unwrap(),
        Some(complete.clone())
    );
    p.commit(&complete, &ctx(), 1).unwrap();
    assert_eq!(p.covered, Some(4));
    assert!(p.commit(&complete, &ctx(), 1).is_err());
    for variant in 0..4 {
        let mut h = frozen.header.clone();
        match variant {
            0 => h.through = Some(5),
            1 => h.context.stream = "User:b".into(),
            2 => h.units[0].parts.pop().map(|_| ()).unwrap(),
            _ => h.digest = "0".repeat(64),
        }
        assert!(validate_delivery(&h, std::slice::from_ref(&complete)).is_err());
    }
    let mut bad = serde_json::to_value(&frozen.parts[0]).unwrap();
    bad["changes"][0]["key"]["extra"] = json!(true);
    rejects::<DeliveryPart>(bad);
    let mut bad = serde_json::to_value(&frozen.header).unwrap();
    bad["units"][0]["extra"] = json!(true);
    rejects::<DeliveryHeader>(bad);
}
#[test]
fn malformed_optional_coverage_and_materialization_owner_are_rejected() {
    let f = frozen();
    let units = vec![DeliveryUnit {
        index: 0,
        through: None,
        changes: f.parts.iter().flat_map(|p| p.changes.clone()).collect(),
    }];
    assert!(
        freeze_delivery(
            ctx(),
            "p".into(),
            DeliveryPurpose::Sync,
            0,
            4,
            4,
            100,
            units,
            1
        )
        .is_err()
    );
    let mut bad = serde_json::to_value(&f.header).unwrap();
    bad["owner"] = json!({"kind":"settlement","batchId":0,"mutationId":1});
    rejects::<DeliveryHeader>(bad);
}
#[test]
fn read_completions_require_definitive_outcome_and_empty_error_records() {
    let error = wire(
        json!({"context":ctx(),"requestId":"q","outcome":{"kind":"failed","code":"query.not_allowed","message":null},"records":[]}),
    );
    assert!(decode::<ReadResponse>(&serde_json::to_vec(&error).unwrap()).is_ok());
    let mut bad = error;
    bad["records"] =
        json!([{"key":{"model":"Entry","identity":{"id":"e"}},"cursor":null,"state":{}}]);
    rejects::<ReadResponse>(bad);
}
#[test]
fn accepted_acknowledgement_requires_every_server_model_input_target() {
    let mut r = request();
    r.mutations[0].operations.push(serde_json::from_value(json!({"step":3,"inputPath":"entry","operation":"create","model":"Entry","identity":{"id":"e"},"value":{"text":"A"}})).unwrap());
    r.digest = batch_digest(&r).unwrap();
    assert!(
        validate_acknowledgement(&r, &ack(&r)).is_err(),
        "missing owned target was accepted"
    );
}
#[test]
fn handshake_and_reads_correlate_store_stream_request_and_modes() {
    let request =
        decode::<HandshakeRequest>(br#"{"protocol":5,"storeId":"store","stream":"User:a"}"#)
            .unwrap();
    let response = decode::<HandshakeResponse>(
        br#"{"protocol":5,"storeId":"store","stream":"User:a","head":0}"#,
    )
    .unwrap();
    response.admit(&request).unwrap();
    let mut other = response.clone();
    other.stream = "User:b".into();
    assert!(other.admit(&request).is_err());
    let mut value = wire(
        json!({"context":ctx(),"requestId":"fetch1","invocation":{"kind":"fetch","key":{"model":"Entry","identity":{"id":"e"}},"version":1}}),
    );
    let default = decode::<ReadRequest>(&serde_json::to_vec(&value).unwrap()).unwrap();
    assert!(default.store);
    value["store"] = json!(true);
    assert_eq!(
        decode::<ReadRequest>(&serde_json::to_vec(&value).unwrap()).unwrap(),
        default
    );
    value["store"] = json!(false);
    let snapshot = decode::<ReadRequest>(&serde_json::to_vec(&value).unwrap()).unwrap();
    assert!(!snapshot.store);
    let response=decode::<ReadResponse>(&serde_json::to_vec(&wire(json!({"context":ctx(),"requestId":"fetch1","outcome":{"kind":"succeeded","result":{"id":"e","text":"a"}},"records":[{"key":{"model":"Entry","identity":{"id":"e"}},"cursor":null,"state":{"text":"a"}}]}))).unwrap()).unwrap();
    response.admit(&snapshot, &ctx()).unwrap();
    let mut wrong = response.clone();
    wrong.request_id = "other".into();
    assert!(wrong.admit(&snapshot, &ctx()).is_err());
    let mut wrong = response;
    wrong.outcome = ReadOutcome::Succeeded {
        result: json!({"id":"e","text":"changed"}),
    };
    assert!(wrong.admit(&snapshot, &ctx()).is_err());
    value["invocation"]["extra"] = json!(true);
    rejects::<ReadRequest>(value);
    let delta = decode::<DeltaRequest>(
        &serde_json::to_vec(&wire(
            json!({"context":ctx(),"after":0,"through":4,"continuation":null}),
        ))
        .unwrap(),
    )
    .unwrap();
    assert!(!delta.bootstrap);
    let f = frozen();
    let response = DeliveryResponse {
        header: f.header,
        parts: f.parts,
    };
    response.admit(&delta, &ctx()).unwrap();
    let mut wrong = delta;
    wrong.bootstrap = true;
    assert!(response.admit(&wrong, &ctx()).is_err());
}
#[test]
fn owned_materialization_has_no_ranges_and_rejects_foreign_keys_or_owner() {
    let req=decode::<MaterializationRequest>(&serde_json::to_vec(&wire(json!({"context":ctx(),"requestId":"owned1","owner":{"kind":"settlement","batchId":1,"mutationId":2},"keys":[{"model":"Entry","identity":{"id":"e"}}],"models":{},"continuation":null}))).unwrap()).unwrap();
    let unit = DeliveryUnit {
        index: 0,
        through: None,
        changes: vec![AuthorityChange::Record {
            key: req.keys[0].clone(),
            cursor: 20,
            state: json!({"text":"a"}),
        }],
    };
    let f = freeze_materialization(
        ctx(),
        "owned".into(),
        req.owner.clone(),
        40,
        100,
        vec![unit.clone()],
        1,
    )
    .unwrap();
    assert_eq!(f.header.after, None);
    assert_eq!(f.header.through, None);
    let response = MaterializationResponse {
        request_id: "owned1".into(),
        delivery: DeliveryResponse {
            header: f.header.clone(),
            parts: f.parts,
        },
    };
    response.admit(&req, &ctx()).unwrap();
    let mut wrong = req.clone();
    wrong.keys[0].identity = json!({"id":"other"});
    assert!(response.admit(&wrong, &ctx()).is_err());
    let mut wrong = req.clone();
    wrong.owner = MaterializationOwner::Settlement {
        batch_id: 1,
        mutation_id: 3,
    };
    assert!(response.admit(&wrong, &ctx()).is_err());
    let mut header = f.header;
    header.after = Some(40);
    header.through = Some(40);
    header.digest = delivery_digest(&header).unwrap();
    assert!(header.validate().is_err());
    let mut covered = unit;
    covered.through = Some(40);
    assert!(
        freeze_materialization(ctx(), "bad".into(), req.owner, 40, 100, vec![covered], 1).is_err()
    );
    let schema = wire(
        json!({"context":ctx(),"requestId":"schema","owner":{"kind":"schema","previousMaterialization":"old"},"keys":[],"models":{"Entry":2},"continuation":null}),
    );
    assert!(decode::<MaterializationRequest>(&serde_json::to_vec(&schema).unwrap()).is_ok());
    let mut bad = schema;
    bad["after"] = json!(0);
    rejects::<MaterializationRequest>(bad);
}
#[test]
fn later_manifest_minimum_cannot_be_hidden_behind_earlier_coverage() {
    let f = frozen();
    let mut header = f.header;
    header.units.push(header.units[0].clone());
    header.units[0].through = Some(3);
    header.units[1].minimum_cursor = Some(2);
    header.digest = delivery_digest(&header).unwrap();
    assert!(header.validate().is_err());
}

#[test]
fn shared_wire_fixture_roundtrips_every_message_and_hash_domain() {
    let request = request();
    let acknowledgement = ack(&request);
    let f = frozen();
    let materialize = MaterializationRequest {
        context: ctx(),
        request_id: "owned1".into(),
        owner: MaterializationOwner::Settlement {
            batch_id: 1,
            mutation_id: 2,
        },
        keys: vec![RecordKey {
            model: "Entry".into(),
            identity: json!({"id":"e"}),
        }],
        models: Default::default(),
        continuation: None,
    };
    let owned = freeze_materialization(
        ctx(),
        "owned".into(),
        materialize.owner.clone(),
        40,
        100,
        vec![DeliveryUnit {
            index: 0,
            through: None,
            changes: vec![AuthorityChange::Record {
                key: materialize.keys[0].clone(),
                cursor: 20,
                state: json!({"text":"accepted"}),
            }],
        }],
        1,
    )
    .unwrap();
    let query = ReadRequest {
        context: ctx(),
        request_id: "q1".into(),
        store: true,
        invocation: ReadInvocation::Query {
            name: "ReadEntries".into(),
            version: 1,
            args: json!({"limit":10}),
        },
    };
    let fetch = ReadRequest {
        context: ctx(),
        request_id: "f1".into(),
        store: false,
        invocation: ReadInvocation::Fetch {
            key: materialize.keys[0].clone(),
            version: 1,
        },
    };
    let snapshot = ReadRecord {
        key: materialize.keys[0].clone(),
        cursor: (),
        state: json!({"text":"accepted"}),
    };
    let success = ReadResponse {
        context: ctx(),
        request_id: "f1".into(),
        outcome: ReadOutcome::Succeeded {
            result: json!({"id":"e","text":"accepted"}),
        },
        records: vec![snapshot.clone()],
    };
    let failure = ReadResponse {
        context: ctx(),
        request_id: "q1".into(),
        outcome: ReadOutcome::Failed {
            code: "query.not_allowed".into(),
            message: None,
        },
        records: vec![],
    };
    let schema = MaterializationRequest {
        context: ctx(),
        request_id: "schema1".into(),
        owner: MaterializationOwner::Schema {
            previous_materialization: "schema0".into(),
        },
        keys: vec![],
        models: std::collections::BTreeMap::from([("Entry".into(), 2)]),
        continuation: None,
    };
    let mut rejected = acknowledgement.clone();
    rejected.results[0].outcome = MutationOutcome::Rejected {
        code: "write.not_allowed".into(),
        message: Some("refused".into()),
    };
    let fixtures = json!({
        "mutationRequest":request,"batchAcknowledgement":acknowledgement,"rejectedAcknowledgement":rejected,
        "handshakeRequest":HandshakeRequest{protocol:5,store_id:"store".into(),stream:"User:a".into()},
        "handshakeResponse":HandshakeResponse{protocol:5,store_id:"store".into(),stream:"User:a".into(),head:40},
        "queryRequest":query,"fetchRequest":fetch,"readSuccess":success,"readFailure":failure,
        "readRecord":snapshot,"streamTarget":SettlementTarget::Stream{key:snapshot.key.clone(),cursor:20,fallback:snapshot.clone()},
        "privateTarget":SettlementTarget::Private{record:snapshot},
        "deltaRequest":DeltaRequest{context:ctx(),after:0,through:4,bootstrap:false,continuation:None},
        "continuedDelta":DeltaRequest{context:ctx(),after:0,through:4,bootstrap:false,continuation:Some(Continuation{plan_id:f.header.plan_id.clone(),digest:f.header.digest.clone(),unit:0,part:1})},
        "deliveryHeader":f.header,"deliveryUnit":DeliveryUnit{index:0,through:Some(4),changes:f.parts.iter().flat_map(|p|p.changes.clone()).collect()},
        "deliveryPart":f.parts[0],"deliveryResponse":DeliveryResponse{header:f.header.clone(),parts:f.parts.clone()},
        "materializationRequest":materialize,"schemaMaterializationRequest":schema,
        "materializationResponse":MaterializationResponse{request_id:"owned1".into(),delivery:DeliveryResponse{header:owned.header.clone(),parts:owned.parts.clone()}},
        "ownedDeliveryHeader":owned.header,"ownedDeliveryUnit":DeliveryUnit{index:0,through:None,changes:owned.parts[0].changes.clone()},
        "removeChange":AuthorityChange::Remove{key:RecordKey{model:"Entry".into(),identity:json!({"id":"removed"})},cursor:30},
        "deliveryProgress":DeliveryProgress::new(&f.header,&ctx(),0).unwrap(),
        "cases":{"bootstrapRecordMovesPastStart":{"startCursor":40,"bootstrapCursor":null,"before":[{"model":"Entry","id":"e","cursor":20,"text":"a"}],"atPlan":[{"model":"Entry","id":"e","cursor":45,"text":"b"}],"observedHead":45,"expected":{"bootstrapCursor":40,"entryText":"b","entryCursor":45}}}
    });
    let saved: Value =
        serde_json::from_str(include_str!("../../../fixtures/protocol/0.5.json")).unwrap();
    assert_eq!(saved, fixtures, "frozen cross-language fixture drift");
    macro_rules! check {
        ($name:literal,$ty:ty) => {{
            let value = decode::<$ty>(&serde_json::to_vec(&saved[$name]).unwrap()).unwrap();
            assert_eq!(serde_json::to_value(value).unwrap(), saved[$name]);
        }};
    }
    check!("mutationRequest", MutationRequest);
    check!("batchAcknowledgement", BatchAcknowledgement);
    check!("rejectedAcknowledgement", BatchAcknowledgement);
    check!("handshakeRequest", HandshakeRequest);
    check!("handshakeResponse", HandshakeResponse);
    check!("queryRequest", ReadRequest);
    check!("fetchRequest", ReadRequest);
    check!("readSuccess", ReadResponse);
    check!("readFailure", ReadResponse);
    check!("readRecord", ReadRecord);
    check!("streamTarget", SettlementTarget);
    check!("privateTarget", SettlementTarget);
    check!("deltaRequest", DeltaRequest);
    check!("continuedDelta", DeltaRequest);
    check!("deliveryHeader", DeliveryHeader);
    check!("deliveryUnit", DeliveryUnit);
    check!("deliveryPart", DeliveryPart);
    check!("deliveryResponse", DeliveryResponse);
    check!("materializationRequest", MaterializationRequest);
    check!("schemaMaterializationRequest", MaterializationRequest);
    check!("materializationResponse", MaterializationResponse);
    check!("ownedDeliveryHeader", DeliveryHeader);
    check!("ownedDeliveryUnit", DeliveryUnit);
    check!("removeChange", AuthorityChange);
    check!("deliveryProgress", DeliveryProgress);
    assert_eq!(
        batch_digest(&request).unwrap(),
        saved["mutationRequest"]["digest"]
    );
}
#[test]
fn schema_transfer_admits_pending_descriptor_before_it_is_enabled() {
    let mut desired = ctx();
    desired.materialization = "schema2".into();
    let f = freeze_materialization(
        desired,
        "schema-plan".into(),
        MaterializationOwner::Schema {
            previous_materialization: "schema1".into(),
        },
        40,
        100,
        vec![DeliveryUnit {
            index: 0,
            through: None,
            changes: vec![],
        }],
        1,
    )
    .unwrap();
    assert!(
        DeliveryProgress::new(&f.header, &ctx(), 0).is_ok(),
        "pending schema transfer requires old enabled context"
    );
    let mut foreign = ctx();
    foreign.materialization = "unrelated".into();
    assert!(DeliveryProgress::new(&f.header, &foreign, 0).is_err());
}
#[test]
fn full_materialization_checks_required_identity_set_before_completion() {
    let request = MaterializationRequest {
        context: ctx(),
        request_id: "materialize".into(),
        owner: MaterializationOwner::Settlement {
            batch_id: 1,
            mutation_id: 1,
        },
        keys: vec![RecordKey {
            model: "Entry".into(),
            identity: json!({"id":"required"}),
        }],
        models: Default::default(),
        continuation: None,
    };
    let empty = DeliveryUnit {
        index: 0,
        through: None,
        changes: vec![],
    };
    let f = freeze_materialization(
        ctx(),
        "empty".into(),
        request.owner.clone(),
        40,
        100,
        vec![empty.clone()],
        1,
    )
    .unwrap();
    assert!(validate_materialization(&request, &f.header, &[empty]).is_err());
    let complete = DeliveryUnit {
        index: 0,
        through: None,
        changes: vec![AuthorityChange::Remove {
            key: request.keys[0].clone(),
            cursor: 30,
        }],
    };
    let f = freeze_materialization(
        ctx(),
        "complete".into(),
        request.owner.clone(),
        40,
        100,
        vec![complete.clone()],
        1,
    )
    .unwrap();
    validate_materialization(&request, &f.header, &[complete]).unwrap();
}
#[test]
fn receipt_stream_target_cannot_exceed_its_fenced_sync_head() {
    let r = request();
    let mut a = ack(&r);
    a.results[0].outcome = MutationOutcome::Accepted {
        sync_cursor: 10,
        result: Value::Null,
        targets: vec![SettlementTarget::Stream {
            key: RecordKey {
                model: "Entry".into(),
                identity: json!({"id":"e"}),
            },
            cursor: 11,
            fallback: ReadRecord {
                key: RecordKey {
                    model: "Entry".into(),
                    identity: json!({"id":"e"}),
                },
                cursor: (),
                state: Value::Null,
            },
        }],
    };
    assert!(validate_acknowledgement(&r, &a).is_err());
}

fn owned_continuation_fixture() -> (MaterializationRequest, MaterializationResponse) {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/protocol/0.5.json")).unwrap();
    let mut request: MaterializationRequest =
        serde_json::from_value(fixture["materializationRequest"].clone()).unwrap();
    let response: MaterializationResponse =
        serde_json::from_value(fixture["materializationResponse"].clone()).unwrap();
    request.continuation = Some(Continuation {
        plan_id: response.delivery.header.plan_id.clone(),
        digest: response.delivery.header.digest.clone(),
        unit: 0,
        part: 0,
    });
    response.admit(&request, &ctx()).unwrap();
    (request, response)
}

#[test]
fn owned_continuation_rejects_replacement_plan_id_under_same_owner_context() {
    let (request, response) = owned_continuation_fixture();
    let header = &response.delivery.header;
    let replacement = freeze_materialization(
        ctx(),
        "replacement-owned".into(),
        request.owner.clone(),
        header.observed_head,
        header.expires_at,
        vec![DeliveryUnit {
            index: 0,
            through: None,
            changes: response.delivery.parts[0].changes.clone(),
        }],
        1,
    )
    .unwrap();
    let replacement = MaterializationResponse {
        request_id: request.request_id.clone(),
        delivery: DeliveryResponse {
            header: replacement.header,
            parts: replacement.parts,
        },
    };
    assert_eq!(
        replacement.delivery.header.context,
        response.delivery.header.context
    );
    assert_eq!(
        replacement.delivery.header.owner,
        response.delivery.header.owner
    );
    assert_ne!(
        replacement.delivery.header.plan_id,
        request.continuation.as_ref().unwrap().plan_id
    );
    assert!(
        replacement.admit(&request, &ctx()).is_err(),
        "owned continuation admitted a replacement plan"
    );
}

#[test]
fn owned_continuation_rejects_changed_digest_under_same_plan_owner_context() {
    let (request, response) = owned_continuation_fixture();
    let header = &response.delivery.header;
    let replacement = freeze_materialization(
        ctx(),
        header.plan_id.clone(),
        request.owner.clone(),
        header.observed_head,
        header.expires_at + 1,
        vec![DeliveryUnit {
            index: 0,
            through: None,
            changes: response.delivery.parts[0].changes.clone(),
        }],
        1,
    )
    .unwrap();
    let replacement = MaterializationResponse {
        request_id: request.request_id.clone(),
        delivery: DeliveryResponse {
            header: replacement.header,
            parts: replacement.parts,
        },
    };
    assert_eq!(
        replacement.delivery.header.context,
        response.delivery.header.context
    );
    assert_eq!(
        replacement.delivery.header.owner,
        response.delivery.header.owner
    );
    assert_eq!(
        replacement.delivery.header.plan_id,
        request.continuation.as_ref().unwrap().plan_id
    );
    assert_ne!(
        replacement.delivery.header.digest,
        request.continuation.as_ref().unwrap().digest
    );
    assert!(
        replacement.admit(&request, &ctx()).is_err(),
        "owned continuation admitted a changed plan digest"
    );
}
