use axton_client::{
    Client, Operation, OperationKind, RecordKey, Schema,
    runtime::{ClientRuntime, Input},
    v04,
};
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};
fn run(r: &mut ClientRuntime<SqliteStore>) -> Vec<Value> {
    while r.step(1, 7) {}
    r.take_events()
        .into_iter()
        .map(|e| serde_json::to_value(e).unwrap())
        .collect()
}
fn task(r: &mut ClientRuntime<SqliteStore>, id: &str, command: Value) -> Vec<Value> {
    r.receive(
        serde_json::from_value::<Input>(json!({"type":"task","requestId":id,"command":command}))
            .unwrap(),
        1,
        7,
    )
    .unwrap();
    run(r)
}
#[test]
fn reset_fences_old_fetch_and_receipt_and_reopens_with_new_incarnation_under_same_binding() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut raw: Value =
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap();
    raw["actions"] = json!([{"name":"Rename","version":1,"inputs":[{"kind":"model","name":"entry","model":"Entry","operation":"update","cardinality":"single"}],"outputs":[]}]);
    let schema = Schema::from_value(raw).unwrap();
    let binding = v04::StoreBinding {
        backend: "b".into(),
        viewer: "a".into(),
        stream: "User:a".into(),
        contract: "app".into(),
    };
    let key = RecordKey {
        model: "Entry".into(),
        identity: json!({"id":"e"}),
    };
    let mut c = Client::open_bound(
        SqliteStore::open_exclusive(&p).unwrap(),
        schema.clone(),
        binding.clone(),
    )
    .unwrap();
    let old = c.request_context().unwrap().clone();
    c.install_stream04(
        &old,
        &v04::StreamRecord {
            key: key.clone(),
            cursor: 57,
            state: json!({"text":"base","note":null}),
        },
    )
    .unwrap();
    c.transaction(|tx| {
        tx.direct(Operation {
            model: "Entry".into(),
            identity: key.identity.clone(),
            op: OperationKind::Update,
            values: Some(json!({"text":"direct"})),
        })
    })
    .unwrap();
    let call = c
        .submit_action("Rename", 1, json!({"entry":{"id":"e","text":"pending"}}))
        .unwrap();
    let intent = c.mutation_intent04(&call.call_id).unwrap().unwrap();
    let receipt = v04::MutationReceipt {
        context: old.clone(),
        intent_digest: intent.digest().unwrap(),
        completion: axton_client::CallCompletion {
            call_id: call.call_id.clone(),
            outcome: axton_client::ActionOutcome::Succeeded {
                result: Value::Null,
            },
        },
        targets: vec![v04::SettlementTarget::Stream {
            key: key.clone(),
            cursor: 58,
            fallback: v04::ReadRecord {
                key: key.clone(),
                cursor: v04::NullCursor,
                state: json!({"text":"accepted","note":null}),
            },
        }],
    };
    c.save_receipt04(&receipt).unwrap();
    let mut r = ClientRuntime::new(c);
    task(&mut r, "connect", json!({"kind":"connect"}));
    let effects = task(
        &mut r,
        "fetch",
        json!({"kind":"fetch","model":"Entry","version":1,"identity":{"id":"e"}}),
    );
    let fetch = effects
        .iter()
        .find(|e| e["operation"]["route"] == "fetch")
        .unwrap()
        .clone();
    let wire: Value = serde_json::from_str(fetch["operation"]["body"].as_str().unwrap()).unwrap();
    let refused = task(&mut r, "keep", json!({"kind":"resetStore"}));
    assert_eq!(
        refused.iter().find(|e| e["requestId"] == "keep").unwrap()["ok"],
        false
    );
    assert_eq!(r.client().pending_count().unwrap(), 1);
    let reset = task(
        &mut r,
        "reset",
        json!({"kind":"resetStore","discardPending":true}),
    );
    assert_eq!(
        reset.iter().find(|e| e["requestId"] == "reset").unwrap()["ok"],
        true
    );
    let active = r.client().request_context().unwrap().clone();
    assert_ne!(active.incarnation, old.incarnation);
    assert_eq!(active.binding, old.binding);
    assert_eq!(r.client().pending_count().unwrap(), 0);
    assert_eq!(r.client().stream_cursor04().unwrap(), 0);
    assert!(r.client().read(&key).unwrap().is_none());
    assert_eq!(
        r.client().record_evidence04(&key).unwrap(),
        v04::RecordEvidence::default()
    );
    assert!(r.client().save_receipt04(&receipt).is_err());
    let response = json!({"context":old,"completion":{"callId":wire["callId"],"outcome":{"status":"succeeded","result":{"id":"e","text":"late","note":null}}},"records":[{"model":"Entry","identity":{"id":"e"},"cursor":null,"state":{"text":"late","note":null}}]});
    r.receive(serde_json::from_value(json!({"type":"effectResult","effectId":fetch["effectId"],"outcome":{"ok":true,"value":{"status":200,"body":response.to_string()}}})).unwrap(),1,7).unwrap();
    run(&mut r);
    assert!(r.client().read(&key).unwrap().is_none());
    drop(r);
    let reopened =
        Client::open_bound(SqliteStore::open_exclusive(&p).unwrap(), schema, binding).unwrap();
    assert_eq!(reopened.request_context().unwrap(), &active);
}
