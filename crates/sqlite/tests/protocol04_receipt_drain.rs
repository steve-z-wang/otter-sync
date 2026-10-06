//! Ready accepted receipts must drain through the actual actor, including after reopen.
use axton_client::{
    Client, RecordKey, Schema,
    runtime::{ClientRuntime, Input},
    v04,
};
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};

fn drain(r: &mut ClientRuntime<SqliteStore>) -> Vec<Value> {
    for _ in 0..1000 {
        if !r.step(1, 7) {
            return r
                .take_events()
                .into_iter()
                .map(|e| serde_json::to_value(e).unwrap())
                .collect();
        }
    }
    panic!("runtime did not reach quiescence");
}
fn answer(r: &mut ClientRuntime<SqliteStore>, effect: &Value, value: Value) -> Vec<Value> {
    r.receive(serde_json::from_value::<Input>(json!({"type":"effectResult","effectId":effect["effectId"],"outcome":{"ok":true,"value":value}})).unwrap(),1,7).unwrap();
    drain(r)
}
fn http(events: &[Value], kind: &str) -> Value {
    events
        .iter()
        .find(|e| {
            e["operation"]["kind"] == "http"
                && serde_json::from_str::<Value>(e["operation"]["body"].as_str().unwrap()).unwrap()
                    ["kind"]
                    == kind
        })
        .unwrap_or_else(|| panic!("missing {kind}: {events:?}"))
        .clone()
}
#[test]
fn reopened_actor_drains_two_accepted_receipts_after_one_authority_page() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
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
    let mut c = Client::open_bound(
        SqliteStore::open_exclusive(&path).unwrap(),
        schema.clone(),
        binding.clone(),
    )
    .unwrap();
    let ctx = c.request_context().unwrap().clone();
    let key = RecordKey {
        model: "Entry".into(),
        identity: json!({"id":"e"}),
    };
    c.apply_cache04(
        &ctx,
        &[v04::ReadRecord {
            key: key.clone(),
            cursor: v04::NullCursor,
            state: json!({"text":"old","note":null}),
        }],
        true,
    )
    .unwrap();
    let mut calls = Vec::new();
    for (cursor, text) in [(260, "first"), (261, "second")] {
        let call = c
            .submit_action("Rename", 1, json!({"entry":{"id":"e","text":text}}))
            .unwrap();
        let intent = c.mutation_intent04(&call.call_id).unwrap().unwrap();
        c.save_receipt04(&v04::MutationReceipt {
            context: ctx.clone(),
            intent_digest: intent.digest().unwrap(),
            completion: axton_client::CallCompletion {
                call_id: call.call_id.clone(),
                outcome: axton_client::ActionOutcome::Succeeded {
                    result: Value::Null,
                },
            },
            targets: vec![v04::SettlementTarget::Stream {
                key: key.clone(),
                cursor,
                fallback: v04::ReadRecord {
                    key: key.clone(),
                    cursor: v04::NullCursor,
                    state: json!({"text":text,"note":null}),
                },
            }],
        })
        .unwrap();
        calls.push(call.call_id);
    }
    assert_eq!(c.pending_count().unwrap(), 2);
    drop(c);
    let mut r = ClientRuntime::new(
        Client::open_bound(SqliteStore::open_exclusive(&path).unwrap(), schema, binding).unwrap(),
    );
    r.receive(
        serde_json::from_value::<Input>(
            json!({"type":"task","requestId":"connect","command":{"kind":"connect"}}),
        )
        .unwrap(),
        1,
        7,
    )
    .unwrap();
    let events = drain(&mut r);
    let events = answer(
        &mut r,
        &http(&events, "start"),
        json!({"status":200,"body":json!({"context":ctx,"manifestId":"m","start":259,"total":0}).to_string()}),
    );
    let socket = events
        .iter()
        .find(|e| e["operation"]["kind"] == "socket")
        .unwrap()
        .clone();
    let tail = http(&events, "tail");
    answer(
        &mut r,
        &socket,
        json!({"event":"message","body":json!({"context":ctx,"cursor":259,"head":261}).to_string()}),
    );
    answer(
        &mut r,
        &tail,
        json!({"status":200,"body":json!({"context":ctx,"manifestId":"m","head":261}).to_string()}),
    );
    // A compacted group installs G=261 while proving the first covered prefix;
    // its second empty unit then proves C=261. No SQL authority writes are used.
    let page = v04::DeltaPage {
        context: ctx,
        page_id: "p".into(),
        from: 259,
        to: 261,
        head: 261,
        units: vec![
            v04::CommitUnit {
                through: 260,
                changes: vec![v04::StreamChange::Upsert {
                    record: v04::StreamRecord {
                        key: key.clone(),
                        cursor: 261,
                        state: json!({"text":"second","note":null}),
                    },
                }],
            },
            v04::CommitUnit {
                through: 261,
                changes: vec![],
            },
        ],
    };
    let events = answer(
        &mut r,
        &socket,
        json!({"event":"message","body":String::from_utf8(v04::encode(&page).unwrap()).unwrap()}),
    );
    assert_eq!(r.client().stream_cursor04().unwrap(), 261);
    assert_eq!(r.client().read(&key).unwrap().unwrap()["text"], "second");
    // The actor must drain ready settlements before making the network cycle
    // Idle. The first completed Call must not strand the second one.
    assert!(r.client().call_completion04(&calls[0]).unwrap().is_some());
    assert_eq!(
        r.client().pending_count().unwrap(),
        0,
        "ready accepted Calls stranded at quiescence: {events:?}"
    );
    for call in calls {
        assert!(
            r.client().call_completion04(&call).unwrap().is_some(),
            "missing durable completion for {call}"
        );
        assert!(
            events
                .iter()
                .any(|e| e["type"] == "callCompleted" && e["callId"] == call),
            "missing completion event for {call}: {events:?}"
        );
    }
}
