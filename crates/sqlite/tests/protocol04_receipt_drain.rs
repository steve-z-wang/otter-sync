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

fn entry_key(id: &str) -> RecordKey {
    RecordKey {
        model: "Entry".into(),
        identity: json!({"id":id}),
    }
}
fn mixed_targets_recover_without_blocking_a_later_ready_receipt(remove: bool) {
    let dir = tempfile::tempdir().unwrap();
    let mut raw: Value =
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap();
    raw["actions"] = json!([
        {"name":"Rename","version":1,"inputs":[{"kind":"model","name":"entry","model":"Entry","operation":"update","cardinality":"single"}],"outputs":[]},
        {"name":"RenameMany","version":1,"inputs":[{"kind":"model","name":"entries","model":"Entry","operation":"update","cardinality":"list"}],"outputs":[]}
    ]);
    let mut c = Client::open_bound(
        SqliteStore::open_exclusive(dir.path().join("db")).unwrap(),
        Schema::from_value(raw).unwrap(),
        v04::StoreBinding {
            backend: "b".into(),
            viewer: "a".into(),
            stream: "User:a".into(),
            contract: "app".into(),
        },
    )
    .unwrap();
    let ctx = c.request_context().unwrap().clone();
    let snapshot = |id: &str, text: &str| v04::ReadRecord {
        key: entry_key(id),
        cursor: v04::NullCursor,
        state: json!({"text":text,"note":null}),
    };
    c.apply_cache04(
        &ctx,
        &[
            snapshot("private", "base-private"),
            snapshot("missing", "base-missing"),
            snapshot("ready", "base-ready"),
        ],
        true,
    )
    .unwrap();
    c.install_stream04(
        &ctx,
        &v04::StreamRecord {
            key: entry_key("covered"),
            cursor: 57,
            state: json!({"text":"covered-authority","note":null}),
        },
    )
    .unwrap();
    let pending = c.submit_action("RenameMany", 1, json!({"entries":[{"id":"private","text":"pending-private"},{"id":"missing","text":"pending-missing"},{"id":"covered","text":"pending-covered"}]})).unwrap();
    let ready = c
        .submit_action(
            "Rename",
            1,
            json!({"entry":{"id":"ready","text":"pending-ready"}}),
        )
        .unwrap();
    let intent = c.mutation_intent04(&pending.call_id).unwrap().unwrap();
    let completion = axton_client::CallCompletion {
        call_id: pending.call_id.clone(),
        outcome: axton_client::ActionOutcome::Succeeded {
            result: Value::Null,
        },
    };
    c.save_receipt04(&v04::MutationReceipt {
        context: ctx.clone(),
        intent_digest: intent.digest().unwrap(),
        completion: completion.clone(),
        targets: vec![
            v04::SettlementTarget::Private {
                record: snapshot("private", "accepted-private"),
            },
            v04::SettlementTarget::Stream {
                key: entry_key("missing"),
                cursor: 60,
                fallback: snapshot("missing", "accepted-fallback"),
            },
            v04::SettlementTarget::Stream {
                key: entry_key("covered"),
                cursor: 57,
                fallback: snapshot("covered", "unused-fallback"),
            },
        ],
    })
    .unwrap();
    let intent = c.mutation_intent04(&ready.call_id).unwrap().unwrap();
    c.save_receipt04(&v04::MutationReceipt {
        context: ctx.clone(),
        intent_digest: intent.digest().unwrap(),
        completion: axton_client::CallCompletion {
            call_id: ready.call_id.clone(),
            outcome: axton_client::ActionOutcome::Succeeded {
                result: Value::Null,
            },
        },
        targets: vec![v04::SettlementTarget::Private {
            record: snapshot("ready", "accepted-ready"),
        }],
    })
    .unwrap();
    let report = c.settle_receipts04().unwrap();
    assert_eq!(report.completions.len(), 1);
    assert_eq!(
        report.completions[0].call_id, ready.call_id,
        "an earlier unready receipt cannot block independent ready work"
    );
    assert_eq!(c.pending_count().unwrap(), 1);
    assert!(c.call_completion04(&pending.call_id).unwrap().is_none());
    assert_eq!(
        c.read(&entry_key("private")).unwrap().unwrap()["text"],
        "pending-private",
        "private target cannot finalize independently of its receipt"
    );
    assert_eq!(
        c.read(&entry_key("ready")).unwrap().unwrap()["text"],
        "accepted-ready"
    );
    let mut runtime = ClientRuntime::new(c);
    runtime
        .receive(
            serde_json::from_value::<Input>(
                json!({"type":"task","requestId":"connect","command":{"kind":"connect"}}),
            )
            .unwrap(),
            1,
            7,
        )
        .unwrap();
    let events = drain(&mut runtime);
    let materialize = http(&events, "materialize");
    let start = http(&events, "start");
    // C is already beyond the historical target, which still needs its own proof.
    let events = answer(
        &mut runtime,
        &start,
        json!({"status":200,"body":json!({"context":ctx,"manifestId":"public","start":80,"total":0}).to_string()}),
    );
    let socket = events
        .iter()
        .find(|e| e["operation"]["kind"] == "socket")
        .unwrap();
    let tail = http(&events, "tail");
    answer(
        &mut runtime,
        socket,
        json!({"event":"message","body":json!({"context":ctx,"cursor":80,"head":80}).to_string()}),
    );
    answer(
        &mut runtime,
        &tail,
        json!({"status":200,"body":json!({"context":ctx,"manifestId":"public","head":80}).to_string()}),
    );
    let request: v04::BootstrapIntent = v04::decode(
        materialize["operation"]["body"]
            .as_str()
            .unwrap()
            .as_bytes(),
    )
    .unwrap();
    let v04::BootstrapIntent::Materialize {
        receipt_targets, ..
    } = request
    else {
        panic!("wrong request");
    };
    assert_eq!(receipt_targets.call_id, pending.call_id);
    assert_eq!(
        receipt_targets.keys,
        vec![entry_key("missing")],
        "private and already-covered targets earn no missing-proof request"
    );
    assert!(
        runtime
            .client()
            .call_completion04(&pending.call_id)
            .unwrap()
            .is_none()
    );
    let events = answer(
        &mut runtime,
        &materialize,
        json!({"status":200,"body":json!({"context":ctx,"manifestId":"receipt","start":90,"total":1}).to_string()}),
    );
    let page = http(&events, "page");
    let change = if remove {
        v04::StreamChange::Remove {
            key: entry_key("missing"),
            cursor: 61,
        }
    } else {
        v04::StreamChange::Upsert {
            record: v04::StreamRecord {
                key: entry_key("missing"),
                cursor: 60,
                state: json!({"text":"materialized-authority","note":null}),
            },
        }
    };
    let manifest = v04::ManifestPage {
        context: ctx,
        manifest_id: "receipt".into(),
        total: 1,
        from: 0,
        to: 1,
        items: vec![v04::ManifestItem { ordinal: 0, change }],
        companions: vec![],
    };
    let events = answer(
        &mut runtime,
        &page,
        json!({"status":200,"body":String::from_utf8(v04::encode(&manifest).unwrap()).unwrap()}),
    );
    assert_eq!(
        runtime
            .client()
            .call_completion04(&pending.call_id)
            .unwrap(),
        Some(completion)
    );
    assert_eq!(runtime.client().pending_count().unwrap(), 0);
    assert!(
        events
            .iter()
            .any(|e| e["type"] == "callCompleted" && e["callId"] == pending.call_id)
    );
    assert_eq!(
        runtime
            .client()
            .read(&entry_key("private"))
            .unwrap()
            .unwrap()["text"],
        "accepted-private"
    );
    assert_eq!(
        runtime
            .client()
            .read(&entry_key("covered"))
            .unwrap()
            .unwrap()["text"],
        "covered-authority"
    );
    assert_eq!(
        runtime
            .client()
            .read(&entry_key("missing"))
            .unwrap()
            .unwrap()["text"],
        if remove {
            "accepted-fallback"
        } else {
            "materialized-authority"
        }
    );
    assert_eq!(
        runtime.client().stream_cursor04().unwrap(),
        80,
        "receipt coverage/fallback never invents a public prefix"
    );
    let evidence = runtime
        .client()
        .record_evidence04(&entry_key("missing"))
        .unwrap();
    if remove {
        assert!(evidence.history.is_empty());
        assert!(evidence.current.is_none());
        let membership = evidence.membership.unwrap();
        assert!(!membership.live);
        assert_eq!(membership.cursor, 61);
    } else {
        assert_eq!(evidence.current.unwrap().cursor, 60);
    }
}
#[test]
fn mixed_receipt_requests_only_missing_proof_and_does_not_starve_ready_receipt() {
    mixed_targets_recover_without_blocking_a_later_ready_receipt(false);
}
#[test]
fn mixed_receipt_later_remove_reassesses_fallback_without_fabricating_authority() {
    mixed_targets_recover_without_blocking_a_later_ready_receipt(true);
}
