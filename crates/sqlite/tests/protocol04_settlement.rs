use axton_client::{
    ActionCallOptions, ActionOutcome, CallCompletion, Client, Operation, OperationKind, RecordKey,
    Schema, v04,
};
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};
fn schema() -> Schema {
    let mut s: Value =
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap();
    s["actions"] = json!([{"name":"Rename","version":1,"inputs":[{"kind":"model","name":"entry","model":"Entry","operation":"update","cardinality":"single"}],"outputs":[]}]);
    Schema::from_value(s).unwrap()
}
fn open(p: &std::path::Path) -> Client<SqliteStore> {
    Client::open_bound(
        SqliteStore::open_exclusive(p).unwrap(),
        schema(),
        v04::StoreBinding {
            backend: "b".into(),
            viewer: "a".into(),
            stream: "User:a".into(),
            contract: "app".into(),
        },
    )
    .unwrap()
}
fn key(id: &str) -> RecordKey {
    RecordKey {
        model: "Entry".into(),
        identity: json!({"id":id}),
    }
}
fn stream(c: &mut Client<SqliteStore>, id: &str, n: u64, text: &str) {
    c.install_stream04(
        &c.request_context().unwrap().clone(),
        &v04::StreamRecord {
            key: key(id),
            cursor: n,
            state: json!({"text":text,"note":null}),
        },
    )
    .unwrap();
}
fn receipt(intent: &v04::MutationIntent, id: &str, n: u64) -> v04::MutationReceipt {
    v04::MutationReceipt {
        context: intent.context.clone(),
        intent_digest: intent.digest().unwrap(),
        completion: CallCompletion {
            call_id: intent.call_id.clone(),
            outcome: ActionOutcome::Succeeded {
                result: Value::Null,
            },
        },
        targets: vec![v04::SettlementTarget::Stream {
            key: key(id),
            cursor: n,
            fallback: v04::ReadRecord {
                key: key(id),
                cursor: v04::NullCursor,
                state: json!({"text":"accepted","note":null}),
            },
        }],
    }
}
#[test]
fn accepted_receipt_persists_awaiting_before_stream_then_completes_after_reopen() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut c = open(&p);
    stream(&mut c, "e", 57, "old");
    let call = c
        .submit_action("Rename", 1, json!({"entry":{"id":"e","text":"optimistic"}}))
        .unwrap();
    let intent = c.mutation_intent04(&call.call_id).unwrap().unwrap();
    let answer = receipt(&intent, "e", 58);
    c.save_receipt04(&answer).unwrap();
    assert!(c.settle_receipts04().unwrap().completions.is_empty());
    drop(c);
    let mut c = open(&p);
    assert!(c.accepted_awaiting04(&call.call_id).unwrap());
    stream(&mut c, "e", 58, "accepted");
    assert_eq!(
        c.settle_receipts04().unwrap().completions,
        vec![answer.completion]
    );
    assert_eq!(c.pending_count().unwrap(), 0);
    assert_eq!(c.read(&key("e")).unwrap().unwrap()["text"], "accepted");
}
#[test]
fn companion_acceptance_does_not_retimestamp_work_across_newer_stream_authority() {
    let d = tempfile::tempdir().unwrap();
    let mut c = open(&d.path().join("db"));
    stream(&mut c, "e", 57, "old");
    stream(&mut c, "other", 57, "old-other");
    let call = c
        .transaction(|tx| {
            let call = tx.submit_mutation(
                "Rename",
                1,
                json!({"entry":{"id":"e","text":"optimistic"}}),
                ActionCallOptions::default(),
            )?;
            tx.append_companion(
                call.ordinal,
                Operation {
                    model: "Entry".into(),
                    identity: json!({"id":"other"}),
                    op: OperationKind::Update,
                    values: Some(json!({"text":"companion"})),
                },
            )?;
            Ok(call)
        })
        .unwrap();
    let intent = c.mutation_intent04(&call.call_id).unwrap().unwrap();
    stream(&mut c, "other", 58, "newer-server");
    stream(&mut c, "e", 58, "accepted");
    c.save_receipt04(&receipt(&intent, "e", 58)).unwrap();
    c.settle_receipts04().unwrap();
    assert_eq!(
        c.read(&key("other")).unwrap().unwrap()["text"],
        "newer-server"
    );
    assert_eq!(
        c.record_evidence04(&key("other"))
            .unwrap()
            .current
            .unwrap()
            .cursor,
        58
    );
}
#[test]
fn local_settlement_failure_keeps_received_receipt_durable_and_queue_owned() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut c = open(&p);
    stream(&mut c, "e", 57, "old");
    let call = c
        .submit_action("Rename", 1, json!({"entry":{"id":"e","text":"optimistic"}}))
        .unwrap();
    let intent = c.mutation_intent04(&call.call_id).unwrap().unwrap();
    let mut answer = receipt(&intent, "e", 58);
    answer.targets = vec![v04::SettlementTarget::Private {
        record: v04::ReadRecord {
            key: key("e"),
            cursor: v04::NullCursor,
            state: json!({"text":123,"note":null}),
        },
    }];
    c.transaction(|tx| {
        tx.direct(Operation {
            model: "Entry".into(),
            identity: json!({"id":"e"}),
            op: OperationKind::Update,
            values: Some(json!({"text":"later-direct"})),
        })
    })
    .unwrap();
    c.save_receipt04(&answer).unwrap();
    assert!(c.settle_receipts04().is_err());
    assert!(c.accepted_awaiting04(&call.call_id).unwrap());
    assert_eq!(c.pending_count().unwrap(), 1);
    assert_eq!(c.read(&key("e")).unwrap().unwrap()["text"], "later-direct");
    drop(c);
    let mut c = open(&p);
    assert!(c.accepted_awaiting04(&call.call_id).unwrap());
    assert!(c.settle_receipts04().is_err());
}
#[test]
fn compacted_remove_fallback_preserves_later_direct_and_never_fabricates_required_position() {
    let d = tempfile::tempdir().unwrap();
    let mut c = open(&d.path().join("db"));
    let context = c.request_context().unwrap().clone();
    c.apply_cache04(
        &context,
        &[v04::ReadRecord {
            key: key("e"),
            cursor: v04::NullCursor,
            state: json!({"text":"cached","note":null}),
        }],
        true,
    )
    .unwrap();
    let call = c
        .submit_action("Rename", 1, json!({"entry":{"id":"e","text":"optimistic"}}))
        .unwrap();
    let intent = c.mutation_intent04(&call.call_id).unwrap().unwrap();
    c.transaction(|tx| {
        tx.direct(Operation {
            model: "Entry".into(),
            identity: json!({"id":"e"}),
            op: OperationKind::Update,
            values: Some(json!({"text":"later-direct"})),
        })
    })
    .unwrap();
    c.apply_delta04(&v04::DeltaPage {
        context,
        page_id: "remove".into(),
        from: 0,
        to: 60,
        head: 60,
        units: vec![v04::CommitUnit {
            through: 60,
            changes: vec![v04::StreamChange::Remove {
                key: key("e"),
                cursor: 60,
            }],
        }],
    })
    .unwrap();
    c.save_receipt04(&receipt(&intent, "e", 59)).unwrap();
    assert_eq!(c.settle_receipts04().unwrap().completions.len(), 1);
    assert_eq!(c.read(&key("e")).unwrap().unwrap()["text"], "later-direct");
    let evidence = c.record_evidence04(&key("e")).unwrap();
    assert!(evidence.history.is_empty());
    assert!(evidence.current.is_none());
    assert_eq!(c.stream_cursor04().unwrap(), 60);
}
#[test]
fn queued_intent_retains_complete_old_context_across_added_model_and_bootstrap_toggle() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut c = open(&p);
    stream(&mut c, "e", 57, "old");
    let call = c
        .submit_action("Rename", 1, json!({"entry":{"id":"e","text":"optimistic"}}))
        .unwrap();
    let intent = c.mutation_intent04(&call.call_id).unwrap().unwrap();
    drop(c);
    let mut raw = serde_json::to_value(schema()).unwrap();
    raw["models"][0]["bootstrap"] = json!(true);
    raw["models"][0]["version"] = json!(2);
    raw["models"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"label","nullable":true,"type":{"kind":"scalar","name":"string"}}));
    let mut extra = raw["models"][0].clone();
    extra["name"] = json!("Extra");
    raw["models"].as_array_mut().unwrap().push(extra);
    let mut c = Client::open_bound(
        SqliteStore::open_exclusive(&p).unwrap(),
        Schema::from_value(raw).unwrap(),
        intent.context.binding.clone(),
    )
    .unwrap();
    assert_ne!(
        c.request_context().unwrap().materialization,
        intent.context.materialization
    );
    assert_eq!(c.mutation_intent04(&call.call_id).unwrap().unwrap(), intent);
    assert!(!intent.models.contains_key("Extra"));
    c.install_stream04(
        &c.request_context().unwrap().clone(),
        &v04::StreamRecord {
            key: key("e"),
            cursor: 57,
            state: json!({"text":"refreshed","note":null,"label":"new-schema"}),
        },
    )
    .unwrap();
    assert_eq!(c.read(&key("e")).unwrap().unwrap()["text"], "optimistic");
    c.save_receipt04(&receipt(&intent, "e", 57)).unwrap();
    assert_eq!(c.settle_receipts04().unwrap().completions.len(), 1);
    let row = c.read(&key("e")).unwrap().unwrap();
    assert_eq!(row["text"], "refreshed");
    assert_eq!(row["label"], "new-schema");
}

#[test]
fn definitive_refusal_settles_dependent_named_calls_in_the_same_commit() {
    let d = tempfile::tempdir().unwrap();
    let mut raw = serde_json::to_value(schema()).unwrap();
    raw["actions"].as_array_mut().unwrap().push(json!({"name":"CreateEntry","version":1,"inputs":[{"kind":"model","name":"entry","model":"Entry","operation":"create","cardinality":"single"}],"outputs":[]}));
    let mut c = Client::open_bound(
        SqliteStore::open_exclusive(d.path().join("db")).unwrap(),
        Schema::from_value(raw).unwrap(),
        v04::StoreBinding {
            backend: "b".into(),
            viewer: "a".into(),
            stream: "User:a".into(),
            contract: "app".into(),
        },
    )
    .unwrap();
    let first = c
        .submit_action(
            "CreateEntry",
            1,
            json!({"entry":{"id":"e","text":"created","note":null}}),
        )
        .unwrap();
    let second = c
        .submit_action("Rename", 1, json!({"entry":{"id":"e","text":"dependent"}}))
        .unwrap();
    let intent = c.mutation_intent04(&first.call_id).unwrap().unwrap();
    let receipt = v04::MutationReceipt {
        context: intent.context.clone(),
        intent_digest: intent.digest().unwrap(),
        completion: CallCompletion {
            call_id: first.call_id.clone(),
            outcome: ActionOutcome::Failed {
                code: "create.not_allowed".into(),
                execution: axton_client::ExecutionState::Rejected,
            },
        },
        targets: vec![],
    };
    c.save_receipt04(&receipt).unwrap();
    let report = c.settle_receipts04().unwrap();
    assert_eq!(report.completions.len(), 2);
    assert_eq!(report.completions[0], receipt.completion);
    assert_eq!(report.completions[1].call_id, second.call_id);
    assert_eq!(
        c.call_completion04(&second.call_id).unwrap(),
        Some(report.completions[1].clone())
    );
    assert!(
        matches!(&report.completions[1].outcome,ActionOutcome::Failed{code,..} if code=="dependency.rejected")
    );
    assert_eq!(c.pending_count().unwrap(), 0);
    assert!(c.read(&key("e")).unwrap().is_none());
}

#[test]
fn private_receipt_does_not_restore_old_snapshot_after_newer_authority_and_direct_patch() {
    let d = tempfile::tempdir().unwrap();
    let mut c = open(&d.path().join("db"));
    stream(&mut c, "e", 57, "old");
    let call = c
        .submit_action("Rename", 1, json!({"entry":{"id":"e","text":"optimistic"}}))
        .unwrap();
    let intent = c.mutation_intent04(&call.call_id).unwrap().unwrap();
    let mut answer = receipt(&intent, "e", 57);
    answer.targets = vec![v04::SettlementTarget::Private {
        record: v04::ReadRecord {
            key: key("e"),
            cursor: v04::NullCursor,
            state: json!({"text":"accepted-old","note":"old-note"}),
        },
    }];
    c.install_stream04(
        &c.request_context().unwrap().clone(),
        &v04::StreamRecord {
            key: key("e"),
            cursor: 58,
            state: json!({"text":"new-server","note":"new-note"}),
        },
    )
    .unwrap();
    c.transaction(|tx| {
        tx.direct(Operation {
            model: "Entry".into(),
            identity: json!({"id":"e"}),
            op: OperationKind::Update,
            values: Some(json!({"text":"later-direct"})),
        })
    })
    .unwrap();
    c.save_receipt04(&answer).unwrap();
    assert_eq!(c.settle_receipts04().unwrap().completions.len(), 1);
    let row = c.read(&key("e")).unwrap().unwrap();
    assert_eq!(row["text"], "later-direct");
    assert_eq!(row["note"], "new-note");
    assert_eq!(c.pending_count().unwrap(), 0);
}

#[test]
fn discarded_unsent_call_completion_survives_reopen_and_cannot_be_sent() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut c = open(&p);
    stream(&mut c, "e", 57, "old");
    let call = c
        .submit_action("Rename", 1, json!({"entry":{"id":"e","text":"optimistic"}}))
        .unwrap();
    let completions = c.discard(call.ordinal).unwrap();
    assert_eq!(completions.len(), 1);
    drop(c);
    let mut c = open(&p);
    assert_eq!(
        c.call_completion04(&call.call_id).unwrap(),
        Some(completions[0].clone())
    );
    assert_eq!(c.pending_count().unwrap(), 0);
}
