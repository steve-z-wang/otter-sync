//! Unsent work account-wide: refused acts keep the act as submitted, failed
//! acts list their failed tasks, a new requirement on a failed task inherits
//! the failure, and every resolution also runs inside a transaction
//! ([#186](https://github.com/zanminwang/axton/issues/186),
//! [#205](https://github.com/zanminwang/axton/issues/205),
//! [#204](https://github.com/zanminwang/axton/issues/204)).
use axton_client::*;
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};

/// `Write` edits a Note; its `blob` field requires `RemoteBlob(key: self)`,
/// and a later `Write` of the same Note is sequenced after an earlier one.
/// `Create` makes a Note.
fn schema() -> Schema {
    Schema::from_value(json!({"enums":[],
        "models":[{"name":"Note","version":1,"identity":["id"],"fields":[
            {"name":"id","type":{"kind":"scalar","name":"string"},"nullable":false},
            {"name":"text","type":{"kind":"scalar","name":"string"},"nullable":false},
            {"name":"blob","type":{"kind":"scalar","name":"string"},"nullable":true}]}],
        "prerequisites":[{"name":"RemoteBlob","fields":[{"name":"key","type":"String"}]}],
        "actions":[
            {"name":"Write","version":1,"outputs":[],
             "inputs":[{"kind":"model","name":"note","model":"Note","operation":"update","cardinality":"single"}],
             "requirements":[{"model":"Note","field":"blob","name":"RemoteBlob","arguments":{"key":"self"}}],
             "sequence":{"after":[{"name":"Write","arguments":{"note":"note"}}]}},
            {"name":"Create","version":1,"outputs":[],
             "inputs":[{"kind":"model","name":"note","model":"Note","operation":"create","cardinality":"single"}]}
        ]}))
    .unwrap()
}

fn open(path: &std::path::Path) -> Client<SqliteStore> {
    Client::open(SqliteStore::open(path).unwrap(), schema()).unwrap()
}

/// A client with Note `n` at text `base`, device-only.
fn seeded(dir: &tempfile::TempDir) -> Client<SqliteStore> {
    let mut client = open(&dir.path().join("db"));
    client
        .transaction(|tx| {
            tx.direct(Operation {
                model: "Note".into(),
                op: OperationKind::Create,
                identity: json!({"id":"n"}),
                values: Some(json!({"text":"base","blob":null})),
            })
        })
        .unwrap();
    client
}

fn write(client: &mut Client<SqliteStore>, text: &str, blob: Option<&str>) -> SubmittedCall {
    client
        .submit_action(
            "Write",
            1,
            json!({"note":{"id":"n","text":text,"blob":blob}}),
        )
        .unwrap()
}

fn blob(key: &str) -> String {
    json!({"arguments":{"key":key},"name":"RemoteBlob"}).to_string()
}

fn text(client: &mut Client<SqliteStore>) -> Value {
    client
        .read(&RecordKey {
            model: "Note".into(),
            identity: json!({"id":"n"}),
        })
        .unwrap()
        .unwrap()["text"]
        .clone()
}

fn ordinals(acts: &[FailedAct]) -> Vec<u64> {
    acts.iter().map(|a| a.ordinal).collect()
}

fn count(client: &mut Client<SqliteStore>, sql: &str) -> u64 {
    client.read_sql(sql, &[]).unwrap()[0]["n"].as_u64().unwrap()
}

#[test]
fn a_refused_act_keeps_what_was_submitted_until_it_is_dismissed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut client = seeded(&dir);
    let call = write(&mut client, "the author's words", None);
    client.freeze().unwrap();
    let receipt = PushReceipt {
        client_id: client.client_id().into(),
        batch_sequence: 1,
        rejections: vec![Rejection {
            ordinal: call.ordinal,
            code: "note.denied".into(),
        }],
        completions: vec![CallCompletion {
            call_id: call.call_id.clone(),
            outcome: ActionOutcome::Failed {
                code: "note.denied".into(),
                execution: ExecutionState::Rejected,
            },
        }],
        records: vec![],
    };
    client.acknowledge(1, receipt).unwrap();
    assert_eq!(text(&mut client), "base", "the optimism is gone");
    drop(client);
    // Retained across a reopen, with the act as submitted.
    let mut client = open(&path);
    let refused = client.refused_acts().unwrap();
    assert_eq!(refused.len(), 1);
    let act = serde_json::to_value(&refused[0]).unwrap();
    assert_eq!(
        act,
        json!({"id":call.ordinal,"name":"Write","version":1,"code":"note.denied","act":{
            "args":{"note":{"id":"n","text":"the author's words","blob":null}},
            "operations":[{"model":"Note","op":"update","identity":{"id":"n"},
                "values":{"text":"the author's words","blob":null}}]}})
    );
    // The author's words come back from the retained act.
    assert_eq!(
        refused[0].act.args.as_ref().unwrap()["note"]["text"],
        "the author's words"
    );
    assert_eq!(
        serde_json::to_value(client.refused_act(call.ordinal).unwrap()).unwrap(),
        act
    );
    assert!(client.refused_act(call.ordinal + 1).unwrap().is_none());
    client.dismiss_rejection(call.ordinal).unwrap();
    assert!(client.refused_acts().unwrap().is_empty());
    assert!(client.refused_act(call.ordinal).unwrap().is_none());
}

#[test]
fn a_legacy_refusal_has_no_args_and_a_malformed_detail_reads_as_an_empty_act() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut client = open(&path);
    let ordinal = client
        .transaction(|tx| {
            tx.enqueue(Mutation::new(
                "Legacy",
                vec![Operation {
                    model: "Note".into(),
                    op: OperationKind::Create,
                    identity: json!({"id":"l"}),
                    values: Some(json!({"text":"legacy","blob":null})),
                }],
            ))
        })
        .unwrap();
    client.drop_mutation(ordinal).unwrap();
    let refused = client.refused_acts().unwrap();
    assert_eq!(refused[0].code, "dropped");
    assert_eq!(refused[0].act.args, None);
    assert_eq!(refused[0].act.operations[0].identity, json!({"id":"l"}));
    drop(client);
    let mut store = SqliteStore::open(&path).unwrap();
    store
        .execute(
            "INSERT INTO axton_rejection (ordinal, name, code, detail) VALUES (99, 'Old', 'denied', '\"not an object\"')",
            &[],
        )
        .unwrap();
    drop(store);
    let mut client = open(&path);
    let old = client.refused_act(99).unwrap().unwrap();
    assert_eq!((old.name.as_str(), old.version), ("Old", 1));
    assert_eq!(old.act.args, None);
    assert!(old.act.operations.is_empty());
}

#[test]
fn a_terminal_failure_lists_the_act_and_a_retry_makes_its_task_pending() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = seeded(&dir);
    let call = write(&mut client, "with a photo", Some("X"));
    assert!(
        client.failed_acts().unwrap().is_empty(),
        "pending, not failed"
    );
    client.outcome(&blob("X"), Some("upload refused")).unwrap();
    let failed = client.failed_acts().unwrap();
    assert_eq!(
        serde_json::to_value(&failed).unwrap(),
        json!([{"ordinal":call.ordinal,"name":"Write","version":1,
            "act":{"args":{"note":{"id":"n","text":"with a photo","blob":"X"}},
                "operations":[{"model":"Note","op":"update","identity":{"id":"n"},
                    "values":{"text":"with a photo","blob":"X"}}]},
            "tasks":[{"key":blob("X"),"name":"RemoteBlob","arguments":{"key":"X"},"error":"upload refused"}]}])
    );
    client.retry_tasks(&[blob("X")]).unwrap();
    assert!(client.failed_acts().unwrap().is_empty());
    assert_eq!(client.pending_tasks().unwrap()[0]["state"], "pending");
    // An opaque key names no prerequisite.
    client
        .transaction(|tx| {
            let mut m = Mutation::new(
                "Legacy",
                vec![Operation {
                    model: "Note".into(),
                    op: OperationKind::Update,
                    identity: json!({"id":"n"}),
                    values: Some(json!({"text":"opaque"})),
                }],
            );
            m.prerequisites.push("upload:1".into());
            tx.enqueue(m)
        })
        .unwrap();
    client.set_readiness("upload:1", Readiness::Failed).unwrap();
    let failed = client.failed_acts().unwrap();
    assert_eq!(failed[0].act.args, None);
    assert_eq!(failed[0].tasks[0].name, None);
    assert_eq!(failed[0].tasks[0].arguments, None);
    // An unknown key changes nothing.
    client.retry_tasks(&["nothing".into()]).unwrap();
    assert_eq!(client.failed_acts().unwrap().len(), 1);
}

#[test]
fn a_new_requirement_on_a_failed_task_inherits_its_failure_and_one_retry_covers_both() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = seeded(&dir);
    let first = write(&mut client, "one", Some("X"));
    client.outcome(&blob("X"), Some("upload refused")).unwrap();
    let second = write(&mut client, "two", Some("X"));
    // Act 2 reports the failure at once; its own row carries it.
    let failed = client.failed_acts().unwrap();
    assert_eq!(ordinals(&failed), vec![first.ordinal, second.ordinal]);
    assert_eq!(failed[1].tasks[0].error, "upload refused");
    let rows = client
        .read_sql(
            "SELECT ordinal, error FROM axton_mutation_prerequisite ORDER BY ordinal",
            &[],
        )
        .unwrap();
    assert_eq!(
        rows,
        vec![
            json!({"ordinal":first.ordinal,"error":"upload refused"}),
            json!({"ordinal":second.ordinal,"error":"upload refused"})
        ]
    );
    // Not reset: the task stays failed and nothing is sendable.
    assert_eq!(client.pending_tasks().unwrap()[0]["state"], "failed");
    assert!(client.freeze().unwrap().is_none());
    // One retry covers every act waiting on the task.
    client.retry_tasks(&[blob("X")]).unwrap();
    assert!(client.failed_acts().unwrap().is_empty());
    client.outcome(&blob("X"), None).unwrap();
    let request =
        PushRequest::decode_actions(&client.freeze().unwrap().unwrap(), &schema()).unwrap();
    let sent: Vec<u64> = request.mutations.iter().map(|m| m.ordinal).collect();
    assert_eq!(sent, vec![first.ordinal, second.ordinal]);
    // A requirement on a task nobody waits on any more starts pending.
    let dir = tempfile::tempdir().unwrap();
    let mut client = seeded(&dir);
    let first = write(&mut client, "one", Some("Y"));
    client.outcome(&blob("Y"), Some("refused")).unwrap();
    client.discard(first.ordinal).unwrap();
    write(&mut client, "two", Some("Y"));
    assert!(client.failed_acts().unwrap().is_empty());
    assert_eq!(client.pending_tasks().unwrap()[0]["state"], "pending");
}

#[test]
fn a_discard_records_no_refusal_and_refuses_a_lifecycle_dependent() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    let create = client
        .submit_action(
            "Create",
            1,
            json!({"note":{"id":"m","text":"new","blob":null}}),
        )
        .unwrap();
    let edit = client
        .submit_action(
            "Write",
            1,
            json!({"note":{"id":"m","text":"edited","blob":null}}),
        )
        .unwrap();
    let completions = client.discard(create.ordinal).unwrap();
    assert_eq!(
        completions,
        vec![
            CallCompletion {
                call_id: create.call_id,
                outcome: ActionOutcome::Failed {
                    code: "dropped".into(),
                    execution: ExecutionState::Rejected
                }
            },
            CallCompletion {
                call_id: edit.call_id,
                outcome: ActionOutcome::Failed {
                    code: "dependency.rejected".into(),
                    execution: ExecutionState::Rejected
                }
            }
        ]
    );
    assert_eq!(client.pending_count().unwrap(), 0);
    assert_eq!(count(&mut client, "SELECT COUNT(*) AS n FROM Note"), 0);
    let refused = client.refused_acts().unwrap();
    assert_eq!(refused.len(), 1, "only the dependent is refused");
    assert_eq!(
        (refused[0].id, refused[0].code.as_str()),
        (edit.ordinal, "dependency.rejected")
    );
    // An unknown ordinal changes nothing; a sent act cannot be discarded.
    assert!(client.discard(create.ordinal).unwrap().is_empty());
    let dir = tempfile::tempdir().unwrap();
    let mut client = seeded(&dir);
    let sent = write(&mut client, "sent", None);
    client.freeze().unwrap();
    assert!(client.discard(sent.ordinal).is_err());
    assert_eq!(client.pending_count().unwrap(), 1);
}

/// The #205 reproducer: the repair replaces a failed act atomically.
#[test]
fn a_replacement_in_the_transaction_that_discards_the_original_is_planned_without_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = seeded(&dir);
    let original = write(&mut client, "draft", Some("X"));
    client.outcome(&blob("X"), Some("upload refused")).unwrap();
    assert_eq!(text(&mut client), "draft");
    let (seen, replacement) = client
        .transaction(|tx| {
            let completions = tx.discard(original.ordinal)?;
            assert_eq!(completions[0].call_id, original.call_id);
            let seen = tx
                .read(&RecordKey {
                    model: "Note".into(),
                    identity: json!({"id":"n"}),
                })?
                .unwrap()["text"]
                .clone();
            let replacement = tx.submit_mutation(
                "Write",
                1,
                json!({"note":{"id":"n","text":"fixed","blob":null}}),
                ActionCallOptions::default(),
            )?;
            Ok((seen, replacement))
        })
        .unwrap();
    assert_eq!(seen, "base", "planned over the base, not the original");
    assert_eq!(text(&mut client), "fixed");
    assert_eq!(
        count(
            &mut client,
            "SELECT COUNT(*) AS n FROM axton_mutation_dependency"
        ),
        0,
        "not sequenced after the original"
    );
    assert!(client.refused_acts().unwrap().is_empty());
    assert!(client.failed_acts().unwrap().is_empty());
    assert!(client.pending_tasks().unwrap().is_empty());
    // It is sent alone and accepted.
    let request =
        PushRequest::decode_actions(&client.freeze().unwrap().unwrap(), &schema()).unwrap();
    assert_eq!(request.mutations.len(), 1);
    assert_eq!(request.mutations[0].ordinal, replacement.ordinal);
    let receipt = PushReceipt {
        client_id: client.client_id().into(),
        batch_sequence: 1,
        rejections: vec![],
        completions: vec![CallCompletion {
            call_id: replacement.call_id,
            outcome: ActionOutcome::Succeeded {
                result: Value::Null,
            },
        }],
        records: vec![AuthorityRecord {
            model: "Note".into(),
            identity: json!({"id":"n"}),
            stamp: 1,
            state: json!({"text":"fixed","blob":null}),
            error: None,
        }],
    };
    client.acknowledge(1, receipt).unwrap();
    assert_eq!(client.pending_count().unwrap(), 0);
    assert_eq!(text(&mut client), "fixed");
}

#[test]
fn a_failure_after_the_discard_rolls_both_back_and_the_original_is_intact() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = seeded(&dir);
    let original = write(&mut client, "draft", Some("X"));
    client.outcome(&blob("X"), Some("upload refused")).unwrap();
    let failed = client
        .transaction(|tx| {
            tx.discard(original.ordinal)?;
            tx.submit_mutation(
                "Write",
                1,
                json!({"note":{"id":"n","text":"fixed","blob":null}}),
                ActionCallOptions::default(),
            )?;
            tx.dismiss_rejection(1)?;
            tx.retry_tasks(&[blob("X")])?;
            Err::<(), _>(invalid("the author cancelled"))
        })
        .unwrap_err();
    assert!(failed.to_string().contains("the author cancelled"));
    assert_eq!(
        text(&mut client),
        "draft",
        "the original's optimism is back"
    );
    assert_eq!(
        ordinals(&client.failed_acts().unwrap()),
        vec![original.ordinal]
    );
    assert_eq!(client.pending_count().unwrap(), 1);
    assert!(client.refused_acts().unwrap().is_empty());
    // A savepoint rollback undoes only the resolutions made in it.
    client
        .transaction(|tx| {
            let _ = tx.savepoint(|tx| {
                tx.discard(original.ordinal)?;
                Err::<(), _>(invalid("undo"))
            });
            tx.retry_tasks(&[blob("X")])
        })
        .unwrap();
    assert_eq!(client.pending_count().unwrap(), 1);
    assert!(
        client.failed_acts().unwrap().is_empty(),
        "the retry committed"
    );
}

#[test]
fn dismissing_in_a_transaction_commits_or_rolls_back_with_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = seeded(&dir);
    let call = write(&mut client, "x", None);
    client.drop_action(call.ordinal).unwrap();
    assert_eq!(client.refused_acts().unwrap().len(), 1);
    let _ = client.transaction(|tx| {
        tx.dismiss_rejection(call.ordinal)?;
        Err::<(), _>(invalid("no"))
    });
    assert_eq!(client.refused_acts().unwrap().len(), 1);
    client
        .transaction(|tx| tx.dismiss_rejection(call.ordinal))
        .unwrap();
    assert!(client.refused_acts().unwrap().is_empty());
}

/// The order of a drop and a new requirement on the same failed key decides
/// (ruled 2026-09-28 on #205): a call submitted while the failed call still
/// waits inherits the failure, but once the drop removed the only waiting
/// call the failed task is gone, and a replacement submitted after it starts
/// a fresh, pending task. Both orders in one transaction.
#[test]
fn in_one_transaction_a_replacement_submitted_after_the_drop_starts_afresh_but_one_submitted_before_it_inherits_the_failure()
 {
    let submit = |tx: &mut ClientTransaction<'_, SqliteStore>, text: &str| {
        tx.submit_mutation(
            "Write",
            1,
            json!({"note":{"id":"n","text":text,"blob":"X"}}),
            ActionCallOptions::default(),
        )
    };
    // Drop first: the replacement's task is pending, and a handler runs it.
    let dir = tempfile::tempdir().unwrap();
    let mut client = seeded(&dir);
    let original = write(&mut client, "draft", Some("X"));
    client.outcome(&blob("X"), Some("upload refused")).unwrap();
    client
        .transaction(|tx| {
            tx.discard(original.ordinal)?;
            submit(tx, "fixed")
        })
        .unwrap();
    assert!(client.failed_acts().unwrap().is_empty());
    assert_eq!(client.pending_tasks().unwrap()[0]["state"], "pending");
    assert_eq!(client.pending_count().unwrap(), 1);
    // Submit first, then drop: the replacement joined the failed task and
    // keeps its failure after the original is gone.
    let dir = tempfile::tempdir().unwrap();
    let mut client = seeded(&dir);
    let original = write(&mut client, "draft", Some("X"));
    client.outcome(&blob("X"), Some("upload refused")).unwrap();
    let late = client
        .transaction(|tx| {
            let call = submit(tx, "fixed")?;
            tx.discard(original.ordinal)?;
            Ok(call)
        })
        .unwrap();
    assert_eq!(ordinals(&client.failed_acts().unwrap()), vec![late.ordinal]);
    assert_eq!(client.pending_tasks().unwrap()[0]["state"], "failed");
}

/// A call whose row was written before failures were inherited, with a NULL
/// error beside a failed row of the same key, is still listed: the key
/// decides, not the row.
#[test]
fn a_call_written_before_inheritance_is_listed_by_its_failed_key() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut client = seeded(&dir);
    let first = write(&mut client, "one", Some("X"));
    client.outcome(&blob("X"), Some("upload refused")).unwrap();
    let second = write(&mut client, "two", Some("X"));
    drop(client);
    let mut store = SqliteStore::open(&path).unwrap();
    store
        .execute(
            "UPDATE axton_mutation_prerequisite SET error=NULL WHERE ordinal=?",
            &[json!(second.ordinal)],
        )
        .unwrap();
    drop(store);
    let mut client = open(&path);
    assert_eq!(
        ordinals(&client.failed_acts().unwrap()),
        vec![first.ordinal, second.ordinal]
    );
    // A call waiting on no failed key is not read at all.
    write(&mut client, "three", None);
    assert_eq!(client.failed_acts().unwrap().len(), 2);
}
