use axton_client::{Client, Schema, v05};
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};
fn schema() -> Schema {
    let mut s: Value =
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap();
    s["actions"] = json!([{"name":"Write","version":1,"inputs":[{"kind":"model","name":"entries","model":"Entry","operation":"create","cardinality":"list"},{"kind":"model","name":"maybe","model":"Entry","operation":"update","cardinality":"optional"}],"outputs":[]}]);
    Schema::from_value(s).unwrap()
}
#[test]
fn fixed_batch_restores_input_presence_and_excludes_new_members() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut c = Client::open05(SqliteStore::open(&p).unwrap(), schema(), "User:u").unwrap();
    let call = c
        .transaction(|tx| tx.submit_mutation05("Write", 1, json!({"entries":[]}), vec![]))
        .unwrap();
    let b = c.freeze_batch05().unwrap().unwrap();
    assert_eq!(b.mutations[0].id, call.ordinal);
    assert_eq!(
        v05::reconstruct_input(&b.mutations[0].operations).unwrap(),
        json!({"entries":[]})
    );
    c.transaction(|tx| {
        tx.submit_mutation05("Write", 1, json!({"entries":[],"maybe":null}), vec![])
    })
    .unwrap();
    assert_eq!(c.freeze_batch05().unwrap().unwrap(), b);
    drop(c);
    let mut c = Client::open05(SqliteStore::open(&p).unwrap(), schema(), "User:u").unwrap();
    assert_eq!(
        v05::encode(&c.freeze_batch05().unwrap().unwrap()).unwrap(),
        v05::encode(&b).unwrap()
    );
}
#[test]
fn refuses_unsupported_file_without_touching_rollback_or_wal_bytes() {
    SqliteStore::set_application_data_directory("/private/tmp/axton-task2-locks").unwrap();
    let d = tempfile::tempdir().unwrap();
    for wal in [false, true] {
        let p = d.path().join(if wal { "wal.db" } else { "rollback.db" });
        let db = rusqlite::Connection::open(&p).unwrap();
        if wal {
            db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;")
                .unwrap();
        }
        db.execute_batch(
            "CREATE TABLE axton_client(id TEXT); INSERT INTO axton_client VALUES('legacy');",
        )
        .unwrap();
        let files = [
            p.clone(),
            std::path::PathBuf::from(format!("{}-wal", p.display())),
            std::path::PathBuf::from(format!("{}-shm", p.display())),
        ];
        let before: Vec<_> = files.iter().map(|p| std::fs::read(p).ok()).collect();
        assert!(SqliteStore::open_exclusive05(&p, "User:u").is_err());
        let after: Vec<_> = files.iter().map(|p| std::fs::read(p).ok()).collect();
        assert_eq!(before, after);
        use sha2::{Digest, Sha256};
        let hashes = |bytes: &[Option<Vec<u8>>]| {
            bytes
                .iter()
                .map(|v| v.as_ref().map(|v| format!("{:x}", Sha256::digest(v))))
                .collect::<Vec<_>>()
        };
        assert_eq!(hashes(&before), hashes(&after));
        println!("unchanged refusal hashes wal={wal}: {:?}", hashes(&after));
    }
}
#[test]
fn nested_rollback_reuses_only_uncommitted_ids_and_nonempty_lists_restore() {
    let d = tempfile::tempdir().unwrap();
    let mut c = Client::open05(
        SqliteStore::open(d.path().join("db")).unwrap(),
        schema(),
        "User:u",
    )
    .unwrap();
    let call=c.transaction(|tx|{let rolled:axton_client::Result<()>=tx.savepoint(|tx|{tx.submit_mutation05("Write",1,json!({"entries":[{"id":"bad","text":"discarded","note":null}]}),vec![])?;Err(axton_client::invalid("rollback"))});assert!(rolled.is_err());tx.submit_mutation05("Write",1,json!({"entries":[{"id":"e1","text":"one","note":null},{"id":"e2","text":"two","note":null}],"maybe":null}),vec![])}).unwrap();
    assert_eq!(call.ordinal, 1);
    let b = c.freeze_batch05().unwrap().unwrap();
    assert_eq!(b.mutations[0].operations.len(), 3);
    assert_eq!(
        v05::reconstruct_input(&b.mutations[0].operations).unwrap(),
        json!({"entries":[{"id":"e1","text":"one","note":null},{"id":"e2","text":"two","note":null}],"maybe":null})
    );
}
#[test]
fn compatible_descriptor_reopen_preserves_assigned_context_and_input() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut c = Client::open05(SqliteStore::open(&p).unwrap(), schema(), "User:u").unwrap();
    c.transaction(|tx| tx.submit_mutation05("Write", 1, json!({"entries":[]}), vec![]))
        .unwrap();
    let b = c.freeze_batch05().unwrap().unwrap();
    drop(c);
    let mut newer = schema();
    newer.models[0].bootstrap = !newer.models[0].bootstrap;
    let mut c = Client::open05(SqliteStore::open(&p).unwrap(), newer, "User:u").unwrap();
    assert_eq!(
        c.request_context05().unwrap().materialization,
        b.context.materialization
    );
    assert!(c.pending_schema05().unwrap().is_some());
    assert_eq!(c.freeze_batch05().unwrap().unwrap(), b);
}
#[test]
fn value_arguments_have_one_entry_and_do_not_create_model_optimism() {
    let d = tempfile::tempdir().unwrap();
    let mut s = serde_json::to_value(schema()).unwrap();
    s["actions"] = json!([{"name":"Arg","version":1,"inputs":[{"kind":"value","name":"label","type":{"kind":"scalar","name":"string"},"nullable":true},{"kind":"value","name":"tags","type":{"kind":"scalar","name":"string"},"nullable":false,"list":true}],"outputs":[]}]);
    let s = Schema::from_value(s).unwrap();
    let mut c =
        Client::open05(SqliteStore::open(d.path().join("db")).unwrap(), s, "User:u").unwrap();
    c.transaction(|tx| {
        tx.submit_mutation05("Arg", 1, json!({"label":null,"tags":["a","b"]}), vec![])
    })
    .unwrap();
    let b = c.freeze_batch05().unwrap().unwrap();
    assert_eq!(b.mutations[0].operations.len(), 2);
    assert!(
        b.mutations[0]
            .operations
            .iter()
            .all(|o| o.operation == v05::Operation::Argument)
    );
    assert_eq!(
        v05::reconstruct_input(&b.mutations[0].operations).unwrap(),
        json!({"label":null,"tags":["a","b"]})
    );
}
#[test]
fn dependent_waits_for_settlement_but_next_independent_batch_can_proceed() {
    let d = tempfile::tempdir().unwrap();
    let mut c = Client::open05(
        SqliteStore::open(d.path().join("db")).unwrap(),
        schema(),
        "User:u",
    )
    .unwrap();
    let first = c
        .transaction(|tx| {
            tx.submit_mutation05(
                "Write",
                1,
                json!({"entries":[{"id":"e","text":"one","note":null}]}),
                vec![],
            )
        })
        .unwrap();
    let b = c.freeze_batch05().unwrap().unwrap();
    let dependent = c
        .transaction(|tx| {
            tx.submit_mutation05(
                "Write",
                1,
                json!({"entries":[],"maybe":{"id":"e","text":"two"}}),
                vec![],
            )
        })
        .unwrap();
    let independent = c
        .transaction(|tx| tx.submit_mutation05("Write", 1, json!({"entries":[]}), vec![]))
        .unwrap();
    let a = v05::BatchAcknowledgement {
        context: b.context.clone(),
        batch_id: b.batch_id,
        digest: b.digest.clone(),
        results: vec![v05::MutationResult {
            mutation_id: first.ordinal,
            outcome: v05::MutationOutcome::Accepted {
                sync_cursor: 9,
                result: Value::Null,
                targets: vec![v05::SettlementTarget::Stream {
                    key: v05::RecordKey {
                        model: "Entry".into(),
                        identity: json!({"id":"e"}),
                    },
                    cursor: 9,
                    fallback: v05::ReadRecord {
                        key: v05::RecordKey {
                            model: "Entry".into(),
                            identity: json!({"id":"e"}),
                        },
                        cursor: (),
                        state: json!({"text":"one","note":null}),
                    },
                }],
            },
        }],
    };
    c.acknowledge_batch05(&a).unwrap();
    let b2 = c.freeze_batch05().unwrap().unwrap();
    assert_eq!(b2.batch_id, 2);
    assert_eq!(
        b2.mutations.iter().map(|m| m.id).collect::<Vec<_>>(),
        vec![independent.ordinal]
    );
    assert!(!b2.mutations.iter().any(|m| m.id == dependent.ordinal));
}
#[test]
fn descriptor_snapshot_reorder_retains_original_hash_in_identical_read_context() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut s = schema();
    let mut v = serde_json::to_value(&s).unwrap();
    v["actions"][0]["input"] = json!({"models":v["models"].clone(),"enums":[]});
    s = Schema::from_value(v).unwrap();
    let mut c = Client::open05(SqliteStore::open(&p).unwrap(), s.clone(), "User:u").unwrap();
    c.transaction(|tx| tx.submit_mutation05("Write", 1, json!({"entries":[]}), vec![]))
        .unwrap();
    let b = c.freeze_batch05().unwrap().unwrap();
    drop(c);
    s.actions[0].input.as_mut().unwrap().models[0]
        .fields
        .reverse();
    let mut c = Client::open05(SqliteStore::open(&p).unwrap(), s, "User:u").unwrap();
    assert_eq!(
        c.request_context05().unwrap().materialization,
        b.context.materialization
    );
    assert_eq!(c.freeze_batch05().unwrap().unwrap(), b);
}
#[test]
fn discard_refuses_assigned_owner_and_dismiss_removes_only_completed_refusal() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut c = Client::open05(SqliteStore::open(&p).unwrap(), schema(), "User:u").unwrap();
    let call = c
        .transaction(|tx| tx.submit_mutation05("Write", 1, json!({"entries":[]}), vec![]))
        .unwrap();
    let report = c
        .transaction(|tx| tx.discard_mutation05(call.ordinal))
        .unwrap();
    assert_eq!(report.completions.len(), 1);
    assert!(c.call_completion05(&call.call_id).unwrap().is_some());
    c.transaction(|tx| tx.dismiss_rejection05(call.ordinal))
        .unwrap();
    assert!(c.call_completion05(&call.call_id).unwrap().is_none());
    let assigned = c
        .transaction(|tx| tx.submit_mutation05("Write", 1, json!({"entries":[]}), vec![]))
        .unwrap();
    let b = c.freeze_batch05().unwrap().unwrap();
    assert!(
        c.transaction(|tx| tx.discard_mutation05(assigned.ordinal))
            .is_err()
    );
    assert!(
        c.transaction(|tx| tx.dismiss_rejection05(assigned.ordinal))
            .is_err()
    );
    assert_eq!(c.freeze_batch05().unwrap().unwrap(), b);
}
#[test]
fn snapshot_refusal_cost_and_stream_admission_are_observed() {
    SqliteStore::set_application_data_directory("/private/tmp/axton-task2-locks").unwrap();
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("large.db");
    let db = rusqlite::Connection::open(&p).unwrap();
    db.execute_batch("CREATE TABLE axton_client(payload BLOB); INSERT INTO axton_client VALUES(zeroblob(16777216));").unwrap();
    drop(db);
    let bytes = std::fs::metadata(&p).unwrap().len();
    let start = std::time::Instant::now();
    let e = SqliteStore::open_exclusive05(&p, "User:u").err().unwrap();
    assert!(e.to_string().contains("unsupported Store format"));
    println!(
        "locked snapshot refusal: {bytes} bytes, {} microseconds",
        start.elapsed().as_micros()
    );
    let p = d.path().join("current.db");
    let mut c = Client::open05(
        SqliteStore::open_exclusive05(&p, "User:u").unwrap(),
        schema(),
        "User:u",
    )
    .unwrap();
    let id = c.request_context05().unwrap().store_id;
    drop(c);
    let before = std::fs::read(&p).unwrap();
    assert!(
        SqliteStore::open_exclusive05(&p, "User:other")
            .err()
            .unwrap()
            .to_string()
            .contains("Stream mismatch")
    );
    assert_eq!(std::fs::read(&p).unwrap(), before);
    let mut c = Client::open05(
        SqliteStore::open_exclusive05(&p, "User:u").unwrap(),
        schema(),
        "User:u",
    )
    .unwrap();
    assert_eq!(c.request_context05().unwrap().store_id, id);
}
#[test]
fn assigned_payload_and_membership_cannot_be_edited_or_cancelled_in_sql() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut c = Client::open05(SqliteStore::open(&p).unwrap(), schema(), "User:u").unwrap();
    let first = c
        .transaction(|tx| tx.submit_mutation05("Write", 1, json!({"entries":[]}), vec![]))
        .unwrap();
    let b = c.freeze_batch05().unwrap().unwrap();
    let later = c
        .transaction(|tx| tx.submit_mutation05("Write", 1, json!({"entries":[]}), vec![]))
        .unwrap();
    let db = rusqlite::Connection::open(&p).unwrap();
    db.execute_batch("PRAGMA foreign_keys=ON").unwrap();
    assert!(
        db.execute(
            "UPDATE axton_mutation_queue_operation SET value='null' WHERE mutation_id=?",
            [first.ordinal as i64]
        )
        .is_err()
    );
    assert!(
        db.execute(
            "DELETE FROM axton_mutation_queue WHERE id=?",
            [first.ordinal as i64]
        )
        .is_err()
    );
    assert!(
        db.execute(
            "UPDATE axton_mutation_queue SET batch_id=1,batch_materialization=? WHERE id=?",
            rusqlite::params![b.context.materialization, later.ordinal as i64]
        )
        .is_err()
    );
    assert_eq!(c.freeze_batch05().unwrap().unwrap(), b);
}
#[test]
fn same_identity_in_two_slots_keeps_paths_and_binding_failure_is_atomic() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut s = serde_json::to_value(schema()).unwrap();
    s["actions"] = json!([{"name":"Pair","version":1,"inputs":[{"kind":"model","name":"first","model":"Entry","operation":"create","cardinality":"single"},{"kind":"model","name":"second","model":"Entry","operation":"update","cardinality":"optional","bindings":[{"slot":"first","fields":["id"]}]}],"outputs":[]}]);
    let mut c = Client::open05(
        SqliteStore::open(&p).unwrap(),
        Schema::from_value(s).unwrap(),
        "User:u",
    )
    .unwrap();
    assert!(c.transaction(|tx|tx.submit_mutation05("Pair",1,json!({"first":{"id":"e","text":"one","note":null},"second":{"id":"other","text":"two"}}),vec![])).is_err());
    assert_eq!(c.store_status05().unwrap().next_mutation_id, 1);
    c.transaction(|tx| {
        tx.submit_mutation05(
            "Pair",
            1,
            json!({"first":{"id":"e","text":"one","note":null},"second":{"id":"e","text":"two"}}),
            vec![],
        )
    })
    .unwrap();
    let b = c.freeze_batch05().unwrap().unwrap();
    assert_eq!(
        b.mutations[0]
            .operations
            .iter()
            .map(|o| o.input_path.as_str())
            .collect::<Vec<_>>(),
        vec!["first", "second"]
    );
}
#[test]
fn unsafe_sequence_counter_rolls_back_model_write_and_mutation_id() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut c = Client::open05(SqliteStore::open(&p).unwrap(), schema(), "User:u").unwrap();
    let db = rusqlite::Connection::open(&p).unwrap();
    db.execute(
        "UPDATE axton_store SET next_local_sequence=?",
        [axton_client::MAX_SAFE_INTEGER as i64],
    )
    .unwrap();
    assert!(
        c.transaction(|tx| tx.submit_mutation05(
            "Write",
            1,
            json!({"entries":[{"id":"e","text":"one","note":null}]}),
            vec![]
        ))
        .is_err()
    );
    assert_eq!(c.store_status05().unwrap().next_mutation_id, 1);
    assert!(
        c.read(&axton_client::RecordKey {
            model: "Entry".into(),
            identity: json!({"id":"e"})
        })
        .unwrap()
        .is_none()
    );
    assert!(c.freeze_batch05().unwrap().is_none());
}
#[test]
fn changed_materialization_remains_desired_until_owned_transfer_commit() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut c = Client::open05(SqliteStore::open(&p).unwrap(), schema(), "User:u").unwrap();
    let original = c.request_context05().unwrap();
    let initial = c.store_status05().unwrap();
    drop(c);
    let mut next = schema();
    next.models[0].bootstrap = !next.models[0].bootstrap;
    let mut c = Client::open05(SqliteStore::open(&p).unwrap(), next.clone(), "User:u").unwrap();
    assert_eq!(c.request_context05().unwrap(), original);
    let pending = c.pending_schema05().unwrap().unwrap();
    assert_ne!(
        pending.desired_context.materialization,
        original.materialization
    );
    assert_eq!(pending.previous_context, original);
    drop(c);
    let mut c = Client::open05(SqliteStore::open(&p).unwrap(), next, "User:u").unwrap();
    assert_eq!(c.request_context05().unwrap(), original);
    c.transaction(|tx| tx.enable_schema05(&pending.previous_context, &pending.desired_context))
        .unwrap();
    assert_eq!(c.request_context05().unwrap(), pending.desired_context);
    assert!(c.pending_schema05().unwrap().is_none());
    let status = c.store_status05().unwrap();
    assert_eq!(
        (status.start_cursor, status.bootstrap_cursor, status.cursor),
        (
            initial.start_cursor,
            initial.bootstrap_cursor,
            initial.cursor
        )
    );
}
#[test]
fn desired_transfer_admission_and_enable_share_the_sqlite_transaction() {
    use axton_client::{ClientStore, RecordKey, authority::Held, engine::Engine};
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut c = Client::open05(SqliteStore::open(&p).unwrap(), schema(), "User:u").unwrap();
    let old = c.request_context05().unwrap();
    let change = v05::AuthorityChange::Record {
        key: v05::RecordKey {
            model: "Entry".into(),
            identity: json!({"id":"e"}),
        },
        cursor: 7,
        state: json!({"text":"base","note":null}),
    };
    c.install_authority05(&old, std::slice::from_ref(&change), None)
        .unwrap();
    drop(c);
    let mut next = schema();
    next.models[0].bootstrap = true;
    let mut c = Client::open05(SqliteStore::open(&p).unwrap(), next.clone(), "User:u").unwrap();
    let pending = c.pending_schema05().unwrap().unwrap();
    assert_eq!(pending.authority_keys, vec![change.key().clone()]);
    assert_eq!(pending.bootstrap_models.get("Entry"), Some(&1));
    drop(c);
    let mut store = SqliteStore::open(&p).unwrap();
    store.begin().unwrap();
    let mut changed = std::collections::BTreeSet::new();
    let mut held = Held::new();
    {
        let mut e = Engine::new(&mut store, &next, &mut changed, false);
        assert!(
            e.stage_authority05(
                &pending.desired_context,
                std::slice::from_ref(&change),
                &mut held
            )
            .is_err()
        );
        let mut foreign = pending.desired_context.clone();
        foreign.materialization = "foreign".into();
        assert!(
            e.stage_materialization05(&foreign, std::slice::from_ref(&change), &mut held)
                .is_err()
        );
        e.stage_materialization05(
            &pending.desired_context,
            std::slice::from_ref(&change),
            &mut held,
        )
        .unwrap();
        assert!(
            e.enable_schema05(&pending.previous_context, &foreign)
                .is_err()
        );
        e.enable_schema05(&pending.previous_context, &pending.desired_context)
            .unwrap();
        e.rebuild_held(&held).unwrap();
    }
    store.rollback().unwrap();
    drop(store);
    let mut c = Client::open05(SqliteStore::open(&p).unwrap(), next.clone(), "User:u").unwrap();
    assert_eq!(c.request_context05().unwrap(), old);
    let key = RecordKey {
        model: "Entry".into(),
        identity: json!({"id":"e"}),
    };
    assert!(
        !c.record_evidence05(&key)
            .unwrap()
            .history
            .contains_key(&pending.desired_context.materialization)
    );
    drop(c);
    let mut store = SqliteStore::open(&p).unwrap();
    store.begin().unwrap();
    {
        let mut e = Engine::new(&mut store, &next, &mut changed, false);
        e.stage_materialization05(&pending.desired_context, &[change], &mut held)
            .unwrap();
        e.enable_schema05(&pending.previous_context, &pending.desired_context)
            .unwrap();
        e.rebuild_held(&held).unwrap();
    }
    store.commit().unwrap();
    drop(store);
    let mut c = Client::open05(SqliteStore::open(&p).unwrap(), next, "User:u").unwrap();
    assert_eq!(c.request_context05().unwrap(), pending.desired_context);
    assert!(c.pending_schema05().unwrap().is_none());
}

#[test]
fn late_cascade_discovery_extends_owned_effects_without_changing_frozen_input() {
    use axton_client::{Operation, OperationKind, RecordKey};
    let mut value = json!({"enums":[],"models":[
        {"name":"Book","identity":["id"],"fields":[{"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"}},{"name":"title","nullable":false,"type":{"kind":"scalar","name":"string"}}]},
        {"name":"Comment","identity":["id"],"fields":[{"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"}},{"name":"bookId","nullable":false,"type":{"kind":"scalar","name":"string"}},{"name":"text","nullable":false,"type":{"kind":"scalar","name":"string"}}],"relations":[{"name":"book","target":"Book","fields":["bookId"],"targetFields":["id"],"onDelete":"delete"}]}
    ]});
    value["actions"] = json!([{"name":"DeleteBook","version":1,"inputs":[{"kind":"model","name":"book","model":"Book","operation":"delete","cardinality":"single"}],"outputs":[]}]);
    let s = Schema::from_value(value).unwrap();
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut c = Client::open05(SqliteStore::open(&p).unwrap(), s.clone(), "User:u").unwrap();
    c.transaction(|tx| {
        tx.direct(Operation {
            model: "Book".into(),
            op: OperationKind::Create,
            identity: json!({"id":"b"}),
            values: Some(json!({"title":"book"})),
        })
    })
    .unwrap();
    let call = c
        .transaction(|tx| tx.submit_mutation05("DeleteBook", 1, json!({"book":{"id":"b"}}), vec![]))
        .unwrap();
    let batch = c.freeze_batch05().unwrap().unwrap();
    c.install_cache05(
        &[v05::ReadRecord {
            key: v05::RecordKey {
                model: "Comment".into(),
                identity: json!({"id":"c"}),
            },
            cursor: (),
            state: json!({"bookId":"b","text":"late child"}),
        }],
        true,
    )
    .unwrap();
    let child = RecordKey {
        model: "Comment".into(),
        identity: json!({"id":"c"}),
    };
    assert!(c.read(&child).unwrap().is_none());
    let ops = c.read_sql("SELECT input_path,kind,model FROM axton_mutation_queue_operation WHERE mutation_id=? ORDER BY step",&[json!(call.ordinal)]).unwrap();
    assert_eq!(ops.len(), 2);
    assert_eq!(ops[1]["input_path"], Value::Null);
    assert_eq!(ops[1]["kind"], "effect");
    assert_eq!(ops[1]["model"], "Comment");
    assert_eq!(c.freeze_batch05().unwrap().unwrap(), batch);
    drop(c);
    let mut c = Client::open05(SqliteStore::open(&p).unwrap(), s, "User:u").unwrap();
    assert_eq!(
        v05::encode(&c.freeze_batch05().unwrap().unwrap()).unwrap(),
        v05::encode(&batch).unwrap()
    );
}
