pub mod common05;
use axton_client::{Client, Operation, OperationKind, Schema, v05};
use axton_sqlite::SqliteStore;
use common05::{key, open};
use serde_json::{Value, json};
fn state(text: Option<&str>) -> Value {
    text.map(|t| json!({"text":t,"note":null}))
        .unwrap_or(Value::Null)
}
fn op(kind: OperationKind, text: Option<&str>) -> Operation {
    Operation {
        model: "Entry".into(),
        identity: json!({"id":"e"}),
        op: kind,
        values: text.map(|t| {
            if kind == OperationKind::Create {
                state(Some(t))
            } else {
                json!({"text":t})
            }
        }),
    }
}
fn stream(c: &mut Client<SqliteStore>, cursor: u64, text: Option<&str>) {
    let context = c.request_context05().unwrap();
    c.install_authority05(
        &context,
        &[v05::AuthorityChange::Record {
            key: v05::RecordKey {
                model: "Entry".into(),
                identity: json!({"id":"e"}),
            },
            cursor,
            state: state(text),
        }],
        None,
    )
    .unwrap();
}
fn cache(c: &mut Client<SqliteStore>, text: Option<&str>, store: bool) {
    c.install_cache05(
        &[v05::ReadRecord {
            key: v05::RecordKey {
                model: "Entry".into(),
                identity: json!({"id":"e"}),
            },
            cursor: (),
            state: state(text),
        }],
        store,
    )
    .unwrap();
}
#[test]
fn direct_content_has_null_cursor_but_stream_history_survives_and_tombstone_guards_reads() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut c = open(&p);
    let context = c.request_context05().unwrap().clone();
    stream(&mut c, 57, Some("A"));
    c.transaction(|tx| tx.direct(op(OperationKind::Update, Some("B"))))
        .unwrap();
    let e = c.record_evidence05(&key()).unwrap();
    assert!(e.current.is_none());
    assert_eq!(e.history[&context.materialization], 57);
    stream(&mut c, 56, Some("old"));
    stream(&mut c, 57, Some("equal but different"));
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "B");
    stream(&mut c, 58, Some("new"));
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "new");
    c.transaction(|tx| tx.direct(op(OperationKind::Delete, None)))
        .unwrap();
    cache(&mut c, Some("refill"), true);
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "refill");
    stream(&mut c, 59, None);
    cache(&mut c, Some("late"), true);
    assert!(c.read(&key()).unwrap().is_none());
    assert!(
        c.record_evidence05(&key())
            .unwrap()
            .current
            .unwrap()
            .deleted
    );
    drop(c);
    let mut c = open(&p);
    cache(&mut c, Some("after-reopen"), true);
    assert!(c.read(&key()).unwrap().is_none());
    assert_eq!(
        c.record_evidence05(&key()).unwrap().history[&context.materialization],
        59
    );
}
#[test]
fn pending_optimism_does_not_release_base_protection_and_null_reads_never_delete() {
    let d = tempfile::tempdir().unwrap();
    let mut c = open(&d.path().join("db"));
    stream(&mut c, 57, Some("A"));
    c.transaction(|tx| {
        tx.submit_mutation05("Edit", 1, json!({"entry":{"id":"e","text":"P"}}), vec![])
    })
    .unwrap();
    assert_eq!(
        c.record_evidence05(&key()).unwrap().current.unwrap().cursor,
        57
    );
    cache(&mut c, Some("ordinary"), true);
    cache(&mut c, None, true);
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "P");
    stream(&mut c, 58, Some("new"));
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "P");
    assert_eq!(
        c.record_evidence05(&key()).unwrap().current.unwrap().cursor,
        58
    );
}
#[test]
fn cache_mode_false_changes_no_rows_or_evidence_and_true_is_best_effort_without_g() {
    let d = tempfile::tempdir().unwrap();
    let mut c = open(&d.path().join("db"));
    cache(&mut c, Some("snapshot"), false);
    assert!(c.read(&key()).unwrap().is_none());
    assert_eq!(
        c.record_evidence05(&key()).unwrap(),
        axton_core::authority::RecordEvidence::default()
    );
    cache(&mut c, Some("newer request finished first"), true);
    cache(&mut c, Some("older request finished last"), true);
    assert_eq!(
        c.read(&key()).unwrap().unwrap()["text"],
        "older request finished last"
    );
    assert_eq!(
        c.record_evidence05(&key()).unwrap(),
        axton_core::authority::RecordEvidence::default()
    );
    cache(&mut c, None, true);
    assert!(c.read(&key()).unwrap().is_some());
}

#[test]
fn rematerialization_keeps_direct_patches_and_pending_without_overwriting_new_fields() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut c = open(&p);
    stream(&mut c, 57, Some("A"));
    c.transaction(|tx| tx.direct(op(OperationKind::Update, Some("D"))))
        .unwrap();
    c.transaction(|tx| {
        tx.submit_mutation05("Edit", 1, json!({"entry":{"id":"e","text":"P"}}), vec![])
    })
    .unwrap();
    let old = c.request_context05().unwrap();
    drop(c);
    let mut changed = serde_json::to_value(common05::schema()).unwrap();
    changed["models"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"label","nullable":true,"type":{"kind":"scalar","name":"string"}}));
    let mut c = Client::open05(
        SqliteStore::open(&p).unwrap(),
        Schema::from_value(changed).unwrap(),
        "User:u",
    )
    .unwrap();
    let pending = c.pending_schema05().unwrap().unwrap();
    c.transaction(|tx| tx.enable_schema05(&pending.previous_context, &pending.desired_context))
        .unwrap();
    let current = c.request_context05().unwrap();
    assert_ne!(old.materialization, current.materialization);
    c.install_authority05(
        &current,
        &[v05::AuthorityChange::Record {
            key: v05::RecordKey {
                model: "Entry".into(),
                identity: key().identity,
            },
            cursor: 57,
            state: json!({"text":"A","note":null,"label":"fresh"}),
        }],
        None,
    )
    .unwrap();
    assert_eq!(
        c.read(&key()).unwrap().unwrap(),
        json!({"id":"e","text":"P","note":null,"label":"fresh"})
    );
    let evidence = c.record_evidence05(&key()).unwrap();
    assert!(evidence.current.is_none());
    assert_eq!(evidence.history[&old.materialization], 57);
    assert_eq!(evidence.history[&current.materialization], 57);
    // Request-owner fencing is covered by actual read05 runtime tests.
}

#[test]
fn same_position_rematerialized_absence_keeps_later_direct_recreation_and_pending_patch() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut c = open(&p);
    stream(&mut c, 57, None);
    c.transaction(|tx| tx.direct(op(OperationKind::Create, Some("direct"))))
        .unwrap();
    c.transaction(|tx| {
        tx.submit_mutation05(
            "Edit",
            1,
            json!({"entry":{"id":"e","text":"pending"}}),
            vec![],
        )
    })
    .unwrap();
    let old = c.request_context05().unwrap();
    drop(c);
    let mut changed = serde_json::to_value(common05::schema()).unwrap();
    changed["models"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"label","nullable":true,"type":{"kind":"scalar","name":"string"}}));
    let mut c = Client::open05(
        SqliteStore::open(&p).unwrap(),
        Schema::from_value(changed).unwrap(),
        "User:u",
    )
    .unwrap();
    let pending = c.pending_schema05().unwrap().unwrap();
    c.transaction(|tx| tx.enable_schema05(&pending.previous_context, &pending.desired_context))
        .unwrap();
    let active = c.request_context05().unwrap();
    c.install_authority05(
        &active,
        &[v05::AuthorityChange::Record {
            key: v05::RecordKey {
                model: "Entry".into(),
                identity: key().identity,
            },
            cursor: 57,
            state: Value::Null,
        }],
        None,
    )
    .unwrap();
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "pending");
    let evidence = c.record_evidence05(&key()).unwrap();
    assert_eq!(evidence.history[&old.materialization], 57);
    assert_eq!(evidence.history[&active.materialization], 57);
    assert!(evidence.current.unwrap().deleted);
    cache(&mut c, Some("late cache"), true);
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "pending");
    assert_eq!(c.pending_count().unwrap(), 1);
}

#[test]
fn repeated_clean_direct_updates_retain_one_patch_and_authority_clears_it() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("db");
    let mut c = open(&path);
    stream(&mut c, 1, Some("replica"));
    for n in 0..20 {
        c.transaction(|tx| tx.direct(op(OperationKind::Update, Some(&format!("local {n}")))))
            .unwrap();
    }
    let rows = c
        .read_sql("SELECT operations FROM axton_local_replica_layer", &[])
        .unwrap();
    assert_eq!(rows.len(), 1);
    let operations: Value = serde_json::from_str(rows[0]["operations"].as_str().unwrap()).unwrap();
    assert_eq!(operations.as_array().unwrap().len(), 1);
    assert_eq!(operations[0]["values"]["text"], "local 19");
    assert_eq!(c.pending_count().unwrap(), 0);
    drop(c);
    let mut c = open(&path);
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "local 19");
    stream(&mut c, 2, Some("server"));
    assert!(
        c.read_sql("SELECT * FROM axton_local_replica_layer", &[])
            .unwrap()
            .is_empty()
    );
    assert!(
        c.read_sql("SELECT * FROM axton_local_write", &[])
            .unwrap()
            .is_empty()
    );
    assert_eq!(c.pending_count().unwrap(), 0);
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "server");
    drop(c);
    assert_eq!(open(&path).read(&key()).unwrap().unwrap()["text"], "server");
}

#[test]
fn direct_writes_survive_refusal_but_newer_authority_retires_them_before_or_after_refusal() {
    for order in ["before", "after", "after-companion"] {
        for authority_before_refusal in [false, true] {
            let d = tempfile::tempdir().unwrap();
            let path = d.path().join("db");
            let mut c = open(&path);
            c.initialize_stream05(0).unwrap();
            stream(&mut c, 1, Some("A"));
            if order == "before" {
                c.transaction(|tx| tx.direct(op(OperationKind::Update, Some("direct"))))
                    .unwrap();
            }
            c.transaction(|tx| {
                tx.submit_mutation05(
                    "Edit",
                    1,
                    json!({"entry":{"id":"e","text":"optimistic"}}),
                    if order == "after-companion" {
                        vec![op(OperationKind::Update, Some("companion"))]
                    } else {
                        vec![]
                    },
                )
            })
            .unwrap();
            if order != "before" {
                c.transaction(|tx| tx.direct(op(OperationKind::Update, Some("direct"))))
                    .unwrap();
            }
            assert_eq!(c.pending_count().unwrap(), 1, "direct is never queued");
            if authority_before_refusal {
                stream(&mut c, 2, Some("new authority"));
            }
            let batch = c.freeze_batch05().unwrap().unwrap();
            c.acknowledge_batch05(&v05::BatchAcknowledgement {
                context: batch.context,
                batch_id: batch.batch_id,
                digest: batch.digest,
                results: vec![v05::MutationResult {
                    mutation_id: batch.mutations[0].id,
                    outcome: v05::MutationOutcome::Rejected {
                        code: "edit.denied".into(),
                        message: None,
                    },
                }],
            })
            .unwrap();
            assert_eq!(c.pending_count().unwrap(), 0);
            assert_eq!(
                c.read(&key()).unwrap().unwrap()["text"],
                if authority_before_refusal {
                    "new authority"
                } else {
                    "direct"
                },
                "{order}/{authority_before_refusal}"
            );
            drop(c);
            let mut c = open(&path);
            assert_eq!(
                c.read(&key()).unwrap().unwrap()["text"],
                if authority_before_refusal {
                    "new authority"
                } else {
                    "direct"
                }
            );
            stream(&mut c, 3, Some("later authority"));
            assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "later authority");
            assert!(
                c.read_sql("SELECT * FROM axton_local_replica_layer", &[])
                    .unwrap()
                    .is_empty()
            );
            assert!(
                c.read_sql("SELECT * FROM axton_local_write", &[])
                    .unwrap()
                    .is_empty()
            );
        }
    }
}
