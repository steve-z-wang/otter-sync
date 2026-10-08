mod common05;
use axton_client::{Client, Operation, OperationKind, RecordKey, v05};
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};
fn key(id: &str) -> RecordKey {
    RecordKey {
        model: "Entry".into(),
        identity: json!({"id":id}),
    }
}
fn edit(id: &str, text: &str) -> Operation {
    Operation {
        model: "Entry".into(),
        op: OperationKind::Update,
        identity: json!({"id":id}),
        values: Some(json!({"text":text})),
    }
}
#[test]
fn scalar_calls_share_commit_but_own_companions_and_independent_outcomes() {
    for first_accepted in [false, true] {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("db");
        let mut s = common05::schema();
        s.actions=serde_json::from_value(json!([{"name":"Ping","version":1,"inputs":[{"kind":"value","name":"label","type":{"kind":"scalar","name":"string"},"nullable":false}],"outputs":[]}])).unwrap();
        let mut c = Client::open05(SqliteStore::open(&path).unwrap(), s.clone(), "User:u").unwrap();
        c.initialize_stream05(0).unwrap();
        c.transaction(|tx| {
            for id in ["a", "b", "independent"] {
                tx.direct(common05::create(
                    "Entry",
                    id,
                    json!({"text":"base","note":null}),
                ))?;
            }
            Ok(())
        })
        .unwrap();
        let (a, b) = c
            .transaction(|tx| {
                let a = tx.submit_mutation05(
                    "Ping",
                    1,
                    json!({"label":"a"}),
                    vec![edit("a", "companion-a")],
                )?;
                let b = tx.submit_mutation05(
                    "Ping",
                    1,
                    json!({"label":"b"}),
                    vec![edit("b", "companion-b")],
                )?;
                tx.direct(edit("independent", "kept"))?;
                Ok((a, b))
            })
            .unwrap();
        let batch = c.freeze_batch05().unwrap().unwrap();
        assert_eq!(batch.mutations.len(), 2);
        assert!(
            batch
                .mutations
                .iter()
                .all(|m| m.operations.iter().all(|op| op.model.is_none()))
        );
        let outcome = |accepted| {
            if accepted {
                v05::MutationOutcome::Accepted {
                    sync_cursor: 0,
                    result: Value::Null,
                    targets: vec![],
                }
            } else {
                v05::MutationOutcome::Rejected {
                    code: "ping.denied".into(),
                    message: None,
                }
            }
        };
        c.acknowledge_batch05(&v05::BatchAcknowledgement {
            context: batch.context.clone(),
            batch_id: batch.batch_id,
            digest: batch.digest.clone(),
            results: vec![
                v05::MutationResult {
                    mutation_id: a.ordinal,
                    outcome: outcome(first_accepted),
                },
                v05::MutationResult {
                    mutation_id: b.ordinal,
                    outcome: outcome(!first_accepted),
                },
            ],
        })
        .unwrap();
        c.settle_ready05().unwrap();
        drop(c);
        let mut c = Client::open05(SqliteStore::open(&path).unwrap(), s, "User:u").unwrap();
        assert_eq!(
            c.read(&key("a")).unwrap().unwrap()["text"],
            if first_accepted {
                "companion-a"
            } else {
                "base"
            }
        );
        assert_eq!(
            c.read(&key("b")).unwrap().unwrap()["text"],
            if first_accepted {
                "base"
            } else {
                "companion-b"
            }
        );
        assert_eq!(
            c.read(&key("independent")).unwrap().unwrap()["text"],
            "kept"
        );
        assert!(c.call_completion05(&a.call_id).unwrap().is_some());
        assert!(c.call_completion05(&b.call_id).unwrap().is_some());
        assert_eq!(c.pending_count().unwrap(), 0);
        let accepted_id = if first_accepted { "a" } else { "b" };
        let context = c.request_context05().unwrap();
        c.install_authority05(
            &context,
            &[v05::AuthorityChange::Record {
                key: v05::RecordKey {
                    model: "Entry".into(),
                    identity: json!({"id":accepted_id}),
                },
                cursor: 1,
                state: json!({"text":"later owner","note":null}),
            }],
            None,
        )
        .unwrap();
        assert_eq!(
            c.read(&key(accepted_id)).unwrap().unwrap()["text"],
            "later owner"
        );
    }
}

fn remove(id: &str) -> Operation {
    Operation {
        model: "Entry".into(),
        op: OperationKind::Delete,
        identity: json!({"id":id}),
        values: None,
    }
}
fn text_of(c: &mut Client<SqliteStore>, id: &str) -> Option<String> {
    c.read(&key(id))
        .unwrap()
        .map(|r| r["text"].as_str().unwrap().to_owned())
}
fn start(comp: &str) -> (tempfile::TempDir, Client<SqliteStore>) {
    let d = tempfile::tempdir().unwrap();
    let mut c = common05::open(&d.path().join("db"));
    c.initialize_stream05(0).unwrap();
    c.transaction(|tx| {
        tx.direct(common05::create(
            "Entry",
            "e",
            json!({"text":"A","note":null}),
        ))?;
        tx.direct(common05::create(
            "Entry",
            "comp",
            json!({"text":comp,"note":null}),
        ))
    })
    .unwrap();
    (d, c)
}
fn direct(c: &mut Client<SqliteStore>, op: Operation) {
    c.transaction(|tx| tx.direct(op)).unwrap();
}
fn call(c: &mut Client<SqliteStore>, companions: Vec<Operation>) {
    c.transaction(|tx| {
        tx.submit_mutation05(
            "Edit",
            1,
            json!({"entry":{"id":"e","text":"B"}}),
            companions,
        )
    })
    .unwrap();
}
fn finish(c: &mut Client<SqliteStore>, accepted: bool) {
    let b = c.freeze_batch05().unwrap().unwrap();
    let results = b
        .mutations
        .iter()
        .map(|m| v05::MutationResult {
            mutation_id: m.id,
            outcome: if accepted {
                v05::MutationOutcome::Accepted {
                    sync_cursor: 0,
                    result: Value::Null,
                    targets: vec![v05::SettlementTarget::Private {
                        record: v05::ReadRecord {
                            key: v05::RecordKey {
                                model: "Entry".into(),
                                identity: json!({"id":"e"}),
                            },
                            cursor: (),
                            state: json!({"text":"SERVER","note":null}),
                        },
                    }],
                }
            } else {
                v05::MutationOutcome::Rejected {
                    code: "edit.denied".into(),
                    message: None,
                }
            },
        })
        .collect();
    c.acknowledge_batch05(&v05::BatchAcknowledgement {
        context: b.context,
        batch_id: b.batch_id,
        digest: b.digest,
        results,
    })
    .unwrap();
    c.settle_ready05().unwrap();
    assert_eq!(c.pending_count().unwrap(), 0);
    assert_eq!(c.before_image_count().unwrap(), 0);
}
#[test]
fn without_later_edits_a_companion_is_kept_on_acceptance_and_undone_on_rejection() {
    for accepted in [false, true] {
        let (_dir, mut c) = start("old");
        call(&mut c, vec![remove("comp")]);
        assert_eq!(text_of(&mut c, "comp"), None);
        finish(&mut c, accepted);
        let expected = match accepted {
            true => None,
            false => Some("old".to_owned()),
        };
        assert_eq!(text_of(&mut c, "comp"), expected, "accepted={accepted}");

        let (_dir, mut c) = start("old");
        call(&mut c, vec![edit("comp", "1")]);
        finish(&mut c, accepted);
        let expected = match accepted {
            true => "1",
            false => "old",
        };
        assert_eq!(
            text_of(&mut c, "comp").as_deref(),
            Some(expected),
            "accepted={accepted}"
        );
    }
}

/// Delete as a companion, then recreate the same identity directly: the new
/// content is neither deleted by acceptance nor replaced by the old content
/// on rejection.
#[test]
fn a_direct_recreate_after_a_companion_delete_keeps_the_new_content() {
    for accepted in [false, true] {
        let (_dir, mut c) = start("old");
        call(&mut c, vec![remove("comp")]);
        direct(
            &mut c,
            common05::create("Entry", "comp", json!({"text":"new","note":null})),
        );
        assert_eq!(text_of(&mut c, "comp").as_deref(), Some("new"));
        finish(&mut c, accepted);
        assert_eq!(
            text_of(&mut c, "comp").as_deref(),
            Some("new"),
            "accepted={accepted}"
        );
    }
}

/// A later independent update of the same field wins over an earlier
/// companion whichever way the companion's call settles.
#[test]
fn a_later_direct_update_outlives_an_earlier_companion_update() {
    for accepted in [false, true] {
        let (_dir, mut c) = start("0");
        call(&mut c, vec![edit("comp", "1")]);
        direct(&mut c, edit("comp", "2"));
        assert_eq!(text_of(&mut c, "comp").as_deref(), Some("2"));
        finish(&mut c, accepted);
        assert_eq!(
            text_of(&mut c, "comp").as_deref(),
            Some("2"),
            "accepted={accepted}"
        );
    }
}

/// A later independent delete is not undone by rejecting an earlier companion,
/// nor replaced by accepting it.
#[test]
fn a_later_direct_delete_outlives_an_earlier_companion_update() {
    for accepted in [false, true] {
        let (_dir, mut c) = start("0");
        call(&mut c, vec![edit("comp", "1")]);
        direct(&mut c, remove("comp"));
        finish(&mut c, accepted);
        assert_eq!(text_of(&mut c, "comp"), None, "accepted={accepted}");
    }
}

#[test]
fn a_direct_edit_follows_the_fate_of_a_pending_companion_create() {
    for accepted in [false, true] {
        let (_d, mut c) = start("old");
        call(
            &mut c,
            vec![common05::create(
                "Entry",
                "fresh",
                json!({"text":"n","note":null}),
            )],
        );
        direct(&mut c, edit("fresh", "n2"));
        assert_eq!(text_of(&mut c, "fresh").as_deref(), Some("n2"));
        finish(&mut c, accepted);
        assert_eq!(
            text_of(&mut c, "fresh"),
            if accepted { Some("n2".into()) } else { None }
        );
    }
}

#[test]
fn overlapping_companions_keep_local_order_across_outcome_order_and_reopen() {
    // Both outcomes together, later acceptance first, and earlier refusal first.
    for held in [None, Some(0), Some(1)] {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("db");
        let mut s = serde_json::to_value(common05::schema()).unwrap();
        s["prerequisites"] = json!([{"name":"Ready","fields":[{"name":"key","type":"String"}]}]);
        s["requirements"] =
            json!([{"model":"Entry","field":"note","name":"Ready","arguments":{"key":"self"}}]);
        s["actions"].as_array_mut().unwrap().push(json!({"name":"HeldEdit","version":1,"inputs":[{"kind":"model","name":"entry","model":"Entry","operation":"update","cardinality":"single","fields":["text","note"]}],"outputs":[]}));
        let schema = axton_client::Schema::from_value(s).unwrap();
        let mut c =
            Client::open05(SqliteStore::open(&path).unwrap(), schema.clone(), "User:u").unwrap();
        c.initialize_stream05(0).unwrap();
        c.transaction(|tx| {
            tx.direct(common05::create(
                "Entry",
                "e",
                json!({"text":"base","note":null}),
            ))?;
            tx.direct(common05::create(
                "Entry",
                "comp",
                json!({"text":"0","note":null}),
            ))
        })
        .unwrap();
        for i in 0..2 {
            let (name, input) = if held == Some(i) {
                (
                    "HeldEdit",
                    json!({"entry":{"id":"e","text":"wire","note":"hold"}}),
                )
            } else {
                ("Edit", json!({"entry":{"id":"e","text":"wire"}}))
            };
            c.transaction(|tx| {
                tx.submit_mutation05(
                    name,
                    1,
                    input,
                    vec![edit("comp", if i == 0 { "1" } else { "2" })],
                )
            })
            .unwrap();
        }
        let settle = |c: &mut Client<SqliteStore>| {
            let b = c.freeze_batch05().unwrap().unwrap();
            let results = b
                .mutations
                .iter()
                .map(|m| v05::MutationResult {
                    mutation_id: m.id,
                    outcome: if m.id == 1 {
                        v05::MutationOutcome::Rejected {
                            code: "edit.denied".into(),
                            message: None,
                        }
                    } else {
                        v05::MutationOutcome::Accepted {
                            sync_cursor: 0,
                            result: Value::Null,
                            targets: vec![v05::SettlementTarget::Private {
                                record: v05::ReadRecord {
                                    key: v05::RecordKey {
                                        model: "Entry".into(),
                                        identity: json!({"id":"e"}),
                                    },
                                    cursor: (),
                                    state: json!({"text":"server","note":null}),
                                },
                            }],
                        }
                    },
                })
                .collect();
            c.acknowledge_batch05(&v05::BatchAcknowledgement {
                context: b.context,
                batch_id: b.batch_id,
                digest: b.digest,
                results,
            })
            .unwrap();
            c.settle_ready05().unwrap();
        };
        settle(&mut c);
        assert_eq!(text_of(&mut c, "comp").as_deref(), Some("2"));
        drop(c);
        let mut c = Client::open05(SqliteStore::open(&path).unwrap(), schema, "User:u").unwrap();
        assert_eq!(text_of(&mut c, "comp").as_deref(), Some("2"));
        if held.is_some() {
            assert_eq!(c.pending_count().unwrap(), 1);
            c.set_readiness(
                &json!({"arguments":{"key":"hold"},"name":"Ready"}).to_string(),
                axton_client::Readiness::Ready,
            )
            .unwrap();
            settle(&mut c);
        }
        assert_eq!(text_of(&mut c, "comp").as_deref(), Some("2"));
        assert_eq!(c.pending_count().unwrap(), 0);
        assert_eq!(c.before_image_count().unwrap(), 0);
    }
}
