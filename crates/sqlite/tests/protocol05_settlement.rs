use axton_client::{Client, Operation, OperationKind, RecordKey, Schema, v05};
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};
fn schema() -> Schema {
    let mut s: Value =
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap();
    s["actions"] = json!([{"name":"Write","version":1,"inputs":[{"kind":"model","name":"entry","model":"Entry","operation":"create","cardinality":"single"}],"outputs":[]}]);
    Schema::from_value(s).unwrap()
}
fn open(p: &std::path::Path) -> Client<SqliteStore> {
    Client::open05(SqliteStore::open(p).unwrap(), schema(), "User:u").unwrap()
}
fn key() -> RecordKey {
    RecordKey {
        model: "Entry".into(),
        identity: json!({"id":"e"}),
    }
}
fn direct(text: &str) -> Operation {
    Operation {
        model: "Entry".into(),
        identity: json!({"id":"e"}),
        op: OperationKind::Update,
        values: Some(json!({"text":text})),
    }
}
fn ack(b: &v05::MutationRequest, outcome: v05::MutationOutcome) -> v05::BatchAcknowledgement {
    v05::BatchAcknowledgement {
        context: b.context.clone(),
        batch_id: b.batch_id,
        digest: b.digest.clone(),
        results: vec![v05::MutationResult {
            mutation_id: b.mutations[0].id,
            outcome,
        }],
    }
}
#[test]
fn rejected_owner_preserves_later_direct_and_completion_after_reopen() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut c = open(&p);
    let call = c
        .transaction(|tx| {
            tx.submit_mutation05(
                "Write",
                1,
                json!({"entry":{"id":"e","text":"A","note":null}}),
                vec![],
            )
        })
        .unwrap();
    let b = c.freeze_batch05().unwrap().unwrap();
    c.transaction(|tx| tx.direct(direct("B"))).unwrap();
    let a = ack(
        &b,
        v05::MutationOutcome::Rejected {
            code: "write.denied".into(),
            message: None,
        },
    );
    c.acknowledge_batch05(&a).unwrap();
    assert_eq!(c.read(&key()).unwrap(), None);
    drop(c);
    let mut c = open(&p);
    assert!(c.call_completion05(&call.call_id).unwrap().is_some());
    assert!(c.freeze_batch05().unwrap().is_none());
    c.acknowledge_batch05(&a).unwrap();
}
#[test]
fn accepted_private_result_waits_for_coverage_then_settles_later_direct() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut c = open(&p);
    let call = c
        .transaction(|tx| {
            tx.submit_mutation05(
                "Write",
                1,
                json!({"entry":{"id":"e","text":"A","note":null}}),
                vec![],
            )
        })
        .unwrap();
    let b = c.freeze_batch05().unwrap().unwrap();
    c.transaction(|tx| tx.direct(direct("B"))).unwrap();
    let a = ack(
        &b,
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
                    state: json!({"text":"accepted","note":null}),
                },
            }],
        },
    );
    c.acknowledge_batch05(&a).unwrap();
    assert!(c.call_completion05(&call.call_id).unwrap().is_none());
    assert!(c.settle_ready05().unwrap().completions.is_empty());
}
fn stream_key(id: &str) -> v05::RecordKey {
    v05::RecordKey {
        model: "Entry".into(),
        identity: json!({"id":id}),
    }
}
fn state(text: &str) -> Value {
    json!({"text":text,"note":null})
}
fn install(c: &mut Client<SqliteStore>, id: &str, cursor: u64, text: Option<&str>) {
    let context = c.request_context05().unwrap();
    c.install_authority05(
        &context,
        &[v05::AuthorityChange::Record {
            key: stream_key(id),
            cursor,
            state: text.map(state).unwrap_or(Value::Null),
        }],
        None,
    )
    .unwrap();
}
#[test]
fn direct_delete_clears_current_tombstone_but_retains_stream_history() {
    let d = tempfile::tempdir().unwrap();
    let mut c = open(&d.path().join("db"));
    install(&mut c, "e", 7, None);
    c.transaction(|tx| {
        tx.direct(Operation {
            model: "Entry".into(),
            identity: json!({"id":"e"}),
            op: OperationKind::Delete,
            values: None,
        })
    })
    .unwrap();
    c.install_cache05(
        &[v05::ReadRecord {
            key: stream_key("e"),
            cursor: (),
            state: state("cache"),
        }],
        true,
    )
    .unwrap();
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "cache");
    install(&mut c, "e", 7, Some("stale"));
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "cache");
}
#[test]
fn newer_stream_supersedes_private_fallback_even_after_direct_releases_protection() {
    let d = tempfile::tempdir().unwrap();
    let mut c = open(&d.path().join("db"));
    let call = c
        .transaction(|tx| {
            tx.submit_mutation05(
                "Write",
                1,
                json!({"entry":{"id":"e","text":"A","note":null}}),
                vec![],
            )
        })
        .unwrap();
    let b = c.freeze_batch05().unwrap().unwrap();
    install(&mut c, "e", 5, Some("server"));
    c.transaction(|tx| tx.direct(direct("B"))).unwrap();
    let a = ack(
        &b,
        v05::MutationOutcome::Accepted {
            sync_cursor: 5,
            result: Value::Null,
            targets: vec![v05::SettlementTarget::Private {
                record: v05::ReadRecord {
                    key: stream_key("e"),
                    cursor: (),
                    state: state("old-private"),
                },
            }],
        },
    );
    c.acknowledge_batch05(&a).unwrap();
    let context = c.request_context05().unwrap();
    c.install_authority05(&context, &[], Some((0, 5))).unwrap();
    c.settle_ready05().unwrap();
    assert!(c.call_completion05(&call.call_id).unwrap().is_some());
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "B");
}
#[test]
fn rejected_update_preserves_later_direct_b_and_historical_evidence() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut s = schema();
    let mut value = serde_json::to_value(&s).unwrap();
    value["actions"][0]["inputs"][0]["operation"] = json!("update");
    s = Schema::from_value(value).unwrap();
    let mut c = Client::open05(SqliteStore::open(&p).unwrap(), s.clone(), "User:u").unwrap();
    install(&mut c, "e", 3, Some("base"));
    let call = c
        .transaction(|tx| {
            tx.submit_mutation05("Write", 1, json!({"entry":{"id":"e","text":"A"}}), vec![])
        })
        .unwrap();
    let b = c.freeze_batch05().unwrap().unwrap();
    c.transaction(|tx| tx.direct(direct("B"))).unwrap();
    let a = ack(
        &b,
        v05::MutationOutcome::Rejected {
            code: "write.denied".into(),
            message: Some("No".into()),
        },
    );
    c.acknowledge_batch05(&a).unwrap();
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "B");
    assert!(c.record_evidence05(&key()).unwrap().current.is_none());
    drop(c);
    let mut c = Client::open05(SqliteStore::open(&p).unwrap(), s, "User:u").unwrap();
    c.acknowledge_batch05(&a).unwrap();
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "B");
    assert!(c.call_completion05(&call.call_id).unwrap().is_some());
}
#[test]
fn private_acceptance_finalizes_at_original_position_and_retains_result_after_cleanup() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut c = open(&p);
    let call = c
        .transaction(|tx| {
            tx.submit_mutation05(
                "Write",
                1,
                json!({"entry":{"id":"e","text":"A","note":null}}),
                vec![],
            )
        })
        .unwrap();
    let b = c.freeze_batch05().unwrap().unwrap();
    c.transaction(|tx| tx.direct(direct("B"))).unwrap();
    let a = ack(
        &b,
        v05::MutationOutcome::Accepted {
            sync_cursor: 0,
            result: Value::Null,
            targets: vec![v05::SettlementTarget::Private {
                record: v05::ReadRecord {
                    key: stream_key("e"),
                    cursor: (),
                    state: state("accepted"),
                },
            }],
        },
    );
    c.acknowledge_batch05(&a).unwrap();
    let context = c.request_context05().unwrap();
    c.install_authority05(&context, &[], Some((0, 0))).unwrap();
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "B");
    assert!(c.call_completion05(&call.call_id).unwrap().is_some());
    drop(c);
    let mut c = open(&p);
    assert!(c.call_completion05(&call.call_id).unwrap().is_some());
    c.acknowledge_batch05(&a).unwrap();
    let db = rusqlite::Connection::open(&p).unwrap();
    assert_eq!(
        db.query_row(
            "SELECT COUNT(*) FROM axton_mutation_queue_operation",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
}
use axton_client::{ClientStore, Result, SqlRows, invalid};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
struct FaultStore {
    inner: SqliteStore,
    fail_commit: Arc<AtomicBool>,
    crash_ack: bool,
}
impl ClientStore for FaultStore {
    fn begin(&mut self) -> Result<()> {
        self.inner.begin()
    }
    fn commit(&mut self) -> Result<()> {
        if self.fail_commit.swap(false, Ordering::SeqCst) {
            Err(invalid("injected commit failure"))
        } else {
            self.inner.commit()
        }
    }
    fn rollback(&mut self) -> Result<()> {
        self.inner.rollback()
    }
    fn savepoint(&mut self, n: &str) -> Result<()> {
        self.inner.savepoint(n)
    }
    fn release(&mut self, n: &str) -> Result<()> {
        self.inner.release(n)
    }
    fn rollback_to(&mut self, n: &str) -> Result<()> {
        self.inner.rollback_to(n)
    }
    fn execute(&mut self, s: &str, p: &[Value]) -> Result<usize> {
        let r = self.inner.execute(s, p)?;
        if std::env::var("AXTON_TASK2_ASSIGNMENT_CRASH")
            .ok()
            .as_deref()
            == Some("before")
            && s == "UPDATE axton_mutation_queue SET batch_digest=? WHERE batch_id=?"
        {
            std::fs::write(
                std::env::var("AXTON_TASK2_ASSIGNMENT_EVIDENCE").unwrap(),
                p[0].as_str().unwrap(),
            )
            .unwrap();
            std::process::exit(80)
        }
        if self.crash_ack && s == "UPDATE axton_store SET last_acknowledged_batch_id=?" {
            std::process::exit(77)
        }
        Ok(r)
    }
    fn execute_batch(&mut self, s: &str) -> Result<()> {
        self.inner.execute_batch(s)
    }
    fn query(&mut self, s: &str, p: &[Value]) -> Result<SqlRows> {
        self.inner.query(s, p)
    }
    fn query_committed(&mut self, s: &str, p: &[Value]) -> Result<SqlRows> {
        self.inner.query_committed(s, p)
    }
}
#[test]
fn commit_failure_rolls_back_assignment_and_acknowledgement() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let failure = Arc::new(AtomicBool::new(false));
    let store = FaultStore {
        inner: SqliteStore::open(&p).unwrap(),
        fail_commit: failure.clone(),
        crash_ack: false,
    };
    let mut c = Client::open05(store, schema(), "User:u").unwrap();
    let call = c
        .transaction(|tx| {
            tx.submit_mutation05(
                "Write",
                1,
                json!({"entry":{"id":"e","text":"A","note":null}}),
                vec![],
            )
        })
        .unwrap();
    failure.store(true, Ordering::SeqCst);
    assert!(c.freeze_batch05().is_err());
    assert_eq!(c.store_status05().unwrap().last_acknowledged_batch_id, 0);
    let b = c.freeze_batch05().unwrap().unwrap();
    let a = ack(
        &b,
        v05::MutationOutcome::Rejected {
            code: "write.denied".into(),
            message: None,
        },
    );
    failure.store(true, Ordering::SeqCst);
    assert!(c.acknowledge_batch05(&a).is_err());
    assert!(c.call_completion05(&call.call_id).unwrap().is_none());
    assert_eq!(c.freeze_batch05().unwrap().unwrap(), b);
    drop(c);
    let mut c = open(&p);
    assert_eq!(c.freeze_batch05().unwrap().unwrap(), b);
    c.acknowledge_batch05(&a).unwrap();
    assert!(c.call_completion05(&call.call_id).unwrap().is_some());
}
#[test]
fn partial_duplicate_or_foreign_acknowledgement_changes_nothing() {
    let d = tempfile::tempdir().unwrap();
    let mut c = open(&d.path().join("db"));
    c.transaction(|tx| {
        tx.submit_mutation05(
            "Write",
            1,
            json!({"entry":{"id":"e","text":"A","note":null}}),
            vec![],
        )
    })
    .unwrap();
    let b = c.freeze_batch05().unwrap().unwrap();
    let a = ack(
        &b,
        v05::MutationOutcome::Rejected {
            code: "write.denied".into(),
            message: None,
        },
    );
    let mut bad = a.clone();
    bad.results.clear();
    assert!(c.acknowledge_batch05(&bad).is_err());
    bad = a.clone();
    bad.results.push(bad.results[0].clone());
    assert!(c.acknowledge_batch05(&bad).is_err());
    bad = a.clone();
    bad.context.store_id = "foreign".into();
    assert!(c.acknowledge_batch05(&bad).is_err());
    assert_eq!(c.freeze_batch05().unwrap().unwrap(), b);
    assert_eq!(c.store_status05().unwrap().last_acknowledged_batch_id, 0);
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "A");
}
fn update_schema() -> Schema {
    let mut s = serde_json::to_value(schema()).unwrap();
    s["actions"][0]["inputs"][0]["operation"] = json!("update");
    Schema::from_value(s).unwrap()
}
#[test]
#[ignore]
fn abrupt_ack_child() {
    let p = std::env::var("AXTON_TASK2_CRASH_DB").unwrap();
    let store = FaultStore {
        inner: SqliteStore::open(&p).unwrap(),
        fail_commit: Arc::new(AtomicBool::new(false)),
        crash_ack: true,
    };
    let mut c = Client::open05(store, update_schema(), "User:u").unwrap();
    let b = c.freeze_batch05().unwrap().unwrap();
    c.acknowledge_batch05(&ack(
        &b,
        v05::MutationOutcome::Rejected {
            code: "write.denied".into(),
            message: None,
        },
    ))
    .unwrap();
    panic!("fault boundary did not terminate child")
}
#[test]
fn abrupt_process_exit_before_ack_commit_restores_exact_batch_and_later_direct() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut c = Client::open05(SqliteStore::open(&p).unwrap(), update_schema(), "User:u").unwrap();
    let context = c.request_context05().unwrap();
    c.install_authority05(
        &context,
        &[v05::AuthorityChange::Record {
            key: stream_key("e"),
            cursor: 1,
            state: state("base"),
        }],
        None,
    )
    .unwrap();
    let call = c
        .transaction(|tx| {
            tx.submit_mutation05("Write", 1, json!({"entry":{"id":"e","text":"A"}}), vec![])
        })
        .unwrap();
    let b = c.freeze_batch05().unwrap().unwrap();
    c.transaction(|tx| tx.direct(direct("B"))).unwrap();
    drop(c);
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "abrupt_ack_child", "--ignored", "--nocapture"])
        .env("AXTON_TASK2_CRASH_DB", &p)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(77));
    let mut c = Client::open05(SqliteStore::open(&p).unwrap(), update_schema(), "User:u").unwrap();
    assert_eq!(
        v05::encode(&c.freeze_batch05().unwrap().unwrap()).unwrap(),
        v05::encode(&b).unwrap()
    );
    assert_eq!(c.store_status05().unwrap().last_acknowledged_batch_id, 0);
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "B");
    let a = ack(
        &b,
        v05::MutationOutcome::Rejected {
            code: "write.denied".into(),
            message: None,
        },
    );
    c.acknowledge_batch05(&a).unwrap();
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "B");
    assert!(c.call_completion05(&call.call_id).unwrap().is_some());
}
#[test]
fn pending_optimism_keeps_authoritative_base_protection() {
    let d = tempfile::tempdir().unwrap();
    let mut c = Client::open05(
        SqliteStore::open(d.path().join("db")).unwrap(),
        update_schema(),
        "User:u",
    )
    .unwrap();
    let ctx = c.request_context05().unwrap();
    c.install_authority05(
        &ctx,
        &[v05::AuthorityChange::Record {
            key: stream_key("e"),
            cursor: 2,
            state: state("base"),
        }],
        None,
    )
    .unwrap();
    c.transaction(|tx| {
        tx.submit_mutation05(
            "Write",
            1,
            json!({"entry":{"id":"e","text":"optimistic"}}),
            vec![],
        )
    })
    .unwrap();
    assert_eq!(
        c.record_evidence05(&key()).unwrap().current.unwrap().cursor,
        2
    );
    c.install_cache05(
        &[v05::ReadRecord {
            key: stream_key("e"),
            cursor: (),
            state: state("cache"),
        }],
        true,
    )
    .unwrap();
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "optimistic");
}
#[test]
fn coverage_cannot_settle_missing_old_target_but_compatible_authority_can() {
    let d = tempfile::tempdir().unwrap();
    let mut c = open(&d.path().join("db"));
    let call = c
        .transaction(|tx| {
            tx.submit_mutation05(
                "Write",
                1,
                json!({"entry":{"id":"e","text":"A","note":null}}),
                vec![],
            )
        })
        .unwrap();
    let b = c.freeze_batch05().unwrap().unwrap();
    let a = ack(
        &b,
        v05::MutationOutcome::Accepted {
            sync_cursor: 5,
            result: Value::Null,
            targets: vec![v05::SettlementTarget::Stream {
                key: stream_key("e"),
                cursor: 5,
                fallback: v05::ReadRecord {
                    key: stream_key("e"),
                    cursor: (),
                    state: state("frozen"),
                },
            }],
        },
    );
    c.acknowledge_batch05(&a).unwrap();
    let context = c.request_context05().unwrap();
    c.install_authority05(&context, &[], Some((0, 40))).unwrap();
    assert!(c.call_completion05(&call.call_id).unwrap().is_none());
    assert_eq!(
        c.pending_settlement05().unwrap()[0].missing_keys,
        vec![stream_key("e")]
    );
    install(&mut c, "e", 45, Some("latest"));
    assert!(c.call_completion05(&call.call_id).unwrap().is_some());
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "latest");
}
#[test]
fn lifecycle_dependents_are_rejected_atomically_and_retained_but_not_replayed() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut s = serde_json::to_value(schema()).unwrap();
    let mut update = s["actions"][0].clone();
    update["name"] = json!("Edit");
    update["inputs"][0]["operation"] = json!("update");
    s["actions"].as_array_mut().unwrap().push(update);
    let mut c = Client::open05(
        SqliteStore::open(&p).unwrap(),
        Schema::from_value(s).unwrap(),
        "User:u",
    )
    .unwrap();
    let first = c
        .transaction(|tx| {
            tx.submit_mutation05(
                "Write",
                1,
                json!({"entry":{"id":"e","text":"A","note":null}}),
                vec![],
            )
        })
        .unwrap();
    let second = c
        .transaction(|tx| {
            tx.submit_mutation05("Edit", 1, json!({"entry":{"id":"e","text":"edit"}}), vec![])
        })
        .unwrap();
    let b = c.freeze_batch05().unwrap().unwrap();
    assert_eq!(b.mutations.len(), 1);
    let report = c
        .acknowledge_batch05(&ack(
            &b,
            v05::MutationOutcome::Rejected {
                code: "write.denied".into(),
                message: None,
            },
        ))
        .unwrap();
    assert_eq!(report.completions.len(), 2);
    assert!(c.call_completion05(&first.call_id).unwrap().is_some());
    assert!(c.call_completion05(&second.call_id).unwrap().is_some());
    assert!(c.freeze_batch05().unwrap().is_none());
    assert!(c.read(&key()).unwrap().is_none());
    let db = rusqlite::Connection::open(&p).unwrap();
    assert_eq!(
        db.query_row(
            "SELECT COUNT(*) FROM axton_mutation_queue_operation",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        2
    );
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM axton_mutation_operation", [], |r| r
            .get::<_, i64>(
            0
        ))
        .unwrap(),
        0
    );
}
#[test]
fn accepted_companion_keeps_original_order_and_later_authority_supersedes_it() {
    let d = tempfile::tempdir().unwrap();
    let mut c = Client::open05(
        SqliteStore::open(d.path().join("db")).unwrap(),
        update_schema(),
        "User:u",
    )
    .unwrap();
    let ctx = c.request_context05().unwrap();
    c.install_authority05(
        &ctx,
        &[
            v05::AuthorityChange::Record {
                key: stream_key("e"),
                cursor: 1,
                state: state("base"),
            },
            v05::AuthorityChange::Record {
                key: stream_key("other"),
                cursor: 1,
                state: state("other"),
            },
        ],
        None,
    )
    .unwrap();
    let companion = Operation {
        model: "Entry".into(),
        identity: json!({"id":"other"}),
        op: OperationKind::Update,
        values: Some(json!({"text":"companion"})),
    };
    c.transaction(|tx| {
        tx.submit_mutation05(
            "Write",
            1,
            json!({"entry":{"id":"e","text":"A"}}),
            vec![companion],
        )
    })
    .unwrap();
    let b = c.freeze_batch05().unwrap().unwrap();
    c.install_authority05(
        &ctx,
        &[
            v05::AuthorityChange::Record {
                key: stream_key("other"),
                cursor: 2,
                state: state("new-authority"),
            },
            v05::AuthorityChange::Record {
                key: stream_key("e"),
                cursor: 2,
                state: state("accepted"),
            },
        ],
        Some((0, 2)),
    )
    .unwrap();
    let a = ack(
        &b,
        v05::MutationOutcome::Accepted {
            sync_cursor: 2,
            result: Value::Null,
            targets: vec![v05::SettlementTarget::Stream {
                key: stream_key("e"),
                cursor: 2,
                fallback: v05::ReadRecord {
                    key: stream_key("e"),
                    cursor: (),
                    state: state("accepted"),
                },
            }],
        },
    );
    c.acknowledge_batch05(&a).unwrap();
    c.settle_ready05().unwrap();
    let key = RecordKey {
        model: "Entry".into(),
        identity: json!({"id":"other"}),
    };
    assert_eq!(c.read(&key).unwrap().unwrap()["text"], "new-authority");
    assert_eq!(
        c.record_evidence05(&key).unwrap().current.unwrap().cursor,
        2
    );
}
#[test]
fn settlement_constraint_failure_retains_acceptance_and_retries_locally() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut s = schema();
    s.models[0].unique.push(vec!["text".into()]);
    let mut c = Client::open05(SqliteStore::open(&p).unwrap(), s.clone(), "User:u").unwrap();
    c.transaction(|tx| {
        tx.direct(Operation {
            model: "Entry".into(),
            identity: json!({"id":"other"}),
            op: OperationKind::Create,
            values: Some(state("taken")),
        })
    })
    .unwrap();
    let call = c
        .transaction(|tx| {
            tx.submit_mutation05(
                "Write",
                1,
                json!({"entry":{"id":"e","text":"optimistic","note":null}}),
                vec![],
            )
        })
        .unwrap();
    let b = c.freeze_batch05().unwrap().unwrap();
    let a = ack(
        &b,
        v05::MutationOutcome::Accepted {
            sync_cursor: 0,
            result: Value::Null,
            targets: vec![v05::SettlementTarget::Private {
                record: v05::ReadRecord {
                    key: stream_key("e"),
                    cursor: (),
                    state: state("taken"),
                },
            }],
        },
    );
    c.acknowledge_batch05(&a).unwrap();
    let ctx = c.request_context05().unwrap();
    assert!(c.install_authority05(&ctx, &[], Some((0, 0))).is_err());
    assert!(c.call_completion05(&call.call_id).unwrap().is_none());
    assert_eq!(c.store_status05().unwrap().last_acknowledged_batch_id, 1);
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "optimistic");
    drop(c);
    let mut c = Client::open05(SqliteStore::open(&p).unwrap(), s, "User:u").unwrap();
    assert!(c.settle_ready05().unwrap().completions.is_empty());
    c.transaction(|tx| {
        tx.direct(Operation {
            model: "Entry".into(),
            identity: json!({"id":"other"}),
            op: OperationKind::Update,
            values: Some(json!({"text":"free"})),
        })
    })
    .unwrap();
    let ctx = c.request_context05().unwrap();
    c.install_authority05(&ctx, &[], Some((0, 0))).unwrap();
    assert!(c.call_completion05(&call.call_id).unwrap().is_some());
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "taken");
}
#[test]
#[ignore]
fn abrupt_assignment_child() {
    let p = std::env::var("AXTON_TASK2_CRASH_DB").unwrap();
    let store = FaultStore {
        inner: SqliteStore::open(&p).unwrap(),
        fail_commit: Arc::new(AtomicBool::new(false)),
        crash_ack: false,
    };
    let mut c = Client::open05(store, schema(), "User:u").unwrap();
    let b = c.freeze_batch05().unwrap().unwrap();
    std::fs::write(
        std::env::var("AXTON_TASK2_ASSIGNMENT_EVIDENCE").unwrap(),
        v05::encode(&b).unwrap(),
    )
    .unwrap();
    std::process::exit(81)
}
#[test]
fn abrupt_assignment_commit_boundaries_preserve_canonical_retry() {
    let d = tempfile::tempdir().unwrap();
    for mode in ["before", "after"] {
        let p = d.path().join(format!("{mode}.db"));
        let evidence = d.path().join(format!("{mode}.evidence"));
        let mut c = open(&p);
        c.transaction(|tx| {
            tx.submit_mutation05(
                "Write",
                1,
                json!({"entry":{"id":"e","text":"A","note":null}}),
                vec![],
            )
        })
        .unwrap();
        drop(c);
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "abrupt_assignment_child",
                "--ignored",
                "--nocapture",
            ])
            .env("AXTON_TASK2_CRASH_DB", &p)
            .env("AXTON_TASK2_ASSIGNMENT_CRASH", mode)
            .env("AXTON_TASK2_ASSIGNMENT_EVIDENCE", &evidence)
            .status()
            .unwrap();
        assert_eq!(result.code(), Some(if mode == "before" { 80 } else { 81 }));
        let mut c = open(&p);
        let b = c.freeze_batch05().unwrap().unwrap();
        if mode == "before" {
            assert_eq!(b.digest, std::fs::read_to_string(&evidence).unwrap());
        } else {
            assert_eq!(v05::encode(&b).unwrap(), std::fs::read(&evidence).unwrap());
        }
        drop(c);
        let mut c = open(&p);
        assert_eq!(c.freeze_batch05().unwrap().unwrap(), b);
    }
}

#[test]
fn zero_start_handshake_does_not_complete_bootstrap() {
    use axton_client::{ClientStore, engine::Engine};
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    drop(open(&p));
    let mut store = SqliteStore::open(&p).unwrap();
    let s = schema();
    let mut changed = std::collections::BTreeSet::new();
    store.begin().unwrap();
    Engine::new(&mut store, &s, &mut changed, false)
        .initialize_stream05(0)
        .unwrap();
    store.commit().unwrap();
    drop(store);
    let mut c = open(&p);
    let status = c.store_status05().unwrap();
    assert_eq!(status.start_cursor, Some(0));
    assert_eq!(status.cursor, Some(0));
    assert_eq!(status.bootstrap_cursor, None);
    drop(c);
    let mut store = SqliteStore::open(&p).unwrap();
    store.begin().unwrap();
    Engine::new(&mut store, &s, &mut changed, false)
        .initialize_stream05(10)
        .unwrap();
    store.commit().unwrap();
    drop(store);
    let mut c = open(&p);
    assert_eq!(c.store_status05().unwrap().start_cursor, Some(0));
    assert_eq!(c.store_status05().unwrap().bootstrap_cursor, None);
}

#[test]
fn discarded_mutation_and_lifecycle_dependent_keep_public_codes_after_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db");
    let mut descriptor: Value =
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap();
    descriptor["actions"] = json!([
        {"name":"Write","version":1,"inputs":[{"kind":"model","name":"entry","model":"Entry","operation":"create","cardinality":"single"}],"outputs":[]},
        {"name":"Edit","version":1,"inputs":[{"kind":"model","name":"entry","model":"Entry","operation":"update","fields":["text"],"cardinality":"single"}],"outputs":[]}
    ]);
    let schema = Schema::from_value(descriptor).unwrap();
    let mut client =
        Client::open05(SqliteStore::open(&path).unwrap(), schema.clone(), "User:u").unwrap();
    let created = client
        .transaction(|tx| {
            tx.submit_mutation05(
                "Write",
                1,
                json!({"entry":{"id":"e","text":"created","note":null}}),
                vec![],
            )
        })
        .unwrap();
    let dependent = client
        .transaction(|tx| {
            tx.submit_mutation05(
                "Edit",
                1,
                json!({"entry":{"id":"e","text":"dependent"}}),
                vec![],
            )
        })
        .unwrap();
    let report = client
        .transaction(|tx| tx.discard_mutation05(created.ordinal))
        .unwrap();
    assert_eq!(report.completions.len(), 2);
    drop(client);
    {
        use axton_client::ClientStore;
        let mut store = SqliteStore::open(&path).unwrap();
        assert_eq!(
            store
                .query(
                    "SELECT id,rejection_acknowledged FROM axton_mutation_queue ORDER BY id",
                    &[]
                )
                .unwrap()
                .rows,
            vec![
                vec![json!(created.ordinal), json!(1)],
                vec![json!(dependent.ordinal), json!(0)]
            ]
        );
    }

    let mut client = Client::open05(SqliteStore::open(&path).unwrap(), schema, "User:u").unwrap();
    for (call, expected) in [(&created, "dropped"), (&dependent, "dependency.rejected")] {
        let completion = client.call_completion05(&call.call_id).unwrap().unwrap();
        assert!(
            matches!(completion.outcome, axton_client::ActionOutcome::Failed { code, .. } if code == expected)
        );
        assert!(
            matches!(client.mutation_result05(call.ordinal).unwrap().unwrap().outcome, v05::MutationOutcome::Rejected { code, .. } if code == expected)
        );
    }
    assert!(client.freeze_batch05().unwrap().is_none());
    assert_eq!(client.read(&key()).unwrap(), None);
}

#[test]
fn dismissed_rejection_keeps_durable_completion_but_retires_input_after_reopen() {
    use axton_client::ClientStore;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db");
    let mut client = open(&path);
    let call = client
        .transaction(|tx| {
            tx.submit_mutation05(
                "Write",
                1,
                json!({"entry":{"id":"e","text":"denied","note":null}}),
                vec![],
            )
        })
        .unwrap();
    let batch = client.freeze_batch05().unwrap().unwrap();
    client
        .acknowledge_batch05(&ack(
            &batch,
            v05::MutationOutcome::Rejected {
                code: "write.denied".into(),
                message: Some("retained outcome".into()),
            },
        ))
        .unwrap();
    client
        .transaction(|tx| tx.dismiss_rejection05(call.ordinal))
        .unwrap();
    drop(client);
    let mut store = SqliteStore::open(&path).unwrap();
    assert_eq!(store.query("SELECT rejection_acknowledged,rejection_code,rejection_message FROM axton_mutation_queue WHERE id=?", &[json!(call.ordinal)]).unwrap().rows,
        vec![vec![json!(1),json!("write.denied"),json!("retained outcome")]]);
    for table in [
        "axton_mutation_queue_operation",
        "axton_mutation_prerequisite",
        "axton_mutation_dependency",
    ] {
        assert!(
            store
                .query(&format!("SELECT * FROM {table}"), &[])
                .unwrap()
                .rows
                .is_empty()
        );
    }
    drop(store);
    let mut client = open(&path);
    let completion = client.call_completion05(&call.call_id).unwrap().unwrap();
    assert!(
        matches!(completion.outcome, axton_client::ActionOutcome::Failed {code, ..}
        if code == "write.denied")
    );
    assert!(client.mutation_result05(call.ordinal).unwrap().is_some());
    client
        .transaction(|tx| tx.dismiss_rejection05(call.ordinal))
        .unwrap();
    assert!(client.freeze_batch05().unwrap().is_none());
}

#[test]
fn existing_format5_adds_rejection_acknowledgement_only_after_valid_admission() {
    use axton_client::ClientStore;
    let directory = tempfile::tempdir().unwrap();
    let fresh_path = directory.path().join("fresh.db");
    drop(open(&fresh_path));
    let mut fresh = SqliteStore::open(&fresh_path).unwrap();
    let metadata = fresh
        .query("SELECT * FROM axton_store", &[])
        .unwrap()
        .rows
        .remove(0);
    let path = directory.path().join("prior-format5.db");
    std::fs::copy(&fresh_path, &path).unwrap();
    let mut prior = SqliteStore::open(&path).unwrap();
    prior
        .execute_batch("ALTER TABLE axton_mutation_queue DROP COLUMN rejection_acknowledged")
        .unwrap();
    drop(prior);
    assert!(Client::open05(SqliteStore::open(&path).unwrap(), schema(), "User:other").is_err());
    let mut prior = SqliteStore::open(&path).unwrap();
    assert!(
        !prior
            .query("PRAGMA table_info(axton_mutation_queue)", &[])
            .unwrap()
            .rows
            .iter()
            .any(|r| r[1] == "rejection_acknowledged")
    );
    drop(prior);
    drop(open(&path));
    let mut prior = SqliteStore::open(&path).unwrap();
    assert!(
        prior
            .query("PRAGMA table_info(axton_mutation_queue)", &[])
            .unwrap()
            .rows
            .iter()
            .any(|r| r[1] == "rejection_acknowledged")
    );
    assert_eq!(
        prior.query("SELECT * FROM axton_store", &[]).unwrap().rows[0],
        metadata
    );
}

#[test]
fn legacy_drop_retains_own_refusal_until_explicit_acknowledgement() {
    use axton_client::ClientStore;
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db");
    let mut client = open(&path);
    let call = client
        .transaction(|tx| {
            tx.submit_mutation05(
                "Write",
                1,
                json!({"entry":{"id":"e","text":"drop","note":null}}),
                vec![],
            )
        })
        .unwrap();
    client
        .transaction(|tx| tx.drop_mutation05(call.ordinal))
        .unwrap();
    drop(client);
    let mut store = SqliteStore::open(&path).unwrap();
    assert_eq!(
        store
            .query(
                "SELECT rejection_acknowledged,rejection_code FROM axton_mutation_queue WHERE id=?",
                &[json!(call.ordinal)]
            )
            .unwrap()
            .rows,
        vec![vec![json!(0), json!("dropped")]]
    );
    assert!(
        !store
            .query(
                "SELECT * FROM axton_mutation_queue_operation WHERE mutation_id=?",
                &[json!(call.ordinal)]
            )
            .unwrap()
            .rows
            .is_empty()
    );
    drop(store);
    let mut client = open(&path);
    let completed = client.call_completion05(&call.call_id).unwrap().unwrap();
    assert!(
        matches!(&completed.outcome, axton_client::ActionOutcome::Failed {code,..} if code=="dropped")
    );
    client
        .transaction(|tx| tx.dismiss_rejection05(call.ordinal))
        .unwrap();
    assert_eq!(
        client.call_completion05(&call.call_id).unwrap(),
        Some(completed)
    );
    assert!(client.freeze_batch05().unwrap().is_none());
}
