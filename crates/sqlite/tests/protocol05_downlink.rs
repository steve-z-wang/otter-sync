use axton_client::{Client, Schema, sync05::DeliveryQueue, v05};
use axton_sqlite::SqliteStore;
fn client(path: &std::path::Path) -> Client<SqliteStore> {
    Client::open05(
        SqliteStore::open(path).unwrap(),
        Schema::from_value(
            serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap(),
        )
        .unwrap(),
        "User:u",
    )
    .unwrap()
}
fn plan(
    context: v05::RequestContext,
    id: &str,
    after: u64,
    through: u64,
    bootstrap: bool,
) -> v05::FrozenDelivery {
    v05::freeze_delivery(
        context,
        id.into(),
        if bootstrap {
            v05::DeliveryPurpose::Bootstrap
        } else {
            v05::DeliveryPurpose::Sync
        },
        after,
        through,
        through,
        10000,
        vec![v05::DeliveryUnit {
            index: 0,
            through: Some(through),
            changes: vec![],
        }],
        1,
    )
    .unwrap()
}
#[test]
fn empty_bootstrap_zero_commits_only_verified_final_and_survives_reopen() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut c = client(&p);
    c.initialize_stream05(0).unwrap();
    assert_eq!(c.store_status05().unwrap().bootstrap_cursor, None);
    let context = c.request_context05().unwrap();
    let f = plan(context.clone(), "p", 0, 0, true);
    let mut q = DeliveryQueue::new(1024 * 1024, 2);
    q.receive(&f.header, &f.parts, &context, 1).unwrap();
    c.apply_next_delivery05(&mut q, 1).unwrap().unwrap();
    assert_eq!(c.store_status05().unwrap().bootstrap_cursor, Some(0));
    drop(c);
    let mut c = client(&p);
    assert_eq!(c.store_status05().unwrap().bootstrap_cursor, Some(0));
    q.receive(&f.header, &f.parts, &context, 2).unwrap();
    assert!(c.apply_next_delivery05(&mut q, 2).unwrap().is_none());
}
#[test]
fn missing_prefix_is_retained_and_repair_capacity_is_reserved() {
    let d = tempfile::tempdir().unwrap();
    let mut c = client(&d.path().join("db"));
    c.initialize_stream05(0).unwrap();
    let context = c.request_context05().unwrap();
    let later = plan(context.clone(), "later", 2, 3, false);
    let early = plan(context.clone(), "early", 0, 2, false);
    let mut q = DeliveryQueue::new(1024 * 1024, 2);
    q.receive(&later.header, &later.parts, &context, 1).unwrap();
    assert!(c.apply_next_delivery05(&mut q, 1).unwrap().is_none());
    q.receive(&early.header, &early.parts, &context, 1).unwrap();
    c.apply_next_delivery05(&mut q, 1).unwrap().unwrap();
    c.apply_next_delivery05(&mut q, 1).unwrap().unwrap();
    assert_eq!(c.store_status05().unwrap().cursor, Some(3));
}
fn fragmented(context: v05::RequestContext, bootstrap: bool) -> v05::FrozenDelivery {
    v05::freeze_delivery(
        context,
        "fragmented".into(),
        if bootstrap {
            v05::DeliveryPurpose::Bootstrap
        } else {
            v05::DeliveryPurpose::Sync
        },
        0,
        2,
        3,
        10000,
        vec![v05::DeliveryUnit {
            index: 0,
            through: Some(2),
            changes: vec![change("a", 1), change("b", 1)],
        }],
        1,
    )
    .unwrap()
}
fn change(id: &str, cursor: u64) -> v05::AuthorityChange {
    v05::AuthorityChange::Record {
        key: v05::RecordKey {
            model: "Entry".into(),
            identity: serde_json::json!({"id":id}),
        },
        cursor,
        state: serde_json::json!({"text":id,"note":null}),
    }
}
#[test]
fn socket_http_overlap_repeated_fragment_and_changed_payload_are_fenced() {
    let d = tempfile::tempdir().unwrap();
    let mut c = client(&d.path().join("db"));
    c.initialize_stream05(0).unwrap();
    let context = c.request_context05().unwrap();
    let f = fragmented(context.clone(), false);
    let mut q = DeliveryQueue::new(1000000, 4);
    q.receive(&f.header, &f.parts[1..], &context, 1).unwrap();
    assert!(c.apply_next_delivery05(&mut q, 1).unwrap().is_none());
    assert_eq!(c.store_status05().unwrap().cursor, Some(0));
    q.receive(&f.header, &f.parts[1..], &context, 1).unwrap();
    let mut changed = f.parts[1].clone();
    changed.changes[0] = change("b", 2);
    assert!(q.receive(&f.header, &[changed], &context, 1).is_err());
    let mut changed_header = f.header.clone();
    changed_header.expires_at += 1;
    assert!(
        q.receive(&changed_header, &f.parts[..1], &context, 1)
            .is_err()
    );
    q.receive(&f.header, &f.parts[..1], &context, 1).unwrap();
    let report = c.apply_next_delivery05(&mut q, 1).unwrap().unwrap();
    assert_eq!(report.applied, 2);
    assert_eq!(c.store_status05().unwrap().cursor, Some(2));
}
#[test]
fn failed_sql_apply_keeps_unit_and_progress_unchanged_until_explicit_retry() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut c = client(&p);
    c.initialize_stream05(0).unwrap();
    let context = c.request_context05().unwrap();
    let mut units = vec![v05::DeliveryUnit {
        index: 0,
        through: Some(1),
        changes: vec![change("a", 1)],
    }];
    if let v05::AuthorityChange::Record { state, .. } = &mut units[0].changes[0] {
        *state = serde_json::json!({"text":7,"note":null})
    }
    let f = v05::freeze_delivery(
        context.clone(),
        "bad".into(),
        v05::DeliveryPurpose::Sync,
        0,
        1,
        1,
        10000,
        units,
        1,
    )
    .unwrap();
    let mut q = DeliveryQueue::new(1000000, 4);
    q.receive(&f.header, &f.parts, &context, 1).unwrap();
    assert!(c.apply_next_delivery05(&mut q, 1).is_err());
    assert_eq!(q.len(), 1);
    let overlapping = plan(context.clone(), "overlapping-good", 0, 1, false);
    q.receive(&overlapping.header, &overlapping.parts, &context, 1)
        .unwrap();
    // A failed Sync unit blocks the lane, including another verified offer.
    assert!(c.apply_next_delivery05(&mut q, 1).unwrap().is_none());
    assert_eq!(c.store_status05().unwrap().cursor, Some(0));
    drop(c);
    let mut c = client(&p);
    assert_eq!(c.store_status05().unwrap().cursor, Some(0));
    assert_eq!(
        c.read(&axton_client::RecordKey {
            model: "Entry".into(),
            identity: serde_json::json!({"id":"a"})
        })
        .unwrap(),
        None
    );
}
#[test]
fn late_generation_and_expired_plan_leave_progress_unchanged() {
    let d = tempfile::tempdir().unwrap();
    let mut c = client(&d.path().join("db"));
    c.initialize_stream05(0).unwrap();
    let context = c.request_context05().unwrap();
    let f = plan(context.clone(), "old", 0, 1, false);
    let mut q = DeliveryQueue::new(1000000, 4);
    assert!(q.receive(&f.header, &f.parts, &context, 10000).is_err());
    let mut replacement = context.clone();
    replacement.materialization = "replacement".into();
    assert!(q.receive(&f.header, &f.parts, &replacement, 1).is_err());
    assert_eq!(c.store_status05().unwrap().cursor, Some(0));
}
#[test]
fn zero_bootstrap_intermediate_no_progress_does_not_mark_complete() {
    let d = tempfile::tempdir().unwrap();
    let mut c = client(&d.path().join("db"));
    c.initialize_stream05(0).unwrap();
    let context = c.request_context05().unwrap();
    let f = v05::freeze_delivery(
        context.clone(),
        "zero".into(),
        v05::DeliveryPurpose::Bootstrap,
        0,
        0,
        2,
        10000,
        vec![
            v05::DeliveryUnit {
                index: 0,
                through: None,
                changes: vec![change("a", 1)],
            },
            v05::DeliveryUnit {
                index: 1,
                through: Some(0),
                changes: vec![change("b", 2)],
            },
        ],
        1,
    )
    .unwrap();
    let mut q = DeliveryQueue::new(1000000, 4);
    q.receive(&f.header, &f.parts[..1], &context, 1).unwrap();
    c.apply_next_delivery05(&mut q, 1).unwrap().unwrap();
    assert_eq!(c.store_status05().unwrap().bootstrap_cursor, None);
    q.receive(&f.header, &f.parts[1..], &context, 1).unwrap();
    c.apply_next_delivery05(&mut q, 1).unwrap().unwrap();
    assert_eq!(c.store_status05().unwrap().bootstrap_cursor, Some(0));
}
#[test]
fn oversized_atomic_unit_spills_to_disk_and_applies_as_one_commit() {
    let d = tempfile::tempdir().unwrap();
    let mut c = client(&d.path().join("db"));
    c.initialize_stream05(0).unwrap();
    let context = c.request_context05().unwrap();
    let f = fragmented(context.clone(), false);
    let mut q = DeliveryQueue::new(1024, 4);
    q.receive(&f.header, &f.parts, &context, 1).unwrap();
    assert!(c.apply_next_delivery05(&mut q, 1).unwrap().is_some());
    assert_eq!(c.store_status05().unwrap().cursor, Some(2));
}
#[test]
fn duplicate_identity_across_units_cannot_commit_later_prefix() {
    let d = tempfile::tempdir().unwrap();
    let mut c = client(&d.path().join("db"));
    c.initialize_stream05(0).unwrap();
    let context = c.request_context05().unwrap();
    let mut f = v05::freeze_delivery(
        context.clone(),
        "duplicated".into(),
        v05::DeliveryPurpose::Sync,
        0,
        2,
        2,
        10000,
        vec![
            v05::DeliveryUnit {
                index: 0,
                through: Some(1),
                changes: vec![change("a", 1)],
            },
            v05::DeliveryUnit {
                index: 1,
                through: Some(2),
                changes: vec![change("b", 2)],
            },
        ],
        1,
    )
    .unwrap();
    f.parts[1].changes[0] = change("a", 2);
    f.header.units[1].digest = v05::unit_digest(&v05::DeliveryUnit {
        index: 1,
        through: Some(2),
        changes: f.parts[1].changes.clone(),
    })
    .unwrap();
    f.header.units[1].parts[0] = v05::part_digest(&f.parts[1]).unwrap();
    f.header.digest = v05::delivery_digest(&f.header).unwrap();
    for part in &mut f.parts {
        part.plan_digest = f.header.digest.clone();
    }
    let mut q = DeliveryQueue::new(1000000, 4);
    q.receive(&f.header, &f.parts, &context, 1).unwrap();
    c.apply_next_delivery05(&mut q, 1).unwrap().unwrap();
    assert!(c.apply_next_delivery05(&mut q, 1).is_err());
    assert_eq!(c.store_status05().unwrap().cursor, Some(1));
}
struct CrashStore {
    store: SqliteStore,
    armed: std::sync::Arc<std::sync::atomic::AtomicBool>,
}
impl axton_client::ClientStore for CrashStore {
    fn begin(&mut self) -> axton_client::Result<()> {
        self.store.begin()
    }
    fn commit(&mut self) -> axton_client::Result<()> {
        self.store.commit()?;
        if self.armed.load(std::sync::atomic::Ordering::SeqCst) {
            std::process::exit(73)
        }
        Ok(())
    }
    fn rollback(&mut self) -> axton_client::Result<()> {
        self.store.rollback()
    }
    fn savepoint(&mut self, n: &str) -> axton_client::Result<()> {
        self.store.savepoint(n)
    }
    fn release(&mut self, n: &str) -> axton_client::Result<()> {
        self.store.release(n)
    }
    fn rollback_to(&mut self, n: &str) -> axton_client::Result<()> {
        self.store.rollback_to(n)
    }
    fn execute(&mut self, s: &str, p: &[serde_json::Value]) -> axton_client::Result<usize> {
        self.store.execute(s, p)
    }
    fn execute_batch(&mut self, s: &str) -> axton_client::Result<()> {
        self.store.execute_batch(s)
    }
    fn query(
        &mut self,
        s: &str,
        p: &[serde_json::Value],
    ) -> axton_client::Result<axton_client::store::SqlRows> {
        self.store.query(s, p)
    }
    fn query_committed(
        &mut self,
        s: &str,
        p: &[serde_json::Value],
    ) -> axton_client::Result<axton_client::store::SqlRows> {
        self.store.query_committed(s, p)
    }
}
#[test]
#[ignore]
fn crash_after_sql_commit_before_dequeue_child() {
    let path = std::env::var("AXTON_TASK5_CRASH_DB").unwrap();
    let armed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let store = CrashStore {
        store: SqliteStore::open(path).unwrap(),
        armed: armed.clone(),
    };
    let mut c = Client::open05(
        store,
        Schema::from_value(
            serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap(),
        )
        .unwrap(),
        "User:u",
    )
    .unwrap();
    c.initialize_stream05(0).unwrap();
    let context = c.request_context05().unwrap();
    let f = v05::freeze_delivery(
        context.clone(),
        "crash-plan".into(),
        v05::DeliveryPurpose::Sync,
        0,
        1,
        1,
        10000,
        vec![v05::DeliveryUnit {
            index: 0,
            through: Some(1),
            changes: vec![change("a", 1)],
        }],
        1,
    )
    .unwrap();
    let mut q = DeliveryQueue::new(1000000, 4);
    q.receive(&f.header, &f.parts, &context, 1).unwrap();
    armed.store(true, std::sync::atomic::Ordering::SeqCst);
    let _ = c.apply_next_delivery05(&mut q, 1);
    panic!("crash commit hook not reached");
}
#[test]
fn crash_after_commit_before_dequeue_reopens_without_reapplying_later_direct() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("db");
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "crash_after_sql_commit_before_dequeue_child",
        ])
        .env("AXTON_TASK5_CRASH_DB", &path)
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(73));
    let mut c = client(&path);
    assert_eq!(c.store_status05().unwrap().cursor, Some(1));
    let key = axton_client::RecordKey {
        model: "Entry".into(),
        identity: serde_json::json!({"id":"a"}),
    };
    assert_eq!(c.read(&key).unwrap().unwrap()["text"], "a");
    c.transaction(|tx| {
        tx.direct(axton_client::Operation {
            model: "Entry".into(),
            identity: key.identity.clone(),
            op: axton_client::OperationKind::Update,
            values: Some(serde_json::json!({"text":"later"})),
        })
    })
    .unwrap();
    let context = c.request_context05().unwrap();
    let f = v05::freeze_delivery(
        context.clone(),
        "crash-plan".into(),
        v05::DeliveryPurpose::Sync,
        0,
        1,
        1,
        10000,
        vec![v05::DeliveryUnit {
            index: 0,
            through: Some(1),
            changes: vec![change("a", 1)],
        }],
        1,
    )
    .unwrap();
    let mut q = DeliveryQueue::new(1000000, 4);
    q.receive(&f.header, &f.parts, &context, 1).unwrap();
    assert!(c.apply_next_delivery05(&mut q, 1).unwrap().is_none());
    assert_eq!(c.read(&key).unwrap().unwrap()["text"], "later");
}
#[test]
fn owned_schema_transfer_waits_for_all_parts_and_reproves_newly_held_keys() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("db");
    let mut c = client(&path);
    c.initialize_stream05(5).unwrap();
    let old = c.request_context05().unwrap();
    c.install_authority05(&old, &[change("a", 2), change("b", 3)], None)
        .unwrap();
    drop(c);
    let mut schema: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap();
    schema["models"][0]["version"] = serde_json::json!(2);
    let mut c = Client::open05(
        SqliteStore::open(&path).unwrap(),
        Schema::from_value(schema).unwrap(),
        "User:u",
    )
    .unwrap();
    let pending = c.pending_schema05().unwrap().unwrap();
    let request = v05::MaterializationRequest {
        context: pending.desired_context.clone(),
        request_id: "schema".into(),
        owner: v05::MaterializationOwner::Schema {
            previous_materialization: old.materialization.clone(),
        },
        keys: pending.authority_keys,
        models: pending.bootstrap_models,
        continuation: None,
    };
    let f = v05::freeze_materialization(
        request.context.clone(),
        "schema-plan".into(),
        request.owner.clone(),
        5,
        10000,
        vec![
            v05::DeliveryUnit {
                index: 0,
                through: None,
                changes: vec![change("a", 2)],
            },
            v05::DeliveryUnit {
                index: 1,
                through: None,
                changes: vec![change("b", 3)],
            },
        ],
        1,
    )
    .unwrap();
    let mut q = DeliveryQueue::new(1000000, 4);
    q.receive_owned(
        &request,
        &v05::MaterializationResponse {
            request_id: "schema".into(),
            delivery: v05::DeliveryResponse {
                header: f.header.clone(),
                parts: f.parts[..1].to_vec(),
            },
        },
        &old,
        1,
    )
    .unwrap();
    assert!(c.apply_next_delivery05(&mut q, 1).unwrap().is_none());
    assert_eq!(c.request_context05().unwrap(), old);
    c.install_authority05(&old, &[change("c", 4)], None)
        .unwrap();
    q.receive_owned(
        &request,
        &v05::MaterializationResponse {
            request_id: "schema".into(),
            delivery: v05::DeliveryResponse {
                header: f.header,
                parts: f.parts[1..].to_vec(),
            },
        },
        &old,
        1,
    )
    .unwrap();
    assert!(c.apply_next_delivery05(&mut q, 1).is_err());
    assert_eq!(c.request_context05().unwrap(), old);
    assert_eq!(c.store_status05().unwrap().cursor, Some(5));
    let pending = c.pending_schema05().unwrap().unwrap();
    let request = v05::MaterializationRequest {
        keys: pending.authority_keys,
        ..request
    };
    let f = v05::freeze_materialization(
        request.context.clone(),
        "schema-repair".into(),
        request.owner.clone(),
        5,
        10000,
        vec![v05::DeliveryUnit {
            index: 0,
            through: None,
            changes: vec![change("a", 2), change("b", 3), change("c", 4)],
        }],
        1,
    )
    .unwrap();
    q.receive_owned(
        &request,
        &v05::MaterializationResponse {
            request_id: "schema".into(),
            delivery: v05::DeliveryResponse {
                header: f.header,
                parts: f.parts,
            },
        },
        &old,
        1,
    )
    .unwrap();
    c.apply_next_delivery05(&mut q, 1).unwrap().unwrap();
    assert_eq!(c.request_context05().unwrap(), request.context);
    assert_eq!(c.store_status05().unwrap().cursor, Some(5));
    assert!(c.pending_schema05().unwrap().is_none());
}
#[test]
fn foreign_live_context_is_only_a_head_hint_for_retained_context_repair() {
    use axton_client::{
        runtime::{EffectOutcome, Event, HttpRoute, Operation},
        sync05::{Control, StoreReport},
    };
    let d = tempfile::tempdir().unwrap();
    let mut c = client(&d.path().join("db"));
    c.initialize_stream05(0).unwrap();
    let context = c.request_context05().unwrap();
    let f = plan(context.clone(), "boot", 0, 0, true);
    let mut q = DeliveryQueue::new(10000, 4);
    q.receive(&f.header, &f.parts, &context, 1).unwrap();
    c.apply_next_delivery05(&mut q, 1).unwrap();
    let status = c.store_status05().unwrap();
    let mut control = Control::new(status.clone());
    control.connect().unwrap();
    let events = control.events();
    let h = events
        .into_iter()
        .find_map(|e| match e {
            Event::Effect {
                effect_id,
                operation:
                    Operation::Http {
                        route: HttpRoute::Handshake,
                        ..
                    },
            } => Some(effect_id),
            _ => None,
        })
        .unwrap();
    control.receive(&h,EffectOutcome{ok:true,value:Some(serde_json::json!({"protocol":5,"storeId":context.store_id,"stream":context.stream,"head":0}).to_string().into()),error:None},1).unwrap();
    let socket = control
        .events()
        .into_iter()
        .find_map(|e| match e {
            Event::Effect {
                effect_id,
                operation: Operation::Socket { .. },
            } => Some(effect_id),
            _ => None,
        })
        .unwrap();
    let _ = control.jobs();
    control.report(StoreReport::Snapshot(status), 1).unwrap();
    let _ = control.jobs();
    let _ = control.events();
    let mut foreign = context.clone();
    foreign.materialization = "foreign".into();
    let f = plan(foreign, "foreign-live", 0, 5, false);
    control.receive(&socket,EffectOutcome{ok:true,value:Some(serde_json::json!({"event":"message","body":serde_json::json!({"header":f.header,"parts":f.parts}).to_string()})),error:None},1).unwrap();
    assert!(
        control
            .jobs()
            .into_iter()
            .all(|j| !matches!(j, axton_client::sync05::StoreCommand::Apply { .. }))
    );
    let repair = control
        .events()
        .into_iter()
        .find_map(|e| match e {
            Event::Effect {
                operation:
                    Operation::Http {
                        route: HttpRoute::Pull,
                        body,
                    },
                ..
            } => Some(serde_json::from_str::<v05::DeltaRequest>(&body).unwrap()),
            _ => None,
        })
        .unwrap();
    assert_eq!(repair.context, context);
    assert_eq!((repair.after, repair.through), (0, 5));
    assert_eq!(c.store_status05().unwrap().cursor, Some(0));
}

#[test]
fn slow_observer_receiver_does_not_hold_apply_and_notices_only_commits() {
    use std::collections::BTreeSet;
    let d = tempfile::tempdir().unwrap();
    let mut c = client(&d.path().join("db"));
    c.initialize_stream05(0).unwrap();
    let context = c.request_context05().unwrap();
    let observer = c.watch(BTreeSet::from(["Entry".into()]));
    let first = v05::freeze_delivery(
        context.clone(),
        "observer-one".into(),
        v05::DeliveryPurpose::Sync,
        0,
        1,
        1,
        10000,
        vec![v05::DeliveryUnit {
            index: 0,
            through: Some(1),
            changes: vec![change("a", 1)],
        }],
        1,
    )
    .unwrap();
    let second = v05::freeze_delivery(
        context.clone(),
        "observer-two".into(),
        v05::DeliveryPurpose::Sync,
        1,
        2,
        2,
        10000,
        vec![v05::DeliveryUnit {
            index: 0,
            through: Some(2),
            changes: vec![change("b", 2)],
        }],
        1,
    )
    .unwrap();
    let mut q = DeliveryQueue::new(100000, 4);
    q.receive(&first.header, &first.parts, &context, 1).unwrap();
    assert!(observer.try_recv().is_err());
    c.apply_next_delivery05(&mut q, 1).unwrap();
    // The observer deliberately does not consume the first committed notice.
    q.receive(&second.header, &second.parts, &context, 1)
        .unwrap();
    c.apply_next_delivery05(&mut q, 1).unwrap();
    assert_eq!(c.store_status05().unwrap().cursor, Some(2));
    assert_eq!(observer.try_iter().count(), 2);
}

#[test]
fn manifest_over_capacity_is_refused_without_data_or_progress() {
    let d = tempfile::tempdir().unwrap();
    let mut c = client(&d.path().join("db"));
    c.initialize_stream05(0).unwrap();
    let before = c.store_status05().unwrap();
    let f = plan(before.context.clone(), "too-large", 0, 1, false);
    let mut q = DeliveryQueue::new(16, 4);
    assert!(q.receive(&f.header, &f.parts, &before.context, 1).is_err());
    assert!(q.is_empty());
    assert_eq!(
        serde_json::to_value(c.store_status05().unwrap()).unwrap(),
        serde_json::to_value(before).unwrap()
    );
}

#[test]
fn terminal_admission_refusal_cancels_control_without_auth_or_backoff() {
    use axton_client::{
        runtime::{Diagnostic, EffectError, EffectOutcome, Event, Operation},
        sync05::Control,
    };
    let d = tempfile::tempdir().unwrap();
    let mut c = client(&d.path().join("db"));
    let status = c.store_status05().unwrap();
    let mut control = Control::new(status.clone());
    control.set_refresh_auth(true);
    control.connect().unwrap();
    let events = control.events();
    let id = events
        .iter()
        .find_map(|e| {
            if let Event::Effect { effect_id, .. } = e {
                Some(effect_id.clone())
            } else {
                None
            }
        })
        .unwrap();
    control
        .receive(
            &id,
            EffectOutcome {
                ok: false,
                value: None,
                error: Some(EffectError {
                    message: "minimum build".into(),
                    status: Some(426),
                    retry: false,
                    refusal: Some("{\"minimumBuild\":7}".into()),
                }),
            },
            1,
        )
        .unwrap();
    let events = control.events();
    assert!(events.iter().any(|e| matches!(
        e,
        Event::Report {
            diagnostic: Diagnostic::Refused { status: 426, .. }
        }
    )));
    assert!(!events.iter().any(|e| matches!(
        e,
        Event::Effect {
            operation: Operation::Timer { .. } | Operation::RefreshAuth,
            ..
        }
    )));
    assert!(control.jobs().is_empty());
    assert_eq!(
        serde_json::to_value(c.store_status05().unwrap()).unwrap(),
        serde_json::to_value(status).unwrap()
    );
}

#[test]
fn explicit_reset_retires_frozen_batch_and_reused_ids_have_new_store_owner() {
    use serde_json::json;
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("db");
    let mut s: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap();
    s["actions"] = json!([{"name":"Write","version":1,"inputs":[{"kind":"model","name":"entry","model":"Entry","operation":"create","cardinality":"single"}],"outputs":[]}]);
    let schema = Schema::from_value(s).unwrap();
    let mut c =
        Client::open05(SqliteStore::open(&path).unwrap(), schema.clone(), "User:u").unwrap();
    let old = c
        .transaction(|tx| {
            tx.submit_mutation05(
                "Write",
                1,
                json!({"entry":{"id":"e","text":"old","note":null}}),
                vec![],
            )
        })
        .unwrap();
    let batch = c.freeze_batch05().unwrap().unwrap();
    assert!(c.reset_store05(false).is_err());
    assert_eq!(c.freeze_batch05().unwrap().unwrap(), batch);
    let report = c.reset_store05(true).unwrap();
    assert_ne!(report.context.store_id, batch.context.store_id);
    assert_eq!(
        report.abandoned_calls,
        vec![axton_client::AbandonedCall {
            call_id: old.call_id,
            frozen: true
        }]
    );
    assert!(
        c.read(&axton_client::RecordKey {
            model: "Entry".into(),
            identity: json!({"id":"e"})
        })
        .unwrap()
        .is_none()
    );
    let late = v05::BatchAcknowledgement {
        context: batch.context.clone(),
        batch_id: batch.batch_id,
        digest: batch.digest,
        results: vec![v05::MutationResult {
            mutation_id: old.ordinal,
            outcome: v05::MutationOutcome::Rejected {
                code: "write.denied".into(),
                message: None,
            },
        }],
    };
    assert!(c.acknowledge_batch05(&late).is_err());
    let call = c
        .transaction(|tx| {
            tx.submit_mutation05(
                "Write",
                1,
                json!({"entry":{"id":"e","text":"new","note":null}}),
                vec![],
            )
        })
        .unwrap();
    let fresh = c.freeze_batch05().unwrap().unwrap();
    assert_eq!(fresh.batch_id, batch.batch_id);
    assert_eq!(call.ordinal, old.ordinal);
    assert_ne!(fresh.context.store_id, batch.context.store_id);
    drop(c);
    let mut c = Client::open05(SqliteStore::open(&path).unwrap(), schema, "User:u").unwrap();
    assert_eq!(c.freeze_batch05().unwrap().unwrap(), fresh);
}

#[test]
fn completed_and_expired_transfer_staging_is_cleared() {
    let d = tempfile::tempdir().unwrap();
    let mut c = client(&d.path().join("db"));
    c.initialize_stream05(0).unwrap();
    let context = c.request_context05().unwrap();
    let f = fragmented(context.clone(), false);
    let mut q = DeliveryQueue::new(100000, 4);
    q.receive(&f.header, &f.parts, &context, 1).unwrap();
    c.apply_next_delivery05(&mut q, 1).unwrap();
    assert_eq!(
        c.read_sql("SELECT COUNT(*) AS n FROM axton_delivery_progress", &[])
            .unwrap()[0]["n"],
        0
    );
    assert_eq!(
        c.read_sql("SELECT COUNT(*) AS n FROM axton_delivery_key", &[])
            .unwrap()[0]["n"],
        0
    );
}

#[test]
fn earlier_missing_prefix_can_replace_near_capacity_future_manifest() {
    let d = tempfile::tempdir().unwrap();
    let mut c = client(&d.path().join("db"));
    c.initialize_stream05(0).unwrap();
    let context = c.request_context05().unwrap();
    let later = plan(context.clone(), "future", 2, 3, false);
    let early = plan(context.clone(), "repair", 0, 2, false);
    let cap = serde_json::to_vec(&later.header).unwrap().len()
        + serde_json::to_vec(&early.header).unwrap().len()
        - 1;
    let mut q = DeliveryQueue::new(cap, 4);
    q.receive(&later.header, &later.parts, &context, 1).unwrap();
    assert!(q.receive(&early.header, &early.parts, &context, 1).is_ok());
    c.apply_next_delivery05(&mut q, 1).unwrap();
    assert_eq!(c.store_status05().unwrap().cursor, Some(2));
}

#[test]
fn stopped_control_ignores_already_dispatched_freeze_report() {
    use axton_client::{
        runtime::{ClientRuntime, Event, HttpRoute, Operation},
        sync05::Control,
    };
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("db");
    let mut s: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap();
    s["actions"] = serde_json::json!([{"name":"Write","version":1,"inputs":[],"outputs":[]}]);
    let mut c = Client::open05(
        SqliteStore::open(&path).unwrap(),
        Schema::from_value(s).unwrap(),
        "User:u",
    )
    .unwrap();
    c.transaction(|tx| tx.submit_mutation05("Write", 1, serde_json::json!({}), vec![]))
        .unwrap();
    let mut control = Control::new(c.store_status05().unwrap());
    control.connect().unwrap();
    control.events();
    control.wake();
    let command = control.jobs().into_iter().next().unwrap();
    let mut runtime = ClientRuntime::new(c);
    let report = runtime.store_worker05(command).unwrap();
    control.stop();
    control.events();
    control.report(report, 1).unwrap();
    assert!(!control.events().iter().any(|e| matches!(
        e,
        Event::Effect {
            operation: Operation::Http {
                route: HttpRoute::Push,
                ..
            },
            ..
        }
    )));
}

#[test]
fn partial_transfer_retention_is_bounded_and_expiry_clears_keys() {
    let d = tempfile::tempdir().unwrap();
    let mut c = client(&d.path().join("db"));
    c.initialize_stream05(0).unwrap();
    let context = c.request_context05().unwrap();
    for index in 0..35 {
        let f = v05::freeze_delivery(
            context.clone(),
            format!("partial{index}"),
            v05::DeliveryPurpose::Sync,
            0,
            2,
            2,
            10000,
            vec![
                v05::DeliveryUnit {
                    index: 0,
                    through: Some(1),
                    changes: vec![change(&format!("row{index}"), 1)],
                },
                v05::DeliveryUnit {
                    index: 1,
                    through: Some(2),
                    changes: vec![],
                },
            ],
            1,
        )
        .unwrap();
        let mut q = DeliveryQueue::new(100000, 4);
        q.receive(&f.header, &f.parts[..1], &context, 1).unwrap();
        c.apply_next_delivery05(&mut q, 1).unwrap().unwrap();
        assert!(c.active_delivery_plans05().unwrap().len() <= 1);
    }
    assert_eq!(c.active_delivery_plans05().unwrap().len(), 1);
    assert_eq!(c.store_status05().unwrap().cursor, Some(1));
    assert_eq!(
        c.read_sql("SELECT COUNT(*) AS n FROM axton_delivery_key", &[])
            .unwrap()[0]["n"],
        1
    );
    assert!(c.cleanup_delivery05(10000).unwrap().is_empty());
    assert_eq!(
        c.read_sql("SELECT COUNT(*) AS n FROM axton_delivery_key", &[])
            .unwrap()[0]["n"],
        0
    );
    let repair = plan(context.clone(), "fresh-prefix", 1, 2, false);
    let mut q = DeliveryQueue::new(100000, 4);
    q.receive(&repair.header, &repair.parts, &context, 10001)
        .unwrap_err();
    // Repair must be newly frozen, rather than reviving an expired transfer.
    let repair = plan(context.clone(), "fresh-prefix", 1, 2, false);
    q.receive(&repair.header, &repair.parts, &context, 2)
        .unwrap();
    c.apply_next_delivery05(&mut q, 2).unwrap().unwrap();
    assert_eq!(c.store_status05().unwrap().cursor, Some(2));
}

#[test]
fn earlier_missing_prefix_can_replace_near_capacity_payload() {
    let d = tempfile::tempdir().unwrap();
    let mut c = client(&d.path().join("db"));
    c.initialize_stream05(0).unwrap();
    let context = c.request_context05().unwrap();
    let later = plan(context.clone(), "future", 2, 3, false);
    let early = plan(context.clone(), "repair", 0, 2, false);
    let budget = serde_json::to_vec(&later.parts[0]).unwrap().len()
        + serde_json::to_vec(&early.parts[0]).unwrap().len()
        - 1;
    let mut q = DeliveryQueue::with_limits(100000, 4, budget);
    q.receive(&later.header, &later.parts, &context, 1).unwrap();
    q.receive(&early.header, &early.parts, &context, 1).unwrap();
    c.apply_next_delivery05(&mut q, 1).unwrap().unwrap();
    assert_eq!(c.store_status05().unwrap().cursor, Some(2));
}

#[test]
#[ignore]
fn crash_spill_has_no_named_remnants_child() {
    let dir = std::env::var("AXTON_TASK5_SPOOL_DIR").unwrap();
    let context = v05::RequestContext {
        protocol: 5,
        store_id: "spool".into(),
        stream: "User:u".into(),
        materialization: "m".into(),
    };
    let mut value = change("a", 1);
    if let v05::AuthorityChange::Record { state, .. } = &mut value {
        state["text"] = serde_json::json!("x".repeat(100000));
    }
    let f = v05::freeze_delivery(
        context.clone(),
        "spilled".into(),
        v05::DeliveryPurpose::Sync,
        0,
        1,
        1,
        10000,
        vec![v05::DeliveryUnit {
            index: 0,
            through: Some(1),
            changes: vec![value],
        }],
        1,
    )
    .unwrap();
    let mut q = DeliveryQueue::new(10000, 4);
    q.receive(&f.header, &f.parts, &context, 1).unwrap();
    assert_eq!(std::fs::read_dir(dir).unwrap().count(), 0);
    std::process::exit(74);
}

#[test]
fn process_crash_spill_reclaims_without_sweeping_other_clients() {
    let d = tempfile::tempdir().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "crash_spill_has_no_named_remnants_child",
        ])
        .env("TMPDIR", d.path())
        .env("AXTON_TASK5_SPOOL_DIR", d.path())
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(74));
    assert_eq!(std::fs::read_dir(d.path()).unwrap().count(), 0);
}

#[test]
fn offline_needs_never_dispatches_owned_http_and_reconnect_cancels_old_owner() {
    use axton_client::{
        runtime::{Event, HttpRoute, Operation},
        sync05::{Control, StoreReport},
    };
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("db");
    let mut c = client(&path);
    c.initialize_stream05(0).unwrap();
    drop(c);
    let mut s: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap();
    s["models"][0]["fields"].as_array_mut().unwrap().push(serde_json::json!({"name":"extra","type":{"kind":"scalar","name":"string"},"nullable":true}));
    let mut c = Client::open05(
        SqliteStore::open(&path).unwrap(),
        Schema::from_value(s).unwrap(),
        "User:u",
    )
    .unwrap();
    let pending = c.pending_schema05().unwrap().unwrap();
    let mut control = Control::new(c.store_status05().unwrap());
    control
        .report(
            StoreReport::Needs {
                status: None,
                schema: Some(pending.clone()),
                settlements: vec![],
            },
            1,
        )
        .unwrap();
    assert!(!control.events().iter().any(|e| matches!(
        e,
        Event::Effect {
            operation: Operation::Http {
                route: HttpRoute::Materialize,
                ..
            },
            ..
        }
    )));
    control.connect().unwrap();
    control.events();
    control
        .report(
            StoreReport::Needs {
                status: None,
                schema: Some(pending),
                settlements: vec![],
            },
            1,
        )
        .unwrap();
    let owner = control
        .events()
        .into_iter()
        .find_map(|e| match e {
            Event::Effect {
                effect_id,
                operation:
                    Operation::Http {
                        route: HttpRoute::Materialize,
                        ..
                    },
            } => Some(effect_id),
            _ => None,
        })
        .unwrap();
    control.connect().unwrap();
    assert!(
        control
            .events()
            .iter()
            .any(|e| matches!(e,Event::CancelEffect{effect_id} if *effect_id==owner))
    );
}

#[test]
fn repair_continuation_can_replace_near_capacity_future_payload() {
    let d = tempfile::tempdir().unwrap();
    let mut c = client(&d.path().join("db"));
    c.initialize_stream05(0).unwrap();
    let context = c.request_context05().unwrap();
    let future = plan(context.clone(), "future", 2, 3, false);
    let repair = fragmented(context.clone(), false);
    let budget = serde_json::to_vec(&future.parts[0]).unwrap().len()
        + repair
            .parts
            .iter()
            .map(|p| serde_json::to_vec(p).unwrap().len())
            .sum::<usize>()
        - 1;
    let mut q = DeliveryQueue::with_limits(100000, 4, budget);
    q.receive(&future.header, &future.parts, &context, 1)
        .unwrap();
    q.receive(&repair.header, &repair.parts[..1], &context, 1)
        .unwrap();
    assert!(c.apply_next_delivery05(&mut q, 1).unwrap().is_none());
    assert_eq!(c.store_status05().unwrap().cursor, Some(0));
    q.receive(&repair.header, &repair.parts[1..], &context, 1)
        .unwrap();
    c.apply_next_delivery05(&mut q, 1).unwrap().unwrap();
    assert_eq!(c.store_status05().unwrap().cursor, Some(2));
}

#[test]
fn background_network_failures_keep_status_during_retry_and_auth_refresh() {
    use axton_client::{
        runtime::{Diagnostic, EffectError, EffectOutcome, Event},
        sync05::Control,
    };
    let d = tempfile::tempdir().unwrap();
    let mut c = client(&d.path().join("db"));
    for status in [503, 401] {
        let mut control = Control::new(c.store_status05().unwrap());
        control.set_refresh_auth(true);
        control.connect().unwrap();
        let id = control
            .events()
            .into_iter()
            .find_map(|e| match e {
                Event::Effect { effect_id, .. } => Some(effect_id),
                _ => None,
            })
            .unwrap();
        control
            .receive(
                &id,
                EffectOutcome {
                    ok: false,
                    value: None,
                    error: Some(EffectError {
                        message: "network failed".into(),
                        status: Some(status),
                        refusal: None,
                        retry: false,
                    }),
                },
                1,
            )
            .unwrap();
        assert!(control.events().iter().any(|e|matches!(e,Event::Report{diagnostic:Diagnostic::Error{status:Some(value),..}} if *value==status)));
    }
}

#[test]
fn settlement_progress_wakes_uplink_once_and_waiting_needs_stays_idle() {
    use axton_client::sync05::{Control, StoreCommand, StoreReport};
    let d = tempfile::tempdir().unwrap();
    let mut c = client(&d.path().join("db"));
    c.initialize_stream05(0).unwrap();
    let status = c.store_status05().unwrap();
    let mut control = Control::new(status.clone());
    control.connect().unwrap();
    control.events();
    control.report(StoreReport::Frozen(None), 1).unwrap();
    control.jobs();
    control
        .report(
            StoreReport::Needs {
                status: None,
                schema: None,
                settlements: vec![],
            },
            1,
        )
        .unwrap();
    assert!(
        control.jobs().is_empty(),
        "waiting settlement must not spin Freeze/Needs"
    );
    control
        .report(
            StoreReport::Needs {
                status: Some(status),
                schema: None,
                settlements: vec![],
            },
            1,
        )
        .unwrap();
    let jobs = control.jobs();
    assert_eq!(jobs.len(), 1);
    assert!(
        matches!(&jobs[0], StoreCommand::Guarded { command, .. } if matches!(**command, StoreCommand::Freeze))
    );
    control
        .report(
            StoreReport::Needs {
                status: None,
                schema: None,
                settlements: vec![],
            },
            1,
        )
        .unwrap();
    assert!(control.jobs().is_empty());
}
