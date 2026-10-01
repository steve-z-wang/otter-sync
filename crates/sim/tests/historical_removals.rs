//! Compatibility fixtures preserve previously saved removal evidence.
//! These events are restored directly; fresh Stream settlement never withdraws.
use axton_server::host::{RecordRef, StreamIntent};
use axton_sim::{Action, Sim, schema::entry_key};
use serde_json::json;
enum Fixture {
    Track(String),
    SavedRemoval(String),
}
fn track(stream: &str) -> Fixture {
    Fixture::Track(stream.into())
}
fn release(stream: &str) -> Fixture {
    Fixture::SavedRemoval(stream.into())
}
fn effects(sim: &mut Sim, effects: Vec<Fixture>) {
    for effect in effects {
        match effect {
            Fixture::Track(stream) => sim
                .apply(Action::StreamDeclarations {
                    intents: vec![StreamIntent::Track {
                        stream,
                        record: RecordRef {
                            model: "Entry".into(),
                            identity: json!({"id":"a"}),
                        },
                    }],
                })
                .unwrap(),
            Fixture::SavedRemoval(stream) => sim
                .apply(Action::RestoreHistoricalRemoval {
                    key: "Entry:a".into(),
                    stream,
                })
                .unwrap(),
        }
    }
}
fn seeded(seed: u64, streams: &[&str]) -> Sim {
    let mut sim = Sim::new(seed, 1);
    for stream in streams {
        sim.apply(Action::Subscribe {
            client: 0,
            stream: stream.to_string(),
        })
        .unwrap();
    }
    sim.apply(Action::Declare {
        key: "Entry:a".into(),
        touch: Some(Some("stored".into())),
        memberships: vec![(streams[0].into(), true)],
    })
    .unwrap();
    sim.settle();
    assert_eq!(sim.read_text(0, &entry_key("a")).as_deref(), Some("stored"));
    sim
}
#[test]
fn second_stream_hold_preserves_content_until_its_own_release() {
    let mut sim = seeded(9101, &["u", "v"]);
    effects(&mut sim, vec![track("u"), track("v")]);
    sim.settle();
    effects(&mut sim, vec![release("u")]);
    sim.settle();
    assert_eq!(sim.read_text(0, &entry_key("a")).as_deref(), Some("stored"));
    effects(&mut sim, vec![release("v")]);
    sim.settle();
    assert_eq!(sim.read_text(0, &entry_key("a")), None);
}

#[test]
fn delayed_old_stream_page_cannot_restore_a_released_replica() {
    let mut sim = seeded(9103, &["u"]);
    sim.apply(Action::ServerChange {
        key: "Entry:a".into(),
        text: Some("old page".into()),
        streams: vec!["u".into()],
    })
    .unwrap();
    sim.apply(Action::Pull { client: 0 }).unwrap();
    sim.apply(Action::Deliver).unwrap(); // Snapshot the upsert, leave its response queued.
    effects(&mut sim, vec![release("u")]);
    sim.apply(Action::Pull { client: 0 }).unwrap();
    sim.apply(Action::Swap { i: 0, j: 1 }).unwrap();
    sim.apply(Action::Deliver).unwrap();
    sim.apply(Action::Swap { i: 0, j: 1 }).unwrap();
    sim.apply(Action::Deliver).unwrap(); // New removal first.
    assert_eq!(sim.read_text(0, &entry_key("a")), None);
    sim.apply(Action::Deliver).unwrap(); // Older page cannot resurrect.
    assert_eq!(sim.read_text(0, &entry_key("a")), None);
}

#[test]
fn pending_edit_and_device_local_patch_survive_release_and_offline_reopen() {
    let mut sim = seeded(9104, &["u", "v"]);
    effects(&mut sim, vec![track("u"), track("v")]);
    sim.settle();
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: axton_sim::MutationSpec::Edit {
            id: "a".into(),
            text: "pending".into(),
        },
    })
    .unwrap();
    effects(&mut sim, vec![release("u")]);
    sim.apply(Action::Pull { client: 0 }).unwrap();
    sim.apply(Action::Deliver).unwrap();
    sim.apply(Action::Deliver).unwrap();
    assert_eq!(
        sim.read_text(0, &entry_key("a")).as_deref(),
        Some("pending"),
        "second hold preserves the optimistic edit"
    );
    effects(&mut sim, vec![release("v")]);
    sim.apply(Action::Pull { client: 0 }).unwrap();
    sim.apply(Action::Deliver).unwrap();
    sim.apply(Action::Deliver).unwrap();
    assert_eq!(
        sim.read_text(0, &entry_key("a")),
        None,
        "pending update cannot render against an absent base"
    );
    sim.apply(Action::Crash { client: 0 }).unwrap();
    sim.apply(Action::Restart { client: 0 }).unwrap();
    assert_eq!(
        sim.read_text(0, &entry_key("a")),
        None,
        "pending update cannot render against an absent base"
    );
    assert_eq!(sim.client(0).pending_count().unwrap(), 1);
    let submitted = sim
        .client(0)
        .read_sql(
            "SELECT \"values\" FROM axton_mutation_operation WHERE model='Entry'",
            &[],
        )
        .unwrap();
    assert!(
        submitted.iter().any(|row| row["values"]
            .as_str()
            .is_some_and(|v| v.contains("pending"))),
        "submitted words survived reopen"
    );
    sim.settle();
    assert_eq!(
        sim.client(0).pending_count().unwrap(),
        0,
        "receipt still settles released pending work"
    );
    assert_eq!(
        sim.read_text(0, &entry_key("a")),
        None,
        "receipt does not repopulate the released base"
    );

    let mut local = seeded(9105, &["u", "v"]);
    effects(&mut local, vec![track("u"), track("v")]);
    local.settle();
    local
        .apply(Action::Direct {
            client: 0,
            key: "Entry:a".into(),
            text: "device".into(),
        })
        .unwrap();
    effects(&mut local, vec![release("u")]);
    local.settle();
    assert_eq!(
        local.read_text(0, &entry_key("a")).as_deref(),
        Some("device"),
        "second hold preserves the direct patch"
    );
    effects(&mut local, vec![release("v")]);
    local.settle();
    assert_eq!(
        local.read_text(0, &entry_key("a")),
        None,
        "direct patch does not retain unrelated replica fields"
    );
    let layers = local
        .client(0)
        .read_sql(
            "SELECT operations FROM axton_local_replica_layer WHERE model='Entry'",
            &[],
        )
        .unwrap();
    assert!(
        layers.iter().any(|row| row["operations"]
            .as_str()
            .is_some_and(|v| v.contains("device"))),
        "direct patch retained as device-local work"
    );
}

#[test]
fn delayed_enrolled_native_load_claim_and_replay_cannot_reenroll_after_removal() {
    use axton_client::{LoadOptions, LoadWorker};
    use axton_core::{LoadBatchRequest, LoadBatchResponse};
    let schema = axton_sim::schema::enrollment_schema();
    let mut config = axton_sim::schema::config();
    config.schema = schema.clone();
    let mut sim = Sim::new_with_schema(9106, 1, schema);
    sim.apply(Action::Subscribe {
        client: 0,
        stream: "u".into(),
    })
    .unwrap();
    sim.apply(Action::Declare {
        key: "Entry:a".into(),
        touch: Some(Some("loaded".into())),
        memberships: vec![],
    })
    .unwrap();
    // Use the production worker's request bytes; capability gaps must stay visible.
    sim.client(0)
        .start_load(
            "EnrolledEntries",
            1,
            &serde_json::json!({"channel":"u"}),
            LoadOptions::default(),
        )
        .unwrap();
    let mut worker = LoadWorker::default();
    worker.wake();
    let dispatch = worker
        .dispatch(sim.client(0), 0, 9106)
        .unwrap()
        .dispatch
        .unwrap();
    let wire: serde_json::Value = serde_json::from_str(&dispatch.body).unwrap();
    assert_eq!(
        wire["capabilities"],
        serde_json::json!(["stream-membership-v1"])
    );
    assert_eq!(wire["loads"][0]["args"], serde_json::json!({"channel":"u"}));
    let request = LoadBatchRequest::decode_envelope(dispatch.body.as_bytes()).unwrap();
    let held = sim
        .host
        .native_load(&config, "owner", dispatch.body.as_bytes())
        .unwrap();
    assert_eq!(sim.host.native_load_calls(), 1);
    let answer: serde_json::Value = serde_json::from_str(&held).unwrap();
    assert_eq!(
        answer["loads"][0]["outcome"]["status"], "succeeded",
        "{answer}"
    );
    sim.settle(); // The enrolled Stream delivers first.
    assert_eq!(sim.read_text(0, &entry_key("a")).as_deref(), Some("loaded"));
    sim.apply(Action::Declare {
        key: "Entry:a".into(),
        touch: None,
        memberships: vec![("u".into(), false)],
    })
    .unwrap();
    sim.settle();
    assert_eq!(sim.read_text(0, &entry_key("a")), None);
    let reply = LoadBatchResponse::decode(held.as_bytes(), &request)
        .unwrap()
        .remove(0);
    sim.client(0)
        .store_load_page(&dispatch.pages[0].fence, reply)
        .unwrap();
    assert_eq!(
        sim.read_text(0, &entry_key("a")),
        None,
        "old enrolled claim cannot revive released content"
    );
    // Durable server call replay returns the original claim, without executing add again.
    let replay = sim
        .host
        .native_load(&config, "owner", dispatch.body.as_bytes())
        .unwrap();
    assert_eq!(replay, held);
    assert_eq!(sim.host.native_load_calls(), 1);
    assert!(sim.host.stored_memberships(&entry_key("a")).is_empty());
    sim.apply(Action::Crash { client: 0 }).unwrap();
    sim.apply(Action::Restart { client: 0 }).unwrap();
    assert_eq!(sim.read_text(0, &entry_key("a")), None);
}

#[test]
fn device_local_create_is_not_deleted_by_matching_replica_release() {
    use axton_client::{Operation, OperationKind};
    let mut sim = Sim::new(9107, 1);
    sim.apply(Action::Subscribe {
        client: 0,
        stream: "u".into(),
    })
    .unwrap();
    sim.client(0)
        .transaction(|tx| {
            tx.direct(Operation {
                model: "Entry".into(),
                op: OperationKind::Create,
                identity: serde_json::json!({"id":"a"}),
                values: Some(serde_json::json!({"text":"local","note":null})),
            })
        })
        .unwrap();
    effects(&mut sim, vec![track("u")]);
    effects(&mut sim, vec![release("u")]);
    sim.settle();
    assert_eq!(sim.read_text(0, &entry_key("a")).as_deref(), Some("local"));
    sim.apply(Action::Crash { client: 0 }).unwrap();
    sim.apply(Action::Restart { client: 0 }).unwrap();
    assert_eq!(sim.read_text(0, &entry_key("a")).as_deref(), Some("local"));
}

#[test]
fn compacted_saved_removal_then_fresh_tracking_survives_offline_reopen() {
    let mut sim = seeded(9108, &["u"]);
    sim.apply(Action::Crash { client: 0 }).unwrap();
    effects(&mut sim, vec![release("u"), track("u")]);
    sim.apply(Action::Restart { client: 0 }).unwrap();
    sim.settle();
    assert_eq!(sim.read_text(0, &entry_key("a")).as_deref(), Some("stored"));
    effects(&mut sim, vec![release("u")]);
    sim.settle();
    sim.apply(Action::Crash { client: 0 }).unwrap();
    sim.apply(Action::Restart { client: 0 }).unwrap();
    assert_eq!(sim.read_text(0, &entry_key("a")), None);
}

#[test]
fn generated_saved_removals_and_tracking_preserve_enrolled_load_replay_through_restart() {
    use axton_client::{LoadOptions, LoadWorker};
    use axton_core::{LoadBatchRequest, LoadBatchResponse};
    for seed in 9300..9312 {
        let schema = axton_sim::schema::enrollment_schema();
        let mut config = axton_sim::schema::config();
        config.schema = schema.clone();
        let mut sim = Sim::new_with_schema(seed, 1, schema);
        for stream in ["u", "v"] {
            sim.apply(Action::Subscribe {
                client: 0,
                stream: stream.into(),
            })
            .unwrap();
        }
        let mut rng = axton_sim::Rng::new(seed);
        for cycle in 0..8 {
            sim.apply(Action::Declare {
                key: "Entry:a".into(),
                touch: Some(Some(format!("seed{seed}-cycle{cycle}"))),
                memberships: vec![],
            })
            .unwrap();
            sim.client(0)
                .start_load(
                    "EnrolledEntries",
                    1,
                    &serde_json::json!({"channel":"u"}),
                    LoadOptions::default(),
                )
                .unwrap();
            let mut worker = LoadWorker::default();
            worker.wake();
            let mut dispatched = worker
                .dispatch(sim.client(0), 0, seed)
                .unwrap()
                .dispatch
                .unwrap();
            let held = sim
                .host
                .native_load(&config, "owner", dispatched.body.as_bytes())
                .unwrap();
            let handlers = sim.host.native_load_calls();
            sim.settle();
            for step in 0..6 {
                let stream = if rng.chance(1, 2) { "u" } else { "v" };
                match rng.below(7) {
                    0 | 5 => effects(&mut sim, vec![track(stream)]),
                    1 | 2 | 6 => effects(&mut sim, vec![release(stream)]),
                    _ => sim
                        .apply(Action::Declare {
                            key: "Entry:a".into(),
                            touch: Some(if rng.chance(1, 4) {
                                None
                            } else {
                                Some(format!("t{cycle}-{step}"))
                            }),
                            memberships: vec![],
                        })
                        .unwrap(),
                }
                sim.apply(Action::Pull { client: 0 }).unwrap();
                if rng.chance(1, 2) {
                    sim.apply(Action::Duplicate).unwrap();
                }
                if rng.chance(1, 3) {
                    sim.apply(Action::Drop).unwrap();
                }
                sim.drain();
            }
            sim.settle();
            if rng.chance(1, 2) {
                sim.apply(Action::Crash { client: 0 }).unwrap();
                sim.apply(Action::Restart { client: 0 }).unwrap();
                let mut resumed = LoadWorker::default();
                resumed.wake();
                dispatched = resumed
                    .dispatch(sim.client(0), 0, seed)
                    .unwrap()
                    .dispatch
                    .unwrap();
            }
            let replay = sim
                .host
                .native_load(&config, "owner", dispatched.body.as_bytes())
                .unwrap();
            assert_eq!(replay, held, "seed{seed} cycle{cycle}: saved call replay");
            assert_eq!(
                sim.host.native_load_calls(),
                handlers,
                "seed{seed} cycle{cycle}: replay does not re-enroll"
            );
            let request = LoadBatchRequest::decode_envelope(dispatched.body.as_bytes()).unwrap();
            let reply = LoadBatchResponse::decode(replay.as_bytes(), &request)
                .unwrap()
                .remove(0);
            sim.client(0)
                .store_load_page(&dispatched.pages[0].fence, reply)
                .unwrap();
            let expected = if sim.host.stored_memberships(&entry_key("a")).is_empty() {
                None
            } else {
                sim.host
                    .state(&entry_key("a"))
                    .map(|s| s["text"].as_str().unwrap().to_string())
            };
            assert_eq!(
                sim.read_text(0, &entry_key("a")),
                expected,
                "seed{seed} cycle{cycle}: older enrolled authority follows final memberships/current content"
            );
            sim.check()
                .unwrap_or_else(|e| panic!("seed{seed} cycle{cycle}: {e}"));
        }
    }
}

#[test]
fn one_saved_removal_preserves_other_rows_through_duplicate_reordered_delivery_and_reopen() {
    let mut sim = Sim::new(9400, 1);
    sim.apply(Action::Subscribe {
        client: 0,
        stream: "u".into(),
    })
    .unwrap();
    for id in ["A", "B", "C"] {
        sim.apply(Action::Declare {
            key: format!("Entry:{id}"),
            touch: Some(Some(format!("{id} text"))),
            memberships: vec![("u".into(), true)],
        })
        .unwrap();
    }
    sim.settle();
    let head = sim.host.head("u");
    sim.apply(Action::RestoreHistoricalRemoval {
        key: "Entry:B".into(),
        stream: "u".into(),
    })
    .unwrap();
    assert_eq!(sim.host.head("u"), head + 1);
    assert!(sim.host.stored_memberships(&entry_key("B")).is_empty());
    for id in ["A", "C"] {
        assert_eq!(sim.host.stored_memberships(&entry_key(id)), ["u"]);
    }
    sim.apply(Action::Pull { client: 0 }).unwrap();
    sim.apply(Action::Duplicate).unwrap();
    sim.apply(Action::Hold).unwrap();
    sim.settle();
    sim.apply(Action::Crash { client: 0 }).unwrap();
    sim.apply(Action::Restart { client: 0 }).unwrap();
    sim.settle();
    assert_eq!(sim.read_text(0, &entry_key("B")), None);
    for id in ["A", "C"] {
        assert_eq!(sim.read_text(0, &entry_key(id)), Some(format!("{id} text")));
    }
    sim.check().unwrap();
}
