//! Scope labels remain server-only; the production Pull path delivers identity releases.
use axton_server::host::{RecordRef, ScopeIntent};
use axton_sim::{Action, Sim, schema::entry_key};

fn add(scope: &str, tags: &[&str]) -> ScopeIntent {
    ScopeIntent::Add {
        scope: scope.into(),
        record: RecordRef {
            model: "Entry".into(),
            identity: serde_json::json!({"id":"a"}),
        },
        tags: tags.iter().map(|v| v.to_string()).collect(),
    }
}
fn withdraw_matching(scope: &str, tag: &str) -> ScopeIntent {
    ScopeIntent::Select {
        scope: scope.into(),
        model: None,
        predicate: serde_json::from_value(serde_json::json!({"tags":{"any":[tag]}})).unwrap(),
        action: axton_server::scope_members::SelectionAction::Remove,
    }
}
fn select(scope: &str, predicate: serde_json::Value) -> ScopeIntent {
    ScopeIntent::Select {
        scope: scope.into(),
        model: None,
        predicate: serde_json::from_value(predicate).unwrap(),
        action: axton_server::scope_members::SelectionAction::Remove,
    }
}
fn detach(scope: &str, tag: &str) -> ScopeIntent {
    ScopeIntent::DetachTags {
        scope: scope.into(),
        tags: vec![tag.into()],
    }
}
fn tags(sim: &mut Sim, intents: Vec<ScopeIntent>) {
    sim.apply(Action::ScopeTags { intents }).unwrap();
}
fn seeded(seed: u64, scopes: &[&str]) -> Sim {
    let mut sim = Sim::new(seed, 1);
    for c in scopes {
        sim.apply(Action::Subscribe {
            client: 0,
            scope: c.to_string(),
        })
        .unwrap();
    }
    sim.apply(Action::Declare {
        key: "Entry:a".into(),
        touch: Some(Some("stored".into())),
        memberships: vec![(scopes[0].into(), true)],
    })
    .unwrap();
    sim.settle();
    assert_eq!(sim.read_text(0, &entry_key("a")).as_deref(), Some("stored"));
    sim
}

#[test]
fn removing_x_releases_the_whole_member_including_y_and_survives_offline_reopen() {
    let mut sim = seeded(9100, &["u"]);
    tags(&mut sim, vec![add("u", &["x", "y"])]);
    tags(&mut sim, vec![withdraw_matching("u", "x")]);
    assert!(sim.host.stored_memberships(&entry_key("a")).is_empty());
    sim.settle();
    assert_eq!(sim.read_text(0, &entry_key("a")), None);
    sim.apply(Action::Crash { client: 0 }).unwrap();
    sim.apply(Action::Restart { client: 0 }).unwrap();
    assert_eq!(sim.read_text(0, &entry_key("a")), None);
}

#[test]
fn second_scope_hold_preserves_content_until_its_own_release() {
    let mut sim = seeded(9101, &["u", "v"]);
    tags(&mut sim, vec![add("u", &["x"]), add("v", &["y"])]);
    sim.settle();
    tags(&mut sim, vec![withdraw_matching("u", "x")]);
    sim.settle();
    assert_eq!(sim.read_text(0, &entry_key("a")).as_deref(), Some("stored"));
    tags(&mut sim, vec![withdraw_matching("v", "y")]);
    sim.settle();
    assert_eq!(sim.read_text(0, &entry_key("a")), None);
}

#[test]
fn compacted_final_upsert_replaces_hidden_tag_history_then_final_remove_evicts() {
    let mut sim = seeded(9102, &["u"]);
    // The host keeps one final position per identity, exactly like production compaction.
    sim.apply(Action::Crash { client: 0 }).unwrap();
    tags(&mut sim, vec![add("u", &["x"])]);
    tags(&mut sim, vec![withdraw_matching("u", "x")]);
    tags(&mut sim, vec![add("u", &["y"])]);
    sim.apply(Action::Restart { client: 0 }).unwrap();
    sim.settle();
    assert_eq!(sim.read_text(0, &entry_key("a")).as_deref(), Some("stored"));
    tags(&mut sim, vec![withdraw_matching("u", "y")]);
    sim.settle();
    assert_eq!(sim.read_text(0, &entry_key("a")), None);
}

#[test]
fn reproducible_tag_histories_converge_after_reordered_delivery_and_restart() {
    for seed in 9200..9224 {
        let mut sim = seeded(seed, &["u", "v"]);
        let mut rng = axton_sim::Rng::new(seed);
        for step in 0..80 {
            let scope = if rng.chance(1, 2) { "u" } else { "v" };
            let tag = if rng.chance(1, 2) { "x" } else { "y" };
            let action = match rng.below(10) {
                0..=2 => Action::ScopeTags {
                    intents: vec![add(scope, &[tag])],
                },
                3 => Action::ScopeTags {
                    intents: vec![withdraw_matching(scope, tag)],
                },
                8 => Action::ScopeTags {
                    intents: vec![detach(scope, tag)],
                },
                9 => Action::ScopeTags {
                    intents: vec![select(scope, serde_json::json!({"tags":{"only":[]}}))],
                },
                4 => Action::Pull { client: 0 },
                5 => Action::Deliver,
                6 => Action::Duplicate,
                _ => Action::Hold,
            };
            sim.apply(action.clone())
                .unwrap_or_else(|e| panic!("seed {seed} step {step} {action:?}: {e}"));
            sim.check()
                .unwrap_or_else(|e| panic!("seed {seed} step {step}: {e}"));
            if step % 20 == 19 {
                sim.apply(Action::Crash { client: 0 }).unwrap();
                sim.apply(Action::Restart { client: 0 }).unwrap();
                sim.settle();
                let expected = !sim.host.stored_memberships(&entry_key("a")).is_empty();
                assert_eq!(
                    sim.read_text(0, &entry_key("a")).is_some(),
                    expected,
                    "seed {seed} step {step}"
                );
            }
        }
    }
}

#[test]
fn delayed_old_scope_page_cannot_restore_a_released_replica() {
    let mut sim = seeded(9103, &["u"]);
    sim.apply(Action::ServerChange {
        key: "Entry:a".into(),
        text: Some("old page".into()),
        scopes: vec!["u".into()],
    })
    .unwrap();
    sim.apply(Action::Pull { client: 0 }).unwrap();
    sim.apply(Action::Deliver).unwrap(); // Snapshot the upsert, leave its response queued.
    tags(
        &mut sim,
        vec![
            add("u", &["x"]),
            select("u", serde_json::json!({"tags":{"only":["x"]}})),
            detach("u", "x"),
        ],
    );
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
    tags(&mut sim, vec![add("u", &["x"]), add("v", &["x"])]);
    sim.settle();
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: axton_sim::MutationSpec::Edit {
            id: "a".into(),
            text: "pending".into(),
        },
    })
    .unwrap();
    tags(
        &mut sim,
        vec![
            add("u", &["x"]),
            select("u", serde_json::json!({"tags":{"only":["x"]}})),
            detach("u", "x"),
        ],
    );
    sim.apply(Action::Pull { client: 0 }).unwrap();
    sim.apply(Action::Deliver).unwrap();
    sim.apply(Action::Deliver).unwrap();
    assert_eq!(
        sim.read_text(0, &entry_key("a")).as_deref(),
        Some("pending"),
        "second hold preserves the optimistic edit"
    );
    tags(
        &mut sim,
        vec![
            select("v", serde_json::json!({"tags":{"only":["x"]}})),
            detach("v", "x"),
        ],
    );
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
    tags(&mut local, vec![add("u", &["x"]), add("v", &["x"])]);
    local.settle();
    local
        .apply(Action::Direct {
            client: 0,
            key: "Entry:a".into(),
            text: "device".into(),
        })
        .unwrap();
    tags(
        &mut local,
        vec![
            add("u", &["x"]),
            select("u", serde_json::json!({"tags":{"only":["x"]}})),
            detach("u", "x"),
        ],
    );
    local.settle();
    assert_eq!(
        local.read_text(0, &entry_key("a")).as_deref(),
        Some("device"),
        "second hold preserves the direct patch"
    );
    tags(
        &mut local,
        vec![
            select("v", serde_json::json!({"tags":{"only":["x"]}})),
            detach("v", "x"),
        ],
    );
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
        scope: "u".into(),
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
        serde_json::json!(["scope-membership-v1"])
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
    sim.settle(); // The enrolled Scope delivers first.
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
        scope: "u".into(),
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
    tags(&mut sim, vec![add("u", &["x"])]);
    tags(&mut sim, vec![withdraw_matching("u", "x")]);
    sim.settle();
    assert_eq!(sim.read_text(0, &entry_key("a")).as_deref(), Some("local"));
    sim.apply(Action::Crash { client: 0 }).unwrap();
    sim.apply(Action::Restart { client: 0 }).unwrap();
    assert_eq!(sim.read_text(0, &entry_key("a")).as_deref(), Some("local"));
}

#[test]
fn reproducible_enrolled_load_histories_mix_tags_touches_delays_and_offline_replay() {
    use axton_client::{LoadOptions, LoadWorker};
    use axton_core::{LoadBatchRequest, LoadBatchResponse};
    for seed in 9300..9312 {
        let schema = axton_sim::schema::enrollment_schema();
        let mut config = axton_sim::schema::config();
        config.schema = schema.clone();
        let mut sim = Sim::new_with_schema(seed, 1, schema);
        for scope in ["u", "v"] {
            sim.apply(Action::Subscribe {
                client: 0,
                scope: scope.into(),
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
                let scope = if rng.chance(1, 2) { "u" } else { "v" };
                match rng.below(7) {
                    0 => tags(&mut sim, vec![add(scope, &["x", "y"])]),
                    1 => tags(&mut sim, vec![withdraw_matching(scope, "x")]),
                    5 => tags(&mut sim, vec![detach(scope, "x")]),
                    6 => tags(
                        &mut sim,
                        vec![select(
                            scope,
                            serde_json::json!({"tags":{"only":["x","y"]}}),
                        )],
                    ),
                    2 => tags(
                        &mut sim,
                        vec![ScopeIntent::Remove {
                            scope: scope.into(),
                            record: RecordRef {
                                model: "Entry".into(),
                                identity: serde_json::json!({"id":"a"}),
                            },
                        }],
                    ),
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
fn exact_only_x_cleanup_preserves_a_c_through_duplicate_reordered_delivery_and_reopen() {
    let mut sim = Sim::new(9400, 1);
    sim.apply(Action::Subscribe {
        client: 0,
        scope: "u".into(),
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
    let added = |id: &str, labels: &[&str]| ScopeIntent::Add {
        scope: "u".into(),
        record: RecordRef {
            model: "Entry".into(),
            identity: serde_json::json!({"id":id}),
        },
        tags: labels.iter().map(|label| (*label).into()).collect(),
    };
    tags(
        &mut sim,
        vec![
            added("A", &["X", "Y"]),
            added("B", &["X"]),
            added("C", &["Y"]),
        ],
    );
    let head = sim.host.head("u");
    tags(
        &mut sim,
        vec![
            select("u", serde_json::json!({"tags":{"only":["X"]}})),
            detach("u", "X"),
        ],
    );
    assert_eq!(sim.host.head("u"), head + 1);
    assert!(sim.host.stored_memberships(&entry_key("B")).is_empty());
    for id in ["A", "C"] {
        assert_eq!(
            sim.host.stored_memberships(&entry_key(id)),
            vec!["u".to_string()]
        );
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
