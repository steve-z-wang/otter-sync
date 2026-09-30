//! Channel labels remain server-only; the production Pull path delivers identity releases.
use axton_server::host::{ChannelIntent, RecordRef};
use axton_sim::{Action, Sim, schema::entry_key};

fn add(channel: &str, tags: &[&str]) -> ChannelIntent {
    ChannelIntent::Add {
        channel: channel.into(),
        record: RecordRef {
            model: "Entry".into(),
            identity: serde_json::json!({"id":"a"}),
        },
        tags: tags.iter().map(|v| v.to_string()).collect(),
    }
}
fn remove_tag(channel: &str, tag: &str) -> ChannelIntent {
    ChannelIntent::RemoveTag {
        channel: channel.into(),
        tag: tag.into(),
    }
}
fn tags(sim: &mut Sim, intents: Vec<ChannelIntent>) {
    sim.apply(Action::ChannelTags { intents }).unwrap();
}
fn seeded(seed: u64, channels: &[&str]) -> Sim {
    let mut sim = Sim::new(seed, 1);
    for c in channels {
        sim.apply(Action::Subscribe {
            client: 0,
            channel: c.to_string(),
        })
        .unwrap();
    }
    sim.apply(Action::Declare {
        key: "Entry:a".into(),
        touch: Some(Some("stored".into())),
        memberships: vec![(channels[0].into(), true)],
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
    tags(&mut sim, vec![remove_tag("u", "x")]);
    assert!(sim.host.stored_memberships(&entry_key("a")).is_empty());
    sim.settle();
    assert_eq!(sim.read_text(0, &entry_key("a")), None);
    sim.apply(Action::Crash { client: 0 }).unwrap();
    sim.apply(Action::Restart { client: 0 }).unwrap();
    assert_eq!(sim.read_text(0, &entry_key("a")), None);
}

#[test]
fn second_channel_hold_preserves_content_until_its_own_release() {
    let mut sim = seeded(9101, &["u", "v"]);
    tags(&mut sim, vec![add("u", &["x"]), add("v", &["y"])]);
    sim.settle();
    tags(&mut sim, vec![remove_tag("u", "x")]);
    sim.settle();
    assert_eq!(sim.read_text(0, &entry_key("a")).as_deref(), Some("stored"));
    tags(&mut sim, vec![remove_tag("v", "y")]);
    sim.settle();
    assert_eq!(sim.read_text(0, &entry_key("a")), None);
}

#[test]
fn compacted_final_upsert_replaces_hidden_tag_history_then_final_remove_evicts() {
    let mut sim = seeded(9102, &["u"]);
    // The host keeps one final position per identity, exactly like production compaction.
    sim.apply(Action::Crash { client: 0 }).unwrap();
    tags(&mut sim, vec![add("u", &["x"])]);
    tags(&mut sim, vec![remove_tag("u", "x")]);
    tags(&mut sim, vec![add("u", &["y"])]);
    sim.apply(Action::Restart { client: 0 }).unwrap();
    sim.settle();
    assert_eq!(sim.read_text(0, &entry_key("a")).as_deref(), Some("stored"));
    tags(&mut sim, vec![remove_tag("u", "y")]);
    sim.settle();
    assert_eq!(sim.read_text(0, &entry_key("a")), None);
}

#[test]
fn reproducible_tag_histories_converge_after_reordered_delivery_and_restart() {
    for seed in 9200..9224 {
        let mut sim = seeded(seed, &["u", "v"]);
        let mut rng = axton_sim::Rng::new(seed);
        for step in 0..80 {
            let channel = if rng.chance(1, 2) { "u" } else { "v" };
            let tag = if rng.chance(1, 2) { "x" } else { "y" };
            let action = match rng.below(8) {
                0..=2 => Action::ChannelTags {
                    intents: vec![add(channel, &[tag])],
                },
                3 => Action::ChannelTags {
                    intents: vec![remove_tag(channel, tag)],
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
fn delayed_old_channel_page_cannot_restore_a_released_replica() {
    let mut sim = seeded(9103, &["u"]);
    sim.apply(Action::ServerChange {
        key: "Entry:a".into(),
        text: Some("old page".into()),
        channels: vec!["u".into()],
    })
    .unwrap();
    sim.apply(Action::Pull { client: 0 }).unwrap();
    sim.apply(Action::Deliver).unwrap(); // Snapshot the upsert, leave its response queued.
    tags(&mut sim, vec![add("u", &["x"]), remove_tag("u", "x")]);
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
    let mut sim = seeded(9104, &["u"]);
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: axton_sim::MutationSpec::Edit {
            id: "a".into(),
            text: "pending".into(),
        },
    })
    .unwrap();
    tags(&mut sim, vec![add("u", &["x"]), remove_tag("u", "x")]);
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

    let mut local = seeded(9105, &["u"]);
    local
        .apply(Action::Direct {
            client: 0,
            key: "Entry:a".into(),
            text: "device".into(),
        })
        .unwrap();
    tags(&mut local, vec![add("u", &["x"]), remove_tag("u", "x")]);
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
        channel: "u".into(),
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
    sim.settle(); // The enrolled Channel delivers first.
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
        channel: "u".into(),
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
    tags(&mut sim, vec![remove_tag("u", "x")]);
    sim.settle();
    assert_eq!(sim.read_text(0, &entry_key("a")).as_deref(), Some("local"));
    sim.apply(Action::Crash { client: 0 }).unwrap();
    sim.apply(Action::Restart { client: 0 }).unwrap();
    assert_eq!(sim.read_text(0, &entry_key("a")).as_deref(), Some("local"));
}
