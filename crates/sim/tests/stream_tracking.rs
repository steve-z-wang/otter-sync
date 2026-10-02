//! Tracking declarations coalesce; targeted invalidation never enrolls recipients.
use axton_server::host::{RecordRef, StreamIntent};
use axton_sim::{Action, Sim, schema::entry_key};
use serde_json::json;
fn reference() -> RecordRef {
    RecordRef {
        model: "Entry".into(),
        identity: json!({"id":"a"}),
    }
}
fn track(stream: &str) -> StreamIntent {
    StreamIntent::Track {
        stream: stream.into(),
        record: reference(),
    }
}
fn invalidate(streams: Option<&[&str]>) -> StreamIntent {
    StreamIntent::Invalidate {
        streams: streams.map(|s| s.iter().map(|v| v.to_string()).collect()),
        record: reference(),
    }
}
fn declare(sim: &mut Sim, declarations: Vec<StreamIntent>) {
    sim.apply(Action::StreamDeclarations {
        intents: declarations,
    })
    .unwrap();
}
fn seeded(seed: u64) -> Sim {
    let mut sim = Sim::new(seed, 2);
    for (client, stream) in [(0, "A"), (1, "B")] {
        sim.apply(Action::Subscribe {
            client,
            stream: stream.into(),
        })
        .unwrap();
    }
    sim.host
        .transact(
            &[(
                entry_key("a"),
                Some(json!({"id":"a","text":"stored","note":null})),
            )],
            vec![reference()],
            vec![track("A"), track("B")],
        )
        .unwrap();
    sim.settle();
    sim
}
#[test]
fn targeted_invalidation_only_positions_existing_selected_holders() {
    let mut sim = seeded(9100);
    let key = entry_key("a");
    let b = sim.host.head("B");
    declare(&mut sim, vec![invalidate(Some(&["A"]))]);
    assert_eq!(sim.host.stamp(&key), 2);
    assert_eq!(sim.host.head("A"), 2);
    assert_eq!(sim.host.head("B"), b);
    declare(&mut sim, vec![invalidate(Some(&["C"]))]);
    assert_eq!(sim.host.stamp(&key), 3);
    assert_eq!(sim.host.head("C"), 0);
    assert_eq!(sim.host.stored_memberships(&key), ["A", "B"]);
}
#[test]
fn repeated_tracking_preserves_stamps_and_positions_and_new_tracking_inherits_stamp() {
    let mut sim = seeded(9101);
    let key = entry_key("a");
    declare(&mut sim, vec![track("A"), track("A"), track("C")]);
    assert_eq!(sim.host.stamp(&key), 1);
    assert_eq!(sim.host.head("A"), 1);
    assert_eq!(sim.host.head("C"), 1);
    assert_eq!(sim.host.stream_stamp("C", &key), Some(1));
}
#[test]
fn declaration_order_does_not_change_final_tracking_or_invalidation() {
    for reverse in [false, true] {
        let mut sim = seeded(9102);
        let mut declarations = vec![
            invalidate(Some(&["A"])),
            track("C"),
            invalidate(Some(&["C", "A"])),
        ];
        if reverse {
            declarations.reverse();
        }
        declare(&mut sim, declarations);
        assert_eq!(sim.host.stamp(&entry_key("a")), 2);
        assert_eq!(
            (sim.host.head("A"), sim.host.head("B"), sim.host.head("C")),
            (2, 1, 1)
        );
        assert_eq!(
            sim.host.stored_memberships(&entry_key("a")),
            ["A", "B", "C"]
        );
    }
}
#[test]
fn global_invalidation_dominates_selected_and_empty_selection_is_a_noop() {
    let mut sim = seeded(9103);
    declare(&mut sim, vec![invalidate(Some(&[]))]);
    assert_eq!(sim.host.stamp(&entry_key("a")), 1);
    declare(
        &mut sim,
        vec![invalidate(Some(&["A"])), invalidate(None), track("C")],
    );
    assert_eq!(sim.host.stamp(&entry_key("a")), 2);
    assert_eq!(
        (sim.host.head("A"), sim.host.head("B"), sim.host.head("C")),
        (2, 2, 1)
    );
}
#[test]
fn absence_keeps_tracking_and_global_recreation_reaches_every_holder() {
    let mut sim = seeded(9104);
    let key = entry_key("a");
    sim.host
        .transact(&[(key.clone(), None)], vec![reference()], vec![])
        .unwrap();
    sim.settle();
    assert_eq!(sim.host.stored_memberships(&key), ["A", "B"]);
    assert_eq!(sim.read_text(0, &key), None);
    assert_eq!(sim.read_text(1, &key), None);
    sim.host
        .transact(
            &[(
                key.clone(),
                Some(json!({"id":"a","text":"again","note":null})),
            )],
            vec![reference()],
            vec![],
        )
        .unwrap();
    sim.settle();
    for client in 0..2 {
        assert_eq!(sim.read_text(client, &key).as_deref(), Some("again"));
    }
}

#[test]
fn targeted_permission_loss_delivers_newer_absence_without_notifying_other_viewer() {
    use axton_core::{StreamChange, StreamPullPage};
    let mut sim = seeded(9105);
    let key = entry_key("a");
    sim.host.hide_for_viewer("alice", &key);
    declare(&mut sim, vec![invalidate(Some(&["A"]))]);
    let request = json!({"capabilities":["stream-authority-v1"],"models":axton_sim::schema::declared_models(),"cursors":{"A":1}});
    let page = StreamPullPage::decode(
        sim.host
            .pull("alice", &serde_json::to_vec(&request).unwrap())
            .unwrap()
            .as_bytes(),
    )
    .unwrap();
    assert_eq!(page.changes.len(), 1);
    match &page.changes[0] {
        StreamChange::Upsert { record, .. } => {
            assert_eq!(record.stamp, 2);
            assert!(record.state.is_null());
            assert!(record.error.is_none());
        }
        _ => panic!("permission loss is content absence, never a removal event"),
    }
    sim.client(0).apply_stream_page(page).unwrap();
    assert_eq!(sim.read_text(0, &key), None);
    assert_eq!(sim.read_text(1, &key).as_deref(), Some("stored"));
    assert_eq!(sim.host.head("B"), 1);
    assert_eq!(sim.host.stored_memberships(&key), ["A", "B"]);
}

#[test]
fn generated_tracking_and_invalidation_converge_with_reordered_delivery_and_restart() {
    for seed in 9200..9224 {
        let mut sim = seeded(seed);
        let mut rng = axton_sim::Rng::new(seed);
        for step in 0..80 {
            let stream = if rng.chance(1, 2) { "A" } else { "B" };
            let action = match rng.below(9) {
                0 => Action::StreamDeclarations {
                    intents: vec![track(stream)],
                },
                1 => Action::StreamDeclarations {
                    intents: vec![invalidate(Some(&[stream]))],
                },
                2 => Action::StreamDeclarations {
                    intents: vec![invalidate(None)],
                },
                3 => Action::Declare {
                    key: "Entry:a".into(),
                    touch: Some(if rng.chance(1, 4) {
                        None
                    } else {
                        Some(format!("seed{seed}-step{step}"))
                    }),
                    memberships: vec![],
                },
                4 => Action::Pull { client: 0 },
                5 => Action::Pull { client: 1 },
                6 => Action::Duplicate,
                7 => Action::Deliver,
                _ => Action::Hold,
            };
            sim.apply(action.clone())
                .unwrap_or_else(|e| panic!("seed {seed} step {step} {action:?}: {e}"));
            sim.check()
                .unwrap_or_else(|e| panic!("seed {seed} step {step}: {e}"));
            if step % 20 == 19 {
                for client in 0..2 {
                    sim.apply(Action::Crash { client }).unwrap();
                    sim.apply(Action::Restart { client }).unwrap();
                }
                sim.settle();
                let expected = sim
                    .host
                    .state(&entry_key("a"))
                    .map(|row| row["text"].as_str().unwrap().to_string());
                for client in 0..2 {
                    assert_eq!(
                        sim.read_text(client, &entry_key("a")),
                        expected,
                        "seed {seed} step {step} client {client}"
                    );
                }
            }
        }
    }
}

#[test]
fn targeted_loader_error_keeps_content_and_never_becomes_absence() {
    let mut sim = seeded(9106);
    let key = entry_key("a");
    sim.host.fail_load_next(&key);
    declare(&mut sim, vec![invalidate(Some(&["A"]))]);
    sim.apply(Action::Pull { client: 0 }).unwrap();
    sim.drain();
    assert_eq!(sim.read_text(0, &key).as_deref(), Some("stored"));
    assert_eq!(
        sim.client(0).record_stamp(&key).unwrap(),
        1,
        "diagnostic does not replace authority"
    );
    assert_eq!(
        sim.client(0).cursor("A").unwrap(),
        Some(2),
        "progress is separate from content success"
    );
    assert_eq!(sim.host.head("B"), 1);
    assert_eq!(sim.read_text(1, &key).as_deref(), Some("stored"));
}

#[test]
fn inferred_global_change_overrides_selected_and_advances_the_record_once() {
    let mut sim = seeded(9107);
    let key = entry_key("a");
    sim.host
        .transact(
            &[(
                key.clone(),
                Some(json!({"id":"a","text":"updated","note":null})),
            )],
            vec![reference(), reference()],
            vec![invalidate(Some(&["A"])), track("C")],
        )
        .unwrap();
    assert_eq!(sim.host.stamp(&key), 2);
    assert_eq!(
        (sim.host.head("A"), sim.host.head("B"), sim.host.head("C")),
        (2, 2, 1)
    );
    sim.settle();
    for client in 0..2 {
        assert_eq!(sim.read_text(client, &key).as_deref(), Some("updated"));
    }
}

#[test]
fn invalidating_an_identity_without_holders_still_advances_without_tracking() {
    let mut sim = Sim::new(9108, 1);
    let key = entry_key("a");
    declare(&mut sim, vec![invalidate(Some(&["A"]))]);
    assert_eq!(sim.host.stamp(&key), 1);
    assert_eq!(sim.host.head("A"), 0);
    assert!(sim.host.stored_memberships(&key).is_empty());
    declare(&mut sim, vec![invalidate(None)]);
    assert_eq!(sim.host.stamp(&key), 2);
    assert!(sim.host.stored_memberships(&key).is_empty());
}
