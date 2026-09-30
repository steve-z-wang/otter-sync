//! Shared settlement: one stamp per changed record, persistent tagged Channel
//! membership reduced from ordered declarations to its final state, and at
//! most one position per affected Channel/record pair, on the Action, legacy
//! and external paths
//! ([spec §3-§5](../../../docs/superpowers/specs/2026-09-30-channel-tags-removal-design.md)).
mod capability;
mod support;
use axton_core::RecordKey;
use axton_server::channel_members::MemberDelta;
use axton_server::host::HostRequest;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use support::*;

fn todo_row(id: &str, title: &str) -> Value {
    json!({"id":id,"title":title})
}
fn project_row(id: &str) -> Value {
    json!({"id":id,"name":"P"})
}
fn effects(changes: Vec<Value>, memberships: Vec<Value>) -> Value {
    json!({"outputs":{},"changes":changes,"memberships":memberships})
}
fn succeeded(receipt: &Value) {
    assert_eq!(receipt["rejections"], json!([]), "{receipt}");
}
fn publishes(backend: &Backend) -> Vec<(String, String, u64)> {
    backend.publishes()
}

/// Spec §4's example: a Todo at stamp 7 in Channels A and B changes once. It
/// advances to 8 exactly once and each Channel gets one new position at 8.
#[test]
fn a_changed_member_advances_once_and_gets_one_new_position_per_channel() {
    let backend = Backend::new();
    backend.seed("Todo", "t", todo_row("t", "old"), Some(7));
    backend.enroll("A", "Todo", "t", 120);
    backend.enroll("B", "Todo", "t", 45);
    let receipt = push(
        &backend,
        1,
        json!({"Todo":1}),
        vec![edit(1, 1, "Edit", "t", "new")],
    );
    succeeded(&receipt);
    assert_eq!(authority(&receipt), [("Todo".into(), "t".into(), 8)]);
    assert_eq!(backend.count("advanceStamp"), 1);
    assert_eq!(backend.stamp("Todo", "t"), Some(8));
    assert_eq!(
        publishes(&backend),
        [("A".into(), "t".into(), 8), ("B".into(), "t".into(), 8)]
    );
    assert_eq!((backend.head("A"), backend.head("B")), (122, 47));
    assert_eq!(backend.invalidation("A", "Todo", "t"), Some((122, 8)));
    assert_eq!(backend.invalidation("B", "Todo", "t"), Some((47, 8)));
    // A record with no membership changes, stamps and reads back, published nowhere.
    backend.seed("Todo", "lone", todo_row("lone", "old"), None);
    backend.clear_log();
    let receipt = push(
        &backend,
        2,
        json!({"Todo":1}),
        vec![edit(1, 2, "Edit", "lone", "new")],
    );
    succeeded(&receipt);
    assert_eq!(backend.count("lockChannels"), 0, "no Channel to lock");
    assert_eq!(backend.count("applyChannelMembers"), 0);
    assert_eq!(backend.stamp("Todo", "lone"), Some(1));
}

/// A record that is both changed and newly added gets one position, at its
/// final stamp, whether the change is an input target or an extra touch and
/// however the membership intents are ordered around it. (Touches and
/// membership intents travel in separate arrays, so an SDK's interleaving of
/// `touch` and `add` calls cannot reach settlement.)
#[test]
fn a_newly_added_changed_record_gets_one_position_at_its_final_stamp() {
    for (label, name, memberships) in [
        ("input target, add", "Edit", vec![add("A", "Todo", "t")]),
        ("extra touch, add", "Settle", vec![add("A", "Todo", "t")]),
        (
            "extra touch, add remove add",
            "Settle",
            vec![
                add("A", "Todo", "t"),
                remove("A", "Todo", "t"),
                add("A", "Todo", "t"),
            ],
        ),
    ] {
        let backend = Backend::new();
        backend.seed("Todo", "t", todo_row("t", "old"), Some(3));
        let changes = if name == "Settle" {
            vec![reference("Todo", "t")]
        } else {
            vec![]
        };
        backend.script(name, effects(changes, memberships));
        let invocation = if name == "Settle" {
            call(1, 1, "Settle", json!({}))
        } else {
            edit(1, 1, "Edit", "t", "new")
        };
        let receipt = push(&backend, 1, json!({"Todo":1}), vec![invocation]);
        succeeded(&receipt);
        assert_eq!(backend.count("advanceStamp"), 1, "{label}");
        assert_eq!(backend.count("ensureStamp"), 0, "{label}");
        assert_eq!(backend.count("applyChannelMembers"), 1, "{label}");
        assert_eq!(backend.deltas().len(), 1, "{label}: one final state");
        assert_eq!(backend.stamp("Todo", "t"), Some(4), "{label}");
        assert_eq!(
            publishes(&backend),
            [("A".into(), "t".into(), 4)],
            "{label}"
        );
        assert_eq!(backend.members("Todo", "t"), ["A"], "{label}");
        assert_eq!(
            backend.invalidation("A", "Todo", "t"),
            Some((1, 4)),
            "{label}"
        );
    }
}

/// Intents reduce against the membership at settlement start: a non-member
/// added then removed is untouched, while a member removed then re-added is
/// a new membership, positioned once as an upsert (never as a removal). No
/// stamp moves and no Channel is created.
#[test]
fn a_non_member_added_then_removed_is_untouched_and_a_re_added_member_is_positioned_once() {
    let backend = Backend::new();
    backend.seed("Project", "p", project_row("p"), Some(5));
    backend.enroll("A", "Project", "p", 9);
    backend.script(
        "Settle",
        effects(
            vec![],
            vec![
                remove("A", "Project", "p"),
                add("B", "Project", "p"),
                add("A", "Project", "p"),
                remove("B", "Project", "p"),
                // A record with no metadata: added then removed.
                add("B", "Project", "q"),
                remove("B", "Project", "q"),
            ],
        ),
    );
    let before = backend.tables();
    let receipt = push(
        &backend,
        1,
        json!({}),
        vec![call(1, 1, "Settle", json!({}))],
    );
    succeeded(&receipt);
    assert_eq!(backend.members("Project", "p"), ["A"]);
    assert_eq!(backend.stamp("Project", "p"), Some(5));
    assert_eq!(backend.count("advanceStamp"), 0);
    let key = |id: &str| RecordKey {
        model: "Project".into(),
        identity: json!({ "id": id }),
    };
    // p ends added in A with no selector after, so its guard is
    // `ensureStamp`; q's last intent is a removal, so it is only locked, and
    // has no row to lock. Both Channels lock first.
    assert_eq!(
        backend.settlement_log(),
        [
            HostRequest::LockChannels {
                channels: vec!["A".into(), "B".into()]
            },
            HostRequest::EnsureStamp {
                model: "Project".into(),
                identity_key: r#"{"id":"p"}"#.into()
            },
            HostRequest::LockRecord {
                model: "Project".into(),
                identity_key: r#"{"id":"q"}"#.into()
            },
            HostRequest::ReadChannelMembers {
                channel: "A".into(),
                explicit_keys: vec![key("p")],
                tags: vec![],
            },
            HostRequest::ReadChannelMembers {
                channel: "B".into(),
                explicit_keys: vec![key("p"), key("q")],
                tags: vec![],
            },
            HostRequest::ApplyChannelMembers {
                deltas: vec![MemberDelta {
                    channel: "A".into(),
                    key: key("p"),
                    present: true,
                    tags: BTreeSet::new(),
                    publish: true,
                }]
            },
        ]
    );
    let after = backend.tables();
    assert_eq!(after.stamps, before.stamps, "no stamp row created or moved");
    assert_eq!(after.memberships, before.memberships);
    assert_eq!(after.heads["A"], before.heads["A"] + 1);
    assert!(!after.heads.contains_key("B"), "no Channel created");
    assert_eq!(backend.invalidation("A", "Project", "p"), Some((11, 5)));
}

/// A removal takes effect only when the record is a member, and a changed
/// record removed from a Channel in the same settlement gets that Channel's
/// removal, not its upsert: the final relationship wins, even for a deletion.
#[test]
fn a_removal_is_net_and_a_record_removed_while_changed_is_positioned_as_removed_there() {
    let backend = Backend::new();
    backend.seed("Todo", "t", todo_row("t", "old"), Some(2));
    backend.enroll("A", "Todo", "t", 0);
    backend.enroll("B", "Todo", "t", 0);
    backend.write("Settle", "Todo", "t", None);
    backend.script(
        "Settle",
        effects(
            vec![reference("Todo", "t")],
            vec![remove("A", "Todo", "t"), remove("C", "Todo", "t")],
        ),
    );
    let receipt = push(
        &backend,
        1,
        json!({}),
        vec![call(1, 1, "Settle", json!({}))],
    );
    succeeded(&receipt);
    assert_eq!(backend.row("Todo", "t"), None);
    assert_eq!(backend.stamp("Todo", "t"), Some(3));
    assert_eq!(backend.members("Todo", "t"), ["B"]);
    assert_eq!(
        publishes(&backend),
        [("B".into(), "t".into(), 3)],
        "the deletion reaches B only"
    );
    assert_eq!(
        backend.removals(),
        [("A".into(), "t".into())],
        "removing a non-member (C) writes nothing"
    );
    // A removal-only settlement locks the existing record and moves no stamp.
    backend.clear_log();
    backend.script("Settle", effects(vec![], vec![remove("B", "Todo", "t")]));
    let receipt = push(
        &backend,
        2,
        json!({}),
        vec![call(1, 2, "Settle", json!({}))],
    );
    succeeded(&receipt);
    assert_eq!(backend.count("lockRecord"), 1);
    assert_eq!(publishes(&backend), []);
    assert_eq!(backend.removals(), [("B".into(), "t".into())]);
    assert_eq!(backend.stamp("Todo", "t"), Some(3));
    assert!(backend.members("Todo", "t").is_empty());
}

/// An output-only read and an unchanged enrollment leave an existing stamp
/// where it is; enrolling a record without metadata initializes it at 1.
/// Re-adding an existing member publishes nothing and keeps its position.
#[test]
fn output_only_reads_and_unchanged_enrollment_keep_the_existing_stamp() {
    let backend = Backend::new();
    backend.seed("Project", "p", project_row("p"), Some(5));
    backend.script(
        "ReadProject",
        json!({"outputs":{"project":{"id":"p"}},"changes":[],"memberships":[]}),
    );
    let receipt = push(
        &backend,
        1,
        json!({"Project":1}),
        vec![call(1, 1, "ReadProject", json!({}))],
    );
    succeeded(&receipt);
    assert_eq!(
        receipt["completions"][0]["outcome"]["result"],
        json!({"project":{"id":"p","name":"P"}})
    );
    assert_eq!(authority(&receipt), [("Project".into(), "p".into(), 5)]);
    assert_eq!(backend.count("advanceStamp"), 0);
    assert_eq!(backend.stamp("Project", "p"), Some(5));
    // Enrollment of an unchanged record distributes its existing stamp.
    backend.seed("Project", "n", project_row("n"), None);
    backend.clear_log();
    backend.script(
        "Settle",
        effects(
            vec![],
            vec![add("A", "Project", "p"), add("A", "Project", "n")],
        ),
    );
    let receipt = push(
        &backend,
        2,
        json!({}),
        vec![call(1, 2, "Settle", json!({}))],
    );
    succeeded(&receipt);
    assert_eq!(backend.count("advanceStamp"), 0);
    assert_eq!(backend.stamp("Project", "p"), Some(5));
    assert_eq!(backend.stamp("Project", "n"), Some(1), "initialized at 1");
    assert_eq!(
        publishes(&backend),
        [("A".into(), "n".into(), 1), ("A".into(), "p".into(), 5)]
    );
    assert_eq!(
        authority(&receipt),
        [],
        "enrollment is not caller authority"
    );
    // Adding an existing member again is not observable.
    backend.clear_log();
    backend.script("Settle", effects(vec![], vec![add("A", "Project", "p")]));
    let receipt = push(
        &backend,
        3,
        json!({}),
        vec![call(1, 3, "Settle", json!({}))],
    );
    succeeded(&receipt);
    assert_eq!(publishes(&backend), []);
    assert_eq!(
        backend.deltas(),
        [MemberDelta {
            channel: "A".into(),
            key: RecordKey {
                model: "Project".into(),
                identity: json!({"id": "p"}),
            },
            present: true,
            tags: BTreeSet::new(),
            publish: false,
        }],
        "an unchanged add keeps its existing position"
    );
    assert_eq!(backend.invalidation("A", "Project", "p"), Some((2, 5)));
    assert_eq!(backend.head("A"), 2);
    assert_eq!(backend.stamp("Project", "p"), Some(5));
}

/// A retried call ID replays its saved outcome: no handler, no stamp, no
/// cursor and no membership change, and the same result bytes.
#[test]
fn a_saved_call_replays_without_restamping_republishing_or_re_enrolling() {
    let backend = Backend::new();
    backend.seed("Todo", "t", todo_row("t", "old"), Some(1));
    backend.seed("Project", "p", project_row("p"), Some(4));
    backend.enroll("B", "Todo", "t", 0);
    backend.script(
        "EditAndRead",
        json!({"outputs":{"todo":{"id":"t"}},"changes":[reference("Project","p")],
               "memberships":[add("A","Todo","t"),add("A","Project","p")]}),
    );
    let first = push(
        &backend,
        1,
        json!({"Todo":1}),
        vec![edit(1, 1, "EditAndRead", "t", "new")],
    );
    succeeded(&first);
    let settled = backend.tables();
    assert_eq!(
        (
            settled.stamps[&record("Todo", "t")],
            settled.stamps[&record("Project", "p")]
        ),
        (2, 5)
    );
    backend.clear_log();
    let replay = push(
        &backend,
        2,
        json!({"Todo":1}),
        vec![edit(1, 1, "EditAndRead", "t", "new")],
    );
    assert_eq!(replay["completions"], first["completions"]);
    assert_eq!(replay["records"], first["records"]);
    assert_eq!(backend.ops(), ["claim", "claimCall", "saveReceipt"]);
    assert_eq!(
        backend.tables(),
        settled,
        "stamps, heads and relationships unchanged"
    );
}

/// Every Channel an intent names or a changed record belongs to is locked,
/// in canonical order, before any record guard; guards follow in canonical
/// record order; the touch recipients are re-read under the locks; then each
/// Channel's members are read and every final state is written at once, in
/// Channel then record order.
#[test]
fn settlement_locks_channels_then_guards_records_in_key_order_then_writes_once() {
    let backend = Backend::new();
    for id in ["p1", "p2", "p3"] {
        backend.seed("Project", id, project_row(id), Some(2));
    }
    backend.enroll("A", "Project", "p3", 0);
    backend.enroll("C", "Project", "p2", 0);
    backend.script(
        "Settle",
        effects(
            vec![reference("Project", "p2")],
            vec![
                add("B", "Project", "p1"),
                remove("A", "Project", "p3"),
                add("A", "Project", "p2"),
                add("A", "Project", "p1"),
            ],
        ),
    );
    let receipt = push(
        &backend,
        1,
        json!({}),
        vec![call(1, 1, "Settle", json!({}))],
    );
    succeeded(&receipt);
    let key = |id: &str| RecordKey {
        model: "Project".into(),
        identity: json!({ "id": id }),
    };
    let identity_key = |id: &str| format!(r#"{{"id":"{id}"}}"#);
    let project = || "Project".to_string();
    let recipients = || HostRequest::Memberships {
        model: project(),
        identity_key: identity_key("p2"),
    };
    let read = |channel: &str, ids: &[&str]| HostRequest::ReadChannelMembers {
        channel: channel.into(),
        explicit_keys: ids.iter().map(|id| key(id)).collect(),
        tags: vec![],
    };
    let delta = |channel: &str, id: &str, present: bool| MemberDelta {
        channel: channel.into(),
        key: key(id),
        present,
        tags: BTreeSet::new(),
        publish: true,
    };
    assert_eq!(
        backend.settlement_log(),
        [
            recipients(),
            HostRequest::LockChannels {
                channels: vec!["A".into(), "B".into(), "C".into()]
            },
            HostRequest::EnsureStamp {
                model: project(),
                identity_key: identity_key("p1")
            },
            HostRequest::AdvanceStamp {
                model: project(),
                identity_key: identity_key("p2")
            },
            HostRequest::LockRecord {
                model: project(),
                identity_key: identity_key("p3")
            },
            recipients(),
            read("A", &["p1", "p2", "p3"]),
            read("B", &["p1"]),
            read("C", &["p2"]),
            HostRequest::ApplyChannelMembers {
                deltas: vec![
                    delta("A", "p1", true),
                    delta("A", "p2", true),
                    delta("A", "p3", false),
                    delta("B", "p1", true),
                    delta("C", "p2", true),
                ]
            },
        ]
    );
    // A's range is consecutive after p3's old position, in delta order.
    assert_eq!(
        backend.positions("A"),
        [
            (2, "p1".into(), "upsert"),
            (3, "p2".into(), "upsert"),
            (4, "p3".into(), "remove")
        ]
    );
}

/// A touch with no declaration still takes its recipients' Channel locks
/// before its record guard, and republishes it once per Channel.
#[test]
fn a_touch_locks_its_recipient_channels_before_its_record() {
    let backend = Backend::new();
    backend.seed("Todo", "t", todo_row("t", "old"), Some(1));
    backend.enroll("B", "Todo", "t", 0);
    backend.enroll("A", "Todo", "t", 0);
    backend.clear_log();
    settle(&backend, vec![reference("Todo", "t")], vec![]);
    assert_eq!(
        backend.ops(),
        [
            "memberships",
            "lockChannels",
            "advanceStamp",
            "memberships",
            "readChannelMembers",
            "readChannelMembers",
            "applyChannelMembers"
        ]
    );
    assert_eq!(
        publishes(&backend),
        [("A".into(), "t".into(), 2), ("B".into(), "t".into(), 2)]
    );
}

/// A competing membership write that commits between resolving the touch
/// recipients and taking their locks invalidates the lock set: the
/// settlement fails as a retryable `transaction.conflict` before writing any
/// member, rather than locking the new Channel out of order. An Action push
/// does not save it as the call's rejection; the whole transaction retries,
/// and the retry locks the new Channel too.
#[test]
fn a_membership_moved_before_the_locks_fails_as_a_retryable_conflict() {
    let backend = Backend::new();
    backend.seed("Todo", "t", todo_row("t", "old"), Some(1));
    backend.enroll("A", "Todo", "t", 0);
    backend.intrude_at_lock("B", "Todo", "t");
    let error = run(axton_server::settle_external(
        &config(),
        &json!({"changes":[reference("Todo","t")],"memberships":[]}),
        &backend,
    ))
    .unwrap_err();
    assert_eq!(error.code, "transaction.conflict", "{error}");
    assert!(error.message.contains("Channel B"), "{}", error.message);
    assert_eq!(backend.count("applyChannelMembers"), 0);
    // An Action call through a push: an error, not a saved rejection.
    let backend = Backend::new();
    backend.seed("Todo", "t", todo_row("t", "old"), Some(1));
    backend.enroll("A", "Todo", "t", 0);
    backend.script("Settle", effects(vec![reference("Todo", "t")], vec![]));
    let before = backend.tables();
    backend.intrude_at_lock("B", "Todo", "t");
    let request = json!({"clientId":"device","batchSequence":1,"models":{},
        "mutations":[call(1, 1, "Settle", json!({}))]});
    let error = run(axton_server::process_action_push(
        &config(),
        "alice",
        &crate::capability::request(request.to_string().as_bytes()),
        &backend,
    ))
    .unwrap_err();
    assert_eq!(error.code, "transaction.conflict");
    // The owning transaction rolls back and runs again: the retry sees B.
    backend.with(|s| s.tables = before);
    backend.enroll("B", "Todo", "t", 0);
    backend.clear_log();
    succeeded(&push(
        &backend,
        1,
        json!({}),
        vec![call(1, 1, "Settle", json!({}))],
    ));
    assert_eq!(
        backend.log()[..].iter().find_map(|request| match request {
            HostRequest::LockChannels { channels } => Some(channels.clone()),
            _ => None,
        }),
        Some(vec!["A".to_string(), "B".to_string()])
    );
    assert_eq!(
        publishes(&backend),
        [("A".into(), "t".into(), 2), ("B".into(), "t".into(), 2)]
    );
}

/// The legacy and external paths settle the same effects through the same
/// algorithm as an Action: the same guards, membership writes and positions.
#[test]
fn legacy_and_external_paths_share_the_settlement() {
    let prepare = || {
        let backend = Backend::new();
        backend.seed("Todo", "t", todo_row("t", "old"), Some(1));
        backend.seed("Project", "p", project_row("p"), Some(3));
        backend.enroll("B", "Todo", "t", 0);
        backend
    };
    let memberships = vec![add("A", "Todo", "t"), add("A", "Project", "p")];
    // An Action: the input target plus an extra touch of p.
    let action = prepare();
    action.script(
        "Edit",
        effects(vec![reference("Project", "p")], memberships.clone()),
    );
    succeeded(&push(
        &action,
        1,
        json!({"Todo":1}),
        vec![edit(1, 1, "Edit", "t", "new")],
    ));
    // A legacy slot handler answering the same effects.
    let legacy = prepare();
    legacy.script(
        "edit",
        json!({"changes":[reference("Project","p")],"memberships":memberships.clone()}),
    );
    let receipt = legacy_push(&legacy, 1, json!({"Todo":1}), "t", "new");
    succeeded(&receipt);
    // An external transaction reporting both records changed.
    let external = prepare();
    let answer = run(axton_server::settle_external(
        &config(),
        &json!({"changes":[reference("Todo","t"),reference("Project","p")],"memberships":memberships}),
        &external,
    ))
    .unwrap();
    assert_eq!(
        answer,
        json!([
            {"model":"Project","identity":{"id":"p"},"stamp":4},
            {"model":"Todo","identity":{"id":"t"},"stamp":2}
        ])
    );
    assert_eq!(action.settlement_log(), legacy.settlement_log());
    assert_eq!(action.settlement_log(), external.settlement_log());
    assert_eq!(
        external.count("load"),
        0,
        "an external settlement reads nothing back"
    );
    for backend in [&action, &legacy, &external] {
        assert_eq!(
            publishes(backend),
            [
                ("A".into(), "p".into(), 4),
                ("A".into(), "t".into(), 2),
                ("B".into(), "t".into(), 2)
            ]
        );
    }
    // The legacy receipt carries its input target only, not the extra touch.
    assert_eq!(
        receipt["records"]
            .as_array()
            .unwrap()
            .iter()
            .map(|record| (record["model"].clone(), record["stamp"].clone()))
            .collect::<Vec<_>>(),
        [(json!("Todo"), json!(2))]
    );
}

/// Tags are validated at settlement as a defense in depth: blank under JS
/// `trim()` or Rust whitespace (U+FEFF and U+0085 included), past 256 UTF-8
/// bytes, or more than 64 distinct on one add. Each refusal is
/// `handler.invalid`, rejects only its call and is never a dropped tag; a
/// valid tag settles as spelled.
#[test]
fn a_tag_breaking_the_tag_rules_rejects_only_its_call() {
    let tagged = |tags: Vec<String>| json!({"kind":"add","channel":"A","record":{"model":"Todo","identity":{"id":"t"}},"tags":tags});
    let many: Vec<String> = (0..65).map(|n| format!("t{n}")).collect();
    for (label, intent) in [
        ("blank tag", tagged(vec!["  ".into()])),
        ("byte order mark", tagged(vec!["\u{feff}".into()])),
        ("next line", tagged(vec!["\u{85}".into()])),
        ("257 bytes", tagged(vec!["x".repeat(257)])),
        ("65 distinct tags", tagged(many)),
        ("blank selector", remove_tag("A", "\t")),
    ] {
        let backend = Backend::new();
        backend.seed("Todo", "t", todo_row("t", "old"), Some(1));
        backend.script("Settle", effects(vec![], vec![intent]));
        let receipt = push(
            &backend,
            1,
            json!({"Todo":1}),
            vec![
                call(1, 1, "Settle", json!({})),
                edit(2, 2, "Edit", "t", "new"),
            ],
        );
        assert_eq!(
            receipt["rejections"],
            json!([{"ordinal":1,"code":"handler.invalid"}]),
            "{label}"
        );
        assert_eq!(backend.count("lockChannels"), 0, "{label}: refused first");
        assert_eq!(backend.stamp("Todo", "t"), Some(2), "{label}");
    }
    // The external settlement refuses with the reason.
    let backend = Backend::new();
    backend.seed("Todo", "t", todo_row("t", "old"), Some(1));
    let error = run(axton_server::settle_external(
        &config(),
        &json!({"changes": [], "memberships": [tagged(vec![String::new()])]}),
        &backend,
    ))
    .unwrap_err();
    assert_eq!(error.code, "handler.invalid");
    assert!(error.message.contains("blank"), "{}", error.message);
    // Spelling is kept: padding, case and 256 bytes of text are tags.
    let backend = Backend::new();
    backend.seed("Todo", "t", todo_row("t", "old"), Some(1));
    let long = "é".repeat(128);
    settle(
        &backend,
        vec![],
        vec![add_tagged("A", "Todo", "t", &[" X ", "x", &long])],
    );
    let mut expected = vec![" X ".to_string(), "x".into(), long];
    expected.sort();
    assert_eq!(backend.tagged_members("A"), [("t".to_string(), expected)]);
}

/// A selector selects by the tags a member has, including ones only an
/// earlier declaration of the same settlement gave it, and removes the whole
/// membership, other tags included. Members of other Channels and members
/// without the tag are untouched; the tag index reads no other member.
#[test]
fn a_tag_selector_removes_whole_memberships_in_its_channel_only() {
    let backend = Backend::new();
    for id in ["A", "B", "C", "D"] {
        backend.seed("Todo", id, todo_row(id, "v1"), Some(1));
    }
    settle(
        &backend,
        vec![],
        vec![
            add_tagged("U", "Todo", "A", &["X", "Y"]),
            add_tagged("U", "Todo", "B", &["X"]),
            add_tagged("U", "Todo", "C", &["Y"]),
            add_tagged("V", "Todo", "A", &["X"]),
        ],
    );
    assert_eq!(backend.head("U"), 3);
    backend.clear_log();
    settle(
        &backend,
        vec![],
        vec![add_tagged("U", "Todo", "D", &["X"]), remove_tag("U", "X")],
    );
    assert_eq!(
        backend.tagged_members("U"),
        [("C".to_string(), vec!["Y".to_string()])]
    );
    assert_eq!(
        backend.tagged_members("V"),
        [("A".to_string(), vec!["X".to_string()])]
    );
    assert_eq!(
        backend.positions("U"),
        [
            (3, "C".into(), "upsert"),
            (4, "A".into(), "remove"),
            (5, "B".into(), "remove")
        ],
        "one removal per member in record order; D never became a member"
    );
    assert_eq!(
        backend.log()[..].iter().find_map(|request| match request {
            HostRequest::ReadChannelMembers { tags, .. } => Some(tags.clone()),
            _ => None,
        }),
        Some(vec!["X".to_string()])
    );
    // No record is guarded but the one named explicitly; no stamp moves.
    assert_eq!(
        backend.count("lockRecord") + backend.count("ensureStamp"),
        1
    );
    assert_eq!(backend.stamp("Todo", "A"), Some(1));
    // Removing a tag nobody carries changes nothing.
    backend.clear_log();
    settle(&backend, vec![], vec![remove_tag("U", "Z")]);
    assert_eq!(backend.count("applyChannelMembers"), 0);
    assert_eq!(backend.head("U"), 5);
}

/// A membership naming a blank Channel or an unregistered Model rejects only
/// its own call; the next call in the batch still settles.
#[test]
fn an_invalid_membership_rejects_only_its_call() {
    for (label, intent, code) in [
        (
            "blank channel",
            json!({"kind":"add","channel":" ","record":{"model":"Todo","identity":{"id":"t"}},"tags":[]}),
            "publish.invalid",
        ),
        (
            "unknown model",
            json!({"kind":"add","channel":"A","record":{"model":"Ghost","identity":{"id":"t"}},"tags":[]}),
            "loader.unregistered",
        ),
    ] {
        let backend = Backend::new();
        backend.seed("Todo", "t", todo_row("t", "old"), Some(1));
        backend.script("Settle", effects(vec![], vec![intent]));
        let receipt = push(
            &backend,
            1,
            json!({"Todo":1}),
            vec![
                call(1, 1, "Settle", json!({})),
                edit(2, 2, "Edit", "t", "new"),
            ],
        );
        assert_eq!(
            receipt["rejections"],
            json!([{"ordinal":1,"code":code}]),
            "{label}"
        );
        assert_eq!(backend.count("applyChannelMembers"), 0, "{label}");
        assert_eq!(backend.stamp("Todo", "t"), Some(2), "{label}");
    }
}

// Delivery under membership: a removal takes a position of kind `remove`
// that replaces the pair's upsert; both pull modes still scan only upserts of
// current members, before the page limit, so a removal is not delivered yet
// but its position is covered by the page range up to the head.

/// `(id, stamp, state)` of each delivered record, in page order.
fn delivered(records: &[axton_core::AuthorityRecord]) -> Vec<(String, u64, Value)> {
    records
        .iter()
        .map(|record| {
            (
                record.identity["id"].as_str().unwrap().to_string(),
                record.stamp,
                record.state.clone(),
            )
        })
        .collect()
}
fn delivered_ids(records: &[axton_core::AuthorityRecord]) -> Vec<String> {
    delivered(records).into_iter().map(|(id, ..)| id).collect()
}
/// `(from, to, head)` of one Channel's range in a delta page.
fn range(page: &axton_core::PullPage, channel: &str) -> (u64, u64, u64) {
    let range = &page.cursors[channel];
    (range.from, range.to, range.head)
}
fn adds(channel: &str, ids: &[String]) -> Vec<Value> {
    ids.iter().map(|id| add(channel, "Todo", id)).collect()
}
fn removes(channel: &str, ids: &[String]) -> Vec<Value> {
    ids.iter().map(|id| remove(channel, "Todo", id)).collect()
}
/// Todo rows `prefix000..`, enrolled in `channel` by one settlement: one
/// position each, in canonical key order, all at stamp 1.
fn published(backend: &Backend, channel: &str, prefix: &str, count: usize) -> Vec<String> {
    let ids: Vec<String> = (0..count).map(|i| format!("{prefix}{i:03}")).collect();
    for id in &ids {
        backend.seed("Todo", id, todo_row(id, "v1"), None);
    }
    settle(backend, vec![], adds(channel, &ids));
    ids
}

#[test]
fn removing_every_remaining_row_yields_a_terminal_advancing_page() {
    let backend = Backend::new();
    let ids = published(&backend, "A", "t", 3);
    assert_eq!(backend.head("A"), 3);
    settle(&backend, vec![], removes("A", &ids));
    assert_eq!(backend.head("A"), 6, "one removal position per member");
    assert_eq!(
        backend.positions("A"),
        [
            (4, "t000".into(), "remove"),
            (5, "t001".into(), "remove"),
            (6, "t002".into(), "remove")
        ],
        "each replaces its pair's upsert"
    );
    for from in [0, 1, 2, 5] {
        let page = pull(&backend, &[("A", from)]);
        assert!(page.changes.is_empty(), "from {from}: {:?}", page.changes);
        assert_eq!(
            range(&page, "A"),
            (from, 6, 6),
            "the page advances to the head"
        );
    }
    let page = bootstrap(&backend, "A", 0, 3);
    assert!(page.records.is_empty());
    assert!(page.terminal());
    assert_eq!((page.from, page.to, page.head), (0, 3, 6));
}

#[test]
fn removed_rows_exceeding_a_page_do_not_starve_later_active_rows() {
    let full = limits_page();
    let backend = Backend::new();
    // 60 removed positions (more than one page) below 55 active ones.
    let ids = published(&backend, "A", "r", 115);
    settle(&backend, vec![], removes("A", &ids[..60]));
    assert_eq!(backend.head("A"), 175, "60 removal positions above them");
    let first = pull(&backend, &[("A", 0)]);
    assert_eq!(delivered_ids(&first.changes), ids[60..60 + full]);
    assert_eq!(
        range(&first, "A"),
        (0, 110, 175),
        "a full page of eligible rows stops at its last cursor"
    );
    let second = pull(&backend, &[("A", 110)]);
    assert_eq!(delivered_ids(&second.changes), ids[110..]);
    assert_eq!(range(&second, "A"), (110, 160, 175));
    // Bootstrap over the whole history pages the same eligible rows.
    let page = bootstrap(&backend, "A", 0, 115);
    assert_eq!(delivered_ids(&page.records), ids[60..110]);
    assert_eq!((page.to, page.terminal()), (110, false));
    let page = bootstrap(&backend, "A", 110, 115);
    assert_eq!(delivered_ids(&page.records), ids[110..]);
    assert!(page.terminal());
    // An origin inside the first eligible page: the scan crosses it, so the
    // page is terminal and carries only the rows at or below it.
    let page = bootstrap(&backend, "A", 0, 100);
    assert_eq!(delivered_ids(&page.records), ids[60..100]);
    assert!(page.terminal());
}

#[test]
fn a_record_removed_then_touched_elsewhere_is_not_exposed_through_its_old_channel() {
    let backend = Backend::new();
    backend.seed("Todo", "t", todo_row("t", "shared"), None);
    settle(
        &backend,
        vec![],
        vec![add("A", "Todo", "t"), add("B", "Todo", "t")],
    );
    settle(&backend, vec![], vec![remove("A", "Todo", "t")]);
    // A later change, published through the Channel it still belongs to.
    backend.seed("Todo", "t", todo_row("t", "after removal"), None);
    settle(&backend, vec![reference("Todo", "t")], vec![]);
    assert_eq!(backend.stamp("Todo", "t"), Some(2));
    assert_eq!(backend.positions("A"), [(2, "t".into(), "remove")]);
    for from in [0, 1] {
        let page = pull(&backend, &[("A", from)]);
        assert!(page.changes.is_empty(), "from {from}: {:?}", page.changes);
        assert_eq!(range(&page, "A"), (from, 2, 2));
    }
    let page = bootstrap(&backend, "A", 0, 1);
    assert!(page.records.is_empty() && page.terminal());
    let page = pull(&backend, &[("B", 1)]);
    assert_eq!(
        delivered(&page.changes),
        [("t".into(), 2, title("after removal"))]
    );
}

#[test]
fn re_adding_a_removed_record_publishes_its_current_state_at_a_fresh_position() {
    let backend = Backend::new();
    for id in ["t", "u"] {
        backend.seed("Todo", id, todo_row(id, "v1"), None);
    }
    settle(
        &backend,
        vec![],
        vec![add("A", "Todo", "t"), add("A", "Todo", "u")],
    );
    backend.seed("Todo", "t", todo_row("t", "v2"), None);
    settle(&backend, vec![reference("Todo", "t")], vec![]);
    assert_eq!(backend.invalidation("A", "Todo", "t"), Some((3, 2)));
    // Remove, then re-add in a separate settlement: not a change.
    settle(&backend, vec![], vec![remove("A", "Todo", "t")]);
    assert_eq!(backend.positions("A")[1..], [(4, "t".into(), "remove")]);
    backend.clear_log();
    settle(&backend, vec![], vec![add("A", "Todo", "t")]);
    assert_eq!(backend.count("advanceStamp"), 0);
    assert_eq!(backend.stamp("Todo", "t"), Some(2));
    assert_eq!(
        backend.invalidation("A", "Todo", "t"),
        Some((5, 2)),
        "a fresh cursor at the unchanged stamp"
    );
    let page = pull(&backend, &[("A", 3)]);
    assert_eq!(delivered(&page.changes), [("t".into(), 2, title("v2"))]);
    assert_eq!(range(&page, "A"), (3, 5, 5));
}

/// The e2e fixture's sequence (remove, then re-add in its own settlement)
/// moves a record above a Bootstrap origin; the fixed interval drops it and
/// ordinary delivery from the origin carries it. A record removed and not
/// re-added is covered by neither: coverage follows membership in the scan
/// snapshot, not every identity ever published.
#[test]
fn a_record_moved_above_the_bootstrap_origin_is_covered_by_live_delivery() {
    let backend = Backend::new();
    for id in ["e1", "e2", "m", "x"] {
        backend.seed("Todo", id, todo_row(id, "history"), None);
    }
    settle(
        &backend,
        vec![],
        ["e1", "e2", "m", "x"]
            .iter()
            .map(|id| add("A", "Todo", id))
            .collect(),
    );
    let origin = backend.head("A");
    assert_eq!(origin, 4);
    settle(&backend, vec![], vec![remove("A", "Todo", "m")]);
    settle(&backend, vec![], vec![add("A", "Todo", "m")]);
    settle(&backend, vec![], vec![remove("A", "Todo", "x")]);
    assert_eq!(backend.invalidation("A", "Todo", "m"), Some((6, 1)));
    let page = bootstrap(&backend, "A", 0, origin);
    assert_eq!(delivered_ids(&page.records), ["e1", "e2"]);
    assert!(page.terminal());
    assert_eq!(page.head, 7, "the barrier covers the re-added position");
    let page = pull(&backend, &[("A", origin)]);
    assert_eq!(
        delivered(&page.changes),
        [("m".into(), 1, title("history"))]
    );
    assert_eq!(range(&page, "A"), (4, 7, 7));
}

#[test]
fn a_deleted_record_stays_enrolled_yields_null_and_its_recreation_distributes_again() {
    let backend = Backend::new();
    backend.seed("Todo", "t", todo_row("t", "v1"), None);
    settle(&backend, vec![], vec![add("A", "Todo", "t")]);
    // The business row is deleted and the change declared: no membership edit.
    backend.with(|s| s.tables.rows.remove(&record("Todo", "t")));
    settle(&backend, vec![reference("Todo", "t")], vec![]);
    assert_eq!(backend.members("Todo", "t"), ["A"]);
    let page = pull(&backend, &[("A", 1)]);
    assert_eq!(delivered(&page.changes), [("t".into(), 2, Value::Null)]);
    // The same identity recreated reaches the same membership, unasked.
    backend.seed("Todo", "t", todo_row("t", "again"), None);
    settle(&backend, vec![reference("Todo", "t")], vec![]);
    let page = pull(&backend, &[("A", 2)]);
    assert_eq!(delivered(&page.changes), [("t".into(), 3, title("again"))]);
    // Deleting and removing in one settlement: the final relationship wins,
    // so A is told of the removal, not the deletion.
    backend.with(|s| s.tables.rows.remove(&record("Todo", "t")));
    settle(
        &backend,
        vec![reference("Todo", "t")],
        vec![remove("A", "Todo", "t")],
    );
    assert_eq!(backend.stamp("Todo", "t"), Some(4));
    assert_eq!(backend.positions("A"), [(4, "t".into(), "remove")]);
    let page = pull(&backend, &[("A", 0)]);
    assert!(page.changes.is_empty(), "{:?}", page.changes);
    assert_eq!(range(&page, "A"), (0, 4, 4));
}

/// Removal takes one `remove` position and fabricates no deletion: the
/// business row and the stamp stay as they were, and no null change stands
/// in for it.
#[test]
fn explicit_removal_is_positioned_and_fabricates_no_deletion() {
    let backend = Backend::new();
    backend.seed("Todo", "t", todo_row("t", "kept"), None);
    settle(&backend, vec![], vec![add("A", "Todo", "t")]);
    let before = backend.tables();
    backend.clear_log();
    settle(&backend, vec![], vec![remove("A", "Todo", "t")]);
    assert_eq!(publishes(&backend), []);
    assert_eq!(backend.removals(), [("A".into(), "t".into())]);
    let after = backend.tables();
    assert_eq!(after.rows, before.rows);
    assert_eq!(after.stamps, before.stamps);
    assert_eq!(after.heads["A"], 2);
    assert_eq!(backend.positions("A"), [(2, "t".into(), "remove")]);
    assert!(after.memberships.is_empty());
    let page = pull(&backend, &[("A", 0)]);
    assert!(
        page.changes.is_empty(),
        "no null change stands in for removal"
    );
    assert_eq!(range(&page, "A"), (0, 2, 2));
}

/// A delivered Todo state: the loader's row without its identity fields.
fn title(title: &str) -> Value {
    json!({ "title": title })
}
fn limits_page() -> usize {
    axton_core::limits::PULL_CHANGES
}

/// One ordered-declaration case against Channel `U`: the members it starts
/// with, the settlement's touched records and ordered intents, the members it
/// ends with and the positions it allocates.
struct Reduction {
    label: &'static str,
    initial: Vec<(&'static str, Vec<&'static str>)>,
    touched: Vec<&'static str>,
    intents: Vec<Value>,
    members: Vec<(&'static str, Vec<&'static str>)>,
    events: Vec<(&'static str, &'static str)>,
}

/// Spec §3 "Ordered declarations": a selector observes the declarations
/// before it, the settlement reduces to one final state per pair, and each
/// pair gets at most one position, never an intermediate one.
#[test]
fn ordered_declarations_reduce_to_the_final_membership_and_its_events() {
    let cases = [
        Reduction {
            label: "{} ; add A/X, removeTag X => {} ; no event",
            initial: vec![],
            touched: vec![],
            intents: vec![add_tagged("U", "Todo", "A", &["X"]), remove_tag("U", "X")],
            members: vec![],
            events: vec![],
        },
        Reduction {
            label: "A:{X,Y} ; removeTag X => {} ; remove A",
            initial: vec![("A", vec!["X", "Y"])],
            touched: vec![],
            intents: vec![remove_tag("U", "X")],
            members: vec![],
            events: vec![("A", "remove")],
        },
        Reduction {
            label: "A:{X} ; add A/Y => A:{X,Y} ; no event",
            initial: vec![("A", vec!["X"])],
            touched: vec![],
            intents: vec![add_tagged("U", "Todo", "A", &["Y"])],
            members: vec![("A", vec!["X", "Y"])],
            events: vec![],
        },
        Reduction {
            label: "A:{X} ; remove A, add A/Y => A:{Y} ; upsert A",
            initial: vec![("A", vec!["X"])],
            touched: vec![],
            intents: vec![
                remove("U", "Todo", "A"),
                add_tagged("U", "Todo", "A", &["Y"]),
            ],
            members: vec![("A", vec!["Y"])],
            events: vec![("A", "upsert")],
        },
        Reduction {
            label: "{} ; removeTag X, add A/X => A:{X} ; upsert A",
            initial: vec![],
            touched: vec![],
            intents: vec![remove_tag("U", "X"), add_tagged("U", "Todo", "A", &["X"])],
            members: vec![("A", vec!["X"])],
            events: vec![("A", "upsert")],
        },
        Reduction {
            label: "A:{X},B:{Y} ; removeTag X, touch B => B:{Y} ; remove A, upsert B",
            initial: vec![("A", vec!["X"]), ("B", vec!["Y"])],
            touched: vec!["B"],
            intents: vec![remove_tag("U", "X")],
            members: vec![("B", vec!["Y"])],
            events: vec![("A", "remove"), ("B", "upsert")],
        },
    ];
    let owned = |pairs: &[(&str, Vec<&str>)]| -> Vec<(String, Vec<String>)> {
        pairs
            .iter()
            .map(|(id, tags)| {
                (
                    id.to_string(),
                    tags.iter().map(|tag| tag.to_string()).collect(),
                )
            })
            .collect()
    };
    for case in cases {
        let label = case.label;
        let backend = Backend::new();
        for id in ["A", "B"] {
            backend.seed("Todo", id, todo_row(id, "v1"), Some(1));
        }
        if !case.initial.is_empty() {
            let adds = case
                .initial
                .iter()
                .map(|(id, tags)| add_tagged("U", "Todo", id, tags))
                .collect();
            settle(&backend, vec![], adds);
        }
        assert_eq!(
            backend.tagged_members("U"),
            owned(&case.initial),
            "{label}: initial"
        );
        let head = backend.head("U");
        let touched = case
            .touched
            .iter()
            .map(|id| reference("Todo", id))
            .collect();
        settle(&backend, touched, case.intents);
        assert_eq!(
            backend.tagged_members("U"),
            owned(&case.members),
            "{label}: final members"
        );
        let events: Vec<(String, &str)> = backend
            .positions("U")
            .into_iter()
            .filter(|(cursor, ..)| *cursor > head)
            .map(|(_, id, kind)| (id, kind))
            .collect();
        let expected: Vec<(String, &str)> = case
            .events
            .iter()
            .map(|(id, kind)| (id.to_string(), *kind))
            .collect();
        assert_eq!(events, expected, "{label}: events");
        assert_eq!(
            backend.head("U"),
            head + expected.len() as u64,
            "{label}: one position per event, none for the rest"
        );
        for id in &case.touched {
            assert_eq!(backend.stamp("Todo", id), Some(2), "{label}: one stamp");
        }
    }
}

/// Settlement holds `readChannelMembers` and `applyChannelMembers` answers to
/// their requests: a member answered twice or never asked for, a position
/// for another Channel, record or kind, a duplicated or missing result, a
/// cursor outside one consecutive range, and a row missing a member are all
/// `host.invalid`, which aborts the delivery instead of becoming a rejection.
#[test]
fn a_channel_answer_that_does_not_match_its_request_is_host_invalid() {
    fn first(answer: &Value) -> Value {
        answer.as_array().unwrap()[0].clone()
    }
    type Tamper = fn(Value) -> Value;
    let cases: [(&str, &str, Tamper, &str); 9] = [
        (
            "a member answered twice",
            "readChannelMembers",
            |answer| json!([first(&answer), first(&answer)]),
            "twice",
        ),
        (
            "a member neither named nor selected",
            "readChannelMembers",
            |answer| {
                let mut rows = answer.as_array().unwrap().clone();
                rows.push(json!({"model":"Todo","identityKey":"{\"id\":\"other\"}","tags":[]}));
                Value::Array(rows)
            },
            "neither names nor selects",
        ),
        (
            "a member missing its tags",
            "readChannelMembers",
            |answer| {
                let mut row = first(&answer);
                row.as_object_mut().unwrap().remove("tags");
                json!([row])
            },
            "missing field `tags`",
        ),
        (
            "a position in another Channel",
            "applyChannelMembers",
            |answer| {
                let mut row = first(&answer);
                row["channel"] = json!("V");
                json!([row])
            },
            "in Channel V for",
        ),
        (
            "a position of another record",
            "applyChannelMembers",
            |answer| {
                let mut row = first(&answer);
                row["identityKey"] = json!("{\"id\":\"other\"}");
                json!([row])
            },
            "for Todo",
        ),
        (
            "a position of the wrong kind",
            "applyChannelMembers",
            |answer| {
                let mut row = first(&answer);
                row["kind"] = json!("remove");
                json!([row])
            },
            "Remove position",
        ),
        (
            "a duplicated position",
            "applyChannelMembers",
            |answer| json!([first(&answer), first(&answer)]),
            "2 positions for 1 deltas",
        ),
        (
            "no position",
            "applyChannelMembers",
            |_| json!([]),
            "0 positions for 1 deltas",
        ),
        (
            "a position missing its kind",
            "applyChannelMembers",
            |answer| {
                let mut row = first(&answer);
                row.as_object_mut().unwrap().remove("kind");
                json!([row])
            },
            "missing field `kind`",
        ),
    ];
    for (label, op, tamper, detail) in cases {
        let backend = Backend::new();
        backend.seed("Todo", "t", todo_row("t", "old"), Some(1));
        settle(&backend, vec![], vec![add_tagged("U", "Todo", "t", &["X"])]);
        backend.tamper(op, tamper);
        backend.script(
            "Settle",
            effects(vec![], vec![add_tagged("U", "Todo", "t", &["Y"])]),
        );
        let request = json!({"clientId":"device","batchSequence":1,"models":{},
            "mutations":[call(1, 1, "Settle", json!({}))]});
        let error = run(axton_server::process_action_push(
            &config(),
            "alice",
            &crate::capability::request(request.to_string().as_bytes()),
            &backend,
        ))
        .unwrap_err();
        assert_eq!(error.code, "host.invalid", "{label}: {error}");
        assert!(error.message.contains(op), "{label}: {}", error.message);
        assert!(error.message.contains(detail), "{label}: {}", error.message);
    }
    // Two published positions of one Channel must be one consecutive range.
    let backend = Backend::new();
    for id in ["a", "b"] {
        backend.seed("Todo", id, todo_row(id, "old"), Some(1));
    }
    backend.tamper("applyChannelMembers", |answer| {
        let mut rows = answer.as_array().unwrap().clone();
        rows[1]["cursor"] = json!(9);
        Value::Array(rows)
    });
    let error = run(axton_server::settle_external(
        &config(),
        &json!({"changes":[],"memberships":[add("U","Todo","a"),add("U","Todo","b")]}),
        &backend,
    ))
    .unwrap_err();
    assert_eq!(error.code, "host.invalid");
    assert!(error.message.contains("consecutive"), "{}", error.message);
}

/// A call rejected after its settlement rolls back its savepoint: members,
/// tags, positions and heads are as before, and the next call still settles.
#[test]
fn a_rejected_call_rolls_back_its_members_tags_and_positions() {
    let backend = Backend::new();
    for id in ["A", "t"] {
        backend.seed("Todo", id, todo_row(id, "old"), Some(1));
    }
    settle(&backend, vec![], vec![add_tagged("U", "Todo", "A", &["X"])]);
    let before = backend.tables();
    backend.refuse_load("Todo", "t");
    backend.script(
        "Edit",
        effects(
            vec![],
            vec![
                add_tagged("U", "Todo", "t", &["Y"]),
                add_tagged("U", "Todo", "A", &["Z"]),
                remove_tag("U", "X"),
            ],
        ),
    );
    backend.script(
        "Settle",
        effects(vec![], vec![add_tagged("U", "Todo", "A", &["W"])]),
    );
    let receipt = push(
        &backend,
        1,
        json!({"Todo":1}),
        vec![
            edit(1, 1, "Edit", "t", "new"),
            call(2, 2, "Settle", json!({})),
        ],
    );
    assert_eq!(
        receipt["rejections"],
        json!([{"ordinal":1,"code":"todo.forbidden"}])
    );
    assert_eq!(
        backend.count("applyChannelMembers"),
        3,
        "both calls settled"
    );
    let after = backend.tables();
    assert_eq!(after.stamps, before.stamps);
    assert_eq!(after.heads, before.heads);
    assert_eq!(after.invalidations, before.invalidations);
    assert_eq!(
        backend.tagged_members("U"),
        [("A".to_string(), vec!["W".to_string(), "X".to_string()])],
        "only the second call's tag union survives"
    );
}
