//! Shared settlement: one stamp per changed record, persistent Channel
//! membership reduced to its final state, and one publication position per
//! affected Channel/record pair, on the Action, legacy and external paths
//! ([spec §4-§6](../../../docs/superpowers/specs/2026-09-26-140-touch-publish-design.md)).
mod support;
use axton_server::host::HostRequest;
use serde_json::{Value, json};
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
    backend
        .log()
        .into_iter()
        .filter_map(|request| match request {
            HostRequest::Publish {
                channel,
                identity,
                stamp,
                ..
            } => Some((channel, identity["id"].as_str().unwrap().to_string(), stamp)),
            _ => None,
        })
        .collect()
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
    assert_eq!((backend.head("A"), backend.head("B")), (121, 46));
    assert_eq!(backend.invalidation("A", "Todo", "t"), Some((121, 8)));
    assert_eq!(backend.invalidation("B", "Todo", "t"), Some((46, 8)));
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
    assert_eq!(backend.count("publish"), 0);
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
        assert_eq!(backend.count("setMembership"), 1, "{label}");
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

/// Intents that cancel are compared with the membership at settlement start:
/// a member removed then re-added, and a non-member added then removed, are
/// unchanged. Nothing is written or published and no stamp moves.
#[test]
fn membership_operations_that_cancel_change_nothing_and_publish_nothing() {
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
    assert_eq!(backend.count("setMembership"), 0);
    assert_eq!(backend.count("publish"), 0);
    assert_eq!(backend.count("advanceStamp"), 0);
    // p keeps a final add (A), so its guard is `ensureStamp`; q's final
    // intent is a removal, so it is only locked, and has no row to lock.
    assert_eq!(
        backend.settlement_log(),
        [
            HostRequest::EnsureStamp {
                model: "Project".into(),
                identity_key: r#"{"id":"p"}"#.into()
            },
            HostRequest::LockRecord {
                model: "Project".into(),
                identity_key: r#"{"id":"q"}"#.into()
            },
            HostRequest::Memberships {
                model: "Project".into(),
                identity_key: r#"{"id":"p"}"#.into()
            },
        ]
    );
    let after = backend.tables();
    assert_eq!(after.stamps, before.stamps, "no stamp row created or moved");
    assert_eq!(after.heads, before.heads);
    assert_eq!(after.memberships, before.memberships);
    assert_eq!(after.invalidations, before.invalidations);
}

/// A removal takes effect only when the record is a member, and a changed
/// record removed from a Channel in the same settlement is not published
/// there: the final relationship wins, even for a deletion.
#[test]
fn a_removal_is_net_and_a_record_removed_while_changed_is_not_published_there() {
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
        backend
            .log()
            .into_iter()
            .filter(|request| matches!(request, HostRequest::SetMembership { .. }))
            .collect::<Vec<_>>(),
        [HostRequest::SetMembership {
            channel: "A".into(),
            model: "Todo".into(),
            identity_key: r#"{"id":"t"}"#.into(),
            present: false
        }],
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
    assert_eq!(backend.count("publish"), 0);
    assert_eq!(backend.stamp("Todo", "t"), Some(3));
    assert!(backend.members("Todo", "t").is_empty());
}

/// An output-only read and an unchanged enrollment leave an existing stamp
/// where it is; enrolling a record without metadata initializes it at 1.
/// Re-adding an existing member publishes nothing.
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
    assert_eq!(backend.count("publish"), 0);
    assert_eq!(backend.count("setMembership"), 0);
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

/// Guards are taken in canonical record order before any membership is read;
/// membership writes and publications then follow Channel, then record order.
#[test]
fn settlement_guards_records_in_key_order_then_writes_in_channel_order() {
    let backend = Backend::new();
    for id in ["p1", "p2", "p3"] {
        backend.seed("Project", id, project_row(id), Some(2));
    }
    backend.enroll("A", "Project", "p3", 0);
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
    let key = |id: &str| format!(r#"{{"id":"{id}"}}"#);
    let project = || "Project".to_string();
    let membership = |channel: &str, id: &str, present: bool| HostRequest::SetMembership {
        channel: channel.into(),
        model: project(),
        identity_key: key(id),
        present,
    };
    let publish = |channel: &str, id: &str, stamp: u64| HostRequest::Publish {
        channel: channel.into(),
        model: project(),
        identity: json!({"id":id}),
        identity_key: key(id),
        stamp,
    };
    assert_eq!(
        backend.settlement_log(),
        [
            HostRequest::EnsureStamp {
                model: project(),
                identity_key: key("p1")
            },
            HostRequest::AdvanceStamp {
                model: project(),
                identity_key: key("p2")
            },
            HostRequest::LockRecord {
                model: project(),
                identity_key: key("p3")
            },
            HostRequest::Memberships {
                model: project(),
                identity_key: key("p1")
            },
            HostRequest::Memberships {
                model: project(),
                identity_key: key("p2")
            },
            HostRequest::Memberships {
                model: project(),
                identity_key: key("p3")
            },
            membership("A", "p1", true),
            membership("A", "p2", true),
            membership("A", "p3", false),
            membership("B", "p1", true),
            publish("A", "p1", 2),
            publish("A", "p2", 3),
            publish("B", "p1", 2),
        ]
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

/// A membership naming a blank Channel or an unregistered Model rejects only
/// its own call; the next call in the batch still settles.
#[test]
fn an_invalid_membership_rejects_only_its_call() {
    for (label, intent, code) in [
        (
            "blank channel",
            json!({"channel":" ","model":"Todo","identity":{"id":"t"},"present":true}),
            "publish.invalid",
        ),
        (
            "unknown model",
            json!({"channel":"A","model":"Ghost","identity":{"id":"t"},"present":true}),
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
        assert_eq!(backend.count("setMembership"), 0, "{label}");
        assert_eq!(backend.stamp("Todo", "t"), Some(2), "{label}");
    }
}

// Delivery under membership (spec §4, "Removal and historical delivery"):
// both pull modes scan only rows whose record is still a member of the
// scanned Channel, before the page limit; removal keeps the row and the head.

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
    assert_eq!(backend.head("A"), 3, "removal never rewinds the head");
    assert_eq!(
        backend.invalidation("A", "Todo", "t000"),
        Some((1, 1)),
        "and never erases the row"
    );
    for from in [0, 1, 2] {
        let page = pull(&backend, &[("A", from)]);
        assert!(page.changes.is_empty(), "from {from}: {:?}", page.changes);
        assert_eq!(
            range(&page, "A"),
            (from, 3, 3),
            "the page advances to the head"
        );
    }
    let page = bootstrap(&backend, "A", 0, 3);
    assert!(page.records.is_empty());
    assert!(page.terminal());
    assert_eq!((page.from, page.to, page.head), (0, 3, 3));
}

#[test]
fn removed_rows_exceeding_a_page_do_not_starve_later_active_rows() {
    let full = limits_page();
    let backend = Backend::new();
    // 60 removed positions (more than one page) below 55 active ones.
    let ids = published(&backend, "A", "r", 115);
    settle(&backend, vec![], removes("A", &ids[..60]));
    let first = pull(&backend, &[("A", 0)]);
    assert_eq!(delivered_ids(&first.changes), ids[60..60 + full]);
    assert_eq!(
        range(&first, "A"),
        (0, 110, 115),
        "a full page of eligible rows stops at its last cursor"
    );
    let second = pull(&backend, &[("A", 110)]);
    assert_eq!(delivered_ids(&second.changes), ids[110..]);
    assert_eq!(range(&second, "A"), (110, 115, 115));
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
    assert_eq!(backend.invalidation("A", "Todo", "t"), Some((1, 1)));
    for from in [0, 1] {
        let page = pull(&backend, &[("A", from)]);
        assert!(page.changes.is_empty(), "from {from}: {:?}", page.changes);
        assert_eq!(range(&page, "A"), (from, 1, 1));
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
    backend.clear_log();
    settle(&backend, vec![], vec![add("A", "Todo", "t")]);
    assert_eq!(backend.count("advanceStamp"), 0);
    assert_eq!(backend.stamp("Todo", "t"), Some(2));
    assert_eq!(
        backend.invalidation("A", "Todo", "t"),
        Some((4, 2)),
        "a fresh cursor at the unchanged stamp"
    );
    let page = pull(&backend, &[("A", 3)]);
    assert_eq!(delivered(&page.changes), [("t".into(), 2, title("v2"))]);
    assert_eq!(range(&page, "A"), (3, 4, 4));
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
    assert_eq!(backend.invalidation("A", "Todo", "m"), Some((5, 1)));
    let page = bootstrap(&backend, "A", 0, origin);
    assert_eq!(delivered_ids(&page.records), ["e1", "e2"]);
    assert!(page.terminal());
    assert_eq!(page.head, 5, "the barrier covers the re-added position");
    let page = pull(&backend, &[("A", origin)]);
    assert_eq!(
        delivered(&page.changes),
        [("m".into(), 1, title("history"))]
    );
    assert_eq!(range(&page, "A"), (4, 5, 5));
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
    // so A is told nothing, not even the deletion.
    backend.with(|s| s.tables.rows.remove(&record("Todo", "t")));
    settle(
        &backend,
        vec![reference("Todo", "t")],
        vec![remove("A", "Todo", "t")],
    );
    assert_eq!(backend.stamp("Todo", "t"), Some(4));
    let page = pull(&backend, &[("A", 0)]);
    assert!(page.changes.is_empty(), "{:?}", page.changes);
    assert_eq!(range(&page, "A"), (0, 3, 3));
}

/// Removal distributes nothing: no position, no deletion, no eviction. The
/// business row, the stamp, the retained row and the head stay as they were.
#[test]
fn explicit_removal_fabricates_no_deletion_and_keeps_the_history() {
    let backend = Backend::new();
    backend.seed("Todo", "t", todo_row("t", "kept"), None);
    settle(&backend, vec![], vec![add("A", "Todo", "t")]);
    let before = backend.tables();
    backend.clear_log();
    settle(&backend, vec![], vec![remove("A", "Todo", "t")]);
    assert_eq!(backend.count("publish"), 0);
    let after = backend.tables();
    assert_eq!(after.rows, before.rows);
    assert_eq!(after.stamps, before.stamps);
    assert_eq!(after.heads, before.heads);
    assert_eq!(after.invalidations, before.invalidations);
    assert!(after.memberships.is_empty());
    let page = pull(&backend, &[("A", 0)]);
    assert!(
        page.changes.is_empty(),
        "no null change stands in for removal"
    );
    assert_eq!(range(&page, "A"), (0, 1, 1));
}

/// A delivered Todo state: the loader's row without its identity fields.
fn title(title: &str) -> Value {
    json!({ "title": title })
}
fn limits_page() -> usize {
    axton_core::limits::PULL_CHANGES
}
