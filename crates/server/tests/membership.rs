//! Final Stream tracking/invalidation plus retained historical removal delivery.
mod capability;
mod support;
use axton_server::host::{GuardMode, HostRequest};
use serde_json::{Value, json};
use support::*;
fn title(t: &str) -> Value {
    json!({"title":t})
}
fn todo_row(id: &str, title: &str) -> Value {
    json!({"id":id,"title":title})
}
fn effects(changes: Vec<Value>, declarations: Vec<Value>) -> Value {
    json!({"outputs":{},"changes":changes,"declarations":declarations})
}
fn invalidate(streams: Option<&[&str]>, model: &str, id: &str) -> Value {
    json!({"kind":"invalidate","streams":streams,"record":reference(model,id)})
}
fn external(
    backend: &Backend,
    changes: Vec<Value>,
    declarations: Vec<Value>,
) -> axton_server::Result<Value> {
    run(axton_server::settle_external(
        &config(),
        &json!({"changes":changes,"declarations":declarations}),
        backend,
    ))
}
fn publishes(backend: &Backend) -> Vec<(String, String, u64)> {
    backend.publishes()
}
fn succeeded(receipt: &Value) {
    assert_eq!(receipt["rejections"], json!([]), "{receipt}");
}
/// Restore saved removal evidence directly, while ordinary tracking/invalidation
/// still passes through real settlement. This is a fixture, never a host verb.
fn historical_settle(backend: &Backend, changes: Vec<Value>, intents: Vec<Value>) {
    let mut tracks = vec![];
    for i in intents {
        if i["kind"] == "remove" {
            backend.saved_removal(
                i["scope"].as_str().unwrap(),
                i["record"]["model"].as_str().unwrap(),
                i["record"]["identity"]["id"].as_str().unwrap(),
            );
        } else {
            tracks.push(i);
        }
    }
    external(backend, changes, tracks).unwrap();
}
fn limits_page() -> usize {
    axton_core::limits::PULL_CHANGES
}
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
/// `(from, to, head)` of one Scope's range in a delta page.
fn range(page: &axton_core::PullPage, scope: &str) -> (u64, u64, u64) {
    let range = &page.cursors[scope];
    (range.from, range.to, range.head)
}
fn adds(scope: &str, ids: &[String]) -> Vec<Value> {
    ids.iter().map(|id| add(scope, "Todo", id)).collect()
}
fn removes(scope: &str, ids: &[String]) -> Vec<Value> {
    ids.iter().map(|id| remove(scope, "Todo", id)).collect()
}
/// Todo rows `prefix000..`, enrolled in `scope` by one settlement: one
/// position each, in canonical key order, all at stamp 1.
fn published(backend: &Backend, scope: &str, prefix: &str, count: usize) -> Vec<String> {
    let ids: Vec<String> = (0..count).map(|i| format!("{prefix}{i:03}")).collect();
    for id in &ids {
        backend.seed("Todo", id, todo_row(id, "v1"), None);
    }
    historical_settle(backend, vec![], adds(scope, &ids));
    ids
}

#[test]
fn removing_every_remaining_row_yields_a_terminal_advancing_page() {
    let backend = Backend::new();
    let ids = published(&backend, "A", "t", 3);
    assert_eq!(backend.head("A"), 3);
    historical_settle(&backend, vec![], removes("A", &ids));
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
    historical_settle(&backend, vec![], removes("A", &ids[..60]));
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
fn a_record_removed_then_touched_elsewhere_is_not_exposed_through_its_old_scope() {
    let backend = Backend::new();
    backend.seed("Todo", "t", todo_row("t", "shared"), None);
    historical_settle(
        &backend,
        vec![],
        vec![add("A", "Todo", "t"), add("B", "Todo", "t")],
    );
    historical_settle(&backend, vec![], vec![remove("A", "Todo", "t")]);
    // A later change, published through the Scope it still belongs to.
    backend.seed("Todo", "t", todo_row("t", "after removal"), None);
    historical_settle(&backend, vec![reference("Todo", "t")], vec![]);
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
    historical_settle(
        &backend,
        vec![],
        vec![add("A", "Todo", "t"), add("A", "Todo", "u")],
    );
    backend.seed("Todo", "t", todo_row("t", "v2"), None);
    historical_settle(&backend, vec![reference("Todo", "t")], vec![]);
    assert_eq!(backend.invalidation("A", "Todo", "t"), Some((3, 2)));
    // Remove, then re-add in a separate settlement: not a change.
    historical_settle(&backend, vec![], vec![remove("A", "Todo", "t")]);
    assert_eq!(backend.positions("A")[1..], [(4, "t".into(), "remove")]);
    backend.clear_log();
    historical_settle(&backend, vec![], vec![add("A", "Todo", "t")]);
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
    historical_settle(
        &backend,
        vec![],
        ["e1", "e2", "m", "x"]
            .iter()
            .map(|id| add("A", "Todo", id))
            .collect(),
    );
    let origin = backend.head("A");
    assert_eq!(origin, 4);
    historical_settle(&backend, vec![], vec![remove("A", "Todo", "m")]);
    historical_settle(&backend, vec![], vec![add("A", "Todo", "m")]);
    historical_settle(&backend, vec![], vec![remove("A", "Todo", "x")]);
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
    historical_settle(&backend, vec![], vec![add("A", "Todo", "t")]);
    // The business row is deleted and the change declared: no membership edit.
    backend.with(|s| s.tables.rows.remove(&record("Todo", "t")));
    historical_settle(&backend, vec![reference("Todo", "t")], vec![]);
    assert_eq!(backend.members("Todo", "t"), ["A"]);
    let page = pull(&backend, &[("A", 1)]);
    assert_eq!(delivered(&page.changes), [("t".into(), 2, Value::Null)]);
    // The same identity recreated reaches the same membership, unasked.
    backend.seed("Todo", "t", todo_row("t", "again"), None);
    historical_settle(&backend, vec![reference("Todo", "t")], vec![]);
    let page = pull(&backend, &[("A", 2)]);
    assert_eq!(delivered(&page.changes), [("t".into(), 3, title("again"))]);
    // Deleting and removing in one settlement: the final relationship wins,
    // so A is told of the removal, not the deletion.
    backend.with(|s| s.tables.rows.remove(&record("Todo", "t")));
    historical_settle(
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
fn saved_removal_is_positioned_and_fabricates_no_deletion() {
    let backend = Backend::new();
    backend.seed("Todo", "t", todo_row("t", "kept"), None);
    historical_settle(&backend, vec![], vec![add("A", "Todo", "t")]);
    let before = backend.tables();
    backend.clear_log();
    historical_settle(&backend, vec![], vec![remove("A", "Todo", "t")]);
    assert_eq!(publishes(&backend), []);
    assert_eq!(backend.positions("A"), [(2, "t".into(), "remove")]);
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
#[test]
fn selected_invalidation_advances_without_enrolling_and_inferred_changes_are_global() {
    let backend = Backend::new();
    backend.seed("Todo", "t", todo_row("t", "old"), Some(7));
    backend.enroll("A", "Todo", "t", 120);
    backend.enroll("B", "Todo", "t", 45);
    external(
        &backend,
        vec![],
        vec![invalidate(Some(&["A"]), "Todo", "t")],
    )
    .unwrap();
    assert_eq!(backend.stamp("Todo", "t"), Some(8));
    assert_eq!(publishes(&backend), [("A".into(), "t".into(), 8)]);
    assert_eq!(backend.head("B"), 46);
    backend.clear_log();
    external(
        &backend,
        vec![],
        vec![invalidate(Some(&["C"]), "Todo", "t")],
    )
    .unwrap();
    assert_eq!(backend.stamp("Todo", "t"), Some(9));
    assert_eq!(backend.members("Todo", "t"), ["A", "B"]);
    assert!(publishes(&backend).is_empty());
    backend.clear_log();
    backend.script(
        "Edit",
        effects(vec![], vec![invalidate(Some(&["A"]), "Todo", "t")]),
    );
    let receipt = push(
        &backend,
        1,
        json!({"Todo":1}),
        vec![edit(1, 1, "Edit", "t", "changed")],
    );
    succeeded(&receipt);
    assert_eq!(authority(&receipt), [("Todo".into(), "t".into(), 10)]);
    assert_eq!(
        publishes(&backend),
        [("A".into(), "t".into(), 10), ("B".into(), "t".into(), 10)]
    );
}
#[test]
fn tracks_and_invalidations_combine_independently_of_order_once_per_pair() {
    let declarations = vec![
        invalidate(Some(&["A"]), "Todo", "t"),
        add("C", "Todo", "t"),
        invalidate(Some(&["C", "A"]), "Todo", "t"),
    ];
    let mut result = vec![];
    for reverse in [false, true] {
        let backend = Backend::new();
        backend.seed("Todo", "t", todo_row("t", "old"), Some(7));
        backend.enroll("A", "Todo", "t", 120);
        backend.enroll("B", "Todo", "t", 45);
        let mut declarations = declarations.clone();
        if reverse {
            declarations.reverse();
        }
        external(&backend, vec![], declarations).unwrap();
        assert_eq!(backend.stamp("Todo", "t"), Some(8));
        assert_eq!(backend.count("guardRecords"), 1);
        let HostRequest::GuardRecords { records } = backend
            .log()
            .into_iter()
            .find(|r| matches!(r, HostRequest::GuardRecords { .. }))
            .unwrap()
        else {
            unreachable!()
        };
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].mode, GuardMode::Advance);
        assert_eq!(
            publishes(&backend),
            [("A".into(), "t".into(), 8), ("C".into(), "t".into(), 8)]
        );
        assert_eq!(backend.head("B"), 46);
        assert_eq!(backend.deltas().len(), 2);
        result.push(backend.tables());
    }
    assert_eq!(result[0], result[1]);
}
#[test]
fn tracking_is_idempotent_and_new_tracking_inherits_authority() {
    let backend = Backend::new();
    backend.seed("Todo", "t", todo_row("t", "old"), Some(7));
    backend.enroll("A", "Todo", "t", 2);
    let before = backend.tables();
    external(
        &backend,
        vec![],
        vec![add("A", "Todo", "t"), add("A", "Todo", "t")],
    )
    .unwrap();
    assert_eq!(backend.tables(), before);
    assert!(publishes(&backend).is_empty());
    backend.clear_log();
    external(&backend, vec![], vec![add("B", "Todo", "t")]).unwrap();
    assert_eq!(backend.stamp("Todo", "t"), Some(7));
    assert_eq!(publishes(&backend), [("B".into(), "t".into(), 7)]);
    backend.clear_log();
    external(&backend, vec![], vec![add("C", "Todo", "new")]).unwrap();
    assert_eq!(backend.stamp("Todo", "new"), Some(1));
    assert_eq!(backend.count("guardRecords"), 1);
}
#[test]
fn global_dominates_selected_and_reaches_new_tracking() {
    let backend = Backend::new();
    backend.seed("Todo", "t", todo_row("t", "old"), Some(7));
    backend.enroll("A", "Todo", "t", 0);
    backend.enroll("B", "Todo", "t", 0);
    external(
        &backend,
        vec![],
        vec![
            invalidate(Some(&["A"]), "Todo", "t"),
            invalidate(None, "Todo", "t"),
            add("C", "Todo", "t"),
            invalidate(Some(&["B"]), "Todo", "t"),
        ],
    )
    .unwrap();
    assert_eq!(backend.stamp("Todo", "t"), Some(8));
    assert_eq!(
        publishes(&backend),
        [
            ("A".into(), "t".into(), 8),
            ("B".into(), "t".into(), 8),
            ("C".into(), "t".into(), 8)
        ]
    );
}
#[test]
fn unheld_invalidation_advances_and_empty_selection_is_noop() {
    let backend = Backend::new();
    external(&backend, vec![], vec![invalidate(None, "Todo", "t")]).unwrap();
    assert_eq!(backend.stamp("Todo", "t"), Some(1));
    assert_eq!(backend.count("applyStreamMembers"), 0);
    backend.clear_log();
    let before = backend.tables();
    external(&backend, vec![], vec![invalidate(Some(&[]), "Todo", "t")]).unwrap();
    assert_eq!(backend.tables(), before);
    assert!(backend.log().is_empty());
}
#[test]
fn bulk_settlement_round_trips_are_constant_and_guards_are_canonical() {
    let backend = Backend::new();
    let mut declarations = vec![];
    for n in (0..1100).rev() {
        declarations.push(add(&format!("s{n:04}"), "Todo", &format!("t{n:04}")));
    }
    declarations.push(add("Z", "Project", "p"));
    external(&backend, vec![], declarations).unwrap();
    assert_eq!(
        backend.ops(),
        [
            "readTracking",
            "lockStreams",
            "guardRecords",
            "readTracking",
            "applyStreamMembers"
        ]
    );
    let HostRequest::GuardRecords { records } = &backend.log()[2] else {
        panic!("guard request")
    };
    assert_eq!(records.len(), 1101);
    assert_eq!(records[0].model, "Project");
    assert!(records.iter().all(|r| r.mode == GuardMode::Ensure));
    let HostRequest::LockStreams { streams } = &backend.log()[1] else {
        panic!("stream locks")
    };
    assert!(streams.windows(2).all(|p| p[0] < p[1]));
    assert_eq!(backend.deltas().len(), 1101);
}
#[test]
fn new_global_holder_after_locks_is_a_retryable_conflict_without_late_lock() {
    let backend = Backend::new();
    backend.seed("Todo", "t", todo_row("t", "old"), Some(7));
    backend.enroll("A", "Todo", "t", 0);
    backend.intrude_at_lock("B", "Todo", "t");
    let error = external(&backend, vec![], vec![invalidate(None, "Todo", "t")]).unwrap_err();
    assert_eq!(error.code, "transaction.conflict");
    assert_eq!(backend.count("lockStreams"), 1);
    assert_eq!(backend.count("applyStreamMembers"), 0);
}
#[test]
fn malformed_tracking_or_positions_abort_and_savepoint_rejection_rolls_back() {
    for (op, tamper) in [
        (
            "readTracking",
            (|_: Value| json!([{"stream":"unrelated","model":"Todo","identityKey":"{\"id\":\"other\"}"}]))
                as fn(Value) -> Value,
        ),
        ("guardRecords", (|_: Value| json!([])) as fn(Value) -> Value),
        (
            "applyStreamMembers",
            (|_: Value| json!([])) as fn(Value) -> Value,
        ),
    ] {
        let backend = Backend::new();
        backend.seed("Todo", "t", todo_row("t", "old"), Some(7));
        backend.enroll("A", "Todo", "t", 0);
        backend.tamper(op, tamper);
        assert_eq!(
            external(&backend, vec![], vec![invalidate(None, "Todo", "t")])
                .unwrap_err()
                .code,
            "host.invalid"
        );
    }
    let backend = Backend::new();
    backend.seed("Todo", "t", todo_row("t", "old"), Some(7));
    let before = backend.tables();
    backend.refuse_load("Todo", "t");
    backend.script("Edit", effects(vec![], vec![add("A", "Todo", "t")]));
    let receipt = push(
        &backend,
        1,
        json!({"Todo":1}),
        vec![edit(1, 1, "Edit", "t", "changed")],
    );
    assert_eq!(receipt["rejections"][0]["code"], "todo.forbidden");
    let mut after = backend.tables();
    assert_eq!(after.calls.len(), 1, "rejection is saved");
    after.calls.clear();
    assert_eq!(after, before);
    assert_eq!(backend.count("rollback"), 1);
}
#[test]
fn claims_only_name_declared_tracking_and_saved_call_replays_exactly() {
    let backend = Backend::new();
    backend.seed("Todo", "t", todo_row("t", "old"), Some(7));
    backend.enroll("A", "Todo", "t", 0);
    backend.script(
        "Edit",
        effects(
            vec![],
            vec![add("B", "Todo", "t"), invalidate(None, "Todo", "t")],
        ),
    );
    let args = vec![edit(1, 1, "Edit", "t", "changed")];
    let first = push(&backend, 1, json!({"Todo":1}), args.clone());
    succeeded(&first);
    assert_eq!(
        first["memberships"],
        json!([{"stream":"B","cursor":1,"model":"Todo","identity":{"id":"t"}}])
    );
    let before = backend.tables();
    backend.clear_log();
    let replay = push(&backend, 1, json!({"Todo":1}), args);
    assert_eq!(replay, first);
    assert_eq!(backend.tables(), before);
    assert_eq!(backend.count("guardRecords"), 0);
    assert_eq!(backend.count("load"), 0);
}
#[test]
fn retired_tags_and_withdrawal_are_rejected_before_host_calls() {
    for intent in [
        remove("A", "Todo", "t"),
        add_tagged("A", "Todo", "t", &["x"]),
        json!({"kind":"select","scope":"A","predicate":{},"action":{"kind":"remove"}}),
    ] {
        let backend = Backend::new();
        assert_eq!(
            external(&backend, vec![], vec![intent]).unwrap_err().code,
            "publish.invalid"
        );
        assert!(backend.log().is_empty());
    }
}
