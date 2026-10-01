//! Guarantees L1, L4, L5 on the simulation (L2, L3 are store contracts in crates/sqlite).
use axton_sim::{
    Action, MutationSpec, Sim,
    schema::{comment_key, entry_key},
};

/// L1: the merged view shows every pending edit in order, before and after settlement
/// of an earlier one.
#[test]
fn l1_merged_view_shows_pending_edits_in_order() {
    let mut sim = Sim::new(41, 1);
    sim.apply(Action::Subscribe {
        client: 0,
        scope: "a".into(),
    })
    .unwrap();
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::CreateEntry {
            id: "e1".into(),
            text: "one".into(),
        },
    })
    .unwrap();
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::Edit {
            id: "e1".into(),
            text: "two".into(),
        },
    })
    .unwrap();
    assert_eq!(sim.read_text(0, &entry_key("e1")).as_deref(), Some("two"));
    sim.apply(Action::Freeze { client: 0 }).unwrap();
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::Edit {
            id: "e1".into(),
            text: "three".into(),
        },
    })
    .unwrap();
    sim.drain();
    sim.apply(Action::Pull { client: 0 }).unwrap();
    sim.drain();
    assert_eq!(sim.read_text(0, &entry_key("e1")).as_deref(), Some("three"));
    sim.settle();
    assert_eq!(sim.read_text(0, &entry_key("e1")).as_deref(), Some("three"));
    sim.check().unwrap();
}

/// L4: a direct write is never pushed and is not undone by a later rejection of a
/// mutation on the same row.
#[test]
fn l4_direct_write_is_never_pushed_and_survives_rejection() {
    let mut sim = Sim::new(42, 1);
    sim.apply(Action::Subscribe {
        client: 0,
        scope: "a".into(),
    })
    .unwrap();
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::CreateEntry {
            id: "e1".into(),
            text: "base".into(),
        },
    })
    .unwrap();
    sim.settle();
    let calls = sim.host.handler_calls();
    sim.apply(Action::Direct {
        client: 0,
        key: "Entry:e1".into(),
        text: "local only".into(),
    })
    .unwrap();
    sim.settle();
    assert_eq!(sim.host.handler_calls(), calls, "nothing was pushed");
    assert_eq!(sim.host.state(&entry_key("e1")).unwrap()["text"], "base");
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::Edit {
            id: "e1".into(),
            text: "rejected".into(),
        },
    })
    .unwrap();
    sim.apply(Action::RejectNext {
        code: "entry.denied".into(),
    })
    .unwrap();
    sim.settle();
    assert_eq!(
        sim.read_text(0, &entry_key("e1")).as_deref(),
        Some("local only"),
        "direct write kept, rejected edit gone"
    );
    // The invariant "no pending means converged" is deliberately not satisfied here:
    // a direct write diverges from the server by design (N4). Skip check().
}

/// L4: a direct write keeps its place after the pending edits written before
/// it. Rejecting the earlier of them neither undoes the direct write nor lets
/// the later pending edit replay over it; the later edit's accepted authority
/// may then replace it.
#[test]
fn l4_direct_write_keeps_its_place_above_earlier_pending_edits() {
    let mut sim = Sim::new(44, 1);
    sim.apply(Action::Subscribe {
        client: 0,
        scope: "a".into(),
    })
    .unwrap();
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::CreateEntry {
            id: "e1".into(),
            text: "base".into(),
        },
    })
    .unwrap();
    sim.settle();
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::Edit {
            id: "e1".into(),
            text: "one".into(),
        },
    })
    .unwrap();
    sim.apply(Action::Freeze { client: 0 }).unwrap();
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::Edit {
            id: "e1".into(),
            text: "two".into(),
        },
    })
    .unwrap();
    sim.apply(Action::Direct {
        client: 0,
        key: "Entry:e1".into(),
        text: "local".into(),
    })
    .unwrap();
    sim.apply(Action::RejectNext {
        code: "entry.denied".into(),
    })
    .unwrap();
    sim.drain();
    assert_eq!(
        sim.read_text(0, &entry_key("e1")).as_deref(),
        Some("local"),
        "the rejection removes only the first edit"
    );
    sim.settle();
    assert_eq!(
        sim.read_text(0, &entry_key("e1")).as_deref(),
        Some("two"),
        "the accepted edit's authority replaces the direct write"
    );
    sim.check().unwrap();
}

/// L4: a direct write on a row whose create is still pending has no rollback base
/// to advance. If the create is rejected, the row goes, direct write included
/// (issue #33).
#[test]
fn l4_direct_write_on_pending_create_goes_with_the_rejected_create() {
    let mut sim = Sim::new(1, 1);
    sim.apply(Action::Subscribe {
        client: 0,
        scope: "a".into(),
    })
    .unwrap();
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::CreateEntry {
            id: "e1".into(),
            text: "orig".into(),
        },
    })
    .unwrap();
    sim.apply(Action::Freeze { client: 0 }).unwrap();
    sim.apply(Action::Direct {
        client: 0,
        key: "Entry:e1".into(),
        text: "direct".into(),
    })
    .unwrap();
    assert_eq!(
        sim.read_text(0, &entry_key("e1")).as_deref(),
        Some("direct"),
        "the direct write is visible while the create is in flight"
    );
    sim.apply(Action::RejectNext {
        code: "sim.denied".into(),
    })
    .unwrap();
    sim.drain();
    assert_eq!(
        sim.read_text(0, &entry_key("e1")),
        None,
        "the rejected create takes the direct write with it"
    );
    sim.check().unwrap();
}

/// L4 with D2: a duplicate of a page the client already applied is stale, so it does
/// not undo a direct write made in between; the direct write still diverges from the
/// server by design.
#[test]
fn l4_stale_duplicate_page_does_not_undo_a_direct_write() {
    let mut sim = Sim::new(1, 1);
    sim.apply(Action::Subscribe {
        client: 0,
        scope: "a".into(),
    })
    .unwrap();
    sim.apply(Action::ServerChange {
        key: "Entry:e1".into(),
        text: Some("server".into()),
        scopes: vec!["a".into()],
    })
    .unwrap();
    sim.apply(Action::Pull { client: 0 }).unwrap();
    sim.apply(Action::Deliver).unwrap(); // server answers the pull
    sim.apply(Action::Duplicate).unwrap(); // the page is now queued twice
    sim.apply(Action::Deliver).unwrap(); // first copy applies
    assert_eq!(
        sim.read_text(0, &entry_key("e1")).as_deref(),
        Some("server")
    );
    sim.apply(Action::Direct {
        client: 0,
        key: "Entry:e1".into(),
        text: "direct".into(),
    })
    .unwrap();
    sim.apply(Action::Deliver).unwrap(); // second copy is stale
    assert_eq!(
        sim.read_text(0, &entry_key("e1")).as_deref(),
        Some("direct"),
        "a stale page does not outrank a direct write"
    );
    sim.check().unwrap();
}

/// L5: deleting an Entry locally cascades to its Comments; the server does the same;
/// both sides agree after settlement.
#[test]
fn l5_delete_cascades_locally_and_on_the_server() {
    let mut sim = Sim::new(43, 1);
    sim.apply(Action::Subscribe {
        client: 0,
        scope: "a".into(),
    })
    .unwrap();
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::CreateEntry {
            id: "e1".into(),
            text: "p".into(),
        },
    })
    .unwrap();
    sim.settle();
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::CreateComment {
            id: "c1".into(),
            entry: "e1".into(),
            text: "c".into(),
        },
    })
    .unwrap();
    sim.settle();
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::DeleteEntry { id: "e1".into() },
    })
    .unwrap();
    assert_eq!(sim.read_text(0, &comment_key("c1")), None, "local cascade");
    sim.settle();
    assert!(
        sim.host.state(&comment_key("c1")).is_none(),
        "server cascade"
    );
    assert_eq!(sim.read_text(0, &comment_key("c1")), None);
    sim.check().unwrap();
}
