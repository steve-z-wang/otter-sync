//! Authority on the simulation: a push completes from its receipt, scope pages and
//! receipts carry the same stamps, and neither can regress the other.
use axton_sim::{Action, MutationSpec, Sim, schema::entry_key};

fn setup(seed: u64) -> Sim {
    let mut sim = Sim::new(seed, 1);
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
    sim
}

fn edit(sim: &mut Sim, client: usize, text: &str) {
    sim.apply(Action::Enqueue {
        client,
        mutation: MutationSpec::Edit {
            id: "e1".into(),
            text: text.into(),
        },
    })
    .unwrap();
}

/// The value the server stored replaces the optimistic value through the receipt
/// alone; a later pending edit replays on top of that base.
#[test]
fn a1_server_value_overrides_optimism_and_later_edits_replay() {
    let mut sim = setup(31);
    edit(&mut sim, 0, "mine");
    // The server "normalizes" by storing something else for the same mutation.
    sim.host.uppercase_next();
    sim.apply(Action::Freeze { client: 0 }).unwrap();
    sim.apply(Action::Deliver).unwrap(); // executed: the server holds "MINE"
    assert_eq!(sim.host.state(&entry_key("e1")).unwrap()["text"], "MINE");
    edit(&mut sim, 0, "later");
    assert_eq!(sim.read_text(0, &entry_key("e1")).as_deref(), Some("later"));
    sim.apply(Action::Deliver).unwrap(); // receipt for batch 2 completes it
    assert_eq!(
        sim.client(0).pending_count().unwrap(),
        1,
        "only the later edit"
    );
    assert_eq!(
        sim.read_text(0, &entry_key("e1")).as_deref(),
        Some("later"),
        "pending edit replays on the new base"
    );
    let base = sim
        .client(0)
        .read_sql("SELECT text FROM axton_before_Entry", &[])
        .unwrap();
    assert_eq!(
        base[0]["text"], "MINE",
        "the base beneath it is the server's value, from the receipt"
    );
    assert_eq!(sim.client(0).record_stamp(&entry_key("e1")).unwrap(), 2);
    sim.settle();
    assert_eq!(sim.read_text(0, &entry_key("e1")).as_deref(), Some("later"));
    assert_eq!(sim.conflicts, 0);
    sim.check().unwrap();
}

/// A2: a page whose scope range is already covered is stale and does not move the cursor back;
/// a page ahead is refused.
#[test]
fn a2_pages_apply_only_in_cursor_order() {
    let mut sim = setup(32);
    sim.apply(Action::ServerChange {
        key: "Entry:e1".into(),
        text: Some("v2".into()),
        scopes: vec!["a".into()],
    })
    .unwrap();
    sim.apply(Action::Pull { client: 0 }).unwrap();
    sim.apply(Action::Deliver).unwrap();
    sim.apply(Action::Duplicate).unwrap(); // the same page twice
    sim.apply(Action::Deliver).unwrap();
    assert_eq!(sim.client(0).cursor("a").unwrap(), Some(2));
    sim.apply(Action::Deliver).unwrap(); // stale duplicate
    assert_eq!(sim.client(0).cursor("a").unwrap(), Some(2));
    sim.check().unwrap();
}

/// The receipt and the scope page for the same change carry the same stamp and
/// content. Whichever arrives first, the receipt completes the batch, the page moves
/// the cursor, nothing conflicts and the row is the server's.
#[test]
fn reordered_receipt_and_page_agree_in_either_order() {
    for page_first in [false, true] {
        let mut sim = setup(33);
        edit(&mut sim, 0, "x");
        sim.apply(Action::Freeze { client: 0 }).unwrap();
        sim.apply(Action::Deliver).unwrap(); // executed, receipt queued
        sim.apply(Action::Pull { client: 0 }).unwrap();
        sim.apply(Action::Hold).unwrap(); // receipt to the back
        sim.apply(Action::Deliver).unwrap(); // pull request -> page queued
        if page_first {
            sim.apply(Action::Hold).unwrap(); // receipt to the back again, page first
        }
        sim.apply(Action::Deliver).unwrap();
        let pending_after_first = sim.client(0).pending_count().unwrap();
        if page_first {
            assert_eq!(pending_after_first, 1, "a page never completes a push");
            assert_eq!(sim.client(0).cursor("a").unwrap(), Some(2));
        } else {
            assert_eq!(pending_after_first, 0, "the receipt completes it alone");
            assert_eq!(sim.client(0).cursor("a").unwrap(), Some(1));
        }
        sim.apply(Action::Deliver).unwrap();
        assert_eq!(sim.client(0).pending_count().unwrap(), 0);
        assert_eq!(sim.client(0).cursor("a").unwrap(), Some(2));
        assert_eq!(sim.read_text(0, &entry_key("e1")).as_deref(), Some("x"));
        assert_eq!(sim.client(0).record_stamp(&entry_key("e1")).unwrap(), 2);
        assert_eq!(sim.host.stamp(&entry_key("e1")), 2);
        assert_eq!(sim.conflicts, 0, "page_first {page_first}");
        sim.check().unwrap();
    }
}

/// HTTP-only completion: a client that follows no scope at all still completes
/// its push from the receipt, with the server's row and the server's stamp.
#[test]
fn a_push_completes_from_its_receipt_with_zero_subscriptions() {
    let mut sim = Sim::new(34, 1);
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::CreateEntry {
            id: "e9".into(),
            text: "nowhere".into(),
        },
    })
    .unwrap();
    assert!(sim.client(0).subscriptions().unwrap().is_empty());
    sim.host.uppercase_next();
    sim.apply(Action::Freeze { client: 0 }).unwrap();
    sim.apply(Action::Deliver).unwrap(); // push
    sim.apply(Action::Deliver).unwrap(); // receipt
    assert_eq!(sim.client(0).pending_count().unwrap(), 0);
    assert_eq!(sim.client(0).last_completed_push().unwrap(), 1);
    assert_eq!(
        sim.read_text(0, &entry_key("e9")).as_deref(),
        Some("NOWHERE"),
        "the row is the server's, not the optimism"
    );
    assert_eq!(sim.host.state(&entry_key("e9")).unwrap()["text"], "NOWHERE");
    assert_eq!(
        sim.client(0).record_stamp(&entry_key("e9")).unwrap(),
        sim.host.stamp(&entry_key("e9"))
    );
    assert_eq!(sim.client(0).before_image_count().unwrap(), 0);
    assert!(
        sim.client(0).freeze().unwrap().is_none(),
        "nothing is re-sent"
    );
    sim.check().unwrap();
}

/// A handler that publishes nowhere (a record with no scope membership) is a
/// legal outcome: the change is stamped, read back and returned; no scope moves.
#[test]
fn a_change_published_to_no_scope_still_completes() {
    let mut sim = Sim::new(35, 1);
    sim.apply(Action::Subscribe {
        client: 0,
        scope: "a".into(),
    })
    .unwrap();
    sim.host.set_membership(&entry_key("e9"), &[]);
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::CreateEntry {
            id: "e9".into(),
            text: "quiet".into(),
        },
    })
    .unwrap();
    sim.settle();
    assert_eq!(sim.client(0).pending_count().unwrap(), 0);
    assert_eq!(sim.read_text(0, &entry_key("e9")).as_deref(), Some("quiet"));
    assert_eq!(sim.host.head("a"), 0, "no scope was told");
    assert_eq!(sim.client(0).record_stamp(&entry_key("e9")).unwrap(), 1);
    sim.check().unwrap();
}

/// A2: a page pulled from scope "a" before an Unsubscribe/Subscribe cycle can
/// still be in flight when the resubscribe resets the scope's cursor to 0; it is
/// dropped as stale, a page from a previous subscription, rather than treated as a
/// gap or applied against the reset cursor. The nine-action repro from issue #32.
#[test]
fn a2_page_from_a_previous_subscription_is_stale_not_a_gap() {
    let mut sim = Sim::new(36, 1);
    sim.apply(Action::Subscribe {
        client: 0,
        scope: "a".into(),
    })
    .unwrap();
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::CreateEntry {
            id: "e1".into(),
            text: "1".into(),
        },
    })
    .unwrap();
    sim.apply(Action::Freeze { client: 0 }).unwrap();
    sim.drain();
    sim.apply(Action::Pull { client: 0 }).unwrap();
    sim.drain();
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::CreateEntry {
            id: "e2".into(),
            text: "2".into(),
        },
    })
    .unwrap();
    sim.apply(Action::Freeze { client: 0 }).unwrap();
    sim.drain();
    sim.apply(Action::Pull { client: 0 }).unwrap();
    sim.apply(Action::Unsubscribe {
        client: 0,
        scope: "a".into(),
    })
    .unwrap();
    sim.apply(Action::Subscribe {
        client: 0,
        scope: "a".into(),
    })
    .unwrap();
    assert!(
        sim.apply(Action::Deliver).is_ok(),
        "the client must drop the stale page rather than error"
    );
    assert_eq!(sim.client(0).cursor("a").unwrap(), Some(0));
    assert_eq!(
        sim.read_text(0, &entry_key("e2")).as_deref(),
        Some("2"),
        "the rows delivered before the cycle are retained"
    );
    sim.check().unwrap();
}

/// Completion never waits for a scope: two batches on scopes the client does
/// not pull (one it follows, one it does not) both complete on their receipts, in
/// sequence, while every cursor stays where it was.
#[test]
fn batches_complete_on_their_receipts_without_any_page() {
    let mut sim = Sim::new(37, 1);
    sim.apply(Action::Subscribe {
        client: 0,
        scope: "slow".into(),
    })
    .unwrap();
    sim.host.set_membership(&entry_key("s"), &["slow"]);
    sim.host.set_membership(&entry_key("n"), &["other"]);
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::CreateEntry {
            id: "s".into(),
            text: "1".into(),
        },
    })
    .unwrap();
    sim.apply(Action::Freeze { client: 0 }).unwrap();
    sim.apply(Action::Deliver).unwrap(); // batch 1 executed
    sim.apply(Action::Deliver).unwrap(); // receipt 1 completes it
    assert_eq!(sim.client(0).pending_count().unwrap(), 0);
    assert_eq!(sim.client(0).last_completed_push().unwrap(), 1);
    assert_eq!(
        sim.client(0).cursor("slow").unwrap(),
        Some(0),
        "no page was pulled"
    );
    assert_eq!(sim.host.head("slow"), 1);
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::CreateEntry {
            id: "n".into(),
            text: "2".into(),
        },
    })
    .unwrap();
    sim.apply(Action::Freeze { client: 0 }).unwrap();
    sim.apply(Action::Deliver).unwrap(); // batch 2 executed
    sim.apply(Action::Deliver).unwrap(); // receipt 2 completes it
    assert_eq!(sim.client(0).pending_count().unwrap(), 0);
    assert_eq!(sim.client(0).last_completed_push().unwrap(), 2);
    assert_eq!(sim.read_text(0, &entry_key("n")).as_deref(), Some("2"));
    assert_eq!(sim.client(0).record_stamp(&entry_key("n")).unwrap(), 1);
    sim.check().unwrap();
    sim.settle();
    assert_eq!(sim.client(0).cursor("slow").unwrap(), Some(1));
    assert_eq!(sim.conflicts, 0);
    sim.check().unwrap();
}

/// Deletion through a receipt: the deleting client's row goes and its stamp is
/// retained; a subscribed peer receives the same deletion at the same stamp through
/// the scope.
#[test]
fn deletion_completes_from_the_receipt_and_reaches_a_peer_at_the_same_stamp() {
    let mut sim = Sim::new(38, 2);
    for i in 0..2 {
        sim.apply(Action::Subscribe {
            client: i,
            scope: "a".into(),
        })
        .unwrap();
    }
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::CreateEntry {
            id: "e1".into(),
            text: "doomed".into(),
        },
    })
    .unwrap();
    sim.settle();
    assert_eq!(
        sim.read_text(1, &entry_key("e1")).as_deref(),
        Some("doomed")
    );
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::DeleteEntry { id: "e1".into() },
    })
    .unwrap();
    sim.apply(Action::Freeze { client: 0 }).unwrap();
    sim.apply(Action::Deliver).unwrap();
    sim.apply(Action::Deliver).unwrap(); // the receipt carries the null state
    assert_eq!(sim.client(0).pending_count().unwrap(), 0);
    assert_eq!(sim.read_text(0, &entry_key("e1")), None);
    let stamp = sim.host.stamp(&entry_key("e1"));
    assert_eq!(stamp, 2);
    assert_eq!(
        sim.client(0).record_stamp(&entry_key("e1")).unwrap(),
        stamp,
        "the deletion's stamp is retained as evidence"
    );
    assert_eq!(
        sim.read_text(1, &entry_key("e1")).as_deref(),
        Some("doomed")
    );
    sim.apply(Action::Pull { client: 1 }).unwrap();
    sim.drain();
    assert_eq!(sim.read_text(1, &entry_key("e1")), None);
    assert_eq!(sim.client(1).record_stamp(&entry_key("e1")).unwrap(), stamp);
    assert_eq!(sim.conflicts, 0);
    sim.check().unwrap();
}

/// Restart keeps retained rows: a record delivered by a scope the client has
/// since left survives a crash, stamp included.
#[test]
fn restart_keeps_rows_retained_after_unsubscribe() {
    let mut sim = setup(39);
    sim.apply(Action::Unsubscribe {
        client: 0,
        scope: "a".into(),
    })
    .unwrap();
    sim.apply(Action::Crash { client: 0 }).unwrap();
    sim.apply(Action::Restart { client: 0 }).unwrap();
    assert!(sim.client(0).subscriptions().unwrap().is_empty());
    assert_eq!(sim.read_text(0, &entry_key("e1")).as_deref(), Some("base"));
    assert_eq!(sim.client(0).record_stamp(&entry_key("e1")).unwrap(), 1);
    assert_eq!(sim.client(0).last_completed_push().unwrap(), 1);
    sim.check().unwrap();
}
