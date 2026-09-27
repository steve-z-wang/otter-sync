//! Distribution on the simulation: channels deliver updates by stamp; records
//! remain local once delivered, whatever happens to the channel.
use axton_sim::{
    Action, MutationSpec, Sim,
    schema::{comment_key, entry_key},
};

fn subscribe(sim: &mut Sim, client: usize, channels: &[&str]) {
    for c in channels {
        sim.apply(Action::Subscribe {
            client,
            channel: c.to_string(),
        })
        .unwrap();
    }
}
fn change(sim: &mut Sim, key: &str, text: Option<&str>, channels: &[&str]) {
    sim.apply(Action::ServerChange {
        key: key.into(),
        text: text.map(str::to_string),
        channels: channels.iter().map(|c| c.to_string()).collect(),
    })
    .unwrap();
}
fn pull(sim: &mut Sim, client: usize, _channel: &str) {
    sim.apply(Action::Pull { client }).unwrap();
}
fn move_to(sim: &mut Sim, key: &str, channels: &[&str]) {
    sim.apply(Action::MoveMembership {
        key: key.into(),
        channels: channels.iter().map(|c| c.to_string()).collect(),
    })
    .unwrap();
}
/// One application transaction for `key`: `touch` is `Some(Some(text))` to
/// write text, `Some(None)` to delete, `None` for no business change; then the
/// ordered membership intents, settled by the engine.
fn declare(sim: &mut Sim, key: &str, touch: Option<Option<&str>>, memberships: &[(&str, bool)]) {
    sim.apply(Action::Declare {
        key: key.into(),
        touch: touch.map(|text| text.map(str::to_string)),
        memberships: memberships
            .iter()
            .map(|(channel, present)| (channel.to_string(), *present))
            .collect(),
    })
    .unwrap();
}
fn stamp_rows(sim: &mut Sim, client: usize) -> Vec<serde_json::Value> {
    sim.client(client)
        .read_sql("SELECT stamp FROM axton_record WHERE model='Entry'", &[])
        .unwrap()
}

/// D1: two clients subscribed to one channel converge on every record after a mix
/// of local edits from both sides.
#[test]
fn d1_two_clients_on_one_channel_converge() {
    let mut sim = Sim::new(11, 2);
    subscribe(&mut sim, 0, &["a"]);
    subscribe(&mut sim, 1, &["a"]);
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::CreateEntry {
            id: "e1".into(),
            text: "from 0".into(),
        },
    })
    .unwrap();
    sim.settle();
    sim.apply(Action::Enqueue {
        client: 1,
        mutation: MutationSpec::Edit {
            id: "e1".into(),
            text: "from 1".into(),
        },
    })
    .unwrap();
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::Edit {
            id: "e1".into(),
            text: "from 0 again".into(),
        },
    })
    .unwrap();
    sim.settle();
    let a = sim.read_text(0, &entry_key("e1"));
    let b = sim.read_text(1, &entry_key("e1"));
    assert_eq!(a, b);
    assert_eq!(
        a.as_deref(),
        Some(
            sim.host.state(&entry_key("e1")).unwrap()["text"]
                .as_str()
                .unwrap()
        )
    );
    assert_eq!(
        sim.client(0).record_stamp(&entry_key("e1")).unwrap(),
        sim.host.stamp(&entry_key("e1"))
    );
    assert_eq!(sim.conflicts, 0);
    sim.check().unwrap();
}

/// D2, fixtures/scenarios/delayed-page: b delivers the newer stamp first; a's page,
/// snapshotted earlier with the older stamp, arrives later and is discarded, but a's
/// cursor still advances.
#[test]
fn d2_delayed_page_from_another_channel_cannot_regress_newer_content() {
    let mut sim = Sim::new(12, 1);
    subscribe(&mut sim, 0, &["a", "b"]);
    // The record lives on both channels. Snapshot a's page while the shared record
    // still holds "old" - a's pull request must be delivered (host.pull() reads the
    // live record at that point) before b's later change overwrites it, or a's page
    // would carry b's content instead of a genuinely stale copy.
    change(&mut sim, "Entry:e1", Some("old"), &["a"]); // stamp 1
    pull(&mut sim, 0, "a");
    sim.apply(Action::Deliver).unwrap(); // a's request -> a's page queued, snapshotting "old"
    change(&mut sim, "Entry:e1", Some("new"), &["b"]); // stamp 2
    pull(&mut sim, 0, "b"); // queue: [a's page, b's request]
    sim.apply(Action::Swap { i: 0, j: 1 }).unwrap(); // queue: [b's request, a's page]
    sim.apply(Action::Deliver).unwrap(); // b's request -> b's page queued (after a's page)
    sim.apply(Action::Swap { i: 0, j: 1 }).unwrap(); // queue: [b's page, a's page]
    sim.apply(Action::Deliver).unwrap(); // b's page
    assert_eq!(sim.read_text(0, &entry_key("e1")).as_deref(), Some("new"));
    assert_eq!(sim.client(0).record_stamp(&entry_key("e1")).unwrap(), 2);
    sim.apply(Action::Deliver).unwrap(); // a's page, stale content at stamp 1
    assert_eq!(sim.read_text(0, &entry_key("e1")).as_deref(), Some("new"));
    assert_eq!(sim.client(0).record_stamp(&entry_key("e1")).unwrap(), 2);
    assert_eq!(sim.client(0).cursor("a").unwrap(), Some(1));
    assert_eq!(sim.client(0).cursor("b").unwrap(), Some(1));
    assert_eq!(sim.conflicts, 0);
    sim.check().unwrap();
}

/// D3: one change allocates one stamp, however many channels it is published to;
/// each channel's cursor still advances on its own, and every channel delivers that
/// same stamp.
#[test]
fn d3_one_change_is_one_stamp_on_every_channel() {
    let mut sim = Sim::new(13, 1);
    subscribe(&mut sim, 0, &["a", "b"]);
    change(&mut sim, "Entry:e1", Some("x"), &["a"]); // stamp 1: a:1
    change(&mut sim, "Entry:e1", Some("y"), &["a", "b"]); // stamp 2: a:2, b:1
    assert_eq!(sim.host.head("a"), 2);
    assert_eq!(sim.host.head("b"), 1);
    assert_eq!(
        sim.host.stamp(&entry_key("e1")),
        2,
        "two changes, two stamps"
    );
    assert_eq!(sim.host.channel_stamp("a", &entry_key("e1")), Some(2));
    assert_eq!(sim.host.channel_stamp("b", &entry_key("e1")), Some(2));
    sim.settle();
    assert_eq!(sim.read_text(0, &entry_key("e1")).as_deref(), Some("y"));
    assert_eq!(sim.client(0).record_stamp(&entry_key("e1")).unwrap(), 2);
    assert_eq!(sim.client(0).cursor("a").unwrap(), Some(2));
    assert_eq!(sim.client(0).cursor("b").unwrap(), Some(1));
    assert_eq!(sim.conflicts, 0);
    sim.check().unwrap();
}

/// D4: a -> b then b -> a. Moving republishes the record to its new channel at its
/// current stamp (no version is invented); the channel it leaves hears nothing and
/// the client keeps the row. Later changes reach it through the new channel only.
#[test]
fn d4_move_between_channels_and_back() {
    let mut sim = Sim::new(14, 1);
    subscribe(&mut sim, 0, &["a", "b"]);
    sim.host.set_membership(&entry_key("e1"), &["a"]);
    change(&mut sim, "Entry:e1", Some("in a"), &["a"]); // stamp 1
    sim.settle();
    assert_eq!(sim.client(0).cursor("a").unwrap(), Some(1));
    // Move to b: b is told at stamp 1, a is told nothing.
    move_to(&mut sim, "Entry:e1", &["b"]);
    assert_eq!(
        sim.host.stamp(&entry_key("e1")),
        1,
        "a move is not a change"
    );
    assert_eq!(sim.host.head("a"), 1);
    assert_eq!(sim.host.head("b"), 1);
    sim.settle();
    assert_eq!(sim.read_text(0, &entry_key("e1")).as_deref(), Some("in a"));
    assert_eq!(sim.client(0).record_stamp(&entry_key("e1")).unwrap(), 1);
    assert_eq!(sim.client(0).cursor("b").unwrap(), Some(1));
    change(&mut sim, "Entry:e1", Some("in b"), &["b"]); // stamp 2
    sim.settle();
    assert_eq!(sim.read_text(0, &entry_key("e1")).as_deref(), Some("in b"));
    assert_eq!(
        sim.client(0).cursor("a").unwrap(),
        Some(1),
        "a heard nothing"
    );
    // Move back to a: a is told at stamp 2.
    move_to(&mut sim, "Entry:e1", &["a"]);
    assert_eq!(sim.host.stamp(&entry_key("e1")), 2);
    sim.settle();
    assert_eq!(sim.read_text(0, &entry_key("e1")).as_deref(), Some("in b"));
    assert_eq!(sim.client(0).cursor("a").unwrap(), Some(2));
    change(&mut sim, "Entry:e1", Some("back in a"), &["a"]); // stamp 3
    sim.settle();
    assert_eq!(
        sim.read_text(0, &entry_key("e1")).as_deref(),
        Some("back in a")
    );
    assert_eq!(sim.client(0).record_stamp(&entry_key("e1")).unwrap(), 3);
    assert_eq!(sim.conflicts, 0);
    sim.check().unwrap();
}

/// D5, fixtures/scenarios/delete-across-channels: the delete is published to both
/// channels; b delivers it first; a's earlier page carrying the older upsert is
/// discarded; a's own delivery of the delete then changes nothing. The stamp row
/// stays as the evidence that keeps stale content from resurrecting the record.
#[test]
fn d5_delete_across_channels_outranks_a_delayed_upsert_and_keeps_its_stamp() {
    let mut sim = Sim::new(15, 1);
    subscribe(&mut sim, 0, &["a", "b"]);
    change(&mut sim, "Entry:e1", Some("v1"), &["a", "b"]); // stamp 1
    sim.settle();
    change(&mut sim, "Entry:e1", Some("v2"), &["a"]); // stamp 2: the delayed upsert
    pull(&mut sim, 0, "a");
    sim.apply(Action::Deliver).unwrap(); // a's page snapshots v2 at stamp 2
    change(&mut sim, "Entry:e1", None, &["b", "a"]); // stamp 3: the delete
    pull(&mut sim, 0, "b"); // queue: [a's page, b's request]
    sim.apply(Action::Swap { i: 0, j: 1 }).unwrap();
    sim.apply(Action::Deliver).unwrap(); // queue: [a's page, b's page]
    sim.apply(Action::Swap { i: 0, j: 1 }).unwrap();
    sim.apply(Action::Deliver).unwrap(); // b's page: the delete
    assert_eq!(
        sim.read_text(0, &entry_key("e1")),
        None,
        "b's delete removes the row"
    );
    assert_eq!(sim.client(0).record_stamp(&entry_key("e1")).unwrap(), 3);
    // b's page was one pull for both channels, so a's own delivery of the
    // delete came with it; the earlier page is now covered on every channel.
    assert_eq!(sim.client(0).cursor("a").unwrap(), Some(3));
    sim.apply(Action::Deliver).unwrap(); // a's page: v2 at stamp 2, older and covered
    assert_eq!(
        sim.read_text(0, &entry_key("e1")),
        None,
        "stale content cannot resurrect a deleted record"
    );
    assert_eq!(sim.client(0).cursor("a").unwrap(), Some(3));
    assert_eq!(
        stamp_rows(&mut sim, 0).len(),
        1,
        "the stamp row is retained"
    );
    pull(&mut sim, 0, "a"); // nothing new on either channel
    sim.drain();
    assert_eq!(sim.read_text(0, &entry_key("e1")), None);
    assert_eq!(sim.client(0).cursor("a").unwrap(), Some(3));
    assert_eq!(sim.client(0).record_stamp(&entry_key("e1")).unwrap(), 3);
    assert_eq!(stamp_rows(&mut sim, 0).len(), 1);
    assert_eq!(sim.conflicts, 0);
    sim.check().unwrap();
}

/// Unsubscribing stops a channel's synchronization and nothing else: every row it
/// delivered stays, a channel the client still follows keeps updating the shared
/// record, and a record only the left channel provides is retained as it was.
#[test]
fn unsubscribe_retains_rows_and_another_channel_still_updates_them() {
    let mut sim = Sim::new(16, 1);
    subscribe(&mut sim, 0, &["a", "b"]);
    sim.host.set_membership(&entry_key("e1"), &["a", "b"]);
    sim.host.set_membership(&entry_key("e2"), &["a"]);
    change(&mut sim, "Entry:e1", Some("shared"), &["a", "b"]);
    change(&mut sim, "Entry:e2", Some("only a"), &["a"]);
    sim.settle();
    sim.apply(Action::Unsubscribe {
        client: 0,
        channel: "a".into(),
    })
    .unwrap();
    assert_eq!(
        sim.read_text(0, &entry_key("e1")).as_deref(),
        Some("shared")
    );
    assert_eq!(
        sim.read_text(0, &entry_key("e2")).as_deref(),
        Some("only a"),
        "a channel is not an owner: its rows stay"
    );
    assert_eq!(sim.client(0).record_stamp(&entry_key("e2")).unwrap(), 1);
    sim.check().unwrap();
    // The other channel keeps the shared record fresh.
    change(&mut sim, "Entry:e1", Some("shared v2"), &["b"]);
    sim.settle();
    assert_eq!(
        sim.read_text(0, &entry_key("e1")).as_deref(),
        Some("shared v2")
    );
    // A change to the retained record on the left channel is not promised to
    // arrive: the row is readable, and stale, which is legal.
    change(&mut sim, "Entry:e2", Some("only a v2"), &["a"]);
    sim.settle();
    assert_eq!(
        sim.read_text(0, &entry_key("e2")).as_deref(),
        Some("only a")
    );
    sim.check().unwrap();
    // Null through the remaining channel is a delete.
    change(&mut sim, "Entry:e1", None, &["b"]);
    sim.settle();
    assert_eq!(sim.read_text(0, &entry_key("e1")), None);
    assert_eq!(sim.client(0).record_stamp(&entry_key("e1")).unwrap(), 3);
    assert_eq!(sim.conflicts, 0);
    sim.check().unwrap();
}

/// D4 with declared child membership: an entry and its comment move from channel a
/// to channel b together and back. Neither move produces a delete anywhere; both
/// records keep their content and stamps through each move and take later changes
/// from whichever channel currently provides them.
#[test]
fn d4_parent_and_child_move_channels_together_without_deletes() {
    let mut sim = Sim::new(44, 1);
    subscribe(&mut sim, 0, &["a", "b"]);
    sim.host.set_membership(&entry_key("e1"), &["a"]);
    sim.host.set_membership(&comment_key("c1"), &["a"]);
    change(&mut sim, "Entry:e1", Some("entry in a"), &["a"]);
    change(&mut sim, "Comment:c1", Some("comment in a"), &["a"]);
    sim.settle();
    assert_eq!(
        sim.read_text(0, &comment_key("c1")).as_deref(),
        Some("comment in a")
    );

    move_to(&mut sim, "Entry:e1", &["b"]);
    assert_eq!(
        sim.host.membership(&comment_key("c1")),
        vec!["b".to_string()]
    );
    assert_eq!(sim.host.stamp(&entry_key("e1")), 1);
    assert_eq!(sim.host.stamp(&comment_key("c1")), 1);
    sim.settle();
    assert_eq!(
        sim.read_text(0, &entry_key("e1")).as_deref(),
        Some("entry in a")
    );
    assert_eq!(
        sim.read_text(0, &comment_key("c1")).as_deref(),
        Some("comment in a")
    );
    assert_eq!(sim.client(0).cursor("b").unwrap(), Some(2));
    change(&mut sim, "Entry:e1", Some("entry in b"), &["b"]);
    change(&mut sim, "Comment:c1", Some("comment in b"), &["b"]);
    sim.settle();
    assert_eq!(
        sim.read_text(0, &entry_key("e1")).as_deref(),
        Some("entry in b")
    );
    assert_eq!(
        sim.read_text(0, &comment_key("c1")).as_deref(),
        Some("comment in b")
    );
    sim.check().unwrap();

    move_to(&mut sim, "Entry:e1", &["a"]);
    sim.settle();
    assert_eq!(
        sim.read_text(0, &entry_key("e1")).as_deref(),
        Some("entry in b"),
        "moving back is not a change either"
    );
    assert_eq!(sim.client(0).record_stamp(&entry_key("e1")).unwrap(), 2);
    change(&mut sim, "Entry:e1", Some("entry back in a"), &["a"]);
    change(&mut sim, "Comment:c1", Some("comment back in a"), &["a"]);
    sim.settle();
    assert_eq!(
        sim.read_text(0, &entry_key("e1")).as_deref(),
        Some("entry back in a")
    );
    assert_eq!(
        sim.read_text(0, &comment_key("c1")).as_deref(),
        Some("comment back in a")
    );
    assert_eq!(sim.conflicts, 0);
    sim.check().unwrap();
}

/// Spec §4: removal stops future distribution and evicts nothing. The client
/// keeps the row it applied; later content reaches no one through the Channel
/// the record left, including a client that subscribes from zero afterwards,
/// which the retained invalidation row would otherwise hand the new content.
#[test]
fn removal_keeps_client_rows_and_hides_later_content_from_the_old_channel() {
    let mut sim = Sim::new(61, 2);
    let e1 = entry_key("e1");
    subscribe(&mut sim, 0, &["a"]);
    declare(&mut sim, "Entry:e1", Some(Some("before")), &[("a", true)]);
    sim.settle();
    assert_eq!(sim.read_text(0, &e1).as_deref(), Some("before"));
    let head = sim.host.head("a");
    declare(&mut sim, "Entry:e1", None, &[("a", false)]);
    assert_eq!(sim.host.head("a"), head, "removal allocates no position");
    assert_eq!(
        sim.host.channel_stamp("a", &e1),
        Some(1),
        "and keeps the row"
    );
    declare(&mut sim, "Entry:e1", Some(Some("after removal")), &[]);
    assert_eq!(sim.host.stamp(&e1), 2);
    assert_eq!(sim.host.head("a"), head, "a change reaches members only");
    sim.settle();
    assert_eq!(
        sim.read_text(0, &e1).as_deref(),
        Some("before"),
        "the row stays as it was: no deletion, no eviction"
    );
    assert_eq!(sim.client(0).record_stamp(&e1).unwrap(), 1);
    assert_eq!(sim.client(0).cursor("a").unwrap(), Some(head));
    subscribe(&mut sim, 1, &["a"]);
    sim.settle();
    assert_eq!(
        sim.read_text(1, &e1),
        None,
        "the old Channel does not expose content published after removal"
    );
    assert_eq!(sim.client(1).cursor("a").unwrap(), Some(head));
    assert_eq!(sim.conflicts, 0);
    sim.check().unwrap();
}

/// Re-adding publishes the record's current state at a fresh position without
/// a new stamp; later touches reach it again. Intents that cancel within one
/// settlement change nothing.
#[test]
fn re_adding_publishes_current_state_and_later_touches_follow() {
    let mut sim = Sim::new(62, 1);
    let e1 = entry_key("e1");
    subscribe(&mut sim, 0, &["a", "b"]);
    declare(&mut sim, "Entry:e1", Some(Some("v1")), &[("a", true)]);
    sim.settle();
    declare(&mut sim, "Entry:e1", None, &[("a", false)]);
    declare(&mut sim, "Entry:e1", Some(Some("v2")), &[]);
    sim.settle();
    assert_eq!(sim.read_text(0, &e1).as_deref(), Some("v1"));
    let head = sim.host.head("a");
    declare(&mut sim, "Entry:e1", None, &[("a", true)]);
    assert_eq!(sim.host.head("a"), head + 1, "a fresh position");
    assert_eq!(sim.host.stamp(&e1), 2, "at the unchanged stamp");
    sim.settle();
    assert_eq!(sim.read_text(0, &e1).as_deref(), Some("v2"));
    assert_eq!(sim.client(0).record_stamp(&e1).unwrap(), 2);
    declare(&mut sim, "Entry:e1", Some(Some("v3")), &[]);
    sim.settle();
    assert_eq!(sim.read_text(0, &e1).as_deref(), Some("v3"));
    // Cancelling intents: a member removed and re-added, a non-member added
    // and removed, in one settlement.
    let heads = (sim.host.head("a"), sim.host.head("b"));
    declare(
        &mut sim,
        "Entry:e1",
        None,
        &[("a", false), ("b", true), ("a", true), ("b", false)],
    );
    assert_eq!((sim.host.head("a"), sim.host.head("b")), heads);
    assert_eq!(sim.host.stored_memberships(&e1), ["a"]);
    assert_eq!(sim.host.stamp(&e1), 3);
    assert_eq!(sim.conflicts, 0);
    sim.check().unwrap();
}

/// A deleted record stays enrolled, so its Channel delivers the deletion, and
/// recreating the same identity reaches the same Channel without a new add.
/// Deleting and removing in one settlement tells the Channel nothing.
#[test]
fn a_deleted_record_stays_enrolled_and_its_recreation_is_delivered_again() {
    let mut sim = Sim::new(63, 1);
    let e1 = entry_key("e1");
    subscribe(&mut sim, 0, &["a"]);
    declare(&mut sim, "Entry:e1", Some(Some("v1")), &[("a", true)]);
    sim.settle();
    declare(&mut sim, "Entry:e1", Some(None), &[]);
    assert_eq!(sim.host.stored_memberships(&e1), ["a"]);
    sim.settle();
    assert_eq!(sim.read_text(0, &e1), None);
    assert_eq!(sim.client(0).record_stamp(&e1).unwrap(), 2);
    declare(&mut sim, "Entry:e1", Some(Some("again")), &[]);
    sim.settle();
    assert_eq!(sim.read_text(0, &e1).as_deref(), Some("again"));
    assert_eq!(sim.client(0).record_stamp(&e1).unwrap(), 3);
    let head = sim.host.head("a");
    declare(&mut sim, "Entry:e1", Some(None), &[("a", false)]);
    assert_eq!(sim.host.head("a"), head, "the final relationship wins");
    sim.settle();
    assert_eq!(
        sim.read_text(0, &e1).as_deref(),
        Some("again"),
        "no deletion reaches a Channel the record left"
    );
    sim.check().unwrap();
}

/// Generated add/remove/touch sequences, with client edits, dropped and
/// duplicated messages and client crashes and restarts in between. Every step
/// keeps the invariants; a row a client holds disappears only by applying the
/// authority of a version at which the server had no record (removal evicts
/// nothing); after settling, every client at a Channel's head holds the
/// server's state of that Channel's members, and a client that subscribes to
/// every Channel from zero only then holds exactly the current members.
#[test]
fn generated_membership_sequences_converge_through_restarts_and_duplicates() {
    let mut comparisons = 0;
    let mut removals = 0;
    for seed in 0..24u64 {
        let mut sim = Sim::new(7000 + seed, 3);
        sim.apply(Action::Crash { client: 2 }).unwrap();
        subscribe(&mut sim, 0, &["a", "b"]);
        subscribe(&mut sim, 1, &["b", "c"]);
        for id in 0..4 {
            declare(
                &mut sim,
                &format!("Entry:g{id}"),
                Some(Some("created")),
                &[(["a", "b", "c"][id % 3], true)],
            );
        }
        // (record, stamp) of every version at which the server had no record.
        let mut deletions = std::collections::BTreeSet::new();
        for step in 0..150 {
            let Some(action) = sim.membership_step() else {
                continue;
            };
            if let Action::Declare { memberships, .. } = &action {
                removals += memberships.iter().filter(|(_, present)| !present).count();
            }
            let held = held_rows(&mut sim);
            sim.apply(action.clone())
                .unwrap_or_else(|e| panic!("seed {seed} step {step} {action:?}: {e}"));
            sim.check()
                .unwrap_or_else(|e| panic!("seed {seed} step {step} {action:?}: {e}"));
            // A stamp at which the record is absent is deletion evidence,
            // whether a deletion or a later change to the deleted record made it.
            for id in 0..4 {
                let key = entry_key(&format!("g{id}"));
                if sim.host.state(&key).is_none() {
                    deletions.insert((key.encoded().unwrap(), sim.host.stamp(&key)));
                }
            }
            for (client, key) in held {
                if sim.is_up(client) && sim.read_text(client, &key).is_none() {
                    let stamp = sim.client(client).record_stamp(&key).unwrap();
                    assert!(
                        deletions.contains(&(key.encoded().unwrap(), stamp)),
                        "seed {seed} step {step}: client {client} lost {key:?} at stamp {stamp} without a deletion"
                    );
                }
            }
        }
        for client in 0..3 {
            sim.apply(Action::Restart { client }).unwrap();
        }
        subscribe(&mut sim, 2, &["a", "b", "c"]);
        sim.settle();
        sim.check().unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        for id in 0..4 {
            let key = entry_key(&format!("g{id}"));
            let expected = if sim.host.stored_memberships(&key).is_empty() {
                None
            } else {
                sim.host.state(&key).map(|state| state["text"].clone())
            };
            let late = sim.client(2).read(&key).unwrap();
            assert_eq!(
                late.map(|state| state["text"].clone()),
                expected,
                "seed {seed}: g{id} for the late subscriber, members {:?}",
                sim.host.stored_memberships(&key)
            );
        }
        comparisons += sim.comparisons;
        assert_eq!(sim.conflicts, 0, "seed {seed}");
    }
    assert!(removals > 100, "the sequences removed members: {removals}");
    assert!(
        comparisons > 500,
        "convergence was actually compared: {comparisons}"
    );
}

/// Every (client, record) whose row a running client currently holds.
fn held_rows(sim: &mut Sim) -> Vec<(usize, axton_core::RecordKey)> {
    let keys: Vec<_> = (0..4).map(|id| entry_key(&format!("g{id}"))).collect();
    let mut held = vec![];
    for client in 0..3 {
        if !sim.is_up(client) {
            continue;
        }
        for key in &keys {
            if sim.read_text(client, key).is_some() {
                held.push((client, key.clone()));
            }
        }
    }
    held
}
