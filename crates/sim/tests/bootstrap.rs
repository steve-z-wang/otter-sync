//! Whole-Scope bootstrap on the simulation
//! ([#151](https://github.com/zanminwang/axton/issues/151)): a client that
//! subscribes from now reaches everything published before its origin through
//! the bounded historical interval, while other clients keep publishing.
//!
//! The simulated backend answers both pull modes through the one public
//! `process_pull` entry point, so the bounded page is the same contract the
//! HTTP adapter serves. The client side is driven the way the Downlink worker
//! drives it - schedule a page from the ledger, apply it with its progress in
//! one transaction, settle a fixed barrier after delivery commits - rather than
//! by a second copy of that worker.
use axton_client::BootstrapPhase;
use axton_sim::{Action, MutationSpec, Sim, schema::entry_key};

fn create(sim: &mut Sim, client: usize, id: &str, text: &str) {
    sim.apply(Action::Enqueue {
        client,
        mutation: MutationSpec::CreateEntry {
            id: id.into(),
            text: text.into(),
        },
    })
    .unwrap();
}
fn edit(sim: &mut Sim, client: usize, id: &str, text: &str) {
    sim.apply(Action::Enqueue {
        client,
        mutation: MutationSpec::Edit {
            id: id.into(),
            text: text.into(),
        },
    })
    .unwrap();
}
/// One application transaction for `key` (see `Action::Declare`).
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
/// Ask for one historical page and deliver everything the network holds.
fn load(sim: &mut Sim, client: usize) {
    sim.apply(Action::LoadPull { client }).unwrap();
    sim.drain();
}
/// The committed historical progress of one registration.
fn progress(sim: &mut Sim, client: usize, channel: &str) -> u64 {
    let id = sim
        .client(client)
        .subscription_state(channel)
        .unwrap()
        .unwrap()
        .subscription_id;
    sim.client(client)
        .bootstrap_state(channel, id)
        .unwrap()
        .cursor
}

/// A client that subscribes from now loads nothing older by subscribing, and
/// Bootstrap covers exactly that history: the interval terminates although two
/// other clients keep publishing, and once writes stop every client holds the
/// same authority.
#[test]
fn a_from_now_client_bootstraps_its_history_while_others_keep_publishing() {
    let mut sim = Sim::new(7, 3);
    for client in [0, 1] {
        sim.apply(Action::Subscribe {
            client,
            channel: "a".into(),
        })
        .unwrap();
    }
    // The Scope's history: sixty records, so the interval needs more than one
    // bounded page.
    for i in 0..60 {
        create(&mut sim, i % 2, &format!("e{i}"), &format!("text {i}"));
    }
    sim.settle();
    let origin = sim.host.head("a");
    assert!(origin >= 60, "the Scope has a history: {origin}");

    // The newcomer registers from now: its origin is the head, and delivery
    // starts after it (D9).
    sim.apply(Action::SubscribeAtHead {
        client: 2,
        channel: "a".into(),
    })
    .unwrap();
    assert_eq!(sim.client(2).cursor("a").unwrap(), Some(origin));
    sim.settle();
    assert_eq!(
        sim.read_text(2, &entry_key("e0")),
        None,
        "subscribing delivered nothing published before the origin"
    );

    sim.apply(Action::Bootstrap {
        client: 2,
        channel: "a".into(),
    })
    .unwrap();
    assert_eq!(sim.bootstrap_phase(2, "a"), BootstrapPhase::Requested);

    // One page at a time, with the other clients publishing beside it. The
    // upper bound is the origin, so the interval finishes although the head
    // does not stop moving.
    let mut pages = 0;
    for round in 0..16 {
        if !matches!(
            sim.bootstrap_phase(2, "a"),
            BootstrapPhase::Requested | BootstrapPhase::Loading
        ) {
            break;
        }
        load(&mut sim, 2);
        pages += 1;
        create(
            &mut sim,
            round % 2,
            &format!("later-{round}"),
            "published later",
        );
        edit(&mut sim, round % 2, "e0", &format!("edited {round}"));
        sim.apply(Action::Freeze { client: round % 2 }).unwrap();
        sim.drain();
    }
    assert!(
        (2..=4).contains(&pages),
        "a bounded interval takes a bounded number of pages: {pages}"
    );
    assert_eq!(
        progress(&mut sim, 2, "a"),
        origin,
        "the historical interval finished at the origin it started with"
    );
    assert!(
        matches!(
            sim.bootstrap_phase(2, "a"),
            BootstrapPhase::CatchingUp | BootstrapPhase::Complete
        ),
        "the interval terminated: {:?}",
        sim.bootstrap_phase(2, "a")
    );

    // Writes stop. Delivery reaches the fixed barrier, the run completes, and
    // every client holds the same authority.
    sim.settle();
    assert_eq!(
        sim.bootstrap_phase(2, "a"),
        BootstrapPhase::Complete,
        "delivery reached the barrier the final page fixed"
    );
    assert_eq!(sim.client(2).cursor("a").unwrap(), Some(sim.host.head("a")));
    for i in 0..60 {
        let key = entry_key(&format!("e{i}"));
        assert_eq!(
            sim.read_text(2, &key),
            sim.read_text(0, &key),
            "e{i} converged on the bootstrapped client"
        );
        assert!(
            sim.read_text(2, &key).is_some(),
            "e{i} was loaded by the historical interval"
        );
    }
    sim.check().unwrap();
    assert_eq!(sim.conflicts, 0);
    assert!(
        sim.reports.is_empty(),
        "nothing failed to apply: {:?}",
        sim.reports
    );
}

/// The historical interval never chases the head: every page of a run carries
/// the origin the subscription committed, and a record republished above it
/// leaves the interval for the subscription's own delivery (spec section 3).
#[test]
fn a_record_republished_above_the_origin_leaves_the_historical_interval() {
    let mut sim = Sim::new(3, 2);
    sim.apply(Action::Subscribe {
        client: 0,
        channel: "a".into(),
    })
    .unwrap();
    create(&mut sim, 0, "moving", "the record that moves");
    for i in 0..4 {
        create(&mut sim, 0, &format!("e{i}"), &format!("text {i}"));
    }
    sim.settle();
    let origin = sim.host.head("a");

    sim.apply(Action::SubscribeAtHead {
        client: 1,
        channel: "a".into(),
    })
    .unwrap();
    sim.apply(Action::Bootstrap {
        client: 1,
        channel: "a".into(),
    })
    .unwrap();
    // Before the scan runs, the record is removed and re-added in separate
    // settlements: its one retained position moves above the origin at its
    // unchanged stamp.
    let stamp = sim.host.stamp(&entry_key("moving"));
    declare(&mut sim, "Entry:moving", None, &[("a", false)]);
    declare(&mut sim, "Entry:moving", None, &[("a", true)]);
    assert!(sim.host.head("a") > origin);
    assert_eq!(sim.host.stamp(&entry_key("moving")), stamp);

    // The interval is still the one the subscription committed, and it covers
    // only what stayed inside it.
    load(&mut sim, 1);
    assert_eq!(progress(&mut sim, 1, "a"), origin);
    assert_eq!(
        sim.read_text(1, &entry_key("e0")),
        Some("text 0".into()),
        "the historical interval delivered what stayed in it"
    );
    assert_eq!(
        sim.read_text(1, &entry_key("moving")),
        None,
        "the record whose position moved above the origin is not the scan's"
    );

    // What left the interval is the subscription's to deliver, and it does.
    sim.settle();
    assert_eq!(
        sim.read_text(1, &entry_key("moving")),
        Some("the record that moves".into())
    );
    assert_eq!(sim.bootstrap_phase(1, "a"), BootstrapPhase::Complete);
    sim.check().unwrap();
}

/// Bootstrap covers membership in each page's snapshot. Sixty removed records
/// (more than a page) sit below sixty-five members: the walk skips them, pages
/// the members in two bounded pages, and terminates at its origin. A crash
/// and restart between pages resumes from committed progress, and a
/// duplicated page request and page change nothing. A record removed and
/// re-added before its page runs moves above the origin and arrives through
/// ordinary delivery; the removed records never reach the new client, and the
/// client that already held them keeps them.
#[test]
fn bootstrap_skips_removed_members_across_pages_through_restart_and_duplicates() {
    let mut sim = Sim::new(9, 2);
    sim.apply(Action::Subscribe {
        client: 0,
        channel: "a".into(),
    })
    .unwrap();
    for i in 0..125 {
        declare(
            &mut sim,
            &format!("Entry:e{i:03}"),
            Some(Some(&format!("text {i}"))),
            &[("a", true)],
        );
    }
    sim.settle();
    for i in 0..60 {
        declare(&mut sim, &format!("Entry:e{i:03}"), None, &[("a", false)]);
    }
    let origin = sim.host.head("a");
    assert_eq!(origin, 125, "removal allocated no position");
    sim.apply(Action::SubscribeAtHead {
        client: 1,
        channel: "a".into(),
    })
    .unwrap();
    sim.apply(Action::Bootstrap {
        client: 1,
        channel: "a".into(),
    })
    .unwrap();

    // First page, asked for twice: the duplicate answer is fenced.
    sim.apply(Action::LoadPull { client: 1 }).unwrap();
    sim.apply(Action::Duplicate).unwrap();
    sim.drain();
    let first = progress(&mut sim, 1, "a");
    assert!(
        first > 60 && first < origin,
        "a full page of members stops at its last cursor: {first}"
    );
    assert_eq!(sim.read_text(1, &entry_key("e060")), Some("text 60".into()));
    // A member of the remaining interval moves above the origin.
    declare(&mut sim, "Entry:e124", None, &[("a", false)]);
    declare(&mut sim, "Entry:e124", None, &[("a", true)]);
    // Crash between pages; the reopened client resumes from its progress.
    sim.apply(Action::Crash { client: 1 }).unwrap();
    sim.apply(Action::Restart { client: 1 }).unwrap();
    assert_eq!(progress(&mut sim, 1, "a"), first);
    load(&mut sim, 1);
    assert_eq!(progress(&mut sim, 1, "a"), origin);
    sim.settle();
    assert_eq!(sim.bootstrap_phase(1, "a"), BootstrapPhase::Complete);
    for i in 0..125 {
        let key = entry_key(&format!("e{i:03}"));
        let expected = (i >= 60).then(|| format!("text {i}"));
        assert_eq!(
            sim.read_text(1, &key),
            expected,
            "e{i:03} on the new client"
        );
        assert_eq!(
            sim.read_text(0, &key),
            Some(format!("text {i}")),
            "e{i:03} kept by the client that already held it"
        );
    }
    sim.check().unwrap();
    assert_eq!(sim.conflicts, 0);
    assert!(sim.reports.is_empty(), "{:?}", sim.reports);
}
