//! Downlink worker transitions: the Rust worker drives a scripted host. Real
//! sockets and HTTP are the SDK suites' job; here every event is a value and
//! every action is asserted. Enqueueing an event only takes it into typed
//! state; pages are applied by the bounded pump (`next`).
mod common;
use axton_client::*;
use common::*;
use serde_json::{Value, json};

/// The behaviour change of [#150](https://github.com/zanminwang/axton/issues/150):
/// a fresh subscription's first delivery boundary is the head its first
/// handshake acknowledged. It asks for no history, and the stream is the truth
/// from that head on.
#[test]
fn a_fresh_subscription_initializes_at_the_acknowledged_head_and_loads_no_history() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    lane.set("a", true);
    assert_eq!(
        lane.cursor("a"),
        None,
        "a registration has no delivery position until a head is acknowledged"
    );
    let actions = lane.send(DownlinkEvent::Start);
    let (epoch, subscribe) = opened(&actions[0]);
    assert_eq!(
        subscribe.channels,
        ["a"],
        "an uninitialized subscription is still asked for on the socket"
    );
    assert_eq!(actions.len(), 1);
    assert_eq!(
        lane.message(epoch, ack(&[("a", 100)])),
        vec![
            DownlinkAction::Changed {
                scopes: vec!["a".into()]
            },
            established(&["a"])
        ],
        "the acknowledged head is committed and announced; no history is pulled"
    );
    let state = lane.client.subscription_state("a").unwrap().unwrap();
    assert_eq!(
        (state.starting_cursor, state.cursor),
        (Some(100), Some(100)),
        "both cursor fields are the acknowledged head"
    );
    // From that boundary the stream applies directly.
    assert_eq!(
        lane.frame(epoch, &page("a", 100, 101, Some("from now"))),
        applied(&["a"])
    );
    assert_eq!(
        (lane.cursor("a"), lane.text()),
        (Some(101), json!("from now"))
    );
}

/// Subscribing offline is durable intent and nothing else: no boundary, no
/// request, nothing for the lane to do until connectivity gives it a head.
#[test]
fn an_offline_subscribe_registers_without_a_boundary_and_asks_for_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    lane.set("a", true);
    assert_eq!(lane.cursor("a"), None);
    assert_eq!(
        lane.client.subscriptions().unwrap(),
        vec![],
        "no delivery position, so no cursor to pull from"
    );
    assert!(lane.client.desired_channels().unwrap().contains("a"));
    assert_eq!(
        lane.client.downlink_request().unwrap(),
        None,
        "never coerced into a request from zero"
    );
    assert_eq!(
        lane.drain(),
        vec![],
        "the lane was never started: the intent waits"
    );
}

/// Zero is a boundary like any other: an unknown Scope acknowledged at head
/// zero is initialized there, not left uninitialized.
#[test]
fn an_acknowledgement_at_head_zero_initializes_at_zero() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    let (epoch, acknowledged) = lane.streaming("a", 0);
    assert_eq!(
        acknowledged,
        vec![
            DownlinkAction::Changed {
                scopes: vec!["a".into()]
            },
            established(&["a"])
        ]
    );
    let state = lane.client.subscription_state("a").unwrap().unwrap();
    assert_eq!((state.starting_cursor, state.cursor), (Some(0), Some(0)));
    assert_eq!(
        lane.frame(epoch, &page("a", 0, 1, Some("first"))),
        applied(&["a"]),
        "the stream applies from zero"
    );
}

/// Reconnect is not a new origin: a subscription saved at 100 keeps it and
/// fetches the gap up to the newly acknowledged 120.
#[test]
fn a_reconnect_catches_up_from_the_saved_cursor_instead_of_the_new_head() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    lane.saved("a", 100);
    let (_, acknowledged) = lane.streaming("a", 120);
    let (id, pull) = request(&acknowledged[0]);
    assert_eq!(cursors(&pull), vec![("a", 100)]);
    assert_eq!(
        acknowledged.len(),
        2,
        "the catch-up and the handshake, nothing else: {acknowledged:?}"
    );
    assert_eq!(
        acknowledged[1],
        established(&["a"]),
        "nothing was initialized, so nothing is announced as committed"
    );
    assert_eq!(
        lane.response(id, &page("a", 100, 120, Some("the gap"))),
        applied(&["a"])
    );
    let state = lane.client.subscription_state("a").unwrap().unwrap();
    assert_eq!(
        (state.starting_cursor, state.cursor),
        (Some(100), Some(120)),
        "the origin is fixed for this subscription; only the cursor moved"
    );
}

/// A head below committed progress is a reported server-state fault: the
/// session ends with it and no cursor is rewound or reinitialized.
#[test]
fn a_head_below_the_committed_cursor_is_a_fault_that_rewinds_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    lane.saved("a", 120);
    lane.set("b", true);
    let (epoch, _) = opened(&lane.send(DownlinkEvent::Start)[0]);
    let actions = lane.message(epoch, ack(&[("a", 100), ("b", 7)]));
    let DownlinkAction::Close {
        epoch: closed,
        reason: Some(reason),
    } = &actions[0]
    else {
        panic!("expected a close with a reason, got {actions:?}")
    };
    assert_eq!(*closed, epoch);
    assert!(
        reason.contains("below its committed cursor 120"),
        "{reason}"
    );
    assert!((200..=300).contains(&wait(&actions[1])));
    assert_eq!(lane.cursor("a"), Some(120), "progress is untouched");
    assert_eq!(
        lane.cursor("b"),
        None,
        "the whole acknowledgement was refused: b was not initialized either"
    );
}

/// The handshake announces the set it covered, which is what the SDKs read
/// their `live` connection status from ([#150](https://github.com/zanminwang/axton/issues/150)
/// Task 3). Every session announces it, whether or not anything was committed
/// for those Scopes; a refused acknowledgement announces nothing, because
/// nothing is established.
#[test]
fn the_handshake_announces_the_acknowledged_set_and_a_refused_one_announces_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    lane.saved("a", 5);
    let (epoch, acknowledged) = lane.streaming("a", 5);
    assert_eq!(
        acknowledged,
        vec![established(&["a"])],
        "nothing was committed and delivery is still established"
    );
    // The socket drops and the next session announces its own handshake again.
    let actions = lane.send(DownlinkEvent::Closed { epoch });
    lane.now += wait(&actions[1]);
    let (next, _) = opened(&lane.drain()[0]);
    assert_eq!(
        lane.message(next, ack(&[("a", 5)])),
        vec![established(&["a"])]
    );
    // An acknowledgement that names another set is refused: the session ends
    // and nothing of it is announced.
    let actions = lane.send(DownlinkEvent::Closed { epoch: next });
    lane.now += wait(&actions[1]);
    let (refused, _) = opened(&lane.drain()[0]);
    let actions = lane.message(refused, ack(&[("a", 5), ("z", 1)]));
    assert!(
        matches!(
            &actions[0],
            DownlinkAction::Close {
                reason: Some(_),
                ..
            }
        ),
        "{actions:?}"
    );
    assert!(
        !actions
            .iter()
            .any(|a| matches!(a, DownlinkAction::Acknowledged { .. })),
        "a refused handshake establishes nothing: {actions:?}"
    );
}

/// The local cursor decides what a page is: at 120 a page spanning 120 to 125
/// applies directly, while one from 124 waits for the pull that connects it.
#[test]
fn a_page_spanning_the_cursor_applies_and_one_starting_beyond_it_is_repaired() {
    let direct = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(direct.path());
    lane.saved("a", 120);
    let (epoch, acknowledged) = lane.streaming("a", 120);
    assert_eq!(
        acknowledged,
        vec![established(&["a"])],
        "at its head: nothing to do but announce the handshake"
    );
    assert_eq!(
        lane.frame(epoch, &page("a", 120, 125, Some("direct"))),
        applied(&["a"])
    );
    assert_eq!(
        (lane.cursor("a"), lane.text()),
        (Some(125), json!("direct"))
    );

    let gapped = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(gapped.path());
    lane.saved("a", 120);
    let (epoch, _) = lane.streaming("a", 120);
    let actions = lane.frame(epoch, &page("a", 124, 125, Some("beyond")));
    let (id, repair) = request(&actions[0]);
    assert_eq!(
        cursors(&repair),
        vec![("a", 120)],
        "one pull from the cursor"
    );
    assert_eq!(actions.len(), 1);
    assert_eq!(
        lane.cursor("a"),
        Some(120),
        "the page waits, nothing applied"
    );
    // The repair connects the held page, which is applied in a commit of its own.
    let actions = lane.response(id, &page("a", 120, 124, Some("repair")));
    assert_eq!(actions, [applied(&["a"]), applied(&["a"])].concat());
    assert_eq!(
        (lane.cursor("a"), lane.text()),
        (Some(125), json!("beyond"))
    );
}

/// One socket carries only what this client subscribes: an initialized Scope
/// keeps its cursor and catches up, one waiting for a boundary is initialized
/// at its head in the same transaction, and a Scope with no row is in neither.
#[test]
fn a_mixed_set_catches_up_one_scope_initializes_another_and_never_asks_for_a_third() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    lane.saved("a", 80);
    lane.set("b", true);
    let actions = lane.send(DownlinkEvent::Start);
    let (epoch, subscribe) = opened(&actions[0]);
    assert_eq!(
        subscribe.channels,
        ["a", "b"],
        "c has no row: it is not desired"
    );
    let acknowledged = lane.message(epoch, ack(&[("a", 100), ("b", 200)]));
    assert_eq!(
        acknowledged[0],
        DownlinkAction::Changed {
            scopes: vec!["b".into()]
        },
        "only b crossed into initialized"
    );
    let (id, pull) = request(&acknowledged[1]);
    assert_eq!(
        cursors(&pull),
        vec![("a", 80), ("b", 200)],
        "a catches up from 80; b asks from the boundary it just committed"
    );
    assert!(!pull.cursors.contains_key("c"));
    assert_eq!(acknowledged.len(), 3);
    assert_eq!(
        acknowledged[2],
        established(&["a", "b"]),
        "the handshake covered the subscribed set, c included in neither"
    );
    let b = lane.client.subscription_state("b").unwrap().unwrap();
    assert_eq!((b.starting_cursor, b.cursor), (Some(200), Some(200)));
    let a = lane.client.subscription_state("a").unwrap().unwrap();
    assert_eq!(
        (a.starting_cursor, a.cursor),
        (Some(80), Some(80)),
        "a's committed progress is preserved, not replaced by 100"
    );
    assert_eq!(
        lane.response(
            id,
            &multi(
                &[("a", 80, 100, 100), ("b", 200, 200, 200)],
                vec![authority(Some("caught up"), 90)]
            )
        ),
        applied(&["a"]),
        "only a moved"
    );
    assert_eq!((lane.cursor("a"), lane.cursor("b")), (Some(100), Some(200)));
    assert!(
        lane.client.subscription_state("c").unwrap().is_none(),
        "c was never touched by any of it"
    );
}

/// Neither lane asks for anything while the only subscription waits for its
/// first boundary: not the automatic catch-up, not a manual delta request.
#[test]
fn an_uninitialized_subscription_produces_no_request_in_either_lane() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    lane.set("a", true);
    let mut cycle = SyncCycle::default();
    cycle.restart();
    assert!(
        cycle.next(&mut lane.client).unwrap().is_none(),
        "a delta request with no initialized subscription is no work"
    );
    let actions = lane.send(DownlinkEvent::Start);
    let (epoch, _) = opened(&actions[0]);
    assert_eq!(
        actions.len(),
        1,
        "the session negotiates before anything is requested"
    );
    // Once the handshake committed the boundary, both lanes ask from it.
    assert_eq!(
        lane.message(epoch, ack(&[("a", 7)])),
        vec![
            DownlinkAction::Changed {
                scopes: vec!["a".into()]
            },
            established(&["a"])
        ]
    );
    let actions = lane.send(DownlinkEvent::Overflow { epoch });
    assert_eq!(cursors(&request(&actions[0]).1), vec![("a", 7)]);
    cycle.restart();
    let manual = cycle.next(&mut lane.client).unwrap().expect("a pull");
    assert_eq!(manual.kind, "pull");
    assert!(manual.body.contains("\"cursors\":{\"a\":7}"), "{manual:?}");
}

/// A duplicate registration changes no membership: the socket keeps streaming,
/// with no close, no reopen and no second subscribe frame.
#[test]
fn a_duplicate_registration_opens_no_new_socket() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    let (epoch, _) = lane.streaming("a", 100);
    let generation = lane.client.subscription_generation();
    lane.set("a", true);
    assert_eq!(
        lane.client.subscription_generation(),
        generation,
        "an identical call is not a membership change"
    );
    assert_eq!(lane.drain(), vec![], "nothing to reconcile");
    assert_eq!(
        lane.frame(epoch, &page("a", 100, 101, Some("still streaming"))),
        applied(&["a"])
    );
}

/// A rolled-back registration is no intent at all: the lane never asks for it.
#[test]
fn a_rolled_back_registration_reaches_no_session() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    let failed: Result<()> = lane.client.transaction(|tx| {
        tx.set_channel("a".into(), true)?;
        Err(invalid("host failure"))
    });
    assert!(failed.is_err());
    assert!(lane.client.subscription_state("a").unwrap().is_none());
    assert_eq!(
        lane.send(DownlinkEvent::Start),
        vec![],
        "nothing subscribed: the lane stays idle"
    );
    lane.set("b", true);
    let (_, subscribe) = opened(&lane.send(DownlinkEvent::Wake)[0]);
    assert_eq!(
        subscribe.channels,
        ["b"],
        "only the committed registration is desired"
    );
}

/// A replica rebuilt from an earlier layout carries the Scope names with no
/// delivery boundary ([rebuild](rebuild.rs)); the lane starts, the first
/// acknowledgement commits their boundaries at the negotiated heads, and
/// streaming resumes from there.
#[test]
fn scopes_carried_through_a_rebuild_initialize_at_the_next_acknowledged_head() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::rebuilt(dir.path());
    assert_eq!((lane.cursor("a"), lane.cursor("b")), (None, None));
    let actions = lane.send(DownlinkEvent::Start);
    let (epoch, subscribe) = opened(&actions[0]);
    assert_eq!(
        subscribe.channels,
        ["a", "b"],
        "the carried Scopes are desired although neither has a boundary"
    );
    assert_eq!(
        lane.message(epoch, ack(&[("a", 30), ("b", 40)])),
        vec![
            DownlinkAction::Changed {
                scopes: vec!["a".into(), "b".into()]
            },
            established(&["a", "b"])
        ],
        "one transaction initializes both, at their own heads"
    );
    assert_eq!((lane.cursor("a"), lane.cursor("b")), (Some(30), Some(40)));
    assert_eq!(
        lane.frame(epoch, &page("a", 30, 31, Some("resumed"))),
        applied(&["a"])
    );
    assert_eq!(lane.text(), "resumed");
}

/// The historical page answering a load of `scope` over `(0, until]`.
fn loaded(scope: &str, until: u64, head: u64) -> String {
    let page = BootstrapPage {
        channel: scope.into(),
        from: 0,
        to: until,
        until,
        head,
        records: vec![],
    };
    String::from_utf8(page.encode().unwrap()).unwrap()
}
/// The id and the kind of every request in these actions, in order.
fn requested(actions: &[DownlinkAction]) -> Vec<(u64, bool)> {
    actions
        .iter()
        .filter_map(|action| match action {
            DownlinkAction::Request {
                request, bootstrap, ..
            } => Some((*request, *bootstrap)),
            _ => None,
        })
        .collect()
}
/// Replace the replica under the lane in place, as the binding's `rebuild`
/// does, and reset the worker for it
/// ([#162](https://github.com/zanminwang/axton/issues/162)).
fn rebuild(lane: &mut Lane) {
    lane.client.rebuild(true).unwrap();
    lane.worker.reset_for_rebuild();
}

/// A rebuild replaces the replica under a running lane: the worker is reset in
/// place, so the lane keeps running without another `start`. The first pump
/// tells the host to abandon the old replica's I/O before anything new, then
/// opens a session for the carried Scopes; nothing the old socket, catch-up or
/// load queued or still delivers applies to the fresh file, because epochs and
/// request ids are never issued twice in the worker's lifetime - across any
/// number of rebuilds ([#162](https://github.com/zanminwang/axton/issues/162)).
#[test]
fn a_rebuild_resets_a_running_lane_in_place_and_fences_what_the_old_replica_had_in_flight() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::rebuildable(&dir.path().join("db"));
    lane.saved("a", 3);
    let id = lane
        .client
        .subscription_state("a")
        .unwrap()
        .unwrap()
        .subscription_id;
    lane.client.request_bootstrap("a", id).unwrap();
    let started = lane.send(DownlinkEvent::Start);
    let [(load, true)] = requested(&started)[..] else {
        panic!("one historical page is asked for: {started:?}")
    };
    let first = started
        .iter()
        .find_map(|a| match a {
            DownlinkAction::Open { epoch, .. } => Some(*epoch),
            _ => None,
        })
        .unwrap_or_else(|| panic!("a session opens: {started:?}"));
    let acknowledged = lane.message(first, ack(&[("a", 5)]));
    let (pull, _) = request(&acknowledged[0]);
    // Queued before the rebuild and never pumped: a streamed page, the
    // catch-up's answer and the historical page's answer.
    lane.enqueue(DownlinkEvent::Message {
        epoch: first,
        body: text(&page("a", 5, 6, Some("stale frame"))),
    });
    lane.enqueue(DownlinkEvent::Response {
        request: pull,
        body: text(&page("a", 3, 5, Some("stale pull"))),
    });
    lane.enqueue(DownlinkEvent::Response {
        request: load,
        body: loaded("a", 3, 5),
    });

    rebuild(&mut lane);
    // Delivered after the reset and before the first pump, while no new
    // session or request exists yet: dropped all the same.
    lane.enqueue(DownlinkEvent::Message {
        epoch: first,
        body: ack(&[("a", 5)]),
    });
    lane.enqueue(DownlinkEvent::Response {
        request: pull,
        body: text(&page("a", 3, 5, Some("racing pull"))),
    });
    lane.enqueue(DownlinkEvent::Response {
        request: load,
        body: loaded("a", 3, 5),
    });
    lane.enqueue(DownlinkEvent::Closed { epoch: first });
    let actions = lane.drain();
    assert_eq!(
        actions[0],
        DownlinkAction::Reset,
        "the host abandons the old I/O before anything new: {actions:?}"
    );
    let (second, subscribe) = opened(&actions[1]);
    assert!(second > first, "a new socket never reuses an epoch");
    assert_eq!(subscribe.channels, ["a"], "the carried Scope is desired");
    assert_eq!(
        actions.len(),
        2,
        "no old close, no old request: {actions:?}"
    );
    assert_eq!(lane.pump(), vec![], "the reset is announced once");
    assert_eq!(
        lane.cursor("a"),
        None,
        "nothing queued before or after the reset applied"
    );
    assert!(lane.client.read(&key()).unwrap().is_none());
    // Whatever the old socket and requests still deliver belongs to nothing.
    for stale in [
        DownlinkEvent::Message {
            epoch: first,
            body: text(&page("a", 5, 6, Some("late frame"))),
        },
        DownlinkEvent::Message {
            epoch: first,
            body: ack(&[("a", 5)]),
        },
        DownlinkEvent::Overflow { epoch: first },
        DownlinkEvent::Closed { epoch: first },
        DownlinkEvent::Response {
            request: pull,
            body: text(&page("a", 3, 5, Some("late pull"))),
        },
        DownlinkEvent::Failed {
            request: pull,
            reason: None,
            status: None,
        },
        DownlinkEvent::Response {
            request: load,
            body: loaded("a", 3, 5),
        },
        DownlinkEvent::Failed {
            request: load,
            reason: None,
            status: Some(400),
        },
    ] {
        assert_eq!(lane.send(stale.clone()), vec![], "{stale:?}");
    }
    assert_eq!(lane.cursor("a"), None);

    // An acknowledgement queued for the rebuilt replica's session is dropped
    // by the next rebuild too: the fence holds however often the replica is
    // replaced under the same worker.
    lane.enqueue(DownlinkEvent::Message {
        epoch: second,
        body: ack(&[("a", 9)]),
    });
    let old = std::mem::replace(&mut lane, Lane::rebuildable(&dir.path().join("again")));
    lane.worker = old.worker;
    lane.set("a", true);
    rebuild(&mut lane);
    let actions = lane.drain();
    assert_eq!(actions[0], DownlinkAction::Reset, "{actions:?}");
    let (third, _) = opened(&actions[1]);
    assert!(third > second);
    assert_eq!(lane.cursor("a"), None, "the queued acknowledgement is gone");
    assert_eq!(
        lane.send(DownlinkEvent::Message {
            epoch: second,
            body: ack(&[("a", 9)]),
        }),
        vec![]
    );
    assert_eq!(
        lane.message(third, ack(&[("a", 9)])),
        vec![
            DownlinkAction::Changed {
                scopes: vec!["a".into()]
            },
            established(&["a"])
        ],
        "the new session commits the carried Scope's first boundary"
    );
    // A gap asks for a repair under an id no earlier request held.
    let repair = lane.frame(third, &page("a", 20, 21, None));
    let (fresh, _) = request(&repair[0]);
    assert!(
        fresh > pull && fresh > load,
        "{fresh} after {pull} and {load}"
    );
}

/// A rebuild keeps the lane's intent: a paused lane stays paused and opens
/// nothing until `resume`, a stopped one stays stopped until `start`, and a
/// lane that never started opens nothing. Each still announces the reset once,
/// so a host can abandon whatever the old replica left in flight
/// ([#162](https://github.com/zanminwang/axton/issues/162)).
#[test]
fn a_rebuild_keeps_a_paused_lane_paused_and_a_stopped_lane_stopped() {
    let dir = tempfile::tempdir().unwrap();
    let mut paused = Lane::rebuildable(&dir.path().join("paused"));
    paused.saved("a", 0);
    let (first, _) = opened(&paused.send(DownlinkEvent::Start)[0]);
    paused.send(DownlinkEvent::Pause);
    rebuild(&mut paused);
    assert_eq!(paused.drain(), vec![DownlinkAction::Reset]);
    assert_eq!(paused.send(DownlinkEvent::Wake), vec![], "still paused");
    let (second, subscribe) = opened(&paused.send(DownlinkEvent::Resume)[0]);
    assert!(second > first);
    assert_eq!(subscribe.channels, ["a"]);

    let mut stopped = Lane::rebuildable(&dir.path().join("stopped"));
    stopped.saved("a", 0);
    let (first, _) = opened(&stopped.send(DownlinkEvent::Start)[0]);
    stopped.send(DownlinkEvent::Stop);
    rebuild(&mut stopped);
    assert_eq!(stopped.drain(), vec![DownlinkAction::Reset]);
    assert_eq!(stopped.send(DownlinkEvent::Wake), vec![], "still stopped");
    assert_eq!(stopped.send(DownlinkEvent::Resume), vec![]);
    let (second, _) = opened(&stopped.send(DownlinkEvent::Start)[0]);
    assert!(second > first);

    let mut idle = Lane::rebuildable(&dir.path().join("idle"));
    idle.set("a", true);
    rebuild(&mut idle);
    assert_eq!(idle.drain(), vec![DownlinkAction::Reset]);
    assert_eq!(idle.send(DownlinkEvent::Wake), vec![], "never started");
}

/// A pump that fails after a rebuild does not lose the reset: whatever the
/// failed pump had collected is dropped, so the next one announces it again,
/// still before it opens anything
/// ([#162](https://github.com/zanminwang/axton/issues/162)).
#[test]
fn a_pump_that_fails_after_a_rebuild_announces_the_reset_on_the_next_one() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::rebuildable(&dir.path().join("db"));
    lane.saved("a", 0);
    let (first, _) = opened(&lane.send(DownlinkEvent::Start)[0]);
    let fresh = lane.client.rebuild(true).unwrap().new_file;
    lane.worker.reset_for_rebuild();
    // The first pump re-evaluates the persisted barriers of the fresh file,
    // which it cannot read while the table is renamed away.
    let mut other = axton_sqlite::SqliteStore::open(std::path::Path::new(&fresh)).unwrap();
    other
        .execute_batch("ALTER TABLE axton_subscription RENAME TO held")
        .unwrap();
    assert!(
        lane.worker
            .handle(&mut lane.client, DownlinkEvent::Next, lane.now, 500)
            .is_err(),
        "the pump fails after it took the reset"
    );
    other
        .execute_batch("ALTER TABLE held RENAME TO axton_subscription")
        .unwrap();
    let actions = lane.drain();
    assert_eq!(
        actions[0],
        DownlinkAction::Reset,
        "the host still abandons the old I/O first: {actions:?}"
    );
    let (second, subscribe) = opened(&actions[1]);
    assert!(second > first);
    assert_eq!(subscribe.channels, ["a"]);
    assert_eq!(lane.pump(), vec![], "and only once");
}

#[test]
fn an_enqueued_page_commits_nothing_until_the_pump_applies_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    lane.saved("a", 0);
    let actions = lane.send(DownlinkEvent::Start);
    let (epoch, _) = opened(&actions[0]);
    lane.enqueue(DownlinkEvent::Message {
        epoch,
        body: ack(&[("a", 0)]),
    });
    assert_eq!(
        lane.drain(),
        vec![established(&["a"])],
        "at the head, with a boundary already committed: nothing to do but announce the handshake"
    );
    // The frame only enters the queue: no transaction runs inside the callback.
    lane.enqueue(DownlinkEvent::Message {
        epoch,
        body: text(&page("a", 0, 1, Some("streamed"))),
    });
    assert_eq!(lane.cursor("a"), Some(0), "the cursor waits for the pump");
    assert_eq!(lane.text(), "local", "the page waits for the pump");
    // The pump applies it: content and cursor in the same commit.
    assert_eq!(lane.pump(), applied(&["a"]));
    assert_eq!(
        (lane.cursor("a"), lane.text()),
        (Some(1), json!("streamed"))
    );
    assert_eq!(lane.pump(), vec![], "nothing left to pump");
}

#[test]
fn one_pump_applies_one_page_so_foreground_work_interleaves() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    let (epoch, _) = lane.streaming("a", 0);
    for at in 0..3u64 {
        lane.enqueue(DownlinkEvent::Message {
            epoch,
            body: text(&page("a", at, at + 1, Some("bulk"))),
        });
    }
    for at in 0..3u64 {
        assert_eq!(lane.pump(), applied(&["a"]), "one commit per pump");
        assert_eq!(lane.cursor("a"), Some(at + 1));
    }
    assert_eq!(lane.pump(), vec![]);
}

#[test]
fn the_worker_outlives_the_socket_it_was_streaming_on() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    let (first, _) = lane.streaming("a", 0);
    assert_eq!(
        lane.frame(first, &page("a", 0, 1, Some("first"))),
        applied(&["a"])
    );
    // The socket is replaced; the worker keeps its durable position and its
    // schedule, and opens the next session itself.
    let closed = lane.send(DownlinkEvent::Closed { epoch: first });
    assert_eq!(
        closed[0],
        DownlinkAction::Close {
            epoch: first,
            reason: None
        }
    );
    lane.now += wait(&closed[1]);
    let actions = lane.drain();
    let (second, subscribe) = opened(&actions[0]);
    assert!(second > first, "a new socket, a new epoch");
    assert_eq!(subscribe.channels, ["a"]);
    let actions = lane.message(second, ack(&[("a", 2)]));
    assert_eq!(
        cursors(&request(&actions[0]).1),
        vec![("a", 1)],
        "the worker resumes from the cursor it committed on the old socket"
    );
    assert_eq!(
        lane.response(request(&actions[0]).0, &page("a", 1, 2, Some("second"))),
        applied(&["a"])
    );
    assert_eq!((lane.cursor("a"), lane.text()), (Some(2), json!("second")));
}

#[test]
fn control_work_flows_while_a_gap_page_waits_for_its_repair() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    let (epoch, _) = lane.streaming("a", 0);
    // A gap holds the front of the page queue and one repair request runs.
    let actions = lane.frame(epoch, &page("a", 5, 6, Some("gap")));
    let (id, repair) = request(&actions[0]);
    assert_eq!(cursors(&repair), vec![("a", 0)]);
    // Frames behind the gap queue up; nothing commits while the repair is out.
    for at in 6..9u64 {
        assert_eq!(
            lane.frame(epoch, &page("a", at, at + 1, Some("behind"))),
            vec![]
        );
    }
    assert_eq!(lane.cursor("a"), Some(0));
    // A stale completion belongs to no request in flight and changes nothing.
    assert_eq!(
        lane.response(id + 7, &page("a", 0, 5, Some("stale"))),
        vec![]
    );
    assert_eq!(lane.cursor("a"), Some(0));
    // The repair the gap waited for is processed although the gap page is
    // still at the front of the queue, and the queue then connects.
    let actions = lane.response(id, &page("a", 0, 5, Some("repaired")));
    assert_eq!(&actions[..2], applied(&["a"]));
    assert_eq!(lane.cursor("a"), Some(9));
    assert_eq!(lane.text(), "behind");
}

#[test]
fn a_socket_that_closes_while_a_repair_is_out_ends_the_session() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    let (epoch, _) = lane.streaming("a", 0);
    let actions = lane.frame(epoch, &page("a", 5, 6, Some("gap")));
    let (id, _) = request(&actions[0]);
    // The socket ends: the control event is delivered although a page is held
    // and an HTTP request is still out.
    let closed = lane.send(DownlinkEvent::Closed { epoch });
    assert_eq!(
        closed[0],
        DownlinkAction::Close {
            epoch,
            reason: None
        }
    );
    assert!((200..=300).contains(&wait(&closed[1])));
    // Its answer belongs to a session that is gone: nothing but the retry is left.
    let late = lane.response(id, &page("a", 0, 5, Some("late")));
    assert_eq!(late.len(), 1, "{late:?}");
    assert!((200..=300).contains(&wait(&late[0])));
    assert_eq!(lane.cursor("a"), Some(0));
}

#[test]
fn an_http_failure_retries_the_session_and_a_stale_failure_is_ignored() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    // The acknowledged head is beyond the cursor: one catch-up is in flight.
    lane.saved("a", 0);
    let (epoch, actions) = lane.streaming("a", 3);
    let (id, _) = request(&actions[0]);
    assert_eq!(
        lane.send(DownlinkEvent::Failed {
            request: id + 1,
            reason: Some("timeout".into()),
            status: None
        }),
        vec![],
        "a failure of another request is not this session's"
    );
    let failed = lane.send(DownlinkEvent::Failed {
        request: id,
        reason: Some("live failed: 503".into()),
        status: Some(503),
    });
    assert_eq!(
        failed[0],
        DownlinkAction::Close {
            epoch,
            reason: None
        },
        "the host already reported the failure; the session ends"
    );
    assert!((200..=300).contains(&wait(&failed[1])));
    assert_eq!(lane.cursor("a"), Some(0));
}

#[test]
fn a_session_subscribes_pulls_only_when_behind_and_then_streams() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    lane.enqueue(DownlinkEvent::Start);
    assert_eq!(
        lane.drain(),
        vec![],
        "no channels: the lane stays idle until a subscribe wakes it"
    );
    lane.saved("a", 0);
    let actions = lane.send(DownlinkEvent::Wake);
    let (epoch, subscribe) = opened(&actions[0]);
    assert_eq!(subscribe.channels, ["a"]);
    assert_eq!(actions.len(), 1);
    // A streamed page before the acknowledgement is a protocol violation.
    let early = lane.frame(epoch, &page("a", 5, 6, Some("early")));
    assert_eq!(
        early[0],
        DownlinkAction::Close {
            epoch,
            reason: Some("live page before acknowledgement".into())
        }
    );
    let backoff = wait(&early[1]);
    assert!((200..=300).contains(&backoff), "{backoff}");
    lane.now += backoff;
    let actions = lane.drain();
    let (epoch, _) = opened(&actions[0]);
    assert_eq!(epoch, 2, "each session has its own epoch");
    // The head is beyond the durable cursor: one pull from the cursor.
    let actions = lane.message(epoch, ack(&[("a", 4)]));
    let (id, pull) = request(&actions[0]);
    assert_eq!(cursors(&pull), vec![("a", 0)]);
    assert_eq!(actions[1..], [established(&["a"])]);
    // Frames streamed while the pull is in flight wait in the queue.
    assert_eq!(
        lane.frame(epoch, &page("a", 3, 4, Some("streamed"))),
        vec![]
    );
    assert_eq!(lane.frame(epoch, &page("a", 5, 6, Some("beyond"))), vec![]);
    // The pull lands; the queue is re-read: the first frame is covered, the
    // second has a gap and stays while one more pull runs from the cursor.
    let actions = lane.response(id, &page("a", 0, 4, Some("caught up")));
    committed(&actions, &["a"]);
    let (id, again) = request(&actions[2]);
    assert_eq!(
        cursors(&again),
        vec![("a", 4)],
        "the held gap recovers from the durable cursor"
    );
    assert_eq!(actions.len(), 3, "the covered frame did nothing");
    assert_eq!(lane.cursor("a"), Some(4));
    assert_eq!(lane.text(), "caught up");
    // The pull connects the held frame, which is applied right after, in a
    // commit of its own.
    let actions = lane.response(id, &page("a", 4, 5, Some("recovered gap")));
    assert_eq!(actions, [applied(&["a"]), applied(&["a"])].concat());
    assert_eq!(lane.cursor("a"), Some(6));
    assert_eq!(lane.text(), "beyond");
    // Streaming: applied frames wake the push lane, covered frames do nothing.
    assert_eq!(
        lane.frame(epoch, &page("a", 6, 7, Some("live"))),
        applied(&["a"])
    );
    assert_eq!(lane.frame(epoch, &page("a", 6, 7, Some("dup"))), vec![]);
    assert_eq!(lane.text(), "live");
    // A gap holds the frame and pulls from the durable cursor; when the pull
    // covers the frame, the frame is discarded.
    let actions = lane.frame(epoch, &page("a", 9, 10, Some("gap")));
    let (id, recover) = request(&actions[0]);
    assert_eq!(cursors(&recover), vec![("a", 7)]);
    assert_eq!(actions.len(), 1);
    assert_eq!(
        lane.response(id, &page("a", 7, 10, Some("recovered"))),
        applied(&["a"])
    );
    assert_eq!(lane.cursor("a"), Some(10));
    assert_eq!(lane.text(), "recovered");
}

#[test]
fn heads_equal_to_the_cursors_mean_no_catch_up_at_all() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    lane.saved("a", 3);
    lane.saved("b", 2);
    let (epoch, subscribe) = opened(&lane.send(DownlinkEvent::Start)[0]);
    assert_eq!(subscribe.channels, ["a", "b"]);
    assert_eq!(
        lane.message(epoch, ack(&[("a", 3), ("b", 2)])),
        vec![established(&["a", "b"])],
        "every channel is at its head: the stream is the truth"
    );
    assert_eq!(
        lane.frame(
            epoch,
            &multi(&[("a", 3, 4, 4)], vec![authority(Some("A"), 2)])
        ),
        applied(&["a"])
    );
    assert_eq!((lane.cursor("a"), lane.cursor("b")), (Some(4), Some(2)));
}

#[test]
fn one_pull_covers_every_channel_and_continues_while_any_channel_is_full() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    lane.saved("b", 0);
    lane.saved("a", 0);
    let actions = lane.send(DownlinkEvent::Start);
    let (epoch, subscribe) = opened(&actions[0]);
    assert_eq!(
        subscribe.channels,
        ["a", "b"],
        "the frame carries the normalized set"
    );
    // `b` is at its head; `a` is behind: still one pull naming both.
    let actions = lane.message(epoch, ack(&[("b", 0), ("a", 51)]));
    let (id, pull) = request(&actions[0]);
    assert_eq!(cursors(&pull), vec![("a", 0), ("b", 0)]);
    assert_eq!(
        actions[1..],
        [established(&["a", "b"])],
        "one request in flight at a time"
    );
    let mut first = full("a", 0);
    first.cursors.insert(
        "b".into(),
        CursorRange {
            from: 0,
            to: 0,
            head: 0,
        },
    );
    let actions = lane.response(id, &first);
    committed(&actions, &["a"]);
    let (id, next) = request(&actions[2]);
    assert_eq!(
        cursors(&next),
        vec![("a", 50), ("b", 0)],
        "a full channel continues"
    );
    assert_eq!(
        lane.response(id, &multi(&[("a", 50, 51, 51), ("b", 0, 0, 0)], vec![])),
        applied(&["a"]),
        "no channel continues: streaming"
    );
}

#[test]
fn overflow_discards_the_queue_and_recovers_every_channel_after_the_request_in_flight() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    lane.saved("a", 0);
    lane.saved("b", 0);
    let (epoch, _) = opened(&lane.send(DownlinkEvent::Start)[0]);
    let actions = lane.message(epoch, ack(&[("a", 1), ("b", 0)]));
    let (id, _) = request(&actions[0]);
    assert_eq!(lane.frame(epoch, &page("a", 1, 2, Some("queued"))), vec![]);
    assert_eq!(
        lane.send(DownlinkEvent::Overflow { epoch }),
        vec![],
        "the request keeps going"
    );
    // The answer lands; the queued frame is gone and another pull follows,
    // because frames may have been lost beyond this answer.
    let actions = lane.response(id, &multi(&[("a", 0, 1, 1), ("b", 0, 0, 0)], vec![]));
    committed(&actions, &["a"]);
    let (id, again) = request(&actions[2]);
    assert_eq!(cursors(&again), vec![("a", 1), ("b", 0)]);
    assert_eq!(actions.len(), 3);
    assert_eq!(
        lane.response(
            id,
            &multi(
                &[("a", 1, 2, 2), ("b", 0, 0, 0)],
                vec![authority(Some("recovered"), 1)]
            )
        ),
        applied(&["a"])
    );
    assert_eq!(lane.text(), "recovered");
    // While streaming, an overflow pulls at once; a second one while pulling
    // asks for one more pull, not two.
    let actions = lane.send(DownlinkEvent::Overflow { epoch });
    let (id, _) = request(&actions[0]);
    assert_eq!(actions.len(), 1);
    assert_eq!(lane.send(DownlinkEvent::Overflow { epoch }), vec![]);
    assert_eq!(lane.send(DownlinkEvent::Overflow { epoch }), vec![]);
    let actions = lane.response(id, &multi(&[("a", 2, 2, 2), ("b", 0, 0, 0)], vec![]));
    let (id, _) = request(&actions[0]);
    assert_eq!(actions.len(), 1);
    assert_eq!(
        lane.response(id, &multi(&[("a", 2, 2, 2), ("b", 0, 0, 0)], vec![])),
        vec![]
    );
}

#[test]
fn the_frame_queue_is_bounded_and_overflows_into_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    lane.saved("a", 0);
    let (epoch, _) = opened(&lane.send(DownlinkEvent::Start)[0]);
    assert_eq!(
        lane.message(epoch, ack(&[("a", 0)])),
        vec![established(&["a"])]
    );
    // A gap frame stays queued behind one pull; the frames after it queue up.
    let actions = lane.frame(epoch, &page("a", 5, 6, Some("gap")));
    let (id, _) = request(&actions[0]);
    for i in 0..QUEUED_FRAMES as u64 - 1 {
        assert_eq!(
            lane.frame(epoch, &page("a", 6 + i, 7 + i, Some("more"))),
            vec![]
        );
    }
    // One more frame than the bound: the queue is discarded; the pull in
    // flight keeps going and another follows it.
    assert_eq!(
        lane.frame(epoch, &page("a", 100, 101, Some("overflow"))),
        vec![]
    );
    let actions = lane.response(id, &page("a", 0, 5, Some("pulled")));
    committed(&actions, &["a"]);
    let (id, again) = request(&actions[2]);
    assert_eq!(cursors(&again), vec![("a", 5)]);
    assert_eq!(
        actions.len(),
        3,
        "no held frame applied: the queue was discarded"
    );
    assert_eq!(lane.response(id, &empty("a", 5)), vec![]);
}

#[test]
fn reports_reach_the_host_as_actions() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    lane.saved("a", 0);
    let (epoch, _) = opened(&lane.send(DownlinkEvent::Start)[0]);
    lane.message(epoch, ack(&[("a", 0)]));
    lane.frame(epoch, &page("a", 0, 1, Some("A")));
    let failed = AuthorityRecord {
        model: "Entry".into(),
        identity: json!({"id":"e"}),
        stamp: 7,
        state: Value::Null,
        error: Some("loader.failed".into()),
    };
    let mut bad = authority_of("x", Some("bad"), 2);
    bad.state = json!({"text":1});
    let actions = lane.frame(epoch, &multi(&[("a", 1, 3, 3)], vec![failed, bad]));
    committed(&actions, &["a"]);
    let reported = reports(&actions[2]);
    assert_eq!(reported.len(), 2);
    assert_eq!(reported[0].kind, ReportKind::ReadFailed);
    assert_eq!(reported[0].code.as_deref(), Some("loader.failed"));
    assert_eq!(reported[1].kind, ReportKind::Skipped);
    assert_eq!(reported[1].identity, json!({"id":"x"}));
    assert_eq!(lane.text(), "A");
    assert_eq!(lane.cursor("a"), Some(3));
    // Reports come back from a pull too.
    let actions = lane.frame(epoch, &page("a", 9, 10, Some("gap")));
    let (id, _) = request(&actions[0]);
    let actions = lane.response(
        id,
        &multi(&[("a", 3, 10, 10)], vec![authority(Some("other"), 1)]),
    );
    committed(&actions, &["a"]);
    assert_eq!(reports(&actions[2])[0].kind, ReportKind::Conflict);
}

#[test]
fn a_subscription_change_ends_the_session_and_the_next_one_uses_the_new_set() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    lane.saved("a", 0);
    let (first, _) = opened(&lane.send(DownlinkEvent::Start)[0]);
    let actions = lane.message(first, ack(&[("a", 3)]));
    let (id, pending) = request(&actions[0]);
    lane.set("b", true);
    // Whatever event comes next observes the committed change: the old session
    // closes without backoff and a new one opens with both channels.
    let actions = lane.send(DownlinkEvent::Wake);
    assert_eq!(
        actions[0],
        DownlinkAction::Close {
            epoch: first,
            reason: None
        }
    );
    let (second, subscribe) = opened(&actions[1]);
    assert_eq!(subscribe.channels, ["a", "b"]);
    assert_eq!(actions.len(), 2);
    // The old session's late answers and frames are ignored.
    assert_eq!(
        lane.response(id, &page("a", pending.cursors["a"], 3, Some("late"))),
        vec![]
    );
    assert_eq!(
        lane.frame(first, &page("a", 0, 1, Some("old socket"))),
        vec![]
    );
    assert_eq!(lane.send(DownlinkEvent::Closed { epoch: first }), vec![]);
    assert_eq!(lane.cursor("a"), Some(0));
    // Unsubscribing everything ends the session and leaves the lane idle.
    lane.message(second, ack(&[("a", 0), ("b", 0)]));
    lane.set("a", false);
    lane.set("b", false);
    assert_eq!(
        lane.drain(),
        vec![DownlinkAction::Close {
            epoch: second,
            reason: None
        }]
    );
    assert_eq!(lane.drain(), vec![]);
}

#[test]
fn a_dropped_socket_reconnects_with_backoff_and_resubscribes() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    lane.saved("a", 0);
    let (first, _) = opened(&lane.send(DownlinkEvent::Start)[0]);
    let actions = lane.message(first, ack(&[("a", 3)]));
    let (id, _) = request(&actions[0]);
    lane.response(id, &page("a", 0, 3, Some("before")));
    let actions = lane.send(DownlinkEvent::Closed { epoch: first });
    assert_eq!(
        actions[0],
        DownlinkAction::Close {
            epoch: first,
            reason: None
        },
        "the host closes whatever is left of the session"
    );
    let backoff = wait(&actions[1]);
    assert!((200..=300).contains(&backoff), "{backoff}");
    assert_eq!(lane.drain(), vec![DownlinkAction::Wait { millis: backoff }]);
    lane.now += backoff;
    let actions = lane.drain();
    let (second, subscribe) = opened(&actions[0]);
    assert_eq!(
        subscribe.channels,
        ["a"],
        "resubscribes without an application event"
    );
    let actions = lane.message(second, ack(&[("a", 5)]));
    assert_eq!(
        cursors(&request(&actions[0]).1),
        vec![("a", 3)],
        "catch-up resumes from the durable cursor"
    );
    // A second failure doubles the wait; a success resets it.
    let actions = lane.send(DownlinkEvent::Closed { epoch: second });
    assert!(wait(&actions[1]) > backoff);
}

#[test]
fn protocol_violations_close_with_a_reason_and_retry() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    lane.saved("a", 0);
    let mut actions = lane.send(DownlinkEvent::Start);
    for (frame, reason) in [
        (
            ack(&[("a", 0), ("b", 0)]),
            "invalid live subscription acknowledgement",
        ),
        (
            r#"{"type":"subscribed","cursors":{"a":-1}}"#.into(),
            "invalid live subscription acknowledgement",
        ),
        (
            r#"{"cursors":{"a":{"from":2,"to":1,"head":1}},"changes":[]}"#.into(),
            "invalid live page: page moves backwards",
        ),
    ] {
        let (epoch, _) = opened(&actions[0]);
        let closed = lane.message(epoch, frame);
        assert_eq!(
            closed[0],
            DownlinkAction::Close {
                epoch,
                reason: Some(reason.into())
            }
        );
        lane.now += wait(&closed[1]);
        actions = lane.drain();
    }
    let (epoch, _) = opened(&actions[0]);
    let actions = lane.message(epoch, ack(&[("a", 1)]));
    request(&actions[0]);
    let actions = lane.message(epoch, ack(&[("a", 1)]));
    assert_eq!(
        actions[0],
        DownlinkAction::Close {
            epoch,
            reason: Some("invalid live subscription acknowledgement".into())
        },
        "a second acknowledgement is a violation"
    );
    lane.now += wait(&actions[1]);
    // A catch-up answer that does not answer the request ends the session too.
    let (epoch, _) = opened(&lane.drain()[0]);
    let actions = lane.message(epoch, ack(&[("a", 1)]));
    let (id, _) = request(&actions[0]);
    let actions = lane.response(id, &empty("other", 0));
    assert_eq!(
        actions[0],
        DownlinkAction::Close {
            epoch,
            reason: Some("response does not match pull request".into())
        }
    );
}

#[test]
fn pause_ends_the_session_without_backoff_resume_reopens_and_stop_is_final() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    lane.saved("a", 0);
    let (first, _) = opened(&lane.send(DownlinkEvent::Start)[0]);
    lane.message(first, ack(&[("a", 0)]));
    assert_eq!(
        lane.send(DownlinkEvent::Pause),
        vec![DownlinkAction::Close {
            epoch: first,
            reason: None
        }]
    );
    assert_eq!(
        lane.send(DownlinkEvent::Wake),
        vec![],
        "paused: a wake schedules nothing"
    );
    assert_eq!(lane.send(DownlinkEvent::Closed { epoch: first }), vec![]);
    let actions = lane.send(DownlinkEvent::Resume);
    let (second, _) = opened(&actions[0]);
    assert!(second > first);
    assert_eq!(
        lane.send(DownlinkEvent::Stop),
        vec![DownlinkAction::Close {
            epoch: second,
            reason: None
        }]
    );
    assert_eq!(lane.drain(), vec![]);
    assert_eq!(lane.send(DownlinkEvent::Wake), vec![]);
    assert_eq!(lane.send(DownlinkEvent::Resume), vec![]);
}

/// An unsubscribe and a recreation replace the subscription: the recreated one
/// is a new identity with no boundary, so the next session initializes it at
/// the head it acknowledges, and whatever the old one had in flight applies to
/// nothing.
#[test]
fn a_recreated_subscription_starts_over_at_the_next_acknowledged_head() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    lane.saved("a", 0);
    let before = lane.client.subscription_state("a").unwrap().unwrap();
    let (first, _) = opened(&lane.send(DownlinkEvent::Start)[0]);
    let actions = lane.message(first, ack(&[("a", 9)]));
    let (id, issued) = request(&actions[0]);
    // Unsubscribe and resubscribe while the request is in flight.
    lane.set("a", false);
    lane.set("a", true);
    let actions = lane.drain();
    assert_eq!(
        actions[0],
        DownlinkAction::Close {
            epoch: first,
            reason: None
        }
    );
    let (second, _) = opened(&actions[1]);
    let after = lane.client.subscription_state("a").unwrap().unwrap();
    assert_ne!(after.subscription_id, before.subscription_id);
    assert_eq!(
        (after.starting_cursor, after.cursor),
        (None, None),
        "the recreated subscription has no boundary to catch up from"
    );
    let actions = lane.message(second, ack(&[("a", 9)]));
    assert_eq!(
        actions,
        vec![
            DownlinkAction::Changed {
                scopes: vec!["a".into()]
            },
            established(&["a"])
        ],
        "it initializes at the head instead of reloading the Scope"
    );
    assert_eq!(lane.cursor("a"), Some(9));
    // The old request's answer reaches no request in flight, and its range is
    // below the new boundary anyway: nothing of it applies.
    assert_eq!(
        lane.response(id, &page("a", issued.cursors["a"], 9, Some("obsolete"))),
        vec![]
    );
    assert_eq!(lane.text(), "local");
    assert_eq!(
        lane.frame(second, &page("a", 9, 10, Some("from now"))),
        applied(&["a"])
    );
    assert_eq!(lane.text(), "from now");
}

#[test]
fn every_event_of_a_replaced_socket_is_fenced_by_its_epoch() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    lane.saved("a", 0);
    let (first, _) = opened(&lane.send(DownlinkEvent::Start)[0]);
    let acknowledged = lane.message(first, ack(&[("a", 1)]));
    let (stale_id, _) = request(&acknowledged[0]);
    let actions = lane.send(DownlinkEvent::Closed { epoch: first });
    lane.now += wait(&actions[1]);
    let (second, _) = opened(&lane.drain()[0]);
    assert!(second > first, "a new socket, a new epoch");
    assert_eq!(
        lane.message(second, ack(&[("a", 0)])),
        vec![established(&["a"])]
    );
    // Whatever the abandoned socket and its request still deliver belongs to
    // no session; the one that replaced it keeps streaming.
    assert_eq!(lane.frame(first, &page("a", 0, 1, Some("late"))), vec![]);
    assert_eq!(lane.message(first, ack(&[("a", 1)])), vec![]);
    assert_eq!(lane.send(DownlinkEvent::Overflow { epoch: first }), vec![]);
    assert_eq!(
        lane.response(stale_id, &page("a", 0, 1, Some("late pull"))),
        vec![]
    );
    assert_eq!(lane.send(DownlinkEvent::Closed { epoch: first }), vec![]);
    assert_eq!(lane.text(), "local", "nothing of the old socket applied");
    assert_eq!(
        lane.frame(second, &page("a", 0, 1, Some("second socket"))),
        applied(&["a"])
    );
    assert_eq!(
        (lane.cursor("a"), lane.text()),
        (Some(1), json!("second socket"))
    );
}

#[test]
fn an_applied_page_leaves_the_push_lane_and_its_frozen_request_alone() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    lane.saved("a", 0);
    lane.client
        .transaction(|tx| tx.enqueue(mutation("queued")))
        .unwrap();
    // The push lane freezes a batch and keeps it in flight.
    let mut cycle = SyncCycle::default();
    cycle.restart_push_only();
    let push = cycle
        .next(&mut lane.client)
        .unwrap()
        .expect("a frozen batch");
    assert_eq!(push.kind, "push");
    let (epoch, _) = opened(&lane.send(DownlinkEvent::Start)[0]);
    let actions = lane.message(epoch, ack(&[("a", 1)]));
    let (id, _) = request(&actions[0]);
    assert_eq!(
        lane.response(id, &page("a", 0, 1, Some("caught up"))),
        applied(&["a"]),
        "the downlink only wakes the push lane"
    );
    assert_eq!(
        lane.frame(epoch, &page("a", 1, 2, Some("streamed"))),
        applied(&["a"])
    );
    assert_eq!(lane.cursor("a"), Some(2));
    // The push lane made its own progress: the same frozen bytes are resent.
    assert_eq!(
        cycle.next(&mut lane.client).unwrap().map(|a| a.body),
        Some(push.body),
        "the downlink never borrows the push cycle"
    );
}

#[test]
fn a_restarted_lane_does_not_close_the_socket_of_the_lane_it_replaced() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    let (first, _) = lane.streaming("a", 0);
    // The lane closed without pumping its stop, as a host that closes does.
    lane.enqueue(DownlinkEvent::Stop);
    let actions = lane.send(DownlinkEvent::Start);
    let (second, subscribe) = opened(&actions[0]);
    assert!(second > first, "the next lane opens its own socket");
    assert_eq!(subscribe.channels, ["a"]);
    assert_eq!(
        actions.len(),
        1,
        "nothing of the replaced lane's session is announced to this one"
    );
}

#[test]
fn a_socket_the_subscription_change_abandoned_reconnects_without_backoff() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    let (first, _) = lane.streaming("a", 0);
    // The host abandons the socket as the application subscribes; the change
    // commits and the dead socket is reported before any wake reaches Rust.
    lane.set("b", true);
    let actions = lane.send(DownlinkEvent::Closed { epoch: first });
    assert_eq!(
        actions[0],
        DownlinkAction::Close {
            epoch: first,
            reason: None
        }
    );
    let (second, subscribe) = opened(&actions[1]);
    assert!(second > first);
    assert_eq!(subscribe.channels, ["a", "b"]);
    assert_eq!(
        actions.len(),
        2,
        "the change invalidated the session, not the transport: no backoff"
    );
    // The failed-attempt count was not touched either, so a real transport
    // failure still waits the first backoff rather than a doubled one.
    let failed = lane.send(DownlinkEvent::Closed { epoch: second });
    let backoff = wait(&failed[1]);
    assert!((200..=300).contains(&backoff), "{backoff}");
}

/// An unsubscribe and a subscribe called back to back commit before the next
/// pump, and a catch-up the replaced session issued is still outstanding: the
/// one pump ends that session and opens the next one for the new membership. A
/// request in flight holds nothing back, because the session it belonged to is
/// gone with it.
#[test]
fn a_commit_pair_replaces_the_session_with_a_catch_up_still_in_flight() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    lane.saved("a", 0);
    let (first, _) = opened(&lane.send(DownlinkEvent::Start)[0]);
    let acknowledged = lane.message(first, ack(&[("a", 1)]));
    let (held, _) = request(&acknowledged[0]);
    lane.set("a", false);
    lane.set("a", true);
    let actions = lane.pump();
    assert_eq!(
        actions[0],
        DownlinkAction::Close {
            epoch: first,
            reason: None
        },
        "the session the commits invalidated ends"
    );
    let (second, subscribe) = opened(&actions[1]);
    assert!(second > first);
    assert_eq!(subscribe.channels, ["a"]);
    assert_eq!(
        actions.len(),
        2,
        "the change ended the session, not the transport: no backoff, no wait"
    );
    // The answer to the request the replaced session issued reaches no request
    // in flight and commits nothing.
    assert_eq!(
        lane.response(held, &page("a", 0, 1, Some("obsolete"))),
        vec![]
    );
    assert_eq!(lane.text(), "local");
    // The recreated subscription initializes at the head of its own handshake.
    assert_eq!(
        lane.message(second, ack(&[("a", 5)])),
        vec![
            DownlinkAction::Changed {
                scopes: vec!["a".into()]
            },
            established(&["a"])
        ]
    );
    assert_eq!(lane.cursor("a"), Some(5));
}

/// The same pair with a pump in between, so the lane sees an empty desired set:
/// it closes the session, opens nothing while no Scope is subscribed, and opens
/// the next session on the wake the recreating commit's host sends. The lane is
/// passive - a commit decides nothing until a pump - so the SDK wakes it for
/// every committed membership change.
#[test]
fn a_lane_left_with_no_scope_opens_the_next_session_on_the_recreating_commit() {
    let dir = tempfile::tempdir().unwrap();
    let mut lane = Lane::new(dir.path());
    lane.saved("a", 0);
    let (first, _) = opened(&lane.send(DownlinkEvent::Start)[0]);
    let acknowledged = lane.message(first, ack(&[("a", 1)]));
    let (held, _) = request(&acknowledged[0]);
    lane.set("a", false);
    assert_eq!(
        lane.pump(),
        vec![DownlinkAction::Close {
            epoch: first,
            reason: None
        }],
        "with no Scope left the session ends and nothing is opened"
    );
    assert_eq!(lane.pump(), vec![], "an unsubscribed lane waits, idle");
    lane.set("a", true);
    let actions = lane.send(DownlinkEvent::Wake);
    let (second, subscribe) = opened(&actions[0]);
    assert!(second > first);
    assert_eq!(subscribe.channels, ["a"]);
    assert_eq!(actions.len(), 1, "no backoff on the way back");
    assert_eq!(
        lane.response(held, &page("a", 0, 1, Some("obsolete"))),
        vec![],
        "the catch-up of the session that went commits nothing"
    );
    assert_eq!(lane.text(), "local");
}
