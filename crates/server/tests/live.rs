//! Transition tests for the per-socket live controller
//! ([Server / Connection / Controller](../../../docs/engineering/architecture/server/connection/controller.md)).
//! Pure state: no host, no socket, no database.
use axton_core::{AuthorityRecord, CursorRange, PullPage, limits};
use axton_server::live::{LiveAction, LiveEvent, Negotiation, Subscriptions};
use serde_json::json;
use std::collections::BTreeMap;

/// The read contracts every negotiation in these tests declares.
fn models() -> BTreeMap<String, u64> {
    BTreeMap::from([("Task".to_string(), 1)])
}

fn negotiation(heads: &[(&str, u64)]) -> Negotiation {
    Negotiation {
        models: models(),
        response: r#"{"cursors":{},"type":"subscribed"}"#.into(),
        heads: heads.iter().map(|(s, h)| ((*s).to_string(), *h)).collect(),
    }
}

/// A page for the given channels: `(channel, from, to, head)`, holding one
/// change per cursor in `from + 1 ..= to` of every channel, capped per channel.
fn page(ranges: &[(&str, u64, u64, u64)]) -> String {
    let mut changes = vec![];
    for (channel, from, to, _) in ranges {
        for cursor in (*from + 1..=*to).take(limits::PULL_CHANGES) {
            changes.push(AuthorityRecord {
                model: "Task".into(),
                identity: json!({"id": format!("{channel}-{cursor}")}),
                stamp: cursor,
                state: json!(null),
                error: None,
            });
        }
    }
    let page = PullPage {
        cursors: ranges
            .iter()
            .map(|(c, from, to, head)| {
                (
                    (*c).to_string(),
                    CursorRange {
                        from: *from,
                        to: *to,
                        head: *head,
                    },
                )
            })
            .collect(),
        changes,
    };
    String::from_utf8(page.encode().unwrap()).unwrap()
}

fn pull(cursors: &[(&str, u64)]) -> LiveAction {
    LiveAction::Pull {
        cursors: cursors
            .iter()
            .map(|(c, n)| ((*c).to_string(), *n))
            .collect(),
        models: models(),
    }
}

fn committed(scope: &str) -> LiveEvent {
    LiveEvent::Committed {
        scope: scope.into(),
    }
}

fn pulled(page: &str) -> LiveEvent {
    LiveEvent::Pulled { page: page.into() }
}

#[test]
fn open_registers_every_scope_before_the_acknowledgement_then_pulls_all_once_from_their_heads() {
    let (subscriptions, actions) = Subscriptions::open(negotiation(&[("a", 3), ("b", 0)]));
    assert_eq!(
        actions,
        vec![
            LiveAction::Listen { scope: "a".into() },
            LiveAction::Listen { scope: "b".into() },
            LiveAction::Send {
                frame: r#"{"cursors":{},"type":"subscribed"}"#.into()
            },
            pull(&[("a", 3), ("b", 0)]),
        ]
    );
    assert!(subscriptions.is_pulling());
    assert!(!subscriptions.is_closed());
}

#[test]
fn a_commit_observed_during_a_pull_produces_one_more_pull_after_it() {
    let (mut subscriptions, _) = Subscriptions::open(negotiation(&[("a", 3)]));
    assert_eq!(subscriptions.handle(committed("a")).unwrap(), vec![]);
    let actions = subscriptions
        .handle(pulled(&page(&[("a", 3, 4, 4)])))
        .unwrap();
    assert_eq!(
        actions,
        vec![
            LiveAction::Send {
                frame: page(&[("a", 3, 4, 4)])
            },
            pull(&[("a", 4)])
        ]
    );
    let actions = subscriptions
        .handle(pulled(&page(&[("a", 4, 4, 4)])))
        .unwrap();
    assert_eq!(actions, vec![], "a page that did not advance is not sent");
    assert!(!subscriptions.is_pulling());
}

#[test]
fn two_commits_during_one_pull_produce_one_extra_pull_not_two() {
    let (mut subscriptions, _) = Subscriptions::open(negotiation(&[("a", 0)]));
    assert_eq!(subscriptions.handle(committed("a")).unwrap(), vec![]);
    assert_eq!(subscriptions.handle(committed("a")).unwrap(), vec![]);
    let actions = subscriptions
        .handle(pulled(&page(&[("a", 0, 2, 2)])))
        .unwrap();
    assert_eq!(
        actions,
        vec![
            LiveAction::Send {
                frame: page(&[("a", 0, 2, 2)])
            },
            pull(&[("a", 2)])
        ]
    );
    let actions = subscriptions
        .handle(pulled(&page(&[("a", 2, 2, 2)])))
        .unwrap();
    assert_eq!(actions, vec![]);
    assert!(!subscriptions.is_pulling());
    assert!(!subscriptions.scopes()[0].pending);
}

#[test]
fn a_channel_below_its_head_continues_and_one_at_its_head_ends_the_drain() {
    let (mut subscriptions, _) = Subscriptions::open(negotiation(&[("a", 0)]));
    let full = limits::PULL_CHANGES as u64;
    let actions = subscriptions
        .handle(pulled(&page(&[("a", 0, full, full + 1)])))
        .unwrap();
    assert_eq!(
        actions,
        vec![
            LiveAction::Send {
                frame: page(&[("a", 0, full, full + 1)])
            },
            pull(&[("a", full)])
        ]
    );
    let actions = subscriptions
        .handle(pulled(&page(&[("a", full, full + 1, full + 1)])))
        .unwrap();
    assert_eq!(
        actions,
        vec![LiveAction::Send {
            frame: page(&[("a", full, full + 1, full + 1)])
        }]
    );
    assert!(!subscriptions.is_pulling());
    assert_eq!(subscriptions.scopes()[0].cursor, full + 1);
    assert_eq!(
        subscriptions.handle(committed("a")).unwrap(),
        vec![pull(&[("a", full + 1)])],
        "the next commit pulls again from the streamed cursor"
    );
}

#[test]
fn commits_on_several_scopes_share_one_pull_and_the_frame_names_only_what_moved() {
    let (mut subscriptions, _) = Subscriptions::open(negotiation(&[("a", 0), ("b", 0)]));
    assert_eq!(
        subscriptions
            .handle(pulled(&page(&[("a", 0, 0, 0), ("b", 0, 0, 0)])))
            .unwrap(),
        vec![]
    );
    assert_eq!(
        subscriptions.handle(committed("b")).unwrap(),
        vec![pull(&[("b", 0)])]
    );
    assert_eq!(
        subscriptions.handle(committed("a")).unwrap(),
        vec![],
        "b's pull is outstanding; a waits for it"
    );
    let actions = subscriptions
        .handle(pulled(&page(&[("b", 0, 1, 1)])))
        .unwrap();
    assert_eq!(
        actions,
        vec![
            LiveAction::Send {
                frame: page(&[("b", 0, 1, 1)])
            },
            pull(&[("a", 0)])
        ],
        "the frame carries b only; a's pending commit pulls next"
    );
    assert_eq!(
        subscriptions
            .handle(pulled(&page(&[("a", 0, 0, 0)])))
            .unwrap(),
        vec![]
    );
    assert!(!subscriptions.is_pulling());
}

#[test]
fn after_closed_no_event_produces_an_action_and_a_late_page_is_not_sent() {
    let (mut subscriptions, _) = Subscriptions::open(negotiation(&[("a", 0), ("b", 0)]));
    assert_eq!(subscriptions.handle(committed("a")).unwrap(), vec![]);
    assert_eq!(subscriptions.handle(LiveEvent::Closed).unwrap(), vec![]);
    assert!(subscriptions.is_closed());
    assert!(subscriptions.scopes().iter().all(|state| !state.pending));
    assert_eq!(
        subscriptions
            .handle(pulled(&page(&[("a", 0, 5, 5), ("b", 0, 0, 0)])))
            .unwrap(),
        vec![]
    );
    assert!(!subscriptions.is_pulling());
    assert_eq!(subscriptions.handle(committed("a")).unwrap(), vec![]);
    assert_eq!(subscriptions.handle(committed("b")).unwrap(), vec![]);
    assert_eq!(subscriptions.handle(LiveEvent::Closed).unwrap(), vec![]);
}

/// A scan filters removed Channel members before its limit, so a page can
/// advance over positions whose records all left the Channel and carry no
/// change. It is still progress: it is sent, it moves the cursor, and at the
/// head it ends the drain, so removed positions cannot stall a live stream.
#[test]
fn a_page_that_advances_over_removed_positions_without_changes_is_progress() {
    let (mut subscriptions, _) = Subscriptions::open(negotiation(&[("a", 3)]));
    let holes = String::from_utf8(
        PullPage {
            cursors: BTreeMap::from([(
                "a".to_string(),
                CursorRange {
                    from: 3,
                    to: 9,
                    head: 9,
                },
            )]),
            changes: vec![],
        }
        .encode()
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        subscriptions.handle(pulled(&holes)).unwrap(),
        vec![LiveAction::Send { frame: holes }]
    );
    assert!(!subscriptions.is_pulling());
    assert_eq!(subscriptions.scopes()[0].cursor, 9);
}

#[test]
fn invalid_page_progression_is_an_error() {
    let (mut subscriptions, _) = Subscriptions::open(negotiation(&[("a", 3)]));
    let wrong_cursor = subscriptions
        .handle(pulled(&page(&[("a", 4, 5, 5)])))
        .unwrap_err();
    assert_eq!(wrong_cursor.code, axton_server::code::LIVE_INVALID_PAGE);
    let (mut subscriptions, _) = Subscriptions::open(negotiation(&[("a", 3)]));
    let wrong_scope = subscriptions
        .handle(pulled(&page(&[("b", 3, 5, 5)])))
        .unwrap_err();
    assert_eq!(wrong_scope.code, axton_server::code::LIVE_INVALID_PAGE);
    let (mut subscriptions, _) = Subscriptions::open(negotiation(&[("a", 3)]));
    let malformed = subscriptions.handle(pulled("{")).unwrap_err();
    assert_eq!(malformed.code, axton_server::code::LIVE_INVALID_PAGE);
}

#[test]
fn an_unknown_scope_or_an_unrequested_page_is_a_host_defect() {
    let (mut subscriptions, _) = Subscriptions::open(negotiation(&[("a", 0)]));
    let unknown = subscriptions.handle(committed("zzz")).unwrap_err();
    assert_eq!(unknown.code, axton_server::code::LIVE_INVALID_EVENT);
    subscriptions
        .handle(pulled(&page(&[("a", 0, 0, 0)])))
        .unwrap();
    let unrequested = subscriptions
        .handle(pulled(&page(&[("a", 0, 0, 0)])))
        .unwrap_err();
    assert_eq!(unrequested.code, axton_server::code::LIVE_INVALID_EVENT);
}

#[test]
fn events_and_actions_cross_the_boundary_as_tagged_json() {
    let event: LiveEvent = serde_json::from_str(r#"{"type":"pulled","page":"{}"}"#).unwrap();
    assert_eq!(event, pulled("{}"));
    let closed: LiveEvent = serde_json::from_str(r#"{"type":"closed"}"#).unwrap();
    assert_eq!(closed, LiveEvent::Closed);
    assert_eq!(
        serde_json::to_value(pull(&[("a", 7)])).unwrap(),
        json!({"type":"pull","cursors":{"a":7},"models":{"Task":1}})
    );
    assert_eq!(
        serde_json::to_value(LiveAction::Listen { scope: "a".into() }).unwrap(),
        json!({"type":"listen","scope":"a"})
    );
}
