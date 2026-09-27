//! Durable whole-Scope Bootstrap: the registration ledger beside the #150
//! subscription row, and the one transaction that applies a historical page's
//! authority together with its progress
//! ([#151](https://github.com/zanminwang/axton/issues/151)).
mod common;
use axton_client::*;
use axton_sqlite::SqliteStore;
use common::*;
use serde_json::json;
use std::cell::{Cell, RefCell};
use std::rc::Rc;

/// Register `scope` and commit the first delivery boundary an acknowledgement
/// at `head` establishes: S = L = `head`, the origin every historical page of
/// this subscription is bounded by.
fn origin(c: &mut Client<SqliteStore>, scope: &str, head: u64) -> SubscriptionState {
    c.transaction(|tx| tx.set_channel(scope.into(), true))
        .unwrap();
    acknowledge(c, &[(scope, head)]);
    c.subscription_state(scope).unwrap().expect("a row")
}
/// One historical page: `(from, to]` of `scope`, bounded by origin `until`,
/// observed at channel head `head`.
fn historical(
    scope: &str,
    from: u64,
    to: u64,
    until: u64,
    head: u64,
    records: Vec<AuthorityRecord>,
) -> BootstrapPage {
    BootstrapPage {
        channel: scope.to_string(),
        from,
        to,
        until,
        head,
        records,
    }
}
fn bootstrap(c: &mut Client<SqliteStore>, scope: &str) -> BootstrapState {
    let id = c
        .subscription_state(scope)
        .unwrap()
        .expect("a row")
        .subscription_id;
    c.bootstrap_state(scope, id).unwrap()
}
/// Apply `page` as the answer to the request the stored state would issue.
fn apply(c: &mut Client<SqliteStore>, scope: &str, page: &BootstrapPage) -> BootstrapApply {
    let state = bootstrap(c, scope);
    c.apply_bootstrap_page(scope, state.subscription_id, state.run, state.cursor, page)
        .unwrap()
}
fn applied(outcome: &BootstrapApply) -> (&BootstrapState, &ApplyReport) {
    match outcome {
        BootstrapApply::Applied { state, report } => (state, report),
        other => panic!("expected an applied page, got {other:?}"),
    }
}
fn failed(outcome: &BootstrapApply) -> (&BootstrapState, &ApplyReport) {
    match outcome {
        BootstrapApply::Failed { state, report } => (state, report),
        other => panic!("expected a failed run, got {other:?}"),
    }
}
fn scopes(c: &Client<SqliteStore>) -> Vec<String> {
    c.last_bootstrap_scopes().iter().cloned().collect()
}
/// A record whose state this client's schema refuses: a `Skipped` report.
fn refused(id: &str, stamp: u64) -> AuthorityRecord {
    let mut record = authority_of(id, Some("x"), stamp);
    record.state = json!({"text": 22});
    record
}
/// A record the server could not read: a `ReadFailed` report.
fn unreadable(id: &str, stamp: u64) -> AuthorityRecord {
    let mut record = authority_of(id, None, stamp);
    record.error = Some("loader.failed".into());
    record
}
fn entry(id: &str) -> RecordKey {
    schema().record_key("Entry", &json!({ "id": id })).unwrap()
}

/// A request for a Scope that is not subscribed - or whose subscription was
/// replaced - names no bootstrap task at all.
#[test]
fn a_closed_subscription_refuses_registration_and_observation() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    let state = origin(&mut c, "a", 0);
    for error in [
        c.request_bootstrap("b", 1).expect_err("no such Scope"),
        c.bootstrap_state("b", 1).expect_err("no such Scope"),
        c.request_bootstrap("a", state.subscription_id + 1)
            .expect_err("another identity"),
        c.bootstrap_state("a", state.subscription_id + 1)
            .expect_err("another identity"),
    ] {
        // The stable prefix is the SDKs' only handle on this refusal: they
        // raise their own `subscription.closed` for it.
        assert!(
            error.to_string().starts_with(SUBSCRIPTION_CLOSED),
            "{error}"
        );
        assert!(error.to_string().contains("is closed"), "{error}");
    }
}

/// Registration is a local write that needs no connection and no boundary: a
/// Scope registered offline carries a requested task at once, and it becomes
/// schedulable work only when #150 commits its origin.
#[test]
fn registration_before_initialization_waits_for_its_origin() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    let registered = c.ensure_subscription("a").unwrap();
    assert_eq!(registered.starting_cursor, None);
    let state = c
        .request_bootstrap("a", registered.subscription_id)
        .unwrap();
    assert_eq!(state.state, BootstrapPhase::Requested);
    assert_eq!((state.run, state.cursor), (1, 0));
    assert_eq!((state.barrier, state.error.as_ref()), (None, None));
    assert!(
        c.bootstrap_tasks().unwrap().is_empty(),
        "an uninitialized subscription has no interval to scan yet"
    );
    acknowledge(&mut c, &[("a", 7)]);
    assert_eq!(
        c.bootstrap_tasks().unwrap(),
        vec![c.bootstrap_state("a", registered.subscription_id).unwrap()],
        "the committed origin makes it schedulable"
    );
}

/// Registering a load changes no membership, so it must not make the open live
/// session stale: the subscription generation and the channel epochs stay put,
/// while the commit still names the Scope whose load changed and still notifies
/// the row's watchers.
#[test]
fn registering_a_load_does_not_invalidate_the_live_session() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    let id = origin(&mut c, "a", 5).subscription_id;
    // A pull issued under the current subscription: a mark would make its answer
    // stale and force the lane to reconnect.
    c.apply_page(page("a", 5, 6, Some("live"))).unwrap();
    let generation = c.subscription_generation();
    let watcher = c.watch(std::collections::BTreeSet::from([
        "axton_subscription".to_string()
    ]));

    let state = c.request_bootstrap("a", id).unwrap();
    assert_eq!(state.state, BootstrapPhase::Requested);
    assert_eq!(
        c.subscription_generation(),
        generation,
        "a load request is not a membership change"
    );
    assert_eq!(
        c.last_bootstrap_scopes(),
        &std::collections::BTreeSet::from(["a".to_string()]),
        "the commit still names the Scope whose load changed"
    );
    assert!(
        !c.last_changed()
            .iter()
            .any(|t| t.contains("axton_bootstrap")),
        "the mark is a signal, not a table: {:?}",
        c.last_changed()
    );
    assert!(watcher.try_recv().is_ok(), "the row's watchers still fire");
    // The epoch map is untouched, so the page answering the pull issued before
    // the registration still applies.
    let report = c.apply_page(page("a", 6, 7, Some("later"))).unwrap();
    assert!(!report.stale, "the pull in flight was not invalidated");
    assert_eq!(c.cursor("a").unwrap(), Some(7));
    // A call that changes nothing names nothing.
    c.request_bootstrap("a", id).unwrap();
    assert!(c.last_bootstrap_scopes().is_empty());
}

/// Only the first active call registers work: duplicate calls share the run,
/// and a completed task answers locally without starting another one.
#[test]
fn duplicate_calls_share_the_run_and_a_complete_task_is_unchanged() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    let id = origin(&mut c, "a", 0).subscription_id;
    let first = c.request_bootstrap("a", id).unwrap();
    let generation = (c.generation(), c.subscription_generation());
    let second = c.request_bootstrap("a", id).unwrap();
    assert_eq!(first, second);
    assert_eq!(first.run, 1);
    assert_eq!(
        (c.generation(), c.subscription_generation()),
        generation,
        "a call that changes nothing commits nothing"
    );
    // The empty interval of a Scope at origin zero completes on its one
    // terminal page, and a later call resolves against it.
    let page = historical("a", 0, 0, 0, 0, vec![]);
    let outcome = apply(&mut c, "a", &page);
    assert_eq!(applied(&outcome).0.state, BootstrapPhase::Complete);
    let again = c.request_bootstrap("a", id).unwrap();
    assert_eq!(again.state, BootstrapPhase::Complete);
    assert_eq!((again.run, again.cursor, again.barrier), (1, 0, Some(0)));
}

/// An explicit retry after a terminal failure is a new run over the same saved
/// progress: the error and the barrier go, B stays, and pages of the failed run
/// can no longer move it.
#[test]
fn a_retry_after_failure_is_a_new_run_over_the_same_progress() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    let id = origin(&mut c, "a", 10).subscription_id;
    c.request_bootstrap("a", id).unwrap();
    let page = historical("a", 0, 4, 10, 12, vec![authority_of("k", Some("K"), 4)]);
    assert_eq!(applied(&apply(&mut c, "a", &page)).0.cursor, 4);
    let bad = historical("a", 4, 6, 10, 12, vec![refused("bad", 6)]);
    let state = failed(&apply(&mut c, "a", &bad)).0.clone();
    assert_eq!(state.state, BootstrapPhase::Failed);
    assert_eq!((state.run, state.cursor), (1, 4));
    assert!(state.error.is_some());

    let retried = c.request_bootstrap("a", id).unwrap();
    assert_eq!(retried.state, BootstrapPhase::Requested);
    assert_eq!(retried.run, 2, "an explicit retry is a new run");
    assert_eq!(retried.cursor, 4, "over the progress the failed run kept");
    assert_eq!((retried.barrier, retried.error.as_ref()), (None, None));
    // A page of the run that failed cannot move the one that replaced it.
    let stale = c
        .apply_bootstrap_page("a", id, 1, 4, &historical("a", 4, 6, 10, 12, vec![]))
        .unwrap();
    assert!(stale.is_stale(), "{stale:?}");
    assert_eq!(bootstrap(&mut c, "a").cursor, 4);
}

/// Unsubscribing removes that epoch's load state with the row; registering the
/// same Scope name again is a new identity whose coverage starts over. The
/// records the removed run applied are retained (D6).
#[test]
fn unsubscribe_removes_the_load_state_and_recreation_starts_over() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    let first = origin(&mut c, "a", 0).subscription_id;
    c.request_bootstrap("a", first).unwrap();
    apply(
        &mut c,
        "a",
        &historical("a", 0, 0, 0, 0, vec![authority(Some("A"), 3)]),
    );
    assert_eq!(bootstrap(&mut c, "a").state, BootstrapPhase::Complete);

    assert!(c.remove_subscription("a", first).unwrap());
    assert!(c.bootstrap_state("a", first).is_err(), "the state is gone");
    let second = origin(&mut c, "a", 0).subscription_id;
    assert_ne!(second, first);
    let fresh = c.bootstrap_state("a", second).unwrap();
    assert_eq!(fresh.state, BootstrapPhase::NotRequested);
    assert_eq!((fresh.run, fresh.cursor, fresh.barrier), (0, 0, None));
    assert_eq!(
        c.read(&key()).unwrap().unwrap()["text"],
        "A",
        "unsubscribing removes no content"
    );
}

/// Every bootstrap field is durable: a reopened replica answers with the same
/// phase, run, progress, barrier and stored failure.
#[test]
fn every_bootstrap_field_survives_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open(&path);
    let id = origin(&mut c, "a", 10).subscription_id;
    c.request_bootstrap("a", id).unwrap();
    apply(&mut c, "a", &historical("a", 0, 4, 10, 12, vec![]));
    let error = BootstrapError::new("bootstrap.records_failed", "one record", vec![]);
    assert!(c.fail_bootstrap("a", id, 1, error).unwrap());
    let before = c.bootstrap_state("a", id).unwrap();
    drop(c);

    let mut c = open(&path);
    assert_eq!(c.bootstrap_state("a", id).unwrap(), before);
    let reopened = c.bootstrap_state("a", id).unwrap();
    assert_eq!(reopened.state, BootstrapPhase::Failed);
    assert_eq!((reopened.run, reopened.cursor), (1, 4));
    assert_eq!(reopened.error.unwrap().message, "one record");
    assert!(
        c.bootstrap_tasks().unwrap().is_empty(),
        "a failed run waits for an explicit retry"
    );
}

/// The identity, the run, the echoed markers and the committed progress each
/// fence a response: a page that fails one of them writes nothing at all - no
/// authority, no progress, no completion.
#[test]
fn a_stale_page_writes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    let id = origin(&mut c, "a", 10).subscription_id;
    c.request_bootstrap("a", id).unwrap();
    let record = vec![authority_of("k", Some("K"), 4)];
    let page = |from, to, until, head| historical("a", from, to, until, head, record.clone());
    // Each case is (why, identity, run, expected_after, page); the stored task
    // is identity `id`, run 1, B = 0, S = 10.
    let cases: Vec<(&str, u64, u64, u64, BootstrapPage)> = vec![
        ("another identity", id + 1, 1, 0, page(0, 4, 10, 12)),
        ("another run", id, 2, 0, page(0, 4, 10, 12)),
        (
            "an expectation ahead of the committed B",
            id,
            1,
            2,
            page(2, 4, 10, 12),
        ),
        (
            "a from that is not the expected B",
            id,
            1,
            0,
            page(1, 4, 10, 12),
        ),
        (
            "another Scope",
            id,
            1,
            0,
            historical("b", 0, 4, 10, 12, record.clone()),
        ),
        ("another origin", id, 1, 0, page(0, 4, 9, 12)),
    ];
    for (why, identity, run, expected_after, page) in cases {
        let outcome = c
            .apply_bootstrap_page("a", identity, run, expected_after, &page)
            .unwrap();
        assert!(outcome.is_stale(), "{why}: {outcome:?}");
        assert_eq!(bootstrap(&mut c, "a").cursor, 0, "{why}");
        assert_eq!(
            bootstrap(&mut c, "a").state,
            BootstrapPhase::Requested,
            "{why}"
        );
        assert!(c.read(&entry("k")).unwrap().is_none(), "{why}");
    }
    // A duplicate of a page already applied cannot move progress backwards.
    assert_eq!(
        applied(&apply(&mut c, "a", &page(0, 4, 10, 12))).0.cursor,
        4
    );
    let duplicate = c
        .apply_bootstrap_page("a", id, 1, 0, &page(0, 4, 10, 12))
        .unwrap();
    assert!(duplicate.is_stale(), "{duplicate:?}");
    assert_eq!(bootstrap(&mut c, "a").cursor, 4);
    // A completed task is not a task a page can move either.
    apply(&mut c, "a", &historical("a", 4, 10, 10, 10, vec![]));
    assert_eq!(bootstrap(&mut c, "a").state, BootstrapPhase::Complete);
    let after = c
        .apply_bootstrap_page(
            "a",
            id,
            1,
            10,
            &historical("a", 10, 10, 10, 10, record.clone()),
        )
        .unwrap();
    assert!(after.is_stale(), "{after:?}");
    assert_eq!(
        c.read(&entry("k")).unwrap().unwrap()["text"],
        "K",
        "from the page that did apply"
    );
}

/// A protocol-invalid envelope commits no authority and no progress, and fails
/// visibly: the interval is not advanced and the phase is untouched, so the
/// caller can mark the run failed itself.
#[test]
fn a_protocol_invalid_page_writes_nothing_and_fails_visibly() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    let id = origin(&mut c, "a", 10).subscription_id;
    c.request_bootstrap("a", id).unwrap();
    let record = vec![authority_of("k", Some("K"), 4)];
    for page in [
        // An origin past the channel head.
        historical("a", 0, 4, 10, 3, record.clone()),
        // A page that reaches past its origin.
        historical("a", 0, 12, 10, 12, record.clone()),
        // A nonterminal page that made no progress.
        historical("a", 0, 0, 10, 12, record.clone()),
    ] {
        let error = c
            .apply_bootstrap_page("a", id, 1, 0, &page)
            .expect_err("a protocol-invalid page");
        assert!(!error.to_string().is_empty(), "{error}");
        let state = bootstrap(&mut c, "a");
        assert_eq!((state.state, state.cursor), (BootstrapPhase::Requested, 0));
        assert!(c.read(&entry("k")).unwrap().is_none());
    }
    // The caller marks the run failed itself, run-fenced.
    let error = BootstrapError::new("protocol.invalid", "malformed page", vec![]);
    assert!(!c.fail_bootstrap("a", id, 2, error.clone()).unwrap());
    assert!(c.fail_bootstrap("a", id, 1, error).unwrap());
    let state = bootstrap(&mut c, "a");
    assert_eq!(state.state, BootstrapPhase::Failed);
    assert_eq!(state.error.unwrap().code, "protocol.invalid");
}

/// An attributable record failure keeps the page's successful authority, marks
/// the run failed with a bounded summary, and leaves the continuation marker
/// where it was, so an explicit retry revisits that page.
#[test]
fn a_record_failure_keeps_the_successful_authority_and_holds_the_interval() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    let id = origin(&mut c, "a", 10).subscription_id;
    c.request_bootstrap("a", id).unwrap();
    let page = historical(
        "a",
        0,
        6,
        10,
        12,
        vec![
            authority_of("good", Some("G"), 2),
            refused("bad", 3),
            unreadable("unread", 4),
            authority_of("also", Some("B"), 5),
        ],
    );
    let outcome = apply(&mut c, "a", &page);
    let (state, report) = failed(&outcome);
    assert_eq!(
        report.applied, 2,
        "the report of what did land is the caller's"
    );
    assert_eq!(state.state, BootstrapPhase::Failed);
    assert_eq!(state.cursor, 0, "the interval does not advance");
    assert_eq!(state.barrier, None);
    let error = state.error.clone().expect("a stored failure");
    assert_eq!(error.code, "bootstrap.records_failed");
    assert!(error.message.contains('a'), "{}", error.message);
    let mut summaries: Vec<(String, String)> = error
        .records
        .iter()
        .map(|r| {
            (
                r.identity["id"].as_str().unwrap().to_string(),
                r.code.clone(),
            )
        })
        .collect();
    summaries.sort();
    assert_eq!(
        summaries,
        vec![
            ("bad".to_string(), "skipped".to_string()),
            ("unread".to_string(), "loader.failed".to_string())
        ]
    );
    // The records that did apply are committed content.
    assert_eq!(c.read(&entry("good")).unwrap().unwrap()["text"], "G");
    assert_eq!(c.read(&entry("also")).unwrap().unwrap()["text"], "B");
    assert!(c.read(&entry("bad")).unwrap().is_none());
    assert_eq!(c.record_stamp(&entry("good")).unwrap(), 2);
    // An explicit retry revisits the same page; the applied records are
    // idempotent by stamp.
    let retried = c.request_bootstrap("a", id).unwrap();
    assert_eq!((retried.run, retried.cursor), (2, 0));
    let good = historical(
        "a",
        0,
        6,
        10,
        12,
        vec![
            authority_of("good", Some("G"), 2),
            authority_of("bad", Some("F"), 3),
            authority_of("unread", Some("U"), 4),
            authority_of("also", Some("B"), 5),
        ],
    );
    let outcome = apply(&mut c, "a", &good);
    let (state, report) = applied(&outcome);
    assert_eq!(state.cursor, 6);
    assert_eq!(state.state, BootstrapPhase::Loading);
    assert_eq!(
        scopes(&c),
        vec!["a".to_string()],
        "the page named its Scope"
    );
    assert_eq!(report.applied, 2, "the two already applied are idempotent");
    assert_eq!(c.read(&entry("bad")).unwrap().unwrap()["text"], "F");
}

/// The stored failure is bounded: at most fifty record summaries and a message
/// cut to 1,024 UTF-8 bytes on a character boundary.
#[test]
fn a_stored_failure_is_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    let id = origin(&mut c, "a", 10).subscription_id;
    c.request_bootstrap("a", id).unwrap();
    // A multi-byte character straddles byte 1,024: the cut falls before it.
    let message = format!("{}\u{e9}{}", "a".repeat(1023), "b".repeat(100));
    let records: Vec<BootstrapRecordFailure> = (0..60)
        .map(|i| BootstrapRecordFailure {
            model: "Entry".into(),
            identity: json!({ "id": format!("e{i}") }),
            stamp: i,
            code: "loader.failed".into(),
        })
        .collect();
    let error = BootstrapError::new("bootstrap.records_failed", message, records);
    assert_eq!(error.message.len(), 1023, "cut on a character boundary");
    assert_eq!(error.records.len(), 50);
    assert!(c.fail_bootstrap("a", id, 1, error.clone()).unwrap());
    let stored = bootstrap(&mut c, "a").error.expect("a stored failure");
    assert_eq!(stored, error);

    // The fields are public, so the bound is enforced where the row is written
    // too, not only by the constructor.
    let retried = c.request_bootstrap("a", id).unwrap();
    let unbounded = BootstrapError {
        code: "protocol.invalid".into(),
        message: "z".repeat(4096),
        records: error
            .records
            .iter()
            .cloned()
            .chain(error.records.clone())
            .collect(),
    };
    assert!(c.fail_bootstrap("a", id, retried.run, unbounded).unwrap());
    let stored = bootstrap(&mut c, "a").error.expect("a stored failure");
    assert_eq!(stored.message.len(), 1024);
    assert_eq!(stored.records.len(), 50);
}

/// A pending Action whose replay no longer fits the new authority is reported
/// for observation and does not fail the run: the authority it applied is
/// coverage, and the queued work stays queued.
#[test]
fn a_diverged_replay_does_not_fail_the_run() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    let id = origin(&mut c, "a", 10).subscription_id;
    seed(&mut c, "local");
    let ordinal = c.transaction(|tx| tx.enqueue(mutation("edited"))).unwrap();
    c.request_bootstrap("a", id).unwrap();
    // The historical record is a deletion: the queued update cannot replay.
    let outcome = apply(
        &mut c,
        "a",
        &historical("a", 0, 10, 10, 10, vec![authority(None, 5)]),
    );
    let (state, report) = applied(&outcome);
    assert_eq!(state.state, BootstrapPhase::Complete);
    assert_eq!(state.cursor, 10);
    assert_eq!(report.diverged(), 1);
    assert_eq!(report.reports[0].ordinal, Some(ordinal));
    assert_eq!(state.error, None, "a divergence is not a coverage failure");
    assert_eq!(c.pending_count().unwrap(), 1, "the edit is still sent");
}

/// A page that fails on one record still reports the `Diverged` replay of
/// another: the failure is coverage, the divergence is observation, and the
/// caller needs both.
#[test]
fn a_failed_page_still_reports_its_diverged_replay() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    let id = origin(&mut c, "a", 10).subscription_id;
    seed(&mut c, "local");
    let ordinal = c.transaction(|tx| tx.enqueue(mutation("edited"))).unwrap();
    c.request_bootstrap("a", id).unwrap();
    // The deletion diverges the queued update; the other record is refused.
    let outcome = apply(
        &mut c,
        "a",
        &historical(
            "a",
            0,
            6,
            10,
            12,
            vec![authority(None, 5), refused("bad", 6)],
        ),
    );
    let (state, report) = failed(&outcome);
    assert_eq!(state.state, BootstrapPhase::Failed);
    assert_eq!(state.cursor, 0, "the interval is held");
    assert_eq!(report.applied, 1, "the deletion did apply");
    assert_eq!(report.diverged(), 1, "and the replay failure is observable");
    assert_eq!(
        report
            .reports
            .iter()
            .find(|r| r.kind == ReportKind::Diverged)
            .and_then(|r| r.ordinal),
        Some(ordinal)
    );
    // Only the record that failed coverage is in the stored bounded error.
    let error = state.error.clone().expect("a stored failure");
    assert_eq!(error.code, "bootstrap.records_failed");
    assert_eq!(error.records.len(), 1, "a divergence is not summarised");
    assert_eq!(error.records[0].identity, json!({"id":"bad"}));
    assert_eq!(error.records[0].code, "skipped");
    assert_eq!(c.pending_count().unwrap(), 1, "the edit is still sent");
    assert_eq!(scopes(&c), vec!["a".to_string()]);
}

/// Newer authority already applied is not replaced by the historical interval,
/// and a deletion is not resurrected by it; a record the interval alone carries
/// lands.
#[test]
fn old_historical_authority_cannot_regress_newer_live_content() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    let id = origin(&mut c, "a", 10).subscription_id;
    // Live delivery moves past the origin: an update, then a deletion.
    c.apply_page(page("a", 10, 14, Some("live"))).unwrap();
    c.apply_page(page("a", 14, 20, None)).unwrap();
    assert!(c.read(&key()).unwrap().is_none());
    c.request_bootstrap("a", id).unwrap();
    let outcome = apply(
        &mut c,
        "a",
        &historical(
            "a",
            0,
            10,
            10,
            20,
            vec![
                authority(Some("historical"), 8),
                authority_of("only", Some("O"), 3),
            ],
        ),
    );
    let (state, report) = applied(&outcome);
    assert_eq!(report.applied, 1, "only the record the interval alone has");
    assert!(report.reports.is_empty());
    assert!(
        c.read(&key()).unwrap().is_none(),
        "an older historical state cannot resurrect a deletion"
    );
    assert_eq!(c.record_stamp(&key()).unwrap(), 20);
    assert_eq!(c.read(&entry("only")).unwrap().unwrap()["text"], "O");
    assert_eq!(state.state, BootstrapPhase::Complete);
}

/// Historical authority lands beneath pending optimism and the queue replays
/// over it: the visible row keeps the local edit, the stamp is the server's.
#[test]
fn historical_authority_lands_beneath_pending_optimism() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    let id = origin(&mut c, "a", 10).subscription_id;
    seed(&mut c, "A");
    c.transaction(|tx| tx.enqueue(mutation("mine"))).unwrap();
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "mine");
    c.request_bootstrap("a", id).unwrap();
    let outcome = apply(
        &mut c,
        "a",
        &historical("a", 0, 10, 10, 10, vec![authority(Some("server"), 6)]),
    );
    let (_, report) = applied(&outcome);
    assert_eq!(report.applied, 1);
    assert_eq!(report.diverged(), 0);
    assert_eq!(
        c.read(&key()).unwrap().unwrap()["text"],
        "mine",
        "the optimistic edit still shows"
    );
    assert_eq!(c.record_stamp(&key()).unwrap(), 6);
    assert_eq!(c.pending_count().unwrap(), 1);
}

/// The terminal page fixes the barrier H and the run waits in `catching_up`
/// until ordinary delivery reaches it. H is not refreshed while waiting.
#[test]
fn the_terminal_page_fixes_a_barrier_that_ordinary_delivery_settles() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    let id = origin(&mut c, "a", 5).subscription_id;
    c.request_bootstrap("a", id).unwrap();
    let first = apply(
        &mut c,
        "a",
        &historical("a", 0, 3, 5, 9, vec![authority_of("k", Some("K"), 2)]),
    );
    let (state, _) = applied(&first);
    assert_eq!(state.state, BootstrapPhase::Loading);
    assert_eq!((state.cursor, state.barrier), (3, None));

    let terminal = apply(&mut c, "a", &historical("a", 3, 5, 5, 9, vec![]));
    let (state, _) = applied(&terminal);
    assert_eq!(state.state, BootstrapPhase::CatchingUp);
    assert_eq!((state.cursor, state.barrier), (5, Some(9)));
    // Waiting out a barrier must not commit an empty transaction once per
    // delivered page: nothing is written, so no generation moves and no watcher
    // is told anything happened.
    let generations = (c.generation(), c.subscription_generation());
    let watcher = c.watch(std::collections::BTreeSet::from([
        "axton_subscription".to_string(),
        "axton_client".to_string(),
    ]));
    for _ in 0..3 {
        assert!(
            c.settle_bootstrap_barriers(&["a".to_string()])
                .unwrap()
                .is_empty(),
            "delivery is behind the barrier"
        );
        assert_eq!(
            (c.generation(), c.subscription_generation()),
            generations,
            "a settlement with nothing to settle commits nothing"
        );
        assert!(
            watcher.try_recv().is_err(),
            "and so notifies no watcher: there was no commit"
        );
    }
    assert_eq!(c.cursor("a").unwrap(), Some(5));
    // A terminal response lost before its commit and retried arrives after the
    // barrier was already stored: the run is catching up, not loading, so the
    // page answers nothing and writes nothing.
    let late = c
        .apply_bootstrap_page("a", id, 1, 5, &historical("a", 5, 5, 5, 11, vec![]))
        .unwrap();
    assert!(late.is_stale(), "{late:?}");
    let state = bootstrap(&mut c, "a");
    assert_eq!(state.state, BootstrapPhase::CatchingUp);
    assert_eq!(state.barrier, Some(9), "H is fixed, never refreshed");

    // Ordinary delivery reaches the barrier; the transaction that observes both
    // conditions marks the run complete.
    c.apply_page(page("a", 5, 9, Some("live"))).unwrap();
    let settled = c.settle_bootstrap_barriers(&["a".to_string()]).unwrap();
    assert_eq!(settled.len(), 1);
    assert_eq!(settled[0].state, BootstrapPhase::Complete);
    assert_eq!(settled[0].barrier, Some(9), "the barrier is retained");
    assert_eq!(
        scopes(&c),
        vec!["a".to_string()],
        "the settlement named the Scope it completed"
    );
    assert!(
        c.settle_bootstrap_barriers(&["a".to_string()])
            .unwrap()
            .is_empty(),
        "settling is idempotent"
    );
    assert_eq!(
        c.bootstrap_state("a", id).unwrap().state,
        BootstrapPhase::Complete
    );
    assert_eq!(
        c.subscription_state("a").unwrap().unwrap(),
        SubscriptionState {
            scope: "a".into(),
            subscription_id: id,
            starting_cursor: Some(5),
            cursor: Some(9),
        },
        "bootstrap never writes the origin or the delivery cursor"
    );
}

/// When delivery is already at or past the head the terminal page observed, the
/// same transaction completes the run.
#[test]
fn a_terminal_page_completes_at_once_when_delivery_is_past_its_barrier() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    let id = origin(&mut c, "a", 5).subscription_id;
    c.request_bootstrap("a", id).unwrap();
    let outcome = apply(
        &mut c,
        "a",
        &historical("a", 0, 5, 5, 5, vec![authority_of("k", Some("K"), 2)]),
    );
    let (state, _) = applied(&outcome);
    assert_eq!(state.state, BootstrapPhase::Complete);
    assert_eq!((state.cursor, state.barrier), (5, Some(5)));
    assert_eq!(c.read(&entry("k")).unwrap().unwrap()["text"], "K");
    assert!(c.bootstrap_tasks().unwrap().is_empty());
    assert_eq!(c.bootstrap_state("a", id).unwrap(), *state);
}

/// A Scope with nothing published completes normally: an empty terminal page at
/// head zero needs no delivery to catch up with.
#[test]
fn an_empty_zero_head_scope_completes_on_its_one_page() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    let id = origin(&mut c, "a", 0).subscription_id;
    c.request_bootstrap("a", id).unwrap();
    let outcome = apply(&mut c, "a", &historical("a", 0, 0, 0, 0, vec![]));
    let (state, report) = applied(&outcome);
    assert_eq!(state.state, BootstrapPhase::Complete);
    assert_eq!((state.cursor, state.barrier), (0, Some(0)));
    assert_eq!(report.applied, 0);
}

/// Tasks are the initialized runs that still have work, in Scope order: the
/// round-robin the scheduler rotates.
#[test]
fn tasks_are_the_initialized_runs_with_work_in_scope_order() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    for (scope, head) in [("a", 5), ("b", 5), ("c", 5), ("d", 5)] {
        let id = origin(&mut c, scope, head).subscription_id;
        if scope != "d" {
            c.request_bootstrap(scope, id).unwrap();
        }
    }
    // b reaches its barrier and waits; c fails.
    apply(&mut c, "b", &historical("b", 0, 5, 5, 9, vec![]));
    let id = bootstrap(&mut c, "c").subscription_id;
    let error = BootstrapError::new("protocol.invalid", "bad", vec![]);
    c.fail_bootstrap("c", id, 1, error).unwrap();
    assert_eq!(
        c.bootstrap_tasks()
            .unwrap()
            .into_iter()
            .map(|t| (t.scope, t.state))
            .collect::<Vec<_>>(),
        vec![
            ("a".to_string(), BootstrapPhase::Requested),
            ("b".to_string(), BootstrapPhase::CatchingUp),
        ]
    );
}

/// A second handle on the client's SQLite file: the tests below corrupt a row
/// with SQL, as a damaged or foreign writer would, and read it back raw.
fn raw_store(path: &std::path::Path) -> SqliteStore {
    SqliteStore::open(path).unwrap()
}
/// Every stored column of `channel`'s row, with each one's SQLite type: what
/// "retained exactly as stored" is checked against.
fn raw_row(store: &mut SqliteStore, channel: &str) -> Vec<serde_json::Value> {
    let rows = store
        .query_committed(
            "SELECT channel, subscription_id, starting_cursor, cursor, bootstrap_state, \
             bootstrap_run, bootstrap_cursor, typeof(bootstrap_cursor), bootstrap_barrier, \
             bootstrap_error, typeof(bootstrap_error) FROM axton_subscription WHERE channel=?",
            &[json!(channel)],
        )
        .unwrap();
    rows.rows
        .into_iter()
        .next()
        .expect("the row is still there")
}

/// One active row that cannot be decoded is its own registration's problem: a
/// named read of it still fails, and so does the strict task list, but the
/// scheduler's scan skips it - wherever it falls in Channel order - and the
/// healthy run beside it keeps its turn. Nothing rewrites the damaged row.
#[test]
fn an_undecodable_active_row_does_not_block_the_schedule() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open(&path);
    let mut ids = std::collections::BTreeMap::new();
    for channel in ["bad", "good", "worse"] {
        let id = origin(&mut c, channel, 5).subscription_id;
        c.request_bootstrap(channel, id).unwrap();
        ids.insert(channel, id);
    }
    let mut raw = raw_store(&path);
    raw.execute(
        "UPDATE axton_subscription SET bootstrap_cursor='x' WHERE channel='bad'",
        &[],
    )
    .unwrap();
    raw.execute(
        "UPDATE axton_subscription SET bootstrap_error='{not json' WHERE channel='worse'",
        &[],
    )
    .unwrap();
    let stored = [raw_row(&mut raw, "bad"), raw_row(&mut raw, "worse")];
    assert_eq!(stored[0][7], json!("text"), "{:?}", stored[0]);
    assert_eq!(stored[1][10], json!("text"), "{:?}", stored[1]);

    // Named reads of a damaged registration never invent a state.
    for channel in ["bad", "worse"] {
        c.bootstrap_state(channel, ids[channel])
            .expect_err("a named read of an undecodable row fails");
        c.request_bootstrap(channel, ids[channel])
            .expect_err("so does registering against it");
    }
    c.bootstrap_tasks()
        .expect_err("the strict task list keeps the decode error visible");

    // The schedule rotates among the healthy runs only.
    for rotation in [None, Some("bad"), Some("good"), Some("worse")] {
        let task = c
            .bootstrap_schedule(rotation)
            .unwrap()
            .expect("the healthy run is schedulable");
        assert_eq!(task.state.scope, "good", "after {rotation:?}");
        assert_eq!(task.origin, 5);
    }
    assert!(c.bootstrap_barriers().unwrap().is_empty());
    let good = bootstrap(&mut c, "good");
    assert_eq!((good.state, good.cursor), (BootstrapPhase::Requested, 0));

    assert_eq!(
        [raw_row(&mut raw, "bad"), raw_row(&mut raw, "worse")],
        stored,
        "the damaged rows are neither reset nor marked"
    );
}

/// On reopen, the barrier scan finds a healthy reached barrier even beside an
/// undecodable active row - one that is itself catching up past its barrier,
/// and so never supplies completion evidence - and the schedule still picks
/// the healthy requested run.
#[test]
fn an_undecodable_active_row_does_not_hide_a_reached_barrier() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open(&path);
    let mut ids = std::collections::BTreeMap::new();
    for channel in ["bad", "good", "waiting"] {
        let id = origin(&mut c, channel, 5).subscription_id;
        c.request_bootstrap(channel, id).unwrap();
        ids.insert(channel, id);
    }
    for channel in ["bad", "waiting"] {
        apply(&mut c, channel, &historical(channel, 0, 5, 5, 9, vec![]));
        c.apply_page(page(channel, 5, 9, Some("live"))).unwrap();
        assert_eq!(bootstrap(&mut c, channel).state, BootstrapPhase::CatchingUp);
    }
    let mut raw = raw_store(&path);
    raw.execute(
        "UPDATE axton_subscription SET bootstrap_error='{not json' WHERE channel='bad'",
        &[],
    )
    .unwrap();
    let stored = raw_row(&mut raw, "bad");
    drop(c);

    let mut c = open(&path);
    c.bootstrap_state("bad", ids["bad"])
        .expect_err("a named read of an undecodable row fails");
    c.bootstrap_tasks()
        .expect_err("the strict task list keeps the decode error visible");
    let waiting = c.bootstrap_barriers().unwrap();
    assert_eq!(waiting, vec!["waiting".to_string()]);
    let task = c.bootstrap_schedule(None).unwrap().expect("a healthy run");
    assert_eq!(task.state.scope, "good");

    let settled = c.settle_bootstrap_barriers(&waiting).unwrap();
    assert_eq!(settled.len(), 1);
    assert_eq!(settled[0].scope, "waiting");
    assert_eq!(settled[0].state, BootstrapPhase::Complete);
    assert_eq!(
        c.bootstrap_state("waiting", ids["waiting"]).unwrap().state,
        BootstrapPhase::Complete
    );
    assert_eq!(
        raw_row(&mut raw, "bad"),
        stored,
        "the damaged row is neither reset, completed nor marked"
    );
}

/// Take `channel` through its whole interval to a barrier ordinary delivery has
/// reached: catching up at H = 9 with L = 9, waiting only for a settlement.
fn reached(c: &mut Client<SqliteStore>, channel: &str) -> u64 {
    let id = origin(c, channel, 5).subscription_id;
    c.request_bootstrap(channel, id).unwrap();
    apply(c, channel, &historical(channel, 0, 5, 5, 9, vec![]));
    c.apply_page(page(channel, 5, 9, Some("live"))).unwrap();
    assert_eq!(bootstrap(c, channel).state, BootstrapPhase::CatchingUp);
    id
}
/// Take `channel` through its whole interval to a barrier at H = 9 that
/// delivery, still at L = 5, has not reached.
fn behind(c: &mut Client<SqliteStore>, channel: &str) -> u64 {
    let id = origin(c, channel, 5).subscription_id;
    c.request_bootstrap(channel, id).unwrap();
    apply(c, channel, &historical(channel, 0, 5, 5, 9, vec![]));
    assert_eq!(bootstrap(c, channel).state, BootstrapPhase::CatchingUp);
    id
}
/// The names a settlement is handed below: the reached runs `a` and `z`, the
/// run `m` still short of its barrier, 1,001 names no row carries, and
/// duplicates of all of them. `a` sorts before every unknown name and `z`
/// after them, so the two reached runs fall in different 900-name chunks.
fn crowd() -> (Vec<String>, usize) {
    let unknown = (0..1001).map(|i| format!("n{i:04}"));
    let mut names: Vec<String> = ["z", "m", "a"].map(String::from).to_vec();
    names.extend(unknown.clone());
    names.extend(["a", "z", "m", "z"].map(String::from));
    names.extend(unknown.step_by(7));
    let distinct = names
        .iter()
        .collect::<std::collections::BTreeSet<_>>()
        .len();
    assert_eq!(distinct, 1004);
    (names, distinct)
}

/// Settlement takes its names as a set: duplicates are one name, more names
/// than SQLite's oldest 999-variable floor are fine, each reached run
/// completes exactly once and in Channel order, a run short of its barrier
/// stays catching up (D10), and a second call finds nothing to write.
#[test]
fn settlement_takes_its_names_as_a_set_of_any_size() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    let ids = [
        reached(&mut c, "a"),
        behind(&mut c, "m"),
        reached(&mut c, "z"),
    ];

    let generation = c.generation();
    assert!(c.settle_bootstrap_barriers(&[]).unwrap().is_empty());
    assert_eq!(c.generation(), generation, "an empty set writes nothing");

    let (names, _) = crowd();
    let settled = c.settle_bootstrap_barriers(&names).unwrap();
    assert_eq!(
        settled
            .iter()
            .map(|s| (s.scope.as_str(), s.state, s.barrier))
            .collect::<Vec<_>>(),
        vec![
            ("a", BootstrapPhase::Complete, Some(9)),
            ("z", BootstrapPhase::Complete, Some(9)),
        ]
    );
    for (channel, id, phase) in [
        ("a", ids[0], BootstrapPhase::Complete),
        ("m", ids[1], BootstrapPhase::CatchingUp),
        ("z", ids[2], BootstrapPhase::Complete),
    ] {
        assert_eq!(c.bootstrap_state(channel, id).unwrap().state, phase);
    }

    let generation = c.generation();
    assert!(
        c.settle_bootstrap_barriers(&names).unwrap().is_empty(),
        "settling is idempotent"
    );
    assert_eq!(
        c.generation(),
        generation,
        "and the second call writes nothing"
    );
}

/// The same crowd through a store that records every read: each barrier
/// candidate query binds at most 900 names and nothing else, a duplicate takes
/// no slot, and more than 900 distinct names take more than one query. This is
/// the proof that does not depend on the host SQLite's variable limit, which
/// modern builds raise far above 999. An empty set runs no SQL at all.
#[test]
fn a_settlement_binds_each_distinct_name_once_in_chunks_of_900() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open(&path);
    reached(&mut c, "a");
    behind(&mut c, "m");
    reached(&mut c, "z");
    drop(c);
    let reads = Reads::default();
    let mut c = Client::open(
        Recording {
            inner: SqliteStore::open(&path).unwrap(),
            reads: reads.clone(),
        },
        schema(),
    )
    .unwrap();

    reads.take();
    assert!(c.settle_bootstrap_barriers(&[]).unwrap().is_empty());
    assert!(reads.take().is_empty(), "an empty set runs no SQL");

    let (names, distinct) = crowd();
    let settled = c.settle_bootstrap_barriers(&names).unwrap();
    assert_eq!(
        settled.iter().map(|s| s.scope.as_str()).collect::<Vec<_>>(),
        vec!["a", "z"]
    );
    let candidates: Vec<usize> = reads
        .take()
        .into_iter()
        .filter(|(sql, _)| sql.contains("cursor >= bootstrap_barrier"))
        .map(|(_, bound)| bound)
        .collect();
    assert!(candidates.len() >= 2, "{candidates:?}");
    assert!(
        candidates.iter().all(|&bound| bound <= 900),
        "{candidates:?}"
    );
    assert_eq!(
        candidates.iter().sum::<usize>(),
        distinct,
        "each distinct name is bound exactly once: {candidates:?}"
    );
}

/// A barrier candidate whose row cannot be decoded is reported and skipped
/// before the write opens, so the healthy candidate beside it still completes.
/// The damaged row supplies no completion evidence: it is neither completed,
/// reset nor marked, its delivery cursor stays where it was, and a named read
/// of it still fails.
#[test]
fn an_undecodable_candidate_does_not_block_a_healthy_settlement() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open(&path);
    let bad = reached(&mut c, "bad");
    let good = reached(&mut c, "good");
    let mut raw = raw_store(&path);
    raw.execute(
        "UPDATE axton_subscription SET bootstrap_error='{not json' WHERE channel='bad'",
        &[],
    )
    .unwrap();
    let stored = raw_row(&mut raw, "bad");
    assert_eq!(
        (&stored[3], &stored[4], &stored[8]),
        (&json!(9), &json!("catching_up"), &json!(9)),
        "still a candidate by the settlement predicate: {stored:?}"
    );

    let names = ["bad", "good"].map(String::from);
    let settled = c.settle_bootstrap_barriers(&names).unwrap();
    assert_eq!(
        settled
            .iter()
            .map(|s| (s.scope.as_str(), s.state))
            .collect::<Vec<_>>(),
        vec![("good", BootstrapPhase::Complete)]
    );
    assert_eq!(
        c.bootstrap_state("good", good).unwrap().state,
        BootstrapPhase::Complete
    );
    let generation = c.generation();
    assert!(c.settle_bootstrap_barriers(&names).unwrap().is_empty());
    assert_eq!(c.generation(), generation, "the damaged row opens no write");

    c.bootstrap_state("bad", bad)
        .expect_err("a named read of an undecodable row fails");
    assert_eq!(
        raw_row(&mut raw, "bad"),
        stored,
        "the damaged row and its cursor are neither completed, reset nor marked"
    );
}

/// The state the binding carries is serializable, with the phase as the stored
/// text and an uncommitted barrier as null.
#[test]
fn bootstrap_state_is_serializable() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    let id = origin(&mut c, "a", 5).subscription_id;
    let state = c.request_bootstrap("a", id).unwrap();
    assert_eq!(
        serde_json::to_value(&state).unwrap(),
        json!({"scope":"a","subscriptionId":id,"state":"requested","run":1,"cursor":0,"barrier":null,"error":null})
    );
    let error = BootstrapError::new(
        "bootstrap.records_failed",
        "one record",
        vec![BootstrapRecordFailure {
            model: "Entry".into(),
            identity: json!({"id":"e"}),
            stamp: 3,
            code: "loader.failed".into(),
        }],
    );
    c.fail_bootstrap("a", id, 1, error).unwrap();
    let state = c.bootstrap_state("a", id).unwrap();
    assert_eq!(
        serde_json::to_value(&state).unwrap(),
        json!({"scope":"a","subscriptionId":id,"state":"failed","run":1,"cursor":0,"barrier":null,
               "error":{"code":"bootstrap.records_failed","message":"one record",
                        "records":[{"model":"Entry","identity":{"id":"e"},"stamp":3,"code":"loader.failed"}]}})
    );
    for phase in [
        BootstrapPhase::NotRequested,
        BootstrapPhase::Requested,
        BootstrapPhase::Loading,
        BootstrapPhase::CatchingUp,
        BootstrapPhase::Complete,
        BootstrapPhase::Failed,
    ] {
        assert_eq!(serde_json::to_value(phase).unwrap(), json!(phase.as_str()));
    }
}

/// The write the page lands in is one transaction: a commit that fails leaves
/// neither the authority nor the progress, and the same page applies afterwards.
#[test]
fn a_failed_commit_leaves_neither_authority_nor_progress() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let trip = Trip::default();
    let mut c = Client::open(
        FailingCommit {
            inner: SqliteStore::open(&path).unwrap(),
            trip: trip.clone(),
        },
        schema(),
    )
    .unwrap();
    let id = origin_generic(&mut c, "a", 10);
    c.request_bootstrap("a", id).unwrap();
    let page = historical("a", 0, 4, 10, 12, vec![authority_of("k", Some("K"), 4)]);
    trip.arm();
    let error = c
        .apply_bootstrap_page("a", id, 1, 0, &page)
        .expect_err("the injected commit failure");
    assert!(error.to_string().contains("injected"), "{error}");
    drop(c);

    let mut c = open(&path);
    assert!(
        c.read(&entry("k")).unwrap().is_none(),
        "no authority from the rolled-back page"
    );
    let state = c.bootstrap_state("a", id).unwrap();
    assert_eq!((state.state, state.cursor), (BootstrapPhase::Requested, 0));
    // The page is still resumable: applying it again commits both halves.
    let outcome = apply(&mut c, "a", &page);
    assert_eq!(applied(&outcome).0.cursor, 4);
    assert_eq!(c.read(&entry("k")).unwrap().unwrap()["text"], "K");
}

/// A reopen immediately after the commit finds the authority and the progress
/// together: they are one unit.
#[test]
fn a_reopen_after_the_commit_finds_authority_and_progress_together() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open(&path);
    let id = origin(&mut c, "a", 10).subscription_id;
    c.request_bootstrap("a", id).unwrap();
    apply(
        &mut c,
        "a",
        &historical("a", 0, 4, 10, 12, vec![authority_of("k", Some("K"), 4)]),
    );
    drop(c);
    let mut c = open(&path);
    assert_eq!(c.read(&entry("k")).unwrap().unwrap()["text"], "K");
    let state = c.bootstrap_state("a", id).unwrap();
    assert_eq!((state.state, state.cursor), (BootstrapPhase::Loading, 4));
}

/// A flag the test arms to fail the next commit.
#[derive(Clone, Default)]
struct Trip(Rc<Cell<bool>>);
impl Trip {
    fn arm(&self) {
        self.0.set(true);
    }
    fn take(&self) -> bool {
        self.0.replace(false)
    }
}
/// A store whose `commit` fails once when armed: the injection point
/// immediately before a page's authority and progress would land.
struct FailingCommit {
    inner: SqliteStore,
    trip: Trip,
}
impl ClientStore for FailingCommit {
    fn begin(&mut self) -> Result<()> {
        self.inner.begin()
    }
    fn commit(&mut self) -> Result<()> {
        if self.trip.take() {
            return Err(invalid("injected commit failure"));
        }
        self.inner.commit()
    }
    fn rollback(&mut self) -> Result<()> {
        self.inner.rollback()
    }
    fn savepoint(&mut self, name: &str) -> Result<()> {
        self.inner.savepoint(name)
    }
    fn release(&mut self, name: &str) -> Result<()> {
        self.inner.release(name)
    }
    fn rollback_to(&mut self, name: &str) -> Result<()> {
        self.inner.rollback_to(name)
    }
    fn execute(&mut self, sql: &str, parameters: &[serde_json::Value]) -> Result<usize> {
        self.inner.execute(sql, parameters)
    }
    fn execute_batch(&mut self, sql: &str) -> Result<()> {
        self.inner.execute_batch(sql)
    }
    fn query(&mut self, sql: &str, parameters: &[serde_json::Value]) -> Result<SqlRows> {
        self.inner.query(sql, parameters)
    }
    fn query_committed(&mut self, sql: &str, parameters: &[serde_json::Value]) -> Result<SqlRows> {
        self.inner.query_committed(sql, parameters)
    }
}
/// `origin` for any store: the fixtures are typed to the SQLite one.
fn origin_generic<S: ClientStore>(c: &mut Client<S>, scope: &str, head: u64) -> u64 {
    let state = c.ensure_subscription(scope).unwrap();
    let expected = std::collections::BTreeMap::from([(scope.to_string(), state.subscription_id)]);
    let heads = std::collections::BTreeMap::from([(scope.to_string(), head)]);
    c.initialize_subscriptions(&expected, &heads).unwrap();
    state.subscription_id
}
/// The reads a [`Recording`] store saw: each statement's SQL and how many
/// values it bound.
#[derive(Clone, Default)]
struct Reads(Rc<RefCell<Vec<(String, usize)>>>);
impl Reads {
    fn record(&self, sql: &str, parameters: &[serde_json::Value]) {
        self.0
            .borrow_mut()
            .push((sql.to_string(), parameters.len()));
    }
    fn take(&self) -> Vec<(String, usize)> {
        std::mem::take(&mut self.0.borrow_mut())
    }
}
/// A SQLite store that records the bind count of every `query` and
/// `query_committed` it forwards: the deterministic view of how many values a
/// statement bound, whatever variable limit the host SQLite was built with.
struct Recording {
    inner: SqliteStore,
    reads: Reads,
}
impl ClientStore for Recording {
    fn begin(&mut self) -> Result<()> {
        self.inner.begin()
    }
    fn commit(&mut self) -> Result<()> {
        self.inner.commit()
    }
    fn rollback(&mut self) -> Result<()> {
        self.inner.rollback()
    }
    fn savepoint(&mut self, name: &str) -> Result<()> {
        self.inner.savepoint(name)
    }
    fn release(&mut self, name: &str) -> Result<()> {
        self.inner.release(name)
    }
    fn rollback_to(&mut self, name: &str) -> Result<()> {
        self.inner.rollback_to(name)
    }
    fn execute(&mut self, sql: &str, parameters: &[serde_json::Value]) -> Result<usize> {
        self.inner.execute(sql, parameters)
    }
    fn execute_batch(&mut self, sql: &str) -> Result<()> {
        self.inner.execute_batch(sql)
    }
    fn query(&mut self, sql: &str, parameters: &[serde_json::Value]) -> Result<SqlRows> {
        self.reads.record(sql, parameters);
        self.inner.query(sql, parameters)
    }
    fn query_committed(&mut self, sql: &str, parameters: &[serde_json::Value]) -> Result<SqlRows> {
        self.reads.record(sql, parameters);
        self.inner.query_committed(sql, parameters)
    }
}
