mod common;
use axton_client::*;
use axton_sqlite::SqliteStore;
use common::*;
use serde_json::{Value, json};
use std::collections::BTreeMap;

fn stamped(channel: &str, from: u64, to: u64, stamp: u64, text: Option<&str>) -> PullPage {
    let mut p = page(channel, from, to, text);
    p.changes[0].stamp = stamp;
    p
}

/// A delete applies across channels by stamp: the record goes on the first
/// channel that delivers it and the stamp stays as evidence; the other
/// channel's copy of the delete is a no-op that still advances its cursor.
#[test]
fn cross_channel_delete_applies_by_stamp_and_retains_the_stamp() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    subscribe(&mut c, "b");
    c.apply_page(stamped("a", 0, 1, 1, Some("A"))).unwrap();
    c.apply_page(stamped("b", 0, 1, 2, Some("B"))).unwrap();
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "B");
    c.apply_page(stamped("a", 1, 2, 3, None)).unwrap();
    assert!(
        c.read(&key()).unwrap().is_none(),
        "a stamped delete applies across channels"
    );
    assert_eq!(c.record_stamp(&key()).unwrap(), 3);
    assert_eq!(table_count(&mut c, "axton_record"), 1);
    c.apply_page(stamped("b", 1, 2, 4, None)).unwrap();
    assert_eq!(c.record_stamp(&key()).unwrap(), 4);
    assert_eq!(
        table_count(&mut c, "axton_record"),
        1,
        "the stamp is retained after every channel confirmed the delete"
    );
    assert_eq!(c.cursor("a").unwrap(), Some(2));
    assert_eq!(c.cursor("b").unwrap(), Some(2));
}

#[test]
fn older_stamp_cannot_regress_newer_authority_but_advances_the_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    subscribe(&mut c, "b");
    c.apply_page(stamped("b", 0, 1, 11, Some("NEW"))).unwrap();
    let report = c.apply_page(stamped("a", 0, 1, 10, Some("OLD"))).unwrap();
    assert_eq!(
        (report.applied, report.skipped(), report.conflicts()),
        (0, 0, 0),
        "an older stamp changes nothing and is not a report either"
    );
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "NEW");
    assert_eq!(c.record_stamp(&key()).unwrap(), 11);
    assert_eq!(c.cursor("a").unwrap(), Some(1));
    let old_delete = c.apply_page(stamped("a", 1, 2, 9, None)).unwrap();
    assert_eq!(old_delete.applied, 0);
    assert!(old_delete.reports.is_empty());
    assert_eq!(
        c.read(&key()).unwrap().unwrap()["text"],
        "NEW",
        "an old tombstone cannot delete newer content"
    );
    assert_eq!(c.cursor("a").unwrap(), Some(2));
}

#[test]
fn equal_stamp_is_idempotent_or_a_diagnostic() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    subscribe(&mut c, "b");
    c.apply_page(stamped("a", 0, 1, 5, Some("X"))).unwrap();
    let same = c.apply_page(stamped("b", 0, 1, 5, Some("X"))).unwrap();
    assert_eq!(same.conflicts(), 0);
    let conflict = c.apply_page(stamped("b", 1, 2, 5, Some("Y"))).unwrap();
    assert_eq!(conflict.conflicts(), 1);
    assert_eq!(conflict.reports[0].stamp, 5);
    assert_eq!(conflict.reports[0].model, "Entry");
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "X");
    assert_eq!(
        c.cursor("b").unwrap(),
        Some(2),
        "the channel is not stalled"
    );
}

#[test]
fn newer_authority_lands_beneath_pending_edits_and_replays_them() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "book");
    c.apply_page(page("book", 0, 1, Some("A"))).unwrap();
    c.transaction(|tx| {
        tx.enqueue(mutation("B"))?;
        Ok(())
    })
    .unwrap();
    c.apply_page(page("book", 1, 2, Some("SERVER"))).unwrap();
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "B");
    assert_eq!(
        c.read_sql("SELECT text FROM axton_before_Entry", &[])
            .unwrap(),
        vec![json!({"text":"SERVER"})]
    );
}

/// A change whose state does not fit the schema is skipped and reported; the
/// page and its cursor still land ([#51](https://github.com/zanminwang/axton/issues/51)).
#[test]
fn a_change_the_schema_refuses_is_reported_and_the_cursor_still_advances() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "book");
    c.apply_page(page("book", 0, 1, Some("A"))).unwrap();
    let mut bad = page("book", 1, 2, Some("B"));
    bad.changes[0].state = json!({"text":22});
    let report = c.apply_page(bad).unwrap();
    assert_eq!(report.skipped(), 1);
    assert_eq!(report.reports[0].kind, ReportKind::Skipped);
    assert_eq!(report.reports[0].identity, json!({"id":"e"}));
    assert_eq!(report.reports[0].stamp, 2);
    assert!(
        report.reports[0].detail["error"].is_string(),
        "{:?}",
        report.reports[0]
    );
    assert_eq!(report.cursors, BTreeMap::from([("book".to_string(), 2)]));
    assert_eq!(c.cursor("book").unwrap(), Some(2));
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "A");
    assert_eq!(
        c.record_stamp(&key()).unwrap(),
        1,
        "a skipped change leaves no stamp"
    );
    let stale = c.apply_page(page("book", 1, 2, Some("Z"))).unwrap();
    assert!(stale.stale);
    assert!(
        c.apply_page(page("book", 5, 6, Some("Z"))).is_err(),
        "cursor gap"
    );
}

/// A parent's authoritative deletion cascades to its declared descendants
/// locally without touching their stamp evidence: a child keeps the stamp it
/// was delivered at, and a newer child page can still bring it back.
#[test]
fn delete_cascades_to_descendants_and_keeps_their_stamps() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = Client::open(
        SqliteStore::open(dir.path().join("db")).unwrap(),
        family_schema(),
    )
    .unwrap();
    subscribe(&mut c, "lib");
    let book = |cursor: u64, state| {
        multi(
            &[("lib", cursor - 1, cursor, cursor)],
            vec![AuthorityRecord {
                model: "Book".into(),
                identity: json!({"id":"b"}),
                stamp: cursor,
                state,
                error: None,
            }],
        )
    };
    let comment = |cursor: u64, stamp, state| {
        multi(
            &[("lib", cursor - 1, cursor, cursor)],
            vec![AuthorityRecord {
                model: "Comment".into(),
                identity: json!({"id":"c"}),
                stamp,
                state,
                error: None,
            }],
        )
    };
    let comment_key = family_schema()
        .record_key("Comment", &json!({"id":"c"}))
        .unwrap();
    c.apply_page(book(1, json!({"title":"T"}))).unwrap();
    c.apply_page(comment(2, 2, json!({"bookId":"b","text":"hi"})))
        .unwrap();
    c.apply_page(book(3, Value::Null)).unwrap();
    assert!(c.query("Comment", &json!({})).unwrap().is_empty());
    assert_eq!(
        c.record_stamp(&comment_key).unwrap(),
        2,
        "the parent's deletion does not rewrite the child's stamp"
    );
    assert_eq!(table_count(&mut c, "axton_record"), 2);
    let stale = c
        .apply_page(comment(4, 2, json!({"bookId":"b","text":"hi"})))
        .unwrap();
    assert_eq!(
        stale.conflicts(),
        1,
        "equal stamp, different content: reported"
    );
    assert!(c.query("Comment", &json!({})).unwrap().is_empty());
    c.apply_page(comment(5, 3, json!({"bookId":"b","text":"again"})))
        .unwrap();
    assert_eq!(
        c.query("Comment", &json!({})).unwrap().len(),
        1,
        "newer child authority applies on its own stamp"
    );
}

/// Unsubscribing stops the channel's delivery and nothing else: rows, stamps,
/// before images and pending edits stay; a page still in flight for the
/// channel is dropped without writing anything.
#[test]
fn unsubscribing_retains_records_and_later_pages_are_dropped() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    c.apply_page(stamped("a", 0, 1, 5, Some("A"))).unwrap();
    let mut clean = stamped("a", 1, 2, 6, Some("C"));
    clean.changes[0].identity = json!({"id":"clean"});
    c.apply_page(clean).unwrap();
    c.transaction(|tx| tx.enqueue(mutation("B"))).unwrap();
    c.freeze().unwrap().unwrap();
    c.transaction(|tx| tx.set_channel("a".into(), false))
        .unwrap();
    assert_eq!(table_count(&mut c, "axton_subscription"), 0);
    assert_eq!(
        c.read(&key()).unwrap().unwrap()["text"],
        "B",
        "the dirty row keeps its pending edit"
    );
    let clean_key = schema()
        .record_key("Entry", &json!({"id":"clean"}))
        .unwrap();
    assert_eq!(
        c.read(&clean_key).unwrap().unwrap()["text"],
        "C",
        "the clean row is retained"
    );
    assert_eq!(
        c.pending_count().unwrap(),
        1,
        "the push in flight is untouched"
    );
    assert_eq!(c.before_image_count().unwrap(), 1, "the base is kept");
    assert_eq!(c.record_stamp(&key()).unwrap(), 5);
    assert_eq!(c.record_stamp(&clean_key).unwrap(), 6);
    let report = c.apply_page(stamped("a", 2, 3, 7, Some("X"))).unwrap();
    assert!(
        report.stale,
        "a page for an unsubscribed channel is dropped whole"
    );
    assert_eq!(table_count(&mut c, "axton_subscription"), 0);
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "B");
    // Completion still works with no subscription at all.
    let r = receipt(&mut c, 1, vec![authority(Some("B"), 8)]);
    c.acknowledge(1, r).unwrap();
    assert_eq!(c.pending_count().unwrap(), 0);
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "B");
}

/// Retained content is still updated by another active channel, and the
/// last subscription going away removes nothing. Everything survives reopen.
#[test]
fn another_channel_updates_retained_content_and_restart_keeps_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open(&path);
    subscribe(&mut c, "a");
    subscribe(&mut c, "b");
    c.apply_page(stamped("a", 0, 1, 1, Some("from a"))).unwrap();
    c.transaction(|tx| tx.set_channel("a".into(), false))
        .unwrap();
    c.apply_page(stamped("b", 0, 1, 2, Some("from b"))).unwrap();
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "from b");
    c.transaction(|tx| tx.set_channel("b".into(), false))
        .unwrap();
    assert!(c.subscriptions().unwrap().is_empty());
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "from b");
    drop(c);
    let mut c = open(&path);
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "from b");
    assert_eq!(c.record_stamp(&key()).unwrap(), 2);
    // A newer deletion still applies; stale content cannot resurrect it.
    subscribe(&mut c, "a");
    c.apply_page(stamped("a", 0, 1, 3, None)).unwrap();
    assert!(c.read(&key()).unwrap().is_none());
    c.apply_page(stamped("a", 1, 2, 2, Some("stale"))).unwrap();
    assert!(c.read(&key()).unwrap().is_none());
    assert_eq!(c.record_stamp(&key()).unwrap(), 3);
}

/// A channel cursor and a record stamp are independent counters: a page whose
/// cursors are far ahead of the stamp, and one whose stamp is far ahead of
/// the cursors, both apply by their own rule.
#[test]
fn cursor_and_stamp_are_independent() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    c.apply_page(stamped("a", 0, 100, 2, Some("low stamp, high cursor")))
        .unwrap();
    assert_eq!(c.cursor("a").unwrap(), Some(100));
    assert_eq!(c.record_stamp(&key()).unwrap(), 2);
    c.apply_page(stamped("a", 100, 101, 900, Some("high stamp")))
        .unwrap();
    assert_eq!(c.cursor("a").unwrap(), Some(101));
    assert_eq!(c.record_stamp(&key()).unwrap(), 900);
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "high stamp");
    // A receipt never moves a cursor.
    c.transaction(|tx| tx.enqueue(mutation("B"))).unwrap();
    c.freeze().unwrap().unwrap();
    let r = receipt(&mut c, 1, vec![authority(Some("B"), 901)]);
    c.acknowledge(1, r).unwrap();
    assert_eq!(c.cursor("a").unwrap(), Some(101));
}

/// A2: a page answering a pull issued before the channel was unsubscribed and
/// subscribed again is stale, not a gap, on every incoming path (issue #32). A page
/// the client never requested that starts beyond its cursor is still a gap.
#[test]
fn page_from_a_previous_subscription_is_stale_not_a_gap() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    c.apply_page(page("a", 0, 1, Some("A"))).unwrap();
    let in_flight = c.downlink_request().unwrap().unwrap();
    assert!(in_flight.contains("\"cursors\":{\"a\":1}"));
    resubscribe(&mut c, "a");
    assert_eq!(c.cursor("a").unwrap(), Some(0));

    // apply_page: the answer to the old request is dropped, the cursor stays at 0
    // and the retained row is untouched.
    let report = c.apply_page(page("a", 1, 2, Some("B"))).unwrap();
    assert!(report.stale, "{report:?}");
    assert_eq!(report.applied, 0);
    assert_eq!(c.cursor("a").unwrap(), Some(0));
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "A");
    // The same page with no request behind it is a genuine gap for direct callers.
    assert!(c.apply_page(page("a", 1, 2, Some("B"))).is_err());
    // A fresh pull from the reset cursor delivers everything.
    let fresh = c.downlink_request().unwrap().unwrap();
    assert!(fresh.contains("\"cursors\":{\"a\":0}"));
    assert_eq!(c.apply_page(page("a", 0, 2, Some("B"))).unwrap().applied, 1);
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "B");

    // receive_downlink: covered rather than recover, so the SDK does not re-catch-up.
    let request = PullRequest::decode(c.downlink_request().unwrap().unwrap().as_bytes()).unwrap();
    assert_eq!(request.cursors["a"], 2);
    resubscribe(&mut c, "a");
    let progress = c
        .receive_downlink(page("a", 2, 3, Some("C")), Some(request))
        .unwrap();
    assert_eq!(progress.disposition, "covered");
    assert_eq!(c.cursor("a").unwrap(), Some(0));
    // The SDK's late-catch-up shape: the old request is still outstanding when the
    // new subscription issues its own request from the same cursor; the fresh
    // answer applies, and the obsolete answer is then dropped.
    let old = PullRequest::decode(c.downlink_request().unwrap().unwrap().as_bytes()).unwrap();
    resubscribe(&mut c, "a");
    let fresh = PullRequest::decode(c.downlink_request().unwrap().unwrap().as_bytes()).unwrap();
    assert_eq!((old.cursors["a"], fresh.cursors["a"]), (0, 0));
    // Stamps keep counting up: the retained row only takes newer content.
    let progress = c
        .receive_downlink(stamped("a", 0, 1, 3, Some("fresh")), Some(fresh))
        .unwrap();
    assert_eq!(progress.disposition, "applied");
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "fresh");
    let progress = c
        .receive_downlink(stamped("a", 0, 1, 4, Some("obsolete")), Some(old))
        .unwrap();
    assert_eq!(progress.disposition, "covered");
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "fresh");
    // Back to a clean channel for the SyncCycle steps below.
    resubscribe(&mut c, "a");
    // An unrequested page beyond the cursor is still a gap to recover from.
    let progress = c
        .receive_downlink(page("a", 2, 3, Some("C")), None)
        .unwrap();
    assert_eq!(progress.disposition, "recover");

    // SyncCycle: completing the old pull after a resubscribe is not an error.
    let mut cycle = SyncCycle::default();
    cycle.restart();
    let action = cycle.next(&mut c).unwrap().unwrap();
    assert_eq!(action.kind, "pull");
    resubscribe(&mut c, "a");
    cycle
        .complete(
            &mut c,
            text(&stamped("a", 0, 1, 5, Some("old"))).as_bytes(),
        )
        .unwrap();
    assert_eq!(
        c.cursor("a").unwrap(),
        Some(0),
        "the stale answer did not move the cursor"
    );
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "fresh");

    // Subscribing another channel does not make channel a's pull stale.
    cycle.restart();
    let action = cycle.next(&mut c).unwrap().unwrap();
    assert_eq!(action.kind, "pull");
    c.transaction(|tx| tx.set_channel("b".into(), true))
        .unwrap();
    cycle
        .complete(
            &mut c,
            text(&stamped("a", 0, 1, 6, Some("kept"))).as_bytes(),
        )
        .unwrap();
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "kept");
}

#[test]
fn older_subscription_response_cannot_discard_a_fresh_response() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    let old = PullRequest::decode(c.downlink_request().unwrap().unwrap().as_bytes()).unwrap();
    resubscribe(&mut c, "a");
    let fresh = PullRequest::decode(c.downlink_request().unwrap().unwrap().as_bytes()).unwrap();
    // The requests have identical wire identities. Receiving the old answer
    // first must not consume the fresh request and discard its later answer.
    c.receive_downlink(stamped("a", 0, 1, 1, Some("old")), Some(old))
        .unwrap();
    let fresh_progress = c
        .receive_downlink(stamped("a", 0, 2, 2, Some("fresh")), Some(fresh))
        .unwrap();
    assert_eq!(fresh_progress.disposition, "applied");
    assert_eq!(c.cursor("a").unwrap(), Some(2));
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "fresh");
}

fn entry(id: &str, stamp: u64, text: &str) -> AuthorityRecord {
    authority_of(id, Some(text), stamp)
}
fn read(c: &mut Client<SqliteStore>, id: &str) -> Option<Value> {
    let key = schema().record_key("Entry", &json!({ "id": id })).unwrap();
    c.read(&key).unwrap()
}

/// One page names every channel: each channel's cursor moves, and a record
/// changed in two channels is in the page once and lands once.
#[test]
fn a_page_moves_every_channel_it_names_and_a_shared_record_lands_once() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    subscribe(&mut c, "b");
    let page = multi(
        &[("a", 0, 2, 2), ("b", 0, 1, 1)],
        vec![entry("e", 3, "shared"), entry("only-a", 2, "A")],
    );
    let report = c.apply_page(page).unwrap();
    assert_eq!(report.applied, 2);
    assert!(report.reports.is_empty());
    assert_eq!(
        report.cursors,
        BTreeMap::from([("a".to_string(), 2), ("b".to_string(), 1)])
    );
    assert_eq!(
        (c.cursor("a").unwrap(), c.cursor("b").unwrap()),
        (Some(2), Some(1))
    );
    assert_eq!(read(&mut c, "e").unwrap()["text"], "shared");
    assert_eq!(c.record_stamp(&key()).unwrap(), 3);
    assert_eq!(read(&mut c, "only-a").unwrap()["text"], "A");
}

/// Gating is per channel: a channel already covered contributes nothing while
/// the others move; a channel with a gap keeps the whole page out. An
/// unsubscribed channel's part is ignored.
#[test]
fn channels_are_gated_one_by_one_and_a_gap_holds_the_whole_page() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    subscribe(&mut c, "b");
    c.apply_page(multi(&[("a", 0, 5, 5)], vec![entry("e", 1, "A")]))
        .unwrap();
    // `a` is covered (to <= cursor); `b` moves; the record lands by stamp.
    let report = c
        .apply_page(multi(
            &[("a", 3, 5, 5), ("b", 0, 2, 2)],
            vec![entry("e", 2, "B")],
        ))
        .unwrap();
    assert_eq!(report.cursors, BTreeMap::from([("b".to_string(), 2)]));
    assert_eq!(read(&mut c, "e").unwrap()["text"], "B");
    // A gap on `b` keeps the page out even though `a` connects.
    let gap = c.apply_page(multi(
        &[("a", 5, 6, 6), ("b", 4, 5, 5)],
        vec![entry("e", 3, "C")],
    ));
    assert!(gap.is_err(), "{gap:?}");
    assert_eq!(
        (c.cursor("a").unwrap(), c.cursor("b").unwrap()),
        (Some(5), Some(2))
    );
    assert_eq!(read(&mut c, "e").unwrap()["text"], "B");
    // Everything covered: stale, nothing written.
    let covered = c
        .apply_page(multi(
            &[("a", 4, 5, 5), ("b", 1, 2, 2)],
            vec![entry("e", 9, "late")],
        ))
        .unwrap();
    assert!(covered.stale);
    assert_eq!(read(&mut c, "e").unwrap()["text"], "B");
    // An unsubscribed channel's part is ignored; the other channel still moves.
    c.transaction(|tx| tx.set_channel("b".into(), false))
        .unwrap();
    let report = c
        .apply_page(multi(
            &[("a", 5, 6, 6), ("b", 2, 3, 3)],
            vec![entry("e", 4, "D")],
        ))
        .unwrap();
    assert_eq!(report.cursors, BTreeMap::from([("a".to_string(), 6)]));
    assert_eq!(table_count(&mut c, "axton_subscription"), 1);
    assert_eq!(read(&mut c, "e").unwrap()["text"], "D");
}

/// The page is one transaction: a change that cannot be applied leaves nothing
/// behind, the other changes land, and the cursors move with the page.
#[test]
fn an_error_change_keeps_local_content_and_stamp_and_is_reported() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    c.apply_page(multi(&[("a", 0, 1, 1)], vec![entry("e", 1, "A")]))
        .unwrap();
    let failed = AuthorityRecord {
        model: "Entry".into(),
        identity: json!({"id":"e"}),
        stamp: 5,
        state: Value::Null,
        error: Some("loader.failed".into()),
    };
    let report = c
        .apply_page(multi(
            &[("a", 1, 3, 3)],
            vec![failed, entry("other", 2, "O")],
        ))
        .unwrap();
    assert_eq!(report.applied, 1);
    assert_eq!(report.read_failed(), 1);
    let failure = &report.reports[0];
    assert_eq!(failure.kind, ReportKind::ReadFailed);
    assert_eq!(failure.code.as_deref(), Some("loader.failed"));
    assert_eq!((failure.model.as_str(), failure.stamp), ("Entry", 5));
    assert_eq!(
        read(&mut c, "e").unwrap()["text"],
        "A",
        "an error change is never a deletion"
    );
    assert_eq!(c.record_stamp(&key()).unwrap(), 1, "the stamp is kept");
    assert_eq!(read(&mut c, "other").unwrap()["text"], "O");
    assert_eq!(c.cursor("a").unwrap(), Some(3));
    // A later delivery of the record corrects it on its own stamp.
    c.apply_page(multi(&[("a", 3, 4, 4)], vec![entry("e", 5, "fixed")]))
        .unwrap();
    assert_eq!(read(&mut c, "e").unwrap()["text"], "fixed");
    assert_eq!(c.record_stamp(&key()).unwrap(), 5);
}

/// A conflict (same stamp, different content) is reported with both sides;
/// the report goes through `receive_downlink` unchanged.
#[test]
fn a_conflict_is_reported_through_receive_downlink() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    c.apply_page(stamped("a", 0, 1, 5, Some("X"))).unwrap();
    let progress = c
        .receive_downlink(stamped("a", 1, 2, 5, Some("Y")), None)
        .unwrap();
    assert_eq!(progress.disposition, "applied");
    assert_eq!(progress.report.conflicts(), 1);
    let conflict = &progress.report.reports[0];
    assert_eq!(conflict.kind, ReportKind::Conflict);
    assert_eq!(conflict.detail["local"]["text"], "X");
    assert_eq!(conflict.detail["incoming"]["text"], "Y");
    assert_eq!(
        progress.report.cursors,
        BTreeMap::from([("a".to_string(), 2)])
    );
    assert!(progress.continues.is_empty());
    assert!(progress.gaps.is_empty());
    // A gap on one channel names it; nothing is applied.
    subscribe(&mut c, "b");
    let progress = c
        .receive_downlink(
            multi(&[("a", 2, 3, 9), ("b", 4, 5, 5)], vec![entry("e", 6, "Z")]),
            None,
        )
        .unwrap();
    assert_eq!(progress.disposition, "recover");
    assert_eq!(progress.gaps, vec!["b".to_string()]);
    assert_eq!(progress.continues, vec!["a".to_string()]);
    assert_eq!(c.cursor("a").unwrap(), Some(2));
    assert_eq!(read(&mut c, "e").unwrap()["text"], "X");
}

/// A record whose content violates a local constraint is skipped and reported
/// without leaving anything half-written; the records around it land and the
/// cursor advances.
#[test]
fn a_record_that_violates_a_local_constraint_is_skipped_alone() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = Client::open(
        SqliteStore::open(dir.path().join("db")).unwrap(),
        family_schema(),
    )
    .unwrap();
    subscribe(&mut c, "lib");
    let comment = |id: &str, text: &str, stamp: u64| AuthorityRecord {
        model: "Comment".into(),
        identity: json!({ "id": id }),
        stamp,
        state: json!({"bookId":"b","text":text}),
        error: None,
    };
    let report = c
        .apply_page(multi(
            &[("lib", 0, 3, 3)],
            vec![
                comment("c1", "same", 1),
                // Same (bookId, text) as c1: the local unique index refuses it.
                comment("c2", "same", 2),
                comment("c3", "other", 3),
            ],
        ))
        .unwrap();
    assert_eq!(report.applied, 2);
    assert_eq!(report.reports.len(), 1);
    assert_eq!(report.reports[0].kind, ReportKind::Skipped);
    assert_eq!(report.reports[0].identity, json!({"id":"c2"}));
    let c2 = family_schema()
        .record_key("Comment", &json!({"id":"c2"}))
        .unwrap();
    assert!(c.read(&c2).unwrap().is_none());
    assert_eq!(c.record_stamp(&c2).unwrap(), 0, "no stamp without content");
    assert_eq!(c.query("Comment", &json!({})).unwrap().len(), 2);
    assert_eq!(c.cursor("lib").unwrap(), Some(3));
}

#[test]
fn channel_membership_page_ignores_unsubscribed_changes_without_progress() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    subscribe(&mut c, "b");
    c.apply_page(page("a", 0, 1, Some("cached"))).unwrap();
    c.transaction(|tx| tx.set_channel("a".into(), false))
        .unwrap();
    c.apply_channel_page(ChannelPullPage {
        cursors: BTreeMap::from([
            (
                "a".into(),
                CursorRange {
                    from: 1,
                    to: 2,
                    head: 2,
                },
            ),
            (
                "b".into(),
                CursorRange {
                    from: 0,
                    to: 1,
                    head: 1,
                },
            ),
        ]),
        changes: vec![ChannelChange::Remove {
            channel: "a".into(),
            cursor: 2,
            key: key(),
        }],
    })
    .unwrap();
    assert_eq!(c.cursor("a").unwrap(), None);
    assert_eq!(c.cursor("b").unwrap(), Some(1));
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "cached");
    assert_eq!(table_count(&mut c, "axton_channel_member"), 0);
}
