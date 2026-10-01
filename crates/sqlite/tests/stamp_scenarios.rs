//! Acceptance scenarios for per-record stamps across scopes. Scopes are
//! delivery paths: they never own a record, and stamp evidence outlives both
//! deletion and unsubscription.
mod common;
use axton_client::*;
use common::*;

fn stamped(scope: &str, from: u64, to: u64, stamp: u64, text: Option<&str>) -> PullPage {
    let mut p = page(scope, from, to, text);
    p.changes[0].stamp = stamp;
    p
}

/// Spec scenario 1: the newer content arrives through B first; A's delayed older page
/// cannot regress it, but A's cursor still advances.
#[test]
fn delayed_page_from_another_scope_cannot_regress_newer_content() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    subscribe(&mut c, "b");
    c.apply_page(stamped("b", 0, 5, 8, Some("new"))).unwrap();
    let report = c.apply_page(stamped("a", 0, 10, 7, Some("old"))).unwrap();
    assert_eq!(report.applied, 0, "older content changes nothing");
    assert_eq!(report.conflicts(), 0);
    assert_eq!(
        report.cursors["a"], 10,
        "but the page still moves the scope"
    );
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "new");
    assert_eq!(c.cursor("a").unwrap(), Some(10));
    assert_eq!(c.cursor("b").unwrap(), Some(5));
    assert_eq!(c.record_stamp(&key()).unwrap(), 8);
    // A catches up with the same change at the same stamp: nothing to change.
    c.apply_page(stamped("a", 10, 11, 8, Some("new"))).unwrap();
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "new");
}

/// Spec scenario 2: the same page delivered twice is idempotent.
#[test]
fn redelivered_page_is_a_no_op() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    c.apply_page(stamped("a", 0, 1, 1, Some("A"))).unwrap();
    let again = c.apply_page(stamped("a", 0, 1, 1, Some("A"))).unwrap();
    assert!(again.stale);
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "A");
    assert_eq!(table_count(&mut c, "axton_record"), 1);
}

/// Spec scenario 6: a delete with a newer stamp removes the record on the first
/// scope that delivers it; the stamp survives so an older upsert arriving in
/// between is discarded, and the other scope's copy of the delete is a no-op.
#[test]
fn delete_keeps_its_stamp_so_stale_content_cannot_resurrect_the_record() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    subscribe(&mut c, "b");
    c.apply_page(stamped("a", 0, 1, 1, Some("A"))).unwrap();
    c.apply_page(stamped("b", 0, 1, 2, Some("B"))).unwrap();
    c.apply_page(stamped("b", 1, 2, 4, None)).unwrap();
    assert!(c.read(&key()).unwrap().is_none());
    assert_eq!(c.record_stamp(&key()).unwrap(), 4);
    // A delayed older upsert (stamp 3) on A must not resurrect the record.
    c.apply_page(stamped("a", 1, 2, 3, Some("A2"))).unwrap();
    assert!(c.read(&key()).unwrap().is_none());
    assert_eq!(c.record_stamp(&key()).unwrap(), 4);
    // A's copy of the delete is the same version: nothing changes, the cursor moves.
    c.apply_page(stamped("a", 2, 3, 4, None)).unwrap();
    assert!(c.read(&key()).unwrap().is_none());
    assert_eq!(c.cursor("a").unwrap(), Some(3));
    assert_eq!(
        table_count(&mut c, "axton_record"),
        1,
        "the stamp is retained"
    );
}

/// Spec scenario 5: a record moves A -> B -> A. Each hop is one change published
/// to the scopes that now provide it; the client keeps the newest stamp
/// whichever scope delivered it, in either arrival order.
#[test]
fn move_between_scopes_and_back() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    subscribe(&mut c, "b");
    c.apply_page(stamped("a", 0, 1, 1, Some("in A"))).unwrap();
    // Move to B: B's upsert (stamp 3) arrives before A's last word (stamp 2).
    c.apply_page(stamped("b", 0, 1, 3, Some("in B"))).unwrap();
    c.apply_page(stamped("a", 1, 2, 2, Some("leaving A")))
        .unwrap();
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "in B");
    assert_eq!(c.record_stamp(&key()).unwrap(), 3);
    // Move back to A: A's upsert (stamp 4), then a deletion (stamp 5) through B.
    c.apply_page(stamped("a", 2, 3, 4, Some("back in A")))
        .unwrap();
    c.apply_page(stamped("b", 1, 2, 5, None)).unwrap();
    assert!(
        c.read(&key()).unwrap().is_none(),
        "the newest stamp is the deletion"
    );
    // The next change on A (stamp 6) restores it.
    c.apply_page(stamped("a", 3, 4, 6, Some("back in A")))
        .unwrap();
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "back in A");
    assert_eq!(c.record_stamp(&key()).unwrap(), 6);
}

/// Spec scenario 8: stamps and tombstones survive close and reopen, and
/// unsubscribing in between removes nothing.
#[test]
fn reopen_preserves_stamps_and_tombstones() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open(&path);
    subscribe(&mut c, "a");
    subscribe(&mut c, "b");
    c.apply_page(stamped("a", 0, 1, 1, Some("A"))).unwrap();
    c.apply_page(stamped("b", 0, 1, 2, Some("B"))).unwrap();
    c.apply_page(stamped("b", 1, 2, 4, None)).unwrap();
    c.transaction(|tx| tx.set_stream("b".into(), false))
        .unwrap();
    drop(c);
    let mut c = open(&path);
    assert!(c.read(&key()).unwrap().is_none());
    assert_eq!(c.record_stamp(&key()).unwrap(), 4);
    c.apply_page(stamped("a", 1, 2, 3, Some("stale"))).unwrap();
    assert!(c.read(&key()).unwrap().is_none());
    c.apply_page(stamped("a", 2, 3, 5, Some("alive"))).unwrap();
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "alive");
    assert_eq!(table_count(&mut c, "axton_record"), 1);
}
