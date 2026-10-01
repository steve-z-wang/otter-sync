//! Durable subscription identity: a subscription is a row with its own
//! allocated ID, and its cursor pair is NULL until a first boundary is
//! committed ([#150](https://github.com/zanminwang/axton/issues/150)).
mod common;
use axton_client::*;
use axton_sqlite::SqliteStore;
use common::*;
use serde_json::json;
use std::collections::BTreeMap;

/// Run `sql` on its own connection to the same file: the tests reach past the
/// Client to prove the table, not the code writing it, holds the contract.
fn sql(path: &std::path::Path, statement: &str) -> Result<()> {
    let mut store = SqliteStore::open(path)?;
    store.execute_batch(statement)
}

fn state(c: &mut Client<SqliteStore>, scope: &str) -> SubscriptionState {
    c.subscription_state(scope).unwrap().expect("a row")
}

/// The brief's contract: one identity per Scope name, allocated once, kept
/// across duplicate registration, replaced by recreation, and unaffected by an
/// unsubscribe naming the identity that is gone.
#[test]
fn duplicate_registration_keeps_one_identity_and_recreation_allocates_another() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    let a = client.ensure_subscription("project:123")?;
    let b = client.ensure_subscription("project:123")?;
    assert_eq!(a.subscription_id, b.subscription_id);
    assert_eq!(a.starting_cursor, None);
    assert_eq!(a.cursor, None);
    client.remove_subscription("project:123", a.subscription_id)?;
    let c = client.ensure_subscription("project:123")?;
    assert_ne!(a.subscription_id, c.subscription_id);
    client.remove_subscription("project:123", a.subscription_id)?;
    assert!(client.subscription_state("project:123")?.is_some());
    Ok(())
}

/// Registration is durable and the allocator never recycles: the reopened
/// replica answers with the same identity, and the next Scope gets a new one.
#[test]
fn identity_and_uninitialized_cursors_survive_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open(&path);
    let first = c.ensure_subscription("a").unwrap();
    c.remove_subscription("a", first.subscription_id).unwrap();
    let second = c.ensure_subscription("a").unwrap();
    drop(c);

    let mut c = open(&path);
    let reopened = state(&mut c, "a");
    assert_eq!(reopened.scope, "a");
    assert_eq!(reopened.subscription_id, second.subscription_id);
    assert_eq!((reopened.starting_cursor, reopened.cursor), (None, None));
    assert_eq!(
        c.ensure_subscription("b").unwrap().subscription_id,
        second.subscription_id + 1,
        "the allocator carries on across the reopen"
    );
}

/// A rolled-back transaction registers nothing: no row, no allocation the
/// next subscribe could skip, no subscription generation change.
#[test]
fn a_rolled_back_registration_leaves_no_subscription() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    let generation = c.subscription_generation();
    let failed: Result<()> = c.transaction(|tx| {
        tx.set_scope("a".into(), true)?;
        Err(invalid("host failure"))
    });
    assert!(failed.is_err());
    assert!(c.subscription_state("a").unwrap().is_none());
    assert_eq!(c.subscription_generation(), generation);
    assert_eq!(
        c.ensure_subscription("b").unwrap().subscription_id,
        1,
        "the rolled-back allocation is not spent"
    );
}

/// A Scope name the wire refuses names no Scope: the ledger refuses it too, at
/// every registration path. A stored row for it would ask a handshake for a
/// scope the server rejects, and every pump - for every other Scope - would
/// fail with it.
#[test]
fn a_blank_scope_name_is_refused_and_registers_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    c.ensure_subscription("a").unwrap();
    let generation = c.subscription_generation();
    for blank in ["", " ", "\t\n", "   "] {
        let refused = c.ensure_subscription(blank).expect_err("a blank name");
        assert!(
            refused.to_string().contains("scope must not be empty"),
            "{refused}"
        );
        assert!(c.subscription_state(blank).unwrap().is_none(), "no row");
        let refused = c
            .transaction(|tx| tx.set_scope(blank.into(), true))
            .expect_err("a blank name, through the transaction path");
        assert!(
            refused.to_string().contains("scope must not be empty"),
            "{refused}"
        );
        assert!(c.subscription_state(blank).unwrap().is_none(), "no row");
    }
    assert_eq!(
        c.subscription_generation(),
        generation,
        "nothing was committed, so no session was invalidated"
    );
    assert_eq!(
        c.desired_scopes().unwrap().into_iter().collect::<Vec<_>>(),
        ["a".to_string()],
        "the Scope that is registered is the only one"
    );
    assert_eq!(
        c.ensure_subscription("b").unwrap().subscription_id,
        2,
        "a refused registration spends no identity"
    );
}

/// The transaction-scoped path registers intent without a boundary, and
/// repeating it changes nothing at all. The boundary is the head the first
/// acknowledgement negotiates.
#[test]
fn set_scope_registers_without_a_boundary_and_repeats_without_a_generation_change() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    c.transaction(|tx| tx.set_scope("a".into(), true)).unwrap();
    let registered = state(&mut c, "a");
    assert_eq!(
        (registered.starting_cursor, registered.cursor),
        (None, None),
        "registration commits no delivery position"
    );
    acknowledge(&mut c, &[("a", 100)]);
    let initialized = state(&mut c, "a");
    assert_eq!(
        (initialized.starting_cursor, initialized.cursor),
        (Some(100), Some(100)),
        "the first acknowledged head is the origin and the cursor"
    );
    c.apply_page(page("a", 100, 102, Some("A"))).unwrap();
    let moved = state(&mut c, "a");
    assert_eq!(
        (moved.starting_cursor, moved.cursor),
        (Some(100), Some(102)),
        "page application advances the cursor and leaves the origin"
    );
    let generation = c.subscription_generation();
    c.transaction(|tx| tx.set_scope("a".into(), true)).unwrap();
    let again = state(&mut c, "a");
    assert_eq!(
        (again.subscription_id, again.starting_cursor, again.cursor),
        (moved.subscription_id, Some(100), Some(102)),
        "a repeated subscribe reads the row and touches no cursor"
    );
    assert_eq!(
        c.subscription_generation(),
        generation,
        "a repeated subscribe is not a membership change"
    );
}

/// The initialization rule in one transaction: only a still-matching row
/// waiting for its boundary takes the acknowledged head, an initialized row
/// keeps its cursor and is reported for catch-up when the head is beyond it,
/// and a row whose identity was replaced takes nothing.
#[test]
fn initialization_commits_waiting_rows_and_leaves_every_other_row_alone() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    let a = c.ensure_subscription("a")?;
    let b = c.ensure_subscription("b")?;
    let d = c.ensure_subscription("d")?;
    // An earlier session gave a and d their first boundaries.
    let first = c.initialize_subscriptions(
        &BTreeMap::from([
            ("a".into(), a.subscription_id),
            ("d".into(), d.subscription_id),
        ]),
        &BTreeMap::from([("a".into(), 80), ("d".into(), 5)]),
    )?;
    assert_eq!(first.initialized, ["a", "d"]);
    assert_eq!(first.catch_up, [] as [String; 0]);
    assert_eq!(first.fault, None);
    // c was recreated after this session snapshotted its identity.
    let snapshotted = c.ensure_subscription("c")?;
    c.remove_subscription("c", snapshotted.subscription_id)?;
    let current = c.ensure_subscription("c")?;
    let outcome = c.initialize_subscriptions(
        &BTreeMap::from([
            ("a".into(), a.subscription_id),
            ("b".into(), b.subscription_id),
            ("c".into(), snapshotted.subscription_id),
            ("d".into(), d.subscription_id),
        ]),
        &BTreeMap::from([
            ("a".into(), 100),
            ("b".into(), 200),
            ("c".into(), 300),
            ("d".into(), 5),
        ]),
    )?;
    assert_eq!(outcome.initialized, ["b"]);
    assert_eq!(outcome.catch_up, ["a"], "a is behind its head");
    assert_eq!(outcome.fault, None);
    let a = state(&mut c, "a");
    assert_eq!(
        (a.starting_cursor, a.cursor),
        (Some(80), Some(80)),
        "an initialized row keeps its progress"
    );
    let b = state(&mut c, "b");
    assert_eq!((b.starting_cursor, b.cursor), (Some(200), Some(200)));
    let c_state = state(&mut c, "c");
    assert_eq!(
        (c_state.subscription_id, c_state.starting_cursor),
        (current.subscription_id, None),
        "a replaced identity is stale work: the current subscription is untouched"
    );
    let d = state(&mut c, "d");
    assert_eq!(
        (d.starting_cursor, d.cursor),
        (Some(5), Some(5)),
        "a head equal to the cursor is neither catch-up nor a change"
    );
    Ok(())
}

/// A head below committed progress is a reported fault: no cursor rewinds and
/// the rows the same acknowledgement would have initialized stay untouched.
#[test]
fn a_head_below_a_committed_cursor_faults_and_initializes_nothing() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    // `a` sorts before the faulting `z`, so a rule that wrote as it went would
    // already have initialized it.
    let a = c.ensure_subscription("a")?;
    subscribe(&mut c, "z");
    c.apply_page(page("z", 0, 10, Some("Z")))?;
    let z = state(&mut c, "z");
    let expected = BTreeMap::from([
        ("a".into(), a.subscription_id),
        ("z".into(), z.subscription_id),
    ]);
    let outcome = c.initialize_subscriptions(
        &expected,
        &BTreeMap::from([("a".into(), 200), ("z".into(), 9)]),
    )?;
    let fault = outcome.fault.expect("a server-state fault");
    assert!(fault.contains("below its committed cursor 10"), "{fault}");
    assert_eq!(outcome.initialized, [] as [String; 0]);
    assert_eq!(outcome.catch_up, [] as [String; 0]);
    assert_eq!(
        (state(&mut c, "z").starting_cursor, c.cursor("z")?),
        (Some(0), Some(10)),
        "progress is not modified by a fault"
    );
    assert_eq!(
        (state(&mut c, "a").starting_cursor, c.cursor("a")?),
        (None, None),
        "the whole acknowledgement was without effect"
    );
    // The same acknowledgement without the fault initializes a and catches z up.
    let outcome = c.initialize_subscriptions(
        &expected,
        &BTreeMap::from([("a".into(), 200), ("z".into(), 11)]),
    )?;
    assert_eq!(
        (outcome.initialized, outcome.catch_up),
        (vec!["a".to_string()], vec!["z".to_string()])
    );
    Ok(())
}

/// An acknowledgement that does not name exactly the session's subscriptions,
/// or carries a head no host can represent, is malformed: nothing is written.
#[test]
fn a_malformed_acknowledgement_writes_nothing() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    let a = c.ensure_subscription("a")?;
    let expected = BTreeMap::from([("a".into(), a.subscription_id)]);
    for heads in [
        BTreeMap::from([("a".into(), 1), ("x".into(), 2)]),
        BTreeMap::new(),
        BTreeMap::from([("a".into(), MAX_SAFE_INTEGER + 1)]),
    ] {
        let outcome = c.initialize_subscriptions(&expected, &heads)?;
        assert!(outcome.fault.is_some(), "{heads:?}");
        assert_eq!(outcome.initialized, [] as [String; 0]);
        assert_eq!(
            (state(&mut c, "a").starting_cursor, c.cursor("a")?),
            (None, None)
        );
        assert!(c.subscription_state("x")?.is_none());
    }
    Ok(())
}

/// The boundary is committed by one transaction: a crash before it leaves the
/// row waiting and the next acknowledgement initializes it at the head of that
/// session, while a committed boundary is resumed after reopen and never
/// re-initialized.
#[test]
fn a_first_boundary_survives_a_reopen_only_once_committed() -> Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open(&path);
    let a = c.ensure_subscription("a")?;
    let expected = BTreeMap::from([("a".into(), a.subscription_id)]);
    drop(c);

    let mut c = open(&path);
    assert_eq!(
        (state(&mut c, "a").starting_cursor, c.cursor("a")?),
        (None, None),
        "a crash before the transaction retries first initialization"
    );
    c.initialize_subscriptions(&expected, &BTreeMap::from([("a".into(), 100)]))?;
    drop(c);

    let mut c = open(&path);
    let resumed = state(&mut c, "a");
    assert_eq!(
        (resumed.starting_cursor, resumed.cursor),
        (Some(100), Some(100)),
        "a crash after it resumes from the committed boundary"
    );
    let outcome = c.initialize_subscriptions(&expected, &BTreeMap::from([("a".into(), 130)]))?;
    assert_eq!(outcome.initialized, [] as [String; 0]);
    assert_eq!(outcome.catch_up, ["a"], "the gap is caught up, not reset");
    assert_eq!(
        (state(&mut c, "a").starting_cursor, c.cursor("a")?),
        (Some(100), Some(100)),
        "only the first initialization writes the origin"
    );
    Ok(())
}

/// Unsubscribing through the transaction path removes whatever identity the
/// Scope holds, and the next subscribe is a new subscription.
#[test]
fn set_scope_removes_the_current_identity_and_recreation_starts_over() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    c.apply_page(page("a", 0, 2, Some("A"))).unwrap();
    let before = state(&mut c, "a");
    let generation = c.subscription_generation();
    c.transaction(|tx| tx.set_scope("a".into(), false)).unwrap();
    assert!(c.subscription_state("a").unwrap().is_none());
    assert!(c.subscription_generation() > generation);
    c.transaction(|tx| tx.set_scope("a".into(), true)).unwrap();
    let after = state(&mut c, "a");
    assert_ne!(after.subscription_id, before.subscription_id);
    assert_eq!(
        (after.starting_cursor, after.cursor),
        (None, None),
        "the recreated subscription waits for its own first boundary"
    );
}

/// `remove_subscription` is fenced by identity: an old handle's unsubscribe
/// cannot delete the subscription that replaced it.
#[test]
fn removal_by_a_stale_identity_changes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    let first = c.ensure_subscription("a").unwrap();
    c.remove_subscription("a", first.subscription_id).unwrap();
    let second = c.ensure_subscription("a").unwrap();
    let generation = c.subscription_generation();
    c.remove_subscription("a", first.subscription_id).unwrap();
    assert_eq!(
        state(&mut c, "a").subscription_id,
        second.subscription_id,
        "the live subscription is untouched"
    );
    assert_eq!(
        c.subscription_generation(),
        generation,
        "nothing changed, so no subscription generation change"
    );
    c.remove_subscription("a", second.subscription_id).unwrap();
    assert!(c.subscription_state("a").unwrap().is_none());
    assert!(c.subscription_generation() > generation);
}

/// An uninitialized row is durable intent without a delivery position: it is
/// in the desired set, out of every pull request, and no page advances it.
#[test]
fn an_uninitialized_subscription_is_never_read_as_cursor_zero() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    c.ensure_subscription("a").unwrap();
    assert_eq!(c.cursor("a").unwrap(), None);
    assert_eq!(c.subscriptions().unwrap(), vec![]);
    assert!(c.desired_scopes().unwrap().contains("a"));
    assert_eq!(
        c.downlink_request().unwrap(),
        None,
        "no initialized subscription, no pull"
    );
    c.apply_page(page("a", 0, 1, Some("A"))).unwrap();
    assert_eq!(
        (state(&mut c, "a").starting_cursor, c.cursor("a").unwrap()),
        (None, None),
        "a page cannot initialize a subscription"
    );
    assert!(c.read(&key()).unwrap().is_none());

    subscribe(&mut c, "b");
    c.apply_page(page("b", 0, 1, Some("B"))).unwrap();
    let request = PullRequest::decode(c.downlink_request().unwrap().unwrap().as_bytes()).unwrap();
    assert_eq!(
        request.cursors,
        std::collections::BTreeMap::from([("b".to_string(), 1)]),
        "only initialized subscriptions ask for changes"
    );
}

/// Cursor advancement updates an existing row and never inserts: a page for a
/// Scope this client unsubscribed cannot resurrect it.
#[test]
fn cursor_advancement_cannot_resurrect_a_removed_subscription() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    c.transaction(|tx| tx.set_scope("a".into(), false)).unwrap();
    c.apply_page(page("a", 0, 1, Some("A"))).unwrap();
    assert!(c.subscription_state("a").unwrap().is_none());
    assert_eq!(table_count(&mut c, "axton_subscription"), 0);
}

/// The allocator obeys the shared safe-integer bound instead of handing out an
/// identity, or storing a counter, no host can represent.
#[test]
fn an_exhausted_allocator_refuses_a_new_subscription() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open(&path);
    drop(c);
    let last = MAX_SAFE_INTEGER - 1;
    sql(
        &path,
        &format!("UPDATE axton_client SET next_subscription = {last}"),
    )
    .unwrap();
    c = open(&path);
    assert_eq!(c.ensure_subscription("a").unwrap().subscription_id, last);
    let error = c.ensure_subscription("b").unwrap_err().to_string();
    assert!(error.contains("exhausted"), "{error}");
    assert!(
        c.subscription_state("b").unwrap().is_none(),
        "the refusal leaves no half-registered row"
    );
    drop(c);
    sql(
        &path,
        &format!("UPDATE axton_client SET next_subscription = {MAX_SAFE_INTEGER}"),
    )
    .unwrap();
    c = open(&path);
    assert!(
        c.ensure_subscription("b").is_err(),
        "the last counter value"
    );
}

/// The stored contract, not the writing code, rejects a half-initialized
/// cursor pair; both fields move together or neither does.
#[test]
fn the_table_refuses_a_half_initialized_cursor_pair() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let c = open(&path);
    drop(c);
    for (starting, cursor) in [("0", "NULL"), ("NULL", "0")] {
        let error = sql(
            &path,
            &format!(
                "INSERT INTO axton_subscription (scope, subscription_id, starting_cursor, cursor) VALUES ('a', 1, {starting}, {cursor})"
            ),
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("CHECK"), "{error}");
    }
    sql(
        &path,
        "INSERT INTO axton_subscription (scope, subscription_id, starting_cursor, cursor) VALUES ('a', 1, 4, 4)",
    )
    .unwrap();
    let error = sql(
        &path,
        "UPDATE axton_subscription SET cursor = 3 WHERE scope = 'a'",
    )
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("CHECK"),
        "a cursor below its origin: {error}"
    );
    let error = sql(
        &path,
        "INSERT INTO axton_subscription (scope, subscription_id, starting_cursor, cursor) VALUES ('b', 1, 0, 0)",
    )
    .unwrap_err()
    .to_string();
    assert!(error.contains("UNIQUE"), "one identity per row: {error}");
}

/// A subscription's identity is stored, serializable state the host can carry;
/// an uninitialized boundary travels as null, never as zero.
#[test]
fn subscription_state_is_serializable() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    let registered = c.ensure_subscription("waiting").unwrap();
    assert_eq!(
        serde_json::to_value(&registered).unwrap(),
        json!({"scope":"waiting","subscriptionId":registered.subscription_id,"startingCursor":null,"cursor":null})
    );
    subscribe(&mut c, "a");
    let state = state(&mut c, "a");
    assert_eq!(
        serde_json::to_value(&state).unwrap(),
        json!({"scope":"a","subscriptionId":state.subscription_id,"startingCursor":0,"cursor":0})
    );
}
