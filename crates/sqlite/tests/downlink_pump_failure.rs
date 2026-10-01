//! A store fault after a worker decision must not discard its host action.
mod common;
use axton_client::runtime::{ClientRuntime, Input};
use axton_client::*;
use axton_core::invalid;
use axton_sqlite::SqliteStore;
use common::*;
use serde_json::{Value, json};
use std::cell::RefCell;
use std::rc::Rc;

#[derive(Clone, Default)]
struct Fault(Rc<RefCell<Option<(&'static str, usize)>>>);
impl Fault {
    fn arm(&self, sql: &'static str) {
        self.arm_after(sql, 0);
    }
    fn arm_after(&self, sql: &'static str, matching_reads: usize) {
        *self.0.borrow_mut() = Some((sql, matching_reads));
    }
    fn armed(&self) -> bool {
        self.0.borrow().is_some()
    }
    fn disarm(&self) {
        self.0.borrow_mut().take();
    }
    fn check(&self, sql: &str) -> Result<()> {
        let mut fault = self.0.borrow_mut();
        if let Some((pattern, remaining)) = fault.as_mut()
            && sql.contains(*pattern)
        {
            if *remaining == 0 {
                fault.take();
                return Err(invalid("injected query failure"));
            }
            *remaining -= 1;
        }
        Ok(())
    }
}
struct FailingQuery {
    inner: SqliteStore,
    fault: Fault,
}
impl ClientStore for FailingQuery {
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
    fn execute(&mut self, sql: &str, parameters: &[Value]) -> Result<usize> {
        self.inner.execute(sql, parameters)
    }
    fn execute_batch(&mut self, sql: &str) -> Result<()> {
        self.inner.execute_batch(sql)
    }
    fn query(&mut self, sql: &str, parameters: &[Value]) -> Result<SqlRows> {
        self.fault.check(sql)?;
        self.inner.query(sql, parameters)
    }
    fn query_committed(&mut self, sql: &str, parameters: &[Value]) -> Result<SqlRows> {
        self.fault.check(sql)?;
        self.inner.query_committed(sql, parameters)
    }
}

fn reopen(path: &std::path::Path) -> (Client<FailingQuery>, Fault, DownlinkWorker) {
    let fault = Fault::default();
    let client = Client::open(
        FailingQuery {
            inner: SqliteStore::open(path).unwrap(),
            fault: fault.clone(),
        },
        schema(),
    )
    .unwrap();
    (client, fault, DownlinkWorker::default())
}

#[test]
fn a_selected_historical_request_is_delivered_after_a_later_read_fails() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut setup = open(&path);
    setup
        .transaction(|tx| tx.set_scope("a".into(), true))
        .unwrap();
    acknowledge(&mut setup, &[("a", 100)]);
    let id = setup
        .subscription_state("a")
        .unwrap()
        .unwrap()
        .subscription_id;
    setup.request_bootstrap("a", id).unwrap();
    drop(setup);

    let (mut client, fault, mut worker) = reopen(&path);
    worker
        .handle(&mut client, DownlinkEvent::Start, 1000, 500)
        .unwrap();
    // The scheduler selected the historical request; opening the socket then
    // reads the plain subscription list and trips this one-shot fault.
    fault.arm("FROM axton_subscription ORDER BY scope");
    assert!(
        worker
            .handle(&mut client, DownlinkEvent::Next, 1000, 500)
            .is_err()
    );
    let actions = worker
        .handle(&mut client, DownlinkEvent::Next, 1000, 500)
        .unwrap();
    let historical: Vec<_> = actions
        .iter()
        .filter_map(|a| match a {
            DownlinkAction::Request {
                request,
                bootstrap: true,
                ..
            } => Some(*request),
            _ => None,
        })
        .collect();
    assert_eq!(
        historical.len(),
        1,
        "the retained slot must have a host request: {actions:?}"
    );
    assert!(worker.handle(&mut client, DownlinkEvent::Next, 1000, 500).unwrap().iter().all(|a| {
        !matches!(a, DownlinkAction::Request { request, bootstrap: true, .. } if *request == historical[0])
    }));
    // The failed begin must also release the driver's attempt, so its socket
    // can open on a bounded retry without any new event from the host.
    let mut opened_later = false;
    for now in [1000, 2000, 4000] {
        let actions = worker
            .handle(&mut client, DownlinkEvent::Next, now, 500)
            .unwrap();
        opened_later |= actions
            .iter()
            .any(|a| matches!(a, DownlinkAction::Open { .. }));
    }
    assert!(
        opened_later,
        "the socket attempt must not remain in flight forever"
    );
}

#[test]
fn pause_or_stop_discards_an_unsent_historical_request_and_restart_selects_a_new_one() {
    for pause in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut setup = open(&path);
        setup
            .transaction(|tx| tx.set_scope("a".into(), true))
            .unwrap();
        acknowledge(&mut setup, &[("a", 100)]);
        let id = setup
            .subscription_state("a")
            .unwrap()
            .unwrap()
            .subscription_id;
        setup.request_bootstrap("a", id).unwrap();
        drop(setup);
        let (mut client, fault, mut worker) = reopen(&path);
        worker
            .handle(&mut client, DownlinkEvent::Start, 1000, 500)
            .unwrap();
        fault.arm("FROM axton_subscription ORDER BY scope");
        assert!(
            worker
                .handle(&mut client, DownlinkEvent::Next, 1000, 500)
                .is_err()
        );
        worker
            .handle(
                &mut client,
                if pause {
                    DownlinkEvent::Pause
                } else {
                    DownlinkEvent::Stop
                },
                1000,
                500,
            )
            .unwrap();
        let stopped = worker
            .handle(&mut client, DownlinkEvent::Next, 1000, 500)
            .unwrap();
        assert!(
            !stopped.iter().any(|a| matches!(
                a,
                DownlinkAction::Request {
                    bootstrap: true,
                    ..
                }
            )),
            "the undelivered request is abandoned by {}: {stopped:?}",
            if pause { "pause" } else { "stop" }
        );
        worker
            .handle(
                &mut client,
                if pause {
                    DownlinkEvent::Resume
                } else {
                    DownlinkEvent::Start
                },
                1000,
                500,
            )
            .unwrap();
        let restarted = worker
            .handle(&mut client, DownlinkEvent::Next, 1000, 500)
            .unwrap();
        assert!(restarted.iter().any(|a| matches!(a, DownlinkAction::Request { request, bootstrap: true, .. } if *request > 1)),
            "resume/start selects a fresh correlated request: {restarted:?}");
    }
}

#[test]
fn a_committed_page_still_announces_its_change_after_a_later_read_fails() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut setup = open(&path);
    setup
        .transaction(|tx| tx.set_scope("a".into(), true))
        .unwrap();
    acknowledge(&mut setup, &[("a", 0)]);
    drop(setup);

    let (mut client, fault, mut worker) = reopen(&path);
    worker
        .handle(&mut client, DownlinkEvent::Start, 1000, 500)
        .unwrap();
    let opening = worker
        .handle(&mut client, DownlinkEvent::Next, 1000, 500)
        .unwrap();
    let (epoch, _) = opened(&opening[0]);
    worker
        .handle(
            &mut client,
            DownlinkEvent::Message {
                epoch,
                body: ack(&[("a", 0)]),
            },
            1000,
            500,
        )
        .unwrap();
    worker
        .handle(&mut client, DownlinkEvent::Next, 1000, 500)
        .unwrap();
    worker
        .handle(
            &mut client,
            DownlinkEvent::Message {
                epoch,
                body: text(&page("a", 0, 1, Some("one"))),
            },
            1000,
            500,
        )
        .unwrap();
    // The page commits before the historical scheduler scans for work.
    fault.arm("AND bootstrap_state='catching_up'");
    assert!(
        worker
            .handle(&mut client, DownlinkEvent::Next, 1000, 500)
            .is_err()
    );
    assert_eq!(client.cursor("a").unwrap(), Some(1));
    worker
        .handle(&mut client, DownlinkEvent::Stop, 1000, 500)
        .unwrap();
    let actions = worker
        .handle(&mut client, DownlinkEvent::Next, 1000, 500)
        .unwrap();
    assert_eq!(
        actions
            .iter()
            .filter(|a| matches!(a, DownlinkAction::Changed { scopes } if scopes == &["a"]))
            .count(),
        1,
        "a committed cursor must be announced once: {actions:?}"
    );
    assert_eq!(client.cursor("a").unwrap(), Some(1));
}

#[test]
fn a_rebuild_drops_an_undelivered_status_from_the_old_replica() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut setup = open(&path);
    setup
        .transaction(|tx| tx.set_scope("a".into(), true))
        .unwrap();
    acknowledge(&mut setup, &[("a", 100)]);
    let id = setup
        .subscription_state("a")
        .unwrap()
        .unwrap()
        .subscription_id;
    setup.request_bootstrap("a", id).unwrap();
    drop(setup);
    let (mut client, fault, mut worker) = reopen(&path);
    worker
        .handle(&mut client, DownlinkEvent::Start, 1000, 500)
        .unwrap();
    let first = worker
        .handle(&mut client, DownlinkEvent::Next, 1000, 500)
        .unwrap();
    let historical = first
        .iter()
        .find_map(|a| match a {
            DownlinkAction::Request {
                request,
                bootstrap: true,
                ..
            } => Some(*request),
            _ => None,
        })
        .unwrap();
    let terminal = ScopeBootstrapPage {
        scope: "a".into(),
        from: 0,
        to: 100,
        until: 100,
        head: 100,
        changes: vec![],
    };
    worker
        .handle(
            &mut client,
            DownlinkEvent::Response {
                request: historical,
                body: String::from_utf8(terminal.encode().unwrap()).unwrap(),
            },
            1000,
            500,
        )
        .unwrap();
    // A changed subscription generation ends the old socket. The historical
    // answer still commits, then the new socket's read fails.
    client
        .transaction(|tx| tx.set_scope("b".into(), true))
        .unwrap();
    fault.arm("FROM axton_subscription ORDER BY scope");
    assert!(
        worker
            .handle(&mut client, DownlinkEvent::Next, 1000, 500)
            .is_err()
    );
    assert_eq!(
        client.bootstrap_state("a", id).unwrap().state,
        BootstrapPhase::Complete
    );
    worker.reset_for_rebuild();
    let actions = worker
        .handle(&mut client, DownlinkEvent::Next, 1000, 500)
        .unwrap();
    assert_eq!(
        actions.first(),
        Some(&DownlinkAction::Reset),
        "reset must fence the old replica first: {actions:?}"
    );
    assert!(
        !actions
            .iter()
            .any(|a| matches!(a, DownlinkAction::Bootstrap(_))),
        "an old status must not satisfy a new replica's waiter: {actions:?}"
    );
}

#[test]
fn a_failed_historical_apply_keeps_its_answer_and_request_slot() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut setup = open(&path);
    setup
        .transaction(|tx| tx.set_scope("a".into(), true))
        .unwrap();
    acknowledge(&mut setup, &[("a", 100)]);
    let id = setup
        .subscription_state("a")
        .unwrap()
        .unwrap()
        .subscription_id;
    setup.request_bootstrap("a", id).unwrap();
    drop(setup);
    let (mut client, fault, mut worker) = reopen(&path);
    worker
        .handle(&mut client, DownlinkEvent::Start, 1000, 500)
        .unwrap();
    let first = worker
        .handle(&mut client, DownlinkEvent::Next, 1000, 500)
        .unwrap();
    let historical = first
        .iter()
        .find_map(|a| match a {
            DownlinkAction::Request {
                request,
                bootstrap: true,
                ..
            } => Some(*request),
            _ => None,
        })
        .unwrap();
    let terminal = ScopeBootstrapPage {
        scope: "a".into(),
        from: 0,
        to: 100,
        until: 100,
        head: 100,
        changes: vec![],
    };
    worker
        .handle(
            &mut client,
            DownlinkEvent::Response {
                request: historical,
                body: String::from_utf8(terminal.encode().unwrap()).unwrap(),
            },
            1000,
            500,
        )
        .unwrap();
    fault.arm("FROM axton_subscription WHERE scope=?");
    assert!(
        worker
            .handle(&mut client, DownlinkEvent::Next, 1000, 500)
            .is_err()
    );
    let actions = worker
        .handle(&mut client, DownlinkEvent::Next, 1000, 500)
        .unwrap();
    assert_eq!(
        client.bootstrap_state("a", id).unwrap().state,
        BootstrapPhase::Complete,
        "the answer still applies after the transient store error: {actions:?}"
    );
    assert_eq!(actions.iter().filter(|a| matches!(a, DownlinkAction::Bootstrap(state) if state.state == BootstrapPhase::Complete)).count(), 1);
}

#[test]
fn a_catch_up_commit_reaches_a_barrier_even_when_the_next_pull_fails() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut setup = open(&path);
    setup
        .transaction(|tx| tx.set_scope("a".into(), true))
        .unwrap();
    acknowledge(&mut setup, &[("a", 100)]);
    let id = setup
        .subscription_state("a")
        .unwrap()
        .unwrap()
        .subscription_id;
    let run = setup.request_bootstrap("a", id).unwrap().run;
    setup
        .apply_scope_bootstrap_page(
            "a",
            id,
            run,
            0,
            &ScopeBootstrapPage {
                scope: "a".into(),
                from: 0,
                to: 100,
                until: 100,
                head: 101,
                changes: vec![],
            },
        )
        .unwrap();
    drop(setup);
    let (mut client, fault, mut worker) = reopen(&path);
    worker
        .handle(&mut client, DownlinkEvent::Start, 1000, 500)
        .unwrap();
    let opening = worker
        .handle(&mut client, DownlinkEvent::Next, 1000, 500)
        .unwrap();
    let (epoch, _) = opened(&opening[0]);
    worker
        .handle(
            &mut client,
            DownlinkEvent::Message {
                epoch,
                body: ack(&[("a", 102)]),
            },
            1000,
            500,
        )
        .unwrap();
    let actions = worker
        .handle(&mut client, DownlinkEvent::Next, 1000, 500)
        .unwrap();
    let (request, _) = request(
        actions
            .iter()
            .find(|a| {
                matches!(
                    a,
                    DownlinkAction::Request {
                        bootstrap: false,
                        ..
                    }
                )
            })
            .unwrap(),
    );
    worker
        .handle(
            &mut client,
            DownlinkEvent::Response {
                request,
                body: text(&multi(
                    &[("a", 100, 101, 102)],
                    vec![authority(Some("one"), 101)],
                )),
            },
            1000,
            500,
        )
        .unwrap();
    fault.arm_after("FROM axton_subscription ORDER BY scope", 1);
    assert!(
        worker
            .handle(&mut client, DownlinkEvent::Next, 1000, 500)
            .is_err()
    );
    assert_eq!(client.cursor("a").unwrap(), Some(101));
    let mut actions = worker
        .handle(&mut client, DownlinkEvent::Next, 1000, 500)
        .unwrap();
    actions.extend(
        worker
            .handle(&mut client, DownlinkEvent::Next, 1000, 500)
            .unwrap(),
    );
    assert_eq!(
        client.bootstrap_state("a", id).unwrap().state,
        BootstrapPhase::Complete,
        "the committed catch-up reaches its barrier despite the next pull error: {actions:?}"
    );
    assert_eq!(actions.iter().filter(|a| matches!(a, DownlinkAction::Bootstrap(state) if state.state == BootstrapPhase::Complete)).count(), 1);
    assert!(
        actions.iter().any(|a| matches!(
            a,
            DownlinkAction::Request {
                bootstrap: false,
                ..
            }
        )),
        "the continuation still asks for the remaining page: {actions:?}"
    );
}

#[test]
fn a_committed_refusal_announces_failed_without_a_post_commit_read() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut setup = open(&path);
    setup
        .transaction(|tx| tx.set_scope("a".into(), true))
        .unwrap();
    acknowledge(&mut setup, &[("a", 100)]);
    let id = setup
        .subscription_state("a")
        .unwrap()
        .unwrap()
        .subscription_id;
    setup.request_bootstrap("a", id).unwrap();
    drop(setup);
    let (mut client, fault, mut worker) = reopen(&path);
    worker
        .handle(&mut client, DownlinkEvent::Start, 1000, 500)
        .unwrap();
    let first = worker
        .handle(&mut client, DownlinkEvent::Next, 1000, 500)
        .unwrap();
    let historical = first
        .iter()
        .find_map(|a| match a {
            DownlinkAction::Request {
                request,
                bootstrap: true,
                ..
            } => Some(*request),
            _ => None,
        })
        .unwrap();
    worker
        .handle(
            &mut client,
            DownlinkEvent::Failed {
                request: historical,
                status: Some(403),
                reason: Some("refused".into()),
            },
            1000,
            500,
        )
        .unwrap();
    fault.arm_after("bootstrap_state, bootstrap_run, bootstrap_cursor", 1);
    let actions = worker
        .handle(&mut client, DownlinkEvent::Next, 1000, 500)
        .unwrap();
    assert!(
        fault.armed(),
        "the status came from the guarded write, with no post-commit read"
    );
    fault.disarm();
    assert_eq!(
        client.bootstrap_state("a", id).unwrap().state,
        BootstrapPhase::Failed,
        "the refusal committed and announced the same state"
    );
    assert_eq!(actions.iter().filter(|a| matches!(a, DownlinkAction::Bootstrap(state) if state.state == BootstrapPhase::Failed)).count(), 1,
        "the stored terminal state must still reach the waiter: {actions:?}");
}

#[test]
fn a_subscription_change_fences_undelivered_session_actions_before_outbox_drain() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut setup = open(&path);
    setup
        .transaction(|tx| tx.set_scope("a".into(), true))
        .unwrap();
    acknowledge(&mut setup, &[("a", 0)]);
    drop(setup);
    let (mut client, fault, mut worker) = reopen(&path);
    worker
        .handle(&mut client, DownlinkEvent::Start, 1000, 500)
        .unwrap();
    let opening = worker
        .handle(&mut client, DownlinkEvent::Next, 1000, 500)
        .unwrap();
    let (epoch, _) = opened(&opening[0]);
    worker
        .handle(
            &mut client,
            DownlinkEvent::Message {
                epoch,
                body: ack(&[("a", 1)]),
            },
            1000,
            500,
        )
        .unwrap();
    worker
        .handle(&mut client, DownlinkEvent::Wake, 1000, 500)
        .unwrap();
    fault.arm("bootstrap_state IN ('requested','loading','catching_up')");
    assert!(
        worker
            .handle(&mut client, DownlinkEvent::Next, 1000, 500)
            .is_err()
    );
    client
        .transaction(|tx| tx.set_scope("b".into(), true))
        .unwrap();
    worker
        .handle(&mut client, DownlinkEvent::Wake, 1000, 500)
        .unwrap();
    let retained = worker
        .handle(&mut client, DownlinkEvent::Next, 1000, 500)
        .unwrap();
    assert!(
        retained
            .iter()
            .any(|a| matches!(a, DownlinkAction::Close { epoch: e, .. } if *e == epoch)),
        "the stale socket is closed: {retained:?}"
    );
    assert!(
        !retained.iter().any(|a| matches!(
            a,
            DownlinkAction::Request {
                bootstrap: false,
                ..
            } | DownlinkAction::Acknowledged { .. }
        )),
        "obsolete session actions must not reach the host: {retained:?}"
    );
    let next = worker
        .handle(&mut client, DownlinkEvent::Next, 1000, 500)
        .unwrap();
    let (_, subscribed) = opened(
        next.iter()
            .find(|a| matches!(a, DownlinkAction::Open { .. }))
            .unwrap(),
    );
    assert_eq!(subscribed.scopes, ["a", "b"]);
}

#[test]
fn a_barrier_scan_failure_after_delivery_commit_is_retried_without_another_event() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut setup = open(&path);
    setup
        .transaction(|tx| tx.set_scope("a".into(), true))
        .unwrap();
    acknowledge(&mut setup, &[("a", 100)]);
    let id = setup
        .subscription_state("a")
        .unwrap()
        .unwrap()
        .subscription_id;
    let run = setup.request_bootstrap("a", id).unwrap().run;
    let terminal = ScopeBootstrapPage {
        scope: "a".into(),
        from: 0,
        to: 100,
        until: 100,
        head: 101,
        changes: vec![],
    };
    setup
        .apply_scope_bootstrap_page("a", id, run, 0, &terminal)
        .unwrap();
    assert_eq!(
        setup.bootstrap_state("a", id).unwrap().state,
        BootstrapPhase::CatchingUp
    );
    drop(setup);

    let (mut client, fault, mut worker) = reopen(&path);
    worker
        .handle(&mut client, DownlinkEvent::Start, 1000, 500)
        .unwrap();
    let opening = worker
        .handle(&mut client, DownlinkEvent::Next, 1000, 500)
        .unwrap();
    let (epoch, _) = opened(&opening[0]);
    worker
        .handle(
            &mut client,
            DownlinkEvent::Message {
                epoch,
                body: ack(&[("a", 100)]),
            },
            1000,
            500,
        )
        .unwrap();
    worker
        .handle(&mut client, DownlinkEvent::Next, 1000, 500)
        .unwrap();
    worker
        .handle(
            &mut client,
            DownlinkEvent::Message {
                epoch,
                body: text(&page("a", 100, 101, Some("caught up"))),
            },
            1000,
            500,
        )
        .unwrap();
    fault.arm("AND bootstrap_state='catching_up'");
    assert!(
        worker
            .handle(&mut client, DownlinkEvent::Next, 1000, 500)
            .is_err()
    );
    assert_eq!(client.cursor("a").unwrap(), Some(101));
    let mut actions = worker
        .handle(&mut client, DownlinkEvent::Next, 1000, 500)
        .unwrap();
    actions.extend(
        worker
            .handle(&mut client, DownlinkEvent::Next, 1000, 500)
            .unwrap(),
    );
    assert_eq!(actions.iter().filter(|a| matches!(a, DownlinkAction::Bootstrap(state) if state.state == BootstrapPhase::Complete)).count(), 1,
        "the committed delivery still settles the barrier without another frame: {actions:?}");
    assert_eq!(
        client.bootstrap_state("a", id).unwrap().state,
        BootstrapPhase::Complete
    );
}

#[test]
fn the_runtime_retries_a_failed_pump_without_an_external_wake() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut setup = open(&path);
    setup
        .transaction(|tx| tx.set_scope("a".into(), true))
        .unwrap();
    acknowledge(&mut setup, &[("a", 100)]);
    let id = setup
        .subscription_state("a")
        .unwrap()
        .unwrap()
        .subscription_id;
    setup.request_bootstrap("a", id).unwrap();
    drop(setup);
    let (client, fault, _) = reopen(&path);
    let mut runtime = ClientRuntime::new(client);
    let mut now = 1000;
    let connect: Input = serde_json::from_value(json!({
        "type":"task", "requestId":"connect", "command":{"kind":"connect"}
    }))
    .unwrap();
    runtime.receive(connect, now, 500).unwrap();
    assert!(runtime.step(now, 500));
    runtime.take_events();
    fault.arm("FROM axton_subscription ORDER BY scope");
    let mut events = vec![];
    for _ in 0..20 {
        if !runtime.step(now, 500) {
            break;
        }
        events.extend(
            runtime
                .take_events()
                .into_iter()
                .map(|e| serde_json::to_value(e).unwrap()),
        );
    }
    assert!(
        events.iter().any(
            |e| e["type"] == "report" && e["diagnostic"]["message"] == "injected query failure"
        ),
        "the injected error was reported: {events:?}"
    );
    assert!(!events.iter().any(|e| e["operation"]["kind"] == "socket"));
    let mut saw_request = false;
    let mut saw_socket = false;
    for _ in 0..5 {
        let timer = events
            .iter()
            .rev()
            .find(|e| e["type"] == "effect" && e["operation"]["kind"] == "timer")
            .expect("a failed pump schedules a bounded retry timer");
        now += timer["operation"]["millis"].as_u64().unwrap();
        let fired: Input = serde_json::from_value(json!({
            "type":"effectResult", "effectId":timer["effectId"],
            "outcome":{"ok":true,"value":null}
        }))
        .unwrap();
        runtime.receive(fired, now, 500).unwrap();
        events.clear();
        for _ in 0..20 {
            if !runtime.step(now, 500) {
                break;
            }
            events.extend(
                runtime
                    .take_events()
                    .into_iter()
                    .map(|e| serde_json::to_value(e).unwrap()),
            );
        }
        saw_request |= events
            .iter()
            .any(|e| e["operation"]["kind"] == "http" && e["operation"]["route"] == "pull");
        saw_socket |= events.iter().any(|e| e["operation"]["kind"] == "socket");
        if saw_request && saw_socket {
            break;
        }
    }
    assert!(
        saw_request && saw_socket,
        "both retained request and socket retry must reach the host"
    );
}

#[test]
fn a_failed_acknowledgement_continues_its_catch_up_request() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut setup = open(&path);
    setup
        .transaction(|tx| tx.set_scope("a".into(), true))
        .unwrap();
    acknowledge(&mut setup, &[("a", 0)]);
    drop(setup);
    let (mut client, fault, mut worker) = reopen(&path);
    worker
        .handle(&mut client, DownlinkEvent::Start, 1000, 500)
        .unwrap();
    let opening = worker
        .handle(&mut client, DownlinkEvent::Next, 1000, 500)
        .unwrap();
    let (epoch, _) = opened(&opening[0]);
    worker
        .handle(
            &mut client,
            DownlinkEvent::Message {
                epoch,
                body: ack(&[("a", 1)]),
            },
            1000,
            500,
        )
        .unwrap();
    fault.arm("FROM axton_subscription ORDER BY scope");
    assert!(
        worker
            .handle(&mut client, DownlinkEvent::Next, 1000, 500)
            .is_err()
    );
    let actions = worker
        .handle(&mut client, DownlinkEvent::Next, 1000, 500)
        .unwrap();
    assert!(
        actions.iter().any(|a| matches!(
            a,
            DownlinkAction::Request {
                bootstrap: false,
                ..
            }
        )),
        "the consumed acknowledgement still starts catch-up: {actions:?}"
    );
}

#[test]
fn a_failed_catch_up_page_keeps_its_answer_and_request_slot() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut setup = open(&path);
    setup
        .transaction(|tx| tx.set_scope("a".into(), true))
        .unwrap();
    acknowledge(&mut setup, &[("a", 0)]);
    drop(setup);
    let (mut client, fault, mut worker) = reopen(&path);
    worker
        .handle(&mut client, DownlinkEvent::Start, 1000, 500)
        .unwrap();
    let opening = worker
        .handle(&mut client, DownlinkEvent::Next, 1000, 500)
        .unwrap();
    let (epoch, _) = opened(&opening[0]);
    worker
        .handle(
            &mut client,
            DownlinkEvent::Message {
                epoch,
                body: ack(&[("a", 1)]),
            },
            1000,
            500,
        )
        .unwrap();
    let actions = worker
        .handle(&mut client, DownlinkEvent::Next, 1000, 500)
        .unwrap();
    let (request, _) = request(
        actions
            .iter()
            .find(|a| {
                matches!(
                    a,
                    DownlinkAction::Request {
                        bootstrap: false,
                        ..
                    }
                )
            })
            .unwrap(),
    );
    worker
        .handle(
            &mut client,
            DownlinkEvent::Response {
                request,
                body: text(&page("a", 0, 1, Some("one"))),
            },
            1000,
            500,
        )
        .unwrap();
    fault.arm("FROM axton_subscription WHERE scope=?");
    assert!(
        worker
            .handle(&mut client, DownlinkEvent::Next, 1000, 500)
            .is_err()
    );
    let actions = worker
        .handle(&mut client, DownlinkEvent::Next, 1000, 500)
        .unwrap();
    assert_eq!(
        client.cursor("a").unwrap(),
        Some(1),
        "the answer must still apply: {actions:?}"
    );
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, DownlinkAction::Changed { scopes } if scopes == &["a"])),
        "the retried answer announces its commit: {actions:?}"
    );
}
