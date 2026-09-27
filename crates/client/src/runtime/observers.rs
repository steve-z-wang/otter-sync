//! Observers: subscription status, Bootstrap waiters and local watches
//! ([#134](https://github.com/zanminwang/axton/issues/134),
//! [#150](https://github.com/zanminwang/axton/issues/150),
//! [#151](https://github.com/zanminwang/axton/issues/151)).
//!
//! The runtime publishes each observer's state as an
//! [`Event::ObserverChanged`] snapshot, emitted only when it differs from the
//! last one emitted for that observer. Snapshots are published at the end of
//! the unit that changed them - after its commit and its task completions -
//! and, for transport facts, right after the effect result that reported
//! them.
//!
//! **Subscription status.** One observer per persistent subscription
//! identity, registered by `scopeSubscribe`. Its `connection` is projected
//! from the runtime-owned connection: no connection, or a paused one, is
//! `offline`; no open session is `connecting`; a catch-up request out is
//! `catching-up`; a session whose handshake covered the Scope is `live`,
//! otherwise `connecting`. The handshake's coverage belongs to the session
//! that acknowledged it and to the registration it covered: a removal forgets
//! its Scope, so a registration created after it is `connecting` until a
//! session of its own acknowledges it. `initialization` is `ready` once the
//! stored starting boundary exists, re-read whenever the Downlink worker
//! commits for the Scope. `bootstrap` is the last committed run of the
//! identity, which never moves backwards within a run; a `requested` run with
//! no starting boundary is `waiting-for-initialization`.
//!
//! **Bootstrap waiters.** `scopeBootstrap` registers (or explicitly retries)
//! the durable load and parks its task on the run the registration answered
//! with. The task completes with `null` when a committed transition of that
//! run is `complete`, fails with the run's stored `{code, message}` when it is
//! `failed`, and fails `bootstrap.superseded` once a later run of the same
//! identity is observed: a waiter never resolves from another run's outcome.
//! A removal or a rebuild fails the identity's waiters `subscription.closed`,
//! the runtime's close fails them `client_closed`; waiting for connectivity is
//! not failure and has no timeout.
//!
//! **Watches.** `watch {model, spec}` runs the query on the committed reader,
//! answers its observer id and publishes the rows; after every commit every
//! watch re-runs - a coarse rule, with no query dependency tracking - and
//! publishes only a result that differs from the last one it published. A re-run that fails is reported
//! and the watch stays. A callback transaction's writes are invisible to a
//! watch until they commit: nothing re-runs while the session is open.
use super::*;
use crate::{BootstrapPhase, BootstrapState, ClientStore};
use std::collections::BTreeSet;

const SUPERSEDED: &str = "bootstrap.superseded";
const CLIENT_CLOSED: &str = "client_closed";

#[derive(Default)]
pub(super) struct Observers {
    /// A commit landed since the watches last ran.
    pub(super) stale: bool,
    /// Every subscription identity this runtime observes or waits on, by
    /// `subscriptionId`.
    registrations: BTreeMap<u64, Registration>,
    /// Watches by the number behind their observer id: registration order.
    watches: BTreeMap<u64, Watch>,
    /// The Scopes the handshake of the session of this epoch covered.
    acknowledged: Option<(u64, BTreeSet<String>)>,
}

struct Registration {
    scope: String,
    /// The observer publishing its status, once `scopeSubscribe` named it; a
    /// registration only waited on by `scopeBootstrap` publishes nothing.
    observer: Option<String>,
    /// A durable starting boundary exists.
    ready: bool,
    /// The last committed run observed.
    run: Option<BootstrapState>,
    waiters: Vec<Waiter>,
    /// The status last published.
    published: Option<Value>,
}

/// One parked `scopeBootstrap` task, attached to the run it registered.
struct Waiter {
    run: u64,
    request_id: String,
}

struct Watch {
    model: String,
    filter: Value,
    rows: Value,
    /// `rows` were published.
    published: bool,
}

/// How far a run has got: a transition never moves the status backwards.
fn rank(phase: BootstrapPhase) -> u8 {
    match phase {
        BootstrapPhase::NotRequested => 0,
        BootstrapPhase::Requested => 1,
        BootstrapPhase::Loading => 2,
        BootstrapPhase::CatchingUp => 3,
        BootstrapPhase::Complete | BootstrapPhase::Failed => 4,
    }
}

impl Registration {
    fn new(scope: String) -> Self {
        Self {
            scope,
            observer: None,
            ready: false,
            run: None,
            waiters: vec![],
            published: None,
        }
    }
    /// The public phase of the stored run and its failure, without the
    /// stored record summaries.
    fn bootstrap(&self) -> Value {
        let Some(run) = &self.run else {
            return json!({"phase": "not-requested", "error": null});
        };
        let phase = match run.state {
            BootstrapPhase::NotRequested => "not-requested",
            BootstrapPhase::Requested if !self.ready => "waiting-for-initialization",
            BootstrapPhase::Requested | BootstrapPhase::Loading => "loading",
            BootstrapPhase::CatchingUp => "catching-up",
            BootstrapPhase::Complete => "complete",
            BootstrapPhase::Failed => "failed",
        };
        let error = run.error.as_ref().map_or(
            Value::Null,
            |e| json!({"code": e.code, "message": e.message}),
        );
        json!({"phase": phase, "error": error})
    }
    fn status(&self, connection: &str) -> Value {
        json!({
            "active": connection != "stopped",
            "initialization": if self.ready { "ready" } else { "pending" },
            "connection": connection,
            "bootstrap": self.bootstrap(),
        })
    }
    fn snapshot(&self, id: u64, status: Value) -> Value {
        json!({"kind": "subscription", "scope": self.scope, "subscriptionId": id, "status": status})
    }
}

impl<S: ClientStore + 'static> ClientRuntime<S> {
    // --- Subscriptions -----------------------------------------------------

    /// `scopeSubscribe {scope}`: register durable intent, and observe the
    /// identity the commit answered with. Repeated calls for one identity
    /// answer the same observer.
    pub(super) fn subscribe_scope(&mut self, scope: &str) -> std::result::Result<Value, String> {
        let state = self
            .client
            .ensure_subscription(scope)
            .map_err(|e| e.to_string())?;
        let id = state.subscription_id;
        let known = self.observers.registrations.contains_key(&id);
        let observer = match self
            .observers
            .registrations
            .get(&id)
            .and_then(|r| r.observer.clone())
        {
            Some(observer) => observer,
            None => self.issue()?.to_string(),
        };
        let registration = self
            .observers
            .registrations
            .entry(id)
            .or_insert_with(|| Registration::new(state.scope.clone()));
        registration.observer = Some(observer.clone());
        registration.ready = state.starting_cursor.is_some();
        if !known {
            // A load of this identity may already be running or finished from
            // before this runtime: its status needs no new transition.
            match self.client.bootstrap_state(&state.scope, id) {
                Ok(run) => self.observe_run(run),
                Err(e) => self.error(e.to_string()),
            }
        }
        Ok(json!({"state": state, "observerId": observer}))
    }

    /// `scopeBootstrap {scope, subscriptionId}`: register the load, or
    /// explicitly retry a failed one, and wait for the run it answered with.
    /// `None` while the task waits; a completed run answers at once.
    pub(super) fn bootstrap_scope(
        &mut self,
        request_id: &str,
        command: &Command,
    ) -> Option<std::result::Result<Value, String>> {
        let generation = self.client.generation();
        let answered = commands::execute(&mut self.client, command)
            .and_then(|value| Ok(serde_json::from_value::<BootstrapState>(value)?));
        self.committed_since(generation);
        let state = match answered {
            Ok(state) => state,
            Err(e) => return Some(Err(e.to_string())),
        };
        let (scope, id, run) = (state.scope.clone(), state.subscription_id, state.run);
        self.observers
            .registrations
            .entry(id)
            .or_insert_with(|| Registration::new(scope.clone()));
        if state.state == BootstrapPhase::Complete {
            self.observe_run(state);
            return Some(Ok(Value::Null));
        }
        if let Some(registration) = self.observers.registrations.get_mut(&id) {
            registration.waiters.push(Waiter {
                run,
                request_id: request_id.to_string(),
            });
        }
        self.observe_run(state);
        // Whatever committed for this identity since the registration's own
        // answer settles the waiter now rather than never.
        match self.client.bootstrap_state(&scope, id) {
            Ok(stored) => self.observe_run(stored),
            Err(e) => self.error(e.to_string()),
        }
        None
    }

    /// One committed state of an identity's load: the waiters of that run
    /// settle whatever the status already shows, older runs' waiters are
    /// superseded, and the status never moves backwards.
    pub(super) fn observe_run(&mut self, state: BootstrapState) {
        let Some(registration) = self.observers.registrations.get_mut(&state.subscription_id)
        else {
            return;
        };
        if registration.scope != state.scope {
            return;
        }
        let outcome = match state.state {
            BootstrapPhase::Complete => Some(Ok(())),
            BootstrapPhase::Failed => Some(Err(match &state.error {
                // A failed run always carries its stored failure; the ledger
                // refuses any other pairing.
                Some(error) => (error.code.clone(), error.message.clone()),
                None => (
                    "bootstrap.failed".to_string(),
                    format!("the bootstrap of {} failed", state.scope),
                ),
            })),
            _ => None,
        };
        let mut settled = vec![];
        let mut superseded = vec![];
        registration.waiters.retain(|waiter| {
            if waiter.run < state.run {
                superseded.push(waiter.request_id.clone());
                false
            } else if waiter.run == state.run && outcome.is_some() {
                settled.push(waiter.request_id.clone());
                false
            } else {
                true
            }
        });
        let newer = registration.run.as_ref().is_none_or(|known| {
            state.run > known.run
                || (state.run == known.run && rank(state.state) >= rank(known.state))
        });
        if newer {
            registration.run = Some(state);
        }
        for request_id in settled {
            match &outcome {
                Some(Ok(())) => self.complete(request_id, Ok(Value::Null)),
                Some(Err((code, message))) => self.fail(
                    request_id,
                    message.clone(),
                    json!({"code": code, "message": message}),
                ),
                None => {}
            }
        }
        for request_id in superseded {
            self.fail(request_id, SUPERSEDED, json!({"code": SUPERSEDED}));
        }
    }

    /// The Downlink worker committed for these Scopes: re-read their stored
    /// starting boundary.
    pub(super) fn scopes_changed(&mut self, scopes: &[String]) {
        for scope in scopes {
            if !self
                .observers
                .registrations
                .values()
                .any(|r| &r.scope == scope)
            {
                continue;
            }
            match self.client.subscription_state(scope) {
                Ok(Some(state)) => {
                    if let Some(registration) = self
                        .observers
                        .registrations
                        .get_mut(&state.subscription_id)
                        .filter(|r| r.scope == state.scope)
                    {
                        registration.ready = state.starting_cursor.is_some();
                    }
                }
                Ok(None) => {}
                Err(e) => self.error(e.to_string()),
            }
        }
    }

    /// The handshake of the open session covered these Scopes.
    pub(super) fn acknowledged(&mut self, scopes: Vec<String>) {
        let Some(epoch) = self.connection.as_ref().and_then(|c| c.session()) else {
            return;
        };
        // Coverage belongs to one session: a new one starts from nothing.
        let acknowledged = &mut self.observers.acknowledged;
        if acknowledged.as_ref().is_none_or(|(of, _)| *of != epoch) {
            *acknowledged = Some((epoch, BTreeSet::new()));
        }
        if let Some((_, covered)) = acknowledged {
            covered.extend(scopes);
        }
    }

    /// An ordinary command committed a removal: the identities it removed are
    /// closed and their Scopes' acknowledgement is forgotten.
    pub(super) fn removed(&mut self, command: &Command, value: &Value) {
        match command {
            Command::ScopeUnsubscribe {
                scope,
                subscription_id,
            } => {
                self.close_registration(*subscription_id, crate::SUBSCRIPTION_CLOSED);
                // Nothing went: another registration is this Scope's current
                // one, and the acknowledgement it may hold is not this one's.
                if value["removed"] == true {
                    self.forget(scope);
                }
            }
            Command::Channel {
                channel,
                subscribed: false,
            } => {
                let ids: Vec<u64> = self
                    .observers
                    .registrations
                    .iter()
                    .filter(|(_, r)| &r.scope == channel)
                    .map(|(id, _)| *id)
                    .collect();
                for id in ids {
                    self.close_registration(id, crate::SUBSCRIPTION_CLOSED);
                }
                self.forget(channel);
            }
            _ => {}
        }
    }
    fn forget(&mut self, scope: &str) {
        if let Some((_, covered)) = &mut self.observers.acknowledged {
            covered.remove(scope);
        }
    }

    /// The replica was replaced: every identity belongs to the file left
    /// behind, so every registration closes the way a removal does, and no
    /// acknowledgement belongs to an identity that still exists.
    pub(super) fn rebuilt_observers(&mut self) {
        let ids: Vec<u64> = self.observers.registrations.keys().copied().collect();
        for id in ids {
            self.close_registration(id, crate::SUBSCRIPTION_CLOSED);
        }
        self.observers.acknowledged = None;
    }

    /// Close one identity: its waiters fail with `code`, and its observer
    /// publishes its terminal snapshot.
    fn close_registration(&mut self, id: u64, code: &str) {
        let Some(registration) = self.observers.registrations.remove(&id) else {
            return;
        };
        for waiter in &registration.waiters {
            self.fail(waiter.request_id.clone(), code, json!({ "code": code }));
        }
        if let Some(observer_id) = &registration.observer {
            let mut snapshot = registration.snapshot(id, registration.status("stopped"));
            snapshot["closed"] = json!(true);
            self.events.push(Event::ObserverChanged {
                observer_id: observer_id.clone(),
                snapshot,
            });
        }
    }

    /// The `connection` of a live registration of `scope`.
    fn connection_status(&self, scope: &str) -> &'static str {
        let Some(connection) = &self.connection else {
            return "offline";
        };
        if connection.paused {
            return "offline";
        }
        let Some(epoch) = connection.session() else {
            return "connecting";
        };
        if connection.catching_up() {
            return "catching-up";
        }
        match &self.observers.acknowledged {
            Some((acknowledged, covered)) if *acknowledged == epoch && covered.contains(scope) => {
                "live"
            }
            _ => "connecting",
        }
    }

    /// Publish every subscription status that changed. Memory only: it runs
    /// after an effect result is admitted as well as after a unit.
    pub(super) fn publish_statuses(&mut self) {
        let mut changed = vec![];
        for (id, registration) in &self.observers.registrations {
            let Some(observer_id) = &registration.observer else {
                continue;
            };
            let status = registration.status(self.connection_status(&registration.scope));
            if registration.published.as_ref() != Some(&status) {
                changed.push((*id, observer_id.clone(), status));
            }
        }
        for (id, observer_id, status) in changed {
            let Some(registration) = self.observers.registrations.get_mut(&id) else {
                continue;
            };
            registration.published = Some(status.clone());
            let snapshot = registration.snapshot(id, status);
            self.events.push(Event::ObserverChanged {
                observer_id,
                snapshot,
            });
        }
    }

    /// Publish what the unit changed: re-run the watches after a commit, then
    /// every status and watch result that differs from the last published.
    pub(super) fn publish(&mut self) {
        if self.observers.stale && self.transaction.is_none() {
            self.observers.stale = false;
            let ids: Vec<u64> = self.observers.watches.keys().copied().collect();
            for id in ids {
                let Some(watch) = self.observers.watches.get(&id) else {
                    continue;
                };
                let rows = self
                    .client
                    .query(&watch.model, &watch.filter)
                    .map_err(|e| e.to_string())
                    .and_then(|rows| serde_json::to_value(rows).map_err(|e| e.to_string()));
                match rows {
                    Ok(rows) => {
                        if let Some(watch) = self.observers.watches.get_mut(&id)
                            && watch.rows != rows
                        {
                            watch.rows = rows;
                            watch.published = false;
                        }
                    }
                    Err(error) => self.error(error),
                }
            }
        }
        self.publish_statuses();
        let mut snapshots = vec![];
        for (id, watch) in &mut self.observers.watches {
            if !watch.published {
                watch.published = true;
                snapshots.push(Event::ObserverChanged {
                    observer_id: id.to_string(),
                    snapshot: json!({"kind": "watch", "rows": watch.rows}),
                });
            }
        }
        self.events.extend(snapshots);
    }

    // --- Watches -----------------------------------------------------------

    /// `watch {model, spec: {filter?}}`: run the query now and publish its
    /// rows after the task's completion.
    pub(super) fn watch(
        &mut self,
        model: &str,
        spec: Option<&WatchSpec>,
    ) -> std::result::Result<Value, String> {
        let filter = spec
            .and_then(|spec| spec.filter.clone())
            .filter(|filter| !filter.is_null())
            .unwrap_or_else(|| json!({}));
        let rows = self
            .client
            .query(model, &filter)
            .map_err(|e| e.to_string())?;
        let rows = serde_json::to_value(rows).map_err(|e| e.to_string())?;
        let id = self.issue()?;
        self.observers.watches.insert(
            id,
            Watch {
                model: model.to_string(),
                filter,
                rows,
                published: false,
            },
        );
        Ok(json!({"observerId": id.to_string()}))
    }
    /// `unwatch {observerId}`: nothing more is published for it.
    pub(super) fn unwatch(&mut self, observer_id: &str) -> std::result::Result<Value, String> {
        if let Ok(id) = observer_id.parse::<u64>() {
            self.observers.watches.remove(&id);
        }
        Ok(Value::Null)
    }

    /// Close: every waiter fails `client_closed`, every subscription stops
    /// and every watch ends, each with its terminal snapshot. The durable
    /// loads are untouched.
    pub(super) fn close_observers(&mut self) {
        for (id, registration) in std::mem::take(&mut self.observers.registrations) {
            for waiter in &registration.waiters {
                self.fail(
                    waiter.request_id.clone(),
                    CLIENT_CLOSED,
                    json!({ "code": CLIENT_CLOSED }),
                );
            }
            if let Some(observer_id) = &registration.observer {
                let mut snapshot = registration.snapshot(id, registration.status("stopped"));
                snapshot["closed"] = json!(true);
                self.events.push(Event::ObserverChanged {
                    observer_id: observer_id.clone(),
                    snapshot,
                });
            }
        }
        for (id, watch) in std::mem::take(&mut self.observers.watches) {
            self.events.push(Event::ObserverChanged {
                observer_id: id.to_string(),
                snapshot: json!({"kind": "watch", "rows": watch.rows, "closed": true}),
            });
        }
        self.observers.acknowledged = None;
    }
}
