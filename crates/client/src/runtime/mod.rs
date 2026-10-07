//! The Rust-owned client runtime: one per open client, generic over the
//! store, driven by an actor that owns no policy
//! ([#134](https://github.com/zanminwang/axton/issues/134)).
//!
//! The runtime owns the client, its local execution queue, the task
//! continuations, the one active application transaction, the connection
//! lanes and the observers. The host owns sockets, HTTP, timers, credentials,
//! language objects and application callback bodies; it learns what to do
//! from [`Event::Effect`]s and reports back with [`Input::EffectResult`]s.
//!
//! Three calls drive it, all non-blocking and all on the one thread that owns
//! the store:
//!
//! - [`ClientRuntime::receive`] admits one [`Input`]. It queues and correlates;
//!   it runs no database work, so an effect result or a close can be admitted
//!   while an application callback holds the transaction.
//! - [`ClientRuntime::step`] runs at most one local unit of work - one task,
//!   one transaction command, one commit or rollback, one pump of a lane - and
//!   says whether it did anything. The actor calls it until it answers `false`
//!   and blocks on its mailbox; the runtime never sleeps and never waits on
//!   the network inside a step, so a host timer is an effect like any other.
//! - [`ClientRuntime::take_events`] hands over what the last steps produced,
//!   in order. A task's completion is queued only after its transaction
//!   committed or rolled back, so an SDK that observes a success and reads the
//!   database sees the committed rows.
//!
//! `now` (milliseconds) and `entropy` are facts the actor supplies on every
//! call; deterministic tests pass their own.
//!
//! The observers publish what a unit changed at its end, after its task
//! completions, as [`Event::ObserverChanged`] snapshots: a watch's rows are
//! always rows the unit committed, and a status describes what is committed.
//!
//! # Scheduling and transaction ownership
//!
//! Ordinary tasks form one FIFO. The first [`Command::Transaction`] to run
//! opens the session (`begin_session`), allocates a `transactionId` and a
//! callback effect, and parks: from then on every ordinary task waits, whether
//! it reads or writes, because it must not see or join the open session. The
//! callback's own [`TransactionCommand`]s arrive as
//! [`Input::TransactionCommand`]s bearing that id and run on a continuation
//! lane ahead of the parked queue, each completing its own request. Nested
//! savepoints keep a stack of runtime-issued `scope` tokens; a command names
//! the innermost open scope or fails without joining, and a `release` /
//! `rollbackSavepoint` pops only the top. A failed command poisons the unit
//! unless the savepoint it ran in rolls back, a wrong-scope command is a
//! structural failure, and the [`Input::CallbackResult`] then commits (`ok`
//! with nothing outstanding, no unreleased savepoint and no recorded failure)
//! or rolls back and fails the parent with the first failure.
//! A transaction command that arrives after the callback result, or that names
//! a transaction that is not open, fails with `transaction_closed`.
//!
//! A `submitMutation` writes a named Mutation into the session. Its call is
//! provisional - never frozen or sent - until the commit, which announces it
//! with [`Event::TransactionCallState`] `committed` before the transaction
//! task's success; a savepoint rollback announces its own scope's calls
//! `rolledBack`, and a rollback, failed commit or close all of them. With
//! `local`, the submission waits on an [`Operation::MutationLocal`] effect: a
//! restricted capability of the same session, named by a `companionId`, whose
//! local reads and writes are the only commands admitted while it runs - its
//! writes becoming the call's companions - and whose own
//! [`Input::CallbackResult`] answers the submission without ending the
//! transaction. onStore callbacks submit no Mutation.
//!
//! [`Input::Close`] is priority control: it rolls back an open session, fails
//! its parent task and every queued task with `client_closed`, cancels every
//! outstanding effect, ends the observers and queues [`Event::RuntimeClosed`]
//! last. After it, [`ClientRuntime::receive`] answers [`BridgeError::Closed`].
//!
//! # Connection lanes, direct calls and effects
//!
//! A `connect` task records the connection intent and starts both lanes: the
//! push lane (`ConnectionDriver` + `SyncCycle`) and the Downlink worker. From
//! then on the runtime decides every request, retry, refresh, cancellation and
//! report, and the host only executes effects: an HTTP post, a socket stream,
//! a timer, a credential refresh, a prerequisite handler. An effect result is
//! a fact; [`ClientRuntime::receive`] correlates it by `effectId` (a result for
//! an id that is not outstanding is ignored: that is the fence for cancelled,
//! duplicate and stale answers) and turns it into a *ready continuation*, or
//! hands it to the Downlink worker as it arrives, without touching the
//! database: the worker's enqueue path only queues, so its bounded frame queue
//! and overflow recovery hold even while a callback keeps the writer. [`ClientRuntime::step`]
//! then runs one unit: the application transaction's own lane first, then
//! ordinary tasks and *lane units* - a ready continuation (a receipt, a direct
//! response, a prerequisite outcome), else a prerequisite turn, a Load unit,
//! a Downlink pump or a push-lane turn, the Load lane alternating with the
//! Downlink and push lanes - in the
//! order they were admitted, so neither starves. Each unit holds
//! at most one local transaction and none is held across an effect: prepare,
//! effect and apply are three units. Every ordinary task or continuation that
//! committed wakes both lanes.
//!
//! # Module layout
//!
//! - [`protocol`]: the envelopes shared with every SDK.
//! - `tasks`: the task table, the queues, request correlation, scheduling and
//!   close.
//! - `transactions`: the active transaction, its capability tokens, savepoint
//!   stack, failure accounting and the callback effect.
//! - `effects`: the effect table, result correlation, ready continuations and
//!   the one credential refresh the lanes and direct calls share.
//! - `lanes`: the connection intent, its controls, the push lane and the
//!   Downlink worker as runtime work.
//! - `direct`: direct Query/Mutation calls, Query once flights and Model
//!   Fetch flights, their deadlines and fences.
//! - `loads`: native Load commands, the Load worker's batches as effects,
//!   page application, and the Load handles' observers and waiters.
//! - `prerequisites`: the scheduler that runs the application's prerequisite
//!   handlers, registered at open, when a task becomes pending, with backoff.
//! - `observers`: subscription status, Bootstrap waiters and local watches,
//!   published as snapshots.
//! - `sql_watches`: watched read-only SQL, re-run only after a commit that
//!   writes a table it reads.
//! - `unsent`: the unsent-work observers and the resolutions a transaction
//!   announces at its commit.
//! - `commands`: the commands executed directly against the client: local
//!   reads and writes, Scope and Bootstrap registrations, the sync state and
//!   the protocol seams (`freeze`, `ack`, `pull`).
mod commands;
mod direct;
mod effects;
mod lanes;
mod loads;
mod observers;
mod prerequisites;
pub mod protocol;
mod sql_watches;
mod tasks;
mod transactions;
mod unsent;

pub use protocol::*;

use crate::{Client, ClientStore, Result, Schema, StoreFactory};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::Path;

/// One open client and everything the runtime decided about it. See the
/// module documentation for the contract of the three driving calls.
pub struct ClientRuntime<S: ClientStore> {
    client: Client<S>,
    protocol05: bool,
    sync05_live: bool,
    sync05_catching_up: bool,
    lanes: lanes::Lanes,
    /// The runtime-owned connection: its intent, lane effects and credential
    /// refresh. `None` until `connect` and after `stop`.
    connection: Option<lanes::Connection>,
    tasks: tasks::Tasks,
    transaction: Option<transactions::Transaction>,
    store_hooks: BTreeSet<String>,
    /// Every effect the host may still answer, by id, and what it was issued
    /// for. A result for an id that is not here is ignored.
    effects: BTreeMap<String, effects::EffectKind>,
    /// Effect results turned into local work, one unit each, in arrival order.
    ready: VecDeque<effects::Ready>,
    directs: direct::Directs,
    /// The prerequisite handlers registered at open and their scheduler.
    prerequisites: prerequisites::Prerequisites,
    /// The Load worker, its batches in flight, and the Load handles'
    /// observers and waiters.
    loads: loads::Loads,
    /// Subscription and watch observers, and the Bootstrap waiters.
    observers: observers::Observers,
    /// The unsent-work observers.
    unsent: unsent::Unsent,
    /// Admissions so far: ordinary tasks and effect results are numbered in
    /// arrival order, and lane work is scheduled by that order too.
    admitted: u64,
    /// The admission count when lane work was first seen ready; `None` while
    /// nothing is ready. An ordinary task admitted before it runs first, one
    /// admitted after it waits: the arrival order of foreground and inbound
    /// work is preserved, and neither starves the other.
    lane_since: Option<u64>,
    /// The one counter behind `transactionId`, `scope` and `effectId`: every
    /// identity the runtime issues is fresh for its lifetime.
    issued: u64,
    /// Capability tokens also fence other Stores and prior runtime opens.
    capability_namespace: Option<String>,
    events: Vec<Event>,
    lifecycle: Lifecycle,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Lifecycle {
    Open,
    /// Close was admitted; the next step performs it ahead of any other work.
    Closing,
    Closed,
}

impl<S: ClientStore + 'static> ClientRuntime<S> {
    /// [`Client::open_at`] under the runtime. Errors are the open errors.
    pub fn open_at(
        path: impl AsRef<Path>,
        schema: Schema,
        factory: StoreFactory<S>,
        discard_pending: bool,
    ) -> Result<Self> {
        Ok(Self::new(Client::open_at(
            path,
            schema,
            factory,
            discard_pending,
        )?))
    }
    /// Wrap an already opened client.
    pub fn new(mut client: Client<S>) -> Self {
        let capability_namespace = client
            .request_context()
            .is_ok()
            .then(|| uuid::Uuid::new_v4().to_string());
        let capability_namespace = capability_namespace.or_else(|| {
            client
                .request_context05()
                .is_ok()
                .then(|| uuid::Uuid::new_v4().to_string())
        });
        let protocol05 = client.request_context05().is_ok();
        Self {
            client,
            protocol05,
            sync05_live: false,
            sync05_catching_up: false,
            lanes: lanes::Lanes::default(),
            connection: None,
            tasks: tasks::Tasks::default(),
            transaction: None,
            store_hooks: BTreeSet::new(),
            effects: BTreeMap::new(),
            ready: VecDeque::new(),
            directs: direct::Directs::default(),
            prerequisites: prerequisites::Prerequisites::default(),
            loads: loads::Loads::default(),
            observers: observers::Observers::default(),
            unsent: unsent::Unsent::default(),
            admitted: 0,
            lane_since: None,
            issued: 0,
            capability_namespace,
            events: vec![],
            lifecycle: Lifecycle::Open,
        }
    }
    /// Register schema Models whose incoming authority requires a callback.
    /// Names are validated once and remain fixed for the runtime lifetime.
    pub fn with_store_hooks(client: Client<S>, models: Vec<String>) -> Result<Self> {
        Self::new(client).register_store_hooks(models)
    }
    /// Configure an opened runtime before any task is admitted.
    pub fn register_store_hooks(mut self, models: Vec<String>) -> Result<Self> {
        if self.client.request_context().is_ok() {
            return Err(crate::invalid(
                "custom store hooks are retired in protocol 4",
            ));
        }
        if self.admitted != 0 || self.lifecycle != Lifecycle::Open || self.transaction.is_some() {
            return Err(crate::invalid("store hooks are fixed at runtime open"));
        }
        let declared = self.client.target_store_hook_models();
        let mut hooks = BTreeSet::new();
        for model in models {
            if !declared.contains(&model) || !hooks.insert(model.clone()) {
                return Err(crate::invalid(format!("invalid store hook Model {model}")));
            }
        }
        self.store_hooks = hooks;
        Ok(self)
    }
    /// What a successful open answers: the client id and the schema check's
    /// outcome, as the SDKs report it in `status()`.
    pub fn opened(&mut self) -> Value {
        let mut opened = json!({
            "clientId": self.client.client_id(),
            "schema": commands::schema_json(self.client.schema_state()),
        });
        if let Ok(context) = self.client.request_context() {
            opened["context"] = json!(context);
        }
        if let Ok(context) = self.client.request_context05() {
            opened["context"] = json!(context);
        }
        opened
    }
    /// The events queued since the last call, in order.
    pub fn take_events(&mut self) -> Vec<Event> {
        std::mem::take(&mut self.events)
    }
    /// Whether [`Event::RuntimeClosed`] has been queued.
    pub fn closed(&self) -> bool {
        self.lifecycle == Lifecycle::Closed
    }
    /// Test seam: the inbound socket frames held and not yet applied.
    #[doc(hidden)]
    pub fn held_frames(&self) -> usize {
        self.lanes.downlink.queued_frames()
    }
    /// Test seam: the client, for inspecting committed state in Rust tests.
    pub fn client(&mut self) -> &mut Client<S> {
        &mut self.client
    }
}

impl<S: ClientStore + 'static> ClientRuntime<S> {
    /// A fresh number for an identity the runtime issues. Never reused; an
    /// exhausted counter refuses rather than wraps.
    fn capability_token(&self, prefix: &str, id: u64) -> String {
        match &self.capability_namespace {
            Some(namespace) => format!("{namespace}:{prefix}{id}"),
            None => format!("{prefix}{id}"),
        }
    }
    fn issue(&mut self) -> std::result::Result<u64, String> {
        self.issued = self
            .issued
            .checked_add(1)
            .ok_or_else(|| "runtime identifiers exhausted".to_string())?;
        Ok(self.issued)
    }
    /// Settle one routed request. An engine refusal of a registration this
    /// client no longer holds carries `{"code":"subscription.closed"}`, so no
    /// SDK has to recognize it by its message.
    fn complete(&mut self, request_id: String, outcome: std::result::Result<Value, String>) {
        match outcome {
            Ok(value) => self.settle(request_id, Ok(value), None),
            Err(error) => {
                let details = error
                    .contains(CLOSED_REGISTRATION)
                    .then(|| json!({ "code": crate::SUBSCRIPTION_CLOSED }));
                self.settle(request_id, Err(error), details)
            }
        }
    }
    /// Fail one routed request with a machine-readable reason.
    fn fail(&mut self, request_id: String, error: impl Into<String>, details: Value) {
        self.settle(request_id, Err(error.into()), Some(details));
    }
    /// The only place a [`Event::TaskCompleted`] is queued, so a request is
    /// completed at most once.
    fn settle(
        &mut self,
        request_id: String,
        outcome: std::result::Result<Value, String>,
        details: Option<Value>,
    ) {
        self.tasks.release(&request_id);
        self.events.push(match outcome {
            Ok(value) => Event::TaskCompleted {
                request_id,
                ok: true,
                value,
                error: None,
                details: None,
            },
            Err(error) => Event::TaskCompleted {
                request_id,
                ok: false,
                value: Value::Null,
                error: Some(error),
                details,
            },
        });
    }
    /// When a unit committed since `generation`, the watches re-run before
    /// the unit ends.
    fn committed_since(&mut self, generation: u64) {
        if self.client.generation() != generation {
            self.observers.stale = true;
            self.reconcile_registrations();
        }
    }
    fn report(&mut self, diagnostic: Diagnostic) {
        self.events.push(Event::Report { diagnostic });
    }
    fn error(&mut self, message: impl Into<String>) {
        self.error_status(message, None);
    }
    /// A failure the application hears about, with the HTTP status it carried.
    fn error_status(&mut self, message: impl Into<String>, status: Option<u16>) {
        self.report(Diagnostic::Error {
            message: message.into(),
            status,
        });
    }
}

/// The prefix every engine refusal of a closed registration carries
/// ([`crate::SUBSCRIPTION_CLOSED`]).
const CLOSED_REGISTRATION: &str = "subscription.closed:";

impl<S: ClientStore + 'static> ClientRuntime<S> {
    pub fn store_worker_busy05(&self) -> bool {
        self.transaction.is_some()
    }
    pub fn store_worker05(
        &mut self,
        command: crate::sync05::StoreCommand,
    ) -> Result<crate::sync05::StoreReport> {
        use crate::sync05::{StoreCommand, StoreReport};
        if let StoreCommand::Guarded { fence, command } = command {
            if !fence.current() || self.client.request_context05()? != fence.context {
                return Ok(StoreReport::Obsolete);
            }
            let report = self.store_worker05(*command).map_err(|e| e.to_string());
            return Ok(StoreReport::Guarded {
                fence,
                report: Box::new(report),
            });
        }
        if self.transaction.is_some() {
            return Err(crate::invalid("Store worker transaction active"));
        }
        let generation = self.client.generation();
        let result = match command {
            StoreCommand::Guarded { .. } => unreachable!(),
            StoreCommand::NetworkState { live, catching_up } => {
                self.sync05_live = live;
                self.sync05_catching_up = catching_up;
                self.publish_statuses();
                return Ok(StoreReport::Snapshot(self.client.store_status05()?));
            }
            StoreCommand::Initialize(head) => {
                self.client.initialize_stream05(head)?;
                self.release_initial_reads05(true);
                StoreReport::Snapshot(self.client.store_status05()?)
            }
            StoreCommand::Needs => {
                // Receipt acceptance is durable first. A failing settlement
                // remains accepted and is retried independently of the wire.
                let report = self.client.settle_ready05()?;
                self.settled(&report);
                StoreReport::Needs {
                    schema: self.client.pending_schema05()?,
                    settlements: self.client.pending_settlement05()?,
                }
            }
            StoreCommand::Cleanup(now) => {
                StoreReport::ActivePlans(self.client.cleanup_delivery05(now)?)
            }
            StoreCommand::Snapshot => StoreReport::Snapshot(self.client.store_status05()?),
            StoreCommand::Freeze => StoreReport::Frozen(self.client.freeze_batch05()?),
            StoreCommand::Acknowledge(receipt) => {
                let report = self.client.acknowledge_batch05(&receipt)?;
                self.settled(&report);
                StoreReport::Committed {
                    status: self.client.store_status05()?,
                    report,
                    plan: None,
                    active_plans: None,
                }
            }
            StoreCommand::Apply {
                plan_id,
                mut queue,
                now,
            } => {
                let report = self
                    .client
                    .apply_next_delivery05(&mut queue, now)?
                    .unwrap_or_default();
                let next = queue.plans.get(&plan_id).map_or(u64::MAX, |p| p.next);
                self.settled(&report);
                if self.client.pending_schema05()?.is_none() {
                    self.release_initial_reads05(true);
                }
                StoreReport::Committed {
                    status: self.client.store_status05()?,
                    report,
                    plan: Some((plan_id, next)),
                    active_plans: Some(self.client.active_delivery_plans05()?),
                }
            }
        };
        self.committed_since(generation);
        self.publish();
        Ok(result)
    }
}
