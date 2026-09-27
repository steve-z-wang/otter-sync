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
//! response, a prerequisite outcome), else a Downlink pump, else a push-lane
//! turn - in the order they were admitted, so neither starves. Each unit holds
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
//! - `direct`: direct Query/Mutation calls, Query once flights, their
//!   deadlines and fences.
//! - `prerequisites`: the prerequisite loop over application handlers.
//! - `observers`: subscription status, Bootstrap waiters and local watches,
//!   published as snapshots.
//! - `commands`: the commands executed directly against the client: local
//!   reads and writes, Scope and Bootstrap registrations, the sync state and
//!   the protocol seams (`freeze`, `ack`, `pull`).
mod commands;
mod direct;
mod effects;
mod lanes;
mod observers;
mod prerequisites;
pub mod protocol;
mod tasks;
mod transactions;

pub use protocol::*;

use crate::{Client, ClientStore, Result, Schema, StoreFactory};
use serde_json::{Value, json};
use std::collections::{BTreeMap, VecDeque};
use std::path::Path;

/// One open client and everything the runtime decided about it. See the
/// module documentation for the contract of the three driving calls.
pub struct ClientRuntime<S: ClientStore> {
    client: Client<S>,
    lanes: lanes::Lanes,
    /// The runtime-owned connection: its intent, lane effects and credential
    /// refresh. `None` until `connect` and after `stop`.
    connection: Option<lanes::Connection>,
    tasks: tasks::Tasks,
    transaction: Option<transactions::Transaction>,
    /// Every effect the host may still answer, by id, and what it was issued
    /// for. A result for an id that is not here is ignored.
    effects: BTreeMap<String, effects::EffectKind>,
    /// Effect results turned into local work, one unit each, in arrival order.
    ready: VecDeque<effects::Ready>,
    directs: direct::Directs,
    prerequisites: Option<prerequisites::Loop>,
    /// Subscription and watch observers, and the Bootstrap waiters.
    observers: observers::Observers,
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
    pub fn new(client: Client<S>) -> Self {
        Self {
            client,
            lanes: lanes::Lanes::default(),
            connection: None,
            tasks: tasks::Tasks::default(),
            transaction: None,
            effects: BTreeMap::new(),
            ready: VecDeque::new(),
            directs: direct::Directs::default(),
            prerequisites: None,
            observers: observers::Observers::default(),
            admitted: 0,
            lane_since: None,
            issued: 0,
            events: vec![],
            lifecycle: Lifecycle::Open,
        }
    }
    /// What a successful open answers: the client id and the schema check's
    /// outcome, as the SDKs report it in `status()`.
    pub fn opened(&self) -> Value {
        json!({
            "clientId": self.client.client_id(),
            "schema": commands::schema_json(self.client.schema_state()),
        })
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

impl<S: ClientStore> ClientRuntime<S> {
    /// A fresh number for an identity the runtime issues. Never reused; an
    /// exhausted counter refuses rather than wraps.
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
