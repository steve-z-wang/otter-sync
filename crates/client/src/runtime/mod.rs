//! Rust-owned local runtime. Tasks route to committed results; application
//! callbacks hold one transaction with nested savepoint capabilities. Direct
//! protocol5 reads and prerequisite handlers run as bounded host effects.
//! Sync05 owns the durable uplink/downlink work independently.
mod commands;
mod direct;
mod effects;
mod lanes;
mod observers;
mod prerequisites;
pub mod protocol;
mod sql_watches;
mod tasks;
mod transactions;
mod unsent;

pub use protocol::*;

use crate::{Client, ClientStore, Result};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

// One open client and everything the runtime decided about it. See the
// module documentation for the contract of the three driving calls.
pub struct ClientRuntime<S: ClientStore> {
    client: Client<S>,
    sync05_live: bool,
    sync05_catching_up: bool,
    // The runtime-owned connection: its intent, lane effects and credential
    // refresh. `None` until `connect` and after `stop`.
    connection: Option<lanes::Connection>,
    tasks: tasks::Tasks,
    transaction: Option<transactions::Transaction>,
    // Every effect the host may still answer, by id, and what it was issued
    // for. A result for an id that is not here is ignored.
    effects: BTreeMap<String, effects::EffectKind>,
    // Effect results turned into local work, one unit each, in arrival order.
    ready: VecDeque<effects::Ready>,
    directs: direct::Directs,
    // The prerequisite handlers registered at open and their scheduler.
    prerequisites: prerequisites::Prerequisites,
    // The Load worker, its batches in flight, and the Load handles'
    // observers and waiters.
    // Subscription and watch observers, and the Bootstrap waiters.
    observers: observers::Observers,
    // The unsent-work observers.
    unsent: unsent::Unsent,
    // Admissions so far: ordinary tasks and effect results are numbered in
    // arrival order, and lane work is scheduled by that order too.
    admitted: u64,
    // The admission count when lane work was first seen ready; `None` while
    // nothing is ready. An ordinary task admitted before it runs first, one
    // admitted after it waits: the arrival order of foreground and inbound
    // work is preserved, and neither starves the other.
    lane_since: Option<u64>,
    // The one counter behind `transactionId`, `scope` and `effectId`: every
    // identity the runtime issues is fresh for its lifetime.
    issued: u64,
    // Capability tokens also fence other Stores and prior runtime opens.
    capability_namespace: Option<String>,
    events: Vec<Event>,
    lifecycle: Lifecycle,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Lifecycle {
    Open,
    // Close was admitted; the next step performs it ahead of any other work.
    Closing,
    Closed,
}

impl<S: ClientStore + 'static> ClientRuntime<S> {
    // [`Client::open_at`] under the runtime. Errors are the open errors.

    // Wrap an already opened client.
    pub fn new(client: Client<S>) -> Self {
        let capability_namespace = Some(uuid::Uuid::new_v4().to_string());
        Self {
            client,
            sync05_live: false,
            sync05_catching_up: false,
            connection: None,
            tasks: tasks::Tasks::default(),
            transaction: None,
            effects: BTreeMap::new(),
            ready: VecDeque::new(),
            directs: direct::Directs::default(),
            prerequisites: prerequisites::Prerequisites::default(),
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
    // Register schema Models whose incoming authority requires a callback.
    // Names are validated once and remain fixed for the runtime lifetime.

    // Configure an opened runtime before any task is admitted.

    // What a successful open answers: the client id and the schema check's
    // outcome, as the SDKs report it in `status()`.
    pub fn opened(&mut self) -> Value {
        let mut opened = json!({
            "clientId": self.client.client_id(),
        });

        if let Ok(context) = self.client.request_context05() {
            opened["context"] = json!(context);
        }
        opened
    }
    // The events queued since the last call, in order.
    pub fn take_events(&mut self) -> Vec<Event> {
        std::mem::take(&mut self.events)
    }
    // Whether [`Event::RuntimeClosed`] has been queued.
    pub fn closed(&self) -> bool {
        self.lifecycle == Lifecycle::Closed
    }
    // Test seam: the inbound socket frames held and not yet applied.
    #[doc(hidden)]
    pub fn held_frames(&self) -> usize {
        0
    }
    // Test seam: the client, for inspecting committed state in Rust tests.
    pub fn client(&mut self) -> &mut Client<S> {
        &mut self.client
    }
}

impl<S: ClientStore + 'static> ClientRuntime<S> {
    // A fresh number for an identity the runtime issues. Never reused; an
    // exhausted counter refuses rather than wraps.
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
    // Settle one routed request. An engine refusal of a registration this
    // client no longer holds carries `{"code":"subscription.closed"}`, so no
    // SDK has to recognize it by its message.
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
    // Fail one routed request with a machine-readable reason.
    fn fail(&mut self, request_id: String, error: impl Into<String>, details: Value) {
        self.settle(request_id, Err(error.into()), Some(details));
    }
    // The only place a [`Event::TaskCompleted`] is queued, so a request is
    // completed at most once.
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
    // When a unit committed since `generation`, the watches re-run before
    // the unit ends.
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
    // A failure the application hears about, with the HTTP status it carried.
    fn error_status(&mut self, message: impl Into<String>, status: Option<u16>) {
        self.report(Diagnostic::Error {
            message: message.into(),
            status,
        });
    }
}

// The prefix every engine refusal of a closed registration carries
// ([`crate::SUBSCRIPTION_CLOSED`]).
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
                let status = if report.completions.is_empty() {
                    None
                } else {
                    Some(self.client.store_status05()?)
                };
                StoreReport::Needs {
                    status,
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
