//! Network control, independent of the writer. Inputs are correlated and staged
//! here; only Store reports can advance committed progress or settle Calls.
use super::{DeliveryQueue, StoreCommand, StoreReport};
use crate::{
    Result, invalid,
    runtime::{Diagnostic, EffectOutcome, Event, HttpRoute, Operation, SocketEvent},
    store05::StoreStatus05,
    v05,
};
use std::collections::{BTreeMap, VecDeque};
#[derive(Clone)]
enum Flight {
    Handshake(v05::HandshakeRequest),
    Delta(v05::DeltaRequest),
    Push(v05::MutationRequest),
    Owned(v05::MaterializationRequest),
    Socket,
    Expire,
    Retry,
    Refresh,
}
pub struct Control {
    context: v05::RequestContext,
    status: StoreStatus05,
    queue: DeliveryQueue,
    flights: BTreeMap<String, Flight>,
    issued: u64,
    connected: bool,
    paused: bool,
    live: bool,
    network_state: Option<(bool, bool)>,
    head: u64,
    events: Vec<Event>,
    jobs: VecDeque<StoreCommand>,
    applying: bool,
    applying_plan: Option<String>,
    freezing: bool,
    failures: u32,
    epoch: std::sync::Arc<std::sync::atomic::AtomicU64>,
    refresh_auth: bool,
    refreshing: bool,
    refresh_waiters: Vec<Flight>,
}
impl Control {
    pub fn new(status: StoreStatus05) -> Self {
        Self {
            context: status.context.clone(),
            head: status.cursor.unwrap_or(0),
            status,
            queue: DeliveryQueue::new(16 * 1024 * 1024, 32),
            flights: BTreeMap::new(),
            issued: 0,
            connected: false,
            paused: false,
            live: false,
            network_state: None,
            events: vec![],
            jobs: VecDeque::new(),
            applying: false,
            applying_plan: None,
            freezing: false,
            failures: 0,
            epoch: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(1)),
            refresh_auth: false,
            refreshing: false,
            refresh_waiters: vec![],
        }
    }
    pub fn set_refresh_auth(&mut self, enabled: bool) {
        self.refresh_auth = enabled;
    }
    pub fn connect(&mut self) -> Result<()> {
        self.connected = true;
        self.paused = false;
        self.handshake()
    }
    fn effect(&mut self, flight: Flight, operation: Operation) {
        self.issued += 1;
        let id = format!("control05:{}", self.issued);
        self.flights.insert(id.clone(), flight);
        self.events.push(Event::Effect {
            effect_id: id,
            operation,
        });
    }
    fn http<T: serde::Serialize>(
        &mut self,
        flight: Flight,
        route: HttpRoute,
        request: &T,
    ) -> Result<()> {
        let body = serde_json::to_string(request)?;
        self.effect(flight, Operation::Http { route, body });
        Ok(())
    }
    fn handshake(&mut self) -> Result<()> {
        self.epoch.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.applying = false;
        self.applying_plan = None;
        self.freezing = false;
        let old = self
            .flights
            .iter()
            .filter(|(_, f)| matches!(f, Flight::Socket | Flight::Delta(_) | Flight::Handshake(_)))
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        for id in old {
            self.flights.remove(&id);
            self.events.push(Event::CancelEffect { effect_id: id });
        }
        let request = v05::HandshakeRequest {
            protocol: 5,
            store_id: self.context.store_id.clone(),
            stream: self.context.stream.clone(),
        };
        self.http(
            Flight::Handshake(request.clone()),
            HttpRoute::Handshake,
            &request,
        )
    }
    pub fn stop(&mut self) {
        self.epoch.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.applying = false;
        self.applying_plan = None;
        self.connected = false;
        self.live = false;
        self.network_state = None;
        for id in self.flights.keys() {
            self.events.push(Event::CancelEffect {
                effect_id: id.clone(),
            });
        }
        self.flights.clear();
        self.jobs.clear();
        self.freezing = false;
        self.refreshing = false;
        self.refresh_waiters.clear();
    }
    pub fn pause(&mut self) {
        self.stop();
        self.paused = true;
    }
    pub fn wake(&mut self) {
        if self.connected
            && !self.freezing
            && !self.flights.values().any(|f| matches!(f, Flight::Push(_)))
        {
            self.freezing = true;
            self.jobs.push_back(StoreCommand::Freeze)
        }
    }
    pub fn accepts(&self, id: &str) -> bool {
        id.starts_with("control05:")
    }
    pub fn events(&mut self) -> Vec<Event> {
        std::mem::take(&mut self.events)
    }
    pub fn jobs(&mut self) -> Vec<StoreCommand> {
        let fence = super::WorkFence05 {
            context: self.context.clone(),
            epoch: self.epoch.load(std::sync::atomic::Ordering::SeqCst),
            current: self.epoch.clone(),
        };
        self.jobs
            .drain(..)
            .map(|command| StoreCommand::Guarded {
                fence: fence.clone(),
                command: Box::new(command),
            })
            .collect()
    }
    pub fn receive(&mut self, id: &str, outcome: EffectOutcome, now: u64) -> Result<()> {
        let Some(flight) = self.flights.get(id).cloned() else {
            return Ok(());
        };
        if !matches!(flight, Flight::Socket) {
            self.flights.remove(id);
        }
        if !outcome.ok {
            if let Some(error) = &outcome.error
                && let (Some(status), Some(body)) = (error.status, &error.refusal)
            {
                let diagnostic = Diagnostic::Refused {
                    message: error.message.clone(),
                    status,
                    body: serde_json::from_str(body)
                        .unwrap_or_else(|_| serde_json::Value::String(body.clone())),
                };
                self.stop();
                self.events.push(Event::Report { diagnostic });
                return Ok(());
            }
            if outcome
                .error
                .as_ref()
                .is_some_and(|e| e.status == Some(401))
                && self.refresh_auth
                && !matches!(flight, Flight::Refresh)
            {
                self.flights.remove(id);
                self.refresh_waiters.push(flight);
                if !self.refreshing {
                    self.refreshing = true;
                    self.effect(Flight::Refresh, Operation::RefreshAuth);
                }
                return Ok(());
            }
            if matches!(flight, Flight::Refresh) {
                self.refreshing = false;
                self.refresh_waiters.clear();
            }
            self.flights.remove(id);
            self.retry(outcome.error.map_or("network failed".into(), |e| e.message));
            return Ok(());
        }
        if matches!(flight, Flight::Refresh) {
            self.refreshing = false;
            for waiter in std::mem::take(&mut self.refresh_waiters) {
                self.resend(waiter)?;
            }
            return Ok(());
        }
        if matches!(flight, Flight::Expire) {
            self.jobs.push_back(StoreCommand::Cleanup(now));
            return self.schedule(now);
        }
        if matches!(flight, Flight::Retry) {
            self.queue.retry_blocked();
            return self.handshake();
        }
        if matches!(flight, Flight::Socket) {
            match serde_json::from_value::<SocketEvent>(outcome.value.unwrap_or_default())? {
                SocketEvent::Opened => {
                    self.live = true;
                    self.schedule(now)?;
                }
                SocketEvent::Closed | SocketEvent::Overflow => {
                    self.flights.remove(id);
                    self.live = false;
                    self.schedule(now)?;
                    self.retry("socket closed or overflow".into());
                }
                SocketEvent::Message { body } => {
                    let raw: serde_json::Value = serde_json::from_str(&body)?;
                    if raw.get("head").is_some() && raw.get("header").is_none() {
                        let ack = v05::decode::<v05::HandshakeResponse>(body.as_bytes())?;
                        ack.admit(&v05::HandshakeRequest {
                            protocol: 5,
                            store_id: self.context.store_id.clone(),
                            stream: self.context.stream.clone(),
                        })?;
                        self.head = self.head.max(ack.head);
                        self.schedule(now)?;
                        return Ok(());
                    }
                    let response: v05::DeliveryResponse = serde_json::from_str(&body)?;
                    // Socket offers are finite ranges. Authentication fixed the
                    // Stream; local Store/context and plan still need admission.
                    if response.header.bootstrap || response.header.owner.is_some() {
                        return Err(invalid("invalid socket delivery purpose"));
                    }
                    response.header.context.admit_store(&self.context)?;
                    if response.header.context.materialization != self.context.materialization {
                        v05::Validate::validate(&response.header)?;
                        self.head = self.head.max(response.header.observed_head);
                        self.schedule(now)?;
                        return Ok(());
                    }
                    self.head = self.head.max(response.header.observed_head);
                    self.queue
                        .receive(&response.header, &response.parts, &self.context, now)?;
                    self.schedule(now)?;
                }
            }
            return Ok(());
        }
        let body = match outcome.value {
            Some(serde_json::Value::String(body)) => body,
            Some(serde_json::Value::Object(v)) => v
                .get("body")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| invalid("HTTP body required"))?
                .into(),
            _ => return Err(invalid("HTTP body required")),
        };
        match flight {
            Flight::Handshake(request) => {
                let response = v05::decode::<v05::HandshakeResponse>(body.as_bytes())?;
                response.admit(&request)?;
                self.head = self.head.max(response.head);
                self.failures = 0;
                self.jobs.push_back(StoreCommand::Initialize(response.head));
                // Socket reception is live immediately, before Store SQL runs.
                self.effect(
                    Flight::Socket,
                    Operation::Socket {
                        subscribe: serde_json::to_string(&request)?,
                    },
                );
            }
            Flight::Delta(request) => {
                let response: v05::DeliveryResponse = serde_json::from_str(&body)?;
                if response.header.context != request.context
                    || response.header.after != Some(request.after)
                    || response.header.through != Some(request.through)
                    || response.header.bootstrap != request.bootstrap
                    || response.header.owner.is_some()
                {
                    return Err(invalid("delta correlation mismatch"));
                }
                if let Some(c) = &request.continuation
                    && (c.plan_id != response.header.plan_id || c.digest != response.header.digest)
                {
                    return Err(invalid("continuation mismatch"));
                }
                if let Some(c) = &request.continuation
                    && !response
                        .parts
                        .iter()
                        .any(|p| p.unit == c.unit && p.part == c.part)
                {
                    return Err(invalid("continuation omitted requested fragment"));
                }
                self.head = self.head.max(response.header.observed_head);
                self.queue
                    .receive(&response.header, &response.parts, &self.context, now)?;
                self.schedule(now)?;
            }
            Flight::Owned(request) => {
                let response: v05::MaterializationResponse = serde_json::from_str(&body)?;
                self.queue
                    .receive_owned(&request, &response, &self.context, now)?;
                self.schedule(now)?;
            }
            Flight::Push(request) => {
                let receipt = v05::decode::<v05::BatchAcknowledgement>(body.as_bytes())?;
                if receipt.context != request.context
                    || receipt.batch_id != request.batch_id
                    || receipt.digest != request.digest
                {
                    return Err(invalid("Batch acknowledgement correlation mismatch"));
                }
                for result in &receipt.results {
                    if let v05::MutationOutcome::Accepted { sync_cursor, .. } = result.outcome {
                        self.head = self.head.max(sync_cursor);
                    }
                }
                self.jobs.push_back(StoreCommand::Acknowledge(receipt));
            }
            _ => {}
        }
        Ok(())
    }
    fn resend(&mut self, flight: Flight) -> Result<()> {
        match flight {
            Flight::Handshake(request) => self.http(
                Flight::Handshake(request.clone()),
                HttpRoute::Handshake,
                &request,
            ),
            Flight::Delta(request) => {
                self.http(Flight::Delta(request.clone()), HttpRoute::Pull, &request)
            }
            Flight::Owned(request) => self.http(
                Flight::Owned(request.clone()),
                HttpRoute::Materialize,
                &request,
            ),
            Flight::Push(request) => {
                self.http(Flight::Push(request.clone()), HttpRoute::Push, &request)
            }
            Flight::Socket => self.handshake(),
            _ => Ok(()),
        }
    }
    fn retry(&mut self, message: String) {
        self.events.push(Event::Report {
            diagnostic: Diagnostic::Error {
                message,
                status: None,
            },
        });
        if self.connected && !self.flights.values().any(|f| matches!(f, Flight::Retry)) {
            self.failures = self.failures.saturating_add(1);
            let millis = (250u64.saturating_mul(1u64 << self.failures.min(7))).min(30000);
            self.effect(Flight::Retry, Operation::Timer { millis });
        }
    }
    pub fn network_error(&mut self, message: String) {
        self.retry(message)
    }
    pub fn failed(&mut self, message: String) {
        if let Some(id) = self.applying_plan.take()
            && let Some(p) = self.queue.plans.get_mut(&id)
        {
            p.blocked = true;
            if p.header.owner.is_some() {
                self.queue.plans.remove(&id);
                self.jobs.push_back(StoreCommand::Needs);
            }
        }
        self.applying = false;
        self.freezing = false;
        self.retry(message)
    }
    pub fn report(&mut self, report: StoreReport, now: u64) -> Result<()> {
        match report {
            StoreReport::Obsolete => return Ok(()),
            StoreReport::Guarded { fence, report } => {
                if !fence.current() || fence.context != self.context {
                    return Ok(());
                }
                return match *report {
                    Ok(report) => self.report(report, now),
                    Err(message) => {
                        self.failed(message);
                        Ok(())
                    }
                };
            }
            StoreReport::Snapshot(status) => {
                if status.context.store_id != self.context.store_id {
                    let reconnect = self.connected;
                    self.stop();
                    self.queue = DeliveryQueue::new(16 * 1024 * 1024, 32);
                    self.applying = false;
                    self.applying_plan = None;
                    self.head = status.cursor.unwrap_or(0);
                    self.context = status.context.clone();
                    if reconnect {
                        self.connect()?;
                    }
                }
                self.context = status.context.clone();
                self.status = status;
                self.wake();
                self.jobs.push_back(StoreCommand::Needs);
            }
            StoreReport::Needs {
                schema,
                settlements,
            } => {
                if let Some(schema) = schema {
                    let request = v05::MaterializationRequest {
                        context: schema.desired_context,
                        request_id: format!("schema:{}", self.issued + 1),
                        owner: v05::MaterializationOwner::Schema {
                            previous_materialization: schema.previous_context.materialization,
                        },
                        keys: schema.authority_keys,
                        models: schema.bootstrap_models,
                        continuation: None,
                    };
                    self.owned(request)?;
                }
                for pending in settlements {
                    if !pending.missing_keys.is_empty() {
                        let request = v05::MaterializationRequest {
                            context: self.context.clone(),
                            request_id: format!("settlement:{}", self.issued + 1),
                            owner: v05::MaterializationOwner::Settlement {
                                batch_id: pending.batch_id,
                                mutation_id: pending.mutation_id,
                            },
                            keys: pending.missing_keys,
                            models: BTreeMap::new(),
                            continuation: None,
                        };
                        self.owned(request)?;
                    }
                }
            }
            StoreReport::Frozen(request) => {
                self.freezing = false;
                if let Some(request) = request {
                    self.http(Flight::Push(request.clone()), HttpRoute::Push, &request)?;
                }
            }
            StoreReport::ActivePlans(active) => {
                self.queue
                    .plans
                    .retain(|id, p| p.next == 0 || active.contains(id));
            }
            StoreReport::Committed {
                status,
                plan,
                active_plans,
                ..
            } => {
                if status.context != self.context {
                    self.queue
                        .plans
                        .retain(|_, p| p.header.context == status.context);
                    let stale = self
                        .flights
                        .iter()
                        .filter_map(|(id, f)| match f {
                            Flight::Delta(r) if r.context != status.context => Some(id.clone()),
                            Flight::Owned(r) if r.context != status.context => Some(id.clone()),
                            _ => None,
                        })
                        .collect::<Vec<_>>();
                    for id in stale {
                        self.flights.remove(&id);
                        self.events.push(Event::CancelEffect { effect_id: id });
                    }
                }
                if let Some(active) = active_plans {
                    self.queue
                        .plans
                        .retain(|id, p| p.next == 0 || active.contains(id));
                }
                self.context = status.context.clone();
                self.status = status;
                self.wake();
                self.jobs.push_back(StoreCommand::Needs);
                if let Some((id, next)) = plan {
                    self.applying = false;
                    self.applying_plan = None;
                    if let Some(p) = self.queue.plans.get_mut(&id) {
                        p.next = next;
                        p.parts.retain(|(unit, _), _| *unit >= next);
                        if next >= p.header.units.len() as u64 {
                            self.queue.plans.remove(&id);
                        }
                    }
                }
            }
        }
        self.schedule(now)
    }
    fn owned(&mut self, request: v05::MaterializationRequest) -> Result<()> {
        if self
            .flights
            .values()
            .any(|f| matches!(f,Flight::Owned(r) if r.owner==request.owner))
            || self
                .queue
                .plans
                .values()
                .any(|p| p.header.owner.as_ref() == Some(&request.owner))
        {
            return Ok(());
        }
        self.http(
            Flight::Owned(request.clone()),
            HttpRoute::Materialize,
            &request,
        )
    }
    fn schedule(&mut self, now: u64) -> Result<()> {
        if !self.connected || self.paused {
            return Ok(());
        }
        let before = self.queue.len();
        self.queue.expire(now);
        if before != self.queue.len() {
            self.jobs.push_back(StoreCommand::Cleanup(now));
        }
        if let Some(expires) = self.queue.plans.values().map(|p| p.header.expires_at).min()
            && !self.flights.values().any(|f| matches!(f, Flight::Expire))
        {
            self.effect(
                Flight::Expire,
                Operation::Timer {
                    millis: expires.saturating_sub(now).max(1),
                },
            );
        }
        let state = (self.live, self.status.cursor.is_none_or(|c| c < self.head));
        if self.network_state != Some(state) {
            self.network_state = Some(state);
            self.jobs.push_back(StoreCommand::NetworkState {
                live: state.0,
                catching_up: state.1,
            });
        }
        if !self.applying {
            for (id, p) in &self.queue.plans {
                if p.blocked
                    || (p.header.owner.is_none()
                        && self.queue.plans.values().any(|blocked| {
                            blocked.blocked
                                && blocked.header.owner.is_none()
                                && blocked.header.bootstrap == p.header.bootstrap
                        }))
                {
                    continue;
                }
                let position = if p.header.bootstrap {
                    self.status.bootstrap_cursor.unwrap_or(0)
                } else {
                    self.status.cursor.unwrap_or(0)
                };
                if p.header.after.is_some_and(|after| after > position) {
                    continue;
                }
                if p.header.owner.is_some()
                    && (p.owner.is_none()
                        || (0..p.header.units.len() as u64).any(|index| !p.complete(index)))
                {
                    continue;
                }
                if p.unit(p.next)?.is_some() {
                    let mut queue = DeliveryQueue::new(16 * 1024 * 1024, 32);
                    queue.plans.insert(id.clone(), p.worker_snapshot());
                    self.jobs.push_back(StoreCommand::Apply {
                        plan_id: id.clone(),
                        queue,
                        now,
                    });
                    self.applying = true;
                    self.applying_plan = Some(id.clone());
                    break;
                }
            }
        }
        if self.flights.values().any(|f| matches!(f, Flight::Delta(_))) {
            return Ok(());
        }
        if let Some(c) = self.queue.continuations().first() {
            let p = &self.queue.plans[&c.plan_id];
            if let Some(original) = &p.owner {
                let mut request = original.clone();
                request.continuation = Some(c.clone());
                if !self
                    .flights
                    .values()
                    .any(|f| matches!(f,Flight::Owned(r) if r.owner==request.owner))
                {
                    return self.http(
                        Flight::Owned(request.clone()),
                        HttpRoute::Materialize,
                        &request,
                    );
                }
            } else if p.header.owner.is_none() {
                let request = v05::DeltaRequest {
                    context: self.context.clone(),
                    after: p.header.after.unwrap(),
                    through: p.header.through.unwrap(),
                    bootstrap: p.header.bootstrap,
                    continuation: Some(c.clone()),
                };
                return self.http(Flight::Delta(request.clone()), HttpRoute::Pull, &request);
            }
        }
        if self.status.start_cursor.is_none() {
            return Ok(());
        }
        let bootstrap = self.status.bootstrap_cursor != self.status.start_cursor;
        let after = if bootstrap {
            self.status.bootstrap_cursor.unwrap_or(0)
        } else {
            self.status.cursor.unwrap_or(0)
        };
        let through = if bootstrap {
            self.status.start_cursor.unwrap()
        } else {
            self.head
        };
        if self.queue.plans.values().any(|p| {
            p.header.bootstrap == bootstrap
                && p.header.after.is_some_and(|a| a <= after)
                && p.header.through.is_some_and(|end| end >= through)
        }) {
            return Ok(());
        }
        if bootstrap || after < through {
            let request = v05::DeltaRequest {
                context: self.context.clone(),
                after,
                through,
                bootstrap,
                continuation: None,
            };
            self.http(Flight::Delta(request.clone()), HttpRoute::Pull, &request)?;
        }
        Ok(())
    }
}
