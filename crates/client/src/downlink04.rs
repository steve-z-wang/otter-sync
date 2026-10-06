//! Protocol-4 carrier branch of the existing DownlinkWorker and lane actions.
use crate::{ApplyReport, Client, ClientStore, ConnectionDriver, DownlinkAction, DownlinkEvent};
use axton_core::{
    Result, invalid,
    v04::{self},
};
use std::collections::{BTreeMap, VecDeque};
#[derive(Clone)]
enum Request {
    Bootstrap(v04::BootstrapIntent, bool),
    Delta(v04::DeltaIntent),
}
#[derive(Default)]
pub(crate) struct Delivery04 {
    running: bool,
    paused: bool,
    context: Option<v04::RequestContext>,
    epoch: u64,
    serial: u64,
    socket: Option<v04::SubscribeIntent>,
    acknowledged: bool,
    events: VecDeque<DownlinkEvent>,
    pages: VecDeque<v04::DeltaPage>,
    requests: BTreeMap<u64, Request>,
    receipt_runs: BTreeMap<String, String>,
    pending: Vec<DownlinkAction>,
    retry_at: u64,
    failures: u32,
    catchup_head: u64,
    delta_limit: u64,
    manifest_limit: u64,
}
impl Delivery04 {
    pub(crate) fn queued_frames(&self) -> usize {
        self.pages.len()
            + self
                .events
                .iter()
                .filter(|e| matches!(e, DownlinkEvent::Message { .. }))
                .count()
    }
    pub(crate) fn enqueue(&mut self, event: DownlinkEvent) {
        if let DownlinkEvent::Message { epoch, .. } = &event
            && self.queued_frames() >= 32
        {
            if !self
                .events
                .iter()
                .any(|event| matches!(event,DownlinkEvent::Overflow{epoch:queued} if queued==epoch))
            {
                self.events
                    .push_back(DownlinkEvent::Overflow { epoch: *epoch });
            }
            return;
        }
        self.events.push_back(event);
    }
    fn close(&mut self, reason: Option<String>) {
        if self.socket.take().is_some() {
            self.pending.push(DownlinkAction::Close {
                epoch: self.epoch,
                reason,
            });
        }
        self.acknowledged = false;
        self.pages.clear();
        self.requests
            .retain(|_, request| matches!(request, Request::Bootstrap(_, _)));
    }
    fn failed(&mut self, now: u64, entropy: u64) {
        self.close(None);
        self.retry_at = now.saturating_add(ConnectionDriver::backoff(self.failures, entropy));
        self.failures = self.failures.saturating_add(1);
    }
    fn request<S: ClientStore>(
        &mut self,
        c: &mut Client<S>,
        owner: &str,
        intent: v04::BootstrapIntent,
        internal: bool,
    ) -> Result<DownlinkAction> {
        let intent = c.frozen_bootstrap04(owner, &intent)?;
        self.serial = self
            .serial
            .checked_add(1)
            .ok_or_else(|| invalid("request identifiers exhausted"))?;
        let request = self.serial;
        let body = String::from_utf8(v04::encode(&intent)?).map_err(|_| invalid("UTF8"))?;
        self.requests
            .insert(request, Request::Bootstrap(intent, internal));
        Ok(DownlinkAction::Request {
            request,
            body,
            bootstrap: true,
        })
    }
    fn delta<S: ClientStore>(&mut self, c: &mut Client<S>) -> Result<DownlinkAction> {
        self.serial = self
            .serial
            .checked_add(1)
            .ok_or_else(|| invalid("request identifiers exhausted"))?;
        let request = self.serial;
        let intent = v04::DeltaIntent {
            context: c.request_context()?.clone(),
            call_id: uuid::Uuid::new_v4().to_string(),
            after: c.stream_cursor04()?,
            models: c.declared_models(),
            limit: if self.delta_limit == 0 {
                128
            } else {
                self.delta_limit
            },
        };
        let body = String::from_utf8(v04::encode(&intent)?).map_err(|_| invalid("UTF8"))?;
        self.requests.insert(request, Request::Delta(intent));
        Ok(DownlinkAction::Request {
            request,
            body,
            bootstrap: false,
        })
    }
    fn report(report: ApplyReport, stream: &str) -> Vec<DownlinkAction> {
        let mut actions = vec![DownlinkAction::Wake { lane: "push" }];
        if !report.cursors.is_empty() {
            actions.push(DownlinkAction::Changed {
                streams: vec![stream.into()],
            });
        }
        if !report.reports.is_empty() {
            actions.push(DownlinkAction::Report {
                reports: report.reports,
            });
        }
        actions
    }
    fn public_status<S: ClientStore>(c: &mut Client<S>) -> Result<DownlinkAction> {
        let stream = c.request_context()?.binding.stream.clone();
        let subscription = c
            .subscription_state(&stream)?
            .ok_or_else(|| invalid("bound subscription missing"))?;
        Ok(DownlinkAction::Bootstrap(
            c.bootstrap_state(&stream, subscription.subscription_id)?,
        ))
    }
    fn has_bootstrap(&self, internal: bool) -> bool {
        self.requests
            .values()
            .any(|r| matches!(r,Request::Bootstrap(_,kind) if *kind==internal))
    }
    pub(crate) fn next<S: ClientStore>(
        &mut self,
        c: &mut Client<S>,
        now: u64,
        entropy: u64,
    ) -> Result<Vec<DownlinkAction>> {
        let mut actions = self.turn(c, now, entropy)?;
        // One turn consumes one event. A Stop/Pause may produce no transport
        // work while a newer Start/Resume is still queued; keep that turn ready.
        if actions.is_empty() && !self.events.is_empty() {
            actions.push(DownlinkAction::Wait { millis: 0 });
        }
        Ok(actions)
    }
    fn turn<S: ClientStore>(
        &mut self,
        c: &mut Client<S>,
        now: u64,
        entropy: u64,
    ) -> Result<Vec<DownlinkAction>> {
        let active = c.request_context()?.clone();
        if self.context.as_ref() != Some(&active) {
            let prior = self.context.replace(active.clone());
            self.close(None);
            self.requests.clear();
            self.receipt_runs.clear();
            self.catchup_head = 0;
            self.delta_limit = 0;
            self.manifest_limit = 0;
            if prior.is_some() {
                self.pending.push(DownlinkAction::Reset);
            }
            if let (Some(page), Some(progress)) = (c.saved_delta04()?, c.delta_progress04()?)
                && page.context == active
                && !progress.complete(&page)
            {
                self.pages.push_back(page);
            }
        }
        if let Some(event) = self.events.pop_front() {
            match event {
                DownlinkEvent::Start => {
                    self.running = true;
                    self.paused = false;
                    self.retry_at = 0;
                }
                DownlinkEvent::Stop => {
                    self.running = false;
                    self.close(None);
                    self.requests.clear();
                }
                DownlinkEvent::Pause => {
                    self.paused = true;
                    self.close(None);
                    self.requests.clear();
                }
                DownlinkEvent::Resume => {
                    self.paused = false;
                    self.retry_at = 0;
                }
                DownlinkEvent::Wake => {}
                DownlinkEvent::Next => {}
                DownlinkEvent::Closed { epoch } | DownlinkEvent::Overflow { epoch } => {
                    if epoch == self.epoch && self.socket.is_some() {
                        self.failed(now, entropy);
                    }
                }
                DownlinkEvent::Failed { request, .. } => {
                    if let Some(request) = self.requests.remove(&request) {
                        match request {
                            Request::Delta(intent) => self.delta_limit = (intent.limit / 2).max(1),
                            Request::Bootstrap(v04::BootstrapIntent::Page { limit, .. }, _) => {
                                self.manifest_limit = (limit / 2).max(1)
                            }
                            _ => {}
                        }
                        self.failed(now, entropy);
                    }
                }
                DownlinkEvent::Response { request, body } => {
                    if let Some(intent) = self.requests.remove(&request) {
                        match intent {
                            Request::Delta(intent) => {
                                let page: v04::DeltaPage = v04::decode(body.as_bytes())?;
                                page.context.admit(&active)?;
                                if page.from != intent.after {
                                    return Err(invalid("Delta response prefix mismatch"));
                                }
                                if page.to > c.stream_cursor04()? {
                                    self.pages.push_front(page);
                                }
                            }
                            Request::Bootstrap(intent, _) => match intent {
                                v04::BootstrapIntent::Start { context, .. } => {
                                    let started: v04::BootstrapStarted =
                                        v04::decode(body.as_bytes())?;
                                    if started.context != context {
                                        return Err(invalid("Bootstrap response context mismatch"));
                                    }
                                    c.start_bootstrap04(&started)?;
                                }
                                v04::BootstrapIntent::Materialize {
                                    context,
                                    receipt_targets,
                                    ..
                                } => {
                                    let started: v04::BootstrapStarted =
                                        v04::decode(body.as_bytes())?;
                                    if started.context != context {
                                        return Err(invalid(
                                            "materialization response context mismatch",
                                        ));
                                    }
                                    c.start_materialize04(&started)?;
                                    self.receipt_runs
                                        .insert(receipt_targets.call_id, started.manifest_id);
                                }
                                v04::BootstrapIntent::Page {
                                    context,
                                    manifest_id,
                                    from,
                                    limit,
                                    ..
                                } => {
                                    let page: v04::ManifestPage = v04::decode(body.as_bytes())?;
                                    if page.context != context
                                        || page.manifest_id != manifest_id
                                        || page.from != from
                                        || page.to - page.from > limit
                                    {
                                        return Err(invalid(
                                            "manifest response correlation mismatch",
                                        ));
                                    }
                                    let report = match c.apply_manifest04(&page) {
                                        Ok(report) => report,
                                        Err(error) => {
                                            self.manifest_limit = (limit / 2).max(1);
                                            self.failed(now, entropy);
                                            return Err(error);
                                        }
                                    };
                                    self.pending
                                        .extend(Self::report(report, &active.binding.stream));
                                }
                                v04::BootstrapIntent::Tail {
                                    context,
                                    manifest_id,
                                    ..
                                } => {
                                    let tail: v04::BootstrapTail = v04::decode(body.as_bytes())?;
                                    if tail.context != context || tail.manifest_id != manifest_id {
                                        return Err(invalid("tail response correlation mismatch"));
                                    }
                                    c.capture_bootstrap_tail04(&tail)?;
                                    self.catchup_head = self.catchup_head.max(tail.head);
                                }
                            },
                        }
                    }
                    self.pending.push(Self::public_status(c)?);
                }
                DownlinkEvent::Message { epoch, body } => {
                    if epoch == self.epoch && self.socket.is_some() {
                        if !self.acknowledged {
                            let ack: v04::SubscribeAcknowledged = v04::decode(body.as_bytes())?;
                            let subscribed = self.socket.as_ref().unwrap();
                            if ack.context != subscribed.context || ack.cursor != subscribed.cursor
                            {
                                return Err(invalid("subscribe ACK mismatch"));
                            }
                            self.catchup_head = self.catchup_head.max(ack.head);
                            self.acknowledged = true;
                            self.failures = 0;
                            self.pending.push(DownlinkAction::Acknowledged {
                                streams: vec![active.binding.stream.clone()],
                            });
                        } else {
                            let page: v04::DeltaPage = v04::decode(body.as_bytes())?;
                            page.context.admit(&active)?;
                            if self.pages.len() >= 32 {
                                self.failed(now, entropy);
                            } else if page.to > c.stream_cursor04()? {
                                self.pages.push_back(page);
                            }
                        }
                    }
                }
            }
        }
        if !self.pending.is_empty() {
            return Ok(std::mem::take(&mut self.pending));
        }
        if !self.running || self.paused {
            return Ok(vec![]);
        }
        if self.retry_at > now {
            return Ok(vec![DownlinkAction::Wait {
                millis: self.retry_at - now,
            }]);
        }
        if let Some(page) = self.pages.front().cloned() {
            let cursor = c.stream_cursor04()?;
            if page.to <= cursor {
                self.pages.pop_front();
                return Ok(vec![DownlinkAction::Wait { millis: 0 }]);
            }
            if page.from <= cursor {
                let progress = match c.begin_delta04(&page) {
                    Ok(progress) => progress,
                    Err(error) => {
                        self.failed(now, entropy);
                        return Err(error);
                    }
                };
                if progress.complete(&page) {
                    self.pages.pop_front();
                    return Ok(vec![DownlinkAction::Wait { millis: 0 }]);
                }
                let report = match c.apply_delta_unit04(&page) {
                    Ok(report) => report,
                    Err(error) => {
                        self.failed(now, entropy);
                        return Err(error);
                    }
                };
                if c.delta_progress04()?
                    .is_some_and(|progress| progress.complete(&page))
                {
                    self.pages.pop_front();
                }
                let mut actions = Self::report(report, &active.binding.stream);
                actions.push(Self::public_status(c)?);
                return Ok(actions);
            }
            if self.acknowledged
                && !self
                    .requests
                    .values()
                    .any(|r| matches!(r, Request::Delta(_)))
            {
                return Ok(vec![self.delta(c)?]);
            }
        }
        if self.acknowledged
            && c.stream_cursor04()? < self.catchup_head
            && self.pages.is_empty()
            && !self
                .requests
                .values()
                .any(|request| matches!(request, Request::Delta(_)))
        {
            return Ok(vec![self.delta(c)?]);
        }
        let coverage = c
            .bootstrap_coverage04()?
            .filter(|coverage| coverage.materialization == active.materialization);
        if coverage.is_none() && !self.has_bootstrap(false) {
            let held_keys = if c.initialized04()? {
                c.held_keys04()?
            } else {
                vec![]
            };
            let intent = v04::BootstrapIntent::Start {
                context: active.clone(),
                call_id: uuid::Uuid::new_v4().to_string(),
                models: c.declared_models(),
                budget: 4096,
                held_keys,
            };
            return Ok(vec![self.request(c, "public:start", intent, false)?]);
        }
        let mut actions = Vec::new();
        if c.initialized04()? && self.socket.is_none() {
            self.epoch = self
                .epoch
                .checked_add(1)
                .ok_or_else(|| invalid("socket epochs exhausted"))?;
            let intent = v04::SubscribeIntent {
                context: active.clone(),
                models: c.declared_models(),
                cursor: c.stream_cursor04()?,
            };
            let subscribe =
                String::from_utf8(v04::encode(&intent)?).map_err(|_| invalid("UTF8"))?;
            self.socket = Some(intent);
            self.acknowledged = false;
            actions.push(DownlinkAction::Open {
                epoch: self.epoch,
                subscribe,
            });
        }
        if !self.has_bootstrap(false)
            && let Some(coverage) = coverage
        {
            if coverage.covered < coverage.total {
                let intent = v04::BootstrapIntent::Page {
                    context: active.clone(),
                    call_id: uuid::Uuid::new_v4().to_string(),
                    manifest_id: coverage.manifest_id.clone(),
                    from: coverage.covered,
                    limit: if self.manifest_limit == 0 {
                        128
                    } else {
                        self.manifest_limit
                    },
                };
                actions.push(self.request(
                    c,
                    &format!(
                        "page:{}:{}:{}",
                        coverage.manifest_id,
                        coverage.covered,
                        if self.manifest_limit == 0 {
                            128
                        } else {
                            self.manifest_limit
                        }
                    ),
                    intent,
                    false,
                )?);
            } else if coverage.tail.is_none() {
                let intent = v04::BootstrapIntent::Tail {
                    context: active.clone(),
                    call_id: uuid::Uuid::new_v4().to_string(),
                    manifest_id: coverage.manifest_id.clone(),
                };
                actions.push(self.request(
                    c,
                    &format!("tail:{}", coverage.manifest_id),
                    intent,
                    false,
                )?);
            }
        }
        if !self.has_bootstrap(true) {
            let runs = self.receipt_runs.clone();
            let mut scheduled = false;
            for (call, id) in runs {
                if !c.accepted_awaiting04(&call)? {
                    self.receipt_runs.remove(&call);
                    continue;
                }
                let coverage = c
                    .manifest_coverage04(&id)?
                    .ok_or_else(|| invalid("missing receipt manifest"))?;
                if coverage.covered < coverage.total {
                    let intent = v04::BootstrapIntent::Page {
                        context: active.clone(),
                        call_id: uuid::Uuid::new_v4().to_string(),
                        manifest_id: id.clone(),
                        from: coverage.covered,
                        limit: if self.manifest_limit == 0 {
                            128
                        } else {
                            self.manifest_limit
                        },
                    };
                    actions.push(self.request(
                        c,
                        &format!(
                            "page:{id}:{}:{}",
                            coverage.covered,
                            if self.manifest_limit == 0 {
                                128
                            } else {
                                self.manifest_limit
                            }
                        ),
                        intent,
                        true,
                    )?);
                    scheduled = true;
                    break;
                } else {
                    self.receipt_runs.remove(&call);
                    actions.push(DownlinkAction::Wake { lane: "push" });
                }
            }
            if !scheduled && let Some(targets) = c.receipt_materialization04()? {
                let owner = format!("receipt:{}", targets.call_id);
                let intent = v04::BootstrapIntent::Materialize {
                    context: active.clone(),
                    call_id: uuid::Uuid::new_v4().to_string(),
                    receipt_targets: targets,
                    budget: 4096,
                };
                actions.push(self.request(c, &owner, intent, true)?);
            }
        }
        Ok(actions)
    }
}
