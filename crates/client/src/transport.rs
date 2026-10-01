//! Runtime transport scheduling. Hosts execute bytes and return bytes; no SDK settlement logic.
use crate::*;
#[derive(Clone, Debug, Serialize)]
pub struct TransportAction {
    pub kind: String,
    pub body: String,
}
pub struct SyncCycle {
    push_only: bool,
    /// Every subscribed stream reached its head in this cycle.
    completed: bool,
    active: Option<TransportAction>,
    history: Option<BootstrapTask>,
    retry_reconciliation: bool,
}
impl Default for SyncCycle {
    fn default() -> Self {
        Self {
            push_only: false,
            completed: false,
            active: None,
            history: None,
            retry_reconciliation: true,
        }
    }
}
impl SyncCycle {
    /// Validate a received push receipt against the still-frozen action and
    /// keep that action until local authority and settlement commit.
    pub(crate) fn receipt_delivery<S: ClientStore>(
        &self,
        client: &mut Client<S>,
        bytes: &[u8],
    ) -> Result<StoreDelivery> {
        let (sequence, receipt) = self.decode_push_receipt(bytes)?;
        client.validate_push_receipt(sequence, &receipt)?;
        Ok(StoreDelivery::Receipt { sequence, receipt })
    }

    fn decode_push_receipt(&self, bytes: &[u8]) -> Result<(u64, PushReceipt)> {
        let action = self
            .active
            .as_ref()
            .ok_or_else(|| invalid("no transport action"))?;
        if action.kind != "push" {
            return Err(invalid("active transport action is not a push"));
        }
        let raw: serde_json::Value = serde_json::from_str(&action.body)?;
        let action_batch = raw["mutations"]
            .as_array()
            .is_some_and(|calls| calls.iter().any(|call| call.get("callId").is_some()));
        let request = if action_batch {
            PushRequest::decode_action_envelope(action.body.as_bytes())?
        } else {
            PushRequest::decode(action.body.as_bytes())?
        };
        let receipt = if action_batch {
            PushReceipt::decode_action_envelope(bytes)?
        } else {
            PushReceipt::decode(bytes)?
        };
        Ok((request.batch_sequence, receipt))
    }
    pub(crate) fn receipt_committed(&mut self) {
        self.active = None;
        self.completed = false;
    }
    pub fn restart(&mut self) {
        self.completed = false;
        self.active = None;
        self.history = None;
        self.retry_reconciliation = true;
        self.push_only = false;
    }
    /// Use HTTP only for queued writes; authoritative pages arrive through the live stream.
    pub fn restart_push_only(&mut self) {
        self.restart();
        self.push_only = true;
    }
    pub fn next<S: ClientStore>(
        &mut self,
        client: &mut Client<S>,
    ) -> Result<Option<TransportAction>> {
        if let Some(action) = &self.active {
            return Ok(Some(action.clone()));
        }
        if let Some(bytes) = client.freeze()? {
            let action = TransportAction {
                kind: "push".into(),
                body: String::from_utf8(with_capabilities(&bytes, &[STREAM_AUTHORITY_CAPABILITY])?)
                    .map_err(|_| invalid("utf8"))?,
            };
            self.active = Some(action.clone());
            return Ok(Some(action));
        }
        if self.push_only {
            return Ok(None);
        }
        if std::mem::take(&mut self.retry_reconciliation) {
            client.retry_reconciliation_failures()?;
        }
        if client.reconciliation_failed()? {
            return Err(invalid(
                "stream reconciliation failed; restart the cycle to retry",
            ));
        }
        if let Some(task) = client.reconciliation_schedule()? {
            let action = TransportAction {
                kind: "pull".into(),
                body: String::from_utf8(task.encode_request(client.declared_models())?)
                    .map_err(|_| invalid("utf8"))?,
            };
            self.history = Some(task);
            self.active = Some(action.clone());
            return Ok(Some(action));
        }
        if self.completed && !client.any_reconciliation_pending()? {
            return Ok(None);
        }
        // One pull covers every subscribed stream; a pull on any other stream
        // would be discarded by `apply_page`.
        let Some(body) = client.downlink_request()? else {
            self.completed = true;
            return Ok(None);
        };
        let action = TransportAction {
            kind: "pull".into(),
            body,
        };
        self.active = Some(action.clone());
        Ok(Some(action))
    }
    /// Apply the answer to the action in flight and return what it could not
    /// apply, for the host to hand to the application.
    pub fn complete<S: ClientStore>(
        &mut self,
        client: &mut Client<S>,
        bytes: &[u8],
    ) -> Result<ApplyReport> {
        let action = self
            .active
            .clone()
            .ok_or_else(|| invalid("no transport action"))?;
        let report = if action.kind == "push" {
            let (sequence, receipt) = self.decode_push_receipt(bytes)?;
            let report = client.acknowledge(sequence, receipt)?;
            self.completed = false;
            report
        } else if let Some(task) = &self.history {
            let page = StreamBootstrapPage::decode(bytes)?;
            let applied = client.apply_stream_history_page(
                true,
                &task.state.stream,
                task.state.subscription_id,
                task.state.run,
                task.state.cursor,
                &page,
            )?;
            self.completed = false;
            match applied {
                BootstrapApply::Failed { .. } => {
                    self.active = None;
                    self.history = None;
                    return Err(invalid("stream reconciliation records failed"));
                }
                BootstrapApply::Applied { report, .. } | BootstrapApply::Detached { report } => {
                    report
                }
                BootstrapApply::Stale => ApplyReport::default(),
            }
        } else {
            let request = PullRequest::decode(action.body.as_bytes())?;
            let page = StreamPullPage::decode(bytes)?;
            if !answers_cursors(&page.cursors, &request) {
                return Err(invalid("response does not match pull request"));
            }
            let end = !page.cursors.values().any(CursorRange::continues);
            let page_streams = page.cursors.keys().cloned().collect::<Vec<_>>();
            let report = client.apply_stream_page(page)?;
            client.settle_bootstrap_barriers(&page_streams)?;
            if end {
                self.completed = !client.any_reconciliation_pending()?;
            }
            report
        };
        self.active = None;
        self.history = None;
        Ok(report)
    }
}

/// Whether a page answers a request: the same streams, each from the cursor
/// the request named.
fn answers(page: &PullPage, request: &PullRequest) -> bool {
    answers_cursors(&page.cursors, request)
}
fn answers_cursors(cursors: &BTreeMap<String, CursorRange>, request: &PullRequest) -> bool {
    cursors.len() == request.cursors.len()
        && cursors
            .iter()
            .all(|(c, r)| request.cursors.get(c) == Some(&r.from))
}

impl<S: ClientStore> Client<S> {
    /// One pull for every initialized subscription from its durable cursor;
    /// `None` when none is initialized, a subscription still waiting for its
    /// first boundary included. The downlink never borrows the mutation cycle:
    /// HTTP writes can progress independently.
    pub fn downlink_request(&mut self) -> Result<Option<String>> {
        let cursors: BTreeMap<String, u64> = self.subscriptions()?.into_iter().collect();
        if cursors.is_empty() {
            return Ok(None);
        }
        let request = PullRequest {
            cursors: cursors.clone(),
            models: self.declared_models(),
        };
        self.pulls.issue(&cursors);
        Ok(Some(
            String::from_utf8(with_capabilities(
                &request.encode()?,
                &[STREAM_AUTHORITY_CAPABILITY],
            )?)
            .map_err(|_| invalid("utf8"))?,
        ))
    }

    /// Whether a page answers a pull this client issued under an earlier
    /// subscription of one of its streams. Such a page is stale: the
    /// resubscribe reset the cursor and a fresh pull from it delivers everything.
    pub(crate) fn stale_subscription_page(&mut self, page: &PullPage) -> bool {
        let cursors = page
            .cursors
            .iter()
            .map(|(c, r)| (c.clone(), r.from))
            .collect();
        self.pulls.stale(&cursors)
    }

    pub(crate) fn admit_stream_downlink(
        &mut self,
        page: &StreamPullPage,
        request: Option<&PullRequest>,
    ) -> Result<DownlinkProgress> {
        page.validate()?;
        self.admit_downlink(
            &PullPage {
                cursors: page.cursors.clone(),
                changes: vec![],
            },
            request,
        )
    }
    pub(crate) fn receive_stream_downlink(
        &mut self,
        page: StreamPullPage,
        request: Option<PullRequest>,
    ) -> Result<DownlinkProgress> {
        page.validate()?;
        let mut progress = self.classify_downlink(
            &PullPage {
                cursors: page.cursors.clone(),
                changes: vec![],
            },
            request.as_ref(),
        )?;
        if progress.disposition == "applied" {
            progress.report = self.apply_stream_page(page)?;
        } else if !progress.report.stale && progress.disposition != "recover" && self.view(|e| Ok(e.scalar("SELECT 1 FROM axton_subscription WHERE reconcile_state='requested' AND reconcile_bound IS NULL LIMIT 1", &[])?.is_some()))? {
            self.write(|e| e.observe_stream_heads(&page, None))?;
        }
        Ok(progress)
    }

    /// One incoming path for HTTP catch-up and WebSocket frames. Optional
    /// request metadata only validates HTTP response identity; the per-stream
    /// cursor policy is shared. Whatever the page could not apply is in the
    /// report, never an error.
    pub fn receive_downlink(
        &mut self,
        page: PullPage,
        request: Option<PullRequest>,
    ) -> Result<DownlinkProgress> {
        let mut progress = self.classify_downlink(&page, request.as_ref())?;
        if progress.disposition == "applied" {
            progress.report = self.apply_current_page(page)?;
        }
        Ok(progress)
    }

    /// Classify a received page for runtime callback admission. Keep pull
    /// correlation available until its prepared authority commits.
    pub(crate) fn admit_downlink(
        &mut self,
        page: &PullPage,
        request: Option<&PullRequest>,
    ) -> Result<DownlinkProgress> {
        let pulls = self.pulls.clone();
        let result = self.classify_downlink(page, request);
        self.pulls = pulls;
        result
    }

    fn classify_downlink(
        &mut self,
        page: &PullPage,
        request: Option<&PullRequest>,
    ) -> Result<DownlinkProgress> {
        page.validate()?;
        if let Some(request) = request
            && !answers(page, request)
        {
            return Err(invalid("response does not match pull request"));
        }
        let continues: Vec<String> = page
            .cursors
            .iter()
            .filter(|(_, r)| r.continues())
            .map(|(c, _)| c.clone())
            .collect();
        let mut progress = DownlinkProgress {
            disposition: "covered",
            gaps: vec![],
            continues,
            report: ApplyReport::default(),
        };
        if self.stale_subscription_page(page) {
            progress.report.stale = true;
            return Ok(progress);
        }
        let subscribed = self.desired_streams()?;
        let mut live = false;
        for (stream, range) in &page.cursors {
            if !subscribed.contains(stream) {
                continue;
            }
            // An uninitialized subscription has no position to compare: its
            // first boundary is not committed, so this page moves nothing.
            let Some(cursor) = self.cursor(stream)? else {
                continue;
            };
            if range.to <= cursor {
                continue;
            }
            if range.from > cursor {
                progress.gaps.push(stream.clone());
            } else {
                live = true;
            }
        }
        if !progress.gaps.is_empty() {
            progress.disposition = "recover";
        } else if live {
            progress.disposition = "applied";
        }
        Ok(progress)
    }
}

/// What became of one incoming page: `applied`, `covered` (nothing new for
/// any subscribed stream), or `recover` (`gaps` names the streams whose
/// `from` is beyond the cursor; nothing was applied). `continues` names the
/// streams the page says hold more.
#[derive(Debug, Serialize)]
pub struct DownlinkProgress {
    pub disposition: &'static str,
    pub gaps: Vec<String>,
    pub continues: Vec<String>,
    pub report: ApplyReport,
}
