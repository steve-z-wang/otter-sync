//! The shared Load worker ([#173](https://github.com/zanminwang/axton/issues/173)):
//! one per client runtime, a state machine that decides which frozen pages go
//! out together, when a page that failed may go again, and in which order the
//! answers are applied. It holds no thread, no timer and no transport: the
//! runtime executes what it decides as effects, one local unit at a time.
//!
//! - **Batches.** A dispatch reads the oldest ready pages
//!   ([`Client::load_ready_pages`]) and sends at most
//!   [`LOAD_BATCH_ITEMS`] of them, within the request byte bound, as one
//!   batch. Nothing waits for a batch to fill. At most [`LOAD_BATCHES`]
//!   batches are out at once.
//! - **One page per job.** A job with a page in flight - requested, answered
//!   and waiting to apply, or applying - is skipped by every dispatch.
//! - **Bounded answers.** A batch keeps its slot until every one of its
//!   outcomes was consumed: applied, recorded as a failure or found stale. At
//!   most `LOAD_BATCHES * LOAD_BATCH_ITEMS` outcomes wait for the writer, and
//!   while they do, no further request is sent.
//! - **Backoff.** A retryable failure keeps its call ID and waits
//!   [`load_backoff`] of its persisted attempt count; a job with persisted
//!   attempts that this worker has not seen fail (a reopen) waits a fresh
//!   bounded delay. A job that waits never holds back a ready one.
//! - **Unsendable and rejected pages.** A frozen page that cannot be sent
//!   even alone (over the request bound) fails its job terminally
//!   (`load.request_too_large`) instead of blocking the jobs behind it. A
//!   whole request the backend refused ([`LoadWorker::rejected`]) is split:
//!   each of its pages goes alone next time, and a page refused alone fails
//!   its job as `load.protocol_invalid`.
use crate::load_ledger::LoadLedgerIssue;
use crate::loads::{LoadFailure, LoadFence, LoadJobError, PROTOCOL_INVALID, REQUEST_TOO_LARGE};
use crate::{Client, ClientStore};
use axton_core::{
    LoadBatchRequest, LoadBatchResponse, LoadPageReply, Result, canonical_json, limits,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// Pages in one batch request.
pub const LOAD_BATCH_ITEMS: usize = limits::LOAD_BATCH_ITEMS;
/// Batch requests out at once.
pub const LOAD_BATCHES: usize = 2;
/// The delay after the first failed attempt of a page.
pub const LOAD_BACKOFF_BASE_MS: u64 = 1_000;
/// No delay exceeds this, jitter included.
pub const LOAD_BACKOFF_CAP_MS: u64 = 30_000;

/// Why a request of one page cannot be sent, decided from the request's own
/// canonical size: over the request bound, or refused for its content.
pub(crate) fn unsendable(request: &LoadBatchRequest) -> &'static str {
    let size = serde_json::to_value(request)
        .ok()
        .and_then(|value| canonical_json(&value).ok())
        .map(|text| text.len());
    match size {
        Some(size) if size > limits::LOAD_REQUEST_BYTES => REQUEST_TOO_LARGE,
        _ => PROTOCOL_INVALID,
    }
}
/// How long a page that failed `attempts` times waits before it is sent
/// again: 1 s doubling per attempt, with the lanes' ±20 % jitter from host
/// entropy, never more than 30 s.
pub fn load_backoff(attempts: u64, entropy: u64) -> u64 {
    let doublings = attempts.saturating_sub(1).min(5);
    let base = (LOAD_BACKOFF_BASE_MS << doublings).min(LOAD_BACKOFF_CAP_MS);
    (base * (800 + entropy % 401) / 1000).min(LOAD_BACKOFF_CAP_MS)
}

/// One page of a batch: the fence its answer must pass and the attempt count
/// it was sent with.
#[derive(Clone, Debug, PartialEq)]
pub struct LoadSent {
    pub fence: LoadFence,
    pub attempts: u64,
}

/// A batch the worker decided to send.
#[derive(Clone, Debug, PartialEq)]
pub struct LoadDispatch {
    pub batch: u64,
    /// The exact canonical request body, sent (and resent) unchanged.
    pub body: String,
    pub pages: Vec<LoadSent>,
}

/// What one dispatch decided.
#[derive(Debug, Default)]
pub struct LoadDispatchStep {
    /// The batch to send, if any page was ready.
    pub dispatch: Option<LoadDispatch>,
    /// Stored rows first seen undecodable by this read; each is reported once.
    pub issues: Vec<LoadLedgerIssue>,
}

/// What an answered or failed page asks the runtime to do.
#[derive(Clone, Debug)]
pub enum LoadAnswer {
    /// A correlated page answer for [`Client::load_page_step`].
    Reply(LoadPageReply),
    /// A failure the page shares with its whole request: a transport
    /// failure, an envelope that did not correlate, a refused credential.
    Failure(LoadFailure),
}

/// One outcome waiting for the writer.
#[derive(Clone, Debug)]
pub struct LoadReceived {
    pub batch: u64,
    pub sent: LoadSent,
    pub answer: LoadAnswer,
}

/// One dispatch step's slot: the pages it sent as one request, and the
/// pages it failed because they cannot be sent. Both count against the slot
/// until their outcomes are consumed, so unsendable failures are bounded
/// like answers.
struct Batch {
    request: LoadBatchRequest,
    body: String,
    /// The pages sent; empty when the step only failed unsendable pages.
    pages: Vec<LoadSent>,
    /// Whether the answer (or the failure) of the request was taken; true
    /// from the start when nothing was sent.
    answered: bool,
    /// Pages, sent or unsendable, whose outcome was not consumed yet.
    unconsumed: usize,
}

/// Why a page with persisted attempts waits, and until when.
struct Backoff {
    call_id: String,
    attempts: u64,
    due: u64,
}

/// The worker's decisions; see the module documentation.
#[derive(Default)]
pub struct LoadWorker {
    batches: BTreeMap<u64, Batch>,
    issued: u64,
    /// Jobs with a page in flight, by the batch that carries it.
    flight: BTreeMap<String, u64>,
    backoff: BTreeMap<String, Backoff>,
    /// Rows reported undecodable; skipped by every later read.
    damaged: BTreeSet<String>,
    /// Frozen pages (job to call ID) that go out only in a request of their
    /// own, because a request carrying them with others was refused whole.
    solo: BTreeMap<String, String>,
    /// A scheduler read failed: look again at this time.
    rescan: Option<u64>,
    outcomes: VecDeque<LoadReceived>,
    /// A dispatch may find ready work.
    dirty: bool,
}

impl LoadWorker {
    /// Something may have become ready: look again on the next dispatch.
    pub fn wake(&mut self) {
        self.dirty = true;
    }
    /// Whether a dispatch is wanted and a batch slot is free.
    pub fn wants_dispatch(&self) -> bool {
        self.dirty && self.batches.len() < LOAD_BATCHES
    }
    /// Whether an outcome waits for the writer.
    pub fn has_outcome(&self) -> bool {
        !self.outcomes.is_empty()
    }
    /// Outcomes waiting for the writer.
    pub fn waiting_outcomes(&self) -> usize {
        self.outcomes.len()
    }
    /// Batches holding a slot.
    pub fn batches(&self) -> usize {
        self.batches.len()
    }
    /// Whether `load_id` has a page in flight.
    pub fn in_flight(&self, load_id: &str) -> bool {
        self.flight.contains_key(load_id)
    }
    /// Whether `load_id` waits for its backoff to pass at `now`.
    pub fn backing_off(&self, load_id: &str, now: u64) -> bool {
        self.backoff.get(load_id).is_some_and(|b| b.due > now)
    }
    /// The earliest time a job that backs off may go again, if one waits.
    pub fn next_due(&self, now: u64) -> Option<u64> {
        self.backoff
            .values()
            .map(|b| b.due)
            .chain(self.rescan)
            .filter(|due| *due > now)
            .min()
    }
    /// A scheduler read failed: dispatch looks again after one bounded
    /// delay, which [`LoadWorker::next_due`] keeps until a dispatch runs.
    pub fn scan_failed(&mut self, now: u64, entropy: u64) {
        self.rescan = Some(now.saturating_add(load_backoff(1, entropy)));
    }
    /// The exact body of a batch whose request is still unanswered.
    pub fn unanswered(&self, batch: u64) -> Option<&str> {
        self.batches
            .get(&batch)
            .filter(|b| !b.answered)
            .map(|b| b.body.as_str())
    }

    /// One dispatch: the oldest ready pages that are not in flight, backing
    /// off or damaged, at most [`LOAD_BATCH_ITEMS`] and within the request
    /// byte bound, become one batch. A page with persisted attempts this
    /// worker has no backoff for starts one now and is left for later. After
    /// a batch the worker stays dirty, so the next dispatch looks again.
    pub fn dispatch<S: ClientStore>(
        &mut self,
        client: &mut Client<S>,
        now: u64,
        entropy: u64,
    ) -> Result<LoadDispatchStep> {
        self.dirty = false;
        self.rescan = None;
        let mut step = LoadDispatchStep::default();
        if self.batches.len() >= LOAD_BATCHES {
            return Ok(step);
        }
        let mut request = LoadBatchRequest { loads: vec![] };
        let mut pages: Vec<LoadSent> = vec![];
        // Pages that cannot be sent even alone, and why; they share the
        // step's slot and its bound of LOAD_BATCH_ITEMS pages.
        let mut unsent: Vec<(LoadSent, LoadFailure)> = vec![];
        // Pages taken (sent, or failed as unsendable), and pages left for a
        // request of their own.
        let mut picked = BTreeSet::new();
        let mut held = BTreeSet::new();
        let mut full = false;
        loop {
            let mut skip: BTreeSet<String> = self.flight.keys().cloned().collect();
            skip.extend(
                self.backoff
                    .iter()
                    .filter(|(_, b)| b.due > now)
                    .map(|(id, _)| id.clone()),
            );
            skip.extend(self.damaged.iter().cloned());
            skip.extend(picked.iter().cloned());
            skip.extend(held.iter().cloned());
            let schedule =
                client.load_ready_pages(LOAD_BATCH_ITEMS - pages.len() - unsent.len(), &skip)?;
            for issue in schedule.issues {
                self.damaged.insert(issue.load_id.clone());
                step.issues.push(issue);
            }
            let found = schedule.pages.len();
            // A page of this read was set aside: read again to fill the batch.
            let mut deferred = false;
            for page in schedule.pages {
                let id = page.fence.load_id.clone();
                match self.backoff.get(&id) {
                    Some(b) if b.call_id == page.fence.call_id && b.attempts == page.attempts => {}
                    _ if page.attempts > 0 => {
                        self.backoff.insert(
                            id,
                            Backoff {
                                call_id: page.fence.call_id.clone(),
                                attempts: page.attempts,
                                due: now.saturating_add(load_backoff(page.attempts, entropy)),
                            },
                        );
                        deferred = true;
                        continue;
                    }
                    _ => {
                        self.backoff.remove(&id);
                    }
                }
                let alone = self.solo.get(&id) == Some(&page.fence.call_id);
                if alone && !pages.is_empty() {
                    held.insert(id);
                    deferred = true;
                    continue;
                }
                request.loads.push(page.intent.clone());
                let sent = LoadSent {
                    fence: page.fence,
                    attempts: page.attempts,
                };
                match request.encode().and_then(|bytes| {
                    let capable = axton_core::with_capabilities(
                        &bytes,
                        &[axton_core::CHANNEL_MEMBERSHIP_CAPABILITY],
                    )?;
                    if request.loads.len() == 1 {
                        // Frozen single pages retain the logical limit across negotiation upgrades.
                        axton_core::check_request_size(&capable, limits::LOAD_REQUEST_BYTES)?;
                    } else if capable.len() > limits::LOAD_REQUEST_BYTES {
                        return Err(axton_core::invalid("Load request exceeds byte limit"));
                    }
                    Ok(capable)
                }) {
                    Ok(_) => {}
                    Err(error) if pages.is_empty() => {
                        // Not even alone: this job fails, the others go on.
                        let code = unsendable(&request);
                        request.loads.pop();
                        unsent.push((
                            sent,
                            LoadFailure::Local(LoadJobError::new(
                                code,
                                format!("the frozen Load page cannot be sent: {error}"),
                                vec![],
                            )),
                        ));
                        picked.insert(id);
                        deferred = true;
                        if unsent.len() == LOAD_BATCH_ITEMS {
                            full = true;
                            break;
                        }
                        continue;
                    }
                    Err(_) => {
                        // The request bound is reached: this page goes with
                        // the next batch.
                        request.loads.pop();
                        full = true;
                        break;
                    }
                }
                picked.insert(id);
                pages.push(sent);
                if alone {
                    full = true;
                    break;
                }
            }
            if full || pages.len() + unsent.len() == LOAD_BATCH_ITEMS || !deferred || found == 0 {
                break;
            }
        }
        if !held.is_empty() {
            // A page that must go alone waits for the next dispatch.
            self.dirty = true;
        }
        if pages.is_empty() && unsent.is_empty() {
            return Ok(step);
        }
        let body = if pages.is_empty() {
            String::new()
        } else {
            String::from_utf8(axton_core::with_capabilities(
                &request.encode()?,
                &[axton_core::CHANNEL_MEMBERSHIP_CAPABILITY],
            )?)
            .map_err(|_| axton_core::invalid("Load request is not UTF-8"))?
        };
        self.issued += 1;
        let batch = self.issued;
        for page in pages.iter().chain(unsent.iter().map(|(sent, _)| sent)) {
            self.flight.insert(page.fence.load_id.clone(), batch);
        }
        self.batches.insert(
            batch,
            Batch {
                request,
                body: body.clone(),
                pages: pages.clone(),
                answered: pages.is_empty(),
                unconsumed: pages.len() + unsent.len(),
            },
        );
        for (sent, failure) in unsent {
            self.outcomes.push_back(LoadReceived {
                batch,
                sent,
                answer: LoadAnswer::Failure(failure),
            });
        }
        // More may be ready than one step holds.
        self.dirty = true;
        if !pages.is_empty() {
            step.dispatch = Some(LoadDispatch { batch, body, pages });
        }
        Ok(step)
    }

    /// The request of `batch` was answered with `body`: every correlated page
    /// waits for the writer. An envelope that does not correlate applies
    /// nothing; each page keeps its frozen call and is retried as a transport
    /// failure. `Err` carries why the envelope was refused.
    pub fn answered(&mut self, batch: u64, body: &[u8]) -> std::result::Result<(), String> {
        let Some(entry) = self.batches.get_mut(&batch).filter(|b| !b.answered) else {
            return Ok(());
        };
        entry.answered = true;
        match LoadBatchResponse::decode(body, &entry.request) {
            Ok(replies) => {
                for reply in replies {
                    let Some(sent) = entry
                        .pages
                        .iter()
                        .find(|p| p.fence.load_id == reply.load_id)
                    else {
                        continue;
                    };
                    self.outcomes.push_back(LoadReceived {
                        batch,
                        sent: sent.clone(),
                        answer: LoadAnswer::Reply(reply),
                    });
                }
                Ok(())
            }
            Err(error) => {
                let message = format!("invalid Load response: {error}");
                for sent in entry.pages.clone() {
                    self.outcomes.push_back(LoadReceived {
                        batch,
                        sent,
                        answer: LoadAnswer::Failure(LoadFailure::transport(message.clone())),
                    });
                }
                Err(message)
            }
        }
    }
    /// The request of `batch` failed as a whole: every page records
    /// `failure`.
    pub fn failed(&mut self, batch: u64, failure: LoadFailure) {
        let Some(entry) = self.batches.get_mut(&batch).filter(|b| !b.answered) else {
            return;
        };
        entry.answered = true;
        for sent in entry.pages.clone() {
            self.outcomes.push_back(LoadReceived {
                batch,
                sent,
                answer: LoadAnswer::Failure(failure.clone()),
            });
        }
    }
    /// The backend refused the whole request of `batch` (a 4xx other than
    /// 401, 408 and 429). A request of several pages is abandoned without a
    /// failure and each of its pages goes alone next time; a page refused
    /// alone fails its job as `load.protocol_invalid`.
    pub fn rejected(&mut self, batch: u64, message: &str) {
        let Some(entry) = self.batches.get(&batch).filter(|b| !b.answered) else {
            return;
        };
        if entry.pages.len() == 1 {
            let failure = LoadFailure::Local(LoadJobError::new(
                PROTOCOL_INVALID,
                format!("the backend refused the Load request: {message}"),
                vec![],
            ));
            return self.failed(batch, failure);
        }
        for sent in &entry.pages {
            self.solo
                .insert(sent.fence.load_id.clone(), sent.fence.call_id.clone());
        }
        self.abandon(batch);
    }
    /// The request of `batch` was abandoned (pause, stop) before an answer:
    /// its pages are released without a failure, so no backoff follows, and
    /// the slot is released once the step's unsendable failures, if any,
    /// were consumed. An answered batch is left alone.
    pub fn abandon(&mut self, batch: u64) {
        let Some(entry) = self.batches.get_mut(&batch).filter(|b| !b.answered) else {
            return;
        };
        for sent in std::mem::take(&mut entry.pages) {
            if self.flight.get(&sent.fence.load_id) == Some(&batch) {
                self.flight.remove(&sent.fence.load_id);
            }
            entry.unconsumed = entry.unconsumed.saturating_sub(1);
        }
        entry.answered = true;
        entry.request.loads.clear();
        if entry.unconsumed == 0 {
            self.batches.remove(&batch);
        }
        self.dirty = true;
    }
    /// Batches whose request has not been answered.
    pub fn unanswered_batches(&self) -> Vec<u64> {
        self.batches
            .iter()
            .filter(|(_, b)| !b.answered)
            .map(|(id, _)| *id)
            .collect()
    }
    /// The next outcome for the writer, oldest first. Its page stays in flight
    /// until [`LoadWorker::consumed`].
    pub fn next_outcome(&mut self) -> Option<LoadReceived> {
        self.outcomes.pop_front()
    }
    /// An outcome that needs one more unit - a failure found while storing -
    /// goes back to the front.
    pub fn requeue(&mut self, outcome: LoadReceived) {
        self.outcomes.push_front(outcome);
    }
    /// The outcome of `load_id` in `batch` was applied, recorded or found
    /// stale: the page leaves flight, and the batch releases its slot once
    /// every outcome of it was consumed.
    pub fn consumed(&mut self, batch: u64, load_id: &str) {
        if self.flight.get(load_id) == Some(&batch) {
            self.flight.remove(load_id);
        }
        let Some(entry) = self.batches.get_mut(&batch) else {
            return;
        };
        entry.unconsumed = entry.unconsumed.saturating_sub(1);
        if entry.unconsumed == 0 && entry.answered {
            self.batches.remove(&batch);
            self.dirty = true;
        }
    }
    /// A retryable failure of the frozen call `call_id` was recorded as its
    /// `attempts`th: it waits until `now + load_backoff(attempts)`.
    pub fn back_off(
        &mut self,
        load_id: &str,
        call_id: &str,
        attempts: u64,
        now: u64,
        entropy: u64,
    ) {
        self.backoff.insert(
            load_id.to_string(),
            Backoff {
                call_id: call_id.to_string(),
                attempts,
                due: now.saturating_add(load_backoff(attempts, entropy)),
            },
        );
    }
    /// The job moved on (a committed page, a terminal failure, a cancel, a
    /// retry, a forget): no backoff of it applies any more.
    pub fn settled(&mut self, load_id: &str) {
        self.backoff.remove(load_id);
        self.solo.remove(load_id);
    }
    /// A retry answered `call_id` as the job's frozen call: when that is the
    /// call already waiting (an idempotent retry of active work), its backoff
    /// and its request of its own stay; a new call starts clean.
    pub fn retried(&mut self, load_id: &str, call_id: Option<&str>) {
        if self.backoff.get(load_id).map(|b| b.call_id.as_str()) != call_id {
            self.backoff.remove(load_id);
        }
        if self.solo.get(load_id).map(String::as_str) != call_id {
            self.solo.remove(load_id);
        }
    }
    /// The replica was replaced or the runtime closed: nothing held belongs
    /// to the current ledger. The next dispatch starts from the ledger.
    pub fn reset(&mut self) {
        *self = Self {
            issued: self.issued,
            dirty: true,
            ..Default::default()
        };
    }
}
