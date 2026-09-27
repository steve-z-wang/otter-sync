//! Whole-Scope Bootstrap: the durable load of everything published to a Scope
//! before its subscription's origin. The ledger lives in the same
//! `axton_subscription` row as the #150 identity and boundary, so a load is
//! bound to one registration and goes with it.
//!
//! Three positions matter. **S** is the subscription's `starting_cursor`, the
//! origin the first acknowledgement committed. **B** is `bootstrap_cursor`, how
//! far the historical scan of `(0, S]` has committed. **L** is the ordinary
//! delivery `cursor`. Bootstrap walks `(B, S]` and never writes S or L;
//! delivery moves L and never writes B. The terminal historical page fixes
//! **H**, the channel head its transaction observed, as a completion barrier:
//! the run completes once `B = S` and `L >= H`
//! ([#151](https://github.com/zanminwang/axton/issues/151)).
use crate::bootstrap_ledger::{LedgerIssue, Loaded};
use crate::store::ClientStore;
use crate::{ApplyReport, Client, Report, ReportKind};
use axton_core::{BootstrapPage, BootstrapRequest, Result, invalid};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The stable code a page whose records could not all be applied fails with.
pub const RECORDS_FAILED: &str = "bootstrap.records_failed";
/// The stable code a response that is not a page of the requested interval
/// fails with: an envelope neither side can attribute to a record.
pub const PROTOCOL_INVALID: &str = "bootstrap.protocol_invalid";
/// The stable code a request the server definitively refused fails with. A
/// transport failure is not this: it keeps the run and is retried.
pub const REQUEST_REJECTED: &str = "bootstrap.request_rejected";
/// The stable prefix every refusal of a registration this client no longer
/// holds carries. An engine error is a message, not a code, so this is what a
/// host has to recognize it by: the SDKs match it and raise their own
/// `subscription.closed` instead of the engine's text
/// ([`crate::bootstrap_ledger`]).
pub const SUBSCRIPTION_CLOSED: &str = "subscription.closed";
/// At most this many record summaries are kept in a stored failure.
pub const MAX_FAILURES: usize = 50;
/// A stored failure message is cut to this many UTF-8 bytes.
pub const MAX_MESSAGE: usize = 1024;

/// How far a Scope's historical load has got. The names are the stored column
/// values, and the phase of a Scope that never asked for one is
/// [`BootstrapPhase::NotRequested`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BootstrapPhase {
    /// No load was ever requested for this registration.
    NotRequested,
    /// Registered and waiting: for the subscription's origin, or for the
    /// scheduler's next page.
    Requested,
    /// At least one page committed and the interval is not finished.
    Loading,
    /// The interval is finished and the barrier H is fixed; ordinary delivery
    /// has not reached it yet.
    CatchingUp,
    /// `B = S` and `L >= H`: the historical interval and the fixed barrier are
    /// both processed.
    Complete,
    /// The run ended on a failure that is terminal until an explicit retry.
    Failed,
}
impl BootstrapPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotRequested => "not_requested",
            Self::Requested => "requested",
            Self::Loading => "loading",
            Self::CatchingUp => "catching_up",
            Self::Complete => "complete",
            Self::Failed => "failed",
        }
    }
    pub(crate) fn parse(text: &str) -> Result<Self> {
        Ok(match text {
            "not_requested" => Self::NotRequested,
            "requested" => Self::Requested,
            "loading" => Self::Loading,
            "catching_up" => Self::CatchingUp,
            "complete" => Self::Complete,
            "failed" => Self::Failed,
            other => return Err(invalid(format!("unknown bootstrap state {other}"))),
        })
    }
    /// Whether a run in this phase still has work the scheduler can issue or
    /// settle.
    pub(crate) fn active(self) -> bool {
        matches!(self, Self::Requested | Self::Loading | Self::CatchingUp)
    }
    /// Whether a page may be asked for. A run that is catching up has none to
    /// ask for: it waits for ordinary delivery to reach its barrier.
    pub(crate) fn schedulable(self) -> bool {
        matches!(self, Self::Requested | Self::Loading)
    }
}

/// One record of a failed page, in the bounded form the failure keeps: what it
/// was, not what it contained. Model payloads never enter the ledger.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapRecordFailure {
    pub model: String,
    pub identity: Value,
    pub stamp: u64,
    pub code: String,
}
impl BootstrapRecordFailure {
    /// The summary of one report that fails Bootstrap coverage, or `None` for
    /// one that does not: only `ReadFailed`, `Skipped` and `Conflict` do, so a
    /// `Diverged` pending-Action replay is never summarised here. A report the
    /// server attributed keeps its code; one this client made carries the kind
    /// that made it.
    fn of(report: &Report) -> Option<Self> {
        let kind = match report.kind {
            ReportKind::ReadFailed => "readFailed",
            ReportKind::Skipped => "skipped",
            ReportKind::Conflict => "conflict",
            ReportKind::Diverged => return None,
        };
        Some(Self {
            model: report.model.clone(),
            identity: report.identity.clone(),
            stamp: report.stamp,
            code: report.code.clone().unwrap_or_else(|| kind.to_string()),
        })
    }
}

/// Why a run failed, in the bounded form the row stores: a code, a message of
/// at most [`MAX_MESSAGE`] UTF-8 bytes and at most [`MAX_FAILURES`] record
/// summaries. [`BootstrapError::new`] is the only way to build one, so nothing
/// unbounded can be stored.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapError {
    pub code: String,
    pub message: String,
    pub records: Vec<BootstrapRecordFailure>,
}
impl BootstrapError {
    pub fn new(
        code: impl Into<String>,
        message: impl Into<String>,
        mut records: Vec<BootstrapRecordFailure>,
    ) -> Self {
        records.truncate(MAX_FAILURES);
        Self {
            code: code.into(),
            message: truncate(message.into(), MAX_MESSAGE),
            records,
        }
    }
}
/// Cut `text` to at most `bytes` UTF-8 bytes, on a character boundary: a
/// message is diagnostic text, never a place to lose a valid string.
pub(crate) fn truncate(text: String, bytes: usize) -> String {
    if text.len() <= bytes {
        return text;
    }
    let mut end = bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    text[..end].to_string()
}

/// One Scope's load, as the ledger holds it. `cursor` is B and `barrier` is H;
/// S and L stay in [`SubscriptionState`], which Bootstrap never writes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapState {
    pub scope: String,
    pub subscription_id: u64,
    pub state: BootstrapPhase,
    /// The retry fence: every call and every response belongs to one run.
    pub run: u64,
    /// B, the committed historical progress.
    pub cursor: u64,
    /// H, the head the terminal page observed; `None` until it commits.
    pub barrier: Option<u64>,
    pub error: Option<BootstrapError>,
}
impl BootstrapState {
    /// Refuse a row whose fields cannot have been written together: a barrier
    /// before the interval finished, or a failure without a failed run.
    pub(crate) fn coherent(&self) -> Result<()> {
        let barrier = matches!(
            self.state,
            BootstrapPhase::CatchingUp | BootstrapPhase::Complete | BootstrapPhase::Failed
        );
        if self.barrier.is_some() && !barrier {
            return Err(invalid(format!(
                "bootstrap barrier stored for {} in state {}",
                self.scope,
                self.state.as_str()
            )));
        }
        if self.error.is_some() && self.state != BootstrapPhase::Failed {
            return Err(invalid(format!(
                "bootstrap failure stored for {} in state {}",
                self.scope,
                self.state.as_str()
            )));
        }
        Ok(())
    }
}

/// What applying one historical page came to.
#[derive(Debug)]
pub enum BootstrapApply {
    /// The response does not answer the stored task: nothing was written.
    Stale,
    /// A record of the page could not be applied. The page's successful
    /// authority is committed, the interval did not advance, and the run is
    /// failed until an explicit retry. The report is still the caller's: the
    /// records that did apply, and every `Diverged` replay, stay observable.
    Failed {
        state: BootstrapState,
        report: ApplyReport,
    },
    /// The page's authority and its progress committed together.
    Applied {
        state: BootstrapState,
        report: ApplyReport,
    },
}
impl BootstrapApply {
    /// Whether the response answered no stored task, so nothing was written.
    pub fn is_stale(&self) -> bool {
        matches!(self, Self::Stale)
    }
    /// What the page applied, for either outcome that applied anything.
    pub fn report(&self) -> Option<&ApplyReport> {
        match self {
            Self::Stale => None,
            Self::Failed { report, .. } | Self::Applied { report, .. } => Some(report),
        }
    }
    /// The state the page committed, for either outcome that wrote one.
    pub fn state(&self) -> Option<&BootstrapState> {
        match self {
            Self::Stale => None,
            Self::Failed { state, .. } | Self::Applied { state, .. } => Some(state),
        }
    }
}

/// One schedulable run and the origin that bounds it: what the scheduler needs
/// to ask for the next page without a second read. S lives in the #150 half of
/// the row, so both halves are read in the one transaction that picked the task.
#[derive(Clone, Debug, PartialEq)]
pub struct BootstrapTask {
    pub state: BootstrapState,
    /// S, the subscription origin every page of this run is bounded by.
    pub origin: u64,
}
impl BootstrapTask {
    /// The page this run asks for next: `(B, S]` with the client's declared
    /// read contracts, as [`PullRequest`](axton_core::PullRequest) carries them.
    pub fn request(&self, models: std::collections::BTreeMap<String, u64>) -> BootstrapRequest {
        BootstrapRequest {
            channel: self.state.scope.clone(),
            models,
            after: self.state.cursor,
            until: self.origin,
        }
    }
}

impl<S: ClientStore> Client<S> {
    /// The next run to ask a page for, rotating: the first schedulable task in
    /// Scope order after `rotation`, or the first of all when that was the last
    /// one. One committed read, so the transaction is closed before the request
    /// leaves; a run that is catching up waits for delivery and is skipped.
    ///
    /// A stored row that cannot be decoded is skipped too, so it cannot stop
    /// every other run's pages; it is not repaired, and a named read of it
    /// ([`Client::bootstrap_state`], [`Client::bootstrap_tasks`]) still fails.
    /// The Downlink worker reports the rows it skipped to the application
    /// ([#163](https://github.com/zanminwang/axton/issues/163)).
    pub fn bootstrap_schedule(&mut self, rotation: Option<&str>) -> Result<Option<BootstrapTask>> {
        Ok(self.bootstrap_schedule_scan(rotation)?.0)
    }
    /// [`Client::bootstrap_schedule`] with an issue for every active row the
    /// read skipped because it cannot be decoded.
    pub(crate) fn bootstrap_schedule_scan(
        &mut self,
        rotation: Option<&str>,
    ) -> Result<(Option<BootstrapTask>, Vec<LedgerIssue>)> {
        self.view(|e| {
            let scan = e.bootstrap_task_scan()?;
            let tasks: Vec<BootstrapTask> = scan
                .rows
                .into_iter()
                .filter(|row| row.state.state.schedulable())
                .filter_map(|row| {
                    Some(BootstrapTask {
                        origin: row.subscription.starting_cursor?,
                        state: row.state,
                    })
                })
                .collect();
            let after = rotation
                .and_then(|last| tasks.iter().find(|t| t.state.scope.as_str() > last))
                .or_else(|| tasks.first());
            Ok((after.cloned(), scan.issues))
        })
    }
    /// Every run waiting for its barrier, in Scope order: what a reopen
    /// re-evaluates before it issues any request. Like the schedule, it skips a
    /// row that cannot be decoded - which therefore never supplies completion
    /// evidence - so one damaged registration cannot hold every other reached
    /// barrier open. The Downlink worker reports the rows it skipped.
    pub fn bootstrap_barriers(&mut self) -> Result<Vec<String>> {
        Ok(self.bootstrap_barriers_scan()?.0)
    }
    /// [`Client::bootstrap_barriers`] with an issue for every active row the
    /// read skipped because it cannot be decoded.
    pub(crate) fn bootstrap_barriers_scan(&mut self) -> Result<(Vec<String>, Vec<LedgerIssue>)> {
        self.view(|e| {
            let scan = e.bootstrap_task_scan()?;
            let waiting = scan
                .rows
                .into_iter()
                .filter(|row| row.state.state == BootstrapPhase::CatchingUp)
                .map(|row| row.state.scope)
                .collect();
            Ok((waiting, scan.issues))
        })
    }
    /// Register a durable load of everything published to `scope` before its
    /// origin, and answer with the stored state the call attached to.
    ///
    /// | Stored phase | Outcome |
    /// | --- | --- |
    /// | `not_requested` | `requested`, run + 1 |
    /// | `requested`, `loading`, `catching_up` | unchanged: the call shares the active run |
    /// | `complete` | unchanged: the call resolves locally, offline included |
    /// | `failed` | `requested`, run + 1, the failure and the barrier cleared, B retained |
    ///
    /// It is one local transaction and needs no connection: a Scope registered
    /// offline carries a requested load until #150 commits its origin.
    pub fn request_bootstrap(
        &mut self,
        scope: &str,
        subscription_id: u64,
    ) -> Result<BootstrapState> {
        // A call that changes nothing is answered from the committed reader, so
        // it neither bumps the client generation nor notifies a watcher. The
        // write re-reads the row inside its transaction, so a concurrent change
        // still wins.
        let stored = self.bootstrap_state(scope, subscription_id)?;
        if stored.state.active() || stored.state == BootstrapPhase::Complete {
            return Ok(stored);
        }
        self.write(|e| {
            let row = e.bootstrap_of(scope, subscription_id)?;
            let mut state = row.state;
            if state.state.active() || state.state == BootstrapPhase::Complete {
                return Ok(state);
            }
            let from_run = state.run;
            state.state = BootstrapPhase::Requested;
            state.run += 1;
            state.barrier = None;
            state.error = None;
            written(e.set_bootstrap(&state, from_run)?)?;
            e.mark_bootstrap(scope);
            Ok(state)
        })
    }
    /// The stored load state of the registration `subscription_id` names.
    pub fn bootstrap_state(&mut self, scope: &str, subscription_id: u64) -> Result<BootstrapState> {
        self.view(|e| Ok(e.bootstrap_of(scope, subscription_id)?.state))
    }
    /// The initialized runs with work left, in Scope order: what the scheduler
    /// rotates through, one page at a time.
    pub fn bootstrap_tasks(&mut self) -> Result<Vec<BootstrapState>> {
        self.view(|e| e.bootstrap_tasks())
    }
    /// Apply one historical page's authority and its progress in one
    /// transaction.
    ///
    /// The row is read again inside that transaction and the response is
    /// refused as [`BootstrapApply::Stale`] - writing nothing at all - unless
    /// it still answers the stored task: the same identity, the same run, a
    /// phase that is still loading, `bootstrap_cursor == expected_after`, and a
    /// page that echoes this Scope, that `from` and the stored origin. A
    /// protocol-invalid envelope is an error and commits nothing. Otherwise the
    /// records are staged by stamp; if any of them failed to read, validate or
    /// apply, the successful ones stay committed, the interval does not advance
    /// and the run is failed until an explicit retry. A `Diverged` replay is
    /// reported, not a coverage failure. On success B moves to `page.to`, and a
    /// terminal page fixes the barrier and completes the run as soon as
    /// delivery has reached it.
    pub fn apply_bootstrap_page(
        &mut self,
        scope: &str,
        subscription_id: u64,
        run: u64,
        expected_after: u64,
        page: &BootstrapPage,
    ) -> Result<BootstrapApply> {
        // Read first so a response that answers nothing opens no transaction;
        // the write repeats every test against the row it writes.
        let answered = |row: Option<&Loaded>| {
            row.is_some_and(|row| answers(row, subscription_id, run, expected_after, page))
        };
        if !self.view(|e| Ok(answered(e.bootstrap_row(scope)?.as_ref())))? {
            return Ok(BootstrapApply::Stale);
        }
        self.write(|e| {
            let Some(row) = e.bootstrap_row(scope)?.filter(|row| answered(Some(row))) else {
                return Ok(BootstrapApply::Stale);
            };
            validate(page, expected_after)?;
            let report = e.apply_records(&page.records)?;
            let mut state = row.state;
            let failures: Vec<BootstrapRecordFailure> = report
                .reports
                .iter()
                .filter_map(BootstrapRecordFailure::of)
                .collect();
            if !failures.is_empty() {
                // The authority that did apply stays: it is coverage the retry
                // need not fetch again. The continuation marker does not move,
                // so the retry revisits this page.
                state.state = BootstrapPhase::Failed;
                state.error = Some(BootstrapError::new(
                    RECORDS_FAILED,
                    format!(
                        "{} of {} records on the bootstrap page ({}, {}] for {scope} could not be applied",
                        failures.len(),
                        page.records.len(),
                        page.from,
                        page.to
                    ),
                    failures,
                ));
                written(e.set_bootstrap(&state, state.run)?)?;
                e.mark_bootstrap(scope);
                return Ok(BootstrapApply::Failed { state, report });
            }
            state.cursor = page.to;
            state.state = BootstrapPhase::Loading;
            if page.terminal() {
                state.barrier = Some(page.head);
                state.state = BootstrapPhase::CatchingUp;
                if row.subscription.cursor.is_some_and(|delivered| delivered >= page.head) {
                    state.state = BootstrapPhase::Complete;
                }
            }
            written(e.set_bootstrap(&state, state.run)?)?;
            e.mark_bootstrap(scope);
            Ok(BootstrapApply::Applied { state, report })
        })
    }
    /// Fail the run `run` names with a bounded error, keeping its progress and
    /// its barrier: how a caller records a failure the ledger cannot see for
    /// itself, such as a protocol-invalid response or a terminal transport
    /// refusal. `false` when the run is no longer the active one.
    pub fn fail_bootstrap(
        &mut self,
        scope: &str,
        subscription_id: u64,
        run: u64,
        error: BootstrapError,
    ) -> Result<bool> {
        self.write(|e| {
            let row = e.bootstrap_of(scope, subscription_id)?;
            let mut state = row.state;
            if state.run != run || !state.state.active() {
                return Ok(false);
            }
            state.state = BootstrapPhase::Failed;
            state.error = Some(error);
            written(e.set_bootstrap(&state, run)?)?;
            e.mark_bootstrap(scope);
            Ok(true)
        })
    }
    /// Complete every named run whose fixed barrier ordinary delivery has
    /// reached, in one transaction, and answer with the states it committed.
    /// The caller runs it after committed delivery progress; a Scope that is
    /// not catching up, or is still behind its barrier, contributes nothing.
    /// An empty list names no Scope and settles nothing.
    ///
    /// Which Scopes are settleable is decided by committed reads, in chunks of
    /// at most 900 names, barrier and delivery cursor included - each chunk is
    /// its own read, with no snapshot across them, which the write's own fence
    /// makes harmless - so a run still short of its barrier opens no
    /// transaction at all: waiting out a barrier must not commit an empty
    /// write - and bump the client generation - once per delivered page. The names are a set, however many there are, and a
    /// candidate whose stored row cannot be decoded is left out, so it
    /// supplies no completion evidence and cannot hold the others open; the
    /// Downlink worker reports the candidates it left out
    /// ([#163](https://github.com/zanminwang/axton/issues/163)).
    pub fn settle_bootstrap_barriers(&mut self, scopes: &[String]) -> Result<Vec<BootstrapState>> {
        Ok(self.settle_bootstrap_barriers_scan(scopes)?.0)
    }
    /// [`Client::settle_bootstrap_barriers`] with an issue for every candidate
    /// it left out because its row cannot be decoded. The committed reads
    /// decide which channels enter the write - only a candidate whose whole
    /// row decoded does, so a damaged one is never written - and whether a
    /// write is worth opening at all; an empty set reads and writes nothing.
    /// The write then re-reads and fences each healthy candidate itself.
    pub(crate) fn settle_bootstrap_barriers_scan(
        &mut self,
        channels: &[String],
    ) -> Result<(Vec<BootstrapState>, Vec<LedgerIssue>)> {
        let scan = self.view(|e| e.settleable_scan(channels))?;
        if scan.rows.is_empty() {
            return Ok((vec![], scan.issues));
        }
        let settled = self.write(|e| {
            let mut settled = vec![];
            for scope in &scan.rows {
                settled.extend(e.settle_barrier(scope)?);
            }
            Ok(settled)
        })?;
        Ok((settled, scan.issues))
    }
}

/// The row was read in this transaction, and one writer holds it: a write that
/// finds nothing means the file changed underneath, so the transaction is
/// refused rather than half applied.
fn written(affected: bool) -> Result<()> {
    if affected {
        return Ok(());
    }
    Err(invalid("the bootstrap row changed during the transaction"))
}
/// Whether a response still answers the stored task. Every test is against
/// what is committed now, so a response that survived an unsubscribe, a retry,
/// a reopen or a rebuild cannot recreate or complete a task that replaced it.
fn answers(
    row: &Loaded,
    subscription_id: u64,
    run: u64,
    expected_after: u64,
    page: &BootstrapPage,
) -> bool {
    row.subscription.subscription_id == subscription_id
        && row.state.run == run
        && matches!(
            row.state.state,
            BootstrapPhase::Requested | BootstrapPhase::Loading
        )
        && row.state.cursor == expected_after
        && page.channel == row.state.scope
        && page.from == expected_after
        && row.subscription.starting_cursor == Some(page.until)
}
/// The envelope rules a page must satisfy before any authority is written:
/// `from <= to <= until <= head` and safe counters ([`BootstrapPage::validate`]),
/// plus progress on every page that does not finish the interval.
fn validate(page: &BootstrapPage, expected_after: u64) -> Result<()> {
    page.validate()?;
    if !page.terminal() && page.to == expected_after {
        return Err(invalid(
            "a bootstrap page that does not finish the interval must advance it",
        ));
    }
    Ok(())
}
