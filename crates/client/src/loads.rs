//! Native durable Load jobs ([#173](https://github.com/zanminwang/axton/issues/173)).
//!
//! A job fills local Models in successive pages until the application backend
//! reports completion. This module owns the job's durable lifecycle - start
//! (with the opt-in once mapping), get, list, cancel, retry, forget and
//! invalidation - and the atomic application of one page. The runtime's Load
//! worker schedules pages from [`Client::load_ready_pages`] and hands each
//! answer back through [`Client::load_page_step`]; this module never talks to
//! a transport.
//!
//! One job row carries its committed continuation, run generation, page count
//! and exactly one frozen page request (call ID plus canonical intent) while it
//! is pending. A page's authority, the writes of its `onStore` hooks and its
//! progress commit in one local transaction or not at all; a failure is
//! recorded afterwards in its own short transaction.
use crate::load_ledger::{LoadLedgerIssue, frozen_intent};
use crate::store::ClientStore;
use crate::{ApplyReport, Client, Report, ReportKind, StoreDelivery, StoreResult};
use axton_core::{
    LoadBatchRequest, LoadError, LoadIntent, LoadItemError, LoadItemErrorKind, LoadNext,
    LoadOutcome, LoadPageReply, LoadPageResponse, Result, Schema, canonical_json, invalid, limits,
    normalize_load_args, validate_load_models,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

/// Load management that would write is refused while an incompatible rebuild
/// waits for the old file's Mutations to drain.
pub const SCHEMA_PENDING: &str = "load.schema_pending";
/// `refresh` without `once`, or a `list` limit outside `1..=100`.
pub const INVALID_OPTIONS: &str = "load.invalid_options";
/// The named job is not in this replica's ledger.
pub const NOT_FOUND: &str = "load.not_found";
/// The job's frozen Load version is no longer retained by the schema.
pub const CONTRACT_UNAVAILABLE: &str = "load.contract_unavailable";
/// A record of a page could not be applied; the whole page rolled back.
pub const STORE_FAILED: &str = "load.store_failed";
/// An `onStore` hook failed; the whole page rolled back.
pub const HOOK_FAILED: &str = "load.hook_failed";
/// A correlated page whose content breaks the Load contract.
pub const PROTOCOL_INVALID: &str = "load.protocol_invalid";
/// A frozen page request that cannot be sent even alone: over the request
/// byte bound.
pub const REQUEST_TOO_LARGE: &str = "load.request_too_large";
/// A correlated page over the page byte or identity bound.
pub const PAGE_TOO_LARGE: &str = "load.page_too_large";
/// A correlated page whose `next` state is not bounded portable JSON.
pub const INVALID_CONTINUATION: &str = "load.invalid_continuation";
/// The error a cancelled job carries.
pub const CANCELLED: &str = "load.cancelled";
/// Forget names a job that is still active.
pub const NOT_TERMINAL: &str = "load.not_terminal";
/// Retry names a job that completed or was cancelled: that needs a new start.
pub const NOT_RETRYABLE: &str = "load.not_retryable";
/// A once mapping names a job that does not exist, or a stored job row
/// cannot be decoded.
pub const LEDGER_INVALID: &str = "load.ledger_invalid";
/// The schema declares no Load of this name (and version).
pub const UNKNOWN: &str = "load.unknown";
/// At most this many record diagnostics are kept in a stored failure.
pub const MAX_DIAGNOSTICS: usize = 20;
/// The most jobs one `list` answers.
pub const MAX_LIST: usize = 100;
/// Undecodable rows one scheduler read reports at most. The read also stops
/// after `limit + skip + MAX_SCAN_ISSUES` rows, so its cost is bounded by
/// what the caller asked for, whatever the ledger holds.
pub const MAX_SCAN_ISSUES: usize = 20;
/// Revision of the once key derivation. Changing it makes every earlier
/// mapping unreachable.
pub const LOAD_ONCE_FORMAT: u64 = 1;

fn coded(code: &str, message: impl std::fmt::Display) -> axton_core::Error {
    invalid(format!("{code}: {message}"))
}

/// A job's phase. The ledger stores only `pending`, `complete`, `failed` and
/// `cancelled`; `loading` and `waiting` are runtime projections of a pending
/// job (a request or application in flight, or offline/backoff).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LoadPhase {
    Pending,
    Loading,
    Waiting,
    Complete,
    Failed,
    Cancelled,
}
impl LoadPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Loading => "loading",
            Self::Waiting => "waiting",
            Self::Complete => "complete",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
    pub(crate) fn parse_durable(text: &str) -> Result<Self> {
        Ok(match text {
            "pending" => Self::Pending,
            "complete" => Self::Complete,
            "failed" => Self::Failed,
            "cancelled" => Self::Cancelled,
            other => return Err(invalid(format!("unknown stored Load phase {other}"))),
        })
    }
    /// Whether the job still has pages to load.
    pub fn active(self) -> bool {
        matches!(self, Self::Pending | Self::Loading | Self::Waiting)
    }
}

/// Why the current frozen page is being sent again with the same call ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LoadRetryClass {
    /// The request did not complete: transport failure, status or deadline.
    Transport,
    /// The backend rolled the item back (`retryable`); nothing was saved.
    Backend,
    /// Local preparation or commit failed; nothing was committed.
    Local,
}
impl LoadRetryClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Transport => "transport",
            Self::Backend => "backend",
            Self::Local => "local",
        }
    }
    pub(crate) fn parse(text: &str) -> Result<Self> {
        Ok(match text {
            "transport" => Self::Transport,
            "backend" => Self::Backend,
            "local" => Self::Local,
            other => return Err(invalid(format!("unknown stored Load retry class {other}"))),
        })
    }
}

/// Call-site controls of a Load start. They never reach the backend.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LoadOptions {
    /// Reuse the job mapped to the canonical key, or register a new one.
    pub once: bool,
    /// With `once`: join an active mapped job, otherwise replace the mapping
    /// with a fresh first-page job.
    pub refresh: bool,
}

/// One record of a failed page, in the bounded form a failure keeps.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LoadDiagnostic {
    pub model: String,
    pub id: Value,
    pub code: String,
}
impl LoadDiagnostic {
    /// The summary of a report that fails a page. A `Diverged` pending replay
    /// does not fail it, as for Bootstrap under D10.
    fn of(report: &Report) -> Option<Self> {
        let code = match report.kind {
            ReportKind::ReadFailed => report.code.clone().unwrap_or_else(|| "readFailed".into()),
            ReportKind::Skipped => "skipped".into(),
            ReportKind::Conflict => "conflict".into(),
            ReportKind::Diverged => return None,
        };
        Some(Self {
            model: report.model.clone(),
            id: report.identity.clone(),
            code,
        })
    }
}

/// A job's stored failure: a code, a message of at most
/// [`limits::LOAD_ERROR_MESSAGE_BYTES`] and at most [`MAX_DIAGNOSTICS`]
/// record diagnostics. [`LoadJobError::new`] is the only way to build one.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LoadJobError {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub diagnostics: Vec<LoadDiagnostic>,
}
impl LoadJobError {
    pub fn new(
        code: impl Into<String>,
        message: impl Into<String>,
        mut diagnostics: Vec<LoadDiagnostic>,
    ) -> Self {
        diagnostics.truncate(MAX_DIAGNOSTICS);
        Self {
            code: code.into(),
            message: crate::bootstrap::truncate(message.into(), limits::LOAD_ERROR_MESSAGE_BYTES),
            diagnostics,
        }
    }
    /// The public `{code, message}` a status carries.
    pub fn public(&self) -> LoadError {
        LoadError {
            code: self.code.clone(),
            message: self.message.clone(),
        }
    }
}

/// The management snapshot of one job: `{id, name, version, phase, pages, error}`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoadStatus {
    pub id: String,
    pub name: String,
    pub version: u64,
    pub phase: LoadPhase,
    pub pages: u64,
    pub error: Option<LoadError>,
}

/// One stored job as the ledger holds it.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LoadJob {
    pub id: String,
    pub name: String,
    pub version: u64,
    /// Canonical normalized business arguments; immutable.
    pub args: Value,
    /// The local read contract of every output Model; immutable.
    pub models: BTreeMap<String, u64>,
    /// The committed continuation: `None` before the first page (and after
    /// the last), `Some` for every later request, `{state: null}` included.
    pub continuation: LoadNext,
    /// The retry fence: an explicit retry starts run + 1.
    pub run: u64,
    pub phase: LoadPhase,
    /// Committed pages, empty pages included.
    pub pages: u64,
    /// The frozen page call ID: present while pending, kept by a failure.
    pub call_id: Option<String>,
    /// The exact frozen page request that call ID sends.
    pub intent: Option<LoadIntent>,
    /// Why the current frozen page is being resent, if it is.
    pub retry: Option<LoadRetryClass>,
    /// Retryable attempts of the current frozen page.
    pub attempts: u64,
    pub error: Option<LoadJobError>,
}
impl LoadJob {
    pub fn status(&self) -> LoadStatus {
        LoadStatus {
            id: self.id.clone(),
            name: self.name.clone(),
            version: self.version,
            phase: self.phase,
            pages: self.pages,
            error: self.error.as_ref().map(LoadJobError::public),
        }
    }
    /// Whether `fence` names this job's current frozen page.
    pub fn answers(&self, fence: &LoadFence) -> bool {
        self.id == fence.load_id
            && self.run == fence.run
            && self.phase == LoadPhase::Pending
            && self.call_id.as_deref() == Some(fence.call_id.as_str())
    }
    /// Refuse fields that cannot have been written together.
    pub(crate) fn coherent(&self) -> Result<()> {
        let frozen = self.call_id.is_some();
        let coherent = self.call_id.is_some() == self.intent.is_some()
            && match self.phase {
                LoadPhase::Pending => frozen && self.error.is_none(),
                LoadPhase::Complete => !frozen && self.error.is_none(),
                LoadPhase::Cancelled => !frozen && self.error.is_some(),
                LoadPhase::Failed => self.error.is_some(),
                LoadPhase::Loading | LoadPhase::Waiting => false,
            };
        if !coherent {
            return Err(invalid(format!(
                "Load {} in phase {} has an inconsistent frozen page or error",
                self.id,
                self.phase.as_str()
            )));
        }
        if let (Some(call), Some(intent)) = (&self.call_id, &self.intent) {
            let expected = frozen_intent(
                &self.id,
                call,
                &self.name,
                self.version,
                &self.args,
                &self.models,
                self.continuation.clone(),
            );
            if *intent != expected {
                return Err(invalid(format!(
                    "Load {} frozen page does not match the job",
                    self.id
                )));
            }
        }
        Ok(())
    }
}

/// What makes a page answer applicable: the replica generation, job, run
/// generation and frozen call ID it was requested under.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoadFence {
    pub replica: u64,
    pub load_id: String,
    pub run: u64,
    pub call_id: String,
}

/// One ready page request: the fence its answer must pass and the exact
/// frozen intent to send.
#[derive(Clone, Debug, PartialEq)]
pub struct LoadPageTask {
    pub fence: LoadFence,
    pub intent: LoadIntent,
    pub attempts: u64,
    pub retry: Option<LoadRetryClass>,
}

/// A bounded scheduler read: ready pages in oldest-ready order, and every row
/// the read skipped because it cannot be decoded.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct LoadSchedule {
    pub pages: Vec<LoadPageTask>,
    pub issues: Vec<LoadLedgerIssue>,
}

/// How a start was answered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LoadStartKind {
    /// A new job with its first page frozen: schedule it.
    Created,
    /// The once key names an active job: observe it; nothing new to schedule.
    Joined,
    /// The once key names a terminal (complete or failed) job: nothing runs.
    Reused,
}
#[derive(Clone, Debug, PartialEq)]
pub struct LoadStarted {
    pub job: LoadJob,
    pub kind: LoadStartKind,
}

/// The canonical once key of one Load call within a replica.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadOnceKey {
    /// Hex SHA-256 of the components below and [`LOAD_ONCE_FORMAT`].
    pub key: String,
    pub name: String,
    pub version: u64,
    /// Canonical JSON of the normalized business arguments.
    pub args: String,
    /// Canonical JSON of the output Models' local read contracts.
    pub models: String,
}

/// A failure to record against one frozen page, classified by mechanism.
#[derive(Clone, Debug, PartialEq)]
pub enum LoadFailure {
    /// A terminal rejection the backend saved (or a deterministic backend
    /// refusal). The job fails; only an explicit retry reads again.
    Backend(LoadError),
    /// A terminal local failure: a page that breaks the Load contract, a
    /// record that cannot be applied, or a failing hook. The page rolled back.
    Local(LoadJobError),
    /// Nothing terminal happened: the same call ID is sent again after a
    /// backoff derived from the persisted attempt count.
    Retryable {
        class: LoadRetryClass,
        message: String,
    },
}
impl LoadFailure {
    /// An `onStore` hook of `model` failed on the given identities.
    pub fn hook_failed(model: &str, identities: &[Value], message: impl Into<String>) -> Self {
        Self::Local(LoadJobError::new(
            HOOK_FAILED,
            message,
            identities
                .iter()
                .map(|id| LoadDiagnostic {
                    model: model.to_string(),
                    id: id.clone(),
                    code: "hookFailed".into(),
                })
                .collect(),
        ))
    }
    /// Local preparation, application or commit returned an error.
    pub fn local_retry(message: impl Into<String>) -> Self {
        Self::Retryable {
            class: LoadRetryClass::Local,
            message: message.into(),
        }
    }
    /// The request did not complete.
    pub fn transport(message: impl Into<String>) -> Self {
        Self::Retryable {
            class: LoadRetryClass::Transport,
            message: message.into(),
        }
    }
    pub fn is_terminal(&self) -> bool {
        !matches!(self, Self::Retryable { .. })
    }
    fn stored(&self) -> Option<LoadJobError> {
        match self {
            Self::Backend(error) => Some(LoadJobError::new(&error.code, &error.message, vec![])),
            Self::Local(error) => Some(error.clone()),
            Self::Retryable { .. } => None,
        }
    }
}

/// What one correlated page answer asks of the caller.
pub enum LoadPageStep {
    /// The answer is not for the current frozen page of this replica: drop it.
    Stale,
    /// A normalized successful page: store it through the owned session
    /// ([`Client::prepare_store`], hooks, [`Client::apply_prepared_store`]).
    Store(StoreDelivery),
    /// Record this failure ([`Client::record_load_failure`]); nothing is stored.
    Record(LoadFailure),
}

/// What storing one Load page came to inside the session.
#[derive(Debug)]
pub enum LoadApply {
    /// The job no longer waits for this page: nothing was written.
    Stale,
    /// A record could not be applied. Refused before any hook; the caller
    /// rolls the session back and records the failure.
    Refused(LoadFailure),
    /// The page's authority and progress, ready to commit together.
    Applied {
        job: Box<LoadJob>,
        report: ApplyReport,
    },
}

/// What [`Client::store_load_page`] or a recorded failure came to.
#[derive(Debug)]
pub enum LoadStored {
    Stale,
    Applied { job: LoadJob, report: ApplyReport },
    Failed(LoadJob),
    Retrying(LoadJob),
}

/// The once key of calling `name` v`version` with `args` against `schema`.
pub fn load_once_key(
    schema: &Schema,
    name: &str,
    version: u64,
    args: &Value,
) -> Result<LoadOnceKey> {
    let (args, models) = load_call(schema, name, version, args)?;
    once_key(name, version, &args, &models)
}
fn once_key(
    name: &str,
    version: u64,
    args: &Value,
    models: &BTreeMap<String, u64>,
) -> Result<LoadOnceKey> {
    let args = canonical_json(args)?;
    let models = canonical_json(&serde_json::to_value(models)?)?;
    let key = crate::query_cache::sha256_hex(&canonical_json(&json!({
        "format": LOAD_ONCE_FORMAT,
        "name": name,
        "version": version,
        "args": args,
        "models": models,
    }))?);
    Ok(LoadOnceKey {
        key,
        name: name.to_string(),
        version,
        args,
        models,
    })
}
/// Normalized arguments and the output Models' local read contracts of a call.
fn load_call(
    schema: &Schema,
    name: &str,
    version: u64,
    args: &Value,
) -> Result<(Value, BTreeMap<String, u64>)> {
    let load = schema.load(name, version).map_err(|e| coded(UNKNOWN, e))?;
    let args = normalize_load_args(schema, load, args)?;
    let mut models = BTreeMap::new();
    for output in &load.outputs {
        if let Some(model) = &output.model {
            models.insert(model.clone(), schema.model(model)?.version);
        }
    }
    validate_load_models(schema, load, &models)?;
    Ok((args, models))
}
/// Whether a job's frozen Load version and Model contracts are still served.
fn available(schema: &Schema, name: &str, version: u64, models: &BTreeMap<String, u64>) -> bool {
    schema.load(name, version).is_ok()
        && models
            .iter()
            .all(|(model, v)| schema.model(model).is_ok_and(|m| m.version == *v))
}
/// The terminal failure of one malformed correlated page, by the step that
/// refused it.
fn item_failure(error: &LoadItemError) -> LoadFailure {
    let code = match error.kind {
        LoadItemErrorKind::PageTooLarge => PAGE_TOO_LARGE,
        LoadItemErrorKind::InvalidContinuation => INVALID_CONTINUATION,
        _ => PROTOCOL_INVALID,
    };
    LoadFailure::Local(LoadJobError::new(code, &error.message, vec![]))
}
fn unavailable_error() -> LoadJobError {
    LoadJobError::new(
        CONTRACT_UNAVAILABLE,
        "the Load version this job was started with is no longer retained",
        vec![],
    )
}
fn cancelled_error() -> LoadJobError {
    LoadJobError::new(CANCELLED, "the Load was cancelled", vec![])
}
fn normalize_id(id: &str) -> Result<String> {
    uuid::Uuid::parse_str(id)
        .map(|id| id.to_string())
        .map_err(|_| coded(NOT_FOUND, format!("{id} is not a Load ID")))
}
/// A fresh job at the first page, with its frozen first request checked
/// against the request envelope bounds before anything is stored.
fn fresh_intent(
    name: &str,
    version: u64,
    args: &Value,
    models: &BTreeMap<String, u64>,
) -> Result<LoadIntent> {
    let intent = frozen_intent(
        &uuid::Uuid::new_v4().to_string(),
        &uuid::Uuid::new_v4().to_string(),
        name,
        version,
        args,
        models,
        None,
    );
    LoadBatchRequest {
        loads: vec![intent.clone()],
    }
    .encode()?;
    Ok(intent)
}

/// What the once mapping decides for one key, read in one transaction.
enum OnceDecision {
    Join(LoadJob),
    Reuse(LoadJob),
    Create,
    Replace(String),
}
fn decide_once<S: ClientStore>(
    e: &mut crate::engine::Engine<'_, S>,
    key: &LoadOnceKey,
    refresh: bool,
) -> Result<OnceDecision> {
    let Some(id) = e.load_once(&key.key)? else {
        return Ok(OnceDecision::Create);
    };
    let job = e.load_job(&id)?.ok_or_else(|| {
        coded(
            LEDGER_INVALID,
            format!("the once mapping of {} names missing Load {id}", key.name),
        )
    })?;
    Ok(match job.phase {
        phase if phase.active() => OnceDecision::Join(job),
        LoadPhase::Complete | LoadPhase::Failed if !refresh => OnceDecision::Reuse(job),
        _ => OnceDecision::Replace(id),
    })
}

impl<S: ClientStore> Client<S> {
    /// The replica generation Load fences carry: it changes whenever a
    /// rebuild replaces the replica this client writes.
    pub fn replica_generation(&self) -> u64 {
        self.replica
    }
    /// Whether answers requested under `fence` may still be applied here.
    pub(crate) fn load_current(&self, fence: &LoadFence) -> bool {
        fence.replica == self.replica && self.schema_state.pending.is_none()
    }
    fn load_writable(&self) -> Result<()> {
        if self.schema_state.pending.is_some() {
            return Err(coded(
                SCHEMA_PENDING,
                "an incompatible schema rebuild waits for unsent Mutations",
            ));
        }
        Ok(())
    }
    /// Fail every pending job whose frozen contract this schema no longer
    /// serves, inside the open transaction of [`Client::open`].
    pub(crate) fn reconcile_loads(store: &mut S, schema: &Schema) -> Result<()> {
        let mut changed = BTreeSet::new();
        crate::engine::Engine::new(store, schema, &mut changed, false).fail_unavailable_loads(
            |name, version, models| available(schema, name, version, models),
            &unavailable_error(),
        )
    }
    /// Start a Load: a local commit that needs no connection.
    ///
    /// An ordinary start always creates an independent job and never reads
    /// or writes a once mapping. A `once` start resolves the canonical key in
    /// the same transaction that would create the job:
    ///
    /// | Mapped job | `once` | `once` + `refresh` |
    /// | --- | --- | --- |
    /// | none, or cancelled | create and map | create and map |
    /// | active | join | join |
    /// | complete or failed | reuse, nothing runs | create and remap |
    pub fn start_load(
        &mut self,
        name: &str,
        version: u64,
        args: &Value,
        options: LoadOptions,
    ) -> Result<LoadStarted> {
        if options.refresh && !options.once {
            return Err(coded(INVALID_OPTIONS, "refresh requires once"));
        }
        if self.session_active() {
            return Err(invalid("client transaction active"));
        }
        self.load_writable()?;
        let (args, models) = load_call(&self.schema, name, version, args)?;
        let intent = fresh_intent(name, version, &args, &models)?;
        if !options.once {
            let job = self.write(|e| {
                e.insert_load(&intent)?;
                e.load_job(&intent.load_id)?
                    .ok_or_else(|| invalid("inserted Load disappeared"))
            })?;
            return Ok(LoadStarted {
                job,
                kind: LoadStartKind::Created,
            });
        }
        let key = once_key(name, version, &args, &models)?;
        // A join or reuse is answered from the committed reader, so it neither
        // bumps the client generation nor notifies a watcher. The write
        // decides again inside its transaction.
        match self.view(|e| decide_once(e, &key, options.refresh))? {
            OnceDecision::Join(job) => {
                return Ok(LoadStarted {
                    job,
                    kind: LoadStartKind::Joined,
                });
            }
            OnceDecision::Reuse(job) => {
                return Ok(LoadStarted {
                    job,
                    kind: LoadStartKind::Reused,
                });
            }
            OnceDecision::Create | OnceDecision::Replace(_) => {}
        }
        self.write(|e| {
            let (job, kind) = match decide_once(e, &key, options.refresh)? {
                OnceDecision::Join(job) => (job, LoadStartKind::Joined),
                OnceDecision::Reuse(job) => (job, LoadStartKind::Reused),
                OnceDecision::Create => {
                    e.insert_load(&intent)?;
                    e.map_load_once(&key, &intent.load_id)?;
                    (
                        e.load_job(&intent.load_id)?
                            .ok_or_else(|| invalid("inserted Load disappeared"))?,
                        LoadStartKind::Created,
                    )
                }
                OnceDecision::Replace(old) => {
                    e.insert_load(&intent)?;
                    e.remap_load_once(&key.key, &old, &intent.load_id)?;
                    (
                        e.load_job(&intent.load_id)?
                            .ok_or_else(|| invalid("inserted Load disappeared"))?,
                        LoadStartKind::Created,
                    )
                }
            };
            Ok(LoadStarted { job, kind })
        })
    }
    /// The stored job `id` of this replica, or `None` (also for a string
    /// that is not a UUID, which no job has).
    pub fn get_load(&mut self, id: &str) -> Result<Option<LoadJob>> {
        let Ok(id) = normalize_id(id) else {
            return Ok(None);
        };
        self.view(|e| e.load_job(&id))
    }
    /// The statuses of the `limit` (1..=100) most recently started jobs,
    /// newest first. A row that cannot be decoded is listed as a failed job
    /// with a `load.ledger_invalid` error ([`LoadLedgerIssue::status`]), never
    /// left out; a named read of it fails.
    pub fn list_loads(&mut self, limit: usize) -> Result<Vec<LoadStatus>> {
        if !(1..=MAX_LIST).contains(&limit) {
            return Err(coded(
                INVALID_OPTIONS,
                format!("list limit must be 1..{MAX_LIST}"),
            ));
        }
        Ok(self
            .view(|e| e.load_scan("", "seq DESC", limit, 0))?
            .into_iter()
            .map(|row| match row {
                Ok(job) => job.status(),
                Err(issue) => issue.status(),
            })
            .collect())
    }
    /// Cancel a pending or failed job and remove its own once mapping in the
    /// same transaction. Already committed pages stay; every outstanding
    /// answer becomes inert. Cancelling a complete or cancelled job changes
    /// nothing.
    pub fn cancel_load(&mut self, id: &str) -> Result<LoadJob> {
        let id = normalize_id(id)?;
        self.write(|e| {
            e.load_job(&id)?
                .ok_or_else(|| coded(NOT_FOUND, format!("Load {id}")))?;
            e.cancel_load(&id, &cancelled_error())?;
            e.load_job(&id)?
                .ok_or_else(|| invalid("cancelled Load disappeared"))
        })
    }
    /// Read a failed job again from its last committed continuation: one
    /// transaction starts run + 1 under a fresh call ID, so every answer to
    /// the old call is inert. Committed Models and pages stay. An active job
    /// is returned unchanged; a complete or cancelled one needs a new start.
    pub fn retry_load(&mut self, id: &str) -> Result<LoadJob> {
        let id = normalize_id(id)?;
        if self.session_active() {
            return Err(invalid("client transaction active"));
        }
        self.load_writable()?;
        let job = self
            .view(|e| e.load_job(&id))?
            .ok_or_else(|| coded(NOT_FOUND, format!("Load {id}")))?;
        match job.phase {
            phase if phase.active() => return Ok(job),
            LoadPhase::Failed => {}
            phase => {
                return Err(coded(
                    NOT_RETRYABLE,
                    format!("Load {id} is {}; start a new Load", phase.as_str()),
                ));
            }
        }
        if !available(&self.schema, &job.name, job.version, &job.models) {
            return Err(coded(CONTRACT_UNAVAILABLE, unavailable_error().message));
        }
        self.write(|e| {
            let job = e
                .load_job(&id)?
                .ok_or_else(|| coded(NOT_FOUND, format!("Load {id}")))?;
            if job.phase == LoadPhase::Failed {
                e.rerun_load(&job)?;
            } else if !job.phase.active() {
                return Err(coded(
                    NOT_RETRYABLE,
                    format!("Load {id} is {}; start a new Load", job.phase.as_str()),
                ));
            }
            e.load_job(&id)?
                .ok_or_else(|| invalid("retried Load disappeared"))
        })
    }
    /// Delete a terminal job, and the once mapping only while it still names
    /// it. An active job is refused.
    pub fn forget_load(&mut self, id: &str) -> Result<()> {
        let id = normalize_id(id)?;
        self.write(|e| {
            let job = e
                .load_job(&id)?
                .ok_or_else(|| coded(NOT_FOUND, format!("Load {id}")))?;
            if job.phase.active() {
                return Err(coded(NOT_TERMINAL, format!("Load {id} is still active")));
            }
            e.forget_load(&id)?;
            Ok(())
        })
    }
    /// Remove every once mapping of `name` for `args`, across every retained
    /// version whose inputs accept them. Deletes no job and no Model, cancels
    /// nothing and needs no network. Answers how many mappings it removed.
    pub fn invalidate_load(&mut self, name: &str, args: &Value) -> Result<usize> {
        if self.session_active() {
            return Err(invalid("client transaction active"));
        }
        self.load_writable()?;
        let versions: Vec<u64> = self
            .schema
            .loads
            .iter()
            .filter(|load| load.name == name)
            .map(|load| load.version)
            .collect();
        if versions.is_empty() {
            return Err(coded(UNKNOWN, format!("unknown Load {name}")));
        }
        let mut keys = vec![];
        let mut refusal = None;
        for version in versions {
            match load_call(&self.schema, name, version, args)
                .and_then(|(args, models)| once_key(name, version, &args, &models))
            {
                Ok(key) => keys.push(key),
                Err(error) => refusal = Some(error),
            }
        }
        if keys.is_empty() {
            return Err(refusal.unwrap_or_else(|| invalid("invalid Load arguments")));
        }
        self.write(|e| {
            let mut removed = 0;
            for key in &keys {
                removed += e.unmap_load_arguments(&key.name, key.version, &key.args)?;
            }
            Ok(removed)
        })
    }
    /// Up to `limit` ready pages in oldest-ready order, leaving out the jobs
    /// in `skip` (a page already in flight, a job backing off, a row already
    /// reported as damaged). Bounded committed reads: one call reads at most
    /// `limit + skip.len() + MAX_SCAN_ISSUES` rows and reports at most
    /// [`MAX_SCAN_ISSUES`] rows that cannot be decoded, which never block
    /// another job. No page is ready while an incompatible rebuild is pending.
    pub fn load_ready_pages(
        &mut self,
        limit: usize,
        skip: &BTreeSet<String>,
    ) -> Result<LoadSchedule> {
        let mut schedule = LoadSchedule::default();
        if limit == 0 || self.schema_state.pending.is_some() {
            return Ok(schedule);
        }
        let replica = self.replica;
        let chunk = limit + skip.len();
        let budget = chunk + MAX_SCAN_ISSUES;
        let (mut offset, mut scanned) = (0, 0);
        'scan: while schedule.pages.len() < limit && scanned < budget {
            let wanted = chunk.min(budget - scanned);
            let rows = self.view(|e| {
                e.load_scan("WHERE phase = 'pending'", "ready, load_id", wanted, offset)
            })?;
            offset += rows.len();
            scanned += rows.len();
            let exhausted = rows.len() < wanted;
            for row in rows {
                match row {
                    Err(issue) if skip.contains(&issue.load_id) => {}
                    Err(issue) => {
                        schedule.issues.push(issue);
                        if schedule.issues.len() == MAX_SCAN_ISSUES {
                            break 'scan;
                        }
                    }
                    Ok(job) if skip.contains(&job.id) => {}
                    Ok(job) if schedule.pages.len() < limit => {
                        let (Some(call_id), Some(intent)) = (job.call_id, job.intent) else {
                            continue;
                        };
                        schedule.pages.push(LoadPageTask {
                            fence: LoadFence {
                                replica,
                                load_id: job.id,
                                run: job.run,
                                call_id,
                            },
                            intent,
                            attempts: job.attempts,
                            retry: job.retry,
                        });
                    }
                    Ok(_) => {}
                }
            }
            if exhausted {
                break;
            }
        }
        Ok(schedule)
    }
    /// Classify one correlated page answer against the frozen page `fence`
    /// names: stale, a failure to record, or a normalized page to store. A
    /// correlated page that is malformed ([`LoadPageReply::page`] is an item
    /// error) or whose content breaks the Load contract fails only its own
    /// job, terminally.
    pub fn load_page_step(
        &mut self,
        fence: &LoadFence,
        reply: LoadPageReply,
    ) -> Result<LoadPageStep> {
        if reply.load_id != fence.load_id || reply.call_id != fence.call_id {
            return Err(invalid("Load page does not answer its fenced request"));
        }
        if !self.load_current(fence) {
            return Ok(LoadPageStep::Stale);
        }
        let Some(job) = self.view(|e| e.load_job(&fence.load_id))? else {
            return Ok(LoadPageStep::Stale);
        };
        let Some(intent) = job.intent.as_ref().filter(|_| job.answers(fence)) else {
            return Ok(LoadPageStep::Stale);
        };
        let page = match reply.page {
            Ok(page) => page,
            Err(error) => return Ok(LoadPageStep::Record(item_failure(&error))),
        };
        Ok(match &page.outcome {
            LoadOutcome::Succeeded { .. } => match page.normalize(&self.schema, intent) {
                Ok(page) => LoadPageStep::Store(StoreDelivery::Load {
                    fence: fence.clone(),
                    page,
                }),
                Err(error) => LoadPageStep::Record(item_failure(&error)),
            },
            LoadOutcome::Failed { error } => {
                LoadPageStep::Record(LoadFailure::Backend(error.clone()))
            }
            LoadOutcome::Retryable { error } => LoadPageStep::Record(LoadFailure::Retryable {
                class: LoadRetryClass::Backend,
                message: format!("{}: {}", error.code, error.message),
            }),
        })
    }
    /// Record a failure of the frozen page `fence` names in its own short
    /// transaction: a terminal one fails the run (keeping committed progress
    /// and the failed call), a retryable one counts an attempt and keeps the
    /// call ID. `None` when the fence is no longer current: nothing changed.
    pub fn record_load_failure(
        &mut self,
        fence: &LoadFence,
        failure: &LoadFailure,
    ) -> Result<Option<LoadJob>> {
        if !self.load_current(fence) {
            return Ok(None);
        }
        self.write(|e| {
            let written = match (failure.stored(), failure) {
                (Some(error), _) => {
                    e.fail_load(&fence.load_id, fence.run, &fence.call_id, &error)?
                }
                (None, LoadFailure::Retryable { class, .. }) => {
                    e.retry_later_load(&fence.load_id, fence.run, &fence.call_id, *class)?
                }
                (None, _) => false,
            };
            if !written {
                return Ok(None);
            }
            e.load_job(&fence.load_id)
        })
    }
    /// Store one correlated page answer without store hooks: the reference
    /// sequence the runtime follows with hooks between preparation and
    /// replay. Classification, preparation, a refusal before any write,
    /// replay and commit; every failure is recorded after the rollback.
    pub fn store_load_page(
        &mut self,
        fence: &LoadFence,
        reply: LoadPageReply,
    ) -> Result<LoadStored> {
        let delivery = match self.load_page_step(fence, reply)? {
            LoadPageStep::Stale => return Ok(LoadStored::Stale),
            LoadPageStep::Record(failure) => return self.settle_load(fence, &failure),
            LoadPageStep::Store(delivery) => delivery,
        };
        self.begin_session()?;
        let prepared = match self.prepare_store(delivery) {
            Ok(prepared) => prepared,
            Err(error) => {
                self.rollback_load_session();
                return self.settle_load(fence, &LoadFailure::local_retry(error.to_string()));
            }
        };
        if let Some(refusal) = prepared.load_refusal().cloned() {
            self.rollback_load_session();
            return self.settle_load(fence, &refusal);
        }
        let stored = self.apply_prepared_store(prepared).and_then(|result| {
            self.commit_session()?;
            Ok(result)
        });
        match stored {
            Ok(StoreResult::Load(LoadApply::Applied { job, report })) => {
                Ok(LoadStored::Applied { job: *job, report })
            }
            Ok(_) => Ok(LoadStored::Stale),
            Err(error) => {
                self.rollback_load_session();
                self.settle_load(fence, &LoadFailure::local_retry(error.to_string()))
            }
        }
    }
    fn rollback_load_session(&mut self) {
        if self.session_active() {
            let _ = self.rollback_session();
        }
    }
    fn settle_load(&mut self, fence: &LoadFence, failure: &LoadFailure) -> Result<LoadStored> {
        Ok(match self.record_load_failure(fence, failure)? {
            None => LoadStored::Stale,
            Some(job) if job.phase == LoadPhase::Failed => LoadStored::Failed(job),
            Some(job) => LoadStored::Retrying(job),
        })
    }
}

impl<S: ClientStore> crate::engine::Engine<'_, S> {
    /// Stage one normalized page for the frozen page `fence` names, then
    /// commit its progress beside it. `current` is the client-level replica
    /// and pending-rebuild fence. A record that cannot be applied refuses the
    /// page ([`LoadApply::Refused`]) - the caller must not commit it - and a
    /// `Diverged` pending replay does not.
    pub(crate) fn apply_load_page_body(
        &mut self,
        fence: &LoadFence,
        page: &LoadPageResponse,
        current: bool,
    ) -> Result<LoadApply> {
        if !current {
            return Ok(LoadApply::Stale);
        }
        let Some(job) = self
            .load_job(&fence.load_id)?
            .filter(|job| job.answers(fence))
        else {
            return Ok(LoadApply::Stale);
        };
        let LoadOutcome::Succeeded { next, .. } = &page.outcome else {
            return Err(invalid("only a succeeded Load page is stored"));
        };
        if page.load_id != fence.load_id || page.call_id != fence.call_id {
            return Err(invalid("Load page does not answer its fenced request"));
        }
        let report = self.apply_records(&page.records)?;
        let diagnostics: Vec<LoadDiagnostic> = report
            .reports
            .iter()
            .filter_map(LoadDiagnostic::of)
            .collect();
        if !diagnostics.is_empty() {
            if matches!(self.stage_mode, crate::authority::StageMode::Replay { .. }) {
                return Err(invalid("a prepared Load page was refused on replay"));
            }
            return Ok(LoadApply::Refused(LoadFailure::Local(LoadJobError::new(
                STORE_FAILED,
                format!(
                    "{} of {} records on the Load page could not be applied",
                    diagnostics.len(),
                    page.records.len()
                ),
                diagnostics,
            ))));
        }
        self.advance_load(&job, next)?;
        let job = self
            .load_job(&fence.load_id)?
            .ok_or_else(|| invalid("advanced Load disappeared"))?;
        Ok(LoadApply::Applied {
            job: Box::new(job),
            report,
        })
    }
}
