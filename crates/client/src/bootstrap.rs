//! Finite Bootstrap status exposed after Store05 commits.
use serde::{Deserialize, Serialize};
use serde_json::Value;
pub const RECORDS_FAILED: &str = "bootstrap.records_failed";
// The stable code a response that is not a page of the requested interval
// fails with: an envelope neither side can attribute to a record.
pub const PROTOCOL_INVALID: &str = "bootstrap.protocol_invalid";
// The stable code a request the server definitively refused fails with. A
// transport failure is not this: it keeps the run and is retried.
pub const REQUEST_REJECTED: &str = "bootstrap.request_rejected";
// The stable prefix every refusal of a registration this client no longer
// holds carries. An engine error is a message, not a code, so this is what a
// host has to recognize it by: the SDKs match it and raise their own
// `subscription.closed` instead of the engine's text
// ([`crate::bootstrap_ledger`]).
pub const SUBSCRIPTION_CLOSED: &str = "subscription.closed";
// At most this many record summaries are kept in a stored failure.
pub const MAX_FAILURES: usize = 50;
// A stored failure message is cut to this many UTF-8 bytes.
pub const MAX_MESSAGE: usize = 1024;

// How far a Stream's historical load has got. The names are the stored column
// values, and the phase of a Stream that never asked for one is
// [`BootstrapPhase::NotRequested`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BootstrapPhase {
    // No load was ever requested for this registration.
    NotRequested,
    // Registered and waiting: for the subscription's origin, or for the
    // scheduler's next page.
    Requested,
    // At least one page committed and the interval is not finished.
    Loading,
    // The interval is finished and the barrier H is fixed; ordinary delivery
    // has not reached it yet.
    CatchingUp,
    // `B = S` and `L >= H`: the historical interval and the fixed barrier are
    // both processed.
    Complete,
    // The run ended on a failure that is terminal until an explicit retry.
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

    // Whether a run in this phase still has work the scheduler can issue or
    // settle.

    // Whether a page may be asked for. A run that is catching up has none to
    // ask for: it waits for ordinary delivery to reach its barrier.
}

// One record of a failed page, in the bounded form the failure keeps: what it
// was, not what it contained. Model payloads never enter the ledger.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapRecordFailure {
    pub model: String,
    pub identity: Value,
    pub stamp: u64,
    pub code: String,
}
impl BootstrapRecordFailure {
    // The summary of one report that fails Bootstrap coverage, or `None` for
    // one that does not: only `ReadFailed`, `Skipped` and `Conflict` do, so a
    // `Diverged` pending-Action replay is never summarised here. A report the
    // server attributed keeps its code; one this client made carries the kind
    // that made it.
}

// Why a run failed, in the bounded form the row stores: a code, a message of
// at most [`MAX_MESSAGE`] UTF-8 bytes and at most [`MAX_FAILURES`] record
// summaries. [`BootstrapError::new`] is the only way to build one, so nothing
// unbounded can be stored.
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
// Cut `text` to at most `bytes` UTF-8 bytes, on a character boundary: a
// message is diagnostic text, never a place to lose a valid string.
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

// One Stream's load, as the ledger holds it. `cursor` is B and `barrier` is H;
// S and L stay in [`SubscriptionState`], which Bootstrap never writes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapState {
    pub stream: String,
    pub subscription_id: u64,
    pub state: BootstrapPhase,
    // The retry fence: every call and every response belongs to one run.
    pub run: u64,
    // B, the committed historical progress.
    pub cursor: u64,
    // H, the head the terminal page observed; `None` until it commits.
    pub barrier: Option<u64>,
    pub error: Option<BootstrapError>,
}
impl BootstrapState {
    // Refuse a row whose fields cannot have been written together: a barrier
    // before the interval finished, or a failure without a failed run.
}

// What applying one historical page came to.
