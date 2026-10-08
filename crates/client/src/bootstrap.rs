//! Finite Bootstrap status exposed after Store05 commits.
use serde::{Deserialize, Serialize};
pub const SUBSCRIPTION_CLOSED: &str = "subscription.closed";

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
}

/// The terminal Bootstrap failure exposed by current status snapshots.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapError {
    pub code: String,
    pub message: String,
}

// One Stream's load, as the ledger holds it. `cursor` is B and `barrier` is H;
// S and L remain in the bound Store status.
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
