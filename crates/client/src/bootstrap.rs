//! Finite Bootstrap status exposed after Store05 commits.
use serde::{Deserialize, Serialize};
pub const SUBSCRIPTION_CLOSED: &str = "subscription.closed";

// Retained Bootstrap phase vocabulary. Bound Store status currently derives
// Requested or Complete from the starting boundary S and Bootstrap cursor B.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BootstrapPhase {
    // Retained phase for a Bootstrap that was not requested.
    NotRequested,
    // B has not reached S, or S is not initialized.
    Requested,
    // Retained intermediate phase for historical progress.
    Loading,
    // Retained intermediate phase for delivery catching up to a barrier.
    CatchingUp,
    // S is initialized and B equals S.
    Complete,
    // Retained terminal failure phase.
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

// Bootstrap view of the bound Store. Serialized names are retained:
// `cursor` is B and `barrier` is S; delivery cursor C is in SubscriptionState.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapState {
    pub stream: String,
    // The bound Store registration identity (currently 1).
    pub subscription_id: u64,
    pub state: BootstrapPhase,
    // Retained observer run identity (currently 1).
    pub run: u64,
    // Committed B, or zero before Bootstrap progress exists.
    pub cursor: u64,
    // Starting boundary S; `None` before initialization.
    pub barrier: Option<u64>,
    pub error: Option<BootstrapError>,
}
