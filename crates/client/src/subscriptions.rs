//! Bound Store subscription status; no subscription ledger.
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionState {
    pub stream: String,
    /// The bound Store registration identity (currently 1).
    pub subscription_id: u64,
    /// Starting boundary S; `None` until initialization commits.
    pub starting_cursor: Option<u64>,
    /// Committed delivery cursor C; `None` before initialization.
    pub cursor: Option<u64>,
}
