//! Bound Store subscription status; no subscription ledger.
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionState {
    pub stream: String,
    pub subscription_id: u64,
    /// The boundary the first initialization committed; `None` until then.
    pub starting_cursor: Option<u64>,
    /// How far delivery has committed, at or above `starting_cursor`.
    pub cursor: Option<u64>,
}
