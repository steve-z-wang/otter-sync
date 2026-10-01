//! Final tracking pair application and retained log positions.
use axton_core::RecordKey;
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PositionKind {
    Upsert,
    Remove,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    try_from = "crate::host::MemberDeltaWire",
    into = "crate::host::MemberDeltaWire"
)]
pub struct MemberDelta {
    pub stream: String,
    pub key: RecordKey,
    pub publish: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    try_from = "crate::host::MemberPositionWire",
    into = "crate::host::MemberPositionWire"
)]
pub struct MemberPosition {
    pub stream: String,
    pub key: RecordKey,
    pub cursor: u64,
    pub kind: PositionKind,
}
