//! Final tracking pair application and retained log positions.
use super::{TrackIntent, cursor};
use axton_core::{RecordKey, canonical_json, check_stream};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PositionKind {
    Upsert,
    Remove,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    try_from = "crate::server_bridge::members::MemberDeltaWire",
    into = "crate::server_bridge::members::MemberDeltaWire"
)]
pub struct MemberDelta {
    pub stream: String,
    pub key: RecordKey,
    pub publish: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    try_from = "crate::server_bridge::members::MemberPositionWire",
    into = "crate::server_bridge::members::MemberPositionWire"
)]
pub struct MemberPosition {
    pub stream: String,
    pub key: RecordKey,
    pub cursor: u64,
    pub kind: PositionKind,
}

/// A record as the Stream operations name it: its Model and the canonical
/// JSON of its identity object (`identityKey`), which must be canonical.
fn record_key(model: String, identity_key: &str) -> std::result::Result<RecordKey, String> {
    if model.is_empty() {
        return Err("a member names no Model".into());
    }
    let identity: Value = serde_json::from_str(identity_key)
        .map_err(|error| format!("invalid identityKey {identity_key:?}: {error}"))?;
    if !identity.is_object() {
        return Err(format!("identityKey {identity_key:?} is not an object"));
    }
    let key = RecordKey { model, identity };
    if encoded_identity(&key) != identity_key {
        return Err(format!("identityKey {identity_key:?} is not canonical"));
    }
    Ok(key)
}
fn encoded_identity(key: &RecordKey) -> String {
    canonical_json(&key.identity).unwrap_or_default()
}

/// `{model, identityKey}` on the wire.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct KeyWire {
    model: String,
    identity_key: String,
}

/// Fresh tracking creates/upserts only; saved removal positions still decode.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct MemberDeltaWire {
    stream: String,
    model: String,
    identity: Value,
    identity_key: String,
    publish: bool,
}
impl TryFrom<MemberDeltaWire> for MemberDelta {
    type Error = String;
    fn try_from(wire: MemberDeltaWire) -> std::result::Result<Self, String> {
        check_stream(&wire.stream).map_err(|e| e.to_string())?;
        let key = record_key(wire.model, &wire.identity_key)?;
        if key.identity != wire.identity {
            return Err("identity and identityKey name different records".into());
        }
        Ok(Self {
            stream: wire.stream,
            key,
            publish: wire.publish,
        })
    }
}
impl From<MemberDelta> for MemberDeltaWire {
    fn from(d: MemberDelta) -> Self {
        Self {
            stream: d.stream,
            identity_key: encoded_identity(&d.key),
            model: d.key.model,
            identity: d.key.identity,
            publish: d.publish,
        }
    }
}

/// A canonical record operand.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields, try_from = "KeyWire")]
pub struct MemberKey {
    pub model: String,
    pub identity_key: String,
}
impl TryFrom<KeyWire> for MemberKey {
    type Error = String;
    fn try_from(w: KeyWire) -> std::result::Result<Self, String> {
        record_key(w.model.clone(), &w.identity_key)?;
        Ok(Self {
            model: w.model,
            identity_key: w.identity_key,
        })
    }
}
impl MemberKey {
    pub fn from_key(k: &RecordKey) -> Self {
        Self {
            model: k.model.clone(),
            identity_key: encoded_identity(k),
        }
    }
    pub fn key(&self) -> RecordKey {
        record_key(self.model.clone(), &self.identity_key).expect("validated member key")
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    rename_all = "camelCase",
    deny_unknown_fields,
    try_from = "TrackingPairWire"
)]
pub struct TrackingPair {
    pub stream: String,
    pub model: String,
    pub identity_key: String,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TrackingPairWire {
    stream: String,
    model: String,
    identity_key: String,
}
impl TryFrom<TrackingPairWire> for TrackingPair {
    type Error = String;
    fn try_from(w: TrackingPairWire) -> std::result::Result<Self, String> {
        check_stream(&w.stream).map_err(|e| e.to_string())?;
        record_key(w.model.clone(), &w.identity_key)?;
        Ok(Self {
            stream: w.stream,
            model: w.model,
            identity_key: w.identity_key,
        })
    }
}
impl TrackingPair {
    pub fn key(&self) -> RecordKey {
        record_key(self.model.clone(), &self.identity_key).expect("validated tracking key")
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GuardMode {
    Ensure,
    Lock,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    rename_all = "camelCase",
    deny_unknown_fields,
    try_from = "GuardRecordWire"
)]
pub struct GuardRecord {
    pub model: String,
    pub identity_key: String,
    pub mode: GuardMode,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GuardRecordWire {
    model: String,
    identity_key: String,
    mode: GuardMode,
}
impl TryFrom<GuardRecordWire> for GuardRecord {
    type Error = String;
    fn try_from(w: GuardRecordWire) -> std::result::Result<Self, String> {
        record_key(w.model.clone(), &w.identity_key)?;
        Ok(Self {
            model: w.model,
            identity_key: w.identity_key,
            mode: w.mode,
        })
    }
}
pub(super) fn guard_order<'de, D: Deserializer<'de>>(
    d: D,
) -> std::result::Result<Vec<GuardRecord>, D::Error> {
    let records = Vec::<GuardRecord>::deserialize(d)?;
    let keys: Vec<String> = records
        .iter()
        .map(|r| {
            record_key(r.model.clone(), &r.identity_key)
                .unwrap()
                .encoded()
                .unwrap()
        })
        .collect();
    if keys.windows(2).any(|p| p[0] >= p[1]) {
        return Err(serde::de::Error::custom(
            "guards must be distinct and canonically ordered",
        ));
    }
    Ok(records)
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BootstrapEffects {
    pub declarations: Vec<TrackIntent>,
}
pub type Tracking = Vec<TrackingPair>;
pub type Guards = Vec<bool>;
/// [`MemberPosition`] on the wire: `{stream, model, identityKey, cursor, kind}`.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct MemberPositionWire {
    stream: String,
    model: String,
    identity_key: String,
    #[serde(with = "cursor")]
    cursor: u64,
    kind: PositionKind,
}
impl TryFrom<MemberPositionWire> for MemberPosition {
    type Error = String;
    fn try_from(wire: MemberPositionWire) -> std::result::Result<Self, String> {
        check_stream(&wire.stream).map_err(|error| format!("invalid stream: {error}"))?;
        Ok(Self {
            stream: wire.stream,
            key: record_key(wire.model, &wire.identity_key)?,
            cursor: wire.cursor,
            kind: wire.kind,
        })
    }
}
impl From<MemberPosition> for MemberPositionWire {
    fn from(position: MemberPosition) -> Self {
        Self {
            stream: position.stream,
            identity_key: encoded_identity(&position.key),
            model: position.key.model,
            cursor: position.cursor,
            kind: position.kind,
        }
    }
}
