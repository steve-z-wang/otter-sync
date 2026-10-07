//! The one typed definition of the host operation contract.
//!
//! Every request the engine may issue is a [`HostRequest`] variant and every
//! answer a host may give is one of the response types below. The TypeScript
//! mirror is `packages/server/host-contract.mts` and the shared examples are
//! `fixtures/protocol/host-operations.json`; a change here belongs in all three.
//!
//! `handle` and `load` may answer a refusal or a failure: a refusal rolls
//! the mutation back to its savepoint and records the code as that
//! mutation's rejection; a failure carries a thrown application error as
//! data. Every other thrown host error still aborts the whole delivery
//! ([#95](https://github.com/zanminwang/axton/issues/95) narrows nothing more).
use crate::stream_members::{MemberDelta, MemberPosition, PositionKind};
use crate::{Error, Host, Result, code, valid_code};
use axton_core::{LoadNext, RecordKey, canonical_json, check_stream, read_counter};
use serde::{Deserialize, Deserializer, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::{collections::BTreeSet, fmt::Display, future::Future, pin::Pin};

/// A counter field that keeps [`read_counter`]'s tolerance (any integral JSON
/// number inside the safe range) and names itself when it refuses a value.
macro_rules! counter_field {
    ($module:ident, $label:literal, $positive:expr) => {
        mod $module {
            use super::*;
            pub fn serialize<S: serde::Serializer>(
                value: &u64,
                serializer: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                serializer.serialize_u64(*value)
            }
            pub fn deserialize<'de, D: Deserializer<'de>>(
                deserializer: D,
            ) -> std::result::Result<u64, D::Error> {
                let value = Value::deserialize(deserializer)?;
                read_counter(&value, $positive).map_err(|error| {
                    serde::de::Error::custom(format!("invalid {}: {error}", $label))
                })
            }
        }
    };
}
counter_field!(counter, "counter", false);
counter_field!(sequence, "sequence", false);
counter_field!(cursor, "cursor", true);
counter_field!(stamp, "stamp", true);

/// `Some(Value::Null)` for an explicit `null`, `None` only when the key is absent.
fn present<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<Value>, D::Error> {
    Value::deserialize(deserializer).map(Some)
}

/// The Streams `lockStreams` names: at least one, each valid, in strictly
/// increasing byte order, which is the one lock order every writer uses.
fn lock_order<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<String>, D::Error> {
    let streams = Vec::<String>::deserialize(deserializer)?;
    if streams.is_empty() {
        return Err(serde::de::Error::custom("lockStreams names no Stream"));
    }
    for stream in &streams {
        check_stream(stream)
            .map_err(|error| serde::de::Error::custom(format!("invalid stream: {error}")))?;
    }
    if streams.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(serde::de::Error::custom(
            "Streams must be distinct and in canonical byte order",
        ));
    }
    Ok(streams)
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
    Advance,
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
fn guard_order<'de, D: Deserializer<'de>>(d: D) -> std::result::Result<Vec<GuardRecord>, D::Error> {
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
/// Full original transaction group keys survive later log compaction.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationGroup {
    #[serde(with = "counter")]
    pub from: u64,
    #[serde(with = "cursor")]
    pub through: u64,
    pub keys: Vec<MemberKey>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManifestSlice {
    #[serde(with = "counter")]
    pub start: u64,
    #[serde(with = "counter")]
    pub total: u64,
    pub models: std::collections::BTreeMap<String, u64>,
    #[serde(with = "counter")]
    pub from: u64,
    #[serde(with = "counter")]
    pub to: u64,
    pub keys: Vec<MemberKey>,
    #[serde(default)]
    pub companions: Vec<MemberKey>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BootstrapEffects {
    pub declarations: Vec<TrackIntent>,
}
pub type Tracking = Vec<TrackingPair>;
pub type Guards = Vec<Option<Stamped>>;
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

/// A required continuation member: `null` or exactly `{"state": …}`.
fn required_next<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<LoadNext, D::Error> {
    LoadNext::deserialize(deserializer)
}

fn nullable_string<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error> {
    Option::<String>::deserialize(deserializer)
}

/// Every operation, in the order [`HostRequest`] declares them. The fixture
/// and `packages/server/host-contract.mts` carry the same list; the contract
/// test checks this one against the enum itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LoaderMode {
    Prepare,
    Canonical,
}

pub const OPERATIONS: [&str; 32] = [
    "protocol05",
    "admitContext",
    "publicationFence",
    "handleBootstrap",
    "createManifest",
    "readCall",
    "readManifest",
    "captureTail",
    "savePublicationGroups",
    "readPublicationGroups",
    "readPositions",
    "claim",
    "saveReceipt",
    "claimCall",
    "saveCall",
    "head",
    "scan",
    "savepoint",
    "rollback",
    "release",
    "handle",
    "handleAction",
    "handleLoad",
    "load",
    "advanceStamp",
    "ensureStamp",
    "readStamps",
    "lockRecord",
    "readTracking",
    "guardRecords",
    "lockStreams",
    "applyStreamMembers",
];

/// Every request the engine issues to a host, tagged by `op` on the wire.
/// Additive protocol-5 storage operations, all inside the caller's transaction.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "op",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum Protocol05Operation {
    Admit {
        owner: String,
        context: axton_core::v05::RequestContext,
    },
    ClaimStore {
        store_id: String,
        principal: String,
        stream: String,
    },
    BeginBatch {
        store_id: String,
        #[serde(with = "cursor")]
        batch_id: u64,
        digest: String,
        #[serde(with = "cursor")]
        count: u64,
    },
    ReadResult {
        store_id: String,
        #[serde(with = "cursor")]
        batch_id: u64,
        #[serde(with = "counter")]
        ordinal: u64,
    },
    ReadResults {
        store_id: String,
        #[serde(with = "cursor")]
        batch_id: u64,
    },
    SaveResult {
        store_id: String,
        #[serde(with = "cursor")]
        batch_id: u64,
        #[serde(with = "counter")]
        ordinal: u64,
        #[serde(with = "cursor")]
        count: u64,
        result: axton_core::v05::MutationResult,
    },
    ReadTracking {
        records: Vec<MemberKey>,
        pairs: Vec<TrackingPair>,
    },
    GuardRecords {
        #[serde(deserialize_with = "guard_order")]
        records: Vec<GuardRecord>,
    },
    ReadPositions {
        stream: String,
        records: Vec<MemberKey>,
    },
    TargetPositions {
        stream: String,
        records: Vec<MemberKey>,
    },
    ApplyStreamMembers {
        deltas: Vec<MemberDelta>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "op",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum HostRequest {
    /// Protocol 5 persistence/application seam; its inner operation is validated by the v05 executor.
    Protocol05 {
        request: Protocol05Operation,
    },
    /// Current authorization, including saved response replay.
    AdmitContext {
        owner: String,
        context: axton_core::v04::RequestContext,
        durable: bool,
    },
    /// Persisted namespace-wide write fence; acquire before relevant work.
    PublicationFence {},
    HandleBootstrap {
        owner: String,
        call_id: String,
        context: axton_core::v04::RequestContext,
    },
    CreateManifest {
        owner: String,
        manifest_id: String,
        context: axton_core::v04::RequestContext,
        #[serde(with = "counter")]
        start: u64,
        models: std::collections::BTreeMap<String, u64>,
        selected: Vec<String>,
        held: Vec<MemberKey>,
        #[serde(with = "cursor")]
        budget: u64,
    },
    ReadCall {
        owner: String,
        call_id: String,
    },
    ReadManifest {
        owner: String,
        manifest_id: String,
        context: axton_core::v04::RequestContext,
        #[serde(with = "counter")]
        from: u64,
        #[serde(with = "cursor")]
        limit: u64,
        unique_models: Vec<String>,
    },
    CaptureTail {
        owner: String,
        manifest_id: String,
        context: axton_core::v04::RequestContext,
        #[serde(with = "counter")]
        head: u64,
    },
    SavePublicationGroups {
        positions: Vec<MemberPosition>,
    },
    ReadPublicationGroups {
        stream: String,
        #[serde(with = "counter")]
        after: u64,
        #[serde(with = "cursor")]
        limit: u64,
    },
    ReadPositions {
        stream: String,
        records: Vec<MemberKey>,
    },
    /// Lock this client's row and report its last accepted batch.
    Claim {
        owner: String,
        client_id: String,
    },
    /// Record the receipt for an accepted batch.
    SaveReceipt {
        owner: String,
        client_id: String,
        sequence: u64,
        receipt: String,
    },
    /// Lock an invocation's immutable request and completed response.
    ClaimCall {
        owner: String,
        call_id: String,
        request: String,
    },
    /// Complete a newly claimed invocation in the caller's transaction.
    SaveCall {
        owner: String,
        call_id: String,
        response: String,
    },
    /// The stream's current head cursor.
    Head {
        stream: String,
    },
    /// Retained upsert and removal log rows after `after`, at most `limit`
    /// in cursor order. Identity comes from centralized record metadata.
    Scan {
        stream: String,
        after: u64,
        limit: u64,
    },
    /// Open the savepoint that isolates one mutation.
    Savepoint {
        ordinal: u64,
    },
    /// Undo one mutation's effects back to its savepoint.
    Rollback {
        ordinal: u64,
    },
    /// Discard one mutation's savepoint, keeping its effects.
    Release {
        ordinal: u64,
    },
    /// Run one mutation's handler. `arguments` carries the decoded slots
    /// verbatim: its shape is the schema's business, not the contract's.
    Handle {
        name: String,
        version: u64,
        arguments: Value,
        owner: String,
        ordinal: u64,
    },
    /// Execute one generated Action handler with its normalized flat arguments.
    HandleAction {
        name: String,
        version: u64,
        arguments: Value,
        owner: String,
        call_id: String,
        ordinal: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        context: Option<axton_core::v04::RequestContext>,
    },
    /// Execute one generated Load handler for one page: the normalized flat
    /// arguments and the page's continuation (`null` on the first page). Its
    /// context declares no changes: the answer carries identities, the next
    /// continuation and the Stream additions its add-only handles declared.
    HandleLoad {
        name: String,
        version: u64,
        arguments: Value,
        #[serde(deserialize_with = "required_next")]
        continuation: LoadNext,
        owner: String,
        call_id: String,
        load_id: String,
    },
    /// Load the current state of these identities as the records of one
    /// retained model read contract (`version`), for this caller. Loads name
    /// no stream: the same identity, version and stamp describe the same
    /// content on every delivery path.
    Load {
        /// Omitted preserves the ordinary read. Preparation returns an empty row list;
        /// canonical reads must follow completed preparation in the same fenced transaction.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mode: Option<LoaderMode>,
        model: String,
        version: u64,
        identities: Vec<Value>,
        owner: String,
    },
    /// Allocate the next stamp of one record: initialize it at 1 or increment it.
    AdvanceStamp {
        model: String,
        identity_key: String,
    },
    /// The record's current stamp, initialized at 1 only when it has none.
    EnsureStamp {
        model: String,
        identity_key: String,
    },
    /// The current stamps of these records of one model, one per key in
    /// request order: an existing stamp is read and never rewritten, and only
    /// a record without one is initialized at 1.
    ReadStamps {
        model: String,
        identity_keys: Vec<String>,
    },
    /// Write-lock one existing record row without changing its stamp
    /// (`UPDATE ... SET stamp=stamp`), so a concurrent writer of the same row
    /// whose snapshot predates this commit restarts instead of acting on it.
    /// Never creates a row: an absent record answers `null`.
    LockRecord {
        model: String,
        identity_key: String,
    },
    ReadTracking {
        records: Vec<MemberKey>,
        pairs: Vec<TrackingPair>,
    },
    GuardRecords {
        #[serde(deserialize_with = "guard_order")]
        records: Vec<GuardRecord>,
    },
    LockStreams {
        #[serde(deserialize_with = "lock_order")]
        streams: Vec<String>,
    },
    ApplyStreamMembers {
        deltas: Vec<MemberDelta>,
    },
}

impl HostRequest {
    pub fn validate_response(&self, value: &Value) -> Result<()> {
        match self {
            Self::ReadTracking { records, pairs } => {
                let rows: Tracking =
                    serde_json::from_value(value.clone()).map_err(|e| self.invalid_response(e))?;
                let mut seen = BTreeSet::new();
                for row in rows {
                    if !seen.insert((
                        row.stream.clone(),
                        row.model.clone(),
                        row.identity_key.clone(),
                    )) {
                        return Err(self.invalid_response("duplicate tracking pair"));
                    }
                    if !records
                        .iter()
                        .any(|r| r.model == row.model && r.identity_key == row.identity_key)
                        && !pairs.contains(&row)
                    {
                        return Err(self.invalid_response("unrelated tracking pair"));
                    }
                }
            }
            Self::GuardRecords { records } => {
                let rows: Guards =
                    serde_json::from_value(value.clone()).map_err(|e| self.invalid_response(e))?;
                if rows.len() != records.len() {
                    return Err(self.invalid_response("wrong guard cardinality"));
                }
                if records
                    .iter()
                    .zip(rows)
                    .any(|(r, s)| r.mode != GuardMode::Lock && s.is_none())
                {
                    return Err(self.invalid_response("null advance/ensure stamp"));
                }
            }
            _ => {}
        }
        Ok(())
    }
    /// The operation, and the ordinal when the operation carries one.
    pub fn label(&self) -> String {
        match self {
            Self::Protocol05 { .. } => "protocol05".into(),
            Self::HandleBootstrap { .. } => "handleBootstrap".into(),
            Self::CreateManifest { .. } => "createManifest".into(),
            Self::ReadCall { .. } => "readCall".into(),
            Self::ReadManifest { .. } => "readManifest".into(),
            Self::CaptureTail { .. } => "captureTail".into(),
            Self::AdmitContext { .. } => "admitContext".into(),
            Self::PublicationFence {} => "publicationFence".into(),
            Self::SavePublicationGroups { .. } => "savePublicationGroups".into(),
            Self::ReadPublicationGroups { .. } => "readPublicationGroups".into(),
            Self::ReadPositions { .. } => "readPositions".into(),
            Self::Claim { .. } => "claim".into(),
            Self::SaveReceipt { .. } => "saveReceipt".into(),
            Self::ClaimCall { .. } => "claimCall".into(),
            Self::SaveCall { .. } => "saveCall".into(),
            Self::Head { .. } => "head".into(),
            Self::Scan { .. } => "scan".into(),
            Self::Savepoint { ordinal } => format!("savepoint(ordinal {ordinal})"),
            Self::Rollback { ordinal } => format!("rollback(ordinal {ordinal})"),
            Self::Release { ordinal } => format!("release(ordinal {ordinal})"),
            Self::Handle { ordinal, .. } => format!("handle(ordinal {ordinal})"),
            Self::HandleAction { ordinal, .. } => format!("handleAction(ordinal {ordinal})"),
            Self::HandleLoad { .. } => "handleLoad".into(),
            Self::Load { .. } => "load".into(),
            Self::AdvanceStamp { .. } => "advanceStamp".into(),
            Self::EnsureStamp { .. } => "ensureStamp".into(),
            Self::ReadStamps { .. } => "readStamps".into(),
            Self::LockRecord { .. } => "lockRecord".into(),
            Self::ReadTracking { .. } => "readTracking".into(),
            Self::GuardRecords { .. } => "guardRecords".into(),
            Self::LockStreams { .. } => "lockStreams".into(),
            Self::ApplyStreamMembers { .. } => "applyStreamMembers".into(),
        }
    }
    /// The code an unusable response to this operation has always carried.
    fn invalid_code(&self) -> &'static str {
        match self {
            Self::Claim { .. }
            | Self::SaveReceipt { .. }
            | Self::ClaimCall { .. }
            | Self::SaveCall { .. }
            | Self::Scan { .. } => code::STORAGE_INVALID,
            Self::Handle { .. }
            | Self::HandleAction { .. }
            | Self::HandleLoad { .. }
            | Self::HandleBootstrap { .. } => code::HANDLER_INVALID,
            Self::Load { .. } => code::LOADER_INVALID,
            Self::Protocol05 { .. }
            | Self::AdmitContext { .. }
            | Self::CreateManifest { .. }
            | Self::ReadCall { .. }
            | Self::ReadManifest { .. }
            | Self::CaptureTail { .. }
            | Self::PublicationFence {}
            | Self::SavePublicationGroups { .. }
            | Self::ReadPublicationGroups { .. }
            | Self::ReadPositions { .. }
            | Self::Head { .. }
            | Self::Savepoint { .. }
            | Self::Rollback { .. }
            | Self::Release { .. }
            | Self::AdvanceStamp { .. }
            | Self::EnsureStamp { .. }
            | Self::ReadStamps { .. }
            | Self::LockRecord { .. }
            | Self::ReadTracking { .. }
            | Self::GuardRecords { .. }
            | Self::LockStreams { .. }
            | Self::ApplyStreamMembers { .. } => code::HOST_INVALID,
        }
    }
    /// A response the protocol cannot use, named by operation.
    pub fn invalid_response(&self, detail: impl Display) -> Error {
        Error::new(
            self.invalid_code(),
            format!("{} response invalid: {detail}", self.label()),
        )
    }
}

/// An operation whose only answer is "done": `saveReceipt`, `savepoint`,
/// `rollback` and `release` all return `null` today.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Acknowledged;

/// The answer to `claim`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Claimed {
    pub client_id: String,
    pub owner: String,
    #[serde(with = "sequence")]
    pub sequence: u64,
    /// Absent and `null` both mean "no stored receipt", as they always have.
    #[serde(default)]
    pub receipt: Option<String>,
}

/// The stored invocation; only the inserting transaction may execute a fresh body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClaimedCall {
    pub fresh: bool,
    pub request: String,
    #[serde(deserialize_with = "nullable_string")]
    pub response: Option<String>,
}

/// The answer to `head`: a bare counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Head(#[serde(with = "counter")] pub u64);

/// One retained stream position with centralized identity. Upserts carry
/// the current content stamp from the Loader's snapshot; removals need no
/// stamp and never enter a Loader. A missing kind is legacy upsert only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Invalidation {
    #[serde(default = "legacy_upsert")]
    pub kind: PositionKind,
    pub stream: String,
    #[serde(with = "cursor")]
    pub cursor: u64,
    pub model: String,
    pub identity: Value,
    pub identity_key: String,
    #[serde(default, with = "stamp", skip_serializing_if = "zero_stamp")]
    pub stamp: u64,
}

fn zero_stamp(stamp: &u64) -> bool {
    *stamp == 0
}

/// The answer to `scan`.
pub type Scanned = Vec<Invalidation>;

/// The answer to `load`: one entry per requested identity, `null` for a record
/// that does not exist for this caller, a refusal the engine records as the
/// mutation's rejection (in a push) or reports for the page (in a pull), or a
/// failure carrying a thrown loader error.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged, try_from = "LoadedWire")]
pub enum Loaded {
    Rows(Vec<Option<Value>>),
    Refused { rejection: String },
    Failed { error: String },
}

#[derive(Deserialize)]
#[serde(untagged)]
enum LoadedWire {
    Rows(Vec<Option<Value>>),
    Object(LoadedObject),
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LoadedObject {
    #[serde(default, deserialize_with = "present")]
    rejection: Option<Value>,
    #[serde(default, deserialize_with = "present")]
    error: Option<Value>,
}
impl TryFrom<LoadedWire> for Loaded {
    type Error = String;
    fn try_from(wire: LoadedWire) -> std::result::Result<Self, String> {
        match wire {
            LoadedWire::Rows(rows) => Ok(Self::Rows(rows)),
            LoadedWire::Object(LoadedObject { rejection, error }) => match (rejection, error) {
                (Some(rejection), None) => rejection
                    .as_str()
                    .filter(|code| valid_code(code))
                    .map(|code| Self::Refused {
                        rejection: code.into(),
                    })
                    .ok_or_else(|| "invalid loader refusal code".into()),
                (None, Some(error)) => error
                    .as_str()
                    .map(|error| Self::Failed {
                        error: error.into(),
                    })
                    .ok_or_else(|| "invalid loader error".into()),
                _ => Err("a load answer carries rows, a refusal or a failure, not several".into()),
            },
        }
    }
}

/// The answer to `advanceStamp` and `ensureStamp`: the record's stamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stamped(#[serde(with = "stamp")] pub u64);

/// The answer to `readStamps`: one stamp per requested key, in request order.
pub type Stamps = Vec<Stamped>;

/// The answer to `lockRecord`: the locked record's unchanged stamp, or `None`
/// when the record has no metadata row (nothing was locked or created).
pub type Locked = Option<Stamped>;

/// The answer to `applyStreamMembers`: one position per delta, in order.
pub type Positions = Vec<MemberPosition>;

/// A record a handler names: an additional changed record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordRef {
    pub model: String,
    pub identity: Value,
}

/// Explicit declarations combine as sets in one settlement.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum StreamIntent {
    Track {
        stream: String,
        record: RecordRef,
    },
    Invalidate {
        #[serde(deserialize_with = "required_streams")]
        streams: Option<Vec<String>>,
        record: RecordRef,
    },
}
impl StreamIntent {
    pub fn record(&self) -> &RecordRef {
        match self {
            Self::Track { record, .. } | Self::Invalidate { record, .. } => record,
        }
    }
    pub fn streams(&self) -> Vec<&str> {
        match self {
            Self::Track { stream, .. } => vec![stream],
            Self::Invalidate { streams, .. } => {
                streams.iter().flatten().map(String::as_str).collect()
            }
        }
    }
}
fn required_streams<'de, D: Deserializer<'de>>(
    d: D,
) -> std::result::Result<Option<Vec<String>>, D::Error> {
    Option::<Vec<String>>::deserialize(d)
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum TrackIntent {
    Track { stream: String, record: RecordRef },
}
impl From<TrackIntent> for StreamIntent {
    fn from(t: TrackIntent) -> Self {
        match t {
            TrackIntent::Track { stream, record } => Self::Track { stream, record },
        }
    }
}

/// The effects of one settlement, shared by modern handlers, legacy handlers
/// and external transactions: the changed records beyond any input targets,
/// and the ordered Stream intents. There is no implicit publication.
fn effects(changes: Value, declarations: Value) -> std::result::Result<Effects, String> {
    let changes: Vec<RecordRef> = serde_json::from_value(changes)
        .map_err(|error| format!("invalid handler changes: {error}"))?;
    let declarations: Vec<StreamIntent> = serde_json::from_value(declarations)
        .map_err(|error| format!("invalid handler declarations: {error}"))?;
    let malformed = |r: &RecordRef| r.model.is_empty() || !r.identity.is_object();
    if changes.iter().any(malformed)
        || declarations
            .iter()
            .any(|m| m.streams().iter().any(|s| check_stream(s).is_err()) || malformed(m.record()))
    {
        return Err("invalid handler settlement".into());
    }
    Ok((changes, declarations))
}
type Effects = (Vec<RecordRef>, Vec<StreamIntent>);

/// The answer to `handle`: the records the handler changed beyond the
/// uploaded operations and its membership intents, a rejection code, or a
/// failure carrying a thrown handler error. Carrying more than one of these,
/// or none, is refused.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged, try_from = "HandledWire")]
pub enum Handled {
    Settled {
        changes: Vec<RecordRef>,
        declarations: Vec<StreamIntent>,
    },
    Rejected {
        rejection: String,
    },
    Failed {
        error: String,
    },
}

/// Action handlers return explicit named fields in addition to their effects.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged, try_from = "HandledActionWire")]
pub enum HandledAction {
    Settled {
        outputs: Value,
        changes: Vec<RecordRef>,
        declarations: Vec<StreamIntent>,
    },
    Rejected {
        rejection: String,
    },
    Failed {
        error: String,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HandledActionWire {
    #[serde(default, deserialize_with = "present")]
    outputs: Option<Value>,
    #[serde(default, deserialize_with = "present")]
    changes: Option<Value>,
    #[serde(default, deserialize_with = "present")]
    declarations: Option<Value>,
    #[serde(default, deserialize_with = "present")]
    rejection: Option<Value>,
    #[serde(default, deserialize_with = "present")]
    error: Option<Value>,
}

impl TryFrom<HandledActionWire> for HandledAction {
    type Error = String;
    fn try_from(wire: HandledActionWire) -> std::result::Result<Self, String> {
        match (
            wire.outputs,
            wire.changes,
            wire.declarations,
            wire.rejection,
            wire.error,
        ) {
            (None, None, None, Some(rejection), None) => rejection
                .as_str()
                .filter(|code| valid_code(code))
                .map(|code| Self::Rejected {
                    rejection: code.into(),
                })
                .ok_or_else(|| "invalid rejection code".into()),
            (None, None, None, None, Some(error)) => error
                .as_str()
                .map(|error| Self::Failed {
                    error: error.into(),
                })
                .ok_or_else(|| "invalid handler error".into()),
            (Some(outputs), Some(changes), Some(declarations), None, None)
                if outputs.is_object() =>
            {
                let (changes, declarations) = effects(changes, declarations)?;
                Ok(Self::Settled {
                    outputs,
                    changes,
                    declarations,
                })
            }
            _ => Err("invalid Action handler settlement".into()),
        }
    }
}

/// A native Load answers its identity lists, continuation and optional tracking.
/// Tracking is legal only beside successful data, never a refusal/failure.
/// The page engine validates continuation, identities and returned-record bounds.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged, try_from = "HandledLoadWire")]
pub enum HandledLoad {
    Settled {
        data: Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        next: Option<Value>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        tracking: Vec<TrackIntent>,
    },
    Rejected {
        rejection: String,
    },
    Failed {
        error: String,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HandledLoadWire {
    #[serde(default, deserialize_with = "present")]
    data: Option<Value>,
    #[serde(default, deserialize_with = "present")]
    next: Option<Value>,
    #[serde(default, deserialize_with = "present")]
    tracking: Option<Value>,
    #[serde(default, deserialize_with = "present")]
    rejection: Option<Value>,
    #[serde(default, deserialize_with = "present")]
    error: Option<Value>,
}

impl TryFrom<HandledLoadWire> for HandledLoad {
    type Error = String;
    fn try_from(wire: HandledLoadWire) -> std::result::Result<Self, String> {
        if wire.tracking.is_some() && wire.data.is_none() {
            return Err("a Load answer carries tracking only beside its data".into());
        }
        match (wire.data, wire.next, wire.rejection, wire.error) {
            (None, None, Some(rejection), None) => rejection
                .as_str()
                .filter(|code| valid_code(code))
                .map(|code| Self::Rejected {
                    rejection: code.into(),
                })
                .ok_or_else(|| "invalid rejection code".into()),
            (None, None, None, Some(error)) => error
                .as_str()
                .map(|error| Self::Failed {
                    error: error.into(),
                })
                .ok_or_else(|| "invalid handler error".into()),
            (Some(data), next, None, None) => {
                let tracking = match wire.tracking {
                    None => vec![],
                    Some(tracking) => serde_json::from_value::<Vec<TrackIntent>>(tracking)
                        .map_err(|e| format!("invalid Load tracking: {e}"))?,
                };
                Ok(Self::Settled {
                    data,
                    next,
                    tracking,
                })
            }
            _ => Err("invalid Load handler settlement".into()),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HandledWire {
    #[serde(default, deserialize_with = "present")]
    changes: Option<Value>,
    #[serde(default, deserialize_with = "present")]
    declarations: Option<Value>,
    #[serde(default, deserialize_with = "present")]
    rejection: Option<Value>,
    #[serde(default, deserialize_with = "present")]
    error: Option<Value>,
}

impl TryFrom<HandledWire> for Handled {
    type Error = String;
    fn try_from(wire: HandledWire) -> std::result::Result<Self, String> {
        match (wire.changes, wire.declarations, wire.rejection, wire.error) {
            (None, None, Some(rejection), None) => rejection
                .as_str()
                .filter(|code| valid_code(code))
                .map(|code| Self::Rejected {
                    rejection: code.into(),
                })
                .ok_or_else(|| "invalid rejection code".into()),
            (None, None, None, Some(error)) => error
                .as_str()
                .map(|error| Self::Failed {
                    error: error.into(),
                })
                .ok_or_else(|| "invalid handler error".into()),
            (_, _, Some(_), _) | (_, _, _, Some(_)) => Err(
                "a settlement carries changes and declarations, a rejection or a failure, not several"
                    .into(),
            ),
            (Some(changes), Some(declarations), None, None) => {
                let (changes, declarations) = effects(changes, declarations)?;
                Ok(Self::Settled {
                    changes,
                    declarations,
                })
            }
            _ => Err("invalid handler settlement".into()),
        }
    }
}

/// Issue one typed request and decode the typed answer. `Host::call` keeps its
/// `Value` shape, so implementations outside this crate still compile.
pub trait HostExt {
    fn call_typed<'a, R: DeserializeOwned + Send + 'a>(
        &'a self,
        request: HostRequest,
    ) -> Pin<Box<dyn Future<Output = Result<R>> + Send + 'a>>;
}

impl<H: Host + ?Sized> HostExt for H {
    fn call_typed<'a, R: DeserializeOwned + Send + 'a>(
        &'a self,
        request: HostRequest,
    ) -> Pin<Box<dyn Future<Output = Result<R>> + Send + 'a>> {
        Box::pin(async move {
            let encoded = serde_json::to_value(&request)
                .map_err(|error| Error::new(code::INTERNAL, error.to_string()))?;
            let response = self.call(encoded).await?;
            request.validate_response(&response)?;
            serde_json::from_value(response).map_err(|error| request.invalid_response(error))
        })
    }
}

fn legacy_upsert() -> PositionKind {
    PositionKind::Upsert
}
