//! The one typed definition of the host operation contract.
//!
//! Every request the engine may issue is a [`HostRequest`] variant and every
//! answer a host may give is one of the response types below. The TypeScript
//! mirror is `packages/server/host-contract.mts` and the shared examples are
//! `fixtures/protocol/host-operations.json`; a change here belongs in all three.
//!
//! `handleAction` and `load` distinguish explicit business refusal from
//! infrastructure failure. A refusal rolls back the Mutation's savepoint;
//! infrastructure failures abort its acceptance transaction for retry.
pub mod error;
pub mod members;
pub use error::{Error, code};
pub use members::*;
pub type Result<T> = std::result::Result<T, Error>;
use axton_core::valid_code;
use axton_core::{check_stream, read_counter};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::{collections::BTreeSet, fmt::Display};

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
counter_field!(cursor, "cursor", true);

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

/// Every operation, in the order [`HostRequest`] declares them. The fixture
/// and `packages/server/host-contract.mts` carry the same list; the contract
/// test checks this one against the enum itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LoaderMode {
    Prepare,
    Canonical,
}

pub const OPERATIONS: [&str; 12] = [
    "protocol05",
    "publicationFence",
    "head",
    "savepoint",
    "rollback",
    "release",
    "handleAction",
    "load",
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
    BootstrapState {
        store_id: String,
    },
    HandleBootstrap05 {
        owner: String,
        store_id: String,
        stream: String,
    },
    FinishBootstrap {
        store_id: String,
    },
    DeliveryHead {
        stream: String,
    },
    DeliveryNow {},
    DeliveryCandidates {
        stream: String,
        after: u64,
        models: Option<Vec<String>>,
        keys: Option<Vec<MemberKey>>,
        capacity: u64,
    },
    SaveDelivery {
        owner: String,
        intent: String,
        header: crate::sync::DeliveryHeader,
        parts: Vec<crate::sync::DeliveryPart>,
    },
    ReadDelivery {
        owner: String,
        context: crate::sync::RequestContext,
        intent: String,
        continuation: crate::sync::Continuation,
    },
    Admit {
        owner: String,
        context: crate::sync::RequestContext,
    },
    /// Read immutable Store ownership without locking Batch progress.
    InspectStore {
        store_id: String,
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
        result: crate::sync::MutationResult,
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
    /// Persisted namespace-wide write fence; acquire before relevant work.
    PublicationFence {},
    /// The stream's current head cursor.
    Head {
        stream: String,
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
    /// Execute one generated Action handler with its normalized flat arguments.
    HandleAction {
        name: String,
        version: u64,
        arguments: Value,
        owner: String,
        call_id: String,
        ordinal: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        context: Option<HandlerContext>,
    },
    /// Load the current state of these identities as the records of one
    /// retained model read contract (`version`), for this caller. Loads name
    /// no Stream: the same identity and version describe current content.
    Load {
        /// Preparation returns empty rows; canonical reads exclude hooks. Frozen
        /// delivery follows preparation in its fenced transaction.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mode: Option<LoaderMode>,
        model: String,
        version: u64,
        identities: Vec<Value>,
        owner: String,
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
                    .any(|(r, s)| r.mode != GuardMode::Lock && !s)
                {
                    return Err(self.invalid_response("missing ensured identity"));
                }
            }
            _ => {}
        }
        Ok(())
    }
    pub fn label(&self) -> String {
        serde_json::to_value(self)
            .ok()
            .and_then(|v| v["op"].as_str().map(str::to_owned))
            .unwrap_or_default()
    }
    fn invalid_code(&self) -> &'static str {
        match self {
            Self::HandleAction { .. } => code::HANDLER_INVALID,
            Self::Load { .. } => code::LOADER_INVALID,
            _ => code::HOST_INVALID,
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

/// The answer to `head`: a bare counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Head(#[serde(with = "counter")] pub u64);

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

/// Trusted carrier constructed only after protocol-5 principal/Store/Stream admission.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HandlerContext {
    pub owner: String,
    pub stream: String,
    pub store_id: String,
    pub materialization: String,
}
