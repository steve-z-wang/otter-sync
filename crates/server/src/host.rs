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
use crate::{Error, Host, Result, code, valid_code};
use axton_core::{check_channel, read_counter};
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

/// A Channel name under the one protocol rule ([`check_channel`]).
fn channel_name<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<String, D::Error> {
    let channel = String::deserialize(deserializer)?;
    check_channel(&channel)
        .map_err(|error| serde::de::Error::custom(format!("invalid channel: {error}")))?;
    Ok(channel)
}

fn nullable_string<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error> {
    Option::<String>::deserialize(deserializer)
}

/// Every operation, in the order [`HostRequest`] declares them. The fixture
/// and `packages/server/host-contract.mts` carry the same list; the contract
/// test checks this one against the enum itself.
pub const OPERATIONS: [&str; 18] = [
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
    "load",
    "advanceStamp",
    "ensureStamp",
    "publish",
    "lockRecord",
    "memberships",
    "setMembership",
];

/// Every request the engine issues to a host, tagged by `op` on the wire.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "op",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum HostRequest {
    /// Lock this client's row and report its last accepted batch.
    Claim { owner: String, client_id: String },
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
    /// The channel's current head cursor.
    Head { channel: String },
    /// Invalidation rows after `after` whose record is still a member of the
    /// channel, at most `limit` of them, in cursor order. Membership filters
    /// before the limit; a removed record's row stays but is not answered.
    Scan {
        channel: String,
        after: u64,
        limit: u64,
    },
    /// Open the savepoint that isolates one mutation.
    Savepoint { ordinal: u64 },
    /// Undo one mutation's effects back to its savepoint.
    Rollback { ordinal: u64 },
    /// Discard one mutation's savepoint, keeping its effects.
    Release { ordinal: u64 },
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
    },
    /// Load the current state of these identities as the records of one
    /// retained model read contract (`version`), for this caller. Loads name
    /// no channel: the same identity, version and stamp describe the same
    /// content on every delivery path.
    Load {
        model: String,
        version: u64,
        identities: Vec<Value>,
        owner: String,
    },
    /// Allocate the next stamp of one record: initialize it at 1 or increment it.
    AdvanceStamp { model: String, identity_key: String },
    /// The record's current stamp, initialized at 1 only when it has none.
    EnsureStamp { model: String, identity_key: String },
    /// Invalidate one record on one channel at this stamp, allocating only
    /// the channel cursor. `stamp` must be the record's current stamp.
    Publish {
        channel: String,
        model: String,
        identity: Value,
        identity_key: String,
        stamp: u64,
    },
    /// Write-lock one existing record row without changing its stamp
    /// (`UPDATE ... SET stamp=stamp`), so a concurrent Repeatable Read writer of
    /// the same row restarts instead of acting on a stale snapshot. Never
    /// creates a row: an absent record answers `null`.
    LockRecord { model: String, identity_key: String },
    /// The Channels this record is a persistent member of, independent of
    /// invalidations and subscribers.
    Memberships { model: String, identity_key: String },
    /// Make the record a member of `channel` (`present: true`, creating the
    /// Channel at head zero if needed) or not (`false`). Idempotent both ways;
    /// never allocates a cursor. The record's metadata must exist to add it.
    SetMembership {
        #[serde(deserialize_with = "channel_name")]
        channel: String,
        model: String,
        identity_key: String,
        present: bool,
    },
}

impl HostRequest {
    /// The operation, and the ordinal when the operation carries one.
    pub fn label(&self) -> String {
        match self {
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
            Self::Load { .. } => "load".into(),
            Self::AdvanceStamp { .. } => "advanceStamp".into(),
            Self::EnsureStamp { .. } => "ensureStamp".into(),
            Self::Publish { .. } => "publish".into(),
            Self::LockRecord { .. } => "lockRecord".into(),
            Self::Memberships { .. } => "memberships".into(),
            Self::SetMembership { .. } => "setMembership".into(),
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
            Self::Handle { .. } | Self::HandleAction { .. } => code::HANDLER_INVALID,
            Self::Load { .. } => code::LOADER_INVALID,
            Self::Head { .. }
            | Self::Savepoint { .. }
            | Self::Rollback { .. }
            | Self::Release { .. }
            | Self::AdvanceStamp { .. }
            | Self::EnsureStamp { .. }
            | Self::Publish { .. }
            | Self::LockRecord { .. }
            | Self::Memberships { .. }
            | Self::SetMembership { .. } => code::HOST_INVALID,
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

/// One row of the answer to `scan`: the invalidation's own cursor with the
/// record's *current* stamp, read from the record metadata in the same
/// snapshot the loader will read. A record may have advanced since it was
/// published; the cursor is delivery progress, the stamp is the content version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Invalidation {
    pub channel: String,
    #[serde(with = "cursor")]
    pub cursor: u64,
    pub model: String,
    pub identity: Value,
    pub identity_key: String,
    #[serde(with = "stamp")]
    pub stamp: u64,
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

/// The answer to `lockRecord`: the locked record's unchanged stamp, or `None`
/// when the record has no metadata row (nothing was locked or created).
pub type Locked = Option<Stamped>;

/// The answer to `memberships`: unique, valid Channel names. The persistence
/// answers them sorted by its own collation; Rust holds them in canonical byte
/// order, so every consumer iterates Channels the same way.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Vec<String>")]
pub struct Memberships(pub BTreeSet<String>);

impl TryFrom<Vec<String>> for Memberships {
    type Error = String;
    fn try_from(channels: Vec<String>) -> std::result::Result<Self, String> {
        let mut members = BTreeSet::new();
        for channel in channels {
            check_channel(&channel).map_err(|error| format!("invalid channel: {error}"))?;
            if !members.insert(channel) {
                return Err("duplicate membership channel".into());
            }
        }
        Ok(Self(members))
    }
}

/// The answer to `publish`: the cursor the channel allocated and the stamp
/// the invalidation carries, which must be the one the request named.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Published {
    #[serde(with = "cursor")]
    pub cursor: u64,
    #[serde(with = "stamp")]
    pub stamp: u64,
}

/// A record a handler names: an additional changed record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordRef {
    pub model: String,
    pub identity: Value,
}

/// One persistent Channel membership declaration: the record should
/// (`present`) or should not be a member of `channel`. Intents form an ordered
/// list; for each Channel/record pair the last one is the desired state
/// ([Publish](../../../docs/engineering/architecture/server/engine/publish.md)).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipIntent {
    pub channel: String,
    pub model: String,
    pub identity: Value,
    pub present: bool,
}

/// The effects of one settlement, shared by modern handlers, legacy handlers
/// and external transactions: the changed records beyond any input targets,
/// and the ordered membership intents. There is no implicit publication.
fn effects(changes: Value, memberships: Value) -> std::result::Result<Effects, String> {
    let changes: Vec<RecordRef> = serde_json::from_value(changes)
        .map_err(|error| format!("invalid handler changes: {error}"))?;
    let memberships: Vec<MembershipIntent> = serde_json::from_value(memberships)
        .map_err(|error| format!("invalid handler memberships: {error}"))?;
    let malformed = |model: &str, identity: &Value| model.is_empty() || !identity.is_object();
    if changes.iter().any(|r| malformed(&r.model, &r.identity))
        || memberships
            .iter()
            .any(|m| m.channel.is_empty() || malformed(&m.model, &m.identity))
    {
        return Err("invalid handler settlement".into());
    }
    Ok((changes, memberships))
}
type Effects = (Vec<RecordRef>, Vec<MembershipIntent>);

/// The answer to `handle`: the records the handler changed beyond the
/// uploaded operations and its membership intents, a rejection code, or a
/// failure carrying a thrown handler error. Carrying more than one of these,
/// or none, is refused.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged, try_from = "HandledWire")]
pub enum Handled {
    Settled {
        changes: Vec<RecordRef>,
        memberships: Vec<MembershipIntent>,
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
        memberships: Vec<MembershipIntent>,
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
    memberships: Option<Value>,
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
            wire.memberships,
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
            (Some(outputs), Some(changes), Some(memberships), None, None)
                if outputs.is_object() =>
            {
                let (changes, memberships) = effects(changes, memberships)?;
                Ok(Self::Settled {
                    outputs,
                    changes,
                    memberships,
                })
            }
            _ => Err("invalid Action handler settlement".into()),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HandledWire {
    #[serde(default, deserialize_with = "present")]
    changes: Option<Value>,
    #[serde(default, deserialize_with = "present")]
    memberships: Option<Value>,
    #[serde(default, deserialize_with = "present")]
    rejection: Option<Value>,
    #[serde(default, deserialize_with = "present")]
    error: Option<Value>,
}

impl TryFrom<HandledWire> for Handled {
    type Error = String;
    fn try_from(wire: HandledWire) -> std::result::Result<Self, String> {
        match (wire.changes, wire.memberships, wire.rejection, wire.error) {
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
                "a settlement carries changes and memberships, a rejection or a failure, not several"
                    .into(),
            ),
            (Some(changes), Some(memberships), None, None) => {
                let (changes, memberships) = effects(changes, memberships)?;
                Ok(Self::Settled {
                    changes,
                    memberships,
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
            serde_json::from_value(response).map_err(|error| request.invalid_response(error))
        })
    }
}
