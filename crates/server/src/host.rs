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
use crate::scope_members::{MemberDelta, MemberPosition, MemberState, PositionKind, check_tag};
use crate::{Error, Host, Result, code, valid_code};
use axton_core::{LoadNext, RecordKey, canonical_json, check_scope, read_counter};
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

/// A Scope name under the one protocol rule ([`check_scope`]).
fn scope_name<'de, D: Deserializer<'de>>(deserializer: D) -> std::result::Result<String, D::Error> {
    let scope = String::deserialize(deserializer)?;
    check_scope(&scope)
        .map_err(|error| serde::de::Error::custom(format!("invalid scope: {error}")))?;
    Ok(scope)
}

/// The Scopes `lockScopes` names: at least one, each valid, in strictly
/// increasing byte order, which is the one lock order every writer uses.
fn lock_order<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<String>, D::Error> {
    let scopes = Vec::<String>::deserialize(deserializer)?;
    if scopes.is_empty() {
        return Err(serde::de::Error::custom("lockScopes names no Scope"));
    }
    for scope in &scopes {
        check_scope(scope)
            .map_err(|error| serde::de::Error::custom(format!("invalid scope: {error}")))?;
    }
    if scopes.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(serde::de::Error::custom(
            "Scopes must be distinct and in canonical byte order",
        ));
    }
    Ok(scopes)
}

/// The tags a selector names: each valid, distinct, in canonical byte order.
fn tag_order<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<String>, D::Error> {
    let tags = Vec::<String>::deserialize(deserializer)?;
    for tag in &tags {
        check_tag(tag).map_err(serde::de::Error::custom)?;
    }
    if tags.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(serde::de::Error::custom(
            "tags must be distinct and in canonical byte order",
        ));
    }
    Ok(tags)
}

/// A record as the Scope operations name it: its Model and the canonical
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

/// Tags as a set: each valid and none repeated, in whatever order answered.
fn tag_set(tags: Vec<String>) -> std::result::Result<BTreeSet<String>, String> {
    let mut set = BTreeSet::new();
    for tag in tags {
        check_tag(&tag)?;
        if !set.insert(tag) {
            return Err("duplicate tag".into());
        }
    }
    Ok(set)
}

/// `{model, identityKey}` on the wire.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct KeyWire {
    model: String,
    identity_key: String,
}

/// `readScopeMembers`' `explicitKeys`: records as `{model, identityKey}`.
mod record_keys {
    use super::*;
    pub fn serialize<S: serde::Serializer>(
        keys: &[RecordKey],
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_seq(keys.iter().map(|key| KeyWire {
            model: key.model.clone(),
            identity_key: encoded_identity(key),
        }))
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Vec<RecordKey>, D::Error> {
        Vec::<KeyWire>::deserialize(deserializer)?
            .into_iter()
            .map(|key| record_key(key.model, &key.identity_key))
            .collect::<std::result::Result<_, _>>()
            .map_err(serde::de::Error::custom)
    }
}

/// [`MemberState`] on the wire: `{model, identityKey, tags}`.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct MemberStateWire {
    model: String,
    identity_key: String,
    tags: Vec<String>,
}
impl TryFrom<MemberStateWire> for MemberState {
    type Error = String;
    fn try_from(wire: MemberStateWire) -> std::result::Result<Self, String> {
        Ok(Self {
            key: record_key(wire.model, &wire.identity_key)?,
            tags: tag_set(wire.tags)?,
        })
    }
}
impl From<MemberState> for MemberStateWire {
    fn from(state: MemberState) -> Self {
        Self {
            identity_key: encoded_identity(&state.key),
            model: state.key.model,
            tags: state.tags.into_iter().collect(),
        }
    }
}

/// [`MemberDelta`] on the wire: `{scope, model, identity, identityKey,
/// present, tags, publish}`. An absent pair carries no tags and always
/// publishes its removal.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct MemberDeltaWire {
    scope: String,
    model: String,
    identity: Value,
    identity_key: String,
    present: bool,
    tags: Vec<String>,
    publish: bool,
}
impl TryFrom<MemberDeltaWire> for MemberDelta {
    type Error = String;
    fn try_from(wire: MemberDeltaWire) -> std::result::Result<Self, String> {
        check_scope(&wire.scope).map_err(|error| format!("invalid scope: {error}"))?;
        let key = record_key(wire.model, &wire.identity_key)?;
        if key.identity != wire.identity {
            return Err("identity and identityKey name different records".into());
        }
        let tags = tag_set(wire.tags)?;
        if !wire.present && (!tags.is_empty() || !wire.publish) {
            return Err("a removed member carries no tags and always publishes".into());
        }
        Ok(Self {
            scope: wire.scope,
            key,
            present: wire.present,
            tags,
            publish: wire.publish,
        })
    }
}
impl From<MemberDelta> for MemberDeltaWire {
    fn from(delta: MemberDelta) -> Self {
        Self {
            scope: delta.scope,
            identity_key: encoded_identity(&delta.key),
            model: delta.key.model,
            identity: delta.key.identity,
            present: delta.present,
            tags: delta.tags.into_iter().collect(),
            publish: delta.publish,
        }
    }
}

/// [`MemberPosition`] on the wire: `{scope, model, identityKey, cursor, kind}`.
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct MemberPositionWire {
    scope: String,
    model: String,
    identity_key: String,
    #[serde(with = "cursor")]
    cursor: u64,
    kind: PositionKind,
}
impl TryFrom<MemberPositionWire> for MemberPosition {
    type Error = String;
    fn try_from(wire: MemberPositionWire) -> std::result::Result<Self, String> {
        check_scope(&wire.scope).map_err(|error| format!("invalid scope: {error}"))?;
        Ok(Self {
            scope: wire.scope,
            key: record_key(wire.model, &wire.identity_key)?,
            cursor: wire.cursor,
            kind: wire.kind,
        })
    }
}
impl From<MemberPosition> for MemberPositionWire {
    fn from(position: MemberPosition) -> Self {
        Self {
            scope: position.scope,
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
pub const OPERATIONS: [&str; 21] = [
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
    "memberships",
    "lockScopes",
    "readScopeMembers",
    "applyScopeMembers",
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
    /// The scope's current head cursor.
    Head { scope: String },
    /// Retained upsert and removal log rows after `after`, at most `limit`
    /// in cursor order. Identity comes from centralized record metadata.
    Scan {
        scope: String,
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
    /// Execute one generated Load handler for one page: the normalized flat
    /// arguments and the page's continuation (`null` on the first page). Its
    /// context declares no changes: the answer carries identities, the next
    /// continuation and the Scope additions its add-only handles declared.
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
    /// no scope: the same identity, version and stamp describe the same
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
    LockRecord { model: String, identity_key: String },
    /// The Scopes this record is a persistent member of, independent of
    /// positions and subscribers: the recipients of a touch.
    Memberships { model: String, identity_key: String },
    /// Serialize membership changes on these Scopes: lock each existing
    /// Scope row, in exactly this order (canonical byte order), until the
    /// transaction ends. Creates no Scope. Every settlement takes its
    /// Scopes this way before any record guard.
    LockScopes {
        #[serde(deserialize_with = "lock_order")]
        scopes: Vec<String>,
    },
    /// The live members of the locked `scope` that `explicitKeys` names or
    /// that carry one of `tags`, or all present members when `all` is true,
    /// each once with its complete current tags. Reads only.
    ReadScopeMembers {
        #[serde(deserialize_with = "scope_name")]
        scope: String,
        #[serde(with = "record_keys")]
        explicit_keys: Vec<RecordKey>,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        all: bool,
        #[serde(deserialize_with = "tag_order")]
        tags: Vec<String>,
    },
    /// Persist final member states, in the caller's transaction, without
    /// re-evaluating any selector. A present delta makes the record a member
    /// with exactly `tags` (its metadata row must exist; a missing Scope
    /// starts at head zero); an absent one deletes the member and its tags. A
    /// published delta takes the Scope's next position (`upsert` or
    /// `remove`), consecutive per Scope in delta order; an unpublished one
    /// keeps the member's existing position. Answers one position per delta,
    /// in delta order.
    ApplyScopeMembers { deltas: Vec<MemberDelta> },
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
            Self::HandleLoad { .. } => "handleLoad".into(),
            Self::Load { .. } => "load".into(),
            Self::AdvanceStamp { .. } => "advanceStamp".into(),
            Self::EnsureStamp { .. } => "ensureStamp".into(),
            Self::ReadStamps { .. } => "readStamps".into(),
            Self::LockRecord { .. } => "lockRecord".into(),
            Self::Memberships { .. } => "memberships".into(),
            Self::LockScopes { .. } => "lockScopes".into(),
            Self::ReadScopeMembers { .. } => "readScopeMembers".into(),
            Self::ApplyScopeMembers { .. } => "applyScopeMembers".into(),
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
            Self::Handle { .. } | Self::HandleAction { .. } | Self::HandleLoad { .. } => {
                code::HANDLER_INVALID
            }
            Self::Load { .. } => code::LOADER_INVALID,
            Self::Head { .. }
            | Self::Savepoint { .. }
            | Self::Rollback { .. }
            | Self::Release { .. }
            | Self::AdvanceStamp { .. }
            | Self::EnsureStamp { .. }
            | Self::ReadStamps { .. }
            | Self::LockRecord { .. }
            | Self::Memberships { .. }
            | Self::LockScopes { .. }
            | Self::ReadScopeMembers { .. }
            | Self::ApplyScopeMembers { .. } => code::HOST_INVALID,
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

/// One retained scope position with centralized identity. Upserts carry
/// the current content stamp from the Loader's snapshot; removals need no
/// stamp and never enter a Loader. A missing kind is legacy upsert only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Invalidation {
    #[serde(default = "legacy_upsert")]
    pub kind: PositionKind,
    pub scope: String,
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

/// The answer to `memberships`: unique, valid Scope names. The persistence
/// answers them sorted by its own collation; Rust holds them in canonical byte
/// order, so every consumer iterates Scopes the same way.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Vec<String>")]
pub struct Memberships(pub BTreeSet<String>);

impl TryFrom<Vec<String>> for Memberships {
    type Error = String;
    fn try_from(scopes: Vec<String>) -> std::result::Result<Self, String> {
        let mut members = BTreeSet::new();
        for scope in scopes {
            check_scope(&scope).map_err(|error| format!("invalid scope: {error}"))?;
            if !members.insert(scope) {
                return Err("duplicate membership scope".into());
            }
        }
        Ok(Self(members))
    }
}

/// The answer to `readScopeMembers`.
pub type ScopeMembers = Vec<MemberState>;

/// The answer to `applyScopeMembers`: one position per delta, in order.
pub type Positions = Vec<MemberPosition>;

/// A record a handler names: an additional changed record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordRef {
    pub model: String,
    pub identity: Value,
}

/// One persistent Scope declaration, tagged by `kind` on the wire. Intents
/// form an ordered list the engine reduces in order to each Scope's final
/// state ([Publish](../../../docs/engineering/architecture/server/engine/publish.md)):
/// `add` makes the record a member and unions `tags` with its labels (`[]`
/// adds none), `remove` releases the record's whole membership, and a predicate selection releases the matching membership.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum ScopeIntent {
    Add {
        scope: String,
        record: RecordRef,
        tags: Vec<String>,
    },
    Remove {
        scope: String,
        record: RecordRef,
    },
    TagAdd {
        scope: String,
        record: RecordRef,
        tags: Vec<String>,
    },
    TagRemove {
        scope: String,
        record: RecordRef,
        tags: Vec<String>,
    },
    DetachTags {
        scope: String,
        tags: Vec<String>,
    },
    Select {
        scope: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        model: Option<String>,
        predicate: crate::scope_predicate::ScopePredicate,
        action: crate::scope_members::SelectionAction,
    },
}

impl ScopeIntent {
    /// The Scope every kind names.
    pub fn scope(&self) -> &str {
        match self {
            Self::Add { scope, .. }
            | Self::Remove { scope, .. }
            | Self::TagAdd { scope, .. }
            | Self::TagRemove { scope, .. }
            | Self::DetachTags { scope, .. }
            | Self::Select { scope, .. } => scope,
        }
    }
    /// Record membership and label edits name one identity; selectors name none.
    pub fn record(&self) -> Option<&RecordRef> {
        match self {
            Self::Add { record, .. }
            | Self::Remove { record, .. }
            | Self::TagAdd { record, .. }
            | Self::TagRemove { record, .. } => Some(record),
            Self::DetachTags { .. } | Self::Select { .. } => None,
        }
    }
}

/// The effects of one settlement, shared by modern handlers, legacy handlers
/// and external transactions: the changed records beyond any input targets,
/// and the ordered Scope intents. There is no implicit publication.
fn effects(changes: Value, memberships: Value) -> std::result::Result<Effects, String> {
    let changes: Vec<RecordRef> = serde_json::from_value(changes)
        .map_err(|error| format!("invalid handler changes: {error}"))?;
    let memberships: Vec<ScopeIntent> = serde_json::from_value(memberships)
        .map_err(|error| format!("invalid handler memberships: {error}"))?;
    let malformed = |r: &RecordRef| r.model.is_empty() || !r.identity.is_object();
    if changes.iter().any(malformed)
        || memberships
            .iter()
            .any(|m| m.scope().is_empty() || m.record().is_some_and(malformed))
    {
        return Err("invalid handler settlement".into());
    }
    Ok((changes, memberships))
}
type Effects = (Vec<RecordRef>, Vec<ScopeIntent>);

/// The answer to `handle`: the records the handler changed beyond the
/// uploaded operations and its membership intents, a rejection code, or a
/// failure carrying a thrown handler error. Carrying more than one of these,
/// or none, is refused.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged, try_from = "HandledWire")]
pub enum Handled {
    Settled {
        changes: Vec<RecordRef>,
        memberships: Vec<ScopeIntent>,
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
        memberships: Vec<ScopeIntent>,
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

/// The answer to `handleLoad`: the page's identity lists, the next
/// continuation and the membership intents its add-only Scope handles
/// declared, a rejection code, or a failure carrying a thrown handler error.
/// A Load context declares no changes, so an answer carrying `changes`, or
/// memberships beside a rejection or failure, or anything else is refused.
///
/// `data` and `next` are carried as answered (`next` is `None` when the
/// member is absent): the engine judges them, so a missing or malformed
/// continuation is the page's `load.invalid_continuation` and malformed data
/// its `handler.invalid`, whichever host bridge produced them. An absent
/// `memberships` (an older host) is an empty list and is encoded absent;
/// `null` or a malformed intent is refused. A structurally valid intent
/// decodes even when a Load may not declare it (a removal, a tag selector, a
/// record outside the page): the engine judges those so every host fails the
/// page alike.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged, try_from = "HandledLoadWire")]
pub enum HandledLoad {
    Settled {
        data: Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        next: Option<Value>,
        #[serde(skip_serializing_if = "Vec::is_empty")]
        memberships: Vec<ScopeIntent>,
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
    memberships: Option<Value>,
    #[serde(default, deserialize_with = "present")]
    rejection: Option<Value>,
    #[serde(default, deserialize_with = "present")]
    error: Option<Value>,
}

impl TryFrom<HandledLoadWire> for HandledLoad {
    type Error = String;
    fn try_from(wire: HandledLoadWire) -> std::result::Result<Self, String> {
        if wire.memberships.is_some() && wire.data.is_none() {
            return Err("a Load answer carries memberships only beside its data".into());
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
                let memberships = match wire.memberships {
                    None => vec![],
                    Some(memberships) => effects(Value::Array(vec![]), memberships)?.1,
                };
                Ok(Self::Settled {
                    data,
                    next,
                    memberships,
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

fn legacy_upsert() -> PositionKind {
    PositionKind::Upsert
}
