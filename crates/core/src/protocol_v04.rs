//! Version-separated 0.4 admission and durable progress contract.
//!
//! These codecs do not switch a runtime to protocol 4. Consumers must first
//! implement the publication, manifest and ownership rules in protocol/0.4.md.
use crate::{RecordKey, Result, canonical_json, check_stream, counter, invalid, normalize_call_id};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

pub const PROTOCOL: u64 = 4;
pub const DIGEST_ALGORITHM: &str = "sha256";
fn digest<T: Serialize + Validate>(value: &T, domain: &str) -> Result<String> {
    let mut digest = Sha256::new();
    digest.update(b"axton:protocol4:sha256:");
    digest.update(domain.as_bytes());
    digest.update([0]);
    digest.update(encode(value)?);
    Ok(format!("{:x}", digest.finalize()))
}
fn check_digest(value: &str) -> Result<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(invalid("invalid SHA-256 digest"));
    }
    Ok(())
}

/// Structural and counter admission shared by every transport adapter.
pub trait Validate {
    fn validate(&self) -> Result<()>;
}
pub fn decode<T: DeserializeOwned + Validate>(bytes: &[u8]) -> Result<T> {
    let value: T = serde_json::from_slice(bytes)?;
    value.validate()?;
    Ok(value)
}
pub fn encode<T: Serialize + Validate>(value: &T) -> Result<Vec<u8>> {
    value.validate()?;
    Ok(canonical_json(&serde_json::to_value(value)?)?.into_bytes())
}
fn nonblank(value: &str) -> Result<()> {
    if value.trim().is_empty() {
        Err(invalid("nonblank identifier required"))
    } else {
        Ok(())
    }
}
fn position(value: u64) -> Result<()> {
    counter(value)?;
    if value == 0 {
        return Err(invalid("positive Stream cursor required"));
    }
    Ok(())
}
fn key(value: &RecordKey) -> Result<()> {
    nonblank(&value.model)?;
    if !value.identity.is_object() {
        return Err(invalid("identity must be an object"));
    }
    Ok(())
}
fn state(value: &Value) -> Result<()> {
    if !value.is_null() && !value.is_object() {
        return Err(invalid("state must be object or null"));
    }
    Ok(())
}

// Strict wrappers are local to v04. The legacy shared types intentionally
// retain their original wire behavior; opaque application Values stay opaque.
fn strict_key<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<RecordKey, D::Error> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Key {
        model: String,
        identity: Value,
    }
    let value = Key::deserialize(deserializer)?;
    Ok(RecordKey {
        model: value.model,
        identity: value.identity,
    })
}
fn strict_completion<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<crate::CallCompletion, D::Error> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase", deny_unknown_fields)]
    struct Completion {
        call_id: String,
        outcome: Outcome,
    }
    #[derive(Deserialize)]
    #[serde(tag = "status", rename_all = "camelCase", deny_unknown_fields)]
    enum Outcome {
        Succeeded {
            result: Value,
        },
        Failed {
            code: String,
            execution: crate::ExecutionState,
        },
    }
    let value = Completion::deserialize(deserializer)?;
    Ok(crate::CallCompletion {
        call_id: value.call_id,
        outcome: match value.outcome {
            Outcome::Succeeded { result } => crate::ActionOutcome::Succeeded { result },
            Outcome::Failed { code, execution } => crate::ActionOutcome::Failed { code, execution },
        },
    })
}

/// Stable identity of one physical Store; credentials and schema hashes are
/// intentionally absent. File exclusivity belongs to the persistence adapter.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoreBinding {
    pub backend: String,
    pub viewer: String,
    pub stream: String,
    pub contract: String,
}
impl Validate for StoreBinding {
    fn validate(&self) -> Result<()> {
        for value in [&self.backend, &self.viewer, &self.contract] {
            nonblank(value)?;
        }
        check_stream(&self.stream)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestContext {
    pub protocol: u64,
    pub binding: StoreBinding,
    pub materialization: String,
    /// Persisted Store lifecycle identity: normal reopen/reconnect retains it;
    /// explicit reset changes it. It is not a connection or actor generation.
    pub incarnation: String,
}
impl Validate for RequestContext {
    fn validate(&self) -> Result<()> {
        if self.protocol != PROTOCOL {
            return Err(invalid("unsupported_protocol"));
        }
        self.binding.validate()?;
        nonblank(&self.materialization)?;
        nonblank(&self.incarnation)
    }
}
impl RequestContext {
    /// Commit-time admission, after authentication. Caller authorization is
    /// never inferred from matching a Stream name.
    pub fn admit(&self, current: &Self) -> Result<()> {
        self.validate()?;
        current.validate()?;
        if self.binding != current.binding {
            return Err(invalid("binding_mismatch"));
        }
        if self.materialization != current.materialization
            || self.incarnation != current.incarnation
        {
            return Err(invalid("context_mismatch"));
        }
        Ok(())
    }
}
fn default_store() -> bool {
    true
}
fn is_true(value: &bool) -> bool {
    *value
}
/// Boolean mode only; omitted and true have identical canonical bytes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReadIntent {
    pub context: RequestContext,
    pub call_id: String,
    pub name: String,
    pub version: u64,
    pub args: Value,
    #[serde(default = "default_store", skip_serializing_if = "is_true")]
    pub store: bool,
}
impl Validate for ReadIntent {
    fn validate(&self) -> Result<()> {
        self.context.validate()?;
        if normalize_call_id(&self.call_id)? != self.call_id {
            return Err(invalid("noncanonical callId"));
        }
        nonblank(&self.name)?;
        position(self.version)?;
        if !self.args.is_object() {
            return Err(invalid("args must be an object"));
        }
        Ok(())
    }
}

/// A required wire null, unlike Option whose missing member also decodes None.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct NullCursor;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadRecord {
    #[serde(flatten)]
    pub key: RecordKey,
    pub cursor: NullCursor,
    pub state: Value,
}
impl Validate for ReadRecord {
    fn validate(&self) -> Result<()> {
        key(&self.key)?;
        state(&self.state)
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamRecord {
    #[serde(flatten)]
    pub key: RecordKey,
    pub cursor: u64,
    pub state: Value,
}
impl Validate for StreamRecord {
    fn validate(&self) -> Result<()> {
        key(&self.key)?;
        position(self.cursor)?;
        state(&self.state)
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum StreamChange {
    Upsert {
        record: StreamRecord,
    },
    Remove {
        #[serde(deserialize_with = "strict_key")]
        key: RecordKey,
        cursor: u64,
    },
}
impl StreamChange {
    pub fn key(&self) -> &RecordKey {
        match self {
            Self::Upsert { record } => &record.key,
            Self::Remove { key, .. } => key,
        }
    }
    pub fn cursor(&self) -> u64 {
        match self {
            Self::Upsert { record } => record.cursor,
            Self::Remove { cursor, .. } => *cursor,
        }
    }
}
impl Validate for StreamChange {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Upsert { record } => record.validate(),
            Self::Remove {
                key: record_key,
                cursor,
            } => {
                key(record_key)?;
                position(*cursor)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorityProtection {
    pub materialization: String,
    pub cursor: u64,
    pub deleted: bool,
}
/// Data-independent admission evidence. Persist this with the corresponding
/// base change; this helper deliberately owns neither rows nor pending overlays.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordEvidence {
    pub membership: Option<MembershipPosition>,
    pub history: BTreeMap<String, u64>,
    pub current: Option<AuthorityProtection>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MembershipPosition {
    pub cursor: u64,
    pub live: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthorityAdmission {
    Duplicate,
    Newer,
    Rematerialize,
}
impl Validate for RecordEvidence {
    fn validate(&self) -> Result<()> {
        for (context, cursor) in &self.history {
            nonblank(context)?;
            position(*cursor)?;
        }
        if let Some(member) = &self.membership {
            position(member.cursor)?;
        }
        if let Some(current) = &self.current {
            nonblank(&current.materialization)?;
            position(current.cursor)?;
            if self.history.get(&current.materialization) != Some(&current.cursor)
                || self.history.values().any(|cursor| *cursor > current.cursor)
            {
                return Err(invalid(
                    "current protection lacks latest historical evidence",
                ));
            }
            if !current.deleted && self.membership.as_ref().is_some_and(|member| !member.live) {
                return Err(invalid("removed membership cannot protect live content"));
            }
        }
        Ok(())
    }
}
impl RecordEvidence {
    pub fn allows_cache(&self) -> bool {
        self.current.is_none()
    }
    /// Local changes clear content authority, never an existing Stream tombstone.
    pub fn direct_write(&mut self) {
        if self.current.as_ref().is_some_and(|value| !value.deleted) {
            self.current = None;
        }
    }
    /// The caller must first admit the active direct/Stream context. A new
    /// context can adapt the same base position, but never an older position.
    pub fn admission(&self, materialization: &str, cursor: u64) -> Result<AuthorityAdmission> {
        self.validate()?;
        nonblank(materialization)?;
        position(cursor)?;
        if self
            .history
            .get(materialization)
            .is_some_and(|old| cursor <= *old)
            || self
                .membership
                .as_ref()
                .is_some_and(|member| !member.live && cursor <= member.cursor)
        {
            return Ok(AuthorityAdmission::Duplicate);
        }
        let previous = self.history.values().copied().max().unwrap_or(0);
        Ok(if cursor < previous {
            AuthorityAdmission::Duplicate
        } else if cursor == previous {
            AuthorityAdmission::Rematerialize
        } else {
            AuthorityAdmission::Newer
        })
    }
    /// Installs evidence only. Rematerialize requires consumers to adapt base
    /// fields and replay surviving direct/pending operations in this commit.
    pub fn install(&mut self, materialization: &str, cursor: u64, deleted: bool) -> Result<bool> {
        let admission = self.admission(materialization, cursor)?;
        if admission == AuthorityAdmission::Duplicate {
            return Ok(false);
        }
        let preserve_direct =
            admission == AuthorityAdmission::Rematerialize && self.current.is_none();
        self.history.insert(materialization.into(), cursor);
        if !preserve_direct || deleted {
            self.current = Some(AuthorityProtection {
                materialization: materialization.into(),
                cursor,
                deleted,
            });
        }
        if self
            .membership
            .as_ref()
            .is_none_or(|member| member.cursor < cursor)
        {
            self.membership = Some(MembershipPosition { cursor, live: true });
        }
        Ok(true)
    }
    /// Remove releases live content protection, preserving content, historical
    /// authority and real absence protection. Old removes cannot undo re-track.
    pub fn remove(&mut self, cursor: u64) -> Result<bool> {
        self.validate()?;
        position(cursor)?;
        let old = self
            .membership
            .as_ref()
            .map_or(0, |member| member.cursor)
            .max(self.history.values().copied().max().unwrap_or(0));
        if cursor <= old {
            return Ok(false);
        }
        self.membership = Some(MembershipPosition {
            cursor,
            live: false,
        });
        if self.current.as_ref().is_some_and(|value| !value.deleted) {
            self.current = None;
        }
        Ok(true)
    }
    pub fn covers(&self, materialization: &str, cursor: u64) -> bool {
        self.history
            .get(materialization)
            .is_some_and(|held| *held >= cursor)
    }
}

/// Fixed-manifest coverage is separate from ordinary Stream progress.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BootstrapCoverage {
    pub manifest_id: String,
    pub materialization: String,
    pub initial_cursor: u64,
    pub total: u64,
    pub covered: u64,
    pub tail: Option<u64>,
}
impl Validate for BootstrapCoverage {
    fn validate(&self) -> Result<()> {
        nonblank(&self.manifest_id)?;
        nonblank(&self.materialization)?;
        for value in [self.initial_cursor, self.total, self.covered] {
            counter(value)?;
        }
        if self.covered > self.total {
            return Err(invalid("manifest coverage exceeds total"));
        }
        if let Some(tail) = self.tail {
            counter(tail)?;
            if self.covered != self.total || tail < self.initial_cursor {
                return Err(invalid("tail precedes bootstrap start"));
            }
        }
        Ok(())
    }
}
impl BootstrapCoverage {
    pub fn new(
        manifest_id: String,
        materialization: String,
        initial_cursor: u64,
        total: u64,
    ) -> Result<Self> {
        let value = Self {
            manifest_id,
            materialization,
            initial_cursor,
            total,
            covered: 0,
            tail: None,
        };
        value.validate()?;
        Ok(value)
    }
    /// Called inside the same commit as this exact ordinal range's rows.
    pub fn commit_prefix(&mut self, from: u64, to: u64) -> Result<()> {
        self.validate()?;
        counter(from)?;
        counter(to)?;
        if from != self.covered || to <= from || to > self.total {
            return Err(invalid("manifest prefix gap or duplicate"));
        }
        self.covered = to;
        Ok(())
    }
    pub fn set_tail(&mut self, tail: u64) -> Result<()> {
        self.validate()?;
        counter(tail)?;
        if self.covered != self.total
            || tail < self.initial_cursor
            || self.tail.is_some_and(|old| old != tail)
        {
            return Err(invalid("bootstrap tail is immutable"));
        }
        self.tail = Some(tail);
        Ok(())
    }
    pub fn complete(&self, cursor: u64) -> Result<bool> {
        self.validate()?;
        counter(cursor)?;
        Ok(self.covered == self.total && self.tail.is_some_and(|tail| cursor >= tail))
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitUnit {
    pub through: u64,
    pub changes: Vec<StreamChange>,
}
/// A page's units are a complete handled-prefix proof, not a claim that every
/// record cursor is <= its unit boundary. Constraint companions may be ahead.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeltaPage {
    pub context: RequestContext,
    pub page_id: String,
    pub from: u64,
    pub to: u64,
    pub head: u64,
    pub units: Vec<CommitUnit>,
}
impl Validate for DeltaPage {
    fn validate(&self) -> Result<()> {
        self.context.validate()?;
        nonblank(&self.page_id)?;
        for value in [self.from, self.to, self.head] {
            counter(value)?;
        }
        if self.from > self.to || self.to > self.head {
            return Err(invalid("invalid page range"));
        }
        let mut previous = self.from;
        let mut identities = BTreeSet::new();
        let mut positions = BTreeSet::new();
        for unit in &self.units {
            counter(unit.through)?;
            if unit.through <= previous || unit.through > self.to {
                return Err(invalid("unit does not extend page prefix"));
            }
            for change in &unit.changes {
                change.validate()?;
                if change.cursor() <= previous || change.cursor() > self.head {
                    return Err(invalid("change outside page snapshot"));
                }
                if !identities.insert(change.key().encoded()?) || !positions.insert(change.cursor())
                {
                    return Err(invalid("duplicate page identity or position"));
                }
            }
            previous = unit.through;
        }
        if previous != self.to {
            return Err(invalid("page units do not cover final prefix"));
        }
        Ok(())
    }
}
/// Persist together with each unit. Page identity binds replay to a domain-separated SHA-256 digest of canonical
/// bytes, including context and all records, not only a transport's pageId.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PageProgress {
    pub page_id: String,
    pub plan: String,
    pub next_unit: u64,
    pub cursor: u64,
}
impl Validate for PageProgress {
    fn validate(&self) -> Result<()> {
        nonblank(&self.page_id)?;
        check_digest(&self.plan)?;
        counter(self.next_unit)?;
        counter(self.cursor)?;
        Ok(())
    }
}
impl PageProgress {
    pub fn new(page: &DeltaPage) -> Result<Self> {
        Ok(Self {
            page_id: page.page_id.clone(),
            plan: digest(page, "delta-page")?,
            next_unit: 0,
            cursor: page.from,
        })
    }
    pub fn commit(&mut self, page: &DeltaPage, unit: u64) -> Result<()> {
        self.validate()?;
        page.validate()?;
        if self.page_id != page.page_id
            || self.plan != digest(page, "delta-page")?
            || unit != self.next_unit
        {
            return Err(invalid("page resume mismatch"));
        }
        let index = usize::try_from(unit).map_err(|_| invalid("unit index overflow"))?;
        let item = page
            .units
            .get(index)
            .ok_or_else(|| invalid("unit outside page"))?;
        let expected = if index == 0 {
            page.from
        } else {
            page.units[index - 1].through
        };
        if self.cursor != expected {
            return Err(invalid("page progress does not match unit prefix"));
        }
        self.cursor = item.through;
        self.next_unit += 1;
        Ok(())
    }
    pub fn complete(&self, page: &DeltaPage) -> bool {
        self.page_id == page.page_id
            && self.next_unit == page.units.len() as u64
            && self.cursor == page.to
            && digest(page, "delta-page").is_ok_and(|value| value == self.plan)
    }
}

/// A private snapshot acknowledges only its enclosing call's owned operations.
/// It is never admissible as generic Stream authority, even after Remove.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum SettlementTarget {
    Stream {
        #[serde(deserialize_with = "strict_key")]
        key: RecordKey,
        cursor: u64,
        /// Same-transaction accepted readback, usable only as owned null state
        /// after a later installed Remove makes required authority unreachable.
        fallback: ReadRecord,
    },
    Private {
        record: ReadRecord,
    },
}
impl Validate for SettlementTarget {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Stream {
                key: record_key,
                cursor,
                fallback,
            } => {
                key(record_key)?;
                position(*cursor)?;
                fallback.validate()?;
                if &fallback.key != record_key {
                    return Err(invalid("settlement fallback key mismatch"));
                }
                Ok(())
            }
            Self::Private { record } => record.validate(),
        }
    }
}
impl SettlementTarget {
    /// Availability only: consumers still reconcile ownership and constraints
    /// atomically before marking the Call complete. Private snapshots never
    /// install authority or advance evidence here.
    pub fn ready(&self, materialization: &str, evidence: &RecordEvidence) -> bool {
        self.disposition(materialization, evidence)
            .is_ok_and(|value| value != SettlementDisposition::AwaitStream)
    }
    /// This snapshot can finalize only the call-owned operation, after a
    /// FinalizeOwnedNull decision; it never grants generic authority admission.
    pub fn owned_snapshot(&self) -> &ReadRecord {
        match self {
            Self::Stream { fallback, .. } => fallback,
            Self::Private { record } => record,
        }
    }
}

/// The frozen retry identity of a durable Mutation. It keeps its original
/// materialization context through compatible schema reconciliation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MutationIntent {
    pub context: RequestContext,
    pub call_id: String,
    pub name: String,
    pub version: u64,
    pub args: Value,
    pub models: BTreeMap<String, u64>,
}
impl Validate for MutationIntent {
    fn validate(&self) -> Result<()> {
        self.context.validate()?;
        if normalize_call_id(&self.call_id)? != self.call_id {
            return Err(invalid("noncanonical callId"));
        }
        nonblank(&self.name)?;
        position(self.version)?;
        if !self.args.is_object() {
            return Err(invalid("args must be object"));
        }
        for (model, version) in &self.models {
            nonblank(model)?;
            position(*version)?;
        }
        Ok(())
    }
}
impl MutationIntent {
    pub fn digest(&self) -> Result<String> {
        digest(self, "mutation-intent")
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MutationReceipt {
    pub context: RequestContext,
    pub intent_digest: String,
    #[serde(deserialize_with = "strict_completion")]
    pub completion: crate::CallCompletion,
    pub targets: Vec<SettlementTarget>,
}
impl Validate for MutationReceipt {
    fn validate(&self) -> Result<()> {
        self.context.validate()?;
        check_digest(&self.intent_digest)?;
        if normalize_call_id(&self.completion.call_id)? != self.completion.call_id {
            return Err(invalid("noncanonical callId"));
        }
        if let crate::ActionOutcome::Failed { code, execution } = &self.completion.outcome
            && (!crate::valid_code(code)
                || *execution != crate::ExecutionState::Rejected
                || !self.targets.is_empty())
        {
            return Err(invalid("invalid definitive Mutation refusal"));
        }
        let mut keys = BTreeSet::new();
        for target in &self.targets {
            target.validate()?;
            if !keys.insert(target.key().encoded()?) {
                return Err(invalid("duplicate settlement target"));
            }
        }
        Ok(())
    }
}
impl MutationReceipt {
    /// This is the durable exception to live direct response admission. Only
    /// the exact retained queued intent may receive its old-context outcome.
    /// `frozen` must come from this active database's retained durable queue,
    /// never a detached old handle or a caller-supplied intent after reset.
    /// It does not admit old-schema Models as active Stream authority.
    pub fn admit(&self, frozen: &MutationIntent, active: &RequestContext) -> Result<()> {
        self.validate()?;
        frozen.validate()?;
        active.validate()?;
        if self.context.binding != active.binding {
            return Err(invalid("binding_mismatch"));
        }
        if self.context.incarnation != active.incarnation {
            return Err(invalid("context_mismatch"));
        }
        if self.context != frozen.context
            || self.completion.call_id != frozen.call_id
            || self.intent_digest != frozen.digest()?
        {
            return Err(invalid("intent_mismatch"));
        }
        Ok(())
    }
    /// Targets derived from retained Mutation slots/arguments, never callback
    /// companions. Refusal settles ownership using outcome alone.
    pub fn validate_targets(&self, optimistic_keys: &[RecordKey]) -> Result<()> {
        self.validate()?;
        if matches!(self.completion.outcome, crate::ActionOutcome::Failed { .. }) {
            return Ok(());
        }
        let expected = optimistic_keys
            .iter()
            .map(|value| {
                key(value)?;
                value.encoded()
            })
            .collect::<Result<BTreeSet<_>>>()?;
        let actual = self
            .targets
            .iter()
            .map(|value| value.key().encoded())
            .collect::<Result<BTreeSet<_>>>()?;
        if expected != actual {
            return Err(invalid("settlement target manifest mismatch"));
        }
        Ok(())
    }
}
impl SettlementTarget {
    pub fn key(&self) -> &RecordKey {
        match self {
            Self::Stream { key, .. } => key,
            Self::Private { record } => &record.key,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestItem {
    pub ordinal: u64,
    pub change: StreamChange,
}
/// Ordinals address an immutable server-persisted identity manifest. Removed
/// keys are explicit covered items, never silently dropped by a log scan.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManifestPage {
    pub context: RequestContext,
    pub manifest_id: String,
    pub total: u64,
    pub from: u64,
    pub to: u64,
    pub items: Vec<ManifestItem>,
}
impl Validate for ManifestPage {
    fn validate(&self) -> Result<()> {
        self.context.validate()?;
        nonblank(&self.manifest_id)?;
        for value in [self.total, self.from, self.to] {
            counter(value)?;
        }
        if self.from > self.to
            || self.to > self.total
            || self.items.len() as u64 != self.to - self.from
        {
            return Err(invalid("manifest ordinal coverage mismatch"));
        }
        let mut keys = BTreeSet::new();
        for (index, item) in self.items.iter().enumerate() {
            counter(item.ordinal)?;
            item.change.validate()?;
            if item.ordinal != self.from + index as u64
                || !keys.insert(item.change.key().encoded()?)
            {
                return Err(invalid("manifest item mismatch or duplicate key"));
            }
        }
        Ok(())
    }
}
impl BootstrapCoverage {
    /// Invoke inside the same transaction as all covered rows/evidence.
    pub fn commit_page(&mut self, page: &ManifestPage, active: &RequestContext) -> Result<()> {
        page.validate()?;
        page.context.admit(active)?;
        self.validate()?;
        if page.manifest_id != self.manifest_id
            || page.context.materialization != self.materialization
            || page.total != self.total
        {
            return Err(invalid("manifest binding mismatch"));
        }
        if self.total == 0 && page.from == 0 && page.to == 0 {
            return Ok(());
        }
        self.commit_prefix(page.from, page.to)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadDisposition {
    SnapshotOnly,
    Absent,
    Protected,
    StoreCache,
    /// Declared delete-cascade reference points to a currently protected
    /// Stream absence. This is not a child tombstone or general FK check.
    DeletedParent,
}
impl ReadRecord {
    /// Evaluated at local commit, never when the network request starts.
    /// Every variant leaves G/C unchanged; Absent never deletes a local row.
    pub fn disposition(&self, store: bool, evidence: &RecordEvidence) -> Result<ReadDisposition> {
        self.validate()?;
        evidence.validate()?;
        Ok(if !store {
            ReadDisposition::SnapshotOnly
        } else if self.state.is_null() {
            ReadDisposition::Absent
        } else if !evidence.allows_cache() {
            ReadDisposition::Protected
        } else {
            ReadDisposition::StoreCache
        })
    }
}
impl ReadRecord {
    /// Narrow read guard for declared device cascades. Missing/unloaded and
    /// locally deleted parents remain valid partial-cache states. Only a
    /// CURRENT Stream tombstone on the returned delete-cascade target guards
    /// this ordinary null-cursor snapshot; it creates no child G/C/authority.
    /// Parent evidence must be read from this Store in the same commit unit.
    pub fn disposition_with_relations(
        &self,
        store: bool,
        evidence: &RecordEvidence,
        schema: &crate::Schema,
        parents: &BTreeMap<String, RecordEvidence>,
    ) -> Result<ReadDisposition> {
        let disposition = self.disposition(store, evidence)?;
        if disposition != ReadDisposition::StoreCache {
            return Ok(disposition);
        }
        for relation in &schema.model(&self.key.model)?.relations {
            if relation.on_delete != "delete" {
                continue;
            }
            let mut identity = serde_json::Map::new();
            for (local, target) in relation.fields.iter().zip(&relation.target_fields) {
                let value = self
                    .key
                    .identity
                    .get(local)
                    .or_else(|| self.state.get(local))
                    .ok_or_else(|| invalid("read snapshot lacks reference field"))?;
                if value.is_null() {
                    identity.clear();
                    break;
                }
                identity.insert(target.clone(), value.clone());
            }
            if identity.is_empty() {
                continue;
            }
            let target = schema.record_key(&relation.target, &Value::Object(identity))?;
            if let Some(parent) = parents.get(&target.encoded()?) {
                parent.validate()?;
                if parent.current.as_ref().is_some_and(|guard| guard.deleted) {
                    return Ok(ReadDisposition::DeletedParent);
                }
            }
        }
        Ok(disposition)
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadResponse {
    pub context: RequestContext,
    #[serde(deserialize_with = "strict_completion")]
    pub completion: crate::CallCompletion,
    pub records: Vec<ReadRecord>,
}
impl Validate for ReadResponse {
    fn validate(&self) -> Result<()> {
        self.context.validate()?;
        if normalize_call_id(&self.completion.call_id)? != self.completion.call_id {
            return Err(invalid("noncanonical callId"));
        }
        if let crate::ActionOutcome::Failed { code, execution } = &self.completion.outcome
            && (!crate::valid_code(code)
                || *execution != crate::ExecutionState::Rejected
                || !self.records.is_empty())
        {
            return Err(invalid("invalid definitive read refusal"));
        }
        let mut keys = BTreeSet::new();
        for record in &self.records {
            record.validate()?;
            if !keys.insert(record.key.encoded()?) {
                return Err(invalid("duplicate read snapshot"));
            }
        }
        Ok(())
    }
}
impl ReadResponse {
    pub fn admit(
        &self,
        call_id: &str,
        requested: &RequestContext,
        active: &RequestContext,
    ) -> Result<()> {
        self.validate()?;
        requested.admit(active)?;
        if &self.context != requested || self.completion.call_id != call_id {
            return Err(invalid("read correlation mismatch"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FetchIntent {
    pub context: RequestContext,
    pub call_id: String,
    pub model: String,
    pub version: u64,
    pub identity: Value,
    #[serde(default = "default_store", skip_serializing_if = "is_true")]
    pub store: bool,
}
impl Validate for FetchIntent {
    fn validate(&self) -> Result<()> {
        self.context.validate()?;
        if normalize_call_id(&self.call_id)? != self.call_id {
            return Err(invalid("noncanonical callId"));
        }
        key(&RecordKey {
            model: self.model.clone(),
            identity: self.identity.clone(),
        })?;
        position(self.version)
    }
}
impl FetchIntent {
    /// A successful Fetch snapshots exactly its requested key in both modes.
    /// Schema-aware consumers additionally normalize state by retained fields.
    pub fn admit_response(&self, response: &ReadResponse, active: &RequestContext) -> Result<()> {
        self.validate()?;
        response.admit(&self.call_id, &self.context, active)?;
        if let crate::ActionOutcome::Succeeded { result } = &response.completion.outcome {
            if response.records.len() != 1 {
                return Err(invalid("Fetch needs exactly one requested snapshot"));
            }
            let record = &response.records[0];
            if record.key.model != self.model || record.key.identity != self.identity {
                return Err(invalid("Fetch snapshot key mismatch"));
            }
            let expected = if record.state.is_null() {
                Value::Null
            } else {
                let mut full = record.key.identity.as_object().cloned().unwrap_or_default();
                for (field, value) in record.state.as_object().into_iter().flatten() {
                    if full.insert(field.clone(), value.clone()).is_some() {
                        return Err(invalid("Fetch state repeats identity field"));
                    }
                }
                Value::Object(full)
            };
            if &expected != result {
                return Err(invalid("Fetch result differs from snapshot"));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SettlementDisposition {
    AwaitStream,
    InstalledStream,
    FinalizeOwnedNull,
    AcknowledgeProtected,
}
impl SettlementTarget {
    /// This decision grants no row write by itself. The engine must retain
    /// later local operations and atomically validate the rebuilt projection.
    pub fn disposition(
        &self,
        materialization: &str,
        evidence: &RecordEvidence,
    ) -> Result<SettlementDisposition> {
        self.validate()?;
        evidence.validate()?;
        nonblank(materialization)?;
        Ok(match self {
            Self::Stream { cursor, .. } => {
                if evidence.covers(materialization, *cursor) {
                    SettlementDisposition::InstalledStream
                } else if evidence
                    .membership
                    .as_ref()
                    .is_some_and(|member| !member.live && member.cursor > *cursor)
                {
                    if evidence.allows_cache() {
                        SettlementDisposition::FinalizeOwnedNull
                    } else {
                        SettlementDisposition::AcknowledgeProtected
                    }
                } else {
                    SettlementDisposition::AwaitStream
                }
            }
            Self::Private { .. } => {
                if evidence.allows_cache() {
                    SettlementDisposition::FinalizeOwnedNull
                } else {
                    SettlementDisposition::AcknowledgeProtected
                }
            }
        })
    }
}
