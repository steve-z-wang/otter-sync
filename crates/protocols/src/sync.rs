//! Protocol 5 contract and pure planning helpers. No network or database I/O.
use axton_core::{Result, canonical_json, check_stream, counter, invalid};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

pub const PROTOCOL: u64 = 5;
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
fn text(v: &str) -> Result<()> {
    if v.trim().is_empty() {
        Err(invalid("blank identifier"))
    } else {
        Ok(())
    }
}
fn positive(v: u64) -> Result<()> {
    counter(v)?;
    if v == 0 {
        Err(invalid("positive counter required"))
    } else {
        Ok(())
    }
}
fn hash(domain: &str, value: &Value) -> Result<String> {
    let mut h = Sha256::new();
    h.update(domain.as_bytes());
    h.update([0]);
    h.update(canonical_json(value)?.as_bytes());
    Ok(format!("{:x}", h.finalize()))
}
fn digest(v: &str) -> Result<()> {
    if v.len() != 64
        || !v
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        Err(invalid("invalid digest"))
    } else {
        Ok(())
    }
}
fn state(v: &Value) -> Result<()> {
    if v.is_null() || v.is_object() {
        Ok(())
    } else {
        Err(invalid("state must be object or null"))
    }
}
// Option<Value> would also accept a missing cursor, which is not a null-cursor read.
fn null_cursor<'de, D: serde::Deserializer<'de>>(d: D) -> std::result::Result<(), D::Error> {
    let v = Value::deserialize(d)?;
    if v.is_null() {
        Ok(())
    } else {
        Err(serde::de::Error::custom("read cursor must be null"))
    }
}
fn required_option<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
) -> std::result::Result<Option<T>, D::Error> {
    Option::<T>::deserialize(d)
}
fn default_store() -> bool {
    true
}
fn serialize_null<S: serde::Serializer>(_: &(), s: S) -> std::result::Result<S::Ok, S::Error> {
    s.serialize_none()
}

/// Store ID is the durable file lifecycle. Schema context may change independently.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RequestContext {
    pub protocol: u64,
    pub store_id: String,
    pub stream: String,
    pub materialization: String,
}
impl Validate for RequestContext {
    fn validate(&self) -> Result<()> {
        if self.protocol != PROTOCOL {
            return Err(invalid("unsupported_protocol"));
        }
        text(&self.store_id)?;
        check_stream(&self.stream)?;
        text(&self.materialization)
    }
}
impl RequestContext {
    pub fn admit_store(&self, active: &Self) -> Result<()> {
        self.validate()?;
        active.validate()?;
        if self.store_id != active.store_id || self.stream != active.stream {
            Err(invalid("Store context mismatch"))
        } else {
            Ok(())
        }
    }
    pub fn admit(&self, active: &Self) -> Result<()> {
        self.admit_store(active)?;
        if self.materialization != active.materialization {
            Err(invalid("materialization mismatch"))
        } else {
            Ok(())
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordKey {
    pub model: String,
    pub identity: Value,
}
impl RecordKey {
    pub fn encoded(&self) -> Result<String> {
        self.validate()?;
        canonical_json(&serde_json::to_value(self)?)
    }
}
impl Validate for RecordKey {
    fn validate(&self) -> Result<()> {
        text(&self.model)?;
        if !self.identity.is_object() {
            return Err(invalid("identity must be object"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadRecord {
    pub key: RecordKey,
    #[serde(deserialize_with = "null_cursor", serialize_with = "serialize_null")]
    pub cursor: (),
    pub state: Value,
}
impl Validate for ReadRecord {
    fn validate(&self) -> Result<()> {
        self.key.validate()?;
        state(&self.state)
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum SettlementTarget {
    Stream {
        key: RecordKey,
        cursor: u64,
        fallback: ReadRecord,
    },
    Private {
        record: ReadRecord,
    },
}
impl SettlementTarget {
    pub fn key(&self) -> &RecordKey {
        match self {
            Self::Stream { key, .. } => key,
            Self::Private { record } => &record.key,
        }
    }
}
impl Validate for SettlementTarget {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Stream {
                key,
                cursor,
                fallback,
            } => {
                key.validate()?;
                positive(*cursor)?;
                fallback.validate()?;
                if key != &fallback.key {
                    return Err(invalid("fallback target mismatch"));
                }
                Ok(())
            }
            Self::Private { record } => record.validate(),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Operation {
    Argument,
    Create,
    Update,
    Delete,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MutationOperation {
    pub step: u64,
    pub input_path: String,
    pub operation: Operation,
    #[serde(deserialize_with = "required_option")]
    pub model: Option<String>,
    pub identity: Value,
    pub value: Value,
}
impl Validate for MutationOperation {
    fn validate(&self) -> Result<()> {
        counter(self.step)?;
        text(&self.input_path)?;
        match self.operation {
            Operation::Argument => {
                if self.model.is_some() || !self.identity.is_null() {
                    return Err(invalid("argument cannot name Model"));
                }
            }
            _ => {
                RecordKey {
                    model: self
                        .model
                        .clone()
                        .ok_or_else(|| invalid("Model operation lacks Model"))?,
                    identity: self.identity.clone(),
                }
                .validate()?;
                if self.operation == Operation::Delete {
                    if !self.value.is_null() {
                        return Err(invalid("delete payload must be null"));
                    }
                } else if !self.value.is_object() {
                    return Err(invalid("Model operation payload must be object"));
                }
            }
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mutation {
    pub id: u64,
    pub name: String,
    pub version: u64,
    pub descriptor: String,
    pub operations: Vec<MutationOperation>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MutationRequest {
    #[serde(flatten)]
    pub context: RequestContext,
    pub batch_id: u64,
    pub digest: String,
    pub mutations: Vec<Mutation>,
}
impl Validate for MutationRequest {
    fn validate(&self) -> Result<()> {
        validate_batch(self)
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum MutationOutcome {
    Accepted {
        sync_cursor: u64,
        result: Value,
        targets: Vec<SettlementTarget>,
    },
    Rejected {
        code: String,
        #[serde(deserialize_with = "required_option")]
        message: Option<String>,
    },
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MutationResult {
    pub mutation_id: u64,
    pub outcome: MutationOutcome,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BatchAcknowledgement {
    #[serde(flatten)]
    pub context: RequestContext,
    pub batch_id: u64,
    pub digest: String,
    pub results: Vec<MutationResult>,
}
impl Validate for BatchAcknowledgement {
    fn validate(&self) -> Result<()> {
        self.context.validate()?;
        positive(self.batch_id)?;
        digest(&self.digest)?;
        let mut ids = BTreeSet::new();
        for result in &self.results {
            positive(result.mutation_id)?;
            if !ids.insert(result.mutation_id) {
                return Err(invalid("duplicate result"));
            }
            match &result.outcome {
                MutationOutcome::Accepted {
                    sync_cursor,
                    targets,
                    ..
                } => {
                    counter(*sync_cursor)?;
                    let mut keys = BTreeSet::new();
                    for target in targets {
                        target.validate()?;
                        if let SettlementTarget::Stream { cursor, .. } = target
                            && cursor > sync_cursor
                        {
                            return Err(invalid("settlement target exceeds fenced head"));
                        }
                        if !keys.insert(target.key().encoded()?) {
                            return Err(invalid("duplicate settlement target"));
                        }
                    }
                }
                MutationOutcome::Rejected { code, .. } => {
                    if !axton_core::valid_code(code) {
                        return Err(invalid("invalid rejection code"));
                    }
                }
            }
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum ReadOutcome {
    Succeeded {
        result: Value,
    },
    Failed {
        code: String,
        #[serde(deserialize_with = "required_option")]
        message: Option<String>,
    },
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReadResponse {
    #[serde(flatten)]
    pub context: RequestContext,
    pub request_id: String,
    pub outcome: ReadOutcome,
    pub records: Vec<ReadRecord>,
}
impl Validate for ReadResponse {
    fn validate(&self) -> Result<()> {
        self.context.validate()?;
        text(&self.request_id)?;
        if let ReadOutcome::Failed { code, .. } = &self.outcome
            && (!axton_core::valid_code(code) || !self.records.is_empty())
        {
            return Err(invalid("invalid definitive read failure"));
        }
        let mut seen = BTreeSet::new();
        for r in &self.records {
            r.validate()?;
            if !seen.insert(r.key.encoded()?) {
                return Err(invalid("duplicate read record"));
            }
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum DeliveryPurpose {
    Bootstrap,
    Sync,
    Settlement { batch_id: u64, mutation_id: u64 },
    Schema { previous_materialization: String },
}
impl Validate for DeliveryPurpose {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Settlement {
                batch_id,
                mutation_id,
            } => {
                positive(*batch_id)?;
                positive(*mutation_id)
            }
            Self::Schema {
                previous_materialization,
            } => text(previous_materialization),
            _ => Ok(()),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Continuation {
    pub plan_id: String,
    pub digest: String,
    pub unit: u64,
    pub part: u64,
}
impl Validate for Continuation {
    fn validate(&self) -> Result<()> {
        text(&self.plan_id)?;
        digest(&self.digest)?;
        counter(self.unit)?;
        counter(self.part)?;
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeltaRequest {
    #[serde(flatten)]
    pub context: RequestContext,
    pub after: u64,
    pub through: u64,
    #[serde(default)]
    pub bootstrap: bool,
    pub continuation: Option<Continuation>,
}
impl Validate for DeltaRequest {
    fn validate(&self) -> Result<()> {
        self.context.validate()?;
        counter(self.after)?;
        counter(self.through)?;
        if self.after > self.through {
            return Err(invalid("inverted range"));
        }
        if let Some(c) = &self.continuation {
            c.validate()?;
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum AuthorityChange {
    Record {
        key: RecordKey,
        cursor: u64,
        state: Value,
    },
    Remove {
        key: RecordKey,
        cursor: u64,
    },
}
impl AuthorityChange {
    pub fn key(&self) -> &RecordKey {
        match self {
            Self::Record { key, .. } | Self::Remove { key, .. } => key,
        }
    }
    pub fn cursor(&self) -> u64 {
        match self {
            Self::Record { cursor, .. } | Self::Remove { cursor, .. } => *cursor,
        }
    }
}
impl Validate for AuthorityChange {
    fn validate(&self) -> Result<()> {
        self.key().validate()?;
        positive(self.cursor())?;
        if let Self::Record { state: v, .. } = self {
            state(v)?;
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryUnit {
    pub index: u64,
    #[serde(deserialize_with = "required_option")]
    pub through: Option<u64>,
    pub changes: Vec<AuthorityChange>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UnitManifest {
    #[serde(deserialize_with = "required_option")]
    pub through: Option<u64>,
    #[serde(deserialize_with = "required_option")]
    pub minimum_cursor: Option<u64>,
    pub digest: String,
    pub parts: Vec<String>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeliveryHeader {
    #[serde(flatten)]
    pub context: RequestContext,
    pub plan_id: String,
    pub digest: String,
    pub bootstrap: bool,
    #[serde(deserialize_with = "required_option")]
    pub owner: Option<MaterializationOwner>,
    #[serde(deserialize_with = "required_option")]
    pub after: Option<u64>,
    #[serde(deserialize_with = "required_option")]
    pub through: Option<u64>,
    pub observed_head: u64,
    pub expires_at: u64,
    pub units: Vec<UnitManifest>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeliveryPart {
    pub plan_id: String,
    pub plan_digest: String,
    pub unit: u64,
    pub part: u64,
    pub changes: Vec<AuthorityChange>,
}

/// Owner is internal durable proof identity, never an application load API.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum MaterializationOwner {
    Settlement { batch_id: u64, mutation_id: u64 },
    Schema { previous_materialization: String },
}
impl Validate for MaterializationOwner {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Settlement {
                batch_id,
                mutation_id,
            } => {
                positive(*batch_id)?;
                positive(*mutation_id)
            }
            Self::Schema {
                previous_materialization,
            } => text(previous_materialization),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MaterializationRequest {
    #[serde(flatten)]
    pub context: RequestContext,
    pub request_id: String,
    pub owner: MaterializationOwner,
    pub keys: Vec<RecordKey>,
    pub models: BTreeMap<String, u64>,
    pub continuation: Option<Continuation>,
}
impl Validate for MaterializationRequest {
    fn validate(&self) -> Result<()> {
        self.context.validate()?;
        text(&self.request_id)?;
        self.owner.validate()?;
        let mut keys = BTreeSet::new();
        for key in &self.keys {
            key.validate()?;
            if !keys.insert(key.encoded()?) {
                return Err(invalid("duplicate materialization key"));
            }
        }
        for (model, version) in &self.models {
            text(model)?;
            positive(*version)?;
        }
        match &self.owner {
            MaterializationOwner::Settlement { .. } => {
                if !self.models.is_empty() || self.keys.is_empty() {
                    return Err(invalid("settlement materializes only owned keys"));
                }
            }
            MaterializationOwner::Schema {
                previous_materialization,
            } => {
                if previous_materialization == &self.context.materialization {
                    return Err(invalid("schema context did not change"));
                }
            }
        }
        if let Some(c) = &self.continuation {
            c.validate()?;
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MaterializationResponse {
    pub request_id: String,
    pub delivery: DeliveryResponse,
}
impl Validate for MaterializationResponse {
    fn validate(&self) -> Result<()> {
        text(&self.request_id)?;
        self.delivery.validate()?;
        if self.delivery.header.owner.is_none() {
            return Err(invalid("owned materialization required"));
        }
        Ok(())
    }
}
impl MaterializationResponse {
    pub fn admit(&self, request: &MaterializationRequest, active: &RequestContext) -> Result<()> {
        self.validate()?;
        request.validate()?;
        admit_owned_context(&request.context, Some(&request.owner), active)?;
        let header = &self.delivery.header;
        if self.request_id != request.request_id
            || header.context != request.context
            || header.owner.as_ref() != Some(&request.owner)
        {
            return Err(invalid("materialization correlation mismatch"));
        }
        if let Some(continuation) = &request.continuation
            && (continuation.plan_id != header.plan_id || continuation.digest != header.digest)
        {
            return Err(invalid("materialization continuation plan mismatch"));
        }
        let requested: BTreeSet<_> = request
            .keys
            .iter()
            .map(RecordKey::encoded)
            .collect::<Result<_>>()?;
        for change in self.delivery.parts.iter().flat_map(|part| &part.changes) {
            if !requested.contains(&change.key().encoded()?)
                && !request.models.contains_key(&change.key().model)
            {
                return Err(invalid("unowned materialization key"));
            }
        }
        Ok(())
    }
}
/// A network response may contain only some parts. Complete units are the sole
/// application boundary; fragment admission never advances B/C.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryResponse {
    pub header: DeliveryHeader,
    pub parts: Vec<DeliveryPart>,
}
impl Validate for DeliveryResponse {
    fn validate(&self) -> Result<()> {
        self.header.validate()?;
        let mut seen = BTreeSet::new();
        for part in &self.parts {
            part.validate()?;
            if part.plan_id != self.header.plan_id
                || part.plan_digest != self.header.digest
                || !seen.insert((part.unit, part.part))
                || self
                    .header
                    .units
                    .get(part.unit as usize)
                    .and_then(|u| u.parts.get(part.part as usize))
                    != Some(&delivery::part_digest(part)?)
            {
                return Err(invalid("response part mismatch"));
            }
        }
        Ok(())
    }
}
impl DeliveryResponse {
    pub fn admit(&self, request: &DeltaRequest, active: &RequestContext) -> Result<()> {
        self.validate()?;
        request.validate()?;
        request.context.admit(active)?;
        if self.header.context != request.context
            || self.header.owner.is_some()
            || self.header.after != Some(request.after)
            || self.header.through != Some(request.through)
            || self.header.bootstrap != request.bootstrap
        {
            return Err(invalid("delta correlation mismatch"));
        }
        if let Some(c) = &request.continuation
            && (c.plan_id != self.header.plan_id || c.digest != self.header.digest)
        {
            return Err(invalid("continuation plan mismatch"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HandshakeRequest {
    pub protocol: u64,
    pub store_id: String,
    pub stream: String,
}
impl Validate for HandshakeRequest {
    fn validate(&self) -> Result<()> {
        if self.protocol != PROTOCOL {
            return Err(invalid("unsupported_protocol"));
        }
        text(&self.store_id)?;
        check_stream(&self.stream)
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HandshakeResponse {
    pub protocol: u64,
    pub store_id: String,
    pub stream: String,
    pub head: u64,
}
impl Validate for HandshakeResponse {
    fn validate(&self) -> Result<()> {
        HandshakeRequest {
            protocol: self.protocol,
            store_id: self.store_id.clone(),
            stream: self.stream.clone(),
        }
        .validate()?;
        counter(self.head)?;
        Ok(())
    }
}
impl HandshakeResponse {
    pub fn admit(&self, request: &HandshakeRequest) -> Result<()> {
        self.validate()?;
        request.validate()?;
        if self.protocol != request.protocol
            || self.store_id != request.store_id
            || self.stream != request.stream
        {
            return Err(invalid("handshake correlation mismatch"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum ReadInvocation {
    Query {
        name: String,
        version: u64,
        args: Value,
    },
    Fetch {
        key: RecordKey,
        version: u64,
    },
}
impl Validate for ReadInvocation {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Query {
                name,
                version,
                args,
            } => {
                text(name)?;
                positive(*version)?;
                if !args.is_object() {
                    return Err(invalid("Query args must be object"));
                }
                Ok(())
            }
            Self::Fetch { key, version } => {
                key.validate()?;
                positive(*version)
            }
        }
    }
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReadRequest {
    #[serde(flatten)]
    pub context: RequestContext,
    pub request_id: String,
    #[serde(default = "default_store")]
    pub store: bool,
    pub invocation: ReadInvocation,
}
impl Validate for ReadRequest {
    fn validate(&self) -> Result<()> {
        self.context.validate()?;
        text(&self.request_id)?;
        self.invocation.validate()
    }
}
impl ReadResponse {
    pub fn admit_correlation(&self, request: &ReadRequest, active: &RequestContext) -> Result<()> {
        self.validate()?;
        request.validate()?;
        request.context.admit(active)?;
        if self.context != request.context || self.request_id != request.request_id {
            return Err(invalid("read correlation mismatch"));
        }
        if let ReadInvocation::Fetch { key, .. } = &request.invocation
            && matches!(self.outcome, ReadOutcome::Succeeded { .. })
        {
            if self.records.len() != 1 || self.records[0].key != *key {
                return Err(invalid("Fetch snapshot mismatch"));
            }
            if self.records[0].state.as_object().is_some_and(|state| {
                key.identity
                    .as_object()
                    .unwrap()
                    .keys()
                    .any(|field| state.contains_key(field))
            }) {
                return Err(invalid("Fetch snapshot repeats identity"));
            }
        }
        Ok(())
    }
    pub fn admit(&self, request: &ReadRequest, active: &RequestContext) -> Result<()> {
        self.admit_correlation(request, active)?;
        if let (ReadInvocation::Fetch { key, .. }, ReadOutcome::Succeeded { result }) =
            (&request.invocation, &self.outcome)
        {
            if self.records.len() != 1 || &self.records[0].key != key {
                return Err(invalid("Fetch snapshot mismatch"));
            }
            let record = &self.records[0];
            let expected = if record.state.is_null() {
                Value::Null
            } else {
                let mut full = key.identity.as_object().unwrap().clone();
                for (k, v) in record.state.as_object().unwrap() {
                    if full.insert(k.clone(), v.clone()).is_some() {
                        return Err(invalid("Fetch snapshot repeats identity"));
                    }
                }
                Value::Object(full)
            };
            if result != &expected {
                return Err(invalid("Fetch result differs from snapshot"));
            }
        }
        Ok(())
    }
}

#[path = "sync/delivery.rs"]
mod delivery;
#[path = "sync/mutation.rs"]
mod mutation;
pub use delivery::*;
pub use mutation::*;

fn admit_owned_context(
    context: &RequestContext,
    owner: Option<&MaterializationOwner>,
    active: &RequestContext,
) -> Result<()> {
    context.admit_store(active)?;
    if let Some(MaterializationOwner::Schema {
        previous_materialization,
    }) = owner
    {
        if &active.materialization != previous_materialization
            && active.materialization != context.materialization
        {
            return Err(invalid("schema transfer context mismatch"));
        }
        Ok(())
    } else {
        context.admit(active)
    }
}
impl DeliveryHeader {
    /// A schema transfer is authorized while its previous descriptor is enabled;
    /// completion enables the desired descriptor. Store/Stream never change.
    pub fn admit(&self, active: &RequestContext) -> Result<()> {
        self.validate()?;
        admit_owned_context(&self.context, self.owner.as_ref(), active)
    }
}
/// Validate the complete owned transfer before enabling a schema context or
/// considering target materialization done. Partial response admission checks
/// only scope/correlation and cannot prove all requested identities arrived.
pub fn validate_materialization(
    request: &MaterializationRequest,
    header: &DeliveryHeader,
    units: &[DeliveryUnit],
) -> Result<()> {
    request.validate()?;
    validate_delivery(header, units)?;
    if header.context != request.context || header.owner.as_ref() != Some(&request.owner) {
        return Err(invalid("owned transfer mismatch"));
    }
    let required: BTreeSet<_> = request
        .keys
        .iter()
        .map(RecordKey::encoded)
        .collect::<Result<_>>()?;
    let mut delivered = BTreeSet::new();
    for change in units.iter().flat_map(|u| &u.changes) {
        let key = change.key().encoded()?;
        if !required.contains(&key) && !request.models.contains_key(&change.key().model) {
            return Err(invalid("unowned materialization identity"));
        }
        delivered.insert(key);
    }
    if !required.is_subset(&delivered) {
        return Err(invalid("incomplete materialization identity set"));
    }
    Ok(())
}
