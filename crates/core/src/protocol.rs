use crate::{
    ActionIntent, ActionOutcome, CallCompletion, MAX_SAFE_INTEGER, RecordKey, Result, Schema,
    canonical_json, invalid,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// Limits both sides enforce without negotiating them on the wire. Every
/// consumer reads them from here; making them configurable is
/// [#11](https://github.com/zanminwang/axton/issues/11). Host resource
/// limits (HTTP body and WebSocket frame sizes, page buffers) are not
/// protocol rules and stay with each transport.
pub mod limits {
    /// A push batch carries between one and this many mutations.
    pub const PUSH_MUTATIONS: usize = 20;
    /// The client freezes a batch only while its canonical bytes stay under this.
    pub const PUSH_BYTES: usize = 256 * 1024;
    /// A pull page carries at most this many changes. A page holding exactly
    /// this many continues: the stream may hold more beyond `to`.
    pub const PULL_CHANGES: usize = 50;
    /// A Load batch carries between one and this many page requests.
    pub const LOAD_BATCH_ITEMS: usize = 8;
    /// The canonical bytes of one Load batch request.
    pub const LOAD_REQUEST_BYTES: usize = 1024 * 1024;
    /// The canonical bytes of one answered page: IDs, outcome and records.
    pub const LOAD_PAGE_BYTES: usize = 1024 * 1024;
    /// The bytes of one Load batch response: eight pages at
    /// [`LOAD_PAGE_BYTES`] plus a 64 KiB allowance for the envelope, so a
    /// full batch of maximal pages stays encodable.
    pub const LOAD_RESPONSE_BYTES: usize = 8 * LOAD_PAGE_BYTES + 64 * 1024;
    /// Identity entries one page returns across all declared lists.
    pub const LOAD_PAGE_IDENTITIES: usize = 1000;
    /// The canonical bytes of one continuation state.
    pub const LOAD_STATE_BYTES: usize = 64 * 1024;
    /// Nested arrays and objects in one continuation state.
    pub const LOAD_STATE_DEPTH: usize = 64;
    /// UTF-8 bytes of one Load item error message.
    pub const LOAD_ERROR_MESSAGE_BYTES: usize = 1024;
    /// Distinct Stream/record pairs one Load page may enroll. Repeated
    /// declarations of a pair count once, like the page's identities.
    pub const LOAD_ENROLLMENT_PAIRS: usize = 1000;
    /// The encoded bytes of one Load page's distinct enrollment: the sum of
    /// the UTF-8 lengths of each pair's canonical JSON intent
    /// `{stream, identity, model, present}` at its canonical identity.
    pub const LOAD_ENROLLMENT_BYTES: usize = 1024 * 1024;
}

pub fn counter(value: u64) -> Result<u64> {
    if value <= MAX_SAFE_INTEGER {
        Ok(value)
    } else {
        Err(invalid("counter outside safe integer range"))
    }
}
pub fn read_counter(value: &Value, positive: bool) -> Result<u64> {
    let f = value
        .as_f64()
        .ok_or_else(|| invalid("counter must be a number"))?;
    if !f.is_finite()
        || f < 0.0
        || f.fract() != 0.0
        || f > MAX_SAFE_INTEGER as f64
        || (positive && f == 0.0)
    {
        return Err(invalid("invalid counter"));
    }
    Ok(f as u64)
}
#[derive(Clone, Debug)]
pub struct RawMutation {
    pub ordinal: u64,
    pub raw: Value,
}
#[derive(Clone, Debug)]
pub struct PushRequest {
    pub client_id: String,
    pub batch_sequence: u64,
    pub mutations: Vec<RawMutation>,
    /// The read contracts the receipt's authority is served at, as in
    /// [`PullRequest::models`]. Frozen with the batch: a retry declares what
    /// the original request declared.
    pub models: BTreeMap<String, u64>,
    pub raw: Value,
}
impl PushRequest {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        Self::decode_inner(bytes, false)
    }
    /// Structural Action batch validation. Unsupported names/versions and
    /// invalid argument values remain per-call failures for the server.
    pub fn decode_action_envelope(bytes: &[u8]) -> Result<Self> {
        check_request_size(bytes, limits::PUSH_BYTES)
            .map_err(|_| invalid("Action push exceeds byte limit"))?;
        let request = Self::decode_inner(bytes, true)?;
        let mut seen = BTreeSet::new();
        for mutation in &request.mutations {
            let intent: ActionIntent = serde_json::from_value(mutation.raw.clone())?;
            if intent.name.trim().is_empty()
                || intent.version == 0
                || counter(intent.version).is_err()
            {
                return Err(invalid("invalid Action name or version"));
            }
            if !seen.insert(crate::normalize_call_id(&intent.call_id)?) {
                return Err(invalid("duplicate Action callId"));
            }
        }
        if request.encode()?.len() > limits::PUSH_BYTES {
            return Err(invalid("canonical Action push exceeds byte limit"));
        }
        Ok(request)
    }
    /// Client-side known-contract validation before local writes or send.
    /// The server uses `decode_action_envelope` and rejects unsupported
    /// Action versions individually inside its batch transaction.
    pub fn decode_actions(bytes: &[u8], schema: &Schema) -> Result<Self> {
        let request = Self::decode_action_envelope(bytes)?;
        for mutation in &request.mutations {
            let intent: ActionIntent = serde_json::from_value(mutation.raw.clone())?;
            let intent = intent.normalize(schema)?;
            crate::validate_action_models(
                schema,
                schema.action(&intent.name, intent.version)?,
                &request.models,
            )?;
        }
        Ok(request)
    }
    fn decode_inner(bytes: &[u8], allow_empty_models: bool) -> Result<Self> {
        let mut raw: Value = serde_json::from_slice(bytes)?;
        // Negotiation is not part of the frozen batch: a retry that now
        // advertises a capability encodes to the bytes first frozen.
        strip_capabilities(&mut raw)?;
        let client_id = nonblank(&raw["clientId"])?;
        let batch_sequence = read_counter(&raw["batchSequence"], true)?;
        let models = read_models_inner(&raw["models"], allow_empty_models)?;
        let acts = raw["mutations"]
            .as_array()
            .ok_or_else(|| invalid("mutations must be array"))?;
        if acts.is_empty() || acts.len() > limits::PUSH_MUTATIONS {
            return Err(invalid(format!(
                "batch must contain 1..{} mutations",
                limits::PUSH_MUTATIONS
            )));
        }
        let mut seen = BTreeSet::new();
        let mut mutations = vec![];
        for act in acts {
            if !act.is_object() {
                return Err(invalid("mutation must be object"));
            }
            let ordinal = read_counter(&act["ordinal"], true)?;
            if !seen.insert(ordinal) {
                return Err(invalid("duplicate ordinal"));
            }
            mutations.push(RawMutation {
                ordinal,
                raw: act.clone(),
            });
        }
        Ok(Self {
            client_id,
            batch_sequence,
            mutations,
            models,
            raw,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(canonical_json(&self.raw)?.into_bytes())
    }
}
fn nonblank(value: &Value) -> Result<String> {
    value
        .as_str()
        .filter(|s| !s.trim().is_empty())
        .map(str::to_string)
        .ok_or_else(|| invalid("expected nonblank string"))
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rejection {
    pub ordinal: u64,
    pub code: String,
}
/// One record's authoritative content at one stamp, as a receipt or a page
/// carries it. Identity is canonical; `state` is a normalized record state or
/// `null` for a deletion. A record the server could not read carries `error`
/// (a code) instead of a state: the client keeps what it has and reports it.
/// It names no stream and no cursor: authority is ordered by stamp alone
/// ([Protocol / Push](../../../docs/engineering/architecture/protocol/push.md)).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AuthorityRecord {
    pub model: String,
    pub identity: Value,
    pub stamp: u64,
    #[serde(default)]
    pub state: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}
impl AuthorityRecord {
    fn validate(&self) -> Result<()> {
        check_key(&self.model, &self.identity)?;
        if self.stamp == 0 || counter(self.stamp).is_err() {
            return Err(invalid("record stamp must be a positive counter"));
        }
        match &self.error {
            Some(code) => {
                if !valid_code(code) {
                    return Err(invalid("record error must be a code"));
                }
                if !self.state.is_null() {
                    return Err(invalid("a record with an error carries no state"));
                }
            }
            None => {
                if !self.state.is_null() && !self.state.is_object() {
                    return Err(invalid("record state must be an object or null"));
                }
            }
        }
        Ok(())
    }
    /// The key two records of one receipt or page must not share.
    pub fn key(&self) -> Result<String> {
        Ok(format!(
            "{}\u{0}{}",
            self.model,
            canonical_json(&self.identity)?
        ))
    }
    /// Whether the record is a read failure rather than authority.
    pub fn is_error(&self) -> bool {
        self.error.is_some()
    }
}
/// The members only a [`StreamChange`] carries. A record names no stream, so
/// a stream page can never pass as record-only authority.
const STREAM_MEMBERS: [&str; 3] = ["stream", "cursor", "kind"];
/// The model and identity rule every record, removal and claim shares.
fn check_key(model: &str, identity: &Value) -> Result<()> {
    if model.is_empty() {
        return Err(invalid("record model must not be empty"));
    }
    if !identity.is_object() {
        return Err(invalid("record identity must be an object"));
    }
    Ok(())
}
/// Refuse retired framework ownership only at the immediate envelope level.
/// Application state, arguments and identities are opaque to this check.
fn reject_legacy_ownership(value: &Value) -> Result<()> {
    if let Some(member) = ["scope", "scopes", "channel", "channels"]
        .iter()
        .find(|member| value.get(**member).is_some())
    {
        return Err(invalid(format!("retired framework member {member}")));
    }
    Ok(())
}
pub(crate) fn decode_record(value: &Value) -> Result<AuthorityRecord> {
    reject_legacy_ownership(value)?;
    if !value.is_object() {
        return Err(invalid("record must be an object"));
    }
    if let Some(member) = STREAM_MEMBERS.iter().find(|m| value.get(**m).is_some()) {
        return Err(invalid(format!("a record names no {member}")));
    }
    if value.get("error").is_none() && value.get("state").is_none() {
        return Err(invalid("record state missing"));
    }
    if value.get("stamp").is_none() {
        return Err(invalid("record stamp missing"));
    }
    let record: AuthorityRecord = serde_json::from_value(value.clone())?;
    record.validate()?;
    Ok(record)
}
fn unique_records(records: &[AuthorityRecord]) -> Result<()> {
    let mut keys = BTreeSet::new();
    for record in records {
        record.validate()?;
        if !keys.insert(record.key()?) {
            return Err(invalid("duplicate record"));
        }
    }
    Ok(())
}
/// A stable machine code: `handler.failed`, `todo.missing`, `loader_failed`.
pub fn valid_code(s: &str) -> bool {
    let mut parts = s.split(['.', '_', '-']);
    let first = parts.next().unwrap_or("");
    first.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
        && first
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        && parts.all(|p| {
            !p.is_empty()
                && p.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        })
}
/// The answer to a push: the batch it answers, which of its mutations were
/// refused, and the authoritative content of every record a successful
/// mutation changed, once per record with its final stamp. Every mutation not
/// listed in `rejections` succeeded. The identity fields are required: a
/// receipt from before this format cannot decode as successful empty authority.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PushReceipt {
    #[serde(rename = "clientId")]
    pub client_id: String,
    #[serde(rename = "batchSequence")]
    pub batch_sequence: u64,
    pub rejections: Vec<Rejection>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub completions: Vec<CallCompletion>,
    pub records: Vec<AuthorityRecord>,
    /// Enrollment claims for returned records ([`MembershipClaim`]); omitted
    /// from the wire when the call enrolled nothing.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub memberships: Vec<MembershipClaim>,
}
impl PushReceipt {
    /// Decode an Action receipt against the frozen ordered calls. A legacy
    /// receipt has no completion contract; only the legacy path may omit it.
    pub fn decode_actions(bytes: &[u8], request: &PushRequest, schema: &Schema) -> Result<Self> {
        Self::decode_actions_inner(bytes, request, schema, None)
    }
    /// Decode a queued Action receipt using the Model read contracts captured
    /// when its batch was frozen. Fresh responses should use `decode_actions`.
    pub fn decode_actions_with_frozen_results(
        bytes: &[u8],
        request: &PushRequest,
        schema: &Schema,
        frozen_reads: &[crate::ModelReadDescriptor],
    ) -> Result<Self> {
        Self::decode_actions_inner(bytes, request, schema, Some(frozen_reads))
    }
    fn decode_actions_inner(
        bytes: &[u8],
        request: &PushRequest,
        schema: &Schema,
        frozen_reads: Option<&[crate::ModelReadDescriptor]>,
    ) -> Result<Self> {
        let value: Value = serde_json::from_slice(bytes)?;
        reject_legacy_ownership(&value)?;
        if value.get("completions").is_none() {
            return Err(invalid("Action completions missing"));
        }
        let mut receipt = Self::decode_inner(value)?;
        if !receipt.answers(&request.client_id, request.batch_sequence) {
            return Err(invalid("Action receipt answers another batch"));
        }
        if receipt.completions.len() != request.mutations.len() {
            return Err(invalid("Action completion count mismatch"));
        }
        let rejections: BTreeMap<u64, &str> = receipt
            .rejections
            .iter()
            .map(|r| (r.ordinal, r.code.as_str()))
            .collect();
        let mut seen = BTreeSet::new();
        let mut failures = 0;
        for (mutation, completion) in request.mutations.iter().zip(&mut receipt.completions) {
            let call: ActionIntent = serde_json::from_value(mutation.raw.clone())?;
            let normalized = call.clone().normalize(schema)?;
            let id = crate::normalize_call_id(&completion.call_id)?;
            if completion.call_id != id || !seen.insert(id.clone()) || normalized.call_id != id {
                return Err(invalid("Action completion callId mismatch"));
            }
            match &mut completion.outcome {
                ActionOutcome::Succeeded { result } => {
                    if rejections.contains_key(&mutation.ordinal) {
                        return Err(invalid("succeeded Action is rejected"));
                    }
                    let action = schema.action(&call.name, call.version)?;
                    *result = match frozen_reads {
                        Some(reads) => crate::validate_action_result_after_read_upgrade(
                            schema, action, reads, result,
                        )?,
                        None => crate::validate_action_result(schema, action, result)?,
                    };
                }
                ActionOutcome::Failed { code, execution } => {
                    if !valid_code(code)
                        || *execution != crate::ExecutionState::Rejected
                        || rejections.get(&mutation.ordinal) != Some(&code.as_str())
                    {
                        return Err(invalid("Action failure/rejection mismatch"));
                    }
                    failures += 1;
                }
            }
        }
        if failures != rejections.len() {
            return Err(invalid("unexpected Action rejection"));
        }
        Ok(receipt)
    }
    /// Temporary compatibility for internal mutation-only fixtures. New
    /// Action callers must use `decode_actions` for exact call correlation.
    pub fn decode_legacy(bytes: &[u8]) -> Result<Self> {
        Self::decode(bytes)
    }
    /// Structural Action receipt ingress. Client::acknowledge validates it
    /// against the persisted frozen request and result read contracts.
    pub fn decode_action_envelope(bytes: &[u8]) -> Result<Self> {
        Self::decode_inner(serde_json::from_slice(bytes)?)
    }
    /// Legacy mutation-only receipt decoder. Action completions require the
    /// frozen request and therefore cannot pass through this path.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let value: Value = serde_json::from_slice(bytes)?;
        reject_legacy_ownership(&value)?;
        if value.get("completions").is_some() {
            return Err(invalid("Action receipt requires request-aware decoder"));
        }
        Self::decode_inner(value)
    }
    fn decode_inner(value: Value) -> Result<Self> {
        if !value.is_object() {
            return Err(invalid("receipt must be an object"));
        }
        for field in ["clientId", "batchSequence", "rejections", "records"] {
            if value.get(field).is_none() {
                return Err(invalid(format!("receipt {field} missing")));
            }
        }
        let batch_sequence = read_counter(&value["batchSequence"], true)?;
        let records = value["records"]
            .as_array()
            .ok_or_else(|| invalid("receipt records must be an array"))?
            .iter()
            .map(decode_record)
            .collect::<Result<Vec<_>>>()?;
        let mut result: Self = serde_json::from_value(value)?;
        result.batch_sequence = batch_sequence;
        result.records = records;
        result.validate()?;
        Ok(result)
    }
    fn validate(&self) -> Result<()> {
        if self.client_id.trim().is_empty() {
            return Err(invalid("receipt clientId must not be blank"));
        }
        if self.batch_sequence == 0 || counter(self.batch_sequence).is_err() {
            return Err(invalid("receipt batchSequence must be a positive counter"));
        }
        let mut seen = BTreeSet::new();
        for r in &self.rejections {
            counter(r.ordinal)?;
            if r.ordinal == 0 || r.code.trim().is_empty() || !seen.insert(r.ordinal) {
                return Err(invalid("invalid rejection"));
            }
        }
        unique_records(&self.records)?;
        // Claims tie to returned authority: an all-rejected receipt has none.
        validate_memberships(&self.memberships, &self.records)?;
        let mut calls = BTreeSet::new();
        for completion in &self.completions {
            if !calls.insert(crate::normalize_call_id(&completion.call_id)?) {
                return Err(invalid("duplicate completion callId"));
            }
        }
        if self.records.iter().any(AuthorityRecord::is_error) {
            return Err(invalid("a receipt carries authority, never a read failure"));
        }
        Ok(())
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        Ok(canonical_json(&serde_json::to_value(self)?)?.into_bytes())
    }
    /// Whether the receipt answers this batch of this client.
    pub fn answers(&self, client_id: &str, batch_sequence: u64) -> bool {
        self.client_id == client_id && self.batch_sequence == batch_sequence
    }
}
/// `{"Task":2,"Note":1}`: one positive version per model, nothing else.
pub fn read_models(value: &Value) -> Result<BTreeMap<String, u64>> {
    read_models_inner(value, false)
}
pub(crate) fn read_action_models(value: &Value) -> Result<BTreeMap<String, u64>> {
    read_models_inner(value, true)
}
fn read_models_inner(value: &Value, allow_empty: bool) -> Result<BTreeMap<String, u64>> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid("models must declare a version per model"))?;
    if object.is_empty() && !allow_empty {
        return Err(invalid("models must declare at least one model"));
    }
    object
        .iter()
        .map(|(name, version)| {
            if name.is_empty() {
                return Err(invalid("model name must not be empty"));
            }
            Ok((name.clone(), read_counter(version, true)?))
        })
        .collect()
}
/// `{"book:demo":42,"inbox:alice":7}`: one cursor per stream, at least one stream.
pub fn read_cursors(value: &Value) -> Result<BTreeMap<String, u64>> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid("cursors must map streams to cursors"))?;
    if object.is_empty() {
        return Err(invalid("cursors must name at least one stream"));
    }
    object
        .iter()
        .map(|(stream, cursor)| {
            check_stream(stream)?;
            Ok((stream.clone(), read_counter(cursor, false)?))
        })
        .collect()
}
/// One pull for every subscribed stream: where the client is in each, and
/// the read contracts it expects ([`read_models`]). The owner comes from
/// authentication; no client id travels.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PullRequest {
    pub models: BTreeMap<String, u64>,
    pub cursors: BTreeMap<String, u64>,
}
impl PullRequest {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let v: Value = serde_json::from_slice(bytes)?;
        reject_legacy_ownership(&v)?;
        read_capabilities(&v)?;
        Ok(Self {
            models: read_models(&v["models"])?,
            cursors: read_cursors(&v["cursors"])?,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        read_models(&serde_json::to_value(&self.models)?)?;
        read_cursors(&serde_json::to_value(&self.cursors)?)?;
        Ok(canonical_json(&serde_json::to_value(self)?)?.into_bytes())
    }
}
/// A stream's progress in one page: the cursor the page starts after, the
/// cursor it reaches, and the stream's head when the page was built. Equal
/// `from` and `to` means nothing changed; `to` below `head` means the stream
/// has more and the client pulls again.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorRange {
    pub from: u64,
    pub to: u64,
    pub head: u64,
}
impl CursorRange {
    /// Whether the stream holds changes beyond `to`.
    pub fn continues(&self) -> bool {
        self.to < self.head
    }
}
fn read_ranges(value: &Value) -> Result<BTreeMap<String, CursorRange>> {
    let ranges = value
        .as_object()
        .ok_or_else(|| invalid("page cursors must map streams to ranges"))?;
    let mut cursors = BTreeMap::new();
    for (stream, range) in ranges {
        check_stream(stream)?;
        let from = read_counter(&range["from"], false)?;
        let to = read_counter(&range["to"], false)?;
        let head = read_counter(&range["head"], false)?;
        cursors.insert(stream.clone(), CursorRange { from, to, head });
    }
    Ok(cursors)
}
fn check_ranges(cursors: &BTreeMap<String, CursorRange>) -> Result<()> {
    if cursors.is_empty() {
        return Err(invalid("page must name at least one stream"));
    }
    for (stream, range) in cursors {
        check_stream(stream)?;
        counter(range.from)?;
        counter(range.to)?;
        counter(range.head)?;
        if range.to < range.from {
            return Err(invalid("page moves backwards"));
        }
        if range.head < range.to {
            return Err(invalid("page reaches past the stream head"));
        }
    }
    Ok(())
}
/// One record-only page for every stream it names: each stream's progress,
/// and the records changed in any of them, once each at its current stamp. A
/// change is the same [`AuthorityRecord`] a receipt carries; a stream never
/// appears on a record. The server scans at most [`limits::PULL_CHANGES`]
/// invalidations per stream; a stream whose `to` is below its head
/// continues. [`StreamPullPage`] is the same envelope with stream changes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PullPage {
    pub cursors: BTreeMap<String, CursorRange>,
    pub changes: Vec<AuthorityRecord>,
}
impl PullPage {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let value: Value = serde_json::from_slice(bytes)?;
        reject_legacy_ownership(&value)?;
        if !value.is_object() {
            return Err(invalid("page must be an object"));
        }
        for field in ["cursors", "changes"] {
            if value.get(field).is_none() {
                return Err(invalid(format!("page {field} missing")));
            }
        }
        let cursors = read_ranges(&value["cursors"])?;
        let changes = value["changes"]
            .as_array()
            .ok_or_else(|| invalid("page changes must be an array"))?
            .iter()
            .map(decode_record)
            .collect::<Result<Vec<_>>>()?;
        let page = Self { cursors, changes };
        page.validate()?;
        Ok(page)
    }
    pub fn validate(&self) -> Result<()> {
        check_ranges(&self.cursors)?;
        if self.changes.len() > limits::PULL_CHANGES * self.cursors.len() {
            return Err(invalid(format!(
                "page exceeds {} changes per stream",
                limits::PULL_CHANGES
            )));
        }
        unique_records(&self.changes)
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        Ok(canonical_json(&serde_json::to_value(self)?)?.into_bytes())
    }
    /// The streams the page names, in canonical order.
    pub fn streams(&self) -> impl Iterator<Item = &str> {
        self.cursors.keys().map(String::as_str)
    }
}

/// The `mode` a bounded pull carries on the shared pull route. An absent mode
/// is the ordinary delta pull; this is the only other value either side
/// accepts ([Protocol / Pull](../../../docs/engineering/architecture/protocol/pull.md)).
pub const BOOTSTRAP_MODE: &str = "bootstrap";
/// Read the pull mode of a request body: `None` for an ordinary delta pull,
/// `Some(mode)` for a present one, which only [`BOOTSTRAP_MODE`] satisfies. A
/// body that is not an object carries no mode; decoding refuses it later.
pub fn pull_mode(bytes: &[u8]) -> Option<String> {
    serde_json::from_slice::<Value>(bytes)
        .ok()?
        .as_object()?
        .get("mode")
        .map(|mode| match mode {
            Value::String(mode) => mode.clone(),
            other => other.to_string(),
        })
}
/// Read the one stream name of a bounded pull envelope.
fn read_stream(value: &Value) -> Result<String> {
    let stream = value
        .as_str()
        .ok_or_else(|| invalid("stream must be a string"))?;
    check_stream(stream)?;
    Ok(stream.to_string())
}
/// Require the bounded-pull mode: the envelope is not a bootstrap one without it.
fn read_bootstrap_mode(value: &Value) -> Result<()> {
    if value.as_str() == Some(BOOTSTRAP_MODE) {
        Ok(())
    } else {
        Err(invalid("mode must be \"bootstrap\""))
    }
}
/// One bounded page request of a Stream's historical interval: the stream it
/// loads, the read contracts it expects (as in [`PullRequest::models`]), the
/// committed progress `after` (B) and the subscription origin `until` (S).
/// The server walks `(after, until]` and never chases a moving head; the owner
/// comes from authentication
/// ([#151](https://github.com/zanminwang/axton/issues/151)).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootstrapRequest {
    pub stream: String,
    pub models: BTreeMap<String, u64>,
    pub after: u64,
    pub until: u64,
}
impl BootstrapRequest {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let value: Value = serde_json::from_slice(bytes)?;
        reject_legacy_ownership(&value)?;
        if !value.is_object() {
            return Err(invalid("bootstrap request must be an object"));
        }
        read_capabilities(&value)?;
        read_bootstrap_mode(&value["mode"])?;
        let request = Self {
            stream: read_stream(&value["stream"])?,
            models: read_models(&value["models"])?,
            after: read_counter(&value["after"], false)?,
            until: read_counter(&value["until"], false)?,
        };
        request.validate()?;
        Ok(request)
    }
    pub fn validate(&self) -> Result<()> {
        check_stream(&self.stream)?;
        read_models(&serde_json::to_value(&self.models)?)?;
        counter(self.after)?;
        counter(self.until)?;
        if self.after > self.until {
            return Err(invalid("bootstrap progress is past its origin"));
        }
        Ok(())
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        Ok(canonical_json(&serde_json::json!({
            "mode": BOOTSTRAP_MODE,
            "stream": self.stream,
            "models": self.models,
            "after": self.after,
            "until": self.until,
        }))?
        .into_bytes())
    }
    /// Whether the interval is already exhausted, so the page is empty.
    pub fn exhausted(&self) -> bool {
        self.after == self.until
    }
}
/// One record-only bounded page of a Stream's historical interval
/// ([`StreamBootstrapPage`] carries stream changes): the echoed stream and
/// origin, the interval `(from, to]` the page covers, the stream head its
/// transaction observed, and the records published at or below `until` in that
/// interval, at most [`limits::PULL_CHANGES`] of them, once each at their
/// current stamp. `to == until` completes the interval, so no done flag
/// travels; `head` is the completion barrier the client stores on that final
/// page ([Protocol / Pull](../../../docs/engineering/architecture/protocol/pull.md)).
#[derive(Clone, Debug, PartialEq)]
pub struct BootstrapPage {
    pub stream: String,
    pub from: u64,
    pub to: u64,
    pub until: u64,
    pub head: u64,
    pub records: Vec<AuthorityRecord>,
}
impl BootstrapPage {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let value: Value = serde_json::from_slice(bytes)?;
        reject_legacy_ownership(&value)?;
        if !value.is_object() {
            return Err(invalid("bootstrap page must be an object"));
        }
        read_bootstrap_mode(&value["mode"])?;
        for field in ["stream", "from", "to", "until", "head", "records"] {
            if value.get(field).is_none() {
                return Err(invalid(format!("bootstrap page {field} missing")));
            }
        }
        let page = Self {
            stream: read_stream(&value["stream"])?,
            from: read_counter(&value["from"], false)?,
            to: read_counter(&value["to"], false)?,
            until: read_counter(&value["until"], false)?,
            head: read_counter(&value["head"], false)?,
            records: value["records"]
                .as_array()
                .ok_or_else(|| invalid("bootstrap page records must be an array"))?
                .iter()
                .map(decode_record)
                .collect::<Result<Vec<_>>>()?,
        };
        page.validate()?;
        Ok(page)
    }
    pub fn validate(&self) -> Result<()> {
        check_interval(&self.stream, self.from, self.to, self.until, self.head)?;
        if self.records.len() > limits::PULL_CHANGES {
            return Err(invalid(format!(
                "bootstrap page exceeds {} records",
                limits::PULL_CHANGES
            )));
        }
        unique_records(&self.records)
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        Ok(canonical_json(&serde_json::json!({
            "mode": BOOTSTRAP_MODE,
            "stream": self.stream,
            "from": self.from,
            "to": self.to,
            "until": self.until,
            "head": self.head,
            "records": self.records,
        }))?
        .into_bytes())
    }
    /// Whether the page finished the historical interval: the client stores
    /// `head` as its completion barrier and asks for no further page.
    pub fn terminal(&self) -> bool {
        self.to == self.until
    }
    /// Whether the page answers this request: the echoed stream and origin,
    /// the requested `from`, and progress that never moves backwards. A
    /// nonterminal page must advance, so a repeated `from` is refused.
    pub fn answers(&self, request: &BootstrapRequest) -> bool {
        answers_interval(&self.stream, self.from, self.to, self.until, request)
    }
}
/// The bounds every bounded page keeps: `from <= to <= until <= head`.
fn check_interval(stream: &str, from: u64, to: u64, until: u64, head: u64) -> Result<()> {
    check_stream(stream)?;
    for cursor in [from, to, until, head] {
        counter(cursor)?;
    }
    if to < from {
        return Err(invalid("bootstrap page moves backwards"));
    }
    if until < to {
        return Err(invalid("bootstrap page reaches past its origin"));
    }
    if head < until {
        return Err(invalid("bootstrap origin is past the stream head"));
    }
    Ok(())
}
fn answers_interval(
    stream: &str,
    from: u64,
    to: u64,
    until: u64,
    request: &BootstrapRequest,
) -> bool {
    stream == request.stream
        && from == request.after
        && until == request.until
        && to >= from
        && (to == until || to > from)
}

/// The one client frame of a live session:
/// `{"type":"subscribe","streams":[…],"models":{…}}`. Streams are normalized
/// on decode and on construction: deduplicated and sorted by UTF-16 code
/// units, the order the acknowledgement uses. `models` declares the read
/// contracts every frame of the session is served at, as in [`PullRequest`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscribeRequest {
    pub streams: Vec<String>,
    pub models: BTreeMap<String, u64>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubscribeWire {
    #[serde(rename = "type")]
    kind: String,
    streams: Vec<String>,
    #[serde(default)]
    models: Value,
}
/// The one stream-name rule: a name that is empty, or nothing but whitespace,
/// names no stream. Every frame that carries stream names is refused for it,
/// and so is a durable client registration, so a Stream no socket could ever
/// subscribe cannot be stored either.
pub fn check_stream(stream: &str) -> Result<()> {
    if stream.trim().is_empty() {
        return Err(invalid("stream must not be empty"));
    }
    Ok(())
}
fn normalize_streams(streams: Vec<String>) -> Result<Vec<String>> {
    if streams.is_empty() {
        return Err(invalid("subscribe requires at least one stream"));
    }
    for stream in &streams {
        check_stream(stream)?;
    }
    let mut streams: Vec<_> = streams
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    streams.sort_by(|left, right| left.encode_utf16().cmp(right.encode_utf16()));
    Ok(streams)
}
impl SubscribeRequest {
    pub fn new(streams: Vec<String>, models: BTreeMap<String, u64>) -> Result<Self> {
        Ok(Self {
            streams: normalize_streams(streams)?,
            models: read_models(&serde_json::to_value(&models)?)?,
        })
    }
    /// Decode the frame. `capabilities` is negotiation metadata the server
    /// checks with [`require_capability`]; it is not part of the subscription.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut raw: Value = serde_json::from_slice(bytes)?;
        strip_capabilities(&mut raw)?;
        let wire: SubscribeWire = serde_json::from_value(raw)?;
        if wire.kind != "subscribe" {
            return Err(invalid("expected one subscribe frame with streams"));
        }
        Self::new(wire.streams, read_models(&wire.models)?)
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(canonical_json(
            &serde_json::json!({"type":"subscribe","streams":self.streams,"models":self.models}),
        )?
        .into_bytes())
    }
}

/// The server's answer to a subscribe frame: every stream's current head.
/// The client compares them with its cursors and catches up over HTTP only
/// where it is behind. Unknown fields are ignored so a newer server can
/// extend the frame.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionAck {
    pub cursors: BTreeMap<String, u64>,
}
#[derive(Deserialize)]
struct AckWire {
    #[serde(rename = "type")]
    kind: String,
    cursors: Value,
}
impl SubscriptionAck {
    pub fn new(cursors: BTreeMap<String, u64>) -> Result<Self> {
        Ok(Self {
            cursors: read_cursors(&serde_json::to_value(&cursors)?)?,
        })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let wire: AckWire = serde_json::from_slice(bytes)
            .map_err(|_| invalid("invalid live subscription acknowledgement"))?;
        if wire.kind != "subscribed" {
            return Err(invalid("invalid live subscription acknowledgement"));
        }
        Self::new(
            read_cursors(&wire.cursors)
                .map_err(|_| invalid("invalid live subscription acknowledgement"))?,
        )
    }
    /// Whether the server acknowledged exactly the requested stream set.
    pub fn confirms(&self, request: &SubscribeRequest) -> bool {
        self.cursors.keys().eq(request.streams.iter())
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(
            canonical_json(&serde_json::json!({"type":"subscribed","cursors":self.cursors}))?
                .into_bytes(),
        )
    }
}

/// A record-only frame the server sends on a live socket
/// ([`StreamLiveMessage`] carries stream pages): the acknowledgement carries a
/// `type`, a page never does ([Protocol / Subscriptions](../../../docs/engineering/architecture/protocol/subscriptions.md)).
#[derive(Clone, Debug, PartialEq)]
pub enum LiveMessage {
    Acknowledged(SubscriptionAck),
    Page(PullPage),
}
impl LiveMessage {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let value: Value = serde_json::from_slice(bytes)?;
        reject_legacy_ownership(&value)?;
        if !value.is_object() {
            return Err(invalid("invalid live frame"));
        }
        if value.get("type").is_some() {
            return Ok(Self::Acknowledged(SubscriptionAck::decode(bytes)?));
        }
        PullPage::decode(bytes)
            .map(Self::Page)
            .map_err(|e| invalid(format!("invalid live page: {e}")))
    }
}

/// The capability a request advertises when its client applies stream
/// membership changes: [`StreamChange`] pages, removals and enrollment
/// [`MembershipClaim`]s. A package version never implies it; only the
/// request's `capabilities` member does.
pub const STREAM_MEMBERSHIP_CAPABILITY: &str = "stream-membership-v1";
/// The stable refusal code of a request that does not advertise a capability
/// the server requires. It is refused before any handler runs or cursor
/// moves: HTTP 426, and a live subscribe before its acknowledgement.
pub const PROTOCOL_UNSUPPORTED: &str = "protocol.unsupported";
/// The request-envelope member carrying transport negotiation.
const CAPABILITIES: &str = "capabilities";

/// The capabilities a request envelope advertises: absent is none; present,
/// it is an array of distinct nonblank names. Unknown names are kept, so a
/// newer client can advertise more than a server requires.
pub fn read_capabilities(envelope: &Value) -> Result<BTreeSet<String>> {
    let Some(value) = envelope.get(CAPABILITIES) else {
        return Ok(BTreeSet::new());
    };
    let names = value
        .as_array()
        .ok_or_else(|| invalid("capabilities must be an array of names"))?;
    let mut capabilities = BTreeSet::new();
    for name in names {
        let name = name
            .as_str()
            .filter(|name| !name.trim().is_empty())
            .ok_or_else(|| invalid("a capability must be a nonblank string"))?;
        if !capabilities.insert(name.to_string()) {
            return Err(invalid("duplicate capability"));
        }
    }
    Ok(capabilities)
}
/// Validate and remove the negotiation member, leaving the logical request.
pub(crate) fn strip_capabilities(envelope: &mut Value) -> Result<()> {
    read_capabilities(envelope)?;
    if let Some(object) = envelope.as_object_mut() {
        object.remove(CAPABILITIES);
    }
    Ok(())
}
/// The logical request of an envelope: everything but its negotiation
/// metadata. Saved-call identity compares this form, so a retry that now
/// advertises a capability is the same call, not a conflicting one. A
/// malformed `capabilities` member is refused.
pub fn logical_request(envelope: &Value) -> Result<Value> {
    let mut logical = envelope.clone();
    strip_capabilities(&mut logical)?;
    Ok(logical)
}
/// Check a request's existing payload bound, allowing only the fixed wire
/// overhead of advertising stream membership on an already frozen request.
/// Requests within the original raw bound retain their legacy size behavior.
/// Above it, both the raw wire and canonical logical payload are bounded;
/// arbitrary capability names never increase the allowance.
pub fn check_request_size(bytes: &[u8], limit: usize) -> Result<()> {
    if bytes.len() <= limit {
        return Ok(());
    }
    const HEADROOM: usize = br#","capabilities":["stream-membership-v1"]"#.len();
    if bytes.len().saturating_sub(limit) > HEADROOM {
        return Err(invalid("request exceeds byte limit"));
    }
    let envelope: Value = serde_json::from_slice(bytes)?;
    if !read_capabilities(&envelope)?.contains(STREAM_MEMBERSHIP_CAPABILITY)
        || canonical_json(&logical_request(&envelope)?)?.len() > limit
    {
        return Err(invalid("request exceeds byte limit"));
    }
    Ok(())
}

/// The canonical bytes of a request envelope advertising `capabilities`,
/// replacing any it advertised before.
pub fn with_capabilities(envelope: &[u8], capabilities: &[&str]) -> Result<Vec<u8>> {
    let mut value: Value = serde_json::from_slice(envelope)?;
    let names: BTreeSet<&str> = capabilities.iter().copied().collect();
    value
        .as_object_mut()
        .ok_or_else(|| invalid("a request envelope must be an object"))?
        .insert(CAPABILITIES.into(), serde_json::json!(names));
    read_capabilities(&value)?;
    Ok(canonical_json(&value)?.into_bytes())
}
/// Why a request fails capability negotiation, before any business handler.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum NegotiationRefusal {
    /// The request does not advertise the required capability.
    #[error("request does not advertise capability {0}")]
    Unsupported(String),
    /// The envelope or its `capabilities` member is malformed.
    #[error("{0}")]
    Malformed(String),
}
impl NegotiationRefusal {
    /// [`PROTOCOL_UNSUPPORTED`], or `request.invalid` for a malformed request.
    pub fn code(&self) -> &'static str {
        match self {
            Self::Unsupported(_) => PROTOCOL_UNSUPPORTED,
            Self::Malformed(_) => "request.invalid",
        }
    }
}
/// The server's admission check: the request envelope must be a JSON object
/// whose well-formed `capabilities` include `capability`.
pub fn require_capability(
    envelope: &[u8],
    capability: &str,
) -> std::result::Result<(), NegotiationRefusal> {
    let malformed = |error: crate::Error| NegotiationRefusal::Malformed(error.to_string());
    let value: Value = serde_json::from_slice(envelope).map_err(|e| malformed(e.into()))?;
    if !value.is_object() {
        return Err(malformed(invalid("a request envelope must be an object")));
    }
    reject_legacy_ownership(&value).map_err(malformed)?;
    if read_capabilities(&value)
        .map_err(malformed)?
        .contains(capability)
    {
        Ok(())
    } else {
        Err(NegotiationRefusal::Unsupported(capability.into()))
    }
}

/// One membership event of a stream page: the stream it belongs to, its log
/// cursor there, and whether the record is a member. An upsert carries the
/// record's current [`AuthorityRecord`] beside those members; a Loader `null`
/// is stamped absence and a Loader failure an `error`, never a removal. A
/// removal carries only the record's identity: no stamp, state, error or
/// tags. The wire is tagged by `kind` (`upsert` or `remove`); serde decoding
/// validates like [`StreamChange::decode`].
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum StreamChange {
    Upsert {
        stream: String,
        cursor: u64,
        #[serde(flatten)]
        record: AuthorityRecord,
    },
    Remove {
        stream: String,
        cursor: u64,
        #[serde(flatten)]
        key: RecordKey,
    },
}
/// Everything a removal may carry.
const REMOVAL_MEMBERS: [&str; 5] = ["stream", "cursor", "kind", "model", "identity"];
impl StreamChange {
    /// Decode one change: a known `kind`, a named stream, a positive safe
    /// cursor, then an upsert's authority record or a removal's identity and
    /// nothing else.
    pub fn decode(value: &Value) -> Result<Self> {
        reject_legacy_ownership(value)?;
        let object = value
            .as_object()
            .ok_or_else(|| invalid("change must be an object"))?;
        let kind = object
            .get("kind")
            .ok_or_else(|| invalid("change kind missing"))?;
        let stream = read_stream(
            object
                .get("stream")
                .ok_or_else(|| invalid("change stream missing"))?,
        )?;
        let cursor = read_counter(
            object
                .get("cursor")
                .ok_or_else(|| invalid("change cursor missing"))?,
            true,
        )?;
        let change = match kind.as_str() {
            Some("upsert") => {
                let mut record = object.clone();
                for member in STREAM_MEMBERS {
                    record.remove(member);
                }
                Self::Upsert {
                    stream,
                    cursor,
                    record: decode_record(&Value::Object(record))?,
                }
            }
            Some("remove") => {
                if let Some(member) = object
                    .keys()
                    .find(|member| !REMOVAL_MEMBERS.contains(&member.as_str()))
                {
                    return Err(invalid(format!("a removal carries no {member}")));
                }
                let model = object
                    .get("model")
                    .and_then(Value::as_str)
                    .ok_or_else(|| invalid("removal model missing"))?;
                Self::Remove {
                    stream,
                    cursor,
                    key: RecordKey {
                        model: model.to_string(),
                        identity: object.get("identity").cloned().unwrap_or_default(),
                    },
                }
            }
            _ => return Err(invalid("unknown change kind")),
        };
        change.validate()?;
        Ok(change)
    }
    pub fn validate(&self) -> Result<()> {
        check_stream(self.stream())?;
        if self.cursor() == 0 || counter(self.cursor()).is_err() {
            return Err(invalid("change cursor must be a positive counter"));
        }
        match self {
            Self::Upsert { record, .. } => record.validate(),
            Self::Remove { key, .. } => check_key(&key.model, &key.identity),
        }
    }
    pub fn stream(&self) -> &str {
        match self {
            Self::Upsert { stream, .. } | Self::Remove { stream, .. } => stream,
        }
    }
    pub fn cursor(&self) -> u64 {
        match self {
            Self::Upsert { cursor, .. } | Self::Remove { cursor, .. } => *cursor,
        }
    }
    /// The wire `kind`: `upsert` or `remove`.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Upsert { .. } => "upsert",
            Self::Remove { .. } => "remove",
        }
    }
    /// The record whose membership the change states.
    pub fn key(&self) -> RecordKey {
        match self {
            Self::Upsert { record, .. } => RecordKey {
                model: record.model.clone(),
                identity: record.identity.clone(),
            },
            Self::Remove { key, .. } => key.clone(),
        }
    }
    /// An upsert's authority record; a removal has none.
    pub fn record(&self) -> Option<&AuthorityRecord> {
        match self {
            Self::Upsert { record, .. } => Some(record),
            Self::Remove { .. } => None,
        }
    }
}
impl<'de> Deserialize<'de> for StreamChange {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        Self::decode(&Value::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}
/// Membership evidence is unique per stream/record pair and per stream
/// position: the stream log holds one row for each.
fn unique_positions<'a>(
    evidence: impl IntoIterator<Item = (&'a str, u64, RecordKey)>,
) -> Result<()> {
    let (mut pairs, mut positions) = (BTreeSet::new(), BTreeSet::new());
    for (stream, cursor, key) in evidence {
        if !pairs.insert((stream, key.encoded()?)) {
            return Err(invalid("duplicate stream/record pair"));
        }
        if !positions.insert((stream, cursor)) {
            return Err(invalid("two records at one stream position"));
        }
    }
    Ok(())
}
/// The event rules of a stream page: each change belongs to a stream the
/// page covers with `from < cursor <= to` in its range, a stream carries at
/// most [`limits::PULL_CHANGES`] changes, and a pair or position appears once.
fn check_changes(
    changes: &[StreamChange],
    range: impl Fn(&str) -> Option<(u64, u64)>,
) -> Result<()> {
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    for change in changes {
        change.validate()?;
        let (from, to) = range(change.stream())
            .ok_or_else(|| invalid("change names a stream the page does not cover"))?;
        if change.cursor() <= from || change.cursor() > to {
            return Err(invalid("change cursor outside its stream's page range"));
        }
        let count = counts.entry(change.stream()).or_default();
        *count += 1;
        if *count > limits::PULL_CHANGES {
            return Err(invalid(format!(
                "page exceeds {} changes per stream",
                limits::PULL_CHANGES
            )));
        }
    }
    unique_positions(changes.iter().map(|c| (c.stream(), c.cursor(), c.key())))
}
fn read_changes(value: &Value, label: &str) -> Result<Vec<StreamChange>> {
    value
        .as_array()
        .ok_or_else(|| invalid(format!("{label} changes must be an array")))?
        .iter()
        .map(StreamChange::decode)
        .collect()
}

/// The [`PullPage`] envelope with stream changes: each stream's progress
/// and the membership events in each, at most [`limits::PULL_CHANGES`] per
/// stream. A record in two streams appears once per stream, since each is
/// separate membership evidence. A page with any change never decodes as a
/// record-only page, nor a record-only change as a stream change.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct StreamPullPage {
    pub cursors: BTreeMap<String, CursorRange>,
    pub changes: Vec<StreamChange>,
}
impl StreamPullPage {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let value: Value = serde_json::from_slice(bytes)?;
        reject_legacy_ownership(&value)?;
        if !value.is_object() {
            return Err(invalid("page must be an object"));
        }
        for field in ["cursors", "changes"] {
            if value.get(field).is_none() {
                return Err(invalid(format!("page {field} missing")));
            }
        }
        let page = Self {
            cursors: read_ranges(&value["cursors"])?,
            changes: read_changes(&value["changes"], "page")?,
        };
        page.validate()?;
        Ok(page)
    }
    pub fn validate(&self) -> Result<()> {
        check_ranges(&self.cursors)?;
        check_changes(&self.changes, |stream| {
            self.cursors.get(stream).map(|range| (range.from, range.to))
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        Ok(canonical_json(&serde_json::to_value(self)?)?.into_bytes())
    }
    /// The streams the page names, in canonical order.
    pub fn streams(&self) -> impl Iterator<Item = &str> {
        self.cursors.keys().map(String::as_str)
    }
}

/// The [`BootstrapPage`] envelope with stream changes in place of records:
/// the same interval, origin and completion rules, and at most
/// [`limits::PULL_CHANGES`] changes, each in the page's stream with
/// `from < cursor <= to`. A change beyond `until` belongs to the delta lane.
#[derive(Clone, Debug, PartialEq)]
pub struct StreamBootstrapPage {
    pub stream: String,
    pub from: u64,
    pub to: u64,
    pub until: u64,
    pub head: u64,
    pub changes: Vec<StreamChange>,
}
impl StreamBootstrapPage {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let value: Value = serde_json::from_slice(bytes)?;
        reject_legacy_ownership(&value)?;
        if !value.is_object() {
            return Err(invalid("bootstrap page must be an object"));
        }
        read_bootstrap_mode(&value["mode"])?;
        for field in ["stream", "from", "to", "until", "head", "changes"] {
            if value.get(field).is_none() {
                return Err(invalid(format!("bootstrap page {field} missing")));
            }
        }
        let page = Self {
            stream: read_stream(&value["stream"])?,
            from: read_counter(&value["from"], false)?,
            to: read_counter(&value["to"], false)?,
            until: read_counter(&value["until"], false)?,
            head: read_counter(&value["head"], false)?,
            changes: read_changes(&value["changes"], "bootstrap page")?,
        };
        page.validate()?;
        Ok(page)
    }
    pub fn validate(&self) -> Result<()> {
        check_interval(&self.stream, self.from, self.to, self.until, self.head)?;
        check_changes(&self.changes, |stream| {
            (stream == self.stream).then_some((self.from, self.to))
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        Ok(canonical_json(&serde_json::json!({
            "mode": BOOTSTRAP_MODE,
            "stream": self.stream,
            "from": self.from,
            "to": self.to,
            "until": self.until,
            "head": self.head,
            "changes": self.changes,
        }))?
        .into_bytes())
    }
    /// Whether the page finished the historical interval, as
    /// [`BootstrapPage::terminal`].
    pub fn terminal(&self) -> bool {
        self.to == self.until
    }
    /// Whether the page answers this request, as [`BootstrapPage::answers`].
    pub fn answers(&self, request: &BootstrapRequest) -> bool {
        answers_interval(&self.stream, self.from, self.to, self.until, request)
    }
}

/// A live frame when the session applies stream changes: the same
/// acknowledgement, or a [`StreamPullPage`].
#[derive(Clone, Debug, PartialEq)]
pub enum StreamLiveMessage {
    Acknowledged(SubscriptionAck),
    Page(StreamPullPage),
}
impl StreamLiveMessage {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let value: Value = serde_json::from_slice(bytes)?;
        reject_legacy_ownership(&value)?;
        if !value.is_object() {
            return Err(invalid("invalid live frame"));
        }
        if value.get("type").is_some() {
            return Ok(Self::Acknowledged(SubscriptionAck::decode(bytes)?));
        }
        StreamPullPage::decode(bytes)
            .map(Self::Page)
            .map_err(|e| invalid(format!("invalid live page: {e}")))
    }
}

/// One enrollment claim a response carries beside the authority records it
/// returns: a stream the call enrolled a returned record in, and the pair's
/// current upsert cursor from the same transaction. It is membership evidence
/// in that stream's cursor order, not another cursor namespace and never a
/// Model field. Serde decoding validates like [`MembershipClaim::decode`].
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MembershipClaim {
    pub stream: String,
    pub cursor: u64,
    pub model: String,
    pub identity: Value,
}
impl MembershipClaim {
    const MEMBERS: [&str; 4] = ["stream", "cursor", "model", "identity"];
    /// Decode exactly `{stream, cursor, model, identity}`.
    pub fn decode(value: &Value) -> Result<Self> {
        reject_legacy_ownership(value)?;
        let object = value
            .as_object()
            .ok_or_else(|| invalid("membership claim must be an object"))?;
        if let Some(member) = object
            .keys()
            .find(|member| !Self::MEMBERS.contains(&member.as_str()))
        {
            return Err(invalid(format!("a membership claim carries no {member}")));
        }
        let claim = Self {
            stream: read_stream(&value["stream"])?,
            cursor: read_counter(&value["cursor"], true)?,
            model: value["model"]
                .as_str()
                .ok_or_else(|| invalid("membership claim model missing"))?
                .to_string(),
            identity: value["identity"].clone(),
        };
        claim.validate()?;
        Ok(claim)
    }
    pub fn validate(&self) -> Result<()> {
        check_stream(&self.stream)?;
        if self.cursor == 0 || counter(self.cursor).is_err() {
            return Err(invalid(
                "membership claim cursor must be a positive counter",
            ));
        }
        check_key(&self.model, &self.identity)
    }
    /// The claimed record.
    pub fn key(&self) -> RecordKey {
        RecordKey {
            model: self.model.clone(),
            identity: self.identity.clone(),
        }
    }
}
impl<'de> Deserialize<'de> for MembershipClaim {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        Self::decode(&Value::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}
/// The response-envelope member carrying enrollment claims.
const MEMBERSHIPS: &str = "memberships";
/// Read the `memberships` metadata of an enrollment-capable response (a Load
/// page, or Mutation or direct Action readback) against the authority records
/// it returns. Absent means the call enrolled nothing.
pub fn read_memberships(
    response: &Value,
    records: &[AuthorityRecord],
) -> Result<Vec<MembershipClaim>> {
    let Some(value) = response.get(MEMBERSHIPS) else {
        return Ok(vec![]);
    };
    let claims = value
        .as_array()
        .ok_or_else(|| invalid("memberships must be an array"))?
        .iter()
        .map(MembershipClaim::decode)
        .collect::<Result<Vec<_>>>()?;
    validate_memberships(&claims, records)?;
    Ok(claims)
}
/// Every claim is valid, names a record the response returns, and is unique
/// per stream/record pair and per stream position.
pub fn validate_memberships(claims: &[MembershipClaim], records: &[AuthorityRecord]) -> Result<()> {
    let returned = records
        .iter()
        .map(|record| {
            RecordKey {
                model: record.model.clone(),
                identity: record.identity.clone(),
            }
            .encoded()
        })
        .collect::<Result<BTreeSet<_>>>()?;
    for claim in claims {
        claim.validate()?;
        if !returned.contains(&claim.key().encoded()?) {
            return Err(invalid(
                "membership claim names a record the response does not return",
            ));
        }
    }
    unique_positions(
        claims
            .iter()
            .map(|c| (c.stream.as_str(), c.cursor, c.key())),
    )
}
