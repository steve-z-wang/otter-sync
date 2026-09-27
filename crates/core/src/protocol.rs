use crate::{
    ActionIntent, ActionOutcome, CallCompletion, MAX_SAFE_INTEGER, Result, Schema, canonical_json,
    invalid,
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
    /// this many continues: the channel may hold more beyond `to`.
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
        if bytes.len() > limits::PUSH_BYTES {
            return Err(invalid("Action push exceeds byte limit"));
        }
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
        let raw: Value = serde_json::from_slice(bytes)?;
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
/// It names no channel and no cursor: authority is ordered by stamp alone
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
        if self.model.is_empty() {
            return Err(invalid("record model must not be empty"));
        }
        if !self.identity.is_object() {
            return Err(invalid("record identity must be an object"));
        }
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
pub(crate) fn decode_record(value: &Value) -> Result<AuthorityRecord> {
    if !value.is_object() {
        return Err(invalid("record must be an object"));
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
/// `{"book:demo":42,"inbox:alice":7}`: one cursor per channel, at least one channel.
pub fn read_cursors(value: &Value) -> Result<BTreeMap<String, u64>> {
    let object = value
        .as_object()
        .ok_or_else(|| invalid("cursors must map channels to cursors"))?;
    if object.is_empty() {
        return Err(invalid("cursors must name at least one channel"));
    }
    object
        .iter()
        .map(|(channel, cursor)| {
            check_channel(channel)?;
            Ok((channel.clone(), read_counter(cursor, false)?))
        })
        .collect()
}
/// One pull for every subscribed channel: where the client is in each, and
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
/// A channel's progress in one page: the cursor the page starts after, the
/// cursor it reaches, and the channel's head when the page was built. Equal
/// `from` and `to` means nothing changed; `to` below `head` means the channel
/// has more and the client pulls again.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorRange {
    pub from: u64,
    pub to: u64,
    pub head: u64,
}
impl CursorRange {
    /// Whether the channel holds changes beyond `to`.
    pub fn continues(&self) -> bool {
        self.to < self.head
    }
}
/// One page for every channel it names: each channel's progress, and the
/// records changed in any of them, once each at its current stamp. A change is
/// the same [`AuthorityRecord`] a receipt carries; a channel never appears on
/// a record. The server scans at most [`limits::PULL_CHANGES`] invalidations
/// per channel; a channel whose `to` is below its head continues.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PullPage {
    pub cursors: BTreeMap<String, CursorRange>,
    pub changes: Vec<AuthorityRecord>,
}
impl PullPage {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let value: Value = serde_json::from_slice(bytes)?;
        if !value.is_object() {
            return Err(invalid("page must be an object"));
        }
        for field in ["cursors", "changes"] {
            if value.get(field).is_none() {
                return Err(invalid(format!("page {field} missing")));
            }
        }
        let cursors_value = value["cursors"]
            .as_object()
            .ok_or_else(|| invalid("page cursors must map channels to ranges"))?;
        let mut cursors = BTreeMap::new();
        for (channel, range) in cursors_value {
            check_channel(channel)?;
            let from = read_counter(&range["from"], false)?;
            let to = read_counter(&range["to"], false)?;
            let head = read_counter(&range["head"], false)?;
            cursors.insert(channel.clone(), CursorRange { from, to, head });
        }
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
        if self.cursors.is_empty() {
            return Err(invalid("page must name at least one channel"));
        }
        for (channel, range) in &self.cursors {
            check_channel(channel)?;
            counter(range.from)?;
            counter(range.to)?;
            counter(range.head)?;
            if range.to < range.from {
                return Err(invalid("page moves backwards"));
            }
            if range.head < range.to {
                return Err(invalid("page reaches past the channel head"));
            }
        }
        if self.changes.len() > limits::PULL_CHANGES * self.cursors.len() {
            return Err(invalid(format!(
                "page exceeds {} changes per channel",
                limits::PULL_CHANGES
            )));
        }
        unique_records(&self.changes)
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        Ok(canonical_json(&serde_json::to_value(self)?)?.into_bytes())
    }
    /// The channels the page names, in canonical order.
    pub fn channels(&self) -> impl Iterator<Item = &str> {
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
/// Read the one channel name of a bounded pull envelope.
fn read_channel(value: &Value) -> Result<String> {
    let channel = value
        .as_str()
        .ok_or_else(|| invalid("channel must be a string"))?;
    check_channel(channel)?;
    Ok(channel.to_string())
}
/// Require the bounded-pull mode: the envelope is not a bootstrap one without it.
fn read_bootstrap_mode(value: &Value) -> Result<()> {
    if value.as_str() == Some(BOOTSTRAP_MODE) {
        Ok(())
    } else {
        Err(invalid("mode must be \"bootstrap\""))
    }
}
/// One bounded page request of a Scope's historical interval: the channel it
/// loads, the read contracts it expects (as in [`PullRequest::models`]), the
/// committed progress `after` (B) and the subscription origin `until` (S).
/// The server walks `(after, until]` and never chases a moving head; the owner
/// comes from authentication
/// ([#151](https://github.com/zanminwang/axton/issues/151)).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootstrapRequest {
    pub channel: String,
    pub models: BTreeMap<String, u64>,
    pub after: u64,
    pub until: u64,
}
impl BootstrapRequest {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let value: Value = serde_json::from_slice(bytes)?;
        if !value.is_object() {
            return Err(invalid("bootstrap request must be an object"));
        }
        read_bootstrap_mode(&value["mode"])?;
        let request = Self {
            channel: read_channel(&value["channel"])?,
            models: read_models(&value["models"])?,
            after: read_counter(&value["after"], false)?,
            until: read_counter(&value["until"], false)?,
        };
        request.validate()?;
        Ok(request)
    }
    pub fn validate(&self) -> Result<()> {
        check_channel(&self.channel)?;
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
            "channel": self.channel,
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
/// One bounded page of a Scope's historical interval: the echoed channel and
/// origin, the interval `(from, to]` the page covers, the channel head its
/// transaction observed, and the records published at or below `until` in that
/// interval, at most [`limits::PULL_CHANGES`] of them, once each at their
/// current stamp. `to == until` completes the interval, so no done flag
/// travels; `head` is the completion barrier the client stores on that final
/// page ([Protocol / Pull](../../../docs/engineering/architecture/protocol/pull.md)).
#[derive(Clone, Debug, PartialEq)]
pub struct BootstrapPage {
    pub channel: String,
    pub from: u64,
    pub to: u64,
    pub until: u64,
    pub head: u64,
    pub records: Vec<AuthorityRecord>,
}
impl BootstrapPage {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let value: Value = serde_json::from_slice(bytes)?;
        if !value.is_object() {
            return Err(invalid("bootstrap page must be an object"));
        }
        read_bootstrap_mode(&value["mode"])?;
        for field in ["channel", "from", "to", "until", "head", "records"] {
            if value.get(field).is_none() {
                return Err(invalid(format!("bootstrap page {field} missing")));
            }
        }
        let page = Self {
            channel: read_channel(&value["channel"])?,
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
        check_channel(&self.channel)?;
        for cursor in [self.from, self.to, self.until, self.head] {
            counter(cursor)?;
        }
        if self.to < self.from {
            return Err(invalid("bootstrap page moves backwards"));
        }
        if self.until < self.to {
            return Err(invalid("bootstrap page reaches past its origin"));
        }
        if self.head < self.until {
            return Err(invalid("bootstrap origin is past the channel head"));
        }
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
            "channel": self.channel,
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
    /// Whether the page answers this request: the echoed channel and origin,
    /// the requested `from`, and progress that never moves backwards. A
    /// nonterminal page must advance, so a repeated `from` is refused.
    pub fn answers(&self, request: &BootstrapRequest) -> bool {
        self.channel == request.channel
            && self.from == request.after
            && self.until == request.until
            && self.to >= self.from
            && (self.terminal() || self.to > self.from)
    }
}

/// The one client frame of a live session:
/// `{"type":"subscribe","channels":[…],"models":{…}}`. Channels are normalized
/// on decode and on construction: deduplicated and sorted by UTF-16 code
/// units, the order the acknowledgement uses. `models` declares the read
/// contracts every frame of the session is served at, as in [`PullRequest`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscribeRequest {
    pub channels: Vec<String>,
    pub models: BTreeMap<String, u64>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubscribeWire {
    #[serde(rename = "type")]
    kind: String,
    channels: Vec<String>,
    #[serde(default)]
    models: Value,
}
/// The one channel-name rule: a name that is empty, or nothing but whitespace,
/// names no channel. Every frame that carries channel names is refused for it,
/// and so is a durable client registration, so a Scope no socket could ever
/// subscribe cannot be stored either.
pub fn check_channel(channel: &str) -> Result<()> {
    if channel.trim().is_empty() {
        return Err(invalid("channel must not be empty"));
    }
    Ok(())
}
fn normalize_channels(channels: Vec<String>) -> Result<Vec<String>> {
    if channels.is_empty() {
        return Err(invalid("subscribe requires at least one channel"));
    }
    for channel in &channels {
        check_channel(channel)?;
    }
    let mut channels: Vec<_> = channels
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    channels.sort_by(|left, right| left.encode_utf16().cmp(right.encode_utf16()));
    Ok(channels)
}
impl SubscribeRequest {
    pub fn new(channels: Vec<String>, models: BTreeMap<String, u64>) -> Result<Self> {
        Ok(Self {
            channels: normalize_channels(channels)?,
            models: read_models(&serde_json::to_value(&models)?)?,
        })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let wire: SubscribeWire = serde_json::from_slice(bytes)?;
        if wire.kind != "subscribe" {
            return Err(invalid("expected one subscribe frame with channels"));
        }
        Self::new(wire.channels, read_models(&wire.models)?)
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(canonical_json(
            &serde_json::json!({"type":"subscribe","channels":self.channels,"models":self.models}),
        )?
        .into_bytes())
    }
}

/// The server's answer to a subscribe frame: every channel's current head.
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
    /// Whether the server acknowledged exactly the requested channel set.
    pub fn confirms(&self, request: &SubscribeRequest) -> bool {
        self.cursors.keys().eq(request.channels.iter())
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(
            canonical_json(&serde_json::json!({"type":"subscribed","cursors":self.cursors}))?
                .into_bytes(),
        )
    }
}

/// A frame the server sends on a live socket: the acknowledgement carries a
/// `type`, a page never does ([Protocol / Subscriptions](../../../docs/engineering/architecture/protocol/subscriptions.md)).
#[derive(Clone, Debug, PartialEq)]
pub enum LiveMessage {
    Acknowledged(SubscriptionAck),
    Page(PullPage),
}
impl LiveMessage {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let value: Value = serde_json::from_slice(bytes)?;
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
