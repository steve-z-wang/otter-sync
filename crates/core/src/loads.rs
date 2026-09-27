//! Native Load contracts. A Load fills declared Model lists page by page until
//! its backend handler returns no continuation. Its descriptor, request and
//! page outcome are separate from Actions: no Action route resolves a Load.
//!
//! Envelope decoders check structure only (IDs, uniqueness, exact correlation,
//! counts, bytes and outcome shape). Unknown operations, invalid arguments,
//! invalid continuation state and malformed page content fail one item, so a
//! sibling page in the same HTTP batch is unaffected.
use crate::actions::{
    normalize_inputs, normalize_uuid, read_contract_schema, snapshot_schema,
    validate_operation_models,
};
use crate::protocol::{decode_record, limits, read_models};
use crate::{
    ActionInputDescriptor, ActionInputSnapshot, ActionOutputDescriptor, AuthorityRecord,
    EnumDescriptor, MAX_SAFE_INTEGER, Result, Schema, canonical_json, counter, invalid,
    store_eligible, valid_code,
};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};

/// Management members of the generated `client.loads` route; no Load may
/// take their method name.
pub const RESERVED_LOAD_NAMES: &[&str] = &["get", "list", "invalidate"];

/// One retained `(name, version)` Load contract. Inputs are ordinary values
/// read against the retained enum snapshot in `input`; every output is a
/// non-null list of Model identities at a retained read contract. Call-site
/// options (once, refresh) are never part of it.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LoadDescriptor {
    pub name: String,
    pub version: u64,
    pub inputs: Vec<ActionInputDescriptor>,
    pub outputs: Vec<ActionOutputDescriptor>,
    #[serde(default)]
    pub input: ActionInputSnapshot,
    #[serde(default)]
    pub output_enums: Vec<EnumDescriptor>,
}

/// The wrapper around one opaque continuation state: `{"state": JSON}`.
/// `LoadNext::None` is the first request or the end of a traversal; a
/// wrapper holding `null` is a legitimate later request. Deserializing checks
/// only the exact wrapper; [`Continuation::normalized`] applies the portable
/// JSON bounds per item.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Continuation {
    pub state: Value,
}
/// `null` (first request, or completion) or `{"state": …}`.
pub type LoadNext = Option<Continuation>;

impl<'de> Deserialize<'de> for Continuation {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        use serde::de::Error;
        let mut raw = Map::<String, Value>::deserialize(deserializer)?;
        let state = raw
            .remove("state")
            .ok_or_else(|| D::Error::custom("continuation state missing"))?;
        if !raw.is_empty() {
            return Err(D::Error::custom("unknown continuation member"));
        }
        Ok(Self { state })
    }
}

impl Continuation {
    /// A continuation whose state is normalized portable JSON.
    pub fn new(state: Value) -> Result<Self> {
        Ok(Self {
            state: normalize_load_state(&state)?,
        })
    }
    /// This continuation with its state checked and normalized.
    pub fn normalized(&self) -> Result<Self> {
        Self::new(self.state.clone())
    }
}

/// Normalize continuation state as portable JSON: at most
/// [`limits::LOAD_STATE_DEPTH`] nested arrays/objects and
/// [`limits::LOAD_STATE_BYTES`] canonical bytes. Every integral number must be
/// a JavaScript safe integer (larger integers are application strings) and
/// becomes an integer, so `1.0` and `-0` read back as `1` and `0`; other
/// numbers stay floats.
pub fn normalize_load_state(state: &Value) -> Result<Value> {
    let normalized = portable(state, 0)?;
    if canonical_json(&normalized)?.len() > limits::LOAD_STATE_BYTES {
        return Err(invalid("continuation state exceeds byte limit"));
    }
    Ok(normalized)
}
fn portable(value: &Value, depth: usize) -> Result<Value> {
    let nested = || {
        if depth + 1 > limits::LOAD_STATE_DEPTH {
            Err(invalid("continuation state exceeds nesting depth"))
        } else {
            Ok(depth + 1)
        }
    };
    Ok(match value {
        Value::Number(number) => {
            let unsafe_integer = || invalid("continuation state integer outside safe range");
            if let Some(n) = number.as_u64() {
                if n > MAX_SAFE_INTEGER {
                    return Err(unsafe_integer());
                }
                Value::from(n)
            } else if let Some(n) = number.as_i64() {
                if n.unsigned_abs() > MAX_SAFE_INTEGER {
                    return Err(unsafe_integer());
                }
                Value::from(n)
            } else {
                let f = number
                    .as_f64()
                    .filter(|f| f.is_finite())
                    .ok_or_else(|| invalid("continuation state number must be finite"))?;
                if f.fract() != 0.0 {
                    Value::from(f)
                } else if f.abs() <= MAX_SAFE_INTEGER as f64 {
                    Value::from(f as i64)
                } else {
                    return Err(unsafe_integer());
                }
            }
        }
        Value::Array(items) => {
            let depth = nested()?;
            Value::Array(
                items
                    .iter()
                    .map(|item| portable(item, depth))
                    .collect::<Result<_>>()?,
            )
        }
        Value::Object(members) => {
            let depth = nested()?;
            Value::Object(
                members
                    .iter()
                    .map(|(key, item)| Ok((key.clone(), portable(item, depth)?)))
                    .collect::<Result<_>>()?,
            )
        }
        other => other.clone(),
    })
}

/// A required member whose value may be `null`: a missing `continuation` or
/// `next` is malformed, never the first page or completion.
fn required_next<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<LoadNext, D::Error> {
    Option::<Continuation>::deserialize(deserializer)
}

/// One page request of one Load job. `loadId` and `callId` are the job and
/// the page's durable call identity; `models` declares the local read
/// contracts the authority is served at. Business `args` never carry the
/// continuation or call-site options.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LoadIntent {
    pub load_id: String,
    pub call_id: String,
    pub name: String,
    pub version: u64,
    pub args: Value,
    #[serde(deserialize_with = "required_next")]
    pub continuation: LoadNext,
    pub models: BTreeMap<String, u64>,
}
impl LoadIntent {
    /// Per-item semantic normalization against the retained Load: canonical
    /// IDs and continuation state, and arguments by the Load's inputs.
    /// Declared read contracts are checked by [`validate_load_models`].
    pub fn normalize(mut self, schema: &Schema) -> Result<Self> {
        self.load_id = normalize_uuid(&self.load_id, "loadId")?;
        self.call_id = normalize_uuid(&self.call_id, "callId")?;
        self.continuation = self
            .continuation
            .as_ref()
            .map(Continuation::normalized)
            .transpose()?;
        let load = schema.load(&self.name, self.version)?;
        self.args = normalize_load_args(schema, load, &self.args)?;
        Ok(self)
    }
    fn validate_envelope(&self) -> Result<()> {
        for (value, label) in [(&self.load_id, "loadId"), (&self.call_id, "callId")] {
            if &normalize_uuid(value, label)? != value {
                return Err(invalid(format!("{label} must be canonical")));
            }
        }
        if self.name.trim().is_empty() || self.version == 0 || counter(self.version).is_err() {
            return Err(invalid("invalid Load name or version"));
        }
        read_models(&serde_json::to_value(&self.models)?)?;
        Ok(())
    }
}

/// `{"loads":[LoadIntent…]}`: one HTTP grouping of independent page requests.
/// A batch has no shared transaction, sequence or ordering.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoadBatchRequest {
    pub loads: Vec<LoadIntent>,
}
impl LoadBatchRequest {
    /// Structural server ingress: bytes, 1..=8 items, canonical unique load
    /// and call IDs, names, versions and declared read contracts. Unknown
    /// operations and invalid args or state remain item rejections.
    pub fn decode_envelope(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > limits::LOAD_REQUEST_BYTES {
            return Err(invalid("Load request exceeds byte limit"));
        }
        let raw: Value = serde_json::from_slice(bytes)?;
        objects(&raw, "Load request")?;
        let mut request: Self = serde_json::from_value(raw)?;
        for item in &mut request.loads {
            item.load_id = normalize_uuid(&item.load_id, "loadId")?;
            item.call_id = normalize_uuid(&item.call_id, "callId")?;
        }
        request.encode()?;
        Ok(request)
    }
    fn validate(&self) -> Result<()> {
        batch_count(self.loads.len())?;
        let (mut loads, mut calls) = (BTreeSet::new(), BTreeSet::new());
        for item in &self.loads {
            item.validate_envelope()?;
            if !loads.insert(&item.load_id) || !calls.insert(&item.call_id) {
                return Err(invalid("duplicate Load loadId or callId"));
            }
        }
        Ok(())
    }
    /// Canonical bytes, refused past [`limits::LOAD_REQUEST_BYTES`].
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let bytes = canonical_json(&serde_json::to_value(self)?)?.into_bytes();
        if bytes.len() > limits::LOAD_REQUEST_BYTES {
            return Err(invalid("canonical Load request exceeds byte limit"));
        }
        Ok(bytes)
    }
}

/// A bounded terminal or retryable item error: `{code, message}`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoadError {
    pub code: String,
    pub message: String,
}
impl LoadError {
    fn validate(&self) -> Result<()> {
        if !valid_code(&self.code) || self.message.len() > limits::LOAD_ERROR_MESSAGE_BYTES {
            return Err(invalid("invalid Load error"));
        }
        Ok(())
    }
}

/// The outcome of one page. `succeeded` carries the declared identity lists
/// and the next continuation; `failed` is a saved (or deterministic) terminal
/// rejection; `retryable` was rolled back and is resent under the same call ID.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase", deny_unknown_fields)]
pub enum LoadOutcome {
    Succeeded {
        data: Value,
        #[serde(deserialize_with = "required_next")]
        next: LoadNext,
    },
    Failed {
        error: LoadError,
    },
    Retryable {
        error: LoadError,
    },
}

/// One correlated page answer: its IDs, outcome and the authority records of
/// the page's distinct identities. Failed and retryable items carry none.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LoadPageResponse {
    pub load_id: String,
    pub call_id: String,
    pub outcome: LoadOutcome,
    pub records: Vec<AuthorityRecord>,
}
impl LoadPageResponse {
    fn decode(value: Value) -> Result<Self> {
        let object = value
            .as_object()
            .ok_or_else(|| invalid("Load page must be an object"))?;
        if !object.get("outcome").is_some_and(Value::is_object) {
            return Err(invalid("Load page outcome must be an object"));
        }
        let records = object
            .get("records")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("Load page records must be an array"))?
            .iter()
            .map(decode_record)
            .collect::<Result<Vec<_>>>()?;
        let mut page: Self = serde_json::from_value(value)?;
        page.load_id = normalize_uuid(&page.load_id, "loadId")?;
        page.call_id = normalize_uuid(&page.call_id, "callId")?;
        page.records = records;
        page.validate_shape()?;
        Ok(page)
    }
    fn validate_shape(&self) -> Result<()> {
        for (value, label) in [(&self.load_id, "loadId"), (&self.call_id, "callId")] {
            if &normalize_uuid(value, label)? != value {
                return Err(invalid(format!("{label} must be canonical")));
            }
        }
        match &self.outcome {
            LoadOutcome::Succeeded { data, .. } => {
                if !data.is_object() {
                    return Err(invalid("Load page data must be an object"));
                }
            }
            LoadOutcome::Failed { error } | LoadOutcome::Retryable { error } => {
                error.validate()?;
                if !self.records.is_empty() {
                    return Err(invalid("an unsuccessful Load page carries no records"));
                }
            }
        }
        Ok(())
    }
    /// Whether this page answers exactly this frozen page request.
    pub fn answers(&self, intent: &LoadIntent) -> bool {
        self.load_id == intent.load_id && self.call_id == intent.call_id
    }
    /// Per-item semantic validation against the frozen request's Load: at
    /// most [`limits::LOAD_PAGE_BYTES`], every declared output as a list of
    /// normalized identities (at most [`limits::LOAD_PAGE_IDENTITIES`] in
    /// all), a portable next state, and exactly one state-bearing record per
    /// distinct identity. A failure here fails only this job.
    pub fn normalize(mut self, schema: &Schema, intent: &LoadIntent) -> Result<Self> {
        if !self.answers(intent) {
            return Err(invalid("Load page answers another request"));
        }
        if canonical_json(&serde_json::to_value(&self)?)?.len() > limits::LOAD_PAGE_BYTES {
            return Err(invalid("Load page exceeds byte limit"));
        }
        if let LoadOutcome::Succeeded { data, next } = &mut self.outcome {
            let load = schema.load(&intent.name, intent.version)?;
            *data = validate_load_data(schema, load, data)?;
            *next = next.as_ref().map(Continuation::normalized).transpose()?;
            validate_load_records(schema, load, data, &self.records)?;
        }
        Ok(self)
    }
}

/// `{"loads":[LoadPageResponse…]}`: exactly one answer per requested page.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoadBatchResponse {
    pub loads: Vec<LoadPageResponse>,
}
impl LoadBatchResponse {
    /// Structural client ingress against the frozen request. Malformed,
    /// duplicate, extra or missing correlations reject the whole envelope
    /// before any page may be applied; order carries no meaning.
    pub fn decode(bytes: &[u8], request: &LoadBatchRequest) -> Result<Self> {
        if bytes.len() > limits::LOAD_RESPONSE_BYTES {
            return Err(invalid("Load response exceeds byte limit"));
        }
        let raw: Value = serde_json::from_slice(bytes)?;
        let items = objects(&raw, "Load response")?;
        let response = Self {
            loads: items
                .iter()
                .cloned()
                .map(LoadPageResponse::decode)
                .collect::<Result<_>>()?,
        };
        response.validate()?;
        if response.loads.len() != request.loads.len()
            || !response
                .loads
                .iter()
                .all(|page| request.loads.iter().any(|intent| page.answers(intent)))
        {
            return Err(invalid("Load response does not answer its request"));
        }
        Ok(response)
    }
    fn validate(&self) -> Result<()> {
        batch_count(self.loads.len())?;
        let (mut loads, mut calls) = (BTreeSet::new(), BTreeSet::new());
        for page in &self.loads {
            page.validate_shape()?;
            if !loads.insert(&page.load_id) || !calls.insert(&page.call_id) {
                return Err(invalid("duplicate Load page correlation"));
            }
        }
        Ok(())
    }
    /// Canonical bytes, refused past [`limits::LOAD_RESPONSE_BYTES`].
    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let bytes = canonical_json(&serde_json::to_value(self)?)?.into_bytes();
        if bytes.len() > limits::LOAD_RESPONSE_BYTES {
            return Err(invalid("canonical Load response exceeds byte limit"));
        }
        Ok(bytes)
    }
}

/// An envelope `{"loads":[object…]}` with nothing else at the top level.
fn objects<'a>(raw: &'a Value, label: &str) -> Result<&'a Vec<Value>> {
    let object = raw
        .as_object()
        .ok_or_else(|| invalid(format!("{label} must be an object")))?;
    let items = object
        .get("loads")
        .and_then(Value::as_array)
        .filter(|_| object.len() == 1)
        .ok_or_else(|| invalid(format!("{label} must be exactly {{\"loads\":[…]}}")))?;
    if !items.iter().all(Value::is_object) {
        return Err(invalid(format!("{label} items must be objects")));
    }
    Ok(items)
}
fn batch_count(count: usize) -> Result<()> {
    if count == 0 || count > limits::LOAD_BATCH_ITEMS {
        return Err(invalid(format!(
            "Load batch must contain 1..{} items",
            limits::LOAD_BATCH_ITEMS
        )));
    }
    Ok(())
}

impl Schema {
    pub fn load(&self, name: &str, version: u64) -> Result<&LoadDescriptor> {
        self.loads
            .iter()
            .find(|l| l.name == name && l.version == version)
            .ok_or_else(|| invalid(format!("unknown Load {name} v{version}")))
    }
    pub(crate) fn validate_loads(&self) -> Result<()> {
        let actions: BTreeSet<String> = self.actions.iter().map(|a| method(&a.name)).collect();
        let mut spellings = BTreeMap::<String, &str>::new();
        let mut seen = BTreeSet::new();
        for load in &self.loads {
            let label = format!("Load {} v{}", load.name, load.version);
            if load.name.is_empty()
                || load.version == 0
                || load.version > MAX_SAFE_INTEGER
                || !seen.insert((load.name.as_str(), load.version))
            {
                return Err(invalid("invalid or duplicate Load descriptor"));
            }
            if RESERVED_LOAD_NAMES
                .iter()
                .any(|r| load.name.eq_ignore_ascii_case(r))
            {
                return Err(invalid(format!("Load name {} is reserved", load.name)));
            }
            let name = method(&load.name);
            if actions.contains(&name)
                || spellings
                    .insert(name, &load.name)
                    .is_some_and(|other| other != load.name)
            {
                return Err(invalid(format!(
                    "Load {} shares the operation name of another operation",
                    load.name
                )));
            }
            if !load.input.models.is_empty() {
                return Err(invalid(format!("{label} cannot retain Model operands")));
            }
            let input_schema = snapshot_schema(self, Some(&load.input));
            let mut names = BTreeSet::new();
            for input in &load.inputs {
                if input.name().is_empty() || !names.insert(input.name()) {
                    return Err(invalid("invalid Load input name"));
                }
                match input {
                    ActionInputDescriptor::Value {
                        value_type,
                        nullable,
                        list,
                        ..
                    } => {
                        if *list && *nullable {
                            return Err(invalid("Load lists cannot be nullable"));
                        }
                        input_schema.validate_action_type(value_type)?;
                    }
                    ActionInputDescriptor::Model { .. } => {
                        return Err(invalid(format!("{label} cannot take a Model operand")));
                    }
                }
            }
            if load.outputs.is_empty() {
                return Err(invalid(format!("{label} declares no output")));
            }
            names.clear();
            for output in &load.outputs {
                if output.name.is_empty() || !names.insert(output.name.as_str()) {
                    return Err(invalid("invalid Load output"));
                }
                if output.cardinality != "list"
                    || !store_eligible(output)
                    || output.value_type.is_some()
                    || !output.metadata.is_empty()
                {
                    return Err(invalid(format!(
                        "{label} output {} must be a list of Model identities",
                        output.name
                    )));
                }
                self.validate_output(&load.inputs, &load.output_enums, output)?;
            }
        }
        Ok(())
    }
}
/// The generated method name an operation takes: its first letter lowered.
fn method(name: &str) -> String {
    let mut chars = name.chars();
    chars
        .next()
        .map(|first| first.to_ascii_lowercase().to_string() + chars.as_str())
        .unwrap_or_default()
}

/// Normalize business arguments by the Load's inputs and retained enums.
pub fn normalize_load_args(schema: &Schema, load: &LoadDescriptor, args: &Value) -> Result<Value> {
    normalize_inputs(
        &snapshot_schema(schema, Some(&load.input)),
        "Load",
        &load.inputs,
        args,
    )
}
/// The page request declares each output Model at its local read version.
pub fn validate_load_models(
    schema: &Schema,
    load: &LoadDescriptor,
    models: &BTreeMap<String, u64>,
) -> Result<()> {
    validate_operation_models(
        schema,
        "Load",
        load.outputs.iter().filter_map(|o| o.model.as_deref()),
        models,
    )
}
/// Normalize succeeded page data: exactly the declared outputs, each a list
/// of identities normalized by the output's retained read contract, at most
/// [`limits::LOAD_PAGE_IDENTITIES`] entries across every list. Repeated
/// identities stay in their lists; authority deduplicates them.
pub fn validate_load_data(schema: &Schema, load: &LoadDescriptor, data: &Value) -> Result<Value> {
    let object = data
        .as_object()
        .ok_or_else(|| invalid("Load data must be an object"))?;
    if object.len() != load.outputs.len()
        || object
            .keys()
            .any(|key| !load.outputs.iter().any(|o| &o.name == key))
    {
        return Err(invalid("Load data must contain exactly declared outputs"));
    }
    let mut entries = 0;
    let mut normalized = Map::new();
    for output in &load.outputs {
        let list = object[&output.name]
            .as_array()
            .ok_or_else(|| invalid(format!("Load output {} must be a list", output.name)))?;
        entries += list.len();
        if entries > limits::LOAD_PAGE_IDENTITIES {
            return Err(invalid(format!(
                "Load page exceeds {} identities",
                limits::LOAD_PAGE_IDENTITIES
            )));
        }
        let (model, version) = output_contract(output)?;
        let (contract, _) = read_contract_schema(schema, model, version)?;
        let identities = list
            .iter()
            .map(|identity| Ok(contract.record_key(model, identity)?.identity))
            .collect::<Result<Vec<_>>>()?;
        normalized.insert(output.name.clone(), Value::Array(identities));
    }
    Ok(Value::Object(normalized))
}
/// Exactly one state-bearing record per distinct `(model, identity)` of the
/// normalized data, and nothing else: no read failure, deletion or
/// unrequested record passes as progress.
fn validate_load_records(
    schema: &Schema,
    load: &LoadDescriptor,
    data: &Value,
    records: &[AuthorityRecord],
) -> Result<()> {
    let key = |model: &str, identity: &Value| -> Result<String> {
        Ok(format!("{model}\u{0}{}", canonical_json(identity)?))
    };
    let mut expected = BTreeSet::new();
    for output in &load.outputs {
        let (model, _) = output_contract(output)?;
        for identity in data[&output.name].as_array().into_iter().flatten() {
            expected.insert(key(model, identity)?);
        }
    }
    let mut seen = BTreeSet::new();
    for record in records {
        if record.is_error() || !record.state.is_object() {
            return Err(invalid("Load page records must carry record state"));
        }
        let output = load
            .outputs
            .iter()
            .find(|o| o.model.as_deref() == Some(record.model.as_str()))
            .ok_or_else(|| invalid("Load page record names an undeclared Model"))?;
        let (model, version) = output_contract(output)?;
        let (contract, _) = read_contract_schema(schema, model, version)?;
        let identity = contract.record_key(model, &record.identity)?.identity;
        let record_key = key(model, &identity)?;
        if !expected.contains(&record_key) || !seen.insert(record_key) {
            return Err(invalid(
                "Load page record does not match one enumerated identity",
            ));
        }
    }
    if seen.len() != expected.len() {
        return Err(invalid("Load page identity has no record"));
    }
    Ok(())
}
fn output_contract(output: &ActionOutputDescriptor) -> Result<(&str, u64)> {
    Ok((
        output
            .model
            .as_deref()
            .ok_or_else(|| invalid("missing Load output Model"))?,
        output
            .model_read_version
            .ok_or_else(|| invalid("missing Load output read version"))?,
    ))
}
