//! Shared Fetch contract: a one-shot remote read of one Model, identified by
//! its generated identity, through the Model's existing versioned Loader
//! ([#153](https://github.com/zanminwang/axton/issues/153)). Fetch declares no
//! Action: requests and responses validate against the requested Model read
//! contract only, never an `ActionDescriptor`. The response reuses the direct
//! completion envelope and [`AuthorityRecord`] encoding.
use crate::actions::{read_field_fallback, read_schema};
use crate::protocol::{decode_record, limits, read_counter, valid_code};
use crate::{
    ActionOutcome, AuthorityRecord, CallCompletion, ExecutionState, ModelReadDescriptor, Result,
    Schema, canonical_json, invalid, normalize_call_id,
};
use serde::Serialize;
use serde_json::{Map, Value};

/// `POST /sync/fetch`:
/// `{callId, model, version, identity, store?}`. `identity` is canonical at
/// the requested read contract. `store` defaults to `true` and is omitted from
/// the wire then, so omitted and `true` share one request identity.
#[derive(Clone, Debug, PartialEq)]
pub struct FetchRequest {
    pub call_id: String,
    pub model: String,
    pub version: u64,
    pub identity: Value,
    pub store: bool,
}
impl FetchRequest {
    const MEMBERS: [&str; 5] = ["callId", "model", "version", "identity", "store"];
    /// Structural ingress: call ID, a named Model, a positive version, an
    /// identity object and a boolean storage policy. The Model read contract
    /// is not consulted, so an unsupported Model or version stays a per-call
    /// outcome for a server that has already claimed the call ID.
    pub fn decode_envelope(bytes: &[u8]) -> Result<Self> {
        crate::check_request_size(bytes, limits::PUSH_BYTES)
            .map_err(|_| invalid("Fetch request exceeds byte limit"))?;
        let mut raw: Value = serde_json::from_slice(bytes)?;
        // Negotiation metadata is not part of the call identity.
        crate::protocol::strip_capabilities(&mut raw)?;
        let object = raw
            .as_object()
            .ok_or_else(|| invalid("Fetch request must be an object"))?;
        if let Some(member) = object
            .keys()
            .find(|key| !Self::MEMBERS.contains(&key.as_str()))
        {
            return Err(invalid(format!("unknown Fetch request member {member}")));
        }
        let call_id = normalize_call_id(
            object
                .get("callId")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid("Fetch callId must be a UUID string"))?,
        )?;
        let model = object
            .get("model")
            .and_then(Value::as_str)
            .filter(|model| !model.trim().is_empty())
            .ok_or_else(|| invalid("Fetch model missing"))?
            .to_string();
        let version = read_counter(
            object
                .get("version")
                .ok_or_else(|| invalid("Fetch version missing"))?,
            true,
        )?;
        let identity = object
            .get("identity")
            .filter(|identity| identity.is_object())
            .ok_or_else(|| invalid("Fetch identity must be an object"))?
            .clone();
        let store = match object.get("store") {
            None => true,
            Some(Value::Bool(store)) => *store,
            Some(_) => return Err(invalid("Fetch store must be a boolean")),
        };
        Ok(Self {
            call_id,
            model,
            version,
            identity,
            store,
        })
    }
    /// Full validation: the structural envelope, then the identity normalized
    /// by the requested read contract. Missing identity input is refused,
    /// never defaulted.
    pub fn decode(bytes: &[u8], schema: &Schema) -> Result<Self> {
        let mut request = Self::decode_envelope(bytes)?;
        request.identity = ReadContract::resolve(schema, &request.model, request.version)?
            .identity(&request.identity)?;
        Ok(request)
    }
    /// Canonical request bytes; the default storage policy is omitted.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut wire = serde_json::json!({
            "callId": self.call_id,
            "model": self.model,
            "version": self.version,
            "identity": self.identity,
        });
        if !self.store {
            wire["store"] = Value::Bool(false);
        }
        let bytes = canonical_json(&wire)?.into_bytes();
        if bytes.len() > limits::PUSH_BYTES {
            return Err(invalid("canonical Fetch request exceeds byte limit"));
        }
        Ok(bytes)
    }
}

/// The answer to one [`FetchRequest`]: its direct completion, whose success
/// result is the complete Model snapshot or `null`, and the authority it
/// carries. A stored success carries exactly one record for the requested
/// identity (stamped `null` for absence); `store: false` and a failure carry
/// none. Decoding validates all of it before a caller sees either part.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct FetchResponse {
    pub completion: CallCompletion,
    pub records: Vec<AuthorityRecord>,
}
impl FetchResponse {
    pub fn decode(bytes: &[u8], request: &FetchRequest, schema: &Schema) -> Result<Self> {
        if bytes.len() > limits::PUSH_BYTES {
            return Err(invalid("Fetch response exceeds byte limit"));
        }
        let raw: Value = serde_json::from_slice(bytes)?;
        let object = raw
            .as_object()
            .ok_or_else(|| invalid("Fetch response must be an object"))?;
        let mut completion: CallCompletion = serde_json::from_value(
            object
                .get("completion")
                .cloned()
                .ok_or_else(|| invalid("Fetch completion missing"))?,
        )?;
        let mut records = object
            .get("records")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid("Fetch records must be an array"))?
            .iter()
            .map(decode_record)
            .collect::<Result<Vec<_>>>()?;
        if completion.call_id != request.call_id {
            return Err(invalid("Fetch completion answers another call"));
        }
        match &mut completion.outcome {
            ActionOutcome::Failed { code, execution } => {
                if !valid_code(code) || *execution != ExecutionState::Rejected {
                    return Err(invalid("invalid Fetch failure"));
                }
                if !records.is_empty() {
                    return Err(invalid("a failed Fetch carries no authority"));
                }
            }
            ActionOutcome::Succeeded { result } => {
                let contract = ReadContract::resolve(schema, &request.model, request.version)?;
                let snapshot = match &*result {
                    Value::Null => None,
                    value => Some(contract.snapshot(value)?),
                };
                if let Some(snapshot) = &snapshot
                    && snapshot.identity != request.identity
                {
                    return Err(invalid("Fetch result describes another identity"));
                }
                if records.len() != usize::from(request.store) {
                    return Err(invalid(
                        "a successful Fetch carries authority for exactly the requested record when storing and none otherwise",
                    ));
                }
                for record in &mut records {
                    *record = contract.authority(record, request, snapshot.as_ref())?;
                }
                *result = snapshot.map_or(Value::Null, Snapshot::into_result);
            }
        }
        Ok(Self {
            completion,
            records,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(canonical_json(&serde_json::to_value(self)?)?.into_bytes())
    }
}

/// One Model read contract, `(name, version)`: the retained read contract
/// when the schema keeps one, else the local Model at that same version.
struct ReadContract {
    descriptor: ModelReadDescriptor,
    schema: Schema,
}
/// A normalized complete Model snapshot, split as authority carries it.
struct Snapshot {
    identity: Value,
    state: Value,
}
impl Snapshot {
    fn into_result(self) -> Value {
        let mut result = self.identity.as_object().cloned().unwrap_or_default();
        result.extend(self.state.as_object().cloned().unwrap_or_default());
        Value::Object(result)
    }
}
impl ReadContract {
    fn resolve(schema: &Schema, model: &str, version: u64) -> Result<Self> {
        let descriptor = match schema.result_model(model, version) {
            Ok(retained) => retained.clone(),
            Err(_) => {
                let local = schema
                    .models
                    .iter()
                    .find(|local| local.name == model && local.version == version)
                    .ok_or_else(|| {
                        invalid(format!("unsupported Fetch Model {model} v{version}"))
                    })?;
                ModelReadDescriptor {
                    name: local.name.clone(),
                    version,
                    identity: local.identity.clone(),
                    fields: local.fields.clone(),
                    enums: schema.enums.clone(),
                }
            }
        };
        let schema = read_schema(schema, &descriptor);
        Ok(Self { descriptor, schema })
    }
    fn identity(&self, identity: &Value) -> Result<Value> {
        Ok(self
            .schema
            .record_key(&self.descriptor.name, identity)?
            .identity)
    }
    /// A result object: the exact identity plus the read fields.
    fn snapshot(&self, value: &Value) -> Result<Snapshot> {
        let object = value
            .as_object()
            .ok_or_else(|| invalid("Fetch result must be a Model object or null"))?;
        let (identity, state): (Map<String, Value>, Map<String, Value>) = object
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .partition(|(name, _)| self.descriptor.identity.contains(name));
        self.normalize(&Value::Object(identity), state)
    }
    /// Same-version compatible read rule, shared with wire authority: fields
    /// the contract does not know are ignored, and a read field the producer
    /// did not carry takes its reader fallback. Values are validated strictly.
    fn normalize(&self, identity: &Value, mut state: Map<String, Value>) -> Result<Snapshot> {
        for field in &self.descriptor.fields {
            if self.descriptor.identity.contains(&field.name) || state.contains_key(&field.name) {
                continue;
            }
            if let Some(fallback) = read_field_fallback(field) {
                state.insert(field.name.clone(), fallback);
            }
        }
        Ok(Snapshot {
            identity: self.identity(identity)?,
            state: self
                .schema
                .validate_state(&self.descriptor.name, &Value::Object(state))?,
        })
    }
    /// The one stored record: the requested identity, a read success, and
    /// the same normalized snapshot as the result (`null` for absence).
    fn authority(
        &self,
        record: &AuthorityRecord,
        request: &FetchRequest,
        snapshot: Option<&Snapshot>,
    ) -> Result<AuthorityRecord> {
        if record.is_error() {
            return Err(invalid("Fetch authority cannot be a read failure"));
        }
        if record.model != request.model {
            return Err(invalid("Fetch authority names another Model"));
        }
        let state = match (snapshot, &record.state) {
            (None, Value::Null) => {
                if self.identity(&record.identity)? != request.identity {
                    return Err(invalid("Fetch authority names another identity"));
                }
                Value::Null
            }
            (Some(snapshot), Value::Object(state)) => {
                let stored = self.normalize(&record.identity, state.clone())?;
                if stored.identity != request.identity {
                    return Err(invalid("Fetch authority names another identity"));
                }
                if stored.state != snapshot.state {
                    return Err(invalid("Fetch result and authority disagree"));
                }
                stored.state
            }
            _ => return Err(invalid("Fetch result and authority disagree")),
        };
        Ok(AuthorityRecord {
            model: record.model.clone(),
            identity: request.identity.clone(),
            stamp: record.stamp,
            state,
            error: None,
        })
    }
}
