//! Shared Action contracts. The compiler, client and server use these same
//! descriptors to normalize invocation data before any application code runs.
use crate::{
    AuthorityRecord, EnumDescriptor, FieldDescriptor, MAX_SAFE_INTEGER, ModelDescriptor, Result,
    Schema, ValueType, canonical_json, invalid,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::collections::BTreeSet;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActionIntent {
    pub call_id: String,
    pub name: String,
    pub version: u64,
    pub args: Value,
    /// Per-invocation output storage policy, outside business args. The
    /// default (all) is omitted, so default requests keep their bytes.
    #[serde(default, skip_serializing_if = "ActionStore::is_all")]
    pub store: ActionStore,
}
impl ActionIntent {
    pub fn normalize(mut self, schema: &Schema) -> Result<Self> {
        self.call_id = normalize_call_id(&self.call_id)?;
        let action = schema.action(&self.name, self.version)?;
        self.args = normalize_action_args(schema, action, &self.args)?;
        self.store.validate(action)?;
        self.store = self.store.canonical();
        Ok(self)
    }
}

/// Which explicit Model outputs of one invocation contribute additional
/// local authority. It never disables authority required by mutation inputs
/// or handler-reported changes, and never changes the returned result.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum ActionStore {
    /// Omitted or `true`: every eligible output is stored.
    #[default]
    All,
    /// `false`: no eligible output is stored.
    None,
    /// Output name to storage; unnamed eligible outputs default to true.
    Outputs(BTreeMap<String, bool>),
}
impl ActionStore {
    pub fn is_all(&self) -> bool {
        matches!(self, Self::All)
    }
    /// Whether the named output contributes additional authority.
    pub fn selects(&self, output: &str) -> bool {
        match self {
            Self::All => true,
            Self::None => false,
            Self::Outputs(map) => map.get(output).copied().unwrap_or(true),
        }
    }
    /// Semantic validation against the retained Action: map keys must name
    /// eligible outputs. Boolean policies are valid for every Action.
    pub fn validate(&self, action: &ActionDescriptor) -> Result<()> {
        if let Self::Outputs(map) = self {
            for name in map.keys() {
                if !action
                    .outputs
                    .iter()
                    .any(|output| &output.name == name && store_eligible(output))
                {
                    return Err(invalid(format!(
                        "store names no explicit Model output {name}"
                    )));
                }
            }
        }
        Ok(())
    }
    /// The canonical policy, taken after [`Self::validate`] has seen every
    /// explicit entry: entries equal to the default (`true`) are dropped and
    /// an empty map is the default. Equivalent policies share one identity.
    pub fn canonical(self) -> Self {
        match self {
            Self::Outputs(map) => {
                let map: BTreeMap<String, bool> =
                    map.into_iter().filter(|(_, stored)| !stored).collect();
                if map.is_empty() {
                    Self::All
                } else {
                    Self::Outputs(map)
                }
            }
            other => other,
        }
    }
    /// The wire value, or `None` for the omitted default. Callers send and
    /// fingerprint the [`Self::canonical`] form.
    pub fn wire(&self) -> Option<Value> {
        match self {
            Self::All => None,
            Self::None => Some(Value::Bool(false)),
            Self::Outputs(map) => Some(serde_json::json!(map)),
        }
    }
    /// Decode the wire value: a boolean or an object of booleans.
    pub fn from_wire(value: &Value) -> Result<Self> {
        match value {
            Value::Bool(true) => Ok(Self::All),
            Value::Bool(false) => Ok(Self::None),
            Value::Object(map) if map.is_empty() => Ok(Self::All),
            Value::Object(map) => map
                .iter()
                .map(|(name, value)| {
                    value
                        .as_bool()
                        .map(|stored| (name.clone(), stored))
                        .ok_or_else(|| invalid("store output value must be boolean"))
                })
                .collect::<Result<_>>()
                .map(Self::Outputs),
            _ => Err(invalid("store must be a boolean or an object of booleans")),
        }
    }
}
impl Serialize for ActionStore {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        self.wire()
            .unwrap_or(Value::Bool(true))
            .serialize(serializer)
    }
}
impl<'de> Deserialize<'de> for ActionStore {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        Self::from_wire(&Value::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}
/// An output a store map may name: an explicit, handler-selected Model
/// output. Scalar values, input-bound outputs and Delete confirmations
/// carry no additional output-driven authority to select.
pub fn store_eligible(output: &ActionOutputDescriptor) -> bool {
    output.kind == "model"
        && matches!(
            output.source,
            ActionOutputSource::Named(ActionNamedSource::HandlerIdentity)
        )
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DirectActionRequest {
    pub call: ActionIntent,
    pub models: BTreeMap<String, u64>,
}
impl DirectActionRequest {
    /// Structural server ingress. Semantic Action name/version/args failures
    /// remain per-call outcomes after the call identity has been claimed.
    pub fn decode_envelope(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > crate::limits::PUSH_BYTES {
            return Err(invalid("direct Action request exceeds byte limit"));
        }
        let raw: Value = serde_json::from_slice(bytes)?;
        // Negotiation metadata is checked here and kept out of the request.
        crate::read_capabilities(&raw)?;
        let models = crate::protocol::read_action_models(&raw["models"])?;
        let call: ActionIntent = serde_json::from_value(
            raw.get("call")
                .cloned()
                .ok_or_else(|| invalid("direct Action call missing"))?,
        )?;
        let call = ActionIntent {
            call_id: normalize_call_id(&call.call_id)?,
            ..call
        };
        if call.name.trim().is_empty() {
            return Err(invalid("direct Action name missing"));
        }
        Ok(Self { call, models })
    }
    pub fn decode(bytes: &[u8], schema: &Schema) -> Result<Self> {
        let mut request = Self::decode_envelope(bytes)?;
        request.call = request.call.normalize(schema)?;
        validate_action_models(
            schema,
            schema.action(&request.call.name, request.call.version)?,
            &request.models,
        )?;
        Ok(request)
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(canonical_json(&serde_json::to_value(self)?)?.into_bytes())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DirectActionResponse {
    pub completion: CallCompletion,
    pub records: Vec<AuthorityRecord>,
    /// Enrollment claims for returned records ([`MembershipClaim`]); omitted
    /// from the wire when the call enrolled nothing.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub memberships: Vec<crate::MembershipClaim>,
}
impl DirectActionResponse {
    pub fn decode(bytes: &[u8], request: &DirectActionRequest, schema: &Schema) -> Result<Self> {
        if bytes.len() > crate::limits::PUSH_BYTES {
            return Err(invalid("direct Action response exceeds byte limit"));
        }
        let raw: Value = serde_json::from_slice(bytes)?;
        let rejection = if raw["completion"]["outcome"]["status"] == "failed" {
            vec![serde_json::json!({"ordinal":1,"code":raw["completion"]["outcome"]["code"]})]
        } else {
            vec![]
        };
        let mut wrapper = serde_json::json!({"clientId":"direct","batchSequence":1,"rejections":rejection,"completions":[raw["completion"]],"records":raw["records"]});
        if let Some(memberships) = raw.get("memberships") {
            wrapper["memberships"] = memberships.clone();
        }
        let mut mutation = serde_json::to_value(&request.call)?;
        mutation["ordinal"] = serde_json::json!(1);
        let frozen = crate::PushRequest::decode_actions(serde_json::json!({"clientId":"direct","batchSequence":1,"models":request.models,"mutations":[mutation]}).to_string().as_bytes(), schema)?;
        let receipt =
            crate::PushReceipt::decode_actions(wrapper.to_string().as_bytes(), &frozen, schema)?;
        // A failed call enrolled nothing, as it carries no records.
        if !rejection.is_empty() && !receipt.memberships.is_empty() {
            return Err(invalid("a failed direct Action carries no memberships"));
        }
        Ok(Self {
            completion: receipt.completions.into_iter().next().unwrap(),
            records: receipt.records,
            memberships: receipt.memberships,
        })
    }
    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(canonical_json(&serde_json::to_value(self)?)?.into_bytes())
    }
}

pub fn normalize_call_id(value: &str) -> Result<String> {
    normalize_uuid(value, "callId")
}
/// A hyphenated RFC 4122 UUID (versions 1 to 8) in canonical lowercase.
pub(crate) fn normalize_uuid(value: &str, label: &str) -> Result<String> {
    let id = uuid::Uuid::parse_str(value).map_err(|_| invalid(format!("invalid {label} UUID")))?;
    if value.len() != 36
        || id.get_variant() != uuid::Variant::RFC4122
        || !(1..=8).contains(&id.get_version_num())
    {
        return Err(invalid(format!("invalid {label} UUID format/version")));
    }
    Ok(id.to_string())
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum ActionOutcome {
    Succeeded {
        result: Value,
    },
    Failed {
        code: String,
        execution: ExecutionState,
    },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ExecutionState {
    Rejected,
    Unknown,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CallCompletion {
    pub call_id: String,
    pub outcome: ActionOutcome,
}

/// The backend business behavior a retained operation declares. It belongs
/// to the `(name, version)` contract and is independent of the delivery path
/// a client chooses. A descriptor without a kind is a Mutation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CallKind {
    /// May change business state or perform external effects.
    #[default]
    Mutation,
    /// Reads without business side effects: no Model operands, handler
    /// changes, publications or sequence policy.
    Query,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActionDescriptor {
    pub name: String,
    pub version: u64,
    #[serde(default)]
    pub kind: CallKind,
    pub inputs: Vec<ActionInputDescriptor>,
    pub outputs: Vec<ActionOutputDescriptor>,
    #[serde(default)]
    pub input: Option<ActionInputSnapshot>,
    #[serde(default, rename = "outputEnums")]
    pub output_enums: Vec<EnumDescriptor>,
    /// Retained policy metadata (bindings, prerequisites and sequence) stays
    /// intact when the client persists and reopens a schema.
    #[serde(flatten)]
    pub policy: BTreeMap<String, Value>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ActionInputSnapshot {
    #[serde(default)]
    pub models: Vec<ModelDescriptor>,
    #[serde(default)]
    pub enums: Vec<EnumDescriptor>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum ActionInputDescriptor {
    Value {
        name: String,
        #[serde(rename = "type")]
        value_type: ValueType,
        nullable: bool,
        #[serde(default)]
        list: bool,
        #[serde(flatten)]
        metadata: BTreeMap<String, Value>,
    },
    Model {
        name: String,
        model: String,
        operation: String,
        cardinality: String,
        #[serde(default, rename = "allowedPatchFields")]
        allowed_patch_fields: Option<Vec<String>>,
        #[serde(flatten)]
        metadata: BTreeMap<String, Value>,
    },
}
impl ActionInputDescriptor {
    pub fn name(&self) -> &str {
        match self {
            Self::Value { name, .. } | Self::Model { name, .. } => name,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ActionOutputDescriptor {
    pub name: String,
    pub kind: String,
    pub cardinality: String,
    pub source: ActionOutputSource,
    #[serde(rename = "type", default)]
    pub value_type: Option<ValueType>,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub model_read_version: Option<u64>,
    #[serde(default)]
    pub handler_type: Option<ActionIdentityType>,
    #[serde(flatten)]
    pub metadata: BTreeMap<String, Value>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ActionOutputSource {
    Named(ActionNamedSource),
    InputIdentity {
        #[serde(rename = "inputIdentity")]
        input_identity: String,
    },
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ActionNamedSource {
    HandlerValue,
    HandlerIdentity,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActionIdentityType {
    pub kind: String,
    pub model: String,
    pub fields: Vec<ActionIdentityField>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActionIdentityField {
    pub name: String,
    #[serde(rename = "type")]
    pub value_type: ValueType,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModelReadDescriptor {
    pub name: String,
    pub version: u64,
    pub identity: Vec<String>,
    pub fields: Vec<FieldDescriptor>,
    #[serde(default)]
    pub enums: Vec<EnumDescriptor>,
}

impl Schema {
    pub fn action(&self, name: &str, version: u64) -> Result<&ActionDescriptor> {
        self.actions
            .iter()
            .find(|a| a.name == name && a.version == version)
            .ok_or_else(|| invalid(format!("unknown Action {name} v{version}")))
    }
    pub(crate) fn validate_actions(&self) -> Result<()> {
        let mut reads = BTreeSet::new();
        for model in &self.result_models {
            if !reads.insert((model.name.as_str(), model.version)) {
                return Err(invalid("duplicate result Model read contract"));
            }
            Schema::from_value(
                serde_json::json!({"models":[{"name":model.name,"version":model.version,"identity":model.identity,"fields":model.fields}],"enums":model.enums}),
            )?;
        }
        let mut seen = BTreeSet::new();
        for action in &self.actions {
            if action.name.is_empty()
                || action.version == 0
                || action.version > MAX_SAFE_INTEGER
                || !seen.insert((action.name.as_str(), action.version))
            {
                return Err(invalid("invalid or duplicate Action descriptor"));
            }
            if action.kind == CallKind::Query {
                validate_query(action)?;
            }
            let mut names = BTreeSet::new();
            let input_schema = input_schema(self, action)?;
            for input in &action.inputs {
                if input.name().is_empty() || !names.insert(input.name()) {
                    return Err(invalid("invalid Action input name"));
                }
                match input {
                    ActionInputDescriptor::Value {
                        value_type,
                        nullable,
                        list,
                        ..
                    } => {
                        if *list && *nullable {
                            return Err(invalid("Action lists cannot be nullable"));
                        }
                        input_schema.validate_action_type(value_type)?;
                    }
                    ActionInputDescriptor::Model {
                        model,
                        operation,
                        cardinality,
                        allowed_patch_fields,
                        ..
                    } => {
                        self.model(model)?;
                        let descriptor = input_schema.model(model)?;
                        if !["create", "update", "delete"].contains(&operation.as_str())
                            || !valid_cardinality(cardinality)
                        {
                            return Err(invalid("invalid Action Model input"));
                        }
                        if let Some(fields) = allowed_patch_fields {
                            let mut seen = BTreeSet::new();
                            if operation != "update"
                                || fields.iter().any(|name| {
                                    !seen.insert(name)
                                        || descriptor.identity.contains(name)
                                        || !descriptor
                                            .fields
                                            .iter()
                                            .any(|field| &field.name == name)
                                })
                            {
                                return Err(invalid("invalid Action allowed patch field"));
                            }
                        }
                    }
                }
            }
            names.clear();
            for output in &action.outputs {
                if output.name.is_empty() || !names.insert(output.name.as_str()) {
                    return Err(invalid("invalid Action output"));
                }
                self.validate_output(&action.inputs, &action.output_enums, output)?;
            }
        }
        Ok(())
    }
    /// One declared output against the schema: its kind, source, value type
    /// or Model read contract and identity handler type. Shared by Actions
    /// and Loads; the caller owns output-name uniqueness.
    pub(crate) fn validate_output(
        &self,
        inputs: &[ActionInputDescriptor],
        output_enums: &[EnumDescriptor],
        output: &ActionOutputDescriptor,
    ) -> Result<()> {
        if !valid_cardinality(&output.cardinality) {
            return Err(invalid("invalid Action output"));
        }
        match output.kind.as_str() {
            "value" => {
                let ty = output
                    .value_type
                    .as_ref()
                    .ok_or_else(|| invalid("missing Action value type"))?;
                let mut local = self.clone();
                if !output_enums.is_empty() {
                    local.enums = output_enums.to_vec();
                }
                local.validate_action_type(ty)?;
                if !matches!(
                    output.source,
                    ActionOutputSource::Named(ActionNamedSource::HandlerValue)
                ) {
                    return Err(invalid("Action value output needs handlerValue source"));
                }
            }
            "model" | "deleteIdentity" => {
                let model = output
                    .model
                    .as_deref()
                    .ok_or_else(|| invalid("missing Action output Model"))?;
                self.model(model)?;
                if output.kind == "model" {
                    self.result_model(
                        model,
                        output
                            .model_read_version
                            .ok_or_else(|| invalid("missing Model read version"))?,
                    )?;
                }
                match &output.source {
                    ActionOutputSource::Named(ActionNamedSource::HandlerIdentity) => {
                        let handler = output
                            .handler_type
                            .as_ref()
                            .ok_or_else(|| invalid("missing Action identity handler type"))?;
                        let descriptor = self.model(model)?;
                        if handler.kind != "identity"
                            || handler.model != model
                            || handler.fields.len() != descriptor.identity.len()
                            || handler.fields.iter().zip(&descriptor.identity).any(
                                |(field, name)| {
                                    field.name != *name
                                        || descriptor
                                            .fields
                                            .iter()
                                            .find(|f| &f.name == name)
                                            .is_none_or(|f| {
                                                serde_json::to_value(&f.value_type).ok()
                                                    != serde_json::to_value(&field.value_type)
                                                        .ok()
                                            })
                                },
                            )
                        {
                            return Err(invalid("invalid Action identity handler type"));
                        }
                    }
                    ActionOutputSource::InputIdentity {
                        input_identity: name,
                    } => {
                        if !inputs.iter().any(|input| matches!(input, ActionInputDescriptor::Model { name: input_name, model: input_model, .. } if input_name == name && input_model == model)) {
                            return Err(invalid(
                                "Action output source does not name matching Model input",
                            ));
                        }
                    }
                    _ => return Err(invalid("invalid Action Model output source")),
                }
            }
            _ => return Err(invalid("invalid Action output kind")),
        }
        Ok(())
    }
    pub fn result_model(&self, name: &str, version: u64) -> Result<&ModelReadDescriptor> {
        self.result_models
            .iter()
            .find(|m| m.name == name && m.version == version)
            .ok_or_else(|| invalid(format!("unknown result Model {name} v{version}")))
    }
    pub(crate) fn validate_action_type(&self, ty: &ValueType) -> Result<()> {
        match ty {
            ValueType::Enum { name } if !self.enums.iter().any(|e| &e.name == name) => {
                Err(invalid("unknown Action enum"))
            }
            ValueType::List { .. } => Err(invalid(
                "Action value type uses cardinality instead of nested list",
            )),
            _ => Ok(()),
        }
    }
}
/// A Query declares no business effect the framework could apply: no Model
/// operands (so no inferred optimism or mutation readback) and no sequence
/// policy. The same rule holds for parsed and hand-written descriptors.
fn validate_query(action: &ActionDescriptor) -> Result<()> {
    if action
        .inputs
        .iter()
        .any(|input| matches!(input, ActionInputDescriptor::Model { .. }))
    {
        return Err(invalid(format!(
            "Query {} v{} cannot take a Model operand",
            action.name, action.version
        )));
    }
    if action
        .policy
        .get("sequence")
        .is_some_and(|sequence| !sequence.is_null())
    {
        return Err(invalid(format!(
            "Query {} v{} cannot declare a sequence",
            action.name, action.version
        )));
    }
    Ok(())
}
pub fn validate_action_models(
    schema: &Schema,
    action: &ActionDescriptor,
    models: &BTreeMap<String, u64>,
) -> Result<()> {
    let required = action
        .inputs
        .iter()
        .filter_map(|input| match input {
            ActionInputDescriptor::Model { model, .. } => Some(model.as_str()),
            _ => None,
        })
        .chain(
            action
                .outputs
                .iter()
                .filter_map(|output| output.model.as_deref()),
        );
    validate_operation_models(schema, "Action", required, models)
}
/// A request's declared local read contracts: every entry at the local
/// Model version, and every Model the operation touches declared.
pub(crate) fn validate_operation_models<'a>(
    schema: &Schema,
    label: &str,
    required: impl Iterator<Item = &'a str>,
    models: &BTreeMap<String, u64>,
) -> Result<()> {
    for (name, version) in models {
        if schema.model(name)?.version != *version {
            return Err(invalid("unsupported local Model read version"));
        }
    }
    for name in required.collect::<BTreeSet<_>>() {
        if models.get(name) != Some(&schema.model(name)?.version) {
            return Err(invalid(format!(
                "{label} needs local Model read contract {name}"
            )));
        }
    }
    Ok(())
}
fn valid_cardinality(s: &str) -> bool {
    ["single", "optional", "list"].contains(&s)
}
fn input_schema(schema: &Schema, action: &ActionDescriptor) -> Result<Schema> {
    Ok(snapshot_schema(schema, action.input.as_ref()))
}
/// The schema an operation's inputs are read against: its retained input
/// snapshot when it has one, otherwise the current schema.
pub(crate) fn snapshot_schema(schema: &Schema, snapshot: Option<&ActionInputSnapshot>) -> Schema {
    let mut result = schema.clone();
    if let Some(input) = snapshot {
        result.models = input.models.clone();
        result.enums = input.enums.clone();
    }
    result
}
pub fn normalize_action_args(
    schema: &Schema,
    action: &ActionDescriptor,
    args: &Value,
) -> Result<Value> {
    let input_schema = input_schema(schema, action)?;
    let normalized = normalize_inputs(&input_schema, "Action", &action.inputs, args)?;
    validate_action_bindings(&input_schema, action, &normalized)?;
    Ok(normalized)
}
/// Business arguments against declared inputs, read in `input_schema`: an
/// object naming only declared inputs, each normalized by its own contract.
pub(crate) fn normalize_inputs(
    input_schema: &Schema,
    label: &str,
    inputs: &[ActionInputDescriptor],
    args: &Value,
) -> Result<Value> {
    let object = args
        .as_object()
        .ok_or_else(|| invalid(format!("{label} args must be an object")))?;
    if object
        .keys()
        .any(|key| !inputs.iter().any(|i| i.name() == key))
    {
        return Err(invalid(format!("{label} args contain undeclared input")));
    }
    let mut normalized = Map::new();
    for input in inputs {
        let missing = Value::Null;
        let value = match object.get(input.name()) {
            Some(value) => value,
            None if matches!(input, ActionInputDescriptor::Model { cardinality, .. } if cardinality == "optional") => {
                &missing
            }
            None => return Err(invalid(format!("{label} required input missing"))),
        };
        let result = match input {
            ActionInputDescriptor::Value {
                name,
                value_type,
                nullable,
                list,
                ..
            } => {
                let ty = if *list {
                    ValueType::List {
                        element: Box::new(value_type.clone()),
                    }
                } else {
                    value_type.clone()
                };
                input_schema.normalize_value(
                    &FieldDescriptor {
                        name: name.clone(),
                        value_type: ty,
                        nullable: *nullable,
                        default: None,
                        create_default: None,
                    },
                    value,
                )?
            }
            ActionInputDescriptor::Model {
                model,
                operation,
                cardinality,
                allowed_patch_fields,
                ..
            } => normalize_cardinality(value, cardinality, |item| {
                normalize_model_input(
                    input_schema,
                    model,
                    operation,
                    allowed_patch_fields.as_deref(),
                    item,
                )
            })?,
        };
        normalized.insert(input.name().to_string(), result);
    }
    Ok(Value::Object(normalized))
}
fn normalize_cardinality(
    value: &Value,
    cardinality: &str,
    one: impl Fn(&Value) -> Result<Value>,
) -> Result<Value> {
    match cardinality {
        "optional" if value.is_null() => Ok(Value::Null),
        "list" => Ok(Value::Array(
            value
                .as_array()
                .ok_or_else(|| invalid("expected Action list"))?
                .iter()
                .map(one)
                .collect::<Result<Vec<_>>>()?,
        )),
        "single" | "optional" => one(value),
        _ => Err(invalid("invalid Action cardinality")),
    }
}
fn normalize_model_input(
    schema: &Schema,
    model: &str,
    operation: &str,
    allowed: Option<&[String]>,
    value: &Value,
) -> Result<Value> {
    match operation {
        "create" => {
            let object = value
                .as_object()
                .ok_or_else(|| invalid("create input must be object"))?;
            let descriptor = schema.model(model)?;
            let identity: Map<String, Value> = descriptor
                .identity
                .iter()
                .filter_map(|k| object.get(k).map(|v| (k.clone(), v.clone())))
                .collect();
            let key = schema.record_key(model, &Value::Object(identity))?;
            let state = schema.normalize_state(model, value)?;
            let mut result = key.identity.as_object().unwrap().clone();
            result.extend(state.as_object().unwrap().clone());
            Ok(Value::Object(result))
        }
        "delete" => {
            let object = value
                .as_object()
                .ok_or_else(|| invalid("delete input must be object"))?;
            let key = schema.record_key(model, value)?;
            if object.len() != key.identity.as_object().unwrap().len() {
                return Err(invalid("delete input has nonidentity fields"));
            }
            Ok(key.identity)
        }
        "update" => {
            let object = value
                .as_object()
                .ok_or_else(|| invalid("update input must be object"))?;
            let descriptor = schema.model(model)?;
            let identity: Map<String, Value> = descriptor
                .identity
                .iter()
                .filter_map(|field| {
                    object
                        .get(field)
                        .map(|value| (field.clone(), value.clone()))
                })
                .collect();
            let identity = schema.record_key(model, &Value::Object(identity))?.identity;
            let patch: Map<String, Value> = object
                .iter()
                .filter(|(name, _)| !descriptor.identity.contains(name))
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect();
            let patch = schema.validate_patch(model, &Value::Object(patch))?;
            if let Some(allowed) = allowed
                && patch
                    .as_object()
                    .unwrap()
                    .keys()
                    .any(|k| !allowed.contains(k))
            {
                return Err(invalid("disallowed Action patch field"));
            }
            let mut flat = identity.as_object().unwrap().clone();
            flat.extend(patch.as_object().unwrap().clone());
            Ok(Value::Object(flat))
        }
        _ => Err(invalid("invalid Action operation")),
    }
}

/// Shared relation binding check for durable submission and server execution.
pub fn validate_action_bindings(
    schema: &Schema,
    action: &ActionDescriptor,
    args: &Value,
) -> Result<()> {
    for input in &action.inputs {
        let ActionInputDescriptor::Model {
            name,
            model,
            operation,
            cardinality,
            metadata,
            ..
        } = input
        else {
            continue;
        };
        let target = &args[name];
        let targets: Vec<&Value> = match cardinality.as_str() {
            "optional" if target.is_null() => vec![],
            "list" => target
                .as_array()
                .ok_or_else(|| invalid("invalid Action list"))?
                .iter()
                .collect(),
            _ => vec![target],
        };
        if targets.is_empty() {
            continue;
        }
        for binding in metadata
            .get("bindings")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let source_name = binding["slot"]
                .as_str()
                .ok_or_else(|| invalid("invalid Action binding slot"))?;
            let source_input = action
                .inputs
                .iter()
                .find(|input| input.name() == source_name)
                .ok_or_else(|| invalid("unknown Action binding source"))?;
            let ActionInputDescriptor::Model {
                model: source_model,
                cardinality: source_cardinality,
                ..
            } = source_input
            else {
                return Err(invalid("Action binding source must be a Model"));
            };
            if source_cardinality != "single" {
                return Err(invalid("Action binding source must be single"));
            }
            let source = &args[source_name];
            if source.is_null() {
                return Err(invalid("Action binding source missing"));
            }
            let source_desc = schema.model(source_model)?;
            let source_identity: Map<String, Value> = source_desc
                .identity
                .iter()
                .filter_map(|field| {
                    source
                        .get(field)
                        .map(|value| (field.clone(), value.clone()))
                })
                .collect();
            let source_identity = schema
                .record_key(source_model, &Value::Object(source_identity))?
                .identity;
            let fields = binding["fields"]
                .as_array()
                .ok_or_else(|| invalid("invalid Action binding fields"))?;
            let local: Vec<&str> = fields
                .iter()
                .map(|v| {
                    v.as_str()
                        .ok_or_else(|| invalid("invalid Action binding field"))
                })
                .collect::<Result<_>>()?;
            if local.len() != source_desc.identity.len() {
                return Err(invalid("Action binding identity arity mismatch"));
            }
            let target_desc = schema.model(model)?;
            for (field, source_field) in local.iter().zip(&source_desc.identity) {
                let target_type = &target_desc
                    .fields
                    .iter()
                    .find(|candidate| candidate.name == *field)
                    .ok_or_else(|| invalid("Action binding target field missing"))?
                    .value_type;
                let source_type = &source_desc
                    .fields
                    .iter()
                    .find(|candidate| &candidate.name == source_field)
                    .ok_or_else(|| invalid("Action binding source field missing"))?
                    .value_type;
                if serde_json::to_value(target_type)? != serde_json::to_value(source_type)? {
                    return Err(invalid("Action binding field type mismatch"));
                }
            }
            for target in &targets {
                for (field, source_field) in local.iter().zip(&source_desc.identity) {
                    if let Some(actual) = target.get(field) {
                        if actual != &source_identity[source_field] {
                            return Err(invalid("Action binding mismatch"));
                        }
                    } else if operation == "create" {
                        return Err(invalid("Action binding create field missing"));
                    }
                }
            }
        }
    }
    Ok(())
}
pub fn validate_action_result(
    schema: &Schema,
    action: &ActionDescriptor,
    result: &Value,
) -> Result<Value> {
    if action.outputs.is_empty() {
        return if result.is_null() {
            Ok(Value::Null)
        } else {
            Err(invalid("void Action result must be null"))
        };
    }
    let object = result
        .as_object()
        .ok_or_else(|| invalid("Action result must be object"))?;
    if object.len() != action.outputs.len()
        || object
            .keys()
            .any(|key| !action.outputs.iter().any(|o| &o.name == key))
    {
        return Err(invalid("Action result must contain exactly named outputs"));
    }
    let mut normalized = Map::new();
    let retained_input = input_schema(schema, action)?;
    for output in &action.outputs {
        let value =
            normalize_cardinality(
                &object[&output.name],
                &output.cardinality,
                |one| match output.kind.as_str() {
                    "value" => {
                        let mut local = schema.clone();
                        if !action.output_enums.is_empty() {
                            local.enums = action.output_enums.clone();
                        }
                        local.normalize_value(
                            &FieldDescriptor {
                                name: output.name.clone(),
                                value_type: output
                                    .value_type
                                    .clone()
                                    .ok_or_else(|| invalid("missing output type"))?,
                                nullable: false,
                                default: None,
                                create_default: None,
                            },
                            one,
                        )
                    }
                    "model" => normalize_result_model(
                        schema,
                        output
                            .model
                            .as_deref()
                            .ok_or_else(|| invalid("missing output model"))?,
                        output
                            .model_read_version
                            .ok_or_else(|| invalid("missing output read version"))?,
                        one,
                    ),
                    "deleteIdentity" => Ok(retained_input
                        .record_key(
                            output
                                .model
                                .as_deref()
                                .ok_or_else(|| invalid("missing output model"))?,
                            one,
                        )?
                        .identity),
                    _ => Err(invalid("invalid output kind")),
                },
            )?;
        normalized.insert(output.name.clone(), value);
    }
    Ok(Value::Object(normalized))
}
/// Validate a saved result against the read contracts frozen for its call,
/// then add only fields introduced by a compatible local read-contract
/// evolution. This is separate from strict validation of a fresh result.
/// Server replay of an older saved response may use the same seam.
pub fn validate_action_result_after_read_upgrade(
    schema: &Schema,
    action: &ActionDescriptor,
    frozen_reads: &[ModelReadDescriptor],
    result: &Value,
) -> Result<Value> {
    let mut frozen_schema = schema.clone();
    frozen_schema.result_models = frozen_reads.to_vec();
    let mut projected = result.clone();
    if let Some(outputs) = projected.as_object_mut() {
        for output in &action.outputs {
            if output.kind != "model" {
                continue;
            }
            let model = output
                .model
                .as_deref()
                .ok_or_else(|| invalid("missing output model"))?;
            let version = output
                .model_read_version
                .ok_or_else(|| invalid("missing output read version"))?;
            let frozen = frozen_schema.result_model(model, version)?;
            let current = schema.result_model(model, version)?;
            let value = outputs
                .get_mut(&output.name)
                .ok_or_else(|| invalid("missing Action output"))?;
            match output.cardinality.as_str() {
                "list" => {
                    for item in value
                        .as_array_mut()
                        .ok_or_else(|| invalid("invalid Action result list"))?
                    {
                        project_to_frozen_read_fields(item, frozen, current)?;
                    }
                }
                "optional" if value.is_null() => {}
                "single" | "optional" => project_to_frozen_read_fields(value, frozen, current)?,
                _ => return Err(invalid("invalid Action result cardinality")),
            }
        }
    }
    validate_action_result(&frozen_schema, action, &projected)?;
    let mut expanded = result.clone();
    if let Some(outputs) = expanded.as_object_mut() {
        for output in &action.outputs {
            if output.kind != "model" {
                continue;
            }
            let model = output
                .model
                .as_deref()
                .ok_or_else(|| invalid("missing output model"))?;
            let version = output
                .model_read_version
                .ok_or_else(|| invalid("missing output read version"))?;
            let frozen = frozen_schema.result_model(model, version)?;
            let current = schema.result_model(model, version)?;
            let value = outputs
                .get_mut(&output.name)
                .ok_or_else(|| invalid("missing Action output"))?;
            match output.cardinality.as_str() {
                "list" => {
                    for item in value
                        .as_array_mut()
                        .ok_or_else(|| invalid("invalid Action result list"))?
                    {
                        add_new_read_fields(item, frozen, current)?;
                    }
                }
                "optional" if value.is_null() => {}
                "single" | "optional" => add_new_read_fields(value, frozen, current)?,
                _ => return Err(invalid("invalid Action result cardinality")),
            }
        }
    }
    validate_action_result(schema, action, &expanded)
}

fn project_to_frozen_read_fields(
    value: &mut Value,
    frozen: &ModelReadDescriptor,
    current: &ModelReadDescriptor,
) -> Result<()> {
    let object = value
        .as_object_mut()
        .ok_or_else(|| invalid("Model result must be object"))?;
    if object
        .keys()
        .any(|name| !current.fields.iter().any(|field| &field.name == name))
    {
        return Err(invalid("Model result contains undeclared current field"));
    }
    object.retain(|name, _| frozen.fields.iter().any(|field| &field.name == name));
    Ok(())
}

fn add_new_read_fields(
    value: &mut Value,
    frozen: &ModelReadDescriptor,
    current: &ModelReadDescriptor,
) -> Result<()> {
    let object = value
        .as_object_mut()
        .ok_or_else(|| invalid("Model result must be object"))?;
    for field in &current.fields {
        if frozen.fields.iter().any(|prior| prior.name == field.name)
            || object.contains_key(&field.name)
        {
            continue;
        }
        let added = read_field_fallback(field)
            .ok_or_else(|| invalid("new Model result field has no default"))?;
        object.insert(field.name.clone(), added);
    }
    Ok(())
}
/// The value a reader supplies for a read field its producer's same-version
/// contract did not carry: `null` when nullable, else the field's internal
/// literal default. `None` means the snapshot is incomplete.
pub(crate) fn read_field_fallback(field: &FieldDescriptor) -> Option<Value> {
    if field.nullable {
        Some(Value::Null)
    } else {
        field.default.clone()
    }
}
/// A schema holding only this retained read contract, so identity and state
/// codecs validate against the read version rather than the local Model.
pub(crate) fn read_schema(schema: &Schema, descriptor: &ModelReadDescriptor) -> Schema {
    let mut local = schema.clone();
    local.models = vec![ModelDescriptor {
        name: descriptor.name.clone(),
        version: descriptor.version,
        identity: descriptor.identity.clone(),
        fields: descriptor.fields.clone(),
        relations: vec![],
        unique: vec![],
    }];
    local.enums = descriptor.enums.clone();
    local
}
/// A retained Model read contract as a one-model schema, so its records and
/// identities normalize by the rules of that `(name, version)`.
pub(crate) fn read_contract_schema<'a>(
    schema: &'a Schema,
    model: &str,
    version: u64,
) -> Result<(Schema, &'a ModelReadDescriptor)> {
    let descriptor = schema.result_model(model, version)?;
    Ok((read_schema(schema, descriptor), descriptor))
}
fn normalize_result_model(
    schema: &Schema,
    model: &str,
    version: u64,
    value: &Value,
) -> Result<Value> {
    let (local, descriptor) = read_contract_schema(schema, model, version)?;
    let object = value
        .as_object()
        .ok_or_else(|| invalid("Model result must be object"))?;
    let identity: Map<String, Value> = descriptor
        .identity
        .iter()
        .filter_map(|k| object.get(k).map(|v| (k.clone(), v.clone())))
        .collect();
    let key = local.record_key(model, &Value::Object(identity))?;
    if object.len() != descriptor.fields.len()
        || object
            .keys()
            .any(|key| !descriptor.fields.iter().any(|f| &f.name == key))
    {
        return Err(invalid("Model result must contain exactly read fields"));
    }
    let state_fields: Map<String, Value> = object
        .iter()
        .filter(|(key, _)| !descriptor.identity.contains(key))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();
    let state = local.validate_state(model, &Value::Object(state_fields))?;
    let mut result = key.identity.as_object().unwrap().clone();
    result.extend(state.as_object().unwrap().clone());
    Ok(Value::Object(result))
}
pub fn materialize_action_model(
    schema: &Schema,
    model: &str,
    version: u64,
    identity: &Value,
    state: &Value,
) -> Result<Value> {
    let (local, descriptor) = read_contract_schema(schema, model, version)?;
    let key = local.record_key(model, identity)?;
    let fields = state
        .as_object()
        .ok_or_else(|| invalid("Model result state must be object"))?;
    if fields.keys().any(|name| {
        descriptor.identity.contains(name)
            || !descriptor.fields.iter().any(|field| &field.name == name)
    }) {
        return Err(invalid("Model result state has unknown or identity field"));
    }
    let normalized = local.validate_state(model, state)?;
    let mut result = key.identity.as_object().unwrap().clone();
    result.extend(normalized.as_object().unwrap().clone());
    Ok(Value::Object(result))
}
