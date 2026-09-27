use crate::{Result, canonical_json, invalid};
use chrono::{DateTime, SecondsFormat};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeSet;

pub const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Schema {
    pub enums: Vec<EnumDescriptor>,
    pub models: Vec<ModelDescriptor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<crate::ActionDescriptor>,
    /// Every retained native Load version, separate from `actions` so no
    /// Action route can resolve one.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub loads: Vec<crate::LoadDescriptor>,
    #[serde(
        default,
        rename = "resultModels",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub result_models: Vec<crate::ModelReadDescriptor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub requirements: Vec<RequirementDescriptor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub prerequisites: Vec<Value>,
    #[serde(
        default,
        rename = "clientPolicies",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub client_policies: Vec<Value>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RequirementDescriptor {
    pub model: String,
    pub field: String,
    pub name: String,
    pub arguments: std::collections::BTreeMap<String, String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EnumDescriptor {
    pub name: String,
    pub values: Vec<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModelDescriptor {
    pub name: String,
    /// The read-contract version of the records this descriptor describes
    /// (`@@version(n)`, 1 when omitted). Independent of mutation versions and
    /// of record stamps; a loader is selected by model name and this version.
    #[serde(default = "first_version")]
    pub version: u64,
    pub identity: Vec<String>,
    pub fields: Vec<FieldDescriptor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub relations: Vec<RelationDescriptor>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unique: Vec<Vec<String>>,
}
fn first_version() -> u64 {
    1
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RelationDescriptor {
    pub name: String,
    pub target: String,
    pub fields: Vec<String>,
    pub target_fields: Vec<String>,
    pub on_delete: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FieldDescriptor {
    pub name: String,
    #[serde(rename = "type")]
    pub value_type: ValueType,
    pub nullable: bool,
    /// Internal literal used by older reconciliation and read-projection
    /// paths. Source `@default` never populates it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<Value>,
    /// Client creation policy from source `@default`: filled only for an
    /// omitted field of a fresh create, never on update, read, sync, replay
    /// or migration ([#27](https://github.com/zanminwang/axton/issues/27)).
    #[serde(
        default,
        rename = "createDefault",
        skip_serializing_if = "Option::is_none"
    )]
    pub create_default: Option<CreateDefault>,
}
/// How a fresh create fills an omitted field. Generated values are produced
/// once by the client before the create is persisted or sent.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum CreateDefault {
    /// A fixed value, already normalized by the field's contract.
    Literal { value: Value },
    /// A canonical lowercase UUID v4, for String and UUID fields.
    Uuid,
    /// The client's UTC wall clock at millisecond precision, for DateTime fields.
    Now,
}
/// Strict tagged form: an unknown kind or any member other than the kind's
/// own is malformed metadata, not something to ignore.
impl<'de> Deserialize<'de> for CreateDefault {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        use serde::de::Error;
        let mut raw = Map::<String, Value>::deserialize(deserializer)?;
        let kind = raw
            .remove("kind")
            .ok_or_else(|| D::Error::custom("createDefault kind missing"))?;
        let value = raw.remove("value");
        if !raw.is_empty() {
            return Err(D::Error::custom("unknown createDefault member"));
        }
        match (kind.as_str(), value) {
            (Some("literal"), Some(value)) => Ok(Self::Literal { value }),
            (Some("uuid"), None) => Ok(Self::Uuid),
            (Some("now"), None) => Ok(Self::Now),
            _ => Err(D::Error::custom("malformed createDefault")),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum ValueType {
    Scalar { name: ScalarType },
    Enum { name: String },
    List { element: Box<ValueType> },
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ScalarType {
    String,
    Boolean,
    Int,
    Float,
    DateTime,
    Uuid,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordKey {
    pub model: String,
    pub identity: Value,
}
impl RecordKey {
    pub fn encoded_identity(&self) -> Result<String> {
        canonical_json(&self.identity)
    }
    pub fn encoded(&self) -> Result<String> {
        canonical_json(&serde_json::json!([self.model, self.identity]))
    }
}

/// Model names that must not become SQLite tables. `axton_` is the framework's
/// own table prefix and `sqlite_` is reserved by SQLite; both are compared
/// case-insensitively because SQLite table names are.
pub fn reserved_model_name(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    lower.starts_with("axton_") || lower.starts_with("sqlite_")
}

impl Schema {
    pub fn from_value(value: Value) -> Result<Self> {
        let schema: Self = serde_json::from_value(value)?;
        schema.validate()?;
        Ok(schema)
    }
    pub fn validate(&self) -> Result<()> {
        if self.models.is_empty() && self.actions.is_empty() {
            return Err(invalid("models or actions must be nonempty"));
        }
        let mut names = BTreeSet::new();
        for en in &self.enums {
            if en.name.is_empty()
                || !names.insert(en.name.as_str())
                || en.values.is_empty()
                || en.values.iter().any(|v| v.is_empty())
                || en.values.iter().collect::<BTreeSet<_>>().len() != en.values.len()
            {
                return Err(invalid("invalid enum descriptor"));
            }
        }
        for model in &self.models {
            if reserved_model_name(&model.name) {
                return Err(invalid(format!(
                    "model name {} uses a reserved prefix",
                    model.name
                )));
            }
            if model.name.is_empty()
                || !names.insert(model.name.as_str())
                || model.identity.is_empty()
            {
                return Err(invalid("invalid model descriptor"));
            }
            if model.version == 0 || model.version > MAX_SAFE_INTEGER {
                return Err(invalid("invalid model version"));
            }
            let mut fields = BTreeSet::new();
            for field in &model.fields {
                if field.name.is_empty() || !fields.insert(field.name.as_str()) {
                    return Err(invalid("duplicate or empty field"));
                }
                self.validate_type(&field.value_type)?;
                if matches!(field.value_type, ValueType::List { .. }) && field.nullable {
                    return Err(invalid("lists cannot be nullable"));
                }
                if let Some(create_default) = &field.create_default {
                    self.validate_create_default(field, create_default)?;
                }
            }
            let mut identities = BTreeSet::new();
            for name in &model.identity {
                let field = model
                    .fields
                    .iter()
                    .find(|f| &f.name == name)
                    .ok_or_else(|| invalid("identity field missing"))?;
                if !identities.insert(name)
                    || field.nullable
                    || !matches!(field.value_type, ValueType::Scalar { .. })
                {
                    return Err(invalid("invalid identity descriptor"));
                }
            }
        }
        for requirement in &self.requirements {
            let model = self.model(&requirement.model)?;
            if !model.fields.iter().any(|f| f.name == requirement.field)
                || !self
                    .prerequisites
                    .iter()
                    .any(|p| p["name"] == requirement.name)
            {
                return Err(invalid("invalid prerequisite requirement"));
            }
            if requirement.arguments.values().any(|v| v != "self") {
                return Err(invalid("unsupported prerequisite argument expression"));
            }
        }
        for model in &self.models {
            for fields in &model.unique {
                if fields.is_empty()
                    || fields.iter().collect::<BTreeSet<_>>().len() != fields.len()
                    || fields
                        .iter()
                        .any(|name| !model.fields.iter().any(|f| &f.name == name))
                {
                    return Err(invalid("invalid unique constraint"));
                }
            }
            let mut relations = BTreeSet::new();
            for relation in &model.relations {
                let target = self.model(&relation.target)?;
                if relation.name.is_empty()
                    || !relations.insert(&relation.name)
                    || relation.fields.len() != target.identity.len()
                    || relation.target_fields != target.identity
                    || !["delete", "none"].contains(&relation.on_delete.as_str())
                {
                    return Err(invalid("invalid reference relation"));
                }
                for (local, remote) in relation.fields.iter().zip(&relation.target_fields) {
                    let local = model
                        .fields
                        .iter()
                        .find(|f| &f.name == local)
                        .ok_or_else(|| invalid("reference field missing"))?;
                    let remote = target
                        .fields
                        .iter()
                        .find(|f| &f.name == remote)
                        .ok_or_else(|| invalid("target field missing"))?;
                    if serde_json::to_value(&local.value_type)?
                        != serde_json::to_value(&remote.value_type)?
                    {
                        return Err(invalid("reference field type mismatch"));
                    }
                }
            }
        }
        self.validate_actions()?;
        self.validate_loads()?;
        Ok(())
    }
    /// A creation default must suit its field: generators by scalar type,
    /// literals by the field's own normalization. Lists take no default.
    pub(crate) fn validate_create_default(
        &self,
        field: &FieldDescriptor,
        create_default: &CreateDefault,
    ) -> Result<()> {
        let bad = || invalid(format!("invalid createDefault for {}", field.name));
        let scalar = match &field.value_type {
            ValueType::Scalar { name } => Some(*name),
            ValueType::Enum { .. } => None,
            ValueType::List { .. } => return Err(bad()),
        };
        match create_default {
            CreateDefault::Uuid
                if matches!(scalar, Some(ScalarType::String | ScalarType::Uuid)) =>
            {
                Ok(())
            }
            CreateDefault::Now if matches!(scalar, Some(ScalarType::DateTime)) => Ok(()),
            CreateDefault::Literal { value } if !value.is_null() => self
                .value(&field.value_type, value)
                .map(|_| ())
                .map_err(|_| bad()),
            _ => Err(bad()),
        }
    }
    fn validate_type(&self, ty: &ValueType) -> Result<()> {
        match ty {
            ValueType::Enum { name } if !self.enums.iter().any(|e| &e.name == name) => {
                Err(invalid("unknown enum"))
            }
            ValueType::List { element } if !matches!(**element, ValueType::Scalar { .. }) => {
                Err(invalid("list elements must be scalar"))
            }
            _ => Ok(()),
        }
    }
    pub fn model(&self, name: &str) -> Result<&ModelDescriptor> {
        self.models
            .iter()
            .find(|m| m.name == name)
            .ok_or_else(|| invalid(format!("unknown model {name}")))
    }
    pub fn record_key(&self, name: &str, identity: &Value) -> Result<RecordKey> {
        let model = self.model(name)?;
        let input = identity
            .as_object()
            .ok_or_else(|| invalid("identity must be an object"))?;
        if input.len() != model.identity.len() || input.keys().any(|k| !model.identity.contains(k))
        {
            return Err(invalid("identity must contain exactly identity fields"));
        }
        let mut result = Map::new();
        for name in &model.identity {
            let field = model
                .fields
                .iter()
                .find(|f| &f.name == name)
                .ok_or_else(|| invalid("identity descriptor missing"))?;
            result.insert(
                name.clone(),
                self.normalize_value(
                    field,
                    input.get(name).ok_or_else(|| invalid("identity missing"))?,
                )?,
            );
        }
        Ok(RecordKey {
            model: name.into(),
            identity: Value::Object(result),
        })
    }
    /// Loader output may contain identity and omit nullable fields. Wire state may not.
    pub fn normalize_state(&self, model: &str, state: &Value) -> Result<Value> {
        self.state(model, state, true)
    }
    pub fn validate_state(&self, model: &str, state: &Value) -> Result<Value> {
        self.state(model, state, false)
    }
    fn state(&self, name: &str, state: &Value, loader: bool) -> Result<Value> {
        let model = self.model(name)?;
        let input = state
            .as_object()
            .ok_or_else(|| invalid("state must be an object"))?;
        if input.keys().any(|k| {
            (loader && !model.fields.iter().any(|f| &f.name == k))
                || (!loader && model.identity.contains(k))
        }) {
            return Err(invalid("unknown or identity state field"));
        }
        let mut result = Map::new();
        for field in model
            .fields
            .iter()
            .filter(|f| !model.identity.contains(&f.name))
        {
            let value = match input.get(&field.name) {
                Some(v) => v,
                None if field.nullable => &Value::Null,
                None => return Err(invalid(format!("missing state field {}", field.name))),
            };
            result.insert(field.name.clone(), self.normalize_value(field, value)?);
        }
        Ok(Value::Object(result))
    }
    pub fn validate_patch(&self, name: &str, patch: &Value) -> Result<Value> {
        let model = self.model(name)?;
        let input = patch
            .as_object()
            .ok_or_else(|| invalid("patch must be an object"))?;
        let mut result = Map::new();
        for (name, value) in input {
            if model.identity.contains(name) {
                return Err(invalid("identity is immutable"));
            }
            let field = model
                .fields
                .iter()
                .find(|f| &f.name == name)
                .ok_or_else(|| invalid("unknown patch field"))?;
            result.insert(name.clone(), self.normalize_value(field, value)?);
        }
        Ok(Value::Object(result))
    }
    pub fn normalize_value(&self, field: &FieldDescriptor, value: &Value) -> Result<Value> {
        if value.is_null() {
            return if field.nullable {
                Ok(Value::Null)
            } else {
                Err(invalid(format!("{} is not nullable", field.name)))
            };
        }
        self.value(&field.value_type, value)
    }
    fn value(&self, ty: &ValueType, value: &Value) -> Result<Value> {
        match ty {
            ValueType::Scalar { name } => scalar(*name, value),
            ValueType::Enum { name } => {
                if value.as_str().is_some_and(|v| {
                    self.enums
                        .iter()
                        .any(|en| &en.name == name && en.values.iter().any(|x| x == v))
                }) {
                    Ok(value.clone())
                } else {
                    Err(invalid("invalid enum value"))
                }
            }
            ValueType::List { element } => {
                let list = value.as_array().ok_or_else(|| invalid("expected list"))?;
                Ok(Value::Array(
                    list.iter()
                        .map(|v| self.value(element, v))
                        .collect::<Result<_>>()?,
                ))
            }
        }
    }
}
fn scalar(ty: ScalarType, v: &Value) -> Result<Value> {
    match ty {
        ScalarType::String if v.is_string() => Ok(v.clone()),
        ScalarType::Boolean if v.is_boolean() => Ok(v.clone()),
        ScalarType::Int => {
            let f = v.as_f64().ok_or_else(|| invalid("expected integer"))?;
            if f.is_finite() && f.fract() == 0.0 && f.abs() <= MAX_SAFE_INTEGER as f64 {
                Ok(Value::from(f as i64))
            } else {
                Err(invalid("integer outside safe range"))
            }
        }
        ScalarType::Float => {
            let f = v
                .as_f64()
                .filter(|f| f.is_finite())
                .ok_or_else(|| invalid("expected finite float"))?;
            Ok(Value::from(if f == 0.0 { 0.0 } else { f }))
        }
        ScalarType::Uuid => {
            let s = v.as_str().ok_or_else(|| invalid("expected UUID"))?;
            let id = uuid::Uuid::parse_str(s).map_err(|_| invalid("invalid UUID"))?;
            if s.len() != 36
                || id.get_variant() != uuid::Variant::RFC4122
                || !(1..=8).contains(&id.get_version_num())
            {
                return Err(invalid("invalid UUID format/version"));
            }
            Ok(Value::String(id.to_string()))
        }
        ScalarType::DateTime => {
            let s = v
                .as_str()
                .ok_or_else(|| invalid("expected zoned dateTime"))?;
            if s.len() < 20 || s.as_bytes().get(10) != Some(&b'T') {
                return Err(invalid("invalid zoned dateTime"));
            }
            let t =
                DateTime::parse_from_rfc3339(s).map_err(|_| invalid("invalid zoned dateTime"))?;
            Ok(Value::String(
                t.with_timezone(&chrono::Utc)
                    .to_rfc3339_opts(SecondsFormat::Millis, true),
            ))
        }
        _ => Err(invalid("invalid scalar type")),
    }
}

/// How a compiled schema relates to the one a local database was built for.
/// The rules are the model read-contract rules ([Models §9](../../../docs/engineering/architecture/schema/models.md)):
/// within a model version only an added nullable field (or one with a
/// default) is compatible; everything else needs a rebuild.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Compatibility {
    Identical,
    Additive(Vec<AdditiveStep>),
    Incompatible(String),
}

/// One change the storage layer can apply in place.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdditiveStep {
    AddModel(String),
    AddField { model: String, field: String },
}

impl Schema {
    /// Classify `incoming` against `stored`, the schema the database was
    /// built for. Requirements, prerequisites and client policies are not
    /// storage and do not take part.
    pub fn compatibility(stored: &Schema, incoming: &Schema) -> Compatibility {
        let mut steps = vec![];
        let enum_of = |schema: &Schema, name: &str| -> Option<Vec<String>> {
            schema
                .enums
                .iter()
                .find(|e| e.name == name)
                .map(|e| e.values.clone())
        };
        for old in &stored.models {
            let Some(new) = incoming.models.iter().find(|m| m.name == old.name) else {
                return Compatibility::Incompatible(format!("model {} was removed", old.name));
            };
            if new.version != old.version {
                return Compatibility::Incompatible(format!(
                    "model {} changed version from {} to {}",
                    old.name, old.version, new.version
                ));
            }
            if new.identity != old.identity {
                return Compatibility::Incompatible(format!(
                    "model {} changed its identity",
                    old.name
                ));
            }
            if new.unique != old.unique {
                return Compatibility::Incompatible(format!(
                    "model {} changed its unique constraints",
                    old.name
                ));
            }
            let relation = |r: &RelationDescriptor| {
                (
                    r.name.clone(),
                    r.target.clone(),
                    r.fields.clone(),
                    r.target_fields.clone(),
                    r.on_delete.clone(),
                )
            };
            let mut old_relations: Vec<_> = old.relations.iter().map(relation).collect();
            let mut new_relations: Vec<_> = new.relations.iter().map(relation).collect();
            old_relations.sort();
            new_relations.sort();
            if old_relations != new_relations {
                return Compatibility::Incompatible(format!(
                    "model {} changed its relations",
                    old.name
                ));
            }
            for field in &old.fields {
                match new.fields.iter().find(|f| f.name == field.name) {
                    None => {
                        return Compatibility::Incompatible(format!(
                            "field {}.{} was removed or renamed",
                            old.name, field.name
                        ));
                    }
                    Some(next) => {
                        let same_type = serde_json::to_value(&next.value_type).ok()
                            == serde_json::to_value(&field.value_type).ok();
                        if !same_type || next.nullable != field.nullable {
                            return Compatibility::Incompatible(format!(
                                "field {}.{} changed its type or nullability",
                                old.name, field.name
                            ));
                        }
                        if let ValueType::Enum { name } = &field.value_type
                            && enum_of(stored, name) != enum_of(incoming, name)
                        {
                            return Compatibility::Incompatible(format!(
                                "enum {name} used by {}.{} changed its values",
                                old.name, field.name
                            ));
                        }
                    }
                }
            }
            for field in &new.fields {
                if old.fields.iter().any(|f| f.name == field.name) {
                    continue;
                }
                if field.nullable || field.default.is_some() {
                    steps.push(AdditiveStep::AddField {
                        model: new.name.clone(),
                        field: field.name.clone(),
                    });
                } else {
                    return Compatibility::Incompatible(format!(
                        "field {}.{} is required and has no default",
                        new.name, field.name
                    ));
                }
            }
        }
        for new in &incoming.models {
            if !stored.models.iter().any(|m| m.name == new.name) {
                steps.push(AdditiveStep::AddModel(new.name.clone()));
            }
        }
        if steps.is_empty() {
            Compatibility::Identical
        } else {
            Compatibility::Additive(steps)
        }
    }
}
