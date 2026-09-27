//! Validate: declarations to a typed, validated schema. Every rule reachable
//! from source reports the offending declaration; the core descriptor check
//! near the end is a backstop at the end of the input. No descriptor is
//! assembled here: [`crate::generate`] renders [`Validated`].
use crate::parse::{ActionInputDecl, Declarations, FieldDecl, ModelDecl, Pos, SlotDecl, at};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// The validated schema: every declaration resolved and every rule applied,
/// in source order. Plain data with no JSON; [`crate::generate`] turns it into
/// the runtime descriptors and the generated code.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Validated {
    pub enums: Vec<Enum>,
    pub models: Vec<Model>,
    /// Every `@@unique`, in declaration order across models.
    pub unique_constraints: Vec<UniqueConstraint>,
    /// Model-typed fields without `@reference`, resolved to the reference they mirror.
    pub inverses: Vec<Inverse>,
    /// Every `@requires`, in field order across models.
    pub requirements: Vec<Requirement>,
    pub prerequisites: Vec<Prerequisite>,
    pub mutations: Vec<Mutation>,
    pub actions: Vec<Action>,
    /// Every `@deprecated`, in source order. A generated-code notice only
    /// ([#91](https://github.com/zanminwang/axton/issues/91)): Generate keeps
    /// it beside the descriptors, never inside them, so no runtime reads it.
    pub deprecations: Vec<Deprecation>,
}
/// One `@deprecated(reason: "…")`, on a field, an enum value or a mutation slot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Deprecation {
    EnumValue {
        enum_name: String,
        value: String,
        reason: Option<String>,
    },
    Field {
        model: String,
        field: String,
        reason: Option<String>,
    },
    Slot {
        mutation: String,
        slot: String,
        reason: Option<String>,
    },
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Enum {
    pub name: String,
    pub values: Vec<String>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Model {
    pub name: String,
    /// The read-contract version declared by `@@version(n)`; 1 when omitted.
    /// Parse enforces the range and uniqueness rules it shares with the
    /// mutation directive; the history compares versions across compiles.
    pub version: u64,
    pub identity: Vec<String>,
    /// Stored fields only; relation fields live in `relations`.
    pub fields: Vec<Field>,
    pub relations: Vec<Relation>,
    /// The field sets of this model's `@@unique` constraints.
    pub unique: Vec<Vec<String>>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub ty: FieldType,
    pub nullable: bool,
    /// Source `@default`: create-only policy, emitted as `createDefault`.
    pub create_default: Option<axton_core::CreateDefault>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FieldType {
    Scalar(Scalar),
    Enum(String),
    List(Box<FieldType>),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scalar {
    String,
    Boolean,
    Int,
    Float,
    Uuid,
    DateTime,
}
impl Scalar {
    /// The source spellings of a scalar type (`Bool` and `Boolean` are aliases).
    fn from_source(name: &str) -> Option<Self> {
        Some(match name {
            "String" => Self::String,
            "Bool" | "Boolean" => Self::Boolean,
            "Int" => Self::Int,
            "Float" => Self::Float,
            "UUID" => Self::Uuid,
            "DateTime" => Self::DateTime,
            _ => return None,
        })
    }
    /// The name the runtime descriptors use.
    pub fn descriptor_name(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Boolean => "boolean",
            Self::Int => "int",
            Self::Float => "float",
            Self::Uuid => "uuid",
            Self::DateTime => "dateTime",
        }
    }
}
/// A `@reference` field: `name` on the declaring model points at `target`
/// through `fields`, which mirror the target's identity `target_fields`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Relation {
    pub name: String,
    pub target: String,
    pub fields: Vec<String>,
    pub target_fields: Vec<String>,
    pub on_delete: OnDelete,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OnDelete {
    None,
    Delete,
}
impl OnDelete {
    pub fn descriptor_name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Delete => "delete",
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UniqueConstraint {
    pub model: String,
    pub fields: Vec<String>,
}
/// A field of `model` typed as `target` without `@reference`: the other side
/// of the relation `reference` declared on `target`, whose foreign key is `fields`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Inverse {
    pub model: String,
    pub name: String,
    pub target: String,
    pub list: bool,
    pub nullable: bool,
    /// The `@inverse(name)` argument that selected the reference, if given.
    pub relation_name: Option<String>,
    pub reference: String,
    pub fields: Vec<String>,
}
/// A `@requires(Prerequisite(field: self, …))` on `model.field`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Requirement {
    pub model: String,
    pub field: String,
    pub prerequisite: String,
    /// The prerequisite's field names in invocation order; each binds `self`,
    /// the only argument expression currently accepted.
    pub arguments: Vec<String>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Prerequisite {
    pub name: String,
    pub fields: Vec<PrerequisiteField>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrerequisiteField {
    pub name: String,
    /// The declared scalar name as written (`Bool` and `Boolean` stay distinct);
    /// the descriptors and the `@requires` type check use it verbatim.
    pub type_name: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Mutation {
    pub name: String,
    pub version: u64,
    pub slots: Vec<Slot>,
    pub sequence: Option<Sequence>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Action {
    pub name: String,
    /// `mutation` or `query`: the backend business contract, retained per version.
    pub kind: axton_core::CallKind,
    pub version: u64,
    pub inputs: Vec<ActionInput>,
    pub outputs: Vec<ActionOutput>,
    pub sequence: Option<Sequence>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActionInput {
    /// Nullable values still require the named argument to be present.
    Value {
        name: String,
        ty: FieldType,
        nullable: bool,
        list: bool,
    },
    Model {
        slot: Slot,
    },
}
/// Every output is explicit and supplied by the handler: a value, or the
/// identity of a Model record the server resolves through its Loader.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActionOutputSource {
    HandlerValue,
    HandlerModelIdentity,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActionOutputType {
    Value(FieldType),
    Model(String),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActionOutput {
    pub name: String,
    pub ty: ActionOutputType,
    pub cardinality: Cardinality,
    pub source: ActionOutputSource,
    pub model_read_version: Option<u64>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Slot {
    pub name: String,
    pub model: String,
    pub operation: Operation,
    pub cardinality: Cardinality,
    /// Always set for `update` slots: the declared `<fields>` or, without a
    /// restriction, every stored field outside the identity.
    pub allowed_patch_fields: Option<Vec<String>>,
    pub bindings: Vec<Binding>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operation {
    Create,
    Update,
    Delete,
}
impl Operation {
    pub fn descriptor_name(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Update => "update",
            Self::Delete => "delete",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cardinality {
    Single,
    Optional,
    List,
}
impl Cardinality {
    pub fn descriptor_name(self) -> &'static str {
        match self {
            Self::Single => "single",
            Self::Optional => "optional",
            Self::List => "list",
        }
    }
}
/// `(relation: slot)` on a slot: the slot's `relation` foreign key `fields`
/// take the identity of the record created in the parent `slot`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Binding {
    pub relation: String,
    pub slot: String,
    pub fields: Vec<String>,
}
/// `@@sequence(after: [Mutation(slot: path), …])`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Sequence {
    pub after: Vec<SequenceCall>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SequenceCall {
    pub mutation: String,
    pub bindings: Vec<SequenceBinding>,
}
/// The target mutation's `slot` receives the record at `path`: a slot of the
/// declaring mutation followed by relation names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SequenceBinding {
    pub slot: String,
    pub path: Vec<String>,
}

fn is_model<'a>(d: &'a Declarations, name: &str) -> Option<&'a ModelDecl> {
    d.models.iter().find(|m| m.name == name)
}
fn field<'a>(m: &'a ModelDecl, name: &str) -> Option<&'a FieldDecl> {
    m.fields.iter().find(|f| f.name == name)
}
fn strings(values: &[Value]) -> Vec<String> {
    values
        .iter()
        .map(|v| v.as_str().unwrap_or_default().to_string())
        .collect()
}
/// The declaration keyword's name in diagnostics.
fn kind_label(kind: axton_core::CallKind) -> &'static str {
    match kind {
        axton_core::CallKind::Mutation => "Mutation",
        axton_core::CallKind::Query => "Query",
    }
}
/// `@default` belongs to stored Model fields; operation inputs and outputs
/// are complete values supplied by the caller or handler.
fn reject_operation_default(f: &FieldDecl) -> Result<(), String> {
    match &f.default {
        Some(default) => Err(at(
            default.pos,
            format!(
                "@default is a Model field attribute; unsupported on operation field {}",
                f.name
            ),
        )),
        None => Ok(()),
    }
}
/// Resolve a field's `@default` against its validated type. Literals are
/// normalized by the same rules the runtime applies to the field's values;
/// functions are recorded, never evaluated.
fn create_default(
    f: &FieldDecl,
    ty: &FieldType,
    enums: &[Enum],
) -> Result<Option<axton_core::CreateDefault>, String> {
    use crate::parse::DefaultExpr;
    use axton_core::CreateDefault;
    let Some(default) = &f.default else {
        return Ok(None);
    };
    let fail = |msg: String| Err(at(default.pos, msg));
    if matches!(ty, FieldType::List(_)) {
        return fail(format!("@default is unsupported on list field {}", f.name));
    }
    let literal = match &default.expr {
        DefaultExpr::Call { name, arguments } => {
            let generator = match name.as_str() {
                "uuid" => CreateDefault::Uuid,
                "now" => CreateDefault::Now,
                _ => {
                    return fail(format!(
                        "unknown default function {name}(); supported: uuid(), now()"
                    ));
                }
            };
            if *arguments != 0 {
                return fail(format!("{name}() takes no arguments"));
            }
            let fits = matches!(
                (&generator, ty),
                (
                    CreateDefault::Uuid,
                    FieldType::Scalar(Scalar::String | Scalar::Uuid)
                ) | (CreateDefault::Now, FieldType::Scalar(Scalar::DateTime))
            );
            if !fits {
                return fail(match generator {
                    CreateDefault::Uuid => {
                        format!("uuid() requires a String or UUID field; {} is not", f.name)
                    }
                    _ => format!("now() requires a DateTime field; {} is not", f.name),
                });
            }
            return Ok(Some(generator));
        }
        DefaultExpr::Identifier(name) if name == "null" => {
            return fail(format!("@default(null) is unsupported on {}", f.name));
        }
        DefaultExpr::Identifier(name) => match ty {
            FieldType::Scalar(Scalar::Boolean) if name == "true" || name == "false" => {
                Value::Bool(name == "true")
            }
            FieldType::Enum(_) => Value::String(name.clone()),
            _ => return fail(format!("invalid default for {}: {name}", f.name)),
        },
        DefaultExpr::String(text) => match ty {
            FieldType::Enum(_) => {
                return fail(format!(
                    "invalid default for {}: an enum default is a member name, not a string",
                    f.name
                ));
            }
            _ => Value::String(text.clone()),
        },
        DefaultExpr::Number(text) => serde_json::from_str(text)
            .map_err(|_| at(default.pos, format!("invalid default number {text}")))?,
    };
    let schema = axton_core::Schema {
        enums: enums
            .iter()
            .map(|e| axton_core::EnumDescriptor {
                name: e.name.clone(),
                values: e.values.clone(),
            })
            .collect(),
        models: vec![],
        actions: vec![],
        result_models: vec![],
        requirements: vec![],
        prerequisites: vec![],
        client_policies: vec![],
    };
    let descriptor = axton_core::FieldDescriptor {
        name: f.name.clone(),
        value_type: serde_json::from_value(crate::generate::field_type(ty))
            .map_err(|e| e.to_string())?,
        nullable: false,
        default: None,
        create_default: None,
    };
    // Strings are only literals of text-backed scalars; numbers only of numeric ones.
    let kind_fits = match (&literal, ty) {
        (Value::String(_), FieldType::Scalar(Scalar::Int | Scalar::Float | Scalar::Boolean)) => {
            false
        }
        (Value::Number(_), FieldType::Scalar(Scalar::Int | Scalar::Float)) => true,
        (Value::Number(_), _) => false,
        _ => true,
    };
    match kind_fits
        .then(|| schema.normalize_value(&descriptor, &literal).ok())
        .flatten()
    {
        Some(value) => Ok(Some(CreateDefault::Literal { value })),
        None => fail(format!("invalid default for {}: {literal}", f.name)),
    }
}
fn action_value_type(f: &FieldDecl, enums: &[Enum], label: &str) -> Result<FieldType, String> {
    reject_operation_default(f)?;
    if !f.attributes.is_empty() || f.deprecated.is_some() {
        return Err(at(
            f.pos,
            format!("unsupported {label} value directive on {}", f.name),
        ));
    }
    if let Some(scalar) = Scalar::from_source(&f.type_name) {
        Ok(FieldType::Scalar(scalar))
    } else if enums.iter().any(|e| e.name == f.type_name) {
        Ok(FieldType::Enum(f.type_name.clone()))
    } else {
        Err(at(
            f.pos,
            format!(
                "unknown or unsupported {label} type {} on {}",
                f.type_name, f.name
            ),
        ))
    }
}
fn cardinality(s: &str) -> Cardinality {
    match s {
        "list" => Cardinality::List,
        "optional" => Cardinality::Optional,
        _ => Cardinality::Single,
    }
}
fn validate_action_slot(
    s: &SlotDecl,
    inputs: &[ActionInputDecl],
    models: &[Model],
) -> Result<Slot, String> {
    if s.deprecated.is_some() {
        return Err(at(
            s.pos,
            format!(
                "unsupported @deprecated on Mutation Model operand {}",
                s.name
            ),
        ));
    }
    let model = models.iter().find(|m| m.name == s.model).ok_or_else(|| {
        at(
            s.pos,
            format!("unknown Mutation model {} on {}", s.model, s.name),
        )
    })?;
    let operation = match s.operation.as_str() {
        "create" => Operation::Create,
        "update" => Operation::Update,
        "delete" => Operation::Delete,
        _ => return Err(at(s.pos, format!("unknown operation on {}", s.name))),
    };
    let mut allowed_patch_fields = s.allowed_patch_fields.clone();
    if operation == Operation::Update {
        if allowed_patch_fields.is_none() {
            allowed_patch_fields = Some(
                model
                    .fields
                    .iter()
                    .filter(|f| !model.identity.contains(&f.name))
                    .map(|f| f.name.clone())
                    .collect(),
            );
        }
        let mut seen = BTreeSet::new();
        for name in allowed_patch_fields.as_ref().unwrap() {
            if !seen.insert(name.as_str())
                || model.identity.contains(name)
                || !model.fields.iter().any(|f| f.name == *name)
            {
                return Err(at(
                    s.pos,
                    format!("invalid allowed patch field {name} on {}", s.name),
                ));
            }
        }
    } else if allowed_patch_fields.is_some() {
        return Err(at(
            s.pos,
            format!("field restriction requires update on {}", s.name),
        ));
    }
    let mut bindings = vec![];
    for (relation, parent) in s
        .relation_bindings
        .as_object()
        .ok_or_else(|| at(s.pos, format!("invalid bindings on {}", s.name)))?
    {
        let rel = model
            .relations
            .iter()
            .find(|r| r.name == *relation)
            .ok_or_else(|| {
                at(
                    s.pos,
                    format!("unknown binding relation {relation} on {}", s.name),
                )
            })?;
        let parent_name = parent
            .as_str()
            .ok_or_else(|| at(s.pos, format!("invalid binding parent on {}", s.name)))?;
        let parent_slot = inputs
            .iter()
            .find_map(|i| match i {
                ActionInputDecl::Model(x) if x.name == parent_name => Some(x),
                _ => None,
            })
            .ok_or_else(|| {
                at(
                    s.pos,
                    format!("unknown parent slot {parent_name} on {}", s.name),
                )
            })?;
        if parent_slot.model != rel.target || parent_slot.cardinality != "single" {
            return Err(at(
                s.pos,
                format!(
                    "binding parent {parent_name} must be single matching model on {}",
                    s.name
                ),
            ));
        }
        bindings.push(Binding {
            relation: relation.clone(),
            slot: parent_name.into(),
            fields: rel.fields.clone(),
        });
    }
    Ok(Slot {
        name: s.name.clone(),
        model: s.model.clone(),
        operation,
        cardinality: cardinality(&s.cardinality),
        allowed_patch_fields,
        bindings,
    })
}

/// Check the declarations and resolve them into the typed schema.
/// Top-level identifiers the generated TypeScript and Dart clients, or the
/// runtime packages they import, declare. A model or enum with one of these
/// names would collide with them in the generated file.
const GENERATED_NAMES: &[&str] = &[
    "BootstrapError",
    "BootstrapFailedException",
    "BootstrapPhase",
    "BootstrapStatus",
    "Call",
    "CallError",
    "CallFailure",
    "CallOptions",
    "CallOutcome",
    "CallPort",
    "CallRejected",
    "CallStatus",
    "CallStore",
    "CallSuccess",
    "Channels",
    "Client",
    "ClientClosedException",
    "ClientSyncState",
    "Connection",
    "DirectMutations",
    "GeneratedClient",
    "GeneratedTransaction",
    "LiveModels",
    "LivePort",
    "Mutate",
    "MutatePort",
    "MutationHandlerCall",
    "MutationHandlers",
    "MutationName",
    "Mutations",
    "PendingMutation",
    "Present",
    "Queries",
    "QueryHandlerCall",
    "QueryHandlers",
    "QueuedQueries",
    "ReadPort",
    "RebuildReport",
    "Rejection",
    "RuntimeConnection",
    "Scopes",
    "Subscription",
    "SubscriptionClosedException",
    "SubscriptionConnection",
    "SubscriptionInitialization",
    "SubscriptionStatus",
    "SyncServer",
    "SyncState",
    "Transaction",
    "TxModels",
    "WritePort",
];

/// Top-level type names the generated TypeScript backend declares for handler
/// calls, Channel membership and touch. A model or enum with one of these names
/// would collide with them in the generated backend file.
const GENERATED_BACKEND_NAMES: &[&str] = &[
    "Channel",
    "HandlerCall",
    "ModelMembership",
    "MutationContext",
    "QueryContext",
    "RecordRef",
    "Touch",
    "TransactionCall",
];

pub fn validate(d: &Declarations) -> Result<Validated, String> {
    let eof = d.end;
    let enums: Vec<Enum> = d
        .enums
        .iter()
        .map(|e| Enum {
            name: e.name.clone(),
            values: e.values.clone(),
        })
        .collect();
    let mut unique_constraints = vec![];
    let mut constraint_pos: Vec<Pos> = vec![];
    for m in &d.models {
        for u in &m.unique {
            unique_constraints.push(UniqueConstraint {
                model: m.name.clone(),
                fields: u.fields.clone(),
            });
            constraint_pos.push(u.pos);
        }
    }
    // Enum and model names in declaration order, for duplicate detection.
    let mut declared_names: Vec<(&str, Pos)> = d
        .enums
        .iter()
        .map(|e| (e.name.as_str(), e.pos))
        .chain(d.models.iter().map(|m| (m.name.as_str(), m.pos)))
        .collect();
    declared_names.sort_by_key(|(_, pos)| (pos.line, pos.col));
    // Descriptor rules that core also enforces, checked here first so the
    // diagnostic names the declaration. Core remains the authority at load time.
    let mut seen_names = BTreeSet::new();
    for (name, pos) in &declared_names {
        if !seen_names.insert(*name) {
            return Err(at(*pos, format!("duplicate declaration {name}")));
        }
        if GENERATED_NAMES.contains(name) {
            return Err(at(
                *pos,
                format!("{name} is a name the generated client uses; choose another"),
            ));
        }
        if GENERATED_BACKEND_NAMES.contains(name) {
            return Err(at(
                *pos,
                format!("{name} is a type name the generated backend declares; choose another"),
            ));
        }
    }
    for m in &d.models {
        if axton_core::reserved_model_name(&m.name) {
            return Err(at(
                m.pos,
                format!(
                    "model name {} uses a reserved prefix (axton_, sqlite_)",
                    m.name
                ),
            ));
        }
        let mut seen_fields = BTreeSet::new();
        for f in &m.fields {
            if !seen_fields.insert(f.name.as_str()) {
                return Err(at(f.pos, "duplicate field"));
            }
            if f.list && f.nullable {
                return Err(at(f.pos, "lists cannot be nullable"));
            }
        }
        if m.identity.is_empty() {
            return Err(at(m.pos, "model requires an @@id identity"));
        }
        let mut seen_identity = BTreeSet::new();
        for id in &m.identity {
            let f =
                field(m, id).ok_or_else(|| at(m.pos, format!("identity field {id} missing")))?;
            if !seen_identity.insert(id.as_str()) {
                return Err(at(m.pos, format!("duplicate identity field {id}")));
            }
            if f.nullable || f.list {
                return Err(at(f.pos, "identity fields must be non-nullable scalars"));
            }
        }
    }
    // The generated backend addresses a Model by its lower-first accessor in
    // `touch` and in every Channel handle, beside the Channel's own `add` and
    // `remove` for mixed record lists ([#140](https://github.com/zanminwang/axton/issues/140)).
    let mut accessors: BTreeMap<String, &str> = BTreeMap::new();
    for m in &d.models {
        let accessor = format!("{}{}", m.name[..1].to_ascii_lowercase(), &m.name[1..]);
        if accessor == "add" || accessor == "remove" {
            return Err(at(
                m.pos,
                format!(
                    "model {} generates the accessor {accessor}, which a Channel reserves for mixed record lists; rename the model",
                    m.name
                ),
            ));
        }
        if let Some(other) = accessors.insert(accessor.clone(), &m.name) {
            return Err(at(
                m.pos,
                format!(
                    "models {other} and {} both generate the accessor {accessor}; rename one",
                    m.name
                ),
            ));
        }
    }
    // Structure: relations, inverses, requirements and field types per model.
    let mut models: Vec<Model> = vec![];
    // Inverse candidates: (declaring model, field, position).
    let mut inverse_fields: Vec<(&ModelDecl, &FieldDecl)> = vec![];
    // Requirement candidates: (declaring model, field, `@requires` arguments).
    let mut requirement_fields: Vec<(&ModelDecl, &FieldDecl, &Value)> = vec![];
    for m in &d.models {
        let mut relations = vec![];
        let mut stored: Vec<&FieldDecl> = vec![];
        for f in &m.fields {
            if let Some(req) = f.attributes.get("requires") {
                requirement_fields.push((m, f, req));
            }
            if let Some(target) = is_model(d, &f.type_name) {
                if let Some(default) = &f.default {
                    return Err(at(
                        default.pos,
                        format!("@default is unsupported on relation field {}", f.name),
                    ));
                }
                if let Some(reference) = f.attributes.get("reference") {
                    if f.list {
                        return Err(at(f.pos, "reference must be singular"));
                    }
                    if reference
                        .as_object()
                        .unwrap()
                        .keys()
                        .any(|k| !["0", "via", "onTargetDelete"].contains(&k.as_str()))
                    {
                        return Err(at(f.pos, "unknown reference argument"));
                    }
                    let via = reference["via"]
                        .as_array()
                        .ok_or_else(|| at(f.pos, "reference requires via fields"))?;
                    if via.len() != target.identity.len() {
                        return Err(at(f.pos, "reference identity arity mismatch"));
                    }
                    for (local, remote) in via.iter().zip(&target.identity) {
                        let lf = m
                            .fields
                            .iter()
                            .find(|x| x.name == *local)
                            .ok_or_else(|| at(f.pos, "unknown reference field"))?;
                        let rf = field(target, remote)
                            .ok_or_else(|| at(f.pos, "unknown target identity"))?;
                        if lf.type_name != rf.type_name || lf.list {
                            return Err(at(f.pos, "reference field type mismatch"));
                        }
                    }
                    let on_delete = match reference.get("onTargetDelete") {
                        None => OnDelete::None,
                        Some(v) if v == "none" => OnDelete::None,
                        Some(v) if v == "delete" => OnDelete::Delete,
                        Some(_) => return Err(at(f.pos, "unsupported onTargetDelete")),
                    };
                    relations.push(Relation {
                        name: f.name.clone(),
                        target: target.name.clone(),
                        fields: strings(via),
                        target_fields: target.identity.clone(),
                        on_delete,
                    });
                } else {
                    inverse_fields.push((m, f));
                }
                continue;
            }
            if f.attributes.contains_key("reference") || f.attributes.contains_key("inverse") {
                return Err(at(f.pos, "relation directive requires model type"));
            }
            stored.push(f);
        }
        let mut fields = vec![];
        for f in &stored {
            let mut ty = if let Some(s) = Scalar::from_source(&f.type_name) {
                FieldType::Scalar(s)
            } else if enums.iter().any(|e| e.name == f.type_name) {
                FieldType::Enum(f.type_name.clone())
            } else {
                return Err(at(
                    f.pos,
                    format!("unknown or unsupported field type {}", f.type_name),
                ));
            };
            if f.list {
                ty = FieldType::List(Box::new(ty));
            }
            let create_default = create_default(f, &ty, &enums)?;
            fields.push(Field {
                name: f.name.clone(),
                ty,
                nullable: f.nullable,
                create_default,
            });
        }
        for (f, decl) in fields.iter().zip(&stored) {
            if m.identity.contains(&f.name) && !matches!(f.ty, FieldType::Scalar(_)) {
                return Err(at(decl.pos, "identity fields must be non-nullable scalars"));
            }
        }
        models.push(Model {
            name: m.name.clone(),
            version: m.version,
            identity: m.identity.clone(),
            fields,
            relations,
            unique: m.unique.iter().map(|u| u.fields.clone()).collect(),
        });
    }
    let model = |name: &str| models.iter().find(|m| m.name == name);
    // Mutations: slot bindings first, then default patch fields.
    let mut mutations: Vec<Mutation> = vec![];
    for m in &d.mutations {
        let mut slots = vec![];
        for s in &m.slots {
            let mut bindings = vec![];
            for (relation, parent) in s.relation_bindings.as_object().unwrap() {
                let model = model(&s.model).ok_or_else(|| at(s.pos, "unknown bound model"))?;
                let rel = model
                    .relations
                    .iter()
                    .find(|r| r.name == *relation)
                    .ok_or_else(|| at(s.pos, "unknown binding relation"))?;
                let parent_slot = m
                    .slots
                    .iter()
                    .find(|x| parent.as_str() == Some(x.name.as_str()))
                    .ok_or_else(|| at(s.pos, "unknown parent slot"))?;
                if parent_slot.model != rel.target || parent_slot.cardinality != "single" {
                    return Err(at(s.pos, "binding parent must be single matching model"));
                }
                bindings.push(Binding {
                    relation: relation.clone(),
                    slot: parent_slot.name.clone(),
                    fields: rel.fields.clone(),
                });
            }
            let operation = match s.operation.as_str() {
                "create" => Operation::Create,
                "update" => Operation::Update,
                _ => Operation::Delete,
            };
            let cardinality = match s.cardinality.as_str() {
                "list" => Cardinality::List,
                "optional" => Cardinality::Optional,
                _ => Cardinality::Single,
            };
            slots.push(Slot {
                name: s.name.clone(),
                model: s.model.clone(),
                operation,
                cardinality,
                allowed_patch_fields: s.allowed_patch_fields.clone(),
                bindings,
            });
        }
        mutations.push(Mutation {
            name: m.name.clone(),
            version: m.version,
            slots,
            sequence: None,
        });
    }
    for (m, decl) in mutations.iter_mut().zip(&d.mutations) {
        for (slot, sdecl) in m.slots.iter_mut().zip(&decl.slots) {
            if slot.operation == Operation::Update && slot.allowed_patch_fields.is_none() {
                let model =
                    model(&slot.model).ok_or_else(|| at(sdecl.pos, "unknown mutation model"))?;
                slot.allowed_patch_fields = Some(
                    model
                        .fields
                        .iter()
                        .filter(|f| !model.identity.contains(&f.name))
                        .map(|f| f.name.clone())
                        .collect(),
                );
            }
        }
    }
    let mut inverses = vec![];
    for (m, f) in inverse_fields {
        let target = is_model(d, &f.type_name).unwrap();
        let relation_name = f
            .attributes
            .get("inverse")
            .map(|v| v["0"].clone())
            .unwrap_or(Value::Null);
        let candidates: Vec<_> = target
            .fields
            .iter()
            .filter(|x| {
                x.type_name == m.name
                    && x.attributes.contains_key("reference")
                    && (relation_name.is_null() || x.attributes["reference"]["0"] == relation_name)
            })
            .collect();
        if candidates.len() != 1 {
            return Err(at(
                f.pos,
                "inverse must resolve to exactly one reference; use a shared relation name",
            ));
        }
        let reference = candidates[0];
        let fields = strings(reference.attributes["reference"]["via"].as_array().unwrap());
        if !f.list {
            let same = |values: &Vec<String>| {
                values.len() == fields.len() && fields.iter().all(|f| values.contains(f))
            };
            let normalized = model(&target.name).unwrap();
            if !same(&normalized.identity) && !normalized.unique.iter().any(same) {
                return Err(at(
                    f.pos,
                    "singular inverse requires unique reference fields",
                ));
            }
        }
        let relation_name = match relation_name {
            Value::Null => None,
            Value::String(name) => Some(name),
            _ => return Err(at(f.pos, "inverse relation name must be an identifier")),
        };
        inverses.push(Inverse {
            model: m.name.clone(),
            name: f.name.clone(),
            target: target.name.clone(),
            list: f.list,
            nullable: f.nullable,
            relation_name,
            reference: reference.name.clone(),
            fields,
        });
    }
    let prerequisites: Vec<Prerequisite> = d
        .prerequisites
        .iter()
        .map(|p| Prerequisite {
            name: p.name.clone(),
            fields: p
                .fields
                .iter()
                .map(|f| PrerequisiteField {
                    name: f.name.clone(),
                    type_name: f.type_name.clone(),
                })
                .collect(),
        })
        .collect();
    let mut prerequisite_names = BTreeSet::new();
    for p in &d.prerequisites {
        if !prerequisite_names.insert(p.name.as_str()) {
            return Err(at(p.pos, "duplicate prerequisite"));
        }
        let mut fields = BTreeSet::new();
        for f in &p.fields {
            if !fields.insert(f.name.as_str())
                || ![
                    "String", "UUID", "DateTime", "Int", "Float", "Bool", "Boolean",
                ]
                .contains(&f.type_name.as_str())
            {
                return Err(at(f.pos, "invalid prerequisite field"));
            }
        }
    }
    let mut requirements = vec![];
    for (m, f, args) in requirement_fields {
        let rpos = f.pos;
        let args = args.as_object().unwrap();
        if args.len() != 1 || !args.contains_key("0") {
            return Err(at(rpos, "requires expects one invocation"));
        }
        let invocation = &args["0"];
        let declaration = prerequisites
            .iter()
            .find(|p| invocation["name"].as_str() == Some(p.name.as_str()))
            .ok_or_else(|| at(rpos, "unknown prerequisite"))?;
        let arguments = invocation["arguments"]
            .as_object()
            .ok_or_else(|| at(rpos, "requires expects invocation arguments"))?;
        if arguments.len() != declaration.fields.len() {
            return Err(at(rpos, "prerequisite argument mismatch"));
        }
        for field in &declaration.fields {
            let expression = arguments
                .get(&field.name)
                .ok_or_else(|| at(rpos, "missing prerequisite argument"))?;
            if expression != "self" {
                return Err(at(rpos, "prerequisite argument currently requires self"));
            }
            if f.type_name != field.type_name {
                return Err(at(rpos, "prerequisite argument type mismatch"));
            }
        }
        requirements.push(Requirement {
            model: m.name.clone(),
            field: f.name.clone(),
            prerequisite: declaration.name.clone(),
            arguments: arguments.keys().cloned().collect(),
        });
    }
    let mut sequences: Vec<Option<Sequence>> = vec![];
    for (m, decl) in mutations.iter().zip(&d.mutations) {
        let Some(s) = &decl.sequence else {
            sequences.push(None);
            continue;
        };
        let qpos = s.pos;
        let sequence = s.arguments.as_object().unwrap();
        if sequence.len() != 1 || !sequence.contains_key("after") {
            return Err(at(qpos, "sequence requires after"));
        }
        let after = sequence["after"]
            .as_array()
            .ok_or_else(|| at(qpos, "sequence after must be list"))?;
        let mut calls = vec![];
        for call in after {
            let target = mutations
                .iter()
                .find(|x| call["name"].as_str() == Some(x.name.as_str()))
                .ok_or_else(|| at(qpos, "unknown sequence mutation"))?;
            let args = call["arguments"]
                .as_object()
                .ok_or_else(|| at(qpos, "sequence requires invocation"))?;
            let mut bindings = vec![];
            for (slot, expression) in args {
                let target_slot = target
                    .slots
                    .iter()
                    .find(|x| x.name == *slot)
                    .ok_or_else(|| at(qpos, "unknown sequence target slot"))?;
                let path: Vec<String> = expression
                    .as_str()
                    .ok_or_else(|| at(qpos, "sequence requires slot path"))?
                    .split('.')
                    .map(str::to_string)
                    .collect();
                let source_slot = m
                    .slots
                    .iter()
                    .find(|x| x.name == path[0])
                    .ok_or_else(|| at(qpos, "unknown sequence source slot"))?;
                let source_pos = decl.slots[m
                    .slots
                    .iter()
                    .position(|x| x.name == source_slot.name)
                    .unwrap()]
                .pos;
                let mut current = model(&source_slot.model)
                    .ok_or_else(|| at(source_pos, "unknown mutation model"))?;
                for part in &path[1..] {
                    let relation = current
                        .relations
                        .iter()
                        .find(|r| r.name == *part)
                        .ok_or_else(|| at(qpos, "unknown sequence relation path"))?;
                    current = model(&relation.target).unwrap();
                }
                if current.name != target_slot.model {
                    return Err(at(qpos, "sequence target model mismatch"));
                }
                bindings.push(SequenceBinding {
                    slot: slot.clone(),
                    path,
                });
            }
            calls.push(SequenceCall {
                mutation: target.name.clone(),
                bindings,
            });
        }
        sequences.push(Some(Sequence { after: calls }));
    }
    for (m, sequence) in mutations.iter_mut().zip(sequences) {
        m.sequence = sequence;
    }
    let mut actions = Vec::new();
    let mut action_names = BTreeSet::new();
    for decl in &d.actions {
        let label = kind_label(decl.kind);
        // `mutations.call` and `queries.enqueue` select the other delivery
        // route and `queries.invalidate` discards saved once results, so an
        // operation of that kind cannot take the member name.
        let reserved: &[&str] = match decl.kind {
            axton_core::CallKind::Mutation => &["call"],
            axton_core::CallKind::Query => &["enqueue", "invalidate"],
        };
        if reserved.iter().any(|r| decl.name.eq_ignore_ascii_case(r)) {
            return Err(at(
                decl.pos,
                format!("{label} name {} is reserved", decl.name),
            ));
        }
        // Generated Dart route classes hold `client` and inherit `Object`
        // members; an operation method cannot reuse those names.
        let method = format!("{}{}", decl.name[..1].to_ascii_lowercase(), &decl.name[1..]);
        if [
            "client",
            "toString",
            "hashCode",
            "runtimeType",
            "noSuchMethod",
        ]
        .contains(&method.as_str())
        {
            return Err(at(
                decl.pos,
                format!(
                    "{label} name {} is reserved: its method {method} would collide with a member of the generated client",
                    decl.name
                ),
            ));
        }
        if declared_names.iter().any(|(name, _)| *name == decl.name) {
            return Err(at(
                decl.pos,
                format!("{label} {} collides with a model or enum", decl.name),
            ));
        }
        // Mutations and Queries share one operation namespace.
        let generated_name = format!("{}{}", decl.name[..1].to_ascii_lowercase(), &decl.name[1..]);
        if !action_names.insert(generated_name) {
            return Err(at(
                decl.pos,
                format!(
                    "duplicate operation {}: a name is declared once across mutation and query",
                    decl.name
                ),
            ));
        }
        if decl.kind == axton_core::CallKind::Query {
            if let Some(s) = &decl.sequence {
                return Err(at(
                    s.pos,
                    format!("Query {} cannot declare @sequence", decl.name),
                ));
            }
            if let Some(ActionInputDecl::Model(s)) = decl
                .inputs
                .iter()
                .find(|input| matches!(input, ActionInputDecl::Model(_)))
            {
                return Err(at(
                    s.pos,
                    format!(
                        "Query {} cannot take Model operand {}; Model operands belong to mutations",
                        decl.name, s.name
                    ),
                ));
            }
        }
        let mut inputs = Vec::new();
        let mut outputs = Vec::new();
        let mut input_names = BTreeSet::new();
        let mut output_names = BTreeSet::new();
        for input in &decl.inputs {
            let (name, pos) = match input {
                ActionInputDecl::Value(f) => (&f.name, f.pos),
                ActionInputDecl::Model(s) => (&s.name, s.pos),
            };
            if !input_names.insert(name.as_str()) {
                return Err(at(pos, format!("duplicate {label} input {name}")));
            }
            match input {
                ActionInputDecl::Value(f) => {
                    let ty = action_value_type(f, &enums, label)?;
                    inputs.push(ActionInput::Value {
                        name: f.name.clone(),
                        ty,
                        nullable: f.nullable,
                        list: f.list,
                    });
                }
                // A Model operand declares a mutation target, never a
                // result: outputs are explicit and form their own namespace.
                ActionInputDecl::Model(s) => {
                    let slot = validate_action_slot(s, &decl.inputs, &models)?;
                    inputs.push(ActionInput::Model { slot });
                }
            }
        }
        for output in &decl.outputs {
            let f = &output.field;
            if !output_names.insert(f.name.as_str()) {
                return Err(at(f.pos, format!("duplicate {label} output {}", f.name)));
            }
            reject_operation_default(f)?;
            let (ty, source, read_version) = if let Some(model) = model(&f.type_name) {
                if !f.attributes.is_empty() || f.deprecated.is_some() {
                    return Err(at(
                        f.pos,
                        format!("unsupported {label} output directive on {}", f.name),
                    ));
                }
                (
                    ActionOutputType::Model(model.name.clone()),
                    ActionOutputSource::HandlerModelIdentity,
                    Some(model.version),
                )
            } else {
                (
                    ActionOutputType::Value(action_value_type(f, &enums, label)?),
                    ActionOutputSource::HandlerValue,
                    None,
                )
            };
            outputs.push(ActionOutput {
                name: f.name.clone(),
                ty,
                cardinality: if f.list {
                    Cardinality::List
                } else if f.nullable {
                    Cardinality::Optional
                } else {
                    Cardinality::Single
                },
                source,
                model_read_version: read_version,
            });
        }
        actions.push(Action {
            name: decl.name.clone(),
            kind: decl.kind,
            version: decl.version,
            inputs,
            outputs,
            sequence: None,
        });
    }
    for (i, decl) in d.actions.iter().enumerate() {
        let Some(s) = &decl.sequence else { continue };
        let qpos = s.pos;
        let sequence = s
            .arguments
            .as_object()
            .ok_or_else(|| at(qpos, "sequence requires after"))?;
        if sequence.len() != 1 || !sequence.contains_key("after") {
            return Err(at(
                qpos,
                format!("sequence requires after on {}", decl.name),
            ));
        }
        let after = sequence["after"]
            .as_array()
            .ok_or_else(|| at(qpos, "sequence after must be list"))?;
        let mut calls = vec![];
        for call in after {
            let target_name = call["name"].as_str().unwrap_or_default();
            let target = actions
                .iter()
                .find(|a| a.name == target_name && a.kind == axton_core::CallKind::Mutation)
                .ok_or_else(|| {
                    at(
                        qpos,
                        format!("unknown sequence Mutation {target_name} on {}", decl.name),
                    )
                })?;
            let args = call["arguments"]
                .as_object()
                .ok_or_else(|| at(qpos, "sequence requires invocation"))?;
            let mut bindings = vec![];
            for (slot_name, expression) in args {
                let target_slot = target
                    .inputs
                    .iter()
                    .find_map(|input| match input {
                        ActionInput::Model { slot } if slot.name == *slot_name => Some(slot),
                        _ => None,
                    })
                    .ok_or_else(|| at(qpos, format!("unknown sequence target slot {slot_name}")))?;
                let path: Vec<String> = expression
                    .as_str()
                    .ok_or_else(|| at(qpos, "sequence requires slot path"))?
                    .split('.')
                    .map(str::to_string)
                    .collect();
                let source_slot = actions[i]
                    .inputs
                    .iter()
                    .find_map(|input| match input {
                        ActionInput::Model { slot } if slot.name == path[0] => Some(slot),
                        _ => None,
                    })
                    .ok_or_else(|| at(qpos, format!("unknown sequence source slot {}", path[0])))?;
                let mut current = model(&source_slot.model).unwrap();
                for part in &path[1..] {
                    let relation = current
                        .relations
                        .iter()
                        .find(|r| r.name == *part)
                        .ok_or_else(|| {
                            at(qpos, format!("unknown sequence relation path {part}"))
                        })?;
                    current = model(&relation.target).unwrap();
                }
                if current.name != target_slot.model {
                    return Err(at(
                        qpos,
                        format!("sequence target model mismatch for {slot_name}"),
                    ));
                }
                bindings.push(SequenceBinding {
                    slot: slot_name.clone(),
                    path,
                });
            }
            calls.push(SequenceCall {
                mutation: target.name.clone(),
                bindings,
            });
        }
        actions[i].sequence = Some(Sequence { after: calls });
    }
    for (c, pos) in unique_constraints.iter().zip(&constraint_pos) {
        let m = model(&c.model).unwrap();
        if c.fields.is_empty()
            || c.fields
                .iter()
                .any(|f| !m.fields.iter().any(|x| x.name == *f))
        {
            return Err(at(*pos, "invalid unique fields"));
        }
    }
    let validated = Validated {
        enums,
        models,
        unique_constraints,
        inverses,
        requirements,
        prerequisites,
        mutations,
        actions,
        deprecations: {
            let mut list = vec![];
            for e in &d.enums {
                for (value, reason) in &e.deprecated {
                    list.push(Deprecation::EnumValue {
                        enum_name: e.name.clone(),
                        value: value.clone(),
                        reason: reason.clone(),
                    });
                }
            }
            for m in &d.models {
                for f in &m.fields {
                    if let Some(reason) = &f.deprecated {
                        list.push(Deprecation::Field {
                            model: m.name.clone(),
                            field: f.name.clone(),
                            reason: reason.clone(),
                        });
                    }
                }
            }
            for m in &d.mutations {
                for s in &m.slots {
                    if let Some(reason) = &s.deprecated {
                        list.push(Deprecation::Slot {
                            mutation: m.name.clone(),
                            slot: s.name.clone(),
                            reason: reason.clone(),
                        });
                    }
                }
            }
            list
        },
    };
    // Backstop: core validates the client descriptor Generate renders. Rules
    // reachable from source are checked above with a location; anything left
    // reports the end of the input.
    axton_core::Schema::from_value(crate::generate::schema(&validated))
        .map_err(|e| at(eof, e.to_string()))?;
    let mut seen = BTreeSet::new();
    for (m, decl) in validated.mutations.iter().zip(&d.mutations) {
        if !seen.insert(m.name.as_str()) {
            return Err(at(decl.pos, "duplicate mutation"));
        }
        let mut slots = BTreeSet::new();
        if m.slots.is_empty() {
            return Err(at(decl.pos, "mutation requires slots"));
        }
        for (slot, sdecl) in m.slots.iter().zip(&decl.slots) {
            let spos = sdecl.pos;
            if !slots.insert(slot.name.as_str()) {
                return Err(at(spos, "duplicate slot"));
            }
            let model = validated
                .models
                .iter()
                .find(|x| x.name == slot.model)
                .ok_or_else(|| at(spos, "unknown mutation model"))?;
            if let Some(fields) = &slot.allowed_patch_fields {
                let mut seen = BTreeSet::new();
                for f in fields {
                    if !seen.insert(f.as_str())
                        || model.identity.contains(f)
                        || !model.fields.iter().any(|x| x.name == *f)
                    {
                        return Err(at(spos, "invalid allowed patch field"));
                    }
                }
            }
        }
        // Operations carry no slot name; the decoders match `(model, op)` in
        // slot order and a non-single slot takes what it can. A later slot of
        // the same kind, with no single slot fixing a position in between, is
        // unreachable or takes an operation meant for the earlier one
        // ([#54](https://github.com/zanminwang/axton/issues/54)).
        for j in 1..m.slots.len() {
            let later = &m.slots[j];
            for i in (0..j).rev() {
                let earlier = &m.slots[i];
                let same = earlier.model == later.model && earlier.operation == later.operation;
                if same && earlier.cardinality != Cardinality::Single {
                    return Err(at(
                        decl.slots[j].pos,
                        format!(
                            "ambiguous slot: {} cannot be told apart from {}",
                            later.name, earlier.name
                        ),
                    ));
                }
                if earlier.cardinality == Cardinality::Single {
                    break;
                }
            }
        }
    }
    Ok(validated)
}
