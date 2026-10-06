//! Parse: schema text to declarations with token positions. No semantic rule
//! lives here; [`crate::validate`] consumes the result.
use axton_core::CallKind;
use serde_json::{Map, Value, json};
#[derive(Clone, Debug)]
struct Token {
    text: String,
    line: usize,
    col: usize,
}
/// Source position of a declaration, kept beside the parsed value so semantic
/// checks can point at the declaration instead of the end of the input.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Pos {
    pub line: usize,
    pub col: usize,
}
/// A diagnostic at a source position: `line:col: message`.
pub fn at(pos: Pos, msg: impl AsRef<str>) -> String {
    format!("{}:{}: {}", pos.line, pos.col, msg.as_ref())
}
struct Parser {
    tokens: Vec<Token>,
    i: usize,
}
impl Parser {
    fn pos(&self) -> Pos {
        let t = &self.tokens[self.i.min(self.tokens.len() - 1)];
        Pos {
            line: t.line,
            col: t.col,
        }
    }
    fn err(&self, msg: impl AsRef<str>) -> String {
        let t = &self.tokens[self.i.min(self.tokens.len() - 1)];
        format!(
            "{}:{}: {} (found '{}')",
            t.line,
            t.col,
            msg.as_ref(),
            t.text
        )
    }
    fn peek(&self) -> &str {
        &self.tokens[self.i].text
    }
    fn take(&mut self) -> String {
        let s = self.peek().to_string();
        if s != "<eof>" {
            self.i += 1;
        }
        s
    }
    fn eat(&mut self, s: &str) -> bool {
        if self.peek() == s {
            self.take();
            true
        } else {
            false
        }
    }
    fn need(&mut self, s: &str) -> Result<(), String> {
        if self.eat(s) {
            Ok(())
        } else {
            Err(self.err(format!("expected {s}")))
        }
    }
    fn ident(&mut self) -> Result<String, String> {
        let s = self.take();
        if s.chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            Ok(s)
        } else {
            Err(self.err("expected identifier"))
        }
    }
    fn expression(&mut self) -> Result<Value, String> {
        if self.eat("[") {
            let mut items = vec![];
            while !self.eat("]") {
                items.push(self.expression()?);
                if self.peek() != "]" {
                    self.need(",")?;
                }
            }
            return Ok(json!(items));
        }
        if self.peek().starts_with('"') {
            return serde_json::from_str(&self.take()).map_err(|_| self.err("invalid string"));
        }
        let mut name = self.ident()?;
        while self.eat(".") {
            name.push('.');
            name.push_str(&self.ident()?);
        }
        if self.peek() == "(" {
            return Ok(json!({"name":name,"arguments":self.arguments()?}));
        }
        Ok(json!(name))
    }
    fn arguments(&mut self) -> Result<Value, String> {
        self.need("(")?;
        let mut args = serde_json::Map::new();
        let mut positional = 0;
        while !self.eat(")") {
            // A name is `ident(.ident)*` before `:`; the dotted form is a
            // sequence argument's relation path into the prior act's slot.
            let mut end = self.i + 1;
            while self.tokens.get(end).is_some_and(|t| t.text == ".") {
                end += 2;
            }
            let named = self.tokens.get(end).is_some_and(|t| t.text == ":");
            let key = if named {
                let mut k = self.ident()?;
                while self.eat(".") {
                    k.push('.');
                    k.push_str(&self.ident()?);
                }
                self.need(":")?;
                k
            } else {
                let k = positional.to_string();
                positional += 1;
                k
            };
            let value = self.expression()?;
            if args.insert(key, value).is_some() {
                return Err(self.err("duplicate argument"));
            }
            if self.peek() != ")" {
                self.need(",")?;
            }
        }
        Ok(Value::Object(args))
    }
    /// The `(n)` of a `@@version` directive: a positive integer within the safe range.
    fn version(&mut self, seen: &mut bool) -> Result<u64, String> {
        if *seen {
            return Err(self.err("duplicate version"));
        }
        *seen = true;
        self.need("(")?;
        let version = self
            .take()
            .parse::<u64>()
            .map_err(|_| self.err("expected positive version"))?;
        if version == 0 || version > axton_core::MAX_SAFE_INTEGER {
            return Err(self.err("version must be positive"));
        }
        self.need(")")?;
        Ok(version)
    }
    /// The arguments of a `@deprecated` directive, GraphQL style: nothing, or
    /// `(reason: "text")`.
    fn deprecation(&mut self) -> Result<Option<String>, String> {
        if !self.eat("(") {
            return Ok(None);
        }
        if self.eat(")") {
            return Ok(None);
        }
        if self.ident()? != "reason" {
            return Err(self.err("deprecated accepts only reason"));
        }
        self.need(":")?;
        if !self.peek().starts_with('"') {
            return Err(self.err("deprecated reason must be a string"));
        }
        let reason: String =
            serde_json::from_str(&self.take()).map_err(|_| self.err("invalid string"))?;
        self.need(")")?;
        Ok(Some(reason))
    }
    fn names(&mut self, end: &str) -> Result<Vec<String>, String> {
        let mut n = vec![];
        while !self.eat(end) {
            n.push(self.ident()?);
            if self.peek() != end {
                self.need(",")?;
            }
        }
        Ok(n)
    }
    fn action_shape(&mut self) -> Result<(bool, bool), String> {
        let list = if self.eat("[") {
            self.need("]")?;
            true
        } else {
            false
        };
        let nullable = self.eat("?");
        if self.peek() == "[" {
            return Err(self.err(if nullable {
                "nullable list elements are unsupported"
            } else {
                "nested lists are unsupported"
            }));
        }
        if list && nullable {
            return Err(self.err("nullable lists are unsupported"));
        }
        Ok((list, nullable))
    }
    fn field(&mut self, action: bool) -> Result<FieldDecl, String> {
        let pos = self.pos();
        let name = self.ident()?;
        let type_name = self.ident()?;
        self.field_after_header(name, type_name, pos, action)
    }
    fn field_after_header(
        &mut self,
        name: String,
        type_name: String,
        pos: Pos,
        action: bool,
    ) -> Result<FieldDecl, String> {
        let (list, nullable) = if action {
            self.action_shape()?
        } else {
            let list = if self.eat("[") {
                self.need("]")?;
                true
            } else {
                false
            };
            (list, self.eat("?"))
        };
        let mut attributes = Map::new();
        let mut deprecated = None;
        let mut default = None;
        while self.peek() == "@" && self.tokens.get(self.i + 1).is_some_and(|t| t.text != "@") {
            let directive_pos = self.pos();
            self.need("@")?;
            let attr = self.ident()?;
            if attr == "deprecated" {
                if deprecated.is_some() {
                    return Err(self.err("duplicate field directive"));
                }
                deprecated = Some(self.deprecation()?);
                continue;
            }
            if attr == "default" {
                if default.is_some() {
                    return Err(self.err("duplicate field directive"));
                }
                default = Some(self.default_value(directive_pos)?);
                continue;
            }
            if !["reference", "inverse", "requires"].contains(&attr.as_str()) {
                return Err(self.err(format!("unsupported field directive {attr}")));
            }
            let args = self.arguments()?;
            if attributes.insert(attr, args).is_some() {
                return Err(self.err("duplicate field directive"));
            }
        }
        Ok(FieldDecl {
            name,
            type_name,
            list,
            nullable,
            attributes,
            deprecated,
            default,
            pos,
        })
    }
    /// The `(expression)` of a `@default` directive: a string, a number, an
    /// identifier (`true`, `false`, `null` or an enum member) or a function
    /// call whose arguments are counted, never evaluated.
    fn default_value(&mut self, pos: Pos) -> Result<DefaultDecl, String> {
        self.need("(")?;
        if self.peek() == ")" {
            return Err(self.err("expected default value"));
        }
        let expr = if self.peek().starts_with('"') {
            DefaultExpr::String(
                serde_json::from_str(&self.take()).map_err(|_| self.err("invalid string"))?,
            )
        } else if self.peek() == "-" || self.peek().starts_with(|c: char| c.is_ascii_digit()) {
            self.number()?
        } else {
            let name = self.ident()?;
            if self.peek() == "(" {
                self.need("(")?;
                let mut arguments = 0;
                while !self.eat(")") {
                    if self.peek() == "<eof>" {
                        return Err(self.err("expected )"));
                    }
                    if self.peek() == "-" || self.peek().starts_with(|c: char| c.is_ascii_digit()) {
                        self.number()?;
                    } else {
                        self.expression()?;
                    }
                    arguments += 1;
                    if self.peek() != ")" {
                        self.need(",")?;
                    }
                }
                DefaultExpr::Call { name, arguments }
            } else {
                DefaultExpr::Identifier(name)
            }
        };
        self.need(")")?;
        Ok(DefaultDecl { expr, pos })
    }
    /// A JSON-grammar number spelled by adjacent tokens (`-`, digits, `.`,
    /// exponent), such as `-1.5e3`.
    fn number(&mut self) -> Result<DefaultExpr, String> {
        let first = self.i;
        let mut text = self.take();
        loop {
            let previous = &self.tokens[self.i - 1];
            let next = &self.tokens[self.i];
            let adjacent = next.line == previous.line
                && next.col == previous.col + previous.text.chars().count();
            let continues = next.text == "."
                || next.text == "-"
                || next.text.starts_with(|c: char| c.is_ascii_alphanumeric());
            if !adjacent || !continues || next.text == "<eof>" {
                break;
            }
            text.push_str(&self.take());
        }
        if serde_json::from_str::<serde_json::Number>(&text).is_err() {
            self.i = first;
            return Err(self.err("invalid default number"));
        }
        Ok(DefaultExpr::Number(text))
    }
    /// `(name Type, slot Model.operation, …)` of an operation declaration.
    fn operation_inputs(&mut self) -> Result<Vec<ActionInputDecl>, String> {
        self.need("(")?;
        let mut inputs = vec![];
        while !self.eat(")") {
            let input_pos = self.pos();
            let input_name = self.ident()?;
            let input_type = self.ident()?;
            let input = if self.peek() == "." {
                ActionInputDecl::Model(self.slot(input_name, input_type, input_pos)?)
            } else {
                ActionInputDecl::Value(
                    self.field_after_header(input_name, input_type, input_pos, true)?,
                )
            };
            inputs.push(input);
            if self.peek() == "<eof>" {
                return Err(self.err("expected )"));
            }
            if self.peek() != ")" {
                self.need(",")?;
            }
        }
        Ok(inputs)
    }
    /// The outputs of an operation after its opening `{`, through the `}`.
    fn operation_outputs(&mut self) -> Result<Vec<ActionOutputDecl>, String> {
        let mut outputs = vec![];
        while !self.eat("}") {
            if self.peek() == "<eof>" {
                return Err(self.err("expected }"));
            }
            outputs.push(ActionOutputDecl {
                field: self.field(true)?,
            });
            self.eat(",");
        }
        Ok(outputs)
    }
    fn slot(&mut self, name: String, model: String, pos: Pos) -> Result<SlotDecl, String> {
        self.need(".")?;
        let operation = self.ident()?;
        if !["create", "update", "delete"].contains(&operation.as_str()) {
            return Err(self.err("unknown operation"));
        }
        let allowed_patch_fields = if self.eat("<") {
            if operation != "update" {
                return Err(self.err("field restriction requires update"));
            }
            Some(self.names(">")?)
        } else {
            None
        };
        let relation_bindings = if self.peek() == "(" {
            self.arguments()?
        } else {
            json!({})
        };
        let (list, nullable) = self.action_shape()?;
        let cardinality = if list {
            "list"
        } else if nullable {
            "optional"
        } else {
            "single"
        };
        let mut deprecated = None;
        while self.peek() == "@" && self.tokens.get(self.i + 1).is_some_and(|t| t.text != "@") {
            self.need("@")?;
            let attr = self.ident()?;
            if attr != "deprecated" {
                return Err(self.err(format!("unsupported slot directive {attr}")));
            }
            if deprecated.is_some() {
                return Err(self.err("duplicate deprecated"));
            }
            deprecated = Some(self.deprecation()?);
        }
        Ok(SlotDecl {
            name,
            model,
            operation,
            cardinality: cardinality.into(),
            allowed_patch_fields,
            relation_bindings,
            deprecated,
            pos,
        })
    }
}
fn lex(s: &str) -> Result<Vec<Token>, String> {
    let chars: Vec<_> = s.chars().collect();
    let (mut i, mut line, mut col) = (0, 1, 1);
    let mut out = vec![];
    while i < chars.len() {
        let c = chars[i];
        if c == '\n' {
            line += 1;
            col = 1;
            i += 1;
            continue;
        }
        if c.is_whitespace() {
            i += 1;
            col += 1;
            continue;
        }
        if c == '/' && chars.get(i + 1) == Some(&'/') {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
                col += 1;
            }
            continue;
        }
        let start = i;
        let column = col;
        if c.is_ascii_alphanumeric() || c == '_' {
            while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                i += 1;
                col += 1;
            }
        } else if c == '"' {
            i += 1;
            col += 1;
            while i < chars.len() && chars[i] != '"' {
                if chars[i] == '\\' {
                    i += 1;
                    col += 1;
                }
                i += 1;
                col += 1;
            }
            if i >= chars.len() {
                return Err(format!("{line}:{column}: unterminated string"));
            }
            i += 1;
            col += 1;
        } else if "{}()[]?,.@<>:-".contains(c) {
            i += 1;
            col += 1;
        } else {
            return Err(format!("{line}:{col}: unsupported character {c}"));
        }
        out.push(Token {
            text: chars[start..i].iter().collect(),
            line,
            col: column,
        });
    }
    out.push(Token {
        text: "<eof>".into(),
        line,
        col,
    });
    Ok(out)
}

/// Everything the source declares, in source order, each with the position of
/// its first token. Expressions (directive arguments, slot bindings, sequence
/// arguments) stay as JSON values; validation gives them meaning.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Declarations {
    pub enums: Vec<EnumDecl>,
    pub models: Vec<ModelDecl>,
    pub mutations: Vec<MutationDecl>,
    pub actions: Vec<ActionDecl>,
    pub loads: Vec<LoadDecl>,
    pub prerequisites: Vec<PrerequisiteDecl>,
    /// Position of the end of input, for diagnostics that have no declaration.
    pub end: Pos,
}
#[derive(Clone, Debug, PartialEq)]
pub struct EnumDecl {
    pub name: String,
    pub values: Vec<String>,
    /// `value @deprecated(reason: "…")`: the value and its optional reason.
    pub deprecated: Vec<(String, Option<String>)>,
    pub pos: Pos,
}
#[derive(Clone, Debug, PartialEq)]
pub struct ModelDecl {
    pub name: String,
    pub abstract_model: bool,
    pub parent: Option<String>,
    pub bootstrap: bool,
    /// The read-contract version declared by `@@version(n)`; 1 when omitted.
    pub version: u64,
    pub identity: Vec<String>,
    pub fields: Vec<FieldDecl>,
    pub unique: Vec<UniqueDecl>,
    pub pos: Pos,
}
#[derive(Clone, Debug, PartialEq)]
pub struct FieldDecl {
    pub name: String,
    pub type_name: String,
    pub list: bool,
    pub nullable: bool,
    /// `@reference`, `@inverse` and `@requires` arguments, keyed by directive name.
    pub attributes: Map<String, Value>,
    /// `@deprecated(reason: "…")`: `Some(reason)` when present, the reason itself optional.
    pub deprecated: Option<Option<String>>,
    /// `@default(…)`: the creation default as written, given meaning by validation.
    pub default: Option<DefaultDecl>,
    pub pos: Pos,
}
/// A `@default` directive and the position of its `@`.
#[derive(Clone, Debug, PartialEq)]
pub struct DefaultDecl {
    pub expr: DefaultExpr,
    pub pos: Pos,
}
/// A default expression as written. Numbers keep their source text so
/// validation can apply the field's own rules (safe integers, finite floats).
#[derive(Clone, Debug, PartialEq)]
pub enum DefaultExpr {
    String(String),
    Number(String),
    Identifier(String),
    Call { name: String, arguments: usize },
}
#[derive(Clone, Debug, PartialEq)]
pub struct UniqueDecl {
    pub fields: Vec<String>,
    pub pos: Pos,
}
#[derive(Clone, Debug, PartialEq)]
pub struct MutationDecl {
    pub name: String,
    pub version: u64,
    pub slots: Vec<SlotDecl>,
    pub sequence: Option<SequenceDecl>,
    pub pos: Pos,
}
#[derive(Clone, Debug, PartialEq)]
pub struct ActionDecl {
    pub name: String,
    /// `mutation Name(...)` or `query Name(...)`.
    pub kind: CallKind,
    pub version: u64,
    pub inputs: Vec<ActionInputDecl>,
    pub outputs: Vec<ActionOutputDecl>,
    pub sequence: Option<SequenceDecl>,
    pub pos: Pos,
}
/// `load Name(inputs) { outputs }`: a native paged read. Inputs share the
/// operation grammar; validation refuses Model operands and non-list outputs.
#[derive(Clone, Debug, PartialEq)]
pub struct LoadDecl {
    pub name: String,
    pub version: u64,
    pub inputs: Vec<ActionInputDecl>,
    pub outputs: Vec<ActionOutputDecl>,
    pub pos: Pos,
}
#[derive(Clone, Debug, PartialEq)]
pub enum ActionInputDecl {
    Value(FieldDecl),
    Model(SlotDecl),
}
#[derive(Clone, Debug, PartialEq)]
pub struct ActionOutputDecl {
    pub field: FieldDecl,
}
#[derive(Clone, Debug, PartialEq)]
pub struct SequenceDecl {
    pub arguments: Value,
    pub pos: Pos,
}
#[derive(Clone, Debug, PartialEq)]
pub struct SlotDecl {
    pub name: String,
    pub model: String,
    pub operation: String,
    pub cardinality: String,
    pub allowed_patch_fields: Option<Vec<String>>,
    /// The `(relation: parentSlot, …)` arguments, as a JSON object.
    pub relation_bindings: Value,
    /// `@deprecated(reason: "…")` on the slot.
    pub deprecated: Option<Option<String>>,
    pub pos: Pos,
}
#[derive(Clone, Debug, PartialEq)]
pub struct PrerequisiteDecl {
    pub name: String,
    pub fields: Vec<PrerequisiteField>,
    pub pos: Pos,
}
#[derive(Clone, Debug, PartialEq)]
pub struct PrerequisiteField {
    pub name: String,
    pub type_name: String,
    pub pos: Pos,
}

/// Turn schema text into declarations. Errors are syntax errors only:
/// `line:col: message (found 'token')`.
pub fn parse(source: &str) -> Result<Declarations, String> {
    let mut p = Parser {
        tokens: lex(source)?,
        i: 0,
    };
    let mut d = Declarations::default();
    while p.peek() != "<eof>" {
        let (mut leading_version, mut leading_version_seen) = (1, false);
        let mut leading_sequence = None;
        while p.peek() == "@" {
            let directive_pos = p.pos();
            p.need("@")?;
            let attr = p.ident()?;
            match attr.as_str() {
                "version" => leading_version = p.version(&mut leading_version_seen)?,
                "sequence" => {
                    if leading_sequence.is_some() {
                        return Err(p.err("duplicate sequence"));
                    }
                    leading_sequence = Some(SequenceDecl {
                        arguments: p.arguments()?,
                        pos: directive_pos,
                    });
                }
                _ => return Err(p.err(format!("unsupported declaration directive {attr}"))),
            }
        }
        if p.peek() == "<eof>" && (leading_version_seen || leading_sequence.is_some()) {
            return Err(p.err("expected declaration after directive"));
        }
        let pos = p.pos();
        if p.peek() == "action" {
            return Err(p.err(
                "action declarations were replaced: declare mutation Name(...) or query Name(...)",
            ));
        }
        let abstract_model = p.eat("abstract");
        if abstract_model {
            p.need("model")?;
        }
        let kind = if abstract_model {
            "model".into()
        } else {
            p.take()
        };
        let name = p.ident()?;
        // `mutation Name(` and `query Name(` are operations; the older
        // `mutation Name { slots }` block keeps its own grammar below.
        let operation = match kind.as_str() {
            "query" => Some(CallKind::Query),
            "mutation" if p.peek() == "(" => Some(CallKind::Mutation),
            _ => None,
        };
        if (leading_version_seen || leading_sequence.is_some())
            && operation.is_none()
            && kind != "model"
            && kind != "load"
        {
            return Err(p.err(format!("declaration directives are unsupported on {kind}")));
        }
        if kind == "model" && leading_sequence.is_some() {
            return Err(p.err("sequence requires mutation"));
        }
        // `load Name(inputs) { outputs }`: braces and at least one output are
        // required, and no sequence policy applies to a read.
        if kind == "load" {
            if let Some(sequence) = &leading_sequence {
                return Err(at(
                    sequence.pos,
                    format!("Load {name} cannot declare @sequence"),
                ));
            }
            let inputs = p.operation_inputs()?;
            if p.peek() != "{" {
                return Err(p.err(format!("expected {{ with the outputs of Load {name}")));
            }
            p.need("{")?;
            if p.peek() == "}" {
                return Err(at(
                    p.pos(),
                    format!("Load {name} requires at least one output"),
                ));
            }
            let outputs = p.operation_outputs()?;
            d.loads.push(LoadDecl {
                name,
                version: leading_version,
                inputs,
                outputs,
                pos,
            });
            continue;
        }
        if let Some(operation) = operation {
            let inputs = p.operation_inputs()?;
            let outputs = if p.eat("{") {
                p.operation_outputs()?
            } else {
                vec![]
            };
            d.actions.push(ActionDecl {
                name,
                kind: operation,
                version: leading_version,
                inputs,
                outputs,
                sequence: leading_sequence,
                pos,
            });
            continue;
        }
        if kind == "prerequisite" {
            p.need("(")?;
            let mut fields = vec![];
            while !p.eat(")") {
                let pos = p.pos();
                let name = p.ident()?;
                let type_name = p.ident()?;
                fields.push(PrerequisiteField {
                    name,
                    type_name,
                    pos,
                });
                if p.peek() != ")" {
                    p.need(",")?;
                }
            }
            d.prerequisites.push(PrerequisiteDecl { name, fields, pos });
            continue;
        }
        let parent = if kind == "model" && p.eat("extends") {
            Some(p.ident()?)
        } else {
            None
        };
        if abstract_model && leading_version_seen {
            return Err(at(pos, "abstract models cannot declare a runtime version"));
        }
        p.need("{")?;
        match kind.as_str() {
            "enum" => {
                let (mut values, mut deprecated) = (vec![], vec![]);
                while !p.eat("}") {
                    let value = p.ident()?;
                    let mut marked = false;
                    while p.peek() == "@" && p.tokens.get(p.i + 1).is_some_and(|t| t.text != "@") {
                        p.need("@")?;
                        let attr = p.ident()?;
                        if attr != "deprecated" {
                            return Err(p.err(format!("unsupported enum value directive {attr}")));
                        }
                        if marked {
                            return Err(p.err("duplicate deprecated"));
                        }
                        marked = true;
                        deprecated.push((value.clone(), p.deprecation()?));
                    }
                    values.push(value);
                    p.eat(",");
                }
                d.enums.push(EnumDecl {
                    name,
                    values,
                    deprecated,
                    pos,
                });
            }
            "model" => {
                let mut bootstrap = false;
                let (mut fields, mut identity, mut unique) = (vec![], vec![], vec![]);
                let (mut version, mut version_seen) = (leading_version, leading_version_seen);
                while !p.eat("}") {
                    let directive_pos = p.pos();
                    if p.eat("@") {
                        p.need("@")?;
                        let attr = p.ident()?;
                        if abstract_model {
                            return Err(at(
                                directive_pos,
                                "abstract models cannot declare model directives",
                            ));
                        }
                        if attr == "bootstrap" {
                            if bootstrap {
                                return Err(at(directive_pos, "duplicate bootstrap"));
                            }
                            bootstrap = true;
                            continue;
                        }
                        if attr == "version" {
                            version = p.version(&mut version_seen)?;
                            continue;
                        }
                        p.need("(")?;
                        let names = p.names(")")?;
                        match attr.as_str() {
                            "id" => {
                                if !identity.is_empty() {
                                    return Err(p.err("duplicate identity"));
                                }
                                identity = names
                            }
                            "unique" => unique.push(UniqueDecl {
                                fields: names,
                                pos: directive_pos,
                            }),
                            _ => return Err(p.err(format!("unsupported model directive {attr}"))),
                        }
                        continue;
                    }
                    fields.push(p.field(false)?);
                }
                d.models.push(ModelDecl {
                    name,
                    abstract_model,
                    parent,
                    bootstrap,
                    version,
                    identity,
                    fields,
                    unique,
                    pos,
                });
            }
            "mutation" => {
                let (mut slots, mut version) = (vec![], 1);
                let mut sequence = None;
                let mut version_seen = false;
                while !p.eat("}") {
                    let directive_pos = p.pos();
                    if p.eat("@") {
                        p.need("@")?;
                        let attr = p.ident()?;
                        if attr == "sequence" {
                            sequence = Some(SequenceDecl {
                                arguments: p.arguments()?,
                                pos: directive_pos,
                            });
                            continue;
                        }
                        if attr != "version" {
                            return Err(p.err(format!("unsupported mutation directive {attr}")));
                        }
                        version = p.version(&mut version_seen)?;
                        continue;
                    }
                    let slot = p.ident()?;
                    let model = p.ident()?;
                    p.need(".")?;
                    let operation = p.ident()?;
                    if !["create", "update", "delete"].contains(&operation.as_str()) {
                        return Err(p.err("unknown operation"));
                    }
                    let allowed_patch_fields = if p.eat("<") {
                        if operation != "update" {
                            return Err(p.err("field restriction requires update"));
                        }
                        Some(p.names(">")?)
                    } else {
                        None
                    };
                    let relation_bindings = if p.peek() == "(" {
                        p.arguments()?
                    } else {
                        json!({})
                    };
                    let cardinality = if p.eat("[") {
                        p.need("]")?;
                        "list"
                    } else if p.eat("?") {
                        "optional"
                    } else {
                        "single"
                    };
                    let mut deprecated = None;
                    while p.peek() == "@" && p.tokens.get(p.i + 1).is_some_and(|t| t.text != "@") {
                        p.need("@")?;
                        let attr = p.ident()?;
                        if attr != "deprecated" {
                            return Err(p.err(format!("unsupported slot directive {attr}")));
                        }
                        if deprecated.is_some() {
                            return Err(p.err("duplicate deprecated"));
                        }
                        deprecated = Some(p.deprecation()?);
                    }
                    slots.push(SlotDecl {
                        name: slot,
                        model,
                        operation,
                        cardinality: cardinality.into(),
                        allowed_patch_fields,
                        relation_bindings,
                        deprecated,
                        pos: directive_pos,
                    });
                }
                d.mutations.push(MutationDecl {
                    name,
                    version,
                    slots,
                    sequence,
                    pos,
                });
            }
            _ => return Err(p.err(format!("unsupported declaration {kind}"))),
        }
    }
    d.end = p.pos();
    Ok(d)
}
