//! Identifiers emitted into the Action TypeScript and Dart contract namespaces.
use crate::parse::{Declarations, Pos};
use serde_json::Value;
use std::collections::BTreeMap;

fn values<'a>(value: &'a Value, key: &str) -> &'a [Value] {
    value[key].as_array().map(Vec::as_slice).unwrap_or(&[])
}
fn name(value: &Value) -> &str {
    value["name"].as_str().unwrap()
}
fn position(declarations: Option<&Declarations>, owner: &str) -> Option<Pos> {
    let declarations = declarations?;
    if let Some(n) = owner.strip_prefix("model ") {
        return declarations
            .models
            .iter()
            .find(|m| m.name == n)
            .map(|m| m.pos);
    }
    if let Some(n) = owner.strip_prefix("enum ") {
        return declarations
            .enums
            .iter()
            .find(|e| e.name == n)
            .map(|e| e.pos);
    }
    if let Some(n) = owner.strip_prefix("mutation ") {
        let n = n.split(' ').next()?;
        return declarations
            .mutations
            .iter()
            .find(|m| m.name == n)
            .map(|m| m.pos);
    }
    if let Some(n) = owner.strip_prefix("Load ") {
        let n = n.split(' ').next()?;
        return declarations
            .loads
            .iter()
            .find(|l| l.name == n)
            .map(|l| l.pos);
    }
    let action = owner
        .strip_prefix("Mutation ")
        .or_else(|| owner.strip_prefix("Query "))?
        .split(' ')
        .next()?;
    declarations
        .actions
        .iter()
        .find(|a| a.name == action)
        .map(|a| a.pos)
}

/// Names the generated clients and backend declare (or re-export) beside
/// Loads, and only when the schema declares one ([#173](https://github.com/zanminwang/axton/issues/173)).
pub(crate) const LOAD_HELPERS: &[&str] = &[
    "JsonValue",
    "Load",
    "LoadContext",
    "LoadError",
    "LoadException",
    "LoadHandlerCall",
    "LoadInvalidations",
    "LoadNext",
    "LoadOptions",
    "LoadPhase",
    "LoadStatus",
    "Loads",
];

/// Names the generated clients declare beside a current Mutation for
/// transactional Mutation enqueue and the transaction's resolutions of unsent
/// work; `SubmitMutationPort` is also the Dart runtime's port those clients
/// bind to.
const TRANSACTION_HELPERS: &[&str] = &[
    "ApplicationTransaction",
    "CompanionContext",
    "CompanionOptions",
    "SubmitMutationOptions",
    "SubmitMutationPort",
    "TransactionMutations",
    "UnsentResolutionPort",
];

/// Members of the generated Dart `{Name}Store` selector (and `Object`).
const STORE_SELECTOR_MEMBERS: &[&str] = &[
    "toWire",
    "toString",
    "hashCode",
    "runtimeType",
    "noSuchMethod",
];

/// The retained kind of an emitted operation descriptor; omitted is a Mutation.
pub(crate) fn kind(action: &Value) -> axton_core::CallKind {
    serde_json::from_value(action["kind"].clone()).unwrap_or_default()
}
/// `Mutation` or `Query`, as diagnostics and generated owners name it.
fn label(action: &Value) -> &'static str {
    match kind(action) {
        axton_core::CallKind::Mutation => "Mutation",
        axton_core::CallKind::Query => "Query",
    }
}

/// Whether an emitted output descriptor may be named by a call's `store`
/// map: the single rule is [`axton_core::store_eligible`].
pub(crate) fn store_eligible(output: &Value) -> bool {
    serde_json::from_value::<axton_core::ActionOutputDescriptor>(output.clone())
        .is_ok_and(|descriptor| axton_core::store_eligible(&descriptor))
}

/// Called once for current declarations and again after retained histories are reconciled.
/// The second call checks names that only exist in older Action/Model versions.
pub(crate) fn check(config: &Value, declarations: Option<&Declarations>) -> Result<(), String> {
    let actions = values(config, "actions");
    let mut names = BTreeMap::<String, String>::new();
    let mut add = |identifier: String, owner: String| -> Result<(), String> {
        if let Some(previous) = names.get(&identifier) {
            if previous != &owner {
                let pos =
                    position(declarations, &owner).or_else(|| position(declarations, previous));
                let message = format!(
                    "generated identifier {identifier} from {owner} collides with {previous}"
                );
                return Err(match pos {
                    Some(pos) => format!("{}:{}: {message}", pos.line, pos.col),
                    None => format!("operation history: {message}"),
                });
            }
        } else {
            names.insert(identifier, owner);
        }
        Ok(())
    };
    let schema = &config["schema"];
    for en in values(schema, "enums") {
        add(name(en).into(), format!("enum {}", name(en)))?;
    }
    for model in values(config, "abstractModels") {
        add(name(model).into(), format!("model {}", name(model)))?;
    }
    for model in values(schema, "models") {
        let n = name(model);
        let owner = format!("model {n}");
        for suffix in [
            "",
            "Identity",
            "Patch",
            "Create",
            "CreateInput",
            "Update",
            "Delete",
            "Filter",
            "Order",
            "OrderField",
            "Model",
            "LiveModel",
            "TxModel",
        ] {
            add(format!("{n}{suffix}"), owner.clone())?;
        }
    }
    for model in values(config, "backendModels") {
        let n = name(model);
        let version = model["version"].as_u64().unwrap_or(1);
        let current = values(schema, "models")
            .iter()
            .find(|m| name(m) == n)
            .and_then(|m| m["version"].as_u64());
        if current != Some(version) {
            add(format!("{n}V{version}"), format!("model {n}"))?;
            add(format!("{n}V{version}Identity"), format!("model {n}"))?;
        }
    }
    // Beside the shared Load names, exactly the per-version backend handler
    // types: `{Name}Input` and `{Name}HandlerOutput`, `V{n}`-prefixed for a
    // retained version.
    let loads = values(config, "loads");
    if !loads.is_empty() {
        for helper in LOAD_HELPERS {
            add((*helper).into(), "load helper".into())?;
        }
    }
    let load_versions = || {
        loads
            .iter()
            .filter_map(|load| Some((load["name"].as_str()?, load["version"].as_u64()?)))
    };
    let mut load_latest = BTreeMap::<&str, u64>::new();
    for (n, version) in load_versions() {
        load_latest
            .entry(n)
            .and_modify(|v| *v = (*v).max(version))
            .or_insert(version);
    }
    for (n, version) in load_versions() {
        let prefix = if version == load_latest[n] {
            n.to_owned()
        } else {
            format!("{n}V{version}")
        };
        for suffix in ["Input", "HandlerOutput"] {
            add(
                format!("{prefix}{suffix}"),
                format!("Load {n} v{version} {suffix}"),
            )?;
        }
    }
    let mutations = if config["backendMutations"].is_array() {
        values(config, "backendMutations")
    } else {
        values(config, "mutations")
    };
    let mut mutation_latest = BTreeMap::<&str, u64>::new();
    for mutation in mutations {
        let n = name(mutation);
        let version = mutation["version"].as_u64().unwrap();
        mutation_latest
            .entry(n)
            .and_modify(|v| *v = (*v).max(version))
            .or_insert(version);
    }
    for mutation in mutations {
        let n = name(mutation);
        let version = mutation["version"].as_u64().unwrap();
        let prefix = if version == mutation_latest[n] {
            n.to_owned()
        } else {
            format!("{n}V{version}")
        };
        add(format!("{prefix}Input"), format!("mutation {n} v{version}"))?;
    }
    for mutation in values(config, "mutations") {
        let n = name(mutation);
        let owner = format!("mutation {n} v{}", mutation["version"]);
        add(format!("{n}Args"), owner.clone())?;
        // Dart's encoder is a top-level function in the same library as
        // generated field classes. Its actual lower-first name must not
        // shadow a type or another Mutation's encoder.
        add(crate::emit::lower(n), owner)?;
    }
    // Model-only schemas still emit every per-model type (such as
    // `{Model}Create`); the remaining names exist only beside operations.
    if actions.is_empty() {
        return Ok(());
    }
    for helper in [
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
        "DirectMutations",
        "MutationContext",
        "MutationHandlerCall",
        "MutationHandlers",
        "Mutations",
        "OnceOptions",
        "Queries",
        "QueryContext",
        "QueryHandlerCall",
        "QueryHandlers",
        "QueryInvalidations",
        "QueuedQueries",
    ] {
        add(helper.into(), "operation helper".into())?;
    }
    let mut latest = BTreeMap::<&str, u64>::new();
    for action in actions {
        let n = name(action);
        let version = action["version"].as_u64().unwrap();
        latest
            .entry(n)
            .and_modify(|v| *v = (*v).max(version))
            .or_insert(version);
    }
    // Transactional Mutation enqueue emits these beside a current Mutation only.
    let current_mutation = actions.iter().any(|action| {
        action["version"].as_u64() == Some(latest[name(action)])
            && kind(action) == axton_core::CallKind::Mutation
    });
    if current_mutation {
        for helper in TRANSACTION_HELPERS {
            add((*helper).into(), "transaction mutation helper".into())?;
        }
    }
    let mut handler_groups = std::collections::BTreeSet::new();
    for action in actions {
        handler_groups.insert((label(action), name(action)));
    }
    for (kind, n) in handler_groups {
        add(format!("{kind}{n}Handlers"), format!("{kind} {n} handlers"))?;
    }
    for action in actions {
        let n = name(action);
        if action["version"].as_u64() != Some(latest[n]) {
            continue;
        }
        let kind = label(action);
        add(format!("{n}Options"), format!("{kind} {n} options"))?;
        add(format!("{n}Store"), format!("{kind} {n} store selector"))?;
    }
    // Store-eligible outputs become fields of the generated Dart selector,
    // so they cannot reuse the names of its inherited or declared members.
    for action in actions {
        let n = name(action);
        if action["version"].as_u64() != Some(latest[n]) {
            continue;
        }
        for output in values(action, "outputs") {
            let output_name = name(output);
            if store_eligible(output) && STORE_SELECTOR_MEMBERS.contains(&output_name) {
                let kind = label(action);
                let owner = format!("{kind} {n}");
                let message = format!(
                    "{kind} {n} output {output_name} is reserved: it would collide with a member of the generated {n}Store selector"
                );
                return Err(match position(declarations, &owner) {
                    Some(pos) => format!("{}:{}: {message}", pos.line, pos.col),
                    None => format!("operation history: {message}"),
                });
            }
        }
    }
    for action in actions {
        let n = name(action);
        let version = action["version"].as_u64().unwrap();
        let retained = version != latest[n];
        let prefix = if retained {
            format!("{n}V{version}")
        } else {
            n.to_owned()
        };
        let owner = format!("{} {n} v{version}", label(action));
        for suffix in ["Input", "HandlerOutput"] {
            add(format!("{prefix}{suffix}"), format!("{owner} {suffix}"))?;
        }
        if !retained {
            add(format!("{prefix}Output"), format!("{owner} Output"))?;
        } else {
            for model in values(&action["input"], "models") {
                for suffix in ["Create", "Identity", "Patch", "Update", "Delete"] {
                    add(
                        format!("{prefix}{}{suffix}", name(model)),
                        format!("{owner} model {} {suffix}", name(model)),
                    )?;
                }
            }
            let mut input_enums = std::collections::BTreeSet::new();
            for en in values(&action["input"], "enums") {
                if input_enums.insert(name(en)) {
                    add(
                        format!("{prefix}{}", name(en)),
                        format!("{owner} input enum {}", name(en)),
                    )?;
                }
            }
            let mut output_enums = std::collections::BTreeSet::new();
            for en in values(action, "outputEnums") {
                if output_enums.insert(name(en)) {
                    add(
                        format!("{prefix}Output{}", name(en)),
                        format!("{owner} output enum {}", name(en)),
                    )?;
                }
            }
        }
        for arg in values(action, "inputs") {
            if arg["kind"] == "model"
                && arg["operation"] == "update"
                && arg["allowedPatchFields"].is_array()
            {
                let slot = arg["name"].as_str().unwrap();
                let upper = format!("{}{}", slot[..1].to_ascii_uppercase(), &slot[1..]);
                add(
                    format!(
                        "{prefix}{upper}{}Update",
                        if retained { "Slot" } else { "" }
                    ),
                    format!("{owner} operand {slot} Update"),
                )?;
            }
        }
    }
    Ok(())
}
