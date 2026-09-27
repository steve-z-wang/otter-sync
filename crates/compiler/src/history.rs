use serde_json::{Value, json};
fn list<'a>(v: &'a Value, k: &str) -> Result<&'a Vec<Value>, String> {
    v[k].as_array()
        .ok_or_else(|| format!("history: expected {k} array"))
}
fn named<'a>(items: &'a [Value], name: &Value) -> Option<&'a Value> {
    items.iter().find(|item| item["name"] == *name)
}
/// A field as a retained contract sees it: creation defaults are client
/// policy for fresh creates, not part of any backend read or input shape,
/// so they are projected out of snapshots and ignored when comparing them.
pub(crate) fn without_creation_policy(field: &Value) -> Value {
    let mut field = field.clone();
    if let Some(object) = field.as_object_mut() {
        object.remove("createDefault");
    }
    field
}
fn shapes(fields: &[Value]) -> Vec<Value> {
    fields.iter().map(without_creation_policy).collect()
}
/// The retained field named like `field`, compared by shape only.
fn same_shape(fields: &[Value], field: &Value) -> Option<bool> {
    named(fields, &field["name"])
        .map(|next| without_creation_policy(next) == without_creation_policy(field))
}
/// Retain the complete kind, input and output contract of each published operation.
/// Output changes always require a new operation version; input compatibility
/// follows the existing mutation operand rules.
pub fn reconcile_action_history(current: &Value, history: Option<&Value>) -> Result<Value, String> {
    let mut result = history
        .cloned()
        .unwrap_or(json!({"formatVersion":1,"actions":{}}));
    if result["formatVersion"] != 1 || !result["actions"].is_object() {
        return Err("unsupported operation history format".into());
    }
    retain_versions(
        &mut result["actions"],
        "operation",
        list(current, "actions")?,
        |action| capture_action(current, action),
        |label, old, snapshot| {
            // Kind is part of the retained backend contract: reclassifying a
            // published version would change how saved calls are executed.
            let (old_kind, new_kind) = (retained_kind(old)?, retained_kind(snapshot)?);
            if old_kind != new_kind {
                return Err(format!(
                    "{label}: kind changed from {} to {}; increase @version",
                    kind_name(old_kind),
                    kind_name(new_kind)
                ));
            }
            same_version_contract(label, old, snapshot)
        },
    )?;
    for versions in result["actions"].as_object().unwrap().values() {
        for snapshot in versions
            .as_object()
            .ok_or("invalid operation history versions")?
            .values()
        {
            // Every retained version, not only the current one, must name a known kind.
            retained_kind(snapshot)?;
            for prerequisite in list(snapshot, "prerequisites")? {
                if named(list(current, "prerequisites")?, &prerequisite["name"])
                    != Some(prerequisite)
                {
                    return Err(format!(
                        "retained operation still requires original prerequisite {}",
                        prerequisite["name"]
                    ));
                }
            }
        }
    }
    Ok(result)
}

/// Retain every published native Load version in `history/loads.json`, the
/// `actions.json` envelope under `loads`. A same-version change must keep
/// outputs (Model lists and their read versions) exactly and inputs
/// compatibly; a retained Load name cannot be removed, so it cannot become a
/// Query or Mutation either ([#173](https://github.com/zanminwang/axton/issues/173)).
pub fn reconcile_load_history(current: &Value, history: Option<&Value>) -> Result<Value, String> {
    let mut result = history
        .cloned()
        .unwrap_or(json!({"formatVersion":1,"loads":{}}));
    if result["formatVersion"] != 1 || !result["loads"].is_object() {
        return Err("unsupported load history format".into());
    }
    let loads = current["loads"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    retain_versions(
        &mut result["loads"],
        "load",
        loads,
        |load| capture_load(current, load),
        same_version_contract,
    )?;
    Ok(result)
}

/// The version rules every retained operation kind shares: a retained name
/// stays declared, a new name begins at version 1, versions never decrease,
/// and `same_version` judges a republished version before it is replaced.
fn retain_versions(
    retained: &mut Value,
    noun: &str,
    operations: &[Value],
    capture: impl Fn(&Value) -> Result<Value, String>,
    same_version: impl Fn(&str, &Value, &Value) -> Result<(), String>,
) -> Result<(), String> {
    for name in retained.as_object().unwrap().keys() {
        if !operations.iter().any(|a| a["name"] == *name) {
            return Err(format!("retained {noun} {name} cannot be removed"));
        }
    }
    for operation in operations {
        let name = operation["name"]
            .as_str()
            .ok_or_else(|| format!("unnamed {noun}"))?;
        let version = operation["version"]
            .as_u64()
            .ok_or_else(|| format!("invalid {noun} version"))?;
        let snapshot = capture(operation)?;
        let versions = retained
            .as_object_mut()
            .unwrap()
            .entry(name)
            .or_insert(json!({}))
            .as_object_mut()
            .ok_or_else(|| format!("invalid {noun} history versions"))?;
        if versions.is_empty() && version != 1 {
            return Err(format!(
                "{name}: initial {noun} history must begin at version 1"
            ));
        }
        let latest = versions
            .keys()
            .map(|v| {
                v.parse::<u64>()
                    .map_err(|_| format!("invalid retained {noun} version"))
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .max()
            .unwrap_or(0);
        if version < latest {
            return Err(format!("{name}: version cannot decrease from {latest}"));
        }
        if let Some(old) = versions.get(&version.to_string()) {
            same_version(&format!("{name} v{version}"), old, &snapshot)?;
        }
        versions.insert(version.to_string(), snapshot);
    }
    Ok(())
}

/// Outputs never change within a version; inputs follow the operand rules.
fn same_version_contract(label: &str, old: &Value, snapshot: &Value) -> Result<(), String> {
    if old["outputs"] != snapshot["outputs"] || old["outputEnums"] != snapshot["outputEnums"] {
        return Err(format!(
            "{label}: incompatible output change; increase @version"
        ));
    }
    if !action_inputs_compatible(old, snapshot)? {
        return Err(format!(
            "{label}: incompatible input change; increase @version"
        ));
    }
    Ok(())
}

/// A Load's retained contract: the descriptor itself, its outputs bound to
/// retained Model read contracts. Call-site options never appear in it.
fn capture_load(config: &Value, load: &Value) -> Result<Value, String> {
    for output in list(load, "outputs")? {
        if let Some(models) = config["backendModels"].as_array()
            && !models.iter().any(|model| {
                model["name"] == output["model"] && model["version"] == output["modelReadVersion"]
            })
        {
            return Err(format!(
                "load output {} has no retained Model read contract",
                output["name"]
            ));
        }
    }
    Ok(json!({
        "name":load["name"],"version":load["version"],"inputs":load["inputs"],
        "outputs":load["outputs"],"input":load["input"],"outputEnums":load["outputEnums"],
    }))
}

/// A retained snapshot's kind; snapshots written before kinds existed are Mutations.
fn retained_kind(snapshot: &Value) -> Result<axton_core::CallKind, String> {
    match snapshot.get("kind") {
        None => Ok(axton_core::CallKind::Mutation),
        Some(kind) => serde_json::from_value(kind.clone())
            .map_err(|_| format!("unsupported retained operation kind {kind}")),
    }
}
fn kind_name(kind: axton_core::CallKind) -> &'static str {
    match kind {
        axton_core::CallKind::Mutation => "mutation",
        axton_core::CallKind::Query => "query",
    }
}

fn action_inputs_compatible(old: &Value, new: &Value) -> Result<bool, String> {
    let previous = list(old, "inputs")?;
    let next = list(new, "inputs")?;
    if previous.len() != next.len() {
        return Ok(false);
    }
    for (left, right) in previous.iter().zip(next) {
        if left["kind"] != right["kind"] {
            return Ok(false);
        }
        if left["kind"] == "value" && left != right {
            return Ok(false);
        }
    }
    let slots = |inputs: &[Value]| {
        inputs
            .iter()
            .filter(|x| x["kind"] == "model")
            .map(|x| {
                let mut slot = x.clone();
                slot.as_object_mut().unwrap().remove("kind");
                slot
            })
            .collect::<Vec<_>>()
    };
    let mut old_mutation = old.clone();
    let mut new_mutation = new.clone();
    old_mutation["slots"] = json!(slots(previous));
    new_mutation["slots"] = json!(slots(next));
    compatible(&old_mutation, &new_mutation)
}

fn capture_action(config: &Value, action: &Value) -> Result<Value, String> {
    let inputs = list(action, "inputs")?;
    let mut outputs = list(action, "outputs")?.clone();
    if let Some(models) = config["backendModels"].as_array() {
        for output in &mut outputs {
            if output["kind"] == "model" {
                let model = models
                    .iter()
                    .find(|model| {
                        model["name"] == output["model"]
                            && model["version"] == output["modelReadVersion"]
                    })
                    .ok_or_else(|| {
                        format!(
                            "operation output {} has no retained Model read contract",
                            output["name"]
                        )
                    })?;
                output["modelReadVersion"] = model["version"].clone();
            }
        }
    }
    let slots: Vec<Value> = inputs
        .iter()
        .filter(|input| input["kind"] == "model")
        .map(|input| {
            let mut slot = input.clone();
            slot.as_object_mut().unwrap().remove("kind");
            slot
        })
        .collect();
    let operand = capture(
        config,
        &json!({"name":action["name"],"version":action["version"],"slots":slots,"sequence":action["sequence"]}),
    )?;
    let referenced = |values: &[Value]| -> Vec<Value> {
        let names: std::collections::BTreeSet<_> = values
            .iter()
            .filter(|v| v["type"]["kind"] == "enum")
            .filter_map(|v| v["type"]["name"].as_str())
            .collect();
        config["schema"]["enums"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["name"].as_str().is_some_and(|n| names.contains(n)))
            .cloned()
            .collect()
    };
    let input_enums = referenced(inputs);
    let output_enums = referenced(&outputs);
    let mut input = operand["input"].clone();
    input["enums"].as_array_mut().unwrap().extend(input_enums);
    let kind = retained_kind(action)?;
    Ok(json!({
        "name":action["name"],"version":action["version"],"kind":kind,"inputs":inputs,"outputs":outputs,
        "input":input,"outputEnums":output_enums,
        "requirements":operand["requirements"],"prerequisites":operand["prerequisites"],
        "sequence":action["sequence"],
    }))
}
/// Preserve historical mutation inputs independently of today's storage schema.
pub fn reconcile_history(current: &Value, history: Option<&Value>) -> Result<Value, String> {
    let mut result = history
        .cloned()
        .unwrap_or(json!({"formatVersion":1,"mutations":{}}));
    if result["formatVersion"] != 1 || !result["mutations"].is_object() {
        return Err("unsupported mutation history format".into());
    }
    let mutations = list(current, "mutations")?;
    for name in result["mutations"].as_object().unwrap().keys() {
        if !mutations.iter().any(|m| m["name"] == *name) {
            return Err(format!("retained mutation {name} cannot be removed"));
        }
    }
    for mutation in mutations {
        let name = mutation["name"].as_str().ok_or("unnamed mutation")?;
        let version = mutation["version"]
            .as_u64()
            .ok_or("invalid mutation version")?;
        let snapshot = capture(current, mutation)?;
        let versions = result["mutations"]
            .as_object_mut()
            .unwrap()
            .entry(name)
            .or_insert(json!({}))
            .as_object_mut()
            .ok_or("invalid history versions")?;
        let latest = versions
            .keys()
            .map(|v| v.parse::<u64>().map_err(|_| "invalid retained version"))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .max()
            .unwrap_or(0);
        if version < latest {
            return Err(format!("{name}: version cannot decrease from {latest}"));
        }
        if let Some(previous) = versions.get(&version.to_string())
            && !compatible(previous, &snapshot)?
        {
            return Err(format!(
                "{name} v{version}: incompatible input change; increase @@version"
            ));
        }
        versions.insert(version.to_string(), snapshot);
    }
    // A retained input may still invoke a prerequisite after a version upgrade.
    for versions in result["mutations"].as_object().unwrap().values() {
        for snapshot in versions
            .as_object()
            .ok_or("invalid history versions")?
            .values()
        {
            for requirement in snapshot["requirements"].as_array().unwrap_or(&vec![]) {
                let declaration = current["prerequisites"]
                    .as_array()
                    .and_then(|d| named(d, &requirement["name"]));
                let retained = snapshot["prerequisites"]
                    .as_array()
                    .and_then(|d| named(d, &requirement["name"]));
                if declaration != retained {
                    return Err(format!(
                        "retained mutation still requires original prerequisite {}",
                        requirement["name"]
                    ));
                }
            }
        }
    }
    Ok(result)
}
fn capture(config: &Value, mutation: &Value) -> Result<Value, String> {
    let slots = list(mutation, "slots")?;
    let mut models = vec![];
    let mut enum_names = std::collections::BTreeSet::new();
    let mut known = serde_json::Map::new();
    for model in list(&config["schema"], "models")? {
        let used: Vec<_> = slots
            .iter()
            .filter(|s| s["model"] == model["name"])
            .collect();
        if used.is_empty() {
            continue;
        }
        let identity = list(model, "identity")?;
        let fields = list(model, "fields")?;
        known.insert(
            model["name"].as_str().unwrap().into(),
            json!(fields.iter().map(|f| f["name"].clone()).collect::<Vec<_>>()),
        );
        let selected: Vec<_> = shapes(fields)
            .into_iter()
            .filter(|f| {
                identity.contains(&f["name"])
                    || used.iter().any(|s| {
                        s["operation"] == "create"
                            || (s["operation"] == "update"
                                && s["allowedPatchFields"]
                                    .as_array()
                                    .is_none_or(|a| a.contains(&f["name"])))
                    })
            })
            .collect();
        for field in &selected {
            if field["type"]["kind"] == "enum" {
                enum_names.insert(field["type"]["name"].as_str().unwrap().to_string());
            }
        }
        models.push(json!({"name":model["name"],"identity":identity,"fields":selected}));
    }
    let enums: Vec<_> = list(&config["schema"], "enums")?
        .iter()
        .filter(|e| enum_names.contains(e["name"].as_str().unwrap()))
        .cloned()
        .collect();
    let requirements: Vec<_> = config["requirements"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter(|r| {
            models.iter().any(|m| {
                m["name"] == r["model"]
                    && m["fields"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|f| f["name"] == r["field"])
            })
        })
        .cloned()
        .collect();
    let prerequisites: Vec<_> = config["prerequisites"]
        .as_array()
        .unwrap_or(&vec![])
        .iter()
        .filter(|p| requirements.iter().any(|r| r["name"] == p["name"]))
        .cloned()
        .collect();
    Ok(
        json!({"name":mutation["name"],"version":mutation["version"],"slots":slots,"input":{"models":models,"enums":enums},"knownFields":known,"requirements":requirements,"prerequisites":prerequisites,"sequence":mutation["sequence"]}),
    )
}
fn compatible(old: &Value, new: &Value) -> Result<bool, String> {
    let a = list(old, "slots")?;
    let b = list(new, "slots")?;
    if a.len() != b.len() {
        return Ok(false);
    }
    for (a, b) in a.iter().zip(b) {
        let mut left = a.clone();
        let mut right = b.clone();
        let old_patch = left
            .as_object_mut()
            .ok_or("invalid slot")?
            .remove("allowedPatchFields");
        let new_patch = right
            .as_object_mut()
            .ok_or("invalid slot")?
            .remove("allowedPatchFields");
        if left != right {
            return Ok(false);
        }
        if let Some(old_patch) = old_patch
            && !old_patch
                .as_array()
                .ok_or("invalid patch fields")?
                .iter()
                .all(|f| {
                    new_patch
                        .as_ref()
                        .and_then(Value::as_array)
                        .is_some_and(|a| a.contains(f))
                })
        {
            return Ok(false);
        }
    }
    for en in list(&old["input"], "enums")? {
        let Some(next) = named(list(&new["input"], "enums")?, &en["name"]) else {
            return Ok(false);
        };
        if !list(en, "values")?
            .iter()
            .all(|v| next["values"].as_array().unwrap().contains(v))
        {
            return Ok(false);
        }
    }
    for model in list(&old["input"], "models")? {
        let Some(next) = named(list(&new["input"], "models")?, &model["name"]) else {
            return Ok(false);
        };
        if model["identity"] != next["identity"] {
            return Ok(false);
        }
        for field in list(model, "fields")? {
            if same_shape(list(next, "fields")?, field) != Some(true) {
                return Ok(false);
            }
        }
        if b.iter()
            .any(|s| s["model"] == model["name"] && s["operation"] == "create")
        {
            for field in list(next, "fields")? {
                if named(list(model, "fields")?, &field["name"]).is_none()
                    && field["nullable"] != true
                {
                    return Ok(false);
                }
            }
        }
    }
    if old["requirements"] != new["requirements"] || old["sequence"] != new["sequence"] {
        return Ok(false);
    }
    Ok(true)
}
/// Preserve every published model read contract, independently of mutation
/// history and of today's storage schema ([#91](https://github.com/zanminwang/axton/issues/91)).
///
/// A snapshot is `{name, version, identity, fields, enums}`: the record
/// structure a loader of that version returns, with the definitions of the
/// enums those fields use as they were when the version was published.
/// A compatible change (an added nullable field) updates the version's
/// snapshot; a breaking change (a required field, a renamed, removed or
/// retyped field, a changed enum) needs a higher `@@version`, and the old
/// snapshot stays untouched.
pub fn reconcile_model_history(current: &Value, history: Option<&Value>) -> Result<Value, String> {
    let mut result = history
        .cloned()
        .unwrap_or(json!({"formatVersion":1,"models":{}}));
    if result["formatVersion"] != 1 || !result["models"].is_object() {
        return Err("unsupported model history format".into());
    }
    let models = list(&current["schema"], "models")?;
    for name in result["models"].as_object().unwrap().keys() {
        if !models.iter().any(|m| m["name"] == *name) {
            return Err(format!("retained model {name} cannot be removed"));
        }
    }
    for model in models {
        let name = model["name"].as_str().ok_or("unnamed model")?;
        let version = model["version"].as_u64().ok_or("invalid model version")?;
        let snapshot = capture_model(current, model)?;
        let versions = result["models"]
            .as_object_mut()
            .unwrap()
            .entry(name)
            .or_insert(json!({}))
            .as_object_mut()
            .ok_or("invalid history versions")?;
        let mut retained: Vec<u64> = versions
            .keys()
            .map(|v| v.parse::<u64>().map_err(|_| "invalid retained version"))
            .collect::<Result<_, _>>()?;
        retained.sort_unstable();
        if let Some(latest) = retained.last()
            && version < *latest
        {
            return Err(format!("{name}: version cannot decrease from {latest}"));
        }
        // Identity is the record key in every table and message; changing it is
        // not a read-contract change and no version permits it yet.
        for previous in versions.values() {
            if previous["identity"] != snapshot["identity"] {
                return Err(format!(
                    "{name}: identity change is not supported; a model version describes the record structure, not its identity"
                ));
            }
        }
        if let Some(previous) = versions.get(&version.to_string())
            && let Some(reason) = model_break(previous, &snapshot)?
        {
            return Err(format!(
                "{name} v{version}: {reason} changes the read contract; increase @@version and keep the old loader"
            ));
        }
        versions.insert(version.to_string(), snapshot);
    }
    Ok(result)
}
fn capture_model(config: &Value, model: &Value) -> Result<Value, String> {
    let fields = list(model, "fields")?;
    let used: std::collections::BTreeSet<&str> = fields
        .iter()
        .filter(|f| f["type"]["kind"] == "enum")
        .filter_map(|f| f["type"]["name"].as_str())
        .collect();
    let enums: Vec<_> = list(&config["schema"], "enums")?
        .iter()
        .filter(|e| e["name"].as_str().is_some_and(|n| used.contains(n)))
        .cloned()
        .collect();
    Ok(json!({
        "name": model["name"],
        "version": model["version"],
        "identity": model["identity"],
        "fields": shapes(fields),
        "enums": enums,
    }))
}
/// The first rule an old reader of `old` would trip over when served `new`,
/// or `None` when every difference is an added nullable field.
fn model_break(old: &Value, new: &Value) -> Result<Option<String>, String> {
    for field in list(old, "fields")? {
        match named(list(new, "fields")?, &field["name"]) {
            None => {
                return Ok(Some(format!(
                    "removing or renaming field {}",
                    field["name"]
                )));
            }
            Some(next) if without_creation_policy(next) != without_creation_policy(field) => {
                return Ok(Some(format!(
                    "changing the type of field {}",
                    field["name"]
                )));
            }
            Some(_) => {}
        }
    }
    for field in list(new, "fields")? {
        if named(list(old, "fields")?, &field["name"]).is_none() && field["nullable"] != true {
            return Ok(Some(format!("adding required field {}", field["name"])));
        }
    }
    for en in list(old, "enums")? {
        // An enum an old reader knows must keep exactly the values it knows:
        // a new value is one it cannot interpret.
        if named(list(new, "enums")?, &en["name"]) != Some(en) {
            return Ok(Some(format!("changing the values of enum {}", en["name"])));
        }
    }
    Ok(None)
}
/// Preserve the reference published model/field-name fence. Stronger type/identity fences are deferred.
/// A model whose version increased is checked by [`reconcile_model_history`]
/// instead: the fence keeps its published field names only while the read
/// contract is the same version.
pub fn check_fence(before: &Value, after: &Value) -> Result<(), String> {
    for model in list(before, "models")? {
        let next = named(list(after, "models")?, &model["name"])
            .ok_or_else(|| format!("schema fence: published model {} removed", model["name"]))?;
        let version = |m: &Value| m.get("version").and_then(Value::as_u64).unwrap_or(1);
        if version(next) > version(model) {
            continue;
        }
        for field in list(model, "fields")? {
            named(list(next, "fields")?, &field["name"]).ok_or_else(|| {
                format!(
                    "schema fence: published field {}.{} removed",
                    model["name"], field["name"]
                )
            })?;
        }
    }
    Ok(())
}
