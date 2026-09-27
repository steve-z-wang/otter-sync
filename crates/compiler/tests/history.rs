use axton_compiler::{check_fence, compile, reconcile_history};
use axton_compiler::{
    reconcile_action_history, reconcile_load_history, reconcile_model_history as reconcile_models,
};
use serde_json::json;

#[test]
fn new_action_names_must_start_at_version_one() {
    let model = "model Todo { id String @@id(id) }";
    let first_at_v2 = compile(&format!(
        "{model} @version(2) mutation Send(to String) {{ id String }}"
    ))
    .unwrap();
    let error = reconcile_action_history(&first_at_v2, None).unwrap_err();
    assert!(error.contains("begin at version 1"), "{error}");

    let existing = compile(&format!("{model} mutation Save(to String) {{ id String }}")).unwrap();
    let history = reconcile_action_history(&existing, None).unwrap();
    let second_at_v2 = compile(&format!(
        "{model} mutation Save(to String) {{ id String }} @version(2) mutation Send(to String) {{ id String }}"
    ))
    .unwrap();
    let error = reconcile_action_history(&second_at_v2, Some(&history)).unwrap_err();
    assert!(
        error.contains("Send") && error.contains("begin at version 1"),
        "{error}"
    );

    let second_at_v1 = compile(&format!(
        "{model} mutation Save(to String) {{ id String }} mutation Send(to String) {{ id String }}"
    ))
    .unwrap();
    let next = reconcile_action_history(&second_at_v1, Some(&history)).unwrap();
    assert_eq!(
        next["actions"]["Save"]["1"],
        history["actions"]["Save"]["1"]
    );
    assert_eq!(next["actions"]["Send"]["1"]["version"], 1);
}

#[test]
fn actions_retain_output_shapes_and_model_read_versions() {
    let first =
        compile("model Todo { id String @@id(id) } mutation Find(query String?) { related Todo? }")
            .unwrap();
    let history = reconcile_action_history(&first, None).unwrap();
    assert_eq!(
        history["actions"]["Find"]["1"]["inputs"][0]["required"],
        true
    );
    assert_eq!(
        history["actions"]["Find"]["1"]["outputs"][0]["modelReadVersion"],
        1
    );
    assert_eq!(
        history["actions"]["Find"]["1"]["outputs"][0]["handlerType"],
        json!({"kind":"identity","model":"Todo","fields":[{"name":"id","type":{"kind":"scalar","name":"string"}}]})
    );
    let changed = compile(
        "model Todo { id String @@id(id) } mutation Find(query String?) { related String? }",
    )
    .unwrap();
    assert!(
        reconcile_action_history(&changed, Some(&history))
            .unwrap_err()
            .contains("output")
    );
    let bumped = compile("model Todo { id String @@id(id) } @version(2) mutation Find(query String?) { related String? }").unwrap();
    let next = reconcile_action_history(&bumped, Some(&history)).unwrap();
    assert_eq!(
        next["actions"]["Find"]["1"],
        history["actions"]["Find"]["1"]
    );
    assert_eq!(next["actions"]["Find"]["2"]["outputs"][0]["kind"], "value");
}

#[test]
fn action_history_fences_input_shapes_and_preserves_compatible_model_additions() {
    let source = "model Todo { id String title String body String @@id(id) } mutation Edit(value String?, todo Todo.update<title,body>) { result String }";
    let history = reconcile_action_history(&compile(source).unwrap(), None).unwrap();
    for changed in [
        source.replace("value String?", "value String"),
        source.replace("value String?", "value String[]"),
        source.replace("todo Todo.update<title,body>", "todo Todo.update<title>"),
        source.replace("result String", "result String?"),
        source.replace("result String", "result String[]"),
        source.replace("result String", "other String"),
    ] {
        assert!(
            reconcile_action_history(&compile(&changed).unwrap(), Some(&history)).is_err(),
            "{changed}"
        );
    }
    let compatible = source.replace("title String", "title String note String?");
    assert!(reconcile_action_history(&compile(&compatible).unwrap(), Some(&history)).is_ok());
    let bumped = source.replace("mutation Edit", "@version(2) mutation Edit");
    let newer = reconcile_action_history(&compile(&bumped).unwrap(), Some(&history)).unwrap();
    assert!(
        reconcile_action_history(&compile(source).unwrap(), Some(&newer))
            .unwrap_err()
            .contains("cannot decrease")
    );
    assert!(
        reconcile_action_history(
            &compile("model Todo { id String @@id(id) }").unwrap(),
            Some(&history)
        )
        .unwrap_err()
        .contains("cannot be removed")
    );
}

#[test]
fn action_identity_source_changes_need_a_version_bump() {
    let first =
        compile("model Todo { id String @@id(id) } mutation Find(todo Todo.update) { todo Todo? }")
            .unwrap();
    let history = reconcile_action_history(&first, None).unwrap();
    let value = compile(
        "model Todo { id String @@id(id) } mutation Find(todo Todo.update) { todo String? }",
    )
    .unwrap();
    assert!(
        reconcile_action_history(&value, Some(&history))
            .unwrap_err()
            .contains("output")
    );
}

/// #140 removed the implicit same-name result of a Model operand. A history
/// retained by an earlier compiler still carries that `inputIdentity` output,
/// so the same source at an unchanged version is an output contract change.
#[test]
fn retained_implicit_operand_outputs_are_not_rewritten_at_an_unchanged_version() {
    let model = "model Todo { id String title String @@id(id) }";
    let current = compile(&format!("{model} mutation Edit(todo Todo.update)")).unwrap();
    let mut external = reconcile_action_history(&current, None).unwrap();
    external["actions"]["Edit"]["1"]["outputs"] = json!([{
        "cardinality":"single","kind":"model","model":"Todo","modelReadVersion":1,
        "name":"todo","source":{"inputIdentity":"todo"}
    }]);
    assert_eq!(
        reconcile_action_history(&current, Some(&external)).unwrap_err(),
        "Edit v1: incompatible output change; increase @version"
    );
    let bumped = compile(&format!(
        "{model} @version(2) mutation Edit(todo Todo.update)"
    ))
    .unwrap();
    let next = reconcile_action_history(&bumped, Some(&external)).unwrap();
    assert_eq!(
        next["actions"]["Edit"]["1"],
        external["actions"]["Edit"]["1"]
    );
    assert_eq!(next["actions"]["Edit"]["2"]["outputs"], json!([]));
}

#[test]
fn action_output_enum_changes_are_fenced_but_input_enum_growth_is_compatible() {
    let first = compile("enum Status { open closed } model Todo { id String @@id(id) } mutation Pick(input Status) { output Status }").unwrap();
    let history = reconcile_action_history(&first, None).unwrap();
    let changed = compile("enum Status { open closed archived } model Todo { id String @@id(id) } mutation Pick(input Status) { output Status }").unwrap();
    assert!(
        reconcile_action_history(&changed, Some(&history))
            .unwrap_err()
            .contains("output")
    );
    let bumped = compile("enum Status { open closed archived } model Todo { id String @@id(id) } @version(2) mutation Pick(input Status) { output Status }").unwrap();
    let next = reconcile_action_history(&bumped, Some(&history)).unwrap();
    assert_eq!(
        next["actions"]["Pick"]["1"]["outputEnums"][0]["values"],
        json!(["open", "closed"])
    );
    assert_eq!(
        next["actions"]["Pick"]["2"]["outputEnums"][0]["values"],
        json!(["open", "closed", "archived"])
    );
    let input_only = compile(
        "enum Status { open closed } model Todo { id String @@id(id) } mutation Filter(input Status)",
    )
    .unwrap();
    let input_history = reconcile_action_history(&input_only, None).unwrap();
    let input_expanded = compile("enum Status { open closed archived } model Todo { id String @@id(id) } mutation Filter(input Status)").unwrap();
    assert!(reconcile_action_history(&input_expanded, Some(&input_history)).is_ok());
}

#[test]
fn retained_action_prerequisites_remain_available_after_a_version_bump() {
    let source = "prerequisite Uploaded(key String) model Todo { id String title String @requires(Uploaded(key: self)) @@id(id) } mutation Save(todo Todo.create)";
    let history = reconcile_action_history(&compile(source).unwrap(), None).unwrap();
    let changed = "model Todo { id String title String @@id(id) } @version(2) mutation Save(todo Todo.create)";
    let error = reconcile_action_history(&compile(changed).unwrap(), Some(&history)).unwrap_err();
    assert!(
        error.contains("retained operation") && error.contains("Uploaded"),
        "{error}"
    );
}
#[test]
fn versions_retain_original_inputs() {
    let v1 =
        compile("model A { id UUID title String @@id(id) } mutation Save { a A.create }").unwrap();
    let history = reconcile_history(&v1, None).unwrap();
    let changed =
        compile("model A { id UUID title String count Int @@id(id) } mutation Save { a A.create }")
            .unwrap();
    assert!(reconcile_history(&changed, Some(&history)).is_err());
    let v2=compile("model A { id UUID title String count Int @@id(id) } mutation Save { a A.create @@version(2) }").unwrap();
    let next = reconcile_history(&v2, Some(&history)).unwrap();
    assert_eq!(
        next["mutations"]["Save"]["1"]["input"]["models"][0]["fields"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert!(reconcile_history(&v1, Some(&next)).is_err());
}
#[test]
fn nullable_addition_compatible_and_fence_blocks_removal() {
    let before =
        compile("model A { id UUID title String @@id(id) } mutation Save { a A.create }").unwrap();
    let history = reconcile_history(&before, None).unwrap();
    let after = compile(
        "model A { id UUID title String note String? @@id(id) } mutation Save { a A.create }",
    )
    .unwrap();
    assert!(reconcile_history(&after, Some(&history)).is_ok());
    assert!(check_fence(&before["schema"], &after["schema"]).is_ok());
    assert!(check_fence(&after["schema"], &before["schema"]).is_err());
}

mod models {
    use axton_compiler::{check_fence, compile, reconcile_model_history};
    use serde_json::{Value, json};

    fn task(fields: &str, version: &str) -> Value {
        compile(&format!(
            "enum Status {{ open closed }} model Task {{ id UUID {fields} @@id(id) {version} }}"
        ))
        .unwrap()
    }
    fn snapshot<'a>(history: &'a Value, model: &str, version: u64) -> &'a Value {
        &history["models"][model][version.to_string()]
    }
    fn field_names(snapshot: &Value) -> Vec<&str> {
        snapshot["fields"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| f["name"].as_str().unwrap())
            .collect()
    }

    #[test]
    fn omitted_version_is_version_one_and_the_snapshot_keeps_the_contract() {
        let implicit = task("title String status Status", "");
        let explicit = task("title String status Status", "@@version(1)");
        assert_eq!(implicit["schema"]["models"][0]["version"], 1);
        assert_eq!(implicit["schema"], explicit["schema"]);
        let history = reconcile_model_history(&implicit, None).unwrap();
        assert_eq!(history["formatVersion"], 1);
        let v1 = snapshot(&history, "Task", 1);
        assert_eq!(v1["name"], "Task");
        assert_eq!(v1["version"], 1);
        assert_eq!(v1["identity"], json!(["id"]));
        assert_eq!(field_names(v1), ["id", "title", "status"]);
        assert_eq!(
            v1["fields"][2]["type"],
            json!({"kind":"enum","name":"Status"})
        );
        assert_eq!(
            v1["enums"],
            json!([{"name":"Status","values":["open","closed"]}])
        );
        assert_eq!(
            reconcile_model_history(&explicit, Some(&history)).unwrap(),
            history,
            "explicit v1 is the same contract"
        );
    }

    #[test]
    fn nullable_addition_keeps_the_version_and_updates_its_contract() {
        let history = reconcile_model_history(&task("title String", ""), None).unwrap();
        let next = reconcile_model_history(&task("title String note String?", ""), Some(&history))
            .unwrap();
        assert_eq!(
            field_names(snapshot(&next, "Task", 1)),
            ["id", "title", "note"]
        );
        assert_eq!(next["models"]["Task"].as_object().unwrap().len(), 1);
    }

    #[test]
    fn breaking_output_changes_need_a_new_version_and_keep_the_old_contract() {
        let history =
            reconcile_model_history(&task("title String status Status", ""), None).unwrap();
        let breaking = [
            ("required field", "title String status Status count Int"),
            ("removed field", "status Status"),
            ("renamed field", "name String status Status"),
            ("changed type", "title Int status Status"),
        ];
        for (rule, fields) in breaking {
            let e = reconcile_model_history(&task(fields, ""), Some(&history)).unwrap_err();
            assert!(e.contains("Task v1"), "{rule}: {e}");
            assert!(e.contains("increase @@version"), "{rule}: {e}");
            let next = reconcile_model_history(&task(fields, "@@version(2)"), Some(&history))
                .unwrap_or_else(|e| panic!("{rule} with a bump: {e}"));
            assert_eq!(
                snapshot(&next, "Task", 1),
                snapshot(&history, "Task", 1),
                "{rule}: v1 stays as published"
            );
            assert_eq!(snapshot(&next, "Task", 2)["version"], 2);
        }
    }

    #[test]
    fn a_new_enum_value_needs_a_new_version_and_never_reaches_the_old_contract() {
        let v1 =
            compile("enum Status { open closed } model Task { id UUID status Status @@id(id) }")
                .unwrap();
        let history = reconcile_model_history(&v1, None).unwrap();
        let expanded = compile(
            "enum Status { open closed archived } model Task { id UUID status Status @@id(id) }",
        )
        .unwrap();
        let e = reconcile_model_history(&expanded, Some(&history)).unwrap_err();
        assert!(e.contains("Task v1") && e.contains("Status"), "{e}");
        let bumped = compile("enum Status { open closed archived } model Task { id UUID status Status @@id(id) @@version(2) }").unwrap();
        let next = reconcile_model_history(&bumped, Some(&history)).unwrap();
        assert_eq!(
            snapshot(&next, "Task", 1)["enums"],
            json!([{"name":"Status","values":["open","closed"]}])
        );
        assert_eq!(
            snapshot(&next, "Task", 2)["enums"],
            json!([{"name":"Status","values":["open","closed","archived"]}])
        );
        // A model that does not use the enum is untouched by its expansion.
        let other = compile("enum Status { open closed } model Task { id UUID status Status @@id(id) } model Note { id UUID text String @@id(id) }").unwrap();
        let history = reconcile_model_history(&other, None).unwrap();
        assert_eq!(snapshot(&history, "Note", 1)["enums"], json!([]));
        let other_expanded = compile("enum Status { open closed archived } model Task { id UUID status Status @@id(id) @@version(2) } model Note { id UUID text String @@id(id) }").unwrap();
        assert!(reconcile_model_history(&other_expanded, Some(&history)).is_ok());
    }

    #[test]
    fn identity_changes_are_refused_at_any_version() {
        let history = reconcile_model_history(&task("title String", ""), None).unwrap();
        let same = compile("model Task { id UUID title String @@id(id, title) }").unwrap();
        let bumped =
            compile("model Task { id UUID title String @@id(id, title) @@version(2) }").unwrap();
        for changed in [same, bumped] {
            let e = reconcile_model_history(&changed, Some(&history)).unwrap_err();
            assert!(e.contains("identity"), "{e}");
        }
    }

    #[test]
    fn history_misuse_follows_the_mutation_rules() {
        let history = reconcile_model_history(&task("title String", "@@version(2)"), None).unwrap();
        let decreased =
            reconcile_model_history(&task("title String", ""), Some(&history)).unwrap_err();
        assert!(decreased.contains("cannot decrease from 2"), "{decreased}");
        let removed = compile("model Other { id UUID @@id(id) }").unwrap();
        let e = reconcile_model_history(&removed, Some(&history)).unwrap_err();
        assert!(e.contains("retained model Task cannot be removed"), "{e}");
        for broken in [
            json!({"formatVersion":2,"models":{}}),
            json!({"formatVersion":1}),
            json!({"formatVersion":1,"models":{"Task":[]}}),
        ] {
            assert!(
                reconcile_model_history(&task("title String", "@@version(2)"), Some(&broken))
                    .is_err(),
                "{broken}"
            );
        }
        // A jump is allowed, like a mutation version.
        assert!(
            reconcile_model_history(&task("title String", "@@version(5)"), Some(&history)).is_ok()
        );
    }

    #[test]
    fn the_fence_defers_to_the_history_for_a_bumped_model() {
        let before = task("title String note String", "");
        let after = task("title String", "@@version(2)");
        assert!(
            check_fence(&before["schema"], &after["schema"]).is_ok(),
            "a bumped model may drop a field; its old contract is retained"
        );
        let unbumped = task("title String", "");
        assert!(check_fence(&before["schema"], &unbumped["schema"]).is_err());
        let gone = compile("model Other { id UUID @@id(id) }").unwrap();
        assert!(
            check_fence(&before["schema"], &gone["schema"]).is_err(),
            "a published model never disappears"
        );
    }
}

#[test]
fn retained_operation_kind_is_part_of_each_versioned_contract() {
    let model = "model Todo { id String @@id(id) }";
    let first = compile(&format!(
        "{model} mutation Find(text String) {{ id String }}"
    ))
    .unwrap();
    let history = reconcile_action_history(&first, None).unwrap();
    assert_eq!(history["actions"]["Find"]["1"]["kind"], "mutation");

    // A snapshot written before kinds existed is a Mutation.
    let mut legacy = history.clone();
    legacy["actions"]["Find"]["1"]
        .as_object_mut()
        .unwrap()
        .remove("kind");
    let next = reconcile_action_history(&first, Some(&legacy)).unwrap();
    assert_eq!(
        next["actions"]["Find"]["1"],
        history["actions"]["Find"]["1"]
    );

    // Reclassifying a published version is refused, in either direction.
    let query = compile(&format!("{model} query Find(text String) {{ id String }}")).unwrap();
    for previous in [&history, &legacy] {
        let error = reconcile_action_history(&query, Some(previous)).unwrap_err();
        assert!(
            error.contains("Find v1") && error.contains("kind changed from mutation to query"),
            "{error}"
        );
    }
    let queried = reconcile_action_history(
        &compile(&format!("{model} query Look(text String) {{ id String }}")).unwrap(),
        None,
    )
    .unwrap();
    let back = compile(&format!(
        "{model} mutation Look(text String) {{ id String }}"
    ))
    .unwrap();
    assert!(
        reconcile_action_history(&back, Some(&queried))
            .unwrap_err()
            .contains("kind changed from query to mutation")
    );
    let mut unknown = history.clone();
    unknown["actions"]["Find"]["1"]["kind"] = json!("action");
    assert!(
        reconcile_action_history(&first, Some(&unknown))
            .unwrap_err()
            .contains("unsupported retained operation kind")
    );

    // A new version may change kind; the old version keeps its own.
    let bumped = compile(&format!(
        "{model} @version(2) query Find(text String) {{ id String }}"
    ))
    .unwrap();
    let next = reconcile_action_history(&bumped, Some(&history)).unwrap();
    assert_eq!(next["actions"]["Find"]["1"]["kind"], "mutation");
    assert_eq!(next["actions"]["Find"]["2"]["kind"], "query");
    let retained = next["actions"]["Find"]["1"].clone();
    let old: axton_core::ActionDescriptor = serde_json::from_value(retained).unwrap();
    assert_eq!(old.kind, axton_core::CallKind::Mutation);
}

#[test]
fn delivery_route_and_store_never_enter_operation_history() {
    let source = "model Todo { id String @@id(id) } query Find(text String) { todo Todo? }";
    let history = reconcile_action_history(&compile(source).unwrap(), None).unwrap();
    let snapshot = history["actions"]["Find"]["1"].as_object().unwrap();
    for key in ["store", "delivery", "route", "direct", "enqueue"] {
        assert!(!snapshot.contains_key(key), "{key}");
    }
    // Recompiling the same contract is a no-op, whatever route callers use.
    assert_eq!(
        reconcile_action_history(&compile(source).unwrap(), Some(&history)).unwrap(),
        history
    );
}

/// Creation policy is not part of any retained contract: changing, adding or
/// removing a default on an existing field keeps every version, while a new
/// required field stays a structural change even with a default (#27).
#[test]
fn default_only_changes_keep_retained_versions_and_required_fields_still_break() {
    use axton_compiler::reconcile_model_history;
    let source = |title: &str, extra: &str| {
        format!(
            "model Todo {{\n id String{}\n title String {title}\n{extra} @@id(id)\n}}\nmutation Add {{ todo Todo.create }}\nmutation Save(todo Todo.create) {{ saved Todo }}\n",
            if title.is_empty() {
                ""
            } else {
                " @default(uuid())"
            }
        )
    };
    let v1 = compile(&source("@default(\"a\")", "")).unwrap();
    let models = reconcile_model_history(&v1, None).unwrap();
    let mutations = reconcile_history(&v1, None).unwrap();
    let mut with_models = v1.clone();
    with_models["backendModels"] = json!([models["models"]["Todo"]["1"]]);
    let actions = reconcile_action_history(&with_models, None).unwrap();
    let text = |v: &serde_json::Value| v.to_string();
    for retained in [&models, &mutations, &actions] {
        assert!(!text(retained).contains("createDefault"), "{retained}");
    }
    // Client resultModels describe complete backend values, not creation policy.
    assert!(!text(&v1["schema"]["resultModels"]).contains("createDefault"));
    assert!(text(&v1["schema"]["models"]).contains("createDefault"));
    for changed in [
        source("@default(\"b\")", ""),
        source("@default(uuid())", ""),
        source("", ""),
    ] {
        let next = compile(&changed).unwrap();
        let next_models = reconcile_model_history(&next, Some(&models)).unwrap();
        assert_eq!(next_models, models, "{changed}");
        assert_eq!(
            reconcile_history(&next, Some(&mutations)).unwrap(),
            mutations
        );
        let mut with_models = next.clone();
        with_models["backendModels"] = json!([next_models["models"]["Todo"]["1"]]);
        assert_eq!(
            reconcile_action_history(&with_models, Some(&actions)).unwrap(),
            actions,
            "{changed}"
        );
    }
    // A hand-authored snapshot that carries creation policy compares by shape.
    let mut authored = models.clone();
    authored["models"]["Todo"]["1"]["fields"][1]["createDefault"] =
        json!({"kind":"literal","value":"z"});
    reconcile_model_history(&v1, Some(&authored)).unwrap();
    // A new required field is a read-contract and input change, default or not.
    let required = compile(&source("@default(\"a\")", " rank Int @default(0)\n")).unwrap();
    let error = reconcile_model_history(&required, Some(&models)).unwrap_err();
    assert!(error.contains("adding required field"), "{error}");
    let error = reconcile_history(&required, Some(&mutations)).unwrap_err();
    assert!(error.contains("incompatible input change"), "{error}");
}

const LOAD_MODELS: &str = "enum Status { open done } model Todo { id UUID title String @@id(id) } model Note { id String @@id(id) }";

/// Reconcile model and Load history the way the CLI does, returning the
/// retained Load history and the merged client schema.
fn retain_loads(
    source: &str,
    models: Option<&serde_json::Value>,
    loads: Option<&serde_json::Value>,
) -> Result<(serde_json::Value, serde_json::Value, serde_json::Value), String> {
    let mut config = compile(source)?;
    let model_history = reconcile_models(&config, models)?;
    let retained: Vec<serde_json::Value> = model_history["models"]
        .as_object()
        .unwrap()
        .values()
        .flat_map(|v| v.as_object().unwrap().values().cloned())
        .collect();
    config["backendModels"] = json!(retained);
    let load_history = reconcile_load_history(&config, loads)?;
    let mut schema = config["schema"].clone();
    schema["resultModels"] = json!(retained);
    schema["loads"] = json!(
        load_history["loads"]
            .as_object()
            .unwrap()
            .values()
            .flat_map(|v| v.as_object().unwrap().values().cloned())
            .collect::<Vec<_>>()
    );
    axton_core::Schema::from_value(schema.clone()).map_err(|e| e.to_string())?;
    Ok((model_history, load_history, schema))
}

#[test]
fn load_history_mirrors_the_operation_envelope() {
    let source = format!("{LOAD_MODELS} load ProjectTodos(projectId UUID) {{ todos Todo[] }}");
    let config = compile(&source).unwrap();
    let history = reconcile_load_history(&config, None).unwrap();
    assert_eq!(history["formatVersion"], 1);
    assert_eq!(history["loads"]["ProjectTodos"]["1"], config["loads"][0]);
    assert_eq!(history.as_object().unwrap().len(), 2);
    // Reconciling again is a fixed point.
    assert_eq!(
        reconcile_load_history(&config, Some(&history)).unwrap(),
        history
    );
    let error =
        reconcile_load_history(&config, Some(&json!({"formatVersion":2,"loads":{}}))).unwrap_err();
    assert!(error.contains("unsupported load history format"), "{error}");
    let error = reconcile_load_history(&config, Some(&json!({"formatVersion":1,"actions":{}})))
        .unwrap_err();
    assert!(error.contains("unsupported load history format"), "{error}");
    let first_at_v2 = compile(&format!(
        "{LOAD_MODELS} @version(2) load Late() {{ todos Todo[] }}"
    ))
    .unwrap();
    let error = reconcile_load_history(&first_at_v2, None).unwrap_err();
    assert!(
        error.contains("Late: initial load history must begin at version 1"),
        "{error}"
    );
}

#[test]
fn same_version_breaking_load_changes_need_a_new_version() {
    let base = "load ProjectTodos(projectId UUID, status Status?) { todos Todo[] notes Note[] }";
    let source = format!("{LOAD_MODELS} {base}");
    let history = reconcile_load_history(&compile(&source).unwrap(), None).unwrap();
    for (changed, needle) in [
        (
            base.replace("projectId UUID", "projectId String"),
            "incompatible input change",
        ),
        (
            base.replace("status Status?", "status Status"),
            "incompatible input change",
        ),
        (
            base.replace("status Status?", "status Status?, extra Int"),
            "incompatible input change",
        ),
        (
            base.replace("projectId UUID, ", ""),
            "incompatible input change",
        ),
        (
            base.replace(" notes Note[]", ""),
            "incompatible output change",
        ),
        (
            base.replace("notes Note[]", "notes Todo[]"),
            "incompatible output change",
        ),
        (
            base.replace("todos Todo[]", "items Todo[]"),
            "incompatible output change",
        ),
    ] {
        let config = compile(&format!("{LOAD_MODELS} {changed}")).unwrap();
        let error = reconcile_load_history(&config, Some(&history)).unwrap_err();
        assert!(
            error.contains(&format!("ProjectTodos v1: {needle}; increase @version")),
            "{changed}: {error}"
        );
    }
    // A widened input enum is compatible: an old caller's values stay valid.
    let widened = LOAD_MODELS.replace("open done", "open done archived");
    let next = reconcile_load_history(
        &compile(&format!("{widened} {base}")).unwrap(),
        Some(&history),
    )
    .unwrap();
    assert_eq!(
        next["loads"]["ProjectTodos"]["1"]["input"]["enums"][0]["values"],
        json!(["open", "done", "archived"])
    );
    // A compatible Model addition does not touch the Load contract.
    let added = LOAD_MODELS.replace("title String", "title String note String?");
    assert_eq!(
        reconcile_load_history(
            &compile(&format!("{added} {base}")).unwrap(),
            Some(&history)
        )
        .unwrap(),
        history
    );
}

#[test]
fn load_version_two_retains_version_one_and_its_model_reader() {
    let v1 = format!("{LOAD_MODELS} load ProjectTodos(projectId UUID) {{ todos Todo[] }}");
    let (models, loads, _) = retain_loads(&v1, None, None).unwrap();
    // A breaking Model read change retypes the output contract: v1 is refused
    // until the Load version moves with it.
    let breaking = v1.replace(
        "model Todo { id UUID title String @@id(id) }",
        "@version(2) model Todo { id UUID title Int @@id(id) }",
    );
    let error = retain_loads(&breaking, Some(&models), Some(&loads)).unwrap_err();
    assert!(
        error.contains("ProjectTodos v1: incompatible output change"),
        "{error}"
    );
    let bumped = breaking.replace("load ProjectTodos", "@version(2) load ProjectTodos");
    let (_, next, schema) = retain_loads(&bumped, Some(&models), Some(&loads)).unwrap();
    assert_eq!(
        next["loads"]["ProjectTodos"]["1"],
        loads["loads"]["ProjectTodos"]["1"]
    );
    assert_eq!(
        next["loads"]["ProjectTodos"]["1"]["outputs"][0]["modelReadVersion"],
        1
    );
    assert_eq!(
        next["loads"]["ProjectTodos"]["2"]["outputs"][0]["modelReadVersion"],
        2
    );
    let schema = axton_core::Schema::from_value(schema).unwrap();
    assert!(schema.load("ProjectTodos", 1).is_ok());
    assert!(schema.load("ProjectTodos", 2).is_ok());
    assert!(schema.result_model("Todo", 1).is_ok());
    let error = retain_loads(
        &bumped.replace("@version(2) load", "load"),
        Some(&models),
        Some(&next),
    )
    .unwrap_err();
    assert!(error.contains("version cannot decrease from 2"), "{error}");
}

#[test]
fn retained_loads_cannot_be_removed_or_change_kind() {
    let load = format!("{LOAD_MODELS} load Find() {{ todos Todo[] }}");
    let history = reconcile_load_history(&compile(&load).unwrap(), None).unwrap();
    for replacement in [
        LOAD_MODELS.to_string(),
        format!("{LOAD_MODELS} query Find() {{ todos Todo[] }}"),
        format!("{LOAD_MODELS} mutation Find() {{ todos Todo[] }}"),
    ] {
        let error =
            reconcile_load_history(&compile(&replacement).unwrap(), Some(&history)).unwrap_err();
        assert!(
            error.contains("retained load Find cannot be removed"),
            "{error}"
        );
    }
    // A retained Query or Mutation cannot become a Load either.
    for kind in ["query", "mutation"] {
        let operation = compile(&format!("{LOAD_MODELS} {kind} Find() {{ n Int }}")).unwrap();
        let actions = reconcile_action_history(&operation, None).unwrap();
        let error = reconcile_action_history(&compile(&load).unwrap(), Some(&actions)).unwrap_err();
        assert!(
            error.contains("retained operation Find cannot be removed"),
            "{error}"
        );
    }
}

#[test]
fn adding_a_load_leaves_operation_and_mutation_history_unchanged() {
    let operations = format!(
        "{LOAD_MODELS} mutation Save {{ todo Todo.create }} mutation Rename(todo Todo.update) {{ n Int }} query Count(status Status?) {{ n Int }}"
    );
    let before = compile(&operations).unwrap();
    let actions = reconcile_action_history(&before, None).unwrap();
    let mutations = reconcile_history(&before, None).unwrap();
    let with_load = compile(&format!(
        "{operations} load ProjectTodos(status Status?) {{ todos Todo[] }}"
    ))
    .unwrap();
    assert_eq!(
        serde_json::to_string_pretty(
            &reconcile_action_history(&with_load, Some(&actions)).unwrap()
        )
        .unwrap(),
        serde_json::to_string_pretty(&actions).unwrap()
    );
    assert_eq!(
        serde_json::to_string_pretty(&reconcile_history(&with_load, Some(&mutations)).unwrap())
            .unwrap(),
        serde_json::to_string_pretty(&mutations).unwrap()
    );
    assert_eq!(with_load["actions"], before["actions"]);
}
