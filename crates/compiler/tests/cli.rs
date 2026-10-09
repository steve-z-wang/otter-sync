use std::{fs, process::Command};
#[test]
fn cli_action_history_versions_outputs_and_refuses_edits_atomically() {
    let (root, input) = workspace("action-history");
    let out = root.join("out");
    let model = input.join("test.model");
    let v1 = "model Todo { id String @@id(id) } mutation SendEmail(to String) { messageId String relatedTodo Todo? }";
    fs::write(&model, v1).unwrap();
    assert!(
        axton(&[input.as_os_str(), out.as_os_str()])
            .status
            .success()
    );
    let actions = input.join("history/actions.json");
    let before: Vec<_> = [
        actions.clone(),
        input.join("history/models.json"),
        input.join("history/mutations.json"),
        out.join("backend.json"),
        out.join("schema.json"),
        out.join("generated.ts"),
    ]
    .iter()
    .map(|p| fs::read(p).unwrap())
    .collect();
    fs::write(&model, v1.replace("messageId String", "messageId Int")).unwrap();
    let rejected = axton(&[input.as_os_str(), out.as_os_str()]);
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("output"));
    let after: Vec<_> = [
        actions.clone(),
        input.join("history/models.json"),
        input.join("history/mutations.json"),
        out.join("backend.json"),
        out.join("schema.json"),
        out.join("generated.ts"),
    ]
    .iter()
    .map(|p| fs::read(p).unwrap())
    .collect();
    assert_eq!(before, after);
    fs::write(
        &model,
        v1.replace(
            "model Todo { id String @@id(id) }",
            "@version(2) model Todo { id String title String @@id(id) }",
        )
        .replace("mutation SendEmail", "@version(2) mutation SendEmail")
        .replace("messageId String", "messageId Int"),
    )
    .unwrap();
    let result = axton(&[input.as_os_str(), out.as_os_str()]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let backend: serde_json::Value =
        serde_json::from_slice(&fs::read(out.join("backend.json")).unwrap()).unwrap();
    assert_eq!(backend["actions"].as_array().unwrap().len(), 2);
    assert_eq!(
        backend["actions"][0]["outputs"][0]["type"]["name"],
        "string"
    );
    assert_eq!(backend["actions"][1]["outputs"][0]["type"]["name"], "int");
    assert_eq!(backend["actions"][0]["outputs"][1]["modelReadVersion"], 1);
    assert_eq!(backend["actions"][1]["outputs"][1]["modelReadVersion"], 2);
    let client: serde_json::Value =
        serde_json::from_slice(&fs::read(out.join("schema.json")).unwrap()).unwrap();
    assert_eq!(client["actions"].as_array().unwrap().len(), 2);
    assert_eq!(client["resultModels"].as_array().unwrap().len(), 2);
    assert_eq!(client["actions"][0]["outputs"][1]["modelReadVersion"], 1);
    assert_eq!(client["resultModels"][0]["version"], 1);
    let retained = fs::read(&actions).unwrap();
    let generated = fs::read(out.join("backend.json")).unwrap();
    assert!(
        axton(&[input.as_os_str(), out.as_os_str()])
            .status
            .success()
    );
    assert_eq!(fs::read(&actions).unwrap(), retained);
    assert_eq!(fs::read(out.join("backend.json")).unwrap(), generated);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cli_compiles_ordinary_only_action_without_dummy_model() {
    let (root, input) = workspace("ordinary-action");
    let out = root.join("out");
    fs::write(
        input.join("test.model"),
        "mutation Send(to String) { messageId String }",
    )
    .unwrap();
    let run = axton(&[input.as_os_str(), out.as_os_str()]);
    assert!(
        run.status.success(),
        "{}",
        String::from_utf8_lossy(&run.stderr)
    );
    let client: serde_json::Value =
        serde_json::from_slice(&fs::read(out.join("schema.json")).unwrap()).unwrap();
    assert_eq!(client["models"], serde_json::json!([]));
    assert_eq!(client["actions"][0]["name"], "Send");
    assert!(axton_core::Schema::from_value(client).is_ok());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cli_rejects_new_action_names_above_v1_without_init_flags() {
    let (root, input) = workspace("new-action-version");
    let out = root.join("out");
    let model = input.join("test.model");
    let base = "model Todo { id String @@id(id) }";
    fs::write(
        &model,
        format!("{base} @version(2) mutation Send(to String) {{ id String }}"),
    )
    .unwrap();
    let rejected = axton(&[input.as_os_str(), out.as_os_str()]);
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("begin at version 1"));
    assert!(!input.join("history/actions.json").exists());
    assert!(!out.exists());

    fs::write(
        &model,
        format!("{base} mutation Save(to String) {{ id String }}"),
    )
    .unwrap();
    let first = axton(&[input.as_os_str(), out.as_os_str()]);
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let paths = [
        input.join("history/actions.json"),
        input.join("history/models.json"),
        input.join("history/mutations.json"),
        out.join("backend.json"),
        out.join("schema.json"),
        out.join("generated.ts"),
    ];
    let before: Vec<_> = paths.iter().map(|path| fs::read(path).unwrap()).collect();
    fs::write(
        &model,
        format!("{base} mutation Save(to String) {{ id String }} @version(2) mutation Send(to String) {{ id String }}"),
    )
    .unwrap();
    let rejected = axton(&[input.as_os_str(), out.as_os_str()]);
    assert!(!rejected.status.success());
    let stderr = String::from_utf8_lossy(&rejected.stderr);
    assert!(
        stderr.contains("Send") && stderr.contains("begin at version 1"),
        "{stderr}"
    );
    let after: Vec<_> = paths.iter().map(|path| fs::read(path).unwrap()).collect();
    assert_eq!(after, before);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cli_action_history_options_require_explicit_initialization() {
    let (root, input) = workspace("action-options");
    let out = root.join("out");
    let path = root.join("action-history.json");
    let model = input.join("test.model");
    fs::write(
        &model,
        "model Todo { id String @@id(id) } @version(2) mutation Send(to String) { id String }",
    )
    .unwrap();
    let args = [
        input.as_os_str(),
        out.as_os_str(),
        "--action-history".as_ref(),
        path.as_os_str(),
    ];
    let missing = axton(&args);
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("missing operation history"));
    let uninitialized = axton(&[
        input.as_os_str(),
        out.as_os_str(),
        "--action-history".as_ref(),
        path.as_os_str(),
        "--initialize-action-history".as_ref(),
    ]);
    assert!(!uninitialized.status.success());
    assert!(String::from_utf8_lossy(&uninitialized.stderr).contains("begin at version 1"));
    assert!(!out.exists());
    fs::write(
        &model,
        "model Todo { id String @@id(id) } mutation Send(to String) { id String }",
    )
    .unwrap();
    assert!(
        axton(&[
            input.as_os_str(),
            out.as_os_str(),
            "--action-history".as_ref(),
            path.as_os_str(),
            "--initialize-action-history".as_ref()
        ])
        .status
        .success()
    );
    assert!(path.exists());
    assert!(!input.join("history/actions.json").exists());
    assert!(
        !axton(&[
            input.as_os_str(),
            out.as_os_str(),
            "--action-history".as_ref(),
            path.as_os_str(),
            "--initialize-action-history".as_ref()
        ])
        .status
        .success()
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cli_refuses_removing_the_last_retained_action() {
    let (root, input) = workspace("action-removal");
    let out = root.join("out");
    let model = input.join("test.model");
    fs::write(
        &model,
        "model Todo { id String @@id(id) } mutation Send(to String) { id String }",
    )
    .unwrap();
    assert!(
        axton(&[input.as_os_str(), out.as_os_str()])
            .status
            .success()
    );
    let actions = input.join("history/actions.json");
    let before_history = fs::read(&actions).unwrap();
    let before_backend = fs::read(out.join("backend.json")).unwrap();
    fs::write(&model, "model Todo { id String @@id(id) }").unwrap();
    let rejected = axton(&[input.as_os_str(), out.as_os_str()]);
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("cannot be removed"));
    assert_eq!(fs::read(&actions).unwrap(), before_history);
    assert_eq!(fs::read(out.join("backend.json")).unwrap(), before_backend);
    fs::remove_dir_all(root).unwrap();
}
#[test]
fn cli_retains_history_and_does_not_overwrite_on_break() {
    let root = std::env::temp_dir().join(format!("axton-compiler-cli-{}", std::process::id()));
    let input = root.join("input");
    let out = root.join("out");
    fs::create_dir_all(&input).unwrap();
    let model = input.join("test.model");
    fs::write(
        &model,
        "model A { id UUID title String @@id(id) } mutation Save { a A.create }",
    )
    .unwrap();
    let compile = || {
        Command::new(env!("CARGO_BIN_EXE_axton"))
            .arg("compile")
            .arg(&input)
            .arg(&out)
            .output()
            .unwrap()
    };
    assert!(compile().status.success());
    let previous = fs::read(out.join("schema.json")).unwrap();
    fs::write(
        &model,
        "model A { id UUID @@id(id) } mutation Save { a A.create }",
    )
    .unwrap();
    let rejected = compile();
    assert!(!rejected.status.success());
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("schema fence"));
    assert_eq!(fs::read(out.join("schema.json")).unwrap(), previous);
    // A required field is a new input contract for the create and a new read
    // contract for the model, so both versions move.
    fs::write(&model,"model A { id UUID title String count Int @@id(id) @@version(2) } mutation Save { a A.create @@version(2) }").unwrap();
    assert!(compile().status.success());
    let backend: serde_json::Value =
        serde_json::from_slice(&fs::read(out.join("backend.json")).unwrap()).unwrap();
    assert!(backend.get("mutations").is_none());
    assert_eq!(
        backend["schema"]["clientPolicies"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let history: serde_json::Value =
        serde_json::from_slice(&fs::read(input.join("history").join("mutations.json")).unwrap())
            .unwrap();
    assert_eq!(
        history["mutations"]["Save"]
            .as_object()
            .unwrap()
            .keys()
            .collect::<Vec<_>>(),
        ["1", "2"]
    );
    assert!(
        !out.join("mutation-history.json").exists(),
        "history is kept beside the schema, not with generated output"
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cli_reads_the_superseded_history_location_and_writes_the_new_default() {
    let (root, input) = workspace("relocation");
    let out = root.join("out");
    let model = input.join("test.model");
    fs::write(
        &model,
        "model A { id UUID title String @@id(id) } mutation Save { a A.create }",
    )
    .unwrap();
    assert!(
        axton(&[input.as_os_str(), out.as_os_str()])
            .status
            .success()
    );
    // Recreate the superseded layout: history in the output directory, nothing beside the schema.
    let superseded = out.join("mutation-history.json");
    let default = input.join("history").join("mutations.json");
    fs::copy(&default, &superseded).unwrap();
    let retained = fs::read(&superseded).unwrap();
    fs::remove_dir_all(input.join("history")).unwrap();
    fs::write(
        &model,
        "model A { id UUID title String count Int @@id(id) } mutation Save { a A.create @@version(2) }",
    )
    .unwrap();
    let moved = axton(&[input.as_os_str(), out.as_os_str()]);
    assert!(moved.status.success());
    let notice = String::from_utf8_lossy(&moved.stderr);
    assert!(
        notice.contains(&format!("{}", superseded.display())),
        "{notice}"
    );
    assert!(
        notice.contains(&format!("{}", default.display())),
        "{notice}"
    );
    assert_eq!(notice.lines().count(), 1, "{notice}");
    let history: serde_json::Value = serde_json::from_slice(&fs::read(&default).unwrap()).unwrap();
    assert_eq!(
        history["mutations"]["Save"]
            .as_object()
            .unwrap()
            .keys()
            .collect::<Vec<_>>(),
        ["1", "2"],
        "the retained version survives the move"
    );
    assert_eq!(
        fs::read(&superseded).unwrap(),
        retained,
        "the old file is left in place, unchanged"
    );
    // A second run reads the new default and no longer reports a move.
    let again = axton(&[input.as_os_str(), out.as_os_str()]);
    assert!(again.status.success());
    assert_eq!(String::from_utf8_lossy(&again.stderr), "");
    let refused = axton(&[
        input.as_os_str(),
        out.as_os_str(),
        "--initialize-mutation-history".as_ref(),
    ]);
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("already exists"));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cli_initializes_the_history_at_the_new_default_when_neither_location_exists() {
    let (root, input) = workspace("initial-history");
    let out = root.join("out");
    fs::write(
        input.join("test.model"),
        "model A { id UUID title String @@id(id) } mutation Save { a A.create }",
    )
    .unwrap();
    let first = axton(&[input.as_os_str(), out.as_os_str()]);
    assert!(first.status.success());
    assert_eq!(String::from_utf8_lossy(&first.stderr), "");
    let history: serde_json::Value =
        serde_json::from_slice(&fs::read(input.join("history").join("mutations.json")).unwrap())
            .unwrap();
    assert_eq!(history["formatVersion"], 1);
    assert_eq!(history["mutations"]["Save"]["1"]["version"], 1);
    assert!(!out.join("mutation-history.json").exists());
    fs::remove_dir_all(root).unwrap();
}
#[test]
fn cli_writes_backend_ts_with_the_requested_runtime_import() {
    let root = std::env::temp_dir().join(format!("axton-compiler-backend-{}", std::process::id()));
    let input = root.join("input");
    let out = root.join("out");
    fs::create_dir_all(&input).unwrap();
    fs::write(
        input.join("test.model"),
        "model A { id UUID title String @@id(id) } mutation Save { a A.create }",
    )
    .unwrap();
    let status = Command::new(env!("CARGO_BIN_EXE_axton"))
        .arg("compile")
        .arg(&input)
        .arg(&out)
        .arg("--backend-runtime")
        .arg("../../packages/backend/server/index.mts")
        .arg("--client-runtime")
        .arg("../../packages/frontend/client-js/index.mts")
        .status()
        .unwrap();
    assert!(status.success());
    let backend = fs::read_to_string(out.join("backend.ts")).unwrap();
    assert!(backend.contains("from \"../../packages/backend/server/index.mts\""));
    assert!(!backend.contains("HandlerCall<Tx, SaveInput>"));
    assert!(!backend.contains("options.handlers"));
    assert!(backend.contains(" a?: { v1(call: LoaderCall<Tx, AIdentity>)"));
    let client = fs::read_to_string(out.join("client.ts")).unwrap();
    assert!(client.contains("from \"../../packages/frontend/client-js/index.mts\""));
    assert!(client.contains(" static async open(options: {path:string;"));
    assert!(!client.contains("owner"));
    fs::remove_dir_all(root).unwrap();
}

fn workspace(tag: &str) -> (std::path::PathBuf, std::path::PathBuf) {
    let root = std::env::temp_dir().join(format!("axton-compiler-{tag}-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    let input = root.join("input");
    fs::create_dir_all(&input).unwrap();
    (root, input)
}

fn axton(args: &[&std::ffi::OsStr]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_axton"))
        .arg("compile")
        .args(args)
        .output()
        .unwrap()
}

#[test]
fn cli_relocates_errors_into_the_file_that_declares_them() {
    let (root, input) = workspace("relocate");
    let out = root.join("out");
    fs::write(
        input.join("a.model"),
        "model Parent {\n id UUID\n @@id(id)\n}\n",
    )
    .unwrap();
    fs::write(
        input.join("b.model"),
        "model Child {\n id UUID\n parent Parent @reference(via: [missing])\n @@id(id)\n}\n",
    )
    .unwrap();
    let rejected = axton(&[input.as_os_str(), out.as_os_str()]);
    assert!(!rejected.status.success());
    let stderr = String::from_utf8_lossy(&rejected.stderr);
    assert!(
        stderr.contains(&format!("{}:3:", input.join("b.model").display())),
        "{stderr}"
    );
    assert!(stderr.contains("unknown reference field"), "{stderr}");
    fs::write(
        input.join("b.model"),
        "model Child {\n id UUID\n @@id(id)\n}\nbogus Stuff {}\n",
    )
    .unwrap();
    let rejected = axton(&[input.as_os_str(), out.as_os_str()]);
    let stderr = String::from_utf8_lossy(&rejected.stderr);
    assert!(
        stderr.contains(&format!("{}:5:", input.join("b.model").display())),
        "{stderr}"
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cli_output_is_deterministic() {
    let (root, input) = workspace("determinism");
    fs::copy(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../fixtures/compiler/relations.model"
        ),
        input.join("relations.model"),
    )
    .unwrap();
    let first = root.join("first");
    let second = root.join("second");
    assert!(
        axton(&[input.as_os_str(), first.as_os_str()])
            .status
            .success()
    );
    assert!(
        axton(&[input.as_os_str(), second.as_os_str()])
            .status
            .success()
    );
    let mut names: Vec<_> = fs::read_dir(&first)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    names.sort();
    assert_eq!(names.len(), 6, "{names:?}");
    assert!(input.join("history").join("mutations.json").exists());
    assert!(input.join("history").join("models.json").exists());
    assert!(!input.join("history").join("actions.json").exists());
    assert_eq!(
        fs::read(input.join("history").join("models.json")).unwrap(),
        {
            fs::remove_file(input.join("history").join("models.json")).unwrap();
            assert!(
                axton(&[input.as_os_str(), first.as_os_str()])
                    .status
                    .success()
            );
            fs::read(input.join("history").join("models.json")).unwrap()
        },
        "a regenerated model history is byte-identical"
    );
    for name in names {
        assert_eq!(
            fs::read(first.join(&name)).unwrap(),
            fs::read(second.join(&name)).unwrap(),
            "{name:?} differs between runs"
        );
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cli_refuses_misuse_of_the_mutation_history() {
    let (root, input) = workspace("history");
    let out = root.join("out");
    fs::write(
        input.join("test.model"),
        "model A { id UUID @@id(id) } mutation Save { a A.create @@version(2) }",
    )
    .unwrap();
    let history = root.join("history.json");
    let missing = axton(&[
        input.as_os_str(),
        out.as_os_str(),
        "--mutation-history".as_ref(),
        history.as_os_str(),
    ]);
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("missing mutation history"));
    assert!(!out.exists(), "a refused compile must not write outputs");
    let not_first = axton(&[
        input.as_os_str(),
        out.as_os_str(),
        "--mutation-history".as_ref(),
        history.as_os_str(),
        "--initialize-mutation-history".as_ref(),
    ]);
    assert!(!not_first.status.success());
    assert!(String::from_utf8_lossy(&not_first.stderr).contains("begin at version 1"));
    fs::write(
        input.join("test.model"),
        "model A { id UUID @@id(id) } mutation Save { a A.create }",
    )
    .unwrap();
    assert!(
        axton(&[
            input.as_os_str(),
            out.as_os_str(),
            "--mutation-history".as_ref(),
            history.as_os_str(),
            "--initialize-mutation-history".as_ref(),
        ])
        .status
        .success()
    );
    assert!(history.exists());
    let again = axton(&[
        input.as_os_str(),
        out.as_os_str(),
        "--mutation-history".as_ref(),
        history.as_os_str(),
        "--initialize-mutation-history".as_ref(),
    ]);
    assert!(!again.status.success());
    assert!(String::from_utf8_lossy(&again.stderr).contains("already exists"));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cli_retains_model_history_and_refuses_a_breaking_read_change_without_a_bump() {
    let (root, input) = workspace("model-history");
    let out = root.join("out");
    let model = input.join("test.model");
    let mutations = input.join("history").join("mutations.json");
    let models = input.join("history").join("models.json");
    fs::write(
        &model,
        "enum Status { open closed } model Task { id UUID title String note String? status Status @@id(id) } mutation Save { t Task.update<title> }",
    )
    .unwrap();
    assert!(
        axton(&[input.as_os_str(), out.as_os_str()])
            .status
            .success()
    );
    let first: serde_json::Value = serde_json::from_slice(&fs::read(&models).unwrap()).unwrap();
    assert_eq!(first["formatVersion"], 1);
    assert_eq!(first["models"]["Task"]["1"]["version"], 1);
    assert_eq!(
        first["models"]["Task"]["1"]["enums"][0]["values"],
        serde_json::json!(["open", "closed"])
    );
    let schema: serde_json::Value =
        serde_json::from_slice(&fs::read(out.join("schema.json")).unwrap()).unwrap();
    assert_eq!(
        schema["models"][0]["version"], 1,
        "the client declares its model version"
    );
    let outputs = || -> Vec<(String, Vec<u8>)> {
        let mut files: Vec<_> = fs::read_dir(&out)
            .unwrap()
            .map(|e| e.unwrap().path())
            .map(|p| {
                (
                    p.file_name().unwrap().to_string_lossy().into_owned(),
                    fs::read(&p).unwrap(),
                )
            })
            .collect();
        files.sort();
        files.push(("mutations.json".into(), fs::read(&mutations).unwrap()));
        files.push(("models.json".into(), fs::read(&models).unwrap()));
        files
    };
    let before = outputs();
    // A breaking read change (a new enum value) and a compatible mutation change in one
    // edit: the compile is refused as a whole, and neither history moves.
    fs::write(
        &model,
        "enum Status { open closed archived } model Task { id UUID title String note String? status Status @@id(id) } mutation Save { t Task.update<title> } mutation Rename { t Task.update<status> }",
    )
    .unwrap();
    let refused = axton(&[input.as_os_str(), out.as_os_str()]);
    assert!(!refused.status.success());
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(stderr.contains("Task v1"), "{stderr}");
    assert!(stderr.contains("enum \"Status\""), "{stderr}");
    assert!(stderr.contains("increase @@version"), "{stderr}");
    assert_eq!(
        outputs(),
        before,
        "a refused compile touches no output and no history"
    );
    // With the bump, the old contract stays as published beside the new one, and the fence
    // lets the bumped model drop a field.
    fs::write(
        &model,
        "enum Status { open closed archived } model Task { id UUID title String status Status @@id(id) @@version(2) } mutation Save { t Task.update<title> } mutation Rename { t Task.update<status> }",
    )
    .unwrap();
    let accepted = axton(&[input.as_os_str(), out.as_os_str()]);
    assert!(
        accepted.status.success(),
        "{}",
        String::from_utf8_lossy(&accepted.stderr)
    );
    let second: serde_json::Value = serde_json::from_slice(&fs::read(&models).unwrap()).unwrap();
    assert_eq!(
        second["models"]["Task"]
            .as_object()
            .unwrap()
            .keys()
            .collect::<Vec<_>>(),
        ["1", "2"]
    );
    assert_eq!(second["models"]["Task"]["1"], first["models"]["Task"]["1"]);
    assert_eq!(
        second["models"]["Task"]["2"]["enums"][0]["values"],
        serde_json::json!(["open", "closed", "archived"])
    );
    let backend: serde_json::Value =
        serde_json::from_slice(&fs::read(out.join("backend.json")).unwrap()).unwrap();
    let retained: Vec<(String, u64)> = backend["models"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| {
            (
                m["name"].as_str().unwrap().into(),
                m["version"].as_u64().unwrap(),
            )
        })
        .collect();
    assert_eq!(retained, [("Task".to_string(), 1), ("Task".to_string(), 2)]);
    assert_eq!(backend["models"][0], first["models"]["Task"]["1"]);
    assert_eq!(backend["schema"]["models"][0]["version"], 2);
    let history: serde_json::Value =
        serde_json::from_slice(&fs::read(&mutations).unwrap()).unwrap();
    assert!(history["mutations"]["Rename"]["1"].is_object());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cli_refuses_misuse_of_the_model_history() {
    let (root, input) = workspace("model-history-misuse");
    let out = root.join("out");
    fs::write(
        input.join("test.model"),
        "model A { id UUID @@id(id) @@version(2) }",
    )
    .unwrap();
    let history = root.join("models.json");
    let missing = axton(&[
        input.as_os_str(),
        out.as_os_str(),
        "--model-history".as_ref(),
        history.as_os_str(),
    ]);
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("missing model history"));
    assert!(!out.exists(), "a refused compile must not write outputs");
    assert!(
        !input.join("history").exists(),
        "a refused compile must not write a history"
    );
    let not_first = axton(&[
        input.as_os_str(),
        out.as_os_str(),
        "--model-history".as_ref(),
        history.as_os_str(),
        "--initialize-model-history".as_ref(),
    ]);
    assert!(!not_first.status.success());
    assert!(
        String::from_utf8_lossy(&not_first.stderr)
            .contains("initial model history must begin at version 1")
    );
    fs::write(input.join("test.model"), "model A { id UUID @@id(id) }").unwrap();
    assert!(
        axton(&[
            input.as_os_str(),
            out.as_os_str(),
            "--model-history".as_ref(),
            history.as_os_str(),
            "--initialize-model-history".as_ref(),
        ])
        .status
        .success()
    );
    assert!(history.exists());
    assert!(
        !input.join("history").join("models.json").exists(),
        "an explicit path replaces the default"
    );
    assert!(
        input.join("history").join("mutations.json").exists(),
        "the mutation history keeps its default"
    );
    let again = axton(&[
        input.as_os_str(),
        out.as_os_str(),
        "--model-history".as_ref(),
        history.as_os_str(),
        "--initialize-model-history".as_ref(),
    ]);
    assert!(!again.status.success());
    assert!(String::from_utf8_lossy(&again.stderr).contains("model history already exists"));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cli_rejects_retained_action_identifier_collisions_before_writes() {
    let (root, input) = workspace("retained-action-name-collision");
    let out = root.join("out");
    let model = input.join("test.model");
    fs::write(&model, "model Todo { id String @@id(id) } mutation Fetch()").unwrap();
    let first = axton(&[input.as_os_str(), out.as_os_str()]);
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let files = [
        input.join("history/actions.json"),
        input.join("history/models.json"),
        input.join("history/mutations.json"),
        out.join("generated.ts"),
        out.join("generated.dart"),
        out.join("backend.ts"),
    ];
    let before: Vec<_> = files.iter().map(|path| fs::read(path).unwrap()).collect();
    fs::write(&model, "model Todo { id String @@id(id) } model FetchV1Input { id String @@id(id) } @version(2) mutation Fetch()").unwrap();
    let second = axton(&[input.as_os_str(), out.as_os_str()]);
    assert!(!second.status.success());
    let error = String::from_utf8_lossy(&second.stderr);
    assert!(
        error.contains("FetchV1Input")
            && error.contains("Mutation Fetch v1")
            && error.contains("model FetchV1Input"),
        "{error}"
    );
    let after: Vec<_> = files.iter().map(|path| fs::read(path).unwrap()).collect();
    assert_eq!(before, after);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cli_disambiguates_retained_operand_helper() {
    let (root, input) = workspace("retained-action-operand-name-collision");
    let out = root.join("out");
    let model = input.join("test.model");
    let v1 =
        "model Todo { id String title String @@id(id) } mutation Edit(todo Todo.update<title>)";
    fs::write(&model, v1).unwrap();
    let first = axton(&[input.as_os_str(), out.as_os_str()]);
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    fs::write(
        &model,
        v1.replace("mutation Edit", "@version(2) mutation Edit"),
    )
    .unwrap();
    let second = axton(&[input.as_os_str(), out.as_os_str()]);
    assert!(
        second.status.success(),
        "{}",
        String::from_utf8_lossy(&second.stderr)
    );
    let dart = fs::read_to_string(out.join("generated.dart")).unwrap();
    assert_eq!(dart.matches("class EditV1TodoUpdate ").count(), 1);
    assert!(dart.contains("class EditV1TodoSlotUpdate "));
    assert!(dart.contains("EditV1TodoSlotUpdate todo"));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cli_rejects_collision_between_retained_and_new_action_symbols() {
    let (root, input) = workspace("retained-action-action-collision");
    let out = root.join("out");
    let model = input.join("test.model");
    fs::write(&model, "model Todo { id String @@id(id) } mutation Fetch()").unwrap();
    assert!(
        axton(&[input.as_os_str(), out.as_os_str()])
            .status
            .success()
    );
    let history = fs::read(input.join("history/actions.json")).unwrap();
    fs::write(
        &model,
        "model Todo { id String @@id(id) } @version(2) mutation Fetch() mutation FetchV1()",
    )
    .unwrap();
    let refused = axton(&[input.as_os_str(), out.as_os_str()]);
    assert!(!refused.status.success());
    let error = String::from_utf8_lossy(&refused.stderr);
    assert!(
        error.contains("FetchV1Input")
            && error.contains("Mutation Fetch v1")
            && error.contains("Mutation FetchV1 v1"),
        "{error}"
    );
    assert_eq!(
        fs::read(input.join("history/actions.json")).unwrap(),
        history
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cli_rejects_retained_model_record_name_used_by_action_schema() {
    let (root, input) = workspace("retained-model-record-name-collision");
    let out = root.join("out");
    let model = input.join("test.model");
    fs::write(&model, "model Todo { id String @@id(id) } mutation Fetch()").unwrap();
    assert!(
        axton(&[input.as_os_str(), out.as_os_str()])
            .status
            .success()
    );
    fs::write(&model, "@version(2) model Todo { id String title String @@id(id) } model TodoV1 { id String @@id(id) } @version(2) mutation Fetch()").unwrap();
    let refused = axton(&[input.as_os_str(), out.as_os_str()]);
    assert!(!refused.status.success());
    let error = String::from_utf8_lossy(&refused.stderr);
    assert!(
        error.contains("TodoV1") && error.contains("model Todo"),
        "{error}"
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cli_rejects_retained_enum_name_matching_action_input_before_writes() {
    let (root, input) = workspace("retained-enum-action-input-collision");
    let out = root.join("out");
    let model = input.join("test.model");
    let v1 = "enum Input { a b } model Todo { id String @@id(id) } mutation Fetch(value Input)";
    fs::write(&model, v1).unwrap();
    let first = axton(&[input.as_os_str(), out.as_os_str()]);
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let files = [
        input.join("history/actions.json"),
        input.join("history/models.json"),
        out.join("generated.dart"),
        out.join("backend.ts"),
    ];
    let before: Vec<_> = files.iter().map(|path| fs::read(path).unwrap()).collect();
    fs::write(
        &model,
        v1.replace("mutation Fetch", "@version(2) mutation Fetch"),
    )
    .unwrap();
    let refused = axton(&[input.as_os_str(), out.as_os_str()]);
    assert!(!refused.status.success());
    let error = String::from_utf8_lossy(&refused.stderr);
    assert!(
        error.contains("FetchV1Input")
            && error.contains("Mutation Fetch v1")
            && error.contains("enum Input"),
        "{error}"
    );
    let after: Vec<_> = files.iter().map(|path| fs::read(path).unwrap()).collect();
    assert_eq!(before, after);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cli_reuses_retained_enum_across_action_members() {
    let (root, input) = workspace("retained-shared-enum");
    let out = root.join("out");
    let model = input.join("test.model");
    let v1 = "enum Status { open closed } model Todo { id String @@id(id) } mutation Fetch(first Status, second Status) { firstStatus Status secondStatus Status }";
    fs::write(&model, v1).unwrap();
    let first = axton(&[input.as_os_str(), out.as_os_str()]);
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    fs::write(
        &model,
        v1.replace("mutation Fetch", "@version(2) mutation Fetch"),
    )
    .unwrap();
    let second = axton(&[input.as_os_str(), out.as_os_str()]);
    assert!(
        second.status.success(),
        "{}",
        String::from_utf8_lossy(&second.stderr)
    );
    let dart = fs::read_to_string(out.join("generated.dart")).unwrap();
    assert_eq!(dart.matches("enum FetchV1Status ").count(), 1);
    assert_eq!(dart.matches("enum FetchV1OutputStatus ").count(), 1);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cli_rejects_retained_enum_name_matching_handler_output() {
    let (root, input) = workspace("retained-enum-handler-output-collision");
    let out = root.join("out");
    let model = input.join("test.model");
    let v1 = "enum HandlerOutput { a b } model Todo { id String @@id(id) } mutation Fetch(value HandlerOutput)";
    fs::write(&model, v1).unwrap();
    assert!(
        axton(&[input.as_os_str(), out.as_os_str()])
            .status
            .success()
    );
    fs::write(
        &model,
        v1.replace("mutation Fetch", "@version(2) mutation Fetch"),
    )
    .unwrap();
    let refused = axton(&[input.as_os_str(), out.as_os_str()]);
    assert!(!refused.status.success());
    let error = String::from_utf8_lossy(&refused.stderr);
    assert!(
        error.contains("FetchV1HandlerOutput")
            && error.contains("Mutation Fetch v1 HandlerOutput")
            && error.contains("input enum HandlerOutput"),
        "{error}"
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cli_refuses_invalid_retained_operation_kinds_before_writes() {
    let (root, input) = workspace("retained-operation-kind");
    let out = root.join("out");
    let model = input.join("test.model");
    fs::write(
        &model,
        "model Todo { id String @@id(id) } mutation Find(todo Todo.delete)",
    )
    .unwrap();
    let first = axton(&[input.as_os_str(), out.as_os_str()]);
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let history_path = input.join("history/actions.json");
    let history: serde_json::Value =
        serde_json::from_slice(&fs::read(&history_path).unwrap()).unwrap();
    fs::write(
        &model,
        "model Todo { id String @@id(id) } @version(2) query Find(text String)",
    )
    .unwrap();
    // A hand-edited older snapshot: an unknown kind, then a Query that still
    // declares its Model operand. Neither is retained or generated.
    for (kind, needle) in [
        ("bogus", "unsupported retained operation kind"),
        ("query", "cannot take a Model operand"),
    ] {
        let mut edited = history.clone();
        edited["actions"]["Find"]["1"]["kind"] = serde_json::json!(kind);
        fs::write(&history_path, serde_json::to_vec_pretty(&edited).unwrap()).unwrap();
        let before = fs::read(out.join("backend.json")).unwrap();
        let refused = axton(&[input.as_os_str(), out.as_os_str()]);
        let error = String::from_utf8_lossy(&refused.stderr);
        assert!(
            !refused.status.success() && error.contains(needle),
            "{kind}: {error}"
        );
        assert_eq!(fs::read(out.join("backend.json")).unwrap(), before);
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cli_retains_load_history_without_rewriting_other_histories() {
    let (root, input) = workspace("retired-load");
    let out = root.join("out");
    fs::write(
        input.join("test.model"),
        "model Todo { id String @@id(id) }",
    )
    .unwrap();
    assert!(
        axton(&[input.as_os_str(), out.as_os_str()])
            .status
            .success()
    );
    let before = fs::read(out.join("schema.json")).unwrap();
    let history = fs::read(input.join("history/models.json")).unwrap();
    fs::write(
        input.join("test.model"),
        "model Todo { id String @@id(id) } load AllTodos() { todos Todo[] }",
    )
    .unwrap();
    let refused = axton(&[input.as_os_str(), out.as_os_str()]);
    assert!(!refused.status.success());
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("Load declarations were removed in 0.4")
    );
    assert_eq!(before, fs::read(out.join("schema.json")).unwrap());
    assert_eq!(
        history,
        fs::read(input.join("history/models.json")).unwrap()
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cli_load_history_options_require_explicit_initialization() {
    let (root, input) = workspace("retired-load-options");
    let out = root.join("out");
    fs::write(
        input.join("test.model"),
        "model Todo { id String @@id(id) } load AllTodos() { todos Todo[] }",
    )
    .unwrap();
    let history = root.join("loads.json");
    let refused = axton(&[
        input.as_os_str(),
        out.as_os_str(),
        "--load-history".as_ref(),
        history.as_os_str(),
        "--initialize-load-history".as_ref(),
    ]);
    assert!(!refused.status.success());
    assert!(
        String::from_utf8_lossy(&refused.stderr)
            .contains("Load history options were removed in 0.4")
    );
    assert!(!history.exists());
    assert!(!out.exists());
    fs::remove_dir_all(root).unwrap();
}

/// Model Fetch reuses each Model's current local read version and declares no
/// operation: compiling it into a schema with retained Model and operation
/// histories leaves the operation history byte-identical
/// ([#153](https://github.com/zanminwang/axton/issues/153)).
#[test]
fn cli_model_fetch_uses_current_model_versions_and_leaves_operation_history() {
    let (root, input) = workspace("model-fetch");
    let out = root.join("out");
    let model = input.join("test.model");
    fs::write(
        &model,
        "model Todo { id String @@id(id) } query Find(text String) { value String }",
    )
    .unwrap();
    let first = axton(&[input.as_os_str(), out.as_os_str()]);
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    let actions = fs::read(input.join("history/actions.json")).unwrap();
    fs::write(
        &model,
        "model Todo { id String title String? @@id(id) @@version(2) } query Find(text String) { value String }",
    )
    .unwrap();
    let second = axton(&[input.as_os_str(), out.as_os_str()]);
    assert!(
        second.status.success(),
        "{}",
        String::from_utf8_lossy(&second.stderr)
    );
    assert_eq!(
        fs::read(input.join("history/actions.json")).unwrap(),
        actions
    );
    let backend: serde_json::Value =
        serde_json::from_slice(&fs::read(out.join("backend.json")).unwrap()).unwrap();
    assert_eq!(
        backend["models"].as_array().unwrap().len(),
        2,
        "v1 is retained"
    );
    let ts = fs::read_to_string(out.join("generated.ts")).unwrap();
    assert!(
        ts.contains(" todo:(identity,options)=>port.fetchModel('Todo',2,encodeTodoIdentity(identity),decodeTodo,options),"),
        "{ts}"
    );
    let dart = fs::read_to_string(out.join("generated.dart")).unwrap();
    assert!(
        dart.contains(
            "_client.fetchModel('Todo', 2, identity.toRecord(), Todo.fromRecord, store: store);"
        ),
        "{dart}"
    );
    fs::remove_dir_all(root).unwrap();
}

/// The client engine's #179 scenarios load this compiled schema, so the
/// fixture is the compiler's current output for its source.
#[test]
fn cli_sequence_paths_fixture_is_current() {
    let (root, input) = workspace("sequence-paths");
    let fixtures = concat!(env!("CARGO_MANIFEST_DIR"), "/../../fixtures");
    fs::copy(
        format!("{fixtures}/schemas/sequence-paths.model"),
        input.join("sequence-paths.model"),
    )
    .unwrap();
    let out = root.join("out");
    assert!(
        axton(&[input.as_os_str(), out.as_os_str()])
            .status
            .success()
    );
    assert_eq!(
        fs::read_to_string(out.join("schema.json")).unwrap(),
        fs::read_to_string(format!("{fixtures}/schemas/sequence-paths.json")).unwrap(),
        "regenerate fixtures/schemas/sequence-paths.json from fixtures/schemas/sequence-paths.model"
    );
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cli_reports_its_version() {
    for flag in ["--version", "-V"] {
        let output = Command::new(env!("CARGO_BIN_EXE_axton"))
            .arg(flag)
            .output()
            .unwrap();
        assert!(output.status.success());
        assert_eq!(
            String::from_utf8(output.stdout).unwrap(),
            format!("axton {}\n", env!("CARGO_PKG_VERSION"))
        );
    }
}

#[test]
fn cli_generated_typescript_imports_the_public_packages_by_default() {
    let (root, input) = workspace("default-runtime");
    let out = root.join("out");
    fs::write(
        input.join("test.model"),
        "model Todo { id String @@id(id) } mutation AddTodo(todo Todo.create)",
    )
    .unwrap();
    let result = axton(&[input.as_os_str(), out.as_os_str()]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let backend = fs::read_to_string(out.join("backend.ts")).unwrap();
    let client = fs::read_to_string(out.join("client.ts")).unwrap();
    assert!(backend.contains("from \"@axtonjs/server\""), "{backend}");
    assert!(client.contains("from \"@axtonjs/client\""), "{client}");
}

#[test]
fn backend_executes_named_actions_while_retained_slot_history_stays_in_client_schema() {
    let (root, input) = workspace("retained-slot-carrier");
    let out = root.join("out");
    fs::write(input.join("test.model"), "model Entry { id String text String @@id(id) } mutation LegacyEdit { entry Entry.update<text> } mutation EditEntry(entry Entry.update<text>)").unwrap();
    let result = axton(&[input.as_os_str(), out.as_os_str()]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let history = fs::read(input.join("history/mutations.json")).unwrap();
    let backend: serde_json::Value =
        serde_json::from_slice(&fs::read(out.join("backend.json")).unwrap()).unwrap();
    assert!(backend.get("mutations").is_none());
    assert_eq!(backend["schema"]["clientPolicies"][0]["name"], "LegacyEdit");
    assert!(
        backend["actions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a["name"] == "EditEntry")
    );
    let generated = fs::read_to_string(out.join("backend.ts")).unwrap();
    assert!(generated.contains("EditEntry"));
    assert!(!generated.contains("RecordHandler"));
    let inline = generated
        .lines()
        .find(|line| line.starts_with("const schema = "))
        .unwrap();
    let inline: serde_json::Value = serde_json::from_str(
        inline
            .strip_prefix("const schema = ")
            .unwrap()
            .strip_suffix(" as const;")
            .unwrap(),
    )
    .unwrap();
    assert_eq!(inline, backend, "both server execution carriers must agree");
    assert!(inline.get("mutations").is_none());
    let client = fs::read_to_string(out.join("generated.ts")).unwrap();
    assert!(client.contains("export type MutationName = 'EditEntry';"));
    assert!(
        axton(&[input.as_os_str(), out.as_os_str()])
            .status
            .success()
    );
    assert_eq!(
        fs::read(input.join("history/mutations.json")).unwrap(),
        history
    );
}
