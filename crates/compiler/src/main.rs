use serde_json::Value;
use std::{
    env, fs,
    path::{Path, PathBuf},
};
fn read_json(path: &Path) -> Result<Value, String> {
    serde_json::from_str(&fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?)
        .map_err(|e| format!("{}: {e}", path.display()))
}
fn run() -> Result<(), String> {
    let args: Vec<_> = env::args().collect();
    if matches!(args.get(1).map(String::as_str), Some("--version" | "-V")) && args.len() == 2 {
        println!("axton {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    if args.len() < 4 || args[1] != "compile" {
        return Err("usage: axton --version | axton compile INPUT_DIR OUTPUT_DIR [--mutation-history FILE] [--initialize-mutation-history] [--model-history FILE] [--initialize-model-history] [--action-history FILE] [--initialize-action-history] [--schema-fence FILE] [--backend-runtime SPEC] [--client-runtime SPEC]".into());
    }
    let input = Path::new(&args[2]);
    let out = Path::new(&args[3]);
    // History belongs beside the schema and is committed to Git, not with disposable output.
    let mut history_path = input.join("history").join("mutations.json");
    let mut model_history_path = input.join("history").join("models.json");
    let mut action_history_path = input.join("history").join("actions.json");
    let superseded = out.join("mutation-history.json");
    let mut fence_path = out.join("schema.json");
    let mut backend_runtime = String::from("@axtonjs/server");
    let mut client_runtime = String::from("@axtonjs/client");
    let mut initialize = false;
    let mut explicit_history = false;
    let mut initialize_models = false;
    let mut explicit_model_history = false;
    let mut initialize_actions = false;
    let mut explicit_action_history = false;
    let mut index = 4;
    while index < args.len() {
        match args[index].as_str() {
            "--initialize-mutation-history" => initialize = true,
            "--initialize-model-history" => initialize_models = true,
            "--initialize-action-history" => initialize_actions = true,
            "--load-history" | "--initialize-load-history" => return Err(
                "Load history options were removed in 0.4; retain legacy descriptors separately"
                    .into(),
            ),
            "--mutation-history" | "--model-history" | "--action-history" | "--schema-fence"
            | "--backend-runtime" | "--client-runtime" => {
                let value = args.get(index + 1).ok_or("missing option value")?;
                match args[index].as_str() {
                    "--mutation-history" => {
                        history_path = PathBuf::from(value);
                        explicit_history = true;
                    }
                    "--model-history" => {
                        model_history_path = PathBuf::from(value);
                        explicit_model_history = true;
                    }
                    "--action-history" => {
                        action_history_path = PathBuf::from(value);
                        explicit_action_history = true;
                    }
                    "--schema-fence" => fence_path = PathBuf::from(value),
                    "--backend-runtime" => backend_runtime = value.clone(),
                    _ => client_runtime = value.clone(),
                }
                index += 1;
            }
            other => return Err(format!("unsupported option {other}")),
        }
        index += 1;
    }
    // An existing history at the superseded location is read and rewritten at the new default,
    // never reinitialized.
    let relocate = !explicit_history && !history_path.exists() && superseded.exists();
    if initialize && (history_path.exists() || relocate) {
        return Err("mutation history already exists; initialization refused".into());
    }
    if explicit_history && !history_path.exists() && !initialize {
        return Err("missing mutation history; restore it or initialize explicitly".into());
    }
    if initialize_models && model_history_path.exists() {
        return Err("model history already exists; initialization refused".into());
    }
    if explicit_model_history && !model_history_path.exists() && !initialize_models {
        return Err("missing model history; restore it or initialize explicitly".into());
    }
    let mut paths = fs::read_dir(&args[2])
        .map_err(|e| e.to_string())?
        .map(|e| e.map(|e| e.path()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    paths.retain(|p| p.extension().is_some_and(|e| e == "model"));
    paths.sort();
    if paths.is_empty() {
        return Err("input directory has no .model files".into());
    }
    let mut source = String::new();
    let mut origins = vec![];
    for path in paths {
        let contents = fs::read_to_string(&path).map_err(|e| e.to_string())?;
        let start = source.chars().filter(|c| *c == '\n').count() + 1;
        source.push_str(&contents);
        source.push('\n');
        origins.push((start, path));
    }
    let mut config = axton_compiler::compile(&source).map_err(|e| {
        let line = e
            .split(':')
            .next()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(1);
        let (start, path) = origins
            .iter()
            .rev()
            .find(|(start, _)| *start <= line)
            .unwrap();
        format!(
            "{}:{}:{}",
            path.display(),
            line - start + 1,
            e.split_once(':').map(|(_, rest)| rest).unwrap_or(&e)
        )
    })?;
    let has_actions = !config["actions"].as_array().unwrap().is_empty();
    let track_actions = has_actions || action_history_path.exists();
    if has_actions {
        if initialize_actions && action_history_path.exists() {
            return Err("operation history already exists; initialization refused".into());
        }
        if explicit_action_history && !action_history_path.exists() && !initialize_actions {
            return Err("missing operation history; restore it or initialize explicitly".into());
        }
    }
    if initialize
        && config["mutations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["version"] != 1)
    {
        return Err("initial mutation history must begin at version 1".into());
    }
    if initialize_models
        && config["schema"]["models"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["version"] != 1)
    {
        return Err("initial model history must begin at version 1".into());
    }
    if fence_path.exists() {
        axton_compiler::check_fence(&read_json(&fence_path)?, &config["schema"])?;
    }
    let history_source = if relocate {
        superseded.clone()
    } else {
        history_path.clone()
    };
    let previous = if history_source.exists() {
        Some(read_json(&history_source)?)
    } else {
        None
    };
    // Both histories are reconciled before anything is written, so a refusal
    // from either leaves every output and both histories as they were.
    let history = axton_compiler::reconcile_history(&config, previous.as_ref())?;
    let previous_models = if model_history_path.exists() {
        Some(read_json(&model_history_path)?)
    } else {
        None
    };
    let model_history = axton_compiler::reconcile_model_history(&config, previous_models.as_ref())?;
    let retained = |history: &Value, key: &str| -> Vec<Value> {
        history[key]
            .as_object()
            .unwrap()
            .values()
            .flat_map(|v| v.as_object().unwrap().values().cloned())
            .collect()
    };
    // Action outputs select a retained Model read version from this list.
    config["backendModels"] = serde_json::json!(retained(&model_history, "models"));
    let action_history = if track_actions {
        let previous_actions = if action_history_path.exists() {
            Some(read_json(&action_history_path)?)
        } else {
            None
        };
        Some(axton_compiler::reconcile_action_history(
            &config,
            previous_actions.as_ref(),
        )?)
    } else {
        None
    };
    let historical = retained(&history, "mutations");
    config["backendMutations"] = serde_json::json!(historical);
    config["schema"]["clientPolicies"] = serde_json::json!(historical);
    if let Some(action_history) = &action_history {
        config["actions"] = serde_json::json!(retained(action_history, "actions"));
        config["schema"]["actions"] = config["actions"].clone();
    }
    config["schema"]["resultModels"] = config["backendModels"].clone();
    // The merged schema, retained versions included, is what clients and the
    // backend load; refuse it here rather than at their startup.
    axton_core::Schema::from_value(config["schema"].clone())
        .map_err(|e| format!("retained schema: {e}"))?;
    axton_compiler::check_action_names(&config)?;
    let mut backend = config.clone();
    // Retained slot descriptors belong to immutable history/client policies.
    // The server executes only named actions.
    backend.as_object_mut().unwrap().remove("mutations");
    backend["models"] = config["backendModels"].clone();
    backend.as_object_mut().unwrap().remove("backendMutations");
    backend.as_object_mut().unwrap().remove("backendModels");
    let mut files = vec![
        (
            out.join("schema.json"),
            serde_json::to_string_pretty(&config["schema"]).unwrap(),
        ),
        (
            out.join("backend.json"),
            serde_json::to_string_pretty(&backend).unwrap(),
        ),
        (
            out.join("generated.ts"),
            axton_compiler::typescript(&config),
        ),
        (
            out.join("backend.ts"),
            axton_compiler::backend_typescript(&config, &backend_runtime),
        ),
        (
            out.join("client.ts"),
            axton_compiler::client_typescript(&config, &client_runtime),
        ),
        (out.join("generated.dart"), axton_compiler::dart(&config)),
        (
            history_path.clone(),
            serde_json::to_string_pretty(&history).unwrap(),
        ),
        (
            model_history_path.clone(),
            serde_json::to_string_pretty(&model_history).unwrap(),
        ),
    ];
    if let Some(action_history) = &action_history {
        files.push((
            action_history_path.clone(),
            serde_json::to_string_pretty(action_history).unwrap(),
        ));
    }
    fs::create_dir_all(out).map_err(|e| e.to_string())?;
    for parent in [
        history_path.parent(),
        model_history_path.parent(),
        if track_actions {
            action_history_path.parent()
        } else {
            None
        },
    ]
    .into_iter()
    .flatten()
    {
        fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    // Stage every file first, then move them into place: a write failure leaves
    // the previous outputs and histories untouched.
    let mut staged = vec![];
    for (path, contents) in files {
        // Keep the full file name so `backend.json` and `backend.ts` stage apart.
        let mut temp = path.clone().into_os_string();
        temp.push(format!(".{}.tmp", std::process::id()));
        let temp = PathBuf::from(temp);
        if let Err(e) = fs::write(&temp, contents) {
            for (temp, _) in &staged {
                let _ = fs::remove_file(temp);
            }
            let _ = fs::remove_file(&temp);
            return Err(format!("{}: {e}", temp.display()));
        }
        staged.push((temp, path));
    }
    for (temp, path) in staged {
        fs::rename(temp, &path).map_err(|e| format!("{}: {e}", path.display()))?;
    }
    if relocate {
        eprintln!(
            "mutation history read from {} and written to {}; the old file is kept and no longer read",
            superseded.display(),
            history_path.display()
        );
    }
    Ok(())
}
fn main() {
    if let Err(e) = run() {
        eprintln!("{e}");
        std::process::exit(1)
    }
}
