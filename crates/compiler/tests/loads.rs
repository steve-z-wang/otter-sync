//! `load Name(inputs) { outputs }`: a native operation that fills Model lists
//! page by page. Its descriptor lives beside, never inside, Mutation and Query.
use axton_compiler::compile;
use serde_json::{Value, json};

const MODELS: &str = "enum Status { open done }
model Todo { id UUID title String @@id(id) }
model Note { id String body String? @@id(id) }
";

fn source(declaration: &str) -> String {
    format!("{MODELS}{declaration}")
}

#[test]
fn native_load_declarations_compile_to_their_own_descriptors() {
    let config = compile(&source(
        "load ProjectTodos(projectId UUID, status Status?, tags String[]) {\n  todos Todo[]\n  notes Note[]\n}",
    ))
    .unwrap();
    let load = &config["loads"][0];
    assert_eq!(
        load,
        &json!({
            "name":"ProjectTodos","version":1,
            "inputs":[
                {"kind":"value","name":"projectId","type":{"kind":"scalar","name":"uuid"},"nullable":false,"list":false,"required":true,"cardinality":"single"},
                {"kind":"value","name":"status","type":{"kind":"enum","name":"Status"},"nullable":true,"list":false,"required":true,"cardinality":"single"},
                {"kind":"value","name":"tags","type":{"kind":"scalar","name":"string"},"nullable":false,"list":true,"required":true,"cardinality":"list"}
            ],
            "outputs":[
                {"name":"todos","kind":"model","cardinality":"list","source":"handlerIdentity","model":"Todo","modelReadVersion":1,
                 "handlerType":{"kind":"identity","model":"Todo","fields":[{"name":"id","type":{"kind":"scalar","name":"uuid"}}]}},
                {"name":"notes","kind":"model","cardinality":"list","source":"handlerIdentity","model":"Note","modelReadVersion":1,
                 "handlerType":{"kind":"identity","model":"Note","fields":[{"name":"id","type":{"kind":"scalar","name":"string"}}]}}
            ],
            "input":{"models":[],"enums":[{"name":"Status","values":["open","done"]}]},
            "outputEnums":[]
        })
    );
    assert_eq!(config["schema"]["loads"], config["loads"]);
    assert_eq!(config["actions"], json!([]));
    assert!(
        config["schema"]
            .get("actions")
            .is_none_or(|a| a == &json!([]))
    );
    let schema = axton_core::Schema::from_value(config["schema"].clone()).unwrap();
    assert!(schema.load("ProjectTodos", 1).is_ok());
    assert!(schema.action("ProjectTodos", 1).is_err());

    let versioned = compile(&source("@version(3) load AllTodos() { todos Todo[] }")).unwrap();
    assert_eq!(versioned["loads"][0]["version"], 3);
    assert_eq!(versioned["loads"][0]["inputs"], json!([]));
    assert_eq!(
        versioned["loads"][0]["input"],
        json!({"models":[],"enums":[]})
    );
}

#[test]
fn loads_coexist_with_mutations_and_queries() {
    let config = compile(&source(
        "mutation Rename(title String) { id String }\nquery Count() { n Int }\nload AllTodos() { todos Todo[], notes Note[] }",
    ))
    .unwrap();
    assert_eq!(config["actions"].as_array().unwrap().len(), 2);
    assert_eq!(config["loads"].as_array().unwrap().len(), 1);
    assert!(
        config["actions"]
            .as_array()
            .unwrap()
            .iter()
            .all(|a| a["name"] != "AllTodos")
    );
}

#[test]
fn once_and_refresh_never_enter_a_load_descriptor() {
    // Business inputs may use these names; call-site options live elsewhere.
    let config = compile(&source(
        "load Todos(once Boolean, refresh Boolean) { todos Todo[] }",
    ))
    .unwrap();
    let load = config["loads"][0].as_object().unwrap();
    let keys: Vec<&str> = load.keys().map(String::as_str).collect();
    let mut expected = vec![
        "input",
        "inputs",
        "name",
        "outputEnums",
        "outputs",
        "version",
    ];
    expected.sort_unstable();
    let mut keys = keys;
    keys.sort_unstable();
    assert_eq!(keys, expected);
    let history = axton_compiler::reconcile_load_history(&config, None).unwrap();
    let snapshot = history["loads"]["Todos"]["1"].as_object().unwrap();
    let mut keys: Vec<&str> = snapshot.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(keys, expected);
    for directive in ["@once load", "@refresh load"] {
        let error =
            compile(&source(&format!("{directive} Todos() {{ todos Todo[] }}"))).unwrap_err();
        assert!(
            error.contains("unsupported declaration directive"),
            "{error}"
        );
    }
}

#[test]
fn schemas_without_loads_emit_no_load_members() {
    let config = compile(&source("query Count() { n Int }")).unwrap();
    assert!(config.get("loads").is_none());
    assert!(config["schema"].get("loads").is_none());
    // Load-only generated names are free when no Load is declared.
    compile("model LoadStatus { id String @@id(id) } query Count() { n Int }").unwrap();
    compile("enum JsonValue { a } model Todo { id String @@id(id) }").unwrap();
}

#[test]
fn load_shapes_are_refused_with_positions() {
    for (declaration, needle) in [
        (
            "load Bad() { count Int }",
            "4:14: Load Bad output count must be a non-null list of Model identities",
        ),
        (
            "load Bad() { counts Int[] }",
            "4:14: Load Bad output counts must be a non-null list of Model identities",
        ),
        (
            "load Bad() { todo Todo }",
            "4:14: Load Bad output todo must be a non-null list of Model identities",
        ),
        (
            "load Bad() { todo Todo? }",
            "4:14: Load Bad output todo must be a non-null list of Model identities",
        ),
        (
            "load Bad() { todos Todo[]? }",
            "nullable lists are unsupported",
        ),
        ("load Bad()", "expected {"),
        (
            "load Bad() {}",
            "4:13: Load Bad requires at least one output",
        ),
        ("load Bad { todos Todo[] }", "expected ("),
        (
            "load Bad(todo Todo.create) { todos Todo[] }",
            "4:10: Load Bad cannot take Model operand todo",
        ),
        (
            "@sequence(after: []) load Bad() { todos Todo[] }",
            "4:1: Load Bad cannot declare @sequence",
        ),
        (
            "load Bad() { todos Todo[] todos Note[] }",
            "4:27: duplicate Load output todos",
        ),
        (
            "load Bad(a String, a Int) { todos Todo[] }",
            "4:20: duplicate Load input a",
        ),
        (
            "load Bad(a Todo) { todos Todo[] }",
            "unknown or unsupported Load type Todo on a",
        ),
        (
            "load Bad() { todos Missing[] }",
            "unknown or unsupported Load type Missing on todos",
        ),
        (
            "load Bad() { todos Todo[] @deprecated }",
            "unsupported Load output directive on todos",
        ),
        (
            "load Bad(a String @default(\"x\")) { todos Todo[] }",
            "@default is a Model field attribute",
        ),
        (
            "load Get() { todos Todo[] }",
            "4:1: Load name Get is reserved",
        ),
        (
            "load list() { todos Todo[] }",
            "4:1: Load name list is reserved",
        ),
        (
            "load Invalidate() { todos Todo[] }",
            "4:1: Load name Invalidate is reserved",
        ),
        (
            "load Client() { todos Todo[] }",
            "4:1: Load name Client is reserved",
        ),
        (
            "load ToString() { todos Todo[] }",
            "4:1: Load name ToString is reserved",
        ),
        (
            "load Todo() { todos Todo[] }",
            "4:1: Load Todo collides with a model or enum",
        ),
    ] {
        let error = compile(&source(declaration)).unwrap_err();
        assert!(error.contains(needle), "{declaration}: {error}");
    }
}

#[test]
fn loads_share_the_normalized_operation_namespace() {
    for (declarations, needle) in [
        (
            "query Find() { n Int }\nload Find() { todos Todo[] }",
            "5:1: duplicate operation Find",
        ),
        (
            "load Find() { todos Todo[] }\nmutation find() { n Int }",
            "5:1: duplicate operation find",
        ),
        (
            "load ProjectTodos() { todos Todo[] }\nload projectTodos() { todos Todo[] }",
            "5:1: duplicate operation projectTodos",
        ),
        (
            "load Find() { todos Todo[] }\n@version(2) load Find() { todos Todo[] }",
            "5:13: duplicate operation Find",
        ),
    ] {
        let error = compile(&source(declarations)).unwrap_err();
        assert!(error.contains(needle), "{declarations}: {error}");
        assert!(error.contains("mutation, query and load"), "{error}");
    }
    // Get/list/invalidate are Load management members, not Query ones.
    compile(&source("query Get() { n Int }")).unwrap();
}

#[test]
fn load_only_generated_names_are_reserved_only_beside_a_load() {
    for name in [
        "Loads",
        "Load",
        "LoadStatus",
        "LoadPhase",
        "LoadOptions",
        "LoadError",
        "LoadException",
        "LoadNext",
        "JsonValue",
        "LoadContext",
    ] {
        let model = format!("model {name} {{ id String @@id(id) }}\n");
        compile(&format!("{model}{MODELS}")).unwrap();
        let error = compile(&format!("{model}{MODELS}load All() {{ todos Todo[] }}")).unwrap_err();
        assert!(
            error.starts_with("1:1:") && error.contains(name),
            "{name}: {error}"
        );
        let enumeration = format!("enum {name} {{ a }}\n{MODELS}load All() {{ todos Todo[] }}");
        assert!(compile(&enumeration).is_err(), "enum {name}");
    }
}

#[test]
fn load_contract_identifiers_are_checked_against_other_generated_names() {
    let error = compile(&format!(
        "model TodosInput {{ id String @@id(id) }}\n{MODELS}load Todos() {{ todos Todo[] }}"
    ))
    .unwrap_err();
    assert!(
        error.starts_with("5:1:") && error.contains("TodosInput"),
        "{error}"
    );
    let value: Value = compile(&source("load Todos() { todos Todo[] }")).unwrap();
    assert!(axton_compiler::check_action_names(&value).is_ok());
}
