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
        "LoadHandlerCall",
        "LoadInvalidations",
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
fn exactly_the_emitted_per_load_backend_identifiers_are_reserved() {
    // The backend declares `{Name}Input` and `{Name}HandlerOutput` per
    // current Load and `{Name}V{n}…` per retained version; nothing else.
    for name in ["TodosInput", "TodosHandlerOutput"] {
        let error = compile(&format!(
            "model {name} {{ id String @@id(id) }}\n{MODELS}load Todos() {{ todos Todo[] }}"
        ))
        .unwrap_err();
        assert!(
            error.contains(&format!("generated identifier {name}")),
            "{name}: {error}"
        );
    }
    for name in ["TodosOutput", "TodosV1Input", "TodosV1HandlerOutput"] {
        compile(&format!(
            "model {name} {{ id String @@id(id) }}\n{MODELS}load Todos() {{ todos Todo[] }}"
        ))
        .unwrap();
    }
    // A retained version reserves its versioned names.
    let mut retained: Value =
        compile(&source("@version(2) load Todos() { todos Todo[] }")).unwrap();
    let mut old = retained["loads"][0].clone();
    old["version"] = json!(1);
    retained["loads"] = json!([old, retained["loads"][0].clone()]);
    assert!(axton_compiler::check_action_names(&retained).is_ok());
    retained["schema"]["models"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"TodosV1HandlerOutput","identity":["id"],"fields":[]}));
    let error = axton_compiler::check_action_names(&retained).unwrap_err();
    assert!(
        error.contains("TodosV1HandlerOutput") && error.contains("Load Todos v1"),
        "{error}"
    );
    let value: Value = compile(&source("load Todos() { todos Todo[] }")).unwrap();
    assert!(axton_compiler::check_action_names(&value).is_ok());
    // A hand-built config is checked without panicking on missing members.
    assert!(
        axton_compiler::check_action_names(&json!({
            "schema":{"models":[],"enums":[]},
            "loads":[{"name":"Todos"}]
        }))
        .is_ok()
    );
    let error = axton_compiler::check_action_names(&json!({
        "schema":{"models":[{"name":"LoadStatus"}],"enums":[]},
        "loads":[{}]
    }))
    .unwrap_err();
    assert!(error.contains("LoadStatus"), "{error}");
}

#[test]
fn the_backend_declares_typed_load_handlers_beside_loaders() {
    let config = compile(&source(
        "load ProjectTodos(projectId UUID, status Status?, tags String[], at DateTime) {\n  todos Todo[]\n  notes Note[]\n}\nload AllNotes() { notes Note[] }",
    ))
    .unwrap();
    let ts = axton_compiler::backend_typescript(&config, "@axtonjs/server");
    for expected in [
        // Enum inputs name the generated enum type.
        "import type { Todo as TodoRecord, TodoIdentity, TodoPatch, Note as NoteRecord, NoteIdentity, NotePatch, Status } from \"./generated.ts\";\n",
        "export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };\n",
        "export type LoadNext = null | { state: JsonValue };\n",
        // An add-only Scope handle: the same lower-first accessors and
        // mixed RecordRef list as a Mutation's Scope, without remove.
        "export interface LoadScope { readonly add: ScopeAdd; tag(labels: string | readonly string[]): { readonly add: ScopeRecords } }\n",
        "export interface LoadContext<Tx> {\n tx: Tx;\n userId: string;\n callId: string;\n loadId: string;\n scope(name: string): LoadScope;\n}\n",
        "export type LoadHandlerCall<Tx, Args> = { ctx: LoadContext<Tx>; args: Args; continuation: LoadNext };\n",
        "export interface ProjectTodosInput {\n projectId: string;\n status: Status | null;\n tags: string[];\n at: Date;\n}\n",
        "export interface ProjectTodosHandlerOutput {\n data: {\n  todos: TodoIdentity[];\n  notes: NoteIdentity[];\n };\n next: LoadNext;\n}\n",
        "export interface AllNotesInput {\n}\n",
        "export interface Loads<Tx> {\n allNotes: { v1(call: LoadHandlerCall<Tx, AllNotesInput>): Promise<AllNotesHandlerOutput> } | ((call: LoadHandlerCall<Tx, AllNotesInput>) => Promise<AllNotesHandlerOutput>);\n projectTodos: { v1(call: LoadHandlerCall<Tx, ProjectTodosInput>): Promise<ProjectTodosHandlerOutput> } | ((call: LoadHandlerCall<Tx, ProjectTodosInput>) => Promise<ProjectTodosHandlerOutput>);\n}\n",
        // A schema with Loads requires the `loads` map.
        "\"loaders\" | \"loads\"> & { handlers?: Handlers<Tx>; mutations?: Mutations<Tx>; queries?: Queries<Tx>; loaders: Loaders<Tx>; loads: Loads<Tx> };\n",
        " loaders: options.loaders as unknown as BackendOptions<Tx>[\"loaders\"], loads: options.loads as unknown as BackendOptions<Tx>[\"loads\"] });\n",
    ] {
        assert!(ts.contains(expected), "{expected}\n---\n{ts}");
    }
    let context = &ts[ts.find("export interface LoadContext<Tx> {").unwrap()..];
    let context = &context[..context.find("\n}\n").unwrap()];
    assert!(
        !context.contains("remove") && !context.contains("touch") && !context.contains("channel"),
        "{context}"
    );
    assert!(ts.contains("export interface Scope {"), "{ts}");
    assert!(!ts.contains("export interface Channel"), "{ts}");
    assert!(ts.contains("export interface MutationContext<Tx> {\n tx: Tx;\n userId: string;\n callId: string;\n scope(name: string): Scope;\n touch: Touch;\n}\n"), "{ts}");
    // Handler types belong to the backend artifact only.
    assert!(!axton_compiler::typescript(&config).contains("LoadHandlerCall"));
}

#[test]
fn retained_load_versions_register_together_with_their_own_contracts() {
    let mut config = compile(
        "enum Status { open done archived }\nmodel Todo { id UUID title String status Status @@id(id) @@version(2) }\n@version(2) load Todos(status Status) { todos Todo[] }",
    )
    .unwrap();
    // The retained v1 reads Todo v1 and accepts the enum values of its time.
    let mut old = config["loads"][0].clone();
    old["version"] = json!(1);
    old["input"]["enums"] = json!([{"name":"Status","values":["open","done"]}]);
    old["outputs"][0]["modelReadVersion"] = json!(1);
    config["loads"] = json!([old, config["loads"][0].clone()]);
    let mut current = config["schema"]["models"][0].clone();
    current["enums"] = config["schema"]["enums"].clone();
    let todo_v1 = json!({"name":"Todo","version":1,"identity":["id"],"fields":[{"name":"id","nullable":false,"type":{"kind":"scalar","name":"uuid"}},{"name":"status","nullable":false,"type":{"kind":"enum","name":"Status"}}],"enums":[{"name":"Status","values":["open","done"]}]});
    config["backendModels"] = json!([todo_v1, current]);
    assert!(axton_compiler::check_action_names(&config).is_ok());
    let ts = axton_compiler::backend_typescript(&config, "@axtonjs/server");
    for expected in [
        "export interface TodosV1Input {\n status: \"open\" | \"done\";\n}\n",
        "export interface TodosV1HandlerOutput {\n data: {\n  todos: TodoV1Identity[];\n };\n next: LoadNext;\n}\n",
        "export interface TodosInput {\n status: Status;\n}\n",
        "export interface TodosHandlerOutput {\n data: {\n  todos: TodoIdentity[];\n };\n next: LoadNext;\n}\n",
        // No bare-function shorthand beyond a v1-only Load.
        " todos: { v1(call: LoadHandlerCall<Tx, TodosV1Input>): Promise<TodosV1HandlerOutput>; v2(call: LoadHandlerCall<Tx, TodosInput>): Promise<TodosHandlerOutput> };\n",
        "export interface TodoV1Identity {\n id: string;\n}\n",
        // Every retained version shares the one context: it enrolls by the
        // current identity, like a Mutation's Scope.
        "export interface LoadScope { readonly add: ScopeAdd; tag(labels: string | readonly string[]): { readonly add: ScopeRecords } }\n",
        " scope(name: string): LoadScope;\n",
    ] {
        assert!(ts.contains(expected), "{expected}\n---\n{ts}");
    }
    let only_v2 =
        compile("model Todo { id UUID @@id(id) }\n@version(2) load Todos() { todos Todo[] }")
            .unwrap();
    let ts = axton_compiler::backend_typescript(&only_v2, "@axtonjs/server");
    assert!(
        ts.contains(
            " todos: { v2(call: LoadHandlerCall<Tx, TodosInput>): Promise<TodosHandlerOutput> };\n"
        ),
        "{ts}"
    );
}

#[test]
fn backends_without_loads_declare_no_load_types() {
    for source in [
        "model Todo { id UUID @@id(id) }",
        "enum Status { open done }\nmodel Todo { id UUID s Status @@id(id) }\nquery Count(s Status) { n Int }",
    ] {
        let ts = axton_compiler::backend_typescript(&compile(source).unwrap(), "@axtonjs/server");
        for absent in [
            "JsonValue",
            "LoadNext",
            "LoadContext",
            "LoadHandlerCall",
            "Loads<Tx>",
            "\"loads\"",
            "loads:",
        ] {
            assert!(!ts.contains(absent), "{absent}: {ts}");
        }
    }
}

#[test]
fn clients_without_loads_generate_no_loads_facade() {
    for source in [
        "model Todo { id UUID @@id(id) }",
        "enum Status { open done }\nmodel Todo { id UUID s Status @@id(id) }\nquery Count(s Status) { n Int }\nmutation Ping()",
    ] {
        let config = compile(source).unwrap();
        let ts = axton_compiler::typescript(&config);
        let client = axton_compiler::client_typescript(&config, "@axtonjs/client");
        let dart = axton_compiler::dart(&config);
        for (output, text) in [
            ("generated.ts", &ts),
            ("client.ts", &client),
            ("dart", &dart),
        ] {
            for absent in [
                "makeLoads",
                "loads",
                "Load,",
                "LoadStatus",
                "LoadInvalidations",
                "startLoad",
            ] {
                assert!(!text.contains(absent), "{output} {absent}: {text}");
            }
        }
    }
}

#[test]
fn the_client_starts_invalidates_and_reattaches_typed_loads() {
    let config = compile(&source(
        "load ProjectTodos(projectId UUID, status Status?, tags String[], at DateTime) { todos Todo[] notes Note[] }\nload AllNotes() { notes Note[] }\nload Flagged(once Boolean, refresh String) { notes Note[] }",
    ))
    .unwrap();
    let ts = axton_compiler::typescript(&config);
    for expected in [
        "import type { Load, LoadOptions, LoadStatus } from './client.ts';",
        "export interface ProjectTodosInput {\n projectId: string;\n status: Status | null;\n tags: string[];\n at: Date;\n}",
        " at: args.at.toISOString(),",
        " projectTodos: (args:ProjectTodosInput, options?:LoadOptions):Promise<Load<'ProjectTodos'>> => port.startLoad('ProjectTodos',1,encodeProjectTodosInput(args),options),",
        " allNotes: (args:AllNotesInput, options?:LoadOptions):Promise<Load<'AllNotes'>>",
        " get: (id:string):Promise<Load|null> => port.getLoad(id),",
        " list: (options?:{limit?:number}):Promise<LoadStatus[]> => port.listLoads(options),",
        "  projectTodos: (args:ProjectTodosInput):Promise<void> => port.invalidateLoad('ProjectTodos',encodeProjectTodosInput(args)),",
    ] {
        assert!(ts.contains(expected), "{expected}: {ts}");
    }
    // Options are a separate argument, so business inputs keep their names.
    assert!(ts.contains("export interface FlaggedInput {\n once: boolean;\n refresh: string;\n}"));
    let client = axton_compiler::client_typescript(&config, "@axtonjs/client");
    for expected in [
        "export { LoadError, type Load, type LoadOptions, type LoadPhase, type LoadStatus } from \"@axtonjs/client\";",
        "import { makeLoads } from \"./generated.ts\";",
        " readonly loads: ReturnType<typeof makeLoads>;",
        "this.loads = makeLoads(client); ",
    ] {
        assert!(client.contains(expected), "{expected}: {client}");
    }
    let dart = axton_compiler::dart(&config);
    for expected in [
        "export 'package:axton/axton.dart' show Load, LoadStatus, LoadPhase, LoadException;\nclass Present<T>",
        " late final LoadInvalidations invalidate = LoadInvalidations(client);",
        " Future<Load> projectTodos({required String projectId, required Status? status, required List<String> tags, required DateTime at, bool once = false, bool refresh = false}) => client.startLoad('ProjectTodos', 1, {'projectId': projectId, 'status': status == null ? null : status.name, 'tags': tags, 'at': at.toAxtonPrecision().toIso8601String()}, once: once, refresh: refresh);",
        " Future<Load> allNotes({bool once = false, bool refresh = false}) => client.startLoad('AllNotes', 1, {}, once: once, refresh: refresh);",
        // Business inputs own `once` and `refresh`: the controls fall back.
        " Future<Load> flagged({required bool once, required String refresh, bool callOnce = false, bool callRefresh = false}) => client.startLoad('Flagged', 1, {'once': once, 'refresh': refresh}, once: callOnce, refresh: callRefresh);",
        " Future<Load?> get(String id) => client.getLoad(id);",
        " Future<List<LoadStatus>> list({int limit = 50}) => client.listLoads(limit: limit);",
        " Future<void> allNotes() => client.invalidateLoad('AllNotes', {});",
        " late final Loads loads = Loads(client);",
    ] {
        assert!(dart.contains(expected), "{expected}: {dart}");
    }
}

#[test]
fn a_load_input_named_client_does_not_shadow_the_dart_route() {
    let config = compile(&source(
        "load ByClient(client String) { notes Note[] }\nload AllNotes() { notes Note[] }",
    ))
    .unwrap();
    let dart = axton_compiler::dart(&config);
    for expected in [
        " Future<Load> byClient({required String client, bool once = false, bool refresh = false}) => this.client.startLoad('ByClient', 1, {'client': client}, once: once, refresh: refresh);",
        " Future<void> byClient({required String client}) => this.client.invalidateLoad('ByClient', {'client': client});",
        // Other Loads keep the plain field reference.
        " Future<Load> allNotes({bool once = false, bool refresh = false}) => client.startLoad('AllNotes', 1, {}, once: once, refresh: refresh);",
        " Future<void> allNotes() => client.invalidateLoad('AllNotes', {});",
    ] {
        assert!(dart.contains(expected), "{expected}: {dart}");
    }
}

#[test]
fn the_client_starts_the_newest_retained_load_version() {
    let mut config: Value = compile(&source("@version(2) load Todos() { todos Todo[] }")).unwrap();
    let mut old = config["loads"][0].clone();
    old["version"] = json!(1);
    config["loads"] = json!([old, config["loads"][0].clone()]);
    let ts = axton_compiler::typescript(&config);
    assert!(ts.contains("port.startLoad('Todos',2,"), "{ts}");
    assert_eq!(ts.matches("export interface TodosInput").count(), 1);
    assert!(
        !ts.contains("TodosV1Input"),
        "the client starts only the newest version"
    );
    let dart = axton_compiler::dart(&config);
    assert!(dart.contains("client.startLoad('Todos', 2, {}"), "{dart}");
}
