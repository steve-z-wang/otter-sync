//! Source `@default` compiles to create-only `createDefault` metadata
//! ([#27](https://github.com/zanminwang/axton/issues/27)).
use axton_compiler::compile;
use serde_json::{Value, json};

const TODO: &str = "enum Status { open closed }
model Todo {
  id String @default(uuid())
  title String @default(\"\")
  done Boolean @default(false)
  priority Int @default(0)
  status Status @default(open)
  createdAt DateTime @default(now())
  @@id(id)
}
mutation AddTodo(todo Todo.create) {}
";

fn field<'a>(config: &'a Value, model: &str, name: &str) -> &'a Value {
    config["schema"]["models"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["name"] == model)
        .unwrap()["fields"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["name"] == name)
        .unwrap()
}

#[test]
fn the_todo_example_compiles_to_tagged_create_defaults() {
    let config = compile(TODO).unwrap();
    let expect = [
        ("id", json!({"kind":"uuid"})),
        ("title", json!({"kind":"literal","value":""})),
        ("done", json!({"kind":"literal","value":false})),
        ("priority", json!({"kind":"literal","value":0})),
        ("status", json!({"kind":"literal","value":"open"})),
        ("createdAt", json!({"kind":"now"})),
    ];
    for (name, create_default) in expect {
        let f = field(&config, "Todo", name);
        assert_eq!(f["createDefault"], create_default, "{name}");
        // Source @default never feeds the internal migration default.
        assert!(f.get("default").is_none(), "{name}: {f}");
    }
    axton_core::Schema::from_value(config["schema"].clone()).unwrap();
    // Nothing is evaluated at compile time: the same source renders the same bytes.
    assert_eq!(compile(TODO).unwrap(), config);
}

#[test]
fn literals_are_normalized_by_their_field_contract() {
    let config = compile(
        "enum Mood { calm busy }
model Reading {
  id UUID @default(uuid())
  score Float @default(-1.5)
  weight Float @default(2)
  small Float @default(1e-3)
  note String? @default(\"q \\\"x\\\" \\\\ \\n $y ''' \\\"\\\"\\\"\")
  mood Mood? @default(busy)
  takenAt DateTime @default(\"2026-01-02T03:04:05+02:00\")
  ref UUID @default(\"01890F47-1234-7123-8123-123456789ABC\")
  label String @default(uuid())
  low Int @default(-9007199254740991)
  plain String?
  @@id(id)
}
",
    )
    .unwrap();
    let default = |name: &str| field(&config, "Reading", name)["createDefault"].clone();
    assert_eq!(default("id"), json!({"kind":"uuid"}));
    assert_eq!(default("score"), json!({"kind":"literal","value":-1.5}));
    assert_eq!(default("weight"), json!({"kind":"literal","value":2.0}));
    assert_eq!(default("small"), json!({"kind":"literal","value":0.001}));
    assert_eq!(
        default("note"),
        json!({"kind":"literal","value":"q \"x\" \\ \n $y ''' \"\"\""})
    );
    assert_eq!(default("mood"), json!({"kind":"literal","value":"busy"}));
    assert_eq!(
        default("takenAt"),
        json!({"kind":"literal","value":"2026-01-02T01:04:05.000Z"})
    );
    assert_eq!(
        default("ref"),
        json!({"kind":"literal","value":"01890f47-1234-7123-8123-123456789abc"})
    );
    assert_eq!(default("label"), json!({"kind":"uuid"}));
    assert_eq!(
        default("low"),
        json!({"kind":"literal","value":-9007199254740991i64})
    );
    assert_eq!(
        default("plain"),
        Value::Null,
        "no metadata without @default"
    );
    axton_core::Schema::from_value(config["schema"].clone()).unwrap();
}

#[test]
fn invalid_defaults_are_rejected_at_the_declaration() {
    let model = |field: &str| {
        format!("enum Status {{ open closed }}\nmodel A {{\n id UUID\n {field}\n @@id(id)\n}}\n")
    };
    let cases: Vec<(String, &str)> = vec![
        (model("n Int @default(1) @default(2)"), "duplicate field directive"),
        (model("tags String[] @default(\"a\")"), "@default is unsupported on list field tags"),
        (model("n Int @default(null)"), "@default(null) is unsupported"),
        (model("n Int? @default(null)"), "@default(null) is unsupported"),
        (model("n Int @default()"), "expected default value"),
        (model("n Int @default(\"1\")"), "invalid default for n"),
        (model("n Int @default(1.5)"), "invalid default for n"),
        (model("n Int @default(9007199254740992)"), "invalid default for n"),
        (model("b Boolean @default(1)"), "invalid default for b"),
        (model("s String @default(1)"), "invalid default for s"),
        (model("s String @default(word)"), "invalid default for s"),
        (model("f Float @default(\"1\")"), "invalid default for f"),
        (model("s Status @default(pending)"), "invalid default for s"),
        (model("s Status @default(\"open\")"), "invalid default for s"),
        (model("t DateTime @default(\"2026-13-01T00:00:00Z\")"), "invalid default for t"),
        (model("t DateTime @default(\"2026-01-01\")"), "invalid default for t"),
        (model("u UUID @default(\"not-a-uuid\")"), "invalid default for u"),
        (model("n Int @default(uuid())"), "uuid() requires a String or UUID field"),
        (model("t String @default(now())"), "now() requires a DateTime field"),
        (model("s String @default(cuid())"), "unknown default function cuid()"),
        (model("s String @default(uuid(4))"), "uuid() takes no arguments"),
        (model("t DateTime @default(now(\"utc\"))"), "now() takes no arguments"),
        (model("n Int @default(01)"), "invalid default number"),
        (
            "model P { id UUID @@id(id) }\nmodel A {\n id UUID\n pId UUID\n p P @reference(via: [pId]) @default(uuid())\n @@id(id)\n}\n".into(),
            "@default is unsupported on relation field p",
        ),
        (
            "model A { id UUID @@id(id) }\nmutation M(x String @default(\"a\"))".into(),
            "@default is a Model field attribute",
        ),
        (
            "model A { id UUID @@id(id) }\nmutation M(x String) { y String @default(\"a\") }".into(),
            "@default is a Model field attribute",
        ),
    ];
    for (source, expected) in cases {
        let err = compile(&source).unwrap_err();
        assert!(err.contains(expected), "{source}\n=> {err}");
        let line = err.split(':').next().unwrap().parse::<usize>().unwrap();
        assert!(line >= 2, "diagnostic names the declaration: {err}");
    }
}

const TYPED: &str = "enum Status { open closed }
model Todo {
  id String @default(uuid())
  title String @default(\"\")
  status Status @default(open)
  createdAt DateTime @default(now())
  note String? @default(\"n\")
  memo String?
  rank Int
  @@id(id)
}
mutation AddTodo(todo Todo.create, maybe Todo.create?, many Todo.create[]) { saved Todo }
";

fn section<'a>(text: &'a str, start: &str) -> &'a str {
    let from = text
        .find(start)
        .unwrap_or_else(|| panic!("missing {start}\n{text}"));
    let rest = &text[from..];
    &rest[..rest.find("\n}").map(|i| i + 2).unwrap_or(rest.len())]
}

#[test]
fn typescript_create_inputs_make_only_defaulted_fields_optional() {
    let config = compile(TYPED).unwrap();
    let ts = axton_compiler::typescript(&config);
    assert_eq!(
        section(&ts, "export interface TodoCreate {"),
        "export interface TodoCreate {\n id?: string;\n title?: string;\n status?: Status;\n createdAt?: Date;\n note?: string | null;\n memo: string | null;\n rank: number;\n}"
    );
    // The full record stays complete.
    assert!(
        section(&ts, "export interface Todo {").contains(" id: string;\n title: string;"),
        "{ts}"
    );
    // Encoders omit missing fields instead of encoding undefined.
    assert!(ts.contains("export function encodeTodoCreate(value:TodoCreate):Record<string,unknown> { return {\n ...(value.id !== undefined ? { id: value.id } : {}),"), "{ts}");
    assert!(ts.contains(" ...(value.createdAt !== undefined ? { createdAt: value.createdAt.toISOString() } : {}),"), "{ts}");
    assert!(ts.contains(" ...(value.note !== undefined ? { note: value.note == null ? null : value.note } : {}),"), "{ts}");
    assert!(ts.contains(" rank: value.rank,\n}; }"), "{ts}");
    assert!(ts.contains("export function encodeTodoCreateIdentity(value:TodoCreate):Record<string,unknown> { return {\n ...(value.id !== undefined ? { id: value.id } : {}),\n}; }"), "{ts}");
    assert!(ts.contains(" create(value:TodoCreate):Promise<void> { return this.port.direct({model:'Todo',op:'create',identity:encodeTodoCreateIdentity(value),values:encodeTodoPatch(value)}); }"), "{ts}");
    let input = section(&ts, "export interface AddTodoInput {");
    assert!(
        input.contains(" todo: TodoCreate;\n maybe?: TodoCreate | null;\n many: TodoCreate[];"),
        "{input}"
    );
    assert!(ts.contains(" todo: encodeTodoCreate(args.todo),"), "{ts}");
    assert!(
        ts.contains(" many: args.many.map(value => encodeTodoCreate(value)),"),
        "{ts}"
    );
    // Handlers receive the expanded, complete values.
    let backend = axton_compiler::backend_typescript(&config, "@axtonjs/server");
    let handler = section(&backend, "export interface AddTodoInput {");
    assert!(
        handler.contains(" todo: Todo;\n maybe?: Todo | null;\n many: Todo[];"),
        "{handler}"
    );
    assert!(!backend.contains("TodoCreate"), "{backend}");
}

#[test]
fn dart_create_inputs_distinguish_omission_from_explicit_null() {
    let config = compile(TYPED).unwrap();
    let dart = axton_compiler::dart(&config);
    assert!(
        dart.contains(
            "abstract interface class TodoCreateInput { Map<String,dynamic> toCreateRecord(); }"
        ),
        "{dart}"
    );
    assert!(
        dart.contains("class Todo implements TodoCreateInput {"),
        "{dart}"
    );
    let create = section(&dart, "class TodoCreate implements TodoCreateInput {");
    for line in [
        " final String? id;",
        " final String? title;",
        " final Status? status;",
        " final DateTime? createdAt;",
        " final Present<String?>? note;",
        " final String? memo;",
        " final int rank;",
        " const TodoCreate({this.id,this.title,this.status,this.createdAt,this.note,required this.memo,required this.rank});",
        " if (id != null) 'id': id!,",
        " if (createdAt != null) 'createdAt': createdAt!.toAxtonPrecision().toIso8601String(),",
        " if (note != null) 'note': note!.value == null ? null : note!.value!,",
        " 'memo': memo == null ? null : memo!,",
        " 'rank': rank,",
    ] {
        assert!(create.contains(line), "{line}\n{create}");
    }
    // The client accepts either create input; handlers receive complete records.
    assert!(dart.contains("required TodoCreateInput todo, TodoCreateInput? maybe, required List<TodoCreateInput> many"), "{dart}");
    let input = section(&dart, "class AddTodoInput implements _DartActionRecord {");
    assert!(
        input.contains(" final Todo todo;\n final Todo? maybe;\n final List<Todo> many;"),
        "{input}"
    );
    assert!(
        dart.contains(" Future<void> create(TodoCreateInput value) {"),
        "{dart}"
    );
    assert!(
        !dart.contains("r'''"),
        "the schema is embedded as an escaped string"
    );
}

#[test]
fn model_only_schemas_expose_create_inputs() {
    let config = compile(
        "model Note {\n id UUID @default(uuid())\n body String @default(\"\")\n @@id(id)\n}\n",
    )
    .unwrap();
    let ts = axton_compiler::typescript(&config);
    assert!(
        ts.contains("export interface NoteCreate {\n id?: string;\n body?: string;\n}"),
        "{ts}"
    );
    assert!(
        ts.contains(" create(value:NoteCreate):Promise<void>"),
        "{ts}"
    );
    let dart = axton_compiler::dart(&config);
    assert!(
        dart.contains("class NoteCreate implements NoteCreateInput {"),
        "{dart}"
    );
    assert!(
        dart.contains(" Future<void> create(NoteCreateInput value) {"),
        "{dart}"
    );
}

#[test]
fn generated_names_reserve_the_create_input_interface() {
    let err =
        compile("model Todo { id UUID @@id(id) }\nmodel TodoCreateInput { id UUID @@id(id) }\n")
            .unwrap_err();
    assert!(err.contains("TodoCreateInput"), "{err}");
}
