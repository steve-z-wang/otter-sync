use axton_compiler::compile;
use axton_compiler::validate::{
    ActionInput, ActionOutputSource, ActionOutputType, Cardinality, FieldType, Scalar,
};
use axton_compiler::{parse, validate};

#[test]
fn generated_actions_bind_to_shared_runtime_and_backend() {
    let descriptor = compile("model Todo { id String at DateTime @@id(id) } mutation Touch(todo Todo.update<at>, when DateTime) { echoed DateTime }").unwrap();
    let model = axton_compiler::typescript(&descriptor);
    let client = axton_compiler::client_typescript(&descriptor, "@axton/client");
    let backend = axton_compiler::backend_typescript(&descriptor, "@axton/server");
    assert!(model.contains("import type { Call, CallOptions, OnceOptions } from './client.ts'"));
    assert!(client.contains("type CallOutcome"));
    assert!(client.contains("readonly mutations:"));
    assert!(client.contains("readonly queries:"));
    assert!(!client.contains("readonly actions:"), "{client}");
    assert!(model.contains("invokeAction"));
    assert!(backend.contains("export function createBackend"));
    assert!(backend.contains("MutationContext<Tx>"));
    assert!(backend.contains("QueryContext<Tx>"));
}

#[test]
fn retained_loader_identity_uses_its_own_datetime_contract() {
    let mut descriptor = compile("model Moment { at DateTime @@id(at) @@version(2) }").unwrap();
    let mut old = descriptor["schema"]["models"][0].clone();
    old["version"] = serde_json::json!(1);
    descriptor["backendModels"] =
        serde_json::json!([old, descriptor["schema"]["models"][0].clone()]);
    let backend = axton_compiler::backend_typescript(&descriptor, "@axton/server");
    assert!(backend.contains("v1(call: LoaderCall<Tx, MomentV1Identity>)"));
}

#[test]
fn action_values_and_explicit_outputs_are_typed() {
    let schema = parse("enum Status { active closed } model Todo { id String @@id(id) } mutation Search(query String?, labels String[]) { count Int status Status? statuses Status[] }").unwrap();
    let action = &validate(&schema).unwrap().actions[0];
    assert_eq!(
        action.inputs[0],
        ActionInput::Value {
            name: "query".into(),
            ty: FieldType::Scalar(Scalar::String),
            nullable: true,
            list: false
        }
    );
    assert_eq!(
        action.inputs[1],
        ActionInput::Value {
            name: "labels".into(),
            ty: FieldType::Scalar(Scalar::String),
            nullable: false,
            list: true
        }
    );
    assert_eq!(
        action
            .outputs
            .iter()
            .map(|x| x.cardinality)
            .collect::<Vec<_>>(),
        [
            Cardinality::Single,
            Cardinality::Optional,
            Cardinality::List
        ]
    );
    assert!(
        action
            .outputs
            .iter()
            .all(|x| x.source == ActionOutputSource::HandlerValue)
    );
    assert_eq!(
        action.outputs[1].ty,
        ActionOutputType::Value(FieldType::Enum("Status".into()))
    );
}

#[test]
fn action_model_operands_imply_no_outputs() {
    let schema = parse("model Todo { id String title String @@id(id) } mutation Edit(one Todo.create, maybe Todo.update<title>?, many Todo.delete[]) { related Todo? }").unwrap();
    let action = &validate(&schema).unwrap().actions[0];
    assert_eq!(action.inputs.len(), 3);
    assert_eq!(action.outputs.len(), 1);
    let related = &action.outputs[0];
    assert_eq!(related.name, "related");
    assert_eq!(related.cardinality, Cardinality::Optional);
    assert_eq!(related.ty, ActionOutputType::Model("Todo".into()));
    assert_eq!(related.source, ActionOutputSource::HandlerModelIdentity);
    assert_eq!(related.model_read_version, Some(1));
}

#[test]
fn action_model_operand_deprecation_is_rejected_at_the_operand() {
    let source = "model Todo { id String @@id(id) }\nmutation Edit(todo Todo.update @deprecated(reason: \"use other\"))";
    let err = validate(&parse(source).unwrap()).unwrap_err();
    assert!(err.starts_with("2:"), "{err}");
    assert!(err.contains("todo") && err.contains("deprecated"), "{err}");
}

#[test]
fn action_void_and_explicit_model_outputs_keep_their_shapes() {
    let schema = parse("model Todo { id String @@id(id) @@version(3) } mutation Void() mutation Load() { one Todo maybe Todo? many Todo[] }").unwrap();
    let valid = validate(&schema).unwrap();
    assert!(valid.actions[0].outputs.is_empty());
    let outputs = &valid.actions[1].outputs;
    assert_eq!(
        outputs.iter().map(|x| x.cardinality).collect::<Vec<_>>(),
        [
            Cardinality::Single,
            Cardinality::Optional,
            Cardinality::List
        ]
    );
    assert!(
        outputs
            .iter()
            .all(|x| x.ty == ActionOutputType::Model("Todo".into())
                && x.source == ActionOutputSource::HandlerModelIdentity
                && x.model_read_version == Some(3))
    );
}

#[test]
fn action_model_operations_keep_each_operand_cardinality() {
    for operation in ["create", "update", "delete"] {
        for (suffix, expected) in [
            ("", Cardinality::Single),
            ("?", Cardinality::Optional),
            ("[]", Cardinality::List),
        ] {
            let source = format!(
                "model Todo {{ id String title String @@id(id) }} mutation Do(todo Todo.{operation}{suffix})"
            );
            let valid = validate(&parse(&source).unwrap()).unwrap();
            let action = &valid.actions[0];
            let ActionInput::Model { slot } = &action.inputs[0] else {
                panic!("expected Model operand")
            };
            assert_eq!(slot.cardinality, expected, "{source}");
            assert!(action.outputs.is_empty(), "{source}");
        }
    }
}

#[test]
fn action_sequence_can_match_all_prior_instances_or_a_list_target() {
    let source = "model Todo { id String @@id(id) } @sequence(after: [Prior()]) mutation Any(todo Todo.update) @sequence(after: [Prior(todos: todo)]) mutation One(todo Todo.update) mutation Prior(todos Todo.create[])";
    let valid = validate(&parse(source).unwrap()).unwrap();
    assert!(
        valid.actions[0].sequence.as_ref().unwrap().after[0]
            .bindings
            .is_empty()
    );
    assert_eq!(
        valid.actions[1].sequence.as_ref().unwrap().after[0].bindings[0].path,
        ["todo"]
    );
}

#[test]
fn action_sequence_unknown_target_names_the_target() {
    let source = "model Todo { id String @@id(id) }\n@sequence(after: [Missing()]) mutation Later(todo Todo.create)";
    let err = validate(&parse(source).unwrap()).unwrap_err();
    assert!(err.starts_with("2:"), "{err}");
    assert!(err.contains("Missing"), "{err}");
}

#[test]
fn action_semantic_errors_name_the_member_and_location() {
    for (source, name) in [
        ("mutation Search(query Object)", "Object"),
        (
            "model Todo { id String @@id(id) } mutation A(todo Todo.create) { todo Todo todo Todo }",
            "todo",
        ),
        ("mutation A(x String, x Int)", "x"),
        ("mutation Call(x String)", "Call"),
        (
            "model Todo { id String @@id(id) } mutation Todo(x String)",
            "Todo",
        ),
        (
            "model Todo { id String @@id(id) } mutation Save(x String) query save(y String)",
            "save",
        ),
        (
            "model Todo { id String @@id(id) } mutation A(todo Todo.update<missing>)",
            "missing",
        ),
    ] {
        let err = validate(&parse(source).unwrap()).unwrap_err();
        assert!(err.contains("1:") && err.contains(name), "{source}: {err}");
    }
}

#[test]
fn action_preserves_restricted_patch_bindings_and_sequence() {
    let schema = parse(r#"
prerequisite Uploaded(key String)
model Parent { id String children Child[] @@id(id) }
model Child { id String parentId String title String @requires(Uploaded(key: self)) parent Parent @reference(via: [parentId]) @@id(id) }
@sequence(after: [Rename(child: child)])
mutation Add(parent Parent.create, child Child.create(parent: parent))
mutation Rename(child Child.update<title>)
"#).unwrap();
    let valid = validate(&schema).unwrap();
    let ActionInput::Model { slot: child } = &valid.actions[0].inputs[1] else {
        panic!("model input")
    };
    assert_eq!(child.bindings[0].slot, "parent");
    assert_eq!(
        valid.actions[0].sequence.as_ref().unwrap().after[0].bindings[0].path,
        ["child"]
    );
    let ActionInput::Model { slot: patch } = &valid.actions[1].inputs[0] else {
        panic!("model input")
    };
    assert_eq!(patch.allowed_patch_fields.as_ref().unwrap(), &["title"]);
    assert_eq!(valid.requirements[0].prerequisite, "Uploaded");
}

#[test]
fn action_value_input_can_share_a_name_with_an_explicit_output() {
    let schema =
        parse("model Todo { id String @@id(id) } mutation Echo(value String) { value String }")
            .unwrap();
    let action = &validate(&schema).unwrap().actions[0];
    assert_eq!(action.inputs.len(), 1);
    assert_eq!(action.outputs.len(), 1);
}

/// #140: a Model operand declares a mutation target, never a result. Outputs
/// are explicit, handler-sourced and independent of inputs with the same name.
const EXPLICIT_RESULTS: &str = "model Todo { id String title String @@id(id) }\nmutation Edit(todo Todo.update)\nmutation EditAndRead(todo Todo.update) {\n  todo Todo\n}";

#[test]
fn model_operands_imply_no_result_and_outputs_share_input_names() {
    let valid = validate(&parse(EXPLICIT_RESULTS).unwrap()).unwrap();
    let [edit, read] = &valid.actions[..] else {
        panic!("two operations")
    };
    assert!(edit.outputs.is_empty(), "{:?}", edit.outputs);
    assert_eq!(edit.inputs, read.inputs);
    let ActionInput::Model { slot } = &read.inputs[0] else {
        panic!("Model operand")
    };
    assert_eq!((slot.name.as_str(), slot.model.as_str()), ("todo", "Todo"));
    assert_eq!(read.outputs.len(), 1);
    let output = &read.outputs[0];
    assert_eq!(output.name, "todo");
    assert_eq!(output.ty, ActionOutputType::Model("Todo".into()));
    assert_eq!(output.cardinality, Cardinality::Single);
    assert_eq!(output.source, ActionOutputSource::HandlerModelIdentity);
    assert_eq!(output.model_read_version, Some(1));

    let descriptor = compile(EXPLICIT_RESULTS).unwrap();
    let actions = descriptor["actions"].as_array().unwrap();
    assert_eq!(actions[0]["outputs"], serde_json::json!([]));
    assert_eq!(actions[0]["inputs"], actions[1]["inputs"]);
    assert_eq!(
        actions[0]["inputs"][0],
        serde_json::json!({"kind":"model","name":"todo","model":"Todo","operation":"update","cardinality":"single","allowedPatchFields":["title"]})
    );
    assert_eq!(
        actions[1]["outputs"],
        serde_json::json!([{
            "name":"todo","kind":"model","cardinality":"single","source":"handlerIdentity",
            "model":"Todo","modelReadVersion":1,
            "handlerType":{"kind":"identity","model":"Todo","fields":[{"name":"id","type":{"kind":"scalar","name":"string"}}]}
        }])
    );
    assert!(!descriptor.to_string().contains("inputIdentity"));
}

#[test]
fn every_operand_shape_adds_no_result_and_explicit_outputs_keep_their_own() {
    for operation in ["create", "update", "delete"] {
        for suffix in ["", "?", "[]"] {
            let source = format!(
                "model Todo {{ id String title String @@id(id) }} mutation Do(todo Todo.{operation}{suffix}) mutation DoAndRead(todo Todo.{operation}{suffix}) {{ todo Todo? others Todo[] }}"
            );
            let valid = validate(&parse(&source).unwrap()).unwrap();
            assert!(valid.actions[0].outputs.is_empty(), "{source}");
            let outputs = &valid.actions[1].outputs;
            assert_eq!(
                outputs
                    .iter()
                    .map(|x| (x.name.as_str(), x.cardinality))
                    .collect::<Vec<_>>(),
                [
                    ("todo", Cardinality::Optional),
                    ("others", Cardinality::List)
                ],
                "{source}"
            );
            assert!(
                outputs
                    .iter()
                    .all(|x| x.ty == ActionOutputType::Model("Todo".into())
                        && x.source == ActionOutputSource::HandlerModelIdentity),
                "{source}"
            );
            let descriptor = compile(&source).unwrap();
            assert!(
                !descriptor.to_string().contains("inputIdentity"),
                "{source}"
            );
            assert!(
                !descriptor.to_string().contains("deleteIdentity"),
                "{source}"
            );
        }
    }
}

#[test]
fn deleted_identities_are_returned_through_explicit_scalar_outputs() {
    let source = "model Project { tenantId String id String @@id(tenantId, id) } mutation Drop(project Project.delete) { tenantId String id String }";
    let valid = validate(&parse(source).unwrap()).unwrap();
    let outputs = &valid.actions[0].outputs;
    assert_eq!(
        outputs.iter().map(|x| x.name.as_str()).collect::<Vec<_>>(),
        ["tenantId", "id"]
    );
    assert!(outputs.iter().all(|x| x.ty
        == ActionOutputType::Value(FieldType::Scalar(Scalar::String))
        && x.source == ActionOutputSource::HandlerValue
        && x.model_read_version.is_none()));
    // A same-name scalar output is the handler's value, not the operand.
    let same = validate(
        &parse(
            "model Todo { id String @@id(id) } mutation Remove(todo Todo.delete) { todo String }",
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        same.actions[0].outputs[0].source,
        ActionOutputSource::HandlerValue
    );
}

#[test]
fn duplicate_outputs_and_duplicate_inputs_fail_independently() {
    // Each refusal names the second declaration within its own namespace.
    for (source, expected) in [
        (
            "model Todo { id String @@id(id) }\nmutation A(todo Todo.update) { todo Todo todo Todo }",
            "2:42: duplicate Mutation output todo",
        ),
        (
            "model Todo { id String @@id(id) }\nmutation A() { todo Todo todo Todo? }",
            "2:26: duplicate Mutation output todo",
        ),
        (
            "model Todo { id String @@id(id) }\nquery A(text String) { text String text Int }",
            "2:36: duplicate Query output text",
        ),
        (
            "model Todo { id String @@id(id) }\nmutation A(todo Todo.update, todo Todo.delete) { todo Todo }",
            "2:30: duplicate Mutation input todo",
        ),
        (
            "model Todo { id String @@id(id) }\nmutation A(todo Todo.update, todo String)",
            "2:30: duplicate Mutation input todo",
        ),
    ] {
        let err = validate(&parse(source).unwrap()).unwrap_err();
        assert_eq!(err, expected, "{source}");
    }
}

#[test]
fn explicit_results_generate_separate_client_and_handler_types() {
    let descriptor = compile(EXPLICIT_RESULTS).unwrap();
    let ts = axton_compiler::typescript(&descriptor);
    let backend = axton_compiler::backend_typescript(&descriptor, "@axton/server");
    let dart = axton_compiler::dart(&descriptor);
    for expected in [
        "export type EditOutput = void;",
        "function decodeEditOutput(_value:unknown):void { return undefined; }",
        "export interface EditAndReadOutput {\n todo: Todo;\n}",
        "export type EditAndReadOptions = CallOptions<'todo'>;",
    ] {
        assert!(ts.contains(expected), "missing {expected}: {ts}");
    }
    for expected in [
        "export interface EditInput {\n todo: TodoUpdate",
        "export type EditHandlerOutput = void;",
        "export interface EditAndReadInput {\n todo: TodoUpdate",
        "export interface EditAndReadHandlerOutput {\n todo: TodoIdentity;\n}",
    ] {
        assert!(backend.contains(expected), "missing {expected}: {backend}");
    }
    for expected in [
        "typedef EditOutput = void;",
        "typedef EditHandlerOutput = void;",
        "class EditAndReadOutput implements _DartActionRecord {\n final Todo todo;",
        "class EditAndReadHandlerOutput implements _DartActionRecord {\n final TodoIdentity todo;",
    ] {
        assert!(dart.contains(expected), "missing {expected}: {dart}");
    }
    assert!(!ts.contains("export interface EditOutput"), "{ts}");
    assert!(!dart.contains("class EditOutput"), "{dart}");
}

#[test]
fn schema_and_mutations() {
    let v=compile("enum Status { active archived } model Entry { owner UUID id UUID title String note String? labels String[] at DateTime status Status @@id(owner,id) @@unique(title) } mutation Edit { entry Entry.update<title,note> @@version(2) }").unwrap();
    assert_eq!(
        v["schema"]["models"][0]["identity"],
        serde_json::json!(["owner", "id"])
    );
    assert_eq!(v["mutations"][0]["version"], 2);
    assert_eq!(
        v["mutations"][0]["slots"][0]["allowedPatchFields"],
        serde_json::json!(["title", "note"])
    );
}
#[test]
fn rejects_unknown_with_location() {
    let e = compile("model A { id UUID @@id(id) }\nunknown Stuff {}").unwrap_err();
    assert!(e.contains("2:"), "{e}");
}
#[test]
fn rejects_invalid_identity() {
    assert!(compile("model A { id UUID? @@id(id) }").is_err());
}
#[test]
fn emitters_include_typed_conversion() {
    let v = compile(include_str!("../../../fixtures/compiler/example.model")).unwrap();
    let ts = axton_compiler::typescript(&v);
    assert!(ts.contains("export interface EntryIdentity"));
    assert!(ts.contains("new Date("));
    assert!(ts.contains("EditEntry"));
    let dart = axton_compiler::dart(&v);
    assert!(dart.contains("class EntryPatch"));
    assert!(dart.contains("DateTime.parse("));
}
#[test]
fn relationships_bindings_and_dependency_metadata() {
    let v=compile(r#"prerequisite Uploaded(key String)
 model Parent { id UUID children Child[] @@id(id) }
 model Child { id UUID parentId UUID label String @requires(Uploaded(key: self)) parent Parent @reference(via: [parentId], onTargetDelete: delete) @@id(id) }
 mutation Add { parent Parent.create children Child.create(parent: parent)[] @@sequence(after: [Rename(parent: parent)]) }
 mutation Rename { parent Parent.update<> }
 "#).unwrap();
    assert_eq!(
        v["schema"]["models"][1]["relations"][0]["fields"],
        serde_json::json!(["parentId"])
    );
    assert_eq!(
        v["mutations"][0]["slots"][1]["bindings"][0]["slot"],
        "parent"
    );
    assert_eq!(v["prerequisites"][0]["name"], "Uploaded");
    // `Parent.update<>` is valid: an empty patch is a no-op update ([#49](https://github.com/zanminwang/axton/issues/49)).
    assert_eq!(v["mutations"][1]["name"], "Rename");
    assert_eq!(
        v["mutations"][1]["slots"][0]["allowedPatchFields"],
        serde_json::json!([])
    );
}
#[test]
fn rejects_dependency_typos() {
    assert!(compile("prerequisite Exists(key String) model A { id UUID label String @requires(Missing(key: self)) @@id(id) }").is_err());
    assert!(
        compile(
            "model A { id UUID @@id(id) } mutation Add { a A.create @@version(2) @@version(3) }"
        )
        .is_err()
    );
}
#[test]
fn singular_inverse_requires_a_unique_foreign_key() {
    assert!(compile("model Parent { id String child Child? @@id(id) } model Child { id String parentId String parent Parent @reference(via:[parentId]) @@id(id) }").is_err());
    assert!(compile("model Parent { id String child Child? @@id(id) } model Child { id String parentId String parent Parent @reference(via:[parentId]) @@id(id) @@unique(parentId) }").is_ok());
}
#[test]
fn backend_emitter_declares_handlers_loaders_and_references() {
    let v = compile(include_str!("../../../fixtures/compiler/relations.model")).unwrap();
    let ts = axton_compiler::backend_typescript(&v, "@axton/server");
    assert!(ts.contains("from \"@axton/server\""));
    assert!(ts.contains("export interface Handlers<Tx> {"));
    assert!(ts.contains(
        " addBook: { v1(call: HandlerCall<Tx, AddBookInput>): Promise<void> } | ((call: HandlerCall<Tx, AddBookInput>) => Promise<void>);"
    ));
    assert!(ts.contains(
        " addComment: { v1(call: HandlerCall<Tx, AddCommentInput>): Promise<void> } | ((call: HandlerCall<Tx, AddCommentInput>) => Promise<void>);"
    ));
    assert!(ts.contains("export interface Loaders<Tx> {"));
    assert!(
        ts.contains(
            " book: { v1(call: LoaderCall<Tx, BookIdentity>): Promise<readonly (Book | null)[]> } | ((call: LoaderCall<Tx, BookIdentity>) => Promise<readonly (Book | null)[]>);"
        )
    );
    assert!(ts.contains("export function Book(identity: BookIdentity): Extract<RecordRef, { model: \"Book\" }> { return { model: \"Book\", identity }; }"));
    assert!(ts.contains("export interface AddBookInput {\n book: Book;\n}"));
    assert!(ts.contains("export function createBackend<Tx>("));
    assert!(!axton_compiler::typescript(&v).contains("backendConfig"));
}
#[test]
fn backend_emitter_generates_channel_touch_and_contexts_per_schema() {
    let v = compile("model Todo { id String @@id(id) }\nmodel Pin { todo String at DateTime @@id(todo, at) }\nmutation Edit(todo Todo.update)\nquery Look(id String) { todo Todo? }\nmutation Legacy { todo Todo.delete }").unwrap();
    let ts = axton_compiler::backend_typescript(&v, "@axton/server");
    // A discriminated reference per Model, and constructors narrowed to their own variant.
    assert!(ts.contains("export type RecordRef = { readonly model: \"Todo\"; readonly identity: TodoIdentity } | { readonly model: \"Pin\"; readonly identity: PinIdentity };\n"), "{ts}");
    assert!(ts.contains("export function Todo(identity: TodoIdentity): Extract<RecordRef, { model: \"Todo\" }> { return { model: \"Todo\", identity }; }"), "{ts}");
    assert!(ts.contains("export function Pin(identity: PinIdentity): Extract<RecordRef, { model: \"Pin\" }> { return { model: \"Pin\", identity }; }"), "{ts}");
    assert!(ts.contains("export interface ModelMembership<Identity> {\n add(identity: Identity): void;\n remove(identity: Identity): void;\n}\n"), "{ts}");
    assert!(ts.contains("export interface Channel {\n todo: ModelMembership<TodoIdentity>;\n pin: ModelMembership<PinIdentity>;\n add(records: readonly RecordRef[]): void;\n remove(records: readonly RecordRef[]): void;\n}\n"), "{ts}");
    assert!(ts.contains("export interface Touch {\n todo(identity: TodoIdentity): void;\n pin(identity: PinIdentity): void;\n}\n"), "{ts}");
    // Concrete contexts: a Mutation, a legacy handler and an external
    // transaction declare through the generated handles; a Query cannot.
    assert!(ts.contains("export interface MutationContext<Tx> {\n tx: Tx;\n userId: string;\n callId: string;\n channel(name: string): Channel;\n touch: Touch;\n}\n"), "{ts}");
    assert!(
        ts.contains(
            "export interface QueryContext<Tx> {\n tx: Tx;\n userId: string;\n callId: string;\n}\n"
        ),
        "{ts}"
    );
    assert!(ts.contains("export interface HandlerCall<Tx, Input> {\n input: Input;\n tx: Tx;\n userId: string;\n channel(name: string): Channel;\n touch: Touch;\n}\n"), "{ts}");
    assert!(ts.contains("export interface TransactionCall<Tx> {\n tx: Tx;\n channel(name: string): Channel;\n touch: Touch;\n}\n"), "{ts}");
    // `backend.transaction` hands its body the same generated handles.
    assert!(
        ts.contains(" return createRuntimeBackend<Tx, TransactionCall<Tx>>({ ...options,"),
        "{ts}"
    );
    // The broad runtime contexts and the retired helpers are not re-exported.
    assert!(ts.contains("export { CallRejected, MutationRejected, devAuth, type LoaderCall } from \"@axton/server\";\n"), "{ts}");
    for retired in [
        "type MutationContext,",
        "type HandlerCall,",
        "type RecordRef }",
        "Publish",
        "publish",
        "changes",
        "encodeTodoIdentity",
    ] {
        assert!(!ts.contains(retired), "{retired}: {ts}");
    }
    // Without Models, a Channel has only its mixed verbs and nothing to name.
    let empty =
        axton_compiler::backend_typescript(&compile("mutation Ping()").unwrap(), "@axton/server");
    assert!(
        empty.contains("export type RecordRef = never;\n"),
        "{empty}"
    );
    assert!(empty.contains("export interface Channel {\n add(records: readonly RecordRef[]): void;\n remove(records: readonly RecordRef[]): void;\n}\n"), "{empty}");
    assert!(empty.contains("export interface Touch {\n}\n"), "{empty}");
}

#[test]
fn backend_emitter_groups_handler_versions_under_the_mutation_name() {
    let v = compile("model A { id String title String @@id(id) } mutation Edit { a A.update<title> @@version(2) }").unwrap();
    let mut with_history = v.clone();
    let mut old = v["mutations"][0].clone();
    old["version"] = serde_json::json!(1);
    with_history["backendMutations"] = serde_json::json!([old, v["mutations"][0].clone()]);
    let ts = axton_compiler::backend_typescript(&with_history, "@axton/server");
    assert!(
        ts.contains(
            " edit: { v1(call: HandlerCall<Tx, EditV1Input>): Promise<void>; v2(call: HandlerCall<Tx, EditInput>): Promise<void> };\n"
        ),
        "{ts}"
    );
    assert!(!ts.contains(" editV1(call:"));
}

#[test]
fn backend_emitter_accepts_a_bare_function_only_for_a_v1_only_mutation() {
    let v = compile("model A { id String title String @@id(id) } mutation Save { a A.create }")
        .unwrap();
    let ts = axton_compiler::backend_typescript(&v, "@axton/server");
    assert!(
        ts.contains(
            " save: { v1(call: HandlerCall<Tx, SaveInput>): Promise<void> } | ((call: HandlerCall<Tx, SaveInput>) => Promise<void>);\n"
        ),
        "{ts}"
    );
    let later = compile(
        "model A { id String title String @@id(id) } mutation Save { a A.create @@version(2) }",
    )
    .unwrap();
    let ts = axton_compiler::backend_typescript(&later, "@axton/server");
    assert!(
        ts.contains(" save: { v2(call: HandlerCall<Tx, SaveInput>): Promise<void> };\n"),
        "{ts}"
    );
    assert!(
        !ts.contains("| ((call: HandlerCall"),
        "a single non-v1 version has no shorthand: {ts}"
    );
}

#[test]
fn generated_clients_expose_one_server_connection() {
    let schema = compile("model Entry { id String title String @@id(id) } mutation Edit { entry Entry.update<title> }").unwrap();
    let ts = axton_compiler::client_typescript(&schema, "@example/custom-runtime");
    assert!(ts.contains("type ServerOptions"));
    assert!(ts.contains("server?: ServerOptions"));
    assert!(ts.contains("client.connect(options.server"));
    assert!(!ts.contains("LiveTransport"));
    assert!(!ts.contains("transport?:"));
    assert!(
        ts.find("connection options were removed").unwrap() < ts.find("await Client.open").unwrap()
    );
    let schema = compile("model Entry { id String title String @@id(id) } mutation Edit { entry Entry.update<title> }").unwrap();
    let dart = axton_compiler::dart(&schema);
    assert!(dart.contains("show RuntimeConnection, SyncServer"));
    assert!(dart.contains("SyncServer? server"));
    assert!(dart.contains("client.connect(server"));
    assert!(!dart.contains("LiveTransport"));
    assert!(!dart.contains("Transport? transport"));
}

/// The generated client is the whole client: typed models (with a per-record
/// sync state), top-level mutations and the runtime members, in both languages.
#[test]
fn generated_clients_are_the_whole_client() {
    let schema = compile("model Entry { id String title String @@id(id) } mutation Edit { entry Entry.update<title> }").unwrap();
    let ts = axton_compiler::client_typescript(&schema, "@example/custom-runtime");
    for member in [
        "readonly mutate: Mutate;",
        "this.mutate = new Mutate(client)",
        "syncState(): Promise<ClientSyncState>",
        "dismissRejection(ordinal: number)",
        "drop(ordinal: number)",
        "pendingTasks()",
        "setReadiness(key: string",
        "runPrerequisites(",
        "async connect(server: ServerOptions",
        "querySpec(model: string",
        "readSql(sql: string",
    ] {
        assert!(ts.contains(member), "missing {member}: {ts}");
    }
    assert!(!ts.contains("status()"), "{ts}");
    let schema = compile(
        "model Entry { id String title String @@id(id) } mutation Edit { entry Entry.update<title> } mutation Touch { entry Entry.update<title> }",
    )
    .unwrap();
    let model = axton_compiler::typescript(&schema);
    assert!(
        model.contains("export type MutationName = 'Edit'|'Touch';"),
        "{model}"
    );
    assert!(
        model.contains("async syncState(identity:EntryIdentity):Promise<SyncState>"),
        "{model}"
    );
    assert!(
        model.contains("export class Mutate { readonly port:MutatePort;"),
        "{model}"
    );
    assert!(
        model.contains(
            "interface WritePort extends ReadPort { direct(operation:object):Promise<void>; }"
        ),
        "transactions expose direct writes without mutation submission: {model}"
    );
    let write_port = model
        .lines()
        .find(|line| line.starts_with("export interface WritePort "))
        .unwrap();
    assert!(!write_port.contains("mutate"), "{write_port}");
    let dart = axton_compiler::dart(&schema);
    for member in [
        "late final Mutate mutate = Mutate(client);",
        "Future<Map<String,dynamic>> syncState() => client.syncState();",
        "Future<void> dismissRejection(int ordinal)",
        "Future<void> drop(int ordinal)",
        "Future<RuntimeConnection> connect(SyncServer server",
        "Future<SyncState> syncState(EntryIdentity identity)",
        "class Mutate { final MutatePort port;",
    ] {
        assert!(dart.contains(member), "missing {member}: {dart}");
    }
}

#[test]
fn generated_transaction_facades_are_local_only() {
    let schema = compile("model Entry { id String title String @@id(id) } mutation Edit { entry Entry.update<title> }").unwrap();
    let ts = axton_compiler::typescript(&schema);
    let ts_transaction = ts
        .lines()
        .find(|line| line.starts_with("export class GeneratedTransaction "))
        .unwrap();
    assert!(
        ts_transaction.contains("readonly models:TxModels;"),
        "{ts_transaction}"
    );
    assert!(!ts_transaction.contains("mutate"), "{ts_transaction}");
    assert!(
        ts.contains("export class Mutate { readonly port:MutatePort;"),
        "{ts}"
    );
    let ts_client = axton_compiler::client_typescript(&schema, "@example/custom-runtime");
    assert!(
        ts_client.contains("this.mutate = new Mutate(client)"),
        "{ts_client}"
    );

    let dart = axton_compiler::dart(&schema);
    let dart_transaction = dart
        .lines()
        .find(|line| line.starts_with("class GeneratedTransaction "))
        .unwrap();
    assert!(
        dart_transaction.contains("late final TxModels models"),
        "{dart_transaction}"
    );
    assert!(!dart_transaction.contains("mutate"), "{dart_transaction}");
    assert!(
        dart.contains("class Mutate { final MutatePort port;"),
        "{dart}"
    );
    assert!(
        dart.contains("late final Mutate mutate = Mutate(client);"),
        "{dart}"
    );
}

fn line_of(error: &str) -> usize {
    error
        .split(':')
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("no line in {error}"))
}

#[test]
fn semantic_errors_report_the_offending_declaration() {
    // Every case pads the input so end-of-file is far below the declaration.
    let pad = "\n\n\n\n// trailing comment\n";
    let cases: &[(&str, usize, &str)] = &[
        (
            "model Parent { id UUID @@id(id) }\nmodel Child {\n id UUID\n parent Parent @reference(via: [missing])\n @@id(id)\n}",
            4,
            "unknown reference field",
        ),
        (
            "model Parent { id UUID @@id(id) }\nmodel Child {\n id UUID\n parentId UUID\n parent Parent @reference(via: [parentId])\n twin Parent @reference(via: [parentId], onTargetDelete: cascade)\n @@id(id)\n}",
            6,
            "unsupported onTargetDelete",
        ),
        (
            "model Parent { id String child Child? @@id(id) }\nmodel Child {\n id String\n parentId String\n parent Parent @reference(via:[parentId])\n @@id(id)\n}",
            1,
            "singular inverse requires unique reference fields",
        ),
        (
            "model A {\n id UUID\n\n label Missing\n @@id(id)\n}",
            4,
            "unknown or unsupported field type",
        ),
        (
            "model Parent { id UUID @@id(id) }\nmodel Child { id UUID parentId UUID parent Parent @reference(via: [parentId]) @@id(id) }\nmutation Add {\n parent Parent.create\n child Child.create(parent: nobody)\n}",
            5,
            "unknown parent slot",
        ),
        (
            "model A { id UUID @@id(id) }\nmutation Add {\n a A.create\n @@sequence(after: [Missing(a: a)])\n}",
            4,
            "unknown sequence mutation",
        ),
        (
            "prerequisite Exists(key String)\nmodel A {\n id UUID\n label String @requires(Missing(key: self))\n @@id(id)\n}",
            4,
            "unknown prerequisite",
        ),
        (
            "model A { id UUID @@id(id) }\nmutation Add { a A.create }\n\nmutation Add { a A.create }",
            4,
            "duplicate mutation",
        ),
        (
            "model A { id UUID @@id(id) }\nmutation Edit {\n a A.update<nope>\n}",
            3,
            "invalid allowed patch field",
        ),
        (
            "model A {\n id UUID\n @@id(id)\n @@unique(missing)\n}",
            4,
            "invalid unique fields",
        ),
        (
            "model A { id UUID @@id(id) }\n\nmodel A { id UUID @@id(id) }",
            3,
            "duplicate",
        ),
        (
            "model A { id UUID @@id(id) }\n\nmodel B {\n id UUID\n}",
            3,
            "identity",
        ),
        (
            "model A { id UUID @@id(id) }\nmutation Remove {\n entries A.delete[]\n maybe A.delete?\n}",
            4,
            "ambiguous slot",
        ),
    ];
    for (source, line, message) in cases {
        let e = compile(&format!("{source}{pad}")).unwrap_err();
        assert!(e.contains(message), "expected {message:?} in {e:?}");
        assert_eq!(line_of(&e), *line, "{e}");
    }
}

#[test]
fn rejects_model_and_enum_names_the_generated_client_uses() {
    for name in [
        "SyncState",
        "Rejection",
        "Mutate",
        "GeneratedClient",
        "Mutations",
        "Queries",
        "DirectMutations",
        "QueuedQueries",
        "CallPort",
        "CallRejected",
        // The shared call handle vocabulary every generated client re-exports.
        "Call",
        "CallStatus",
        "CallOutcome",
        "CallError",
        "CallOptions",
        "CallStore",
        "CallSuccess",
        "CallFailure",
        "Transaction",
        // The Scope facade and the handle types it re-exports
        // ([#150](https://github.com/zanminwang/axton/issues/150)).
        "Scopes",
        "Subscription",
        "SubscriptionStatus",
        "SubscriptionInitialization",
        "SubscriptionConnection",
        "SubscriptionClosedException",
    ] {
        let e = compile(&format!("model {name} {{ id UUID @@id(id) }}")).unwrap_err();
        assert!(e.contains("generated client"), "{name}: {e}");
        let e = compile(&format!(
            "enum {name} {{ a b }}\nmodel Other {{ id UUID @@id(id) }}"
        ))
        .unwrap_err();
        assert!(e.contains("generated client"), "enum {name}: {e}");
    }
    // The generated backend's handler and publish vocabulary
    // ([#140](https://github.com/zanminwang/axton/issues/140)): the diagnostic
    // names the backend, which is where these types are declared.
    for name in [
        "MutationContext",
        "QueryContext",
        "Channel",
        "Touch",
        "ModelMembership",
        "RecordRef",
        "HandlerCall",
        "TransactionCall",
    ] {
        let e = compile(&format!("model {name} {{ id UUID @@id(id) }}")).unwrap_err();
        assert!(
            e.contains(&format!(
                "{name} is a type name the generated backend declares"
            )),
            "{name}: {e}"
        );
        let e = compile(&format!(
            "enum {name} {{ a b }}\nmodel Other {{ id UUID @@id(id) }}"
        ))
        .unwrap_err();
        assert!(e.contains("generated backend"), "enum {name}: {e}");
    }
    for name in [
        "Status",
        "SyncStates",
        "Rejections",
        // Retired Action vocabulary is no longer generated.
        "Actions",
        "ActionCall",
        "Calls",
        "Order",
        "Scope",
        "Subscriptions",
    ] {
        assert!(
            compile(&format!("model {name} {{ id UUID @@id(id) }}")).is_ok(),
            "{name} should stay valid"
        );
    }
}

#[test]
fn model_accessors_are_unique_and_leave_the_channel_verbs_free() {
    // `ctx.touch.todo` and `ctx.channel(name).todo` use the lower-first
    // accessor, so two Models must not share one.
    let e = compile("model Todo { id String @@id(id) }\n\nmodel todo { id String @@id(id) }\n\n")
        .unwrap_err();
    assert!(
        e.contains("models Todo and todo both generate the accessor todo"),
        "{e}"
    );
    assert_eq!(line_of(&e), 3, "{e}");
    // A Channel keeps `add` and `remove` for mixed record lists.
    for name in ["Add", "add", "Remove", "remove"] {
        let e = compile(&format!(
            "model Other {{ id UUID @@id(id) }}\n\nmodel {name} {{ id UUID @@id(id) }}\n\n"
        ))
        .unwrap_err();
        assert!(
            e.contains(&format!(
                "model {name} generates the accessor {}, which a Channel reserves for mixed record lists",
                name.to_ascii_lowercase()
            )),
            "{name}: {e}"
        );
        assert_eq!(line_of(&e), 3, "{name}: {e}");
    }
    // No other accessor is reserved: dictionary keys are own properties.
    for name in [
        "Adds",
        "Removal",
        "Constructor",
        "__proto__",
        "ToString",
        "Publish",
        // A Channel handle is no function, so function members stay free too.
        "Name",
        "Length",
        "Bind",
        "Apply",
    ] {
        assert!(
            compile(&format!("model {name} {{ id UUID @@id(id) }}")).is_ok(),
            "{name} should stay valid"
        );
    }
}

#[test]
fn rejects_reserved_model_names_at_the_declaration() {
    for name in ["sqlite_entry", "SQLite_Entry", "axton_entry", "AXTON_entry"] {
        let e = compile(&format!(
            "model Other {{ id UUID @@id(id) }}\n\nmodel {name} {{ id UUID @@id(id) }}\n\n\n"
        ))
        .unwrap_err();
        assert!(e.contains("reserved"), "{name}: {e}");
        assert_eq!(line_of(&e), 3, "{name}: {e}");
    }
    for name in ["Sqlite", "sqlitex", "my_sqlite_table", "axtonEntry"] {
        assert!(
            compile(&format!("model {name} {{ id UUID @@id(id) }}")).is_ok(),
            "{name} should stay valid"
        );
    }
}

#[test]
fn structural_refusals_a_schema_author_is_likely_to_hit() {
    // Each case names the rule, the input that breaks it and a valid twin that
    // differs only in that rule, so the refusal is attributable to the rule alone.
    let cases: &[(&str, &str, &str, &str)] = &[
        (
            "duplicate field",
            "model A {\n id UUID\n text String\n text String\n @@id(id)\n}",
            "model A {\n id UUID\n text String\n note String\n @@id(id)\n}",
            "duplicate field",
        ),
        (
            "reference identity arity mismatch",
            "model Parent { id UUID kind String @@id(id, kind) }\nmodel Child {\n id UUID\n parentId UUID\n parent Parent @reference(via: [parentId])\n @@id(id)\n}",
            "model Parent { id UUID kind String @@id(id, kind) }\nmodel Child {\n id UUID\n parentId UUID\n parentKind String\n parent Parent @reference(via: [parentId, parentKind])\n @@id(id)\n}",
            "reference identity arity mismatch",
        ),
        (
            "reference field type mismatch",
            "model Parent { id UUID @@id(id) }\nmodel Child {\n id UUID\n parentId String\n parent Parent @reference(via: [parentId])\n @@id(id)\n}",
            "model Parent { id UUID @@id(id) }\nmodel Child {\n id UUID\n parentId UUID\n parent Parent @reference(via: [parentId])\n @@id(id)\n}",
            "reference field type mismatch",
        ),
        (
            "unknown reference argument",
            "model Parent { id UUID @@id(id) }\nmodel Child {\n id UUID\n parentId UUID\n parent Parent @reference(via: [parentId], cascade: true)\n @@id(id)\n}",
            "model Parent { id UUID @@id(id) }\nmodel Child {\n id UUID\n parentId UUID\n parent Parent @reference(via: [parentId], onTargetDelete: delete)\n @@id(id)\n}",
            "unknown reference argument",
        ),
        (
            "ambiguous inverse",
            "model Parent {\n id UUID\n children Child[]\n @@id(id)\n}\nmodel Child {\n id UUID\n parentId UUID\n otherId UUID\n parent Parent @reference(via: [parentId])\n other Parent @reference(via: [otherId])\n @@id(id)\n}",
            "model Parent {\n id UUID\n children Child[] @inverse(owner)\n @@id(id)\n}\nmodel Child {\n id UUID\n parentId UUID\n otherId UUID\n parent Parent @reference(owner, via: [parentId])\n other Parent @reference(via: [otherId])\n @@id(id)\n}",
            "inverse must resolve to exactly one reference",
        ),
        (
            "binding parent must be a single slot of the referenced model",
            "model Parent { id UUID @@id(id) }\nmodel Child { id UUID parentId UUID parent Parent @reference(via: [parentId]) @@id(id) }\nmutation Add {\n parents Parent.create[]\n child Child.create(parent: parents)\n}",
            "model Parent { id UUID @@id(id) }\nmodel Child { id UUID parentId UUID parent Parent @reference(via: [parentId]) @@id(id) }\nmutation Add {\n parent Parent.create\n child Child.create(parent: parent)\n}",
            "binding parent must be single matching model",
        ),
        (
            "sequence target model mismatch",
            "model A { id UUID @@id(id) }\nmodel B { id UUID @@id(id) }\nmutation First { a A.create }\nmutation Second {\n b B.create\n @@sequence(after: [First(a: b)])\n}",
            "model A { id UUID text String @@id(id) }\nmutation First { a A.create }\nmutation Second {\n a A.update<text>\n @@sequence(after: [First(a: a)])\n}",
            "sequence target model mismatch",
        ),
        (
            "list slot followed by an optional slot of the same model and operation",
            "model A { id UUID text String @@id(id) }\nmutation Remove {\n entries A.delete[]\n maybe A.delete?\n}",
            "model A { id UUID text String @@id(id) }\nmutation Remove {\n entries A.delete[]\n maybe A.update?\n}",
            "ambiguous slot",
        ),
        (
            "optional slot followed by a single slot of the same model and operation",
            "model A { id UUID text String @@id(id) }\nmutation Remove {\n first A.delete?\n second A.delete\n}",
            "model A { id UUID text String @@id(id) }\nmutation Remove {\n first A.delete\n second A.delete?\n}",
            "ambiguous slot",
        ),
        (
            "same model and operation separated only by an optional slot",
            "model A { id UUID text String @@id(id) }\nmodel B { id UUID @@id(id) }\nmutation Remove {\n first A.delete[]\n other B.create?\n second A.delete?\n}",
            "model A { id UUID text String @@id(id) }\nmodel B { id UUID @@id(id) }\nmutation Remove {\n first A.delete[]\n other B.create\n second A.delete?\n}",
            "ambiguous slot",
        ),
    ];
    for (rule, invalid, valid, message) in cases {
        let e = compile(invalid).unwrap_err();
        assert!(e.contains(message), "{rule}: expected {message:?} in {e:?}");
        assert!(
            compile(valid).is_ok(),
            "{rule}: the valid twin was refused: {:?}",
            compile(valid).err()
        );
    }
}

#[test]
fn model_versions_reach_every_generated_surface() {
    // The client's declared read contract is the `version` of each model in the
    // embedded schema; the backend additionally carries every retained contract.
    let v = compile("enum Status { open closed } model Task { id UUID status Status @@id(id) @@version(2) } model Note { id UUID @@id(id) }").unwrap();
    assert_eq!(v["schema"]["models"][0]["version"], 2);
    assert_eq!(
        v["schema"]["models"][1]["version"], 1,
        "omitted is version 1"
    );
    let ts = axton_compiler::typescript(&v);
    assert!(
        ts.contains(r#""name":"Task","relations":[],"unique":[],"version":2}"#),
        "{ts}"
    );
    assert!(
        ts.contains(r#""name":"Note","relations":[],"unique":[],"version":1}"#),
        "{ts}"
    );
    let dart = axton_compiler::dart(&v);
    assert!(
        dart.contains(r#""name":"Task","relations":[],"unique":[],"version":2}"#),
        "{dart}"
    );
    let mut with_history = v.clone();
    let old = serde_json::json!({"name":"Task","version":1,"identity":["id"],"fields":[{"name":"id","nullable":false,"type":{"kind":"scalar","name":"uuid"}}],"enums":[]});
    with_history["backendModels"] = serde_json::json!([old]);
    let backend = axton_compiler::backend_typescript(&with_history, "@axton/server");
    assert!(backend.contains(r#""models":[{"enums":[],"fields":[{"name":"id","nullable":false,"type":{"kind":"scalar","name":"uuid"}}],"identity":["id"],"name":"Task","version":1}]"#), "{backend}");
    assert!(!backend.contains("backendModels"), "{backend}");
}

#[test]
fn backend_emitter_groups_loader_versions_under_the_model_name() {
    let v = compile("enum Status { open closed archived } model Task { id UUID title String status Status @@id(id) @@version(2) } model Note { id UUID text String @@id(id) }").unwrap();
    let mut with_history = v.clone();
    // The retained v1 contract: no `title`, and `Status` as it was published.
    let old = serde_json::json!({"name":"Task","version":1,"identity":["id"],"fields":[{"name":"id","nullable":false,"type":{"kind":"scalar","name":"uuid"}},{"name":"status","nullable":false,"type":{"kind":"enum","name":"Status"}}],"enums":[{"name":"Status","values":["open","closed"]}]});
    let mut current = v["schema"]["models"][0].clone();
    current["enums"] = v["schema"]["enums"].clone();
    let note = serde_json::json!({"name":"Note","version":1,"identity":["id"],"fields":v["schema"]["models"][1]["fields"],"enums":[]});
    with_history["backendModels"] = serde_json::json!([note, old, current]);
    let ts = axton_compiler::backend_typescript(&with_history, "@axton/server");
    // An older contract is its own record type, with the enum values of its time inline.
    assert!(
        ts.contains(
            "export interface TaskV1 {\n id: string;\n status: \"open\" | \"closed\";\n}\n"
        ),
        "{ts}"
    );
    assert!(ts.contains("export type Task = TaskRecord;"), "{ts}");
    assert!(
        !ts.contains("TaskV2"),
        "the latest version keeps the plain name: {ts}"
    );
    assert!(
        ts.contains(" task: { v1(call: LoaderCall<Tx, TaskV1Identity>): Promise<readonly (TaskV1 | null)[]>; v2(call: LoaderCall<Tx, TaskIdentity>): Promise<readonly (Task | null)[]> };\n"),
        "{ts}"
    );
    assert!(
        ts.contains(" note: { v1(call: LoaderCall<Tx, NoteIdentity>): Promise<readonly (Note | null)[]> } | ((call: LoaderCall<Tx, NoteIdentity>) => Promise<readonly (Note | null)[]>);\n"),
        "{ts}"
    );
    // Without a history the schema's own version is the only retained one; a
    // single non-v1 version has no shorthand.
    let ts = axton_compiler::backend_typescript(&v, "@axton/server");
    assert!(
        ts.contains(" task: { v2(call: LoaderCall<Tx, TaskIdentity>): Promise<readonly (Task | null)[]> };\n"),
        "{ts}"
    );
    assert!(!ts.contains("TaskV1"), "{ts}");
}

#[test]
fn deprecations_reach_every_generated_surface_and_leave_the_descriptors_alone() {
    let v = compile("enum Status { active archived @deprecated(reason: \"use closed\") closed }\nmodel Task { id UUID name String @deprecated(reason: \"renamed to title\") title String legacy Int? @deprecated status Status @@id(id) }\nmutation Edit { task Task.update<title> old Task.update<name>? @deprecated(reason: \"use task\") }").unwrap();
    // The runtime descriptors do not carry deprecation: it is a generated-code notice only.
    assert!(
        !v["schema"].to_string().contains("deprecat"),
        "{}",
        v["schema"]
    );
    assert!(!v["mutations"].to_string().contains("deprecat"));
    assert_eq!(
        v["deprecations"],
        serde_json::json!([
            {"kind":"enumValue","enum":"Status","value":"archived","reason":"use closed"},
            {"kind":"field","model":"Task","field":"name","reason":"renamed to title"},
            {"kind":"field","model":"Task","field":"legacy","reason":null},
            {"kind":"slot","mutation":"Edit","slot":"old","reason":"use task"}
        ])
    );
    let ts = axton_compiler::typescript(&v);
    assert!(ts.contains("export interface Task {\n id: string;\n /** @deprecated renamed to title */\n name: string;\n title: string;\n /** @deprecated */\n legacy: number | null;\n"), "{ts}");
    assert!(
        ts.contains(
            "export interface TaskPatch {\n /** @deprecated renamed to title */\n name?: string;\n"
        ),
        "{ts}"
    );
    assert!(ts.contains("/** @deprecated \"archived\": use closed */\nexport type Status = \"active\" | \"archived\" | \"closed\";"), "{ts}");
    assert!(ts.contains("export interface EditArgs {\n task: { identity:TaskIdentity; values:Pick<TaskPatch, \"title\"> };\n /** @deprecated use task */\n old?: { identity:TaskIdentity; values:Pick<TaskPatch, \"name\"> };\n}"), "{ts}");
    let backend = axton_compiler::backend_typescript(&v, "@axton/server");
    assert!(backend.contains("export interface EditInput {\n task: { identity: TaskIdentity; patch: Pick<TaskPatch, \"title\"> };\n /** @deprecated use task */\n old: { identity: TaskIdentity; patch: Pick<TaskPatch, \"name\"> } | null;\n}"), "{backend}");
    let dart = axton_compiler::dart(&v);
    assert!(
        dart.contains("enum Status { active, @Deprecated('use closed') archived, closed }"),
        "{dart}"
    );
    assert!(dart.contains("class Task implements TaskCreateInput {\n final String id;\n @Deprecated('renamed to title')\n final String name;\n final String title;\n @Deprecated('')\n final int? legacy;\n"), "{dart}");
    assert!(
        dart.contains(
            "class TaskPatch {\n @Deprecated('renamed to title')\n final Present<String>? name;\n"
        ),
        "{dart}"
    );
    assert!(dart.contains("class TaskFilter {\n final Present<String>? id;\n @Deprecated('renamed to title')\n final Present<String>? name;\n"), "{dart}");
    assert!(dart.contains("Map<String,dynamic> edit({required EditTaskUpdate task,@Deprecated('use task') EditOldUpdate? old})"), "{dart}");
    assert!(dart.contains(" Future<int> edit({required EditTaskUpdate task,@Deprecated('use task') EditOldUpdate? old})"), "{dart}");
}
#[test]
fn action_descriptors_separate_values_operands_and_output_sources() {
    let source = "model Todo { id String title String @@id(id) } mutation Save(label String?, todo Todo.create, maybe Todo.update<title>?, gone Todo.delete[]) { related Todo? count Int }";
    let descriptor = axton_compiler::compile(source).unwrap();
    let action = &descriptor["actions"][0];
    assert_eq!(action["name"], "Save");
    assert_eq!(action["inputs"][0]["kind"], "value");
    assert_eq!(action["inputs"][0]["required"], true);
    assert_eq!(action["inputs"][0]["nullable"], true);
    assert_eq!(action["inputs"][1]["kind"], "model");
    assert_eq!(action["inputs"][2]["cardinality"], "optional");
    assert_eq!(
        action["inputs"][2]["allowedPatchFields"],
        serde_json::json!(["title"])
    );
    assert_eq!(action["inputs"][3]["operation"], "delete");
    assert_eq!(action["inputs"][3]["cardinality"], "list");
    // Operands contribute no outputs; each output names its handler source.
    assert_eq!(action["outputs"].as_array().unwrap().len(), 2);
    assert_eq!(
        action["outputs"][0]["source"],
        serde_json::json!("handlerIdentity")
    );
    assert_eq!(
        action["outputs"][0]["handlerType"]["fields"][0]["name"],
        "id"
    );
    assert_eq!(action["outputs"][0]["modelReadVersion"], 1);
    assert_eq!(
        action["outputs"][1]["source"],
        serde_json::json!("handlerValue")
    );
    assert!(action["outputs"][1].get("handlerType").is_none());
    assert!(
        descriptor["schema"]["clientPolicies"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn action_descriptors_keep_bindings_sequence_prerequisites_and_composite_keys() {
    let source = r#"
prerequisite Uploaded(key String)
model Parent { id String children Child[] @@id(id) }
model Child { tenant String id String parentId String title String @requires(Uploaded(key: self)) parent Parent @reference(via: [parentId]) @@id(tenant, id) }
@sequence(after: [Rename(child: child)])
mutation Add(parent Parent.create, child Child.create(parent: parent)) { found Child[] }
mutation Rename(child Child.update<title>)
"#;
    let descriptors = axton_compiler::compile(source).unwrap();
    let action = &descriptors["actions"][0];
    assert_eq!(action["inputs"][1]["bindings"][0]["slot"], "parent");
    assert_eq!(
        action["sequence"]["after"][0]["arguments"]["child"],
        "child"
    );
    assert_eq!(action["outputs"][0]["cardinality"], "list");
    assert_eq!(
        action["outputs"][0]["handlerType"]["fields"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        action["outputs"][0]["handlerType"]["fields"][0]["name"],
        "tenant"
    );
    let history = axton_compiler::reconcile_action_history(&descriptors, None).unwrap();
    let snapshot = &history["actions"]["Add"]["1"];
    assert_eq!(snapshot["requirements"][0]["name"], "Uploaded");
    assert_eq!(snapshot["prerequisites"][0]["name"], "Uploaded");
    assert_eq!(snapshot["sequence"], action["sequence"]);
}

#[test]
fn retained_generated_action_binding_validates_without_relation_snapshots() {
    let source = "model Parent { id String children Child[] @@id(id) } model Child { id String parentId String parent Parent @reference(via: [parentId]) @@id(id) } mutation Add(parent Parent.create, child Child.create(parent: parent))";
    let mut config = axton_compiler::compile(source).unwrap();
    let models = axton_compiler::reconcile_model_history(&config, None).unwrap();
    let retained_models: Vec<_> = models["models"]
        .as_object()
        .unwrap()
        .values()
        .flat_map(|versions| versions.as_object().unwrap().values().cloned())
        .collect();
    config["backendModels"] = serde_json::json!(retained_models);
    let history = axton_compiler::reconcile_action_history(&config, None).unwrap();
    let snapshot = history["actions"]["Add"]["1"].clone();
    assert!(
        snapshot["input"]["models"]
            .as_array()
            .unwrap()
            .iter()
            .all(|model| model.get("relations").is_none())
    );
    let mut schema = config["schema"].clone();
    schema["actions"] = serde_json::json!([snapshot]);
    schema["resultModels"] = serde_json::json!(retained_models);
    let schema = axton_core::Schema::from_value(schema).unwrap();
    let action = schema.action("Add", 1).unwrap();
    assert!(
        axton_core::normalize_action_args(
            &schema,
            action,
            &serde_json::json!({"parent":{"id":"p"},"child":{"id":"c","parentId":"p"}})
        )
        .is_ok()
    );
    assert!(
        axton_core::normalize_action_args(
            &schema,
            action,
            &serde_json::json!({"parent":{"id":"p"},"child":{"id":"c","parentId":"other"}})
        )
        .is_err()
    );
}

#[test]
fn action_typescript_emits_flattened_operands_for_generated_client() {
    let v = compile("model Todo { id String title String @@id(id) } mutation AddTodo(todo Todo.create, patch Todo.update<title>?, gone Todo.delete[], label String?) { related Todo? matches Todo[] count Int }").unwrap();
    let ts = axton_compiler::typescript(&v);
    // Without creation defaults a create input requires every field.
    assert!(
        ts.contains("export interface TodoCreate {\n id: string;\n title: string;\n}"),
        "{ts}"
    );
    assert!(ts.contains("export type TodoUpdate<K extends keyof TodoPatch = keyof TodoPatch> = TodoIdentity & Partial<Pick<TodoPatch, K>>;"), "{ts}");
    assert!(
        ts.contains("export type TodoDelete = TodoIdentity;"),
        "{ts}"
    );
    assert!(ts.contains("export interface AddTodoInput"), "{ts}");
    assert!(ts.contains("label: string | null;"), "{ts}");
    assert!(
        ts.contains("import type { Call, CallOptions, OnceOptions } from './client.ts'"),
        "{ts}"
    );
    assert!(!ts.contains("makeActions"), "{ts}");
    // Mutations default to the durable route; `call` is direct.
    assert!(
        ts.contains("export function makeMutations(port:CallPort) { return {\n addTodo: (args:AddTodoInput, options?:AddTodoOptions):Promise<Call<AddTodoOutput>> => port.invokeAction('AddTodo',1,encodeAddTodoInput(args),decodeAddTodoOutput,options),\n call: {\n  addTodo: (args:AddTodoInput, options?:AddTodoOptions):Promise<AddTodoOutput> => port.invokeDirectAction('AddTodo',1,encodeAddTodoInput(args),decodeAddTodoOutput,options),\n }\n}; }"),
        "{ts}"
    );
    assert!(
        ts.contains("export function makeQueries(port:CallPort) { return {\n enqueue: {\n },\n invalidate: {\n }\n}; }"),
        "{ts}"
    );
    assert!(!ts.contains("class GeneratedClient {"), "{ts}");
}

#[test]
fn action_backend_emits_versioned_handler_identity_contracts_with_factory() {
    let v = compile("model Todo { id String title String @@id(id) } mutation AddTodo(todo Todo.create) { relatedTodo Todo? }").unwrap();
    let mut retained = v.clone();
    let mut old = retained["actions"][0].clone();
    old["outputs"].as_array_mut().unwrap().pop();
    old["input"] = serde_json::json!({"models": [v["schema"]["models"][0].clone()], "enums": []});
    old["outputEnums"] = serde_json::json!([]);
    let mut current = v["actions"][0].clone();
    current["version"] = serde_json::json!(2);
    retained["actions"] = serde_json::json!([old, current]);
    let ts = axton_compiler::backend_typescript(&retained, "@axton/server");
    assert!(
        ts.contains("export type AddTodoV1HandlerOutput = void;"),
        "{ts}"
    );
    assert!(ts.contains("export interface AddTodoHandlerOutput"), "{ts}");
    assert!(ts.contains("relatedTodo: TodoIdentity | null;"), "{ts}");
    assert!(
        ts.contains(
            "v1(call: MutationHandlerCall<Tx, AddTodoV1Input>): Promise<AddTodoV1HandlerOutput>"
        ),
        "{ts}"
    );
    assert!(
        ts.contains(
            "v2(call: MutationHandlerCall<Tx, AddTodoInput>): Promise<AddTodoHandlerOutput>"
        ),
        "{ts}"
    );
    assert!(
        ts.contains("mutations: Mutations<Tx>; queries?: Queries<Tx>"),
        "{ts}"
    );
    assert!(ts.contains("export function createBackend<Tx>"), "{ts}");
    assert!(ts.contains("createRuntimeBackend"), "{ts}");
    assert!(ts.contains("BackendOptions"), "{ts}");
}

#[test]
fn mixed_action_and_legacy_backend_keeps_handler_context_in_scope() {
    let v = compile("model Todo { id String @@id(id) } mutation Legacy { todo Todo.delete } mutation New(todo Todo.delete)").unwrap();
    let ts = axton_compiler::backend_typescript(&v, "@axton/server");
    assert!(
        ts.contains("legacy: { v1(call: HandlerCall<Tx, LegacyInput>)"),
        "{ts}"
    );
    assert!(
        ts.contains("new: { v1(call: MutationHandlerCall<Tx, NewInput>)"),
        "{ts}"
    );
    assert!(
        ts.contains("handlers: Handlers<Tx>; mutations: Mutations<Tx>; queries?: Queries<Tx>"),
        "{ts}"
    );
    assert!(ts.contains("export function createBackend<Tx>"), "{ts}");
}

#[test]
fn action_dart_emits_concrete_client_and_versioned_handler_contracts() {
    let v = compile("model Todo { id String title String @@id(id) } query Search(query String?) { relatedTodo Todo? } mutation Ping()").unwrap();
    let dart = axton_compiler::dart(&v);
    for expected in [
        "show RuntimeConnection, SyncServer, Call, CallOutcome, CallSuccess, CallFailure, CallStatus, CallError, CallStore",
        "required String? query",
        "class TodoIdentity",
        "required this.relatedTodo",
        "TodoIdentity? relatedTodo",
        "Todo? relatedTodo",
        "late final Mutations mutations = Mutations(client);",
        "late final Queries queries = Queries(client);",
        "late final DirectMutations call = DirectMutations(client);",
        "late final QueuedQueries enqueue = QueuedQueries(client);",
        "typedef PingOutput = void;",
        "abstract interface class QuerySearchHandlers<Ctx> {\n Future<SearchHandlerOutput> v1(QueryHandlerCall<Ctx, SearchInput> call);",
        "abstract interface class MutationPingHandlers<Ctx> {\n Future<PingHandlerOutput> v1(MutationHandlerCall<Ctx, PingInput> call);",
    ] {
        assert!(dart.contains(expected), "missing {expected}: {dart}");
    }
    // Each route fixes its return type: the Query is direct by default and
    // durable under `enqueue`; the Mutation the other way round.
    let class = |name: &str| {
        let start = dart.find(&format!("\nclass {name} {{")).unwrap();
        let end = dart[start + 1..].find("\n}\n").unwrap();
        dart[start..start + 1 + end].to_string()
    };
    assert!(class("Queries").contains("Future<SearchOutput> search("));
    assert!(class("QueuedQueries").contains("Future<Call<SearchOutput>> search("));
    assert!(class("Mutations").contains("Future<Call<PingOutput>> ping("));
    assert!(class("DirectMutations").contains("Future<PingOutput> ping("));
    assert!(!class("Mutations").contains("search("));
    assert!(!class("Queries").contains("ping("));
    assert!(!dart.contains("class Actions"));
}

#[test]
fn model_only_dart_keeps_local_crud_without_action_symbols() {
    let v = compile("model Note { id String label String @@id(id) }").unwrap();
    let dart = axton_compiler::dart(&v);
    assert!(
        dart.contains("class NoteLiveModel extends NoteTxModel"),
        "{dart}"
    );
    assert!(!dart.contains("late final Mutations mutations"), "{dart}");
    assert!(!dart.contains("late final Queries queries"), "{dart}");
}

#[test]
fn action_dart_binds_shared_runtime_and_retained_codecs() {
    let v = compile("enum Mood { calm loud } model Note { id String at DateTime mood Mood @@id(id) } mutation Save(note Note.create, changed Note.update<at>?, stamps DateTime[], when DateTime?) { saved Note? at DateTime moods Mood[] }").unwrap();
    let dart = axton_compiler::dart(&v);
    for expected in [
        "show RuntimeConnection, SyncServer, Call, CallOutcome, CallSuccess, CallFailure, CallStatus, CallError, CallStore",
        "late final Mutations mutations = Mutations(client)",
        "client.invokeAction<SaveOutput>('Save', 1",
        "client.invokeDirectAction<SaveOutput>('Save', 1",
        "Duration directTimeout = const Duration(seconds: 30)",
        "directTimeout:directTimeout",
        "DateTime.parse(",
        ".toUtc().toIso8601String()",
        "if (changed != null)",
        "class NoteLiveModel extends NoteTxModel",
    ] {
        assert!(dart.contains(expected), "missing {expected}: {dart}");
    }
    assert!(!dart.contains("abstract interface class Call<T>"), "{dart}");
}

#[test]
fn action_dart_retains_output_read_version_and_enum_snapshot() {
    let mut v = compile("enum Status { open closed } model Todo { id String title String @@id(id) @@version(2) } mutation Add() { related Todo? state Status }").unwrap();
    let mut old = v["actions"][0].clone();
    old["version"] = serde_json::json!(1);
    old["outputs"][0]["modelReadVersion"] = serde_json::json!(1);
    old["outputEnums"] = serde_json::json!([{"name":"Status","values":["open"]}]);
    old["input"] = serde_json::json!({"models":[],"enums":[]});
    let mut current = v["actions"][0].clone();
    current["version"] = serde_json::json!(2);
    v["actions"] = serde_json::json!([old, current]);
    v["backendModels"] = serde_json::json!([{"name":"Todo","version":1,"identity":["id"],"fields":[{"name":"id","type":{"kind":"scalar","name":"string"},"nullable":false},{"name":"title","type":{"kind":"scalar","name":"string"},"nullable":false}]}]);
    let dart = axton_compiler::dart(&v);
    assert!(dart.contains("class TodoV1Identity"), "{dart}");
    assert!(dart.contains("TodoV1Identity? related"), "{dart}");
    assert!(dart.contains("enum AddV1OutputStatus { open }"), "{dart}");
    assert!(dart.contains("AddV1OutputStatus state"), "{dart}");
}

#[test]
fn action_generated_identifiers_reject_current_collisions_with_positions() {
    let cases = [
        (
            "model FetchInput { id String @@id(id) } mutation Fetch()",
            "FetchInput",
        ),
        (
            "model FetchOutput { id String @@id(id) } mutation Fetch()",
            "FetchOutput",
        ),
        (
            "model FetchHandlerOutput { id String @@id(id) } mutation Fetch()",
            "FetchHandlerOutput",
        ),
        (
            "model Todo { id String @@id(id) } mutation Fetch { todo Todo.create } mutation Fetch()",
            "FetchInput",
        ),
        (
            "enum CallOutcome { open } model Todo { id String @@id(id) } mutation Fetch()",
            "CallOutcome",
        ),
        (
            "model CallSuccess { id String @@id(id) } mutation Fetch()",
            "CallSuccess",
        ),
        (
            "model CallFailure { id String @@id(id) } query Fetch()",
            "CallFailure",
        ),
        (
            "model Mutations { id String @@id(id) } mutation Fetch()",
            "Mutations",
        ),
        (
            "enum QueuedQueries { open } model Todo { id String @@id(id) } query Fetch()",
            "QueuedQueries",
        ),
        (
            "model CallPort { id String @@id(id) } mutation Fetch()",
            "CallPort",
        ),
        (
            "enum QueryContext { open } model Todo { id String @@id(id) } query Fetch()",
            "QueryContext",
        ),
        (
            "model CallRejected { id String @@id(id) } mutation Fetch()",
            "CallRejected",
        ),
        (
            "model MutationFetchHandlers { id String @@id(id) } mutation Fetch()",
            "MutationFetchHandlers",
        ),
        (
            "model QueryFetchHandlers { id String @@id(id) } query Fetch()",
            "QueryFetchHandlers",
        ),
        (
            "model FetchOptions { id String @@id(id) } query Fetch()",
            "FetchOptions",
        ),
        (
            "model Todo { id String title String @@id(id) } model FetchTodoUpdate { id String @@id(id) } mutation Fetch(todo Todo.update<title>)",
            "FetchTodoUpdate",
        ),
    ];
    for (source, name) in cases {
        let error = compile(source).unwrap_err();
        assert!(
            error.contains(name)
                && (error.contains("Mutation")
                    || error.contains("Query")
                    || error.contains("generated client"))
                && error.starts_with("1:"),
            "{source}: {error}"
        );
    }
}

#[test]
fn action_generated_identifiers_reject_other_actions_without_overbanning() {
    assert!(
        compile("model Todo { id String @@id(id) } mutation Fetch() mutation FetchInput()").is_ok()
    );
    assert!(compile("model FetchV1Input { id String @@id(id) } mutation Fetch()").is_ok());
}

#[test]
fn backend_enum_list_handler_outputs_preserve_latest_and_retained_union_cardinality() {
    let old = compile("enum Status { open closed } model Todo { id String @@id(id) } mutation Fetch() { states Status[] }").unwrap();
    let history = axton_compiler::reconcile_action_history(&old, None).unwrap();
    let mut latest = compile("enum Status { open closed archived } model Todo { id String @@id(id) } @version(2) mutation Fetch() { states Status[] }").unwrap();
    let history = axton_compiler::reconcile_action_history(&latest, Some(&history)).unwrap();
    latest["actions"] = serde_json::Value::Array(
        history["actions"]["Fetch"]
            .as_object()
            .unwrap()
            .values()
            .cloned()
            .collect(),
    );
    let emitted = axton_compiler::backend_typescript(&latest, "@axton/server");
    let interface = |name: &str| {
        let marker = format!("export interface {name} {{");
        emitted
            .split_once(&marker)
            .unwrap()
            .1
            .split_once("}\n")
            .unwrap()
            .0
    };
    assert!(
        interface("FetchV1HandlerOutput").contains("states: (\"open\" | \"closed\")[];"),
        "{emitted}"
    );
    assert!(
        interface("FetchHandlerOutput")
            .contains("states: (\"open\" | \"closed\" | \"archived\")[];"),
        "{emitted}"
    );
}

#[test]
fn action_only_and_model_only_clients_have_no_legacy_mutate_facade() {
    for schema in [
        "model Todo { id String @@id(id) } mutation AddTodo(todo Todo.create)",
        "model Todo { id String @@id(id) }",
    ] {
        let v = compile(schema).unwrap();
        let ts = axton_compiler::typescript(&v);
        let dart = axton_compiler::dart(&v);
        assert!(!ts.contains("export class Mutate"), "{ts}");
        assert!(!ts.contains("export interface MutatePort"), "{ts}");
        assert!(!dart.contains("class Mutate"), "{dart}");
        assert!(!dart.contains("late final Mutate mutate"), "{dart}");
        let client = axton_compiler::client_typescript(&v, "@axton/client");
        assert!(!client.contains("readonly mutate"), "{client}");
        assert!(!client.contains("new Mutate(client)"), "{client}");
        let backend = axton_compiler::backend_typescript(&v, "@axton/server");
        assert!(!backend.contains("MutationRejected"), "{backend}");
    }
}

/// The Scope facade is the public spelling the generated client carries: a thin
/// delegate to the runtime, with the handle types named through the generated
/// module and no get-only accessor
/// ([#150](https://github.com/zanminwang/axton/issues/150)).
#[test]
fn generated_clients_expose_the_scope_facade() {
    let schema = compile("model Entry { id String title String @@id(id) } mutation Edit { entry Entry.update<title> }").unwrap();
    let ts = axton_compiler::client_typescript(&schema, "@example/custom-runtime");
    for line in [
        "type Subscription, type SubscriptionStatus",
        " subscribe(scope: string): Promise<Subscription> { return this.#client.subscribeScope(scope); }",
        " readonly scopes: Scopes;",
        "this.scopes = new Scopes(client);",
        " subscribe(channel: string): Promise<Subscription> { return this.#client.subscribe(channel); }",
    ] {
        assert!(ts.contains(line), "{line} missing from {ts}");
    }
    assert!(
        !ts.contains("get(scope"),
        "a get-only accessor is deliberately omitted: {ts}"
    );
    let dart = axton_compiler::dart(&schema);
    for line in [
        "Subscription, SubscriptionStatus, SubscriptionInitialization, SubscriptionConnection",
        "class Scopes { final Client client; Scopes(this.client);",
        " Future<Subscription> subscribe(String scope) => client.subscribeScope(scope);",
        " late final Scopes scopes = Scopes(client);",
        " Future<Subscription> subscribe(String channel) => client.subscribe(channel);",
    ] {
        assert!(dart.contains(line), "{line} missing from {dart}");
    }
    assert!(
        !dart.contains("get(String scope"),
        "a get-only accessor is deliberately omitted: {dart}"
    );
}

#[test]
fn action_store_options_name_only_explicit_model_outputs_in_both_languages() {
    let v = compile("model Todo { id String title String @@id(id) } mutation AddTodo(todo Todo.create, gone Todo.delete[]) { related Todo? matches Todo[] count Int } mutation Ping() query Open(store String) { main Todo }").unwrap();
    let ts = axton_compiler::typescript(&v);
    // Input-bound, Delete-confirmation and scalar outputs are not keys.
    assert!(
        ts.contains("export type AddTodoOptions = CallOptions<'related'|'matches'>;"),
        "{ts}"
    );
    assert!(
        ts.contains("export type PingOptions = { store?: boolean };"),
        "{ts}"
    );
    assert!(
        ts.contains("export type OpenOptions = CallOptions<'main'>;"),
        "{ts}"
    );
    let client = axton_compiler::client_typescript(&v, "@axton/client");
    assert!(client.contains("type CallOptions"), "{client}");
    // Both routes of each kind take the same typed store options; the
    // direct Query route adds its once controls.
    assert!(
        ts.contains("  open: (args:OpenInput, options?:OpenOptions):Promise<Call<OpenOutput>> => port.invokeAction('Open',1,"),
        "{ts}"
    );
    assert!(
        ts.contains(" open: (args:OpenInput, options?:OpenOptions & OnceOptions):Promise<OpenOutput> => port.invokeQuery('Open',1,"),
        "{ts}"
    );
    let dart = axton_compiler::dart(&v);
    assert!(
        dart.contains("const AddTodoStore.outputs({this.related, this.matches}) : _mode = 2;"),
        "{dart}"
    );
    assert!(
        dart.contains("const PingStore.none() : _mode = 1;"),
        "{dart}"
    );
    assert!(!dart.contains("PingStore.outputs"), "{dart}");
    assert!(
        dart.contains("Future<Call<PingOutput>> ping({PingStore? store})"),
        "{dart}"
    );
    // A business input named store keeps its name; the selector moves aside.
    assert!(
        dart.contains("Future<OpenOutput> open({required String store, OpenStore? outputStore, bool once = false, bool refresh = false})"),
        "{dart}"
    );
    assert!(
        dart.contains(
            "Future<Call<OpenOutput>> open({required String store, OpenStore? outputStore})"
        ),
        "{dart}"
    );
    assert!(dart.contains("store: outputStore);"), "{dart}");
    // The option is a call-time choice: no descriptor or history policy.
    let descriptor = serde_json::to_string(&v).unwrap();
    assert!(!descriptor.contains("ephemeral"), "{descriptor}");
    for output in v["actions"][0]["outputs"].as_array().unwrap() {
        assert!(output.get("store").is_none(), "{output}");
    }
}

#[test]
fn action_store_selector_names_join_generated_identifier_checks() {
    let error = compile("model PingStore { id String @@id(id) } mutation Ping()").unwrap_err();
    assert!(error.contains("PingStore"), "{error}");
    let error = compile("model PingOptions { id String @@id(id) } mutation Ping()").unwrap_err();
    assert!(error.contains("PingOptions"), "{error}");
}

#[test]
fn store_eligible_outputs_cannot_reuse_dart_selector_member_names() {
    for member in [
        "toWire",
        "toString",
        "hashCode",
        "runtimeType",
        "noSuchMethod",
    ] {
        let error = compile(&format!(
            "model Todo {{ id String @@id(id) }} mutation Find() {{ {member} Todo? }}"
        ))
        .unwrap_err();
        assert!(
            error.contains(&format!("output {member} is reserved")),
            "{error}"
        );
        assert!(error.contains("FindStore"), "{error}");
    }
    // A scalar output is not a selector field and keeps the name.
    compile("mutation Count() { toWire Int }").unwrap();
    // Other store-eligible names still compile.
    compile("model Todo { id String @@id(id) } mutation Find() { all Todo? none Todo[] }").unwrap();
}

#[test]
fn generated_dart_reexports_action_store() {
    let v = compile("mutation Ping()").unwrap();
    let dart = axton_compiler::dart(&v);
    assert!(dart.contains("CallError, CallStore,"), "{dart}");
    assert!(
        dart.contains("final class PingStore extends CallStore"),
        "{dart}"
    );
}

#[test]
fn mutation_and_query_descriptors_carry_their_kind() {
    let v = compile(
        "model Todo { id String title String @@id(id) @@version(2) }\nmutation AddTodo(todo Todo.create) {}\nquery FindTodos(text String, cursor String?) { todos Todo[] nextCursor String? }",
    )
    .unwrap();
    let actions = v["schema"]["actions"].as_array().unwrap();
    assert_eq!(actions[0]["kind"], "mutation");
    assert_eq!(actions[0]["outputs"], serde_json::json!([]));
    assert_eq!(actions[1]["kind"], "query");
    assert_eq!(actions[1]["outputs"][0]["source"], "handlerIdentity");
    assert_eq!(actions[1]["outputs"][0]["cardinality"], "list");
    assert_eq!(actions[1]["outputs"][0]["modelReadVersion"], 2);
    assert_eq!(actions[1]["outputs"][1]["cardinality"], "optional");
    assert_eq!(v["actions"], v["schema"]["actions"]);
    // A Query-only schema with no Models is a complete contract.
    let only = compile("query Ping(text String) { echo String }").unwrap();
    assert_eq!(only["schema"]["actions"][0]["kind"], "query");
}

#[test]
fn queries_reject_mutation_operands_and_sequence_at_the_member() {
    for (source, at, needle) in [
        (
            "model Todo { id String @@id(id) }\nquery Edit(text String, todo Todo.create)",
            "2:25:",
            "Model operand todo",
        ),
        (
            "model Todo { id String @@id(id) }\nquery Edit(todo Todo.update)",
            "2:12:",
            "Model operand todo",
        ),
        (
            "model Todo { id String @@id(id) }\nquery Edit(todos Todo.delete[])",
            "2:12:",
            "Model operand todos",
        ),
        (
            "model Todo { id String @@id(id) }\nmutation Add(todo Todo.create)\n@sequence(after: [Add()]) query Find()",
            "3:1:",
            "cannot declare @sequence",
        ),
        (
            "model Todo { id String @@id(id) }\nquery Find()\n@sequence(after: [Find()]) mutation Add(todo Todo.create)",
            "3:1:",
            "unknown sequence Mutation Find",
        ),
    ] {
        let error = compile(source).unwrap_err();
        assert!(
            error.starts_with(at) && error.contains(needle),
            "{source}: {error}"
        );
    }
}

#[test]
fn operation_names_share_one_namespace_and_reserve_route_members() {
    for (source, needle) in [
        (
            "mutation Find()\nquery Find()",
            "2:1: duplicate operation Find",
        ),
        (
            "query Find()\nmutation find()",
            "2:1: duplicate operation find",
        ),
        ("mutation Call()", "1:1: Mutation name Call is reserved"),
        ("mutation call()", "1:1: Mutation name call is reserved"),
        ("query Enqueue()", "1:1: Query name Enqueue is reserved"),
        ("query enqueue()", "1:1: Query name enqueue is reserved"),
        ("mutation Client()", "1:1: Mutation name Client is reserved"),
        ("query ToString()", "1:1: Query name ToString is reserved"),
        (
            "mutation HashCode()",
            "1:1: Mutation name HashCode is reserved",
        ),
        (
            "model Todo { id String @@id(id) }\nquery Todo()",
            "2:1: Query Todo collides with a model or enum",
        ),
    ] {
        let error = compile(source).unwrap_err();
        assert!(error.starts_with(needle), "{source}: {error}");
    }
    // Each route member is reserved only in the namespace that has it; a
    // Query named Call still collides with the generated CallOptions helper.
    compile("mutation Enqueue()").unwrap();
    let error = compile("query Call()").unwrap_err();
    assert!(error.contains("CallOptions"), "{error}");
}

#[test]
fn a_kind_change_at_a_new_version_registers_each_version_under_its_own_kind() {
    let v1 = compile("model Todo { id String @@id(id) } mutation Find(text String) { count Int }")
        .unwrap();
    let mut retained = compile(
        "model Todo { id String @@id(id) } @version(2) query Find(text String) { count Int }",
    )
    .unwrap();
    // Retain both versions the way the CLI does, from the operation history.
    let history = axton_compiler::reconcile_action_history(&v1, None).unwrap();
    let history = axton_compiler::reconcile_action_history(&retained, Some(&history)).unwrap();
    let versions: Vec<serde_json::Value> = history["actions"]["Find"]
        .as_object()
        .unwrap()
        .values()
        .cloned()
        .collect();
    retained["actions"] = serde_json::json!(versions);
    retained["schema"]["actions"] = retained["actions"].clone();
    let backend = axton_compiler::backend_typescript(&retained, "@axton/server");
    let section = |name: &str| {
        let start = backend
            .find(&format!("export interface {name}<Tx> {{"))
            .unwrap();
        let end = backend[start..].find("\n}\n").unwrap();
        backend[start..start + end].to_string()
    };
    assert_eq!(
        section("Mutations"),
        "export interface Mutations<Tx> {\n find: { v1(call: MutationHandlerCall<Tx, FindV1Input>): Promise<FindV1HandlerOutput> } | ((call: MutationHandlerCall<Tx, FindV1Input>) => Promise<FindV1HandlerOutput>);"
    );
    assert_eq!(
        section("Queries"),
        "export interface Queries<Tx> {\n find: { v2(call: QueryHandlerCall<Tx, FindInput>): Promise<FindHandlerOutput> };"
    );
    assert!(
        backend.contains("mutations: Mutations<Tx>; queries: Queries<Tx>"),
        "{backend}"
    );
    // Current clients expose the name only under its current kind.
    let ts = axton_compiler::typescript(&retained);
    assert!(
        ts.contains("export function makeMutations(port:CallPort) { return {\n call: {\n }\n}; }"),
        "{ts}"
    );
    assert!(
        ts.contains(" find: (args:FindInput, options?:FindOptions & OnceOptions):Promise<FindOutput> => port.invokeQuery('Find',2,"),
        "{ts}"
    );
    let dart = axton_compiler::dart(&retained);
    assert!(
        dart.contains("abstract interface class MutationFindHandlers<Ctx> {\n Future<FindV1HandlerOutput> v1(MutationHandlerCall<Ctx, FindV1Input> call);\n}"),
        "{dart}"
    );
    assert!(
        dart.contains("abstract interface class QueryFindHandlers<Ctx> {\n Future<FindHandlerOutput> v2(QueryHandlerCall<Ctx, FindInput> call);\n}"),
        "{dart}"
    );
    axton_compiler::check_action_names(&retained).unwrap();
}

#[test]
fn direct_queries_generate_once_options_and_typed_invalidators() {
    let v = compile("model Todo { id String title String @@id(id) } query GetTodos(projectId String) { todos Todo[] total Int } query Ping() mutation Rename(todo Todo.update)").unwrap();
    let ts = axton_compiler::typescript(&v);
    for expected in [
        "import type { Call, CallOptions, OnceOptions } from './client.ts'",
        "invokeQuery<T>(name:string,version:number,args:object,decode:(value:unknown)=>T,options?:CallOptions&OnceOptions):Promise<T>;",
        "invalidateQuery(name:string,version:number,args:object):Promise<void>;",
        "export function makeQueries(port:CallPort) { return {\n getTodos: (args:GetTodosInput, options?:GetTodosOptions & OnceOptions):Promise<GetTodosOutput> => port.invokeQuery('GetTodos',1,encodeGetTodosInput(args),decodeGetTodosOutput,options),\n ping: (args:PingInput, options?:PingOptions & OnceOptions):Promise<PingOutput> => port.invokeQuery('Ping',1,encodePingInput(args),decodePingOutput,options),\n enqueue: {\n  getTodos: (args:GetTodosInput, options?:GetTodosOptions):Promise<Call<GetTodosOutput>> => port.invokeAction(",
        " invalidate: {\n  getTodos: (args:GetTodosInput):Promise<void> => port.invalidateQuery('GetTodos',1,encodeGetTodosInput(args)),\n  ping: (args:PingInput):Promise<void> => port.invalidateQuery('Ping',1,encodePingInput(args)),\n }\n}; }",
    ] {
        assert!(ts.contains(expected), "missing {expected}: {ts}");
    }
    // Mutation routes keep the store-only options and the generic methods.
    let mutations = &ts[ts.find("export function makeMutations").unwrap()
        ..ts.find("export function makeQueries").unwrap()];
    assert!(!mutations.contains("OnceOptions"), "{mutations}");
    assert!(!mutations.contains("invokeQuery"), "{mutations}");
    let client = axton_compiler::client_typescript(&v, "@axton/client");
    assert!(client.contains("type OnceOptions"), "{client}");
    let dart = axton_compiler::dart(&v);
    let class = |name: &str| {
        let start = dart.find(&format!("\nclass {name} {{")).unwrap();
        let end = dart[start + 1..].find("\n}\n").unwrap();
        dart[start..start + 1 + end].to_string()
    };
    let queries = class("Queries");
    assert!(
        queries.contains("Future<GetTodosOutput> getTodos({required String projectId, GetTodosStore? store, bool once = false, bool refresh = false}) => client.invokeQuery<GetTodosOutput>('GetTodos', 1, {'projectId': _dartActionEncode(projectId)}, "),
        "{queries}"
    );
    assert!(
        queries.contains("store: store, once: once, refresh: refresh);"),
        "{queries}"
    );
    assert!(
        queries.contains(
            "Future<PingOutput> ping({PingStore? store, bool once = false, bool refresh = false})"
        ),
        "{queries}"
    );
    assert!(
        queries.contains("late final QueryInvalidations invalidate = QueryInvalidations(client);"),
        "{queries}"
    );
    let invalidations = class("QueryInvalidations");
    assert!(
        invalidations.contains("Future<void> getTodos({required String projectId}) => client.invalidateQuery('GetTodos', 1, {'projectId': _dartActionEncode(projectId)});"),
        "{invalidations}"
    );
    assert!(
        invalidations.contains("Future<void> ping() => client.invalidateQuery('Ping', 1, {});"),
        "{invalidations}"
    );
    for route in ["QueuedQueries", "Mutations", "DirectMutations"] {
        let body = class(route);
        assert!(!body.contains("once"), "{route}: {body}");
        assert!(!body.contains("invokeQuery"), "{route}: {body}");
    }
}

#[test]
fn once_controls_take_collision_safe_dart_names_beside_business_inputs() {
    let v =
        compile("query Find(once Boolean, refresh Boolean, store String, callOnce Int) { n Int }")
            .unwrap();
    let dart = axton_compiler::dart(&v);
    assert!(
        dart.contains("Future<FindOutput> find({required bool once, required bool refresh, required String store, required int callOnce, FindStore? outputStore, bool callOnce$ = false, bool callRefresh = false}) => client.invokeQuery<FindOutput>('Find', 1, "),
        "{dart}"
    );
    assert!(
        dart.contains("store: outputStore, once: callOnce$, refresh: callRefresh);"),
        "{dart}"
    );
    assert!(
        dart.contains("Future<void> find({required bool once, required bool refresh, required String store, required int callOnce}) => client.invalidateQuery('Find', 1, "),
        "{dart}"
    );
}

#[test]
fn invalidate_is_reserved_in_the_query_namespace_only() {
    for (source, needle) in [
        (
            "query Invalidate()",
            "1:1: Query name Invalidate is reserved",
        ),
        (
            "query invalidate()",
            "1:1: Query name invalidate is reserved",
        ),
    ] {
        let error = compile(source).unwrap_err();
        assert!(error.starts_with(needle), "{source}: {error}");
    }
    compile("mutation Invalidate()").unwrap();
    for helper in ["OnceOptions", "QueryInvalidations"] {
        let error = compile(&format!(
            "model {helper} {{ id String @@id(id) }} query Ping()"
        ))
        .unwrap_err();
        assert!(error.contains("operation helper"), "{helper}: {error}");
    }
}
