use axton_compiler::parse::ActionInputDecl;
use axton_compiler::validate::{Cardinality, FieldType, OnDelete, Operation, Scalar};
use axton_compiler::{Pos, compile, generate, parse, validate};
use axton_core::CallKind;

const SOURCE: &str = "prerequisite Uploaded(key String)\nenum Status { active archived }\nmodel Parent {\n id UUID\n children Child[]\n @@id(id)\n @@unique(id)\n @@version(2)\n}\nmodel Child {\n id UUID\n parentId UUID\n label String @requires(Uploaded(key: self))\n parent Parent @reference(via: [parentId], onTargetDelete: delete)\n @@id(id)\n}\nmutation Add {\n parent Parent.create\n children Child.create(parent: parent)[]\n @@version(2)\n @@sequence(after: [Rename(parent: parent)])\n}\nmutation Rename { parent Parent.update<> }\n";

fn pos(line: usize, col: usize) -> Pos {
    Pos { line, col }
}

#[test]
fn parse_keeps_every_declaration_with_its_position() {
    let d = parse(SOURCE).unwrap();
    assert_eq!(d.prerequisites.len(), 1);
    assert_eq!(d.prerequisites[0].name, "Uploaded");
    assert_eq!(d.prerequisites[0].pos, pos(1, 1));
    assert_eq!(d.prerequisites[0].fields[0].name, "key");
    assert_eq!(d.prerequisites[0].fields[0].type_name, "String");
    assert_eq!(d.prerequisites[0].fields[0].pos, pos(1, 23));
    assert_eq!(d.enums[0].name, "Status");
    assert_eq!(d.enums[0].values, vec!["active", "archived"]);
    assert_eq!(d.enums[0].pos, pos(2, 1));
    let parent = &d.models[0];
    assert_eq!((parent.name.as_str(), parent.pos), ("Parent", pos(3, 1)));
    assert_eq!(parent.identity, vec!["id"]);
    assert_eq!(parent.fields[1].name, "children");
    assert!(parent.fields[1].list && !parent.fields[1].nullable);
    assert_eq!(parent.fields[1].pos, pos(5, 2));
    assert_eq!(parent.unique[0].fields, vec!["id"]);
    assert_eq!(parent.unique[0].pos, pos(7, 2));
    assert_eq!(parent.version, 2);
    let child = &d.models[1];
    assert_eq!(child.version, 1, "a model without @@version is version 1");
    let label = &child.fields[2];
    assert_eq!(label.pos, pos(13, 2));
    assert_eq!(
        label.attributes["requires"],
        serde_json::json!({"0":{"name":"Uploaded","arguments":{"key":"self"}}})
    );
    assert_eq!(
        child.fields[3].attributes["reference"],
        serde_json::json!({"via":["parentId"],"onTargetDelete":"delete"})
    );
    let add = &d.mutations[0];
    assert_eq!(
        (add.name.as_str(), add.version, add.pos),
        ("Add", 2, pos(17, 1))
    );
    assert_eq!(add.slots[0].pos, pos(18, 2));
    assert_eq!(add.slots[1].cardinality, "list");
    assert_eq!(
        add.slots[1].relation_bindings,
        serde_json::json!({"parent":"parent"})
    );
    let sequence = add.sequence.as_ref().unwrap();
    assert_eq!(sequence.pos, pos(21, 2));
    assert_eq!(sequence.arguments["after"][0]["name"], "Rename");
    assert_eq!(d.mutations[1].slots[0].allowed_patch_fields, Some(vec![]));
    assert_eq!(d.end, pos(24, 1));
}

#[test]
fn deprecated_is_a_field_level_directive_with_an_optional_reason() {
    // GraphQL style: on a field, an enum value or a mutation slot; the reason is optional.
    let d = parse("enum Status { active archived @deprecated(reason: \"use closed\") closed }\nmodel Task {\n id UUID\n name String @deprecated(reason: \"renamed to title\")\n title String\n legacy Int? @deprecated\n @@id(id)\n}\nmutation Edit { task Task.update<title> old Task.update<name>? @deprecated(reason: \"use task\") }").unwrap();
    assert_eq!(d.enums[0].values, vec!["active", "archived", "closed"]);
    assert_eq!(
        d.enums[0].deprecated,
        vec![(String::from("archived"), Some(String::from("use closed")))]
    );
    let task = &d.models[0];
    assert_eq!(
        task.fields[1].deprecated,
        Some(Some("renamed to title".into()))
    );
    assert_eq!(task.fields[2].deprecated, None);
    assert_eq!(
        task.fields[3].deprecated,
        Some(None),
        "a bare @deprecated has no reason"
    );
    assert_eq!(d.mutations[0].slots[0].deprecated, None);
    assert_eq!(
        d.mutations[0].slots[1].deprecated,
        Some(Some("use task".into()))
    );
    for (source, message) in [
        (
            "model A { id UUID @deprecated(why: \"x\") @@id(id) }",
            "deprecated accepts only reason",
        ),
        (
            "model A { id UUID @deprecated(reason: x) @@id(id) }",
            "deprecated reason must be a string",
        ),
        (
            "model A { id UUID @deprecated @deprecated @@id(id) }",
            "duplicate field directive",
        ),
        (
            "enum S { a @deprecated @deprecated }",
            "duplicate deprecated",
        ),
    ] {
        let e = parse(source).unwrap_err();
        assert!(e.contains(message), "{source}: {e}");
    }
}

#[test]
fn model_version_follows_the_mutation_rules() {
    // The same declaration, the same range and the same duplicate refusal as a mutation.
    for (source, message) in [
        (
            "model A { id UUID @@id(id) @@version(0) }",
            "version must be positive",
        ),
        (
            "model A { id UUID @@id(id) @@version(x) }",
            "expected positive version",
        ),
        (
            "model A { id UUID @@id(id) @@version(9007199254740992) }",
            "version must be positive",
        ),
        (
            "model A { id UUID @@id(id) @@version(1) @@version(2) }",
            "duplicate version",
        ),
    ] {
        let e = parse(source).unwrap_err();
        assert!(e.contains(message), "{source}: {e}");
    }
    assert_eq!(
        parse("model A { id UUID @@id(id) @@version(9007199254740991) }")
            .unwrap()
            .models[0]
            .version,
        9007199254740991
    );
}

#[test]
fn parse_reports_syntax_errors_with_the_found_token_and_nothing_semantic() {
    let e =
        parse("model A { id UUID @@id(id) }\nmodel B { id Nope @@id(id) @@bogus(x) }").unwrap_err();
    assert!(e.starts_with("2:"), "{e}");
    assert!(e.contains("unsupported model directive bogus"), "{e}");
    assert!(e.contains("(found"), "{e}");
    // A semantic mistake (unknown type) parses fine; only validate refuses it.
    let d = parse("model B {\n id UUID\n label Nope\n @@id(id)\n}").unwrap();
    assert_eq!(d.models[0].fields[1].type_name, "Nope");
    let e = validate(&d).unwrap_err();
    assert!(e.starts_with("3:"), "{e}");
    assert!(e.contains("unknown or unsupported field type"), "{e}");
}

#[test]
fn compile_is_parse_then_validate_then_generate_and_declarations_are_plain_data() {
    let d = parse(SOURCE).unwrap();
    assert_eq!(
        generate::descriptors(&validate(&d).unwrap()),
        compile(SOURCE).unwrap()
    );
    assert_eq!(parse(SOURCE).unwrap(), d, "parsing is deterministic");
    let copy = d.clone();
    assert_eq!(validate(&copy).unwrap(), validate(&d).unwrap());
}

#[test]
fn validate_resolves_declarations_into_typed_data_without_descriptors() {
    let v = validate(&parse(SOURCE).unwrap()).unwrap();
    assert_eq!(v.enums[0].values, vec!["active", "archived"]);
    let child = &v.models[1];
    assert_eq!(child.name, "Child");
    assert_eq!((v.models[0].version, child.version), (2, 1));
    // Relation fields leave the stored fields and resolve to their target.
    assert_eq!(
        child
            .fields
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>(),
        ["id", "parentId", "label"]
    );
    assert_eq!(child.fields[0].ty, FieldType::Scalar(Scalar::Uuid));
    assert_eq!(child.relations[0].name, "parent");
    assert_eq!(child.relations[0].fields, vec!["parentId"]);
    assert_eq!(child.relations[0].target_fields, vec!["id"]);
    assert_eq!(child.relations[0].on_delete, OnDelete::Delete);
    assert_eq!(v.models[0].unique, vec![vec!["id".to_string()]]);
    assert_eq!(v.unique_constraints[0].model, "Parent");
    let inverse = &v.inverses[0];
    assert_eq!(
        (inverse.model.as_str(), inverse.name.as_str()),
        ("Parent", "children")
    );
    assert_eq!(
        (inverse.reference.as_str(), inverse.fields.as_slice()),
        ("parent", &["parentId".to_string()][..])
    );
    assert_eq!(inverse.relation_name, None);
    assert_eq!(v.requirements[0].prerequisite, "Uploaded");
    assert_eq!(v.requirements[0].arguments, vec!["key"]);
    assert_eq!(v.prerequisites[0].fields[0].type_name, "String");
    let add = &v.mutations[0];
    assert_eq!((add.name.as_str(), add.version), ("Add", 2));
    assert_eq!(add.slots[1].operation, Operation::Create);
    assert_eq!(add.slots[1].cardinality, Cardinality::List);
    assert_eq!(add.slots[1].bindings[0].slot, "parent");
    assert_eq!(add.slots[1].bindings[0].fields, vec!["parentId"]);
    let sequence = add.sequence.as_ref().unwrap();
    assert_eq!(sequence.after[0].mutation, "Rename");
    assert_eq!(sequence.after[0].bindings[0].path, vec!["parent"]);
    // An update slot without a restriction is resolved to every non-identity stored field.
    let rename = &v.mutations[1];
    assert_eq!(rename.slots[0].allowed_patch_fields, Some(vec![]));
    let d =
        parse("model A { id UUID title String @@id(id) } mutation Edit { a A.update }").unwrap();
    let v = validate(&d).unwrap();
    assert_eq!(
        v.mutations[0].slots[0].allowed_patch_fields,
        Some(vec!["title".to_string()])
    );
}

#[test]
fn generate_is_a_pure_function_of_the_validated_schema() {
    let v = validate(&parse(SOURCE).unwrap()).unwrap();
    let once = serde_json::to_string_pretty(&generate::descriptors(&v)).unwrap();
    let again = serde_json::to_string_pretty(&generate::descriptors(&v.clone())).unwrap();
    assert_eq!(once, again);
    assert_eq!(generate::descriptors(&v)["schema"], generate::schema(&v));
    assert_eq!(
        generate::typescript(&generate::descriptors(&v)),
        axton_compiler::typescript(&compile(SOURCE).unwrap())
    );
    // The client descriptor is what core loads.
    axton_core::Schema::from_value(generate::schema(&v)).unwrap();
}

#[test]
fn semantic_errors_come_from_validate_alone() {
    // Validate refuses before any descriptor exists; the error names the declaration.
    let d = parse(
        "model A {
 id UUID
 @@id(id)
}
mutation Edit {
 a A.update<nope>
}


",
    )
    .unwrap();
    let e = validate(&d).unwrap_err();
    assert!(e.starts_with("6:"), "{e}");
    assert!(e.contains("invalid allowed patch field"), "{e}");
    let e = validate(
        &parse(
            "model A { id UUID @@id(id) }
model Parent {
 id UUID
 children A[]
 @@id(id)
}

",
        )
        .unwrap(),
    )
    .unwrap_err();
    assert!(e.starts_with("4:"), "{e}");
    assert!(
        e.contains("inverse must resolve to exactly one reference"),
        "{e}"
    );
}

#[test]
fn actions_parse_values_models_and_named_outputs_with_positions() {
    let d = parse("mutation AddTodo(todo Todo.create)\nmutation Search(query String?)\nmutation SendEmail(to String, body String) { messageId String }\nmutation GetTodos(projectId String) { todos Todo[] }").unwrap();
    assert_eq!(d.actions.len(), 4);
    assert_eq!(
        (
            d.actions[0].name.as_str(),
            d.actions[0].version,
            d.actions[0].pos
        ),
        ("AddTodo", 1, pos(1, 1))
    );
    match &d.actions[0].inputs[0] {
        ActionInputDecl::Model(slot) => assert_eq!(
            (
                slot.name.as_str(),
                slot.model.as_str(),
                slot.operation.as_str(),
                slot.pos
            ),
            ("todo", "Todo", "create", pos(1, 18))
        ),
        other => panic!("expected model input: {other:?}"),
    }
    match &d.actions[1].inputs[0] {
        ActionInputDecl::Value(field) => assert_eq!(
            (
                field.name.as_str(),
                field.type_name.as_str(),
                field.nullable,
                field.pos
            ),
            ("query", "String", true, pos(2, 17))
        ),
        other => panic!("expected value input: {other:?}"),
    }
    assert_eq!(d.actions[2].outputs[0].field.name, "messageId");
    assert_eq!(d.actions[2].outputs[0].field.pos, pos(3, 46));
    assert_eq!(d.actions[3].outputs[0].field.type_name, "Todo");
    assert!(d.actions[3].outputs[0].field.list);
    assert_eq!(d.actions[3].outputs[0].field.pos, pos(4, 39));
}

#[test]
fn actions_parse_declaration_annotations_and_model_operand_details() {
    let d = parse("@version(2)\n@sequence(after: [Rename(todo: todo)])\nmutation AddTodo(todo Todo.create, maybe Todo.update<title>(parent: todo)?, children Todo.delete[]) { related Todo? }").unwrap();
    let a = &d.actions[0];
    assert_eq!((a.version, a.pos), (2, pos(3, 1)));
    assert_eq!(a.sequence.as_ref().unwrap().pos, pos(2, 1));
    assert_eq!(
        a.sequence.as_ref().unwrap().arguments["after"][0]["name"],
        "Rename"
    );
    match &a.inputs[1] {
        ActionInputDecl::Model(slot) => {
            assert_eq!(slot.cardinality, "optional");
            assert_eq!(slot.allowed_patch_fields, Some(vec!["title".into()]));
            assert_eq!(slot.relation_bindings["parent"], "todo");
            assert_eq!(slot.pos, pos(3, 36));
        }
        other => panic!("expected model input: {other:?}"),
    }
    match &a.inputs[2] {
        ActionInputDecl::Model(slot) => assert_eq!(slot.cardinality, "list"),
        other => panic!("expected model input: {other:?}"),
    }
    assert!(a.outputs[0].field.nullable);
}

#[test]
fn actions_report_precise_delimiter_and_list_syntax_errors() {
    for (source, expected) in [
        ("mutation A(x String", "1:20: expected )"),
        ("mutation A(x String) { result String", "1:37: expected }"),
        (
            "mutation A(x String) { result String[][] }",
            "1:39: nested lists are unsupported",
        ),
        (
            "mutation A(x String?[]) {}",
            "1:21: nullable list elements are unsupported",
        ),
        (
            "mutation A(x String[]?) {}",
            "1:23: nullable lists are unsupported",
        ),
    ] {
        let error = parse(source).unwrap_err();
        assert!(error.starts_with(expected), "{source}: {error}");
    }
}

#[test]
fn model_accepts_leading_version_and_rejects_conflicting_or_misplaced_directives() {
    let model = &parse("@version(3)\nmodel Todo { id UUID @@id(id) }")
        .unwrap()
        .models[0];
    assert_eq!((model.version, model.pos), (3, pos(2, 1)));
    for (source, message) in [
        (
            "@version(2) model Todo { id UUID @@id(id) @@version(3) }",
            "duplicate version",
        ),
        (
            "@sequence(after: []) model Todo { id UUID @@id(id) }",
            "sequence requires mutation",
        ),
        (
            "@version(2) mutation Old { todo Todo.create }",
            "declaration directives are unsupported on mutation",
        ),
        ("@version(2) @version(3) mutation A()", "duplicate version"),
        ("@sequence() @sequence() mutation A()", "duplicate sequence"),
        (
            "mutation A() @version(2)",
            "expected declaration after directive",
        ),
    ] {
        let error = parse(source).unwrap_err();
        assert!(error.contains(message), "{source}: {error}");
    }
}

#[test]
fn action_value_members_reuse_field_deprecation_annotations() {
    let action = &parse(
        "mutation SendEmail(to String @deprecated(reason: \"use recipient\")) { messageId String @deprecated }",
    )
    .unwrap()
    .actions[0];
    match &action.inputs[0] {
        ActionInputDecl::Value(field) => {
            assert_eq!(field.deprecated, Some(Some("use recipient".into())));
        }
        other => panic!("expected value input: {other:?}"),
    }
    assert_eq!(action.outputs[0].field.deprecated, Some(None));
}

#[test]
fn operation_keywords_select_kind_and_keep_the_legacy_block_separate() {
    let d = parse(
        "mutation Add(title String)\n@version(2) query Find(text String?) { count Int }\nmutation Old { todo Todo.create }\nmutation Plain() {}",
    )
    .unwrap();
    let kinds: Vec<_> = d
        .actions
        .iter()
        .map(|a| (a.name.as_str(), a.kind, a.version, a.pos))
        .collect();
    assert_eq!(
        kinds,
        [
            ("Add", CallKind::Mutation, 1, pos(1, 1)),
            ("Find", CallKind::Query, 2, pos(2, 13)),
            ("Plain", CallKind::Mutation, 1, pos(4, 1)),
        ]
    );
    // The older slot block is not an operation and keeps its own grammar.
    assert_eq!(d.mutations.len(), 1);
    assert_eq!(d.mutations[0].name, "Old");
}

#[test]
fn retired_action_keyword_and_blockless_queries_are_syntax_errors() {
    for (source, message) in [
        (
            "action Send(to String)",
            "1:1: action declarations were replaced",
        ),
        (
            "@version(2) action Send(to String)",
            "1:13: action declarations were replaced",
        ),
        ("query Find { todo Todo.create }", "1:12: expected ("),
    ] {
        let error = parse(source).unwrap_err();
        assert!(error.starts_with(message), "{source}: {error}");
    }
}

#[test]
fn default_keeps_its_expression_kind_and_position() {
    use axton_compiler::parse::DefaultExpr;
    let d = parse("model A {\n id UUID @default(uuid())\n n Int @default(-1.5e3)\n s Status @default(open)\n t String @default(\"x\")\n @@id(id)\n}").unwrap();
    let fields = &d.models[0].fields;
    let default = |i: usize| fields[i].default.clone().unwrap();
    assert_eq!(
        default(0).expr,
        DefaultExpr::Call {
            name: "uuid".into(),
            arguments: 0
        }
    );
    assert_eq!(default(0).pos, pos(2, 10));
    assert_eq!(default(1).expr, DefaultExpr::Number("-1.5e3".into()));
    assert_eq!(default(2).expr, DefaultExpr::Identifier("open".into()));
    assert_eq!(default(3).expr, DefaultExpr::String("x".into()));
    assert!(
        fields[0].attributes.is_empty(),
        "@default is not a relation attribute"
    );
}

#[test]
fn sequence_argument_names_may_be_relation_paths() {
    let d = parse("@sequence(after: [Add(members.space: request.space, other: request)])\nmutation Propose(request Request.create)").unwrap();
    let call = &d.actions[0].sequence.as_ref().unwrap().arguments["after"][0];
    assert_eq!(call["arguments"]["members.space"], "request.space");
    assert_eq!(call["arguments"]["other"], "request");
    for (source, message) in [(
        "@sequence(after: [Add(members.space request)]) mutation A()",
        "1:37: expected ,",
    )] {
        let error = parse(source).unwrap_err();
        assert!(error.starts_with(message), "{source}: {error}");
    }
}
