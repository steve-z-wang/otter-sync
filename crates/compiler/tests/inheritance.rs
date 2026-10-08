use axton_compiler::{compile, dart, typescript};
use serde_json::json;

const SHARED: &str = r#"
abstract model Fields {
  id UUID @default(uuid())
  text String @default("") @deprecated(reason: "old")
}
abstract model Dated extends Fields { createdAt DateTime @default(now()) }
model Draft extends Dated { @@id(id) }
model Entry extends Dated { journalId UUID @@id(journalId, id) @@bootstrap @@version(2) }
"#;

#[test]
fn abstract_fields_expand_without_inheriting_model_directives() {
    let c = compile(SHARED).unwrap();
    let models = c["schema"]["models"].as_array().unwrap();
    assert_eq!(models.len(), 2);
    assert_eq!(models[0]["name"], "Draft");
    assert_eq!(models[0]["identity"], json!(["id"]));
    assert_eq!(models[0]["version"], 1);
    assert!(models[0].get("bootstrap").is_none());
    assert_eq!(models[0]["unique"], json!([]));
    assert_eq!(
        models[0]["fields"][0]["createDefault"],
        json!({"kind":"uuid"})
    );
    assert_eq!(
        models[0]["fields"][1]["createDefault"],
        json!({"kind":"literal", "value":""})
    );
    assert_eq!(models[1]["identity"], json!(["journalId", "id"]));
    assert_eq!(models[1]["bootstrap"], true);
    assert_eq!(models[1]["version"], 2);
    assert_eq!(c["loaders"], json!(["Draft", "Entry"]));
    assert!(
        c["deprecations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|d| d["model"] == "Entry" && d["field"] == "text")
    );
    axton_core::Schema::from_value(c["schema"].clone()).unwrap();
}

#[test]
fn inherited_relations_and_requirements_are_checked_in_each_concrete_model() {
    let c = compile(
        r#"
prerequisite Upload(uri String)
abstract model ParentFields { id UUID children Child[] @inverse(parent) }
model Parent extends ParentFields { @@id(id) }
abstract model ChildFields {
 id UUID
 parentId UUID
 parent Parent @reference(parent, via: [parentId], onTargetDelete: delete)
 uri String @requires(Upload(uri: self))
}
model Child extends ChildFields { @@id(id) }
"#,
    )
    .unwrap();
    assert_eq!(
        c["schema"]["models"][1]["relations"][0]["onDelete"],
        "delete"
    );
    assert_eq!(c["requirements"][0]["model"], "Child");
    assert_eq!(c["inverses"][0]["reference"], "parent");
    let bad = compile("model Parent { id UUID @@id(id) } abstract model Fields { parent Parent @reference(via: [parentId]) } model Child extends Fields { id UUID @@id(id) }").unwrap_err();
    assert!(bad.contains("unknown reference field"), "{bad}");
}

#[test]
fn inheritance_rejects_cycles_concrete_parents_conflicts_and_abstract_targets() {
    for (source, expected) in [
        (
            "abstract model A extends B { a String } abstract model B extends A { b String } model C extends A { @@id(a) }",
            "inheritance cycle",
        ),
        (
            "model A { id UUID @@id(id) } model B extends A { @@id(id) }",
            "abstract",
        ),
        (
            "abstract model A { id UUID } model B extends A { id UUID @@id(id) }",
            "duplicate field",
        ),
        (
            "model B extends Missing { id UUID @@id(id) }",
            "unknown parent",
        ),
        (
            "abstract model A { id UUID } model B { id UUID a A @reference(via: [id]) @@id(id) }",
            "abstract",
        ),
        (
            "abstract model A { id UUID } model B extends A { value String }",
            "requires an @@id",
        ),
        (
            "model A { id UUID @@id(id) @@bootstrap @@bootstrap }",
            "duplicate bootstrap",
        ),
        (
            "abstract model A { id UUID @@id(id) } model B extends A { @@id(id) }",
            "abstract",
        ),
        (
            "abstract model A { id UUID @@bootstrap } model B extends A { @@id(id) }",
            "abstract",
        ),
        (
            "abstract model A { id UUID @@version(2) } model B extends A { @@id(id) }",
            "abstract",
        ),
        (
            "abstract model A { id UUID @@unique(id) } model B extends A { @@id(id) }",
            "abstract",
        ),
    ] {
        let error = compile(source).unwrap_err();
        assert!(error.contains(expected), "expected {expected}: {error}");
    }
}

#[test]
fn shared_field_types_inherit_without_identity_or_create_input_collisions() {
    let c = compile(SHARED).unwrap();
    let ts = typescript(&c);
    assert!(ts.contains("export interface Fields {"));
    assert!(ts.contains("export interface Dated extends Fields {"));
    assert!(ts.contains("export interface Entry extends Dated {"));
    assert!(ts.contains("export interface EntryCreate {"));
    assert!(ts.contains("export interface EntryIdentity {"));
    let d = dart(&c);
    assert!(d.contains("abstract interface class Fields {"));
    assert!(d.contains("abstract interface class Dated implements Fields {"));
    assert!(d.contains("class Entry implements EntryCreateInput, Dated {"));
}

#[test]
fn bootstrap_defaults_false_and_is_not_a_read_contract_field() {
    let c = compile("model Entry { id UUID @@id(id) @@bootstrap }").unwrap();
    assert_eq!(c["schema"]["models"][0]["bootstrap"], true);
    assert!(c["schema"]["resultModels"][0].get("bootstrap").is_none());
    let schema = axton_core::Schema::from_value(c["schema"].clone()).unwrap();
    assert_eq!(
        serde_json::to_value(schema).unwrap()["models"][0]["bootstrap"],
        true
    );
}

#[test]
fn inherited_field_validity_is_not_inferred_from_a_sibling_model() {
    let error = compile("abstract model Fields { id UUID? } model Valid extends Fields { key UUID @@id(key) } model Invalid extends Fields { @@id(id) }").unwrap_err();
    assert!(
        error.contains("identity fields must be non-nullable"),
        "{error}"
    );
    let error = compile("abstract model Fields { id UUID } model Child extends Fields { @@id(id) } abstract model ChildIdentity { value String }").unwrap_err();
    assert!(
        error.contains("generated identifier ChildIdentity"),
        "{error}"
    );
}

#[test]
fn bootstrap_flag_does_not_change_normal_record_validation() {
    let c =
        compile("model Initial { id UUID @@id(id) @@bootstrap } model Later { id UUID @@id(id) }")
            .unwrap();
    let schema = axton_core::Schema::from_value(c["schema"].clone()).unwrap();
    assert!(!schema.models[1].bootstrap);
    assert!(
        schema
            .normalize_state(
                "Later",
                &json!({"id":"123e4567-e89b-42d3-a456-426614174000"})
            )
            .is_ok()
    );
}

#[test]
fn unused_abstract_declarations_still_validate_field_annotations() {
    for (field, expected) in [
        (
            "owner Parent @reference(typo: [missing])",
            "unknown reference argument",
        ),
        (
            "owners Parent[] @reference(via: [id])",
            "reference must be singular",
        ),
        (
            "owner Parent @default(uuid())",
            "@default is unsupported on relation",
        ),
        (
            "uri String @requires(Missing(uri: self))",
            "unknown prerequisite",
        ),
        ("owner Parent @inverse(typo: true)", "inverse relation name"),
        ("id UUID @default(now())", "now()"),
    ] {
        let source =
            format!("model Parent {{ id UUID @@id(id) }} abstract model Unused {{ {field} }}");
        let error = compile(&source).unwrap_err();
        assert!(error.contains(expected), "{error}");
    }
}

#[test]
fn abstract_names_only_collide_with_current_emitted_operation_contracts() {
    for source in [
        "model Entry { id UUID @@id(id) } abstract model AddInput { value String } mutation Add { entry Entry.create }",
        "model Entry { id UUID @@id(id) } abstract model AddArgs { value String } mutation Add { entry Entry.create }",
    ] {
        compile(source).unwrap();
    }
    assert!(compile("model Entry { id UUID @@id(id) } abstract model SearchInput { value String } query Search() { count Int }").is_err());
}

#[test]
fn concrete_fields_may_complete_inherited_relation_annotations() {
    let c=compile("model Parent { id UUID @@id(id) } abstract model Fields { id UUID parent Parent @reference(via: [parentId]) } model Child extends Fields { parentId UUID @@id(id) }").unwrap();
    assert_eq!(
        c["schema"]["models"][1]["relations"][0]["fields"],
        json!(["parentId"])
    );
}

#[test]
fn unused_abstract_relation_may_leave_foreign_keys_to_future_concrete_models() {
    compile("model Parent { id UUID @@id(id) } abstract model Fields { parent Parent @reference(via: [parentId]) }").unwrap();
}

#[test]
fn abstract_names_do_not_shadow_conditional_model_fetch_helpers() {
    let error =
        compile("model Entry { id UUID @@id(id) } abstract model FetchModels { value String }")
            .unwrap_err();
    assert!(error.contains("reserved"), "{error}");
}

#[test]
fn retired_generic_encoder_functions_do_not_reserve_abstract_type_names() {
    for source in [
        "model Entry { id UUID @@id(id) } abstract model add { value String } mutation Add { entry Entry.create }",
        "model Entry { id UUID @@id(id) } abstract model aDD { value String } mutation ADD { entry Entry.create }",
        "model Entry { id UUID @@id(id) } enum add { one } mutation Add { entry Entry.create }",
        "model add { id UUID @@id(id) } mutation Add { entry add.create }",
        "model Entry { id UUID @@id(id) } mutation Add { entry Entry.create } mutation add { entry Entry.delete }",
    ] {
        let schema = compile(source).unwrap();
        let dart = axton_compiler::dart(&schema);
        assert!(!dart.contains("dynamic add("));
    }
}
