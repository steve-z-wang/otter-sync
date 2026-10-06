//! Load is a retired public schema surface in 0.4.
use axton_compiler::{compile, parse, validate};
#[test]
fn all_load_shapes_fail_with_the_retirement_contract() {
    for load in [
        "load All() { rows Entry[] }",
        "load Page(id String) { row Entry? }",
    ] {
        let source = format!("model Entry {{ id String @@id(id) }} {load}");
        let error = compile(&source).unwrap_err();
        assert!(
            error.contains("Load declarations were removed"),
            "{source}: {error}"
        );
        assert!(
            validate(&parse(&source).unwrap())
                .unwrap_err()
                .contains("Load declarations were removed")
        );
    }
}
#[test]
fn generated_clients_and_backends_have_no_load_manager() {
    let v = compile("model Entry { id String @@id(id) } query Find() { rows Entry[] }").unwrap();
    for output in [
        axton_compiler::typescript(&v),
        axton_compiler::client_typescript(&v, "runtime"),
        axton_compiler::dart(&v),
    ] {
        for absent in [
            "makeLoads",
            "class Loads",
            "LoadHandlerCall",
            "readonly loads",
            "late final Loads",
        ] {
            assert!(!output.contains(absent), "{absent}");
        }
    }
    assert!(!axton_compiler::backend_typescript(&v, "runtime").contains("export interface Loads"));
}
