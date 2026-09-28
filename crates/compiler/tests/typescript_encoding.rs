use axton_compiler::compile;

/// #182: a list of `update` operands is encoded as one object per element.
/// An arrow whose body is a bare `{…}` is a block, not an object, so the
/// object literal must be parenthesized for the generated TypeScript to parse.
#[test]
fn a_list_of_update_operands_encodes_each_element_as_an_object() {
    let v = compile(
        "model T { id String name String @@id(id) }\n\
         mutation Rename(items T.update<name>[])\n",
    )
    .unwrap();
    let ts = axton_compiler::typescript(&v);
    assert!(
        ts.contains("=> ({...encodeTIdentity(") && ts.contains("...encodeTPatch("),
        "missing parenthesized update encoder: {ts}"
    );
    assert!(
        !ts.contains("=> {...encodeTIdentity("),
        "block-bodied arrow: {ts}"
    );
}
