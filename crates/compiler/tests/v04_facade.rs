use axton_compiler::compile;

const SCHEMA: &str = "model Entry { id String title String @@id(id) } mutation Publish(entry Entry.create, call String) { entry Entry } query Find(id String) { entry Entry? }";

#[test]
fn typescript_named_mutation_uses_one_input_or_callback_scope() {
    let schema = compile(SCHEMA).unwrap();
    let output = axton_compiler::typescript(&schema);
    assert!(
        output.contains(
            "input:PublishInput | ((tx:CompanionContext) => PublishInput | Promise<PublishInput>)"
        ),
        "typed callback missing"
    );
    assert!(!output.contains(" call: {"), "direct mutation lane remains");
    assert!(
        !output.contains(" enqueue: {"),
        "durable query lane remains"
    );
    assert!(
        !output.contains("export type PublishOptions"),
        "Mutation options remain"
    );
    assert!(output.contains("export type FindOptions = QueryOptions;"));
}

#[test]
fn dart_named_mutation_preserves_call_field_and_typed_callback() {
    let schema = compile(SCHEMA).unwrap();
    let output = axton_compiler::dart(&schema);
    assert!(output.contains("class PublishInput"), "typed input missing");
    assert!(
        output.contains("final String call;"),
        "business call field lost"
    );
    assert!(
        output.contains("call(PublishInput input)"),
        "typed input invocation missing"
    );
    assert!(
        output.contains("withTransaction(FutureOr<PublishInput> Function(CompanionContext tx)"),
        "typed callback invocation missing"
    );
    assert!(!output.contains("class DirectMutations"));
    assert!(!output.contains("class QueuedQueries"));
}

#[test]
fn retired_load_declaration_is_rejected() {
    let error =
        compile("model Entry { id String @@id(id) } load LoadEntries() { entries Entry[] }")
            .unwrap_err();
    assert!(error.contains("Load declarations were removed"), "{error}");
}

#[test]
fn backend_tracks_current_or_explicit_streams_with_typed_preparation() {
    let output = axton_compiler::backend_typescript(&compile(SCHEMA).unwrap(), "@axtonjs/server");
    assert!(output.contains("readonly stream: Stream;"));
    assert!(output.contains("streams(names: readonly string[]): Stream;"));
    assert!(output.contains("readonly stream: LoadStream;"));
    assert!(
        output.contains("export interface LoaderHooks<Tx>"),
        "typed preparation missing"
    );
    assert!(
        output.contains("bootstrap?: (call: {ctx: QueryContext<Tx>})"),
        "typed Bootstrap missing"
    );
}

#[test]
fn dart_query_business_client_does_not_shadow_runtime_owner() {
    let descriptor = axton_compiler::compile("query Find(client String) { value String }").unwrap();
    let dart = axton_compiler::dart(&descriptor);
    assert!(
        dart.contains("this.client.invokeQuery<FindOutput>"),
        "{dart}"
    );
    assert!(!dart.contains("invalidateQuery"), "{dart}");
    assert!(dart.contains("required String client"), "{dart}");
}

#[test]
fn dart_retained_slot_helpers_do_not_emit_unused_private_aliases() {
    let mut descriptor = compile("model Entry { id String text String @@id(id) }").unwrap();
    descriptor["mutations"] = serde_json::json!([{ "name": "Rename", "version": 1, "slots": [{ "name": "entry", "model": "Entry", "operation": "update", "cardinality": "single", "allowedPatchFields": ["text"] }] }]);
    let dart = axton_compiler::dart(&descriptor);
    assert!(
        dart.contains("Map<String,dynamic> rename("),
        "slot encoder missing"
    );
    assert!(
        !dart.contains("final _rename = rename;"),
        "retired private facade alias remains"
    );
}

#[test]
fn generated_backend_requires_protocol5_without_protocol4_identity() {
    let output = axton_compiler::backend_typescript(&compile(SCHEMA).unwrap(), "@axtonjs/server");
    assert!(
        output.contains("protocol5: NonNullable<BackendOptions<Tx>[\"protocol5\"]>"),
        "new backend lacks required protocol5 authority configuration"
    );
    assert!(
        !output.contains("protocol4: NonNullable<BackendOptions<Tx>[\"protocol4\"]>"),
        "new generated backend requires retired public identity/config"
    );
}
