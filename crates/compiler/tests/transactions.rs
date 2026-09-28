//! Transactional Mutation enqueue: the generated application transaction
//! queues typed Mutations, a Mutation's `local` callback sees Models only, and
//! the onStore transaction keeps its local-only surface.
use axton_compiler::compile;

const SCHEMA: &str = "model Composition { id String title String @@id(id) } model Entry { id String title String at DateTime @@id(id) } mutation PublishEntry(entry Entry.create, source String) { published Entry } mutation Rename(id String, title String) mutation Ping() query FindEntries(title String) { entries Entry[] }";

fn line<'a>(text: &'a str, prefix: &str) -> &'a str {
    text.lines()
        .find(|line| line.starts_with(prefix))
        .unwrap_or_else(|| panic!("no line starting with {prefix}: {text}"))
}

/// The lines of one emitted block, from its opening line to the first line
/// that closes it.
fn block(text: &str, prefix: &str, end: &str) -> String {
    let start = text
        .find(&format!("\n{prefix}"))
        .unwrap_or_else(|| panic!("no block {prefix}: {text}"))
        + 1;
    let rest = &text[start..];
    let stop = rest
        .find(end)
        .unwrap_or_else(|| panic!("unclosed {prefix}"))
        + end.len();
    rest[..stop].to_string()
}

#[test]
fn typescript_application_transactions_queue_typed_mutations() {
    let v = compile(SCHEMA).unwrap();
    let ts = axton_compiler::typescript(&v);
    for expected in [
        "export type SubmitMutationOptions = CallOptions & { local?: (port:WritePort) => Promise<void> };",
        "export interface SubmitMutationPort { submitMutation<T>(name:string,version:number,args:object,decode:(value:unknown)=>T,options?:SubmitMutationOptions):Promise<Call<T>>; }",
        "export class CompanionContext { readonly models:TxModels; constructor(port:WritePort) { this.models=txModels(port); } }",
        "export type CompanionOptions = { local?: (local:CompanionContext) => Promise<void> };",
        "export class ApplicationTransaction extends GeneratedTransaction { readonly mutations:ReturnType<typeof makeTransactionMutations>; constructor(transaction:WritePort & SubmitMutationPort) { super(transaction); this.mutations=makeTransactionMutations(transaction); } }",
    ] {
        assert!(ts.contains(expected), "missing {expected}: {ts}");
    }
    let facade = block(&ts, "export function makeTransactionMutations(", "\n}; }\n");
    for expected in [
        " publishEntry: (args:PublishEntryInput, options?:PublishEntryOptions & CompanionOptions):Promise<Call<PublishEntryOutput>> => port.submitMutation('PublishEntry',1,encodePublishEntryInput(args),decodePublishEntryOutput,submit(options)),",
        " rename: (args:RenameInput, options?:RenameOptions & CompanionOptions):Promise<Call<RenameOutput>> => port.submitMutation('Rename',1,encodeRenameInput(args),decodeRenameOutput,submit(options)),",
        " ping: (args:PingInput, options?:PingOptions & CompanionOptions):Promise<Call<PingOutput>> => port.submitMutation('Ping',1,encodePingInput(args),decodePingOutput,submit(options)),",
        // The raw callback receives the restricted port and is wrapped once.
        "local:(companion:WritePort)=>local(new CompanionContext(companion))",
    ] {
        assert!(facade.contains(expected), "missing {expected}: {facade}");
    }
    // Only queued Mutations: no Query, no direct `call` route.
    assert!(!facade.contains("findEntries"), "{facade}");
    assert!(!facade.contains("call:"), "{facade}");
    assert!(!facade.contains("invokeAction"), "{facade}");
    // The companion context and the onStore transaction carry no Mutations.
    let companion = line(&ts, "export class CompanionContext ");
    assert!(!companion.contains("mutations"), "{companion}");
    assert!(!companion.contains("channels"), "{companion}");
    let store = line(&ts, "export class GeneratedTransaction ");
    assert!(!store.contains("mutations"), "{store}");
    assert!(
        ts.contains("export type StoreHandler<Identity, Model> = (tx:GeneratedTransaction,"),
        "{ts}"
    );
    // `local` belongs only to the transaction facade.
    let standalone = block(&ts, "export function makeMutations(", "\n}; }\n");
    assert!(!standalone.contains("local"), "{standalone}");
    assert!(!standalone.contains("CompanionOptions"), "{standalone}");
    let queries = block(&ts, "export function makeQueries(", "\n}; }\n");
    assert!(!queries.contains("CompanionOptions"), "{queries}");
    assert!(!line(&ts, "export type PublishEntryOptions").contains("local"));

    let client = axton_compiler::client_typescript(&v, "@axton/client");
    assert!(
        client.contains(" transaction<T>(body: (tx: ApplicationTransaction) => Promise<T>): Promise<T> { return this.client.transaction((tx) => body(new ApplicationTransaction(tx))); }"),
        "{client}"
    );
    assert!(
        line(&client, "import { schema, liveModels,").contains(" ApplicationTransaction,"),
        "{client}"
    );
    // onStore hooks still wrap the raw transaction in the local-only facade.
    assert!(
        client.contains("compositionHook(new GeneratedTransaction(tx),"),
        "{client}"
    );
}

#[test]
fn dart_application_transactions_queue_typed_mutations() {
    let v = compile(SCHEMA).unwrap();
    let dart = axton_compiler::dart(&v);
    for expected in [
        "class CompanionContext { final TxModels models; CompanionContext(WritePort port) : models = TxModels(port); }",
        "class ApplicationTransaction extends GeneratedTransaction { late final TransactionMutations mutations = TransactionMutations(transaction); ApplicationTransaction(super.transaction); }",
        " Future<T> transaction<T>(Future<T> Function(ApplicationTransaction tx) body) => client.transaction((tx) => body(ApplicationTransaction(tx)));",
        "  if (compositionHook != null) rawHooks['Composition'] = (tx, changes) => compositionHook(GeneratedTransaction(tx),",
    ] {
        assert!(dart.contains(expected), "missing {expected}: {dart}");
    }
    let facade = block(&dart, "class TransactionMutations {", "\n}\n");
    for expected in [
        " final SubmitMutationPort _port; TransactionMutations(this._port);",
        " Future<Call<PublishEntryOutput>> publishEntry({required EntryCreateInput entry, required String source, PublishEntryStore? store, Future<void> Function(CompanionContext local)? local}) => _port.submitMutation<PublishEntryOutput>('PublishEntry', 1, {'entry': _dartActionEncode(entry), 'source': _dartActionEncode(source)}, (value) { final row = (value as Map).cast<String,dynamic>(); return PublishEntryOutput(published: Entry.fromRecord((row['published'] as Map).cast<String,dynamic>())); }, store: store, local: local == null ? null : (port) => local(CompanionContext(port)));",
        " Future<Call<PingOutput>> ping({PingStore? store, Future<void> Function(CompanionContext local)? local}) => _port.submitMutation<PingOutput>('Ping', 1, {}, (_) {}, store: store, local: local == null ? null : (port) => local(CompanionContext(port)));",
    ] {
        assert!(facade.contains(expected), "missing {expected}: {facade}");
    }
    assert!(!facade.contains("findEntries"), "{facade}");
    assert!(!facade.contains(" call"), "{facade}");
    let store = line(&dart, "class GeneratedTransaction ");
    assert!(!store.contains("mutations"), "{store}");
    assert!(
        dart.contains(
            "typedef StoreHandler<I, M> = FutureOr<void> Function(GeneratedTransaction tx,"
        ),
        "{dart}"
    );
    let companion = line(&dart, "class CompanionContext ");
    assert!(!companion.contains("mutations"), "{companion}");
    assert!(!companion.contains("channels"), "{companion}");
    // The client-level routes keep no `local` parameter.
    for class in [
        "class Mutations {",
        "class DirectMutations {",
        "class Queries {",
    ] {
        let routes = block(&dart, class, "\n}\n");
        assert!(!routes.contains("CompanionContext"), "{routes}");
    }
}

/// Business inputs are Dart named parameters, so the callback parameter
/// steps aside exactly as the store selector does.
#[test]
fn dart_local_parameter_never_collides_with_business_inputs() {
    let v = compile("mutation Mark(local Boolean, store String, callLocal Int)").unwrap();
    let dart = axton_compiler::dart(&v);
    let facade = block(&dart, "class TransactionMutations {", "\n}\n");
    assert!(
        facade.contains(" Future<Call<MarkOutput>> mark({required bool local, required String store, required int callLocal, MarkStore? outputStore, Future<void> Function(CompanionContext local)? callLocal$}) => _port.submitMutation<MarkOutput>('Mark', 1, {'local': _dartActionEncode(local), 'store': _dartActionEncode(store), 'callLocal': _dartActionEncode(callLocal)}, (_) {}, store: outputStore, local: callLocal$ == null ? null : (port) => callLocal$(CompanionContext(port)));"),
        "{facade}"
    );
}

/// A Mutation reclassified as a Query in its latest version is not queued in
/// a transaction; its retained Mutation version is history only.
#[test]
fn only_current_mutations_join_the_transaction_facade() {
    let mut v = compile(
        "model Todo { id String @@id(id) } query GetTodos() { todos Todo[] } mutation Ping()",
    )
    .unwrap();
    let mut old = v["actions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["name"] == "GetTodos")
        .unwrap()
        .clone();
    old["version"] = serde_json::json!(1);
    old["kind"] = serde_json::json!("mutation");
    old["input"] = serde_json::json!({"models":[],"enums":[]});
    old["outputEnums"] = serde_json::json!([]);
    let mut current = v["actions"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["name"] == "GetTodos")
        .unwrap()
        .clone();
    current["version"] = serde_json::json!(2);
    v["actions"] = serde_json::json!([
        old,
        current,
        v["actions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a["name"] == "Ping")
            .unwrap()
    ]);
    let ts = axton_compiler::typescript(&v);
    let facade = block(&ts, "export function makeTransactionMutations(", "\n}; }\n");
    assert!(facade.contains(" ping:"), "{facade}");
    assert!(!facade.contains("getTodos"), "{facade}");
    let dart = axton_compiler::dart(&v);
    let facade = block(&dart, "class TransactionMutations {", "\n}\n");
    assert!(facade.contains(" ping("), "{facade}");
    assert!(!facade.contains("getTodos"), "{facade}");
}

/// Schemas without a current Mutation keep the transaction they had: no
/// Mutation namespace, companion context or submission port.
#[test]
fn schemas_without_mutations_keep_the_local_transaction() {
    for source in [
        "model Entry { id String @@id(id) }",
        "query Ping() { value String }",
        "model Entry { id String @@id(id) } query Find(id String) { entry Entry? }",
        "model Entry { id String title String @@id(id) } mutation Edit { entry Entry.update<title> }",
    ] {
        let v = compile(source).unwrap();
        let ts = axton_compiler::typescript(&v);
        let client = axton_compiler::client_typescript(&v, "@axton/client");
        let dart = axton_compiler::dart(&v);
        for text in [&ts, &client, &dart] {
            for absent in [
                "ApplicationTransaction",
                "CompanionContext",
                "CompanionOptions",
                "SubmitMutation",
                "submitMutation",
                "TransactionMutations",
            ] {
                assert!(!text.contains(absent), "{source}: {absent} in {text}");
            }
        }
        assert!(
            client.contains(" transaction<T>(body: (tx: GeneratedTransaction) => Promise<T>): Promise<T> { return this.client.transaction((tx) => body(new GeneratedTransaction(tx))); }"),
            "{client}"
        );
        assert!(
            dart.contains(" Future<T> transaction<T>(Future<T> Function(GeneratedTransaction tx) body) => client.transaction((tx) => body(GeneratedTransaction(tx)));"),
            "{dart}"
        );
    }
}

/// The helper names are reserved only where they are emitted: beside a
/// current Mutation.
#[test]
fn transaction_mutation_helper_names_are_reserved_beside_mutations() {
    for name in [
        "ApplicationTransaction",
        "CompanionContext",
        "CompanionOptions",
        "SubmitMutationOptions",
        "SubmitMutationPort",
        "TransactionMutations",
    ] {
        let error = compile(&format!(
            "model {name} {{ id String @@id(id) }}\nmutation Ping()"
        ))
        .unwrap_err();
        assert!(
            error.contains(&format!("generated identifier {name}")),
            "{name}: {error}"
        );
        assert!(error.starts_with("1:"), "{name}: {error}");
        let error = compile(&format!("enum {name} {{ a b }}\nmutation Ping()")).unwrap_err();
        assert!(
            error.contains(&format!("generated identifier {name}")),
            "enum {name}: {error}"
        );
        compile(&format!(
            "model {name} {{ id String @@id(id) }} query Ping() {{ value String }}"
        ))
        .unwrap_or_else(|e| panic!("{name} is free without Mutations: {e}"));
        compile(&format!("model {name} {{ id String @@id(id) }}"))
            .unwrap_or_else(|e| panic!("{name} is free in a model-only schema: {e}"));
    }
}
