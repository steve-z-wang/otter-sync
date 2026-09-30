//! Query once result snapshots (#158): canonical keys, additive storage,
//! generation fencing and the Rust coordinator, against real SQLite.
use axton_client::query_cache::QueryCacheKey;
use axton_client::*;
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};

const PROJECT: &str = "0b4e7a0e-5d6b-4f0e-9b1c-3f7c2a1d9e10";

fn fields() -> Value {
    json!([{"name":"id","type":{"kind":"scalar","name":"string"},"nullable":false},
           {"name":"title","type":{"kind":"scalar","name":"string"},"nullable":false}])
}
fn handler() -> Value {
    json!({"kind":"identity","model":"Todo","fields":[{"name":"id","type":{"kind":"scalar","name":"string"}}]})
}
/// `GetTodos` returns Models plus scalar/list/date metadata; `Ping` is a
/// parameterless void Query; `Rename` is a Mutation.
fn schema_value() -> Value {
    json!({"enums":[],
        "models":[{"name":"Todo","version":1,"identity":["id"],"fields":fields()}],
        "resultModels":[{"name":"Todo","version":1,"identity":["id"],"fields":fields(),"enums":[]}],
        "actions":[
            {"name":"GetTodos","version":1,"kind":"query","inputs":[
                {"kind":"value","name":"projectId","type":{"kind":"scalar","name":"uuid"},"nullable":false},
                {"kind":"value","name":"since","type":{"kind":"scalar","name":"dateTime"},"nullable":true},
                {"kind":"value","name":"tags","type":{"kind":"scalar","name":"string"},"nullable":false,"list":true}],
             "outputs":[
                {"name":"todos","kind":"model","model":"Todo","modelReadVersion":1,"cardinality":"list","source":"handlerIdentity","handlerType":handler()},
                {"name":"total","kind":"value","type":{"kind":"scalar","name":"int"},"cardinality":"single","source":"handlerValue"},
                {"name":"asOf","kind":"value","type":{"kind":"scalar","name":"dateTime"},"cardinality":"single","source":"handlerValue"},
                {"name":"next","kind":"value","type":{"kind":"scalar","name":"string"},"cardinality":"optional","source":"handlerValue"}]},
            {"name":"Ping","version":1,"kind":"query","inputs":[],"outputs":[]},
            {"name":"Rename","version":1,"inputs":[{"kind":"model","name":"todo","model":"Todo","operation":"update","cardinality":"single"}],"outputs":[]}]
    })
}
fn schema() -> Schema {
    Schema::from_value(schema_value()).unwrap()
}
/// The same client contract plus one more Query: compatible for the Model
/// tables, a different result-cache contract.
fn extended_schema() -> Schema {
    let mut value = schema_value();
    value["actions"].as_array_mut().unwrap().push(
        json!({"name":"Count","version":1,"kind":"query","inputs":[],"outputs":[
            {"name":"n","kind":"value","type":{"kind":"scalar","name":"int"},"cardinality":"single","source":"handlerValue"}]}),
    );
    Schema::from_value(value).unwrap()
}
fn open(path: &std::path::Path) -> Client<SqliteStore> {
    Client::open(SqliteStore::open(path).unwrap(), schema()).unwrap()
}
fn args() -> Value {
    json!({"projectId":PROJECT,"since":"2026-01-02T03:04:05Z","tags":["a","b"]})
}
fn key(client: &Client<SqliteStore>, args: Value, store: ActionStore) -> QueryCacheKey {
    client
        .query_cache_key("GetTodos", 1, &args, &store)
        .unwrap()
}
fn result() -> Value {
    json!({"todos":[{"id":"a","title":"A"}],"total":1,"asOf":"2026-01-02T03:04:05.000Z","next":null})
}
fn seed(client: &mut Client<SqliteStore>, id: &str) {
    client
        .transaction(|tx| {
            tx.direct(Operation {
                model: "Todo".into(),
                op: OperationKind::Create,
                identity: json!({ "id": id }),
                values: Some(json!({"title":"local"})),
            })
        })
        .unwrap();
}
fn count(client: &mut Client<SqliteStore>) -> u64 {
    client
        .read_sql("SELECT COUNT(*) AS n FROM axton_query_cache", &[])
        .unwrap()[0]["n"]
        .as_u64()
        .unwrap()
}

#[test]
fn equivalent_arguments_and_store_policies_share_one_key() {
    let dir = tempfile::tempdir().unwrap();
    let client = open(&dir.path().join("db"));
    let base = key(&client, args(), ActionStore::All);
    // Key order, UUID case and zone offsets normalize through the Query's
    // own argument validation.
    let reordered = json!({"tags":["a","b"],"since":"2026-01-02T04:04:05+01:00","projectId":PROJECT.to_uppercase()});
    assert_eq!(key(&client, reordered, ActionStore::All), base);
    // Omitted, `true` and an all-true map are the same canonical policy
    // (GetTodos' only eligible output is `todos`).
    assert_eq!(
        key(
            &client,
            args(),
            ActionStore::Outputs([("todos".to_string(), true)].into())
        ),
        base
    );
    assert_eq!(base.name, "GetTodos");
    assert_eq!(base.version, 1);
    assert_eq!(base.store, "true");
    assert_eq!(
        base.args,
        r#"{"projectId":"0b4e7a0e-5d6b-4f0e-9b1c-3f7c2a1d9e10","since":"2026-01-02T03:04:05.000Z","tags":["a","b"]}"#
    );
    assert_eq!(base.contract, client.query_cache_contract());
    assert_eq!(
        base.key.len(),
        64,
        "hex SHA-256 of the canonical components"
    );
}

#[test]
fn different_arguments_store_variants_and_order_get_distinct_keys() {
    let dir = tempfile::tempdir().unwrap();
    let client = open(&dir.path().join("db"));
    let base = key(&client, args(), ActionStore::All).key;
    let mut distinct = vec![base.clone()];
    let mut add = |k: QueryCacheKey| {
        assert!(!distinct.contains(&k.key), "{k:?}");
        distinct.push(k.key);
    };
    let mut reversed = args();
    reversed["tags"] = json!(["b", "a"]);
    add(key(&client, reversed, ActionStore::All));
    let mut null_since = args();
    null_since["since"] = Value::Null;
    add(key(&client, null_since, ActionStore::All));
    let none = key(&client, args(), ActionStore::None);
    assert_eq!(none.store, "false");
    // A map that disables every eligible output is the same policy as false.
    assert_eq!(
        key(
            &client,
            args(),
            ActionStore::Outputs([("todos".to_string(), false)].into()),
        ),
        none
    );
    add(none);
}

#[test]
fn a_selective_store_map_is_its_own_variant() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut value = schema_value();
    let mut extra = value["actions"][0]["outputs"][0].clone();
    extra["name"] = json!("featured");
    extra["cardinality"] = json!("optional");
    value["actions"][0]["outputs"]
        .as_array_mut()
        .unwrap()
        .push(extra);
    let client = Client::open(
        SqliteStore::open(&path).unwrap(),
        Schema::from_value(value).unwrap(),
    )
    .unwrap();
    let k = |store| {
        client
            .query_cache_key("GetTodos", 1, &args(), &store)
            .unwrap()
    };
    // With no eligible output, every store policy stores the same nothing.
    assert_eq!(
        client
            .query_cache_key("Ping", 1, &json!({}), &ActionStore::None)
            .unwrap(),
        client
            .query_cache_key("Ping", 1, &json!({}), &ActionStore::All)
            .unwrap()
    );
    let selective = k(ActionStore::Outputs([("todos".to_string(), false)].into()));
    assert_eq!(selective.store, r#"{"todos":false}"#);
    assert_ne!(selective, k(ActionStore::None));
    assert_ne!(selective, k(ActionStore::All));
    assert_eq!(
        k(ActionStore::Outputs(
            [
                ("todos".to_string(), false),
                ("featured".to_string(), false)
            ]
            .into()
        )),
        k(ActionStore::None)
    );
}

#[test]
fn keys_are_only_for_queries_and_invalid_input_fails_before_the_cache() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    let store = ActionStore::All;
    assert!(
        client
            .query_cache_key("Rename", 1, &json!({"todo":{"id":"a","title":"x"}}), &store)
            .is_err(),
        "a Mutation has no once cache"
    );
    assert!(
        client
            .query_cache_key("Missing", 1, &json!({}), &store)
            .is_err()
    );
    assert!(
        client
            .query_cache_key("GetTodos", 2, &args(), &store)
            .is_err()
    );
    let mut undeclared = args();
    undeclared["extra"] = json!(1);
    assert!(
        client
            .query_cache_key("GetTodos", 1, &undeclared, &store)
            .is_err()
    );
    let mut bad_uuid = args();
    bad_uuid["projectId"] = json!("not-a-uuid");
    assert!(
        client
            .query_cache_key("GetTodos", 1, &bad_uuid, &store)
            .is_err()
    );
    assert!(
        client
            .query_cache_key(
                "GetTodos",
                1,
                &args(),
                &ActionStore::Outputs([("total".to_string(), false)].into())
            )
            .is_err(),
        "store may name only explicit Model outputs"
    );
    assert!(
        client
            .invalidate_query_once("Rename", 1, &json!({"todo":{"id":"a","title":"x"}}))
            .is_err()
    );
    assert_eq!(count(&mut client), 0);
}

#[test]
fn the_contract_is_a_fingerprint_of_the_complete_client_schema() {
    let dir = tempfile::tempdir().unwrap();
    let a = open(&dir.path().join("a"));
    let b = Client::open(
        SqliteStore::open(dir.path().join("b")).unwrap(),
        extended_schema(),
    )
    .unwrap();
    let again = open(&dir.path().join("c"));
    assert_eq!(a.query_cache_contract(), again.query_cache_contract());
    assert_ne!(a.query_cache_contract(), b.query_cache_contract());
    assert_ne!(
        key(&a, args(), ActionStore::All).key,
        key(&b, args(), ActionStore::All).key
    );
}

#[test]
fn an_existing_database_gains_the_cache_table_without_touching_its_queue() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut client = open(&path);
    seed(&mut client, "a");
    client
        .submit_action("Rename", 1, json!({"todo":{"id":"a","title":"x"}}))
        .unwrap();
    let frozen = client.freeze().unwrap().unwrap();
    drop(client);
    // A database written before this table existed.
    SqliteStore::open(&path)
        .unwrap()
        .execute_batch("DROP TABLE axton_query_cache")
        .unwrap();
    let mut client = open(&path);
    assert_eq!(count(&mut client), 0);
    assert_eq!(client.pending_count().unwrap(), 1);
    assert_eq!(client.freeze().unwrap().unwrap(), frozen);
    assert!(!client.schema_state().rebuilt);
}

#[test]
fn successful_empty_and_void_results_persist_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut client = open(&path);
    let todos = key(&client, args(), ActionStore::All);
    let ping = client
        .query_cache_key("Ping", 1, &json!({}), &ActionStore::All)
        .unwrap();
    assert_eq!(client.query_cache_entry(&todos).unwrap(), None);
    let empty = json!({"todos":[],"total":0,"asOf":"2026-01-02T03:04:05.000Z","next":null});
    assert!(client.save_query_result(&todos, None, &empty).unwrap());
    assert!(client.save_query_result(&ping, None, &Value::Null).unwrap());
    drop(client);
    let mut client = open(&path);
    let entry = client.query_cache_entry(&todos).unwrap().unwrap();
    assert_eq!(entry.result, Some(empty));
    // A cached JSON null is a successful void result, not an empty row.
    let entry = client.query_cache_entry(&ping).unwrap().unwrap();
    assert_eq!(entry.result, Some(Value::Null));
    assert_eq!(
        client
            .read_sql("SELECT COUNT(*) AS n FROM Todo", &[])
            .unwrap()[0]["n"],
        0,
        "saving a snapshot never writes Models"
    );
}

#[test]
fn a_result_that_violates_the_output_contract_is_never_saved() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    let todos = key(&client, args(), ActionStore::All);
    assert!(
        client
            .save_query_result(&todos, None, &json!({"todos":[]}))
            .is_err()
    );
    assert_eq!(count(&mut client), 0);
}

#[test]
fn invalidation_leaves_a_fenced_tombstone_for_every_store_variant() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    let all = key(&client, args(), ActionStore::All);
    let none = key(&client, args(), ActionStore::None);
    let mut other_args = args();
    other_args["tags"] = json!([]);
    let other = key(&client, other_args, ActionStore::All);
    for k in [&all, &none, &other] {
        assert!(client.save_query_result(k, None, &result()).unwrap());
    }
    let before = client.query_cache_entry(&all).unwrap().unwrap().generation;
    // Equivalent arguments name the same argument set.
    let equivalent =
        json!({"tags":["a","b"],"since":"2026-01-02T03:04:05.000+00:00","projectId":PROJECT});
    client
        .invalidate_query_once("GetTodos", 1, &equivalent)
        .unwrap();
    for k in [&all, &none] {
        let entry = client.query_cache_entry(k).unwrap().unwrap();
        assert_eq!(entry.result, None, "tombstone");
        assert_ne!(entry.generation, before);
    }
    assert_eq!(
        client.query_cache_entry(&other).unwrap().unwrap().result,
        Some(result()),
        "another argument set is untouched"
    );
    // A save fenced by the old generation cannot repopulate it.
    assert!(
        !client
            .save_query_result(&all, Some(&before), &result())
            .unwrap()
    );
    assert!(!client.save_query_result(&all, None, &result()).unwrap());
    assert_eq!(
        client.query_cache_entry(&all).unwrap().unwrap().result,
        None
    );
    let current = client.query_cache_entry(&all).unwrap().unwrap().generation;
    assert!(
        client
            .save_query_result(&all, Some(&current), &result())
            .unwrap()
    );
    assert_eq!(
        client.query_cache_entry(&all).unwrap().unwrap().generation,
        current,
        "a successful save keeps the generation"
    );
}

#[test]
fn independent_databases_hold_independent_results() {
    let dir = tempfile::tempdir().unwrap();
    let mut first = open(&dir.path().join("first"));
    let mut second = open(&dir.path().join("second"));
    let k = key(&first, args(), ActionStore::All);
    assert_eq!(k, key(&second, args(), ActionStore::All));
    assert!(first.save_query_result(&k, None, &result()).unwrap());
    assert!(first.query_cache_entry(&k).unwrap().is_some());
    assert_eq!(second.query_cache_entry(&k).unwrap(), None);
}

#[test]
fn a_schema_change_prunes_obsolete_partitions_on_open() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut client = open(&path);
    let old = key(&client, args(), ActionStore::All);
    assert!(client.save_query_result(&old, None, &result()).unwrap());
    drop(client);
    // Reopening with the same contract keeps the snapshot.
    let mut client = open(&path);
    assert!(client.query_cache_entry(&old).unwrap().is_some());
    drop(client);
    let mut client = Client::open(SqliteStore::open(&path).unwrap(), extended_schema()).unwrap();
    assert_eq!(count(&mut client), 0, "the old contract's rows are gone");
    let new = key(&client, args(), ActionStore::All);
    assert!(client.save_query_result(&new, None, &result()).unwrap());
    drop(client);
    // Going back is another contract change: nothing is resurrected.
    let mut client = open(&path);
    assert_eq!(count(&mut client), 0);
}

#[test]
fn a_rebuilt_replica_starts_with_an_empty_cache() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let factory = || -> StoreFactory<SqliteStore> { Box::new(|p| SqliteStore::open(p)) };
    let mut client = Client::open_at(&path, schema(), factory(), false).unwrap();
    let k = key(&client, args(), ActionStore::All);
    assert!(client.save_query_result(&k, None, &result()).unwrap());
    drop(client);
    // An incompatible Model change rebuilds beside the old file.
    let mut value = schema_value();
    value["models"][0]["fields"][1]["type"]["name"] = json!("int");
    value["resultModels"][0]["fields"][1]["type"]["name"] = json!("int");
    let mut client =
        Client::open_at(&path, Schema::from_value(value).unwrap(), factory(), false).unwrap();
    assert!(client.schema_state().rebuilt);
    assert_eq!(count(&mut client), 0);
}

// ---- Coordinator: begin / finish / fail / invalidate -------------------

fn once(refresh: bool) -> QueryOnceOptions {
    QueryOnceOptions {
        store: ActionStore::All,
        refresh,
    }
}
fn begin(client: &mut Client<SqliteStore>, refresh: bool) -> QueryOnce {
    client
        .begin_query_once("GetTodos", 1, &args(), &once(refresh))
        .unwrap()
}
/// The Fetch decision's flight and exact prepared request.
fn fetch(decision: QueryOnce) -> (String, DirectActionRequest) {
    match decision {
        QueryOnce::Fetch { flight_id, request } => (flight_id, request),
        other => panic!("expected Fetch, got {other:?}"),
    }
}
fn cached(decision: QueryOnce) -> Value {
    match decision {
        QueryOnce::Cached { result } => result,
        other => panic!("expected Cached, got {other:?}"),
    }
}
fn titled(title: &str) -> Value {
    json!({"todos":[{"id":"a","title":title}],"total":1,"asOf":"2026-01-02T03:04:05.000Z","next":null})
}
/// A successful direct response carrying `result` and `Todo a` at `stamp`.
fn succeeded(request: &DirectActionRequest, title: &str, stamp: u64) -> Vec<u8> {
    json!({"completion":{"callId":request.call.call_id,"outcome":{"status":"succeeded","result":titled(title)}},
           "records":[{"model":"Todo","identity":{"id":"a"},"stamp":stamp,"state":{"title":title}}]})
    .to_string()
    .into_bytes()
}
fn failed(request: &DirectActionRequest) -> Vec<u8> {
    json!({"completion":{"callId":request.call.call_id,"outcome":{"status":"failed","code":"backend.down","execution":"rejected"}},"records":[]})
        .to_string()
        .into_bytes()
}
fn todo_title(client: &mut Client<SqliteStore>) -> Option<Value> {
    client
        .read(&RecordKey {
            model: "Todo".into(),
            identity: json!({"id":"a"}),
        })
        .unwrap()
        .map(|todo| todo["title"].clone())
}
fn saved(client: &mut Client<SqliteStore>) -> Option<Value> {
    let k = key(client, args(), ActionStore::All);
    client.query_cache_entry(&k).unwrap().and_then(|e| e.result)
}

#[test]
fn a_miss_fetches_once_and_later_calls_reuse_the_saved_result() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    let (flight, request) = fetch(begin(&mut client, false));
    assert_eq!(request.call.name, "GetTodos");
    assert_eq!(saved(&mut client), None, "nothing is cached before success");
    let report = client
        .finish_query_once(&flight, &succeeded(&request, "A", 1))
        .unwrap();
    assert_eq!(report.completions.len(), 1);
    assert_eq!(report.completions[0].call_id, request.call.call_id);
    assert_eq!(
        todo_title(&mut client),
        Some(json!("A")),
        "authority applied"
    );
    assert_eq!(cached(begin(&mut client, false)), titled("A"));
    // Equivalent arguments hit the same snapshot.
    let equivalent = json!({"tags":["a","b"],"since":"2026-01-02T04:04:05+01:00","projectId":PROJECT.to_uppercase()});
    assert_eq!(
        cached(
            client
                .begin_query_once("GetTodos", 1, &equivalent, &once(false))
                .unwrap()
        ),
        titled("A")
    );
}

#[test]
fn concurrent_misses_join_one_flight() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    let (flight, request) = fetch(begin(&mut client, false));
    assert_eq!(
        begin(&mut client, false),
        QueryOnce::Join {
            flight_id: flight.clone()
        }
    );
    // A refresh with no saved result joins the same uncached request.
    assert_eq!(
        begin(&mut client, true),
        QueryOnce::Join {
            flight_id: flight.clone()
        }
    );
    client
        .finish_query_once(&flight, &succeeded(&request, "A", 1))
        .unwrap();
    assert_eq!(cached(begin(&mut client, false)), titled("A"));
}

#[test]
fn refreshes_coalesce_while_plain_once_keeps_hitting_the_old_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    let (first, request) = fetch(begin(&mut client, false));
    client
        .finish_query_once(&first, &succeeded(&request, "A", 1))
        .unwrap();
    let (refresh, refresh_request) = fetch(begin(&mut client, true));
    assert_ne!(refresh, first);
    assert_ne!(
        refresh_request.call.call_id, request.call.call_id,
        "a refresh is a fresh direct call"
    );
    assert_eq!(
        begin(&mut client, true),
        QueryOnce::Join {
            flight_id: refresh.clone()
        }
    );
    assert_eq!(cached(begin(&mut client, false)), titled("A"));
    client
        .finish_query_once(&refresh, &succeeded(&refresh_request, "B", 2))
        .unwrap();
    assert_eq!(cached(begin(&mut client, false)), titled("B"));
    assert_eq!(todo_title(&mut client), Some(json!("B")));
}

#[test]
fn a_failed_refresh_keeps_the_previous_result() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    let (first, request) = fetch(begin(&mut client, false));
    client
        .finish_query_once(&first, &succeeded(&request, "A", 1))
        .unwrap();
    // A backend failure outcome.
    let (refresh, refresh_request) = fetch(begin(&mut client, true));
    let report = client
        .finish_query_once(&refresh, &failed(&refresh_request))
        .unwrap();
    assert!(matches!(
        report.completions[0].outcome,
        ActionOutcome::Failed { .. }
    ));
    assert_eq!(cached(begin(&mut client, false)), titled("A"));
    // A transport failure the host reports.
    let (refresh, _) = fetch(begin(&mut client, true));
    assert!(client.fail_query_once(&refresh));
    assert!(!client.fail_query_once(&refresh), "released exactly once");
    assert_eq!(cached(begin(&mut client, false)), titled("A"));
}

#[test]
fn a_failed_miss_caches_nothing_and_a_retry_is_a_fresh_call() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    let (flight, request) = fetch(begin(&mut client, false));
    client
        .finish_query_once(&flight, &failed(&request))
        .unwrap();
    assert_eq!(saved(&mut client), None);
    assert_eq!(count(&mut client), 0);
    let (retry, retry_request) = fetch(begin(&mut client, false));
    assert_ne!(retry, flight);
    assert_ne!(retry_request.call.call_id, request.call.call_id);
    assert!(client.fail_query_once(&retry));
    let (again, _) = fetch(begin(&mut client, false));
    assert_ne!(again, retry);
}

#[test]
fn an_invalid_response_releases_the_flight_and_caches_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    let (flight, request) = fetch(begin(&mut client, false));
    let mut other = request.clone();
    other.call.call_id = "7c9e6679-7425-40de-944b-e07fc1f90ae7".into();
    assert!(
        client
            .finish_query_once(&flight, &succeeded(&other, "A", 1))
            .is_err(),
        "a response for another call"
    );
    assert!(
        client
            .finish_query_once(&flight, &succeeded(&request, "A", 1))
            .is_err(),
        "the flight is gone after its terminal completion"
    );
    assert!(client.finish_query_once("unknown", b"{}").is_err());
    assert_eq!(saved(&mut client), None);
    assert_eq!(todo_title(&mut client), None);
    fetch(begin(&mut client, false));
}

#[test]
fn default_direct_calls_never_join_or_populate_the_cache() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    let (_flight, request) = fetch(begin(&mut client, false));
    let plain = client.prepare_action("GetTodos", 1, args()).unwrap();
    assert_ne!(plain.call.call_id, request.call.call_id);
    client
        .apply_action_response(&plain, &succeeded(&plain, "P", 1))
        .unwrap();
    assert_eq!(count(&mut client), 0);
    assert_eq!(todo_title(&mut client), Some(json!("P")));
}

#[test]
fn once_is_rejected_for_mutations_invalid_input_and_inside_transactions() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    seed(&mut client, "a");
    assert!(
        client
            .begin_query_once(
                "Rename",
                1,
                &json!({"todo":{"id":"a","title":"x"}}),
                &once(false)
            )
            .is_err()
    );
    assert!(
        client
            .begin_query_once("GetTodos", 1, &json!({}), &once(false))
            .is_err()
    );
    assert_eq!(client.pending_count().unwrap(), 0);
    let (flight, request) = fetch(begin(&mut client, false));
    client
        .finish_query_once(&flight, &succeeded(&request, "A", 1))
        .unwrap();
    client.begin_session().unwrap();
    assert!(
        client
            .begin_query_once("GetTodos", 1, &args(), &once(false))
            .is_err(),
        "even a hit is refused inside an application transaction"
    );
    assert!(
        client
            .invalidate_query_once("GetTodos", 1, &args())
            .is_err()
    );
    client.rollback_session().unwrap();
    assert_eq!(cached(begin(&mut client, false)), titled("A"));
}

#[test]
fn a_hit_never_reapplies_the_snapshot_to_models_or_wakes_watchers() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    let (flight, request) = fetch(begin(&mut client, false));
    client
        .finish_query_once(&flight, &succeeded(&request, "A", 1))
        .unwrap();
    // Newer authority, then a local edit, change the Model.
    let plain = client.prepare_action("GetTodos", 1, args()).unwrap();
    client
        .apply_action_response(&plain, &succeeded(&plain, "B", 5))
        .unwrap();
    client
        .transaction(|tx| {
            tx.direct(Operation {
                model: "Todo".into(),
                op: OperationKind::Update,
                identity: json!({"id":"a"}),
                values: Some(json!({"title":"C"})),
            })
        })
        .unwrap();
    let watcher = client.watch(["Todo".to_string()].into());
    let generation = client.generation();
    assert_eq!(cached(begin(&mut client, false)), titled("A"));
    assert_eq!(todo_title(&mut client), Some(json!("C")));
    assert_eq!(client.generation(), generation, "a hit commits nothing");
    assert!(watcher.try_recv().is_err());
    assert_eq!(
        client
            .record_stamp(&RecordKey {
                model: "Todo".into(),
                identity: json!({"id":"a"}),
            })
            .unwrap(),
        5
    );
}

#[test]
fn store_variants_do_not_satisfy_each_other() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    let no_store = QueryOnceOptions {
        store: ActionStore::None,
        refresh: false,
    };
    let (flight, request) = fetch(
        client
            .begin_query_once("GetTodos", 1, &args(), &no_store)
            .unwrap(),
    );
    assert_eq!(request.call.store, ActionStore::None);
    let response = json!({"completion":{"callId":request.call.call_id,"outcome":{"status":"succeeded","result":titled("A")}},"records":[]});
    client
        .finish_query_once(&flight, response.to_string().as_bytes())
        .unwrap();
    assert_eq!(todo_title(&mut client), None, "store:false writes no Model");
    // The complete snapshot is still persisted for the store:false variant.
    assert_eq!(
        cached(
            client
                .begin_query_once("GetTodos", 1, &args(), &no_store)
                .unwrap()
        ),
        titled("A")
    );
    // A call that asks for Models is a different key: it must fetch.
    fetch(begin(&mut client, false));
}

#[test]
fn invalidation_fences_an_older_miss_without_disturbing_the_newer_flight() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    let (old, old_request) = fetch(begin(&mut client, false));
    client
        .invalidate_query_once("GetTodos", 1, &args())
        .unwrap();
    let (new, new_request) = fetch(begin(&mut client, false));
    assert_ne!(new, old, "a new generation never joins the older request");
    // The older request still completes for its callers and applies its
    // stamped authority, but cannot repopulate the invalidated key.
    let report = client
        .finish_query_once(&old, &succeeded(&old_request, "OLD", 1))
        .unwrap();
    assert_eq!(report.completions[0].call_id, old_request.call.call_id);
    assert_eq!(todo_title(&mut client), Some(json!("OLD")));
    assert_eq!(saved(&mut client), None);
    // The newer flight is still joinable and saves.
    assert_eq!(
        begin(&mut client, false),
        QueryOnce::Join {
            flight_id: new.clone()
        }
    );
    client
        .finish_query_once(&new, &succeeded(&new_request, "NEW", 2))
        .unwrap();
    assert_eq!(cached(begin(&mut client, false)), titled("NEW"));
}

#[test]
fn an_older_failure_cannot_release_a_newer_flight() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    let (old, _) = fetch(begin(&mut client, false));
    client
        .invalidate_query_once("GetTodos", 1, &args())
        .unwrap();
    let (new, _) = fetch(begin(&mut client, false));
    assert!(client.fail_query_once(&old));
    assert_eq!(
        begin(&mut client, false),
        QueryOnce::Join {
            flight_id: new.clone()
        }
    );
}

#[test]
fn invalidation_during_a_refresh_leaves_the_key_empty() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    let (first, request) = fetch(begin(&mut client, false));
    client
        .finish_query_once(&first, &succeeded(&request, "A", 1))
        .unwrap();
    let (refresh, refresh_request) = fetch(begin(&mut client, true));
    client
        .invalidate_query_once("GetTodos", 1, &args())
        .unwrap();
    client
        .finish_query_once(&refresh, &succeeded(&refresh_request, "B", 2))
        .unwrap();
    assert_eq!(saved(&mut client), None);
    fetch(begin(&mut client, false));
}

#[test]
fn a_failed_commit_leaves_neither_snapshot_nor_authority() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut client = open(&path);
    let (flight, request) = fetch(begin(&mut client, false));
    // Real SQLite refuses the cache write inside the same transaction as
    // the authority it would commit with.
    SqliteStore::open(&path)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER refuse_cache BEFORE INSERT ON axton_query_cache BEGIN SELECT RAISE(ABORT, 'cache refused'); END",
        )
        .unwrap();
    let error = client
        .finish_query_once(&flight, &succeeded(&request, "A", 1))
        .unwrap_err();
    assert!(error.to_string().contains("cache refused"), "{error}");
    assert_eq!(todo_title(&mut client), None, "authority rolled back");
    assert_eq!(saved(&mut client), None);
    // The flight is released; a retry is a new request.
    let (retry, retry_request) = fetch(begin(&mut client, false));
    assert_ne!(retry, flight);
    SqliteStore::open(&path)
        .unwrap()
        .execute_batch("DROP TRIGGER refuse_cache")
        .unwrap();
    client
        .finish_query_once(&retry, &succeeded(&retry_request, "A", 1))
        .unwrap();
    assert_eq!(todo_title(&mut client), Some(json!("A")));
    assert_eq!(cached(begin(&mut client, false)), titled("A"));
}

#[test]
fn a_plain_result_with_no_records_is_saved_in_its_own_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    let decision = client
        .begin_query_once("Ping", 1, &json!({}), &once(false))
        .unwrap();
    let (flight, request) = fetch(decision);
    let generation = client.generation();
    let response = json!({"completion":{"callId":request.call.call_id,"outcome":{"status":"succeeded","result":null}},"records":[]});
    client
        .finish_query_once(&flight, response.to_string().as_bytes())
        .unwrap();
    assert!(client.generation() > generation, "committed locally");
    assert_eq!(
        cached(
            client
                .begin_query_once("Ping", 1, &json!({}), &once(false))
                .unwrap()
        ),
        Value::Null
    );
}

#[test]
fn a_malformed_snapshot_is_invalidated_and_missed_but_read_errors_surface() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut client = open(&path);
    let (flight, request) = fetch(begin(&mut client, false));
    client
        .finish_query_once(&flight, &succeeded(&request, "A", 1))
        .unwrap();
    let before = {
        let k = key(&client, args(), ActionStore::All);
        client.query_cache_entry(&k).unwrap().unwrap().generation
    };
    SqliteStore::open(&path)
        .unwrap()
        .execute_batch("UPDATE axton_query_cache SET result = '{\"todos\":7}'")
        .unwrap();
    fetch(begin(&mut client, false));
    let k = key(&client, args(), ActionStore::All);
    let entry = client.query_cache_entry(&k).unwrap().unwrap();
    assert_eq!(entry.result, None);
    assert_ne!(entry.generation, before);
    drop(client);
    let mut client = open(&path);
    SqliteStore::open(&path)
        .unwrap()
        .execute_batch("DROP TABLE axton_query_cache")
        .unwrap();
    assert!(
        client
            .begin_query_once("GetTodos", 1, &args(), &once(false))
            .is_err(),
        "a database failure is an error, not a miss"
    );
}

#[test]
fn a_snapshot_that_is_not_json_is_invalidated_and_missed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut client = open(&path);
    let (flight, request) = fetch(begin(&mut client, false));
    client
        .finish_query_once(&flight, &succeeded(&request, "A", 1))
        .unwrap();
    SqliteStore::open(&path)
        .unwrap()
        .execute_batch("UPDATE axton_query_cache SET result = 'not json {'")
        .unwrap();
    let (retry, retry_request) = fetch(begin(&mut client, false));
    let k = key(&client, args(), ActionStore::All);
    assert_eq!(client.query_cache_entry(&k).unwrap().unwrap().result, None);
    client
        .finish_query_once(&retry, &succeeded(&retry_request, "B", 2))
        .unwrap();
    assert_eq!(cached(begin(&mut client, false)), titled("B"));
}

#[test]
fn saved_results_survive_reopen_but_active_flights_do_not() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut client = open(&path);
    let (flight, request) = fetch(begin(&mut client, false));
    client
        .finish_query_once(&flight, &succeeded(&request, "A", 1))
        .unwrap();
    let mut other = args();
    other["tags"] = json!([]);
    let pending = client
        .begin_query_once("GetTodos", 1, &other, &once(false))
        .unwrap();
    let (pending, _) = fetch(pending);
    drop(client);
    let mut client = open(&path);
    assert_eq!(cached(begin(&mut client, false)), titled("A"));
    // The unfinished request was not durable work: a new request is needed.
    let (fresh, _) = fetch(
        client
            .begin_query_once("GetTodos", 1, &other, &once(false))
            .unwrap(),
    );
    assert_ne!(fresh, pending);
    assert!(
        client.finish_query_once(&pending, b"{}").is_err(),
        "a flight of the closed runtime matches nothing"
    );
    assert_eq!(client.pending_count().unwrap(), 0);
}

#[test]
fn delayed_query_once_stores_result_without_resurrecting_models_and_cache_hit_stays_inert() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open(&path);
    let record = AuthorityRecord {
        model: "Todo".into(),
        identity: json!({"id":"a"}),
        stamp: 7,
        state: json!({"title":"base"}),
        error: None,
    };
    let todo = RecordKey {
        model: "Todo".into(),
        identity: json!({"id":"a"}),
    };
    let sub = c.ensure_subscription("a").unwrap();
    c.initialize_subscriptions(
        &[("a".into(), sub.subscription_id)].into(),
        &[("a".into(), 0)].into(),
    )
    .unwrap();
    c.apply_channel_page(ChannelPullPage {
        cursors: [(
            "a".into(),
            CursorRange {
                from: 0,
                to: 1,
                head: 1,
            },
        )]
        .into(),
        changes: vec![ChannelChange::Upsert {
            channel: "a".into(),
            cursor: 1,
            record,
        }],
    })
    .unwrap();
    let (flight, request) = fetch(begin(&mut c, false));
    c.apply_channel_page(ChannelPullPage {
        cursors: [(
            "a".into(),
            CursorRange {
                from: 1,
                to: 2,
                head: 2,
            },
        )]
        .into(),
        changes: vec![ChannelChange::Remove {
            channel: "a".into(),
            cursor: 2,
            key: todo.clone(),
        }],
    })
    .unwrap();
    let report = c
        .finish_query_once(&flight, &succeeded(&request, "late", 99))
        .unwrap();
    assert_eq!(report.applied, 0, "old Query body must be fenced");
    assert_eq!(report.completions.len(), 1);
    assert!(c.read(&todo).unwrap().is_none());
    drop(c);
    let mut c = open(&path);
    assert_eq!(cached(begin(&mut c, false)), titled("late"));
    assert!(
        c.read(&todo).unwrap().is_none(),
        "cache hit never replays authority"
    );
    let (flight, request) = fetch(begin(&mut c, true));
    c.finish_query_once(&flight, &succeeded(&request, "fresh", 7))
        .unwrap();
    assert_eq!(c.read(&todo).unwrap().unwrap()["title"], "fresh");
}

#[test]
fn plain_direct_query_uses_frozen_epoch_and_stale_claim_cannot_bypass_fresh_epoch() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    let sub = c.ensure_subscription("a").unwrap();
    c.initialize_subscriptions(
        &[("a".into(), sub.subscription_id)].into(),
        &[("a".into(), 0)].into(),
    )
    .unwrap();
    let todo = RecordKey {
        model: "Todo".into(),
        identity: json!({"id":"a"}),
    };
    c.apply_channel_page(ChannelPullPage {
        cursors: [(
            "a".into(),
            CursorRange {
                from: 0,
                to: 1,
                head: 1,
            },
        )]
        .into(),
        changes: vec![ChannelChange::Upsert {
            channel: "a".into(),
            cursor: 1,
            record: AuthorityRecord {
                model: "Todo".into(),
                identity: todo.identity.clone(),
                stamp: 7,
                state: json!({"title":"base"}),
                error: None,
            },
        }],
    })
    .unwrap();
    let old = c.prepare_action("GetTodos", 1, args()).unwrap();
    c.apply_channel_page(ChannelPullPage {
        cursors: [(
            "a".into(),
            CursorRange {
                from: 1,
                to: 2,
                head: 2,
            },
        )]
        .into(),
        changes: vec![ChannelChange::Remove {
            channel: "a".into(),
            cursor: 2,
            key: todo.clone(),
        }],
    })
    .unwrap();
    let report = c
        .apply_action_response(&old, &succeeded(&old, "late", 99))
        .unwrap();
    assert_eq!(report.applied, 0);
    assert_eq!(report.completions.len(), 1);
    assert!(c.read(&todo).unwrap().is_none());
    let fresh = c.prepare_action("GetTodos", 1, args()).unwrap();
    let mut response: Value =
        serde_json::from_slice(&succeeded(&fresh, "stale enrollment", 99)).unwrap();
    response["memberships"] =
        json!([{"channel":"a","cursor":1,"model":"Todo","identity":{"id":"a"}}]);
    let report = c
        .apply_action_response(&fresh, &serde_json::to_vec(&response).unwrap())
        .unwrap();
    assert_eq!(report.applied, 0);
    assert!(c.read(&todo).unwrap().is_none());
    let fresh = c.prepare_action("GetTodos", 1, args()).unwrap();
    let mut response: Value = serde_json::from_slice(&succeeded(&fresh, "restored", 7)).unwrap();
    response["memberships"] =
        json!([{"channel":"a","cursor":3,"model":"Todo","identity":{"id":"a"}}]);
    assert_eq!(
        c.apply_action_response(&fresh, &serde_json::to_vec(&response).unwrap())
            .unwrap()
            .applied,
        1
    );
    assert_eq!(c.read(&todo).unwrap().unwrap()["title"], "restored");
}
