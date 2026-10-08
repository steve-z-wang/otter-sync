use axton_client::{
    Client, Schema,
    runtime::{ClientRuntime, Input},
};
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicU64, Ordering};
static REQUEST: AtomicU64 = AtomicU64::new(0);
fn next_request() -> String {
    format!("r{}", REQUEST.fetch_add(1, Ordering::SeqCst))
}
const ID: &str = "01890f47-1234-7123-8123-123456789abc";
fn send(r: &mut ClientRuntime<SqliteStore>, input: Value) -> Vec<Value> {
    r.receive(serde_json::from_value::<Input>(input).unwrap(), 1, 7)
        .unwrap();
    while r.step(1, 7) {}
    r.take_events()
        .into_iter()
        .map(|e| serde_json::to_value(e).unwrap())
        .collect()
}
fn schema_value() -> Value {
    serde_json::from_str::<Value>(include_str!(
        "../../../fixtures/protocol/action-results.json"
    ))
    .unwrap()["schema"]
        .clone()
}
fn runtime(schema: Value) -> (tempfile::TempDir, ClientRuntime<SqliteStore>) {
    let d = tempfile::tempdir().unwrap();
    let mut c = Client::open05(
        SqliteStore::open(d.path().join("db")).unwrap(),
        Schema::from_value(schema).unwrap(),
        "User:a",
    )
    .unwrap();
    c.initialize_stream05(0).unwrap();
    let mut r = ClientRuntime::new(c);
    send(
        &mut r,
        json!({"type":"task","requestId":"c","command":{"kind":"connect"}}),
    );
    (d, r)
}
fn fetch(
    r: &mut ClientRuntime<SqliteStore>,
    version: u64,
    store: bool,
    result: Value,
    state: Value,
) -> Value {
    let request_id = next_request();
    let e = send(
        r,
        json!({"type":"task","requestId":request_id,"command":{"kind":"fetch","model":"Todo","version":version,"identity":{"id":ID},"store":store}}),
    );
    let http = e
        .iter()
        .find(|e| e["operation"]["route"] == "fetch")
        .unwrap_or_else(|| panic!("Fetch effect missing: {e:?}"));
    let b: Value = serde_json::from_str(http["operation"]["body"].as_str().unwrap()).unwrap();
    let response = json!({"protocol":5,"storeId":b["storeId"],"stream":b["stream"],"materialization":b["materialization"],"requestId":b["requestId"],"outcome":{"kind":"succeeded","result":result},"records":[{"key":{"model":"Todo","identity":{"id":ID}},"cursor":null,"state":state}]});
    send(r,json!({"type":"effectResult","effectId":http["effectId"],"outcome":{"ok":true,"value":response.to_string()}}))
        .into_iter().find(|e|e["requestId"]==request_id).unwrap()
}
fn read(r: &mut ClientRuntime<SqliteStore>) -> Value {
    let request_id = next_request();
    send(r,json!({"type":"task","requestId":request_id,"command":{"kind":"read","key":{"model":"Todo","identity":{"id":ID}}}}))
        .into_iter().find(|e|e["requestId"]==request_id).unwrap()["value"].clone()
}
#[test]
fn retained_fetch_snapshot_is_not_validated_as_current_model() {
    let (_d, mut r) = runtime(schema_value());
    let snapshot = json!({"id":ID,"title":"A"});
    let done = fetch(&mut r, 1, false, snapshot.clone(), json!({"title":"A"}));
    assert_eq!(done["ok"], true, "{done}");
    assert_eq!(done["value"]["outcome"]["result"], snapshot);
    assert_eq!(
        read(&mut r),
        Value::Null,
        "store:false never installs a caller-version row"
    );
}
#[test]
fn retained_fetch_caller_snapshot_and_current_cache_projection_are_independent() {
    let (_d, mut r) = runtime(schema_value());
    let done = fetch(
        &mut r,
        1,
        true,
        json!({"id":ID,"title":"A"}),
        json!({"title":"A","done":false}),
    );
    assert_eq!(done["ok"], true, "{done}");
    assert_eq!(
        done["value"]["outcome"]["result"],
        json!({"id":ID,"title":"A"})
    );
    assert_eq!(read(&mut r), json!({"id":ID,"title":"A","done":false}));
    let bad = fetch(
        &mut r,
        1,
        true,
        json!({"id":ID,"title":"B"}),
        json!({"title":"B"}),
    );
    assert_eq!(
        bad["ok"], false,
        "current required done must not be synthesized: {bad}"
    );
    assert_eq!(read(&mut r), json!({"id":ID,"title":"A","done":false}));
    let bad = fetch(
        &mut r,
        2,
        false,
        json!({"id":ID,"title":"A"}),
        json!({"title":"A"}),
    );
    assert_eq!(
        bad["error"], "fetch.invalid_response",
        "v1 cannot satisfy v2: {bad}"
    );
}
#[test]
fn fetch_snapshot_follows_same_version_read_fallback_and_unknown_field_rules() {
    let mut schema = schema_value();
    let fields = schema["resultModels"][0]["fields"].as_array_mut().unwrap();
    fields.push(json!({"name":"note","type":{"kind":"scalar","name":"string"},"nullable":true}));
    fields.push(json!({"name":"flag","type":{"kind":"scalar","name":"boolean"},"nullable":false,"default":true}));
    schema["models"][0]["version"] = json!(1);
    schema["models"][0]["fields"] = schema["resultModels"][0]["fields"].clone();
    for (result, state) in [
        (json!({"id":ID,"title":"A"}), json!({"title":"A"})),
        (
            json!({"id":ID,"title":"A","extra":1}),
            json!({"title":"A","extra":1}),
        ),
        (
            json!({"id":ID,"title":"A"}),
            json!({"title":"A","note":null,"flag":true}),
        ),
    ] {
        let (_d, mut r) = runtime(schema.clone());
        let done = fetch(&mut r, 1, false, result, state);
        assert_eq!(done["ok"], true, "{done}");
        assert_eq!(
            done["value"]["outcome"]["result"],
            json!({"id":ID,"title":"A","note":null,"flag":true})
        );
        assert_eq!(read(&mut r), Value::Null);
    }
    for (result, state, error) in [
        (
            json!({"id":ID,"title":"A"}),
            json!({"title":"A","flag":false}),
            "fetch.store_failed",
        ),
        (json!({"id":ID}), json!({}), "fetch.invalid_response"),
        (
            json!({"id":ID,"title":"A","note":7}),
            json!({"title":"A","note":7}),
            "fetch.invalid_response",
        ),
        (
            json!({"id":"01890f47-1234-7123-8123-123456789abd","title":"A"}),
            json!({"title":"A"}),
            "fetch.invalid_response",
        ),
    ] {
        let (_d, mut r) = runtime(schema.clone());
        let done = fetch(&mut r, 1, false, result, state);
        assert_eq!(done["error"], error, "{done}");
        assert_eq!(read(&mut r), Value::Null);
    }
    let (_d, mut r) = runtime(schema);
    let explicit = json!({"id":ID,"title":"A","note":"n","flag":false});
    let done = fetch(
        &mut r,
        1,
        false,
        explicit.clone(),
        json!({"title":"A","note":"n","flag":false}),
    );
    assert_eq!(done["ok"], true, "{done}");
    assert_eq!(done["value"]["outcome"]["result"], explicit);
}

#[test]
fn retained_fetch_projection_can_rename_fields_without_fabricating_current_values() {
    let mut schema = schema_value();
    schema["models"][0]["fields"][1]["name"] = json!("heading");
    let (_d, mut r) = runtime(schema);
    let done = fetch(
        &mut r,
        1,
        true,
        json!({"id":ID,"title":"caller"}),
        json!({"heading":"cache","done":false}),
    );
    assert_eq!(done["ok"], true, "{done}");
    assert_eq!(
        done["value"]["outcome"]["result"],
        json!({"id":ID,"title":"caller"})
    );
    assert_eq!(
        read(&mut r),
        json!({"id":ID,"heading":"cache","done":false})
    );
}

#[test]
fn not_found_fetch_returns_null_in_both_modes_without_deleting_cached_content() {
    for store in [false, true] {
        let (_d, mut r) = runtime(schema_value());
        let existing = json!({"id":ID,"title":"existing","done":false});
        assert_eq!(
            fetch(
                &mut r,
                2,
                true,
                existing.clone(),
                json!({"title":"existing","done":false})
            )["ok"],
            true
        );
        let done = fetch(&mut r, 2, store, Value::Null, Value::Null);
        assert_eq!(done["ok"], true, "store={store}: {done}");
        assert_eq!(done["value"]["outcome"]["result"], Value::Null);
        assert_eq!(
            read(&mut r),
            existing,
            "ordinary null never deletes cached data"
        );
    }
}
#[test]
fn same_version_fetch_rejects_null_record_disagreement_in_both_modes() {
    for store in [false, true] {
        for (result, state) in [
            (Value::Null, json!({"title":"row","done":false})),
            (json!({"id":ID,"title":"row","done":false}), Value::Null),
        ] {
            let (_d, mut r) = runtime(schema_value());
            let done = fetch(&mut r, 2, store, result, state);
            assert_eq!(done["error"], "fetch.store_failed", "store={store}: {done}");
            assert_eq!(read(&mut r), Value::Null);
        }
    }
}
#[test]
fn retained_fetch_null_caller_and_current_cache_projection_remain_independent() {
    for store in [false, true] {
        let (_d, mut r) = runtime(schema_value());
        let done = fetch(
            &mut r,
            1,
            store,
            Value::Null,
            json!({"title":"cache","done":false}),
        );
        assert_eq!(done["ok"], true, "store={store}: {done}");
        assert_eq!(done["value"]["outcome"]["result"], Value::Null);
        assert_eq!(
            read(&mut r),
            if store {
                json!({"id":ID,"title":"cache","done":false})
            } else {
                Value::Null
            }
        );
        let done = fetch(
            &mut r,
            1,
            store,
            json!({"id":ID,"title":"caller"}),
            Value::Null,
        );
        assert_eq!(done["ok"], true, "store={store}: {done}");
        assert_eq!(
            done["value"]["outcome"]["result"],
            json!({"id":ID,"title":"caller"})
        );
        assert_eq!(
            read(&mut r),
            if store {
                json!({"id":ID,"title":"cache","done":false})
            } else {
                Value::Null
            }
        );
    }
}
