use axton_client::{ActionOutcome, CallCompletion, Client, Schema, v04};
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};
fn open(p: &std::path::Path) -> Client<SqliteStore> {
    let schema = Schema::from_value(
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap(),
    )
    .unwrap();
    Client::open_bound(
        SqliteStore::open_exclusive(p).unwrap(),
        schema,
        v04::StoreBinding {
            backend: "b".into(),
            viewer: "a".into(),
            stream: "User:a".into(),
            contract: "app".into(),
        },
    )
    .unwrap()
}
fn response(request: &v04::FetchIntent, text: Option<&str>) -> v04::ReadResponse {
    let state = text
        .map(|text| json!({"text":text,"note":null}))
        .unwrap_or(Value::Null);
    let result = if state.is_null() {
        Value::Null
    } else {
        json!({"id":"e","text":text.unwrap(),"note":null})
    };
    v04::ReadResponse {
        context: request.context.clone(),
        completion: CallCompletion {
            call_id: request.call_id.clone(),
            outcome: ActionOutcome::Succeeded { result },
        },
        records: vec![v04::ReadRecord {
            key: axton_client::RecordKey {
                model: "Entry".into(),
                identity: json!({"id":"e"}),
            },
            cursor: v04::NullCursor,
            state,
        }],
    }
}
#[test]
fn fetch_completion_and_mode_survive_reopen_without_rewriting_the_snapshot() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut c = open(&p);
    let request = c
        .prepare_fetch04("Entry", 1, &json!({"id":"e"}), false)
        .unwrap();
    let answer = response(&request, Some("returned"));
    c.apply_fetch04(&request, &answer).unwrap();
    assert!(c.read(&answer.records[0].key).unwrap().is_none());
    let mut changed = request.clone();
    changed.store = true;
    assert!(
        c.apply_fetch04(&changed, &response(&changed, Some("wrong-mode")))
            .err()
            .unwrap()
            .to_string()
            .contains("intent_mismatch")
    );
    drop(c);
    let mut c = open(&p);
    let report = c
        .apply_fetch04(&request, &response(&request, Some("different retry")))
        .unwrap();
    assert_eq!(report.completions[0], answer.completion);
    assert!(c.read(&answer.records[0].key).unwrap().is_none());
}
#[test]
fn fetch_failure_rolls_back_cache_and_completion_but_keeps_frozen_request_retryable() {
    let d = tempfile::tempdir().unwrap();
    let mut c = open(&d.path().join("db"));
    let request = c
        .prepare_fetch04("Entry", 1, &json!({"id":"e"}), true)
        .unwrap();
    let mut bad = response(&request, Some("A"));
    bad.records[0].state["note"] = json!(123);
    bad.completion.outcome = ActionOutcome::Succeeded {
        result: json!({"id":"e","text":"A","note":123}),
    };
    assert!(c.apply_fetch04(&request, &bad).is_err());
    assert!(c.read(&bad.records[0].key).unwrap().is_none());
    assert!(c.read_completion04(&request.call_id).unwrap().is_none());
    let good = response(&request, Some("A"));
    c.apply_fetch04(&request, &good).unwrap();
    assert_eq!(
        c.read_completion04(&request.call_id).unwrap().unwrap(),
        good.completion
    );
    assert_eq!(c.read(&good.records[0].key).unwrap().unwrap()["text"], "A");
    assert!(
        c.record_evidence04(&good.records[0].key)
            .unwrap()
            .history
            .is_empty()
    );
}
#[test]
fn query_cache_modes_and_projection_context_are_distinct_even_without_model_outputs() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut descriptor: Value =
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap();
    descriptor["actions"] =
        json!([{"name":"Ping","kind":"query","version":1,"inputs":[],"outputs":[]}]);
    let schema = Schema::from_value(descriptor).unwrap();
    let binding = v04::StoreBinding {
        backend: "b".into(),
        viewer: "a".into(),
        stream: "User:a".into(),
        contract: "app".into(),
    };
    let c = Client::open_bound(
        SqliteStore::open_exclusive(&p).unwrap(),
        schema.clone(),
        binding.clone(),
    )
    .unwrap();
    let yes = c
        .query_cache_key("Ping", 1, &json!({}), &axton_client::ActionStore::All)
        .unwrap();
    let no = c
        .query_cache_key("Ping", 1, &json!({}), &axton_client::ActionStore::None)
        .unwrap();
    assert_ne!(yes.key, no.key);
    drop(c);
    let c = Client::open_bound_with_projection(
        SqliteStore::open_exclusive(&p).unwrap(),
        schema,
        binding,
        "2",
    )
    .unwrap();
    assert_ne!(
        yes.key,
        c.query_cache_key("Ping", 1, &json!({}), &axton_client::ActionStore::All)
            .unwrap()
            .key
    );
}
