use axton_client::{
    Client, Schema,
    runtime::{ClientRuntime, Input},
    v04,
};
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};
fn run(r: &mut ClientRuntime<SqliteStore>) -> Vec<Value> {
    while r.step(1, 7) {}
    r.take_events()
        .into_iter()
        .map(|e| serde_json::to_value(e).unwrap())
        .collect()
}
fn send(r: &mut ClientRuntime<SqliteStore>, input: Value) -> Vec<Value> {
    r.receive(serde_json::from_value::<Input>(input).unwrap(), 1, 7)
        .unwrap();
    run(r)
}
fn open(p: &std::path::Path) -> ClientRuntime<SqliteStore> {
    let mut s: Value =
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap();
    s["actions"] = json!([{"name":"Rename","version":1,"inputs":[{"kind":"model","name":"entry","model":"Entry","operation":"update","cardinality":"single"}],"outputs":[]}]);
    let mut c = Client::open_bound(
        SqliteStore::open_exclusive(p).unwrap(),
        Schema::from_value(s).unwrap(),
        v04::StoreBinding {
            backend: "b".into(),
            viewer: "a".into(),
            stream: "User:a".into(),
            contract: "app".into(),
        },
    )
    .unwrap();
    c.install_stream04(
        &c.request_context().unwrap().clone(),
        &v04::StreamRecord {
            key: axton_client::RecordKey {
                model: "Entry".into(),
                identity: json!({"id":"e"}),
            },
            cursor: 57,
            state: json!({"text":"old","note":null}),
        },
    )
    .unwrap();
    ClientRuntime::new(c)
}
fn command(
    r: &mut ClientRuntime<SqliteStore>,
    id: &str,
    tx: &Value,
    companion: Option<&Value>,
    command: Value,
) -> Vec<Value> {
    let mut input =
        json!({"type":"transactionCommand","requestId":id,"transactionId":tx,"command":command});
    if let Some(token) = companion {
        input["companionId"] = token.clone();
    }
    send(r, input)
}
#[test]
fn mutation_callback_reads_before_input_and_queues_companions_before_optimism() {
    let d = tempfile::tempdir().unwrap();
    let mut r = open(&d.path().join("db"));
    let events = send(
        &mut r,
        json!({"type":"task","requestId":"tx","command":{"kind":"transaction"}}),
    );
    let root = events
        .iter()
        .find(|e| e["operation"]["kind"] == "callback")
        .unwrap();
    let tx = root["operation"]["transactionId"].clone();
    let events = command(
        &mut r,
        "act",
        &tx,
        None,
        json!({"kind":"submitMutation","name":"Rename","version":1,"local":true}),
    );
    let local = events
        .iter()
        .find(|e| e["operation"]["kind"] == "mutationLocal")
        .unwrap();
    let token = &local["operation"]["companionId"];
    let read = command(
        &mut r,
        "before",
        &tx,
        Some(token),
        json!({"kind":"read","key":{"model":"Entry","identity":{"id":"e"}}}),
    );
    assert_eq!(
        read.iter().find(|e| e["requestId"] == "before").unwrap()["value"]["text"],
        "old"
    );
    command(
        &mut r,
        "write",
        &tx,
        Some(token),
        json!({"kind":"direct","operation":{"model":"Entry","op":"update","identity":{"id":"e"},"values":{"text":"callback"}}}),
    );
    let read = command(
        &mut r,
        "after",
        &tx,
        Some(token),
        json!({"kind":"read","key":{"model":"Entry","identity":{"id":"e"}}}),
    );
    assert_eq!(
        read.iter().find(|e| e["requestId"] == "after").unwrap()["value"]["text"],
        "callback"
    );
    let done = send(
        &mut r,
        json!({"type":"callbackResult","effectId":local["effectId"],"transactionId":tx,"companionId":token,"ok":true,"input":{"entry":{"id":"e","text":"optimistic"}}}),
    );
    assert_eq!(
        done.iter().find(|e| e["requestId"] == "act").unwrap()["ok"],
        true
    );
    send(
        &mut r,
        json!({"type":"callbackResult","effectId":root["effectId"],"transactionId":tx,"ok":true}),
    );
    let key = axton_client::RecordKey {
        model: "Entry".into(),
        identity: json!({"id":"e"}),
    };
    assert_eq!(
        r.client().read(&key).unwrap().unwrap()["text"],
        "optimistic"
    );
    assert_eq!(
        r.client()
            .record_evidence04(&key)
            .unwrap()
            .current
            .unwrap()
            .cursor,
        57
    );
    assert_eq!(r.client().pending_count().unwrap(), 1);
}
#[test]
fn invalid_callback_input_rolls_back_its_writes_and_allows_outer_transaction_to_commit() {
    let d = tempfile::tempdir().unwrap();
    let mut r = open(&d.path().join("db"));
    let events = send(
        &mut r,
        json!({"type":"task","requestId":"tx","command":{"kind":"transaction"}}),
    );
    let root = events
        .iter()
        .find(|e| e["operation"]["kind"] == "callback")
        .unwrap();
    let tx = root["operation"]["transactionId"].clone();
    command(
        &mut r,
        "outer",
        &tx,
        None,
        json!({"kind":"direct","operation":{"model":"Entry","op":"update","identity":{"id":"e"},"values":{"note":"outer"}}}),
    );
    let events = command(
        &mut r,
        "act",
        &tx,
        None,
        json!({"kind":"submitMutation","name":"Rename","version":1,"local":true}),
    );
    let local = events
        .iter()
        .find(|e| e["operation"]["kind"] == "mutationLocal")
        .unwrap();
    command(
        &mut r,
        "owned",
        &tx,
        Some(&local["operation"]["companionId"]),
        json!({"kind":"direct","operation":{"model":"Entry","op":"update","identity":{"id":"e"},"values":{"text":"temporary"}}}),
    );
    let done = send(
        &mut r,
        json!({"type":"callbackResult","effectId":local["effectId"],"transactionId":tx,"companionId":local["operation"]["companionId"],"ok":true,"input":{"entry":{"id":"e","text":123}}}),
    );
    assert_eq!(
        done.iter().find(|e| e["requestId"] == "act").unwrap()["ok"],
        false
    );
    let done = send(
        &mut r,
        json!({"type":"callbackResult","effectId":root["effectId"],"transactionId":tx,"ok":true}),
    );
    assert_eq!(
        done.iter().find(|e| e["requestId"] == "tx").unwrap()["ok"],
        true
    );
    let key = axton_client::RecordKey {
        model: "Entry".into(),
        identity: json!({"id":"e"}),
    };
    let row = r.client().read(&key).unwrap().unwrap();
    assert_eq!(row["text"], "old");
    assert_eq!(row["note"], "outer");
    assert_eq!(r.client().pending_count().unwrap(), 0);
}

#[test]
fn transaction_capability_from_another_client_cannot_join_a_matching_local_counter() {
    let d = tempfile::tempdir().unwrap();
    let mut a = open(&d.path().join("a"));
    let mut b = open(&d.path().join("b"));
    let ae = send(
        &mut a,
        json!({"type":"task","requestId":"a","command":{"kind":"transaction"}}),
    );
    let be = send(
        &mut b,
        json!({"type":"task","requestId":"b","command":{"kind":"transaction"}}),
    );
    let ar = ae
        .iter()
        .find(|e| e["operation"]["kind"] == "callback")
        .unwrap();
    let br = be
        .iter()
        .find(|e| e["operation"]["kind"] == "callback")
        .unwrap();
    let foreign = command(
        &mut b,
        "foreign",
        &ar["operation"]["transactionId"],
        None,
        json!({"kind":"direct","operation":{"model":"Entry","op":"update","identity":{"id":"e"},"values":{"text":"cross-client"}}}),
    );
    assert_eq!(
        foreign
            .iter()
            .find(|e| e["requestId"] == "foreign")
            .unwrap()["ok"],
        false
    );
    assert_ne!(
        ar["operation"]["transactionId"],
        br["operation"]["transactionId"]
    );
    let done = send(
        &mut b,
        json!({"type":"callbackResult","effectId":br["effectId"],"transactionId":br["operation"]["transactionId"],"ok":true}),
    );
    assert_eq!(
        done.iter().find(|e| e["requestId"] == "b").unwrap()["ok"],
        true
    );
    assert_eq!(
        b.client()
            .read(&axton_client::RecordKey {
                model: "Entry".into(),
                identity: json!({"id":"e"})
            })
            .unwrap()
            .unwrap()["text"],
        "old"
    );
}
