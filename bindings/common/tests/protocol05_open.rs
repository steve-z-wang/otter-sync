use axton_binding::actor;
use serde_json::{Value, json};
use std::{sync::mpsc, time::Duration};
fn open(path: &std::path::Path, stream: Value) -> (u64, Value) {
    let (tx, rx) = mpsc::channel();
    let id=actor::open(json!({"type":"open","requestId":"open","path":path,"schema":serde_json::from_str::<Value>(include_str!("../../../fixtures/schemas/entry.json")).unwrap(),"protocol":5,"stream":stream}),Box::new(move|id|{let _=tx.send(id);})).unwrap();
    loop {
        rx.recv_timeout(Duration::from_secs(10)).unwrap();
        if let Some(result) = actor::drain(id)
            .into_iter()
            .find(|e| e["requestId"] == "open")
        {
            return (id, result);
        }
    }
}
#[test]
fn native_open_requires_stream_and_owns_physical_file() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let (id, missing) = open(&p, Value::Null);
    assert_eq!(missing["ok"], false);
    actor::detach(id);
    axton_sqlite::SqliteStore::set_application_data_directory(
        std::env::temp_dir().join("axton-task8-open-locks"),
    )
    .unwrap();
    let stream = json!("User:a");
    let (id, first) = open(&p, stream.clone());
    assert_eq!(first["ok"], true, "{first:?}");
    assert_eq!(first["value"]["context"]["protocol"], 5);
    let (second, result) = open(&p, stream);
    assert_eq!(result["ok"], false);
    assert!(result["error"].as_str().unwrap().contains("store_in_use"));
    actor::detach(second);
    actor::detach(id);
}
