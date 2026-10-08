//! Reproducible diagnostic, not a throughput guarantee. Each enqueue is a real SQLite commit.
use axton_client::Client;
use axton_core::{Schema, v05};
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};
use std::time::Instant;
fn main() {
    let mut descriptor: Value =
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap();
    descriptor["actions"] = json!([{"name":"Edit","version":1,"inputs":[{"kind":"model","name":"entry","model":"Entry","operation":"update","cardinality":"single","fields":["text"]}],"outputs":[]}]);
    let schema = Schema::from_value(descriptor).unwrap();
    for count in [10, 1000] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("capacity.sqlite");
        let mut client = Client::open05(
            SqliteStore::open(&path).unwrap(),
            schema.clone(),
            "User:diagnostic",
        )
        .unwrap();
        client.initialize_stream05(0).unwrap();
        let context = client.request_context05().unwrap();
        let authority = |cursor| v05::AuthorityChange::Record {
            key: v05::RecordKey {
                model: "Entry".into(),
                identity: json!({"id":"one"}),
            },
            cursor,
            state: json!({"text":"authority","note":null}),
        };
        client
            .install_authority05(&context, &[authority(1)], Some((0, 1)))
            .unwrap();
        let mut samples = Vec::new();
        for i in 0..count {
            let start = Instant::now();
            client
                .transaction(|tx| {
                    tx.submit_mutation05(
                        "Edit",
                        1,
                        json!({"entry":{"id":"one","text":format!("local-{i}")}}),
                        vec![],
                    )?;
                    Ok(())
                })
                .unwrap();
            samples.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        samples.sort_by(f64::total_cmp);
        let start = Instant::now();
        client
            .install_authority05(&context, &[authority(2)], Some((1, 2)))
            .unwrap();
        let replay_ms = start.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(client.pending_count().unwrap(), count);
        let key = schema.record_key("Entry", &json!({"id":"one"})).unwrap();
        assert_eq!(
            client.read(&key).unwrap().unwrap()["text"],
            format!("local-{}", count - 1)
        );
        // Count enqueue and authority-page commits only; subscription setup is excluded.
        println!(
            "{}",
            json!({"queue":count,"enqueue_p50_ms":samples[(count-1)/2],"enqueue_p95_ms":samples[(count*95/100).min(count-1)],"page_replay_ms":replay_ms,"enqueue_and_page_commits":count+2})
        );
    }
}
