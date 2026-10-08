//! Original released files remain recoverable; format5 adopts a fresh file.
use axton_client::{
    Client, ClientStore, Operation, OperationKind, RecordKey, Schema, sync05::DeliveryQueue, v05,
};
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};
fn schema() -> Schema {
    Schema::from_value(serde_json::from_str(include_str!("fixtures/schema.json")).unwrap()).unwrap()
}
type CatalogRows = Vec<Vec<Value>>;
type FileSnapshot = (CatalogRows, Vec<(String, CatalogRows)>);
fn snapshot(path: &std::path::Path) -> FileSnapshot {
    let mut s = SqliteStore::open(path).unwrap();
    let catalog = s
        .query(
            "SELECT type,name,sql FROM sqlite_master ORDER BY type,name",
            &[],
        )
        .unwrap()
        .rows;
    let tables = s
        .query(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name",
            &[],
        )
        .unwrap()
        .rows;
    let rows = tables
        .into_iter()
        .map(|r| {
            let n = r[0].as_str().unwrap().to_owned();
            let rows = s
                .query(&format!("SELECT * FROM \"{n}\" ORDER BY rowid"), &[])
                .unwrap()
                .rows;
            (n, rows)
        })
        .collect();
    (catalog, rows)
}
#[test]
fn original_v02_layout_is_refused_unchanged_and_fresh_file_is_independent() {
    for marker in [0, 1] {
        let d = tempfile::tempdir().unwrap();
        let old = d.path().join("original.sqlite");
        let fresh = d.path().join("format5.sqlite");
        let mut raw = SqliteStore::open(&old).unwrap();
        raw.execute_batch(include_str!("fixtures/v02-framework.sql"))
            .unwrap();
        raw.execute_batch(include_str!("fixtures/sqlite-state.sql"))
            .unwrap();
        raw.execute_batch(&format!(
            "UPDATE axton_client SET channel_membership_version={marker}"
        ))
        .unwrap();
        drop(raw);
        let saved = snapshot(&old);
        let bytes = std::fs::read(&old).unwrap();
        for _ in 0..2 {
            let error = Client::open05(SqliteStore::open(&old).unwrap(), schema(), "User:u")
                .err()
                .unwrap();
            assert!(error.to_string().contains("unsupported Store format"));
            assert_eq!(snapshot(&old), saved);
            assert_eq!(std::fs::read(&old).unwrap(), bytes);
        }
        let mut c = Client::open05(SqliteStore::open(&fresh).unwrap(), schema(), "User:u").unwrap();
        assert!(c.query("Todo", &json!({})).unwrap().is_empty());
        assert_eq!(c.pending_count().unwrap(), 0);
        assert_eq!(c.read_sql("SELECT count(*) AS n FROM sqlite_master WHERE type='table' AND name IN ('axton_stream_member','axton_record','axton_v04_call','axton_call','axton_query_cache','axton_load','axton_mutation')", &[]).unwrap()[0]["n"], 0);
        c.transaction(|tx| {
            tx.direct(Operation {
                model: "Todo".into(),
                op: OperationKind::Create,
                identity: json!({"id":"new"}),
                values: Some(json!({"title":"fresh","channel":"opaque Channel business field"})),
            })
        })
        .unwrap();
        drop(c);
        let mut c = Client::open05(SqliteStore::open(&fresh).unwrap(), schema(), "User:u").unwrap();
        assert_eq!(
            c.query("Todo", &json!({})).unwrap()[0]["channel"],
            "opaque Channel business field"
        );
        assert_eq!(snapshot(&old), saved);
        assert_eq!(std::fs::read(&old).unwrap(), bytes);
    }
}
#[test]
fn canonical_remove_delivery_advances_prefix_preserves_content_and_history() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("db");
    let mut c = Client::open05(SqliteStore::open(&path).unwrap(), schema(), "User:u").unwrap();
    c.initialize_stream05(0).unwrap();
    let context = c.request_context05().unwrap();
    let k = v05::RecordKey {
        model: "Todo".into(),
        identity: json!({"id":"e"}),
    };
    let mut q = DeliveryQueue::new(100_000, 8);
    for (id, purpose, after, through, changes) in [
        (
            "initial",
            v05::DeliveryPurpose::Bootstrap,
            0,
            0,
            vec![v05::AuthorityChange::Record {
                key: k.clone(),
                cursor: 1,
                state: json!({"title":"kept","channel":"opaque"}),
            }],
        ),
        (
            "remove",
            v05::DeliveryPurpose::Sync,
            0,
            2,
            vec![v05::AuthorityChange::Remove {
                key: k.clone(),
                cursor: 2,
            }],
        ),
    ] {
        let p = v05::freeze_delivery(
            context.clone(),
            id.into(),
            purpose,
            after,
            through,
            through.max(1),
            8_000_000_000_000_000,
            vec![v05::DeliveryUnit {
                index: 0,
                through: Some(through),
                changes,
            }],
            100,
        )
        .unwrap();
        q.receive(&p.header, &p.parts, &context, 1).unwrap();
        c.apply_next_delivery05(&mut q, 1).unwrap();
    }
    let key = RecordKey {
        model: k.model,
        identity: k.identity,
    };
    assert_eq!(c.store_status05().unwrap().cursor, Some(2));
    assert_eq!(c.read(&key).unwrap().unwrap()["title"], "kept");
    let evidence = c.record_evidence05(&key).unwrap();
    assert!(evidence.current.is_none());
    assert_eq!(evidence.history[&context.materialization], 1);
    drop(c);
    let mut c = Client::open05(SqliteStore::open(&path).unwrap(), schema(), "User:u").unwrap();
    assert_eq!(c.store_status05().unwrap().cursor, Some(2));
    assert_eq!(c.read(&key).unwrap().unwrap()["title"], "kept");
    assert!(c.record_evidence05(&key).unwrap().current.is_none());
}
