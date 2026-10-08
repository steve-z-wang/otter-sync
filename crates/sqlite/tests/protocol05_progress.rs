use axton_client::{Client, RecordKey, Schema, sync05::DeliveryQueue, v05};
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};
fn schema() -> Schema {
    let mut s: Value =
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap();
    s["models"][0]["unique"] = json!([["text"]]);
    Schema::from_value(s).unwrap()
}
fn key(id: &str) -> RecordKey {
    RecordKey {
        model: "Entry".into(),
        identity: json!({"id":id}),
    }
}
fn change(id: &str, cursor: u64, text: &str) -> v05::AuthorityChange {
    v05::AuthorityChange::Record {
        key: v05::RecordKey {
            model: "Entry".into(),
            identity: json!({"id":id}),
        },
        cursor,
        state: json!({"text":text,"note":null}),
    }
}
fn receive(
    c: &mut Client<SqliteStore>,
    q: &mut DeliveryQueue,
    id: &str,
    after: u64,
    through: u64,
    head: u64,
    changes: Vec<v05::AuthorityChange>,
) {
    let context = c.request_context05().unwrap();
    let p = v05::freeze_delivery(
        context.clone(),
        id.into(),
        if id == "boot" {
            v05::DeliveryPurpose::Bootstrap
        } else {
            v05::DeliveryPurpose::Sync
        },
        after,
        through,
        head,
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
}
#[test]
fn unique_transfer_is_final_projection_atomic_and_ahead_content_does_not_advance_prefix() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("db");
    let mut c = Client::open05(SqliteStore::open(&path).unwrap(), schema(), "User:u").unwrap();
    c.initialize_stream05(0).unwrap();
    let mut q = DeliveryQueue::new(100_000, 8);
    receive(&mut c, &mut q, "boot", 0, 0, 0, vec![]);
    c.apply_next_delivery05(&mut q, 1).unwrap();
    let context = c.request_context05().unwrap();
    c.install_authority05(
        &context,
        &[change("x", 1, "alpha"), change("y", 2, "beta")],
        None,
    )
    .unwrap();
    receive(
        &mut c,
        &mut q,
        "swap",
        0,
        3,
        4,
        vec![change("x", 3, "beta"), change("y", 4, "alpha")],
    );
    c.apply_next_delivery05(&mut q, 1).unwrap();
    assert_eq!(c.store_status05().unwrap().cursor, Some(3));
    assert_eq!(
        c.record_evidence05(&key("y"))
            .unwrap()
            .current
            .unwrap()
            .cursor,
        4
    );
    assert_eq!(c.read(&key("x")).unwrap().unwrap()["text"], "beta");
    assert_eq!(c.read(&key("y")).unwrap().unwrap()["text"], "alpha");
    receive(
        &mut c,
        &mut q,
        "bad",
        3,
        6,
        6,
        vec![change("x", 5, "same"), change("y", 6, "same")],
    );
    assert!(c.apply_next_delivery05(&mut q, 1).is_err());
    assert_eq!(c.store_status05().unwrap().cursor, Some(3));
    assert_eq!(
        c.record_evidence05(&key("x"))
            .unwrap()
            .current
            .unwrap()
            .cursor,
        3
    );
    assert_eq!(c.read(&key("x")).unwrap().unwrap()["text"], "beta");
    drop(c);
    let mut c = Client::open05(SqliteStore::open(&path).unwrap(), schema(), "User:u").unwrap();
    assert_eq!(c.store_status05().unwrap().cursor, Some(3));
    assert_eq!(c.read(&key("y")).unwrap().unwrap()["text"], "alpha");
}

#[test]
fn bootstrap_unique_release_closes_cached_conflict_in_one_current_atomic_unit() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("db");
    let mut c = Client::open05(SqliteStore::open(&path).unwrap(), schema(), "User:u").unwrap();
    c.initialize_stream05(40).unwrap();
    c.install_cache05(
        &[
            v05::ReadRecord {
                key: v05::RecordKey {
                    model: "Entry".into(),
                    identity: json!({"id":"a"}),
                },
                cursor: (),
                state: json!({"text":"x","note":null}),
            },
            v05::ReadRecord {
                key: v05::RecordKey {
                    model: "Entry".into(),
                    identity: json!({"id":"b"}),
                },
                cursor: (),
                state: json!({"text":"y","note":null}),
            },
        ],
        true,
    )
    .unwrap();
    let mut q = DeliveryQueue::new(100_000, 8);
    receive(
        &mut c,
        &mut q,
        "boot",
        0,
        40,
        40,
        vec![change("b", 40, "x"), change("a", 30, "z")],
    );
    c.apply_next_delivery05(&mut q, 1).unwrap();
    assert_eq!(c.read(&key("a")).unwrap().unwrap()["text"], "z");
    assert_eq!(c.read(&key("b")).unwrap().unwrap()["text"], "x");
    assert_eq!(c.store_status05().unwrap().cursor, Some(40));
    drop(c);
    let mut c = Client::open05(SqliteStore::open(&path).unwrap(), schema(), "User:u").unwrap();
    assert_eq!(c.read(&key("a")).unwrap().unwrap()["text"], "z");
    assert_eq!(c.read(&key("b")).unwrap().unwrap()["text"], "x");
    assert_eq!(c.store_status05().unwrap().cursor, Some(40));
}
