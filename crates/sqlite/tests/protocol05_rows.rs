use axton_client::ClientStore;
use axton_client::ddl::reconcile;
use axton_client::engine::Engine;
use axton_client::rows::{decode_row, merge_identity};
use axton_core::Schema;
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};
use std::collections::BTreeSet;

fn schema() -> Schema {
    Schema::from_value(json!({"enums":[],"models":[{"name":"Task","identity":["id"],"fields":[
        {"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"}},
        {"name":"title","nullable":false,"type":{"kind":"scalar","name":"string"}},
        {"name":"done","nullable":false,"type":{"kind":"scalar","name":"boolean"}},
        {"name":"tags","nullable":false,"type":{"kind":"list","element":{"kind":"scalar","name":"string"}}}]}]})).unwrap()
}
fn store() -> (tempfile::TempDir, SqliteStore) {
    let dir = tempfile::tempdir().unwrap();
    let mut s = SqliteStore::open(dir.path().join("db")).unwrap();
    s.begin().unwrap();
    reconcile(&mut s, &schema()).unwrap();
    s.commit().unwrap();
    (dir, s)
}
fn row(id: &str, title: &str) -> Value {
    json!({"id":id,"title":title,"done":false,"tags":["a","b"]})
}

#[test]
fn model_rows_round_trip_booleans_lists_and_copy_aside() {
    let (_d, mut s) = store();
    let schema = schema();
    let model = schema.model("Task").unwrap();
    let mut changed = BTreeSet::new();
    s.begin().unwrap();
    let mut e = Engine::new(&mut s, &schema, &mut changed, false);
    e.row_insert("Task", model, &row("t1", "A")).unwrap();
    assert!(e.row_insert("Task", model, &row("t1", "A")).is_err());
    e.row_upsert("Task", model, &row("t1", "B")).unwrap();
    assert_eq!(
        e.row_get("Task", model, &json!({"id":"t1"})).unwrap(),
        Some(row("t1", "B"))
    );
    e.copy_aside(model, &json!({"id":"t1"})).unwrap();
    e.copy_aside(model, &json!({"id":"missing"})).unwrap();
    assert_eq!(
        e.row_get("axton_before_Task", model, &json!({"id":"t1"}))
            .unwrap(),
        Some(row("t1", "B"))
    );
    assert_eq!(e.count("axton_before_Task").unwrap(), 1);
    assert_eq!(
        e.identities_where("Task", model, &[("done".into(), json!(false))])
            .unwrap(),
        vec![json!({"id":"t1"})]
    );
    assert!(
        e.identities_where("Task", model, &[("title".into(), json!("nope"))])
            .unwrap()
            .is_empty()
    );
    e.row_delete("Task", model, &json!({"id":"t1"})).unwrap();
    assert_eq!(e.row_get("Task", model, &json!({"id":"t1"})).unwrap(), None);
    assert_eq!(
        changed,
        BTreeSet::from(["Task".to_string(), "axton_before_Task".to_string()])
    );
    s.rollback().unwrap();
    assert_eq!(
        decode_row(
            model,
            &["done".into(), "tags".into()],
            &[json!(1), json!("[\"x\"]")]
        )
        .unwrap(),
        json!({"done":true,"tags":["x"]})
    );
    assert_eq!(
        merge_identity(&json!({"id":"t1"}), &json!({"title":"T"})),
        json!({"id":"t1","title":"T"})
    );
}
