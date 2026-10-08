use axton_client::ClientStore;
use axton_client::ddl::reconcile;
use axton_core::Schema;
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};

fn schema(fields: Value) -> Schema {
    Schema::from_value(json!({"enums":[],"models":[{"name":"Task","identity":["id"],"fields":fields,"unique":[["title"]]}]})).unwrap()
}
fn base() -> Value {
    json!([{"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"}},
           {"name":"title","nullable":false,"type":{"kind":"scalar","name":"string"}},
           {"name":"done","nullable":false,"type":{"kind":"scalar","name":"boolean"}}])
}
fn columns(s: &mut SqliteStore, table: &str) -> Vec<(String, String, i64)> {
    s.query_committed(&format!("PRAGMA table_info(\"{table}\")"), &[])
        .unwrap()
        .rows
        .into_iter()
        .map(|r| {
            (
                r[1].as_str().unwrap().into(),
                r[2].as_str().unwrap().into(),
                r[5].as_i64().unwrap(),
            )
        })
        .collect()
}
fn open(dir: &tempfile::TempDir, schema: &Schema) -> axton_core::Result<SqliteStore> {
    let mut s = SqliteStore::open(dir.path().join("db")).unwrap();
    s.begin().unwrap();
    match reconcile(&mut s, schema) {
        Ok(()) => {
            s.commit().unwrap();
            Ok(s)
        }
        Err(e) => {
            s.rollback().unwrap();
            Err(e)
        }
    }
}

#[test]
fn creates_model_before_and_framework_tables() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = open(&dir, &schema(base())).unwrap();
    assert_eq!(
        columns(&mut s, "Task"),
        vec![
            ("id".into(), "TEXT".into(), 1),
            ("title".into(), "TEXT".into(), 0),
            ("done".into(), "INTEGER".into(), 0)
        ]
    );
    assert_eq!(
        columns(&mut s, "axton_before_Task"),
        columns(&mut s, "Task")
    );
    for table in ["Task", "axton_before_Task"] {
        assert!(!columns(&mut s, table).is_empty(), "{table}");
    }
    let indexes = s.query_committed("SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='Task' AND name='Task_title_unique'", &[]).unwrap();
    assert_eq!(indexes.rows.len(), 1);
    assert!(
        s.execute("INSERT INTO \"Task\" VALUES ('a','same',0)", &[])
            .is_ok()
    );
    assert!(
        s.execute("INSERT INTO \"Task\" VALUES ('b','same',0)", &[])
            .is_err()
    );
}

#[test]
fn adds_missing_columns_to_both_tables_and_keeps_unknown_ones() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = open(&dir, &schema(base())).unwrap();
    s.execute("INSERT INTO \"Task\" VALUES ('a','t',1)", &[])
        .unwrap();
    s.execute_batch("ALTER TABLE \"Task\" ADD COLUMN legacy TEXT")
        .unwrap();
    drop(s);
    let mut fields = base().as_array().unwrap().clone();
    fields.push(json!({"name":"note","nullable":true,"type":{"kind":"scalar","name":"string"}}));
    fields.push(
        json!({"name":"rank","nullable":false,"type":{"kind":"scalar","name":"int"},"default":3}),
    );
    let mut s = open(&dir, &schema(Value::Array(fields))).unwrap();
    let names: Vec<String> = columns(&mut s, "Task").into_iter().map(|c| c.0).collect();
    assert_eq!(names, vec!["id", "title", "done", "legacy", "note", "rank"]);
    let before: Vec<String> = columns(&mut s, "axton_before_Task")
        .into_iter()
        .map(|c| c.0)
        .collect();
    assert_eq!(before, vec!["id", "title", "done", "note", "rank"]);
    assert_eq!(
        s.query_committed("SELECT rank, note FROM \"Task\"", &[])
            .unwrap()
            .rows,
        vec![vec![json!(3), Value::Null]]
    );
}

#[test]
fn rejects_non_nullable_column_without_default_identity_change_and_type_change() {
    let dir = tempfile::tempdir().unwrap();
    drop(open(&dir, &schema(base())).unwrap());
    let mut fields = base().as_array().unwrap().clone();
    fields.push(json!({"name":"rank","nullable":false,"type":{"kind":"scalar","name":"int"}}));
    assert!(open(&dir, &schema(Value::Array(fields))).is_err());
    let retyped = json!([{"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"}},
                         {"name":"title","nullable":false,"type":{"kind":"scalar","name":"string"}},
                         {"name":"done","nullable":false,"type":{"kind":"scalar","name":"string"}}]);
    assert!(open(&dir, &schema(retyped)).is_err());
    let composite = Schema::from_value(
        json!({"enums":[],"models":[{"name":"Task","identity":["id","title"],"fields":base()}]}),
    )
    .unwrap();
    assert!(open(&dir, &composite).is_err());
    let mut s = SqliteStore::open(dir.path().join("db")).unwrap();
    assert_eq!(
        columns(&mut s, "Task").len(),
        3,
        "a failed reconciliation changes nothing"
    );
}
