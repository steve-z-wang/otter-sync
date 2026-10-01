use axton_client::ClientStore;
use axton_client::ddl::{FRAMEWORK_DDL, FRAMEWORK_TABLES, reconcile};
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
    s.execute_batch(FRAMEWORK_DDL).unwrap();
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
    for table in FRAMEWORK_TABLES {
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

/// A database laid out by the checkpoint-era runtime is refused before
/// anything is written, and the refusal leaves it exactly as found
/// ([#55](https://github.com/zanminwang/axton/issues/55)).
#[test]
fn a_database_from_the_checkpoint_era_is_refused_untouched() {
    let dir = tempfile::tempdir().unwrap();
    let entry: Schema = Schema::from_value(
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap(),
    )
    .unwrap();
    let refused = |path: &std::path::Path| {
        let err = axton_client::Client::open(SqliteStore::open(path).unwrap(), entry.clone())
            .err()
            .expect("an earlier layout is refused");
        assert!(err.to_string().contains("earlier AXTON runtime"), "{err}");
    };
    let count = |path: &std::path::Path, sql: &str| -> i64 {
        let mut s = SqliteStore::open(path).unwrap();
        s.query_committed(sql, &[]).unwrap().rows[0][0]
            .as_i64()
            .unwrap()
    };

    // A checkpoint table beside the current layout.
    let checkpoint = dir.path().join("checkpoint");
    {
        let mut s = SqliteStore::open(&checkpoint).unwrap();
        s.execute_batch(FRAMEWORK_DDL).unwrap();
        s.execute_batch(
            "CREATE TABLE axton_push_checkpoint (push INTEGER NOT NULL, scope TEXT NOT NULL, cursor INTEGER NOT NULL, PRIMARY KEY (push, scope));
             INSERT INTO axton_push_checkpoint VALUES (1, 'book', 2);",
        )
        .unwrap();
    }
    refused(&checkpoint);
    assert_eq!(
        count(
            &checkpoint,
            "SELECT COUNT(*) FROM axton_push_checkpoint WHERE push = 1 AND scope = 'book' AND cursor = 2"
        ),
        1,
        "the checkpoint row is left untouched"
    );

    // An `axton_client` table without the completion column.
    let narrow = dir.path().join("narrow");
    {
        let mut s = SqliteStore::open(&narrow).unwrap();
        s.execute_batch(
            "CREATE TABLE axton_client (client_id TEXT PRIMARY KEY, next_ordinal INTEGER NOT NULL, next_push INTEGER NOT NULL, generation INTEGER NOT NULL);
             INSERT INTO axton_client VALUES ('old', 3, 2, 1);",
        )
        .unwrap();
    }
    refused(&narrow);
    assert_eq!(
        count(
            &narrow,
            "SELECT COUNT(*) FROM axton_client WHERE client_id = 'old' AND next_ordinal = 3 AND next_push = 2 AND generation = 1"
        ),
        1,
        "the client row is left untouched"
    );
    assert_eq!(
        columns(&mut SqliteStore::open(&narrow).unwrap(), "axton_client").len(),
        4,
        "no column was added"
    );

    // A database this runtime created reopens.
    let fresh = dir.path().join("fresh");
    drop(axton_client::Client::open(SqliteStore::open(&fresh).unwrap(), entry.clone()).unwrap());
    axton_client::Client::open(SqliteStore::open(&fresh).unwrap(), entry).unwrap();
}

/// A database from before the divergence column ([#122](https://github.com/zanminwang/axton/issues/122))
/// gets the column in place: its queued work stays and is still sendable.
#[test]
fn a_queue_without_the_divergence_column_gains_it_in_place() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let entry: Schema = Schema::from_value(
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap(),
    )
    .unwrap();
    {
        let mut c =
            axton_client::Client::open(SqliteStore::open(&path).unwrap(), entry.clone()).unwrap();
        c.transaction(|tx| {
            tx.enqueue(axton_client::Mutation::new(
                "Create",
                vec![axton_client::Operation {
                    model: "Entry".into(),
                    op: axton_client::OperationKind::Create,
                    identity: json!({"id":"e"}),
                    values: Some(json!({"text":"queued","note":null})),
                }],
            ))
        })
        .unwrap();
    }
    SqliteStore::open(&path)
        .unwrap()
        .execute_batch("ALTER TABLE axton_mutation DROP COLUMN diverged")
        .unwrap();
    let mut c = axton_client::Client::open(SqliteStore::open(&path).unwrap(), entry).unwrap();
    assert_eq!(c.pending_count().unwrap(), 1, "the queued mutation is kept");
    assert!(c.freeze().unwrap().is_some(), "and it is still sent");
    let mut s = SqliteStore::open(&path).unwrap();
    assert!(
        columns(&mut s, "axton_mutation")
            .iter()
            .any(|(name, _, _)| name == "diverged")
    );
}

/// A subscription ledger from before the bootstrap columns
/// ([#151](https://github.com/zanminwang/axton/issues/151)) gains them in
/// place: the identities and the committed delivery boundaries stay, and the
/// added fields read as a load that was never requested. Opening again changes
/// nothing.
#[test]
fn a_subscription_ledger_without_bootstrap_columns_gains_them_in_place() {
    use axton_client::{BootstrapPhase, Client};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let entry: Schema = Schema::from_value(
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap(),
    )
    .unwrap();
    let before = {
        let mut c = Client::open(SqliteStore::open(&path).unwrap(), entry.clone()).unwrap();
        let waiting = c.ensure_subscription("waiting").unwrap();
        let live = c.ensure_subscription("live").unwrap();
        c.initialize_subscriptions(
            &std::collections::BTreeMap::from([("live".into(), live.subscription_id)]),
            &std::collections::BTreeMap::from([("live".into(), 12)]),
        )
        .unwrap();
        (waiting.subscription_id, live.subscription_id)
    };
    let mut store = SqliteStore::open(&path).unwrap();
    for column in [
        "bootstrap_state",
        "bootstrap_run",
        "bootstrap_cursor",
        "bootstrap_barrier",
        "bootstrap_error",
    ] {
        store
            .execute_batch(&format!(
                "ALTER TABLE axton_subscription DROP COLUMN {column}"
            ))
            .unwrap();
    }
    drop(store);

    for pass in ["the first open", "the second open"] {
        let mut c = Client::open(SqliteStore::open(&path).unwrap(), entry.clone()).unwrap();
        assert!(!c.schema_state().rebuilt, "{pass}: opened in place");
        let waiting = c.subscription_state("waiting").unwrap().unwrap();
        assert_eq!(waiting.subscription_id, before.0, "{pass}");
        assert_eq!((waiting.starting_cursor, waiting.cursor), (None, None));
        let live = c.subscription_state("live").unwrap().unwrap();
        assert_eq!(live.subscription_id, before.1, "{pass}");
        assert_eq!(
            (live.starting_cursor, live.cursor),
            (Some(12), Some(12)),
            "{pass}: the committed boundary is kept"
        );
        for (scope, id) in [("waiting", before.0), ("live", before.1)] {
            let state = c.bootstrap_state(scope, id).unwrap();
            assert_eq!(state.state, BootstrapPhase::NotRequested, "{pass} {scope}");
            assert_eq!((state.run, state.cursor, state.barrier), (0, 0, None));
            assert_eq!(state.error, None);
        }
    }
    let mut store = SqliteStore::open(&path).unwrap();
    assert!(
        columns(&mut store, "axton_subscription")
            .iter()
            .any(|(name, _, _)| name == "bootstrap_state")
    );
}

/// The local layout is a public, stable contract
/// ([#184](https://github.com/zanminwang/axton/issues/184)): a Model's table
/// is named exactly the Model and its columns exactly its fields, in
/// declaration order; every other table the engine owns - the framework
/// tables and each Model's before-image table - is named `axton_*`. The
/// engine creates no view or trigger.
#[test]
fn model_tables_carry_model_and_field_names_and_every_engine_table_is_prefixed() {
    let dir = tempfile::tempdir().unwrap();
    let text = |name: &str, nullable: bool| json!({"name":name,"nullable":nullable,"type":{"kind":"scalar","name":"string"}});
    let schema = Schema::from_value(json!({"enums":[],"models":[
        {"name":"Space","identity":["id"],"fields":[text("id", false), text("title", false)]},
        {"name":"MomentPlacement","identity":["id"],"fields":[
            text("id", false), text("spaceId", false),
            {"name":"position","nullable":false,"type":{"kind":"scalar","name":"int"}},
            text("note", true)],
         "unique":[["spaceId","position"]],
         "relations":[{"name":"space","target":"Space","fields":["spaceId"],"targetFields":["id"],"onDelete":"delete"}]}
    ]}))
    .unwrap();
    let path = dir.path().join("db");
    drop(axton_client::Client::open(SqliteStore::open(&path).unwrap(), schema).unwrap());
    let mut s = SqliteStore::open(&path).unwrap();
    let names = |s: &mut SqliteStore, kind: &str| -> Vec<String> {
        s.query_committed(
            "SELECT name FROM sqlite_schema WHERE type = ? AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' ORDER BY name",
            &[json!(kind)],
        )
        .unwrap()
        .rows
        .into_iter()
        .map(|r| r[0].as_str().unwrap().to_string())
        .collect()
    };
    let tables = names(&mut s, "table");
    let models = ["MomentPlacement", "Space"];
    for model in models {
        assert!(tables.iter().any(|t| t == model), "{tables:?}");
    }
    let engine: Vec<&String> = tables
        .iter()
        .filter(|t| !models.contains(&t.as_str()))
        .collect();
    assert!(
        engine.iter().all(|t| t.starts_with("axton_")),
        "an engine table escapes the prefix: {engine:?}"
    );
    for table in FRAMEWORK_TABLES
        .iter()
        .map(|t| t.to_string())
        .chain(models.iter().map(|m| format!("axton_before_{m}")))
    {
        assert!(engine.contains(&&table), "{table} in {engine:?}");
    }
    let fields = |s: &mut SqliteStore, table: &str| -> Vec<String> {
        columns(s, table)
            .into_iter()
            .map(|(name, _, _)| name)
            .collect()
    };
    assert_eq!(fields(&mut s, "Space"), ["id", "title"]);
    assert_eq!(
        fields(&mut s, "MomentPlacement"),
        ["id", "spaceId", "position", "note"]
    );
    assert!(names(&mut s, "view").is_empty() && names(&mut s, "trigger").is_empty());
}

#[test]
fn epoch_column_migration_is_atomic_on_late_ddl_failure() {
    let dir = tempfile::tempdir().unwrap();
    let mut s = SqliteStore::open(dir.path().join("db")).unwrap();
    s.execute_batch(FRAMEWORK_DDL).unwrap();
    s.execute_batch("INSERT INTO axton_client(client_id,next_ordinal,next_push,generation,next_subscription) VALUES('client',2,1,1,2); INSERT INTO axton_mutation(ordinal,name,version) VALUES(1,'pending',1); INSERT INTO axton_subscription(stream,subscription_id,starting_cursor,cursor) VALUES('a',1,5,9); ALTER TABLE axton_client DROP COLUMN store_epoch; ALTER TABLE axton_mutation DROP COLUMN store_epoch; ALTER TABLE axton_load DROP COLUMN store_epoch; ALTER TABLE axton_load RENAME TO old_load; CREATE VIEW axton_load AS SELECT * FROM old_load;").unwrap();
    assert!(axton_client::ddl::add_framework_columns(&mut s).is_err());
    assert!(
        !columns(&mut s, "axton_client")
            .iter()
            .any(|(n, _, _)| n == "store_epoch"),
        "earlier ALTER must roll back"
    );
    assert!(
        !columns(&mut s, "axton_mutation")
            .iter()
            .any(|(n, _, _)| n == "store_epoch")
    );
    assert_eq!(
        s.query_committed("SELECT ordinal,name FROM axton_mutation", &[])
            .unwrap()
            .rows,
        vec![vec![json!(1), json!("pending")]]
    );
    assert_eq!(
        s.query_committed(
            "SELECT subscription_id,starting_cursor,cursor FROM axton_subscription",
            &[]
        )
        .unwrap()
        .rows,
        vec![vec![json!(1), json!(5), json!(9)]]
    );
    s.execute_batch("DROP VIEW axton_load; ALTER TABLE old_load RENAME TO axton_load;")
        .unwrap();
    axton_client::ddl::add_framework_columns(&mut s).unwrap();
    drop(s);
    let mut s = SqliteStore::open(dir.path().join("db")).unwrap();
    axton_client::ddl::add_framework_columns(&mut s).unwrap();
    assert_eq!(
        s.query_committed("SELECT store_epoch FROM axton_client", &[])
            .unwrap()
            .rows,
        vec![vec![json!(0)]]
    );
    assert_eq!(
        s.query_committed("SELECT ordinal,store_epoch FROM axton_mutation", &[])
            .unwrap()
            .rows,
        vec![vec![json!(1), json!(0)]]
    );
    assert_eq!(
        s.query_committed(
            "SELECT subscription_id,starting_cursor,cursor FROM axton_subscription",
            &[]
        )
        .unwrap()
        .rows,
        vec![vec![json!(1), json!(5), json!(9)]]
    );
}
