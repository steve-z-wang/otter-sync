//! Abrupt process exits at the local Mutation/companion commit boundary.
use axton_client::{Client, Operation, OperationKind, RecordKey, Schema};
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
fn open(path: &std::path::Path) -> Client<SqliteStore> {
    let schema = Schema::from_value(json!({"enums":[],"models":[{"name":"Entry","identity":["id"],"fields":[{"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"}},{"name":"text","nullable":false,"type":{"kind":"scalar","name":"string"}}]}],"actions":[{"name":"Publish","version":1,"inputs":[{"kind":"model","name":"entry","model":"Entry","operation":"create","cardinality":"single"}],"outputs":[]}]})).unwrap();
    Client::open05(SqliteStore::open(path).unwrap(), schema, "User:u").unwrap()
}
fn key(id: &str) -> RecordKey {
    RecordKey {
        model: "Entry".into(),
        identity: json!({"id":id}),
    }
}
fn draft() -> Operation {
    Operation {
        model: "Entry".into(),
        op: OperationKind::Create,
        identity: json!({"id":"draft"}),
        values: Some(json!({"text":"local"})),
    }
}
#[test]
#[ignore = "child entry invoked and killed by the two interruption tests"]
fn child_process_entry() {
    let Ok(path) = std::env::var("AXTON_LOCAL05_CRASH_PATH") else {
        return;
    };
    let mut c = open(std::path::Path::new(&path));
    c.begin_session().unwrap();
    c.session(|tx| {
        tx.submit_mutation05(
            "Publish",
            1,
            json!({"entry":{"id":"p","text":"published"}}),
            vec![Operation {
                model: "Entry".into(),
                op: OperationKind::Delete,
                identity: json!({"id":"draft"}),
                values: None,
            }],
        )
    })
    .unwrap();
    if std::env::var("AXTON_LOCAL05_CRASH_COMMIT").unwrap() == "yes" {
        c.commit_session().unwrap();
    }
    println!("local05-ready");
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    std::io::stdin().read_line(&mut line).unwrap();
    panic!("parent must kill the parked child");
}
fn interrupted(committed: bool) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open(&path);
    c.transaction(|tx| tx.direct(draft())).unwrap();
    drop(c);
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "child_process_entry", "--ignored", "--nocapture"])
        .env("AXTON_LOCAL05_CRASH_PATH", &path)
        .env(
            "AXTON_LOCAL05_CRASH_COMMIT",
            if committed { "yes" } else { "no" },
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut out = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    loop {
        line.clear();
        assert_ne!(
            out.read_line(&mut line).unwrap(),
            0,
            "child exited before boundary"
        );
        if line.trim() == "local05-ready" {
            break;
        }
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let mut c = open(&path);
    assert_eq!(c.read(&key("draft")).unwrap().is_none(), committed);
    assert_eq!(c.read(&key("p")).unwrap().is_some(), committed);
    assert_eq!(c.pending_count().unwrap(), usize::from(committed));
    let batch = c.freeze_batch05().unwrap();
    if committed {
        let batch = batch.unwrap();
        assert_eq!(batch.mutations.len(), 1);
        let bytes = axton_client::v05::encode(&batch).unwrap();
        assert!(!String::from_utf8(bytes).unwrap().contains("draft"));
        let rows = c
            .read_sql(
                "SELECT kind FROM axton_mutation_operation ORDER BY position",
                &[],
            )
            .unwrap();
        assert_eq!(
            rows,
            vec![json!({"kind":"companion"}), json!({"kind":"wire"})]
        );
    } else {
        assert!(batch.is_none());
        assert!(
            c.read_sql("SELECT * FROM axton_mutation_operation", &[])
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            c.read_sql("SELECT next_mutation_id AS n FROM axton_store", &[])
                .unwrap()[0]["n"],
            Value::from(1)
        );
    }
}
#[test]
fn killed_before_local_commit_keeps_no_call_companion_or_allocated_identity() {
    interrupted(false)
}
#[test]
fn killed_after_local_commit_keeps_call_and_companion_without_sending_companion() {
    interrupted(true)
}
