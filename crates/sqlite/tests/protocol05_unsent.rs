//! Unsent work account-wide: refused acts keep the act as submitted, failed
//! acts list their failed tasks, a new requirement on a failed task inherits
//! the failure, and every resolution also runs inside a transaction
//! ([#186](https://github.com/zanminwang/axton/issues/186),
//! [#205](https://github.com/zanminwang/axton/issues/205),
//! [#204](https://github.com/zanminwang/axton/issues/204)).
use axton_client::*;
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};

/// `Write` edits a Note; its `blob` field requires `RemoteBlob(key: self)`,
/// and a later `Write` of the same Note is sequenced after an earlier one.
/// `Create` makes a Note.
fn schema() -> Schema {
    Schema::from_value(json!({"enums":[],
        "models":[{"name":"Note","version":1,"identity":["id"],"fields":[
            {"name":"id","type":{"kind":"scalar","name":"string"},"nullable":false},
            {"name":"text","type":{"kind":"scalar","name":"string"},"nullable":false},
            {"name":"blob","type":{"kind":"scalar","name":"string"},"nullable":true}]}],
        "prerequisites":[{"name":"RemoteBlob","fields":[{"name":"key","type":"String"}]}],
        "actions":[
            {"name":"Write","version":1,"outputs":[],
             "inputs":[{"kind":"model","name":"note","model":"Note","operation":"update","cardinality":"single"}],
             "requirements":[{"model":"Note","field":"blob","name":"RemoteBlob","arguments":{"key":"self"}}],
             "sequence":{"after":[{"name":"Write","arguments":{"note":"note"}}]}},
            {"name":"Create","version":1,"outputs":[],
             "inputs":[{"kind":"model","name":"note","model":"Note","operation":"create","cardinality":"single"}]}
        ]}))
    .unwrap()
}

fn open(path: &std::path::Path) -> Client<SqliteStore> {
    Client::open05(SqliteStore::open(path).unwrap(), schema(), "User:u").unwrap()
}

/// A client with Note `n` at text `base`, device-only.
fn seeded(dir: &tempfile::TempDir) -> Client<SqliteStore> {
    let mut client = open(&dir.path().join("db"));
    client
        .transaction(|tx| {
            tx.direct(Operation {
                model: "Note".into(),
                op: OperationKind::Create,
                identity: json!({"id":"n"}),
                values: Some(json!({"text":"base","blob":null})),
            })
        })
        .unwrap();
    client
}

fn write(client: &mut Client<SqliteStore>, text: &str, blob: Option<&str>) -> SubmittedCall {
    client
        .transaction(|tx| {
            tx.submit_mutation05(
                "Write",
                1,
                json!({"note":{"id":"n","text":text,"blob":blob}}),
                vec![],
            )
        })
        .unwrap()
}

fn blob(key: &str) -> String {
    json!({"arguments":{"key":key},"name":"RemoteBlob"}).to_string()
}

fn text(client: &mut Client<SqliteStore>) -> Value {
    client
        .read(&RecordKey {
            model: "Note".into(),
            identity: json!({"id":"n"}),
        })
        .unwrap()
        .unwrap()["text"]
        .clone()
}

fn ordinals(acts: &[FailedAct]) -> Vec<u64> {
    acts.iter().map(|a| a.ordinal).collect()
}

fn count(client: &mut Client<SqliteStore>, sql: &str) -> u64 {
    client.read_sql(sql, &[]).unwrap()[0]["n"].as_u64().unwrap()
}

#[test]
fn a_replacement_in_the_transaction_that_discards_the_original_is_planned_without_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = seeded(&dir);
    let original = write(&mut client, "draft", Some("X"));
    client.outcome(&blob("X"), Some("upload refused")).unwrap();
    assert_eq!(text(&mut client), "draft");
    let (seen, replacement) = client
        .transaction(|tx| {
            let completions = tx.discard_mutation05(original.ordinal)?;
            assert_eq!(completions.completions[0].call_id, original.call_id);
            let seen = tx
                .read(&RecordKey {
                    model: "Note".into(),
                    identity: json!({"id":"n"}),
                })?
                .unwrap()["text"]
                .clone();
            let replacement = tx.submit_mutation05(
                "Write",
                1,
                json!({"note":{"id":"n","text":"fixed","blob":null}}),
                vec![],
            )?;
            Ok((seen, replacement))
        })
        .unwrap();
    assert_eq!(seen, "base", "planned over the base, not the original");
    assert_eq!(text(&mut client), "fixed");
    assert_eq!(
        count(
            &mut client,
            "SELECT COUNT(*) AS n FROM axton_mutation_dependency"
        ),
        0,
        "not sequenced after the original"
    );
    assert!(client.refused_acts05().unwrap().is_empty());
    assert!(client.failed_acts().unwrap().is_empty());
    assert!(client.pending_tasks().unwrap().is_empty());
    let batch = client.freeze_batch05().unwrap().unwrap();
    assert_eq!(batch.mutations.len(), 1);
    assert_eq!(batch.mutations[0].id, replacement.ordinal);
}
#[test]
fn a_failure_after_the_discard_rolls_both_back_and_the_original_is_intact() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = seeded(&dir);
    let original = write(&mut client, "draft", Some("X"));
    client.outcome(&blob("X"), Some("upload refused")).unwrap();
    let failed = client
        .transaction(|tx| {
            tx.discard_mutation05(original.ordinal)?;
            tx.submit_mutation05(
                "Write",
                1,
                json!({"note":{"id":"n","text":"fixed","blob":null}}),
                vec![],
            )?;
            tx.dismiss_rejection05(1)?;
            tx.retry_tasks(&[blob("X")])?;
            Err::<(), _>(invalid("the author cancelled"))
        })
        .unwrap_err();
    assert!(failed.to_string().contains("the author cancelled"));
    assert_eq!(
        text(&mut client),
        "draft",
        "the original's optimism is back"
    );
    assert_eq!(
        ordinals(&client.failed_acts().unwrap()),
        vec![original.ordinal]
    );
    assert_eq!(client.pending_count().unwrap(), 1);
    assert!(client.refused_acts05().unwrap().is_empty());
    // A savepoint rollback undoes only the resolutions made in it.
    client
        .transaction(|tx| {
            let _ = tx.savepoint(|tx| {
                tx.discard_mutation05(original.ordinal)?;
                Err::<(), _>(invalid("undo"))
            });
            tx.retry_tasks(&[blob("X")])
        })
        .unwrap();
    assert_eq!(client.pending_count().unwrap(), 1);
    assert!(
        client.failed_acts().unwrap().is_empty(),
        "the retry committed"
    );
}

#[test]
fn in_one_transaction_a_replacement_submitted_after_the_drop_starts_afresh_but_one_submitted_before_it_inherits_the_failure()
 {
    let submit = |tx: &mut ClientTransaction<'_, SqliteStore>, text: &str| {
        tx.submit_mutation05(
            "Write",
            1,
            json!({"note":{"id":"n","text":text,"blob":"X"}}),
            vec![],
        )
    };
    // Drop first: the replacement's task is pending, and a handler runs it.
    let dir = tempfile::tempdir().unwrap();
    let mut client = seeded(&dir);
    let original = write(&mut client, "draft", Some("X"));
    client.outcome(&blob("X"), Some("upload refused")).unwrap();
    client
        .transaction(|tx| {
            tx.discard_mutation05(original.ordinal)?;
            submit(tx, "fixed")
        })
        .unwrap();
    assert!(client.failed_acts().unwrap().is_empty());
    assert_eq!(client.pending_tasks().unwrap()[0]["state"], "pending");
    assert_eq!(client.pending_count().unwrap(), 1);
    // Submit first, then drop: the replacement joined the failed task and
    // keeps its failure after the original is gone.
    let dir = tempfile::tempdir().unwrap();
    let mut client = seeded(&dir);
    let original = write(&mut client, "draft", Some("X"));
    client.outcome(&blob("X"), Some("upload refused")).unwrap();
    let late = client
        .transaction(|tx| {
            let call = submit(tx, "fixed")?;
            tx.discard_mutation05(original.ordinal)?;
            Ok(call)
        })
        .unwrap();
    assert_eq!(ordinals(&client.failed_acts().unwrap()), vec![late.ordinal]);
    assert_eq!(client.pending_tasks().unwrap()[0]["state"], "failed");
}

/// A call whose row was written before failures were inherited, with a NULL
/// error beside a failed row of the same key, is still listed: the key
/// decides, not the row.
#[test]
fn a_call_written_before_inheritance_is_listed_by_its_failed_key() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut client = seeded(&dir);
    let first = write(&mut client, "one", Some("X"));
    client.outcome(&blob("X"), Some("upload refused")).unwrap();
    let second = write(&mut client, "two", Some("X"));
    drop(client);
    let mut store = SqliteStore::open(&path).unwrap();
    store
        .execute(
            "UPDATE axton_mutation_prerequisite SET error=NULL WHERE ordinal=?",
            &[json!(second.ordinal)],
        )
        .unwrap();
    drop(store);
    let mut client = open(&path);
    assert_eq!(
        ordinals(&client.failed_acts().unwrap()),
        vec![first.ordinal, second.ordinal]
    );
    // A call waiting on no failed key is not read at all.
    write(&mut client, "three", None);
    assert_eq!(client.failed_acts().unwrap().len(), 2);
}
