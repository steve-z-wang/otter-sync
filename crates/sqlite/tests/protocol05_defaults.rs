use axton_client::*;
use axton_sqlite::SqliteStore;
use serde_json::{Map, Value, json};
fn todo_fields() -> Vec<Value> {
    vec![
        json!({"name":"id","type":{"kind":"scalar","name":"string"},"nullable":false,"createDefault":{"kind":"uuid"}}),
        json!({"name":"title","type":{"kind":"scalar","name":"string"},"nullable":false,"createDefault":{"kind":"literal","value":""}}),
        json!({"name":"done","type":{"kind":"scalar","name":"boolean"},"nullable":false,"createDefault":{"kind":"literal","value":false}}),
        json!({"name":"priority","type":{"kind":"scalar","name":"int"},"nullable":false,"createDefault":{"kind":"literal","value":0}}),
        json!({"name":"status","type":{"kind":"enum","name":"Status"},"nullable":false,"createDefault":{"kind":"literal","value":"open"}}),
        json!({"name":"createdAt","type":{"kind":"scalar","name":"dateTime"},"nullable":false,"createDefault":{"kind":"now"}}),
        json!({"name":"note","type":{"kind":"scalar","name":"string"},"nullable":true,"createDefault":{"kind":"literal","value":"n"}}),
        json!({"name":"memo","type":{"kind":"scalar","name":"string"},"nullable":true}),
    ]
}
fn create(name: &str, input: &str, cardinality: &str) -> Value {
    json!({"name":name,"version":1,"inputs":[{"kind":"model","name":input,"model":"Todo","operation":"create","cardinality":cardinality}],"outputs":[]})
}
fn schema() -> Schema {
    // A retained operation whose input contract predates `note`: its
    // current default must not be injected into that contract.
    let mut old_fields = todo_fields();
    old_fields.retain(|f| f["name"] != "note");
    for f in &mut old_fields {
        f.as_object_mut().unwrap().remove("createDefault");
        // A field the old contract shaped differently: the current Int
        // default does not belong to it.
        if f["name"] == "priority" {
            *f =
                json!({"name":"priority","type":{"kind":"scalar","name":"string"},"nullable":true});
        }
    }
    let mut old = create("AddOld", "todo", "single");
    old["input"] = json!({"models":[{"name":"Todo","version":1,"identity":["id"],"fields":old_fields}],"enums":[{"name":"Status","values":["open","closed"]}]});
    Schema::from_value(json!({
        "enums":[{"name":"Status","values":["open","closed"]}],
        "models":[{"name":"Todo","version":1,"identity":["id"],"fields":todo_fields()}],
        "actions":[
            create("Add", "todo", "single"),
            create("AddMaybe", "todo", "optional"),
            create("AddMany", "todos", "list"),
            old,
            {"name":"Edit","version":1,"inputs":[{"kind":"model","name":"todo","model":"Todo","operation":"update","cardinality":"single"}],"outputs":[]},
        ]
    }))
    .unwrap()
}
fn open(path: &std::path::Path) -> Client<SqliteStore> {
    Client::open05(SqliteStore::open(path).unwrap(), schema(), "User:u").unwrap()
}
fn rows(client: &mut Client<SqliteStore>) -> Vec<Value> {
    client.query("Todo", &json!({})).unwrap()
}
fn local_create(values: Value) -> Operation {
    Operation {
        model: "Todo".into(),
        op: OperationKind::Create,
        identity: json!({}),
        values: Some(values),
    }
}
fn assert_uuid_v4(value: &Value) -> String {
    let text = value.as_str().expect("generated id is text").to_string();
    let parsed = uuid::Uuid::parse_str(&text).unwrap();
    assert_eq!(parsed.get_version_num(), 4, "{text}");
    assert_eq!(text, parsed.hyphenated().to_string(), "canonical lowercase");
    text
}
fn assert_client_millis(value: &Value) {
    let text = value.as_str().expect("generated time is text");
    assert_eq!(text.len(), 24, "{text}");
    assert!(text.ends_with('Z') && text.as_bytes()[19] == b'.', "{text}");
    let at = chrono::DateTime::parse_from_rfc3339(text).unwrap();
    let skew = (chrono::Utc::now() - at.with_timezone(&chrono::Utc))
        .num_seconds()
        .abs();
    assert!(skew < 60, "client wall clock: {text}");
}
fn only(object: &Value, keys: &[&str]) -> Map<String, Value> {
    keys.iter()
        .map(|k| (k.to_string(), object[*k].clone()))
        .collect()
}

#[test]
fn local_create_fills_only_omitted_fields_and_each_create_generates_anew() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    client
        .transaction(|tx| tx.direct(local_create(json!({"title":"explicit"}))))
        .unwrap();
    client
        .transaction(|tx| tx.direct(local_create(json!({"note":null,"priority":7}))))
        .unwrap();
    let mut all = rows(&mut client);
    all.sort_by_key(|r| r["title"].as_str().unwrap().to_string());
    let (blank, explicit) = (&all[0], &all[1]);
    let first = assert_uuid_v4(&explicit["id"]);
    let second = assert_uuid_v4(&blank["id"]);
    assert_ne!(first, second, "a second create generates its own id");
    assert_client_millis(&explicit["createdAt"]);
    assert_eq!(
        only(
            explicit,
            &["title", "done", "priority", "status", "note", "memo"]
        ),
        only(
            &json!({"title":"explicit","done":false,"priority":0,"status":"open","note":"n","memo":null}),
            &["title", "done", "priority", "status", "note", "memo"]
        )
    );
    assert_eq!(
        blank["title"], "",
        "a literal default fills the omitted field"
    );
    assert_eq!(blank["priority"], 7, "an explicit value wins");
    assert_eq!(
        blank["note"],
        Value::Null,
        "explicit nullable null stays null"
    );
    // An explicit null never requests a default for a required field.
    let err = client
        .transaction(|tx| tx.direct(local_create(json!({"done":null}))))
        .unwrap_err();
    assert!(err.to_string().contains("not nullable"), "{err}");
    // A supplied identity is kept (and normalized as before).
    client
        .transaction(|tx| {
            tx.direct(Operation {
                model: "Todo".into(),
                op: OperationKind::Create,
                identity: json!({"id":"mine"}),
                values: Some(json!({})),
            })
        })
        .unwrap();
    assert!(rows(&mut client).iter().any(|r| r["id"] == "mine"));
    // An identity key in the state is misplaced, not a reason to generate one.
    assert!(
        client
            .transaction(|tx| tx.direct(local_create(json!({"id":"misplaced"}))))
            .is_err()
    );
    assert_eq!(rows(&mut client).len(), 3);
}

#[test]
fn a_rolled_back_transaction_keeps_no_generated_record() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    let result: Result<()> = client.transaction(|tx| {
        tx.direct(local_create(json!({})))?;
        assert_eq!(tx.query("Todo", &json!({}))?.len(), 1, "visible inside");
        Err(axton_core::invalid("abort"))
    });
    assert!(result.is_err());
    assert!(rows(&mut client).is_empty());
    // A failing nested scope rolls back only its own create.
    client
        .transaction(|tx| {
            tx.direct(local_create(json!({"title":"kept"})))?;
            let nested: Result<()> = tx.savepoint(|inner| {
                inner.direct(local_create(json!({"title":"dropped"})))?;
                Err(axton_core::invalid("abort"))
            });
            assert!(nested.is_err());
            Ok(())
        })
        .unwrap();
    let all = rows(&mut client);
    assert_eq!(all.len(), 1);
    assert_eq!(all[0]["title"], "kept");
}

#[test]
fn update_omission_changes_nothing_and_never_evaluates_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"));
    client
        .transaction(|tx| {
            tx.direct(Operation {
                model: "Todo".into(),
                op: OperationKind::Create,
                identity: json!({"id":"t"}),
                values: Some(json!({"title":"a","done":true,"priority":5,"status":"closed","createdAt":"2020-01-01T00:00:00.000Z","note":"x"})),
            })
        })
        .unwrap();
    client
        .transaction(|tx| {
            tx.direct(Operation {
                model: "Todo".into(),
                op: OperationKind::Update,
                identity: json!({"id":"t"}),
                values: Some(json!({"title":"b"})),
            })
        })
        .unwrap();
    let row = &rows(&mut client)[0];
    assert_eq!(
        row,
        &json!({"id":"t","title":"b","done":true,"priority":5,"status":"closed","createdAt":"2020-01-01T00:00:00.000Z","note":"x","memo":null})
    );
    // An update without identity is refused, never given a generated one.
    let err = client
        .transaction(|tx| {
            tx.direct(Operation {
                model: "Todo".into(),
                op: OperationKind::Update,
                identity: json!({}),
                values: Some(json!({"title":"c"})),
            })
        })
        .unwrap_err();
    assert!(err.to_string().contains("identity"), "{err}");
    client
        .transaction(|tx| {
            tx.submit_mutation05("Edit", 1, json!({"todo":{"id":"t","title":"d"}}), vec![])
        })
        .unwrap();
    let batch = client.freeze_batch05().unwrap().unwrap();
    assert_eq!(
        v05::reconstruct_input(&batch.mutations[0].operations).unwrap(),
        json!({"todo":{"id":"t","title":"d"}})
    );
}

fn prepared(name: &str, args: Value) -> Result<Value> {
    let d = tempfile::tempdir().unwrap();
    let mut c = open(&d.path().join("db"));
    c.transaction(|tx| tx.submit_mutation05(name, 1, args, vec![]))?;
    let b = c.freeze_batch05()?.unwrap();
    v05::reconstruct_input(&b.mutations[0].operations)
}
#[test]
fn optional_and_list_create_defaults_preserve_input_presence() {
    assert_eq!(prepared("AddMaybe", json!({})).unwrap(), json!({}));
    assert_eq!(
        prepared("AddMaybe", json!({"todo":null})).unwrap(),
        json!({"todo":null})
    );
    let present = prepared("AddMaybe", json!({"todo":{}})).unwrap();
    assert_uuid_v4(&present["todo"]["id"]);
    let many = prepared("AddMany", json!({"todos":[{},{"title":"b","id":"given"}]})).unwrap();
    let items = many["todos"].as_array().unwrap();
    assert_uuid_v4(&items[0]["id"]);
    assert_eq!(items[0]["title"], "");
    assert_eq!(items[1]["id"], "given");
    assert_eq!(items[1]["status"], "open");
    assert_eq!(
        prepared("AddMany", json!({"todos":[]})).unwrap(),
        json!({"todos":[]})
    );
    assert!(prepared("Add", json!({})).is_err());
}
#[test]
fn durable_create_and_local_companion_defaults_expand_once_before_freeze() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut c = open(&p);
    c.transaction(|tx| {
        tx.submit_mutation05(
            "Add",
            1,
            json!({"todo":{"title":"sent"}}),
            vec![local_create(json!({"title":"local"}))],
        )
    })
    .unwrap();
    let b = c.freeze_batch05().unwrap().unwrap();
    let args = v05::reconstruct_input(&b.mutations[0].operations).unwrap();
    let sent = assert_uuid_v4(&args["todo"]["id"]);
    assert_client_millis(&args["todo"]["createdAt"]);
    assert_eq!(args["todo"]["done"], false);
    let mut visible = rows(&mut c);
    visible.sort_by_key(|r| r["title"].as_str().unwrap().to_string());
    let local = assert_uuid_v4(&visible[0]["id"]);
    assert_ne!(local, sent);
    assert_client_millis(&visible[0]["createdAt"]);
    assert_eq!(visible[1], args["todo"]);
    assert!(
        !String::from_utf8(v05::encode(&b).unwrap())
            .unwrap()
            .contains(&local)
    );
    drop(c);
    let mut c = open(&p);
    assert_eq!(c.freeze_batch05().unwrap().unwrap(), b);
    let mut reopened = rows(&mut c);
    reopened.sort_by_key(|r| r["title"].as_str().unwrap().to_string());
    assert_eq!(reopened, visible);
}
