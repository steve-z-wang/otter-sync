//! Create-only Model defaults are materialized once by the client before a
//! fresh create is persisted or sent, and never anywhere else
//! ([#27](https://github.com/zanminwang/axton/issues/27)).
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
    Client::open(SqliteStore::open(path).unwrap(), schema()).unwrap()
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
    // A Mutation update operand is not filled either.
    client
        .submit_action("Edit", 1, json!({"todo":{"id":"t","title":"d"}}))
        .unwrap();
    let args: Value = serde_json::from_str(
        client
            .read_sql("SELECT args FROM axton_mutation", &[])
            .unwrap()[0]["args"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(args, json!({"todo":{"id":"t","title":"d"}}));
}

#[test]
fn durable_create_persists_expanded_args_that_optimism_frozen_bytes_and_reopen_share() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut client = open(&path);
    let first = client
        .submit_action("Add", 1, json!({"todo":{"title":"a"}}))
        .unwrap();
    client
        .submit_action("Add", 1, json!({"todo":{"title":"b"}}))
        .unwrap();
    let stored = client
        .read_sql(
            "SELECT call_id, args FROM axton_mutation ORDER BY ordinal",
            &[],
        )
        .unwrap();
    let args: Value = serde_json::from_str(stored[0]["args"].as_str().unwrap()).unwrap();
    let second: Value = serde_json::from_str(stored[1]["args"].as_str().unwrap()).unwrap();
    assert_eq!(stored[0]["call_id"], first.call_id);
    let todo = &args["todo"];
    let id = assert_uuid_v4(&todo["id"]);
    assert_client_millis(&todo["createdAt"]);
    assert_ne!(second["todo"]["id"], todo["id"]);
    assert_eq!(
        todo,
        &json!({"id":id,"title":"a","done":false,"priority":0,"status":"open","createdAt":todo["createdAt"],"note":"n","memo":null})
    );
    // The optimistic row is exactly the expanded operand.
    let row = rows(&mut client)
        .into_iter()
        .find(|r| r["id"] == todo["id"])
        .unwrap();
    assert_eq!(&row, todo);
    let bytes = client.freeze().unwrap().unwrap();
    let request = PushRequest::decode_actions(&bytes, &schema()).unwrap();
    assert_eq!(request.mutations[0].raw["args"], args);
    // Reopening, rebuilding the frozen batch and retrying generate nothing.
    drop(client);
    let mut client = open(&path);
    assert_eq!(client.freeze().unwrap().unwrap(), bytes);
    let again: Value = serde_json::from_str(
        client
            .read_sql("SELECT args FROM axton_mutation ORDER BY ordinal", &[])
            .unwrap()[0]["args"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(again, args);
    assert_eq!(
        rows(&mut client)
            .into_iter()
            .find(|r| r["id"] == todo["id"])
            .unwrap(),
        row
    );
}

#[test]
fn direct_create_request_carries_generated_values_once() {
    let dir = tempfile::tempdir().unwrap();
    let client = open(&dir.path().join("db"));
    let prepared = client
        .prepare_action("Add", 1, json!({"todo":{"status":"closed"}}))
        .unwrap();
    let todo = &prepared.call.args["todo"];
    assert_uuid_v4(&todo["id"]);
    assert_client_millis(&todo["createdAt"]);
    assert_eq!(todo["status"], "closed");
    assert_eq!(todo["title"], "");
    // The encoded body is what transport retries resend; decoding it on the
    // server side yields exactly these values.
    let body = prepared.encode().unwrap();
    let decoded = DirectActionRequest::decode(&body, &schema()).unwrap();
    assert_eq!(decoded.call.args, prepared.call.args);
    let other = client
        .prepare_action("Add", 1, json!({"todo":{"status":"closed"}}))
        .unwrap();
    assert_ne!(other.call.args["todo"]["id"], todo["id"]);
}

#[test]
fn optional_and_list_create_operands_fill_each_present_item() {
    let dir = tempfile::tempdir().unwrap();
    let client = open(&dir.path().join("db"));
    let absent = client.prepare_action("AddMaybe", 1, json!({})).unwrap();
    assert_eq!(absent.call.args, json!({"todo":null}));
    let null = client
        .prepare_action("AddMaybe", 1, json!({"todo":null}))
        .unwrap();
    assert_eq!(null.call.args, json!({"todo":null}));
    let present = client
        .prepare_action("AddMaybe", 1, json!({"todo":{}}))
        .unwrap();
    assert_uuid_v4(&present.call.args["todo"]["id"]);
    let many = client
        .prepare_action(
            "AddMany",
            1,
            json!({"todos":[{},{"title":"b","id":"given"}]}),
        )
        .unwrap();
    let items = many.call.args["todos"].as_array().unwrap();
    assert_uuid_v4(&items[0]["id"]);
    assert_eq!(items[0]["title"], "");
    assert_eq!(items[1]["id"], "given");
    assert_eq!(items[1]["title"], "b");
    assert_eq!(items[1]["status"], "open");
    let empty = client
        .prepare_action("AddMany", 1, json!({"todos":[]}))
        .unwrap();
    assert_eq!(empty.call.args, json!({"todos":[]}));
    // A required create operand stays required.
    assert!(client.prepare_action("Add", 1, json!({})).is_err());
}

#[test]
fn a_retained_input_contract_only_receives_defaults_for_its_own_fields() {
    let dir = tempfile::tempdir().unwrap();
    let client = open(&dir.path().join("db"));
    let prepared = client
        .prepare_action("AddOld", 1, json!({"todo":{}}))
        .unwrap();
    let todo = prepared.call.args["todo"].as_object().unwrap();
    assert!(!todo.contains_key("note"), "{todo:?}");
    assert_uuid_v4(&todo["id"]);
    assert_eq!(todo["title"], "");
    // Same name, different shape: the current default is not injected.
    assert_eq!(todo["priority"], Value::Null, "{todo:?}");
}

#[test]
fn low_level_enqueue_create_stores_concrete_values_before_replay() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut client = open(&path);
    client
        .transaction(|tx| tx.enqueue(Mutation::new("AddTodo", vec![local_create(json!({}))])))
        .unwrap();
    let stored = client
        .read_sql("SELECT identity FROM axton_mutation_operation", &[])
        .unwrap();
    let identity: Value = serde_json::from_str(stored[0]["identity"].as_str().unwrap()).unwrap();
    let id = assert_uuid_v4(&identity["id"]);
    let row = rows(&mut client)[0].clone();
    assert_eq!(row["id"], id);
    assert_client_millis(&row["createdAt"]);
    let bytes = client.freeze().unwrap().unwrap();
    drop(client);
    let mut client = open(&path);
    assert_eq!(client.freeze().unwrap().unwrap(), bytes);
    assert_eq!(rows(&mut client)[0], row);
}

/// A Mutation submitted inside a transaction and its local companion create
/// are both fresh creates: each omitted field is generated once, before the
/// call and the companion are stored, and nothing is generated again on
/// freeze, reopen or retry.
#[test]
fn a_transaction_mutation_and_its_companion_create_generate_values_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut client = open(&path);
    let call = client
        .transaction(|tx| {
            let call = tx.submit_mutation(
                "Add",
                1,
                json!({"todo":{"title":"sent"}}),
                ActionCallOptions::default(),
            )?;
            tx.append_companion(call.ordinal, local_create(json!({"title":"local"})))?;
            Ok(call)
        })
        .unwrap();
    let stored = client
        .read_sql("SELECT call_id, args FROM axton_mutation", &[])
        .unwrap();
    assert_eq!(stored[0]["call_id"], call.call_id);
    let args: Value = serde_json::from_str(stored[0]["args"].as_str().unwrap()).unwrap();
    let todo = &args["todo"];
    let sent = assert_uuid_v4(&todo["id"]);
    assert_client_millis(&todo["createdAt"]);
    assert_eq!(todo["done"], false);
    let companion = client
        .read_sql(
            "SELECT identity, \"values\" FROM axton_mutation_operation WHERE kind='companion'",
            &[],
        )
        .unwrap();
    assert_eq!(companion.len(), 1);
    let identity: Value = serde_json::from_str(companion[0]["identity"].as_str().unwrap()).unwrap();
    let local = assert_uuid_v4(&identity["id"]);
    assert_ne!(local, sent, "each create generates its own id");
    let values: Value = serde_json::from_str(companion[0]["values"].as_str().unwrap()).unwrap();
    assert_client_millis(&values["createdAt"]);
    assert_eq!(values["status"], "open");
    // The visible rows are exactly the stored values.
    let mut visible = rows(&mut client);
    visible.sort_by_key(|r| r["title"].as_str().unwrap().to_string());
    assert_eq!(&visible[1], todo);
    assert_eq!(visible[0]["id"], local);
    assert_eq!(visible[0]["createdAt"], values["createdAt"]);
    // Only the expanded business args are sent, the same bytes after reopen.
    let bytes = client.freeze().unwrap().unwrap();
    let request = PushRequest::decode_actions(&bytes, &schema()).unwrap();
    assert_eq!(request.mutations[0].raw["args"], args);
    assert!(!String::from_utf8(bytes.clone()).unwrap().contains(&local));
    drop(client);
    let mut client = open(&path);
    assert_eq!(client.freeze().unwrap().unwrap(), bytes);
    let mut again = rows(&mut client);
    again.sort_by_key(|r| r["title"].as_str().unwrap().to_string());
    assert_eq!(again, visible);
}

#[test]
fn servers_and_loaders_never_synthesize_defaults() {
    let schema = schema();
    let complete = json!({"id":"t","title":"a","done":false,"priority":0,"status":"open","createdAt":"2020-01-01T00:00:00.000Z","note":null,"memo":null});
    let mut missing = complete.clone();
    missing.as_object_mut().unwrap().remove("done");
    let body = |todo: &Value| {
        json!({"call":{"callId":"0190f4c8-0000-7000-8000-000000000001","name":"Add","version":1,"args":{"todo":todo}},"models":{"Todo":1}})
            .to_string()
    };
    DirectActionRequest::decode(body(&complete).as_bytes(), &schema).unwrap();
    let err = DirectActionRequest::decode(body(&missing).as_bytes(), &schema).unwrap_err();
    assert!(
        err.to_string().contains("missing state field done"),
        "{err}"
    );
    let mut no_id = complete.clone();
    no_id.as_object_mut().unwrap().remove("id");
    assert!(DirectActionRequest::decode(body(&no_id).as_bytes(), &schema).is_err());
    // A Loader record missing a required field is malformed, default or not.
    let mut state = missing.clone();
    state.as_object_mut().unwrap().remove("id");
    assert!(schema.normalize_state("Todo", &state).is_err());
    assert!(schema.validate_state("Todo", &state).is_err());
}

fn open_at(path: &std::path::Path, schema: Schema) -> Client<SqliteStore> {
    Client::open_at(path, schema, Box::new(|p| SqliteStore::open(p)), false).unwrap()
}

fn with_fields(edit: impl FnOnce(&mut Vec<Value>)) -> Schema {
    let mut value = serde_json::to_value(schema()).unwrap();
    edit(value["models"][0]["fields"].as_array_mut().unwrap());
    Schema::from_value(value).unwrap()
}

#[test]
fn a_default_only_change_opens_in_place_and_rewrites_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut client = open_at(&path, schema());
    client
        .submit_action("Add", 1, json!({"todo":{"title":"queued"}}))
        .unwrap();
    client
        .transaction(|tx| tx.direct(local_create(json!({"title":"local"}))))
        .unwrap();
    let before = rows(&mut client);
    let args = client
        .read_sql("SELECT args FROM axton_mutation", &[])
        .unwrap();
    let bytes = client.freeze().unwrap().unwrap();
    drop(client);
    // Change a literal, switch a nullable field to a generator and remove now().
    let changed = with_fields(|fields| {
        fields[1]["createDefault"] = json!({"kind":"literal","value":"changed"});
        fields[6]["createDefault"] = json!({"kind":"uuid"});
        fields[5].as_object_mut().unwrap().remove("createDefault");
    });
    let mut client = open_at(&path, changed);
    assert!(!client.schema_state().rebuilt);
    assert!(client.schema_state().pending.is_none());
    assert_eq!(rows(&mut client), before, "existing rows are not rewritten");
    assert_eq!(
        client
            .read_sql("SELECT args FROM axton_mutation", &[])
            .unwrap(),
        args
    );
    assert_eq!(
        client.freeze().unwrap().unwrap(),
        bytes,
        "frozen bytes unchanged"
    );
    // New creates follow the new policy; a removed default is required again.
    assert!(
        client
            .transaction(|tx| tx.direct(local_create(json!({}))))
            .unwrap_err()
            .to_string()
            .contains("missing state field createdAt")
    );
    client
        .transaction(|tx| {
            tx.direct(local_create(
                json!({"createdAt":"2026-01-01T00:00:00.000Z"}),
            ))
        })
        .unwrap();
    let fresh = rows(&mut client)
        .into_iter()
        .find(|r| !before.contains(r))
        .unwrap();
    assert_eq!(fresh["title"], "changed");
    assert_uuid_v4(&fresh["note"]);
}

#[test]
fn a_creation_default_never_backfills_a_new_required_column() {
    let dir = tempfile::tempdir().unwrap();
    let rank = |extra: Value| {
        with_fields(move |fields| {
            let mut field =
                json!({"name":"rank","type":{"kind":"scalar","name":"int"},"nullable":false});
            field
                .as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            fields.push(field);
        })
    };
    // Only the pre-existing internal default may fill historical rows in place.
    let path = dir.path().join("internal");
    let mut client = open_at(&path, schema());
    client
        .transaction(|tx| tx.direct(local_create(json!({"title":"old"}))))
        .unwrap();
    drop(client);
    let mut client = open_at(&path, rank(json!({"default":3})));
    assert!(!client.schema_state().rebuilt);
    assert_eq!(rows(&mut client)[0]["rank"], 3);
    // A source @default is creation policy: the old file is left behind, and
    // no row is given a default, generated id or creation time it never had.
    let path = dir.path().join("policy");
    let mut client = open_at(&path, schema());
    client
        .transaction(|tx| tx.direct(local_create(json!({"title":"old"}))))
        .unwrap();
    drop(client);
    let mut client = open_at(
        &path,
        rank(json!({"createDefault":{"kind":"literal","value":3}})),
    );
    assert!(client.schema_state().rebuilt);
    assert!(rows(&mut client).is_empty());
}
