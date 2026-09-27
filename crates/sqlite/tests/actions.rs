use axton_client::*;
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};

fn scalar_schema() -> Schema {
    Schema::from_value(json!({"enums":[],"models":[],"actions":[{"name":"Ping","version":1,"inputs":[{"kind":"value","name":"label","type":{"kind":"scalar","name":"string"},"nullable":false}],"outputs":[]}]})).unwrap()
}

fn model_schema() -> Schema {
    Schema::from_value(json!({"enums":[],"models":[{"name":"Todo","version":1,"identity":["id"],"fields":[{"name":"id","type":{"kind":"scalar","name":"string"},"nullable":false},{"name":"title","type":{"kind":"scalar","name":"string"},"nullable":false}]}],"actions":[{"name":"Add","version":1,"inputs":[{"kind":"model","name":"todo","model":"Todo","operation":"create","cardinality":"single"},{"kind":"model","name":"maybe","model":"Todo","operation":"update","cardinality":"optional"},{"kind":"model","name":"gone","model":"Todo","operation":"delete","cardinality":"list"}],"outputs":[]},{"name":"Rename","version":1,"inputs":[{"kind":"model","name":"todo","model":"Todo","operation":"update","cardinality":"single"}],"outputs":[]}]})).unwrap()
}

fn open(path: &std::path::Path, schema: Schema) -> Client<SqliteStore> {
    Client::open(SqliteStore::open(path).unwrap(), schema).unwrap()
}

#[test]
fn direct_action_has_no_optimism_and_commits_authority_before_completion() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"), model_schema());
    let prepared = client
        .prepare_action(
            "Rename",
            1,
            json!({"todo":{"id":"direct","title":"server"}}),
        )
        .unwrap();
    assert_eq!(client.pending_count().unwrap(), 0);
    assert_eq!(
        client
            .read(&RecordKey {
                model: "Todo".into(),
                identity: json!({"id":"direct"})
            })
            .unwrap(),
        None
    );
    let response = json!({"completion":{"callId":prepared.call.call_id,"outcome":{"status":"succeeded","result":null}},"records":[{"model":"Todo","identity":{"id":"direct"},"stamp":1,"state":{"title":"server"}}]});
    let report = client
        .apply_action_response(&prepared, response.to_string().as_bytes())
        .unwrap();
    assert_eq!(report.completions[0].call_id, prepared.call.call_id);
    assert_eq!(
        client
            .read(&RecordKey {
                model: "Todo".into(),
                identity: json!({"id":"direct"})
            })
            .unwrap()
            .unwrap()["title"],
        "server"
    );
    assert_eq!(client.pending_count().unwrap(), 0);
    assert!(client.subscriptions().unwrap().is_empty());
}

#[test]
fn model_free_action_persists_canonical_intent_and_freezes_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut client = open(&path, scalar_schema());
    assert!(client.submit_action("Ping", 1, json!({"label":3})).is_err());
    assert_eq!(client.pending_count().unwrap(), 0);
    let submitted = client
        .submit_action("Ping", 1, json!({"label":"hello"}))
        .unwrap();
    assert_eq!(submitted.ordinal, 1);
    assert_eq!(
        client
            .read_sql("SELECT COUNT(*) AS n FROM axton_mutation_operation", &[])
            .unwrap()[0]["n"],
        0
    );
    let stored = client
        .read_sql("SELECT call_id, args FROM axton_mutation", &[])
        .unwrap();
    assert_eq!(stored[0]["call_id"], submitted.call_id);
    assert_eq!(
        serde_json::from_str::<Value>(stored[0]["args"].as_str().unwrap()).unwrap(),
        json!({"label":"hello"})
    );
    drop(client);
    let mut client = open(&path, scalar_schema());
    let bytes = client.freeze().unwrap().unwrap();
    let request = PushRequest::decode_actions(&bytes, &scalar_schema()).unwrap();
    assert_eq!(request.mutations[0].raw["callId"], submitted.call_id);
    assert_eq!(request.mutations[0].raw["args"], json!({"label":"hello"}));
    drop(client);
    let mut client = open(&path, scalar_schema());
    assert_eq!(client.freeze().unwrap().unwrap(), bytes);
}

#[test]
fn action_create_and_queue_rollback_together_on_failed_optimism() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"), model_schema());
    let args = json!({"todo":{"id":"t1","title":"A"},"gone":[]});
    client.submit_action("Add", 1, args.clone()).unwrap();
    assert!(client.submit_action("Add", 1, args).is_err());
    assert_eq!(client.pending_count().unwrap(), 1);
    assert_eq!(
        client
            .read_sql("SELECT COUNT(*) AS n FROM Todo", &[])
            .unwrap()[0]["n"],
        1
    );
    assert_eq!(
        client
            .read_sql("SELECT COUNT(*) AS n FROM axton_mutation", &[])
            .unwrap()[0]["n"],
        1
    );
    let bytes = client.freeze().unwrap().unwrap();
    let request = PushRequest::decode_actions(&bytes, &model_schema()).unwrap();
    assert_eq!(request.mutations[0].raw["args"]["maybe"], Value::Null);
    assert_eq!(request.mutations[0].raw["args"]["gone"], json!([]));
}

#[test]
fn flat_update_preserves_omission_and_restricted_fields_in_queued_operation() {
    let mut raw = serde_json::to_value(model_schema()).unwrap();
    raw["models"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"note","type":{"kind":"scalar","name":"string"},"nullable":true}));
    raw["actions"][1]["inputs"][0]["allowedPatchFields"] = json!(["title"]);
    let schema = Schema::from_value(raw).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"), schema);
    client
        .transaction(|tx| {
            tx.direct(Operation {
                model: "Todo".into(),
                op: OperationKind::Create,
                identity: json!({"id":"t"}),
                values: Some(json!({"title":"A","note":"kept"})),
            })
        })
        .unwrap();
    assert!(
        client
            .submit_action("Rename", 1, json!({"todo":{"id":"t","note":"blocked"}}))
            .is_err()
    );
    client
        .submit_action("Rename", 1, json!({"todo":{"id":"t","title":"B"}}))
        .unwrap();
    let rows = client
        .read_sql(
            "SELECT identity, \"values\" FROM axton_mutation_operation",
            &[],
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(rows[0]["identity"].as_str().unwrap()).unwrap(),
        json!({"id":"t"})
    );
    assert_eq!(
        serde_json::from_str::<Value>(rows[0]["values"].as_str().unwrap()).unwrap(),
        json!({"title":"B"})
    );
    assert_eq!(
        client
            .read_sql("SELECT note FROM Todo WHERE id='t'", &[])
            .unwrap()[0]["note"],
        "kept"
    );
}

#[test]
fn flat_composite_delete_queues_identity_without_public_wrappers() {
    let schema = Schema::from_value(json!({"enums":[],"models":[{"name":"Book","identity":["slug","edition"],"fields":[{"name":"slug","type":{"kind":"scalar","name":"string"},"nullable":false},{"name":"edition","type":{"kind":"scalar","name":"int"},"nullable":false},{"name":"title","type":{"kind":"scalar","name":"string"},"nullable":false}]}],"actions":[{"name":"Delete","version":1,"inputs":[{"kind":"model","name":"book","model":"Book","operation":"delete","cardinality":"single"}],"outputs":[]}]})).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"), schema);
    client
        .transaction(|tx| {
            tx.direct(Operation {
                model: "Book".into(),
                op: OperationKind::Create,
                identity: json!({"slug":"s","edition":2}),
                values: Some(json!({"title":"A"})),
            })
        })
        .unwrap();
    client
        .submit_action("Delete", 1, json!({"book":{"edition":2,"slug":"s"}}))
        .unwrap();
    let rows = client
        .read_sql(
            "SELECT identity,op,\"values\" FROM axton_mutation_operation",
            &[],
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(rows[0]["identity"].as_str().unwrap()).unwrap(),
        json!({"slug":"s","edition":2})
    );
    assert_eq!(rows[0]["op"], "delete");
    assert!(rows[0]["values"].is_null());
    assert_eq!(
        client
            .read_sql("SELECT COUNT(*) AS n FROM Book", &[])
            .unwrap()[0]["n"],
        0
    );
}

#[test]
fn receipt_completion_is_correlated_and_transient() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut client = open(&path, scalar_schema());
    let submitted = client
        .submit_action("Ping", 1, json!({"label":"ok"}))
        .unwrap();
    client.freeze().unwrap();
    let mut receipt = PushReceipt {
        client_id: client.client_id().into(),
        batch_sequence: 1,
        rejections: vec![],
        completions: vec![CallCompletion {
            call_id: submitted.call_id.clone(),
            outcome: ActionOutcome::Succeeded {
                result: Value::Null,
            },
        }],
        records: vec![],
    };
    let wrong = "01890f47-1234-7123-8123-123456789abc".to_string();
    receipt.completions[0].call_id = wrong;
    assert!(client.acknowledge(1, receipt.clone()).is_err());
    assert_eq!(client.pending_count().unwrap(), 1);
    receipt.completions[0].call_id = submitted.call_id.clone();
    let report = client.acknowledge(1, receipt).unwrap();
    assert_eq!(report.completions.len(), 1);
    assert_eq!(report.completions[0].call_id, submitted.call_id);
    assert_eq!(client.pending_count().unwrap(), 0);
    drop(client);
    let mut client = open(&path, scalar_schema());
    assert!(client.rejections().unwrap().is_empty());
    assert_eq!(
        client
            .read_sql("SELECT COUNT(*) AS n FROM axton_mutation", &[])
            .unwrap()[0]["n"],
        0
    );
}

#[test]
fn rejecting_parent_completes_unsent_lifecycle_dependent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut client = open(&path, model_schema());
    let parent = client
        .submit_action("Add", 1, json!({"todo":{"id":"t","title":"A"},"gone":[]}))
        .unwrap();
    let child = client
        .submit_action("Rename", 1, json!({"todo":{"id":"t","title":"B"}}))
        .unwrap();
    let frozen = client.freeze().unwrap().unwrap();
    let request = PushRequest::decode_actions(&frozen, &model_schema()).unwrap();
    assert_eq!(request.mutations.len(), 1);
    let receipt = PushReceipt {
        client_id: client.client_id().into(),
        batch_sequence: 1,
        rejections: vec![Rejection {
            ordinal: parent.ordinal,
            code: "todo.denied".into(),
        }],
        completions: vec![CallCompletion {
            call_id: parent.call_id.clone(),
            outcome: ActionOutcome::Failed {
                code: "todo.denied".into(),
                execution: ExecutionState::Rejected,
            },
        }],
        records: vec![],
    };
    let report = client.acknowledge(1, receipt).unwrap();
    assert_eq!(report.completions.len(), 2);
    assert_eq!(
        report.completions[1],
        CallCompletion {
            call_id: child.call_id,
            outcome: ActionOutcome::Failed {
                code: "dependency.rejected".into(),
                execution: ExecutionState::Rejected
            }
        }
    );
    assert_eq!(client.pending_count().unwrap(), 0);
    assert_eq!(client.rejections().unwrap().len(), 2);
    drop(client);
    let mut client = open(&path, model_schema());
    assert_eq!(client.rejections().unwrap().len(), 2);
}

#[test]
fn omitted_optional_and_empty_list_still_queue_an_action() {
    let dir = tempfile::tempdir().unwrap();
    let mut raw = serde_json::to_value(model_schema()).unwrap();
    raw["actions"].as_array_mut().unwrap().push(json!({"name":"Touch","version":1,"inputs":[{"kind":"model","name":"maybe","model":"Todo","operation":"update","cardinality":"optional"},{"kind":"model","name":"gone","model":"Todo","operation":"delete","cardinality":"list"}],"outputs":[]}));
    let schema = Schema::from_value(raw).unwrap();
    let mut client = open(&dir.path().join("db"), schema.clone());
    let submitted = client
        .submit_action("Touch", 1, json!({"gone":[]}))
        .unwrap();
    assert_eq!(client.pending_count().unwrap(), 1);
    assert_eq!(
        client
            .read_sql("SELECT COUNT(*) AS n FROM axton_mutation_operation", &[])
            .unwrap()[0]["n"],
        0
    );
    let request = PushRequest::decode_actions(&client.freeze().unwrap().unwrap(), &schema).unwrap();
    assert_eq!(request.mutations[0].raw["callId"], submitted.call_id);
    assert_eq!(
        request.mutations[0].raw["args"],
        json!({"maybe":null,"gone":[]})
    );
}

#[test]
fn sequence_metadata_links_earlier_value_only_calls() {
    let dir = tempfile::tempdir().unwrap();
    let mut raw = serde_json::to_value(scalar_schema()).unwrap();
    raw["actions"].as_array_mut().unwrap().push(json!({"name":"After","version":1,"inputs":[{"kind":"value","name":"label","type":{"kind":"scalar","name":"string"},"nullable":false}],"outputs":[],"sequence":{"after":[{"name":"Ping","arguments":{}}]}}));
    let schema = Schema::from_value(raw).unwrap();
    let mut client = open(&dir.path().join("db"), schema);
    let first = client
        .submit_action("Ping", 1, json!({"label":"one"}))
        .unwrap();
    let second = client
        .submit_action("After", 1, json!({"label":"two"}))
        .unwrap();
    let deps = client
        .read_sql(
            "SELECT ordinal, depends_on, kind FROM axton_mutation_dependency",
            &[],
        )
        .unwrap();
    assert_eq!(
        deps,
        vec![json!({"ordinal":second.ordinal,"depends_on":first.ordinal,"kind":"sequence"})]
    );
}

#[test]
fn retained_action_version_keeps_original_queued_args_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let original = scalar_schema();
    let mut client = open(&path, original.clone());
    let call = client
        .submit_action("Ping", 1, json!({"label":"old"}))
        .unwrap();
    drop(client);
    let mut raw = serde_json::to_value(original).unwrap();
    let mut latest = raw["actions"][0].clone();
    latest["version"] = json!(2);
    latest["inputs"][0]["name"] = json!("newLabel");
    raw["actions"].as_array_mut().unwrap().push(latest);
    let newer = Schema::from_value(raw).unwrap();
    let mut client = open(&path, newer.clone());
    let request = PushRequest::decode_actions(&client.freeze().unwrap().unwrap(), &newer).unwrap();
    assert_eq!(request.mutations[0].raw["version"], 1);
    assert_eq!(request.mutations[0].raw["callId"], call.call_id);
    assert_eq!(request.mutations[0].raw["args"], json!({"label":"old"}));
}

#[test]
fn frozen_model_declaration_survives_incompatible_version_upgrade() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let old_schema = model_schema();
    let factory =
        || Box::new(|path: &std::path::Path| SqliteStore::open(path)) as StoreFactory<SqliteStore>;
    let mut client = Client::open_at(&path, old_schema.clone(), factory(), false).unwrap();
    let call = client
        .submit_action("Add", 1, json!({"todo":{"id":"t","title":"A"},"gone":[]}))
        .unwrap();
    let frozen = client.freeze().unwrap().unwrap();
    assert_eq!(
        PushRequest::decode_actions(&frozen, &old_schema)
            .unwrap()
            .models["Todo"],
        1
    );
    drop(client);
    let mut raw = serde_json::to_value(old_schema).unwrap();
    raw["models"][0]["version"] = json!(2);
    let latest = Schema::from_value(raw).unwrap();
    let mut client = Client::open_at(&path, latest, factory(), false).unwrap();
    assert!(client.schema_state().pending.is_some());
    assert_eq!(client.freeze().unwrap().unwrap(), frozen);
    assert_eq!(
        client
            .read_sql("SELECT call_id, args FROM axton_mutation", &[])
            .unwrap()[0]["call_id"],
        call.call_id
    );
}

#[test]
fn action_bindings_validate_required_create_fields_before_writes() {
    let schema = Schema::from_value(json!({"enums":[],"models":[
        {"name":"Parent","identity":["id"],"fields":[{"name":"id","type":{"kind":"scalar","name":"string"},"nullable":false}]},
        {"name":"Child","identity":["id"],"fields":[{"name":"id","type":{"kind":"scalar","name":"string"},"nullable":false},{"name":"parentId","type":{"kind":"scalar","name":"string"},"nullable":false},{"name":"title","type":{"kind":"scalar","name":"string"},"nullable":false}],"relations":[{"name":"parent","target":"Parent","fields":["parentId"],"targetFields":["id"],"onDelete":"none"}]}
    ],"actions":[{"name":"AddPair","version":1,"inputs":[{"kind":"model","name":"parent","model":"Parent","operation":"create","cardinality":"single"},{"kind":"model","name":"child","model":"Child","operation":"create","cardinality":"single","bindings":[{"slot":"parent","fields":["parentId"]}]}],"outputs":[]}]})).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"), schema);
    let invalid = json!({"parent":{"id":"p"},"child":{"id":"c","parentId":"other","title":"A"}});
    assert!(client.submit_action("AddPair", 1, invalid).is_err());
    assert_eq!(client.pending_count().unwrap(), 0);
    assert_eq!(
        client
            .read_sql("SELECT COUNT(*) AS n FROM Child", &[])
            .unwrap()[0]["n"],
        0
    );
    let valid = json!({"parent":{"id":"p"},"child":{"id":"c","parentId":"p","title":"A"}});
    client.submit_action("AddPair", 1, valid).unwrap();
    assert_eq!(client.pending_count().unwrap(), 1);
    assert_eq!(
        client
            .read_sql("SELECT COUNT(*) AS n FROM axton_mutation_operation", &[])
            .unwrap()[0]["n"],
        2
    );
}

#[test]
fn additive_model_field_keeps_frozen_bytes_and_accepts_old_result_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut raw = serde_json::to_value(model_schema()).unwrap();
    raw["actions"][1]["outputs"] = json!([{"name":"todo","kind":"model","model":"Todo","modelReadVersion":1,"cardinality":"single","source":{"inputIdentity":"todo"}}]);
    raw["resultModels"] = json!([{"name":"Todo","version":1,"identity":["id"],"fields":raw["models"][0]["fields"],"enums":[]}]);
    let old = Schema::from_value(raw.clone()).unwrap();
    let factory =
        || Box::new(|path: &std::path::Path| SqliteStore::open(path)) as StoreFactory<SqliteStore>;
    let mut client = Client::open_at(&path, old.clone(), factory(), false).unwrap();
    client
        .transaction(|tx| {
            tx.direct(Operation {
                model: "Todo".into(),
                op: OperationKind::Create,
                identity: json!({"id":"t"}),
                values: Some(json!({"title":"A"})),
            })
        })
        .unwrap();
    let call = client
        .submit_action("Rename", 1, json!({"todo":{"id":"t","title":"B"}}))
        .unwrap();
    let frozen = client.freeze().unwrap().unwrap();
    let frozen_reads = client
        .read_sql("SELECT push_results FROM axton_client", &[])
        .unwrap()[0]["push_results"]
        .clone();
    let frozen_reads_value: Value = serde_json::from_str(frozen_reads.as_str().unwrap()).unwrap();
    assert_eq!(frozen_reads_value[0]["fields"].as_array().unwrap().len(), 2);
    drop(client);
    let note = json!({"name":"note","type":{"kind":"scalar","name":"string"},"nullable":true});
    raw["models"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(note.clone());
    raw["resultModels"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(note);
    let flag = json!({"name":"flag","type":{"kind":"scalar","name":"boolean"},"nullable":false,"default":true});
    raw["models"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(flag.clone());
    raw["resultModels"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(flag);
    let newer = Schema::from_value(raw).unwrap();
    let mut client = Client::open_at(&path, newer, factory(), false).unwrap();
    assert!(client.schema_state().pending.is_none());
    assert_eq!(client.freeze().unwrap().unwrap(), frozen);
    assert_eq!(
        client
            .read_sql("SELECT push_results FROM axton_client", &[])
            .unwrap()[0]["push_results"],
        frozen_reads
    );
    let receipt = PushReceipt {
        client_id: client.client_id().into(),
        batch_sequence: 1,
        rejections: vec![],
        completions: vec![CallCompletion {
            call_id: call.call_id,
            outcome: ActionOutcome::Succeeded {
                result: json!({"todo":{"id":"t","title":"B"}}),
            },
        }],
        records: vec![AuthorityRecord {
            model: "Todo".into(),
            identity: json!({"id":"t"}),
            stamp: 1,
            state: json!({"title":"B","note":null,"flag":true}),
            error: None,
        }],
    };
    let report = client.acknowledge(1, receipt).unwrap();
    assert_eq!(
        report.completions[0].outcome,
        ActionOutcome::Succeeded {
            result: json!({"todo":{"id":"t","title":"B","note":null,"flag":true}})
        }
    );
    assert_eq!(client.pending_count().unwrap(), 0);
    assert!(
        client
            .read_sql("SELECT push_results FROM axton_client", &[])
            .unwrap()[0]["push_results"]
            .is_null()
    );
}

#[test]
fn same_schema_receipt_cannot_omit_a_declared_nullable_result_field() {
    let dir = tempfile::tempdir().unwrap();
    let mut raw = serde_json::to_value(model_schema()).unwrap();
    let note = json!({"name":"note","type":{"kind":"scalar","name":"string"},"nullable":true});
    raw["models"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(note);
    raw["actions"][1]["outputs"] = json!([{"name":"todo","kind":"model","model":"Todo","modelReadVersion":1,"cardinality":"single","source":{"inputIdentity":"todo"}}]);
    raw["resultModels"] = json!([{"name":"Todo","version":1,"identity":["id"],"fields":raw["models"][0]["fields"],"enums":[]}]);
    let schema = Schema::from_value(raw).unwrap();
    let mut client = open(&dir.path().join("db"), schema);
    client
        .transaction(|tx| {
            tx.direct(Operation {
                model: "Todo".into(),
                op: OperationKind::Create,
                identity: json!({"id":"t"}),
                values: Some(json!({"title":"A","note":null})),
            })
        })
        .unwrap();
    let call = client
        .submit_action("Rename", 1, json!({"todo":{"id":"t","title":"B"}}))
        .unwrap();
    client.freeze().unwrap();
    let mut receipt = PushReceipt {
        client_id: client.client_id().into(),
        batch_sequence: 1,
        rejections: vec![],
        completions: vec![CallCompletion {
            call_id: call.call_id,
            outcome: ActionOutcome::Succeeded {
                result: json!({"todo":{"id":"t","title":"B"}}),
            },
        }],
        records: vec![AuthorityRecord {
            model: "Todo".into(),
            identity: json!({"id":"t"}),
            stamp: 1,
            state: json!({"title":"B","note":null}),
            error: None,
        }],
    };
    assert!(client.acknowledge(1, receipt.clone()).is_err());
    assert_eq!(client.pending_count().unwrap(), 1);
    if let ActionOutcome::Succeeded { result } = &mut receipt.completions[0].outcome {
        result["todo"]["note"] = Value::Null;
    }
    assert_eq!(client.acknowledge(1, receipt).unwrap().completions.len(), 1);
}

#[test]
fn upgraded_server_may_return_new_fields_for_an_old_frozen_result_contract() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut raw = serde_json::to_value(model_schema()).unwrap();
    raw["actions"][1]["outputs"] = json!([{"name":"todo","kind":"model","model":"Todo","modelReadVersion":1,"cardinality":"single","source":{"inputIdentity":"todo"}}]);
    raw["resultModels"] = json!([{"name":"Todo","version":1,"identity":["id"],"fields":raw["models"][0]["fields"],"enums":[]}]);
    let old = Schema::from_value(raw.clone()).unwrap();
    let factory =
        || Box::new(|path: &std::path::Path| SqliteStore::open(path)) as StoreFactory<SqliteStore>;
    let mut client = Client::open_at(&path, old, factory(), false).unwrap();
    client
        .transaction(|tx| {
            tx.direct(Operation {
                model: "Todo".into(),
                op: OperationKind::Create,
                identity: json!({"id":"t"}),
                values: Some(json!({"title":"A"})),
            })
        })
        .unwrap();
    let call = client
        .submit_action("Rename", 1, json!({"todo":{"id":"t","title":"B"}}))
        .unwrap();
    let frozen = client.freeze().unwrap().unwrap();
    drop(client);
    let note = json!({"name":"note","type":{"kind":"scalar","name":"string"},"nullable":true});
    raw["models"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(note.clone());
    raw["resultModels"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(note);
    let mut client =
        Client::open_at(&path, Schema::from_value(raw).unwrap(), factory(), false).unwrap();
    assert_eq!(client.freeze().unwrap().unwrap(), frozen);
    let mut receipt = PushReceipt {
        client_id: client.client_id().into(),
        batch_sequence: 1,
        rejections: vec![],
        completions: vec![CallCompletion {
            call_id: call.call_id,
            outcome: ActionOutcome::Succeeded {
                result: json!({"todo":{"id":"t","title":"B","note":"server"}}),
            },
        }],
        records: vec![AuthorityRecord {
            model: "Todo".into(),
            identity: json!({"id":"t"}),
            stamp: 1,
            state: json!({"title":"B","note":"server"}),
            error: None,
        }],
    };
    if let ActionOutcome::Succeeded { result } = &mut receipt.completions[0].outcome {
        result["todo"]["unknown"] = json!(1);
    }
    assert!(client.acknowledge(1, receipt.clone()).is_err());
    if let ActionOutcome::Succeeded { result } = &mut receipt.completions[0].outcome {
        result["todo"].as_object_mut().unwrap().remove("unknown");
    }
    let report = client.acknowledge(1, receipt).unwrap();
    assert_eq!(
        report.completions[0].outcome,
        ActionOutcome::Succeeded {
            result: json!({"todo":{"id":"t","title":"B","note":"server"}})
        }
    );
}

#[test]
fn explicit_rebuild_reports_abandoned_frozen_and_unsent_calls() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let old = model_schema();
    let factory =
        || Box::new(|path: &std::path::Path| SqliteStore::open(path)) as StoreFactory<SqliteStore>;
    let mut client = Client::open_at(&path, old.clone(), factory(), false).unwrap();
    let frozen = client
        .submit_action("Add", 1, json!({"todo":{"id":"t","title":"A"},"gone":[]}))
        .unwrap();
    client.freeze().unwrap();
    let unsent = client
        .submit_action("Rename", 1, json!({"todo":{"id":"t","title":"B"}}))
        .unwrap();
    drop(client);
    let mut raw = serde_json::to_value(old).unwrap();
    raw["models"][0]["version"] = json!(2);
    let newer = Schema::from_value(raw).unwrap();
    let mut client = Client::open_at(&path, newer, factory(), false).unwrap();
    let report = client.rebuild(true).unwrap();
    assert_eq!(report.left_pending, 2);
    assert_eq!(
        report.abandoned_calls,
        vec![
            AbandonedCall {
                call_id: frozen.call_id,
                frozen: true
            },
            AbandonedCall {
                call_id: unsent.call_id,
                frozen: false
            }
        ]
    );
    assert_eq!(client.pending_count().unwrap(), 0);
}

#[test]
fn explicit_unsent_discard_emits_terminal_action_completion() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"), scalar_schema());
    let call = client
        .submit_action("Ping", 1, json!({"label":"x"}))
        .unwrap();
    let events = client.drop_action(call.ordinal).unwrap();
    assert_eq!(
        events,
        vec![CallCompletion {
            call_id: call.call_id,
            outcome: ActionOutcome::Failed {
                code: "dropped".into(),
                execution: ExecutionState::Rejected
            }
        }]
    );
    assert_eq!(client.pending_count().unwrap(), 0);
    assert_eq!(client.rejections().unwrap()[0].code, "dropped");
}

#[test]
fn legacy_discard_refuses_action_so_it_cannot_lose_terminal_event() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"), scalar_schema());
    let call = client
        .submit_action("Ping", 1, json!({"label":"x"}))
        .unwrap();
    assert!(client.drop_mutation(call.ordinal).is_err());
    assert_eq!(client.pending_count().unwrap(), 1);
    assert!(client.rejections().unwrap().is_empty());
    assert_eq!(
        client.drop_action(call.ordinal).unwrap()[0].call_id,
        call.call_id
    );
}

#[test]
fn legacy_discard_refuses_a_legacy_parent_with_action_descendant() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"), model_schema());
    let parent = client
        .transaction(|tx| {
            tx.enqueue(Mutation::new(
                "LegacyAdd",
                vec![Operation {
                    model: "Todo".into(),
                    op: OperationKind::Create,
                    identity: json!({"id":"t"}),
                    values: Some(json!({"title":"A"})),
                }],
            ))
        })
        .unwrap();
    let child = client
        .submit_action("Rename", 1, json!({"todo":{"id":"t","title":"B"}}))
        .unwrap();
    assert!(client.drop_mutation(parent).is_err());
    assert_eq!(client.pending_count().unwrap(), 2);
    assert!(client.rejections().unwrap().is_empty());
    let events = client.drop_action(parent).unwrap();
    assert_eq!(events[0].call_id, child.call_id);
}

#[test]
fn retained_action_requirements_create_prerequisite_from_operand() {
    let dir = tempfile::tempdir().unwrap();
    let mut raw = serde_json::to_value(model_schema()).unwrap();
    raw["actions"][0]["requirements"] =
        json!([{"model":"Todo","field":"title","name":"Uploaded","arguments":{"key":"self"}}]);
    let schema = Schema::from_value(raw).unwrap();
    let mut client = open(&dir.path().join("db"), schema);
    client
        .submit_action(
            "Add",
            1,
            json!({"todo":{"id":"t","title":"asset"},"gone":[]}),
        )
        .unwrap();
    assert_eq!(
        client.pending_tasks().unwrap(),
        vec![
            json!({"name":"Uploaded","arguments":{"key":"asset"},"key":"{\"arguments\":{\"key\":\"asset\"},\"name\":\"Uploaded\"}","state":"pending"})
        ]
    );
    assert!(client.freeze().unwrap().is_none());
}

#[test]
fn raw_queue_cannot_forge_an_action_intent_or_bypass_legacy_operation_rule() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"), scalar_schema());
    assert!(
        client
            .transaction(|tx| tx.enqueue(Mutation::new("Ping", vec![])))
            .is_err()
    );
    let mut forged = Mutation::new("Ping", vec![]);
    forged.call_id = Some("not-a-uuid".into());
    forged.args = Some(json!({"label":"x"}));
    assert!(client.transaction(|tx| tx.enqueue(forged)).is_err());
    assert_eq!(client.pending_count().unwrap(), 0);
}

fn store_schema() -> Schema {
    let fields = json!([{"name":"id","type":{"kind":"scalar","name":"string"},"nullable":false},{"name":"title","type":{"kind":"scalar","name":"string"},"nullable":false}]);
    let handler = json!({"kind":"identity","model":"Todo","fields":[{"name":"id","type":{"kind":"scalar","name":"string"}}]});
    Schema::from_value(json!({"enums":[],
        "models":[{"name":"Todo","version":1,"identity":["id"],"fields":fields}],
        "resultModels":[{"name":"Todo","version":1,"identity":["id"],"fields":fields,"enums":[]}],
        "actions":[
            {"name":"Search","version":1,"inputs":[{"kind":"value","name":"store","type":{"kind":"scalar","name":"string"},"nullable":false}],
             "outputs":[{"name":"todos","kind":"model","model":"Todo","modelReadVersion":1,"cardinality":"list","source":"handlerIdentity","handlerType":handler}]},
            {"name":"Save","version":1,"inputs":[{"kind":"model","name":"todo","model":"Todo","operation":"create","cardinality":"single"}],
             "outputs":[{"name":"mainTodo","kind":"model","model":"Todo","modelReadVersion":1,"cardinality":"single","source":"handlerIdentity","handlerType":handler},
                        {"name":"suggestions","kind":"model","model":"Todo","modelReadVersion":1,"cardinality":"list","source":"handlerIdentity","handlerType":handler},
                        {"name":"todo","kind":"model","model":"Todo","modelReadVersion":1,"cardinality":"single","source":{"inputIdentity":"todo"}}]}]
    }))
    .unwrap()
}

fn with_store(store: ActionStore) -> ActionCallOptions {
    ActionCallOptions { store }
}

#[test]
fn durable_store_policy_survives_reopen_freeze_and_retry() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut client = open(&path, store_schema());
    let disabled = client
        .submit_action_with_options(
            "Search",
            1,
            json!({"store":"business"}),
            with_store(ActionStore::None),
        )
        .unwrap();
    let mapped = client
        .submit_action_with_options(
            "Save",
            1,
            json!({"todo":{"id":"t","title":"A"}}),
            with_store(ActionStore::Outputs(
                [
                    ("suggestions".to_string(), false),
                    ("mainTodo".to_string(), true),
                ]
                .into(),
            )),
        )
        .unwrap();
    let default = client
        .submit_action("Search", 1, json!({"store":"plain"}))
        .unwrap();
    let stored = client
        .read_sql(
            "SELECT call_id, store FROM axton_mutation ORDER BY ordinal",
            &[],
        )
        .unwrap();
    assert_eq!(stored[0]["call_id"], disabled.call_id);
    assert_eq!(stored[0]["store"], "false");
    assert_eq!(
        stored[1]["store"], r#"{"suggestions":false}"#,
        "explicit true entries are validated, then dropped from the canonical form"
    );
    assert_eq!(stored[2]["store"], Value::Null);
    drop(client);
    let mut client = open(&path, store_schema());
    let bytes = client.freeze().unwrap().unwrap();
    let request = PushRequest::decode_actions(&bytes, &store_schema()).unwrap();
    let raw: Vec<&Value> = request.mutations.iter().map(|m| &m.raw).collect();
    assert_eq!(raw[0]["callId"], disabled.call_id);
    assert_eq!(raw[0]["store"], json!(false));
    assert_eq!(raw[0]["args"], json!({"store":"business"}));
    assert_eq!(raw[1]["callId"], mapped.call_id);
    assert_eq!(raw[1]["store"], json!({"suggestions":false}));
    assert_eq!(raw[2]["callId"], default.call_id);
    assert!(raw[2].get("store").is_none());
    drop(client);
    // A retry after restart resends the frozen bytes, policy included.
    let mut client = open(&path, store_schema());
    assert_eq!(client.freeze().unwrap().unwrap(), bytes);
}

#[test]
fn invalid_store_policy_fails_before_optimism_or_enqueue() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"), store_schema());
    for (key, value) in [
        ("todo", false),
        ("missing", false),
        ("store", false),
        ("missing", true),
    ] {
        let options = with_store(ActionStore::Outputs([(key.to_string(), value)].into()));
        assert!(
            client
                .submit_action_with_options(
                    "Save",
                    1,
                    json!({"todo":{"id":"t","title":"A"}}),
                    options.clone(),
                )
                .is_err(),
            "{key}"
        );
        assert!(
            client
                .prepare_action_with_options("Search", 1, json!({"store":"x"}), options)
                .is_err()
        );
    }
    assert_eq!(client.pending_count().unwrap(), 0);
    assert_eq!(
        client
            .read_sql("SELECT COUNT(*) AS n FROM Todo", &[])
            .unwrap()[0]["n"],
        0
    );
}

#[test]
fn queued_action_without_a_store_column_keeps_default_policy() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let submitted = open(&path, store_schema())
        .submit_action("Search", 1, json!({"store":"old"}))
        .unwrap();
    SqliteStore::open(&path)
        .unwrap()
        .execute_batch("ALTER TABLE axton_mutation DROP COLUMN store")
        .unwrap();
    let mut client = open(&path, store_schema());
    assert_eq!(client.pending_count().unwrap(), 1);
    let bytes = client.freeze().unwrap().unwrap();
    let request = PushRequest::decode_actions(&bytes, &store_schema()).unwrap();
    assert_eq!(request.mutations[0].raw["callId"], submitted.call_id);
    assert!(request.mutations[0].raw.get("store").is_none());
}

#[test]
fn direct_store_policy_is_prepared_outside_args_and_applies_returned_authority() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = open(&dir.path().join("db"), store_schema());
    let all_true = client
        .prepare_action_with_options(
            "Search",
            1,
            json!({"store":"business"}),
            with_store(ActionStore::Outputs([("todos".to_string(), true)].into())),
        )
        .unwrap();
    assert_eq!(all_true.call.store, ActionStore::All, "canonical form");
    let prepared = client
        .prepare_action_with_options(
            "Search",
            1,
            json!({"store":"business"}),
            with_store(ActionStore::None),
        )
        .unwrap();
    assert_eq!(prepared.call.store, ActionStore::None);
    let wire: Value = serde_json::from_slice(&prepared.encode().unwrap()).unwrap();
    assert_eq!(wire["call"]["store"], json!(false));
    assert_eq!(wire["call"]["args"], json!({"store":"business"}));
    let response = json!({"completion":{"callId":prepared.call.call_id,"outcome":{"status":"succeeded","result":{"todos":[{"id":"t","title":"A"}]}}},"records":[]});
    let report = client
        .apply_action_response_bytes(&prepared.encode().unwrap(), response.to_string().as_bytes())
        .unwrap();
    assert_eq!(report.completions.len(), 1);
    assert_eq!(
        client
            .read_sql("SELECT COUNT(*) AS n FROM Todo", &[])
            .unwrap()[0]["n"],
        0
    );
    assert_eq!(client.pending_count().unwrap(), 0);
}

// Named Mutations submitted inside an existing transaction, with local
// companions owned by one call.

fn companion_schema() -> Schema {
    let text =
        |name: &str| json!({"name":name,"type":{"kind":"scalar","name":"string"},"nullable":false});
    Schema::from_value(json!({"enums":[],
        "models":[
            {"name":"Composition","version":1,"identity":["id"],"fields":[text("id"),text("title")]},
            {"name":"Draft","version":1,"identity":["id"],"fields":[text("id"),text("compositionId"),text("name")],
             "relations":[{"name":"composition","target":"Composition","fields":["compositionId"],"targetFields":["id"],"onDelete":"delete"}]},
            {"name":"Entry","version":1,"identity":["id"],"fields":[
                {"name":"id","type":{"kind":"scalar","name":"string"},"nullable":false,"createDefault":{"kind":"uuid"}},
                text("title"),
                {"name":"createdAt","type":{"kind":"scalar","name":"dateTime"},"nullable":false,"createDefault":{"kind":"now"}}]}],
        "actions":[
            {"name":"PublishEntry","version":1,"inputs":[{"kind":"model","name":"entry","model":"Entry","operation":"create","cardinality":"single"}],"outputs":[]},
            {"name":"Ping","version":1,"inputs":[{"kind":"value","name":"label","type":{"kind":"scalar","name":"string"},"nullable":false}],"outputs":[]},
            {"name":"Find","version":1,"kind":"query","inputs":[],"outputs":[]}]
    }))
    .unwrap()
}

type Local = Client<SqliteStore>;

fn key(model: &str, id: &str) -> RecordKey {
    RecordKey {
        model: model.into(),
        identity: json!({ "id": id }),
    }
}

fn title(client: &mut Local, model: &str, id: &str) -> Option<String> {
    client
        .read(&key(model, id))
        .unwrap()
        .map(|row| row["title"].as_str().unwrap().to_owned())
}

fn exists(client: &mut Local, model: &str, id: &str) -> bool {
    client.read(&key(model, id)).unwrap().is_some()
}

fn local_op(model: &str, op: OperationKind, id: &str, values: Option<Value>) -> Operation {
    Operation {
        model: model.into(),
        op,
        identity: json!({ "id": id }),
        values,
    }
}

fn delete_composition(id: &str) -> Operation {
    local_op("Composition", OperationKind::Delete, id, None)
}

fn retitle(id: &str, title: &str) -> Operation {
    local_op(
        "Composition",
        OperationKind::Update,
        id,
        Some(json!({ "title": title })),
    )
}

/// A client holding local-only Compositions `c1` ("draft", with Draft `d1`)
/// and `c2` ("second").
fn seeded(path: &std::path::Path) -> Local {
    let mut client = open(path, companion_schema());
    client
        .transaction(|tx| {
            for (id, title) in [("c1", "draft"), ("c2", "second")] {
                tx.direct(local_op(
                    "Composition",
                    OperationKind::Create,
                    id,
                    Some(json!({ "title": title })),
                ))?;
            }
            tx.direct(local_op(
                "Draft",
                OperationKind::Create,
                "d1",
                Some(json!({"compositionId":"c1","name":"intro"})),
            ))
        })
        .unwrap();
    client
}

fn publish(title: &str) -> Value {
    json!({ "entry": { "title": title } })
}

fn count(client: &mut Local, sql: &str) -> u64 {
    client.read_sql(sql, &[]).unwrap()[0]["n"].as_u64().unwrap()
}

/// The queued operations as `(kind, model, id, op)` in position order.
fn queued_ops(client: &mut Local) -> Vec<(String, String, String, String)> {
    client
        .read_sql(
            "SELECT ordinal, position, kind, model, identity, op FROM axton_mutation_operation ORDER BY ordinal, position",
            &[],
        )
        .unwrap()
        .iter()
        .map(|row| {
            let identity: Value = serde_json::from_str(row["identity"].as_str().unwrap()).unwrap();
            (
                row["kind"].as_str().unwrap().to_owned(),
                row["model"].as_str().unwrap().to_owned(),
                identity["id"].as_str().unwrap().to_owned(),
                row["op"].as_str().unwrap().to_owned(),
            )
        })
        .collect()
}

fn op_row(kind: &str, model: &str, id: &str, op: &str) -> (String, String, String, String) {
    (kind.into(), model.into(), id.into(), op.into())
}

fn stored_args(client: &mut Local, call: &SubmittedCall) -> Value {
    let rows = client
        .read_sql(
            "SELECT args FROM axton_mutation WHERE call_id=?",
            &[json!(call.call_id)],
        )
        .unwrap();
    serde_json::from_str(rows[0]["args"].as_str().unwrap()).unwrap()
}

/// Freeze the next batch and check its calls carry only canonical intents:
/// no operations, no local Model and no companion. (The batch's `models`
/// declaration names every read contract, local Models included.)
fn frozen(client: &mut Local) -> (Vec<u8>, PushRequest) {
    let bytes = client.freeze().unwrap().unwrap();
    let request = PushRequest::decode_actions(&bytes, &companion_schema()).unwrap();
    let calls = request.raw["mutations"].to_string();
    for local in ["Composition", "Draft", "companion", "operations"] {
        assert!(!calls.contains(local), "{local} reached the wire: {calls}");
    }
    for mutation in &request.mutations {
        let mut keys: Vec<&str> = mutation
            .raw
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        keys.sort();
        assert_eq!(keys, ["args", "callId", "name", "ordinal", "version"]);
    }
    (bytes, request)
}

/// Settle the frozen batch: each call accepted (with the authority of the
/// Entry it created) or rejected.
fn settle(client: &mut Local, request: &PushRequest, outcomes: &[(&SubmittedCall, bool)]) {
    let mut receipt = PushReceipt {
        client_id: client.client_id().into(),
        batch_sequence: request.batch_sequence,
        rejections: vec![],
        completions: vec![],
        records: vec![],
    };
    for (call, accepted) in outcomes {
        if *accepted {
            receipt.completions.push(CallCompletion {
                call_id: call.call_id.clone(),
                outcome: ActionOutcome::Succeeded {
                    result: Value::Null,
                },
            });
            let raw = &request
                .mutations
                .iter()
                .find(|m| m.raw["callId"] == call.call_id)
                .unwrap()
                .raw;
            if let Some(entry) = raw["args"].get("entry") {
                let mut state = entry.clone();
                let id = state.as_object_mut().unwrap().remove("id").unwrap();
                receipt.records.push(AuthorityRecord {
                    model: "Entry".into(),
                    identity: json!({ "id": id }),
                    stamp: 1,
                    state,
                    error: None,
                });
            }
        } else {
            receipt.rejections.push(Rejection {
                ordinal: call.ordinal,
                code: "denied".into(),
            });
            receipt.completions.push(CallCompletion {
                call_id: call.call_id.clone(),
                outcome: ActionOutcome::Failed {
                    code: "denied".into(),
                    execution: ExecutionState::Rejected,
                },
            });
        }
    }
    client.acknowledge(request.batch_sequence, receipt).unwrap();
}

/// Nothing pending and nothing retained for recovery.
fn assert_settled(client: &mut Local) {
    assert_eq!(client.pending_count().unwrap(), 0);
    assert_eq!(client.before_image_count().unwrap(), 0);
    assert_eq!(
        count(client, "SELECT COUNT(*) AS n FROM axton_local_write"),
        0
    );
}

/// The motivating workflow: read a Composition, submit the business
/// Mutation from it and delete the Composition as that call's companion, in
/// one local commit that survives reopen. Only the canonical intent is sent;
/// acceptance keeps the deletion and rejection restores the Composition.
#[test]
fn a_named_mutation_and_its_local_delete_commit_together_and_settle_with_the_call() {
    for accepted in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut client = seeded(&path);
        let call = client
            .transaction(|tx| {
                let composition = tx.read(&key("Composition", "c1"))?.unwrap();
                let title = composition["title"].as_str().unwrap().to_owned();
                let call = tx.submit_mutation(
                    "PublishEntry",
                    1,
                    publish(&title),
                    ActionCallOptions::default(),
                )?;
                tx.append_companion(call.ordinal, delete_composition("c1"))?;
                // The transaction sees the call's optimism and its companion.
                assert!(tx.read(&key("Composition", "c1"))?.is_none());
                assert!(tx.read(&key("Draft", "d1"))?.is_none());
                Ok(call)
            })
            .unwrap();
        assert_eq!(call.ordinal, 1);
        let args = stored_args(&mut client, &call);
        let entry_id = args["entry"]["id"].as_str().unwrap().to_owned();
        assert_eq!(args["entry"]["title"], "draft");
        assert!(args["entry"]["createdAt"].is_string(), "{args}");
        // Queue rows: the wire create, then the companion delete with its
        // cascade stored at the delete that caused it.
        assert_eq!(
            queued_ops(&mut client),
            vec![
                op_row("wire", "Entry", &entry_id, "create"),
                op_row("companion", "Draft", "d1", "delete"),
                op_row("companion", "Composition", "c1", "delete"),
            ]
        );
        // Visible records and the recovery metadata the settlement needs.
        assert_eq!(
            title(&mut client, "Entry", &entry_id).as_deref(),
            Some("draft")
        );
        assert!(!exists(&mut client, "Composition", "c1"));
        assert!(!exists(&mut client, "Draft", "d1"));
        assert_eq!(
            title(&mut client, "Composition", "c2").as_deref(),
            Some("second")
        );
        assert_eq!(
            client
                .read_sql("SELECT id, title FROM axton_before_Composition", &[])
                .unwrap(),
            vec![json!({"id":"c1","title":"draft"})]
        );
        assert_eq!(
            count(&mut client, "SELECT COUNT(*) AS n FROM axton_before_Draft"),
            1
        );
        // Offline commit, then reopen: the call and its companion remain
        // and the canonical request is all that is sent.
        drop(client);
        let mut client = open(&path, companion_schema());
        assert_eq!(client.pending_count().unwrap(), 1);
        assert!(!exists(&mut client, "Composition", "c1"));
        let (bytes, request) = frozen(&mut client);
        assert_eq!(request.mutations.len(), 1);
        assert_eq!(request.mutations[0].raw["callId"], call.call_id);
        assert_eq!(request.mutations[0].raw["args"], args);
        drop(client);
        let mut client = open(&path, companion_schema());
        assert_eq!(client.freeze().unwrap().unwrap(), bytes);
        settle(&mut client, &request, &[(&call, accepted)]);
        assert_eq!(exists(&mut client, "Composition", "c1"), !accepted);
        assert_eq!(exists(&mut client, "Draft", "d1"), !accepted);
        assert_eq!(exists(&mut client, "Entry", &entry_id), accepted);
        if !accepted {
            assert_eq!(
                title(&mut client, "Composition", "c1").as_deref(),
                Some("draft")
            );
        }
        assert_settled(&mut client);
    }
}

/// A transaction that fails after submitting and appending keeps nothing:
/// no queue row, optimism, companion, recovery metadata or call ordinal.
#[test]
fn a_failing_transaction_keeps_no_call_companion_or_recovery_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = seeded(&dir.path().join("db"));
    let failed = client.transaction(|tx| {
        let call = tx.submit_mutation(
            "PublishEntry",
            1,
            publish("draft"),
            ActionCallOptions::default(),
        )?;
        tx.append_companion(call.ordinal, delete_composition("c1"))?;
        Err::<(), _>(invalid("application callback threw"))
    });
    assert!(failed.is_err());
    assert_eq!(client.pending_count().unwrap(), 0);
    assert_eq!(
        count(
            &mut client,
            "SELECT COUNT(*) AS n FROM axton_mutation_operation"
        ),
        0
    );
    assert_eq!(count(&mut client, "SELECT COUNT(*) AS n FROM Entry"), 0);
    assert_eq!(client.before_image_count().unwrap(), 0);
    assert_eq!(
        title(&mut client, "Composition", "c1").as_deref(),
        Some("draft")
    );
    assert!(exists(&mut client, "Draft", "d1"));
    assert!(client.freeze().unwrap().is_none());
    // The rolled-back call left no ordinal behind either.
    let call = client
        .transaction(|tx| {
            tx.submit_mutation(
                "Ping",
                1,
                json!({"label":"x"}),
                ActionCallOptions::default(),
            )
        })
        .unwrap();
    assert_eq!(call.ordinal, 1);
}

/// A Mutation with only scalar inputs has no inferred optimism; its
/// companion is still owned, never sent and settled with it.
#[test]
fn a_scalar_only_mutation_carries_a_companion_that_never_reaches_the_wire() {
    for accepted in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let mut client = seeded(&dir.path().join("db"));
        let call = client
            .transaction(|tx| {
                let call = tx.submit_mutation(
                    "Ping",
                    1,
                    json!({"label":"x"}),
                    ActionCallOptions::default(),
                )?;
                tx.append_companion(call.ordinal, retitle("c1", "pinged"))?;
                Ok(call)
            })
            .unwrap();
        assert_eq!(
            queued_ops(&mut client),
            vec![op_row("companion", "Composition", "c1", "update")]
        );
        assert_eq!(
            title(&mut client, "Composition", "c1").as_deref(),
            Some("pinged")
        );
        let (_, request) = frozen(&mut client);
        assert_eq!(request.mutations[0].raw["args"], json!({"label":"x"}));
        settle(&mut client, &request, &[(&call, accepted)]);
        let expected = if accepted { "pinged" } else { "draft" };
        assert_eq!(
            title(&mut client, "Composition", "c1").as_deref(),
            Some(expected)
        );
        assert_settled(&mut client);
    }
}

/// Two calls in one transaction: one local commit, companions owned by the
/// call whose callback wrote them, and separate outcomes.
#[test]
fn two_calls_share_a_commit_but_own_their_companions_and_outcomes() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = seeded(&dir.path().join("db"));
    let (first, second) = client
        .transaction(|tx| {
            let first = tx.submit_mutation(
                "PublishEntry",
                1,
                publish("one"),
                ActionCallOptions::default(),
            )?;
            tx.append_companion(first.ordinal, delete_composition("c1"))?;
            let second = tx.submit_mutation(
                "PublishEntry",
                1,
                publish("two"),
                ActionCallOptions::default(),
            )?;
            tx.append_companion(second.ordinal, retitle("c2", "published"))?;
            // A companion cannot join an earlier call once a later one exists.
            assert!(
                tx.append_companion(first.ordinal, retitle("c2", "late"))
                    .is_err()
            );
            Ok((first, second))
        })
        .unwrap();
    assert_ne!(first.call_id, second.call_id);
    assert_eq!((first.ordinal, second.ordinal), (1, 2));
    let ops = queued_ops(&mut client);
    assert_eq!(ops.len(), 5, "{ops:?}");
    assert_eq!(
        title(&mut client, "Composition", "c2").as_deref(),
        Some("published")
    );
    // Sharing the transaction adds no dependency between the calls.
    assert_eq!(
        count(
            &mut client,
            "SELECT COUNT(*) AS n FROM axton_mutation_dependency"
        ),
        0
    );
    let (_, request) = frozen(&mut client);
    assert_eq!(request.mutations.len(), 2);
    // First accepted, second rejected: each call settles only its own.
    settle(&mut client, &request, &[(&first, true), (&second, false)]);
    assert!(!exists(&mut client, "Composition", "c1"));
    assert!(!exists(&mut client, "Draft", "d1"));
    assert_eq!(
        title(&mut client, "Composition", "c2").as_deref(),
        Some("second")
    );
    assert_settled(&mut client);
}

/// Ordinary writes in the same transaction are independent: they are never
/// attached to a call and survive either outcome of an earlier companion.
#[test]
fn independent_writes_in_the_transaction_are_not_companions() {
    for accepted in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let mut client = seeded(&dir.path().join("db"));
        let call = client
            .transaction(|tx| {
                tx.direct(retitle("c2", "independent"))?;
                let call = tx.submit_mutation(
                    "PublishEntry",
                    1,
                    publish("draft"),
                    ActionCallOptions::default(),
                )?;
                tx.append_companion(call.ordinal, delete_composition("c1"))?;
                // Recreate the deleted identity once the companion is done.
                tx.direct(local_op(
                    "Composition",
                    OperationKind::Create,
                    "c1",
                    Some(json!({"title":"new"})),
                ))?;
                // A companion cannot follow an independent write made after
                // its call: it would settle out of local order.
                assert!(
                    tx.append_companion(call.ordinal, retitle("c1", "late"))
                        .is_err()
                );
                Ok(call)
            })
            .unwrap();
        let ops = queued_ops(&mut client);
        assert!(!ops.iter().any(|(_, _, id, _)| id == "c2"), "{ops:?}");
        assert_eq!(
            count(
                &mut client,
                "SELECT COUNT(*) AS n FROM axton_local_write WHERE disposition='independent'"
            ),
            1
        );
        let (_, request) = frozen(&mut client);
        settle(&mut client, &request, &[(&call, accepted)]);
        assert_eq!(
            title(&mut client, "Composition", "c1").as_deref(),
            Some("new")
        );
        assert_eq!(
            title(&mut client, "Composition", "c2").as_deref(),
            Some("independent")
        );
        assert_settled(&mut client);
    }
}

/// Invalid args or store policy, unknown operations and Queries are refused
/// before any write; the transaction stays usable. The standalone entry keeps
/// queueing a Query.
#[test]
fn invalid_submissions_and_queries_are_refused_before_any_write() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = seeded(&dir.path().join("db"));
    let call = client
        .transaction(|tx| {
            let options = ActionCallOptions::default;
            assert!(
                tx.submit_mutation("PublishEntry", 1, json!({"entry":{"title":3}}), options())
                    .is_err()
            );
            assert!(
                tx.submit_mutation(
                    "PublishEntry",
                    1,
                    json!({"entry":{"title":"a"},"extra":1}),
                    options()
                )
                .is_err()
            );
            assert!(tx.submit_mutation("Ping", 1, json!({}), options()).is_err());
            assert!(
                tx.submit_mutation("Missing", 1, json!({}), options())
                    .is_err()
            );
            assert!(
                tx.submit_mutation("Ping", 2, json!({"label":"x"}), options())
                    .is_err()
            );
            let store = ActionCallOptions {
                store: ActionStore::Outputs([("missing".to_string(), false)].into()),
            };
            assert!(
                tx.submit_mutation("PublishEntry", 1, publish("a"), store)
                    .is_err()
            );
            let query = tx
                .submit_mutation("Find", 1, json!({}), options())
                .unwrap_err();
            assert!(query.to_string().contains("Query"), "{query}");
            assert!(tx.query("Entry", &json!({}))?.is_empty());
            tx.submit_mutation("Ping", 1, json!({"label":"x"}), options())
        })
        .unwrap();
    assert_eq!(call.ordinal, 1);
    assert_eq!(client.pending_count().unwrap(), 1);
    assert_eq!(
        count(
            &mut client,
            "SELECT COUNT(*) AS n FROM axton_mutation_operation"
        ),
        0
    );
    let query = client.submit_action("Find", 1, json!({})).unwrap();
    assert_eq!(query.ordinal, 2);
    assert_eq!(client.pending_count().unwrap(), 2);
}

/// A duplicate create identity fails its submission or companion alone;
/// a companion validates ownership, and a failed companion that the
/// transaction propagates rolls everything back.
#[test]
fn duplicates_and_failed_companions_leave_no_partial_writes() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = seeded(&dir.path().join("db"));
    let earlier = client
        .transaction(|tx| {
            tx.submit_mutation(
                "Ping",
                1,
                json!({"label":"x"}),
                ActionCallOptions::default(),
            )
        })
        .unwrap();
    let legacy = client
        .transaction(|tx| {
            tx.enqueue(Mutation::new(
                "Legacy",
                vec![local_op(
                    "Composition",
                    OperationKind::Update,
                    "c2",
                    Some(json!({"title":"legacy"})),
                )],
            ))
        })
        .unwrap();
    let call = client
        .transaction(|tx| {
            // Only a call this transaction submitted takes companions.
            assert!(
                tx.append_companion(earlier.ordinal, retitle("c1", "x"))
                    .is_err()
            );
            assert!(tx.append_companion(legacy, retitle("c1", "x")).is_err());
            assert!(tx.append_companion(99, retitle("c1", "x")).is_err());
            let raw = tx.enqueue(Mutation::new(
                "Legacy",
                vec![local_op(
                    "Composition",
                    OperationKind::Update,
                    "c2",
                    Some(json!({"title":"raw"})),
                )],
            ))?;
            assert!(tx.append_companion(raw, retitle("c1", "x")).is_err());
            let call = tx.submit_mutation(
                "PublishEntry",
                1,
                json!({"entry":{"id":"e1","title":"one"}}),
                ActionCallOptions::default(),
            )?;
            assert!(
                tx.submit_mutation(
                    "PublishEntry",
                    1,
                    json!({"entry":{"id":"e1","title":"again"}}),
                    ActionCallOptions::default(),
                )
                .is_err()
            );
            tx.append_companion(call.ordinal, delete_composition("c1"))?;
            // Failed companions: a duplicate create, an update of a missing
            // record, an invalid value and an unknown Model. Each fails alone.
            let failures = [
                local_op(
                    "Composition",
                    OperationKind::Create,
                    "c2",
                    Some(json!({"title":"dup"})),
                ),
                retitle("gone", "x"),
                local_op(
                    "Composition",
                    OperationKind::Update,
                    "c2",
                    Some(json!({"title":7})),
                ),
                local_op("Unknown", OperationKind::Delete, "x", None),
            ];
            for failure in failures {
                assert!(tx.append_companion(call.ordinal, failure).is_err());
            }
            Ok(call)
        })
        .unwrap();
    assert_eq!(client.pending_count().unwrap(), 4);
    let ops: Vec<_> = queued_ops(&mut client)
        .into_iter()
        .filter(|(kind, ..)| kind == "companion")
        .collect();
    assert_eq!(
        ops,
        vec![
            op_row("companion", "Draft", "d1", "delete"),
            op_row("companion", "Composition", "c1", "delete"),
        ]
    );
    assert_eq!(title(&mut client, "Entry", "e1").as_deref(), Some("one"));
    assert_eq!(
        title(&mut client, "Composition", "c2").as_deref(),
        Some("raw")
    );
    assert_eq!(call.ordinal, 4);

    // Propagating a failed companion fails the whole transaction.
    let before = client.pending_count().unwrap();
    let failed = client.transaction(|tx| {
        let call = tx.submit_mutation(
            "Ping",
            1,
            json!({"label":"y"}),
            ActionCallOptions::default(),
        )?;
        tx.append_companion(call.ordinal, retitle("c2", "kept?"))?;
        tx.append_companion(call.ordinal, retitle("gone", "x"))
    });
    assert!(failed.is_err());
    assert_eq!(client.pending_count().unwrap(), before);
    assert_eq!(
        title(&mut client, "Composition", "c2").as_deref(),
        Some("raw")
    );

    // A failed companion keeps no recovery metadata either: the call commits
    // with neither a companion row nor a before image of the records the
    // failed companions touched.
    let images = client.before_image_count().unwrap();
    let ping = client
        .transaction(|tx| {
            let call = tx.submit_mutation(
                "Ping",
                1,
                json!({"label":"z"}),
                ActionCallOptions::default(),
            )?;
            for failure in [
                retitle("gone", "x"),
                local_op(
                    "Composition",
                    OperationKind::Update,
                    "c2",
                    Some(json!({"title":7})),
                ),
            ] {
                assert!(tx.append_companion(call.ordinal, failure).is_err());
            }
            Ok(call)
        })
        .unwrap();
    assert_eq!(client.pending_count().unwrap(), before + 1);
    assert_eq!(client.before_image_count().unwrap(), images);
    assert_eq!(
        count(
            &mut client,
            &format!(
                "SELECT COUNT(*) AS n FROM axton_mutation_operation WHERE ordinal={}",
                ping.ordinal
            )
        ),
        0
    );
    // A companion delete of a missing record is no failure: like a direct
    // delete it removes nothing, and it is kept with its call.
    let absent = client
        .transaction(|tx| {
            let call = tx.submit_mutation(
                "Ping",
                1,
                json!({"label":"absent"}),
                ActionCallOptions::default(),
            )?;
            tx.append_companion(
                call.ordinal,
                local_op("Composition", OperationKind::Delete, "gone", None),
            )?;
            Ok(call)
        })
        .unwrap();
    assert!(!exists(&mut client, "Composition", "gone"));
    assert_eq!(
        count(
            &mut client,
            &format!(
                "SELECT COUNT(*) AS n FROM axton_mutation_operation WHERE ordinal={} AND kind='companion'",
                absent.ordinal
            )
        ),
        1
    );
}

/// Rolling back an existing savepoint discards the calls and companions
/// made in it and keeps the earlier ones. A raw queue row that later reuses
/// the discarded ordinal, even one shaped as a canonical Action call, takes
/// no companion.
#[test]
fn savepoint_rollback_discards_its_calls_and_their_companions() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = seeded(&dir.path().join("db"));
    let (kept, discarded, raw) = client
        .transaction(|tx| {
            let kept = tx.submit_mutation(
                "PublishEntry",
                1,
                publish("kept"),
                ActionCallOptions::default(),
            )?;
            tx.append_companion(kept.ordinal, delete_composition("c1"))?;
            let mut discarded = None;
            let scope = tx.savepoint(|tx| {
                let call = tx.submit_mutation(
                    "Ping",
                    1,
                    json!({"label":"gone"}),
                    ActionCallOptions::default(),
                )?;
                tx.append_companion(call.ordinal, retitle("c2", "discarded"))?;
                discarded = Some(call);
                Err::<(), _>(invalid("scope failed"))
            });
            assert!(scope.is_err());
            let discarded = discarded.unwrap();
            assert!(
                tx.append_companion(discarded.ordinal, retitle("c2", "x"))
                    .is_err()
            );
            let mut forged = Mutation::new("Ping", vec![]);
            forged.call_id = Some(uuid::Uuid::new_v4().to_string());
            forged.args = Some(json!({"label":"raw"}));
            let raw = tx.enqueue(forged)?;
            assert_eq!(raw, discarded.ordinal, "the discarded ordinal is reused");
            assert!(tx.append_companion(raw, retitle("c2", "x")).is_err());
            Ok((kept, discarded, raw))
        })
        .unwrap();
    assert_eq!(client.pending_count().unwrap(), 2);
    assert_eq!(
        count(
            &mut client,
            &format!(
                "SELECT COUNT(*) AS n FROM axton_mutation WHERE call_id='{}'",
                discarded.call_id
            )
        ),
        0
    );
    assert_eq!(
        title(&mut client, "Composition", "c2").as_deref(),
        Some("second")
    );
    assert!(!exists(&mut client, "Composition", "c1"));
    let entry = stored_args(&mut client, &kept)["entry"]["id"].clone();
    assert_eq!(
        queued_ops(&mut client),
        vec![
            op_row("wire", "Entry", entry.as_str().unwrap(), "create"),
            op_row("companion", "Draft", "d1", "delete"),
            op_row("companion", "Composition", "c1", "delete"),
        ]
    );
    assert_eq!(raw, 2);
}

/// The runtime's session keeps the same rule as `Client::transaction`:
/// rolling back a session savepoint discards the calls submitted in it, so a
/// raw row that reuses a discarded ordinal takes no companion, while a call
/// from a released scope keeps taking them.
#[test]
fn a_session_savepoint_rollback_discards_its_calls() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = seeded(&dir.path().join("db"));
    let ping = |label: &str| {
        let args = json!({ "label": label });
        move |tx: &mut ClientTransaction<'_, SqliteStore>| {
            tx.submit_mutation("Ping", 1, args, ActionCallOptions::default())
        }
    };
    client.begin_session().unwrap();
    client.session(ping("kept")).unwrap();
    client.session_savepoint().unwrap();
    let discarded = client.session(ping("gone")).unwrap();
    client
        .session(|tx| tx.append_companion(discarded.ordinal, retitle("c2", "discarded")))
        .unwrap();
    client.session_rollback_savepoint().unwrap();
    assert!(
        client
            .session(|tx| tx.append_companion(discarded.ordinal, retitle("c2", "x")))
            .is_err()
    );
    let raw = client
        .session(|tx| {
            let mut forged = Mutation::new("Ping", vec![]);
            forged.call_id = Some(uuid::Uuid::new_v4().to_string());
            forged.args = Some(json!({"label":"raw"}));
            tx.enqueue(forged)
        })
        .unwrap();
    assert_eq!(raw, discarded.ordinal, "the discarded ordinal is reused");
    assert!(
        client
            .session(|tx| tx.append_companion(raw, retitle("c2", "x")))
            .is_err()
    );
    client.session_savepoint().unwrap();
    let released = client.session(ping("released")).unwrap();
    client.session_release().unwrap();
    client
        .session(|tx| tx.append_companion(released.ordinal, retitle("c2", "released")))
        .unwrap();
    client.commit_session().unwrap();
    assert_eq!(client.pending_count().unwrap(), 3);
    assert_eq!(
        title(&mut client, "Composition", "c2").as_deref(),
        Some("released")
    );
    assert_eq!(
        queued_ops(&mut client),
        vec![op_row("companion", "Composition", "c2", "update")]
    );
}
