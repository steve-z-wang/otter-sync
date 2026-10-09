use axton_client::{ActionOutcome, Client, Schema};
use axton_protocols::sync as v05;
use axton_server::{Config, Host, HostResult};
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};
use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Mutex};

// Deterministic persistence seam: actual server code owns admission, progress,
// outcomes and replay; no test constructs an acknowledgement or terminal Call.
#[derive(Default)]
struct State {
    last_batch: u64,
    progress: u64,
    digest: Option<String>,
    count: Option<u64>,
    last_digest: Option<String>,
    last_count: Option<u64>,
    results: BTreeMap<u64, Value>,
    executions: Vec<String>,
}
#[derive(Default)]
struct BatchHost(Mutex<State>);
impl Host for BatchHost {
    fn call(&self, request: Value) -> Pin<Box<dyn Future<Output = HostResult<Value>> + Send + '_>> {
        Box::pin(async move {
            let mut s = self.0.lock().unwrap();
            let q = &request["request"];
            Ok(match (request["op"].as_str(), q["op"].as_str()) {
                (Some("publicationFence" | "savepoint" | "rollback" | "release"), _) => Value::Null,
                (Some("head"), _) => json!(0),
                (Some("handleAction"), _) => {
                    assert_eq!(request["version"], 1, "unsupported handler must never run");
                    s.executions
                        .push(request["arguments"]["text"].as_str().unwrap().into());
                    json!({"outputs":{},"changes":[],"declarations":[]})
                }
                (_, Some("claimStore")) => {
                    json!({"principal":"alice","stream":"User:alice","lastProcessedBatchId":s.last_batch,"progress":s.progress,"currentDigest":s.digest,"currentCount":s.count,"lastDigest":s.last_digest,"lastCount":s.last_count})
                }
                (_, Some("admit")) => json!(true),
                (_, Some("beginBatch")) => {
                    s.digest = Some(q["digest"].as_str().unwrap().into());
                    s.count = q["count"].as_u64();
                    s.results.clear();
                    Value::Null
                }
                (_, Some("readResult")) => s
                    .results
                    .get(&q["ordinal"].as_u64().unwrap())
                    .cloned()
                    .unwrap_or(Value::Null),
                (_, Some("targetPositions")) => json!([]),
                (_, Some("saveResult")) => {
                    let ordinal = q["ordinal"].as_u64().unwrap();
                    assert_eq!(ordinal, s.progress);
                    s.results.insert(ordinal, q["result"].clone());
                    s.progress += 1;
                    if s.progress == s.count.unwrap() {
                        s.last_batch = q["batchId"].as_u64().unwrap();
                        s.last_digest = s.digest.take();
                        s.last_count = s.count.take();
                        s.progress = 0;
                    }
                    Value::Null
                }
                _ => return Err(format!("unexpected host operation {request}")),
            })
        })
    }
}
fn action(version: u64) -> Value {
    json!({"name":"Say","version":version,"kind":"mutation","inputs":[{"kind":"value","name":"text","type":{"kind":"scalar","name":"string"},"nullable":false}],"outputs":[]})
}
fn schema() -> Schema {
    let mut retired = action(1);
    retired["name"] = json!("Retired");
    Schema::from_value(json!({"enums":[],"models":[],"actions":[action(1),action(2),retired]}))
        .unwrap()
}
fn config() -> Config {
    Config::decode(json!({"schema":{"enums":[],"models":[],"actions":[action(1)]},"mutations":[],"loaders":[],"protocol5":{"projectionGeneration":"1"}})).unwrap()
}
async fn execute(c: &Config, h: &BatchHost, batch: &v05::MutationRequest) -> String {
    let bytes = v05::encode(batch).unwrap();
    let frozen = axton_server::validate_mutation_batch(c, &bytes)
        .expect("a valid unsupported member must not refuse the entire frozen Batch");
    let mut results = vec![];
    for ordinal in 0..batch.mutations.len() {
        results.push(
            axton_server::process_batch_member(c, "alice", frozen.as_bytes(), ordinal as u64, h)
                .await
                .unwrap(),
        );
    }
    axton_server::encode_batch_acknowledgement(frozen.as_bytes(), &results).unwrap()
}
fn run(f: impl Future<Output = ()>) {
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(std::pin::pin!(f).as_mut().poll(&mut cx).is_ready());
}
fn mixed_batch(unsupported_name: &str, unsupported_version: u64) {
    run(async {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("client.sqlite");
        let mut client =
            Client::open05(SqliteStore::open(&path).unwrap(), schema(), "User:alice").unwrap();
        client.initialize_stream05(0).unwrap();
        let mut calls = vec![];
        for (name, version, text) in [
            ("Say", 1, "before"),
            (unsupported_name, unsupported_version, "unsupported"),
            ("Say", 1, "after"),
        ] {
            calls.push(
                client
                    .transaction(|tx| {
                        tx.submit_mutation05(name, version, json!({"text":text}), vec![])
                    })
                    .unwrap(),
            );
        }
        let batch = client.freeze_batch05().unwrap().unwrap();
        assert_eq!(batch.mutations.len(), 3);
        let host = BatchHost::default();
        let first = execute(&config(), &host, &batch).await;
        let ack: v05::BatchAcknowledgement = v05::decode(first.as_bytes()).unwrap();
        assert!(matches!(
            ack.results[0].outcome,
            v05::MutationOutcome::Accepted { .. }
        ));
        assert_eq!(
            ack.results[1].outcome,
            v05::MutationOutcome::Rejected {
                code: "mutation_version_unsupported".into(),
                message: None
            }
        );
        assert!(matches!(
            ack.results[2].outcome,
            v05::MutationOutcome::Accepted { .. }
        ));
        assert_eq!(host.0.lock().unwrap().executions, ["before", "after"]);
        assert_eq!(host.0.lock().unwrap().last_batch, batch.batch_id);
        assert_eq!(execute(&config(), &host, &batch).await, first);
        assert_eq!(host.0.lock().unwrap().executions, ["before", "after"]);
        client.acknowledge_batch05(&ack).unwrap();
        client.settle_ready05().unwrap();
        for (index, call) in calls.iter().enumerate() {
            let completion = client.call_completion05(&call.call_id).unwrap().unwrap();
            if index == 1 {
                assert!(
                    matches!(completion.outcome, ActionOutcome::Failed { ref code, .. } if code == "mutation_version_unsupported")
                );
            } else {
                assert!(matches!(
                    completion.outcome,
                    ActionOutcome::Succeeded { .. }
                ));
            }
        }
        drop(client);
        let mut client =
            Client::open05(SqliteStore::open(&path).unwrap(), schema(), "User:alice").unwrap();
        assert!(client.freeze_batch05().unwrap().is_none());
        assert!(
            client
                .call_completion05(&calls[1].call_id)
                .unwrap()
                .is_some()
        );
        client
            .transaction(|tx| tx.submit_mutation05("Say", 1, json!({"text":"next"}), vec![]))
            .unwrap();
        let next = client.freeze_batch05().unwrap().unwrap();
        assert_eq!(next.batch_id, batch.batch_id + 1);
        let next_ack = execute(&config(), &host, &next).await;
        client
            .acknowledge_batch05(&v05::decode(next_ack.as_bytes()).unwrap())
            .unwrap();
        assert_eq!(
            host.0.lock().unwrap().executions,
            ["before", "after", "next"]
        );
    });
}

#[test]
fn unsupported_version_between_supported_calls_is_durable_replayable_and_settles_on_client() {
    mixed_batch("Say", 2);
}
#[test]
fn unretained_name_between_supported_calls_is_durable_replayable_and_settles_on_client() {
    mixed_batch("Retired", 1);
}
#[test]
fn malformed_wire_bound_slot_and_forbidden_patch_remain_structural_request_errors() {
    let s = Schema::from_value(json!({"enums":[],"models":[{
        "name":"Entry","identity":["id"],"fields":[
            {"name":"id","type":{"kind":"scalar","name":"string"},"nullable":false},
            {"name":"text","type":{"kind":"scalar","name":"string"},"nullable":false}
        ]}],"actions":[{
        "name":"Pair","version":1,"kind":"mutation","outputs":[],"inputs":[
            {"kind":"model","name":"first","model":"Entry","operation":"create","cardinality":"single"},
            {"kind":"model","name":"second","model":"Entry","operation":"update","cardinality":"single","allowedPatchFields":["text"],"bindings":[{"slot":"first","fields":["id"]}]}
        ]}]})).unwrap();
    let config = Config::decode(json!({"schema":s,"mutations":[],"loaders":["Entry"]})).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let mut client = Client::open05(
        SqliteStore::open(directory.path().join("client.sqlite")).unwrap(),
        s,
        "User:alice",
    )
    .unwrap();
    client
        .transaction(|tx| {
            tx.submit_mutation05(
                "Pair",
                1,
                json!({"first":{"id":"e","text":"before"},"second":{"id":"e","text":"after"}}),
                vec![],
            )
        })
        .unwrap();
    let batch = client.freeze_batch05().unwrap().unwrap();
    assert!(axton_server::validate_mutation_batch(&config, &v05::encode(&batch).unwrap()).is_ok());
    assert_eq!(
        axton_server::validate_mutation_batch(&config, b"not JSON")
            .unwrap_err()
            .code,
        "request.invalid"
    );
    let mut unsupported = batch.clone();
    unsupported.mutations[0].name = "Unretained".into();
    unsupported.digest = v05::batch_digest(&unsupported).unwrap();
    // Unsupported semantic version does not excuse malformed generic intent.
    unsupported.mutations[0].operations[0].input_path = "broken[".into();
    assert_eq!(
        axton_server::validate_mutation_batch(&config, &serde_json::to_vec(&unsupported).unwrap())
            .unwrap_err()
            .code,
        "request.invalid"
    );
    unsupported = batch.clone();
    unsupported.mutations[0].name = "Unretained".into();
    unsupported.digest = "0".repeat(64);
    assert_eq!(
        axton_server::validate_mutation_batch(&config, &serde_json::to_vec(&unsupported).unwrap())
            .unwrap_err()
            .code,
        "request.invalid"
    );
    let mut corrupt = batch.clone();
    corrupt.digest = "0".repeat(64);
    assert_eq!(
        axton_server::validate_mutation_batch(&config, &serde_json::to_vec(&corrupt).unwrap())
            .unwrap_err()
            .code,
        "request.invalid"
    );
    corrupt = batch.clone();
    corrupt.mutations[0].operations[1].identity = json!({"id":"other"});
    corrupt.digest = v05::batch_digest(&corrupt).unwrap();
    assert_eq!(
        axton_server::validate_mutation_batch(&config, &v05::encode(&corrupt).unwrap())
            .unwrap_err()
            .code,
        "request.invalid"
    );
    corrupt = batch;
    corrupt.mutations[0].operations[1].value["undeclared"] = json!(true);
    corrupt.digest = v05::batch_digest(&corrupt).unwrap();
    assert_eq!(
        axton_server::validate_mutation_batch(&config, &v05::encode(&corrupt).unwrap())
            .unwrap_err()
            .code,
        "request.invalid"
    );
}

#[test]
fn retained_query_cannot_enter_a_mutation_batch() {
    let directory = tempfile::tempdir().unwrap();
    let mut client = Client::open05(
        SqliteStore::open(directory.path().join("client.sqlite")).unwrap(),
        schema(),
        "User:alice",
    )
    .unwrap();
    client
        .transaction(|tx| tx.submit_mutation05("Say", 1, json!({"text":"intent"}), vec![]))
        .unwrap();
    let batch = client.freeze_batch05().unwrap().unwrap();
    let mut query = action(1);
    query["kind"] = json!("query");
    let config = Config::decode(
        json!({"schema":{"enums":[],"models":[],"actions":[query]},"mutations":[],"loaders":[]}),
    )
    .unwrap();
    assert_eq!(
        axton_server::validate_mutation_batch(&config, &v05::encode(&batch).unwrap())
            .unwrap_err()
            .code,
        "request.invalid"
    );
}
