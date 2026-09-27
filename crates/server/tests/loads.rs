//! Native Load pages on the server: one page is one owner-scoped durable call
//! executed in the caller's transaction, resolved through batched stamp and
//! Loader reads, and replayed from its saved outcome.
mod support;

use axton_core::Continuation;
use axton_server::{
    Config, Host, HostResult, code, host::HostRequest, process_action, process_load,
    validate_load_batch,
};
use serde_json::{Value, json};
use std::{future::Future, pin::Pin};
use support::{Backend, run};

fn field(name: &str) -> Value {
    json!({"name":name,"type":{"kind":"scalar","name":"string"},"nullable":false})
}
fn list_output(name: &str, model: &str) -> Value {
    json!({"name":name,"kind":"model","cardinality":"list","source":"handlerIdentity","model":model,"modelReadVersion":1,
        "handlerType":{"kind":"identity","model":model,"fields":[{"name":"id","type":{"kind":"scalar","name":"string"}}]}})
}

/// Todo {id, title} and Project {id, name}; `ProjectTodos(projectId String)
/// { todos Todo[], projects Project[] }`; the Action `Settle()`. `todo_fields`
/// lets a test publish a compatible change of the Todo read contract.
fn config_with(todo_fields: Value) -> Config {
    let project = json!([field("id"), field("name")]);
    Config::decode(json!({
        "schema":{"enums":[],
            "models":[
                {"name":"Todo","version":1,"identity":["id"],"fields":todo_fields},
                {"name":"Project","version":1,"identity":["id"],"fields":project}],
            "resultModels":[
                {"name":"Todo","version":1,"identity":["id"],"fields":todo_fields,"enums":[]},
                {"name":"Project","version":1,"identity":["id"],"fields":project,"enums":[]}],
            "actions":[{"name":"Settle","version":1,"inputs":[],"outputs":[]}],
            "loads":[{"name":"ProjectTodos","version":1,
                "inputs":[{"kind":"value","name":"projectId","type":{"kind":"scalar","name":"string"},"nullable":false,"list":false,"required":true,"cardinality":"single"}],
                "outputs":[list_output("todos","Todo"),list_output("projects","Project")],
                "input":{"models":[],"enums":[]},"outputEnums":[]}]},
        "mutations":[],
        "loaders":["Todo","Project"]
    }))
    .unwrap()
}
fn config() -> Config {
    config_with(json!([field("id"), field("title")]))
}

fn id(n: u64) -> String {
    format!("01890f47-1234-7123-8123-{n:012x}")
}
const LOAD: u64 = 0xaaa;

/// One page request of the `ProjectTodos` job `LOAD` under call `call`.
fn item(call: u64, continuation: Value) -> Value {
    json!({"loadId":id(LOAD),"callId":id(call),"name":"ProjectTodos","version":1,
        "args":{"projectId":"p1"},"continuation":continuation,"models":{"Todo":1,"Project":1}})
}
fn page_as(host: &impl Host, config: &Config, owner: &str, item: &Value) -> Value {
    let text = run(process_load(
        config,
        owner,
        item.to_string().as_bytes(),
        host,
    ))
    .unwrap();
    serde_json::from_str(&text).unwrap()
}
/// One page by `alice`. Every succeeded page the server answers (and saves)
/// passes the client's own per-item decoder and validation.
fn page(backend: &Backend, item: &Value) -> Value {
    let answered = page_as(backend, &config(), "alice", item);
    if answered["outcome"]["status"] == "succeeded" {
        client_accepts(&answered, item);
    }
    answered
}
fn client_accepts(page: &Value, item: &Value) {
    let intent: axton_core::LoadIntent = serde_json::from_value(item.clone()).unwrap();
    let intent = intent.normalize(&config().schema).unwrap();
    axton_core::LoadPageResponse::decode_item(page)
        .unwrap()
        .normalize(&config().schema, &intent)
        .unwrap_or_else(|error| panic!("the client refuses {page}: {error:?}"));
}
fn todo(n: usize) -> String {
    format!("t{n}")
}
fn ids(ids: &[&str]) -> Value {
    Value::Array(ids.iter().map(|id| json!({ "id": id })).collect())
}
fn answer(todos: Value, projects: Value, next: Value) -> Value {
    json!({"data":{"todos":todos,"projects":projects},"next":next})
}
fn outcome_code(page: &Value) -> &str {
    assert_eq!(page["outcome"]["status"], "failed", "{page}");
    assert_eq!(page["records"], json!([]), "{page}");
    page["outcome"]["error"]["code"].as_str().unwrap()
}
/// The continuation each `handleLoad` request carried, in order.
fn continuations(backend: &Backend) -> Vec<Option<Continuation>> {
    backend
        .log()
        .into_iter()
        .filter_map(|request| match request {
            HostRequest::HandleLoad { continuation, .. } => Some(continuation),
            _ => None,
        })
        .collect()
}
/// The saved response of `call`, if any.
fn saved(backend: &Backend, call: u64) -> Option<Value> {
    backend.with(|s| {
        s.tables.calls.get(&id(call)).and_then(|(_, response)| {
            response
                .as_deref()
                .map(|r| serde_json::from_str(r).unwrap())
        })
    })
}
fn seed_todos(backend: &Backend, count: usize) {
    for n in 1..=count {
        backend.seed(
            "Todo",
            &todo(n),
            json!({"id":todo(n),"title":format!("T{n}")}),
            None,
        );
    }
}

#[test]
fn first_and_later_pages_carry_their_continuation_and_answer_data_next_and_authority() {
    let backend = Backend::new();
    seed_todos(&backend, 2);
    backend.seed("Project", "p1", json!({"id":"p1","name":"P"}), Some(3));
    let state = json!({"after":"t2","path":[1,{"deep":[null,true,2.5]}],"empty":{}});
    backend.script(
        "ProjectTodos",
        answer(ids(&["t2", "t1"]), ids(&["p1"]), json!({ "state": state })),
    );
    let first = page(&backend, &item(1, Value::Null));
    assert_eq!(first["loadId"], id(LOAD));
    assert_eq!(first["callId"], id(1));
    assert_eq!(first["outcome"]["status"], "succeeded");
    assert_eq!(
        first["outcome"]["data"],
        json!({"todos":[{"id":"t2"},{"id":"t1"}],"projects":[{"id":"p1"}]}),
        "the handler's identity lists keep their order"
    );
    assert_eq!(first["outcome"]["next"], json!({ "state": state }));
    assert_eq!(
        first["records"],
        json!([
            {"model":"Project","identity":{"id":"p1"},"stamp":3,"state":{"name":"P"}},
            {"model":"Todo","identity":{"id":"t1"},"stamp":1,"state":{"title":"T1"}},
            {"model":"Todo","identity":{"id":"t2"},"stamp":1,"state":{"title":"T2"}}
        ])
    );
    let handled = backend
        .log()
        .into_iter()
        .find(|request| matches!(request, HostRequest::HandleLoad { .. }))
        .unwrap();
    assert_eq!(
        serde_json::to_value(handled).unwrap(),
        json!({"op":"handleLoad","name":"ProjectTodos","version":1,"arguments":{"projectId":"p1"},
            "continuation":null,"owner":"alice","callId":id(1),"loadId":id(LOAD)})
    );

    // The next request passes the returned wrapper back verbatim; a final
    // empty page completes with `next: null`.
    backend.script("ProjectTodos", answer(json!([]), json!([]), Value::Null));
    let last = page(&backend, &item(2, first["outcome"]["next"].clone()));
    assert_eq!(last["outcome"]["next"], Value::Null);
    assert_eq!(last["outcome"]["data"], json!({"todos":[],"projects":[]}));
    assert_eq!(last["records"], json!([]));

    // `{state: null}` is a later page, never the first one.
    page(&backend, &item(3, json!({ "state": null })));
    assert_eq!(
        continuations(&backend),
        vec![
            None,
            Some(Continuation {
                state: state.clone()
            }),
            Some(Continuation { state: Value::Null })
        ]
    );
}

#[test]
fn an_empty_page_that_continues_succeeds_without_stamp_or_loader_reads() {
    let backend = Backend::new();
    backend.script(
        "ProjectTodos",
        answer(json!([]), json!([]), json!({"state":{"cursor":7}})),
    );
    let empty = page(&backend, &item(1, Value::Null));
    assert_eq!(empty["outcome"]["status"], "succeeded");
    assert_eq!(empty["outcome"]["next"], json!({"state":{"cursor":7}}));
    assert_eq!(empty["records"], json!([]));
    assert_eq!(backend.count("readStamps"), 0);
    assert_eq!(backend.count("load"), 0);
}

#[test]
fn repeated_identities_are_resolved_once_per_record_with_one_stamp_and_one_loader_read_per_model() {
    let backend = Backend::new();
    seed_todos(&backend, 2);
    backend.seed("Project", "p1", json!({"id":"p1","name":"P"}), None);
    backend.script(
        "ProjectTodos",
        answer(
            ids(&["t1", "t2", "t1", "t2"]),
            ids(&["p1", "p1"]),
            Value::Null,
        ),
    );
    let result = page(&backend, &item(1, Value::Null));
    assert_eq!(
        result["outcome"]["data"]["todos"],
        ids(&["t1", "t2", "t1", "t2"]),
        "the lists keep repeated identities"
    );
    assert_eq!(result["records"].as_array().unwrap().len(), 3);
    let stamp_reads: Vec<(String, Vec<String>)> = backend
        .log()
        .into_iter()
        .filter_map(|request| match request {
            HostRequest::ReadStamps {
                model,
                identity_keys,
            } => Some((model, identity_keys)),
            _ => None,
        })
        .collect();
    assert_eq!(
        stamp_reads,
        vec![
            ("Project".into(), vec![r#"{"id":"p1"}"#.into()]),
            (
                "Todo".into(),
                vec![r#"{"id":"t1"}"#.into(), r#"{"id":"t2"}"#.into()]
            ),
        ]
    );
    assert_eq!(backend.loaded_models(), vec!["Project", "Todo"]);
}

#[test]
fn a_thousand_identity_page_takes_a_fixed_number_of_host_round_trips() {
    let backend = Backend::new();
    seed_todos(&backend, 1000);
    let all: Vec<String> = (1..=1000).map(todo).collect();
    let all: Vec<&str> = all.iter().map(String::as_str).collect();
    backend.script(
        "ProjectTodos",
        answer(ids(&all), json!([]), json!({"state":"t1000"})),
    );
    let result = page(&backend, &item(1, Value::Null));
    assert_eq!(result["outcome"]["status"], "succeeded");
    assert_eq!(result["records"].as_array().unwrap().len(), 1000);
    assert_eq!(
        backend.ops(),
        [
            "claimCall",
            "savepoint",
            "handleLoad",
            "readStamps",
            "load",
            "release",
            "saveCall"
        ]
    );

    // One identity more is a typed size failure, saved before any read.
    backend.clear_log();
    backend.seed("Todo", "extra", json!({"id":"extra","title":"X"}), None);
    let mut over = all.clone();
    over.push("extra");
    backend.script("ProjectTodos", answer(ids(&over), json!([]), Value::Null));
    let refused = page(&backend, &item(2, Value::Null));
    assert_eq!(outcome_code(&refused), code::LOAD_PAGE_TOO_LARGE);
    assert_eq!(backend.count("readStamps"), 0);
    assert_eq!(saved(&backend, 2), Some(refused));
}

#[test]
fn existing_stamps_are_read_without_rewriting_and_only_missing_ones_start_at_one() {
    let backend = Backend::new();
    seed_todos(&backend, 2);
    backend.seed("Todo", "t1", json!({"id":"t1","title":"T1"}), Some(5));
    backend.script(
        "ProjectTodos",
        answer(ids(&["t1", "t2"]), json!([]), Value::Null),
    );
    let result = page(&backend, &item(1, Value::Null));
    let stamps: Vec<u64> = result["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|record| record["stamp"].as_u64().unwrap())
        .collect();
    assert_eq!(stamps, [5, 1]);
    assert_eq!(backend.stamp("Todo", "t1"), Some(5));
    assert_eq!(backend.stamp("Todo", "t2"), Some(1));
    assert_eq!(backend.count("ensureStamp"), 0);
    assert_eq!(backend.count("advanceStamp"), 0);
}

#[test]
fn an_absent_record_fails_the_whole_page_and_rolls_back_its_stamps_and_writes() {
    let backend = Backend::new();
    seed_todos(&backend, 1);
    backend.script(
        "ProjectTodos",
        answer(ids(&["t1", "gone"]), json!([]), json!({"state":1})),
    );
    backend.write(
        "ProjectTodos",
        "Todo",
        "t1",
        Some(json!({"id":"t1","title":"written by a read"})),
    );
    let failed = page(&backend, &item(1, Value::Null));
    assert_eq!(outcome_code(&failed), code::LOAD_RECORD_UNAVAILABLE);
    assert!(
        failed["outcome"]["error"]["message"]
            .as_str()
            .unwrap()
            .contains("gone")
    );
    assert_eq!(backend.stamp("Todo", "t1"), None, "stamps roll back");
    assert_eq!(backend.stamp("Todo", "gone"), None);
    assert_eq!(backend.row("Todo", "t1").unwrap()["title"], "T1");
    assert_eq!(backend.count("rollback"), 1);
    assert_eq!(saved(&backend, 1), Some(failed.clone()));

    // The rejection is replayed until an explicit retry uses a new call ID.
    backend.clear_log();
    assert_eq!(page(&backend, &item(1, Value::Null)), failed);
    assert_eq!(backend.ops(), ["claimCall"]);
}

#[test]
fn loader_refusal_and_unreadable_rows_are_saved_page_failures() {
    let backend = Backend::new();
    seed_todos(&backend, 2);
    backend.refuse_load("Todo", "t2");
    backend.script(
        "ProjectTodos",
        answer(ids(&["t1", "t2"]), json!([]), Value::Null),
    );
    assert_eq!(
        outcome_code(&page(&backend, &item(1, Value::Null))),
        "todo.forbidden"
    );

    let backend = Backend::new();
    backend.seed("Todo", "t1", json!({"id":"t1","title":7}), None);
    backend.script("ProjectTodos", answer(ids(&["t1"]), json!([]), Value::Null));
    let invalid = page(&backend, &item(1, Value::Null));
    assert_eq!(outcome_code(&invalid), code::LOADER_INVALID);
    assert_eq!(saved(&backend, 1), Some(invalid));
    assert_eq!(backend.stamp("Todo", "t1"), None);
}

#[test]
fn handler_rejection_and_failure_roll_back_its_writes_before_saving() {
    for (answer, expected) in [
        (json!({"rejection":"project.closed"}), "project.closed"),
        (json!({"error":"TypeError: boom"}), code::HANDLER_FAILED),
    ] {
        let backend = Backend::new();
        seed_todos(&backend, 1);
        backend.script("ProjectTodos", answer);
        backend.write("ProjectTodos", "Todo", "t1", None);
        let failed = page(&backend, &item(1, Value::Null));
        assert_eq!(outcome_code(&failed), expected);
        assert!(
            backend.row("Todo", "t1").is_some(),
            "{expected}: write rolled back"
        );
        assert_eq!(saved(&backend, 1), Some(failed));
    }
}

#[test]
fn the_load_context_is_read_only_and_a_forged_settlement_is_refused() {
    let backend = Backend::new();
    seed_todos(&backend, 1);
    let mut forged = answer(ids(&["t1"]), json!([]), Value::Null);
    forged["changes"] = json!([{"model":"Todo","identity":{"id":"t1"}}]);
    forged["memberships"] =
        json!([{"channel":"c","model":"Todo","identity":{"id":"t1"},"present":true}]);
    backend.script("ProjectTodos", forged);
    let refused = page(&backend, &item(1, Value::Null));
    assert_eq!(outcome_code(&refused), code::HANDLER_INVALID);
    assert_eq!(backend.stamp("Todo", "t1"), None);
    assert!(backend.members("Todo", "t1").is_empty());
    assert_eq!(backend.count("readStamps"), 0);
}

#[test]
fn data_that_is_not_exactly_the_declared_identity_lists_is_an_invalid_handler_answer() {
    for data in [
        json!({"todos":[]}),
        json!({"todos":[],"projects":[],"extra":[]}),
        json!({"todos":{"id":"t1"},"projects":[]}),
        json!({"todos":[{"id":"t1","title":"T"}],"projects":[]}),
        json!({"todos":[{"id":7}],"projects":[]}),
        json!({"todos":[null],"projects":[]}),
    ] {
        let backend = Backend::new();
        seed_todos(&backend, 1);
        backend.script("ProjectTodos", json!({"data":data,"next":null}));
        assert_eq!(
            outcome_code(&page(&backend, &item(1, Value::Null))),
            code::HANDLER_INVALID,
            "{data}"
        );
    }
}

fn nested(depth: usize) -> Value {
    (0..depth).fold(json!(0), |inner, _| json!([inner]))
}

#[test]
fn continuation_state_is_bounded_portable_json_in_both_directions() {
    // An incoming state past the bounds fails before the handler runs.
    for state in [
        nested(65),
        json!(9007199254740992_u64),
        json!("x".repeat(64 * 1024)),
    ] {
        let backend = Backend::new();
        let failed = page(&backend, &item(1, json!({ "state": state })));
        assert_eq!(outcome_code(&failed), code::LOAD_INVALID_CONTINUATION);
        assert_eq!(backend.count("handleLoad"), 0);
        assert_eq!(saved(&backend, 1), Some(failed));
    }
    // At the bounds it passes, normalized.
    let backend = Backend::new();
    backend.script(
        "ProjectTodos",
        answer(json!([]), json!([]), json!({"state": nested(64)})),
    );
    let ok = page(&backend, &item(1, json!({"state": {"n": 1.0}})));
    assert_eq!(ok["outcome"]["next"], json!({"state": nested(64)}));
    assert_eq!(
        continuations(&backend),
        vec![Some(Continuation {
            state: json!({"n": 1})
        })]
    );
    // A returned state past the bounds is a saved rejection, checked in Rust
    // whatever the host bridge let through.
    for state in [nested(65), json!(-9007199254740992_i64), json!(1e300)] {
        let backend = Backend::new();
        backend.script(
            "ProjectTodos",
            answer(json!([]), json!([]), json!({ "state": state })),
        );
        assert_eq!(
            outcome_code(&page(&backend, &item(1, Value::Null))),
            code::LOAD_INVALID_CONTINUATION,
            "{state}"
        );
    }
    // A malformed wrapper is not a continuation at all.
    let backend = Backend::new();
    backend.script(
        "ProjectTodos",
        answer(json!([]), json!([]), json!({"state":1,"more":2})),
    );
    assert_eq!(
        outcome_code(&page(&backend, &item(1, Value::Null))),
        code::HANDLER_INVALID
    );
}

#[test]
fn unknown_operations_invalid_args_and_undeclared_contracts_are_saved_item_rejections() {
    let cases = [
        (json!({"name":"Missing"}), code::LOAD_VERSION_UNSUPPORTED),
        (json!({"version":2}), code::LOAD_VERSION_UNSUPPORTED),
        (json!({"args":{"projectId":7}}), code::LOAD_INVALID),
        (json!({"args":{}}), code::LOAD_INVALID),
        (
            json!({"models":{"Todo":1}}),
            code::MODEL_VERSION_UNSUPPORTED,
        ),
        (
            json!({"models":{"Todo":2,"Project":1}}),
            code::MODEL_VERSION_UNSUPPORTED,
        ),
        (
            json!({"models":{"Todo":1,"Project":1,"Ghost":1}}),
            code::MODEL_VERSION_UNSUPPORTED,
        ),
    ];
    for (n, (change, expected)) in cases.into_iter().enumerate() {
        let backend = Backend::new();
        let mut request = item(n as u64 + 1, Value::Null);
        for (key, value) in change.as_object().unwrap() {
            request[key] = value.clone();
        }
        let failed = page(&backend, &request);
        assert_eq!(outcome_code(&failed), expected, "{change}");
        assert_eq!(backend.count("handleLoad"), 0, "{change}");
        assert_eq!(saved(&backend, n as u64 + 1), Some(failed), "{change}");
    }
}

#[test]
fn a_repeated_call_id_replays_its_saved_page_without_handler_loader_or_stamps() {
    let backend = Backend::new();
    seed_todos(&backend, 1);
    backend.script(
        "ProjectTodos",
        answer(ids(&["t1"]), json!([]), json!({"state":{"after":"t1"}})),
    );
    let first = page(&backend, &item(1, Value::Null));
    // Business rows and stamps change after the page committed.
    backend.seed("Todo", "t1", json!({"id":"t1","title":"changed"}), Some(9));
    backend.script("ProjectTodos", answer(json!([]), json!([]), Value::Null));
    backend.clear_log();
    let mut upper = item(1, Value::Null);
    upper["callId"] = json!(id(1).to_uppercase());
    upper["loadId"] = json!(id(LOAD).to_uppercase());
    for request in [item(1, Value::Null), upper] {
        assert_eq!(page(&backend, &request), first);
    }
    assert_eq!(backend.ops(), ["claimCall", "claimCall"]);
    assert_eq!(first["records"][0]["state"]["title"], "T1");
    assert_eq!(first["outcome"]["next"], json!({"state":{"after":"t1"}}));
}

#[test]
fn a_reused_call_id_with_another_request_or_kind_conflicts_without_saving() {
    let backend = Backend::new();
    backend.script("ProjectTodos", answer(json!([]), json!([]), Value::Null));
    let first = page(&backend, &item(1, Value::Null));
    let requests = backend.with(|s| s.tables.calls.clone());
    for other in [
        item(1, json!({"state":null})),
        {
            let mut other = item(1, Value::Null);
            other["loadId"] = json!(id(0xbbb));
            other
        },
        {
            let mut other = item(1, Value::Null);
            other["args"] = json!({"projectId":"p2"});
            other
        },
    ] {
        let conflict = page(&backend, &other);
        assert_eq!(outcome_code(&conflict), code::CALL_IDENTITY_CONFLICT);
        assert_eq!(conflict["callId"], id(1));
    }
    assert_eq!(backend.with(|s| s.tables.calls.clone()), requests);
    assert_eq!(saved(&backend, 1), Some(first));

    // An Action cannot replay a Load page's call ID, nor the reverse.
    let action = |call: u64| {
        let request =
            json!({"call":{"callId":id(call),"name":"Settle","version":1,"args":{}},"models":{}});
        let text = run(process_action(
            &config(),
            "alice",
            request.to_string().as_bytes(),
            &backend,
        ))
        .unwrap();
        serde_json::from_str::<Value>(&text).unwrap()
    };
    assert_eq!(
        action(1)["completion"]["outcome"]["code"],
        code::CALL_IDENTITY_CONFLICT
    );
    assert_eq!(action(2)["completion"]["outcome"]["status"], "succeeded");
    assert_eq!(
        outcome_code(&page(&backend, &item(2, Value::Null))),
        code::CALL_IDENTITY_CONFLICT
    );
    let claims: Vec<Value> = backend
        .log()
        .into_iter()
        .filter_map(|request| match request {
            HostRequest::ClaimCall { request, .. } => serde_json::from_str(&request).ok(),
            _ => None,
        })
        .collect();
    assert_eq!(claims[0]["kind"], "load", "the saved page names its kind");
}

#[test]
fn the_claim_is_owner_scoped() {
    let backend = Backend::new();
    backend.script("ProjectTodos", answer(json!([]), json!([]), Value::Null));
    page_as(&backend, &config(), "bob", &item(1, Value::Null));
    let owners: Vec<String> = backend
        .log()
        .into_iter()
        .filter_map(|request| match request {
            HostRequest::ClaimCall { owner, .. }
            | HostRequest::SaveCall { owner, .. }
            | HostRequest::HandleLoad { owner, .. } => Some(owner),
            HostRequest::Load { owner, .. } => Some(owner),
            _ => None,
        })
        .collect();
    assert_eq!(owners, ["bob", "bob", "bob"]);
    let error = run(process_load(
        &config(),
        " ",
        item(2, Value::Null).to_string().as_bytes(),
        &backend,
    ))
    .unwrap_err();
    assert_eq!(error.code, code::PRINCIPAL_INVALID);
}

#[test]
fn replay_normalizes_saved_authority_for_the_current_read_contract() {
    let backend = Backend::new();
    seed_todos(&backend, 1);
    backend.script("ProjectTodos", answer(ids(&["t1"]), json!([]), Value::Null));
    let first = page(&backend, &item(1, Value::Null));
    let note = json!({"name":"note","type":{"kind":"scalar","name":"string"},"nullable":true});
    let upgraded = config_with(json!([field("id"), field("title"), note]));
    let replay = page_as(&backend, &upgraded, "alice", &item(1, Value::Null));
    assert_eq!(replay["outcome"], first["outcome"]);
    assert_eq!(
        replay["records"][0]["state"],
        json!({"title":"T1","note":null})
    );
}

#[test]
fn an_oversized_page_is_a_saved_failure_and_keeps_none_of_its_stamps() {
    let backend = Backend::new();
    for n in 1..=2 {
        backend.seed(
            "Todo",
            &todo(n),
            json!({"id":todo(n),"title":"x".repeat(600 * 1024)}),
            None,
        );
    }
    backend.script(
        "ProjectTodos",
        answer(ids(&["t1", "t2"]), json!([]), Value::Null),
    );
    let failed = page(&backend, &item(1, Value::Null));
    assert_eq!(outcome_code(&failed), code::LOAD_PAGE_TOO_LARGE);
    assert_eq!(backend.stamp("Todo", "t1"), None);
    assert_eq!(saved(&backend, 1), Some(failed));
}

/// Answers every operation from the backend except one, which it replaces.
struct Replacing<'a> {
    backend: &'a Backend,
    op: &'static str,
    answer: Result<Value, String>,
}
impl Host for Replacing<'_> {
    fn call(&self, raw: Value) -> Pin<Box<dyn Future<Output = HostResult<Value>> + Send + '_>> {
        if raw["op"] == self.op {
            let answer = self.answer.clone();
            return Box::pin(async move { answer });
        }
        self.backend.call(raw)
    }
}

#[test]
fn infrastructure_faults_and_host_defects_are_errors_never_saved_outcomes() {
    let cases = [
        ("saveCall", Err("connection reset".to_string()), code::HOST),
        ("load", Err("statement timeout".to_string()), code::HOST),
        ("readStamps", Ok(json!([1])), code::HOST_INVALID),
        ("readStamps", Ok(json!([0, 1])), code::HOST_INVALID),
        ("handleLoad", Err("pool timeout".to_string()), code::HOST),
    ];
    for (op, answer, expected) in cases {
        let backend = Backend::new();
        seed_todos(&backend, 2);
        backend.script("ProjectTodos", answer_of_two());
        let host = Replacing {
            backend: &backend,
            op,
            answer,
        };
        let error = run(process_load(
            &config(),
            "alice",
            item(1, Value::Null).to_string().as_bytes(),
            &host,
        ))
        .unwrap_err();
        assert_eq!(error.code, expected, "{op}");
        assert_eq!(saved(&backend, 1), None, "{op}: nothing saved");
    }
    // A saved response this kind cannot read is a storage fault.
    let backend = Backend::new();
    page(&backend, &item(1, Value::Null));
    backend.with(|s| {
        s.tables.calls.get_mut(&id(1)).unwrap().1 = Some(r#"{"completion":{}}"#.into());
    });
    let error = run(process_load(
        &config(),
        "alice",
        item(1, Value::Null).to_string().as_bytes(),
        &backend,
    ))
    .unwrap_err();
    assert_eq!(error.code, code::STORAGE_INVALID);
}
fn answer_of_two() -> Value {
    answer(ids(&["t1", "t2"]), json!([]), Value::Null)
}

#[test]
fn the_batch_validator_is_structural_and_answers_canonical_items_in_order() {
    let mut first = item(1, Value::Null);
    first["callId"] = json!(id(1).to_uppercase());
    let mut unknown = item(2, json!({"state":[1,2]}));
    unknown["loadId"] = json!(id(0xbbb));
    unknown["name"] = json!("Unknown");
    unknown["args"] = json!({"anything":true});
    let items =
        validate_load_batch(json!({"loads":[first, unknown]}).to_string().as_bytes()).unwrap();
    assert_eq!(items.len(), 2);
    let decoded: Vec<Value> = items
        .iter()
        .map(|item| serde_json::from_str(item).unwrap())
        .collect();
    assert_eq!(decoded[0]["callId"], id(1), "IDs are canonical");
    assert_eq!(
        decoded[1]["name"], "Unknown",
        "an unknown Load is an item outcome"
    );
    assert_eq!(
        items[0],
        axton_core::canonical_json(&decoded[0]).unwrap(),
        "each item is canonical JSON"
    );
    let nine: Vec<Value> = (1..=9)
        .map(|n| {
            let mut one = item(n, Value::Null);
            one["loadId"] = json!(id(0x100 + n));
            one
        })
        .collect();
    let mut duplicate = item(2, Value::Null);
    duplicate["callId"] = json!(id(1));
    duplicate["loadId"] = json!(id(0xccc));
    for refused in [
        json!({"loads":[]}),
        json!({"loads":nine}),
        json!({"loads":[item(1, Value::Null), item(2, Value::Null)]}),
        json!({"loads":[item(1, Value::Null), duplicate]}),
        json!({"loads":[item(1, Value::Null)],"extra":1}),
        json!({"loads":[{"loadId":"not-a-uuid"}]}),
    ] {
        let error = validate_load_batch(refused.to_string().as_bytes()).unwrap_err();
        assert_eq!(error.code, code::REQUEST_INVALID, "{refused}");
    }
}
