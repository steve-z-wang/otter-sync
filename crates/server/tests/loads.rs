//! Native Load pages on the server: one page is one owner-scoped durable call
//! executed in the caller's transaction, resolved through batched stamp and
//! Loader reads, and replayed from its saved outcome.
mod capability;
mod support;

use axton_core::{Continuation, LoadBatchRequest, LoadBatchResponse, canonical_json, limits};
use axton_server::{
    Config, Host, HostResult, LoadFault, LoadItemAnswer, code, encode_load_batch,
    host::HostRequest, load_fault_outcome, process_action, process_load, validate_load_batch,
};
use serde_json::{Value, json};
use std::{future::Future, pin::Pin};
use support::{Backend, Tables, add, remove, run};

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
        &crate::capability::request(item.to_string().as_bytes()),
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
fn a_loader_failure_a_miscounted_answer_and_an_unregistered_loader_are_saved_page_failures() {
    for (loaded, expected) in [
        (json!({"error":"TypeError: boom"}), code::LOADER_FAILED),
        (json!([{"id":"t1","title":"T1"}]), code::LOADER_INVALID),
    ] {
        let backend = Backend::new();
        seed_todos(&backend, 2);
        backend.script("ProjectTodos", answer_of_two());
        let host = Replacing {
            backend: &backend,
            op: "load",
            answer: Ok(loaded.clone()),
        };
        let failed = page_as(&host, &config(), "alice", &item(1, Value::Null));
        assert_eq!(outcome_code(&failed), expected, "{loaded}");
        assert_eq!(saved(&backend, 1), Some(failed), "{loaded}");
        assert_eq!(
            backend.stamp("Todo", "t1"),
            None,
            "{loaded}: stamps roll back"
        );
    }
    // A Model whose Loader is not registered fails before any read.
    let mut unregistered = config();
    unregistered.loaders.retain(|model| model != "Project");
    let backend = Backend::new();
    seed_todos(&backend, 1);
    backend.script(
        "ProjectTodos",
        answer(ids(&["t1"]), ids(&["p1"]), Value::Null),
    );
    let failed = page_as(&backend, &unregistered, "alice", &item(1, Value::Null));
    assert_eq!(outcome_code(&failed), code::LOADER_UNREGISTERED);
    assert_eq!(saved(&backend, 1), Some(failed));
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
    forged["memberships"] = json!([{"kind":"add","channel":"c","record":{"model":"Todo","identity":{"id":"t1"}},"tags":[]}]);
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
    // A missing or malformed wrapper is the same saved rejection whatever
    // host bridge produced it, and it is judged before the data.
    for answered in [
        answer(json!([]), json!([]), json!({"state":1,"more":2})),
        answer(json!([]), json!([]), json!({})),
        answer(json!([]), json!([]), json!(1)),
        json!({"data":{"todos":[],"projects":[]}}),
        json!({"data":[],"next":{"state":1,"more":2}}),
    ] {
        let backend = Backend::new();
        backend.script("ProjectTodos", answered.clone());
        let failed = page(&backend, &item(1, Value::Null));
        assert_eq!(
            outcome_code(&failed),
            code::LOAD_INVALID_CONTINUATION,
            "{answered}"
        );
        assert_eq!(saved(&backend, 1), Some(failed), "{answered}");
    }
    // Data that is not an object, with a valid `next`, is an invalid answer.
    let backend = Backend::new();
    backend.script("ProjectTodos", json!({"data":[],"next":null}));
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
            &crate::capability::request(request.to_string().as_bytes()),
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
        &crate::capability::request(item(2, Value::Null).to_string().as_bytes()),
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
        // A retryable database error the bridge rethrows from a Loader (a
        // Loader's own throw is a saved `loader.failed`).
        (
            "load",
            Err("serialization failure rethrown by the bridge".to_string()),
            code::HOST,
        ),
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
            &crate::capability::request(item(1, Value::Null).to_string().as_bytes()),
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
        &crate::capability::request(item(1, Value::Null).to_string().as_bytes()),
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
    let items = validate_load_batch(&crate::capability::request(
        json!({"loads":[first, unknown]}).to_string().as_bytes(),
    ))
    .unwrap();
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
        let error =
            validate_load_batch(&crate::capability::request(refused.to_string().as_bytes()))
                .unwrap_err();
        assert_eq!(error.code, code::REQUEST_INVALID, "{refused}");
    }
}

/// A title length that makes the one-record page of `t1` encode to exactly
/// `limits::LOAD_PAGE_BYTES - slack` bytes.
fn title_for(slack: usize) -> usize {
    let backend = Backend::new();
    backend.seed("Todo", "t1", json!({"id":"t1","title":""}), None);
    backend.script("ProjectTodos", answer(ids(&["t1"]), json!([]), Value::Null));
    let empty = page(&backend, &item(1, Value::Null)).to_string().len();
    limits::LOAD_PAGE_BYTES - slack - empty
}

#[test]
fn a_replayed_page_that_outgrows_the_page_bound_answers_an_unsaved_page_too_large() {
    let backend = Backend::new();
    let title = "x".repeat(title_for(4));
    backend.seed("Todo", "t1", json!({"id":"t1","title":title}), None);
    backend.script("ProjectTodos", answer(ids(&["t1"]), json!([]), Value::Null));
    let first = page(&backend, &item(1, Value::Null));
    assert_eq!(first["outcome"]["status"], "succeeded");
    assert_eq!(first.to_string().len(), limits::LOAD_PAGE_BYTES - 4);
    let saved_page = saved(&backend, 1);
    // A compatible read-contract change adds `"note":null` to the record.
    let note = json!({"name":"note","type":{"kind":"scalar","name":"string"},"nullable":true});
    let upgraded = config_with(json!([field("id"), field("title"), note]));
    backend.clear_log();
    let replay = page_as(&backend, &upgraded, "alice", &item(1, Value::Null));
    assert_eq!(outcome_code(&replay), code::LOAD_PAGE_TOO_LARGE);
    assert_eq!(backend.ops(), ["claimCall"], "no handler, Loader or save");
    assert_eq!(saved(&backend, 1), saved_page, "the saved page is kept");
    // Under the contract it was saved for, it still replays.
    assert_eq!(page(&backend, &item(1, Value::Null)), first);
}

/// `count` distinct items of the `ProjectTodos` Load, canonical as the
/// batch validator answers them, and the request the client froze.
fn batch_items(count: u64) -> (Vec<String>, LoadBatchRequest) {
    let items: Vec<Value> = (1..=count)
        .map(|n| {
            let mut one = item(n, Value::Null);
            one["loadId"] = json!(id(0x100 + n));
            one
        })
        .collect();
    let body = json!({ "loads": items }).to_string();
    (
        validate_load_batch(&crate::capability::request(body.as_bytes())).unwrap(),
        LoadBatchRequest::decode_envelope(body.as_bytes()).unwrap(),
    )
}
fn process(backend: &Backend, item: &str) -> String {
    run(process_load(
        &config(),
        "alice",
        &crate::capability::request(item.as_bytes()),
        backend,
    ))
    .unwrap()
}
fn decoded(response: &str) -> Vec<Value> {
    serde_json::from_str::<Value>(response).unwrap()["loads"]
        .as_array()
        .unwrap()
        .clone()
}

#[test]
fn faults_are_classified_in_rust_into_bounded_unsaved_items() {
    let (items, request) = batch_items(6);
    let backend = Backend::new();
    backend.script("ProjectTodos", answer(json!([]), json!([]), Value::Null));
    let engine = |code: &str, message: String| {
        LoadItemAnswer::Fault(LoadFault::Engine {
            code: code.into(),
            message,
        })
    };
    let answers = vec![
        LoadItemAnswer::Page(process(&backend, &items[0])),
        engine(code::HOST, "connection reset by 10.0.0.7".into()),
        engine(code::STORAGE_INVALID, "é".repeat(2000)),
        engine("Not A Code", "defect".into()),
        LoadItemAnswer::Fault(LoadFault::Conflict),
        LoadItemAnswer::Fault(LoadFault::Unavailable),
    ];
    let response = encode_load_batch(&items, answers).unwrap();
    let loads = decoded(&response);
    let outcome = |n: usize| {
        let outcome = &loads[n]["outcome"];
        (
            outcome["status"].as_str().unwrap().to_string(),
            outcome["error"]["code"].as_str().unwrap_or("").to_string(),
        )
    };
    assert_eq!(outcome(0), ("succeeded".into(), "".into()));
    assert_eq!(
        outcome(1),
        ("retryable".into(), code::SERVER_UNAVAILABLE.into())
    );
    assert!(
        !response.contains("10.0.0.7"),
        "no host text reaches the client"
    );
    assert_eq!(outcome(2), ("failed".into(), code::STORAGE_INVALID.into()));
    assert!(
        loads[2]["outcome"]["error"]["message"]
            .as_str()
            .unwrap()
            .len()
            <= 1024
    );
    assert_eq!(outcome(3), ("failed".into(), code::INTERNAL.into()));
    assert_eq!(
        outcome(4),
        ("retryable".into(), code::TRANSACTION_CONFLICT.into())
    );
    assert_eq!(
        outcome(5),
        ("retryable".into(), code::SERVER_UNAVAILABLE.into())
    );
    for (n, load) in loads.iter().enumerate() {
        let item: Value = serde_json::from_str(&items[n]).unwrap();
        assert_eq!(
            (&load["loadId"], &load["callId"]),
            (&item["loadId"], &item["callId"])
        );
    }
    // The client accepts the envelope and every item on its own terms.
    let replies = LoadBatchResponse::decode(response.as_bytes(), &request).unwrap();
    assert!(replies.iter().all(|reply| reply.page.is_ok()));
    // The answers must pair with the items.
    let error = encode_load_batch(&items, vec![]).unwrap_err();
    assert_eq!(error.code, code::INTERNAL);
}

#[test]
fn a_page_that_answers_another_item_or_is_malformed_fails_only_its_own_item() {
    let (items, _) = batch_items(3);
    let backend = Backend::new();
    backend.script("ProjectTodos", answer(json!([]), json!([]), Value::Null));
    // The first item is answered with its sibling's page.
    let second = process(&backend, &items[1]);
    let answers = vec![
        LoadItemAnswer::Page(second.clone()),
        LoadItemAnswer::Page(second),
        LoadItemAnswer::Page(r#"{"loadId":1}"#.into()),
    ];
    let loads = decoded(&encode_load_batch(&items, answers).unwrap());
    assert_eq!(loads[0]["outcome"]["error"]["code"], code::INTERNAL);
    assert_eq!(loads[0]["records"], json!([]));
    assert_eq!(loads[1]["outcome"]["status"], "succeeded");
    assert_eq!(loads[2]["outcome"]["error"]["code"], code::INTERNAL);
}

#[test]
fn the_response_holds_eight_full_pages_within_its_bound_and_an_oversized_page_fails_alone() {
    let (items, request) = batch_items(8);
    let title = "x".repeat(title_for(0));
    let backend = Backend::new();
    backend.seed("Todo", "t1", json!({"id":"t1","title":title}), None);
    backend.script("ProjectTodos", answer(ids(&["t1"]), json!([]), Value::Null));
    let pages: Vec<String> = items.iter().map(|item| process(&backend, item)).collect();
    assert!(
        pages
            .iter()
            .all(|page| page.len() == limits::LOAD_PAGE_BYTES)
    );
    let full = encode_load_batch(
        &items,
        pages.iter().cloned().map(LoadItemAnswer::Page).collect(),
    )
    .unwrap();
    assert!(full.len() > 8 * limits::LOAD_PAGE_BYTES);
    assert!(full.len() <= limits::LOAD_RESPONSE_BYTES);
    let replies = LoadBatchResponse::decode(full.as_bytes(), &request).unwrap();
    assert!(replies.iter().all(|reply| reply.page.is_ok()));
    // A page past the page bound (here grown by a byte) is replaced by an
    // unsaved `load.page_too_large`; its siblings keep their pages.
    let mut grown: Value = serde_json::from_str(&pages[3]).unwrap();
    grown["records"][0]["state"]["title"] = json!(format!("{title}y"));
    let mut answers: Vec<LoadItemAnswer> = pages.into_iter().map(LoadItemAnswer::Page).collect();
    answers[3] = LoadItemAnswer::Page(axton_core::canonical_json(&grown).unwrap());
    let loads = decoded(&encode_load_batch(&items, answers).unwrap());
    assert_eq!(
        loads[3]["outcome"]["error"]["code"],
        code::LOAD_PAGE_TOO_LARGE
    );
    assert_eq!(loads[3]["records"], json!([]));
    assert!(
        loads
            .iter()
            .enumerate()
            .all(|(n, load)| n == 3 || load["outcome"]["status"] == "succeeded")
    );
}

/// A settled answer whose add-only Channel handles declared `memberships`.
fn enrolling(todos: Value, projects: Value, memberships: Vec<Value>) -> Value {
    let mut answered = answer(todos, projects, Value::Null);
    answered["memberships"] = Value::Array(memberships);
    answered
}
/// Everything a page may change besides its own saved call: rows, stamps,
/// heads, positions and memberships.
fn durable(backend: &Backend) -> Tables {
    let mut tables = backend.tables();
    tables.calls.clear();
    tables
}
/// The operations shared settlement issued, by name.
fn settlement_ops(backend: &Backend) -> Vec<String> {
    let settlement = [
        "advanceStamp",
        "ensureStamp",
        "lockRecord",
        "memberships",
        "lockChannels",
        "readChannelMembers",
        "applyChannelMembers",
    ];
    backend
        .ops()
        .into_iter()
        .filter(|op| settlement.contains(&op.as_str()))
        .collect()
}
fn repeated(op: &str, count: usize) -> Vec<String> {
    vec![op.to_string(); count]
}
fn strings(ops: &[&str]) -> Vec<String> {
    ops.iter().map(|op| op.to_string()).collect()
}

#[test]
fn a_page_enrolls_loaded_records_at_their_unchanged_stamps_once_per_new_pair() {
    let backend = Backend::new();
    seed_todos(&backend, 2);
    backend.seed("Todo", "t1", json!({"id":"t1","title":"T1"}), Some(5));
    backend.script(
        "ProjectTodos",
        enrolling(
            ids(&["t1", "t2"]),
            json!([]),
            vec![add("c", "Todo", "t1"), add("c", "Todo", "t2")],
        ),
    );
    let first = page(&backend, &item(1, Value::Null));
    assert_eq!(first["outcome"]["status"], "succeeded", "{first}");
    assert_eq!(
        first["outcome"]["data"],
        json!({"todos":[{"id":"t1"},{"id":"t2"}],"projects":[]}),
        "the page answers exactly what it answered before enrollment existed"
    );
    assert_eq!(
        first["memberships"],
        json!([
            {"channel":"c","cursor":1,"model":"Todo","identity":{"id":"t1"}},
            {"channel":"c","cursor":2,"model":"Todo","identity":{"id":"t2"}}
        ])
    );
    assert_eq!(saved(&backend, 1), Some(first.clone()));
    assert_eq!(backend.members("Todo", "t1"), ["c"]);
    assert_eq!(backend.members("Todo", "t2"), ["c"]);
    // A loaded record keeps its stamp; one without evidence starts at 1. Each
    // position carries exactly the stamp the page's own record carries.
    assert_eq!(backend.stamp("Todo", "t1"), Some(5));
    assert_eq!(backend.stamp("Todo", "t2"), Some(1));
    assert_eq!(backend.invalidation("c", "Todo", "t1"), Some((1, 5)));
    assert_eq!(backend.invalidation("c", "Todo", "t2"), Some((2, 1)));
    for record in first["records"].as_array().unwrap() {
        let id = record["identity"]["id"].as_str().unwrap();
        let (_, stamp) = backend.invalidation("c", "Todo", id).unwrap();
        assert_eq!(record["stamp"], json!(stamp), "{id}");
    }
    assert_eq!(backend.head("c"), 2);
    assert_eq!(
        backend.ops(),
        [
            strings(&[
                "claimCall",
                "savepoint",
                "handleLoad",
                "lockChannels",
                "readStamps",
                "load"
            ]),
            repeated("ensureStamp", 2),
            strings(&["readChannelMembers", "applyChannelMembers"]),
            strings(&["release", "saveCall"]),
        ]
        .concat(),
        "the Channels lock before the page's reads; settlement runs after them and before its release; nothing advances a stamp"
    );

    // A fresh page re-adding existing members publishes nothing more: each
    // unchanged member keeps, and answers, its existing position.
    backend.clear_log();
    let before = durable(&backend);
    let again = page(&backend, &item(2, Value::Null));
    assert_eq!(again["outcome"]["status"], "succeeded");
    assert_eq!(durable(&backend), before);
    assert_eq!(
        settlement_ops(&backend),
        [
            strings(&["lockChannels"]),
            repeated("ensureStamp", 2),
            strings(&["readChannelMembers", "applyChannelMembers"]),
        ]
        .concat()
    );
    assert!(backend.deltas().iter().all(|delta| !delta.publish));
}

/// An enrolling page locks exactly its Channels, once and in byte order,
/// before `readStamps` may insert a record row; a page that enrolls nothing
/// locks none.
#[test]
fn an_enrolling_page_locks_its_channels_before_any_record_row() {
    let backend = Backend::new();
    seed_todos(&backend, 1);
    backend.script(
        "ProjectTodos",
        enrolling(
            ids(&["t1"]),
            json!([]),
            vec![add("d", "Todo", "t1"), add("c", "Todo", "t1")],
        ),
    );
    let result = page(&backend, &item(1, Value::Null));
    assert_eq!(result["outcome"]["status"], "succeeded", "{result}");
    let ops = backend.ops();
    let at = |op: &str| ops.iter().position(|name| name == op).unwrap();
    assert!(at("lockChannels") < at("readStamps"), "{ops:?}");
    assert_eq!(backend.count("lockChannels"), 1, "{ops:?}");
    assert!(backend.log().contains(&HostRequest::LockChannels {
        channels: vec!["c".into(), "d".into()]
    }));

    backend.clear_log();
    backend.script("ProjectTodos", answer(ids(&["t1"]), json!([]), Value::Null));
    let quiet = page(&backend, &item(2, Value::Null));
    assert_eq!(quiet["outcome"]["status"], "succeeded", "{quiet}");
    assert_eq!(backend.count("lockChannels"), 0);
}

/// A Load's add carries tags like a Mutation's: a repeated pair unions its
/// tags into the first, and the pair still takes one position.
#[test]
fn a_page_enrolls_with_tags_and_a_repeated_pair_unions_them() {
    let backend = Backend::new();
    seed_todos(&backend, 1);
    backend.script(
        "ProjectTodos",
        enrolling(
            ids(&["t1"]),
            json!([]),
            vec![
                support::add_tagged("c", "Todo", "t1", &["X"]),
                support::add_tagged("c", "Todo", "t1", &["Y", "X"]),
            ],
        ),
    );
    let result = page(&backend, &item(1, Value::Null));
    assert_eq!(result["outcome"]["status"], "succeeded", "{result}");
    assert_eq!(
        backend.tagged_members("c"),
        [("t1".to_string(), vec!["X".to_string(), "Y".to_string()])]
    );
    assert_eq!(backend.positions("c"), [(1, "t1".into(), "upsert")]);
}

#[test]
fn valid_load_declarations_can_union_more_than_64_tags_and_replay() {
    let backend = Backend::new();
    seed_todos(&backend, 1);
    let tags: Vec<String> = (0..64).map(|i| format!("t{i:02}")).collect();
    let refs: Vec<&str> = tags.iter().map(String::as_str).collect();
    backend.script(
        "ProjectTodos",
        enrolling(
            ids(&["t1"]),
            json!([]),
            vec![
                support::add_tagged("c", "Todo", "t1", &refs),
                support::add_tagged("c", "Todo", "t1", &["t00", "t64"]),
            ],
        ),
    );
    let request = item(1, Value::Null);
    let result = page(&backend, &request);
    assert_eq!(result["outcome"]["status"], "succeeded", "{result}");
    let expected: Vec<String> = (0..65).map(|i| format!("t{i:02}")).collect();
    assert_eq!(backend.tagged_members("c"), [("t1".to_string(), expected)]);
    assert_eq!(backend.positions("c"), [(1, "t1".into(), "upsert")]);
    assert_eq!(page(&backend, &request), result);
    assert_eq!(backend.positions("c"), [(1, "t1".into(), "upsert")]);
}

#[test]
fn one_record_joins_several_channels_without_republishing_to_its_existing_ones() {
    let backend = Backend::new();
    backend.seed("Todo", "t1", json!({"id":"t1","title":"T1"}), Some(3));
    backend.enroll("b", "Todo", "t1", 7);
    backend.script(
        "ProjectTodos",
        enrolling(
            ids(&["t1"]),
            json!([]),
            vec![add("a", "Todo", "t1"), add("c", "Todo", "t1")],
        ),
    );
    let result = page(&backend, &item(1, Value::Null));
    assert_eq!(result["outcome"]["status"], "succeeded", "{result}");
    assert_eq!(backend.members("Todo", "t1"), ["a", "b", "c"]);
    assert_eq!(backend.invalidation("a", "Todo", "t1"), Some((1, 3)));
    assert_eq!(backend.invalidation("c", "Todo", "t1"), Some((1, 3)));
    assert_eq!(backend.invalidation("b", "Todo", "t1"), Some((8, 3)));
    assert_eq!(
        backend.head("b"),
        8,
        "an existing Channel is not republished"
    );
    assert_eq!(backend.stamp("Todo", "t1"), Some(3));
    assert_eq!(
        settlement_ops(&backend),
        strings(&[
            "lockChannels",
            "ensureStamp",
            "readChannelMembers",
            "readChannelMembers",
            "applyChannelMembers"
        ])
    );
}

#[test]
fn repeated_declarations_across_outputs_and_mixed_lists_are_one_effect_per_pair() {
    let backend = Backend::new();
    seed_todos(&backend, 2);
    backend.seed("Project", "p1", json!({"id":"p1","name":"P"}), None);
    backend.script(
        "ProjectTodos",
        enrolling(
            ids(&["t1", "t2", "t1"]),
            ids(&["p1", "p1"]),
            vec![
                add("c", "Todo", "t1"),
                add("c", "Project", "p1"),
                add("c", "Todo", "t1"),
                add("c", "Todo", "t2"),
                add("d", "Project", "p1"),
                add("c", "Project", "p1"),
            ],
        ),
    );
    let result = page(&backend, &item(1, Value::Null));
    assert_eq!(result["outcome"]["status"], "succeeded", "{result}");
    assert_eq!(backend.members("Todo", "t1"), ["c"]);
    assert_eq!(backend.members("Todo", "t2"), ["c"]);
    assert_eq!(backend.members("Project", "p1"), ["c", "d"]);
    assert_eq!(backend.head("c"), 3, "one position per distinct pair");
    assert_eq!(backend.head("d"), 1);
    assert_eq!(backend.count("ensureStamp"), 3, "one guard per record");
    assert_eq!(backend.count("memberships"), 0, "nothing is touched");
    assert_eq!(
        backend.count("readChannelMembers"),
        2,
        "one read per Channel"
    );
    assert_eq!(backend.deltas().len(), 4, "one final state per pair");
    assert_eq!(backend.publishes().len(), 4);
    assert_eq!(backend.count("advanceStamp"), 0);
}

/// A page that must fail with `expected` before any stamp or Loader read,
/// saved as a terminal rejection that keeps nothing it did.
fn refused_before_resolution(backend: &Backend, config: &Config, expected: &str, case: &str) {
    let before = durable(backend);
    let failed = page_as(backend, config, "alice", &item(1, Value::Null));
    assert_eq!(outcome_code(&failed), expected, "{case}");
    assert_eq!(saved(backend, 1), Some(failed), "{case}");
    assert_eq!(durable(backend), before, "{case}: nothing is kept");
    assert_eq!(backend.count("rollback"), 1, "{case}");
    assert_eq!(backend.count("readStamps"), 0, "{case}");
    assert_eq!(backend.count("load"), 0, "{case}");
    assert_eq!(settlement_ops(backend), Vec::<String>::new(), "{case}");
}

#[test]
fn enrollment_a_load_may_not_declare_is_a_saved_handler_failure_that_keeps_nothing() {
    let intent = |channel: &str, model: &str, identity: Value| json!({"kind":"add","channel":channel,"record":{"model":model,"identity":identity},"tags":[]});
    let cases = [
        ("a removal", vec![remove("c", "Todo", "t1")]),
        (
            "a removal after an addition",
            vec![add("c", "Todo", "t1"), remove("d", "Todo", "t1")],
        ),
        ("a record outside the page", vec![add("c", "Todo", "t9")]),
        (
            "an identity of another Model",
            vec![add("c", "Project", "t1")],
        ),
        ("a Model outside the schema", vec![add("c", "Ghost", "t1")]),
        (
            "65 tags in one declaration",
            vec![
                json!({"kind":"add","channel":"c","record":{"model":"Todo","identity":{"id":"t1"}},"tags": (0..65).map(|i| format!("t{i}")).collect::<Vec<_>>()}),
            ],
        ),
        ("a blank Channel", vec![add("  ", "Todo", "t1")]),
        (
            "a mistyped identity",
            vec![intent("c", "Todo", json!({"id":7}))],
        ),
        (
            "an identity with extra members",
            vec![intent("c", "Todo", json!({"id":"t1","title":"T1"}))],
        ),
        (
            "an invalid declaration after a valid one",
            vec![add("c", "Todo", "t1"), add("c", "Todo", "t9")],
        ),
        // A Load has no tag selector, and its tags follow the add rules:
        // each is refused before any read, never dropped.
        (
            "a tag selector",
            vec![json!({"kind":"removeTag","channel":"c","tag":"X"})],
        ),
        (
            "a blank tag",
            vec![support::add_tagged("c", "Todo", "t1", &["\u{feff}"])],
        ),
        (
            "a tag past 256 bytes",
            vec![support::add_tagged("c", "Todo", "t1", &[&"x".repeat(257)])],
        ),
        (
            "a repeated pair adding a blank tag",
            vec![
                add("c", "Todo", "t1"),
                support::add_tagged("c", "Todo", "t1", &[" "]),
            ],
        ),
    ];
    for (case, memberships) in cases {
        let backend = Backend::new();
        seed_todos(&backend, 1);
        backend.seed("Project", "p1", json!({"id":"p1","name":"P"}), Some(2));
        backend.script(
            "ProjectTodos",
            enrolling(ids(&["t1"]), ids(&["p1"]), memberships),
        );
        refused_before_resolution(&backend, &config(), code::HANDLER_INVALID, case);
    }
}

#[test]
fn an_empty_page_cannot_enroll_and_an_invalid_page_fails_before_its_enrollment() {
    let cases = [
        (
            "an empty page",
            enrolling(json!([]), json!([]), vec![add("c", "Todo", "t1")]),
            code::HANDLER_INVALID,
        ),
        (
            "an invalid continuation",
            {
                let mut answered = enrolling(ids(&["t1"]), json!([]), vec![add("c", "Todo", "t1")]);
                answered["next"] = json!({"state":1,"more":2});
                answered
            },
            code::LOAD_INVALID_CONTINUATION,
        ),
        (
            "invalid data",
            json!({"data":{"todos":[{"id":7}],"projects":[]},"next":null,
                "memberships":[add("c", "Todo", "t1")]}),
            code::HANDLER_INVALID,
        ),
    ];
    for (case, answered, expected) in cases {
        let backend = Backend::new();
        seed_todos(&backend, 1);
        backend.script("ProjectTodos", answered);
        refused_before_resolution(&backend, &config(), expected, case);
    }
}

#[test]
fn enrolling_a_model_without_a_registered_loader_is_loader_unregistered_before_any_read() {
    let mut unregistered = config();
    unregistered.loaders.retain(|model| model != "Project");
    let backend = Backend::new();
    seed_todos(&backend, 1);
    backend.seed("Project", "p1", json!({"id":"p1","name":"P"}), Some(2));
    backend.script(
        "ProjectTodos",
        enrolling(ids(&["t1"]), json!([]), vec![add("c", "Project", "p1")]),
    );
    refused_before_resolution(
        &backend,
        &unregistered,
        code::LOADER_UNREGISTERED,
        "device-only",
    );
}

#[test]
fn enrollment_is_bounded_by_distinct_pairs_after_deduplication() {
    let channels: Vec<String> = (0..limits::LOAD_ENROLLMENT_PAIRS)
        .map(|n| format!("c{n}"))
        .collect();
    // Exactly the bound, each pair declared twice.
    let backend = Backend::new();
    seed_todos(&backend, 1);
    let twice: Vec<Value> = channels
        .iter()
        .chain(&channels)
        .map(|channel| add(channel, "Todo", "t1"))
        .collect();
    backend.script("ProjectTodos", enrolling(ids(&["t1"]), json!([]), twice));
    let result = page(&backend, &item(1, Value::Null));
    assert_eq!(result["outcome"]["status"], "succeeded");
    assert_eq!(
        backend.members("Todo", "t1").len(),
        limits::LOAD_ENROLLMENT_PAIRS
    );
    assert_eq!(backend.publishes().len(), limits::LOAD_ENROLLMENT_PAIRS);

    // One distinct pair more is a saved size failure that keeps nothing.
    let backend = Backend::new();
    seed_todos(&backend, 1);
    let over: Vec<Value> = channels
        .iter()
        .map(String::as_str)
        .chain(["one-more"])
        .map(|channel| add(channel, "Todo", "t1"))
        .collect();
    backend.script("ProjectTodos", enrolling(ids(&["t1"]), json!([]), over));
    refused_before_resolution(
        &backend,
        &config(),
        code::LOAD_PAGE_TOO_LARGE,
        "one pair over",
    );
}

/// The encoded bytes of one `t1` enrollment into `channel`, as the shared
/// fixture defines them.
fn pair_bytes(channel: &str) -> usize {
    canonical_json(&add(channel, "Todo", "t1")).unwrap().len()
}

#[test]
fn enrollment_is_bounded_by_its_encoded_bytes_after_deduplication() {
    // 256 distinct Channels whose pairs encode to exactly the bound.
    let count = 256;
    let each = limits::LOAD_ENROLLMENT_BYTES / count;
    assert_eq!(each * count, limits::LOAD_ENROLLMENT_BYTES);
    let channels: Vec<String> = (0..count)
        .map(|n| {
            let name = format!("{n:03}");
            let pad = each - pair_bytes(&name);
            format!("{name}{}", "x".repeat(pad))
        })
        .collect();
    assert_eq!(
        channels.iter().map(|c| pair_bytes(c)).sum::<usize>(),
        limits::LOAD_ENROLLMENT_BYTES
    );
    let backend = Backend::new();
    seed_todos(&backend, 1);
    let mut at_bound: Vec<Value> = channels.iter().map(|c| add(c, "Todo", "t1")).collect();
    at_bound.push(add(&channels[0], "Todo", "t1"));
    backend.script("ProjectTodos", enrolling(ids(&["t1"]), json!([]), at_bound));
    let result = page(&backend, &item(1, Value::Null));
    assert_eq!(
        result["outcome"]["status"], "succeeded",
        "a repeated pair counts once"
    );
    assert_eq!(backend.members("Todo", "t1").len(), count);

    // One byte more is a saved size failure.
    let backend = Backend::new();
    seed_todos(&backend, 1);
    let mut over = channels.clone();
    over[count - 1].push('x');
    let over = over.iter().map(|c| add(c, "Todo", "t1")).collect();
    backend.script("ProjectTodos", enrolling(ids(&["t1"]), json!([]), over));
    refused_before_resolution(
        &backend,
        &config(),
        code::LOAD_PAGE_TOO_LARGE,
        "one byte over",
    );

    // A repeated pair's unioned tag is measured again: three bytes over.
    let backend = Backend::new();
    seed_todos(&backend, 1);
    let mut tagged: Vec<Value> = channels.iter().map(|c| add(c, "Todo", "t1")).collect();
    tagged.push(support::add_tagged(&channels[0], "Todo", "t1", &["y"]));
    backend.script("ProjectTodos", enrolling(ids(&["t1"]), json!([]), tagged));
    refused_before_resolution(
        &backend,
        &config(),
        code::LOAD_PAGE_TOO_LARGE,
        "a unioned tag over",
    );
}

#[test]
fn the_enrollment_bounds_are_the_shared_cross_language_fixture() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../fixtures/protocol/load-enrollment-limits.json"
    ))
    .unwrap();
    assert_eq!(fixture["pairs"], json!(limits::LOAD_ENROLLMENT_PAIRS));
    assert_eq!(fixture["bytes"], json!(limits::LOAD_ENROLLMENT_BYTES));
    let intents = fixture["intents"].as_array().unwrap();
    assert!(!intents.is_empty());
    for case in intents {
        let encoded = canonical_json(&case["intent"]).unwrap();
        assert_eq!(
            encoded,
            case["encoded"].as_str().unwrap(),
            "{}",
            case["name"]
        );
        assert_eq!(json!(encoded.len()), case["bytes"], "{}", case["name"]);
    }
    assert_eq!(pair_bytes("project:p1"), 96, "the helper measures alike");
}

#[test]
fn a_page_that_fails_after_validation_keeps_no_enrollment_or_initialized_stamp() {
    type Arrange = fn(&Backend);
    let cases: [(&str, Arrange, Option<Value>, &str); 5] = [
        (
            "an absent row",
            |backend| backend.with(|s| s.tables.rows.clear()),
            None,
            code::LOAD_RECORD_UNAVAILABLE,
        ),
        (
            "a Loader refusal",
            |backend| backend.refuse_load("Todo", "t2"),
            None,
            "todo.forbidden",
        ),
        (
            "a Loader throw",
            |_| {},
            Some(json!({"error":"TypeError: boom"})),
            code::LOADER_FAILED,
        ),
        (
            "an invalid row",
            |backend| backend.seed("Todo", "t2", json!({"id":"t2","title":7}), None),
            None,
            code::LOADER_INVALID,
        ),
        (
            "an oversized page",
            |backend| {
                for n in 1..=2 {
                    backend.seed(
                        "Todo",
                        &todo(n),
                        json!({"id":todo(n),"title":"x".repeat(600 * 1024)}),
                        None,
                    );
                }
            },
            None,
            code::LOAD_PAGE_TOO_LARGE,
        ),
    ];
    for (case, arrange, loaded, expected) in cases {
        let backend = Backend::new();
        seed_todos(&backend, 2);
        arrange(&backend);
        backend.script(
            "ProjectTodos",
            enrolling(
                ids(&["t1", "t2"]),
                json!([]),
                vec![add("c", "Todo", "t1"), add("c", "Todo", "t2")],
            ),
        );
        let before = durable(&backend);
        let failed = match loaded {
            Some(loaded) => page_as(
                &Replacing {
                    backend: &backend,
                    op: "load",
                    answer: Ok(loaded),
                },
                &config(),
                "alice",
                &item(1, Value::Null),
            ),
            None => page(&backend, &item(1, Value::Null)),
        };
        assert_eq!(outcome_code(&failed), expected, "{case}");
        assert!(
            failed.get("memberships").is_none(),
            "{case}: a failed Loader never claims enrollment"
        );
        assert_eq!(saved(&backend, 1), Some(failed), "{case}");
        assert_eq!(
            durable(&backend),
            before,
            "{case}: no membership, position or stamp"
        );
        // Only the Channel locks taken before the reads; no settlement ran.
        assert_eq!(
            settlement_ops(&backend),
            strings(&["lockChannels"]),
            "{case}"
        );
    }
}

#[test]
fn a_host_fault_during_enrollment_escapes_the_page_transaction_and_a_retry_enrolls_once() {
    let cases = [
        (
            "ensureStamp",
            Err("connection reset".to_string()),
            code::HOST,
        ),
        (
            "lockChannels",
            Err("connection reset".to_string()),
            code::HOST,
        ),
        (
            "readChannelMembers",
            Err("serialization failure".to_string()),
            code::HOST,
        ),
        (
            "applyChannelMembers",
            Err("deadlock detected".to_string()),
            code::HOST,
        ),
        ("saveCall", Err("connection reset".to_string()), code::HOST),
        (
            "applyChannelMembers",
            Ok(
                json!([{"channel":"c","model":"Todo","identityKey":"{\"id\":\"t1\"}",
                "cursor":1,"kind":"remove"}]),
            ),
            code::HOST_INVALID,
        ),
    ];
    for (op, answer, expected) in cases {
        let backend = Backend::new();
        seed_todos(&backend, 1);
        backend.script(
            "ProjectTodos",
            enrolling(ids(&["t1"]), json!([]), vec![add("c", "Todo", "t1")]),
        );
        let committed = backend.tables();
        let host = Replacing {
            backend: &backend,
            op,
            answer,
        };
        let error = run(process_load(
            &config(),
            "alice",
            &crate::capability::request(item(1, Value::Null).to_string().as_bytes()),
            &host,
        ))
        .unwrap_err();
        assert_eq!(error.code, expected, "{op}");
        assert_eq!(saved(&backend, 1), None, "{op}: no success is saved");
        let outcome = load_fault_outcome(&LoadFault::Engine {
            code: error.code.clone(),
            message: error.message.clone(),
        });
        let retryable = matches!(outcome, axton_core::LoadOutcome::Retryable { .. });
        assert_eq!(retryable, expected == code::HOST, "{op}: {outcome:?}");

        // The host rolls the whole transaction back; resending the same call
        // ID then executes the page afresh and enrolls it once.
        backend.with(|s| s.tables = committed);
        let retried = page(&backend, &item(1, Value::Null));
        assert_eq!(retried["outcome"]["status"], "succeeded", "{op}");
        assert_eq!(backend.members("Todo", "t1"), ["c"], "{op}");
        assert_eq!(
            backend.invalidation("c", "Todo", "t1"),
            Some((1, 1)),
            "{op}"
        );
    }
    // A settlement whose Channel locks went stale, once the carrier's own
    // retries ran out, is retryable like a serialization conflict.
    let outcome = load_fault_outcome(&LoadFault::Engine {
        code: code::TRANSACTION_CONFLICT.into(),
        message: "Todo joined Channel d after settlement locked its Channels".into(),
    });
    assert!(
        matches!(&outcome, axton_core::LoadOutcome::Retryable { error } if error.code == code::TRANSACTION_CONFLICT),
        "{outcome:?}"
    );
}

#[test]
fn a_replayed_page_neither_enrolls_nor_undoes_a_later_removal_and_a_fresh_page_re_adds() {
    let backend = Backend::new();
    seed_todos(&backend, 1);
    backend.script(
        "ProjectTodos",
        enrolling(ids(&["t1"]), json!([]), vec![add("c", "Todo", "t1")]),
    );
    let first = page(&backend, &item(1, Value::Null));
    assert_eq!(
        first["memberships"],
        json!([{ "channel":"c", "cursor":1, "model":"Todo", "identity":{"id":"t1"} }])
    );
    assert_eq!(backend.members("Todo", "t1"), ["c"]);
    assert_eq!(backend.invalidation("c", "Todo", "t1"), Some((1, 1)));

    // An application later removes the record from the Channel.
    support::settle(&backend, vec![], vec![remove("c", "Todo", "t1")]);
    assert!(backend.members("Todo", "t1").is_empty());
    let removed = durable(&backend);

    backend.clear_log();
    assert_eq!(page(&backend, &item(1, Value::Null)), first);
    assert_eq!(
        backend.ops(),
        ["claimCall"],
        "no handler, Loader or settlement"
    );
    assert_eq!(durable(&backend), removed, "the removal stands");

    // A fresh page may add it again, at a new position and the same stamp.
    let fresh = page(&backend, &item(2, Value::Null));
    assert_eq!(fresh["outcome"]["status"], "succeeded");
    assert_eq!(backend.members("Todo", "t1"), ["c"]);
    assert_eq!(backend.invalidation("c", "Todo", "t1"), Some((3, 1)));
    assert_eq!(backend.stamp("Todo", "t1"), Some(1));
}

#[test]
fn batch_siblings_enroll_or_fail_independently() {
    let (items, request) = batch_items(3);
    let backend = Backend::new();
    seed_todos(&backend, 2);
    let answers: Vec<LoadItemAnswer> = [
        enrolling(ids(&["t1"]), json!([]), vec![add("c", "Todo", "t1")]),
        enrolling(ids(&["t1"]), json!([]), vec![add("x", "Todo", "t2")]),
        enrolling(ids(&["t2"]), json!([]), vec![add("d", "Todo", "t2")]),
    ]
    .into_iter()
    .zip(&items)
    .map(|(answered, item)| {
        backend.script("ProjectTodos", answered);
        LoadItemAnswer::Page(process(&backend, item))
    })
    .collect();
    let response = encode_load_batch(&items, answers).unwrap();
    let loads = decoded(&response);
    assert_eq!(loads[0]["outcome"]["status"], "succeeded");
    assert_eq!(outcome_code(&loads[1]), code::HANDLER_INVALID);
    assert_eq!(loads[2]["outcome"]["status"], "succeeded");
    assert_eq!(backend.members("Todo", "t1"), ["c"]);
    assert_eq!(backend.members("Todo", "t2"), ["d"]);
    assert_eq!(backend.head("x"), 0);
    let replies = LoadBatchResponse::decode(response.as_bytes(), &request).unwrap();
    assert!(replies.iter().all(|reply| reply.page.is_ok()));
}

#[test]
fn enrollment_adds_per_record_guards_and_one_read_per_channel_and_one_write_to_the_fixed_page_path()
{
    let fixed_page = |settlement: Vec<String>| {
        [
            strings(&[
                "claimCall",
                "savepoint",
                "handleLoad",
                "lockChannels",
                "readStamps",
                "load",
            ]),
            settlement,
            strings(&["release", "saveCall"]),
        ]
        .concat()
    };
    let backend = Backend::new();
    seed_todos(&backend, 1000);
    let all: Vec<String> = (1..=1000).map(todo).collect();
    let all: Vec<&str> = all.iter().map(String::as_str).collect();
    let into = |channel: &str, ids: &[&str]| -> Vec<Value> {
        ids.iter().map(|id| add(channel, "Todo", id)).collect()
    };

    // 1,000 records newly joining one Channel: per record one ensureStamp,
    // then one member read and one write for the whole page.
    let settled = |guards: usize, reads: usize| {
        [
            repeated("ensureStamp", guards),
            repeated("readChannelMembers", reads),
            strings(&["applyChannelMembers"]),
        ]
        .concat()
    };
    backend.script(
        "ProjectTodos",
        enrolling(ids(&all), json!([]), into("c", &all)),
    );
    let result = page(&backend, &item(1, Value::Null));
    assert_eq!(result["outcome"]["status"], "succeeded");
    assert_eq!(backend.ops(), fixed_page(settled(1000, 1)));
    assert_eq!(backend.publishes().len(), 1000);

    // The same 1,000 re-added by a fresh page: the same operations, and the
    // write only answers their existing positions.
    backend.clear_log();
    page(&backend, &item(2, Value::Null));
    assert_eq!(backend.ops(), fixed_page(settled(1000, 1)));
    assert_eq!(backend.publishes().len(), 0);

    // 500 records joining two new Channels: the per-record work is shared.
    backend.clear_log();
    let half = &all[..500];
    backend.script(
        "ProjectTodos",
        enrolling(
            ids(half),
            json!([]),
            [into("a", half), into("b", half)].concat(),
        ),
    );
    page(&backend, &item(3, Value::Null));
    assert_eq!(backend.ops(), fixed_page(settled(500, 2)));
    assert_eq!(backend.publishes().len(), 1000);
}

#[test]
fn enrollment_normalizes_intent_and_saved_claim_identities_without_refreshing_cursors() {
    let lower = "01890f47-1234-7123-8123-123456789abc";
    let upper = lower.to_uppercase();
    let original_config = config();
    let mut schema = serde_json::to_value(&original_config.schema).unwrap();
    schema["models"][0]["fields"][0]["type"]["name"] = json!("uuid");
    schema["resultModels"][0]["fields"][0]["type"]["name"] = json!("uuid");
    schema["loads"][0]["outputs"][0]["handlerType"]["fields"][0]["type"]["name"] = json!("uuid");
    let cfg = Config::decode(json!({"schema":schema,"loaders":["Todo","Project"],"mutations":[]}))
        .unwrap();
    let backend = Backend::new();
    backend.seed("Todo", lower, json!({"id":lower,"title":"T"}), Some(1));
    backend.with(|s| {
        s.tables.heads.insert("c".into(), 9);
    });
    backend.script(
        "ProjectTodos",
        enrolling(ids(&[lower]), json!([]), vec![add("c", "Todo", &upper)]),
    );
    let first = page_as(&backend, &cfg, "alice", &item(1, Value::Null));
    assert_eq!(
        first["memberships"],
        json!([{ "channel":"c","cursor":10,"model":"Todo","identity":{"id":lower} }])
    );
    support::settle(&backend, vec![], vec![remove("c", "Todo", lower)]);
    let removed = durable(&backend);
    // A saved legacy representation uses the equivalent noncanonical UUID.
    backend.with(|s| {
        let response = s.tables.calls.get_mut(&id(1)).unwrap().1.as_mut().unwrap();
        let mut value: Value = serde_json::from_str(response).unwrap();
        value["records"][0]["identity"]["id"] = json!(upper);
        value["memberships"][0]["identity"]["id"] = json!(upper);
        *response = value.to_string();
    });
    backend.clear_log();
    let replay = page_as(&backend, &cfg, "alice", &item(1, Value::Null));
    assert_eq!(replay["records"][0]["identity"]["id"], lower);
    assert_eq!(replay["memberships"], first["memberships"]);
    assert_eq!(backend.ops(), ["claimCall"]);
    assert_eq!(durable(&backend), removed);
}

#[test]
fn upgraded_retry_compares_saved_logical_load_without_reenrolling_or_inventing_claims() {
    let backend = Backend::new();
    backend.seed("Todo", "t1", json!({"title":"first"}), None);
    backend.script("ProjectTodos", json!({"data":{"todos":[{"id":"t1"}],"projects":[]},"next":null,"memberships":[add("room","Todo","t1")]}));
    let first = page(&backend, &item(1, Value::Null));
    assert!(!first["memberships"].as_array().unwrap().is_empty());
    {
        let mut state = backend.0.lock().unwrap();
        let saved = state.tables.calls.get_mut(&id(1)).unwrap();
        let mut request: Value = serde_json::from_str(&saved.0).unwrap();
        request["capabilities"] = json!(["channel-membership-v1"]);
        saved.0 = request.to_string();
        let mut response: Value = serde_json::from_str(saved.1.as_ref().unwrap()).unwrap();
        response.as_object_mut().unwrap().remove("memberships");
        saved.1 = Some(response.to_string());
        state.tables.memberships.clear();
        state.log.clear();
    }
    let before = backend.0.lock().unwrap().tables.clone();
    let replay = page(&backend, &item(1, Value::Null));
    assert_eq!(replay["outcome"], first["outcome"]);
    assert!(replay.get("memberships").is_none());
    assert_eq!(backend.ops(), ["claimCall"]);
    assert_eq!(backend.0.lock().unwrap().tables, before);
}
