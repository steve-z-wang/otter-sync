//! Unsent work through the runtime: observers publish the refusals, the acts
//! blocked on a failed task and the pending count after the commits that
//! change them, and a transaction's resolutions take effect at once but
//! announce only after its commit
//! ([#186](https://github.com/zanminwang/axton/issues/186),
//! [#205](https://github.com/zanminwang/axton/issues/205),
//! [#204](https://github.com/zanminwang/axton/issues/204)). The test is the
//! host over a real SQLite store; no sleeps, no threads.
mod common;

use axton_client::runtime::{ClientRuntime, Input};
use axton_client::*;
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};
use std::collections::BTreeMap;

const NOW: u64 = 1_000;
const ENTROPY: u64 = 200;

/// `Write` edits a Note and its `blob` requires `RemoteBlob(key: self)`; a
/// later `Write` of the same Note follows an earlier one. `Create` makes one.
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
fn blob(key: &str) -> String {
    json!({"arguments":{"key":key},"name":"RemoteBlob"}).to_string()
}
fn write(text: &str, blob: Option<&str>) -> Value {
    json!({"note":{"id":"n","text":text,"blob":blob}})
}

struct Host {
    runtime: ClientRuntime<SqliteStore>,
    open: BTreeMap<String, Value>,
    _dir: tempfile::TempDir,
}
struct Tx {
    effect: String,
    transaction: String,
}
impl Host {
    fn new(handlers: &[&str], hooks: &[&str]) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let client =
            Client::open(SqliteStore::open(dir.path().join("db")).unwrap(), schema()).unwrap();
        let runtime =
            ClientRuntime::with_store_hooks(client, hooks.iter().map(|h| h.to_string()).collect())
                .unwrap()
                .register_prerequisite_handlers(handlers.iter().map(|h| h.to_string()).collect())
                .unwrap();
        let mut host = Self {
            runtime,
            open: BTreeMap::new(),
            _dir: dir,
        };
        host.task(
            "seed",
            json!({"kind":"direct","operation":{"model":"Note","op":"create","identity":{"id":"n"},"values":{"text":"base","blob":null}}}),
        );
        host.run();
        host
    }
    fn submit(&mut self, input: Value) {
        let input: Input = serde_json::from_value(input).unwrap();
        self.runtime.receive(input, NOW, ENTROPY).unwrap();
    }
    fn task(&mut self, id: &str, command: Value) {
        self.submit(json!({"type":"task","requestId":id,"command":command}));
    }
    fn command(&mut self, id: &str, tx: &Tx, scope: Option<&str>, command: Value) {
        let mut input = json!({"type":"transactionCommand","requestId":id,"transactionId":tx.transaction,"command":command});
        if let Some(scope) = scope {
            input["scope"] = json!(scope);
        }
        self.submit(input);
    }
    fn callback(&mut self, tx: &Tx, ok: bool) {
        let mut input = json!({"type":"callbackResult","effectId":tx.effect,"transactionId":tx.transaction,"ok":ok});
        if !ok {
            input["error"] = json!("the author cancelled");
        }
        self.submit(input);
    }
    fn run(&mut self) -> Vec<Value> {
        let mut events = self.take();
        while self.runtime.step(NOW, ENTROPY) {
            events.extend(self.take());
        }
        events
    }
    fn take(&mut self) -> Vec<Value> {
        let events: Vec<Value> = self
            .runtime
            .take_events()
            .into_iter()
            .map(|e| serde_json::to_value(e).unwrap())
            .collect();
        for event in &events {
            let id = event["effectId"].as_str().map(str::to_string);
            match event["type"].as_str().unwrap() {
                "effect" => {
                    self.open.insert(id.unwrap(), event["operation"].clone());
                }
                "cancelEffect" => {
                    self.open.remove(&id.unwrap());
                }
                _ => {}
            }
        }
        events
    }
    fn prerequisites(&self) -> Vec<String> {
        self.open
            .iter()
            .filter(|(_, op)| op["kind"] == "prerequisite")
            .map(|(id, _)| id.clone())
            .collect()
    }
    fn answer(&mut self, id: &str, outcome: Value) {
        self.open.remove(id);
        self.submit(json!({"type":"effectResult","effectId":id,"outcome":outcome}));
    }
    /// Run the one handler in flight to a terminal failure or a success.
    fn settle_handler(&mut self, ok: bool) {
        let running = self.prerequisites();
        assert_eq!(running.len(), 1, "one handler runs: {:?}", self.open);
        let outcome = if ok {
            json!({"ok":true})
        } else {
            json!({"ok":false,"error":{"message":"upload refused"}})
        };
        self.answer(&running[0], outcome);
        self.run();
    }
    /// The value a task answered.
    fn value(&mut self, id: &str, command: Value) -> Value {
        self.task(id, command);
        let events = self.run();
        let done = completion(&events, id);
        assert_eq!(done["ok"], true, "{done}");
        done["value"].clone()
    }
    fn watch(&mut self, view: &str) -> String {
        let id = format!("watch-{view}");
        self.value(&id, json!({"kind":"unsentWatch","view":view}))["observerId"]
            .as_str()
            .unwrap()
            .to_string()
    }
    fn begin(&mut self, id: &str) -> Tx {
        self.task(id, json!({"kind":"transaction"}));
        let events = self.run();
        let effect = events
            .iter()
            .find(|e| e["type"] == "effect" && e["operation"]["kind"] == "callback")
            .unwrap();
        Tx {
            effect: effect["effectId"].as_str().unwrap().into(),
            transaction: effect["operation"]["transactionId"]
                .as_str()
                .unwrap()
                .into(),
        }
    }
    fn failures(&mut self) -> Vec<u64> {
        self.runtime
            .client()
            .failed_acts()
            .unwrap()
            .iter()
            .map(|a| a.ordinal)
            .collect()
    }
    fn text(&mut self) -> Value {
        self.runtime
            .client()
            .read(&RecordKey {
                model: "Note".into(),
                identity: json!({"id":"n"}),
            })
            .unwrap()
            .unwrap()["text"]
            .clone()
    }
}
fn completion<'a>(events: &'a [Value], id: &str) -> &'a Value {
    events
        .iter()
        .find(|e| e["type"] == "taskCompleted" && e["requestId"] == id)
        .unwrap_or_else(|| panic!("{id} did not complete: {events:?}"))
}
fn snapshots(events: &[Value], observer: &str) -> Vec<Value> {
    events
        .iter()
        .filter(|e| e["type"] == "observerChanged" && e["observerId"] == observer)
        .map(|e| e["snapshot"].clone())
        .collect()
}
fn call_completions(events: &[Value]) -> Vec<(String, String)> {
    events
        .iter()
        .filter(|e| e["type"] == "callCompleted")
        .map(|e| {
            (
                e["callId"].as_str().unwrap().to_string(),
                e["outcome"]["code"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

#[test]
fn an_unsent_observer_publishes_its_first_result_then_only_what_changed() {
    let mut h = Host::new(&[], &[]);
    h.task("pending", json!({"kind":"unsentWatch","view":"pending"}));
    h.task(
        "rejections",
        json!({"kind":"unsentWatch","view":"rejections"}),
    );
    let events = h.run();
    // Each answers its observer id, then publishes its first result.
    let pending = completion(&events, "pending")["value"]["observerId"]
        .as_str()
        .unwrap()
        .to_string();
    let refused = completion(&events, "rejections")["value"]["observerId"]
        .as_str()
        .unwrap()
        .to_string();
    let answered = events
        .iter()
        .position(|e| e["requestId"] == "pending")
        .unwrap();
    let published = events
        .iter()
        .position(|e| e["observerId"] == pending.as_str())
        .unwrap();
    assert!(answered < published, "{events:?}");
    assert_eq!(
        snapshots(&events, &pending),
        vec![json!({"kind":"pending","count":0})]
    );
    assert_eq!(
        snapshots(&events, &refused),
        vec![json!({"kind":"rejections","items":[]})]
    );
    // A commit that changes neither answer publishes nothing.
    h.task(
        "direct",
        json!({"kind":"direct","operation":{"model":"Note","op":"update","identity":{"id":"n"},"values":{"text":"local"}}}),
    );
    let events = h.run();
    assert!(snapshots(&events, &pending).is_empty(), "{events:?}");
    assert!(snapshots(&events, &refused).is_empty(), "{events:?}");
    // An enqueue changes the count, and so does a settlement.
    for id in ["first", "second"] {
        h.task(
            id,
            json!({"kind":"submitAction","name":"Write","version":1,"args":write("the author's words",None)}),
        );
    }
    let events = h.run();
    assert_eq!(
        snapshots(&events, &pending),
        vec![
            json!({"kind":"pending","count":1}),
            json!({"kind":"pending","count":2})
        ]
    );
    let frozen = h.value("freeze", json!({"kind":"freeze"}));
    let request: Value = serde_json::from_str(frozen.as_str().unwrap()).unwrap();
    let completions: Vec<Value> = request["mutations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| json!({"callId":m["callId"],"outcome":{"status":"succeeded","result":null}}))
        .collect();
    assert_eq!(completions.len(), 2);
    let client_id = h.runtime.client().client_id().to_string();
    h.task(
        "ack",
        json!({"kind":"ack","sequence":1,"receipt":{"clientId":client_id,"batchSequence":1,"rejections":[],
            "completions":completions,
            "records":[{"model":"Note","identity":{"id":"n"},"stamp":1,"state":{"text":"the author's words","blob":null}}]}}),
    );
    let events = h.run();
    assert_eq!(completion(&events, "ack")["ok"], true, "{events:?}");
    assert_eq!(
        snapshots(&events, &pending),
        vec![json!({"kind":"pending","count":0})]
    );
    // A read changes neither answer and publishes nothing.
    h.task("status", json!({"kind":"status"}));
    let events = h.run();
    assert!(snapshots(&events, &pending).is_empty());
    h.task(
        "third",
        json!({"kind":"submitAction","name":"Write","version":1,"args":write("the author's words",None)}),
    );
    let events = h.run();
    let call = completion(&events, "third")["value"].clone();
    assert_eq!(
        snapshots(&events, &pending),
        vec![json!({"kind":"pending","count":1})]
    );
    // A drop changes both.
    let ordinal = call["ordinal"].as_u64().unwrap();
    h.task("drop", json!({"kind":"drop","ordinal":ordinal}));
    let events = h.run();
    assert_eq!(
        snapshots(&events, &pending),
        vec![json!({"kind":"pending","count":0})]
    );
    let items = &snapshots(&events, &refused)[0]["items"];
    assert_eq!(
        items,
        &json!([{"id":ordinal,"name":"Write","version":1,"code":"dropped","act":{
            "args":write("the author's words",None),
            "operations":[{"model":"Note","op":"update","identity":{"id":"n"},
                "values":{"text":"the author's words","blob":null}}]}}])
    );
    // `rejectionGet` reads one; `dismiss` removes it and the stream says so.
    assert_eq!(
        h.value("get", json!({"kind":"rejectionGet","id":ordinal})),
        items[0]
    );
    assert_eq!(
        h.value("missing", json!({"kind":"rejectionGet","id":ordinal + 1})),
        Value::Null
    );
    h.task("dismiss", json!({"kind":"dismiss","ordinal":ordinal}));
    let events = h.run();
    assert_eq!(
        snapshots(&events, &refused),
        vec![json!({"kind":"rejections","items":[]})]
    );
    // `unwatch` ends one with no snapshot; close ends the other with its last
    // result and `closed`.
    h.value("unwatch", json!({"kind":"unwatch","observerId":refused}));
    h.submit(json!({"type":"close"}));
    let events = h.run();
    assert!(snapshots(&events, &refused).is_empty());
    assert_eq!(
        snapshots(&events, &pending),
        vec![json!({"kind":"pending","count":0,"closed":true})]
    );
    assert_eq!(events.last().unwrap()["type"], "runtimeClosed");
}

#[test]
fn a_terminal_handler_failure_lists_the_act_and_a_retry_runs_the_handler_again() {
    let mut h = Host::new(&["RemoteBlob"], &[]);
    let failures = h.watch("failures");
    h.run();
    let call = h.value(
        "submit",
        json!({"kind":"submitAction","name":"Write","version":1,"args":write("photo",Some("X"))}),
    );
    let ordinal = call["ordinal"].as_u64().unwrap();
    // A transient failure backs off on a timer; neither lists the act.
    let running = h.prerequisites();
    assert_eq!(running.len(), 1);
    h.answer(
        &running[0],
        json!({"ok":false,"error":{"message":"offline","retry":true}}),
    );
    let events = h.run();
    let timer = events
        .iter()
        .find(|e| e["operation"]["kind"] == "timer")
        .unwrap()["effectId"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(snapshots(&events, &failures).is_empty());
    h.answer(&timer, json!({"ok":true}));
    let events = h.run();
    assert!(
        snapshots(&events, &failures).is_empty(),
        "no timer publishes"
    );
    // The handler runs again; a terminal failure lists the act.
    let running = h.prerequisites();
    assert_eq!(running.len(), 1);
    h.answer(
        &running[0],
        json!({"ok":false,"error":{"message":"upload refused"}}),
    );
    let events = h.run();
    assert_eq!(
        snapshots(&events, &failures),
        vec![
            json!({"kind":"failures","items":[{"ordinal":ordinal,"name":"Write","version":1,
            "act":{"args":write("photo",Some("X")),"operations":[{"model":"Note","op":"update",
                "identity":{"id":"n"},"values":{"text":"photo","blob":"X"}}]},
            "tasks":[{"key":blob("X"),"name":"RemoteBlob","arguments":{"key":"X"},"error":"upload refused"}]}]})
        ]
    );
    assert!(
        h.prerequisites().is_empty(),
        "a failed task is not run again"
    );
    // #204: a second act on the failed task is listed at once, with it.
    let second = h.value(
        "second",
        json!({"kind":"submitAction","name":"Write","version":1,"args":write("again",Some("X"))}),
    )["ordinal"]
        .as_u64()
        .unwrap();
    let events = h.run();
    let _ = events;
    assert_eq!(h.failures(), vec![ordinal, second]);
    assert!(h.prerequisites().is_empty(), "no automatic reset");
    // One retry runs the handler again; its success empties the stream.
    h.task("retry", json!({"kind":"retryTasks","keys":[blob("X")]}));
    let events = h.run();
    assert_eq!(completion(&events, "retry")["ok"], true);
    assert_eq!(
        snapshots(&events, &failures),
        vec![json!({"kind":"failures","items":[]})]
    );
    h.settle_handler(true);
    assert!(h.runtime.client().pending_tasks().unwrap().is_empty());
    assert_eq!(h.runtime.client().pending_count().unwrap(), 2);
}

#[test]
fn a_discard_announces_its_call_and_a_refused_dependent_and_records_no_refusal() {
    let mut h = Host::new(&[], &[]);
    let refused = h.watch("rejections");
    h.run();
    let create = h.value(
        "create",
        json!({"kind":"submitAction","name":"Create","version":1,"args":{"note":{"id":"m","text":"new","blob":null}}}),
    );
    let edit = h.value(
        "edit",
        json!({"kind":"submitAction","name":"Write","version":1,"args":{"note":{"id":"m","text":"edited","blob":null}}}),
    );
    h.task(
        "discard",
        json!({"kind":"discard","ordinal":create["ordinal"]}),
    );
    let events = h.run();
    assert_eq!(
        call_completions(&events),
        vec![
            (
                create["callId"].as_str().unwrap().to_string(),
                "dropped".to_string()
            ),
            (
                edit["callId"].as_str().unwrap().to_string(),
                "dependency.rejected".to_string()
            )
        ]
    );
    let items = &snapshots(&events, &refused)[0]["items"];
    assert_eq!(items.as_array().unwrap().len(), 1, "{items}");
    assert_eq!(items[0]["id"], edit["ordinal"]);
    assert_eq!(items[0]["code"], "dependency.rejected");
}

/// The #205 reproducer through the runtime: the discard takes effect for the
/// later commands of the transaction, and its announcement waits for the
/// commit.
#[test]
fn resolutions_in_a_transaction_take_effect_at_once_and_announce_after_the_commit() {
    let mut h = Host::new(&["RemoteBlob"], &[]);
    let original = h.value(
        "submit",
        json!({"kind":"submitAction","name":"Write","version":1,"args":write("draft",Some("X"))}),
    );
    h.settle_handler(false);
    let failures = h.watch("failures");
    h.run();
    let tx = h.begin("tx");
    h.command(
        "discard",
        &tx,
        None,
        json!({"kind":"discard","ordinal":original["ordinal"]}),
    );
    h.command(
        "read",
        &tx,
        None,
        json!({"kind":"read","key":{"model":"Note","identity":{"id":"n"}}}),
    );
    h.command(
        "replace",
        &tx,
        None,
        json!({"kind":"submitMutation","name":"Write","version":1,"args":write("fixed",None)}),
    );
    let events = h.run();
    assert_eq!(completion(&events, "discard")["value"], Value::Null);
    assert_eq!(
        completion(&events, "read")["value"]["text"],
        "base",
        "the original's optimism is gone for the rest of the transaction"
    );
    assert_eq!(completion(&events, "replace")["ok"], true);
    assert!(
        call_completions(&events).is_empty(),
        "nothing before the commit"
    );
    assert!(snapshots(&events, &failures).is_empty());
    h.callback(&tx, true);
    let events = h.run();
    let committed = events
        .iter()
        .position(|e| e["type"] == "transactionCallState")
        .unwrap();
    let dropped = events
        .iter()
        .position(|e| e["type"] == "callCompleted")
        .unwrap();
    let answered = events.iter().position(|e| e["requestId"] == "tx").unwrap();
    assert!(committed < dropped && dropped < answered, "{events:?}");
    assert_eq!(
        call_completions(&events),
        vec![(
            original["callId"].as_str().unwrap().to_string(),
            "dropped".to_string()
        )]
    );
    assert_eq!(
        snapshots(&events, &failures),
        vec![json!({"kind":"failures","items":[]})]
    );
    assert!(h.runtime.client().refused_acts().unwrap().is_empty());
    assert_eq!(h.text(), "fixed");
    assert_eq!(
        h.runtime
            .client()
            .read_sql("SELECT COUNT(*) AS n FROM axton_mutation_dependency", &[])
            .unwrap()[0]["n"],
        0
    );
}

#[test]
fn a_retry_in_a_transaction_runs_the_handler_only_after_the_commit() {
    let mut h = Host::new(&["RemoteBlob"], &[]);
    h.value(
        "submit",
        json!({"kind":"submitAction","name":"Write","version":1,"args":write("draft",Some("X"))}),
    );
    h.settle_handler(false);
    let tx = h.begin("tx");
    h.command(
        "retry",
        &tx,
        None,
        json!({"kind":"retryTasks","keys":[blob("X")]}),
    );
    h.command(
        "tasks",
        &tx,
        None,
        json!({"kind":"sql","sql":"SELECT error FROM axton_mutation_prerequisite","parameters":[]}),
    );
    let events = h.run();
    assert_eq!(
        completion(&events, "tasks")["value"],
        json!([{"error":null}]),
        "pending for the rest of the transaction"
    );
    assert!(h.prerequisites().is_empty(), "no handler before the commit");
    h.callback(&tx, true);
    h.run();
    assert_eq!(
        h.prerequisites().len(),
        1,
        "the handler runs once committed"
    );
    h.settle_handler(true);
    assert!(h.failures().is_empty());
}

#[test]
fn a_rollback_or_a_savepoint_rollback_undoes_the_resolutions_and_announces_nothing() {
    let mut h = Host::new(&["RemoteBlob"], &[]);
    let original = h.value(
        "submit",
        json!({"kind":"submitAction","name":"Write","version":1,"args":write("draft",Some("X"))}),
    );
    let ordinal = original["ordinal"].as_u64().unwrap();
    h.settle_handler(false);
    let failures = h.watch("failures");
    h.run();
    // A throw after the discard rolls both back; the original is intact.
    let tx = h.begin("tx");
    h.command(
        "discard",
        &tx,
        None,
        json!({"kind":"discard","ordinal":ordinal}),
    );
    h.command(
        "replace",
        &tx,
        None,
        json!({"kind":"submitMutation","name":"Write","version":1,"args":write("fixed",None)}),
    );
    h.command(
        "retry",
        &tx,
        None,
        json!({"kind":"retryTasks","keys":[blob("X")]}),
    );
    h.run();
    h.callback(&tx, false);
    let events = h.run();
    assert_eq!(completion(&events, "tx")["ok"], false);
    assert!(call_completions(&events).is_empty(), "{events:?}");
    assert!(snapshots(&events, &failures).is_empty(), "nothing changed");
    assert!(h.prerequisites().is_empty());
    assert_eq!(h.failures(), vec![ordinal]);
    assert_eq!(h.text(), "draft");
    assert_eq!(h.runtime.client().pending_count().unwrap(), 1);
    // A savepoint rollback forgets only the resolutions made in it.
    let tx = h.begin("tx2");
    h.command("sp", &tx, None, json!({"kind":"savepoint"}));
    let events = h.run();
    let scope = completion(&events, "sp")["value"]["scope"]
        .as_str()
        .unwrap()
        .to_string();
    h.command(
        "discard",
        &tx,
        Some(&scope),
        json!({"kind":"discard","ordinal":ordinal}),
    );
    h.command(
        "undo",
        &tx,
        Some(&scope),
        json!({"kind":"rollbackSavepoint","scope":scope}),
    );
    h.command(
        "dismiss",
        &tx,
        None,
        json!({"kind":"dismiss","ordinal":ordinal}),
    );
    h.run();
    h.callback(&tx, true);
    let events = h.run();
    assert_eq!(completion(&events, "tx2")["ok"], true, "{events:?}");
    assert!(call_completions(&events).is_empty(), "{events:?}");
    assert_eq!(h.failures(), vec![ordinal]);
}

#[test]
fn neither_a_store_hook_nor_a_local_callback_resolves_unsent_work() {
    let mut h = Host::new(&[], &["Note"]);
    let call = h.value(
        "submit",
        json!({"kind":"submitAction","name":"Write","version":1,"args":write("draft",None)}),
    );
    let ordinal = call["ordinal"].as_u64().unwrap();
    // A Mutation's local callback may only read and write locally.
    let tx = h.begin("tx");
    h.command(
        "local",
        &tx,
        None,
        json!({"kind":"submitMutation","name":"Create","version":1,"args":{"note":{"id":"m","text":"x","blob":null}},"local":true}),
    );
    let events = h.run();
    let local = events
        .iter()
        .find(|e| e["operation"]["kind"] == "mutationLocal")
        .unwrap();
    let companion = local["operation"]["companionId"]
        .as_str()
        .unwrap()
        .to_string();
    h.submit(
        json!({"type":"transactionCommand","requestId":"companion","transactionId":tx.transaction,
        "companionId":companion,"command":{"kind":"discard","ordinal":ordinal}}),
    );
    let events = h.run();
    assert_eq!(
        completion(&events, "companion")["error"],
        "invalid transaction capability"
    );
    h.submit(
        json!({"type":"callbackResult","effectId":local["effectId"],"transactionId":tx.transaction,
        "companionId":companion,"ok":true}),
    );
    h.run();
    h.callback(&tx, true);
    let events = h.run();
    assert_eq!(completion(&events, "tx")["ok"], false);
    assert_eq!(h.runtime.client().pending_count().unwrap(), 1);
    // The onStore transaction resolves nothing either.
    h.task(
        "sub",
        json!({"kind":"channel","channel":"feed","subscribed":true}),
    );
    h.run();
    common::acknowledge(h.runtime.client(), &[("feed", 0)]);
    let page = json!({"cursors":{"feed":{"from":0,"to":1,"head":1}},"changes":[
        {"model":"Note","identity":{"id":"o"},"stamp":1,"state":{"text":"server","blob":null}}]});
    h.task("pull", json!({"kind":"pull","page":page}));
    let events = h.run();
    let hook = events
        .iter()
        .find(|e| e["operation"]["kind"] == "storeCallback")
        .unwrap();
    let hook = Tx {
        effect: hook["effectId"].as_str().unwrap().into(),
        transaction: hook["operation"]["transactionId"].as_str().unwrap().into(),
    };
    h.command(
        "discard",
        &hook,
        None,
        json!({"kind":"discard","ordinal":ordinal}),
    );
    h.command(
        "retry",
        &hook,
        None,
        json!({"kind":"retryTasks","keys":["k"]}),
    );
    h.command(
        "dismiss",
        &hook,
        None,
        json!({"kind":"dismiss","ordinal":ordinal}),
    );
    let events = h.run();
    for id in ["discard", "retry", "dismiss"] {
        assert_eq!(
            completion(&events, id)["error"],
            "store hook cannot resolve unsent work",
            "{events:?}"
        );
    }
    h.callback(&hook, true);
    h.run();
    assert_eq!(h.runtime.client().pending_count().unwrap(), 1);
}
