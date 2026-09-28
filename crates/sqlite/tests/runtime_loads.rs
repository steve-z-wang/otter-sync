//! Native Loads as runtime work ([#173](https://github.com/zanminwang/axton/issues/173)):
//! the Load commands, bounded batches as `load` HTTP effects beside the push
//! and Downlink lanes, backoff and credential refresh, page application
//! through the owned store session, handle observers and waiters, and the
//! pending-rebuild parking - over a real SQLite store. The test is the host:
//! it answers every effect with a fixed clock that a fired timer advances.
mod common;
use axton_client::runtime::{ClientRuntime, Input};
use axton_client::*;
use axton_sqlite::SqliteStore;
use common::*;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::Path;

const ENTROPY: u64 = 200;
/// The attempt deadline `connect` names; backoff timers are shorter.
const DEADLINE: u64 = 5_000;
const PROJECT: &str = "0190f0e0-1111-7222-8333-444455556666";

fn schema_value() -> Value {
    let mut schema = load_schema_value();
    schema["actions"] = json!([{"name":"Ping","version":1,"inputs":[],"outputs":[]}]);
    schema
}
fn factory() -> StoreFactory<SqliteStore> {
    Box::new(|p| SqliteStore::open(p))
}

struct Host<S: ClientStore = SqliteStore> {
    runtime: ClientRuntime<S>,
    now: u64,
    open: BTreeMap<String, Value>,
    _dir: Option<tempfile::TempDir>,
}
fn host() -> Host {
    host_at(tempfile::tempdir().unwrap(), &[])
}
fn hooked() -> Host {
    host_at(tempfile::tempdir().unwrap(), &["Entry"])
}
fn host_at(dir: tempfile::TempDir, hooks: &[&str]) -> Host {
    let runtime = open_runtime(&dir.path().join("db"), schema_value(), hooks);
    Host::of(runtime, Some(dir))
}
fn open_runtime(path: &Path, schema: Value, hooks: &[&str]) -> ClientRuntime<SqliteStore> {
    ClientRuntime::open_at(path, Schema::from_value(schema).unwrap(), factory(), false)
        .unwrap()
        .register_store_hooks(hooks.iter().map(|h| h.to_string()).collect())
        .unwrap()
}
impl<S: ClientStore + 'static> Host<S> {
    fn of(runtime: ClientRuntime<S>, dir: Option<tempfile::TempDir>) -> Self {
        Self {
            runtime,
            now: 1_000,
            open: BTreeMap::new(),
            _dir: dir,
        }
    }
    fn submit(&mut self, input: Value) {
        let input: Input = serde_json::from_value(input).unwrap();
        self.runtime.receive(input, self.now, ENTROPY).unwrap();
    }
    fn task(&mut self, id: &str, command: Value) {
        self.submit(json!({"type":"task","requestId":id,"command":command}));
    }
    fn connect(&mut self, refresh: bool) {
        self.task(
            "connect",
            json!({"kind":"connect","directTimeoutMs":DEADLINE,"refreshAuth":refresh}),
        );
        let events = self.run();
        assert!(
            events
                .iter()
                .any(|e| e["requestId"] == "connect" && e["ok"] == true)
        );
    }
    fn record(&mut self, events: &[Value]) {
        for event in events {
            match event["type"].as_str().unwrap() {
                "effect" => {
                    self.open.insert(
                        event["effectId"].as_str().unwrap().into(),
                        event["operation"].clone(),
                    );
                }
                "cancelEffect" => {
                    self.open.remove(event["effectId"].as_str().unwrap());
                }
                _ => {}
            }
        }
    }
    fn take(&mut self) -> Vec<Value> {
        let events: Vec<Value> = self
            .runtime
            .take_events()
            .into_iter()
            .map(|e| serde_json::to_value(e).unwrap())
            .collect();
        self.record(&events);
        events
    }
    fn run(&mut self) -> Vec<Value> {
        let mut events = self.take();
        while self.runtime.step(self.now, ENTROPY) {
            events.extend(self.take());
        }
        events
    }
    /// One step at a time until an event matches.
    fn until_event(&mut self, matches: impl Fn(&Value) -> bool) -> Vec<Value> {
        let mut events = self.take();
        while !events.iter().any(&matches) {
            assert!(
                self.runtime.step(self.now, ENTROPY),
                "never observed: {events:?}"
            );
            events.extend(self.take());
        }
        events
    }
    /// Run one task to quiescence and answer its completion.
    fn call(&mut self, id: &str, command: Value) -> Value {
        self.task(id, command);
        let events = self.run();
        completion(&events, id)
    }
    fn outstanding(&self, kind: &str, route: Option<&str>) -> Vec<(String, Value)> {
        self.open
            .iter()
            .filter(|(_, op)| op["kind"] == kind && route.is_none_or(|r| op["route"] == r))
            .map(|(id, op)| (id.clone(), op.clone()))
            .collect()
    }
    fn one(&self, kind: &str, route: Option<&str>) -> (String, Value) {
        let found = self.outstanding(kind, route);
        assert_eq!(found.len(), 1, "one {kind} {route:?}: {:?}", self.open);
        found.into_iter().next().unwrap()
    }
    /// Every Load batch out: its effect ID and its request body.
    fn batches(&self) -> Vec<(String, Value)> {
        self.outstanding("http", Some("load"))
            .into_iter()
            .map(|(id, op)| {
                (
                    id,
                    serde_json::from_str(op["body"].as_str().unwrap()).unwrap(),
                )
            })
            .collect()
    }
    fn batch(&self) -> (String, Value) {
        let mut batches = self.batches();
        assert_eq!(batches.len(), 1, "one Load batch: {:?}", self.open);
        batches.pop().unwrap()
    }
    /// The backoff timer: every outstanding timer shorter than the deadline.
    fn backoff(&self) -> (String, u64) {
        let timers: Vec<(String, u64)> = self
            .outstanding("timer", None)
            .into_iter()
            .map(|(id, op)| (id, op["millis"].as_u64().unwrap()))
            .filter(|(_, millis)| *millis < DEADLINE)
            .collect();
        assert_eq!(timers.len(), 1, "one backoff timer: {:?}", self.open);
        timers[0].clone()
    }
    fn answer(&mut self, id: &str, outcome: Value) {
        self.open.remove(id);
        self.submit(json!({"type":"effectResult","effectId":id,"outcome":outcome}));
    }
    fn ok(&mut self, id: &str, body: &str) {
        self.answer(id, json!({"ok":true,"value":{"status":200,"body":body}}));
    }
    fn fail(&mut self, id: &str, message: &str, status: Option<u16>) {
        let mut error = json!({"message":message});
        if let Some(status) = status {
            error["status"] = json!(status);
        }
        self.answer(id, json!({"ok":false,"error":error}));
    }
    fn fire(&mut self, timer: &str) {
        let millis = self.open[timer]["millis"].as_u64().unwrap();
        self.now += millis;
        self.answer(timer, json!({"ok":true,"value":null}));
    }
    fn client(&mut self) -> &mut Client<S> {
        self.runtime.client()
    }
    fn entry(&mut self, id: &str) -> Option<Value> {
        let key = load_schema()
            .record_key("Entry", &json!({ "id": id }))
            .unwrap();
        self.client().read(&key).unwrap()
    }
    fn job(&mut self, id: &str) -> LoadJob {
        self.client().get_load(id).unwrap().expect("a stored job")
    }
    /// Start `Entries` for `project`; answers the task's value.
    fn start(&mut self, id: &str, extra: Value) -> Value {
        let mut command = json!({"kind":"loadStart","name":"Entries","version":1,
            "args":{"projectId":PROJECT,"since":null}});
        for (k, v) in extra.as_object().unwrap() {
            command[k] = v.clone();
        }
        let answer = self.call(id, command);
        assert_eq!(answer["ok"], true, "{answer}");
        answer["value"].clone()
    }
    /// Start `Recent` (no arguments) and answer its load ID.
    fn recent(&mut self, id: &str) -> String {
        let answer = self.call(
            id,
            json!({"kind":"loadStart","name":"Recent","version":1,"args":{}}),
        );
        assert_eq!(answer["ok"], true, "{answer}");
        answer["value"]["loadId"].as_str().unwrap().to_string()
    }
}

fn completion(events: &[Value], id: &str) -> Value {
    events
        .iter()
        .find(|e| e["type"] == "taskCompleted" && e["requestId"] == id)
        .unwrap_or_else(|| panic!("{id} did not complete: {events:?}"))
        .clone()
}
fn completed(events: &[Value], id: &str) -> bool {
    events
        .iter()
        .any(|e| e["type"] == "taskCompleted" && e["requestId"] == id)
}
fn position(events: &[Value], matches: impl Fn(&Value) -> bool) -> usize {
    events
        .iter()
        .position(matches)
        .unwrap_or_else(|| panic!("not found in {events:?}"))
}
/// The phases one Load observer published, in order.
fn phases(events: &[Value], observer: &Value) -> Vec<String> {
    events
        .iter()
        .filter(|e| e["type"] == "observerChanged" && e["observerId"] == *observer)
        .map(|e| {
            e["snapshot"]["status"]["phase"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect()
}
fn errors(events: &[Value]) -> Vec<String> {
    events
        .iter()
        .filter(|e| e["type"] == "report" && e["diagnostic"]["kind"] == "error")
        .map(|e| e["diagnostic"]["message"].as_str().unwrap().to_string())
        .collect()
}
fn intents(body: &Value) -> Vec<Value> {
    body["loads"].as_array().unwrap().clone()
}
fn ids(body: &Value) -> Vec<String> {
    intents(body)
        .iter()
        .map(|i| i["loadId"].as_str().unwrap().to_string())
        .collect()
}
fn call_of(body: &Value, load: &str) -> String {
    intents(body)
        .into_iter()
        .find(|i| i["loadId"] == load)
        .unwrap()["callId"]
        .as_str()
        .unwrap()
        .to_string()
}
/// A successful page for `intent`: Entry `(id, text, stamp)` rows, and
/// `next` as the continuation state (`None` completes).
fn page(intent: &Value, entries: &[(&str, &str, u64)], next: Option<Value>) -> Value {
    json!({
        "loadId": intent["loadId"], "callId": intent["callId"],
        "outcome": {"status":"succeeded",
            "data":{"entries": entries.iter().map(|(id,_,_)| json!({"id":id})).collect::<Vec<_>>()},
            "next": next.map(|state| json!({"state": state}))},
        "records": entries.iter().map(|(id,text,stamp)| json!({"model":"Entry","identity":{"id":id},
            "stamp":stamp,"state":{"text":text,"note":null}})).collect::<Vec<_>>()
    })
}
fn rejected(intent: &Value, status: &str, code: &str) -> Value {
    json!({"loadId": intent["loadId"], "callId": intent["callId"],
        "outcome": {"status": status, "error": {"code": code, "message": "no"}}, "records": []})
}
fn response(items: Vec<Value>) -> String {
    json!({ "loads": items }).to_string()
}
/// Answer every page of `body` with an empty final page.
fn finish_all(body: &Value) -> String {
    response(intents(body).iter().map(|i| page(i, &[], None)).collect())
}

// ------------------------------------------------------------- lifecycle

#[test]
fn a_start_is_accepted_offline_and_its_wait_resolves_after_the_final_commit() {
    let mut h = host();
    let started = h.start("start", json!({}));
    let (id, observer) = (started["loadId"].clone(), started["observerId"].clone());
    assert_eq!(started["start"], "created");
    assert_eq!(started["status"]["phase"], "waiting", "offline work waits");
    assert!(
        h.batches().is_empty(),
        "nothing is sent without a connection"
    );
    h.task("wait", json!({"kind":"loadWait","loadId":id}));
    let events = h.run();
    assert!(!completed(&events, "wait"));
    h.connect(false);
    let (http, body) = h.batch();
    assert_eq!(ids(&body), [id.as_str().unwrap()]);
    let intent = &intents(&body)[0];
    assert_eq!(intent["continuation"], Value::Null, "the first page");
    assert_eq!(intent["models"], json!({"Entry":1}));
    assert_eq!(intent["callId"], json!(h.job(id.as_str().unwrap()).call_id));
    // A first page with a next state, then the final page.
    h.ok(
        &http,
        &response(vec![page(
            intent,
            &[("a", "A", 1)],
            Some(json!({"after":"a"})),
        )]),
    );
    let events = h.run();
    assert!(!completed(&events, "wait"));
    assert!(h.entry("a").is_some());
    let (http, body) = h.batch();
    let intent = &intents(&body)[0];
    assert_eq!(intent["continuation"], json!({"state":{"after":"a"}}));
    h.ok(&http, &response(vec![page(intent, &[("b", "B", 1)], None)]));
    let events = h.until_event(|e| e["requestId"] == "wait");
    assert_eq!(completion(&events, "wait")["ok"], true);
    assert!(h.entry("b").is_some(), "the wait resolves after the commit");
    let events = [events, h.run()].concat();
    assert!(h.batches().is_empty());
    let job = h.job(id.as_str().unwrap());
    assert_eq!((job.phase, job.pages), (LoadPhase::Complete, 2));
    let seen = phases(&events, &observer);
    assert_eq!(
        seen.last().map(String::as_str),
        Some("complete"),
        "{seen:?}"
    );
    // A wait on a complete job answers at once.
    assert_eq!(
        h.call("again", json!({"kind":"loadWait","loadId":id}))["ok"],
        true
    );
}

#[test]
fn handles_observe_projected_phases_and_dispose_releases_only_their_observer() {
    let mut h = host();
    h.connect(false);
    let started = h.start("start", json!({}));
    let id = started["loadId"].clone();
    let first = started["observerId"].clone();
    let events = h.run();
    let reattached = h.call("get", json!({"kind":"loadGet","loadId":id}));
    let second = reattached["value"]["observerId"].clone();
    assert_ne!(first, second, "each handle has its own observer");
    assert_eq!(reattached["value"]["loadId"], id);
    assert_eq!(
        h.call(
            "missing",
            json!({"kind":"loadGet","loadId":"00000000-0000-4000-8000-000000000000"})
        )["value"],
        Value::Null
    );
    let _ = events;
    assert_eq!(
        h.call("status", json!({"kind":"loadStatus","loadId":id}))["value"]["phase"],
        "loading"
    );
    let listed = h.call("list", json!({"kind":"loadList"}));
    assert_eq!(listed["value"][0]["id"], id);
    assert_eq!(listed["value"][0]["phase"], "loading");
    let refused = h.call("bad", json!({"kind":"loadList","limit":0}));
    assert_eq!(refused["details"]["code"], "load.invalid_options");
    // Dispose ends one handle's publications; the job and the other go on.
    assert_eq!(
        h.call("dispose", json!({"kind":"loadDispose","observerId":first}))["ok"],
        true
    );
    let (http, body) = h.batch();
    h.ok(&http, &finish_all(&body));
    let events = h.run();
    assert!(phases(&events, &first).is_empty());
    assert_eq!(phases(&events, &second), ["complete"]);
}

#[test]
fn management_refusals_carry_their_codes() {
    let mut h = host();
    let bad = h.call(
        "refresh",
        json!({"kind":"loadStart","name":"Entries","version":1,"args":{"projectId":PROJECT,"since":null},"refresh":true}),
    );
    assert_eq!(bad["details"]["code"], "load.invalid_options");
    let bad = h.call(
        "type",
        json!({"kind":"loadStart","name":"Entries","version":1,"args":{"projectId":PROJECT,"since":null},"once":"yes"}),
    );
    assert_eq!(bad["details"]["code"], "load.invalid_options");
    let bad = h.call(
        "unknown",
        json!({"kind":"loadStart","name":"Nope","version":1,"args":{}}),
    );
    assert_eq!(bad["details"]["code"], "load.unknown");
    let id = h.start("start", json!({}))["loadId"].clone();
    let bad = h.call("forget", json!({"kind":"loadForget","loadId":id}));
    assert_eq!(bad["details"]["code"], "load.not_terminal");
    let missing = "00000000-0000-4000-8000-000000000000";
    for kind in ["loadWait", "loadCancel", "loadRetry", "loadStatus"] {
        let bad = h.call(kind, json!({"kind":kind,"loadId":missing}));
        assert_eq!(bad["details"]["code"], "load.not_found", "{kind}: {bad}");
    }
    let cancelled = h.call("cancel", json!({"kind":"loadCancel","loadId":id}));
    assert_eq!(cancelled["value"]["phase"], "cancelled");
    let bad = h.call("retry", json!({"kind":"loadRetry","loadId":id}));
    assert_eq!(bad["details"]["code"], "load.not_retryable");
    assert_eq!(
        h.call("forget", json!({"kind":"loadForget","loadId":id}))["ok"],
        true
    );
    assert_eq!(
        h.call("gone", json!({"kind":"loadGet","loadId":id}))["value"],
        Value::Null
    );
}

// ------------------------------------------------------ batches and lanes

/// A receipt settling every mutation of a push body.
fn receipt(client_id: &str, body: &str) -> String {
    let push: Value = serde_json::from_str(body).unwrap();
    let completions: Vec<Value> = push["mutations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| json!({"callId":m["callId"],"outcome":{"status":"succeeded","result":null}}))
        .collect();
    json!({"clientId":client_id,"batchSequence":push["batchSequence"],"rejections":[],"completions":completions,"records":[]})
        .to_string()
}
fn pull(from: u64, to: u64, id: &str, text: &str) -> String {
    json!({"cursors":{"book":{"from":from,"to":to,"head":to}},"changes":[{"model":"Entry","identity":{"id":id},"stamp":to,"state":{"text":text,"note":null}}]}).to_string()
}

#[test]
fn nine_ready_jobs_go_out_as_eight_and_one_and_a_slow_batch_holds_back_no_lane() {
    let mut h = host();
    let started: Vec<String> = (0..9).map(|n| h.recent(&format!("start{n}"))).collect();
    h.connect(false);
    let mut batches = h.batches();
    assert_eq!(
        batches.len(),
        2,
        "two independent batches, no waiting to fill"
    );
    batches.sort_by_key(|(_, body)| std::cmp::Reverse(intents(body).len()));
    let (slow, first) = batches[0].clone();
    let (fast, second) = batches[1].clone();
    assert_eq!(ids(&first), started[..8], "the oldest eight, in order");
    assert_eq!(ids(&second), started[8..]);
    // The eight-page batch stays out. The second answers and applies.
    h.ok(
        &fast,
        &response(vec![page(&intents(&second)[0], &[("n", "N", 1)], None)]),
    );
    h.run();
    assert!(h.entry("n").is_some());
    assert_eq!(h.job(&started[8]).phase, LoadPhase::Complete);
    // A Mutation goes out and settles, live pages apply and foreground
    // reads answer, all while the slow batch is out.
    let client_id = h.client().client_id().to_string();
    let submitted = h.call(
        "ping",
        json!({"kind":"submitAction","name":"Ping","version":1,"args":{}}),
    );
    let call = submitted["value"]["callId"].clone();
    let (push, body) = h.one("http", Some("push"));
    h.ok(&push, &receipt(&client_id, body["body"].as_str().unwrap()));
    let events = h.run();
    assert!(
        events
            .iter()
            .any(|e| e["type"] == "callCompleted" && e["callId"] == call),
        "{events:?}"
    );
    h.task("subscribe", json!({"kind":"scopeSubscribe","scope":"book"}));
    h.run();
    let (socket, _) = h.one("socket", None);
    h.answer(
        &socket,
        json!({"ok":true,"value":{"event":"message","body":ack(&[("book", 0)])}}),
    );
    h.run();
    h.answer(
        &socket,
        json!({"ok":true,"value":{"event":"message","body":pull(0, 1, "live", "L")}}),
    );
    h.run();
    assert!(h.entry("live").is_some(), "live Downlink pages apply");
    let read = h.call(
        "read",
        json!({"kind":"read","key":{"model":"Entry","identity":{"id":"live"}}}),
    );
    assert_eq!(read["value"]["text"], "L");
    assert_eq!(
        h.batches()[0].0,
        slow,
        "the slow batch is still the one out"
    );
    h.ok(&slow, &finish_all(&first));
    h.run();
    for id in &started {
        assert_eq!(h.job(id).phase, LoadPhase::Complete);
    }
}

#[test]
fn sixteen_answers_waiting_behind_a_hook_stop_further_requests() {
    let mut h = hooked();
    let started: Vec<String> = (0..20).map(|n| h.recent(&format!("s{n}"))).collect();
    h.connect(false);
    let batches = h.batches();
    assert_eq!(batches.len(), 2);
    let (first_id, first) = batches
        .iter()
        .find(|(_, b)| ids(b)[0] == started[0])
        .unwrap()
        .clone();
    let (second_id, second) = batches
        .iter()
        .find(|(_, b)| ids(b)[0] == started[8])
        .unwrap()
        .clone();
    // The first page carries a record for a hooked Model: its application
    // parks on the hook and holds the writer.
    let mut items: Vec<Value> = intents(&first)
        .iter()
        .map(|i| page(i, &[], Some(json!(2))))
        .collect();
    items[0] = page(&intents(&first)[0], &[("h", "H", 1)], Some(json!(2)));
    h.ok(&first_id, &response(items));
    h.ok(
        &second_id,
        &response(
            intents(&second)
                .iter()
                .map(|i| page(i, &[], Some(json!(2))))
                .collect(),
        ),
    );
    let events = h.run();
    let hook = events
        .iter()
        .find(|e| e["operation"]["kind"] == "storeCallback")
        .unwrap_or_else(|| panic!("{events:?}"))
        .clone();
    assert!(
        h.batches().is_empty(),
        "sixteen outcomes waiting: no further request"
    );
    // A foreground read waits while the hook holds the writer.
    h.task(
        "read",
        json!({"kind":"read","key":{"model":"Entry","identity":{"id":"h"}}}),
    );
    let events = h.run();
    assert!(!completed(&events, "read"));
    h.submit(json!({"type":"callbackResult","effectId":hook["effectId"],
        "transactionId":hook["operation"]["transactionId"],"ok":true}));
    // One page per lane unit: the batch's slot frees once its eighth
    // outcome was consumed, and the next request carries the four jobs that
    // never went out ahead of the requeued ones.
    let events = h.run();
    assert_eq!(completion(&events, "read")["value"]["text"], "H");
    let batches = h.batches();
    assert_eq!(batches.len(), 2, "{batches:?}");
    let mut sent: Vec<Vec<String>> = batches.iter().map(|(_, b)| ids(b)).collect();
    sent.sort_by_key(|ids| ids[0] != started[16]);
    let expected: Vec<String> = started[16..].iter().chain(&started[..4]).cloned().collect();
    assert_eq!(
        sent[0], expected,
        "never-sent jobs first, then requeued ones in commit order"
    );
    assert_eq!(sent[1], started[4..12]);
    for id in &started {
        assert!(h.job(id).pages <= 1);
    }
}

// ------------------------------------------------- failures and backoff

#[test]
fn mixed_items_settle_independently_and_a_job_in_backoff_holds_back_no_other() {
    let mut h = host();
    let done = h.recent("done");
    let refused = h.recent("refused");
    let later = h.recent("later");
    for (wait, id) in [("wait-done", &done), ("wait-refused", &refused)] {
        h.task(wait, json!({"kind":"loadWait","loadId":id}));
    }
    let observer =
        h.call("get", json!({"kind":"loadGet","loadId":later}))["value"]["observerId"].clone();
    h.connect(false);
    let (http, body) = h.batch();
    let items = intents(&body);
    let retried_call = call_of(&body, &later);
    h.ok(
        &http,
        &response(vec![
            page(&items[0], &[("d", "D", 1)], None),
            rejected(&items[1], "failed", "handler.failed"),
            rejected(&items[2], "retryable", "server.unavailable"),
        ]),
    );
    let events = h.run();
    assert_eq!(completion(&events, "wait-done")["ok"], true);
    let failure = completion(&events, "wait-refused");
    assert_eq!(failure["details"]["code"], "handler.failed");
    assert!(h.entry("d").is_some());
    let job = h.job(&refused);
    assert_eq!(job.phase, LoadPhase::Failed);
    assert_eq!(job.error.unwrap().code, "handler.failed");
    let job = h.job(&later);
    assert_eq!(job.phase, LoadPhase::Pending);
    assert_eq!(job.call_id.as_deref(), Some(retried_call.as_str()));
    assert_eq!(
        (job.retry, job.attempts),
        (Some(LoadRetryClass::Backend), 1)
    );
    assert_eq!(phases(&events, &observer).last().unwrap(), "waiting");
    let (timer, millis) = h.backoff();
    assert_eq!(millis, 1_000, "1 s base at mid entropy");
    // A job started now goes out at once, past the one backing off.
    let fresh = h.recent("fresh");
    let (http, body) = h.batch();
    assert_eq!(ids(&body), [fresh.as_str()]);
    h.ok(&http, &finish_all(&body));
    h.run();
    // The delay passes: the same frozen call goes again.
    h.fire(&timer);
    let events = h.run();
    let (http, body) = h.batch();
    assert_eq!(ids(&body), [later.as_str()]);
    assert_eq!(
        call_of(&body, &later),
        retried_call,
        "the frozen call is resent"
    );
    assert_eq!(phases(&events, &observer).last().unwrap(), "loading");
    // A second retryable failure doubles the delay.
    h.ok(
        &http,
        &response(vec![rejected(
            &intents(&body)[0],
            "retryable",
            "server.unavailable",
        )]),
    );
    h.run();
    assert_eq!(h.backoff().1, 2_000);
    assert_eq!(h.job(&later).attempts, 2);
}

#[test]
fn a_lost_response_or_a_deadline_resends_the_same_call_and_late_answers_are_inert() {
    let mut h = host();
    let id = h.recent("start");
    h.connect(false);
    let (http, body) = h.batch();
    // The server committed, the response was lost: same bytes again.
    h.fail(&http, "connection reset", None);
    let events = h.run();
    assert_eq!(errors(&events), ["connection reset"]);
    let job = h.job(&id);
    assert_eq!(
        (job.retry, job.attempts),
        (Some(LoadRetryClass::Transport), 1)
    );
    let (timer, _) = h.backoff();
    h.fire(&timer);
    h.run();
    let (http, again) = h.batch();
    assert_eq!(again, body, "the frozen request is resent unchanged");
    // The deadline passes first: the request is abandoned.
    let (deadline, op) = h
        .outstanding("timer", None)
        .into_iter()
        .find(|(_, op)| op["millis"] == DEADLINE)
        .unwrap();
    assert_eq!(op["millis"], DEADLINE);
    h.fire(&deadline);
    let events = h.run();
    assert!(
        events.contains(&json!({"type":"cancelEffect","effectId":http})),
        "{events:?}"
    );
    assert_eq!(h.job(&id).attempts, 2);
    // Its late answer is ignored.
    h.ok(&http, &finish_all(&body));
    h.run();
    assert_eq!(h.job(&id).phase, LoadPhase::Pending);
    let (timer, millis) = h.backoff();
    assert_eq!(millis, 2_000);
    h.fire(&timer);
    h.run();
    let (http, again) = h.batch();
    assert_eq!(again, body);
    h.ok(&http, &finish_all(&body));
    h.run();
    assert_eq!(h.job(&id).phase, LoadPhase::Complete);
    // A response that does not correlate applies nothing and backs off.
    let other = h.recent("other");
    let (http, body) = h.batch();
    h.ok(&http, &response(vec![]));
    let events = h.run();
    assert!(
        errors(&events)[0].starts_with("invalid Load response"),
        "{events:?}"
    );
    let job = h.job(&other);
    assert_eq!((job.phase, job.attempts), (LoadPhase::Pending, 1));
    assert_eq!(job.call_id.unwrap(), call_of(&body, &other));
}

#[test]
fn a_401_shares_one_refresh_and_only_an_explicit_refusal_is_unauthorized() {
    let mut h = host();
    h.connect(true);
    // Two batches meet a 401 together: one refresh, then each resends once.
    let ids_sent: Vec<String> = (0..9).map(|n| h.recent(&format!("s{n}"))).collect();
    let batches = h.batches();
    assert_eq!(batches.len(), 2);
    for (http, _) in &batches {
        h.fail(http, "unauthorized", Some(401));
    }
    h.run();
    let (refresh, _) = h.one("refreshAuth", None);
    h.answer(&refresh, json!({"ok":true}));
    h.run();
    let resent = h.batches();
    assert_eq!(resent.len(), 2);
    let mut before: Vec<Value> = batches.iter().map(|(_, b)| b.clone()).collect();
    let mut after: Vec<Value> = resent.iter().map(|(_, b)| b.clone()).collect();
    before.sort_by_key(|b| b.to_string());
    after.sort_by_key(|b| b.to_string());
    assert_eq!(before, after, "the same bodies go again");
    // A second 401 after the refresh backs off rather than refreshing again.
    let (http, body) = resent[0].clone();
    h.fail(&http, "unauthorized", Some(401));
    h.run();
    assert!(h.outstanding("refreshAuth", None).is_empty());
    let first = ids(&body)[0].clone();
    assert_eq!(h.job(&first).attempts, 1);
    let (http, body) = resent[1].clone();
    h.ok(&http, &finish_all(&body));
    h.run();
    let _ = ids_sent;

    // A transient refresh failure backs off like a transport failure.
    let transient = h.recent("transient");
    let (http, _) = h
        .batches()
        .into_iter()
        .find(|(_, b)| ids(b) == [transient.clone()])
        .unwrap();
    h.fail(&http, "unauthorized", Some(401));
    h.run();
    let (refresh, _) = h.one("refreshAuth", None);
    h.answer(
        &refresh,
        json!({"ok":false,"error":{"message":"network down"}}),
    );
    h.run();
    let job = h.job(&transient);
    assert_eq!((job.phase, job.attempts), (LoadPhase::Pending, 1));

    // A refresh the server refused fails the batch's jobs unauthorized.
    let refused = h.recent("refused");
    h.task("wait", json!({"kind":"loadWait","loadId":refused}));
    h.run();
    let (http, _) = h
        .batches()
        .into_iter()
        .find(|(_, b)| ids(b) == [refused.clone()])
        .unwrap();
    h.fail(&http, "unauthorized", Some(401));
    h.run();
    let (refresh, _) = h.one("refreshAuth", None);
    h.answer(
        &refresh,
        json!({"ok":false,"error":{"message":"refresh token revoked","status":401}}),
    );
    let events = h.run();
    let failure = completion(&events, "wait");
    assert_eq!(failure["details"]["code"], "load.unauthorized");
    let job = h.job(&refused);
    assert_eq!(job.phase, LoadPhase::Failed);
    assert_eq!(job.error.unwrap().code, "load.unauthorized");
}

#[test]
fn pause_abandons_without_backoff_resume_resends_and_stop_keeps_the_job() {
    let mut h = host();
    let id = h.recent("start");
    let observer =
        h.call("get", json!({"kind":"loadGet","loadId":id}))["value"]["observerId"].clone();
    h.connect(false);
    let (http, body) = h.batch();
    h.task("pause", json!({"kind":"connection","event":"pause"}));
    let events = h.run();
    assert!(events.contains(&json!({"type":"cancelEffect","effectId":http})));
    assert!(h.batches().is_empty());
    assert!(
        h.outstanding("timer", None).is_empty(),
        "no deadline, no backoff"
    );
    assert_eq!(phases(&events, &observer).last().unwrap(), "waiting");
    let job = h.job(&id);
    assert_eq!((job.phase, job.attempts), (LoadPhase::Pending, 0));
    // A late answer to the abandoned request is ignored.
    h.ok(&http, &finish_all(&body));
    h.run();
    assert_eq!(h.job(&id).pages, 0);
    h.task("resume", json!({"kind":"connection","event":"resume"}));
    h.run();
    let (http, again) = h.batch();
    assert_eq!(again, body, "the same frozen page, at once");
    h.task("stop", json!({"kind":"connection","event":"stop"}));
    let events = h.run();
    assert!(events.contains(&json!({"type":"cancelEffect","effectId":http})));
    let job = h.job(&id);
    assert_eq!((job.phase, job.attempts), (LoadPhase::Pending, 0));
    assert_eq!(
        h.call("status", json!({"kind":"loadStatus","loadId":id}))["value"]["phase"],
        "waiting"
    );
}

// ------------------------------------------- retry, cancel and fencing

#[test]
fn explicit_retry_rereads_the_committed_continuation_under_a_new_call() {
    let mut h = host();
    h.connect(false);
    let id = h.recent("start");
    let (http, body) = h.batch();
    h.ok(
        &http,
        &response(vec![page(
            &intents(&body)[0],
            &[("a", "A", 1)],
            Some(json!("p2")),
        )]),
    );
    h.run();
    let (http, body) = h.batch();
    let failed_call = call_of(&body, &id);
    h.task("wait", json!({"kind":"loadWait","loadId":id}));
    h.run();
    h.ok(
        &http,
        &response(vec![rejected(
            &intents(&body)[0],
            "failed",
            "cursor.expired",
        )]),
    );
    let events = h.run();
    assert_eq!(
        completion(&events, "wait")["details"]["code"],
        "cursor.expired"
    );
    // A wait on the failed run throws its recorded error at once.
    let again = h.call("again", json!({"kind":"loadWait","loadId":id}));
    assert_eq!(again["details"]["code"], "cursor.expired");
    let retried = h.call("retry", json!({"kind":"loadRetry","loadId":id}));
    assert_eq!(retried["ok"], true);
    let (http, body) = h.batch();
    let intent = intents(&body)[0].clone();
    assert_ne!(intent["callId"], json!(failed_call), "a new call ID");
    assert_eq!(
        intent["continuation"],
        json!({"state":"p2"}),
        "from the committed page"
    );
    assert_eq!(h.job(&id).run, 2);
    assert_eq!(h.job(&id).pages, 1, "committed progress stays");
    // Retrying active work is idempotent: no second request.
    assert_eq!(
        h.call("idle", json!({"kind":"loadRetry","loadId":id}))["ok"],
        true
    );
    assert_eq!(h.batches().len(), 1);
    h.task("wait2", json!({"kind":"loadWait","loadId":id}));
    h.run();
    h.ok(&http, &response(vec![page(&intent, &[], None)]));
    let events = h.run();
    assert_eq!(completion(&events, "wait2")["ok"], true);
    assert!(
        !completed(&events, "wait"),
        "the old waiter already settled once"
    );
}

#[test]
fn cancel_racing_an_admitted_page_serializes_by_admission_order() {
    // The page is admitted first: it applies, then the cancel stops the job
    // before any later page is requested.
    let mut h = host();
    h.connect(false);
    let id = h.recent("start");
    h.task("wait", json!({"kind":"loadWait","loadId":id}));
    h.run();
    let (http, body) = h.batch();
    h.ok(
        &http,
        &response(vec![page(
            &intents(&body)[0],
            &[("a", "A", 1)],
            Some(json!(2)),
        )]),
    );
    h.task("cancel", json!({"kind":"loadCancel","loadId":id}));
    let events = h.run();
    assert!(h.entry("a").is_some(), "the earlier page applied");
    assert_eq!(completion(&events, "cancel")["value"]["phase"], "cancelled");
    assert_eq!(
        completion(&events, "wait")["details"]["code"],
        "load.cancelled"
    );
    assert!(h.batches().is_empty(), "no later page");
    let job = h.job(&id);
    assert_eq!((job.phase, job.pages), (LoadPhase::Cancelled, 1));

    // The cancel is admitted first: the page that arrives after it is inert.
    let mut h = host();
    h.connect(false);
    let id = h.recent("start");
    let (http, body) = h.batch();
    h.task("cancel", json!({"kind":"loadCancel","loadId":id}));
    h.ok(
        &http,
        &response(vec![page(
            &intents(&body)[0],
            &[("a", "A", 1)],
            Some(json!(2)),
        )]),
    );
    h.run();
    assert!(h.entry("a").is_none(), "the page after the cancel is inert");
    assert!(h.batches().is_empty());
    let job = h.job(&id);
    assert_eq!((job.phase, job.pages), (LoadPhase::Cancelled, 0));
    // Cancelling a complete job leaves its completion.
    let done = h.recent("done");
    let (http, body) = h.batch();
    h.ok(&http, &finish_all(&body));
    h.run();
    let answer = h.call("cancel2", json!({"kind":"loadCancel","loadId":done}));
    assert_eq!(answer["value"]["phase"], "complete");
}

#[test]
fn a_held_page_whose_job_moved_on_while_it_waited_for_the_writer_is_inert() {
    let mut h = hooked();
    h.connect(false);
    let blocker = h.recent("blocker");
    let target = h.recent("target");
    let batches = h.batches();
    // The blocker's page parks on its hook; the target's page waits behind
    // it; a cancel admitted after both waits too, and runs before the target
    // page's own lane turn.
    let (http, body) = batches
        .iter()
        .find(|(_, b)| ids(b)[0] == blocker)
        .unwrap()
        .clone();
    let items = intents(&body);
    let mut answers = vec![];
    for intent in &items {
        if intent["loadId"] == blocker.as_str() {
            answers.push(page(intent, &[("h", "H", 1)], None));
        } else {
            answers.push(page(intent, &[("t", "T", 1)], None));
        }
    }
    h.ok(&http, &response(answers));
    let events = h.run();
    let hook = events
        .iter()
        .find(|e| e["operation"]["kind"] == "storeCallback")
        .unwrap()
        .clone();
    h.task("cancel", json!({"kind":"loadCancel","loadId":target}));
    h.run();
    h.submit(json!({"type":"callbackResult","effectId":hook["effectId"],
        "transactionId":hook["operation"]["transactionId"],"ok":true}));
    let events = h.run();
    assert_eq!(completion(&events, "cancel")["value"]["phase"], "cancelled");
    assert!(h.entry("h").is_some());
    assert!(
        h.entry("t").is_none(),
        "the target's held page stored nothing"
    );
    assert_eq!(h.job(&target).pages, 0);
    assert!(
        !events
            .iter()
            .any(|e| e["operation"]["kind"] == "storeCallback"),
        "no hook ran for the inert page"
    );
}

// --------------------------------------------------- once and invalidation

#[test]
fn completed_once_hits_touch_nothing_and_active_joins_share_one_request() {
    let mut h = host();
    h.connect(false);
    let first = h.start("first", json!({"once":true}));
    let joined = h.start("joined", json!({"once":true}));
    assert_eq!(joined["start"], "joined");
    assert_eq!(joined["loadId"], first["loadId"]);
    assert_ne!(joined["observerId"], first["observerId"]);
    let (http, body) = h.batch();
    assert_eq!(intents(&body).len(), 1, "one request for the joined job");
    h.ok(
        &http,
        &response(vec![page(&intents(&body)[0], &[("a", "A", 1)], None)]),
    );
    h.run();
    let watch = h.call("watch", json!({"kind":"watch","model":"Entry"}));
    let watch = watch["value"]["observerId"].clone();
    h.run();
    let generation = h.client().generation();
    h.task(
        "hit",
        json!({"kind":"loadStart","name":"Entries","version":1,
            "args":{"since":null,"projectId":PROJECT.to_uppercase()},"once":true}),
    );
    let events = h.run();
    let hit = completion(&events, "hit");
    assert_eq!(hit["value"]["start"], "reused");
    assert_eq!(hit["value"]["status"]["phase"], "complete");
    assert_eq!(h.client().generation(), generation, "nothing committed");
    assert!(h.batches().is_empty(), "no request");
    assert!(
        !events
            .iter()
            .any(|e| e["type"] == "observerChanged" && e["observerId"] == watch),
        "no Model watch change"
    );
    assert_eq!(
        phases(&events, &hit["value"]["observerId"]),
        ["complete"],
        "the returned handle's observer still initializes"
    );
    assert!(
        position(&events, |e| e["requestId"] == "hit")
            < position(&events, |e| e["observerId"] == hit["value"]["observerId"])
    );
}

#[test]
fn a_late_page_after_invalidation_stores_but_never_restores_reuse() {
    let mut h = host();
    h.connect(false);
    let first = h.start("first", json!({"once":true}));
    let (http, body) = h.batch();
    let removed = h.call(
        "invalidate",
        json!({"kind":"loadInvalidate","name":"Entries","args":{"projectId":PROJECT,"since":null}}),
    );
    assert_eq!(removed["value"], json!({"removed":1}));
    h.ok(
        &http,
        &response(vec![page(&intents(&body)[0], &[("a", "A", 1)], None)]),
    );
    h.run();
    assert!(h.entry("a").is_some(), "the invalidated job still stores");
    assert_eq!(
        h.job(first["loadId"].as_str().unwrap()).phase,
        LoadPhase::Complete
    );
    let next = h.start("next", json!({"once":true}));
    assert_eq!(next["start"], "created", "its completion cannot be reused");
    assert_ne!(next["loadId"], first["loadId"]);
    assert_eq!(h.batches().len(), 1);
}

// ------------------------------------------------ transactions and hooks

#[test]
fn load_management_is_refused_inside_a_callback_or_a_store_hook() {
    let mut h = hooked();
    h.task("tx", json!({"kind":"transaction"}));
    let events = h.run();
    let callback = events
        .iter()
        .find(|e| e["operation"]["kind"] == "callback")
        .unwrap()
        .clone();
    let transaction = callback["operation"]["transactionId"].clone();
    h.submit(
        json!({"type":"transactionCommand","requestId":"inner","transactionId":transaction,
        "command":{"kind":"loadStart","name":"Recent","version":1,"args":{}}}),
    );
    // An ordinary start admitted meanwhile waits for the writer.
    h.task(
        "outer",
        json!({"kind":"loadStart","name":"Recent","version":1,"args":{}}),
    );
    let events = h.run();
    assert_eq!(completion(&events, "inner")["ok"], false);
    assert!(!completed(&events, "outer"));
    // A Mutation's local callback is no wider: its Load command is refused
    // and fails the submission.
    h.submit(
        json!({"type":"transactionCommand","requestId":"submit","transactionId":transaction,
        "command":{"kind":"submitMutation","name":"Ping","version":1,"args":{},"local":true}}),
    );
    let events = h.run();
    let local = events
        .iter()
        .find(|e| e["operation"]["kind"] == "mutationLocal")
        .unwrap_or_else(|| panic!("{events:?}"))
        .clone();
    let companion = local["operation"]["companionId"].clone();
    h.submit(
        json!({"type":"transactionCommand","requestId":"local","transactionId":transaction,
        "companionId":companion,"command":{"kind":"loadStart","name":"Recent","version":1,"args":{}}}),
    );
    let events = h.run();
    assert_eq!(completion(&events, "local")["ok"], false);
    h.submit(
        json!({"type":"callbackResult","effectId":local["effectId"],"transactionId":transaction,
        "companionId":companion,"ok":true}),
    );
    let events = h.run();
    assert_eq!(completion(&events, "submit")["ok"], false);
    h.submit(json!({"type":"callbackResult","effectId":callback["effectId"],"transactionId":transaction,"ok":true}));
    let events = h.run();
    assert_eq!(
        completion(&events, "tx")["ok"],
        false,
        "the refused command poisoned it"
    );
    assert_eq!(completion(&events, "outer")["ok"], true);
    // A store hook has the same local-only capability.
    h.connect(false);
    let id = h.recent("start");
    let (http, body) = h
        .batches()
        .into_iter()
        .find(|(_, b)| ids(b).contains(&id))
        .unwrap();
    let items: Vec<Value> = intents(&body)
        .iter()
        .map(|i| page(i, &[("h", "H", 1)], None))
        .take(1)
        .chain(intents(&body).iter().skip(1).map(|i| page(i, &[], None)))
        .collect();
    h.ok(&http, &response(items));
    let events = h.run();
    let hook = events
        .iter()
        .find(|e| e["operation"]["kind"] == "storeCallback")
        .unwrap()
        .clone();
    let transaction = hook["operation"]["transactionId"].clone();
    for (n, command) in [
        json!({"kind":"loadStart","name":"Recent","version":1,"args":{}}),
        json!({"kind":"loadCancel","loadId":id}),
        json!({"kind":"loadInvalidate","name":"Recent","args":{}}),
    ]
    .into_iter()
    .enumerate()
    {
        h.submit(
            json!({"type":"transactionCommand","requestId":format!("hook{n}"),
            "transactionId":transaction,"command":command}),
        );
    }
    // A local-only command is still available to the hook.
    h.submit(
        json!({"type":"transactionCommand","requestId":"local","transactionId":transaction,
        "command":{"kind":"read","key":{"model":"Entry","identity":{"id":"h"}}}}),
    );
    // Nor can it submit a Mutation, with or without a local callback, or
    // claim a companion token.
    h.submit(
        json!({"type":"transactionCommand","requestId":"hook-submit","transactionId":transaction,
        "command":{"kind":"submitMutation","name":"Ping","version":1,"args":{},"local":true}}),
    );
    h.submit(
        json!({"type":"transactionCommand","requestId":"hook-token","transactionId":transaction,
        "companionId":"c1","command":{"kind":"read","key":{"model":"Entry","identity":{"id":"h"}}}}),
    );
    let events = h.run();
    for n in 0..3 {
        assert_eq!(completion(&events, &format!("hook{n}"))["ok"], false);
    }
    assert_eq!(completion(&events, "local")["ok"], true);
    assert_eq!(
        completion(&events, "hook-submit")["error"],
        "store hook cannot submit a Mutation"
    );
    assert_eq!(
        completion(&events, "hook-token")["error"],
        "invalid transaction capability"
    );
    assert!(
        !events
            .iter()
            .any(|e| e["operation"]["kind"] == "mutationLocal"),
        "{events:?}"
    );
    // The refused commands poisoned the hook: the page fails its job
    // terminally, and it is not read again.
    h.submit(json!({"type":"callbackResult","effectId":hook["effectId"],"transactionId":transaction,"ok":true}));
    h.run();
    let job = h.job(&id);
    assert_eq!(job.phase, LoadPhase::Failed);
    assert_eq!(job.error.unwrap().code, "load.hook_failed");
    assert!(h.entry("h").is_none());
    assert!(h.batches().iter().all(|(_, body)| !ids(body).contains(&id)));
    assert_eq!(h.client().pending_count().unwrap(), 0);
}

/// A Load page is incoming authority like any delivery: while a Mutation's
/// local callback holds the writer it waits, and it is stored - through its
/// onStore hook - only after the transaction committed.
#[test]
fn a_load_page_waits_while_a_local_callback_holds_the_writer() {
    let mut h = hooked();
    h.connect(false);
    let id = h.recent("start");
    let (http, body) = h.batch();
    h.task("tx", json!({"kind":"transaction"}));
    let events = h.run();
    let callback = events
        .iter()
        .find(|e| e["operation"]["kind"] == "callback")
        .unwrap()
        .clone();
    let transaction = callback["operation"]["transactionId"].clone();
    h.submit(
        json!({"type":"transactionCommand","requestId":"submit","transactionId":transaction,
        "command":{"kind":"submitMutation","name":"Ping","version":1,"args":{},"local":true}}),
    );
    let events = h.run();
    let local = events
        .iter()
        .find(|e| e["operation"]["kind"] == "mutationLocal")
        .unwrap_or_else(|| panic!("{events:?}"))
        .clone();
    let companion = local["operation"]["companionId"].clone();
    // The page arrives while the local callback runs.
    let items: Vec<Value> = intents(&body)
        .iter()
        .map(|i| {
            if i["loadId"] == id.as_str() {
                page(i, &[("l", "loaded", 1)], None)
            } else {
                page(i, &[], None)
            }
        })
        .collect();
    h.ok(&http, &response(items));
    h.submit(
        json!({"type":"transactionCommand","requestId":"read","transactionId":transaction,
        "companionId":companion,"command":{"kind":"read","key":{"model":"Entry","identity":{"id":"l"}}}}),
    );
    h.submit(
        json!({"type":"transactionCommand","requestId":"write","transactionId":transaction,
        "companionId":companion,"command":{"kind":"direct","operation":{"model":"Entry","op":"create",
            "identity":{"id":"mine"},"values":{"text":"local","note":null}}}}),
    );
    let events = h.run();
    assert_eq!(
        completion(&events, "read")["value"],
        Value::Null,
        "not stored"
    );
    assert_eq!(completion(&events, "write")["ok"], true);
    h.submit(
        json!({"type":"callbackResult","effectId":local["effectId"],"transactionId":transaction,
        "companionId":companion,"ok":true}),
    );
    let events = h.run();
    let call = completion(&events, "submit")["value"]["callId"].clone();
    assert!(
        !events
            .iter()
            .any(|e| e["operation"]["kind"] == "storeCallback"),
        "{events:?}"
    );
    assert!(h.entry("l").is_none());
    assert_eq!(h.job(&id).pages, 0);
    h.submit(json!({"type":"callbackResult","effectId":callback["effectId"],"transactionId":transaction,"ok":true}));
    let events = h.run();
    let committed = position(&events, |e| {
        *e == json!({"type":"transactionCallState","callId":call,"state":"committed"})
    });
    let answered = position(&events, |e| e["requestId"] == "tx");
    let hook = position(&events, |e| e["operation"]["kind"] == "storeCallback");
    assert!(committed < answered && answered < hook, "{events:?}");
    assert_eq!(h.entry("mine").unwrap()["text"], "local");
    assert!(h.entry("l").is_none(), "the page waits for its hook");
    let hook = events[hook].clone();
    h.submit(json!({"type":"callbackResult","effectId":hook["effectId"],
        "transactionId":hook["operation"]["transactionId"],"ok":true}));
    h.run();
    assert_eq!(h.entry("l").unwrap()["text"], "loaded");
    assert_eq!(h.job(&id).phase, LoadPhase::Complete);
}

#[test]
fn a_page_its_hook_refuses_rolls_back_whole_and_a_retry_commits_it() {
    let mut h = hooked();
    h.connect(false);
    let id = h.recent("start");
    h.task("wait", json!({"kind":"loadWait","loadId":id}));
    h.run();
    let (http, body) = h.batch();
    h.ok(
        &http,
        &response(vec![page(
            &intents(&body)[0],
            &[("a", "A", 1), ("b", "B", 1)],
            None,
        )]),
    );
    let events = h.run();
    let hook = events
        .iter()
        .find(|e| e["operation"]["kind"] == "storeCallback")
        .unwrap()
        .clone();
    assert_eq!(hook["operation"]["changes"].as_array().unwrap().len(), 2);
    let transaction = hook["operation"]["transactionId"].clone();
    // The hook writes, then fails: nothing of the page survives.
    h.submit(json!({"type":"transactionCommand","requestId":"write","transactionId":transaction,
        "command":{"kind":"direct","operation":{"model":"Entry","op":"create","identity":{"id":"side"},"values":{"text":"side","note":null}}}}));
    h.run();
    h.submit(json!({"type":"callbackResult","effectId":hook["effectId"],"transactionId":transaction,"ok":false,"error":"refused"}));
    let events = h.run();
    let report = events
        .iter()
        .find(|e| e["type"] == "report" && e["diagnostic"]["kind"] == "storeHook")
        .unwrap();
    assert_eq!(report["diagnostic"]["path"], "load");
    assert_eq!(
        completion(&events, "wait")["details"]["code"],
        "load.hook_failed"
    );
    for row in ["a", "b", "side"] {
        assert!(h.entry(row).is_none(), "{row} rolled back");
    }
    let job = h.job(&id);
    assert_eq!((job.phase, job.pages), (LoadPhase::Failed, 0));
    let error = job.error.unwrap();
    assert_eq!(error.code, "load.hook_failed");
    assert_eq!(
        error
            .diagnostics
            .iter()
            .map(|d| d.id.clone())
            .collect::<Vec<_>>(),
        [json!({"id":"a"}), json!({"id":"b"})]
    );
    // The explicit retry re-reads; this time the hook accepts.
    h.call("retry", json!({"kind":"loadRetry","loadId":id}));
    let (http, body) = h.batch();
    h.ok(
        &http,
        &response(vec![page(&intents(&body)[0], &[("a", "A", 1)], None)]),
    );
    let events = h.run();
    let hook = events
        .iter()
        .find(|e| e["operation"]["kind"] == "storeCallback")
        .unwrap()
        .clone();
    assert!(h.entry("a").is_none(), "nothing before the hook finished");
    h.submit(json!({"type":"callbackResult","effectId":hook["effectId"],"transactionId":hook["operation"]["transactionId"],"ok":true}));
    h.run();
    assert!(h.entry("a").is_some());
    assert_eq!(h.job(&id).phase, LoadPhase::Complete);
}

#[test]
fn a_page_with_a_record_that_cannot_apply_is_refused_before_any_hook() {
    let mut h = hooked();
    h.connect(false);
    let id = h.recent("start");
    let (http, body) = h.batch();
    let mut item = page(&intents(&body)[0], &[("a", "A", 1), ("b", "B", 1)], None);
    item["records"][1]["state"] = json!({"text": 5, "note": null});
    h.ok(&http, &response(vec![item]));
    let events = h.run();
    assert!(
        !events
            .iter()
            .any(|e| e["operation"]["kind"] == "storeCallback"),
        "no hook ran"
    );
    assert!(h.entry("a").is_none());
    let job = h.job(&id);
    assert_eq!(job.phase, LoadPhase::Failed);
    let error = job.error.unwrap();
    assert_eq!(error.code, "load.store_failed");
    assert_eq!(error.diagnostics[0].code, "skipped");
    // The application hears which record refused the page, through the
    // records report every other delivery uses; the good record is absent.
    let reports: Vec<&Value> = events
        .iter()
        .filter(|e| e["type"] == "report" && e["diagnostic"]["kind"] == "records")
        .collect();
    assert_eq!(reports.len(), 1, "{events:?}");
    let records = reports[0]["diagnostic"]["reports"].as_array().unwrap();
    assert_eq!(records.len(), 1, "{records:?}");
    assert_eq!(records[0]["kind"], "skipped");
    assert_eq!(records[0]["model"], "Entry");
    assert_eq!(records[0]["identity"], json!({"id":"b"}));
}

#[test]
fn a_failed_page_commit_backs_off_and_resends_the_same_call() {
    let dir = tempfile::tempdir().unwrap();
    let (store, fail) = CommitFaultStore::open(&dir.path().join("db"));
    let client = Client::open(store, Schema::from_value(schema_value()).unwrap()).unwrap();
    let mut h = Host::of(ClientRuntime::new(client), Some(dir));
    h.connect(false);
    let answer = h.call(
        "start",
        json!({"kind":"loadStart","name":"Recent","version":1,"args":{}}),
    );
    let id = answer["value"]["loadId"].as_str().unwrap().to_string();
    let (http, body) = h.batch();
    fail.store(true, std::sync::atomic::Ordering::SeqCst);
    h.ok(
        &http,
        &response(vec![page(&intents(&body)[0], &[("a", "A", 1)], None)]),
    );
    h.run();
    assert!(h.entry("a").is_none());
    let job = h.job(&id);
    assert_eq!(
        (job.phase, job.retry, job.attempts),
        (LoadPhase::Pending, Some(LoadRetryClass::Local), 1)
    );
    let (timer, _) = h.backoff();
    h.fire(&timer);
    h.run();
    let (http, again) = h.batch();
    assert_eq!(again, body, "the same frozen call");
    h.ok(&http, &finish_all(&body));
    h.run();
    assert_eq!(h.job(&id).phase, LoadPhase::Complete);
}

// ------------------------------------------------- rebuild and close

fn breaking_value() -> Value {
    let mut schema = schema_value();
    for models in ["models", "resultModels"] {
        schema[models][0]["fields"]
            .as_array_mut()
            .unwrap()
            .push(json!({"name":"due","nullable":false,"type":{"kind":"scalar","name":"string"}}));
    }
    schema
}

#[test]
fn a_pending_rebuild_parks_loads_beside_the_mutation_drain_and_the_rebuild_ends_their_handles() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = Client::open_at(
        &path,
        Schema::from_value(schema_value()).unwrap(),
        factory(),
        false,
    )
    .unwrap();
    let args = json!({"projectId":PROJECT,"since":null});
    let active = c
        .start_load("Entries", 1, &args, LoadOptions::default())
        .unwrap()
        .job
        .id;
    let cancellable = c
        .start_load("Recent", 1, &json!({}), LoadOptions::default())
        .unwrap()
        .job
        .id;
    c.transaction(|tx| {
        tx.enqueue(Mutation::new(
            "Edit",
            vec![create("Entry", "q", json!({"text":"queued","note":null}))],
        ))
    })
    .unwrap();
    drop(c);
    // The application now asks for an incompatible schema, with a store hook
    // on its Entry: the old file keeps running until its Mutation drains.
    let runtime = open_runtime(&path, breaking_value(), &["Entry"]);
    let mut h = Host::of(runtime, Some(dir));
    assert!(h.client().schema_state().pending.is_some());
    h.connect(false);
    assert!(h.batches().is_empty(), "no page goes out");
    let (push, op) = h.one("http", Some("push"));
    for (n, command) in [
        json!({"kind":"loadStart","name":"Recent","version":1,"args":{}}),
        json!({"kind":"loadStart","name":"Recent","version":1,"args":{},"once":true,"refresh":true}),
        json!({"kind":"loadRetry","loadId":active}),
        json!({"kind":"loadInvalidate","name":"Recent","args":{}}),
    ]
    .into_iter()
    .enumerate()
    {
        let refused = h.call(&format!("refused{n}"), command);
        assert_eq!(refused["details"]["code"], "load.schema_pending", "{refused}");
    }
    let handle = h.call("get", json!({"kind":"loadGet","loadId":active}));
    let observer = handle["value"]["observerId"].clone();
    assert_eq!(handle["value"]["status"]["phase"], "waiting");
    assert_eq!(
        h.call("list", json!({"kind":"loadList","limit":10}))["value"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        h.call("cancel", json!({"kind":"loadCancel","loadId":cancellable}))["value"]["phase"],
        "cancelled"
    );
    h.task("wait", json!({"kind":"loadWait","loadId":active}));
    h.run();
    // The Mutation drains while the Loads stay parked.
    let client_id = h.client().client_id().to_string();
    let body: Value = serde_json::from_str(op["body"].as_str().unwrap()).unwrap();
    h.ok(
        &push,
        &json!({"clientId":client_id,"batchSequence":body["batchSequence"],"rejections":[],
            "records":[{"model":"Entry","identity":{"id":"q"},"stamp":1,"state":{"text":"queued","note":null}}]})
        .to_string(),
    );
    h.run();
    assert_eq!(h.client().pending_count().unwrap(), 0);
    assert!(h.batches().is_empty());
    // The rebuild abandons both jobs and ends their handles and waiters.
    h.task("rebuild", json!({"kind":"rebuild"}));
    let events = h.run();
    let rebuilt = completion(&events, "rebuild");
    assert_eq!(
        rebuilt["value"]["abandonedLoads"],
        json!([active, cancellable])
    );
    assert_eq!(
        completion(&events, "wait")["details"]["code"],
        "load.schema_changed"
    );
    let ended = events
        .iter()
        .find(|e| e["type"] == "observerChanged" && e["observerId"] == observer)
        .unwrap();
    assert_eq!(ended["snapshot"]["closed"], true);
    assert_eq!(ended["snapshot"]["code"], "load.schema_changed");
    // The handle's last status says why it ended.
    assert_eq!(ended["snapshot"]["status"]["id"], json!(active));
    assert_eq!(ended["snapshot"]["status"]["phase"], "failed");
    assert_eq!(
        ended["snapshot"]["status"]["error"]["code"],
        "load.schema_changed"
    );
    assert_eq!(
        h.call("gone", json!({"kind":"loadGet","loadId":active}))["value"],
        Value::Null
    );
    // Managing an abandoned job - any ID spelling - answers schema_changed,
    // not not_found; an ID the old replica never held is still not_found.
    for (n, id) in [active.clone(), cancellable.clone(), active.to_uppercase()]
        .into_iter()
        .enumerate()
    {
        for kind in [
            "loadStatus",
            "loadWait",
            "loadCancel",
            "loadRetry",
            "loadForget",
        ] {
            let refused = h.call(&format!("{kind}{n}"), json!({"kind":kind,"loadId":id}));
            assert_eq!(refused["ok"], false, "{kind} {id}");
            assert_eq!(
                refused["details"]["code"], "load.schema_changed",
                "{kind} {id}: {refused}"
            );
            assert!(refused["details"]["message"].is_string());
        }
    }
    let unknown = h.call(
        "unknown",
        json!({"kind":"loadWait","loadId":"0190f0e0-0000-7000-8000-000000000000"}),
    );
    assert_eq!(unknown["details"]["code"], "load.not_found");
    // A new start on the fresh replica runs the target schema's hook.
    let fresh = h.recent("fresh");
    let (http, body) = h.batch();
    assert_eq!(ids(&body), [fresh.as_str()]);
    let mut item = page(&intents(&body)[0], &[("n", "N", 1)], None);
    item["records"][0]["state"]["due"] = json!("today");
    h.ok(&http, &response(vec![item]));
    let events = h.run();
    let hook = events
        .iter()
        .find(|e| e["operation"]["kind"] == "storeCallback")
        .unwrap_or_else(|| panic!("{events:?}"))
        .clone();
    h.submit(json!({"type":"callbackResult","effectId":hook["effectId"],"transactionId":hook["operation"]["transactionId"],"ok":true}));
    h.run();
    assert_eq!(h.job(&fresh).phase, LoadPhase::Complete);
}

#[test]
fn close_rejects_waiters_and_ends_handles_without_cancelling_the_job() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let runtime = open_runtime(&path, schema_value(), &[]);
    let mut h = Host::of(runtime, None);
    h.connect(false);
    let started = h.start("start", json!({}));
    let id = started["loadId"].as_str().unwrap().to_string();
    h.task("wait", json!({"kind":"loadWait","loadId":id}));
    h.run();
    let (http, _) = h.batch();
    h.submit(json!({"type":"close"}));
    let events = h.run();
    assert_eq!(
        completion(&events, "wait")["details"]["code"],
        "client_closed"
    );
    assert!(events.contains(&json!({"type":"cancelEffect","effectId":http})));
    let ended = events
        .iter()
        .find(|e| e["type"] == "observerChanged" && e["observerId"] == started["observerId"])
        .unwrap();
    assert_eq!(ended["snapshot"]["closed"], true);
    assert_eq!(events.last().unwrap()["type"], "runtimeClosed");
    drop(h);
    let mut c = Client::open_at(
        &path,
        Schema::from_value(schema_value()).unwrap(),
        factory(),
        false,
    )
    .unwrap();
    let job = c.get_load(&id).unwrap().unwrap();
    assert_eq!(
        (job.phase, job.attempts),
        (LoadPhase::Pending, 0),
        "the job survives"
    );
    drop(dir);
}

#[test]
fn load_pages_and_downlink_pages_alternate_one_application_per_turn() {
    let mut h = host();
    let started: Vec<String> = (0..4).map(|n| h.recent(&format!("s{n}"))).collect();
    h.connect(false);
    h.task("subscribe", json!({"kind":"scopeSubscribe","scope":"book"}));
    h.run();
    let (socket, _) = h.one("socket", None);
    h.answer(
        &socket,
        json!({"ok":true,"value":{"event":"message","body":ack(&[("book", 0)])}}),
    );
    h.run();
    let (http, body) = h.batch();
    assert_eq!(ids(&body), started);
    // Four live pages and four Load pages arrive together.
    for n in 0..4u64 {
        h.answer(
            &socket,
            json!({"ok":true,"value":{"event":"message","body":pull(n, n + 1, &format!("l{n}"), "L")}}),
        );
    }
    let items: Vec<Value> = intents(&body)
        .iter()
        .enumerate()
        .map(|(n, i)| page(i, &[(&format!("a{n}"), "A", 1)], None))
        .collect();
    h.ok(&http, &response(items));
    h.take();
    let count = |h: &mut Host, prefix: &str| {
        (0..4)
            .filter(|n| h.entry(&format!("{prefix}{n}")).is_some())
            .count()
    };
    let mut seen = vec![(0, 0)];
    while h.runtime.step(h.now, ENTROPY) {
        h.take();
        let now = (count(&mut h, "l"), count(&mut h, "a"));
        if now != *seen.last().unwrap() {
            seen.push(now);
        }
    }
    assert_eq!(*seen.last().unwrap(), (4, 4), "{seen:?}");
    for pair in seen.windows(2) {
        let (before, after) = (pair[0], pair[1]);
        assert!(
            after.1 <= before.1 + 1 && after.0 <= before.0 + 1,
            "one application per unit: {seen:?}"
        );
    }
    assert!(
        seen.iter()
            .any(|(live, load)| *live == 1 && *load >= 1 && *load < 4),
        "the Load lane does not wait for the Downlink to drain: {seen:?}"
    );
    assert!(
        seen.iter()
            .any(|(live, load)| *load == 1 && *live >= 1 && *live < 4),
        "nor the Downlink for the Load lane: {seen:?}"
    );
}

#[test]
fn stale_timer_callback_and_refresh_answers_change_nothing() {
    let mut h = hooked();
    h.connect(true);
    // A backoff timer outlives the job it was set for.
    let cancelled = h.recent("first");
    let (http, _) = h.batch();
    h.fail(&http, "offline", None);
    h.run();
    let (timer, _) = h.backoff();
    h.call("cancel", json!({"kind":"loadCancel","loadId":cancelled}));
    h.fire(&timer);
    h.run();
    assert!(h.batches().is_empty(), "the stale timer sends nothing");
    // A callback answer that names another effect joins nothing; the page
    // commits only with its own, and its observer publishes after that.
    let id = h.recent("second");
    let observer =
        h.call("get", json!({"kind":"loadGet","loadId":id}))["value"]["observerId"].clone();
    let (http, body) = h.batch();
    h.ok(
        &http,
        &response(vec![page(&intents(&body)[0], &[("a", "A", 1)], None)]),
    );
    let events = h.run();
    let hook = events
        .iter()
        .find(|e| e["operation"]["kind"] == "storeCallback")
        .unwrap()
        .clone();
    h.submit(json!({"type":"callbackResult","effectId":"999999","transactionId":hook["operation"]["transactionId"],"ok":true}));
    h.run();
    assert!(h.entry("a").is_none());
    assert_eq!(h.job(&id).phase, LoadPhase::Pending);
    h.submit(json!({"type":"callbackResult","effectId":hook["effectId"],"transactionId":hook["operation"]["transactionId"],"ok":true}));
    h.until_event(|e| {
        e["observerId"] == observer && e["snapshot"]["status"]["phase"] == "complete"
    });
    assert_eq!(
        h.job(&id).phase,
        LoadPhase::Complete,
        "published after its commit"
    );
    assert!(h.entry("a").is_some());
    // A refresh answered after stop resends nothing.
    let third = h.recent("third");
    let (http, _) = h.batch();
    h.fail(&http, "unauthorized", Some(401));
    h.run();
    let (refresh, _) = h.one("refreshAuth", None);
    h.task("stop", json!({"kind":"connection","event":"stop"}));
    let events = h.run();
    assert!(events.contains(&json!({"type":"cancelEffect","effectId":refresh})));
    h.answer(&refresh, json!({"ok":true}));
    h.run();
    assert!(h.batches().is_empty());
    let job = h.job(&third);
    assert_eq!((job.phase, job.attempts), (LoadPhase::Pending, 0));
}

// ------------------------------------------------------ fix round 1

#[test]
fn an_unsendable_page_fails_its_job_while_the_others_complete() {
    let mut h = host();
    let oversized = oversized_next_page(h.client());
    h.task("wait", json!({"kind":"loadWait","loadId":oversized}));
    let healthy = h.recent("healthy");
    h.task(
        "connect",
        json!({"kind":"connect","directTimeoutMs":DEADLINE}),
    );
    let events = h.run();
    let failure = completion(&events, "wait");
    assert_eq!(failure["details"]["code"], "load.request_too_large");
    let (http, body) = h.batch();
    assert_eq!(ids(&body), [healthy.as_str()], "the younger job goes out");
    h.ok(&http, &finish_all(&body));
    h.run();
    assert_eq!(h.job(&healthy).phase, LoadPhase::Complete);
    let job = h.job(&oversized);
    assert_eq!((job.phase, job.pages), (LoadPhase::Failed, 1));
    assert_eq!(job.error.unwrap().code, "load.request_too_large");
}

#[test]
fn a_whole_request_4xx_splits_the_batch_and_a_page_refused_alone_fails() {
    let mut h = host();
    let started: Vec<String> = (0..3).map(|n| h.recent(&format!("s{n}"))).collect();
    h.task("wait", json!({"kind":"loadWait","loadId":started[0]}));
    h.connect(false);
    let (http, body) = h.batch();
    assert_eq!(ids(&body), started);
    h.fail(&http, "HTTP 413", Some(413));
    let events = h.run();
    assert_eq!(errors(&events), ["HTTP 413"]);
    assert!(
        h.outstanding("timer", None)
            .iter()
            .all(|(_, op)| op["millis"] == DEADLINE),
        "no backoff"
    );
    let mut singles = h.batches();
    singles.sort_by_key(|(_, b)| ids(b)[0].clone());
    assert_eq!(singles.len(), 2, "requests of one, two at a time");
    for (_, single) in &singles {
        assert_eq!(intents(single).len(), 1);
        let id = &ids(single)[0];
        assert_eq!(
            call_of(single, id),
            call_of(&body, id),
            "the same frozen call"
        );
        assert_eq!(h.job(id).attempts, 0, "a refused request counts no attempt");
    }
    // The first job's page is refused alone: that job fails, only that one.
    let (first, _) = singles
        .iter()
        .find(|(_, b)| ids(b)[0] == started[0])
        .unwrap()
        .clone();
    h.fail(&first, "HTTP 400", Some(400));
    let events = h.run();
    assert_eq!(
        completion(&events, "wait")["details"]["code"],
        "load.protocol_invalid"
    );
    assert_eq!(h.job(&started[0]).phase, LoadPhase::Failed);
    // The others complete, each alone.
    while h.job(&started[1]).phase != LoadPhase::Complete
        || h.job(&started[2]).phase != LoadPhase::Complete
    {
        let (http, body) = h.batches().into_iter().next().expect("a request");
        assert_eq!(intents(&body).len(), 1);
        h.ok(&http, &finish_all(&body));
        h.run();
    }
}

#[test]
fn a_408_429_or_5xx_backs_off_without_splitting() {
    for status in [408u16, 429, 503] {
        let mut h = host();
        let started: Vec<String> = (0..2).map(|n| h.recent(&format!("s{n}"))).collect();
        h.connect(false);
        let (http, body) = h.batch();
        h.fail(&http, "busy", Some(status));
        h.run();
        assert!(
            h.batches().is_empty(),
            "{status}: nothing resent before the delay"
        );
        let (timer, _) = h.backoff();
        for id in &started {
            assert_eq!(h.job(id).attempts, 1, "{status}");
        }
        h.fire(&timer);
        h.run();
        let (_, again) = h.batch();
        assert_eq!(again, body, "{status}: the same two pages together");
    }
}

#[test]
fn retrying_a_job_that_backs_off_keeps_its_delay() {
    let mut h = host();
    h.connect(false);
    let id = h.recent("start");
    let (http, body) = h.batch();
    h.fail(&http, "offline", None);
    h.run();
    let (timer, millis) = h.backoff();
    assert_eq!(millis, 1_000);
    let retried = h.call("retry", json!({"kind":"loadRetry","loadId":id}));
    assert_eq!(retried["value"]["phase"], "waiting");
    let events = h.run();
    assert!(!events.contains(&json!({"type":"cancelEffect","effectId":timer})));
    assert!(h.batches().is_empty(), "no request before the delay");
    assert_eq!(h.backoff(), (timer.clone(), 1_000), "the same timer");
    h.fire(&timer);
    h.run();
    let (_, again) = h.batch();
    assert_eq!(again, body);
}

/// A SQLite store whose next scheduler read of ready Loads fails once.
struct ScanFaultStore {
    inner: SqliteStore,
    fail_scan: std::sync::Arc<std::sync::atomic::AtomicBool>,
}
impl ScanFaultStore {
    fn scan(&self, sql: &str) -> Result<()> {
        if sql.contains("WHERE phase = 'pending' ORDER BY ready")
            && self
                .fail_scan
                .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(axton_core::invalid("injected scan failure"));
        }
        Ok(())
    }
}
impl ClientStore for ScanFaultStore {
    fn begin(&mut self) -> Result<()> {
        self.inner.begin()
    }
    fn commit(&mut self) -> Result<()> {
        self.inner.commit()
    }
    fn rollback(&mut self) -> Result<()> {
        self.inner.rollback()
    }
    fn savepoint(&mut self, name: &str) -> Result<()> {
        self.inner.savepoint(name)
    }
    fn release(&mut self, name: &str) -> Result<()> {
        self.inner.release(name)
    }
    fn rollback_to(&mut self, name: &str) -> Result<()> {
        self.inner.rollback_to(name)
    }
    fn execute(&mut self, sql: &str, parameters: &[Value]) -> Result<usize> {
        self.inner.execute(sql, parameters)
    }
    fn execute_batch(&mut self, sql: &str) -> Result<()> {
        self.inner.execute_batch(sql)
    }
    fn query(&mut self, sql: &str, parameters: &[Value]) -> Result<SqlRows> {
        self.scan(sql)?;
        self.inner.query(sql, parameters)
    }
    fn query_committed(&mut self, sql: &str, parameters: &[Value]) -> Result<SqlRows> {
        self.scan(sql)?;
        self.inner.query_committed(sql, parameters)
    }
}

#[test]
fn a_failed_scheduler_read_is_retried_on_its_own_timer() {
    let dir = tempfile::tempdir().unwrap();
    let fail = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let store = ScanFaultStore {
        inner: SqliteStore::open(dir.path().join("db")).unwrap(),
        fail_scan: fail.clone(),
    };
    let client = Client::open(store, Schema::from_value(schema_value()).unwrap()).unwrap();
    let mut h = Host::of(ClientRuntime::new(client), Some(dir));
    h.connect(false);
    fail.store(true, std::sync::atomic::Ordering::SeqCst);
    h.task(
        "second",
        json!({"kind":"loadStart","name":"Recent","version":1,"args":{}}),
    );
    let events = h.run();
    let second = completion(&events, "second")["value"]["loadId"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(
        errors(&events)[0].starts_with("load scheduling failed"),
        "{events:?}"
    );
    assert!(h.batches().is_empty(), "the failed read sent nothing");
    let (retry, millis) = h.backoff();
    assert_eq!(millis, 1_000);
    h.fire(&retry);
    h.run();
    let (_, body) = h.batch();
    assert_eq!(ids(&body), [second.as_str()]);
}

#[test]
fn a_start_whose_first_request_exceeds_the_bound_is_coded_and_stores_nothing() {
    let mut h = host();
    let tag = "x".repeat(limits::LOAD_REQUEST_BYTES);
    for (kind, options) in [
        ("start", json!({})),
        ("once", json!({"once":true})),
        ("refresh", json!({"once":true,"refresh":true})),
    ] {
        let mut command =
            json!({"kind":"loadStart","name":"Tagged","version":1,"args":{"tags":[tag]}});
        for (k, v) in options.as_object().unwrap() {
            command[k] = v.clone();
        }
        let refused = h.call(kind, command);
        assert_eq!(refused["ok"], false, "{kind}");
        assert_eq!(
            refused["details"]["code"], "load.request_too_large",
            "{kind}: {}",
            refused["details"]
        );
        assert!(refused["details"]["message"].is_string());
    }
    assert!(
        h.call("list", json!({"kind":"loadList"}))["value"]
            .as_array()
            .unwrap()
            .is_empty(),
        "nothing was stored"
    );
    // A start that fits is unaffected.
    let fits = h.call(
        "fits",
        json!({"kind":"loadStart","name":"Tagged","version":1,"args":{"tags":["x"]}}),
    );
    assert_eq!(fits["ok"], true, "{fits}");
}

#[test]
fn invalid_business_arguments_are_coded_for_every_start_and_invalidation() {
    let mut h = host();
    let cases = [
        ("uuid", json!({"projectId":"not-a-uuid","since":null})),
        ("type", json!({"projectId":5,"since":null})),
        ("missing", json!({"since":null})),
        (
            "undeclared",
            json!({"projectId":PROJECT,"since":null,"extra":1}),
        ),
    ];
    for (case, args) in cases {
        for (kind, options) in [
            ("start", json!({})),
            ("once", json!({"once":true})),
            ("refresh", json!({"once":true,"refresh":true})),
        ] {
            let mut command = json!({"kind":"loadStart","name":"Entries","version":1,"args":args});
            for (k, v) in options.as_object().unwrap() {
                command[k] = v.clone();
            }
            let refused = h.call(&format!("{case}-{kind}"), command);
            assert_eq!(refused["ok"], false, "{case} {kind}");
            assert_eq!(
                refused["details"]["code"], "load.invalid_args",
                "{case} {kind}: {refused}"
            );
            assert!(refused["details"]["message"].is_string());
        }
        let refused = h.call(
            &format!("{case}-invalidate"),
            json!({"kind":"loadInvalidate","name":"Entries","args":args}),
        );
        assert_eq!(
            refused["details"]["code"], "load.invalid_args",
            "{case} invalidate: {refused}"
        );
    }
    assert!(
        h.call("list", json!({"kind":"loadList"}))["value"]
            .as_array()
            .unwrap()
            .is_empty(),
        "nothing was stored"
    );
}

/// An admission refusal is the client's, not the page's: the batch is
/// abandoned without failing its job or counting an attempt, nothing is
/// retried, and a later connection resends the same frozen page (#181).
#[test]
fn an_admission_refusal_leaves_the_job_pending_for_a_later_connection() {
    let mut h = host();
    let id = h.recent("start");
    h.connect(true);
    let (http, body) = h.batch();
    h.answer(
        &http,
        json!({"ok":false,"error":{"message":"load failed: 426","status":426,"refusal":"{\"minimumBuild\":7}"}}),
    );
    let events = h.run();
    let refused: Vec<&Value> = events
        .iter()
        .filter(|e| e["type"] == "report" && e["diagnostic"]["kind"] == "refused")
        .collect();
    assert_eq!(refused.len(), 1, "{events:?}");
    assert_eq!(refused[0]["diagnostic"]["body"], json!({"minimumBuild":7}));
    assert_eq!(errors(&events), Vec::<String>::new());
    assert!(h.open.is_empty(), "nothing is retried: {:?}", h.open);
    let job = h.job(&id);
    assert_eq!((job.phase, job.attempts), (LoadPhase::Pending, 0));

    h.connect(true);
    let (_, again) = h.batch();
    assert_eq!(again, body, "the same frozen page and call ID");
}

/// A Load page commits Entry rows, so a statement reading Entry re-emits
/// after each page; the job's own ledger commits write no table it reads and
/// re-run nothing ([#184](https://github.com/zanminwang/axton/issues/184)).
#[test]
fn a_load_page_re_emits_a_watched_statement_and_its_ledger_commits_do_not() {
    let mut h = host();
    // Every re-run publishes: its `random()` column differs.
    let answer = h.call(
        "watch",
        json!({"kind":"watchSql","sql":"SELECT group_concat(id) AS ids, random() AS r FROM Entry","parameters":[]}),
    );
    let observer = answer["value"]["observerId"].clone();
    let ids = |events: &[Value]| -> Vec<Value> {
        events
            .iter()
            .filter(|e| e["type"] == "observerChanged" && e["observerId"] == observer)
            .map(|e| e["snapshot"]["rows"][0]["ids"].clone())
            .collect()
    };
    h.task(
        "start",
        json!({"kind":"loadStart","name":"Entries","version":1,"args":{"projectId":PROJECT,"since":null}}),
    );
    let events = h.run();
    assert!(completion(&events, "start")["ok"] == true);
    assert!(ids(&events).is_empty(), "the start commits only its job");
    h.connect(false);
    let (http, body) = h.batch();
    let intent = &intents(&body)[0];
    h.ok(
        &http,
        &response(vec![page(
            intent,
            &[("a", "A", 1)],
            Some(json!({"after":"a"})),
        )]),
    );
    let events = h.run();
    assert_eq!(ids(&events), [json!("a")], "the first page");
    let (http, body) = h.batch();
    let intent = &intents(&body)[0];
    h.ok(&http, &response(vec![page(intent, &[("b", "B", 1)], None)]));
    let events = h.run();
    assert_eq!(ids(&events), [json!("a,b")], "the final page");
}
