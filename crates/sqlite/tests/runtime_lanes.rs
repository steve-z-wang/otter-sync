//! The runtime owns the operation lifecycles
//! ([#134](https://github.com/zanminwang/axton/issues/134)): the connection
//! lanes, the Downlink worker, direct calls, Query once, prerequisites,
//! rebuild fencing, and the observers - subscription status, Bootstrap
//! waiters and local watches - over a real SQLite store. The test is
//! the host: it answers every effect the runtime asks for, with a fixed
//! clock that a fired timer advances. No sleeps, no threads.
mod common;
use axton_client::runtime::{ClientRuntime, Input};
use axton_client::*;
use axton_sqlite::SqliteStore;
use common::ack;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::Path;

const ENTROPY: u64 = 200;

fn schema_value() -> Value {
    let mut schema: Value =
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap();
    schema["actions"] = json!([
        {"name":"Rename","version":1,"inputs":[{"kind":"model","name":"entry","model":"Entry","operation":"update","cardinality":"single"}],
         "outputs":[{"name":"text","kind":"value","type":{"kind":"scalar","name":"string"},"cardinality":"single","source":"handlerValue"}]},
        {"name":"Echo","version":1,"kind":"query","inputs":[{"kind":"value","name":"label","type":{"kind":"scalar","name":"string"},"nullable":false}],
         "outputs":[{"name":"label","kind":"value","type":{"kind":"scalar","name":"string"},"cardinality":"single","source":"handlerValue"}]},
        {"name":"Ping","version":1,"inputs":[],"outputs":[]}
    ]);
    schema
}
fn factory() -> StoreFactory<SqliteStore> {
    Box::new(|p| SqliteStore::open(p))
}

/// The host of one runtime: it keeps every event, the effects still
/// outstanding, and the clock.
struct Host {
    runtime: ClientRuntime<SqliteStore>,
    now: u64,
    open: BTreeMap<String, Value>,
    _dir: Option<tempfile::TempDir>,
}
fn host() -> Host {
    host_with(schema_value())
}
fn host_with(schema: Value) -> Host {
    let dir = tempfile::tempdir().unwrap();
    let runtime = ClientRuntime::open_at(
        dir.path().join("db"),
        Schema::from_value(schema).unwrap(),
        factory(),
        false,
    )
    .unwrap();
    Host::of(runtime, Some(dir))
}
impl Host {
    fn of(runtime: ClientRuntime<SqliteStore>, dir: Option<tempfile::TempDir>) -> Self {
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
            json!({"kind":"connect","directTimeoutMs":1000,"refreshAuth":refresh}),
        );
        let events = self.run();
        assert!(events.contains(&done("connect", Value::Null)), "{events:?}");
    }
    /// Track what the host was asked to do.
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
    /// What the runtime said since the last call: admission first, then every
    /// step until it has nothing runnable.
    fn run(&mut self) -> Vec<Value> {
        let mut events = self.take();
        while self.runtime.step(self.now, ENTROPY) {
            events.extend(self.take());
        }
        events
    }
    /// One step at a time until `id` completes: what the task's completion is
    /// observed with, and nothing after it.
    fn until(&mut self, id: &str) -> Vec<Value> {
        self.until_event(|e| e["type"] == "taskCompleted" && e["requestId"] == id)
    }
    /// One step at a time until an event matches: the database can be
    /// inspected at the very point it is observed.
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
    /// The outstanding effects of one kind (and route, for HTTP).
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
    fn http(&self, route: &str) -> (String, String) {
        let (id, op) = self.one("http", Some(route));
        (id, op["body"].as_str().unwrap().to_string())
    }
    fn socket(&self) -> String {
        self.one("socket", None).0
    }
    fn answer(&mut self, id: &str, outcome: Value) {
        let streaming = self.open.get(id).is_some_and(|op| op["kind"] == "socket")
            && outcome["ok"] == true
            && outcome["value"]["event"] != "closed";
        if !streaming {
            self.open.remove(id);
        }
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
    fn frame(&mut self, socket: &str, body: &str) {
        self.answer(
            socket,
            json!({"ok":true,"value":{"event":"message","body":body}}),
        );
    }
    /// The timer's time passed: the clock moves and the timer answers.
    fn fire(&mut self, timer: &str) {
        let millis = self.open[timer]["millis"].as_u64().unwrap();
        self.now += millis;
        self.answer(timer, json!({"ok":true,"value":null}));
    }
    fn client(&mut self) -> &mut Client<SqliteStore> {
        self.runtime.client()
    }
    fn text(&mut self, id: &str) -> Option<Value> {
        let key = RecordKey {
            model: "Entry".into(),
            identity: json!({ "id": id }),
        };
        self.client()
            .read(&key)
            .unwrap()
            .map(|row| row["text"].clone())
    }
    /// Subscribe `book`, and on the socket that follows acknowledge `head`.
    fn streaming(&mut self, head: u64) -> String {
        self.task("subscribe", json!({"kind":"scopeSubscribe","scope":"book"}));
        let events = self.run();
        assert_eq!(sockets(&events).len(), 1, "{events:?}");
        let socket = self.socket();
        self.frame(&socket, &ack(&[("book", head)]));
        self.run();
        socket
    }
    /// Run one task to quiescence and answer its completion.
    fn call(&mut self, id: &str, command: Value) -> Value {
        self.task(id, command);
        let events = self.run();
        self.completion(&events, id).clone()
    }
    fn completion<'a>(&self, events: &'a [Value], id: &str) -> &'a Value {
        events
            .iter()
            .find(|e| e["type"] == "taskCompleted" && e["requestId"] == id)
            .unwrap_or_else(|| panic!("{id} did not complete: {events:?}"))
    }
}

fn done(id: &str, value: Value) -> Value {
    json!({"type":"taskCompleted","requestId":id,"ok":true,"value":value})
}
fn failed(id: &str, error: &str) -> Value {
    json!({"type":"taskCompleted","requestId":id,"ok":false,"value":null,"error":error})
}
/// The socket effects asked for: one per session.
fn sockets(events: &[Value]) -> Vec<Value> {
    events
        .iter()
        .filter(|e| e["type"] == "effect" && e["operation"]["kind"] == "socket")
        .cloned()
        .collect()
}
/// The subscription statuses published, in order.
fn statuses(events: &[Value]) -> Vec<Value> {
    events
        .iter()
        .filter(|e| e["type"] == "observerChanged" && e["snapshot"]["kind"] == "subscription")
        .map(|e| e["snapshot"]["status"].clone())
        .collect()
}
/// The `connection` of every subscription status published, in order.
fn connections(events: &[Value]) -> Vec<String> {
    statuses(events)
        .iter()
        .map(|s| s["connection"].as_str().unwrap().to_string())
        .collect()
}
/// The Bootstrap phase of every subscription status published, in order.
fn phases(events: &[Value]) -> Vec<String> {
    statuses(events)
        .iter()
        .map(|s| s["bootstrap"]["phase"].as_str().unwrap().to_string())
        .collect()
}
fn position(events: &[Value], matches: impl Fn(&Value) -> bool) -> usize {
    events
        .iter()
        .position(matches)
        .unwrap_or_else(|| panic!("not found in {events:?}"))
}
fn errors(events: &[Value]) -> Vec<String> {
    events
        .iter()
        .filter(|e| e["type"] == "report" && e["diagnostic"]["kind"] == "error")
        .map(|e| e["diagnostic"]["message"].as_str().unwrap().to_string())
        .collect()
}
fn cancelled(events: &[Value], id: &str) -> bool {
    events.contains(&json!({"type":"cancelEffect","effectId":id}))
}
fn page(from: u64, to: u64, id: &str, text: &str) -> String {
    json!({"cursors":{"book":{"from":from,"to":to,"head":to}},"changes":[{"model":"Entry","identity":{"id":id},"stamp":to,"state":{"text":text,"note":null}}]}).to_string()
}
fn call_id(body: &str) -> String {
    serde_json::from_str::<Value>(body).unwrap()["call"]["callId"]
        .as_str()
        .unwrap()
        .to_string()
}
fn create(id: &str, text: &str) -> Value {
    json!({"kind":"direct","operation":{"model":"Entry","op":"create","identity":{"id":id},"values":{"text":text}}})
}
fn rename(text: &str) -> Value {
    json!({"kind":"invoke","name":"Rename","version":1,"args":{"entry":{"id":"e","text":text}}})
}
fn renamed(body: &str, text: &str) -> String {
    json!({"completion":{"callId":call_id(body),"outcome":{"status":"succeeded","result":{"text":text}}},
           "records":[{"model":"Entry","identity":{"id":"e"},"stamp":1,"state":{"text":text,"note":null}}]})
    .to_string()
}
fn echo(extra: Value) -> Value {
    let mut command = json!({"kind":"invoke","name":"Echo","version":1,"args":{"label":"hi"}});
    for (k, v) in extra.as_object().unwrap() {
        command[k] = v.clone();
    }
    command
}
fn echoed(body: &str, label: &str) -> String {
    json!({"completion":{"callId":call_id(body),"outcome":{"status":"succeeded","result":{"label":label}}},"records":[]})
        .to_string()
}
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

#[test]
fn connect_starts_both_lanes_and_the_worker_streams_catches_up_and_applies() {
    let mut h = host();
    h.connect(true);
    assert!(
        h.outstanding("socket", None).is_empty(),
        "no Scope is registered: no socket"
    );
    h.task("again", json!({"kind":"connect"}));
    assert!(
        h.run()
            .contains(&failed("again", "connection already active"))
    );
    // The host cannot drive the lanes: there is no command for it.
    h.task("start", json!({"kind":"connection","event":"start"}));
    h.task("pump", json!({"kind":"downlink","event":"next"}));
    let events = h.run();
    assert!(
        h.completion(&events, "start")["error"]
            .as_str()
            .unwrap()
            .starts_with("unknown variant `start`")
    );
    assert!(
        h.completion(&events, "pump")["error"]
            .as_str()
            .unwrap()
            .starts_with("unknown variant `downlink`")
    );

    h.task("subscribe", json!({"kind":"scopeSubscribe","scope":"book"}));
    let events = h.run();
    let socket = h.socket();
    let subscribe: Value =
        serde_json::from_str(h.open[&socket]["subscribe"].as_str().unwrap()).unwrap();
    assert_eq!(
        subscribe,
        json!({"type":"subscribe","channels":["book"],"models":{"Entry":1}})
    );
    assert_eq!(sockets(&events).len(), 1);
    assert!(
        position(&events, |e| e["type"] == "taskCompleted"
            && e["requestId"] == "subscribe")
            < position(&events, |e| e["type"] == "effect"
                && e["effectId"] == socket.as_str()),
        "the registration commits before the socket is asked for"
    );
    assert_eq!(connections(&events), ["connecting"]);

    // The handshake commits the first boundary before the status says so.
    h.answer(&socket, json!({"ok":true,"value":{"event":"opened"}}));
    h.frame(&socket, &ack(&[("book", 0)]));
    let mut events = h.until_event(|e| e["type"] == "observerChanged");
    assert_eq!(h.client().cursor("book").unwrap(), Some(0));
    events.extend(h.run());
    assert_eq!(
        statuses(&events),
        [
            json!({"active":true,"initialization":"ready","connection":"live","bootstrap":{"phase":"not-requested","error":null}})
        ]
    );
    assert_eq!(h.client().cursor("book").unwrap(), Some(0));

    // A streamed page applies in one transaction; the status is unchanged.
    h.frame(&socket, &page(0, 1, "e", "first"));
    let events = h.run();
    assert_eq!(statuses(&events), Vec::<Value>::new());
    assert_eq!(h.text("e"), Some(json!("first")));

    // A gap asks for a catch-up over HTTP; a page streamed meanwhile waits.
    h.frame(&socket, &page(5, 6, "e", "gap"));
    let events = h.run();
    let (pull, body) = h.http("pull");
    assert_eq!(
        serde_json::from_str::<Value>(&body).unwrap(),
        json!({"cursors":{"book":1},"models":{"Entry":1}})
    );
    assert_eq!(connections(&events), ["catching-up"]);
    h.frame(&socket, &page(1, 2, "e", "queued"));
    h.run();
    assert_eq!(
        h.text("e"),
        Some(json!("first")),
        "nothing applies while a catch-up is out"
    );
    // Its answer overlaps the queued page: applied once, the queued page is
    // covered, and the gap still does not connect, so one more catch-up runs.
    h.ok(&pull, &page(1, 3, "e", "incoming overlap"));
    let events = h.run();
    assert_eq!(connections(&events), ["live", "catching-up"]);
    let (_, body) = h.http("pull");
    assert_eq!(
        serde_json::from_str::<Value>(&body).unwrap()["cursors"],
        json!({"book":3})
    );
    assert_eq!(h.client().cursor("book").unwrap(), Some(3));
    assert_eq!(h.text("e"), Some(json!("incoming overlap")));
    assert_eq!(sockets(&events), Vec::<Value>::new());
}

#[test]
fn a_direct_call_waits_without_the_writer_and_succeeds_only_after_its_apply_commits() {
    let mut h = host();
    h.connect(false);
    let socket = h.streaming(0);
    h.task("seed", create("e", "seed"));
    h.run();
    h.task("call", rename("server"));
    h.run();
    let (http, body) = h.http("action");
    let (timer, deadline) = h.one("timer", None);
    assert_eq!(deadline["millis"], 1000);
    assert_eq!(
        serde_json::from_str::<Value>(&body).unwrap()["call"]["name"],
        "Rename"
    );
    // While the request is out, a local write and an unrelated page commit.
    h.task("local", create("x", "local"));
    h.frame(&socket, &page(0, 1, "p", "paged"));
    let events = h.run();
    assert!(events.contains(&done("local", Value::Null)));
    assert_eq!(h.text("x"), Some(json!("local")));
    assert_eq!(h.text("p"), Some(json!("paged")));
    assert!(!events.iter().any(|e| e["requestId"] == "call"));
    // The answer: its apply commits, then the call's completion, then the task.
    h.ok(&http, &renamed(&body, "server"));
    let mut events = h.until_event(|e| e["type"] == "callCompleted");
    assert_eq!(h.text("e"), Some(json!("server")), "committed first");
    events.extend(h.run());
    let completed = position(&events, |e| e["type"] == "callCompleted");
    let success = position(&events, |e| e["requestId"] == "call");
    assert!(completed < success, "{events:?}");
    assert_eq!(
        events[success],
        done(
            "call",
            json!({"outcome":{"status":"succeeded","result":{"text":"server"}}})
        )
    );
    assert_eq!(events[completed]["callId"], call_id(&body));
    assert_eq!(
        h.text("e"),
        Some(json!("server")),
        "observed after the commit"
    );
    assert!(cancelled(&events, &timer), "the deadline is gone");

    // `store:false` with nothing to write opens no transaction.
    h.task("quiet", echo(json!({"store":false})));
    h.run();
    let (http, body) = h.http("action");
    let generation = h.client().generation();
    h.ok(&http, &echoed(&body, "hi"));
    let events = h.run();
    assert_eq!(h.client().generation(), generation, "nothing committed");
    assert_eq!(
        events.last().unwrap(),
        &done(
            "quiet",
            json!({"outcome":{"status":"succeeded","result":{"label":"hi"}}})
        )
    );

    // The result is the invocation's snapshot: optimism queued after the
    // response changes the Model, not the returned outcome.
    h.task("snapshot", rename("authority"));
    h.run();
    let (http, body) = h.http("action");
    h.ok(&http, &renamed(&body, "authority"));
    h.task(
        "optimism",
        json!({"kind":"enqueue","mutation":{"name":"Edit","operations":[{"model":"Entry","op":"update","identity":{"id":"e"},"values":{"text":"local"}}]}}),
    );
    let events = h.run();
    assert_eq!(
        h.completion(&events, "snapshot"),
        &done(
            "snapshot",
            json!({"outcome":{"status":"succeeded","result":{"text":"authority"}}})
        )
    );
    assert_eq!(h.completion(&events, "optimism")["ok"], true);
    assert_eq!(h.text("e"), Some(json!("local")));
}
#[test]
fn the_push_lane_freezes_sends_settles_and_backs_off_with_one_shared_refresh() {
    let mut h = host();
    h.connect(true);
    let client_id = h.client().client_id().to_string();
    h.task(
        "submit",
        json!({"kind":"submitAction","name":"Ping","version":1,"args":{}}),
    );
    let events = h.run();
    let accepted = h.completion(&events, "submit").clone();
    let call = accepted["value"]["callId"].as_str().unwrap().to_string();
    assert_eq!(accepted["value"]["ordinal"], 1);
    assert!(!events.iter().any(|e| e["type"] == "callCompleted"));
    let (push, body) = h.http("push");
    assert!(
        position(&events, |e| e["requestId"] == "submit")
            < position(&events, |e| e["effectId"] == push.as_str()),
        "acceptance completes before the lane sends it"
    );
    // The receipt settles in one transaction; the call's outcome follows it.
    h.ok(&push, &receipt(&client_id, &body));
    let mut events = h.until_event(|e| e["type"] == "callCompleted");
    assert_eq!(h.client().pending_count().unwrap(), 0, "settled first");
    events.extend(h.run());
    let outcome = position(&events, |e| e["type"] == "callCompleted");
    assert_eq!(
        events[outcome],
        json!({"type":"callCompleted","callId":call,"outcome":{"status":"succeeded","result":null}})
    );
    assert_eq!(h.client().pending_count().unwrap(), 0);
    assert!(h.outstanding("http", Some("push")).is_empty());

    // A transport failure: reported, backed off, retried with the same bytes.
    h.task(
        "again",
        json!({"kind":"submitAction","name":"Ping","version":1,"args":{}}),
    );
    h.run();
    let (push, body) = h.http("push");
    h.fail(&push, "offline", None);
    let events = h.run();
    assert_eq!(errors(&events), ["offline"]);
    let (timer, wait) = h.one("timer", None);
    assert!((200..=300).contains(&wait["millis"].as_u64().unwrap()));
    assert!(h.outstanding("http", Some("push")).is_empty());
    h.fire(&timer);
    h.run();
    let (push, retried) = h.http("push");
    assert_eq!(retried, body, "the frozen batch is sent again unchanged");

    // A 401 on the push and on the socket together: one refresh for both.
    // The commits of the subscription wake the push lane, which still has
    // its one batch out.
    let socket = h.streaming(0);
    assert_eq!(h.http("push").0, push, "one frozen batch in flight");
    h.fail(&push, "unauthorized", Some(401));
    h.fail(&socket, "unauthorized", Some(401));
    let events = h.run();
    assert_eq!(errors(&events), ["unauthorized", "unauthorized"]);
    let (refresh, _) = h.one("refreshAuth", None);
    assert!(
        h.outstanding("timer", None).is_empty(),
        "both wait for the refresh before backing off"
    );
    assert_eq!(connections(&events), ["connecting"], "the session ended");
    h.answer(&refresh, json!({"ok":true,"value":null}));
    h.run();
    assert!(h.outstanding("refreshAuth", None).is_empty());
    assert_eq!(
        h.outstanding("timer", None).len(),
        2,
        "each lane backs off on its own schedule: {:?}",
        h.open
    );
}

#[test]
fn receipts_direct_applies_and_pages_each_commit_in_their_own_step_beside_a_bootstrap() {
    let mut h = host();
    h.connect(false);
    let client_id = h.client().client_id().to_string();
    h.task("subscribe", json!({"kind":"scopeSubscribe","scope":"book"}));
    let events = h.run();
    let subscription =
        h.completion(&events, "subscribe")["value"]["state"]["subscriptionId"].clone();
    h.task(
        "load",
        json!({"kind":"scopeBootstrap","scope":"book","subscriptionId":subscription}),
    );
    h.run();
    let socket = h.socket();
    h.frame(&socket, &ack(&[("book", 7)]));
    h.run();
    let (load, body) = h.http("pull");
    assert_eq!(
        serde_json::from_str::<Value>(&body).unwrap(),
        json!({"mode":"bootstrap","channel":"book","models":{"Entry":1},"after":0,"until":7})
    );
    // Live traffic flows beside the outstanding historical page.
    h.frame(&socket, &page(7, 8, "live", "streamed"));
    h.run();
    assert_eq!(h.text("live"), Some(json!("streamed")));
    // A push, a direct call and a page, all answered before any step runs.
    h.task("seed", create("e", "seed"));
    h.task(
        "submit",
        json!({"kind":"submitAction","name":"Ping","version":1,"args":{}}),
    );
    h.task("call", rename("direct"));
    h.run();
    let (push, push_body) = h.http("push");
    let (call, call_body) = h.http("action");
    h.ok(&push, &receipt(&client_id, &push_body));
    h.ok(&call, &renamed(&call_body, "direct"));
    h.frame(&socket, &page(8, 9, "live", "again"));
    h.ok(&load, &json!({"mode":"bootstrap","channel":"book","from":0,"to":7,"until":7,"head":9,"records":[]}).to_string());
    let mut events = h.take();
    let mut steps = 0;
    loop {
        let before = h.client().generation();
        if !h.runtime.step(h.now, ENTROPY) {
            break;
        }
        steps += 1;
        let after = h.client().generation();
        assert!(after - before <= 1, "one commit per step");
        events.extend(h.take());
    }
    assert!(steps >= 4);
    assert!(events.iter().any(|e| e["type"] == "callCompleted"));
    assert_eq!(h.completion(&events, "call")["ok"], true);
    assert_eq!(h.text("live"), Some(json!("again")));
    assert_eq!(h.text("e"), Some(json!("direct")));
    // The terminal page fixed the barrier at head 9, which live delivery
    // reaches in its own commits: the run completes whichever came first.
    let run = h
        .client()
        .bootstrap_state("book", subscription.as_u64().unwrap())
        .unwrap();
    assert_eq!(run.barrier, Some(9));
    assert_eq!(
        phases(&events).last().map(String::as_str),
        Some("complete"),
        "delivery reached the barrier: {events:?}"
    );
    assert_eq!(h.completion(&events, "load"), &done("load", Value::Null));

    // Duplicate and stale answers change nothing.
    let generation = h.client().generation();
    h.ok(&push, &receipt(&client_id, &push_body));
    h.ok(&call, &renamed(&call_body, "twice"));
    h.ok(&load, "{}");
    assert_eq!(h.run(), Vec::<Value>::new());
    assert_eq!(h.client().generation(), generation);
    // An answer on a socket the runtime cancelled is fenced as well.
    h.task("pause", json!({"kind":"connection","event":"pause"}));
    let events = h.run();
    assert!(cancelled(&events, &socket));
    h.frame(&socket, &page(9, 10, "live", "late"));
    h.answer(&socket, json!({"ok":true,"value":{"event":"closed"}}));
    assert_eq!(h.run(), Vec::<Value>::new());
    assert_eq!(h.text("live"), Some(json!("again")));
}

#[test]
fn pause_abandons_lane_io_without_backoff_resume_asks_again_and_stop_is_final() {
    let mut h = host();
    h.task("subscribe", json!({"kind":"scopeSubscribe","scope":"book"}));
    let events = h.run();
    let subscription =
        h.completion(&events, "subscribe")["value"]["state"]["subscriptionId"].clone();
    // Controls with no connection are no-ops.
    for event in ["pause", "resume", "wake", "stop"] {
        h.task(event, json!({"kind":"connection","event":event}));
        assert_eq!(h.run(), vec![done(event, Value::Null)]);
    }
    h.connect(false);
    h.task(
        "load",
        json!({"kind":"scopeBootstrap","scope":"book","subscriptionId":subscription}),
    );
    h.run();
    let socket = h.socket();
    h.frame(&socket, &ack(&[("book", 5)]));
    h.run();
    let (load, _) = h.http("pull");
    h.frame(&socket, &page(7, 8, "e", "gap"));
    h.run();
    let catch_up = h
        .outstanding("http", Some("pull"))
        .into_iter()
        .find(|(id, _)| *id != load)
        .unwrap()
        .0;
    h.task(
        "submit",
        json!({"kind":"submitAction","name":"Ping","version":1,"args":{}}),
    );
    h.run();
    let (push, body) = h.http("push");

    h.task("pause", json!({"kind":"connection","event":"pause"}));
    let events = h.run();
    assert!(events.contains(&done("pause", Value::Null)));
    for id in [&socket, &load, &catch_up, &push] {
        assert!(cancelled(&events, id), "{id} abandoned: {events:?}");
    }
    assert_eq!(
        errors(&events),
        Vec::<String>::new(),
        "the lane's own abort"
    );
    assert!(
        h.open.is_empty(),
        "no backoff timer, nothing in flight: {:?}",
        h.open
    );
    assert_eq!(connections(&events), ["offline"]);
    // Paused, nothing is asked for, even after a commit.
    h.task(
        "more",
        json!({"kind":"submitAction","name":"Ping","version":1,"args":{}}),
    );
    h.run();
    assert!(h.open.is_empty(), "{:?}", h.open);

    h.task("resume", json!({"kind":"connection","event":"resume"}));
    let events = h.run();
    assert_eq!(connections(&events), ["connecting"]);
    let (_, resent) = h.http("push");
    assert_eq!(
        resent, body,
        "the abandoned batch is still in flight and is sent again"
    );
    let socket = h.socket();
    let (reloaded, again) = h
        .outstanding("http", Some("pull"))
        .into_iter()
        .next()
        .expect("the bootstrap page is asked for again");
    assert_ne!(reloaded, load);
    assert_eq!(
        serde_json::from_str::<Value>(again["body"].as_str().unwrap()).unwrap()["mode"],
        "bootstrap"
    );

    // Stop: a direct call in flight is unavailable, everything is abandoned.
    h.task("call", rename("x"));
    h.run();
    let (call, _) = h.http("action");
    h.task("stop", json!({"kind":"connection","event":"stop"}));
    let events = h.run();
    assert!(events.contains(&failed_with(
        "call",
        "action.unavailable",
        json!({"code":"action.unavailable"})
    )));
    assert!(cancelled(&events, &call) && cancelled(&events, &socket));
    assert_eq!(connections(&events), ["offline"]);
    assert!(h.open.is_empty(), "{:?}", h.open);
    // A stopped connection: invoke is unavailable, controls are no-ops, and a
    // new connect starts over.
    h.task("late", rename("y"));
    h.task("wake", json!({"kind":"connection","event":"wake"}));
    let events = h.run();
    assert!(events.contains(&failed("late", "action.unavailable")));
    assert!(events.contains(&done("wake", Value::Null)));
    assert!(h.open.is_empty());
    h.connect(false);
    h.socket();
}

#[test]
fn a_direct_call_times_out_refreshes_once_on_401_and_fails_unavailable_at_close() {
    let mut h = host();
    h.connect(true);
    // The deadline passes first: the request is abandoned, its late answer
    // is fenced.
    h.task("slow", rename("slow"));
    h.run();
    let (http, body) = h.http("action");
    let (timer, _) = h.one("timer", None);
    h.fire(&timer);
    let events = h.run();
    assert!(cancelled(&events, &http));
    assert!(events.contains(&failed_with(
        "slow",
        "action.execution_unknown",
        json!({"code":"action.execution_unknown","message":"direct call timed out"})
    )));
    h.ok(&http, &renamed(&body, "slow"));
    assert_eq!(h.run(), Vec::<Value>::new());
    assert_eq!(h.text("e"), None);

    // A 401: one refresh, then the same body once more under the same timer.
    h.task("auth", rename("auth"));
    h.run();
    let (http, body) = h.http("action");
    let (timer, _) = h.one("timer", None);
    h.fail(&http, "unauthorized", Some(401));
    let events = h.run();
    assert_eq!(
        errors(&events),
        Vec::<String>::new(),
        "the call's own failure"
    );
    let (refresh, _) = h.one("refreshAuth", None);
    assert!(h.outstanding("http", Some("action")).is_empty());
    h.answer(&refresh, json!({"ok":true,"value":null}));
    h.run();
    let (retry, resent) = h.http("action");
    assert_eq!(resent, body);
    assert_eq!(
        h.one("timer", None).0,
        timer,
        "the same deadline keeps running"
    );
    h.ok(&retry, &renamed(&body, "auth"));
    let events = h.run();
    assert_eq!(h.completion(&events, "auth")["ok"], true);
    assert!(cancelled(&events, &timer));

    // A second 401 after the retry is not retried again.
    h.task("twice", rename("twice"));
    h.run();
    let (http, _) = h.http("action");
    h.fail(&http, "unauthorized", Some(401));
    h.run();
    let (refresh, _) = h.one("refreshAuth", None);
    h.answer(&refresh, json!({"ok":true,"value":null}));
    h.run();
    let (http, _) = h.http("action");
    h.fail(&http, "unauthorized", Some(401));
    let events = h.run();
    assert!(events.contains(&failed_with(
        "twice",
        "action.execution_unknown",
        json!({"code":"action.execution_unknown","message":"unauthorized","status":401})
    )));
    assert!(h.outstanding("refreshAuth", None).is_empty());
    // A failed refresh fails the call as unknown and is reported.
    h.task("refused", rename("refused"));
    h.run();
    let (http, _) = h.http("action");
    h.fail(&http, "unauthorized", Some(401));
    h.run();
    let (refresh, _) = h.one("refreshAuth", None);
    h.fail(&refresh, "no credentials", None);
    let events = h.run();
    assert_eq!(errors(&events), ["no credentials"]);
    // The refusal is the cause: its own message, and no status it lacked.
    assert!(events.contains(&failed_with(
        "refused",
        "action.execution_unknown",
        json!({"code":"action.execution_unknown","message":"no credentials"})
    )));
    // Any other failure: unknown at once.
    h.task("down", rename("down"));
    h.run();
    let (http, _) = h.http("action");
    h.fail(&http, "HTTP 503", Some(503));
    assert!(h.run().contains(&failed_with(
        "down",
        "action.execution_unknown",
        json!({"code":"action.execution_unknown","message":"HTTP 503","status":503})
    )));

    // Close while a call is out: unavailable, and nothing answers later.
    h.task("closing", rename("closing"));
    h.run();
    let (http, body) = h.http("action");
    h.submit(json!({"type":"close"}));
    let events = h.run();
    assert!(events.contains(&failed_with(
        "closing",
        "action.unavailable",
        json!({"code":"action.unavailable"})
    )));
    assert!(cancelled(&events, &http));
    assert_eq!(events.last().unwrap(), &json!({"type":"runtimeClosed"}));
    let late: Input = serde_json::from_value(json!({"type":"effectResult","effectId":http,"outcome":{"ok":true,"value":{"status":200,"body":renamed(&body, "late")}}})).unwrap();
    assert!(h.runtime.receive(late, h.now, ENTROPY).is_err());
}

#[test]
fn query_once_is_decided_fetched_joined_and_released_by_the_runtime() {
    let mut h = host();
    // Refresh without once is not an option.
    h.task("bad", echo(json!({"refresh":true})));
    assert!(h.run().contains(&failed("bad", "action.invalid_options")));
    // No connection: a miss is unavailable and releases its flight.
    h.task("offline", echo(json!({"once":true})));
    assert!(h.run().contains(&failed("offline", "action.unavailable")));
    h.connect(false);
    // Two concurrent calls: one request, two completions.
    h.task("first", echo(json!({"once":true})));
    h.task("joined", echo(json!({"once":true})));
    h.run();
    let (http, body) = h.http("action");
    let generation = h.client().generation();
    h.ok(&http, &echoed(&body, "hi"));
    let events = h.run();
    let outcome = json!({"outcome":{"status":"succeeded","result":{"label":"hi"}}});
    assert_eq!(
        h.completion(&events, "first"),
        &done("first", outcome.clone())
    );
    assert_eq!(
        h.completion(&events, "joined"),
        &done("joined", outcome.clone())
    );
    assert_ne!(h.client().generation(), generation, "the result is saved");
    // A hit: no effect, no write.
    let generation = h.client().generation();
    h.task("cached", echo(json!({"once":true})));
    let events = h.run();
    assert_eq!(events, vec![done("cached", outcome.clone())]);
    assert_eq!(h.client().generation(), generation);
    // A refresh fetches; its failure keeps the saved result.
    h.task("refresh", echo(json!({"once":true,"refresh":true})));
    h.task("refresh-joined", echo(json!({"once":true,"refresh":true})));
    h.run();
    let (http, _) = h.http("action");
    h.fail(&http, "offline", None);
    let events = h.run();
    let offline = json!({"code":"action.execution_unknown","message":"offline"});
    assert!(events.contains(&failed_with(
        "refresh",
        "action.execution_unknown",
        offline.clone()
    )));
    assert!(events.contains(&failed_with(
        "refresh-joined",
        "action.execution_unknown",
        offline
    )));
    h.task("still", echo(json!({"once":true})));
    assert_eq!(h.run(), vec![done("still", outcome.clone())]);
    // The failed flight was released: the next refresh fetches again.
    h.task("next", echo(json!({"once":true,"refresh":true})));
    h.run();
    h.http("action");
    // Once is a task, never a command of an open transaction.
    h.task("tx", json!({"kind":"transaction"}));
    let events = h.run();
    let effect = events
        .iter()
        .find(|e| e["operation"]["kind"] == "callback")
        .unwrap();
    let transaction = effect["operation"]["transactionId"].clone();
    h.submit(json!({"type":"transactionCommand","requestId":"inside","transactionId":transaction,"command":echo(json!({"once":true}))}));
    let events = h.run();
    assert!(
        h.completion(&events, "inside")["error"]
            .as_str()
            .unwrap()
            .starts_with("unknown variant `invoke`"),
        "{events:?}"
    );
    h.submit(json!({"type":"callbackResult","effectId":effect["effectId"],"transactionId":transaction,"ok":true}));
    h.run();
    // Close with a joined caller: both fail as unavailable.
    h.task("waiting", echo(json!({"once":true,"refresh":true})));
    let events = h.run();
    assert!(!events.iter().any(|e| e["requestId"] == "waiting"));
    h.submit(json!({"type":"close"}));
    let events = h.run();
    assert!(events.contains(&failed_with(
        "next",
        "action.unavailable",
        json!({"code":"action.unavailable"})
    )));
    assert!(events.contains(&failed_with(
        "waiting",
        "action.unavailable",
        json!({"code":"action.unavailable"})
    )));
}

#[test]
fn prerequisites_run_as_effects_and_every_outcome_wakes_the_push_lane() {
    let mut h = host();
    h.connect(false);
    h.task("seed", create("e", "seed"));
    let key = |id: &str| json!({"name":"upload","arguments":{"id":id}}).to_string();
    h.task(
        "edit",
        json!({"kind":"enqueue","mutation":{"name":"Edit","operations":[{"model":"Entry","op":"update","identity":{"id":"e"},"values":{"text":"edited"}}],"prerequisites":[key("a"),key("b")]}}),
    );
    h.run();
    assert!(h.outstanding("http", Some("push")).is_empty(), "blocked");
    h.task(
        "run",
        json!({"kind":"runPrerequisites","handlers":["upload"]}),
    );
    h.task(
        "twice",
        json!({"kind":"runPrerequisites","handlers":["upload"]}),
    );
    let events = h.run();
    assert!(events.contains(&failed("twice", "prerequisites already running")));
    let (first, op) = h.one("prerequisite", None);
    assert_eq!(op["name"], "upload");
    assert_eq!(op["key"], key("a"));
    assert_eq!(op["arguments"], json!({"id":"a"}));
    // A failing handler keeps its reason; the loop goes on.
    let generation = h.client().generation();
    h.fail(&first, "disk full", None);
    h.run();
    assert_ne!(h.client().generation(), generation, "the outcome committed");
    let (second, op) = h.one("prerequisite", None);
    assert_eq!(op["key"], key("b"));
    h.task("tasks", json!({"kind":"tasks"}));
    let events = h.run();
    let tasks = h.completion(&events, "tasks")["value"].clone();
    let a = tasks
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["key"] == key("a"))
        .unwrap();
    assert_eq!(
        (a["state"].clone(), a["error"].clone()),
        (json!("failed"), json!("disk full"))
    );
    h.answer(&second, json!({"ok":true,"value":null}));
    let events = h.run();
    assert!(events.contains(&done("run", Value::Null)));
    assert!(h.outstanding("prerequisite", None).is_empty());
    // The push lane woke after each outcome and found the mutation still
    // blocked by the failed task.
    assert!(h.outstanding("http", Some("push")).is_empty());
    h.task(
        "ready",
        json!({"kind":"readiness","key":key("a"),"state":"ready"}),
    );
    h.run();
    h.http("push");
}

/// A replica left incompatible with unsent work: rebuild is pending, and
/// `book` is carried over by name.
fn pending_rebuild(dir: &Path) -> ClientRuntime<SqliteStore> {
    rebuildable(dir, true, true)
}
/// A replica left incompatible with unsent work, optionally subscribed to
/// `book` and with a durable load of it registered.
fn rebuildable(dir: &Path, channel: bool, bootstrap: bool) -> ClientRuntime<SqliteStore> {
    let path = dir.join("db");
    {
        let mut runtime = ClientRuntime::open_at(
            &path,
            Schema::from_value(schema_value()).unwrap(),
            factory(),
            false,
        )
        .unwrap();
        let client = runtime.client();
        if channel {
            client
                .transaction(|tx| tx.set_channel("book".into(), true))
                .unwrap();
        }
        client
            .transaction(|tx| {
                tx.enqueue(Mutation::new(
                    "Edit",
                    vec![common::create("Entry", "e", json!({"text":"unsent"}))],
                ))
            })
            .unwrap();
        if bootstrap {
            let subscription = client.ensure_subscription("book").unwrap();
            client
                .request_bootstrap("book", subscription.subscription_id)
                .unwrap();
        }
        client.submit_action("Ping", 1, json!({})).unwrap();
    }
    let mut breaking = schema_value();
    breaking["models"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"due","nullable":false,"type":{"kind":"scalar","name":"string"}}));
    ClientRuntime::open_at(
        &path,
        Schema::from_value(breaking).unwrap(),
        factory(),
        false,
    )
    .unwrap()
}

#[test]
fn a_rebuild_fences_old_lane_io_and_reopens_in_the_same_intent() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = Host::of(pending_rebuild(dir.path()), Some(dir));
    let status = {
        h.task("status", json!({"kind":"status"}));
        let events = h.run();
        h.completion(&events, "status")["value"].clone()
    };
    assert!(status["schema"]["pending"].is_object(), "{status}");
    h.connect(false);
    let (old_push, _) = h.http("push");
    let old_socket = h.socket();
    h.frame(&old_socket, &ack(&[("book", 2)]));
    h.run();
    let (old_load, _) = h.http("pull");
    h.frame(&old_socket, &page(3, 4, "e", "gap"));
    h.run();
    let old_pull = h
        .outstanding("http", Some("pull"))
        .into_iter()
        .find(|(id, _)| *id != old_load)
        .unwrap()
        .0;
    let generation = h.client().generation();
    // A refused rebuild changes nothing.
    h.task("refused", json!({"kind":"rebuild"}));
    let events = h.run();
    assert_eq!(h.completion(&events, "refused")["ok"], false);
    assert!(
        !events.iter().any(|e| e["type"] == "cancelEffect"),
        "{events:?}"
    );
    assert_eq!(h.client().generation(), generation);
    assert_eq!(h.socket(), old_socket);

    h.task("rebuild", json!({"kind":"rebuild","discardPending":true}));
    let events = h.run();
    let report = h.completion(&events, "rebuild")["value"].clone();
    assert_eq!(report["leftPending"], 2);
    // The call left behind was never sent (the batch out held the plain
    // mutation): it is rejected, announced before the rebuild's own answer.
    let abandoned = report["abandonedCalls"][0]["callId"].clone();
    assert_eq!(report["abandonedCalls"][0]["frozen"], false);
    let outcome = position(&events, |e| e["type"] == "callCompleted");
    assert_eq!(
        events[outcome],
        json!({"type":"callCompleted","callId":abandoned,"outcome":{"status":"failed","code":"abandoned","execution":"rejected"}})
    );
    assert!(outcome < position(&events, |e| e["requestId"] == "rebuild"));
    for id in [&old_socket, &old_pull, &old_load, &old_push] {
        assert!(cancelled(&events, id), "{id}: {events:?}");
    }
    // A new session follows without another connect.
    let socket = h.socket();
    assert_ne!(socket, old_socket);
    assert_eq!(sockets(&events).len(), 1);
    // Old answers change nothing.
    let cursors = h.client().subscriptions().unwrap();
    let generation = h.client().generation();
    h.ok(
        &old_load,
        &json!({"mode":"bootstrap","channel":"book","from":0,"to":2,"until":2,"head":2,"records":[]})
            .to_string(),
    );
    h.ok(&old_push, "{}");
    h.frame(&old_socket, &ack(&[("book", 9)]));
    h.frame(&old_socket, &page(0, 1, "e", "old"));
    h.ok(&old_pull, &page(0, 4, "e", "old"));
    h.fail(&old_pull, "late", Some(503));
    h.answer(&old_socket, json!({"ok":true,"value":{"event":"overflow"}}));
    h.answer(&old_socket, json!({"ok":true,"value":{"event":"closed"}}));
    assert_eq!(h.run(), Vec::<Value>::new());
    assert_eq!(h.client().generation(), generation);
    assert_eq!(h.client().subscriptions().unwrap(), cursors);
    assert_eq!(h.text("e"), None);
    assert_eq!(h.socket(), socket);
    // The new session initializes the carried Scope at its own head.
    h.frame(&socket, &ack(&[("book", 5)]));
    h.run();
    assert_eq!(h.client().cursor("book").unwrap(), Some(5));
}

/// A record the replaced replica's I/O would write if it reached the fresh
/// file, complete under the rebuilt schema.
fn late() -> Value {
    json!({"model":"Entry","identity":{"id":"late"},"stamp":9,"state":{"text":"late","note":null,"due":"now"}})
}
/// A historical page of `book` over `(0, 7]` with the barrier at head 9.
fn historical(records: Value) -> String {
    json!({"mode":"bootstrap","channel":"book","from":0,"to":7,"until":7,"head":9,"records":records})
        .to_string()
}
impl Host {
    /// Deliver everything the replaced replica's socket, catch-up, historical
    /// page and push can still answer - every socket event, an answer and a
    /// failure of each request - and check that none of it reaches the fresh
    /// replica: nothing is said, nothing commits, no cursor moves, no stale
    /// record lands.
    fn inert(&mut self, socket: &str, pull: Option<&str>, load: Option<&str>, push: Option<&str>) {
        let generation = self.client().generation();
        let cursors = self.client().subscriptions().unwrap();
        let page =
            json!({"cursors":{"book":{"from":0,"to":9,"head":9}},"changes":[late()]}).to_string();
        self.frame(socket, &page);
        self.frame(socket, &ack(&[("book", 3)]));
        self.answer(socket, json!({"ok":true,"value":{"event":"overflow"}}));
        if let Some(pull) = pull {
            self.ok(pull, &page);
            self.fail(pull, "late", None);
        }
        if let Some(load) = load {
            self.ok(load, &historical(json!([late()])));
            self.fail(load, "HTTP 400", Some(400));
            self.fail(load, "late", None);
        }
        if let Some(push) = push {
            self.ok(push, "{}");
            self.fail(push, "unauthorized", Some(401));
        }
        self.answer(socket, json!({"ok":true,"value":{"event":"closed"}}));
        assert_eq!(self.run(), Vec::<Value>::new(), "old I/O answers nothing");
        assert_eq!(self.client().generation(), generation, "nothing committed");
        assert_eq!(self.client().subscriptions().unwrap(), cursors);
        assert_eq!(self.text("late"), None, "no stale record landed");
    }
}

#[test]
fn a_paused_lane_stays_paused_through_a_rebuild_until_resume() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = Host::of(pending_rebuild(dir.path()), Some(dir));
    h.connect(false);
    let old_socket = h.socket();
    h.frame(&old_socket, &ack(&[("book", 7)]));
    h.run();
    let (old_load, _) = h.http("pull");
    let (old_push, _) = h.http("push");
    h.task("pause", json!({"kind":"connection","event":"pause"}));
    h.run();
    assert!(h.open.is_empty(), "{:?}", h.open);
    h.task("rebuild", json!({"kind":"rebuild","discardPending":true}));
    let events = h.run();
    assert_eq!(h.completion(&events, "rebuild")["ok"], true);
    assert!(
        h.open.is_empty(),
        "paused: nothing is asked for: {:?}",
        h.open
    );
    // The old I/O - a historical page among it - is inert.
    h.inert(&old_socket, None, Some(&old_load), Some(&old_push));
    h.task("wake", json!({"kind":"connection","event":"wake"}));
    assert_eq!(h.run(), vec![done("wake", Value::Null)], "still paused");
    assert!(h.open.is_empty(), "{:?}", h.open);
    // Resume opens exactly one session and asks for nothing else.
    h.task("resume", json!({"kind":"connection","event":"resume"}));
    let events = h.run();
    assert_eq!(sockets(&events).len(), 1, "resume opens once: {events:?}");
    assert_ne!(h.socket(), old_socket);
    assert!(h.outstanding("http", None).is_empty(), "{:?}", h.open);
    h.task("again", json!({"kind":"connection","event":"wake"}));
    assert!(sockets(&h.run()).is_empty(), "and only once");
}

#[test]
fn a_stopped_lane_stays_stopped_through_a_rebuild_until_connect() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = Host::of(pending_rebuild(dir.path()), Some(dir));
    h.connect(false);
    let old_socket = h.socket();
    let (old_push, _) = h.http("push");
    h.task("stop", json!({"kind":"connection","event":"stop"}));
    h.run();
    assert!(h.open.is_empty());
    h.task("rebuild", json!({"kind":"rebuild","discardPending":true}));
    let events = h.run();
    assert_eq!(h.completion(&events, "rebuild")["ok"], true);
    assert!(h.open.is_empty(), "stopped: nothing restarts: {:?}", h.open);
    assert!(sockets(&events).is_empty());
    h.inert(&old_socket, None, None, Some(&old_push));
    for event in ["wake", "resume"] {
        h.task(event, json!({"kind":"connection","event":event}));
        assert_eq!(h.run(), vec![done(event, Value::Null)]);
        assert!(h.open.is_empty(), "{event} restarts nothing: {:?}", h.open);
    }
    h.connect(false);
    assert_ne!(h.socket(), old_socket);
}

/// A rebuild under a running lane keeps it running: without another
/// `connect` the old replica's I/O is abandoned and a session for the carried
/// Scope opens on a new socket. Nothing the old socket or its catch-up had
/// delivered but not yet applied, or still delivers - before the new
/// handshake or while a new catch-up is in flight - reaches the fresh file,
/// and the lane then advances from the new acknowledged head
/// ([#162](https://github.com/zanminwang/axton/issues/162)).
#[test]
fn a_rebuild_keeps_a_running_lane_running_under_fresh_effects() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = Host::of(rebuildable(dir.path(), true, false), Some(dir));
    h.connect(false);
    let (old_push, _) = h.http("push");
    let old = h.socket();
    h.frame(&old, &ack(&[("book", 2)]));
    h.frame(&old, &page(4, 5, "e", "gap"));
    h.run();
    let (pull, _) = h.http("pull");
    // Admitted after the rebuild task: queued when it runs, never applied.
    h.task("rebuild", json!({"kind":"rebuild","discardPending":true}));
    let covering =
        json!({"cursors":{"book":{"from":2,"to":5,"head":5}},"changes":[late()]}).to_string();
    h.frame(&old, &covering);
    h.ok(&pull, &covering);
    let events = h.run();
    assert_eq!(h.completion(&events, "rebuild")["ok"], true);
    // The catch-up had answered; the socket and the push are abandoned.
    for id in [&old, &old_push] {
        assert!(cancelled(&events, id), "{id} abandoned: {events:?}");
    }
    assert_eq!(
        Vec::from_iter(h.client().desired_channels().unwrap()),
        ["book"],
        "the Scope is carried"
    );
    assert!(
        h.client().subscriptions().unwrap().is_empty(),
        "with no origin"
    );
    assert_eq!(h.text("late"), None, "nothing queued applied");
    assert_eq!(sockets(&events).len(), 1, "a new session, once: {events:?}");
    let new = h.socket();
    assert_ne!(new, old);
    assert_eq!(h.run(), Vec::<Value>::new());
    h.inert(&old, Some(&pull), None, Some(&old_push));

    // The new handshake commits the carried Scope's first boundary.
    h.frame(&new, &ack(&[("book", 5)]));
    h.run();
    assert_eq!(h.client().cursor("book").unwrap(), Some(5));
    h.frame(&new, &page(7, 8, "e", "gap"));
    h.run();
    let (fresh, body) = h.http("pull");
    assert_ne!(fresh, pull);
    assert_eq!(
        serde_json::from_str::<Value>(&body).unwrap()["cursors"],
        json!({"book":5})
    );
    // With a new catch-up in flight the old answers still match nothing.
    h.inert(&old, Some(&pull), None, Some(&old_push));
    let answer = json!({"cursors":{"book":{"from":5,"to":8,"head":8}},"changes":[{"model":"Entry","identity":{"id":"fresh"},"stamp":8,"state":{"text":"fresh","note":null,"due":"now"}}]});
    h.ok(&fresh, &answer.to_string());
    h.run();
    assert_eq!(h.client().cursor("book").unwrap(), Some(8));
    assert_eq!(h.text("fresh"), Some(json!("fresh")));
}

/// The same transition with a historical page in flight: its answer or
/// failure - queued before the rebuild, delivered before the new handshake or
/// while the fresh identity's own page is out - completes and fails nothing
/// on the fresh replica, and the fresh page is asked for anew
/// ([#162](https://github.com/zanminwang/axton/issues/162)).
#[test]
fn a_rebuild_fences_a_bootstrap_page_in_flight() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = Host::of(pending_rebuild(dir.path()), Some(dir));
    let subscription = h
        .client()
        .subscription_state("book")
        .unwrap()
        .unwrap()
        .subscription_id;
    h.connect(false);
    let (old_push, _) = h.http("push");
    let old = h.socket();
    h.frame(&old, &ack(&[("book", 7)]));
    h.run();
    let (load, _) = h.http("pull");
    h.task("rebuild", json!({"kind":"rebuild","discardPending":true}));
    h.ok(&load, &historical(json!([late()])));
    let events = h.run();
    assert_eq!(h.completion(&events, "rebuild")["ok"], true);
    assert!(cancelled(&events, &old), "{events:?}");
    let new = h.socket();
    assert_ne!(new, old);
    let carried =
        h.call("state", json!({"kind":"scopeState","scope":"book"}))["value"]["subscriptionId"]
            .as_u64()
            .unwrap();
    assert_ne!(carried, subscription, "a fresh identity");
    h.inert(&old, None, Some(&load), Some(&old_push));
    let untouched = h.client().bootstrap_state("book", carried).unwrap();
    assert_eq!(untouched.run, 0, "no run of the fresh identity was touched");
    assert_eq!(untouched.error, None);

    // The new session commits the origin; a new load asks for its first page.
    h.frame(&new, &ack(&[("book", 7)]));
    h.run();
    h.task(
        "load",
        json!({"kind":"scopeBootstrap","scope":"book","subscriptionId":carried}),
    );
    h.run();
    let (fresh, body) = h.http("pull");
    assert_ne!(fresh, load);
    assert_eq!(
        serde_json::from_str::<Value>(&body).unwrap()["mode"],
        "bootstrap"
    );
    let requested = h.client().bootstrap_state("book", carried).unwrap();
    assert_eq!(requested.run, 1);
    h.inert(&old, None, Some(&load), Some(&old_push));
    assert_eq!(
        h.client().bootstrap_state("book", carried).unwrap(),
        requested,
        "the old answer and failures neither complete nor fail the fresh run"
    );
    h.ok(&fresh, &historical(json!([])));
    h.run();
    let applied = h.client().bootstrap_state("book", carried).unwrap();
    assert_eq!(
        serde_json::to_value(&applied).unwrap(),
        json!({"scope":"book","subscriptionId":carried,"state":"catching_up","run":1,"cursor":7,"barrier":9,"error":null}),
        "the fresh page applies to the fresh run"
    );
}

/// A running lane with no Channel still resets on a rebuild, and then idles
/// until a registration wakes it
/// ([#162](https://github.com/zanminwang/axton/issues/162)).
#[test]
fn a_rebuild_under_a_running_lane_with_no_channel_resets_and_idles() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = Host::of(rebuildable(dir.path(), false, false), Some(dir));
    h.connect(false);
    assert!(
        h.outstanding("socket", None).is_empty(),
        "nothing subscribed"
    );
    let (push, _) = h.http("push");
    h.task("rebuild", json!({"kind":"rebuild","discardPending":true}));
    let events = h.run();
    assert_eq!(h.completion(&events, "rebuild")["ok"], true);
    assert!(cancelled(&events, &push));
    assert!(h.open.is_empty(), "still idle: {:?}", h.open);
    h.task(
        "subscribe",
        json!({"kind":"channel","channel":"book","subscribed":true}),
    );
    let events = h.run();
    assert_eq!(sockets(&events).len(), 1, "{events:?}");
}

/// A refused rebuild changes neither the replica nor the lane: nothing is
/// cancelled, and the old socket, catch-up and historical page stay the
/// lane's own - their answers apply as they would have
/// ([#162](https://github.com/zanminwang/axton/issues/162)).
#[test]
fn a_refused_rebuild_leaves_the_lane_and_its_effects_valid() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = Host::of(pending_rebuild(dir.path()), Some(dir));
    let subscription = h
        .client()
        .subscription_state("book")
        .unwrap()
        .unwrap()
        .subscription_id;
    h.connect(false);
    let socket = h.socket();
    h.frame(&socket, &ack(&[("book", 7)]));
    h.run();
    let (load, _) = h.http("pull");
    h.frame(
        &socket,
        &json!({"cursors":{"book":{"from":8,"to":9,"head":9}},"changes":[]}).to_string(),
    );
    h.run();
    let pull = h
        .outstanding("http", Some("pull"))
        .into_iter()
        .find(|(id, _)| *id != load)
        .unwrap()
        .0;
    let generation = h.client().generation();
    h.task("refused", json!({"kind":"rebuild"}));
    let events = h.run();
    assert_eq!(h.completion(&events, "refused")["ok"], false);
    assert!(
        !events.iter().any(|e| e["type"] == "cancelEffect"),
        "{events:?}"
    );
    assert_eq!(h.client().generation(), generation);
    let status = h.call("status", json!({"kind":"status"}))["value"].clone();
    assert!(
        status["schema"]["pending"].is_object(),
        "the old replica stays"
    );
    // The catch-up still answers.
    h.ok(
        &pull,
        &json!({"cursors":{"book":{"from":7,"to":9,"head":9}},"changes":[]}).to_string(),
    );
    h.run();
    assert_eq!(h.client().cursor("book").unwrap(), Some(9));
    // The historical page still answers, for the same identity.
    h.ok(&load, &historical(json!([])));
    h.run();
    let run = h.client().bootstrap_state("book", subscription).unwrap();
    assert_eq!(run.barrier, Some(9), "{run:?}");
    // The socket still streams.
    h.frame(
        &socket,
        &json!({"cursors":{"book":{"from":9,"to":10,"head":10}},"changes":[]}).to_string(),
    );
    h.run();
    assert_eq!(h.client().cursor("book").unwrap(), Some(10));
    assert_eq!(h.socket(), socket);
}

/// Arrival order is kept between foreground and inbound work: a frame that
/// arrives after the application asked to unsubscribe is not applied ahead of
/// that request, however the units interleave with an open transaction. The
/// SDK queues used to guarantee this by accident; the runtime guarantees it by
/// admission order.
#[test]
fn inbound_work_admitted_after_an_ordinary_task_runs_after_it() {
    let mut h = host();
    h.connect(false);
    let socket = h.streaming(0);
    h.frame(&socket, &page(0, 1, "live", "first"));
    h.run();
    assert_eq!(h.text("live"), Some(json!("first")));
    // A callback holds the writer; the application asks to drop the Scope
    // while it is open, and only then does an obsolete frame arrive.
    h.task("tx", json!({"kind":"transaction"}));
    let events = h.run();
    let callback = events
        .iter()
        .find(|e| e["type"] == "effect" && e["operation"]["kind"] == "callback")
        .unwrap();
    let (effect, transaction) = (
        callback["effectId"].as_str().unwrap().to_string(),
        callback["operation"]["transactionId"]
            .as_str()
            .unwrap()
            .to_string(),
    );
    h.task(
        "unsubscribe",
        json!({"kind":"channel","channel":"book","subscribed":false}),
    );
    h.task(
        "resubscribe",
        json!({"kind":"scopeSubscribe","scope":"book"}),
    );
    h.frame(&socket, &page(1, 2, "live", "obsolete"));
    assert!(
        h.run().is_empty(),
        "nothing runs while the callback holds the writer"
    );
    h.submit(
        json!({"type":"callbackResult","effectId":effect,"transactionId":transaction,"ok":true}),
    );
    let events = h.run();
    let unsubscribed = position(&events, |e| {
        e["type"] == "taskCompleted" && e["requestId"] == "unsubscribe"
    });
    let resubscribed = position(&events, |e| {
        e["type"] == "taskCompleted" && e["requestId"] == "resubscribe"
    });
    assert!(unsubscribed < resubscribed);
    // The membership change made the session stale, so the worker dropped the
    // frame instead of applying it: the record keeps its retained content and
    // a fresh session is asked for.
    assert_eq!(h.text("live"), Some(json!("first")));
    assert!(!sockets(&events).is_empty(), "{events:?}");
}

/// Socket frames admitted while a callback holds the writer go to the
/// Downlink worker at once: its bounded queue holds them, one past the bound
/// discards them all, and after the commit the lane recovers from the durable
/// cursor with one catch-up. Nothing is applied while the callback is open.
#[test]
fn frames_admitted_behind_an_open_callback_stay_within_the_worker_bound() {
    let mut h = host();
    h.connect(false);
    let socket = h.streaming(0);
    let (effect, transaction) = h.begin("tx");
    for i in 0..=QUEUED_FRAMES as u64 {
        h.frame(&socket, &page(i, i + 1, "e", &format!("frame {i}")));
        assert!(
            h.runtime.held_frames() <= QUEUED_FRAMES,
            "frame {i}: {} held",
            h.runtime.held_frames()
        );
    }
    assert!(
        h.run().is_empty(),
        "nothing runs while the callback holds the writer"
    );
    assert_eq!(h.client().cursor("book").unwrap(), Some(0));
    assert!(h.outstanding("http", Some("pull")).is_empty());
    h.submit(
        json!({"type":"callbackResult","effectId":effect,"transactionId":transaction,"ok":true}),
    );
    let events = h.run();
    assert_eq!(h.completion(&events, "tx")["ok"], true);
    assert_eq!(
        h.text("e"),
        None,
        "no held frame applied: the queue overflowed"
    );
    assert_eq!(h.client().cursor("book").unwrap(), Some(0));
    let (pull, body) = h.http("pull");
    assert_eq!(
        serde_json::from_str::<Value>(&body).unwrap()["cursors"],
        json!({"book":0}),
        "recovery starts from the durable cursor"
    );
    h.ok(&pull, &page(0, 65, "e", "recovered"));
    h.run();
    assert_eq!(h.text("e"), Some(json!("recovered")));
    assert_eq!(h.client().cursor("book").unwrap(), Some(65));
    assert!(h.outstanding("http", Some("pull")).is_empty());
}

// --- Observers ---------------------------------------------------------------

/// The snapshots one observer published, in order.
fn snapshots(events: &[Value], observer: &Value) -> Vec<Value> {
    events
        .iter()
        .filter(|e| e["type"] == "observerChanged" && &e["observerId"] == observer)
        .map(|e| e["snapshot"].clone())
        .collect()
}
fn status(initialization: &str, connection: &str, phase: &str) -> Value {
    json!({"active":connection != "stopped","initialization":initialization,"connection":connection,"bootstrap":{"phase":phase,"error":null}})
}
fn failed_with(id: &str, error: &str, details: Value) -> Value {
    json!({"type":"taskCompleted","requestId":id,"ok":false,"value":null,"error":error,"details":details})
}
fn completed(events: &[Value], id: &str) -> bool {
    events
        .iter()
        .any(|e| e["type"] == "taskCompleted" && e["requestId"] == id)
}
impl Host {
    /// Subscribe `scope` and answer its identity and observer.
    fn subscribe(&mut self, id: &str, scope: &str) -> (u64, Value, Vec<Value>) {
        self.task(id, json!({"kind":"scopeSubscribe","scope":scope}));
        let events = self.run();
        let value = self.completion(&events, id)["value"].clone();
        (
            value["state"]["subscriptionId"].as_u64().unwrap(),
            value["observerId"].clone(),
            events,
        )
    }
    fn bootstrap(&mut self, id: &str, subscription: u64) {
        self.task(
            id,
            json!({"kind":"scopeBootstrap","scope":"book","subscriptionId":subscription}),
        );
    }
}

/// The projection the SDK handles computed from lane signals, now computed
/// by the runtime: every transition of the connection, published once, after
/// the commit it describes; a removal ends the observer with one terminal
/// snapshot, and a recreated registration is `connecting` until a session
/// acknowledges it.
#[test]
fn subscription_status_follows_the_lanes_and_a_removal_closes_it_once() {
    let mut h = host();
    // Offline: the registration answers its observer, and the first snapshot
    // follows the completion.
    let (subscription, observer, events) = h.subscribe("subscribe", "book");
    assert!(observer.is_string(), "{events:?}");
    assert!(
        position(&events, |e| e["type"] == "taskCompleted")
            < position(&events, |e| e["type"] == "observerChanged")
    );
    assert_eq!(
        snapshots(&events, &observer),
        [
            json!({"kind":"subscription","scope":"book","subscriptionId":subscription,"status":status("pending","offline","not-requested")})
        ]
    );
    // The same identity answers the same observer and publishes nothing new.
    let (again, same, events) = h.subscribe("again", "book");
    assert_eq!((again, &same), (subscription, &observer));
    assert_eq!(statuses(&events), Vec::<Value>::new());

    // connect -> connecting (the socket is asked for) -> live on the handshake.
    h.task("connect", json!({"kind":"connect"}));
    let events = h.run();
    assert_eq!(sockets(&events).len(), 1);
    assert_eq!(connections(&events), ["connecting"]);
    assert!(
        position(&events, |e| e["requestId"] == "connect")
            < position(&events, |e| e["type"] == "observerChanged")
    );
    let socket = h.socket();
    h.answer(&socket, json!({"ok":true,"value":{"event":"opened"}}));
    assert_eq!(
        h.run(),
        Vec::<Value>::new(),
        "an open socket is not live yet"
    );
    h.frame(&socket, &ack(&[("book", 0)]));
    let events = h.until_event(|e| e["type"] == "observerChanged");
    assert_eq!(
        h.client().cursor("book").unwrap(),
        Some(0),
        "the boundary committed before the status"
    );
    assert_eq!(
        statuses(&events),
        [status("ready", "live", "not-requested")]
    );
    assert_eq!(h.run(), Vec::<Value>::new());

    // A gap asks for a catch-up: catching-up until it answers.
    h.frame(&socket, &page(5, 6, "e", "gap"));
    assert_eq!(connections(&h.run()), ["catching-up"]);
    let (pull, _) = h.http("pull");
    h.ok(&pull, &page(0, 6, "e", "caught up"));
    let events = h.run();
    assert_eq!(connections(&events), ["live"]);
    assert_eq!(h.text("e"), Some(json!("caught up")));

    // pause -> offline, resume -> connecting until the new session's handshake.
    h.task("pause", json!({"kind":"connection","event":"pause"}));
    assert_eq!(connections(&h.run()), ["offline"]);
    h.task("resume", json!({"kind":"connection","event":"resume"}));
    assert_eq!(connections(&h.run()), ["connecting"]);
    let socket = h.socket();
    h.frame(&socket, &ack(&[("book", 6)]));
    assert_eq!(connections(&h.run()), ["live"]);

    // The removal commits and ends the observer with one terminal snapshot.
    h.task(
        "unsubscribe",
        json!({"kind":"scopeUnsubscribe","scope":"book","subscriptionId":subscription}),
    );
    let events = h.run();
    assert_eq!(
        h.completion(&events, "unsubscribe")["value"],
        json!({"removed":true})
    );
    assert_eq!(
        snapshots(&events, &observer),
        [
            json!({"kind":"subscription","scope":"book","subscriptionId":subscription,"status":status("ready","stopped","not-requested"),"closed":true})
        ]
    );
    h.task("wake", json!({"kind":"connection","event":"wake"}));
    h.task(
        "twice",
        json!({"kind":"scopeUnsubscribe","scope":"book","subscriptionId":subscription}),
    );
    let events = h.run();
    assert_eq!(snapshots(&events, &observer), Vec::<Value>::new());
    assert_eq!(
        h.completion(&events, "twice")["value"],
        json!({"removed":false})
    );

    // A recreated registration is a new identity with a new observer: the old
    // session's handshake was not its own, so it is connecting until one is.
    let (recreated, renewed, events) = h.subscribe("recreate", "book");
    assert_ne!(recreated, subscription);
    assert_ne!(renewed, observer);
    assert_eq!(
        snapshots(&events, &renewed)[0]["status"],
        status("pending", "connecting", "not-requested")
    );
    let socket = h.socket();
    h.frame(&socket, &ack(&[("book", 6)]));
    let events = h.run();
    assert_eq!(
        snapshots(&events, &renewed).last().unwrap()["status"],
        status("ready", "live", "not-requested")
    );
    // Stopping the connection takes every live handle offline.
    h.task("stop", json!({"kind":"connection","event":"stop"}));
    assert_eq!(connections(&h.run()), ["offline"]);
}

/// `bootstrap()` waits in the runtime: registration answers only after the
/// completion of its run commits; calls during a run share it; a call after
/// completion answers at once with no effect; a completion committed before
/// the call runs is still its answer.
#[test]
fn bootstrap_waiters_answer_after_the_completion_commit_and_share_the_run() {
    let mut h = host();
    h.connect(false);
    let (subscription, observer, _) = h.subscribe("subscribe", "book");
    h.bootstrap("first", subscription);
    let events = h.run();
    assert!(!completed(&events, "first"), "{events:?}");
    assert_eq!(phases(&events), ["waiting-for-initialization"]);
    // A second call during the run shares it.
    h.bootstrap("second", subscription);
    let events = h.run();
    assert!(!completed(&events, "second"), "{events:?}");
    assert_eq!(statuses(&events), Vec::<Value>::new());

    // The handshake bounds the interval: one page, which fixes the barrier.
    let socket = h.socket();
    h.frame(&socket, &ack(&[("book", 7)]));
    let events = h.run();
    assert_eq!(phases(&events), ["loading"]);
    let (load, body) = h.http("pull");
    assert_eq!(
        serde_json::from_str::<Value>(&body).unwrap()["mode"],
        "bootstrap"
    );
    h.ok(&load, &json!({"mode":"bootstrap","channel":"book","from":0,"to":7,"until":7,"head":9,"records":[]}).to_string());
    let events = h.run();
    assert_eq!(phases(&events), ["catching-up"]);
    assert!(!completed(&events, "first") && !completed(&events, "second"));

    // Delivery reaches the barrier: the completion commits, then both calls
    // answer, then the status says so.
    h.frame(&socket, &page(7, 9, "e", "delivered"));
    let mut events = h.until("first");
    assert_eq!(
        h.client()
            .bootstrap_state("book", subscription)
            .unwrap()
            .state,
        BootstrapPhase::Complete,
        "the completion committed first"
    );
    events.extend(h.run());
    let first = position(&events, |e| *e == done("first", Value::Null));
    let second = position(&events, |e| *e == done("second", Value::Null));
    let published = position(&events, |e| e["observerId"] == observer);
    assert!(first < second && second < published);
    assert_eq!(phases(&events), ["complete"]);

    // A call after completion answers locally: no effect, no commit, no
    // snapshot.
    h.bootstrap("after", subscription);
    assert_eq!(h.run(), vec![done("after", Value::Null)]);

    // The completion of a new identity's run commits before its call runs:
    // the call still answers with it.
    let (other, _, _) = h.subscribe("other", "shelf");
    h.task(
        "load shelf",
        json!({"kind":"scopeBootstrap","scope":"shelf","subscriptionId":other}),
    );
    h.run();
    let socket = h.socket();
    h.frame(&socket, &ack(&[("book", 9), ("shelf", 3)]));
    h.run();
    let (load, _) = h.http("pull");
    h.ok(&load, &json!({"mode":"bootstrap","channel":"shelf","from":0,"to":3,"until":3,"head":3,"records":[]}).to_string());
    // The page answers - and completes the run - ahead of the late call.
    h.task(
        "late",
        json!({"kind":"scopeBootstrap","scope":"shelf","subscriptionId":other}),
    );
    let events = h.run();
    let early = position(&events, |e| *e == done("load shelf", Value::Null));
    let late = position(&events, |e| *e == done("late", Value::Null));
    assert!(early < late, "{events:?}");
    assert!(h.outstanding("http", Some("pull")).is_empty());
}

/// A failed run fails its waiters with its stored failure; an explicit call
/// retries with a new run; a waiter whose run was replaced without its outcome
/// being observed is superseded, never answered by the new run; a removal
/// fails the rest `subscription.closed`.
#[test]
fn a_failed_run_fails_its_waiters_and_a_retry_supersedes_an_unobserved_run() {
    let mut h = host();
    h.connect(false);
    let (subscription, observer, _) = h.subscribe("subscribe", "book");
    let socket = h.socket();
    h.frame(&socket, &ack(&[("book", 7)]));
    h.run();
    h.bootstrap("first", subscription);
    h.run();
    let (load, _) = h.http("pull");
    h.fail(&load, "HTTP 403", Some(403));
    let events = h.run();
    let first = h.completion(&events, "first").clone();
    assert_eq!(first["details"]["code"], "bootstrap.request_rejected");
    assert_eq!(first["error"], first["details"]["message"]);
    let failure = statuses(&events).last().unwrap()["bootstrap"].clone();
    assert_eq!(failure["phase"], "failed");
    assert_eq!(failure["error"], first["details"]);
    let reported = events
        .iter()
        .find(|e| e["type"] == "report" && e["diagnostic"]["kind"] == "error")
        .unwrap();
    assert_eq!(
        reported["diagnostic"],
        json!({"kind":"error","message":"HTTP 403","status":403}),
        "the failure reaches onError with its status"
    );

    // An explicit call retries: a new run, loading again.
    h.bootstrap("retry", subscription);
    let events = h.run();
    assert!(!completed(&events, "retry"));
    assert_eq!(phases(&events), ["loading"]);
    let run = h.client().bootstrap_state("book", subscription).unwrap();
    assert_eq!(run.run, 2);
    // That run fails where no transition is observed (another writer of the
    // same file); the next call starts run 3, and the waiter of run 2 can no
    // longer see its own outcome.
    assert!(
        h.client()
            .fail_bootstrap(
                "book",
                subscription,
                2,
                BootstrapError::new("bootstrap.protocol_invalid", "elsewhere", vec![]),
            )
            .unwrap()
    );
    h.bootstrap("again", subscription);
    let events = h.run();
    assert!(events.contains(&failed_with(
        "retry",
        "bootstrap.superseded",
        json!({"code":"bootstrap.superseded"})
    )));
    assert!(!completed(&events, "again"));
    assert_eq!(
        h.client()
            .bootstrap_state("book", subscription)
            .unwrap()
            .run,
        3
    );

    // The removal takes the load state with the row: the waiter fails
    // closed, then the observer ends.
    h.task(
        "unsubscribe",
        json!({"kind":"scopeUnsubscribe","scope":"book","subscriptionId":subscription}),
    );
    let events = h.run();
    let closed = position(&events, |e| {
        *e == failed_with(
            "again",
            "subscription.closed",
            json!({"code":"subscription.closed"}),
        )
    });
    assert!(closed < position(&events, |e| e["observerId"] == observer));
    assert_eq!(snapshots(&events, &observer)[0]["closed"], true);
    // A call that reaches the engine after the row went is refused the same
    // way, with the code the SDKs map.
    h.bootstrap("gone", subscription);
    let events = h.run();
    let gone = h.completion(&events, "gone");
    assert_eq!(gone["details"], json!({"code":"subscription.closed"}));
    assert!(
        gone["error"]
            .as_str()
            .unwrap()
            .starts_with("subscription.closed:")
    );
}

/// A rebuild replaces every registration: waiters fail `subscription.closed`
/// and every subscription observer ends; the watches stay and re-run against
/// the new replica.
#[test]
fn a_rebuild_closes_every_subscription_observer_and_keeps_the_watches() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = Host::of(pending_rebuild(dir.path()), Some(dir));
    let (subscription, observer, _) = h.subscribe("subscribe", "book");
    h.bootstrap("load", subscription);
    h.task("watch", json!({"kind":"watch","model":"Entry","spec":{}}));
    let events = h.run();
    let watch = h.completion(&events, "watch")["value"]["observerId"].clone();
    assert_eq!(
        snapshots(&events, &watch)[0]["rows"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    h.task("rebuild", json!({"kind":"rebuild","discardPending":true}));
    let events = h.run();
    assert!(events.contains(&failed_with(
        "load",
        "subscription.closed",
        json!({"code":"subscription.closed"})
    )));
    let ended = snapshots(&events, &observer);
    assert_eq!(ended.len(), 1);
    assert_eq!(ended[0]["closed"], true);
    assert_eq!(ended[0]["status"]["connection"], "stopped");
    assert_eq!(
        snapshots(&events, &watch),
        [json!({"kind":"watch","rows":[]})],
        "the unsent row stayed in the old file"
    );
    // The carried Scope is a fresh identity with its own observer.
    let (fresh, renewed, _) = h.subscribe("again", "book");
    assert_ne!(renewed, observer);
    assert_ne!(fresh, subscription);
}

/// Close fails every waiter `client_closed` and ends every observer before
/// `runtimeClosed`; the durable load is untouched.
#[test]
fn close_fails_waiters_and_ends_every_observer_before_runtime_closed() {
    let mut h = host();
    let (subscription, observer, _) = h.subscribe("subscribe", "book");
    h.bootstrap("load", subscription);
    h.task("watch", json!({"kind":"watch","model":"Entry"}));
    let events = h.run();
    let watch = h.completion(&events, "watch")["value"]["observerId"].clone();
    h.submit(json!({"type":"close"}));
    let events = h.run();
    let load = position(&events, |e| {
        *e == failed_with("load", "client_closed", json!({"code":"client_closed"}))
    });
    let subscription_end = position(&events, |e| e["observerId"] == observer);
    let watch_end = position(&events, |e| e["observerId"] == watch);
    assert!(load < subscription_end && subscription_end < watch_end);
    assert_eq!(
        events[subscription_end]["snapshot"],
        json!({"kind":"subscription","scope":"book","subscriptionId":subscription,"status":status("pending","stopped","waiting-for-initialization"),"closed":true})
    );
    assert_eq!(
        events[watch_end]["snapshot"],
        json!({"kind":"watch","rows":[],"closed":true})
    );
    assert_eq!(events.last().unwrap(), &json!({"type":"runtimeClosed"}));
    assert_eq!(
        h.client()
            .bootstrap_state("book", subscription)
            .unwrap()
            .state,
        BootstrapPhase::Requested,
        "the durable task stays for a reopen"
    );
}

/// The watch loop in the runtime: the initial snapshot follows the
/// registration, a commit re-runs every watch, an equal result is
/// suppressed, `unwatch` stops it, and an open callback transaction's writes
/// are invisible until they commit.
#[test]
fn watches_re_run_after_commits_and_publish_only_what_changed() {
    let mut h = host();
    h.task("seed", create("e", "first"));
    h.task(
        "all",
        json!({"kind":"watch","model":"Entry","spec":{"filter":{}}}),
    );
    h.task(
        "one",
        json!({"kind":"watch","model":"Entry","spec":{"filter":{"id":"e"}}}),
    );
    let events = h.run();
    let all = h.completion(&events, "all")["value"]["observerId"].clone();
    let one = h.completion(&events, "one")["value"]["observerId"].clone();
    assert_ne!(all, one);
    let row = |id: &str, text: &str| json!({"id":id,"text":text,"note":null});
    assert_eq!(
        snapshots(&events, &all),
        [json!({"kind":"watch","rows":[row("e", "first")]})]
    );
    assert!(
        position(&events, |e| e["requestId"] == "all")
            < position(&events, |e| e["observerId"] == all)
    );
    // Another record: the filtered watch re-runs to an equal result.
    h.task("other", create("f", "second"));
    let events = h.run();
    assert!(
        position(&events, |e| e["requestId"] == "other")
            < position(&events, |e| e["observerId"] == all)
    );
    assert_eq!(
        snapshots(&events, &all),
        [json!({"kind":"watch","rows":[row("e", "first"), row("f", "second")]})]
    );
    assert_eq!(snapshots(&events, &one), Vec::<Value>::new());
    // A commit that touches no Model re-runs both and publishes nothing.
    let generation = h.client().generation();
    h.task("scope", json!({"kind":"scopeSubscribe","scope":"book"}));
    let events = h.run();
    assert_ne!(h.client().generation(), generation);
    assert!(snapshots(&events, &all).is_empty() && snapshots(&events, &one).is_empty());

    // A callback's write is not visible until it commits.
    h.task("tx", json!({"kind":"transaction"}));
    let events = h.run();
    let callback = events
        .iter()
        .find(|e| e["type"] == "effect" && e["operation"]["kind"] == "callback")
        .unwrap()
        .clone();
    let transaction = callback["operation"]["transactionId"].clone();
    h.submit(json!({"type":"transactionCommand","requestId":"write","transactionId":transaction,"command":{"kind":"direct","operation":{"model":"Entry","op":"update","identity":{"id":"e"},"values":{"text":"inside"}}}}));
    let events = h.run();
    assert!(events.contains(&done("write", Value::Null)));
    assert!(
        !events.iter().any(|e| e["type"] == "observerChanged"),
        "{events:?}"
    );
    h.submit(json!({"type":"callbackResult","effectId":callback["effectId"],"transactionId":transaction,"ok":true}));
    let events = h.run();
    let committed = position(&events, |e| *e == done("tx", Value::Null));
    assert!(committed < position(&events, |e| e["observerId"] == one));
    assert_eq!(
        snapshots(&events, &one),
        [json!({"kind":"watch","rows":[row("e", "inside")]})]
    );

    // unwatch: nothing more for that observer, the other goes on.
    h.task("stop", json!({"kind":"unwatch","observerId":one}));
    h.task("edit", json!({"kind":"direct","operation":{"model":"Entry","op":"update","identity":{"id":"e"},"values":{"text":"after"}}}));
    let events = h.run();
    assert!(events.contains(&done("stop", Value::Null)));
    assert!(snapshots(&events, &one).is_empty());
    assert_eq!(snapshots(&events, &all).len(), 1);
    // A watch the query cannot run for is refused, and registers nothing.
    h.task("bad", json!({"kind":"watch","model":"Nope"}));
    let events = h.run();
    assert_eq!(h.completion(&events, "bad")["ok"], false);
    assert!(!events.iter().any(|e| e["type"] == "observerChanged"));
}

/// A response already in hand when the connection stops is known to have
/// executed: it is applied once the writer is free and the call completes
/// with its outcome. A close applies nothing, a response in hand included.
#[test]
fn a_response_in_hand_survives_stop_but_not_close() {
    let mut h = host();
    h.connect(false);
    h.task("seed", create("e", "seed"));
    h.run();
    h.task("call", rename("server"));
    h.run();
    let (http, body) = h.http("action");
    // The stop is admitted first, the response right after it.
    h.task("stop", json!({"kind":"connection","event":"stop"}));
    h.ok(&http, &renamed(&body, "server"));
    let events = h.run();
    let stopped = position(&events, |e| *e == done("stop", Value::Null));
    let call = position(&events, |e| e["requestId"] == "call");
    assert!(stopped < call, "{events:?}");
    assert_eq!(events[call]["ok"], true);
    assert_eq!(
        events[call]["value"]["outcome"]["status"], "succeeded",
        "{events:?}"
    );
    assert_eq!(h.text("e"), Some(json!("server")));

    h.connect(false);
    h.task("closing", rename("closing"));
    h.run();
    let (http, body) = h.http("action");
    h.ok(&http, &renamed(&body, "closing"));
    h.submit(json!({"type":"close"}));
    let events = h.run();
    assert!(events.contains(&failed_with(
        "closing",
        "action.unavailable",
        json!({"code":"action.unavailable"})
    )));
    assert_eq!(events.last().unwrap(), &json!({"type":"runtimeClosed"}));
    assert_eq!(h.text("e"), Some(json!("server")));
}

// --- Command contract --------------------------------------------------------

/// Whether a task failed, with its error.
fn refused(completion: &Value) -> Option<String> {
    (completion["ok"] == false).then(|| completion["error"].as_str().unwrap().to_string())
}
/// Open a callback transaction and answer its effect and transaction ids.
impl Host {
    fn begin(&mut self, id: &str) -> (Value, Value) {
        self.task(id, json!({"kind":"transaction"}));
        let events = self.run();
        let callback = events
            .iter()
            .find(|e| e["type"] == "effect" && e["operation"]["kind"] == "callback")
            .unwrap_or_else(|| panic!("a callback: {events:?}"));
        (
            callback["effectId"].clone(),
            callback["operation"]["transactionId"].clone(),
        )
    }
}

/// The Scope commands the SDK handles are built on: register durable intent,
/// read the committed state, and remove exactly the registration an identity
/// names. An uninitialized boundary travels as JSON `null`, never as zero;
/// a malformed identity is refused; Scope work is never transaction work
/// ([#150](https://github.com/zanminwang/axton/issues/150)).
#[test]
fn scope_commands_register_read_and_remove_one_identity() {
    let mut h = host();
    let state = json!({"kind":"scopeState","scope":"book"});
    assert_eq!(
        h.call("state", state.clone())["value"],
        Value::Null,
        "no row means unsubscribed"
    );
    let generation = h.client().generation();
    let registered = h.call("subscribe", json!({"kind":"scopeSubscribe","scope":"book"}));
    assert_eq!(
        registered["value"]["state"],
        json!({"scope":"book","subscriptionId":1,"startingCursor":null,"cursor":null}),
        "a fresh registration carries no boundary at all, not zero"
    );
    assert_ne!(
        h.client().generation(),
        generation,
        "the registration committed"
    );
    let generation = h.client().generation();
    let again = h.call("again", json!({"kind":"scopeSubscribe","scope":"book"}));
    assert_eq!(
        again["value"], registered["value"],
        "the same identity and observer"
    );
    assert_eq!(h.client().generation(), generation, "nothing was written");
    assert_eq!(
        h.call("read", state.clone())["value"],
        registered["value"]["state"]
    );
    // A name the wire refuses names no Scope: nothing is written for it.
    for blank in ["", " ", "\t\n"] {
        let subscribe = h.call("blank", json!({"kind":"scopeSubscribe","scope":blank}));
        assert!(refused(&subscribe).is_some(), "{blank:?}: {subscribe}");
        let channel = h.call(
            "blank channel",
            json!({"kind":"channel","channel":blank,"subscribed":true}),
        );
        assert!(refused(&channel).is_some(), "the same rule: {blank:?}");
    }
    assert_eq!(
        Vec::from_iter(h.client().desired_channels().unwrap()),
        ["book"],
        "nothing of a refused registration was written"
    );
    // The Channel command shares the ledger: the same row, the same identity.
    h.call(
        "channel",
        json!({"kind":"channel","channel":"book","subscribed":true}),
    );
    assert_eq!(h.call("read", state.clone())["value"]["subscriptionId"], 1);
    for malformed in [
        json!(null),
        json!("1"),
        json!(0),
        json!(-1),
        json!(1.5),
        json!(9007199254740992u64),
    ] {
        let removed = h.call(
            "malformed",
            json!({"kind":"scopeUnsubscribe","scope":"book","subscriptionId":malformed}),
        );
        assert!(refused(&removed).is_some(), "refused: {malformed}");
    }
    assert_eq!(
        h.call(
            "other",
            json!({"kind":"scopeUnsubscribe","scope":"book","subscriptionId":2})
        )["value"],
        json!({"removed":false}),
        "another identity's unsubscribe removes nothing"
    );
    assert_eq!(
        Vec::from_iter(h.client().desired_channels().unwrap()),
        ["book"]
    );
    let generation = h.client().generation();
    assert_eq!(
        h.call(
            "remove",
            json!({"kind":"scopeUnsubscribe","scope":"book","subscriptionId":1})
        )["value"],
        json!({"removed":true})
    );
    assert_ne!(h.client().generation(), generation);
    assert_eq!(h.call("read", state)["value"], Value::Null);
    // Identities are never recycled.
    assert_eq!(
        h.call("next", json!({"kind":"scopeSubscribe","scope":"book"}))["value"]["state"]["subscriptionId"],
        2
    );

    // Scope work is not transaction work: a callback cannot name it, and a
    // task waits outside the open transaction until it ends.
    let (effect, transaction) = h.begin("tx");
    for (i, command) in [
        json!({"kind":"scopeSubscribe","scope":"other"}),
        json!({"kind":"scopeState","scope":"other"}),
        json!({"kind":"scopeUnsubscribe","scope":"book","subscriptionId":2}),
    ]
    .into_iter()
    .enumerate()
    {
        let id = format!("inside{i}");
        h.submit(json!({"type":"transactionCommand","requestId":id,"transactionId":transaction,"command":command}));
        let events = h.run();
        assert!(
            refused(h.completion(&events, &id))
                .unwrap()
                .starts_with("unknown variant"),
            "{events:?}"
        );
    }
    h.task("outside", json!({"kind":"scopeSubscribe","scope":"other"}));
    assert_eq!(h.run(), Vec::<Value>::new(), "it waits for the callback");
    h.submit(
        json!({"type":"callbackResult","effectId":effect,"transactionId":transaction,"ok":true}),
    );
    let events = h.run();
    assert_eq!(
        h.completion(&events, "tx")["ok"],
        false,
        "the refusals poisoned it"
    );
    assert_eq!(h.completion(&events, "outside")["ok"], true);
}

/// The Bootstrap commands behind `bootstrap()`: registration is a local write
/// that needs no connection, the stored run is readable by the same identity,
/// the lane asks for the first page once an origin exists and asks once, and
/// an identity that is malformed or not this Scope's is refused with the
/// code the SDKs map ([#151](https://github.com/zanminwang/axton/issues/151)).
#[test]
fn bootstrap_commands_register_read_and_schedule_one_page() {
    let mut h = host();
    let (subscription, _, _) = h.subscribe("subscribe", "book");
    let stored = json!({"kind":"scopeBootstrapState","scope":"book","subscriptionId":subscription});
    let generation = h.client().generation();
    h.bootstrap("load", subscription);
    let events = h.run();
    assert!(!completed(&events, "load"), "it waits for its run");
    assert_ne!(
        h.client().generation(),
        generation,
        "the registration committed"
    );
    assert_eq!(
        h.call("stored", stored.clone())["value"],
        json!({"scope":"book","subscriptionId":subscription,"state":"requested","run":1,"cursor":0,"barrier":null,"error":null})
    );
    // Connected, before an origin: no interval, no page.
    h.connect(false);
    assert!(
        h.outstanding("http", Some("pull")).is_empty(),
        "{:?}",
        h.open
    );
    // The acknowledgement commits the origin, which bounds the interval.
    let socket = h.socket();
    h.frame(&socket, &ack(&[("book", 7)]));
    h.run();
    let (page, body) = h.http("pull");
    assert_eq!(
        serde_json::from_str::<Value>(&body).unwrap(),
        json!({"mode":"bootstrap","channel":"book","models":{"Entry":1},"after":0,"until":7})
    );
    // Nothing asks twice: a wake finds the request in flight.
    h.call("wake", json!({"kind":"connection","event":"wake"}));
    assert_eq!(
        h.outstanding("http", Some("pull")).len(),
        1,
        "one at a time"
    );
    h.ok(&page, &historical(json!([])));
    h.run();
    assert_eq!(
        h.call("stored", stored)["value"],
        json!({"scope":"book","subscriptionId":subscription,"state":"catching_up","run":1,"cursor":7,"barrier":9,"error":null}),
        "the terminal page fixed the barrier; delivery at 7 has not reached 9"
    );
    for malformed in [json!(null), json!("1"), json!(0), json!(-1), json!(1.5)] {
        let answer = h.call(
            "malformed",
            json!({"kind":"scopeBootstrap","scope":"book","subscriptionId":malformed}),
        );
        assert!(refused(&answer).is_some(), "refused: {malformed}");
    }
    for kind in ["scopeBootstrap", "scopeBootstrapState"] {
        let other = h.call(
            "other",
            json!({"kind":kind,"scope":"book","subscriptionId":99}),
        );
        // The stable prefix and code both SDKs raise `subscription.closed` by.
        let error = refused(&other).unwrap();
        assert!(error.starts_with("subscription.closed:"), "{error}");
        assert!(error.contains("is closed"), "{error}");
        assert_eq!(other["details"], json!({"code":"subscription.closed"}));
        let absent = h.call(
            "absent",
            json!({"kind":kind,"scope":"absent","subscriptionId":1}),
        );
        assert!(
            refused(&absent).is_some(),
            "{kind} of a Scope not subscribed"
        );
    }
    // Load work is not transaction work.
    let (effect, transaction) = h.begin("tx");
    h.submit(
        json!({"type":"transactionCommand","requestId":"inside","transactionId":transaction,
        "command":{"kind":"scopeBootstrap","scope":"book","subscriptionId":subscription}}),
    );
    let events = h.run();
    assert!(refused(h.completion(&events, "inside")).is_some());
    h.submit(
        json!({"type":"callbackResult","effectId":effect,"transactionId":transaction,"ok":false}),
    );
    h.run();
}

/// Closing the client is not unsubscribing: the rows and their boundaries
/// survive a close and a reopen, and only `scopeUnsubscribe` removes one,
/// durably.
#[test]
fn closing_a_client_keeps_the_subscriptions_unsubscribe_removes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let open = || {
        let runtime = ClientRuntime::open_at(
            &path,
            Schema::from_value(schema_value()).unwrap(),
            factory(),
            false,
        )
        .unwrap();
        Host::of(runtime, None)
    };
    let close = |mut h: Host| {
        h.submit(json!({"type":"close"}));
        assert_eq!(h.run().last().unwrap(), &json!({"type":"runtimeClosed"}));
    };
    let state = json!({"kind":"scopeState","scope":"book"});
    let mut h = open();
    let (subscription, _, _) = h.subscribe("subscribe", "book");
    h.connect(false);
    let socket = h.socket();
    h.frame(&socket, &ack(&[("book", 7)]));
    h.run();
    let initialized = h.call("state", state.clone())["value"].clone();
    assert_eq!(
        initialized,
        json!({"scope":"book","subscriptionId":subscription,"startingCursor":7,"cursor":7}),
        "the acknowledged head is the committed boundary"
    );
    close(h);
    let mut h = open();
    assert_eq!(
        h.call("state", state.clone())["value"],
        initialized,
        "closing the client deleted nothing"
    );
    assert_eq!(
        h.call(
            "remove",
            json!({"kind":"scopeUnsubscribe","scope":"book","subscriptionId":subscription})
        )["value"],
        json!({"removed":true})
    );
    close(h);
    let mut h = open();
    assert_eq!(
        h.call("state", state)["value"],
        Value::Null,
        "an unsubscribe is durable"
    );
    close(h);
}

/// The store policy travels beside the business arguments on both routes,
/// never inside them, and a malformed one is refused on both.
#[test]
fn action_store_option_travels_beside_args_on_both_routes() {
    let mut h = host_with(
        json!({"enums":[],"models":[],"actions":[{"name":"Ping","version":1,"inputs":[{"kind":"value","name":"store","type":{"kind":"scalar","name":"string"},"nullable":false}],"outputs":[]}]}),
    );
    let ping = |store: Value, kind: &str| json!({"kind":kind,"name":"Ping","version":1,"args":{"store":"biz"},"store":store});
    for bad in [json!({"missing":false}), json!("no"), json!(null)] {
        for kind in ["submitAction", "invoke"] {
            let answer = h.call("bad", ping(bad.clone(), kind));
            assert!(refused(&answer).is_some(), "{kind} store {bad}: {answer}");
        }
    }
    // The durable route: the frozen batch carries the policy beside the args.
    assert_eq!(
        h.call("submit", ping(json!(false), "submitAction"))["ok"],
        true
    );
    let frozen = h.call("freeze", json!({"kind":"freeze"}))["value"].clone();
    let frozen: Value = serde_json::from_str(frozen.as_str().unwrap()).unwrap();
    assert_eq!(frozen["mutations"][0]["store"], false);
    assert_eq!(frozen["mutations"][0]["args"], json!({"store":"biz"}));
    // The direct route: the request carries it the same way.
    h.connect(false);
    h.task("call", ping(json!(false), "invoke"));
    h.run();
    let (_, body) = h.http("action");
    let body: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(body["call"]["store"], false);
    assert_eq!(body["call"]["args"], json!({"store":"biz"}));
}

/// Query once through the runtime: the exact request carries no cache
/// control; malformed options, a Mutation and a callback are refused; and an
/// invalidation - never transaction work - makes the next call fetch.
#[test]
fn query_once_refuses_bad_options_and_an_invalidation_makes_the_next_call_fetch() {
    let mut h = host();
    h.connect(false);
    h.task("first", echo(json!({"once":true})));
    h.run();
    let (http, body) = h.http("action");
    let request: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(request["call"]["name"], "Echo");
    assert_eq!(request["call"]["callId"], call_id(&body).as_str());
    assert!(
        request["call"].get("once").is_none() && request["call"].get("refresh").is_none(),
        "no cache control reaches the wire: {request}"
    );
    h.ok(&http, &echoed(&body, "hi"));
    h.run();
    for bad in [
        echo(json!({"once":true,"refresh":"yes"})),
        echo(json!({"once":true,"store":7})),
    ] {
        let answer = h.call("bad", bad.clone());
        assert!(refused(&answer).is_some(), "{bad}: {answer}");
    }
    let mutation = h.call(
        "mutation",
        json!({"kind":"invoke","name":"Ping","version":1,"args":{},"once":true}),
    );
    assert!(refused(&mutation).is_some(), "a Mutation has no once route");
    assert!(h.outstanding("http", Some("action")).is_empty());
    // A hit, then the invalidation - waiting outside an open callback, which
    // cannot name it - and the next call fetches.
    assert_eq!(h.call("hit", echo(json!({"once":true})))["ok"], true);
    assert!(h.outstanding("http", Some("action")).is_empty());
    let invalidate =
        json!({"kind":"invalidateQueryOnce","name":"Echo","version":1,"args":{"label":"hi"}});
    let (effect, transaction) = h.begin("tx");
    h.submit(json!({"type":"transactionCommand","requestId":"inside","transactionId":transaction,"command":invalidate}));
    h.task("invalidate", invalidate.clone());
    let events = h.run();
    assert!(refused(h.completion(&events, "inside")).is_some());
    assert!(!completed(&events, "invalidate"), "{events:?}");
    h.submit(
        json!({"type":"callbackResult","effectId":effect,"transactionId":transaction,"ok":false}),
    );
    let events = h.run();
    assert_eq!(
        h.completion(&events, "invalidate"),
        &done("invalidate", Value::Null)
    );
    h.task("after", echo(json!({"once":true})));
    h.run();
    h.http("action");
}

/// A direct call never joins the durable queue and a response moves no
/// delivery position.
#[test]
fn a_direct_call_stays_off_the_queue_and_moves_no_cursor() {
    let mut h = host();
    h.connect(false);
    h.task(
        "call",
        json!({"kind":"invoke","name":"Ping","version":1,"args":{}}),
    );
    h.run();
    let (http, body) = h.http("action");
    assert_eq!(h.client().pending_count().unwrap(), 0, "off the queue");
    let response = json!({"completion":{"callId":call_id(&body),"outcome":{"status":"succeeded","result":null}},"records":[]});
    h.ok(&http, &response.to_string());
    let events = h.run();
    assert_eq!(
        h.completion(&events, "call"),
        &done(
            "call",
            json!({"outcome":{"status":"succeeded","result":null}})
        )
    );
    assert_eq!(h.client().pending_count().unwrap(), 0);
    assert!(h.client().subscriptions().unwrap().is_empty(), "no cursor");
    assert!(
        h.outstanding("http", Some("push")).is_empty(),
        "nothing to push"
    );
}

/// The push lane keeps the receipt's authority and leaves reads to the
/// stream: the receipt completes the push without a pull, and the stream
/// later carrying the same authority only advances the cursor.
#[test]
fn a_receipt_applies_its_authority_and_leaves_reads_to_the_stream() {
    let mut h = host();
    h.connect(false);
    let socket = h.streaming(0);
    h.task(
        "create",
        json!({"kind":"enqueue","mutation":{"name":"Create","operations":[{"model":"Entry","op":"create","identity":{"id":"e"},"values":{"text":"B","note":null}}]}}),
    );
    h.run();
    let (push, body) = h.http("push");
    let client_id = h.client().client_id().to_string();
    let batch: Value = serde_json::from_str(&body).unwrap();
    let receipt = json!({"clientId":client_id,"batchSequence":batch["batchSequence"],"rejections":[],
        "records":[{"model":"Entry","identity":{"id":"e"},"stamp":1,"state":{"text":"normalized","note":null}}]});
    h.ok(&push, &receipt.to_string());
    h.run();
    assert_eq!(h.client().pending_count().unwrap(), 0);
    assert_eq!(h.text("e"), Some(json!("normalized")));
    assert!(
        h.outstanding("http", None).is_empty(),
        "no pull: {:?}",
        h.open
    );
    h.frame(&socket, &page(0, 1, "e", "normalized"));
    h.run();
    assert_eq!(h.client().cursor("book").unwrap(), Some(1));
    assert_eq!(h.text("e"), Some(json!("normalized")));
}

/// A dropped socket backs its own lane off while the push lane goes on; no
/// command drives either lane by hand.
#[test]
fn a_dropped_socket_backs_its_lane_off_while_the_push_lane_goes_on() {
    let mut h = host();
    h.connect(false);
    let socket = h.streaming(0);
    h.answer(&socket, json!({"ok":true,"value":{"event":"closed"}}));
    let events = h.run();
    assert_eq!(errors(&events), ["socket closed"]);
    assert_eq!(connections(&events), ["connecting"]);
    h.one("timer", None);
    assert!(h.outstanding("socket", None).is_empty());
    // The push lane is not waiting on that backoff.
    h.task(
        "submit",
        json!({"kind":"submitAction","name":"Ping","version":1,"args":{}}),
    );
    h.run();
    h.http("push");
    // The commit's wake lets the worker look again: it still backs off.
    assert!(h.outstanding("socket", None).is_empty(), "{:?}", h.open);
    let (timer, _) = h.one("timer", None);
    for bad in [
        json!({"kind":"downlink","event":"next"}),
        json!({"kind":"connection","event":"unknown"}),
        json!({"kind":"connection"}),
    ] {
        let answer = h.call("bad", bad.clone());
        assert!(refused(&answer).is_some(), "{bad}");
    }
    h.fire(&timer);
    h.run();
    h.socket();
}

/// Delivery policy is the same streamed or answered: a covered page changes
/// nothing, an overlap applies once, a catch-up in flight leaves the push in
/// flight alone, and an answer that does not match its request ends the
/// session with that reason.
#[test]
fn covered_and_overlapping_pages_apply_once_beside_a_push_in_flight() {
    let mut h = host();
    h.connect(false);
    let socket = h.streaming(0);
    let first = page(0, 1, "e", "first");
    h.frame(&socket, &first);
    h.run();
    assert_eq!(h.text("e"), Some(json!("first")));
    let generation = h.client().generation();
    h.frame(&socket, &first);
    h.run();
    assert_eq!(h.client().generation(), generation, "covered");
    // A streamed overlap applies once, from the boundary.
    h.frame(&socket, &page(0, 2, "e", "incoming overlap"));
    h.run();
    assert_eq!(h.client().cursor("book").unwrap(), Some(2));
    assert_eq!(h.text("e"), Some(json!("incoming overlap")));
    // A gap asks for a catch-up while a push is out: neither disturbs the other.
    h.frame(&socket, &page(4, 5, "e", "gap"));
    h.task(
        "edit",
        json!({"kind":"enqueue","mutation":{"name":"Edit","operations":[{"model":"Entry","op":"update","identity":{"id":"e"},"values":{"text":"B"}}]}}),
    );
    h.run();
    let (pull, _) = h.http("pull");
    let push = h.http("push");
    h.ok(
        &pull,
        &json!({"cursors":{"book":{"from":2,"to":5,"head":5}},"changes":[]}).to_string(),
    );
    h.run();
    assert_eq!(h.client().cursor("book").unwrap(), Some(5));
    assert_eq!(h.http("push"), push, "the catch-up never touches the push");
    // An answer for another request ends the session with the reason.
    h.frame(&socket, &page(7, 8, "e", "gap"));
    h.run();
    let (pull, _) = h.http("pull");
    h.ok(
        &pull,
        &json!({"cursors":{"other":{"from":0,"to":2,"head":2}},"changes":[]}).to_string(),
    );
    let events = h.run();
    assert!(
        errors(&events).contains(&"response does not match pull request".to_string()),
        "{events:?}"
    );
    assert!(cancelled(&events, &socket));
    assert_eq!(h.client().cursor("book").unwrap(), Some(5));
}

/// A Bootstrap row the ledger cannot decode reaches the application once per
/// unchanged defect: once when the lane starts, not again on a wake or a
/// handshake, and once more for the next connection
/// ([#163](https://github.com/zanminwang/axton/issues/163)).
#[test]
fn a_ledger_issue_reaches_the_application_once_per_unchanged_defect() {
    let mut h = host();
    let path = h._dir.as_ref().unwrap().path().join("db");
    let issues = |events: &[Value]| -> Vec<String> {
        errors(events)
            .into_iter()
            .filter(|e| e.starts_with("bootstrap ledger bad: "))
            .collect()
    };
    let (subscription, _, _) = h.subscribe("subscribe", "bad");
    h.connect(false);
    let socket = h.socket();
    h.frame(&socket, &ack(&[("bad", 100)]));
    h.run();
    h.call("stop", json!({"kind":"connection","event":"stop"}));
    h.task(
        "load",
        json!({"kind":"scopeBootstrap","scope":"bad","subscriptionId":subscription}),
    );
    h.run();
    // Damaged behind the runtime's back, as a foreign writer would.
    let mut raw = SqliteStore::open(&path).unwrap();
    assert_eq!(
        raw.execute(
            "UPDATE axton_subscription SET bootstrap_cursor='x' WHERE channel=?",
            &[json!("bad")]
        )
        .unwrap(),
        1
    );
    h.task("connect", json!({"kind":"connect"}));
    let events = h.run();
    let reported = issues(&events);
    assert_eq!(reported.len(), 1, "{events:?}");
    assert!(reported[0].contains("cannot be decoded"), "{reported:?}");
    h.task("wake", json!({"kind":"connection","event":"wake"}));
    assert_eq!(issues(&h.run()), Vec::<String>::new(), "once");
    let socket = h.socket();
    h.frame(&socket, &ack(&[("bad", 100)]));
    assert_eq!(issues(&h.run()), Vec::<String>::new(), "still once");
    h.call("stop", json!({"kind":"connection","event":"stop"}));
    h.task("again", json!({"kind":"connect"}));
    assert_eq!(
        issues(&h.run()).len(),
        1,
        "the next connection hears it once"
    );
}

/// A commit made through the application callback is a commit like any
/// other: what it queued is pushed without another wake, and a Scope it
/// registered is followed. The push lane is idle when the callback opens, so
/// nothing but the commit itself can start the send.
#[test]
fn a_committed_callback_transaction_wakes_the_lanes() {
    let mut h = host();
    h.connect(false);
    h.run();
    assert!(h.outstanding("http", Some("push")).is_empty(), "idle lane");
    h.task("tx", json!({"kind":"transaction"}));
    let events = h.run();
    let callback = events
        .iter()
        .find(|e| e["type"] == "effect" && e["operation"]["kind"] == "callback")
        .unwrap();
    let (effect, transaction) = (
        callback["effectId"].as_str().unwrap().to_string(),
        callback["operation"]["transactionId"]
            .as_str()
            .unwrap()
            .to_string(),
    );
    h.submit(json!({"type":"transactionCommand","requestId":"enqueue","transactionId":transaction,
        "command":{"kind":"enqueue","mutation":{"name":"Edit","operations":[{"model":"Entry","op":"create","identity":{"id":"e"},"values":{"text":"queued","note":null}}]}}}));
    h.submit(
        json!({"type":"transactionCommand","requestId":"subscribe","transactionId":transaction,
        "command":{"kind":"channel","channel":"book","subscribed":true}}),
    );
    let events = h.run();
    assert_eq!(h.completion(&events, "enqueue")["ok"], true);
    assert_eq!(h.completion(&events, "subscribe")["ok"], true);
    assert!(
        h.outstanding("http", Some("push")).is_empty(),
        "nothing sends before the commit"
    );
    h.submit(
        json!({"type":"callbackResult","effectId":effect,"transactionId":transaction,"ok":true}),
    );
    let events = h.run();
    assert_eq!(h.completion(&events, "tx")["ok"], true);
    assert_eq!(h.client().pending_count().unwrap(), 1);
    let (_, body) = h.http("push");
    assert!(
        body.contains("\"queued\""),
        "the queued mutation is sent: {body}"
    );
    assert!(
        !h.outstanding("socket", None).is_empty(),
        "the registered Scope is followed"
    );
}
