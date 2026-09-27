//! The native actor carrier: one runtime per client on its own thread, an
//! outbox drained on wake, and detach before the wake sink is released
//! ([#134](https://github.com/zanminwang/axton/issues/134)). The wake channel
//! is the only barrier; a timeout only turns a lost wake into a failure.
use axton_binding::actor::{self, WakeSink};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::time::Duration;

const LOST_WAKE: Duration = Duration::from_secs(20);

fn schema() -> Value {
    serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap()
}
fn channel_sink() -> (WakeSink, Receiver<u64>) {
    let (sender, receiver) = mpsc::channel();
    (
        Box::new(move |runtime| {
            let _ = sender.send(runtime);
        }),
        receiver,
    )
}
/// One opened runtime and the wakes it sends.
struct Carrier {
    id: u64,
    wakes: Receiver<u64>,
    seen: Vec<Value>,
}
impl Carrier {
    fn open(path: &std::path::Path) -> (Self, Value) {
        let (sink, wakes) = channel_sink();
        let id = actor::open(
            json!({"type":"open","requestId":"open","path":path,"schema":schema()}),
            sink,
        )
        .unwrap();
        assert!(id > 0);
        let mut carrier = Self {
            id,
            wakes,
            seen: vec![],
        };
        let opened = carrier.until(|e| e["requestId"] == "open");
        (carrier, opened)
    }
    fn submit(&self, message: Value) {
        actor::submit(self.id, message).unwrap();
    }
    fn task(&self, id: &str, command: Value) {
        self.submit(json!({"type":"task","requestId":id,"command":command}));
    }
    /// Drain on every wake until an event matches; answer it. Events drained
    /// on the way are kept in `seen`.
    fn until(&mut self, matches: impl Fn(&Value) -> bool) -> Value {
        loop {
            if let Some(found) = self.seen.iter().position(&matches) {
                return self.seen.remove(found);
            }
            let woken = self.wakes.recv_timeout(LOST_WAKE).expect("a wake was lost");
            assert_eq!(woken, self.id);
            self.seen.extend(actor::drain(self.id));
        }
    }
    fn completed(&mut self, id: &str) -> Value {
        self.until(|e| e["type"] == "taskCompleted" && e["requestId"] == id)
    }
    /// Every event in order, up to and including the first that matches.
    fn through(&mut self, matches: impl Fn(&Value) -> bool) -> Vec<Value> {
        loop {
            if let Some(found) = self.seen.iter().position(&matches) {
                return self.seen.drain(..=found).collect();
            }
            let woken = self.wakes.recv_timeout(LOST_WAKE).expect("a wake was lost");
            assert_eq!(woken, self.id);
            self.seen.extend(actor::drain(self.id));
        }
    }
}
fn create(id: &str) -> Value {
    json!({"kind":"direct","operation":{"model":"Entry","op":"create","identity":{"id":id},"values":{"text":"hi"}}})
}
fn read(id: &str) -> Value {
    json!({"kind":"read","key":{"model":"Entry","identity":{"id":id}}})
}

#[test]
fn open_answers_on_the_wake_and_a_failed_open_closes_the_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let (mut carrier, opened) = Carrier::open(&dir.path().join("db"));
    assert_eq!(opened["ok"], true);
    assert!(!opened["value"]["clientId"].as_str().unwrap().is_empty());
    assert_eq!(opened["value"]["schema"]["rebuilt"], false);
    carrier.task("1", create("e"));
    assert_eq!(carrier.completed("1")["ok"], true);
    carrier.submit(json!({"type":"close"}));
    carrier.until(|e| e["type"] == "runtimeClosed");
    // The end is announced only once the store is released: the carrier can
    // delete or reopen the files the moment it sees `runtimeClosed`.
    for suffix in ["-wal", "-shm"] {
        let sidecar = dir.path().join(format!("db{suffix}"));
        assert!(
            !sidecar.exists(),
            "{} still held after runtimeClosed",
            sidecar.display()
        );
    }
    assert_eq!(
        actor::submit(carrier.id, json!({"type":"close"})),
        Err("client_closed".to_string())
    );
    actor::detach(carrier.id);
    assert!(actor::drain(carrier.id).is_empty());

    let (sink, wakes) = channel_sink();
    let missing = dir.path().join("missing").join("sub").join("db");
    let id = actor::open(
        json!({"type":"open","requestId":"7","path":missing,"schema":schema()}),
        sink,
    )
    .unwrap();
    let mut failed = Carrier {
        id,
        wakes,
        seen: vec![],
    };
    let answer = failed.completed("7");
    assert_eq!(answer["ok"], false);
    assert!(!answer["error"].as_str().unwrap().is_empty());
    failed.until(|e| e["type"] == "runtimeClosed");
    actor::detach(id);
    assert_eq!(
        actor::submit(id, json!({"type":"close"})),
        Err("client_closed".to_string())
    );

    // Admission refuses what it cannot route; nothing is spawned for it.
    let (sink, _) = channel_sink();
    assert!(actor::open(json!({"type":"open"}), sink).is_err());
    // A malformed envelope is a protocol report on the runtime, not a refusal.
    let (mut carrier, _) = Carrier::open(&dir.path().join("other"));
    carrier.submit(json!({"type":"nope"}));
    let report = carrier.until(|e| e["type"] == "report");
    assert_eq!(report["diagnostic"]["kind"], "protocol");
    // A routable envelope whose command does not decode completes its
    // request with the decoding error: no waiter is left behind.
    carrier.task("bad", json!({"kind":"nope"}));
    let refused = carrier.completed("bad");
    assert_eq!(refused["ok"], false);
    assert!(
        refused["error"]
            .as_str()
            .unwrap()
            .starts_with("unknown variant `nope`"),
        "{refused}"
    );
    actor::detach(carrier.id);
    assert_eq!(
        actor::submit(carrier.id, json!({"type":"close"})),
        Err("client_closed".to_string())
    );
}

#[test]
fn every_completion_is_delivered_exactly_once_across_repeated_drains() {
    let dir = tempfile::tempdir().unwrap();
    let (carrier, _) = Carrier::open(&dir.path().join("db"));
    const TASKS: usize = 300;
    for i in 0..TASKS {
        let command = if i % 3 == 0 {
            create(&format!("e{i}"))
        } else {
            read("e0")
        };
        carrier.task(&i.to_string(), command);
    }
    let mut counts = BTreeMap::<String, usize>::new();
    // Drain on every wake while the actor keeps publishing; stop when every
    // request has been seen. A lost wake would block here until the timeout.
    while counts.len() < TASKS {
        let woken = carrier
            .wakes
            .recv_timeout(LOST_WAKE)
            .expect("a wake was lost");
        assert_eq!(woken, carrier.id);
        for event in actor::drain(carrier.id) {
            if event["type"] == "taskCompleted" {
                assert_eq!(event["ok"], true, "{event}");
                *counts
                    .entry(event["requestId"].as_str().unwrap().to_string())
                    .or_default() += 1;
            }
        }
    }
    assert!(counts.values().all(|n| *n == 1), "{counts:?}");
    // Nothing more arrives for them.
    assert!(
        actor::drain(carrier.id)
            .iter()
            .all(|e| e["type"] != "taskCompleted")
    );
    actor::detach(carrier.id);
}

#[test]
fn a_client_parked_in_a_callback_does_not_hold_up_another_client() {
    let dir = tempfile::tempdir().unwrap();
    let (mut a, _) = Carrier::open(&dir.path().join("a"));
    a.task("1", json!({"kind":"transaction"}));
    let effect = a.until(|e| e["type"] == "effect");
    let transaction = effect["operation"]["transactionId"].clone();
    a.submit(json!({"type":"transactionCommand","requestId":"2","transactionId":transaction,"command":create("e")}));
    assert_eq!(a.completed("2")["ok"], true);

    // A holds its transaction open; B opens, writes, reads and closes.
    let (mut b, _) = Carrier::open(&dir.path().join("b"));
    b.task("1", create("e"));
    b.task("2", read("e"));
    assert_eq!(b.completed("1")["ok"], true);
    assert_eq!(b.completed("2")["value"]["text"], "hi");
    b.submit(json!({"type":"close"}));
    b.until(|e| e["type"] == "runtimeClosed");
    actor::detach(b.id);

    // A's callback finishes only now and commits.
    a.task("3", read("e"));
    a.submit(json!({"type":"callbackResult","effectId":effect["effectId"],"transactionId":transaction,"ok":true}));
    assert_eq!(a.completed("1")["ok"], true);
    assert_eq!(a.completed("3")["value"]["text"], "hi");
    actor::detach(a.id);
}

#[test]
fn detach_stops_wakes_before_it_returns_and_closes_a_live_runtime() {
    let dir = tempfile::tempdir().unwrap();
    // Close while a callback is open, then detach.
    let (mut carrier, _) = Carrier::open(&dir.path().join("db"));
    carrier.task("1", json!({"kind":"transaction"}));
    let effect = carrier.until(|e| e["type"] == "effect");
    carrier.task("2", read("e"));
    carrier.submit(json!({"type":"close"}));
    assert_eq!(
        carrier.until(|e| e["type"] == "cancelEffect")["effectId"],
        effect["effectId"]
    );
    assert_eq!(carrier.completed("1")["error"], "client_closed");
    assert_eq!(carrier.completed("2")["error"], "client_closed");
    carrier.until(|e| e["type"] == "runtimeClosed");
    actor::detach(carrier.id);
    drop(carrier.wakes);
    assert!(actor::submit(carrier.id, json!({"type":"close"})).is_err());

    // Detach a runtime that is still live and parked in a callback: after
    // detach returns, the sink is never called again, whatever the actor
    // still publishes while it closes.
    let detached = Arc::new(AtomicBool::new(false));
    let late = Arc::new(AtomicUsize::new(0));
    let (sender, wakes) = mpsc::channel();
    let sink: WakeSink = {
        let (detached, late) = (detached.clone(), late.clone());
        Box::new(move |runtime| {
            if detached.load(Ordering::SeqCst) {
                late.fetch_add(1, Ordering::SeqCst);
            }
            let _ = sender.send(runtime);
        })
    };
    let id = actor::open(
        json!({"type":"open","requestId":"open","path":dir.path().join("live"),"schema":schema()}),
        sink,
    )
    .unwrap();
    let mut live = Carrier {
        id,
        wakes,
        seen: vec![],
    };
    live.completed("open");
    live.task("1", json!({"kind":"transaction"}));
    let effect = live.until(|e| e["type"] == "effect");
    let tx = effect["operation"]["transactionId"].clone();
    // The open transaction holds a write the detach must roll back.
    live.submit(json!({"type":"transactionCommand","requestId":"w","transactionId":tx,"command":create("e")}));
    assert_eq!(live.completed("w")["ok"], true);
    for i in 0..50 {
        live.task(&format!("r{i}"), read("e"));
    }
    actor::detach(id);
    detached.store(true, Ordering::SeqCst);
    drop(live.wakes);
    assert_eq!(
        actor::submit(id, json!({"type":"close"})),
        Err("client_closed".to_string())
    );
    assert!(actor::drain(id).is_empty());
    // The detached actor ends on its own: it rolled back and released the
    // file, so no sidecar is left and a new runtime on the same path sees
    // nothing of the uncommitted write.
    assert!(
        actor::wait_closed(id, LOST_WAKE),
        "the detached actor never ended"
    );
    for suffix in ["-wal", "-shm"] {
        let sidecar = dir.path().join(format!("live{suffix}"));
        assert!(!sidecar.exists(), "{} still held", sidecar.display());
    }
    let (mut again, _) = Carrier::open(&dir.path().join("live"));
    again.task("1", read("e"));
    assert!(again.completed("1")["value"].is_null());
    actor::detach(again.id);
    assert_eq!(late.load(Ordering::SeqCst), 0);
}

#[test]
fn transaction_isolation_and_closed_handles_match_the_session_contract() {
    let dir = tempfile::tempdir().unwrap();
    let (mut c, _) = Carrier::open(&dir.path().join("db"));
    c.task("1", json!({"kind":"transaction"}));
    let effect = c.until(|e| e["type"] == "effect");
    let tx = effect["operation"]["transactionId"].clone();
    c.submit(json!({"type":"transactionCommand","requestId":"2","transactionId":tx,"command":create("e")}));
    c.submit(
        json!({"type":"transactionCommand","requestId":"3","transactionId":tx,"command":read("e")}),
    );
    assert_eq!(c.completed("2")["ok"], true);
    assert_eq!(c.completed("3")["value"]["text"], "hi");
    // Uncommitted work publishes nothing beyond its commands' answers.
    assert!(c.seen.is_empty(), "{:?}", c.seen);
    c.submit(json!({"type":"callbackResult","effectId":effect["effectId"],"transactionId":tx,"ok":false,"error":"rolled back"}));
    assert_eq!(c.completed("1")["error"], "rolled back");
    c.task("4", read("e"));
    assert!(c.completed("4")["value"].is_null());
    c.submit(json!({"type":"close"}));
    c.until(|e| e["type"] == "runtimeClosed");
    assert_eq!(
        actor::submit(
            c.id,
            json!({"type":"task","requestId":"5","command":read("e")})
        ),
        Err("client_closed".to_string())
    );
    actor::detach(c.id);
    // A transaction command outside its transaction is closed, and ordinary
    // tasks wait outside an open one.
    let (mut c, _) = Carrier::open(&dir.path().join("scoped"));
    // No transaction yet: a transaction command is closed, a plain read served.
    c.submit(json!({"type":"transactionCommand","requestId":"1","transactionId":"tx1","command":read("e")}));
    assert_eq!(c.completed("1")["error"], "transaction_closed");
    c.task("2", read("e"));
    assert!(c.completed("2")["value"].is_null());
    c.task("3", json!({"kind":"transaction"}));
    let effect = c.until(|e| e["type"] == "effect");
    let tx = effect["operation"]["transactionId"].clone();
    c.submit(json!({"type":"transactionCommand","requestId":"4","transactionId":tx,"command":create("e")}));
    assert_eq!(c.completed("4")["ok"], true);
    // While the callback owns the transaction, lane commands wait.
    for (i, kind) in ["freeze", "status", "tasks"].iter().enumerate() {
        c.task(&format!("sync{i}"), json!({"kind":kind}));
    }
    c.submit(
        json!({"type":"transactionCommand","requestId":"5","transactionId":tx,"command":read("e")}),
    );
    assert_eq!(c.completed("5")["value"]["text"], "hi");
    assert!(c.seen.iter().all(|e| e["type"] != "taskCompleted"));
    c.submit(
        json!({"type":"callbackResult","effectId":effect["effectId"],"transactionId":tx,"ok":true}),
    );
    assert_eq!(c.completed("3")["ok"], true);
    assert_eq!(c.completed("sync0")["ok"], true);
    assert_eq!(c.completed("sync1")["value"]["pending"], 0);
    assert_eq!(c.completed("sync2")["ok"], true);
    // After the commit the same token is closed again.
    c.submit(
        json!({"type":"transactionCommand","requestId":"6","transactionId":tx,"command":read("e")}),
    );
    assert_eq!(c.completed("6")["error"], "transaction_closed");
    c.task("7", read("e"));
    assert_eq!(c.completed("7")["value"]["text"], "hi");
    c.submit(json!({"type":"close"}));
    c.until(|e| e["type"] == "runtimeClosed");
    actor::detach(c.id);
    assert_eq!(
        actor::submit(c.id, json!({"type":"close"})),
        Err("client_closed".to_string())
    );
}

#[test]
fn a_client_waiting_on_the_network_holds_no_writer_and_no_other_client_waits() {
    let dir = tempfile::tempdir().unwrap();
    let mut with_action = schema();
    with_action["actions"] = json!([{"name":"Rename","version":1,"inputs":[{"kind":"model","name":"entry","model":"Entry","operation":"update","cardinality":"single"}],"outputs":[]}]);
    let (sink, wakes) = channel_sink();
    let id = actor::open(
        json!({"type":"open","requestId":"open","path":dir.path().join("a"),"schema":with_action}),
        sink,
    )
    .unwrap();
    let mut a = Carrier {
        id,
        wakes,
        seen: vec![],
    };
    assert_eq!(a.completed("open")["ok"], true);
    a.task("connect", json!({"kind":"connect"}));
    assert_eq!(a.completed("connect")["ok"], true);
    a.task(
        "call",
        json!({"kind":"invoke","name":"Rename","version":1,"args":{"entry":{"id":"e","text":"server"}}}),
    );
    let request = a.until(|e| e["type"] == "effect" && e["operation"]["route"] == "action");
    let body: Value = serde_json::from_str(request["operation"]["body"].as_str().unwrap()).unwrap();

    // While A's request is out, A itself writes and reads, and B does too.
    a.task("local", create("x"));
    a.task("read", read("x"));
    assert_eq!(a.completed("local")["ok"], true);
    assert_eq!(a.completed("read")["value"]["text"], "hi");
    let (mut b, _) = Carrier::open(&dir.path().join("b"));
    for i in 0..20 {
        b.task(&format!("w{i}"), create(&format!("e{i}")));
        b.task(&format!("r{i}"), read(&format!("e{i}")));
    }
    for i in 0..20 {
        assert_eq!(b.completed(&format!("w{i}"))["ok"], true);
        assert_eq!(b.completed(&format!("r{i}"))["value"]["text"], "hi");
    }
    b.submit(json!({"type":"close"}));
    b.until(|e| e["type"] == "runtimeClosed");
    actor::detach(b.id);
    assert!(
        a.seen
            .iter()
            .all(|e| !(e["type"] == "taskCompleted" && e["requestId"] == "call")),
        "the call waits for its response"
    );

    // The response arrives: the call succeeds once its apply committed.
    let response = json!({"completion":{"callId":body["call"]["callId"],"outcome":{"status":"succeeded","result":null}},"records":[{"model":"Entry","identity":{"id":"e"},"stamp":1,"state":{"text":"server","note":null}}]});
    a.submit(json!({"type":"effectResult","effectId":request["effectId"],"outcome":{"ok":true,"value":{"status":200,"body":response.to_string()}}}));
    let done = a.completed("call");
    assert_eq!(
        done["value"],
        json!({"outcome":{"status":"succeeded","result":null}})
    );
    a.task("after", read("e"));
    assert_eq!(a.completed("after")["value"]["text"], "server");
    a.submit(json!({"type":"close"}));
    a.until(|e| e["type"] == "runtimeClosed");
    actor::detach(a.id);
}

/// Observer snapshots cross the carrier in the order the runtime published
/// them: after the commit they describe and after the task that registered
/// the observer, and the terminal ones before `runtimeClosed`.
#[test]
fn observer_snapshots_arrive_after_the_commit_they_describe() {
    let dir = tempfile::tempdir().unwrap();
    let (mut carrier, _) = Carrier::open(&dir.path().join("db"));
    carrier.task(
        "watch",
        json!({"kind":"watch","model":"Entry","spec":{"filter":{}}}),
    );
    let events = carrier.through(|e| e["type"] == "observerChanged");
    let kinds: Vec<&str> = events.iter().map(|e| e["type"].as_str().unwrap()).collect();
    assert_eq!(kinds, ["taskCompleted", "observerChanged"], "{events:?}");
    let watch = events[0]["value"]["observerId"].clone();
    assert_eq!(events[1]["observerId"], watch);
    assert_eq!(events[1]["snapshot"], json!({"kind":"watch","rows":[]}));

    carrier.task("1", create("e"));
    let events = carrier.through(|e| e["type"] == "observerChanged");
    let kinds: Vec<&str> = events.iter().map(|e| e["type"].as_str().unwrap()).collect();
    assert_eq!(kinds, ["taskCompleted", "observerChanged"], "{events:?}");
    assert_eq!(
        events[1]["snapshot"]["rows"],
        json!([{"id":"e","text":"hi","note":null}])
    );

    carrier.task("subscribe", json!({"kind":"scopeSubscribe","scope":"book"}));
    let events = carrier.through(|e| e["type"] == "observerChanged");
    let kinds: Vec<&str> = events.iter().map(|e| e["type"].as_str().unwrap()).collect();
    assert_eq!(
        kinds,
        ["taskCompleted", "observerChanged"],
        "the registration commits; the watch re-runs to an equal result: {events:?}"
    );
    let subscription = events[0]["value"]["observerId"].clone();
    assert_eq!(events[1]["observerId"], subscription);
    assert_eq!(events[1]["snapshot"]["status"]["connection"], "offline");

    carrier.submit(json!({"type":"close"}));
    let events = carrier.through(|e| e["type"] == "runtimeClosed");
    let ended: Vec<&Value> = events
        .iter()
        .filter(|e| e["type"] == "observerChanged")
        .map(|e| &e["observerId"])
        .collect();
    assert_eq!(ended, [&subscription, &watch]);
    assert!(
        events
            .iter()
            .filter(|e| e["type"] == "observerChanged")
            .all(|e| e["snapshot"]["closed"] == true)
    );
    actor::detach(carrier.id);
}

/// A carrier instance lost without detaching - a Node env torn down by
/// `worker.terminate()` - detaches every runtime it still held: its teardown
/// hook is installed once per instance, a runtime it detached itself is
/// forgotten, and runtimes of other instances are untouched.
#[test]
fn a_lost_carrier_detaches_every_runtime_it_still_held() {
    let dir = tempfile::tempdir().unwrap();
    let owners = actor::Owners::new();
    let installs = AtomicUsize::new(0);
    let install = || {
        installs.fetch_add(1, Ordering::SeqCst);
        Ok::<(), String>(())
    };
    let mut opened = BTreeMap::new();
    for (owner, name) in [(1, "a"), (1, "b"), (1, "c"), (2, "other")] {
        owners.watch(owner, install).unwrap();
        let (carrier, _) = Carrier::open(&dir.path().join(name));
        owners.opened(owner, carrier.id);
        opened.insert(name, carrier);
    }
    assert_eq!(installs.load(Ordering::SeqCst), 2, "one hook per instance");
    // A refused installation leaves the instance untracked; the next open
    // tries again.
    assert_eq!(owners.watch(3, || Err("no hook")), Err("no hook"));
    owners.watch(3, install).unwrap();
    assert_eq!(installs.load(Ordering::SeqCst), 3);

    // The carrier detached one runtime itself: it is forgotten.
    let b = opened["b"].id;
    actor::detach(b);
    owners.detached(b);

    // The first instance's teardown hook runs.
    let mut lost = owners.lost(1);
    lost.sort_unstable();
    let mut held = vec![opened["a"].id, opened["c"].id];
    held.sort_unstable();
    assert_eq!(lost, held);
    for id in held {
        assert_eq!(
            actor::submit(id, json!({"type":"close"})),
            Err("client_closed".to_string())
        );
        assert!(actor::wait_closed(id, LOST_WAKE), "{id} never ended");
    }
    assert!(owners.lost(1).is_empty(), "lost once");
    // The other instance's runtime still serves.
    let other = opened.get_mut("other").unwrap();
    other.task("1", read("e"));
    assert!(other.completed("1")["value"].is_null());
    // An instance at a reused address is a new one: its hook is installed.
    owners.watch(1, install).unwrap();
    assert_eq!(installs.load(Ordering::SeqCst), 4);
    assert_eq!(owners.lost(2), vec![other.id]);
    assert!(actor::wait_closed(other.id, LOST_WAKE));
}

/// What a Dart `NativeFinalizer` relies on: detach from any thread, several
/// times at once and again later, and for an id never opened, is safe and
/// closes the runtime exactly once.
#[test]
fn ffi_detach_is_safe_from_any_thread_repeatedly_and_for_unknown_ids() {
    let dir = tempfile::tempdir().unwrap();
    let (carrier, _) = Carrier::open(&dir.path().join("db"));
    let id = carrier.id;
    let finalizers: Vec<_> = (0..4)
        .map(|_| {
            std::thread::spawn(move || {
                axton_binding::ffi::detach(id);
                axton_binding::ffi::detach(id);
            })
        })
        .collect();
    for finalizer in finalizers {
        finalizer.join().unwrap();
    }
    axton_binding::ffi::detach(id);
    axton_binding::ffi::detach(u64::MAX);
    assert_eq!(
        actor::submit(id, json!({"type":"close"})),
        Err("client_closed".to_string())
    );
    assert!(actor::wait_closed(id, LOST_WAKE), "the actor never ended");
}
