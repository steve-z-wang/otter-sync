//! The runtime runs the prerequisite handlers registered at open whenever a
//! task becomes pending, and retries a transient failure with its own backoff
//! ([#185](https://github.com/zanminwang/axton/issues/185)). The test is the
//! host over a real SQLite store: it answers every effect with a fixed clock
//! that a fired timer advances. No sleeps, no threads.
use axton_client::runtime::{ClientRuntime, Input};
use axton_client::*;
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::path::Path;

const ENTROPY: u64 = 200;

/// `Entry.note` is `@requires(RemoteBlob(key: self))`; `Scan` is declared
/// but gets no handler in these tests.
fn schema() -> Schema {
    let mut schema: Value =
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap();
    schema["prerequisites"] = json!([
        {"name":"RemoteBlob","fields":[{"name":"key","type":"String"}]},
        {"name":"Scan","fields":[{"name":"key","type":"String"}]}
    ]);
    schema["requirements"] = json!([
        {"model":"Entry","field":"note","name":"RemoteBlob","arguments":{"key":"self"}},
        {"model":"Entry","field":"text","name":"Scan","arguments":{"key":"self"}}
    ]);
    schema["actions"] = json!([
      {"name":"Edit","version":1,"inputs":[{"kind":"model","name":"entry","model":"Entry","operation":"update","cardinality":"single","fields":["note"]}],"outputs":[]},
      {"name":"ScanEntry","version":1,"inputs":[{"kind":"model","name":"entry","model":"Entry","operation":"update","cardinality":"single","fields":["text"]}],"outputs":[]}
    ]);
    Schema::from_value(schema).unwrap()
}
fn runtime(path: &Path, handlers: &[&str]) -> ClientRuntime<SqliteStore> {
    ClientRuntime::new(
        Client::open05(SqliteStore::open(path).unwrap(), schema(), "User:u").unwrap(),
    )
    .register_prerequisite_handlers(handlers.iter().map(|h| h.to_string()).collect())
    .unwrap()
}
fn blob(key: &str) -> String {
    json!({"arguments":{"key":key},"name":"RemoteBlob"}).to_string()
}

struct Host {
    runtime: ClientRuntime<SqliteStore>,
    now: u64,
    open: BTreeMap<String, Value>,
    dir: tempfile::TempDir,
}
impl Host {
    fn new(handlers: &[&str]) -> Self {
        let dir = tempfile::tempdir().unwrap();
        Self {
            runtime: runtime(&dir.path().join("db"), handlers),
            now: 1_000,
            open: BTreeMap::new(),
            dir,
        }
    }
    /// Drop the runtime as a crash would and open the same file again.
    fn reopen(self, handlers: &[&str]) -> Self {
        let Host {
            runtime: old, dir, ..
        } = self;
        drop(old);
        Self {
            runtime: runtime(&dir.path().join("db"), handlers),
            now: 1_000,
            open: BTreeMap::new(),
            dir,
        }
    }
    fn submit(&mut self, input: Value) {
        let input: Input = serde_json::from_value(input).unwrap();
        self.runtime.receive(input, self.now, ENTROPY).unwrap();
    }
    fn task(&mut self, id: &str, command: Value) {
        self.submit(json!({"type":"task","requestId":id,"command":command}));
    }
    fn run(&mut self) -> Vec<Value> {
        let mut events = self.take();
        while self.runtime.step(self.now, ENTROPY) {
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
    fn outstanding(&self, kind: &str) -> Vec<(String, Value)> {
        self.open
            .iter()
            .filter(|(_, op)| op["kind"] == kind)
            .map(|(id, op)| (id.clone(), op.clone()))
            .collect()
    }
    fn one(&self, kind: &str) -> (String, Value) {
        let found = self.outstanding(kind);
        assert_eq!(found.len(), 1, "one {kind}: {:?}", self.open);
        found.into_iter().next().unwrap()
    }
    fn answer(&mut self, id: &str, outcome: Value) {
        self.open.remove(id);
        self.submit(json!({"type":"effectResult","effectId":id,"outcome":outcome}));
    }
    fn succeed(&mut self, id: &str) {
        self.answer(id, json!({"ok":true}));
    }
    fn fail(&mut self, id: &str, message: &str, retry: bool) {
        let mut error = json!({"message":message});
        if retry {
            error["retry"] = json!(true);
        }
        self.answer(id, json!({"ok":false,"error":error}));
    }
    fn fire(&mut self, timer: &str) {
        let millis = self.open[timer]["millis"].as_u64().unwrap();
        self.now += millis;
        self.succeed(timer);
    }
    /// Seed `e` and queue a Mutation that sets its note to `key`.
    fn attach(&mut self, key: &str) {
        self.task(
            "seed",
            json!({"kind":"direct","operation":{"model":"Entry","op":"create","identity":{"id":"e"},"values":{"text":"A"}}}),
        );
        self.edit("attach", key);
    }
    fn edit(&mut self, id: &str, key: &str) {
        self.task(
            id,
            json!({"kind":"submitAction","name":"Edit","version":1,"args":{"entry":{"id":"e","note":key}}}),
        );
    }
    fn tasks(&mut self) -> Vec<Value> {
        self.task("tasks", json!({"kind":"tasks"}));
        let events = self.run();
        let completion = events
            .iter()
            .find(|e| e["type"] == "taskCompleted" && e["requestId"] == "tasks")
            .unwrap();
        completion["value"].as_array().unwrap().clone()
    }
}

#[test]
fn a_commit_that_queues_a_task_runs_its_handler_with_no_host_call() {
    let mut h = Host::new(&["RemoteBlob"]);
    h.run();
    assert!(
        h.outstanding("prerequisite").is_empty(),
        "nothing pending yet"
    );
    h.attach("asset");
    h.run();
    let (effect, op) = h.one("prerequisite");
    assert_eq!(
        op,
        json!({"kind":"prerequisite","key":blob("asset"),"name":"RemoteBlob","arguments":{"key":"asset"}})
    );
    let generation = h.runtime.client().generation();
    h.succeed(&effect);
    h.run();
    assert_ne!(h.runtime.client().generation(), generation, "resolved");
    assert!(h.tasks().is_empty());
    assert!(h.outstanding("prerequisite").is_empty());
    // A later commit that queues another task runs it too, one at a time.
    h.edit("second", "b");
    h.edit("third", "c");
    h.run();
    let (effect, op) = h.one("prerequisite");
    assert_eq!(op["arguments"], json!({"key":"b"}));
    h.succeed(&effect);
    h.run();
    let (_, op) = h.one("prerequisite");
    assert_eq!(op["arguments"], json!({"key":"c"}));
}

#[test]
fn a_task_pending_at_restart_runs_after_reopen() {
    let mut h = Host::new(&["RemoteBlob"]);
    h.attach("asset");
    h.run();
    let (_, op) = h.one("prerequisite");
    assert_eq!(op["key"], blob("asset"));
    // The process ends while the handler runs; nothing was recorded.
    let mut h = h.reopen(&["RemoteBlob"]);
    h.run();
    let (effect, op) = h.one("prerequisite");
    assert_eq!(op["key"], blob("asset"));
    h.succeed(&effect);
    h.run();
    assert!(h.tasks().is_empty());
}

#[test]
fn a_transient_failure_retries_with_growing_backoff() {
    let mut h = Host::new(&["RemoteBlob"]);
    h.attach("asset");
    h.run();
    let mut delays = vec![];
    for _ in 0..3 {
        let (effect, _) = h.one("prerequisite");
        h.fail(&effect, "offline", true);
        h.run();
        assert!(h.outstanding("prerequisite").is_empty(), "backing off");
        let (timer, op) = h.one("timer");
        delays.push(op["millis"].as_u64().unwrap());
        // Still pending, not failed, while it backs off.
        let tasks = h.tasks();
        assert_eq!(tasks[0]["state"], "pending", "{tasks:?}");
        h.fire(&timer);
        h.run();
    }
    // 1 s doubling per attempt; this entropy is the jitter's midpoint.
    assert_eq!(delays, vec![1_000, 2_000, 4_000]);
    let (effect, _) = h.one("prerequisite");
    h.succeed(&effect);
    h.run();
    assert!(h.tasks().is_empty());
    assert!(h.outstanding("timer").is_empty());
}

#[test]
fn a_reset_clears_the_backoff_and_runs_at_once() {
    let mut h = Host::new(&["RemoteBlob"]);
    h.attach("asset");
    h.run();
    let (effect, _) = h.one("prerequisite");
    h.fail(&effect, "offline", true);
    h.run();
    let (timer, _) = h.one("timer");
    h.task(
        "reset",
        json!({"kind":"readiness","key":blob("asset"),"state":"pending"}),
    );
    h.run();
    let (effect, _) = h.one("prerequisite");
    assert!(!h.open.contains_key(&timer), "the backoff timer is dropped");
    // The count starts over: the next transient failure waits 1 s again.
    h.fail(&effect, "offline", true);
    h.run();
    assert_eq!(h.one("timer").1["millis"], 1_000);
}

#[test]
fn a_task_backing_off_holds_back_no_other_task() {
    let mut h = Host::new(&["RemoteBlob"]);
    h.attach("a");
    h.edit("second", "b");
    h.run();
    let (effect, op) = h.one("prerequisite");
    assert_eq!(op["arguments"], json!({"key":"a"}));
    h.fail(&effect, "offline", true);
    h.run();
    let (effect, op) = h.one("prerequisite");
    assert_eq!(op["arguments"], json!({"key":"b"}));
    h.succeed(&effect);
    h.run();
    assert!(h.outstanding("prerequisite").is_empty(), "a waits");
    let (timer, _) = h.one("timer");
    h.fire(&timer);
    h.run();
    assert_eq!(h.one("prerequisite").1["arguments"], json!({"key":"a"}));
}

#[test]
fn a_terminal_failure_stays_failed_and_visible() {
    let mut h = Host::new(&["RemoteBlob"]);
    h.attach("asset");
    h.run();
    let (effect, _) = h.one("prerequisite");
    h.fail(&effect, "file is gone", false);
    h.run();
    assert!(h.outstanding("prerequisite").is_empty());
    assert!(h.outstanding("timer").is_empty(), "no retry is scheduled");
    let tasks = h.tasks();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0]["state"], "failed");
    assert_eq!(tasks[0]["error"], "file is gone");
    // A later commit does not run it again; only a reset does.
    h.edit("unrelated", "asset");
    h.run();
    assert!(h.outstanding("prerequisite").is_empty());
    h.task(
        "retry",
        json!({"kind":"readiness","key":blob("asset"),"state":"pending"}),
    );
    h.run();
    let (effect, _) = h.one("prerequisite");
    h.succeed(&effect);
    h.run();
    assert!(h.tasks().is_empty());
}

#[test]
fn close_during_a_run_cancels_it_and_ignores_a_late_answer() {
    let mut h = Host::new(&["RemoteBlob"]);
    h.attach("asset");
    h.run();
    let (effect, _) = h.one("prerequisite");
    h.submit(json!({"type":"close"}));
    let events = h.run();
    assert!(
        events.contains(&json!({"type":"cancelEffect","effectId":effect})),
        "{events:?}"
    );
    assert_eq!(events.last().unwrap(), &json!({"type":"runtimeClosed"}));
    assert!(
        !events
            .iter()
            .any(|e| e["type"] == "report" || e["ok"] == false),
        "close reports nothing: {events:?}"
    );
    let late = serde_json::from_value::<Input>(
        json!({"type":"effectResult","effectId":effect,"outcome":{"ok":true}}),
    )
    .unwrap();
    assert!(h.runtime.receive(late, h.now, ENTROPY).is_err());
}

#[test]
fn a_prerequisite_without_a_handler_is_left_for_the_app() {
    let mut h = Host::new(&["RemoteBlob"]);
    // `text` requires Scan, which has no handler: its task stays pending.
    h.task(
        "seed",
        json!({"kind":"direct","operation":{"model":"Entry","op":"create","identity":{"id":"e"},"values":{"text":"A"}}}),
    );
    h.task(
        "scan",
        json!({"kind":"submitAction","name":"ScanEntry","version":1,"args":{"entry":{"id":"e","text":"scanned"}}}),
    );
    h.run();
    assert!(h.outstanding("prerequisite").is_empty());
    let tasks = h.tasks();
    assert_eq!(tasks.len(), 1);
    assert_eq!(
        (tasks[0]["name"].clone(), tasks[0]["state"].clone()),
        (json!("Scan"), json!("pending"))
    );
    // Without any handler nothing runs at all.
    let mut bare = Host::new(&[]);
    bare.attach("asset");
    bare.run();
    assert!(bare.outstanding("prerequisite").is_empty());
}

#[test]
fn only_declared_prerequisites_can_be_registered_once_at_open() {
    let dir = tempfile::tempdir().unwrap();
    let open = || {
        ClientRuntime::new(
            Client::open05(
                SqliteStore::open(dir.path().join("db")).unwrap(),
                schema(),
                "User:u",
            )
            .unwrap(),
        )
    };
    let refused = open()
        .register_prerequisite_handlers(vec!["Upload".into()])
        .err()
        .unwrap();
    assert!(
        refused
            .to_string()
            .contains("invalid prerequisite handler Upload"),
        "{refused}"
    );
    let duplicate = open()
        .register_prerequisite_handlers(vec!["RemoteBlob".into(), "RemoteBlob".into()])
        .err()
        .unwrap();
    assert!(duplicate.to_string().contains("RemoteBlob"), "{duplicate}");
    assert!(
        open()
            .register_prerequisite_handlers(vec!["RemoteBlob".into(), "Scan".into()])
            .is_ok()
    );
}

#[test]
fn explicit_reset_cancels_the_run_and_rescans_the_store() {
    let mut h = Host::new(&[]);
    h.attach("old");
    h.run();
    let Host {
        runtime: old_runtime,
        dir,
        ..
    } = h;
    drop(old_runtime);
    let path = dir.path().join("db");
    let runtime = runtime(&path, &["RemoteBlob"]);
    let mut h = Host {
        runtime,
        now: 1_000,
        open: BTreeMap::new(),
        dir,
    };
    h.run();
    let (effect, op) = h.one("prerequisite");
    assert_eq!(op["key"], blob("old"));
    h.task(
        "rebuild",
        json!({"kind":"resetStore","discardPending":true}),
    );
    let events = h.run();
    assert!(
        events.contains(&json!({"type":"cancelEffect","effectId":effect})),
        "{events:?}"
    );
    // The fresh replica has no task; a new one runs as usual.
    assert!(h.outstanding("prerequisite").is_empty());
    h.task(
        "seed",
        json!({"kind":"direct","operation":{"model":"Entry","op":"create","identity":{"id":"e"},"values":{"text":"A"}}}),
    );
    h.edit("attach", "fresh");
    h.run();
    assert_eq!(h.one("prerequisite").1["arguments"], json!({"key":"fresh"}));
}

#[test]
fn readiness_outcomes_release_only_ready_canonical_batches() {
    let mut h = Host::new(&["RemoteBlob"]);
    h.task(
        "connect",
        json!({"kind":"connect","directTimeoutMs":1000,"refreshAuth":false}),
    );
    h.attach("asset");
    h.run();
    assert!(
        h.runtime.client().freeze_batch05().unwrap().is_none(),
        "blocked by pending task"
    );
    let (effect, _) = h.one("prerequisite");
    h.fail(&effect, "disk full", false);
    h.run();
    assert!(
        h.runtime.client().freeze_batch05().unwrap().is_none(),
        "terminal failure remains blocked"
    );
    h.task(
        "retry",
        json!({"kind":"readiness","key":blob("asset"),"state":"pending"}),
    );
    h.run();
    let (effect, _) = h.one("prerequisite");
    h.succeed(&effect);
    h.run();
    assert!(
        h.runtime.client().freeze_batch05().unwrap().is_some(),
        "success released the canonical batch"
    );
}

#[test]
fn the_host_timer_is_the_clock_of_record_for_a_retry() {
    let mut h = Host::new(&["RemoteBlob"]);
    h.attach("asset");
    h.run();
    let (effect, _) = h.one("prerequisite");
    h.fail(&effect, "offline", true);
    h.run();
    let (timer, _) = h.one("timer");
    // The timer fires before the runtime's clock reaches the due time.
    h.succeed(&timer);
    h.run();
    assert!(h.outstanding("timer").is_empty(), "not waited for again");
    h.one("prerequisite");
}
