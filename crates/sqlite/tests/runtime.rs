//! The Rust-owned client runtime over a real SQLite store, driven step by step
//! with fixed clock and entropy facts: task correlation, transaction ownership
//! across the application callback, savepoint scopes, close and commit
//! failure ([#134](https://github.com/zanminwang/axton/issues/134)).
mod common;
use axton_client::runtime::{BridgeError, ClientRuntime, Event, Input};
use axton_client::*;
use axton_sqlite::SqliteStore;
use common::{key, schema};
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

const NOW: u64 = 1_000;
const ENTROPY: u64 = 7;

struct Harness<S: ClientStore + 'static> {
    runtime: ClientRuntime<S>,
    _dir: tempfile::TempDir,
}
fn harness() -> Harness<SqliteStore> {
    let dir = tempfile::tempdir().unwrap();
    let client = Client::open(SqliteStore::open(dir.path().join("db")).unwrap(), schema()).unwrap();
    Harness {
        runtime: ClientRuntime::new(client),
        _dir: dir,
    }
}
fn hooked_harness(models: &[&str]) -> Harness<SqliteStore> {
    let dir = tempfile::tempdir().unwrap();
    let mut descriptor = serde_json::to_value(schema()).unwrap();
    let second = descriptor["models"][0].clone();
    descriptor["models"][0]["name"] = json!("Alpha");
    let mut second = second;
    second["name"] = json!("Entry");
    descriptor["models"].as_array_mut().unwrap().push(second);
    let schema = Schema::from_value(descriptor).unwrap();
    let client = Client::open(SqliteStore::open(dir.path().join("db")).unwrap(), schema).unwrap();
    Harness {
        runtime: ClientRuntime::with_store_hooks(
            client,
            models.iter().map(|s| s.to_string()).collect(),
        )
        .unwrap(),
        _dir: dir,
    }
}

#[test]
fn store_hook_registration_freezes_after_first_task_admission() {
    let dir = tempfile::tempdir().unwrap();
    let client = Client::open(SqliteStore::open(dir.path().join("db")).unwrap(), schema()).unwrap();
    let mut runtime = ClientRuntime::new(client);
    runtime
        .receive(
            serde_json::from_value(json!({"type":"task","requestId":"1","command":read("e")}))
                .unwrap(),
            NOW,
            ENTROPY,
        )
        .unwrap();
    let error = ClientRuntime::register_store_hooks(runtime, vec!["Entry".into()])
        .err()
        .unwrap();
    assert!(error.to_string().contains("fixed at runtime open"));
}
impl<S: ClientStore + 'static> Harness<S> {
    fn submit(&mut self, input: Value) -> std::result::Result<(), BridgeError> {
        let input: Input = serde_json::from_value(input).unwrap();
        self.runtime.receive(input, NOW, ENTROPY)
    }
    fn task(&mut self, id: &str, command: Value) {
        self.submit(json!({"type":"task","requestId":id,"command":command}))
            .unwrap();
    }
    fn command(&mut self, id: &str, transaction: &str, scope: Option<&str>, command: Value) {
        let mut input = json!({"type":"transactionCommand","requestId":id,"transactionId":transaction,"command":command});
        if let Some(scope) = scope {
            input["scope"] = json!(scope);
        }
        self.submit(input).unwrap();
    }
    fn callback(&mut self, open: &Open, ok: bool, error: Option<&str>) {
        let mut input = json!({"type":"callbackResult","effectId":open.effect,"transactionId":open.transaction,"ok":ok});
        if let Some(error) = error {
            input["error"] = json!(error);
        }
        self.submit(input).unwrap();
    }
    /// Step until the runtime has nothing runnable, then hand over what it said.
    fn run(&mut self) -> Vec<Value> {
        while self.runtime.step(NOW, ENTROPY) {}
        self.events()
    }
    /// Step until an event matches: what it is observed with, and nothing
    /// after it. The database can be inspected at that very point.
    fn until(&mut self, matches: impl Fn(&Value) -> bool) -> Vec<Value> {
        let mut events = self.events();
        while !events.iter().any(&matches) {
            assert!(
                self.runtime.step(NOW, ENTROPY),
                "never observed: {events:?}"
            );
            events.extend(self.events());
        }
        events
    }
    fn completed(&mut self, id: &str) -> Vec<Value> {
        self.until(|e| e["type"] == "taskCompleted" && e["requestId"] == id)
    }
    fn events(&mut self) -> Vec<Value> {
        self.runtime
            .take_events()
            .into_iter()
            .map(|event| serde_json::to_value(event).unwrap())
            .collect()
    }
    /// Submit a `transaction` task and answer its callback effect's identities.
    fn begin(&mut self, id: &str) -> Open {
        self.task(id, json!({"kind":"transaction"}));
        let events = self.run();
        assert_eq!(events.len(), 1, "{events:?}");
        let effect = &events[0];
        assert_eq!(effect["type"], "effect");
        assert_eq!(effect["operation"]["kind"], "callback");
        assert_eq!(effect["operation"]["requestId"], id);
        Open {
            effect: effect["effectId"].as_str().unwrap().to_string(),
            transaction: effect["operation"]["transactionId"]
                .as_str()
                .unwrap()
                .to_string(),
        }
    }
    fn committed(&mut self) -> Option<Value> {
        self.runtime.client().read(&key()).unwrap()
    }
}
struct Open {
    effect: String,
    transaction: String,
}
fn create(id: &str, text: &str) -> Value {
    json!({"kind":"direct","operation":{"model":"Entry","op":"create","identity":{"id":id},"values":{"text":text}}})
}
fn read(id: &str) -> Value {
    json!({"kind":"read","key":{"model":"Entry","identity":{"id":id}}})
}
fn missing_model() -> Value {
    json!({"kind":"direct","operation":{"model":"Nope","op":"create","identity":{"id":"x"},"values":{}}})
}
fn done(id: &str, value: Value) -> Value {
    json!({"type":"taskCompleted","requestId":id,"ok":true,"value":value})
}
fn failed(id: &str, error: &str) -> Value {
    json!({"type":"taskCompleted","requestId":id,"ok":false,"value":null,"error":error})
}
fn row(text: &str) -> Value {
    json!({"id":"e","text":text,"note":null})
}

#[test]
fn authority_hooks_rotate_capabilities_and_commit_after_all_models() {
    let mut h = hooked_harness(&["Entry", "Alpha"]);
    h.task(
        "1",
        json!({"kind":"channel","channel":"feed","subscribed":true}),
    );
    assert_eq!(h.run(), vec![done("1", Value::Null)]);
    common::acknowledge(h.runtime.client(), &[("feed", 0)]);
    let page = json!({
        "cursors":{"feed":{"from":0,"to":2,"head":2}},
        "changes":[
            {"model":"Entry","identity":{"id":"e"},"stamp":1,"state":{"text":"entry","note":null}},
            {"model":"Alpha","identity":{"id":"e"},"stamp":2,"state":{"text":"alpha","note":null}}
        ]
    });
    h.task("2", json!({"kind":"pull","page":page}));
    let first = h.run();
    assert_eq!(first.len(), 1, "{first:?}");
    assert_eq!(first[0]["operation"]["kind"], "storeCallback", "{first:?}");
    assert_eq!(first[0]["operation"]["model"], "Alpha");
    assert_eq!(
        first[0]["operation"]["changes"].as_array().unwrap().len(),
        1
    );
    let alpha = Open {
        effect: first[0]["effectId"].as_str().unwrap().into(),
        transaction: first[0]["operation"]["transactionId"]
            .as_str()
            .unwrap()
            .into(),
    };
    h.task("3", read("e"));
    assert!(h.run().is_empty());
    h.command("4", &alpha.transaction, None, read("e"));
    assert_eq!(h.run(), vec![done("4", Value::Null)]);
    h.callback(&alpha, true, None);
    let second = h.run();
    assert_eq!(second.len(), 1, "{second:?}");
    assert_eq!(second[0]["operation"]["kind"], "storeCallback");
    assert_eq!(second[0]["operation"]["model"], "Entry");
    let entry = Open {
        effect: second[0]["effectId"].as_str().unwrap().into(),
        transaction: second[0]["operation"]["transactionId"]
            .as_str()
            .unwrap()
            .into(),
    };
    assert_ne!(alpha.effect, entry.effect);
    assert_ne!(alpha.transaction, entry.transaction);
    h.callback(&alpha, true, None);
    h.command("5", &alpha.transaction, None, read("e"));
    assert_eq!(h.run(), vec![failed("5", "transaction_closed")]);
    h.callback(&entry, true, None);
    assert_eq!(
        h.run(),
        vec![
            done(
                "2",
                json!({"applied":2,"stale":false,"cursors":{"feed":2},"completions":[],"reports":[]})
            ),
            done("3", row("entry"))
        ]
    );
    assert_eq!(h.committed(), Some(row("entry")));
}

#[test]
fn failed_authority_hook_rolls_back_and_close_cancels_a_stalled_hook() {
    for close in [false, true] {
        let mut h = hooked_harness(&["Entry"]);
        h.task(
            "1",
            json!({"kind":"channel","channel":"feed","subscribed":true}),
        );
        h.run();
        common::acknowledge(h.runtime.client(), &[("feed", 0)]);
        let page = json!({"cursors":{"feed":{"from":0,"to":1,"head":1}},"changes":[{"model":"Entry","identity":{"id":"e"},"stamp":1,"state":{"text":"server","note":null}}]});
        h.task("2", json!({"kind":"pull","page":page}));
        let first = h.run();
        let hook = Open {
            effect: first[0]["effectId"].as_str().unwrap().into(),
            transaction: first[0]["operation"]["transactionId"]
                .as_str()
                .unwrap()
                .into(),
        };
        h.command(
            "written",
            &hook.transaction,
            None,
            create("local", "hook write"),
        );
        assert_eq!(h.run(), vec![done("written", Value::Null)]);
        h.command(
            "3",
            &hook.transaction,
            None,
            json!({"kind":"enqueue","mutation":{"name":"Edit","operations":[]}}),
        );
        assert_eq!(h.run(), vec![failed("3", "store hook cannot enqueue")]);
        if close {
            h.submit(json!({"type":"close"})).unwrap();
            let events = h.run();
            assert_eq!(
                events[0],
                json!({"type":"cancelEffect","effectId":hook.effect})
            );
            assert!(
                events
                    .iter()
                    .any(|event| event == &failed("2", "client_closed")
                        || event["requestId"] == "2"
                            && event["details"]["code"] == "client_closed")
            );
            assert_eq!(h.submit(json!({"type":"callbackResult","effectId":hook.effect,"transactionId":hook.transaction,"ok":true})), Err(BridgeError::Closed));
        } else {
            h.callback(&hook, true, None);
            let events = h.run();
            assert_eq!(events.len(), 2);
            let task = events
                .iter()
                .find(|event| event["requestId"] == "2")
                .unwrap();
            assert_eq!(task["details"]["code"], "store_hook_failed");
            assert_eq!(task["details"]["model"], "Entry");
            assert_eq!(task["details"]["path"], "pull");
            assert_eq!(task["details"]["callbackEffectId"], hook.effect);
            assert_eq!(task["ok"], false);
            let diagnostic = events
                .iter()
                .find(|event| event["type"] == "report")
                .unwrap();
            assert_eq!(diagnostic["diagnostic"]["kind"], "storeHook");
        }
        assert_eq!(h.committed(), None);
        let local = schema()
            .record_key("Entry", &json!({"id":"local"}))
            .unwrap();
        assert_eq!(h.runtime.client().read(&local).unwrap(), None);
    }
}

#[test]
fn ack_authority_uses_the_registered_hook_before_settlement() {
    let mut h = hooked_harness(&["Entry"]);
    h.task("seed", create("e", "start"));
    assert_eq!(h.run(), vec![done("seed", Value::Null)]);
    h.task(
        "1",
        json!({"kind":"enqueue","mutation":common::mutation("local")}),
    );
    let queued = h.run();
    assert_eq!(queued[0]["ok"], true, "{queued:?}");
    h.task("2", json!({"kind":"freeze"}));
    let frozen = h.run();
    let push: Value = serde_json::from_str(frozen[0]["value"].as_str().unwrap()).unwrap();
    let receipt = json!({"clientId":h.runtime.client().client_id(),"batchSequence":push["batchSequence"],"rejections":[],"records":[{"model":"Entry","identity":{"id":"e"},"stamp":1,"state":{"text":"server","note":null}}]});
    h.task(
        "3",
        json!({"kind":"ack","sequence":push["batchSequence"],"receipt":receipt}),
    );
    let events = h.run();
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(events[0]["operation"]["kind"], "storeCallback");
    assert_eq!(h.committed().unwrap()["text"], "local");
    let hook = Open {
        effect: events[0]["effectId"].as_str().unwrap().into(),
        transaction: events[0]["operation"]["transactionId"]
            .as_str()
            .unwrap()
            .into(),
    };
    h.callback(&hook, true, None);
    let events = h.run();
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(events[0]["requestId"], "3");
    assert_eq!(events[0]["ok"], true);
    assert_eq!(h.committed().unwrap()["text"], "server");
}

#[test]
fn tasks_complete_in_order_to_their_own_request_and_a_duplicate_never_runs_twice() {
    let mut h = harness();
    h.task("1", read("e"));
    h.task("2", create("e", "hi"));
    h.task("3", missing_model());
    h.task("4", read("e"));
    // A second submission of a routed id is a protocol report, not a task.
    h.task("4", create("e", "twice"));
    let reported = h.events();
    assert_eq!(
        reported,
        vec![
            json!({"type":"report","diagnostic":{"kind":"protocol","message":"duplicate request id 4"}})
        ]
    );
    // A write's success is observed only once it committed.
    assert_eq!(
        h.completed("2"),
        vec![done("1", Value::Null), done("2", Value::Null)]
    );
    assert_eq!(h.committed(), Some(row("hi")));
    let events = h.run();
    assert_eq!(events.len(), 2, "{events:?}");
    assert_eq!(events[0]["requestId"], "3");
    assert_eq!(events[0]["ok"], false);
    assert!(!events[0]["error"].as_str().unwrap().is_empty());
    assert_eq!(events[1], done("4", row("hi")));
    // Nothing else ran: the duplicate's write never happened.
    assert_eq!(h.committed(), Some(row("hi")));
    // Once completed, an id no longer routes anything; the runtime is idle.
    assert!(!h.runtime.step(NOW, ENTROPY));
    // A command that does not decode is still routed: its request fails
    // with the decoding error in its turn, so no waiter is left behind.
    h.task("5", json!({"kind":"nope"}));
    h.task("6", read("e"));
    let events = h.run();
    assert_eq!(events.len(), 2, "{events:?}");
    assert_eq!(events[0]["requestId"], "5");
    assert_eq!(events[0]["ok"], false);
    assert!(
        events[0]["error"]
            .as_str()
            .unwrap()
            .starts_with("unknown variant `nope`"),
        "{events:?}"
    );
    assert_eq!(events[1], done("6", row("hi")));
}

#[test]
fn a_callback_transaction_owns_the_writer_until_its_result_commits() {
    let mut h = harness();
    let a = h.begin("1");
    // An ordinary read waits outside the open transaction.
    h.task("2", read("e"));
    assert_eq!(h.run(), Vec::<Value>::new());
    // The callback's own write runs on the continuation lane.
    h.command("3", &a.transaction, None, create("e", "hi"));
    assert_eq!(h.run(), vec![done("3", Value::Null)]);
    // A wrong token joins nothing.
    h.command("4", "tx999", None, read("e"));
    assert_eq!(h.run(), vec![failed("4", "transaction_closed")]);
    h.command("5", &a.transaction, None, read("e"));
    assert_eq!(h.run(), vec![done("5", row("hi"))]);
    // Nothing is committed yet: the committed reader does not see the row.
    assert_eq!(h.committed(), None);
    h.callback(&a, true, None);
    // The parent succeeds only once the callback's writes committed.
    assert_eq!(h.completed("1"), vec![done("1", Value::Null)]);
    assert_eq!(h.committed(), Some(row("hi")));
    assert_eq!(h.run(), vec![done("2", row("hi"))]);
    // After the result the transaction is closed to its own token too.
    h.command("6", &a.transaction, None, read("e"));
    assert_eq!(h.run(), vec![failed("6", "transaction_closed")]);
    // Identities are strings from one counter and are never reused.
    let b = h.begin("7");
    assert_ne!(a.transaction, b.transaction);
    assert_ne!(a.effect, b.effect);
    h.callback(&b, true, None);
    let events = h.run();
    assert_eq!(events.last(), Some(&done("7", Value::Null)));
}

#[test]
fn savepoint_scopes_admit_only_the_innermost_open_scope() {
    // A command naming the wrong scope fails structurally and poisons the unit.
    let mut h = harness();
    let a = h.begin("1");
    h.command("2", &a.transaction, None, json!({"kind":"savepoint"}));
    let events = h.run();
    let scope = events[0]["value"]["scope"].as_str().unwrap().to_string();
    assert_eq!(events, vec![done("2", json!({"scope":scope}))]);
    assert!(scope.starts_with("sp"));
    h.command("3", &a.transaction, None, create("e", "outer"));
    assert_eq!(h.run(), vec![failed("3", "invalid transaction scope")]);
    h.command("4", &a.transaction, Some(&scope), create("e", "inner"));
    h.command(
        "5",
        &a.transaction,
        Some(&scope),
        json!({"kind":"release","scope":scope}),
    );
    assert_eq!(
        h.run(),
        vec![done("4", Value::Null), done("5", Value::Null)]
    );
    h.callback(&a, true, None);
    assert_eq!(h.run(), vec![failed("1", "invalid transaction scope")]);
    assert_eq!(h.committed(), None);

    // A failure inside a savepoint that rolls back leaves the outer unit
    // committable; nested scopes release in order.
    let mut h = harness();
    let a = h.begin("1");
    h.command("2", &a.transaction, None, create("e", "outer"));
    h.command("3", &a.transaction, None, json!({"kind":"savepoint"}));
    let events = h.run();
    assert_eq!(events[0], done("2", Value::Null));
    let outer = events[1]["value"]["scope"].as_str().unwrap().to_string();
    h.command(
        "4",
        &a.transaction,
        Some(&outer),
        json!({"kind":"savepoint"}),
    );
    let inner = h.run()[0]["value"]["scope"].as_str().unwrap().to_string();
    assert_ne!(inner, outer);
    h.command("5", &a.transaction, Some(&inner), missing_model());
    let events = h.run();
    assert_eq!(events[0]["requestId"], "5");
    assert_eq!(events[0]["ok"], false);
    h.command(
        "6",
        &a.transaction,
        Some(&inner),
        json!({"kind":"rollbackSavepoint","scope":inner}),
    );
    h.command(
        "7",
        &a.transaction,
        Some(&outer),
        json!({"kind":"release","scope":outer}),
    );
    assert_eq!(
        h.run(),
        vec![done("6", Value::Null), done("7", Value::Null)]
    );
    h.callback(&a, true, None);
    assert_eq!(h.run(), vec![done("1", Value::Null)]);
    assert_eq!(h.committed(), Some(row("outer")));

    // A failure caught at the top level still poisons the commit.
    let mut h = harness();
    let a = h.begin("1");
    h.command("2", &a.transaction, None, create("e", "kept?"));
    h.command("3", &a.transaction, None, missing_model());
    let events = h.run();
    assert_eq!(events[0], done("2", Value::Null));
    let error = events[1]["error"].as_str().unwrap().to_string();
    h.callback(&a, true, None);
    assert_eq!(h.run(), vec![failed("1", &error)]);
    assert_eq!(h.committed(), None);

    // `release` pops only the top scope.
    let mut h = harness();
    let a = h.begin("1");
    h.command("2", &a.transaction, None, json!({"kind":"savepoint"}));
    let outer = h.run()[0]["value"]["scope"].as_str().unwrap().to_string();
    h.command(
        "3",
        &a.transaction,
        Some(&outer),
        json!({"kind":"savepoint"}),
    );
    let inner = h.run()[0]["value"]["scope"].as_str().unwrap().to_string();
    h.command(
        "4",
        &a.transaction,
        Some(&inner),
        json!({"kind":"release","scope":outer}),
    );
    assert_eq!(h.run(), vec![failed("4", "invalid transaction scope")]);
    // The inner scope is still the top one.
    h.command("5", &a.transaction, Some(&inner), create("e", "x"));
    assert_eq!(h.run(), vec![done("5", Value::Null)]);
    h.callback(&a, true, None);
    assert_eq!(h.run(), vec![failed("1", "invalid transaction scope")]);

    // An unclosed savepoint at a successful result rolls back.
    let mut h = harness();
    let a = h.begin("1");
    h.command("2", &a.transaction, None, create("e", "x"));
    h.command("3", &a.transaction, None, json!({"kind":"savepoint"}));
    assert_eq!(h.run().len(), 2);
    h.callback(&a, true, None);
    assert_eq!(h.run(), vec![failed("1", "unclosed savepoint")]);
    assert_eq!(h.committed(), None);
    // The runtime is usable afterwards.
    h.task("4", read("e"));
    assert_eq!(h.run(), vec![done("4", Value::Null)]);
}

#[test]
fn callback_failure_rolls_back_and_stale_or_late_messages_join_nothing() {
    let mut h = harness();
    let a = h.begin("1");
    h.command("2", &a.transaction, None, create("e", "hi"));
    assert_eq!(h.run(), vec![done("2", Value::Null)]);
    // A result naming another effect or transaction is ignored.
    h.submit(
        json!({"type":"callbackResult","effectId":"999","transactionId":a.transaction,"ok":true}),
    )
    .unwrap();
    h.submit(
        json!({"type":"callbackResult","effectId":a.effect,"transactionId":"tx999","ok":true}),
    )
    .unwrap();
    // So is an effect result for the callback: only `callbackResult` ends it.
    h.submit(json!({"type":"effectResult","effectId":a.effect,"outcome":{"ok":true,"value":null}}))
        .unwrap();
    assert_eq!(h.run(), Vec::<Value>::new());
    h.command("3", &a.transaction, None, read("e"));
    assert_eq!(h.run(), vec![done("3", row("hi"))]);
    h.callback(&a, false, Some("boom"));
    assert_eq!(h.run(), vec![failed("1", "boom")]);
    assert_eq!(h.committed(), None);
    h.command("4", &a.transaction, None, read("e"));
    assert_eq!(h.run(), vec![failed("4", "transaction_closed")]);
    // A second result for the finished transaction is ignored as well.
    h.callback(&a, true, None);
    assert_eq!(h.run(), Vec::<Value>::new());

    // A failure without a message still fails the parent.
    let b = h.begin("5");
    h.callback(&b, false, None);
    assert_eq!(h.run(), vec![failed("5", "transaction failed")]);

    // A result that arrives while submitted commands are still queued means
    // the application did not await them: nothing commits.
    let c = h.begin("6");
    h.command("7", &c.transaction, None, create("e", "unawaited"));
    h.callback(&c, true, None);
    // Commands after the result are closed at once.
    h.command("8", &c.transaction, None, read("e"));
    assert_eq!(h.events(), vec![failed("8", "transaction_closed")]);
    assert_eq!(
        h.run(),
        vec![
            failed("7", "transaction_closed"),
            failed("6", "unawaited transaction operation")
        ]
    );
    assert_eq!(h.committed(), None);
}

#[test]
fn close_during_a_callback_rolls_back_and_releases_every_waiter() {
    let mut h = harness();
    let a = h.begin("1");
    h.command("2", &a.transaction, None, create("e", "hi"));
    assert_eq!(h.run(), vec![done("2", Value::Null)]);
    h.task("3", read("e"));
    h.command("4", &a.transaction, None, read("e"));
    h.submit(json!({"type":"close"})).unwrap();
    // Close is admitted as control: nothing more is.
    assert_eq!(
        h.submit(json!({"type":"task","requestId":"5","command":read("e")})),
        Err(BridgeError::Closed)
    );
    assert_eq!(
        h.run(),
        vec![
            json!({"type":"cancelEffect","effectId":a.effect}),
            failed("1", "client_closed"),
            failed("4", "client_closed"),
            failed("3", "client_closed"),
            json!({"type":"runtimeClosed"}),
        ]
    );
    assert!(h.runtime.closed());
    assert!(!h.runtime.step(NOW, ENTROPY));
    assert_eq!(h.submit(json!({"type":"close"})), Err(BridgeError::Closed));
    assert_eq!(h.committed(), None);
    // The rollback released the writer: another client can write the file.
    let dir = h._dir.path().join("db");
    let mut other = Client::open(SqliteStore::open(&dir).unwrap(), schema()).unwrap();
    let _ = other.generation();
    assert!(other.read(&key()).unwrap().is_none());
}

/// A SQLite store whose next `commit` fails once when armed.
struct FailingCommit {
    inner: SqliteStore,
    armed: Arc<AtomicBool>,
}

struct FaultRollback {
    inner: SqliteStore,
    fail_commit: Arc<AtomicBool>,
    fail_rollback: Arc<AtomicBool>,
}
impl ClientStore for FaultRollback {
    fn begin(&mut self) -> Result<()> {
        self.inner.begin()
    }
    fn commit(&mut self) -> Result<()> {
        if self.fail_commit.swap(false, Ordering::SeqCst) {
            return Err(invalid("injected commit failure"));
        }
        self.inner.commit()
    }
    fn rollback(&mut self) -> Result<()> {
        if self.fail_rollback.swap(false, Ordering::SeqCst) {
            return Err(invalid("injected physical rollback failure"));
        }
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
        self.inner.query(sql, parameters)
    }
    fn query_committed(&mut self, sql: &str, parameters: &[Value]) -> Result<SqlRows> {
        self.inner.query_committed(sql, parameters)
    }
}

#[test]
fn authority_rollback_failure_reports_cleanup_and_closes_before_next_write() {
    for commit_failure in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let fail_commit = Arc::new(AtomicBool::new(false));
        let fail_rollback = Arc::new(AtomicBool::new(false));
        let client = Client::open(
            FaultRollback {
                inner: SqliteStore::open(dir.path().join("db")).unwrap(),
                fail_commit: fail_commit.clone(),
                fail_rollback: fail_rollback.clone(),
            },
            schema(),
        )
        .unwrap();
        let mut h = Harness {
            runtime: ClientRuntime::with_store_hooks(client, vec!["Entry".into()]).unwrap(),
            _dir: dir,
        };
        h.task(
            "channel",
            json!({"kind":"channel","channel":"feed","subscribed":true}),
        );
        h.run();
        let state = h
            .runtime
            .client()
            .subscription_state("feed")
            .unwrap()
            .unwrap();
        h.runtime
            .client()
            .initialize_subscriptions(
                &std::collections::BTreeMap::from([("feed".into(), state.subscription_id)]),
                &std::collections::BTreeMap::from([("feed".into(), 0)]),
            )
            .unwrap();
        h.task("owner", json!({"kind":"pull","page":{"cursors":{"feed":{"from":0,"to":1,"head":1}},"changes":[{"model":"Entry","identity":{"id":"e"},"stamp":1,"state":{"text":"server","note":null}}]}}));
        let first = h.run();
        assert_eq!(first[0]["operation"]["kind"], "storeCallback", "{first:?}");
        let hook = Open {
            effect: first[0]["effectId"].as_str().unwrap().into(),
            transaction: first[0]["operation"]["transactionId"]
                .as_str()
                .unwrap()
                .into(),
        };
        h.task("next", create("later", "must not run"));
        fail_rollback.store(true, Ordering::SeqCst);
        if commit_failure {
            fail_commit.store(true, Ordering::SeqCst);
        }
        h.callback(
            &hook,
            commit_failure,
            (!commit_failure).then_some("hook boom"),
        );
        let events = h.run();
        let owner = events
            .iter()
            .find(|event| event["requestId"] == "owner")
            .unwrap();
        assert_eq!(
            owner["error"],
            if commit_failure {
                "injected commit failure"
            } else {
                "hook boom"
            },
            "{events:?}"
        );
        assert_eq!(
            events
                .iter()
                .filter(|event| event["type"] == "report"
                    && event["diagnostic"]["message"]
                        .as_str()
                        .unwrap_or("")
                        .contains("injected physical rollback failure"))
                .count(),
            1,
            "{events:?}"
        );
        assert!(
            events
                .iter()
                .any(|event| event["requestId"] == "next" && event["error"] == "client_closed"),
            "{events:?}"
        );
        assert_eq!(events.last().unwrap()["type"], "runtimeClosed");
        assert!(h.runtime.closed());
    }
}
impl ClientStore for FailingCommit {
    fn begin(&mut self) -> Result<()> {
        self.inner.begin()
    }
    fn commit(&mut self) -> Result<()> {
        if self.armed.swap(false, Ordering::SeqCst) {
            return Err(invalid("injected commit failure"));
        }
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
        self.inner.query(sql, parameters)
    }
    fn query_committed(&mut self, sql: &str, parameters: &[Value]) -> Result<SqlRows> {
        self.inner.query_committed(sql, parameters)
    }
}

#[test]
fn a_failed_commit_fails_the_task_and_lets_no_success_or_change_escape() {
    let dir = tempfile::tempdir().unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let store = FailingCommit {
        inner: SqliteStore::open(dir.path().join("db")).unwrap(),
        armed: armed.clone(),
    };
    let mut h = Harness {
        runtime: ClientRuntime::new(Client::open(store, schema()).unwrap()),
        _dir: dir,
    };
    let generation = h.runtime.client().generation();
    // The callback transaction.
    let a = h.begin("1");
    h.command("2", &a.transaction, None, create("e", "hi"));
    assert_eq!(h.run(), vec![done("2", Value::Null)]);
    armed.store(true, Ordering::SeqCst);
    h.callback(&a, true, None);
    assert_eq!(h.run(), vec![failed("1", "injected commit failure")]);
    assert_eq!(h.runtime.client().generation(), generation);
    assert_eq!(h.committed(), None);
    // An ordinary write task.
    armed.store(true, Ordering::SeqCst);
    h.task("3", create("e", "hi"));
    assert_eq!(h.run(), vec![failed("3", "injected commit failure")]);
    assert_eq!(h.committed(), None);
    // The writer is free again: a later transaction commits.
    let c = h.begin("6");
    h.command("7", &c.transaction, None, create("e", "later"));
    assert_eq!(h.run(), vec![done("7", Value::Null)]);
    h.callback(&c, true, None);
    assert_eq!(h.run(), vec![done("6", Value::Null)]);
    assert_eq!(h.committed(), Some(row("later")));
}

#[test]
fn envelope_fixtures_decode_and_re_encode_unchanged() {
    let fixture: Value =
        serde_json::from_str(include_str!("../../../fixtures/bridge/envelopes.json")).unwrap();
    let inputs = fixture["inputs"].as_array().unwrap();
    let events = fixture["events"].as_array().unwrap();
    assert!(inputs.len() >= 10 && events.len() >= 15);
    for wire in inputs {
        let typed: Input =
            serde_json::from_value(wire.clone()).unwrap_or_else(|e| panic!("{wire}: {e}"));
        assert_eq!(&serde_json::to_value(&typed).unwrap(), wire);
    }
    for wire in events {
        let typed: Event =
            serde_json::from_value(wire.clone()).unwrap_or_else(|e| panic!("{wire}: {e}"));
        assert_eq!(&serde_json::to_value(&typed).unwrap(), wire);
    }
}

/// The id and body of the one `http` effect among `events`.
fn http_effect(events: &[Value]) -> (String, String) {
    let effects: Vec<&Value> = events
        .iter()
        .filter(|e| e["type"] == "effect" && e["operation"]["kind"] == "http")
        .collect();
    assert_eq!(effects.len(), 1, "{events:?}");
    (
        effects[0]["effectId"].as_str().unwrap().to_string(),
        effects[0]["operation"]["body"]
            .as_str()
            .unwrap()
            .to_string(),
    )
}

#[test]
fn a_direct_apply_that_fails_to_commit_fails_the_call_and_lets_nothing_escape() {
    let dir = tempfile::tempdir().unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let store = FailingCommit {
        inner: SqliteStore::open(dir.path().join("db")).unwrap(),
        armed: armed.clone(),
    };
    let mut raw = serde_json::to_value(schema()).unwrap();
    raw["actions"] = json!([{"name":"Rename","version":1,"inputs":[{"kind":"model","name":"entry","model":"Entry","operation":"update","cardinality":"single"}],"outputs":[]}]);
    let client = Client::open(store, Schema::from_value(raw).unwrap()).unwrap();
    let mut h = Harness {
        runtime: ClientRuntime::new(client),
        _dir: dir,
    };
    h.task("connect", json!({"kind":"connect"}));
    assert!(h.run().contains(&done("connect", Value::Null)));
    let rename = json!({"kind":"invoke","name":"Rename","version":1,"args":{"entry":{"id":"e","text":"server"}}});
    let answer = |body: &str| {
        let call: Value = serde_json::from_str(body).unwrap();
        json!({"ok":true,"value":{"status":200,"body":json!({"completion":{"callId":call["call"]["callId"],"outcome":{"status":"succeeded","result":null}},"records":[{"model":"Entry","identity":{"id":"e"},"stamp":1,"state":{"text":"server","note":null}}]}).to_string()}})
    };
    h.task("1", rename.clone());
    let (effect, body) = http_effect(&h.run());
    let generation = h.runtime.client().generation();
    armed.store(true, Ordering::SeqCst);
    h.submit(json!({"type":"effectResult","effectId":effect,"outcome":answer(&body)}))
        .unwrap();
    let events = h.run();
    // The apply's own failure travels as the cause.
    let mut unknown = failed("1", "action.execution_unknown");
    unknown["details"] =
        json!({"code":"action.execution_unknown","message":"injected commit failure"});
    assert!(events.contains(&unknown), "{events:?}");
    assert!(
        !events.iter().any(|e| e["type"] == "callCompleted"),
        "{events:?}"
    );
    assert_eq!(h.runtime.client().generation(), generation);
    assert_eq!(h.committed(), None);
    // The writer is free: a later call applies and succeeds.
    h.task("2", rename);
    let (effect, body) = http_effect(&h.run());
    h.submit(json!({"type":"effectResult","effectId":effect,"outcome":answer(&body)}))
        .unwrap();
    let events = h.completed("2");
    assert_eq!(h.committed().unwrap()["text"], "server", "committed first");
    assert!(events.contains(&done(
        "2",
        json!({"outcome":{"status":"succeeded","result":null}})
    )));
    assert_eq!(h.committed().unwrap()["text"], "server");
}

/// The protocol seams settle calls too: `drop` and `ack` announce every
/// completion as `callCompleted`, after the commit and before the task's own
/// answer, which keeps its value.
#[test]
fn drop_and_ack_announce_their_completions_as_call_completed() {
    let mut h = harness();
    let schema = {
        let mut schema = serde_json::to_value(common::schema()).unwrap();
        schema["actions"] = json!([{"name":"Ping","version":1,"inputs":[],"outputs":[]}]);
        schema
    };
    let dir = tempfile::tempdir().unwrap();
    h.runtime = ClientRuntime::new(
        Client::open(
            SqliteStore::open(dir.path().join("db")).unwrap(),
            Schema::from_value(schema).unwrap(),
        )
        .unwrap(),
    );
    let ping = json!({"kind":"submitAction","name":"Ping","version":1,"args":{}});
    h.task("dropped", ping.clone());
    let events = h.run();
    let dropped = events[events.len() - 1]["value"].clone();
    h.task("drop", json!({"kind":"drop","ordinal":dropped["ordinal"]}));
    let events = h.run();
    let announced = position(&events, |e| e["type"] == "callCompleted");
    assert_eq!(events[announced]["callId"], dropped["callId"]);
    let answered = position(&events, |e| e["requestId"] == "drop");
    assert!(announced < answered);
    assert_eq!(
        events[answered]["value"]["completions"][0]["callId"], dropped["callId"],
        "the value is unchanged"
    );

    h.task("sent", ping);
    let events = h.run();
    let sent = events[events.len() - 1]["value"].clone();
    h.task("freeze", json!({"kind":"freeze"}));
    let events = h.run();
    let push: Value =
        serde_json::from_str(events[events.len() - 1]["value"].as_str().unwrap()).unwrap();
    let client_id = h.runtime.client().client_id().to_string();
    let receipt = json!({"clientId":client_id,"batchSequence":push["batchSequence"],"rejections":[],
        "completions":[{"callId":sent["callId"],"outcome":{"status":"succeeded","result":null}}],"records":[]});
    h.task(
        "ack",
        json!({"kind":"ack","sequence":push["batchSequence"],"receipt":receipt}),
    );
    // The call's outcome is announced once the receipt committed.
    let mut events = h.until(|e| e["type"] == "callCompleted");
    assert_eq!(h.runtime.client().pending_count().unwrap(), 0);
    events.extend(h.run());
    let announced = position(&events, |e| e["type"] == "callCompleted");
    let answered = position(&events, |e| e["requestId"] == "ack");
    assert!(announced < answered, "{events:?}");
    assert_eq!(
        events[announced],
        json!({"type":"callCompleted","callId":sent["callId"],"outcome":{"status":"succeeded","result":null}})
    );
    assert_eq!(
        events[answered]["value"]["completions"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}
fn position(events: &[Value], matches: impl Fn(&Value) -> bool) -> usize {
    events
        .iter()
        .position(matches)
        .unwrap_or_else(|| panic!("not found in {events:?}"))
}

/// The entry schema with a `Ping` action, and the same schema with a
/// required field the stored rows lack.
fn schemas() -> (Value, Value) {
    let mut schema = serde_json::to_value(common::schema()).unwrap();
    schema["actions"] = json!([{"name":"Ping","version":1,"inputs":[],"outputs":[]}]);
    let mut breaking = schema.clone();
    breaking["models"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"due","nullable":false,"type":{"kind":"scalar","name":"string"}}));
    (schema, breaking)
}
fn at(path: &std::path::Path, schema: &Value) -> Harness<SqliteStore> {
    Harness {
        runtime: ClientRuntime::open_at(
            path,
            Schema::from_value(schema.clone()).unwrap(),
            Box::new(|p| SqliteStore::open(p)),
            false,
        )
        .unwrap(),
        _dir: tempfile::tempdir().unwrap(),
    }
}
impl<S: ClientStore + 'static> Harness<S> {
    /// Run one task to quiescence: its completion and everything said with it.
    fn call(&mut self, id: &str, command: Value) -> (Value, Vec<Value>) {
        self.task(id, command);
        let events = self.run();
        let completion = events[position(&events, |e| {
            e["type"] == "taskCompleted" && e["requestId"] == id
        })]
        .clone();
        (completion, events)
    }
    fn close(mut self) {
        self.submit(json!({"type":"close"})).unwrap();
        assert_eq!(self.run().last().unwrap(), &json!({"type":"runtimeClosed"}));
    }
}

/// A dropped call and the calls a rebuild leaves behind keep their identity to
/// their terminal outcome: `drop` completes the call as `dropped`, and the
/// rebuild reports each abandoned call - frozen ones as possibly executed -
/// and completes it.
#[test]
fn dropped_and_abandoned_calls_keep_their_terminal_identity() {
    let dir = tempfile::tempdir().unwrap();
    let (schema, breaking) = schemas();
    let ping = json!({"kind":"submitAction","name":"Ping","version":1,"args":{}});
    for frozen in [false, true] {
        let path = dir.path().join(if frozen { "frozen" } else { "unsent" });
        let mut h = at(&path, &schema);
        let first = h.call("first", ping.clone()).0["value"].clone();
        if !frozen {
            let (dropped, events) =
                h.call("drop", json!({"kind":"drop","ordinal":first["ordinal"]}));
            assert_eq!(
                dropped["value"]["completions"][0]["callId"],
                first["callId"]
            );
            assert_eq!(
                dropped["value"]["completions"][0]["outcome"]["code"],
                "dropped"
            );
            let announced = &events[position(&events, |e| e["type"] == "callCompleted")];
            assert_eq!(announced["callId"], first["callId"]);
            assert_eq!(announced["outcome"]["code"], "dropped");
        }
        let left = h.call("left", ping.clone()).0["value"].clone();
        if frozen {
            assert!(h.call("freeze", json!({"kind":"freeze"})).0["value"].is_string());
        }
        h.close();
        let mut h = at(&path, &breaking);
        let (report, events) = h.call("rebuild", json!({"kind":"rebuild","discardPending":true}));
        let abandoned = report["value"]["abandonedCalls"]
            .as_array()
            .unwrap()
            .clone();
        assert!(
            abandoned.contains(&json!({"callId":left["callId"],"frozen":frozen})),
            "{abandoned:?}"
        );
        if frozen {
            assert!(abandoned.contains(&json!({"callId":first["callId"],"frozen":true})));
        }
        for call in &abandoned {
            let execution = if call["frozen"] == true {
                "unknown"
            } else {
                "rejected"
            };
            assert!(
                events.contains(&json!({"type":"callCompleted","callId":call["callId"],
                    "outcome":{"status":"failed","code":"abandoned","execution":execution}})),
                "{events:?}"
            );
        }
    }
}

/// An incompatible schema at open keeps the old file while it holds unsent
/// work, which is still settled through the same runtime; `rebuild` is
/// refused until then and afterwards switches to a fresh file. Every step is
/// visible in `status().schema`, and a watch re-runs on the fresh file.
#[test]
fn an_incompatible_schema_keeps_its_file_until_the_work_is_settled_and_rebuilt() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let (schema, breaking) = schemas();
    let mut h = at(&path, &schema);
    assert_eq!(h.runtime.opened()["schema"]["rebuilt"], false);
    h.call("seed", create("e", "A"));
    h.call(
        "edit",
        json!({"kind":"enqueue","mutation":{"name":"Edit","operations":[{"model":"Entry","op":"update","identity":{"id":"e"},"values":{"text":"B"}}]}}),
    );
    h.call("freeze", json!({"kind":"freeze"}));
    h.close();

    let mut h = at(&path, &breaking);
    let opened = h.runtime.opened();
    assert_eq!(opened["schema"]["rebuilt"], false);
    assert_eq!(opened["schema"]["pending"]["pending"], 1);
    assert!(
        opened["schema"]["pending"]["reason"]
            .as_str()
            .unwrap()
            .contains("due")
    );
    let status = h.call("status", json!({"kind":"status"})).0["value"].clone();
    assert_eq!(status["pending"], 1);
    assert_eq!(
        status["schema"]["pending"]["oldFile"].as_str().unwrap(),
        path.to_string_lossy()
    );
    let (refused, _) = h.call("refused", json!({"kind":"rebuild"}));
    assert_eq!(refused["ok"], false, "unsent work blocks the rebuild");
    let (watch, _) = h.call("watch", json!({"kind":"watch","model":"Entry"}));
    let watch = watch["value"]["observerId"].clone();
    let receipt = json!({"clientId":opened["clientId"],"batchSequence":1,"rejections":[],
        "records":[{"model":"Entry","identity":{"id":"e"},"stamp":2,"state":{"text":"B","note":null}}]});
    let (acked, _) = h.call("ack", json!({"kind":"ack","sequence":1,"receipt":receipt}));
    assert_eq!(acked["ok"], true, "{acked}");
    let (report, events) = h.call("rebuild", json!({"kind":"rebuild"}));
    assert_eq!(report["value"]["leftPending"], 0);
    assert!(
        report["value"]["newFile"]
            .as_str()
            .unwrap()
            .ends_with("db.1")
    );
    assert!(
        events.contains(&json!({"type":"observerChanged","observerId":watch,"snapshot":{"kind":"watch","rows":[]}})),
        "the watch re-ran on the fresh file: {events:?}"
    );
    let status = h.call("status", json!({"kind":"status"})).0["value"].clone();
    assert_eq!(status["schema"]["rebuilt"], true);
    assert!(status["schema"]["pending"].is_null());
    assert_eq!(
        h.call("read", read("e")).0["value"],
        Value::Null,
        "the fresh file is empty"
    );
    assert!(path.exists(), "the old file is kept");
}

#[test]
fn pending_rebuild_drains_old_authority_then_activates_target_hooks() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db-hooks");
    let (schema, mut target) = schemas();
    let mut alpha = schema["models"][0].clone();
    alpha["name"] = json!("Alpha");
    target["models"].as_array_mut().unwrap().push(alpha);
    let mut old = at(&path, &schema);
    old.call("seed", create("e", "old"));
    old.call(
        "edit",
        json!({"kind":"enqueue","mutation":common::mutation("local")}),
    );
    let push: Value = serde_json::from_str(
        old.call("freeze", json!({"kind":"freeze"})).0["value"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    let client_id = old.runtime.client().client_id().to_string();
    old.close();
    let client = Client::open_at(
        &path,
        Schema::from_value(target).unwrap(),
        Box::new(|p| SqliteStore::open(p)),
        false,
    )
    .unwrap();
    let mut h = Harness {
        runtime: ClientRuntime::with_store_hooks(client, vec!["Entry".into(), "Alpha".into()])
            .unwrap(),
        _dir: tempfile::tempdir().unwrap(),
    };
    assert!(h.runtime.opened()["schema"]["pending"].is_object());
    let receipt = json!({"clientId":client_id,"batchSequence":push["batchSequence"],"rejections":[],"records":[{"model":"Entry","identity":{"id":"e"},"stamp":1,"state":{"text":"server","note":null}}]});
    let (acked, events) = h.call(
        "ack",
        json!({"kind":"ack","sequence":push["batchSequence"],"receipt":receipt}),
    );
    assert_eq!(acked["ok"], true, "{events:?}");
    assert!(
        events
            .iter()
            .all(|event| event["operation"]["kind"] != "storeCallback")
    );
    assert_eq!(h.call("rebuild", json!({"kind":"rebuild"})).0["ok"], true);
    h.call(
        "channel",
        json!({"kind":"channel","channel":"feed","subscribed":true}),
    );
    common::acknowledge(h.runtime.client(), &[("feed", 0)]);
    h.task("pull", json!({"kind":"pull","page":{"cursors":{"feed":{"from":0,"to":1,"head":1}},"changes":[{"model":"Alpha","identity":{"id":"a"},"stamp":1,"state":{"text":"server","note":null}}]}}));
    let events = h.run();
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(events[0]["operation"]["kind"], "storeCallback");
    assert_eq!(events[0]["operation"]["model"], "Alpha");
}

// --- Transactional Mutations and local callbacks ------------------------------

/// The entry schema with the `Publish` (creates an Entry) and `Ping`
/// Mutations and the `Find` Query.
fn mutation_schema(models: Value) -> Schema {
    let mut raw = serde_json::to_value(schema()).unwrap();
    if !models.is_null() {
        raw["models"] = models;
    }
    raw["actions"] = json!([
        {"name":"Publish","version":1,"inputs":[{"kind":"model","name":"entry","model":"Entry","operation":"create","cardinality":"single"}],"outputs":[]},
        {"name":"Ping","version":1,"inputs":[],"outputs":[]},
        {"name":"Find","version":1,"kind":"query","inputs":[],"outputs":[]}
    ]);
    Schema::from_value(raw).unwrap()
}
fn mutation_harness() -> Harness<SqliteStore> {
    let dir = tempfile::tempdir().unwrap();
    let client = Client::open(
        SqliteStore::open(dir.path().join("db")).unwrap(),
        mutation_schema(Value::Null),
    )
    .unwrap();
    Harness {
        runtime: ClientRuntime::new(client),
        _dir: dir,
    }
}
fn submit_mutation(name: &str, args: Value, local: bool) -> Value {
    let mut command = json!({"kind":"submitMutation","name":name,"version":1,"args":args});
    if local {
        command["local"] = json!(true);
    }
    command
}
fn ping() -> Value {
    submit_mutation("Ping", json!({}), false)
}
/// `Publish` of Entry `id`, asking for a local callback.
fn publish(id: &str) -> Value {
    submit_mutation(
        "Publish",
        json!({"entry":{"id":id,"text":"published","note":null}}),
        true,
    )
}
fn delete(id: &str) -> Value {
    json!({"kind":"direct","operation":{"model":"Entry","op":"delete","identity":{"id":id}}})
}
fn call_state(call: &Value, state: &str) -> Value {
    json!({"type":"transactionCallState","callId":call,"state":state})
}
fn call_states(events: &[Value]) -> Vec<Value> {
    events
        .iter()
        .filter(|e| e["type"] == "transactionCallState")
        .cloned()
        .collect()
}
/// The `callId` a successful submission answered.
fn submitted(events: &[Value], id: &str) -> Value {
    let completion = &events[position(events, |e| {
        e["type"] == "taskCompleted" && e["requestId"] == id
    })];
    assert_eq!(completion["ok"], true, "{completion}");
    completion["value"]["callId"].clone()
}
const CAPABILITY: &str = "invalid transaction capability";
/// The local callback a `submitMutation` asked for.
struct LocalCallback {
    effect: String,
    transaction: String,
    companion: String,
}
impl<S: ClientStore + 'static> Harness<S> {
    /// Submit a Mutation asking for a local callback and answer the
    /// `mutationLocal` effect's identities: the submission itself waits.
    fn local(
        &mut self,
        id: &str,
        transaction: &str,
        scope: Option<&str>,
        command: Value,
    ) -> LocalCallback {
        self.command(id, transaction, scope, command);
        let events = self.run();
        assert_eq!(events.len(), 1, "{events:?}");
        let effect = &events[0];
        assert_eq!(effect["type"], "effect", "{effect}");
        assert_eq!(effect["operation"]["kind"], "mutationLocal", "{effect}");
        assert_eq!(effect["operation"]["requestId"], id);
        assert_eq!(effect["operation"]["transactionId"], transaction);
        LocalCallback {
            effect: effect["effectId"].as_str().unwrap().to_string(),
            transaction: transaction.to_string(),
            companion: effect["operation"]["companionId"]
                .as_str()
                .unwrap()
                .to_string(),
        }
    }
    /// A command of the local callback: it carries the companion token.
    fn companion(&mut self, id: &str, local: &LocalCallback, scope: Option<&str>, command: Value) {
        let mut input = json!({"type":"transactionCommand","requestId":id,
            "transactionId":local.transaction,"companionId":local.companion,"command":command});
        if let Some(scope) = scope {
            input["scope"] = json!(scope);
        }
        self.submit(input).unwrap();
    }
    fn finish_local(&mut self, local: &LocalCallback, ok: bool, error: Option<&str>) {
        let mut input = json!({"type":"callbackResult","effectId":local.effect,
            "transactionId":local.transaction,"companionId":local.companion,"ok":ok});
        if let Some(error) = error {
            input["error"] = json!(error);
        }
        self.submit(input).unwrap();
    }
    fn entry(&mut self, id: &str) -> Option<Value> {
        let key = RecordKey {
            model: "Entry".into(),
            identity: json!({ "id": id }),
        };
        self.runtime.client().read(&key).unwrap()
    }
    /// The committed queue: how many calls wait.
    fn pending(&mut self) -> u64 {
        self.runtime
            .client()
            .read_sql("SELECT COUNT(*) AS n FROM axton_mutation", &[])
            .unwrap()[0]["n"]
            .as_u64()
            .unwrap()
    }
    /// The committed queued operations as `kind model id op`, in order.
    fn operations(&mut self) -> Vec<String> {
        self.runtime
            .client()
            .read_sql(
                "SELECT kind, model, identity, op FROM axton_mutation_operation ORDER BY ordinal, position",
                &[],
            )
            .unwrap()
            .iter()
            .map(|row| {
                let identity: Value =
                    serde_json::from_str(row["identity"].as_str().unwrap()).unwrap();
                format!(
                    "{} {} {} {}",
                    row["kind"].as_str().unwrap(),
                    row["model"].as_str().unwrap(),
                    identity["id"].as_str().unwrap(),
                    row["op"].as_str().unwrap()
                )
            })
            .collect()
    }
    /// Every committed row a transactional submission can write: the queue,
    /// the local write journal, rejections, record stamps, the client
    /// counters, and the Entry rows with their before images.
    fn recovery_state(&mut self) -> Value {
        let mut state = serde_json::Map::new();
        for (name, sql) in [
            (
                "axton_mutation",
                "SELECT * FROM axton_mutation ORDER BY ordinal",
            ),
            (
                "axton_mutation_operation",
                "SELECT * FROM axton_mutation_operation ORDER BY ordinal, position",
            ),
            (
                "axton_local_write",
                "SELECT * FROM axton_local_write ORDER BY sequence",
            ),
            (
                "axton_rejection",
                "SELECT * FROM axton_rejection ORDER BY ordinal",
            ),
            (
                "axton_record",
                "SELECT * FROM axton_record ORDER BY model, identity",
            ),
            (
                "axton_client",
                "SELECT next_ordinal, next_push, last_completed_push FROM axton_client",
            ),
            ("Entry", "SELECT * FROM Entry ORDER BY id"),
            (
                "axton_before_Entry",
                "SELECT * FROM axton_before_Entry ORDER BY id",
            ),
        ] {
            let rows = self.runtime.client().read_sql(sql, &[]).unwrap();
            state.insert(name.into(), Value::Array(rows));
        }
        Value::Object(state)
    }
}

/// A named Mutation submitted by the callback answers its `{callId,
/// ordinal}` at once but stays provisional: nothing is durable or frozen
/// until the commit, and the commit turns it `committed` before the
/// transaction's own success.
#[test]
fn a_named_mutation_submitted_in_a_callback_is_provisional_until_the_commit() {
    let mut h = mutation_harness();
    let a = h.begin("tx");
    h.command("s", &a.transaction, None, ping());
    let events = h.run();
    let call = submitted(&events, "s");
    assert!(call.is_string());
    assert_eq!(events, vec![done("s", json!({"callId":call,"ordinal":1}))]);
    assert_eq!(h.pending(), 0, "nothing is durable before the commit");
    h.task("freeze", json!({"kind":"freeze"}));
    assert_eq!(
        h.run(),
        Vec::<Value>::new(),
        "a freeze waits for the writer"
    );
    h.callback(&a, true, None);
    let events = h.run();
    assert_eq!(events.len(), 3, "{events:?}");
    assert_eq!(events[0], call_state(&call, "committed"));
    assert_eq!(events[1], done("tx", Value::Null));
    let push: Value = serde_json::from_str(events[2]["value"].as_str().unwrap()).unwrap();
    assert_eq!(events[2]["requestId"], "freeze");
    assert_eq!(push["mutations"][0]["callId"], call);
    assert_eq!(h.pending(), 1);
    // A Query is refused, like an invalid submission; neither writes.
    let b = h.begin("tx2");
    h.command(
        "q",
        &b.transaction,
        None,
        submit_mutation("Find", json!({}), false),
    );
    h.command(
        "bad",
        &b.transaction,
        None,
        submit_mutation("Ping", json!({"extra":1}), false),
    );
    h.command(
        "flag",
        &b.transaction,
        None,
        json!({"kind":"submitMutation","name":"Ping","version":1,"args":{},"local":"yes"}),
    );
    let events = h.run();
    for id in ["q", "bad", "flag"] {
        let completion = &events[position(&events, |e| e["requestId"] == id)];
        assert_eq!(completion["ok"], false, "{completion}");
    }
    assert!(
        events[position(&events, |e| e["requestId"] == "flag")]["error"]
            .as_str()
            .unwrap()
            .contains("local must be a boolean")
    );
    h.callback(&b, true, None);
    let events = h.run();
    assert_eq!(events.len(), 1, "{events:?}");
    assert_eq!(events[0]["requestId"], "tx2");
    assert_eq!(events[0]["ok"], false, "a refused submission poisons it");
    assert_eq!(h.pending(), 1);
}

/// With `local`, the submission parks on a `mutationLocal` effect naming it.
/// The callback reads the call's optimism, its writes are the call's
/// companions, and its successful end answers the submission - without
/// committing or closing the outer transaction.
#[test]
fn a_local_callback_runs_inside_its_submission_and_its_writes_are_companions() {
    let mut h = mutation_harness();
    h.task("seed", create("draft", "local"));
    assert_eq!(h.run(), vec![done("seed", Value::Null)]);
    let a = h.begin("tx");
    let local = h.local("s", &a.transaction, None, publish("p"));
    assert_ne!(local.effect, a.effect);
    h.companion("r", &local, None, read("p"));
    h.companion("d", &local, None, delete("draft"));
    assert_eq!(
        h.run(),
        vec![
            done("r", json!({"id":"p","text":"published","note":null})),
            done("d", Value::Null)
        ]
    );
    h.finish_local(&local, true, None);
    let events = h.run();
    let call = submitted(&events, "s");
    assert_eq!(events, vec![done("s", json!({"callId":call,"ordinal":1}))]);
    // The parent capability is restored; nothing committed yet.
    h.command("after", &a.transaction, None, read("draft"));
    assert_eq!(h.run(), vec![done("after", Value::Null)]);
    assert_eq!(h.entry("draft").unwrap()["text"], "local");
    assert_eq!(h.pending(), 0);
    h.callback(&a, true, None);
    assert_eq!(
        h.run(),
        vec![call_state(&call, "committed"), done("tx", Value::Null)]
    );
    assert_eq!(h.entry("draft"), None);
    assert_eq!(h.entry("p").unwrap()["text"], "published");
    assert_eq!(
        h.operations(),
        ["wire Entry p create", "companion Entry draft delete"]
    );
    let (frozen, _) = h.call("freeze", json!({"kind":"freeze"}));
    let body = frozen["value"].as_str().unwrap();
    assert!(!body.contains("draft"), "a companion is never sent: {body}");
}

/// While a local callback runs, only its own local reads and writes are
/// admitted. The parent's captured handle, another or an expired companion
/// token, and anything but a local read or write are refused - a
/// structural failure that fails the submission and the transaction.
#[test]
fn a_local_callback_admits_only_its_own_local_commands() {
    let mut h = mutation_harness();
    let a = h.begin("tx");
    let local = h.local("s", &a.transaction, None, publish("p"));
    // The parent's captured handle.
    h.command("parent-write", &a.transaction, None, create("x", "x"));
    h.command("parent-submit", &a.transaction, None, ping());
    h.command("parent-read", &a.transaction, None, read("p"));
    // A token that names no open local callback.
    h.submit(
        json!({"type":"transactionCommand","requestId":"wrong","transactionId":a.transaction,
        "companionId":"c999","command":read("p")}),
    )
    .unwrap();
    let events = h.run();
    for id in ["parent-write", "parent-submit", "parent-read", "wrong"] {
        assert!(events.contains(&failed(id, CAPABILITY)), "{id}: {events:?}");
    }
    // The callback's own commands: local reads and writes only.
    let enqueue = json!({"kind":"enqueue","mutation":{"name":"Edit","operations":[
        {"model":"Entry","op":"update","identity":{"id":"p"},"values":{"text":"raw"}}]}});
    let refused = [
        ("enqueue", enqueue),
        ("submit", ping()),
        (
            "channel",
            json!({"kind":"channel","channel":"book","subscribed":true}),
        ),
        ("savepoint", json!({"kind":"savepoint"})),
        ("release", json!({"kind":"release"})),
    ];
    for (id, command) in refused.clone() {
        h.companion(id, &local, None, command);
    }
    h.companion(
        "load",
        &local,
        None,
        json!({"kind":"loadStart","name":"Recent","version":1,"args":{}}),
    );
    h.companion("own", &local, None, read("p"));
    let events = h.run();
    for (id, _) in &refused {
        assert!(events.contains(&failed(id, CAPABILITY)), "{id}: {events:?}");
    }
    assert_eq!(
        events[position(&events, |e| e["requestId"] == "load")]["ok"],
        false
    );
    assert_eq!(
        events[position(&events, |e| e["requestId"] == "own")]["ok"],
        true
    );
    h.finish_local(&local, true, None);
    assert_eq!(h.run(), vec![failed("s", CAPABILITY)]);
    // Once it finished, its token has expired.
    h.companion("expired", &local, None, read("p"));
    assert_eq!(h.run(), vec![failed("expired", CAPABILITY)]);
    h.callback(&a, true, None);
    assert_eq!(h.run(), vec![failed("tx", CAPABILITY)]);
    assert_eq!(h.pending(), 0);
    assert_eq!(h.entry("x"), None);
}

/// A `callbackResult` ends the local callback only when it names that
/// callback's effect, transaction and companion token; the outer callback's
/// result never ends it, and its end never ends the outer transaction.
#[test]
fn local_callback_results_correlate_only_with_their_own_tokens() {
    let mut h = mutation_harness();
    let a = h.begin("tx");
    let local = h.local("s", &a.transaction, None, publish("p"));
    let mismatched = [
        // No companion token: the outer result form for the local effect.
        json!({"type":"callbackResult","effectId":local.effect,"transactionId":a.transaction,"ok":true}),
        // The outer effect with the companion token.
        json!({"type":"callbackResult","effectId":a.effect,"transactionId":a.transaction,"companionId":local.companion,"ok":true}),
        json!({"type":"callbackResult","effectId":local.effect,"transactionId":a.transaction,"companionId":"c999","ok":true}),
        json!({"type":"callbackResult","effectId":local.effect,"transactionId":"tx999","companionId":local.companion,"ok":true}),
        // An effect result is not a callback's end either.
        json!({"type":"effectResult","effectId":local.effect,"outcome":{"ok":true,"value":null}}),
    ];
    for input in mismatched {
        h.submit(input).unwrap();
        assert_eq!(h.run(), Vec::<Value>::new());
    }
    h.companion("own", &local, None, read("p"));
    assert_eq!(h.run().len(), 1, "the local callback is still open");
    h.finish_local(&local, true, None);
    let events = h.run();
    let call = submitted(&events, "s");
    // A stale second result changes nothing; the outer transaction is open.
    h.finish_local(&local, false, Some("late"));
    assert_eq!(h.run(), Vec::<Value>::new());
    assert_eq!(h.pending(), 0);
    h.command("more", &a.transaction, None, create("x", "x"));
    assert_eq!(h.run(), vec![done("more", Value::Null)]);
    h.callback(&a, true, None);
    assert_eq!(
        h.run(),
        vec![call_state(&call, "committed"), done("tx", Value::Null)]
    );
    assert_eq!(h.pending(), 1);
}

/// A failed local callback fails its submission with its error and poisons
/// the transaction, unless the savepoint the submission ran in rolls back.
/// Unawaited callback work fails it, and the outer callback ending while the
/// local one runs fails both.
#[test]
fn a_failed_local_callback_fails_its_submission_under_the_transaction_rules() {
    // A thrown callback.
    let mut h = mutation_harness();
    h.task("seed", create("draft", "local"));
    h.run();
    let a = h.begin("tx");
    let local = h.local("s", &a.transaction, None, publish("p"));
    h.companion("d", &local, None, delete("draft"));
    assert_eq!(h.run(), vec![done("d", Value::Null)]);
    h.finish_local(&local, false, Some("boom"));
    assert_eq!(h.run(), vec![failed("s", "boom")]);
    h.callback(&a, true, None);
    assert_eq!(h.run(), vec![failed("tx", "boom")]);
    assert_eq!(h.pending(), 0);
    assert_eq!(h.entry("draft").unwrap()["text"], "local");

    // A failed companion write the callback caught still fails it.
    let a = h.begin("tx2");
    let local = h.local("s2", &a.transaction, None, publish("p"));
    h.companion("bad", &local, None, missing_model());
    let error = h.run()[0]["error"].as_str().unwrap().to_string();
    h.finish_local(&local, true, None);
    assert_eq!(h.run(), vec![failed("s2", &error)]);
    h.callback(&a, true, None);
    assert_eq!(h.run(), vec![failed("tx2", &error)]);

    // Inside a savepoint that rolls back, the rest of the unit commits.
    let a = h.begin("tx3");
    h.command("sp", &a.transaction, None, json!({"kind":"savepoint"}));
    let scope = h.run()[0]["value"]["scope"].as_str().unwrap().to_string();
    let local = h.local("s3", &a.transaction, Some(&scope), publish("p"));
    h.companion("d3", &local, Some(&scope), delete("draft"));
    h.run();
    h.finish_local(&local, false, Some("boom"));
    assert_eq!(h.run(), vec![failed("s3", "boom")]);
    h.command(
        "rb",
        &a.transaction,
        Some(&scope),
        json!({"kind":"rollbackSavepoint","scope":scope}),
    );
    h.command("kept", &a.transaction, None, create("kept", "yes"));
    assert_eq!(
        h.run(),
        vec![done("rb", Value::Null), done("kept", Value::Null)]
    );
    h.callback(&a, true, None);
    assert_eq!(h.run(), vec![done("tx3", Value::Null)]);
    assert_eq!(h.entry("kept").unwrap()["text"], "yes");
    assert_eq!(h.entry("draft").unwrap()["text"], "local");
    assert_eq!(h.pending(), 0);

    // Work the local callback did not await.
    let a = h.begin("tx4");
    let local = h.local("s4", &a.transaction, None, publish("p"));
    h.companion("unawaited", &local, None, delete("draft"));
    h.finish_local(&local, true, None);
    assert_eq!(
        h.run(),
        vec![
            failed("unawaited", "transaction_closed"),
            failed("s4", "unawaited transaction operation")
        ]
    );
    h.callback(&a, true, None);
    assert_eq!(
        h.run(),
        vec![failed("tx4", "unawaited transaction operation")]
    );

    // The outer callback ends while the local one still runs.
    let a = h.begin("tx5");
    let local = h.local("s5", &a.transaction, None, publish("p"));
    h.callback(&a, true, None);
    let events = h.run();
    assert_eq!(
        events,
        vec![
            json!({"type":"cancelEffect","effectId":local.effect}),
            failed("s5", "transaction_closed"),
            failed("tx5", "unawaited transaction operation"),
        ]
    );
    // Its late result and commands join nothing.
    h.finish_local(&local, true, None);
    h.companion("late", &local, None, read("p"));
    assert_eq!(h.run(), vec![failed("late", "transaction_closed")]);
    assert_eq!(h.pending(), 0);
    assert_eq!(h.entry("draft").unwrap()["text"], "local");
}

/// Calls are provisional per savepoint scope: rolling a scope back turns
/// exactly its calls `rolledBack`, a released scope's calls survive into the
/// parent, and the commit turns the survivors `committed`.
#[test]
fn savepoint_rollback_rolls_back_only_the_calls_of_its_scope() {
    let mut h = mutation_harness();
    let a = h.begin("tx");
    h.command("a", &a.transaction, None, ping());
    h.command("sp", &a.transaction, None, json!({"kind":"savepoint"}));
    let events = h.run();
    let first = submitted(&events, "a");
    let scope = events[1]["value"]["scope"].as_str().unwrap().to_string();
    h.command("b", &a.transaction, Some(&scope), ping());
    let events = h.run();
    let second = submitted(&events, "b");
    let local = h.local("c", &a.transaction, Some(&scope), publish("p"));
    h.finish_local(&local, true, None);
    let third = submitted(&h.run(), "c");
    h.command(
        "rb",
        &a.transaction,
        Some(&scope),
        json!({"kind":"rollbackSavepoint","scope":scope}),
    );
    assert_eq!(
        h.run(),
        vec![
            call_state(&second, "rolledBack"),
            call_state(&third, "rolledBack"),
            done("rb", Value::Null)
        ]
    );
    h.command("sp2", &a.transaction, None, json!({"kind":"savepoint"}));
    let scope = h.run()[0]["value"]["scope"].as_str().unwrap().to_string();
    h.command("d", &a.transaction, Some(&scope), ping());
    h.command(
        "rel",
        &a.transaction,
        Some(&scope),
        json!({"kind":"release","scope":scope}),
    );
    let events = h.run();
    let fourth = submitted(&events, "d");
    assert!(call_states(&events).is_empty(), "{events:?}");
    h.callback(&a, true, None);
    assert_eq!(
        h.run(),
        vec![
            call_state(&first, "committed"),
            call_state(&fourth, "committed"),
            done("tx", Value::Null)
        ]
    );
    let calls: Vec<Value> = h
        .runtime
        .client()
        .read_sql("SELECT call_id FROM axton_mutation ORDER BY ordinal", &[])
        .unwrap()
        .into_iter()
        .map(|row| row["call_id"].clone())
        .collect();
    assert_eq!(calls, vec![first, fourth]);
}

/// A rollback - a failed callback or a failed commit - turns every
/// provisional call `rolledBack` before the transaction's failure, and none
/// of them is ever frozen.
#[test]
fn a_rollback_or_failed_commit_turns_every_provisional_call_rolled_back() {
    let mut h = mutation_harness();
    let a = h.begin("tx");
    h.command("a", &a.transaction, None, ping());
    h.command("b", &a.transaction, None, ping());
    let events = h.run();
    let (first, second) = (submitted(&events, "a"), submitted(&events, "b"));
    h.callback(&a, false, Some("boom"));
    assert_eq!(
        h.run(),
        vec![
            call_state(&first, "rolledBack"),
            call_state(&second, "rolledBack"),
            failed("tx", "boom")
        ]
    );
    let (frozen, _) = h.call("freeze", json!({"kind":"freeze"}));
    assert_eq!(frozen, done("freeze", Value::Null));

    let dir = tempfile::tempdir().unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let store = FailingCommit {
        inner: SqliteStore::open(dir.path().join("db")).unwrap(),
        armed: armed.clone(),
    };
    let mut h = Harness {
        runtime: ClientRuntime::new(Client::open(store, mutation_schema(Value::Null)).unwrap()),
        _dir: dir,
    };
    let a = h.begin("tx");
    h.command("a", &a.transaction, None, ping());
    let call = submitted(&h.run(), "a");
    armed.store(true, Ordering::SeqCst);
    h.callback(&a, true, None);
    assert_eq!(
        h.run(),
        vec![
            call_state(&call, "rolledBack"),
            failed("tx", "injected commit failure")
        ]
    );
    assert_eq!(h.pending(), 0);
    let (frozen, _) = h.call("freeze", json!({"kind":"freeze"}));
    assert_eq!(frozen, done("freeze", Value::Null));
}

/// Where the local unit read -> submit -> local companion delete -> later
/// outer write -> commit is made to fail.
#[derive(Clone, Copy, Debug, PartialEq)]
enum LocalStep {
    Read,
    Submit,
    Companion,
    Callback,
    OuterWrite,
    Commit,
}

/// Spec §10 #1: the read, the submission, its local companion delete, a
/// later outer write and the commit are one local unit. A failure injected
/// at any step, with the remaining steps still issued, leaves none of it:
/// no queue row, before image, local write journal row, record stamp or
/// counter survives, and no call is ever committed. Without a failure the
/// same sequence commits all of it, so the store is writable after each
/// failure and the comparison is not vacuous.
#[test]
fn a_failure_at_any_local_step_leaves_no_call_companion_or_recovery_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let store = FailingCommit {
        inner: SqliteStore::open(dir.path().join("db")).unwrap(),
        armed: armed.clone(),
    };
    let mut h = Harness {
        runtime: ClientRuntime::new(Client::open(store, mutation_schema(Value::Null)).unwrap()),
        _dir: dir,
    };
    h.task("seed", create("draft", "local"));
    assert_eq!(h.run(), vec![done("seed", Value::Null)]);
    let initial = h.recovery_state();
    let edit = |id: &str| {
        json!({"kind":"direct","operation":{"model":"Entry","op":"update",
            "identity":{"id":id},"values":{"text":"edited"}}})
    };
    let steps = [
        LocalStep::Read,
        LocalStep::Submit,
        LocalStep::Companion,
        LocalStep::Callback,
        LocalStep::OuterWrite,
        LocalStep::Commit,
    ];
    for fail in steps.into_iter().map(Some).chain([None]) {
        let at = |step| fail == Some(step);
        let a = h.begin("tx");
        let read_command = if at(LocalStep::Read) {
            json!({"kind":"read","key":{"model":"Nope","identity":{"id":"draft"}}})
        } else {
            read("draft")
        };
        h.command("read", &a.transaction, None, read_command);
        let events = h.run();
        assert_eq!(
            events[0]["ok"],
            !at(LocalStep::Read),
            "{fail:?}: {events:?}"
        );
        let mut call = None;
        if at(LocalStep::Submit) {
            h.command(
                "s",
                &a.transaction,
                None,
                submit_mutation("Publish", json!({"entry":{"id":"p","extra":1}}), true),
            );
            let events = h.run();
            assert_eq!(events.len(), 1, "{events:?}");
            assert_eq!(events[0]["ok"], false, "{events:?}");
        } else {
            let local = h.local("s", &a.transaction, None, publish("p"));
            let companion = if at(LocalStep::Companion) {
                missing_model()
            } else {
                delete("draft")
            };
            h.companion("d", &local, None, companion);
            let events = h.run();
            assert_eq!(
                events[0]["ok"],
                !at(LocalStep::Companion),
                "{fail:?}: {events:?}"
            );
            if at(LocalStep::Callback) {
                h.finish_local(&local, false, Some("boom"));
            } else {
                h.finish_local(&local, true, None);
            }
            let events = h.run();
            if at(LocalStep::Companion) || at(LocalStep::Callback) {
                assert_eq!(events[0]["ok"], false, "{fail:?}: {events:?}");
            } else {
                call = Some(submitted(&events, "s"));
            }
        }
        // The outer write edits the call's Entry, or the Draft when no call
        // was submitted; either way it is a write the rollback must undo.
        let write = if at(LocalStep::OuterWrite) {
            missing_model()
        } else if at(LocalStep::Submit) {
            edit("draft")
        } else {
            edit("p")
        };
        h.command("w", &a.transaction, None, write);
        let events = h.run();
        assert_eq!(
            events[0]["ok"],
            !at(LocalStep::OuterWrite),
            "{fail:?}: {events:?}"
        );
        armed.store(at(LocalStep::Commit), Ordering::SeqCst);
        h.callback(&a, true, None);
        let events = h.run();
        let outcome = events.last().unwrap();
        assert_eq!(outcome["requestId"], "tx");
        let Some(fail) = fail else {
            assert_eq!(outcome["ok"], true, "{events:?}");
            assert_eq!(
                call_states(&events),
                vec![call_state(call.as_ref().unwrap(), "committed")]
            );
            continue;
        };
        assert_eq!(outcome["ok"], false, "{fail:?}: {events:?}");
        // A call that was answered is rolled back; a failed one never had a
        // Call to announce.
        let expected: Vec<Value> = call
            .iter()
            .map(|call| call_state(call, "rolledBack"))
            .collect();
        assert_eq!(call_states(&events), expected, "{fail:?}");
        assert_eq!(
            h.recovery_state(),
            initial,
            "{fail:?} left part of the unit"
        );
        let (frozen, _) = h.call("freeze", json!({"kind":"freeze"}));
        assert_eq!(frozen, done("freeze", Value::Null), "{fail:?}");
    }
    // The unit without a failure: every piece committed together.
    let state = h.recovery_state();
    assert_eq!(state["axton_mutation"].as_array().unwrap().len(), 1);
    assert_eq!(
        h.operations(),
        ["wire Entry p create", "companion Entry draft delete"]
    );
    assert_eq!(
        state["axton_before_Entry"],
        json!([{"id":"draft","text":"local","note":null}])
    );
    let journal = state["axton_local_write"].as_array().unwrap();
    assert_eq!(journal.len(), 1, "{journal:?}");
    assert_eq!(journal[0]["disposition"], "independent");
    assert_eq!(h.entry("draft"), None);
    assert_eq!(h.entry("p").unwrap()["text"], "edited");
    assert_ne!(state["axton_client"], initial["axton_client"]);
}

/// Close while a local callback runs: the session rolls back, both callback
/// effects are cancelled, every provisional call is `rolledBack`, and the
/// parked submission and the transaction fail `client_closed` before the
/// runtime's end.
#[test]
fn close_during_a_local_callback_rolls_back_and_releases_its_submission() {
    let mut h = mutation_harness();
    let a = h.begin("tx");
    h.command("a", &a.transaction, None, ping());
    let call = submitted(&h.run(), "a");
    let local = h.local("s", &a.transaction, None, publish("p"));
    h.companion("queued", &local, None, read("p"));
    h.submit(json!({"type":"close"})).unwrap();
    let events = h.run();
    for cancelled in [&a.effect, &local.effect] {
        assert!(
            events.contains(&json!({"type":"cancelEffect","effectId":cancelled})),
            "{events:?}"
        );
    }
    let rolled = position(&events, |e| *e == call_state(&call, "rolledBack"));
    let parent = position(&events, |e| *e == failed("tx", "client_closed"));
    assert!(rolled < parent, "{events:?}");
    assert!(events.contains(&failed("s", "client_closed")));
    assert!(events.contains(&failed("queued", "client_closed")));
    assert_eq!(events.last(), Some(&json!({"type":"runtimeClosed"})));
    assert_eq!(call_states(&events).len(), 1);
}

/// onStore keeps its local-only capability: a named Mutation is refused by
/// the owner, not by decoding, and a companion token names nothing there.
#[test]
fn a_store_hook_cannot_submit_a_mutation_or_run_a_local_callback() {
    let dir = tempfile::tempdir().unwrap();
    let client = Client::open(
        SqliteStore::open(dir.path().join("db")).unwrap(),
        mutation_schema(Value::Null),
    )
    .unwrap();
    let mut h = Harness {
        runtime: ClientRuntime::with_store_hooks(client, vec!["Entry".into()]).unwrap(),
        _dir: dir,
    };
    h.task(
        "sub",
        json!({"kind":"channel","channel":"feed","subscribed":true}),
    );
    h.run();
    common::acknowledge(h.runtime.client(), &[("feed", 0)]);
    let page = json!({"cursors":{"feed":{"from":0,"to":1,"head":1}},"changes":[
        {"model":"Entry","identity":{"id":"e"},"stamp":1,"state":{"text":"server","note":null}}]});
    h.task("pull", json!({"kind":"pull","page":page}));
    let events = h.run();
    assert_eq!(
        events[0]["operation"]["kind"], "storeCallback",
        "{events:?}"
    );
    let hook = Open {
        effect: events[0]["effectId"].as_str().unwrap().into(),
        transaction: events[0]["operation"]["transactionId"]
            .as_str()
            .unwrap()
            .into(),
    };
    h.command("submit", &hook.transaction, None, ping());
    h.command(
        "local",
        &hook.transaction,
        None,
        submit_mutation(
            "Publish",
            json!({"entry":{"id":"p","text":"x","note":null}}),
            true,
        ),
    );
    h.submit(
        json!({"type":"transactionCommand","requestId":"token","transactionId":hook.transaction,
        "companionId":"c1","command":read("e")}),
    )
    .unwrap();
    h.command("read", &hook.transaction, None, read("e"));
    let events = h.run();
    assert_eq!(
        events,
        vec![
            failed("submit", "store hook cannot submit a Mutation"),
            failed("local", "store hook cannot submit a Mutation"),
            failed("token", CAPABILITY),
            done("read", Value::Null),
        ],
        "no local callback was asked for"
    );
    h.callback(&hook, true, None);
    let events = h.run();
    assert_eq!(events.last().unwrap()["requestId"], "pull");
    assert_eq!(events.last().unwrap()["ok"], false, "{events:?}");
    assert_eq!(h.pending(), 0);
    assert_eq!(h.committed(), None);
}
