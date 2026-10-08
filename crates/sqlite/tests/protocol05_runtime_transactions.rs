pub mod common05;
use axton_client::runtime::{BridgeError, ClientRuntime, Input};
use axton_client::*;
use axton_sqlite::SqliteStore;
use common05::{key, schema};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
const NOW: u64 = 1000;
const ENTROPY: u64 = 7;
struct Harness<S: ClientStore + 'static> {
    runtime: ClientRuntime<S>,
    _dir: tempfile::TempDir,
}
struct Open {
    effect: String,
    transaction: String,
}
struct FailingCommit {
    inner: SqliteStore,
    armed: Arc<AtomicBool>,
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
    fn read_tables(&mut self, sql: &str) -> Result<std::collections::BTreeSet<String>> {
        self.inner.read_tables(sql)
    }
}
struct LocalCallback {
    effect: String,
    transaction: String,
    companion: String,
    input: Value,
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
        let input = command["args"].clone();
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
            input,
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
            "transactionId":local.transaction,"companionId":local.companion,"ok":ok,"input":local.input});
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
            .read_sql("SELECT COUNT(*) AS n FROM axton_mutation_queue WHERE reconciled=0 AND rejection_code IS NULL", &[])
            .unwrap()[0]["n"]
            .as_u64()
            .unwrap()
    }
    /// The committed queued operations as `kind model id op`, in order.
    fn operations(&mut self) -> Vec<String> {
        self.runtime
            .client()
            .read_sql(
                "SELECT o.kind,o.model,o.identity,o.operation AS op FROM axton_mutation_queue_operation o JOIN axton_mutation_queue q ON q.id=o.mutation_id WHERE o.model IS NOT NULL AND q.reconciled=0 AND q.rejection_code IS NULL ORDER BY o.mutation_id,o.step",
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
                "SELECT * FROM axton_mutation_queue ORDER BY id",
            ),
            (
                "axton_mutation_operation",
                "SELECT * FROM axton_mutation_queue_operation ORDER BY mutation_id,step",
            ),
            (
                "axton_local_write",
                "SELECT * FROM axton_local_write ORDER BY sequence",
            ),
            (
                "axton_rejection",
                "SELECT id,rejection_code,rejection_message FROM axton_mutation_queue ORDER BY id",
            ),
            (
                "axton_record",
                "SELECT * FROM axton_authority ORDER BY model, identity",
            ),
            (
                "axton_client",
                "SELECT next_mutation_id,last_acknowledged_batch_id,next_local_sequence FROM axton_store",
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
fn harness() -> Harness<SqliteStore> {
    let dir = tempfile::tempdir().unwrap();
    let client = Client::open05(
        SqliteStore::open(dir.path().join("db")).unwrap(),
        schema(),
        "User:u",
    )
    .unwrap();
    Harness {
        runtime: ClientRuntime::new(client),
        _dir: dir,
    }
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
fn position(events: &[Value], matches: impl Fn(&Value) -> bool) -> usize {
    events
        .iter()
        .position(matches)
        .unwrap_or_else(|| panic!("not found in {events:?}"))
}
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
    let client = Client::open05(
        SqliteStore::open(dir.path().join("db")).unwrap(),
        mutation_schema(Value::Null),
        "User:u",
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
fn submitted(events: &[Value], id: &str) -> Value {
    let completion = &events[position(events, |e| {
        e["type"] == "taskCompleted" && e["requestId"] == id
    })];
    assert_eq!(completion["ok"], true, "{completion}");
    completion["value"]["callId"].clone()
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
    assert!(!scope.is_empty());
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
    let mut other = Client::open05(SqliteStore::open(&dir).unwrap(), schema(), "User:u").unwrap();
    let _ = other.generation();
    assert!(other.read(&key()).unwrap().is_none());
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
        runtime: ClientRuntime::new(Client::open05(store, schema(), "User:u").unwrap()),
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
fn a_named_mutation_submitted_in_a_callback_is_provisional_until_the_commit() {
    let mut h = mutation_harness();
    let a = h.begin("tx");
    h.command("s", &a.transaction, None, ping());
    let events = h.run();
    let call = submitted(&events, "s");
    assert!(call.is_string());
    assert_eq!(events, vec![done("s", json!({"callId":call,"ordinal":1}))]);
    assert_eq!(h.pending(), 0, "nothing is durable before the commit");
    h.task("barrier", json!({"kind":"status"}));
    assert_eq!(
        h.run(),
        Vec::<Value>::new(),
        "a status task waits for the writer"
    );
    h.callback(&a, true, None);
    let events = h.run();
    assert_eq!(events.len(), 3, "{events:?}");
    assert_eq!(events[0], call_state(&call, "committed"));
    assert_eq!(events[1], done("tx", Value::Null));
    assert_eq!(events[2]["requestId"], "barrier");
    assert_eq!(events[2]["ok"], true);
    let batch = h.runtime.client().freeze_batch05().unwrap().unwrap();
    assert_eq!(batch.mutations[0].id, 1);
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
        vec![done("r", Value::Null), done("d", Value::Null)]
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
        ["companion Entry draft delete", "wire Entry p create"]
    );
    let batch = h.runtime.client().freeze_batch05().unwrap().unwrap();
    let body = String::from_utf8(axton_core::v05::encode(&batch).unwrap()).unwrap();
    assert!(!body.contains("draft"), "a companion is never sent: {body}");
}

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
            "scope",
            json!({"kind":"stream","stream":"book","subscribed":true}),
        ),
        ("discard", json!({"kind":"discard","ordinal":1})),
        ("dismiss", json!({"kind":"dismiss","ordinal":1})),
        ("retry", json!({"kind":"retryTasks","keys":[]})),
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
        if ["enqueue", "scope"].contains(id) {
            assert_eq!(
                events[position(&events, |e| e["requestId"] == *id)]["ok"],
                false
            );
        } else {
            assert!(events.contains(&failed(id, CAPABILITY)), "{id}: {events:?}");
        }
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
    let failed_local = h.run();
    assert_eq!(failed_local[0]["ok"], false);
    assert!(
        failed_local[0]["error"]
            .as_str()
            .unwrap()
            .starts_with("unknown variant `enqueue`")
    );
    // Once it finished, its token has expired.
    h.companion("expired", &local, None, read("p"));
    assert_eq!(h.run(), vec![failed("expired", CAPABILITY)]);
    h.callback(&a, true, None);
    assert_eq!(h.run(), vec![failed("tx", CAPABILITY)]);
    assert_eq!(h.pending(), 0);
    assert_eq!(h.entry("x"), None);
}

#[test]
fn a_local_callback_command_outside_its_scope_fails_its_submission_and_the_transaction() {
    let mut h = mutation_harness();
    h.task("seed", create("draft", "local"));
    assert_eq!(h.run(), vec![done("seed", Value::Null)]);
    let a = h.begin("tx");
    h.command("sp", &a.transaction, None, json!({"kind":"savepoint"}));
    let scope = h.run()[0]["value"]["scope"].as_str().unwrap().to_string();
    let local = h.local("s", &a.transaction, Some(&scope), publish("p"));
    // The outer scope, then a scope that was never opened.
    h.companion("outer", &local, None, delete("draft"));
    h.companion("unknown", &local, Some("sp999"), read("draft"));
    assert_eq!(
        h.run(),
        vec![
            failed("outer", "invalid transaction scope"),
            failed("unknown", "invalid transaction scope")
        ]
    );
    h.finish_local(&local, true, None);
    assert_eq!(h.run(), vec![failed("s", "invalid transaction scope")]);
    h.command(
        "rb",
        &a.transaction,
        Some(&scope),
        json!({"kind":"rollbackSavepoint","scope":scope}),
    );
    assert_eq!(h.run(), vec![done("rb", Value::Null)]);
    h.callback(&a, true, None);
    assert_eq!(h.run(), vec![failed("tx", "invalid transaction scope")]);
    assert_eq!(h.pending(), 0);
    assert_eq!(h.entry("draft").unwrap()["text"], "local");
}

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
    h.callback(&a, false, Some("boom"));
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
    h.callback(&a, false, Some(&error));
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
    h.callback(&a, false, Some("unawaited transaction operation"));
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
        .read_sql("SELECT (SELECT id FROM axton_store)||':'||id AS call_id FROM axton_mutation_queue WHERE reconciled=0 AND rejection_code IS NULL ORDER BY id", &[])
        .unwrap()
        .into_iter()
        .map(|row| row["call_id"].clone())
        .collect();
    assert_eq!(calls, vec![first, fourth]);
}

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
    assert!(h.runtime.client().freeze_batch05().unwrap().is_none());

    let dir = tempfile::tempdir().unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let store = FailingCommit {
        inner: SqliteStore::open(dir.path().join("db")).unwrap(),
        armed: armed.clone(),
    };
    let mut h = Harness {
        runtime: ClientRuntime::new(
            Client::open05(store, mutation_schema(Value::Null), "User:u").unwrap(),
        ),
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
    assert!(h.runtime.client().freeze_batch05().unwrap().is_none());
}

#[test]
fn a_failure_at_any_local_step_leaves_no_call_companion_or_recovery_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let store = FailingCommit {
        inner: SqliteStore::open(dir.path().join("db")).unwrap(),
        armed: armed.clone(),
    };
    let mut h = Harness {
        runtime: ClientRuntime::new(
            Client::open05(store, mutation_schema(Value::Null), "User:u").unwrap(),
        ),
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
                submit_mutation("Publish", json!({"entry":{"id":"p","extra":1}}), false),
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
        } else if call.is_none() {
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
        let body_failed = fail.is_some() && !at(LocalStep::Commit);
        h.callback(&a, !body_failed, body_failed.then_some("boom"));
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
        assert!(
            h.runtime.client().freeze_batch05().unwrap().is_none(),
            "{fail:?}"
        );
    }
    // The unit without a failure: every piece committed together.
    let state = h.recovery_state();
    assert_eq!(state["axton_mutation"].as_array().unwrap().len(), 1);
    assert_eq!(
        h.operations(),
        ["companion Entry draft delete", "wire Entry p create"]
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
const CAPABILITY: &str = "invalid transaction capability";

#[derive(Clone, Copy, Debug, PartialEq)]
enum LocalStep {
    Read,
    Submit,
    Companion,
    Callback,
    OuterWrite,
    Commit,
}

fn recovery_schema() -> Schema {
    let mut raw = serde_json::to_value(mutation_schema(Value::Null)).unwrap();
    raw["actions"].as_array_mut().unwrap().push(json!({"name":"Edit","version":1,"inputs":[{"kind":"model","name":"entry","model":"Entry","operation":"update","cardinality":"single","fields":["text"]}],"outputs":[]}));
    raw["prerequisites"] = json!([{"name":"Media","fields":[{"name":"key","type":"String"}]}]);
    raw["requirements"] =
        json!([{"model":"Entry","field":"note","name":"Media","arguments":{"key":"self"}}]);
    Schema::from_value(raw).unwrap()
}
fn recovery_harness() -> (
    Harness<FailingCommit>,
    Arc<AtomicBool>,
    SubmittedCall,
    SubmittedCall,
    SubmittedCall,
    String,
) {
    let dir = tempfile::tempdir().unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let mut c = Client::open05(
        FailingCommit {
            inner: SqliteStore::open(dir.path().join("db")).unwrap(),
            armed: armed.clone(),
        },
        recovery_schema(),
        "User:u",
    )
    .unwrap();
    c.initialize_stream05(0).unwrap();
    let owner = c
        .transaction(|tx| {
            tx.submit_mutation05(
                "Publish",
                1,
                json!({"entry":{"id":"e","text":"draft","note":"asset"}}),
                vec![],
            )
        })
        .unwrap();
    let child = c
        .transaction(|tx| {
            tx.submit_mutation05(
                "Edit",
                1,
                json!({"entry":{"id":"e","text":"edited"}}),
                vec![],
            )
        })
        .unwrap();
    let refused = c
        .transaction(|tx| tx.submit_mutation05("Ping", 1, json!({}), vec![]))
        .unwrap();
    let b = c.freeze_batch05().unwrap().unwrap();
    assert_eq!(b.mutations.len(), 1);
    c.acknowledge_batch05(&v05::BatchAcknowledgement {
        context: b.context,
        batch_id: b.batch_id,
        digest: b.digest,
        results: vec![v05::MutationResult {
            mutation_id: refused.ordinal,
            outcome: v05::MutationOutcome::Rejected {
                code: "ping.denied".into(),
                message: None,
            },
        }],
    })
    .unwrap();
    let task = c.pending_tasks().unwrap().remove(0)["key"]
        .as_str()
        .unwrap()
        .to_owned();
    c.outcome(&task, Some("file is gone")).unwrap();
    let runtime = ClientRuntime::new(c)
        .register_prerequisite_handlers(vec!["Media".into()])
        .unwrap();
    (
        Harness { runtime, _dir: dir },
        armed,
        owner,
        child,
        refused,
        task,
    )
}
fn unsent_events(events: &[Value]) -> Vec<Value> {
    events
        .iter()
        .filter(|e| e["type"] == "observerChanged")
        .cloned()
        .collect()
}
fn resolution_events(events: &[Value]) -> Vec<Value> {
    events
        .iter()
        .filter(|e| e["type"] == "callCompleted" || e["operation"]["kind"] == "prerequisite")
        .cloned()
        .collect()
}
fn observe_recovery<S: ClientStore + 'static>(h: &mut Harness<S>) -> Vec<String> {
    let mut ids = vec![];
    for view in ["pending", "failures", "rejections"] {
        let (completed, events) = h.call(
            &format!("watch-{view}"),
            json!({"kind":"unsentWatch","view":view}),
        );
        let id = completed["value"]["observerId"]
            .as_str()
            .unwrap()
            .to_owned();
        assert_eq!(
            events[0], completed,
            "registration completion precedes first snapshot"
        );
        assert_eq!(events.len(), 2, "{events:?}");
        assert_eq!(events[1]["observerId"], id);
        ids.push(id);
    }
    ids
}
#[test]
fn receipt_commit_publishes_retained_unsent_and_model_observers_together() {
    use axton_client::sync05::{StoreCommand, StoreReport};
    let mut h = mutation_harness();
    h.runtime
        .store_worker05(StoreCommand::Initialize(0))
        .unwrap();
    h.events();
    let ids = observe_recovery(&mut h);
    let (watch, _) = h.call("models", json!({"kind":"watch","model":"Entry"}));
    let watch = watch["value"]["observerId"].clone();
    let (sql, _) = h.watch_sql("sql", "SELECT id,text,note FROM Entry", json!([]));
    let (_, initial) = h.call(
        "subscribe",
        json!({"kind":"streamSubscribe","stream":"User:u"}),
    );
    assert!(
        initial
            .iter()
            .any(|e| e["snapshot"]["status"]["initialization"] == "ready")
    );

    let (submitted, events) = h.call(
        "publish",
        json!({"kind":"submitAction","name":"Publish","version":1,
        "args":{"entry":{"id":"e","text":"optimistic","note":null}}}),
    );
    assert_eq!(submitted["ok"], true);
    assert!(
        events
            .iter()
            .any(|e| e["observerId"] == ids[0] && e["snapshot"]["count"] == 1)
    );
    assert_eq!(rows(&events, &watch), [json!([row("optimistic")])]);
    assert_eq!(rows(&events, &sql), [json!([row("optimistic")])]);
    let StoreReport::Frozen(Some(batch)) = h.runtime.store_worker05(StoreCommand::Freeze).unwrap()
    else {
        panic!("expected frozen Mutation");
    };
    assert!(h.events().is_empty(), "freeze changes no observer result");
    let receipt = v05::BatchAcknowledgement {
        context: batch.context,
        batch_id: batch.batch_id,
        digest: batch.digest,
        results: vec![v05::MutationResult {
            mutation_id: batch.mutations[0].id,
            outcome: v05::MutationOutcome::Rejected {
                code: "action.invalid".into(),
                message: None,
            },
        }],
    };
    h.runtime
        .store_worker05(StoreCommand::Acknowledge(receipt.clone()))
        .unwrap();
    let events = h.events();
    assert_eq!(h.pending(), 0, "receipt is durably settled");
    assert_eq!(h.runtime.client().refused_acts05().unwrap().len(), 1);
    assert_eq!(rows(&events, &watch), [json!([])], "optimism rolls back");
    assert_eq!(rows(&events, &sql), [json!([])]);
    let pending = events
        .iter()
        .filter(|e| e["observerId"] == ids[0])
        .collect::<Vec<_>>();
    assert_eq!(pending.len(), 1, "retained pending observer: {events:?}");
    assert_eq!(pending[0]["snapshot"]["count"], 0);
    let refused = events
        .iter()
        .filter(|e| e["observerId"] == ids[2])
        .collect::<Vec<_>>();
    assert_eq!(refused.len(), 1, "retained refusal observer: {events:?}");
    assert_eq!(refused[0]["snapshot"]["items"][0]["code"], "action.invalid");
    assert_eq!(
        refused[0]["snapshot"]["items"][0]["act"]["args"]["entry"]["text"],
        "optimistic"
    );
    assert!(
        !events
            .iter()
            .any(|e| e["snapshot"]["kind"] == "subscription" || e["observerId"] == ids[1]),
        "unchanged status and failures publish no duplicates"
    );
    assert!(
        position(&events, |e| e["type"] == "callCompleted")
            < position(&events, |e| e["observerId"] == ids[0])
    );

    h.runtime
        .store_worker05(StoreCommand::Acknowledge(receipt))
        .unwrap();
    assert!(
        unsent_events(&h.events()).is_empty(),
        "replayed receipt publishes no duplicate"
    );
    let (_, events) = h.call(
        "dismiss",
        json!({"kind":"dismiss","ordinal":submitted["value"]["ordinal"]}),
    );
    assert!(
        events
            .iter()
            .any(|e| e["observerId"] == ids[2] && e["snapshot"]["items"] == json!([])),
        "ordinary commits still publish retained refusals"
    );
}

#[test]
fn accepted_receipt_commit_publishes_retained_pending_observer() {
    use axton_client::sync05::{StoreCommand, StoreReport};
    let mut h = mutation_harness();
    h.runtime
        .store_worker05(StoreCommand::Initialize(0))
        .unwrap();
    h.events();
    let context = h.runtime.client().request_context05().unwrap();
    let delivery = v05::freeze_delivery(
        context.clone(),
        "bootstrap".into(),
        v05::DeliveryPurpose::Bootstrap,
        0,
        0,
        0,
        10000,
        vec![v05::DeliveryUnit {
            index: 0,
            through: Some(0),
            changes: vec![],
        }],
        1,
    )
    .unwrap();
    let mut queue = axton_client::sync05::DeliveryQueue::new(1024 * 1024, 2);
    queue
        .receive(&delivery.header, &delivery.parts, &context, NOW)
        .unwrap();
    h.runtime
        .store_worker05(StoreCommand::Apply {
            plan_id: "bootstrap".into(),
            queue,
            now: NOW,
        })
        .unwrap();
    h.events();
    let ids = observe_recovery(&mut h);
    let (submitted, _) = h.call(
        "ping",
        json!({"kind":"submitAction","name":"Ping","version":1,"args":{}}),
    );
    assert_eq!(submitted["ok"], true);
    let StoreReport::Frozen(Some(batch)) = h.runtime.store_worker05(StoreCommand::Freeze).unwrap()
    else {
        panic!("expected frozen Mutation");
    };
    h.events();
    h.runtime
        .store_worker05(StoreCommand::Acknowledge(v05::BatchAcknowledgement {
            context: batch.context,
            batch_id: batch.batch_id,
            digest: batch.digest,
            results: vec![v05::MutationResult {
                mutation_id: batch.mutations[0].id,
                outcome: v05::MutationOutcome::Accepted {
                    sync_cursor: 0,
                    result: Value::Null,
                    targets: vec![],
                },
            }],
        }))
        .unwrap();
    assert!(
        unsent_events(&h.events()).is_empty(),
        "acceptance awaits settlement"
    );
    h.runtime.store_worker05(StoreCommand::Needs).unwrap();
    let events = h.events();
    assert_eq!(h.pending(), 0);
    assert_eq!(
        unsent_events(&events),
        vec![json!({"type":"observerChanged","observerId":ids[0],
        "snapshot":{"kind":"pending","count":0}})]
    );
}
#[test]
fn unsent_observers_publish_initial_then_changed_inputs_and_end_on_unwatch_or_close() {
    let (mut h, _armed, owner, child, refused, _key) = recovery_harness();
    let ids = observe_recovery(&mut h);
    assert_eq!(
        h.runtime.client().failed_acts().unwrap()[0].act.args,
        Some(json!({"entry":{"id":"e","text":"draft","note":"asset"}}))
    );
    assert_eq!(
        h.runtime.client().refused_acts05().unwrap()[0].id,
        refused.ordinal
    );
    let (_, events) = h.call("unrelated", create("unrelated", "local"));
    assert!(
        unsent_events(&events).is_empty(),
        "unchanged views publish no duplicate"
    );
    let (_, events) = h.call("discard", json!({"kind":"discard","ordinal":owner.ordinal}));
    let completions = resolution_events(&events);
    assert_eq!(completions.len(), 2, "{events:?}");
    assert!(completions.iter().any(|e| e["callId"] == owner.call_id));
    assert!(completions.iter().any(|e| e["callId"] == child.call_id));
    assert_eq!(
        unsent_events(&events).len(),
        3,
        "pending/failures/refusals all changed"
    );
    let rejections = unsent_events(&events)
        .into_iter()
        .find(|e| e["snapshot"]["kind"] == "rejections")
        .unwrap();
    assert_eq!(
        rejections["snapshot"]["items"][0]["act"]["args"],
        json!({"entry":{"id":"e","text":"edited"}})
    );
    h.call("unwatch", json!({"kind":"unwatch","observerId":ids[2]}));
    let (_, events) = h.call("dismiss", json!({"kind":"dismiss","ordinal":child.ordinal}));
    assert!(
        unsent_events(&events).is_empty(),
        "unwatched refusal stream stops"
    );
    h.submit(json!({"type":"close"})).unwrap();
    let events = h.run();
    let terminal = unsent_events(&events);
    assert_eq!(terminal.len(), 2);
    assert!(terminal.iter().all(|e| e["snapshot"]["closed"] == true));
    assert_eq!(events.last().unwrap()["type"], "runtimeClosed");
}
#[test]
fn transactional_discard_and_dismiss_change_local_reads_but_announce_only_after_commit() {
    let (mut h, _armed, owner, child, refused, _key) = recovery_harness();
    observe_recovery(&mut h);
    let open = h.begin("tx-discard");
    h.command(
        "discard",
        &open.transaction,
        None,
        json!({"kind":"discard","ordinal":owner.ordinal}),
    );
    h.command(
        "dismiss",
        &open.transaction,
        None,
        json!({"kind":"dismiss","ordinal":refused.ordinal}),
    );
    h.command("local-read", &open.transaction, None, read("e"));
    let events = h.run();
    assert!(resolution_events(&events).is_empty());
    assert!(unsent_events(&events).is_empty());
    assert_eq!(events.last().unwrap(), &done("local-read", Value::Null));
    assert!(
        h.entry("e").is_some(),
        "committed reader keeps old optimism"
    );
    assert!(
        h.runtime
            .client()
            .call_completion05(&owner.call_id)
            .unwrap()
            .is_none()
    );
    h.callback(&open, true, None);
    let events = h.run();
    assert_eq!(resolution_events(&events).len(), 2, "{events:?}");
    assert_eq!(unsent_events(&events).len(), 3);
    assert_eq!(h.entry("e"), None);
    assert_eq!(
        h.runtime
            .client()
            .refused_acts05()
            .unwrap()
            .iter()
            .map(|r| r.id)
            .collect::<Vec<_>>(),
        vec![child.ordinal]
    );
    assert!(
        h.runtime
            .client()
            .call_completion05(&owner.call_id)
            .unwrap()
            .is_some()
    );
    assert!(
        h.runtime
            .client()
            .call_completion05(&child.call_id)
            .unwrap()
            .is_some()
    );
}
#[test]
fn transactional_retry_changes_task_reads_but_handler_starts_only_after_commit() {
    let (mut h, _armed, _owner, _child, _refused, key) = recovery_harness();
    observe_recovery(&mut h);
    let open = h.begin("tx-retry");
    h.command(
        "retry",
        &open.transaction,
        None,
        json!({"kind":"retryTasks","keys":[key]}),
    );
    h.command("local-task",&open.transaction,None,json!({"kind":"sql","sql":"SELECT error FROM axton_mutation_prerequisite WHERE key=?","parameters":[key]}));
    let events = h.run();
    assert!(resolution_events(&events).is_empty());
    assert!(unsent_events(&events).is_empty());
    assert_eq!(events.last().unwrap()["value"][0]["error"], Value::Null);
    assert_eq!(
        h.runtime.client().pending_tasks().unwrap()[0]["state"],
        "failed"
    );
    h.callback(&open, true, None);
    let events = h.run();
    let effects = resolution_events(&events);
    assert_eq!(effects.len(), 1, "{events:?}");
    assert_eq!(effects[0]["operation"]["kind"], "prerequisite");
    assert_eq!(effects[0]["operation"]["arguments"], json!({"key":"asset"}));
    assert_eq!(
        unsent_events(&events).len(),
        1,
        "failed-input view clears after commit"
    );
    h.submit(json!({"type":"effectResult","effectId":effects[0]["effectId"],"outcome":{"ok":false,"error":{"message":"missing asset","retry":false}}})).unwrap();
    let failed = h.run();
    assert!(
        resolution_events(&failed).is_empty(),
        "terminal failure does not retry"
    );
    let changed = unsent_events(&failed);
    assert_eq!(changed.len(), 1, "{failed:?}");
    assert_eq!(
        changed[0]["snapshot"]["items"][0]["act"]["args"],
        json!({"entry":{"id":"e","text":"draft","note":"asset"}})
    );
    assert_eq!(
        changed[0]["snapshot"]["items"][0]["tasks"][0]["error"],
        "missing asset"
    );
    let (_, events) = h.call("retry-again", json!({"kind":"retryTasks","keys":[key]}));
    assert_eq!(
        resolution_events(&events).len(),
        1,
        "explicit retry runs handler again"
    );
    assert_eq!(
        unsent_events(&events).len(),
        1,
        "failure snapshot clears after committed retry"
    );
}
#[test]
fn unsent_resolutions_roll_back_at_savepoint_outer_failure_failed_commit_and_priority_close() {
    for mode in ["savepoint", "rollback", "failed-commit", "close"] {
        let (mut h, armed, owner, child, refused, key) = recovery_harness();
        observe_recovery(&mut h);
        let original = h.recovery_state();
        let open = h.begin("resolution-tx");
        let scope = if mode == "savepoint" {
            h.command(
                "savepoint",
                &open.transaction,
                None,
                json!({"kind":"savepoint"}),
            );
            Some(h.run()[0]["value"]["scope"].as_str().unwrap().to_owned())
        } else {
            None
        };
        h.command(
            "retry",
            &open.transaction,
            scope.as_deref(),
            json!({"kind":"retryTasks","keys":[key]}),
        );
        h.command(
            "discard",
            &open.transaction,
            scope.as_deref(),
            json!({"kind":"discard","ordinal":owner.ordinal}),
        );
        h.command(
            "dismiss",
            &open.transaction,
            scope.as_deref(),
            json!({"kind":"dismiss","ordinal":refused.ordinal}),
        );
        let events = h.run();
        assert!(
            events
                .iter()
                .all(|e| e["type"] == "taskCompleted" && e["ok"] == true),
            "{mode}: {events:?}"
        );
        let events = match mode {
            "savepoint" => {
                h.command(
                    "undo",
                    &open.transaction,
                    scope.as_deref(),
                    json!({"kind":"rollbackSavepoint","scope":scope}),
                );
                assert_eq!(h.run(), vec![done("undo", Value::Null)]);
                h.callback(&open, true, None);
                h.run()
            }
            "rollback" => {
                h.callback(&open, false, Some("outer failed"));
                h.run()
            }
            "failed-commit" => {
                armed.store(true, Ordering::SeqCst);
                h.callback(&open, true, None);
                h.run()
            }
            "close" => {
                h.submit(json!({"type":"close"})).unwrap();
                h.run()
            }
            _ => unreachable!(),
        };
        assert!(resolution_events(&events).is_empty(), "{mode}: {events:?}");
        assert!(
            unsent_events(&events)
                .iter()
                .all(|e| e["snapshot"]["closed"] == true),
            "only close ends observers: {mode}: {events:?}"
        );
        assert_eq!(
            h.recovery_state(),
            original,
            "{mode}: all durable state unchanged"
        );
        assert!(
            h.runtime
                .client()
                .call_completion05(&owner.call_id)
                .unwrap()
                .is_none()
        );
        assert!(
            h.runtime
                .client()
                .call_completion05(&child.call_id)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            h.runtime.client().pending_tasks().unwrap()[0]["state"],
            "failed"
        );
    }
}

fn observer_harness() -> Harness<SqliteStore> {
    let mut raw = serde_json::to_value(mutation_schema(Value::Null)).unwrap();
    let text = |name: &str, nullable: bool| json!({"name":name,"nullable":nullable,"type":{"kind":"scalar","name":"string"}});
    let models = raw["models"].as_array_mut().unwrap();
    models.push(json!({"name":"Media","identity":["id"],"fields":[text("id",false),text("entryId",false),text("url",false),text("caption",true)]}));
    models.push(json!({"name":"Person","identity":["id"],"fields":[text("id",false),text("entryId",false),text("name",false)]}));
    models.push(
        json!({"name":"Note","identity":["id"],"fields":[text("id",false),text("body",false)]}),
    );
    let dir = tempfile::tempdir().unwrap();
    Harness {
        runtime: ClientRuntime::new(
            Client::open05(
                SqliteStore::open(dir.path().join("db")).unwrap(),
                Schema::from_value(raw).unwrap(),
                "User:u",
            )
            .unwrap(),
        ),
        _dir: dir,
    }
}
fn sql_snapshots(events: &[Value], observer: &Value) -> Vec<Value> {
    events
        .iter()
        .filter(|e| e["type"] == "observerChanged" && e["observerId"] == *observer)
        .map(|e| e["snapshot"].clone())
        .collect()
}
const JOURNAL: &str = "SELECT e.id AS entry, e.text AS text, m.url AS media, p.name AS person \
    FROM Entry e JOIN Media m ON m.entryId = e.id JOIN Person p ON p.entryId = e.id ORDER BY e.id";
/// Every re-run of this statement publishes: its `random()` column differs.
const PROBE: &str = "SELECT count(*) AS n, random() AS r FROM Entry";
fn put(model: &str, id: &str, values: Value) -> Value {
    json!({"kind":"direct","operation":{"model":model,"op":"create","identity":{"id":id},"values":values}})
}
fn change(model: &str, id: &str, values: Value) -> Value {
    json!({"kind":"direct","operation":{"model":model,"op":"update","identity":{"id":id},"values":values}})
}
fn watch_sql(sql: &str, parameters: Value) -> Value {
    json!({"kind":"watchSql","sql":sql,"parameters":parameters})
}
/// The rows of every snapshot `observer` published, in order.
fn rows(events: &[Value], observer: &Value) -> Vec<Value> {
    sql_snapshots(events, observer)
        .into_iter()
        .map(|snapshot| snapshot["rows"].clone())
        .collect()
}
fn journal(entry: &str, text: &str, media: &str, person: &str) -> Value {
    json!({"entry":entry,"text":text,"media":media,"person":person})
}

impl<S: ClientStore + 'static> Harness<S> {
    fn completion<'a>(&self, events: &'a [Value], id: &str) -> &'a Value {
        events
            .iter()
            .find(|e| e["type"] == "taskCompleted" && e["requestId"] == id)
            .unwrap()
    }
    fn watch_sql(&mut self, id: &str, sql: &str, parameters: Value) -> (Value, Vec<Value>) {
        let (done, events) = self.call(id, watch_sql(sql, parameters));
        let observer = done["value"]["observerId"].clone();
        assert!(observer.is_string(), "{events:?}");
        assert!(
            position(&events, |e| e["requestId"] == id)
                < position(&events, |e| e["observerId"] == observer)
        );
        (observer, events)
    }
    fn commit(&mut self, id: &str, command: Value) -> Vec<Value> {
        let (done, events) = self.call(id, command);
        assert_eq!(done["ok"], true, "{done}");
        events
    }
}

#[test]
fn a_join_over_three_models_re_emits_after_a_commit_to_each_and_nothing_else() {
    let mut h = observer_harness();
    h.commit("entry", put("Entry", "e", json!({"text":"first"})));
    h.commit(
        "media",
        put("Media", "m", json!({"entryId":"e","url":"a.jpg"})),
    );
    h.commit(
        "person",
        put("Person", "p", json!({"entryId":"e","name":"Ann"})),
    );
    let (page, events) = h.watch_sql("journal", JOURNAL, json!([]));
    assert_eq!(
        rows(&events, &page),
        [json!([journal("e", "first", "a.jpg", "Ann")])]
    );
    let (probe, events) = h.watch_sql("probe", PROBE, json!([]));
    assert_eq!(rows(&events, &probe).len(), 1);

    // A commit to each joined Model re-emits the join.
    let events = h.commit("text", change("Entry", "e", json!({"text":"second"})));
    assert_eq!(
        rows(&events, &page),
        [json!([journal("e", "second", "a.jpg", "Ann")])]
    );
    assert_eq!(rows(&events, &probe).len(), 1, "Entry is the probe's table");
    let events = h.commit("url", change("Media", "m", json!({"url":"b.jpg"})));
    assert_eq!(
        rows(&events, &page),
        [json!([journal("e", "second", "b.jpg", "Ann")])]
    );
    assert!(
        rows(&events, &probe).is_empty(),
        "Media is not read by the probe"
    );
    let events = h.commit("name", change("Person", "p", json!({"name":"Bea"})));
    assert_eq!(
        rows(&events, &page),
        [json!([journal("e", "second", "b.jpg", "Bea")])]
    );
    assert!(rows(&events, &probe).is_empty());

    // An unrelated Model, a Scope registration and a queued call write no
    // table either statement reads: neither re-runs.
    let generation = h.runtime.client().generation();
    let events = h.commit("note", put("Note", "n", json!({"body":"aside"})));
    let events = [
        events,
        h.commit(
            "queued",
            json!({"kind":"submitAction","name":"Ping","version":1,"args":{}}),
        ),
    ]
    .concat();
    assert!(h.runtime.client().generation() >= generation + 2);
    assert!(rows(&events, &page).is_empty() && rows(&events, &probe).is_empty());
    // A commit to a joined table that leaves the answer unchanged re-runs the
    // join and publishes nothing.
    let events = h.commit(
        "caption",
        change("Media", "m", json!({"caption":"unselected"})),
    );
    assert!(rows(&events, &page).is_empty(), "{events:?}");
}

#[test]
fn a_watched_statement_refuses_writes_and_engine_tables_and_lives_like_a_watch() {
    let mut h = harness();
    for (id, sql) in [
        ("write", "DELETE FROM Entry RETURNING id"),
        ("pragma", "PRAGMA table_info(Entry)"),
        ("engine", "SELECT count(*) AS n FROM axton_mutation_queue"),
        (
            "before",
            "SELECT e.id FROM Entry e JOIN axton_before_Entry b ON b.id = e.id",
        ),
        ("missing", "SELECT id FROM Nope"),
    ] {
        h.task(id, watch_sql(sql, json!([])));
        let events = h.run();
        assert_eq!(h.completion(&events, id)["ok"], false, "{sql}");
        assert!(
            !events.iter().any(|e| e["type"] == "observerChanged"),
            "{events:?}"
        );
    }

    h.commit("seed", create("e", "first"));
    let (one, events) = h.watch_sql("one", "SELECT text FROM Entry WHERE id = ?", json!(["e"]));
    assert_eq!(rows(&events, &one), [json!([{"text":"first"}])]);
    // Fails to run while exactly two Entries exist.
    let (fragile, events) = h.watch_sql(
        "fragile",
        "SELECT CASE WHEN count(*) = 2 THEN abs(-9223372036854775807 - 1) ELSE count(*) END AS n FROM Entry",
        json!([]),
    );
    assert_eq!(rows(&events, &fragile), [json!([{"n":1}])]);
    let events = h.commit("second", create("f", "second"));
    assert_eq!(
        events
            .iter()
            .filter(|e| e["type"] == "report" && e["diagnostic"]["kind"] == "error")
            .count(),
        1,
        "{events:?}"
    );
    assert!(rows(&events, &fragile).is_empty() && rows(&events, &one).is_empty());
    let events = h.commit("third", create("g", "third"));
    assert_eq!(
        rows(&events, &fragile),
        [json!([{"n":3}])],
        "the watch stayed"
    );

    // A callback's write is not visible until it commits.
    h.task("tx", json!({"kind":"transaction"}));
    let events = h.run();
    let callback = events
        .iter()
        .find(|e| e["type"] == "effect" && e["operation"]["kind"] == "callback")
        .unwrap()
        .clone();
    let transaction = callback["operation"]["transactionId"].clone();
    h.submit(json!({"type":"transactionCommand","requestId":"write","transactionId":transaction,"command":{"kind":"direct","operation":{"model":"Entry","op":"update","identity":{"id":"e"},"values":{"text":"inside"}}}})).unwrap();
    let events = h.run();
    assert!(events.contains(&done("write", Value::Null)));
    assert!(
        !events.iter().any(|e| e["type"] == "observerChanged"),
        "{events:?}"
    );
    h.submit(json!({"type":"callbackResult","effectId":callback["effectId"],"transactionId":transaction,"ok":true})).unwrap();
    let events = h.run();
    assert!(
        position(&events, |e| *e == done("tx", Value::Null))
            < position(&events, |e| e["observerId"] == one)
    );
    assert_eq!(rows(&events, &one), [json!([{"text":"inside"}])]);

    // unwatch: nothing more for that observer; the other goes on.
    h.task("stop", json!({"kind":"unwatch","observerId":one}));
    let events = [h.run(), h.commit("fourth", create("h", "fourth"))].concat();
    assert!(events.contains(&done("stop", Value::Null)));
    assert!(rows(&events, &one).is_empty());
    assert_eq!(rows(&events, &fragile), [json!([{"n":4}])]);

    // Close ends it with its last rows.
    h.submit(json!({"type":"close"})).unwrap();
    let events = h.run();
    assert_eq!(
        sql_snapshots(&events, &fragile),
        [json!({"kind":"watch","rows":[{"n":4}],"closed":true})]
    );
    assert!(sql_snapshots(&events, &one).is_empty());
}

#[test]
fn unwatch_and_close_remove_the_engine_watcher_and_a_tableless_statement_registers_none() {
    let mut h = observer_harness();
    let before = h.runtime.client().watcher_count();
    let (none, events) = h.watch_sql("constant", "SELECT 1 AS one", json!([]));
    assert_eq!(rows(&events, &none), [json!([{"one":1}])]);
    let (schema, _) = h.watch_sql(
        "schema",
        "SELECT count(*) AS n FROM sqlite_master",
        json!([]),
    );
    assert_eq!(
        h.runtime.client().watcher_count(),
        before,
        "no table a commit writes, no watcher"
    );
    // Mount and unmount a view over rarely written data many times.
    for round in 0..5 {
        let (notes, _) = h.watch_sql(&format!("notes{round}"), "SELECT id FROM Note", json!([]));
        assert_eq!(h.runtime.client().watcher_count(), before + 1);
        h.commit(
            &format!("stop{round}"),
            json!({"kind":"unwatch","observerId":notes}),
        );
        assert_eq!(h.runtime.client().watcher_count(), before, "round {round}");
    }
    let (kept, _) = h.watch_sql("kept", JOURNAL, json!([]));
    assert_eq!(h.runtime.client().watcher_count(), before + 1);
    let events = h.commit("note", put("Note", "n", json!({"body":"aside"})));
    assert!(rows(&events, &none).is_empty() && rows(&events, &schema).is_empty());
    h.submit(json!({"type":"close"})).unwrap();
    let events = h.run();
    assert_eq!(sql_snapshots(&events, &kept)[0]["closed"], true);
    assert_eq!(sql_snapshots(&events, &none)[0]["closed"], true);
    assert_eq!(
        h.runtime.client().watcher_count(),
        before,
        "close removes it"
    );
}

#[test]
fn a_direct_apply_that_fails_to_commit_fails_the_call_and_lets_nothing_escape() {
    let directory = tempfile::tempdir().unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let mut client = Client::open05(
        FailingCommit {
            inner: SqliteStore::open(directory.path().join("db")).unwrap(),
            armed: armed.clone(),
        },
        schema(),
        "User:u",
    )
    .unwrap();
    client.initialize_stream05(0).unwrap();
    let mut h = Harness {
        runtime: ClientRuntime::new(client),
        _dir: directory,
    };
    let (observer, events) =
        h.watch_sql("rows", "SELECT id,text FROM Entry ORDER BY id", json!([]));
    assert_eq!(rows(&events, &observer), [json!([])]);
    h.task("connect", json!({"kind":"connect"}));
    h.run();
    for (id, fail) in [("bad", true), ("good", false)] {
        h.task(
            id,
            json!({"kind":"fetch","model":"Entry","version":1,"identity":{"id":"e"},"store":true}),
        );
        let events = h.run();
        let http = events
            .iter()
            .find(|e| e["operation"]["route"] == "fetch")
            .unwrap();
        let request: Value =
            serde_json::from_str(http["operation"]["body"].as_str().unwrap()).unwrap();
        let response = json!({"protocol":5,"storeId":request["storeId"],"stream":request["stream"],"materialization":request["materialization"],"requestId":request["requestId"],"outcome":{"kind":"succeeded","result":{"id":"e","text":"returned","note":null}},"records":[{"key":{"model":"Entry","identity":{"id":"e"}},"cursor":null,"state":{"text":"returned","note":null}}]});
        armed.store(fail, Ordering::SeqCst);
        h.submit(json!({"type":"effectResult","effectId":http["effectId"],"outcome":{"ok":true,"value":response.to_string()}})).unwrap();
        let events = h.run();
        let completion = h.completion(&events, id);
        assert_eq!(completion["ok"], !fail);
        if fail {
            assert_eq!(completion["error"], "fetch.store_failed");
            assert!(rows(&events, &observer).is_empty());
            assert!(h.entry("e").is_none());
        } else {
            assert_eq!(
                rows(&events, &observer),
                [json!([{"id":"e","text":"returned"}])]
            );
            assert_eq!(h.entry("e"), Some(row("returned")));
            assert!(
                position(&events, |e| e["requestId"] == id)
                    < position(&events, |e| e["observerId"] == observer)
            );
        }
    }
}

#[test]
fn every_commit_path_re_emits_a_watched_statement() {
    let mut h = harness();
    h.runtime.client().initialize_stream05(0).unwrap();
    let context = h.runtime.client().request_context05().unwrap();
    let record = |cursor, text: &str, note: Value| v05::AuthorityChange::Record {
        key: v05::RecordKey {
            model: "Entry".into(),
            identity: json!({"id":"e"}),
        },
        cursor,
        state: json!({"text":text,"note":note}),
    };
    h.runtime
        .client()
        .install_authority05(&context, &[record(1, "server", Value::Null)], Some((0, 1)))
        .unwrap();
    let (observer, events) = h.watch_sql(
        "rows",
        "SELECT id,text,note FROM Entry ORDER BY id",
        json!([]),
    );
    assert_eq!(rows(&events, &observer), [json!([row("server")])]);
    let transaction = h.begin("optimism");
    h.command(
        "mine",
        &transaction.transaction,
        None,
        submit_mutation("Edit", json!({"entry":{"id":"e","text":"mine"}}), false),
    );
    let provisional = h.run();
    assert!(rows(&provisional, &observer).is_empty());
    h.callback(&transaction, true, None);
    let events = h.run();
    assert_eq!(
        rows(&events, &observer),
        [json!([row("mine")])],
        "named optimism commit"
    );
    let batch = h.runtime.client().freeze_batch05().unwrap().unwrap();
    h.runtime
        .client()
        .install_authority05(&context, &[record(2, "scope", json!("n"))], Some((1, 2)))
        .unwrap();
    let events = h.call("authority-publish", json!({"kind":"status"})).1;
    assert_eq!(
        rows(&events, &observer),
        [json!([{"id":"e","text":"mine","note":"n"}])],
        "authority replays pending update"
    );
    h.runtime
        .client()
        .acknowledge_batch05(&v05::BatchAcknowledgement {
            context: batch.context,
            batch_id: batch.batch_id,
            digest: batch.digest,
            results: vec![v05::MutationResult {
                mutation_id: batch.mutations[0].id,
                outcome: v05::MutationOutcome::Rejected {
                    code: "edit.refused".into(),
                    message: None,
                },
            }],
        })
        .unwrap();
    let events = h.call("settlement-publish", json!({"kind":"status"})).1;
    assert_eq!(
        rows(&events, &observer),
        [json!([{"id":"e","text":"scope","note":"n"}])],
        "rejection commit"
    );
    h.runtime
        .client()
        .install_cache05(
            &[v05::ReadRecord {
                key: v05::RecordKey {
                    model: "Entry".into(),
                    identity: json!({"id":"f"}),
                },
                cursor: (),
                state: json!({"text":"cached","note":null}),
            }],
            true,
        )
        .unwrap();
    let events = h.call("cache-publish", json!({"kind":"status"})).1;
    assert_eq!(
        rows(&events, &observer),
        [json!([{"id":"e","text":"scope","note":"n"},{"id":"f","text":"cached","note":null}])],
        "ordinary cache commit"
    );
}
