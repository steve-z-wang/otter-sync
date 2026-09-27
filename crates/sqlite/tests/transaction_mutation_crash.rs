//! Process interruption around a named Mutation submitted in a transaction
//! with a local companion.
//!
//! Each test seeds a SQLite file, then runs this same test executable as a
//! child process in its child-only mode ([`child_process_entry`]). The child
//! submits `PublishEntry` inside `Client::transaction`, deletes the local
//! Composition as the call's companion, and continues until the boundary the
//! parent named. There it reports what it did on stdout and blocks on stdin.
//! The parent waits for that report, kills the child (SIGKILL on Unix)
//! without closing its client, reopens the same file and checks that the
//! queue, the records and the recovery metadata agree.
//!
//! Boundaries covered: before the local commit, after the local commit,
//! before the receipt commit (receipt and store-hook writes made, not
//! committed), after a failed receipt transaction rolled back (a hook that
//! failed before the receipt was replayed, and a receipt replayed and then
//! rolled back), and after the receipt commit (store-hook and plain routes).
//!
//! Limits. The kill ends the process; the operating system keeps what SQLite
//! already wrote. These tests do not simulate power loss, storage or
//! filesystem failures, torn writes or a kill inside SQLite's own commit.
//! The transactions are small, so the before-local-commit kill shows that
//! uncommitted in-process state is lost; it does not exercise recovery that
//! ignores uncommitted pages already spilled to the WAL. The before-receipt
//! commit and rollback boundaries run on the store-hook route only, the one
//! receipt entry that can stop inside its transaction. Plain
//! `Client::acknowledge` runs the same `Engine::acknowledge` in one SQLite
//! transaction; only its after-commit boundary is exercised here, and SQLite
//! atomicity is what covers a kill inside it.
use axton_client::*;
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};
use std::fs::File;
use std::io::{BufRead, BufReader, Lines, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdout, Command, ExitStatus, Stdio};

/// The child-only mode's configuration, as JSON.
const CHILD_ENV: &str = "AXTON_TRANSACTION_MUTATION_CRASH_CHILD";
/// The child entry's test name, run with `--exact`.
const CHILD_TEST: &str = "child_process_entry";
/// Marks the child's report lines among the test harness's own output.
const EVENT: &str = "axton-crash-child-event ";

fn schema() -> Schema {
    let text =
        |name: &str| json!({"name":name,"type":{"kind":"scalar","name":"string"},"nullable":false});
    Schema::from_value(json!({"enums":[],
        "models":[
            {"name":"Composition","version":1,"identity":["id"],"fields":[text("id"),text("title")]},
            {"name":"Draft","version":1,"identity":["id"],"fields":[text("id"),text("compositionId"),text("name")],
             "relations":[{"name":"composition","target":"Composition","fields":["compositionId"],"targetFields":["id"],"onDelete":"delete"}]},
            {"name":"Entry","version":1,"identity":["id"],"fields":[
                {"name":"id","type":{"kind":"scalar","name":"string"},"nullable":false,"createDefault":{"kind":"uuid"}},
                text("title")]}],
        "actions":[
            {"name":"PublishEntry","version":1,"inputs":[{"kind":"model","name":"entry","model":"Entry","operation":"create","cardinality":"single"}],"outputs":[]}]
    }))
    .unwrap()
}

type Local = Client<SqliteStore>;

fn open(path: &Path) -> Local {
    Client::open(SqliteStore::open(path).unwrap(), schema()).unwrap()
}

fn key(model: &str, id: &str) -> RecordKey {
    RecordKey {
        model: model.into(),
        identity: json!({ "id": id }),
    }
}

fn op(model: &str, kind: OperationKind, id: &str, values: Option<Value>) -> Operation {
    Operation {
        model: model.into(),
        op: kind,
        identity: json!({ "id": id }),
        values,
    }
}

fn title(client: &mut Local, model: &str, id: &str) -> Option<String> {
    client
        .read(&key(model, id))
        .unwrap()
        .map(|row| row["title"].as_str().unwrap().to_owned())
}

fn exists(client: &mut Local, model: &str, id: &str) -> bool {
    client.read(&key(model, id)).unwrap().is_some()
}

fn count(client: &mut Local, sql: &str) -> u64 {
    client.read_sql(sql, &[]).unwrap()[0]["n"].as_u64().unwrap()
}

/// A file holding local-only Compositions `c1` ("draft", with Draft `d1`)
/// and `c2` ("second"). Returns the next call ordinal it would allocate.
fn seed(path: &Path) -> u64 {
    let mut client = open(path);
    client
        .transaction(|tx| {
            for (id, title) in [("c1", "draft"), ("c2", "second")] {
                tx.direct(op(
                    "Composition",
                    OperationKind::Create,
                    id,
                    Some(json!({ "title": title })),
                ))?;
            }
            tx.direct(op(
                "Draft",
                OperationKind::Create,
                "d1",
                Some(json!({"compositionId":"c1","name":"intro"})),
            ))
        })
        .unwrap();
    next_ordinal(&mut client)
}

fn next_ordinal(client: &mut Local) -> u64 {
    client
        .read_sql("SELECT next_ordinal AS n FROM axton_client", &[])
        .unwrap()[0]["n"]
        .as_u64()
        .unwrap()
}

/// The queued operations as `(kind, model, id, op)` in position order.
fn queued_ops(client: &mut Local) -> Vec<(String, String, String, String)> {
    client
        .read_sql(
            "SELECT kind, model, identity, op FROM axton_mutation_operation ORDER BY ordinal, position",
            &[],
        )
        .unwrap()
        .iter()
        .map(|row| {
            let identity: Value = serde_json::from_str(row["identity"].as_str().unwrap()).unwrap();
            (
                row["kind"].as_str().unwrap().to_owned(),
                row["model"].as_str().unwrap().to_owned(),
                identity["id"].as_str().unwrap().to_owned(),
                row["op"].as_str().unwrap().to_owned(),
            )
        })
        .collect()
}

fn op_row(kind: &str, model: &str, id: &str, op: &str) -> (String, String, String, String) {
    (kind.into(), model.into(), id.into(), op.into())
}

/// The receipt the backend returns for the frozen batch: the call accepted
/// with the authority of the Entry it created, or rejected.
fn receipt(client_id: &str, request: &PushRequest, accepted: bool) -> PushReceipt {
    let call = &request.mutations[0].raw;
    let call_id = call["callId"].as_str().unwrap().to_owned();
    let mut receipt = PushReceipt {
        client_id: client_id.into(),
        batch_sequence: request.batch_sequence,
        rejections: vec![],
        completions: vec![],
        records: vec![],
    };
    if accepted {
        let mut state = call["args"]["entry"].clone();
        let id = state.as_object_mut().unwrap().remove("id").unwrap();
        receipt.records.push(AuthorityRecord {
            model: "Entry".into(),
            identity: json!({ "id": id }),
            stamp: 1,
            state,
            error: None,
        });
        receipt.completions.push(CallCompletion {
            call_id,
            outcome: ActionOutcome::Succeeded {
                result: Value::Null,
            },
        });
    } else {
        receipt.rejections.push(Rejection {
            ordinal: call["ordinal"].as_u64().unwrap(),
            code: "denied".into(),
        });
        receipt.completions.push(CallCompletion {
            call_id,
            outcome: ActionOutcome::Failed {
                code: "denied".into(),
                execution: ExecutionState::Rejected,
            },
        });
    }
    receipt
}

fn decode(request: &str) -> PushRequest {
    PushRequest::decode_actions(request.as_bytes(), &schema()).unwrap()
}

/// The frozen request carries no companion data in any field. Its `models`
/// declaration names every read contract with its version (local Models
/// included), and nothing else.
fn assert_no_companion_data(request: &[u8]) {
    let mut raw: Value = serde_json::from_slice(request).unwrap();
    let models = raw.as_object_mut().unwrap().remove("models").unwrap();
    assert!(
        models.as_object().unwrap().values().all(Value::is_u64),
        "{models}"
    );
    let rest = raw.to_string();
    for local in ["Composition", "Draft", "companion"] {
        assert!(!rest.contains(local), "{local} reached the wire: {rest}");
    }
}

/// Every row the call and its settlement can touch: the queue, the local
/// write journal, rejections, record stamps, the client counters, and the
/// Model rows with their before images.
fn snapshot(query: &mut dyn FnMut(&str) -> Vec<Value>) -> Value {
    let mut tables = serde_json::Map::new();
    for (name, sql) in [
        (
            "axton_mutation",
            "SELECT ordinal, name, version, push, diverged, call_id, args, store FROM axton_mutation ORDER BY ordinal",
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
    ] {
        tables.insert(name.into(), Value::Array(query(sql)));
    }
    for model in ["Composition", "Draft", "Entry"] {
        for table in [model.to_owned(), format!("axton_before_{model}")] {
            let rows = query(&format!("SELECT * FROM {table} ORDER BY id"));
            tables.insert(table, Value::Array(rows));
        }
    }
    Value::Object(tables)
}

// The child-only mode.

/// Where the child stops and waits to be killed.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Boundary {
    /// Call, optimism and companion written; the transaction is open.
    BeforeLocalCommit,
    /// The transaction committed; nothing was frozen.
    AfterLocalCommit,
    /// The batch was frozen, and the receipt and a store hook's write were
    /// applied in the receipt transaction, which is still open.
    BeforeReceiptCommit,
    /// A store hook failed before the receipt was replayed, and the receipt
    /// transaction rolled back. (Preflight had already undone its own
    /// writes, so only the hook's write was pending.)
    HookFailedBeforeReplay,
    /// The receipt was replayed with its hook's write, settling the call in
    /// the open transaction, and then the transaction rolled back, as when
    /// its commit fails.
    ReplayedReceiptRolledBack,
    /// The receipt transaction committed.
    AfterReceiptCommit,
}

impl Boundary {
    fn name(self) -> &'static str {
        match self {
            Self::BeforeLocalCommit => "before_local_commit",
            Self::AfterLocalCommit => "after_local_commit",
            Self::BeforeReceiptCommit => "before_receipt_commit",
            Self::HookFailedBeforeReplay => "hook_failed_before_replay",
            Self::ReplayedReceiptRolledBack => "replayed_receipt_rolled_back",
            Self::AfterReceiptCommit => "after_receipt_commit",
        }
    }
    fn parse(name: &str) -> Self {
        [
            Self::BeforeLocalCommit,
            Self::AfterLocalCommit,
            Self::BeforeReceiptCommit,
            Self::HookFailedBeforeReplay,
            Self::ReplayedReceiptRolledBack,
            Self::AfterReceiptCommit,
        ]
        .into_iter()
        .find(|boundary| boundary.name() == name)
        .unwrap_or_else(|| panic!("unknown boundary {name}"))
    }
}

/// How the child applies the receipt: through the store-hook route
/// (`prepare_store`, hook write, `apply_prepared_store`, `commit_session`)
/// or the plain `Client::acknowledge`.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Route {
    Hooked,
    Plain,
}

/// Child-only entry: does nothing unless the parent spawned this executable
/// with [`CHILD_ENV`] set.
#[test]
#[ignore = "child-process entry of the interruption tests; they run it"]
fn child_process_entry() {
    let Ok(config) = std::env::var(CHILD_ENV) else {
        return;
    };
    let config: Value = serde_json::from_str(&config).unwrap();
    let path = PathBuf::from(config["path"].as_str().unwrap());
    let boundary = Boundary::parse(config["boundary"].as_str().unwrap());
    let accepted = config["accepted"].as_bool().unwrap();
    let route = if config["hooked"].as_bool().unwrap() {
        Route::Hooked
    } else {
        Route::Plain
    };
    let mut client = open(&path);
    let call = client
        .transaction(|tx| {
            let composition = tx.read(&key("Composition", "c1"))?.unwrap();
            let call = tx.submit_mutation(
                "PublishEntry",
                1,
                json!({ "entry": { "title": composition["title"] } }),
                ActionCallOptions::default(),
            )?;
            // The `local` callback's one write: the Composition is deleted as
            // this call's companion (its Draft cascades with it).
            tx.append_companion(
                call.ordinal,
                op("Composition", OperationKind::Delete, "c1", None),
            )?;
            if boundary == Boundary::BeforeLocalCommit {
                park(json!({"event":"uncommitted","callId":call.call_id,"ordinal":call.ordinal}));
            }
            Ok(call)
        })
        .unwrap();
    let submitted = json!({"callId":call.call_id,"ordinal":call.ordinal});
    if boundary == Boundary::AfterLocalCommit {
        park(json!({"event":"committed","call":submitted}));
    }
    let bytes = client.freeze().unwrap().unwrap();
    let request = String::from_utf8(bytes).unwrap();
    let receipt = receipt(client.client_id(), &decode(&request), accepted);
    let report = json!({"call":submitted,"request":request});
    let hooked = |client: &mut Local| {
        client.begin_session().unwrap();
        let prepared = client
            .prepare_store(StoreDelivery::Receipt {
                sequence: receipt.batch_sequence,
                receipt: receipt.clone(),
            })
            .unwrap();
        // An onStore hook's local write, in the receipt's transaction.
        client
            .session(|tx| {
                tx.direct(op(
                    "Composition",
                    OperationKind::Update,
                    "c2",
                    Some(json!({"title":"hooked"})),
                ))
            })
            .unwrap();
        prepared
    };
    match boundary {
        Boundary::BeforeLocalCommit | Boundary::AfterLocalCommit => unreachable!(),
        Boundary::BeforeReceiptCommit => {
            let prepared = hooked(&mut client);
            client.apply_prepared_store(prepared).unwrap();
            park(json!({"event":"settling","report":report}));
        }
        Boundary::HookFailedBeforeReplay => {
            let before = snapshot(&mut |sql| client.read_sql(sql, &[]).unwrap());
            let _prepared = hooked(&mut client);
            // The hook fails: its write rolls back with the transaction.
            client.rollback_session().unwrap();
            let after = snapshot(&mut |sql| client.read_sql(sql, &[]).unwrap());
            assert_eq!(after, before);
            park(json!({"event":"rolled_back","report":report,"before":before}));
        }
        Boundary::ReplayedReceiptRolledBack => {
            let before = snapshot(&mut |sql| client.read_sql(sql, &[]).unwrap());
            let prepared = hooked(&mut client);
            let StoreResult::Receipt(applied) = client.apply_prepared_store(prepared).unwrap()
            else {
                panic!("a receipt delivery stores as a receipt");
            };
            assert_eq!(applied.completions.len(), 1);
            // The receipt's effects exist in the open transaction: the call
            // left the queue and the batch is recorded as completed.
            let during = snapshot(&mut |sql| client.session_sql(sql, &[]).unwrap());
            assert_eq!(during["axton_mutation"], json!([]));
            assert_eq!(
                during["axton_client"][0]["last_completed_push"],
                receipt.batch_sequence
            );
            assert_ne!(during, before);
            // The commit fails: the whole receipt transaction rolls back.
            client.rollback_session().unwrap();
            let after = snapshot(&mut |sql| client.read_sql(sql, &[]).unwrap());
            assert_eq!(after, before);
            park(json!({"event":"rolled_back","report":report,"before":before}));
        }
        Boundary::AfterReceiptCommit => {
            let completions = match route {
                Route::Hooked => {
                    let prepared = hooked(&mut client);
                    let StoreResult::Receipt(applied) =
                        client.apply_prepared_store(prepared).unwrap()
                    else {
                        panic!("a receipt delivery stores as a receipt");
                    };
                    client.commit_session().unwrap();
                    applied.completions
                }
                Route::Plain => {
                    client
                        .acknowledge(receipt.batch_sequence, receipt.clone())
                        .unwrap()
                        .completions
                }
            };
            park(json!({"event":"settled","report":report,"completions":completions.len()}));
        }
    }
}

/// Report `event` to the parent and wait to be killed. The client stays
/// open. If the parent goes away instead, abort: nothing more is written.
fn park(event: Value) -> ! {
    let mut out = std::io::stdout().lock();
    writeln!(out, "\n{EVENT}{event}").unwrap();
    out.flush().unwrap();
    drop(out);
    let mut line = String::new();
    let _ = std::io::stdin().read_line(&mut line);
    std::process::abort();
}

// The parent side.

/// A spawned child, killed and reaped on drop if a test fails first.
struct Crashing {
    child: Child,
    lines: Lines<BufReader<ChildStdout>>,
    stderr: PathBuf,
}

impl Crashing {
    fn spawn(dir: &Path, path: &Path, boundary: Boundary, accepted: bool, route: Route) -> Self {
        let stderr = dir.join(format!("{}.stderr", boundary.name()));
        let config = json!({
            "path": path,
            "boundary": boundary.name(),
            "accepted": accepted,
            "hooked": route == Route::Hooked,
        });
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                CHILD_TEST,
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD_ENV, config.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(File::create(&stderr).unwrap())
            .spawn()
            .unwrap();
        let lines = BufReader::new(child.stdout.take().unwrap()).lines();
        Self {
            child,
            lines,
            stderr,
        }
    }

    /// Block until the child reports `event` at its boundary.
    fn wait_for(&mut self, event: &str) -> Value {
        for line in self.lines.by_ref() {
            let line = line.unwrap();
            if let Some(at) = line.find(EVENT) {
                let reported: Value = serde_json::from_str(&line[at + EVENT.len()..]).unwrap();
                assert_eq!(reported["event"], event, "{reported}");
                return reported;
            }
        }
        let status = self.child.wait().unwrap();
        panic!(
            "child ended ({status}) before {event}: {}",
            std::fs::read_to_string(&self.stderr).unwrap_or_default()
        );
    }

    /// Kill the child where it waits, without closing its client.
    fn kill(mut self) {
        let status = self.terminate().unwrap();
        assert!(!status.success(), "{status}");
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(
                status.signal(),
                Some(9),
                "{status}: {}",
                std::fs::read_to_string(&self.stderr).unwrap_or_default()
            );
        }
    }

    /// SIGKILL the child and reap it. Its stdin stays open until then:
    /// `Child::wait` would close it first, and a child woken by that EOF
    /// could abort before the kill lands.
    fn terminate(&mut self) -> std::io::Result<ExitStatus> {
        let stdin = self.child.stdin.take();
        self.child.kill()?;
        let status = self.child.wait();
        drop(stdin);
        status
    }
}

impl Drop for Crashing {
    fn drop(&mut self) {
        let _ = self.terminate();
    }
}

/// The durable state of a call the child committed: the one queue row with
/// its canonical intent, its wire create and its companion delete with the
/// cascade (never appended again), the visible optimism, and the before
/// images that settlement restores from. Returns the Entry's identity.
fn assert_committed(client: &mut Local, call: &Value, frozen: bool) -> String {
    assert_eq!(client.pending_count().unwrap(), 1);
    let rows = client
        .read_sql(
            "SELECT ordinal, call_id, args, push FROM axton_mutation",
            &[],
        )
        .unwrap();
    assert_eq!(rows[0]["ordinal"], call["ordinal"]);
    assert_eq!(rows[0]["call_id"], call["callId"]);
    assert_eq!(rows[0]["push"].is_null(), !frozen, "{rows:?}");
    let args: Value = serde_json::from_str(rows[0]["args"].as_str().unwrap()).unwrap();
    assert_eq!(args["entry"]["title"], "draft");
    let entry = args["entry"]["id"].as_str().unwrap().to_owned();
    assert_eq!(
        queued_ops(client),
        vec![
            op_row("wire", "Entry", &entry, "create"),
            op_row("companion", "Draft", "d1", "delete"),
            op_row("companion", "Composition", "c1", "delete"),
        ]
    );
    assert_eq!(title(client, "Entry", &entry).as_deref(), Some("draft"));
    assert!(!exists(client, "Composition", "c1"));
    assert!(!exists(client, "Draft", "d1"));
    assert_eq!(
        title(client, "Composition", "c2").as_deref(),
        Some("second")
    );
    assert_eq!(
        client
            .read_sql("SELECT id, title FROM axton_before_Composition", &[])
            .unwrap(),
        vec![json!({"id":"c1","title":"draft"})]
    );
    assert_eq!(
        count(client, "SELECT COUNT(*) AS n FROM axton_before_Draft"),
        1
    );
    assert_eq!(
        count(client, "SELECT COUNT(*) AS n FROM axton_local_write"),
        0
    );
    assert_eq!(
        count(client, "SELECT COUNT(*) AS n FROM axton_rejection"),
        0
    );
    entry
}

/// The call settled exactly once: nothing queued or retained for recovery,
/// the companion kept on acceptance and undone on rejection, one rejection
/// record when rejected, and the batch recorded as completed.
fn assert_settled(client: &mut Local, request: &PushRequest, accepted: bool, c2: &str) {
    let entry = request.mutations[0].raw["args"]["entry"]["id"]
        .as_str()
        .unwrap();
    assert_eq!(client.pending_count().unwrap(), 0);
    assert_eq!(
        count(client, "SELECT COUNT(*) AS n FROM axton_mutation_operation"),
        0
    );
    assert_eq!(client.before_image_count().unwrap(), 0);
    assert_eq!(
        count(client, "SELECT COUNT(*) AS n FROM axton_local_write"),
        0
    );
    assert_eq!(exists(client, "Composition", "c1"), !accepted);
    assert_eq!(exists(client, "Draft", "d1"), !accepted);
    if !accepted {
        assert_eq!(title(client, "Composition", "c1").as_deref(), Some("draft"));
    }
    assert_eq!(exists(client, "Entry", entry), accepted);
    assert_eq!(
        client.record_stamp(&key("Entry", entry)).unwrap(),
        u64::from(accepted)
    );
    assert_eq!(title(client, "Composition", "c2").as_deref(), Some(c2));
    assert_eq!(
        count(client, "SELECT COUNT(*) AS n FROM axton_rejection"),
        u64::from(!accepted)
    );
    assert_eq!(
        client.last_completed_push().unwrap(),
        request.batch_sequence
    );
    assert!(client.freeze().unwrap().is_none());
}

/// A second delivery of the same receipt is stale: no completion is
/// reported again and nothing changes.
fn assert_redelivery_is_stale(client: &mut Local, request: &PushRequest, accepted: bool, c2: &str) {
    let again = receipt(client.client_id(), request, accepted);
    let report = client.acknowledge(request.batch_sequence, again).unwrap();
    assert!(report.stale);
    assert!(report.completions.is_empty());
    assert_settled(client, request, accepted, c2);
}

/// After a kill before any receipt committed: the call is still queued as
/// the child committed it, the retried request is the child's frozen
/// request byte for byte, and its receipt settles the call exactly once.
fn assert_retry_settles_once(path: &Path, report: &Value, accepted: bool) {
    let mut client = open(path);
    assert_committed(&mut client, &report["call"], true);
    assert_eq!(client.last_completed_push().unwrap(), 0);
    let sent = report["request"].as_str().unwrap();
    let bytes = client.freeze().unwrap().unwrap();
    assert_eq!(bytes, sent.as_bytes());
    assert_no_companion_data(&bytes);
    let request = decode(sent);
    settle_once(&mut client, &request, accepted);
    assert_settled(&mut client, &request, accepted, "second");
    assert_redelivery_is_stale(&mut client, &request, accepted, "second");
}

/// Deliver the receipt once, as a restarted client would, and check the
/// single completion it reports.
fn settle_once(client: &mut Local, request: &PushRequest, accepted: bool) {
    let receipt = receipt(client.client_id(), request, accepted);
    let expected = receipt.completions.clone();
    let report = client.acknowledge(request.batch_sequence, receipt).unwrap();
    assert!(!report.stale);
    assert_eq!(report.completions, expected);
}

/// Killed inside the transaction: neither the call, its optimism, its
/// companion nor any recovery metadata survives, and its ordinal is unused.
#[test]
fn killed_before_the_local_commit_keeps_no_call_companion_or_recovery_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let ordinal = seed(&path);
    let mut child = Crashing::spawn(
        dir.path(),
        &path,
        Boundary::BeforeLocalCommit,
        true,
        Route::Plain,
    );
    let reported = child.wait_for("uncommitted");
    assert_eq!(reported["ordinal"], ordinal);
    child.kill();

    let mut client = open(&path);
    assert_eq!(client.pending_count().unwrap(), 0);
    for table in ["axton_mutation", "axton_mutation_operation", "Entry"] {
        assert_eq!(
            count(&mut client, &format!("SELECT COUNT(*) AS n FROM {table}")),
            0,
            "{table}"
        );
    }
    assert_eq!(
        client
            .read_sql(
                "SELECT COUNT(*) AS n FROM axton_mutation WHERE call_id=?",
                &[reported["callId"].clone()]
            )
            .unwrap()[0]["n"],
        0
    );
    assert_eq!(client.before_image_count().unwrap(), 0);
    assert_eq!(
        count(&mut client, "SELECT COUNT(*) AS n FROM axton_local_write"),
        0
    );
    assert_eq!(
        title(&mut client, "Composition", "c1").as_deref(),
        Some("draft")
    );
    assert!(exists(&mut client, "Draft", "d1"));
    assert_eq!(next_ordinal(&mut client), ordinal);
    assert!(client.freeze().unwrap().is_none());
}

/// Killed after the commit: the call and its companion survive, the
/// request frozen after restart carries the call's identity and is the same
/// on every retry, and either outcome settles the companion once.
#[test]
fn killed_after_the_local_commit_keeps_the_call_and_its_companion_for_either_outcome() {
    for accepted in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        seed(&path);
        let mut child = Crashing::spawn(
            dir.path(),
            &path,
            Boundary::AfterLocalCommit,
            accepted,
            Route::Plain,
        );
        let call = child.wait_for("committed")["call"].clone();
        child.kill();

        let mut client = open(&path);
        assert_committed(&mut client, &call, false);
        let bytes = client.freeze().unwrap().unwrap();
        let request = decode(std::str::from_utf8(&bytes).unwrap());
        assert_eq!(request.mutations.len(), 1);
        assert_eq!(request.mutations[0].raw["callId"], call["callId"]);
        assert_eq!(request.mutations[0].raw["ordinal"], call["ordinal"]);
        assert_no_companion_data(&bytes);
        assert_committed(&mut client, &call, true);
        // A retry after another restart sends the same request.
        drop(client);
        let mut client = open(&path);
        assert_eq!(client.freeze().unwrap().unwrap(), bytes);
        assert_committed(&mut client, &call, true);
        settle_once(&mut client, &request, accepted);
        assert_settled(&mut client, &request, accepted, "second");
        drop(client);
        let mut client = open(&path);
        assert_redelivery_is_stale(&mut client, &request, accepted, "second");
    }
}

/// Killed with the receipt and a store hook's write applied but not
/// committed: both are gone after restart, the frozen request is retried
/// unchanged, and its receipt then settles the call once.
#[test]
fn killed_before_the_receipt_commit_retries_the_same_request_and_settles_once() {
    for accepted in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        seed(&path);
        let mut child = Crashing::spawn(
            dir.path(),
            &path,
            Boundary::BeforeReceiptCommit,
            accepted,
            Route::Hooked,
        );
        let report = child.wait_for("settling")["report"].clone();
        child.kill();
        assert_retry_settles_once(&path, &report, accepted);
    }
}

/// A receipt transaction that fails rolls back: a hook failing before the
/// receipt is replayed, or a receipt replayed (the call settled in the open
/// transaction, which the child checks) whose transaction then rolls back.
/// The queue, records and recovery metadata return to their pre-receipt
/// rows, a kill afterwards changes nothing, and the retried request
/// settles the call once.
#[test]
fn a_failed_receipt_transaction_rolls_back_and_the_retry_settles_once() {
    for boundary in [
        Boundary::HookFailedBeforeReplay,
        Boundary::ReplayedReceiptRolledBack,
    ] {
        for accepted in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            seed(&path);
            let mut child = Crashing::spawn(dir.path(), &path, boundary, accepted, Route::Hooked);
            let reported = child.wait_for("rolled_back");
            child.kill();

            let mut client = open(&path);
            assert_eq!(
                snapshot(&mut |sql| client.read_sql(sql, &[]).unwrap()),
                reported["before"],
                "{boundary:?}"
            );
            drop(client);
            assert_retry_settles_once(&path, &reported["report"], accepted);
        }
    }
}

/// Killed after the receipt committed, through the store-hook route or the
/// plain one: the settlement (and the hook's write) survives as exactly one
/// result, nothing is sent again and a redelivered receipt is stale.
#[test]
fn killed_after_the_receipt_commit_keeps_exactly_one_settled_result() {
    for route in [Route::Hooked, Route::Plain] {
        for accepted in [true, false] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("db");
            seed(&path);
            let mut child = Crashing::spawn(
                dir.path(),
                &path,
                Boundary::AfterReceiptCommit,
                accepted,
                route,
            );
            let reported = child.wait_for("settled");
            assert_eq!(reported["completions"], 1);
            child.kill();

            let mut client = open(&path);
            let request = decode(reported["report"]["request"].as_str().unwrap());
            let c2 = if route == Route::Hooked {
                "hooked"
            } else {
                "second"
            };
            assert_settled(&mut client, &request, accepted, c2);
            assert_redelivery_is_stale(&mut client, &request, accepted, c2);
        }
    }
}
