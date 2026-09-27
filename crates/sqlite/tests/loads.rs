//! Native durable Load jobs over real SQLite ([#173](https://github.com/zanminwang/axton/issues/173)):
//! the job ledger and its frozen page requests across reopen, the opt-in once
//! mapping, explicit retry, and a page's authority, hook writes and progress
//! committed as one unit.
mod common;
use axton_client::loads::{
    CANCELLED, CONTRACT_UNAVAILABLE, HOOK_FAILED, INVALID_CONTINUATION, INVALID_OPTIONS,
    LEDGER_INVALID, NOT_FOUND, NOT_RETRYABLE, NOT_TERMINAL, PAGE_TOO_LARGE, PROTOCOL_INVALID,
    STORE_FAILED,
};
use axton_client::*;
use axton_sqlite::SqliteStore;
use common::*;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::path::Path;

const PROJECT: &str = "0190f0e0-1111-7222-8333-444455556666";

fn open_db(path: &Path) -> Client<SqliteStore> {
    Client::open(SqliteStore::open(path).unwrap(), load_schema()).unwrap()
}
fn args() -> Value {
    json!({"projectId": PROJECT, "since": null})
}
fn plain() -> LoadOptions {
    LoadOptions::default()
}
fn once() -> LoadOptions {
    LoadOptions {
        once: true,
        refresh: false,
    }
}
fn refresh() -> LoadOptions {
    LoadOptions {
        once: true,
        refresh: true,
    }
}
fn start(c: &mut Client<SqliteStore>, options: LoadOptions) -> LoadStarted {
    c.start_load("Entries", 1, &args(), options).unwrap()
}
fn job(c: &mut Client<SqliteStore>, id: &str) -> LoadJob {
    c.get_load(id).unwrap().expect("a stored job")
}
fn fence(c: &mut Client<SqliteStore>, id: &str) -> LoadFence {
    let job = job(c, id);
    LoadFence {
        replica: c.replica_generation(),
        load_id: job.id,
        run: job.run,
        call_id: job.call_id.expect("a frozen page"),
    }
}
fn store(
    c: &mut Client<SqliteStore>,
    id: &str,
    entries: &[(&str, &str, u64)],
    next: Option<Value>,
) -> LoadStored {
    let fence = fence(c, id);
    c.store_load_page(&fence, reply(load_page(&fence, entries, next)))
        .unwrap()
}
fn entry(c: &mut Client<SqliteStore>, id: &str) -> Option<Value> {
    c.read(
        &load_schema()
            .record_key("Entry", &json!({ "id": id }))
            .unwrap(),
    )
    .unwrap()
}
fn stamp(c: &mut Client<SqliteStore>, id: &str) -> u64 {
    c.record_stamp(
        &load_schema()
            .record_key("Entry", &json!({ "id": id }))
            .unwrap(),
    )
    .unwrap()
}
fn fails_with<T: std::fmt::Debug>(result: Result<T>, code: &str) {
    let error = result.unwrap_err().to_string();
    assert!(
        error.starts_with(code),
        "{error} does not start with {code}"
    );
}
fn raw(path: &Path, sql: &str) {
    SqliteStore::open(path).unwrap().execute_batch(sql).unwrap();
}
fn prepare(
    c: &mut Client<SqliteStore>,
    fence: &LoadFence,
    page: LoadPageResponse,
) -> PreparedStore {
    let LoadPageStep::Store(delivery) = c.load_page_step(fence, reply(page)).unwrap() else {
        panic!("a page to store")
    };
    c.begin_session().unwrap();
    c.prepare_store(delivery).unwrap()
}
fn hook_write(c: &mut Client<SqliteStore>, id: &str) {
    c.session(|tx| tx.direct(create("Entry", id, json!({"text":"hook","note":null}))))
        .unwrap();
}
fn applied(stored: LoadStored) -> LoadJob {
    match stored {
        LoadStored::Applied { job, .. } => job,
        other => panic!("expected an applied page, got {other:?}"),
    }
}
fn failed(stored: LoadStored) -> LoadJob {
    match stored {
        LoadStored::Failed(job) => job,
        other => panic!("expected a failed page, got {other:?}"),
    }
}
fn backend_failure(fence: &LoadFence, code: &str) -> LoadPageResponse {
    LoadPageResponse {
        load_id: fence.load_id.clone(),
        call_id: fence.call_id.clone(),
        outcome: LoadOutcome::Failed {
            error: LoadError {
                code: code.into(),
                message: "rejected".into(),
            },
        },
        records: vec![],
    }
}

// ---------------------------------------------------------------- the ledger

#[test]
fn a_start_offline_survives_reopen_with_its_frozen_request() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open_db(&path);
    let started = c
        .start_load(
            "Entries",
            1,
            &json!({"since":"2026-01-01T02:00:00+02:00","projectId":PROJECT.to_uppercase()}),
            plain(),
        )
        .unwrap();
    assert_eq!(started.kind, LoadStartKind::Created);
    let created = started.job;
    assert_eq!(created.phase, LoadPhase::Pending);
    assert_eq!((created.run, created.pages, created.attempts), (1, 0, 0));
    assert_eq!(created.continuation, None, "the first page is null");
    assert_eq!(
        created.args,
        json!({"projectId":PROJECT,"since":"2026-01-01T00:00:00.000Z"}),
        "arguments are stored normalized"
    );
    let intent = created.intent.clone().unwrap();
    assert_eq!(intent.call_id, created.call_id.clone().unwrap());
    assert_eq!(intent.load_id, created.id);
    assert_eq!(intent.args, created.args);
    assert_eq!(intent.continuation, None);
    assert_eq!(intent.models, [("Entry".to_string(), 1)].into());
    assert_eq!(
        created.status(),
        LoadStatus {
            id: created.id.clone(),
            name: "Entries".into(),
            version: 1,
            phase: LoadPhase::Pending,
            pages: 0,
            error: None,
        }
    );
    // No Mutation ordinal, push sequence or subscription was consumed.
    assert_eq!(c.pending_count().unwrap(), 0);
    assert_eq!(table_count(&mut c, "axton_mutation"), 0);
    assert_eq!(table_count(&mut c, "axton_subscription"), 0);
    drop(c);
    let mut c = open_db(&path);
    assert_eq!(job(&mut c, &created.id), created, "the exact job survives");
    let schedule = c.load_ready_pages(8, &BTreeSet::new()).unwrap();
    assert_eq!(schedule.pages.len(), 1);
    assert_eq!(
        schedule.pages[0].intent, intent,
        "the frozen request is unchanged"
    );
    assert_eq!(schedule.pages[0].fence.call_id, intent.call_id);
    assert_eq!(schedule.pages[0].fence.run, 1);
}

#[test]
fn ordinary_starts_are_independent_and_reattach_by_id() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open_db(&dir.path().join("db"));
    let first = start(&mut c, plain()).job;
    let second = start(&mut c, plain()).job;
    assert_ne!(first.id, second.id);
    assert_ne!(first.call_id, second.call_id);
    assert_eq!(table_count(&mut c, "axton_load_once"), 0);
    assert_eq!(
        job(&mut c, &first.id.to_uppercase()),
        first,
        "get normalizes the ID"
    );
    let listed: Vec<String> = c
        .list_loads(50)
        .unwrap()
        .into_iter()
        .map(|j| j.id)
        .collect();
    assert_eq!(
        listed,
        vec![second.id.clone(), first.id.clone()],
        "newest first"
    );
    assert_eq!(c.list_loads(1).unwrap()[0].id, second.id);
    fails_with(c.list_loads(0), INVALID_OPTIONS);
    fails_with(c.list_loads(101), INVALID_OPTIONS);
    assert_eq!(c.list_loads(100).unwrap().len(), 2);
    assert!(
        c.get_load(&uuid_like(9)).unwrap().is_none(),
        "an unknown ID is null"
    );
    assert!(c.get_load("not-an-id").unwrap().is_none());
    fails_with(c.cancel_load("not-an-id"), NOT_FOUND);
    fails_with(
        c.start_load(
            "Entries",
            1,
            &args(),
            LoadOptions {
                once: false,
                refresh: true,
            },
        ),
        INVALID_OPTIONS,
    );
    assert_eq!(
        table_count(&mut c, "axton_load"),
        2,
        "refused before persistence"
    );
}
fn uuid_like(n: u64) -> String {
    format!("01890f47-1234-7123-8123-{n:012x}")
}

#[test]
fn pages_advance_the_committed_continuation_until_the_backend_ends() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open_db(&path);
    let id = start(&mut c, plain()).job.id;
    let first_call = job(&mut c, &id).call_id.unwrap();
    let after_one = applied(store(
        &mut c,
        &id,
        &[("a", "A", 1), ("b", "B", 1)],
        Some(json!({"after":"b"})),
    ));
    assert_eq!(after_one.pages, 1);
    assert_eq!(after_one.phase, LoadPhase::Pending);
    assert_eq!(
        after_one.continuation,
        Some(Continuation {
            state: json!({"after":"b"})
        })
    );
    let second_call = after_one.call_id.clone().unwrap();
    assert_ne!(second_call, first_call, "each page has its own call ID");
    assert_eq!(
        after_one.intent.as_ref().unwrap().continuation,
        after_one.continuation
    );
    // {state: null} is a later request, never the first page or the end.
    let after_two = applied(store(&mut c, &id, &[], Some(Value::Null)));
    assert_eq!(after_two.pages, 2, "an empty page counts");
    assert_eq!(
        after_two.continuation,
        Some(Continuation { state: Value::Null })
    );
    drop(c);
    let mut c = open_db(&path);
    assert_eq!(
        job(&mut c, &id).continuation,
        Some(Continuation { state: Value::Null })
    );
    let done = applied(store(&mut c, &id, &[("c", "C", 2)], None));
    assert_eq!(done.phase, LoadPhase::Complete);
    assert_eq!(
        (done.pages, done.call_id.clone(), done.intent.clone()),
        (3, None, None)
    );
    // Model data and completion belong to the same committed snapshot.
    let snapshot = c
        .read_sql(
            "SELECT (SELECT COUNT(*) FROM \"Entry\") AS rows, phase, pages FROM axton_load WHERE load_id = ?",
            &[json!(id)],
        )
        .unwrap();
    assert_eq!(snapshot[0], json!({"rows":3,"phase":"complete","pages":3}));
    assert!(
        c.load_ready_pages(8, &BTreeSet::new())
            .unwrap()
            .pages
            .is_empty()
    );
}

#[test]
fn a_committed_page_requeues_its_job_behind_ready_peers() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open_db(&dir.path().join("db"));
    let a = start(&mut c, plain()).job.id;
    let b = start(&mut c, plain()).job.id;
    let d = start(&mut c, plain()).job.id;
    let ready = |c: &mut Client<SqliteStore>, limit, skip: &[&String]| -> Vec<String> {
        let skip = skip.iter().map(|s| s.to_string()).collect();
        c.load_ready_pages(limit, &skip)
            .unwrap()
            .pages
            .into_iter()
            .map(|p| p.fence.load_id)
            .collect()
    };
    assert_eq!(ready(&mut c, 8, &[]), vec![a.clone(), b.clone(), d.clone()]);
    assert_eq!(ready(&mut c, 2, &[]), vec![a.clone(), b.clone()], "bounded");
    assert_eq!(
        ready(&mut c, 2, &[&a]),
        vec![b.clone(), d.clone()],
        "in flight is skipped"
    );
    applied(store(&mut c, &a, &[], Some(json!(1))));
    assert_eq!(ready(&mut c, 8, &[]), vec![b.clone(), d.clone(), a.clone()]);
    assert!(ready(&mut c, 0, &[]).is_empty());
}

#[test]
fn a_damaged_job_fails_visibly_without_blocking_healthy_jobs() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open_db(&path);
    let broken = start(&mut c, plain()).job.id;
    let healthy = start(&mut c, plain()).job.id;
    raw(
        &path,
        &format!("UPDATE axton_load SET intent = 'not json' WHERE load_id = '{broken}'"),
    );
    let schedule = c.load_ready_pages(1, &BTreeSet::new()).unwrap();
    assert_eq!(schedule.pages.len(), 1);
    assert_eq!(schedule.pages[0].fence.load_id, healthy);
    assert_eq!(schedule.issues.len(), 1);
    assert_eq!(schedule.issues[0].load_id, broken);
    let error = c.get_load(&broken).unwrap_err().to_string();
    assert!(error.contains(&broken), "{error}");
    assert_eq!(
        c.list_loads(10)
            .unwrap()
            .into_iter()
            .map(|j| j.id)
            .collect::<Vec<_>>(),
        vec![healthy.clone()]
    );
    applied(store(&mut c, &healthy, &[("a", "A", 1)], None));
}

#[test]
fn the_ledger_tables_are_added_beside_existing_work() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open_db(&path);
    c.transaction(|tx| {
        tx.enqueue(Mutation::new(
            "Edit",
            vec![create("Entry", "q", json!({"text":"queued","note":null}))],
        ))
    })
    .unwrap();
    let frozen = c.freeze().unwrap().unwrap();
    drop(c);
    // A database written before the Load ledger existed.
    raw(&path, "DROP TABLE axton_load; DROP TABLE axton_load_once");
    let mut c = open_db(&path);
    assert!(!c.schema_state().rebuilt);
    assert_eq!(c.pending_count().unwrap(), 1);
    assert_eq!(
        c.freeze().unwrap().unwrap(),
        frozen,
        "the queue is untouched"
    );
    assert_eq!(start(&mut c, once()).kind, LoadStartKind::Created);
    assert_eq!(table_count(&mut c, "axton_load_once"), 1);
}

// ---------------------------------------------------------------- lifecycle

#[test]
fn cancel_is_idempotent_fences_the_frozen_page_and_keeps_committed_pages() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open_db(&dir.path().join("db"));
    let id = start(&mut c, plain()).job.id;
    applied(store(&mut c, &id, &[("a", "A", 1)], Some(json!("p2"))));
    let frozen = fence(&mut c, &id);
    let cancelled = c.cancel_load(&id).unwrap();
    assert_eq!(cancelled.phase, LoadPhase::Cancelled);
    assert_eq!(cancelled.error.as_ref().unwrap().code, CANCELLED);
    assert_eq!((cancelled.pages, cancelled.call_id.clone()), (1, None));
    assert_eq!(c.cancel_load(&id).unwrap(), cancelled, "idempotent");
    // A page frozen before the cancel committed is inert.
    let late = c
        .store_load_page(&frozen, reply(load_page(&frozen, &[("b", "B", 1)], None)))
        .unwrap();
    assert!(matches!(late, LoadStored::Stale));
    assert!(entry(&mut c, "b").is_none());
    assert_eq!(
        entry(&mut c, "a").unwrap()["text"],
        "A",
        "committed data stays"
    );
    let failure = LoadFailure::transport("offline");
    assert_eq!(c.record_load_failure(&frozen, &failure).unwrap(), None);
    fails_with(c.retry_load(&id), NOT_RETRYABLE);
    // Cancelling a complete job leaves completion intact.
    let done = start(&mut c, plain()).job.id;
    applied(store(&mut c, &done, &[], None));
    let complete = c.cancel_load(&done).unwrap();
    assert_eq!(
        (complete.phase, complete.error),
        (LoadPhase::Complete, None)
    );
    fails_with(c.cancel_load(&uuid_like(1)), NOT_FOUND);
}

#[test]
fn forget_removes_only_terminal_jobs() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open_db(&dir.path().join("db"));
    let id = start(&mut c, plain()).job.id;
    let frozen = fence(&mut c, &id);
    fails_with(c.forget_load(&id), NOT_TERMINAL);
    assert!(c.get_load(&id).unwrap().is_some());
    c.cancel_load(&id).unwrap();
    c.forget_load(&id).unwrap();
    assert!(c.get_load(&id).unwrap().is_none());
    fails_with(c.forget_load(&id), NOT_FOUND);
    fails_with(c.cancel_load(&id), NOT_FOUND);
    fails_with(c.retry_load(&id), NOT_FOUND);
    let late = c
        .store_load_page(&frozen, reply(load_page(&frozen, &[("a", "A", 1)], None)))
        .unwrap();
    assert!(
        matches!(late, LoadStored::Stale),
        "a forgotten job takes no page"
    );
    assert!(entry(&mut c, "a").is_none());
    let done = start(&mut c, plain()).job.id;
    applied(store(&mut c, &done, &[("a", "A", 1)], None));
    c.forget_load(&done).unwrap();
    assert_eq!(
        entry(&mut c, "a").unwrap()["text"],
        "A",
        "forget deletes no Model"
    );
}

#[test]
fn backend_terminal_and_retryable_outcomes_are_recorded_apart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open_db(&path);
    let id = start(&mut c, plain()).job.id;
    let frozen = fence(&mut c, &id);
    let retryable = LoadPageResponse {
        load_id: frozen.load_id.clone(),
        call_id: frozen.call_id.clone(),
        outcome: LoadOutcome::Retryable {
            error: LoadError {
                code: "backend.unavailable".into(),
                message: "try later".into(),
            },
        },
        records: vec![],
    };
    let LoadStored::Retrying(waiting) = c.store_load_page(&frozen, reply(retryable)).unwrap()
    else {
        panic!("retrying")
    };
    assert_eq!(waiting.phase, LoadPhase::Pending);
    assert_eq!(
        (waiting.attempts, waiting.retry),
        (1, Some(LoadRetryClass::Backend))
    );
    assert_eq!(
        waiting.call_id.as_deref(),
        Some(frozen.call_id.as_str()),
        "same call ID"
    );
    drop(c);
    // Automatic recovery after reopen resends the same frozen call.
    let mut c = open_db(&path);
    let resumed = c
        .load_ready_pages(8, &BTreeSet::new())
        .unwrap()
        .pages
        .remove(0);
    assert_eq!(
        resumed.fence,
        LoadFence {
            replica: c.replica_generation(),
            ..frozen.clone()
        }
    );
    assert_eq!(resumed.attempts, 1);
    let transport = c
        .record_load_failure(&resumed.fence, &LoadFailure::transport("timeout"))
        .unwrap()
        .unwrap();
    assert_eq!(
        (transport.attempts, transport.retry),
        (2, Some(LoadRetryClass::Transport))
    );
    let resumed = resumed.fence;
    let rejected = failed(
        c.store_load_page(&resumed, reply(backend_failure(&resumed, "handler.failed")))
            .unwrap(),
    );
    assert_eq!(rejected.phase, LoadPhase::Failed);
    let error = rejected.error.clone().unwrap();
    assert_eq!(
        (error.code.as_str(), error.message.as_str()),
        ("handler.failed", "rejected")
    );
    assert_eq!(
        rejected.call_id.as_deref(),
        Some(frozen.call_id.as_str()),
        "the rejected call is kept"
    );
    assert_eq!(
        rejected.status().error,
        Some(LoadError {
            code: "handler.failed".into(),
            message: "rejected".into()
        })
    );
    assert!(
        c.load_ready_pages(8, &BTreeSet::new())
            .unwrap()
            .pages
            .is_empty()
    );
}

#[test]
fn a_malformed_correlated_page_fails_only_its_own_job() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open_db(&dir.path().join("db"));
    let broken = start(&mut c, plain()).job.id;
    let sibling = start(&mut c, plain()).job.id;
    let frozen = fence(&mut c, &broken);
    let mut page = load_page(&frozen, &[("a", "A", 1)], None);
    page.records.clear();
    let job = failed(c.store_load_page(&frozen, reply(page)).unwrap());
    assert_eq!(job.error.unwrap().code, PROTOCOL_INVALID);
    assert!(entry(&mut c, "a").is_none());
    let sibling = applied(store(&mut c, &sibling, &[("a", "A", 1)], None));
    assert_eq!(sibling.phase, LoadPhase::Complete);
}

#[test]
fn a_decoded_batch_fails_only_the_malformed_item() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open_db(&dir.path().join("db"));
    let ids: Vec<String> = (0..4).map(|_| start(&mut c, plain()).job.id).collect();
    let schedule = c.load_ready_pages(8, &BTreeSet::new()).unwrap();
    let request = LoadBatchRequest {
        loads: schedule.pages.iter().map(|p| p.intent.clone()).collect(),
    };
    let fences: Vec<LoadFence> = schedule.pages.iter().map(|p| p.fence.clone()).collect();
    let mut items: Vec<Value> = fences
        .iter()
        .map(|f| serde_json::to_value(load_page(f, &[("a", "A", 1)], None)).unwrap())
        .collect();
    // Malformed shape, a page over the identity bound and invalid next state.
    items[0]["outcome"]["status"] = json!("unknown");
    let many: Vec<Value> = (0..1001).map(|n| json!({"id": format!("x{n}")})).collect();
    items[1]["outcome"]["data"] = json!({ "entries": many });
    let mut deep = json!(1);
    for _ in 0..70 {
        deep = json!([deep]);
    }
    items[2]["outcome"]["next"] = json!({ "state": deep });
    let bytes = serde_json::to_vec(&json!({ "loads": items })).unwrap();
    let replies = LoadBatchResponse::decode(&bytes, &request).unwrap();
    let mut codes = vec![];
    for reply in replies {
        let fence = fences
            .iter()
            .find(|f| f.load_id == reply.load_id)
            .unwrap()
            .clone();
        match c.store_load_page(&fence, reply).unwrap() {
            LoadStored::Failed(job) => codes.push((job.id.clone(), job.error.unwrap().code)),
            LoadStored::Applied { job, .. } => codes.push((job.id.clone(), "applied".into())),
            other => panic!("{other:?}"),
        }
    }
    codes.sort_by_key(|(id, _)| ids.iter().position(|i| i == id));
    let codes: Vec<&str> = codes.iter().map(|(_, code)| code.as_str()).collect();
    assert_eq!(
        codes,
        vec![
            PROTOCOL_INVALID,
            PAGE_TOO_LARGE,
            INVALID_CONTINUATION,
            "applied"
        ]
    );
    assert_eq!(entry(&mut c, "a").unwrap()["text"], "A");
}

// ------------------------------------------------------ atomic application

#[test]
fn a_failing_hook_leaves_no_authority_hook_write_or_progress() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open_db(&dir.path().join("db"));
    let id = start(&mut c, plain()).job.id;
    let frozen = fence(&mut c, &id);
    let prepared = prepare(
        &mut c,
        &frozen,
        load_page(&frozen, &[("a", "A", 1)], Some(json!({"after":"a"}))),
    );
    assert!(prepared.load_refusal().is_none());
    assert_eq!(prepared.changes()["Entry"].len(), 1);
    hook_write(&mut c, "hooked");
    // The hook failed: the runtime rolls the whole unit back, then records.
    c.rollback_session().unwrap();
    let failure = LoadFailure::hook_failed("Entry", &[json!({"id":"a"})], "hook threw");
    assert!(failure.is_terminal());
    let job = c.record_load_failure(&frozen, &failure).unwrap().unwrap();
    assert_eq!(job.phase, LoadPhase::Failed);
    let error = job.error.clone().unwrap();
    assert_eq!(error.code, HOOK_FAILED);
    assert_eq!(
        error.diagnostics,
        vec![LoadDiagnostic {
            model: "Entry".into(),
            id: json!({"id":"a"}),
            code: "hookFailed".into()
        }]
    );
    assert!(entry(&mut c, "a").is_none(), "no authority");
    assert_eq!(stamp(&mut c, "a"), 0);
    assert!(entry(&mut c, "hooked").is_none(), "no hook write");
    assert_eq!(
        (job.pages, job.continuation.clone()),
        (0, None),
        "no progress"
    );
    assert_eq!(job.call_id.as_deref(), Some(frozen.call_id.as_str()));
}

#[test]
fn a_successful_hook_commits_with_the_page() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open_db(&dir.path().join("db"));
    let id = start(&mut c, plain()).job.id;
    let frozen = fence(&mut c, &id);
    let prepared = prepare(&mut c, &frozen, load_page(&frozen, &[("a", "A", 1)], None));
    hook_write(&mut c, "hooked");
    let result = c.apply_prepared_store(prepared).unwrap();
    c.commit_session().unwrap();
    let StoreResult::Load(LoadApply::Applied { job, report }) = result else {
        panic!("applied")
    };
    assert_eq!(report.applied, 1);
    assert_eq!((job.phase, job.pages), (LoadPhase::Complete, 1));
    let committed = c
        .read_sql(
            "SELECT (SELECT COUNT(*) FROM \"Entry\") AS rows, pages FROM axton_load WHERE load_id = ?",
            &[json!(id)],
        )
        .unwrap();
    assert_eq!(committed[0], json!({"rows":2,"pages":1}));
}

#[test]
fn one_invalid_record_refuses_the_whole_page_before_any_hook() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open_db(&dir.path().join("db"));
    let id = start(&mut c, plain()).job.id;
    let frozen = fence(&mut c, &id);
    let mut page = load_page(&frozen, &[("a", "A", 1), ("b", "B", 1)], Some(json!(2)));
    page.records[1].state = json!({"text": 99, "note": null});
    let prepared = prepare(&mut c, &frozen, page.clone());
    let Some(LoadFailure::Local(error)) = prepared.load_refusal().cloned() else {
        panic!("refused")
    };
    assert_eq!(error.code, STORE_FAILED);
    assert_eq!(
        error.diagnostics,
        vec![LoadDiagnostic {
            model: "Entry".into(),
            id: json!({"id":"b"}),
            code: "skipped".into()
        }]
    );
    assert!(
        c.apply_prepared_store(prepared).is_err(),
        "a refused preparation cannot be replayed"
    );
    assert!(!c.session_active());
    assert!(entry(&mut c, "a").is_none());
    // The reference sequence records it after the rollback.
    let job = failed(c.store_load_page(&frozen, reply(page)).unwrap());
    assert_eq!(job.error.unwrap().code, STORE_FAILED);
    assert!(
        entry(&mut c, "a").is_none(),
        "the valid record rolled back too"
    );
    assert_eq!(job.pages, 0);
}

#[test]
fn a_local_constraint_failure_has_the_same_no_progress_outcome() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open_db(&path);
    let id = start(&mut c, plain()).job.id;
    raw(
        &path,
        "CREATE TRIGGER refuse_bad BEFORE INSERT ON \"Entry\" WHEN NEW.id = 'bad' BEGIN SELECT RAISE(ABORT, 'refused'); END",
    );
    let job = failed(store(
        &mut c,
        &id,
        &[("a", "A", 1), ("bad", "B", 1)],
        Some(json!(1)),
    ));
    let error = job.error.unwrap();
    assert_eq!(error.code, STORE_FAILED);
    assert_eq!(error.diagnostics[0].id, json!({"id":"bad"}));
    assert!(entry(&mut c, "a").is_none());
    assert_eq!((job.pages, job.continuation), (0, None));
}

#[test]
fn a_preparation_error_keeps_the_call_id_for_backoff() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open_db(&path);
    let id = start(&mut c, plain()).job.id;
    let frozen = fence(&mut c, &id);
    raw(
        &path,
        "CREATE TRIGGER refuse_progress BEFORE UPDATE ON axton_load WHEN NEW.pages > OLD.pages BEGIN SELECT RAISE(ABORT, 'disk trouble'); END",
    );
    let LoadStored::Retrying(job) = store(&mut c, &id, &[("a", "A", 1)], None) else {
        panic!("retrying")
    };
    assert_eq!((job.attempts, job.retry), (1, Some(LoadRetryClass::Local)));
    assert_eq!(job.call_id.as_deref(), Some(frozen.call_id.as_str()));
    assert_eq!((job.phase, job.pages), (LoadPhase::Pending, 0));
    assert!(entry(&mut c, "a").is_none(), "nothing advanced");
    raw(&path, "DROP TRIGGER refuse_progress");
    let done = applied(store(&mut c, &id, &[("a", "A", 1)], None));
    assert_eq!(
        (done.phase, done.attempts, done.retry),
        (LoadPhase::Complete, 0, None)
    );
}

#[test]
fn failure_diagnostics_are_bounded() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open_db(&dir.path().join("db"));
    let id = start(&mut c, plain()).job.id;
    let frozen = fence(&mut c, &id);
    let ids: Vec<String> = (0..25).map(|n| format!("r{n}")).collect();
    let entries: Vec<(&str, &str, u64)> = ids.iter().map(|id| (id.as_str(), "x", 1)).collect();
    let mut page = load_page(&frozen, &entries, None);
    for record in &mut page.records {
        record.state = json!({"text": 1, "note": null});
    }
    let job = failed(c.store_load_page(&frozen, reply(page)).unwrap());
    let error = job.error.unwrap();
    assert_eq!(error.diagnostics.len(), 20);
    assert!(error.message.starts_with("25 of 25"), "{}", error.message);
}

#[test]
fn stamps_decide_content_and_only_divergent_equal_stamps_fail() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open_db(&dir.path().join("db"));
    let first = start(&mut c, plain()).job.id;
    applied(store(&mut c, &first, &[("a", "A", 3), ("b", "B", 1)], None));
    // An older and an equal identical stamp are valid no-ops.
    let second = start(&mut c, plain()).job.id;
    let job = applied(store(
        &mut c,
        &second,
        &[("a", "old", 2), ("b", "B", 1)],
        None,
    ));
    assert_eq!(job.phase, LoadPhase::Complete);
    assert_eq!(entry(&mut c, "a").unwrap()["text"], "A");
    // A repeated identity is applied once.
    let third = start(&mut c, plain()).job.id;
    let frozen = fence(&mut c, &third);
    let mut page = load_page(&frozen, &[("c", "C", 1)], None);
    page.outcome = LoadOutcome::Succeeded {
        data: json!({"entries":[{"id":"c"},{"id":"c"}]}),
        next: None,
    };
    let LoadStored::Applied { report, .. } = c.store_load_page(&frozen, reply(page)).unwrap()
    else {
        panic!("applied")
    };
    assert_eq!(report.applied, 1);
    // The same stamp with different content fails the page.
    let fourth = start(&mut c, plain()).job.id;
    let job = failed(store(
        &mut c,
        &fourth,
        &[("d", "D", 1), ("b", "changed", 1)],
        None,
    ));
    let error = job.error.unwrap();
    assert_eq!(error.code, STORE_FAILED);
    assert_eq!(error.diagnostics[0].code, "conflict");
    assert!(entry(&mut c, "d").is_none());
    assert_eq!(entry(&mut c, "b").unwrap()["text"], "B");
}

#[test]
fn loads_coexist_with_optimism_and_channel_delivery_by_stamp() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open_db(&dir.path().join("db"));
    subscribe(&mut c, "ch");
    c.apply_page(page("ch", 0, 1, Some("channel"))).unwrap();
    let edit = |text: &str| Operation {
        model: "Entry".into(),
        op: OperationKind::Update,
        identity: json!({"id":"e"}),
        values: Some(json!({ "text": text })),
    };
    c.transaction(|tx| tx.enqueue(Mutation::new("Edit", vec![edit("optimistic")])))
        .unwrap();
    let id = start(&mut c, plain()).job.id;
    applied(store(&mut c, &id, &[("e", "loaded", 2)], Some(json!(1))));
    assert_eq!(
        entry(&mut c, "e").unwrap()["text"],
        "optimistic",
        "optimism stays visible"
    );
    assert_eq!(stamp(&mut c, "e"), 2);
    // A later Channel update wins; an older Load page cannot regress it.
    c.apply_page(page("ch", 1, 5, Some("live"))).unwrap();
    applied(store(&mut c, &id, &[("e", "stale", 4)], None));
    assert_eq!(stamp(&mut c, "e"), 5);
    c.drop_mutation(1).unwrap();
    assert_eq!(entry(&mut c, "e").unwrap()["text"], "live");
}

#[test]
fn a_diverged_replay_does_not_fail_the_page() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open_db(&dir.path().join("db"));
    let ordinal = c
        .transaction(|tx| {
            tx.enqueue(Mutation::new(
                "Edit",
                vec![create("Entry", "n", json!({"text":"local","note":null}))],
            ))
        })
        .unwrap();
    let id = start(&mut c, plain()).job.id;
    let LoadStored::Applied { job, report } = store(&mut c, &id, &[("n", "server", 1)], None)
    else {
        panic!("applied")
    };
    assert_eq!(report.diverged(), 1);
    assert_eq!(report.reports[0].ordinal, Some(ordinal));
    assert_eq!((job.phase, job.error), (LoadPhase::Complete, None));
    assert_eq!(c.pending_count().unwrap(), 1, "the create is still sent");
}

// ---------------------------------------------------------------- retry

#[test]
fn retry_rereads_from_the_committed_continuation_under_a_new_call() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open_db(&path);
    let id = start(&mut c, plain()).job.id;
    applied(store(
        &mut c,
        &id,
        &[("a", "A", 1)],
        Some(json!({"after":"a"})),
    ));
    // 1. A hook failure.
    let old = fence(&mut c, &id);
    let prepared = prepare(&mut c, &old, load_page(&old, &[("b", "B", 1)], None));
    hook_write(&mut c, "hooked");
    drop(prepared);
    c.rollback_session().unwrap();
    c.record_load_failure(&old, &LoadFailure::hook_failed("Entry", &[], "hook threw"))
        .unwrap();
    let retried = c.retry_load(&id).unwrap();
    assert_eq!(
        (retried.phase, retried.run, retried.pages),
        (LoadPhase::Pending, 2, 1)
    );
    assert_ne!(retried.call_id.as_deref(), Some(old.call_id.as_str()));
    assert_eq!(retried.error, None);
    let intent = retried.intent.clone().unwrap();
    assert_eq!(
        intent.continuation,
        Some(Continuation {
            state: json!({"after":"a"})
        })
    );
    assert_eq!(intent.call_id, retried.call_id.clone().unwrap());
    assert_eq!(
        entry(&mut c, "a").unwrap()["text"],
        "A",
        "committed Models stay"
    );
    // A late answer to the old call ID is inert.
    let late = c
        .store_load_page(&old, reply(load_page(&old, &[("b", "B", 1)], None)))
        .unwrap();
    assert!(matches!(late, LoadStored::Stale));
    assert!(entry(&mut c, "b").is_none());
    assert_eq!(
        c.record_load_failure(&old, &LoadFailure::transport("late"))
            .unwrap(),
        None
    );
    // Retrying active work changes nothing.
    assert_eq!(c.retry_load(&id).unwrap(), retried);
    assert_eq!(
        c.load_ready_pages(8, &BTreeSet::new()).unwrap().pages.len(),
        1
    );
    // 2. A persisting store failure fails the new run again, then retries.
    let job = failed(store(
        &mut c,
        &id,
        &[("b", "B", 1), ("a", "changed", 1)],
        None,
    ));
    assert_eq!(
        (job.run, job.error.unwrap().code.as_str()),
        (2, STORE_FAILED)
    );
    let third = c.retry_load(&id).unwrap();
    assert_eq!((third.run, third.pages), (3, 1));
    // 3. A committed backend rejection, then a retry that completes.
    let current = fence(&mut c, &id);
    failed(
        c.store_load_page(&current, reply(backend_failure(&current, "loader.failed")))
            .unwrap(),
    );
    drop(c);
    let mut c = open_db(&path);
    let fourth = c.retry_load(&id).unwrap();
    assert_eq!(fourth.run, 4);
    assert_ne!(fourth.call_id.as_deref(), Some(current.call_id.as_str()));
    let done = applied(store(&mut c, &id, &[("b", "B", 1)], None));
    assert_eq!(
        (done.phase, done.pages, done.run),
        (LoadPhase::Complete, 2, 4)
    );
    fails_with(c.retry_load(&id), NOT_RETRYABLE);
}

// ---------------------------------------------------------------- once

#[test]
fn once_starts_of_one_key_share_one_job_and_ordinary_starts_ignore_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open_db(&path);
    let first = start(&mut c, once());
    assert_eq!(first.kind, LoadStartKind::Created);
    let joined = start(&mut c, once());
    assert_eq!(
        (joined.kind, &joined.job.id),
        (LoadStartKind::Joined, &first.job.id)
    );
    let same = c
        .start_load(
            "Entries",
            1,
            &json!({"since":null,"projectId":PROJECT.to_uppercase()}),
            once(),
        )
        .unwrap();
    assert_eq!(same.job.id, first.job.id, "the key is normalized");
    let independent = start(&mut c, plain());
    assert_ne!(independent.job.id, first.job.id);
    applied(store(&mut c, &independent.job.id, &[], None));
    assert_eq!(table_count(&mut c, "axton_load_once"), 1);
    drop(c);
    let mut c = open_db(&path);
    assert_eq!(start(&mut c, once()).job.id, first.job.id, "across reopen");
    assert_eq!(
        c.load_ready_pages(8, &BTreeSet::new()).unwrap().pages.len(),
        1,
        "a join schedules nothing new"
    );
}

#[test]
fn a_completed_once_hit_works_offline_across_reopen_without_applying_anything() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open_db(&path);
    let id = start(&mut c, once()).job.id;
    applied(store(&mut c, &id, &[("a", "A", 1)], None));
    let recent = c
        .start_load("Recent", 1, &json!({}), once())
        .unwrap()
        .job
        .id;
    applied(store(&mut c, &recent, &[("b", "B", 1)], None));
    drop(c);
    let mut c = open_db(&path);
    let generation = c.generation();
    let hit = start(&mut c, once());
    assert_eq!(hit.kind, LoadStartKind::Reused);
    assert_eq!(
        (hit.job.id.as_str(), hit.job.phase, hit.job.pages),
        (id.as_str(), LoadPhase::Complete, 1)
    );
    let no_args = c.start_load("Recent", 1, &json!({}), once()).unwrap();
    assert_eq!(
        (no_args.kind, no_args.job.id),
        (LoadStartKind::Reused, recent)
    );
    assert_eq!(c.generation(), generation, "a hit commits nothing");
    assert_eq!(table_count(&mut c, "axton_load"), 2);
    assert!(
        c.load_ready_pages(8, &BTreeSet::new())
            .unwrap()
            .pages
            .is_empty()
    );
}

#[test]
fn the_once_key_normalizes_arguments_and_separates_versions_and_contracts() {
    let schema = load_schema();
    let key = |schema: &Schema, version, args: Value| {
        load_once_key(schema, "Entries", version, &args)
            .unwrap()
            .key
    };
    let base = key(
        &schema,
        1,
        json!({"projectId":PROJECT,"since":"2026-01-01T00:00:00Z"}),
    );
    assert_eq!(
        key(
            &schema,
            1,
            json!({"since":"2026-01-01T02:00:00.000+02:00","projectId":PROJECT.to_uppercase()})
        ),
        base
    );
    assert_ne!(
        key(&schema, 1, json!({"projectId":PROJECT,"since":null})),
        base
    );
    assert_ne!(
        key(
            &schema,
            2,
            json!({"projectId":PROJECT,"since":"2026-01-01T00:00:00Z"})
        ),
        base,
        "another version"
    );
    let mut value = load_schema_value();
    value["models"][0]["version"] = json!(2);
    let contract = Schema::from_value(value).unwrap();
    assert_ne!(
        key(
            &contract,
            1,
            json!({"projectId":PROJECT,"since":"2026-01-01T00:00:00Z"})
        ),
        base,
        "another local Model read contract"
    );
    let tags = |list: Value| {
        load_once_key(&schema, "Tagged", 1, &json!({ "tags": list }))
            .unwrap()
            .key
    };
    assert_ne!(
        tags(json!(["a", "b"])),
        tags(json!(["b", "a"])),
        "list order matters"
    );
    assert_eq!(
        load_once_key(&schema, "Recent", 1, &json!({}))
            .unwrap()
            .args,
        "{}"
    );
    assert!(
        load_once_key(&schema, "Entries", 1, &json!({"projectId":PROJECT})).is_err(),
        "omitted is not null"
    );
}

#[test]
fn a_failed_once_job_is_returned_without_an_automatic_retry() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open_db(&dir.path().join("db"));
    let id = start(&mut c, once()).job.id;
    let frozen = fence(&mut c, &id);
    failed(
        c.store_load_page(&frozen, reply(backend_failure(&frozen, "handler.failed")))
            .unwrap(),
    );
    let hit = start(&mut c, once());
    assert_eq!(hit.kind, LoadStartKind::Reused);
    assert_eq!(
        (hit.job.id.as_str(), hit.job.phase, hit.job.run),
        (id.as_str(), LoadPhase::Failed, 1)
    );
    assert!(
        c.load_ready_pages(8, &BTreeSet::new())
            .unwrap()
            .pages
            .is_empty()
    );
    // Its explicit retry resumes the same mapped job.
    c.retry_load(&id).unwrap();
    let joined = start(&mut c, once());
    assert_eq!((joined.kind, joined.job.id), (LoadStartKind::Joined, id));
}

#[test]
fn refresh_joins_active_work_and_replaces_terminal_work_on_acceptance() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open_db(&dir.path().join("db"));
    let old = start(&mut c, once()).job.id;
    let joined = start(&mut c, refresh());
    assert_eq!((joined.kind, &joined.job.id), (LoadStartKind::Joined, &old));
    applied(store(&mut c, &old, &[("a", "A", 1)], Some(json!(1))));
    applied(store(&mut c, &old, &[], None));
    let replaced = start(&mut c, refresh());
    assert_eq!(replaced.kind, LoadStartKind::Created);
    let new = replaced.job.id;
    assert_ne!(new, old);
    assert_eq!(
        (replaced.job.pages, replaced.job.continuation),
        (0, None),
        "from the first page"
    );
    assert_eq!(
        job(&mut c, &old).phase,
        LoadPhase::Complete,
        "old handles keep completion"
    );
    assert_eq!(start(&mut c, once()).job.id, new);
    // A failed refresh stays the mapped failure; no stale completion returns.
    let frozen = fence(&mut c, &new);
    failed(
        c.store_load_page(&frozen, reply(backend_failure(&frozen, "handler.failed")))
            .unwrap(),
    );
    let hit = start(&mut c, once());
    assert_eq!(
        (hit.kind, hit.job.id.as_str(), hit.job.phase),
        (LoadStartKind::Reused, new.as_str(), LoadPhase::Failed)
    );
    let again = start(&mut c, refresh());
    assert_eq!(again.kind, LoadStartKind::Created);
    assert_eq!(table_count(&mut c, "axton_load_once"), 1);
    assert_eq!(table_count(&mut c, "axton_load"), 3);
}

#[test]
fn invalidation_removes_mappings_across_versions_and_nothing_else() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open_db(&dir.path().join("db"));
    let v1 = start(&mut c, once()).job.id;
    applied(store(&mut c, &v1, &[("a", "A", 1)], None));
    let v2 = c.start_load("Entries", 2, &args(), once()).unwrap().job.id;
    let tagged = c
        .start_load("Tagged", 1, &json!({"tags":["x"]}), once())
        .unwrap()
        .job
        .id;
    let other = c
        .start_load(
            "Entries",
            1,
            &json!({"projectId":PROJECT,"since":"2026-01-01T00:00:00Z"}),
            once(),
        )
        .unwrap()
        .job
        .id;
    let removed = c
        .invalidate_load(
            "Entries",
            &json!({"since":null,"projectId":PROJECT.to_uppercase()}),
        )
        .unwrap();
    assert_eq!(removed, 2);
    assert_eq!(table_count(&mut c, "axton_load"), 4, "no job is deleted");
    assert_eq!(
        job(&mut c, &v2).phase,
        LoadPhase::Pending,
        "nothing is cancelled"
    );
    assert_eq!(
        entry(&mut c, "a").unwrap()["text"],
        "A",
        "no Model is deleted"
    );
    assert_eq!(
        c.start_load("Tagged", 1, &json!({"tags":["x"]}), once())
            .unwrap()
            .job
            .id,
        tagged
    );
    assert_eq!(
        c.start_load(
            "Entries",
            1,
            &json!({"projectId":PROJECT,"since":"2026-01-01T00:00:00Z"}),
            once()
        )
        .unwrap()
        .job
        .id,
        other
    );
    let fresh = start(&mut c, once());
    assert_eq!(fresh.kind, LoadStartKind::Created);
    assert_ne!(fresh.job.id, v1);
    assert!(
        c.invalidate_load("Entries", &json!({"projectId":PROJECT}))
            .is_err(),
        "omitted is not null"
    );
    assert!(c.invalidate_load("Missing", &json!({})).is_err());
    assert_eq!(c.invalidate_load("Recent", &json!({})).unwrap(), 0);
}

#[test]
fn completion_after_invalidation_or_replacement_never_restores_a_mapping() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open_db(&dir.path().join("db"));
    // Invalidation racing completion.
    let first = start(&mut c, once()).job.id;
    c.invalidate_load("Entries", &args()).unwrap();
    applied(store(&mut c, &first, &[("a", "A", 1)], None));
    let second = start(&mut c, once());
    assert_eq!(second.kind, LoadStartKind::Created);
    let second = second.job.id;
    assert_ne!(second, first);
    // A replacement that completes before the invalidated job.
    c.invalidate_load("Entries", &args()).unwrap();
    let third = start(&mut c, once()).job.id;
    applied(store(&mut c, &third, &[("b", "B", 1)], None));
    applied(store(&mut c, &second, &[("b", "B", 1)], None));
    let hit = start(&mut c, once());
    assert_eq!(
        (hit.kind, hit.job.id.as_str()),
        (LoadStartKind::Reused, third.as_str())
    );
    // Management of old jobs never erases the newer mapping.
    c.forget_load(&first).unwrap();
    c.cancel_load(&second).unwrap();
    assert_eq!(start(&mut c, once()).job.id, third);
    let replacement = start(&mut c, refresh()).job.id;
    let frozen = fence(&mut c, &replacement);
    failed(
        c.store_load_page(&frozen, reply(backend_failure(&frozen, "handler.failed")))
            .unwrap(),
    );
    let newest = start(&mut c, refresh()).job.id;
    c.retry_load(&replacement).unwrap();
    c.cancel_load(&replacement).unwrap();
    c.forget_load(&third).unwrap();
    assert_eq!(start(&mut c, once()).job.id, newest);
}

#[test]
fn cancel_removes_its_own_mapping_and_complete_cancel_keeps_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open_db(&dir.path().join("db"));
    let active = start(&mut c, once()).job.id;
    c.cancel_load(&active).unwrap();
    assert_eq!(table_count(&mut c, "axton_load_once"), 0);
    let next = start(&mut c, once());
    assert_eq!(next.kind, LoadStartKind::Created);
    applied(store(&mut c, &next.job.id, &[], None));
    c.cancel_load(&next.job.id).unwrap();
    let hit = start(&mut c, once());
    assert_eq!(
        (hit.kind, hit.job.id),
        (LoadStartKind::Reused, next.job.id.clone())
    );
    c.forget_load(&next.job.id).unwrap();
    assert_eq!(
        start(&mut c, once()).kind,
        LoadStartKind::Created,
        "forget removed it"
    );
}

#[test]
fn a_mapping_to_a_missing_job_is_a_visible_ledger_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open_db(&path);
    let id = start(&mut c, once()).job.id;
    raw(
        &path,
        &format!("DELETE FROM axton_load WHERE load_id = '{id}'"),
    );
    fails_with(c.start_load("Entries", 1, &args(), once()), LEDGER_INVALID);
    fails_with(
        c.start_load("Entries", 1, &args(), refresh()),
        LEDGER_INVALID,
    );
    assert_eq!(start(&mut c, plain()).kind, LoadStartKind::Created);
    c.invalidate_load("Entries", &args()).unwrap();
    assert_eq!(start(&mut c, once()).kind, LoadStartKind::Created);
}

// ---------------------------------------------------------------- contracts

#[test]
fn a_compatible_reopen_keeps_retained_jobs_and_fails_removed_versions() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open_db(&path);
    let v1 = start(&mut c, plain()).job;
    let v2 = c.start_load("Entries", 2, &args(), plain()).unwrap().job;
    drop(c);
    // A later schema keeps v2 and drops v1.
    let mut value = load_schema_value();
    value["loads"].as_array_mut().unwrap().remove(0);
    let later = Schema::from_value(value).unwrap();
    let mut c = Client::open(SqliteStore::open(&path).unwrap(), later).unwrap();
    assert!(!c.schema_state().rebuilt);
    let removed = job(&mut c, &v1.id);
    assert_eq!(removed.phase, LoadPhase::Failed);
    assert_eq!(removed.error.unwrap().code, CONTRACT_UNAVAILABLE);
    assert_eq!(
        job(&mut c, &v2.id),
        v2,
        "a retained version keeps its exact frozen page"
    );
    fails_with(c.retry_load(&v1.id), CONTRACT_UNAVAILABLE);
    fails_with(c.start_load("Entries", 1, &args(), plain()), "unknown Load");
    drop(c);
    // Adding a version keeps in-flight jobs of the earlier one.
    let mut value = load_schema_value();
    let mut v3 = value["loads"][1].clone();
    v3["version"] = json!(3);
    value["loads"].as_array_mut().unwrap().push(v3);
    let mut c = Client::open(
        SqliteStore::open(&path).unwrap(),
        Schema::from_value(value).unwrap(),
    )
    .unwrap();
    assert_eq!(job(&mut c, &v2.id), v2);
}
