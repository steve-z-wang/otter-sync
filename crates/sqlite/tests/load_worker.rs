//! The shared Load worker's decisions over a real SQLite ledger
//! ([#173](https://github.com/zanminwang/axton/issues/173)): bounded batches
//! with no waiting to fill, one page per job, a slot held until every answer
//! was consumed, oldest-ready order with requeue after a committed page, and
//! capped jittered backoff from persisted attempts. The test plays the
//! runtime: it applies each answer with the ledger's reference sequence and
//! tells the worker it was consumed. Time and entropy are values.
mod common;
use axton_client::*;
use axton_sqlite::SqliteStore;
use common::*;
use serde_json::{Value, json};
use std::path::Path;

const ENTROPY: u64 = 200;

fn open_db(path: &Path) -> Client<SqliteStore> {
    Client::open(SqliteStore::open(path).unwrap(), load_schema()).unwrap()
}
fn start(c: &mut Client<SqliteStore>) -> String {
    c.start_load("Recent", 1, &json!({}), LoadOptions::default())
        .unwrap()
        .job
        .id
}
fn dispatch(w: &mut LoadWorker, c: &mut Client<SqliteStore>, now: u64) -> Option<LoadDispatch> {
    w.dispatch(c, now, ENTROPY).unwrap().dispatch
}
fn loads(d: &LoadDispatch) -> Vec<String> {
    d.pages.iter().map(|p| p.fence.load_id.clone()).collect()
}
/// The response to a dispatched batch: every page answered by `answer`.
fn respond(d: &LoadDispatch, answer: impl Fn(&LoadFence) -> Value) -> Vec<u8> {
    let request: Value = serde_json::from_str(&d.body).unwrap();
    assert_eq!(request["loads"].as_array().unwrap().len(), d.pages.len());
    serde_json::to_vec(
        &json!({"loads": d.pages.iter().map(|p| answer(&p.fence)).collect::<Vec<_>>()}),
    )
    .unwrap()
}
fn page_value(fence: &LoadFence, next: Option<Value>) -> Value {
    serde_json::to_value(load_page(fence, &[], next)).unwrap()
}
/// Apply one received answer the way the runtime does, and consume it.
fn consume(w: &mut LoadWorker, c: &mut Client<SqliteStore>, now: u64) -> LoadStored {
    let received = w.next_outcome().expect("an outcome");
    let stored = match received.answer {
        LoadAnswer::Reply(reply) => c.store_load_page(&received.sent.fence, reply).unwrap(),
        LoadAnswer::Failure(failure) => {
            match c
                .record_load_failure(&received.sent.fence, &failure)
                .unwrap()
            {
                Some(job) if job.phase == LoadPhase::Failed => LoadStored::Failed(job),
                Some(job) => LoadStored::Retrying(job),
                None => LoadStored::Stale,
            }
        }
    };
    let id = received.sent.fence.load_id.clone();
    match &stored {
        LoadStored::Retrying(job) => w.back_off(
            &id,
            job.call_id.as_deref().unwrap(),
            job.attempts,
            now,
            ENTROPY,
        ),
        _ => w.settled(&id),
    }
    w.consumed(received.batch, &id);
    stored
}

#[test]
fn nine_ready_jobs_make_a_batch_of_eight_and_one_without_waiting_to_fill() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open_db(&dir.path().join("db"));
    let mut w = LoadWorker::default();
    // One ready job goes alone: nothing waits to fill a batch.
    let alone = start(&mut c);
    w.wake();
    assert!(w.wants_dispatch());
    let first = dispatch(&mut w, &mut c, 0).unwrap();
    assert_eq!(loads(&first), [alone]);
    w.answered(first.batch, &respond(&first, |f| page_value(f, None)))
        .unwrap();
    assert!(matches!(
        consume(&mut w, &mut c, 0),
        LoadStored::Applied { .. }
    ));
    // Nine ready jobs: the oldest eight, then one.
    let started: Vec<String> = (0..9).map(|_| start(&mut c)).collect();
    w.wake();
    let eight = dispatch(&mut w, &mut c, 0).unwrap();
    assert_eq!(loads(&eight), started[..8]);
    let request = LoadBatchRequest::decode_envelope(eight.body.as_bytes()).unwrap();
    assert_eq!(
        String::from_utf8(
            with_capabilities(&request.encode().unwrap(), &[STREAM_AUTHORITY_CAPABILITY]).unwrap()
        )
        .unwrap(),
        eight.body,
        "the canonical request body"
    );
    assert!(w.wants_dispatch(), "more may be ready");
    let one = dispatch(&mut w, &mut c, 0).unwrap();
    assert_eq!(loads(&one), started[8..]);
    assert_ne!(eight.batch, one.batch);
    assert_eq!(w.batches(), 2);
    assert!(!w.wants_dispatch(), "two batches out: no third");
}

#[test]
fn a_job_never_has_two_pages_in_flight() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open_db(&dir.path().join("db"));
    let mut w = LoadWorker::default();
    let a = start(&mut c);
    w.wake();
    let sent = dispatch(&mut w, &mut c, 0).unwrap();
    assert!(w.in_flight(&a));
    // Requested: a later dispatch leaves it out.
    let b = start(&mut c);
    w.wake();
    assert_eq!(loads(&dispatch(&mut w, &mut c, 0).unwrap()), [b]);
    // Answered and waiting for the writer: still left out.
    w.answered(
        sent.batch,
        &respond(&sent, |f| page_value(f, Some(json!(2)))),
    )
    .unwrap();
    w.wake();
    assert!(dispatch(&mut w, &mut c, 0).is_none());
    assert!(w.in_flight(&a));
    // Applied: its next page is a new frozen call and may go.
    assert!(matches!(
        consume(&mut w, &mut c, 0),
        LoadStored::Applied { .. }
    ));
    assert!(!w.in_flight(&a));
    assert!(w.wants_dispatch());
    let next = dispatch(&mut w, &mut c, 0).unwrap();
    assert_eq!(loads(&next), [a]);
    assert_ne!(next.pages[0].fence.call_id, sent.pages[0].fence.call_id);
}

#[test]
fn sixteen_unconsumed_outcomes_hold_both_slots_until_each_batch_is_consumed() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open_db(&dir.path().join("db"));
    let mut w = LoadWorker::default();
    let started: Vec<String> = (0..20).map(|_| start(&mut c)).collect();
    w.wake();
    let first = dispatch(&mut w, &mut c, 0).unwrap();
    let second = dispatch(&mut w, &mut c, 0).unwrap();
    for batch in [&first, &second] {
        w.answered(
            batch.batch,
            &respond(batch, |f| page_value(f, Some(json!(2)))),
        )
        .unwrap();
    }
    assert_eq!(w.waiting_outcomes(), 16);
    w.wake();
    assert!(!w.wants_dispatch(), "sixteen outcomes: no slot");
    assert!(dispatch(&mut w, &mut c, 0).is_none());
    // Seven of the first batch consumed: its slot is still held.
    for _ in 0..7 {
        consume(&mut w, &mut c, 0);
    }
    assert!(!w.wants_dispatch());
    assert!(dispatch(&mut w, &mut c, 0).is_none());
    // Its eighth frees the slot: the never-sent jobs go first, then the
    // requeued ones in commit order.
    consume(&mut w, &mut c, 0);
    assert!(w.wants_dispatch());
    let third = dispatch(&mut w, &mut c, 0).unwrap();
    let expected: Vec<String> = started[16..].iter().chain(&started[..4]).cloned().collect();
    assert_eq!(loads(&third), expected);
    assert!(!w.wants_dispatch());
}

#[test]
fn backoff_doubles_from_one_second_is_jittered_and_capped() {
    assert_eq!(load_backoff(1, 200), 1_000);
    assert_eq!(load_backoff(2, 200), 2_000);
    assert_eq!(load_backoff(3, 200), 4_000);
    assert_eq!(load_backoff(1, 0), 800, "-20 % jitter");
    assert_eq!(load_backoff(1, 400), 1_200, "+20 % jitter");
    assert_eq!(
        load_backoff(0, 200),
        1_000,
        "a first retry never waits less"
    );
    for attempts in [6, 7, 64, u64::MAX] {
        for entropy in [0, 200, 400, u64::MAX] {
            let delay = load_backoff(attempts, entropy);
            assert!(
                (24_000..=30_000).contains(&delay),
                "{attempts} {entropy}: {delay}"
            );
        }
    }
}

#[test]
fn a_job_backing_off_holds_back_no_ready_job_and_goes_again_when_due() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open_db(&dir.path().join("db"));
    let mut w = LoadWorker::default();
    let slow = start(&mut c);
    w.wake();
    let sent = dispatch(&mut w, &mut c, 0).unwrap();
    w.failed(sent.batch, LoadFailure::transport("offline"));
    let LoadStored::Retrying(job) = consume(&mut w, &mut c, 1_000) else {
        panic!("a retryable failure")
    };
    assert_eq!(
        job.call_id.as_deref(),
        Some(sent.pages[0].fence.call_id.as_str())
    );
    assert!(w.backing_off(&slow, 1_000));
    assert_eq!(w.next_due(1_000), Some(2_000));
    // Nine more jobs: the oldest one backs off, the next eight go.
    let others: Vec<String> = (0..9).map(|_| start(&mut c)).collect();
    w.wake();
    let batch = dispatch(&mut w, &mut c, 1_500).unwrap();
    assert_eq!(loads(&batch), others[..8]);
    // Due: it goes again under its frozen call, ahead of younger jobs.
    w.wake();
    let again = dispatch(&mut w, &mut c, 2_000).unwrap();
    assert_eq!(loads(&again), [slow.clone(), others[8].clone()]);
    assert_eq!(again.pages[0].fence.call_id, sent.pages[0].fence.call_id);
    assert_eq!(again.pages[0].attempts, 1);
}

#[test]
fn persisted_attempts_wait_a_fresh_bounded_delay_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open_db(&path);
    let mut w = LoadWorker::default();
    let id = start(&mut c);
    w.wake();
    let sent = dispatch(&mut w, &mut c, 0).unwrap();
    w.failed(sent.batch, LoadFailure::transport("offline"));
    consume(&mut w, &mut c, 0);
    w.wake();
    let again = dispatch(&mut w, &mut c, 1_000).unwrap();
    w.failed(again.batch, LoadFailure::transport("offline"));
    consume(&mut w, &mut c, 1_000);
    assert_eq!(c.get_load(&id).unwrap().unwrap().attempts, 2);
    drop(c);
    // A fresh worker over the reopened ledger: no monotonic deadline
    // survived, so the job waits the delay of its two attempts from now.
    let mut c = open_db(&path);
    let mut w = LoadWorker::default();
    w.wake();
    assert!(dispatch(&mut w, &mut c, 50_000).is_none());
    assert!(w.backing_off(&id, 50_000));
    assert_eq!(w.next_due(50_000), Some(52_000));
    w.wake();
    assert!(dispatch(&mut w, &mut c, 51_999).is_none());
    w.wake();
    let resent = dispatch(&mut w, &mut c, 52_000).unwrap();
    assert_eq!(resent.pages[0].fence.call_id, sent.pages[0].fence.call_id);
}

#[test]
fn an_uncorrelated_response_keeps_every_frozen_call_and_a_pause_counts_no_attempt() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open_db(&dir.path().join("db"));
    let mut w = LoadWorker::default();
    let a = start(&mut c);
    let b = start(&mut c);
    w.wake();
    let sent = dispatch(&mut w, &mut c, 0).unwrap();
    // Only one of two pages answered: the envelope is refused whole.
    let missing =
        serde_json::to_vec(&json!({"loads":[page_value(&sent.pages[0].fence, None)]})).unwrap();
    assert!(w.answered(sent.batch, &missing).is_err());
    for id in [&a, &b] {
        let LoadStored::Retrying(job) = consume(&mut w, &mut c, 0) else {
            panic!("retried")
        };
        assert_eq!(&job.id, id);
        assert_eq!(job.retry, Some(LoadRetryClass::Transport));
        assert_eq!(c.get_load(id).unwrap().unwrap().pages, 0);
    }
    // An abandoned (paused) request releases its slot and its jobs without
    // an attempt; once their delay passed they go at once.
    w.wake();
    let resent = dispatch(&mut w, &mut c, 1_000).unwrap();
    w.abandon(resent.batch);
    assert_eq!(w.batches(), 0);
    assert!(!w.in_flight(&a));
    w.wake();
    let after = dispatch(&mut w, &mut c, 1_000).unwrap();
    assert_eq!(loads(&after), [a.clone(), b.clone()]);
    assert_eq!(c.get_load(&a).unwrap().unwrap().attempts, 1);
}

#[test]
fn a_batch_stops_at_the_request_byte_bound() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open_db(&dir.path().join("db"));
    let mut w = LoadWorker::default();
    // Each frozen request carries about 300 KB of arguments: three fit in
    // one 1 MiB request, the fourth goes with the next batch.
    let started: Vec<String> = (0..4)
        .map(|n| {
            let tag = format!("{n}{}", "x".repeat(300_000));
            c.start_load(
                "Tagged",
                1,
                &json!({ "tags": [tag] }),
                LoadOptions::default(),
            )
            .unwrap()
            .job
            .id
        })
        .collect();
    w.wake();
    let first = dispatch(&mut w, &mut c, 0).unwrap();
    assert_eq!(loads(&first), started[..3]);
    assert!(first.body.len() <= limits::LOAD_REQUEST_BYTES);
    let second = dispatch(&mut w, &mut c, 0).unwrap();
    assert_eq!(loads(&second), started[3..]);
}

#[test]
fn damaged_rows_are_reported_once_and_skipped() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open_db(&path);
    let mut w = LoadWorker::default();
    let broken = start(&mut c);
    let healthy = start(&mut c);
    SqliteStore::open(&path)
        .unwrap()
        .execute_batch(&format!(
            "UPDATE axton_load SET intent = 'x' WHERE load_id = '{broken}'"
        ))
        .unwrap();
    w.wake();
    let step = w.dispatch(&mut c, 0, ENTROPY).unwrap();
    assert_eq!(
        step.issues
            .iter()
            .map(|i| i.load_id.clone())
            .collect::<Vec<_>>(),
        [broken]
    );
    assert_eq!(loads(&step.dispatch.unwrap()), [healthy]);
    w.wake();
    let step = w.dispatch(&mut c, 0, ENTROPY).unwrap();
    assert!(step.issues.is_empty(), "reported once");
    assert!(step.dispatch.is_none());
}

#[test]
fn a_page_that_cannot_be_sent_even_alone_fails_its_job_and_blocks_no_other() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open_db(&dir.path().join("db"));
    let mut w = LoadWorker::default();
    // The oldest ready job's frozen page is over the request bound.
    let oversized = oversized_next_page(&mut c);
    let healthy: Vec<String> = (0..2).map(|_| start(&mut c)).collect();
    w.wake();
    let step = w.dispatch(&mut c, 0, ENTROPY).unwrap();
    assert_eq!(
        loads(&step.dispatch.unwrap()),
        healthy,
        "the younger jobs go"
    );
    assert!(w.in_flight(&oversized), "its failure waits for the writer");
    assert_eq!(w.batches(), 1, "the failure shares the step's one slot");
    let LoadStored::Failed(job) = consume(&mut w, &mut c, 0) else {
        panic!("a terminal failure")
    };
    assert_eq!(job.id, oversized);
    let error = job.error.unwrap();
    assert_eq!(error.code, loads::REQUEST_TOO_LARGE);
    assert_eq!(job.pages, 1, "committed progress stays");
    assert!(!w.in_flight(&oversized));
    // Nothing is left to block: a later dispatch sends new work at once.
    let later = start(&mut c);
    w.wake();
    assert_eq!(loads(&dispatch(&mut w, &mut c, 0).unwrap()), [later]);
}

#[test]
fn a_rejected_request_splits_into_requests_of_one_and_a_rejected_one_fails() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open_db(&dir.path().join("db"));
    let mut w = LoadWorker::default();
    let started: Vec<String> = (0..3).map(|_| start(&mut c)).collect();
    w.wake();
    let sent = dispatch(&mut w, &mut c, 0).unwrap();
    assert_eq!(loads(&sent), started);
    w.rejected(sent.batch, "HTTP 413");
    assert_eq!(w.batches(), 0, "no failure: the slot is released");
    assert!(!w.has_outcome());
    // Each page now goes alone, two requests at a time.
    let one = dispatch(&mut w, &mut c, 0).unwrap();
    let two = dispatch(&mut w, &mut c, 0).unwrap();
    assert_eq!(loads(&one), [started[0].clone()]);
    assert_eq!(loads(&two), [started[1].clone()]);
    assert_eq!(one.pages[0].fence.call_id, sent.pages[0].fence.call_id);
    // A page refused alone fails its job.
    w.rejected(one.batch, "HTTP 413");
    let LoadStored::Failed(job) = consume(&mut w, &mut c, 0) else {
        panic!("refused alone")
    };
    assert_eq!(job.error.unwrap().code, loads::PROTOCOL_INVALID);
    // The third, still marked, goes alone too, even with a new job ready.
    let fresh = start(&mut c);
    let three = dispatch(&mut w, &mut c, 0).unwrap();
    assert_eq!(loads(&three), [started[2].clone()]);
    w.answered(two.batch, &respond(&two, |f| page_value(f, None)))
        .unwrap();
    consume(&mut w, &mut c, 0);
    assert_eq!(loads(&dispatch(&mut w, &mut c, 0).unwrap()), [fresh]);
}

#[test]
fn a_failed_scheduler_read_keeps_its_retry_until_a_dispatch_runs() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open_db(&dir.path().join("db"));
    let mut w = LoadWorker::default();
    let backing = start(&mut c);
    w.scan_failed(0, ENTROPY);
    assert_eq!(w.next_due(0), Some(1_000));
    // Another job's backoff, later or not, never hides the retry.
    w.back_off(&backing, "call", 3, 0, ENTROPY);
    assert_eq!(w.next_due(0), Some(1_000));
    w.settled(&backing);
    assert_eq!(
        w.next_due(0),
        Some(1_000),
        "nothing but a dispatch clears it"
    );
    w.wake();
    dispatch(&mut w, &mut c, 1_000);
    assert_eq!(w.next_due(1_000), None);
}

#[test]
fn unsendable_failures_count_against_the_slots_like_answers() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open_db(&dir.path().join("db"));
    let mut w = LoadWorker::default();
    let oversized: Vec<String> = (0..17).map(|_| oversized_next_page(&mut c)).collect();
    let healthy = start(&mut c);
    w.wake();
    // Each step fails at most eight, and each step's failures hold a slot
    // until they are consumed: sixteen waiting outcomes stop the worker.
    for _ in 0..2 {
        let step = w.dispatch(&mut c, 0, ENTROPY).unwrap();
        assert!(step.dispatch.is_none(), "nothing to send yet");
    }
    assert_eq!(w.waiting_outcomes(), 16);
    assert_eq!(w.batches(), 2);
    assert!(!w.wants_dispatch());
    assert!(w.dispatch(&mut c, 0, ENTROPY).unwrap().dispatch.is_none());
    assert_eq!(w.waiting_outcomes(), 16, "no seventeenth failure queued");
    for _ in 0..8 {
        let LoadStored::Failed(job) = consume(&mut w, &mut c, 0) else {
            panic!("a terminal failure")
        };
        assert_eq!(job.error.unwrap().code, loads::REQUEST_TOO_LARGE);
    }
    // One step's failures consumed: its slot frees, and the next step fails
    // the last oversized job and sends the healthy one.
    assert!(w.wants_dispatch());
    let step = w.dispatch(&mut c, 0, ENTROPY).unwrap();
    assert_eq!(loads(&step.dispatch.unwrap()), [healthy]);
    assert_eq!(w.waiting_outcomes(), 9);
    while w.has_outcome() {
        consume(&mut w, &mut c, 0);
    }
    for id in &oversized {
        assert_eq!(c.get_load(id).unwrap().unwrap().phase, LoadPhase::Failed);
    }
}

#[test]
fn delayed_load_page_keeps_epoch_across_restart_and_advances_continuation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open_db(&path);
    subscribe(&mut c, "a");
    c.apply_stream_page(StreamPullPage {
        cursors: [(
            "a".into(),
            CursorRange {
                from: 0,
                to: 1,
                head: 1,
            },
        )]
        .into(),
        changes: vec![StreamChange::Upsert {
            stream: "a".into(),
            cursor: 1,
            record: authority(Some("base"), 7),
        }],
    })
    .unwrap();
    let id = start(&mut c);
    let job = c.get_load(&id).unwrap().unwrap();
    let fence = LoadFence {
        replica: c.replica_generation(),
        load_id: id.clone(),
        run: job.run,
        call_id: job.call_id.unwrap(),
    };
    c.apply_stream_page(StreamPullPage {
        cursors: [(
            "a".into(),
            CursorRange {
                from: 1,
                to: 2,
                head: 2,
            },
        )]
        .into(),
        changes: vec![StreamChange::Remove {
            stream: "a".into(),
            cursor: 2,
            key: key(),
        }],
    })
    .unwrap();
    legacy_eviction(&mut c, &path, &key());
    let retry = c
        .record_load_failure(
            &fence,
            &LoadFailure::Retryable {
                class: LoadRetryClass::Transport,
                message: "lost response".into(),
            },
        )
        .unwrap()
        .unwrap();
    assert_eq!(retry.store_token.epoch, 0);
    assert_eq!(retry.call_id.as_deref(), Some(fence.call_id.as_str()));
    assert_eq!(retry.attempts, 1);
    drop(c);
    let mut c = open_db(&path);
    let retry = c.get_load(&id).unwrap().unwrap();
    assert_eq!(retry.store_token.epoch, 0);
    assert_eq!(retry.call_id.as_deref(), Some(fence.call_id.as_str()));
    let stored = c
        .store_load_page(
            &fence,
            reply(load_page(&fence, &[("e", "late", 99)], Some(json!("next")))),
        )
        .unwrap();
    let LoadStored::Applied { job, report } = stored else {
        panic!("page should settle")
    };
    assert_eq!(report.applied, 0, "old Load positive must be fenced");
    assert_eq!(job.pages, 1);
    assert_eq!(job.continuation.unwrap().state, json!("next"));
    assert!(c.read(&key()).unwrap().is_none());
    assert_eq!(c.record_stamp(&key()).unwrap(), 7);
    // The continuation is a new logical page, captured after the release.
    let next = LoadFence {
        replica: c.replica_generation(),
        load_id: id,
        run: job.run,
        call_id: job.call_id.unwrap(),
    };
    c.store_load_page(&next, reply(load_page(&next, &[("e", "fresh", 7)], None)))
        .unwrap();
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "fresh");
}

#[test]
fn legacy_load_and_queue_receive_epoch_zero_without_rewriting_saved_work() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open_db(&path);
    c.transaction(|tx| {
        tx.direct(Operation {
            model: "Entry".into(),
            identity: key().identity,
            op: OperationKind::Create,
            values: Some(json!({"text":"local","note":null})),
        })
    })
    .unwrap();
    c.transaction(|tx| tx.enqueue(mutation("pending"))).unwrap();
    let frozen = c.freeze().unwrap().unwrap();
    let id = start(&mut c);
    let saved = c.get_load(&id).unwrap().unwrap();
    drop(c);
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch("ALTER TABLE axton_client DROP COLUMN store_epoch; ALTER TABLE axton_mutation DROP COLUMN store_epoch; ALTER TABLE axton_load DROP COLUMN store_epoch;").unwrap();
    drop(conn);
    let mut c = open_db(&path);
    let job = c.get_load(&id).unwrap().unwrap();
    assert_eq!(job, saved);
    assert_eq!(job.store_token.epoch, 0);
    assert_eq!(c.freeze().unwrap().unwrap(), frozen);
    assert_eq!(c.pending_count().unwrap(), 1);
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "pending");
    assert_eq!(
        c.read_sql("SELECT store_epoch FROM axton_client", &[])
            .unwrap()[0]["store_epoch"],
        0
    );
    assert_eq!(
        c.read_sql("SELECT store_epoch FROM axton_mutation", &[])
            .unwrap()[0]["store_epoch"],
        0
    );
    drop(c);
    let mut c = open_db(&path);
    assert_eq!(c.get_load(&id).unwrap().unwrap(), saved);
    assert_eq!(c.freeze().unwrap().unwrap(), frozen);
}

#[test]
fn native_load_dispatch_advertises_scope_membership() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open_db(&dir.path().join("db"));
    start(&mut c);
    let mut w = LoadWorker::default();
    w.wake();
    let batch = dispatch(&mut w, &mut c, 0).unwrap();
    let envelope: Value = serde_json::from_str(&batch.body).unwrap();
    assert!(
        read_capabilities(&envelope)
            .unwrap()
            .contains(STREAM_AUTHORITY_CAPABILITY)
    );
}

#[test]
fn a_saved_exact_limit_page_reopens_with_its_identity_and_negotiation_headroom() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open_db(&path);
    let empty = c
        .start_load("Tagged", 1, &json!({"tags":[""]}), LoadOptions::default())
        .unwrap()
        .job;
    let ready = c
        .load_ready_pages(1, &std::collections::BTreeSet::new())
        .unwrap()
        .pages
        .remove(0);
    let overhead = LoadBatchRequest {
        loads: vec![ready.intent],
    }
    .encode()
    .unwrap()
    .len();
    c.cancel_load(&empty.id).unwrap();
    let args = json!({"tags":["x".repeat(limits::LOAD_REQUEST_BYTES - overhead)]});
    let saved = c
        .start_load("Tagged", 1, &args, LoadOptions::default())
        .unwrap()
        .job;
    let ready = c
        .load_ready_pages(1, &std::collections::BTreeSet::new())
        .unwrap()
        .pages
        .remove(0);
    let logical = LoadBatchRequest {
        loads: vec![ready.intent.clone()],
    }
    .encode()
    .unwrap();
    assert_eq!(logical.len(), limits::LOAD_REQUEST_BYTES);
    // A pre-upgrade frozen page carries no negotiation and retains its epoch.
    let before = c
        .read_sql(
            "SELECT call_id, intent, store_epoch FROM axton_load WHERE load_id=?",
            &[json!(saved.id)],
        )
        .unwrap();
    drop(c);
    let mut raw = SqliteStore::open(&path).unwrap();
    raw.execute_batch("ALTER TABLE axton_client DROP COLUMN stream_membership_version; ALTER TABLE axton_client DROP COLUMN store_epoch; ALTER TABLE axton_load DROP COLUMN store_epoch").unwrap();
    drop(raw);
    let mut c = open_db(&path);
    let mut w = LoadWorker::default();
    w.wake();
    let sent = dispatch(&mut w, &mut c, 0).expect("a valid saved page must remain sendable");
    assert_eq!(loads(&sent), std::slice::from_ref(&saved.id));
    assert_eq!(sent.pages[0].fence, ready.fence);
    let negotiation_bytes =
        serde_json::to_vec(&json!({"capabilities":[STREAM_AUTHORITY_CAPABILITY]}))
            .unwrap()
            .len()
            - 1;
    assert_eq!(
        sent.body.len(),
        limits::LOAD_REQUEST_BYTES + negotiation_bytes
    );
    assert_eq!(
        LoadBatchRequest::decode_envelope(sent.body.as_bytes())
            .unwrap()
            .loads[0],
        ready.intent
    );
    assert_eq!(
        c.read_sql(
            "SELECT call_id, intent, store_epoch FROM axton_load WHERE load_id=?",
            &[json!(saved.id)]
        )
        .unwrap(),
        before
    );
    assert_eq!(c.get_load(&saved.id).unwrap().unwrap().phase, saved.phase);
    assert!(w.next_outcome().is_none());
}

#[test]
fn multiple_pages_partition_at_the_final_wire_bound_without_single_page_headroom() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open_db(&dir.path().join("db"));
    let probes: Vec<_> = (0..2)
        .map(|_| {
            c.start_load("Tagged", 1, &json!({"tags":[""]}), LoadOptions::default())
                .unwrap()
                .job
        })
        .collect();
    let intents = c
        .load_ready_pages(2, &std::collections::BTreeSet::new())
        .unwrap()
        .pages
        .into_iter()
        .map(|p| p.intent)
        .collect();
    let overhead = LoadBatchRequest { loads: intents }.encode().unwrap().len();
    for job in probes {
        c.cancel_load(&job.id).unwrap();
    }
    let remaining = limits::LOAD_REQUEST_BYTES - overhead;
    let ids: Vec<_> = [remaining / 2, remaining - remaining / 2]
        .into_iter()
        .map(|size| {
            c.start_load(
                "Tagged",
                1,
                &json!({"tags":["x".repeat(size)]}),
                LoadOptions::default(),
            )
            .unwrap()
            .job
            .id
        })
        .collect();
    let intents = c
        .load_ready_pages(2, &std::collections::BTreeSet::new())
        .unwrap()
        .pages
        .into_iter()
        .map(|p| p.intent)
        .collect();
    assert_eq!(
        LoadBatchRequest { loads: intents }.encode().unwrap().len(),
        limits::LOAD_REQUEST_BYTES
    );
    let mut w = LoadWorker::default();
    w.wake();
    let first = dispatch(&mut w, &mut c, 0).unwrap();
    let second = dispatch(&mut w, &mut c, 0).unwrap();
    assert_eq!(loads(&first), ids[..1]);
    assert_eq!(loads(&second), ids[1..]);
    assert!(first.body.len() <= limits::LOAD_REQUEST_BYTES);
    assert!(second.body.len() <= limits::LOAD_REQUEST_BYTES);
}
