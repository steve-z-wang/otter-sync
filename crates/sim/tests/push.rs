//! Guarantees P1–P6 on the simulation, and the receipt's completion semantics:
//! duplicated, lost and retried receipts, overlapping records, rejected siblings.
use axton_core::PushReceipt;
use axton_sim::{Action, MutationSpec, Sim, schema::entry_key};

fn setup(seed: u64) -> Sim {
    let mut sim = Sim::new(seed, 1);
    sim.apply(Action::Subscribe {
        client: 0,
        scope: "a".into(),
    })
    .unwrap();
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::CreateEntry {
            id: "e1".into(),
            text: "base".into(),
        },
    })
    .unwrap();
    sim.settle();
    sim
}
fn edit(sim: &mut Sim, text: &str) {
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::Edit {
            id: "e1".into(),
            text: text.into(),
        },
    })
    .unwrap();
}

/// P1: a push whose receipt is lost is re-sent with the same bytes and executes
/// once; the retry is answered with the stored receipt.
#[test]
fn p1_lost_receipt_retry_executes_once_and_returns_the_stored_receipt() {
    let mut sim = setup(21);
    edit(&mut sim, "x");
    sim.apply(Action::Freeze { client: 0 }).unwrap();
    sim.apply(Action::Deliver).unwrap(); // executes, receipt queued
    assert_eq!(sim.host.handler_calls(), 2);
    sim.apply(Action::Drop).unwrap(); // receipt lost
    sim.apply(Action::Crash { client: 0 }).unwrap();
    sim.apply(Action::Restart { client: 0 }).unwrap();
    assert_eq!(sim.client(0).pending_count().unwrap(), 1);
    sim.apply(Action::Freeze { client: 0 }).unwrap(); // same batch again
    sim.apply(Action::Deliver).unwrap();
    assert_eq!(
        sim.host.handler_calls(),
        2,
        "cached receipt, no second execution"
    );
    sim.apply(Action::Deliver).unwrap(); // the stored receipt completes the batch
    assert_eq!(sim.client(0).pending_count().unwrap(), 0);
    let id = sim.client(0).client_id().to_string();
    let stored = PushReceipt::decode(sim.host.receipt(&id, 2).unwrap().as_bytes()).unwrap();
    assert_eq!(sim.clients[0].receipts[&2], stored);
    assert_eq!(sim.read_text(0, &entry_key("e1")).as_deref(), Some("x"));
    sim.settle();
    sim.check().unwrap();
}

/// P2: the client numbers batches contiguously; the server refuses a gap and an
/// overlap in process.
#[test]
fn p2_contiguous_sequence_and_server_refuses_gap_and_overlap() {
    let mut sim = setup(22);
    edit(&mut sim, "one");
    sim.apply(Action::Freeze { client: 0 }).unwrap();
    sim.settle();
    edit(&mut sim, "two");
    sim.apply(Action::Freeze { client: 0 }).unwrap();
    sim.settle();
    let sequences: Vec<u64> = sim.clients[0].receipts.keys().copied().collect();
    assert_eq!(sequences, vec![1, 2, 3]);
    assert_eq!(sim.client(0).last_completed_push().unwrap(), 3);
    // Hand-built gap and overlap against the host directly.
    let id = sim.client(0).client_id().to_string();
    let batch = |seq: u64| {
        let body = serde_json::json!({"capabilities":["scope-membership-v1"],"clientId":id,"batchSequence":seq,"models":axton_sim::schema::declared_models(),"mutations":[{"ordinal":99,"name":"Edit","version":1,"operations":[{"model":"Entry","op":"update","identity":{"id":"e1"},"values":{"text":"z"}}]}]});
        let bytes = axton_core::canonical_json(&body).unwrap().into_bytes();
        axton_core::PushRequest::decode(&bytes).unwrap();
        bytes
    };
    assert_eq!(sim.host.push("u", &batch(5)).unwrap_err(), "gap");
    assert_eq!(sim.host.push("u", &batch(2)).unwrap_err(), "overlap");
    sim.check().unwrap();
}

/// P3 is proven in crates/sqlite/tests/push.rs; the simulation adds the cross-batch
/// clause: a lifecycle dependent is sent only after its parent's receipt arrives.
#[test]
fn p3_lifecycle_dependent_waits_for_the_parent_receipt() {
    let mut sim = Sim::new(23, 1);
    sim.apply(Action::Subscribe {
        client: 0,
        scope: "a".into(),
    })
    .unwrap();
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::CreateEntry {
            id: "e1".into(),
            text: "new".into(),
        },
    })
    .unwrap();
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::CreateComment {
            id: "c1".into(),
            entry: "e1".into(),
            text: "child".into(),
        },
    })
    .unwrap();
    sim.apply(Action::Freeze { client: 0 }).unwrap();
    assert_eq!(sim.net.len(), 1);
    let bytes = match sim.net.pop().unwrap() {
        axton_sim::net::Message::Push { bytes, .. } => bytes,
        _ => unreachable!(),
    };
    let first = axton_core::PushRequest::decode(&bytes).unwrap();
    assert_eq!(
        first.mutations.len(),
        1,
        "the child is not in the parent's batch"
    );
    sim.apply(Action::Freeze { client: 0 }).unwrap();
    // freeze() is idempotent while a push is in flight (this is the P1 retry
    // mechanism): with no receipt yet for push 1, it re-encodes and re-sends that
    // same still-unacknowledged batch rather than opening a new one, so the queue
    // gains one message again here - but it is the parent's batch, byte-identical
    // to `first`, not a new batch carrying the child.
    assert_eq!(
        sim.net.len(),
        1,
        "freeze retries the parent's unacknowledged push rather than sending nothing"
    );
    let retried = match sim.net.pop().unwrap() {
        axton_sim::net::Message::Push { bytes, .. } => bytes,
        _ => unreachable!(),
    };
    assert_eq!(
        retried, bytes,
        "the retry is byte-identical to the parent's push; the child never entered a batch"
    );
    // Re-send the parent and let it through.
    sim.net
        .send(axton_sim::net::Message::Push { client: 0, bytes });
    sim.drain();
    sim.apply(Action::Freeze { client: 0 }).unwrap();
    assert_eq!(sim.net.len(), 1, "now the child goes");
    sim.settle();
    sim.check().unwrap();
}

/// P4: the frozen bytes are identical across a crash and across a duplicate freeze.
#[test]
fn p4_frozen_bytes_are_stable() {
    let mut sim = setup(24);
    edit(&mut sim, "x");
    let a = sim.client(0).freeze().unwrap().unwrap();
    let b = sim.client(0).freeze().unwrap().unwrap();
    assert_eq!(a, b);
    sim.apply(Action::Crash { client: 0 }).unwrap();
    sim.apply(Action::Restart { client: 0 }).unwrap();
    let c = sim.client(0).freeze().unwrap().unwrap();
    assert_eq!(a, c);
    sim.check().unwrap();
}

/// P5: a rejected mutation rolls back and its lifecycle dependent is rejected with it.
#[test]
fn p5_rejection_rolls_back_and_rejects_dependents() {
    let mut sim = Sim::new(25, 1);
    sim.apply(Action::Subscribe {
        client: 0,
        scope: "a".into(),
    })
    .unwrap();
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::CreateEntry {
            id: "e1".into(),
            text: "new".into(),
        },
    })
    .unwrap();
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::CreateComment {
            id: "c1".into(),
            entry: "e1".into(),
            text: "child".into(),
        },
    })
    .unwrap();
    sim.apply(Action::RejectNext {
        code: "entry.denied".into(),
    })
    .unwrap();
    sim.settle();
    assert_eq!(
        sim.read_text(0, &entry_key("e1")),
        None,
        "parent rolled back"
    );
    assert_eq!(
        sim.read_text(0, &axton_sim::schema::comment_key("c1")),
        None,
        "dependent rolled back"
    );
    let rejections = sim.client(0).rejections().unwrap();
    assert_eq!(rejections.len(), 2);
    assert!(rejections.iter().any(|r| r.code == "entry.denied"));
    assert!(rejections.iter().any(|r| r.code == "dependency.rejected"));
    assert_eq!(sim.host.handler_calls(), 1, "the dependent was never sent");
    sim.check().unwrap();
}

/// P6: a handler failure rejects only that mutation; the rest of the batch
/// commits normally, exactly like any other business rejection.
#[test]
fn p6_handler_failure_rejects_one_mutation_and_the_batch_commits() {
    let mut sim = setup(26);
    edit(&mut sim, "x");
    sim.apply(Action::FailNext).unwrap();
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::CreateEntry {
            id: "e2".into(),
            text: "yes".into(),
        },
    })
    .unwrap();
    sim.settle();
    let rejections = sim.client(0).rejections().unwrap();
    assert_eq!(rejections.len(), 1);
    assert_eq!(rejections[0].code, "handler.failed");
    assert_eq!(sim.read_text(0, &entry_key("e1")).as_deref(), Some("base"));
    assert_eq!(sim.host.state(&entry_key("e1")).unwrap()["text"], "base");
    assert_eq!(
        sim.read_text(0, &entry_key("e2")).as_deref(),
        Some("yes"),
        "the other mutation in the batch still commits"
    );
    assert_eq!(sim.host.state(&entry_key("e2")).unwrap()["text"], "yes");
    assert_eq!(sim.host.accepted(), 2, "the initial create and e2's create");
    assert_eq!(sim.host.rejected(), 1);
    sim.check().unwrap();
}

/// A broken transaction (a host infrastructure error on `rollback`, not a
/// handler rejection) fails the whole delivery: nothing is committed, and the
/// client's retry with the same bytes executes each mutation exactly once.
/// `rollback` only runs after a refused mutation, so this pairs `BreakNext`
/// with a rejection to reach it.
#[test]
fn p6_a_broken_transaction_fails_the_delivery_and_the_retry_executes_once() {
    let mut sim = setup(31);
    sim.apply(Action::RejectNext {
        code: "entry.denied".into(),
    })
    .unwrap();
    sim.apply(Action::BreakNext).unwrap();
    edit(&mut sim, "x");
    sim.apply(Action::Freeze { client: 0 }).unwrap();
    let bytes = match sim.net.pop().unwrap() {
        axton_sim::net::Message::Push { bytes, .. } => bytes,
        _ => unreachable!(),
    };
    sim.net.send(axton_sim::net::Message::Push {
        client: 0,
        bytes: bytes.clone(),
    });
    sim.apply(Action::Deliver).unwrap();
    assert_eq!(sim.host.state(&entry_key("e1")).unwrap()["text"], "base");
    assert_eq!(sim.host.head("a"), 1);
    assert_eq!(sim.host.stamp(&entry_key("e1")), 1);
    sim.drain(); // PushFailed is consumed
    assert_eq!(
        sim.client(0).freeze().unwrap().unwrap(),
        bytes,
        "same bytes on retry"
    );
    // The injected rejection was already consumed on the failed attempt, so
    // the retry runs the same mutation through to a real commit.
    sim.settle();
    assert_eq!(sim.host.state(&entry_key("e1")).unwrap()["text"], "x");
    assert_eq!(sim.host.stamp(&entry_key("e1")), 2);
    sim.check().unwrap();
}

/// Receipts the client holds are exactly what the server stored, and carry the
/// authority the framework read back.
#[test]
fn receipts_round_trip() {
    let mut sim = setup(27);
    edit(&mut sim, "x");
    sim.settle();
    let r: &PushReceipt = sim.clients[0].receipts.get(&2).unwrap();
    assert_eq!(r.records.len(), 1);
    assert_eq!(r.records[0].stamp, sim.host.stamp(&entry_key("e1")));
    assert_eq!(r.records[0].state["text"], "x");
    sim.check().unwrap();
}

/// A duplicated receipt is a no-op: it completes nothing twice, touches no row and
/// leaves an edit queued after the first copy exactly as it was.
#[test]
fn a_duplicated_receipt_is_a_no_op() {
    let mut sim = setup(28);
    edit(&mut sim, "x");
    sim.apply(Action::Freeze { client: 0 }).unwrap();
    sim.apply(Action::Deliver).unwrap(); // receipt queued
    sim.apply(Action::Duplicate).unwrap(); // twice
    sim.apply(Action::Deliver).unwrap(); // first copy completes the batch
    assert_eq!(sim.client(0).pending_count().unwrap(), 0);
    assert_eq!(sim.client(0).last_completed_push().unwrap(), 2);
    edit(&mut sim, "y");
    assert_eq!(sim.client(0).pending_count().unwrap(), 1);
    sim.apply(Action::Deliver).unwrap(); // second copy: stale
    assert_eq!(sim.client(0).pending_count().unwrap(), 1);
    assert_eq!(sim.client(0).last_completed_push().unwrap(), 2);
    assert_eq!(sim.read_text(0, &entry_key("e1")).as_deref(), Some("y"));
    assert_eq!(sim.client(0).record_stamp(&entry_key("e1")).unwrap(), 2);
    assert_eq!(sim.conflicts, 0);
    sim.check().unwrap();
    sim.settle();
    assert_eq!(sim.read_text(0, &entry_key("e1")).as_deref(), Some("y"));
    sim.check().unwrap();
}

/// Two mutations on the same record in one batch: the receipt reports that record
/// once, at the stamp of the last successful mutation, and both complete.
#[test]
fn overlapping_records_in_one_batch_complete_at_the_last_stamp() {
    let mut sim = setup(29);
    edit(&mut sim, "one");
    edit(&mut sim, "two");
    sim.apply(Action::Freeze { client: 0 }).unwrap();
    assert_eq!(sim.net.len(), 1);
    sim.apply(Action::Deliver).unwrap();
    assert_eq!(sim.host.handler_calls(), 3, "both mutations ran");
    sim.apply(Action::Deliver).unwrap();
    assert_eq!(sim.client(0).pending_count().unwrap(), 0);
    let r = &sim.clients[0].receipts[&2];
    assert_eq!(r.records.len(), 1, "one record, reported once");
    assert_eq!(r.records[0].stamp, 3);
    assert_eq!(r.records[0].state["text"], "two");
    assert_eq!(sim.host.stamp(&entry_key("e1")), 3);
    assert_eq!(sim.client(0).record_stamp(&entry_key("e1")).unwrap(), 3);
    assert_eq!(sim.read_text(0, &entry_key("e1")).as_deref(), Some("two"));
    assert_eq!(sim.client(0).before_image_count().unwrap(), 0);
    assert_eq!(sim.conflicts, 0);
    sim.check().unwrap();
}

/// A rejected mutation beside an accepted one in the same batch: the rejected
/// optimism is rolled back, the accepted record takes the server's authority, the
/// batch completes and the rejection is retained.
#[test]
fn a_rejected_mutation_beside_an_accepted_one() {
    let mut sim = setup(30);
    sim.apply(Action::RejectNext {
        code: "entry.denied".into(),
    })
    .unwrap();
    edit(&mut sim, "no"); // the first handler call: rejected
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::CreateEntry {
            id: "e2".into(),
            text: "yes".into(),
        },
    })
    .unwrap();
    sim.apply(Action::Freeze { client: 0 }).unwrap();
    assert_eq!(sim.net.len(), 1, "both in one batch");
    sim.apply(Action::Deliver).unwrap();
    sim.apply(Action::Deliver).unwrap();
    assert_eq!(sim.client(0).pending_count().unwrap(), 0);
    let r = &sim.clients[0].receipts[&2];
    assert_eq!(r.rejections.len(), 1);
    assert_eq!(r.rejections[0].code, "entry.denied");
    assert_eq!(r.records.len(), 1, "only the accepted record is reported");
    assert_eq!(r.records[0].identity["id"], "e2");
    assert_eq!(
        sim.read_text(0, &entry_key("e1")).as_deref(),
        Some("base"),
        "the rejected edit is rolled back"
    );
    assert_eq!(sim.host.state(&entry_key("e1")).unwrap()["text"], "base");
    assert_eq!(
        sim.host.stamp(&entry_key("e1")),
        1,
        "a rejection advances nothing"
    );
    assert_eq!(sim.read_text(0, &entry_key("e2")).as_deref(), Some("yes"));
    assert_eq!(sim.client(0).record_stamp(&entry_key("e2")).unwrap(), 1);
    let rejections = sim.client(0).rejections().unwrap();
    assert_eq!(rejections.len(), 1);
    assert_eq!(rejections[0].code, "entry.denied");
    assert_eq!(sim.conflicts, 0);
    sim.check().unwrap();
}
