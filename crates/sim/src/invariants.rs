//! What must be true after every step. Each check reads the clients and the host;
//! none of them mutates anything except the high-water marks on Sim. Two more
//! checks are stepwise rather than periodic and are called from `Sim::apply`:
//! `unsubscribe_cannot_remove_content` and `no_pending_operation_is_lost_on_reopen`.
use crate::{Sim, sim::ReopenState};
use axton_core::PushReceipt;
use serde_json::{Value, json};
use std::collections::BTreeSet;

type Check = fn(&mut Sim) -> Result<(), String>;

const CHECKS: &[(&str, Check)] = &[
    ("stamps never decrease", stamps_never_decrease),
    ("cursors never decrease", cursors_never_decrease),
    ("no mutation executes twice", no_mutation_executes_twice),
    ("no pending means converged", no_pending_means_converged),
    ("receipts match server", receipts_match_server),
    (
        "completed work had a matching response",
        completed_work_had_a_matching_response,
    ),
    (
        "republication cannot advance a stamp",
        republication_cannot_advance_a_stamp,
    ),
];

pub fn check(sim: &mut Sim) -> Result<(), String> {
    let mut failures = vec![];
    for (name, f) in CHECKS {
        if let Err(e) = f(sim) {
            failures.push(format!("{name}: {e}"));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("\n"))
    }
}

fn up(sim: &Sim) -> Vec<usize> {
    (0..sim.clients.len()).filter(|&i| sim.is_up(i)).collect()
}

/// Authority never regresses: a client's stamp for a record only ever grows. A
/// stamp row is retained across unsubscribe and deletion alike (it is the evidence
/// that keeps stale content from resurrecting a record), so the high-water mark
/// never needs forgetting; a client with no row yet simply has nothing to compare.
fn stamps_never_decrease(sim: &mut Sim) -> Result<(), String> {
    let keys = sim.host.stamped_keys();
    for i in up(sim) {
        for key in &keys {
            let rows = sim
                .client(i)
                .read_sql(
                    "SELECT stamp FROM axton_record WHERE model = ? AND identity = ?",
                    &[json!(key.model), json!(key.encoded_identity().unwrap())],
                )
                .map_err(|e| e.to_string())?;
            let Some(now) = rows.first().and_then(|r| r["stamp"].as_u64()) else {
                continue;
            };
            let slot = sim
                .seen_stamps
                .entry((i, key.encoded().unwrap()))
                .or_insert(0);
            if now < *slot {
                return Err(format!(
                    "client {i} {} stamp {now} < {}",
                    key.encoded().unwrap(),
                    *slot
                ));
            }
            *slot = now;
        }
    }
    Ok(())
}

fn cursors_never_decrease(sim: &mut Sim) -> Result<(), String> {
    for i in up(sim) {
        let subs = sim.client(i).subscriptions().map_err(|e| e.to_string())?;
        // Unsubscribing and resubscribing intentionally restarts a channel's cursor at
        // 0 (the next sync of that channel is a fresh one; the records it delivered
        // stay): forget the high-water mark for any channel the client is not
        // currently subscribed to, so that legitimate reset is not mistaken for a
        // regression.
        let subscribed: BTreeSet<String> = subs.iter().map(|(c, _)| c.clone()).collect();
        sim.seen_cursors
            .retain(|(ci, channel), _| *ci != i || subscribed.contains(channel));
        for (channel, cursor) in subs {
            let slot = sim.seen_cursors.entry((i, channel.clone())).or_insert(0);
            if cursor < *slot {
                return Err(format!(
                    "client {i} channel {channel} cursor {cursor} < {}",
                    *slot
                ));
            }
            *slot = cursor;
        }
    }
    Ok(())
}

/// No mutation executes twice: the (clientId, batchSequence, ordinal) triples the
/// host recorded for every `handle` call are pairwise distinct across the whole run.
/// A retry that reached the handler again (instead of being answered from the stored
/// receipt) would duplicate one of these triples.
fn no_mutation_executes_twice(sim: &mut Sim) -> Result<(), String> {
    let mut seen = BTreeSet::new();
    for triple in sim.host.handler_invocations() {
        if !seen.insert(triple.clone()) {
            return Err(format!(
                "handler invoked twice for client {} batch {} ordinal {}",
                triple.0, triple.1, triple.2
            ));
        }
    }
    Ok(())
}

fn normalized(state: Option<Value>, model: &str) -> Option<Value> {
    state.map(|v| {
        let mut m = v.as_object().cloned().unwrap_or_default();
        if model == "Entry" {
            m.entry("note").or_insert(Value::Null);
        }
        Value::Object(m)
    })
}

fn no_pending_means_converged(sim: &mut Sim) -> Result<(), String> {
    for i in up(sim) {
        if sim.client(i).pending_count().map_err(|e| e.to_string())? != 0 {
            continue;
        }
        for (channel, cursor) in sim.client(i).subscriptions().map_err(|e| e.to_string())? {
            if cursor != sim.host.head(&channel) {
                continue;
            }
            for key in sim.host.channel_records(&channel) {
                // A direct write shadows this (client, key) pair on purpose (N4/L4):
                // it never reaches the server, so no channel's invalidation stream
                // can ever agree with it. Exempt exactly this pair, not the whole
                // client or channel.
                if sim.direct_writes.contains(&(i, key.encoded().unwrap())) {
                    continue;
                }
                // The last delivery of this record to this client could not be
                // applied (a read failure or a skipped change): the client keeps
                // its earlier content on purpose until the record is delivered
                // again. Exempt exactly this pair; `Sim::settle` republishes it.
                if sim.stale_reads.contains(&(i, key.encoded().unwrap())) {
                    continue;
                }
                // A record's invalidation row on this channel outlives its
                // membership: removal keeps the row, and the scan skips it. Once
                // `channel` is no longer among the record's stored memberships,
                // being at its head proves nothing about this record: the
                // client's copy is retained data that only another channel it
                // follows could refresh.
                if !sim
                    .host
                    .stored_memberships(&key)
                    .iter()
                    .any(|m| m == &channel)
                {
                    continue;
                }
                // This channel's own invalidation for `key` is behind the record's
                // stamp when a change was published to other channels only: this
                // channel was never told, so its head says nothing about that change.
                if sim
                    .host
                    .channel_stamp(&channel, &key)
                    .is_some_and(|stamp| stamp < sim.host.stamp(&key))
                {
                    continue;
                }
                let local = sim.client(i).read(&key).map_err(|e| e.to_string())?;
                let server = normalized(sim.host.state(&key), &key.model);
                sim.comparisons += 1;
                if local != server {
                    return Err(format!(
                        "client {i} at head of {channel} but {} is {local:?}, server has {server:?} (not converged)",
                        key.encoded().unwrap()
                    ));
                }
            }
        }
    }
    Ok(())
}

fn receipts_match_server(sim: &mut Sim) -> Result<(), String> {
    for i in up(sim) {
        let id = sim.client(i).client_id().to_string();
        for (sequence, receipt) in sim.clients[i].receipts.clone() {
            if let Some(stored) = sim.host.receipt(&id, sequence) {
                let server = PushReceipt::decode(stored.as_bytes()).map_err(|e| e.to_string())?;
                if server != receipt {
                    return Err(format!(
                        "client {i} receipt {sequence} differs from the server's"
                    ));
                }
            }
        }
    }
    Ok(())
}

/// The sequences of the batches still in this client's queue.
fn queued_pushes(sim: &mut Sim, i: usize) -> Result<BTreeSet<u64>, String> {
    Ok(sim
        .client(i)
        .read_sql(
            "SELECT DISTINCT push FROM axton_mutation WHERE push IS NOT NULL",
            &[],
        )
        .map_err(|e| e.to_string())?
        .iter()
        .filter_map(|r| r["push"].as_u64())
        .collect())
}

/// Completed work had a matching response: a frozen batch leaves the queue only
/// through its receipt, and the client's completion counter names exactly the
/// highest batch that did so - a batch the server actually answered, never one it
/// has not seen.
fn completed_work_had_a_matching_response(sim: &mut Sim) -> Result<(), String> {
    for i in up(sim) {
        let queued = queued_pushes(sim, i)?;
        let mut highest = 0;
        for sequence in sim.clients[i].pushes.keys().copied().collect::<Vec<_>>() {
            if queued.contains(&sequence) {
                continue;
            }
            if !sim.clients[i].receipts.contains_key(&sequence) {
                return Err(format!(
                    "client {i} batch {sequence} left the queue without a receipt"
                ));
            }
            highest = highest.max(sequence);
        }
        let completed = sim
            .client(i)
            .last_completed_push()
            .map_err(|e| e.to_string())?;
        let id = sim.client(i).client_id().to_string();
        let answered = sim.host.client_sequence(&id);
        if completed > answered {
            return Err(format!(
                "client {i} completion counter {completed} claims a batch the server never answered (last answered {answered})"
            ));
        }
        if completed != highest {
            return Err(format!(
                "client {i} completion counter {completed} does not name the highest completed batch {highest}"
            ));
        }
    }
    Ok(())
}

/// Republication cannot advance a stamp: on the server, every record's stamp is
/// exactly the number of business changes committed to it, plus one if its first
/// publication had to initialize missing metadata. Publishing an existing record to
/// a channel, however often, contributes nothing.
fn republication_cannot_advance_a_stamp(sim: &mut Sim) -> Result<(), String> {
    for (key, stamp, advances, initialized) in sim.host.stamp_accounting() {
        let expected = advances + u64::from(initialized);
        if stamp != expected {
            return Err(format!(
                "{} is at stamp {stamp} after {advances} advances{}",
                key.encoded().unwrap(),
                if initialized {
                    " and one initialization"
                } else {
                    ""
                }
            ));
        }
    }
    Ok(())
}

/// Everything unsubscribing must leave alone: the visible rows of every model, the
/// stamp rows, and the pending work (queue and before images).
#[derive(Debug, PartialEq)]
pub struct ContentSnapshot {
    rows: Vec<Value>,
    stamps: Vec<Value>,
    pending: usize,
    before_images: usize,
}

pub fn content_snapshot(sim: &mut Sim, client: usize) -> Result<ContentSnapshot, String> {
    let c = sim.client(client);
    let mut rows = vec![];
    for model in ["Entry", "Comment"] {
        let sql = format!("SELECT '{model}' AS model, * FROM \"{model}\" ORDER BY id");
        rows.extend(c.read_sql(&sql, &[]).map_err(|e| e.to_string())?);
    }
    let stamps = c
        .read_sql(
            "SELECT model, identity, stamp FROM axton_record ORDER BY model, identity",
            &[],
        )
        .map_err(|e| e.to_string())?;
    Ok(ContentSnapshot {
        rows,
        stamps,
        pending: c.pending_count().map_err(|e| e.to_string())?,
        before_images: c.before_image_count().map_err(|e| e.to_string())?,
    })
}

/// Unsubscribe cannot remove content: called by `Sim::apply` right after an
/// `Action::Unsubscribe`, with the snapshot taken right before it.
pub fn unsubscribe_cannot_remove_content(
    sim: &mut Sim,
    client: usize,
    before: &ContentSnapshot,
) -> Result<(), String> {
    let after = content_snapshot(sim, client)?;
    if before.rows != after.rows {
        return Err(format!(
            "unsubscribe cannot remove content: client {client} rows changed from {:?} to {:?}",
            before.rows, after.rows
        ));
    }
    if before.stamps != after.stamps {
        return Err(format!(
            "unsubscribe cannot remove content: client {client} stamp evidence changed from {:?} to {:?}",
            before.stamps, after.stamps
        ));
    }
    if (before.pending, before.before_images) != (after.pending, after.before_images) {
        return Err(format!(
            "unsubscribe cannot remove content: client {client} pending work changed from {} operations / {} bases to {} / {}",
            before.pending, before.before_images, after.pending, after.before_images
        ));
    }
    Ok(())
}

/// The queued ordinals and the completion counter, as `Action::Crash` records them.
pub fn reopen_state(sim: &mut Sim, client: usize) -> Result<ReopenState, String> {
    let c = sim.client(client);
    let ordinals = c
        .read_sql("SELECT ordinal FROM axton_mutation", &[])
        .map_err(|e| e.to_string())?
        .iter()
        .filter_map(|r| r["ordinal"].as_u64())
        .collect();
    let completed = c.last_completed_push().map_err(|e| e.to_string())?;
    Ok((ordinals, completed))
}

/// No pending operation is lost on reopen: called by `Sim::apply` after an
/// `Action::Restart` reopened a crashed client, against what `Action::Crash` saw.
pub fn no_pending_operation_is_lost_on_reopen(
    sim: &mut Sim,
    client: usize,
    before: &ReopenState,
) -> Result<(), String> {
    let after = reopen_state(sim, client)?;
    if before.0 != after.0 {
        let lost: Vec<u64> = before.0.difference(&after.0).copied().collect();
        return Err(format!(
            "no pending operation is lost on reopen: client {client} queued {:?} before the crash, {:?} after; lost {lost:?}",
            before.0, after.0
        ));
    }
    if before.1 != after.1 {
        return Err(format!(
            "no pending operation is lost on reopen: client {client} completion counter was {} before the crash, {} after",
            before.1, after.1
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{Action, MutationSpec, Sim, schema::entry_key};

    #[test]
    fn invariants_hold_through_a_plain_round_trip() {
        let mut sim = Sim::new(3, 2);
        for i in 0..2 {
            sim.apply(Action::Subscribe {
                client: i,
                channel: "a".into(),
            })
            .unwrap();
        }
        sim.check().unwrap();
        sim.apply(Action::Enqueue {
            client: 0,
            mutation: MutationSpec::CreateEntry {
                id: "e1".into(),
                text: "hi".into(),
            },
        })
        .unwrap();
        sim.check().unwrap();
        sim.settle();
        sim.check().unwrap();
        assert_eq!(sim.read_text(1, &entry_key("e1")), Some("hi".into()));
    }

    #[test]
    fn a_violated_invariant_is_reported() {
        let mut sim = Sim::new(4, 1);
        sim.apply(Action::Subscribe {
            client: 0,
            channel: "a".into(),
        })
        .unwrap();
        sim.apply(Action::Enqueue {
            client: 0,
            mutation: MutationSpec::CreateEntry {
                id: "e1".into(),
                text: "hi".into(),
            },
        })
        .unwrap();
        sim.settle();
        // Corrupt the server behind the client's back: content differs at head.
        sim.host.set_state(
            &entry_key("e1"),
            Some(serde_json::json!({"id":"e1","text":"other","note":null})),
        );
        let err = sim.check().unwrap_err();
        assert!(err.contains("converged"), "{err}");
    }

    /// Run `sql` against a crashed client's file behind the engine's back, then
    /// restart it: the way these tests forge a state the engine never produces. The
    /// crash record is cleared first, since the reopen check would otherwise catch
    /// the forgery itself instead of the periodic checker under test.
    fn corrupt(sim: &mut Sim, client: usize, sql: &str) {
        use axton_client::store::ClientStore;
        sim.apply(Action::Crash { client }).unwrap();
        let mut store = axton_sqlite::SqliteStore::open(&sim.clients[client].path).unwrap();
        store.execute(sql, &[]).unwrap();
        drop(store);
        sim.clients[client].crash_state = None;
        sim.apply(Action::Restart { client }).unwrap();
    }

    fn frozen_batch(sim: &mut Sim, id: &str) {
        sim.apply(Action::Enqueue {
            client: 0,
            mutation: MutationSpec::CreateEntry {
                id: id.into(),
                text: "hi".into(),
            },
        })
        .unwrap();
        sim.apply(Action::Freeze { client: 0 }).unwrap();
    }

    #[test]
    fn a_batch_gone_without_a_receipt_is_reported() {
        let mut sim = Sim::new(6, 1);
        sim.apply(Action::Subscribe {
            client: 0,
            channel: "a".into(),
        })
        .unwrap();
        frozen_batch(&mut sim, "e1");
        sim.check().unwrap();
        corrupt(&mut sim, 0, "DELETE FROM axton_mutation");
        let err = sim.check().unwrap_err();
        assert!(
            err.contains("completed work had a matching response")
                && err.contains("batch 1 left the queue without a receipt"),
            "{err}"
        );
    }

    #[test]
    fn a_forged_completion_counter_is_reported() {
        let mut sim = Sim::new(5, 1);
        sim.apply(Action::Subscribe {
            client: 0,
            channel: "a".into(),
        })
        .unwrap();
        frozen_batch(&mut sim, "e1");
        sim.apply(Action::Deliver).unwrap(); // push reaches the server
        sim.apply(Action::Deliver).unwrap(); // receipt completes batch 1
        assert_eq!(sim.client(0).last_completed_push().unwrap(), 1);
        sim.check().unwrap();
        corrupt(
            &mut sim,
            0,
            "UPDATE axton_client SET last_completed_push = 9",
        );
        let err = sim.check().unwrap_err();
        assert!(
            err.contains("completed work had a matching response")
                && err.contains("completion counter 9 claims a batch the server never answered"),
            "{err}"
        );
    }

    #[test]
    fn a_regressed_stamp_is_reported() {
        let mut sim = Sim::new(7, 1);
        sim.apply(Action::Subscribe {
            client: 0,
            channel: "a".into(),
        })
        .unwrap();
        frozen_batch(&mut sim, "e1"); // stamp 1
        sim.settle();
        sim.apply(Action::ServerChange {
            key: "Entry:e1".into(),
            text: Some("v2".into()),
            channels: vec!["a".into()],
        })
        .unwrap(); // stamp 2
        sim.settle();
        assert_eq!(sim.client(0).record_stamp(&entry_key("e1")).unwrap(), 2);
        sim.check().unwrap();
        corrupt(
            &mut sim,
            0,
            "UPDATE axton_record SET stamp = stamp - 1 WHERE stamp > 1",
        );
        let err = sim.check().unwrap_err();
        assert!(
            err.contains("stamps never decrease") && err.contains("stamp 1 < 2"),
            "{err}"
        );
    }

    #[test]
    fn an_erased_pending_operation_is_reported_on_reopen() {
        use axton_client::store::ClientStore;
        let mut sim = Sim::new(8, 1);
        frozen_batch(&mut sim, "e1");
        sim.apply(Action::Crash { client: 0 }).unwrap();
        let mut store = axton_sqlite::SqliteStore::open(&sim.clients[0].path).unwrap();
        store.execute("DELETE FROM axton_mutation", &[]).unwrap();
        drop(store);
        let err = sim.apply(Action::Restart { client: 0 }).unwrap_err();
        assert!(
            err.contains("no pending operation is lost on reopen") && err.contains("lost [1]"),
            "{err}"
        );
    }

    #[test]
    fn an_unsubscribe_retains_content_and_resubscribing_restarts_the_cursor() {
        let mut sim = Sim::new(9, 1);
        sim.apply(Action::Subscribe {
            client: 0,
            channel: "a".into(),
        })
        .unwrap();
        frozen_batch(&mut sim, "e1");
        sim.settle();
        assert_eq!(sim.client(0).cursor("a").unwrap(), Some(1));
        // A second edit is left pending so the unsubscribe has queue state to keep.
        sim.apply(Action::Enqueue {
            client: 0,
            mutation: MutationSpec::Edit {
                id: "e1".into(),
                text: "pending".into(),
            },
        })
        .unwrap();
        assert_eq!(sim.client(0).pending_count().unwrap(), 1);
        // `apply` runs `unsubscribe_cannot_remove_content` itself.
        sim.apply(Action::Unsubscribe {
            client: 0,
            channel: "a".into(),
        })
        .unwrap();
        assert_eq!(
            sim.read_text(0, &entry_key("e1")).as_deref(),
            Some("pending")
        );
        assert_eq!(sim.client(0).pending_count().unwrap(), 1);
        assert_eq!(sim.client(0).record_stamp(&entry_key("e1")).unwrap(), 1);
        assert!(sim.client(0).subscriptions().unwrap().is_empty());
        sim.check().unwrap();
        sim.apply(Action::Subscribe {
            client: 0,
            channel: "a".into(),
        })
        .unwrap();
        // The cursor restarted at 0 in a new generation; that is not a regression.
        assert_eq!(sim.client(0).cursor("a").unwrap(), Some(0));
        sim.check().unwrap();
        sim.settle();
        assert_eq!(
            sim.read_text(0, &entry_key("e1")).as_deref(),
            Some("pending")
        );
        assert_eq!(sim.client(0).pending_count().unwrap(), 0);
        sim.check().unwrap();
    }
}
