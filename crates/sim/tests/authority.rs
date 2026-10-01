//! Authority on the simulation: a push completes from its receipt, stream pages and
//! receipts carry the same stamps, and neither can regress the other.
use axton_sim::{Action, MutationSpec, Sim, schema::entry_key};

fn setup(seed: u64) -> Sim {
    let mut sim = Sim::new(seed, 1);
    sim.apply(Action::Subscribe {
        client: 0,
        stream: "a".into(),
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

fn edit(sim: &mut Sim, client: usize, text: &str) {
    sim.apply(Action::Enqueue {
        client,
        mutation: MutationSpec::Edit {
            id: "e1".into(),
            text: text.into(),
        },
    })
    .unwrap();
}

/// The value the server stored replaces the optimistic value through the receipt
/// alone; a later pending edit replays on top of that base.
#[test]
fn a1_server_value_overrides_optimism_and_later_edits_replay() {
    let mut sim = setup(31);
    edit(&mut sim, 0, "mine");
    // The server "normalizes" by storing something else for the same mutation.
    sim.host.uppercase_next();
    sim.apply(Action::Freeze { client: 0 }).unwrap();
    sim.apply(Action::Deliver).unwrap(); // executed: the server holds "MINE"
    assert_eq!(sim.host.state(&entry_key("e1")).unwrap()["text"], "MINE");
    edit(&mut sim, 0, "later");
    assert_eq!(sim.read_text(0, &entry_key("e1")).as_deref(), Some("later"));
    sim.apply(Action::Deliver).unwrap(); // receipt for batch 2 completes it
    assert_eq!(
        sim.client(0).pending_count().unwrap(),
        1,
        "only the later edit"
    );
    assert_eq!(
        sim.read_text(0, &entry_key("e1")).as_deref(),
        Some("later"),
        "pending edit replays on the new base"
    );
    let base = sim
        .client(0)
        .read_sql("SELECT text FROM axton_before_Entry", &[])
        .unwrap();
    assert_eq!(
        base[0]["text"], "MINE",
        "the base beneath it is the server's value, from the receipt"
    );
    assert_eq!(sim.client(0).record_stamp(&entry_key("e1")).unwrap(), 2);
    sim.settle();
    assert_eq!(sim.read_text(0, &entry_key("e1")).as_deref(), Some("later"));
    assert_eq!(sim.conflicts, 0);
    sim.check().unwrap();
}

/// A2: a page whose stream range is already covered is stale and does not move the cursor back;
/// a page ahead is refused.
#[test]
fn a2_pages_apply_only_in_cursor_order() {
    let mut sim = setup(32);
    sim.apply(Action::ServerChange {
        key: "Entry:e1".into(),
        text: Some("v2".into()),
        streams: vec!["a".into()],
    })
    .unwrap();
    sim.apply(Action::Pull { client: 0 }).unwrap();
    sim.apply(Action::Deliver).unwrap();
    sim.apply(Action::Duplicate).unwrap(); // the same page twice
    sim.apply(Action::Deliver).unwrap();
    assert_eq!(sim.client(0).cursor("a").unwrap(), Some(2));
    sim.apply(Action::Deliver).unwrap(); // stale duplicate
    assert_eq!(sim.client(0).cursor("a").unwrap(), Some(2));
    sim.check().unwrap();
}

/// The receipt and the stream page for the same change carry the same stamp and
/// content. Whichever arrives first, the receipt completes the batch, the page moves
/// the cursor, nothing conflicts and the row is the server's.
#[test]
fn reordered_receipt_and_page_agree_in_either_order() {
    for page_first in [false, true] {
        let mut sim = setup(33);
        edit(&mut sim, 0, "x");
        sim.apply(Action::Freeze { client: 0 }).unwrap();
        sim.apply(Action::Deliver).unwrap(); // executed, receipt queued
        sim.apply(Action::Pull { client: 0 }).unwrap();
        sim.apply(Action::Hold).unwrap(); // receipt to the back
        sim.apply(Action::Deliver).unwrap(); // pull request -> page queued
        if page_first {
            sim.apply(Action::Hold).unwrap(); // receipt to the back again, page first
        }
        sim.apply(Action::Deliver).unwrap();
        let pending_after_first = sim.client(0).pending_count().unwrap();
        if page_first {
            assert_eq!(pending_after_first, 1, "a page never completes a push");
            assert_eq!(sim.client(0).cursor("a").unwrap(), Some(2));
        } else {
            assert_eq!(pending_after_first, 0, "the receipt completes it alone");
            assert_eq!(sim.client(0).cursor("a").unwrap(), Some(1));
        }
        sim.apply(Action::Deliver).unwrap();
        assert_eq!(sim.client(0).pending_count().unwrap(), 0);
        assert_eq!(sim.client(0).cursor("a").unwrap(), Some(2));
        assert_eq!(sim.read_text(0, &entry_key("e1")).as_deref(), Some("x"));
        assert_eq!(sim.client(0).record_stamp(&entry_key("e1")).unwrap(), 2);
        assert_eq!(sim.host.stamp(&entry_key("e1")), 2);
        assert_eq!(sim.conflicts, 0, "page_first {page_first}");
        sim.check().unwrap();
    }
}

/// HTTP-only completion: a client that follows no stream at all still completes
/// its push from the receipt, with the server's row and the server's stamp.
#[test]
fn a_push_completes_from_its_receipt_with_zero_subscriptions() {
    let mut sim = Sim::new(34, 1);
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::CreateEntry {
            id: "e9".into(),
            text: "nowhere".into(),
        },
    })
    .unwrap();
    assert!(sim.client(0).subscriptions().unwrap().is_empty());
    sim.host.uppercase_next();
    sim.apply(Action::Freeze { client: 0 }).unwrap();
    sim.apply(Action::Deliver).unwrap(); // push
    sim.apply(Action::Deliver).unwrap(); // receipt
    assert_eq!(sim.client(0).pending_count().unwrap(), 0);
    assert_eq!(sim.client(0).last_completed_push().unwrap(), 1);
    assert_eq!(
        sim.read_text(0, &entry_key("e9")).as_deref(),
        Some("NOWHERE"),
        "the row is the server's, not the optimism"
    );
    assert_eq!(sim.host.state(&entry_key("e9")).unwrap()["text"], "NOWHERE");
    assert_eq!(
        sim.client(0).record_stamp(&entry_key("e9")).unwrap(),
        sim.host.stamp(&entry_key("e9"))
    );
    assert_eq!(sim.client(0).before_image_count().unwrap(), 0);
    assert!(
        sim.client(0).freeze().unwrap().is_none(),
        "nothing is re-sent"
    );
    sim.check().unwrap();
}

/// A handler that publishes nowhere (a record with no stream membership) is a
/// legal outcome: the change is stamped, read back and returned; no stream moves.
#[test]
fn a_change_published_to_no_stream_still_completes() {
    let mut sim = Sim::new(35, 1);
    sim.apply(Action::Subscribe {
        client: 0,
        stream: "a".into(),
    })
    .unwrap();
    sim.host.set_membership(&entry_key("e9"), &[]);
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::CreateEntry {
            id: "e9".into(),
            text: "quiet".into(),
        },
    })
    .unwrap();
    sim.settle();
    assert_eq!(sim.client(0).pending_count().unwrap(), 0);
    assert_eq!(sim.read_text(0, &entry_key("e9")).as_deref(), Some("quiet"));
    assert_eq!(sim.host.head("a"), 0, "no stream was told");
    assert_eq!(sim.client(0).record_stamp(&entry_key("e9")).unwrap(), 1);
    sim.check().unwrap();
}

/// A2: a page pulled from stream "a" before an Unsubscribe/Subscribe cycle can
/// still be in flight when the resubscribe resets the stream's cursor to 0; it is
/// dropped as stale, a page from a previous subscription, rather than treated as a
/// gap or applied against the reset cursor. The nine-action repro from issue #32.
#[test]
fn a2_page_from_a_previous_subscription_is_stale_not_a_gap() {
    let mut sim = Sim::new(36, 1);
    sim.apply(Action::Subscribe {
        client: 0,
        stream: "a".into(),
    })
    .unwrap();
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::CreateEntry {
            id: "e1".into(),
            text: "1".into(),
        },
    })
    .unwrap();
    sim.apply(Action::Freeze { client: 0 }).unwrap();
    sim.drain();
    sim.apply(Action::Pull { client: 0 }).unwrap();
    sim.drain();
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::CreateEntry {
            id: "e2".into(),
            text: "2".into(),
        },
    })
    .unwrap();
    sim.apply(Action::Freeze { client: 0 }).unwrap();
    sim.drain();
    sim.apply(Action::Pull { client: 0 }).unwrap();
    sim.apply(Action::Unsubscribe {
        client: 0,
        stream: "a".into(),
    })
    .unwrap();
    sim.apply(Action::Subscribe {
        client: 0,
        stream: "a".into(),
    })
    .unwrap();
    assert!(
        sim.apply(Action::Deliver).is_ok(),
        "the client must drop the stale page rather than error"
    );
    assert_eq!(sim.client(0).cursor("a").unwrap(), Some(0));
    assert_eq!(
        sim.read_text(0, &entry_key("e2")).as_deref(),
        Some("2"),
        "the rows delivered before the cycle are retained"
    );
    sim.check().unwrap();
}

/// Completion never waits for a stream: two batches on streams the client does
/// not pull (one it follows, one it does not) both complete on their receipts, in
/// sequence, while every cursor stays where it was.
#[test]
fn batches_complete_on_their_receipts_without_any_page() {
    let mut sim = Sim::new(37, 1);
    sim.apply(Action::Subscribe {
        client: 0,
        stream: "slow".into(),
    })
    .unwrap();
    sim.host.set_membership(&entry_key("s"), &["slow"]);
    sim.host.set_membership(&entry_key("n"), &["other"]);
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::CreateEntry {
            id: "s".into(),
            text: "1".into(),
        },
    })
    .unwrap();
    sim.apply(Action::Freeze { client: 0 }).unwrap();
    sim.apply(Action::Deliver).unwrap(); // batch 1 executed
    sim.apply(Action::Deliver).unwrap(); // receipt 1 completes it
    assert_eq!(sim.client(0).pending_count().unwrap(), 0);
    assert_eq!(sim.client(0).last_completed_push().unwrap(), 1);
    assert_eq!(
        sim.client(0).cursor("slow").unwrap(),
        Some(0),
        "no page was pulled"
    );
    assert_eq!(sim.host.head("slow"), 1);
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::CreateEntry {
            id: "n".into(),
            text: "2".into(),
        },
    })
    .unwrap();
    sim.apply(Action::Freeze { client: 0 }).unwrap();
    sim.apply(Action::Deliver).unwrap(); // batch 2 executed
    sim.apply(Action::Deliver).unwrap(); // receipt 2 completes it
    assert_eq!(sim.client(0).pending_count().unwrap(), 0);
    assert_eq!(sim.client(0).last_completed_push().unwrap(), 2);
    assert_eq!(sim.read_text(0, &entry_key("n")).as_deref(), Some("2"));
    assert_eq!(sim.client(0).record_stamp(&entry_key("n")).unwrap(), 1);
    sim.check().unwrap();
    sim.settle();
    assert_eq!(sim.client(0).cursor("slow").unwrap(), Some(1));
    assert_eq!(sim.conflicts, 0);
    sim.check().unwrap();
}

/// Deletion through a receipt: the deleting client's row goes and its stamp is
/// retained; a subscribed peer receives the same deletion at the same stamp through
/// the stream.
#[test]
fn deletion_completes_from_the_receipt_and_reaches_a_peer_at_the_same_stamp() {
    let mut sim = Sim::new(38, 2);
    for i in 0..2 {
        sim.apply(Action::Subscribe {
            client: i,
            stream: "a".into(),
        })
        .unwrap();
    }
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::CreateEntry {
            id: "e1".into(),
            text: "doomed".into(),
        },
    })
    .unwrap();
    sim.settle();
    assert_eq!(
        sim.read_text(1, &entry_key("e1")).as_deref(),
        Some("doomed")
    );
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::DeleteEntry { id: "e1".into() },
    })
    .unwrap();
    sim.apply(Action::Freeze { client: 0 }).unwrap();
    sim.apply(Action::Deliver).unwrap();
    sim.apply(Action::Deliver).unwrap(); // the receipt carries the null state
    assert_eq!(sim.client(0).pending_count().unwrap(), 0);
    assert_eq!(sim.read_text(0, &entry_key("e1")), None);
    let stamp = sim.host.stamp(&entry_key("e1"));
    assert_eq!(stamp, 2);
    assert_eq!(
        sim.client(0).record_stamp(&entry_key("e1")).unwrap(),
        stamp,
        "the deletion's stamp is retained as evidence"
    );
    assert_eq!(
        sim.read_text(1, &entry_key("e1")).as_deref(),
        Some("doomed")
    );
    sim.apply(Action::Pull { client: 1 }).unwrap();
    sim.drain();
    assert_eq!(sim.read_text(1, &entry_key("e1")), None);
    assert_eq!(sim.client(1).record_stamp(&entry_key("e1")).unwrap(), stamp);
    assert_eq!(sim.conflicts, 0);
    sim.check().unwrap();
}

/// Restart keeps retained rows: a record delivered by a stream the client has
/// since left survives a crash, stamp included.
#[test]
fn restart_keeps_rows_retained_after_unsubscribe() {
    let mut sim = setup(39);
    sim.apply(Action::Unsubscribe {
        client: 0,
        stream: "a".into(),
    })
    .unwrap();
    sim.apply(Action::Crash { client: 0 }).unwrap();
    sim.apply(Action::Restart { client: 0 }).unwrap();
    assert!(sim.client(0).subscriptions().unwrap().is_empty());
    assert_eq!(sim.read_text(0, &entry_key("e1")).as_deref(), Some("base"));
    assert_eq!(sim.client(0).record_stamp(&entry_key("e1")).unwrap(), 1);
    assert_eq!(sim.client(0).last_completed_push().unwrap(), 1);
    sim.check().unwrap();
}

/// A canonical null is Model authority, independently of the transport that
/// delivered it. The other path's older positive answer cannot resurrect it.
#[test]
fn newer_null_wins_between_fetch_and_stream_in_both_orders_through_restart() {
    use axton_core::{ActionOutcome, AuthorityRecord, CallCompletion, FetchResponse};
    use serde_json::json;
    for fetch_null in [false, true] {
        let mut sim = setup(40);
        sim.apply(Action::ServerChange {
            key: "Entry:e1".into(),
            text: None,
            streams: vec!["a".into()],
        })
        .unwrap();
        let fetched = |state, stamp| FetchResponse {
            completion: CallCompletion {
                call_id: "123e4567-e89b-42d3-a456-426614174000".into(),
                outcome: ActionOutcome::Succeeded {
                    result: json!(null),
                },
            },
            records: vec![AuthorityRecord {
                model: "Entry".into(),
                identity: json!({"id":"e1"}),
                stamp,
                state,
                error: None,
            }],
        };
        if fetch_null {
            sim.client(0)
                .apply_fetch_response(&fetched(json!(null), 2))
                .unwrap();
            // The ordinary Stream still has an old covered positive page in flight.
            let old = axton_core::StreamPullPage::decode(&serde_json::to_vec(&json!({
                "cursors":{"a":{"from":1,"to":2,"head":2}},
                "changes":[{"kind":"upsert","stream":"a","cursor":2,"model":"Entry","identity":{"id":"e1"},"stamp":1,"state":{"text":"base","note":null}}]
            })).unwrap()).unwrap();
            sim.client(0).apply_stream_page(old).unwrap();
        } else {
            sim.settle();
            sim.client(0)
                .apply_fetch_response(&fetched(json!({"text":"base","note":null}), 1))
                .unwrap();
        }
        assert_eq!(sim.read_text(0, &entry_key("e1")), None);
        assert_eq!(sim.client(0).record_stamp(&entry_key("e1")).unwrap(), 2);
        sim.apply(Action::Crash { client: 0 }).unwrap();
        sim.apply(Action::Restart { client: 0 }).unwrap();
        assert_eq!(sim.read_text(0, &entry_key("e1")), None);
        assert_eq!(sim.client(0).record_stamp(&entry_key("e1")).unwrap(), 2);
        assert!(
            sim.client(0)
                .read_sql(
                    "SELECT name FROM sqlite_master WHERE name='axton_stream_member'",
                    &[]
                )
                .unwrap()
                .is_empty()
        );
        sim.check().unwrap();
    }
}

/// Action, receipt and explicit Bootstrap absence share the same authority as
/// Stream/Fetch. A delayed positive Load can complete without resurrecting it.
#[test]
fn action_receipt_and_bootstrap_null_outrank_old_positive_load_and_fetch() {
    use axton_client::{LoadFence, LoadOptions, LoadStored};
    use axton_core::{LoadPageReply, LoadPageResponse, Schema};
    use serde_json::json;
    for source in ["action", "receipt", "bootstrap"] {
        let mut value = serde_json::to_value(axton_sim::schema::enrollment_schema()).unwrap();
        value["actions"] = json!([{"name":"Inspect","version":1,"inputs":[],"outputs":[]}]);
        let schema = Schema::from_value(value).unwrap();
        let mut sim = Sim::new_with_schema(41, 1, schema);
        sim.apply(Action::Subscribe {
            client: 0,
            stream: "a".into(),
        })
        .unwrap();
        sim.apply(Action::Declare {
            key: "Entry:e1".into(),
            touch: Some(Some("base".into())),
            memberships: vec![("a".into(), true), ("b".into(), true)],
        })
        .unwrap();
        sim.settle();
        sim.apply(Action::SubscribeAtHead {
            client: 0,
            stream: "b".into(),
        })
        .unwrap();
        let load = sim
            .client(0)
            .start_load(
                "EnrolledEntries",
                1,
                &json!({"channel":"a"}),
                LoadOptions::default(),
            )
            .unwrap()
            .job;
        let fence = LoadFence {
            replica: sim.client(0).replica_generation(),
            load_id: load.id,
            run: load.run,
            call_id: load.call_id.unwrap(),
        };
        let authority = |state, stamp| json!({"model":"Entry","identity":{"id":"e1"},"stamp":stamp,"state":state});
        let old_page = LoadPageResponse::decode_item(&json!({
            "loadId":fence.load_id,"callId":fence.call_id,
            "outcome":{"status":"succeeded","data":{"entries":[{"id":"e1"}]},"next":null},
            "records":[authority(json!({"text":"base","note":null}),1)]
        }))
        .unwrap();
        if source != "receipt" {
            sim.apply(Action::ServerChange {
                key: "Entry:e1".into(),
                text: None,
                streams: vec!["a".into(), "b".into()],
            })
            .unwrap();
        }
        match source {
            "action" => {
                let request = sim
                    .client(0)
                    .prepare_action("Inspect", 1, json!({}))
                    .unwrap();
                let reply = json!({"completion":{"callId":request.call.call_id,"outcome":{"status":"succeeded","result":null}},"records":[authority(json!(null),2)]});
                sim.client(0)
                    .apply_action_response(&request, &serde_json::to_vec(&reply).unwrap())
                    .unwrap();
            }
            "receipt" => {
                sim.apply(Action::Enqueue {
                    client: 0,
                    mutation: MutationSpec::DeleteEntry { id: "e1".into() },
                })
                .unwrap();
                sim.apply(Action::Freeze { client: 0 }).unwrap();
                sim.apply(Action::Deliver).unwrap();
                sim.apply(Action::Deliver).unwrap();
                assert_eq!(sim.client(0).pending_count().unwrap(), 0);
            }
            "bootstrap" => {
                let registration = sim
                    .client(0)
                    .subscription_state("b")
                    .unwrap()
                    .unwrap()
                    .subscription_id;
                let run = sim
                    .client(0)
                    .request_bootstrap("b", registration)
                    .unwrap()
                    .run;
                let page = axton_core::StreamBootstrapPage::decode(&serde_json::to_vec(&json!({
                    "mode":"bootstrap","stream":"b","from":0,"to":1,"until":1,"head":2,
                    "changes":[{"stream":"b","cursor":1,"kind":"upsert","model":"Entry","identity":{"id":"e1"},"stamp":2,"state":null}]
                })).unwrap()).unwrap();
                sim.client(0)
                    .apply_stream_bootstrap_page("b", registration, run, 0, &page)
                    .unwrap();
            }
            _ => unreachable!(),
        }
        assert_eq!(
            sim.read_text(0, &entry_key("e1")),
            None,
            "{source} supplies null authority"
        );
        let stored = sim
            .client(0)
            .store_load_page(
                &fence,
                LoadPageReply {
                    load_id: fence.load_id.clone(),
                    call_id: fence.call_id.clone(),
                    page: Ok(old_page),
                },
            )
            .unwrap();
        assert!(
            matches!(stored, LoadStored::Applied { .. }),
            "{source}: old Load still completes"
        );
        assert_eq!(
            sim.read_text(0, &entry_key("e1")),
            None,
            "{source}: old Load cannot resurrect"
        );
        assert_eq!(sim.client(0).record_stamp(&entry_key("e1")).unwrap(), 2);
        let old_fetch = axton_core::FetchResponse {
            completion: axton_core::CallCompletion {
                call_id: "123e4567-e89b-42d3-a456-426614174000".into(),
                outcome: axton_core::ActionOutcome::Succeeded {
                    result: json!({"id":"e1","text":"base","note":null}),
                },
            },
            records: vec![axton_core::AuthorityRecord {
                model: "Entry".into(),
                identity: json!({"id":"e1"}),
                stamp: 1,
                state: json!({"text":"base","note":null}),
                error: None,
            }],
        };
        sim.client(0).apply_fetch_response(&old_fetch).unwrap();
        assert_eq!(
            sim.read_text(0, &entry_key("e1")),
            None,
            "{source}: old Fetch cannot resurrect"
        );
        // N2 intentionally forbids null Model outputs in a successful Load.
        // Refusal keeps every authority stamp and page-progress field unchanged.
        let malformed = sim
            .client(0)
            .start_load(
                "EnrolledEntries",
                1,
                &json!({"channel":"a"}),
                LoadOptions::default(),
            )
            .unwrap()
            .job;
        let malformed_fence = LoadFence {
            replica: sim.client(0).replica_generation(),
            load_id: malformed.id,
            run: malformed.run,
            call_id: malformed.call_id.unwrap(),
        };
        let null_page = LoadPageResponse::decode_item(&json!({
            "loadId":malformed_fence.load_id,"callId":malformed_fence.call_id,
            "outcome":{"status":"succeeded","data":{"entries":[{"id":"e1"}]},"next":null},
            "records":[authority(json!(null),99)]
        }))
        .unwrap();
        let refused = sim
            .client(0)
            .store_load_page(
                &malformed_fence,
                LoadPageReply {
                    load_id: malformed_fence.load_id.clone(),
                    call_id: malformed_fence.call_id.clone(),
                    page: Ok(null_page),
                },
            )
            .unwrap();
        let LoadStored::Failed(refused) = refused else {
            panic!("null Load must fail");
        };
        assert_eq!(refused.pages, 0);
        assert_eq!(
            refused.error.unwrap().code,
            axton_client::loads::PROTOCOL_INVALID
        );
        assert_eq!(sim.client(0).record_stamp(&entry_key("e1")).unwrap(), 2);
        sim.apply(Action::Crash { client: 0 }).unwrap();
        sim.apply(Action::Restart { client: 0 }).unwrap();
        assert_eq!(sim.read_text(0, &entry_key("e1")), None);
        sim.settle();
        sim.check().unwrap();
    }
}
