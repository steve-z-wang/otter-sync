mod common;
use axton_client::*;
use common::*;
use serde_json::json;
use std::collections::BTreeMap;

fn up(scope: &str, cursor: u64, stamp: u64, text: Option<&str>) -> StreamChange {
    StreamChange::Upsert {
        stream: scope.into(),
        cursor,
        record: authority(text, stamp),
    }
}
fn remove(scope: &str, cursor: u64) -> StreamChange {
    StreamChange::Remove {
        stream: scope.into(),
        cursor,
        key: key(),
    }
}
fn deliver(c: &mut Client<axton_sqlite::SqliteStore>, scope: &str, to: u64, change: StreamChange) {
    let from = c.cursor(scope).unwrap().unwrap();
    c.apply_stream_page(StreamPullPage {
        cursors: BTreeMap::from([(scope.into(), CursorRange { from, to, head: to })]),
        changes: vec![change],
    })
    .unwrap();
}
fn holds(c: &mut Client<axton_sqlite::SqliteStore>) -> u64 {
    c.read_sql(
        "SELECT COUNT(*) AS n FROM sqlite_master WHERE name='axton_stream_member'",
        &[],
    )
    .unwrap()[0]["n"]
        .as_u64()
        .unwrap()
}
#[test]
fn two_streams_keep_one_record_after_both_removals() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    subscribe(&mut c, "b");
    deliver(&mut c, "a", 1, up("a", 1, 7, Some("A")));
    deliver(&mut c, "b", 4, up("b", 4, 7, Some("A")));
    assert_eq!(holds(&mut c), 0);
    deliver(&mut c, "a", 2, remove("a", 2));
    assert!(c.read(&key()).unwrap().is_some());
    assert_eq!(holds(&mut c), 0);
    deliver(&mut c, "b", 5, remove("b", 5));
    assert!(c.read(&key()).unwrap().is_some());
    assert_eq!(holds(&mut c), 0);
    assert_eq!(c.record_stamp(&key()).unwrap(), 7);
    deliver(&mut c, "a", 3, up("a", 3, 7, Some("A")));
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "A");
    assert_eq!(holds(&mut c), 0);
    let conflict = c
        .apply_stream_page(StreamPullPage {
            cursors: BTreeMap::from([(
                "a".into(),
                CursorRange {
                    from: 3,
                    to: 4,
                    head: 4,
                },
            )]),
            changes: vec![up("a", 4, 7, Some("conflicting"))],
        })
        .unwrap();
    assert_eq!(conflict.conflicts(), 1);
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "A");
}

#[test]
fn cached_untracked_authority_is_retained_and_stamped_null_stays_global() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    subscribe(&mut c, "b");
    c.apply_page(page("a", 0, 1, Some("cache"))).unwrap();
    assert_eq!(holds(&mut c), 0);
    deliver(&mut c, "a", 2, remove("a", 2));
    assert!(c.read(&key()).unwrap().is_some());
    deliver(&mut c, "a", 3, up("a", 3, 2, Some("again")));
    deliver(&mut c, "b", 1, up("b", 1, 3, None));
    assert!(c.read(&key()).unwrap().is_none());
    deliver(&mut c, "b", 2, remove("b", 2));
    deliver(&mut c, "a", 4, remove("a", 4));
    let from = c.cursor("a").unwrap().unwrap();
    let report = c
        .apply_stream_page(StreamPullPage {
            cursors: BTreeMap::from([(
                "a".into(),
                CursorRange {
                    from,
                    to: 5,
                    head: 5,
                },
            )]),
            changes: vec![up("a", 5, 3, Some("forbidden"))],
        })
        .unwrap();
    assert_eq!(report.conflicts(), 1);
    assert!(c.read(&key()).unwrap().is_none());
}
#[test]
fn removal_and_other_scope_upsert_fold_before_release() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    subscribe(&mut c, "b");
    deliver(&mut c, "a", 1, up("a", 1, 7, Some("base")));
    let report = c
        .apply_stream_page(StreamPullPage {
            cursors: BTreeMap::from([
                (
                    "a".into(),
                    CursorRange {
                        from: 1,
                        to: 2,
                        head: 2,
                    },
                ),
                (
                    "b".into(),
                    CursorRange {
                        from: 0,
                        to: 1,
                        head: 1,
                    },
                ),
            ]),
            changes: vec![remove("a", 2), up("b", 1, 7, Some("base"))],
        })
        .unwrap();
    assert_eq!(report.conflicts(), 0);
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "base");
    assert_eq!(holds(&mut c), 0);
}
#[test]
fn removal_preserves_local_create_and_patch_until_newer_authority() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open(&path);
    subscribe(&mut c, "a");
    seed(&mut c, "local");
    deliver(&mut c, "a", 1, remove("a", 1));
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "local");
    drop(c);
    let mut c = open(&path);
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "local");
    deliver(&mut c, "a", 2, up("a", 2, 1, Some("server")));
    c.transaction(|tx| tx.direct(update("patch"))).unwrap();
    deliver(&mut c, "a", 3, remove("a", 3));
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "patch");
    assert_eq!(table_count(&mut c, "axton_local_replica_layer"), 1);
    deliver(&mut c, "a", 4, up("a", 4, 2, Some("server")));
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "server");
    assert_eq!(table_count(&mut c, "axton_local_replica_layer"), 0);
}
#[test]
fn pending_update_and_accepted_or_rejected_receipt_survive_removal() {
    for rejected in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut c = open(&dir.path().join("db"));
        subscribe(&mut c, "a");
        deliver(&mut c, "a", 1, up("a", 1, 7, Some("base")));
        let ordinal = c.transaction(|tx| tx.enqueue(mutation("pending"))).unwrap();
        c.freeze().unwrap();
        deliver(&mut c, "a", 2, remove("a", 2));
        assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "pending");
        assert_eq!(table_count(&mut c, "axton_mutation"), 1);
        let r = if rejected {
            rejecting(&mut c, 1, &[ordinal], "no", vec![authority(Some("old"), 8)])
        } else {
            receipt(&mut c, 1, vec![authority(Some("accepted"), 8)])
        };
        c.acknowledge(1, r).unwrap();
        assert_eq!(
            c.read(&key()).unwrap().unwrap()["text"],
            if rejected { "old" } else { "accepted" }
        );
        assert_eq!(table_count(&mut c, "axton_mutation"), 0);
        assert_eq!(c.record_stamp(&key()).unwrap(), 8);
        assert_eq!(table_count(&mut c, "axton_rejection"), u64::from(rejected));
    }
}
#[test]
fn prepared_removal_preserves_state_and_cursor_and_runs_no_store_hook() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    deliver(&mut c, "a", 1, up("a", 1, 7, Some("base")));
    c.begin_session().unwrap();
    let prepared = c
        .prepare_store(StoreDelivery::StreamPage(StreamPullPage {
            cursors: BTreeMap::from([(
                "a".into(),
                CursorRange {
                    from: 1,
                    to: 2,
                    head: 2,
                },
            )]),
            changes: vec![remove("a", 2)],
        }))
        .unwrap();
    assert!(prepared.changes().is_empty());

    assert!(c.session(|tx| tx.read(&key())).unwrap().is_some());
    assert_eq!(c.cursor("a").unwrap(), Some(1));
    c.apply_prepared_store(prepared).unwrap();
    c.commit_session().unwrap();
    assert!(c.read(&key()).unwrap().is_some());
    assert_eq!(c.cursor("a").unwrap(), Some(2));
}

fn store_enrolled(
    c: &mut Client<axton_sqlite::SqliteStore>,
    claims: Vec<MembershipClaim>,
    text: &str,
    stamp: u64,
) -> Result<StoreResult> {
    c.begin_session()?;
    let prepared = match c.prepare_store(StoreDelivery::Direct {
        response: DirectActionResponse {
            completion: CallCompletion {
                call_id: "read".into(),
                outcome: ActionOutcome::Succeeded {
                    result: json!(null),
                },
            },
            records: vec![authority(Some(text), stamp)],
            memberships: claims,
        },
        snapshot: None,
    }) {
        Ok(prepared) => prepared,
        Err(error) => {
            c.rollback_session()?;
            return Err(error);
        }
    };
    let result = c.apply_prepared_store(prepared)?;
    c.commit_session()?;
    Ok(result)
}
fn claim(scope: &str, cursor: u64) -> MembershipClaim {
    MembershipClaim {
        stream: scope.into(),
        cursor,
        model: "Entry".into(),
        identity: json!({"id":"e"}),
    }
}
#[test]
fn historical_claims_do_not_gate_newer_direct_or_bootstrap_authority() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    c.transaction(|tx| tx.set_stream("a".into(), true)).unwrap();
    acknowledge(&mut c, &[("a", 10)]);
    let id = c.subscription_state("a").unwrap().unwrap().subscription_id;
    let run = c.request_bootstrap("a", id).unwrap().run;
    store_enrolled(&mut c, vec![claim("a", 8)], "base", 7).unwrap();
    deliver(&mut c, "a", 11, remove("a", 11));
    store_enrolled(&mut c, vec![claim("a", 8)], "delayed", 99).unwrap();
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "delayed");
    assert_eq!(c.record_stamp(&key()).unwrap(), 99);
    let result = c
        .apply_stream_bootstrap_page(
            "a",
            id,
            run,
            0,
            &StreamBootstrapPage {
                stream: "a".into(),
                from: 0,
                to: 10,
                until: 10,
                head: 11,
                changes: vec![up("a", 8, 99, Some("delayed"))],
            },
        )
        .unwrap();
    assert!(matches!(result, BootstrapApply::Applied { .. }));
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "delayed");
    assert_eq!(holds(&mut c), 0);
}
#[test]
fn historical_claim_cursor_does_not_conflict_with_stream_removal() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    subscribe(&mut c, "b");
    store_enrolled(&mut c, vec![claim("a", 2)], "base", 7).unwrap();
    let result = c.apply_stream_page(StreamPullPage {
        cursors: BTreeMap::from([
            (
                "a".into(),
                CursorRange {
                    from: 0,
                    to: 2,
                    head: 2,
                },
            ),
            (
                "b".into(),
                CursorRange {
                    from: 0,
                    to: 1,
                    head: 1,
                },
            ),
        ]),
        changes: vec![up("b", 1, 8, Some("new")), remove("a", 2)],
    });
    assert!(result.is_ok());
    assert_eq!(holds(&mut c), 0);
    assert_eq!(c.cursor("a").unwrap(), Some(2));
    assert_eq!(c.cursor("b").unwrap(), Some(1));
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "new");
}
#[test]
fn parent_removal_preserves_parent_and_child() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = Client::open(
        axton_sqlite::SqliteStore::open(dir.path().join("db")).unwrap(),
        family_schema(),
    )
    .unwrap();
    subscribe(&mut c, "a");
    subscribe(&mut c, "b");
    let parent = RecordKey {
        model: "Book".into(),
        identity: json!({"id":"p"}),
    };
    let child = RecordKey {
        model: "Comment".into(),
        identity: json!({"id":"c"}),
    };
    c.apply_stream_page(StreamPullPage {
        cursors: BTreeMap::from([
            (
                "a".into(),
                CursorRange {
                    from: 0,
                    to: 1,
                    head: 1,
                },
            ),
            (
                "b".into(),
                CursorRange {
                    from: 0,
                    to: 1,
                    head: 1,
                },
            ),
        ]),
        changes: vec![
            StreamChange::Upsert {
                stream: "a".into(),
                cursor: 1,
                record: AuthorityRecord {
                    model: parent.model.clone(),
                    identity: parent.identity.clone(),
                    stamp: 1,
                    state: json!({"title":"book"}),
                    error: None,
                },
            },
            StreamChange::Upsert {
                stream: "b".into(),
                cursor: 1,
                record: AuthorityRecord {
                    model: child.model.clone(),
                    identity: child.identity.clone(),
                    stamp: 1,
                    state: json!({"bookId":"p","text":"child"}),
                    error: None,
                },
            },
        ],
    })
    .unwrap();
    c.apply_stream_page(StreamPullPage {
        cursors: BTreeMap::from([(
            "a".into(),
            CursorRange {
                from: 1,
                to: 2,
                head: 2,
            },
        )]),
        changes: vec![StreamChange::Remove {
            stream: "a".into(),
            cursor: 2,
            key: parent.clone(),
        }],
    })
    .unwrap();
    assert!(c.read(&parent).unwrap().is_some());
    assert!(c.read(&child).unwrap().is_some());
}
#[test]
fn legacy_authoritative_absence_is_not_reclassified_as_eviction() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    c.apply_page(page("a", 0, 1, None)).unwrap();
    drop(c);
    {
        let db = rusqlite::Connection::open(dir.path().join("db")).unwrap();
        db.execute("ALTER TABLE axton_record DROP COLUMN base_state", [])
            .unwrap();
        db.execute("ALTER TABLE axton_record DROP COLUMN evicted_at", [])
            .unwrap();
    }
    let mut c = open(&dir.path().join("db"));
    deliver(&mut c, "a", 2, remove("a", 2));
    let result = c
        .apply_stream_page(StreamPullPage {
            cursors: BTreeMap::from([(
                "a".into(),
                CursorRange {
                    from: 2,
                    to: 3,
                    head: 3,
                },
            )]),
            changes: vec![up("a", 3, 1, Some("forbidden"))],
        })
        .unwrap();
    assert_eq!(result.conflicts(), 1);
    assert!(c.read(&key()).unwrap().is_none());
}

#[test]
fn cascading_authoritative_null_cannot_be_restored_at_childs_old_stamp_after_release() {
    for child_evicted_first in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut c = Client::open(
            axton_sqlite::SqliteStore::open(dir.path().join("db")).unwrap(),
            family_schema(),
        )
        .unwrap();
        subscribe(&mut c, "a");
        subscribe(&mut c, "b");
        let parent = RecordKey {
            model: "Book".into(),
            identity: json!({"id":"p"}),
        };
        let child = RecordKey {
            model: "Comment".into(),
            identity: json!({"id":"c"}),
        };
        let child_record = AuthorityRecord {
            model: child.model.clone(),
            identity: child.identity.clone(),
            stamp: 7,
            state: json!({"bookId":"p","text":"child"}),
            error: None,
        };
        c.apply_stream_page(StreamPullPage {
            cursors: BTreeMap::from([
                (
                    "a".into(),
                    CursorRange {
                        from: 0,
                        to: 1,
                        head: 1,
                    },
                ),
                (
                    "b".into(),
                    CursorRange {
                        from: 0,
                        to: 1,
                        head: 1,
                    },
                ),
            ]),
            changes: vec![
                StreamChange::Upsert {
                    stream: "a".into(),
                    cursor: 1,
                    record: AuthorityRecord {
                        model: parent.model.clone(),
                        identity: parent.identity.clone(),
                        stamp: 1,
                        state: json!({"title":"book"}),
                        error: None,
                    },
                },
                StreamChange::Upsert {
                    stream: "b".into(),
                    cursor: 1,
                    record: child_record.clone(),
                },
            ],
        })
        .unwrap();
        if child_evicted_first {
            c.apply_stream_page(StreamPullPage {
                cursors: BTreeMap::from([(
                    "b".into(),
                    CursorRange {
                        from: 1,
                        to: 2,
                        head: 2,
                    },
                )]),
                changes: vec![StreamChange::Remove {
                    stream: "b".into(),
                    cursor: 2,
                    key: child.clone(),
                }],
            })
            .unwrap();
            c.transaction(|tx| {
                tx.direct(Operation {
                    model: "Comment".into(),
                    identity: json!({"id":"c"}),
                    op: OperationKind::Update,
                    values: Some(json!({"text":"local child"})),
                })
            })
            .unwrap();
        }
        c.apply_stream_page(StreamPullPage {
            cursors: BTreeMap::from([(
                "a".into(),
                CursorRange {
                    from: 1,
                    to: 2,
                    head: 2,
                },
            )]),
            changes: vec![StreamChange::Upsert {
                stream: "a".into(),
                cursor: 2,
                record: AuthorityRecord {
                    model: parent.model.clone(),
                    identity: parent.identity.clone(),
                    stamp: 2,
                    state: json!(null),
                    error: None,
                },
            }],
        })
        .unwrap();
        let from = c.cursor("b").unwrap().unwrap();
        c.apply_stream_page(StreamPullPage {
            cursors: BTreeMap::from([(
                "b".into(),
                CursorRange {
                    from,
                    to: from + 1,
                    head: from + 1,
                },
            )]),
            changes: vec![StreamChange::Remove {
                stream: "b".into(),
                cursor: from + 1,
                key: child.clone(),
            }],
        })
        .unwrap();
        let report = c
            .apply_stream_page(StreamPullPage {
                cursors: BTreeMap::from([(
                    "b".into(),
                    CursorRange {
                        from: from + 1,
                        to: from + 2,
                        head: from + 2,
                    },
                )]),
                changes: vec![StreamChange::Upsert {
                    stream: "b".into(),
                    cursor: from + 2,
                    record: child_record,
                }],
            })
            .unwrap();
        assert!(
            c.read(&child).unwrap().is_none(),
            "cascade absence survives child_evicted_first={child_evicted_first}"
        );
        assert_eq!(report.conflicts(), 1);
    }
}

#[test]
fn prepared_bootstrap_rollback_retains_no_authority_or_run_progress() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    c.transaction(|tx| tx.set_stream("a".into(), true)).unwrap();
    acknowledge(&mut c, &[("a", 1)]);
    let id = c.subscription_state("a").unwrap().unwrap().subscription_id;
    let run = c.request_bootstrap("a", id).unwrap().run;
    c.begin_session().unwrap();
    let prepared = c
        .prepare_store(StoreDelivery::StreamBootstrap {
            stream: "a".into(),
            subscription_id: id,
            run,
            expected_after: 0,
            page: StreamBootstrapPage {
                stream: "a".into(),
                from: 0,
                to: 1,
                until: 1,
                head: 1,
                changes: vec![up("a", 1, 7, Some("base"))],
            },
        })
        .unwrap();
    assert_eq!(prepared.accepted(), &[0]);

    c.rollback_session().unwrap();
    assert_eq!(holds(&mut c), 0);
    assert_eq!(c.bootstrap_state("a", id).unwrap().cursor, 0);
    c.begin_session().unwrap();
    assert!(c.apply_prepared_store(prepared).is_err());
    c.rollback_session().unwrap();
    assert_eq!(holds(&mut c), 0);
    assert!(c.read(&key()).unwrap().is_none());
}
#[test]
fn invalid_controls_roll_back_and_bad_bodies_keep_no_authority() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    let malformed = StreamPullPage {
        cursors: BTreeMap::from([(
            "a".into(),
            CursorRange {
                from: 0,
                to: 1,
                head: 1,
            },
        )]),
        changes: vec![remove("a", 2)],
    };
    assert!(c.apply_stream_page(malformed).is_err());
    assert_eq!(c.cursor("a").unwrap(), Some(0));
    assert_eq!(holds(&mut c), 0);
    let mut record = authority(Some("invalid"), 7);
    record.state = json!({"text":99});
    let report = c
        .apply_stream_page(StreamPullPage {
            cursors: BTreeMap::from([(
                "a".into(),
                CursorRange {
                    from: 0,
                    to: 1,
                    head: 1,
                },
            )]),
            changes: vec![StreamChange::Upsert {
                stream: "a".into(),
                cursor: 1,
                record,
            }],
        })
        .unwrap();
    assert_eq!(report.skipped(), 1);
    assert_eq!(holds(&mut c), 0);
    assert_eq!(c.record_stamp(&key()).unwrap(), 0);
    assert_eq!(c.cursor("a").unwrap(), Some(1));
    assert!(StreamPullPage::decode(&serde_json::to_vec(&json!({"cursors":{"a":{"from":1,"to":2,"head":2}},"changes":[{"kind":"remove","stream":"a","cursor":2,"model":"Entry","identity":{"id":"e"},"stamp":9,"state":null}]})).unwrap()).is_err());
}
#[test]
fn removal_keeps_direct_create_until_newer_receipt_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open(&path);
    subscribe(&mut c, "a");
    deliver(&mut c, "a", 1, up("a", 1, 7, Some("base")));
    c.transaction(|tx| {
        tx.enqueue(mutation("pending"))?;
        tx.direct(Operation {
            model: "Entry".into(),
            identity: json!({"id":"e"}),
            op: OperationKind::Delete,
            values: None,
        })?;
        tx.direct(create(
            "Entry",
            "e",
            json!({"text":"local recreated","note":null}),
        ))
    })
    .unwrap();
    c.freeze().unwrap();
    deliver(&mut c, "a", 2, remove("a", 2));
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "local recreated");
    let r = receipt(&mut c, 1, vec![authority(Some("newer receipt"), 8)]);
    c.acknowledge(1, r).unwrap();
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "newer receipt");
    drop(c);
    let mut c = open(&path);
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "newer receipt");
    deliver(&mut c, "a", 3, remove("a", 3));
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "newer receipt");
    deliver(&mut c, "a", 4, up("a", 4, 9, Some("base")));
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "base");
}
#[test]
fn accepted_companion_create_is_preserved_as_local_work_after_release() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    seed(&mut c, "wire base");
    c.transaction(|tx| {
        let mut m = mutation("wire updated");
        m.companion.push(create(
            "Entry",
            "local",
            json!({"text":"companion","note":null}),
        ));
        tx.enqueue(m)
    })
    .unwrap();
    c.freeze().unwrap();
    let r = receipt(&mut c, 1, vec![authority(Some("wire updated"), 7)]);
    c.acknowledge(1, r).unwrap();
    let local_key = RecordKey {
        model: "Entry".into(),
        identity: json!({"id":"local"}),
    };
    c.apply_stream_page(StreamPullPage {
        cursors: BTreeMap::from([(
            "a".into(),
            CursorRange {
                from: 0,
                to: 1,
                head: 1,
            },
        )]),
        changes: vec![StreamChange::Remove {
            stream: "a".into(),
            cursor: 1,
            key: local_key.clone(),
        }],
    })
    .unwrap();
    assert_eq!(c.read(&local_key).unwrap().unwrap()["text"], "companion");
    assert_eq!(c.pending_count().unwrap(), 0);
}

#[test]
fn legacy_unstamped_direct_create_survives_upgrade_and_release() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open(&path);
    subscribe(&mut c, "a");
    seed(&mut c, "legacy local");
    drop(c);
    {
        let db = rusqlite::Connection::open(&path).unwrap();
        db.execute_batch("DROP TABLE axton_local_replica_layer; ALTER TABLE axton_client DROP COLUMN local_authority_version; ALTER TABLE axton_client DROP COLUMN stream_membership_version")
            .unwrap();
    }
    let mut c = open(&path);
    deliver(&mut c, "a", 1, remove("a", 1));
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "legacy local");
    assert_eq!(c.pending_count().unwrap(), 0);
    drop(c);
    let mut c = open(&path);
    deliver(&mut c, "a", 2, remove("a", 2));
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "legacy local");
}

#[test]
fn equal_stamp_receipt_cannot_repopulate_absence_after_removal() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    deliver(&mut c, "a", 1, up("a", 1, 7, Some("base")));
    c.transaction(|tx| tx.enqueue(mutation("pending"))).unwrap();
    c.freeze().unwrap();
    deliver(&mut c, "a", 2, up("a", 2, 8, None));
    deliver(&mut c, "a", 3, remove("a", 3));
    let r = receipt(&mut c, 1, vec![authority(Some("late body"), 8)]);
    c.acknowledge(1, r).unwrap();
    assert!(c.read(&key()).unwrap().is_none());
    assert_eq!(c.record_stamp(&key()).unwrap(), 8);
    assert_eq!(c.pending_count().unwrap(), 0);
}

#[test]
fn uppercase_uuid_historical_claims_do_not_gate_normalized_authority() {
    for stamp in [7, 99] {
        let dir = tempfile::tempdir().unwrap();
        let schema=Schema::from_value(json!({"enums":[],"models":[{"name":"Entry","identity":["id"],"fields":[{"name":"id","nullable":false,"type":{"kind":"scalar","name":"uuid"}},{"name":"text","nullable":false,"type":{"kind":"scalar","name":"string"}}]}]})).unwrap();
        let mut c = Client::open(
            axton_sqlite::SqliteStore::open(dir.path().join("db")).unwrap(),
            schema,
        )
        .unwrap();
        subscribe(&mut c, "a");
        let lower = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee";
        let upper = lower.to_uppercase();
        let key = RecordKey {
            model: "Entry".into(),
            identity: json!({"id":lower}),
        };
        c.apply_stream_page(StreamPullPage {
            cursors: BTreeMap::from([(
                "a".into(),
                CursorRange {
                    from: 0,
                    to: 1,
                    head: 1,
                },
            )]),
            changes: vec![StreamChange::Upsert {
                stream: "a".into(),
                cursor: 1,
                record: AuthorityRecord {
                    model: "Entry".into(),
                    identity: key.identity.clone(),
                    stamp: 7,
                    state: json!({"text":"base"}),
                    error: None,
                },
            }],
        })
        .unwrap();
        c.apply_stream_page(StreamPullPage {
            cursors: BTreeMap::from([(
                "a".into(),
                CursorRange {
                    from: 1,
                    to: 2,
                    head: 2,
                },
            )]),
            changes: vec![StreamChange::Remove {
                stream: "a".into(),
                cursor: 2,
                key: key.clone(),
            }],
        })
        .unwrap();
        c.begin_session().unwrap();
        let prepared = c
            .prepare_store(StoreDelivery::Direct {
                response: DirectActionResponse {
                    completion: CallCompletion {
                        call_id: "old-enrolled-read".into(),
                        outcome: ActionOutcome::Succeeded {
                            result: json!({"done":true}),
                        },
                    },
                    records: vec![AuthorityRecord {
                        model: "Entry".into(),
                        identity: json!({"id":upper}),
                        stamp,
                        state: json!({"text":"delayed"}),
                        error: None,
                    }],
                    memberships: vec![MembershipClaim {
                        stream: "a".into(),
                        cursor: 1,
                        model: "Entry".into(),
                        identity: json!({"id":upper}),
                    }],
                },
                snapshot: None,
            })
            .unwrap();
        let result = c.apply_prepared_store(prepared).unwrap();
        c.commit_session().unwrap();
        let StoreResult::Direct(report) = result else {
            panic!("expected direct completion")
        };
        assert_eq!(report.completions[0].call_id, "old-enrolled-read");
        assert_eq!(
            c.read(&key).unwrap().unwrap()["text"],
            if stamp > 7 { "delayed" } else { "base" }
        );
        assert_eq!(c.record_stamp(&key).unwrap(), stamp);
        assert_eq!(holds(&mut c), 0);
    }
}

fn fetched(call_id: &str, text: Option<&str>, stamp: u64) -> FetchResponse {
    FetchResponse {
        completion: CallCompletion {
            call_id: call_id.into(),
            outcome: ActionOutcome::Succeeded {
                result: json!(text),
            },
        },
        records: vec![authority(text, stamp)],
    }
}

#[test]
fn frozen_fetch_body_is_suppressed_but_fresh_fetch_and_null_are_admitted() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    deliver(&mut c, "a", 1, up("a", 1, 7, Some("base")));
    let old = c.prepare_fetch("Entry", 1, &key().identity, true).unwrap();
    legacy_eviction(&mut c, &dir.path().join("db"), &key());
    let report = c
        .apply_fetch_response(&fetched(&old.call_id, Some("old"), 99))
        .unwrap();
    assert_eq!(report.completions.len(), 1);
    assert_eq!(report.applied, 0, "old positive Fetch must be fenced");
    assert!(c.read(&key()).unwrap().is_none());
    assert_eq!(c.record_stamp(&key()).unwrap(), 7);
    let fresh = c.prepare_fetch("Entry", 1, &key().identity, true).unwrap();
    assert_eq!(
        c.apply_fetch_response(&fetched(&fresh.call_id, Some("fresh"), 7))
            .unwrap()
            .applied,
        1
    );
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "fresh");
    assert_eq!(
        c.apply_fetch_response(&fetched(&old.call_id, None, 100))
            .unwrap()
            .applied,
        1
    );
    assert!(c.read(&key()).unwrap().is_none());
    assert_eq!(c.record_stamp(&key()).unwrap(), 100);
}

#[test]
fn fresh_queued_receipt_after_release_is_admitted_across_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open(&path);
    subscribe(&mut c, "a");
    deliver(&mut c, "a", 1, up("a", 1, 7, Some("base")));
    legacy_eviction(&mut c, &dir.path().join("db"), &key());
    c.transaction(|tx| {
        tx.enqueue(Mutation::new(
            "Create",
            vec![Operation {
                model: "Entry".into(),
                identity: key().identity,
                op: OperationKind::Create,
                values: Some(json!({"text":"fresh","note":null})),
            }],
        ))
    })
    .unwrap();
    c.freeze().unwrap();
    drop(c);
    let mut c = open(&path);
    let r = receipt(&mut c, 1, vec![authority(Some("fresh"), 8)]);
    let report = c.acknowledge(1, r).unwrap();
    assert_eq!(
        report.applied, 1,
        "fresh receipt token must survive restart"
    );
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "fresh");
    assert_eq!(c.pending_count().unwrap(), 0);
}

#[test]
fn mixed_epoch_queue_freezes_separate_receipts_and_keeps_tokens_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open(&path);
    subscribe(&mut c, "a");
    deliver(&mut c, "a", 1, up("a", 1, 7, Some("base")));
    c.transaction(|tx| {
        tx.enqueue(Mutation::new(
            "Create",
            vec![Operation {
                model: "Entry".into(),
                identity: json!({"id":"other"}),
                op: OperationKind::Create,
                values: Some(json!({"text":"other","note":null})),
            }],
        ))
    })
    .unwrap();
    legacy_eviction(&mut c, &dir.path().join("db"), &key());
    c.transaction(|tx| {
        tx.enqueue(Mutation::new(
            "Create",
            vec![Operation {
                model: "Entry".into(),
                identity: key().identity,
                op: OperationKind::Create,
                values: Some(json!({"text":"fresh","note":null})),
            }],
        ))
    })
    .unwrap();
    drop(c);
    let mut c = open(&path);
    let first: serde_json::Value = serde_json::from_slice(&c.freeze().unwrap().unwrap()).unwrap();
    assert_eq!(
        first["mutations"].as_array().unwrap().len(),
        1,
        "receipt must have one unambiguous epoch"
    );
    let r = receipt(&mut c, 1, vec![authority_of("other", Some("other"), 1)]);
    c.acknowledge(1, r).unwrap();
    c.freeze().unwrap();
    let r = receipt(&mut c, 2, vec![authority(Some("fresh"), 8)]);
    assert_eq!(c.acknowledge(2, r).unwrap().applied, 1);
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "fresh");
}

#[test]
fn fresh_receipt_ignores_historical_enrollment_for_authority() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    deliver(&mut c, "a", 1, up("a", 1, 7, Some("base")));
    deliver(&mut c, "a", 2, remove("a", 2));
    c.transaction(|tx| {
        tx.enqueue(Mutation::new(
            "Create",
            vec![Operation {
                model: "Entry".into(),
                identity: json!({"id":"other"}),
                op: OperationKind::Create,
                values: Some(json!({"text":"other","note":null})),
            }],
        ))
    })
    .unwrap();
    c.freeze().unwrap();
    let mut r = receipt(
        &mut c,
        1,
        vec![
            authority(Some("old saved enrollment"), 99),
            authority_of("other", Some("other"), 1),
        ],
    );
    r.memberships = vec![MembershipClaim {
        stream: "a".into(),
        cursor: 1,
        model: "Entry".into(),
        identity: key().identity,
    }];
    let report = c.acknowledge(1, r).unwrap();
    assert_eq!(
        report.applied, 2,
        "historical claims do not suppress canonical records"
    );
    assert_eq!(
        c.read(&key()).unwrap().unwrap()["text"],
        "old saved enrollment"
    );
    assert_eq!(c.pending_count().unwrap(), 0);
}

#[test]
fn prepared_fetch_owns_frozen_token_even_when_request_owner_retires() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    deliver(&mut c, "a", 1, up("a", 1, 7, Some("base")));
    legacy_eviction(&mut c, &dir.path().join("db"), &key());
    let fresh = c.prepare_fetch("Entry", 1, &key().identity, true).unwrap();
    c.begin_session().unwrap();
    let prepared = c
        .prepare_store(StoreDelivery::Fetch {
            response: fetched(&fresh.call_id, Some("fresh"), 7),
        })
        .unwrap();
    assert_eq!(prepared.accepted().len(), 1);
    c.retire_request(&fresh.call_id);
    c.apply_prepared_store(prepared).unwrap();
    c.commit_session().unwrap();
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "fresh");
    // Retired identity is historical work; it is never assigned current epoch.
    let report = c
        .apply_fetch_response(&fetched(&fresh.call_id, Some("late duplicate"), 99))
        .unwrap();
    assert_eq!(report.applied, 0);
    assert_eq!(c.record_stamp(&key()).unwrap(), 7);
}

#[test]
fn stream_removal_never_allocates_eviction_epochs_or_record_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    subscribe(&mut c, "b");
    deliver(&mut c, "a", 1, up("a", 1, 7, Some("base")));
    deliver(&mut c, "b", 1, up("b", 1, 7, Some("base")));
    let epoch = |c: &mut Client<axton_sqlite::SqliteStore>| {
        c.read_sql("SELECT store_epoch FROM axton_client", &[])
            .unwrap()[0]["store_epoch"]
            .as_u64()
            .unwrap()
    };
    deliver(&mut c, "a", 2, remove("a", 2));
    assert_eq!(epoch(&mut c), 0);
    c.begin_session().unwrap();
    let prepared = c
        .prepare_store(StoreDelivery::StreamPage(StreamPullPage {
            cursors: [(
                "b".into(),
                CursorRange {
                    from: 1,
                    to: 2,
                    head: 2,
                },
            )]
            .into(),
            changes: vec![remove("b", 2)],
        }))
        .unwrap();
    assert_eq!(
        c.session_sql("SELECT store_epoch FROM axton_client", &[])
            .unwrap()[0]["store_epoch"],
        0
    );
    c.apply_prepared_store(prepared).unwrap();
    c.commit_session().unwrap();
    assert_eq!(epoch(&mut c), 0);
    assert_eq!(
        c.read_sql("SELECT evicted_at FROM axton_record", &[])
            .unwrap()[0]["evicted_at"],
        0
    );
    // Identical and stale scope evidence cannot mint another eviction.
    c.apply_stream_page(StreamPullPage {
        cursors: [(
            "b".into(),
            CursorRange {
                from: 1,
                to: 2,
                head: 2,
            },
        )]
        .into(),
        changes: vec![remove("b", 2)],
    })
    .unwrap();
    assert_eq!(epoch(&mut c), 0);
    c.apply_stream_page(StreamPullPage {
        cursors: [(
            "b".into(),
            CursorRange {
                from: 0,
                to: 1,
                head: 2,
            },
        )]
        .into(),
        changes: vec![remove("b", 1)],
    })
    .unwrap();
    assert_eq!(epoch(&mut c), 0);
    // New untracked cache identity is still fenced by its removal.
    let other = RecordKey {
        model: "Entry".into(),
        identity: json!({"id":"other"}),
    };
    c.apply_stream_page(StreamPullPage {
        cursors: [(
            "a".into(),
            CursorRange {
                from: 2,
                to: 3,
                head: 3,
            },
        )]
        .into(),
        changes: vec![StreamChange::Remove {
            stream: "a".into(),
            cursor: 3,
            key: other.clone(),
        }],
    })
    .unwrap();
    assert_eq!(epoch(&mut c), 0);
    assert!(
        c.read_sql(
            "SELECT evicted_at FROM axton_record WHERE identity=?",
            &[json!(other.encoded_identity().unwrap())]
        )
        .unwrap()
        .is_empty()
    );
}

#[test]
fn old_queued_write_keeps_epoch_and_frozen_bytes_across_release_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open(&path);
    subscribe(&mut c, "a");
    deliver(&mut c, "a", 1, up("a", 1, 7, Some("base")));
    c.transaction(|tx| tx.enqueue(mutation("pending"))).unwrap();
    let frozen = c.freeze().unwrap().unwrap();
    legacy_eviction(&mut c, &dir.path().join("db"), &key());
    drop(c);
    let mut c = open(&path);
    assert_eq!(c.freeze().unwrap().unwrap(), frozen);
    assert_eq!(
        c.read_sql("SELECT store_epoch FROM axton_mutation", &[])
            .unwrap()[0]["store_epoch"],
        0
    );
    let r = receipt(&mut c, 1, vec![authority(Some("late"), 99)]);
    let report = c.acknowledge(1, r).unwrap();
    assert_eq!(report.applied, 0);
    assert_eq!(c.pending_count().unwrap(), 0);
    assert!(c.read(&key()).unwrap().is_none());
    assert_eq!(c.record_stamp(&key()).unwrap(), 7);
}

#[test]
fn fresh_framework_catalog_has_authority_marker_without_ownership() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open(&path);
    let objects = c
        .read_sql(
            "SELECT name, sql FROM sqlite_master WHERE name LIKE 'axton_%' OR name LIKE 'sqlite_autoindex_axton_%' ORDER BY name",
            &[],
        )
        .unwrap();
    let catalog = serde_json::to_string(&objects).unwrap();
    assert!(!catalog.contains("channel"), "{catalog}");
    for table in ["axton_subscription", "axton_client"] {
        let columns = c
            .read_sql(
                &format!("SELECT name FROM pragma_table_info('{table}')"),
                &[],
            )
            .unwrap();
        let names: Vec<_> = columns
            .iter()
            .map(|column| column["name"].as_str().unwrap())
            .collect();
        assert!(
            names.contains(&if table == "axton_client" {
                "local_authority_version"
            } else {
                "stream"
            }),
            "{names:?}"
        );
        assert!(!names.iter().any(|name| name.contains("channel")));
    }
}

#[test]
fn stream_removal_and_unsubscribe_preserve_canonical_state() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    subscribe(&mut c, "b");
    deliver(&mut c, "a", 1, up("a", 1, 7, Some("base")));
    deliver(&mut c, "b", 1, up("b", 1, 7, Some("base")));
    deliver(&mut c, "a", 2, remove("a", 2));
    deliver(&mut c, "b", 2, remove("b", 2));
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "base");
    assert_eq!(c.record_stamp(&key()).unwrap(), 7);
    let a = c.ensure_subscription("a").unwrap().subscription_id;
    c.remove_subscription("a", a).unwrap();
    let b = c.ensure_subscription("b").unwrap().subscription_id;
    c.remove_subscription("b", b).unwrap();
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "base");
}
