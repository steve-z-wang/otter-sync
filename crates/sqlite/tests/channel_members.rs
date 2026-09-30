mod common;
use axton_client::*;
use common::*;
use serde_json::json;
use std::collections::BTreeMap;

fn up(channel: &str, cursor: u64, stamp: u64, text: Option<&str>) -> ChannelChange {
    ChannelChange::Upsert {
        channel: channel.into(),
        cursor,
        record: authority(text, stamp),
    }
}
fn remove(channel: &str, cursor: u64) -> ChannelChange {
    ChannelChange::Remove {
        channel: channel.into(),
        cursor,
        key: key(),
    }
}
fn deliver(
    c: &mut Client<axton_sqlite::SqliteStore>,
    channel: &str,
    to: u64,
    change: ChannelChange,
) {
    let from = c.cursor(channel).unwrap().unwrap();
    c.apply_channel_page(ChannelPullPage {
        cursors: BTreeMap::from([(channel.into(), CursorRange { from, to, head: to })]),
        changes: vec![change],
    })
    .unwrap();
}
fn holds(c: &mut Client<axton_sqlite::SqliteStore>) -> u64 {
    c.read_sql(
        "SELECT COUNT(*) AS n FROM axton_channel_member WHERE present=1",
        &[],
    )
    .unwrap()[0]["n"]
        .as_u64()
        .unwrap()
}
#[test]
fn two_channels_release_only_last_replica_and_restore_same_stamp() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    subscribe(&mut c, "b");
    deliver(&mut c, "a", 1, up("a", 1, 7, Some("A")));
    deliver(&mut c, "b", 4, up("b", 4, 7, Some("A")));
    assert_eq!(holds(&mut c), 2);
    deliver(&mut c, "a", 2, remove("a", 2));
    assert!(c.read(&key()).unwrap().is_some());
    assert_eq!(holds(&mut c), 1);
    deliver(&mut c, "b", 5, remove("b", 5));
    assert!(c.read(&key()).unwrap().is_none());
    assert_eq!(holds(&mut c), 0);
    assert_eq!(c.record_stamp(&key()).unwrap(), 7);
    deliver(&mut c, "a", 3, up("a", 3, 7, Some("A")));
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "A");
    assert_eq!(holds(&mut c), 1);
    let conflict = c
        .apply_channel_page(ChannelPullPage {
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
fn cached_untracked_authority_is_released_and_stamped_null_stays_global() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    subscribe(&mut c, "b");
    c.apply_page(page("a", 0, 1, Some("cache"))).unwrap();
    assert_eq!(holds(&mut c), 0);
    deliver(&mut c, "a", 2, remove("a", 2));
    assert!(c.read(&key()).unwrap().is_none());
    deliver(&mut c, "a", 3, up("a", 3, 2, Some("again")));
    deliver(&mut c, "b", 1, up("b", 1, 3, None));
    assert!(c.read(&key()).unwrap().is_none());
    deliver(&mut c, "b", 2, remove("b", 2));
    deliver(&mut c, "a", 4, remove("a", 4));
    let from = c.cursor("a").unwrap().unwrap();
    let report = c
        .apply_channel_page(ChannelPullPage {
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
fn removal_and_other_channel_upsert_fold_before_release() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    subscribe(&mut c, "b");
    deliver(&mut c, "a", 1, up("a", 1, 7, Some("base")));
    let report = c
        .apply_channel_page(ChannelPullPage {
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
    assert_eq!(holds(&mut c), 1);
}
#[test]
fn clean_local_create_survives_but_patch_does_not_keep_unrelated_replica_fields() {
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
    assert!(c.read(&key()).unwrap().is_none());
    assert_eq!(table_count(&mut c, "axton_local_replica_layer"), 1);
    deliver(&mut c, "a", 4, up("a", 4, 1, Some("server")));
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "server");
    assert_eq!(table_count(&mut c, "axton_local_replica_layer"), 0);
}
#[test]
fn pending_update_and_accepted_or_rejected_receipt_survive_release() {
    for rejected in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut c = open(&dir.path().join("db"));
        subscribe(&mut c, "a");
        deliver(&mut c, "a", 1, up("a", 1, 7, Some("base")));
        let ordinal = c.transaction(|tx| tx.enqueue(mutation("pending"))).unwrap();
        c.freeze().unwrap();
        deliver(&mut c, "a", 2, remove("a", 2));
        assert!(c.read(&key()).unwrap().is_none());
        assert_eq!(table_count(&mut c, "axton_mutation"), 1);
        let r = if rejected {
            rejecting(&mut c, 1, &[ordinal], "no", vec![authority(Some("old"), 8)])
        } else {
            receipt(&mut c, 1, vec![authority(Some("accepted"), 8)])
        };
        c.acknowledge(1, r).unwrap();
        assert!(c.read(&key()).unwrap().is_none());
        assert_eq!(table_count(&mut c, "axton_mutation"), 0);
        assert_eq!(c.record_stamp(&key()).unwrap(), 7);
        assert_eq!(table_count(&mut c, "axton_rejection"), u64::from(rejected));
    }
}
#[test]
fn prepared_removal_rolls_back_members_and_cursor_and_runs_no_store_hook() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    deliver(&mut c, "a", 1, up("a", 1, 7, Some("base")));
    c.begin_session().unwrap();
    let prepared = c
        .prepare_store(StoreDelivery::ChannelPage(ChannelPullPage {
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
    assert_eq!(
        c.session_sql("SELECT present FROM axton_channel_member", &[])
            .unwrap()[0]["present"],
        1
    );
    assert!(c.session(|tx| tx.read(&key())).unwrap().is_some());
    assert_eq!(c.cursor("a").unwrap(), Some(1));
    c.apply_prepared_store(prepared).unwrap();
    c.commit_session().unwrap();
    assert!(c.read(&key()).unwrap().is_none());
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
fn claim(channel: &str, cursor: u64) -> MembershipClaim {
    MembershipClaim {
        channel: channel.into(),
        cursor,
        model: "Entry".into(),
        identity: json!({"id":"e"}),
    }
}
#[test]
fn stale_claim_and_bootstrap_cannot_restore_removal_even_with_newer_body() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    c.transaction(|tx| tx.set_channel("a".into(), true))
        .unwrap();
    acknowledge(&mut c, &[("a", 10)]);
    let id = c.subscription_state("a").unwrap().unwrap().subscription_id;
    let run = c.request_bootstrap("a", id).unwrap().run;
    store_enrolled(&mut c, vec![claim("a", 8)], "base", 7).unwrap();
    deliver(&mut c, "a", 11, remove("a", 11));
    store_enrolled(&mut c, vec![claim("a", 8)], "delayed", 99).unwrap();
    assert!(c.read(&key()).unwrap().is_none());
    assert_eq!(c.record_stamp(&key()).unwrap(), 7);
    let result = c
        .apply_channel_bootstrap_page(
            "a",
            id,
            run,
            0,
            &ChannelBootstrapPage {
                channel: "a".into(),
                from: 0,
                to: 10,
                until: 10,
                head: 11,
                changes: vec![up("a", 8, 99, Some("old bootstrap"))],
            },
        )
        .unwrap();
    assert!(matches!(result, BootstrapApply::Applied { .. }));
    assert!(c.read(&key()).unwrap().is_none());
    assert_eq!(holds(&mut c), 0);
}
#[test]
fn equal_cursor_conflict_rolls_back_whole_delivery() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    subscribe(&mut c, "b");
    store_enrolled(&mut c, vec![claim("a", 2)], "base", 7).unwrap();
    let result = c.apply_channel_page(ChannelPullPage {
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
    assert!(result.is_err());
    assert_eq!(holds(&mut c), 1);
    assert_eq!(c.cursor("a").unwrap(), Some(0));
    assert_eq!(c.cursor("b").unwrap(), Some(0));
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "base");
}
#[test]
fn parent_release_leaves_independently_held_child() {
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
    c.apply_channel_page(ChannelPullPage {
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
            ChannelChange::Upsert {
                channel: "a".into(),
                cursor: 1,
                record: AuthorityRecord {
                    model: parent.model.clone(),
                    identity: parent.identity.clone(),
                    stamp: 1,
                    state: json!({"title":"book"}),
                    error: None,
                },
            },
            ChannelChange::Upsert {
                channel: "b".into(),
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
    c.apply_channel_page(ChannelPullPage {
        cursors: BTreeMap::from([(
            "a".into(),
            CursorRange {
                from: 1,
                to: 2,
                head: 2,
            },
        )]),
        changes: vec![ChannelChange::Remove {
            channel: "a".into(),
            cursor: 2,
            key: parent.clone(),
        }],
    })
    .unwrap();
    assert!(c.read(&parent).unwrap().is_none());
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
        .apply_channel_page(ChannelPullPage {
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
        c.apply_channel_page(ChannelPullPage {
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
                ChannelChange::Upsert {
                    channel: "a".into(),
                    cursor: 1,
                    record: AuthorityRecord {
                        model: parent.model.clone(),
                        identity: parent.identity.clone(),
                        stamp: 1,
                        state: json!({"title":"book"}),
                        error: None,
                    },
                },
                ChannelChange::Upsert {
                    channel: "b".into(),
                    cursor: 1,
                    record: child_record.clone(),
                },
            ],
        })
        .unwrap();
        if child_evicted_first {
            c.apply_channel_page(ChannelPullPage {
                cursors: BTreeMap::from([(
                    "b".into(),
                    CursorRange {
                        from: 1,
                        to: 2,
                        head: 2,
                    },
                )]),
                changes: vec![ChannelChange::Remove {
                    channel: "b".into(),
                    cursor: 2,
                    key: child.clone(),
                }],
            })
            .unwrap();
            c.transaction(|tx| {
                tx.direct(create(
                    "Comment",
                    "c",
                    json!({"bookId":"p","text":"local child"}),
                ))
            })
            .unwrap();
        }
        c.apply_channel_page(ChannelPullPage {
            cursors: BTreeMap::from([(
                "a".into(),
                CursorRange {
                    from: 1,
                    to: 2,
                    head: 2,
                },
            )]),
            changes: vec![ChannelChange::Upsert {
                channel: "a".into(),
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
        c.apply_channel_page(ChannelPullPage {
            cursors: BTreeMap::from([(
                "b".into(),
                CursorRange {
                    from,
                    to: from + 1,
                    head: from + 1,
                },
            )]),
            changes: vec![ChannelChange::Remove {
                channel: "b".into(),
                cursor: from + 1,
                key: child.clone(),
            }],
        })
        .unwrap();
        let report = c
            .apply_channel_page(ChannelPullPage {
                cursors: BTreeMap::from([(
                    "b".into(),
                    CursorRange {
                        from: from + 1,
                        to: from + 2,
                        head: from + 2,
                    },
                )]),
                changes: vec![ChannelChange::Upsert {
                    channel: "b".into(),
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
fn prepared_bootstrap_rollback_retains_no_members_or_run_progress() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    c.transaction(|tx| tx.set_channel("a".into(), true))
        .unwrap();
    acknowledge(&mut c, &[("a", 1)]);
    let id = c.subscription_state("a").unwrap().unwrap().subscription_id;
    let run = c.request_bootstrap("a", id).unwrap().run;
    c.begin_session().unwrap();
    let prepared = c
        .prepare_store(StoreDelivery::ChannelBootstrap {
            scope: "a".into(),
            subscription_id: id,
            run,
            expected_after: 0,
            page: ChannelBootstrapPage {
                channel: "a".into(),
                from: 0,
                to: 1,
                until: 1,
                head: 1,
                changes: vec![up("a", 1, 7, Some("base"))],
            },
        })
        .unwrap();
    assert_eq!(prepared.accepted(), &[0]);
    assert_eq!(
        c.session_sql("SELECT COUNT(*) AS n FROM axton_channel_member", &[])
            .unwrap()[0]["n"],
        0
    );
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
fn invalid_controls_roll_back_and_bad_bodies_keep_positive_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    let malformed = ChannelPullPage {
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
    assert!(c.apply_channel_page(malformed).is_err());
    assert_eq!(c.cursor("a").unwrap(), Some(0));
    assert_eq!(holds(&mut c), 0);
    let mut record = authority(Some("invalid"), 7);
    record.state = json!({"text":99});
    let report = c
        .apply_channel_page(ChannelPullPage {
            cursors: BTreeMap::from([(
                "a".into(),
                CursorRange {
                    from: 0,
                    to: 1,
                    head: 1,
                },
            )]),
            changes: vec![ChannelChange::Upsert {
                channel: "a".into(),
                cursor: 1,
                record,
            }],
        })
        .unwrap();
    assert_eq!(report.skipped(), 1);
    assert_eq!(holds(&mut c), 1);
    assert_eq!(c.record_stamp(&key()).unwrap(), 0);
    assert_eq!(c.cursor("a").unwrap(), Some(1));
    assert!(ChannelPullPage::decode(&serde_json::to_vec(&json!({"cursors":{"a":{"from":1,"to":2,"head":2}},"changes":[{"kind":"remove","channel":"a","cursor":2,"model":"Entry","identity":{"id":"e"},"stamp":9,"state":null}]})).unwrap()).is_err());
}
#[test]
fn dirty_direct_create_survives_release_and_settlement_then_reopen() {
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
    let r = receipt(&mut c, 1, vec![authority(Some("stale receipt"), 8)]);
    c.acknowledge(1, r).unwrap();
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "local recreated");
    drop(c);
    let mut c = open(&path);
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "local recreated");
    deliver(&mut c, "a", 3, remove("a", 3));
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "local recreated");
    deliver(&mut c, "a", 4, up("a", 4, 7, Some("base")));
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
    c.apply_channel_page(ChannelPullPage {
        cursors: BTreeMap::from([(
            "a".into(),
            CursorRange {
                from: 0,
                to: 1,
                head: 1,
            },
        )]),
        changes: vec![ChannelChange::Remove {
            channel: "a".into(),
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
        db.execute("DROP TABLE axton_local_replica_layer", [])
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
fn receipt_cannot_repopulate_released_authoritative_absence() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = open(&dir.path().join("db"));
    subscribe(&mut c, "a");
    deliver(&mut c, "a", 1, up("a", 1, 7, Some("base")));
    c.transaction(|tx| tx.enqueue(mutation("pending"))).unwrap();
    c.freeze().unwrap();
    deliver(&mut c, "a", 2, up("a", 2, 8, None));
    deliver(&mut c, "a", 3, remove("a", 3));
    let r = receipt(&mut c, 1, vec![authority(Some("late body"), 9)]);
    c.acknowledge(1, r).unwrap();
    assert!(c.read(&key()).unwrap().is_none());
    assert_eq!(c.record_stamp(&key()).unwrap(), 8);
    assert_eq!(c.pending_count().unwrap(), 0);
}
