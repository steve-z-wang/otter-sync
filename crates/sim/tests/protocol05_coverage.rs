use axton_core::v05::*;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
fn ctx() -> RequestContext {
    RequestContext {
        protocol: 5,
        store_id: "store".into(),
        stream: "User:a".into(),
        materialization: "m1".into(),
    }
}
fn key(model: &str, id: &str) -> RecordKey {
    RecordKey {
        model: model.into(),
        identity: json!({"id":id}),
    }
}
fn record(model: &str, id: &str, cursor: u64, value: Value) -> AuthorityChange {
    AuthorityChange::Record {
        key: key(model, id),
        cursor,
        state: value,
    }
}
fn plan(
    changes: Vec<AuthorityChange>,
    after: u64,
    through: u64,
    head: u64,
    unique: &[&str],
    edges: &[(RecordKey, RecordKey)],
) -> Vec<DeliveryUnit> {
    plan_units(
        &changes,
        &unique.iter().map(|m| m.to_string()).collect(),
        edges,
        after,
        through,
        head,
    )
    .unwrap()
}
fn freeze(
    units: Vec<DeliveryUnit>,
    purpose: DeliveryPurpose,
    after: u64,
    through: u64,
    head: u64,
    limit: usize,
) -> FrozenDelivery {
    match purpose {
        DeliveryPurpose::Settlement {
            batch_id,
            mutation_id,
        } => freeze_materialization(
            ctx(),
            "plan".into(),
            MaterializationOwner::Settlement {
                batch_id,
                mutation_id,
            },
            head,
            100,
            units,
            limit,
        ),
        DeliveryPurpose::Schema {
            previous_materialization,
        } => freeze_materialization(
            ctx(),
            "plan".into(),
            MaterializationOwner::Schema {
                previous_materialization,
            },
            head,
            100,
            units,
            limit,
        ),
        _ => freeze_delivery(
            ctx(),
            "plan".into(),
            purpose,
            after,
            through,
            head,
            100,
            units,
            limit,
        ),
    }
    .unwrap()
}
// Authority overlay harness. Constraint checks happen on the whole candidate
// projection before publishing it; production planner/stager choose the unit.
fn apply(base: &mut BTreeMap<String, (u64, Value)>, unit: &DeliveryUnit, unique: bool) {
    let mut next = base.clone();
    for change in &unit.changes {
        let id = change.key().encoded().unwrap();
        if next
            .get(&id)
            .is_some_and(|(cursor, _)| *cursor >= change.cursor())
        {
            continue;
        }
        match change {
            AuthorityChange::Record { cursor, state, .. } => {
                next.insert(id, (*cursor, state.clone()));
            }
            AuthorityChange::Remove { cursor, .. } => {
                next.insert(id, (*cursor, Value::Null));
            }
        }
    }
    if unique {
        let values: Vec<_> = next
            .values()
            .filter_map(|(_, v)| v.get("unique").filter(|v| !v.is_null()))
            .collect();
        let distinct: BTreeSet<_> = values.iter().map(|v| v.to_string()).collect();
        assert_eq!(
            values.len(),
            distinct.len(),
            "atomic projection violates unique constraint"
        );
    }
    *base = next;
}
#[test]
fn a5_bootstrap_record_moves_past_start_and_later_sync_wins() {
    let units = plan(
        vec![record("Entry", "e", 45, json!({"text":"b"}))],
        0,
        40,
        45,
        &[],
        &[],
    );
    assert_eq!(units[0].through, Some(40));
    assert_eq!(units[0].changes[0].cursor(), 45);
    let frozen = freeze(units, DeliveryPurpose::Bootstrap, 0, 40, 45, 1);
    let mut progress = DeliveryProgress::new(&frozen.header, &ctx(), 0).unwrap();
    assert_eq!(progress.covered, None);
    let mut base = BTreeMap::from([(
        key("Entry", "e").encoded().unwrap(),
        (20, json!({"text":"a"})),
    )]);
    let unit = progress
        .stage_with_limit(&frozen.parts[0], &ctx(), 1, 1024 * 1024)
        .unwrap()
        .unwrap();
    // Backend's row moving after freeze must not relabel or alter frozen b@45.
    let current = record("Entry", "e", 47, json!({"text":"new backend"}));
    assert_eq!(current.cursor(), 47);
    apply(&mut base, &unit, false);
    progress.commit(&unit, &ctx(), 1).unwrap();
    assert_eq!(
        base[&key("Entry", "e").encoded().unwrap()],
        (45, json!({"text":"b"}))
    );
    assert_eq!(progress.covered, Some(40));
    let mut concurrent = BTreeMap::from([(
        key("Entry", "e").encoded().unwrap(),
        (46, json!({"text":"sync"})),
    )]);
    apply(&mut concurrent, &unit, false);
    assert_eq!(
        concurrent[&key("Entry", "e").encoded().unwrap()],
        (46, json!({"text":"sync"}))
    );
}
#[test]
fn a5_expiry_mid_unit_preserves_progress_and_restart_sees_remove() {
    let units = plan(
        vec![
            record("Entry", "a", 10, json!({"text":"a"})),
            record("Entry", "b", 20, json!({"text":"b"})),
            record("Entry", "c", 20, json!({"text":"c"})),
        ],
        0,
        40,
        40,
        &[],
        &[],
    );
    let frozen = freeze(units, DeliveryPurpose::Bootstrap, 0, 40, 40, 1);
    assert_eq!(frozen.header.units.len(), 2);
    let mut progress = DeliveryProgress::new(&frozen.header, &ctx(), 0).unwrap();
    let mut base = BTreeMap::new();
    let unit = progress
        .stage_with_limit(&frozen.parts[0], &ctx(), 1, 1024 * 1024)
        .unwrap()
        .unwrap();
    apply(&mut base, &unit, false);
    progress.commit(&unit, &ctx(), 1).unwrap();
    assert_eq!(progress.covered, Some(19));
    assert!(
        progress
            .stage_with_limit(&frozen.parts[1], &ctx(), 1, 1024 * 1024)
            .unwrap()
            .is_none()
    );
    assert_eq!(progress.covered, Some(19));
    let before = progress.clone();
    assert!(
        progress
            .stage_with_limit(&frozen.parts[2], &ctx(), 100, 1024 * 1024)
            .is_err()
    );
    assert_eq!(progress, before);
    // Lost staging cannot make B=S. Replacement from committed B includes newer Remove.
    let replacement = plan(
        vec![
            AuthorityChange::Remove {
                key: key("Entry", "a"),
                cursor: 42,
            },
            record("Entry", "b", 20, json!({"text":"b"})),
            record("Entry", "c", 20, json!({"text":"c"})),
        ],
        19,
        40,
        42,
        &[],
        &[],
    );
    for unit in &replacement {
        apply(&mut base, unit, false);
    }
    assert!(base[&key("Entry", "a").encoded().unwrap()].1.is_null());
    assert_eq!(replacement.last().unwrap().through, Some(40));
}
#[test]
fn a5_zero_start_and_empty_selection_are_real_completed_boundaries() {
    for changes in [
        vec![],
        vec![record("Entry", "new", 3, json!({"text":"new"}))],
    ] {
        let units = plan(changes, 0, 0, 3, &[], &[]);
        let frozen = freeze(units, DeliveryPurpose::Bootstrap, 0, 0, 3, 1);
        let mut p = DeliveryProgress::new(&frozen.header, &ctx(), 0).unwrap();
        assert_eq!(p.covered, None);
        for part in &frozen.parts {
            if let Some(unit) = p.stage_with_limit(part, &ctx(), 1, 1024 * 1024).unwrap() {
                p.commit(&unit, &ctx(), 1).unwrap();
            }
        }
        assert_eq!(p.covered, Some(0));
        assert_eq!(p.next_unit, frozen.header.units.len() as u64);
    }
    let empty = plan(vec![], 0, 40, 50, &[], &[]);
    assert_eq!(empty[0].through, Some(40));
    assert!(empty[0].changes.is_empty());
}
#[test]
fn a6_compacted_unique_transfer_fragments_and_crash_keep_valid_final_projection() {
    let changes = vec![
        record("Item", "A", 5, json!({"unique":"y"})),
        record("Item", "B", 3, json!({"unique":"x"})),
        record("Other", "c", 8, json!({"text":"c"})),
    ];
    let units = plan(changes, 1, 8, 8, &["Item"], &[]);
    assert_eq!(units.len(), 2);
    assert_eq!(units[0].changes.len(), 2);
    assert_eq!(units[0].through, Some(7));
    let frozen = freeze(units, DeliveryPurpose::Sync, 1, 8, 8, 1);
    for order in [[0, 1], [1, 0]] {
        let mut p = DeliveryProgress::new(&frozen.header, &ctx(), 0).unwrap();
        let mut base = BTreeMap::from([(
            key("Item", "A").encoded().unwrap(),
            (1, json!({"unique":"x"})),
        )]);
        assert!(
            p.stage_with_limit(&frozen.parts[order[0]], &ctx(), 1, 1024 * 1024)
                .unwrap()
                .is_none()
        );
        assert_eq!(p.covered, None);
        assert_eq!(base.len(), 1);
        // Serialize incomplete parts as the real durable restart boundary.
        p = decode(&encode(&p).unwrap()).unwrap();
        let unit = p
            .stage_with_limit(&frozen.parts[order[1]], &ctx(), 1, 1024 * 1024)
            .unwrap()
            .unwrap();
        apply(&mut base, &unit, true);
        p.commit(&unit, &ctx(), 1).unwrap();
        assert_eq!(
            base[&key("Item", "A").encoded().unwrap()].1,
            json!({"unique":"y"})
        );
        assert_eq!(
            base[&key("Item", "B").encoded().unwrap()].1,
            json!({"unique":"x"})
        );
        assert_eq!(p.covered, Some(7));
        p = decode(&encode(&p).unwrap()).unwrap();
        let unit = p
            .stage_with_limit(&frozen.parts[2], &ctx(), 1, 1024 * 1024)
            .unwrap()
            .unwrap();
        apply(&mut base, &unit, true);
        p.commit(&unit, &ctx(), 1).unwrap();
        assert_eq!(p.covered, Some(8));
        assert_eq!(base.len(), 3);
    }
}
#[test]
fn a6_same_cursor_and_dependency_components_are_transitive() {
    let changes = vec![
        record("A", "a", 4, json!({})),
        record("B", "b", 4, json!({})),
        record("B", "c", 7, json!({})),
        record("C", "d", 9, json!({})),
    ];
    let units = plan(changes, 0, 9, 9, &["B"], &[(key("B", "c"), key("C", "d"))]);
    assert_eq!(units.len(), 1);
    assert_eq!(units[0].changes.len(), 4);
    let frozen = freeze(units, DeliveryPurpose::Sync, 0, 9, 9, 1);
    let mut p = DeliveryProgress::new(&frozen.header, &ctx(), 0).unwrap();
    for part in frozen.parts.iter().rev() {
        let ready = p.stage_with_limit(part, &ctx(), 1, 1024 * 1024).unwrap();
        if part.part != 0 {
            assert!(ready.is_none());
            assert_eq!(p.covered, None);
        } else {
            let unit = ready.unwrap();
            assert_eq!(unit.changes.len(), 4);
            p.commit(&unit, &ctx(), 1).unwrap();
        }
    }
    assert_eq!(p.covered, Some(9));
}
#[test]
fn a7_no_progress_ahead_units_and_empty_range_do_not_skip_required_data() {
    let units = plan(
        vec![
            record("A", "a", 45, json!({})),
            record("B", "b", 46, json!({})),
        ],
        0,
        40,
        46,
        &[],
        &[],
    );
    assert_eq!(units[0].through, Some(39));
    assert_eq!(units[1].through, Some(40));
    let units = plan(
        vec![
            record("A", "a", 45, json!({})),
            record("B", "b", 46, json!({})),
        ],
        40,
        40,
        46,
        &[],
        &[],
    );
    assert_eq!(units[0].through, None);
    assert_eq!(units[1].through, Some(40));
    let frozen = freeze(units, DeliveryPurpose::Bootstrap, 40, 40, 46, 1);
    let mut p = DeliveryProgress::new(&frozen.header, &ctx(), 0).unwrap();
    assert!(
        p.stage_with_limit(&frozen.parts[1], &ctx(), 1, 1024 * 1024)
            .is_err()
    );
    assert_eq!(p.next_unit, 0);
    let unit = p
        .stage_with_limit(&frozen.parts[0], &ctx(), 1, 1024 * 1024)
        .unwrap()
        .unwrap();
    p.commit(&unit, &ctx(), 1).unwrap();
    assert_eq!(p.covered, None);
    let mut wrong = ctx();
    wrong.materialization = "m2".into();
    assert!(
        p.stage_with_limit(&frozen.parts[1], &wrong, 1, 1024 * 1024)
            .is_err()
    );
    let unit = p
        .stage_with_limit(&frozen.parts[1], &ctx(), 1, 1024 * 1024)
        .unwrap()
        .unwrap();
    p.commit(&unit, &ctx(), 1).unwrap();
    assert_eq!(p.covered, Some(40));
}
#[test]
fn a8_receipt_and_authority_orders_owned_private_remove_and_old_targets() {
    let target = SettlementTarget::Stream {
        key: key("Entry", "e"),
        cursor: 20,
        fallback: ReadRecord {
            key: key("Entry", "e"),
            cursor: (),
            state: json!({"text":"accepted"}),
        },
    };
    let mut evidence = BTreeMap::new();
    assert!(
        !settlement_ready(20, Some(40), std::slice::from_ref(&target), "m1", &evidence).unwrap()
    );
    evidence.insert(
        key("Entry", "e").encoded().unwrap(),
        TargetEvidence {
            content_cursor: Some(21),
            materialization: Some("m1".into()),
            removed_cursor: None,
            protected: true,
        },
    );
    assert!(
        !settlement_ready(20, Some(19), std::slice::from_ref(&target), "m1", &evidence).unwrap()
    );
    assert!(
        settlement_ready(20, Some(40), std::slice::from_ref(&target), "m1", &evidence).unwrap()
    );
    assert!(
        !settlement_ready(20, Some(40), std::slice::from_ref(&target), "m2", &evidence).unwrap()
    );
    let removed = TargetEvidence {
        content_cursor: None,
        materialization: None,
        removed_cursor: Some(22),
        protected: false,
    };
    assert_eq!(
        target_disposition(&target, "m1", &removed).unwrap(),
        TargetDisposition::FinalizeOwned
    );
    let protected = TargetEvidence {
        protected: true,
        ..removed
    };
    assert_eq!(
        target_disposition(&target, "m1", &protected).unwrap(),
        TargetDisposition::PreserveAuthority
    );
    let private = SettlementTarget::Private {
        record: ReadRecord {
            key: key("Entry", "private"),
            cursor: (),
            state: json!({"text":"private"}),
        },
    };
    assert!(settlement_ready(40, Some(40), &[private], "m1", &BTreeMap::new()).unwrap());
    assert!(settlement_ready(40, Some(40), &[], "m1", &BTreeMap::new()).unwrap());
    assert!(!settlement_ready(40, None, &[], "m1", &BTreeMap::new()).unwrap());
}
#[test]
fn a14_owned_schema_materialization_installs_without_range_coverage() {
    let unit = DeliveryUnit {
        index: 0,
        through: None,
        changes: vec![record("Entry", "e", 20, json!({"newField":"new"}))],
    };
    for purpose in [
        DeliveryPurpose::Schema {
            previous_materialization: "m0".into(),
        },
        DeliveryPurpose::Settlement {
            batch_id: 1,
            mutation_id: 2,
        },
    ] {
        let frozen = freeze(vec![unit.clone()], purpose, 40, 40, 40, 1);
        let mut p = DeliveryProgress::new(&frozen.header, &ctx(), 0).unwrap();
        let complete = p
            .stage_with_limit(&frozen.parts[0], &ctx(), 1, 1024 * 1024)
            .unwrap()
            .unwrap();
        p.commit(&complete, &ctx(), 1).unwrap();
        assert_eq!(p.covered, None);
        assert_eq!(p.next_unit, 1);
    }
}
#[test]
fn a15_one_publication_position_per_affected_stream_and_overflow_is_atomic() {
    let heads = BTreeMap::from([("User:a".into(), 5), ("User:b".into(), 20)]);
    let affected = BTreeSet::from(["User:a".into(), "User:b".into()]);
    let positions = publication_positions(&heads, &affected).unwrap();
    assert_eq!(
        positions,
        BTreeMap::from([("User:a".into(), 6), ("User:b".into(), 21)])
    );
    assert_eq!(heads["User:a"], 5);
    let many = plan(
        vec![
            record("Entry", "a", 6, json!({})),
            record("Entry", "b", 6, json!({})),
        ],
        5,
        6,
        6,
        &[],
        &[],
    );
    assert_eq!(many.len(), 1);
    assert_eq!(many[0].changes.len(), 2);
    let overflow = BTreeMap::from([
        ("User:a".into(), axton_core::MAX_SAFE_INTEGER),
        ("User:b".into(), 20),
    ]);
    assert!(publication_positions(&overflow, &affected).is_err());
    assert_eq!(overflow["User:b"], 20);
}

#[test]
fn many_independent_units_preserve_ordered_coverage_and_ahead_barrier() {
    let count = 256usize;
    let head = (count * 3) as u64;
    let changes: Vec<_> = (0..count)
        .rev()
        .map(|index| {
            record(
                "Independent",
                &index.to_string(),
                ((index + 1) * 3) as u64,
                json!({"index":index}),
            )
        })
        .collect();
    for through in [40, head] {
        let units = plan(changes.clone(), 0, through, head, &[], &[]);
        assert_eq!(units.len(), count);
        for (index, unit) in units.iter().enumerate() {
            assert_eq!(unit.index, index as u64);
            assert_eq!(unit.changes.len(), 1);
            assert_eq!(unit.changes[0].cursor(), ((index + 1) * 3) as u64);
            let expected = if index + 1 == count {
                Some(through)
            } else if through == head {
                Some(((index + 2) * 3 - 1) as u64)
            } else if index < 13 {
                Some((((index + 2) * 3 - 1) as u64).min(39))
            } else {
                None
            };
            assert_eq!(unit.through, expected, "coverage at unit {index}");
        }
        let frozen = freeze(units.clone(), DeliveryPurpose::Sync, 0, through, head, 1);
        validate_delivery(&frozen.header, &units).unwrap();
        let mut invalid = frozen.header;
        invalid.units[128].minimum_cursor = Some(1);
        invalid.digest = delivery_digest(&invalid).unwrap();
        assert!(
            invalid.validate().is_err(),
            "a late low cursor crossed committed coverage"
        );
    }
}
