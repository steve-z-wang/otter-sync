use axton_core::v04;
use axton_core::v04::*;
use serde_json::json;

fn context() -> RequestContext {
    RequestContext {
        protocol: 4,
        binding: StoreBinding {
            backend: "backend".into(),
            viewer: "alice".into(),
            stream: "User:alice".into(),
            contract: "app".into(),
        },
        materialization: "schema-one".into(),
        incarnation: "open-one".into(),
    }
}
fn key() -> axton_core::RecordKey {
    axton_core::RecordKey {
        model: "Entry".into(),
        identity: json!({"id":"x"}),
    }
}

#[test]
fn ordinary_read_requires_explicit_null_cursor_and_refuses_legacy_authority() {
    let base = json!({"model":"Entry", "identity":{"id":"x"}, "cursor":null, "state":{"text":"A"}});
    assert!(decode::<ReadRecord>(&serde_json::to_vec(&base).unwrap()).is_ok());
    for field in ["cursor", "stamp"] {
        let mut value = base.clone();
        value[field] = json!(57);
        assert!(decode::<ReadRecord>(&serde_json::to_vec(&value).unwrap()).is_err());
    }
    let mut missing = base;
    missing.as_object_mut().unwrap().remove("cursor");
    assert!(decode::<ReadRecord>(&serde_json::to_vec(&missing).unwrap()).is_err());
}

#[test]
fn cache_admission_uses_current_protection_not_historical_position() {
    let mut evidence = RecordEvidence::default();
    assert!(evidence.install("schema-one", 57, false).unwrap());
    assert!(!evidence.allows_cache());
    evidence.direct_write();
    assert!(evidence.allows_cache());
    assert!(!evidence.install("schema-one", 57, false).unwrap());
    assert_eq!(evidence.history["schema-one"], 57);
    assert!(evidence.install("schema-one", 58, true).unwrap());
    evidence.direct_write();
    assert!(!evidence.allows_cache());
    assert!(!evidence.install("schema-one", 56, false).unwrap());
}

#[test]
fn new_materialization_accepts_same_position_without_erasing_old_history() {
    let mut evidence = RecordEvidence::default();
    evidence.install("schema-one", 57, false).unwrap();
    assert!(evidence.install("schema-two", 57, false).unwrap());
    assert_eq!(evidence.history.len(), 2);
    assert_eq!(
        evidence.current.as_ref().unwrap().materialization,
        "schema-two"
    );
}

#[test]
fn bootstrap_requires_manifest_coverage_even_when_cursor_passes_barrier() {
    let mut coverage =
        BootstrapCoverage::new("manifest".into(), "schema-one".into(), 100, 1).unwrap();
    assert!(coverage.set_tail(101).is_err());
    assert!(!coverage.complete(149).unwrap());
    // X moved from 50 to 101 to 150. Cover its fixed manifest ordinal,
    // independent of either its old scan position or the delta barrier.
    coverage.commit_prefix(0, 1).unwrap();
    coverage.set_tail(101).unwrap();
    assert!(coverage.complete(149).unwrap());
    assert!(coverage.commit_prefix(0, 1).is_err());
    assert!(coverage.set_tail(150).is_err());
    let restored: BootstrapCoverage = decode(&encode(&coverage).unwrap()).unwrap();
    assert_eq!(restored, coverage);
}

#[test]
fn request_binding_and_mode_are_part_of_canonical_replay_identity() {
    let request = ReadIntent {
        context: context(),
        call_id: "00000000-0000-4000-8000-000000000001".into(),
        name: "Entries".into(),
        version: 1,
        args: json!({}),
        store: true,
    };
    let bytes = encode(&request).unwrap();
    let mut explicit: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    explicit["store"] = json!(true);
    let explicit: ReadIntent = decode(&serde_json::to_vec(&explicit).unwrap()).unwrap();
    assert_eq!(encode(&explicit).unwrap(), bytes);
    let mut temporary = request.clone();
    temporary.store = false;
    assert_ne!(encode(&temporary).unwrap(), bytes);
    let mut changed = context();
    changed.binding.stream = "Workspace:one".into();
    assert!(request.context.admit(&changed).is_err());
    changed = context();
    changed.materialization = "schema-two".into();
    assert!(request.context.admit(&changed).is_err());
    assert!(
        decode::<ReadIntent>(&json!({"store":{"entries":false}}).to_string().into_bytes()).is_err()
    );
}

#[test]
fn partial_page_progress_requires_exact_durable_unit_prefix() {
    let page = DeltaPage {
        context: context(),
        page_id: "page-one".into(),
        from: 10,
        to: 20,
        head: 25,
        units: vec![
            CommitUnit {
                through: 15,
                changes: vec![StreamChange::Upsert {
                    record: StreamRecord {
                        key: key(),
                        cursor: 25,
                        state: json!({"text":"A"}),
                    },
                }],
            },
            CommitUnit {
                through: 20,
                changes: vec![],
            },
        ],
    };
    page.validate().unwrap();
    let mut progress = PageProgress::new(&page).unwrap();
    progress.commit(&page, 0).unwrap();
    assert_eq!(progress.cursor, 15);
    assert!(progress.commit(&page, 0).is_err());
    let mut restored: PageProgress = decode(&encode(&progress).unwrap()).unwrap();
    restored.commit(&page, 1).unwrap();
    assert_eq!(restored.cursor, 20);
    assert!(restored.complete(&page));
    let mut other = page.clone();
    other.page_id = "other".into();
    assert!(progress.commit(&other, 1).is_err());
}

#[test]
fn private_settlement_has_no_cursor_and_never_claims_stream_authority() {
    let target = SettlementTarget::Private {
        record: ReadRecord {
            key: key(),
            cursor: NullCursor,
            state: json!({"text":"B"}),
        },
    };
    let bytes = encode(&target).unwrap();
    assert!(
        String::from_utf8(bytes.clone())
            .unwrap()
            .contains("\"cursor\":null")
    );
    let mut raw: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    raw["record"]["cursor"] = json!(58);
    assert!(decode::<SettlementTarget>(&serde_json::to_vec(&raw).unwrap()).is_err());
    let mut evidence = RecordEvidence::default();
    evidence.install("schema-one", 57, false).unwrap();
    assert!(target.ready("schema-one", &evidence));
    assert!(!evidence.allows_cache());
    // A current-protected, previously removed identity finishes its Call
    // while retaining A@57. Private B is an owned accepted state, not authority.
    assert_eq!(evidence.current.as_ref().unwrap().cursor, 57);
    let stream = SettlementTarget::Stream {
        key: key(),
        cursor: 58,
        fallback: ReadRecord {
            key: key(),
            cursor: NullCursor,
            state: json!({"text":"B"}),
        },
    };
    assert!(!stream.ready("schema-one", &evidence));
    evidence.install("schema-one", 58, false).unwrap();
    assert!(stream.ready("schema-one", &evidence));
}

#[test]
fn envelope_refuses_other_protocol_and_unsafe_positions() {
    let mut ctx = context();
    ctx.protocol = 3;
    assert!(encode(&ctx).is_err());
    let target = SettlementTarget::Stream {
        key: key(),
        cursor: axton_core::MAX_SAFE_INTEGER + 1,
        fallback: ReadRecord {
            key: key(),
            cursor: NullCursor,
            state: json!({"text":"B"}),
        },
    };
    assert!(encode(&target).is_err());
}

#[test]
fn membership_remove_releases_live_content_but_preserves_true_deletion() {
    let mut live = RecordEvidence::default();
    live.install("schema-one", 57, false).unwrap();
    assert!(live.remove(58).unwrap());
    assert!(live.allows_cache());
    assert_eq!(live.history["schema-one"], 57);
    assert!(!live.remove(57).unwrap());
    assert!(!live.install("schema-two", 57, false).unwrap());
    assert!(live.install("schema-two", 59, false).unwrap());
    assert!(!live.remove(58).unwrap());
    assert!(!live.allows_cache());
    let mut deleted = RecordEvidence::default();
    deleted.install("schema-one", 57, true).unwrap();
    deleted.remove(58).unwrap();
    assert!(!deleted.allows_cache());
    assert_eq!(deleted.history["schema-one"], 57);
}

#[test]
fn rematerialization_preserves_direct_guard_state_and_never_regresses_position() {
    let mut evidence = RecordEvidence::default();
    evidence.install("schema-one", 57, false).unwrap();
    evidence.direct_write();
    assert_eq!(
        evidence.admission("schema-two", 57).unwrap(),
        AuthorityAdmission::Rematerialize
    );
    evidence.install("schema-two", 57, false).unwrap();
    assert!(evidence.allows_cache()); // actual consumer must replay direct D over adapted base
    assert_eq!(evidence.history.len(), 2);
    evidence.install("schema-two", 58, false).unwrap();
    assert_eq!(
        evidence.admission("schema-three", 57).unwrap(),
        AuthorityAdmission::Duplicate
    );
    assert!(!evidence.install("schema-three", 57, false).unwrap());
    assert_eq!(evidence.current.as_ref().unwrap().cursor, 58);
}

fn mutation() -> MutationIntent {
    MutationIntent {
        context: context(),
        call_id: "00000000-0000-4000-8000-000000000001".into(),
        name: "Edit".into(),
        version: 1,
        args: json!({"entry":{"id":"x","text":"B"}}),
        models: std::collections::BTreeMap::from([("Entry".into(), 1)]),
    }
}
fn receipt(intent: &MutationIntent, targets: Vec<SettlementTarget>) -> MutationReceipt {
    MutationReceipt {
        context: intent.context.clone(),
        intent_digest: intent.digest().unwrap(),
        completion: axton_core::CallCompletion {
            call_id: intent.call_id.clone(),
            outcome: axton_core::ActionOutcome::Succeeded { result: json!({}) },
        },
        targets,
    }
}

#[test]
fn frozen_mutation_receipt_survives_context_rollover_but_not_rebinding_or_intent_change() {
    let intent = mutation();
    let response = receipt(
        &intent,
        vec![SettlementTarget::Private {
            record: ReadRecord {
                key: key(),
                cursor: NullCursor,
                state: json!({"text":"B"}),
            },
        }],
    );
    let mut active = context();
    active.materialization = "schema-two".into();
    assert!(intent.context.admit(&active).is_err());
    response.admit(&intent, &active).unwrap();
    response.validate_targets(&[key()]).unwrap(); // Draft companion intentionally absent
    let mut changed = intent.clone();
    changed.args["entry"]["text"] = json!("D");
    assert!(response.admit(&changed, &active).is_err());
    active.binding.stream = "Workspace:two".into();
    assert!(response.admit(&intent, &active).is_err());
}

#[test]
fn receipt_requires_exact_server_visible_targets_without_local_companions() {
    let intent = mutation();
    let empty = receipt(&intent, vec![]);
    assert!(empty.validate_targets(&[key()]).is_err());
    empty.validate_targets(&[]).unwrap(); // no input/no-op can still settle owned Draft locally
    let private = SettlementTarget::Private {
        record: ReadRecord {
            key: key(),
            cursor: NullCursor,
            state: json!({"text":"B"}),
        },
    };
    let duplicate = receipt(&intent, vec![private.clone(), private]);
    assert!(encode(&duplicate).is_err());
    let mut rejected = empty;
    rejected.completion.outcome = axton_core::ActionOutcome::Failed {
        code: "edit.not_allowed".into(),
        execution: axton_core::ExecutionState::Rejected,
    };
    rejected.validate_targets(&[key()]).unwrap(); // outcome removes local owned operations
}

#[test]
fn exact_manifest_page_covers_keys_even_after_remove_and_survives_restart() {
    let page = ManifestPage {
        context: context(),
        manifest_id: "manifest".into(),
        total: 1,
        from: 0,
        to: 1,
        companions: vec![],
        items: vec![ManifestItem {
            ordinal: 0,
            change: StreamChange::Remove {
                key: key(),
                cursor: 150,
            },
        }],
    };
    let decoded: ManifestPage = decode(&encode(&page).unwrap()).unwrap();
    let mut coverage =
        BootstrapCoverage::new("manifest".into(), "schema-one".into(), 100, 1).unwrap();
    coverage.commit_page(&decoded, &context()).unwrap();
    coverage.set_tail(151).unwrap();
    assert!(!coverage.complete(150).unwrap());
    let restored: BootstrapCoverage = decode(&encode(&coverage).unwrap()).unwrap();
    assert!(restored.complete(151).unwrap());
    let mut missing = page.clone();
    missing.items.clear();
    assert!(encode(&missing).is_err());
    let mut other = context();
    other.binding.viewer = "bob".into();
    assert!(coverage.commit_page(&page, &other).is_err());
}

#[test]
fn persisted_page_plan_uses_digest_and_detects_payload_change_under_same_id() {
    let page = DeltaPage {
        context: context(),
        page_id: "page".into(),
        from: 0,
        to: 1,
        head: 1,
        units: vec![CommitUnit {
            through: 1,
            changes: vec![StreamChange::Upsert {
                record: StreamRecord {
                    key: key(),
                    cursor: 1,
                    state: json!({"text":"A"}),
                },
            }],
        }],
    };
    let mut progress = PageProgress::new(&page).unwrap();
    assert_eq!(progress.plan.len(), 64);
    let mut changed = page.clone();
    if let StreamChange::Upsert { record } = &mut changed.units[0].changes[0] {
        record.state["text"] = json!("B");
    }
    assert!(progress.commit(&changed, 0).is_err());
    progress.commit(&page, 0).unwrap();
}

#[test]
fn read_modes_and_missing_snapshots_never_implicitly_delete() {
    let present = ReadRecord {
        key: key(),
        cursor: NullCursor,
        state: json!({"text":"cache"}),
    };
    let absent = ReadRecord {
        key: key(),
        cursor: NullCursor,
        state: json!(null),
    };
    let mut evidence = RecordEvidence::default();
    assert_eq!(
        present.disposition(false, &evidence).unwrap(),
        ReadDisposition::SnapshotOnly
    );
    assert_eq!(
        present.disposition(true, &evidence).unwrap(),
        ReadDisposition::StoreCache
    );
    assert_eq!(
        absent.disposition(true, &evidence).unwrap(),
        ReadDisposition::Absent
    );
    evidence.install("schema-one", 57, true).unwrap();
    assert_eq!(
        present.disposition(true, &evidence).unwrap(),
        ReadDisposition::Protected
    );
    assert_eq!(
        present.disposition(false, &evidence).unwrap(),
        ReadDisposition::SnapshotOnly
    );
    assert_eq!(evidence.history["schema-one"], 57);
}

#[test]
fn direct_response_admission_is_stricter_than_retained_receipt_admission() {
    let intent = mutation();
    let read = ReadResponse {
        context: intent.context.clone(),
        completion: axton_core::CallCompletion {
            call_id: intent.call_id.clone(),
            outcome: axton_core::ActionOutcome::Succeeded {
                result: json!(null),
            },
        },
        records: vec![],
    };
    read.admit(&intent.call_id, &intent.context, &context())
        .unwrap();
    let mut active = context();
    active.materialization = "schema-two".into();
    assert!(
        read.admit(&intent.call_id, &intent.context, &active)
            .is_err()
    );
    receipt(&intent, vec![]).admit(&intent, &active).unwrap();
}

#[test]
fn private_disposition_never_releases_stream_protection() {
    let private = SettlementTarget::Private {
        record: ReadRecord {
            key: key(),
            cursor: NullCursor,
            state: json!({"text":"B"}),
        },
    };
    let mut evidence = RecordEvidence::default();
    evidence.install("schema-one", 57, false).unwrap();
    assert_eq!(
        private.disposition("schema-one", &evidence).unwrap(),
        SettlementDisposition::AcknowledgeProtected
    );
    evidence.remove(58).unwrap();
    assert_eq!(
        private.disposition("schema-one", &evidence).unwrap(),
        SettlementDisposition::FinalizeOwnedNull
    );
    assert_eq!(evidence.history["schema-one"], 57);
    evidence.install("schema-one", 59, true).unwrap();
    assert_eq!(
        private.disposition("schema-one", &evidence).unwrap(),
        SettlementDisposition::AcknowledgeProtected
    );
    assert!(!evidence.allows_cache());
}

#[test]
fn fetch_store_default_normalizes_and_requires_exact_identity() {
    let value = json!({"context":context(),"callId":"00000000-0000-4000-8000-000000000001","model":"Entry","version":1,"identity":{"id":"x"}});
    let default: FetchIntent = decode(value.to_string().as_bytes()).unwrap();
    let mut explicit = value.clone();
    explicit["store"] = json!(true);
    let explicit: FetchIntent = decode(explicit.to_string().as_bytes()).unwrap();
    assert_eq!(encode(&default).unwrap(), encode(&explicit).unwrap());
    let mut response = ReadResponse {
        context: context(),
        completion: axton_core::CallCompletion {
            call_id: default.call_id.clone(),
            outcome: axton_core::ActionOutcome::Succeeded {
                result: json!({"id":"x","text":"B"}),
            },
        },
        records: vec![ReadRecord {
            key: key(),
            cursor: NullCursor,
            state: json!({"text":"B"}),
        }],
    };
    default.admit_response(&response, &context()).unwrap();
    response.records[0].key.identity = json!({"id":"other"});
    assert!(default.admit_response(&response, &context()).is_err());
}

#[test]
fn later_unit_cannot_reveal_a_change_below_an_already_committed_prefix() {
    let page = DeltaPage {
        context: context(),
        page_id: "gap".into(),
        from: 0,
        to: 7,
        head: 7,
        units: vec![
            CommitUnit {
                through: 4,
                changes: vec![],
            },
            CommitUnit {
                through: 7,
                changes: vec![StreamChange::Upsert {
                    record: StreamRecord {
                        key: key(),
                        cursor: 2,
                        state: json!({"text":"missed"}),
                    },
                }],
            },
        ],
    };
    assert!(page.validate().is_err());
}

#[test]
fn shared_wire_fixture_is_consumable_by_all_contract_paths() {
    let fixtures: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/protocol/0.4.json")).unwrap();
    let bytes = |name: &str| serde_json::to_vec(&fixtures[name]).unwrap();
    let active: RequestContext = decode(&bytes("context")).unwrap();
    let _: ReadRecord = decode(&bytes("readRecord")).unwrap();
    let fetch: FetchIntent = decode(&bytes("fetchIntent")).unwrap();
    let response: ReadResponse = decode(&bytes("readResponse")).unwrap();
    fetch.admit_response(&response, &active).unwrap();
    let intent: MutationIntent = decode(&bytes("mutationIntent")).unwrap();
    let receipt: MutationReceipt = decode(&bytes("mutationReceipt")).unwrap();
    receipt.admit(&intent, &active).unwrap();
    receipt.validate_targets(&[key()]).unwrap();
    let tracked: MutationReceipt = decode(&bytes("trackedMutationReceipt")).unwrap();
    tracked.admit(&intent, &active).unwrap();
    tracked.validate_targets(&[key()]).unwrap();
    let mut missed = RecordEvidence::default();
    missed.install("schema-one", 57, false).unwrap();
    missed.remove(60).unwrap();
    assert_eq!(
        tracked.targets[0]
            .disposition("schema-one", &missed)
            .unwrap(),
        SettlementDisposition::FinalizeOwnedNull
    );
    assert_eq!(
        tracked.targets[0].owned_snapshot().state,
        json!({"text":"SERVER-B"})
    );
    let manifest: ManifestPage = decode(&bytes("manifestPage")).unwrap();
    let mut coverage =
        BootstrapCoverage::new("manifest".into(), active.materialization.clone(), 100, 1).unwrap();
    coverage.commit_page(&manifest, &active).unwrap();
    let page: DeltaPage = decode(&bytes("deltaPage")).unwrap();
    let mut progress = PageProgress::new(&page).unwrap();
    progress.commit(&page, 0).unwrap();
    assert_eq!(progress.cursor, 15);
    progress.commit(&page, 1).unwrap();
    assert!(progress.complete(&page));
}

#[test]
fn persisted_incarnation_survives_reopen_but_reset_rejects_old_receipts() {
    let intent = mutation();
    let reopened: RequestContext = decode(&encode(&context()).unwrap()).unwrap();
    intent.context.admit(&reopened).unwrap();
    let response = receipt(&intent, vec![]);
    response.admit(&intent, &reopened).unwrap();
    let mut reset = reopened.clone();
    reset.incarnation = "reset-two".into();
    assert!(response.admit(&intent, &reset).is_err());
}

#[test]
fn restored_bootstrap_state_cannot_claim_tail_before_manifest_coverage() {
    let invalid = json!({"manifestId":"manifest","materialization":"schema-one","initialCursor":100,"total":1,"covered":0,"tail":101});
    assert!(decode::<BootstrapCoverage>(invalid.to_string().as_bytes()).is_err());
}

#[test]
fn restored_evidence_cannot_protect_content_superseded_by_remove_or_another_context() {
    let mut evidence = RecordEvidence::default();
    evidence.install("schema-one", 57, false).unwrap();
    let mut removed = evidence.clone();
    removed.membership = Some(MembershipPosition {
        cursor: 58,
        live: false,
    });
    assert!(encode(&removed).is_err());
    evidence.history.insert("schema-two".into(), 58);
    assert!(encode(&evidence).is_err());
}

fn tracked_target() -> SettlementTarget {
    decode(json!({"kind":"stream","key":{"model":"Entry","identity":{"id":"x"}},"cursor":59,
        "fallback":{"model":"Entry","identity":{"id":"x"},"cursor":null,"state":{"text":"SERVER-B"}}}).to_string().as_bytes()).unwrap()
}

#[test]
fn tracked_receipt_before_or_after_compacted_remove_has_a_finite_owned_fallback() {
    let target = tracked_target();
    assert_eq!(target.owned_snapshot().state, json!({"text":"SERVER-B"}));
    for receipt_first in [true, false] {
        let mut evidence = RecordEvidence::default();
        evidence.install("schema-one", 57, false).unwrap();
        if receipt_first {
            assert_eq!(
                target.disposition("schema-one", &evidence).unwrap(),
                SettlementDisposition::AwaitStream
            );
        }
        evidence.remove(60).unwrap();
        assert!(!evidence.install("schema-one", 59, false).unwrap());
        assert_eq!(
            target.disposition("schema-one", &evidence).unwrap(),
            SettlementDisposition::FinalizeOwnedNull
        );
        assert!(target.ready("schema-one", &evidence));
        assert_eq!(evidence.history["schema-one"], 57);
        // A later direct write remains owned at its later log position;
        // the fallback is availability evidence, never an authority install.
        evidence.direct_write();
        assert_eq!(
            target.disposition("schema-one", &evidence).unwrap(),
            SettlementDisposition::FinalizeOwnedNull
        );
        assert_eq!(evidence.history["schema-one"], 57);
    }
}

#[test]
fn tracked_fallback_cannot_replace_retained_tombstone_or_later_retrack_authority() {
    let target = tracked_target();
    let mut deleted = RecordEvidence::default();
    deleted.install("schema-one", 57, true).unwrap();
    deleted.remove(60).unwrap();
    assert_eq!(
        target.disposition("schema-one", &deleted).unwrap(),
        SettlementDisposition::AcknowledgeProtected
    );
    assert!(!deleted.allows_cache());
    let mut live = RecordEvidence::default();
    live.install("schema-one", 57, false).unwrap();
    live.remove(60).unwrap();
    live.install("schema-one", 61, false).unwrap();
    assert_eq!(
        target.disposition("schema-one", &live).unwrap(),
        SettlementDisposition::InstalledStream
    );
    assert!(!live.remove(60).unwrap());
    assert_eq!(live.current.unwrap().cursor, 61);
}

#[test]
fn tracked_fallback_survives_retained_receipt_context_translation_without_claiming_g() {
    let intent = mutation();
    let target = tracked_target();
    let response = receipt(&intent, vec![target.clone()]);
    let mut active = context();
    active.materialization = "schema-two".into();
    response.admit(&intent, &active).unwrap();
    let mut evidence = RecordEvidence::default();
    evidence.install("schema-one", 57, false).unwrap();
    evidence.remove(60).unwrap();
    assert_eq!(
        target.disposition("schema-two", &evidence).unwrap(),
        SettlementDisposition::FinalizeOwnedNull
    );
    assert!(!evidence.history.contains_key("schema-two"));
    let mut wrong = json!({"kind":"stream","key":{"model":"Entry","identity":{"id":"x"}},"cursor":59,
        "fallback":{"model":"Entry","identity":{"id":"other"},"cursor":null,"state":{"text":"SERVER-B"}}});
    assert!(decode::<SettlementTarget>(wrong.to_string().as_bytes()).is_err());
    wrong["fallback"]["identity"] = json!({"id":"x"});
    wrong["fallback"]["cursor"] = json!(59);
    assert!(decode::<SettlementTarget>(wrong.to_string().as_bytes()).is_err());
}

#[test]
fn strict_v04_nested_keys_and_completions_reject_legacy_envelope_members() {
    let fixtures: serde_json::Value =
        serde_json::from_str(include_str!("../../../fixtures/protocol/0.4.json")).unwrap();
    for response_name in ["mutationReceipt", "readResponse"] {
        let mut value = fixtures[response_name].clone();
        value["completion"]["stamp"] = json!(57);
        let rejected = if response_name == "mutationReceipt" {
            decode::<MutationReceipt>(value.to_string().as_bytes()).is_err()
        } else {
            decode::<ReadResponse>(value.to_string().as_bytes()).is_err()
        };
        assert!(
            rejected,
            "accepted nested completion stamp in {response_name}"
        );
        let mut value = fixtures[response_name].clone();
        value["completion"]["outcome"]["cursor"] = json!(57);
        let rejected = if response_name == "mutationReceipt" {
            decode::<MutationReceipt>(value.to_string().as_bytes()).is_err()
        } else {
            decode::<ReadResponse>(value.to_string().as_bytes()).is_err()
        };
        assert!(
            rejected,
            "accepted nested outcome cursor in {response_name}"
        );
    }
    let mut page = fixtures["manifestPage"].clone();
    page["items"][0]["change"]["key"]["stamp"] = json!(57);
    assert!(decode::<ManifestPage>(page.to_string().as_bytes()).is_err());
    let target = json!({"kind":"stream","key":{"model":"Entry","identity":{"id":"x"},"stamp":57},"cursor":59,
        "fallback":{"model":"Entry","identity":{"id":"x"},"cursor":null,"state":{"text":"B"}}});
    assert!(decode::<SettlementTarget>(target.to_string().as_bytes()).is_err());
}

fn relation_schema(on_delete: &str) -> axton_core::Schema {
    axton_core::Schema::from_value(json!({"enums":[],"models":[
        {"name":"Parent","identity":["id"],"fields":[{"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"}}]},
        {"name":"Child","identity":["id"],"fields":[{"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"}},{"name":"parentId","nullable":true,"type":{"kind":"scalar","name":"string"}}],
         "relations":[{"name":"parent","target":"Parent","fields":["parentId"],"targetFields":["id"],"onDelete":on_delete}]}
    ]})).unwrap()
}

#[test]
fn cascading_reference_cache_guard_uses_only_a_current_parent_stream_tombstone() {
    let schema = relation_schema("delete");
    let child = ReadRecord {
        key: axton_core::RecordKey {
            model: "Child".into(),
            identity: json!({"id":"c"}),
        },
        cursor: NullCursor,
        state: json!({"parentId":"p"}),
    };
    let evidence = RecordEvidence::default();
    let mut parents = std::collections::BTreeMap::new();
    assert_eq!(
        child
            .disposition_with_relations(true, &evidence, &schema, &parents)
            .unwrap(),
        ReadDisposition::StoreCache
    ); // unloaded parent
    let parent_key = axton_core::RecordKey {
        model: "Parent".into(),
        identity: json!({"id":"p"}),
    }
    .encoded()
    .unwrap();
    let mut parent = RecordEvidence::default();
    parent.install("schema-one", 57, false).unwrap();
    parent.direct_write();
    parents.insert(parent_key.clone(), parent.clone());
    assert_eq!(
        child
            .disposition_with_relations(true, &evidence, &schema, &parents)
            .unwrap(),
        ReadDisposition::StoreCache
    ); // locally deleted
    parent.install("schema-one", 58, true).unwrap();
    parents.insert(parent_key.clone(), parent.clone());
    assert_eq!(
        child
            .disposition_with_relations(true, &evidence, &schema, &parents)
            .unwrap(),
        ReadDisposition::DeletedParent
    );
    assert_eq!(
        child
            .disposition_with_relations(false, &evidence, &schema, &parents)
            .unwrap(),
        ReadDisposition::SnapshotOnly
    );
    assert_eq!(
        child
            .disposition_with_relations(true, &evidence, &relation_schema("none"), &parents)
            .unwrap(),
        ReadDisposition::StoreCache
    );
    let mut unbound = child.clone();
    unbound.state["parentId"] = json!(null);
    assert_eq!(
        unbound
            .disposition_with_relations(true, &evidence, &schema, &parents)
            .unwrap(),
        ReadDisposition::StoreCache
    );
    parent.install("schema-one", 59, false).unwrap();
    parents.insert(parent_key, parent);
    assert_eq!(
        child
            .disposition_with_relations(true, &evidence, &schema, &parents)
            .unwrap(),
        ReadDisposition::StoreCache
    );
    assert!(evidence.history.is_empty()); // guard creates no child authority
}

#[test]
fn strict_v04_deserialization_keeps_application_values_opaque_and_legacy_codecs_unchanged() {
    let value = json!({"callId":"00000000-0000-4000-8000-000000000001","stamp":57,"outcome":{"status":"succeeded","result":{"stamp":99},"cursor":3}});
    assert!(serde_json::from_value::<axton_core::CallCompletion>(value).is_ok()); // unchanged 0.3 contract
    let intent = mutation();
    let mut value = serde_json::to_value(receipt(&intent, vec![])).unwrap();
    value["completion"]["outcome"]["result"] = json!({"stamp":57,"cursor":3,"scope":"application"});
    assert!(decode::<MutationReceipt>(value.to_string().as_bytes()).is_ok());
    let record = json!({"model":"Entry","identity":{"id":"x","stamp":1},"cursor":null,"state":{"cursor":2,"stamp":3,"scope":"application"}});
    assert!(decode::<ReadRecord>(record.to_string().as_bytes()).is_ok());
}

#[test]
fn carrier_requests_bind_one_stream_context_and_bounded_resume() {
    let context = serde_json::from_value::<v04::RequestContext>(json!({"protocol":4,"binding":{"backend":"api","viewer":"alice","stream":"User:alice","contract":"app"},"materialization":"m1","incarnation":"store1"})).unwrap();
    let ack = v04::SubscribeAcknowledged {
        context: context.clone(),
        cursor: 0,
        head: 7,
    };
    assert!(v04::encode(&ack).is_ok());
    let delta = v04::DeltaIntent {
        context: context.clone(),
        call_id: "01890f47-1234-7123-8123-123456789ab1".into(),
        after: 0,
        models: std::collections::BTreeMap::from([("Todo".into(), 1)]),
        limit: 100,
    };
    assert_eq!(
        v04::decode::<v04::DeltaIntent>(&v04::encode(&delta).unwrap()).unwrap(),
        delta
    );
    let start = v04::BootstrapIntent::Start {
        context,
        call_id: delta.call_id.clone(),
        models: delta.models.clone(),
        budget: 1000,
        held_keys: vec![],
    };
    assert!(v04::encode(&start).is_ok());
    let mut bad = serde_json::to_value(&delta).unwrap();
    bad["limit"] = json!(0);
    assert!(v04::decode::<v04::DeltaIntent>(&serde_json::to_vec(&bad).unwrap()).is_err());
    bad = serde_json::to_value(&delta).unwrap();
    bad["streams"] = json!(["Other"]);
    assert!(v04::decode::<v04::DeltaIntent>(&serde_json::to_vec(&bad).unwrap()).is_err());
}

#[test]
fn shared_materialization_id_ignores_carrier_metadata_and_binds_read_shape_and_projection() {
    let base = json!({"enums":[],"models":[{"name":"Todo","identity":["id"],"fields":[{"name":"id","type":{"kind":"scalar","name":"string"},"nullable":false},{"name":"text","type":{"kind":"scalar","name":"string"},"nullable":true}]}]});
    let schema: axton_core::Schema = serde_json::from_value(base.clone()).unwrap();
    let hash = v04::materialization_id(&schema, "1").unwrap();
    let mut decorated = base.clone();
    decorated["models"][0]["fields"][1]["createDefault"] = json!({"kind":"literal","value":"x"});
    decorated["requirements"] =
        json!([{ "model":"Todo","field":"text","name":"prepare","arguments":{} }]);
    assert_eq!(
        hash,
        v04::materialization_id(&serde_json::from_value(decorated).unwrap(), "1").unwrap()
    );
    assert_ne!(hash, v04::materialization_id(&schema, "2").unwrap());
    let mut marked = base;
    marked["models"][0]["bootstrap"] = json!(true);
    assert_ne!(
        hash,
        v04::materialization_id(&serde_json::from_value(marked).unwrap(), "1").unwrap()
    );
}

#[test]
fn bootstrap_manifest_binds_held_evidence_and_owned_receipt_targets() {
    let start = json!({"kind":"start","context":context(),"callId":"01890f47-1234-7123-8123-123456789ab1","models":{"Entry":1},"budget":10,"heldKeys":[key()],"receiptTargets":{"callId":"01890f47-1234-7123-8123-123456789ab2","keys":[{"model":"Entry","identity":{"id":"y"}}]}});
    let mut start = start;
    let targets = start
        .as_object_mut()
        .unwrap()
        .remove("receiptTargets")
        .unwrap();
    let materialize = json!({"kind":"materialize","context":context(),"callId":"01890f47-1234-7123-8123-123456789ab3","receiptTargets":targets,"budget":10});
    assert!(decode::<BootstrapIntent>(&serde_json::to_vec(&materialize).unwrap()).is_ok());
    let decoded = decode::<BootstrapIntent>(&serde_json::to_vec(&start).unwrap()).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), start);
    let mut malformed = start.clone();
    malformed["heldKeys"][0]["stamp"] = json!(9);
    assert!(decode::<BootstrapIntent>(&serde_json::to_vec(&malformed).unwrap()).is_err());
    malformed = materialize.clone();
    malformed["receiptTargets"]["keys"][0]["stamp"] = json!(9);
    assert!(decode::<BootstrapIntent>(&serde_json::to_vec(&malformed).unwrap()).is_err());
    malformed = start.clone();
    malformed["heldKeys"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::to_value(key()).unwrap());
    assert!(decode::<BootstrapIntent>(&serde_json::to_vec(&malformed).unwrap()).is_err());
}

#[test]
fn manifest_constraint_companions_add_authority_without_ordinal_coverage() {
    let page = json!({"context":context(),"manifestId":"m","total":2,"from":0,"to":1,"items":[{"ordinal":0,"change":{"kind":"remove","key":{"model":"Entry","identity":{"id":"x"}},"cursor":57}}],"companions":[{"kind":"remove","key":{"model":"Entry","identity":{"id":"y"}},"cursor":58}]});
    let decoded = decode::<ManifestPage>(&serde_json::to_vec(&page).unwrap()).unwrap();
    let mut coverage = BootstrapCoverage::new("m".into(), "schema-one".into(), 50, 2).unwrap();
    coverage.commit_page(&decoded, &context()).unwrap();
    assert_eq!(coverage.covered, 1);
    assert_eq!(coverage.tail, None);
    let mut duplicate = page.clone();
    duplicate["companions"][0]["key"]["identity"] = json!({"id":"x"});
    assert!(decode::<ManifestPage>(&serde_json::to_vec(&duplicate).unwrap()).is_err());
}
