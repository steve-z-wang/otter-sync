use axton_core::authority::*;

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

#[test]
fn restored_evidence_cannot_protect_content_superseded_by_remove_or_another_context() {
    let mut evidence = RecordEvidence::default();
    evidence.install("schema-one", 57, false).unwrap();
    let mut removed = evidence.clone();
    removed.membership = Some(MembershipPosition {
        cursor: 58,
        live: false,
    });
    assert!(removed.validate().is_err());
    evidence.history.insert("schema-two".into(), 58);
    assert!(evidence.validate().is_err());
}
