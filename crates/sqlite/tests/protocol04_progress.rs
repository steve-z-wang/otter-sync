use axton_client::{Client, RecordKey, Schema, v04};
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};
fn schema(unique: bool) -> Schema {
    let mut s: Value =
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap();
    if unique {
        s["models"][0]["unique"] = json!([["text"]]);
    }
    Schema::from_value(s).unwrap()
}
fn open(p: &std::path::Path, unique: bool) -> Client<SqliteStore> {
    Client::open_bound(
        SqliteStore::open_exclusive(p).unwrap(),
        schema(unique),
        v04::StoreBinding {
            backend: "b".into(),
            viewer: "a".into(),
            stream: "User:a".into(),
            contract: "app".into(),
        },
    )
    .unwrap()
}
fn key(id: &str) -> RecordKey {
    RecordKey {
        model: "Entry".into(),
        identity: json!({"id":id}),
    }
}
fn change(id: &str, n: u64, text: &str) -> v04::StreamChange {
    v04::StreamChange::Upsert {
        record: v04::StreamRecord {
            key: key(id),
            cursor: n,
            state: json!({"text":text,"note":null}),
        },
    }
}
fn page(
    c: &Client<SqliteStore>,
    id: &str,
    from: u64,
    to: u64,
    head: u64,
    units: Vec<v04::CommitUnit>,
) -> v04::DeltaPage {
    v04::DeltaPage {
        context: c.request_context().unwrap().clone(),
        page_id: id.into(),
        from,
        to,
        head,
        units,
    }
}
#[test]
fn unique_transfer_is_final_projection_atomic_and_ahead_content_does_not_advance_prefix() {
    let d = tempfile::tempdir().unwrap();
    let mut c = open(&d.path().join("db"), true);
    let ctx = c.request_context().unwrap().clone();
    for (id, n, text) in [("x", 1, "alpha"), ("y", 2, "beta")] {
        if let v04::StreamChange::Upsert { record } = change(id, n, text) {
            c.install_stream04(&ctx, &record).unwrap();
        }
    }
    let p = page(
        &c,
        "swap",
        0,
        3,
        4,
        vec![v04::CommitUnit {
            through: 3,
            changes: vec![change("x", 3, "beta"), change("y", 4, "alpha")],
        }],
    );
    c.apply_delta04(&p).unwrap();
    assert_eq!(c.stream_cursor04().unwrap(), 3);
    assert_eq!(
        c.record_evidence04(&key("y"))
            .unwrap()
            .current
            .unwrap()
            .cursor,
        4
    );
    assert_eq!(c.read(&key("x")).unwrap().unwrap()["text"], "beta");
    assert_eq!(c.read(&key("y")).unwrap().unwrap()["text"], "alpha");
    c.apply_delta04(&p).unwrap();
    assert_eq!(c.stream_cursor04().unwrap(), 3);
    let bad = page(
        &c,
        "bad",
        3,
        6,
        6,
        vec![v04::CommitUnit {
            through: 6,
            changes: vec![change("x", 5, "same"), change("y", 6, "same")],
        }],
    );
    assert!(c.apply_delta04(&bad).is_err());
    assert_eq!(c.stream_cursor04().unwrap(), 3);
    assert_eq!(
        c.record_evidence04(&key("x"))
            .unwrap()
            .current
            .unwrap()
            .cursor,
        3
    );
    assert_eq!(c.read(&key("x")).unwrap().unwrap()["text"], "beta");
}
#[test]
fn failed_later_unit_retains_proven_prefix_and_exact_plan_after_real_reopen() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("db");
    let mut c = open(&path, false);
    let mut invalid = change("y", 2, "B");
    if let v04::StreamChange::Upsert { record } = &mut invalid {
        record.state = json!({"text":null,"note":null});
    }
    let p = page(
        &c,
        "partial",
        0,
        2,
        2,
        vec![
            v04::CommitUnit {
                through: 1,
                changes: vec![change("x", 1, "A")],
            },
            v04::CommitUnit {
                through: 2,
                changes: vec![invalid],
            },
        ],
    );
    assert!(c.apply_delta04(&p).is_err());
    assert_eq!(c.stream_cursor04().unwrap(), 1);
    assert_eq!(c.delta_progress04().unwrap().unwrap().next_unit, 1);
    assert!(c.read(&key("y")).unwrap().is_none());
    assert_eq!(
        c.record_evidence04(&key("y")).unwrap(),
        v04::RecordEvidence::default()
    );
    drop(c);
    let mut c = open(&path, false);
    assert_eq!(c.delta_progress04().unwrap().unwrap().next_unit, 1);
    assert_eq!(c.stream_cursor04().unwrap(), 1);
    assert!(c.apply_delta04(&p).is_err());
    let mut changed = p.clone();
    changed.units[1].changes = vec![change("y", 2, "fixed")];
    assert!(
        c.apply_delta04(&changed)
            .err()
            .unwrap()
            .to_string()
            .contains("page resume mismatch")
    );
    assert_eq!(c.stream_cursor04().unwrap(), 1);
}
#[test]
fn bootstrap_coverage_and_fixed_tail_survive_reopen_without_claiming_delta_progress() {
    let d = tempfile::tempdir().unwrap();
    let path = d.path().join("db");
    let mut c = open(&path, false);
    let ctx = c.request_context().unwrap().clone();
    c.start_bootstrap04(&v04::BootstrapStarted {
        context: ctx.clone(),
        manifest_id: "m".into(),
        start: 100,
        total: 2,
    })
    .unwrap();
    c.apply_manifest04(&v04::ManifestPage {
        companions: vec![],
        context: ctx.clone(),
        manifest_id: "m".into(),
        total: 2,
        from: 0,
        to: 1,
        items: vec![v04::ManifestItem {
            ordinal: 0,
            change: change("x", 150, "moved"),
        }],
    })
    .unwrap();
    let tail = v04::BootstrapTail {
        context: ctx.clone(),
        manifest_id: "m".into(),
        head: 150,
    };
    assert!(c.capture_bootstrap_tail04(&tail).is_err());
    assert_eq!(c.stream_cursor04().unwrap(), 100);
    c.apply_manifest04(&v04::ManifestPage {
        companions: vec![],
        context: ctx.clone(),
        manifest_id: "m".into(),
        total: 2,
        from: 1,
        to: 2,
        items: vec![v04::ManifestItem {
            ordinal: 1,
            change: v04::StreamChange::Remove {
                key: key("y"),
                cursor: 120,
            },
        }],
    })
    .unwrap();
    let p = page(
        &c,
        "before-tail",
        100,
        149,
        150,
        vec![v04::CommitUnit {
            through: 149,
            changes: vec![],
        }],
    );
    c.apply_delta04(&p).unwrap();
    c.capture_bootstrap_tail04(&tail).unwrap();
    assert_eq!(c.stream_cursor04().unwrap(), 149);
    assert!(!c.bootstrap_complete04().unwrap());
    drop(c);
    let mut c = open(&path, false);
    assert_eq!(c.bootstrap_coverage04().unwrap().unwrap().tail, Some(150));
    assert!(!c.bootstrap_complete04().unwrap());
    let mut changed = tail.clone();
    changed.head = 151;
    assert!(c.capture_bootstrap_tail04(&changed).is_err());
    let p = page(
        &c,
        "tail",
        149,
        150,
        200,
        vec![v04::CommitUnit {
            through: 150,
            changes: vec![],
        }],
    );
    c.apply_delta04(&p).unwrap();
    assert!(c.bootstrap_complete04().unwrap());
    assert_eq!(c.stream_cursor04().unwrap(), 150);
}
#[test]
fn manifest_companion_unique_transfer_is_atomic_but_does_not_cover_an_ordinal() {
    let d = tempfile::tempdir().unwrap();
    let mut c = open(&d.path().join("db"), true);
    let ctx = c.request_context().unwrap().clone();
    for (id, n, text) in [("x", 1, "alpha"), ("y", 2, "beta")] {
        if let v04::StreamChange::Upsert { record } = change(id, n, text) {
            c.install_stream04(&ctx, &record).unwrap();
        }
    }
    c.start_bootstrap04(&v04::BootstrapStarted {
        context: ctx.clone(),
        manifest_id: "m".into(),
        start: 2,
        total: 2,
    })
    .unwrap();
    c.apply_manifest04(&v04::ManifestPage {
        context: ctx.clone(),
        manifest_id: "m".into(),
        total: 2,
        from: 0,
        to: 1,
        items: vec![v04::ManifestItem {
            ordinal: 0,
            change: change("x", 3, "beta"),
        }],
        companions: vec![change("y", 4, "alpha")],
    })
    .unwrap();
    assert_eq!(c.read(&key("x")).unwrap().unwrap()["text"], "beta");
    assert_eq!(c.read(&key("y")).unwrap().unwrap()["text"], "alpha");
    assert!(
        c.capture_bootstrap_tail04(&v04::BootstrapTail {
            context: ctx,
            manifest_id: "m".into(),
            head: 4
        })
        .is_err()
    );
}
#[test]
fn targeted_materialization_before_and_during_public_bootstrap_never_advances_or_replaces_it() {
    let d = tempfile::tempdir().unwrap();
    let mut c = open(&d.path().join("db"), false);
    let ctx = c.request_context().unwrap().clone();
    let started = |id: &str, n, total| v04::BootstrapStarted {
        context: ctx.clone(),
        manifest_id: id.into(),
        start: n,
        total,
    };
    c.start_materialize04(&started("receipt1", 80, 1)).unwrap();
    assert_eq!(c.stream_cursor04().unwrap(), 0);
    assert!(c.bootstrap_coverage04().unwrap().is_none());
    c.start_bootstrap04(&started("public", 100, 2)).unwrap();
    c.start_materialize04(&started("receipt2", 120, 1)).unwrap();
    assert_eq!(c.stream_cursor04().unwrap(), 100);
    assert_eq!(
        c.bootstrap_coverage04().unwrap().unwrap().manifest_id,
        "public"
    );
    for id in ["receipt1", "receipt2"] {
        c.apply_manifest04(&v04::ManifestPage {
            context: ctx.clone(),
            manifest_id: id.into(),
            total: 1,
            from: 0,
            to: 1,
            items: vec![v04::ManifestItem {
                ordinal: 0,
                change: change(id, 57, id),
            }],
            companions: vec![],
        })
        .unwrap();
    }
    assert_eq!(c.bootstrap_coverage04().unwrap().unwrap().covered, 0);
    assert_eq!(c.stream_cursor04().unwrap(), 100);
}
#[test]
fn bootstrap_cross_transaction_unique_release_companion_closes_cached_conflict() {
    let d = tempfile::tempdir().unwrap();
    let mut c = open(&d.path().join("db"), true);
    let ctx = c.request_context().unwrap().clone();
    c.apply_cache04(
        &ctx,
        &[
            v04::ReadRecord {
                key: key("a"),
                cursor: v04::NullCursor,
                state: json!({"text":"x","note":null}),
            },
            v04::ReadRecord {
                key: key("b"),
                cursor: v04::NullCursor,
                state: json!({"text":"y","note":null}),
            },
        ],
        true,
    )
    .unwrap();
    c.start_bootstrap04(&v04::BootstrapStarted {
        context: ctx.clone(),
        manifest_id: "m".into(),
        start: 40,
        total: 2,
    })
    .unwrap();
    c.apply_manifest04(&v04::ManifestPage {
        context: ctx.clone(),
        manifest_id: "m".into(),
        total: 2,
        from: 0,
        to: 1,
        items: vec![v04::ManifestItem {
            ordinal: 0,
            change: change("b", 40, "x"),
        }],
        companions: vec![change("a", 30, "z")],
    })
    .unwrap();
    assert_eq!(c.read(&key("a")).unwrap().unwrap()["text"], "z");
    assert_eq!(c.read(&key("b")).unwrap().unwrap()["text"], "x");
    assert_eq!(c.stream_cursor04().unwrap(), 40);
    assert_eq!(c.bootstrap_coverage04().unwrap().unwrap().covered, 1);
    c.apply_manifest04(&v04::ManifestPage {
        context: ctx,
        manifest_id: "m".into(),
        total: 2,
        from: 1,
        to: 2,
        items: vec![v04::ManifestItem {
            ordinal: 1,
            change: change("a", 30, "z"),
        }],
        companions: vec![],
    })
    .unwrap();
    assert_eq!(c.bootstrap_coverage04().unwrap().unwrap().covered, 2);
}

#[test]
fn completed_old_manifest_is_not_current_completion_after_schema_rematerialization() {
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("db");
    let mut c = open(&p, false);
    let old = c.request_context().unwrap().clone();
    c.start_bootstrap04(&v04::BootstrapStarted {
        context: old.clone(),
        manifest_id: "old".into(),
        start: 100,
        total: 0,
    })
    .unwrap();
    c.capture_bootstrap_tail04(&v04::BootstrapTail {
        context: old.clone(),
        manifest_id: "old".into(),
        head: 100,
    })
    .unwrap();
    assert!(c.bootstrap_complete04().unwrap());
    drop(c);
    let mut raw = serde_json::to_value(schema(false)).unwrap();
    raw["models"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"label","nullable":true,"type":{"kind":"scalar","name":"string"}}));
    let mut c = Client::open_bound(
        SqliteStore::open_exclusive(&p).unwrap(),
        Schema::from_value(raw).unwrap(),
        old.binding,
    )
    .unwrap();
    assert!(!c.bootstrap_complete04().unwrap());
    assert!(c.bootstrap_coverage04().unwrap().is_none());
    assert_eq!(c.stream_cursor04().unwrap(), 100);
}
#[test]
fn receipt_and_public_manifest_purposes_cannot_alias_one_coverage_identity() {
    let d = tempfile::tempdir().unwrap();
    let mut c = open(&d.path().join("db"), false);
    let started = v04::BootstrapStarted {
        context: c.request_context().unwrap().clone(),
        manifest_id: "same".into(),
        start: 100,
        total: 0,
    };
    c.start_materialize04(&started).unwrap();
    assert!(c.start_bootstrap04(&started).is_err());
    assert!(c.bootstrap_coverage04().unwrap().is_none());
    assert_eq!(c.stream_cursor04().unwrap(), 0);
}
