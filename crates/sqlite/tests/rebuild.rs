//! Schema compatibility at open ([#20]): identical and additive schemas open
//! the file in place; an incompatible schema, or an earlier framework layout,
//! gets a fresh file beside the old one, which is kept. Unsent work in the old
//! file is sent first or explicitly discarded with a report.
//!
//! [#20]: https://github.com/zanminwang/axton/issues/20
mod common;
use axton_client::*;
use axton_sqlite::SqliteStore;
use common::*;
use serde_json::{Value, json};
use std::path::{Path, PathBuf};

fn factory() -> StoreFactory<SqliteStore> {
    Box::new(|p| SqliteStore::open(p))
}
fn open_at(path: &Path, schema: Schema) -> Client<SqliteStore> {
    Client::open_at(path, schema, factory(), false).unwrap()
}
fn wider() -> Schema {
    let mut v = serde_json::to_value(schema()).unwrap();
    v["models"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"extra","nullable":true,"type":{"kind":"scalar","name":"string"}}));
    Schema::from_value(v).unwrap()
}
fn breaking() -> Schema {
    let mut v = serde_json::to_value(schema()).unwrap();
    v["models"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"due","nullable":false,"type":{"kind":"scalar","name":"string"}}));
    Schema::from_value(v).unwrap()
}
fn sidecar(path: &Path) -> Option<String> {
    std::fs::read_to_string(schema_store::sidecar_of(path))
        .ok()
        .map(|s| s.trim().to_string())
}
fn descriptor(c: &mut Client<SqliteStore>) -> String {
    c.read_sql("SELECT descriptor FROM axton_schema", &[])
        .unwrap()[0]["descriptor"]
        .as_str()
        .unwrap()
        .to_string()
}
fn numbered(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("db.") && n[3..].chars().all(|c| c.is_ascii_digit()))
        .collect();
    names.sort();
    names
}

#[test]
fn unchanged_and_additive_schemas_open_in_place() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open_at(&path, schema());
    seed(&mut c, "A");
    assert!(!c.schema_state().rebuilt);
    assert!(descriptor(&mut c).contains("\"Entry\""));
    drop(c);
    let mut c = open_at(&path, schema());
    assert!(!c.schema_state().rebuilt);
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "A");
    drop(c);
    let mut c = open_at(&path, wider());
    assert!(
        !c.schema_state().rebuilt,
        "a nullable field is added in place"
    );
    let row = c.read(&key()).unwrap().unwrap();
    assert_eq!(row["text"], "A");
    assert_eq!(row["extra"], Value::Null);
    assert!(
        descriptor(&mut c).contains("\"extra\""),
        "the stored descriptor follows"
    );
    assert_eq!(sidecar(&path), None, "no rebuild, no sidecar");
}

#[test]
fn an_incompatible_schema_gets_a_fresh_file_and_keeps_the_old_one() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open_at(&path, schema());
    subscribe(&mut c, "book");
    c.apply_page(page("book", 0, 1, Some("A"))).unwrap();
    assert_eq!(c.cursor("book").unwrap(), Some(1));
    drop(c);
    let mut c = open_at(&path, breaking());
    let state = c.schema_state().clone();
    assert!(state.rebuilt);
    let report = state.last_rebuild.unwrap();
    assert!(report.reason.contains("due"), "{}", report.reason);
    assert_eq!((report.left_pending, report.left_direct), (0, 0));
    assert_eq!(sidecar(&path).as_deref(), Some("db.1"));
    assert!(path.exists(), "the old file is kept");
    assert!(dir.path().join("db.1").exists());
    assert!(
        c.read(&key()).unwrap().is_none(),
        "the new file starts empty"
    );
    let carried = c.subscription_state("book").unwrap().expect("carried over");
    assert_eq!(
        (carried.starting_cursor, carried.cursor),
        (None, None),
        "the Scope carries over, its delivery boundary does not"
    );
    assert_eq!(
        carried.subscription_id, 2,
        "a fresh identity above the replaced replica's counter"
    );
    assert!(
        c.subscriptions().unwrap().is_empty(),
        "an uninitialized subscription asks for nothing"
    );
    // The old file still holds its row and its own schema.
    let mut old = Client::open(SqliteStore::open(&path).unwrap(), schema()).unwrap();
    assert_eq!(old.read(&key()).unwrap().unwrap()["text"], "A");
    drop(old);
    drop(c);
    // Reopening follows the sidecar and rebuilds nothing.
    let mut c = open_at(&path, breaking());
    assert!(!c.schema_state().rebuilt);
    assert_eq!(
        c.subscription_state("book").unwrap(),
        Some(carried),
        "the rebuilt file keeps the carried subscription"
    );
}

#[test]
fn an_earlier_framework_layout_is_rebuilt_beside_not_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut s = SqliteStore::open(&path).unwrap();
    s.execute_batch(ddl::FRAMEWORK_DDL).unwrap();
    s.execute_batch("CREATE TABLE axton_push_checkpoint (push INTEGER NOT NULL, scope TEXT NOT NULL, cursor INTEGER NOT NULL, PRIMARY KEY (push, scope)); INSERT INTO axton_push_checkpoint VALUES (1,'a',1); INSERT INTO axton_client (client_id, next_ordinal, next_push, generation, next_subscription) VALUES ('old',1,1,1,8); INSERT INTO axton_subscription (stream, subscription_id, starting_cursor, cursor) VALUES ('a', 7, 9, 9)").unwrap();
    drop(s);
    assert!(
        Client::open(SqliteStore::open(&path).unwrap(), schema()).is_err(),
        "a bare store still refuses the legacy layout"
    );
    let mut c = open_at(&path, schema());
    assert!(c.schema_state().rebuilt);
    assert!(
        c.schema_state()
            .last_rebuild
            .as_ref()
            .unwrap()
            .reason
            .contains("axton_push_checkpoint")
    );
    let carried = c.subscription_state("a").unwrap().expect("carried over");
    assert_eq!((carried.starting_cursor, carried.cursor), (None, None));
    assert_eq!(
        carried.subscription_id, 8,
        "the allocator carries forward, so no identity is reissued"
    );
    assert_ne!(c.client_id(), "old", "a fresh client identity");
    let mut s = SqliteStore::open(&path).unwrap();
    let rows = s
        .query_committed("SELECT COUNT(*) AS n FROM axton_push_checkpoint", &[])
        .unwrap();
    assert_eq!(rows.rows[0][0], json!(1), "the old file is untouched");
}

/// The framework layout this issue replaced: `axton_subscription` without a
/// subscription identity and an `axton_client` without the allocator
/// ([#150](https://github.com/zanminwang/axton/issues/150)). It cannot be opened
/// in place, and the rebuild carries the Scope names with no delivery boundary.
#[test]
fn a_subscription_table_without_identities_is_rebuilt_beside() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut s = SqliteStore::open(&path).unwrap();
    s.execute_batch(
        "CREATE TABLE axton_client (client_id TEXT PRIMARY KEY, next_ordinal INTEGER NOT NULL, next_push INTEGER NOT NULL, generation INTEGER NOT NULL, last_completed_push INTEGER NOT NULL DEFAULT 0, push_models TEXT, push_results TEXT);
         CREATE TABLE axton_subscription (scope TEXT PRIMARY KEY, cursor INTEGER NOT NULL);
         INSERT INTO axton_client (client_id, next_ordinal, next_push, generation) VALUES ('old', 1, 1, 1);
         INSERT INTO axton_subscription VALUES ('a', 9);
         INSERT INTO axton_subscription VALUES ('b', 4);",
    )
    .unwrap();
    drop(s);
    let mut s = SqliteStore::open(&path).unwrap();
    assert!(
        matches!(ddl::check_layout(&mut s).unwrap(), ddl::Layout::Legacy(reason) if reason.contains("next_subscription")),
        "an allocator-less client row is an earlier layout"
    );
    drop(s);
    assert!(
        Client::open(SqliteStore::open(&path).unwrap(), schema()).is_err(),
        "a bare store refuses it rather than converting it"
    );

    let mut c = open_at(&path, schema());
    assert!(c.schema_state().rebuilt);
    assert!(
        c.schema_state()
            .last_rebuild
            .as_ref()
            .unwrap()
            .reason
            .contains("next_subscription")
    );
    let mut identities = vec![];
    for scope in ["a", "b"] {
        let carried = c.subscription_state(scope).unwrap().expect("carried over");
        assert_eq!(
            (carried.starting_cursor, carried.cursor),
            (None, None),
            "the Scope carries over, the old cursor does not"
        );
        identities.push(carried.subscription_id);
    }
    assert_eq!(
        identities,
        vec![1, 2],
        "the replaced layout allocated no identity, so this replica starts at one"
    );
    assert!(c.subscriptions().unwrap().is_empty());
    drop(c);

    let mut s = SqliteStore::open(&path).unwrap();
    assert_eq!(
        s.query_committed("SELECT cursor FROM axton_subscription ORDER BY scope", &[])
            .unwrap()
            .rows
            .iter()
            .map(|r| r[0].clone())
            .collect::<Vec<_>>(),
        vec![json!(9), json!(4)],
        "the old file keeps its own cursors"
    );
}

#[test]
fn an_abandoned_partial_rebuild_is_removed_and_retried() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    drop(open_at(&path, schema()));
    // A crash left `db.1` behind without moving the sidecar.
    drop(SqliteStore::open(dir.path().join("db.1")).unwrap());
    std::fs::write(dir.path().join("db.1"), b"garbage").unwrap();
    assert_eq!(sidecar(&path), None);
    let mut c = open_at(&path, breaking());
    assert!(c.schema_state().rebuilt);
    assert_eq!(
        numbered(dir.path()),
        vec!["db.1".to_string()],
        "the stray was replaced"
    );
    assert_eq!(sidecar(&path).as_deref(), Some("db.1"));
    assert!(c.read(&key()).unwrap().is_none());
}

#[test]
fn unsent_work_keeps_the_old_file_open_until_it_is_sent_then_rebuild_switches() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open_at(&path, schema());
    seed(&mut c, "A");
    c.transaction(|tx| tx.enqueue(mutation("B")).map(|_| ()))
        .unwrap();
    let frozen = c.freeze().unwrap().unwrap();
    drop(c);
    let mut c = open_at(&path, breaking());
    assert!(!c.schema_state().rebuilt);
    let pending = c.schema_state().pending.clone().unwrap();
    assert_eq!(pending.pending, 1);
    assert_eq!(
        pending.direct, 0,
        "the seeded row is covered by its pending mutation"
    );
    assert!(pending.reason.contains("due"));
    assert_eq!(sidecar(&path), None);
    assert_eq!(
        c.freeze().unwrap().unwrap(),
        frozen,
        "the old file's frozen bytes are sent unchanged"
    );
    assert_eq!(
        c.read(&key()).unwrap().unwrap()["text"],
        "B",
        "reads use the old schema"
    );
    let err = c.rebuild(false).unwrap_err();
    assert!(err.to_string().contains("unsent"), "{err}");
    let r = receipt(&mut c, 1, vec![authority(Some("B"), 2)]);
    c.acknowledge(1, r).unwrap();
    assert_eq!(c.pending_count().unwrap(), 0);
    let report = c.rebuild(false).unwrap();
    assert_eq!(report.left_pending, 0);
    assert_eq!(report.left_direct, 0, "the row now has a stamp");
    assert!(c.schema_state().rebuilt);
    assert!(c.schema_state().pending.is_none());
    assert_eq!(sidecar(&path).as_deref(), Some("db.1"));
    assert!(
        c.read(&key()).unwrap().is_none(),
        "the new file is empty until it syncs"
    );
    // The client now speaks the new schema: a row with the new required field lands.
    c.transaction(|tx| {
        tx.direct(Operation {
            model: "Entry".into(),
            op: OperationKind::Create,
            identity: json!({"id":"e"}),
            values: Some(json!({"text":"new","note":null,"due":"today"})),
        })
    })
    .unwrap();
    assert_eq!(c.read(&key()).unwrap().unwrap()["due"], "today");
}

#[test]
fn discarding_pending_work_reports_what_the_old_file_keeps() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open_at(&path, schema());
    seed(&mut c, "A");
    c.transaction(|tx| {
        tx.direct(Operation {
            model: "Entry".into(),
            op: OperationKind::Create,
            identity: json!({"id":"local"}),
            values: Some(json!({"text":"never sent","note":null})),
        })?;
        tx.enqueue(mutation("B")).map(|_| ())
    })
    .unwrap();
    drop(c);
    let mut c = open_at(&path, breaking());
    assert!(c.schema_state().pending.is_some());
    let report = c.rebuild(true).unwrap();
    assert_eq!(report.left_pending, 1);
    assert_eq!(
        report.left_direct, 1,
        "the row only a direct write created stays behind; the seeded row belongs to its pending mutation"
    );
    assert!(report.old_file.ends_with("db"));
    assert!(report.new_file.ends_with("db.1"));
    assert!(path.exists());
    assert!(c.schema_state().pending.is_none());
    drop(c);
    // Opening with `discard_pending` from the start rebuilds at once.
    let path2: PathBuf = dir.path().join("other");
    let mut c = open_at(&path2, schema());
    seed(&mut c, "A");
    c.transaction(|tx| tx.enqueue(mutation("B")).map(|_| ()))
        .unwrap();
    drop(c);
    let c = Client::open_at(&path2, breaking(), factory(), true).unwrap();
    assert!(c.schema_state().rebuilt);
    assert_eq!(
        c.schema_state().last_rebuild.as_ref().unwrap().left_pending,
        1
    );
}

#[test]
fn a_current_layout_file_without_a_descriptor_adopts_the_schema_it_opens_with() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open_at(&path, schema());
    seed(&mut c, "A");
    c.read_sql("DELETE FROM axton_schema", &[]).ok();
    let mut s = SqliteStore::open(&path).unwrap();
    s.execute("DELETE FROM axton_schema", &[]).unwrap();
    drop(s);
    drop(c);
    let mut c = open_at(&path, wider());
    assert!(
        !c.schema_state().rebuilt,
        "reconciliation succeeded, so the file is adopted"
    );
    assert!(descriptor(&mut c).contains("\"extra\""));
    assert_eq!(c.read(&key()).unwrap().unwrap()["text"], "A");
    drop(c);
    let mut s = SqliteStore::open(&path).unwrap();
    s.execute("DELETE FROM axton_schema", &[]).unwrap();
    drop(s);
    let c = open_at(&path, breaking());
    assert!(
        c.schema_state().rebuilt,
        "reconciliation failed, so the file is rebuilt beside"
    );
}

fn breaking_again() -> Schema {
    let mut v = serde_json::to_value(breaking()).unwrap();
    v["models"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"owner","nullable":false,"type":{"kind":"scalar","name":"string"}}));
    Schema::from_value(v).unwrap()
}

/// Every earlier generation survives later rebuilds: only a numbered file
/// above the one in use (never pointed at) is removed, and numbers only grow.
#[test]
fn earlier_generations_survive_later_rebuilds() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    drop(open_at(&path, schema()));
    drop(open_at(&path, breaking()));
    assert_eq!(sidecar(&path).as_deref(), Some("db.1"));
    drop(open_at(&path, breaking_again()));
    assert_eq!(sidecar(&path).as_deref(), Some("db.2"));
    assert!(path.exists(), "the first generation is kept");
    assert_eq!(
        numbered(dir.path()),
        vec!["db.1".to_string(), "db.2".to_string()]
    );
    // The application deletes the oldest numbered generation; numbering still grows.
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(dir.path().join(format!("db.1{suffix}")));
    }
    drop(open_at(&path, schema()));
    assert_eq!(sidecar(&path).as_deref(), Some("db.3"));
    assert_eq!(
        numbered(dir.path()),
        vec!["db.2".to_string(), "db.3".to_string()]
    );
}

/// A database from before descriptors were stored opens in place when its
/// tables fit, and records the descriptor; it is rebuilt only when they do not.
#[test]
fn a_database_without_a_descriptor_is_rebuilt_only_when_its_tables_do_not_fit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    drop(open_at(&path, schema()));
    {
        let mut store = SqliteStore::open(&path).unwrap();
        store.execute("DELETE FROM axton_schema", &[]).unwrap();
    }
    let mut c = open_at(&path, wider());
    assert!(!c.schema_state().rebuilt, "compatible tables open in place");
    assert!(sidecar(&path).is_none());
    assert!(descriptor(&mut c).contains("extra"));
    drop(c);
    {
        let mut store = SqliteStore::open(&path).unwrap();
        store.execute("DELETE FROM axton_schema", &[]).unwrap();
    }
    let c = open_at(&path, breaking());
    assert!(
        c.schema_state().rebuilt,
        "a missing required column rebuilds"
    );
    assert_eq!(sidecar(&path).as_deref(), Some("db.1"));
}

/// A rebuild resets loading coverage with the delivery boundary: the carried
/// Scope's fresh identity holds a load that was never requested, so no
/// completeness is claimed across the replacement
/// ([#151](https://github.com/zanminwang/axton/issues/151)).
#[test]
fn a_rebuild_resets_the_bootstrap_state_with_the_fresh_identity() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open_at(&path, schema());
    let old = {
        subscribe(&mut c, "book");
        let id = c
            .subscription_state("book")
            .unwrap()
            .unwrap()
            .subscription_id;
        c.request_bootstrap("book", id).unwrap();
        let page = BootstrapPage {
            stream: "book".into(),
            from: 0,
            to: 0,
            until: 0,
            head: 0,
            records: vec![],
        };
        let outcome = c.apply_bootstrap_page("book", id, 1, 0, &page).unwrap();
        assert_eq!(
            outcome.state().map(|s| s.state),
            Some(BootstrapPhase::Complete)
        );
        id
    };
    drop(c);

    let mut c = open_at(&path, breaking());
    assert!(c.schema_state().rebuilt);
    let carried = c.subscription_state("book").unwrap().expect("carried over");
    assert_ne!(carried.subscription_id, old, "a fresh identity");
    assert!(
        c.bootstrap_state("book", old).is_err(),
        "the completed run belonged to the replaced identity"
    );
    let state = c.bootstrap_state("book", carried.subscription_id).unwrap();
    assert_eq!(state.state, BootstrapPhase::NotRequested);
    assert_eq!((state.run, state.cursor, state.barrier), (0, 0, None));
    assert_eq!(state.error, None);
    assert!(c.bootstrap_tasks().unwrap().is_empty());
}

/// The Load schema with a required field added: incompatible, so it needs a
/// fresh replica ([#173](https://github.com/zanminwang/axton/issues/173)).
fn load_breaking() -> Schema {
    let mut v = load_schema_value();
    v["models"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"due","nullable":false,"type":{"kind":"scalar","name":"string"}}));
    Schema::from_value(v).unwrap()
}
fn load_args() -> Value {
    json!({"projectId":"0190f0e0-1111-7222-8333-444455556666","since":null})
}
fn load_fence(c: &mut Client<SqliteStore>, id: &str) -> LoadFence {
    let job = c.get_load(id).unwrap().unwrap();
    LoadFence {
        replica: c.replica_generation(),
        load_id: job.id,
        run: job.run,
        call_id: job.call_id.unwrap(),
    }
}
fn refused_pending<T: std::fmt::Debug>(result: Result<T>) {
    let error = result.unwrap_err().to_string();
    assert!(error.starts_with("load.schema_pending"), "{error}");
}

#[test]
fn a_pending_rebuild_parks_loads_and_the_rebuild_abandons_them() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open_at(&path, load_schema());
    let active = c
        .start_load("Entries", 1, &load_args(), LoadOptions::default())
        .unwrap()
        .job;
    let once = LoadOptions {
        once: true,
        refresh: false,
    };
    let mapped = c.start_load("Entries", 1, &load_args(), once).unwrap().job;
    let finished = c.start_load("Recent", 1, &json!({}), once).unwrap().job;
    let frozen = load_fence(&mut c, &finished.id);
    c.store_load_page(&frozen, reply(load_page(&frozen, &[], None)))
        .unwrap();
    let failed = c
        .start_load("Entries", 2, &load_args(), LoadOptions::default())
        .unwrap()
        .job;
    let frozen = load_fence(&mut c, &failed.id);
    c.record_load_failure(
        &frozen,
        &LoadFailure::Backend(LoadError {
            code: "handler.failed".into(),
            message: "no".into(),
        }),
    )
    .unwrap();
    c.transaction(|tx| {
        tx.enqueue(Mutation::new(
            "Edit",
            vec![create("Entry", "q", json!({"text":"queued","note":null}))],
        ))
        .map(|_| ())
    })
    .unwrap();
    let frozen_push = c.freeze().unwrap().unwrap();
    let in_flight = load_fence(&mut c, &active.id);
    drop(c);

    let mut c = open_at(&path, load_breaking());
    assert!(c.schema_state().pending.is_some());
    let generation = c.generation();
    // No start, retry or invalidation persists anything or runs.
    refused_pending(c.start_load("Entries", 1, &load_args(), LoadOptions::default()));
    refused_pending(c.start_load("Entries", 1, &load_args(), once));
    refused_pending(c.start_load(
        "Entries",
        1,
        &load_args(),
        LoadOptions {
            once: true,
            refresh: true,
        },
    ));
    refused_pending(c.retry_load(&failed.id));
    refused_pending(c.invalidate_load("Entries", &load_args()));
    assert_eq!(c.generation(), generation, "nothing was committed");
    assert_eq!(table_count(&mut c, "axton_load"), 4);
    assert_eq!(table_count(&mut c, "axton_load_once"), 2);
    // Existing jobs neither dispatch nor apply; answers in flight are inert.
    assert!(
        c.load_ready_pages(8, &Default::default())
            .unwrap()
            .pages
            .is_empty()
    );
    let parked = load_fence(&mut c, &active.id);
    assert_eq!(parked.call_id, in_flight.call_id);
    let late = c
        .store_load_page(&parked, reply(load_page(&parked, &[("a", "A", 1)], None)))
        .unwrap();
    assert!(matches!(late, LoadStored::Stale));
    assert_eq!(
        c.record_load_failure(&parked, &LoadFailure::transport("x"))
            .unwrap(),
        None
    );
    assert_eq!(c.get_load(&active.id).unwrap().unwrap().pages, 0);
    // get and list read the old ledger; cancel and forget still work.
    assert_eq!(c.list_loads(10).unwrap().len(), 4);
    assert_eq!(
        c.cancel_load(&mapped.id).unwrap().phase,
        LoadPhase::Cancelled
    );
    c.forget_load(&mapped.id).unwrap();
    assert!(c.get_load(&mapped.id).unwrap().is_none());
    // The Mutation drain is unchanged.
    assert_eq!(c.freeze().unwrap().unwrap(), frozen_push);
    let r = receipt(&mut c, 1, vec![authority_of("q", Some("queued"), 1)]);
    c.acknowledge(1, r).unwrap();
    assert_eq!(c.pending_count().unwrap(), 0);

    let report = c.rebuild(false).unwrap();
    assert_eq!(
        report.abandoned_loads,
        vec![active.id.clone(), finished.id.clone(), failed.id.clone()],
        "every job left in the old file, oldest first"
    );
    assert!(c.replica_generation() > parked.replica);
    assert!(
        c.get_load(&active.id).unwrap().is_none(),
        "no old job reaches the new replica"
    );
    assert_eq!(table_count(&mut c, "axton_load"), 0);
    assert_eq!(table_count(&mut c, "axton_load_once"), 0);
    let late = c
        .store_load_page(&parked, reply(load_page(&parked, &[("a", "A", 1)], None)))
        .unwrap();
    assert!(
        matches!(late, LoadStored::Stale),
        "an old replica's answer is inert"
    );
    let fresh = c.start_load("Recent", 1, &json!({}), once).unwrap();
    assert_eq!(
        fresh.kind,
        LoadStartKind::Created,
        "no old completion satisfies once"
    );
    assert_ne!(fresh.job.id, finished.id);
}

#[test]
fn an_incompatible_open_without_unsent_work_abandons_every_load() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut c = open_at(&path, load_schema());
    let first = c
        .start_load("Entries", 1, &load_args(), LoadOptions::default())
        .unwrap()
        .job
        .id;
    let second = c
        .start_load(
            "Recent",
            1,
            &json!({}),
            LoadOptions {
                once: true,
                refresh: false,
            },
        )
        .unwrap()
        .job
        .id;
    drop(c);
    let mut c = open_at(&path, load_breaking());
    let report = c.schema_state().last_rebuild.clone().unwrap();
    assert_eq!(report.abandoned_loads, vec![first.clone(), second]);
    assert!(c.get_load(&first).unwrap().is_none());
    assert_eq!(table_count(&mut c, "axton_load_once"), 0);
    // A file from before the Load ledger reports none.
    drop(c);
    let other = dir.path().join("plain");
    let mut c = open_at(&other, schema());
    seed(&mut c, "A");
    drop(c);
    SqliteStore::open(&other)
        .unwrap()
        .execute_batch("DROP TABLE axton_load; DROP TABLE axton_load_once")
        .unwrap();
    let c = open_at(&other, breaking());
    assert!(
        c.schema_state()
            .last_rebuild
            .clone()
            .unwrap()
            .abandoned_loads
            .is_empty()
    );
}
