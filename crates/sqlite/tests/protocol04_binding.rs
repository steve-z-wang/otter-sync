use axton_client::v04::StoreBinding;
use axton_client::{Client, ClientStore, Schema};
use axton_sqlite::SqliteStore;
use serde_json::json;

fn schema() -> Schema {
    Schema::from_value(
        serde_json::from_str(include_str!("../../../fixtures/schemas/entry.json")).unwrap(),
    )
    .unwrap()
}
fn binding(viewer: &str) -> StoreBinding {
    StoreBinding {
        backend: "backend-one".into(),
        viewer: viewer.into(),
        stream: format!("User:{viewer}"),
        contract: "application-one".into(),
    }
}
#[test]
fn bound_reopen_keeps_incarnation_and_rejects_rebinding_before_schema_changes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let client = Client::open_bound(
        SqliteStore::open_exclusive(&path).unwrap(),
        schema(),
        binding("alice"),
    )
    .unwrap();
    let context = client.request_context().unwrap().clone();
    drop(client);
    let mut changed = serde_json::to_value(schema()).unwrap();
    changed["models"][0]["fields"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"newColumn","nullable":true,"type":{"kind":"scalar","name":"string"}}));
    let changed = Schema::from_value(changed).unwrap();
    let failed = Client::open_bound(
        SqliteStore::open_exclusive(&path).unwrap(),
        changed,
        binding("bob"),
    );
    assert!(
        failed
            .err()
            .unwrap()
            .to_string()
            .contains("binding_mismatch")
    );
    let mut raw = SqliteStore::open_exclusive(&path).unwrap();
    assert!(
        !raw.query("PRAGMA table_info(Entry)", &[])
            .unwrap()
            .rows
            .iter()
            .any(|r| r[1] == "newColumn")
    );
    drop(raw);
    let reopened = Client::open_bound(
        SqliteStore::open_exclusive(&path).unwrap(),
        schema(),
        binding("alice"),
    )
    .unwrap();
    assert_eq!(reopened.request_context().unwrap(), &context);
}
#[test]
fn exclusive_store_rejects_another_open_of_the_same_physical_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let first = SqliteStore::open_exclusive(&path).unwrap();
    assert!(
        SqliteStore::open_exclusive(&path)
            .err()
            .unwrap()
            .to_string()
            .contains("store_in_use")
    );
    #[cfg(unix)]
    {
        let alias = dir.path().join("alias");
        std::os::unix::fs::symlink(&path, &alias).unwrap();
        assert!(
            SqliteStore::open_exclusive(&alias)
                .err()
                .unwrap()
                .to_string()
                .contains("store_in_use")
        );
        let hard = dir.path().join("hard");
        std::fs::hard_link(&path, &hard).unwrap();
        assert!(
            SqliteStore::open_exclusive(&hard)
                .err()
                .unwrap()
                .to_string()
                .contains("store_in_use")
        );
    }
    drop(first);
    SqliteStore::open_exclusive(&path).unwrap();
}
#[test]
fn exclusive_lock_is_held_across_processes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let _first = SqliteStore::open_exclusive(&path).unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "exclusive_lock_child",
            "--nocapture",
        ])
        .env("AXTON_LOCK_TEST_PATH", &path)
        .status()
        .unwrap();
    assert!(status.success());
}
/// Executed as a separate process by exclusive_lock_is_held_across_processes.
#[test]
#[ignore]
fn exclusive_lock_child() {
    let path = std::env::var_os("AXTON_LOCK_TEST_PATH").expect("parent supplies database path");
    assert!(
        SqliteStore::open_exclusive(path)
            .err()
            .unwrap()
            .to_string()
            .contains("store_in_use")
    );
}

#[test]
fn bound_store_cannot_add_remove_or_initialize_a_different_stream() {
    let dir = tempfile::tempdir().unwrap();
    let mut client = Client::open_bound(
        SqliteStore::open_exclusive(dir.path().join("db")).unwrap(),
        schema(),
        binding("alice"),
    )
    .unwrap();
    let subscription = client.subscription_state("User:alice").unwrap().unwrap();
    assert!(client.ensure_subscription("User:bob").is_err());
    assert!(
        client
            .remove_subscription("User:alice", subscription.subscription_id)
            .is_err()
    );
    assert!(
        client
            .transaction(|tx| tx.set_stream("User:bob".into(), true))
            .is_err()
    );
    assert!(
        client
            .transaction(|tx| tx.set_stream("User:alice".into(), false))
            .is_err()
    );
    assert!(
        client
            .initialize_subscriptions(
                &std::collections::BTreeMap::from([(
                    "User:alice".into(),
                    subscription.subscription_id
                )]),
                &std::collections::BTreeMap::from([("User:alice".into(), 99)])
            )
            .is_err()
    );
    assert_eq!(client.stream_cursor04().unwrap(), 0);
    assert_eq!(client.subscription_states().unwrap(), vec![subscription]);
}

#[cfg(unix)]
#[test]
fn exclusive_owner_close_releases_lock_while_preexec_child_retains_descriptors() {
    unsafe extern "C" {
        fn fork() -> i32;
        fn pipe(fds: *mut i32) -> i32;
        fn read(fd: i32, buf: *mut u8, count: usize) -> isize;
        fn write(fd: i32, buf: *const u8, count: usize) -> isize;
        fn close(fd: i32) -> i32;
        fn waitpid(pid: i32, status: *mut i32, options: i32) -> i32;
        fn _exit(status: i32) -> !;
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let first = SqliteStore::open_exclusive(&path).unwrap();
    let mut ready = [0; 2];
    let mut release = [0; 2];
    assert_eq!(unsafe { pipe(ready.as_mut_ptr()) }, 0);
    assert_eq!(unsafe { pipe(release.as_mut_ptr()) }, 0);
    let child = unsafe { fork() };
    assert!(child >= 0);
    if child == 0 {
        // The harness is multithreaded: no Rust allocation, SQLite, panic or
        // destructor is allowed after fork. Hold inherited descriptors until
        // the parent has closed its actual Store and attempted to reopen it.
        unsafe {
            close(ready[0]);
            close(release[1]);
            let byte = 1_u8;
            let sent = write(ready[1], &byte, 1);
            let mut command = 0_u8;
            let received = read(release[0], &mut command, 1);
            close(ready[1]);
            close(release[0]);
            _exit(if sent == 1 && received == 1 { 0 } else { 1 });
        }
    }
    unsafe {
        close(ready[1]);
        close(release[0]);
    }
    let mut byte = 0_u8;
    let child_ready = unsafe { read(ready[0], &mut byte, 1) };
    drop(first);
    let reopened = SqliteStore::open_exclusive(&path);

    // Always release/reap before asserting the reopen outcome, including the
    // red case. A test failure must not leave the inherited child lock alive.
    let released = unsafe { write(release[1], &byte, 1) };
    unsafe {
        close(ready[0]);
        close(release[1]);
    }
    let mut status = 0;
    let reaped = unsafe { waitpid(child, &mut status, 0) };
    assert_eq!(child_ready, 1);
    assert_eq!(released, 1);
    assert_eq!(reaped, child);
    assert_eq!(status, 0);
    let second = reopened.expect("closed owner must release ownership before child exec/exit");
    assert!(
        SqliteStore::open_exclusive(&path)
            .err()
            .unwrap()
            .to_string()
            .contains("store_in_use"),
        "explicit owner release must preserve exclusivity of the new Store"
    );
    drop(second);
    SqliteStore::open_exclusive(&path).unwrap();
}
