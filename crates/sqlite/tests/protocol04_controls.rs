use axton_client::{Client, DownlinkAction, DownlinkEvent, DownlinkWorker, Schema, v04};
use axton_sqlite::SqliteStore;
use serde_json::Value;

#[test]
fn queued_restart_and_resume_are_not_stranded_after_stop_or_pause() {
    for pause in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let schema = Schema::from_value(
            serde_json::from_str::<Value>(include_str!("../../../fixtures/schemas/entry.json"))
                .unwrap(),
        )
        .unwrap();
        let mut client = Client::open_bound(
            SqliteStore::open_exclusive(directory.path().join("db")).unwrap(),
            schema,
            v04::StoreBinding {
                backend: "b".into(),
                viewer: "a".into(),
                stream: "User:a".into(),
                contract: "app".into(),
            },
        )
        .unwrap();
        let mut worker = DownlinkWorker::default();
        worker
            .handle(&mut client, DownlinkEvent::Start, 1, 7)
            .unwrap();
        assert!(
            worker
                .handle(&mut client, DownlinkEvent::Next, 1, 7)
                .unwrap()
                .iter()
                .any(|a| matches!(a, DownlinkAction::Request { .. }))
        );
        worker
            .handle(
                &mut client,
                if pause {
                    DownlinkEvent::Pause
                } else {
                    DownlinkEvent::Stop
                },
                1,
                7,
            )
            .unwrap();
        worker
            .handle(
                &mut client,
                if pause {
                    DownlinkEvent::Resume
                } else {
                    DownlinkEvent::Start
                },
                1,
                7,
            )
            .unwrap();
        let actions = worker
            .handle(&mut client, DownlinkEvent::Next, 1, 7)
            .unwrap();
        assert!(
            !actions.is_empty(),
            "queued resume/restart must keep the lane scheduled: {actions:?}"
        );
        assert!(
            worker
                .handle(&mut client, DownlinkEvent::Next, 1, 7)
                .unwrap()
                .iter()
                .any(|a| matches!(a, DownlinkAction::Request { .. }))
        );
    }
}
