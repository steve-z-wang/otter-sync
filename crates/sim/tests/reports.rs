//! Pulls on the simulation: one request for every channel, and records that
//! cannot be read or applied fail alone and are reported (#95, #51, #122).
use axton_client::ReportKind;
use axton_sim::{Action, MutationSpec, Sim, schema::entry_key};

fn subscribe(sim: &mut Sim, client: usize, channels: &[&str]) {
    for c in channels {
        sim.apply(Action::Subscribe {
            client,
            channel: c.to_string(),
        })
        .unwrap();
    }
}
fn member(sim: &mut Sim, id: &str, channels: &[&str]) {
    sim.host.set_membership(&entry_key(id), channels);
}
fn change(sim: &mut Sim, id: &str, text: Option<&str>, channels: &[&str]) {
    sim.apply(Action::ServerChange {
        key: format!("Entry:{id}"),
        text: text.map(str::to_string),
        channels: channels.iter().map(|c| c.to_string()).collect(),
    })
    .unwrap();
}
fn pull(sim: &mut Sim, client: usize) {
    sim.apply(Action::Pull { client }).unwrap();
    sim.drain();
}

/// A record published to two channels the client follows arrives once, in one
/// pull, and moves both cursors.
#[test]
fn two_channels_sharing_a_record_arrive_in_one_pull() {
    let mut sim = Sim::new(31, 1);
    subscribe(&mut sim, 0, &["a", "b"]);
    member(&mut sim, "e1", &["a", "b"]);
    change(&mut sim, "e1", Some("hi"), &["a", "b"]);
    pull(&mut sim, 0);
    assert_eq!(sim.read_text(0, &entry_key("e1")).as_deref(), Some("hi"));
    assert_eq!(sim.client(0).cursor("a").unwrap(), Some(1));
    assert_eq!(sim.client(0).cursor("b").unwrap(), Some(1));
    assert_eq!(sim.client(0).record_stamp(&entry_key("e1")).unwrap(), 1);
    assert!(sim.reports.is_empty());
    sim.check().unwrap();
}

/// A loader failure for one record leaves that record's local content and
/// stamp alone, reports it, and lets the rest of the page apply; the next
/// publication corrects it.
#[test]
fn a_loader_failure_isolates_one_record() {
    let mut sim = Sim::new(32, 1);
    subscribe(&mut sim, 0, &["a"]);
    member(&mut sim, "e1", &["a"]);
    member(&mut sim, "e2", &["a"]);
    change(&mut sim, "e1", Some("v1"), &["a"]);
    pull(&mut sim, 0);
    assert_eq!(sim.read_text(0, &entry_key("e1")).as_deref(), Some("v1"));
    change(&mut sim, "e1", Some("v2"), &["a"]);
    change(&mut sim, "e2", Some("other"), &["a"]);
    sim.apply(Action::FailLoadNext {
        key: "Entry:e1".into(),
    })
    .unwrap();
    pull(&mut sim, 0);
    assert_eq!(
        sim.read_text(0, &entry_key("e1")).as_deref(),
        Some("v1"),
        "the unreadable record keeps its local content"
    );
    assert_eq!(sim.client(0).record_stamp(&entry_key("e1")).unwrap(), 1);
    assert_eq!(
        sim.read_text(0, &entry_key("e2")).as_deref(),
        Some("other"),
        "the rest of the page applies"
    );
    assert_eq!(
        sim.client(0).cursor("a").unwrap(),
        Some(sim.host.head("a")),
        "the cursor still advances"
    );
    let failed: Vec<_> = sim
        .reports
        .iter()
        .filter(|r| r.kind == ReportKind::ReadFailed)
        .collect();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].identity["id"], "e1");
    assert_eq!(failed[0].code.as_deref(), Some("loader.failed"));
    sim.check().unwrap();
    sim.settle();
    assert_eq!(sim.read_text(0, &entry_key("e1")).as_deref(), Some("v2"));
    assert!(sim.stale_reads.is_empty());
    sim.check().unwrap();
}

/// A loader refusal is reported with the loader's own code.
#[test]
fn a_loader_refusal_carries_its_code() {
    let mut sim = Sim::new(33, 1);
    subscribe(&mut sim, 0, &["a"]);
    member(&mut sim, "e1", &["a"]);
    change(&mut sim, "e1", Some("secret"), &["a"]);
    sim.apply(Action::RefuseLoadNext {
        key: "Entry:e1".into(),
    })
    .unwrap();
    pull(&mut sim, 0);
    assert_eq!(sim.read_text(0, &entry_key("e1")), None);
    assert_eq!(sim.client(0).record_stamp(&entry_key("e1")).unwrap(), 0);
    assert_eq!(sim.reports.len(), 1);
    assert_eq!(sim.reports[0].kind, ReportKind::ReadFailed);
    assert_eq!(sim.reports[0].code.as_deref(), Some("sim.refused"));
    sim.check().unwrap();
}

/// A delivered change the client's schema refuses is skipped alone and
/// reported; the next delivery corrects it.
#[test]
fn a_malformed_change_is_skipped_and_reported() {
    let mut sim = Sim::new(34, 1);
    subscribe(&mut sim, 0, &["a"]);
    member(&mut sim, "e1", &["a"]);
    member(&mut sim, "e2", &["a"]);
    change(&mut sim, "e1", Some("one"), &["a"]);
    change(&mut sim, "e2", Some("two"), &["a"]);
    sim.apply(Action::CorruptNextPage).unwrap();
    pull(&mut sim, 0);
    let skipped: Vec<_> = sim
        .reports
        .iter()
        .filter(|r| r.kind == ReportKind::Skipped)
        .collect();
    assert_eq!(skipped.len(), 1);
    let bad = skipped[0].identity["id"].as_str().unwrap().to_string();
    let good = if bad == "e1" { "e2" } else { "e1" };
    assert_eq!(sim.read_text(0, &entry_key(&bad)), None);
    assert!(sim.read_text(0, &entry_key(good)).is_some());
    assert_eq!(sim.client(0).cursor("a").unwrap(), Some(sim.host.head("a")));
    sim.check().unwrap();
    sim.settle();
    assert!(sim.read_text(0, &entry_key(&bad)).is_some());
    sim.check().unwrap();
}

/// A pending edit that no longer replays over newer authority is reported as
/// diverged, stays queued and is still sent; completion clears the mark.
#[test]
fn a_divergence_is_reported_and_cleared_by_completion() {
    let mut sim = Sim::new(35, 1);
    subscribe(&mut sim, 0, &["a"]);
    member(&mut sim, "e1", &["a"]);
    change(&mut sim, "e1", Some("base"), &["a"]);
    pull(&mut sim, 0);
    sim.apply(Action::Enqueue {
        client: 0,
        mutation: MutationSpec::Edit {
            id: "e1".into(),
            text: "mine".into(),
        },
    })
    .unwrap();
    assert_eq!(sim.read_text(0, &entry_key("e1")).as_deref(), Some("mine"));
    change(&mut sim, "e1", None, &["a"]);
    pull(&mut sim, 0);
    assert_eq!(
        sim.read_text(0, &entry_key("e1")),
        None,
        "the base is visible: the record is gone"
    );
    assert_eq!(
        sim.client(0).pending_count().unwrap(),
        1,
        "the edit is still queued"
    );
    let diverged: Vec<_> = sim
        .reports
        .iter()
        .filter(|r| r.kind == ReportKind::Diverged)
        .collect();
    assert_eq!(diverged.len(), 1);
    assert!(diverged[0].ordinal.is_some());
    let status = sim.client(0).record_status(&entry_key("e1")).unwrap();
    assert_eq!(status["pending"][0]["diverged"], true);
    sim.check().unwrap();
    sim.settle();
    assert_eq!(sim.client(0).pending_count().unwrap(), 0);
    let status = sim.client(0).record_status(&entry_key("e1")).unwrap();
    assert!(status["pending"].as_array().unwrap().is_empty());
    sim.check().unwrap();
}

/// Seed 206's minimized regression: a channel frame keeps both occurrences;
/// skipping malformed authority on one does not prevent the other from applying.
#[test]
fn skipped_occurrence_does_not_hide_successful_sibling_channel_authority() {
    let mut sim = Sim::new(206, 1);
    for channel in ["a", "b"] {
        sim.apply(Action::Subscribe {
            client: 0,
            channel: channel.into(),
        })
        .unwrap();
    }
    sim.apply(Action::ServerChange {
        key: "Entry:e1".into(),
        text: Some("valid sibling".into()),
        channels: vec!["a".into(), "b".into()],
    })
    .unwrap();
    sim.apply(Action::CorruptNextPage).unwrap();
    sim.apply(Action::Pull { client: 0 }).unwrap();
    sim.drain();
    assert_eq!(
        sim.read_text(0, &entry_key("e1")).as_deref(),
        Some("valid sibling")
    );
    assert!(
        sim.reports
            .iter()
            .any(|report| report.kind == ReportKind::Skipped)
    );
    sim.check().unwrap();
}
