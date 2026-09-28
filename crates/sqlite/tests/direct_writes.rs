//! Direct writes to a Model that Channels also deliver (L4,
//! [#188](https://github.com/zanminwang/axton/issues/188)). An application
//! may store canonical data it received over another transport, such as a
//! REST lookup, with a plain local write. This file pins what then happens,
//! once per canonical source: a Channel page, a native Load page and a Fetch.
//!
//! - The direct write is device-only: it is never queued or sent.
//! - Newer canonical data for the same identity replaces it, and nothing
//!   replays it afterwards.
//! - Refusing a pending Mutation that holds the same row removes only that
//!   Mutation's optimism; the direct write stays until canonical data
//!   replaces it.
//!
//! A redelivery at the stamp the device already holds is not newer canonical
//! data; the last test pins its current outcome.
mod common;
use axton_client::loads::STORE_FAILED;
use axton_client::*;
use axton_sqlite::SqliteStore;
use common::*;
use serde_json::{Value, json};
use std::path::PathBuf;

#[derive(Clone, Copy, Debug)]
enum Source {
    Channel,
    Load,
    Fetch,
}
const SOURCES: [Source; 3] = [Source::Channel, Source::Load, Source::Fetch];

/// How a direct write reaches `Entry e`.
#[derive(Clone, Copy, Debug)]
enum Direct {
    /// A pending edit, then the direct write.
    After,
    /// The direct write, then a pending edit.
    Before,
    /// A pending edit whose `local` companion edits the same field, then the
    /// direct write.
    AfterCompanion,
}
const DIRECTS: [Direct; 3] = [Direct::After, Direct::Before, Direct::AfterCompanion];

/// One device holding `Entry e`, subscribed to channel `ch`.
struct Device {
    _dir: tempfile::TempDir,
    path: PathBuf,
    c: Client<SqliteStore>,
    cursor: u64,
}

impl Device {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut c = Client::open(SqliteStore::open(&path).unwrap(), load_schema()).unwrap();
        subscribe(&mut c, "ch");
        Self {
            _dir: dir,
            path,
            c,
            cursor: 0,
        }
    }
    fn key() -> RecordKey {
        load_schema()
            .record_key("Entry", &json!({"id":"e"}))
            .unwrap()
    }
    fn text(&mut self) -> Option<String> {
        self.c
            .read(&Self::key())
            .unwrap()
            .map(|row| row["text"].as_str().unwrap().to_owned())
    }
    fn stamp(&mut self) -> u64 {
        self.c.record_stamp(&Self::key()).unwrap()
    }
    fn direct(&mut self, op: Operation) {
        self.c.transaction(|tx| tx.direct(op)).unwrap();
    }
    fn direct_text(&mut self, text: &str) {
        self.direct(update(text));
    }
    fn enqueue(&mut self, m: Mutation) {
        self.c.transaction(|tx| tx.enqueue(m)).unwrap();
    }
    /// Freeze the next batch and refuse every Mutation in it.
    fn refuse_next(&mut self) {
        let batch = PushRequest::decode(&self.c.freeze().unwrap().unwrap()).unwrap();
        let ordinals: Vec<u64> = batch.mutations.iter().map(|m| m.ordinal).collect();
        let r = rejecting(
            &mut self.c,
            batch.batch_sequence,
            &ordinals,
            "denied",
            vec![],
        );
        self.c.acknowledge(batch.batch_sequence, r).unwrap();
    }
    fn reopen(&mut self) {
        let c = Client::open(SqliteStore::open(&self.path).unwrap(), load_schema()).unwrap();
        self.c = c;
    }
    /// Nothing is queued and there is nothing to send.
    fn assert_nothing_queued(&mut self, context: &str) {
        assert_eq!(
            self.c.pending_count().unwrap(),
            0,
            "{context}: nothing pending"
        );
        assert!(
            self.c.freeze().unwrap().is_none(),
            "{context}: nothing to send"
        );
    }
    /// No base and no local write is retained that could replay later.
    fn assert_nothing_retained(&mut self, context: &str) {
        assert_eq!(
            self.c.before_image_count().unwrap(),
            0,
            "{context}: no base is retained"
        );
        assert_eq!(
            table_count(&mut self.c, "axton_local_write"),
            0,
            "{context}: no local write is retained"
        );
    }
    /// Deliver `Entry e` as `text` at `stamp` through `source` and return how
    /// the source answered, without judging it.
    fn try_deliver(&mut self, source: Source, text: &str, stamp: u64) -> Delivered {
        match source {
            Source::Channel => {
                let from = self.cursor;
                self.cursor += 1;
                let page = multi(
                    &[("ch", from, self.cursor, self.cursor)],
                    vec![authority(Some(text), stamp)],
                );
                Delivered::Channel(self.c.apply_page(page).unwrap())
            }
            Source::Load => {
                let job = self
                    .c
                    .start_load("Entries", 1, &load_args(), LoadOptions::default())
                    .unwrap()
                    .job;
                let fence = LoadFence {
                    replica: self.c.replica_generation(),
                    load_id: job.id,
                    run: job.run,
                    call_id: job.call_id.expect("a frozen page"),
                };
                let page = load_page(&fence, &[("e", text, stamp)], None);
                Delivered::Load(Box::new(
                    self.c.store_load_page(&fence, reply(page)).unwrap(),
                ))
            }
            Source::Fetch => {
                let response = FetchResponse {
                    completion: CallCompletion {
                        call_id: "123e4567-e89b-42d3-a456-426614174000".into(),
                        outcome: ActionOutcome::Succeeded {
                            result: json!({"id":"e","text":text,"note":null}),
                        },
                    },
                    records: vec![authority(Some(text), stamp)],
                };
                Delivered::Fetch(self.c.apply_fetch_response(&response))
            }
        }
    }
    /// Deliver canonical data that must be stored.
    fn deliver(&mut self, source: Source, text: &str, stamp: u64) {
        match self.try_deliver(source, text, stamp) {
            Delivered::Channel(report) => {
                assert_eq!(
                    (report.applied, report.conflicts()),
                    (1, 0),
                    "{source:?}: the page is applied"
                );
            }
            Delivered::Load(stored) => {
                let LoadStored::Applied { job, report } = *stored else {
                    panic!("{source:?}: expected an applied page, got {stored:?}")
                };
                assert_eq!(job.phase, LoadPhase::Complete, "{source:?}");
                assert_eq!(job.error, None, "{source:?}");
                assert_eq!(report.applied, 1, "{source:?}: the page is applied");
            }
            Delivered::Fetch(result) => {
                assert_eq!(
                    result.expect("the Fetch is stored").applied,
                    1,
                    "{source:?}: the record is stored"
                );
            }
        }
    }
}

enum Delivered {
    Channel(ApplyReport),
    Load(Box<LoadStored>),
    Fetch(Result<ApplyReport>),
}

fn load_args() -> Value {
    json!({"projectId": "0190f0e0-1111-7222-8333-444455556666", "since": null})
}

fn create_e(text: &str) -> Operation {
    create("Entry", "e", json!({"text": text, "note": null}))
}

fn delete_e() -> Operation {
    Operation {
        model: "Entry".into(),
        op: OperationKind::Delete,
        identity: json!({"id":"e"}),
        values: None,
    }
}

/// A Mutation that edits `Entry e` on the wire and, when `companion` is set,
/// also edits the same field locally through its `local` companion.
fn pending_edit(companion: bool) -> Mutation {
    let mut m = mutation("optimistic");
    if companion {
        m.companion = vec![update("companion")];
    }
    m
}

/// A device holding canonical `Entry e` ("canonical" at stamp 1) with a
/// direct write "direct" on it and a pending edit that holds the same row,
/// in the order `how` names.
fn direct_write_on_a_pending_row(source: Source, how: Direct) -> Device {
    let mut d = Device::new();
    d.deliver(source, "canonical", 1);
    match how {
        Direct::After => {
            d.enqueue(pending_edit(false));
            d.direct_text("direct");
        }
        Direct::Before => {
            d.direct_text("direct");
            d.enqueue(pending_edit(false));
        }
        Direct::AfterCompanion => {
            d.enqueue(pending_edit(true));
            assert_eq!(d.text().as_deref(), Some("companion"));
            d.direct_text("direct");
        }
    }
    let visible = match how {
        Direct::Before => "optimistic",
        Direct::After | Direct::AfterCompanion => "direct",
    };
    assert_eq!(
        d.text().as_deref(),
        Some(visible),
        "{source:?} {how:?}: the latest local write shows"
    );
    assert_eq!(
        d.c.pending_count().unwrap(),
        1,
        "{source:?} {how:?}: only the Mutation is queued"
    );
    d
}

/// A direct write never becomes a queued operation, and the first newer
/// canonical delivery of the same identity replaces it: a row with no stamp
/// (created from another transport), a stamped row updated in place and a
/// stamped row deleted locally. Nothing is retained that could replay the
/// direct write, across reopen too.
#[test]
fn a_direct_write_is_never_queued_and_newer_canonical_data_replaces_it() {
    for source in SOURCES {
        // The issue's case: the device has no row; another transport's answer
        // is stored with a direct create; a Channel, Load or Fetch delivers it.
        let mut d = Device::new();
        d.direct(create_e("from rest"));
        d.assert_nothing_queued(&format!("{source:?} create"));
        assert_eq!(
            d.stamp(),
            0,
            "{source:?}: a direct write allocates no stamp"
        );
        d.deliver(source, "canonical", 1);
        assert_eq!(d.text().as_deref(), Some("canonical"), "{source:?} create");
        assert_eq!(d.stamp(), 1);
        d.assert_nothing_queued(&format!("{source:?} create"));
        d.assert_nothing_retained(&format!("{source:?} create"));
        d.reopen();
        assert_eq!(d.text().as_deref(), Some("canonical"), "{source:?} create");
        d.assert_nothing_queued(&format!("{source:?} create after reopen"));

        // A stamped row updated in place, then newer canonical data.
        let mut d = Device::new();
        d.deliver(source, "canonical 1", 1);
        d.direct_text("from rest");
        d.assert_nothing_queued(&format!("{source:?} update"));
        assert_eq!(d.stamp(), 1, "{source:?}: the stamp is not advanced");
        d.deliver(source, "canonical 2", 2);
        assert_eq!(
            d.text().as_deref(),
            Some("canonical 2"),
            "{source:?} update"
        );
        d.assert_nothing_queued(&format!("{source:?} update"));
        d.assert_nothing_retained(&format!("{source:?} update"));
        d.reopen();
        assert_eq!(
            d.text().as_deref(),
            Some("canonical 2"),
            "{source:?} update"
        );

        // A stamped row deleted locally, then newer canonical data.
        let mut d = Device::new();
        d.deliver(source, "canonical 1", 1);
        d.direct(delete_e());
        assert_eq!(d.text(), None);
        d.assert_nothing_queued(&format!("{source:?} delete"));
        d.deliver(source, "canonical 2", 2);
        assert_eq!(
            d.text().as_deref(),
            Some("canonical 2"),
            "{source:?} delete"
        );
        d.assert_nothing_queued(&format!("{source:?} delete"));
        d.assert_nothing_retained(&format!("{source:?} delete"));
    }
}

/// A direct write on a row that a pending Mutation also holds survives that
/// Mutation's refusal: the rollback removes only the refused Mutation's
/// optimism, its `local` companion included, whether the direct write came
/// before or after it.
#[test]
fn a_refused_mutation_leaves_a_direct_write_on_its_row() {
    for source in SOURCES {
        for how in DIRECTS {
            let mut d = direct_write_on_a_pending_row(source, how);
            d.refuse_next();
            assert_eq!(
                d.text().as_deref(),
                Some("direct"),
                "{source:?} {how:?}: only the refused Mutation's change is removed"
            );
            assert_eq!(d.c.rejections().unwrap().len(), 1, "{source:?} {how:?}");
            assert_eq!(d.stamp(), 1, "{source:?} {how:?}");
            d.assert_nothing_queued(&format!("{source:?} {how:?}"));
            d.reopen();
            assert_eq!(
                d.text().as_deref(),
                Some("direct"),
                "{source:?} {how:?}: across reopen"
            );
        }
    }
}

/// After the refusal, newer canonical data for the row replaces the direct
/// write that survived it, and nothing replays the direct write afterwards.
#[test]
fn after_a_refusal_newer_canonical_data_replaces_the_surviving_direct_write() {
    for source in SOURCES {
        for how in DIRECTS {
            let mut d = direct_write_on_a_pending_row(source, how);
            d.refuse_next();
            assert_eq!(d.text().as_deref(), Some("direct"), "{source:?} {how:?}");
            d.deliver(source, "canonical 2", 2);
            assert_eq!(
                d.text().as_deref(),
                Some("canonical 2"),
                "{source:?} {how:?}: the delivered row wins"
            );
            d.assert_nothing_queued(&format!("{source:?} {how:?}"));
            d.assert_nothing_retained(&format!("{source:?} {how:?}"));
            d.reopen();
            assert_eq!(
                d.text().as_deref(),
                Some("canonical 2"),
                "{source:?} {how:?}: across reopen"
            );
        }
    }
}

/// Newer canonical data that arrives while the Mutation is still pending
/// replaces the direct write beneath the pending edit. The later refusal
/// then shows the delivered row: the direct write does not come back.
#[test]
fn canonical_data_during_a_pending_mutation_retires_the_direct_write_for_good() {
    for source in SOURCES {
        for how in DIRECTS {
            let mut d = direct_write_on_a_pending_row(source, how);
            d.deliver(source, "canonical 2", 2);
            let visible = match how {
                Direct::AfterCompanion => "companion",
                Direct::After | Direct::Before => "optimistic",
            };
            assert_eq!(
                d.text().as_deref(),
                Some(visible),
                "{source:?} {how:?}: the pending edit replays over the delivered row"
            );
            d.refuse_next();
            assert_eq!(
                d.text().as_deref(),
                Some("canonical 2"),
                "{source:?} {how:?}: the direct write does not come back"
            );
            d.assert_nothing_queued(&format!("{source:?} {how:?}"));
            d.assert_nothing_retained(&format!("{source:?} {how:?}"));
        }
    }
}

/// Current behaviour, not yet a decided guarantee: a redelivery at the stamp
/// the device already holds is not newer canonical data. The direct write
/// changed the row that stamp describes, so the redelivery is an equal-stamp
/// conflict (D2, D8). A Channel page reports it and keeps the direct write; a
/// Load page or a Fetch carrying it is refused whole (N2, Fetch). Nothing is
/// queued either way.
#[test]
fn an_equal_stamp_redelivery_is_not_newer_canonical_data() {
    for source in SOURCES {
        let mut d = Device::new();
        d.deliver(source, "canonical", 1);
        d.direct_text("from rest");
        match d.try_deliver(source, "canonical", 1) {
            Delivered::Channel(report) => {
                assert_eq!((report.applied, report.conflicts()), (0, 1));
                assert_eq!(
                    d.c.cursor("ch").unwrap(),
                    Some(d.cursor),
                    "the cursor moves"
                );
            }
            Delivered::Load(stored) => {
                let LoadStored::Failed(job) = *stored else {
                    panic!("expected a failed Load page, got {stored:?}")
                };
                let error = job.error.expect("the page fails its job");
                assert_eq!(error.code, STORE_FAILED);
                assert_eq!(error.diagnostics[0].code, "conflict");
            }
            Delivered::Fetch(result) => {
                let error = result.expect_err("the Fetch is refused");
                assert!(error.to_string().contains("conflicts"), "{error}");
            }
        }
        assert_eq!(
            d.text().as_deref(),
            Some("from rest"),
            "{source:?}: the direct write is kept"
        );
        assert_eq!(d.stamp(), 1, "{source:?}");
        d.assert_nothing_queued(&format!("{source:?}"));
    }
}
