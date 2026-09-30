//! Bounded pull: the `mode: "bootstrap"` request walks the historical
//! interval `(after, until]` of one channel with the ordinary scan and the
//! ordinary Loader resolution, and stops at the subscription origin while the
//! channel head keeps moving ([#151](https://github.com/zanminwang/axton/issues/151)).
use axton_core::{BootstrapPage, PullPage, limits};
use axton_server::{Config, Host, HostResult, code, host::HostRequest};
use serde_json::{Value, json};
use std::{
    future::Future,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll, Waker},
};

fn run<T>(future: impl Future<Output = T>) -> T {
    let mut f = std::pin::pin!(future);
    let mut cx = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(result) = f.as_mut().poll(&mut cx) {
            return result;
        }
    }
}
/// Two models so a page can prove the grouped, per-model load both pull modes
/// share; `Note` is a second loader with its own identity shape.
fn config() -> Config {
    Config::decode(json!({
        "schema":{"enums":[],"models":[
            {"name":"Entry","identity":["id"],"fields":[
                {"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"}},
                {"name":"text","nullable":false,"type":{"kind":"scalar","name":"string"}}]},
            {"name":"Note","identity":["id"],"fields":[
                {"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"}},
                {"name":"body","nullable":true,"type":{"kind":"scalar","name":"string"}}]}]},
        "loaders":["Entry","Note"],
        "mutations":[]
    }))
    .unwrap()
}
/// One invalidation row: the record's position in the channel with the
/// record's current stamp, as `scan` answers it.
fn row(channel: &str, cursor: u64, model: &str, id: &str, stamp: u64) -> Value {
    json!({"channel":channel,"cursor":cursor,"model":model,"identity":{"id":id},
           "identityKey":format!("{{\"id\":\"{id}\"}}"),"stamp":stamp})
}
fn entry(cursor: u64, id: &str, stamp: u64) -> Value {
    row("a", cursor, "Entry", id, stamp)
}

/// An in-memory host over one channel: `head` answers the configured head, and
/// `scan` answers the configured rows of the scanned channel whose cursor is
/// above `after`, sorted by cursor and truncated to `limit`, as PostgreSQL's
/// cursor-ordered scan does. In `verbatim` mode it answers the configured rows
/// exactly as given instead, so a test can feed the engine an answer the scan
/// contract refuses. Every `load` is recorded to prove which identities a page
/// actually read.
struct Scoped {
    head: u64,
    rows: Vec<Value>,
    loads: Mutex<Vec<(String, u64, Vec<Value>)>>,
    scans: Mutex<Vec<(String, u64, u64)>>,
    refuse: Option<String>,
    verbatim: bool,
}
impl Scoped {
    fn new(head: u64, rows: Vec<Value>) -> Self {
        Self {
            head,
            rows,
            loads: Mutex::new(vec![]),
            scans: Mutex::new(vec![]),
            refuse: None,
            verbatim: false,
        }
    }
    /// A host whose `scan` answers the configured rows exactly as given -
    /// unsorted and unfiltered - so a test can feed the engine an answer the
    /// scan contract refuses.
    fn raw(head: u64, rows: Vec<Value>) -> Self {
        Self {
            verbatim: true,
            ..Self::new(head, rows)
        }
    }
    /// Every identity the loaders were asked for, model first, in call order.
    fn loaded(&self) -> Vec<(String, Vec<String>)> {
        self.loads
            .lock()
            .unwrap()
            .iter()
            .map(|(model, _, identities)| {
                (
                    model.clone(),
                    identities
                        .iter()
                        .map(|i| i["id"].as_str().unwrap().to_string())
                        .collect(),
                )
            })
            .collect()
    }
}
impl Host for Scoped {
    fn call(&self, r: Value) -> Pin<Box<dyn Future<Output = HostResult<Value>> + Send + '_>> {
        Box::pin(async move {
            let request: HostRequest = serde_json::from_value(r)
                .map_err(|error| format!("unsupported host request: {error}"))?;
            Ok(match &request {
                HostRequest::Head { .. } => json!(self.head),
                HostRequest::Scan {
                    channel,
                    after,
                    limit,
                } => {
                    self.scans
                        .lock()
                        .unwrap()
                        .push((channel.clone(), *after, *limit));
                    if self.verbatim {
                        Value::Array(self.rows.iter().take(*limit as usize).cloned().collect())
                    } else {
                        let mut rows: Vec<Value> = self
                            .rows
                            .iter()
                            .filter(|row| {
                                row["channel"] == json!(channel)
                                    && row["cursor"].as_u64().unwrap() > *after
                            })
                            .cloned()
                            .collect();
                        rows.sort_by_key(|row| row["cursor"].as_u64().unwrap());
                        rows.truncate(*limit as usize);
                        Value::Array(rows)
                    }
                }
                HostRequest::Load {
                    model,
                    version,
                    identities,
                    ..
                } => {
                    self.loads
                        .lock()
                        .unwrap()
                        .push((model.clone(), *version, identities.clone()));
                    if let Some(rejection) = &self.refuse {
                        return Ok(json!({"rejection":rejection}));
                    }
                    Value::Array(
                        identities
                            .iter()
                            .map(|identity| match model.as_str() {
                                "Note" => json!({"id":identity["id"],"body":null}),
                                _ => json!({"id":identity["id"],"text":"t"}),
                            })
                            .collect(),
                    )
                }
                other => return Err(format!("unsupported {}", other.label())),
            })
        })
    }
}
/// A bootstrap request on channel `a` for the interval `(after, until]`.
fn body(after: u64, until: u64) -> Vec<u8> {
    json!({"mode":"bootstrap","channel":"a","models":{"Entry":1,"Note":1},
           "after":after,"until":until})
    .to_string()
    .into_bytes()
}
fn bootstrap(host: &Scoped, after: u64, until: u64) -> BootstrapPage {
    let text = run(axton_server::process_pull(
        &config(),
        "u",
        &body(after, until),
        host,
    ))
    .unwrap();
    BootstrapPage::decode(text.as_bytes()).unwrap()
}
fn ids(page: &BootstrapPage) -> Vec<String> {
    page.records
        .iter()
        .map(|r| r.identity["id"].as_str().unwrap().to_string())
        .collect()
}

#[test]
fn a_record_republished_above_the_origin_leaves_the_historical_interval() {
    // S = 100: the row at 40 is historical, the row at 120 belongs to the
    // subscription's own delivery and is never loaded. The page is terminal:
    // the scan crossed the origin, so nothing below it remains.
    let host = Scoped::new(140, vec![entry(40, "e", 7), entry(120, "f", 9)]);
    let page = bootstrap(&host, 0, 100);
    assert_eq!(
        (page.from, page.to, page.until, page.head),
        (0, 100, 100, 140)
    );
    assert!(page.terminal(), "crossing the origin finishes the interval");
    assert_eq!(ids(&page), ["e"]);
    assert_eq!(
        host.loaded(),
        [("Entry".to_string(), vec!["e".to_string()])],
        "only the historical identity is read"
    );
    assert_eq!(page.records[0].stamp, 7);
    assert_eq!(page.records[0].state, json!({"text":"t"}));
}

#[test]
fn a_full_scan_below_the_origin_is_nonterminal_and_stops_at_its_last_cursor() {
    let rows: Vec<Value> = (1..=limits::PULL_CHANGES as u64)
        .map(|i| entry(i, &format!("e{i}"), i))
        .collect();
    let host = Scoped::new(400, rows);
    let page = bootstrap(&host, 0, 100);
    assert_eq!(page.to, limits::PULL_CHANGES as u64);
    assert!(!page.terminal(), "the interval continues above `to`");
    assert_eq!(page.records.len(), limits::PULL_CHANGES);
    assert_eq!(
        *host.scans.lock().unwrap(),
        [("a".to_string(), 0, limits::PULL_CHANGES as u64)],
        "one bounded scan per page"
    );
    // The next page starts where this one stopped and finishes the interval.
    let next = bootstrap(&host, page.to, 100);
    assert_eq!((next.from, next.to), (limits::PULL_CHANGES as u64, 100));
    assert!(next.terminal());
    assert!(next.records.is_empty());
}

#[test]
fn a_full_scan_whose_last_row_sits_on_the_origin_is_terminal() {
    // Fifty rows, the fiftieth exactly at S: the interval is exhausted and the
    // row at the origin is itself historical.
    let mut rows: Vec<Value> = (1..limits::PULL_CHANGES as u64)
        .map(|i| entry(i, &format!("e{i}"), i))
        .collect();
    rows.push(entry(100, "last", 5));
    let host = Scoped::new(140, rows);
    let page = bootstrap(&host, 0, 100);
    assert_eq!(page.to, 100);
    assert!(page.terminal());
    assert_eq!(page.records.len(), limits::PULL_CHANGES);
    assert!(
        ids(&page).contains(&"last".to_string()),
        "a row at S is historical"
    );
}

#[test]
fn an_empty_scan_is_terminal_and_carries_the_current_head() {
    let host = Scoped::new(140, vec![]);
    let page = bootstrap(&host, 0, 100);
    assert_eq!(
        (page.from, page.to, page.until, page.head),
        (0, 100, 100, 140)
    );
    assert!(page.terminal());
    assert!(page.records.is_empty());
    assert!(host.loads.lock().unwrap().is_empty(), "nothing to read");
}

#[test]
fn an_exhausted_interval_is_an_empty_terminal_page_carrying_the_head() {
    let host = Scoped::new(140, vec![entry(40, "e", 7)]);
    let page = bootstrap(&host, 100, 100);
    assert_eq!(
        (page.from, page.to, page.until, page.head),
        (100, 100, 100, 140)
    );
    assert!(page.terminal());
    assert!(page.records.is_empty());
    assert!(
        host.scans.lock().unwrap().is_empty(),
        "an exhausted interval scans nothing"
    );
    // A zero-head empty Scope completes the same way.
    let empty = Scoped::new(0, vec![]);
    let page = bootstrap(&empty, 0, 0);
    assert_eq!((page.from, page.to, page.until, page.head), (0, 0, 0, 0));
    assert!(page.terminal());
}

#[test]
fn an_origin_above_the_head_is_refused() {
    let host = Scoped::new(80, vec![entry(40, "e", 7)]);
    let err = run(axton_server::process_pull(
        &config(),
        "u",
        &body(0, 100),
        &host,
    ))
    .unwrap_err();
    assert_eq!(err.code, code::REQUEST_INVALID, "{err}");
    assert!(err.message.contains("ahead of head"), "{err}");
    assert!(host.scans.lock().unwrap().is_empty(), "nothing is scanned");
}

#[test]
fn a_bootstrap_request_is_refused_like_an_ordinary_pull_when_it_is_malformed() {
    let host = Scoped::new(140, vec![]);
    let pull = |raw: Value| {
        run(axton_server::process_pull(
            &config(),
            "u",
            raw.to_string().as_bytes(),
            &host,
        ))
    };
    // A present mode that is not the string `bootstrap` is neither mode.
    for mode in [
        json!("snapshot"),
        json!(null),
        json!(1),
        json!(true),
        json!({}),
    ] {
        let err = pull(json!({"mode":mode,"channel":"a","models":{"Entry":1},"after":0,"until":1}))
            .unwrap_err();
        assert_eq!(err.code, code::REQUEST_INVALID, "mode {mode}: {err}");
        assert!(err.message.contains("mode"), "mode {mode}: {err}");
    }
    // A bootstrap envelope whose own fields are wrong is a request error.
    for bad in [
        json!({"mode":"bootstrap","channel":"","models":{"Entry":1},"after":0,"until":1}),
        json!({"mode":"bootstrap","channel":" ","models":{"Entry":1},"after":0,"until":1}),
        json!({"mode":"bootstrap","channel":"a","models":{"Entry":1},"after":2,"until":1}),
        json!({"mode":"bootstrap","channel":"a","models":{"Entry":1},"until":1}),
        json!({"mode":"bootstrap","channel":"a","models":{"Entry":1},"after":-1,"until":1}),
        json!({"mode":"bootstrap","channel":"a","models":{},"after":0,"until":1}),
    ] {
        assert_eq!(
            pull(bad.clone()).unwrap_err().code,
            code::REQUEST_INVALID,
            "{bad}"
        );
    }
    // An undeclared or unretained read contract is refused with its own code.
    let err =
        pull(json!({"mode":"bootstrap","channel":"a","models":{"Entry":9},"after":0,"until":1}))
            .unwrap_err();
    assert_eq!(err.code, code::MODEL_VERSION_UNSUPPORTED, "{err}");
    let err = run(axton_server::process_pull(
        &config(),
        " ",
        &body(0, 1),
        &host,
    ))
    .unwrap_err();
    assert_eq!(err.code, code::PRINCIPAL_INVALID, "{err}");
}

#[test]
fn a_page_holding_a_model_the_client_did_not_declare_is_refused() {
    let host = Scoped::new(140, vec![row("a", 10, "Note", "n", 3)]);
    let request =
        json!({"mode":"bootstrap","channel":"a","models":{"Entry":1},"after":0,"until":100});
    let err = run(axton_server::process_pull(
        &config(),
        "u",
        request.to_string().as_bytes(),
        &host,
    ))
    .unwrap_err();
    assert_eq!(err.code, code::MODEL_VERSION_UNSUPPORTED, "{err}");
}

#[test]
fn both_pull_modes_resolve_records_through_the_same_grouped_loader_helper() {
    // Two models in one page: each is loaded once, with all its identities,
    // at the declared version; a refusal becomes that record's error change.
    let rows = vec![
        entry(10, "e", 7),
        row("a", 20, "Note", "n", 3),
        entry(30, "f", 9),
    ];
    let host = Scoped::new(140, rows.clone());
    let page = bootstrap(&host, 0, 100);
    assert_eq!(ids(&page), ["e", "f", "n"], "canonical record order");
    assert_eq!(
        host.loaded(),
        [
            ("Entry".to_string(), vec!["e".to_string(), "f".to_string()]),
            ("Note".to_string(), vec!["n".to_string()])
        ],
        "one grouped call per model"
    );
    assert_eq!(page.records[2].state, json!({"body":null}));
    // The same records, over the ordinary pull mode, carry the same authority.
    let ordinary = axton_core::PullRequest {
        cursors: [("a".to_string(), 0)].into(),
        models: [("Entry".to_string(), 1), ("Note".to_string(), 1)].into(),
    }
    .encode()
    .unwrap();
    let host = Scoped::new(140, rows.clone());
    let text = run(axton_server::process_pull(&config(), "u", &ordinary, &host)).unwrap();
    let delta = PullPage::decode(text.as_bytes()).unwrap();
    assert_eq!(delta.changes, page.records, "one resolution for both modes");
    // A refusal isolates the record in either mode, with the refusal code.
    let mut refusing = Scoped::new(140, rows);
    refusing.refuse = Some("entry.forbidden".into());
    let page = bootstrap(&refusing, 0, 100);
    assert_eq!(
        page.records
            .iter()
            .map(|r| r.error.as_deref())
            .collect::<Vec<_>>(),
        [
            Some("entry.forbidden"),
            Some("entry.forbidden"),
            Some("entry.forbidden")
        ]
    );
    assert!(page.records.iter().all(|r| r.state.is_null()));
}

#[test]
fn a_malformed_scan_row_fails_the_request() {
    let cases: [(Value, &str); 4] = [
        (entry(0, "e", 7), "a cursor at or below `after`"),
        (entry(400, "e", 7), "a cursor above the head"),
        (row("other", 10, "Entry", "e", 7), "another channel"),
        (
            json!({"channel":"a","cursor":10,"model":"Entry","identity":{"id":"e"},
                   "identityKey":"{\"id\":\"other\"}","stamp":7}),
            "a noncanonical identity key",
        ),
    ];
    for (bad, why) in cases {
        let host = Scoped::raw(140, vec![bad]);
        let err = run(axton_server::process_pull(
            &config(),
            "u",
            &body(0, 100),
            &host,
        ))
        .unwrap_err();
        assert_eq!(err.code, code::STORAGE_INVALID, "{why}: {err}");
    }
    // A row of a model with no registered loader is a configuration fault.
    let host = Scoped::raw(140, vec![row("a", 10, "Other", "e", 7)]);
    let err = run(axton_server::process_pull(
        &config(),
        "u",
        &body(0, 100),
        &host,
    ))
    .unwrap_err();
    assert_eq!(err.code, code::LOADER_UNREGISTERED, "{err}");
}

#[test]
fn successive_pages_drop_a_record_republished_above_the_origin() {
    // The first page reads `e` at cursor 10. Between the pages `e` is
    // republished to 120, above S: its row leaves the historical interval and
    // the second page does not deliver it again.
    let first = Scoped::new(140, vec![entry(10, "e", 7), entry(60, "f", 8)]);
    let page = bootstrap(&first, 0, 100);
    assert_eq!(ids(&page), ["e", "f"]);
    assert!(page.terminal());
    let moved = Scoped::new(160, vec![entry(60, "f", 8), entry(120, "e", 11)]);
    let second = bootstrap(&moved, 60, 100);
    assert_eq!((second.from, second.to, second.head), (60, 100, 160));
    assert!(second.terminal());
    assert!(
        second.records.is_empty(),
        "the republished record belongs to the subscription's delivery"
    );
}

#[test]
fn the_ordinary_pull_page_is_unchanged() {
    // Pins the ordinary page's bytes across the shared-helper refactor: two
    // models, a deletion and a full channel that continues.
    struct Fixed;
    impl Host for Fixed {
        fn call(&self, r: Value) -> Pin<Box<dyn Future<Output = HostResult<Value>> + Send + '_>> {
            Box::pin(async move {
                let request: HostRequest = serde_json::from_value(r).map_err(|e| e.to_string())?;
                Ok(match &request {
                    HostRequest::Head { channel } => {
                        json!(if channel == "a" { 80 } else { 9 })
                    }
                    HostRequest::Scan { channel, .. } if channel == "a" => Value::Array(
                        (1..=limits::PULL_CHANGES as u64)
                            .map(|i| entry(i, &format!("e{i}"), i))
                            .collect(),
                    ),
                    HostRequest::Scan { .. } => json!([row("b", 9, "Note", "n", 4)]),
                    HostRequest::Load {
                        model, identities, ..
                    } => Value::Array(
                        identities
                            .iter()
                            .enumerate()
                            .map(|(i, identity)| match (model.as_str(), i) {
                                ("Note", _) => json!({"id":identity["id"],"body":"b"}),
                                (_, 0) => Value::Null,
                                _ => json!({"id":identity["id"],"text":"t"}),
                            })
                            .collect(),
                    ),
                    other => return Err(format!("unsupported {}", other.label())),
                })
            })
        }
    }
    let request = axton_core::PullRequest {
        cursors: [("a".to_string(), 0), ("b".to_string(), 0)].into(),
        models: [("Entry".to_string(), 1), ("Note".to_string(), 1)].into(),
    }
    .encode()
    .unwrap();
    let text = run(axton_server::process_pull(&config(), "u", &request, &Fixed)).unwrap();
    let page = PullPage::decode(text.as_bytes()).unwrap();
    assert_eq!(page.cursors["a"].to, limits::PULL_CHANGES as u64);
    assert_eq!(page.cursors["a"].head, 80);
    assert!(page.cursors["a"].continues());
    assert_eq!(page.cursors["b"].to, 9);
    assert_eq!(page.changes.len(), limits::PULL_CHANGES + 1);
    assert!(page.changes[0].state.is_null(), "a tombstone is a deletion");
    assert_eq!(
        text,
        String::from_utf8(page.encode().unwrap()).unwrap(),
        "the page the engine emits is canonical"
    );
    // Byte-for-byte: the pinned prefix and suffix of the ordinary page.
    assert!(
        text.starts_with(
            r#"{"changes":[{"identity":{"id":"e1"},"model":"Entry","stamp":1,"state":null},"#
        ),
        "{text}"
    );
    assert!(
        text.ends_with(
            r#"{"identity":{"id":"n"},"model":"Note","stamp":4,"state":{"body":"b"}}],"cursors":{"a":{"from":0,"head":80,"to":50},"b":{"from":0,"head":9,"to":9}}}"#
        ),
        "{text}"
    );
}

fn channel_page(host: &Scoped, request: Value) -> Value {
    serde_json::from_str(
        &run(axton_server::process_channel_pull(
            &config(),
            "alice",
            &serde_json::to_vec(&request).unwrap(),
            host,
        ))
        .unwrap(),
    )
    .unwrap()
}
fn removed(cursor: u64, id: &str) -> Value {
    let mut row = entry(cursor, id, 1);
    row["kind"] = json!("remove");
    row
}
#[test]
fn channel_pages_keep_mixed_changes_and_each_channels_provenance_with_one_loader_read() {
    let host = Scoped::new(
        9,
        vec![
            entry(2, "e", 3),
            removed(7, "gone"),
            row("b", 8, "Entry", "e", 3),
        ],
    );
    let page = channel_page(&host, json!({"models":{"Entry":1},"cursors":{"a":0,"b":0}}));
    assert_eq!(page["changes"].as_array().unwrap().len(), 3);
    assert_eq!(page["changes"][1]["kind"], "remove");
    assert_eq!(page["changes"][2]["channel"], "b");
    assert_eq!(host.loaded(), vec![("Entry".into(), vec!["e".into()])]);
}
#[test]
fn channel_delta_and_bootstrap_advance_over_compacted_gaps_after_a_full_last_page() {
    for bootstrap in [false, true] {
        let host = Scoped::new(
            100,
            (1..=50).map(|i| removed(i, &format!("e{i}"))).collect(),
        );
        let request = if bootstrap {
            json!({"mode":"bootstrap","models":{"Entry":1},"channel":"a","after":0,"until":90})
        } else {
            json!({"models":{"Entry":1},"cursors":{"a":0}})
        };
        let page = channel_page(&host, request);
        assert_eq!(page["changes"].as_array().unwrap().len(), 50);
        assert_eq!(
            if bootstrap {
                &page["to"]
            } else {
                &page["cursors"]["a"]["to"]
            },
            &json!(if bootstrap { 90 } else { 100 })
        );
        assert!(host.loaded().is_empty());
    }
}
#[test]
fn channel_pages_continue_after_fifty_and_bootstrap_excludes_rows_moved_above_its_bound() {
    let host = Scoped::new(
        100,
        (1..=51)
            .map(|i| removed(i, &format!("e{i}")))
            .chain([entry(100, "moved", 4)])
            .collect(),
    );
    let first = channel_page(
        &host,
        json!({"mode":"bootstrap","models":{"Entry":1},"channel":"a","after":0,"until":90}),
    );
    assert_eq!(first["to"], 50);
    let second = channel_page(
        &host,
        json!({"mode":"bootstrap","models":{"Entry":1},"channel":"a","after":50,"until":90}),
    );
    assert_eq!(second["to"], 90);
    assert_eq!(second["changes"].as_array().unwrap().len(), 1);
    assert!(host.loaded().is_empty());
    let delta = channel_page(&host, json!({"models":{"Entry":1},"cursors":{"a":90}}));
    assert_eq!(delta["changes"][0]["identity"]["id"], "moved");
}
