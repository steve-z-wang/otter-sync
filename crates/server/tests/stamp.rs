//! Pull copies the current record stamp from the scan row, covers every
//! channel of one request and isolates a record its loader cannot read; an
//! external notification allocates one stamp per record and publishes it at
//! that stamp.
use axton_server::{Config, Host, host::HostRequest};
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
fn config() -> Config {
    Config::decode(json!({
        "schema":{"enums":[],"models":[{"name":"Entry","identity":["id"],"fields":[
            {"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"}},
            {"name":"text","nullable":false,"type":{"kind":"scalar","name":"string"}}]}]},
        "loaders":["Entry"],
        "mutations":[]
    }))
    .unwrap()
}
/// `scan` returns the given rows; `publish` returns the given value. Both stay
/// raw `Value`s: these tests feed the engine answers the contract refuses.
/// Records start in no Channel and every membership write is acknowledged.
struct Fixed {
    scan: Value,
    publish: Value,
    published: Mutex<Vec<HostRequest>>,
    loaded: Mutex<Vec<u64>>,
    advanced: Mutex<Vec<String>>,
    ensured: Mutex<Vec<String>>,
}
impl Fixed {
    fn new(scan: Value, publish: Value) -> Self {
        Self {
            scan,
            publish,
            published: Mutex::new(vec![]),
            loaded: Mutex::new(vec![]),
            advanced: Mutex::new(vec![]),
            ensured: Mutex::new(vec![]),
        }
    }
}
impl Host for Fixed {
    fn call(
        &self,
        r: Value,
    ) -> Pin<Box<dyn Future<Output = axton_server::HostResult<Value>> + Send + '_>> {
        Box::pin(async move {
            let request: HostRequest = serde_json::from_value(r)
                .map_err(|error| format!("unsupported host request: {error}"))?;
            Ok(match &request {
                HostRequest::Head { .. } => json!(5),
                HostRequest::Scan { .. } => self.scan.clone(),
                HostRequest::Load { version, .. } => {
                    self.loaded.lock().unwrap().push(*version);
                    json!([{"id":"e","text":"t"}])
                }
                HostRequest::AdvanceStamp { identity_key, .. } => {
                    self.advanced.lock().unwrap().push(identity_key.clone());
                    json!(9)
                }
                HostRequest::EnsureStamp { identity_key, .. } => {
                    self.ensured.lock().unwrap().push(identity_key.clone());
                    json!(9)
                }
                HostRequest::Publish { .. } => {
                    self.published.lock().unwrap().push(request.clone());
                    self.publish.clone()
                }
                HostRequest::LockRecord { .. } => json!(9),
                HostRequest::Memberships { .. } => json!([]),
                HostRequest::SetMembership { .. } => Value::Null,
                other => return Err(format!("unsupported {}", other.label())),
            })
        })
    }
}
fn pull_body() -> Vec<u8> {
    pull_body_declaring(&[("Entry", 1)])
}
/// A pull on `a` from cursor 0 declaring these read contracts.
fn pull_body_declaring(models: &[(&str, u64)]) -> Vec<u8> {
    axton_core::PullRequest {
        cursors: [("a".to_string(), 0)].into(),
        models: models
            .iter()
            .map(|(name, version)| ((*name).to_string(), *version))
            .collect(),
    }
    .encode()
    .unwrap()
}
fn row(stamp: Value) -> Value {
    let mut row = json!({"channel":"a","cursor":1,"model":"Entry","identity":{"id":"e"},"identityKey":"{\"id\":\"e\"}"});
    if !stamp.is_null() {
        row["stamp"] = stamp;
    }
    json!([row])
}

#[test]
fn pull_copies_the_row_stamp_into_the_change() {
    let host = Fixed::new(row(json!(7)), Value::Null);
    let text = run(axton_server::process_pull(
        &config(),
        "u",
        &pull_body(),
        &host,
    ))
    .unwrap();
    let page = axton_core::PullPage::decode(text.as_bytes()).unwrap();
    assert_eq!(page.changes[0].stamp, 7);
    assert_eq!(
        *host.loaded.lock().unwrap(),
        [1],
        "the load names the model version it serves"
    );
}

#[test]
fn pull_normalizes_loader_rows_with_the_retained_contract_of_the_served_version() {
    // The retained contract of the served version, not the current schema,
    // decides which fields a loader may return.
    let host = Fixed::new(row(json!(7)), Value::Null);
    let mut c = json!({
        "schema":{"enums":[],"models":[{"name":"Entry","version":2,"identity":["id"],"fields":[
            {"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"}},
            {"name":"text","nullable":false,"type":{"kind":"scalar","name":"string"}},
            {"name":"note","nullable":true,"type":{"kind":"scalar","name":"string"}}]}]},
        "loaders":["Entry"],
        "mutations":[]
    });
    c["models"] = json!([
        {"name":"Entry","version":1,"identity":["id"],"enums":[],"fields":[
            {"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"}},
            {"name":"text","nullable":false,"type":{"kind":"scalar","name":"string"}}]},
        {"name":"Entry","version":2,"identity":["id"],"enums":[],"fields":c["schema"]["models"][0]["fields"].clone()}
    ]);
    let config = Config::decode(c).unwrap();
    // An old client declares v1: the v1 loader runs and the v1 contract shapes the row.
    let text = run(axton_server::process_pull(
        &config,
        "u",
        &pull_body_declaring(&[("Entry", 1)]),
        &host,
    ))
    .unwrap();
    let page = axton_core::PullPage::decode(text.as_bytes()).unwrap();
    assert_eq!(page.changes[0].state, json!({"text":"t"}));
    // A new client declares v2 for the same data: the v2 loader and contract.
    let text = run(axton_server::process_pull(
        &config,
        "u",
        &pull_body_declaring(&[("Entry", 2)]),
        &host,
    ))
    .unwrap();
    let page = axton_core::PullPage::decode(text.as_bytes()).unwrap();
    assert_eq!(page.changes[0].state, json!({"text":"t","note":null}));
    assert_eq!(
        *host.loaded.lock().unwrap(),
        [1, 2],
        "each pull reaches the loader of the version it declared"
    );
    // A declared version that is not retained, or a model this backend does
    // not have, is refused before anything is scanned or loaded.
    for (models, detail) in [
        (&[("Entry", 3)][..], json!({"model":"Entry","version":3})),
        (
            &[("Entry", 2), ("Ghost", 1)][..],
            json!({"model":"Ghost","version":1}),
        ),
    ] {
        let err = run(axton_server::process_pull(
            &config,
            "u",
            &pull_body_declaring(models),
            &host,
        ))
        .unwrap_err();
        assert_eq!(
            err.code,
            axton_server::code::MODEL_VERSION_UNSUPPORTED,
            "{err}"
        );
        assert_eq!(err.details, detail, "{err}");
    }
    assert_eq!(
        *host.loaded.lock().unwrap(),
        [1, 2],
        "a refused declaration loads nothing"
    );
}

#[test]
fn a_page_holding_a_model_the_client_did_not_declare_is_refused_whole() {
    // Pending per-read isolation (#95): the pull is refused as a whole, with
    // the model named, rather than skipped or served at a guessed version.
    let host = Fixed::new(row(json!(7)), Value::Null);
    let c = json!({
        "schema":{"enums":[],"models":[
            {"name":"Entry","identity":["id"],"fields":[
                {"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"}},
                {"name":"text","nullable":false,"type":{"kind":"scalar","name":"string"}}]},
            {"name":"Note","identity":["id"],"fields":[
                {"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"}}]}]},
        "loaders":["Entry","Note"],
        "mutations":[]
    });
    let config = Config::decode(c).unwrap();
    let err = run(axton_server::process_pull(
        &config,
        "u",
        &pull_body_declaring(&[("Note", 1)]),
        &host,
    ))
    .unwrap_err();
    assert_eq!(
        err.code,
        axton_server::code::MODEL_VERSION_UNSUPPORTED,
        "{err}"
    );
    assert_eq!(err.details, json!({"model":"Entry"}));
    assert!(host.loaded.lock().unwrap().is_empty(), "no loader ran");
}

#[test]
fn pull_rejects_rows_without_a_positive_stamp() {
    for bad in [Value::Null, json!(0), json!(-1), json!(9007199254740992u64)] {
        let host = Fixed::new(row(bad.clone()), Value::Null);
        let err = run(axton_server::process_pull(
            &config(),
            "u",
            &pull_body(),
            &host,
        ))
        .unwrap_err();
        assert!(err.message.contains("stamp"), "{bad}: {err}");
        assert_eq!(err.code, axton_server::code::STORAGE_INVALID);
    }
}

#[test]
fn an_external_settlement_advances_one_stamp_per_record_and_distributes_it_at_that_stamp() {
    let enroll = |channel: &str| json!({"channel":channel,"model":"Entry","identity":{"id":"e"},"present":true});
    let settlement = json!({
        "changes":[{"model":"Entry","identity":{"id":"e"}}],
        "memberships":[enroll("a"),enroll("b")]
    });
    let ok = Fixed::new(json!([]), json!({"cursor":3,"stamp":9}));
    let answer = run(axton_server::settle_external(&config(), &settlement, &ok)).unwrap();
    assert_eq!(
        answer,
        json!([{"model":"Entry","identity":{"id":"e"},"stamp":9}]),
        "the changed records come back with their stamps"
    );
    let published = ok.published.lock().unwrap();
    assert_eq!(published.len(), 2, "one invalidation per channel");
    for request in published.iter() {
        let HostRequest::Publish { stamp, .. } = request else {
            panic!("not a publish");
        };
        assert_eq!(*stamp, 9, "both channels carry the one allocated stamp");
    }
    drop(published);
    // An enrolled but unchanged record keeps its stamp; `ensureStamp`
    // initializes it.
    let ensure = Fixed::new(json!([]), json!({"cursor":4,"stamp":9}));
    let membership_only = json!({"changes":[],"memberships":[enroll("a")]});
    run(axton_server::settle_external(
        &config(),
        &membership_only,
        &ensure,
    ))
    .unwrap();
    assert_eq!(
        ensure.ensured.lock().unwrap().len(),
        1,
        "an unchanged member is initialized, not advanced"
    );
    assert!(ensure.advanced.lock().unwrap().is_empty());
    // The host must echo the stamp the engine named; anything else is unusable.
    for bad in [
        json!(3),
        json!({"cursor":3}),
        json!({"cursor":3,"stamp":0}),
        json!({"cursor":3,"stamp":8}),
    ] {
        let host = Fixed::new(json!([]), bad.clone());
        let err = run(axton_server::settle_external(&config(), &settlement, &host)).unwrap_err();
        assert_eq!(err.code, axton_server::code::HOST_INVALID, "{bad}: {err}");
    }
    // A membership naming no channel is refused: a blank name is no more a
    // channel than an empty one, and nothing is published for it.
    for blank in ["", " ", "\t\n"] {
        let host = Fixed::new(json!([]), json!({"cursor":3,"stamp":9}));
        let err = run(axton_server::settle_external(
            &config(),
            &json!({"changes":[{"model":"Entry","identity":{"id":"e"}}],
                    "memberships":[{"channel":blank,"model":"Entry","identity":{"id":"e"},"present":true}]}),
            &host,
        ))
        .unwrap_err();
        assert_eq!(err.code, axton_server::code::PUBLISH_INVALID, "{blank:?}");
        assert!(host.published.lock().unwrap().is_empty(), "{blank:?}");
    }
    // A rejection, a malformed settlement or the retired `publications`
    // shape is refused before any host call: there is no implicit publication.
    for bad in [
        json!({"rejection":"x"}),
        json!({"changes":[]}),
        json!({"channels":["a"]}),
        json!({"changes":[],"publications":[{"channel":"a"}]}),
        json!({"changes":[],"memberships":[],"publications":[]}),
        json!({"changes":[],"memberships":[{"channel":"a","model":"Entry","identity":{"id":"e"}}]}),
        json!({"changes":[],"memberships":[{"channel":"a","model":"Entry","identity":{"id":"e"},"present":"yes"}]}),
    ] {
        let host = Fixed::new(json!([]), json!({"cursor":3,"stamp":9}));
        let err = run(axton_server::settle_external(&config(), &bad, &host)).unwrap_err();
        assert_eq!(
            err.code,
            axton_server::code::PUBLISH_INVALID,
            "{bad}: {err}"
        );
        assert!(host.published.lock().unwrap().is_empty());
    }
}

#[test]
fn live_negotiation_establishes_current_heads_and_rejects_cursor_modes() {
    let host = Fixed::new(json!([]), Value::Null);
    let result = run(axton_server::live::negotiate(
        &config(),
        "u",
        br#"{"type":"subscribe","channels":["a"],"models":{"Entry":1}}"#,
        &host,
    ))
    .unwrap();
    assert_eq!(result.heads["a"], 5);
    assert_eq!(
        result.models.get("Entry"),
        Some(&1),
        "the session keeps the declaration"
    );
    // The declaration is checked at the handshake, like a pull's.
    for (frame, code) in [
        (
            r#"{"type":"subscribe","channels":["a"]}"#,
            axton_server::code::REQUEST_INVALID,
        ),
        (
            r#"{"type":"subscribe","channels":["a"],"models":{"Entry":2}}"#,
            axton_server::code::MODEL_VERSION_UNSUPPORTED,
        ),
        (
            r#"{"type":"subscribe","channels":["a"],"models":{"Ghost":1}}"#,
            axton_server::code::MODEL_VERSION_UNSUPPORTED,
        ),
    ] {
        let err = run(axton_server::live::negotiate(
            &config(),
            "u",
            frame.as_bytes(),
            &host,
        ))
        .unwrap_err();
        assert_eq!(err.code, code, "{frame}: {err}");
    }
    for cursors in [
        json!({"a":0}),
        json!({}),
        json!({"a":6}),
        json!({"a":-1}),
        json!({"a":1.5}),
        json!({"a":"0"}),
        json!({"a":null}),
        json!({"a":9007199254740992u64}),
        json!({"a":0,"b":0}),
        json!(null),
        json!([]),
    ] {
        let request =
            json!({"type":"subscribe","channels":["a"],"models":{"Entry":1},"cursors":cursors});
        assert!(
            run(axton_server::live::negotiate(
                &config(),
                "u",
                request.to_string().as_bytes(),
                &host
            ))
            .is_err(),
            "accepted {request}"
        );
    }
}

/// A scripted host for pulls: per-channel scan rows and per-call load answers.
struct Multi {
    scans: BTreeMap<String, Value>,
    heads: BTreeMap<String, u64>,
    /// Answers for `load`, consumed in call order; a missing answer loads rows.
    loads: Mutex<Vec<Value>>,
    log: Mutex<Vec<HostRequest>>,
}
use std::collections::BTreeMap;
impl Host for Multi {
    fn call(
        &self,
        r: Value,
    ) -> Pin<Box<dyn Future<Output = axton_server::HostResult<Value>> + Send + '_>> {
        Box::pin(async move {
            let request: HostRequest = serde_json::from_value(r)
                .map_err(|error| format!("unsupported host request: {error}"))?;
            self.log.lock().unwrap().push(request.clone());
            Ok(match &request {
                HostRequest::Head { channel } => {
                    json!(self.heads.get(channel).copied().unwrap_or(0))
                }
                HostRequest::Scan { channel, .. } => {
                    self.scans.get(channel).cloned().unwrap_or(json!([]))
                }
                HostRequest::Load { identities, .. } => {
                    let scripted = self.loads.lock().unwrap();
                    if scripted.is_empty() {
                        Value::Array(
                            identities
                                .iter()
                                .map(|id| json!({"id":id["id"],"text":format!("t-{}", id["id"].as_str().unwrap())}))
                                .collect(),
                        )
                    } else {
                        drop(scripted);
                        self.loads.lock().unwrap().remove(0)
                    }
                }
                other => return Err(format!("unsupported {}", other.label())),
            })
        })
    }
}
fn scan_row(channel: &str, cursor: u64, id: &str, stamp: u64) -> Value {
    json!({"channel":channel,"cursor":cursor,"model":"Entry","identity":{"id":id},"identityKey":format!("{{\"id\":\"{id}\"}}"),"stamp":stamp})
}
fn multi(scans: &[(&str, Vec<Value>, u64)], loads: Vec<Value>) -> Multi {
    Multi {
        scans: scans
            .iter()
            .map(|(c, rows, _)| (c.to_string(), json!(rows)))
            .collect(),
        heads: scans
            .iter()
            .map(|(c, _, head)| (c.to_string(), *head))
            .collect(),
        loads: Mutex::new(loads),
        log: Mutex::new(vec![]),
    }
}
fn pull_all(host: &Multi, cursors: &[(&str, u64)]) -> axton_server::Result<axton_core::PullPage> {
    let request = axton_core::PullRequest {
        cursors: cursors.iter().map(|(c, n)| (c.to_string(), *n)).collect(),
        models: [("Entry".to_string(), 1)].into(),
    }
    .encode()
    .unwrap();
    run(axton_server::process_pull(&config(), "u", &request, host))
        .map(|text| axton_core::PullPage::decode(text.as_bytes()).unwrap())
}

/// One pull covers every channel: each channel scans after its own cursor and
/// reports its own progress and head, and a record both channels changed is
/// delivered once at its current stamp.
#[test]
fn one_pull_covers_every_channel_and_delivers_a_shared_record_once() {
    let host = multi(
        &[
            (
                "a",
                vec![scan_row("a", 1, "e", 4), scan_row("a", 2, "f", 1)],
                2,
            ),
            ("b", vec![scan_row("b", 7, "e", 4)], 9),
        ],
        vec![],
    );
    let page = pull_all(&host, &[("a", 0), ("b", 5)]).unwrap();
    assert_eq!(
        page.cursors["a"],
        axton_core::CursorRange {
            from: 0,
            to: 2,
            head: 2
        }
    );
    assert_eq!(
        page.cursors["b"],
        axton_core::CursorRange {
            from: 5,
            to: 9,
            head: 9
        }
    );
    assert_eq!(page.changes.len(), 2, "e once, f once");
    assert_eq!(page.changes[0].identity["id"], "e");
    assert_eq!(page.changes[0].stamp, 4);
    assert_eq!(page.changes[0].state["text"], "t-e");
    let loads = host
        .log
        .lock()
        .unwrap()
        .iter()
        .filter(|r| matches!(r, HostRequest::Load { .. }))
        .count();
    assert_eq!(loads, 1, "one load per model for the whole page");
}

/// A channel whose scan filled the page stops at its last row below the head
/// and continues; the other channel reaches its head.
#[test]
fn a_full_channel_continues_independently_of_the_others() {
    let rows: Vec<Value> = (1..=50)
        .map(|c| scan_row("a", c, &format!("r{c}"), 1))
        .collect();
    let host = multi(&[("a", rows, 80), ("b", vec![], 3)], vec![]);
    let page = pull_all(&host, &[("a", 0), ("b", 3)]).unwrap();
    assert_eq!(
        page.cursors["a"],
        axton_core::CursorRange {
            from: 0,
            to: 50,
            head: 80
        }
    );
    assert!(page.cursors["a"].continues());
    assert_eq!(
        page.cursors["b"],
        axton_core::CursorRange {
            from: 3,
            to: 3,
            head: 3
        }
    );
    assert!(!page.cursors["b"].continues());
    assert_eq!(page.changes.len(), 50);
}

/// A loader that refuses a batched call is asked one identity at a time; the
/// refused record becomes an error change, the others get their rows.
#[test]
fn a_loader_refusal_isolates_one_record_after_a_per_identity_retry() {
    let host = multi(
        &[(
            "a",
            vec![scan_row("a", 1, "e", 2), scan_row("a", 2, "g", 5)],
            2,
        )],
        vec![
            json!({"rejection":"entry.forbidden"}), // the batched call
            json!([{"id":"e","text":"t-e"}]),       // e alone
            json!({"rejection":"entry.forbidden"}), // g alone
        ],
    );
    let page = pull_all(&host, &[("a", 0)]).unwrap();
    assert_eq!(page.changes.len(), 2);
    assert_eq!(page.changes[0].state["text"], "t-e");
    assert!(!page.changes[0].is_error());
    assert_eq!(page.changes[1].error.as_deref(), Some("entry.forbidden"));
    assert_eq!(
        page.changes[1].stamp, 5,
        "the failed record keeps its current stamp"
    );
    assert!(page.changes[1].state.is_null());
    let loads: Vec<usize> = host
        .log
        .lock()
        .unwrap()
        .iter()
        .filter_map(|r| match r {
            HostRequest::Load { identities, .. } => Some(identities.len()),
            _ => None,
        })
        .collect();
    assert_eq!(
        loads,
        vec![2, 1, 1],
        "one batched call, then one per identity"
    );
}

/// A thrown loader error answered as a failure isolates the same way, coded
/// `loader.failed`; a single-record call needs no retry.
#[test]
fn a_loader_failure_is_an_error_change_and_a_single_record_needs_no_retry() {
    let host = multi(
        &[("a", vec![scan_row("a", 1, "e", 2)], 1)],
        vec![json!({"error":"boom"})],
    );
    let page = pull_all(&host, &[("a", 0)]).unwrap();
    assert_eq!(page.changes[0].error.as_deref(), Some("loader.failed"));
    let loads = host
        .log
        .lock()
        .unwrap()
        .iter()
        .filter(|r| matches!(r, HostRequest::Load { .. }))
        .count();
    assert_eq!(loads, 1);
}

/// A row the served contract does not accept fails only its record, coded
/// `loader.invalid`; the other records of the same batched call keep their rows.
#[test]
fn a_malformed_loader_row_fails_only_its_record() {
    let host = multi(
        &[(
            "a",
            vec![scan_row("a", 1, "e", 2), scan_row("a", 2, "g", 5)],
            2,
        )],
        vec![json!([{"id":"e","text":"t-e"},{"id":"g","text":7}])],
    );
    let page = pull_all(&host, &[("a", 0)]).unwrap();
    assert_eq!(page.changes[0].state["text"], "t-e");
    assert_eq!(page.changes[1].error.as_deref(), Some("loader.invalid"));
    assert_eq!(page.changes[1].stamp, 5);
    assert_eq!(page.cursors["a"].to, 2, "the channel still advances");
}

/// A batched answer with the wrong number of rows cannot be matched to its
/// records: each is loaded on its own, and a single-identity answer of the
/// wrong length fails only that record.
#[test]
fn a_misaligned_loader_answer_is_retried_per_identity() {
    let host = multi(
        &[(
            "a",
            vec![scan_row("a", 1, "e", 2), scan_row("a", 2, "g", 5)],
            2,
        )],
        vec![
            json!([{"id":"e","text":"t-e"}]), // the batched call: one row for two records
            json!([{"id":"e","text":"t-e"}]), // e alone
            json!([]),                        // g alone: misaligned
        ],
    );
    let page = pull_all(&host, &[("a", 0)]).unwrap();
    assert_eq!(page.changes[0].state["text"], "t-e");
    assert_eq!(page.changes[1].error.as_deref(), Some("loader.invalid"));
    let loads: Vec<usize> = host
        .log
        .lock()
        .unwrap()
        .iter()
        .filter_map(|r| match r {
            HostRequest::Load { identities, .. } => Some(identities.len()),
            _ => None,
        })
        .collect();
    assert_eq!(loads, vec![2, 1, 1]);
}

/// A host error (not a failure answer) still fails the request: nothing about
/// a broken transaction is a record's fault.
#[test]
fn a_thrown_host_error_still_fails_the_pull() {
    struct Throws;
    impl Host for Throws {
        fn call(
            &self,
            r: Value,
        ) -> Pin<Box<dyn Future<Output = axton_server::HostResult<Value>> + Send + '_>> {
            Box::pin(async move {
                let request: HostRequest = serde_json::from_value(r).map_err(|e| e.to_string())?;
                Ok(match request {
                    HostRequest::Head { .. } => json!(1),
                    HostRequest::Scan { .. } => json!([scan_row("a", 1, "e", 2)]),
                    HostRequest::Load { .. } => return Err("connection reset".into()),
                    other => return Err(format!("unsupported {}", other.label())),
                })
            })
        }
    }
    let request = axton_core::PullRequest {
        cursors: [("a".to_string(), 0)].into(),
        models: [("Entry".to_string(), 1)].into(),
    }
    .encode()
    .unwrap();
    let err = run(axton_server::process_pull(
        &config(),
        "u",
        &request,
        &Throws,
    ))
    .unwrap_err();
    assert_eq!(err.code, axton_server::code::HOST);
}

/// A cursor past a channel's head is refused, naming the channel.
#[test]
fn a_cursor_ahead_of_its_channel_head_is_refused() {
    let host = multi(&[("a", vec![], 2), ("b", vec![], 9)], vec![]);
    let err = pull_all(&host, &[("a", 3), ("b", 0)]).unwrap_err();
    assert_eq!(err.code, axton_server::code::REQUEST_INVALID);
    assert!(err.message.contains("on a"), "{err}");
}
