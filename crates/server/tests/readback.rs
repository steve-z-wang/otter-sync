//! Authoritative readback in a push: each successful mutation settles the
//! records it changed (one stamp each, distributed to their Channels), reads
//! its uploaded targets back through the loaders at the declared version
//! inside its own savepoint, and the receipt carries the last successful
//! authority per target record. Extra handler changes are settled, not read.
use axton_core::{PushReceipt, RecordKey};
use axton_server::{Config, Host, HostResult, code, host::HostRequest};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
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

fn field(name: &str, nullable: bool) -> Value {
    json!({"name":name,"nullable":nullable,"type":{"kind":"scalar","name":"string"}})
}
fn mutations() -> Value {
    json!([
        {"name":"create","version":1,"slots":[{"name":"entry","model":"Entry","operation":"create","cardinality":"single"}]},
        {"name":"edit","version":1,"slots":[{"name":"entry","model":"Entry","operation":"update","cardinality":"single","allowedPatchFields":["text"]}]},
        {"name":"editMany","version":1,"slots":[{"name":"entries","model":"Entry","operation":"update","cardinality":"list","allowedPatchFields":["text"]}]},
        {"name":"remove","version":1,"slots":[{"name":"entry","model":"Entry","operation":"delete","cardinality":"single"}]}
    ])
}
/// One model Entry {id, text} with create, edit, editMany and remove mutations.
fn config() -> Config {
    Config::decode(json!({
        "schema":{"enums":[],"models":[{"name":"Entry","identity":["id"],"fields":[field("id",false),field("text",false)]}]},
        "loaders":["Entry"],
        "mutations":mutations()
    }))
    .unwrap()
}
/// Entry plus a second model Note {id}, loaded by `loaders`.
fn config_with_note(loaders: &[&str]) -> Config {
    Config::decode(json!({
        "schema":{"enums":[],"models":[
            {"name":"Entry","identity":["id"],"fields":[field("id",false),field("text",false)]},
            {"name":"Note","identity":["id"],"fields":[field("id",false)]}]},
        "loaders":loaders,
        "mutations":mutations()
    }))
    .unwrap()
}
/// Entry at schema version 2 {id, text, note} retaining v1 {id, text} as well.
fn config_with_versions() -> Config {
    let mut c = json!({
        "schema":{"enums":[],"models":[{"name":"Entry","version":2,"identity":["id"],"fields":[field("id",false),field("text",false),field("note",true)]}]},
        "loaders":["Entry"],
        "mutations":mutations()
    });
    c["models"] = json!([
        {"name":"Entry","version":1,"identity":["id"],"enums":[],"fields":[field("id",false),field("text",false)]},
        {"name":"Entry","version":2,"identity":["id"],"enums":[],"fields":[field("id",false),field("text",false),field("note",true)]}
    ]);
    Config::decode(c).unwrap()
}

fn create(ordinal: u64, id: &str, text: &str) -> Value {
    json!({"ordinal":ordinal,"name":"create","operations":[{"model":"Entry","op":"create","identity":{"id":id},"values":{"text":text}}]})
}
fn edit(ordinal: u64, id: &str, text: &str) -> Value {
    json!({"ordinal":ordinal,"name":"edit","operations":[{"model":"Entry","op":"update","identity":{"id":id},"values":{"text":text}}]})
}
fn remove(ordinal: u64, id: &str) -> Value {
    json!({"ordinal":ordinal,"name":"remove","operations":[{"model":"Entry","op":"delete","identity":{"id":id}}]})
}
fn push_declaring(client: &str, sequence: u64, models: Value, mutations: Vec<Value>) -> Vec<u8> {
    json!({"clientId":client,"batchSequence":sequence,"models":models,"mutations":mutations})
        .to_string()
        .into_bytes()
}
fn push(sequence: u64, mutations: Vec<Value>) -> Vec<u8> {
    push_declaring("c", sequence, json!({"Entry":1}), mutations)
}
fn key(model: &str, id: &str) -> RecordKey {
    RecordKey {
        model: model.into(),
        identity: json!({ "id": id }),
    }
}
fn record(model: &str, id: &str) -> Value {
    json!({"model":model,"identity":{"id":id}})
}
fn settled(changes: Value, memberships: Value) -> Value {
    json!({"changes":changes,"memberships":memberships})
}
fn add(channel: &str, model: &str, id: &str) -> Value {
    json!({"channel":channel,"model":model,"identity":{"id":id},"present":true})
}
fn decode(text: &str) -> PushReceipt {
    PushReceipt::decode(text.as_bytes()).unwrap()
}

/// Everything a savepoint isolates: business rows, record stamps, channel
/// heads and persistent memberships.
#[derive(Default, Clone)]
struct Store {
    business: BTreeMap<String, Value>,
    stamps: BTreeMap<String, u64>,
    heads: BTreeMap<String, u64>,
    /// `(stamp key, channel)`.
    members: BTreeSet<(String, String)>,
}
type Answer = HostResult<Value>;
#[derive(Default)]
struct State {
    store: Store,
    /// Per client: last accepted sequence and its receipt.
    clients: BTreeMap<String, (u64, Option<String>)>,
    savepoints: Vec<(u64, Store)>,
    log: Vec<HostRequest>,
    loads: usize,
    // The script.
    /// The settlement `handle` answers per ordinal; default is a quiet success.
    settlements: BTreeMap<u64, Value>,
    /// Extra business writes the handler makes per ordinal, beyond the uploaded operations.
    writes: BTreeMap<u64, Vec<(RecordKey, Option<Value>)>>,
    /// Whether `handle` applies the uploaded operations to the business table.
    skip_operations: bool,
    /// Answers for the n-th `load` call (0-based) instead of the table.
    load_overrides: BTreeMap<usize, Answer>,
    /// A fixed answer for every `publish` instead of echoing the stamp.
    publish_answer: Option<Answer>,
    /// The fields a loader of this version returns; others are the whole row.
    load_fields: BTreeMap<u64, Vec<String>>,
    /// When set, `rollback` throws instead of restoring the savepoint.
    fail_rollback: bool,
}
/// A scripted in-memory host: a business table keyed by encoded record key,
/// stamp counters, channel heads, a savepoint stack, stored receipts and a log
/// of every request it received.
struct Scripted {
    state: Mutex<State>,
}
fn stamp_key(model: &str, identity_key: &str) -> String {
    format!("{model} {identity_key}")
}
impl Scripted {
    fn new() -> Self {
        Self {
            state: Mutex::new(State::default()),
        }
    }
    fn with<T>(&self, f: impl FnOnce(&mut State) -> T) -> T {
        f(&mut self.state.lock().unwrap())
    }
    fn seed(&self, model: &str, id: &str, row: Value) {
        let encoded = key(model, id).encoded().unwrap();
        self.with(|s| s.store.business.insert(encoded, row));
    }
    fn settle(&self, ordinal: u64, settlement: Value) {
        self.with(|s| s.settlements.insert(ordinal, settlement));
    }
    fn write(&self, ordinal: u64, model: &str, id: &str, row: Option<Value>) {
        self.with(|s| {
            s.writes
                .entry(ordinal)
                .or_default()
                .push((key(model, id), row))
        });
    }
    fn answer_load(&self, index: usize, answer: Answer) {
        self.with(|s| s.load_overrides.insert(index, answer));
    }
    fn answer_publish(&self, answer: Answer) {
        self.with(|s| s.publish_answer = Some(answer));
    }
    fn log(&self) -> Vec<HostRequest> {
        self.with(|s| s.log.clone())
    }
    fn labels(&self) -> Vec<String> {
        self.log().iter().map(HostRequest::label).collect()
    }
    fn count(&self, op: &str) -> usize {
        self.labels().iter().filter(|l| l.starts_with(op)).count()
    }
    fn business(&self, model: &str, id: &str) -> Option<Value> {
        let encoded = key(model, id).encoded().unwrap();
        self.with(|s| s.store.business.get(&encoded).cloned())
    }
    fn stamp(&self, model: &str, id: &str) -> Option<u64> {
        let identity_key = key(model, id).encoded_identity().unwrap();
        self.with(|s| {
            s.store
                .stamps
                .get(&stamp_key(model, &identity_key))
                .copied()
        })
    }
    /// Apply one mutation's decoded slot arguments to the business table.
    fn apply(store: &mut Store, arguments: &Value) {
        let rows = arguments
            .as_object()
            .into_iter()
            .flat_map(|slots| slots.values())
            .flat_map(|slot| match slot {
                Value::Array(rows) => rows.clone(),
                Value::Null => vec![],
                row => vec![row.clone()],
            });
        for row in rows {
            let key = RecordKey {
                model: "Entry".into(),
                identity: row["identity"].clone(),
            };
            let encoded = key.encoded().unwrap();
            let identity = row["identity"].as_object().cloned().unwrap_or_default();
            if let Some(data) = row.get("data").and_then(Value::as_object) {
                let mut full = identity;
                full.extend(data.clone());
                store.business.insert(encoded, Value::Object(full));
            } else if let Some(patch) = row.get("patch").and_then(Value::as_object) {
                let mut full = store
                    .business
                    .get(&encoded)
                    .and_then(Value::as_object)
                    .cloned()
                    .unwrap_or(identity);
                full.extend(patch.clone());
                store.business.insert(encoded, Value::Object(full));
            } else {
                store.business.remove(&encoded);
            }
        }
    }
    fn answer(&self, raw: Value) -> Answer {
        let request: HostRequest = serde_json::from_value(raw)
            .map_err(|error| format!("unsupported host request: {error}"))?;
        let mut s = self.state.lock().unwrap();
        s.log.push(request.clone());
        Ok(match request {
            HostRequest::Claim { owner, client_id } => {
                let (sequence, receipt) = s.clients.get(&client_id).cloned().unwrap_or((0, None));
                json!({"clientId":client_id,"owner":owner,"sequence":sequence,"receipt":receipt})
            }
            HostRequest::ClaimCall { .. }
            | HostRequest::SaveCall { .. }
            | HostRequest::HandleAction { .. } => {
                return Err("call persistence is outside this readback host".into());
            }
            HostRequest::SaveReceipt {
                client_id,
                sequence,
                receipt,
                ..
            } => {
                s.clients.insert(client_id, (sequence, Some(receipt)));
                Value::Null
            }
            HostRequest::Head { channel } => {
                json!(s.store.heads.get(&channel).copied().unwrap_or(0))
            }
            HostRequest::Scan { .. } => json!([]),
            HostRequest::Savepoint { ordinal } => {
                let snapshot = s.store.clone();
                s.savepoints.push((ordinal, snapshot));
                Value::Null
            }
            HostRequest::Rollback { ordinal } => {
                if s.fail_rollback {
                    return Err("rollback failed".into());
                }
                // Like SQL `ROLLBACK TO SAVEPOINT`: restores the snapshot and
                // keeps the savepoint open for the `release` that follows.
                let (opened, snapshot) = s.savepoints.last().expect("no savepoint to roll back");
                assert_eq!(
                    *opened, ordinal,
                    "rollback of a savepoint that is not the innermost"
                );
                s.store = snapshot.clone();
                Value::Null
            }
            HostRequest::Release { ordinal } => {
                let (opened, _) = s.savepoints.pop().expect("no savepoint to release");
                assert_eq!(
                    opened, ordinal,
                    "release of a savepoint that is not the innermost"
                );
                Value::Null
            }
            HostRequest::Handle {
                arguments, ordinal, ..
            } => {
                if !s.skip_operations {
                    Self::apply(&mut s.store, &arguments);
                }
                for (key, row) in s.writes.get(&ordinal).cloned().unwrap_or_default() {
                    let encoded = key.encoded().unwrap();
                    match row {
                        Some(row) => s.store.business.insert(encoded, row),
                        None => s.store.business.remove(&encoded),
                    };
                }
                s.settlements
                    .get(&ordinal)
                    .cloned()
                    .unwrap_or_else(|| settled(json!([]), json!([])))
            }
            HostRequest::Load {
                model,
                version,
                identities,
                ..
            } => {
                let index = s.loads;
                s.loads += 1;
                if let Some(answer) = s.load_overrides.remove(&index) {
                    return answer;
                }
                // A loader of an older version returns rows of that version's shape.
                let fields = s.load_fields.get(&version).cloned();
                Value::Array(
                    identities
                        .iter()
                        .map(|identity| {
                            let key = RecordKey {
                                model: model.clone(),
                                identity: identity.clone(),
                            };
                            let row = s.store.business.get(&key.encoded().unwrap()).cloned();
                            match (row, &fields) {
                                (Some(Value::Object(row)), Some(fields)) => Value::Object(
                                    row.into_iter()
                                        .filter(|(name, _)| fields.contains(name))
                                        .collect(),
                                ),
                                (Some(row), _) => row,
                                (None, _) => Value::Null,
                            }
                        })
                        .collect(),
                )
            }
            HostRequest::AdvanceStamp {
                model,
                identity_key,
            } => {
                let stamp = s
                    .store
                    .stamps
                    .entry(stamp_key(&model, &identity_key))
                    .or_insert(0);
                *stamp += 1;
                json!(*stamp)
            }
            HostRequest::EnsureStamp {
                model,
                identity_key,
            } => json!(
                *s.store
                    .stamps
                    .entry(stamp_key(&model, &identity_key))
                    .or_insert(1)
            ),
            HostRequest::Publish { channel, stamp, .. } => {
                if let Some(answer) = s.publish_answer.clone() {
                    return answer;
                }
                let head = s.store.heads.entry(channel).or_insert(0);
                *head += 1;
                json!({"cursor":*head,"stamp":stamp})
            }
            HostRequest::LockRecord {
                model,
                identity_key,
            } => json!(
                s.store
                    .stamps
                    .get(&stamp_key(&model, &identity_key))
                    .copied()
            ),
            HostRequest::Memberships {
                model,
                identity_key,
            } => {
                let record = stamp_key(&model, &identity_key);
                json!(
                    s.store
                        .members
                        .iter()
                        .filter(|(member, _)| *member == record)
                        .map(|(_, channel)| channel.clone())
                        .collect::<Vec<_>>()
                )
            }
            HostRequest::SetMembership {
                channel,
                model,
                identity_key,
                present,
            } => {
                let record = stamp_key(&model, &identity_key);
                if present {
                    if !s.store.stamps.contains_key(&record) {
                        return Err(format!("{record} has no metadata to enroll"));
                    }
                    s.store.heads.entry(channel.clone()).or_insert(0);
                    s.store.members.insert((record, channel));
                } else {
                    s.store.members.remove(&(record, channel));
                }
                Value::Null
            }
        })
    }
}
impl Host for Scripted {
    fn call(&self, raw: Value) -> Pin<Box<dyn Future<Output = HostResult<Value>> + Send + '_>> {
        let answer = self.answer(raw);
        Box::pin(async move { answer })
    }
}

/// A push against `config` by owner `u`.
fn process(config: &Config, body: &[u8], host: &Scripted) -> axton_server::Result<String> {
    run(axton_server::process_push(config, "u", body, host))
}

/// Every `publish` in the log carries the stamp the record currently has.
fn assert_publishes_carry_current_stamps(host: &Scripted) {
    for request in host.log() {
        if let HostRequest::Publish {
            model,
            identity_key,
            stamp,
            ..
        } = request
        {
            let current = host.with(|s| {
                s.store
                    .stamps
                    .get(&stamp_key(&model, &identity_key))
                    .copied()
            });
            assert_eq!(current, Some(stamp), "publish of {model} {identity_key}");
        }
    }
}

/// A success reads back each changed record once at its allocated stamp with
/// the loader's normalized state, in the order stamp, membership, load; a
/// record in no Channel is published nowhere.
#[test]
fn success_reads_back_each_changed_record_once_at_its_stamp() {
    let host = Scripted::new();
    host.seed("Entry", "a", json!({"id":"a","text":"old"}));
    let text = process(&config(), &push(1, vec![edit(1, "a", "typed")]), &host).unwrap();
    let receipt = decode(&text);
    assert!(receipt.answers("c", 1));
    assert!(receipt.rejections.is_empty());
    assert_eq!(
        receipt.records,
        vec![axton_core::AuthorityRecord {
            model: "Entry".into(),
            identity: json!({"id":"a"}),
            stamp: 1,
            state: json!({"text":"typed"}),
            error: None,
        }]
    );
    assert_eq!(
        host.labels(),
        [
            "claim",
            "savepoint(ordinal 1)",
            "handle(ordinal 1)",
            "advanceStamp",
            "memberships",
            "load",
            "release(ordinal 1)",
            "saveReceipt"
        ]
    );
    let HostRequest::Load {
        model,
        version,
        identities,
        owner,
    } = &host.log()[5]
    else {
        panic!("not a load");
    };
    assert_eq!(
        (model.as_str(), *version, owner.as_str()),
        ("Entry", 1, "u")
    );
    assert_eq!(*identities, vec![json!({"id":"a"})]);
    assert_eq!(host.count("publish"), 0);
    // A second batch advances the same record's stamp.
    let text = process(&config(), &push(2, vec![edit(1, "a", "again")]), &host).unwrap();
    assert_eq!(decode(&text).records[0].stamp, 2);
}

/// Two operations on one record in one mutation share one stamp and one row.
#[test]
fn repeated_operations_on_one_record_share_one_stamp() {
    let host = Scripted::new();
    host.seed("Entry", "a", json!({"id":"a","text":"old"}));
    let body = push(
        1,
        vec![json!({"ordinal":1,"name":"editMany","operations":[
            {"model":"Entry","op":"update","identity":{"id":"a"},"values":{"text":"first"}},
            {"model":"Entry","op":"update","identity":{"id":"a"},"values":{"text":"second"}}]})],
    );
    let receipt = decode(&process(&config(), &body, &host).unwrap());
    assert_eq!(receipt.records.len(), 1);
    assert_eq!(receipt.records[0].stamp, 1);
    assert_eq!(receipt.records[0].state, json!({"text":"second"}));
    assert_eq!(host.count("advanceStamp"), 1);
    assert_eq!(host.count("load"), 1);
}

/// Two mutations in one batch touching the same record: the receipt carries
/// only the later result at the later stamp.
#[test]
fn a_later_mutation_on_the_same_record_replaces_the_earlier_result() {
    let host = Scripted::new();
    let body = push(1, vec![create(1, "a", "new"), edit(2, "a", "edited")]);
    let receipt = decode(&process(&config(), &body, &host).unwrap());
    assert!(receipt.rejections.is_empty());
    assert_eq!(receipt.records.len(), 1);
    assert_eq!(receipt.records[0].stamp, 2);
    assert_eq!(receipt.records[0].state, json!({"text":"edited"}));
    assert_eq!(host.count("advanceStamp"), 2);
}

/// Records the handler adds via `changes` are settled (stamped and
/// distributed) but not read back: only the uploaded target is the caller's
/// authority. A record the handler only enrolls gets `ensureStamp` and is
/// published at that stamp, and is absent from the receipt.
#[test]
fn handler_changes_are_settled_not_read_back_and_enrolled_records_publish_at_their_stamp() {
    let host = Scripted::new();
    host.seed("Entry", "a", json!({"id":"a","text":"old"}));
    host.write(
        1,
        "Entry",
        "b",
        Some(json!({"id":"b","text":"from handler"})),
    );
    host.settle(
        1,
        settled(
            json!([record("Entry", "b")]),
            json!([add("shared", "Entry", "c")]),
        ),
    );
    let receipt =
        decode(&process(&config(), &push(1, vec![edit(1, "a", "typed")]), &host).unwrap());
    assert_eq!(
        receipt
            .records
            .iter()
            .map(|r| (r.identity["id"].as_str().unwrap(), r.stamp, r.state.clone()))
            .collect::<Vec<_>>(),
        [("a", 1, json!({"text":"typed"}))]
    );
    assert_eq!(
        host.stamp("Entry", "b"),
        Some(1),
        "b is settled all the same"
    );
    assert_eq!(
        host.labels(),
        [
            "claim",
            "savepoint(ordinal 1)",
            "handle(ordinal 1)",
            "advanceStamp",
            "advanceStamp",
            "ensureStamp",
            "memberships",
            "memberships",
            "memberships",
            "setMembership",
            "publish",
            "load",
            "release(ordinal 1)",
            "saveReceipt"
        ]
    );
    let log = host.log();
    assert!(
        matches!(&log[3], HostRequest::AdvanceStamp { identity_key, .. } if identity_key == r#"{"id":"a"}"#)
    );
    assert!(
        matches!(&log[4], HostRequest::AdvanceStamp { identity_key, .. } if identity_key == r#"{"id":"b"}"#)
    );
    assert!(
        matches!(&log[5], HostRequest::EnsureStamp { identity_key, .. } if identity_key == r#"{"id":"c"}"#)
    );
    assert_eq!(
        log[10],
        HostRequest::Publish {
            channel: "shared".into(),
            model: "Entry".into(),
            identity: json!({"id":"c"}),
            identity_key: r#"{"id":"c"}"#.into(),
            stamp: 1,
        }
    );
    assert!(
        matches!(&log[11], HostRequest::Load { identities, .. } if *identities == vec![json!({"id":"a"})])
    );
    assert_publishes_carry_current_stamps(&host);
}

/// A handler that enrolls its changed records publishes each at the stamp it
/// was allocated; there is no implicit publication of the change set.
#[test]
fn enrolled_changes_publish_at_their_allocated_stamps() {
    let host = Scripted::new();
    host.seed("Entry", "a", json!({"id":"a","text":"old"}));
    host.write(
        1,
        "Entry",
        "b",
        Some(json!({"id":"b","text":"from handler"})),
    );
    host.settle(
        1,
        settled(
            json!([record("Entry", "b")]),
            json!([add("shared", "Entry", "a"), add("shared", "Entry", "b")]),
        ),
    );
    // Record b has been stamped before: its next stamp is 4, not 1.
    host.with(|s| {
        s.store
            .stamps
            .insert(stamp_key("Entry", r#"{"id":"b"}"#), 3)
    });
    let receipt =
        decode(&process(&config(), &push(1, vec![edit(1, "a", "typed")]), &host).unwrap());
    assert_eq!(
        receipt.records.iter().map(|r| r.stamp).collect::<Vec<_>>(),
        [1]
    );
    let published: Vec<(String, u64)> = host
        .log()
        .into_iter()
        .filter_map(|r| match r {
            HostRequest::Publish {
                channel,
                identity_key,
                stamp,
                ..
            } => {
                assert_eq!(channel, "shared");
                Some((identity_key, stamp))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        published,
        [
            (r#"{"id":"a"}"#.to_string(), 1),
            (r#"{"id":"b"}"#.to_string(), 4)
        ]
    );
    assert_eq!(host.count("ensureStamp"), 0);
    assert_publishes_carry_current_stamps(&host);
    // A later change of the enrolled record reaches the Channel again without
    // any declaration.
    host.settle(1, settled(json!([]), json!([])));
    let receipt =
        decode(&process(&config(), &push(2, vec![edit(1, "a", "again")]), &host).unwrap());
    assert_eq!(receipt.records[0].stamp, 2);
    assert_eq!(host.count("publish"), 3);
    assert!(matches!(
        host.log().iter().rfind(|r| matches!(r, HostRequest::Publish { .. })),
        Some(HostRequest::Publish { identity_key, stamp: 2, .. }) if identity_key == r#"{"id":"a"}"#
    ));
}

/// A change with no membership publishes nothing and creates no Channel.
#[test]
fn a_change_without_membership_publishes_nothing() {
    let host = Scripted::new();
    host.seed("Entry", "a", json!({"id":"a","text":"old"}));
    host.settle(1, settled(json!([]), json!([])));
    let receipt =
        decode(&process(&config(), &push(1, vec![edit(1, "a", "typed")]), &host).unwrap());
    assert_eq!(receipt.records.len(), 1);
    assert_eq!(host.count("publish"), 0);
    assert_eq!(host.count("ensureStamp"), 0);
    assert_eq!(host.with(|s| s.store.heads.len()), 0);
}

/// A publish answer whose stamp is not the one the engine named fails the push
/// with `host.invalid`.
#[test]
fn a_publish_that_echoes_another_stamp_is_host_invalid() {
    let host = Scripted::new();
    host.seed("Entry", "a", json!({"id":"a","text":"old"}));
    host.settle(1, settled(json!([]), json!([add("shared", "Entry", "a")])));
    host.answer_publish(Ok(json!({"cursor":1,"stamp":99})));
    let err = process(&config(), &push(1, vec![edit(1, "a", "typed")]), &host).unwrap_err();
    assert_eq!(err.code, code::HOST_INVALID, "{err}");
    assert!(err.message.contains("publish"), "{err}");
    assert_eq!(host.count("saveReceipt"), 0);
}

/// A handler that changes nothing beyond the operations still gets a new stamp
/// for each operation target and the record is read back as it is.
#[test]
fn a_quiet_success_still_advances_the_stamp_and_reads_back() {
    let host = Scripted::new();
    host.seed("Entry", "a", json!({"id":"a","text":"untouched"}));
    host.with(|s| {
        s.skip_operations = true;
        s.store
            .stamps
            .insert(stamp_key("Entry", r#"{"id":"a"}"#), 4);
    });
    let receipt =
        decode(&process(&config(), &push(1, vec![edit(1, "a", "ignored")]), &host).unwrap());
    assert_eq!(receipt.records.len(), 1);
    assert_eq!(receipt.records[0].stamp, 5);
    assert_eq!(receipt.records[0].state, json!({"text":"untouched"}));
    assert_eq!(host.stamp("Entry", "a"), Some(5));
}

/// A deleted record reads back as `null` state at its new stamp.
#[test]
fn a_deleted_record_reads_back_as_null() {
    let host = Scripted::new();
    host.seed("Entry", "a", json!({"id":"a","text":"old"}));
    let receipt = decode(&process(&config(), &push(1, vec![remove(1, "a")]), &host).unwrap());
    assert_eq!(receipt.records.len(), 1);
    assert_eq!(receipt.records[0].identity, json!({"id":"a"}));
    assert_eq!(receipt.records[0].stamp, 1);
    assert_eq!(receipt.records[0].state, Value::Null);
    assert_eq!(host.business("Entry", "a"), None);
}

/// Each record is loaded by the loader of the version the request declared
/// and normalized with that version's retained contract.
#[test]
fn records_are_loaded_at_the_declared_version() {
    let config = config_with_versions();
    let host = Scripted::new();
    host.with(|s| s.load_fields.insert(1, vec!["id".into(), "text".into()]));
    host.seed("Entry", "a", json!({"id":"a","text":"old","note":"kept"}));
    let receipt = decode(
        &process(
            &config,
            &push_declaring("c", 1, json!({"Entry":1}), vec![edit(1, "a", "typed")]),
            &host,
        )
        .unwrap(),
    );
    assert_eq!(receipt.records[0].state, json!({"text":"typed"}));
    assert!(matches!(
        &host.log()[5],
        HostRequest::Load { version: 1, .. }
    ));
    let receipt = decode(
        &process(
            &config,
            &push_declaring("c", 2, json!({"Entry":2}), vec![edit(1, "a", "again")]),
            &host,
        )
        .unwrap(),
    );
    assert_eq!(
        receipt.records[0].state,
        json!({"text":"again","note":"kept"})
    );
    assert_eq!(receipt.records[0].stamp, 2);
    let versions: Vec<u64> = host
        .log()
        .into_iter()
        .filter_map(|r| match r {
            HostRequest::Load { version, .. } => Some(version),
            _ => None,
        })
        .collect();
    assert_eq!(versions, [1, 2]);
}

/// A handler's extra change of a model the client did not declare is settled
/// without caller authority: the mutation succeeds and the Note is never
/// loaded for this caller. An uploaded target whose model the client did not
/// declare still rejects its mutation with `model_version_unsupported` and
/// rolls it back; the rest of the batch stands.
#[test]
fn an_undeclared_extra_change_is_settled_but_an_undeclared_target_rejects_its_mutation() {
    let config = config_with_note(&["Entry", "Note"]);
    let host = Scripted::new();
    host.seed("Entry", "a", json!({"id":"a","text":"old"}));
    host.seed("Entry", "b", json!({"id":"b","text":"old"}));
    host.write(1, "Note", "n", Some(json!({"id":"n"})));
    host.settle(1, settled(json!([record("Note", "n")]), json!([])));
    let body = push_declaring(
        "c",
        1,
        json!({"Entry":1}),
        vec![edit(1, "a", "typed"), edit(2, "b", "kept")],
    );
    let receipt = decode(&process(&config, &body, &host).unwrap());
    assert!(receipt.rejections.is_empty());
    assert_eq!(
        receipt
            .records
            .iter()
            .map(|r| (r.model.as_str(), r.identity["id"].as_str().unwrap()))
            .collect::<Vec<_>>(),
        [("Entry", "a"), ("Entry", "b")]
    );
    assert_eq!(host.stamp("Note", "n"), Some(1));
    assert_eq!(host.business("Note", "n"), Some(json!({"id":"n"})));
    assert!(
        host.log()
            .iter()
            .all(|r| !matches!(r, HostRequest::Load { model, .. } if model == "Note"))
    );
    // Declaring only Note, the Entry target cannot be served.
    let host = Scripted::new();
    host.seed("Entry", "a", json!({"id":"a","text":"old"}));
    let body = push_declaring("c", 1, json!({"Note":1}), vec![edit(1, "a", "lost")]);
    let receipt = decode(&process(&config, &body, &host).unwrap());
    assert_eq!(
        receipt.rejections,
        vec![axton_core::Rejection {
            ordinal: 1,
            code: code::MODEL_VERSION_UNSUPPORTED.into()
        }]
    );
    assert!(receipt.records.is_empty());
    assert_eq!(host.count("load"), 0);
    assert_eq!(
        host.business("Entry", "a"),
        Some(json!({"id":"a","text":"old"}))
    );
    assert_eq!(host.stamp("Entry", "a"), None, "the stamp rolled back");
}

/// A loader refusal rejects that mutation with its code and rolls it back; an
/// earlier successful mutation keeps its result.
#[test]
fn a_loader_refusal_rejects_the_mutation_and_keeps_earlier_results() {
    let host = Scripted::new();
    host.seed("Entry", "a", json!({"id":"a","text":"old"}));
    host.seed("Entry", "b", json!({"id":"b","text":"old"}));
    host.answer_load(1, Ok(json!({"rejection":"task.forbidden"})));
    let body = push(1, vec![edit(1, "a", "kept"), edit(2, "b", "lost")]);
    let receipt = decode(&process(&config(), &body, &host).unwrap());
    assert_eq!(
        receipt.rejections,
        vec![axton_core::Rejection {
            ordinal: 2,
            code: "task.forbidden".into()
        }]
    );
    assert_eq!(receipt.records.len(), 1);
    assert_eq!(receipt.records[0].identity, json!({"id":"a"}));
    assert_eq!(receipt.records[0].stamp, 1);
    assert_eq!(receipt.records[0].state, json!({"text":"kept"}));
    let labels = host.labels();
    assert_eq!(
        &labels[7..],
        [
            "savepoint(ordinal 2)",
            "handle(ordinal 2)",
            "advanceStamp",
            "memberships",
            "load",
            "rollback(ordinal 2)",
            "release(ordinal 2)",
            "saveReceipt"
        ]
    );
    assert_eq!(
        host.business("Entry", "b"),
        Some(json!({"id":"b","text":"old"}))
    );
    assert_eq!(
        host.stamp("Entry", "b"),
        None,
        "the stamp allocation was rolled back"
    );
    assert_eq!(host.stamp("Entry", "a"), Some(1));
}

/// A loader that fails (a thrown host error) aborts the whole push under
/// `host`; no receipt is saved.
#[test]
fn a_loader_host_error_fails_the_whole_push() {
    let host = Scripted::new();
    host.seed("Entry", "a", json!({"id":"a","text":"old"}));
    host.answer_load(0, Err("loader exploded".into()));
    let err = process(&config(), &push(1, vec![edit(1, "a", "typed")]), &host).unwrap_err();
    assert_eq!(err.code, code::HOST);
    assert_eq!(err.message, "loader exploded");
    assert_eq!(host.count("saveReceipt"), 0);
    assert_eq!(host.count("handle"), 1, "the handler had already run");
}

/// Loader rows the contract cannot accept, or a misaligned row count, reject
/// only the mutation being read back, with `loader.invalid`.
#[test]
fn invalid_loader_rows_reject_only_their_mutation() {
    for rows in [
        json!([1]),
        json!([{"id":"a"}]),
        json!([{"id":"a","text":"t","extra":1}]),
        json!([]),
        json!([{"id":"a","text":"t"},{"id":"b","text":"t"}]),
    ] {
        let host = Scripted::new();
        host.seed("Entry", "a", json!({"id":"a","text":"old"}));
        host.answer_load(0, Ok(rows.clone()));
        let receipt =
            decode(&process(&config(), &push(1, vec![edit(1, "a", "typed")]), &host).unwrap());
        assert_eq!(
            receipt.rejections,
            vec![axton_core::Rejection {
                ordinal: 1,
                code: code::LOADER_INVALID.into()
            }],
            "{rows}"
        );
        assert!(receipt.records.is_empty(), "{rows}");
        assert_eq!(host.count("rollback"), 1, "{rows}");
    }
}

/// A success followed by a rejected mutation on the same record keeps the
/// first success's record and stamp in the receipt.
#[test]
fn a_later_rejection_on_the_same_record_keeps_the_first_success() {
    let host = Scripted::new();
    host.seed("Entry", "a", json!({"id":"a","text":"old"}));
    host.settle(2, json!({"rejection":"entry.stale"}));
    let body = push(1, vec![edit(1, "a", "kept"), edit(2, "a", "lost")]);
    let receipt = decode(&process(&config(), &body, &host).unwrap());
    assert_eq!(
        receipt.rejections,
        vec![axton_core::Rejection {
            ordinal: 2,
            code: "entry.stale".into()
        }]
    );
    assert_eq!(receipt.records.len(), 1);
    assert_eq!(receipt.records[0].stamp, 1);
    assert_eq!(receipt.records[0].state, json!({"text":"kept"}));
    assert_eq!(
        &host.labels()[7..],
        [
            "savepoint(ordinal 2)",
            "handle(ordinal 2)",
            "rollback(ordinal 2)",
            "release(ordinal 2)",
            "saveReceipt"
        ],
        "a rejected mutation is neither stamped nor loaded"
    );
    assert_eq!(
        host.business("Entry", "a"),
        Some(json!({"id":"a","text":"kept"}))
    );
    assert_eq!(host.stamp("Entry", "a"), Some(1));
}

/// A publication the host cannot carry out aborts the whole push under `host`.
#[test]
fn a_failed_publication_fails_the_push() {
    let host = Scripted::new();
    host.seed("Entry", "a", json!({"id":"a","text":"old"}));
    host.settle(1, settled(json!([]), json!([add("shared", "Entry", "a")])));
    host.answer_publish(Err("channel down".into()));
    let err = process(&config(), &push(1, vec![edit(1, "a", "typed")]), &host).unwrap_err();
    assert_eq!(err.code, code::HOST);
    assert_eq!(err.message, "channel down");
    assert_eq!(host.count("saveReceipt"), 0);
}

/// A retry of an accepted batch answers with the stored receipt bytes verbatim
/// and runs no handler, even after a later write changed the business state.
#[test]
fn a_retried_batch_answers_from_storage_without_running_a_handler() {
    let host = Scripted::new();
    host.seed("Entry", "a", json!({"id":"a","text":"old"}));
    let first = process(&config(), &push(1, vec![edit(1, "a", "one")]), &host).unwrap();
    // Another client changes the same record afterwards.
    let other = process(
        &config(),
        &push_declaring("d", 1, json!({"Entry":1}), vec![edit(1, "a", "two")]),
        &host,
    )
    .unwrap();
    assert_eq!(decode(&other).records[0].stamp, 2);
    assert_eq!(
        host.business("Entry", "a"),
        Some(json!({"id":"a","text":"two"}))
    );
    let before = host.labels();
    let retried = process(&config(), &push(1, vec![edit(1, "a", "one")]), &host).unwrap();
    assert_eq!(retried, first, "the stored bytes are returned verbatim");
    assert_eq!(decode(&retried).records[0].state, json!({"text":"one"}));
    let mut expected = before;
    expected.push("claim".into());
    assert_eq!(host.labels(), expected, "a retry only claims");
    assert_eq!(
        host.business("Entry", "a"),
        Some(json!({"id":"a","text":"two"}))
    );
}

/// A mutation at an unsupported version is rejected alone; its handler never
/// runs and the batch commits.
#[test]
fn an_unsupported_version_rejects_only_that_mutation() {
    let host = Scripted::new();
    host.seed("Entry", "a", json!({"id":"a","text":"old"}));
    host.seed("Entry", "b", json!({"id":"b","text":"old"}));
    let mut bad = edit(1, "a", "typed");
    bad["version"] = json!(9);
    let body = push(1, vec![bad, edit(2, "b", "kept")]);
    let receipt = decode(&process(&config(), &body, &host).unwrap());
    assert_eq!(
        receipt.rejections,
        vec![axton_core::Rejection {
            ordinal: 1,
            code: code::MUTATION_VERSION_UNSUPPORTED.into()
        }]
    );
    assert_eq!(receipt.records.len(), 1);
    assert_eq!(receipt.records[0].identity, json!({"id":"b"}));
    assert_eq!(host.count("handle"), 1, "ordinal 1's handler never ran");
    assert_eq!(host.count("saveReceipt"), 1);
}

/// A thrown handler error answered as `Failed` rejects only that mutation
/// with `handler.failed`.
#[test]
fn a_handler_failure_rejects_only_that_mutation() {
    let host = Scripted::new();
    host.seed("Entry", "a", json!({"id":"a","text":"old"}));
    host.seed("Entry", "b", json!({"id":"b","text":"old"}));
    host.settle(1, json!({"error":"boom"}));
    let body = push(1, vec![edit(1, "a", "lost"), edit(2, "b", "kept")]);
    let receipt = decode(&process(&config(), &body, &host).unwrap());
    assert_eq!(
        receipt.rejections,
        vec![axton_core::Rejection {
            ordinal: 1,
            code: code::HANDLER_FAILED.into()
        }]
    );
    assert_eq!(receipt.records.len(), 1);
    assert_eq!(receipt.records[0].identity, json!({"id":"b"}));
    assert_eq!(
        &host.labels()[..5],
        [
            "claim",
            "savepoint(ordinal 1)",
            "handle(ordinal 1)",
            "rollback(ordinal 1)",
            "release(ordinal 1)"
        ]
    );
}

/// A loader failure answered as `Failed` rejects the mutation with
/// `loader.failed` and rolls back its writes.
#[test]
fn a_loader_failure_rejects_only_that_mutation() {
    let host = Scripted::new();
    host.seed("Entry", "a", json!({"id":"a","text":"old"}));
    host.seed("Entry", "b", json!({"id":"b","text":"old"}));
    host.answer_load(0, Ok(json!({"error":"loader broke"})));
    let body = push(1, vec![edit(1, "a", "lost"), edit(2, "b", "kept")]);
    let receipt = decode(&process(&config(), &body, &host).unwrap());
    assert_eq!(
        receipt.rejections,
        vec![axton_core::Rejection {
            ordinal: 1,
            code: code::LOADER_FAILED.into()
        }]
    );
    assert_eq!(receipt.records.len(), 1);
    assert_eq!(receipt.records[0].identity, json!({"id":"b"}));
    assert_eq!(
        host.business("Entry", "a"),
        Some(json!({"id":"a","text":"old"})),
        "the loader failure's business write was rolled back"
    );
}

/// A rollback the host cannot perform fails the whole delivery and stores no
/// receipt; a retry of the same bytes runs the handlers again.
#[test]
fn a_failed_rollback_fails_the_delivery() {
    let host = Scripted::new();
    host.seed("Entry", "a", json!({"id":"a","text":"old"}));
    host.settle(1, json!({"error":"boom"}));
    host.with(|s| s.fail_rollback = true);
    let body = push(1, vec![edit(1, "a", "lost")]);
    let err = process(&config(), &body, &host).unwrap_err();
    assert_eq!(err.code, code::HOST, "{err}");
    assert_eq!(host.count("saveReceipt"), 0);
    assert_eq!(host.count("handle"), 1);
    // Retrying the same batch bytes runs the handler again: nothing was saved.
    host.with(|s| s.fail_rollback = false);
    let _ = process(&config(), &body, &host);
    assert_eq!(host.count("handle"), 2, "the handler ran again on retry");
}

/// A declared but unretained model version rejects only the mutation that
/// touches it; the batch is not refused before `claim`.
#[test]
fn an_unretained_declaration_rejects_only_the_touching_mutation() {
    let mut mutations = mutations().as_array().unwrap().clone();
    mutations.push(
        json!({"name":"editNote","version":1,"slots":[{"name":"note","model":"Note","operation":"update","cardinality":"single","allowedPatchFields":["text"]}]}),
    );
    let config = Config::decode(json!({
        "schema":{"enums":[],"models":[
            {"name":"Entry","identity":["id"],"fields":[field("id",false),field("text",false)]},
            {"name":"Note","identity":["id"],"fields":[field("id",false),field("text",false)]}]},
        "loaders":["Entry","Note"],
        "mutations":mutations
    }))
    .unwrap();
    let host = Scripted::new();
    host.seed("Entry", "a", json!({"id":"a","text":"old"}));
    host.seed("Note", "n", json!({"id":"n","text":"old"}));
    let body = push_declaring(
        "c",
        1,
        json!({"Entry":9,"Note":1}),
        vec![
            edit(1, "a", "typed"),
            json!({"ordinal":2,"name":"editNote","operations":[{"model":"Note","op":"update","identity":{"id":"n"},"values":{"text":"new"}}]}),
        ],
    );
    let receipt = decode(&process(&config, &body, &host).unwrap());
    assert_eq!(
        receipt.rejections,
        vec![axton_core::Rejection {
            ordinal: 1,
            code: code::MODEL_VERSION_UNSUPPORTED.into()
        }]
    );
    assert_eq!(
        receipt
            .records
            .iter()
            .map(|r| (r.model.as_str(), r.identity["id"].as_str().unwrap()))
            .collect::<Vec<_>>(),
        [("Note", "n")]
    );
    assert_eq!(host.count("claim"), 1, "the request was claimed");
}

/// Handler `changes` or a membership naming a model without a registered
/// loader is a `loader.unregistered` error that aborts the push.
#[test]
fn handler_changes_naming_an_unregistered_model_are_an_error() {
    let config = config_with_note(&["Entry"]);
    let host = Scripted::new();
    host.seed("Entry", "a", json!({"id":"a","text":"old"}));
    host.settle(1, settled(json!([record("Note", "n")]), json!([])));
    let body = push_declaring(
        "c",
        1,
        json!({"Entry":1,"Note":1}),
        vec![edit(1, "a", "typed")],
    );
    let err = process(&config, &body, &host).unwrap_err();
    assert_eq!(err.code, code::LOADER_UNREGISTERED, "{err}");
    assert_eq!(host.count("advanceStamp"), 0);
    assert_eq!(host.count("saveReceipt"), 0);
    // The same for a membership of an unregistered model.
    let host = Scripted::new();
    host.seed("Entry", "a", json!({"id":"a","text":"old"}));
    host.settle(1, settled(json!([]), json!([add("shared", "Note", "n")])));
    let err = process(&config, &body, &host).unwrap_err();
    assert_eq!(err.code, code::LOADER_UNREGISTERED, "{err}");
}
