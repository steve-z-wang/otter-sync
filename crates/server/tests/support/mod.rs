//! An in-memory backend for settlement regressions: business rows, record
//! stamps, Channel heads, invalidations, persistent memberships, saved calls
//! and receipts, all restored together by a savepoint rollback. Handlers are
//! scripted by name; every request is logged in order.
#![allow(dead_code)]
use axton_core::RecordKey;
use axton_server::{Config, Host, HostResult, host::HostRequest};
use serde_json::{Map, Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
    sync::Mutex,
    task::{Context, Poll, Waker},
};

pub fn run<T>(future: impl Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut context) {
            return value;
        }
    }
}

/// `(model, canonical identity key)` of a single-`id` record.
pub fn record(model: &str, id: &str) -> (String, String) {
    let key = RecordKey {
        model: model.into(),
        identity: json!({ "id": id }),
    };
    (model.into(), key.encoded_identity().unwrap())
}
pub fn reference(model: &str, id: &str) -> Value {
    json!({"model":model,"identity":{"id":id}})
}
pub fn add(channel: &str, model: &str, id: &str) -> Value {
    json!({"channel":channel,"model":model,"identity":{"id":id},"present":true})
}
pub fn remove(channel: &str, model: &str, id: &str) -> Value {
    json!({"channel":channel,"model":model,"identity":{"id":id},"present":false})
}

/// A business row to set (`Some`) or delete (`None`) by `(model, identity key)`.
type Write = ((String, String), Option<Value>);

/// Everything a savepoint isolates.
#[derive(Clone, Default, PartialEq, Debug)]
pub struct Tables {
    /// Business rows by `(model, identity key)`.
    pub rows: BTreeMap<(String, String), Value>,
    pub stamps: BTreeMap<(String, String), u64>,
    pub heads: BTreeMap<String, u64>,
    /// `(channel, model, identity key)` to `(cursor, stamp)`.
    pub invalidations: BTreeMap<(String, String, String), (u64, u64)>,
    /// `(model, identity key, channel)`, the primary key order of `axton_membership`.
    pub memberships: BTreeSet<(String, String, String)>,
    /// Saved calls: request and response.
    pub calls: BTreeMap<String, (String, Option<String>)>,
}

#[derive(Default)]
pub struct State {
    pub tables: Tables,
    savepoints: Vec<Tables>,
    clients: BTreeMap<String, (u64, Option<String>)>,
    pub log: Vec<HostRequest>,
    /// The settlement each handler name answers.
    scripts: BTreeMap<String, Value>,
    /// Extra business writes per handler name, beyond its input operands.
    writes: BTreeMap<String, Vec<Write>>,
    /// Records whose loader refuses the caller.
    refused: BTreeSet<(String, String)>,
}

pub struct Backend(pub Mutex<State>);

impl Default for Backend {
    fn default() -> Self {
        Self::new()
    }
}

impl Backend {
    pub fn new() -> Self {
        Self(Mutex::new(State::default()))
    }
    pub fn with<T>(&self, f: impl FnOnce(&mut State) -> T) -> T {
        f(&mut self.0.lock().unwrap())
    }
    /// A business row with an optional existing stamp.
    pub fn seed(&self, model: &str, id: &str, row: Value, stamp: Option<u64>) {
        self.with(|s| {
            s.tables.rows.insert(record(model, id), row);
            if let Some(stamp) = stamp {
                s.tables.stamps.insert(record(model, id), stamp);
            }
        });
    }
    /// A persistent membership that already exists, at head `head` if new.
    pub fn enroll(&self, channel: &str, model: &str, id: &str, head: u64) {
        self.with(|s| {
            let (model, key) = record(model, id);
            assert!(
                s.tables.stamps.contains_key(&(model.clone(), key.clone())),
                "membership needs record metadata"
            );
            s.tables.heads.entry(channel.into()).or_insert(head);
            s.tables.memberships.insert((model, key, channel.into()));
        });
    }
    pub fn script(&self, name: &str, answer: Value) {
        self.with(|s| s.scripts.insert(name.into(), answer));
    }
    pub fn write(&self, name: &str, model: &str, id: &str, row: Option<Value>) {
        self.with(|s| {
            s.writes
                .entry(name.into())
                .or_default()
                .push((record(model, id), row))
        });
    }
    pub fn refuse_load(&self, model: &str, id: &str) {
        self.with(|s| s.refused.insert(record(model, id)));
    }
    pub fn tables(&self) -> Tables {
        self.with(|s| s.tables.clone())
    }
    pub fn row(&self, model: &str, id: &str) -> Option<Value> {
        self.with(|s| s.tables.rows.get(&record(model, id)).cloned())
    }
    pub fn stamp(&self, model: &str, id: &str) -> Option<u64> {
        self.with(|s| s.tables.stamps.get(&record(model, id)).copied())
    }
    pub fn head(&self, channel: &str) -> u64 {
        self.with(|s| s.tables.heads.get(channel).copied().unwrap_or(0))
    }
    pub fn members(&self, model: &str, id: &str) -> Vec<String> {
        let (model, key) = record(model, id);
        self.with(|s| {
            s.tables
                .memberships
                .iter()
                .filter(|(m, k, _)| *m == model && *k == key)
                .map(|(_, _, channel)| channel.clone())
                .collect()
        })
    }
    /// The `(cursor, stamp)` of the record's invalidation on `channel`.
    pub fn invalidation(&self, channel: &str, model: &str, id: &str) -> Option<(u64, u64)> {
        let (model, key) = record(model, id);
        self.with(|s| {
            s.tables
                .invalidations
                .get(&(channel.into(), model, key))
                .copied()
        })
    }
    pub fn log(&self) -> Vec<HostRequest> {
        self.with(|s| s.log.clone())
    }
    pub fn clear_log(&self) {
        self.with(|s| s.log.clear());
    }
    /// Each logged operation's name, without ordinals.
    pub fn ops(&self) -> Vec<String> {
        self.log()
            .iter()
            .map(|request| {
                let value = serde_json::to_value(request).unwrap();
                value["op"].as_str().unwrap().to_string()
            })
            .collect()
    }
    pub fn count(&self, op: &str) -> usize {
        self.ops().iter().filter(|name| *name == op).count()
    }
    /// The settlement's own requests: guards, membership reads and writes, and
    /// publications, in the order issued.
    pub fn settlement_log(&self) -> Vec<HostRequest> {
        self.log()
            .into_iter()
            .filter(|request| {
                matches!(
                    request,
                    HostRequest::AdvanceStamp { .. }
                        | HostRequest::EnsureStamp { .. }
                        | HostRequest::LockRecord { .. }
                        | HostRequest::Memberships { .. }
                        | HostRequest::SetMembership { .. }
                        | HostRequest::Publish { .. }
                )
            })
            .collect()
    }
    /// The models the loader was asked for, in order.
    pub fn loaded_models(&self) -> Vec<String> {
        self.log()
            .into_iter()
            .filter_map(|request| match request {
                HostRequest::Load { model, .. } => Some(model),
                _ => None,
            })
            .collect()
    }

    /// Merge every input operand of the call into its business row.
    fn apply(tables: &mut Tables, arguments: &Value) {
        let Some(arguments) = arguments.as_object() else {
            return;
        };
        for (name, value) in arguments {
            // Legacy slots carry `{identity, patch|data}`; Actions carry flat rows.
            let (identity, fields) = match value.get("identity") {
                Some(identity) => (
                    identity.clone(),
                    value
                        .get("patch")
                        .or_else(|| value.get("data"))
                        .cloned()
                        .unwrap_or(json!({})),
                ),
                None if value.get("id").is_some() => (json!({"id":value["id"]}), value.clone()),
                None => continue,
            };
            let model = if name == "project" { "Project" } else { "Todo" };
            let key = (
                model.to_string(),
                RecordKey {
                    model: model.into(),
                    identity: identity.clone(),
                }
                .encoded_identity()
                .unwrap(),
            );
            let mut row: Map<String, Value> = tables
                .rows
                .get(&key)
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_else(|| identity.as_object().cloned().unwrap_or_default());
            row.extend(fields.as_object().cloned().unwrap_or_default());
            tables.rows.insert(key, Value::Object(row));
        }
    }

    fn answer(&self, raw: Value) -> HostResult<Value> {
        let request: HostRequest = serde_json::from_value(raw)
            .map_err(|error| format!("unsupported host request: {error}"))?;
        let mut s = self.0.lock().unwrap();
        s.log.push(request.clone());
        let action = matches!(request, HostRequest::HandleAction { .. });
        Ok(match request {
            HostRequest::Claim { owner, client_id } => {
                let (sequence, receipt) = s.clients.get(&client_id).cloned().unwrap_or((0, None));
                json!({"clientId":client_id,"owner":owner,"sequence":sequence,"receipt":receipt})
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
            HostRequest::ClaimCall {
                call_id, request, ..
            } => {
                if let Some((stored, response)) = s.tables.calls.get(&call_id) {
                    json!({"fresh":false,"request":stored,"response":response})
                } else {
                    s.tables.calls.insert(call_id, (request.clone(), None));
                    json!({"fresh":true,"request":request,"response":null})
                }
            }
            HostRequest::SaveCall {
                call_id, response, ..
            } => {
                s.tables
                    .calls
                    .get_mut(&call_id)
                    .ok_or("call not claimed")?
                    .1 = Some(response);
                Value::Null
            }
            HostRequest::Head { channel } => {
                json!(s.tables.heads.get(&channel).copied().unwrap_or(0))
            }
            HostRequest::Scan {
                channel,
                after,
                limit,
            } => {
                // `SQL.SCAN`: the Channel's positions after `after` whose record
                // is still a member, in cursor order, each with the record's
                // current stamp, at most `limit`. Membership filters before the
                // limit, so removed positions never fill a page.
                let mut rows: Vec<(u64, String, String)> = s
                    .tables
                    .invalidations
                    .iter()
                    .filter(|((c, model, key), (cursor, _))| {
                        *c == channel
                            && *cursor > after
                            && s.tables.memberships.contains(&(
                                model.clone(),
                                key.clone(),
                                channel.clone(),
                            ))
                    })
                    .map(|((_, model, key), (cursor, _))| (*cursor, model.clone(), key.clone()))
                    .collect();
                rows.sort();
                rows.truncate(limit as usize);
                let mut scanned = vec![];
                for (cursor, model, key) in rows {
                    let stamp = s
                        .tables
                        .stamps
                        .get(&(model.clone(), key.clone()))
                        .ok_or_else(|| format!("Record metadata missing for {model} {key}"))?;
                    let identity: Value = serde_json::from_str(&key).map_err(|e| e.to_string())?;
                    scanned.push(json!({"channel":channel,"cursor":cursor,"model":model,
                        "identity":identity,"identityKey":key,"stamp":stamp}));
                }
                Value::Array(scanned)
            }
            HostRequest::Savepoint { .. } => {
                let snapshot = s.tables.clone();
                s.savepoints.push(snapshot);
                Value::Null
            }
            HostRequest::Rollback { .. } => {
                s.tables = s.savepoints.last().cloned().ok_or("no savepoint")?;
                Value::Null
            }
            HostRequest::Release { .. } => {
                s.savepoints.pop().ok_or("no savepoint")?;
                Value::Null
            }
            HostRequest::Handle {
                name, arguments, ..
            }
            | HostRequest::HandleAction {
                name, arguments, ..
            } => {
                Self::apply(&mut s.tables, &arguments);
                for (key, row) in s.writes.get(&name).cloned().unwrap_or_default() {
                    match row {
                        Some(row) => s.tables.rows.insert(key, row),
                        None => s.tables.rows.remove(&key),
                    };
                }
                s.scripts.get(&name).cloned().unwrap_or_else(|| {
                    if action {
                        json!({"outputs":{},"changes":[],"memberships":[]})
                    } else {
                        json!({"changes":[],"memberships":[]})
                    }
                })
            }
            HostRequest::Load {
                model, identities, ..
            } => {
                let keys: Vec<(String, String)> = identities
                    .iter()
                    .map(|identity| {
                        (
                            model.clone(),
                            RecordKey {
                                model: model.clone(),
                                identity: identity.clone(),
                            }
                            .encoded_identity()
                            .unwrap(),
                        )
                    })
                    .collect();
                if keys.iter().any(|key| s.refused.contains(key)) {
                    json!({"rejection": format!("{}.forbidden", model.to_lowercase())})
                } else {
                    Value::Array(
                        keys.iter()
                            .map(|key| s.tables.rows.get(key).cloned().unwrap_or(Value::Null))
                            .collect(),
                    )
                }
            }
            HostRequest::AdvanceStamp {
                model,
                identity_key,
            } => {
                let stamp = s.tables.stamps.entry((model, identity_key)).or_insert(0);
                *stamp += 1;
                json!(*stamp)
            }
            HostRequest::EnsureStamp {
                model,
                identity_key,
            } => json!(*s.tables.stamps.entry((model, identity_key)).or_insert(1)),
            HostRequest::Publish {
                channel,
                model,
                identity_key,
                stamp,
                ..
            } => {
                let current = s
                    .tables
                    .stamps
                    .get(&(model.clone(), identity_key.clone()))
                    .copied();
                if current != Some(stamp) {
                    return Err(format!(
                        "publish of {model} {identity_key} at {stamp}, record is at {current:?}"
                    ));
                }
                let head = s.tables.heads.entry(channel.clone()).or_insert(0);
                *head += 1;
                let cursor = *head;
                s.tables
                    .invalidations
                    .insert((channel, model, identity_key), (cursor, stamp));
                json!({"cursor":cursor,"stamp":stamp})
            }
            HostRequest::LockRecord {
                model,
                identity_key,
            } => json!(s.tables.stamps.get(&(model, identity_key)).copied()),
            HostRequest::Memberships {
                model,
                identity_key,
            } => json!(
                s.tables
                    .memberships
                    .iter()
                    .filter(|(m, k, _)| *m == model && *k == identity_key)
                    .map(|(_, _, channel)| channel.clone())
                    .collect::<Vec<_>>()
            ),
            HostRequest::SetMembership {
                channel,
                model,
                identity_key,
                present,
            } => {
                if present {
                    if !s
                        .tables
                        .stamps
                        .contains_key(&(model.clone(), identity_key.clone()))
                    {
                        return Err(format!(
                            "membership of {model} {identity_key} needs its record metadata"
                        ));
                    }
                    s.tables.heads.entry(channel.clone()).or_insert(0);
                    s.tables.memberships.insert((model, identity_key, channel));
                } else {
                    s.tables.memberships.remove(&(model, identity_key, channel));
                }
                Value::Null
            }
        })
    }
}

impl Host for Backend {
    fn call(&self, raw: Value) -> Pin<Box<dyn Future<Output = HostResult<Value>> + Send + '_>> {
        let answer = self.answer(raw);
        Box::pin(async move { answer })
    }
}

fn string_field(name: &str) -> Value {
    json!({"name":name,"type":{"kind":"scalar","name":"string"},"nullable":false})
}
fn identity_type(model: &str) -> Value {
    json!({"kind":"identity","model":model,"fields":[{"name":"id","type":{"kind":"scalar","name":"string"}}]})
}
fn model_output(name: &str, model: &str) -> Value {
    json!({"name":name,"kind":"model","model":model,"modelReadVersion":1,"cardinality":"single","source":"handlerIdentity","handlerType":identity_type(model)})
}

/// Todo {id, title} and Project {id, name}, both loaded. Actions:
/// - `Edit(todo Todo.update)` with no outputs;
/// - `EditAndRead(todo Todo.update) { todo Todo }`, a same-name output;
/// - `EditAndReadProject(todo Todo.update) { project Project }`;
/// - `Settle()` and `ReadProject() { project Project }` with no operands.
///
/// The legacy mutation `edit` updates one Todo slot.
pub fn config() -> Config {
    let todo = json!([string_field("id"), string_field("title")]);
    let project = json!([string_field("id"), string_field("name")]);
    let input = json!([{"kind":"model","name":"todo","model":"Todo","operation":"update","cardinality":"single","allowedPatchFields":["title"]}]);
    Config::decode(json!({
        "schema":{"enums":[],
            "models":[
                {"name":"Todo","version":1,"identity":["id"],"fields":todo},
                {"name":"Project","version":1,"identity":["id"],"fields":project}],
            "resultModels":[
                {"name":"Todo","version":1,"identity":["id"],"fields":todo,"enums":[]},
                {"name":"Project","version":1,"identity":["id"],"fields":project,"enums":[]}],
            "actions":[
                {"name":"Edit","version":1,"inputs":input,"outputs":[]},
                {"name":"EditAndRead","version":1,"inputs":input,"outputs":[model_output("todo","Todo")]},
                {"name":"EditAndReadProject","version":1,"inputs":input,"outputs":[model_output("project","Project")]},
                {"name":"Settle","version":1,"inputs":[],"outputs":[]},
                {"name":"ReadProject","version":1,"inputs":[],"outputs":[model_output("project","Project")]}]},
        "mutations":[{"name":"edit","version":1,"slots":[{"name":"todo","model":"Todo","operation":"update","cardinality":"single","allowedPatchFields":["title"]}]}],
        "loaders":["Todo","Project"]
    }))
    .unwrap()
}

/// A call ID unique per `n`.
pub fn call_id(n: u64) -> String {
    format!("01890f47-1234-7123-8123-{n:012x}")
}
pub fn call(ordinal: u64, n: u64, name: &str, args: Value) -> Value {
    json!({"ordinal":ordinal,"callId":call_id(n),"name":name,"version":1,"args":args})
}
pub fn edit(ordinal: u64, n: u64, name: &str, id: &str, title: &str) -> Value {
    call(ordinal, n, name, json!({"todo":{"id":id,"title":title}}))
}

/// One durable Action batch from client `device` of owner `alice`.
pub fn push(backend: &Backend, sequence: u64, models: Value, calls: Vec<Value>) -> Value {
    let request =
        json!({"clientId":"device","batchSequence":sequence,"models":models,"mutations":calls});
    let text = run(axton_server::process_action_push(
        &config(),
        "alice",
        request.to_string().as_bytes(),
        backend,
    ))
    .unwrap();
    serde_json::from_str(&text).unwrap()
}

/// One legacy push of the `edit` mutation from client `legacy`.
pub fn legacy_push(
    backend: &Backend,
    sequence: u64,
    models: Value,
    id: &str,
    title: &str,
) -> Value {
    let request = json!({"clientId":"legacy","batchSequence":sequence,"models":models,"mutations":[
        {"ordinal":1,"name":"edit","operations":[{"model":"Todo","op":"update","identity":{"id":id},"values":{"title":title}}]}
    ]});
    let text = run(axton_server::process_push(
        &config(),
        "alice",
        request.to_string().as_bytes(),
        backend,
    ))
    .unwrap();
    serde_json::from_str(&text).unwrap()
}

/// The records of a receipt as `(model, id, stamp)`.
pub fn authority(receipt: &Value) -> Vec<(String, String, u64)> {
    receipt["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|record| {
            (
                record["model"].as_str().unwrap().to_string(),
                record["identity"]["id"].as_str().unwrap().to_string(),
                record["stamp"].as_u64().unwrap(),
            )
        })
        .collect()
}

/// One delta pull of `cursors` by `alice`, declaring Todo and Project v1.
pub fn pull(backend: &Backend, cursors: &[(&str, u64)]) -> axton_core::PullPage {
    let cursors: Map<String, Value> = cursors
        .iter()
        .map(|(channel, cursor)| ((*channel).to_string(), json!(cursor)))
        .collect();
    let request = json!({"cursors":cursors,"models":{"Todo":1,"Project":1}});
    let text = run(axton_server::process_pull(
        &config(),
        "alice",
        request.to_string().as_bytes(),
        backend,
    ))
    .unwrap();
    axton_core::PullPage::decode(text.as_bytes()).unwrap()
}

/// One bounded Bootstrap page of `channel`'s interval `(after, until]`.
pub fn bootstrap(
    backend: &Backend,
    channel: &str,
    after: u64,
    until: u64,
) -> axton_core::BootstrapPage {
    let request = json!({"mode":"bootstrap","channel":channel,"models":{"Todo":1,"Project":1},
        "after":after,"until":until});
    let text = run(axton_server::process_pull(
        &config(),
        "alice",
        request.to_string().as_bytes(),
        backend,
    ))
    .unwrap();
    axton_core::BootstrapPage::decode(text.as_bytes()).unwrap()
}

/// One external settlement (`backend.transaction`): the records it reports
/// changed and its ordered membership intents.
pub fn settle(backend: &Backend, changes: Vec<Value>, memberships: Vec<Value>) {
    run(axton_server::settle_external(
        &config(),
        &json!({"changes":changes,"memberships":memberships}),
        backend,
    ))
    .unwrap();
}
