//! An in-memory backend for settlement regressions: business rows, record
//! stamps, Scope heads, positions, tagged persistent memberships, saved
//! calls and receipts, all restored together by a savepoint rollback.
//! Handlers are scripted by name; every request is logged in order.
#![allow(dead_code)]
use crate::capability;
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
pub fn add(stream: &str, model: &str, id: &str) -> Value {
    json!({"kind":"track","stream":stream,"record":{"model":model,"identity":{"id":id}}})
}
pub fn remove(scope: &str, model: &str, id: &str) -> Value {
    json!({"kind":"remove","scope":scope,"record":{"model":model,"identity":{"id":id}}})
}
pub fn add_tagged(scope: &str, model: &str, id: &str, tags: &[&str]) -> Value {
    json!({"kind":"add","scope":scope,"record":{"model":model,"identity":{"id":id}},"tags":tags})
}
pub fn remove_tag(scope: &str, tag: &str) -> Value {
    json!({"kind":"select","scope":scope,"predicate":{"tags":{"any":[tag]}},"action":{"kind":"remove"}})
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
    /// `(scope, model, identity key)` to its latest position: the cursor
    /// and, for an upsert, the record's stamp when it was positioned (test
    /// evidence only; the contract's positions carry no stamp); `None` for a
    /// removal.
    pub invalidations: BTreeMap<(String, String, String), (u64, Option<u64>)>,
    /// `(model, identity key, scope)` to the member's tags.
    pub memberships: BTreeMap<(String, String, String), BTreeSet<String>>,
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
    /// A competing writer's committed enrollment `(scope, model, id)`, made
    /// visible when the next `lockScopes` is granted.
    intrusion: Option<(String, String, String)>,
    /// Rewrites the real answer of one operation, to test conformance checks.
    tampered: BTreeMap<String, fn(Value) -> Value>,
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
    /// A persistent, untagged membership that already exists, positioned
    /// at the next cursor of a Scope at head `head` if new.
    pub fn enroll(&self, scope: &str, model: &str, id: &str, head: u64) {
        self.with(|s| enroll(&mut s.tables, scope, model, id, head));
    }
    /// Restore one historical server removal position; not a fresh application verb.
    pub fn saved_removal(&self, stream: &str, model: &str, id: &str) {
        self.with(|s| {
            let (model, key) = record(model, id);
            s.tables
                .memberships
                .remove(&(model.clone(), key.clone(), stream.into()));
            let head = s.tables.heads.entry(stream.into()).or_default();
            *head += 1;
            s.tables
                .invalidations
                .insert((stream.into(), model, key), (*head, None));
        });
    }
    /// A competing writer enrolls `id` in `scope` and commits while this
    /// settlement waits for its `lockScopes`.
    pub fn intrude_at_lock(&self, scope: &str, model: &str, id: &str) {
        self.with(|s| s.intrusion = Some((scope.into(), model.into(), id.into())));
    }
    /// Answer `op` with `f` applied to the answer it would have given.
    pub fn tamper(&self, op: &str, f: fn(Value) -> Value) {
        self.with(|s| s.tampered.insert(op.into(), f));
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
    pub fn head(&self, scope: &str) -> u64 {
        self.with(|s| s.tables.heads.get(scope).copied().unwrap_or(0))
    }
    pub fn members(&self, model: &str, id: &str) -> Vec<String> {
        let (model, key) = record(model, id);
        self.with(|s| {
            s.tables
                .memberships
                .keys()
                .filter(|(m, k, _)| *m == model && *k == key)
                .map(|(_, _, scope)| scope.clone())
                .collect()
        })
    }
    /// The `(cursor, stamp)` of the record's latest position on `scope`
    /// when it is an upsert; `None` when there is none or it is a removal.
    pub fn invalidation(&self, scope: &str, model: &str, id: &str) -> Option<(u64, u64)> {
        let (model, key) = record(model, id);
        self.with(|s| {
            s.tables
                .invalidations
                .get(&(scope.into(), model, key))
                .and_then(|(cursor, stamp)| stamp.map(|stamp| (*cursor, stamp)))
        })
    }
    /// The live members of `scope` as `(id, tags)`, in canonical key order.
    pub fn tagged_members(&self, scope: &str) -> Vec<(String, Vec<String>)> {
        self.with(|s| {
            s.tables
                .memberships
                .iter()
                .filter(|((_, _, c), _)| c == scope)
                .map(|((_, key, _), tags)| {
                    let identity: Value = serde_json::from_str(key).unwrap();
                    (
                        identity["id"].as_str().unwrap().to_string(),
                        tags.iter().cloned().collect(),
                    )
                })
                .collect()
        })
    }
    /// Every retained position of `scope` as `(cursor, id, kind)`, in cursor order.
    pub fn positions(&self, scope: &str) -> Vec<(u64, String, &'static str)> {
        self.with(|s| {
            let mut rows: Vec<(u64, String, &'static str)> = s
                .tables
                .invalidations
                .iter()
                .filter(|((c, _, _), _)| c == scope)
                .map(|((_, _, key), (cursor, stamp))| {
                    let identity: Value = serde_json::from_str(key).unwrap();
                    let kind = if stamp.is_some() { "upsert" } else { "remove" };
                    (*cursor, identity["id"].as_str().unwrap().to_string(), kind)
                })
                .collect();
            rows.sort();
            rows
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
    /// The settlement's own requests: touch recipients, Scope locks, record
    /// guards, member reads and the one write, in the order issued.
    pub fn settlement_log(&self) -> Vec<HostRequest> {
        self.log()
            .into_iter()
            .filter(|request| {
                matches!(
                    request,
                    HostRequest::AdvanceStamp { .. }
                        | HostRequest::EnsureStamp { .. }
                        | HostRequest::LockRecord { .. }
                        | HostRequest::LockStreams { .. }
                        | HostRequest::ReadTracking { .. }
                        | HostRequest::GuardRecords { .. }
                        | HostRequest::ApplyStreamMembers { .. }
                )
            })
            .collect()
    }
    /// The deltas every logged `applyScopeMembers` carried, in order.
    pub fn deltas(&self) -> Vec<axton_server::stream_members::MemberDelta> {
        self.log()
            .into_iter()
            .flat_map(|request| match request {
                HostRequest::ApplyStreamMembers { deltas } => deltas,
                _ => vec![],
            })
            .collect()
    }
    /// The logged published upserts as `(scope, id, stamp)`: the stamp is
    /// the one recorded at the pair's latest position.
    pub fn publishes(&self) -> Vec<(String, String, u64)> {
        self.deltas()
            .into_iter()
            .filter(|delta| delta.publish)
            .map(|delta| {
                let id = delta.key.identity["id"].as_str().unwrap().to_string();
                let (_, stamp) = self
                    .invalidation(&delta.stream, &delta.key.model, &id)
                    .expect("a published upsert is positioned");
                (delta.stream, id, stamp)
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
        let op = raw["op"].as_str().unwrap_or_default().to_string();
        let answer = self.answer_request(raw)?;
        Ok(match self.with(|s| s.tampered.get(&op).copied()) {
            Some(tamper) => tamper(answer),
            None => answer,
        })
    }

    fn answer_request(&self, raw: Value) -> HostResult<Value> {
        let request: HostRequest = serde_json::from_value(raw)
            .map_err(|error| format!("unsupported host request: {error}"))?;
        let mut s = self.0.lock().unwrap();
        s.log.push(request.clone());
        let action = matches!(request, HostRequest::HandleAction { .. });
        Ok(match request {
            HostRequest::ReadCall { .. } => Value::Null,
            HostRequest::HandleBootstrap { .. } => json!({"declarations":[]}),
            HostRequest::CreateManifest { .. } | HostRequest::ReadManifest { .. } => {
                json!({"start":0,"total":0,"models":{},"from":0,"to":0,"keys":[]})
            }
            HostRequest::CaptureTail { head, .. } => json!(head),
            HostRequest::AdmitContext { .. } => json!(true),
            HostRequest::PublicationFence {} | HostRequest::SavePublicationGroups { .. } => {
                Value::Null
            }
            HostRequest::ReadPublicationGroups { .. } | HostRequest::ReadPositions { .. } => {
                json!([])
            }
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
            HostRequest::Head { stream: scope } => {
                json!(s.tables.heads.get(&scope).copied().unwrap_or(0))
            }
            HostRequest::Scan {
                stream: scope,
                after,
                limit,
            } => {
                // Scan the compacted log, including retained removals.
                let mut rows: Vec<(u64, String, String)> = s
                    .tables
                    .invalidations
                    .iter()
                    .filter(|((c, _, _), (cursor, _))| *c == scope && *cursor > after)
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
                    let kind = if s.tables.memberships.contains_key(&(
                        model.clone(),
                        key.clone(),
                        scope.clone(),
                    )) {
                        "upsert"
                    } else {
                        "remove"
                    };
                    scanned.push(
                        json!({"kind":kind,"stream":scope,"cursor":cursor,"model":model,
                        "identity":identity,"identityKey":key,"stamp":stamp}),
                    );
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
                        json!({"outputs":{},"changes":[],"declarations":[]})
                    } else {
                        json!({"changes":[],"declarations":[]})
                    }
                })
            }
            // A Load handler writes nothing through the framework; its scripted
            // business writes exist only to prove a rejected page rolls back.
            HostRequest::HandleLoad { name, .. } => {
                for (key, row) in s.writes.get(&name).cloned().unwrap_or_default() {
                    match row {
                        Some(row) => s.tables.rows.insert(key, row),
                        None => s.tables.rows.remove(&key),
                    };
                }
                s.scripts
                    .get(&name)
                    .cloned()
                    .unwrap_or_else(|| json!({"data":{},"next":null}))
            }
            HostRequest::Load {
                mode: Some(axton_server::host::LoaderMode::Prepare),
                ..
            } => json!([]),
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
            // `SQL.READ_STAMPS`: existing stamps are read, missing ones start at 1.
            HostRequest::ReadStamps {
                model,
                identity_keys,
            } => Value::Array(
                identity_keys
                    .into_iter()
                    .map(|key| json!(*s.tables.stamps.entry((model.clone(), key)).or_insert(1)))
                    .collect(),
            ),
            HostRequest::LockRecord {
                model,
                identity_key,
            } => json!(s.tables.stamps.get(&(model, identity_key)).copied()),
            HostRequest::GuardRecords { records } => Value::Array(
                records
                    .into_iter()
                    .map(|r| {
                        let key = (r.model, r.identity_key);
                        match r.mode {
                            axton_server::host::GuardMode::Advance => {
                                let stamp = s.tables.stamps.entry(key).or_insert(0);
                                *stamp += 1;
                                json!(*stamp)
                            }
                            axton_server::host::GuardMode::Ensure => {
                                json!(*s.tables.stamps.entry(key).or_insert(1))
                            }
                            axton_server::host::GuardMode::Lock => json!(s.tables.stamps.get(&key)),
                        }
                    })
                    .collect(),
            ),
            // One process: the lock is the order check the decoder already
            // made. A scripted competing writer commits while it is awaited.
            HostRequest::LockStreams { .. } => {
                if let Some((scope, model, id)) = s.intrusion.take() {
                    enroll(&mut s.tables, &scope, &model, &id, 0);
                }
                Value::Null
            }
            HostRequest::ReadTracking { records, pairs } => Value::Array(
                s.tables
                    .memberships
                    .keys()
                    .filter(|(m, k, c)| {
                        records
                            .iter()
                            .any(|r| r.model == *m && r.identity_key == *k)
                            || pairs
                                .iter()
                                .any(|p| p.model == *m && p.identity_key == *k && p.stream == *c)
                    })
                    .map(|(m, k, c)| json!({"stream":c,"model":m,"identityKey":k}))
                    .collect(),
            ),
            // `SQL.APPLY_SCOPE_MEMBERS`: final states, as given. Each
            // published delta takes its Scope's next cursor; an unpublished
            // one keeps its member's position.
            HostRequest::ApplyStreamMembers { deltas } => {
                let mut positions = vec![];
                for delta in deltas {
                    let model = delta.key.model.clone();
                    let key = delta.key.encoded_identity().unwrap();
                    let member = (model.clone(), key.clone(), delta.stream.clone());
                    let pair = (delta.stream.clone(), model.clone(), key.clone());
                    let stamp = s.tables.stamps.get(&(model.clone(), key.clone())).copied();
                    let Some(stamp) = stamp else {
                        return Err(format!("Record metadata missing for {model} {key}"));
                    };
                    let was_member = s.tables.memberships.contains_key(&member);
                    s.tables.memberships.insert(member, BTreeSet::new());
                    let head = s.tables.heads.entry(delta.stream.clone()).or_insert(0);
                    let (cursor, kind) = if delta.publish {
                        *head += 1;
                        let cursor = *head;
                        let kept = Some(stamp);
                        s.tables.invalidations.insert(pair, (cursor, kept));
                        (cursor, "upsert")
                    } else {
                        match s.tables.invalidations.get(&pair) {
                            Some((cursor, Some(_))) if was_member => (*cursor, "upsert"),
                            _ => {
                                return Err(format!(
                                    "{model} {key} keeps no position in {}",
                                    delta.stream
                                ));
                            }
                        }
                    };
                    positions.push(json!({"stream":delta.stream,"model":model,
                        "identityKey":key,"cursor":cursor,"kind":kind}));
                }
                Value::Array(positions)
            }
        })
    }
}

/// An untagged member positioned at its Scope's next cursor, the Scope
/// starting at `head` if new.
fn enroll(tables: &mut Tables, scope: &str, model: &str, id: &str, head: u64) {
    let (model, key) = record(model, id);
    let stamp = *tables
        .stamps
        .get(&(model.clone(), key.clone()))
        .expect("membership needs record metadata");
    let head = tables.heads.entry(scope.into()).or_insert(head);
    *head += 1;
    let cursor = *head;
    tables.invalidations.insert(
        (scope.into(), model.clone(), key.clone()),
        (cursor, Some(stamp)),
    );
    tables
        .memberships
        .insert((model, key, scope.into()), BTreeSet::new());
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
        &capability::request(request.to_string().as_bytes()),
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
        &capability::request(request.to_string().as_bytes()),
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
        .map(|(scope, cursor)| ((*scope).to_string(), json!(cursor)))
        .collect();
    let request = json!({"cursors":cursors,"models":{"Todo":1,"Project":1}});
    let text = run(axton_server::process_pull(
        &config(),
        "alice",
        &capability::request(request.to_string().as_bytes()),
        backend,
    ))
    .unwrap();
    capability::pull(text.as_bytes()).unwrap()
}

/// One bounded Bootstrap page of `scope`'s interval `(after, until]`.
pub fn bootstrap(
    backend: &Backend,
    scope: &str,
    after: u64,
    until: u64,
) -> axton_core::BootstrapPage {
    let request = json!({"mode":"bootstrap","stream":scope,"models":{"Todo":1,"Project":1},
        "after":after,"until":until});
    let text = run(axton_server::process_pull(
        &config(),
        "alice",
        &capability::request(request.to_string().as_bytes()),
        backend,
    ))
    .unwrap();
    capability::bootstrap(text.as_bytes()).unwrap()
}

/// One external settlement (`backend.transaction`): the records it reports
/// changed and its ordered membership intents.
pub fn settle(backend: &Backend, changes: Vec<Value>, memberships: Vec<Value>) {
    run(axton_server::settle_external(
        &config(),
        &json!({"changes":changes,"declarations":memberships}),
        backend,
    ))
    .unwrap();
}
