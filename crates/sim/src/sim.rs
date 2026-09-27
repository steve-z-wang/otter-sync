//! The arena. Holds the clients, the host, the network and the RNG; applies actions;
//! records the trace. Invariants live in invariants.rs; random stepping in step.rs.
use crate::{
    host::MemHost,
    net::{Message, Network},
    rng::Rng,
    schema,
};
use axton_client::{BootstrapPhase, Client, Operation, OperationKind, Report, ReportKind};
use axton_core::{BootstrapPage, PullPage, PushReceipt, PushRequest, RecordKey};
use axton_server::host::{MembershipIntent, RecordRef};
use axton_sqlite::SqliteStore;
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MutationSpec {
    CreateEntry {
        id: String,
        text: String,
    },
    Edit {
        id: String,
        text: String,
    },
    DeleteEntry {
        id: String,
    },
    CreateComment {
        id: String,
        entry: String,
        text: String,
    },
    EditComment {
        id: String,
        text: String,
    },
    DeleteComment {
        id: String,
    },
}

impl MutationSpec {
    pub fn key(&self) -> RecordKey {
        match self {
            MutationSpec::CreateEntry { id, .. }
            | MutationSpec::Edit { id, .. }
            | MutationSpec::DeleteEntry { id } => schema::entry_key(id),
            MutationSpec::CreateComment { id, .. }
            | MutationSpec::EditComment { id, .. }
            | MutationSpec::DeleteComment { id } => schema::comment_key(id),
        }
    }
    pub fn build(&self) -> axton_client::Mutation {
        match self {
            MutationSpec::CreateEntry { id, text } => schema::create_entry(id, text),
            MutationSpec::Edit { id, text } => schema::edit(id, text),
            MutationSpec::DeleteEntry { id } => schema::delete_entry(id),
            MutationSpec::CreateComment { id, entry, text } => {
                schema::create_comment(id, entry, text)
            }
            MutationSpec::EditComment { id, text } => schema::edit_comment(id, text),
            MutationSpec::DeleteComment { id } => schema::delete_comment(id),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    Enqueue {
        client: usize,
        mutation: MutationSpec,
    },
    Direct {
        client: usize,
        key: String,
        text: String,
    },
    Subscribe {
        client: usize,
        channel: String,
    },
    /// Subscribe from now: the registration initializes at the channel's
    /// current head, so everything already published is history this client
    /// only reaches through Bootstrap
    /// ([#151](https://github.com/zanminwang/axton/issues/151)).
    SubscribeAtHead {
        client: usize,
        channel: String,
    },
    Unsubscribe {
        client: usize,
        channel: String,
    },
    /// Register the durable load of everything published to `channel` before
    /// this subscription's origin, as `bootstrap()` does.
    Bootstrap {
        client: usize,
        channel: String,
    },
    /// Ask for the next historical page of whichever run's turn it is. Nothing
    /// schedulable: nothing to ask for.
    LoadPull {
        client: usize,
    },
    Freeze {
        client: usize,
    },
    /// One pull for every channel the client subscribes to, from its cursors.
    Pull {
        client: usize,
    },
    Deliver,
    Drop,
    Duplicate,
    Hold,
    Swap {
        i: usize,
        j: usize,
    },
    Crash {
        client: usize,
    },
    Restart {
        client: usize,
    },
    /// A business change to `key` (`None` deletes it) distributed to
    /// `channels` by low-level publication: the record is enrolled in each of
    /// them first, since a scan answers members only, and its other
    /// memberships are not published to.
    ServerChange {
        key: String,
        text: Option<String>,
        channels: Vec<String>,
    },
    /// A record's real membership legitimately moves to `channels`: every new member
    /// is told about the record at its current stamp (a republication, which never
    /// advances a version). The channels it leaves hear nothing: loads are
    /// channel-blind, so a client that only follows a vacated channel keeps its last
    /// content as legitimately retained data.
    MoveMembership {
        key: String,
        channels: Vec<String>,
    },
    /// One application transaction outside any push (`backend.transaction`)
    /// for `key`, settled by the engine. `touch` is the business change it
    /// declares: `Some(Some(text))` writes that text, `Some(None)` deletes the
    /// record (an Entry takes its Comments with it, each touched too), `None`
    /// changes nothing. `memberships` are the record's ordered Channel add
    /// (`true`) and remove (`false`) intents; the engine reduces them to the
    /// final relationship. The record's routing follows the stored result, so
    /// later handler calls keep it.
    Declare {
        key: String,
        touch: Option<Option<String>>,
        memberships: Vec<(String, bool)>,
    },
    RejectNext {
        code: String,
    },
    /// The next `handle` call fails: the engine rejects just that mutation
    /// with `handler.failed`, the same as any other business rejection.
    FailNext,
    /// The next `rollback` (following the rejected or failed mutation it
    /// pairs with) is a host infrastructure error: the whole delivery fails
    /// and nothing in it is committed.
    BreakNext,
    /// The loader throws for `key` (until asked for it alone): that record is
    /// delivered as an error change, the rest of the page is unaffected.
    FailLoadNext {
        key: String,
    },
    /// The loader refuses `key` with `sim.refused`: the same, with that code.
    RefuseLoadNext {
        key: String,
    },
    /// The next page a client receives has its first change forged without a
    /// required field: the client skips and reports that change alone.
    CorruptNextPage,
}

pub struct Slot {
    pub path: PathBuf,
    /// The schema this slot opens with; `Sim::upgrade` replaces it.
    pub schema: axton_core::Schema,
    pub client: Option<Client<SqliteStore>>,
    pub enqueued: Vec<u64>,
    pub receipts: BTreeMap<u64, PushReceipt>,
    /// Every batch this client froze, by sequence, with the ordinals it carried
    /// (decoded from the frozen bytes at `Action::Freeze`). A batch that later
    /// leaves the queue must have done so through its receipt;
    /// `completed_work_had_a_matching_response` compares this record with the live
    /// queue and the completion counter.
    pub pushes: BTreeMap<u64, Vec<u64>>,
    /// Subscription generation per channel: bumped every time the client goes from
    /// unsubscribed to subscribed.
    pub generations: BTreeMap<String, u64>,
    /// The Scope whose historical page was asked for last: the rotation's
    /// position, as the Downlink worker keeps it.
    pub bootstrap_rotation: Option<String>,
    /// What the queue held when the client last crashed: the queued ordinals and
    /// the completion counter. `Action::Restart` requires the reopened client to
    /// show exactly this (no pending operation is lost on reopen). Cleared by the
    /// restart; a test that corrupts the file behind the engine's back clears it
    /// itself, since it is forging a state the engine never produced.
    pub crash_state: Option<ReopenState>,
}

/// The durable queue state a crash must not lose: queued ordinals and the last
/// completed push.
pub type ReopenState = (BTreeSet<u64>, u64);

pub struct Sim {
    pub host: MemHost,
    pub net: Network,
    pub rng: Rng,
    pub trace: Vec<Action>,
    pub clients: Vec<Slot>,
    pub seen_stamps: BTreeMap<(usize, String), u64>,
    pub seen_cursors: BTreeMap<(usize, String), u64>,
    pub known_entries: Vec<String>,
    pub known_comments: Vec<String>,
    pub(crate) next_id: u64,
    /// (client, encoded key) pairs that received a direct write since the last
    /// authoritative content for that key landed on that client. A direct write
    /// diverges from the server by design (N4/L4); `no_pending_means_converged`
    /// exempts exactly these pairs rather than the whole client or channel.
    pub direct_writes: BTreeSet<(usize, String)>,
    /// Whether the random stepper (`step.rs::choose`) may generate `Action::Direct`.
    /// Defaults to true; tests/invariants.rs runs the R2 runner both ways.
    pub generate_direct: bool,
    /// Whether `Action::ServerChange` (`step.rs::choose`) may publish to (and so
    /// enroll the record in) a channel outside a record's real, explicitly-set
    /// routing. Defaults to false: an application's handler publishes to the
    /// channels that provide a record, and the random runner generates what
    /// applications do. Since loads are channel-blind, the extra channel is
    /// harmless to the engine - it simply delivers the same content at the same
    /// stamp until the next handler call removes it - so a test may turn this on
    /// to prove exactly that.
    pub generate_membership_faults: bool,
    /// Count of actual (client, key) content comparisons `no_pending_means_converged`
    /// has made across the run - the checks it skips (not at head, exempted by a
    /// direct write, membership or channel-stamp gate) do not count. The R2 runner
    /// asserts a floor on the sum across seeds so this coverage cannot silently drop.
    pub comparisons: usize,
    /// Equal-stamp content conflicts every receipt and page reported across the run
    /// (`ApplyReport::conflicts`). The engine never applies such content; a test that
    /// injects none expects this to stay 0.
    pub conflicts: usize,
    /// Every report a receipt or page produced, in order: what the application
    /// would have been told.
    pub reports: Vec<Report>,
    /// (client, encoded key) pairs whose last delivery could not be applied (a read
    /// failure or a skipped change): the client keeps its earlier content on
    /// purpose until the record is delivered again, so `no_pending_means_converged`
    /// exempts exactly these pairs. Cleared when newer authority lands.
    pub stale_reads: BTreeSet<(usize, String)>,
    /// Whether the next page delivered to a client is forged (`Action::CorruptNextPage`).
    pub corrupt_next_page: bool,
    _dir: tempfile::TempDir,
}

pub const OWNER: &str = "u";

/// (client index, pending mutation count, subscriptions, durable loads) used to
/// detect convergence in `Sim::settle`. The loads are there so a settle keeps
/// going while a historical interval still has pages to ask for.
type SimSnapshot = (
    usize,
    u64,
    Vec<(String, u64)>,
    Vec<(String, &'static str, u64)>,
);

/// Opens through the file-selection path (sidecar, descriptor, compatibility), the
/// way an SDK does, so a restart after a rebuild lands on the rebuilt file.
fn open(path: &PathBuf, schema: &axton_core::Schema, discard_pending: bool) -> Client<SqliteStore> {
    Client::open_at(
        path,
        schema.clone(),
        Box::new(|p| SqliteStore::open(p)),
        discard_pending,
    )
    .unwrap()
}

pub fn parse_key(s: &str) -> RecordKey {
    let (model, id) = s.split_once(':').expect("key as Model:id");
    match model {
        "Entry" => schema::entry_key(id),
        "Comment" => schema::comment_key(id),
        other => panic!("unknown model {other}"),
    }
}

impl Sim {
    pub fn new(seed: u64, clients: usize) -> Sim {
        let dir = tempfile::tempdir().unwrap();
        let clients = (0..clients)
            .map(|i| {
                let path = dir.path().join(format!("client-{i}.sqlite"));
                let schema = schema::schema();
                Slot {
                    client: Some(open(&path, &schema, false)),
                    path,
                    schema,
                    enqueued: vec![],
                    receipts: BTreeMap::new(),
                    pushes: BTreeMap::new(),
                    generations: BTreeMap::new(),
                    bootstrap_rotation: None,
                    crash_state: None,
                }
            })
            .collect();
        Sim {
            host: MemHost::new(),
            net: Network::new(),
            rng: Rng::new(seed),
            trace: vec![],
            clients,
            seen_stamps: BTreeMap::new(),
            seen_cursors: BTreeMap::new(),
            known_entries: vec![],
            known_comments: vec![],
            next_id: 0,
            direct_writes: BTreeSet::new(),
            generate_direct: true,
            generate_membership_faults: false,
            comparisons: 0,
            conflicts: 0,
            reports: vec![],
            stale_reads: BTreeSet::new(),
            corrupt_next_page: false,
            _dir: dir,
        }
    }
    pub fn check(&mut self) -> Result<(), String> {
        crate::invariants::check(self)
    }
    /// Reopen a running client with `schema` the way an app upgrade does: an
    /// identical or additive schema opens in place; an incompatible one is rebuilt
    /// beside, unless unsent work keeps the old file open (`discard_pending` leaves
    /// that work behind). A rebuilt file is a new client to the simulation: the
    /// slot's push bookkeeping and the high-water marks start over, since nothing
    /// in the old file belongs to it.
    pub fn upgrade(
        &mut self,
        client: usize,
        schema: axton_core::Schema,
        discard_pending: bool,
    ) -> axton_client::SchemaState {
        self.clients[client].client = None;
        self.clients[client].schema = schema;
        let slot = &self.clients[client];
        let reopened = open(&slot.path, &slot.schema, discard_pending);
        let state = reopened.schema_state().clone();
        self.clients[client].client = Some(reopened);
        if state.rebuilt {
            self.forget(client);
        }
        state
    }
    /// `Client::rebuild` on a client whose old file was kept open for unsent work.
    pub fn rebuild(
        &mut self,
        client: usize,
        discard_pending: bool,
    ) -> Result<axton_client::RebuildReport, String> {
        let report = self
            .client(client)
            .rebuild(discard_pending)
            .map_err(|e| e.to_string())?;
        self.forget(client);
        Ok(report)
    }
    fn forget(&mut self, client: usize) {
        let slot = &mut self.clients[client];
        slot.enqueued.clear();
        slot.receipts.clear();
        slot.pushes.clear();
        slot.crash_state = None;
        slot.bootstrap_rotation = None;
        for generation in slot.generations.values_mut() {
            *generation += 1;
        }
        self.seen_stamps.retain(|(i, _), _| *i != client);
        self.seen_cursors.retain(|(i, _), _| *i != client);
        self.direct_writes.retain(|(i, _)| *i != client);
    }
    pub fn client(&mut self, i: usize) -> &mut Client<SqliteStore> {
        self.clients[i].client.as_mut().expect("client is crashed")
    }
    pub fn is_up(&self, i: usize) -> bool {
        self.clients[i].client.is_some()
    }
    pub fn read_text(&mut self, client: usize, key: &RecordKey) -> Option<String> {
        self.client(client)
            .read(key)
            .unwrap()
            .and_then(|v| v["text"].as_str().map(str::to_string))
    }
    fn ensure_membership(&mut self, spec: &MutationSpec) {
        let key = spec.key();
        if self.host.has_membership(&key) {
            return;
        }
        let channels = match spec {
            MutationSpec::CreateComment { entry, .. } => {
                let parent = self.host.membership(&schema::entry_key(entry));
                if parent.is_empty() {
                    vec!["a".to_string()]
                } else {
                    parent
                }
            }
            _ => vec!["a".to_string()],
        };
        let refs: Vec<&str> = channels.iter().map(String::as_str).collect();
        self.host.set_membership(&key, &refs);
    }
    /// Move `key`'s real membership to exactly `channels` in one external
    /// settlement: a removal for every stored Channel it leaves, an add for
    /// every one it names. The engine publishes it to each newly joined Channel
    /// at its current stamp (initializing a missing one, never advancing it);
    /// Channels it stays in or leaves are told nothing.
    fn move_membership(&mut self, key: &RecordKey, channels: &[String]) -> Result<(), String> {
        let stored = self.host.stored_memberships(key);
        let mut intents: Vec<(String, bool)> = stored
            .iter()
            .filter(|channel| !channels.contains(channel))
            .map(|channel| (channel.clone(), false))
            .collect();
        intents.extend(channels.iter().map(|channel| (channel.clone(), true)));
        self.declare(key, &[], vec![], &intents)
    }
    /// One external settlement of `changes` and `key`'s ordered membership
    /// intents after `writes`; the record's routing then follows its stored
    /// memberships.
    fn declare(
        &mut self,
        key: &RecordKey,
        writes: &[(RecordKey, Option<serde_json::Value>)],
        changes: Vec<RecordKey>,
        intents: &[(String, bool)],
    ) -> Result<(), String> {
        let memberships = intents
            .iter()
            .map(|(channel, present)| MembershipIntent {
                channel: channel.clone(),
                model: key.model.clone(),
                identity: key.identity.clone(),
                present: *present,
            })
            .collect();
        let changes = changes
            .into_iter()
            .map(|key| RecordRef {
                model: key.model,
                identity: key.identity,
            })
            .collect();
        self.host.transact(writes, changes, memberships)?;
        let stored = self.host.stored_memberships(key);
        let refs: Vec<&str> = stored.iter().map(String::as_str).collect();
        self.host.set_membership(key, &refs);
        Ok(())
    }
    pub fn apply(&mut self, action: Action) -> Result<(), String> {
        self.trace.push(action.clone());
        match action {
            Action::Enqueue { client, mutation } => {
                self.ensure_membership(&mutation);
                let m = mutation.build();
                let ordinal = self
                    .client(client)
                    .transaction(|tx| tx.enqueue(m))
                    .map_err(|e| e.to_string())?;
                self.clients[client].enqueued.push(ordinal);
            }
            Action::Direct { client, key, text } => {
                let key = parse_key(&key);
                let op = Operation {
                    model: key.model.clone(),
                    op: OperationKind::Update,
                    identity: key.identity.clone(),
                    values: Some(json!({ "text": text })),
                };
                self.client(client)
                    .transaction(|tx| tx.direct(op))
                    .map_err(|e| e.to_string())?;
                self.direct_writes.insert((client, key.encoded().unwrap()));
            }
            Action::Subscribe { client, channel } => {
                let fresh = self
                    .client(client)
                    .subscriptions()
                    .map_err(|e| e.to_string())?
                    .iter()
                    .all(|(c, _)| c != &channel);
                self.client(client)
                    .transaction(|tx| tx.set_channel(channel.clone(), true))
                    .map_err(|e| e.to_string())?;
                // Registration is intent only; the simulated client is one whose
                // session acknowledged head zero, so the whole published log is
                // what it converges on
                // ([#150](https://github.com/zanminwang/axton/issues/150)).
                let state = self
                    .client(client)
                    .subscription_state(&channel)
                    .map_err(|e| e.to_string())?
                    .ok_or("the registration left no subscription")?;
                if state.starting_cursor.is_none() {
                    self.client(client)
                        .initialize_subscriptions(
                            &BTreeMap::from([(channel.clone(), state.subscription_id)]),
                            &BTreeMap::from([(channel.clone(), 0)]),
                        )
                        .map_err(|e| e.to_string())?;
                }
                if fresh {
                    *self.clients[client].generations.entry(channel).or_insert(0) += 1;
                }
            }
            Action::SubscribeAtHead { client, channel } => {
                let fresh = self
                    .client(client)
                    .subscriptions()
                    .map_err(|e| e.to_string())?
                    .iter()
                    .all(|(c, _)| c != &channel);
                self.client(client)
                    .transaction(|tx| tx.set_channel(channel.clone(), true))
                    .map_err(|e| e.to_string())?;
                // The acknowledgement this client's session would get names the
                // head as it is now, so nothing published before it is delivered
                // by subscribing (D9).
                let state = self
                    .client(client)
                    .subscription_state(&channel)
                    .map_err(|e| e.to_string())?
                    .ok_or("the registration left no subscription")?;
                if state.starting_cursor.is_none() {
                    let head = self.host.head(&channel);
                    self.client(client)
                        .initialize_subscriptions(
                            &BTreeMap::from([(channel.clone(), state.subscription_id)]),
                            &BTreeMap::from([(channel.clone(), head)]),
                        )
                        .map_err(|e| e.to_string())?;
                }
                if fresh {
                    *self.clients[client].generations.entry(channel).or_insert(0) += 1;
                }
            }
            Action::Bootstrap { client, channel } => {
                let id = self
                    .client(client)
                    .subscription_state(&channel)
                    .map_err(|e| e.to_string())?
                    .ok_or("a load needs a registered subscription")?
                    .subscription_id;
                self.client(client)
                    .request_bootstrap(&channel, id)
                    .map_err(|e| e.to_string())?;
            }
            Action::LoadPull { client } => {
                // The rotation and the bound come from the ledger, the way the
                // Downlink worker's schedule reads them; a run that is catching
                // up has no page to ask for.
                let rotation = self.clients[client].bootstrap_rotation.clone();
                let Some(task) = self
                    .client(client)
                    .bootstrap_schedule(rotation.as_deref())
                    .map_err(|e| e.to_string())?
                else {
                    return Ok(());
                };
                let models = self.client(client).declared_models();
                let request = task.request(models);
                let bytes = request.encode().map_err(|e| e.to_string())?;
                self.clients[client].bootstrap_rotation = Some(task.state.scope.clone());
                self.net.send(Message::Load {
                    client,
                    scope: task.state.scope.clone(),
                    subscription_id: task.state.subscription_id,
                    run: task.state.run,
                    after: task.state.cursor,
                    bytes,
                });
            }
            Action::Unsubscribe { client, channel } => {
                // A channel is a delivery path, not an owner: unsubscribing must
                // leave every visible row, every stamp and every pending operation
                // exactly as it found them.
                let before = crate::invariants::content_snapshot(self, client)?;
                self.client(client)
                    .transaction(|tx| tx.set_channel(channel, false))
                    .map_err(|e| e.to_string())?;
                crate::invariants::unsubscribe_cannot_remove_content(self, client, &before)?;
            }
            Action::Freeze { client } => {
                if let Some(bytes) = self.client(client).freeze().map_err(|e| e.to_string())? {
                    let request = PushRequest::decode(&bytes).map_err(|e| e.to_string())?;
                    let ordinals = request.mutations.iter().map(|m| m.ordinal).collect();
                    self.clients[client]
                        .pushes
                        .insert(request.batch_sequence, ordinals);
                    self.net.send(Message::Push { client, bytes });
                }
            }
            Action::Pull { client } => {
                // Issued through the client so it can tell a page from an earlier
                // subscription of a channel apart from a gap (A2). Nothing
                // subscribed: nothing to pull.
                let Some(body) = self
                    .client(client)
                    .downlink_request()
                    .map_err(|e| e.to_string())?
                else {
                    return Ok(());
                };
                self.net.send(Message::Pull {
                    client,
                    bytes: body.into_bytes(),
                });
            }
            Action::Deliver => self.deliver()?,
            Action::Drop => {
                self.net.drop_front();
            }
            Action::Duplicate => {
                self.net.duplicate_front();
            }
            Action::Hold => {
                self.net.hold_front();
            }
            Action::Swap { i, j } => {
                self.net.swap(i, j);
            }
            Action::Crash { client } => {
                if self.is_up(client) {
                    let state = crate::invariants::reopen_state(self, client)?;
                    self.clients[client].crash_state = Some(state);
                }
                self.clients[client].client = None;
            }
            Action::Restart { client } => {
                if self.clients[client].client.is_none() {
                    let slot = &self.clients[client];
                    let reopened = open(&slot.path, &slot.schema, false);
                    self.clients[client].client = Some(reopened);
                    // The rotation is the worker's own memory, not durable state:
                    // a reopened lane starts its turn-taking over.
                    self.clients[client].bootstrap_rotation = None;
                    if let Some(before) = self.clients[client].crash_state.take() {
                        crate::invariants::no_pending_operation_is_lost_on_reopen(
                            self, client, &before,
                        )?;
                    }
                }
            }
            Action::ServerChange {
                key,
                text,
                channels,
            } => {
                let k = parse_key(&key);
                let id = k.identity["id"].clone();
                let state = text.map(|t| {
                    if k.model == "Entry" {
                        json!({"id": id, "text": t, "note": null})
                    } else {
                        json!({"id": id, "entryId": "e1", "text": t})
                    }
                });
                // A real DeleteEntry mutation cascades: the client-side engine drops a
                // Comment locally the moment it learns its parent Entry's authority
                // went to None (`stage_authority` in authority.rs walks `descendants`).
                // Nulling an Entry here without also removing its Comments would leave
                // the server holding a Comment the client is bound to cascade-drop, a
                // state the real handler never produces - so mirror the cascade,
                // publishing each dropped Comment on its own real channels.
                if state.is_none() && k.model == "Entry" {
                    for (encoded_key, value) in self.host.records() {
                        if !encoded_key.starts_with("[\"Comment\"") || value["entryId"] != id {
                            continue;
                        }
                        let Some(comment_id) = value["id"].as_str() else {
                            continue;
                        };
                        let child_key = schema::comment_key(comment_id);
                        self.host.set_state(&child_key, None);
                        let membership = self.host.membership(&child_key);
                        let child_refs: Vec<&str> = if membership.is_empty() {
                            channels.iter().map(String::as_str).collect()
                        } else {
                            membership.iter().map(String::as_str).collect()
                        };
                        self.host.notify(&child_key, &child_refs);
                    }
                }
                self.host.set_state(&k, state);
                let refs: Vec<&str> = channels.iter().map(String::as_str).collect();
                self.host.notify(&k, &refs);
            }
            Action::MoveMembership { key, channels } => {
                let k = parse_key(&key);
                self.move_membership(&k, &channels)?;
                // Child membership follows the parent: a moved Entry takes its
                // Comments to the same channels, so a client that follows the
                // destination sees the pair together rather than a parent whose
                // children it can never receive.
                if k.model == "Entry" {
                    let id = k.identity["id"].clone();
                    let child_ids: Vec<String> = self
                        .host
                        .records()
                        .into_iter()
                        .filter(|(ek, v)| ek.starts_with("[\"Comment\"") && v["entryId"] == id)
                        .filter_map(|(_, v)| v["id"].as_str().map(str::to_string))
                        .collect();
                    for comment_id in child_ids {
                        self.move_membership(&schema::comment_key(&comment_id), &channels)?;
                    }
                }
            }
            Action::Declare {
                key,
                touch,
                memberships,
            } => {
                let k = parse_key(&key);
                let id = k.identity["id"].clone();
                let mut writes = vec![];
                let mut changes = vec![];
                if let Some(text) = touch {
                    // A deleted Entry takes its Comments with it, as the real
                    // handler's cascade does; each is a change of its own.
                    if text.is_none() && k.model == "Entry" {
                        for (_, value) in self.host.records() {
                            if value["entryId"] == id
                                && let Some(comment_id) = value["id"].as_str()
                            {
                                let child = schema::comment_key(comment_id);
                                writes.push((child.clone(), None));
                                changes.push(child);
                            }
                        }
                    }
                    let state = text.map(|t| {
                        if k.model == "Entry" {
                            json!({"id": id, "text": t, "note": null})
                        } else {
                            json!({"id": id, "entryId": "e1", "text": t})
                        }
                    });
                    writes.push((k.clone(), state));
                    changes.push(k.clone());
                    if k.model == "Entry"
                        && let Some(id) = id.as_str()
                        && !self.known_entries.iter().any(|known| known == id)
                    {
                        self.known_entries.push(id.to_string());
                    }
                }
                self.declare(&k, &writes, changes, &memberships)?;
            }
            Action::RejectNext { code } => self.host.reject_next(&code),
            Action::FailNext => self.host.fail_next(),
            Action::BreakNext => self.host.break_next(),
            Action::FailLoadNext { key } => self.host.fail_load_next(&parse_key(&key)),
            Action::RefuseLoadNext { key } => self.host.refuse_load_next(&parse_key(&key)),
            Action::CorruptNextPage => self.corrupt_next_page = true,
        }
        Ok(())
    }
    fn deliver(&mut self) -> Result<(), String> {
        let Some(message) = self.net.pop() else {
            return Ok(());
        };
        let client = message.client();
        match message {
            Message::Push { bytes, .. } => {
                let sequence = PushRequest::decode(&bytes)
                    .map_err(|e| e.to_string())?
                    .batch_sequence;
                match self.host.push(OWNER, &bytes) {
                    Ok(receipt) => self.net.send(Message::Receipt {
                        client,
                        sequence,
                        bytes: receipt.into_bytes(),
                    }),
                    Err(error) => self.net.send(Message::PushFailed {
                        client,
                        sequence,
                        error,
                    }),
                }
            }
            Message::Receipt {
                sequence, bytes, ..
            } => {
                if !self.is_up(client) {
                    self.net.send(Message::Receipt {
                        client,
                        sequence,
                        bytes,
                    });
                    return Ok(());
                }
                let receipt = PushReceipt::decode(&bytes).map_err(|e| e.to_string())?;
                // The receipt completes the batch at once: its authority is applied
                // by stamp and the completed operations leave the queue. A stale
                // duplicate changes nothing and reports itself as such.
                match self.client(client).acknowledge(sequence, receipt.clone()) {
                    Ok(report) => {
                        self.conflicts += report.conflicts();
                        self.reports.extend(report.reports);
                        self.clients[client].receipts.insert(sequence, receipt);
                    }
                    Err(e) => return Err(e.to_string()),
                }
            }
            Message::Pull { bytes, .. } => {
                let page = self.host.pull(OWNER, &bytes)?;
                self.net.send(Message::Page {
                    client,
                    bytes: page.into_bytes(),
                });
            }
            // Both pull modes go through the same public entry point, so the
            // simulated backend dispatches a bootstrap request exactly as the
            // HTTP adapter does ([#151](https://github.com/zanminwang/axton/issues/151)).
            Message::Load {
                scope,
                subscription_id,
                run,
                after,
                bytes,
                ..
            } => {
                let page = self.host.pull(OWNER, &bytes)?;
                self.net.send(Message::LoadPage {
                    client,
                    scope,
                    subscription_id,
                    run,
                    after,
                    bytes: page.into_bytes(),
                });
            }
            Message::LoadPage {
                scope,
                subscription_id,
                run,
                after,
                bytes,
                ..
            } => {
                if !self.is_up(client) {
                    self.net.send(Message::LoadPage {
                        client,
                        scope,
                        subscription_id,
                        run,
                        after,
                        bytes,
                    });
                    return Ok(());
                }
                let page = BootstrapPage::decode(&bytes).map_err(|e| e.to_string())?;
                let applied = self
                    .client(client)
                    .apply_bootstrap_page(&scope, subscription_id, run, after, &page)
                    .map_err(|e| e.to_string())?;
                let reports = applied
                    .report()
                    .map(|report| (report.conflicts(), report.reports.clone()));
                if let Some((conflicts, reports)) = reports {
                    self.conflicts += conflicts;
                    // A historical record the client could not apply leaves it
                    // behind on purpose until the record is delivered again:
                    // the same exemption an ordinary page's failure takes.
                    for entry in &reports {
                        if matches!(
                            entry.kind,
                            ReportKind::ReadFailed | ReportKind::Skipped | ReportKind::Conflict
                        ) {
                            let key = schema::schema()
                                .record_key(&entry.model, &entry.identity)
                                .map_err(|e| e.to_string())?;
                            self.stale_reads.insert((client, key.encoded().unwrap()));
                        }
                    }
                    self.reports.extend(reports);
                }
            }
            Message::Page { bytes, .. } => {
                if !self.is_up(client) {
                    self.net.send(Message::Page { client, bytes });
                    return Ok(());
                }
                let mut page = PullPage::decode(&bytes).map_err(|e| e.to_string())?;
                if self.corrupt_next_page
                    && let Some(first) = page.changes.iter_mut().find(|c| c.error.is_none())
                {
                    // A change without its required `text`: the schema refuses it.
                    self.corrupt_next_page = false;
                    if let Some(state) = first.state.as_object_mut() {
                        state.remove("text");
                    } else {
                        first.state = json!({});
                    }
                }
                // A key this page carries a newer authoritative change for is no
                // longer shadowed by an earlier direct write on this client. Newer
                // is the client's own rule (D2): the change's stamp beats the
                // record's local stamp. A stale copy of a page the client already
                // applied (a duplicate, a late retry) leaves the direct write in
                // place, so it must keep the exemption too. The same snapshot shows
                // that a change the client could not apply left nothing behind.
                let mut touched = vec![];
                // Keys the page delivers at the client's stamp or beyond: once
                // applied without a report, the client holds the server's content.
                let mut confirmed = vec![];
                let mut before = BTreeMap::new();
                for change in &page.changes {
                    let Ok(key) = schema::schema().record_key(&change.model, &change.identity)
                    else {
                        continue;
                    };
                    let local = self
                        .client(client)
                        .read_sql(
                            "SELECT stamp FROM axton_record WHERE model = ? AND identity = ?",
                            &[json!(key.model), json!(key.encoded_identity().unwrap())],
                        )
                        .map_err(|e| e.to_string())?
                        .first()
                        .and_then(|r| r["stamp"].as_u64())
                        .unwrap_or(0);
                    let content = self.client(client).read(&key).map_err(|e| e.to_string())?;
                    before.insert(key.encoded().unwrap(), (local, content));
                    if change.stamp > local && change.error.is_none() {
                        touched.push(key.encoded().unwrap());
                    }
                    if change.stamp >= local && change.error.is_none() {
                        confirmed.push(key.encoded().unwrap());
                    }
                }
                let ranges = page.cursors.clone();
                // Entries this page deletes: their comments cascade locally, so a
                // comment's row may go even when its own change was not applied.
                let deleted_entries: BTreeSet<String> = page
                    .changes
                    .iter()
                    .filter(|c| c.model == "Entry" && c.error.is_none() && c.state.is_null())
                    .filter_map(|c| c.identity["id"].as_str().map(str::to_string))
                    .collect();
                let report = self
                    .client(client)
                    .apply_page(page)
                    .map_err(|e| e.to_string())?;
                self.conflicts += report.conflicts();
                // A page moves a channel to its `to` or not at all.
                for (channel, cursor) in &report.cursors {
                    if ranges.get(channel).map(|r| r.to) != Some(*cursor) {
                        return Err(format!(
                            "client {client} channel {channel} cursor {cursor} landed inside the page"
                        ));
                    }
                }
                for entry in &report.reports {
                    let key = schema::schema()
                        .record_key(&entry.model, &entry.identity)
                        .map_err(|e| e.to_string())?;
                    let encoded = key.encoded().unwrap();
                    if matches!(
                        entry.kind,
                        ReportKind::ReadFailed | ReportKind::Skipped | ReportKind::Conflict
                    ) {
                        let now_stamp = self
                            .client(client)
                            .record_stamp(&key)
                            .map_err(|e| e.to_string())?;
                        let now = self.client(client).read(&key).map_err(|e| e.to_string())?;
                        let (then_stamp, then) = before.get(&encoded).cloned().unwrap_or((0, None));
                        // The stamp never moves for a change that was not applied. The
                        // content does not either, unless a parent deletion in the same
                        // page cascaded the row away (and then it is gone, not rewritten).
                        let cascaded = key.model == "Comment"
                            && now.is_none()
                            && then
                                .as_ref()
                                .and_then(|row| row["entryId"].as_str())
                                .is_some_and(|parent| deleted_entries.contains(parent));
                        if now_stamp != then_stamp || (now != then && !cascaded) {
                            return Err(format!(
                                "client {client} {encoded}: a {:?} change changed local content or stamp",
                                entry.kind
                            ));
                        }
                        touched.retain(|k| k != &encoded);
                        confirmed.retain(|k| k != &encoded);
                        // The client missed something only if the delivery was
                        // newer than what it holds.
                        let missed = before
                            .get(&encoded)
                            .is_some_and(|(stamp, _)| entry.stamp > *stamp);
                        if entry.kind != ReportKind::Conflict && missed {
                            self.stale_reads.insert((client, encoded));
                        }
                    }
                }
                self.reports.extend(report.reports);
                for key in touched {
                    self.direct_writes.remove(&(client, key));
                }
                // A page the client dropped (stale, covered) confirmed nothing.
                if !report.stale {
                    for key in confirmed {
                        self.stale_reads.remove(&(client, key));
                    }
                }
                // The transaction that moved delivery may have reached a fixed
                // completion barrier; nothing else settles one.
                let moved: Vec<String> = ranges.keys().cloned().collect();
                self.client(client)
                    .settle_bootstrap_barriers(&moved)
                    .map_err(|e| e.to_string())?;
            }
            Message::PushFailed { .. } => {}
        }
        Ok(())
    }
    /// Deliver every queued message in order, with no faults. Messages re-queued for a
    /// crashed client stop the drain, otherwise it would spin.
    pub fn drain(&mut self) {
        let mut budget = self.net.len() * 4 + 16;
        while !self.net.is_empty() && budget > 0 {
            budget -= 1;
            self.apply(Action::Deliver).unwrap();
        }
    }
    /// Push and pull everything for every running client until nothing changes.
    pub fn settle(&mut self) {
        let mut republished = BTreeSet::new();
        for _ in 0..16 {
            let before = self.snapshot();
            for i in 0..self.clients.len() {
                if !self.is_up(i) {
                    continue;
                }
                self.apply(Action::Freeze { client: i }).unwrap();
                self.apply(Action::Pull { client: i }).unwrap();
                // A registered load is work the lane would issue too; a client
                // with none adds nothing to the trace.
                if self.client(i).bootstrap_schedule(None).unwrap().is_some() {
                    self.apply(Action::LoadPull { client: i }).unwrap();
                }
            }
            // A record a client could not read is corrected the next time it is
            // published: settle republishes it on its channels at its stamp, once.
            // A client that does not follow those channels keeps its retained copy
            // (still exempt from the convergence check).
            let stale: BTreeSet<String> = self.stale_reads.iter().map(|(_, k)| k.clone()).collect();
            for encoded in stale {
                if !republished.insert(encoded.clone()) {
                    continue;
                }
                let key = schema::key_from_encoded(&encoded);
                for channel in self.host.stored_memberships(&key) {
                    self.host.ensure_publish(&key, &channel);
                }
            }
            self.drain();
            if self.snapshot() == before {
                return;
            }
        }
        panic!("settle did not converge in 16 rounds");
    }
    fn snapshot(&mut self) -> Vec<SimSnapshot> {
        let mut out = vec![];
        for i in 0..self.clients.len() {
            if !self.is_up(i) {
                continue;
            }
            let c = self.client(i);
            let loads = c
                .bootstrap_tasks()
                .unwrap()
                .into_iter()
                .map(|state| (state.scope, state.state.as_str(), state.cursor))
                .collect();
            out.push((
                i,
                c.pending_count().unwrap() as u64,
                c.subscriptions().unwrap(),
                loads,
            ));
        }
        out
    }
    /// The committed phase of one registration's durable load.
    pub fn bootstrap_phase(&mut self, client: usize, channel: &str) -> BootstrapPhase {
        let id = self
            .client(client)
            .subscription_state(channel)
            .unwrap()
            .expect("a registered subscription")
            .subscription_id;
        self.client(client)
            .bootstrap_state(channel, id)
            .unwrap()
            .state
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::entry_key;

    #[test]
    fn one_client_round_trip_through_the_network() {
        let mut sim = Sim::new(1, 1);
        sim.apply(Action::Subscribe {
            client: 0,
            channel: "a".into(),
        })
        .unwrap();
        sim.apply(Action::Enqueue {
            client: 0,
            mutation: MutationSpec::CreateEntry {
                id: "e1".into(),
                text: "hi".into(),
            },
        })
        .unwrap();
        assert_eq!(sim.read_text(0, &entry_key("e1")), Some("hi".into()));
        assert_eq!(sim.client(0).pending_count().unwrap(), 1);
        sim.apply(Action::Freeze { client: 0 }).unwrap();
        assert_eq!(sim.net.len(), 1);
        sim.apply(Action::Deliver).unwrap(); // push reaches the server, receipt queued
        assert_eq!(sim.host.handler_calls(), 1);
        sim.apply(Action::Deliver).unwrap(); // receipt reaches the client
        assert_eq!(
            sim.client(0).pending_count().unwrap(),
            0,
            "the receipt completes the batch on its own"
        );
        assert_eq!(sim.client(0).last_completed_push().unwrap(), 1);
        assert_eq!(sim.client(0).record_stamp(&entry_key("e1")).unwrap(), 1);
        sim.apply(Action::Pull { client: 0 }).unwrap();
        sim.apply(Action::Deliver).unwrap(); // pull reaches server, page queued
        sim.apply(Action::Deliver).unwrap(); // page reaches client: same stamp, no rewrite
        assert_eq!(sim.client(0).pending_count().unwrap(), 0);
        assert_eq!(sim.client(0).cursor("a").unwrap(), Some(1));
        assert_eq!(sim.read_text(0, &entry_key("e1")), Some("hi".into()));
        assert_eq!(sim.conflicts, 0);
        assert_eq!(sim.trace.len(), 8);
    }

    #[test]
    fn crash_and_restart_keep_the_frozen_batch_and_settle_resolves_everything() {
        let mut sim = Sim::new(2, 1);
        sim.apply(Action::Subscribe {
            client: 0,
            channel: "a".into(),
        })
        .unwrap();
        sim.apply(Action::Enqueue {
            client: 0,
            mutation: MutationSpec::CreateEntry {
                id: "e1".into(),
                text: "hi".into(),
            },
        })
        .unwrap();
        sim.apply(Action::Freeze { client: 0 }).unwrap();
        sim.apply(Action::Crash { client: 0 }).unwrap();
        sim.apply(Action::Drop).unwrap(); // the push is lost
        sim.apply(Action::Restart { client: 0 }).unwrap();
        sim.settle();
        assert_eq!(sim.host.handler_calls(), 1);
        assert_eq!(sim.client(0).pending_count().unwrap(), 0);
        assert_eq!(sim.host.state(&entry_key("e1")).unwrap()["text"], "hi");
    }
}
