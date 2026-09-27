//! The scripted host every Downlink worker test drives: one lane, one client,
//! and the matchers its actions are asserted with. Real sockets and HTTP are
//! the SDK suites' job; here every event is a value and every action is
//! asserted ([downlink_worker.rs](../downlink_worker.rs),
//! [bootstrap_worker.rs](../bootstrap_worker.rs)).
use super::*;
use axton_client::*;
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};

pub fn text(page: &PullPage) -> String {
    String::from_utf8(page.encode().unwrap()).unwrap()
}
/// The acknowledgement: every channel at its current head.
pub fn ack(heads: &[(&str, u64)]) -> String {
    let ack =
        SubscriptionAck::new(heads.iter().map(|(c, h)| (c.to_string(), *h)).collect()).unwrap();
    String::from_utf8(ack.encode().unwrap()).unwrap()
}
/// What an applied page answers: the push lane wakes and the Scopes whose
/// cursors the commit moved.
pub fn applied(scopes: &[&str]) -> Vec<DownlinkAction> {
    vec![
        DownlinkAction::Wake { lane: "push" },
        DownlinkAction::Changed {
            scopes: scopes.iter().map(|s| s.to_string()).collect(),
        },
    ]
}
/// The handshake's own action: delivery is established for these Scopes,
/// whether or not the acknowledgement committed a boundary for any of them.
pub fn established(scopes: &[&str]) -> DownlinkAction {
    DownlinkAction::Acknowledged {
        scopes: scopes.iter().map(|s| s.to_string()).collect(),
    }
}
pub fn request(action: &DownlinkAction) -> (u64, PullRequest) {
    match action {
        DownlinkAction::Request { request, body, .. } => {
            (*request, PullRequest::decode(body.as_bytes()).unwrap())
        }
        other => panic!("expected a request, got {other:?}"),
    }
}
pub fn cursors(request: &PullRequest) -> Vec<(&str, u64)> {
    request
        .cursors
        .iter()
        .map(|(c, n)| (c.as_str(), *n))
        .collect()
}
pub fn opened(action: &DownlinkAction) -> (u64, SubscribeRequest) {
    match action {
        DownlinkAction::Open { epoch, subscribe } => (
            *epoch,
            SubscribeRequest::decode(subscribe.as_bytes()).unwrap(),
        ),
        other => panic!("expected an open, got {other:?}"),
    }
}
pub fn wait(action: &DownlinkAction) -> u64 {
    match action {
        DownlinkAction::Wait { millis } => *millis,
        other => panic!("expected a wait, got {other:?}"),
    }
}
pub fn waiting(action: &DownlinkAction) -> bool {
    matches!(action, DownlinkAction::Wait { .. })
}
pub fn reports(action: &DownlinkAction) -> &[Report] {
    match action {
        DownlinkAction::Report { reports } => reports,
        other => panic!("expected reports, got {other:?}"),
    }
}
/// The first two actions of an applied page: the push wake and the commit.
pub fn committed(actions: &[DownlinkAction], scopes: &[&str]) {
    assert_eq!(&actions[..2], &applied(scopes)[..]);
}
pub fn empty(channel: &str, at: u64) -> PullPage {
    multi(&[(channel, at, at, at)], vec![])
}
/// A full page of `channel` from `from`: fifty records, the channel continues.
pub fn full(channel: &str, from: u64) -> PullPage {
    let to = from + limits::PULL_CHANGES as u64;
    multi(
        &[(channel, from, to, to + 1)],
        (1..=limits::PULL_CHANGES as u64)
            .map(|i| AuthorityRecord {
                model: "Entry".into(),
                identity: json!({"id":format!("{i}")}),
                stamp: from + i,
                state: json!({"text":"bulk","note":null}),
                error: None,
            })
            .collect(),
    )
}

pub struct Lane {
    pub client: Client<SqliteStore>,
    pub worker: DownlinkWorker,
    pub now: u64,
}
impl Lane {
    pub fn new(dir: &std::path::Path) -> Self {
        Self::of(super::open(&dir.join("db")))
    }
    /// A replica rebuilt from the layout this issue replaced: `a` and `b` carry
    /// over as Scope names with fresh identities and no delivery boundary, and
    /// their old cursors stay in the file that was left behind
    /// ([rebuild](rebuild.rs)).
    pub fn rebuilt(dir: &std::path::Path) -> Self {
        let path = dir.join("db");
        let mut store = SqliteStore::open(&path).unwrap();
        store
            .execute_batch(
                "CREATE TABLE axton_client (client_id TEXT PRIMARY KEY, next_ordinal INTEGER NOT NULL, next_push INTEGER NOT NULL, generation INTEGER NOT NULL, last_completed_push INTEGER NOT NULL DEFAULT 0, push_models TEXT, push_results TEXT);
                 CREATE TABLE axton_subscription (channel TEXT PRIMARY KEY, cursor INTEGER NOT NULL);
                 INSERT INTO axton_client (client_id, next_ordinal, next_push, generation) VALUES ('old', 1, 1, 1);
                 INSERT INTO axton_subscription VALUES ('a', 9);
                 INSERT INTO axton_subscription VALUES ('b', 4);",
            )
            .unwrap();
        drop(store);
        let factory: StoreFactory<SqliteStore> = Box::new(|p| SqliteStore::open(p));
        let client = Client::open_at(&path, schema(), factory, false).unwrap();
        assert!(client.schema_state().rebuilt);
        Self::of(client)
    }
    /// A replica whose schema no longer fits, held open in place because it
    /// still has unsent work: `rebuild(true)` then switches this client to a
    /// fresh file in the same process, carrying its Scope names with fresh
    /// identities and no delivery boundary ([rebuild](rebuild.rs)). Until then
    /// the old file syncs like any other, so a lane can run on it first.
    pub fn rebuildable(path: &std::path::Path) -> Self {
        let factory = || -> StoreFactory<SqliteStore> { Box::new(|p| SqliteStore::open(p)) };
        let mut client = Client::open_at(path, schema(), factory(), false).unwrap();
        super::seed(&mut client, "old");
        client
            .transaction(|tx| tx.enqueue(mutation("unsent")).map(|_| ()))
            .unwrap();
        drop(client);
        let mut breaking = serde_json::to_value(schema()).unwrap();
        breaking["models"][0]["fields"]
            .as_array_mut()
            .unwrap()
            .push(json!({"name":"due","nullable":false,"type":{"kind":"scalar","name":"string"}}));
        let breaking = Schema::from_value(breaking).unwrap();
        let client = Client::open_at(path, breaking, factory(), false).unwrap();
        assert!(client.schema_state().pending.is_some(), "the rebuild waits");
        Self {
            client,
            worker: DownlinkWorker::default(),
            now: 1_000,
        }
    }
    pub fn of(mut client: Client<SqliteStore>) -> Self {
        super::seed(&mut client, "local");
        Self {
            client,
            worker: DownlinkWorker::default(),
            now: 1_000,
        }
    }
    /// One enqueue: typed state only. It answers with no actions, so nothing
    /// the host does depends on a callback's return.
    pub fn enqueue(&mut self, event: DownlinkEvent) {
        assert_eq!(
            self.worker
                .handle(&mut self.client, event, self.now, 500)
                .unwrap(),
            vec![],
            "an enqueue decides nothing: the pump answers"
        );
    }
    /// One bounded pump: at most one page application, and at most one sleep.
    /// Both host loops sleep the last `wait` of a pump, so a pump that asked
    /// for two would silently discard the earlier - and shorter - one.
    pub fn pump(&mut self) -> Vec<DownlinkAction> {
        let actions = self
            .worker
            .handle(&mut self.client, DownlinkEvent::Next, self.now, 500)
            .unwrap();
        assert!(
            actions.iter().filter(|a| waiting(a)).count() <= 1,
            "one pump asks for one sleep: {actions:?}"
        );
        actions
    }
    /// The host loop: pump until the worker waits or has nothing left, in order.
    pub fn drain(&mut self) -> Vec<DownlinkAction> {
        let mut actions = vec![];
        for _ in 0..=QUEUED_FRAMES + 2 {
            let pumped = self.pump();
            let stop = pumped.is_empty() || pumped.iter().any(waiting);
            actions.extend(pumped);
            if stop {
                return actions;
            }
        }
        panic!("the pump never went idle: {actions:?}")
    }
    /// Enqueue one event and run the host loop over its consequences.
    pub fn send(&mut self, event: DownlinkEvent) -> Vec<DownlinkAction> {
        self.enqueue(event);
        self.drain()
    }
    pub fn message(&mut self, epoch: u64, body: String) -> Vec<DownlinkAction> {
        self.send(DownlinkEvent::Message { epoch, body })
    }
    pub fn frame(&mut self, epoch: u64, page: &PullPage) -> Vec<DownlinkAction> {
        self.message(epoch, text(page))
    }
    pub fn response(&mut self, request: u64, page: &PullPage) -> Vec<DownlinkAction> {
        self.send(DownlinkEvent::Response {
            request,
            body: text(page),
        })
    }
    /// How far `channel` committed delivery; `None` while it waits for its
    /// first boundary.
    pub fn cursor(&mut self, channel: &str) -> Option<u64> {
        self.client.cursor(channel).unwrap()
    }
    pub fn set(&mut self, channel: &str, subscribed: bool) {
        self.client
            .transaction(|tx| tx.set_channel(channel.into(), subscribed))
            .unwrap();
    }
    /// A subscription an earlier session left at `cursor`: registered, and its
    /// first boundary already committed there, as that session's
    /// acknowledgement did. Every session this lane then opens negotiates
    /// against durable progress instead of initializing.
    pub fn saved(&mut self, channel: &str, cursor: u64) {
        self.set(channel, true);
        acknowledge(&mut self.client, &[(channel, cursor)]);
        assert_eq!(self.cursor(channel), Some(cursor));
    }
    pub fn text(&mut self) -> Value {
        self.client.read(&key()).unwrap().unwrap()["text"].clone()
    }
    /// Subscribe `channel`, start the lane and acknowledge at `head`: the
    /// epoch of the socket and what the acknowledgement asked for. A channel
    /// with no committed boundary initializes at `head`; one [`Lane::saved`]
    /// left behind catches up to it.
    pub fn streaming(&mut self, channel: &str, head: u64) -> (u64, Vec<DownlinkAction>) {
        self.set(channel, true);
        let actions = self.send(DownlinkEvent::Start);
        let (epoch, subscribe) = opened(&actions[0]);
        assert_eq!(subscribe.channels, [channel]);
        let acknowledged = self.message(epoch, ack(&[(channel, head)]));
        (epoch, acknowledged)
    }
}
