//! The native runtime actor: one [`ClientRuntime`] and its SQLite connection
//! per open client, on a thread of its own, reached through a mailbox and an
//! outbox ([#134](https://github.com/zanminwang/axton/issues/134)).
//!
//! A carrier sees four calls. [`open`] registers the runtime and starts its
//! thread; [`submit`] admits one envelope into the mailbox (admission, never
//! completion); the actor publishes events into the outbox and calls the
//! carrier's [`WakeSink`] when the outbox turns non-empty; [`drain`] takes
//! what is there. [`detach`] ends the carrier's side for good.
//!
//! The process-wide registry lock covers only map lookups and insertions: it
//! is never held during database work, while waiting for a callback, or while
//! a wake sink runs, so independent clients never wait on one another.
//!
//! Wakes are edge-triggered with a drain/recheck handshake. The first publish
//! into an empty-or-drained outbox wakes; later publishes coalesce into the
//! same wake until a drain resets it, so a publish racing a drain either lands
//! in that drain or wakes again, and nothing is lost. The sink runs outside
//! the events lock but under the sink lock, which [`detach`] takes to clear
//! it: once `detach` returns, no invocation is in flight and none follows, so
//! the carrier can free what the sink points at.
//!
//! A carrier that can be torn down without detaching - a Node env ended by
//! `worker.terminate()` - records what it opened in [`Owners`] and detaches it
//! from its teardown hook, so a lost carrier never leaves the actor and its
//! SQLite transaction alive. [`wait_closed`] waits for an actor to release its
//! store.
use axton_client::Schema;
use axton_client::runtime::{BridgeError, ClientRuntime, Diagnostic, Event, Input};
use axton_sqlite::SqliteStore;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::hash::{BuildHasher, RandomState};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Tells the carrier that runtime `id` has events to drain. It must return
/// promptly and must not call back into this module on the calling thread: it
/// runs on the actor's thread under the sink lock.
pub type WakeSink = Box<dyn Fn(u64) + Send + Sync>;

const CLOSED: &str = "client_closed";

/// What the mailbox carries: an admitted input, or an envelope that did not
/// decode, reported in order with the runtime's own events.
enum Mail {
    /// Boxed: a command carries a whole mutation or query.
    Input(Box<Input>),
    Malformed(String),
}

struct Pending {
    queue: Vec<Event>,
    wake_pending: bool,
}
struct Outbox {
    id: u64,
    events: Mutex<Pending>,
    sink: Mutex<Option<WakeSink>>,
    /// Set before `runtimeClosed` is published: admission refuses from then on.
    closed: AtomicBool,
}
impl Outbox {
    fn publish(&self, batch: Vec<Event>) {
        if batch.is_empty() {
            return;
        }
        let notify = {
            let mut pending = lock(&self.events);
            pending.queue.extend(batch);
            !std::mem::replace(&mut pending.wake_pending, true)
        };
        if notify {
            let sink = lock(&self.sink);
            if let Some(sink) = sink.as_ref() {
                // A panicking sink must not take the actor, and the client's
                // open transaction, down with it.
                let _ = catch_unwind(AssertUnwindSafe(|| sink(self.id)));
            }
        }
    }
    fn drain(&self) -> Vec<Event> {
        let mut pending = lock(&self.events);
        pending.wake_pending = false;
        std::mem::take(&mut pending.queue)
    }
}

struct Handle {
    sender: Sender<Mail>,
    outbox: Arc<Outbox>,
}
struct Registry {
    next: u64,
    runtimes: BTreeMap<u64, Handle>,
}
static REGISTRY: Mutex<Registry> = Mutex::new(Registry {
    next: 0,
    runtimes: BTreeMap::new(),
});

/// The actor threads that have not ended, and the signal of each end.
struct Running {
    ids: Mutex<BTreeSet<u64>>,
    ended: Condvar,
}
static RUNNING: Running = Running {
    ids: Mutex::new(BTreeSet::new()),
    ended: Condvar::new(),
};
/// Held by an actor thread for its whole life; its drop - after the runtime
/// and its store are gone, on every exit path - announces the end.
struct Alive(u64);
impl Drop for Alive {
    fn drop(&mut self) {
        lock(&RUNNING.ids).remove(&self.0);
        RUNNING.ended.notify_all();
    }
}

/// Wait up to `timeout` for the thread of `runtime` to end, and say whether
/// it has: from then on its store is closed and its files are released. True
/// at once for a runtime that ended or never existed. It never runs work, so
/// a carrier may call it after [`detach`]; not from a wake sink, whose actor
/// would be waiting on it.
pub fn wait_closed(runtime: u64, timeout: Duration) -> bool {
    let ids = lock(&RUNNING.ids);
    let (ids, _) = RUNNING
        .ended
        .wait_timeout_while(ids, timeout, |ids| ids.contains(&runtime))
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    !ids.contains(&runtime)
}

/// The runtimes each carrier instance opened and has not detached, keyed by
/// an address that identifies the instance while it lives (a Node env). The
/// instance's teardown hook calls [`Owners::lost`], which detaches whatever
/// it still holds; that is how a lost carrier closes its runtimes.
pub struct Owners {
    opened: Mutex<BTreeMap<usize, BTreeSet<u64>>>,
}
impl Default for Owners {
    fn default() -> Self {
        Self::new()
    }
}
impl Owners {
    pub const fn new() -> Self {
        Self {
            opened: Mutex::new(BTreeMap::new()),
        }
    }
    /// Track `owner`, calling `install` - which registers its teardown hook -
    /// only the first time, until [`Owners::lost`] forgets it. A failed
    /// `install` leaves it untracked and is answered.
    pub fn watch<E>(
        &self,
        owner: usize,
        install: impl FnOnce() -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        if let std::collections::btree_map::Entry::Vacant(entry) = lock(&self.opened).entry(owner) {
            install()?;
            entry.insert(BTreeSet::new());
        }
        Ok(())
    }
    /// `owner` opened `runtime`.
    pub fn opened(&self, owner: usize, runtime: u64) {
        lock(&self.opened).entry(owner).or_default().insert(runtime);
    }
    /// The carrier detached `runtime` itself. The owner stays tracked: its
    /// hook is still installed.
    pub fn detached(&self, runtime: u64) {
        for runtimes in lock(&self.opened).values_mut() {
            runtimes.remove(&runtime);
        }
    }
    /// `owner` is gone: forget it and [`detach`] every runtime it still held,
    /// which closes each one's actor. Answers those runtimes. The lock is not
    /// held while detaching.
    pub fn lost(&self, owner: usize) -> Vec<u64> {
        let held = lock(&self.opened).remove(&owner).unwrap_or_default();
        for runtime in &held {
            detach(*runtime);
        }
        held.into_iter().collect()
    }
}

/// A poisoned lock only means another thread panicked while holding it; the
/// guarded maps and queues stay consistent, so carry on with them.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Open a runtime for `{"type":"open","requestId","path","schema","discardPending"?}`
/// and answer its id (never 0, never reused). The open itself runs on the new
/// thread and completes `requestId` through the outbox; a failed open
/// completes it with the error and closes the runtime. Only a request that
/// cannot be routed at all is refused here.
pub fn open(request: Value, wake: WakeSink) -> std::result::Result<u64, String> {
    if request["type"] != "open" {
        return Err("open request must have type open".into());
    }
    let request_id = request["requestId"]
        .as_str()
        .ok_or("requestId must be string")?
        .to_string();
    let (sender, receiver) = mpsc::channel();
    // The id is allocated and registered before the thread exists, so no wake
    // can name an id the carrier does not know yet.
    let outbox = {
        let mut registry = lock(&REGISTRY);
        let id = registry
            .next
            .checked_add(1)
            .ok_or("runtime identifiers exhausted")?;
        registry.next = id;
        let outbox = Arc::new(Outbox {
            id,
            events: Mutex::new(Pending {
                queue: vec![],
                wake_pending: false,
            }),
            sink: Mutex::new(Some(wake)),
            closed: AtomicBool::new(false),
        });
        registry.runtimes.insert(
            id,
            Handle {
                sender,
                outbox: outbox.clone(),
            },
        );
        lock(&RUNNING.ids).insert(id);
        outbox
    };
    let id = outbox.id;
    let alive = Alive(id);
    let spawned = std::thread::Builder::new()
        .name(format!("axton-runtime-{id}"))
        .spawn(move || {
            let _alive = alive;
            run(request_id, request, receiver, outbox)
        });
    if let Err(e) = spawned {
        // The closure, and `alive` with it, was dropped: the id is not running.
        lock(&REGISTRY).runtimes.remove(&id);
        return Err(e.to_string());
    }
    Ok(id)
}

/// Admit one envelope. `Err("client_closed")` when the runtime is unknown,
/// detached or closed; an envelope that does not decode is admitted and
/// answered with a protocol report, since the carrier cannot tell it apart
/// from a newer SDK's message.
pub fn submit(runtime: u64, message: Value) -> std::result::Result<(), String> {
    let (sender, outbox) = {
        let registry = lock(&REGISTRY);
        let handle = registry.runtimes.get(&runtime).ok_or(CLOSED)?;
        (handle.sender.clone(), handle.outbox.clone())
    };
    if outbox.closed.load(Ordering::SeqCst) {
        return Err(CLOSED.into());
    }
    let mail = match serde_json::from_value::<Input>(message) {
        Ok(input) => Mail::Input(Box::new(input)),
        Err(e) => Mail::Malformed(BridgeError::Malformed(e.to_string()).to_string()),
    };
    sender.send(mail).map_err(|_| CLOSED.to_string())
}

/// Take the events published so far, in order, as JSON. Empty when there are
/// none or the runtime is unknown.
pub fn drain(runtime: u64) -> Vec<Value> {
    let outbox = lock(&REGISTRY)
        .runtimes
        .get(&runtime)
        .map(|handle| handle.outbox.clone());
    let Some(outbox) = outbox else {
        return vec![];
    };
    outbox
        .drain()
        .into_iter()
        .map(|event| {
            serde_json::to_value(event).unwrap_or_else(
                |e| json!({"type":"report","diagnostic":{"kind":"error","message":e.to_string()}}),
            )
        })
        .collect()
}

/// End the carrier's side of `runtime`: stop its wakes for good, forget it,
/// and close it if its thread still runs. When this returns the sink is not
/// running and will never run again. It does not wait for the actor to end
/// ([`wait_closed`] does); it blocks only while a wake already in flight
/// returns.
///
/// Safe from any thread, concurrently, repeatedly, and for an id that was
/// already detached, closed or never opened: all of those return at once. It
/// runs no carrier code, so a finalizer (a Dart `NativeFinalizer`, a Node env
/// cleanup hook) may call it. It must not be called from inside the wake sink
/// of the same runtime, which runs under the lock it takes.
pub fn detach(runtime: u64) {
    let outbox = lock(&REGISTRY)
        .runtimes
        .get(&runtime)
        .map(|handle| handle.outbox.clone());
    let Some(outbox) = outbox else {
        return;
    };
    // Waits for an invocation in flight; dropped outside the lock.
    let sink = lock(&outbox.sink).take();
    drop(sink);
    let handle = lock(&REGISTRY).runtimes.remove(&runtime);
    if let Some(handle) = handle {
        // A thread that already exited has dropped its mailbox; either way
        // the last sender goes with the handle.
        let _ = handle.sender.send(Mail::Input(Box::new(Input::Close)));
    }
}

/// The facts the actor supplies to every call.
fn facts() -> (u64, u64) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0);
    (now, RandomState::new().hash_one(now))
}

fn open_runtime(request: &Value) -> axton_client::Result<ClientRuntime<SqliteStore>> {
    let path = request["path"]
        .as_str()
        .ok_or_else(|| axton_client::invalid("path must be string"))?;
    let discard = match request.get("discardPending") {
        None | Some(Value::Null) => false,
        Some(Value::Bool(discard)) => *discard,
        Some(_) => return Err(axton_client::invalid("discardPending must be bool")),
    };
    ClientRuntime::open_at(
        path,
        Schema::from_value(request["schema"].clone())?,
        Box::new(|file| SqliteStore::open(file)),
        discard,
    )
}

/// The actor's thread: open, then admit and step until the runtime closes or
/// its carrier is gone.
fn run(request_id: String, request: Value, mailbox: Receiver<Mail>, outbox: Arc<Outbox>) {
    let opened = catch_unwind(|| open_runtime(&request))
        .unwrap_or_else(|_| Err(axton_client::invalid("runtime panic")));
    let mut runtime = match opened {
        Ok(runtime) => runtime,
        Err(e) => {
            outbox.closed.store(true, Ordering::SeqCst);
            outbox.publish(vec![
                Event::TaskCompleted {
                    request_id,
                    ok: false,
                    value: Value::Null,
                    error: Some(e.to_string()),
                    details: None,
                },
                Event::RuntimeClosed,
            ]);
            return;
        }
    };
    outbox.publish(vec![Event::TaskCompleted {
        request_id,
        ok: true,
        value: runtime.opened(),
        error: None,
        details: None,
    }]);
    let served = catch_unwind(AssertUnwindSafe(|| serve(&mut runtime, &mailbox, &outbox)));
    if served.is_ok() {
        // Release the store before announcing the end: once the carrier sees
        // `runtimeClosed`, the database files are no longer held.
        drop(runtime);
        outbox.publish(vec![Event::RuntimeClosed]);
    } else {
        // The runtime's state is not trusted after a panic; dropping it
        // closes its connections, which rolls back an open transaction.
        drop(runtime);
        outbox.closed.store(true, Ordering::SeqCst);
        outbox.publish(vec![
            Event::Report {
                diagnostic: Diagnostic::Error {
                    message: "runtime panic".into(),
                    status: None,
                },
            },
            Event::RuntimeClosed,
        ]);
    }
}

fn serve(runtime: &mut ClientRuntime<SqliteStore>, mailbox: &Receiver<Mail>, outbox: &Outbox) {
    let mut arrived = VecDeque::new();
    loop {
        // Admit everything already delivered, without blocking.
        let mut lost = false;
        loop {
            match mailbox.try_recv() {
                Ok(mail) => arrived.push_back(mail),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    lost = true;
                    break;
                }
            }
        }
        for mail in arrived.drain(..) {
            admit(runtime, mail, outbox);
        }
        if lost {
            // The carrier is gone without closing: never leave the SQLite
            // transaction open behind it.
            let (now, entropy) = facts();
            let _ = runtime.receive(Input::Close, now, entropy);
        }
        // Step until idle, yielding to new mail after every unit so control
        // and callback answers are admitted between units.
        loop {
            let (now, entropy) = facts();
            if !runtime.step(now, entropy) {
                break;
            }
            flush(runtime, outbox);
            match mailbox.try_recv() {
                Ok(mail) => {
                    arrived.push_back(mail);
                    break;
                }
                Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Disconnected) => break,
            }
        }
        flush(runtime, outbox);
        if runtime.closed() {
            return;
        }
        // Block for the next mail. A disconnected mailbox answers at once;
        // the next turn sees it and closes.
        if arrived.is_empty()
            && let Ok(mail) = mailbox.recv()
        {
            arrived.push_back(mail);
        }
    }
}

fn admit(runtime: &mut ClientRuntime<SqliteStore>, mail: Mail, outbox: &Outbox) {
    match mail {
        Mail::Input(input) => {
            let (now, entropy) = facts();
            // After close nothing is admitted; `runtimeClosed` tells the SDK
            // to settle whatever it still routes.
            let _ = runtime.receive(*input, now, entropy);
        }
        Mail::Malformed(message) => {
            flush(runtime, outbox);
            outbox.publish(vec![Event::Report {
                diagnostic: Diagnostic::Protocol { message },
            }]);
        }
    }
}

fn flush(runtime: &mut ClientRuntime<SqliteStore>, outbox: &Outbox) {
    let mut events = runtime.take_events();
    if runtime.closed() {
        outbox.closed.store(true, Ordering::SeqCst);
        // `run` announces the end once the runtime and its store are dropped.
        events.retain(|event| !matches!(event, Event::RuntimeClosed));
    }
    outbox.publish(events);
}
