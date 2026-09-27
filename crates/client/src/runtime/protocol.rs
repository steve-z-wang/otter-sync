//! The bridge contract between an SDK and its Rust-owned client runtime
//! ([#134](https://github.com/zanminwang/axton/issues/134)).
//!
//! An SDK submits complete tasks and answers the effects the runtime asks
//! for; the runtime owns every decision in between and reports outcomes as
//! events. JSON stays the cross-language encoding: every [`Input`] and
//! [`Event`] is one tagged object, and every identifier the runtime allocates
//! travels as a string so no language has to represent a 64-bit counter.
//!
//! Identities:
//!
//! | Identity | Allocated by | Purpose |
//! | --- | --- | --- |
//! | `requestId` | the SDK, increasing per bridge, never reused | route one submitted task or transaction command to its one terminal outcome |
//! | `effectId` | the runtime, unique for its lifetime | correlate one HTTP/timer/callback effect or the lifetime of one socket |
//! | `transactionId` | the runtime, fresh per callback transaction | admit commands only into the transaction that owns them |
//! | `scope` | the runtime, fresh per nested savepoint | admit commands only into the innermost open savepoint |
//! | `callId` | the existing durable call identity | final Call outcomes; never replaced by a request id |
//! | `observerId` | the runtime, unique for its lifetime | route watch/subscription snapshots ([`Event::ObserverChanged`]) |
//!
//! Admission is not completion: the carrier acknowledges that a message was
//! copied into the runtime's mailbox, and the public Promise/Future resolves
//! only from the matching [`Event::TaskCompleted`]. A duplicate active request
//! id is a protocol error reported through [`Diagnostic::Protocol`]; it never
//! executes a second time and never touches the first route.
//!
//! Commands are typed: a task carries a [`Command`] and a callback's command a
//! [`TransactionCommand`], each one `{kind, …}` object. An envelope that does
//! not decode at all is a [`Diagnostic::Protocol`] report, since nothing in it
//! can be routed. An envelope that decodes but whose command does not - an
//! unknown `kind`, a missing or mistyped field - is still admitted under its
//! request id and completes that request with the decoding error, in its
//! turn, so no SDK waiter is left without an answer.
use crate::{Mutation, QuerySpec, Readiness, RecordKey, Report};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

/// What an SDK sends to its runtime.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Input {
    /// One complete unit of application work. A [`Command::Transaction`]
    /// asks for an application callback: the runtime answers with an
    /// [`Operation::Callback`] effect and completes the task only after the
    /// callback's result committed or rolled back.
    #[serde(rename_all = "camelCase")]
    Task {
        request_id: String,
        #[serde(deserialize_with = "command")]
        command: Command,
    },
    /// A command of the application callback that owns `transaction_id`:
    /// reads, writes, and `savepoint` / `release` / `rollbackSavepoint`.
    /// `scope` names the innermost open savepoint the command runs in, or is
    /// absent at the transaction's top level. A command naming a transaction
    /// that is not open fails with `transaction_closed`; one naming a scope
    /// that is not the innermost open one fails with `invalid transaction
    /// scope`, a structural failure that rolls the whole unit back at the
    /// callback's end. Neither joins the session. These commands are serviced
    /// on their own lane, never behind the ordinary queue their parent task is
    /// holding.
    #[serde(rename_all = "camelCase")]
    TransactionCommand {
        request_id: String,
        transaction_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        scope: Option<String>,
        #[serde(deserialize_with = "transaction_command")]
        command: TransactionCommand,
    },
    /// The application callback of `effect_id` finished. `ok` commits the
    /// transaction (the SDK retains the callback's own return value in
    /// language memory and resolves with it once the commit is confirmed);
    /// a failure rolls it back and the parent task fails with `error`, which
    /// is the SDK's rendering of the thrown value. A result naming a
    /// transaction that is not open is ignored.
    #[serde(rename_all = "camelCase")]
    CallbackResult {
        effect_id: String,
        transaction_id: String,
        ok: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    /// The host's answer to one effect. HTTP, timer, credential and
    /// prerequisite effects are single-use: their first result retires the
    /// id and a later one is ignored. A socket effect is a stream: every
    /// result carries a [`SocketEvent`] value under the same id until
    /// `closed`, and a result for an id the runtime cancelled or never issued
    /// is ignored.
    #[serde(rename_all = "camelCase")]
    EffectResult {
        effect_id: String,
        outcome: EffectOutcome,
    },
    /// Close the runtime: priority control, never a task parked behind a
    /// callback. Every pending task completes or fails, an open transaction
    /// rolls back, every effect is cancelled, observers end, then
    /// [`Event::RuntimeClosed`] is the last event.
    Close,
}

/// The command of one task: everything an SDK asks of its runtime outside an
/// application callback. Reads run on the committed state; every write owns
/// its own local transaction; the lifecycles (`connect`, `invoke`,
/// `runPrerequisites`, `transaction`, `scopeBootstrap`, `watch`) are decided
/// by the runtime. A counter the engine validates (`version` of a durable
/// call, `subscriptionId`, `ordinal`, `sequence`) must be a positive safe
/// integer and is refused otherwise.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum Command {
    // --- Reads ---
    /// One record, or `null`.
    Read { key: RecordKey },
    /// The rows of `model` matching `filter` (every row when absent).
    Query {
        model: String,
        #[serde(
            default,
            deserialize_with = "present",
            skip_serializing_if = "Option::is_none"
        )]
        filter: Option<Value>,
    },
    /// A read-only SQL statement over the committed state.
    Sql { sql: String, parameters: Vec<Value> },
    #[serde(rename_all = "camelCase")]
    QuerySpec { model: String, query: QuerySpec },
    /// The record a relation of `key` points at, or `null`.
    Related { key: RecordKey, relation: String },
    /// The records of `source` whose `relation` points at `key`.
    Referencing {
        key: RecordKey,
        source: String,
        relation: String,
    },
    /// `status()`: the client's sync state.
    Status,
    /// One record's pending mutations and retained rejections.
    RecordStatus { key: RecordKey },
    /// The prerequisite tasks and their states.
    Tasks,

    // --- Writes, each in its own local transaction ---
    /// Queue a mutation; answers its ordinal.
    Enqueue { mutation: Mutation },
    /// Apply one local-only operation.
    Direct { operation: crate::Operation },
    /// Subscribe or unsubscribe a Channel.
    Channel { channel: String, subscribed: bool },
    /// Submit a durable Action call; answers `{callId, ordinal}`. `store` is
    /// the call's store policy, beside its arguments, never inside them.
    SubmitAction {
        name: String,
        #[serde(deserialize_with = "counter")]
        version: u64,
        args: Value,
        #[serde(
            default,
            deserialize_with = "present",
            skip_serializing_if = "Option::is_none"
        )]
        store: Option<Value>,
    },
    /// Record a prerequisite task's readiness.
    Readiness { key: String, state: Readiness },
    /// Drop a queued call or mutation; its call completes as `dropped`.
    Drop {
        #[serde(deserialize_with = "counter")]
        ordinal: u64,
    },
    /// Dismiss a retained rejection.
    Dismiss {
        #[serde(deserialize_with = "counter")]
        ordinal: u64,
    },
    /// Discard the saved Query once results of one argument set.
    InvalidateQueryOnce {
        name: String,
        #[serde(deserialize_with = "counter")]
        version: u64,
        args: Value,
    },
    /// Leave an incompatible replica behind for a fresh file; refused while
    /// unsent work remains unless `discardPending`.
    #[serde(rename_all = "camelCase")]
    Rebuild {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        discard_pending: Option<bool>,
    },

    // --- Protocol seams for tests and tools; the lanes never use them ---
    /// Freeze the next push batch; answers its body or `null`.
    Freeze,
    /// Settle batch `sequence` with `receipt`.
    Ack {
        #[serde(deserialize_with = "counter")]
        sequence: u64,
        receipt: Value,
    },
    /// Apply one pull page.
    Pull { page: Value },

    // --- Scope and Bootstrap ---
    /// Register durable intent to follow `scope`; answers the stored state
    /// and the observer publishing its status.
    ScopeSubscribe { scope: String },
    /// The stored state of `scope`, or `null`.
    ScopeState { scope: String },
    /// Register (or retry) the durable load of one identity and wait for the
    /// run it answered with.
    #[serde(rename_all = "camelCase")]
    ScopeBootstrap {
        scope: String,
        #[serde(deserialize_with = "counter")]
        subscription_id: u64,
    },
    /// The stored run of one identity's load.
    #[serde(rename_all = "camelCase")]
    ScopeBootstrapState {
        scope: String,
        #[serde(deserialize_with = "counter")]
        subscription_id: u64,
    },
    /// Remove exactly the registration `subscriptionId` names.
    #[serde(rename_all = "camelCase")]
    ScopeUnsubscribe {
        scope: String,
        #[serde(deserialize_with = "counter")]
        subscription_id: u64,
    },

    // --- Runtime-owned lifecycles ---
    /// Run an application callback in one local transaction.
    Transaction,
    /// Record the connection intent and start both lanes. `directTimeoutMs`
    /// bounds one direct call (1 to 2147483647, 30000 when absent).
    #[serde(rename_all = "camelCase")]
    Connect {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        direct_timeout_ms: Option<u64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        refresh_auth: Option<bool>,
    },
    /// Control the runtime-owned connection; a no-op without one.
    Connection { event: ConnectionEvent },
    /// A direct Query or Mutation call; with `once`, a Query answered from
    /// the saved result when there is one (`refresh` fetches anyway).
    Invoke {
        name: String,
        version: u64,
        args: Value,
        #[serde(
            default,
            deserialize_with = "present",
            skip_serializing_if = "Option::is_none"
        )]
        store: Option<Value>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        once: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        refresh: Option<bool>,
    },
    /// Run the prerequisite tasks these handlers can take until none is left.
    RunPrerequisites { handlers: Vec<String> },
    /// Observe a local query; answers the observer id.
    Watch {
        model: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        spec: Option<WatchSpec>,
    },
    /// Stop publishing a watch.
    #[serde(rename_all = "camelCase")]
    Unwatch { observer_id: String },

    /// Never on the wire: a command that did not decode. Its request is
    /// completed with `error`, the decoding failure.
    #[serde(skip)]
    Malformed { error: String },
}

/// What a `watch` observes: the rows of its model matching `filter` (every
/// row when absent).
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct WatchSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter: Option<Value>,
}

/// The controls of the runtime-owned connection.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum ConnectionEvent {
    /// Abandon lane I/O without backoff and schedule nothing until `resume`.
    Pause,
    Resume,
    /// Look for work now.
    Wake,
    /// End the connection: lane I/O is abandoned and direct calls still
    /// waiting on the network fail as unavailable.
    Stop,
}

/// A command of the application callback that owns a transaction: reads and
/// writes inside its session, and its savepoints. `release` and
/// `rollbackSavepoint` name the scope they close, or the innermost one when
/// `scope` is absent.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum TransactionCommand {
    Read {
        key: RecordKey,
    },
    Query {
        model: String,
        #[serde(
            default,
            deserialize_with = "present",
            skip_serializing_if = "Option::is_none"
        )]
        filter: Option<Value>,
    },
    Sql {
        sql: String,
        parameters: Vec<Value>,
    },
    QuerySpec {
        model: String,
        query: QuerySpec,
    },
    Related {
        key: RecordKey,
        relation: String,
    },
    Referencing {
        key: RecordKey,
        source: String,
        relation: String,
    },
    Enqueue {
        mutation: Mutation,
    },
    Direct {
        operation: crate::Operation,
    },
    Channel {
        channel: String,
        subscribed: bool,
    },
    /// Open a savepoint; answers `{scope}`, the token its commands name.
    Savepoint,
    Release {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        scope: Option<String>,
    },
    RollbackSavepoint {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        scope: Option<String>,
    },
    /// Never on the wire: a command that did not decode. Its request fails
    /// with `error` and, like any failed command, poisons the transaction.
    #[serde(skip)]
    Malformed {
        error: String,
    },
}

/// A task's command, or [`Command::Malformed`] with why it did not decode.
fn command<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Command, D::Error> {
    let value = Value::deserialize(deserializer)?;
    Ok(
        serde_json::from_value(value).unwrap_or_else(|e| Command::Malformed {
            error: e.to_string(),
        }),
    )
}
/// A callback's command, or [`TransactionCommand::Malformed`].
fn transaction_command<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<TransactionCommand, D::Error> {
    let value = Value::deserialize(deserializer)?;
    Ok(
        serde_json::from_value(value).unwrap_or_else(|e| TransactionCommand::Malformed {
            error: e.to_string(),
        }),
    )
}
/// An optional field whose explicit `null` is kept apart from its absence:
/// absent is `None`, `null` is `Some(Value::Null)`.
fn present<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Value>, D::Error> {
    Value::deserialize(deserializer).map(Some)
}
/// A positive safe integer, refused as the engine refuses it.
fn counter<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    let value = Value::deserialize(deserializer)?;
    crate::read_counter(&value, true).map_err(serde::de::Error::custom)
}

/// What one effect came to. `ok` with `value` for a success, otherwise
/// `error`. Socket results put the [`SocketEvent`] in `value`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct EffectOutcome {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<EffectError>,
}

impl EffectOutcome {
    pub fn success(value: Value) -> Self {
        Self {
            ok: true,
            value: Some(value),
            error: None,
        }
    }
    pub fn failure(message: impl Into<String>, status: Option<u16>) -> Self {
        Self {
            ok: false,
            value: None,
            error: Some(EffectError {
                message: message.into(),
                status,
            }),
        }
    }
}

/// Why an effect failed, as the host saw it. `status` is the HTTP status the
/// failure carried, when it had one: the runtime tells a refusal the server
/// decided from a transport failure by it, and a 401 is what asks for a
/// credential refresh.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct EffectError {
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
}

/// The `value` of a socket effect's results: the stream of one socket
/// session. `opened` is optional information; `message` carries one frame;
/// `overflow` says the host's own frame buffer dropped frames; `closed` ends
/// the stream (a failure to open or a dropped socket is reported as an
/// `ok: false` result instead and ends it the same way).
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(tag = "event", rename_all = "camelCase")]
pub enum SocketEvent {
    Opened,
    Message { body: String },
    Overflow,
    Closed,
}

/// What the runtime tells its SDK, in the order it happened.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Event {
    /// The one terminal outcome of a submitted task or transaction command.
    /// The SDK removes the route before it runs application code and settles
    /// it exactly once. `error` is the human message; no SDK branch depends
    /// on its wording beyond the codes it already recognizes
    /// (`client_closed`, `transaction_closed`, `transaction_active`, …).
    #[serde(rename_all = "camelCase")]
    TaskCompleted {
        request_id: String,
        ok: bool,
        /// The task's value when `ok`; `null` otherwise.
        #[serde(default)]
        value: Value,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<String>,
        /// The machine-readable reason of a failure, when it has one: an
        /// object whose `code` the SDK maps to its public error instead of
        /// matching `error`. Absent otherwise. A failure of a registration
        /// this client no longer holds carries `{"code":"subscription.closed"}`;
        /// a `scopeBootstrap` waiter fails with `{"code":"bootstrap.superseded"}`,
        /// `{"code":"subscription.closed"}`, `{"code":"client_closed"}`, or the
        /// stored failure of its run, `{"code", "message"}` (`error` is then
        /// that stored message). A direct call that fails once it was sent
        /// carries `{"code"}` with its `error` - `action.unavailable` on stop
        /// or close - and, for `action.execution_unknown` with a known cause,
        /// that cause as `{"code", "message", "status"?}`: the transport
        /// failure, the refused credential refresh, the failed apply, or
        /// `"direct call timed out"` for the deadline.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        details: Option<Value>,
    },
    /// Host work the runtime needs: execute it and answer with an
    /// [`Input::EffectResult`] (or, for a callback, an
    /// [`Input::CallbackResult`]).
    #[serde(rename_all = "camelCase")]
    Effect {
        effect_id: String,
        operation: Operation,
    },
    /// Abort the host work of this effect if it is still running. A late
    /// answer is fenced in the runtime either way; cancellation only frees
    /// the platform resource.
    #[serde(rename_all = "camelCase")]
    CancelEffect { effect_id: String },
    /// A durable call's final outcome, emitted after the settlement that
    /// decided it committed. The SDK created the Call handle when the
    /// submission task completed, so this can never outrun its registration.
    #[serde(rename_all = "camelCase")]
    CallCompleted { call_id: String, outcome: Value },
    /// The state of one observer, emitted only when it differs from the last
    /// one emitted for it, and after the commit it describes. The SDK
    /// delivers it to the language-level listeners; a listener's exception
    /// changes nothing here. `snapshot` is one of:
    ///
    /// - a subscription observer (`scopeSubscribe`):
    ///   `{"kind":"subscription","scope","subscriptionId","status":{"active",
    ///   "initialization":"pending"|"ready","connection":"offline"|"connecting"|
    ///   "catching-up"|"live"|"stopped","bootstrap":{"phase":"not-requested"|
    ///   "waiting-for-initialization"|"loading"|"catching-up"|"complete"|
    ///   "failed","error":null|{"code","message"}}}}` - the SDK
    ///   `SubscriptionStatus`, verbatim;
    /// - a watch observer (`watch`): `{"kind":"watch","rows":[…]}`.
    ///
    /// A terminal snapshot adds `"closed": true` and nothing follows it for
    /// that observer: a subscription that was removed, replaced by a rebuild
    /// or stopped with the runtime (`active: false`, `connection: "stopped"`),
    /// or a watch ended by the runtime's close (carrying its last rows). An
    /// `unwatch` ends a watch with no snapshot.
    #[serde(rename_all = "camelCase")]
    ObserverChanged {
        observer_id: String,
        snapshot: Value,
    },
    /// Something the application should hear about that is not a task
    /// outcome: records a delivery could not apply, a lane failure, or a
    /// bridge protocol violation.
    Report { diagnostic: Diagnostic },
    /// The runtime is gone; nothing follows. The SDK drains it, releases its
    /// platform resources and detaches the carrier.
    RuntimeClosed,
}

/// The host work one effect asks for.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum Operation {
    /// Run the application's transaction callback for the `transaction` task
    /// `request_id`; its commands carry `transaction_id`, and its end is a
    /// [`Input::CallbackResult`] for this effect.
    #[serde(rename_all = "camelCase")]
    Callback {
        transaction_id: String,
        request_id: String,
    },
    /// `POST` `body` to the route: `push` is `/sync/mutations`, `pull` is
    /// `/sync/pull`, `action` is `/sync/actions`. Answer `ok` with
    /// `{"status": <HTTP status>, "body": <response text>}` (a bare string is
    /// read as the body), or a failure carrying the HTTP status when there was
    /// one: a non-2xx answer is a failure, and a 401 is what asks for a
    /// credential refresh.
    Http { route: HttpRoute, body: String },
    /// Open the live socket, send `subscribe` once it is open, and stream its
    /// frames as [`SocketEvent`] results under this id until it closes or is
    /// cancelled.
    Socket { subscribe: String },
    /// Answer after `millis`; cancellation clears the timer.
    Timer { millis: u64 },
    /// Run the application's `refreshAuth` once; answer when it settles.
    /// The runtime issues at most one at a time.
    RefreshAuth,
    /// Run the application's prerequisite handler `name` with `arguments`;
    /// answer `ok` when it resolves, or a failure whose message is the reason
    /// to keep for the task `key`.
    Prerequisite {
        key: String,
        name: String,
        arguments: Value,
    },
}

/// Which backend route an HTTP effect posts to.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum HttpRoute {
    /// `/sync/mutations`: a frozen push batch.
    Push,
    /// `/sync/pull`: an ordinary catch-up or a Bootstrap page.
    Pull,
    /// `/sync/actions`: a direct Query or Mutation.
    Action,
}

/// What a [`Event::Report`] carries.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum Diagnostic {
    /// Records a receipt, page or direct response could not apply; the
    /// client stays consistent and the application hears about each one.
    Records { reports: Vec<Report> },
    /// A lane or effect failure the application's `onError` would have seen:
    /// a transport error, a protocol violation the runtime closed a socket
    /// for, a failed credential refresh, a watch that failed to re-run.
    /// `status` is the HTTP status the failure carried, when it had one, so
    /// the SDK can hand `onError` an error with that status.
    Error {
        message: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        status: Option<u16>,
    },
    /// The SDK violated the bridge contract: a malformed envelope or a
    /// duplicate active request id. Nothing executed for it.
    Protocol { message: String },
}

/// Why the runtime refused to admit an input. Admission failures are answered
/// synchronously by the carrier; they are never task outcomes.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum BridgeError {
    /// The runtime has closed; nothing more is admitted.
    #[error("client_closed")]
    Closed,
    /// The envelope did not decode as an [`Input`].
    #[error("malformed bridge message: {0}")]
    Malformed(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn envelopes_round_trip_with_the_documented_spellings() {
        let inputs = [
            json!({"type":"task","requestId":"42","command":{"kind":"read","key":{"model":"Todo","identity":{"id":"t"}}}}),
            json!({"type":"transactionCommand","requestId":"43","transactionId":"tx7","scope":"sp1","command":{"kind":"release","scope":"sp1"}}),
            json!({"type":"callbackResult","effectId":"5","transactionId":"tx7","ok":false,"error":"boom"}),
            json!({"type":"effectResult","effectId":"101","outcome":{"ok":true,"value":{"event":"message","body":"{}"}}}),
            json!({"type":"effectResult","effectId":"102","outcome":{"ok":false,"error":{"message":"pull failed","status":401}}}),
            json!({"type":"close"}),
        ];
        for wire in inputs {
            let typed: Input = serde_json::from_value(wire.clone()).unwrap();
            assert_eq!(serde_json::to_value(&typed).unwrap(), wire);
        }
        match serde_json::from_value(inputs_task()).unwrap() {
            Input::Task {
                request_id,
                command: Command::Read { key },
            } => {
                assert_eq!(request_id, "42");
                assert_eq!(key.model, "Todo");
            }
            other => panic!("{other:?}"),
        }
        match serde_json::from_value(json!({"type":"effectResult","effectId":"102","outcome":{"ok":false,"error":{"message":"pull failed","status":401}}})).unwrap() {
            Input::EffectResult { effect_id, outcome } => {
                assert_eq!(effect_id, "102");
                assert_eq!(outcome, EffectOutcome::failure("pull failed", Some(401)));
            }
            other => panic!("{other:?}"),
        }
        let events = [
            (
                json!({"type":"taskCompleted","requestId":"42","ok":true,"value":null}),
                Event::TaskCompleted {
                    request_id: "42".into(),
                    ok: true,
                    value: Value::Null,
                    error: None,
                    details: None,
                },
            ),
            (
                json!({"type":"taskCompleted","requestId":"43","ok":false,"value":null,"error":"transaction_closed"}),
                Event::TaskCompleted {
                    request_id: "43".into(),
                    ok: false,
                    value: Value::Null,
                    error: Some("transaction_closed".into()),
                    details: None,
                },
            ),
            (
                json!({"type":"effect","effectId":"5","operation":{"kind":"callback","transactionId":"tx7","requestId":"42"}}),
                Event::Effect {
                    effect_id: "5".into(),
                    operation: Operation::Callback {
                        transaction_id: "tx7".into(),
                        request_id: "42".into(),
                    },
                },
            ),
            (
                json!({"type":"effect","effectId":"6","operation":{"kind":"http","route":"action","body":"{}"}}),
                Event::Effect {
                    effect_id: "6".into(),
                    operation: Operation::Http {
                        route: HttpRoute::Action,
                        body: "{}".into(),
                    },
                },
            ),
            (
                json!({"type":"effect","effectId":"7","operation":{"kind":"timer","millis":250}}),
                Event::Effect {
                    effect_id: "7".into(),
                    operation: Operation::Timer { millis: 250 },
                },
            ),
            (
                json!({"type":"cancelEffect","effectId":"6"}),
                Event::CancelEffect {
                    effect_id: "6".into(),
                },
            ),
            (
                json!({"type":"report","diagnostic":{"kind":"protocol","message":"duplicate request id 42"}}),
                Event::Report {
                    diagnostic: Diagnostic::Protocol {
                        message: "duplicate request id 42".into(),
                    },
                },
            ),
            (
                json!({"type":"taskCompleted","requestId":"44","ok":false,"value":null,"error":"no page","details":{"code":"bootstrap.request_rejected","message":"no page"}}),
                Event::TaskCompleted {
                    request_id: "44".into(),
                    ok: false,
                    value: Value::Null,
                    error: Some("no page".into()),
                    details: Some(json!({"code":"bootstrap.request_rejected","message":"no page"})),
                },
            ),
            (
                json!({"type":"observerChanged","observerId":"3","snapshot":{"kind":"watch","rows":[]}}),
                Event::ObserverChanged {
                    observer_id: "3".into(),
                    snapshot: json!({"kind":"watch","rows":[]}),
                },
            ),
            (
                json!({"type":"report","diagnostic":{"kind":"error","message":"HTTP 503","status":503}}),
                Event::Report {
                    diagnostic: Diagnostic::Error {
                        message: "HTTP 503".into(),
                        status: Some(503),
                    },
                },
            ),
            (json!({"type":"runtimeClosed"}), Event::RuntimeClosed),
        ];
        for (wire, typed) in events {
            assert_eq!(
                serde_json::from_value::<Event>(wire.clone()).unwrap(),
                typed
            );
            assert_eq!(serde_json::to_value(&typed).unwrap(), wire);
        }
        assert!(serde_json::from_value::<Input>(json!({"type":"nope"})).is_err());
    }

    fn inputs_task() -> Value {
        json!({"type":"task","requestId":"42","command":{"kind":"read","key":{"model":"Todo","identity":{"id":"t"}}}})
    }

    /// A command that does not decode still routes its request: the envelope
    /// is admitted and the command carries the decoding error.
    #[test]
    fn a_malformed_command_keeps_its_request_id() {
        let cases = [
            (json!({"kind":"nope"}), "unknown variant `nope`"),
            (json!({"read":true}), "missing field `kind`"),
            (json!({"kind":"read"}), "missing field `key`"),
            (
                json!({"kind":"scopeUnsubscribe","scope":"book","subscriptionId":0}),
                "invalid counter",
            ),
            (
                json!({"kind":"connection","event":"start"}),
                "unknown variant `start`",
            ),
        ];
        for (command, error) in cases {
            let wire = json!({"type":"task","requestId":"9","command":command});
            match serde_json::from_value(wire).unwrap() {
                Input::Task {
                    request_id,
                    command: Command::Malformed { error: got },
                } => {
                    assert_eq!(request_id, "9");
                    assert!(got.contains(error), "{got} for {command}");
                }
                other => panic!("{other:?}"),
            }
        }
        let wire = json!({"type":"transactionCommand","requestId":"10","transactionId":"tx1","command":{"kind":"invoke"}});
        match serde_json::from_value(wire).unwrap() {
            Input::TransactionCommand {
                request_id,
                command: TransactionCommand::Malformed { error },
                ..
            } => {
                assert_eq!(request_id, "10");
                assert!(error.contains("unknown variant `invoke`"), "{error}");
            }
            other => panic!("{other:?}"),
        }
        // An envelope without a routable request id is still refused whole.
        assert!(
            serde_json::from_value::<Input>(json!({"type":"task","command":{"kind":"status"}}))
                .is_err()
        );
        // An explicit null option is kept apart from an absent one.
        match serde_json::from_value(json!({"type":"task","requestId":"1","command":{"kind":"invoke","name":"N","version":1,"args":{},"store":null}})).unwrap() {
            Input::Task {
                command: Command::Invoke { store, .. },
                ..
            } => assert_eq!(store, Some(Value::Null)),
            other => panic!("{other:?}"),
        }
    }
}
