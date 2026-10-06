//! Durable subscriptions: the intent to follow a Stream, the identity allocated
//! to that registration, and the delivery boundary it committed. A row means
//! subscribed; NULL cursors mean the intent is durable but its first boundary
//! is not committed yet, and zero is an initialized position, never a stand-in
//! for uninitialized
//! ([#150](https://github.com/zanminwang/axton/issues/150)).
use crate::engine::{Engine, as_u64};
use crate::store::ClientStore;
use crate::{Client, SUBSCRIPTION_MARK};
use axton_core::{MAX_SAFE_INTEGER, Result, check_stream, invalid};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;

/// One stored subscription. `subscription_id` is client-local and never
/// recycled: it fences a handle, an acknowledgement or a request against the
/// registration it was made for, including a recreation at the same Stream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionState {
    pub stream: String,
    pub subscription_id: u64,
    /// The boundary the first initialization committed; `None` until then.
    pub starting_cursor: Option<u64>,
    /// How far delivery has committed, at or above `starting_cursor`.
    pub cursor: Option<u64>,
}

/// What one acknowledgement's initialization decided, in one local
/// transaction: nothing else of it is observable, and a `fault` means nothing
/// was written at all.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Initialization {
    /// The Streams whose first delivery boundary this transaction committed.
    pub initialized: Vec<String>,
    /// The already initialized Streams whose committed cursor is behind the
    /// acknowledged head: ordinary catch-up fills the gap.
    pub catch_up: Vec<String>,
    /// Why the acknowledgement was refused, with no row touched: a protocol or
    /// server-state fault the session ends with.
    pub fault: Option<String>,
}

const COLUMNS: &str = "stream, subscription_id, starting_cursor, cursor";

fn optional(value: &Value) -> Result<Option<u64>> {
    if value.is_null() {
        return Ok(None);
    }
    as_u64(value).map(Some)
}
fn decode(row: &[Value]) -> Result<SubscriptionState> {
    Ok(SubscriptionState {
        stream: row[0]
            .as_str()
            .ok_or_else(|| invalid("stored Stream name is not text"))?
            .to_string(),
        subscription_id: as_u64(&row[1])?,
        starting_cursor: optional(&row[2])?,
        cursor: optional(&row[3])?,
    })
}

impl<S: ClientStore> Engine<'_, S> {
    /// Record that this transaction changed which Streams are subscribed. The
    /// mark is stripped before the changed set reaches watchers; it bumps the
    /// subscription generation and makes pulls in flight stale.
    pub(crate) fn mark_subscription(&mut self, stream: &str) {
        self.changed.insert(format!("{SUBSCRIPTION_MARK}{stream}"));
    }
    pub fn subscription(&mut self, stream: &str) -> Result<Option<SubscriptionState>> {
        let rows = self.rows(
            &format!("SELECT {COLUMNS} FROM axton_subscription WHERE stream=?"),
            &[json!(stream)],
        )?;
        rows.rows.first().map(|r| decode(r)).transpose()
    }
    /// Every subscription, subscribed order, initialized or not.
    pub fn subscription_states(&mut self) -> Result<Vec<SubscriptionState>> {
        let rows = self.rows(
            &format!("SELECT {COLUMNS} FROM axton_subscription ORDER BY stream"),
            &[],
        )?;
        rows.rows.iter().map(|r| decode(r)).collect()
    }
    /// The initialized subscriptions with their cursors. An uninitialized one
    /// has no delivery position and is left out: it belongs to the desired set
    /// ([`Engine::subscription_states`]), not to a pull.
    pub fn subscriptions(&mut self) -> Result<Vec<(String, u64)>> {
        Ok(self
            .subscription_states()?
            .into_iter()
            .filter_map(|s| s.cursor.map(|c| (s.stream, c)))
            .collect())
    }
    /// How far `stream` committed delivery: `None` when it has no
    /// subscription, and `None` while its subscription is uninitialized.
    pub fn cursor(&mut self, stream: &str) -> Result<Option<u64>> {
        Ok(self.subscription(stream)?.and_then(|s| s.cursor))
    }
    /// The subscription for `stream`, registering it when absent, and whether
    /// this call created it. Insert-if-absent, never an upsert: a repeated
    /// registration reads the stored identity and cursors untouched. A name the
    /// wire refuses ([`check_stream`]) is refused here, so every registration
    /// path - `set_stream`, `streamSubscribe`, the generated facade - is held to
    /// the one rule.
    pub fn ensure_subscription(&mut self, stream: &str) -> Result<(SubscriptionState, bool)> {
        check_stream(stream)?;
        if let Some(state) = self.subscription(stream)? {
            return Ok((state, false));
        }
        let subscription_id = self.bump("next_subscription")?;
        let state = SubscriptionState {
            stream: stream.to_string(),
            subscription_id,
            starting_cursor: None,
            cursor: None,
        };
        self.exec(
            "axton_subscription",
            &format!("INSERT INTO axton_subscription ({COLUMNS}) VALUES (?,?,NULL,NULL)"),
            &[json!(stream), json!(subscription_id)],
        )?;
        Ok((state, true))
    }
    /// Commit the first delivery boundary of an uninitialized subscription:
    /// both cursors at `cursor`, for that identity only. `false` when the row
    /// is gone, holds another identity, or is already initialized - all of
    /// which mean this initialization no longer applies.
    pub fn initialize_subscription(
        &mut self,
        stream: &str,
        subscription_id: u64,
        cursor: u64,
    ) -> Result<bool> {
        let affected = self.exec(
            "axton_subscription",
            "UPDATE axton_subscription SET starting_cursor=?, cursor=? WHERE stream=? AND subscription_id=? AND starting_cursor IS NULL",
            &[json!(cursor), json!(cursor), json!(stream), json!(subscription_id)],
        )?;
        Ok(affected == 1)
    }
    /// Establish the first delivery boundaries one acknowledgement negotiated.
    /// `expected` is the identity map the session snapshotted when it
    /// subscribed and `heads` what the server acknowledged for exactly those
    /// Streams. Every Stream is decided before anything is written, so a fault
    /// leaves the whole acknowledgement without effect:
    ///
    /// - an acknowledgement that names another set, or a head no host can
    ///   represent, is malformed and is refused;
    /// - a Stream whose stored subscription is another one, or none, is stale
    ///   work and is skipped;
    /// - an uninitialized Stream takes `starting_cursor = cursor = head`;
    /// - a head below an initialized cursor is a server-state fault, never a
    ///   rewind;
    /// - any other initialized Stream keeps its cursor, and a head beyond it is
    ///   reported for catch-up.
    pub fn initialize_subscriptions(
        &mut self,
        expected: &BTreeMap<String, u64>,
        heads: &BTreeMap<String, u64>,
    ) -> Result<Initialization> {
        let mut outcome = Initialization::default();
        if !heads.keys().eq(expected.keys()) {
            outcome.fault =
                Some("acknowledged Streams are not the ones this session subscribed".into());
            return Ok(outcome);
        }
        let mut boundaries = Vec::new();
        for (stream, head) in heads {
            if *head > MAX_SAFE_INTEGER {
                outcome.fault = Some(format!(
                    "acknowledged head {head} for {stream} is beyond the safe integer range"
                ));
                return Ok(outcome);
            }
            let Some(state) = self.subscription(stream)? else {
                continue;
            };
            if expected.get(stream) != Some(&state.subscription_id) {
                continue;
            }
            // The stored pair moves together, so the cursor tells an
            // uninitialized subscription from an initialized one at zero.
            match state.cursor {
                None => boundaries.push((stream.clone(), state.subscription_id, *head)),
                Some(cursor) if *head < cursor => {
                    outcome.fault = Some(format!(
                        "acknowledged head {head} for {stream} is below its committed cursor {cursor}"
                    ));
                    return Ok(outcome);
                }
                Some(cursor) => {
                    if *head > cursor {
                        outcome.catch_up.push(stream.clone());
                    }
                }
            }
        }
        for (stream, subscription_id, head) in boundaries {
            if self.initialize_subscription(&stream, subscription_id, head)? {
                outcome.initialized.push(stream);
            }
        }
        Ok(outcome)
    }
    /// Move the cursor of the initialized subscription `subscription_id`
    /// names. An update, never an insert: it cannot resurrect a Stream this
    /// client unsubscribed, cannot initialize one whose first boundary is still
    /// pending, and cannot move a subscription that replaced the one the caller
    /// read. A caller reads the row before it moves it, so no row to update is
    /// a fault, not a no-op.
    pub fn advance_cursor(
        &mut self,
        stream: &str,
        subscription_id: u64,
        cursor: u64,
    ) -> Result<()> {
        let affected = self.exec(
            "axton_subscription",
            "UPDATE axton_subscription SET cursor=? WHERE stream=? AND subscription_id=? AND cursor IS NOT NULL",
            &[json!(cursor), json!(stream), json!(subscription_id)],
        )?;
        if affected != 1 {
            return Err(invalid(format!(
                "no initialized subscription {subscription_id} for {stream}; its cursor cannot advance"
            )));
        }
        Ok(())
    }
    /// Unsubscribe `stream`, and whether a row went. `subscription_id` fences
    /// the removal to one registration: an old handle cannot delete the
    /// subscription that replaced it. `None` removes whichever is stored.
    pub fn remove_subscription(
        &mut self,
        stream: &str,
        subscription_id: Option<u64>,
    ) -> Result<bool> {
        let affected = match subscription_id {
            Some(id) => self.exec(
                "axton_subscription",
                "DELETE FROM axton_subscription WHERE stream=? AND subscription_id=?",
                &[json!(stream), json!(id)],
            )?,
            None => self.exec(
                "axton_subscription",
                "DELETE FROM axton_subscription WHERE stream=?",
                &[json!(stream)],
            )?,
        };
        Ok(affected > 0)
    }
    /// Raise the allocator to `next` so a rebuilt replica cannot reissue an
    /// identity the replica it replaced handed out. Lowering it is refused by
    /// the statement itself.
    pub fn carry_subscription_allocator(&mut self, next: u64) -> Result<()> {
        let next = next.min(MAX_SAFE_INTEGER);
        self.exec(
            "axton_client",
            "UPDATE axton_client SET next_subscription=? WHERE next_subscription<?",
            &[json!(next), json!(next)],
        )?;
        Ok(())
    }
}

impl<S: ClientStore> Client<S> {
    /// Register durable intent to follow `stream` and answer with its stored
    /// state. Repeating it returns the same identity and the same cursors; a
    /// new registration starts uninitialized, with no delivery position until
    /// its first boundary is committed. A name the wire refuses
    /// ([`check_stream`]) is refused here too: a row no session could ever
    /// subscribe for would fail every handshake and stop every other Stream.
    pub fn ensure_subscription(&mut self, stream: &str) -> Result<SubscriptionState> {
        if let Some(context) = &self.context04
            && context.binding.stream != stream
        {
            return Err(invalid("binding_mismatch"));
        }
        check_stream(stream)?;
        // A registration that already exists is answered from the committed
        // reader: repeating it writes nothing, so it neither bumps the client
        // generation nor notifies a watcher. The write below re-reads the row
        // inside its transaction, so a concurrent registration still wins.
        if let Some(state) = self.subscription_state(stream)? {
            return Ok(state);
        }
        self.write(|e| {
            let (state, created) = e.ensure_subscription(stream)?;
            if created {
                e.mark_subscription(stream);
            }
            Ok(state)
        })
    }
    pub fn subscription_state(&mut self, stream: &str) -> Result<Option<SubscriptionState>> {
        self.view(|e| e.subscription(stream))
    }
    /// Every subscription with its identity and boundary: the set a live
    /// session subscribes for, and the identities its acknowledgement is
    /// fenced by.
    pub fn subscription_states(&mut self) -> Result<Vec<SubscriptionState>> {
        self.view(|e| e.subscription_states())
    }
    /// Commit the first delivery boundaries one acknowledgement negotiated, in
    /// one local transaction; see [`Engine::initialize_subscriptions`]. The
    /// answer says which Streams were initialized - status follows the commit -
    /// and which initialized ones need catch-up.
    pub fn initialize_subscriptions(
        &mut self,
        expected: &BTreeMap<String, u64>,
        heads: &BTreeMap<String, u64>,
    ) -> Result<Initialization> {
        if self.context04.is_some() {
            return Err(invalid("protocol-4 ACK cannot initialize delivery cursor"));
        }

        self.write(|e| e.initialize_subscriptions(expected, heads))
    }
    /// Unsubscribe the registration `subscription_id` names, and whether a row
    /// went. A Stream whose current subscription is another one is left alone:
    /// an old handle cannot remove the subscription that replaced it.
    pub fn remove_subscription(&mut self, stream: &str, subscription_id: u64) -> Result<bool> {
        if self.context04.is_some() {
            return Err(invalid("bound Stream cannot be removed"));
        }
        self.write(|e| {
            let removed = e.remove_subscription(stream, Some(subscription_id))?;
            if removed {
                e.mark_subscription(stream);
            }
            Ok(removed)
        })
    }
    /// How far `stream` committed delivery; see [`Engine::cursor`].
    pub fn cursor(&mut self, stream: &str) -> Result<Option<u64>> {
        self.view(|e| e.cursor(stream))
    }
    /// The initialized subscriptions with their cursors; see
    /// [`Engine::subscriptions`].
    pub fn subscriptions(&mut self) -> Result<Vec<(String, u64)>> {
        self.view(|e| e.subscriptions())
    }
    /// Every subscribed Stream, whether or not its first boundary is committed:
    /// the set a live session asks for.
    pub fn desired_streams(&mut self) -> Result<std::collections::BTreeSet<String>> {
        Ok(self
            .subscription_states()?
            .into_iter()
            .map(|s| s.stream)
            .collect())
    }
}
