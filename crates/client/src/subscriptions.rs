//! Durable subscriptions: the intent to follow a Scope, the identity allocated
//! to that registration, and the delivery boundary it committed. A row means
//! subscribed; NULL cursors mean the intent is durable but its first boundary
//! is not committed yet, and zero is an initialized position, never a stand-in
//! for uninitialized
//! ([#150](https://github.com/zanminwang/axton/issues/150)).
use crate::engine::{Engine, as_u64};
use crate::store::ClientStore;
use crate::{Client, SUBSCRIPTION_MARK};
use axton_core::{MAX_SAFE_INTEGER, Result, check_channel, invalid};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;

/// One stored subscription. `subscription_id` is client-local and never
/// recycled: it fences a handle, an acknowledgement or a request against the
/// registration it was made for, including a recreation at the same Scope.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionState {
    pub scope: String,
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
    /// The Scopes whose first delivery boundary this transaction committed.
    pub initialized: Vec<String>,
    /// The already initialized Scopes whose committed cursor is behind the
    /// acknowledged head: ordinary catch-up fills the gap.
    pub catch_up: Vec<String>,
    /// Why the acknowledgement was refused, with no row touched: a protocol or
    /// server-state fault the session ends with.
    pub fault: Option<String>,
}

const COLUMNS: &str = "channel, subscription_id, starting_cursor, cursor";

fn optional(value: &Value) -> Result<Option<u64>> {
    if value.is_null() {
        return Ok(None);
    }
    as_u64(value).map(Some)
}
fn decode(row: &[Value]) -> Result<SubscriptionState> {
    Ok(SubscriptionState {
        scope: row[0]
            .as_str()
            .ok_or_else(|| invalid("stored Scope name is not text"))?
            .to_string(),
        subscription_id: as_u64(&row[1])?,
        starting_cursor: optional(&row[2])?,
        cursor: optional(&row[3])?,
    })
}

impl<S: ClientStore> Engine<'_, S> {
    /// Record that this transaction changed which Scopes are subscribed. The
    /// mark is stripped before the changed set reaches watchers; it bumps the
    /// subscription generation and makes pulls in flight stale.
    pub(crate) fn mark_subscription(&mut self, channel: &str) {
        self.changed.insert(format!("{SUBSCRIPTION_MARK}{channel}"));
    }
    pub fn subscription(&mut self, channel: &str) -> Result<Option<SubscriptionState>> {
        let rows = self.rows(
            &format!("SELECT {COLUMNS} FROM axton_subscription WHERE channel=?"),
            &[json!(channel)],
        )?;
        rows.rows.first().map(|r| decode(r)).transpose()
    }
    /// Every subscription, subscribed order, initialized or not.
    pub fn subscription_states(&mut self) -> Result<Vec<SubscriptionState>> {
        let rows = self.rows(
            &format!("SELECT {COLUMNS} FROM axton_subscription ORDER BY channel"),
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
            .filter_map(|s| s.cursor.map(|c| (s.scope, c)))
            .collect())
    }
    /// How far `channel` committed delivery: `None` when it has no
    /// subscription, and `None` while its subscription is uninitialized.
    pub fn cursor(&mut self, channel: &str) -> Result<Option<u64>> {
        Ok(self.subscription(channel)?.and_then(|s| s.cursor))
    }
    /// The subscription for `channel`, registering it when absent, and whether
    /// this call created it. Insert-if-absent, never an upsert: a repeated
    /// registration reads the stored identity and cursors untouched. A name the
    /// wire refuses ([`check_channel`]) is refused here, so every registration
    /// path - `set_channel`, `scopeSubscribe`, the generated facade - is held to
    /// the one rule.
    pub fn ensure_subscription(&mut self, channel: &str) -> Result<(SubscriptionState, bool)> {
        check_channel(channel)?;
        if let Some(state) = self.subscription(channel)? {
            return Ok((state, false));
        }
        let subscription_id = self.bump("next_subscription")?;
        let state = SubscriptionState {
            scope: channel.to_string(),
            subscription_id,
            starting_cursor: None,
            cursor: None,
        };
        self.exec(
            "axton_subscription",
            &format!("INSERT INTO axton_subscription ({COLUMNS}) VALUES (?,?,NULL,NULL)"),
            &[json!(channel), json!(subscription_id)],
        )?;
        if self
            .scalar(
                "SELECT 1 FROM axton_channel_member WHERE channel=? LIMIT 1",
                &[json!(channel)],
            )?
            .is_some()
        {
            self.exec("axton_subscription", "UPDATE axton_subscription SET reconcile_state='requested', reconcile_run=1 WHERE channel=? AND subscription_id=?", &[json!(channel), json!(subscription_id)])?;
        }
        Ok((state, true))
    }
    /// Commit the first delivery boundary of an uninitialized subscription:
    /// both cursors at `cursor`, for that identity only. `false` when the row
    /// is gone, holds another identity, or is already initialized - all of
    /// which mean this initialization no longer applies.
    pub fn initialize_subscription(
        &mut self,
        channel: &str,
        subscription_id: u64,
        cursor: u64,
    ) -> Result<bool> {
        let affected = self.exec(
            "axton_subscription",
            "UPDATE axton_subscription SET starting_cursor=?, cursor=? WHERE channel=? AND subscription_id=? AND starting_cursor IS NULL",
            &[json!(cursor), json!(cursor), json!(channel), json!(subscription_id)],
        )?;
        Ok(affected == 1)
    }
    /// Establish the first delivery boundaries one acknowledgement negotiated.
    /// `expected` is the identity map the session snapshotted when it
    /// subscribed and `heads` what the server acknowledged for exactly those
    /// Scopes. Every Scope is decided before anything is written, so a fault
    /// leaves the whole acknowledgement without effect:
    ///
    /// - an acknowledgement that names another set, or a head no host can
    ///   represent, is malformed and is refused;
    /// - a Scope whose stored subscription is another one, or none, is stale
    ///   work and is skipped;
    /// - an uninitialized Scope takes `starting_cursor = cursor = head`;
    /// - a head below an initialized cursor is a server-state fault, never a
    ///   rewind;
    /// - any other initialized Scope keeps its cursor, and a head beyond it is
    ///   reported for catch-up.
    pub fn initialize_subscriptions(
        &mut self,
        expected: &BTreeMap<String, u64>,
        heads: &BTreeMap<String, u64>,
    ) -> Result<Initialization> {
        let mut outcome = Initialization::default();
        if !heads.keys().eq(expected.keys()) {
            outcome.fault =
                Some("acknowledged Scopes are not the ones this session subscribed".into());
            return Ok(outcome);
        }
        let mut boundaries = Vec::new();
        for (scope, head) in heads {
            if *head > MAX_SAFE_INTEGER {
                outcome.fault = Some(format!(
                    "acknowledged head {head} for {scope} is beyond the safe integer range"
                ));
                return Ok(outcome);
            }
            let Some(state) = self.subscription(scope)? else {
                continue;
            };
            if expected.get(scope) != Some(&state.subscription_id) {
                continue;
            }
            // The stored pair moves together, so the cursor tells an
            // uninitialized subscription from an initialized one at zero.
            match state.cursor {
                None => boundaries.push((scope.clone(), state.subscription_id, *head)),
                Some(cursor) if *head < cursor => {
                    outcome.fault = Some(format!(
                        "acknowledged head {head} for {scope} is below its committed cursor {cursor}"
                    ));
                    return Ok(outcome);
                }
                Some(cursor) => {
                    if *head > cursor {
                        outcome.catch_up.push(scope.clone());
                    }
                }
            }
        }
        for (scope, head) in heads {
            if let Some(id) = expected.get(scope) {
                self.exec("axton_subscription", "UPDATE axton_subscription SET reconcile_bound=? WHERE channel=? AND subscription_id=? AND reconcile_bound IS NULL AND reconcile_state='requested'", &[json!(head),json!(scope),json!(id)])?;
            }
        }
        for (scope, subscription_id, head) in boundaries {
            if self.initialize_subscription(&scope, subscription_id, head)? {
                outcome.initialized.push(scope);
            }
        }
        Ok(outcome)
    }
    /// Move the cursor of the initialized subscription `subscription_id`
    /// names. An update, never an insert: it cannot resurrect a Scope this
    /// client unsubscribed, cannot initialize one whose first boundary is still
    /// pending, and cannot move a subscription that replaced the one the caller
    /// read. A caller reads the row before it moves it, so no row to update is
    /// a fault, not a no-op.
    pub fn advance_cursor(
        &mut self,
        channel: &str,
        subscription_id: u64,
        cursor: u64,
    ) -> Result<()> {
        let affected = self.exec(
            "axton_subscription",
            "UPDATE axton_subscription SET cursor=? WHERE channel=? AND subscription_id=? AND cursor IS NOT NULL",
            &[json!(cursor), json!(channel), json!(subscription_id)],
        )?;
        if affected != 1 {
            return Err(invalid(format!(
                "no initialized subscription {subscription_id} for {channel}; its cursor cannot advance"
            )));
        }
        Ok(())
    }
    /// Unsubscribe `channel`, and whether a row went. `subscription_id` fences
    /// the removal to one registration: an old handle cannot delete the
    /// subscription that replaced it. `None` removes whichever is stored.
    pub fn remove_subscription(
        &mut self,
        channel: &str,
        subscription_id: Option<u64>,
    ) -> Result<bool> {
        let affected = match subscription_id {
            Some(id) => self.exec(
                "axton_subscription",
                "DELETE FROM axton_subscription WHERE channel=? AND subscription_id=?",
                &[json!(channel), json!(id)],
            )?,
            None => self.exec(
                "axton_subscription",
                "DELETE FROM axton_subscription WHERE channel=?",
                &[json!(channel)],
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
    /// Register durable intent to follow `scope` and answer with its stored
    /// state. Repeating it returns the same identity and the same cursors; a
    /// new registration starts uninitialized, with no delivery position until
    /// its first boundary is committed. A name the wire refuses
    /// ([`check_channel`]) is refused here too: a row no session could ever
    /// subscribe for would fail every handshake and stop every other Scope.
    pub fn ensure_subscription(&mut self, scope: &str) -> Result<SubscriptionState> {
        check_channel(scope)?;
        // A registration that already exists is answered from the committed
        // reader: repeating it writes nothing, so it neither bumps the client
        // generation nor notifies a watcher. The write below re-reads the row
        // inside its transaction, so a concurrent registration still wins.
        if let Some(state) = self.subscription_state(scope)? {
            return Ok(state);
        }
        self.write(|e| {
            let (state, created) = e.ensure_subscription(scope)?;
            if created {
                e.mark_subscription(scope);
            }
            Ok(state)
        })
    }
    pub fn subscription_state(&mut self, scope: &str) -> Result<Option<SubscriptionState>> {
        self.view(|e| e.subscription(scope))
    }
    /// Every subscription with its identity and boundary: the set a live
    /// session subscribes for, and the identities its acknowledgement is
    /// fenced by.
    pub fn subscription_states(&mut self) -> Result<Vec<SubscriptionState>> {
        self.view(|e| e.subscription_states())
    }
    /// Commit the first delivery boundaries one acknowledgement negotiated, in
    /// one local transaction; see [`Engine::initialize_subscriptions`]. The
    /// answer says which Scopes were initialized - status follows the commit -
    /// and which initialized ones need catch-up.
    pub fn initialize_subscriptions(
        &mut self,
        expected: &BTreeMap<String, u64>,
        heads: &BTreeMap<String, u64>,
    ) -> Result<Initialization> {
        self.write(|e| e.initialize_subscriptions(expected, heads))
    }
    /// Unsubscribe the registration `subscription_id` names, and whether a row
    /// went. A Scope whose current subscription is another one is left alone:
    /// an old handle cannot remove the subscription that replaced it.
    pub fn remove_subscription(&mut self, scope: &str, subscription_id: u64) -> Result<bool> {
        self.write(|e| {
            let removed = e.remove_subscription(scope, Some(subscription_id))?;
            if removed {
                e.mark_subscription(scope);
            }
            Ok(removed)
        })
    }
    /// How far `channel` committed delivery; see [`Engine::cursor`].
    pub fn cursor(&mut self, channel: &str) -> Result<Option<u64>> {
        self.view(|e| e.cursor(channel))
    }
    /// The initialized subscriptions with their cursors; see
    /// [`Engine::subscriptions`].
    pub fn subscriptions(&mut self) -> Result<Vec<(String, u64)>> {
        self.view(|e| e.subscriptions())
    }
    /// Every subscribed Scope, whether or not its first boundary is committed:
    /// the set a live session asks for.
    pub fn desired_channels(&mut self) -> Result<std::collections::BTreeSet<String>> {
        Ok(self
            .subscription_states()?
            .into_iter()
            .map(|s| s.scope)
            .collect())
    }
}
