//! The SQL of the bootstrap ledger: the columns of `axton_subscription` that
//! carry a Scope's historical load, how one row decodes into the #150
//! subscription state and the load beside it, and the fenced statement that
//! writes the load back. The phases, the bounds and the transitions are in
//! [`bootstrap`](crate::bootstrap); this module only reads and writes rows
//! ([#151](https://github.com/zanminwang/axton/issues/151)).
use crate::bootstrap::{BootstrapPhase, BootstrapState, truncate};
use crate::engine::{Engine, as_u64};
use crate::store::{ClientStore, SqlRows};
use crate::{BOOTSTRAP_MARK, BootstrapError, SUBSCRIPTION_CLOSED, SubscriptionState};
use axton_core::{Result, invalid};
use serde_json::{Value, json};
use std::collections::BTreeSet;

/// The whole subscription row: the #150 identity and boundary, and the load
/// beside it. Both halves are read at once so one transaction decides on S, L
/// and B together.
pub(crate) struct Loaded {
    pub subscription: SubscriptionState,
    pub state: BootstrapState,
}

const COLUMNS: &str = "channel, subscription_id, starting_cursor, cursor, \
     bootstrap_state, bootstrap_run, bootstrap_cursor, bootstrap_barrier, bootstrap_error";

/// A decode error is cut to this many UTF-8 bytes before it enters a
/// [`LedgerIssue`]: it is a reason, never a copy of what the row stores.
const MAX_DETAIL: usize = 200;

/// How many channel names one settlement candidate query binds at most: below
/// SQLite's older 999-variable floor, whatever limit the host build raised it
/// to, with nothing else bound beside them.
const SETTLE_CHUNK: usize = 900;

/// One stored row a tolerant scan could not decode
/// ([#163](https://github.com/zanminwang/axton/issues/163)). The row stays
/// exactly as stored - nothing here resets, fails or completes it - and a
/// named read of it keeps failing; the issue is only the account a scan that
/// skipped it gives of why.
///
/// `fingerprint` is what tells one defect from a changed one, for whoever
/// reports it once: the channel, the raw subscription identity, the raw
/// Bootstrap fields, and a delivery position only while that value itself fails
/// to decode. A valid origin or delivery cursor is left out, so ordinary
/// delivery moving an otherwise damaged row does not make it a new defect. It
/// is a comparison key, never part of what the application is told.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LedgerIssue {
    pub channel: String,
    pub detail: String,
    pub fingerprint: String,
}
/// What a tolerant scan of the ledger came to: the rows that decoded, and one
/// [`LedgerIssue`] for each row that did not.
pub(crate) struct LedgerScan<T> {
    pub rows: Vec<T>,
    pub issues: Vec<LedgerIssue>,
}

/// Why a row whose primary key is not text cannot be read at all.
const KEY_NOT_TEXT: &str = "stored Scope name is not text";

fn optional(value: &Value) -> Result<Option<u64>> {
    if value.is_null() {
        return Ok(None);
    }
    as_u64(value).map(Some)
}
fn decode(row: &[Value]) -> Result<Loaded> {
    let scope = row[0]
        .as_str()
        .ok_or_else(|| invalid(KEY_NOT_TEXT))?
        .to_string();
    let subscription_id = as_u64(&row[1])?;
    let state = BootstrapState {
        scope: scope.clone(),
        subscription_id,
        state: BootstrapPhase::parse(
            row[4]
                .as_str()
                .ok_or_else(|| invalid("stored bootstrap state is not text"))?,
        )?,
        run: as_u64(&row[5])?,
        cursor: as_u64(&row[6])?,
        barrier: optional(&row[7])?,
        error: match row[8].as_str() {
            Some(text) => Some(serde_json::from_str(text)?),
            None => None,
        },
    };
    state.coherent()?;
    Ok(Loaded {
        subscription: SubscriptionState {
            scope,
            subscription_id,
            starting_cursor: optional(&row[2])?,
            cursor: optional(&row[3])?,
        },
        state,
    })
}
/// Decode one row selected as [`COLUMNS`], containing a decode failure to that
/// row: the outer error is one no row can be isolated from, the inner one is
/// the row's own. A row is isolated by its primary key, the channel; a key that
/// is not text names no channel to isolate, so it fails the whole read, as
/// every SQL and store error before it already has.
fn decode_keyed(row: &[Value]) -> Result<std::result::Result<Loaded, LedgerIssue>> {
    let channel = row[0].as_str().ok_or_else(|| invalid(KEY_NOT_TEXT))?;
    let error = match decode(row) {
        Ok(loaded) => return Ok(Ok(loaded)),
        Err(error) => error,
    };
    // Only a value that is itself invalid stays in: the origin and the delivery
    // cursor are the #150 half, which ordinary delivery moves.
    let position = |value: &Value| match optional(value) {
        Ok(_) => Value::Null,
        Err(_) => value.clone(),
    };
    let fingerprint = json!([
        channel,
        row[1],
        position(&row[2]),
        position(&row[3]),
        row[4],
        row[5],
        row[6],
        row[7],
        row[8],
    ])
    .to_string();
    Ok(Err(LedgerIssue {
        channel: channel.to_string(),
        detail: format!(
            "the stored Bootstrap row cannot be decoded: {}",
            truncate(error.to_string(), MAX_DETAIL)
        ),
        fingerprint,
    }))
}
/// The error every call that names a registration this client no longer holds
/// is refused with. It opens with the stable [`SUBSCRIPTION_CLOSED`] prefix,
/// which is all a host has to go by - an engine error carries a message, not a
/// code - and both SDKs raise their own `subscription.closed` for it rather
/// than this text, so a `bootstrap()` that raced an unsubscribe fails the way
/// a call through an already closed handle does.
fn closed(scope: &str, subscription_id: u64) -> axton_core::Error {
    invalid(format!(
        "{SUBSCRIPTION_CLOSED}: subscription {subscription_id} for {scope} is closed; \
         it has no bootstrap state"
    ))
}

impl<S: ClientStore> Engine<'_, S> {
    /// Record that this transaction changed `channel`'s load. The mark is
    /// stripped before the changed set reaches watchers and hosts, and it
    /// bumps no subscription generation: a load changes no membership, so it
    /// must not make the open live session stale or a pull in flight
    /// ([`BOOTSTRAP_MARK`]).
    pub(crate) fn mark_bootstrap(&mut self, channel: &str) {
        self.changed.insert(format!("{BOOTSTRAP_MARK}{channel}"));
    }
    /// The whole row for `channel`, or `None` when it is not subscribed.
    pub(crate) fn bootstrap_row(&mut self, channel: &str) -> Result<Option<Loaded>> {
        let rows = self.rows(
            &format!("SELECT {COLUMNS} FROM axton_subscription WHERE channel=?"),
            &[json!(channel)],
        )?;
        rows.rows.first().map(|r| decode(r)).transpose()
    }
    /// The row `subscription_id` names, or the closed error: a registration
    /// that is gone, or one another registration replaced, has no load state
    /// and never adopts another one's.
    pub(crate) fn bootstrap_of(&mut self, channel: &str, subscription_id: u64) -> Result<Loaded> {
        match self.bootstrap_row(channel)? {
            Some(row) if row.subscription.subscription_id == subscription_id => Ok(row),
            _ => Err(closed(channel, subscription_id)),
        }
    }
    /// Write every bootstrap field of one row, fenced by the identity and by
    /// the run it was read at. `false` means the row moved underneath and
    /// nothing was written.
    pub(crate) fn set_bootstrap(&mut self, state: &BootstrapState, from_run: u64) -> Result<bool> {
        state.coherent()?;
        // Bounded here, at the one place a failure is stored: the fields are
        // public, so a caller could hand over a message or a list this row must
        // not carry.
        let error = match &state.error {
            Some(error) => json!(serde_json::to_string(&BootstrapError::new(
                error.code.clone(),
                error.message.clone(),
                error.records.clone(),
            ))?),
            None => Value::Null,
        };
        let affected = self.exec(
            "axton_subscription",
            "UPDATE axton_subscription SET bootstrap_state=?, bootstrap_run=?, bootstrap_cursor=?, \
             bootstrap_barrier=?, bootstrap_error=? \
             WHERE channel=? AND subscription_id=? AND bootstrap_run=?",
            &[
                json!(state.state.as_str()),
                json!(state.run),
                json!(state.cursor),
                json!(state.barrier),
                error,
                json!(state.scope),
                json!(state.subscription_id),
                json!(from_run),
            ],
        )?;
        Ok(affected == 1)
    }
    /// The initialized runs that still have work, in Scope order. A run whose
    /// subscription has no origin yet has no interval to scan: it is registered
    /// work, not schedulable work.
    pub(crate) fn bootstrap_tasks(&mut self) -> Result<Vec<BootstrapState>> {
        Ok(self
            .bootstrap_task_rows()?
            .into_iter()
            .map(|row| row.state)
            .collect())
    }
    /// The same runs with the subscription half beside them, every one
    /// decoded or the read fails: what a direct caller of
    /// [`Client::bootstrap_tasks`](crate::Client::bootstrap_tasks) is owed,
    /// since a list without the damaged registration would claim it has no work.
    pub(crate) fn bootstrap_task_rows(&mut self) -> Result<Vec<Loaded>> {
        self.active_rows()?.rows.iter().map(|r| decode(r)).collect()
    }
    /// The same runs, read tolerantly: the rows that decode, and an issue for
    /// each that does not. The scheduler and the barrier scan read this, so one
    /// damaged registration cannot stop every other run's pages or its
    /// completion; S lives in the #150 columns, which bound the next page.
    pub(crate) fn bootstrap_task_scan(&mut self) -> Result<LedgerScan<Loaded>> {
        let mut scan = LedgerScan {
            rows: vec![],
            issues: vec![],
        };
        for row in &self.active_rows()?.rows {
            match decode_keyed(row)? {
                Ok(loaded) => scan.rows.push(loaded),
                Err(issue) => scan.issues.push(issue),
            }
        }
        Ok(scan)
    }
    /// The initialized rows whose run still has work, in channel order, as
    /// stored.
    fn active_rows(&mut self) -> Result<SqlRows> {
        self.rows(
            &format!(
                "SELECT {COLUMNS} FROM axton_subscription \
                 WHERE starting_cursor IS NOT NULL AND bootstrap_state IN ('requested','loading','catching_up') \
                 ORDER BY channel"
            ),
            &[],
        )
    }
    /// The named Scopes whose fixed barrier ordinary delivery has reached: the
    /// runs a settlement would actually complete, sorted and each once. Read on
    /// the committed reader so a caller can tell there is nothing to do without
    /// opening a write.
    ///
    /// The names are a set: duplicates are bound once, and the set is queried
    /// in chunks of [`SETTLE_CHUNK`] with no other value bound, so no input
    /// outgrows the variable limit of any supported SQLite, and an empty set
    /// queries nothing. Each candidate's whole row is decoded here, before its
    /// name can reach a write: one that cannot be is an issue and is left
    /// out, so it neither completes nor stops the healthy candidates beside it
    /// ([#163](https://github.com/zanminwang/axton/issues/163)).
    pub(crate) fn settleable_scan(&mut self, channels: &[String]) -> Result<LedgerScan<String>> {
        let unique: Vec<&str> = channels
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        let mut scan = LedgerScan {
            rows: vec![],
            issues: vec![],
        };
        for chunk in unique.chunks(SETTLE_CHUNK) {
            let named = vec!["?"; chunk.len()].join(",");
            let rows = self.rows(
                &format!(
                    "SELECT {COLUMNS} FROM axton_subscription \
                     WHERE starting_cursor IS NOT NULL \
                       AND bootstrap_state='catching_up' AND bootstrap_barrier IS NOT NULL \
                       AND cursor IS NOT NULL AND cursor >= bootstrap_barrier \
                       AND channel IN ({named}) ORDER BY channel"
                ),
                &chunk
                    .iter()
                    .map(|channel| json!(channel))
                    .collect::<Vec<_>>(),
            )?;
            for row in &rows.rows {
                match decode_keyed(row)? {
                    Ok(loaded) => scan.rows.push(loaded.state.scope),
                    Err(issue) => scan.issues.push(issue),
                }
            }
        }
        scan.rows.sort();
        scan.rows.dedup();
        Ok(scan)
    }
    /// Mark a run complete when the barrier it fixed has been reached, and
    /// answer with the state it committed. Reading L and writing the phase in
    /// one transaction is what makes the completion evidence exact.
    pub(crate) fn settle_barrier(&mut self, channel: &str) -> Result<Option<BootstrapState>> {
        let Some(row) = self.bootstrap_row(channel)? else {
            return Ok(None);
        };
        let (Some(barrier), BootstrapPhase::CatchingUp) = (row.state.barrier, row.state.state)
        else {
            return Ok(None);
        };
        // A subscription without a committed position has not reached anything:
        // it is never read as position zero (D9).
        if row
            .subscription
            .cursor
            .is_none_or(|delivered| delivered < barrier)
        {
            return Ok(None);
        }
        let mut state = row.state;
        state.state = BootstrapPhase::Complete;
        if self.set_bootstrap(&state, state.run)? {
            self.mark_bootstrap(channel);
            return Ok(Some(state));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A row of `bad` as [`COLUMNS`] selects it: identity 3, origin 5, delivery
    /// at `cursor`, a requested run 1 whose progress is `progress`.
    fn row(cursor: Value, progress: Value) -> Vec<Value> {
        vec![
            json!("bad"),
            json!(3),
            json!(5),
            cursor,
            json!("requested"),
            json!(1),
            progress,
            Value::Null,
            Value::Null,
        ]
    }
    fn issue(row: &[Value]) -> LedgerIssue {
        match decode_keyed(row).expect("a keyed row") {
            Ok(_) => panic!("expected an undecodable row"),
            Err(issue) => issue,
        }
    }

    #[test]
    fn a_decodable_row_is_kept() {
        let loaded = match decode_keyed(&row(json!(7), json!(2))).unwrap() {
            Ok(loaded) => loaded,
            Err(issue) => panic!("{issue:?}"),
        };
        assert_eq!(
            (loaded.state.scope.as_str(), loaded.state.cursor),
            ("bad", 2)
        );
    }

    #[test]
    fn a_row_without_a_text_key_fails_the_read() {
        let mut keyless = row(json!(7), json!("x"));
        keyless[0] = json!(7);
        assert!(decode_keyed(&keyless).is_err());
    }

    /// Valid delivery movement is not a new defect; a changed Bootstrap field, a
    /// changed identity or a changed invalid position is.
    #[test]
    fn the_fingerprint_ignores_valid_delivery_and_tracks_the_defect() {
        let first = issue(&row(json!(7), json!("x")));
        assert_eq!(first.channel, "bad");
        assert_eq!(
            first.detail,
            "the stored Bootstrap row cannot be decoded: expected an unsigned integer"
        );
        assert!(!first.detail.contains(&first.fingerprint));
        assert_eq!(first, issue(&row(json!(9), json!("x"))));
        assert_ne!(
            first.fingerprint,
            issue(&row(json!(7), json!("y"))).fingerprint
        );
        let mut replaced = row(json!(7), json!("x"));
        replaced[1] = json!(4);
        assert_ne!(first.fingerprint, issue(&replaced).fingerprint);
        let invalid = issue(&row(json!("L"), json!(2)));
        assert_ne!(
            invalid.fingerprint,
            issue(&row(json!("M"), json!(2))).fingerprint
        );
    }

    /// The reason is cut on a character boundary, so a long stored value never
    /// travels whole.
    #[test]
    fn the_detail_is_bounded() {
        let mut long = row(json!(7), json!(2));
        long[4] = json!("é".repeat(500));
        let issue = issue(&long);
        let reason = issue
            .detail
            .strip_prefix("the stored Bootstrap row cannot be decoded: ")
            .unwrap();
        assert!(reason.len() <= MAX_DETAIL, "{}", reason.len());
        assert!(reason.starts_with("unknown bootstrap state é"));
    }
}
