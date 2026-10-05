//! One applier for authoritative records, whichever path delivered them: a
//! push receipt or a scope page. Content is ordered by record stamp alone;
//! scopes and cursors never enter here
//! ([Settlement](../../../docs/engineering/architecture/client/engine/settlement.md)).
use crate::engine::Engine;
use crate::rows::merge_identity;
use crate::store::ClientStore;
use crate::{ApplyReport, StoreChange};
use crate::{Report, ReportKind};
use axton_core::{AuthorityRecord, RecordKey, Result, invalid};
use serde_json::{Value, json};
use std::collections::BTreeMap;

/// How one authoritative record compared with what the client already held.
#[derive(Debug, PartialEq)]
pub enum Disposition {
    /// A newer stamp: the content was staged and the stamp stored.
    Applied,
    /// An older stamp: nothing changed.
    Older,
    /// The same stamp with the same content: nothing to rewrite.
    Same,
    /// The same stamp with different content: reported, never applied.
    Conflict {
        local: Option<Value>,
        incoming: Option<Value>,
    },
}

/// Keys whose staged base must be replayed once the caller has finished its
/// own queue changes. A key is held while it has pending operations; its
/// authority lands in the before image and the visible row is rebuilt from
/// there, so a base staged before a completed operation is removed still
/// carries the right content afterwards.
pub type Held = BTreeMap<String, RecordKey>;

#[derive(Clone, Debug)]
pub(crate) struct StageEntry {
    pub(crate) change: Option<(String, StoreChange)>,
    pub(crate) diagnostic: Option<Report>,
}

#[derive(Default)]
pub(crate) enum StageMode {
    #[default]
    Normal,
    Capture(Vec<StageEntry>),
    Replay {
        entries: Vec<StageEntry>,
        next: usize,
    },
}

impl<S: ClientStore> Engine<'_, S> {
    /// Stage one authoritative record by stamp. A newer stamp stores its
    /// content beneath the pending operations (collected in `held`) or in the
    /// visible row of a clean record, and stores the stamp, deletions included:
    /// the stamp is what keeps older content from resurrecting the record. A
    /// deletion also stages the deletion of every declared cascade descendant,
    /// without touching the descendants' own stamp evidence.
    pub fn stage_authority(
        &mut self,
        record: &AuthorityRecord,
        held: &mut Held,
    ) -> Result<Disposition> {
        let key = self.schema.record_key(&record.model, &record.identity)?;
        let incoming = if record.state.is_null() {
            None
        } else {
            Some(merge_identity(
                &key.identity,
                &self.schema.validate_state(&record.model, &record.state)?,
            ))
        };
        let local = self.record_stamp(&key)?;
        if record.stamp < local {
            return Ok(Disposition::Older);
        }
        if record.stamp == local && !self.replica_evicted(&key)? {
            // The held base is the last authority this client applied; the
            // visible row may carry optimism on top of it.
            let current = self.truth(&key)?;
            return Ok(if current == incoming {
                Disposition::Same
            } else {
                Disposition::Conflict {
                    local: current,
                    incoming,
                }
            });
        }
        self.stage_one(&key, incoming.as_ref(), held)?;
        if incoming.is_none() {
            for child in self.descendants(&key)? {
                self.stage_one(&child, None, held)?;
            }
        }
        self.set_record_stamp(&key, record.stamp)?;
        self.set_base_state(
            &key,
            if incoming.is_some() {
                "materialized"
            } else {
                "absent"
            },
        )?;
        Ok(Disposition::Applied)
    }
    /// New authority replaces the base, and with it the settled local writes
    /// retained on that base: later server authority may replace a direct
    /// write or an accepted companion (L4). Pending operations replay on it.
    pub(crate) fn stage_one(
        &mut self,
        key: &RecordKey,
        value: Option<&Value>,
        held: &mut Held,
    ) -> Result<()> {
        self.clear_local_layer(key)?;
        if value.is_none() {
            self.set_base_state(key, "absent")?;
        }
        if self.dirty(key)? {
            self.before_set(key, value)?;
            self.delete_local_writes(key)?;
            held.insert(key.encoded()?, key.clone());
        } else {
            self.main_set(key, value)?;
        }
        Ok(())
    }
    /// Replay the remaining operations of every held key over its staged base
    /// and extend queued deletes to descendants that appeared. Called once,
    /// after the caller's queue changes, so each key is rebuilt from the final
    /// queue state. Every replay that failed is a `Diverged` report.
    pub fn rebuild_held(&mut self, held: &Held) -> Result<Vec<Report>> {
        let mut reports = vec![];
        for key in held.values() {
            if let Some(ordinal) = self.rebuild(key)? {
                let stamp = self.record_stamp(key)?;
                let mut report =
                    Report::new(ReportKind::Diverged, &key.model, &key.identity, stamp);
                report.ordinal = Some(ordinal);
                reports.push(report);
            }
        }
        self.refresh_pending()?;
        Ok(reports)
    }
    /// Stage one delivered record in its own savepoint. A record the server
    /// could not read, or one this client cannot apply (a state the schema
    /// refuses, a local constraint it violates), is reported and leaves
    /// nothing half-written; the caller carries on with the next record.
    pub(crate) fn stage_isolated(
        &mut self,
        record: &AuthorityRecord,
        held: &mut Held,
    ) -> Result<(bool, Option<Report>)> {
        if let StageMode::Replay { entries, next } = &mut self.stage_mode {
            let entry = entries
                .get(*next)
                .cloned()
                .ok_or_else(|| invalid("prepared delivery has more records than preflight"))?;
            *next += 1;
            if entry.change.is_some() {
                let disposition = self.stage_authority(record, held)?;
                if disposition != Disposition::Applied {
                    return Err(invalid("prepared authority changed before replay"));
                }
                return Ok((true, None));
            }
            return Ok((false, entry.diagnostic));
        }
        let mut entry = Report::new(
            ReportKind::ReadFailed,
            &record.model,
            &record.identity,
            record.stamp,
        );
        if let Some(code) = &record.error {
            entry.code = Some(code.clone());
            if let StageMode::Capture(entries) = &mut self.stage_mode {
                entries.push(StageEntry {
                    change: None,
                    diagnostic: Some(entry.clone()),
                });
            }
            return Ok((false, Some(entry)));
        }
        self.store.savepoint("record")?;
        let held_before = held.clone();
        let changed_before = self.changed.clone();
        let staged = self.stage_authority(record, held);
        match &staged {
            Ok(_) => self.store.release("record")?,
            Err(_) => {
                self.store.rollback_to("record")?;
                *held = held_before;
                *self.changed = changed_before;
            }
        }
        let result = match staged {
            Ok(Disposition::Applied) => (true, None),
            Ok(Disposition::Older | Disposition::Same) => (false, None),
            Ok(Disposition::Conflict { local, incoming }) => {
                entry.kind = ReportKind::Conflict;
                entry.detail = json!({ "local": local, "incoming": incoming });
                (false, Some(entry))
            }
            Err(e) => {
                entry.kind = ReportKind::Skipped;
                entry.detail = json!({ "error": e.to_string() });
                (false, Some(entry))
            }
        };
        if matches!(self.stage_mode, StageMode::Capture(_)) {
            let change = if result.0 {
                let key = self.schema.record_key(&record.model, &record.identity)?;
                let change = if record.state.is_null() {
                    StoreChange::Delete {
                        identity: key.identity,
                    }
                } else {
                    StoreChange::Upsert {
                        identity: key.identity.clone(),
                        row: merge_identity(
                            &key.identity,
                            &self.schema.validate_state(&record.model, &record.state)?,
                        ),
                    }
                };
                Some((record.model.clone(), change))
            } else {
                None
            };
            if let StageMode::Capture(entries) = &mut self.stage_mode {
                entries.push(StageEntry {
                    change,
                    diagnostic: result.1.clone(),
                });
            }
        }
        Ok(result)
    }
    /// Stage every record of one delivery, then rebuild the held keys once.
    /// A record that cannot be staged is reported and leaves nothing behind;
    /// the others are unaffected.
    pub fn apply_records(&mut self, records: &[AuthorityRecord]) -> Result<ApplyReport> {
        let mut report = ApplyReport::default();
        let mut held = Held::new();
        for record in records {
            let (applied, entry) = self.stage_isolated(record, &mut held)?;
            report.applied += usize::from(applied);
            report.reports.extend(entry);
        }
        report.reports.extend(self.rebuild_held(&held)?);
        Ok(report)
    }
}
