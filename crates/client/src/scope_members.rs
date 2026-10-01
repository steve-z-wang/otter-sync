//! Durable per-stream ownership evidence. A removal releases a replica base;
//! it is never authoritative absence or a cascading domain delete.
use crate::authority::{Held, StageEntry, StageMode};
use crate::engine::{Engine, as_u64};
use crate::store::ClientStore;
use crate::{ApplyReport, Operation};
use axton_core::{MAX_SAFE_INTEGER, MembershipClaim, RecordKey, Result, StreamChange, invalid};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug)]
pub struct MemberEvidence {
    pub stream: String,
    pub key: RecordKey,
    pub cursor: u64,
    pub present: bool,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MembershipMerge {
    Newer,
    Identical,
    Older,
}

impl<S: ClientStore> Engine<'_, S> {
    pub fn merge_member(&mut self, evidence: MemberEvidence) -> Result<MembershipMerge> {
        if evidence.stream.is_empty() || evidence.cursor == 0 || evidence.cursor > MAX_SAFE_INTEGER
        {
            return Err(invalid("invalid membership evidence"));
        }
        let key = self
            .schema
            .record_key(&evidence.key.model, &evidence.key.identity)?;
        let parameters = [
            json!(evidence.stream),
            json!(key.model),
            json!(key.encoded_identity()?),
        ];
        let rows = self.rows("SELECT cursor, present FROM axton_scope_member WHERE scope=? AND model=? AND identity=?", &parameters)?;
        if let Some(row) = rows.rows.first() {
            let cursor = as_u64(&row[0])?;
            if evidence.cursor < cursor {
                return Ok(MembershipMerge::Older);
            }
            if evidence.cursor == cursor {
                if evidence.present != (as_u64(&row[1])? == 1) {
                    return Err(invalid("equal membership cursor has conflicting presence"));
                }
                return Ok(MembershipMerge::Identical);
            }
        }
        self.exec("axton_scope_member", "INSERT INTO axton_scope_member(scope, model, identity, cursor, present) VALUES(?,?,?,?,?) ON CONFLICT(scope,model,identity) DO UPDATE SET cursor=excluded.cursor,present=excluded.present", &[parameters[0].clone(),parameters[1].clone(),parameters[2].clone(),json!(evidence.cursor),json!(u8::from(evidence.present))])?;
        Ok(MembershipMerge::Newer)
    }
    pub fn merge_memberships(&mut self, claims: &[MembershipClaim]) -> Result<()> {
        for claim in claims {
            claim.validate()?;
            self.merge_member(MemberEvidence {
                stream: claim.stream.clone(),
                key: claim.key(),
                cursor: claim.cursor,
                present: true,
            })?;
        }
        Ok(())
    }
    pub fn held(&mut self, key: &RecordKey) -> Result<bool> {
        Ok(self.scalar("SELECT 1 FROM axton_scope_member WHERE model=? AND identity=? AND present=1 LIMIT 1", &[json!(key.model),json!(key.encoded_identity()?)])?.is_some())
    }
    pub fn replica_evicted(&mut self, key: &RecordKey) -> Result<bool> {
        Ok(self
            .scalar(
                "SELECT base_state FROM axton_record WHERE model=? AND identity=?",
                &[json!(key.model), json!(key.encoded_identity()?)],
            )?
            .as_ref()
            .and_then(Value::as_str)
            == Some("evicted"))
    }
    pub(crate) fn set_base_state(&mut self, key: &RecordKey, state: &str) -> Result<()> {
        self.exec(
            "axton_record",
            "UPDATE axton_record SET base_state=? WHERE model=? AND identity=?",
            &[
                json!(state),
                json!(key.model),
                json!(key.encoded_identity()?),
            ],
        )?;
        Ok(())
    }
    pub(crate) fn clear_local_layer(&mut self, key: &RecordKey) -> Result<()> {
        self.exec(
            "axton_local_replica_layer",
            "DELETE FROM axton_local_replica_layer WHERE model=? AND identity=?",
            &[json!(key.model), json!(key.encoded_identity()?)],
        )?;
        Ok(())
    }
    fn local_layer(&mut self, key: &RecordKey) -> Result<Vec<Operation>> {
        self.scalar(
            "SELECT operations FROM axton_local_replica_layer WHERE model=? AND identity=?",
            &[json!(key.model), json!(key.encoded_identity()?)],
        )?
        .map(|value| {
            serde_json::from_str(
                value
                    .as_str()
                    .ok_or_else(|| invalid("local layer is not JSON"))?,
            )
            .map_err(Into::into)
        })
        .transpose()
        .map(|value| value.unwrap_or_default())
    }
    pub(crate) fn retain_local_operation(
        &mut self,
        key: &RecordKey,
        operation: &Operation,
    ) -> Result<()> {
        let mut operations = self.local_layer(key)?;
        // A create/delete cuts off earlier local operations; adjacent updates
        // coalesce their patches without changing lifecycle ordering.
        if matches!(
            operation.op,
            crate::OperationKind::Create | crate::OperationKind::Delete
        ) {
            operations.clear();
        }
        if operation.op == crate::OperationKind::Update
            && operations
                .last()
                .is_some_and(|last| last.op == crate::OperationKind::Update)
        {
            let patch = operation
                .values
                .as_ref()
                .and_then(Value::as_object)
                .ok_or_else(|| invalid("local patch missing"))?;
            let last = operations
                .last_mut()
                .unwrap()
                .values
                .as_mut()
                .and_then(Value::as_object_mut)
                .ok_or_else(|| invalid("local patch missing"))?;
            last.extend(patch.clone());
        } else {
            operations.push(operation.clone());
        }
        self.exec("axton_local_replica_layer", "INSERT INTO axton_local_replica_layer(model,identity,operations) VALUES(?,?,?) ON CONFLICT(model,identity) DO UPDATE SET operations=excluded.operations", &[json!(key.model),json!(key.encoded_identity()?),json!(serde_json::to_string(&operations)?)])?;
        Ok(())
    }
    /// Release only the server base, retaining its stamp and all local work.
    pub fn release_replica(&mut self, key: &RecordKey) -> Result<()> {
        let key = self.schema.record_key(&key.model, &key.identity)?;
        if self.held(&key)? {
            return Ok(());
        }
        let stamp = self.record_stamp(&key)?;
        let prior = self.scalar(
            "SELECT base_state FROM axton_record WHERE model=? AND identity=?",
            &[json!(key.model), json!(key.encoded_identity()?)],
        )?;
        let base = if self.dirty(&key)? {
            self.before_get(&key)?
        } else {
            self.read_row(&key)?
        };
        let absent = prior == Some(json!("absent"))
            || (prior == Some(json!("legacy")) && stamp > 0 && base.is_none());
        let mut operations = self.local_layer(&key)?;
        // Before the operation layer existed, an unstamped base could only
        // have been created locally. Preserve that complete local creation;
        // a stamped cache is never promoted this way.
        if stamp == 0
            && operations.is_empty()
            && let Some(row) = base
        {
            let operation = Operation {
                model: key.model.clone(),
                identity: key.identity.clone(),
                op: crate::OperationKind::Create,
                values: Some(row),
            };
            self.retain_local_operation(&key, &operation)?;
            operations.push(operation);
        }
        let mut local = None;
        for operation in operations {
            crate::mutate::apply_settled(&mut local, &operation);
        }
        if self.dirty(&key)? {
            self.before_set(&key, local.as_ref())?;
            self.rebuild(&key)?;
            self.refresh_pending()?;
        } else {
            self.main_set(&key, local.as_ref())?;
        }
        self.set_record_stamp(&key, stamp)?;
        // Authoritative absence is stronger than cache eviction, including at
        // an equal stamp. Never turn it into restorable positive authority.
        self.set_base_state(&key, if absent { "absent" } else { "evicted" })?;
        Ok(())
    }
    pub(crate) fn skip_authority_occurrence(&mut self) -> Result<()> {
        match &mut self.stage_mode {
            StageMode::Capture(entries) => entries.push(StageEntry {
                change: None,
                diagnostic: None,
            }),
            StageMode::Replay { entries, next } => {
                if entries
                    .get(*next)
                    .is_none_or(|entry| entry.change.is_some())
                {
                    return Err(invalid("prepared membership admission changed"));
                }
                *next += 1;
            }
            StageMode::Normal => {}
        }
        Ok(())
    }
    /// Enrollment evidence is merged before any of the returned bodies.
    pub fn apply_enrolled_records(
        &mut self,
        records: &[axton_core::AuthorityRecord],
        claims: &[MembershipClaim],
    ) -> Result<ApplyReport> {
        self.apply_enrolled_records_at(records, claims, crate::StoreToken::default())
    }
    pub fn apply_enrolled_records_at(
        &mut self,
        records: &[axton_core::AuthorityRecord],
        claims: &[MembershipClaim],
        token: crate::StoreToken,
    ) -> Result<ApplyReport> {
        axton_core::validate_memberships(claims, records)?;
        let enrolled: BTreeSet<_> = claims
            .iter()
            .map(|claim| {
                self.schema
                    .record_key(&claim.model, &claim.identity)?
                    .encoded()
            })
            .collect::<Result<_>>()?;
        self.merge_memberships(claims)?;
        let mut report = ApplyReport::default();
        let mut pending = Held::new();
        for record in records {
            let key = self.schema.record_key(&record.model, &record.identity)?;
            if !record.state.is_null()
                && ((enrolled.contains(&key.encoded()?) && !self.held(&key)?)
                    || !self.admit_positive_body(&key, token)?)
            {
                self.skip_authority_occurrence()?;
                continue;
            }
            let (applied, diagnostic) = self.stage_isolated(record, &mut pending)?;
            report.applied += usize::from(applied);
            report.reports.extend(diagnostic);
        }
        report.reports.extend(self.rebuild_held(&pending)?);
        Ok(report)
    }
    pub(crate) fn apply_stream_changes(&mut self, changes: &[StreamChange]) -> Result<ApplyReport> {
        let mut releases = BTreeMap::new();
        // Merge the complete delivery before staging any body or release.
        for change in changes {
            let (stream, cursor, key, present) = match change {
                StreamChange::Upsert {
                    stream,
                    cursor,
                    record,
                } => (
                    stream,
                    *cursor,
                    self.schema.record_key(&record.model, &record.identity)?,
                    true,
                ),
                StreamChange::Remove {
                    stream,
                    cursor,
                    key,
                } => (
                    stream,
                    *cursor,
                    self.schema.record_key(&key.model, &key.identity)?,
                    false,
                ),
            };
            let merged = self.merge_member(MemberEvidence {
                stream: stream.clone(),
                cursor,
                key: key.clone(),
                present,
            })?;
            if !present && merged == MembershipMerge::Newer {
                releases.insert(key.encoded()?, key);
            }
        }
        let mut report = ApplyReport::default();
        let mut held = Held::new();
        for change in changes {
            if let StreamChange::Upsert { record, .. } = change {
                let key = self.schema.record_key(&record.model, &record.identity)?;
                if !record.state.is_null() && !self.held(&key)? {
                    self.skip_authority_occurrence()?;
                    continue;
                }
                let (applied, diagnostic) = self.stage_isolated(record, &mut held)?;
                report.applied += usize::from(applied);
                report.reports.extend(diagnostic);
            }
        }
        report.reports.extend(self.rebuild_held(&held)?);
        for key in releases.values() {
            if !self.held(key)? {
                self.evict_at_next_epoch(key)?;
            }
        }
        Ok(report)
    }
}
