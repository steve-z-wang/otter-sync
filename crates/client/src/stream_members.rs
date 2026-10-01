//! Canonical authority and retained device-local operation history.
use crate::authority::{Held, StageEntry, StageMode};
use crate::engine::Engine;
use crate::store::ClientStore;
use crate::{ApplyReport, Operation};
use axton_core::{MembershipClaim, RecordKey, Result, StreamChange, invalid};
use serde_json::{Value, json};

impl<S: ClientStore> Engine<'_, S> {
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
                    return Err(invalid("prepared legacy epoch admission changed"));
                }
                *next += 1;
            }
            StageMode::Normal => {}
        }
        Ok(())
    }
    /// Historical claims are validated but confer no local ownership.
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
        let mut report = ApplyReport::default();
        let mut pending = Held::new();
        for record in records {
            let key = self.schema.record_key(&record.model, &record.identity)?;
            if !record.state.is_null() && !self.admit_positive_body(&key, token)? {
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
        let mut report = ApplyReport::default();
        let mut pending = Held::new();
        for change in changes {
            if let StreamChange::Upsert { record, .. } = change {
                let (applied, diagnostic) = self.stage_isolated(record, &mut pending)?;
                report.applied += usize::from(applied);
                report.reports.extend(diagnostic);
            }
        }
        report.reports.extend(self.rebuild_held(&pending)?);
        Ok(report)
    }
}
