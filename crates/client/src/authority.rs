//! Rebuild held projections after canonical base or queue ownership changes.
use crate::mutate::apply_settled;
use crate::{ClientStore, Operation, OperationKind, Report, ReportKind, engine::Engine};
use axton_core::{RecordKey, Result, invalid};
use serde_json::{Value, json};
use std::collections::BTreeMap;
pub type Held = BTreeMap<String, RecordKey>;
impl<S: ClientStore> Engine<'_, S> {
    pub(crate) fn clear_local_layer(&mut self, key: &RecordKey) -> Result<()> {
        self.exec(
            "axton_local_replica_layer",
            "DELETE FROM axton_local_replica_layer WHERE model=? AND identity=?",
            &[json!(key.model), json!(key.encoded_identity()?)],
        )?;
        Ok(())
    }
    pub(crate) fn local_layer(&mut self, key: &RecordKey) -> Result<Vec<Operation>> {
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
    pub(crate) fn stage_preserving_local(
        &mut self,
        key: &axton_core::RecordKey,
        incoming: Option<&Value>,
        held: &mut Held,
    ) -> Result<()> {
        let mut adapted = incoming.cloned();
        for operation in self.local_layer(key)? {
            if operation.op == OperationKind::Create && adapted.is_some() {
                // Old creates own original fields, not fields introduced by rematerialization.
                if let (Some(row), Some(fields)) = (
                    adapted.as_mut(),
                    operation.values.as_ref().and_then(Value::as_object),
                ) {
                    for (field, value) in fields {
                        row[field] = value.clone();
                    }
                }
            } else {
                apply_settled(&mut adapted, &operation);
            }
        }
        if self.dirty(key)? {
            self.before_set(key, adapted.as_ref())?;
            held.insert(key.encoded()?, key.clone());
        } else {
            self.main_set(key, adapted.as_ref())?;
        }
        Ok(())
    }
    pub fn rebuild_held(&mut self, held: &Held) -> Result<Vec<Report>> {
        let mut reports = vec![];
        for key in held.values() {
            if let Some(ordinal) = self.rebuild(key)? {
                let mut report = Report::new(ReportKind::Diverged, &key.model, &key.identity);
                report.ordinal = Some(ordinal);
                reports.push(report);
            }
        }
        self.refresh_pending()?;
        Ok(reports)
    }
    pub(crate) fn stage_one(
        &mut self,
        key: &RecordKey,
        value: Option<&Value>,
        held: &mut Held,
    ) -> Result<()> {
        self.clear_local_layer(key)?;
        if self.dirty(key)? {
            self.before_set(key, value)?;
            self.delete_local_writes(key)?;
            held.insert(key.encoded()?, key.clone());
        } else {
            self.main_set(key, value)?;
        }
        Ok(())
    }
}
