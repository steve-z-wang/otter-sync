//! Pending mutations, their operations, dependencies, prerequisites, the push in flight and rejections.
use crate::engine::{Engine, as_u64};
use crate::store::ClientStore;
use crate::{Mutation, Operation, OperationKind};
use axton_core::{
    ActionStore, ModelReadDescriptor, RecordKey, Rejection, Result, canonical_json, invalid,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OpKind {
    Wire,
    Companion,
    Effect,
}
#[derive(Clone, Debug)]
pub struct QueuedOp {
    pub ordinal: u64,
    pub position: u64,
    pub kind: OpKind,
    pub op: Operation,
}
/// Why a settled local write is still retained in the journal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LocalWriteKind {
    /// A direct write made while the record had pending work.
    Independent,
    /// The companion of a call the server accepted, kept at its position
    /// until no earlier pending work on the record needs it.
    Accepted,
}
/// A settled local write retained at its place in a record's local order:
/// an accepted companion at its owner's `(ordinal, position)`, an
/// independent write after every operation of the last ordinal allocated
/// before it. `sequence` orders writes that share a place.
#[derive(Clone, Debug)]
pub struct LocalWrite {
    pub sequence: u64,
    pub ordinal: u64,
    pub position: Option<u64>,
    pub kind: LocalWriteKind,
    pub op: Operation,
}
#[derive(Clone, Debug)]
pub struct Queued {
    pub ordinal: u64,
    pub push: Option<u64>,
    /// A replay of one of its operations failed over newer authority; the
    /// base is visible and the mutation is still sent.
    pub diverged: bool,
    pub mutation: Mutation,
}

fn text(value: &Value) -> String {
    value.as_str().unwrap_or("").to_string()
}
fn op_text(op: OperationKind) -> &'static str {
    match op {
        OperationKind::Create => "create",
        OperationKind::Update => "update",
        OperationKind::Delete => "delete",
    }
}
fn kind_text(kind: OpKind) -> &'static str {
    match kind {
        OpKind::Wire => "wire",
        OpKind::Companion => "companion",
        OpKind::Effect => "effect",
    }
}
fn decode_operation(
    model: &Value,
    identity: &Value,
    op: &Value,
    values: &Value,
) -> Result<Operation> {
    let op = match op.as_str() {
        Some("create") => OperationKind::Create,
        Some("update") => OperationKind::Update,
        Some("delete") => OperationKind::Delete,
        _ => return Err(invalid("unknown operation")),
    };
    Ok(Operation {
        model: text(model),
        op,
        identity: serde_json::from_str(identity.as_str().unwrap_or("null"))?,
        values: values.as_str().map(serde_json::from_str).transpose()?,
    })
}
fn decode_op(row: &[Value]) -> Result<QueuedOp> {
    // columns: ordinal, position, kind, model, identity, op, values
    let kind = match row[2].as_str() {
        Some("wire") => OpKind::Wire,
        Some("companion") => OpKind::Companion,
        Some("effect") => OpKind::Effect,
        _ => return Err(invalid("unknown operation kind")),
    };
    Ok(QueuedOp {
        ordinal: as_u64(&row[0])?,
        position: as_u64(&row[1])?,
        kind,
        op: decode_operation(&row[3], &row[4], &row[5], &row[6])?,
    })
}
fn decode_local_write(row: &[Value]) -> Result<LocalWrite> {
    // columns: sequence, ordinal, position, disposition, model, identity, op, values
    let kind = match row[3].as_str() {
        Some("independent") => LocalWriteKind::Independent,
        Some("accepted") => LocalWriteKind::Accepted,
        _ => return Err(invalid("unknown local write disposition")),
    };
    Ok(LocalWrite {
        sequence: as_u64(&row[0])?,
        ordinal: as_u64(&row[1])?,
        position: if row[2].is_null() {
            None
        } else {
            Some(as_u64(&row[2])?)
        },
        kind,
        op: decode_operation(&row[4], &row[5], &row[6], &row[7])?,
    })
}
fn values_text(op: &Operation) -> Result<Value> {
    Ok(match &op.values {
        Some(v) => json!(serde_json::to_string(v)?),
        None => Value::Null,
    })
}

impl<S: ClientStore> Engine<'_, S> {
    pub(crate) fn bump(&mut self, column: &str) -> Result<u64> {
        let current = self
            .scalar(&format!("SELECT {column} FROM axton_client"), &[])?
            .ok_or_else(|| invalid("client row missing"))?;
        let value = as_u64(&current)?;
        let next = value
            .checked_add(1)
            .filter(|v| *v <= axton_core::MAX_SAFE_INTEGER)
            .ok_or_else(|| invalid("counter exhausted"))?;
        self.exec(
            "axton_client",
            &format!("UPDATE axton_client SET {column}=?"),
            &[json!(next)],
        )?;
        Ok(value)
    }
    pub fn allocate_ordinal(&mut self) -> Result<u64> {
        self.bump("next_ordinal")
    }
    pub fn allocate_push(&mut self) -> Result<u64> {
        self.bump("next_push")
    }
    fn insert_op(
        &mut self,
        ordinal: u64,
        position: u64,
        kind: OpKind,
        op: &Operation,
    ) -> Result<()> {
        let key = self.schema.record_key(&op.model, &op.identity)?;
        self.exec(
            "axton_mutation_operation",
            "INSERT INTO axton_mutation_operation (ordinal, position, kind, model, identity, op, \"values\") VALUES (?,?,?,?,?,?,?)",
            &[
                json!(ordinal),
                json!(position),
                json!(kind_text(kind)),
                json!(op.model),
                json!(key.encoded_identity()?),
                json!(op_text(op.op)),
                values_text(op)?,
            ],
        )?;
        Ok(())
    }
    /// Store a call with its operations grouped by kind: wire, companion,
    /// then effect.
    pub fn insert_mutation(&mut self, ordinal: u64, mutation: &Mutation) -> Result<()> {
        let grouped: Vec<(OpKind, &Operation)> = mutation
            .operations
            .iter()
            .map(|op| (OpKind::Wire, op))
            .chain(mutation.companion.iter().map(|op| (OpKind::Companion, op)))
            .chain(mutation.effects.iter().map(|op| (OpKind::Effect, op)))
            .collect();
        self.insert_mutation_ordered(ordinal, mutation, &grouped)
    }
    /// Store a call whose operations take their positions from `ordered`,
    /// the local order they were applied in: a cascade delete sits at the
    /// delete that caused it, before the call's later operations. `ordered`
    /// holds exactly the call's wire, companion and effect operations.
    pub(crate) fn insert_mutation_ordered(
        &mut self,
        ordinal: u64,
        mutation: &Mutation,
        ordered: &[(OpKind, &Operation)],
    ) -> Result<()> {
        let args = mutation.args.as_ref().map(canonical_json).transpose()?;
        // NULL is the default (all) policy, including for rows written
        // before the column existed.
        let store = mutation
            .store
            .wire()
            .as_ref()
            .map(canonical_json)
            .transpose()?;
        self.exec(
            "axton_mutation",
            "INSERT INTO axton_mutation (ordinal, name, version, push, call_id, args, store) VALUES (?,?,?,NULL,?,?,?)",
            &[
                json!(ordinal),
                json!(mutation.name),
                json!(mutation.version),
                mutation.call_id.as_ref().map_or(Value::Null, |v| json!(v)),
                args.map_or(Value::Null, Value::String),
                store.map_or(Value::Null, Value::String),
            ],
        )?;
        for (position, (kind, op)) in (0u64..).zip(ordered) {
            self.insert_op(ordinal, position, *kind, op)?;
        }
        for (kind, deps) in [
            ("lifecycle", &mutation.lifecycle_dependencies),
            ("sequence", &mutation.sequence_dependencies),
        ] {
            for dep in deps {
                self.exec("axton_mutation_dependency", "INSERT OR IGNORE INTO axton_mutation_dependency (ordinal, depends_on, kind) VALUES (?,?,?)", &[json!(ordinal), json!(dep), json!(kind)])?;
            }
        }
        for key in &mutation.prerequisites {
            self.exec("axton_mutation_prerequisite", "INSERT OR IGNORE INTO axton_mutation_prerequisite (ordinal, key, error) VALUES (?,?,NULL)", &[json!(ordinal), json!(key)])?;
        }
        Ok(())
    }
    pub fn add_effect(&mut self, ordinal: u64, op: &Operation) -> Result<()> {
        self.append_op(ordinal, OpKind::Effect, op)
    }
    /// Store `op` at the next position of queued call `ordinal`: after every
    /// operation the call already holds. A cascade delete appended later is
    /// an effect of a wire delete, or a companion when the delete it extends
    /// is the call's companion.
    pub(crate) fn append_op(&mut self, ordinal: u64, kind: OpKind, op: &Operation) -> Result<()> {
        let next = self.scalar(
            "SELECT COALESCE(MAX(position), -1) + 1 FROM axton_mutation_operation WHERE ordinal=?",
            &[json!(ordinal)],
        )?;
        let position = as_u64(&next.unwrap_or(json!(0)))?;
        self.insert_op(ordinal, position, kind, op)
    }
    /// Whether an independent direct write was journaled after call
    /// `ordinal`, i.e. placed after every operation of that call.
    pub(crate) fn independent_writes_after(&mut self, ordinal: u64) -> Result<bool> {
        Ok(self
            .scalar(
                "SELECT 1 FROM axton_local_write WHERE disposition='independent' AND ordinal>=? LIMIT 1",
                &[json!(ordinal)],
            )?
            .is_some())
    }
    /// The operations of one queued call with their positions and kinds.
    pub(crate) fn call_ops(&mut self, ordinal: u64) -> Result<Vec<QueuedOp>> {
        Ok(self
            .ops_by_ordinal("WHERE ordinal=?", &[json!(ordinal)])?
            .into_values()
            .flatten()
            .collect())
    }
    /// The last ordinal allocated: every queued operation at or below it was
    /// written before anything written now.
    pub(crate) fn last_ordinal(&mut self) -> Result<u64> {
        let next = self
            .scalar("SELECT next_ordinal FROM axton_client", &[])?
            .ok_or_else(|| invalid("client row missing"))?;
        Ok(as_u64(&next)?.saturating_sub(1))
    }
    /// Retain a settled local write at its place in the record's local order.
    pub(crate) fn insert_local_write(
        &mut self,
        ordinal: u64,
        position: Option<u64>,
        kind: LocalWriteKind,
        op: &Operation,
    ) -> Result<()> {
        let key = self.schema.record_key(&op.model, &op.identity)?;
        let disposition = match kind {
            LocalWriteKind::Independent => "independent",
            LocalWriteKind::Accepted => "accepted",
        };
        self.exec(
            "axton_local_write",
            "INSERT INTO axton_local_write (ordinal, position, disposition, model, identity, op, \"values\") VALUES (?,?,?,?,?,?,?)",
            &[
                json!(ordinal),
                position.map_or(Value::Null, |p| json!(p)),
                json!(disposition),
                json!(op.model),
                json!(key.encoded_identity()?),
                json!(op_text(op.op)),
                values_text(op)?,
            ],
        )?;
        Ok(())
    }
    /// The settled local writes retained for one record, in allocation order.
    pub(crate) fn local_writes_for(&mut self, key: &RecordKey) -> Result<Vec<LocalWrite>> {
        let rows = self.rows(
            "SELECT sequence, ordinal, position, disposition, model, identity, op, \"values\" FROM axton_local_write WHERE model=? AND identity=? ORDER BY sequence",
            &[json!(key.model), json!(key.encoded_identity()?)],
        )?;
        rows.rows.iter().map(|r| decode_local_write(r)).collect()
    }
    pub(crate) fn delete_local_write(&mut self, sequence: u64) -> Result<()> {
        self.exec(
            "axton_local_write",
            "DELETE FROM axton_local_write WHERE sequence=?",
            &[json!(sequence)],
        )?;
        Ok(())
    }
    /// Forget every settled local write of one record: new authority replaced
    /// the base they belong to.
    pub(crate) fn delete_local_writes(&mut self, key: &RecordKey) -> Result<()> {
        self.exec(
            "axton_local_write",
            "DELETE FROM axton_local_write WHERE model=? AND identity=?",
            &[json!(key.model), json!(key.encoded_identity()?)],
        )?;
        Ok(())
    }
    fn ops_by_ordinal(
        &mut self,
        filter: &str,
        params: &[Value],
    ) -> Result<BTreeMap<u64, Vec<QueuedOp>>> {
        let rows = self.rows(&format!("SELECT ordinal, position, kind, model, identity, op, \"values\" FROM axton_mutation_operation {filter} ORDER BY ordinal, position"), params)?;
        let mut result: BTreeMap<u64, Vec<QueuedOp>> = BTreeMap::new();
        for row in &rows.rows {
            let op = decode_op(row)?;
            result.entry(op.ordinal).or_default().push(op);
        }
        Ok(result)
    }
    fn queued_where(&mut self, filter: &str, params: &[Value]) -> Result<Vec<Queued>> {
        let mutations = self.rows(
            &format!(
                "SELECT ordinal, name, version, push, diverged, call_id, args, store FROM axton_mutation {filter} ORDER BY ordinal"
            ),
            params,
        )?;
        if mutations.rows.is_empty() {
            return Ok(vec![]);
        }
        let ops = self.ops_by_ordinal(filter, params)?;
        let deps = self.rows(
            &format!(
                "SELECT ordinal, depends_on, kind FROM axton_mutation_dependency {filter} ORDER BY ordinal, depends_on"
            ),
            params,
        )?;
        let prerequisites = self.rows(
            &format!(
                "SELECT ordinal, key FROM axton_mutation_prerequisite {filter} ORDER BY ordinal, key"
            ),
            params,
        )?;
        let mut result = vec![];
        for row in &mutations.rows {
            let ordinal = as_u64(&row[0])?;
            let mut mutation = Mutation::new(text(&row[1]), vec![]);
            mutation.version = as_u64(&row[2])?;
            mutation.call_id = row[5].as_str().map(str::to_owned);
            mutation.args = row[6].as_str().map(serde_json::from_str).transpose()?;
            mutation.store = match row[7].as_str() {
                Some(text) => ActionStore::from_wire(&serde_json::from_str(text)?)?,
                None => ActionStore::All,
            };
            for op in ops.get(&ordinal).into_iter().flatten() {
                match op.kind {
                    OpKind::Wire => mutation.operations.push(op.op.clone()),
                    OpKind::Companion => mutation.companion.push(op.op.clone()),
                    OpKind::Effect => mutation.effects.push(op.op.clone()),
                }
            }
            for dep in deps
                .rows
                .iter()
                .filter(|d| as_u64(&d[0]).ok() == Some(ordinal))
            {
                let target = as_u64(&dep[1])?;
                if dep[2] == "lifecycle" {
                    mutation.lifecycle_dependencies.push(target);
                } else {
                    mutation.sequence_dependencies.push(target);
                }
            }
            for p in prerequisites
                .rows
                .iter()
                .filter(|p| as_u64(&p[0]).ok() == Some(ordinal))
            {
                mutation.prerequisites.push(text(&p[1]));
            }
            result.push(Queued {
                ordinal,
                push: row[3].as_u64(),
                diverged: row[4].as_u64().unwrap_or(0) != 0,
                mutation,
            });
        }
        Ok(result)
    }
    pub fn queued(&mut self) -> Result<Vec<Queued>> {
        self.queued_where("", &[])
    }
    pub fn queued_one(&mut self, ordinal: u64) -> Result<Option<Queued>> {
        Ok(self
            .queued_where("WHERE ordinal=?", &[json!(ordinal)])?
            .into_iter()
            .next())
    }
    pub fn ops_for(&mut self, key: &RecordKey) -> Result<Vec<QueuedOp>> {
        Ok(self
            .ops_by_ordinal(
                "WHERE model=? AND identity=?",
                &[json!(key.model), json!(key.encoded_identity()?)],
            )?
            .into_values()
            .flatten()
            .collect())
    }
    /// Mark a queued mutation as diverged; completion or rejection removes
    /// the row and with it the mark.
    pub fn set_diverged(&mut self, ordinal: u64) -> Result<()> {
        self.exec(
            "axton_mutation",
            "UPDATE axton_mutation SET diverged=1 WHERE ordinal=?",
            &[json!(ordinal)],
        )?;
        Ok(())
    }
    pub fn dirty(&mut self, key: &RecordKey) -> Result<bool> {
        Ok(self
            .scalar(
                "SELECT 1 FROM axton_mutation_operation WHERE model=? AND identity=? LIMIT 1",
                &[json!(key.model), json!(key.encoded_identity()?)],
            )?
            .is_some())
    }
    pub fn delete_mutations(&mut self, ordinals: &[u64]) -> Result<()> {
        for ordinal in ordinals {
            self.exec(
                "axton_mutation",
                "DELETE FROM axton_mutation WHERE ordinal=?",
                &[json!(ordinal)],
            )?;
        }
        for table in [
            "axton_mutation_operation",
            "axton_mutation_dependency",
            "axton_mutation_prerequisite",
        ] {
            self.changed.insert(table.into());
        }
        Ok(())
    }
    pub fn assign_push(&mut self, ordinals: &[u64], push: u64) -> Result<()> {
        for ordinal in ordinals {
            self.exec(
                "axton_mutation",
                "UPDATE axton_mutation SET push=? WHERE ordinal=?",
                &[json!(push), json!(ordinal)],
            )?;
        }
        Ok(())
    }
    /// The push that was frozen and not yet completed, if any. Completion
    /// deletes a push's rows, so any assigned push is in flight; the queue
    /// never holds more than one.
    pub fn in_flight(&mut self) -> Result<Option<u64>> {
        let rows = self.rows(
            "SELECT DISTINCT push FROM axton_mutation WHERE push IS NOT NULL ORDER BY push",
            &[],
        )?;
        let pushes: Vec<u64> = rows
            .rows
            .iter()
            .map(|r| as_u64(&r[0]))
            .collect::<Result<_>>()?;
        if pushes.len() > 1 {
            return Err(invalid("more than one push in flight"));
        }
        Ok(pushes.first().copied())
    }
    /// The sequence of the last push a receipt completed. A receipt at or
    /// below it is a duplicate and changes nothing.
    pub fn last_completed_push(&mut self) -> Result<u64> {
        let value = self
            .scalar("SELECT last_completed_push FROM axton_client", &[])?
            .ok_or_else(|| invalid("client row missing"))?;
        as_u64(&value)
    }
    /// Remember that `push` completed and forget its frozen declaration.
    pub fn set_last_completed_push(&mut self, push: u64) -> Result<()> {
        self.exec(
            "axton_client",
            "UPDATE axton_client SET last_completed_push=?, push_models=NULL, push_results=NULL",
            &[json!(push)],
        )?;
        Ok(())
    }
    /// The read contracts the push in flight declared, frozen when it was
    /// allocated so a retry sends what the original request sent.
    pub fn push_models(&mut self) -> Result<Option<Value>> {
        Ok(self
            .scalar("SELECT push_models FROM axton_client", &[])?
            .and_then(|v| v.as_str().map(serde_json::from_str::<Value>))
            .transpose()?)
    }
    pub fn set_push_models(&mut self, models: &Value) -> Result<()> {
        self.exec(
            "axton_client",
            "UPDATE axton_client SET push_models=?",
            &[json!(serde_json::to_string(models)?)],
        )?;
        Ok(())
    }
    pub fn push_result_reads(&mut self) -> Result<Option<Vec<ModelReadDescriptor>>> {
        self.scalar("SELECT push_results FROM axton_client", &[])?
            .and_then(|v| v.as_str().map(str::to_owned))
            .map(|text| serde_json::from_str(&text).map_err(Into::into))
            .transpose()
    }
    pub fn set_push_result_reads(&mut self, reads: &[ModelReadDescriptor]) -> Result<()> {
        self.exec(
            "axton_client",
            "UPDATE axton_client SET push_results=?",
            &[json!(canonical_json(&serde_json::to_value(reads)?)?)],
        )?;
        Ok(())
    }
    pub fn prerequisite_keys(&mut self) -> Result<Vec<(String, Option<String>)>> {
        let rows = self.rows(
            "SELECT key, MAX(error) FROM axton_mutation_prerequisite GROUP BY key ORDER BY key",
            &[],
        )?;
        Ok(rows
            .rows
            .into_iter()
            .map(|r| (text(&r[0]), r[1].as_str().map(str::to_owned)))
            .collect())
    }
    pub fn resolve_prerequisite(&mut self, key: &str) -> Result<usize> {
        self.exec(
            "axton_mutation_prerequisite",
            "DELETE FROM axton_mutation_prerequisite WHERE key=?",
            &[json!(key)],
        )
    }
    pub fn fail_prerequisite(&mut self, key: &str, error: &str) -> Result<usize> {
        self.exec(
            "axton_mutation_prerequisite",
            "UPDATE axton_mutation_prerequisite SET error=? WHERE key=?",
            &[json!(error), json!(key)],
        )
    }
    pub fn reset_prerequisite(&mut self, key: &str) -> Result<usize> {
        self.exec(
            "axton_mutation_prerequisite",
            "UPDATE axton_mutation_prerequisite SET error=NULL WHERE key=?",
            &[json!(key)],
        )
    }
    pub fn insert_rejection(
        &mut self,
        ordinal: u64,
        name: &str,
        code: &str,
        detail: &Value,
    ) -> Result<()> {
        self.exec(
            "axton_rejection",
            "INSERT OR REPLACE INTO axton_rejection (ordinal, name, code, detail) VALUES (?,?,?,?)",
            &[
                json!(ordinal),
                json!(name),
                json!(code),
                json!(serde_json::to_string(detail)?),
            ],
        )?;
        Ok(())
    }
    pub fn rejections(&mut self) -> Result<Vec<Rejection>> {
        let rows = self.rows(
            "SELECT ordinal, code FROM axton_rejection ORDER BY ordinal",
            &[],
        )?;
        rows.rows
            .iter()
            .map(|r| {
                Ok(Rejection {
                    ordinal: as_u64(&r[0])?,
                    code: text(&r[1]),
                })
            })
            .collect()
    }
    pub fn rejection_details(&mut self) -> Result<Vec<Value>> {
        let rows = self.rows("SELECT detail FROM axton_rejection ORDER BY ordinal", &[])?;
        rows.rows
            .iter()
            .map(|r| Ok(serde_json::from_str(r[0].as_str().unwrap_or("null"))?))
            .collect()
    }
    pub fn delete_rejection(&mut self, ordinal: u64) -> Result<()> {
        self.exec(
            "axton_rejection",
            "DELETE FROM axton_rejection WHERE ordinal=?",
            &[json!(ordinal)],
        )?;
        Ok(())
    }
}
