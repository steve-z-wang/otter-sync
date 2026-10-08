//! Pending mutations, their operations, dependencies, prerequisites, the push in flight and rejections.
use crate::engine::{Engine, as_u64};
use crate::store::ClientStore;
use crate::{Mutation, Operation, OperationKind};
use axton_core::{RecordKey, Result, invalid};
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
// Why a settled local write is still retained in the journal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LocalWriteKind {
    // A direct write made while the record had pending work.
    Independent,
    // The companion of a call the server accepted, kept at its position
    // until no earlier pending work on the record needs it.
    Accepted,
}
// A settled local write retained at its place in a record's local order:
// an accepted companion at its owner's `(ordinal, position)`, an
// independent write after every operation of the last ordinal allocated
// before it. `sequence` orders writes that share a place.
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
    // A replay of one of its operations failed over newer authority; the
    // base is visible and the mutation is still sent.
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
    pub fn allocate_ordinal(&mut self) -> Result<u64> {
        self.allocate05("next_mutation_id")
    }

    fn insert_op(
        &mut self,
        ordinal: u64,
        position: u64,
        kind: OpKind,
        op: &Operation,
    ) -> Result<()> {
        self.insert_owned05(ordinal, position, kind, None, op)
    }
    // Store a call with its operations grouped by kind: wire, companion,
    // then effect.

    // Store a call whose operations take their positions from `ordered`,
    // the local order they were applied in: a cascade delete sits at the
    // delete that caused it, before the call's later operations. `ordered`
    // holds exactly the call's wire, companion and effect operations.

    pub fn add_effect(&mut self, ordinal: u64, op: &Operation) -> Result<()> {
        self.append_op(ordinal, OpKind::Effect, op)
    }
    // Store `op` at the next position of queued call `ordinal`: after every
    // operation the call already holds. A cascade delete appended later is
    // an effect of a wire delete, or a companion when the delete it extends
    // is the call's companion.
    pub(crate) fn append_op(&mut self, ordinal: u64, kind: OpKind, op: &Operation) -> Result<()> {
        let next = self.scalar(
            "SELECT COALESCE(MAX(step), -1) + 1 FROM axton_mutation_queue_operation WHERE mutation_id=?",
            &[json!(ordinal)],
        )?;
        let position = as_u64(&next.unwrap_or(json!(0)))?;
        self.insert_op(ordinal, position, kind, op)
    }
    // Whether an independent direct write was journaled after call
    // `ordinal`, i.e. placed after every operation of that call.
    pub(crate) fn independent_writes_after(&mut self, ordinal: u64) -> Result<bool> {
        Ok(self
            .scalar(
                "SELECT 1 FROM axton_local_write WHERE disposition='independent' AND ordinal>=? LIMIT 1",
                &[json!(ordinal)],
            )?
            .is_some())
    }
    // The operations of one queued call with their positions and kinds.
    pub(crate) fn call_ops(&mut self, ordinal: u64) -> Result<Vec<QueuedOp>> {
        Ok(self
            .ops_by_ordinal("AND q.id=?", &[json!(ordinal)])?
            .into_values()
            .flatten()
            .collect())
    }
    // The last ordinal allocated: every queued operation at or below it was
    // written before anything written now.
    pub(crate) fn last_ordinal(&mut self) -> Result<u64> {
        let next = self
            .scalar("SELECT next_mutation_id FROM axton_store", &[])?
            .ok_or_else(|| invalid("Store row missing"))?;
        Ok(as_u64(&next)?.saturating_sub(1))
    }
    // Retain a settled local write at its place in the record's local order.
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
        let mut parameters = vec![
            json!(ordinal),
            position.map_or(Value::Null, |p| json!(p)),
            json!(disposition),
            json!(op.model),
            json!(key.encoded_identity()?),
            json!(op_text(op.op)),
            values_text(op)?,
        ];
        parameters.insert(0, json!(self.allocate05("next_local_sequence")?));
        let sql = "INSERT INTO axton_local_write(sequence,ordinal,position,disposition,model,identity,op,\"values\") VALUES(?,?,?,?,?,?,?,?)";
        self.exec("axton_local_write", sql, &parameters)?;
        Ok(())
    }
    // The settled local writes retained for one record, in allocation order.
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
    // Forget every settled local write of one record: new authority replaced
    // the base they belong to.
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
        let rows = self.rows(&format!("SELECT o.mutation_id,o.step,o.kind,o.model,o.identity,o.operation,o.value FROM axton_mutation_queue_operation o JOIN axton_mutation_queue q ON q.id=o.mutation_id WHERE o.model IS NOT NULL AND q.reconciled=0 AND q.rejection_code IS NULL {filter} ORDER BY o.mutation_id,o.step"), params)?;
        let mut result: BTreeMap<u64, Vec<QueuedOp>> = BTreeMap::new();
        for row in &rows.rows {
            let op = decode_op(row)?;
            result.entry(op.ordinal).or_default().push(op);
        }
        Ok(result)
    }
    pub(crate) fn queued_where(&mut self, filter: &str, params: &[Value]) -> Result<Vec<Queued>> {
        let mutations = self.rows(
            &format!(
                "SELECT q.id,q.name,q.descriptor_version,q.batch_id,q.diverged,(SELECT id FROM axton_store)||':'||q.id FROM axton_mutation_queue q WHERE q.reconciled=0 AND q.rejection_code IS NULL {filter} ORDER BY q.id"
            ),
            params,
        )?;
        if mutations.rows.is_empty() {
            return Ok(vec![]);
        }
        let ops = self.ops_by_ordinal(filter, params)?;
        let deps = self.rows(
            &format!(
                "SELECT d.ordinal,d.depends_on,d.kind FROM axton_mutation_dependency d JOIN axton_mutation_queue q ON q.id=d.ordinal WHERE q.reconciled=0 AND q.rejection_code IS NULL {filter} ORDER BY d.ordinal,d.depends_on"
            ),
            params,
        )?;
        let prerequisites = self.rows(
            &format!(
                "SELECT p.ordinal,p.key FROM axton_mutation_prerequisite p JOIN axton_mutation_queue q ON q.id=p.ordinal WHERE q.reconciled=0 AND q.rejection_code IS NULL {filter} ORDER BY p.ordinal,p.key"
            ),
            params,
        )?;
        let mut result = vec![];
        for row in &mutations.rows {
            let ordinal = as_u64(&row[0])?;
            let mut mutation = Mutation::new(text(&row[1]), vec![]);
            mutation.version = as_u64(&row[2])?;
            mutation.call_id = row[5].as_str().map(str::to_owned);
            mutation.args = Some(self.input05(ordinal)?);
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
            .queued_where("AND q.id=?", &[json!(ordinal)])?
            .into_iter()
            .next())
    }
    pub fn ops_for(&mut self, key: &RecordKey) -> Result<Vec<QueuedOp>> {
        Ok(self
            .ops_by_ordinal(
                "AND o.model=? AND o.identity=?",
                &[json!(key.model), json!(key.encoded_identity()?)],
            )?
            .into_values()
            .flatten()
            .collect())
    }
    // Mark a queued mutation as diverged; completion or rejection removes
    // the row and with it the mark.
    pub fn set_diverged(&mut self, ordinal: u64) -> Result<()> {
        self.exec(
            "axton_mutation_queue",
            "UPDATE axton_mutation_queue SET diverged=1 WHERE id=?",
            &[json!(ordinal)],
        )?;
        Ok(())
    }
    pub fn dirty(&mut self, key: &RecordKey) -> Result<bool> {
        Ok(self
            .scalar(
                "SELECT 1 FROM axton_mutation_queue_operation o JOIN axton_mutation_queue q ON q.id=o.mutation_id WHERE o.model=? AND o.identity=? AND q.reconciled=0 AND q.rejection_code IS NULL LIMIT 1",
                &[json!(key.model), json!(key.encoded_identity()?)],
            )?
            .is_some())
    }

    // The push that was frozen and not yet completed, if any. Completion
    // deletes a push's rows, so any assigned push is in flight; the queue
    // never holds more than one.

    // The sequence of the last push a receipt completed. A receipt at or
    // below it is a duplicate and changes nothing.

    // Remember that `push` completed and forget its frozen declaration.

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

    pub fn rejection_details(&mut self) -> Result<Vec<Value>> {
        Ok(self.refused_acts()?.into_iter().map(|act| {
                let records:Vec<_>=act.act.operations.iter().map(|op|json!({"model":op.model,"identity":op.identity})).collect();
                json!({"ordinal":act.id,"code":act.code,"mutation":{"name":act.name,"version":act.version,"args":act.act.args,"operations":act.act.operations},"records":records})
            }).collect())
    }
}
