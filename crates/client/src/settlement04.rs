//! Durable accepted receipts precede locally fallible ownership settlement.
use crate::{
    ApplyReport, Client, ClientStore, Mutation, Operation, OperationKind,
    authority::Held,
    engine::{Engine, as_u64},
    queue::{LocalWriteKind, OpKind},
};
use axton_core::{
    ActionOutcome, CallKind, RecordKey, Result, invalid,
    v04::{self, Validate},
};
use serde_json::{Value, json};
fn encoded<T: serde::Serialize + Validate>(value: &T) -> Result<Value> {
    Ok(json!(
        String::from_utf8(v04::encode(value)?).map_err(|_| invalid("protocol UTF8"))?
    ))
}
fn decoded<T: serde::de::DeserializeOwned + Validate>(value: Value) -> Result<T> {
    v04::decode(
        value
            .as_str()
            .ok_or_else(|| invalid("invalid protocol state"))?
            .as_bytes(),
    )
}
impl<S: ClientStore> Engine<'_, S> {
    pub(crate) fn complete_calls04(
        &mut self,
        completions: &[axton_core::CallCompletion],
    ) -> Result<()> {
        if self.context04()?.is_none() {
            return Ok(());
        }
        for completion in completions {
            self.exec("axton_v04_completion", "INSERT INTO axton_v04_completion(call_id,completion) VALUES(?,?) ON CONFLICT(call_id) DO UPDATE SET completion=excluded.completion", &[json!(completion.call_id),json!(serde_json::to_string(completion)?)])?;
            self.exec("axton_v04_op", "DELETE FROM axton_v04_op WHERE ordinal=(SELECT ordinal FROM axton_v04_call WHERE call_id=?)", &[json!(completion.call_id)])?;
            self.exec(
                "axton_v04_call",
                "UPDATE axton_v04_call SET status='completed' WHERE call_id=?",
                &[json!(completion.call_id)],
            )?;
        }
        Ok(())
    }
    pub(crate) fn capture_operation04(
        &mut self,
        ordinal: u64,
        position: u64,
        key: &RecordKey,
    ) -> Result<()> {
        if self.context04()?.is_none() {
            return Ok(());
        }
        let generation = self
            .scalar(
                "SELECT generation FROM axton_v04_record WHERE model=? AND identity=?",
                &[json!(key.model), json!(key.encoded_identity()?)],
            )?
            .unwrap_or(json!(0));
        self.exec(
            "axton_v04_op",
            "INSERT INTO axton_v04_op(ordinal,position,generation) VALUES(?,?,?)",
            &[json!(ordinal), json!(position), generation],
        )?;
        Ok(())
    }
    pub(crate) fn freeze_mutation04(&mut self, ordinal: u64, mutation: &Mutation) -> Result<()> {
        let Some(context) = self.context04()? else {
            return Ok(());
        };
        let (Some(call_id), Some(args)) = (&mutation.call_id, &mutation.args) else {
            return Ok(());
        };
        if self.schema.action(&mutation.name, mutation.version)?.kind != CallKind::Mutation {
            return Err(invalid("durable calls require a named Mutation"));
        }
        let intent = v04::MutationIntent {
            context,
            call_id: call_id.clone(),
            name: mutation.name.clone(),
            version: mutation.version,
            args: args.clone(),
            models: crate::declared_models(self.schema),
        };
        self.exec(
            "axton_v04_call",
            "INSERT INTO axton_v04_call(call_id,ordinal,intent,status) VALUES(?,?,?,'queued')",
            &[json!(call_id), json!(ordinal), encoded(&intent)?],
        )?;
        Ok(())
    }
    fn intent04(&mut self, call_id: &str) -> Result<Option<v04::MutationIntent>> {
        self.scalar(
            "SELECT intent FROM axton_v04_call WHERE call_id=?",
            &[json!(call_id)],
        )?
        .map(decoded)
        .transpose()
    }
    fn same_generation04(&mut self, ordinal: u64, position: u64, key: &RecordKey) -> Result<bool> {
        let original = self
            .scalar(
                "SELECT generation FROM axton_v04_op WHERE ordinal=? AND position=?",
                &[json!(ordinal), json!(position)],
            )?
            .ok_or_else(|| invalid("missing original operation generation"))?;
        let current = self
            .scalar(
                "SELECT generation FROM axton_v04_record WHERE model=? AND identity=?",
                &[json!(key.model), json!(key.encoded_identity()?)],
            )?
            .unwrap_or(json!(0));
        Ok(original == current)
    }
    fn settle_call04(
        &mut self,
        receipt: &v04::MutationReceipt,
        ordinal: u64,
    ) -> Result<ApplyReport> {
        let descriptor=self.rows("SELECT descriptor,projection_generation FROM axton_v04_descriptor WHERE materialization=?",&[json!(receipt.context.materialization)])?.rows.into_iter().next().ok_or_else(||invalid("missing retained materialization descriptor"))?;
        let schema = crate::Schema::from_value(serde_json::from_str(
            descriptor[0]
                .as_str()
                .ok_or_else(|| invalid("invalid retained descriptor"))?,
        )?)?;
        if v04::materialization_id(
            &schema,
            descriptor[1]
                .as_str()
                .ok_or_else(|| invalid("invalid projection generation"))?,
        )? != receipt.context.materialization
        {
            return Err(invalid("retained materialization mismatch"));
        }
        let intent = self
            .intent04(&receipt.completion.call_id)?
            .ok_or_else(|| invalid("unknown call"))?;
        if let ActionOutcome::Succeeded { result } = &receipt.completion.outcome {
            axton_core::validate_action_result(
                &schema,
                schema.action(&intent.name, intent.version)?,
                result,
            )?;
        }
        for target in &receipt.targets {
            let record = match target {
                v04::SettlementTarget::Stream { fallback, .. } => fallback,
                v04::SettlementTarget::Private { record } => record,
            };
            if schema.record_key(&record.key.model, &record.key.identity)? != record.key {
                return Err(invalid("noncanonical frozen target identity"));
            }
            if !record.state.is_null() {
                schema.validate_state(&record.key.model, &record.state)?;
            }
        }
        let ops = self.call_ops(ordinal)?;
        let mut held = Held::new();
        for op in &ops {
            let key = self.schema.record_key(&op.op.model, &op.op.identity)?;
            held.insert(key.encoded()?, key);
        }
        let mut completions = vec![receipt.completion.clone()];
        if let ActionOutcome::Failed { code, .. } = &receipt.completion.outcome {
            let (affected, dependent_completions) =
                self.mark_rejected_with_completions(&[axton_core::Rejection {
                    ordinal,
                    code: code.clone(),
                }])?;
            held.extend(affected);
            completions.extend(
                dependent_completions
                    .into_iter()
                    .filter(|completion| completion.call_id != receipt.completion.call_id),
            );
        } else {
            let mut dispositions = Vec::new();
            for target in &receipt.targets {
                let disposition = target.disposition(
                    &self
                        .context04()?
                        .ok_or_else(|| invalid("missing active context"))?
                        .materialization,
                    &self.evidence04(target.key())?,
                )?;
                if disposition == v04::SettlementDisposition::AwaitStream {
                    return Ok(ApplyReport::default());
                }
                dispositions.push(disposition);
            }
            for (target, disposition) in receipt.targets.iter().zip(dispositions) {
                if disposition != v04::SettlementDisposition::FinalizeOwnedNull {
                    continue;
                }
                let record = match target {
                    v04::SettlementTarget::Stream { fallback, .. } => fallback,
                    v04::SettlementTarget::Private { record } => record,
                };
                let position = ops
                    .iter()
                    .filter(|op| {
                        op.kind == OpKind::Wire
                            && op.op.model == record.key.model
                            && op.op.identity == record.key.identity
                    })
                    .map(|op| op.position)
                    .max()
                    .ok_or_else(|| invalid("target has no owned operation"))?;
                // A frozen private snapshot keeps the original operation's order.
                // Releasing live protection with a later direct write must not
                // revive an operation already superseded by Stream authority.
                if !self.same_generation04(ordinal, position, &record.key)? {
                    continue;
                }
                let mut values = if record.state.is_null() {
                    None
                } else {
                    let mut row = self
                        .before_get(&record.key)?
                        .unwrap_or_else(|| record.key.identity.clone());
                    for (k, v) in record
                        .state
                        .as_object()
                        .ok_or_else(|| invalid("invalid frozen snapshot"))?
                    {
                        row[k] = v.clone();
                    }
                    for field in &self.schema.model(&record.key.model)?.identity {
                        row.as_object_mut().unwrap().remove(field);
                    }
                    Some(row)
                };
                let mut operation = Operation {
                    model: record.key.model.clone(),
                    identity: record.key.identity.clone(),
                    op: if values.is_some() {
                        OperationKind::Create
                    } else {
                        OperationKind::Delete
                    },
                    values: values.take(),
                };
                crate::defaults::fill_operation(self.schema, &mut operation);
                if let Some(state) = &operation.values {
                    self.schema.validate_state(&operation.model, state)?;
                }
                self.insert_local_write(
                    ordinal,
                    Some(position),
                    LocalWriteKind::Accepted,
                    &operation,
                )?;
            }
            for op in ops.iter().filter(|op| op.kind == OpKind::Companion) {
                let key = self.schema.record_key(&op.op.model, &op.op.identity)?;
                if receipt.targets.iter().any(|target| target.key() == &key) {
                    continue;
                }
                if self.same_generation04(ordinal, op.position, &key)? {
                    self.insert_local_write(
                        ordinal,
                        Some(op.position),
                        LocalWriteKind::Accepted,
                        &op.op,
                    )?;
                    self.direct_evidence04(&key)?;
                }
            }
            self.delete_mutations(&[ordinal])?;
        }
        let reports = self.rebuild_held(&held)?;
        self.complete_calls04(&completions)?;
        Ok(ApplyReport {
            reports,
            completions,
            ..Default::default()
        })
    }
}
impl<S: ClientStore> Client<S> {
    pub fn call_completion04(
        &mut self,
        call_id: &str,
    ) -> Result<Option<axton_core::CallCompletion>> {
        self.request_context()?;
        self.view(|e| {
            e.scalar(
                "SELECT completion FROM axton_v04_completion WHERE call_id=?",
                &[json!(call_id)],
            )?
            .map(|value| {
                serde_json::from_str(
                    value
                        .as_str()
                        .ok_or_else(|| invalid("invalid saved completion"))?,
                )
                .map_err(Into::into)
            })
            .transpose()
        })
    }
    pub fn mutation_intent04(&mut self, call_id: &str) -> Result<Option<v04::MutationIntent>> {
        self.view(|e| e.intent04(call_id))
    }
    pub fn accepted_awaiting04(&mut self, call_id: &str) -> Result<bool> {
        self.view(|e| {
            Ok(e.scalar(
                "SELECT status FROM axton_v04_call WHERE call_id=?",
                &[json!(call_id)],
            )? == Some(json!("acceptedAwaiting")))
        })
    }
    pub fn save_receipt04(&mut self, receipt: &v04::MutationReceipt) -> Result<()> {
        let active = self.request_context()?.clone();
        self.write(|e| {
            let intent = e
                .intent04(&receipt.completion.call_id)?
                .ok_or_else(|| invalid("unknown durable call"))?;
            receipt.admit(&intent, &active)?;
            let ordinal = as_u64(
                &e.scalar(
                    "SELECT ordinal FROM axton_v04_call WHERE call_id=?",
                    &[json!(intent.call_id)],
                )?
                .unwrap(),
            )?;
            let keys = e
                .call_ops(ordinal)?
                .into_iter()
                .filter(|op| op.kind == OpKind::Wire)
                .map(|op| e.schema.record_key(&op.op.model, &op.op.identity))
                .collect::<Result<Vec<_>>>()?;
            let saved = e
                .scalar(
                    "SELECT receipt FROM axton_v04_call WHERE call_id=?",
                    &[json!(intent.call_id)],
                )?
                .filter(|v| !v.is_null());
            if let Some(saved) = saved {
                if saved != encoded(receipt)? {
                    return Err(invalid("receipt_mismatch"));
                }
                return Ok(());
            }
            receipt.validate_targets(&keys)?;
            e.exec(
                "axton_v04_call",
                "UPDATE axton_v04_call SET receipt=?,status='acceptedAwaiting' WHERE call_id=?",
                &[encoded(receipt)?, json!(intent.call_id)],
            )?;
            Ok(())
        })
    }
    pub fn settle_receipts04(&mut self) -> Result<ApplyReport> {
        let receipts=self.view(|e|{e.rows("SELECT ordinal,receipt FROM axton_v04_call WHERE status='acceptedAwaiting' ORDER BY ordinal",&[])?.rows.into_iter().map(|row|Ok((as_u64(&row[0])?,decoded::<v04::MutationReceipt>(row[1].clone())?))).collect::<Result<Vec<_>>>()})?;
        for (ordinal, receipt) in receipts {
            let ready = self.view(|e| {
                for target in &receipt.targets {
                    if target.disposition(
                        &e.context04()?
                            .ok_or_else(|| invalid("missing active context"))?
                            .materialization,
                        &e.evidence04(target.key())?,
                    )? == v04::SettlementDisposition::AwaitStream
                    {
                        return Ok(false);
                    }
                }
                Ok(true)
            })?;
            if ready {
                let report = self.write(|e| e.settle_call04(&receipt, ordinal))?;
                if !report.completions.is_empty() {
                    return Ok(report);
                }
            }
        }
        Ok(ApplyReport::default())
    }
}
impl<S: ClientStore> Client<S> {
    pub(crate) fn freeze_mutation_wire04(&mut self) -> Result<Option<Vec<u8>>> {
        self.write(|e| {
            let queue = e.queued()?;
            let blocked = e
                .prerequisite_keys()?
                .into_iter()
                .map(|(key, _)| key)
                .collect::<std::collections::BTreeSet<_>>();
            let unsent = queue
                .iter()
                .filter(|q| q.push.is_none())
                .map(|q| q.ordinal)
                .collect::<std::collections::BTreeSet<_>>();
            for q in queue {
                let Some(call_id) = &q.mutation.call_id else {
                    return Err(invalid(
                        "anonymous mutations are not supported in protocol4",
                    ));
                };
                let status = e.scalar(
                    "SELECT status FROM axton_v04_call WHERE call_id=?",
                    &[json!(call_id)],
                )?;
                if status != Some(json!("queued")) && status != Some(json!("sent")) {
                    continue;
                }
                if q.push.is_none()
                    && (q
                        .mutation
                        .prerequisites
                        .iter()
                        .any(|key| blocked.contains(key))
                        || q.mutation
                            .lifecycle_dependencies
                            .iter()
                            .chain(&q.mutation.sequence_dependencies)
                            .any(|id| unsent.contains(id)))
                {
                    continue;
                }
                let intent = e
                    .intent04(call_id)?
                    .ok_or_else(|| invalid("missing frozen mutation intent"))?;
                if q.push.is_none() {
                    let push = e.allocate_push()?;
                    e.assign_push(&[q.ordinal], push)?;
                    e.exec(
                        "axton_v04_call",
                        "UPDATE axton_v04_call SET status='sent' WHERE call_id=?",
                        &[json!(call_id)],
                    )?;
                }
                return Ok(Some(v04::encode(&intent)?));
            }
            Ok(None)
        })
    }
}
