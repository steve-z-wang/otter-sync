//! Freeze pushes from queued rows and complete them from their receipts.
use crate::authority::Held;
use crate::engine::Engine;
use crate::queue::{LocalWriteKind, Queued};
use crate::store::ClientStore;
use crate::{ApplyReport, Mutation, Operation, OperationKind};
use axton_core::{
    ActionOutcome, CallCompletion, ExecutionState, PushReceipt, PushRequest, RecordKey, Rejection,
    Result, canonical_json, invalid, limits,
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

fn keys_of<'a>(
    schema: &axton_core::Schema,
    ops: impl Iterator<Item = &'a Operation>,
) -> Result<BTreeSet<String>> {
    ops.map(|op| schema.record_key(&op.model, &op.identity)?.encoded())
        .collect()
}
fn all_ops(m: &Mutation) -> impl Iterator<Item = &Operation> {
    m.operations.iter().chain(&m.companion).chain(&m.effects)
}

struct AdmittedReceipt {
    mutations: Vec<Queued>,
    receipt: PushReceipt,
    rejected: BTreeSet<u64>,
    wire_rows: BTreeSet<String>,
    covered: BTreeSet<String>,
}

impl<S: ClientStore> Engine<'_, S> {
    fn client_id(&mut self) -> Result<String> {
        Ok(self
            .scalar("SELECT client_id FROM axton_client", &[])?
            .and_then(|v| v.as_str().map(str::to_owned))
            .unwrap_or_default())
    }
    fn request_json(&mut self, push: u64, models: &Value, mutations: &[Queued]) -> Result<Value> {
        let acts: Vec<Value> = mutations
            .iter()
            .map(|q| match (&q.mutation.call_id, &q.mutation.args) {
                (Some(call_id), Some(args)) => {
                    let mut call = json!({"ordinal":q.ordinal,"callId":call_id,"name":q.mutation.name,"version":q.mutation.version,"args":args});
                    if let Some(store) = q.mutation.store.wire() {
                        call["store"] = store;
                    }
                    call
                }
                _ => json!({"ordinal":q.ordinal,"name":q.mutation.name,"version":q.mutation.version,"operations":q.mutation.operations}),
            })
            .collect();
        Ok(
            json!({"clientId":self.client_id()?,"batchSequence":push,"models":models,"mutations":acts}),
        )
    }
    /// The bytes of the push in flight, re-encoded from its rows and its
    /// frozen declaration: byte for byte what was sent, across restarts.
    pub fn encode_push(&mut self, push: u64) -> Result<Vec<u8>> {
        let mutations: Vec<Queued> = self
            .queued()?
            .into_iter()
            .filter(|q| q.push == Some(push))
            .collect();
        let models = self
            .push_models()?
            .ok_or_else(|| invalid("push in flight has no frozen declaration"))?;
        let request = self.request_json(push, &models, &mutations)?;
        let bytes = canonical_json(&request)?.into_bytes();
        if mutations
            .first()
            .is_some_and(|q| q.mutation.call_id.is_some())
        {
            PushRequest::decode_actions(&bytes, self.schema)?.encode()
        } else {
            PushRequest::decode(&bytes)?.encode()
        }
    }
    pub fn freeze(&mut self, max_bytes: usize) -> Result<Option<Vec<u8>>> {
        if max_bytes == 0 {
            return Ok(None);
        }
        if let Some(push) = self.in_flight()? {
            return Ok(Some(self.encode_push(push)?));
        }
        let queue = self.queued()?;
        let blocked_keys: BTreeSet<String> = self
            .prerequisite_keys()?
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        let unsent: BTreeSet<u64> = queue
            .iter()
            .filter(|q| q.push.is_none())
            .map(|q| q.ordinal)
            .collect();
        let mut selected: Vec<Queued> = vec![];
        let mut chosen = BTreeSet::new();
        let next_push = self
            .scalar("SELECT next_push FROM axton_client", &[])?
            .map(|v| crate::engine::as_u64(&v))
            .transpose()?
            .unwrap_or(1);
        let models = serde_json::to_value(crate::declared_models(self.schema))?;
        for q in queue.iter().filter(|q| q.push.is_none()) {
            if selected.first().is_some_and(|first| {
                first.mutation.call_id.is_some() != q.mutation.call_id.is_some()
            }) {
                continue;
            }
            if q.mutation
                .prerequisites
                .iter()
                .any(|k| blocked_keys.contains(k))
            {
                continue;
            }
            let blocked = q
                .mutation
                .lifecycle_dependencies
                .iter()
                .any(|d| unsent.contains(d))
                || q.mutation
                    .sequence_dependencies
                    .iter()
                    .any(|d| unsent.contains(d) && !chosen.contains(d));
            if blocked {
                continue;
            }
            if !selected.is_empty() {
                let mut candidate = selected.clone();
                candidate.push(q.clone());
                if canonical_json(&self.request_json(next_push, &models, &candidate)?)?.len()
                    > max_bytes
                {
                    continue;
                }
            }
            chosen.insert(q.ordinal);
            selected.push(q.clone());
            if selected.len() == limits::PUSH_MUTATIONS {
                break;
            }
        }
        if selected.is_empty() {
            return Ok(None);
        }
        let push = self.allocate_push()?;
        let ordinals: Vec<u64> = selected.iter().map(|q| q.ordinal).collect();
        self.assign_push(&ordinals, push)?;
        self.set_push_models(&models)?;
        if selected
            .first()
            .is_some_and(|q| q.mutation.call_id.is_some())
        {
            let mut needed = BTreeSet::new();
            for queued in &selected {
                let action = self
                    .schema
                    .action(&queued.mutation.name, queued.mutation.version)?;
                for output in &action.outputs {
                    if output.kind == "model" {
                        needed.insert((
                            output
                                .model
                                .clone()
                                .ok_or_else(|| invalid("Action output Model missing"))?,
                            output
                                .model_read_version
                                .ok_or_else(|| invalid("Action output read version missing"))?,
                        ));
                    }
                }
            }
            let reads = needed
                .into_iter()
                .map(|(name, version)| self.schema.result_model(&name, version).cloned())
                .collect::<Result<Vec<_>>>()?;
            self.set_push_result_reads(&reads)?;
        }
        Ok(Some(self.encode_push(push)?))
    }
    /// Complete the push in flight from its receipt, all in the caller's
    /// transaction: stage the returned authority beneath the queue as it was
    /// sent, record rejections, remove the completed operations, replay what
    /// remains, and remember the completion. Any failure leaves the frozen
    /// batch for retry. A duplicate receipt changes nothing.
    fn admit_receipt(
        &mut self,
        sequence: u64,
        receipt: &PushReceipt,
    ) -> Result<Option<AdmittedReceipt>> {
        if receipt.batch_sequence != sequence {
            return Err(invalid("receipt answers another batch"));
        }
        if receipt.client_id != self.client_id()? {
            return Err(invalid("receipt answers another client"));
        }
        if sequence <= self.last_completed_push()? {
            return Ok(None);
        }
        let Some(push) = self.in_flight()? else {
            return Err(invalid("unknown batch receipt"));
        };
        if push != sequence {
            return Err(invalid("receipt does not answer the push in flight"));
        }
        let schema = self.schema;
        let mutations: Vec<Queued> = self
            .queued()?
            .into_iter()
            .filter(|q| q.push == Some(push))
            .collect();
        let receipt = if mutations.iter().any(|q| q.mutation.call_id.is_some()) {
            let request = PushRequest::decode_actions(&self.encode_push(push)?, self.schema)?;
            let reads = self
                .push_result_reads()?
                .ok_or_else(|| invalid("frozen Action result contracts missing"))?;
            PushReceipt::decode_actions_with_frozen_results(
                &receipt.encode()?,
                &request,
                self.schema,
                &reads,
            )?
        } else {
            PushReceipt::decode(&receipt.encode()?)?
        };
        let ordinals: BTreeSet<u64> = mutations.iter().map(|q| q.ordinal).collect();
        if receipt
            .rejections
            .iter()
            .any(|r| !ordinals.contains(&r.ordinal))
        {
            return Err(invalid("rejection ordinal not in batch"));
        }
        let rejected: BTreeSet<u64> = receipt.rejections.iter().map(|r| r.ordinal).collect();
        let accepted: Vec<&Queued> = mutations
            .iter()
            .filter(|q| !rejected.contains(&q.ordinal))
            .collect();
        // Every record the accepted operations targeted must come back with
        // its authority: a receipt that omits one cannot complete the batch.
        let mut covered = BTreeSet::new();
        for record in &receipt.records {
            covered.insert(
                schema
                    .record_key(&record.model, &record.identity)?
                    .encoded()?,
            );
        }
        let mut wire_rows = BTreeSet::new();
        for q in &accepted {
            wire_rows.extend(keys_of(schema, q.mutation.operations.iter())?);
        }
        if let Some(missing) = wire_rows.iter().find(|k| !covered.contains(*k)) {
            return Err(invalid(format!(
                "receipt omits the authority of accepted record {missing}"
            )));
        }
        Ok(Some(AdmittedReceipt {
            mutations,
            receipt,
            rejected,
            wire_rows,
            covered,
        }))
    }

    pub(crate) fn validate_receipt(&mut self, sequence: u64, receipt: &PushReceipt) -> Result<()> {
        self.admit_receipt(sequence, receipt).map(|_| ())
    }

    pub fn acknowledge(&mut self, sequence: u64, receipt: &PushReceipt) -> Result<ApplyReport> {
        let mut report = ApplyReport::default();
        let Some(AdmittedReceipt {
            mutations,
            receipt,
            rejected,
            wire_rows,
            covered,
        }) = self.admit_receipt(sequence, receipt)?
        else {
            report.stale = true;
            return Ok(report);
        };
        let schema = self.schema;
        let push = sequence;
        let accepted: Vec<&Queued> = mutations
            .iter()
            .filter(|q| !rejected.contains(&q.ordinal))
            .collect();
        // Every record the batch touched is rebuilt once the rows are gone.
        let mut affected: Held = Held::new();
        for q in &mutations {
            for op in all_ops(&q.mutation) {
                let key = schema.record_key(&op.model, &op.identity)?;
                affected.insert(key.encoded()?, key);
            }
        }
        // A record the receipt answered for takes the server's authority,
        // and so do the descendants of a companion delete of such a record.
        let mut answered: BTreeSet<String> = wire_rows.union(&covered).cloned().collect();
        for q in &accepted {
            for op in &q.mutation.companion {
                let key = schema.record_key(&op.model, &op.identity)?;
                if op.op == OperationKind::Delete && answered.contains(&key.encoded()?) {
                    for child in self.descendants(&key)? {
                        answered.insert(child.encoded()?);
                    }
                }
            }
        }
        // Every other accepted companion keeps its local effect at its
        // original position; the replay below folds it into the base once no
        // earlier pending operation on the record precedes it.
        for q in &accepted {
            for companion in self.companion_ops(q.ordinal)? {
                let key = schema.record_key(&companion.op.model, &companion.op.identity)?;
                let encoded = key.encoded()?;
                if answered.contains(&encoded) {
                    continue;
                }
                self.insert_local_write(
                    q.ordinal,
                    Some(companion.position),
                    LocalWriteKind::Accepted,
                    &companion.op,
                )?;
                affected.insert(encoded, key);
            }
        }
        // Authority is staged while the queue still says which records hold a
        // base; equal stamps compare against that base, not the optimism.
        // Each record fails alone, as on a page: one this client cannot
        // apply is reported and never holds the receipt, and so the queue,
        // back.
        for record in &receipt.records {
            let (applied, entry) = self.stage_isolated(record, &mut affected)?;
            report.applied += usize::from(applied);
            report.reports.extend(entry.map(|mut entry| {
                if let Value::Object(detail) = &mut entry.detail {
                    detail.insert("batch".into(), json!(sequence));
                }
                entry
            }));
        }
        let (rejected_affected, removed_completions) =
            self.mark_rejected_with_completions(&receipt.rejections)?;
        affected.extend(rejected_affected);
        let completed: Vec<u64> = accepted.iter().map(|q| q.ordinal).collect();
        self.delete_mutations(&completed)?;
        report.reports.extend(self.rebuild_held(&affected)?);
        self.set_last_completed_push(push)?;
        report.completions = receipt.completions.clone();
        report.completions.extend(removed_completions);
        Ok(report)
    }
    /// Drop rejected mutations and everything whose lifecycle depended on them,
    /// keep a durable record of why, and rebuild the rows they touched.
    pub fn remove_rejected(&mut self, rejections: &[Rejection]) -> Result<Vec<CallCompletion>> {
        let (affected, completions) = self.mark_rejected_with_completions(rejections)?;
        self.rebuild_held(&affected)?;
        Ok(completions)
    }
    /// Record the rejections, drop their mutations and lifecycle dependents,
    /// and return the records and Action completions for the caller.
    pub fn mark_rejected_with_completions(
        &mut self,
        rejections: &[Rejection],
    ) -> Result<(Held, Vec<CallCompletion>)> {
        let schema = self.schema;
        let queue = self.queued()?;
        let mut rejected: BTreeMap<u64, String> = rejections
            .iter()
            .map(|r| (r.ordinal, r.code.clone()))
            .collect();
        loop {
            let more: Vec<u64> = queue
                .iter()
                .filter(|q| {
                    !rejected.contains_key(&q.ordinal)
                        && q.mutation
                            .lifecycle_dependencies
                            .iter()
                            .any(|d| rejected.contains_key(d))
                })
                .map(|q| q.ordinal)
                .collect();
            if more.is_empty() {
                break;
            }
            for id in more {
                rejected.insert(id, "dependency.rejected".into());
            }
        }
        let mut affected: BTreeMap<String, RecordKey> = BTreeMap::new();
        let mut completions = Vec::new();
        for q in queue.iter().filter(|q| rejected.contains_key(&q.ordinal)) {
            let code = &rejected[&q.ordinal];
            if q.push.is_none()
                && let Some(call_id) = &q.mutation.call_id
            {
                completions.push(CallCompletion {
                    call_id: call_id.clone(),
                    outcome: ActionOutcome::Failed {
                        code: code.clone(),
                        execution: ExecutionState::Rejected,
                    },
                });
            }
            for op in all_ops(&q.mutation) {
                let key = schema.record_key(&op.model, &op.identity)?;
                affected.insert(key.encoded()?, key);
            }
            let records: Vec<Value> = all_ops(&q.mutation)
                .map(|op| json!({"model":op.model,"identity":op.identity}))
                .collect();
            let detail =
                json!({"ordinal":q.ordinal,"code":code,"mutation":q.mutation,"records":records});
            self.insert_rejection(q.ordinal, &q.mutation.name, code, &detail)?;
        }
        let ordinals: Vec<u64> = rejected.keys().copied().collect();
        self.delete_mutations(&ordinals)?;
        Ok((affected, completions))
    }
}
