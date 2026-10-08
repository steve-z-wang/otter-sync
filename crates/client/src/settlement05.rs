use crate::{authority::Held, engine::Engine, *};
use axton_core::authority as evidence;
use serde_json::{Value, json};
impl<S: ClientStore> Engine<'_, S> {
    pub(crate) fn evidence05(&mut self, key: &RecordKey) -> Result<evidence::RecordEvidence> {
        self.scalar(
            "SELECT evidence FROM axton_authority WHERE model=? AND identity=?",
            &[json!(key.model), json!(key.encoded_identity()?)],
        )?
        .map(|v| crate::mutation_queue::decode(&v))
        .transpose()
        .map(|v| v.unwrap_or_default())
    }
    pub(crate) fn direct_evidence05(&mut self, key: &RecordKey) -> Result<()> {
        let mut e = self.evidence05(key)?;
        e.current = None;
        self.set_evidence05(key, &e)
    }
    pub(crate) fn set_evidence05(
        &mut self,
        key: &RecordKey,
        e: &evidence::RecordEvidence,
    ) -> Result<()> {
        self.exec("axton_authority","INSERT INTO axton_authority(model,identity,evidence) VALUES(?,?,?) ON CONFLICT(model,identity) DO UPDATE SET evidence=excluded.evidence",&[json!(key.model),json!(key.encoded_identity()?),crate::mutation_queue::text(e)?])?;
        Ok(())
    }
}
use crate::engine::as_u64;
use crate::mutation_queue::{decode, text};
use crate::queue::OpKind;
use axton_core::v05::{self, Validate};
use std::collections::BTreeMap;
#[derive(Clone, Debug)]
pub struct PendingSettlement05 {
    pub mutation_id: u64,
    pub batch_id: u64,
    pub missing_keys: Vec<v05::RecordKey>,
}
impl<S: ClientStore> Engine<'_, S> {
    fn outcome05(&mut self, id: u64) -> Result<Option<v05::MutationOutcome>> {
        let r=self.rows("SELECT sync_cursor,result,targets,rejection_code,rejection_message FROM axton_mutation_queue WHERE id=?",&[json!(id)])?.rows;
        let Some(r) = r.first() else { return Ok(None) };
        if let Some(code) = r[3].as_str() {
            return Ok(Some(v05::MutationOutcome::Rejected {
                code: code.into(),
                message: r[4].as_str().map(str::to_owned),
            }));
        }
        if r[0].is_null() {
            return Ok(None);
        }
        Ok(Some(v05::MutationOutcome::Accepted {
            sync_cursor: as_u64(&r[0])?,
            result: decode(&r[1])?,
            targets: decode(&r[2])?,
        }))
    }
    fn completion05(&mut self, id: u64) -> Result<CallCompletion> {
        let outcome = match self
            .outcome05(id)?
            .ok_or_else(|| invalid("outcome missing"))?
        {
            v05::MutationOutcome::Accepted { result, .. } => ActionOutcome::Succeeded { result },
            v05::MutationOutcome::Rejected { code, .. } => ActionOutcome::Failed {
                code,
                execution: ExecutionState::Rejected,
            },
        };
        Ok(CallCompletion {
            call_id: format!("{}:{id}", self.context05()?.store_id),
            outcome,
        })
    }
    fn acknowledge05(&mut self, ack: &v05::BatchAcknowledgement) -> Result<ApplyReport> {
        ack.validate()?;
        ack.context.admit_store(&self.context05()?)?;
        let last = as_u64(
            &self
                .scalar("SELECT last_acknowledged_batch_id FROM axton_store", &[])?
                .unwrap(),
        )?;
        if ack.batch_id <= last {
            let rows=self.rows("SELECT id,batch_digest,batch_materialization FROM axton_mutation_queue WHERE batch_id=? ORDER BY id",&[json!(ack.batch_id)])?.rows;
            if rows.len() != ack.results.len() || rows.is_empty() {
                return Err(invalid("unknown acknowledged Batch"));
            }
            for r in rows {
                let id = as_u64(&r[0])?;
                let result = ack
                    .results
                    .iter()
                    .find(|r| r.mutation_id == id)
                    .ok_or_else(|| invalid("acknowledgement membership mismatch"))?;
                if r[1] != ack.digest
                    || r[2] != ack.context.materialization
                    || self.outcome05(id)?.as_ref() != Some(&result.outcome)
                {
                    return Err(invalid("acknowledgement replay differs"));
                }
            }
            return Ok(ApplyReport::default());
        }
        if ack.batch_id != last + 1 {
            return Err(invalid("Batch acknowledgement skipped"));
        }
        let request = self.reconstruct_batch05(ack.batch_id)?;
        v05::validate_acknowledgement(&request, ack)?;
        // Validate every result and snapshot before changing any durable outcome.
        for result in &ack.results {
            if let v05::MutationOutcome::Accepted {
                result: value,
                targets,
                ..
            } = &result.outcome
            {
                let row=self.rows("SELECT name,descriptor_version,descriptor FROM axton_mutation_queue WHERE id=?",&[json!(result.mutation_id)])?.rows.remove(0);
                let schema = self.retained_schema05(row[2].as_str().unwrap())?;
                let action = schema.action(row[0].as_str().unwrap(), as_u64(&row[1])?)?;
                axton_core::validate_action_result(&schema, action, value)?;
                for target in targets {
                    let k = target.key();
                    let canonical = schema.record_key(&k.model, &k.identity)?;
                    if canonical.identity != k.identity {
                        return Err(invalid("noncanonical settlement identity"));
                    }
                    let r = match target {
                        v05::SettlementTarget::Stream { fallback, .. } => fallback,
                        v05::SettlementTarget::Private { record } => record,
                    };
                    if !r.state.is_null() {
                        schema.validate_state(&k.model, &r.state)?;
                    }
                }
            }
        }
        let mut rejected = BTreeMap::new();
        for result in &ack.results {
            let id = result.mutation_id;
            match &result.outcome {
                v05::MutationOutcome::Accepted {
                    sync_cursor,
                    result,
                    targets,
                } => {
                    self.exec("axton_mutation_queue","UPDATE axton_mutation_queue SET sync_cursor=?,result=?,targets=? WHERE id=?",&[json!(sync_cursor),text(result)?,text(targets)?,json!(id)])?;
                }
                v05::MutationOutcome::Rejected { code, message } => {
                    rejected.insert(id, (code.clone(), message.clone()));
                }
            }
        }
        let report = self.reject_owned05(rejected)?;
        self.exec(
            "axton_store",
            "UPDATE axton_store SET last_acknowledged_batch_id=?",
            &[json!(ack.batch_id)],
        )?;
        Ok(report)
    }
    fn reject_owned05(
        &mut self,
        mut rejected: BTreeMap<u64, (String, Option<String>)>,
    ) -> Result<ApplyReport> {
        let mut held = Held::new();
        loop {
            let mut added = false;
            let dependencies=self.rows("SELECT ordinal,depends_on FROM axton_mutation_dependency WHERE kind='lifecycle'",&[])?.rows;
            for r in dependencies {
                let id = as_u64(&r[0])?;
                let dep = as_u64(&r[1])?;
                if rejected.contains_key(&dep) && !rejected.contains_key(&id) {
                    rejected.insert(id, ("dependency.rejected".into(), None));
                    added = true;
                }
            }
            if !added {
                break;
            }
        }
        let mut completions = vec![];
        for (id, (code, message)) in rejected {
            for op in self.call_ops(id)? {
                let key = self.schema.record_key(&op.op.model, &op.op.identity)?;
                held.insert(key.encoded()?, key);
            }
            self.exec("axton_mutation_queue","UPDATE axton_mutation_queue SET rejection_code=?,rejection_message=?,reconciled=1 WHERE id=?",&[json!(code),message.map_or(Value::Null,Value::String),json!(id)])?;
            completions.push(self.completion05(id)?);
        }
        let reports = self.rebuild_held(&held)?;
        Ok(ApplyReport {
            reports,
            completions,
            ..Default::default()
        })
    }
    fn target_evidence05(&mut self, key: &v05::RecordKey) -> Result<v05::TargetEvidence> {
        let key = self.schema.record_key(&key.model, &key.identity)?;
        let evidence = self.evidence05(&key)?;
        let materialization = self.context05()?.materialization;
        let cursor = evidence.history.get(&materialization).copied();
        Ok(v05::TargetEvidence {
            content_cursor: cursor,
            materialization: cursor.map(|_| materialization),
            removed_cursor: evidence.membership.filter(|m| !m.live).map(|m| m.cursor),
            protected: evidence.current.is_some(),
        })
    }
    pub fn pending_settlement05(&mut self) -> Result<Vec<PendingSettlement05>> {
        let context = self.context05()?;
        let rows=self.rows("SELECT id,batch_id,targets FROM axton_mutation_queue WHERE sync_cursor IS NOT NULL AND reconciled=0 ORDER BY id",&[])?.rows;
        let mut pending = vec![];
        for r in rows {
            let targets: Vec<v05::SettlementTarget> = decode(&r[2])?;
            let mut missing_keys = vec![];
            for target in targets {
                if v05::target_disposition(
                    &target,
                    &context.materialization,
                    &self.target_evidence05(target.key())?,
                )? == v05::TargetDisposition::AwaitAuthority
                {
                    missing_keys.push(target.key().clone())
                }
            }
            pending.push(PendingSettlement05 {
                mutation_id: as_u64(&r[0])?,
                batch_id: as_u64(&r[1])?,
                missing_keys,
            })
        }
        Ok(pending)
    }
    fn owned05(&mut self, id: u64, step: u64, key: &RecordKey) -> Result<bool> {
        let saved=self.scalar("SELECT owner_history FROM axton_mutation_queue_operation WHERE mutation_id=? AND step=?",&[json!(id),json!(step)])?.ok_or_else(||invalid("owned operation missing"))?;
        let history: BTreeMap<String, u64> = decode(&saved)?;
        Ok(self
            .evidence05(key)?
            .history
            .values()
            .max()
            .copied()
            .unwrap_or(0)
            <= history.values().max().copied().unwrap_or(0))
    }
    fn retain_owned05(&mut self, id: u64, step: u64, op: &Operation) -> Result<()> {
        let seq=self.scalar("SELECT local_sequence FROM axton_mutation_queue_operation WHERE mutation_id=? AND step=?",&[json!(id),json!(step)])?.ok_or_else(||invalid("owned operation missing"))?;
        self.exec("axton_local_write","INSERT INTO axton_local_write(sequence,ordinal,position,disposition,model,identity,op,\"values\") VALUES(?,?,?,'accepted',?,?,?,?)",&[seq,json!(id),json!(step),json!(op.model),text(&op.identity)?,serde_json::to_value(op.op)?,op.values.as_ref().map(text).transpose()?.unwrap_or(Value::Null)])?;
        Ok(())
    }
    /// Run inside the same transaction as authority/progress. The caller rebuilds
    /// held keys and emits returned completion events only after COMMIT.
    pub fn reconcile_ready05(&mut self, held: &mut Held) -> Result<Vec<CallCompletion>> {
        let context = self.context05()?;
        let cursor = self
            .scalar("SELECT cursor FROM axton_store", &[])?
            .and_then(|v| v.as_u64());
        let pending = self.pending_settlement05()?;
        let mut completions = vec![];
        for pending in pending {
            let id = pending.mutation_id;
            let Some(v05::MutationOutcome::Accepted {
                sync_cursor,
                targets,
                ..
            }) = self.outcome05(id)?
            else {
                continue;
            };
            let mut evidence = BTreeMap::new();
            for target in &targets {
                evidence.insert(
                    target.key().encoded()?,
                    self.target_evidence05(target.key())?,
                );
            }
            if !v05::settlement_ready(
                sync_cursor,
                cursor,
                &targets,
                &context.materialization,
                &evidence,
            )? {
                continue;
            }
            let ops = self.call_ops(id)?;
            for op in &ops {
                let k = self.schema.record_key(&op.op.model, &op.op.identity)?;
                held.insert(k.encoded()?, k);
            }
            for target in &targets {
                if v05::target_disposition(
                    target,
                    &context.materialization,
                    &evidence[&target.key().encoded()?],
                )? != v05::TargetDisposition::FinalizeOwned
                {
                    continue;
                }
                let record = match target {
                    v05::SettlementTarget::Stream { fallback, .. } => fallback,
                    v05::SettlementTarget::Private { record } => record,
                };
                let key = self
                    .schema
                    .record_key(&record.key.model, &record.key.identity)?;
                let Some(position) = ops
                    .iter()
                    .filter(|o| {
                        o.kind == OpKind::Wire
                            && o.op.model == key.model
                            && o.op.identity == key.identity
                    })
                    .map(|o| o.position)
                    .max()
                else {
                    return Err(invalid("unowned private target"));
                };
                if !self.owned05(id, position, &key)? {
                    continue;
                }
                let mut op = Operation {
                    model: key.model.clone(),
                    identity: key.identity.clone(),
                    op: if record.state.is_null() {
                        OperationKind::Delete
                    } else {
                        OperationKind::Create
                    },
                    values: if record.state.is_null() {
                        None
                    } else {
                        Some(record.state.clone())
                    },
                };
                crate::defaults::fill_operation(self.schema, &mut op);
                if let Some(v) = &op.values {
                    self.schema.validate_state(&op.model, v)?;
                }
                self.retain_owned05(id, position, &op)?;
            }
            for op in ops.iter().filter(|o| o.kind == OpKind::Companion) {
                let key = self.schema.record_key(&op.op.model, &op.op.identity)?;
                if targets
                    .iter()
                    .any(|t| t.key().model == key.model && t.key().identity == key.identity)
                {
                    continue;
                }
                if self.owned05(id, op.position, &key)? {
                    self.retain_owned05(id, op.position, &op.op)?;
                    self.direct_evidence05(&key)?;
                }
            }
            self.exec(
                "axton_mutation_queue",
                "UPDATE axton_mutation_queue SET reconciled=1 WHERE id=?",
                &[json!(id)],
            )?;
            self.exec(
                "axton_mutation_queue_operation",
                "DELETE FROM axton_mutation_queue_operation WHERE mutation_id=?",
                &[json!(id)],
            )?;
            completions.push(self.completion05(id)?);
        }
        Ok(completions)
    }
}
impl<S: ClientStore> Client<S> {
    pub fn acknowledge_batch05(&mut self, ack: &v05::BatchAcknowledgement) -> Result<ApplyReport> {
        self.request_context05()?;
        self.write(|e| e.acknowledge05(ack))
    }
    pub fn pending_settlement05(&mut self) -> Result<Vec<PendingSettlement05>> {
        self.request_context05()?;
        self.view(|e| e.pending_settlement05())
    }
    pub fn settle_ready05(&mut self) -> Result<ApplyReport> {
        self.request_context05()?;
        self.write(|e| {
            let mut held = Held::new();
            let completions = e.reconcile_ready05(&mut held)?;
            let reports = e.rebuild_held(&held)?;
            Ok(ApplyReport {
                reports,
                completions,
                ..Default::default()
            })
        })
    }
    pub fn call_completion05(&mut self, call: &str) -> Result<Option<CallCompletion>> {
        let context = self.request_context05()?;
        let prefix = format!("{}:", context.store_id);
        let Some(id) = call
            .strip_prefix(&prefix)
            .and_then(|s| s.parse::<u64>().ok())
        else {
            return Err(invalid("foreign Call"));
        };
        self.view(|e| {
            if e.scalar(
                "SELECT reconciled FROM axton_mutation_queue WHERE id=?",
                &[json!(id)],
            )? == Some(json!(1))
            {
                e.completion05(id).map(Some)
            } else {
                Ok(None)
            }
        })
    }
}
impl<S: ClientStore> Engine<'_, S> {
    /// Stage admitted authority beneath local replay. No progress is inferred
    /// from record cursors; the owning delivery worker commits coverage separately.
    pub fn stage_authority05(
        &mut self,
        context: &v05::RequestContext,
        changes: &[v05::AuthorityChange],
        held: &mut Held,
    ) -> Result<usize> {
        context.admit(&self.context05()?)?;
        self.stage_changes05(context, changes, held)
    }
    pub fn stage_materialization05(
        &mut self,
        context: &v05::RequestContext,
        changes: &[v05::AuthorityChange],
        held: &mut Held,
    ) -> Result<usize> {
        let active = self.context05()?;
        context.admit_store(&active)?;
        if context.materialization != active.materialization
            && self
                .pending_schema05()?
                .is_none_or(|p| p.desired_context != *context)
        {
            return Err(invalid("unowned desired schema context"));
        }
        self.stage_changes05(context, changes, held)
    }
    fn stage_changes05(
        &mut self,
        context: &v05::RequestContext,
        changes: &[v05::AuthorityChange],
        held: &mut Held,
    ) -> Result<usize> {
        // Immediate UNIQUE checks must see the final atomic projection, rather
        // than a changed row conflicting with another row's superseded value.
        // The enclosing Store transaction rolls this transient removal back on
        // any admission, constraint or commit failure.
        for change in changes {
            change.validate()?;
            let k = change.key();
            let key = self.schema.record_key(&k.model, &k.identity)?;
            if key.identity != k.identity {
                return Err(invalid("noncanonical authority key"));
            }
            if let v05::AuthorityChange::Record { cursor, .. } = change
                && self
                    .evidence05(&key)?
                    .admission(&context.materialization, *cursor)?
                    != evidence::AuthorityAdmission::Duplicate
            {
                self.main_set(&key, None)?;
            }
        }
        let mut applied = 0;
        let mut ordered: Vec<_> = changes.iter().collect();
        ordered.sort_by_key(|c| {
            (
                c.cursor(),
                matches!(c,v05::AuthorityChange::Record{state,..} if state.is_null()),
            )
        });
        for change in ordered {
            change.validate()?;
            let k = change.key();
            let key = self.schema.record_key(&k.model, &k.identity)?;
            if key.identity != k.identity {
                return Err(invalid("noncanonical authority key"));
            }
            let mut evidence = self.evidence05(&key)?;
            match change {
                v05::AuthorityChange::Remove { cursor, .. } => {
                    if evidence.remove(*cursor)? {
                        self.set_evidence05(&key, &evidence)?;
                        applied += 1;
                    }
                }
                v05::AuthorityChange::Record { cursor, state, .. } => {
                    let admission = evidence.admission(&context.materialization, *cursor)?;
                    if admission == evidence::AuthorityAdmission::Duplicate {
                        continue;
                    }
                    let incoming = if state.is_null() {
                        None
                    } else {
                        Some(crate::rows::merge_identity(
                            &key.identity,
                            &self.schema.validate_state(&key.model, state)?,
                        ))
                    };
                    evidence.install(&context.materialization, *cursor, incoming.is_none())?;
                    self.set_evidence05(&key, &evidence)?;
                    self.exec(
                        "axton_authority",
                        "UPDATE axton_authority SET base=? WHERE model=? AND identity=?",
                        &[text(&incoming)?, json!(key.model), text(&key.identity)?],
                    )?;
                    if admission == evidence::AuthorityAdmission::Newer {
                        self.stage_one(&key, incoming.as_ref(), held)?;
                    } else {
                        self.stage_preserving_local(&key, incoming.as_ref(), held)?;
                    }
                    if incoming.is_none() {
                        for child in self.descendants_where(&key, |e, k| {
                            Ok(e.evidence05(k)?
                                .history
                                .values()
                                .max()
                                .copied()
                                .unwrap_or(0)
                                <= *cursor)
                        })? {
                            if admission == evidence::AuthorityAdmission::Newer {
                                self.stage_one(&child, None, held)?;
                            } else {
                                self.stage_preserving_local(&child, None, held)?;
                            }
                            self.direct_evidence05(&child)?;
                        }
                    }
                    applied += 1;
                }
            }
        }
        Ok(applied)
    }
    pub fn stage_cache05(
        &mut self,
        records: &[v05::ReadRecord],
        store: bool,
        held: &mut Held,
    ) -> Result<usize> {
        let mut applied = 0;
        for record in records {
            record.validate()?;
            let k = &record.key;
            let key = self.schema.record_key(&k.model, &k.identity)?;
            if key.identity != k.identity {
                return Err(invalid("noncanonical cache identity"));
            }
            let row = if record.state.is_null() {
                None
            } else {
                Some(crate::rows::merge_identity(
                    &key.identity,
                    &self.schema.validate_state(&k.model, &record.state)?,
                ))
            };
            if !store || row.is_none() || self.evidence05(&key)?.current.is_some() {
                continue;
            }
            let row = row.unwrap();
            let mut blocked = false;
            for relation in self.schema.model(&key.model)?.relations.clone() {
                if relation.on_delete != "delete" {
                    continue;
                }
                let mut identity = serde_json::Map::new();
                for (field, target) in relation.fields.iter().zip(&relation.target_fields) {
                    if row[field].is_null() {
                        identity.clear();
                        break;
                    }
                    identity.insert(target.clone(), row[field].clone());
                }
                if !identity.is_empty() {
                    let parent = self
                        .schema
                        .record_key(&relation.target, &Value::Object(identity))?;
                    if self.evidence05(&parent)?.current.is_some_and(|e| e.deleted) {
                        blocked = true;
                        break;
                    }
                }
            }
            if blocked {
                continue;
            }
            let evidence = self.evidence05(&key)?;
            self.set_evidence05(&key, &evidence)?;
            self.stage_one(&key, Some(&row), held)?;
            applied += 1;
        }
        Ok(applied)
    }
    /// An already validated handshake establishes S and the normal starting
    /// prefix once. Bootstrap coverage remains absent until its final owned unit.
    /// Repeated handshakes never restamp the file's start.
    pub fn initialize_stream05(&mut self, start: u64) -> Result<()> {
        axton_core::counter(start)?;
        self.exec(
            "axton_store",
            "UPDATE axton_store SET start_cursor=?,cursor=? WHERE start_cursor IS NULL",
            &[json!(start), json!(start)],
        )?;
        Ok(())
    }
    /// Called after an admitted complete delivery unit. Coverage and data must
    /// share the transaction; record cursors never call this method.
    pub fn commit_sync_coverage05(&mut self, after: u64, through: u64) -> Result<()> {
        axton_core::counter(after)?;
        axton_core::counter(through)?;
        if through < after {
            return Err(invalid("invalid coverage"));
        }
        let cursor = self
            .scalar("SELECT cursor FROM axton_store", &[])?
            .filter(|v| !v.is_null())
            .map(|v| as_u64(&v))
            .transpose()?;
        if cursor.unwrap_or(0) != after {
            return Err(invalid("coverage prefix gap"));
        }
        self.exec(
            "axton_store",
            "UPDATE axton_store SET cursor=?",
            &[json!(through)],
        )?;
        Ok(())
    }
}
impl<S: ClientStore> Client<S> {
    /// Storage primitive for an already admitted complete authority unit.
    /// Task 5 supplies immutable plan/progress validation and lane ownership.
    pub fn install_authority05(
        &mut self,
        context: &v05::RequestContext,
        changes: &[v05::AuthorityChange],
        coverage: Option<(u64, u64)>,
    ) -> Result<ApplyReport> {
        context.admit(&self.request_context05()?)?;
        self.write(|e| {
            let mut held = Held::new();
            let applied = e.stage_authority05(context, changes, &mut held)?;
            if let Some((after, through)) = coverage {
                e.commit_sync_coverage05(after, through)?;
            }
            let completions = e.reconcile_ready05(&mut held)?;
            let reports = e.rebuild_held(&held)?;
            Ok(ApplyReport {
                applied,
                reports,
                completions,
                ..Default::default()
            })
        })
    }
    pub fn install_cache05(
        &mut self,
        records: &[v05::ReadRecord],
        store: bool,
    ) -> Result<ApplyReport> {
        self.request_context05()?;
        self.write(|e| {
            let mut held = Held::new();
            let applied = e.stage_cache05(records, store, &mut held)?;
            let reports = e.rebuild_held(&held)?;
            Ok(ApplyReport {
                applied,
                reports,
                ..Default::default()
            })
        })
    }
    pub fn record_evidence05(&mut self, key: &RecordKey) -> Result<evidence::RecordEvidence> {
        self.request_context05()?;
        self.view(|e| e.evidence05(key))
    }
}
impl<S: ClientStore> ClientTransaction<'_, S> {
    pub fn discard_mutation05(&mut self, id: u64) -> Result<ApplyReport> {
        self.cancel_unsent05(id, true)
    }
    pub fn drop_mutation05(&mut self, id: u64) -> Result<ApplyReport> {
        self.cancel_unsent05(id, false)
    }
    fn cancel_unsent05(&mut self, id: u64, acknowledge: bool) -> Result<ApplyReport> {
        self.savepoint(|tx| {
            let row = tx
                .engine
                .rows(
                    "SELECT batch_id,reconciled FROM axton_mutation_queue WHERE id=?",
                    &[json!(id)],
                )?
                .rows;
            let Some(row) = row.first() else {
                return Err(invalid("unknown Mutation"));
            };
            if !row[0].is_null() {
                return Err(invalid("possibly sent Mutation cannot be discarded"));
            }
            if row[1] == 1 {
                return Ok(ApplyReport::default());
            }
            let report = tx
                .engine
                .reject_owned05(BTreeMap::from([(id, ("dropped".into(), None))]))?;
            if acknowledge {
                tx.engine.acknowledge_rejection05(id)?;
            }
            Ok(report)
        })
    }
    pub fn dismiss_rejection05(&mut self, id: u64) -> Result<()> {
        self.savepoint(|tx| tx.engine.acknowledge_rejection05(id))
    }
}
impl<S: ClientStore> Client<S> {
    pub fn mutation_result05(&mut self, id: u64) -> Result<Option<v05::MutationResult>> {
        self.request_context05()?;
        self.view(|e| {
            if e.scalar(
                "SELECT reconciled FROM axton_mutation_queue WHERE id=?",
                &[json!(id)],
            )? == Some(json!(1))
            {
                Ok(e.outcome05(id)?.map(|outcome| v05::MutationResult {
                    mutation_id: id,
                    outcome,
                }))
            } else {
                Ok(None)
            }
        })
    }
}

impl<S: ClientStore> Engine<'_, S> {
    fn acknowledge_rejection05(&mut self, id: u64) -> Result<()> {
        if self
            .scalar(
                "SELECT 1 FROM axton_mutation_queue WHERE id=?",
                &[json!(id)],
            )?
            .is_none()
        {
            return Ok(());
        }
        if self.scalar("SELECT 1 FROM axton_mutation_queue WHERE id=? AND rejection_code IS NOT NULL AND reconciled=1", &[json!(id)])?.is_none() {
            return Err(invalid("completed rejection required"));
        }
        self.exec(
            "axton_mutation_queue",
            "UPDATE axton_mutation_queue SET rejection_acknowledged=1 WHERE id=?",
            &[json!(id)],
        )?;
        self.exec(
            "axton_mutation_queue_operation",
            "DELETE FROM axton_mutation_queue_operation WHERE mutation_id=?",
            &[json!(id)],
        )?;
        self.exec(
            "axton_mutation_prerequisite",
            "DELETE FROM axton_mutation_prerequisite WHERE ordinal=?",
            &[json!(id)],
        )?;
        self.exec(
            "axton_mutation_dependency",
            "DELETE FROM axton_mutation_dependency WHERE ordinal=? OR depends_on=?",
            &[json!(id), json!(id)],
        )?;
        Ok(())
    }
}
