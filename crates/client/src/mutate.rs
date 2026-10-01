//! Optimistic writes, truth holding, rebuild and cascades. Authority lands
//! through `authority.rs`.
use crate::ddl::before_table;
use crate::engine::Engine;
use crate::queue::{LocalWrite, LocalWriteKind, OpKind, QueuedOp};
use crate::rows::merge_identity;
use crate::store::ClientStore;
use crate::{Mutation, Operation, OperationKind, actions, policies};
use axton_core::{ActionIntent, RecordKey, Result, Schema, invalid};
use serde_json::Value;
use std::collections::BTreeSet;

/// One entry of a record's local history above its base.
enum Step {
    /// An operation of a call still in the queue: wire, companion or effect.
    Pending(QueuedOp),
    /// A settled local write retained at its place.
    Settled(LocalWrite),
}
impl Step {
    /// Committed local order: a call's operations by position, an accepted
    /// companion at its owner's position, an independent write after every
    /// operation of the ordinal it followed.
    fn order(&self) -> (u64, u8, u64, u64) {
        match self {
            Step::Pending(q) => (q.ordinal, 0, q.position, 0),
            Step::Settled(w) => match w.kind {
                LocalWriteKind::Accepted => {
                    (w.ordinal, 0, w.position.unwrap_or_default(), w.sequence)
                }
                LocalWriteKind::Independent => (w.ordinal, 1, 0, w.sequence),
            },
        }
    }
}

/// Apply a settled local write. It was validated when it was made, so it is
/// never a replay conflict: a create sets the row it created (a recreation
/// keeps its newer content), an update of a row that no longer exists
/// changes nothing (it cannot preserve a record whose creation went), and a
/// delete removes the row.
pub(crate) fn apply_settled(row: &mut Option<Value>, op: &Operation) {
    match op.op {
        OperationKind::Create => {
            if let Some(values) = &op.values {
                *row = Some(merge_identity(&op.identity, values));
            }
        }
        OperationKind::Update => {
            if let (Some(current), Some(patch)) =
                (row.as_mut(), op.values.as_ref().and_then(Value::as_object))
            {
                for (k, v) in patch {
                    current[k] = v.clone();
                }
            }
        }
        OperationKind::Delete => *row = None,
    }
}
/// The base with only the settled writes applied: what stays visible when a
/// pending operation no longer replays.
fn settled_view(base: &Option<Value>, steps: &[Step]) -> Option<Value> {
    let mut row = base.clone();
    for step in steps {
        if let Step::Settled(write) = step {
            apply_settled(&mut row, &write.op);
        }
    }
    row
}

pub fn apply_to_row(row: &mut Option<Value>, op: &Operation) -> Result<()> {
    match op.op {
        OperationKind::Create => {
            if row.is_some() {
                return Err(invalid("create already exists"));
            }
            let values = op
                .values
                .as_ref()
                .ok_or_else(|| invalid("create values missing"))?;
            *row = Some(merge_identity(&op.identity, values));
        }
        OperationKind::Update => {
            let current = row.as_mut().ok_or_else(|| invalid("update row missing"))?;
            let patch = op
                .values
                .as_ref()
                .and_then(Value::as_object)
                .ok_or_else(|| invalid("patch missing"))?;
            for (k, v) in patch {
                current[k] = v.clone();
            }
        }
        OperationKind::Delete => {
            *row = None;
        }
    }
    Ok(())
}

fn normalize(schema: &Schema, op: &mut Operation) -> Result<()> {
    op.identity = schema.record_key(&op.model, &op.identity)?.identity;
    match op.op {
        OperationKind::Create => {
            let values = op
                .values
                .as_ref()
                .ok_or_else(|| invalid("create values missing"))?;
            op.values = Some(schema.normalize_state(&op.model, values)?);
        }
        OperationKind::Update => {
            let values = op
                .values
                .as_ref()
                .ok_or_else(|| invalid("update values missing"))?;
            op.values = Some(schema.validate_patch(&op.model, values)?);
        }
        OperationKind::Delete => {
            if op.values.is_some() {
                return Err(invalid("delete cannot contain values"));
            }
        }
    }
    Ok(())
}

impl<S: ClientStore> Engine<'_, S> {
    pub fn read_row(&mut self, key: &RecordKey) -> Result<Option<Value>> {
        let model = self.schema.model(&key.model)?.clone();
        self.row_get(&key.model, &model, &key.identity)
    }
    pub(crate) fn before_get(&mut self, key: &RecordKey) -> Result<Option<Value>> {
        let model = self.schema.model(&key.model)?.clone();
        self.row_get(&before_table(&key.model), &model, &key.identity)
    }
    pub(crate) fn before_set(&mut self, key: &RecordKey, row: Option<&Value>) -> Result<()> {
        let model = self.schema.model(&key.model)?.clone();
        let table = before_table(&key.model);
        match row {
            Some(row) => self.row_upsert(&table, &model, row),
            None => self.row_delete(&table, &model, &key.identity),
        }
    }
    pub(crate) fn main_set(&mut self, key: &RecordKey, row: Option<&Value>) -> Result<()> {
        let model = self.schema.model(&key.model)?.clone();
        match row {
            Some(row) => self.row_upsert(&key.model, &model, row),
            None => self.row_delete(&key.model, &model, &key.identity),
        }
    }
    /// The last known server state of a record with the settled local writes
    /// on it: the before image and its retained writes while it is dirty with
    /// pending mutations, otherwise the visible row itself.
    pub fn truth(&mut self, key: &RecordKey) -> Result<Option<Value>> {
        if self.dirty(key)? {
            let base = self.before_get(key)?;
            let steps = self.history(key)?;
            Ok(settled_view(&base, &steps))
        } else {
            self.read_row(key)
        }
    }
    /// Everything above one record's base, in committed local order.
    fn history(&mut self, key: &RecordKey) -> Result<Vec<Step>> {
        let mut steps: Vec<Step> = self.ops_for(key)?.into_iter().map(Step::Pending).collect();
        steps.extend(self.local_writes_for(key)?.into_iter().map(Step::Settled));
        steps.sort_by_key(Step::order);
        Ok(steps)
    }
    pub fn apply_main(&mut self, op: &Operation) -> Result<()> {
        let key = self.schema.record_key(&op.model, &op.identity)?;
        let model = self.schema.model(&op.model)?.clone();
        match op.op {
            OperationKind::Create => {
                let values = op
                    .values
                    .as_ref()
                    .ok_or_else(|| invalid("create values missing"))?;
                let row = merge_identity(&op.identity, values);
                if self.read_row(&key)?.is_some() {
                    return Err(invalid("create already exists"));
                }
                self.row_insert(&op.model, &model, &row)
            }
            OperationKind::Update => {
                let mut row = self.read_row(&key)?;
                apply_to_row(&mut row, op)?;
                self.row_upsert(
                    &op.model,
                    &model,
                    row.as_ref().ok_or_else(|| invalid("update row missing"))?,
                )
            }
            OperationKind::Delete => self.row_delete(&op.model, &model, &op.identity),
        }
    }
    pub fn hold_truth(&mut self, key: &RecordKey) -> Result<()> {
        if self.dirty(key)? {
            return Ok(());
        }
        let model = self.schema.model(&key.model)?.clone();
        self.copy_aside(&model, &key.identity)
    }
    /// Rebuild one record from its base and its history in committed local
    /// order. Settled writes no earlier pending operation precedes are folded
    /// into the base and forgotten; with nothing pending left the base itself
    /// becomes the row. When a pending operation no longer replays, the base
    /// with the settled writes stays visible and the failing mutation's
    /// ordinal is returned and marked diverged; the queue is untouched
    /// ([#122](https://github.com/zanminwang/axton/issues/122)).
    pub fn rebuild(&mut self, key: &RecordKey) -> Result<Option<u64>> {
        let mut base = self.before_get(key)?;
        let mut steps = self.history(key)?;
        let settled = steps
            .iter()
            .take_while(|step| matches!(step, Step::Settled(_)))
            .count();
        for step in steps.drain(..settled) {
            if let Step::Settled(write) = step {
                apply_settled(&mut base, &write.op);
                self.retain_local_operation(key, &write.op)?;
                self.delete_local_write(write.sequence)?;
            }
        }
        if steps.is_empty() {
            self.main_set(key, base.as_ref())?;
            self.before_set(key, None)?;
            return Ok(None);
        }
        if settled > 0 {
            self.before_set(key, base.as_ref())?;
        }
        let mut row = base.clone();
        let mut failed = None;
        for step in &steps {
            match step {
                Step::Pending(queued) => {
                    if apply_to_row(&mut row, &queued.op).is_err() {
                        failed = Some(queued.ordinal);
                        break;
                    }
                }
                Step::Settled(write) => apply_settled(&mut row, &write.op),
            }
        }
        let fallback = settled_view(&base, &steps);
        let result = if failed.is_some() {
            fallback.clone()
        } else {
            row
        };
        if self.main_set(key, result.as_ref()).is_err() {
            self.main_set(key, fallback.as_ref())?;
        }
        if let Some(ordinal) = failed {
            self.set_diverged(ordinal)?;
        }
        Ok(failed)
    }
    /// Every record reachable from `parent` through declared cascading deletes.
    pub fn descendants(&mut self, parent: &RecordKey) -> Result<Vec<RecordKey>> {
        let schema = self.schema;
        let mut seen = BTreeSet::from([parent.encoded()?]);
        let mut todo = vec![parent.clone()];
        let mut result = vec![];
        while let Some(parent) = todo.pop() {
            for model in &schema.models {
                for relation in &model.relations {
                    if relation.target != parent.model || relation.on_delete != "delete" {
                        continue;
                    }
                    let filter: Vec<(String, Value)> = relation
                        .fields
                        .iter()
                        .zip(&relation.target_fields)
                        .map(|(local, target)| (local.clone(), parent.identity[target].clone()))
                        .collect();
                    let mut identities = self.identities_where(&model.name, model, &filter)?;
                    identities.extend(self.identities_where(
                        &before_table(&model.name),
                        model,
                        &filter,
                    )?);
                    for identity in identities {
                        let child = schema.record_key(&model.name, &identity)?;
                        if seen.insert(child.encoded()?) {
                            todo.push(child.clone());
                            result.push(child);
                        }
                    }
                }
            }
        }
        Ok(result)
    }
    /// Whether the last delete of `parent` by call `ordinal` is followed by a
    /// local recreation: a create later in the same call, or a settled local
    /// write (a direct write or an accepted companion) after it. A recreation
    /// by a later pending call is not one: the server has yet to answer both.
    fn recreated_locally(&mut self, parent: &RecordKey, ordinal: u64) -> Result<bool> {
        let steps = self.history(parent)?;
        let Some(at) = steps.iter().rposition(|step| {
            matches!(step, Step::Pending(q) if q.ordinal == ordinal && q.op.op == OperationKind::Delete)
        }) else {
            return Ok(false);
        };
        Ok(steps[at + 1..].iter().any(|step| match step {
            Step::Pending(q) => q.ordinal == ordinal && q.op.op == OperationKind::Create,
            Step::Settled(w) => w.op.op == OperationKind::Create,
        }))
    }
    /// Extend queued deletes to descendants that appeared after they were
    /// queued. A parent visible again because a local write recreated it
    /// after the delete keeps its current children: they belong to that
    /// recreation, not to the earlier delete. A companion delete's cascade
    /// stays the call's companion.
    pub fn refresh_pending(&mut self) -> Result<()> {
        for queued in self.queued()? {
            let deletes: Vec<(OpKind, Operation)> = queued
                .mutation
                .operations
                .iter()
                .map(|op| (OpKind::Effect, op))
                .chain(
                    queued
                        .mutation
                        .companion
                        .iter()
                        .map(|op| (OpKind::Companion, op)),
                )
                .filter(|(_, op)| op.op == OperationKind::Delete)
                .map(|(kind, op)| (kind, op.clone()))
                .collect();
            for (kind, op) in deletes {
                let parent = self.schema.record_key(&op.model, &op.identity)?;
                if self.read_row(&parent)?.is_some()
                    && self.recreated_locally(&parent, queued.ordinal)?
                {
                    continue;
                }
                for child in self.descendants(&parent)? {
                    let already = queued
                        .mutation
                        .effects
                        .iter()
                        .chain(&queued.mutation.companion)
                        .any(|e| {
                            e.op == OperationKind::Delete
                                && e.model == child.model
                                && e.identity == child.identity
                        });
                    if already {
                        continue;
                    }
                    self.hold_truth(&child)?;
                    self.append_op(
                        queued.ordinal,
                        kind,
                        &Operation {
                            model: child.model.clone(),
                            identity: child.identity.clone(),
                            op: OperationKind::Delete,
                            values: None,
                        },
                    )?;
                    // A child whose own replay fails was already reported when
                    // it was held; here only the delete effect is extended.
                    self.rebuild(&child)?;
                }
            }
        }
        Ok(())
    }
    pub fn enqueue(&mut self, mut mutation: Mutation) -> Result<u64> {
        if mutation.name.trim().is_empty()
            || mutation.version == 0
            || (mutation.operations.is_empty() && mutation.call_id.is_none())
        {
            return Err(invalid("invalid named mutation"));
        }
        if mutation.call_id.is_some() != mutation.args.is_some() {
            return Err(invalid("Action identity and args must appear together"));
        }
        if mutation.call_id.is_none() && !mutation.store.is_all() {
            return Err(invalid("store policy requires an Action call"));
        }
        if let (Some(call_id), Some(args)) = (&mutation.call_id, &mutation.args) {
            let intent = ActionIntent {
                call_id: call_id.clone(),
                name: mutation.name.clone(),
                version: mutation.version,
                args: args.clone(),
                store: mutation.store.clone(),
            }
            .normalize(self.schema)?;
            // Persist the canonical policy; validation saw the explicit one.
            mutation.store = intent.store.clone();
            let descriptor = self.schema.action(&intent.name, intent.version)?;
            actions::validate_bindings(self.schema, descriptor, &intent.args)?;
            let expected = actions::derive_operations(self.schema, descriptor, &intent.args)?;
            if intent.call_id != *call_id
                || intent.args != *args
                || serde_json::to_value(expected)? != serde_json::to_value(&mutation.operations)?
                || !mutation.companion.is_empty()
                || !mutation.effects.is_empty()
                || !mutation.prerequisites.is_empty()
                || !mutation.lifecycle_dependencies.is_empty()
                || !mutation.sequence_dependencies.is_empty()
            {
                return Err(invalid(
                    "Action queue row does not match its canonical intent",
                ));
            }
        }
        for dependency in mutation
            .lifecycle_dependencies
            .iter()
            .chain(&mutation.sequence_dependencies)
        {
            if self.queued_one(*dependency)?.is_none() {
                return Err(invalid("unknown mutation dependency"));
            }
        }
        if mutation.call_id.is_none() {
            // A fresh low-level create: its values become concrete before
            // they are queued. An Action's operations were derived from
            // already expanded args and are checked against them above.
            for op in mutation
                .operations
                .iter_mut()
                .chain(&mut mutation.companion)
            {
                crate::defaults::fill_operation(self.schema, op);
            }
        }
        mutation.effects.clear();
        // Every operation in the local order it is applied in: a delete's
        // cascade first, then the delete. The cascade of a companion delete is
        // the call's companion too: it settles locally with the call, never
        // on the wire.
        let mut ordered: Vec<(OpKind, Operation)> = vec![];
        let wire = mutation.operations.len();
        let mut all: Vec<Operation> = mutation
            .operations
            .drain(..)
            .chain(mutation.companion.drain(..))
            .collect();
        // Each hold_truth runs before its operation reaches the queue, so `dirty`
        // still reflects only earlier mutations.
        for (index, op) in all.iter_mut().enumerate() {
            normalize(self.schema, op)?;
            let (own, cascade) = if index < wire {
                (OpKind::Wire, OpKind::Effect)
            } else {
                (OpKind::Companion, OpKind::Companion)
            };
            self.apply_in_order(op, own, cascade, |_, kind, op| {
                ordered.push((kind, op));
                Ok(())
            })?;
        }
        let of = |kind: OpKind| -> Vec<Operation> {
            ordered
                .iter()
                .filter(|(k, _)| *k == kind)
                .map(|(_, op)| op.clone())
                .collect()
        };
        mutation.operations = of(OpKind::Wire);
        mutation.companion = of(OpKind::Companion);
        mutation.effects = of(OpKind::Effect);
        policies::derive(self, &mut mutation)?;
        let ordinal = self.allocate_ordinal()?;
        let ordered: Vec<(OpKind, &Operation)> =
            ordered.iter().map(|(kind, op)| (*kind, op)).collect();
        self.insert_mutation_ordered(ordinal, &mutation, &ordered)?;
        Ok(ordinal)
    }
    /// Append a local companion to queued call `ordinal`, after everything
    /// the call already wrote: a fresh create gets its generated values, the
    /// operation is normalized and applied, a delete cascades to its
    /// descendants, and each record's base is held first. The operations are
    /// stored as the call's companions in the order they were applied, so
    /// they settle with its outcome and are never sent. The call must be the
    /// latest one, unsent and a canonical Action call, with no independent
    /// write journaled after it: a companion then never settles out of local
    /// order.
    pub(crate) fn append_companion(
        &mut self,
        ordinal: u64,
        mut operation: Operation,
    ) -> Result<()> {
        let owner = self
            .queued_one(ordinal)?
            .ok_or_else(|| invalid("companion owner is not queued"))?;
        if owner.mutation.call_id.is_none() || owner.push.is_some() {
            return Err(invalid("companion owner is not an unsent Mutation call"));
        }
        if self.last_ordinal()? != ordinal || self.independent_writes_after(ordinal)? {
            return Err(invalid(
                "a companion must follow its Mutation before any later write",
            ));
        }
        crate::defaults::fill_operation(self.schema, &mut operation);
        normalize(self.schema, &mut operation)?;
        self.apply_in_order(
            &operation,
            OpKind::Companion,
            OpKind::Companion,
            |engine, kind, op| engine.append_op(ordinal, kind, &op),
        )
    }
    /// Apply one queued operation in local order and hand every write to
    /// `record` as it happens: each record's base is held first, a delete's
    /// cascade to its descendants comes before the delete (as `cascade`),
    /// then the operation itself (as `own`). Queued calls and appended
    /// companions share it, so a cascade is stored at its trigger either way.
    fn apply_in_order(
        &mut self,
        op: &Operation,
        own: OpKind,
        cascade: OpKind,
        mut record: impl FnMut(&mut Self, OpKind, Operation) -> Result<()>,
    ) -> Result<()> {
        let key = self.schema.record_key(&op.model, &op.identity)?;
        self.hold_truth(&key)?;
        if op.op == OperationKind::Delete {
            for child in self.descendants(&key)? {
                self.hold_truth(&child)?;
                let delete = Operation {
                    model: child.model,
                    identity: child.identity,
                    op: OperationKind::Delete,
                    values: None,
                };
                self.apply_main(&delete)?;
                record(self, cascade, delete)?;
            }
        }
        self.apply_main(op)?;
        record(self, own, op.clone())
    }
    /// A local write that is never sent: it moves the truth along with the row.
    pub fn direct(&mut self, mut operation: Operation) -> Result<()> {
        crate::defaults::fill_operation(self.schema, &mut operation);
        normalize(self.schema, &mut operation)?;
        let key = self
            .schema
            .record_key(&operation.model, &operation.identity)?;
        if operation.op == OperationKind::Delete {
            for child in self.descendants(&key)? {
                self.direct_one(Operation {
                    model: child.model,
                    identity: child.identity,
                    op: OperationKind::Delete,
                    values: None,
                })?;
            }
        }
        self.direct_one(operation)
    }
    /// On a dirty record the write is retained after everything already
    /// written, so settling earlier work neither undoes nor reorders it; on a
    /// clean record its operation is retained separately for replica release.
    fn direct_one(&mut self, operation: Operation) -> Result<()> {
        let key = self
            .schema
            .record_key(&operation.model, &operation.identity)?;
        let is_dirty = self.dirty(&key)?;
        self.apply_main(&operation)?;
        if is_dirty {
            let after = self.last_ordinal()?;
            self.insert_local_write(after, None, LocalWriteKind::Independent, &operation)?;
        } else {
            self.retain_local_operation(&key, &operation)?;
        }
        Ok(())
    }
    /// Stop following a scope, whichever subscription it holds; whether one
    /// was removed. Records it delivered stay: a scope is a delivery path,
    /// not an owner, so local content, stamps, before images and pending
    /// operations are all retained.
    pub fn unsubscribe(&mut self, scope: &str) -> Result<bool> {
        self.remove_subscription(scope, None)
    }
}
