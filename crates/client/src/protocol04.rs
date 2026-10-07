//! Persisted protocol-4 state on the existing Engine/Client storage path.
use crate::{Client, ClientStore, Result, Schema, invalid, schema_store};
use axton_core::{
    Compatibility,
    v04::{self, Validate},
};
use serde_json::json;

const STORE_DDL: &str = "CREATE TABLE IF NOT EXISTS axton_v04_store (
  singleton INTEGER PRIMARY KEY CHECK(singleton=1), context TEXT NOT NULL, cursor INTEGER NOT NULL DEFAULT 0, initialized INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS axton_v04_descriptor (materialization TEXT PRIMARY KEY, descriptor TEXT NOT NULL, projection_generation TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS axton_v04_record (model TEXT NOT NULL, identity TEXT NOT NULL, evidence TEXT NOT NULL, base TEXT NOT NULL DEFAULT 'null', generation INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(model,identity));
CREATE TABLE IF NOT EXISTS axton_v04_page (singleton INTEGER PRIMARY KEY CHECK(singleton=1), page TEXT NOT NULL, progress TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS axton_v04_call (call_id TEXT PRIMARY KEY, ordinal INTEGER NOT NULL, intent TEXT NOT NULL, receipt TEXT, status TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS axton_v04_completion (call_id TEXT PRIMARY KEY, completion TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS axton_v04_op (ordinal INTEGER NOT NULL, position INTEGER NOT NULL, generation INTEGER NOT NULL, PRIMARY KEY(ordinal,position));
CREATE TABLE IF NOT EXISTS axton_v04_delivery_request (owner TEXT PRIMARY KEY, intent TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS axton_v04_request (call_id TEXT PRIMARY KEY, intent TEXT NOT NULL, response TEXT);
CREATE TABLE IF NOT EXISTS axton_v04_bootstrap (manifest_id TEXT PRIMARY KEY, purpose TEXT NOT NULL, active INTEGER NOT NULL, coverage TEXT NOT NULL);";

// A read-version increase is not itself a storage break. Preserve the complete
// original descriptor separately; only the compatibility comparison aligns versions.
fn storage_compatibility(stored: &Schema, incoming: &Schema) -> Compatibility {
    let mut comparison = stored.clone();
    for old in &mut comparison.models {
        if let Some(new) = incoming.models.iter().find(|model| model.name == old.name) {
            if new.version < old.version {
                return Compatibility::Incompatible(format!(
                    "model {} read version regressed",
                    old.name
                ));
            }
            old.version = new.version;
        }
    }
    Schema::compatibility(&comparison, incoming)
}

fn has_table<S: ClientStore>(store: &mut S, table: &str) -> Result<bool> {
    Ok(!store
        .query(
            "SELECT name FROM sqlite_schema WHERE type='table' AND name=?",
            &[json!(table)],
        )?
        .rows
        .is_empty())
}
pub(crate) fn read_context<S: ClientStore>(store: &mut S) -> Result<Option<v04::RequestContext>> {
    if !has_table(store, "axton_v04_store")? {
        return Ok(None);
    }
    let rows = store.query("SELECT context FROM axton_v04_store WHERE singleton=1", &[])?;
    rows.rows
        .first()
        .map(|row| {
            v04::decode(
                row[0]
                    .as_str()
                    .ok_or_else(|| invalid("Store context is not JSON"))?
                    .as_bytes(),
            )
        })
        .transpose()
}
#[derive(Clone, Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResetStoreReport {
    pub context: v04::RequestContext,
    pub abandoned_calls: Vec<crate::AbandonedCall>,
}
impl<S: ClientStore> Client<S> {
    /// Stable binding admission occurs before any schema coordination write.
    /// A 0.3 file must be left behind explicitly; it is never silently adopted.
    pub fn open_bound(store: S, schema: Schema, binding: v04::StoreBinding) -> Result<Self> {
        Self::open_bound_with_projection(store, schema, binding, "1")
    }
    pub fn open_bound_with_projection(
        mut store: S,
        schema: Schema,
        binding: v04::StoreBinding,
        projection_generation: &str,
    ) -> Result<Self> {
        schema.validate()?;
        binding.validate()?;
        let saved = read_context(&mut store)?;
        if let Some(context) = &saved {
            if context.binding != binding {
                return Err(invalid("binding_mismatch"));
            }
            if let Some(previous) = schema_store::read_descriptor(&mut store)?
                && let Compatibility::Incompatible(reason) =
                    storage_compatibility(&previous, &schema)
            {
                return Err(invalid(format!("schema_incompatible: {reason}")));
            }
        } else if has_table(&mut store, "axton_client")? {
            return Err(invalid(
                "protocol_mismatch: protocol-4 requires a fresh Store",
            ));
        }
        let materialization = v04::materialization_id(&schema, projection_generation)?;
        let context = v04::RequestContext {
            protocol: 4,
            binding,
            materialization,
            incarnation: saved
                .as_ref()
                .map(|context| context.incarnation.clone())
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
        };
        // Retain descriptors, including former contexts, before reconciliation.
        store.begin()?;
        let initialized = (|| {
            store.execute_batch(STORE_DDL)?;
            if saved.is_none() {
                store.execute(
                    "INSERT INTO axton_v04_store(singleton,context) VALUES(1,?)",
                    &[json!(
                        String::from_utf8(v04::encode(&context)?)
                            .map_err(|_| invalid("context UTF8"))?
                    )],
                )?;
            }
            store.execute("INSERT OR IGNORE INTO axton_v04_descriptor(materialization,descriptor,projection_generation) VALUES(?,?,?)",&[json!(context.materialization),json!(schema_store::descriptor_text(&schema)?),json!(projection_generation)])?;
            Ok::<_, axton_core::Error>(())
        })();
        if let Err(error) = initialized {
            store.rollback()?;
            return Err(error);
        }
        if let Err(error) = store.commit() {
            let _ = store.rollback();
            return Err(error);
        }
        let mut client = Self::open(store, schema)?;
        client.write(|engine| {
            let (subscription, _) = engine.ensure_subscription(&context.binding.stream)?;
            if engine.scalar(
                "SELECT initialized FROM axton_v04_store WHERE singleton=1",
                &[],
            )? == Some(json!(1))
            {
                let cursor = engine.cursor04()?;
                engine.initialize_subscription(
                    &context.binding.stream,
                    subscription.subscription_id,
                    cursor,
                )?;
            }
            engine.exec(
                "axton_v04_store",
                "UPDATE axton_v04_store SET context=? WHERE singleton=1",
                &[json!(
                    String::from_utf8(v04::encode(&context)?)
                        .map_err(|_| invalid("context UTF8"))?
                )],
            )?;
            Ok(())
        })?;
        client.query_contract = format!(
            "{}:{}",
            crate::query_cache::contract_fingerprint(&client.schema)?,
            context.materialization
        );
        let contract = client.query_contract.clone();
        client.write(|engine| {
            crate::query_cache::prune(engine.store, &contract)?;
            Ok(())
        })?;
        client.context04 = Some(context);
        Ok(client)
    }
    /// Explicit lifecycle reset under the same file ownership and stable binding.
    /// All data and delivery metadata retire atomically with the old incarnation.
    pub fn reset_store04(&mut self, discard_pending: bool) -> Result<ResetStoreReport> {
        let mut context = self.request_context()?.clone();
        if self.session.is_some() {
            return Err(invalid("client transaction active"));
        }
        context.incarnation = uuid::Uuid::new_v4().to_string();
        let schema_text = schema_store::descriptor_text(&self.schema)?;
        let schema = self.schema.clone();
        let report = self.write(|engine| {
            let queue = engine.queued()?;
            if !queue.is_empty() && !discard_pending { return Err(invalid("pending work prevents Store reset")); }
            let mut abandoned_calls = vec![];
            for queued in &queue {
                if let Some(call_id) = &queued.mutation.call_id {
                    let status = engine.scalar("SELECT status FROM axton_v04_call WHERE call_id=?", &[json!(call_id)])?;
                    abandoned_calls.push(crate::AbandonedCall { call_id:call_id.clone(), frozen:queued.push.is_some() || status==Some(json!("acceptedAwaiting")) });
                }
            }
            let projection = engine.scalar("SELECT projection_generation FROM axton_v04_descriptor WHERE materialization=?", &[json!(context.materialization)])?.ok_or_else(||invalid("missing active descriptor"))?;
            engine.store.execute_batch("PRAGMA defer_foreign_keys=ON")?;
            for model in &schema.models {
                engine.exec(&model.name, &format!("DELETE FROM {}", crate::ddl::quote(&model.name)), &[])?;
            }
            let tables = engine.rows("SELECT name FROM sqlite_schema WHERE type='table' AND name GLOB 'axton_*' AND name NOT IN ('axton_client','axton_schema') ORDER BY name", &[])?.rows;
            for table in tables {
                let name = table[0].as_str().ok_or_else(|| invalid("invalid engine table"))?;
                engine.exec(name, &format!("DELETE FROM {}", crate::ddl::quote(name)), &[])?;
            }
            engine.exec("axton_client", "UPDATE axton_client SET push_models=NULL,push_results=NULL,last_completed_push=0,store_epoch=store_epoch+1", &[])?;
            engine.exec("axton_v04_store", "INSERT INTO axton_v04_store(singleton,context,cursor,initialized) VALUES(1,?,0,0)", &[json!(String::from_utf8(v04::encode(&context)?).map_err(|_|invalid("context UTF8"))?)])?;
            engine.exec("axton_v04_descriptor", "INSERT INTO axton_v04_descriptor(materialization,descriptor,projection_generation) VALUES(?,?,?)", &[json!(context.materialization),json!(schema_text),projection])?;
            engine.ensure_subscription(&context.binding.stream)?;
            engine.mark_subscription(&context.binding.stream);
            Ok(ResetStoreReport { context:context.clone(), abandoned_calls })
        })?;
        self.context04 = Some(context);
        self.replica += 1;
        Ok(report)
    }
    pub fn request_context(&self) -> Result<&v04::RequestContext> {
        self.context04
            .as_ref()
            .ok_or_else(|| invalid("protocol-4 Store binding required"))
    }
}

use crate::{
    ApplyReport, OperationKind, authority::Held, engine::Engine, mutate::apply_settled,
    rows::merge_identity,
};
use serde_json::Value;
use std::collections::BTreeMap;

impl<S: ClientStore> Engine<'_, S> {
    pub(crate) fn context04(&mut self) -> Result<Option<v04::RequestContext>> {
        if self
            .scalar(
                "SELECT name FROM sqlite_schema WHERE name='axton_v04_store' AND type='table'",
                &[],
            )?
            .is_none()
        {
            return Ok(None);
        }
        self.scalar("SELECT context FROM axton_v04_store WHERE singleton=1", &[])?
            .map(|value| {
                v04::decode(
                    value
                        .as_str()
                        .ok_or_else(|| invalid("Store context not JSON"))?
                        .as_bytes(),
                )
            })
            .transpose()
    }
    pub(crate) fn evidence04(
        &mut self,
        key: &axton_core::RecordKey,
    ) -> Result<v04::RecordEvidence> {
        let value = self.scalar(
            "SELECT evidence FROM axton_v04_record WHERE model=? AND identity=?",
            &[json!(key.model), json!(key.encoded_identity()?)],
        )?;
        value
            .map(|value| {
                v04::decode(
                    value
                        .as_str()
                        .ok_or_else(|| invalid("Record evidence not JSON"))?
                        .as_bytes(),
                )
            })
            .transpose()
            .map(|evidence| evidence.unwrap_or_default())
    }
    pub(crate) fn set_evidence04(
        &mut self,
        key: &axton_core::RecordKey,
        evidence: &v04::RecordEvidence,
    ) -> Result<()> {
        self.exec("axton_v04_record","INSERT INTO axton_v04_record(model,identity,evidence) VALUES(?,?,?) ON CONFLICT(model,identity) DO UPDATE SET evidence=excluded.evidence",&[json!(key.model),json!(key.encoded_identity()?),json!(String::from_utf8(v04::encode(evidence)?).map_err(|_|invalid("evidence UTF8"))?)])?;
        Ok(())
    }
    pub(crate) fn direct_evidence04(&mut self, key: &axton_core::RecordKey) -> Result<()> {
        if self.is05()? {
            return self.direct_evidence05(key);
        }
        if self.context04()?.is_none() {
            return Ok(());
        }
        let mut evidence = self.evidence04(key)?;
        evidence.direct_write();
        self.set_evidence04(key, &evidence)
    }
    fn normalize_snapshot04(
        &self,
        key: &axton_core::RecordKey,
        state: &Value,
    ) -> Result<Option<Value>> {
        self.schema.record_key(&key.model, &key.identity)?;
        if state.is_null() {
            Ok(None)
        } else {
            Ok(Some(merge_identity(
                &key.identity,
                &self.schema.validate_state(&key.model, state)?,
            )))
        }
    }
    fn base04(
        &mut self,
        key: &axton_core::RecordKey,
        state: Option<&Value>,
        new_generation: bool,
    ) -> Result<()> {
        self.exec("axton_v04_record","UPDATE axton_v04_record SET base=?,generation=generation+? WHERE model=? AND identity=?",&[json!(serde_json::to_string(&state)?),json!(u8::from(new_generation)),json!(key.model),json!(key.encoded_identity()?)])?;
        Ok(())
    }
    pub(crate) fn stage_preserving_local04(
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
    pub(crate) fn stage_stream04(
        &mut self,
        context: &v04::RequestContext,
        record: &v04::StreamRecord,
        held: &mut Held,
    ) -> Result<bool> {
        record.validate()?;
        let key = self
            .schema
            .record_key(&record.key.model, &record.key.identity)?;
        if key != record.key {
            return Err(invalid("noncanonical Stream identity"));
        }
        let mut evidence = self.evidence04(&key)?;
        let admission = evidence.admission(&context.materialization, record.cursor)?;
        if admission == v04::AuthorityAdmission::Duplicate {
            return Ok(false);
        }
        let incoming = self.normalize_snapshot04(&key, &record.state)?;
        evidence.install(&context.materialization, record.cursor, incoming.is_none())?;
        self.set_evidence04(&key, &evidence)?;
        self.base04(
            &key,
            incoming.as_ref(),
            admission == v04::AuthorityAdmission::Newer,
        )?;
        if admission == v04::AuthorityAdmission::Newer {
            self.stage_one(&key, incoming.as_ref(), held)?;
        } else {
            self.stage_preserving_local04(&key, incoming.as_ref(), held)?;
        }
        if incoming.is_none() {
            let children = self.descendants_where(&key, |engine, child| {
                // A later child publication already supersedes this older parent deletion.
                Ok(engine
                    .evidence04(child)?
                    .history
                    .values()
                    .copied()
                    .max()
                    .unwrap_or(0)
                    <= record.cursor)
            })?;
            for child in children {
                if admission == v04::AuthorityAdmission::Newer {
                    self.stage_one(&child, None, held)?;
                } else {
                    self.stage_preserving_local04(&child, None, held)?;
                }
                // Device cascade is null local content, not a child tombstone.
                self.direct_evidence04(&child)?;
            }
        }
        Ok(true)
    }
    pub(crate) fn stage_cache04(
        &mut self,
        records: &[v04::ReadRecord],
        store: bool,
        held: &mut Held,
    ) -> Result<usize> {
        let mut applied = 0;
        for record in records {
            record.validate()?;
            let key = self
                .schema
                .record_key(&record.key.model, &record.key.identity)?;
            if key != record.key {
                return Err(invalid("noncanonical read identity"));
            }
            let incoming = self.normalize_snapshot04(&key, &record.state)?;
            let evidence = self.evidence04(&key)?;
            let mut parents = BTreeMap::new();
            if store
                && evidence.allows_cache()
                && let Some(row) = incoming.as_ref()
            {
                for relation in &self.schema.model(&key.model)?.relations.clone() {
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
                        parents.insert(parent.encoded()?, self.evidence04(&parent)?);
                    }
                }
            }
            if record.disposition_with_relations(store, &evidence, self.schema, &parents)?
                != v04::ReadDisposition::StoreCache
            {
                continue;
            }
            self.set_evidence04(&key, &evidence)?;
            self.base04(&key, incoming.as_ref(), false)?;
            self.stage_one(&key, incoming.as_ref(), held)?;
            applied += 1;
        }
        Ok(applied)
    }
}
impl<S: ClientStore> Client<S> {
    /// Snapshot installation is shared by the actual delta/manifest paths.
    pub fn install_stream04(
        &mut self,
        context: &v04::RequestContext,
        record: &v04::StreamRecord,
    ) -> Result<ApplyReport> {
        context.admit(self.request_context()?)?;
        self.write(|engine| {
            let mut held = Held::new();
            let applied = engine.stage_stream04(context, record, &mut held)?;
            let reports = engine.rebuild_held(&held)?;
            Ok(ApplyReport {
                applied: usize::from(applied),
                reports,
                ..Default::default()
            })
        })
    }
    /// Shared commit-time ordinary cache admission. Completions are persisted
    /// by the enclosing Query/Fetch delivery in this same transaction.
    pub fn apply_cache04(
        &mut self,
        context: &v04::RequestContext,
        records: &[v04::ReadRecord],
        store: bool,
    ) -> Result<ApplyReport> {
        context.admit(self.request_context()?)?;
        self.write(|engine| {
            let mut held = Held::new();
            let applied = engine.stage_cache04(records, store, &mut held)?;
            let reports = engine.rebuild_held(&held)?;
            Ok(ApplyReport {
                applied,
                reports,
                ..Default::default()
            })
        })
    }
    pub fn record_evidence04(
        &mut self,
        key: &axton_core::RecordKey,
    ) -> Result<v04::RecordEvidence> {
        self.request_context()?;
        let key = self.schema.record_key(&key.model, &key.identity)?;
        self.view(|engine| engine.evidence04(&key))
    }
}
