//! Protocol-5 file state. Store and queue tables are the durable lifecycle truth.
use crate::{
    Client, ClientStore, Result, Schema, ddl,
    engine::{Engine, as_u64},
    invalid,
};
use axton_core::canonical_json;
use axton_protocols::sync as v05;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

pub(crate) const DDL: &str = r#"
CREATE TABLE axton_store(singleton INTEGER PRIMARY KEY CHECK(singleton=1), format INTEGER NOT NULL CHECK(format=5), id TEXT NOT NULL, stream TEXT NOT NULL, materialization TEXT NOT NULL, desired_materialization TEXT NOT NULL, schema_descriptor TEXT NOT NULL, enabled_descriptor TEXT NOT NULL, next_mutation_id INTEGER NOT NULL DEFAULT 1, next_local_sequence INTEGER NOT NULL DEFAULT 1, last_acknowledged_batch_id INTEGER NOT NULL DEFAULT 0, start_cursor INTEGER, bootstrap_cursor INTEGER, cursor INTEGER, generation INTEGER NOT NULL DEFAULT 1);
CREATE TABLE axton_descriptor(context TEXT PRIMARY KEY, materialization TEXT NOT NULL, descriptor TEXT NOT NULL, projection_generation TEXT NOT NULL);
CREATE TABLE axton_mutation_queue(id INTEGER PRIMARY KEY, name TEXT NOT NULL, descriptor_version INTEGER NOT NULL, descriptor TEXT NOT NULL REFERENCES axton_descriptor(context), batch_id INTEGER, batch_materialization TEXT, batch_digest TEXT, sync_cursor INTEGER, result TEXT, targets TEXT, rejection_code TEXT, rejection_message TEXT, rejection_acknowledged INTEGER NOT NULL DEFAULT 0, reconciled INTEGER NOT NULL DEFAULT 0, diverged INTEGER NOT NULL DEFAULT 0);
CREATE TABLE axton_mutation_queue_operation(mutation_id INTEGER NOT NULL REFERENCES axton_mutation_queue(id) ON DELETE CASCADE, step INTEGER NOT NULL, local_sequence INTEGER NOT NULL UNIQUE, input_path TEXT, kind TEXT NOT NULL, model TEXT, identity TEXT, operation TEXT NOT NULL, value TEXT, owner_history TEXT NOT NULL, PRIMARY KEY(mutation_id,step));
CREATE TABLE axton_mutation_dependency(ordinal INTEGER NOT NULL REFERENCES axton_mutation_queue(id) ON DELETE CASCADE, depends_on INTEGER NOT NULL REFERENCES axton_mutation_queue(id), kind TEXT NOT NULL, PRIMARY KEY(ordinal,depends_on), CHECK(depends_on<ordinal));
CREATE TABLE axton_mutation_prerequisite(ordinal INTEGER NOT NULL REFERENCES axton_mutation_queue(id) ON DELETE CASCADE,key TEXT NOT NULL,error TEXT,PRIMARY KEY(ordinal,key));
CREATE TABLE axton_local_write(sequence INTEGER PRIMARY KEY, ordinal INTEGER NOT NULL,position INTEGER,disposition TEXT NOT NULL,model TEXT NOT NULL,identity TEXT NOT NULL,op TEXT NOT NULL,"values" TEXT);
CREATE INDEX axton_local_write_record ON axton_local_write(model,identity,sequence);
CREATE INDEX axton_queue_operation_record ON axton_mutation_queue_operation(model,identity,mutation_id,step);

CREATE TABLE axton_local_replica_layer(model TEXT NOT NULL,identity TEXT NOT NULL,operations TEXT NOT NULL,PRIMARY KEY(model,identity));
CREATE TABLE axton_authority(model TEXT NOT NULL,identity TEXT NOT NULL,evidence TEXT NOT NULL,base TEXT NOT NULL DEFAULT 'null',PRIMARY KEY(model,identity));
CREATE TRIGGER axton_queue_frozen_update BEFORE UPDATE OF name,descriptor_version,descriptor,batch_id ON axton_mutation_queue WHEN OLD.batch_id IS NOT NULL AND (NEW.name<>OLD.name OR NEW.descriptor_version<>OLD.descriptor_version OR NEW.descriptor<>OLD.descriptor OR NEW.batch_id IS NOT OLD.batch_id) BEGIN SELECT RAISE(ABORT,'assigned mutation is immutable'); END;
CREATE TRIGGER axton_operation_frozen_update BEFORE UPDATE ON axton_mutation_queue_operation WHEN OLD.input_path IS NOT NULL AND EXISTS(SELECT 1 FROM axton_mutation_queue WHERE id=OLD.mutation_id AND batch_id IS NOT NULL) BEGIN SELECT RAISE(ABORT,'assigned input is immutable'); END;
CREATE TRIGGER axton_operation_frozen_insert BEFORE INSERT ON axton_mutation_queue_operation WHEN NEW.input_path IS NOT NULL AND EXISTS(SELECT 1 FROM axton_mutation_queue WHERE id=NEW.mutation_id AND batch_id IS NOT NULL) BEGIN SELECT RAISE(ABORT,'assigned input is immutable'); END;
CREATE TRIGGER axton_queue_frozen_delete BEFORE DELETE ON axton_mutation_queue WHEN OLD.batch_id>(SELECT last_acknowledged_batch_id FROM axton_store) BEGIN SELECT RAISE(ABORT,'possibly sent Mutation cannot be cancelled'); END;
CREATE TRIGGER axton_operation_frozen_delete BEFORE DELETE ON axton_mutation_queue_operation WHEN OLD.input_path IS NOT NULL AND EXISTS(SELECT 1 FROM axton_mutation_queue WHERE id=OLD.mutation_id AND batch_id IS NOT NULL AND reconciled=0) BEGIN SELECT RAISE(ABORT,'assigned input is immutable'); END;
CREATE TRIGGER axton_queue_assignment BEFORE UPDATE OF batch_id ON axton_mutation_queue WHEN OLD.batch_id IS NULL AND NEW.batch_id IS NOT NULL AND (NEW.batch_id<>(SELECT last_acknowledged_batch_id+1 FROM axton_store) OR EXISTS(SELECT 1 FROM axton_mutation_queue WHERE batch_id>(SELECT last_acknowledged_batch_id FROM axton_store) AND (batch_id<>NEW.batch_id OR batch_digest IS NOT NULL))) BEGIN SELECT RAISE(ABORT,'Batch membership is fixed'); END;
CREATE TRIGGER axton_queue_envelope BEFORE UPDATE OF batch_materialization,batch_digest ON axton_mutation_queue WHEN OLD.batch_digest IS NOT NULL AND (NEW.batch_digest IS NOT OLD.batch_digest OR NEW.batch_materialization IS NOT OLD.batch_materialization) BEGIN SELECT RAISE(ABORT,'Batch envelope is immutable'); END;
"#;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StoreStatus05 {
    pub context: v05::RequestContext,
    pub next_mutation_id: u64,
    pub next_local_sequence: u64,
    pub last_acknowledged_batch_id: u64,
    pub start_cursor: Option<u64>,
    pub bootstrap_cursor: Option<u64>,
    pub cursor: Option<u64>,
}
pub fn admit05<S: ClientStore>(store: &mut S, stream: &str) -> Result<bool> {
    axton_core::check_stream(stream)?;
    let names = store
        .query(
            "SELECT name FROM sqlite_schema WHERE type='table' AND name LIKE 'axton_%'",
            &[],
        )?
        .rows;
    if names.is_empty() {
        return Ok(false);
    }
    if !names.iter().any(|r| r[0] == "axton_store") {
        return Err(invalid("unsupported Store format"));
    }
    let rows = store
        .query("SELECT format,stream FROM axton_store", &[])?
        .rows;
    if rows.len() != 1 || rows[0][0] != 5 {
        return Err(invalid("unsupported Store format"));
    }
    if rows[0][1] != stream {
        return Err(invalid("Stream mismatch"));
    }
    let row=store.query("SELECT id,stream,materialization,schema_descriptor,next_mutation_id,next_local_sequence,last_acknowledged_batch_id,start_cursor,bootstrap_cursor,cursor,generation FROM axton_store",&[])?.rows;
    let r = &row[0];
    let context = v05::RequestContext {
        protocol: 5,
        store_id: r[0]
            .as_str()
            .ok_or_else(|| invalid("Store ID missing"))?
            .into(),
        stream: r[1].as_str().unwrap().into(),
        materialization: r[2]
            .as_str()
            .ok_or_else(|| invalid("materialization missing"))?
            .into(),
    };
    axton_protocols::sync::Validate::validate(&context)?;
    for value in &r[4..] {
        if !value.is_null() {
            axton_core::counter(as_u64(value)?)?;
        }
    }
    let descriptor = store
        .query(
            "SELECT descriptor FROM axton_descriptor WHERE context=?",
            &[r[3].clone()],
        )?
        .rows;
    if descriptor.len() != 1 {
        return Err(invalid("retained Store descriptor missing"));
    }
    for sql in [
        "SELECT id,name,descriptor_version,descriptor,batch_id,sync_cursor,result,targets,rejection_code,rejection_message,rejection_acknowledged,reconciled FROM axton_mutation_queue LIMIT 0",
        "SELECT mutation_id,step,local_sequence,input_path,model,identity,operation,value,owner_history FROM axton_mutation_queue_operation LIMIT 0",
        "SELECT sequence,ordinal,position,disposition,model,identity,op,\"values\" FROM axton_local_write LIMIT 0",
        "SELECT model,identity,evidence,base FROM axton_authority LIMIT 0",
        "SELECT desired_materialization,enabled_descriptor FROM axton_store LIMIT 0",
    ] {
        store.query(sql, &[])?;
    }
    Ok(true)
}
impl<S: ClientStore> Client<S> {
    pub fn open05(store: S, schema: Schema, stream: &str) -> Result<Self> {
        Self::open05_with_projection(store, schema, stream, "1")
    }
    pub fn open05_with_projection(
        mut store: S,
        schema: Schema,
        stream: &str,
        projection: &str,
    ) -> Result<Self> {
        schema.validate()?;
        let existing = admit05(&mut store, stream)?;
        let materialization = axton_protocols::sync::materialization_id(&schema, projection)?;
        let descriptor_text = canonical_json(&serde_json::to_value(&schema)?)?;
        let mut hash = Sha256::new();
        hash.update(b"axton:stored-descriptor:5\0");
        hash.update(descriptor_text.as_bytes());
        hash.update([0]);
        hash.update(projection.as_bytes());
        let descriptor_context = format!("{:x}", hash.finalize());
        if existing {
            let old=store.query("SELECT descriptor FROM axton_descriptor WHERE context=(SELECT schema_descriptor FROM axton_store)",&[])?.rows;
            let old: Schema = serde_json::from_str(
                old.first()
                    .and_then(|r| r.first())
                    .ok_or_else(|| invalid("Store descriptor missing"))?
                    .as_str()
                    .ok_or_else(|| invalid("descriptor missing"))?,
            )?;
            let mut comparison = old.clone();
            for m in &mut comparison.models {
                if let Some(new) = schema.models.iter().find(|n| n.name == m.name) {
                    if new.version < m.version {
                        return Err(invalid("Model version regressed"));
                    }
                    m.version = new.version
                }
            }
            if let axton_core::Compatibility::Incompatible(reason) =
                Schema::compatibility(&comparison, &schema)
            {
                return Err(invalid(reason));
            }
        }
        store.begin()?;
        let opened = (|| {
            if !existing {
                store.execute_batch(DDL)?;
                store.execute("INSERT INTO axton_store(singleton,format,id,stream,materialization,desired_materialization,schema_descriptor,enabled_descriptor) VALUES(1,5,?,?,?,?,?,?)",&[json!(uuid::Uuid::new_v4().to_string()),json!(stream),json!(materialization),json!(materialization),json!(descriptor_context),json!(descriptor_context)])?;
            }
            ddl::reconcile(&mut store, &schema)?;
            store.execute("INSERT OR IGNORE INTO axton_descriptor(context,materialization,descriptor,projection_generation) VALUES(?,?,?,?)",&[json!(descriptor_context),json!(materialization),json!(descriptor_text),json!(projection)])?;
            store.execute(
                "UPDATE axton_store SET desired_materialization=?,schema_descriptor=?,enabled_descriptor=CASE WHEN materialization=? THEN ? ELSE enabled_descriptor END",
                &[json!(materialization),json!(descriptor_context),json!(materialization),json!(descriptor_context)],
            )?;
            let rows = store
                .query("SELECT id,generation,materialization FROM axton_store", &[])?
                .rows;
            Ok::<_, axton_core::Error>((
                rows[0][0].as_str().unwrap().to_owned(),
                as_u64(&rows[0][1])?,
                rows[0][2].as_str().unwrap().to_owned(),
            ))
        })();
        let (client_id, generation, enabled_materialization) = match opened {
            Ok(v) => v,
            Err(e) => {
                store.rollback()?;
                return Err(e);
            }
        };
        if let Err(e) = store.commit() {
            let _ = store.rollback();
            return Err(e);
        }
        let context05 = Some(v05::RequestContext {
            protocol: 5,
            store_id: client_id.clone(),
            stream: stream.into(),
            materialization: enabled_materialization,
        });
        Ok(Self {
            store,
            context05,
            schema,
            client_id,
            generation,
            watchers: vec![],
            watcher_ids: 0,
            session: None,
            last_changed: BTreeSet::new(),
            last_bootstrap: BTreeSet::new(),
        })
    }
    pub fn request_context05(&mut self) -> Result<v05::RequestContext> {
        if self.context05.is_none() {
            return Err(invalid("protocol05 Store required"));
        }
        self.view(|e| e.context05())
    }
    pub fn store_status05(&mut self) -> Result<StoreStatus05> {
        let context = self.request_context05()?;
        self.view(|e|{let r=e.rows("SELECT next_mutation_id,next_local_sequence,last_acknowledged_batch_id,start_cursor,bootstrap_cursor,cursor FROM axton_store",&[])?.rows.remove(0);Ok(StoreStatus05{context,next_mutation_id:as_u64(&r[0])?,next_local_sequence:as_u64(&r[1])?,last_acknowledged_batch_id:as_u64(&r[2])?,start_cursor:r[3].as_u64(),bootstrap_cursor:r[4].as_u64(),cursor:r[5].as_u64()})})
    }
}
impl<S: ClientStore> Engine<'_, S> {
    pub fn context05(&mut self) -> Result<v05::RequestContext> {
        let r = self
            .rows("SELECT id,stream,materialization FROM axton_store", &[])?
            .rows
            .remove(0);
        Ok(v05::RequestContext {
            protocol: 5,
            store_id: r[0].as_str().unwrap().into(),
            stream: r[1].as_str().unwrap().into(),
            materialization: r[2].as_str().unwrap().into(),
        })
    }
    pub(crate) fn allocate05(&mut self, column: &str) -> Result<u64> {
        let n = as_u64(
            &self
                .scalar(&format!("SELECT {column} FROM axton_store"), &[])?
                .ok_or_else(|| invalid("Store missing"))?,
        )?;
        let next = n
            .checked_add(1)
            .filter(|v| *v <= axton_core::MAX_SAFE_INTEGER)
            .ok_or_else(|| invalid("counter exhausted"))?;
        self.exec(
            "axton_store",
            &format!("UPDATE axton_store SET {column}=?"),
            &[json!(next)],
        )?;
        Ok(n)
    }
}
#[derive(Clone, Debug)]
pub struct PendingSchema05 {
    pub previous_context: v05::RequestContext,
    pub desired_context: v05::RequestContext,
    pub authority_keys: Vec<v05::RecordKey>,
    pub bootstrap_models: std::collections::BTreeMap<String, u64>,
}
impl<S: ClientStore> Engine<'_, S> {
    pub fn pending_schema05(&mut self) -> Result<Option<PendingSchema05>> {
        let previous_context = self.context05()?;
        let row=self.rows("SELECT desired_materialization,enabled_descriptor,schema_descriptor FROM axton_store",&[])?.rows.remove(0);
        if row[0] == previous_context.materialization {
            return Ok(None);
        }
        let mut desired_context = previous_context.clone();
        desired_context.materialization = row[0]
            .as_str()
            .ok_or_else(|| invalid("desired context missing"))?
            .into();
        let previous_schema = self.retained_schema05(row[1].as_str().unwrap())?;
        let desired_schema = self.retained_schema05(row[2].as_str().unwrap())?;
        let bootstrap_models = desired_schema
            .models
            .iter()
            .filter(|m| {
                m.bootstrap
                    && previous_schema
                        .models
                        .iter()
                        .find(|old| old.name == m.name)
                        .is_none_or(|old| !old.bootstrap)
            })
            .map(|m| (m.name.clone(), m.version))
            .collect();
        let rows = self
            .rows(
                "SELECT model,identity,evidence FROM axton_authority ORDER BY model,identity",
                &[],
            )?
            .rows;
        let mut authority_keys = vec![];
        for row in rows {
            let evidence: axton_core::authority::RecordEvidence =
                crate::mutation_queue::decode(&row[2])?;
            if !evidence.history.is_empty() && evidence.membership.is_some_and(|m| m.live) {
                authority_keys.push(v05::RecordKey {
                    model: row[0].as_str().unwrap().into(),
                    identity: crate::mutation_queue::decode(&row[1])?,
                });
            }
        }
        Ok(Some(PendingSchema05 {
            previous_context,
            desired_context,
            authority_keys,
            bootstrap_models,
        }))
    }
    /// Task 5 calls this only in the final, validated complete owned transfer
    /// transaction. Pair admission prevents an obsolete plan enabling a schema.
    pub fn enable_schema05(
        &mut self,
        previous: &v05::RequestContext,
        desired: &v05::RequestContext,
    ) -> Result<()> {
        let pending = self
            .pending_schema05()?
            .ok_or_else(|| invalid("no pending schema transfer"))?;
        if &pending.previous_context != previous || &pending.desired_context != desired {
            return Err(invalid("schema transfer context mismatch"));
        }
        self.exec("axton_store","UPDATE axton_store SET materialization=desired_materialization,enabled_descriptor=schema_descriptor",&[])?;
        Ok(())
    }
}
impl<S: ClientStore> Client<S> {
    pub fn pending_schema05(&mut self) -> Result<Option<PendingSchema05>> {
        self.request_context05()?;
        self.view(|e| e.pending_schema05())
    }
}
impl<S: ClientStore> crate::ClientTransaction<'_, S> {
    pub fn enable_schema05(
        &mut self,
        previous: &v05::RequestContext,
        desired: &v05::RequestContext,
    ) -> Result<()> {
        self.savepoint(|tx| tx.engine.enable_schema05(previous, desired))
    }
}
