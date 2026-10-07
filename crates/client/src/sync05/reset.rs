use crate::{AbandonedCall, Client, ClientStore, Result, ddl, engine::as_u64, invalid, v05};
use serde::Serialize;
use serde_json::json;
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ResetStoreReport05 {
    pub context: v05::RequestContext,
    pub abandoned_calls: Vec<AbandonedCall>,
}
impl<S: ClientStore> Client<S> {
    /// Explicitly retire all local state under the same physical-file lease.
    /// A new Store ID prevents reused Batch counters from aliasing cloud dedup.
    pub fn reset_store05(&mut self, discard_pending: bool) -> Result<ResetStoreReport05> {
        self.request_context05()?;
        if self.session.is_some() {
            return Err(invalid("client transaction active"));
        }
        let schema = self.schema.clone();
        let fresh = uuid::Uuid::new_v4().to_string();
        let report=self.write(|e| {
            let pending=e.rows("SELECT id,batch_id FROM axton_mutation_queue WHERE reconciled=0 AND rejection_code IS NULL ORDER BY id",&[])?.rows;
            if !pending.is_empty() && !discard_pending {return Err(invalid("pending work prevents Store reset"));}
            let old=e.context05()?;
            let abandoned_calls=pending.into_iter().map(|row|Ok(AbandonedCall{call_id:format!("{}:{}",old.store_id,as_u64(&row[0])?),frozen:!row[1].is_null()})).collect::<Result<Vec<_>>>()?;
            // Explicit discard first retires assignment guards; it does not
            // mutate any frozen input or pretend that its execution is known.
            e.exec("axton_store","UPDATE axton_store SET last_acknowledged_batch_id=COALESCE((SELECT MAX(batch_id) FROM axton_mutation_queue),last_acknowledged_batch_id)",&[])?;
            e.exec("axton_mutation_queue","UPDATE axton_mutation_queue SET reconciled=1",&[])?;
            e.store.execute_batch("PRAGMA defer_foreign_keys=ON")?;
            e.exec("axton_mutation_dependency","DELETE FROM axton_mutation_dependency",&[])?;
            e.exec("axton_mutation_prerequisite","DELETE FROM axton_mutation_prerequisite",&[])?;
            e.exec("axton_mutation_queue_operation","DELETE FROM axton_mutation_queue_operation",&[])?;
            e.exec("axton_mutation_queue","DELETE FROM axton_mutation_queue",&[])?;
            for model in &schema.models {
                e.exec(&model.name,&format!("DELETE FROM {}",ddl::quote(&model.name)),&[])?;
            }
            let tables=e.rows("SELECT name FROM sqlite_schema WHERE type='table' AND name GLOB 'axton_*' AND name NOT IN ('axton_store','axton_descriptor','axton_schema') ORDER BY name",&[])?.rows;
            for table in tables {
                let name=table[0].as_str().ok_or_else(||invalid("invalid engine table"))?;
                e.exec(name,&format!("DELETE FROM {}",ddl::quote(name)),&[])?;
            }
            e.exec("axton_store","UPDATE axton_store SET id=?,materialization=desired_materialization,enabled_descriptor=schema_descriptor,next_mutation_id=1,next_local_sequence=1,last_acknowledged_batch_id=0,start_cursor=NULL,bootstrap_cursor=NULL,cursor=NULL",&[json!(fresh)])?;
            Ok(ResetStoreReport05{context:e.context05()?,abandoned_calls})
        })?;
        self.client_id = report.context.store_id.clone();
        self.context05 = Some(report.context.clone());
        Ok(report)
    }
}
