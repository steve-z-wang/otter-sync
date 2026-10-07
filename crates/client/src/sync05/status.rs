use crate::{
    Client, ClientStore, Result,
    unsent::{RefusedAct, SubmittedAct},
    v05,
};
use serde_json::json;
impl<S: ClientStore> Client<S> {
    pub fn refused_acts05(&mut self) -> Result<Vec<RefusedAct>> {
        self.view(|e|{
   let rows=e.rows("SELECT id,name,descriptor_version,rejection_code FROM axton_mutation_queue WHERE rejection_code IS NOT NULL ORDER BY id",&[])?.rows;
   rows.into_iter().map(|row|{
    let id=crate::engine::as_u64(&row[0])?;let ops=e.wire_ops05(id)?;let args=v05::reconstruct_input(&ops)?;
    let operations=ops.into_iter().filter_map(|op|{
     let kind=match op.operation {v05::Operation::Create=>crate::OperationKind::Create,v05::Operation::Update=>crate::OperationKind::Update,v05::Operation::Delete=>crate::OperationKind::Delete,v05::Operation::Argument=>return None};
     Some(crate::Operation{model:op.model?,identity:op.identity,op:kind,values:if op.value.is_null(){None}else{Some(op.value)}})
    }).collect();
    Ok(RefusedAct{id,name:row[1].as_str().unwrap().into(),version:crate::engine::as_u64(&row[2])?,code:row[3].as_str().unwrap().into(),act:SubmittedAct{args:Some(args),operations}})
   }).collect()
  })
    }
    pub fn status_snapshot05(&mut self) -> Result<serde_json::Value> {
        let status = self.store_status05()?;
        let pending = self.pending_schema05()?.is_some();
        Ok(
            json!({"clientId":status.context.store_id,"context":status.context,"pending":self.pending_count()?,"beforeImages":self.before_image_count()?,"cursors":{status.context.stream.clone():status.cursor},"streams":[status.context.stream],"rejections":self.refused_acts05()?,"schema":{"rebuilt":false,"pending":pending}}),
        )
    }
}
