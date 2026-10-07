use crate::{Client, ClientStore, Result, unsent::RefusedAct};
use serde_json::json;
impl<S: ClientStore> Client<S> {
    pub fn refused_acts05(&mut self) -> Result<Vec<RefusedAct>> {
        self.view(|e| e.refused_acts())
    }
    pub fn status_snapshot05(&mut self) -> Result<serde_json::Value> {
        let status = self.store_status05()?;
        let pending = self.pending_schema05()?.is_some();
        Ok(json!({
            "clientId": status.context.store_id,
            "context": status.context,
            "pending": self.pending_count()?,
            "beforeImages": self.before_image_count()?,
            "cursors": {status.context.stream.clone(): status.cursor},
            "streams": [status.context.stream],
            "rejections": self.refused_acts05()?,
            "schema": {"rebuilt": false, "pending": pending}
        }))
    }
}
