//! Frozen local admission evidence, independent of content stamps and wire metadata.
use crate::engine::{Engine, as_u64};
use crate::{Client, ClientStore};
use axton_core::{RecordKey, Result};
use serde_json::json;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StoreToken {
    pub epoch: u64,
}
impl<S: ClientStore> Client<S> {
    pub(crate) fn freeze_request(&self, call_id: &str) {
        self.request_tokens
            .borrow_mut()
            .entry(call_id.into())
            .or_insert(self.store_epoch);
    }
    /// Release a transient call after completion/cancellation. Durable work
    /// stores its own token; prepared deliveries retain an owned copy.
    #[doc(hidden)]
    pub fn retire_request(&self, call_id: &str) {
        self.request_tokens.borrow_mut().remove(call_id);
    }
    /// Unknown/historical calls are old work, never implicitly fresh reads.
    pub(crate) fn request_token(&self, call_id: &str) -> StoreToken {
        self.request_tokens
            .borrow()
            .get(call_id)
            .copied()
            .unwrap_or_default()
    }
}
impl<S: ClientStore> Engine<'_, S> {
    pub fn store_token(&mut self) -> Result<StoreToken> {
        Ok(StoreToken {
            epoch: as_u64(
                &self
                    .scalar("SELECT store_epoch FROM axton_client", &[])?
                    .unwrap_or(json!(0)),
            )?,
        })
    }
    pub(crate) fn admit_positive_body(
        &mut self,
        key: &RecordKey,
        token: StoreToken,
    ) -> Result<bool> {
        let evicted = self
            .scalar(
                "SELECT evicted_at FROM axton_record WHERE model=? AND identity=?",
                &[json!(key.model), json!(key.encoded_identity()?)],
            )?
            .unwrap_or(json!(0));
        Ok(token.epoch >= as_u64(&evicted)?)
    }
}
