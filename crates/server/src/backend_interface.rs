//! Application host invocation under the caller's transaction context.
pub use axton_protocols::server_bridge::*;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::{future::Future, pin::Pin};
/// The host reports its own failures as text; the engine files them under the `host` code.
pub type HostResult<T> = std::result::Result<T, String>;
pub trait Host: Send + Sync {
    fn call(&self, request: Value) -> Pin<Box<dyn Future<Output = HostResult<Value>> + Send + '_>>;
}
pub trait HostExt {
    fn call_typed<'a, R: DeserializeOwned + Send + 'a>(
        &'a self,
        request: HostRequest,
    ) -> Pin<Box<dyn Future<Output = Result<R>> + Send + 'a>>;
}

impl<H: Host + ?Sized> HostExt for H {
    fn call_typed<'a, R: DeserializeOwned + Send + 'a>(
        &'a self,
        request: HostRequest,
    ) -> Pin<Box<dyn Future<Output = Result<R>> + Send + 'a>> {
        Box::pin(async move {
            let encoded = serde_json::to_value(&request)
                .map_err(|error| Error::new(code::INTERNAL, error.to_string()))?;
            let response = self.call(encoded).await?;
            request.validate_response(&response)?;
            serde_json::from_value(response).map_err(|error| request.invalid_response(error))
        })
    }
}
