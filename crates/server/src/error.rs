//! The structured error that crosses the server's language boundary.
//!
//! `code` is the stable machine name a transport maps to a status; `message`
//! is for people and may change; `details` carries the fields a code promises
//! (when a code needs structured context). Rewording a message must
//! never change how a caller classifies the error.
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Codes the HTTP transport maps to a client-visible status. Every other code
/// is a server-side failure the transport reports as `500 {code: "server"}`.
pub mod code {
    /// The request body could not be decoded, or names a cursor ahead of the Stream head.
    pub const REQUEST_INVALID: &str = "request.invalid";
    /// A named Mutation version is not retained: a durable per-member refusal,
    /// never a whole-Batch transport failure.
    pub const MUTATION_VERSION_UNSUPPORTED: &str = "mutation_version_unsupported";
    /// A model read contract this backend does not serve: the client declared
    /// a model or version that is not retained, or a page holds a model the
    /// client did not declare. In a push this rejects only the mutation that
    /// touched the model; a page still fails whole (per-read isolation for
    /// pages is [#95](https://github.com/zanminwang/axton/issues/95)).
    pub const MODEL_VERSION_UNSUPPORTED: &str = "model_version_unsupported";
    /// The owner is blank.
    pub const PRINCIPAL_INVALID: &str = "principal.invalid";
    /// The backend configuration is invalid.
    pub const CONFIG_INVALID: &str = "config.invalid";
    /// The application's host callback failed; the message is the host's own.
    pub const HOST: &str = "host";
    /// The host callback returned a value the protocol cannot use.
    pub const HOST_INVALID: &str = "host.invalid";
    /// Persisted sync metadata is inconsistent with the request.
    pub const STORAGE_INVALID: &str = "storage.invalid";
    /// A handler settled a mutation with an invalid rejection code or checkpoint.
    pub const HANDLER_INVALID: &str = "handler.invalid";
    /// The handler threw an error that is not a business rejection; the error
    /// reached `onError`. Aborts the acceptance transaction.
    pub const HANDLER_FAILED: &str = "handler.failed";
    /// A loader threw an error that is not a business rejection; the error
    /// reached `onError`. Aborts the acceptance transaction.
    pub const LOADER_FAILED: &str = "loader.failed";
    /// A loader is not registered for the model.
    pub const LOADER_UNREGISTERED: &str = "loader.unregistered";
    /// A loader returned rows the schema cannot accept.
    pub const LOADER_INVALID: &str = "loader.invalid";
    /// A publish request is malformed.
    pub const PUBLISH_INVALID: &str = "publish.invalid";
    /// A live page does not continue the subscription it was produced for.
    pub const LIVE_INVALID_PAGE: &str = "live.invalid_page";
    /// The host drove a live session with an event it cannot accept: an unknown
    /// Stream, a pull it was not asked for, or a session handle that is not open.
    pub const LIVE_INVALID_EVENT: &str = "live.invalid_event";
    /// A serialization conflict, deadlock or changed holder set: the whole
    /// application transaction must retry with a fresh snapshot.
    pub const TRANSACTION_CONFLICT: &str = "transaction.conflict";
    /// Encoding a response failed.
    pub const INTERNAL: &str = "internal";
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Error {
    pub code: String,
    pub message: String,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub details: Value,
}

impl Error {
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            details: Value::Null,
        }
    }
    /// An error whose code is its whole meaning, such as `gap` or a rejection code.
    pub fn code(code: impl Into<String>) -> Self {
        let code = code.into();
        Self {
            message: code.clone(),
            code,
            details: Value::Null,
        }
    }
    pub fn with_details(mut self, details: Value) -> Self {
        self.details = details;
        self
    }
    /// A failure reported by the application's host callback.
    pub fn host(message: impl Into<String>) -> Self {
        Self::new(code::HOST, message)
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.message.is_empty() || self.message == self.code {
            write!(f, "{}", self.code)
        } else {
            write!(f, "{}: {}", self.code, self.message)
        }
    }
}

impl std::error::Error for Error {}

/// Host callbacks report failures as plain text; the engine files them under `host`.
impl From<String> for Error {
    fn from(message: String) -> Self {
        Self::host(message)
    }
}
