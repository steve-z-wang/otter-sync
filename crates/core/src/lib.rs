//! Shared schema-driven semantics. Business model types live in generated SDKs.
mod actions;
mod fetch;
mod loads;
mod protocol;
mod schema;
#[path = "protocol_v04.rs"]
pub mod v04;
pub use actions::*;
pub use fetch::*;
pub use loads::*;
pub use protocol::*;
pub use schema::*;

pub type Result<T> = std::result::Result<T, Error>;
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    Invalid(String),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
pub fn invalid(message: impl Into<String>) -> Error {
    Error::Invalid(message.into())
}
pub fn canonical_json(value: &serde_json::Value) -> Result<String> {
    match value {
        serde_json::Value::Object(map) => {
            let mut keys: Vec<_> = map.keys().collect();
            // JavaScript Array.sort compares UTF-16 code units, not UTF-8 bytes.
            keys.sort_by(|a, b| a.encode_utf16().cmp(b.encode_utf16()));
            let entries = keys
                .into_iter()
                .map(|key| {
                    Ok(format!(
                        "{}:{}",
                        serde_json::to_string(key)?,
                        canonical_json(&map[key])?
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(format!("{{{}}}", entries.join(",")))
        }
        serde_json::Value::Array(values) => Ok(format!(
            "[{}]",
            values
                .iter()
                .map(canonical_json)
                .collect::<Result<Vec<_>>>()?
                .join(",")
        )),
        _ => Ok(serde_jcs::to_string(value)?),
    }
}
