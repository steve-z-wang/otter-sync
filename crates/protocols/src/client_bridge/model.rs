use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum OperationKind {
    Create,
    Update,
    Delete,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Operation {
    pub model: String,
    pub op: OperationKind,
    pub identity: Value,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub values: Option<Value>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Readiness {
    Pending,
    Ready,
    Failed,
}
// Why one record or one queued mutation could not be applied as delivered.
// Every kind leaves the client consistent; the report is for the application
// ([#51](https://github.com/zanminwang/axton/issues/51),
// [#122](https://github.com/zanminwang/axton/issues/122)).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ReportKind {
    // A queued operation no longer replays over the new base: the base is
    // visible and the mutation is still sent (`ordinal`).
    Diverged,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Report {
    pub kind: ReportKind,
    pub model: String,
    pub identity: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ordinal: Option<u64>,
    #[serde(default)]
    pub detail: Value,
}
impl Report {
    pub fn new(kind: ReportKind, model: &str, identity: &Value) -> Self {
        Self {
            kind,
            model: model.to_string(),
            identity: identity.clone(),
            code: None,
            ordinal: None,
            detail: Value::Null,
        }
    }
}
