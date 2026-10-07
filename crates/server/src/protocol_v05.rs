//! Protocol-5 carrier admission and publication adapter. Envelopes belong to core.
use crate::{
    Config, Host, HostResult, Result,
    host::{HostExt, HostRequest},
    internal, request_invalid,
};
use axton_core::{ActionInputDescriptor, CallKind, v05};
use serde_json::{Value, json};
use std::{future::Future, pin::Pin};

/// Trusted carrier constructed only after protocol-5 principal/Store/Stream admission.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct HandlerContext {
    pub owner: String,
    pub stream: String,
    pub store_id: String,
    pub materialization: String,
}
pub(crate) fn handler_context(owner: &str, context: &v05::RequestContext) -> HandlerContext {
    HandlerContext {
        owner: owner.into(),
        stream: context.stream.clone(),
        store_id: context.store_id.clone(),
        materialization: context.materialization.clone(),
    }
}

pub fn validate_mutation_batch(config: &Config, bytes: &[u8]) -> Result<String> {
    let request: v05::MutationRequest = v05::decode(bytes).map_err(request_invalid)?;
    for mutation in &request.mutations {
        validate_member(config, mutation)?;
    }
    String::from_utf8(v05::encode(&request).map_err(request_invalid)?).map_err(request_invalid)
}
pub(crate) fn validate_member(config: &Config, m: &v05::Mutation) -> Result<Value> {
    let action = config
        .schema
        .action(&m.name, m.version)
        .map_err(request_invalid)?;
    if action.kind != CallKind::Mutation {
        return Err(request_invalid("expected Mutation"));
    }
    // The artifact fingerprint is immutable Batch intent, not today's schema authority.
    // Trusted retained name/version normalization permits compatible descriptor evolution.
    for op in &m.operations {
        let slot = op.input_path.split('[').next().unwrap_or_default();
        let input = action
            .inputs
            .iter()
            .find(|i| i.name() == slot)
            .ok_or_else(|| request_invalid("unknown input path"))?;
        match input {
            ActionInputDescriptor::Value { .. } => {
                if op.operation != v05::Operation::Argument {
                    return Err(request_invalid("value input must be argument"));
                }
            }
            ActionInputDescriptor::Model {
                model, operation, ..
            } => {
                if op.operation == v05::Operation::Argument {
                    if !op.value.is_null() && op.value != json!([]) {
                        return Err(request_invalid("Model argument must be null or empty list"));
                    }
                } else if op.model.as_ref() != Some(model)
                    || serde_json::to_value(&op.operation).map_err(internal)? != json!(operation)
                {
                    return Err(request_invalid("Model slot operation mismatch"));
                }
            }
        }
    }
    let args = v05::reconstruct_input(&m.operations).map_err(request_invalid)?;
    let normalized = axton_core::normalize_action_args(&config.schema, action, &args)
        .map_err(request_invalid)?;
    Ok(normalized)
}
/// Adapts existing normalization/Loader machinery to the v05 persistence seam.
/// Every nested Loader publication uses the same transaction's reservations.
pub(crate) struct Publication05<'a, H: Host>(pub &'a H, std::sync::Mutex<Option<String>>);
impl<'a, H: Host> Publication05<'a, H> {
    pub(crate) fn new(host: &'a H) -> Self {
        Self(host, std::sync::Mutex::new(None))
    }
    pub(crate) fn refusal(&self) -> Option<String> {
        self.1.lock().ok().and_then(|mut saved| saved.take())
    }
}
impl<H: Host> Host for Publication05<'_, H> {
    fn publication05(&self) -> bool {
        true
    }
    fn call(
        &self,
        mut request: Value,
    ) -> Pin<Box<dyn Future<Output = HostResult<Value>> + Send + '_>> {
        Box::pin(async move {
            let loader = request["op"] == "load";
            if matches!(
                request["op"].as_str(),
                Some("readTracking" | "readPositions" | "guardRecords" | "applyStreamMembers")
            ) {
                request = json!({"op":"protocol05","request":request});
            }
            let answer = self.0.call(request).await?;
            if loader
                && let Ok(crate::host::Loaded::Refused { rejection }) =
                    serde_json::from_value(answer.clone())
            {
                *self
                    .1
                    .lock()
                    .map_err(|_| "refusal tracker poisoned".to_string())? = Some(rejection);
            }
            Ok(answer)
        })
    }
}
pub(crate) async fn call<T: serde::de::DeserializeOwned + Send>(
    host: &impl Host,
    request: Value,
) -> Result<T> {
    host.call_typed(HostRequest::Protocol05 {
        request: serde_json::from_value(request).map_err(internal)?,
    })
    .await
}
pub async fn settle_external05(
    config: &Config,
    settlement: &Value,
    host: &impl Host,
) -> Result<Value> {
    crate::settle_external(config, settlement, &Publication05::new(host)).await
}
pub async fn process_batch_member(
    config: &Config,
    owner: &str,
    bytes: &[u8],
    ordinal: u64,
    host: &impl Host,
) -> Result<String> {
    let request: v05::MutationRequest =
        v05::decode(validate_mutation_batch(config, bytes)?.as_bytes()).map_err(request_invalid)?;
    let result = crate::mutation_batch::execute(config, owner, &request, ordinal, host).await?;
    axton_core::canonical_json(&serde_json::to_value(result).map_err(internal)?).map_err(internal)
}
pub fn encode_batch_acknowledgement(bytes: &[u8], results: &[String]) -> Result<String> {
    let request: v05::MutationRequest = v05::decode(bytes).map_err(request_invalid)?;
    let ack = v05::BatchAcknowledgement {
        context: request.context.clone(),
        batch_id: request.batch_id,
        digest: request.digest.clone(),
        results: results
            .iter()
            .map(|s| serde_json::from_str(s).map_err(crate::storage_invalid))
            .collect::<Result<_>>()?,
    };
    v05::validate_acknowledgement(&request, &ack).map_err(crate::storage_invalid)?;
    String::from_utf8(v05::encode(&ack).map_err(internal)?).map_err(internal)
}

/// Read contexts retained by the host, independent of Store/principal identity.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProtocolConfig {
    #[serde(default = "generation")]
    pub projection_generation: String,
    #[serde(default)]
    pub materializations:
        std::collections::BTreeMap<String, crate::materialization::RetainedMaterialization>,
}
fn generation() -> String {
    "1".into()
}
