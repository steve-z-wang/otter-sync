//! Retained authenticated read contracts, independent of wire carriers.
use crate::{Config, Error, Result, request_invalid};
use serde::{Deserialize, Serialize};
#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RetainedMaterialization {
    pub schema: axton_core::Schema,
    #[serde(default = "default_projection")]
    pub projection_generation: String,
}
fn default_projection() -> String {
    "1".into()
}
pub(crate) fn model_versions(
    config: &Config,
    materialization: &str,
    active: &str,
    retained_contexts: &std::collections::BTreeMap<String, RetainedMaterialization>,
    identity: fn(&axton_core::Schema, &str) -> axton_core::Result<String>,
) -> Result<std::collections::BTreeMap<String, u64>> {
    if materialization == active {
        return Ok(config
            .schema
            .models
            .iter()
            .map(|m| (m.name.clone(), m.version))
            .collect());
    }
    let retained = retained_contexts
        .get(materialization)
        .ok_or_else(|| Error::code("context_mismatch"))?;
    if identity(&retained.schema, &retained.projection_generation).map_err(request_invalid)?
        != materialization
    {
        return Err(Error::code("context_mismatch"));
    }
    let mut models = std::collections::BTreeMap::new();
    for model in &retained.schema.models {
        let served = config
            .contract(&model.name, model.version)
            .ok_or_else(|| Error::code("context_mismatch"))?;
        let mut expected = retained.schema.clone();
        expected.models = vec![model.clone()];
        let mut actual = served.clone();
        actual.models[0].bootstrap = model.bootstrap;
        if identity(&expected, "read-contract-check").map_err(request_invalid)?
            != identity(&actual, "read-contract-check").map_err(request_invalid)?
        {
            return Err(Error::code("context_mismatch"));
        }
        models.insert(model.name.clone(), model.version);
    }
    Ok(models)
}
