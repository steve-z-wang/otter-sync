//! Canonical Loader snapshots and retained action result assembly.
use crate::actions::input_identities;
use crate::backend_interface::HostExt;
use crate::{Config, Error, Host, Result, code, internal};
use axton_core::{ActionDescriptor, ActionOutputSource, RecordKey, materialize_action_model};
use axton_protocols::server_bridge::{HostRequest, Loaded, LoaderMode};
use serde_json::{Map, Value};
use std::collections::BTreeMap;
/// Loader reads of this invocation, deduplicated by record and read version.
#[derive(Default)]
struct Reads(BTreeMap<(String, u64), Value>, bool);
impl Reads {
    /// A read that may reuse an earlier read of the same record and version.
    async fn cached(
        &mut self,
        config: &Config,
        owner: &str,
        key: &RecordKey,
        version: u64,
        host: &impl Host,
    ) -> Result<Value> {
        let slot = (key.encoded().map_err(internal)?, version);
        if let Some(state) = self.0.get(&slot) {
            return Ok(state.clone());
        }
        self.fresh(config, owner, key, version, host).await
    }
    /// A read taken now, after the preceding publication.
    async fn fresh(
        &mut self,
        config: &Config,
        owner: &str,
        key: &RecordKey,
        version: u64,
        host: &impl Host,
    ) -> Result<Value> {
        let state = load_state(config, owner, key, version, self.1, host).await?;
        self.0
            .insert((key.encoded().map_err(internal)?, version), state.clone());
        Ok(state)
    }
}

pub(crate) async fn load_state(
    config: &Config,
    owner: &str,
    key: &RecordKey,
    version: u64,
    canonical: bool,
    host: &impl Host,
) -> Result<Value> {
    let loaded: Loaded = host
        .call_typed(HostRequest::Load {
            mode: canonical.then_some(LoaderMode::Canonical),
            model: key.model.clone(),
            version,
            identities: vec![key.identity.clone()],
            owner: owner.into(),
        })
        .await?;
    match loaded {
        Loaded::Rows(rows) if rows.len() == 1 => match rows.into_iter().next().unwrap() {
            Some(row) => config
                .contract(&key.model, version)
                .ok_or_else(|| Error::code(code::MODEL_VERSION_UNSUPPORTED))?
                .normalize_state(&key.model, &row)
                .map_err(|_| Error::code(code::LOADER_INVALID)),
            None => Ok(Value::Null),
        },
        Loaded::Refused { rejection } => Err(Error::code(rejection)),
        Loaded::Failed { .. } => Err(Error::code(code::LOADER_FAILED)),
        _ => Err(Error::code(code::LOADER_INVALID)),
    }
}
/// Results use current Loader snapshots, separate from delivery authority.
fn snapshot_selection(
    config: &Config,
    action: &ActionDescriptor,
    args: &Value,
    explicit: &Map<String, Value>,
    output: &axton_core::ActionOutputDescriptor,
) -> Result<Value> {
    let selected = match &output.source {
        ActionOutputSource::Named(_) => explicit
            .get(&output.name)
            .ok_or_else(|| Error::code(code::HANDLER_INVALID))?
            .clone(),
        ActionOutputSource::InputIdentity { input_identity } => {
            let input = action
                .inputs
                .iter()
                .find(|i| i.name() == input_identity)
                .ok_or_else(|| Error::code(code::HANDLER_INVALID))?;
            let model = output
                .model
                .as_deref()
                .ok_or_else(|| Error::code(code::HANDLER_INVALID))?;
            let ids = input_identities(&config.schema, model, &args[input_identity], input)?;
            match output.cardinality.as_str() {
                "list" => Value::Array(ids),
                "optional" if ids.is_empty() => Value::Null,
                _ if ids.len() == 1 => ids[0].clone(),
                _ => return Err(Error::code(code::HANDLER_INVALID)),
            }
        }
    };
    Ok(selected)
}
pub(crate) struct SnapshotPolicy<'a> {
    pub models: &'a BTreeMap<String, u64>,
    pub canonical: bool,
    pub cache: bool,
}

pub(crate) async fn assemble_snapshots(
    config: &Config,
    owner: &str,
    action: &ActionDescriptor,
    args: &Value,
    outputs: &Value,
    policy: SnapshotPolicy<'_>,
    host: &impl Host,
) -> Result<(Value, Vec<axton_protocols::sync::ReadRecord>)> {
    use axton_protocols::sync::{ReadRecord, RecordKey as ReadKey};
    let explicit = outputs
        .as_object()
        .ok_or_else(|| Error::code(code::HANDLER_INVALID))?;
    if explicit.keys().any(|name| {
        !action
            .outputs
            .iter()
            .any(|o| o.name == *name && matches!(o.source, ActionOutputSource::Named(_)))
    }) {
        return Err(Error::code(code::HANDLER_INVALID));
    }
    if action.outputs.is_empty() {
        return Ok((Value::Null, vec![]));
    }
    let mut result = Map::new();
    let mut snapshots = BTreeMap::new();
    let mut reads = Reads(BTreeMap::new(), policy.canonical);
    for output in &action.outputs {
        let selected = snapshot_selection(config, action, args, explicit, output)?;
        if output.kind != "model" {
            result.insert(output.name.clone(), selected);
            continue;
        }
        let model = output
            .model
            .as_deref()
            .ok_or_else(|| Error::code(code::HANDLER_INVALID))?;
        let version = output
            .model_read_version
            .ok_or_else(|| Error::code(code::HANDLER_INVALID))?;
        let identities: Vec<Value> = match output.cardinality.as_str() {
            "list" => selected
                .as_array()
                .ok_or_else(|| Error::code(code::HANDLER_INVALID))?
                .clone(),
            "optional" if selected.is_null() => vec![],
            _ => vec![selected],
        };
        let mut values = vec![];
        for identity in identities {
            let key = config
                .contract(model, version)
                .ok_or_else(|| Error::code(code::MODEL_VERSION_UNSUPPORTED))?
                .record_key(model, &identity)
                .map_err(|_| Error::code(code::HANDLER_INVALID))?;
            let state = reads.cached(config, owner, &key, version, host).await?;
            let cache_version = *policy
                .models
                .get(model)
                .ok_or_else(|| Error::code(code::MODEL_VERSION_UNSUPPORTED))?;
            let cache_state = if policy.cache {
                reads
                    .cached(config, owner, &key, cache_version, host)
                    .await?
            } else {
                // Non-storing reads carry caller evidence, never a cache projection.
                state.clone()
            };
            snapshots.insert(
                key.encoded().map_err(internal)?,
                ReadRecord {
                    key: ReadKey {
                        model: key.model.clone(),
                        identity: key.identity.clone(),
                    },
                    cursor: (),
                    state: cache_state,
                },
            );
            if state.is_null() {
                if output.cardinality != "optional" {
                    return Err(Error::code(code::LOADER_INVALID));
                }
                values.push(Value::Null);
            } else {
                values.push(
                    materialize_action_model(&config.schema, model, version, &key.identity, &state)
                        .map_err(|_| Error::code(code::LOADER_INVALID))?,
                );
            }
        }
        result.insert(
            output.name.clone(),
            if output.cardinality == "list" {
                Value::Array(values)
            } else {
                values.into_iter().next().unwrap_or(Value::Null)
            },
        );
    }
    Ok((Value::Object(result), snapshots.into_values().collect()))
}

pub(crate) fn snapshot_keys(
    config: &Config,
    action: &ActionDescriptor,
    args: &Value,
    outputs: &Value,
) -> Result<Vec<RecordKey>> {
    let explicit = outputs
        .as_object()
        .ok_or_else(|| Error::code(code::HANDLER_INVALID))?;
    let mut keys = Vec::new();
    for output in action.outputs.iter().filter(|o| o.kind == "model") {
        let selected = snapshot_selection(config, action, args, explicit, output)?;
        let model = output
            .model
            .as_deref()
            .ok_or_else(|| Error::code(code::HANDLER_INVALID))?;
        let version = output
            .model_read_version
            .ok_or_else(|| Error::code(code::HANDLER_INVALID))?;
        let identities = match output.cardinality.as_str() {
            "list" => selected
                .as_array()
                .ok_or_else(|| Error::code(code::HANDLER_INVALID))?
                .clone(),
            "optional" if selected.is_null() => vec![],
            _ => vec![selected],
        };
        for identity in identities {
            keys.push(
                config
                    .contract(model, version)
                    .ok_or_else(|| Error::code(code::MODEL_VERSION_UNSUPPORTED))?
                    .record_key(model, &identity)
                    .map_err(|_| Error::code(code::HANDLER_INVALID))?,
            );
        }
    }
    Ok(keys)
}

/// Ordinary protocol-5 reads do not run Loader preparation publications.
pub(crate) async fn load_one_canonical_state(
    config: &Config,
    owner: &str,
    key: &RecordKey,
    version: u64,
    host: &impl Host,
) -> Result<Value> {
    load_state(config, owner, key, version, true, host).await
}
