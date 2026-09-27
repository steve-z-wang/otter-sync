//! Action result assembly and output-driven authority. Every Model output is
//! returned as this invocation's Loader snapshot at its retained result read
//! version. Additional authority is the positive union of identities chosen
//! by the outputs the invocation's `store` policy enables; authority required
//! by mutation inputs arrives already read back and is never subtracted. An
//! extra changed record is caller authority only when an enabled output
//! selects it, and then at the stamp settlement already allocated.
use crate::actions::input_identities;
use crate::host::{HostExt, HostRequest, Loaded, Stamped};
use crate::{Config, Error, Host, Result, code, internal};
use axton_core::{
    ActionDescriptor, ActionOutputSource, ActionStore, AuthorityRecord, RecordKey,
    materialize_action_model, store_eligible,
};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

pub(crate) struct ResultReadback<'a> {
    /// Required authority read back for mutation input targets.
    pub records: &'a [AuthorityRecord],
    /// The stamp settlement allocated to each changed record, keyed canonically.
    pub stamps: &'a BTreeMap<String, u64>,
    /// The client's declared authority read versions.
    pub models: &'a BTreeMap<String, u64>,
    /// The validated per-invocation storage policy.
    pub store: &'a ActionStore,
}

/// Loader reads of this invocation, deduplicated by record and read version.
#[derive(Default)]
struct Reads(BTreeMap<(String, u64), Value>);
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
    /// A read taken now, after any stamp evidence it must follow.
    async fn fresh(
        &mut self,
        config: &Config,
        owner: &str,
        key: &RecordKey,
        version: u64,
        host: &impl Host,
    ) -> Result<Value> {
        let state = load_one_state(config, owner, key, version, host).await?;
        self.0
            .insert((key.encoded().map_err(internal)?, version), state.clone());
        Ok(state)
    }
}

async fn load_one_state(
    config: &Config,
    owner: &str,
    key: &RecordKey,
    version: u64,
    host: &impl Host,
) -> Result<Value> {
    let loaded: Loaded = host
        .call_typed(HostRequest::Load {
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

/// Assemble the named result and the additional authority its enabled
/// outputs contribute beyond `readback.records`.
pub(crate) async fn assemble_result(
    config: &Config,
    owner: &str,
    action: &ActionDescriptor,
    args: &Value,
    outputs: &Value,
    readback: ResultReadback<'_>,
    host: &impl Host,
) -> Result<(Value, Vec<AuthorityRecord>)> {
    let explicit = outputs
        .as_object()
        .ok_or_else(|| Error::code(code::HANDLER_INVALID))?;
    if action.outputs.is_empty() && explicit.is_empty() {
        return Ok((Value::Null, vec![]));
    }
    if explicit.keys().any(|name| {
        !action.outputs.iter().any(|output| {
            output.name == *name && matches!(output.source, ActionOutputSource::Named(_))
        })
    }) {
        return Err(Error::code(code::HANDLER_INVALID));
    }
    let ResultReadback {
        records,
        stamps,
        models,
        store,
    } = readback;
    let mut result = Map::new();
    let mut additional: BTreeMap<String, AuthorityRecord> = BTreeMap::new();
    let mut reads = Reads::default();
    for output in &action.outputs {
        let selected = match &output.source {
            ActionOutputSource::Named(_) => explicit
                .get(&output.name)
                .ok_or_else(|| Error::code(code::HANDLER_INVALID))?
                .clone(),
            ActionOutputSource::InputIdentity { input_identity } => {
                let input = action
                    .inputs
                    .iter()
                    .find(|input| input.name() == input_identity)
                    .ok_or_else(|| Error::code(code::HANDLER_INVALID))?;
                let model = output
                    .model
                    .as_deref()
                    .ok_or_else(|| Error::code(code::HANDLER_INVALID))?;
                let identities =
                    input_identities(&config.schema, model, &args[input_identity], input)?;
                match output.cardinality.as_str() {
                    "list" => Value::Array(identities),
                    "optional" if identities.is_empty() => Value::Null,
                    _ if identities.len() == 1 => identities[0].clone(),
                    _ => return Err(Error::code(code::HANDLER_INVALID)),
                }
            }
        };
        if output.kind == "model" {
            let model = output
                .model
                .as_deref()
                .ok_or_else(|| Error::code(code::HANDLER_INVALID))?;
            let version = output
                .model_read_version
                .ok_or_else(|| Error::code(code::HANDLER_INVALID))?;
            // The read contract stays required even when storage is off.
            let authority_version = *models
                .get(model)
                .ok_or_else(|| Error::code(code::MODEL_VERSION_UNSUPPORTED))?;
            if config.contract(model, authority_version).is_none() {
                return Err(Error::code(code::MODEL_VERSION_UNSUPPORTED));
            }
            // Input-bound outputs are required authority already; only
            // explicit outputs follow the invocation's policy.
            let enabled = !store_eligible(output) || store.selects(&output.name);
            let one = |identity: &Value| -> Result<RecordKey> {
                config
                    .schema
                    .record_key(model, identity)
                    .map_err(|_| Error::code(code::HANDLER_INVALID))
            };
            let identities: Vec<RecordKey> = match output.cardinality.as_str() {
                "list" => selected
                    .as_array()
                    .ok_or_else(|| Error::code(code::HANDLER_INVALID))?
                    .iter()
                    .map(one)
                    .collect::<Result<_>>()?,
                "optional" if selected.is_null() => vec![],
                _ => vec![one(&selected)?],
            };
            let mut values = vec![];
            for key in &identities {
                let changed = records
                    .iter()
                    .find(|record| record.model == key.model && record.identity == key.identity);
                let encoded = key.encoded().map_err(internal)?;
                let adds = enabled && changed.is_none() && !additional.contains_key(&encoded);
                // Stamp evidence for new authority precedes reading its content.
                // A record this settlement changed already has its one stamp.
                let stamp = if adds && let Some(stamp) = stamps.get(&encoded) {
                    Some(*stamp)
                } else if adds {
                    let Stamped(stamp) = host
                        .call_typed(HostRequest::EnsureStamp {
                            model: model.into(),
                            identity_key: key.encoded_identity().map_err(internal)?,
                        })
                        .await?;
                    Some(stamp)
                } else {
                    None
                };
                let state = if let Some(record) = changed.filter(|_| authority_version == version) {
                    record.state.clone()
                } else if let Some(record) = additional
                    .get(&encoded)
                    .filter(|_| authority_version == version)
                {
                    record.state.clone()
                } else if adds && authority_version == version {
                    reads.fresh(config, owner, key, version, host).await?
                } else {
                    reads.cached(config, owner, key, version, host).await?
                };
                if let Some(stamp) = stamp {
                    let authority_state = if authority_version == version {
                        state.clone()
                    } else {
                        reads
                            .fresh(config, owner, key, authority_version, host)
                            .await?
                    };
                    additional.insert(
                        encoded,
                        AuthorityRecord {
                            model: model.into(),
                            identity: key.identity.clone(),
                            stamp,
                            state: authority_state,
                            error: None,
                        },
                    );
                }
                if state.is_null() {
                    if output.cardinality != "optional"
                        || matches!(&output.source, ActionOutputSource::InputIdentity { .. })
                    {
                        return Err(Error::code(code::LOADER_INVALID));
                    }
                    values.push(Value::Null);
                } else {
                    values.push(
                        materialize_action_model(
                            &config.schema,
                            model,
                            version,
                            &key.identity,
                            &state,
                        )
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
        } else {
            if output.kind == "deleteIdentity" {
                let model = output
                    .model
                    .as_deref()
                    .ok_or_else(|| Error::code(code::HANDLER_INVALID))?;
                let identities: Vec<&Value> = if output.cardinality == "list" {
                    selected
                        .as_array()
                        .ok_or_else(|| Error::code(code::HANDLER_INVALID))?
                        .iter()
                        .collect()
                } else if selected.is_null() {
                    vec![]
                } else {
                    vec![&selected]
                };
                for identity in identities {
                    if !records.iter().any(|record| {
                        record.model == model
                            && record.identity == *identity
                            && record.state.is_null()
                    }) {
                        return Err(Error::code(code::LOADER_INVALID));
                    }
                }
            }
            result.insert(output.name.clone(), selected);
        }
    }
    Ok((Value::Object(result), additional.into_values().collect()))
}
