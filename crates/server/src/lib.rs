//! Server protocol orchestration. Host calls run in the application's outer transaction.
mod action_results;
mod delivery_plan;
pub use delivery_plan::{
    handshake05, process_delivery05, process_live05, process_materialization05, process_read05,
};
mod actions;
pub mod backend_interface;
pub mod live;
mod materialization;
pub use materialization::RetainedMaterialization;
mod protocol_v05;
pub use protocol_v05::{
    encode_batch_acknowledgement, process_batch_member, settle_external05, validate_mutation_batch,
};
mod mutation_batch;
mod settlement;
use axton_core::{Schema, read_counter};
pub use axton_protocols::server_bridge::{Error, code};
use axton_protocols::server_bridge::{Handled, Head, HostRequest};
use backend_interface::HostExt;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use settlement::Changes;
use std::collections::{BTreeMap, BTreeSet};
pub type Result<T> = std::result::Result<T, Error>;
pub use backend_interface::{Host, HostResult};
#[derive(Clone, Deserialize, Serialize)]
pub struct Config {
    pub schema: Schema,
    #[serde(default)]
    pub protocol5: Option<protocol_v05::ProtocolConfig>,
    pub loaders: Vec<String>,
    /// Every retained model read contract, one per `(name, version)`. Absent
    /// in a hand-written config, in which case each model is retained at the
    /// schema's own version.
    #[serde(default)]
    pub models: Vec<ModelContract>,
}
/// One retained model read contract, as the compiler keeps it in
/// `history/models.json`: the record structure a loader of `version` returns
/// and the enums those fields use, as they were when the version was published.
#[derive(Clone, Deserialize, Serialize)]
pub struct ModelContract {
    pub name: String,
    pub version: u64,
    pub identity: Vec<String>,
    pub fields: Vec<axton_core::FieldDescriptor>,
    #[serde(default)]
    pub enums: Vec<axton_core::EnumDescriptor>,
    /// The contract as a one-model schema, for normalizing loader rows.
    #[serde(skip)]
    contract: Option<Schema>,
}
impl ModelContract {
    fn schema(&self) -> Schema {
        Schema {
            enums: self.enums.clone(),
            actions: vec![],
            loads: vec![],
            result_models: vec![],
            models: vec![axton_core::ModelDescriptor {
                name: self.name.clone(),
                bootstrap: false,
                version: self.version,
                identity: self.identity.clone(),
                fields: self.fields.clone(),
                relations: vec![],
                unique: vec![],
            }],
            requirements: vec![],
            prerequisites: vec![],
            client_policies: vec![],
        }
    }
}
impl Config {
    pub fn decode(value: Value) -> Result<Self> {
        if value.get("protocol4").is_some()
            || value
                .get("mutations")
                .and_then(Value::as_array)
                .is_some_and(|items| !items.is_empty())
        {
            return Err(config_invalid("legacy server configuration is unsupported"));
        }

        let c: Self = serde_json::from_value(value).map_err(config_invalid)?;
        c.schema.validate().map_err(config_invalid)?;
        for loader in &c.loaders {
            c.schema.model(loader).map_err(config_invalid)?;
        }
        c.refuse_device_only_on_the_wire()?;
        let mut c = c;
        if c.models.is_empty() {
            c.models = c
                .schema
                .models
                .iter()
                .map(|m| ModelContract {
                    name: m.name.clone(),
                    version: m.version,
                    identity: m.identity.clone(),
                    fields: m.fields.clone(),
                    enums: c
                        .schema
                        .enums
                        .iter()
                        .filter(|e| {
                            m.fields.iter().any(|f| {
                                matches!(&f.value_type, axton_core::ValueType::Enum { name } if *name == e.name)
                            })
                        })
                        .cloned()
                        .collect(),
                    contract: None,
                })
                .collect();
        }
        let mut retained = BTreeSet::new();
        for contract in &mut c.models {
            let current = c.schema.model(&contract.name).map_err(config_invalid)?;
            if read_counter(&json!(contract.version), true).is_err()
                || !retained.insert((contract.name.clone(), contract.version))
                || contract.identity != current.identity
            {
                return Err(Error::new(
                    code::CONFIG_INVALID,
                    format!(
                        "invalid model contract {} v{}",
                        contract.name, contract.version
                    ),
                ));
            }
            let schema = contract.schema();
            schema.validate().map_err(config_invalid)?;
            contract.contract = Some(schema);
        }
        for model in &c.schema.models {
            if !retained.contains(&(model.name.clone(), model.version)) {
                return Err(Error::new(
                    code::CONFIG_INVALID,
                    format!(
                        "model {} v{} is not a retained contract",
                        model.name, model.version
                    ),
                ));
            }
        }
        Ok(c)
    }
    /// A Model without a Loader is device-only
    /// ([#187](https://github.com/zanminwang/axton/issues/187)): it is never
    /// published, so no retained Mutation may carry it in a wire slot, and no
    /// retained Mutation, Query or Load may return it, since every Model
    /// output resolves through its Loader.
    fn refuse_device_only_on_the_wire(&self) -> Result<()> {
        let refuse = |kind: &str,
                      name: &str,
                      version: u64,
                      place: &str,
                      member: &str,
                      model: &str| {
            if self.loaders.iter().any(|loader| loader == model) {
                return Ok(());
            }
            Err(Error::new(
                code::CONFIG_INVALID,
                format!(
                    "{kind} {name} v{version} {place} {member} names Model {model}, which has no Loader; a Model without a Loader is device-only and never on the wire"
                ),
            ))
        };
        for action in &self.schema.actions {
            let kind = match action.kind {
                axton_core::CallKind::Mutation => "Mutation",
                axton_core::CallKind::Query => "Query",
            };
            for input in &action.inputs {
                if let axton_core::ActionInputDescriptor::Model { name, model, .. } = input {
                    refuse(kind, &action.name, action.version, "slot", name, model)?;
                }
            }
            for output in &action.outputs {
                if let Some(model) = &output.model {
                    refuse(
                        kind,
                        &action.name,
                        action.version,
                        "output",
                        &output.name,
                        model,
                    )?;
                }
            }
        }
        for load in &self.schema.loads {
            for output in &load.outputs {
                if let Some(model) = &output.model {
                    refuse(
                        "Load",
                        &load.name,
                        load.version,
                        "output",
                        &output.name,
                        model,
                    )?;
                }
            }
        }
        Ok(())
    }
    /// Check a client's declared read contracts: every declared model must
    /// exist and every declared version must be retained. Nothing is inferred
    /// for a model the client did not declare; a page holding one is refused
    /// by [`process_pull`] with the same code.
    pub fn check_declared(&self, models: &BTreeMap<String, u64>) -> Result<()> {
        for (name, version) in models {
            if self.schema.model(name).is_err() {
                return Err(Error::new(
                    code::MODEL_VERSION_UNSUPPORTED,
                    format!("model {name} is not served by this backend"),
                )
                .with_details(json!({"model":name,"version":version})));
            }
            if self.contract(name, *version).is_none() {
                return Err(Error::new(
                    code::MODEL_VERSION_UNSUPPORTED,
                    format!("model {name} v{version} is not a retained read contract"),
                )
                .with_details(json!({"model":name,"version":version})));
            }
        }
        Ok(())
    }
    /// The read contract a loader of `version` serves for `model`, or `None`
    /// when that version is not retained.
    pub fn contract(&self, model: &str, version: u64) -> Option<&Schema> {
        self.models
            .iter()
            .find(|m| m.name == model && m.version == version)
            .and_then(|m| m.contract.as_ref())
    }
}
fn config_invalid(e: impl std::fmt::Display) -> Error {
    Error::new(code::CONFIG_INVALID, e.to_string())
}
fn internal(e: impl std::fmt::Display) -> Error {
    Error::new(code::INTERNAL, e.to_string())
}
fn storage_invalid(e: impl std::fmt::Display) -> Error {
    Error::new(code::STORAGE_INVALID, e.to_string())
}
fn request_invalid(e: impl std::fmt::Display) -> Error {
    Error::new(code::REQUEST_INVALID, e.to_string())
}
fn principal(owner: &str) -> Result<()> {
    if owner.trim().is_empty() {
        Err(Error::new(code::PRINCIPAL_INVALID, "invalid principal"))
    } else {
        Ok(())
    }
}
async fn head(host: &impl Host, stream: &str) -> Result<u64> {
    let Head(cursor) = host
        .call_typed(HostRequest::Head {
            stream: stream.into(),
        })
        .await?;
    Ok(cursor)
}
/// Settle explicit external publication in the caller's transaction. No implicit enrollment.
pub async fn settle_external(
    config: &Config,
    settlement: &Value,
    host: &impl Host,
) -> Result<Value> {
    let settled: Handled = serde_json::from_value(settlement.clone())
        .map_err(|e| Error::new(code::PUBLISH_INVALID, e.to_string()))?;
    let Handled::Settled {
        changes,
        declarations,
    } = settled
    else {
        return Err(Error::new(
            code::PUBLISH_INVALID,
            "an external settlement carries changes and declarations",
        ));
    };
    let mut changed = Changes::new();
    for record in &changes {
        settlement::insert(&mut changed, settlement::resolve(config, record)?)?;
    }
    settlement::settle_changes(config, &changed, &declarations, host).await?;
    Ok(Value::Array(
        changed
            .values()
            .map(|key| json!({"model": key.model, "identity": key.identity}))
            .collect(),
    ))
}

/// Canonical protocol-5 read context, independent of authenticated Store binding.
pub fn materialization_id05(config: &Config, projection_generation: &str) -> Result<String> {
    axton_protocols::sync::materialization_id(&config.schema, projection_generation)
        .map_err(config_invalid)
}
