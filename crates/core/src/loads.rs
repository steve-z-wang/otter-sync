//! Validation of immutable historical Load descriptors; no runtime carrier.
use crate::actions::snapshot_schema;
use crate::{
    ActionInputDescriptor, ActionInputSnapshot, ActionOutputDescriptor, EnumDescriptor,
    MAX_SAFE_INTEGER, Result, Schema, invalid, store_eligible,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
pub const RESERVED_LOAD_NAMES: &[&str] = &["get", "list", "invalidate"];
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LoadDescriptor {
    pub name: String,
    pub version: u64,
    pub inputs: Vec<ActionInputDescriptor>,
    pub outputs: Vec<ActionOutputDescriptor>,
    #[serde(default)]
    pub input: ActionInputSnapshot,
    #[serde(default)]
    pub output_enums: Vec<EnumDescriptor>,
}

impl Schema {
    pub fn load(&self, name: &str, version: u64) -> Result<&LoadDescriptor> {
        self.loads
            .iter()
            .find(|l| l.name == name && l.version == version)
            .ok_or_else(|| invalid(format!("unknown Load {name} v{version}")))
    }
    pub(crate) fn validate_loads(&self) -> Result<()> {
        let actions: BTreeSet<String> = self.actions.iter().map(|a| method(&a.name)).collect();
        let mut spellings = BTreeMap::<String, &str>::new();
        let mut seen = BTreeSet::new();
        for load in &self.loads {
            let label = format!("Load {} v{}", load.name, load.version);
            if load.name.is_empty()
                || load.version == 0
                || load.version > MAX_SAFE_INTEGER
                || !seen.insert((load.name.as_str(), load.version))
            {
                return Err(invalid("invalid or duplicate Load descriptor"));
            }
            if RESERVED_LOAD_NAMES
                .iter()
                .any(|r| load.name.eq_ignore_ascii_case(r))
            {
                return Err(invalid(format!("Load name {} is reserved", load.name)));
            }
            let name = method(&load.name);
            if actions.contains(&name)
                || spellings
                    .insert(name, &load.name)
                    .is_some_and(|other| other != load.name)
            {
                return Err(invalid(format!(
                    "Load {} shares the operation name of another operation",
                    load.name
                )));
            }
            if !load.input.models.is_empty() {
                return Err(invalid(format!("{label} cannot retain Model operands")));
            }
            let input_schema = snapshot_schema(self, Some(&load.input));
            let mut names = BTreeSet::new();
            for input in &load.inputs {
                if input.name().is_empty() || !names.insert(input.name()) {
                    return Err(invalid("invalid Load input name"));
                }
                match input {
                    ActionInputDescriptor::Value {
                        value_type,
                        nullable,
                        list,
                        ..
                    } => {
                        if *list && *nullable {
                            return Err(invalid("Load lists cannot be nullable"));
                        }
                        input_schema.validate_action_type(value_type)?;
                    }
                    ActionInputDescriptor::Model { .. } => {
                        return Err(invalid(format!("{label} cannot take a Model operand")));
                    }
                }
            }
            if load.outputs.is_empty() {
                return Err(invalid(format!("{label} declares no output")));
            }
            names.clear();
            // Records are keyed by Model, so every output of one Model reads
            // through one contract version.
            let mut reads = BTreeMap::<&str, Option<u64>>::new();
            for output in &load.outputs {
                if output.name.is_empty() || !names.insert(output.name.as_str()) {
                    return Err(invalid("invalid Load output"));
                }
                if let Some(model) = output.model.as_deref()
                    && *reads.entry(model).or_insert(output.model_read_version)
                        != output.model_read_version
                {
                    return Err(invalid(format!(
                        "{label} reads Model {model} at two contract versions"
                    )));
                }
                if output.cardinality != "list"
                    || !store_eligible(output)
                    || output.value_type.is_some()
                    || !output.metadata.is_empty()
                {
                    return Err(invalid(format!(
                        "{label} output {} must be a list of Model identities",
                        output.name
                    )));
                }
                self.validate_output(&load.inputs, &load.output_enums, output)?;
            }
        }
        Ok(())
    }
}
/// The generated method name an operation takes: its first letter lowered.
fn method(name: &str) -> String {
    let mut chars = name.chars();
    chars
        .next()
        .map(|first| first.to_ascii_lowercase().to_string() + chars.as_str())
        .unwrap_or_default()
}
