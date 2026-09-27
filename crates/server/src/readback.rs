//! Mandatory caller authority for one mutation's input targets: its loaders
//! read the targets back inside the same savepoint at the client's declared
//! versions, at the stamps settlement already allocated. Extra changed
//! records are not read here; they are no caller's authority
//! ([Server / Push](../../../docs/engineering/architecture/server/engine/push.md)).
use crate::host::{HostExt, HostRequest, Loaded};
use crate::settlement::{Changes, unregistered};
use crate::{Config, Host, Result, code, internal};
use axton_core::AuthorityRecord;
use serde_json::Value;
use std::collections::BTreeMap;

/// What one mutation's readback produced: its authority, or the code that
/// refused it. A refusal rolls the mutation back; nothing else in the batch
/// is affected.
pub(crate) enum Outcome {
    Records(Vec<AuthorityRecord>),
    Refused(String),
}

/// Read `targets` back at the client's declared versions, each at the stamp
/// settlement allocated for it in `stamps`. Allocates no stamp.
pub(crate) async fn read_back(
    config: &Config,
    declared: &BTreeMap<String, u64>,
    owner: &str,
    targets: &Changes,
    stamps: &BTreeMap<String, u64>,
    host: &impl Host,
) -> Result<Outcome> {
    // Every target model must be one the client declared a read contract
    // for; otherwise the response could not be applied. A missing declaration
    // is a read refusal of this mutation, never a fallback to another version.
    let mut versions: BTreeMap<&str, u64> = BTreeMap::new();
    for key in targets.values() {
        if !config.loaders.contains(&key.model) {
            return Err(unregistered());
        }
        let Some(version) = declared.get(&key.model) else {
            return Ok(Outcome::Refused(code::MODEL_VERSION_UNSUPPORTED.into()));
        };
        if config.contract(&key.model, *version).is_none() {
            return Ok(Outcome::Refused(code::MODEL_VERSION_UNSUPPORTED.into()));
        }
        versions.insert(&key.model, *version);
    }
    // Loaders read the targets grouped by model, at the declared version.
    let mut groups: BTreeMap<&str, Vec<&String>> = BTreeMap::new();
    for (encoded, key) in targets {
        groups.entry(&key.model).or_default().push(encoded);
    }
    let mut records = vec![];
    for (model, encoded_keys) in groups {
        let version = versions[model];
        let contract = config
            .contract(model, version)
            .ok_or_else(|| internal(format!("model {model} v{version} is not retained")))?;
        let identities: Vec<Value> = encoded_keys
            .iter()
            .map(|k| targets[*k].identity.clone())
            .collect();
        let loaded: Loaded = host
            .call_typed(HostRequest::Load {
                model: model.to_string(),
                version,
                identities,
                owner: owner.into(),
            })
            .await?;
        let rows = match loaded {
            Loaded::Refused { rejection } => return Ok(Outcome::Refused(rejection)),
            Loaded::Rows(rows) => rows,
            // A thrown loader error rejects only this mutation; it never
            // aborts the rest of the batch.
            Loaded::Failed { .. } => return Ok(Outcome::Refused(code::LOADER_FAILED.into())),
        };
        // An answer the served contract cannot accept is this mutation's
        // content problem: it rejects only this mutation.
        if rows.len() != encoded_keys.len() {
            return Ok(Outcome::Refused(code::LOADER_INVALID.into()));
        }
        for (encoded, state) in encoded_keys.iter().zip(rows) {
            let key = &targets[*encoded];
            let state = match state {
                None => Value::Null,
                Some(state) => match contract.normalize_state(model, &state) {
                    Ok(state) => state,
                    Err(_) => return Ok(Outcome::Refused(code::LOADER_INVALID.into())),
                },
            };
            let stamp = *stamps
                .get(*encoded)
                .ok_or_else(|| internal(format!("input target {encoded} was not settled")))?;
            records.push(AuthorityRecord {
                model: key.model.clone(),
                identity: key.identity.clone(),
                stamp,
                state,
                error: None,
            });
        }
    }
    Ok(Outcome::Records(records))
}
