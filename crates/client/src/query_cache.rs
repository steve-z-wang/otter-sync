//! Query once result snapshots ([#158](https://github.com/zanminwang/axton/issues/158)).
//!
//! A direct Query invoked with `once` persists its complete successful result
//! and later matching calls reuse that saved value instead of issuing a
//! request. The snapshot is keyed by the compiled client contract, the Query,
//! its canonical arguments and its canonical store policy. It is never
//! applied to Models: a hit returns the saved result and writes nothing.
use crate::engine::Engine;
use crate::store::ClientStore;
use crate::{ActionCallOptions, ApplyReport, Client, schema_store};
use axton_core::{
    ActionStore, CallKind, DirectActionRequest, DirectActionResponse, Result, Schema,
    canonical_json, invalid, normalize_action_args, validate_action_result,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// Invocation controls of a direct Query `once` call.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QueryOnceOptions {
    /// Which explicit Model outputs also update local Models. Part of the key.
    pub store: ActionStore,
    /// Always observe a new network outcome; replace the snapshot on success.
    pub refresh: bool,
}

/// What a host does for one `once` call.
#[derive(Clone, Debug)]
pub enum QueryOnce {
    /// A saved result: return it; no request, no Model write.
    Cached { result: Value },
    /// Wait for the host's shared outcome of an active request.
    Join { flight_id: String },
    /// Execute this exact direct request, then [`Client::finish_query_once`]
    /// or [`Client::fail_query_once`] with the flight.
    Fetch {
        flight_id: String,
        request: DirectActionRequest,
    },
}

impl PartialEq for QueryOnce {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Cached { result: a }, Self::Cached { result: b }) => a == b,
            (Self::Join { flight_id: a }, Self::Join { flight_id: b }) => a == b,
            (
                Self::Fetch {
                    flight_id: a,
                    request: x,
                },
                Self::Fetch {
                    flight_id: b,
                    request: y,
                },
            ) => a == b && x.encode().ok() == y.encode().ok(),
            _ => false,
        }
    }
}

/// One active request: memory-only, never durable work.
struct Flight {
    key: QueryCacheKey,
    /// The row generation the request saw; `None` when no row existed.
    generation: Option<String>,
    request: DirectActionRequest,
}

/// Active once requests of one open runtime, by flight ID, with a join
/// index by (key, generation) so an invalidated generation and its
/// successor can be in flight together.
#[derive(Default)]
pub(crate) struct QueryFlights {
    flights: BTreeMap<String, Flight>,
    joins: BTreeMap<(String, Option<String>), String>,
}
impl QueryFlights {
    /// Retire exactly this flight and only its own join entry.
    fn take(&mut self, flight_id: &str) -> Option<Flight> {
        let flight = self.flights.remove(flight_id)?;
        let join = (flight.key.key.clone(), flight.generation.clone());
        if self.joins.get(&join).map(String::as_str) == Some(flight_id) {
            self.joins.remove(&join);
        }
        Some(flight)
    }
}

/// Revision of the key derivation and stored result encoding. Changing it
/// makes every earlier row unreachable, like a contract change.
pub const QUERY_CACHE_FORMAT: u64 = 1;

/// The canonical identity of one saved Query result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryCacheKey {
    /// Hex SHA-256 of the canonical components below and the format.
    pub key: String,
    /// Fingerprint of the complete compiled client schema.
    pub contract: String,
    pub name: String,
    pub version: u64,
    /// Canonical JSON of the arguments after the Query's own validation.
    pub args: String,
    /// Canonical JSON of the store policy (`true`, `false` or a map).
    pub store: String,
}

/// A committed cache row. `result` is `None` for an invalidated tombstone,
/// which keeps a generation so older responses cannot repopulate it.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryCacheEntry {
    pub generation: String,
    pub result: Option<Value>,
}

pub(crate) fn sha256_hex(text: &str) -> String {
    Sha256::digest(text.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// The contract fingerprint of a compiled client schema: any change to it,
/// including Query outputs and Model read contracts, is a new partition.
pub fn contract_fingerprint(schema: &Schema) -> Result<String> {
    Ok(sha256_hex(&format!(
        "{QUERY_CACHE_FORMAT}:{}",
        schema_store::descriptor_text(schema)?
    )))
}

pub(crate) fn derive_key(
    schema: &Schema,
    contract: &str,
    name: &str,
    version: u64,
    args: &Value,
    store: &ActionStore,
    protocol04: bool,
) -> Result<QueryCacheKey> {
    let action = schema.action(name, version)?;
    if action.kind != CallKind::Query {
        return Err(invalid(format!(
            "once is only available for direct Queries; {name} v{version} is a Mutation"
        )));
    }
    let args = canonical_json(&normalize_action_args(schema, action, args)?)?;
    store.validate(action)?;
    // Omitted, `true` and all-true maps share the default; a map disabling
    // every eligible output is the same policy as `false`; and with no
    // eligible output every policy stores the same nothing.
    let mut eligible = action
        .outputs
        .iter()
        .filter(|output| axton_core::store_eligible(output))
        .peekable();
    let store = if protocol04 {
        match store {
            ActionStore::All | ActionStore::None => store.clone(),
            _ => return Err(invalid("Query store must be a boolean")),
        }
    } else {
        match store.clone().canonical() {
            _ if eligible.peek().is_none() => ActionStore::All,
            ActionStore::Outputs(map)
                if eligible.all(|output| map.get(&output.name) == Some(&false)) =>
            {
                ActionStore::None
            }
            other => other,
        }
    };
    let store = canonical_json(&store.wire().unwrap_or(json!(true)))?;
    let key = sha256_hex(&canonical_json(&json!({
        "format": QUERY_CACHE_FORMAT,
        "contract": contract,
        "name": name,
        "version": version,
        "args": args,
        "store": store,
    }))?);
    Ok(QueryCacheKey {
        key,
        contract: contract.to_string(),
        name: name.to_string(),
        version,
        args,
        store,
    })
}

/// Remove rows written under any other contract. Runs inside the open
/// transaction, so repeated schema changes never accumulate snapshots.
pub(crate) fn prune<S: ClientStore>(store: &mut S, contract: &str) -> Result<()> {
    store.execute(
        "DELETE FROM axton_query_cache WHERE contract <> ?",
        &[json!(contract)],
    )?;
    Ok(())
}

impl<S: ClientStore> Engine<'_, S> {
    /// The committed row for `key`: `Ok(None)` when there is none,
    /// `Ok(Some(Err(_)))` when its content cannot be decoded. SQL failures
    /// are errors.
    fn query_cache_row(&mut self, key: &QueryCacheKey) -> Result<Option<Result<QueryCacheEntry>>> {
        let rows = self.rows(
            "SELECT generation, result FROM axton_query_cache WHERE key = ?",
            &[json!(key.key)],
        )?;
        let Some(row) = rows.rows.into_iter().next() else {
            return Ok(None);
        };
        Ok(Some((|| {
            let generation = row[0]
                .as_str()
                .ok_or_else(|| invalid("query cache generation is not text"))?
                .to_string();
            let result = match &row[1] {
                Value::Null => None,
                Value::String(text) => Some(serde_json::from_str(text)?),
                _ => return Err(invalid("query cache result is not text")),
            };
            Ok(QueryCacheEntry { generation, result })
        })()))
    }
    pub(crate) fn query_cache_entry(
        &mut self,
        key: &QueryCacheKey,
    ) -> Result<Option<QueryCacheEntry>> {
        self.query_cache_row(key)?.transpose()
    }
    /// Save `result` when the row still has the generation the request saw
    /// (`None`: no row existed). Returns whether it was saved; a newer
    /// generation means the key was invalidated after the request began.
    pub(crate) fn save_query_result(
        &mut self,
        key: &QueryCacheKey,
        generation: Option<&str>,
        result: &Value,
    ) -> Result<bool> {
        let current = self.query_cache_entry(key)?.map(|entry| entry.generation);
        if current.as_deref() != generation {
            return Ok(false);
        }
        let text = canonical_json(result)?;
        match generation {
            Some(generation) => {
                self.exec(
                    "axton_query_cache",
                    "UPDATE axton_query_cache SET result = ? WHERE key = ? AND generation = ?",
                    &[json!(text), json!(key.key), json!(generation)],
                )?;
            }
            None => {
                self.exec(
                    "axton_query_cache",
                    "INSERT INTO axton_query_cache (key, contract, name, version, args, store, generation, result) VALUES (?,?,?,?,?,?,?,?)",
                    &[
                        json!(key.key),
                        json!(key.contract),
                        json!(key.name),
                        json!(key.version),
                        json!(key.args),
                        json!(key.store),
                        json!(uuid::Uuid::new_v4().to_string()),
                        json!(text),
                    ],
                )?;
            }
        }
        Ok(true)
    }
    /// Discard one row's result and change its generation.
    pub(crate) fn invalidate_query_key(&mut self, key: &QueryCacheKey) -> Result<()> {
        self.exec(
            "axton_query_cache",
            "UPDATE axton_query_cache SET result = NULL, generation = ? WHERE key = ?",
            &[json!(uuid::Uuid::new_v4().to_string()), json!(key.key)],
        )?;
        Ok(())
    }
    /// Clear the result and change the generation of every store variant of
    /// one Query argument set, and leave a fresh tombstone for each `extra`
    /// key (active requests with no row yet) so none of them can repopulate
    /// it. Returns nothing: the commit is the outcome.
    pub(crate) fn invalidate_query_results(
        &mut self,
        contract: &str,
        name: &str,
        version: u64,
        args: &str,
        extra: &[QueryCacheKey],
    ) -> Result<()> {
        let rows = self.rows(
            "SELECT key FROM axton_query_cache WHERE contract = ? AND name = ? AND version = ? AND args = ?",
            &[json!(contract), json!(name), json!(version), json!(args)],
        )?;
        for row in rows.rows {
            self.exec(
                "axton_query_cache",
                "UPDATE axton_query_cache SET result = NULL, generation = ? WHERE key = ?",
                &[json!(uuid::Uuid::new_v4().to_string()), row[0].clone()],
            )?;
        }
        for key in extra {
            self.exec(
                "axton_query_cache",
                "INSERT INTO axton_query_cache (key, contract, name, version, args, store, generation, result) VALUES (?,?,?,?,?,?,?,NULL) ON CONFLICT(key) DO NOTHING",
                &[
                    json!(key.key),
                    json!(key.contract),
                    json!(key.name),
                    json!(key.version),
                    json!(key.args),
                    json!(key.store),
                    json!(uuid::Uuid::new_v4().to_string()),
                ],
            )?;
        }
        Ok(())
    }
}

impl<S: ClientStore> Client<S> {
    /// The fingerprint partitioning this client's saved Query results.
    pub fn query_cache_contract(&self) -> &str {
        &self.query_contract
    }
    /// The canonical cache key of a direct Query call. Fails for a Mutation,
    /// unknown Query, invalid arguments or an invalid store policy, before
    /// any cache access.
    pub fn query_cache_key(
        &self,
        name: &str,
        version: u64,
        args: &Value,
        store: &ActionStore,
    ) -> Result<QueryCacheKey> {
        derive_key(
            &self.schema,
            &self.query_contract,
            name,
            version,
            args,
            store,
            self.context04.is_some(),
        )
    }
    /// The committed row for `key`, if any.
    pub fn query_cache_entry(&mut self, key: &QueryCacheKey) -> Result<Option<QueryCacheEntry>> {
        if self.session_active() {
            return Err(invalid("client transaction active"));
        }
        self.view(|engine| engine.query_cache_entry(key))
    }
    /// Validate `result` under the Query's current output contract and save
    /// it in its own local transaction, fenced by `generation` (see
    /// [`Engine::save_query_result`]). Writes no Model.
    pub fn save_query_result(
        &mut self,
        key: &QueryCacheKey,
        generation: Option<&str>,
        result: &Value,
    ) -> Result<bool> {
        let action = self.schema.action(&key.name, key.version)?;
        let result = validate_action_result(&self.schema, action, result)?;
        self.write(|engine| engine.save_query_result(key, generation, &result))
    }
    /// Invalidate every store variant of one Query argument set in the
    /// current contract, in one local transaction. Needs no network. Active
    /// requests of the older generation still complete for their callers
    /// but can no longer save their result.
    pub fn invalidate_query_once(&mut self, name: &str, version: u64, args: &Value) -> Result<()> {
        if self.session_active() {
            return Err(invalid("client transaction active"));
        }
        let key = self.query_cache_key(name, version, args, &ActionStore::All)?;
        // Requests that began with no row get a tombstone too, so the
        // generation they saw (none) no longer matches.
        let unrowed: Vec<QueryCacheKey> = self
            .query_flights
            .flights
            .values()
            .filter(|flight| {
                flight.generation.is_none()
                    && flight.key.name == key.name
                    && flight.key.version == key.version
                    && flight.key.args == key.args
            })
            .map(|flight| flight.key.clone())
            .collect();
        self.write(|engine| {
            engine.invalidate_query_results(
                &key.contract,
                &key.name,
                key.version,
                &key.args,
                &unrowed,
            )
        })
    }
    /// Decide one `once` call: a valid saved result unless `refresh`, else
    /// join the active request of the same key and generation, else start a
    /// new one. Validation, the Query-only rule and the application
    /// transaction guard apply before any cache access; a hit needs no
    /// network. Database read failures are errors, never misses.
    pub fn begin_query_once(
        &mut self,
        name: &str,
        version: u64,
        args: &Value,
        options: &QueryOnceOptions,
    ) -> Result<QueryOnce> {
        if self.session_active() {
            return Err(invalid("client transaction active"));
        }
        let key = self.query_cache_key(name, version, args, &options.store)?;
        let action = self.schema.action(name, version)?.clone();
        // A row that does not decode, or a snapshot the current output
        // contract refuses, is discarded and this call misses.
        let usable = match self.view(|engine| engine.query_cache_row(&key))? {
            None => Ok(None),
            Some(Err(_)) => Err(()),
            Some(Ok(entry)) => match &entry.result {
                None => Ok(Some(entry)),
                Some(result) => match validate_action_result(&self.schema, &action, result) {
                    Ok(result) if !options.refresh => return Ok(QueryOnce::Cached { result }),
                    Ok(_) => Ok(Some(entry)),
                    Err(_) => Err(()),
                },
            },
        };
        let entry = match usable {
            Ok(entry) => entry,
            Err(()) => {
                self.write(|engine| engine.invalidate_query_key(&key))?;
                self.view(|engine| engine.query_cache_entry(&key))?
            }
        };
        let generation = entry.map(|entry| entry.generation);
        let join = (key.key.clone(), generation.clone());
        if let Some(flight_id) = self.query_flights.joins.get(&join) {
            return Ok(QueryOnce::Join {
                flight_id: flight_id.clone(),
            });
        }
        let request = self.prepare_action_with_options(
            name,
            version,
            args.clone(),
            ActionCallOptions {
                store: options.store.clone(),
            },
        )?;
        let flight_id = uuid::Uuid::new_v4().to_string();
        self.query_flights.joins.insert(join, flight_id.clone());
        self.query_flights.flights.insert(
            flight_id.clone(),
            Flight {
                key,
                generation,
                request: request.clone(),
            },
        );
        Ok(QueryOnce::Fetch { flight_id, request })
    }
    /// Complete a Fetch with the direct response to its exact request. The
    /// flight is retired on every path. Authority is applied under the
    /// existing stamp rules and, when the call succeeded and the key still
    /// has the generation the request saw, the result is saved in the same
    /// local transaction; nothing is exposed before that commit.
    pub fn finish_query_once(&mut self, flight_id: &str, response: &[u8]) -> Result<ApplyReport> {
        let flight = self
            .query_flights
            .take(flight_id)
            .ok_or_else(|| invalid("unknown query once flight"))?;
        let report = DirectActionResponse::decode(response, &flight.request, &self.schema)
            .and_then(|response| {
                self.apply_direct_response(
                    response,
                    Some((&flight.key, flight.generation.as_deref())),
                )
            });
        self.retire_request(&flight.request.call.call_id);
        report
    }
    /// Decode a flight's received response without retiring it. A store hook
    /// may still roll back, so the flight remains owned until local commit.
    pub(crate) fn decode_query_once_store(
        &self,
        flight_id: &str,
        response: &[u8],
    ) -> Result<crate::StoreDelivery> {
        let flight = self
            .query_flights
            .flights
            .get(flight_id)
            .ok_or_else(|| invalid("unknown query once flight"))?;
        let response = DirectActionResponse::decode(response, &flight.request, &self.schema)?;
        Ok(crate::StoreDelivery::Direct {
            response,
            snapshot: Some((flight.key.clone(), flight.generation.clone())),
        })
    }
    /// Release a Fetch whose request produced no applicable response
    /// (transport failure, close, cancellation). Returns whether it was
    /// active; an older flight never releases a newer one.
    pub fn fail_query_once(&mut self, flight_id: &str) -> bool {
        if let Some(flight) = self.query_flights.take(flight_id) {
            self.retire_request(&flight.request.call.call_id);
            true
        } else {
            false
        }
    }
}
impl<S: ClientStore> Client<S> {
    pub(crate) fn finish_query_once04(
        &mut self,
        flight_id: &str,
        request: &axton_core::v04::ReadIntent,
        response: &axton_core::v04::ReadResponse,
    ) -> Result<ApplyReport> {
        let flight = self
            .query_flights
            .take(flight_id)
            .ok_or_else(|| invalid("unknown query once flight"))?;
        self.apply_query04(
            request,
            response,
            Some((&flight.key, flight.generation.as_deref())),
        )
    }
}
