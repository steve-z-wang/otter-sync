//! Bounded loading of a Scope's historical interval, and the record
//! resolution both pull modes share.
//!
//! A bootstrap request walks `(after, until]` of one channel with the same
//! cursor-ordered `scan` and the same grouped Loader reads the ordinary delta
//! pull uses, in the caller's transaction. The upper bound is the
//! subscription's origin S, fixed for the whole walk, so the walk terminates
//! under sustained publication: a record republished above S leaves the
//! historical interval and belongs to that subscription's own delivery
//! ([Server / Engine / Pull](../../../docs/engineering/architecture/server/engine/pull.md)).
use crate::{
    Config, Error, Host, Result, code, head,
    host::{HostExt, HostRequest, Invalidation, Loaded},
    internal, request_invalid, storage_invalid,
};
use axton_core::{AuthorityRecord, BootstrapPage, BootstrapRequest, RecordKey, limits};
use serde_json::{Value, json};
use std::collections::{BTreeMap, btree_map::Entry};

/// Serve one bounded page of a Scope's historical interval. The head is read
/// once, in this transaction: it bounds the scan, is echoed as the client's
/// completion barrier, and an origin above it is a fault rather than a bound
/// to chase. An exhausted interval scans nothing.
pub(crate) async fn process_bootstrap(
    config: &Config,
    owner: &str,
    bytes: &[u8],
    host: &impl Host,
) -> Result<String> {
    let request = BootstrapRequest::decode(bytes).map_err(request_invalid)?;
    config.check_declared(&request.models)?;
    let channel = request.channel.as_str();
    let maximum = head(host, channel).await?;
    // The client only ever sends an origin the server acknowledged, so a head
    // below it is a server-state fault, not a bound to wait for.
    if request.until > maximum {
        return Err(request_invalid(format!(
            "bootstrap origin ahead of head on {channel}"
        )));
    }
    // Every historical record once, keyed canonically, at its current stamp.
    let mut historical: BTreeMap<String, (RecordKey, u64)> = BTreeMap::new();
    let mut to = request.until;
    if !request.exhausted() {
        let rows = legacy_rows(config, channel, request.after, maximum, host).await?;
        if rows.len() > limits::PULL_CHANGES {
            return Err(storage_invalid("invalid scan size"));
        }
        let mut previous = request.after;
        let mut last_historical = request.after;
        for row in &rows {
            let key = validate_row(config, channel, maximum, previous, row)?;
            previous = row.cursor;
            // Only a position at or below the origin is historical; a record
            // published above it is the subscription's to deliver.
            if row.cursor > request.until {
                continue;
            }
            last_historical = row.cursor;
            if row.kind == crate::channel_members::PositionKind::Upsert {
                insert(&mut historical, key, row.stamp)?;
            }
        }
        // The interval is finished when the scan ran out of rows or reached
        // the origin; otherwise the page stops at its last historical cursor.
        // The scan answers only rows whose record is still a member, filtered
        // before the limit, so removed positions are holes: a short scan has
        // covered every eligible row up to the head, and a full one advances
        // to a cursor it actually returned.
        to = if rows.len() < limits::PULL_CHANGES || previous >= request.until {
            request.until
        } else {
            last_historical
        };
        // Defensive, and unreachable while the scan honours its contract: with
        // strictly increasing cursors a full scan that reached no historical row
        // has already crossed the origin and is therefore terminal. Reported
        // rather than assumed, because an empty nonterminal page would make the
        // client ask for the same interval forever.
        if to != request.until && to <= request.after {
            return Err(storage_invalid("bootstrap page makes no progress"));
        }
    }
    let records = resolve_records(
        config,
        owner,
        &request.models,
        historical.into_values().collect(),
        host,
    )
    .await?;
    let page = BootstrapPage {
        channel: request.channel,
        from: request.after,
        to,
        until: request.until,
        head: maximum,
        records,
    };
    String::from_utf8(page.encode().map_err(internal)?).map_err(internal)
}
/// Validate one scan row against the rules both pull modes apply, and answer
/// its canonical record key: the row belongs to the scanned channel, its cursor
/// advances past `previous` without passing the channel head, its model has a
/// registered loader, and its stored identity key is the canonical encoding of
/// its identity. The caller decides what to do with the row; this decides
/// whether the row is usable at all.
pub(crate) fn validate_row(
    config: &Config,
    channel: &str,
    maximum: u64,
    previous: u64,
    row: &Invalidation,
) -> Result<RecordKey> {
    if row.channel != channel || row.cursor <= previous || row.cursor > maximum {
        return Err(storage_invalid("invalid invalidation order"));
    }
    if row.kind == crate::channel_members::PositionKind::Upsert && row.stamp == 0 {
        return Err(storage_invalid("upsert stamp missing"));
    }
    if !config.loaders.contains(&row.model) {
        return Err(crate::settlement::unregistered(&row.model));
    }
    let key = config
        .schema
        .record_key(&row.model, &row.identity)
        .map_err(storage_invalid)?;
    if row.identity_key != key.encoded_identity().map_err(storage_invalid)? {
        return Err(storage_invalid("noncanonical identity"));
    }
    Ok(key)
}
/// Keep one entry per record, keyed canonically, at the highest stamp seen for
/// it: a record published to two scanned channels is one entry at its current
/// stamp, whichever mode collected it.
pub(crate) fn insert(
    records: &mut BTreeMap<String, (RecordKey, u64)>,
    key: RecordKey,
    stamp: u64,
) -> Result<()> {
    match records.entry(key.encoded().map_err(internal)?) {
        Entry::Vacant(slot) => {
            slot.insert((key, stamp));
        }
        Entry::Occupied(mut slot) => {
            let entry = slot.get_mut();
            entry.1 = entry.1.max(stamp);
        }
    }
    Ok(())
}
/// Resolve the authority of a page's records: group them by model, call each
/// declared version's Loader once with all its identities, normalize the rows
/// against that retained read contract, and answer receipt-shaped records in
/// canonical record order. Both pull modes resolve here, so a page is the
/// same authority whichever mode delivered it.
///
/// A record whose read fails fails alone: a batched call that cannot say which
/// record failed is retried one identity at a time, and each failing identity
/// becomes an `error` record carrying the refusal code or `loader.failed`. A
/// `null` row is a deletion. A model the client did not declare is not in its
/// read contract and refuses the page.
pub(crate) async fn resolve_records(
    config: &Config,
    owner: &str,
    models: &BTreeMap<String, u64>,
    records: Vec<(RecordKey, u64)>,
    host: &impl Host,
) -> Result<Vec<AuthorityRecord>> {
    let records = {
        let mut canonical: BTreeMap<String, (RecordKey, u64)> = BTreeMap::new();
        for (key, stamp) in records {
            insert(&mut canonical, key, stamp)?;
        }
        canonical
    };
    // Loaders read every changed record grouped by model, at the declared
    // version; a model the client did not declare is not in its read contract.
    let mut groups: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (encoded, (key, _)) in &records {
        groups
            .entry(key.model.clone())
            .or_default()
            .push(encoded.clone());
    }
    let mut states: BTreeMap<String, std::result::Result<Value, String>> = BTreeMap::new();
    for (model, encoded_keys) in groups {
        let version = *models.get(&model).ok_or_else(|| {
            Error::new(
                code::MODEL_VERSION_UNSUPPORTED,
                format!("model {model} is not declared by the client"),
            )
            .with_details(json!({"model":model}))
        })?;
        let contract = config
            .contract(&model, version)
            .ok_or_else(|| internal(format!("model {model} v{version} is not retained")))?;
        let identities: Vec<Value> = encoded_keys
            .iter()
            .map(|k| records[k].0.identity.clone())
            .collect();
        let loaded: Loaded = host
            .call_typed(HostRequest::Load {
                model: model.clone(),
                version,
                identities: identities.clone(),
                owner: owner.into(),
            })
            .await?;
        // A record whose read fails in any way fails alone: a refusal or a
        // thrown error answered as data, a row the served contract does not
        // accept, or a batched answer that cannot be matched to its records.
        let normalize = |state: Option<Value>| -> std::result::Result<Value, String> {
            match state {
                None => Ok(Value::Null),
                Some(state) => contract
                    .normalize_state(&model, &state)
                    .map_err(|_| code::LOADER_INVALID.to_string()),
            }
        };
        let outcome: Vec<std::result::Result<Value, String>> = match loaded {
            Loaded::Rows(rows) if rows.len() == encoded_keys.len() => {
                rows.into_iter().map(normalize).collect()
            }
            Loaded::Rows(_) if encoded_keys.len() == 1 => {
                vec![Err(code::LOADER_INVALID.to_string())]
            }
            refused @ (Loaded::Refused { .. } | Loaded::Failed { .. })
                if encoded_keys.len() == 1 =>
            {
                vec![Err(refusal_code(refused))]
            }
            _ => {
                // One call for many records could not say which record failed:
                // ask for each on its own so the others still get their rows.
                let mut each = Vec::with_capacity(encoded_keys.len());
                for identity in &identities {
                    let one: Loaded = host
                        .call_typed(HostRequest::Load {
                            model: model.clone(),
                            version,
                            identities: vec![identity.clone()],
                            owner: owner.into(),
                        })
                        .await?;
                    each.push(match one {
                        Loaded::Rows(mut rows) if rows.len() == 1 => normalize(rows.remove(0)),
                        Loaded::Rows(_) => Err(code::LOADER_INVALID.to_string()),
                        refused => Err(refusal_code(refused)),
                    });
                }
                each
            }
        };
        for (encoded, state) in encoded_keys.iter().zip(outcome) {
            states.insert(encoded.clone(), state);
        }
    }
    // Canonical record order: the key order of the map the identities were
    // grouped from, whichever mode collected them.
    Ok(records
        .into_iter()
        .map(|(encoded, (key, stamp))| {
            let (state, error) = match states.remove(&encoded) {
                Some(Ok(state)) => (state, None),
                Some(Err(code)) => (Value::Null, Some(code)),
                None => (Value::Null, None),
            };
            AuthorityRecord {
                model: key.model,
                identity: key.identity,
                stamp,
                state,
                error,
            }
        })
        .collect())
}
/// The code a refused or failed load contributes to a record's `error`.
fn refusal_code(loaded: Loaded) -> String {
    match loaded {
        Loaded::Refused { rejection } => rejection,
        Loaded::Failed { .. } | Loaded::Rows(_) => code::LOADER_FAILED.into(),
    }
}

/// Scan a bounded interval, including tombstones. A one-row probe identifies
/// a terminal full page even when compaction leaves a gap before the bound.
async fn channel_rows(
    config: &Config,
    channel: &str,
    after: u64,
    bound: u64,
    maximum: u64,
    host: &impl Host,
) -> Result<(Vec<Invalidation>, u64)> {
    if after == bound {
        return Ok((vec![], bound));
    }
    let rows: Vec<Invalidation> = host
        .call_typed(HostRequest::Scan {
            channel: channel.into(),
            after,
            limit: limits::PULL_CHANGES as u64,
        })
        .await?;
    if rows.len() > limits::PULL_CHANGES {
        return Err(storage_invalid("invalid scan size"));
    }
    let mut previous = after;
    for row in &rows {
        validate_row(config, channel, maximum, previous, row)?;
        previous = row.cursor;
    }
    let full = rows.len() == limits::PULL_CHANGES;
    let rows: Vec<_> = rows.into_iter().filter(|row| row.cursor <= bound).collect();
    let mut to = bound;
    if full && previous < bound {
        let later: Vec<Invalidation> = host
            .call_typed(HostRequest::Scan {
                channel: channel.into(),
                after: previous,
                limit: 1,
            })
            .await?;
        if later.len() > 1 {
            return Err(storage_invalid("invalid continuation scan size"));
        }
        if let Some(row) = later.first() {
            validate_row(config, channel, maximum, previous, row)?;
            if row.cursor <= bound {
                to = previous;
            }
        }
    }
    Ok((rows, to))
}

/// Resolve content once per identity, retaining every channel pair's evidence.
async fn channel_changes(
    config: &Config,
    owner: &str,
    models: &BTreeMap<String, u64>,
    rows: Vec<Invalidation>,
    host: &impl Host,
) -> Result<Vec<axton_core::ChannelChange>> {
    let rows = rows
        .into_iter()
        .map(|row| {
            let key = config
                .schema
                .record_key(&row.model, &row.identity)
                .map_err(storage_invalid)?;
            Ok((row, key))
        })
        .collect::<Result<Vec<_>>>()?;
    let keys = rows
        .iter()
        .filter(|(row, _)| row.kind == crate::channel_members::PositionKind::Upsert)
        .map(|(row, key)| (key.clone(), row.stamp))
        .collect();
    let authority = resolve_records(config, owner, models, keys, host).await?;
    let records: BTreeMap<_, _> = authority
        .into_iter()
        .map(|r| {
            let key = RecordKey {
                model: r.model.clone(),
                identity: r.identity.clone(),
            }
            .encoded()
            .map_err(internal)?;
            Ok((key, r))
        })
        .collect::<Result<_>>()?;
    rows.into_iter()
        .map(|(row, key)| {
            Ok(match row.kind {
                crate::channel_members::PositionKind::Remove => axton_core::ChannelChange::Remove {
                    channel: row.channel,
                    cursor: row.cursor,
                    key,
                },
                crate::channel_members::PositionKind::Upsert => {
                    let encoded = key.encoded().map_err(internal)?;
                    axton_core::ChannelChange::Upsert {
                        channel: row.channel,
                        cursor: row.cursor,
                        record: records
                            .get(&encoded)
                            .ok_or_else(|| internal("missing resolved authority"))?
                            .clone(),
                    }
                }
            })
        })
        .collect()
}

pub(crate) async fn process_channel_delta(
    config: &Config,
    owner: &str,
    bytes: &[u8],
    host: &impl Host,
) -> Result<String> {
    let request = axton_core::PullRequest::decode(bytes).map_err(request_invalid)?;
    config.check_declared(&request.models)?;
    let mut cursors = BTreeMap::new();
    let mut rows = vec![];
    for (channel, from) in request.cursors {
        let maximum = head(host, &channel).await?;
        if from > maximum {
            return Err(request_invalid("cursor ahead of head"));
        }
        let (page, to) = channel_rows(config, &channel, from, maximum, maximum, host).await?;
        rows.extend(page);
        cursors.insert(
            channel,
            axton_core::CursorRange {
                from,
                to,
                head: maximum,
            },
        );
    }
    let changes = channel_changes(config, owner, &request.models, rows, host).await?;
    String::from_utf8(
        axton_core::ChannelPullPage { cursors, changes }
            .encode()
            .map_err(internal)?,
    )
    .map_err(internal)
}

pub(crate) async fn process_channel_bootstrap(
    config: &Config,
    owner: &str,
    bytes: &[u8],
    host: &impl Host,
) -> Result<String> {
    let request = BootstrapRequest::decode(bytes).map_err(request_invalid)?;
    config.check_declared(&request.models)?;
    let maximum = head(host, &request.channel).await?;
    if request.until > maximum {
        return Err(request_invalid("bootstrap origin ahead of head"));
    }
    let (rows, to) = channel_rows(
        config,
        &request.channel,
        request.after,
        request.until,
        maximum,
        host,
    )
    .await?;
    let changes = channel_changes(config, owner, &request.models, rows, host).await?;
    String::from_utf8(
        axton_core::ChannelBootstrapPage {
            channel: request.channel,
            from: request.after,
            to,
            until: request.until,
            head: maximum,
            changes,
        }
        .encode()
        .map_err(internal)?,
    )
    .map_err(internal)
}

/// Staged record-only readers retain their old page limit after filtering.
/// Only this compatibility path projects removals away; channel delivery
/// consumes the unfiltered log directly.
pub(crate) async fn legacy_rows(
    config: &Config,
    channel: &str,
    after: u64,
    maximum: u64,
    host: &impl Host,
) -> Result<Vec<Invalidation>> {
    let mut result = vec![];
    let mut cursor = after;
    loop {
        let limit = (limits::PULL_CHANGES - result.len()) as u64;
        let rows: Vec<Invalidation> = host
            .call_typed(HostRequest::Scan {
                channel: channel.into(),
                after: cursor,
                limit,
            })
            .await?;
        if rows.len() > limit as usize {
            return Err(storage_invalid("invalid scan size"));
        }
        let exhausted = rows.len() < limit as usize;
        for row in rows {
            validate_row(config, channel, maximum, cursor, &row)?;
            cursor = row.cursor;
            if row.kind == crate::channel_members::PositionKind::Upsert {
                result.push(row);
            }
        }
        if exhausted || result.len() == limits::PULL_CHANGES || cursor == maximum {
            return Ok(result);
        }
    }
}
