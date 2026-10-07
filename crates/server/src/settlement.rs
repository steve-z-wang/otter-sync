//! One normalized settlement: canonical stream locks, bulk guards and final tracking.
use crate::host::{
    Acknowledged, GuardMode, GuardRecord, Guards, HostExt, HostRequest, MemberKey, Positions,
    RecordRef, StreamIntent, Tracking, TrackingPair,
};
use crate::stream_members::{MemberDelta, MemberPosition, PositionKind};
use crate::{Config, Error, Host, Result, code, internal};
use axton_core::{MembershipClaim, RecordKey};
use std::collections::{BTreeMap, BTreeSet};
pub(crate) type Changes = BTreeMap<String, RecordKey>;
pub(crate) struct Settlement {
    pub stamps: BTreeMap<String, u64>,
}
impl std::ops::Deref for Settlement {
    type Target = BTreeMap<String, u64>;
    fn deref(&self) -> &Self::Target {
        &self.stamps
    }
}
pub(crate) fn unregistered(model: &str) -> Error {
    Error::new(
        code::LOADER_UNREGISTERED,
        format!("Model {model} has no registered Loader: it is device-only and never published"),
    )
}
pub(crate) fn resolve(config: &Config, record: &RecordRef) -> Result<RecordKey> {
    if !config.loaders.contains(&record.model) {
        return Err(unregistered(&record.model));
    }
    config
        .schema
        .record_key(&record.model, &record.identity)
        .map_err(|e| Error::new(code::HANDLER_INVALID, e.to_string()))
}
pub(crate) fn insert(changes: &mut Changes, key: RecordKey) -> Result<()> {
    changes.insert(key.encoded().map_err(internal)?, key);
    Ok(())
}
#[derive(Clone)]
enum Selection {
    Global,
    Selected(BTreeSet<String>),
}
impl Selection {
    fn selects(&self, stream: &str) -> bool {
        match self {
            Self::Global => true,
            Self::Selected(s) => s.contains(stream),
        }
    }
}
pub(crate) async fn lock_streams(streams: &BTreeSet<String>, host: &impl Host) -> Result<()> {
    if !streams.is_empty() {
        let Acknowledged = host
            .call_typed(HostRequest::LockStreams {
                streams: streams.iter().cloned().collect(),
            })
            .await?;
    }
    Ok(())
}
pub(crate) async fn settle_changes(
    config: &Config,
    changed: &Changes,
    declarations: &[StreamIntent],
    host: &impl Host,
) -> Result<Settlement> {
    settle_locked(config, changed, declarations, &BTreeSet::new(), host).await
}
pub(crate) async fn settle_locked(
    config: &Config,
    changed: &Changes,
    declarations: &[StreamIntent],
    held: &BTreeSet<String>,
    host: &impl Host,
) -> Result<Settlement> {
    let mut records = changed.clone();
    let mut invalidations: BTreeMap<String, Selection> = changed
        .keys()
        .map(|k| (k.clone(), Selection::Global))
        .collect();
    let mut tracks: BTreeMap<(String, String), RecordKey> = BTreeMap::new();
    let mut named = BTreeSet::new();
    for key in changed.values() {
        if !config.loaders.contains(&key.model) {
            return Err(unregistered(&key.model));
        }
    }
    for intent in declarations {
        for stream in intent.streams() {
            axton_core::check_stream(stream)
                .map_err(|_| Error::new(code::PUBLISH_INVALID, "stream must not be blank"))?;
        }
        let key = resolve(config, intent.record())?;
        let encoded = key.encoded().map_err(internal)?;
        match intent {
            StreamIntent::Track { stream, .. } => {
                named.insert(stream.clone());
                tracks.insert((stream.clone(), encoded.clone()), key.clone());
            }
            StreamIntent::Invalidate { streams, .. } => {
                if let Some(streams) = streams {
                    if streams.is_empty() {
                        continue;
                    }
                    let selected: BTreeSet<String> = streams.iter().cloned().collect();
                    named.extend(selected.clone());
                    match invalidations
                        .entry(encoded.clone())
                        .or_insert_with(|| Selection::Selected(BTreeSet::new()))
                    {
                        Selection::Global => {}
                        Selection::Selected(s) => s.extend(selected),
                    }
                } else {
                    invalidations.insert(encoded.clone(), Selection::Global);
                }
            }
        }
        records.insert(encoded, key);
    }
    if records.is_empty() {
        return Ok(Settlement {
            stamps: BTreeMap::new(),
        });
    }
    let globals: Vec<MemberKey> = invalidations
        .iter()
        .filter(|(_, s)| matches!(s, Selection::Global))
        .map(|(k, _)| MemberKey::from_key(&records[k]))
        .collect();
    let mut pairs: BTreeMap<(String, String), TrackingPair> = BTreeMap::new();
    for ((stream, encoded), key) in &tracks {
        pairs.insert(
            (stream.clone(), encoded.clone()),
            TrackingPair {
                stream: stream.clone(),
                model: key.model.clone(),
                identity_key: key.encoded_identity().map_err(internal)?,
            },
        );
    }
    for (encoded, selection) in &invalidations {
        if let Selection::Selected(streams) = selection {
            let key = &records[encoded];
            for stream in streams {
                pairs.insert(
                    (stream.clone(), encoded.clone()),
                    TrackingPair {
                        stream: stream.clone(),
                        model: key.model.clone(),
                        identity_key: key.encoded_identity().map_err(internal)?,
                    },
                );
            }
        }
    }
    let request = HostRequest::ReadTracking {
        records: globals,
        pairs: pairs.into_values().collect(),
    };
    let before: Tracking = host.call_typed(request.clone()).await?;
    let mut locked = named;
    locked.extend(before.iter().map(|p| p.stream.clone()));
    if !locked.is_subset(held) {
        // A caller holding earlier locks must never extend them below an already
        // acquired name. Loads declare all names before their stamp reads.
        if !held.is_empty() {
            return Err(Error::new(
                code::TRANSACTION_CONFLICT,
                "settlement requires streams outside the held lock set",
            ));
        }
        lock_streams(&locked, host).await?;
    }
    let track_keys: BTreeSet<String> = tracks.keys().map(|(_, k)| k.clone()).collect();
    let guards: Vec<GuardRecord> = records
        .iter()
        .map(|(encoded, key)| GuardRecord {
            model: key.model.clone(),
            identity_key: key.encoded_identity().expect("canonical key"),
            mode: if invalidations.contains_key(encoded) {
                GuardMode::Advance
            } else if track_keys.contains(encoded) {
                GuardMode::Ensure
            } else {
                GuardMode::Lock
            },
        })
        .collect();
    let guarded: Guards = host
        .call_typed(HostRequest::GuardRecords { records: guards })
        .await?;
    let mut stamps = BTreeMap::new();
    for ((encoded, _), stamp) in records.iter().zip(guarded) {
        if invalidations.contains_key(encoded) {
            stamps.insert(encoded.clone(), stamp.expect("advance validated").0);
        }
    }
    let after: Tracking = host.call_typed(request).await?;
    let mut final_pairs: BTreeMap<(String, String), RecordKey> = BTreeMap::new();
    for pair in after {
        let key = pair.key();
        let encoded = key.encoded().map_err(internal)?;
        if matches!(invalidations.get(&encoded), Some(Selection::Global))
            && !locked.contains(&pair.stream)
        {
            return Err(Error::new(
                code::TRANSACTION_CONFLICT,
                "a new global holder appeared outside the canonical lock set; retry the whole transaction",
            ));
        }
        final_pairs.insert((pair.stream, encoded), key);
    }
    let existing: BTreeSet<(String, String)> = final_pairs.keys().cloned().collect();
    final_pairs.extend(tracks);
    let deltas: Vec<MemberDelta> = final_pairs
        .into_iter()
        .map(|((stream, encoded), key)| MemberDelta {
            publish: !existing.contains(&(stream.clone(), encoded.clone()))
                || invalidations
                    .get(&encoded)
                    .is_some_and(|s| s.selects(&stream)),
            stream,
            key,
        })
        .collect();
    if !deltas.is_empty() {
        let request = HostRequest::ApplyStreamMembers { deltas };
        let positions: Positions = host.call_typed(request.clone()).await?;
        if host.publication05() {
            check_positions05(&request, &positions)?;
        } else {
            check_positions(&request, &positions)?;
        }
        if config.protocol4.is_some() && !host.publication05() {
            let HostRequest::ApplyStreamMembers { deltas } = &request else {
                unreachable!()
            };
            let published = positions
                .into_iter()
                .zip(deltas)
                .filter_map(|(position, delta)| delta.publish.then_some(position))
                .collect::<Vec<_>>();
            if !published.is_empty() {
                let _: Acknowledged = host
                    .call_typed(HostRequest::SavePublicationGroups {
                        positions: published,
                    })
                    .await?;
            }
        }
    }
    Ok(Settlement { stamps })
}
fn check_positions(request: &HostRequest, positions: &[MemberPosition]) -> Result<()> {
    let HostRequest::ApplyStreamMembers { deltas } = request else {
        return Err(internal(
            "check_positions needs an applyStreamMembers request",
        ));
    };
    if positions.len() != deltas.len() {
        return Err(request.invalid_response(format!(
            "answers {} positions for {} deltas",
            positions.len(),
            deltas.len()
        )));
    }
    let mut published: BTreeMap<&str, Vec<u64>> = BTreeMap::new();
    let mut kept: BTreeMap<&str, Vec<u64>> = BTreeMap::new();
    for (delta, position) in deltas.iter().zip(positions) {
        let kind = PositionKind::Upsert;
        if position.stream != delta.stream || position.key != delta.key || position.kind != kind {
            return Err(request.invalid_response(format!(
                "answers a {:?} position of {} {} in Stream {} for {} {} in Stream {}",
                position.kind,
                position.key.model,
                position.key.identity,
                position.stream,
                delta.key.model,
                delta.key.identity,
                delta.stream
            )));
        }
        let cursors = if delta.publish {
            &mut published
        } else {
            &mut kept
        };
        cursors
            .entry(delta.stream.as_str())
            .or_default()
            .push(position.cursor);
    }
    for (stream, cursors) in &published {
        if cursors.windows(2).any(|pair| pair[1] != pair[0] + 1) {
            return Err(request.invalid_response(format!(
                "Stream {stream} positions {cursors:?} are not one consecutive range"
            )));
        }
        let start = cursors[0];
        if kept
            .get(stream)
            .is_some_and(|kept| kept.iter().any(|cursor| *cursor >= start))
        {
            return Err(request.invalid_response(format!(
                "Stream {stream} keeps a position at or above its new range from {start}"
            )));
        }
    }
    for (stream, cursors) in &kept {
        if cursors.iter().collect::<BTreeSet<_>>().len() != cursors.len() {
            return Err(request
                .invalid_response(format!("Stream {stream} answers one kept position twice")));
        }
    }
    Ok(())
}

/// Preserve saved cursor evidence while adapting its identity like readback.
pub(crate) fn current_claims(
    config: &Config,
    claims: Vec<MembershipClaim>,
) -> Result<Vec<MembershipClaim>> {
    claims
        .into_iter()
        .map(|mut claim| {
            let key = config
                .schema
                .record_key(&claim.model, &claim.identity)
                .map_err(crate::storage_invalid)?;
            claim.identity = key.identity;
            Ok(claim)
        })
        .collect()
}

fn check_positions05(request: &HostRequest, positions: &[MemberPosition]) -> Result<()> {
    let HostRequest::ApplyStreamMembers { deltas } = request else {
        return Err(internal("publication deltas missing"));
    };
    if positions.len() != deltas.len() {
        return Err(request.invalid_response("wrong position count"));
    }
    let mut published = BTreeMap::new();
    for (d, p) in deltas.iter().zip(positions) {
        axton_core::counter(p.cursor).map_err(crate::storage_invalid)?;
        if p.cursor == 0 || d.stream != p.stream || d.key != p.key || p.kind != PositionKind::Upsert
        {
            return Err(request.invalid_response("wrong publication position"));
        }
        if d.publish
            && published
                .insert(&d.stream, p.cursor)
                .is_some_and(|old| old != p.cursor)
        {
            return Err(request.invalid_response("one Mutation has multiple Stream cursors"));
        }
    }
    Ok(())
}
