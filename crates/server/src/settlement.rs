//! Shared settlement of one Mutation's, legacy mutation's or external
//! transaction's effects, inside the application's transaction: record
//! guards, membership reduction, stamp allocation and publication
//! ([Publish](../../../docs/engineering/architecture/server/engine/publish.md)).
//! Every step is a host operation; no application SQL lives here.
use crate::host::{
    Acknowledged, HostExt, HostRequest, Locked, MembershipIntent, Memberships, Published,
    RecordRef, Stamped,
};
use crate::{Config, Error, Host, Result, code, internal};
use axton_core::RecordKey;
use std::collections::{BTreeMap, BTreeSet};

/// Records in canonical key order, deduplicated by `(model, identity)`.
pub(crate) type Changes = BTreeMap<String, RecordKey>;

pub(crate) fn unregistered() -> Error {
    Error::new(code::LOADER_UNREGISTERED, "unregistered loader")
}

/// Resolve a record a handler named into a canonical key, refusing models
/// this backend does not load.
pub(crate) fn resolve(config: &Config, record: &RecordRef) -> Result<RecordKey> {
    if !config.loaders.contains(&record.model) {
        return Err(unregistered());
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

/// The guard settlement took on one record before reading its membership.
enum Guard {
    /// `advanceStamp`: the record changed and has this new stamp.
    Changed(u64),
    /// `ensureStamp` or `lockRecord`: the record is unchanged at this stamp.
    Unchanged(u64),
    /// `lockRecord` found no metadata row: the record has no membership.
    Absent,
}

/// Settle `changed` records and ordered membership intents.
///
/// Membership intents reduce to the final desired state per Channel/record
/// pair. The union of changed and membership records is then guarded in
/// canonical key order: a changed record advances its stamp once, an
/// unchanged record with a final add ensures its stamp, and a remove-only
/// record is only locked (a record without metadata is left alone). Only
/// after every guard is initial membership read. Net membership changes are
/// applied, and publications made, in Channel then record order: a changed
/// record reaches each of its final Channels, an unchanged record each Channel
/// it newly joined, each pair once.
///
/// Answers the stamp allocated to each changed record, keyed canonically.
pub(crate) async fn settle_changes(
    config: &Config,
    changed: &Changes,
    memberships: &[MembershipIntent],
    host: &impl Host,
) -> Result<BTreeMap<String, u64>> {
    for key in changed.values() {
        if !config.loaders.contains(&key.model) {
            return Err(unregistered());
        }
    }
    // The last intent per (record, Channel) pair is its desired state.
    let mut desired: BTreeMap<(String, String), bool> = BTreeMap::new();
    let mut records = changed.clone();
    for intent in memberships {
        // The one Channel-name rule, as every frame and registration applies
        // it: a name that is nothing but whitespace names no Channel either.
        if axton_core::check_channel(&intent.channel).is_err() {
            return Err(Error::new(
                code::PUBLISH_INVALID,
                "channel must not be blank",
            ));
        }
        let key = resolve(
            config,
            &RecordRef {
                model: intent.model.clone(),
                identity: intent.identity.clone(),
            },
        )?;
        let encoded = key.encoded().map_err(internal)?;
        desired.insert((encoded.clone(), intent.channel.clone()), intent.present);
        records.entry(encoded).or_insert(key);
    }
    let intents = |encoded| intents_of(&desired, encoded);

    // One guard per record, in canonical key order, before any membership read.
    let mut guards: BTreeMap<&String, Guard> = BTreeMap::new();
    for (encoded, key) in &records {
        let model = key.model.clone();
        let identity_key = key.encoded_identity().map_err(internal)?;
        let guard = if changed.contains_key(encoded) {
            let Stamped(stamp) = host
                .call_typed(HostRequest::AdvanceStamp {
                    model,
                    identity_key,
                })
                .await?;
            Guard::Changed(stamp)
        } else if intents(encoded).any(|(_, present)| present) {
            let Stamped(stamp) = host
                .call_typed(HostRequest::EnsureStamp {
                    model,
                    identity_key,
                })
                .await?;
            Guard::Unchanged(stamp)
        } else {
            let locked: Locked = host
                .call_typed(HostRequest::LockRecord {
                    model,
                    identity_key,
                })
                .await?;
            match locked {
                Some(Stamped(stamp)) => Guard::Unchanged(stamp),
                None => Guard::Absent,
            }
        };
        guards.insert(encoded, guard);
    }

    // Initial membership, read under each record's guard; then the net
    // relationship changes and the pairs to publish, each at most once.
    let mut transitions: BTreeSet<(&String, &String, bool)> = BTreeSet::new();
    let mut publications: BTreeMap<(String, &String), u64> = BTreeMap::new();
    for (encoded, guard) in &guards {
        let (stamp, changed) = match guard {
            Guard::Changed(stamp) => (*stamp, true),
            Guard::Unchanged(stamp) => (*stamp, false),
            Guard::Absent => continue,
        };
        let key = &records[*encoded];
        let Memberships(initial) = host
            .call_typed(HostRequest::Memberships {
                model: key.model.clone(),
                identity_key: key.encoded_identity().map_err(internal)?,
            })
            .await?;
        let mut members = initial.clone();
        for (channel, present) in intents(encoded) {
            if present && !initial.contains(channel) {
                transitions.insert((channel, encoded, true));
                members.insert(channel.clone());
                if !changed {
                    publications.insert((channel.clone(), encoded), stamp);
                }
            } else if !present && initial.contains(channel) {
                transitions.insert((channel, encoded, false));
                members.remove(channel);
            }
        }
        if changed {
            for channel in members {
                publications.insert((channel, encoded), stamp);
            }
        }
    }

    for (channel, encoded, present) in transitions {
        let key = &records[encoded];
        let Acknowledged = host
            .call_typed(HostRequest::SetMembership {
                channel: channel.clone(),
                model: key.model.clone(),
                identity_key: key.encoded_identity().map_err(internal)?,
                present,
            })
            .await?;
    }
    for ((channel, encoded), stamp) in publications {
        publish_one(host, &channel, &records[encoded], stamp).await?;
    }
    Ok(guards
        .into_iter()
        .filter_map(|(encoded, guard)| match guard {
            Guard::Changed(stamp) => Some((encoded.clone(), stamp)),
            _ => None,
        })
        .collect())
}

/// The desired state of every Channel pair of one record, in Channel order.
fn intents_of<'a>(
    desired: &'a BTreeMap<(String, String), bool>,
    encoded: &'a String,
) -> impl Iterator<Item = (&'a String, bool)> + 'a {
    desired
        .range((encoded.clone(), String::new())..)
        .take_while(move |((record, _), _)| record == encoded)
        .map(|((_, channel), present)| (channel, *present))
}

/// Invalidate one record on one Channel at its current stamp.
async fn publish_one(host: &impl Host, channel: &str, key: &RecordKey, stamp: u64) -> Result<()> {
    let request = HostRequest::Publish {
        channel: channel.into(),
        model: key.model.clone(),
        identity: key.identity.clone(),
        identity_key: key.encoded_identity().map_err(internal)?,
        stamp,
    };
    let Published { stamp: carried, .. } = host.call_typed(request.clone()).await?;
    if carried != stamp {
        return Err(request.invalid_response(format!(
            "invalidation carries stamp {carried}, record is at {stamp}"
        )));
    }
    Ok(())
}
