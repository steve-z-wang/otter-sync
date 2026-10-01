//! Shared settlement of one Mutation's, legacy mutation's, Load page's or
//! external transaction's effects, inside the application's transaction:
//! Scope locks, record guards, membership reduction, stamp allocation and
//! positions ([Publish](../../../docs/engineering/architecture/server/engine/publish.md)).
//! Every step is a host operation; no application SQL lives here.
use crate::host::{
    Acknowledged, HostExt, HostRequest, Locked, Memberships, Positions, RecordRef, ScopeIntent,
    ScopeMembers, Stamped,
};
use crate::scope_members::{
    Declaration, MemberDelta, MemberPosition, MemberState, PositionKind, SelectionAction,
    declared_tags, reduce,
};
use crate::{Config, Error, Host, Result, code, internal};
use axton_core::{AuthorityRecord, MembershipClaim, RecordKey};
use std::collections::{BTreeMap, BTreeSet};

/// Records in canonical key order, deduplicated by `(model, identity)`.
pub(crate) type Changes = BTreeMap<String, RecordKey>;

/// Content stamps and pair positions committed by one settlement.
pub(crate) struct Settlement {
    pub stamps: BTreeMap<String, u64>,
    pub positions: Vec<MemberPosition>,
}
impl std::ops::Deref for Settlement {
    type Target = BTreeMap<String, u64>;
    fn deref(&self) -> &Self::Target {
        &self.stamps
    }
}
impl Settlement {
    pub fn claims(
        &self,
        config: &Config,
        intents: &[ScopeIntent],
        records: &[AuthorityRecord],
    ) -> Result<Vec<MembershipClaim>> {
        let mut claims = BTreeMap::new();
        for intent in intents {
            if let ScopeIntent::Add { scope, record, .. } = intent {
                let key = resolve(config, record)?;
                for position in &self.positions {
                    if position.kind == PositionKind::Upsert
                        && &position.scope == scope
                        && position.key == key
                        && let Some(returned) = records
                            .iter()
                            .find(|r| r.model == key.model && r.identity == key.identity)
                    {
                        claims.insert(
                            (scope.clone(), key.encoded().map_err(internal)?),
                            MembershipClaim {
                                scope: scope.clone(),
                                cursor: position.cursor,
                                model: returned.model.clone(),
                                identity: returned.identity.clone(),
                            },
                        );
                    }
                }
            }
        }
        Ok(claims.into_values().collect())
    }
}

/// The one refusal of a Model without a registered Loader: device-only
/// ([#187](https://github.com/zanminwang/axton/issues/187)), so never
/// published, read back, pulled, fetched or loaded.
pub(crate) fn unregistered(model: &str) -> Error {
    Error::new(
        code::LOADER_UNREGISTERED,
        format!("Model {model} has no registered Loader: it is device-only and never published"),
    )
}

/// Resolve a record a handler named into a canonical key, refusing models
/// this backend does not load.
pub(crate) fn resolve(config: &Config, record: &RecordRef) -> Result<RecordKey> {
    if !config.loaders.contains(&record.model) {
        return Err(unregistered(&record.model));
    }
    config
        .schema
        .record_key(&record.model, &record.identity)
        .map_err(|e| Error::new(code::HANDLER_INVALID, e.to_string()))
}

/// A tag or selector that breaks the tag rules
/// ([`check_tag`], [`declared_tags`]): refused with its declaration.
pub(crate) fn invalid_tags(scope: &str, reason: impl std::fmt::Display) -> Error {
    Error::new(code::HANDLER_INVALID, format!("Scope {scope}: {reason}"))
}

fn label_tags(scope: &str, tags: &[String]) -> Result<BTreeSet<String>> {
    if tags.is_empty() {
        return Err(invalid_tags(
            scope,
            "label operation must name at least one tag",
        ));
    }
    declared_tags(tags).map_err(|reason| invalid_tags(scope, reason))
}

pub(crate) fn insert(changes: &mut Changes, key: RecordKey) -> Result<()> {
    changes.insert(key.encoded().map_err(internal)?, key);
    Ok(())
}

/// The guard settlement took on one record after locking its Scopes.
enum Guard {
    /// `advanceStamp`: the record changed and has this new stamp.
    Changed(u64),
    /// `ensureStamp`, or `lockRecord` on an existing row.
    Present,
    /// `lockRecord` found no metadata row: the record is nobody's member.
    Absent,
}

/// Settle `changed` records and ordered membership intents.
///
/// 1. Validate every intent before any host call: a Scope name, a
///    registered Model and identity, tag spelling and count.
/// 2. Resolve the Scopes to lock: every Scope an intent names plus every
///    Scope a changed record is a member of (`memberships`, a touch's
///    recipients), and `lockScopes` them in canonical order.
/// 3. Guard the records the settlement names in canonical key order: a
///    changed one advances its stamp once, one an add certainly leaves a
///    member ensures its stamp, any other is only locked.
/// 4. Re-read each changed record's Scopes under the locks. One outside
///    the locked set means a competing membership write moved it: the
///    settlement fails `transaction.conflict`, so the owning transaction
///    retries whole, and never locks out of order.
/// 5. Per locked Scope, read the members its declarations and touches
///    reach (`readScopeMembers`), reduce the declarations in order
///    ([`reduce`]) and collect the deltas; a record without metadata that
///    ends a member is given its first stamp.
/// 6. `applyScopeMembers` the deltas at once, and check each position.
///
/// Answers the stamp allocated to each changed record, keyed canonically.
pub(crate) async fn settle_changes(
    config: &Config,
    changed: &Changes,
    memberships: &[ScopeIntent],
    host: &impl Host,
) -> Result<Settlement> {
    settle_locked(config, changed, memberships, &BTreeSet::new(), host).await
}

/// Lock `scopes` in canonical byte order; nothing when there are none.
pub(crate) async fn lock_scopes(scopes: &BTreeSet<String>, host: &impl Host) -> Result<()> {
    if !scopes.is_empty() {
        let Acknowledged = host
            .call_typed(HostRequest::LockScopes {
                scopes: scopes.iter().cloned().collect(),
            })
            .await?;
    }
    Ok(())
}

/// [`settle_changes`] in a transaction that already holds the locks of
/// `held` ([`lock_scopes`]), taken before an earlier record write such as
/// a Load page's `readStamps`. Settlement locks again only when it needs a
/// Scope outside them, which the caller must rule out when its own record
/// writes preceded settlement.
pub(crate) async fn settle_locked(
    config: &Config,
    changed: &Changes,
    memberships: &[ScopeIntent],
    held: &BTreeSet<String>,
    host: &impl Host,
) -> Result<Settlement> {
    for key in changed.values() {
        if !config.loaders.contains(&key.model) {
            return Err(unregistered(&key.model));
        }
    }
    // 1. Each Scope's declarations, in order, and every record to guard.
    let mut scopes: BTreeMap<String, Vec<Declaration>> = BTreeMap::new();
    let mut records = changed.clone();
    for intent in memberships {
        // The one Scope-name rule, as every frame and registration applies
        // it: a name that is nothing but whitespace names no Scope either.
        if axton_core::check_scope(intent.scope()).is_err() {
            return Err(Error::new(code::PUBLISH_INVALID, "scope must not be blank"));
        }
        let declaration = match intent {
            ScopeIntent::Add {
                scope,
                record,
                tags,
            } => Declaration::Add {
                key: resolve(config, record)?,
                tags: declared_tags(tags).map_err(|reason| invalid_tags(scope, reason))?,
            },
            ScopeIntent::Remove { record, .. } => Declaration::Remove {
                key: resolve(config, record)?,
            },
            ScopeIntent::TagAdd {
                scope,
                record,
                tags,
            } => Declaration::TagAdd {
                key: resolve(config, record)?,
                tags: label_tags(scope, tags)?,
            },
            ScopeIntent::TagRemove {
                scope,
                record,
                tags,
            } => Declaration::TagRemove {
                key: resolve(config, record)?,
                tags: label_tags(scope, tags)?,
            },
            ScopeIntent::DetachTags { scope, tags } => Declaration::DetachTags {
                tags: label_tags(scope, tags)?,
            },
            ScopeIntent::Select {
                scope,
                model,
                predicate,
                action,
            } => {
                if let Some(model) = model {
                    if !config.loaders.contains(model) {
                        return Err(unregistered(model));
                    }
                    config
                        .schema
                        .model(model)
                        .map_err(|e| invalid_tags(scope, e))?;
                }
                match action {
                    SelectionAction::TagAdd { tags } | SelectionAction::TagRemove { tags } => {
                        label_tags(scope, &tags.iter().cloned().collect::<Vec<_>>())?;
                    }
                    SelectionAction::Remove => {}
                }
                Declaration::Select {
                    model: model.clone(),
                    predicate: predicate.clone(),
                    action: action.clone(),
                }
            }
        };
        if let Some(key) = declaration.key() {
            insert(&mut records, key.clone())?;
        }
        scopes
            .entry(intent.scope().to_string())
            .or_default()
            .push(declaration);
    }
    let certain = certainly_present(&scopes)?;

    // 2. The Scopes to lock, then the locks, before any record guard.
    let mut locked: BTreeSet<String> = scopes.keys().cloned().collect();
    for key in changed.values() {
        let Memberships(recipients) = host.call_typed(memberships_of(key)?).await?;
        locked.extend(recipients);
    }
    if !locked.is_subset(held) {
        lock_scopes(&locked, host).await?;
    }

    // 3. One guard per record, in canonical key order.
    let mut guards: BTreeMap<String, Guard> = BTreeMap::new();
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
        } else if certain.contains(encoded) {
            let Stamped(_) = host
                .call_typed(HostRequest::EnsureStamp {
                    model,
                    identity_key,
                })
                .await?;
            Guard::Present
        } else {
            let locked: Locked = host
                .call_typed(HostRequest::LockRecord {
                    model,
                    identity_key,
                })
                .await?;
            match locked {
                Some(_) => Guard::Present,
                None => Guard::Absent,
            }
        };
        guards.insert(encoded.clone(), guard);
    }

    // 4. The touch recipients again, under the locks: the resolved set holds.
    let mut recipients: BTreeMap<&String, BTreeSet<String>> = BTreeMap::new();
    for (encoded, key) in changed {
        let Memberships(current) = host.call_typed(memberships_of(key)?).await?;
        if let Some(moved) = current.difference(&locked).next() {
            return Err(Error::new(
                code::TRANSACTION_CONFLICT,
                format!(
                    "{} {} joined Scope {moved} after settlement locked its Scopes; the transaction must retry",
                    key.model, key.identity
                ),
            ));
        }
        recipients.insert(encoded, current);
    }

    // 5. Read, reduce and collect each Scope's deltas.
    let touched: BTreeSet<String> = changed.keys().cloned().collect();
    let mut deltas: Vec<MemberDelta> = vec![];
    for scope in &locked {
        let declarations = scopes.get(scope).map(Vec::as_slice).unwrap_or(&[]);
        let mut explicit: BTreeMap<String, RecordKey> = BTreeMap::new();
        let mut tags: BTreeSet<String> = BTreeSet::new();
        let mut all = false;
        for declaration in declarations {
            match declaration {
                Declaration::Add { key, .. }
                | Declaration::Remove { key }
                | Declaration::TagAdd { key, .. }
                | Declaration::TagRemove { key, .. } => {
                    insert(&mut explicit, key.clone())?;
                }
                Declaration::DetachTags { tags: labels } => {
                    tags.extend(labels.iter().cloned());
                }
                Declaration::Select { predicate, .. } => {
                    tags.extend(predicate.labels());
                    all |= predicate.requires_all();
                }
            }
        }
        for (encoded, current) in &recipients {
            if current.contains(scope) {
                explicit.insert((*encoded).clone(), changed[*encoded].clone());
            }
        }
        if !all && explicit.is_empty() && tags.is_empty() {
            continue;
        }
        let request = HostRequest::ReadScopeMembers {
            scope: scope.clone(),
            explicit_keys: explicit.into_values().collect(),
            all,
            tags: tags.into_iter().collect(),
        };
        let members: ScopeMembers = host.call_typed(request.clone()).await?;
        check_members(&request, &members)?;
        deltas.extend(reduce(scope, members, declarations, &touched)?);
    }
    let joined: BTreeSet<String> = deltas
        .iter()
        .filter(|delta| delta.present)
        .map(|delta| delta.key.encoded().map_err(internal))
        .collect::<Result<_>>()?;
    for encoded in joined {
        if let Some(guard @ Guard::Absent) = guards.get_mut(&encoded) {
            let key = &records[&encoded];
            let Stamped(_) = host
                .call_typed(HostRequest::EnsureStamp {
                    model: key.model.clone(),
                    identity_key: key.encoded_identity().map_err(internal)?,
                })
                .await?;
            *guard = Guard::Present;
        }
    }

    // 6. Persist every final state at once.
    let positions = if !deltas.is_empty() {
        let request = HostRequest::ApplyScopeMembers { deltas };
        let positions: Positions = host.call_typed(request.clone()).await?;
        check_positions(&request, &positions)?;
        positions
    } else {
        vec![]
    };
    Ok(Settlement {
        positions,
        stamps: guards
            .into_iter()
            .filter_map(|(encoded, guard)| match guard {
                Guard::Changed(stamp) => Some((encoded, stamp)),
                _ => None,
            })
            .collect(),
    })
}

fn memberships_of(key: &RecordKey) -> Result<HostRequest> {
    Ok(HostRequest::Memberships {
        model: key.model.clone(),
        identity_key: key.encoded_identity().map_err(internal)?,
    })
}

/// The records an add leaves a member whatever the Scope held before: the
/// last add or remove naming the pair is an add, and no selector follows it
/// on that Scope. Only these are guarded with `ensureStamp` up front;
/// another record that ends a member without metadata gets it afterwards.
fn certainly_present(scopes: &BTreeMap<String, Vec<Declaration>>) -> Result<BTreeSet<String>> {
    let mut certain = BTreeSet::new();
    for declarations in scopes.values() {
        let selector = declarations.iter().rposition(|declaration| {
            matches!(
                declaration,
                Declaration::Select {
                    action: SelectionAction::Remove,
                    ..
                }
            )
        });
        let mut last: BTreeMap<String, (usize, bool)> = BTreeMap::new();
        for (index, declaration) in declarations.iter().enumerate() {
            match declaration {
                Declaration::Add { key, .. } => {
                    last.insert(key.encoded().map_err(internal)?, (index, true));
                }
                Declaration::Remove { key } => {
                    last.insert(key.encoded().map_err(internal)?, (index, false));
                }
                Declaration::TagAdd { .. }
                | Declaration::TagRemove { .. }
                | Declaration::DetachTags { .. }
                | Declaration::Select { .. } => {}
            }
        }
        certain.extend(
            last.into_iter()
                .filter(|(_, (index, add))| *add && selector.is_none_or(|at| at < *index))
                .map(|(encoded, _)| encoded),
        );
    }
    Ok(certain)
}

/// A `readScopeMembers` answer names each record once, and only records the
/// request named or that carry a requested tag.
fn check_members(request: &HostRequest, members: &[MemberState]) -> Result<()> {
    let HostRequest::ReadScopeMembers {
        explicit_keys,
        tags,
        all,
        ..
    } = request
    else {
        return Err(internal("check_members needs a readScopeMembers request"));
    };
    let explicit: BTreeSet<String> = explicit_keys
        .iter()
        .map(|key| key.encoded().map_err(internal))
        .collect::<Result<_>>()?;
    let mut seen = BTreeSet::new();
    for member in members {
        let encoded = member.key.encoded().map_err(internal)?;
        if !seen.insert(encoded.clone()) {
            return Err(request.invalid_response(format!("answers {encoded} twice")));
        }
        if !all && !explicit.contains(&encoded) && !tags.iter().any(|tag| member.tags.contains(tag))
        {
            return Err(request.invalid_response(format!(
                "answers {encoded}, which the request neither names nor selects by tag"
            )));
        }
    }
    Ok(())
}

/// An `applyScopeMembers` answer holds one position per delta, in delta
/// order, naming the delta's Scope and record with the kind its presence
/// implies. Per Scope, the published positions are consecutive and above
/// every kept one, and no cursor repeats.
fn check_positions(request: &HostRequest, positions: &[MemberPosition]) -> Result<()> {
    let HostRequest::ApplyScopeMembers { deltas } = request else {
        return Err(internal(
            "check_positions needs an applyScopeMembers request",
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
        let kind = if delta.present {
            PositionKind::Upsert
        } else {
            PositionKind::Remove
        };
        if position.scope != delta.scope || position.key != delta.key || position.kind != kind {
            return Err(request.invalid_response(format!(
                "answers a {:?} position of {} {} in Scope {} for {} {} in Scope {}",
                position.kind,
                position.key.model,
                position.key.identity,
                position.scope,
                delta.key.model,
                delta.key.identity,
                delta.scope
            )));
        }
        let cursors = if delta.publish {
            &mut published
        } else {
            &mut kept
        };
        cursors
            .entry(delta.scope.as_str())
            .or_default()
            .push(position.cursor);
    }
    for (scope, cursors) in &published {
        if cursors.windows(2).any(|pair| pair[1] != pair[0] + 1) {
            return Err(request.invalid_response(format!(
                "Scope {scope} positions {cursors:?} are not one consecutive range"
            )));
        }
        let start = cursors[0];
        if kept
            .get(scope)
            .is_some_and(|kept| kept.iter().any(|cursor| *cursor >= start))
        {
            return Err(request.invalid_response(format!(
                "Scope {scope} keeps a position at or above its new range from {start}"
            )));
        }
    }
    for (scope, cursors) in &kept {
        if cursors.iter().collect::<BTreeSet<_>>().len() != cursors.len() {
            return Err(
                request.invalid_response(format!("Scope {scope} answers one kept position twice"))
            );
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
