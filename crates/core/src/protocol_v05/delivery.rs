//! Frozen planning, fragment admission and owned settlement proof helpers.
use super::*;

/// Conservative constraint components over an already selected, fenced current
/// projection. Excluded Models are never fetched/enrolled by this helper.
/// Dependency edges must be supplied only for atomic reconstruction, not
/// navigation relations. Same-cursor and unique-Model union is transitive.
pub fn plan_units(
    changes: &[AuthorityChange],
    unique_models: &BTreeSet<String>,
    dependencies: &[(RecordKey, RecordKey)],
    after: u64,
    through: u64,
    head: u64,
) -> Result<Vec<DeliveryUnit>> {
    for c in [after, through, head] {
        counter(c)?;
    }
    if after > through || through > head {
        return Err(invalid("invalid planner range"));
    }
    let mut candidates = Vec::new();
    let mut keys = BTreeSet::new();
    for change in changes {
        change.validate()?;
        if change.cursor() > head {
            return Err(invalid("candidate newer than frozen head"));
        }
        if !keys.insert(change.key().encoded()?) {
            return Err(invalid("duplicate candidate"));
        }
        if change.cursor() > after {
            candidates.push(change.clone());
        }
    }
    candidates.sort_by_key(|c| (c.cursor(), c.key().encoded().unwrap()));
    let mut components: Vec<usize> = (0..candidates.len()).collect();
    fn root(ids: &mut [usize], mut at: usize) -> usize {
        while ids[at] != at {
            ids[at] = ids[ids[at]];
            at = ids[at];
        }
        at
    }
    fn union(ids: &mut [usize], a: usize, b: usize) {
        let a = root(ids, a);
        let b = root(ids, b);
        ids[b] = a;
    }
    let mut cursors = BTreeMap::new();
    let mut models = BTreeMap::new();
    let mut identities = BTreeMap::new();
    for (index, change) in candidates.iter().enumerate() {
        identities.insert(change.key().encoded()?, index);
        if let Some(previous) = cursors.insert(change.cursor(), index) {
            union(&mut components, previous, index);
        }
        if unique_models.contains(&change.key().model)
            && let Some(previous) = models.insert(change.key().model.clone(), index)
        {
            union(&mut components, previous, index);
        }
    }
    for (left, right) in dependencies {
        left.validate()?;
        right.validate()?;
        if let (Some(&a), Some(&b)) = (
            identities.get(&left.encoded()?),
            identities.get(&right.encoded()?),
        ) {
            union(&mut components, a, b);
        }
    }
    for index in 0..components.len() {
        components[index] = root(&mut components, index);
    }
    let mut groups = BTreeMap::<usize, Vec<AuthorityChange>>::new();
    for (index, change) in candidates.into_iter().enumerate() {
        groups.entry(components[index]).or_default().push(change);
    }
    let mut groups: Vec<_> = groups.into_values().collect();
    groups.sort_by_key(|g| (g[0].cursor(), g[0].key().encoded().unwrap()));
    if groups.is_empty() {
        return Ok(vec![DeliveryUnit {
            index: 0,
            through: Some(through),
            changes: vec![],
        }]);
    }
    let mut covered = after;
    let mut units = Vec::new();
    for (index, group) in groups.iter().enumerate() {
        // Components are ordered by their first (minimum-cursor) record.
        let next = groups.get(index + 1).map(|next| next[0].cursor());
        let boundary = if let Some(next) = next {
            through
                .checked_sub(1)
                .map(|cap| cap.min(next - 1))
                .filter(|b| *b > covered)
        } else {
            Some(through)
        };
        if let Some(b) = boundary {
            covered = b;
        }
        units.push(DeliveryUnit {
            index: index as u64,
            through: boundary,
            changes: group.clone(),
        });
    }
    Ok(units)
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FrozenDelivery {
    pub header: DeliveryHeader,
    pub parts: Vec<DeliveryPart>,
}
/// Persist together with the Store cursor and unit apply. Calling commit is
/// proof bookkeeping; database consumers must roll it back if apply fails.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DeliveryProgress {
    pub header: DeliveryHeader,
    pub next_unit: u64,
    pub covered: Option<u64>,
    pub parts: BTreeMap<u64, Vec<AuthorityChange>>,
}
impl Validate for DeliveryProgress {
    fn validate(&self) -> Result<()> {
        self.header.validate()?;
        counter(self.next_unit)?;
        if self.next_unit > self.header.units.len() as u64 {
            return Err(invalid("progress beyond plan"));
        }
        let expected = self.header.units[..self.next_unit as usize]
            .iter()
            .filter_map(|u| u.through)
            .next_back();
        if self.covered != expected {
            return Err(invalid("progress coverage mismatch"));
        }
        if self.next_unit == self.header.units.len() as u64 {
            if !self.parts.is_empty() {
                return Err(invalid("completed staging"));
            }
            return Ok(());
        }
        for (index, changes) in &self.parts {
            let part = DeliveryPart {
                plan_id: self.header.plan_id.clone(),
                plan_digest: self.header.digest.clone(),
                unit: self.next_unit,
                part: *index,
                changes: changes.clone(),
            };
            self.check_part(&part)?;
        }
        Ok(())
    }
}
impl DeliveryProgress {
    pub fn new(header: &DeliveryHeader, active: &RequestContext, now: u64) -> Result<Self> {
        header.validate()?;
        header.admit(active)?;
        counter(now)?;
        if now >= header.expires_at {
            return Err(invalid("plan expired"));
        }
        Ok(Self {
            header: header.clone(),
            next_unit: 0,
            covered: None,
            parts: BTreeMap::new(),
        })
    }
    fn admit(&self, active: &RequestContext, now: u64) -> Result<()> {
        self.validate()?;
        self.header.admit(active)?;
        counter(now)?;
        if now >= self.header.expires_at {
            return Err(invalid("plan expired"));
        }
        Ok(())
    }
    fn check_part(&self, part: &DeliveryPart) -> Result<()> {
        part.validate()?;
        if part.plan_id != self.header.plan_id
            || part.plan_digest != self.header.digest
            || part.unit != self.next_unit
        {
            return Err(invalid("part plan or prefix mismatch"));
        }
        let unit = self
            .header
            .units
            .get(part.unit as usize)
            .ok_or_else(|| invalid("unknown unit"))?;
        if unit.parts.get(part.part as usize) != Some(&part_digest(part)?) {
            return Err(invalid("part digest mismatch"));
        }
        Ok(())
    }
    fn complete(&self) -> Result<Option<DeliveryUnit>> {
        let Some(manifest) = self.header.units.get(self.next_unit as usize) else {
            return Ok(None);
        };
        if self.parts.len() != manifest.parts.len() {
            return Ok(None);
        }
        let unit = DeliveryUnit {
            index: self.next_unit,
            through: manifest.through,
            changes: self.parts.values().flatten().cloned().collect(),
        };
        unit.validate()?;
        if unit.changes.iter().map(AuthorityChange::cursor).min() != manifest.minimum_cursor
            || unit.changes.iter().any(|c| {
                c.cursor() > self.header.observed_head
                    || self.header.after.is_some_and(|after| c.cursor() <= after)
            })
            || unit_digest(&unit)? != manifest.digest
        {
            return Err(invalid("assembled unit digest mismatch"));
        }
        Ok(Some(unit))
    }
    pub fn stage_with_limit(
        &mut self,
        part: &DeliveryPart,
        active: &RequestContext,
        now: u64,
        capacity: usize,
    ) -> Result<Option<DeliveryUnit>> {
        self.admit(active, now)?;
        self.check_part(part)?;
        let mut staged = self.clone();
        staged.parts.insert(part.part, part.changes.clone());
        if serde_json::to_vec(&staged.parts)?.len() > capacity {
            return Err(invalid("staging capacity exceeded"));
        }
        let complete = staged.complete()?;
        *self = staged;
        Ok(complete)
    }
    pub fn commit(&mut self, unit: &DeliveryUnit, active: &RequestContext, now: u64) -> Result<()> {
        self.admit(active, now)?;
        if self.complete()?.as_ref() != Some(unit) {
            return Err(invalid("complete staged unit required"));
        }
        if let Some(c) = unit.through {
            self.covered = Some(c);
        }
        self.next_unit += 1;
        self.parts.clear();
        Ok(())
    }
}
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TargetEvidence {
    pub content_cursor: Option<u64>,
    pub materialization: Option<String>,
    pub removed_cursor: Option<u64>,
    pub protected: bool,
}
impl Validate for TargetEvidence {
    fn validate(&self) -> Result<()> {
        if self.content_cursor.is_some() != self.materialization.is_some() {
            return Err(invalid("incomplete target evidence"));
        }
        if let Some(c) = self.content_cursor {
            positive(c)?;
        }
        if let Some(c) = self.removed_cursor {
            positive(c)?;
        }
        if let Some(m) = &self.materialization {
            text(m)?;
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TargetDisposition {
    AwaitAuthority,
    InstalledAuthority,
    FinalizeOwned,
    PreserveAuthority,
}
/// FinalizeOwned grants only ownership cleanup/fallback: no membership or
/// positive Stream evidence and no permission to overwrite later local work.
pub fn target_disposition(
    target: &SettlementTarget,
    materialization: &str,
    evidence: &TargetEvidence,
) -> Result<TargetDisposition> {
    target.validate()?;
    text(materialization)?;
    evidence.validate()?;
    Ok(match target {
        SettlementTarget::Stream { cursor, .. } => {
            if evidence.materialization.as_deref() == Some(materialization)
                && evidence.content_cursor.is_some_and(|c| c >= *cursor)
            {
                TargetDisposition::InstalledAuthority
            } else if evidence.removed_cursor.is_some_and(|c| c > *cursor) {
                if evidence.protected {
                    TargetDisposition::PreserveAuthority
                } else {
                    TargetDisposition::FinalizeOwned
                }
            } else {
                TargetDisposition::AwaitAuthority
            }
        }
        SettlementTarget::Private { .. } => {
            if evidence.protected {
                TargetDisposition::PreserveAuthority
            } else {
                TargetDisposition::FinalizeOwned
            }
        }
    })
}
pub fn settlement_ready(
    sync_cursor: u64,
    normal_cursor: Option<u64>,
    targets: &[SettlementTarget],
    materialization: &str,
    evidence: &BTreeMap<String, TargetEvidence>,
) -> Result<bool> {
    counter(sync_cursor)?;
    if let Some(c) = normal_cursor {
        counter(c)?;
    }
    text(materialization)?;
    let mut ready = normal_cursor.is_some_and(|c| c >= sync_cursor);
    let mut keys = BTreeSet::new();
    for target in targets {
        let key = target.key().encoded()?;
        if !keys.insert(key.clone()) {
            return Err(invalid("duplicate settlement target"));
        }
        if target_disposition(
            target,
            materialization,
            evidence.get(&key).unwrap_or(&TargetEvidence::default()),
        )? == TargetDisposition::AwaitAuthority
        {
            ready = false;
        }
    }
    Ok(ready)
}
/// Pure reservation contract. The server must lock and commit these positions
/// with one Mutation transaction; this helper does not provide concurrency.
pub fn publication_positions(
    heads: &BTreeMap<String, u64>,
    affected: &BTreeSet<String>,
) -> Result<BTreeMap<String, u64>> {
    affected
        .iter()
        .map(|stream| {
            check_stream(stream)?;
            let head = *heads.get(stream).unwrap_or(&0);
            counter(head)?;
            let next = head
                .checked_add(1)
                .ok_or_else(|| invalid("cursor overflow"))?;
            counter(next)?;
            Ok((stream.clone(), next))
        })
        .collect()
}

fn unit_digest(unit: &DeliveryUnit) -> Result<String> {
    hash("axton:delivery-unit:5", &serde_json::to_value(unit)?)
}
pub(super) fn part_digest(part: &DeliveryPart) -> Result<String> {
    hash(
        "axton:delivery-part:5",
        &serde_json::json!({"unit":part.unit,"part":part.part,"changes":part.changes}),
    )
}
pub fn delivery_digest(header: &DeliveryHeader) -> Result<String> {
    let mut value = serde_json::to_value(header)?;
    value.as_object_mut().unwrap().remove("digest");
    hash("axton:delivery-plan:5", &value)
}
impl Validate for DeliveryHeader {
    fn validate(&self) -> Result<()> {
        self.context.validate()?;
        text(&self.plan_id)?;
        digest(&self.digest)?;
        for c in [self.observed_head, self.expires_at] {
            counter(c)?;
        }
        if self.units.is_empty() {
            return Err(invalid("empty plan manifest"));
        }
        match (&self.owner, self.after, self.through) {
            (Some(owner), None, None) => {
                owner.validate()?;
                if let MaterializationOwner::Schema {
                    previous_materialization,
                } = owner
                    && previous_materialization == &self.context.materialization
                {
                    return Err(invalid("schema context did not change"));
                }
                if self.bootstrap || self.units.iter().any(|u| u.through.is_some()) {
                    return Err(invalid("materialization cannot claim range coverage"));
                }
            }
            (None, Some(after), Some(through)) => {
                counter(after)?;
                counter(through)?;
                if after > through || through > self.observed_head {
                    return Err(invalid("invalid delivery range"));
                }
            }
            _ => return Err(invalid("range/owner mismatch")),
        }
        let mut suffix_minima = vec![None; self.units.len() + 1];
        for (index, unit) in self.units.iter().enumerate().rev() {
            suffix_minima[index] = match (unit.minimum_cursor, suffix_minima[index + 1]) {
                (Some(cursor), Some(later)) => Some(cursor.min(later)),
                (minimum, None) | (None, minimum) => minimum,
            };
        }
        let mut covered = self.after;
        for (index, unit) in self.units.iter().enumerate() {
            digest(&unit.digest)?;
            if unit.parts.is_empty() {
                return Err(invalid("unit has no parts"));
            }
            for p in &unit.parts {
                digest(p)?;
            }
            if let Some(cursor) = unit.minimum_cursor {
                positive(cursor)?;
                if cursor > self.observed_head || self.after.is_some_and(|after| cursor <= after) {
                    return Err(invalid("invalid candidate minimum"));
                }
            }
            if let Some(through) = unit.through {
                counter(through)?;
                if covered.is_none_or(|covered| through < covered)
                    || self.through.is_none_or(|end| through > end)
                    || (index + 1 < self.units.len() && Some(through) == self.through)
                    || suffix_minima[index + 1].is_some_and(|cursor| cursor <= through)
                {
                    return Err(invalid("invalid unit coverage"));
                }
                covered = Some(through);
            }
        }
        if self.owner.is_none() && self.units.last().unwrap().through != self.through {
            return Err(invalid("incomplete range coverage"));
        }
        if delivery_digest(self)? != self.digest {
            return Err(invalid("plan digest mismatch"));
        }
        Ok(())
    }
}
impl Validate for DeliveryUnit {
    fn validate(&self) -> Result<()> {
        counter(self.index)?;
        if let Some(c) = self.through {
            counter(c)?;
        }
        let mut keys = BTreeSet::new();
        for change in &self.changes {
            change.validate()?;
            if !keys.insert(change.key().encoded()?) {
                return Err(invalid("duplicate unit key"));
            }
        }
        Ok(())
    }
}
impl Validate for DeliveryPart {
    fn validate(&self) -> Result<()> {
        text(&self.plan_id)?;
        digest(&self.plan_digest)?;
        counter(self.unit)?;
        counter(self.part)?;
        let mut keys = BTreeSet::new();
        for c in &self.changes {
            c.validate()?;
            if !keys.insert(c.key().encoded()?) {
                return Err(invalid("duplicate part key"));
            }
        }
        Ok(())
    }
}
/// Complete delivery validation; a fragment is never itself an apply boundary.
pub fn validate_delivery(header: &DeliveryHeader, units: &[DeliveryUnit]) -> Result<()> {
    header.validate()?;
    if units.len() != header.units.len() {
        return Err(invalid("unit count mismatch"));
    }
    let mut keys = BTreeSet::new();
    for (index, unit) in units.iter().enumerate() {
        unit.validate()?;
        let manifest = &header.units[index];
        if unit.index != index as u64
            || unit.through != manifest.through
            || unit.changes.iter().map(AuthorityChange::cursor).min() != manifest.minimum_cursor
            || unit_digest(unit)? != manifest.digest
        {
            return Err(invalid("unit digest or coverage mismatch"));
        }
        for change in &unit.changes {
            if change.cursor() > header.observed_head
                || header.after.is_some_and(|after| change.cursor() <= after)
                || !keys.insert(change.key().encoded()?)
            {
                return Err(invalid("candidate outside range or duplicate identity"));
            }
        }
    }
    Ok(())
}
/// Freeze complete final-state units before transporting any page. Part size
/// is transport-only; changing it creates a different immutable plan.
#[allow(clippy::too_many_arguments)]
pub fn freeze_delivery(
    context: RequestContext,
    plan_id: String,
    purpose: DeliveryPurpose,
    after: u64,
    through: u64,
    observed_head: u64,
    expires_at: u64,
    units: Vec<DeliveryUnit>,
    part_size: usize,
) -> Result<FrozenDelivery> {
    if !matches!(purpose, DeliveryPurpose::Bootstrap | DeliveryPurpose::Sync) {
        return Err(invalid("use owned materialization envelope"));
    }
    freeze(
        context,
        plan_id,
        purpose == DeliveryPurpose::Bootstrap,
        None,
        Some(after),
        Some(through),
        observed_head,
        expires_at,
        units,
        part_size,
    )
}
#[allow(clippy::too_many_arguments)]
pub fn freeze_materialization(
    context: RequestContext,
    plan_id: String,
    owner: MaterializationOwner,
    observed_head: u64,
    expires_at: u64,
    units: Vec<DeliveryUnit>,
    part_size: usize,
) -> Result<FrozenDelivery> {
    freeze(
        context,
        plan_id,
        false,
        Some(owner),
        None,
        None,
        observed_head,
        expires_at,
        units,
        part_size,
    )
}
#[allow(clippy::too_many_arguments)]
fn freeze(
    context: RequestContext,
    plan_id: String,
    bootstrap: bool,
    owner: Option<MaterializationOwner>,
    after: Option<u64>,
    through: Option<u64>,
    observed_head: u64,
    expires_at: u64,
    units: Vec<DeliveryUnit>,
    part_size: usize,
) -> Result<FrozenDelivery> {
    if part_size == 0 {
        return Err(invalid("zero part size"));
    }
    let mut manifests = Vec::new();
    let mut parts = Vec::new();
    for unit in &units {
        unit.validate()?;
        let chunks: Vec<_> = if unit.changes.is_empty() {
            vec![&[][..]]
        } else {
            unit.changes.chunks(part_size).collect()
        };
        let mut hashes = Vec::new();
        for (index, changes) in chunks.iter().enumerate() {
            let part = DeliveryPart {
                plan_id: plan_id.clone(),
                plan_digest: String::new(),
                unit: unit.index,
                part: index as u64,
                changes: changes.to_vec(),
            };
            hashes.push(part_digest(&part)?);
            parts.push(part);
        }
        manifests.push(UnitManifest {
            through: unit.through,
            minimum_cursor: unit.changes.iter().map(AuthorityChange::cursor).min(),
            digest: unit_digest(unit)?,
            parts: hashes,
        });
    }
    let mut header = DeliveryHeader {
        context,
        plan_id,
        digest: String::new(),
        bootstrap,
        owner,
        after,
        through,
        observed_head,
        expires_at,
        units: manifests,
    };
    header.digest = delivery_digest(&header)?;
    validate_delivery(&header, &units)?;
    for part in &mut parts {
        part.plan_digest = header.digest.clone();
    }
    Ok(FrozenDelivery { header, parts })
}
