use crate::{Result, invalid, v05};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use v05::Validate;

#[derive(Clone)]
pub(crate) struct StagedPart {
    memory: Option<Arc<v05::DeliveryPart>>,
    disk: Option<Arc<Spool>>,
    bytes: usize,
}
struct Spool(std::fs::File);
impl StagedPart {
    fn load(&self) -> Result<v05::DeliveryPart> {
        if let Some(part) = &self.memory {
            return Ok((**part).clone());
        }
        let file = &self.disk.as_ref().unwrap().0;
        let mut bytes = vec![0; self.bytes];
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            file.read_exact_at(&mut bytes, 0)
                .map_err(|e| invalid(format!("delivery staging: {e}")))?;
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::FileExt;
            let mut offset = 0;
            while offset < bytes.len() {
                let count = file
                    .seek_read(&mut bytes[offset..], offset as u64)
                    .map_err(|e| invalid(format!("delivery staging: {e}")))?;
                if count == 0 {
                    return Err(invalid("truncated delivery staging"));
                }
                offset += count;
            }
        }
        Ok(serde_json::from_slice(&bytes)?)
    }
    fn new(part: &v05::DeliveryPart, memory: bool) -> Result<Self> {
        let bytes = serde_json::to_vec(part)?;
        if memory {
            return Ok(Self {
                memory: Some(Arc::new(part.clone())),
                disk: None,
                bytes: bytes.len(),
            });
        }
        let path =
            std::env::temp_dir().join(format!("axton-delivery-{}.json", uuid::Uuid::new_v4()));
        use std::io::Write;
        let mut options = std::fs::OpenOptions::new();
        options.create_new(true).write(true).read(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options
            .open(&path)
            .map_err(|e| invalid(format!("delivery staging: {e}")))?;
        // Unlink while the handle is alive: the kernel reclaims it even on
        // process exit. No per-Store sweep can touch another live client.
        if let Err(error) = std::fs::remove_file(&path) {
            drop(file);
            let _ = std::fs::remove_file(&path);
            return Err(invalid(format!("anonymous delivery staging: {error}")));
        }
        file.write_all(&bytes)
            .map_err(|e| invalid(format!("delivery staging: {e}")))?;
        Ok(Self {
            memory: None,
            disk: Some(Arc::new(Spool(file))),
            bytes: bytes.len(),
        })
    }
}
#[derive(Clone)]
pub(crate) struct Plan {
    pub header: Arc<v05::DeliveryHeader>,
    pub parts: BTreeMap<(u64, u64), StagedPart>,
    pub next: u64,
    pub blocked: bool,
    pub owner: Option<v05::MaterializationRequest>,
    owner_keys: Arc<BTreeSet<String>>,
    metadata_bytes: usize,
}
impl Plan {
    pub fn worker_snapshot(&self) -> Self {
        let parts = if self.header.owner.is_some() {
            self.parts.clone()
        } else {
            self.parts
                .range((self.next, 0)..=(self.next, u64::MAX))
                .map(|(k, p)| (*k, p.clone()))
                .collect()
        };
        Self {
            header: self.header.clone(),
            parts,
            next: self.next,
            blocked: self.blocked,
            owner: self.owner.clone(),
            owner_keys: self.owner_keys.clone(),
            metadata_bytes: self.metadata_bytes,
        }
    }
    pub fn complete(&self, index: u64) -> bool {
        self.header
            .units
            .get(index as usize)
            .is_some_and(|manifest| {
                (0..manifest.parts.len() as u64).all(|part| self.parts.contains_key(&(index, part)))
            })
    }
    pub fn unit(&self, index: u64) -> Result<Option<v05::DeliveryUnit>> {
        if !self.complete(index) {
            return Ok(None);
        }
        let Some(manifest) = self.header.units.get(index as usize) else {
            return Ok(None);
        };
        let mut changes = Vec::new();
        for part in 0..manifest.parts.len() as u64 {
            let Some(p) = self.parts.get(&(index, part)) else {
                return Ok(None);
            };
            let part = p.load()?;
            if v05::part_digest(&part)?
                != *manifest
                    .parts
                    .get(part.part as usize)
                    .ok_or_else(|| invalid("disk staging part index"))?
            {
                return Err(invalid("disk staging digest mismatch"));
            }
            changes.extend(part.changes);
        }
        let unit = v05::DeliveryUnit {
            index,
            through: manifest.through,
            changes,
        };
        unit.validate()?;
        if v05::unit_digest(&unit)? != manifest.digest
            || unit.changes.iter().map(v05::AuthorityChange::cursor).min()
                != manifest.minimum_cursor
        {
            return Err(invalid("unit digest mismatch"));
        }
        Ok(Some(unit))
    }
}
/// Bounded network staging. Admission performs no SQL; consumers never hold its
/// lock across Store work. One slot is reserved for repair; larger components spill to bounded disk staging.
#[derive(Clone)]
pub struct DeliveryQueue {
    pub(crate) plans: BTreeMap<String, Plan>,
    capacity: usize,
    slots: usize,
    payload_limit: usize,
}
impl DeliveryQueue {
    pub fn new(capacity: usize, slots: usize) -> Self {
        Self::with_limits(capacity, slots, 256 * 1024 * 1024)
    }
    pub fn with_limits(capacity: usize, slots: usize, payload_limit: usize) -> Self {
        Self {
            plans: BTreeMap::new(),
            capacity,
            slots: slots.max(2),
            payload_limit,
        }
    }
    pub fn receive(
        &mut self,
        header: &v05::DeliveryHeader,
        parts: &[v05::DeliveryPart],
        active: &v05::RequestContext,
        now: u64,
    ) -> Result<()> {
        if header.context.store_id != active.store_id || header.context.stream != active.stream {
            return Err(invalid("foreign Store delivery"));
        }
        if now >= header.expires_at {
            return Err(invalid("delivery.expired"));
        }
        let existing = self.plans.get(&header.plan_id);
        let metadata_bytes = match existing {
            Some(plan) => plan.metadata_bytes,
            None => serde_json::to_vec(header)?.len(),
        };
        if let Some(plan) = existing {
            if plan.header.as_ref() != header {
                return Err(invalid("changed immutable plan"));
            }
        } else {
            header.admit(active)?;
            let earliest = self
                .plans
                .values()
                .filter(|p| p.header.owner.is_none() && p.header.bootstrap == header.bootstrap)
                .filter_map(|p| p.header.after)
                .min();
            let repair = header.after.is_some_and(|a| earliest.is_none_or(|e| a < e));
            if self.plans.len() >= self.slots || (!repair && self.plans.len() >= self.slots - 1) {
                return Err(invalid("delivery queue overflow"));
            }
        }
        let mut extra = 0;
        for part in parts {
            part.validate()?;
            if part.plan_id != header.plan_id
                || part.plan_digest != header.digest
                || header
                    .units
                    .get(part.unit as usize)
                    .and_then(|u| u.parts.get(part.part as usize))
                    != Some(&v05::part_digest(part)?)
            {
                return Err(invalid("fragment correlation mismatch"));
            }
            if part.changes.iter().any(|c| {
                c.cursor() > header.observed_head || header.after.is_some_and(|a| c.cursor() <= a)
            }) {
                return Err(invalid("fragment outside plan"));
            }
            if existing.is_none_or(|p| !p.parts.contains_key(&(part.unit, part.part))) {
                extra += serde_json::to_vec(part)?.len();
            }
        }
        let metadata_new = if existing.is_none() {
            metadata_bytes
        } else {
            0
        };
        let is_new = existing.is_none();
        // Recoverable future offers may be replaced to admit an earlier
        // prefix, without advancing coverage or disturbing its SQL snapshot.
        let mut candidates: Vec<_> = self
            .plans
            .iter()
            .filter_map(|(id, p)| match (header.after, p.header.after) {
                (Some(after), Some(future))
                    if is_new
                        && header.owner.is_none()
                        && p.header.owner.is_none()
                        && header.bootstrap == p.header.bootstrap
                        && future > after =>
                {
                    Some((future, id.clone()))
                }
                _ => None,
            })
            .collect();
        candidates.sort_by(|a, b| b.cmp(a));
        let totals = |plans: &BTreeMap<String, Plan>| {
            (
                plans.values().map(|p| p.metadata_bytes).sum::<usize>(),
                plans
                    .values()
                    .flat_map(|p| p.parts.values())
                    .map(|p| p.bytes)
                    .sum::<usize>(),
            )
        };
        for (_, id) in candidates {
            let (metadata, total) = totals(&self.plans);
            if metadata.saturating_add(metadata_new) <= self.capacity
                && total.saturating_add(extra) <= self.payload_limit
            {
                break;
            }
            self.plans.remove(&id);
        }
        let (metadata_used, total) = totals(&self.plans);
        if metadata_used.saturating_add(metadata_new) > self.capacity {
            return Err(invalid("delivery manifest capacity exceeded"));
        }
        if total.saturating_add(extra) > self.payload_limit {
            return Err(invalid("delivery disk staging capacity exceeded"));
        }
        let mut used = self
            .plans
            .values()
            .flat_map(|p| p.parts.values())
            .filter(|p| p.memory.is_some())
            .map(|p| p.bytes)
            .sum::<usize>();
        let memory_budget = self
            .capacity
            .saturating_sub(metadata_used.saturating_add(metadata_new));
        for p in self.plans.values_mut() {
            for part in p.parts.values_mut() {
                if used <= memory_budget {
                    break;
                }
                if part.memory.is_some() {
                    let payload = part.load()?;
                    let disk = StagedPart::new(&payload, false)?;
                    used -= part.bytes;
                    *part = disk;
                }
            }
        }
        let existing = self.plans.get(&header.plan_id);
        let mut staged = Vec::new();
        let mut memory = used;
        for part in parts {
            if existing.is_some_and(|p| {
                part.unit < p.next || p.parts.contains_key(&(part.unit, part.part))
            }) {
                continue;
            }
            let size = serde_json::to_vec(part)?.len();
            let headers = metadata_used
                + if existing.is_none() {
                    metadata_bytes
                } else {
                    0
                };
            let in_memory =
                memory.saturating_add(size).saturating_add(headers) <= self.capacity / 2;
            if in_memory {
                memory += size;
            }
            staged.push(((part.unit, part.part), StagedPart::new(part, in_memory)?));
        }
        let plan = self
            .plans
            .entry(header.plan_id.clone())
            .or_insert_with(|| Plan {
                header: Arc::new(header.clone()),
                parts: BTreeMap::new(),
                next: 0,
                blocked: false,
                owner: None,
                owner_keys: Arc::default(),
                metadata_bytes,
            });
        for (key, part) in staged {
            plan.parts.insert(key, part);
        }
        Ok(())
    }
    pub fn receive_owned(
        &mut self,
        request: &v05::MaterializationRequest,
        response: &v05::MaterializationResponse,
        active: &v05::RequestContext,
        now: u64,
    ) -> Result<()> {
        if let Some(plan) = self.plans.get(&response.delivery.header.plan_id)
            && let Some(original) = &plan.owner
        {
            request.validate()?;
            let mut current = request.clone();
            current.continuation = original.continuation.clone();
            if *original != current || response.request_id != request.request_id {
                return Err(invalid("changed materialization owner"));
            }
            if let Some(c) = &request.continuation
                && (c.plan_id != response.delivery.header.plan_id
                    || c.digest != response.delivery.header.digest
                    || !response
                        .delivery
                        .parts
                        .iter()
                        .any(|p| p.unit == c.unit && p.part == c.part))
            {
                return Err(invalid("materialization continuation mismatch"));
            }
            for change in response.delivery.parts.iter().flat_map(|p| &p.changes) {
                if !plan.owner_keys.contains(&change.key().encoded()?)
                    && !request.models.contains_key(&change.key().model)
                {
                    return Err(invalid("unowned materialization key"));
                }
            }
        } else {
            response.admit(request, active)?;
        }
        self.receive(
            &response.delivery.header,
            &response.delivery.parts,
            active,
            now,
        )?;
        let plan = self
            .plans
            .get_mut(&response.delivery.header.plan_id)
            .unwrap();
        if let Some(original) = &plan.owner {
            let mut current = request.clone();
            current.continuation = original.continuation.clone();
            if *original != current {
                return Err(invalid("changed materialization owner"));
            }
        } else {
            plan.owner_keys = Arc::new(
                request
                    .keys
                    .iter()
                    .map(v05::RecordKey::encoded)
                    .collect::<Result<_>>()?,
            );
            plan.owner = Some(request.clone());
        }
        Ok(())
    }
    pub fn expire(&mut self, now: u64) {
        self.plans.retain(|_, p| now < p.header.expires_at);
    }
    pub fn retry_blocked(&mut self) {
        for p in self.plans.values_mut() {
            p.blocked = false;
        }
    }
    pub fn len(&self) -> usize {
        self.plans.len()
    }
    pub fn is_empty(&self) -> bool {
        self.plans.is_empty()
    }
    pub fn continuations(&self) -> Vec<v05::Continuation> {
        self.plans
            .values()
            .filter(|p| !p.blocked)
            .filter_map(|p| {
                let end = if p.header.owner.is_some() {
                    p.header.units.len() as u64
                } else {
                    p.next + 1
                };
                for unit in p.next..end {
                    let manifest = p.header.units.get(unit as usize)?;
                    if let Some(part) = (0..manifest.parts.len() as u64)
                        .find(|part| !p.parts.contains_key(&(unit, *part)))
                    {
                        return Some(v05::Continuation {
                            plan_id: p.header.plan_id.clone(),
                            digest: p.header.digest.clone(),
                            unit,
                            part,
                        });
                    }
                }
                None
            })
            .collect()
    }
}
