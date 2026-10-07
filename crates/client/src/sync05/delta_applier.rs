use super::DeliveryQueue;
use crate::{ApplyReport, Client, ClientStore, Result, authority::Held, invalid, v05};
use serde_json::{Value, json};
const DDL: &str = "CREATE TABLE IF NOT EXISTS axton_delivery_progress(plan_id TEXT PRIMARY KEY,digest TEXT NOT NULL,header TEXT NOT NULL,next_unit INTEGER NOT NULL,covered INTEGER)";
impl<S: ClientStore> Client<S> {
    pub fn cleanup_delivery05(&mut self, now: u64) -> Result<Vec<String>> {
        self.write(|e| {
            e.exec("axton_delivery_progress", DDL, &[])?;
            e.exec("axton_delivery_key", "CREATE TABLE IF NOT EXISTS axton_delivery_key(plan_id TEXT NOT NULL,identity TEXT NOT NULL,PRIMARY KEY(plan_id,identity))", &[])?;
            e.exec("axton_delivery_key", "DELETE FROM axton_delivery_key WHERE plan_id IN (SELECT plan_id FROM axton_delivery_progress WHERE json_extract(header,'$.expiresAt')<=?)", &[json!(now)])?;
            e.exec("axton_delivery_progress", "DELETE FROM axton_delivery_progress WHERE json_extract(header,'$.expiresAt')<=?", &[json!(now)])?;
            Ok(e.rows("SELECT plan_id FROM axton_delivery_progress", &[])?.rows.into_iter().filter_map(|r| r[0].as_str().map(str::to_owned)).collect())
        })
    }
    pub fn active_delivery_plans05(&mut self) -> Result<Vec<String>> {
        self.view(|e| {
            Ok(e.rows("SELECT plan_id FROM axton_delivery_progress", &[])?
                .rows
                .into_iter()
                .filter_map(|r| r[0].as_str().map(str::to_owned))
                .collect())
        })
    }
    pub fn initialize_stream05(&mut self, start: u64) -> Result<()> {
        self.write(|e| e.initialize_stream05(start))
    }
    pub fn apply_next_delivery05(
        &mut self,
        q: &mut DeliveryQueue,
        now: u64,
    ) -> Result<Option<ApplyReport>> {
        q.expire(now);
        let context = self.request_context05()?;
        let status = self.store_status05()?;
        // Surviving contiguous coverage is the receipt for completed ordinary
        // transfers. Replays must not restore cache state after a later read.
        q.plans.retain(|_, p| {
            let position = if p.header.bootstrap {
                status.bootstrap_cursor
            } else {
                status.cursor
            };
            !(p.header.owner.is_none()
                && p.header.context == context
                && position
                    .zip(p.header.through)
                    .is_some_and(|(c, through)| c >= through))
        });
        let mut selected = None;
        for (id, p) in &q.plans {
            if p.blocked
                || (p.header.owner.is_none()
                    && q.plans.values().any(|blocked| {
                        blocked.blocked
                            && blocked.header.owner.is_none()
                            && blocked.header.bootstrap == p.header.bootstrap
                    }))
            {
                continue;
            }
            p.header.context.admit_store(&context)?;
            if p.header.owner.is_none() && p.header.context != context {
                continue;
            }
            if let Some(after) = p.header.after {
                let position = if p.header.bootstrap {
                    status.bootstrap_cursor.unwrap_or(0)
                } else {
                    status.cursor.ok_or_else(|| invalid("handshake required"))?
                };
                if after > position {
                    continue;
                }
            }
            if let Some(unit) = p.unit(p.next)? {
                selected = Some((id.clone(), p.header.clone(), unit, p.owner.clone()));
                break;
            }
        }
        let Some((id, header, unit, owner)) = selected else {
            self.cleanup_delivery05(now)?;
            return Ok(None);
        };
        // Owned transfers prove the entire identity set before installation.
        let owned = if let Some(request) = &owner {
            let plan = &q.plans[&id];
            let mut units = Vec::new();
            for index in 0..header.units.len() as u64 {
                let Some(u) = plan.unit(index)? else {
                    return Ok(None);
                };
                units.push(u)
            }
            v05::validate_materialization(request, &header, &units)?;
            Some(units)
        } else {
            None
        };
        let applied=self.write(|e|{
            e.exec("axton_delivery_progress",DDL,&[])?;
            e.exec("axton_delivery_key", "CREATE TABLE IF NOT EXISTS axton_delivery_key(plan_id TEXT NOT NULL,identity TEXT NOT NULL,PRIMARY KEY(plan_id,identity))", &[])?;
            e.exec("axton_delivery_key", "DELETE FROM axton_delivery_key WHERE plan_id IN (SELECT plan_id FROM axton_delivery_progress WHERE json_extract(header,'$.expiresAt')<=?)", &[json!(now)])?;
            e.exec("axton_delivery_progress", "DELETE FROM axton_delivery_progress WHERE json_extract(header,'$.expiresAt')<=?", &[json!(now)])?;
            let saved=e.rows("SELECT digest,next_unit FROM axton_delivery_progress WHERE plan_id=?",&[json!(id)])?.rows;
            let next=if let Some(row)=saved.first(){
                if row[0]!=header.digest {return Err(invalid("persisted plan digest mismatch"))}
                row[1].as_u64().ok_or_else(||invalid("invalid delivery progress"))?
            }else{0};
            if next>unit.index {return Ok((None,next))}
            if next!=unit.index {return Err(invalid("delivery unit prefix gap"))}
            let active=e.context05()?;
            header.context.admit_store(&active)?;
            if header.owner.is_none(){header.context.admit(&active)?;}
            let mut held=Held::new();
            let changes=owned.as_ref().map(|units|units.iter().flat_map(|u|u.changes.clone()).collect::<Vec<_>>()).unwrap_or_else(||unit.changes.clone());
            e.exec("axton_delivery_key","CREATE TABLE IF NOT EXISTS axton_delivery_key(plan_id TEXT NOT NULL,identity TEXT NOT NULL,PRIMARY KEY(plan_id,identity))",&[])?;
            for change in &changes {e.exec("axton_delivery_key","INSERT INTO axton_delivery_key(plan_id,identity) VALUES(?,?)",&[json!(id),json!(change.key().encoded()?)])?;}
            let applied=if header.owner.is_some(){e.stage_materialization05(&header.context,&changes,&mut held)?}else{e.stage_authority05(&header.context,&changes,&mut held)?};
            let final_unit=owned.is_some() || unit.index+1==header.units.len() as u64;
            if header.bootstrap {
                let start=e.scalar("SELECT start_cursor FROM axton_store",&[])?.ok_or_else(||invalid("handshake required"))?;
                if header.through!=start.as_u64(){return Err(invalid("bootstrap start mismatch"))}
                if let Some(through)=unit.through {
                    let current=e.scalar("SELECT bootstrap_cursor FROM axton_store",&[])?.and_then(|v|v.as_u64()).unwrap_or(0);
                    if header.after.unwrap()>current {return Err(invalid("bootstrap prefix gap"))}
                    if through>current || final_unit {e.exec("axton_store","UPDATE axton_store SET bootstrap_cursor=?",&[json!(through.max(current))])?;}
                }
            }else if header.owner.is_none() {
                let current=e.scalar("SELECT cursor FROM axton_store",&[])?.and_then(|v|v.as_u64()).ok_or_else(||invalid("handshake required"))?;
                if header.after.unwrap()>current {return Err(invalid("sync prefix gap"))}
                if let Some(through)=unit.through.filter(|c|*c>current) {e.commit_sync_coverage05(current,through)?;}
            }
            if let Some(v05::MaterializationOwner::Schema{previous_materialization})=&header.owner {
                let pending=e.pending_schema05()?.ok_or_else(||invalid("schema transfer no longer pending"))?;
                if &pending.previous_context.materialization!=previous_materialization || pending.desired_context!=header.context {return Err(invalid("schema transfer replaced"))}
                // Reprove every currently held key, including authority received
                // while this desired-context transfer was in flight.
                let keys=changes.iter().map(|c|c.key().encoded()).collect::<Result<std::collections::BTreeSet<_>>>()?;
                if pending.authority_keys.iter().any(|k|k.encoded().is_ok_and(|k|!keys.contains(&k))) {return Err(invalid("schema transfer missing newly held identity"))}
                e.enable_schema05(&pending.previous_context,&pending.desired_context)?;
            }
            let completions=e.reconcile_ready05(&mut held)?;
            let reports=e.rebuild_held(&held)?;
            let next=if owned.is_some(){header.units.len() as u64}else{unit.index+1};
            if final_unit {
                e.exec("axton_delivery_key","DELETE FROM axton_delivery_key WHERE plan_id=?",&[json!(id)])?;
                e.exec("axton_delivery_progress","DELETE FROM axton_delivery_progress WHERE plan_id=?",&[json!(id)])?;
            } else if saved.is_empty() {
                e.exec("axton_delivery_progress","INSERT INTO axton_delivery_progress(plan_id,digest,header,next_unit,covered) VALUES(?,?,?,?,?)",&[json!(id),json!(header.digest),json!(serde_json::to_string(header.as_ref())?),json!(next),unit.through.map_or(Value::Null,Value::from)])?;
            }else{
                e.exec("axton_delivery_progress","UPDATE axton_delivery_progress SET next_unit=?,covered=COALESCE(?,covered) WHERE plan_id=?",&[json!(next),unit.through.map_or(Value::Null,Value::from),json!(id)])?;
            }
            // Each ordinary coverage lane has one durable active transfer.
            // Replacing its partial plan preserves authority/coverage; control
            // drops the retired staging and repairs from the durable prefix.
            if header.owner.is_none() {
                e.exec("axton_delivery_key","DELETE FROM axton_delivery_key WHERE plan_id IN (SELECT plan_id FROM axton_delivery_progress WHERE plan_id<>? AND json_extract(header,'$.owner') IS NULL AND json_extract(header,'$.bootstrap')=?)",&[json!(id),json!(header.bootstrap)])?;
                e.exec("axton_delivery_progress","DELETE FROM axton_delivery_progress WHERE plan_id<>? AND json_extract(header,'$.owner') IS NULL AND json_extract(header,'$.bootstrap')=?",&[json!(id),json!(header.bootstrap)])?;
            }
            Ok((Some(ApplyReport{applied,reports,completions,..Default::default()}),next))
        });
        match applied {
            Ok((report, next)) => {
                let active = self.active_delivery_plans05()?;
                q.plans
                    .retain(|other, p| other == &id || p.next == 0 || active.contains(other));
                let plan = q.plans.get_mut(&id).unwrap();
                plan.next = next;
                plan.parts.retain(|(unit, _), _| *unit >= next);
                if next >= header.units.len() as u64 {
                    q.plans.remove(&id);
                }
                Ok(report)
            }
            Err(error) => {
                q.plans.get_mut(&id).unwrap().blocked = true;
                Err(error)
            }
        }
    }
}
impl<S: ClientStore> Client<S> {
    pub fn subscription_state05(
        &mut self,
        stream: &str,
    ) -> Result<Option<crate::subscriptions::SubscriptionState>> {
        let status = self.store_status05()?;
        if status.context.stream != stream {
            return Ok(None);
        }
        Ok(Some(crate::subscriptions::SubscriptionState {
            stream: stream.into(),
            subscription_id: 1,
            starting_cursor: status.start_cursor,
            cursor: status.cursor,
        }))
    }
    pub fn bootstrap_state05(
        &mut self,
        stream: &str,
        id: u64,
    ) -> Result<crate::bootstrap::BootstrapState> {
        let status = self.store_status05()?;
        if status.context.stream != stream || id != 1 {
            return Err(invalid("subscription.closed: foreign Stream registration"));
        }
        Ok(crate::bootstrap::BootstrapState {
            stream: stream.into(),
            subscription_id: 1,
            run: 1,
            cursor: status.bootstrap_cursor.unwrap_or(0),
            barrier: status.start_cursor,
            state: if status.start_cursor.is_some()
                && status.start_cursor == status.bootstrap_cursor
            {
                crate::bootstrap::BootstrapPhase::Complete
            } else {
                crate::bootstrap::BootstrapPhase::Requested
            },
            error: None,
        })
    }
}
