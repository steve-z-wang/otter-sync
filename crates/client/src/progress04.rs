//! Durable protocol-4 page plans and identity coverage, installed by the same Engine.
use crate::{
    ApplyReport, Client, ClientStore,
    authority::Held,
    engine::{Engine, as_u64},
};
use axton_core::{
    Result, invalid,
    v04::{self, Validate},
};
use serde_json::{Value, json};

fn encoded<T: serde::Serialize + Validate>(value: &T) -> Result<Value> {
    Ok(json!(
        String::from_utf8(v04::encode(value)?).map_err(|_| invalid("protocol UTF8"))?
    ))
}
fn decoded<T: serde::de::DeserializeOwned + Validate>(value: Value) -> Result<T> {
    v04::decode(
        value
            .as_str()
            .ok_or_else(|| invalid("protocol state not JSON"))?
            .as_bytes(),
    )
}
impl<S: ClientStore> Engine<'_, S> {
    pub(crate) fn cursor04(&mut self) -> Result<u64> {
        as_u64(
            &self
                .scalar("SELECT cursor FROM axton_v04_store WHERE singleton=1", &[])?
                .ok_or_else(|| invalid("Store binding required"))?,
        )
    }
    fn progress04(&mut self) -> Result<Option<v04::PageProgress>> {
        self.scalar("SELECT progress FROM axton_v04_page WHERE singleton=1", &[])?
            .map(decoded)
            .transpose()
    }
    pub(crate) fn coverage04(
        &mut self,
        manifest_id: Option<&str>,
    ) -> Result<Option<v04::BootstrapCoverage>> {
        let coverage = match manifest_id {
            Some(id) => self.scalar(
                "SELECT coverage FROM axton_v04_bootstrap WHERE manifest_id=?",
                &[json!(id)],
            )?,
            None => self.scalar(
                "SELECT coverage FROM axton_v04_bootstrap WHERE purpose='bootstrap' AND active=1",
                &[],
            )?,
        };
        coverage.map(decoded).transpose()
    }
    fn write_coverage04(&mut self, coverage: &v04::BootstrapCoverage) -> Result<()> {
        self.exec(
            "axton_v04_bootstrap",
            "UPDATE axton_v04_bootstrap SET coverage=? WHERE manifest_id=?",
            &[encoded(coverage)?, json!(coverage.manifest_id)],
        )?;
        Ok(())
    }
    pub(crate) fn stage_group04(
        &mut self,
        context: &v04::RequestContext,
        changes: &[v04::StreamChange],
    ) -> Result<ApplyReport> {
        // SQLite's immediate UNIQUE checks require removing conflicting old
        // values before staging the complete final atomic group. No observer
        // sees this transient projection; failure rolls back the entire unit.
        for change in changes {
            change.validate()?;
            let key = self
                .schema
                .record_key(&change.key().model, &change.key().identity)?;
            if key != *change.key() {
                return Err(invalid("noncanonical Stream identity"));
            }
            if let v04::StreamChange::Upsert { record } = change
                && self
                    .evidence04(&key)?
                    .admission(&context.materialization, record.cursor)?
                    != v04::AuthorityAdmission::Duplicate
            {
                self.main_set(&key, None)?;
            }
        }
        let mut held = Held::new();
        let mut report = ApplyReport::default();
        // Publication positions, not transport array order, determine when a
        // declared cascade can replace older child content. Stage equal-position
        // live records before absences so final group projection is consistent.
        let mut ordered: Vec<_> = changes.iter().collect();
        ordered.sort_by_key(|change| match change {
            v04::StreamChange::Upsert { record } => (record.cursor, record.state.is_null()),
            v04::StreamChange::Remove { cursor, .. } => (*cursor, false),
        });
        for change in ordered {
            match change {
                v04::StreamChange::Upsert { record } => {
                    report.applied += usize::from(self.stage_stream04(context, record, &mut held)?)
                }
                v04::StreamChange::Remove { key, cursor } => {
                    let mut evidence = self.evidence04(key)?;
                    evidence.remove(*cursor)?;
                    self.set_evidence04(key, &evidence)?;
                }
            }
        }
        report.reports = self.rebuild_held(&held)?;
        Ok(report)
    }
}
impl<S: ClientStore> Client<S> {
    pub fn stream_cursor04(&mut self) -> Result<u64> {
        self.request_context()?;
        self.view(|engine| engine.cursor04())
    }
    pub fn delta_progress04(&mut self) -> Result<Option<v04::PageProgress>> {
        self.request_context()?;
        self.view(|engine| engine.progress04())
    }
    pub fn begin_delta04(&mut self, page: &v04::DeltaPage) -> Result<v04::PageProgress> {
        page.validate()?;
        page.context.admit(self.request_context()?)?;
        self.write(|engine|{
            if let Some(progress)=engine.progress04()? && progress.page_id==page.page_id {
                let mut probe=progress.clone();
                if progress.complete(page){return Ok(progress);}
                // The real helper validates the entire frozen payload digest,
                // not merely the page's caller-supplied identity.
                probe.commit(page,progress.next_unit)?;
                return Ok(progress);
            }
            if engine.cursor04()?!=page.from{return Err(invalid("delta prefix gap"));}
            let progress=v04::PageProgress::new(page)?;
            engine.exec("axton_v04_page","INSERT INTO axton_v04_page(singleton,page,progress) VALUES(1,?,?) ON CONFLICT(singleton) DO UPDATE SET page=excluded.page,progress=excluded.progress",&[encoded(page)?,encoded(&progress)?])?;
            Ok(progress)
        })
    }
    /// One real SQLite transaction owns this whole proven atomic group.
    pub fn apply_delta_unit04(&mut self, page: &v04::DeltaPage) -> Result<ApplyReport> {
        page.validate()?;
        page.context.admit(self.request_context()?)?;
        self.write(|engine| {
            let mut progress = engine
                .progress04()?
                .ok_or_else(|| invalid("delta plan required"))?;
            if progress.complete(page) {
                return Ok(ApplyReport::default());
            }
            let index = progress.next_unit;
            progress.commit(page, index)?;
            let changes = &page.units[index as usize].changes;
            if engine.cursor04()?
                != if index == 0 {
                    page.from
                } else {
                    page.units[index as usize - 1].through
                }
            {
                return Err(invalid("delta prefix mismatch"));
            }
            let mut report = engine.stage_group04(&page.context, changes)?;
            engine.exec(
                "axton_v04_store",
                "UPDATE axton_v04_store SET cursor=?,initialized=1 WHERE singleton=1",
                &[json!(progress.cursor)],
            )?;
            engine.exec(
                "axton_v04_page",
                "UPDATE axton_v04_page SET progress=? WHERE singleton=1",
                &[encoded(&progress)?],
            )?;
            engine.exec("axton_subscription","UPDATE axton_subscription SET cursor=? WHERE stream=? AND starting_cursor IS NOT NULL",&[json!(progress.cursor),json!(page.context.binding.stream)])?;
            report
                .cursors
                .insert(page.context.binding.stream.clone(), progress.cursor);
            Ok(report)
        })
    }
    /// Tools may drain a page; the runtime schedules its units individually.
    pub fn apply_delta04(&mut self, page: &v04::DeltaPage) -> Result<ApplyReport> {
        let mut progress = self.begin_delta04(page)?;
        let mut report = ApplyReport::default();
        while !progress.complete(page) {
            let applied = self.apply_delta_unit04(page)?;
            report.applied += applied.applied;
            report.reports.extend(applied.reports);
            report.cursors.extend(applied.cursors);
            progress = self
                .delta_progress04()?
                .ok_or_else(|| invalid("delta progress disappeared"))?;
        }
        Ok(report)
    }
    pub fn start_bootstrap04(
        &mut self,
        started: &v04::BootstrapStarted,
    ) -> Result<v04::BootstrapCoverage> {
        self.start_manifest04(started, true)
    }
    /// Receipt-owned materialization installs records without establishing delivery C.
    pub fn start_materialize04(
        &mut self,
        started: &v04::BootstrapStarted,
    ) -> Result<v04::BootstrapCoverage> {
        self.start_manifest04(started, false)
    }
    fn start_manifest04(
        &mut self,
        started: &v04::BootstrapStarted,
        public: bool,
    ) -> Result<v04::BootstrapCoverage> {
        started.validate()?;
        started.context.admit(self.request_context()?)?;
        self.write(|engine|{
            if let Some(saved)=engine.coverage04(Some(&started.manifest_id))? {
                let purpose = engine.scalar("SELECT purpose FROM axton_v04_bootstrap WHERE manifest_id=?", &[json!(started.manifest_id)])?;
                if purpose != Some(json!(if public {"bootstrap"}else{"materialize"})) { return Err(invalid("manifest purpose mismatch")); }
                if saved.initial_cursor!=started.start||saved.total!=started.total||saved.materialization!=started.context.materialization{return Err(invalid("manifest binding mismatch"));}
                return Ok(saved);
            }
            let coverage=v04::BootstrapCoverage::new(started.manifest_id.clone(),started.context.materialization.clone(),started.start,started.total)?;
            if public { engine.exec("axton_v04_bootstrap","UPDATE axton_v04_bootstrap SET active=0 WHERE purpose='bootstrap'",&[])?; }
            engine.exec("axton_v04_bootstrap","INSERT INTO axton_v04_bootstrap(manifest_id,purpose,active,coverage) VALUES(?,?,1,?)",&[json!(started.manifest_id),json!(if public {"bootstrap"}else{"materialize"}),encoded(&coverage)?])?;
            // A first Bootstrap establishes the future delivery boundary only.
            // Rematerialization leaves the already initialized shared C alone.
            if public {
                engine.exec("axton_v04_store","UPDATE axton_v04_store SET cursor=?,initialized=1 WHERE singleton=1 AND initialized=0",&[json!(started.start)])?;
                let (subscription,_)=engine.ensure_subscription(&started.context.binding.stream)?;
                let cursor=engine.cursor04()?;
                engine.initialize_subscription(&started.context.binding.stream,subscription.subscription_id,cursor)?;
            }
            Ok(coverage)
        })
    }
    pub fn apply_manifest04(&mut self, page: &v04::ManifestPage) -> Result<ApplyReport> {
        page.validate()?;
        page.context.admit(self.request_context()?)?;
        let active = self.request_context()?.clone();
        self.write(|engine| {
            let mut coverage = engine
                .coverage04(Some(&page.manifest_id))?
                .ok_or_else(|| invalid("manifest not started"))?;
            coverage.commit_page(page, &active)?;
            let changes = page
                .items
                .iter()
                .map(|item| item.change.clone())
                .chain(page.companions.iter().cloned())
                .collect::<Vec<_>>();
            let report = engine.stage_group04(&page.context, &changes)?;
            engine.write_coverage04(&coverage)?;
            Ok(report)
        })
    }
    pub fn capture_bootstrap_tail04(&mut self, tail: &v04::BootstrapTail) -> Result<()> {
        tail.validate()?;
        tail.context.admit(self.request_context()?)?;
        self.write(|engine| {
            let mut coverage = engine
                .coverage04(Some(&tail.manifest_id))?
                .ok_or_else(|| invalid("manifest not started"))?;
            if coverage.manifest_id != tail.manifest_id {
                return Err(invalid("manifest binding mismatch"));
            }
            coverage.set_tail(tail.head)?;
            engine.write_coverage04(&coverage)
        })
    }
    pub fn bootstrap_coverage04(&mut self) -> Result<Option<v04::BootstrapCoverage>> {
        let active = self.request_context()?.materialization.clone();
        self.view(|engine| {
            Ok(engine
                .coverage04(None)?
                .filter(|coverage| coverage.materialization == active))
        })
    }
    pub fn bootstrap_complete04(&mut self) -> Result<bool> {
        let active = self.request_context()?.materialization.clone();
        self.view(|engine| {
            let Some(coverage) = engine
                .coverage04(None)?
                .filter(|coverage| coverage.materialization == active)
            else {
                return Ok(false);
            };
            coverage.complete(engine.cursor04()?)
        })
    }
}
