//! Reversible preparation of one owned incoming-authority delivery. The host
//! keeps its session open between preparation and replaying the selected rows.
use crate::authority::{StageEntry, StageMode};
use crate::engine::Engine;
use crate::query_cache::QueryCacheKey;
use crate::store::ClientStore;
use crate::{ApplyReport, BootstrapApply, BootstrapState, Client};
use axton_core::{BootstrapPage, DirectActionResponse, PullPage, PushReceipt, Result, invalid};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

/// One incoming Model snapshot selected for storage.
#[derive(Clone, Debug, PartialEq, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum StoreChange {
    Upsert { identity: Value, row: Value },
    Delete { identity: Value },
}

/// Owned delivery with the guards needed to replay it in the same session.
/// This is a low-level runtime interface, not an SDK hook registration API.
#[doc(hidden)]
pub enum StoreDelivery {
    Page(PullPage),
    Bootstrap {
        scope: String,
        subscription_id: u64,
        run: u64,
        expected_after: u64,
        page: BootstrapPage,
    },
    Direct {
        response: DirectActionResponse,
        snapshot: Option<(QueryCacheKey, Option<String>)>,
    },
    Receipt {
        sequence: u64,
        receipt: PushReceipt,
    },
}

/// Result of replaying a prepared delivery in the host transaction.
#[doc(hidden)]
pub enum StoreResult {
    Page(ApplyReport),
    Bootstrap(crate::BootstrapApply),
    Direct(ApplyReport),
    Receipt(ApplyReport),
}
impl StoreResult {
    pub fn as_page(&self) -> Option<&ApplyReport> {
        if let Self::Page(report) = self {
            Some(report)
        } else {
            None
        }
    }
}

/// Opaque preflight result. Its occurrence selection and guards cannot be
/// changed by a caller between preflight and replay.
#[doc(hidden)]
pub struct PreparedStore {
    delivery: StoreDelivery,
    entries: Vec<StageEntry>,
    accepted: Vec<usize>,
    changes: BTreeMap<String, Vec<StoreChange>>,
    session_id: u64,
    page_guards: Vec<(String, u64, u64)>,
    bootstrap_admitted: Option<BootstrapState>,
}
impl PreparedStore {
    pub fn accepted(&self) -> &[usize] {
        &self.accepted
    }
    pub fn changes(&self) -> &BTreeMap<String, Vec<StoreChange>> {
        &self.changes
    }
}

impl<S: ClientStore> Client<S> {
    fn staged<T>(
        &mut self,
        mode: StageMode,
        body: impl FnOnce(&mut Engine<'_, S>) -> Result<T>,
    ) -> Result<(T, StageMode)> {
        self.session(|tx| {
            tx.engine.stage_mode = mode;
            let result = body(&mut tx.engine);
            let mode = std::mem::take(&mut tx.engine.stage_mode);
            result.map(|result| (result, mode))
        })
    }

    fn run_store(
        &mut self,
        delivery: &StoreDelivery,
        mode: StageMode,
        prepared: Option<&PreparedStore>,
    ) -> Result<(StoreResult, StageMode)> {
        match delivery {
            StoreDelivery::Page(page) => {
                page.validate()?;
                if prepared.is_none() && self.stale_subscription_page(page) {
                    return Ok((
                        StoreResult::Page(ApplyReport {
                            stale: true,
                            ..Default::default()
                        }),
                        mode,
                    ));
                }
                self.staged(mode, |e| {
                    e.apply_page_body(page, prepared.map(|p| p.page_guards.as_slice()))
                        .map(StoreResult::Page)
                })
            }
            StoreDelivery::Bootstrap {
                scope,
                subscription_id,
                run,
                expected_after,
                page,
            } => self.staged(mode, |e| {
                let outcome = if let Some(prepared) = prepared {
                    e.apply_bootstrap_prepared_body(
                        scope,
                        *subscription_id,
                        *run,
                        *expected_after,
                        page,
                        prepared.bootstrap_admitted.as_ref(),
                    )?
                } else {
                    e.apply_bootstrap_page_body(
                        scope,
                        *subscription_id,
                        *run,
                        *expected_after,
                        page,
                    )?
                };
                Ok(StoreResult::Bootstrap(outcome))
            }),
            StoreDelivery::Direct { response, snapshot } => self.staged(mode, |e| {
                e.apply_direct_response_body(
                    response,
                    snapshot
                        .as_ref()
                        .map(|(key, generation)| (key, generation.as_deref())),
                )
                .map(StoreResult::Direct)
            }),
            StoreDelivery::Receipt { sequence, receipt } => self.staged(mode, |e| {
                e.acknowledge(*sequence, receipt).map(StoreResult::Receipt)
            }),
        }
    }

    /// Preflight a complete delivery inside the active session, then undo all
    /// database and in-memory writes. The caller can read the original view
    /// and invoke callbacks before `apply_prepared_store`.
    #[doc(hidden)]
    pub fn prepare_store(&mut self, delivery: StoreDelivery) -> Result<PreparedStore> {
        let session_id = self
            .session
            .as_ref()
            .ok_or_else(|| invalid("store preparation requires an active transaction"))?
            .id;
        let changed = self.session.as_ref().unwrap().changed.clone();
        let pulls = self.pulls.clone();
        self.session_savepoint()?;
        let preflight = self.run_store(&delivery, StageMode::Capture(vec![]), None);
        let rollback = self.session_rollback_savepoint();
        self.session.as_mut().unwrap().changed = changed;
        self.pulls = pulls;
        rollback?;
        let (result, mode) = preflight?;
        let StageMode::Capture(entries) = mode else {
            unreachable!()
        };
        let mut accepted = vec![];
        let mut changes: BTreeMap<String, Vec<StoreChange>> = BTreeMap::new();
        for (index, entry) in entries.iter().enumerate() {
            if let Some((model, change)) = &entry.change {
                accepted.push(index);
                changes
                    .entry(model.clone())
                    .or_default()
                    .push(change.clone());
            }
        }
        let page_guards = if let StoreResult::Page(report) = &result {
            self.session(|tx| {
                report
                    .cursors
                    .iter()
                    .map(|(channel, to)| {
                        let state = tx
                            .engine
                            .subscription(channel)?
                            .ok_or_else(|| invalid("prepared subscription disappeared"))?;
                        Ok((channel.clone(), state.subscription_id, *to))
                    })
                    .collect::<Result<Vec<_>>>()
            })?
        } else {
            vec![]
        };
        let bootstrap_admitted = if let StoreResult::Bootstrap(
            BootstrapApply::Applied { state, .. } | BootstrapApply::Failed { state, .. },
        ) = result
        {
            Some(state)
        } else {
            None
        };
        Ok(PreparedStore {
            delivery,
            entries,
            accepted,
            changes,
            session_id,
            page_guards,
            bootstrap_admitted,
        })
    }

    /// Replay exactly the occurrences admitted by preflight. A promised row
    /// that fails now aborts the complete host transaction.
    #[doc(hidden)]
    pub fn apply_prepared_store(&mut self, prepared: PreparedStore) -> Result<StoreResult> {
        let current = self
            .session
            .as_ref()
            .ok_or_else(|| invalid("store replay requires an active transaction"))?
            .id;
        if current != prepared.session_id {
            return Err(invalid("prepared delivery belongs to another transaction"));
        }
        let count = prepared.entries.len();
        let mode = StageMode::Replay {
            entries: prepared.entries.clone(),
            next: 0,
        };
        let replay = self.run_store(&prepared.delivery, mode, Some(&prepared));
        let (result, mode) = match replay {
            Ok(value) => value,
            Err(error) => {
                self.rollback_session()?;
                return Err(error);
            }
        };
        let StageMode::Replay { next, .. } = mode else {
            unreachable!()
        };
        if next != count {
            self.rollback_session()?;
            return Err(invalid("prepared delivery did not replay all occurrences"));
        }
        if let StoreDelivery::Page(page) = &prepared.delivery {
            self.session.as_mut().unwrap().pull_pages.push(
                page.cursors
                    .iter()
                    .map(|(name, range)| (name.clone(), range.from))
                    .collect(),
            );
        }
        Ok(result)
    }
}
