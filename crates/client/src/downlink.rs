//! Apply a pull page: every scope it names is gated by its cursor, every
//! change lands by its stamp, and the cursors move only after the whole page
//! did ([Distribution](../../../docs/engineering/architecture/client/engine/distribution.md)).
use crate::engine::Engine;
use crate::store::ClientStore;
use crate::{ApplyReport, Client};
use axton_core::{PullPage, Result, invalid};
use std::collections::BTreeMap;

/// A scope that held an initialized subscription when the page was gated and
/// does not hold one now: the page cannot be applied to whatever took its place.
const REPLACED: &str = "subscription removed or uninitialized during page application";

impl<S: ClientStore> Client<S> {
    /// Apply one page in one transaction: all changes, then all cursors. A
    /// scope the client no longer subscribes to, or whose cursor already
    /// covers the range, contributes nothing; a scope whose `from` is beyond
    /// the cursor is a gap and the page is not applied at all. A change that
    /// cannot be applied is reported and leaves nothing behind.
    pub fn apply_page(&mut self, page: PullPage) -> Result<ApplyReport> {
        page.validate()?;
        // A page answering a pull issued before a scope was unsubscribed and
        // subscribed again was built against a cursor this subscription no longer
        // has; it is stale, not a gap, and the next pull from the reset cursor
        // delivers everything.
        if self.stale_subscription_page(&page) {
            return Ok(ApplyReport {
                stale: true,
                ..Default::default()
            });
        }
        self.apply_current_page(page)
    }
    /// The scopes of the page that move this client's cursors: each with its
    /// new cursor. `Err` names a gap. An empty map means the page is covered.
    pub(crate) fn moving_scopes(&mut self, page: &PullPage) -> Result<BTreeMap<String, u64>> {
        let mut moving = BTreeMap::new();
        for (scope, range) in &page.cursors {
            // Only an initialized subscription has a position a page can move.
            // A scope this client unsubscribed, or one still waiting for its
            // first boundary, contributes nothing: that part of the page - a
            // pull still in flight when the unsubscribe committed - is ignored.
            let Some(current) = self.view(|e| e.cursor(scope))? else {
                continue;
            };
            if range.to <= current {
                continue;
            }
            if range.from > current {
                return Err(invalid("pull cursor gap"));
            }
            moving.insert(scope.clone(), range.to);
        }
        Ok(moving)
    }
    /// `apply_page` after the subscription-epoch check; the check consumes the
    /// matching request, so each incoming page runs it exactly once.
    pub(crate) fn apply_current_page(&mut self, page: PullPage) -> Result<ApplyReport> {
        let moving = self.moving_scopes(&page)?;
        if moving.is_empty() {
            return Ok(ApplyReport {
                stale: true,
                ..Default::default()
            });
        }
        self.write(|e| e.apply_page_body(&page, None))
    }
}

impl<S: ClientStore> Engine<'_, S> {
    pub(crate) fn apply_page_body(
        &mut self,
        page: &PullPage,
        guards: Option<&[(String, u64, u64)]>,
    ) -> Result<ApplyReport> {
        let mut moving = BTreeMap::new();
        if let Some(guards) = guards {
            moving.extend(guards.iter().map(|(scope, _, to)| (scope.clone(), *to)));
        } else {
            for (scope, range) in &page.cursors {
                let Some(current) = self.cursor(scope)? else {
                    continue;
                };
                if range.to <= current {
                    continue;
                }
                if range.from > current {
                    return Err(invalid("pull cursor gap"));
                }
                moving.insert(scope.clone(), range.to);
            }
        }
        if moving.is_empty() {
            return Ok(ApplyReport {
                stale: true,
                ..Default::default()
            });
        }
        {
            // The identity each cursor is advanced under is read here, in the
            // writing transaction, so the update cannot land on a subscription
            // that replaced the one the page was gated against.
            let mut advances = Vec::new();
            for (scope, range) in &page.cursors {
                let Some(to) = moving.get(scope) else {
                    continue;
                };
                let identity = match self.subscription(scope)? {
                    Some(state) if guards.is_some() => {
                        let Some((_, expected, _)) =
                            guards.unwrap().iter().find(|(name, _, _)| name == scope)
                        else {
                            continue;
                        };
                        if state.subscription_id != *expected || state.cursor.is_none() {
                            continue;
                        }
                        state.subscription_id
                    }
                    Some(state) => match state.cursor {
                        Some(current) if current == range.from || current < *to => {
                            state.subscription_id
                        }
                        Some(_) => return Err(invalid("cursor moved during page application")),
                        None => return Err(invalid(REPLACED)),
                    },
                    None if guards.is_some() => continue,
                    None => return Err(invalid(REPLACED)),
                };
                advances.push((scope.clone(), identity, *to));
            }
            // Content first, by stamp alone: a record shared by two scopes is
            // in the page once and lands once.
            let mut report = self.apply_records(&page.changes)?;
            for (scope, identity, to) in &advances {
                self.advance_cursor(scope, *identity, *to)?;
            }
            report.cursors = advances
                .iter()
                .map(|(scope, _, to)| (scope.clone(), *to))
                .collect();
            Ok(report)
        }
    }
}

impl<S: ClientStore> Client<S> {
    /// Apply scope membership and authority atomically, retaining provenance.
    pub fn apply_scope_page(&mut self, page: axton_core::ScopePullPage) -> Result<ApplyReport> {
        page.validate()?;
        let legacy = PullPage {
            cursors: page.cursors.clone(),
            changes: vec![],
        };
        if self.stale_subscription_page(&legacy) {
            return Ok(ApplyReport {
                stale: true,
                ..Default::default()
            });
        }
        self.write(|engine| engine.apply_scope_page_body(&page, None))
    }
}
impl<S: ClientStore> Engine<'_, S> {
    pub(crate) fn observe_scope_heads(
        &mut self,
        page: &axton_core::ScopePullPage,
        guards: Option<&[(String, u64, u64)]>,
    ) -> Result<()> {
        for (scope, range) in &page.cursors {
            let Some(state) = self.subscription(scope)? else {
                continue;
            };
            let Some(current) = state.cursor else {
                continue;
            };
            if range.head < current || range.from > current {
                continue;
            }
            if guards.is_some_and(|guards| {
                !guards
                    .iter()
                    .any(|(name, id, _)| name == scope && *id == state.subscription_id)
            }) {
                continue;
            }
            if self.exec("axton_subscription", "UPDATE axton_subscription SET reconcile_bound=? WHERE scope=? AND subscription_id=? AND reconcile_state='requested' AND reconcile_bound IS NULL", &[serde_json::json!(range.head),serde_json::json!(scope),serde_json::json!(state.subscription_id)])? > 0 { self.mark_bootstrap(scope); }
        }
        Ok(())
    }
    pub(crate) fn apply_scope_page_body(
        &mut self,
        page: &axton_core::ScopePullPage,
        guards: Option<&[(String, u64, u64)]>,
    ) -> Result<ApplyReport> {
        page.validate()?;
        self.observe_scope_heads(page, guards)?;
        let mut advances = Vec::new();
        for (scope, range) in &page.cursors {
            let Some(state) = self.subscription(scope)? else {
                continue;
            };
            let Some(current) = state.cursor else {
                continue;
            };
            if let Some(guards) = guards
                && !guards.iter().any(|(name, id, to)| {
                    name == scope && *id == state.subscription_id && *to == range.to
                })
            {
                continue;
            }
            if range.to <= current {
                continue;
            }
            if range.from > current {
                return Err(invalid("pull cursor gap"));
            }
            advances.push((scope.clone(), state.subscription_id, range.to));
        }
        if advances.is_empty() && guards.is_none_or(|guards| guards.is_empty()) {
            return Ok(ApplyReport {
                stale: true,
                ..Default::default()
            });
        }
        let changes: Vec<_> = page
            .changes
            .iter()
            .filter(|change| {
                let scope = match change {
                    axton_core::ScopeChange::Upsert { scope, .. }
                    | axton_core::ScopeChange::Remove { scope, .. } => scope,
                };
                // Preflight already admitted these occurrences. A hook may
                // replace the registration without retracting their authority;
                // only progress remains tied to the old identity.
                if let Some(guards) = guards {
                    guards.iter().any(|(name, _, _)| name == scope)
                } else {
                    advances.iter().any(|(name, _, _)| name == scope)
                }
            })
            .cloned()
            .collect();
        let mut report = self.apply_scope_changes(&changes)?;
        for (scope, id, to) in advances {
            self.advance_cursor(&scope, id, to)?;
            report.cursors.insert(scope, to);
        }
        Ok(report)
    }
}
