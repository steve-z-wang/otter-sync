//! Apply a pull page: every stream it names is gated by its cursor, every
//! change lands by its stamp, and the cursors move only after the whole page
//! did ([Distribution](../../../docs/engineering/architecture/client/engine/distribution.md)).
use crate::engine::Engine;
use crate::store::ClientStore;
use crate::{ApplyReport, Client};
use axton_core::{PullPage, Result, invalid};
use std::collections::BTreeMap;

/// A stream that held an initialized subscription when the page was gated and
/// does not hold one now: the page cannot be applied to whatever took its place.
const REPLACED: &str = "subscription removed or uninitialized during page application";

impl<S: ClientStore> Client<S> {
    /// Apply one page in one transaction: all changes, then all cursors. A
    /// stream the client no longer subscribes to, or whose cursor already
    /// covers the range, contributes nothing; a stream whose `from` is beyond
    /// the cursor is a gap and the page is not applied at all. A change that
    /// cannot be applied is reported and leaves nothing behind.
    pub fn apply_page(&mut self, page: PullPage) -> Result<ApplyReport> {
        page.validate()?;
        // A page answering a pull issued before a stream was unsubscribed and
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
    /// The streams of the page that move this client's cursors: each with its
    /// new cursor. `Err` names a gap. An empty map means the page is covered.
    pub(crate) fn moving_streams(&mut self, page: &PullPage) -> Result<BTreeMap<String, u64>> {
        let mut moving = BTreeMap::new();
        for (stream, range) in &page.cursors {
            // Only an initialized subscription has a position a page can move.
            // A stream this client unsubscribed, or one still waiting for its
            // first boundary, contributes nothing: that part of the page - a
            // pull still in flight when the unsubscribe committed - is ignored.
            let Some(current) = self.view(|e| e.cursor(stream))? else {
                continue;
            };
            if range.to <= current {
                continue;
            }
            if range.from > current {
                return Err(invalid("pull cursor gap"));
            }
            moving.insert(stream.clone(), range.to);
        }
        Ok(moving)
    }
    /// `apply_page` after the subscription-epoch check; the check consumes the
    /// matching request, so each incoming page runs it exactly once.
    pub(crate) fn apply_current_page(&mut self, page: PullPage) -> Result<ApplyReport> {
        let moving = self.moving_streams(&page)?;
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
            moving.extend(guards.iter().map(|(stream, _, to)| (stream.clone(), *to)));
        } else {
            for (stream, range) in &page.cursors {
                let Some(current) = self.cursor(stream)? else {
                    continue;
                };
                if range.to <= current {
                    continue;
                }
                if range.from > current {
                    return Err(invalid("pull cursor gap"));
                }
                moving.insert(stream.clone(), range.to);
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
            for (stream, range) in &page.cursors {
                let Some(to) = moving.get(stream) else {
                    continue;
                };
                let identity = match self.subscription(stream)? {
                    Some(state) if guards.is_some() => {
                        let Some((_, expected, _)) =
                            guards.unwrap().iter().find(|(name, _, _)| name == stream)
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
                advances.push((stream.clone(), identity, *to));
            }
            // Content first, by stamp alone: a record shared by two streams is
            // in the page once and lands once.
            let mut report = self.apply_records(&page.changes)?;
            for (stream, identity, to) in &advances {
                self.advance_cursor(stream, *identity, *to)?;
            }
            report.cursors = advances
                .iter()
                .map(|(stream, _, to)| (stream.clone(), *to))
                .collect();
            Ok(report)
        }
    }
}

impl<S: ClientStore> Client<S> {
    /// Apply canonical Stream authority and delivery progress atomically.
    pub fn apply_stream_page(&mut self, page: axton_core::StreamPullPage) -> Result<ApplyReport> {
        if self.context04.is_some() {
            return Err(invalid("legacy protocol seam is retired in protocol 4"));
        }
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
        self.write(|engine| engine.apply_stream_page_body(&page, None))
    }
}
impl<S: ClientStore> Engine<'_, S> {
    pub(crate) fn apply_stream_page_body(
        &mut self,
        page: &axton_core::StreamPullPage,
        guards: Option<&[(String, u64, u64)]>,
    ) -> Result<ApplyReport> {
        page.validate()?;
        let mut advances = Vec::new();
        for (stream, range) in &page.cursors {
            let Some(state) = self.subscription(stream)? else {
                continue;
            };
            let Some(current) = state.cursor else {
                continue;
            };
            if let Some(guards) = guards
                && !guards.iter().any(|(name, id, to)| {
                    name == stream && *id == state.subscription_id && *to == range.to
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
            advances.push((stream.clone(), state.subscription_id, range.to));
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
                let stream = match change {
                    axton_core::StreamChange::Upsert { stream, .. }
                    | axton_core::StreamChange::Remove { stream, .. } => stream,
                };
                // Preflight already admitted these occurrences. A hook may
                // replace the registration without retracting their authority;
                // only progress remains tied to the old identity.
                if let Some(guards) = guards {
                    guards.iter().any(|(name, _, _)| name == stream)
                } else {
                    advances.iter().any(|(name, _, _)| name == stream)
                }
            })
            .cloned()
            .collect();
        let mut report = self.apply_stream_changes(&changes)?;
        for (stream, id, to) in advances {
            self.advance_cursor(&stream, id, to)?;
            report.cursors.insert(stream, to);
        }
        Ok(report)
    }
}
