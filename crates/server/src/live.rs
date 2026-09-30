//! The live subscription controller: negotiation, page validation, and the
//! per-socket drain policy ([`Subscriptions`]). The host owns the socket, the
//! commit hub and the database; it feeds [`LiveEvent`]s and executes the
//! [`LiveAction`]s it gets back, keeping no sync decision of its own.
use crate::{Error, Host, Result, code, head, principal, process_pull};
use axton_core::{CursorRange, PullPage, PullRequest, SubscribeRequest, SubscriptionAck};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Serialize)]
pub struct Negotiation {
    /// The acknowledgement frame: every channel's head.
    pub response: String,
    /// The accepted channels with their heads at negotiation.
    pub heads: BTreeMap<String, u64>,
    /// The read contracts the client declared; every page of the session is
    /// pulled at these versions.
    pub models: BTreeMap<String, u64>,
}

#[derive(Debug, Serialize)]
pub struct PageProgress {
    pub page: String,
    pub cursors: BTreeMap<String, CursorRange>,
}

/// The subscribe frame's shape and channel normalization are protocol rules
/// ([`SubscribeRequest`]); this maps their refusal to the request code.
pub fn decode_subscribe(bytes: &[u8]) -> Result<SubscribeRequest> {
    SubscribeRequest::decode(bytes).map_err(|e| Error::new(code::REQUEST_INVALID, e.to_string()))
}

pub async fn negotiate(
    config: &crate::Config,
    owner: &str,
    bytes: &[u8],
    host: &impl Host,
) -> Result<Negotiation> {
    principal(owner)?;
    let request = decode_subscribe(bytes)?;
    config.check_declared(&request.models)?;
    let mut heads = BTreeMap::new();
    for channel in &request.channels {
        heads.insert(channel.clone(), head(host, channel).await?);
    }
    let ack = SubscriptionAck::new(heads.clone())
        .and_then(|ack| ack.encode())
        .map_err(|error| Error::new(code::INTERNAL, error.to_string()))?;
    let response =
        String::from_utf8(ack).map_err(|error| Error::new(code::INTERNAL, error.to_string()))?;
    Ok(Negotiation {
        response,
        heads,
        models: request.models,
    })
}

/// A page the host pulled must answer exactly the cursors that were asked:
/// the same channels, each starting at its requested cursor.
pub fn page_progress(page: &str, expected: &BTreeMap<String, u64>) -> Result<PageProgress> {
    let invalid = |m: String| Error::new(code::LIVE_INVALID_PAGE, m);
    let decoded = PullPage::decode(page.as_bytes())
        .map_err(|e| invalid(format!("invalid live page: {e}")))?;
    if !decoded.cursors.keys().eq(expected.keys()) {
        return Err(invalid("invalid live page channels".into()));
    }
    for (channel, range) in &decoded.cursors {
        if range.from != expected[channel] {
            return Err(invalid(format!(
                "invalid live page progression on {channel}"
            )));
        }
    }
    Ok(PageProgress {
        page: page.into(),
        cursors: decoded.cursors,
    })
}

pub async fn pull(
    config: &crate::Config,
    owner: &str,
    cursors: &BTreeMap<String, u64>,
    models: &BTreeMap<String, u64>,
    host: &impl Host,
) -> Result<PageProgress> {
    let request = PullRequest {
        models: models.clone(),
        cursors: cursors.clone(),
    }
    .encode()
    .map_err(|error| Error::new(code::REQUEST_INVALID, error.to_string()))?;
    let page = process_pull(config, owner, &request, host).await?;
    page_progress(&page, cursors)
}

/// What the host reports to a socket's [`Subscriptions`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum LiveEvent {
    /// A transaction that touched `scope` committed (from the host's commit hub).
    Committed { scope: String },
    /// The host finished the pull a [`LiveAction::Pull`] asked for; `page` is
    /// the page text `pull` returned.
    Pulled { page: String },
    /// The socket closed or failed; nothing more will be sent.
    Closed,
}

/// What the host executes, in order, for one event.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum LiveAction {
    /// Register a commit listener for `scope`; the host reports each commit as
    /// [`LiveEvent::Committed`]. Issued before anything is sent.
    Listen { scope: String },
    /// Send this frame on the socket (the acknowledgement or a page).
    Send { frame: String },
    /// Run `pull(owner, cursors, models)` in a transaction and report the page
    /// as [`LiveEvent::Pulled`]. At most one pull is outstanding per session;
    /// `models` are the session's declared read contracts.
    Pull {
        cursors: BTreeMap<String, u64>,
        models: BTreeMap<String, u64>,
    },
}

/// One accepted scope: the cursor streamed so far and whether a commit
/// arrived that has not been pulled yet.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScopeState {
    pub scope: String,
    pub cursor: u64,
    pub pending: bool,
}

/// The per-socket state machine. Registration precedes the acknowledgement,
/// and every scope is drained once from its negotiated head, so a commit
/// landing between negotiation and registration is caught by that first
/// drain. One pull is outstanding at a time and covers every scope with a
/// pending commit; a page that leaves a scope below its head, or a commit
/// observed during the pull, queues the next pull.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Subscriptions {
    scopes: Vec<ScopeState>,
    /// The cursors the outstanding pull was asked for, if one is outstanding.
    running: Option<BTreeMap<String, u64>>,
    closed: bool,
    models: BTreeMap<String, u64>,
}

impl Subscriptions {
    /// Starts the session for a negotiation: `[Listen …, Send ack, Pull all]`.
    pub fn open(negotiation: Negotiation) -> (Self, Vec<LiveAction>) {
        let scopes: Vec<ScopeState> = negotiation
            .heads
            .iter()
            .map(|(scope, head)| ScopeState {
                scope: scope.clone(),
                cursor: *head,
                pending: false,
            })
            .collect();
        let mut actions: Vec<LiveAction> = scopes
            .iter()
            .map(|state| LiveAction::Listen {
                scope: state.scope.clone(),
            })
            .collect();
        actions.push(LiveAction::Send {
            frame: negotiation.response,
        });
        let cursors: BTreeMap<String, u64> = scopes
            .iter()
            .map(|state| (state.scope.clone(), state.cursor))
            .collect();
        actions.push(LiveAction::Pull {
            cursors: cursors.clone(),
            models: negotiation.models.clone(),
        });
        (
            Self {
                scopes,
                running: Some(cursors),
                closed: false,
                models: negotiation.models,
            },
            actions,
        )
    }

    /// Applies one event and answers the actions it calls for. An event the
    /// session cannot accept (an unknown scope, or a `Pulled` no pull is
    /// outstanding for) is a host defect reported as `live.invalid_event`; an
    /// invalid page progression is `live.invalid_page`. The host reports either
    /// and closes the socket.
    pub fn handle(&mut self, event: LiveEvent) -> Result<Vec<LiveAction>> {
        self.handle_page(event, false)
    }

    /// Apply a channel-aware live page without projecting away removals.
    pub fn handle_channel(&mut self, event: LiveEvent) -> Result<Vec<LiveAction>> {
        self.handle_page(event, true)
    }

    fn handle_page(&mut self, event: LiveEvent, channel: bool) -> Result<Vec<LiveAction>> {
        match event {
            LiveEvent::Committed { scope } => {
                self.scope_mut(&scope)?.pending = true;
                if self.closed || self.running.is_some() {
                    return Ok(vec![]);
                }
                Ok(self.next_pull().into_iter().collect())
            }
            LiveEvent::Pulled { page } => {
                let Some(asked) = self.running.take() else {
                    return Err(Error::new(
                        code::LIVE_INVALID_EVENT,
                        "no pull is outstanding",
                    ));
                };
                if self.closed {
                    return Ok(vec![]);
                }
                let progress = if channel {
                    channel_page_progress(&page, &asked)?
                } else {
                    page_progress(&page, &asked)?
                };
                let mut actions = vec![];
                let advanced = progress.cursors.values().any(|range| range.to > range.from);
                if advanced {
                    actions.push(LiveAction::Send {
                        frame: progress.page,
                    });
                }
                for (scope, range) in &progress.cursors {
                    let state = self.scope_mut(scope)?;
                    state.cursor = range.to;
                    if range.continues() {
                        state.pending = true;
                    }
                }
                actions.extend(self.next_pull());
                Ok(actions)
            }
            LiveEvent::Closed => {
                self.closed = true;
                for state in &mut self.scopes {
                    state.pending = false;
                }
                Ok(vec![])
            }
        }
    }

    /// The pull for every pending scope, clearing their flags; none when
    /// nothing is pending.
    fn next_pull(&mut self) -> Option<LiveAction> {
        let pending: BTreeSet<String> = self
            .scopes
            .iter()
            .filter(|state| state.pending)
            .map(|state| state.scope.clone())
            .collect();
        if pending.is_empty() {
            return None;
        }
        let mut cursors = BTreeMap::new();
        for state in &mut self.scopes {
            if pending.contains(&state.scope) {
                state.pending = false;
                cursors.insert(state.scope.clone(), state.cursor);
            }
        }
        self.running = Some(cursors.clone());
        Some(LiveAction::Pull {
            cursors,
            models: self.models.clone(),
        })
    }

    /// The accepted scopes in acknowledgement order, with their drain state.
    pub fn scopes(&self) -> &[ScopeState] {
        &self.scopes
    }

    /// Whether a pull is outstanding.
    pub fn is_pulling(&self) -> bool {
        self.running.is_some()
    }

    /// Whether `Closed` was observed; nothing is sent afterwards.
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    fn scope_mut(&mut self, scope: &str) -> Result<&mut ScopeState> {
        self.scopes
            .iter_mut()
            .find(|state| state.scope == scope)
            .ok_or_else(|| {
                Error::new(
                    code::LIVE_INVALID_EVENT,
                    format!("scope {scope} is not subscribed"),
                )
            })
    }
}

/// Validate channel-aware live progress, preserving the complete frame.
pub fn channel_page_progress(page: &str, expected: &BTreeMap<String, u64>) -> Result<PageProgress> {
    let invalid = |m| Error::new(code::LIVE_INVALID_PAGE, m);
    let decoded =
        axton_core::ChannelPullPage::decode(page.as_bytes()).map_err(|e| invalid(e.to_string()))?;
    if !decoded.cursors.keys().eq(expected.keys())
        || decoded
            .cursors
            .iter()
            .any(|(channel, range)| range.from != expected[channel])
    {
        return Err(invalid("invalid live page progression".into()));
    }
    Ok(PageProgress {
        page: page.into(),
        cursors: decoded.cursors,
    })
}

/// Pull channel membership events for a live session in the host transaction.
pub async fn channel_pull(
    config: &crate::Config,
    owner: &str,
    cursors: &BTreeMap<String, u64>,
    models: &BTreeMap<String, u64>,
    host: &impl Host,
) -> Result<PageProgress> {
    let request = PullRequest {
        models: models.clone(),
        cursors: cursors.clone(),
    }
    .encode()
    .map_err(|e| Error::new(code::REQUEST_INVALID, e.to_string()))?;
    let page = crate::process_channel_pull(config, owner, &request, host).await?;
    channel_page_progress(&page, cursors)
}
