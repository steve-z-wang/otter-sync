//! The live subscription controller: negotiation, page validation, and the
//! per-socket drain policy ([`Subscriptions`]). The host owns the socket, the
//! commit hub and the database; it feeds [`LiveEvent`]s and executes the
//! [`LiveAction`]s it gets back, keeping no sync decision of its own.
use crate::{Error, Host, Result, code, head, principal};
use axton_core::{CursorRange, PullRequest, SubscribeRequest, SubscriptionAck};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Serialize)]
pub struct Negotiation {
    /// The acknowledgement frame: every stream's head.
    pub response: String,
    /// The accepted streams with their heads at negotiation.
    pub heads: BTreeMap<String, u64>,
    /// The read contracts the client declared; every page of the session is
    /// pulled at these versions.
    pub models: BTreeMap<String, u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<axton_core::v04::RequestContext>,
}

#[derive(Debug, Serialize)]
pub struct PageProgress {
    pub page: String,
    pub cursors: BTreeMap<String, CursorRange>,
}

/// The subscribe frame's shape and stream normalization are protocol rules
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
    if crate::protocol_v04::is_request(bytes) {
        let request: axton_core::v04::SubscribeIntent =
            axton_core::v04::decode(bytes).map_err(crate::request_invalid)?;
        crate::protocol_v04::admit(config, owner, &request.context, false, host).await?;
        if crate::protocol_v04::models(config, &request.context)? != request.models {
            return Err(Error::code("context_mismatch"));
        }
        let head = head(host, &request.context.binding.stream).await?;
        if request.cursor > head {
            return Err(crate::request_invalid("cursor ahead of head"));
        }
        let response = String::from_utf8(
            axton_core::v04::encode(&axton_core::v04::SubscribeAcknowledged {
                context: request.context.clone(),
                cursor: request.cursor,
                head,
            })
            .map_err(crate::internal)?,
        )
        .map_err(crate::internal)?;
        return Ok(Negotiation {
            response,
            heads: BTreeMap::from([(request.context.binding.stream.clone(), request.cursor)]),
            models: request.models,
            context: Some(request.context),
        });
    }
    crate::admit_protocol(bytes)?;
    principal(owner)?;
    let request = decode_subscribe(bytes)?;
    config.check_declared(&request.models)?;
    let mut heads = BTreeMap::new();
    for stream in &request.streams {
        heads.insert(stream.clone(), head(host, stream).await?);
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
        context: None,
    })
}

/// A page the host pulled must answer exactly the cursors that were asked:
/// the same streams, each starting at its requested cursor.
pub fn page_progress(page: &str, expected: &BTreeMap<String, u64>) -> Result<PageProgress> {
    stream_page_progress(page, expected)
}

pub async fn pull(
    config: &crate::Config,
    owner: &str,
    cursors: &BTreeMap<String, u64>,
    models: &BTreeMap<String, u64>,
    host: &impl Host,
) -> Result<PageProgress> {
    stream_pull(config, owner, cursors, models, host).await
}

/// What the host reports to a socket's [`Subscriptions`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum LiveEvent {
    /// A transaction that touched `stream` committed (from the host's commit hub).
    Committed { stream: String },
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
    /// Register a commit listener for `stream`; the host reports each commit as
    /// [`LiveEvent::Committed`]. Issued before anything is sent.
    Listen { stream: String },
    /// Send this frame on the socket (the acknowledgement or a page).
    Send { frame: String },
    /// Run `pull(owner, cursors, models)` in a transaction and report the page
    /// as [`LiveEvent::Pulled`]. At most one pull is outstanding per session;
    /// `models` are the session's declared read contracts.
    /// Existing pull carrier with a strict immutable v04 DeltaIntent.
    PullV04 { request: String },
    Pull {
        cursors: BTreeMap<String, u64>,
        models: BTreeMap<String, u64>,
    },
}

/// One accepted stream: the cursor streamed so far and whether a commit
/// arrived that has not been pulled yet.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamState {
    pub stream: String,
    pub cursor: u64,
    pub pending: bool,
}

/// The per-socket state machine. Registration precedes the acknowledgement,
/// and every stream is drained once from its negotiated head, so a commit
/// landing between negotiation and registration is caught by that first
/// drain. One pull is outstanding at a time and covers every stream with a
/// pending commit; a page that leaves a stream below its head, or a commit
/// observed during the pull, queues the next pull.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Subscriptions {
    streams: Vec<StreamState>,
    /// The cursors the outstanding pull was asked for, if one is outstanding.
    running: Option<BTreeMap<String, u64>>,
    closed: bool,
    models: BTreeMap<String, u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    context: Option<axton_core::v04::RequestContext>,
}

impl Subscriptions {
    /// Starts the session for a negotiation: `[Listen …, Send ack, Pull all]`.
    pub fn open(negotiation: Negotiation) -> (Self, Vec<LiveAction>) {
        let streams: Vec<StreamState> = negotiation
            .heads
            .iter()
            .map(|(stream, head)| StreamState {
                stream: stream.clone(),
                cursor: *head,
                pending: false,
            })
            .collect();
        let mut actions: Vec<LiveAction> = streams
            .iter()
            .map(|state| LiveAction::Listen {
                stream: state.stream.clone(),
            })
            .collect();
        actions.push(LiveAction::Send {
            frame: negotiation.response,
        });
        let cursors: BTreeMap<String, u64> = streams
            .iter()
            .map(|state| (state.stream.clone(), state.cursor))
            .collect();
        actions.push(pull_action(
            &cursors,
            &negotiation.models,
            negotiation.context.as_ref(),
        ));
        (
            Self {
                streams,
                running: Some(cursors),
                closed: false,
                models: negotiation.models,
                context: negotiation.context,
            },
            actions,
        )
    }

    /// Applies one event and answers the actions it calls for. An event the
    /// session cannot accept (an unknown stream, or a `Pulled` no pull is
    /// outstanding for) is a host defect reported as `live.invalid_event`; an
    /// invalid page progression is `live.invalid_page`. The host reports either
    /// and closes the socket.
    pub fn handle(&mut self, event: LiveEvent) -> Result<Vec<LiveAction>> {
        self.handle_page(event)
    }

    /// Apply a stream-aware live page without projecting away removals.
    pub fn handle_stream(&mut self, event: LiveEvent) -> Result<Vec<LiveAction>> {
        self.handle_page(event)
    }

    fn handle_page(&mut self, event: LiveEvent) -> Result<Vec<LiveAction>> {
        match event {
            LiveEvent::Committed { stream } => {
                self.stream_mut(&stream)?.pending = true;
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
                let progress = if let Some(context) = &self.context {
                    let decoded: axton_core::v04::DeltaPage =
                        axton_core::v04::decode(page.as_bytes())
                            .map_err(|e| Error::new(code::LIVE_INVALID_PAGE, e.to_string()))?;
                    if &decoded.context != context
                        || asked.len() != 1
                        || asked.get(&context.binding.stream) != Some(&decoded.from)
                    {
                        return Err(Error::code(code::LIVE_INVALID_PAGE));
                    }
                    PageProgress {
                        page,
                        cursors: BTreeMap::from([(
                            context.binding.stream.clone(),
                            CursorRange {
                                from: decoded.from,
                                to: decoded.to,
                                head: decoded.head,
                            },
                        )]),
                    }
                } else {
                    stream_page_progress(&page, &asked)?
                };
                let mut actions = vec![];
                let advanced = progress.cursors.values().any(|range| range.to > range.from);
                if advanced {
                    actions.push(LiveAction::Send {
                        frame: progress.page,
                    });
                }
                for (stream, range) in &progress.cursors {
                    let state = self.stream_mut(stream)?;
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
                for state in &mut self.streams {
                    state.pending = false;
                }
                Ok(vec![])
            }
        }
    }

    /// The pull for every pending stream, clearing their flags; none when
    /// nothing is pending.
    fn next_pull(&mut self) -> Option<LiveAction> {
        let pending: BTreeSet<String> = self
            .streams
            .iter()
            .filter(|state| state.pending)
            .map(|state| state.stream.clone())
            .collect();
        if pending.is_empty() {
            return None;
        }
        let mut cursors = BTreeMap::new();
        for state in &mut self.streams {
            if pending.contains(&state.stream) {
                state.pending = false;
                cursors.insert(state.stream.clone(), state.cursor);
            }
        }
        self.running = Some(cursors.clone());
        Some(pull_action(&cursors, &self.models, self.context.as_ref()))
    }

    /// The accepted streams in acknowledgement order, with their drain state.
    pub fn streams(&self) -> &[StreamState] {
        &self.streams
    }

    /// Whether a pull is outstanding.
    pub fn is_pulling(&self) -> bool {
        self.running.is_some()
    }

    /// Whether `Closed` was observed; nothing is sent afterwards.
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    fn stream_mut(&mut self, stream: &str) -> Result<&mut StreamState> {
        self.streams
            .iter_mut()
            .find(|state| state.stream == stream)
            .ok_or_else(|| {
                Error::new(
                    code::LIVE_INVALID_EVENT,
                    format!("stream {stream} is not subscribed"),
                )
            })
    }
}

/// Validate stream-aware live progress, preserving the complete frame.
pub fn stream_page_progress(page: &str, expected: &BTreeMap<String, u64>) -> Result<PageProgress> {
    let invalid = |m| Error::new(code::LIVE_INVALID_PAGE, m);
    let decoded =
        axton_core::StreamPullPage::decode(page.as_bytes()).map_err(|e| invalid(e.to_string()))?;
    if !decoded.cursors.keys().eq(expected.keys())
        || decoded
            .cursors
            .iter()
            .any(|(stream, range)| range.from != expected[stream])
    {
        return Err(invalid("invalid live page progression".into()));
    }
    Ok(PageProgress {
        page: page.into(),
        cursors: decoded.cursors,
    })
}

/// Pull stream membership events for a live session in the host transaction.
pub async fn stream_pull(
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
    let request =
        axton_core::with_capabilities(&request, &[axton_core::STREAM_AUTHORITY_CAPABILITY])
            .map_err(crate::internal)?;
    let page = crate::process_stream_pull(config, owner, &request, host).await?;
    stream_page_progress(&page, cursors)
}

fn pull_action(
    cursors: &BTreeMap<String, u64>,
    models: &BTreeMap<String, u64>,
    context: Option<&axton_core::v04::RequestContext>,
) -> LiveAction {
    match context {
        None => LiveAction::Pull {
            cursors: cursors.clone(),
            models: models.clone(),
        },
        Some(context) => {
            let intent = axton_core::v04::DeltaIntent {
                context: context.clone(),
                call_id: uuid::Uuid::new_v4().to_string(),
                after: cursors[&context.binding.stream],
                models: models.clone(),
                limit: 1000,
            };
            LiveAction::PullV04 {
                request: String::from_utf8(
                    axton_core::v04::encode(&intent).expect("validated live context and cursor"),
                )
                .expect("JSON is UTF8"),
            }
        }
    }
}
