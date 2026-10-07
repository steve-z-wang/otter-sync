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
    Listen {
        stream: String,
    },
    /// Send this frame on the socket (the acknowledgement or a page).
    Send {
        frame: String,
    },
    /// Run `pull(owner, cursors, models)` in a transaction and report the page
    /// as [`LiveEvent::Pulled`]. At most one pull is outstanding per session;
    /// `models` are the session's declared read contracts.
    /// Existing pull carrier with a strict immutable v04 DeltaIntent.
    PullV04 {
        request: String,
    },
    PullV05 {
        request: String,
    },
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Live05Progress {
    header: axton_core::v05::DeliveryHeader,
    next_unit: u64,
    parts: BTreeSet<u64>,
}

/// The per-socket state machine. Registration precedes the acknowledgement,
/// and every stream is drained once from its negotiated head, so a commit
/// landing between negotiation and registration is caught by that first
/// drain. One pull is outstanding at a time and covers every stream with a
/// pending commit; a page that leaves a stream below its head, or a commit
/// observed during the pull, queues the next pull.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Subscriptions {
    #[serde(default)]
    context05: Option<axton_core::v05::RequestContext>,
    #[serde(default)]
    progress05: Option<Live05Progress>,
    streams: Vec<StreamState>,
    /// The cursors the outstanding pull was asked for, if one is outstanding.
    running: Option<BTreeMap<String, u64>>,
    closed: bool,
    models: BTreeMap<String, u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    context: Option<axton_core::v04::RequestContext>,
}

impl Subscriptions {
    pub fn open05(
        context: axton_core::v05::RequestContext,
        head: u64,
        response: String,
    ) -> (Self, Vec<LiveAction>) {
        let stream = context.stream.clone();
        let request = serde_json::to_string(&axton_core::v05::DeltaRequest {
            context: context.clone(),
            after: head,
            through: head,
            bootstrap: false,
            continuation: None,
        })
        .unwrap();
        (
            Self {
                streams: vec![StreamState {
                    stream: stream.clone(),
                    cursor: head,
                    pending: false,
                }],
                running: Some(BTreeMap::from([(stream.clone(), head)])),
                closed: false,
                models: BTreeMap::new(),
                context: None,
                context05: Some(context),
                progress05: None,
            },
            vec![
                LiveAction::Listen { stream },
                LiveAction::Send { frame: response },
                LiveAction::PullV05 { request },
            ],
        )
    }
    fn handle05(&mut self, event: LiveEvent) -> Result<Vec<LiveAction>> {
        use axton_core::v05;
        if self.closed {
            return Ok(vec![]);
        }
        let context = self.context05.clone().unwrap();
        match event {
            LiveEvent::Closed => {
                self.closed = true;
                self.progress05 = None;
                Ok(vec![])
            }
            LiveEvent::Committed { stream } => {
                self.stream_mut(&stream)?.pending = true;
                if self.running.is_some() {
                    return Ok(vec![]);
                }
                self.streams[0].pending = false;
                let after = self.streams[0].cursor;
                self.running = Some(BTreeMap::from([(stream, after)]));
                let request = serde_json::to_string(&v05::DeltaRequest {
                    context,
                    after,
                    through: after,
                    bootstrap: false,
                    continuation: None,
                })
                .map_err(crate::internal)?;
                Ok(vec![LiveAction::PullV05 { request }])
            }
            LiveEvent::Pulled { page } => {
                let d: v05::DeliveryResponse = serde_json::from_str(&page)
                    .map_err(|e| Error::new(code::LIVE_INVALID_PAGE, e.to_string()))?;
                if self.running.is_none()
                    || d.header.context != context
                    || d.header.bootstrap
                    || d.header.owner.is_some()
                    || d.header.after
                        != self.running.as_ref().unwrap().get(&context.stream).copied()
                {
                    return Err(Error::code(code::LIVE_INVALID_PAGE));
                }
                if self.progress05.is_none() {
                    use v05::Validate;
                    d.header.validate().map_err(crate::storage_invalid)?;
                    self.progress05 = Some(Live05Progress {
                        header: d.header.clone(),
                        next_unit: 0,
                        parts: BTreeSet::new(),
                    });
                }
                let progress = self.progress05.as_mut().unwrap();
                if progress.header.plan_id != d.header.plan_id
                    || progress.header.digest != d.header.digest
                {
                    return Err(Error::code(code::LIVE_INVALID_PAGE));
                }
                for part in &d.parts {
                    if part.unit != progress.next_unit {
                        return Err(Error::code(code::LIVE_INVALID_PAGE));
                    }
                    crate::delivery_plan::verify_indexed_part(&progress.header, part)?;
                    progress.parts.insert(part.part);
                    let manifest = &progress.header.units[progress.next_unit as usize];
                    if progress.parts.len() == manifest.parts.len() {
                        if let Some(through) = manifest.through {
                            self.streams[0].cursor = through;
                        }
                        progress.next_unit += 1;
                        progress.parts.clear();
                    }
                }
                let page = serde_json::to_string(&v05::DeliveryResponse {
                    header: progress.header.clone(),
                    parts: d.parts,
                })
                .map_err(crate::internal)?;
                let mut actions = vec![LiveAction::Send { frame: page }];
                if progress.next_unit == progress.header.units.len() as u64 {
                    self.progress05 = None;
                    self.running = None;
                    if self.streams[0].pending {
                        actions.extend(self.handle05(LiveEvent::Committed {
                            stream: context.stream,
                        })?);
                    }
                } else {
                    let manifest = &progress.header.units[progress.next_unit as usize];
                    let part = (0..manifest.parts.len() as u64)
                        .find(|p| !progress.parts.contains(p))
                        .ok_or_else(|| Error::code(code::LIVE_INVALID_PAGE))?;
                    let r = v05::DeltaRequest {
                        context,
                        after: progress.header.after.unwrap(),
                        through: progress.header.through.unwrap(),
                        bootstrap: false,
                        continuation: Some(v05::Continuation {
                            plan_id: progress.header.plan_id.clone(),
                            digest: progress.header.digest.clone(),
                            unit: progress.next_unit,
                            part,
                        }),
                    };
                    actions.push(LiveAction::PullV05 {
                        request: serde_json::to_string(&r).map_err(crate::internal)?,
                    });
                }
                Ok(actions)
            }
        }
    }

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
                context05: None,
                progress05: None,
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
        if self.context05.is_some() {
            return self.handle05(event);
        }
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

pub async fn negotiate05(
    config: &crate::Config,
    owner: &str,
    bytes: &[u8],
    host: &impl Host,
) -> Result<(Subscriptions, Vec<LiveAction>)> {
    let response = crate::handshake05(config, owner, bytes, host).await?;
    let ack: axton_core::v05::HandshakeResponse =
        axton_core::v05::decode(response.as_bytes()).map_err(crate::internal)?;
    let context = axton_core::v05::RequestContext {
        protocol: 5,
        store_id: ack.store_id,
        stream: ack.stream,
        materialization: crate::materialization_id05(
            config,
            config
                .protocol5
                .as_ref()
                .map(|p| p.projection_generation.as_str())
                .unwrap_or("1"),
        )?,
    };
    Ok(Subscriptions::open05(context, ack.head, response))
}
