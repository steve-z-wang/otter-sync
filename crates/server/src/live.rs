//! Single-Stream protocol-5 live drain controller.
use crate::{Error, Host, Result, code};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
/// What the host reports to a socket's [`Subscriptions`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum LiveEvent {
    /// A transaction that touched `stream` committed (from the host's commit hub).
    Committed { stream: String },
    /// The host finished the pull a [`LiveAction::PullV05`] asked for; `page` is
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
    /// Run this protocol-5 delivery request in a transaction and report its
    /// response as [`LiveEvent::Pulled`]. At most one pull is outstanding.
    PullV05 { request: String },
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
    header: axton_protocols::sync::DeliveryHeader,
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
    context05: Option<axton_protocols::sync::RequestContext>,
    #[serde(default)]
    progress05: Option<Live05Progress>,
    streams: Vec<StreamState>,
    /// The cursors the outstanding pull was asked for, if one is outstanding.
    running: Option<BTreeMap<String, u64>>,
    closed: bool,
}

impl Subscriptions {
    pub fn streams(&self) -> &[StreamState] {
        &self.streams
    }
    pub fn open05(
        context: axton_protocols::sync::RequestContext,
        head: u64,
        response: String,
    ) -> (Self, Vec<LiveAction>) {
        let stream = context.stream.clone();
        let request = serde_json::to_string(&axton_protocols::sync::DeltaRequest {
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
        use axton_protocols::sync as v05;
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
    pub fn handle(&mut self, event: LiveEvent) -> Result<Vec<LiveAction>> {
        self.handle05(event)
    }
    pub fn handle_stream(&mut self, event: LiveEvent) -> Result<Vec<LiveAction>> {
        self.handle05(event)
    }
}
pub async fn negotiate05(
    config: &crate::Config,
    owner: &str,
    bytes: &[u8],
    host: &impl Host,
) -> Result<(Subscriptions, Vec<LiveAction>)> {
    let response = crate::handshake05(config, owner, bytes, host).await?;
    let ack: axton_protocols::sync::HandshakeResponse =
        axton_protocols::sync::decode(response.as_bytes()).map_err(crate::internal)?;
    let context = axton_protocols::sync::RequestContext {
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
