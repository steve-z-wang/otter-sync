//! The socket session: the wire subscription the [`DownlinkWorker`] asked for,
//! the epoch that fences its frames, and the handshake order. It owns no queue,
//! no cursor and no client: what a frame means and when to catch up belongs to
//! the worker
//! ([Live session](../../../docs/engineering/architecture/client/connection/controller/live-session.md)).
use crate::*;

/// One socket attempt.
struct Session {
    epoch: u64,
    /// The subscription generation the channels were snapshotted under.
    generation: u64,
    /// What the socket asked for; the acknowledgement must confirm exactly it.
    subscribe: SubscribeRequest,
    acknowledged: bool,
}

/// The identifier after `last`, for a fence the host echoes back: a socket
/// epoch or a Downlink request id. Each crosses the binding as a JSON number,
/// so none beyond the safe integer range is ever issued; an exhausted counter
/// fails the pump instead of wrapping onto an identifier already used, which
/// would let an abandoned socket or request answer for a new one.
pub(crate) fn allocate(last: u64, what: &str) -> Result<u64> {
    last.checked_add(1)
        .filter(|next| *next <= MAX_SAFE_INTEGER)
        .ok_or_else(|| invalid(format!("{what} exhausted")))
}

/// One socket session at a time, each with its own epoch.
#[derive(Default)]
pub struct LiveSession {
    epoch: u64,
    session: Option<Session>,
}

impl LiveSession {
    /// Begin a session for `channels` under `generation`: the epoch that fences
    /// its frames and the subscribe frame the host sends once the socket opens.
    pub fn begin(
        &mut self,
        channels: Vec<String>,
        models: BTreeMap<String, u64>,
        generation: u64,
    ) -> Result<(u64, String)> {
        let subscribe = SubscribeRequest::new(channels, models)?;
        let frame = String::from_utf8(subscribe.encode()?).map_err(|_| invalid("utf8"))?;
        self.epoch = allocate(self.epoch, "socket epoch")?;
        self.session = Some(Session {
            epoch: self.epoch,
            generation,
            subscribe,
            acknowledged: false,
        });
        Ok((self.epoch, frame))
    }
    /// Whether a session is open.
    pub fn open(&self) -> bool {
        self.session.is_some()
    }
    /// Whether `epoch` names the session that is open: whatever an abandoned
    /// socket still delivers belongs to no session.
    pub fn current(&self, epoch: u64) -> bool {
        self.session.as_ref().is_some_and(|s| s.epoch == epoch)
    }
    /// The subscription generation the open session subscribed under.
    pub fn generation(&self) -> Option<u64> {
        self.session.as_ref().map(|s| s.generation)
    }
    /// Whether the handshake completed and the stream is in order.
    pub fn acknowledged(&self) -> bool {
        self.session.as_ref().is_some_and(|s| s.acknowledged)
    }
    /// End the session: the epoch of the socket the host closes, if one is open.
    pub fn close(&mut self) -> Option<u64> {
        self.session.take().map(|s| s.epoch)
    }
    /// The acknowledgement in handshake order: the first one of the session, for
    /// exactly the channels it subscribed. From here pages are in order.
    pub fn acknowledge(&mut self, ack: &SubscriptionAck) -> Result<()> {
        let Some(session) = self.session.as_mut() else {
            return Err(invalid("invalid live subscription acknowledgement"));
        };
        if session.acknowledged || !ack.confirms(&session.subscribe) {
            return Err(invalid("invalid live subscription acknowledgement"));
        }
        session.acknowledged = true;
        Ok(())
    }
    /// Whether a streamed page is in order: only an acknowledged session streams.
    pub fn streamed(&self) -> Result<()> {
        if !self.acknowledged() {
            return Err(invalid("live page before acknowledgement"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn an_exhausted_epoch_fails_the_session_instead_of_wrapping() {
        assert_eq!(allocate(0, "socket epoch").unwrap(), 1);
        assert_eq!(
            allocate(MAX_SAFE_INTEGER - 1, "socket epoch").unwrap(),
            MAX_SAFE_INTEGER
        );
        assert!(allocate(u64::MAX, "request id").is_err());
        let mut live = LiveSession {
            epoch: MAX_SAFE_INTEGER,
            session: None,
        };
        let error = live
            .begin(vec!["a".into()], BTreeMap::from([("Entry".into(), 1)]), 1)
            .unwrap_err();
        assert!(
            error.to_string().contains("socket epoch exhausted"),
            "{error}"
        );
        assert!(!live.open(), "no session began");
        assert_eq!(live.epoch, MAX_SAFE_INTEGER, "the counter did not move");
    }
}
