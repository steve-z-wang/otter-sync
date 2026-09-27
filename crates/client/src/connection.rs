//! Transient connection lifecycle. The host supplies clock, entropy, timers and network I/O.
use serde::Serialize;
#[derive(Default)]
pub struct ConnectionDriver {
    running: bool,
    paused: bool,
    dirty: bool,
    in_flight: bool,
    attempt: u32,
    due: u64,
}
#[derive(Debug, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ConnectionAction {
    Idle,
    Sync,
    Wait { millis: u64 },
}
impl ConnectionDriver {
    pub fn start(&mut self, now: u64) {
        *self = Self {
            running: true,
            dirty: true,
            due: now,
            ..Default::default()
        };
    }
    pub fn stop(&mut self) {
        *self = Self::default();
    }
    /// The work the lane was scheduling is gone - the replica under it was
    /// replaced - but the application's intent is not: a running lane stays
    /// running with a sync due at once, a paused one stays paused, and a
    /// stopped one stays stopped. Nothing is in flight and no failure is
    /// counted any more, since the attempts belonged to what was replaced.
    pub fn restart(&mut self) {
        *self = Self {
            running: self.running,
            paused: self.paused,
            dirty: self.running,
            ..Default::default()
        };
    }
    /// Whether the lane was started and not stopped since, paused or not.
    pub fn running(&self) -> bool {
        self.running
    }
    pub fn pause(&mut self) {
        self.paused = true;
    }
    pub fn resume(&mut self, now: u64) {
        self.paused = false;
        self.dirty = true;
        self.due = now;
    }
    pub fn wake(&mut self) {
        self.dirty = true;
    }
    /// Whether the lane may do work at all: started and not paused. Work that
    /// rides on the lane without riding on its socket - a Bootstrap page
    /// ([Downlink worker](../../../docs/engineering/architecture/client/connection/controller/downlink-worker.md))
    /// - is scheduled only while this holds.
    pub fn active(&self) -> bool {
        self.running && !self.paused
    }
    /// Whether a started lane is paused.
    pub fn paused(&self) -> bool {
        self.running && self.paused
    }
    /// How long to wait before the attempt after `attempt` failed ones: 250 ms
    /// doubling per attempt, capped at 30 s, with ±20 % jitter from host
    /// entropy. One policy, so every retry on the lane is bounded the same way.
    pub fn backoff(attempt: u32, entropy: u64) -> u64 {
        let base = 250u64.saturating_mul(1u64 << attempt.min(7)).min(30_000);
        base * (800 + entropy % 401) / 1000
    }
    pub fn next(&mut self, now: u64) -> ConnectionAction {
        if !self.running || self.paused || self.in_flight || !self.dirty {
            return ConnectionAction::Idle;
        }
        if self.due > now {
            return ConnectionAction::Wait {
                millis: self.due - now,
            };
        }
        self.dirty = false;
        self.in_flight = true;
        ConnectionAction::Sync
    }
    pub fn complete(&mut self, success: bool, now: u64, entropy: u64) {
        if !self.running || !self.in_flight {
            return;
        }
        self.in_flight = false;
        if success {
            self.attempt = 0;
            self.due = now;
        } else {
            let delay = Self::backoff(self.attempt, entropy);
            self.attempt = self.attempt.saturating_add(1);
            self.due = now.saturating_add(delay);
            self.dirty = true;
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn lifecycle_keeps_writes_during_request_and_pauses_work() {
        let mut d = ConnectionDriver::default();
        d.start(10);
        assert_eq!(d.next(10), ConnectionAction::Sync);
        d.wake();
        assert_eq!(d.next(10), ConnectionAction::Idle);
        d.complete(true, 12, 0);
        assert_eq!(d.next(12), ConnectionAction::Sync);
        d.pause();
        d.complete(true, 13, 0);
        d.wake();
        assert_eq!(d.next(99), ConnectionAction::Idle);
        d.resume(100);
        assert_eq!(d.next(100), ConnectionAction::Sync);
        d.stop();
        d.complete(false, 101, 0);
        assert_eq!(d.next(1000), ConnectionAction::Idle);
    }
    #[test]
    fn restart_keeps_the_intent_and_forgets_the_attempt_in_flight() {
        let mut d = ConnectionDriver::default();
        d.start(0);
        assert_eq!(d.next(0), ConnectionAction::Sync);
        d.complete(false, 0, 0);
        d.restart();
        assert!(d.running());
        assert_eq!(
            d.next(0),
            ConnectionAction::Sync,
            "due at once: neither the attempt in flight nor its backoff survives"
        );
        d.restart();
        assert_eq!(d.next(0), ConnectionAction::Sync, "nothing is in flight");
        d.pause();
        d.restart();
        assert_eq!(d.next(0), ConnectionAction::Idle, "still paused");
        d.resume(5);
        assert_eq!(d.next(5), ConnectionAction::Sync);
        d.stop();
        d.restart();
        assert!(!d.running());
        assert_eq!(d.next(5), ConnectionAction::Idle, "still stopped");
    }
    #[test]
    fn retry_backoff_is_bounded_and_wake_does_not_busy_loop() {
        let mut d = ConnectionDriver::default();
        d.start(0);
        let mut now = 0;
        for _ in 0..40 {
            assert_eq!(d.next(now), ConnectionAction::Sync);
            d.complete(false, now, 200);
            d.wake();
            let ConnectionAction::Wait { millis } = d.next(now) else {
                panic!("retry must wait")
            };
            assert!((250..=30_000).contains(&millis));
            now += millis;
        }
        assert_eq!(d.next(now), ConnectionAction::Sync);
        d.complete(true, now, 0);
        assert_eq!(d.next(now), ConnectionAction::Idle);
    }
}
