//! Direct-read connection intent. Sync05 independently owns uplink/downlink effects.
use super::effects::Waiter;
use super::*;
pub(super) struct Connection {
    pub(super) timeout: u64,
    pub(super) refresh: bool,
    pub(super) paused: bool,
    pub(super) refreshing: Option<String>,
    pub(super) waiters: Vec<Waiter>,
}
impl<S: ClientStore + 'static> ClientRuntime<S> {
    pub(super) fn connect(
        &mut self,
        timeout: Option<u64>,
        refresh: bool,
        _now: u64,
        _entropy: u64,
    ) -> std::result::Result<Value, String> {
        if self.connection.is_some() {
            return Err("connection already active".into());
        }
        let timeout = timeout.unwrap_or(30_000);
        if !(1..=2_147_483_647).contains(&timeout) {
            return Err("directTimeoutMs must be an integer from 1 to 2147483647".into());
        }
        self.connection = Some(Connection {
            timeout,
            refresh,
            paused: false,
            refreshing: None,
            waiters: vec![],
        });
        self.prerequisites.wake();
        Ok(Value::Null)
    }
    pub(super) fn control(
        &mut self,
        event: ConnectionEvent,
        now: u64,
        entropy: u64,
    ) -> std::result::Result<Value, String> {
        if self.connection.is_none() {
            return Err("connection not active".into());
        }
        match event {
            ConnectionEvent::Stop => {
                self.release_initial_reads05(false);
                self.stop_lanes()
            }
            ConnectionEvent::Pause => self.connection.as_mut().unwrap().paused = true,
            ConnectionEvent::Resume => self.connection.as_mut().unwrap().paused = false,
            ConnectionEvent::Wake => self.wake_lanes(now, entropy),
        };
        Ok(Value::Null)
    }
    pub(super) fn stop_lanes(&mut self) {
        if let Some(refresh) = self.connection.as_ref().and_then(|c| c.refreshing.clone()) {
            self.cancel_effect(&refresh);
        }
        self.fail_directs_in_flight(direct::Failure::Unavailable);
        self.connection = None;
    }
    pub(super) fn wake_lanes(&mut self, _now: u64, _entropy: u64) {
        self.prerequisites.wake()
    }
    pub(super) fn close_lanes(&mut self) {
        self.connection = None;
    }
    pub(super) fn settled(&mut self, report: &crate::ApplyReport) {
        if !report.reports.is_empty() {
            self.report(Diagnostic::Records {
                reports: report.reports.clone(),
            });
        }
        for completion in &report.completions {
            self.events.push(Event::CallCompleted {
                call_id: completion.call_id.clone(),
                outcome: serde_json::to_value(&completion.outcome).unwrap_or(Value::Null),
            });
        }
    }
}
