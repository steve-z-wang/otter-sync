//! The Store worker protocol. Commands contain admitted facts, never sockets.
use super::DeliveryQueue;
use crate::{ApplyReport, store05::StoreStatus05, v05};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
#[derive(Clone)]
pub struct WorkFence05 {
    pub context: v05::RequestContext,
    pub epoch: u64,
    pub(crate) current: Arc<AtomicU64>,
}
impl WorkFence05 {
    pub fn current(&self) -> bool {
        self.current.load(Ordering::SeqCst) == self.epoch
    }
}
pub enum StoreCommand {
    Guarded {
        fence: WorkFence05,
        command: Box<StoreCommand>,
    },
    Initialize(u64),
    NetworkState {
        live: bool,
        catching_up: bool,
    },
    Freeze,
    Acknowledge(v05::BatchAcknowledgement),
    Apply {
        plan_id: String,
        queue: DeliveryQueue,
        now: u64,
    },
    Cleanup(u64),
    Snapshot,
    Needs,
}
pub enum StoreReport {
    Guarded {
        fence: WorkFence05,
        report: Box<std::result::Result<StoreReport, String>>,
    },
    Obsolete,
    ActivePlans(Vec<String>),
    Snapshot(StoreStatus05),
    Needs {
        schema: Option<crate::store05::PendingSchema05>,
        settlements: Vec<crate::settlement05::PendingSettlement05>,
    },
    Frozen(Option<v05::MutationRequest>),
    Committed {
        status: StoreStatus05,
        report: ApplyReport,
        plan: Option<(String, u64)>,
        active_plans: Option<Vec<String>>,
    },
}
