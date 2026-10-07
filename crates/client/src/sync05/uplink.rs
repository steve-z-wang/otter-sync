//! The Store worker protocol. Commands contain admitted facts, never sockets.
use super::DeliveryQueue;
use crate::{ApplyReport, store05::StoreStatus05, v05};
pub enum StoreCommand {
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
    Snapshot,
    Needs,
}
pub enum StoreReport {
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
    },
}
