//! Deterministic simulation of N clients and one server in one process.
//! See docs/testing.md, section "Simulation crate".
pub mod host;
pub mod invariants;
pub mod net;
pub mod oracle04;
pub mod rng;
pub mod schema;
pub mod shrink;
pub mod sim;
pub mod step;
pub use rng::Rng;
pub use sim::{Action, MutationSpec, Sim, Slot};
pub use step::Failure;
