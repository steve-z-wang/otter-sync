//! The schema compiler: [`parse`] turns `.model` text into declarations,
//! [`validate`] checks them into a typed [`Validated`] schema, and
//! [`generate`] renders the runtime descriptors and the generated code.
use serde_json::Value;
mod action_names;
mod emit;
mod emit_loads;
mod emit_transactions;
pub mod generate;
mod history;
pub mod parse;
pub mod validate;
pub use emit::{backend_typescript, client_typescript, dart, typescript};
pub use history::{
    check_fence, reconcile_action_history, reconcile_history, reconcile_load_history,
    reconcile_model_history,
};
pub use parse::{Declarations, Pos, parse};
pub use validate::{Validated, validate};

/// Parse, validate and generate the descriptors of one schema source.
pub fn compile(source: &str) -> Result<Value, String> {
    let declarations = parse(source)?;
    let config = generate::descriptors(&validate(&declarations)?);
    action_names::check(&config, Some(&declarations))?;
    Ok(config)
}

/// Check Action contract identifiers after retained histories are reconciled.
pub fn check_action_names(config: &Value) -> Result<(), String> {
    action_names::check(config, None)
}
