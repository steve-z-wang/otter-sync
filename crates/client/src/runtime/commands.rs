//! The commands executed directly against the client: the local reads and
//! writes, the Stream and Bootstrap registrations, the sync state, and the
//! protocol seams ([#134](https://github.com/zanminwang/axton/issues/134)).
//!
//! A task runs only while no application transaction is open, so its reads
//! use the committed reader and each of its writes owns its own local
//! transaction. A callback's commands run inside the session it owns. The
//! lifecycles - `transaction`, `connect`, `connection`, `invoke`, `fetch`,
//! `rebuild`, `streamSubscribe`, `streamBootstrap`, `watch`, `watchSql`
//! and `unwatch` - are the runtime's own and never reach [`execute`].
use crate::*;
use axton_protocols::client_bridge::{Command, TransactionCommand};
use serde_json::{Value, json};

// Execute one task command against the committed client.
pub(super) fn execute<S: ClientStore + 'static>(
    client: &mut Client<S>,
    command: &Command,
) -> Result<Value> {
    Ok(match command {
        Command::Read { key } => client.read(key)?.unwrap_or(Value::Null),
        Command::Query { model, filter } => {
            serde_json::to_value(client.query(model, &filter_of(filter))?)?
        }
        Command::Sql { sql, parameters } => {
            serde_json::to_value(client.read_sql(sql, parameters)?)?
        }
        Command::QuerySpec { model, query } => {
            serde_json::to_value(client.query_spec(model, query)?)?
        }
        Command::Related { key, relation } => client.related(key, relation)?.unwrap_or(Value::Null),
        Command::Referencing {
            key,
            source,
            relation,
        } => serde_json::to_value(client.referencing(key, source, relation)?)?,

        Command::Direct { operation } => {
            let operation = operation.clone();
            client.transaction(|tx| tx.direct(operation))?;
            Value::Null
        }

        Command::SubmitAction {
            name,
            version,
            args,
            store,
        } => {
            if store.is_some() {
                return Err(invalid("Mutation does not accept store"));
            }
            let submitted = client
                .transaction(|tx| tx.submit_mutation05(name, *version, args.clone(), vec![]))?;
            json!({"callId":submitted.call_id,"ordinal":submitted.ordinal})
        }
        // The Stream commands behind the SDK subscription handles: each owns its
        // own local transaction. An uninitialized boundary answers as `null`,
        // never as zero ([#150](https://github.com/zanminwang/axton/issues/150)).
        Command::StreamState { stream } => json!(client.subscription_state05(stream)?),
        Command::StreamBootstrapState {
            stream,
            subscription_id,
        } => json!(client.bootstrap_state05(stream, *subscription_id)?),

        Command::Readiness { key, state } => {
            client.set_readiness(key, *state)?;
            Value::Null
        }
        Command::Drop { ordinal } => json!(client.transaction(|tx| tx.drop_mutation05(*ordinal))?),
        Command::Dismiss { ordinal } => {
            client.transaction(|tx| tx.dismiss_rejection05(*ordinal))?;
            Value::Null
        }
        Command::RejectionGet { id } => json!(
            client
                .refused_acts05()?
                .into_iter()
                .find(|act| act.id == *id)
        ),
        Command::RetryTasks { keys } => {
            client.retry_tasks(keys)?;
            Value::Null
        }
        Command::Discard { ordinal } => {
            json!(client.transaction(|tx| tx.discard_mutation05(*ordinal))?)
        }
        Command::RecordStatus { key } => client.record_status(key)?,
        Command::CallCompletion { call_id } => json!(client.call_completion05(call_id)?),
        Command::Tasks => json!(client.pending_tasks()?),
        Command::Status => client.status_snapshot05()?,
        Command::Malformed { error } => return Err(invalid(error.clone())),
        Command::Transaction
        | Command::Connect { .. }
        | Command::Connection { .. }
        | Command::Invoke { .. }
        | Command::Fetch { .. }
        | Command::StreamBootstrap { .. }
        | Command::ResetStore { .. }
        | Command::StreamSubscribe { .. }
        | Command::Watch { .. }
        | Command::WatchSql { .. }
        | Command::Unwatch { .. }
        | Command::UnsentWatch { .. } => {
            return Err(invalid("a runtime lifecycle is not a client command"));
        }
    })
}

// Execute one read or write of the open application transaction inside its
// session. The savepoint commands and `submitMutation`, which may start a
// local callback, are the transaction's own.
pub(super) fn execute_in_session<S: ClientStore>(
    client: &mut Client<S>,
    command: &TransactionCommand,
) -> Result<Value> {
    Ok(match command {
        TransactionCommand::Read { key } => {
            client.session(|tx| tx.read(key))?.unwrap_or(Value::Null)
        }
        TransactionCommand::Query { model, filter } => {
            let filter = filter_of(filter);
            serde_json::to_value(client.session(|tx| tx.query(model, &filter))?)?
        }
        TransactionCommand::Sql { sql, parameters } => {
            serde_json::to_value(client.session_sql(sql, parameters)?)?
        }
        TransactionCommand::QuerySpec { model, query } => {
            serde_json::to_value(client.session(|tx| tx.query_spec(model, query))?)?
        }
        TransactionCommand::Related { key, relation } => client
            .session(|tx| tx.related(key, relation))?
            .unwrap_or(Value::Null),
        TransactionCommand::Referencing {
            key,
            source,
            relation,
        } => serde_json::to_value(client.session(|tx| tx.referencing(key, source, relation))?)?,
        TransactionCommand::Direct { operation } => {
            let operation = operation.clone();
            client.session(|tx| tx.direct(operation))?;
            Value::Null
        }

        TransactionCommand::Malformed { error } => return Err(invalid(error.clone())),
        TransactionCommand::Savepoint
        | TransactionCommand::Release { .. }
        | TransactionCommand::RollbackSavepoint { .. }
        | TransactionCommand::SubmitMutation { .. }
        | TransactionCommand::Dismiss { .. }
        | TransactionCommand::RetryTasks { .. }
        | TransactionCommand::Discard { .. } => {
            return Err(invalid("a transaction lifecycle is not a client command"));
        }
    })
}

// A query's filter; every row when absent.
fn filter_of(filter: &Option<Value>) -> Value {
    filter.clone().unwrap_or_else(|| json!({}))
}
// Invocation options sent beside, never inside, an Action's business args.
// An absent `store` is the default policy.
