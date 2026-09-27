//! The commands executed directly against the client: the local reads and
//! writes, the Scope and Bootstrap registrations, the sync state, and the
//! protocol seams ([#134](https://github.com/zanminwang/axton/issues/134)).
//!
//! A task runs only while no application transaction is open, so its reads
//! use the committed reader and each of its writes owns its own local
//! transaction. A callback's commands run inside the session it owns. The
//! lifecycles - `transaction`, `connect`, `connection`, `invoke`,
//! `runPrerequisites`, `rebuild`, `scopeSubscribe`, `scopeBootstrap`, `watch`
//! and `unwatch` - are the runtime's own and never reach [`execute`].
use super::protocol::{Command, TransactionCommand};
use crate::*;
use serde_json::{Value, json};
use std::collections::BTreeMap;

/// Execute one task command against the committed client.
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
        Command::Enqueue { mutation } => {
            let mutation = mutation.clone();
            json!(client.transaction(|tx| tx.enqueue(mutation))?)
        }
        Command::Direct { operation } => {
            let operation = operation.clone();
            client.transaction(|tx| tx.direct(operation))?;
            Value::Null
        }
        Command::Channel {
            channel,
            subscribed,
        } => {
            let (channel, subscribed) = (channel.clone(), *subscribed);
            client.transaction(|tx| tx.set_channel(channel, subscribed))?;
            Value::Null
        }
        Command::SubmitAction {
            name,
            version,
            args,
            store,
        } => {
            let submitted =
                client.submit_action_with_options(name, *version, args.clone(), options(store)?)?;
            json!({"callId":submitted.call_id,"ordinal":submitted.ordinal})
        }
        // The Scope commands behind the SDK subscription handles: each owns its
        // own local transaction. An uninitialized boundary answers as `null`,
        // never as zero ([#150](https://github.com/zanminwang/axton/issues/150)).
        Command::ScopeState { scope } => match client.subscription_state(scope)? {
            Some(state) => serde_json::to_value(state)?,
            None => Value::Null,
        },
        // The durable load of a Scope's history
        // ([#151](https://github.com/zanminwang/axton/issues/151)).
        Command::ScopeBootstrap {
            scope,
            subscription_id,
        } => serde_json::to_value(client.request_bootstrap(scope, *subscription_id)?)?,
        Command::ScopeBootstrapState {
            scope,
            subscription_id,
        } => serde_json::to_value(client.bootstrap_state(scope, *subscription_id)?)?,
        Command::ScopeUnsubscribe {
            scope,
            subscription_id,
        } => json!({"removed":client.remove_subscription(scope, *subscription_id)?}),
        Command::Freeze => match client.freeze()? {
            Some(bytes) => json!(String::from_utf8(bytes).map_err(|_| invalid("utf8"))?),
            None => Value::Null,
        },
        Command::InvalidateQueryOnce {
            name,
            version,
            args,
        } => {
            client.invalidate_query_once(name, *version, args)?;
            Value::Null
        }
        Command::Ack { sequence, receipt } => {
            let bytes = serde_json::to_vec(receipt)?;
            let receipt = if receipt.get("completions").is_some() {
                PushReceipt::decode_action_envelope(&bytes)?
            } else {
                PushReceipt::decode(&bytes)?
            };
            serde_json::to_value(client.acknowledge(*sequence, receipt)?)?
        }
        Command::Pull { page } => {
            let page = PullPage::decode(serde_json::to_string(page)?.as_bytes())?;
            serde_json::to_value(client.apply_page(page)?)?
        }
        Command::Readiness { key, state } => {
            client.set_readiness(key, *state)?;
            Value::Null
        }
        Command::Drop { ordinal } => json!({"completions":client.drop_action(*ordinal)?}),
        Command::Dismiss { ordinal } => {
            client.dismiss_rejection(*ordinal)?;
            Value::Null
        }
        Command::RecordStatus { key } => client.record_status(key)?,
        Command::Tasks => json!(client.pending_tasks()?),
        Command::Status => {
            json!({"clientId":client.client_id(),"pending":client.pending_count()?,"beforeImages":client.before_image_count()?,"cursors":client.subscriptions()?.into_iter().collect::<BTreeMap<_,_>>(),"channels":client.desired_channels()?,"rejections":client.rejections()?,"schema":schema_json(client.schema_state())})
        }
        Command::Malformed { error } => return Err(invalid(error.clone())),
        Command::Transaction
        | Command::Connect { .. }
        | Command::Connection { .. }
        | Command::Invoke { .. }
        | Command::RunPrerequisites { .. }
        | Command::Rebuild { .. }
        | Command::ScopeSubscribe { .. }
        | Command::Watch { .. }
        | Command::Unwatch { .. } => {
            return Err(invalid("a runtime lifecycle is not a client command"));
        }
    })
}

/// Execute one read or write of the open application transaction inside its
/// session. The savepoint commands are the transaction's own.
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
        TransactionCommand::Enqueue { mutation } => {
            let mutation = mutation.clone();
            json!(client.session(|tx| tx.enqueue(mutation))?)
        }
        TransactionCommand::Channel {
            channel,
            subscribed,
        } => {
            let (channel, subscribed) = (channel.clone(), *subscribed);
            client.session(|tx| tx.set_channel(channel, subscribed))?;
            Value::Null
        }
        TransactionCommand::Malformed { error } => return Err(invalid(error.clone())),
        TransactionCommand::Savepoint
        | TransactionCommand::Release { .. }
        | TransactionCommand::RollbackSavepoint { .. } => {
            return Err(invalid("a savepoint is the transaction's own command"));
        }
    })
}

/// The schema check's outcome as language packages report it in `status()`.
pub(super) fn schema_json(state: &SchemaState) -> Value {
    json!({
        "rebuilt": state.rebuilt,
        "pending": state.pending.as_ref().map(|p| json!({"oldFile":p.old_file,"reason":p.reason,"pending":p.pending,"direct":p.direct})),
        "lastRebuild": state.last_rebuild.as_ref().map(rebuild_json),
    })
}
/// What a rebuild reports, as `rebuild` answers it and `status()` keeps it.
pub(super) fn rebuild_json(report: &RebuildReport) -> Value {
    json!({"oldFile":report.old_file,"newFile":report.new_file,"reason":report.reason,"leftPending":report.left_pending,"leftDirect":report.left_direct,"abandonedCalls":abandoned_json(&report.abandoned_calls)})
}
fn abandoned_json(calls: &[AbandonedCall]) -> Vec<Value> {
    calls
        .iter()
        .map(|call| json!({"callId":call.call_id,"frozen":call.frozen}))
        .collect()
}
/// A query's filter; every row when absent.
fn filter_of(filter: &Option<Value>) -> Value {
    filter.clone().unwrap_or_else(|| json!({}))
}
/// Invocation options sent beside, never inside, an Action's business args.
/// An absent `store` is the default policy.
pub(super) fn options(store: &Option<Value>) -> Result<ActionCallOptions> {
    Ok(ActionCallOptions {
        store: match store {
            None => ActionStore::All,
            Some(store) => ActionStore::from_wire(store)?,
        },
    })
}
