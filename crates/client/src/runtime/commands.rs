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
use super::protocol::{Command, TransactionCommand};
use crate::*;
use serde_json::{Value, json};
use std::collections::BTreeMap;

/// Execute one task command against the committed client.
pub(super) fn execute<S: ClientStore + 'static>(
    client: &mut Client<S>,
    command: &Command,
) -> Result<Value> {
    if (client.request_context05().is_ok() || client.request_context().is_ok())
        && matches!(
            command,
            Command::Freeze
                | Command::Ack { .. }
                | Command::Pull { .. }
                | Command::Enqueue { .. }
                | Command::InvalidateQueryOnce { .. }
        )
    {
        return Err(invalid("legacy protocol seam is retired in protocol 4"));
    }
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
        Command::Stream { stream, subscribed } => {
            let (stream, subscribed) = (stream.clone(), *subscribed);
            client.transaction(|tx| tx.set_stream(stream, subscribed))?;
            Value::Null
        }
        Command::SubmitAction {
            name,
            version,
            args,
            store,
        } => {
            if client.request_context().is_ok() && store.is_some() {
                return Err(invalid("Mutation does not accept store"));
            }
            let submitted = if client.request_context05().is_ok() {
                if store.is_some() {
                    return Err(invalid("Mutation does not accept store"));
                }
                client
                    .transaction(|tx| tx.submit_mutation05(name, *version, args.clone(), vec![]))?
            } else {
                client.submit_action_with_options(name, *version, args.clone(), options(store)?)?
            };
            json!({"callId":submitted.call_id,"ordinal":submitted.ordinal})
        }
        // The Stream commands behind the SDK subscription handles: each owns its
        // own local transaction. An uninitialized boundary answers as `null`,
        // never as zero ([#150](https://github.com/zanminwang/axton/issues/150)).
        Command::StreamState { stream } => match if client.request_context05().is_ok() {
            client.subscription_state05(stream)?
        } else {
            client.subscription_state(stream)?
        } {
            Some(state) => serde_json::to_value(state)?,
            None => Value::Null,
        },
        // The durable load of a Stream's history
        // ([#151](https://github.com/zanminwang/axton/issues/151)).
        Command::StreamBootstrap {
            stream,
            subscription_id,
        } => serde_json::to_value(client.request_bootstrap(stream, *subscription_id)?)?,
        Command::StreamBootstrapState {
            stream,
            subscription_id,
        } => serde_json::to_value(if client.request_context05().is_ok() {
            client.bootstrap_state05(stream, *subscription_id)?
        } else {
            client.bootstrap_state(stream, *subscription_id)?
        })?,
        Command::StreamUnsubscribe {
            stream,
            subscription_id,
        } => json!({"removed":client.remove_subscription(stream, *subscription_id)?}),
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
            let page = StreamPullPage::decode(serde_json::to_string(page)?.as_bytes())?;
            serde_json::to_value(client.apply_stream_page(page)?)?
        }
        Command::Readiness { key, state } => {
            client.set_readiness(key, *state)?;
            Value::Null
        }
        Command::Drop { ordinal } => {
            if client.request_context05().is_ok() {
                json!(client.transaction(|tx| tx.discard_mutation05(*ordinal))?)
            } else {
                json!({"completions":client.drop_action(*ordinal)?})
            }
        }
        Command::Dismiss { ordinal } => {
            if client.request_context05().is_ok() {
                client.transaction(|tx| tx.dismiss_rejection05(*ordinal))?;
            } else {
                client.dismiss_rejection(*ordinal)?;
            }
            Value::Null
        }
        Command::RejectionGet { id } => {
            if client.request_context05().is_ok() {
                json!(
                    client
                        .refused_acts05()?
                        .into_iter()
                        .find(|act| act.id == *id)
                )
            } else {
                serde_json::to_value(client.refused_act(*id)?)?
            }
        }
        Command::RetryTasks { keys } => {
            client.retry_tasks(keys)?;
            Value::Null
        }
        Command::Discard { ordinal } if client.request_context05().is_ok() => {
            json!(client.transaction(|tx| tx.discard_mutation05(*ordinal))?)
        }
        Command::Discard { ordinal } => json!({"completions":client.discard(*ordinal)?}),
        Command::RecordStatus { key } => client.record_status(key)?,
        Command::CallCompletion { call_id } => {
            if client.request_context05().is_ok() {
                json!(client.call_completion05(call_id)?)
            } else {
                json!(client.call_completion04(call_id)?)
            }
        }
        Command::Tasks => json!(client.pending_tasks()?),
        Command::Status if client.request_context05().is_ok() => client.status_snapshot05()?,
        Command::Status => {
            json!({"clientId":client.client_id(),"pending":client.pending_count()?,"beforeImages":client.before_image_count()?,"cursors":client.subscriptions()?.into_iter().collect::<BTreeMap<_,_>>(),"streams":client.desired_streams()?,"rejections":client.rejections()?,"schema":schema_json(client.schema_state())})
        }
        Command::Malformed { error } => return Err(invalid(error.clone())),
        Command::Transaction
        | Command::Connect { .. }
        | Command::Connection { .. }
        | Command::Invoke { .. }
        | Command::Fetch { .. }
        | Command::ResetStore { .. }
        | Command::Rebuild { .. }
        | Command::StreamSubscribe { .. }
        | Command::Watch { .. }
        | Command::WatchSql { .. }
        | Command::Unwatch { .. }
        | Command::UnsentWatch { .. }
        | Command::LoadStart { .. }
        | Command::LoadGet { .. }
        | Command::LoadStatus { .. }
        | Command::LoadList { .. }
        | Command::LoadWait { .. }
        | Command::LoadCancel { .. }
        | Command::LoadRetry { .. }
        | Command::LoadForget { .. }
        | Command::LoadInvalidate { .. }
        | Command::LoadDispose { .. } => {
            return Err(invalid("a runtime lifecycle is not a client command"));
        }
    })
}

/// Execute one read or write of the open application transaction inside its
/// session. The savepoint commands and `submitMutation`, which may start a
/// local callback, are the transaction's own.
pub(super) fn execute_in_session<S: ClientStore>(
    client: &mut Client<S>,
    command: &TransactionCommand,
) -> Result<Value> {
    if client.request_context().is_ok() && matches!(command, TransactionCommand::Enqueue { .. }) {
        return Err(invalid("legacy protocol seam is retired in protocol 4"));
    }
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
        TransactionCommand::Stream { stream, subscribed } => {
            let (stream, subscribed) = (stream.clone(), *subscribed);
            client.session(|tx| tx.set_stream(stream, subscribed))?;
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
    json!({"oldFile":report.old_file,"newFile":report.new_file,"reason":report.reason,"leftPending":report.left_pending,"leftDirect":report.left_direct,"abandonedCalls":abandoned_json(&report.abandoned_calls),"abandonedLoads":report.abandoned_loads})
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
