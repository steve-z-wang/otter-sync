//! The one open application transaction: its capability tokens, savepoint
//! stack, failure accounting and callback effect
//! ([#134](https://github.com/zanminwang/axton/issues/134)).
//!
//! A failed command poisons the unit unless the savepoint it ran in rolls
//! back, a command in the wrong scope is a structural failure no rollback
//! clears, and the callback's result decides commit or rollback only once
//! every submitted command has run.
//!
//! An application callback may submit named Mutations (`submitMutation`).
//! Each call is provisional until the commit: the commit turns the surviving
//! calls `committed`, and a savepoint rollback turns exactly its scope's
//! calls `rolledBack`, as a rollback, a failed commit or a close does for
//! all of them. A submission with `local` runs the Mutation's local callback
//! as a restricted capability of the same session, right after the call was
//! written: while it runs, only its own local reads and writes are admitted -
//! its writes become the call's companions - and its end answers the
//! submission without committing or closing the transaction.
use super::*;
use crate::{ClientStore, SubmittedCall};
use std::collections::VecDeque;

const INVALID_SCOPE: &str = "invalid transaction scope";
const CLOSED: &str = "transaction_closed";
// A command the active capability does not admit: the parent's handle
// while a local callback runs, a companion token that names no running
// local callback, or anything but a local read or write from one.
const CAPABILITY: &str = "invalid transaction capability";
const UNAWAITED: &str = "unawaited transaction operation";

pub(super) struct Transaction {
    pub(super) owner: TransactionOwner,
    transaction_id: String,
    effect_id: String,
    // Open savepoints, innermost last.
    scopes: Vec<Scope>,
    // The first engine failure not cleared by a savepoint rollback.
    failure: Option<String>,
    // The first wrong-scope command; it always rolls the unit back.
    structural: Option<String>,
    // The callback's result, once it arrived: `(ok, error)`.
    finishing: Option<(bool, Option<String>)>,
    // Submitted commands of the callback, in order.
    pub(super) lane: VecDeque<Continuation>,
    // The Mutation local callback running inside the transaction.
    pub(super) local: Option<LocalCallback>,
    // The calls submitted in the transaction and not rolled back, in
    // order: provisional until the commit.
    pub(super) calls: Vec<String>,
    // What the transaction's resolutions of unsent work announce once it
    // commits, in order ([#205](https://github.com/zanminwang/axton/issues/205)).
    pub(super) resolved: Vec<Resolved>,
}
// One announcement a resolution made in a transaction defers to its commit.
pub(super) enum Resolved {
    // A discarded Call, or a dependent refused with it, completed.
    Completed(crate::CallCompletion),
    // A task was made pending: its backoff is over.
    Retried(String),
}
// One running Mutation local callback: the restricted capability its
// token admits and the submission waiting for its end.
pub(super) struct LocalCallback {
    companion_id: String,
    effect_id: String,
    // The `submitMutation` command answered at its end.
    pub(super) request_id: String,
    call: SubmittedCall,
    deferred: Option<DeferredMutation>,
    // The first failure of its own commands.
    failure: Option<String>,
    // Its result includes the input returned after the local callback.
    finishing: Option<(bool, Option<String>, Option<Value>)>,
}
struct DeferredMutation {
    name: String,
    version: u64,
    operations: Vec<crate::Operation>,
}
impl Transaction {
    fn new(owner: TransactionOwner, transaction_id: String, effect_id: String) -> Self {
        Self {
            owner,
            transaction_id,
            effect_id,
            scopes: vec![],
            failure: None,
            structural: None,
            finishing: None,
            lane: VecDeque::new(),
            local: None,
            calls: vec![],
            resolved: vec![],
        }
    }
}
pub(super) enum TransactionOwner {
    Application { request_id: String },
}

struct Scope {
    token: String,
    // `failure` when the savepoint opened: what a rollback restores.
    failure: Option<String>,
    // How many calls were submitted before it opened: a rollback turns the
    // later ones `rolledBack`.
    calls: usize,
    // How many resolutions were made before it opened: a rollback forgets
    // the later ones.
    resolved: usize,
}
pub(super) struct Continuation {
    pub(super) request_id: String,
    pub(super) transaction_id: String,
    pub(super) scope: Option<String>,
    pub(super) companion_id: Option<String>,
    pub(super) command: TransactionCommand,
}
// What a Mutation local callback may do: read, and write local companions.
fn local_command(command: &TransactionCommand) -> bool {
    matches!(
        command,
        TransactionCommand::Read { .. }
            | TransactionCommand::Query { .. }
            | TransactionCommand::Sql { .. }
            | TransactionCommand::QuerySpec { .. }
            | TransactionCommand::Related { .. }
            | TransactionCommand::Referencing { .. }
            | TransactionCommand::Direct { .. }
            | TransactionCommand::Malformed { .. }
    )
}

impl<S: ClientStore + 'static> ClientRuntime<S> {
    // Run a `transaction` task: open the session, issue its capability and
    // ask the host for the callback. The task parks until the result.
    pub(super) fn open_transaction(&mut self, request_id: String) {
        let issued = self.issue().and_then(|transaction| {
            self.issue()
                .map(|effect| (self.capability_token("tx", transaction), effect.to_string()))
        });
        let (transaction_id, effect_id) = match issued {
            Ok(ids) => ids,
            Err(error) => return self.complete(request_id, Err(error)),
        };
        if let Err(e) = self.client.begin_session() {
            return self.complete(request_id, Err(e.to_string()));
        }
        self.effects
            .insert(effect_id.clone(), effects::EffectKind::Callback);
        self.events.push(Event::Effect {
            effect_id: effect_id.clone(),
            operation: Operation::Callback {
                transaction_id: transaction_id.clone(),
                request_id: request_id.clone(),
            },
        });
        self.transaction = Some(Transaction::new(
            TransactionOwner::Application { request_id },
            transaction_id,
            effect_id,
        ));
    }

    // Admit one command of the callback: onto the lane when it names the
    // open transaction and the result has not arrived, otherwise closed at
    // once.
    pub(super) fn continue_transaction(&mut self, command: Continuation) {
        match &mut self.transaction {
            Some(open)
                if open.transaction_id == command.transaction_id && open.finishing.is_none() =>
            {
                open.lane.push_back(command);
            }
            _ => self.complete(command.request_id, Err(CLOSED.into())),
        }
    }
    // Record the callback's result when it names the open transaction and
    // its callback effect - or, with a companion token, the running local
    // callback's effect and token; anything else is stale and ignored.
    pub(super) fn callback_result(
        &mut self,
        effect_id: &str,
        transaction_id: &str,
        companion_id: Option<&str>,
        ok: bool,
        error: Option<String>,
        input: Option<Value>,
    ) {
        let Some(open) = &mut self.transaction else {
            return;
        };
        if open.transaction_id != transaction_id {
            return;
        }
        match companion_id {
            None => {
                if open.effect_id == effect_id && open.finishing.is_none() {
                    open.finishing = Some((ok, error));
                }
            }
            Some(token) => {
                if let Some(local) = &mut open.local
                    && local.companion_id == token
                    && local.effect_id == effect_id
                    && local.finishing.is_none()
                {
                    local.finishing = Some((ok, error, input));
                }
            }
        }
    }
    // One unit of the open transaction: its finish once the result arrived,
    // else one command of its lane. False while it waits on the callback.
    pub(super) fn step_transaction(&mut self, now: u64, entropy: u64) -> bool {
        let Some(open) = &mut self.transaction else {
            return false;
        };
        if let Some((ok, error)) = open.finishing.take() {
            self.finish_transaction(ok, error, now, entropy);
            return true;
        }
        if let Some((ok, error, input)) = open.local.as_mut().and_then(|l| l.finishing.take()) {
            self.finish_local(ok, error, input);
            return true;
        }
        let Some(command) = open.lane.pop_front() else {
            return false;
        };
        // A submission with a local callback answers at the callback's end.
        if let Some(outcome) = self.run_command(&command) {
            self.complete(command.request_id, outcome);
        }
        true
    }
    // Run one command inside the session under the scope and capability
    // rules, keeping the failure accounting. `None` while the command waits
    // for the local callback it started.
    fn run_command(
        &mut self,
        command: &Continuation,
    ) -> Option<std::result::Result<Value, String>> {
        let Some(open) = &mut self.transaction else {
            return Some(Err(CLOSED.into()));
        };
        // The local callback's own command: admitted only by its token.
        let own = match (&open.local, &command.companion_id) {
            (None, None) => false,
            (Some(local), Some(token)) if local.companion_id == *token => true,
            _ => {
                open.structural.get_or_insert_with(|| CAPABILITY.into());
                return Some(Err(CAPABILITY.into()));
            }
        };
        if own && !local_command(&command.command) {
            open.structural.get_or_insert_with(|| CAPABILITY.into());
            if let Some(local) = &mut open.local {
                local.failure.get_or_insert_with(|| CAPABILITY.into());
            }
            return Some(Err(CAPABILITY.into()));
        }
        let top = open.scopes.last().map(|s| s.token.clone());
        // `release` / `rollbackSavepoint` close the scope they name, the
        // innermost one when they name none; either way it must be open.
        let closes = match &command.command {
            TransactionCommand::Release { scope }
            | TransactionCommand::RollbackSavepoint { scope } => {
                Some(scope.clone().or_else(|| top.clone()))
            }
            _ => None,
        };
        let in_scope = command.scope == top
            && closes
                .as_ref()
                .is_none_or(|named| named.is_some() && *named == top);
        if !in_scope {
            open.structural.get_or_insert_with(|| INVALID_SCOPE.into());
            if own && let Some(local) = &mut open.local {
                local.failure.get_or_insert_with(|| INVALID_SCOPE.into());
            }
            return Some(Err(INVALID_SCOPE.into()));
        }
        let outcome = match &command.command {
            TransactionCommand::Savepoint => self.savepoint().map(Some),
            TransactionCommand::Release { .. } => {
                self.pop_scope();
                self.client.session_release().map(|()| Some(Value::Null))
            }
            // The scope check above guarantees an open scope; without one,
            // no call is announced rolled back.
            TransactionCommand::RollbackSavepoint { .. } => match self.pop_scope() {
                None => Err(crate::invalid("no open savepoint to roll back")),
                Some((restored, calls, resolved)) => {
                    self.client.session_rollback_savepoint().map(|()| {
                        let mut discarded = vec![];
                        if let Some(open) = &mut self.transaction {
                            open.failure = restored;
                            discarded = open.calls.split_off(calls.min(open.calls.len()));
                            open.resolved.truncate(resolved);
                        }
                        self.call_transitions(discarded, CallTransition::RolledBack);
                        Some(Value::Null)
                    })
                }
            },
            TransactionCommand::SubmitMutation {
                name,
                version,
                args,
                store,
                local,
            } => self.submit_mutation(&command.request_id, name, *version, args, store, local),
            TransactionCommand::Dismiss { .. }
            | TransactionCommand::RetryTasks { .. }
            | TransactionCommand::Discard { .. } => self.resolve_in_transaction(&command.command),
            // The local callback's writes are its call's companions (`own`
            // guarantees an open local callback).
            TransactionCommand::Direct { operation } if own => {
                if open
                    .local
                    .as_ref()
                    .is_some_and(|local| local.deferred.is_some())
                {
                    self.client
                        .session(|tx| tx.preview_callback(operation.clone()))
                        .map(|operations| {
                            self.transaction
                                .as_mut()
                                .unwrap()
                                .local
                                .as_mut()
                                .unwrap()
                                .deferred
                                .as_mut()
                                .unwrap()
                                .operations
                                .extend(operations);
                            Some(Value::Null)
                        })
                } else {
                    match open.local.as_ref().map(|local| local.call.ordinal) {
                        None => Err(crate::invalid("companion write without its local callback")),
                        Some(ordinal) => self
                            .client
                            .session(|tx| tx.append_companion(ordinal, operation.clone()))
                            .map(|()| Some(Value::Null)),
                    }
                }
            }
            _ => commands::execute_in_session(&mut self.client, &command.command).map(Some),
        };
        match outcome {
            Ok(value) => value.map(Ok),
            Err(e) => {
                let error = e.to_string();
                if let Some(open) = &mut self.transaction {
                    if !(own
                        && open
                            .local
                            .as_ref()
                            .is_some_and(|local| local.deferred.is_some()))
                    {
                        open.failure.get_or_insert_with(|| error.clone());
                    }
                    if own && let Some(local) = &mut open.local {
                        local.failure.get_or_insert_with(|| error.clone());
                    }
                }
                Some(Err(error))
            }
        }
    }
    // `submitMutation`: write the call into the session. Without `local`
    // it answers at once; with it, the local callback's effect is issued
    // and the answer waits for that callback's end (`None`). Nothing runs
    // between the call's write and its callback: every other command is
    // refused while the callback runs, and nothing else can take the writer.
    fn submit_mutation(
        &mut self,
        request_id: &str,
        name: &str,
        version: u64,
        args: &Value,
        store: &Option<Value>,
        local: &Option<Value>,
    ) -> Result<Option<Value>> {
        let local = match local {
            None | Some(Value::Null) => false,
            Some(Value::Bool(local)) => *local,
            Some(_) => {
                return Err(crate::invalid(
                    "invalid mutation options: local must be a boolean",
                ));
            }
        };
        let bound = true;
        if bound && store.is_some() {
            return Err(crate::invalid("Mutation does not accept store"));
        }
        let deferred = if bound && local {
            if self.client.schema.action(name, version)?.kind != axton_core::CallKind::Mutation {
                return Err(crate::invalid("only a Mutation can be submitted"));
            }
            self.client.session_savepoint()?;
            Some(DeferredMutation {
                name: name.into(),
                version,
                operations: vec![],
            })
        } else {
            None
        };
        let call = if deferred.is_some() {
            SubmittedCall {
                call_id: String::new(),
                ordinal: 0,
            }
        } else {
            self.client
                .session(|tx| tx.submit_mutation05(name, version, args.clone(), vec![]))?
        };
        if !local {
            let answer = submission(&call);
            if let Some(open) = &mut self.transaction {
                open.calls.push(call.call_id);
            }
            return Ok(Some(answer));
        }
        let companion = self.issue().map_err(crate::invalid)?;
        let companion_id = self.capability_token("c", companion);
        let effect_id = self.issue().map_err(crate::invalid)?.to_string();
        let Some(open) = &mut self.transaction else {
            return Err(crate::invalid(CLOSED));
        };
        let transaction_id = open.transaction_id.clone();
        open.local = Some(LocalCallback {
            companion_id: companion_id.clone(),
            effect_id: effect_id.clone(),
            request_id: request_id.to_string(),
            call,
            deferred,
            failure: None,
            finishing: None,
        });
        self.effects
            .insert(effect_id.clone(), effects::EffectKind::Callback);
        self.events.push(Event::Effect {
            effect_id,
            operation: Operation::MutationLocal {
                transaction_id,
                companion_id,
                request_id: request_id.to_string(),
            },
        });
        Ok(None)
    }
    // The local callback ended: its commands still queued were not awaited,
    // and its submission is answered - with the call, which is then
    // provisional like any other, or with the first failure, which poisons
    // the transaction. The parent's capability is back either way.
    fn finish_local(&mut self, ok: bool, error: Option<String>, input: Option<Value>) {
        let Some(open) = &mut self.transaction else {
            return;
        };
        let Some(mut local) = open.local.take() else {
            return;
        };
        let token = Some(local.companion_id.clone());
        let (unawaited, lane): (VecDeque<_>, VecDeque<_>) = std::mem::take(&mut open.lane)
            .into_iter()
            .partition(|command| command.companion_id == token);
        open.lane = lane;
        let mut refusal = if !ok {
            Some(error.unwrap_or_else(|| "local callback failed".into()))
        } else if !unawaited.is_empty() {
            Some(UNAWAITED.to_string())
        } else {
            local.failure
        };
        let deferred = local.deferred.take();
        let isolated = deferred.is_some();
        if let Some(deferred) = deferred {
            if let Err(error) = self.client.session_rollback_savepoint() {
                refusal = Some(error.to_string());
            }
            if refusal.is_none() {
                let submitted = input
                    .ok_or_else(|| crate::invalid("Mutation callback must return input"))
                    .and_then(|input| {
                        self.client.session(|tx| {
                            tx.submit_mutation05(
                                &deferred.name,
                                deferred.version,
                                input,
                                deferred.operations,
                            )
                        })
                    });
                match submitted {
                    Ok(call) => local.call = call,
                    Err(error) => refusal = Some(error.to_string()),
                }
            }
        }
        let open = self.transaction.as_mut().unwrap();
        match &refusal {
            Some(refusal) if !isolated => {
                open.failure.get_or_insert_with(|| refusal.clone());
            }
            Some(_) => {}
            None => open.calls.push(local.call.call_id.clone()),
        }
        self.effects.remove(&local.effect_id);
        for command in unawaited {
            self.complete(command.request_id, Err(CLOSED.into()));
        }
        let outcome = match refusal {
            Some(refusal) => Err(refusal),
            None => Ok(submission(&local.call)),
        };
        self.complete(local.request_id, outcome);
    }
    // Announce where provisional calls went.
    pub(super) fn call_transitions(&mut self, calls: Vec<String>, state: CallTransition) {
        for call_id in calls {
            self.events
                .push(Event::TransactionCallState { call_id, state });
        }
    }
    fn savepoint(&mut self) -> Result<Value> {
        let scope = self.issue().map_err(crate::invalid)?;
        let token = self.capability_token("sp", scope);
        self.client.session_savepoint()?;
        if let Some(open) = &mut self.transaction {
            let failure = open.failure.clone();
            let calls = open.calls.len();
            let resolved = open.resolved.len();
            open.scopes.push(Scope {
                token: token.clone(),
                failure,
                calls,
                resolved,
            });
        }
        Ok(json!({ "scope": token }))
    }
    // Pop the top scope - the client pops its savepoint name before the
    // store call can fail, so the stacks stay aligned - and answer the
    // failure it opened under and the calls submitted and resolutions made
    // before it.
    fn pop_scope(&mut self) -> Option<(Option<String>, usize, usize)> {
        self.transaction
            .as_mut()
            .and_then(|open| open.scopes.pop())
            .map(|scope| (scope.failure, scope.calls, scope.resolved))
    }
    // Commit or roll back once the callback finished, then settle its
    // unawaited commands and the parent task.
    fn finish_transaction(&mut self, ok: bool, error: Option<String>, now: u64, entropy: u64) {
        let Some(mut open) = self.transaction.take() else {
            return;
        };
        self.effects.remove(&open.effect_id);
        let calls = std::mem::take(&mut open.calls);
        let resolved = std::mem::take(&mut open.resolved);
        // A local callback still running was not awaited: its effect is
        // cancelled and its submission closed.
        let local = open.local.take();
        let unawaited = !open.lane.is_empty() || local.is_some();
        let refusal = if !ok {
            Some(error.unwrap_or_else(|| "transaction failed".into()))
        } else if let Some(structural) = open.structural {
            Some(structural)
        } else if unawaited {
            Some(UNAWAITED.into())
        } else if !open.scopes.is_empty() {
            Some("unclosed savepoint".into())
        } else {
            open.failure
        };
        if let Some(local) = local {
            self.cancel_effect(&local.effect_id);
            self.complete(local.request_id, Err(CLOSED.into()));
        }
        for command in open.lane {
            self.complete(command.request_id, Err(CLOSED.into()));
        }
        let TransactionOwner::Application { request_id } = open.owner;
        let outcome = match refusal {
            Some(refusal) => {
                if let Err(e) = self.client.rollback_session() {
                    self.error(format!("transaction rollback failed: {e}"));
                }
                self.call_transitions(calls, CallTransition::RolledBack);
                Err(refusal)
            }
            None => {
                let generation = self.client.generation();
                // A failed commit has already rolled back inside the client.
                let committed = self.client.commit_session().map_err(|e| e.to_string());
                // The calls leave their provisional state before the
                // transaction's own answer, and only once the commit is known.
                let state = if committed.is_ok() {
                    CallTransition::Committed
                } else {
                    CallTransition::RolledBack
                };
                self.call_transitions(calls, state);
                if committed.is_ok() {
                    self.announce_resolved(resolved);
                }
                self.committed_since(generation);
                // What the callback queued or subscribed is the lanes' work
                // now, as after any other commit.
                if self.client.generation() != generation {
                    self.wake_lanes(now, entropy);
                }
                committed.map(|()| Value::Null)
            }
        };
        self.complete(request_id, outcome);
    }
}
// What a submission answers: the durable call identity and its ordinal.
fn submission(call: &SubmittedCall) -> Value {
    json!({"callId": call.call_id, "ordinal": call.ordinal})
}
