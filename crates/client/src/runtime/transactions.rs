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
use crate::{ClientStore, PreparedStore, StoreChange, StoreDelivery, StoreResult, SubmittedCall};
use std::collections::VecDeque;

const INVALID_SCOPE: &str = "invalid transaction scope";
const CLOSED: &str = "transaction_closed";
/// A command the active capability does not admit: the parent's handle
/// while a local callback runs, a companion token that names no running
/// local callback, or anything but a local read or write from one.
const CAPABILITY: &str = "invalid transaction capability";
const UNAWAITED: &str = "unawaited transaction operation";

pub(super) struct Transaction {
    pub(super) owner: TransactionOwner,
    transaction_id: String,
    effect_id: String,
    /// Open savepoints, innermost last.
    scopes: Vec<Scope>,
    /// The first engine failure not cleared by a savepoint rollback.
    failure: Option<String>,
    /// The first wrong-scope command; it always rolls the unit back.
    structural: Option<String>,
    /// The callback's result, once it arrived: `(ok, error)`.
    finishing: Option<(bool, Option<String>)>,
    /// Submitted commands of the callback, in order.
    pub(super) lane: VecDeque<Continuation>,
    current_model: Option<String>,
    /// The Mutation local callback running inside the transaction.
    pub(super) local: Option<LocalCallback>,
    /// The calls submitted in the transaction and not rolled back, in
    /// order: provisional until the commit.
    pub(super) calls: Vec<String>,
}
/// One running Mutation local callback: the restricted capability its
/// token admits and the submission waiting for its end.
pub(super) struct LocalCallback {
    companion_id: String,
    effect_id: String,
    /// The `submitMutation` command answered at its end.
    pub(super) request_id: String,
    call: SubmittedCall,
    /// The first failure of its own commands.
    failure: Option<String>,
    /// Its result, once it arrived: `(ok, error)`.
    finishing: Option<(bool, Option<String>)>,
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
            current_model: None,
            local: None,
            calls: vec![],
        }
    }
}
pub(super) enum TransactionOwner {
    Application {
        request_id: String,
    },
    Authority {
        prepared: Box<PreparedStore>,
        continuation: StoreContinuation,
        pending: VecDeque<(String, Vec<StoreChange>)>,
    },
}
pub(super) enum StoreContinuation {
    Ack {
        request_id: String,
    },
    Pull {
        request_id: String,
    },
    Direct {
        request_id: String,
    },
    /// A Model Fetch's single-record delivery.
    Fetch {
        request_id: String,
    },
    Push,
    Downlink {
        token: crate::downlink_worker::StoreToken,
    },
    /// One page of the Load worker's `batch`.
    Load {
        batch: u64,
        sent: crate::LoadSent,
    },
}
impl StoreContinuation {
    fn path(&self) -> &'static str {
        match self {
            Self::Ack { .. } | Self::Push => "receipt",
            Self::Pull { .. } => "pull",
            Self::Direct { .. } => "direct",
            Self::Fetch { .. } => "fetch",
            Self::Downlink { token } => token.path(),
            Self::Load { .. } => "load",
        }
    }
    /// The delivery failed. `callback` names the store hook effect when a
    /// hook refused it, `model` that hook's Model, and `identities` the
    /// records the hook was handed (a Load page keeps them as diagnostics).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn fail<S: ClientStore + 'static>(
        self,
        runtime: &mut ClientRuntime<S>,
        error: String,
        callback: Option<&str>,
        model: Option<&str>,
        identities: &[Value],
        now: u64,
        entropy: u64,
    ) {
        let path = self.path();
        if let Some(model) = model {
            runtime.report(Diagnostic::StoreHook {
                code: "store_hook_failed".into(),
                model: model.into(),
                path: path.into(),
                message: error.clone(),
                callback_effect_id: callback.map(str::to_string),
            });
        }
        match self {
            Self::Ack { request_id } | Self::Pull { request_id } => {
                if let Some(effect_id) = callback {
                    runtime.fail(request_id, error, json!({"code":"store_hook_failed","model":model,"path":path,"callbackEffectId":effect_id}));
                } else {
                    runtime.complete(request_id, Err(error));
                }
            }
            Self::Direct { request_id } => {
                if let Some(effect_id) = callback {
                    runtime.fail_direct(&request_id, &error, json!({"code":"store_hook_failed","model":model,"path":path,"callbackEffectId":effect_id}));
                } else {
                    runtime.fail_direct(
                        &request_id,
                        direct::EXECUTION_UNKNOWN,
                        direct::transport_failure(&EffectError {
                            message: error,
                            status: None,
                        }),
                    );
                }
            }
            // A read claims nothing about side effects: the refused store is
            // the cause, with the hook's callback when one refused it.
            Self::Fetch { request_id } => {
                let mut details = json!({"code": direct::FETCH_STORE_FAILED, "message": error});
                if let Some(effect_id) = callback {
                    details["model"] = json!(model);
                    details["path"] = json!(path);
                    details["callbackEffectId"] = json!(effect_id);
                }
                runtime.fail_direct(&request_id, direct::FETCH_STORE_FAILED, details);
            }
            Self::Push => {
                if callback.is_none() {
                    runtime.error(error);
                }
                runtime.push_failed(now, entropy);
            }
            Self::Downlink { token } => {
                runtime.downlink_store_failed(token, error, callback.is_some(), now, entropy)
            }
            // The page rolled back: a hook failure is terminal, any other
            // failure retries the same call. The next Load unit records it.
            Self::Load { batch, sent } => {
                let failure = match (callback, model) {
                    (Some(_), Some(model)) => {
                        crate::LoadFailure::hook_failed(model, identities, error)
                    }
                    _ => crate::LoadFailure::local_retry(error),
                };
                runtime.requeue_load(batch, sent, failure);
            }
        }
    }
}
struct Scope {
    token: String,
    /// `failure` when the savepoint opened: what a rollback restores.
    failure: Option<String>,
    /// How many calls were submitted before it opened: a rollback turns the
    /// later ones `rolledBack`.
    calls: usize,
}
pub(super) struct Continuation {
    pub(super) request_id: String,
    pub(super) transaction_id: String,
    pub(super) scope: Option<String>,
    pub(super) companion_id: Option<String>,
    pub(super) command: TransactionCommand,
}
/// What a Mutation local callback may do: read, and write local companions.
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
    /// Run a `transaction` task: open the session, issue its capability and
    /// ask the host for the callback. The task parks until the result.
    pub(super) fn open_transaction(&mut self, request_id: String) {
        let issued = self.issue().and_then(|transaction| {
            self.issue()
                .map(|effect| (format!("tx{transaction}"), effect.to_string()))
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
    pub(super) fn has_store_hook_candidate(&self, models: impl Iterator<Item = String>) -> bool {
        self.client.store_hooks_active()
            && models
                .into_iter()
                .any(|model| self.store_hooks.contains(&model))
    }
    /// Discard authority state if it still owns a logical session, then report
    /// any physical rollback failure once. A failed physical rollback closes
    /// the runtime before its queued work can touch the uncertain connection.
    pub(super) fn abort_authority_session(&mut self) {
        if self.client.session_active() {
            let _ = self.client.rollback_session();
        }
        if let Some(error) = self.client.take_physical_rollback_failure() {
            self.error(format!("authority rollback failed: {error}"));
            self.lifecycle = Lifecycle::Closing;
        }
    }
    pub(super) fn open_store(
        &mut self,
        delivery: StoreDelivery,
        continuation: StoreContinuation,
        now: u64,
        entropy: u64,
    ) {
        if let Err(error) = self.client.begin_session() {
            continuation.fail(self, error.to_string(), None, None, &[], now, entropy);
            return;
        }
        let prepared = match self.client.prepare_store(delivery) {
            Ok(prepared) => prepared,
            Err(error) => {
                self.abort_authority_session();
                continuation.fail(self, error.to_string(), None, None, &[], now, entropy);
                return;
            }
        };
        // A Load page with a record that cannot be applied is refused before
        // any hook runs; nothing of it is kept.
        if let Some(refusal) = prepared.load_refusal().cloned() {
            let reports = prepared.load_refusal_reports().to_vec();
            self.abort_authority_session();
            // The application hears which records refused the page, as for
            // every other delivery; the job keeps their bounded summary.
            if !reports.is_empty() {
                self.report(Diagnostic::Records { reports });
            }
            match continuation {
                StoreContinuation::Load { batch, sent } => self.requeue_load(batch, sent, refusal),
                continuation => continuation.fail(
                    self,
                    "a refused Load page cannot be stored".into(),
                    None,
                    None,
                    &[],
                    now,
                    entropy,
                ),
            }
            return;
        }
        let pending: VecDeque<_> = prepared
            .changes()
            .iter()
            .filter(|(model, changes)| self.store_hooks.contains(*model) && !changes.is_empty())
            .map(|(model, changes)| (model.clone(), changes.clone()))
            .collect();
        if pending.is_empty() {
            self.finish_store_owner(prepared, continuation, now, entropy);
            return;
        }
        let transaction_id = match self.issue() {
            Ok(id) => format!("tx{id}"),
            Err(error) => {
                self.abort_authority_session();
                continuation.fail(self, error, None, None, &[], now, entropy);
                return;
            }
        };
        let effect_id = match self.issue() {
            Ok(id) => id.to_string(),
            Err(error) => {
                self.abort_authority_session();
                continuation.fail(self, error, None, None, &[], now, entropy);
                return;
            }
        };
        self.transaction = Some(Transaction::new(
            TransactionOwner::Authority {
                prepared: Box::new(prepared),
                continuation,
                pending,
            },
            transaction_id,
            effect_id,
        ));
        self.emit_next_store_callback();
    }
    fn emit_next_store_callback(&mut self) {
        let Some(open) = &mut self.transaction else {
            return;
        };
        let TransactionOwner::Authority { pending, .. } = &mut open.owner else {
            return;
        };
        let Some((model, changes)) = pending.pop_front() else {
            return;
        };
        open.current_model = Some(model.clone());
        let effect_id = open.effect_id.clone();
        self.effects
            .insert(effect_id.clone(), effects::EffectKind::Callback);
        self.events.push(Event::Effect {
            effect_id,
            operation: Operation::StoreCallback {
                transaction_id: open.transaction_id.clone(),
                model,
                changes,
            },
        });
    }
    /// Admit one command of the callback: onto the lane when it names the
    /// open transaction and the result has not arrived, otherwise closed at
    /// once.
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
    /// Record the callback's result when it names the open transaction and
    /// its callback effect - or, with a companion token, the running local
    /// callback's effect and token; anything else is stale and ignored.
    pub(super) fn callback_result(
        &mut self,
        effect_id: &str,
        transaction_id: &str,
        companion_id: Option<&str>,
        ok: bool,
        error: Option<String>,
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
                    local.finishing = Some((ok, error));
                }
            }
        }
    }
    /// One unit of the open transaction: its finish once the result arrived,
    /// else one command of its lane. False while it waits on the callback.
    pub(super) fn step_transaction(&mut self, now: u64, entropy: u64) -> bool {
        let Some(open) = &mut self.transaction else {
            return false;
        };
        if let Some((ok, error)) = open.finishing.take() {
            self.finish_transaction(ok, error, now, entropy);
            return true;
        }
        if let Some((ok, error)) = open.local.as_mut().and_then(|l| l.finishing.take()) {
            self.finish_local(ok, error);
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
    /// Run one command inside the session under the scope and capability
    /// rules, keeping the failure accounting. `None` while the command waits
    /// for the local callback it started.
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
        let authority = matches!(open.owner, TransactionOwner::Authority { .. });
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
                Some((restored, calls)) => self.client.session_rollback_savepoint().map(|()| {
                    let mut discarded = vec![];
                    if let Some(open) = &mut self.transaction {
                        open.failure = restored;
                        discarded = open.calls.split_off(calls.min(open.calls.len()));
                    }
                    self.call_transitions(discarded, CallTransition::RolledBack);
                    Some(Value::Null)
                }),
            },
            TransactionCommand::Enqueue { .. } if authority => {
                Err(crate::invalid("store hook cannot enqueue"))
            }
            // onStore stays local-only: refused by its owner, not by decoding.
            TransactionCommand::SubmitMutation { .. } if authority => {
                Err(crate::invalid(crate::STORE_HOOK_SUBMIT))
            }
            TransactionCommand::SubmitMutation {
                name,
                version,
                args,
                store,
                local,
            } => self.submit_mutation(&command.request_id, name, *version, args, store, local),
            // The local callback's writes are its call's companions (`own`
            // guarantees an open local callback).
            TransactionCommand::Direct { operation } if own => {
                match open.local.as_ref().map(|local| local.call.ordinal) {
                    None => Err(crate::invalid("companion write without its local callback")),
                    Some(ordinal) => {
                        let operation = operation.clone();
                        self.client
                            .session(|tx| tx.append_companion(ordinal, operation))
                            .map(|()| Some(Value::Null))
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
                    open.failure.get_or_insert_with(|| error.clone());
                    if own && let Some(local) = &mut open.local {
                        local.failure.get_or_insert_with(|| error.clone());
                    }
                }
                Some(Err(error))
            }
        }
    }
    /// `submitMutation`: write the call into the session. Without `local`
    /// it answers at once; with it, the local callback's effect is issued
    /// and the answer waits for that callback's end (`None`). Nothing runs
    /// between the call's write and its callback: every other command is
    /// refused while the callback runs, and nothing else can take the writer.
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
        let options = commands::options(store)?;
        let call = self
            .client
            .session(|tx| tx.submit_mutation(name, version, args.clone(), options))?;
        if !local {
            let answer = submission(&call);
            if let Some(open) = &mut self.transaction {
                open.calls.push(call.call_id);
            }
            return Ok(Some(answer));
        }
        let companion_id = format!("c{}", self.issue().map_err(crate::invalid)?);
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
    /// The local callback ended: its commands still queued were not awaited,
    /// and its submission is answered - with the call, which is then
    /// provisional like any other, or with the first failure, which poisons
    /// the transaction. The parent's capability is back either way.
    fn finish_local(&mut self, ok: bool, error: Option<String>) {
        let Some(open) = &mut self.transaction else {
            return;
        };
        let Some(local) = open.local.take() else {
            return;
        };
        let token = Some(local.companion_id.clone());
        let (unawaited, lane): (VecDeque<_>, VecDeque<_>) = std::mem::take(&mut open.lane)
            .into_iter()
            .partition(|command| command.companion_id == token);
        open.lane = lane;
        let refusal = if !ok {
            Some(error.unwrap_or_else(|| "local callback failed".into()))
        } else if !unawaited.is_empty() {
            Some(UNAWAITED.to_string())
        } else {
            local.failure
        };
        match &refusal {
            Some(refusal) => {
                open.failure.get_or_insert_with(|| refusal.clone());
            }
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
    /// Announce where provisional calls went.
    pub(super) fn call_transitions(&mut self, calls: Vec<String>, state: CallTransition) {
        for call_id in calls {
            self.events
                .push(Event::TransactionCallState { call_id, state });
        }
    }
    fn savepoint(&mut self) -> Result<Value> {
        let token = format!("sp{}", self.issue().map_err(crate::invalid)?);
        self.client.session_savepoint()?;
        if let Some(open) = &mut self.transaction {
            let failure = open.failure.clone();
            let calls = open.calls.len();
            open.scopes.push(Scope {
                token: token.clone(),
                failure,
                calls,
            });
        }
        Ok(json!({ "scope": token }))
    }
    /// Pop the top scope - the client pops its savepoint name before the
    /// store call can fail, so the stacks stay aligned - and answer the
    /// failure it opened under and the calls submitted before it.
    fn pop_scope(&mut self) -> Option<(Option<String>, usize)> {
        self.transaction
            .as_mut()
            .and_then(|open| open.scopes.pop())
            .map(|scope| (scope.failure, scope.calls))
    }
    /// Commit or roll back once the callback finished, then settle its
    /// unawaited commands and the parent task.
    fn finish_transaction(&mut self, ok: bool, error: Option<String>, now: u64, entropy: u64) {
        let Some(mut open) = self.transaction.take() else {
            return;
        };
        self.effects.remove(&open.effect_id);
        let calls = std::mem::take(&mut open.calls);
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
        if matches!(open.owner, TransactionOwner::Authority { .. }) {
            if let Some(refusal) = refusal {
                self.abort_authority_session();
                if let TransactionOwner::Authority {
                    continuation,
                    prepared,
                    ..
                } = open.owner
                {
                    let identities =
                        loads::hook_identities(&prepared, open.current_model.as_deref());
                    continuation.fail(
                        self,
                        refusal,
                        Some(&open.effect_id),
                        open.current_model.as_deref(),
                        &identities,
                        now,
                        entropy,
                    );
                }
            } else if let TransactionOwner::Authority {
                prepared,
                continuation,
                pending,
            } = open.owner
            {
                if !pending.is_empty() {
                    let transaction_id = self.issue().map(|id| format!("tx{id}"));
                    let effect_id = self.issue().map(|id| id.to_string());
                    match (transaction_id, effect_id) {
                        (Ok(transaction_id), Ok(effect_id)) => {
                            self.transaction = Some(Transaction::new(
                                TransactionOwner::Authority {
                                    prepared,
                                    continuation,
                                    pending,
                                },
                                transaction_id,
                                effect_id,
                            ));
                            self.emit_next_store_callback();
                        }
                        _ => {
                            self.abort_authority_session();
                            continuation.fail(
                                self,
                                "runtime identifiers exhausted".into(),
                                None,
                                None,
                                &[],
                                now,
                                entropy,
                            );
                        }
                    }
                } else {
                    self.finish_store_owner(*prepared, continuation, now, entropy);
                }
            }
            return;
        }
        let TransactionOwner::Application { request_id } = open.owner else {
            unreachable!()
        };
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
    fn finish_store_owner(
        &mut self,
        prepared: PreparedStore,
        continuation: StoreContinuation,
        now: u64,
        entropy: u64,
    ) {
        let generation = self.client.generation();
        let result = self
            .client
            .apply_prepared_store(prepared)
            .and_then(|result| {
                let value = match &result {
                    StoreResult::Page(report) | StoreResult::Receipt(report) => {
                        serde_json::to_value(report)?
                    }
                    StoreResult::Direct(_)
                    | StoreResult::Fetch(_)
                    | StoreResult::Bootstrap(_)
                    | StoreResult::Load(_) => Value::Null,
                };
                // A Load page is committed only when it applied: one whose job
                // moved on wrote nothing, and a refused one must keep nothing.
                if matches!(
                    result,
                    StoreResult::Load(crate::LoadApply::Stale | crate::LoadApply::Refused { .. })
                ) {
                    self.client.rollback_session()?;
                } else {
                    self.client.commit_session()?;
                }
                Ok((value, result))
            });
        if result.is_err() {
            self.abort_authority_session();
        }
        self.committed_since(generation);
        // A Load page changes no queue, Channel or cursor: the lanes have
        // nothing new to look at.
        if self.client.generation() != generation
            && !matches!(continuation, StoreContinuation::Load { .. })
        {
            self.wake_lanes(now, entropy);
        }
        match result {
            Ok((_, StoreResult::Load(apply))) => match continuation {
                StoreContinuation::Load { batch, sent } => self.load_stored(batch, sent, apply),
                continuation => continuation.fail(
                    self,
                    "a Load page answered another delivery".into(),
                    None,
                    None,
                    &[],
                    now,
                    entropy,
                ),
            },
            Ok((_, StoreResult::Direct(report))) => {
                if let StoreContinuation::Direct { request_id } = continuation {
                    self.finish_direct_store(request_id, report);
                } else {
                    unreachable!()
                }
            }
            Ok((_, StoreResult::Fetch(report))) => {
                if let StoreContinuation::Fetch { request_id } = continuation {
                    self.finish_fetch(request_id, report);
                } else {
                    unreachable!()
                }
            }
            Ok((_, StoreResult::Receipt(report)))
                if matches!(continuation, StoreContinuation::Push) =>
            {
                self.push_store_committed(report);
            }
            Ok((_, result)) if matches!(continuation, StoreContinuation::Downlink { .. }) => {
                if let StoreContinuation::Downlink { token } = continuation {
                    self.lanes.downlink.store_committed(token, result);
                    if let Some(connection) = &mut self.connection {
                        connection.downlink.dirty = true;
                    }
                }
            }
            Ok((value, _)) => match continuation {
                StoreContinuation::Ack { request_id } | StoreContinuation::Pull { request_id } => {
                    self.seam_completions(&value);
                    self.complete(request_id, Ok(value));
                }
                _ => unreachable!(),
            },
            Err(error) => continuation.fail(self, error.to_string(), None, None, &[], now, entropy),
        }
    }
}
/// What a submission answers: the durable call identity and its ordinal.
fn submission(call: &SubmittedCall) -> Value {
    json!({"callId": call.call_id, "ordinal": call.ordinal})
}
