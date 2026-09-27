//! The one open application transaction: its capability tokens, savepoint
//! stack, failure accounting and callback effect
//! ([#134](https://github.com/zanminwang/axton/issues/134)).
//!
//! A failed command poisons the unit unless the savepoint it ran in rolls
//! back, a command in the wrong scope is a structural failure no rollback
//! clears, and the callback's result decides commit or rollback only once
//! every submitted command has run.
use super::*;
use crate::ClientStore;
use std::collections::VecDeque;

const INVALID_SCOPE: &str = "invalid transaction scope";
const CLOSED: &str = "transaction_closed";

pub(super) struct Transaction {
    pub(super) request_id: String,
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
}
struct Scope {
    token: String,
    /// `failure` when the savepoint opened: what a rollback restores.
    failure: Option<String>,
}
pub(super) struct Continuation {
    pub(super) request_id: String,
    pub(super) transaction_id: String,
    pub(super) scope: Option<String>,
    pub(super) command: TransactionCommand,
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
        self.transaction = Some(Transaction {
            request_id,
            transaction_id,
            effect_id,
            scopes: vec![],
            failure: None,
            structural: None,
            finishing: None,
            lane: VecDeque::new(),
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
    /// its callback effect; anything else is stale and ignored.
    pub(super) fn callback_result(
        &mut self,
        effect_id: &str,
        transaction_id: &str,
        ok: bool,
        error: Option<String>,
    ) {
        if let Some(open) = &mut self.transaction
            && open.effect_id == effect_id
            && open.transaction_id == transaction_id
            && open.finishing.is_none()
        {
            open.finishing = Some((ok, error));
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
        let Some(command) = open.lane.pop_front() else {
            return false;
        };
        let outcome = self.run_command(&command);
        self.complete(command.request_id, outcome);
        true
    }
    /// Run one command inside the session under the scope rule, keeping the
    /// failure accounting.
    fn run_command(&mut self, command: &Continuation) -> std::result::Result<Value, String> {
        let Some(open) = &mut self.transaction else {
            return Err(CLOSED.into());
        };
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
            return Err(INVALID_SCOPE.into());
        }
        let outcome = match &command.command {
            TransactionCommand::Savepoint => self.savepoint(),
            TransactionCommand::Release { .. } => {
                self.pop_scope();
                self.client.session_release().map(|()| Value::Null)
            }
            TransactionCommand::RollbackSavepoint { .. } => {
                let restored = self.pop_scope();
                self.client.session_rollback_savepoint().map(|()| {
                    if let Some(open) = &mut self.transaction {
                        open.failure = restored;
                    }
                    Value::Null
                })
            }
            _ => commands::execute_in_session(&mut self.client, &command.command),
        };
        outcome.map_err(|e| {
            let error = e.to_string();
            if let Some(open) = &mut self.transaction {
                open.failure.get_or_insert_with(|| error.clone());
            }
            error
        })
    }
    fn savepoint(&mut self) -> Result<Value> {
        let token = format!("sp{}", self.issue().map_err(crate::invalid)?);
        self.client.session_savepoint()?;
        if let Some(open) = &mut self.transaction {
            let failure = open.failure.clone();
            open.scopes.push(Scope {
                token: token.clone(),
                failure,
            });
        }
        Ok(json!({ "scope": token }))
    }
    /// Pop the top scope - the client pops its savepoint name before the
    /// store call can fail, so the stacks stay aligned - and answer the
    /// failure it opened under.
    fn pop_scope(&mut self) -> Option<String> {
        self.transaction
            .as_mut()
            .and_then(|open| open.scopes.pop())
            .and_then(|scope| scope.failure)
    }
    /// Commit or roll back once the callback finished, then settle its
    /// unawaited commands and the parent task.
    fn finish_transaction(&mut self, ok: bool, error: Option<String>, now: u64, entropy: u64) {
        let Some(open) = self.transaction.take() else {
            return;
        };
        self.effects.remove(&open.effect_id);
        let unawaited = !open.lane.is_empty();
        let refusal = if !ok {
            Some(error.unwrap_or_else(|| "transaction failed".into()))
        } else if let Some(structural) = open.structural {
            Some(structural)
        } else if unawaited {
            Some("unawaited transaction operation".into())
        } else if !open.scopes.is_empty() {
            Some("unclosed savepoint".into())
        } else {
            open.failure
        };
        for command in open.lane {
            self.complete(command.request_id, Err(CLOSED.into()));
        }
        let outcome = match refusal {
            Some(refusal) => {
                if let Err(e) = self.client.rollback_session() {
                    self.error(format!("transaction rollback failed: {e}"));
                }
                Err(refusal)
            }
            None => {
                let generation = self.client.generation();
                // A failed commit has already rolled back inside the client.
                let committed = self.client.commit_session().map_err(|e| e.to_string());
                self.committed_since(generation);
                // What the callback queued or subscribed is the lanes' work
                // now, as after any other commit.
                if self.client.generation() != generation {
                    self.wake_lanes(now, entropy);
                }
                committed.map(|()| Value::Null)
            }
        };
        self.complete(open.request_id, outcome);
    }
}
