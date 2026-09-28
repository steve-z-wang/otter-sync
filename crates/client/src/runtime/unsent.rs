//! Unsent work observers and resolutions made in a transaction
//! ([#186](https://github.com/zanminwang/axton/issues/186),
//! [#205](https://github.com/zanminwang/axton/issues/205)).
//!
//! **Observers.** `unsentWatch {view}` reads the retained refusals, the acts
//! blocked on a failed task or the pending count on the committed reader,
//! answers its observer id and publishes the result. Like a watch, it re-runs
//! after every commit - never on a timer, never while a callback transaction
//! is open - and publishes only a result that differs from the last one it
//! published; a re-run that fails is reported and the observer stays. Close
//! ends it with a terminal snapshot carrying its last result; `unwatch` ends
//! it with none.
//!
//! **Resolutions in a transaction.** `dismiss`, `retryTasks` and `discard`
//! run inside the open application transaction, so later reads and
//! submissions of the same callback see their effect, and they commit or roll
//! back with it. What they announce is deferred to the commit: a discarded
//! Call's completion (and its refused dependents') and the reset of a retried
//! task's backoff, which lets the handler registered at open run it. A
//! savepoint rollback forgets the ones made in its scope; a rollback, a failed
//! commit or close forgets them all.
use super::transactions::Resolved;
use super::*;
use crate::ClientStore;

#[derive(Default)]
pub(super) struct Unsent {
    /// Observers by the number behind their observer id.
    observers: BTreeMap<u64, Observer>,
}

struct Observer {
    view: UnsentView,
    /// The snapshot last computed.
    snapshot: Value,
    /// `snapshot` was published.
    published: bool,
}

impl<S: ClientStore + 'static> ClientRuntime<S> {
    /// The current snapshot of `view` on the committed state.
    fn unsent_snapshot(&mut self, view: UnsentView) -> std::result::Result<Value, String> {
        let snapshot = match view {
            UnsentView::Rejections => self
                .client
                .refused_acts()
                .map(|items| json!({"kind": "rejections", "items": items})),
            UnsentView::Failures => self
                .client
                .failed_acts()
                .map(|items| json!({"kind": "failures", "items": items})),
            UnsentView::Pending => self
                .client
                .pending_count()
                .map(|count| json!({"kind": "pending", "count": count})),
        };
        snapshot.map_err(|e| e.to_string())
    }
    /// `unsentWatch {view}`: read it now and publish it after the task's
    /// completion. A first read that fails registers nothing.
    pub(super) fn unsent_watch(&mut self, view: UnsentView) -> std::result::Result<Value, String> {
        let snapshot = self.unsent_snapshot(view)?;
        let id = self.issue()?;
        self.unsent.observers.insert(
            id,
            Observer {
                view,
                snapshot,
                published: false,
            },
        );
        Ok(json!({"observerId": id.to_string()}))
    }
    /// `unwatch {observerId}` of an unsent-work observer: nothing more is
    /// published for it.
    pub(super) fn unwatch_unsent(&mut self, observer_id: &str) {
        if let Ok(id) = observer_id.parse::<u64>() {
            self.unsent.observers.remove(&id);
        }
    }
    /// Re-run every observer after a commit, then publish each result that
    /// differs from the last one published. It runs before the watches'
    /// own publication, which clears the commit mark.
    pub(super) fn publish_unsent(&mut self) {
        if self.observers.stale && self.transaction.is_none() {
            let observers: Vec<(u64, UnsentView)> = self
                .unsent
                .observers
                .iter()
                .map(|(id, observer)| (*id, observer.view))
                .collect();
            for (id, view) in observers {
                match self.unsent_snapshot(view) {
                    Ok(snapshot) => {
                        if let Some(observer) = self.unsent.observers.get_mut(&id)
                            && observer.snapshot != snapshot
                        {
                            observer.snapshot = snapshot;
                            observer.published = false;
                        }
                    }
                    Err(error) => self.error(error),
                }
            }
        }
        let mut snapshots = vec![];
        for (id, observer) in &mut self.unsent.observers {
            if !observer.published {
                observer.published = true;
                snapshots.push(Event::ObserverChanged {
                    observer_id: id.to_string(),
                    snapshot: observer.snapshot.clone(),
                });
            }
        }
        self.events.extend(snapshots);
    }
    /// Close: every observer ends with its last result and `closed: true`.
    pub(super) fn close_unsent(&mut self) {
        for (id, observer) in std::mem::take(&mut self.unsent.observers) {
            let mut snapshot = observer.snapshot;
            snapshot["closed"] = json!(true);
            self.events.push(Event::ObserverChanged {
                observer_id: id.to_string(),
                snapshot,
            });
        }
    }
    /// A resolution of the open application transaction: run it in the
    /// session and keep what it announces for the commit. A discard answers
    /// `null`; its completions are final only once the transaction commits.
    pub(super) fn resolve_in_transaction(
        &mut self,
        command: &TransactionCommand,
    ) -> Result<Option<Value>> {
        let resolved = match command {
            TransactionCommand::Dismiss { ordinal } => {
                self.client.session(|tx| tx.dismiss_rejection(*ordinal))?;
                vec![]
            }
            TransactionCommand::RetryTasks { keys } => {
                self.client.session(|tx| tx.retry_tasks(keys))?;
                keys.iter().cloned().map(Resolved::Retried).collect()
            }
            TransactionCommand::Discard { ordinal } => self
                .client
                .session(|tx| tx.discard(*ordinal))?
                .into_iter()
                .map(Resolved::Completed)
                .collect(),
            _ => return Err(crate::invalid("not a resolution of unsent work")),
        };
        if let Some(open) = &mut self.transaction {
            open.resolved.extend(resolved);
        }
        Ok(Some(Value::Null))
    }
    /// The transaction committed: announce its resolutions.
    pub(super) fn announce_resolved(&mut self, resolved: Vec<Resolved>) {
        for resolution in resolved {
            match resolution {
                Resolved::Completed(completion) => self.events.push(Event::CallCompleted {
                    call_id: completion.call_id,
                    outcome: serde_json::to_value(completion.outcome).unwrap_or(Value::Null),
                }),
                Resolved::Retried(key) => self.prerequisite_readiness(&key),
            }
        }
    }
}
