//! Unsent work, account-wide: the acts the server refused, with the act as
//! submitted, and the queued acts blocked on a terminally failed prerequisite
//! task, with their resolutions ([#186](https://github.com/zanminwang/axton/issues/186),
//! [#205](https://github.com/zanminwang/axton/issues/205),
//! [#204](https://github.com/zanminwang/axton/issues/204)).
//!
//! These read models reconstruct named input and operations from the canonical
//! Mutation queue. Refused work retains the author's input until acknowledged;
//! failed work names the prerequisite keys that still block it.
use crate::Operation;
use crate::engine::{Engine, as_u64};
use crate::store::ClientStore;
use axton_core::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

// The normalized named input and declared Model operations as submitted.
// Local companions and cascade effects are not part of the act.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SubmittedAct {
    pub args: Option<Value>,
    pub operations: Vec<Operation>,
}

// One retained refusal. `id` is the act's ordinal, the key `get` and
// `dismiss` take.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RefusedAct {
    pub id: u64,
    pub name: String,
    pub version: u64,
    pub code: String,
    pub act: SubmittedAct,
}

// A prerequisite task that failed terminally: its key, the prerequisite
// name and arguments of a schema-derived key (`None` for an opaque one) and
// the reason it failed.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FailedTask {
    pub key: String,
    pub name: Option<String>,
    pub arguments: Option<Value>,
    pub error: String,
}

// A queued act that waits on at least one failed task.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FailedAct {
    pub ordinal: u64,
    pub name: String,
    pub version: u64,
    pub act: SubmittedAct,
    pub tasks: Vec<FailedTask>,
}

fn failed_task(key: &str, error: &str) -> FailedTask {
    let invocation = serde_json::from_str::<Value>(key)
        .ok()
        .filter(Value::is_object);
    FailedTask {
        key: key.to_string(),
        name: invocation
            .as_ref()
            .and_then(|i| i["name"].as_str())
            .map(str::to_string),
        arguments: invocation
            .as_ref()
            .and_then(|i| i.get("arguments"))
            .cloned(),
        error: error.to_string(),
    }
}

impl<S: ClientStore> Engine<'_, S> {
    // Every retained refusal, oldest first.
    pub fn refused_acts(&mut self) -> Result<Vec<RefusedAct>> {
        self.refused_where("", &[])
    }
    // One retained refusal, or `None`.
    pub fn refused_act(&mut self, id: u64) -> Result<Option<RefusedAct>> {
        Ok(self
            .refused_where("WHERE ordinal=?", &[serde_json::json!(id)])?
            .into_iter()
            .next())
    }
    fn refused_where(&mut self, filter: &str, params: &[Value]) -> Result<Vec<RefusedAct>> {
        let filter = if filter.is_empty() {
            String::new()
        } else {
            format!(
                " AND {}",
                filter.trim_start_matches("WHERE ").replace("ordinal", "id")
            )
        };
        let rows=self.rows(&format!("SELECT id,name,descriptor_version,rejection_code FROM axton_mutation_queue WHERE rejection_code IS NOT NULL AND rejection_acknowledged=0 {filter} ORDER BY id"),params)?.rows;
        rows.into_iter()
            .map(|row| {
                let id = as_u64(&row[0])?;
                let ops = self.wire_ops05(id)?;
                let args = crate::v05::reconstruct_input(&ops)?;
                let operations = ops
                    .into_iter()
                    .filter_map(|op| {
                        let kind = match op.operation {
                            crate::v05::Operation::Create => crate::OperationKind::Create,
                            crate::v05::Operation::Update => crate::OperationKind::Update,
                            crate::v05::Operation::Delete => crate::OperationKind::Delete,
                            crate::v05::Operation::Argument => return None,
                        };
                        Some(crate::Operation {
                            model: op.model?,
                            identity: op.identity,
                            op: kind,
                            values: (!op.value.is_null()).then_some(op.value),
                        })
                    })
                    .collect();
                Ok(RefusedAct {
                    id,
                    name: row[1].as_str().unwrap().into(),
                    version: as_u64(&row[2])?,
                    code: row[3].as_str().unwrap().into(),
                    act: SubmittedAct {
                        args: Some(args),
                        operations,
                    },
                })
            })
            .collect()
    }
    // The unsent acts waiting on a failed task, oldest first, each with its
    // failed tasks in key order. A task's state is the one every act waiting
    // on it shares ([`Engine::prerequisite_keys`]).
    pub fn failed_acts(&mut self) -> Result<Vec<FailedAct>> {
        let failed: BTreeMap<String, String> = self
            .prerequisite_keys()?
            .into_iter()
            .filter_map(|(key, error)| error.map(|error| (key, error)))
            .collect();
        if failed.is_empty() {
            return Ok(vec![]);
        }
        // Only the calls waiting on a failed key are read: every table the
        // queue reader joins is bound to the canonical Mutation id. A key rather than a
        // row decides, so a row written before failures were inherited
        // (#204) is still listed.
        let waiting = self.queued_where(
            "AND q.id IN (SELECT ordinal FROM axton_mutation_prerequisite WHERE key IN (SELECT key FROM axton_mutation_prerequisite WHERE error IS NOT NULL))",
            &[],
        )?;
        let mut acts = vec![];
        for queued in waiting {
            if queued.push.is_some() {
                continue;
            }
            let tasks: Vec<FailedTask> = queued
                .mutation
                .prerequisites
                .iter()
                .filter_map(|key| failed.get(key).map(|error| failed_task(key, error)))
                .collect();
            if tasks.is_empty() {
                continue;
            }
            let act = SubmittedAct {
                args: Some(crate::v05::reconstruct_input(
                    &self.wire_ops05(queued.ordinal)?,
                )?),
                operations: queued.mutation.operations.clone(),
            };
            acts.push(FailedAct {
                ordinal: queued.ordinal,
                name: queued.mutation.name.clone(),
                version: queued.mutation.version,
                act,
                tasks,
            });
        }
        Ok(acts)
    }
    // Make each task pending again, for every act waiting on it. A key no
    // act waits on changes nothing.
    pub fn retry_tasks(&mut self, keys: &[String]) -> Result<()> {
        for key in keys {
            self.reset_prerequisite(key)?;
        }
        Ok(())
    }
    // Remove unsent act `ordinal` and its optimism, recording no refusal
    // for it: the author decided. Its lifecycle dependents are refused
    // `dependency.rejected` and retained as refusals. Answers the terminal
    // completions of the Calls it removed; an unknown ordinal changes
    // nothing, and an act already sent cannot be discarded.
}
