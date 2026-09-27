//! The SQL of the native Load ledger ([#173](https://github.com/zanminwang/axton/issues/173)):
//! how one `axton_load` row decodes into a [`LoadJob`], the fenced statements
//! that move it, and the `axton_load_once` key-to-job mapping. Lifecycle rules
//! and their public entry points are in [`loads`](crate::loads); this module
//! only reads and writes rows.
use crate::engine::{Engine, as_u64};
use crate::loads::{LoadJob, LoadJobError, LoadOnceKey, LoadPhase, LoadRetryClass};
use crate::store::ClientStore;
use axton_core::{Continuation, LoadIntent, LoadNext, Result, canonical_json, invalid};
use serde_json::{Value, json};
use std::collections::BTreeMap;

const TABLE: &str = "axton_load";
const ONCE: &str = "axton_load_once";
const COLUMNS: &str = "load_id, name, version, args, models, continuation, run, phase, \
     pages, call_id, intent, retry, attempts, error";

/// A decode error is cut to this many UTF-8 bytes before it enters a
/// [`LoadLedgerIssue`]: it is a reason, never a copy of what the row stores.
const MAX_DETAIL: usize = 200;

/// One stored job a tolerant scan could not decode. The row stays exactly as
/// stored and a named read of it keeps failing; the issue is the account a
/// scan that skipped it gives, so one damaged job never blocks the others.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LoadLedgerIssue {
    pub load_id: String,
    pub detail: String,
}

fn text<'a>(value: &'a Value, what: &str) -> Result<&'a str> {
    value
        .as_str()
        .ok_or_else(|| invalid(format!("stored Load {what} is not text")))
}
fn optional_text<'a>(value: &'a Value, what: &str) -> Result<Option<&'a str>> {
    if value.is_null() {
        return Ok(None);
    }
    text(value, what).map(Some)
}

/// Decode one row selected as [`COLUMNS`] and refuse fields that cannot have
/// been written together.
fn decode(row: &[Value]) -> Result<LoadJob> {
    let id = text(&row[0], "ID")?.to_string();
    let continuation: LoadNext = optional_text(&row[5], "continuation")?
        .map(serde_json::from_str::<Continuation>)
        .transpose()?;
    let job = LoadJob {
        id,
        name: text(&row[1], "name")?.to_string(),
        version: as_u64(&row[2])?,
        args: serde_json::from_str(text(&row[3], "args")?)?,
        models: serde_json::from_str(text(&row[4], "models")?)?,
        continuation,
        run: as_u64(&row[6])?,
        phase: LoadPhase::parse_durable(text(&row[7], "phase")?)?,
        pages: as_u64(&row[8])?,
        call_id: optional_text(&row[9], "call ID")?.map(str::to_string),
        intent: optional_text(&row[10], "intent")?
            .map(serde_json::from_str::<LoadIntent>)
            .transpose()?,
        retry: optional_text(&row[11], "retry class")?
            .map(LoadRetryClass::parse)
            .transpose()?,
        attempts: as_u64(&row[12])?,
        error: optional_text(&row[13], "error")?
            .map(serde_json::from_str::<LoadJobError>)
            .transpose()?,
    };
    job.coherent()?;
    Ok(job)
}
/// Decode one row, containing a decode failure to that row. A primary key
/// that is not text names no job to isolate, so it fails the whole read.
fn decode_keyed(row: &[Value]) -> Result<std::result::Result<LoadJob, LoadLedgerIssue>> {
    let load_id = text(&row[0], "ID")?.to_string();
    Ok(decode(row).map_err(|error| LoadLedgerIssue {
        load_id,
        detail: crate::bootstrap::truncate(error.to_string(), MAX_DETAIL),
    }))
}

/// The canonical frozen page request of `job` at `continuation` under `call_id`.
pub(crate) fn frozen_intent(
    job_id: &str,
    call_id: &str,
    name: &str,
    version: u64,
    args: &Value,
    models: &BTreeMap<String, u64>,
    continuation: LoadNext,
) -> LoadIntent {
    LoadIntent {
        load_id: job_id.to_string(),
        call_id: call_id.to_string(),
        name: name.to_string(),
        version,
        args: args.clone(),
        continuation,
        models: models.clone(),
    }
}
fn next_text(next: &LoadNext) -> Result<Value> {
    Ok(match next {
        None => Value::Null,
        Some(continuation) => json!(canonical_json(&serde_json::to_value(continuation)?)?),
    })
}
fn intent_text(intent: &LoadIntent) -> Result<Value> {
    Ok(json!(canonical_json(&serde_json::to_value(intent)?)?))
}
fn error_text(error: &LoadJobError) -> Result<Value> {
    Ok(json!(canonical_json(&serde_json::to_value(error)?)?))
}
fn written(affected: usize) -> Result<()> {
    if affected == 1 {
        return Ok(());
    }
    Err(invalid("the Load row changed during the transaction"))
}

impl<S: ClientStore> Engine<'_, S> {
    /// The stored job `id`, `None` when there is none; a row that cannot be
    /// decoded is an error naming it.
    pub(crate) fn load_job(&mut self, id: &str) -> Result<Option<LoadJob>> {
        let rows = self.rows(
            &format!("SELECT {COLUMNS} FROM {TABLE} WHERE load_id = ?"),
            &[json!(id)],
        )?;
        rows.rows
            .first()
            .map(|row| decode(row).map_err(|e| invalid(format!("Load {id} cannot be read: {e}"))))
            .transpose()
    }
    /// Up to `limit` jobs in `order` from `offset`, each decoded on its own.
    pub(crate) fn load_scan(
        &mut self,
        filter: &str,
        order: &str,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<std::result::Result<LoadJob, LoadLedgerIssue>>> {
        let rows = self.rows(
            &format!("SELECT {COLUMNS} FROM {TABLE} {filter} ORDER BY {order} LIMIT ? OFFSET ?"),
            &[json!(limit), json!(offset)],
        )?;
        rows.rows.iter().map(|row| decode_keyed(row)).collect()
    }
    /// The next value of a monotone ordering column.
    fn load_counter(&mut self, column: &str) -> Result<u64> {
        let value = self.scalar(
            &format!("SELECT COALESCE(MAX({column}), 0) + 1 FROM {TABLE}"),
            &[],
        )?;
        as_u64(&value.unwrap_or(Value::Null))
    }
    /// Insert a fresh job at the first page with its frozen first request.
    pub(crate) fn insert_load(&mut self, intent: &LoadIntent) -> Result<()> {
        let seq = self.load_counter("seq")?;
        let ready = self.load_counter("ready")?;
        self.exec(
            TABLE,
            "INSERT INTO axton_load (load_id, seq, ready, name, version, args, models, continuation, run, phase, pages, call_id, intent, attempts) \
             VALUES (?,?,?,?,?,?,?,NULL,1,'pending',0,?,?,0)",
            &[
                json!(intent.load_id),
                json!(seq),
                json!(ready),
                json!(intent.name),
                json!(intent.version),
                json!(canonical_json(&intent.args)?),
                json!(canonical_json(&serde_json::to_value(&intent.models)?)?),
                json!(intent.call_id),
                intent_text(intent)?,
            ],
        )?;
        Ok(())
    }
    /// Commit one page's progress for the frozen call it answers: the page
    /// count, the committed continuation, and either the next frozen request
    /// (requeued behind every ready peer) or completion.
    pub(crate) fn advance_load(&mut self, job: &LoadJob, next: &LoadNext) -> Result<()> {
        let ready = self.load_counter("ready")?;
        let call_id = job
            .call_id
            .as_deref()
            .ok_or_else(|| invalid("an active Load has no frozen page"))?;
        let fence = [json!(job.id), json!(job.run), json!(call_id)];
        let affected = match next {
            None => self.exec(
                TABLE,
                "UPDATE axton_load SET phase = 'complete', pages = pages + 1, continuation = NULL, \
                 call_id = NULL, intent = NULL, retry = NULL, attempts = 0, ready = ? \
                 WHERE load_id = ? AND run = ? AND call_id = ? AND phase = 'pending'",
                &[
                    json!(ready),
                    fence[0].clone(),
                    fence[1].clone(),
                    fence[2].clone(),
                ],
            )?,
            Some(_) => {
                let call = uuid::Uuid::new_v4().to_string();
                let intent = frozen_intent(
                    &job.id,
                    &call,
                    &job.name,
                    job.version,
                    &job.args,
                    &job.models,
                    next.clone(),
                );
                self.exec(
                    TABLE,
                    "UPDATE axton_load SET pages = pages + 1, continuation = ?, call_id = ?, \
                     intent = ?, retry = NULL, attempts = 0, ready = ? \
                     WHERE load_id = ? AND run = ? AND call_id = ? AND phase = 'pending'",
                    &[
                        next_text(next)?,
                        json!(call),
                        intent_text(&intent)?,
                        json!(ready),
                        fence[0].clone(),
                        fence[1].clone(),
                        fence[2].clone(),
                    ],
                )?
            }
        };
        written(affected)
    }
    /// Fail the run the fence names, keeping its frozen call and committed
    /// progress. `false` when that call is no longer the current one.
    pub(crate) fn fail_load(
        &mut self,
        id: &str,
        run: u64,
        call_id: &str,
        error: &LoadJobError,
    ) -> Result<bool> {
        Ok(self.exec(
            TABLE,
            "UPDATE axton_load SET phase = 'failed', error = ?, retry = NULL \
             WHERE load_id = ? AND run = ? AND call_id = ? AND phase = 'pending'",
            &[error_text(error)?, json!(id), json!(run), json!(call_id)],
        )? == 1)
    }
    /// Count one more retryable attempt of the current frozen call, which
    /// stays exactly as it is. `false` when that call is no longer current.
    pub(crate) fn retry_later_load(
        &mut self,
        id: &str,
        run: u64,
        call_id: &str,
        class: LoadRetryClass,
    ) -> Result<bool> {
        Ok(self.exec(
            TABLE,
            "UPDATE axton_load SET attempts = attempts + 1, retry = ? \
             WHERE load_id = ? AND run = ? AND call_id = ? AND phase = 'pending'",
            &[json!(class.as_str()), json!(id), json!(run), json!(call_id)],
        )? == 1)
    }
    /// Begin run `job.run + 1` of a failed job from its committed continuation
    /// under a fresh call ID.
    pub(crate) fn rerun_load(&mut self, job: &LoadJob) -> Result<()> {
        let ready = self.load_counter("ready")?;
        let call = uuid::Uuid::new_v4().to_string();
        let intent = frozen_intent(
            &job.id,
            &call,
            &job.name,
            job.version,
            &job.args,
            &job.models,
            job.continuation.clone(),
        );
        written(self.exec(
            TABLE,
            "UPDATE axton_load SET run = run + 1, phase = 'pending', call_id = ?, intent = ?, \
             error = NULL, retry = NULL, attempts = 0, ready = ? \
             WHERE load_id = ? AND run = ? AND phase = 'failed'",
            &[
                json!(call),
                intent_text(&intent)?,
                json!(ready),
                json!(job.id),
                json!(job.run),
            ],
        )?)
    }
    /// Cancel a pending or failed job: no frozen page remains, so every
    /// outstanding answer is inert, and its own once mapping goes with it.
    pub(crate) fn cancel_load(&mut self, id: &str, error: &LoadJobError) -> Result<bool> {
        let affected = self.exec(
            TABLE,
            "UPDATE axton_load SET phase = 'cancelled', call_id = NULL, intent = NULL, \
             retry = NULL, error = ? WHERE load_id = ? AND phase IN ('pending','failed')",
            &[error_text(error)?, json!(id)],
        )?;
        if affected == 1 {
            self.unmap_load(id)?;
        }
        Ok(affected == 1)
    }
    /// Delete a terminal job and the mapping that still names it.
    pub(crate) fn forget_load(&mut self, id: &str) -> Result<bool> {
        let affected = self.exec(
            TABLE,
            "DELETE FROM axton_load WHERE load_id = ? AND phase IN ('complete','failed','cancelled')",
            &[json!(id)],
        )?;
        if affected == 1 {
            self.unmap_load(id)?;
        }
        Ok(affected == 1)
    }
    /// Fail every pending job whose frozen contract `is_available` refuses,
    /// as `error`. Rows that cannot be decoded are left for a named read.
    pub(crate) fn fail_unavailable_loads(
        &mut self,
        is_available: impl Fn(&str, u64, &BTreeMap<String, u64>) -> bool,
        error: &LoadJobError,
    ) -> Result<()> {
        let rows = self.rows(
            "SELECT load_id, name, version, models FROM axton_load WHERE phase = 'pending'",
            &[],
        )?;
        for row in rows.rows {
            let decoded = (|| {
                Ok::<_, axton_core::Error>((
                    text(&row[0], "ID")?.to_string(),
                    text(&row[1], "name")?.to_string(),
                    as_u64(&row[2])?,
                    serde_json::from_str::<BTreeMap<String, u64>>(text(&row[3], "models")?)?,
                ))
            })();
            let Ok((id, name, version, models)) = decoded else {
                continue;
            };
            if !is_available(&name, version, &models) {
                self.exec(
                    TABLE,
                    "UPDATE axton_load SET phase = 'failed', error = ?, retry = NULL \
                     WHERE load_id = ? AND phase = 'pending'",
                    &[error_text(error)?, json!(id)],
                )?;
            }
        }
        Ok(())
    }

    /// The job ID a once key maps to.
    pub(crate) fn load_once(&mut self, key: &str) -> Result<Option<String>> {
        let value = self.scalar(
            "SELECT load_id FROM axton_load_once WHERE key = ?",
            &[json!(key)],
        )?;
        value
            .map(|v| Ok(text(&v, "once mapping")?.to_string()))
            .transpose()
    }
    pub(crate) fn map_load_once(&mut self, key: &LoadOnceKey, id: &str) -> Result<()> {
        self.exec(
            ONCE,
            "INSERT INTO axton_load_once (key, name, version, args, models, load_id) VALUES (?,?,?,?,?,?)",
            &[
                json!(key.key),
                json!(key.name),
                json!(key.version),
                json!(key.args),
                json!(key.models),
                json!(id),
            ],
        )?;
        Ok(())
    }
    /// Point `key` from `from` to `to`, only while it still names `from`.
    pub(crate) fn remap_load_once(&mut self, key: &str, from: &str, to: &str) -> Result<()> {
        written(self.exec(
            ONCE,
            "UPDATE axton_load_once SET load_id = ? WHERE key = ? AND load_id = ?",
            &[json!(to), json!(key), json!(from)],
        )?)
    }
    /// Remove the mapping that still names `id`, if one does.
    fn unmap_load(&mut self, id: &str) -> Result<()> {
        self.exec(
            ONCE,
            "DELETE FROM axton_load_once WHERE load_id = ?",
            &[json!(id)],
        )?;
        Ok(())
    }
    /// Remove every mapping of one Load version and canonical arguments,
    /// whatever Model contracts it was keyed under.
    pub(crate) fn unmap_load_arguments(
        &mut self,
        name: &str,
        version: u64,
        args: &str,
    ) -> Result<usize> {
        self.exec(
            ONCE,
            "DELETE FROM axton_load_once WHERE name = ? AND version = ? AND args = ?",
            &[json!(name), json!(version), json!(args)],
        )
    }
}
