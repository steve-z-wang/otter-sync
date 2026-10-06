//! Frozen read calls and atomic cache/completion installation.
use crate::{ApplyReport, CallCompletion, Client, ClientStore, authority::Held, engine::Engine};
use axton_core::{
    Result, invalid,
    v04::{self, Validate},
};
use serde_json::{Value, json};
fn text<T: serde::Serialize + Validate>(value: &T) -> Result<String> {
    String::from_utf8(v04::encode(value)?).map_err(|_| invalid("protocol UTF8"))
}
impl<S: ClientStore> Engine<'_, S> {
    fn completion04(&mut self, call_id: &str) -> Result<Option<CallCompletion>> {
        self.scalar(
            "SELECT response FROM axton_v04_request WHERE call_id=?",
            &[json!(call_id)],
        )?
        .filter(|v| !v.is_null())
        .map(|v| {
            let response: v04::ReadResponse = v04::decode(
                v.as_str()
                    .ok_or_else(|| invalid("invalid stored response"))?
                    .as_bytes(),
            )?;
            Ok(response.completion)
        })
        .transpose()
    }
    fn freeze_fetch04(&mut self, request: &v04::FetchIntent) -> Result<()> {
        let intent = text(request)?;
        if let Some(saved) = self.scalar(
            "SELECT intent FROM axton_v04_request WHERE call_id=?",
            &[json!(request.call_id)],
        )? {
            if saved != json!(intent) {
                return Err(invalid("intent_mismatch"));
            }
        } else {
            self.exec(
                "axton_v04_request",
                "INSERT INTO axton_v04_request(call_id,intent) VALUES(?,?)",
                &[json!(request.call_id), json!(intent)],
            )?;
        }
        Ok(())
    }
}
impl<S: ClientStore> Client<S> {
    pub fn prepare_fetch04(
        &mut self,
        model: &str,
        version: u64,
        identity: &Value,
        store: bool,
    ) -> Result<v04::FetchIntent> {
        if self.schema.model(model)?.version != version {
            return Err(invalid("Fetch is not the active Model read version"));
        }
        let request = v04::FetchIntent {
            context: self.request_context()?.clone(),
            call_id: uuid::Uuid::new_v4().to_string(),
            model: model.into(),
            version,
            identity: self.schema.record_key(model, identity)?.identity,
            store,
        };
        request.validate()?;
        self.write(|engine| engine.freeze_fetch04(&request))?;
        Ok(request)
    }
    pub fn read_completion04(&mut self, call_id: &str) -> Result<Option<CallCompletion>> {
        self.view(|engine| engine.completion04(call_id))
    }
    pub fn apply_fetch04(
        &mut self,
        request: &v04::FetchIntent,
        response: &v04::ReadResponse,
    ) -> Result<ApplyReport> {
        request.admit_response(response, self.request_context()?)?;
        self.write(|engine| {
            // Compare immutable dispatch bytes even when the call already completed.
            let saved = engine
                .scalar(
                    "SELECT intent FROM axton_v04_request WHERE call_id=?",
                    &[json!(request.call_id)],
                )?
                .ok_or_else(|| invalid("unknown read call"))?;
            if saved != json!(text(request)?) {
                return Err(invalid("intent_mismatch"));
            }
            if let Some(completion) = engine.completion04(&request.call_id)? {
                return Ok(ApplyReport {
                    completions: vec![completion],
                    ..Default::default()
                });
            }
            let mut held = Held::new();
            let applied = engine.stage_cache04(&response.records, request.store, &mut held)?;
            let reports = engine.rebuild_held(&held)?;
            engine.exec(
                "axton_v04_request",
                "UPDATE axton_v04_request SET response=? WHERE call_id=?",
                &[json!(text(response)?), json!(request.call_id)],
            )?;
            Ok(ApplyReport {
                applied,
                reports,
                completions: vec![response.completion.clone()],
                ..Default::default()
            })
        })
    }
}
impl<S: ClientStore> Client<S> {
    pub(crate) fn freeze_query04(
        &mut self,
        request: &crate::DirectActionRequest,
        store: &crate::ActionStore,
    ) -> Result<v04::ReadIntent> {
        if self
            .schema
            .action(&request.call.name, request.call.version)?
            .kind
            != axton_core::CallKind::Query
        {
            return Err(invalid("direct invocation requires a named Query"));
        }
        let store = match store {
            crate::ActionStore::All => true,
            crate::ActionStore::None => false,
            _ => return Err(invalid("Query store must be a boolean")),
        };
        let request = v04::ReadIntent {
            context: self.request_context()?.clone(),
            call_id: request.call.call_id.clone(),
            name: request.call.name.clone(),
            version: request.call.version,
            args: request.call.args.clone(),
            store,
        };
        let intent = text(&request)?;
        self.write(|engine| {
            engine.exec(
                "axton_v04_request",
                "INSERT INTO axton_v04_request(call_id,intent) VALUES(?,?)",
                &[json!(request.call_id), json!(intent)],
            )?;
            Ok(())
        })?;
        Ok(request)
    }
    pub(crate) fn apply_query04(
        &mut self,
        request: &v04::ReadIntent,
        response: &v04::ReadResponse,
        snapshot: Option<(&crate::QueryCacheKey, Option<&str>)>,
    ) -> Result<ApplyReport> {
        request.validate()?;
        response.admit(&request.call_id, &request.context, self.request_context()?)?;
        if let axton_core::ActionOutcome::Succeeded { result } = &response.completion.outcome {
            axton_core::validate_action_result(
                &self.schema,
                self.schema.action(&request.name, request.version)?,
                result,
            )?;
        }
        self.write(|engine| {
            if engine.scalar(
                "SELECT intent FROM axton_v04_request WHERE call_id=?",
                &[json!(request.call_id)],
            )? != Some(json!(text(request)?))
            {
                return Err(invalid("intent_mismatch"));
            }
            if let Some(completion) = engine.completion04(&request.call_id)? {
                return Ok(ApplyReport {
                    completions: vec![completion],
                    ..Default::default()
                });
            }
            let mut held = Held::new();
            let applied = engine.stage_cache04(&response.records, request.store, &mut held)?;
            let reports = engine.rebuild_held(&held)?;
            if let (Some((key, generation)), axton_core::ActionOutcome::Succeeded { result }) =
                (snapshot, &response.completion.outcome)
            {
                engine.save_query_result(key, generation, result)?;
            }
            engine.exec(
                "axton_v04_request",
                "UPDATE axton_v04_request SET response=? WHERE call_id=?",
                &[json!(text(response)?), json!(request.call_id)],
            )?;
            Ok(ApplyReport {
                applied,
                reports,
                completions: vec![response.completion.clone()],
                ..Default::default()
            })
        })
    }
}
