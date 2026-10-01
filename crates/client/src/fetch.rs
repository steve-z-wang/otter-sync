//! Client side of Model Fetch: one remote read of one Model through its
//! versioned Loader, stored locally by default
//! ([#153](https://github.com/zanminwang/axton/issues/153)).
//!
//! The client builds the canonical request at its own local Model read
//! version, so stored authority always matches local storage, and applies the
//! one record a stored response carries under the shared stamp rules. Unlike a
//! stream page or a direct Action response, a Fetch is one delivery of one
//! record: a record this client cannot store - an equal-stamp conflict or a
//! state the local tables refuse - fails the whole delivery instead of being
//! reported and skipped, so a successful Fetch always means its authority was
//! stored or was a stale/identical no-op. The runtime owns the request
//! lifecycle ([`crate::runtime`]); nothing here touches the network, the
//! Mutation queue, Stream membership or cursors.
use crate::engine::Engine;
use crate::{ApplyReport, Client, ClientStore, ReportKind};
use axton_core::{FetchRequest, FetchResponse, Result, invalid};
use serde_json::Value;

impl<S: ClientStore> Client<S> {
    /// The canonical request for `model` at `version`, which must be the local
    /// Model read version. The identity is normalized through the same read
    /// contract a server uses, so equal identities spell one canonical request
    /// whatever their property order or numeric spelling. Nothing is written.
    pub fn prepare_fetch(
        &self,
        model: &str,
        version: u64,
        identity: &Value,
        store: bool,
    ) -> Result<FetchRequest> {
        let local = self.schema.model(model)?;
        if local.version != version {
            return Err(invalid(format!(
                "Fetch {model} v{version} is not the local read version v{}",
                local.version
            )));
        }
        let request = FetchRequest {
            call_id: uuid::Uuid::new_v4().to_string(),
            model: model.to_string(),
            version,
            identity: self.schema.record_key(model, identity)?.identity,
            store,
        };
        let request = FetchRequest::decode(&request.encode()?, &self.schema)?;
        if request.store {
            self.freeze_request(&request.call_id);
        }
        Ok(request)
    }
    /// Validate a response against the frozen request bytes it answers.
    pub fn decode_fetch(
        &self,
        request: &[u8],
        response: &[u8],
    ) -> Result<(FetchRequest, FetchResponse)> {
        let request = FetchRequest::decode(request, &self.schema)?;
        let response = FetchResponse::decode(response, &request, &self.schema)?;
        Ok((request, response))
    }
    /// Store a validated response in one local transaction: its one record
    /// under the stamp rules, all or nothing. A response with no authority
    /// (`store: false`, or a failed completion) opens no transaction. The
    /// report carries the completion; its authority is stored only when this
    /// returns `Ok`.
    pub fn apply_fetch_response(&mut self, response: &FetchResponse) -> Result<ApplyReport> {
        if response.records.is_empty() {
            let mut report = ApplyReport::default();
            report.completions.push(response.completion.clone());
            self.retire_request(&response.completion.call_id);
            return Ok(report);
        }
        let token = self.request_token(&response.completion.call_id);
        let report = self.write(|engine| engine.apply_fetch_body(response, token))?;
        self.retire_request(&response.completion.call_id);
        Ok(report)
    }
}

impl<S: ClientStore> Engine<'_, S> {
    /// Stage the response's authority as one delivery. Older and identical
    /// stamps are valid no-ops; a pending optimistic write keeps its visible
    /// row and replays over the new base. Any record the client cannot store
    /// refuses the delivery, which the caller's transaction then rolls back.
    pub(crate) fn apply_fetch_body(
        &mut self,
        response: &FetchResponse,
        token: crate::StoreToken,
    ) -> Result<ApplyReport> {
        let mut report = self.apply_enrolled_records_at(&response.records, &[], token)?;
        if let Some(refused) = report
            .reports
            .iter()
            .find(|report| report.kind != ReportKind::Diverged)
        {
            return Err(invalid(match refused.kind {
                ReportKind::Conflict => format!(
                    "Fetch authority for {} conflicts with local content at stamp {}",
                    refused.model, refused.stamp
                ),
                _ => format!(
                    "Fetch authority for {} could not be stored: {}",
                    refused.model,
                    refused.detail["error"]
                        .as_str()
                        .unwrap_or("the record was refused")
                ),
            }));
        }
        report.completions.push(response.completion.clone());
        Ok(report)
    }
}
