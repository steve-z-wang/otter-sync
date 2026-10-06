//! Durable carrier identity and recovery reads used by the existing DownlinkWorker.
use crate::{Client, ClientStore};
use axton_core::{
    RecordKey, Result, invalid,
    v04::{self},
};
use serde_json::json;
impl<S: ClientStore> Client<S> {
    pub(crate) fn initialized04(&mut self) -> Result<bool> {
        self.view(|e| {
            Ok(e.scalar(
                "SELECT initialized FROM axton_v04_store WHERE singleton=1",
                &[],
            )?
            .unwrap_or(json!(0))
                == json!(1))
        })
    }
    pub(crate) fn held_keys04(&mut self) -> Result<Vec<RecordKey>> {
        self.view(|e| {
            e.rows(
                "SELECT model,identity,evidence FROM axton_v04_record ORDER BY model,identity",
                &[],
            )?
            .rows
            .into_iter()
            .map(|row| {
                let evidence = v04::decode::<v04::RecordEvidence>(
                    row[2]
                        .as_str()
                        .ok_or_else(|| invalid("record evidence"))?
                        .as_bytes(),
                );
                evidence.and_then(|evidence| {
                    if evidence.history.is_empty() && evidence.membership.is_none() {
                        return Ok(None);
                    }
                    Ok(Some(RecordKey {
                        model: row[0].as_str().ok_or_else(|| invalid("model"))?.into(),
                        identity: serde_json::from_str(
                            row[1].as_str().ok_or_else(|| invalid("identity"))?,
                        )?,
                    }))
                })
            })
            .collect::<Result<Vec<_>>>()
            .map(|keys| keys.into_iter().flatten().collect())
        })
    }
    pub(crate) fn manifest_coverage04(
        &mut self,
        id: &str,
    ) -> Result<Option<v04::BootstrapCoverage>> {
        self.view(|e| {
            e.scalar(
                "SELECT coverage FROM axton_v04_bootstrap WHERE manifest_id=?",
                &[json!(id)],
            )?
            .map(|v| v04::decode(v.as_str().ok_or_else(|| invalid("coverage"))?.as_bytes()))
            .transpose()
        })
    }
    pub(crate) fn saved_delta04(&mut self) -> Result<Option<v04::DeltaPage>> {
        self.view(|e| {
            e.scalar("SELECT page FROM axton_v04_page WHERE singleton=1", &[])?
                .map(|v| v04::decode(v.as_str().ok_or_else(|| invalid("delta plan"))?.as_bytes()))
                .transpose()
        })
    }
    pub(crate) fn frozen_bootstrap04(
        &mut self,
        owner: &str,
        intent: &v04::BootstrapIntent,
    ) -> Result<v04::BootstrapIntent> {
        let owner = format!(
            "{}:{}:{owner}",
            self.request_context()?.incarnation,
            self.request_context()?.materialization
        );
        self.write(|e| {
            if let Some(saved) = e.scalar(
                "SELECT intent FROM axton_v04_delivery_request WHERE owner=?",
                &[json!(owner)],
            )? {
                return v04::decode(
                    saved
                        .as_str()
                        .ok_or_else(|| invalid("carrier intent"))?
                        .as_bytes(),
                );
            }
            let bytes = v04::encode(intent)?;
            e.exec(
                "axton_v04_delivery_request",
                "INSERT INTO axton_v04_delivery_request(owner,intent) VALUES(?,?)",
                &[
                    json!(owner),
                    json!(String::from_utf8(bytes).map_err(|_| invalid("UTF8"))?),
                ],
            )?;
            Ok(intent.clone())
        })
    }
    pub(crate) fn receipt_materialization04(
        &mut self,
    ) -> Result<Option<v04::BootstrapReceiptTargets>> {
        let active = self.request_context()?.clone();
        self.view(|e|{for row in e.rows("SELECT receipt FROM axton_v04_call WHERE status='acceptedAwaiting' ORDER BY ordinal",&[])?.rows{let receipt:v04::MutationReceipt=v04::decode(row[0].as_str().ok_or_else(||invalid("receipt"))?.as_bytes())?;let mut keys=Vec::new();for target in &receipt.targets{if target.disposition(&active.materialization,&e.evidence04(target.key())?)?==v04::SettlementDisposition::AwaitStream{keys.push(target.key().clone());}}if !keys.is_empty(){return Ok(Some(v04::BootstrapReceiptTargets{call_id:receipt.completion.call_id,keys}));}}Ok(None)})
    }
}
