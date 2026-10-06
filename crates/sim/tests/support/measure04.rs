//! Actual common actor/SQLite measurement carrier. HTTP responses below are
//! authored fixtures, not a production backend or authority installer helper.
use axton_binding::actor;
use serde_json::{Value, json};
use std::{
    sync::mpsc,
    time::{Duration, Instant},
};
type Check<T> = Result<T, String>;
const PAYLOAD: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
struct Host {
    id: u64,
    wake: mpsc::Receiver<u64>,
    events: Vec<Value>,
    sequence: u64,
    context: Value,
    manifest_count: Option<usize>,
    response_bytes: usize,
    pages: usize,
    directory: tempfile::TempDir,
}
impl Drop for Host {
    fn drop(&mut self) {
        actor::detach(self.id);
        assert!(
            actor::wait_closed(self.id, Duration::from_secs(20)),
            "measurement actor did not close"
        );
    }
}
impl Host {
    fn open(count: Option<usize>) -> Check<Self> {
        let directory = tempfile::tempdir().map_err(|e| e.to_string())?;
        let (send, wake) = mpsc::channel();
        let schema = json!({"enums":[],"models":[{"name":"Entry","version":1,"identity":["id"],"bootstrap":count.is_some(),"fields":[{"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"}},{"name":"text","nullable":false,"type":{"kind":"scalar","name":"string"}}]}]});
        let id=actor::open(json!({"type":"open","requestId":"open","path":directory.path().join("store.sqlite"),"schema":schema,"binding":{"backend":"measure04","viewer":"alice","stream":"User:alice","contract":"fixture"}}),Box::new(move|id|{let _=send.send(id);})).map_err(|e|e.to_string())?;
        let mut host = Self {
            id,
            wake,
            events: vec![],
            sequence: 0,
            context: Value::Null,
            manifest_count: count,
            response_bytes: 0,
            pages: 0,
            directory,
        };
        let opened = host.wait(|e| e["requestId"] == "open")?;
        if opened["ok"] != true {
            return Err(format!("open {opened}"));
        }
        host.context = opened["value"]["context"].clone();
        Ok(host)
    }
    fn send(&self, value: Value) -> Check<()> {
        actor::submit(self.id, value)
    }
    fn wait(&mut self, predicate: impl Fn(&Value) -> bool) -> Check<Value> {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if Instant::now() >= deadline {
                return Err("measurement event deadline exceeded".into());
            }
            if let Some(index) = self.events.iter().position(&predicate) {
                return Ok(self.events.remove(index));
            }
            if self.manifest_count.is_some()
                && let Some(index) = self.events.iter().position(|e| {
                    e["type"] == "effect"
                        && (e["operation"]["kind"] == "http" || e["operation"]["kind"] == "socket")
                })
            {
                let effect = self.events.remove(index);
                self.answer(&effect)?;
                continue;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            self.wake
                .recv_timeout(remaining)
                .map_err(|e| format!("wake {e}: {:?}", self.events))?;
            self.events.extend(actor::drain(self.id));
        }
    }
    fn task(&mut self, command: Value) -> Check<Value> {
        self.sequence += 1;
        let request = format!("task{}", self.sequence);
        self.send(json!({"type":"task","requestId":request,"command":command}))?;
        self.completed(&request)
    }
    fn completed(&mut self, request: &str) -> Check<Value> {
        let result = self.wait(|e| e["type"] == "taskCompleted" && e["requestId"] == request)?;
        if result["ok"] == true {
            Ok(result["value"].clone())
        } else {
            Err(format!("task {result}"))
        }
    }
    fn sql(&mut self, sql: &str) -> Check<Value> {
        self.task(json!({"kind":"sql","sql":sql,"parameters":[]}))
    }
    fn answer(&mut self, effect: &Value) -> Check<()> {
        let count = self.manifest_count.unwrap();
        let operation = &effect["operation"];
        let value = if operation["kind"] == "socket" {
            let subscribe: Value =
                serde_json::from_str(operation["subscribe"].as_str().ok_or("subscribe")?)
                    .map_err(|e| e.to_string())?;
            json!({"event":"message","body":json!({"context":self.context,"cursor":subscribe["cursor"],"head":count}).to_string()})
        } else {
            let intent: Value = serde_json::from_str(operation["body"].as_str().ok_or("intent")?)
                .map_err(|e| e.to_string())?;
            let body = match intent["kind"].as_str() {
                Some("start") => {
                    json!({"context":self.context,"manifestId":"measured","start":count,"total":count})
                }
                Some("page") => {
                    let from = intent["from"].as_u64().ok_or("from")? as usize;
                    let limit = intent["limit"].as_u64().ok_or("limit")? as usize;
                    let to = (from + limit).min(count);
                    let items=(from..to).map(|ordinal|json!({"ordinal":ordinal,"change":{"kind":"upsert","record":{"model":"Entry","identity":{"id":format!("k{ordinal}")},"cursor":ordinal+1,"state":{"text":PAYLOAD}}}})).collect::<Vec<_>>();
                    self.pages += 1;
                    json!({"context":self.context,"manifestId":"measured","total":count,"from":from,"to":to,"items":items})
                }
                Some("tail") => {
                    json!({"context":self.context,"manifestId":"measured","head":count})
                }
                None => {
                    json!({"context":self.context,"pageId":"measured-empty","from":intent["after"],"to":intent["after"],"head":count,"units":[]})
                }
                _ => return Err(format!("unexpected {intent}")),
            };
            let body = body.to_string();
            self.response_bytes += body.len();
            json!({"status":200,"body":body})
        };
        self.send(json!({"type":"effectResult","effectId":effect["effectId"],"outcome":{"ok":true,"value":value}}))
    }
    fn file_bytes(&self) -> u64 {
        ["store.sqlite", "store.sqlite-wal"]
            .iter()
            .map(|name| {
                std::fs::metadata(self.directory.path().join(name))
                    .map(|m| m.len())
                    .unwrap_or(0)
            })
            .sum()
    }
}

pub fn local_commit(count: usize) -> Check<Value> {
    let mut host = Host::open(None)?;
    let start = Instant::now();
    host.send(json!({"type":"task","requestId":"transaction","command":{"kind":"transaction"}}))?;
    let callback = host.wait(|e| e["type"] == "effect" && e["operation"]["kind"] == "callback")?;
    let transaction = &callback["operation"]["transactionId"];
    let mut command_bytes = 0;
    for index in 0..count {
        let command = json!({"type":"transactionCommand","requestId":format!("write{index}"),"transactionId":transaction,"command":{"kind":"direct","operation":{"model":"Entry","identity":{"id":format!("k{index}")},"op":"create","values":{"text":PAYLOAD}}}});
        command_bytes += command.to_string().len();
        host.send(command)?;
        host.completed(&format!("write{index}"))?;
    }
    host.send(json!({"type":"callbackResult","effectId":callback["effectId"],"transactionId":transaction,"ok":true}))?;
    host.completed("transaction")?;
    let elapsed = start.elapsed().as_micros();
    assert_eq!(
        host.sql("SELECT count(*) n,sum(length(text)) bytes FROM Entry")?,
        json!([{"n":count,"bytes":count*PAYLOAD.len()}])
    );
    assert_eq!(
        host.sql("SELECT cursor FROM axton_v04_store")?[0]["cursor"],
        0
    );
    Ok(
        json!({"kind":"localTransaction","records":count,"contentBytes":count*PAYLOAD.len(),"carrierCommandBytes":command_bytes,"elapsedMicros":elapsed,"sqliteAndWalBytes":host.file_bytes()}),
    )
}

pub fn manifest(count: usize) -> Check<Value> {
    let mut host = Host::open(Some(count))?;
    let start = Instant::now();
    host.task(json!({"kind":"connect"}))?;
    let deadline = Instant::now() + Duration::from_secs(20);
    let coverage = loop {
        let rows =
            host.sql("SELECT coverage FROM axton_v04_bootstrap WHERE purpose='bootstrap'")?;
        if let Some(row) = rows.as_array().and_then(|r| r.first()) {
            let coverage: Value = serde_json::from_str(row["coverage"].as_str().unwrap())
                .map_err(|e| e.to_string())?;
            if coverage["covered"] == coverage["total"]
                && let Some(tail) = coverage["tail"].as_u64()
                && host.sql("SELECT cursor FROM axton_v04_store")?[0]["cursor"]
                    .as_u64()
                    .is_some_and(|cursor| cursor >= tail)
            {
                break coverage;
            }
        }
        if Instant::now() >= deadline {
            return Err("manifest did not complete".into());
        }
    };
    let elapsed = start.elapsed().as_micros();
    assert_eq!(coverage["covered"], count);
    assert_eq!(coverage["tail"], count);
    assert_eq!(
        host.sql("SELECT cursor FROM axton_v04_store")?[0]["cursor"],
        count
    );
    assert_eq!(host.sql("SELECT count(*) n FROM Entry")?[0]["n"], count);
    assert_eq!(
        host.sql("SELECT count(*) n FROM axton_v04_record")?[0]["n"],
        count
    );
    for row in host
        .sql("SELECT identity,evidence FROM axton_v04_record")?
        .as_array()
        .unwrap()
    {
        let identity: Value =
            serde_json::from_str(row["identity"].as_str().unwrap()).map_err(|e| e.to_string())?;
        let ordinal: usize = identity["id"]
            .as_str()
            .unwrap()
            .strip_prefix('k')
            .ok_or("identity")?
            .parse::<usize>()
            .map_err(|e| e.to_string())?;
        let evidence: Value =
            serde_json::from_str(row["evidence"].as_str().unwrap()).map_err(|e| e.to_string())?;
        assert_eq!(
            evidence["current"],
            json!({"cursor":ordinal+1,"deleted":false,"materialization":host.context["materialization"]})
        );
        assert_eq!(
            evidence["history"][host.context["materialization"].as_str().unwrap()],
            ordinal + 1
        );
    }
    let metadata =
        host.sql("SELECT sum(length(coverage)) n FROM axton_v04_bootstrap")?[0]["n"].clone();
    Ok(
        json!({"kind":"manifest","records":count,"elapsedMicros":elapsed,"coverageBytes":metadata,"responseBytes":host.response_bytes,"pages":host.pages,"sqliteAndWalBytes":host.file_bytes()}),
    )
}
