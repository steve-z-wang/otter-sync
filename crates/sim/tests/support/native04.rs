//! Fixed normalization: UUID identities, context hashes and physical paths map
//! one-to-one to trace symbols. Every queried row/field is retained; no expected mask.
use axton_binding::actor;
use axton_sim::oracle04::ScenarioAdapter;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::mpsc,
    time::{Duration, Instant},
};

type Check<T> = Result<T, String>;
const X: &str = "01890f47-1234-7123-8123-000000000001";
fn schema() -> Value {
    json!({"enums":[],"models":[{"name":"Entry","version":1,"identity":["id"],"fields":[{"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"}},{"name":"text","nullable":false,"type":{"kind":"scalar","name":"string"}}]}]})
}
struct Store {
    actor: Option<u64>,
    wakes: mpsc::Receiver<u64>,
    events: Vec<Value>,
    context: Value,
    path: String,
    binding: String,
    symbol: String,
    socket: Option<String>,
    counter: u64,
    plans: BTreeMap<String, String>,
}
impl Drop for Store {
    fn drop(&mut self) {
        if let Some(id) = self.actor.take() {
            actor::detach(id);
        }
    }
}
impl Store {
    fn send(&self, message: Value) -> Check<()> {
        actor::submit(self.actor.ok_or("closed")?, message)
    }
    fn reply(&self, effect: &Value, value: Value) -> Check<()> {
        self.send(json!({"type":"effectResult","effectId":effect["effectId"],"outcome":{"ok":true,"value":value}}))
    }
    fn wait(&mut self, matches: impl Fn(&Value) -> bool) -> Check<Value> {
        loop {
            if let Some(at) = self.events.iter().position(&matches) {
                return Ok(self.events.remove(at));
            }
            // These are only authored initial transport answers, not a server oracle.
            if let Some(at) = self.events.iter().position(|e| {
                e["type"] == "effect"
                    && (e["operation"]["kind"] == "socket"
                        || (e["operation"]["kind"] == "http" && e["operation"]["route"] == "pull"))
            }) {
                let effect = self.events.remove(at);
                if effect["operation"]["kind"] == "socket" {
                    let intent: Value = serde_json::from_str(
                        effect["operation"]["subscribe"]
                            .as_str()
                            .ok_or("subscribe")?,
                    )
                    .map_err(|e| e.to_string())?;
                    self.socket = effect["effectId"].as_str().map(str::to_owned);
                    self.reply(&effect,json!({"event":"message","body":json!({"context":self.context,"cursor":intent["cursor"],"head":intent["cursor"]}).to_string()}))?;
                } else {
                    let intent: Value =
                        serde_json::from_str(effect["operation"]["body"].as_str().ok_or("body")?)
                            .map_err(|e| e.to_string())?;
                    let body = match intent["kind"].as_str() {
                        Some("start") => {
                            json!({"context":self.context,"manifestId":"bootstrap","start":0,"total":0})
                        }
                        Some("tail") => {
                            json!({"context":self.context,"manifestId":"bootstrap","head":0})
                        }
                        _ => return Err(format!("unexpected pull {intent}")),
                    };
                    self.reply(&effect, json!({"status":200,"body":body.to_string()}))?;
                }
                continue;
            }
            self.wakes
                .recv_timeout(Duration::from_secs(15))
                .map_err(|e| format!("actor wake: {e}; pending {:?}", self.events))?;
            self.events
                .extend(actor::drain(self.actor.ok_or("closed")?));
        }
    }
    fn task(&mut self, command: Value) -> Check<Value> {
        self.counter += 1;
        let request = format!("task{}", self.counter);
        self.send(json!({"type":"task","requestId":request,"command":command}))?;
        let result = self.wait(|e| e["type"] == "taskCompleted" && e["requestId"] == request)?;
        if result["ok"] == false {
            Err(result["error"].as_str().unwrap_or("task failed").to_owned())
        } else {
            Ok(result["value"].clone())
        }
    }
    fn sql(&mut self, sql: &str) -> Check<Value> {
        self.task(json!({"kind":"sql","sql":sql,"parameters":[]}))
    }
    fn connect(&mut self) -> Check<()> {
        self.task(json!({"kind":"connect"}))?;
        // A committed read is a queue barrier, not a timing sleep.
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            let rows = self.sql("SELECT coverage FROM axton_v04_bootstrap ORDER BY manifest_id")?;
            if self.socket.is_some()
                && rows.as_array().is_some_and(|r| {
                    r.iter().any(|r| {
                    serde_json::from_str::<Value>(r["coverage"].as_str().unwrap()).unwrap()["tail"]
                        == 0
                })
                })
            {
                return Ok(());
            }
        }
        Err("initial transport did not settle".into())
    }
    fn delta(&mut self, event: &Value) -> Check<()> {
        if self.socket.is_none() {
            self.connect()?;
        }
        let from = self.sql("SELECT cursor FROM axton_v04_store")?[0]["cursor"]
            .as_u64()
            .ok_or("C")?;
        let through = event["cursor"].as_u64().ok_or("cursor")?;
        let changes = if event["kind"] == "advance" {
            json!([])
        } else {
            json!([{"kind":"upsert","record":{"model":"Entry","identity":{"id":X},"cursor":through,"state":event["state"]}}])
        };
        let page = json!({"context":self.context,"pageId":format!("prefix{through}"),"from":from,"to":through,"head":through,"units":[{"through":through,"changes":changes}]});
        self.send(json!({"type":"effectResult","effectId":self.socket,"outcome":{"ok":true,"value":{"event":"message","body":page.to_string()}}}))?;
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            if self.sql("SELECT cursor FROM axton_v04_store")?[0]["cursor"] == through {
                return Ok(());
            }
        }
        Err("Delta did not commit".into())
    }
    fn snapshot(&mut self) -> Check<Value> {
        let mut rows = json!({});
        for mut row in self
            .sql("SELECT * FROM Entry ORDER BY id")?
            .as_array()
            .ok_or("rows")?
            .clone()
        {
            let id = row.as_object_mut().unwrap().remove("id").unwrap();
            if id != X {
                return Err("unregistered identity".into());
            }
            rows["Entry:x"] = row;
        }
        let mut value = json!({"path":self.path,"binding":self.binding,"context":self.symbol,"incarnation":1,"open":true,"rows":rows,"C":self.sql("SELECT cursor FROM axton_v04_store")?[0]["cursor"]});
        for (name, sql, encoded) in [
            (
                "authority",
                "SELECT model,identity,evidence,base,generation FROM axton_v04_record ORDER BY model,identity",
                vec!["identity", "evidence", "base"],
            ),
            (
                "localLayers",
                "SELECT * FROM axton_local_replica_layer ORDER BY model,identity",
                vec!["identity", "operations"],
            ),
            (
                "localWrites",
                "SELECT * FROM axton_local_write ORDER BY sequence",
                vec!["identity", "values"],
            ),
            (
                "ownedOperations",
                "SELECT * FROM axton_mutation_operation ORDER BY ordinal,position",
                vec!["identity", "values"],
            ),
            (
                "pendingOperations",
                "SELECT * FROM axton_v04_op ORDER BY ordinal,position",
                vec![],
            ),
            (
                "queue",
                "SELECT * FROM axton_mutation ORDER BY ordinal",
                vec!["args", "store"],
            ),
            (
                "calls",
                "SELECT * FROM axton_v04_call ORDER BY ordinal",
                vec!["intent", "receipt"],
            ),
            (
                "completions",
                "SELECT * FROM axton_v04_completion ORDER BY call_id",
                vec!["completion"],
            ),
            (
                "reads",
                "SELECT * FROM axton_v04_request ORDER BY call_id",
                vec!["intent", "response"],
            ),
            (
                "page",
                "SELECT * FROM axton_v04_page ORDER BY singleton",
                vec!["page", "progress"],
            ),
            (
                "manifests",
                "SELECT * FROM axton_v04_bootstrap ORDER BY manifest_id",
                vec!["coverage"],
            ),
        ] {
            let mut data = self.sql(sql)?;
            for row in data.as_array_mut().ok_or("snapshot rows")? {
                for column in &encoded {
                    if let Some(text) = row[column].as_str() {
                        row[column] = serde_json::from_str(text)
                            .map_err(|e| format!("decode {name}.{column}: {e}"))?;
                    }
                }
            }
            if name == "page" {
                for row in data.as_array_mut().ok_or("pages")? {
                    let id = row["progress"]["pageId"]
                        .as_str()
                        .ok_or("page ID")?
                        .to_owned();
                    let digest = row["progress"]["plan"]
                        .as_str()
                        .ok_or("plan digest")?
                        .to_owned();
                    if self.plans.get(&id).is_some_and(|saved| saved != &digest) {
                        return Err("immutable page digest changed".into());
                    }
                    self.plans.insert(id.clone(), digest);
                    row["progress"]["plan"] = json!(format!("plan:{id}"));
                }
            }
            value[name] = data;
        }
        replace_incarnation(
            &mut value,
            self.context["incarnation"].as_str().ok_or("incarnation")?,
        );
        normalize(
            &mut value,
            self.context["materialization"]
                .as_str()
                .ok_or("materialization")?,
            &self.symbol,
        );
        Ok(value)
    }
}
fn replace_incarnation(value: &mut Value, incarnation: &str) {
    match value {
        Value::String(text) if text == incarnation => *text = "inc1".into(),
        Value::Array(items) => {
            for item in items {
                replace_incarnation(item, incarnation);
            }
        }
        Value::Object(map) => {
            for item in map.values_mut() {
                replace_incarnation(item, incarnation);
            }
        }
        _ => {}
    }
}
fn normalize(value: &mut Value, materialization: &str, symbol: &str) {
    match value {
        Value::String(text) if text == materialization => *text = symbol.to_owned(),
        Value::String(text) if text == X => *text = "x".into(),
        Value::Array(items) => {
            for item in items {
                normalize(item, materialization, symbol);
            }
        }
        Value::Object(map) => {
            if let Some(cursor) = map.remove(materialization) {
                map.insert(symbol.to_owned(), cursor);
            }
            for item in map.values_mut() {
                normalize(item, materialization, symbol);
            }
        }
        _ => {}
    }
}
pub struct Adapter {
    directory: tempfile::TempDir,
    stores: BTreeMap<String, Store>,
    snapshot: Value,
}
impl Drop for Adapter {
    fn drop(&mut self) {
        // End actors before the TempDir field can unlink their open databases.
        for store in self.stores.values_mut() {
            if let Some(id) = store.actor.take() {
                actor::detach(id);
                assert!(
                    actor::wait_closed(id, Duration::from_secs(20)),
                    "trace actor did not close"
                );
            }
        }
    }
}
impl Adapter {
    pub fn new() -> Self {
        Self {
            directory: tempfile::tempdir().unwrap(),
            stores: BTreeMap::new(),
            snapshot: json!({"stores":{}}),
        }
    }
    fn open(&mut self, event: &Value) -> Check<()> {
        let name = event["store"].as_str().ok_or("store")?.to_owned();
        let path = event["path"].as_str().ok_or("path")?.to_owned();
        let binding = event["binding"].as_str().ok_or("binding")?.to_owned();
        let symbol = event["context"].as_str().ok_or("context")?.to_owned();
        let (tx, wakes) = mpsc::channel();
        let id=actor::open(json!({"type":"open","requestId":"open","path":self.directory.path().join(&path),"schema":schema(),"binding":{"backend":"native04","viewer":binding.strip_prefix("User:").ok_or("binding")?,"stream":binding,"contract":"fixture"}}),Box::new(move|id|{let _=tx.send(id);})).map_err(|e|e.to_string())?;
        let mut store = Store {
            actor: Some(id),
            wakes,
            events: vec![],
            context: Value::Null,
            path,
            binding,
            symbol,
            socket: None,
            counter: 0,
            plans: BTreeMap::new(),
        };
        let opened = store.wait(|e| e["requestId"] == "open")?;
        if opened["ok"] == false {
            return Err(opened["error"].as_str().unwrap_or("open failed").to_owned());
        }
        store.context = opened["value"]["context"].clone();
        if let Some(previous) = self.stores.get(&name)
            && previous.context != store.context
        {
            return Err("normal reopen changed context/incarnation".into());
        }
        if let Some(previous) = self.stores.get(&name) {
            store.plans = previous.plans.clone();
        }
        self.stores.insert(name, store);
        Ok(())
    }
    fn step(&mut self, event: &Value) -> Check<Value> {
        let name = event["store"].as_str().ok_or("store")?;
        if event["kind"] == "open" {
            self.open(event)?;
            return Ok(Value::Null);
        }
        if event["kind"] == "restart" {
            let s = self.stores.get_mut(name).ok_or("store")?;
            let open = json!({"kind":"open","store":name,"path":s.path,"binding":s.binding,"context":s.symbol});
            if let Some(id) = s.actor.take() {
                actor::detach(id);
                if !actor::wait_closed(id, Duration::from_secs(15)) {
                    return Err("actor did not release Store".into());
                }
            }
            self.open(&open)?;
            return Ok(Value::Null);
        }
        let s = self.stores.get_mut(name).ok_or("store")?;
        match event["kind"].as_str().ok_or("kind")? {
            "direct"=>s.task(json!({"kind":"direct","operation":{"model":"Entry","identity":{"id":X},"op":event["op"],"values":event["state"]}})),
            "stream"|"advance"=>{s.delta(event)?;Ok(Value::Null)},
            "close"=>{s.send(json!({"type":"close"}))?;s.wait(|e|e["type"]=="runtimeClosed")?;if let Some(id)=s.actor.take(){actor::detach(id);if !actor::wait_closed(id,Duration::from_secs(15)){return Err("actor did not release Store".into());}}Ok(Value::Null)},
            _=>Err("unsupported native fixture event".into()),
        }
    }
    fn collect(&mut self) -> Check<()> {
        for (name, s) in &mut self.stores {
            if s.actor.is_some() {
                self.snapshot["stores"][name] = s.snapshot()?;
            } else {
                self.snapshot["stores"][name]["open"] = json!(false);
            }
        }
        Ok(())
    }
}
impl ScenarioAdapter for Adapter {
    fn apply(&mut self, event: &Value) -> Check<Value> {
        let result = self.step(event);
        self.collect()?;
        result
    }
    fn snapshot(&self) -> Value {
        self.snapshot.clone()
    }
}
