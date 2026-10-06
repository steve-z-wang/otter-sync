//! Independent protocol-0.4 requirements model, not a production engine oracle.
//! No production admission, DTO, storage or settlement code is imported.
use serde_json::{Value, json};
use std::collections::BTreeMap;

type Check<T> = Result<T, String>;
fn text<'a>(value: &'a Value, field: &str) -> Check<&'a str> {
    value[field]
        .as_str()
        .ok_or_else(|| format!("missing {field}"))
}
fn number(value: &Value, field: &str) -> Check<u64> {
    value[field]
        .as_u64()
        .filter(|n| *n <= 9_007_199_254_740_991)
        .ok_or_else(|| format!("invalid {field}"))
}
fn patch(base: &Value, change: &Value) -> Value {
    if change.is_null() {
        return Value::Null;
    }
    let mut row = base.as_object().cloned().unwrap_or_default();
    if let Some(fields) = change.as_object() {
        row.extend(fields.clone());
    }
    Value::Object(row)
}
#[derive(Clone, Default)]
struct Row {
    base: Value,
    generation: u64,
    guard: Option<(u64, bool)>,
    history: BTreeMap<String, u64>,
    membership: Option<(u64, bool)>,
}
#[derive(Clone)]
struct Operation {
    key: String,
    state: Value,
    owner: Option<String>,
    generation: u64,
}
#[derive(Clone)]
struct Store {
    path: String,
    binding: String,
    context: String,
    incarnation: u64,
    open: bool,
    rows: BTreeMap<String, Row>,
    operations: Vec<Operation>,
    calls: BTreeMap<String, Value>,
    returns: BTreeMap<String, Value>,
    requests: BTreeMap<String, bool>,
    cursor: u64,
    progress: Value,
    bootstrap: Value,
}
impl Store {
    fn visible(&self, key: &str) -> Value {
        let mut state = self
            .rows
            .get(key)
            .map(|row| row.base.clone())
            .unwrap_or(Value::Null);
        for operation in self.operations.iter().filter(|op| op.key == key) {
            state = patch(&state, &operation.state);
        }
        state
    }
    fn stream(&mut self, event: &Value) -> Check<()> {
        let key = text(event, "key")?.to_owned();
        let cursor = number(event, "cursor")?;
        let context = event["context"]
            .as_str()
            .unwrap_or(&self.context)
            .to_owned();
        if context != self.context {
            return Err("context_mismatch".into());
        }
        let row = self.rows.entry(key.clone()).or_default();
        let previous = row.history.values().copied().max().unwrap_or(0);
        if cursor == 0 {
            return Err("invalid cursor".into());
        }
        if row
            .history
            .get(&context)
            .is_some_and(|held| *held >= cursor)
            || row
                .membership
                .is_some_and(|(held, live)| !live && held >= cursor)
            || previous > cursor
        {
            return Ok(());
        }
        let rematerialize = previous == cursor;
        row.base = event["state"].clone();
        row.history.insert(context, cursor);
        if !rematerialize {
            row.generation += 1;
            self.operations
                .retain(|op| op.key != key || op.owner.is_some());
        }
        let direct = self
            .operations
            .iter()
            .any(|op| op.key == key && op.owner.is_none());
        row.guard = if direct && !row.base.is_null() {
            None
        } else {
            Some((cursor, row.base.is_null()))
        };
        row.membership = Some((cursor, true));
        Ok(())
    }
    fn direct(&mut self, event: &Value, owner: Option<String>) -> Check<()> {
        let key = text(event, "key")?.to_owned();
        let state = event["state"].clone();
        let row = self.rows.entry(key.clone()).or_default();
        if owner.is_none() && row.guard.is_some_and(|(_, deleted)| !deleted) {
            row.guard = None;
        }
        self.operations.push(Operation {
            key,
            state,
            owner,
            generation: row.generation,
        });
        Ok(())
    }
    fn remove(&mut self, event: &Value) -> Check<()> {
        let row = self.rows.entry(text(event, "key")?.to_owned()).or_default();
        let cursor = number(event, "cursor")?;
        let old = row
            .history
            .values()
            .copied()
            .max()
            .unwrap_or(0)
            .max(row.membership.map_or(0, |(n, _)| n));
        if cursor > old {
            row.membership = Some((cursor, false));
            if row.guard.is_some_and(|(_, deleted)| !deleted) {
                row.guard = None;
            }
        }
        Ok(())
    }
    fn read(&mut self, event: &Value) -> Check<()> {
        if event["context"]
            .as_str()
            .is_some_and(|context| context != self.context)
        {
            return Err("context_mismatch".into());
        }
        let id = text(event, "id")?.to_owned();
        let store_result = event["storeResult"].as_bool().unwrap_or(true);
        if self
            .requests
            .get(&id)
            .is_some_and(|mode| *mode != store_result)
        {
            return Err("intent_mismatch".into());
        }
        if self.returns.contains_key(&id) {
            return Ok(());
        }
        self.requests.insert(id.clone(), store_result);
        self.returns.insert(id, event["result"].clone());
        if !store_result {
            return Ok(());
        }
        for (key, state) in event["records"]
            .as_object()
            .ok_or("records must be an object")?
        {
            if state.is_null() {
                continue;
            }
            let deleted_parent = event["cascadeParents"][key].as_str().is_some_and(|parent| {
                self.rows
                    .get(parent)
                    .is_some_and(|row| row.guard.is_some_and(|(_, deleted)| deleted))
            });
            if !deleted_parent && self.rows.get(key).is_none_or(|row| row.guard.is_none()) {
                self.rows.entry(key.clone()).or_default().base = state.clone();
                self.operations
                    .retain(|op| &op.key != key || op.owner.is_some());
            }
        }
        Ok(())
    }
    fn enqueue(&mut self, event: &Value) -> Check<()> {
        let id = text(event, "id")?.to_owned();
        if self.calls.contains_key(&id) {
            return Err("duplicate Call".into());
        }
        for operation in event["operations"]
            .as_array()
            .ok_or("operations must be array")?
        {
            self.direct(operation, Some(id.clone()))?;
        }
        self.calls.insert(id, json!({"status":"queued","context":self.context,"incarnation":self.incarnation,"targets":[],"result":null}));
        Ok(())
    }
    fn receipt(&mut self, event: &Value) -> Check<()> {
        let id = text(event, "id")?;
        let call = self.calls.get_mut(id).ok_or("unknown Call")?;
        if call["context"] != event["context"] || call["incarnation"] != self.incarnation {
            return Err("intent_mismatch".into());
        }
        if call["status"] != "queued" {
            return Err("Call already accepted".into());
        }
        call["status"] = json!("acceptedAwaiting");
        call["targets"] = event["targets"].clone();
        call["result"] = event["result"].clone();
        Ok(())
    }
    fn settle(&mut self, event: &Value) -> Check<()> {
        let id = text(event, "id")?;
        let call = self.calls.get(id).ok_or("unknown Call")?.clone();
        if call["status"] == "completed" {
            return Ok(());
        }
        if call["status"] != "acceptedAwaiting" {
            return Err("Call not accepted".into());
        }
        let targets = call["targets"].as_array().ok_or("targets must be array")?;
        let mut canonical = BTreeMap::new();
        for target in targets {
            let key = text(target, "key")?;
            let row = self.rows.get(key).cloned().unwrap_or_default();
            if let Some(required) = target["cursor"].as_u64() {
                if row
                    .history
                    .get(&self.context)
                    .is_some_and(|installed| *installed >= required)
                {
                    canonical.insert(key.to_owned(), None);
                    continue;
                }
                // This is membership loss, never proof of the required content.
                if !row
                    .membership
                    .is_some_and(|(position, live)| !live && position > required)
                {
                    return Ok(());
                }
            }
            canonical.insert(
                key.to_owned(),
                if row.guard.is_none() {
                    Some(target["state"].clone())
                } else {
                    None
                },
            );
        }
        if event["failLocal"] == true {
            return Err("injected failure".into());
        }
        for operation in &mut self.operations {
            if operation.owner.as_deref() != Some(id) {
                continue;
            }
            // Acceptance finalizes the original owned operation; it cannot
            // move that operation after intervening Stream authority.
            if self
                .rows
                .get(&operation.key)
                .is_some_and(|row| row.generation != operation.generation)
            {
                continue;
            }
            if let Some(state) = canonical.get(&operation.key) {
                if let Some(state) = state {
                    operation.state = state.clone();
                    operation.owner = None;
                }
                // Mark authority-acknowledged owned operations for removal below.
            } else {
                operation.owner = None; // device-only companion survives success
                if let Some(row) = self.rows.get_mut(&operation.key)
                    && row.guard.is_some_and(|(_, deleted)| !deleted)
                {
                    row.guard = None;
                }
            }
        }
        self.operations
            .retain(|operation| operation.owner.as_deref() != Some(id));
        self.calls.get_mut(id).unwrap()["status"] = json!("completed");
        Ok(())
    }
    fn refuse(&mut self, event: &Value) -> Check<()> {
        let id = text(event, "id")?;
        let call = self.calls.get_mut(id).ok_or("unknown Call")?;
        if call["status"] != "queued" {
            return Err("Call not queued".into());
        }
        call["status"] = json!("refused");
        call["result"] = event["result"].clone();
        self.operations
            .retain(|operation| operation.owner.as_deref() != Some(id));
        Ok(())
    }
    fn page(&mut self, event: &Value) -> Check<()> {
        let from = number(event, "from")?;
        if from != self.cursor {
            return Err("prefix mismatch".into());
        }
        if !self.progress.is_null()
            && self.progress["next"].as_u64()
                != self.progress["units"]
                    .as_array()
                    .map(|units| units.len() as u64)
        {
            return Err("page already active".into());
        }
        let to = number(event, "to")?;
        let head = if event.get("head").is_some() {
            number(event, "head")?
        } else {
            to
        };
        if to < from || to > head {
            return Err("invalid prefix proof".into());
        }
        let units = event["units"].as_array().ok_or("units must be array")?;
        let mut prefix = from;
        for unit in units {
            let through = number(unit, "through")?;
            if through <= prefix || through > to {
                return Err("invalid prefix proof".into());
            }
            for change in unit["changes"].as_array().ok_or("changes must be array")? {
                let position = number(change, "cursor")?;
                if position <= prefix || position > head {
                    return Err("invalid prefix proof".into());
                }
            }
            prefix = through;
        }
        if prefix != to {
            return Err("invalid prefix proof".into());
        }
        self.progress = json!({"id":event["id"],"from":from,"to":to,"next":0,"units":units});
        Ok(())
    }
    fn unit(&mut self, event: &Value) -> Check<()> {
        let index = number(event, "index")?;
        if self.progress["next"] != index || self.progress["id"] != event["id"] {
            return Err("unit prefix mismatch".into());
        }
        let unit = self.progress["units"]
            .get(index as usize)
            .ok_or("unit out of range")?
            .clone();
        for change in unit["changes"].as_array().ok_or("changes must be array")? {
            match text(change, "kind")? {
                "stream" => self.stream(change)?,
                "remove" => self.remove(change)?,
                _ => return Err("invalid change".into()),
            }
        }
        if event["failLocal"] == true {
            return Err("injected failure".into());
        }
        if let Some(field) = unit["uniqueField"].as_str() {
            let mut seen = std::collections::BTreeSet::new();
            for key in self.rows.keys() {
                let state = self.visible(key);
                if !state[field].is_null() && !seen.insert(state[field].to_string()) {
                    return Err("unique constraint".into());
                }
            }
        }
        self.cursor = number(&unit, "through")?;
        self.progress["next"] = json!(index + 1);
        if self.bootstrap["tail"]
            .as_u64()
            .is_some_and(|tail| self.cursor >= tail)
        {
            self.bootstrap["completed"] = json!(true);
        }
        Ok(())
    }
    fn bootstrap_start(&mut self, event: &Value) -> Check<()> {
        self.bootstrap = json!({"id":event["id"],"keys":event["keys"],"barrier":number(event,"barrier")?,"next":0,"tail":null,"completed":false});
        Ok(())
    }
    fn bootstrap_item(&mut self, event: &Value) -> Check<()> {
        let ordinal = number(event, "ordinal")?;
        if self.bootstrap["next"] != ordinal
            || self.bootstrap["keys"].get(ordinal as usize) != Some(&event["change"]["key"])
        {
            return Err("manifest prefix mismatch".into());
        }
        match text(&event["change"], "kind")? {
            "stream" => self.stream(&event["change"])?,
            "remove" => self.remove(&event["change"])?,
            _ => return Err("invalid change".into()),
        }
        if event["failLocal"] == true {
            return Err("injected failure".into());
        }
        self.bootstrap["next"] = json!(ordinal + 1);
        Ok(())
    }
    fn bootstrap_tail(&mut self, event: &Value) -> Check<()> {
        let keys = self.bootstrap["keys"].as_array().ok_or("no manifest")?;
        if self.bootstrap["next"] != keys.len() {
            return Err("manifest incomplete".into());
        }
        let tail = number(event, "cursor")?;
        if let Some(captured) = self.bootstrap["tail"].as_u64() {
            return if captured == tail {
                Ok(())
            } else {
                Err("tail mismatch".into())
            };
        }
        if tail < self.bootstrap["barrier"].as_u64().ok_or("no barrier")? {
            return Err("prefix regression".into());
        }
        self.bootstrap["tail"] = json!(tail);
        self.bootstrap["completed"] = json!(self.cursor >= tail);
        Ok(())
    }
    fn snapshot(&self) -> Value {
        let mut rows = serde_json::Map::new();
        let mut base = serde_json::Map::new();
        let mut evidence = serde_json::Map::new();
        for (key, row) in &self.rows {
            let state = self.visible(key);
            if !state.is_null() {
                rows.insert(key.clone(), state);
            }
            base.insert(key.clone(), row.base.clone());
            evidence.insert(key.clone(),json!({"cursor":row.guard.map(|(n,_)|n),"deleted":row.guard.is_some_and(|(_,deleted)|deleted),"G":row.history,"membership":row.membership.map(|(cursor,live)|json!({"cursor":cursor,"live":live}))}));
        }
        let operations = |owned: bool| {
            self.operations
                .iter()
                .filter(|op| op.owner.is_some() == owned)
                .map(|op| {
                    let mut value = json!({"key":op.key,"state":op.state});
                    if let Some(owner) = &op.owner {
                        value["call"] = json!(owner);
                    }
                    value
                })
                .collect::<Vec<_>>()
        };
        let queue = self
            .calls
            .iter()
            .filter(|(_, call)| call["status"] == "queued" || call["status"] == "acceptedAwaiting")
            .map(|(id, _)| id)
            .collect::<Vec<_>>();
        json!({"path":self.path,"binding":self.binding,"context":self.context,"incarnation":self.incarnation,"open":self.open,"rows":rows,"base":base,"evidence":evidence,"direct":operations(false),"pending":operations(true),"queue":queue,"calls":self.calls,"returns":self.returns,"requests":self.requests,"C":self.cursor,"progress":self.progress,"bootstrap":self.bootstrap})
    }
}
#[derive(Clone, Default)]
pub struct Oracle {
    stores: BTreeMap<String, Store>,
}
impl Oracle {
    /// Atomic symbolic unit. Real consumers must supply their own commits/reopens.
    pub fn apply(&mut self, event: &Value) -> Check<Value> {
        let before = self.clone();
        match self.step(event) {
            Ok(()) => Ok(self.snapshot()),
            Err(error) => {
                *self = before;
                Err(error)
            }
        }
    }
    fn step(&mut self, event: &Value) -> Check<()> {
        let kind = text(event, "kind")?;
        let name = text(event, "store")?.to_owned();
        if kind == "open" {
            let path = text(event, "path")?;
            let binding = text(event, "binding")?;
            if self
                .stores
                .values()
                .any(|store| store.path == path && store.open)
            {
                return Err("store_in_use".into());
            }
            if let Some(store) = self.stores.get_mut(&name) {
                if store.path != path || store.binding != binding {
                    return Err("binding_mismatch".into());
                }
                store.open = true;
            } else {
                self.stores.insert(
                    name,
                    Store {
                        path: path.into(),
                        binding: binding.into(),
                        context: text(event, "context")?.into(),
                        incarnation: 1,
                        open: true,
                        rows: BTreeMap::new(),
                        operations: vec![],
                        calls: BTreeMap::new(),
                        returns: BTreeMap::new(),
                        requests: BTreeMap::new(),
                        cursor: 0,
                        progress: Value::Null,
                        bootstrap: Value::Null,
                    },
                );
            }
            return Ok(());
        }
        if kind == "transaction" {
            for child in event["events"]
                .as_array()
                .ok_or("events must be an array")?
            {
                self.step(child)?;
            }
            return Ok(());
        }
        let store = self.stores.get_mut(&name).ok_or("unknown Store")?;
        if !store.open && kind != "restart" {
            return Err("closed Store".into());
        }
        match kind {
            "enqueue" => store.enqueue(event),
            "receipt" => store.receipt(event),
            "settle" => store.settle(event),
            "refuse" => store.refuse(event),
            "page" => store.page(event),
            "unit" => store.unit(event),
            "bootstrap" => store.bootstrap_start(event),
            "item" => store.bootstrap_item(event),
            "tail" => store.bootstrap_tail(event),
            "stream" => store.stream(event),
            "direct" => store.direct(event, None),
            "remove" => store.remove(event),
            "read" => store.read(event),
            "context" => {
                store.context = text(event, "context")?.into();
                Ok(())
            }
            "advance" => {
                let cursor = number(event, "cursor")?;
                if cursor < store.cursor {
                    return Err("prefix regression".into());
                }
                store.cursor = cursor;
                Ok(())
            }
            "restart" => Ok(()),
            "close" => {
                store.open = false;
                Ok(())
            }
            "fail" => Err("injected failure".into()),
            _ => Err(format!("unknown event {kind}")),
        }
    }
    pub fn snapshot(&self) -> Value {
        json!({"stores":self.stores.iter().map(|(name,store)|(name.clone(),store.snapshot())).collect::<BTreeMap<_,_>>()})
    }
}

/// Neutral JSON traces can be replayed against real server/client adapters later.
#[derive(Clone)]
pub struct Scenario {
    pub name: String,
    pub requirements: Vec<String>,
    steps: Vec<Value>,
}
pub fn scenarios(source: &str) -> Check<Vec<Scenario>> {
    let value: Value = serde_json::from_str(source).map_err(|error| error.to_string())?;
    value
        .as_array()
        .ok_or("scenarios must be an array")?
        .iter()
        .map(|scenario| {
            Ok(Scenario {
                name: text(scenario, "name")?.into(),
                requirements: scenario["requirements"]
                    .as_array()
                    .ok_or("requirements must be an array")?
                    .iter()
                    .map(|id| {
                        id.as_str()
                            .map(str::to_owned)
                            .ok_or_else(|| "invalid requirement".to_owned())
                    })
                    .collect::<Check<_>>()?,
                steps: scenario["steps"]
                    .as_array()
                    .ok_or("steps must be an array")?
                    .clone(),
            })
        })
        .collect()
}
fn expected_patch(expected: &mut Value, change: &Value, field: &str) {
    if [
        "rows",
        "base",
        "returns",
        "requests",
        "calls",
        "direct",
        "pending",
        "queue",
        "progress",
        "bootstrap",
    ]
    .contains(&field)
        || !change.is_object()
    {
        *expected = change.clone();
    } else if let Some(map) = change.as_object() {
        if !expected.is_object() {
            *expected = json!({});
        }
        for (key, value) in map {
            expected_patch(
                expected
                    .as_object_mut()
                    .unwrap()
                    .entry(key)
                    .or_insert(Value::Null),
                value,
                key,
            );
        }
    }
}
/// A production adapter must normalize its actual committed storage into this
/// snapshot vocabulary. Failed apply must expose the unchanged durable snapshot.
pub trait ScenarioAdapter {
    fn apply(&mut self, event: &Value) -> Check<Value>;
    fn snapshot(&self) -> Value;
}
impl ScenarioAdapter for Oracle {
    fn apply(&mut self, event: &Value) -> Check<Value> {
        Oracle::apply(self, event)
    }
    fn snapshot(&self) -> Value {
        Oracle::snapshot(self)
    }
}
impl Scenario {
    pub fn verify(&self) -> Check<Vec<Value>> {
        self.verify_with(&mut Oracle::default())
    }
    /// The same authored expectations can check an engine without deriving any
    /// expected state from that engine or from this requirements model.
    pub fn verify_with(&self, adapter: &mut impl ScenarioAdapter) -> Check<Vec<Value>> {
        let mut expected = json!({"stores":{}});
        let mut trace = vec![];
        for (index, step) in self.steps.iter().enumerate() {
            let outcome = adapter.apply(&step["event"]);
            if let Some(error) = step["error"].as_str() {
                if outcome.as_ref().err().map(String::as_str) != Some(error) {
                    return Err(format!(
                        "{} step {index}: expected error {error}, got {outcome:?}",
                        self.name
                    ));
                }
            } else {
                outcome.map_err(|error| format!("{} step {index}: {error}", self.name))?;
            }
            expected_patch(&mut expected, &step["expect"], "");
            let actual = adapter.snapshot();
            if actual != expected {
                return Err(format!(
                    "{} step {index}: expected {expected}\nactual {actual}",
                    self.name
                ));
            }
            trace.push(actual);
        }
        Ok(trace)
    }
}
