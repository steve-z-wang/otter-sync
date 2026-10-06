//! Independent bounded device-only expectation model. No production imports.
//! Set/Clear describe desired content; lowering uses valid create/delete carrier
//! commands. Full metadata expectations follow the authored no-Stream fixture.
use axton_sim::oracle04::{ScenarioAdapter, scenarios};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    Set { store: usize, text: String },
    Clear { store: usize },
    Restart { store: usize },
}
const NAMES: [&str; 3] = ["alice", "workspaceA", "workspaceB"];
// Two physical files intentionally share a Stream; one belongs to another viewer.
const BINDINGS: [&str; 3] = ["User:alice", "User:workspaceA", "User:alice"];

pub fn generate(seed: u64, count: usize) -> Vec<Action> {
    assert!(count <= 64, "bounded intent count");
    let mut random = seed ^ 0x9e3779b97f4a7c15;
    let mut next = || {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        random
    };
    (0..count)
        .map(|index| {
            let store = (next() % 3) as usize;
            match next() % 4 {
                0 => Action::Clear { store },
                1 => Action::Restart { store },
                _ => Action::Set {
                    store,
                    text: format!("seed-{seed}-intent-{index}"),
                },
            }
        })
        .collect()
}

struct Expected {
    visible: [Option<String>; 3],
    touched: [bool; 3],
    opened: [bool; 3],
}
impl Expected {
    fn snapshot(&self) -> Value {
        let mut stores = json!({});
        for (index, name) in NAMES.iter().enumerate().filter(|(i, _)| self.opened[*i]) {
            let visible = &self.visible[index];
            let mut rows = json!({});
            if let Some(text) = visible {
                rows["Entry:x"] = json!({"text":text});
            }
            let operation = match visible {
                Some(text) => {
                    json!({"model":"Entry","identity":{"id":"x"},"op":"create","values":{"text":text}})
                }
                None => json!({"model":"Entry","identity":{"id":"x"},"op":"delete"}),
            };
            let authority = if self.touched[index] {
                json!([{"model":"Entry","identity":{"id":"x"},"evidence":{"membership":null,"history":{},"current":null},"base":null,"generation":0}])
            } else {
                json!([])
            };
            let layers = if self.touched[index] {
                json!([{"model":"Entry","identity":{"id":"x"},"operations":[operation]}])
            } else {
                json!([])
            };
            stores[*name] = json!({"path":format!("{name}.sqlite"),"binding":BINDINGS[index],"context":"s1","incarnation":1,"open":true,"rows":rows,"authority":authority,"localLayers":layers,"localWrites":[],"ownedOperations":[],"pendingOperations":[],"queue":[],"calls":[],"completions":[],"reads":[],"C":0,"page":[],"manifests":[]});
        }
        json!({"stores":stores})
    }
    fn emit(&self, steps: &mut Vec<Value>, event: Value) {
        steps.push(json!({"event":event,"expect":self.snapshot()}));
    }
    fn clear(&mut self, steps: &mut Vec<Value>, store: usize) {
        if self.visible[store].take().is_some() {
            self.emit(
                steps,
                json!({"kind":"direct","store":NAMES[store],"op":"delete","state":null}),
            );
        }
    }
}

pub fn scenario(seed: u64, actions: &[Action]) -> Value {
    let mut model = Expected {
        visible: [None, None, None],
        touched: [false; 3],
        opened: [false; 3],
    };
    let mut steps = vec![];
    for (index, name) in NAMES.iter().enumerate() {
        model.opened[index] = true;
        model.emit(&mut steps,json!({"kind":"open","store":name,"path":format!("{name}.sqlite"),"binding":BINDINGS[index],"context":"s1"}));
    }
    for action in actions {
        match action {
            Action::Set { store, text } => {
                model.clear(&mut steps, *store);
                model.visible[*store] = Some(text.clone());
                model.touched[*store] = true;
                model.emit(&mut steps,json!({"kind":"direct","store":NAMES[*store],"op":"create","state":{"text":text}}));
            }
            Action::Clear { store } => model.clear(&mut steps, *store),
            Action::Restart { store } => {
                model.emit(&mut steps, json!({"kind":"restart","store":NAMES[*store]}))
            }
        }
    }
    json!({"name":format!("native-local-seed-{seed}"),"requirements":["V01","V07","V11","V13"],"steps":steps})
}

pub fn verify(
    seed: u64,
    actions: &[Action],
    adapter: &mut impl ScenarioAdapter,
) -> Result<(), String> {
    verify_json(&scenario(seed, actions), adapter)
}
pub fn verify_json(scenario: &Value, adapter: &mut impl ScenarioAdapter) -> Result<(), String> {
    scenarios(&json!([scenario]).to_string())?[0]
        .verify_with(adapter)
        .map(|_| ())
}

pub struct Reduction {
    pub actions: Vec<Action>,
    pub checks: usize,
    pub budget_exhausted: bool,
}
/// Deletion reduction rebuilds legal carriers and independent expectations.
/// A bounded budget may return a useful candidate before 1-minimality.
pub fn reduce(actions: &[Action], mut fails: impl FnMut(&[Action]) -> bool) -> Reduction {
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut result = actions.to_vec();
    let mut checks = 0;
    let mut width = result.len().div_ceil(2).max(1);
    loop {
        let mut start = 0;
        while start < result.len() {
            if checks >= 128 || Instant::now() >= deadline {
                return Reduction {
                    actions: result,
                    checks,
                    budget_exhausted: true,
                };
            }
            let end = (start + width).min(result.len());
            let mut candidate = result.clone();
            candidate.drain(start..end);
            checks += 1;
            if fails(&candidate) {
                result = candidate;
                start = 0;
            } else {
                start = end;
            }
        }
        if width == 1 {
            return Reduction {
                actions: result,
                checks,
                budget_exhausted: false,
            };
        }
        width = width.div_ceil(2);
    }
}

pub fn save_failure(
    seed: u64,
    original: &[Action],
    reduced: &Reduction,
    error: &str,
) -> Result<PathBuf, String> {
    let output = json!({"seed":seed,"error":error,"original":scenario(seed,original),"reduced":scenario(seed,&reduced.actions),"reduction":{"checks":reduced.checks,"budgetExhausted":reduced.budget_exhausted,"maxChecks":128,"seconds":30}});
    let mut file = tempfile::Builder::new()
        .prefix(&format!("axton04-failure-{seed}-"))
        .suffix(".json")
        .tempfile()
        .map_err(|e| e.to_string())?;
    std::io::Write::write_all(&mut file, &serde_json::to_vec_pretty(&output).unwrap())
        .map_err(|e| e.to_string())?;
    let (_, path) = file.keep().map_err(|e| e.error.to_string())?;
    Ok(path)
}
