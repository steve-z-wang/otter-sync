//! Bounded deterministic local-carrier schedules, independent expected state.
#[path = "support/native04.rs"]
mod native04;
#[path = "support/seeded04.rs"]
mod seeded04;
use axton_sim::oracle04::ScenarioAdapter;
use serde_json::Value;

#[test]
fn bounded_seeds_replay_full_committed_native_snapshots() {
    if let Ok(path) = std::env::var("AXTON04_REPLAY") {
        let artifact: Value =
            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        seeded04::verify_json(&artifact["reduced"], &mut native04::Adapter::new()).unwrap();
        return;
    }
    let seeds = std::env::var("AXTON04_SEED")
        .map(|value| vec![value.parse::<u64>().expect("decimal AXTON04_SEED")])
        .unwrap_or_else(|_| vec![1, 7, 42, 321, 1024, 65537, 987654321, u64::MAX]);
    for seed in seeds {
        let actions = seeded04::generate(seed, 24);
        if let Err(error) = seeded04::verify(seed, &actions, &mut native04::Adapter::new()) {
            let reduced = seeded04::reduce(&actions, |candidate| {
                seeded04::verify(seed, candidate, &mut native04::Adapter::new()).is_err()
            });
            let path = seeded04::save_failure(seed, &actions, &reduced, &error).unwrap();
            panic!(
                "seed {seed}: {error}; replay/reduced artifact {}",
                path.display()
            );
        }
    }
}

// A deliberate observation fault validates replay/reduction over actual native
// executions. It is not a simulated production engine or product acceptance.
struct CorruptObservation(native04::Adapter);
impl ScenarioAdapter for CorruptObservation {
    fn apply(&mut self, event: &Value) -> Result<Value, String> {
        self.0.apply(event)
    }
    fn snapshot(&self) -> Value {
        let mut snapshot = self.0.snapshot();
        for store in snapshot["stores"].as_object_mut().unwrap().values_mut() {
            if store["rows"]["Entry:x"]["text"] == "fault-signal" {
                store["rows"]["Entry:x"]["text"] = "corrupt".into();
            }
        }
        snapshot
    }
}
#[test]
fn failure_replay_reduces_to_one_required_intent_without_changing_expectations() {
    let mut actions = seeded04::generate(42, 12);
    actions.insert(
        6,
        seeded04::Action::Set {
            store: 1,
            text: "fault-signal".into(),
        },
    );
    let replay = |candidate: &[seeded04::Action]| {
        seeded04::verify(
            42,
            candidate,
            &mut CorruptObservation(native04::Adapter::new()),
        )
        .is_err()
    };
    assert!(replay(&actions));
    let reduced = seeded04::reduce(&actions, replay);
    assert_eq!(
        reduced.actions,
        vec![seeded04::Action::Set {
            store: 1,
            text: "fault-signal".into()
        }]
    );
    assert!(!reduced.budget_exhausted);
    assert!(replay(&reduced.actions));
    assert!(!replay(&[]));
    // JSON replay contains independent expectations, not actual snapshots.
    let encoded = serde_json::to_string(&seeded04::scenario(42, &reduced.actions)).unwrap();
    assert!(encoded.contains("fault-signal"));
    assert!(!encoded.contains("corrupt"));
    let artifact =
        seeded04::save_failure(42, &actions, &reduced, "injected observation fault").unwrap();
    let saved: Value = serde_json::from_str(&std::fs::read_to_string(&artifact).unwrap()).unwrap();
    assert_eq!(saved["reduction"]["budgetExhausted"], false);
    seeded04::verify_json(&saved["reduced"], &mut native04::Adapter::new()).unwrap();
    std::fs::remove_file(artifact).unwrap();
}
