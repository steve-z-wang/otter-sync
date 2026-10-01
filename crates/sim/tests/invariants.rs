//! R2: random operation sequences with every invariant checked after every step.
//! Default is quick; SIM_SEEDS and SIM_STEPS scale it up for a long run.
use axton_sim::Sim;

fn env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[test]
fn random_sequences_violate_no_invariant() {
    let seeds = env("SIM_SEEDS", 60);
    let steps = env("SIM_STEPS", 120);
    let mut comparisons = 0;
    for seed in 0..seeds as u64 {
        match Sim::run_with(seed, 3, steps, false) {
            Ok(n) => comparisons += n,
            Err(failure) => panic!("{failure}"),
        }
    }
    assert!(
        comparisons >= 1000,
        "only {comparisons} content comparisons across all seeds; coverage dropped \
         (periodic settle in Sim::run_with should keep this well above the floor)"
    );
}

#[test]
fn random_sequences_with_direct_writes() {
    let seeds = env("SIM_SEEDS", 60);
    let steps = env("SIM_STEPS", 120);
    for seed in 0..seeds as u64 {
        if let Err(failure) = Sim::run(seed, 3, steps) {
            panic!("{failure}");
        }
    }
}

/// Publishing a change to a scope outside a record's membership is harmless to
/// the engine: loads are scope-blind, so the extra scope delivers the same
/// content at the same stamp. Every invariant still holds with such faults generated.
#[test]
fn publication_outside_membership_violates_no_invariant() {
    for seed in 200..220u64 {
        let mut sim = Sim::new(seed, 2);
        sim.generate_direct = false;
        sim.generate_membership_faults = true;
        for i in 0..2 {
            sim.apply(axton_sim::Action::Subscribe {
                client: i,
                scope: "a".into(),
            })
            .unwrap();
        }
        for step in 0..100 {
            if let Err(error) = sim.step().and_then(|()| sim.check()) {
                let minimal = axton_sim::shrink::shrink(seed, 2, sim.trace.clone());
                let failure = axton_sim::Failure {
                    seed,
                    step,
                    error,
                    trace: sim.trace.clone(),
                    minimal,
                };
                panic!("{failure}");
            }
        }
        for i in 0..2 {
            sim.apply(axton_sim::Action::Restart { client: i }).unwrap();
        }
        sim.settle();
        sim.check().unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        assert_eq!(
            sim.conflicts, 0,
            "seed {seed}: same stamp, same content, never a conflict"
        );
    }
}

#[test]
fn every_run_ends_converged_after_settle() {
    for seed in 100..110u64 {
        let mut sim = Sim::new(seed, 2);
        sim.generate_direct = false;
        for i in 0..2 {
            sim.apply(axton_sim::Action::Subscribe {
                client: i,
                scope: "a".into(),
            })
            .unwrap();
        }
        for step in 0..80 {
            if let Err(error) = sim.step() {
                let minimal = axton_sim::shrink::shrink(seed, 2, sim.trace.clone());
                let failure = axton_sim::Failure {
                    seed,
                    step,
                    error,
                    trace: sim.trace.clone(),
                    minimal,
                };
                panic!("{failure}");
            }
        }
        for i in 0..2 {
            sim.apply(axton_sim::Action::Restart { client: i }).unwrap();
        }
        sim.settle();
        sim.check().unwrap_or_else(|e| panic!("seed {seed}: {e}"));
    }
}
