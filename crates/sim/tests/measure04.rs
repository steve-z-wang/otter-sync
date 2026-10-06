//! Measurements of actual actor/SQLite work, not production server capacity.
#[path = "support/measure04.rs"]
mod measure04;

#[test]
fn actual_commit_sizes_and_finite_coverage_report_observed_costs() {
    let mut results = vec![];
    for sample in 0..3 {
        for count in [1, 16, 128, 512] {
            let mut observation = measure04::local_commit(count).unwrap();
            observation["sample"] = sample.into();
            results.push(observation);
        }
        for count in [0, 16, 128, 512] {
            let mut observation = measure04::manifest(count).unwrap();
            observation["sample"] = sample.into();
            results.push(observation);
        }
    }
    println!(
        "AXTON04_MEASUREMENTS={}",
        serde_json::to_string(&results).unwrap()
    );
    if let Ok(path) = std::env::var("AXTON04_MEASUREMENTS") {
        std::fs::write(path, serde_json::to_vec_pretty(&results).unwrap()).unwrap();
    }
}
