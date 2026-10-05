//! Hand-authored native carrier traces; expected state never comes from engine output.
#[path = "support/native04.rs"]
mod native04;
use axton_sim::oracle04::scenarios;

#[test]
fn authored_native_traces_compare_complete_committed_snapshots() {
    for scenario in scenarios(include_str!(
        "../../../integration/0.4/native-scenarios.json"
    ))
    .unwrap()
    {
        scenario.verify_with(&mut native04::Adapter::new()).unwrap();
    }
}
