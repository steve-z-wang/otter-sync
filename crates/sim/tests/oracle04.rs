//! Protocol-0.4 requirements oracle; this does not execute a production engine.
use axton_sim::oracle04::{Oracle, scenarios};
use serde_json::json;

#[test]
fn hand_authored_protocol04_traces_match_the_independent_oracle() {
    let fixture = include_str!("../../../integration/0.4/scenarios.json");
    let errors = scenarios(fixture)
        .unwrap()
        .into_iter()
        .filter_map(|scenario| scenario.verify().err())
        .collect::<Vec<_>>();
    assert!(errors.is_empty(), "{}", errors.join("\n\n"));
}

#[test]
fn trace_assertions_report_the_exact_counterexample_step() {
    let bad = json!({"name":"wrong-expected-state","requirements":["V01"],"steps":[{"event":{"kind":"open","store":"alice","path":"a.sqlite","binding":"User:alice","context":"s1"},"expect":{"stores":{"alice":{"C":99}}}}]});
    let error = scenarios(&json!([bad]).to_string()).unwrap()[0]
        .verify()
        .unwrap_err();
    assert!(error.contains("wrong-expected-state step 0"), "{error}");
}

#[test]
fn an_oracle_event_failure_rolls_back_the_whole_unit() {
    let mut oracle = Oracle::default();
    oracle.apply(&json!({"kind":"open","store":"alice","path":"a.sqlite","binding":"User:alice","context":"s1"})).unwrap();
    let before = oracle.snapshot();
    let event = json!({"kind":"transaction","store":"alice","events":[{"kind":"direct","store":"alice","key":"Entry:x","state":{"text":"unsaved"}},{"kind":"fail","store":"alice"}]});
    assert!(oracle.apply(&event).is_err());
    assert_eq!(oracle.snapshot(), before);
}

fn opened() -> Oracle {
    let mut oracle = Oracle::default();
    oracle.apply(&json!({"kind":"open","store":"alice","path":"a.sqlite","binding":"User:alice","context":"s1"})).unwrap();
    oracle
}

#[test]
fn a_captured_bootstrap_tail_cannot_be_replaced() {
    let mut oracle = opened();
    oracle
        .apply(&json!({"kind":"bootstrap","store":"alice","id":"b","keys":[],"barrier":10}))
        .unwrap();
    oracle
        .apply(&json!({"kind":"tail","store":"alice","cursor":12}))
        .unwrap();
    let before = oracle.snapshot();
    assert!(
        oracle
            .apply(&json!({"kind":"tail","store":"alice","cursor":13}))
            .is_err()
    );
    assert_eq!(oracle.snapshot(), before);
    oracle
        .apply(&json!({"kind":"tail","store":"alice","cursor":12}))
        .unwrap();
}

#[test]
fn compacted_group_can_install_ahead_of_its_proven_prefix_but_not_head() {
    let mut oracle = opened();
    oracle.apply(&json!({"kind":"page","store":"alice","id":"p","from":0,"to":1,"head":3,"units":[{"through":1,"changes":[{"kind":"stream","key":"Entry:x","cursor":3,"state":{"text":"new"}}]}]})).unwrap();
    oracle
        .apply(&json!({"kind":"unit","store":"alice","id":"p","index":0}))
        .unwrap();
    let snapshot = oracle.snapshot();
    assert_eq!(snapshot["stores"]["alice"]["C"], 1);
    assert_eq!(
        snapshot["stores"]["alice"]["evidence"]["Entry:x"]["G"]["s1"],
        3
    );
    assert!(oracle.apply(&json!({"kind":"page","store":"alice","id":"bad","from":1,"to":2,"head":3,"units":[{"through":2,"changes":[{"kind":"stream","key":"Entry:y","cursor":4,"state":{}}]}]})).is_err());
}

#[test]
fn private_receipt_does_not_restore_fields_superseded_by_stream() {
    let mut oracle = opened();
    oracle.apply(&json!({"kind":"enqueue","store":"alice","id":"q","operations":[{"key":"Entry:x","state":{"text":"optimistic","note":"old"}}]})).unwrap();
    oracle.apply(&json!({"kind":"stream","store":"alice","key":"Entry:x","cursor":58,"state":{"text":"new","note":"stream"}})).unwrap();
    oracle
        .apply(&json!({"kind":"direct","store":"alice","key":"Entry:x","state":{"note":"later"}}))
        .unwrap();
    oracle.apply(&json!({"kind":"receipt","store":"alice","id":"q","context":"s1","targets":[{"key":"Entry:x","cursor":null,"state":{"text":"accepted","note":"old"}}],"result":true})).unwrap();
    oracle
        .apply(&json!({"kind":"settle","store":"alice","id":"q"}))
        .unwrap();
    assert_eq!(
        oracle.snapshot()["stores"]["alice"]["rows"]["Entry:x"],
        json!({"text":"new","note":"later"})
    );
}

#[test]
fn accepting_a_companion_keeps_its_original_position_before_later_authority() {
    let mut oracle = opened();
    oracle.apply(&json!({"kind":"enqueue","store":"alice","id":"q","operations":[{"key":"Draft:x","state":{"text":"companion"}}]})).unwrap();
    oracle.apply(&json!({"kind":"stream","store":"alice","key":"Draft:x","cursor":58,"state":{"text":"new"}})).unwrap();
    oracle.apply(&json!({"kind":"receipt","store":"alice","id":"q","context":"s1","targets":[],"result":true})).unwrap();
    oracle
        .apply(&json!({"kind":"settle","store":"alice","id":"q"}))
        .unwrap();
    let store = &oracle.snapshot()["stores"]["alice"];
    assert_eq!(store["rows"]["Draft:x"], json!({"text":"new"}));
    assert_eq!(store["evidence"]["Draft:x"]["cursor"], 58);
}

#[test]
fn current_materialization_authority_can_complete_a_retained_receipt() {
    let mut oracle = opened();
    oracle.apply(&json!({"kind":"enqueue","store":"alice","id":"q","operations":[{"key":"Entry:x","state":{"text":"optimistic"}}]})).unwrap();
    oracle.apply(&json!({"kind":"receipt","store":"alice","id":"q","context":"s1","targets":[{"key":"Entry:x","cursor":58,"state":{"text":"oldshape"}}],"result":true})).unwrap();
    oracle
        .apply(&json!({"kind":"context","store":"alice","context":"s2"}))
        .unwrap();
    oracle.apply(&json!({"kind":"stream","store":"alice","key":"Entry:x","cursor":58,"state":{"text":"newshape","label":"newfield"}})).unwrap();
    oracle
        .apply(&json!({"kind":"settle","store":"alice","id":"q"}))
        .unwrap();
    let store = &oracle.snapshot()["stores"]["alice"];
    assert_eq!(store["calls"]["q"]["status"], "completed");
    assert_eq!(
        store["rows"]["Entry:x"],
        json!({"text":"newshape","label":"newfield"})
    );
}
