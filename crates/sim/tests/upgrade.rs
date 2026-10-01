//! An app upgrade reopens a client with a newer schema. Additive changes open in
//! place; an incompatible one gets a fresh file that converges like a new client,
//! after the old file's unsent work is sent from it or explicitly left behind.
use axton_client::{Operation, OperationKind};
use axton_core::Schema;
use axton_sim::{Action, MutationSpec, Sim, schema::entry_key};
use serde_json::json;

fn variant(edit: impl FnOnce(&mut serde_json::Value)) -> Schema {
    let mut value = serde_json::to_value(axton_sim::schema::schema()).unwrap();
    edit(&mut value);
    Schema::from_value(value).unwrap()
}
/// A new model is the additive case that leaves every existing row untouched.
fn additive() -> Schema {
    variant(|v| {
        v["models"].as_array_mut().unwrap().push(json!({
            "name":"Tag","identity":["id"],
            "fields":[{"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"}}]
        }))
    })
}
/// Retyping `note` is incompatible, while every page the server serves in this
/// simulation still applies to the rebuilt file (`note` is always null here), so
/// the scenario can watch it converge. A version bump would be refused by the
/// server as an unknown read contract.
fn breaking() -> Schema {
    variant(|v| v["models"][0]["fields"][2]["type"] = json!({"kind":"scalar","name":"int"}))
}
fn create(sim: &mut Sim, client: usize, id: &str, text: &str) {
    sim.apply(Action::Enqueue {
        client,
        mutation: MutationSpec::CreateEntry {
            id: id.into(),
            text: text.into(),
        },
    })
    .unwrap();
}
fn setup() -> Sim {
    let mut sim = Sim::new(7, 2);
    for client in 0..2 {
        sim.apply(Action::Subscribe {
            client,
            stream: "a".into(),
        })
        .unwrap();
    }
    create(&mut sim, 0, "e1", "one");
    sim.settle();
    assert_eq!(sim.read_text(1, &entry_key("e1")).as_deref(), Some("one"));
    sim.check().unwrap();
    sim
}

#[test]
fn an_additive_schema_opens_in_place_and_keeps_everything() {
    let mut sim = setup();
    let state = sim.upgrade(0, additive(), false);
    assert!(!state.rebuilt && state.pending.is_none());
    assert_eq!(sim.read_text(0, &entry_key("e1")).as_deref(), Some("one"));
    sim.check().unwrap();
}

#[test]
fn an_incompatible_schema_sends_unsent_work_from_the_old_file_then_rebuilds_and_converges() {
    let mut sim = setup();
    let old_file = sim.clients[0].path.clone();
    create(&mut sim, 0, "e2", "two");
    sim.apply(Action::Freeze { client: 0 }).unwrap();
    let state = sim.upgrade(0, breaking(), false);
    assert!(!state.rebuilt, "unsent work keeps the old file open");
    let pending = state.pending.expect("the old file reports its unsent work");
    assert_eq!((pending.pending, pending.direct), (1, 0));
    assert!(pending.reason.contains("note"), "{}", pending.reason);
    assert!(
        sim.rebuild(0, false).is_err(),
        "a rebuild waits for the unsent work"
    );
    sim.settle();
    assert_eq!(sim.read_text(1, &entry_key("e2")).as_deref(), Some("two"));
    assert_eq!(sim.client(0).pending_count().unwrap(), 0);

    let report = sim.rebuild(0, false).unwrap();
    assert_eq!((report.left_pending, report.left_direct), (0, 0));
    assert!(old_file.exists(), "the old file is kept");
    assert_ne!(report.new_file, old_file.to_string_lossy());
    assert_eq!(
        sim.read_text(0, &entry_key("e1")),
        None,
        "fresh file, no rows"
    );
    let carried = sim
        .client(0)
        .subscription_state("a")
        .unwrap()
        .expect("the Stream is carried over");
    assert_eq!(
        (carried.starting_cursor, carried.cursor),
        (None, None),
        "the replacement resets the delivery boundary, so nothing claims old progress"
    );
    assert!(sim.client(0).subscriptions().unwrap().is_empty());
    sim.check().unwrap();
    // Registering the Stream again commits a boundary for the new identity, and
    // the rebuilt file converges from it like a new client.
    sim.apply(Action::Subscribe {
        client: 0,
        stream: "a".into(),
    })
    .unwrap();
    assert_eq!(
        sim.client(0).subscriptions().unwrap(),
        vec![("a".to_string(), 0)]
    );
    sim.settle();
    assert_eq!(sim.read_text(0, &entry_key("e1")).as_deref(), Some("one"));
    assert_eq!(sim.read_text(0, &entry_key("e2")).as_deref(), Some("two"));
    sim.check().unwrap();

    // A restart lands on the rebuilt file, not the old one.
    sim.apply(Action::Crash { client: 0 }).unwrap();
    sim.apply(Action::Restart { client: 0 }).unwrap();
    assert_eq!(sim.read_text(0, &entry_key("e2")).as_deref(), Some("two"));
    assert!(!sim.client(0).schema_state().rebuilt);
    sim.check().unwrap();
}

#[test]
fn discarding_unsent_work_rebuilds_at_once_and_reports_what_the_old_file_keeps() {
    let mut sim = setup();
    create(&mut sim, 0, "e2", "two");
    // A record that exists only through a direct write: nothing will ever send it.
    sim.client(0)
        .transaction(|tx| {
            tx.direct(Operation {
                model: "Entry".into(),
                op: OperationKind::Create,
                identity: json!({"id": "e3"}),
                values: Some(json!({"id": "e3", "text": "local", "note": null})),
            })
        })
        .unwrap();
    let state = sim.upgrade(0, breaking(), true);
    assert!(state.rebuilt);
    let report = state.last_rebuild.expect("the report of the rebuild");
    assert_eq!((report.left_pending, report.left_direct), (1, 1));
    assert_eq!(sim.read_text(0, &entry_key("e2")), None);
    sim.apply(Action::Subscribe {
        client: 0,
        stream: "a".into(),
    })
    .unwrap();
    sim.settle();
    assert_eq!(sim.read_text(0, &entry_key("e1")).as_deref(), Some("one"));
    assert_eq!(
        sim.read_text(1, &entry_key("e2")),
        None,
        "left work never reaches the server"
    );
    sim.check().unwrap();
}
