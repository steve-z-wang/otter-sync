//! Guarantees R1 and R3 on the simulation. R2 is tests/invariants.rs.
use axton_sim::{Action, MutationSpec, Sim, schema::entry_key};

/// R1: with every message dropped, local writes keep working; once delivery resumes
/// the client converges with the server.
#[test]
fn r1_writes_continue_while_unreachable_and_converge_after() {
    let mut sim = Sim::new(51, 1);
    sim.apply(Action::Subscribe {
        client: 0,
        stream: "a".into(),
    })
    .unwrap();
    for i in 0..5 {
        sim.apply(Action::Enqueue {
            client: 0,
            mutation: if i == 0 {
                MutationSpec::CreateEntry {
                    id: "e1".into(),
                    text: "0".into(),
                }
            } else {
                MutationSpec::Edit {
                    id: "e1".into(),
                    text: i.to_string(),
                }
            },
        })
        .unwrap();
        sim.apply(Action::Freeze { client: 0 }).unwrap();
        sim.apply(Action::Drop).unwrap(); // the server never hears it
    }
    assert_eq!(sim.read_text(0, &entry_key("e1")).as_deref(), Some("4"));
    assert_eq!(sim.host.handler_calls(), 0);
    sim.settle();
    assert_eq!(sim.host.state(&entry_key("e1")).unwrap()["text"], "4");
    assert_eq!(sim.client(0).pending_count().unwrap(), 0);
    sim.check().unwrap();
}

/// R3: crash after every single step of a full round trip; nothing is lost and the
/// run still converges.
fn script() -> Vec<Action> {
    vec![
        Action::Subscribe {
            client: 0,
            stream: "a".into(),
        },
        Action::Enqueue {
            client: 0,
            mutation: MutationSpec::CreateEntry {
                id: "e1".into(),
                text: "v".into(),
            },
        },
        Action::Freeze { client: 0 },
        Action::Deliver,
        Action::Deliver,
        Action::Pull { client: 0 },
        Action::Deliver,
        Action::Deliver,
        Action::Enqueue {
            client: 0,
            mutation: MutationSpec::Edit {
                id: "e1".into(),
                text: "w".into(),
            },
        },
        Action::Freeze { client: 0 },
        Action::RejectNext {
            code: "entry.denied".into(),
        },
        Action::Deliver,
        Action::Deliver,
    ]
}

#[test]
fn r3_crash_after_every_step_loses_nothing() {
    let mut reference = Sim::new(52, 1);
    for a in script() {
        reference.apply(a).unwrap();
    }
    reference.settle();
    let expected = reference.read_text(0, &entry_key("e1"));
    let expected_rejections = reference.client(0).rejections().unwrap().len();
    for crash_at in 0..script().len() {
        let mut sim = Sim::new(52, 1);
        for (i, a) in script().into_iter().enumerate() {
            sim.apply(a).unwrap();
            sim.check()
                .unwrap_or_else(|e| panic!("crash_at {crash_at} step {i}: {e}"));
            if i == crash_at {
                sim.apply(Action::Crash { client: 0 }).unwrap();
                sim.apply(Action::Restart { client: 0 }).unwrap();
                sim.check()
                    .unwrap_or_else(|e| panic!("after restart at {crash_at}: {e}"));
            }
        }
        sim.settle();
        assert_eq!(
            sim.read_text(0, &entry_key("e1")),
            expected,
            "crash_at {crash_at}"
        );
        assert_eq!(
            sim.client(0).rejections().unwrap().len(),
            expected_rejections,
            "crash_at {crash_at}"
        );
        sim.check().unwrap();
    }
}
