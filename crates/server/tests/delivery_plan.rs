use axton_protocols::sync::{self as v05, DeltaRequest, RequestContext};
use axton_server::{Config, Host, HostResult};
use serde_json::{Value, json};
use std::{future::Future, pin::Pin, sync::Mutex};
struct TestHost {
    calls: Mutex<Vec<Value>>,
}
impl Host for TestHost {
    fn call(&self, r: Value) -> Pin<Box<dyn Future<Output = HostResult<Value>> + Send + '_>> {
        Box::pin(async move {
            self.calls.lock().unwrap().push(r.clone());
            let q = &r["request"];
            Ok(match (r["op"].as_str(), q["op"].as_str()) {
                (Some("publicationFence"), _) => Value::Null,
                (_, Some("claimStore")) => json!({"principal":"alice","stream":"User:alice"}),
                (_, Some("admit")) => json!(true),
                (_, Some("bootstrapState")) => json!(true),
                (_, Some("deliveryHead")) => json!(45),
                (_, Some("deliveryNow")) => json!(1000),
                (_, Some("deliveryCandidates")) => {
                    json!([{ "model":"Entry","identityKey":"{\"id\":\"e\"}","cursor":45,"kind":"upsert"}])
                }
                (Some("load"), _) if r["mode"] == "prepare" => json!([]),
                (Some("load"), _) => json!([{"text":"at45"}]),
                (_, Some("saveDelivery")) => json!(true),
                _ => return Err(format!("unexpected {r}")),
            })
        })
    }
}
fn config() -> Config {
    Config::decode(json!({"schema":{"enums":[],"models":[{"name":"Entry","version":1,"bootstrap":true,"identity":["id"],"fields":[{"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"}},{"name":"text","nullable":false,"type":{"kind":"scalar","name":"string"}}]}],"actions":[]},"mutations":[],"loaders":["Entry"],"protocol5":{"projectionGeneration":"1"}})).unwrap()
}
#[test]
fn bootstrap_freezes_moved_ahead_authority_without_claiming_its_position() {
    run(async {
        let c = config();
        let h = TestHost {
            calls: Mutex::new(vec![]),
        };
        let r = DeltaRequest {
            context: RequestContext {
                protocol: 5,
                store_id: "s".into(),
                stream: "User:alice".into(),
                materialization: v05::materialization_id(&c.schema, "1").unwrap(),
            },
            after: 0,
            through: 40,
            bootstrap: true,
            continuation: None,
        };
        let answer = axton_server::process_delivery05(&c, "alice", &v05::encode(&r).unwrap(), &h)
            .await
            .unwrap();
        let d: v05::DeliveryResponse = v05::decode(answer.as_bytes()).unwrap();
        assert_eq!(d.header.observed_head, 45);
        assert_eq!(d.header.units.last().unwrap().through, Some(40));
        assert_eq!(d.parts[0].changes[0].cursor(), 45);
        assert_eq!(
            h.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|v| v["op"] == "load" && v["mode"] == "canonical")
                .count(),
            1
        );
    });
}
fn run(f: impl Future<Output = ()>) {
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    let mut f = std::pin::pin!(f);
    assert!(f.as_mut().poll(&mut cx).is_ready());
}
#[test]
fn live_coverage_waits_for_every_frozen_fragment() {
    let c = config();
    let context = RequestContext {
        protocol: 5,
        store_id: "s".into(),
        stream: "User:alice".into(),
        materialization: v05::materialization_id(&c.schema, "1").unwrap(),
    };
    let frozen = v05::freeze_delivery(
        context.clone(),
        "p".into(),
        v05::DeliveryPurpose::Sync,
        40,
        45,
        45,
        10000,
        vec![v05::DeliveryUnit {
            index: 0,
            through: Some(45),
            changes: (0..3)
                .map(|i| v05::AuthorityChange::Record {
                    key: v05::RecordKey {
                        model: "Entry".into(),
                        identity: json!({"id":i.to_string()}),
                    },
                    cursor: 45,
                    state: json!({"text":"x"}),
                })
                .collect(),
        }],
        1,
    )
    .unwrap();
    let (mut live, _) =
        axton_server::live::Subscriptions::open05(context.clone(), 40, "ack".into());
    for (i, part) in frozen.parts.iter().enumerate() {
        let d = v05::DeliveryResponse {
            header: frozen.header.clone(),
            parts: vec![part.clone()],
        };
        let actions = live
            .handle(axton_server::live::LiveEvent::Pulled {
                page: String::from_utf8(v05::encode(&d).unwrap()).unwrap(),
            })
            .unwrap();
        assert_eq!(live.streams()[0].cursor, if i == 2 { 45 } else { 40 });
        assert!(!actions.is_empty());
    }
}
#[test]
fn preparation_precedes_observed_head_capture() {
    run(async {
        let c = config();
        let h = TestHost {
            calls: Mutex::new(vec![]),
        };
        let r = DeltaRequest {
            context: RequestContext {
                protocol: 5,
                store_id: "s".into(),
                stream: "User:alice".into(),
                materialization: v05::materialization_id(&c.schema, "1").unwrap(),
            },
            after: 0,
            through: 40,
            bootstrap: true,
            continuation: None,
        };
        axton_server::process_delivery05(&c, "alice", &v05::encode(&r).unwrap(), &h)
            .await
            .unwrap();
        let calls = h.calls.lock().unwrap();
        let prepare = calls
            .iter()
            .position(|r| r["op"] == "load" && r["mode"] == "prepare")
            .expect("missing preparation");
        let head = calls
            .iter()
            .rposition(|r| r["request"]["op"] == "deliveryHead")
            .unwrap();
        assert!(prepare < head);
    });
}

#[test]
fn live_advances_each_complete_unit_while_retaining_the_plan_start() {
    let c = config();
    let context = RequestContext {
        protocol: 5,
        store_id: "s".into(),
        stream: "User:alice".into(),
        materialization: v05::materialization_id(&c.schema, "1").unwrap(),
    };
    let units = (0..2)
        .map(|i| v05::DeliveryUnit {
            index: i,
            through: Some(41 + i * 4),
            changes: vec![v05::AuthorityChange::Record {
                key: v05::RecordKey {
                    model: "Entry".into(),
                    identity: json!({"id":i.to_string()}),
                },
                cursor: 41 + i * 4,
                state: json!({"text":"x"}),
            }],
        })
        .collect();
    let frozen = v05::freeze_delivery(
        context.clone(),
        "p".into(),
        v05::DeliveryPurpose::Sync,
        40,
        45,
        45,
        10000,
        units,
        500,
    )
    .unwrap();
    let (mut live, _) = axton_server::live::Subscriptions::open05(context, 40, "ack".into());
    for (i, part) in frozen.parts.iter().enumerate() {
        let d = v05::DeliveryResponse {
            header: frozen.header.clone(),
            parts: vec![part.clone()],
        };
        live.handle(axton_server::live::LiveEvent::Pulled {
            page: String::from_utf8(v05::encode(&d).unwrap()).unwrap(),
        })
        .unwrap();
        assert_eq!(live.streams()[0].cursor, if i == 0 { 41 } else { 45 });
    }
}
