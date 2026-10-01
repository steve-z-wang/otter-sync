mod capability;
mod support;
use axton_server::{Config, Error};
use serde_json::json;
use support::{Backend, run};
fn config() -> Config {
    Config::decode(json!({"schema":{"enums":[],"models":[{"name":"Entry","identity":["id"],"fields":[{"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"}}]}]},"loaders":["Entry"],"mutations":[]})).unwrap()
}
#[test]
fn every_external_ingress_refuses_missing_capability_before_host_effects() {
    let config = config();
    let host = Backend::new();
    for bytes in [
        b"{}".as_slice(),
        b"{\"capabilities\":[\"scope-membership-v1\"]}".as_slice(),
    ] {
        let errors: Vec<Error> = vec![
            run(axton_server::process_push(&config, "alice", bytes, &host)).unwrap_err(),
            run(axton_server::process_action_push(
                &config, "alice", bytes, &host,
            ))
            .unwrap_err(),
            run(axton_server::process_action(&config, "alice", bytes, &host)).unwrap_err(),
            run(axton_server::process_fetch(&config, "alice", bytes, &host)).unwrap_err(),
            run(axton_server::process_load(&config, "alice", bytes, &host)).unwrap_err(),
            run(axton_server::process_pull(&config, "alice", bytes, &host)).unwrap_err(),
            run(axton_server::process_stream_pull(
                &config, "alice", bytes, &host,
            ))
            .unwrap_err(),
            run(axton_server::live::negotiate(
                &config, "alice", bytes, &host,
            ))
            .unwrap_err(),
            axton_server::validate_load_batch(bytes).unwrap_err(),
        ];
        for error in errors {
            assert_eq!(error.code, "protocol.unsupported");
        }
        assert!(host.0.lock().unwrap().log.is_empty());
    }
}
#[test]
fn malformed_negotiation_is_request_invalid_before_host_effects() {
    let config = config();
    let host = Backend::new();
    for bytes in [b"{\"capabilities\":true}".as_slice(), b"[]", b"{"] {
        assert_eq!(
            run(axton_server::process_stream_pull(
                &config, "alice", bytes, &host
            ))
            .unwrap_err()
            .code,
            "request.invalid"
        );
        assert_eq!(
            run(axton_server::live::negotiate(
                &config, "alice", bytes, &host
            ))
            .unwrap_err()
            .code,
            "request.invalid"
        );
        assert_eq!(
            axton_server::validate_load_batch(bytes).unwrap_err().code,
            "request.invalid"
        );
    }
    assert!(host.0.lock().unwrap().log.is_empty());
}

#[test]
fn public_live_progress_refuses_record_only_new_pages() {
    let legacy = json!({"cursors":{"room":{"from":0,"to":1,"head":1}},"changes":[{"model":"Entry","identity":{"id":"e"},"stamp":1,"state":null}]}).to_string();
    let expected = std::collections::BTreeMap::from([("room".to_string(), 0)]);
    assert_eq!(
        axton_server::live::page_progress(&legacy, &expected)
            .unwrap_err()
            .code,
        "live.invalid_page"
    );
}
