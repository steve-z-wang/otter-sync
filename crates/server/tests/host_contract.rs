use axton_server::Config;
use serde_json::json;
#[test]
fn old_server_configuration_is_refused() {
    let c = json!({"schema":{"enums":[],"models":[],"actions":[]},"loaders":[],"protocol4":{}});
    assert_eq!(Config::decode(c).err().unwrap().code, "config.invalid");
}
