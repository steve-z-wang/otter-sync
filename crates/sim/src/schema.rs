//! The one schema every simulation uses: an Entry with Comment children. Two models
//! and one cascading relation reach every distribution scenario; more would add
//! time, not information.
use axton_client::{Mutation, Operation, OperationKind};
use axton_core::{RecordKey, Schema};
use axton_server::Config;
use serde_json::{Value, json};

pub fn schema() -> Schema {
    Schema::from_value(json!({"enums":[],"models":[
        {"name":"Entry","identity":["id"],"fields":[
            {"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"}},
            {"name":"text","nullable":false,"type":{"kind":"scalar","name":"string"}},
            {"name":"note","nullable":true,"type":{"kind":"scalar","name":"string"}}]},
        {"name":"Comment","identity":["id"],"fields":[
            {"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"}},
            {"name":"entryId","nullable":false,"type":{"kind":"scalar","name":"string"}},
            {"name":"text","nullable":false,"type":{"kind":"scalar","name":"string"}}],
         "relations":[{"name":"entry","target":"Entry","fields":["entryId"],"targetFields":["id"],"onDelete":"delete"}]}
    ]}))
    .unwrap()
}

fn slot(name: &str, model: &str, operation: &str, patch: &[&str]) -> Value {
    json!({"name":name,"model":model,"operation":operation,"cardinality":"single","allowedPatchFields":patch})
}

pub fn config() -> Config {
    Config::decode(json!({
        "schema": schema(),
        "loaders": ["Entry", "Comment"],
        "mutations": [
            {"name":"CreateEntry","version":1,"slots":[slot("entry","Entry","create",&[])]},
            {"name":"Edit","version":1,"slots":[slot("entry","Entry","update",&["text"])]},
            {"name":"DeleteEntry","version":1,"slots":[slot("entry","Entry","delete",&[])]},
            {"name":"CreateComment","version":1,"slots":[slot("comment","Comment","create",&[])]},
            {"name":"EditComment","version":1,"slots":[slot("comment","Comment","update",&["text"])]},
            {"name":"DeleteComment","version":1,"slots":[slot("comment","Comment","delete",&[])]}
        ]
    }))
    .unwrap()
}
/// The read contracts a simulated client declares: every model at its schema version.
pub fn declared_models() -> std::collections::BTreeMap<String, u64> {
    schema()
        .models
        .iter()
        .map(|m| (m.name.clone(), m.version))
        .collect()
}
pub fn entry_key(id: &str) -> RecordKey {
    schema().record_key("Entry", &json!({ "id": id })).unwrap()
}
pub fn comment_key(id: &str) -> RecordKey {
    schema()
        .record_key("Comment", &json!({ "id": id }))
        .unwrap()
}
/// The key behind its canonical encoding (`["Model",{"id":…}]`).
pub fn key_from_encoded(encoded: &str) -> RecordKey {
    let parts: Vec<Value> = serde_json::from_str(encoded).expect("encoded key");
    schema()
        .record_key(parts[0].as_str().expect("model"), &parts[1])
        .expect("known key")
}
fn op(model: &str, kind: OperationKind, id: &str, values: Option<Value>) -> Operation {
    Operation {
        model: model.into(),
        op: kind,
        identity: json!({ "id": id }),
        values,
    }
}
pub fn create_entry(id: &str, text: &str) -> Mutation {
    Mutation::new(
        "CreateEntry",
        vec![op(
            "Entry",
            OperationKind::Create,
            id,
            Some(json!({"text": text, "note": null})),
        )],
    )
}
pub fn edit(id: &str, text: &str) -> Mutation {
    Mutation::new(
        "Edit",
        vec![op(
            "Entry",
            OperationKind::Update,
            id,
            Some(json!({"text": text})),
        )],
    )
}
pub fn delete_entry(id: &str) -> Mutation {
    Mutation::new(
        "DeleteEntry",
        vec![op("Entry", OperationKind::Delete, id, None)],
    )
}
pub fn create_comment(id: &str, entry_id: &str, text: &str) -> Mutation {
    Mutation::new(
        "CreateComment",
        vec![op(
            "Comment",
            OperationKind::Create,
            id,
            Some(json!({"entryId": entry_id, "text": text})),
        )],
    )
}
pub fn edit_comment(id: &str, text: &str) -> Mutation {
    Mutation::new(
        "EditComment",
        vec![op(
            "Comment",
            OperationKind::Update,
            id,
            Some(json!({"text": text})),
        )],
    )
}
pub fn delete_comment(id: &str) -> Mutation {
    Mutation::new(
        "DeleteComment",
        vec![op("Comment", OperationKind::Delete, id, None)],
    )
}

/// A native Load contract used by enrolled-response delivery traces.
pub fn enrollment_schema() -> Schema {
    let mut value = serde_json::to_value(schema()).unwrap();
    value["resultModels"] = json!([{"name":"Entry","version":1,"identity":["id"],"fields":value["models"][0]["fields"],"enums":[]}]);
    value["loads"] = json!([{"name":"EnrolledEntries","version":1,
      "inputs":[{"kind":"value","name":"channel","type":{"kind":"scalar","name":"string"},"nullable":false,"list":false,"required":true,"cardinality":"single"}],
      "outputs":[{"name":"entries","kind":"model","cardinality":"list","source":"handlerIdentity","model":"Entry","modelReadVersion":1,
        "handlerType":{"kind":"identity","model":"Entry","fields":[{"name":"id","type":{"kind":"scalar","name":"string"}}]}}],
      "input":{"models":[],"enums":[]},"outputEnums":[]}]);
    Schema::from_value(value).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_and_config_validate_and_keys_encode() {
        let s = schema();
        assert!(s.model("Entry").is_ok());
        assert!(s.model("Comment").is_ok());
        let c = config();
        assert_eq!(c.loaders, vec!["Entry".to_string(), "Comment".to_string()]);
        assert_eq!(c.mutations.len(), 6);
        assert_eq!(
            entry_key("e1").encoded().unwrap(),
            "[\"Entry\",{\"id\":\"e1\"}]"
        );
        assert_eq!(comment_key("c1").model, "Comment");
        assert_eq!(edit("e1", "x").operations.len(), 1);
    }
}
