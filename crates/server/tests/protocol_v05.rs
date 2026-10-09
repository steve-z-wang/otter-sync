use axton_protocols::sync::{
    self as v05, Mutation, MutationOperation, MutationRequest, Operation, RequestContext,
};
use axton_server::{Config, validate_mutation_batch};
use serde_json::json;
fn config() -> Config {
    Config::decode(json!({"schema":{"enums":[],"models":[],"actions":[{"name":"Say","version":1,"kind":"mutation","inputs":[{"kind":"value","name":"text","type":{"kind":"scalar","name":"string"},"nullable":false,"list":false}],"outputs":[]}]},"mutations":[],"loaders":[]})).unwrap()
}
fn request() -> MutationRequest {
    let c = config();
    let mut r = MutationRequest {
        context: RequestContext {
            protocol: 5,
            store_id: "s".into(),
            stream: "User:a".into(),
            materialization: "m".into(),
        },
        batch_id: 1,
        digest: "".into(),
        mutations: vec![Mutation {
            id: 1,
            name: "Say".into(),
            version: 1,
            descriptor: v05::mutation_descriptor_digest(&c.schema.actions[0]).unwrap(),
            operations: vec![MutationOperation {
                step: 1,
                input_path: "text".into(),
                operation: Operation::Argument,
                model: None,
                identity: json!(null),
                value: json!("hello"),
            }],
        }],
    };
    r.digest = v05::batch_digest(&r).unwrap();
    r
}
#[test]
fn fingerprint_is_immutable_intent_not_current_schema_authority() {
    let mut r = request();
    r.mutations[0].descriptor = "foreign".into();
    r.digest = v05::batch_digest(&r).unwrap();
    assert!(validate_mutation_batch(&config(), &v05::encode(&r).unwrap()).is_ok());
}
#[test]
fn rejects_noncanonical_input_before_execution() {
    let mut r = request();
    r.mutations[0].operations[0].value = json!(42);
    r.digest = v05::batch_digest(&r).unwrap();
    assert!(validate_mutation_batch(&config(), &v05::encode(&r).unwrap()).is_err());
}

#[test]
fn compatible_field_reorder_admits_frozen_old_artifact() {
    let model = json!({"name":"Todo","version":1,"identity":["id"],"fields":[{"name":"id","nullable":false,"type":{"kind":"scalar","name":"string"}},{"name":"title","nullable":false,"type":{"kind":"scalar","name":"string"}}]});
    let action = json!({"name":"Write","version":1,"kind":"mutation","inputs":[{"kind":"model","name":"todo","model":"Todo","operation":"create","cardinality":"single"}],"outputs":[],"input":{"models":[model.clone()],"enums":[]}});
    let old=Config::decode(json!({"schema":{"enums":[],"models":[model],"actions":[action]},"mutations":[],"loaders":["Todo"]})).unwrap();
    let mut new = old.clone();
    new.schema.models[0].fields.reverse();
    new.schema.actions[0].input.as_mut().unwrap().models[0]
        .fields
        .reverse();
    assert_eq!(
        v05::materialization_id(&old.schema, "1").unwrap(),
        v05::materialization_id(&new.schema, "1").unwrap()
    );
    assert_ne!(
        v05::mutation_descriptor_digest(&old.schema.actions[0]).unwrap(),
        v05::mutation_descriptor_digest(&new.schema.actions[0]).unwrap()
    );
    let mut r = MutationRequest {
        context: RequestContext {
            protocol: 5,
            store_id: "s".into(),
            stream: "User:a".into(),
            materialization: v05::materialization_id(&old.schema, "1").unwrap(),
        },
        batch_id: 1,
        digest: String::new(),
        mutations: vec![Mutation {
            id: 1,
            name: "Write".into(),
            version: 1,
            descriptor: v05::mutation_descriptor_digest(&old.schema.actions[0]).unwrap(),
            operations: vec![MutationOperation {
                step: 1,
                input_path: "todo".into(),
                operation: Operation::Create,
                model: Some("Todo".into()),
                identity: json!({"id":"x"}),
                value: json!({"title":"hello"}),
            }],
        }],
    };
    r.digest = v05::batch_digest(&r).unwrap();
    assert!(validate_mutation_batch(&old, &v05::encode(&r).unwrap()).is_ok());
    assert!(validate_mutation_batch(&new, &v05::encode(&r).unwrap()).is_ok());
    let note = axton_core::FieldDescriptor {
        name: "note".into(),
        nullable: true,
        value_type: axton_core::ValueType::Scalar {
            name: axton_core::ScalarType::String,
        },
        default: None,
        create_default: None,
    };
    new.schema.models[0].fields.push(note.clone());
    new.schema.actions[0].input.as_mut().unwrap().models[0]
        .fields
        .push(note);
    assert!(
        validate_mutation_batch(&new, &v05::encode(&r).unwrap()).is_ok(),
        "trusted nullable input widening accepts frozen old payload"
    );
}
