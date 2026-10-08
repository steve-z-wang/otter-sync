use axton_core::*;
use serde_json::{Value, json};
type DescriptorEdit = (&'static str, fn(&mut Value), &'static str);
fn id(n: u64) -> String {
    format!("01890f47-1234-7123-8123-{n:012x}")
}

fn model(name: &str, extra: Value) -> Value {
    let mut fields = vec![
        json!({"name":"id","type":{"kind":"scalar","name":"uuid"},"nullable":false}),
        json!({"name":"title","type":{"kind":"scalar","name":"string"},"nullable":false}),
    ];
    if let Value::Array(more) = extra {
        fields.extend(more);
    }
    json!({"name":name,"version":1,"identity":["id"],"fields":fields})
}

fn output(name: &str, model: &str) -> Value {
    json!({
        "name":name,"kind":"model","cardinality":"list","source":"handlerIdentity",
        "model":model,"modelReadVersion":1,
        "handlerType":{"kind":"identity","model":model,"fields":[
            {"name":"id","type":{"kind":"scalar","name":"uuid"}}
        ]}
    })
}

fn load_descriptor() -> Value {
    json!({
        "name":"ProjectTodos","version":1,
        "inputs":[
            {"kind":"value","name":"projectId","type":{"kind":"scalar","name":"uuid"},"nullable":false,"list":false,"required":true,"cardinality":"single"},
            {"kind":"value","name":"status","type":{"kind":"enum","name":"Status"},"nullable":true,"list":false,"required":true,"cardinality":"single"},
            {"kind":"value","name":"tags","type":{"kind":"scalar","name":"string"},"nullable":false,"list":true,"required":true,"cardinality":"list"}
        ],
        "outputs":[output("todos","Todo"), output("notes","Note")],
        "input":{"models":[],"enums":[{"name":"Status","values":["open","done"]}]},
        "outputEnums":[]
    })
}

fn raw_schema() -> Value {
    let read = |name: &str| {
        let mut m = model(name, json!([]));
        m["enums"] = json!([]);
        m
    };
    json!({
        "enums":[{"name":"Status","values":["open","done"]}],
        "models":[model("Todo", json!([])), model("Note", json!([]))],
        "resultModels":[read("Todo"), read("Note")],
        "actions":[{"name":"Send","version":1,"kind":"mutation","inputs":[],"outputs":[]}],
        "loads":[load_descriptor()]
    })
}

fn schema() -> Schema {
    Schema::from_value(raw_schema()).unwrap()
}
fn fails<T: std::fmt::Debug, E: std::fmt::Display>(
    result: std::result::Result<T, E>,
    fragment: &str,
) {
    let error = result.unwrap_err().to_string();
    assert!(
        error.contains(fragment),
        "expected `{fragment}`, got `{error}`"
    );
}

#[test]
fn malformed_load_descriptors_are_refused() {
    let edit = |change: &dyn Fn(&mut Value)| {
        let mut raw = raw_schema();
        change(&mut raw["loads"][0]);
        Schema::from_value(raw)
    };
    assert!(edit(&|_| {}).is_ok());
    let cases: Vec<DescriptorEdit> = vec![
        (
            "kind member",
            |l| l["kind"] = json!("query"),
            "unknown field `kind`",
        ),
        (
            "sequence member",
            |l| l["sequence"] = json!(null),
            "unknown field `sequence`",
        ),
        (
            "once member",
            |l| l["once"] = json!(true),
            "unknown field `once`",
        ),
        (
            "version zero",
            |l| l["version"] = json!(0),
            "invalid or duplicate Load descriptor",
        ),
        (
            "empty name",
            |l| l["name"] = json!(""),
            "invalid or duplicate Load descriptor",
        ),
        (
            "no outputs",
            |l| l["outputs"] = json!([]),
            "declares no output",
        ),
        (
            "model operand",
            |l| {
                l["inputs"].as_array_mut().unwrap().push(
                    json!({"kind":"model","name":"todo","model":"Todo","operation":"create","cardinality":"single"}),
                )
            },
            "cannot take a Model operand",
        ),
        (
            "scalar output",
            |l| l["outputs"][0] = json!({"name":"count","kind":"value","type":{"kind":"scalar","name":"int"},"cardinality":"list","source":"handlerValue"}),
            "output count must be a list of Model identities",
        ),
        (
            "single output",
            |l| l["outputs"][0]["cardinality"] = json!("single"),
            "output todos must be a list of Model identities",
        ),
        (
            "optional output",
            |l| l["outputs"][0]["cardinality"] = json!("optional"),
            "output todos must be a list of Model identities",
        ),
        (
            "input identity output",
            |l| l["outputs"][0]["source"] = json!({"inputIdentity":"todo"}),
            "output todos must be a list of Model identities",
        ),
        (
            "unknown read contract",
            |l| l["outputs"][0]["modelReadVersion"] = json!(2),
            "unknown result Model Todo v2",
        ),
        (
            "missing read contract",
            |l| {
                l["outputs"][0]
                    .as_object_mut()
                    .unwrap()
                    .remove("modelReadVersion");
            },
            "missing Model read version",
        ),
        (
            "wrong handler type",
            |l| l["outputs"][0]["handlerType"]["model"] = json!("Note"),
            "invalid Action identity handler type",
        ),
        (
            "one Model at two read versions",
            |l| {
                let mut again = l["outputs"][0].clone();
                again["name"] = json!("older");
                again["modelReadVersion"] = json!(2);
                l["outputs"].as_array_mut().unwrap().push(again);
            },
            "reads Model Todo at two contract versions",
        ),
        (
            "duplicate output",
            |l| l["outputs"][1]["name"] = json!("todos"),
            "invalid Load output",
        ),
        (
            "duplicate input",
            |l| l["inputs"][1]["name"] = json!("projectId"),
            "invalid Load input name",
        ),
        (
            "nullable list input",
            |l| l["inputs"][2]["nullable"] = json!(true),
            "Load lists cannot be nullable",
        ),
        (
            "enum outside snapshot",
            |l| l["input"]["enums"] = json!([]),
            "unknown Action enum",
        ),
        (
            "snapshot operand model",
            |l| l["input"]["models"] = json!([model("Todo", json!([]))]),
            "cannot retain Model operands",
        ),
        (
            "reserved get",
            |l| l["name"] = json!("Get"),
            "Load name Get is reserved",
        ),
        (
            "reserved list",
            |l| l["name"] = json!("list"),
            "Load name list is reserved",
        ),
        (
            "reserved invalidate",
            |l| l["name"] = json!("Invalidate"),
            "Load name Invalidate is reserved",
        ),
        (
            "action name",
            |l| l["name"] = json!("Send"),
            "shares the operation name of another operation",
        ),
        (
            "normalized action name",
            |l| l["name"] = json!("send"),
            "shares the operation name of another operation",
        ),
    ];
    for (label, change, fragment) in cases {
        let error = edit(&change).unwrap_err().to_string();
        assert!(error.contains(fragment), "{label}: {error}");
    }
    let mut duplicate = raw_schema();
    duplicate["loads"]
        .as_array_mut()
        .unwrap()
        .push(load_descriptor());
    fails(
        Schema::from_value(duplicate),
        "invalid or duplicate Load descriptor",
    );
    let mut spelled = raw_schema();
    let mut other = load_descriptor();
    other["name"] = json!("projectTodos");
    spelled["loads"].as_array_mut().unwrap().push(other);
    fails(
        Schema::from_value(spelled),
        "shares the operation name of another operation",
    );
    let mut versions = raw_schema();
    let mut v2 = load_descriptor();
    v2["version"] = json!(2);
    versions["loads"].as_array_mut().unwrap().push(v2);
    assert!(
        Schema::from_value(versions)
            .unwrap()
            .load("ProjectTodos", 2)
            .is_ok()
    );
}

#[test]
fn schemas_without_loads_serialize_unchanged() {
    let mut raw = raw_schema();
    raw.as_object_mut().unwrap().remove("loads");
    let without = Schema::from_value(raw).unwrap();
    assert!(without.loads.is_empty());
    assert!(
        serde_json::to_value(&without)
            .unwrap()
            .get("loads")
            .is_none()
    );
    let with = serde_json::to_value(schema()).unwrap();
    assert_eq!(with["loads"][0]["name"], "ProjectTodos");
}
