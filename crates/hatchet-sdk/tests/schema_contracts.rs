#[path = "support/grpc.rs"]
mod grpc;
use another_hatchet_sdk::*;
use grpc::*;
use prost::Message;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

fn rules(schema: Value) -> TaskConfig {
    TaskConfig::default().with_input_schema_value(schema)
}
#[test]
fn schema_constraints_composition_refs_formats_and_boolean_schemas_are_enforced() {
    let schema = json!({"$schema":"https://json-schema.org/draft/2020-12/schema","$defs":{"positive":{"type":"integer","minimum":1,"maximum":10}},"type":"object","required":["count","name","tags","email"],"additionalProperties":false,"properties":{"count":{"$ref":"#/$defs/positive"},"name":{"type":"string","minLength":3,"maxLength":8,"pattern":"^[a-z]+$"},"tags":{"type":"array","minItems":1,"maxItems":2,"uniqueItems":true,"items":{"enum":["a","b"]}},"email":{"type":"string","format":"email"}}});
    let cfg = rules(schema);
    cfg.validate_input_schema().unwrap();
    let valid = json!({"count":2,"name":"alice","tags":["a"],"email":"a@example.com"});
    assert!(cfg.validate_input_value(&valid).is_empty());
    for (field, value) in [
        ("count", json!(0)),
        ("count", json!(11)),
        ("name", json!("AB")),
        ("name", json!("toolongname")),
        ("tags", json!([])),
        ("tags", json!(["a", "a"])),
        ("tags", json!(["a", "b", "a"])),
        ("email", json!("private-invalid-secret")),
        ("extra", json!(true)),
    ] {
        let mut bad = valid.clone();
        bad[field] = value;
        let errors = cfg.validate_input_value(&bad);
        assert!(!errors.is_empty(), "constraint not enforced: {field}");
        assert!(errors.iter().all(|e| !e.contains("private-invalid-secret")));
    }
    for (schema, good, bad) in [
        (
            json!({"allOf":[{"type":"integer","minimum":1},{"multipleOf":2}]}),
            json!(2),
            json!(3),
        ),
        (
            json!({"anyOf":[{"const":"yes"},{"type":"integer"}]}),
            json!(3),
            json!(false),
        ),
        (
            json!({"oneOf":[{"type":"integer"},{"type":"number"}]}),
            json!(1.5),
            json!(1),
        ),
        (
            json!({"not":{"const":"blocked"}}),
            json!("ok"),
            json!("blocked"),
        ),
        (json!({"type":["null","string"]}), json!(null), json!(3)),
        (
            json!({"if":{"type":"integer"},"then":{"minimum":2},"else":{"const":"ok"}}),
            json!("ok"),
            json!(1),
        ),
        (
            json!({"type":"array","prefixItems":[{"type":"integer"},{"type":"string"}],"items":false}),
            json!([1, "x"]),
            json!([1, "x", 3]),
        ),
        (
            json!({"type":"object","dependentRequired":{"a":["b"]}}),
            json!({"a":1,"b":2}),
            json!({"a":1}),
        ),
    ] {
        let cfg = rules(schema);
        cfg.validate_input_schema().unwrap();
        assert!(cfg.validate_input_value(&good).is_empty());
        assert!(!cfg.validate_input_value(&bad).is_empty());
    }
    assert!(
        rules(json!(true))
            .validate_input_value(&json!(null))
            .is_empty()
    );
    assert!(
        !rules(json!(false))
            .validate_input_value(&json!({}))
            .is_empty()
    );
    let recursive = rules(
        json!({"$defs":{"node":{"type":"object","properties":{"value":{"type":"integer"},"next":{"anyOf":[{"type":"null"},{"$ref":"#/$defs/node"}]}},"required":["value"]}},"$ref":"#/$defs/node"}),
    );
    assert!(
        recursive
            .validate_input_value(&json!({"value":1,"next":{"value":2,"next":null}}))
            .is_empty()
    );
    assert!(
        !recursive
            .validate_input_value(&json!({"value":1,"next":{"value":"bad"}}))
            .is_empty()
    );
}
#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
#[schemars(crate = "another_hatchet_sdk::schemars")]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Profile {
    user_id: u32,
    choice: Choice,
    #[serde(default)]
    note: Option<String>,
}
#[derive(Debug, Serialize, Deserialize, schemars::JsonSchema)]
#[schemars(crate = "another_hatchet_sdk::schemars")]
#[serde(untagged)]
enum Choice {
    Name(String),
    Number(i32),
}
#[test]
fn generated_schemas_follow_serde_names_defaults_enums_and_local_definitions() {
    let config = TaskConfig::builder().input_schema_for::<Profile>().build();
    let schema = config.input_schema.as_ref().unwrap();
    assert_eq!(
        schema["$schema"],
        "https://json-schema.org/draft/2020-12/schema"
    );
    assert!(schema["properties"].get("userId").is_some());
    assert!(schema.get("$defs").is_some());
    config.validate_input_schema().unwrap();
    assert!(
        config
            .validate_input_value(&json!({"userId":1,"choice":"name"}))
            .is_empty()
    );
    assert!(
        config
            .validate_input_value(&json!({"userId":1,"choice":2,"note":null}))
            .is_empty()
    );
    for invalid in [
        json!({"user_id":1,"choice":"name"}),
        json!({"userId":1,"choice":true}),
        json!({"userId":1,"choice":"name","extra":true}),
    ] {
        assert!(!config.validate_input_value(&invalid).is_empty());
    }
}
#[tokio::test]
async fn generated_workflow_schema_is_registered_as_exact_metadata_and_invalid_schemas_do_not_mutate()
 {
    let h = harness(
        "v0.114.0",
        vec![Reply::ok(
            proto::v1::CreateWorkflowVersionResponse::default(),
        )],
    )
    .await;
    let task = h.client.task::<Value, Value>("typed");
    let declaration = WorkflowDeclaration::<Value, Value>::new("typed")
        .input_schema_for::<Profile>()
        .step(&task, TaskConfig::default());
    let expected = declaration.input_schema.clone().unwrap();
    h.client.register_workflow(declaration).await.unwrap();
    let reqs = h.requests.lock().await;
    let (_, body) = reqs
        .iter()
        .find(|(p, _)| p.ends_with("/PutWorkflow"))
        .unwrap();
    let request = proto::v1::CreateWorkflowVersionRequest::decode(body.as_slice()).unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&request.input_json_schema.unwrap()).unwrap(),
        expected
    );
    drop(reqs);
    let before = h.requests.lock().await.len();
    for invalid in [
        json!({"type":7}),
        json!({"$ref":"#/$defs/missing"}),
        json!({"$ref":"http://[::1]:1/no-network"}),
        json!({"$ref":"file:///nonexistent/no-file"}),
    ] {
        assert!(matches!(
            h.client
                .register_workflow(
                    WorkflowDeclaration::<Value, Value>::new("invalid").input_schema(invalid)
                )
                .await,
            Err(Error::Config(_))
        ));
    }
    assert_eq!(h.requests.lock().await.len(), before);
}
#[tokio::test]
async fn rejected_registration_preserves_the_real_engine_error_after_schema_validation() {
    let h = harness("v0.114.0", vec![Reply::error(3, vec![]); 4]).await;
    assert!(
        h.client
            .register_workflow(
                WorkflowDeclaration::<Value, Value>::new("rejected").input_schema_for::<Profile>()
            )
            .await
            .is_err()
    );
}
fn ids() -> ContextIds {
    ContextIds {
        workflow_name: "schema-wf".into(),
        task_name: "schema-task".into(),
        workflow_run_id: WorkflowRunId("schema-run".into()),
        task_run_id: "schema-task-run".into(),
        workflow_id: None,
        workflow_version_id: None,
    }
}
#[tokio::test]
async fn runtime_validates_normal_durable_and_each_batch_member_before_handler_and_decode_is_not_retried()
 {
    let client = Client::new(Arc::new(InMemoryTransport::new()));
    let schema = json!({"type":"integer","minimum":3});
    let normal = client
        .task::<Value, Value>("normal")
        .with_config(rules(schema.clone()));
    let durable = client
        .durable_task::<Value, Value>("durable")
        .with_config(rules(schema.clone()));
    let batch = client
        .task::<Value, Value>("batch")
        .with_config(TaskConfig {
            batch: Some(BatchConfig {
                max_size: 2,
                max_interval: Some(Duration::from_secs(1)),
                group_key: None,
                group_max_runs: None,
                broadcast_output: false,
            }),
            ..rules(schema)
        });
    let narrow = client.task::<u8, u8>("narrow");
    let calls = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&calls);
    let dcount = Arc::clone(&calls);
    let bcount = Arc::clone(&calls);
    let worker = client
        .worker("schema-runtime")
        .register_task(&normal, move |v, _: InMemoryContext<InMemoryTransport>| {
            count.fetch_add(1, Ordering::SeqCst);
            async { Ok(v) }
        })
        .register_durable_task(
            &durable,
            move |v, _: InMemoryDurableContext<InMemoryTransport>| {
                dcount.fetch_add(1, Ordering::SeqCst);
                async { Ok(v) }
            },
        )
        .register_task(&batch, move |v, _: InMemoryContext<InMemoryTransport>| {
            bcount.fetch_add(1, Ordering::SeqCst);
            async { Ok(v) }
        })
        .register_task(
            &narrow,
            |v, _: InMemoryContext<InMemoryTransport>| async move { Ok(v) },
        )
        .build()
        .start()
        .unwrap();
    for error in [
        worker
            .run_task_json("normal", json!(2), ids())
            .await
            .unwrap_err(),
        worker
            .run_durable_task_json("durable", json!(2), ids(), 1)
            .await
            .unwrap_err(),
        worker
            .run_task_json("batch", json!({"one":3,"two":2}), ids())
            .await
            .unwrap_err(),
        worker
            .run_task_json("narrow", json!(300), ids())
            .await
            .unwrap_err(),
    ] {
        assert!(matches!(error, Error::InputValidation(_)));
        assert!(!error.is_retryable());
    }
    let key = "🔐".repeat(10000);
    let Error::InputValidation(messages) = worker
        .run_task_json("batch", json!({key:2}), ids())
        .await
        .unwrap_err()
    else {
        panic!("expected input validation")
    };
    assert_eq!(messages.len(), 1);
    assert!(messages[0].len() <= 515);
    assert!(messages[0].ends_with("..."));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        worker
            .run_task_json("normal", json!(3), ids())
            .await
            .unwrap(),
        json!(3)
    );
    assert_eq!(
        worker
            .run_durable_task_json("durable", json!(3), ids(), 1)
            .await
            .unwrap(),
        json!(3)
    );
    assert_eq!(
        worker
            .run_task_json("batch", json!({"one":3,"two":4}), ids())
            .await
            .unwrap(),
        json!({"one":3,"two":4})
    );
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    worker.shutdown().await.unwrap();
}
#[tokio::test]
async fn invalid_worker_schemas_fail_before_start_and_external_resources_are_never_retrieved() {
    let listener = tokio::net::TcpListener::bind("[::1]:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), br#"{"type":"integer"}"#).unwrap();
    let client = Client::new(Arc::new(InMemoryTransport::new()));
    for schema in [
        json!({"type":"made-up"}),
        json!({"$ref":format!("http://{address}/schema")}),
        json!({"$ref":format!("file://{}",file.path().display())}),
    ] {
        let task = client
            .task::<Value, Value>("invalid")
            .with_config(rules(schema));
        assert!(matches!(
            client
                .worker("invalid")
                .register_task(
                    &task,
                    |v, _: InMemoryContext<InMemoryTransport>| async move { Ok(v) }
                )
                .build()
                .start(),
            Err(Error::Config(_))
        ));
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err()
    );
}

#[test]
fn schema_diagnostics_omit_nested_values_and_bound_error_count_and_path_bytes() {
    for schema in [
        json!({"propertyNames":{"pattern":"^[a-z]+$"}}),
        json!({"additionalProperties":false}),
    ] {
        let errors = rules(schema).validate_input_value(&json!({"private-secret-431":3}));
        assert!(!errors.is_empty());
        assert!(!errors.join(" ").contains("private-secret-431"));
    }
    let input = Value::Array((0..1000).map(|_| json!("private-value")).collect());
    let errors = rules(json!({"items":{"type":"integer"}})).validate_input_value(&input);
    assert_eq!(errors.len(), 16);
    assert!(errors.last().unwrap().contains("omitted"));
    assert!(!errors.join(" ").contains("private-value"));
    let key = "🔐".repeat(10000);
    let input = json!({key:"wrong"});
    let errors =
        rules(json!({"additionalProperties":{"type":"integer"}})).validate_input_value(&input);
    assert!(!errors.is_empty());
    assert!(errors[0].len() <= 515);
    assert!(errors[0].ends_with("..."));
}
