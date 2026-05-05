use std::collections::HashMap;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use hatchet_sdk::*;

fn test_ids(task_name: &str) -> ContextIds {
    ContextIds {
        workflow_name: "test-wf".to_owned(),
        task_name: task_name.to_owned(),
        workflow_run_id: WorkflowRunId("run-test-1".to_owned()),
        task_run_id: format!("task-{task_name}"),
        workflow_id: None,
        workflow_version_id: None,
    }
}

// =============================================================================
// Phase 1: Wire Conditions Into Registration Payload
// =============================================================================

#[test]
fn task_conditions_to_pb_assigns_correct_actions() {
    let step = WorkflowStep {
        task_name: "step-one".to_owned(),
        config: TaskConfig::default(),
        parents: Vec::new(),
        is_durable: false,
        wait_for: vec![Condition::sleep_for(Duration::from_secs(5)).into()],
        cancel_if: vec![Condition::event("cancel-event").build().into()],
        skip_if: vec![Condition::parent("parent-step").build().into()],
    };

    let rendered = task_conditions_to_pb(&step, None);

    assert_eq!(rendered.sleep_conditions.len(), 1);
    assert_eq!(
        rendered.sleep_conditions[0].base.action,
        ConditionAction::Queue
    );

    assert_eq!(rendered.user_event_conditions.len(), 1);
    assert_eq!(
        rendered.user_event_conditions[0].base.action,
        ConditionAction::Cancel
    );

    assert_eq!(rendered.parent_override_conditions.len(), 1);
    assert_eq!(
        rendered.parent_override_conditions[0].base.action,
        ConditionAction::Skip
    );
}

#[test]
fn task_conditions_to_pb_applies_namespace_to_event_keys() {
    let step = WorkflowStep {
        task_name: "step-one".to_owned(),
        config: TaskConfig::default(),
        parents: Vec::new(),
        is_durable: false,
        wait_for: vec![Condition::event("user:created").build().into()],
        cancel_if: vec![Condition::event("order:cancelled").build().into()],
        skip_if: Vec::new(),
    };

    let rendered = task_conditions_to_pb(&step, Some("tenant-a"));

    assert_eq!(rendered.user_event_conditions.len(), 2);
    assert_eq!(
        rendered.user_event_conditions[0].event_key,
        "tenant-a/user:created"
    );
    assert_eq!(
        rendered.user_event_conditions[1].event_key,
        "tenant-a/order:cancelled"
    );
}

#[test]
fn task_conditions_to_pb_empty_conditions_backward_compatible() {
    let step = WorkflowStep {
        task_name: "step-one".to_owned(),
        config: TaskConfig::default(),
        parents: Vec::new(),
        is_durable: false,
        wait_for: Vec::new(),
        cancel_if: Vec::new(),
        skip_if: Vec::new(),
    };

    let rendered = task_conditions_to_pb(&step, None);
    assert!(rendered.is_empty());
}

#[tokio::test]
async fn registration_payload_includes_rendered_conditions() {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(transport.clone());

    let task = client.task::<serde_json::Value, serde_json::Value>("process");

    let declaration = WorkflowDeclaration::<serde_json::Value, serde_json::Value>::new("test-wf")
        .step_with(
            &task,
            TaskConfig::default(),
            StepConditions::new()
                .wait_for(Condition::sleep_for(Duration::from_secs(10)))
                .cancel_if(Condition::event("abort").build()),
        );

    let _ = client.register_workflow(declaration).await;

    let registrations = transport.workflow_registrations().await;
    assert_eq!(registrations.len(), 1);

    let reg = &registrations[0];
    assert_eq!(reg.tasks.len(), 1);
    assert_eq!(reg.tasks[0].conditions.sleep_conditions.len(), 1);
    assert_eq!(reg.tasks[0].conditions.user_event_conditions.len(), 1);
}

// =============================================================================
// Phase 2: Context Trait Completeness
// =============================================================================

#[tokio::test]
async fn context_errors_returns_populated_map() {
    let transport = Arc::new(InMemoryTransport::new());
    let mut errors = HashMap::new();
    errors.insert("step-a".to_owned(), "failed to parse".to_owned());

    let ctx = InMemoryContext::new(test_ids("my-task"), transport).with_errors(errors);

    let ctx_errors = ctx.errors();
    assert_eq!(ctx_errors.len(), 1);
    assert_eq!(ctx_errors.get("step-a").unwrap(), "failed to parse");
}

#[tokio::test]
async fn context_triggers_returns_trigger_data() {
    let transport = Arc::new(InMemoryTransport::new());
    let trigger = TriggerData {
        triggered_by_event: true,
        event_key: Some("user:signup".to_owned()),
        ..TriggerData::default()
    };
    let ctx = InMemoryContext::new(test_ids("my-task"), transport).with_trigger_data(trigger);

    let triggers = ctx.triggers();
    assert!(triggers.triggered_by_event);
    assert_eq!(triggers.event_key.as_deref(), Some("user:signup"));
}

#[tokio::test]
async fn context_cancel_marks_run() {
    let transport = Arc::new(InMemoryTransport::new());
    let ctx = InMemoryContext::new(test_ids("my-task"), transport);

    // cancel should not error
    ctx.cancel().await.unwrap();
}

#[tokio::test]
async fn context_workflow_id_fields() {
    let transport = Arc::new(InMemoryTransport::new());
    let ids = ContextIds {
        workflow_name: "wf".to_owned(),
        task_name: "t".to_owned(),
        workflow_run_id: WorkflowRunId("run-1".to_owned()),
        task_run_id: "task-1".to_owned(),
        workflow_id: Some("wf-id-123".to_owned()),
        workflow_version_id: Some("v-456".to_owned()),
    };
    let ctx = InMemoryContext::new(ids, transport);

    assert_eq!(ctx.workflow_id(), Some("wf-id-123"));
    assert_eq!(ctx.workflow_version_id(), Some("v-456"));
}

// =============================================================================
// Phase 3: DurableContext Completeness (now + memo)
// =============================================================================

#[tokio::test]
async fn durable_now_returns_same_value() {
    let transport = Arc::new(InMemoryTransport::new());
    let ctx = InMemoryDurableContext::new(test_ids("durable-task"), transport, 1);

    let first = ctx.now().await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    let second = ctx.now().await;

    // Both calls should return the exact same memoized value
    assert_eq!(first, second);
}

#[tokio::test]
async fn durable_memo_caches_and_deduplicates() {
    let transport = Arc::new(InMemoryTransport::new());
    let ctx = InMemoryDurableContext::new(test_ids("durable-task"), transport, 1);

    let v1: serde_json::Value = serde_json::json!(42);
    let v2: serde_json::Value = serde_json::json!(999);

    let first = ctx.memo("key-a", v1.clone()).await.unwrap();
    let second = ctx.memo("key-a", v2).await.unwrap();

    // Should return cached first value, not the second
    assert_eq!(first, v1);
    assert_eq!(second, v1);
}

#[tokio::test]
async fn durable_memo_different_keys_different_values() {
    let transport = Arc::new(InMemoryTransport::new());
    let ctx = InMemoryDurableContext::new(test_ids("durable-task"), transport, 1);

    let a = ctx.memo("key-a", serde_json::json!("alpha")).await.unwrap();
    let b = ctx.memo("key-b", serde_json::json!("beta")).await.unwrap();

    assert_eq!(a, serde_json::json!("alpha"));
    assert_eq!(b, serde_json::json!("beta"));
}

// =============================================================================
// Phase 4: Namespace Support
// =============================================================================

#[tokio::test]
async fn namespace_applied_to_workflow_name_and_events() {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(transport.clone()).with_namespace("acme");

    let declaration = WorkflowDeclaration::<serde_json::Value, serde_json::Value>::new("my-flow")
        .on_event("user:created")
        .on_event("order:placed");

    let registered = client.register_workflow(declaration).await;

    assert_eq!(registered.name, "acme/my-flow");
    assert_eq!(
        registered.on_events,
        vec!["acme/user:created", "acme/order:placed"]
    );
}

#[tokio::test]
async fn no_namespace_leaves_names_unchanged() {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(transport.clone());

    let declaration = WorkflowDeclaration::<serde_json::Value, serde_json::Value>::new("my-flow")
        .on_event("user:created");

    let registered = client.register_workflow(declaration).await;

    assert_eq!(registered.name, "my-flow");
    assert_eq!(registered.on_events, vec!["user:created"]);
}

// =============================================================================
// Phase 5: WorkflowDeclaration Lifecycle Hooks
// =============================================================================

#[test]
fn lifecycle_hooks_stored_on_declaration() {
    let client = Client::new(Arc::new(InMemoryTransport::new()));
    let failure_handler = client.task::<(), ()>("on-fail");
    let success_handler = client.task::<(), ()>("on-success");

    let decl = WorkflowDeclaration::<serde_json::Value, serde_json::Value>::new("my-wf")
        .on_failure_task(&failure_handler)
        .on_success_task(&success_handler)
        .description("A test workflow")
        .version("1.2.3");

    assert_eq!(decl.on_failure.as_deref(), Some("on-fail"));
    assert_eq!(decl.on_success.as_deref(), Some("on-success"));
    assert_eq!(decl.description.as_deref(), Some("A test workflow"));
    assert_eq!(decl.version.as_deref(), Some("1.2.3"));
}

#[test]
fn lifecycle_hooks_absent_by_default() {
    let decl = WorkflowDeclaration::<serde_json::Value, serde_json::Value>::new("simple");
    assert!(decl.on_failure.is_none());
    assert!(decl.on_success.is_none());
    assert!(decl.description.is_none());
    assert!(decl.version.is_none());
}

#[tokio::test]
async fn lifecycle_hooks_serialized_in_registration() {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(transport.clone());
    let failure_handler = client.task::<(), ()>("on-fail");

    let decl = WorkflowDeclaration::<serde_json::Value, serde_json::Value>::new("hooks-wf")
        .on_failure_task(&failure_handler)
        .description("With hooks");

    let registered = client.register_workflow(decl).await;
    assert_eq!(registered.on_failure.as_deref(), Some("on-fail"));
}

// =============================================================================
// Phase 6: Worker Lifecycle + Client Gaps
// =============================================================================

#[test]
fn tenant_client_is_accessible() {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(transport);

    // Just verify the accessor compiles and doesn't panic
    let _tenant = client.tenant();
}

#[test]
fn run_ref_creates_valid_ref() {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(transport);

    let run_ref: WorkflowRunRef<serde_json::Value> = client.run_ref("existing-run-123");
    assert_eq!(run_ref.run_id().0, "existing-run-123");
}

#[tokio::test]
async fn worker_pause_unpause_labels() {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(transport);

    let task = client.task::<serde_json::Value, serde_json::Value>("noop");

    let handle = client
        .worker("test-worker")
        .register_task(&task, |_input, _ctx| async { Ok(serde_json::json!(null)) })
        .build()
        .start()
        .unwrap();

    // Test pause/unpause
    assert!(!handle.is_paused().await.unwrap());
    handle.pause().await.unwrap();
    assert!(handle.is_paused().await.unwrap());
    handle.unpause().await.unwrap();
    assert!(!handle.is_paused().await.unwrap());

    // Test upsert_labels
    let mut labels = std::collections::BTreeMap::new();
    labels.insert("region".to_owned(), "us-east".to_owned());
    handle.upsert_labels(labels).await.unwrap();

    let current = handle.labels().await.unwrap();
    assert_eq!(current.get("region").unwrap(), "us-east");

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn worker_wait_until_ready_and_health_server() {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(transport);
    let task = client.task::<serde_json::Value, serde_json::Value>("noop");
    let port = match TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => listener.local_addr().unwrap().port(),
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return,
        Err(error) => panic!("ephemeral port bind should succeed: {error}"),
    };

    let start_result = client
        .worker("healthy-worker")
        .health_port(port)
        .register_task(&task, |_input, _ctx| async { Ok(serde_json::json!(null)) })
        .build()
        .start();

    let handle = match start_result {
        Ok(handle) => handle,
        Err(hatchet_sdk::Error::Worker(message)) if message.contains("Operation not permitted") => {
            return;
        }
        Err(error) => panic!("worker start should succeed: {error}"),
    };

    handle
        .wait_until_ready(Duration::from_secs(1))
        .await
        .unwrap();

    let response = reqwest::get(format!("http://127.0.0.1:{port}/health"))
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();

    assert_eq!(response["worker_name"], "healthy-worker");
    assert_eq!(response["healthy"], true);

    handle.shutdown().await.unwrap();
}

#[tokio::test]
async fn workflow_handle_helpers_cover_run_many_and_scheduling() {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(Arc::clone(&transport));
    let workflow = client.workflow::<u32, u32>("helper.workflow");

    client
        .worker("helper-worker")
        .register_task(
            &client.task::<u32, u32>("helper.workflow"),
            |input, _ctx| async move { Ok(input + 1) },
        )
        .build()
        .start()
        .unwrap()
        .wait_until_ready(Duration::from_secs(1))
        .await
        .unwrap();

    let outputs = workflow
        .run_many([1_u32, 2_u32], RunOpts::default())
        .await
        .unwrap();
    assert_eq!(outputs, vec![2, 3]);

    let schedule = workflow
        .schedule("daily", "0 0 * * *", 7_u32)
        .await
        .unwrap();
    assert_eq!(schedule.workflow_name, "helper.workflow");

    let delay = workflow
        .delay("later", Duration::from_secs(30), 9_u32)
        .await
        .unwrap();
    assert_eq!(delay.expression, "in 30000ms");

    let cron = workflow.cron("nightly", "0 2 * * *").await.unwrap();
    assert_eq!(cron.workflow_name, "helper.workflow");
}

#[tokio::test]
async fn durable_steps_register_and_execute_in_workflow_graph() {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(Arc::clone(&transport));
    let durable = client.durable_task::<u32, u32>("durable.step");

    client
        .worker("durable-worker")
        .register_durable_task(&durable, |input, _ctx| async move { Ok(input + 10) })
        .build()
        .start()
        .unwrap()
        .wait_until_ready(Duration::from_secs(1))
        .await
        .unwrap();

    let declaration =
        WorkflowDeclaration::<serde_json::Value, serde_json::Value>::new("durable-wf")
            .durable_step(&durable, TaskConfig::default());
    client.register_workflow(declaration).await;

    let registrations = transport.workflow_registrations().await;
    assert!(registrations[0].tasks[0].is_durable);

    let output = client
        .workflow::<u32, u32>("durable-wf")
        .run(5, RunOpts::default())
        .await
        .unwrap();
    assert_eq!(output, 15);
}

#[tokio::test]
async fn on_success_hook_is_materialized_as_terminal_step() {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(Arc::clone(&transport));
    let leaf = client.task::<u32, u32>("leaf");
    let on_success = client.task::<u32, u32>("hook.success");

    client
        .worker("hook-worker")
        .register_task(&leaf, |input, _ctx| async move { Ok(input + 1) })
        .register_task(&on_success, |input, _ctx| async move { Ok(input * 2) })
        .build()
        .start()
        .unwrap()
        .wait_until_ready(Duration::from_secs(1))
        .await
        .unwrap();

    let declaration = WorkflowDeclaration::<serde_json::Value, serde_json::Value>::new("hook-wf")
        .step(&leaf, TaskConfig::default())
        .on_success_task(&on_success);
    client.register_workflow(declaration).await;

    let registrations = transport.workflow_registrations().await;
    assert_eq!(registrations[0].tasks.len(), 2);
    assert_eq!(registrations[0].tasks[1].action, "default:hook.success");
    assert_eq!(registrations[0].tasks[1].parents, vec!["leaf"]);

    let output = client
        .workflow::<u32, u32>("hook-wf")
        .run(3, RunOpts::default())
        .await
        .unwrap();
    assert_eq!(output, 6);
}

#[tokio::test]
async fn workers_client_pause_lifecycle_helpers() {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(transport);

    client
        .workers()
        .update(WorkerInfo {
            name: "worker-a".to_owned(),
            paused: false,
            labels: Default::default(),
        })
        .await;

    assert!(!client.workers().is_paused("worker-a").await.unwrap());
    client.workers().pause("worker-a").await.unwrap();
    assert!(client.workers().is_paused("worker-a").await.unwrap());
    client.workers().unpause("worker-a").await.unwrap();
    assert!(!client.workers().is_paused("worker-a").await.unwrap());
}

// =============================================================================
// Phase 7: Display Implementations
// =============================================================================

#[test]
fn condition_display_formatting() {
    let sleep = Condition::sleep_for(Duration::from_secs(10));
    assert_eq!(format!("{sleep}"), "SleepFor(10000ms)");

    let event = Condition::event("user:created").build();
    assert_eq!(format!("{event}"), "Event(user:created)");

    let parent = Condition::parent("step-a").build();
    assert_eq!(format!("{parent}"), "Parent(step-a)");
}

#[test]
fn condition_action_display_formatting() {
    assert_eq!(format!("{}", ConditionAction::Queue), "Queue");
    assert_eq!(format!("{}", ConditionAction::Cancel), "Cancel");
    assert_eq!(format!("{}", ConditionAction::Skip), "Skip");
    assert_eq!(format!("{}", ConditionAction::Create), "Create");
}

#[test]
fn conditions_or_display_formatting() {
    let or_cond = or([
        Condition::sleep_for(Duration::from_secs(5)),
        Condition::event("trigger").build(),
    ]);
    let display = format!("{or_cond}");
    assert!(display.starts_with("Or("));
    assert!(display.contains("SleepFor(5000ms)"));
    assert!(display.contains("Event(trigger)"));
}

// =============================================================================
// Rendered Conditions utility methods
// =============================================================================

#[test]
fn rendered_conditions_is_empty() {
    let empty = RenderedConditions::default();
    assert!(empty.is_empty());

    let step = WorkflowStep {
        task_name: "s".to_owned(),
        config: TaskConfig::default(),
        parents: Vec::new(),
        is_durable: false,
        wait_for: vec![Condition::sleep_for(Duration::from_secs(1)).into()],
        cancel_if: Vec::new(),
        skip_if: Vec::new(),
    };
    let rendered = task_conditions_to_pb(&step, None);
    assert!(!rendered.is_empty());
}

#[test]
fn rendered_conditions_extend_merges() {
    let step_a = WorkflowStep {
        task_name: "a".to_owned(),
        config: TaskConfig::default(),
        parents: Vec::new(),
        is_durable: false,
        wait_for: vec![Condition::sleep_for(Duration::from_secs(1)).into()],
        cancel_if: Vec::new(),
        skip_if: Vec::new(),
    };
    let step_b = WorkflowStep {
        task_name: "b".to_owned(),
        config: TaskConfig::default(),
        parents: Vec::new(),
        is_durable: false,
        wait_for: Vec::new(),
        cancel_if: vec![Condition::event("cancel").build().into()],
        skip_if: Vec::new(),
    };

    let mut a = task_conditions_to_pb(&step_a, None);
    let b = task_conditions_to_pb(&step_b, None);
    a.extend(b);

    assert_eq!(a.sleep_conditions.len(), 1);
    assert_eq!(a.user_event_conditions.len(), 1);
}

// =============================================================================
// Serialization of RenderedConditions (transport readiness)
// =============================================================================

#[test]
fn rendered_conditions_serializable() {
    let step = WorkflowStep {
        task_name: "s".to_owned(),
        config: TaskConfig::default(),
        parents: Vec::new(),
        is_durable: false,
        wait_for: vec![Condition::sleep_for(Duration::from_secs(5)).into()],
        cancel_if: vec![Condition::event("abort").build().into()],
        skip_if: vec![Condition::parent("parent-step").build().into()],
    };
    let rendered = task_conditions_to_pb(&step, None);

    // Should serialize without error
    let json = serde_json::to_value(&rendered).unwrap();
    assert!(json.get("sleep_conditions").unwrap().is_array());
    assert!(json.get("user_event_conditions").unwrap().is_array());
    assert!(json.get("parent_override_conditions").unwrap().is_array());
}
