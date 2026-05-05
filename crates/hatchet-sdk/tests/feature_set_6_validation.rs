use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use hatchet_sdk::{
    Client, ConcurrencyConfig, Condition, ConditionAction, Conditions, Context, ContextExt,
    ContextIds, DurableContext, DurableContextExt, EventRecord, InMemoryContext,
    InMemoryDurableContext, InMemoryTransport, LabelValue, RateLimitDuration, RunOpts, RunStatus,
    StepConditions, TaskConfig, WorkerInfo, WorkflowDeclaration, WorkflowRunId,
    conditions_to_rendered, or, render_conditions,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct WorkflowInput {
    value: u32,
}

type WorkflowOutput = u32;

fn base_ids() -> ContextIds {
    ContextIds {
        workflow_name: "parent-workflow".to_owned(),
        task_name: "parent-task".to_owned(),
        workflow_run_id: WorkflowRunId("parent-run-1".to_owned()),
        task_run_id: "task-run-1".to_owned(),
        workflow_id: None,
        workflow_version_id: None,
    }
}

#[tokio::test]
async fn validates_declaration_and_run_submission() {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(Arc::clone(&transport));

    let workflow = client.workflow::<WorkflowInput, WorkflowOutput>("email.workflow");
    let opts = RunOpts::default();
    let single = workflow
        .run_no_wait(WorkflowInput { value: 7 }, opts.clone())
        .await;
    let many = workflow
        .run_many_no_wait(
            [
                WorkflowInput { value: 8 },
                WorkflowInput { value: 9 },
                WorkflowInput { value: 10 },
            ],
            opts,
        )
        .await;

    assert!(single.is_ok());
    assert!(many.is_ok());

    let submitted = transport.submitted_runs().await;
    assert_eq!(submitted.len(), 4);
    assert_eq!(submitted[0].workflow_name, "email.workflow");
    assert_eq!(submitted[0].input, json!({ "value": 7 }));

    let run_records = client.runs().list().await.unwrap();
    assert_eq!(run_records.len(), 4);
}

#[tokio::test]
async fn validates_child_workflow_metadata_propagation() {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(Arc::clone(&transport));
    let context = InMemoryContext::new(base_ids(), Arc::clone(&transport));
    let child = client.workflow::<WorkflowInput, WorkflowOutput>("child.workflow");

    let first = context
        .run_child(&child, WorkflowInput { value: 1 }, RunOpts::default())
        .await;
    let second = context
        .run_child_no_wait(&child, WorkflowInput { value: 2 }, RunOpts::default())
        .await;
    let bulk = context
        .bulk_run_children_no_wait(
            &child,
            [
                WorkflowInput { value: 3 },
                WorkflowInput { value: 4 },
                WorkflowInput { value: 5 },
            ],
            RunOpts::default(),
        )
        .await;

    assert!(first.is_ok());
    assert!(second.is_ok());
    assert!(bulk.is_ok());

    let submitted = transport.submitted_runs().await;
    assert_eq!(submitted.len(), 5);

    let first_meta = &submitted[0].opts;
    assert_eq!(
        first_meta.parent_workflow_run_id.as_deref(),
        Some("parent-run-1")
    );
    assert_eq!(first_meta.parent_task_run_id.as_deref(), Some("task-run-1"));
    assert_eq!(first_meta.child_index, Some(0));

    let second_meta = &submitted[1].opts;
    assert_eq!(second_meta.child_index, Some(1));

    let bulk_indices: Vec<_> = submitted[2..]
        .iter()
        .map(|run| run.opts.child_index)
        .collect();
    assert_eq!(bulk_indices, vec![Some(2), Some(3), Some(4)]);
}

#[tokio::test]
async fn validates_durable_wait_and_spawn_children() {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(Arc::clone(&transport));
    let durable = Arc::new(InMemoryDurableContext::new(
        base_ids(),
        Arc::clone(&transport),
        3,
    ));
    let child = client.workflow::<WorkflowInput, WorkflowOutput>("durable.child");

    let emit_ctx = Arc::clone(&durable);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let _ = emit_ctx.emit_event("approval", json!({ "approved": true }));
    });

    let received = durable
        .wait_for_event("approval", Duration::from_millis(250))
        .await;
    assert_eq!(durable.invocation_count(), 3);
    assert_eq!(received.ok().flatten(), Some(json!({ "approved": true })));

    let spawned = durable
        .spawn_children(
            &child,
            [WorkflowInput { value: 11 }, WorkflowInput { value: 12 }],
            RunOpts::default(),
        )
        .await;
    assert!(spawned.is_ok());
    assert_eq!(spawned.ok().map_or(0, |runs| runs.len()), 2);

    let submitted = transport.submitted_runs().await;
    assert_eq!(submitted.len(), 2);
}

#[tokio::test]
async fn validates_durable_wait_or_lookback_and_event_matching() {
    let transport = Arc::new(InMemoryTransport::new());
    let durable = Arc::new(InMemoryDurableContext::new(
        base_ids(),
        Arc::clone(&transport),
        1,
    ));

    let historical_payload = json!({
        "user": { "email": "ops@example.com" },
        "approved": true
    });
    durable
        .emit_scoped_event("approval.history", "tenant-a", historical_payload.clone())
        .expect("historical event should record");

    let looked_back = durable
        .wait_for(or([
            Condition::event("approval.history")
                .scope("tenant-a")
                .lookback(Duration::from_secs(1))
                .readable_data_key("approval-hit")
                .expression("user.email != null")
                .build(),
            Condition::sleep_for(Duration::from_millis(50)),
        ]))
        .await
        .expect("lookback wait should resolve");
    assert_eq!(looked_back.matched_condition_index, 0);
    assert_eq!(looked_back.payload, Some(historical_payload));
    assert_eq!(
        looked_back.matched_readable_data_key.as_deref(),
        Some("approval-hit")
    );

    durable
        .emit_event("approval.null", json!({ "user": { "email": null } }))
        .expect("null payload should record");
    let filtered = durable
        .wait_for(or([
            Condition::event("approval.null")
                .lookback(Duration::from_secs(1))
                .expression("user.email != null")
                .build(),
            Condition::sleep_for(Duration::from_millis(5)),
        ]))
        .await
        .expect("filtered wait should resolve");
    assert_eq!(filtered.matched_condition_index, 1);

    let emit_ctx = Arc::clone(&durable);
    let started = tokio::time::Instant::now();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let _ = emit_ctx.emit_event("approval.live", json!({ "approved": true }));
    });

    let raced = durable
        .wait_for(or([
            Condition::sleep_for(Duration::from_millis(200)),
            Condition::event("approval.live")
                .expression("approved == true")
                .readable_data_key("approval-live")
                .build(),
        ]))
        .await
        .expect("or wait should resolve");
    assert_eq!(raced.matched_condition_index, 1);
    assert_eq!(raced.payload, Some(json!({ "approved": true })));
    assert_eq!(
        raced.matched_readable_data_key.as_deref(),
        Some("approval-live")
    );
    assert!(started.elapsed() < Duration::from_millis(150));
}

#[test]
fn validates_condition_metadata_rendering_for_ts_parity() {
    let rendered = render_conditions(
        ConditionAction::Create,
        [
            or([
                Condition::sleep_for(Duration::from_secs(5)).readable_data_key("sleep-branch"),
                Condition::event("approval.live")
                    .scope("tenant-a")
                    .readable_data_key("event-branch")
                    .build(),
            ]),
            Condition::event("approval.audit").build().into(),
        ],
    );

    assert_eq!(rendered.len(), 3);
    assert_eq!(rendered[0].metadata().action, Some(ConditionAction::Create));
    assert_eq!(rendered[1].metadata().action, Some(ConditionAction::Create));
    assert_eq!(rendered[2].metadata().action, Some(ConditionAction::Create));

    let first_or_group = rendered[0]
        .metadata()
        .or_group_id
        .clone()
        .expect("first OR condition should have a group id");
    assert_eq!(
        rendered[1].metadata().or_group_id.as_deref(),
        Some(first_or_group.as_str())
    );
    assert_eq!(rendered[2].metadata().or_group_id, None);

    let protocol = conditions_to_rendered(&rendered);
    assert_eq!(protocol.sleep_conditions.len(), 1);
    assert_eq!(protocol.user_event_conditions.len(), 2);

    let sleep = &protocol.sleep_conditions[0];
    assert_eq!(sleep.base.readable_data_key, "sleep-branch");
    assert_eq!(sleep.base.action, ConditionAction::Create);
    assert_eq!(sleep.base.or_group_id, first_or_group);

    let first_event = &protocol.user_event_conditions[0];
    assert_eq!(first_event.base.readable_data_key, "event-branch");
    assert_eq!(first_event.base.action, ConditionAction::Create);
    assert_eq!(first_event.scope.as_deref(), Some("tenant-a"));

    let standalone_event = &protocol.user_event_conditions[1];
    assert_eq!(standalone_event.base.readable_data_key, "approval.audit");
    assert_eq!(standalone_event.base.action, ConditionAction::Create);
    assert!(!standalone_event.base.or_group_id.is_empty());
}

#[tokio::test]
async fn validates_worker_dispatch_paths() {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(Arc::clone(&transport));

    let add_task = client.task::<u32, u32>("add-two");
    let durable_task = client.durable_task::<u32, u32>("multiply-three");

    let worker = client
        .worker("test-worker")
        .slots(2)
        .durable_slots(2)
        .register_task(&add_task, |input, _ctx| async move { Ok(input + 2) })
        .register_durable_task(&durable_task, |input, ctx| async move {
            Ok(input * 3 + ctx.invocation_count())
        })
        .build();

    let handle = worker.start().expect("worker should start");
    let ids = base_ids();

    let normal = handle.run_task(&add_task, 5_u32, ids.clone()).await;
    let durable = handle.run_durable_task(&durable_task, 4_u32, ids, 2).await;

    assert_eq!(normal.ok(), Some(7));
    assert_eq!(durable.ok(), Some(14));

    let stopped = handle.shutdown().await;
    assert!(stopped.is_ok());
}

#[tokio::test]
async fn validates_worker_runtime_enforcement() {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(Arc::clone(&transport));

    let guarded = client
        .task::<WorkflowInput, WorkflowOutput>("guarded")
        .with_config(
            TaskConfig::builder()
                .desired_label(
                    "region",
                    hatchet_sdk::DesiredLabelConfig::new("us").required(true),
                )
                .input_schema(json!({
                    "type": "object",
                    "required": ["value"],
                    "properties": {
                        "value": { "type": "integer" }
                    }
                }))
                .build(),
        );

    let invalid = client
        .task::<WorkflowInput, WorkflowOutput>("invalid")
        .with_config(
            TaskConfig::builder()
                .input_schema(json!({
                    "type": "object",
                    "required": ["tenant"]
                }))
                .build(),
        );

    let worker = client
        .worker("runtime-enforcement")
        .labels(BTreeMap::from([("region".to_owned(), "us".to_owned())]))
        .register_task(&guarded, |input, _ctx| async move { Ok(input.value + 1) })
        .register_task(&invalid, |input, _ctx| async move { Ok(input.value + 1) })
        .build();

    let handle = worker.start().expect("worker should start");
    let guarded_ok = handle
        .run_task(&guarded, WorkflowInput { value: 10 }, base_ids())
        .await;
    assert_eq!(guarded_ok.ok(), Some(11));

    let invalid_result = handle
        .run_task(&invalid, WorkflowInput { value: 10 }, base_ids())
        .await;
    assert!(matches!(
        invalid_result,
        Err(hatchet_sdk::Error::InputValidation(_))
    ));

    handle.pause().await.expect("pause should succeed");
    let paused_result = handle
        .run_task(&guarded, WorkflowInput { value: 11 }, base_ids())
        .await;
    assert!(paused_result.is_err());
    handle.shutdown().await.expect("shutdown should succeed");
}

#[tokio::test]
async fn validates_feature_client_operations() {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(Arc::clone(&transport));

    let workflow = client.workflow::<WorkflowInput, WorkflowOutput>("feature.workflow");
    let run = workflow
        .run_no_wait(WorkflowInput { value: 42 }, RunOpts::default())
        .await
        .expect("run should succeed");
    let run_id = run.run_id().clone();

    let workflow_def = client
        .workflows()
        .put("feature.workflow", json!({ "v": 1 }))
        .await;
    assert_eq!(workflow_def.version, 1);
    assert_eq!(
        client.workflows().version("feature.workflow").await.ok(),
        Some(1)
    );

    let created_schedule = client
        .schedules()
        .create(
            "feature.workflow",
            "daily",
            "0 0 * * *",
            json!({ "kind": "daily" }),
        )
        .await
        .unwrap();
    assert_eq!(created_schedule.name, "daily");
    assert_eq!(
        client.schedules().get("daily").await.ok().map(|s| s.name),
        Some("daily".to_owned())
    );

    let created_cron = client
        .crons()
        .create("feature.workflow", "nightly", "0 2 * * *", json!({}))
        .await
        .unwrap();
    assert_eq!(created_cron.name, "nightly");

    let worker_info = WorkerInfo {
        name: "worker-a".to_owned(),
        paused: false,
        labels: BTreeMap::from([("region".to_owned(), "us".to_owned())]),
    };
    let updated_worker = client.workers().update(worker_info).await;
    assert_eq!(updated_worker.name, "worker-a");
    assert!(client.workers().get("worker-a").await.is_ok());

    let upserted = client
        .rate_limits()
        .upsert("default", 10, RateLimitDuration::Minute)
        .await
        .unwrap();
    assert_eq!(upserted, "default");
    assert_eq!(
        client
            .rate_limits()
            .list()
            .await
            .unwrap()
            .get("default")
            .copied(),
        Some(10)
    );
    client.rate_limits().put("compat", 5).await;
    assert_eq!(
        client
            .rate_limits()
            .list()
            .await
            .unwrap()
            .get("compat")
            .copied(),
        Some(5)
    );
    client.rate_limits().delete("compat").await;
    assert!(!client
        .rate_limits()
        .list()
        .await
        .unwrap()
        .contains_key("compat"));

    client
        .events()
        .push(EventRecord {
            key: "order.created".to_owned(),
            payload: json!({ "id": "ord-1" }),
        })
        .await;
    assert_eq!(client.events().list().await.unwrap().len(), 1);

    let created_filter = client
        .filters()
        .create(
            "feature.workflow",
            "high-priority",
            "payload.priority == 'high'",
        )
        .await
        .unwrap();
    assert_eq!(created_filter.name, "high-priority");

    let created_webhook = client
        .webhooks()
        .create("ops", "https://example.com/webhook")
        .await
        .unwrap();
    assert_eq!(created_webhook.name, "ops");

    let status_before = client.runs().get_status(&run_id).await;
    assert_eq!(status_before.ok(), Some(RunStatus::Queued));
    assert!(client.runs().cancel(&run_id).await.is_ok());
    let status_after = client.runs().get_status(&run_id).await;
    assert_eq!(status_after.ok(), Some(RunStatus::Cancelled));

    let replay = client
        .runs()
        .replay(&run_id)
        .await
        .expect("replay should succeed");
    assert!(replay.0.starts_with("run-"));

    let queue_metrics = client.metrics().get_queue_metrics().await.unwrap();
    assert_eq!(queue_metrics.total_runs, 2);
    assert_eq!(queue_metrics.queued_runs, 1);

    let cel = client
        .cel()
        .debug("payload.id != ''", &json!({ "id": "ord-1" }));
    assert!(cel.accepted);

    assert!(client.schedules().delete("daily").await.is_ok());
    assert!(client.crons().delete("nightly").await.is_ok());
    assert!(client.filters().delete("high-priority").await.is_ok());
    assert!(client.webhooks().delete("ops").await.is_ok());
}

#[tokio::test]
async fn validates_task_config_and_workflow_declaration_builders() {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(Arc::clone(&transport));

    let validate = client
        .task::<WorkflowInput, WorkflowOutput>("validate")
        .with_config(
            TaskConfig::builder()
                .retries(3)
                .concurrency(ConcurrencyConfig::new("input.tenant").max_runs(2))
                .build(),
        );
    let charge = client.task::<WorkflowInput, WorkflowOutput>("charge");

    let declaration = WorkflowDeclaration::<WorkflowInput, WorkflowOutput>::new("checkout")
        .on_event("order.created")
        .task_defaults(TaskConfig::builder().retries(2).build())
        .step(&validate, validate.config().clone())
        .step_after(&charge, &[&validate], TaskConfig::default());

    let registered = client.register_workflow(declaration.clone()).await;
    assert_eq!(registered.name, "checkout");
    assert_eq!(registered.steps.len(), 2);
    assert_eq!(registered.steps[1].parents, vec!["validate".to_owned()]);
    assert!(client.workflows().get("checkout").await.is_ok());
}

#[tokio::test]
async fn validates_expanded_context_and_durable_helpers() {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(Arc::clone(&transport));
    let workflow = client.workflow::<WorkflowInput, WorkflowOutput>("ctx.child");
    let context = InMemoryContext::new(base_ids(), Arc::clone(&transport))
        .with_run_opts(&RunOpts {
            additional_metadata: [("source".to_owned(), "test".to_owned())]
                .into_iter()
                .collect(),
            priority: None,
            sticky: None,
            child_key: Some("child-key".to_owned()),
            desired_worker_labels: Default::default(),
            parent_workflow_run_id: Some("parent-run-1".to_owned()),
            parent_task_run_id: Some("task-run-1".to_owned()),
            child_index: Some(7),
        })
        .with_retry_count(2)
        .with_filter_payload(json!({"ok": true}))
        .with_parent_output(json!({"value": 41}))
        .with_worker_id("worker-1")
        .with_worker_label("region", "us");

    assert_eq!(context.retry_count(), 2);
    assert_eq!(context.child_index(), Some(7));
    assert_eq!(context.child_key(), Some("child-key"));
    assert_eq!(context.parent_workflow_run_id(), Some("parent-run-1"));
    assert_eq!(
        context.additional_metadata().get("source"),
        Some(&"test".to_owned())
    );
    assert_eq!(context.filter_payload(), Some(&json!({"ok": true})));
    assert_eq!(context.worker_id(), Some("worker-1"));
    assert_eq!(
        context.worker_labels().get("region"),
        Some(&LabelValue::String("us".to_owned()))
    );
    assert!(
        context
            .parent_output::<serde_json::Value>()
            .await
            .ok()
            .flatten()
            .is_some()
    );

    let no_wait = ContextExt::run_child_no_wait(
        &context,
        &workflow,
        WorkflowInput { value: 1 },
        RunOpts::default(),
    )
    .await
    .expect("child run should submit");
    assert!(no_wait.run_id().0.starts_with("run-"));

    let durable = Arc::new(InMemoryDurableContext::new(
        base_ids(),
        Arc::clone(&transport),
        1,
    ));
    let memo = durable.memo("now", json!({"v": 1})).await.expect("memo");
    let waited = durable
        .wait_for(Condition::sleep_for(Duration::from_millis(1)).into())
        .await
        .expect("wait_for");
    let now_a = durable.now().await;
    let now_b = durable.now().await;

    assert_eq!(memo, json!({"v": 1}));
    assert_eq!(waited.matched_condition_index, 0);
    assert_eq!(now_a, now_b);
    assert!(!durable.supports_eviction());

    let outputs = DurableContextExt::spawn_children(
        &*durable,
        &workflow,
        [WorkflowInput { value: 2 }, WorkflowInput { value: 3 }],
        RunOpts::default(),
    )
    .await;
    assert!(outputs.is_err());
}

#[tokio::test]
async fn validates_workflow_scheduler_executes_registered_steps() {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(Arc::clone(&transport));

    let validate = client.task::<WorkflowInput, WorkflowOutput>("checkout.validate");
    let finalize = client.task::<WorkflowInput, WorkflowOutput>("checkout.finalize");

    let worker = client
        .worker("scheduler-worker")
        .register_task(&validate, |input, ctx| async move {
            ctx.put_stream(json!("validate")).await?;
            Ok(input.value + 1)
        })
        .register_task(&finalize, |input, ctx| async move {
            ctx.put_stream(json!("finalize")).await?;
            Ok(input.value + 10)
        })
        .build();
    let handle = worker.start().expect("worker should start");

    let declaration =
        WorkflowDeclaration::<WorkflowInput, WorkflowOutput>::new("checkout.workflow")
            .step(&validate, TaskConfig::default())
            .step_after(&finalize, &[&validate], TaskConfig::default());
    client.register_workflow(declaration).await;

    let workflow = client.workflow::<WorkflowInput, WorkflowOutput>("checkout.workflow");
    let run_ref = workflow
        .run_no_wait(WorkflowInput { value: 1 }, RunOpts::default())
        .await
        .expect("run ref should exist");
    let output = run_ref.output().await.expect("workflow should execute");
    let stream = run_ref.stream().await.expect("stream should load");
    assert_eq!(output, 11);
    assert_eq!(stream, vec![json!("validate"), json!("finalize")]);

    handle.shutdown().await.expect("shutdown should succeed");
}

#[test]
fn validates_parent_condition_construction_and_rendering() {
    // Parent condition with expression
    let parent = Condition::parent("validate-task")
        .expression("input.status == 'complete'")
        .readable_data_key("parent-check")
        .build();

    match &parent {
        Condition::Parent(inner) => {
            assert_eq!(inner.parent_task_name, "validate-task");
            assert_eq!(
                inner.expression.as_deref(),
                Some("input.status == 'complete'")
            );
            assert_eq!(
                inner.metadata.readable_data_key.as_deref(),
                Some("parent-check")
            );
        }
        _ => panic!("expected Parent variant"),
    }

    // Parent condition with default readable_data_key
    let parent_default = Condition::parent("finalize").build();
    match &parent_default {
        Condition::Parent(inner) => {
            assert_eq!(
                inner.metadata.readable_data_key.as_deref(),
                Some("parent-finalize")
            );
        }
        _ => panic!("expected Parent variant"),
    }

    // Parent condition renders into parent_override_conditions
    let rendered = render_conditions(
        ConditionAction::Queue,
        [parent.into(), parent_default.into()],
    );
    assert_eq!(rendered.len(), 2);
    assert_eq!(rendered[0].metadata().action, Some(ConditionAction::Queue));
    assert_eq!(rendered[1].metadata().action, Some(ConditionAction::Queue));

    let protocol = conditions_to_rendered(&rendered);
    assert_eq!(protocol.parent_override_conditions.len(), 2);
    assert_eq!(protocol.sleep_conditions.len(), 0);
    assert_eq!(protocol.user_event_conditions.len(), 0);

    let first = &protocol.parent_override_conditions[0];
    assert_eq!(first.parent_readable_id, "validate-task");
    assert_eq!(first.base.readable_data_key, "parent-check");
    assert_eq!(first.base.action, ConditionAction::Queue);
    assert_eq!(first.base.expression, "input.status == 'complete'");

    let second = &protocol.parent_override_conditions[1];
    assert_eq!(second.parent_readable_id, "finalize");
    assert_eq!(second.base.readable_data_key, "parent-finalize");
}

#[test]
fn validates_parent_condition_in_or_group() {
    let conditions = or([
        Condition::parent("step-a")
            .expression("input.ok == true")
            .build(),
        Condition::event("timeout").build(),
        Condition::sleep_for(Duration::from_secs(30)),
    ]);

    let rendered = render_conditions(ConditionAction::Cancel, [conditions]);
    assert_eq!(rendered.len(), 3);

    // All three should share the same OR group id
    let or_group = rendered[0]
        .metadata()
        .or_group_id
        .as_ref()
        .expect("should have or_group_id");
    assert_eq!(
        rendered[1].metadata().or_group_id.as_deref(),
        Some(or_group.as_str())
    );
    assert_eq!(
        rendered[2].metadata().or_group_id.as_deref(),
        Some(or_group.as_str())
    );

    let protocol = conditions_to_rendered(&rendered);
    assert_eq!(protocol.parent_override_conditions.len(), 1);
    assert_eq!(protocol.user_event_conditions.len(), 1);
    assert_eq!(protocol.sleep_conditions.len(), 1);

    // All share the same OR group in protocol output
    assert_eq!(
        protocol.parent_override_conditions[0].base.or_group_id,
        protocol.user_event_conditions[0].base.or_group_id
    );
    assert_eq!(
        protocol.parent_override_conditions[0].base.or_group_id,
        protocol.sleep_conditions[0].base.or_group_id
    );
}

#[test]
fn validates_default_readable_keys_across_all_condition_types() {
    // Sleep: defaults to "sleep-{duration}"
    let sleep = Condition::sleep_for(Duration::from_secs(10));
    let rendered_sleep = render_conditions(ConditionAction::Create, [sleep.into()]);
    let protocol = conditions_to_rendered(&rendered_sleep);
    assert_eq!(
        protocol.sleep_conditions[0].base.readable_data_key,
        "sleep-10s"
    );

    // Sleep ms: defaults to "sleep-{millis}ms"
    let sleep_ms = Condition::sleep_for(Duration::from_millis(500));
    let rendered = render_conditions(ConditionAction::Create, [sleep_ms.into()]);
    let protocol = conditions_to_rendered(&rendered);
    assert_eq!(
        protocol.sleep_conditions[0].base.readable_data_key,
        "sleep-500ms"
    );

    // SleepUntil: defaults to "sleep-until"
    let sleep_until = Condition::sleep_until(std::time::SystemTime::now());
    let rendered = render_conditions(ConditionAction::Create, [sleep_until.into()]);
    let protocol = conditions_to_rendered(&rendered);
    assert_eq!(
        protocol.sleep_until_conditions[0].base.readable_data_key,
        "sleep-until"
    );

    // Event: defaults to event key
    let event = Condition::event("user:created").build();
    let rendered = render_conditions(ConditionAction::Create, [event.into()]);
    let protocol = conditions_to_rendered(&rendered);
    assert_eq!(
        protocol.user_event_conditions[0].base.readable_data_key,
        "user:created"
    );

    // Parent: defaults to "parent-{task_name}"
    let parent = Condition::parent("validate").build();
    let rendered = render_conditions(ConditionAction::Create, [parent.into()]);
    let protocol = conditions_to_rendered(&rendered);
    assert_eq!(
        protocol.parent_override_conditions[0]
            .base
            .readable_data_key,
        "parent-validate"
    );
}

#[test]
fn validates_declaration_level_conditions_on_workflow_steps() {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(Arc::clone(&transport));

    let validate = client.task::<WorkflowInput, WorkflowOutput>("validate");
    let charge = client.task::<WorkflowInput, WorkflowOutput>("charge");
    let notify = client.task::<WorkflowInput, WorkflowOutput>("notify");

    let declaration = WorkflowDeclaration::<WorkflowInput, WorkflowOutput>::new("checkout")
        .on_event("order.created")
        .step_with(
            &validate,
            TaskConfig::default(),
            StepConditions::new()
                .wait_for(or([
                    Condition::event("approval")
                        .expression("approved == true")
                        .build(),
                    Condition::sleep_for(Duration::from_secs(3600)),
                ]))
                .cancel_if(Condition::event("order.cancelled").build()),
        )
        .step_after_with(
            &charge,
            &[&validate],
            TaskConfig::default(),
            StepConditions::new().skip_if(
                Condition::parent("validate")
                    .expression("input.skip_charge == true")
                    .build(),
            ),
        )
        .step_after(&notify, &[&charge], TaskConfig::default());

    assert_eq!(declaration.steps.len(), 3);

    // First step: has wait_for and cancel_if
    let step_validate = &declaration.steps[0];
    assert_eq!(step_validate.task_name, "validate");
    assert_eq!(step_validate.wait_for.len(), 1);
    assert_eq!(step_validate.cancel_if.len(), 1);
    assert_eq!(step_validate.skip_if.len(), 0);

    // wait_for has an OR group with 2 conditions
    match &step_validate.wait_for[0] {
        Conditions::Or(conditions) => {
            assert_eq!(conditions.len(), 2);
            let or_group = conditions[0].metadata().or_group_id.as_ref().unwrap();
            assert_eq!(
                conditions[1].metadata().or_group_id.as_deref(),
                Some(or_group.as_str())
            );
        }
        _ => panic!("expected Or variant for wait_for"),
    }

    // cancel_if has a single event condition
    match &step_validate.cancel_if[0] {
        Conditions::Single(Condition::Event(event)) => {
            assert_eq!(event.event_key, "order.cancelled");
        }
        _ => panic!("expected Single Event for cancel_if"),
    }

    // Second step: has skip_if with parent condition
    let step_charge = &declaration.steps[1];
    assert_eq!(step_charge.task_name, "charge");
    assert_eq!(step_charge.parents, vec!["validate"]);
    assert_eq!(step_charge.skip_if.len(), 1);
    assert_eq!(step_charge.wait_for.len(), 0);
    assert_eq!(step_charge.cancel_if.len(), 0);

    match &step_charge.skip_if[0] {
        Conditions::Single(Condition::Parent(parent)) => {
            assert_eq!(parent.parent_task_name, "validate");
            assert_eq!(
                parent.expression.as_deref(),
                Some("input.skip_charge == true")
            );
        }
        _ => panic!("expected Single Parent for skip_if"),
    }

    // Third step: no conditions
    let step_notify = &declaration.steps[2];
    assert_eq!(step_notify.task_name, "notify");
    assert_eq!(step_notify.wait_for.len(), 0);
    assert_eq!(step_notify.cancel_if.len(), 0);
    assert_eq!(step_notify.skip_if.len(), 0);
}

#[test]
fn validates_complex_nested_or_groups_with_mixed_condition_types() {
    // Mirrors the TS test "should handle complex AND/OR combinations"
    let rendered = render_conditions(
        ConditionAction::Create,
        [
            or([
                Condition::sleep_for(Duration::from_secs(5)),
                Condition::event("user:update").build(),
            ]),
            Condition::event("user:create").build().into(),
            or([
                Condition::event("user:update").build(),
                Condition::event("user:delete").build(),
            ]),
        ],
    );

    assert_eq!(rendered.len(), 5);

    // All should have Create action
    for condition in &rendered {
        assert_eq!(condition.metadata().action, Some(ConditionAction::Create));
    }

    // First two share an OR group
    let first_or_group = rendered[0].metadata().or_group_id.as_ref().unwrap();
    assert_eq!(
        rendered[1].metadata().or_group_id.as_deref(),
        Some(first_or_group.as_str())
    );

    // Third (standalone) has no OR group
    assert!(rendered[2].metadata().or_group_id.is_none());

    // Fourth and fifth share a different OR group
    let second_or_group = rendered[3].metadata().or_group_id.as_ref().unwrap();
    assert_eq!(
        rendered[4].metadata().or_group_id.as_deref(),
        Some(second_or_group.as_str())
    );
    assert_ne!(first_or_group, second_or_group);
}

#[test]
fn validates_condition_serialization_round_trip() {
    // Verify conditions survive serde round-trip (needed for workflow declaration transport)
    let original = Condition::event("approval")
        .expression("approved == true")
        .scope("tenant-a")
        .lookback(Duration::from_secs(60))
        .readable_data_key("approval-check")
        .action(ConditionAction::Queue)
        .or_group_id("custom-group")
        .build();

    let json = serde_json::to_string(&original).expect("should serialize");
    let deserialized: Condition = serde_json::from_str(&json).expect("should deserialize");

    assert_eq!(original, deserialized);

    // Round-trip a Conditions::Or
    let or_conditions = or([
        Condition::sleep_for(Duration::from_secs(5)),
        Condition::parent("step-a")
            .expression("done == true")
            .build(),
    ]);

    let json = serde_json::to_string(&or_conditions).expect("should serialize Or");
    let deserialized: Conditions = serde_json::from_str(&json).expect("should deserialize Or");

    assert_eq!(or_conditions, deserialized);
}
