//! Management requests must use the engine's current, non-deprecated contracts.
mod support;
use another_hatchet_sdk::*;
use serde_json::{Value, json};
use support::*;

const ID: &str = "00000000-0000-4000-8000-000000000001";
const OTHER: &str = "00000000-0000-4000-8000-000000000002";
fn reply(body: Value) -> RestReply {
    RestReply {
        status: 200,
        body: body.to_string(),
        delay: std::time::Duration::ZERO,
    }
}
async fn fixture(replies: Vec<RestReply>) -> Harness {
    harness(
        proto::v1::RunStatus::Running,
        false,
        replies,
        retry_config(3),
    )
    .await
}
fn parts(raw: &[u8]) -> (&str, Value) {
    let (headers, body) = std::str::from_utf8(raw)
        .unwrap()
        .split_once("\r\n\r\n")
        .unwrap();
    (
        headers.lines().next().unwrap(),
        if body.is_empty() {
            Value::Null
        } else {
            serde_json::from_str(body).unwrap()
        },
    )
}
fn workflow() -> Value {
    json!({"rows":[{"metadata":{"id":ID},"name":"orders & returns"}]})
}
fn filter() -> Value {
    json!({"metadata":{"id":OTHER},"expression":"input.ok","scope":"scope","payload":{},"workflowId":ID})
}
fn webhook() -> Value {
    json!({"metadata":{"id":ID},"name":"orders/created","sourceName":"GENERIC","authType":"API_KEY","eventKeyExpression":"'order'","staticPayload":{}})
}

#[tokio::test]
async fn engine_release_is_fetched_and_capabilities_are_available_on_the_feature_client() {
    let h = fixture(vec![graph_reply(false)]).await;
    let version = h.client.engine_version().await.unwrap();
    assert_eq!(version.to_string(), "0.94.14");
    assert!(version.supports(EngineFeature::DurableEviction));
    assert!(!version.supports(EngineFeature::RetryAfter));
    let details = h
        .client
        .runs()
        .get_details(&WorkflowRunId(ID.into()))
        .await
        .unwrap();
    assert_eq!(details.task_runs["leaf"].external_id, "task-1");
}

#[tokio::test]
async fn searches_resolve_names_and_preserve_pagination_and_all_filters() {
    let h = fixture(vec![
        reply(workflow()),
        reply(json!({"rows":[{"metadata":{"id":ID},"status":"FAILED","output":{"ok":false},"additionalMetadata":{"team":"a & b"},"createdAt":"2026-10-01T01:00:00Z","errorMessage":"test failure","duration":-1}],"pagination":{"current_page":2,"next_page":3,"num_pages":4}})),
    ])
    .await;
    let page = h
        .client
        .runs()
        .list_page(RunListOptions {
            filter: RunQuery {
                since: Some("2026-10-01T01:00:00+01:00".into()),
                until: Some("2026-10-02T00:00:00Z".into()),
                statuses: vec![RunStatus::Failed, RunStatus::Cancelled],
                workflow_names: vec!["orders & returns".into()],
                additional_metadata: std::collections::BTreeMap::from([
                    ("team".into(), "a & b".into()),
                    ("region".into(), "us".into()),
                ]),
            },
            offset: 4,
            limit: Some(2),
            worker_id: Some(ID.into()),
            parent_task_external_id: Some(OTHER.into()),
            triggering_event_external_id: Some(ID.into()),
            only_tasks: true,
            include_payloads: Some(false),
        })
        .await
        .unwrap();
    assert_eq!(page.pagination.unwrap().next_page, Some(3));
    assert_eq!(
        page.rows[0].summary.as_ref().unwrap().output,
        Some(json!({"ok":false}))
    );
    assert_eq!(page.rows[0].opts.additional_metadata["team"], "a & b");
    let requests = h.requests.lock().await;
    assert!(
        parts(&requests[0])
            .0
            .contains("name=orders%20%26%20returns")
    );
    let line = parts(&requests[1]).0;
    for part in [
        "offset=4",
        "limit=2",
        "only_tasks=true",
        "include_payloads=false",
        "statuses=FAILED&statuses=CANCELLED",
        "since=2026-10-01T01%3A00%3A00%2B01%3A00",
        "additional_metadata=team%3Aa%20%26%20b",
        "worker_id=",
        "parent_task_external_id=",
        "triggering_event_external_id=",
        "workflow_ids=",
    ] {
        assert!(line.contains(part), "{line} lacks {part}");
    }
    assert!(!line.contains("workflowName="));
}

#[tokio::test]
async fn bulk_selections_and_replay_use_task_ids_and_never_retry_writes() {
    let h = fixture(vec![
        reply(json!({"ids":[OTHER]})),
        reply(workflow()),
        reply(json!({"ids":[ID,OTHER]})),
        error_reply(503),
    ])
    .await;
    let result = h
        .client
        .runs()
        .replay_many(RunSelection::ExternalIds(vec![ID.into(), OTHER.into()]))
        .await
        .unwrap();
    assert_eq!(result.ids, vec![OTHER]);
    let cancelled = h
        .client
        .runs()
        .cancel_many(RunSelection::Filter(RunQuery {
            workflow_names: vec!["orders & returns".into()],
            statuses: vec![RunStatus::Running],
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(cancelled.ids.len(), 2);
    assert!(
        h.client
            .runs()
            .cancel_many(RunSelection::ExternalIds(vec![ID.into()]))
            .await
            .is_err()
    );
    assert!(
        h.client
            .runs()
            .replay_many(RunSelection::ExternalIds(vec![]))
            .await
            .is_err()
    );
    let requests = h.requests.lock().await;
    assert_eq!(requests.len(), 4);
    assert_eq!(parts(&requests[0]).1, json!({"externalIds":[ID,OTHER]}));
    assert_eq!(parts(&requests[2]).1["filter"]["workflowIds"], json!([ID]));
    assert_eq!(
        parts(&requests[2]).1["filter"]["statuses"],
        json!(["RUNNING"])
    );
    assert!(
        parts(&requests[3])
            .0
            .starts_with("POST /api/v1/stable/tenants/synthetic/tasks/cancel ")
    );
}

#[tokio::test]
async fn legacy_replay_preserves_the_workflow_id_and_rejects_an_empty_acknowledgment() {
    let h = fixture(vec![
        reply(json!({"ids":[OTHER]})),
        reply(json!({"ids":[]})),
    ])
    .await;
    assert_eq!(
        h.client
            .runs()
            .replay(&WorkflowRunId(ID.into()))
            .await
            .unwrap()
            .0,
        ID
    );
    assert!(
        h.client
            .runs()
            .replay(&WorkflowRunId(ID.into()))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn workflow_delete_and_pause_use_actual_ids_and_guard_deprecated_engines() {
    let supported = workflow();
    let h = harness_with_version(
        proto::v1::RunStatus::Running,
        false,
        vec![
            reply(workflow()),
            RestReply {
                status: 204,
                body: String::new(),
                delay: std::time::Duration::ZERO,
            },
            reply(supported.clone()),
            reply(json!({"isPaused":true})),
            reply(supported),
            reply(json!({"isPaused":false})),
        ],
        retry_config(0),
        "v0.111.0",
    )
    .await;
    h.client
        .workflows()
        .delete("orders & returns")
        .await
        .unwrap();
    let options = WorkflowPauseOptions {
        paused_workflow_queue_ttl: "1h".into(),
        paused_workflow_cron_run_queue_behavior: PausedRunBehavior::Drop,
        paused_workflow_scheduled_run_queue_behavior: PausedRunBehavior::Queue,
    };
    let old = fixture(vec![reply(workflow())]).await;
    assert!(matches!(
        old.client
            .workflows()
            .pause("orders & returns", options.clone())
            .await,
        Err(Error::UnsupportedOperation(_))
    ));
    assert!(matches!(
        old.client.workflows().unpause("orders & returns").await,
        Err(Error::UnsupportedOperation(_))
    ));
    assert!(old.requests.lock().await.is_empty());
    h.client
        .workflows()
        .pause("orders & returns", options)
        .await
        .unwrap();
    h.client
        .workflows()
        .unpause("orders & returns")
        .await
        .unwrap();
    let requests = h.requests.lock().await;
    assert_eq!(requests.len(), 6);
    assert!(
        parts(&requests[1])
            .0
            .starts_with(&format!("DELETE /api/v1/workflows/{ID} "))
    );
    assert_eq!(
        parts(&requests[3]).1,
        json!({"pause":{"action":"pause","pausedWorkflowQueueTTL":"1h","pausedWorkflowCronRunQueueBehavior":"DROP","pausedWorkflowScheduledRunQueueBehavior":"QUEUE"}})
    );
    assert_eq!(parts(&requests[5]).1, json!({"pause":{"action":"unpause"}}));
}

#[tokio::test]
async fn filter_crud_uses_workflow_id_scope_and_optional_patch_fields() {
    let h = fixture(vec![
        reply(workflow()),
        reply(filter()),
        reply(filter()),
        reply(filter()),
    ])
    .await;
    let created = h
        .client
        .filters()
        .create("orders & returns", "scope", "input.ok")
        .await
        .unwrap();
    assert_eq!(created.name, OTHER);
    assert_eq!(
        h.client
            .filters()
            .get_record(OTHER)
            .await
            .unwrap()
            .workflow_id,
        ID
    );
    h.client
        .filters()
        .update_options(
            OTHER,
            FilterUpdate {
                payload: Some(serde_json::Map::new()),
                scope: Some("new".into()),
                expression: None,
            },
        )
        .await
        .unwrap();
    let requests = h.requests.lock().await;
    assert_eq!(
        parts(&requests[1]).1,
        json!({"workflowId":ID,"scope":"scope","expression":"input.ok"})
    );
    assert_eq!(parts(&requests[3]).1, json!({"scope":"new","payload":{}}));
    assert!(parts(&requests[3]).0.starts_with(&format!(
        "PATCH /api/v1/stable/tenants/synthetic/filters/{OTHER} "
    )));
}

#[tokio::test]
async fn cel_evaluates_on_the_engine_and_tenant_details_preserve_settings() {
    let h=fixture(vec![reply(json!({"status":"SUCCESS","output":false})),reply(json!({"status":"ERROR","error":"bad expression"})),reply(json!({"metadata":{"id":ID},"name":"Tenant","slug":"tenant","version":"V1","analyticsOptOut":true}))]).await;
    let result = h
        .client
        .cel()
        .debug_with_options(CelDebugOptions {
            expression: "false".into(),
            additional_metadata: Some(serde_json::Map::new()),
            filter_payload: Some(serde_json::Map::new()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(result.output, Some(false));
    assert_eq!(result.status, "SUCCESS");
    assert_eq!(
        h.client
            .cel()
            .debug("bad", &json!({}))
            .await
            .unwrap()
            .error
            .as_deref(),
        Some("bad expression")
    );
    let tenant = h.client.tenant().get().await.unwrap();
    assert_eq!(tenant.settings["analyticsOptOut"], true);
    let requests = h.requests.lock().await;
    assert_eq!(
        parts(&requests[0]).1,
        json!({"expression":"false","input":{},"additionalMetadata":{},"filterPayload":{}})
    );
    assert!(
        parts(&requests[2])
            .0
            .starts_with("GET /api/v1/tenants/synthetic ")
    );
}

#[tokio::test]
async fn inbound_webhooks_have_typed_auth_and_escaped_names_separate_from_workers() {
    let h = fixture(vec![
        reply(webhook()),
        reply(webhook()),
        reply(webhook()),
        reply(json!({"rows":[webhook()]})),
        reply(json!({})),
    ])
    .await;
    let auth = WebhookAuth::ApiKey {
        header_name: "X-Key".into(),
        api_key: "synthetic-secret".into(),
    };
    assert!(!format!("{auth:?}").contains("synthetic-secret"));
    h.client
        .webhooks()
        .create_inbound(WebhookCreate {
            name: "orders/created".into(),
            source_name: WebhookSource::Generic,
            auth,
            options: WebhookUpdate {
                event_key_expression: Some("'order'".into()),
                ..Default::default()
            },
        })
        .await
        .unwrap();
    assert_eq!(
        h.client
            .webhooks()
            .get("orders/created")
            .await
            .unwrap()
            .auth_type,
        "API_KEY"
    );
    h.client
        .webhooks()
        .update(
            "orders/created",
            WebhookUpdate {
                return_event_as_response_payload: Some(false),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(h.client.webhooks().list_inbound().await.unwrap().len(), 1);
    h.client
        .webhooks()
        .delete_inbound("orders/created")
        .await
        .unwrap();
    let requests = h.requests.lock().await;
    assert_eq!(
        parts(&requests[0]).1,
        json!({"name":"orders/created","sourceName":"GENERIC","authType":"API_KEY","auth":{"headerName":"X-Key","apiKey":"synthetic-secret"},"eventKeyExpression":"'order'"})
    );
    assert!(
        parts(&requests[1])
            .0
            .contains("/webhooks/orders%2Fcreated ")
    );
    assert_eq!(
        parts(&requests[2]).1,
        json!({"returnEventAsResponsePayload":false})
    );
}

#[tokio::test]
async fn bulk_schedule_operations_return_partial_success_without_sequential_writes() {
    let h = fixture(vec![
        reply(json!({"updatedIds":[ID],"errors":[{"id":OTHER,"error":"already triggered"}]})),
        reply(json!({"deletedIds":[ID],"errors":[{"id":OTHER,"error":"not found"}]})),
        reply(json!({"metadata":{"id":ID},"triggerAt":"2027-01-01T00:00:00Z"})),
    ])
    .await;
    let result = h
        .client
        .schedules()
        .bulk_update(vec![
            ScheduleUpdate {
                id: ID.into(),
                trigger_at: "2027-01-01T00:00:00Z".into(),
            },
            ScheduleUpdate {
                id: OTHER.into(),
                trigger_at: "2027-01-01T00:00:00Z".into(),
            },
        ])
        .await
        .unwrap();
    assert_eq!(result.errors[0].id.as_deref(), Some(OTHER));
    assert_eq!(result.updated_ids, vec![ID]);
    let result = h
        .client
        .schedules()
        .bulk_delete(vec![ID.into(), OTHER.into()])
        .await
        .unwrap();
    assert_eq!(result.errors.len(), 1);
    h.client
        .schedules()
        .reschedule(ID, "2027-01-01T00:00:00Z")
        .await
        .unwrap();
    let requests = h.requests.lock().await;
    assert_eq!(requests.len(), 3);
    assert!(
        parts(&requests[0])
            .0
            .contains("/workflows/scheduled/bulk-update ")
    );
    assert_eq!(
        parts(&requests[1]).1,
        json!({"scheduledWorkflowRunIds":[ID,OTHER]})
    );
    assert_eq!(
        parts(&requests[2]).1,
        json!({"triggerAt":"2027-01-01T00:00:00Z"})
    );
}

#[tokio::test]
async fn supported_metrics_preserve_cancelled_counts_queue_details_stats_and_text() {
    let mut text = reply(json!({}));
    text.body = "# HELP count demo\ncount 3\n".into();
    let h=fixture(vec![reply(json!([{"status":"CANCELLED","count":3},{"status":"RUNNING","count":4,"runningDetailCount":{"evicted":2}}])),reply(json!({"queues":{"default":7,"limit":{"queued":2}}})),reply(json!({"orders":{"queued":{"total":7,"queues":{"default":7}},"running":{"total":4}}})),text]).await;
    let metrics = h
        .client
        .metrics()
        .get_task_status_metrics(RunListOptions::default())
        .await
        .unwrap();
    assert_eq!(metrics[0].count, 3);
    assert_eq!(metrics[0].status, "CANCELLED");
    assert_eq!(
        h.client
            .metrics()
            .get_task_queue_metrics()
            .await
            .unwrap()
            .queues
            .unwrap()["default"],
        7
    );
    assert_eq!(
        h.client.metrics().get_task_stats().await.unwrap()["orders"]
            .queued
            .as_ref()
            .unwrap()
            .total,
        Some(7)
    );
    assert!(
        h.client
            .metrics()
            .scrape_prometheus_metrics()
            .await
            .unwrap()
            .contains("count 3")
    );
    assert!(matches!(
        h.client.metrics().get_workflow_metrics("orders").await,
        Err(Error::UnsupportedOperation(_))
    ));
    assert!(matches!(
        h.client.metrics().get_queue_metrics().await,
        Err(Error::UnsupportedOperation(_))
    ));
    let requests = h.requests.lock().await;
    assert_eq!(requests.len(), 4);
    for (index, path) in [
        (0, "/task-metrics?since="),
        (1, "/step-run-queue-metrics "),
        (2, "/task-stats "),
        (3, "/prometheus-metrics "),
    ] {
        assert!(parts(&requests[index]).0.contains(path));
    }
}

#[tokio::test]
async fn unknown_and_older_engine_versions_cannot_silently_start_a_worker() {
    for (version, expected) in [
        ("development", "invalid engine version"),
        ("v0.78.22", "SlotConfig requires engine"),
    ] {
        let h = harness_with_version(
            proto::v1::RunStatus::Running,
            false,
            vec![reply(json!({}))],
            retry_config(0),
            version,
        )
        .await;
        let worker = h.client.worker("version-gated").build().start().unwrap();
        let error = worker
            .wait_until_ready(std::time::Duration::from_secs(2))
            .await
            .unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
        let _ = worker.shutdown().await;
        assert!(h.requests.lock().await.is_empty());
    }
}
