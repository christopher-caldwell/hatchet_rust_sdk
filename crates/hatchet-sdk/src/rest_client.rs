//! REST API client for Hatchet management API operations.
//!
//! Used by feature clients to back their methods with real engine state
//! rather than in-memory simulation.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use serde::Deserialize;
use serde_json::Value;

use crate::clients::{
    Cron, DurableBranchResult, FilterDefinition, RestoreTaskResult, RunRecord, RunStatus, Schedule,
    WebhookDefinition, WorkerInfo, WorkflowDefinition,
};
use crate::error::Result;
use crate::retry::{RetryConfig, retry_if};

const RUN_METADATA_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Shared REST client for Hatchet's management API.
///
/// Holds a pre-authenticated `reqwest::Client` and the config needed to build
/// per-tenant URLs. Feature clients call methods on this struct when a real
/// engine is available (i.e., `GrpcTransport` is in use).
#[derive(Clone)]
pub struct RestApiClient {
    client: reqwest::Client,
    base_url: String, // http://...:8888/api/v1
    pub tenant_id: String,
    namespace: Option<String>,
}

pub(crate) struct RunGraph {
    pub output_tasks: Vec<String>,
    pub on_failure_task: Option<String>,
}

impl std::fmt::Debug for RestApiClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RestApiClient")
            .field("base_url", &self.base_url)
            .field("tenant_id", &self.tenant_id)
            .finish()
    }
}

impl RestApiClient {
    /// Creates a new REST API client.
    #[must_use]
    pub fn new(client: reqwest::Client, base_url: String, tenant_id: String) -> Arc<Self> {
        Self::new_with_namespace(client, base_url, tenant_id, None)
    }

    pub(crate) fn new_with_namespace(
        client: reqwest::Client,
        base_url: String,
        tenant_id: String,
        namespace: Option<String>,
    ) -> Arc<Self> {
        Arc::new(Self {
            client,
            base_url,
            tenant_id,
            namespace,
        })
    }

    fn tenant_url(&self, path: &str) -> String {
        format!("{}/tenants/{}{}", self.base_url, self.tenant_id, path)
    }

    fn stable_tenant_url(&self, path: &str) -> String {
        format!(
            "{}/stable/tenants/{}{}",
            self.base_url, self.tenant_id, path
        )
    }

    fn workflow_crons_url(&self, workflow_name: &str) -> String {
        self.tenant_url(&format!(
            "/workflows/{}/crons",
            urlencoding_simple(&crate::config::apply_workflow_namespace(
                workflow_name,
                self.namespace.as_deref()
            ))
        ))
    }

    fn workflow_schedules_url(&self, workflow_name: &str) -> String {
        self.tenant_url(&format!(
            "/workflows/{}/scheduled",
            urlencoding_simple(&crate::config::apply_workflow_namespace(
                workflow_name,
                self.namespace.as_deref()
            ))
        ))
    }

    async fn get_json<T: for<'de> Deserialize<'de>>(&self, url: &str) -> Result<T> {
        let resp = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| crate::error::Error::Transport(format!("REST GET failed: {e}")))?;
        if !resp.status().is_success() {
            return Err(crate::error::Error::Transport(format!(
                "REST GET {url}: HTTP {}",
                resp.status()
            )));
        }
        resp.json::<T>().await.map_err(|e| {
            crate::error::Error::Transport(format!(
                "REST decode failed: {e}; cause: {}",
                std::error::Error::source(&e)
                    .map(ToString::to_string)
                    .unwrap_or_default()
            ))
        })
    }

    async fn post_json<T: for<'de> Deserialize<'de>>(&self, url: &str, body: &Value) -> Result<T> {
        let resp = self
            .client
            .post(url)
            .json(body)
            .send()
            .await
            .map_err(|e| crate::error::Error::Transport(format!("REST POST failed: {e}")))?;
        if !resp.status().is_success() {
            return Err(crate::error::Error::Transport(format!(
                "REST POST {url}: HTTP {}",
                resp.status()
            )));
        }
        resp.json::<T>().await.map_err(|e| {
            crate::error::Error::Transport(format!(
                "REST decode failed: {e}; cause: {}",
                std::error::Error::source(&e)
                    .map(ToString::to_string)
                    .unwrap_or_default()
            ))
        })
    }

    async fn patch_json<T: for<'de> Deserialize<'de>>(&self, url: &str, body: &Value) -> Result<T> {
        let response = self
            .client
            .patch(url)
            .json(body)
            .send()
            .await
            .map_err(|e| crate::Error::Transport(format!("REST PATCH failed: {e}")))?;
        if !response.status().is_success() {
            return Err(crate::Error::Transport(format!(
                "REST PATCH {url}: HTTP {}",
                response.status()
            )));
        }
        response.json().await.map_err(|e| {
            crate::Error::Transport(format!(
                "REST decode failed: {e}; cause: {}",
                std::error::Error::source(&e)
                    .map(ToString::to_string)
                    .unwrap_or_default()
            ))
        })
    }

    /// Triggers a top-level workflow through the current stable API.
    ///
    /// # Errors
    ///
    /// Returns an error for an HTTP failure or a response without a run ID.
    pub async fn trigger_workflow(
        &self,
        workflow_name: &str,
        input: &Value,
        additional_metadata: &std::collections::HashMap<String, String>,
        priority: Option<i32>,
    ) -> Result<String> {
        let url = self.stable_tenant_url("/workflow-runs/trigger");
        let response: Value = self
            .post_json(
                &url,
                &serde_json::json!({
                    "workflowName": workflow_name,
                    "input": input,
                    "additionalMetadata": additional_metadata,
                    "priority": priority,
                    "returnOnlyId": true,
                }),
            )
            .await?;
        response
            .pointer("/run/metadata/id")
            .or_else(|| response.pointer("/metadata/id"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| {
                crate::error::Error::Transport(
                    "REST trigger response did not include a workflow run ID".to_owned(),
                )
            })
    }

    async fn delete_empty(&self, url: &str) -> Result<()> {
        let resp = self
            .client
            .delete(url)
            .send()
            .await
            .map_err(|e| crate::error::Error::Transport(format!("REST DELETE failed: {e}")))?;
        if !resp.status().is_success() {
            return Err(crate::error::Error::Transport(format!(
                "REST DELETE {url}: HTTP {}",
                resp.status()
            )));
        }
        Ok(())
    }

    // --- Runs ---

    pub async fn list_runs(&self) -> Result<Vec<RunRecord>> {
        Ok(self
            .list_run_page(&crate::RunListOptions::default())
            .await?
            .rows)
    }

    pub async fn get_run(&self, run_id: &str) -> Result<RunRecord> {
        let details = self.get_run_details(run_id).await?;
        Ok(details.into_record())
    }

    async fn get_run_details(&self, run_id: &str) -> Result<V1WorkflowRunDetails> {
        let url = format!("{}/stable/workflow-runs/{run_id}", self.base_url);
        self.get_json(&url).await
    }

    async fn get_run_details_for_poll(
        &self,
        run_id: &str,
        retry_config: &RetryConfig,
    ) -> Result<V1WorkflowRunDetails> {
        let url = format!("{}/stable/workflow-runs/{run_id}", self.base_url);
        let read = retry_if(
            || async {
                self.client
                    .get(&url)
                    .timeout(RUN_METADATA_REQUEST_TIMEOUT)
                    .send()
                    .await?
                    .error_for_status()?
                    .json::<V1WorkflowRunDetails>()
                    .await
            },
            retry_config,
            |error: &reqwest::Error| {
                error.is_connect()
                    || error.is_timeout()
                    || error.is_request()
                    || error.is_body()
                    || error.status().is_some_and(|status| {
                        // The OLAP read model can lag a successful trigger.
                        matches!(status.as_u16(), 404 | 408 | 429) || status.is_server_error()
                    })
            },
        );
        let result = if let Some(max_elapsed) = retry_config.max_elapsed {
            tokio::time::timeout(max_elapsed, read).await.map_err(|_| {
                crate::error::Error::Transport(format!(
                    "REST run metadata retry deadline exceeded for '{run_id}'"
                ))
            })?
        } else {
            read.await
        };
        result.map_err(|error| {
            crate::error::Error::Transport(format!(
                "REST GET {url} failed: {error}; cause: {}",
                std::error::Error::source(&error)
                    .map(ToString::to_string)
                    .unwrap_or_default()
            ))
        })
    }

    pub(crate) async fn run_graph(
        &self,
        run_id: &str,
        retry_config: &RetryConfig,
    ) -> Result<RunGraph> {
        let details = self.get_run_details_for_poll(run_id, retry_config).await?;
        let leaves = details.output_tasks();
        if leaves.is_empty() {
            return Err(crate::error::Error::Transport(format!(
                "workflow run '{run_id}' has no output tasks in its engine shape"
            )));
        }
        Ok(RunGraph {
            output_tasks: leaves,
            on_failure_task: details.on_failure_task().map(ToOwned::to_owned),
        })
    }

    pub(crate) async fn completed_main_output(
        &self,
        run_id: &str,
        retry_config: &RetryConfig,
    ) -> Result<Option<Value>> {
        self.get_run_details_for_poll(run_id, retry_config)
            .await?
            .completed_output()
    }

    pub async fn cancel_run(&self, run_id: &str) -> Result<()> {
        self.mutate_runs(
            "cancel",
            &crate::RunSelection::ExternalIds(vec![run_id.to_owned()]),
        )
        .await?;
        Ok(())
    }

    pub async fn replay_run(&self, run_id: &str) -> Result<String> {
        let result = self
            .mutate_runs(
                "replay",
                &crate::RunSelection::ExternalIds(vec![run_id.to_owned()]),
            )
            .await?;
        if result.ids.is_empty() {
            return Err(crate::Error::Transport(
                "engine replay returned no task IDs".to_owned(),
            ));
        }
        // Replay reuses the existing run. Returned IDs identify tasks, not a new workflow.
        Ok(run_id.to_owned())
    }

    pub(crate) async fn mutate_runs(
        &self,
        action: &str,
        selection: &crate::RunSelection,
    ) -> Result<crate::TaskMutationResult> {
        let body = match selection {
            crate::RunSelection::ExternalIds(ids) => {
                if ids.is_empty() {
                    return Err(crate::Error::InputValidation(vec![
                        "at least one external ID is required".to_owned(),
                    ]));
                }
                for id in ids {
                    validate_uuid(id)?;
                }
                serde_json::json!({"externalIds":ids})
            }
            crate::RunSelection::Filter(filter) => {
                serde_json::json!({"filter":self.run_query(filter).await?})
            }
        };
        self.post_json(&self.stable_tenant_url(&format!("/tasks/{action}")), &body)
            .await
    }

    async fn run_query(&self, query: &crate::RunQuery) -> Result<Value> {
        let mut ids = Vec::new();
        for name in &query.workflow_names {
            ids.push(self.workflow_id(name).await?);
        }
        let statuses = query
            .statuses
            .iter()
            .map(status_name)
            .collect::<Result<Vec<_>>>()?;
        Ok(serde_json::json!({
            "since":query.since.clone().unwrap_or_else(|| self.default_since()),
            "until":query.until, "workflowIds":ids, "statuses":statuses,
            "additionalMetadata":query.additional_metadata.iter().map(|(k,v)| format!("{k}:{v}")).collect::<Vec<_>>()
        }))
    }

    pub(crate) async fn list_run_page(
        &self,
        options: &crate::RunListOptions,
    ) -> Result<crate::RunPage> {
        if options.limit == Some(0)
            || options.limit.is_some_and(|v| v > i64::MAX as u64)
            || options.offset > i64::MAX as u64
        {
            return Err(crate::Error::InputValidation(vec![
                "limit must be positive and pagination must fit signed 64-bit integers".to_owned(),
            ]));
        }
        let filter = self.run_query(&options.filter).await?;
        let mut pairs = vec![
            (
                "since",
                filter["since"].as_str().unwrap_or_default().to_owned(),
            ),
            ("offset", options.offset.to_string()),
            ("limit", options.limit.unwrap_or(200).to_string()),
            ("only_tasks", options.only_tasks.to_string()),
        ];
        for (key, value) in [
            ("until", options.filter.until.as_ref()),
            ("worker_id", options.worker_id.as_ref()),
            (
                "parent_task_external_id",
                options.parent_task_external_id.as_ref(),
            ),
            (
                "triggering_event_external_id",
                options.triggering_event_external_id.as_ref(),
            ),
        ] {
            if let Some(value) = value {
                if key != "until" {
                    validate_uuid(value)?;
                }
                pairs.push((key, value.clone()));
            }
        }
        for (key, json_key) in [
            ("statuses", "statuses"),
            ("workflow_ids", "workflowIds"),
            ("additional_metadata", "additionalMetadata"),
        ] {
            if let Some(values) = filter[json_key].as_array() {
                for value in values.iter().filter_map(Value::as_str) {
                    pairs.push((key, value.to_owned()));
                }
            }
        }
        if let Some(include) = options.include_payloads {
            pairs.push(("include_payloads", include.to_string()));
        }
        let qs = pairs
            .into_iter()
            .map(|(k, v)| format!("{k}={}", urlencoding_simple(&v)))
            .collect::<Vec<_>>()
            .join("&");
        let response: V1WorkflowRunList = self
            .get_json(&self.stable_tenant_url(&format!("/workflow-runs?{qs}")))
            .await?;
        Ok(crate::RunPage {
            rows: response.rows.into_iter().map(run_record_from_v1).collect(),
            pagination: response.pagination,
        })
    }

    async fn workflow_id(&self, name: &str) -> Result<String> {
        let row = self.workflow_row(name).await?;
        row["metadata"]["id"]
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| crate::Error::Transport("workflow response omitted its ID".to_owned()))
    }

    async fn workflow_row(&self, name: &str) -> Result<Value> {
        let name = crate::config::apply_workflow_namespace(name, self.namespace.as_deref());
        let response: Value = self
            .get_json(&self.tenant_url(&format!(
                "/workflows?name={}&limit=1",
                urlencoding_simple(&name)
            )))
            .await?;
        response["rows"]
            .as_array()
            .and_then(|rows| {
                rows.iter()
                    .find(|row| row["name"].as_str() == Some(name.as_str()))
            })
            .cloned()
            .ok_or_else(|| crate::Error::Worker(format!("workflow not found: {name}")))
    }

    pub async fn branch_durable_task(
        &self,
        task_external_id: &str,
        node_id: i64,
        branch_id: i64,
    ) -> Result<DurableBranchResult> {
        let task_id = uuid::Uuid::parse_str(task_external_id).map_err(|error| {
            crate::error::Error::InputValidation(vec![format!("invalid task external ID: {error}")])
        })?;
        self.post_json(
            &self.stable_tenant_url("/durable-tasks/branch"),
            &serde_json::json!({"taskExternalId":task_id.to_string(), "nodeId":node_id,"branchId":branch_id}),
        ).await
    }

    pub async fn restore_task(&self, task_external_id: &str) -> Result<RestoreTaskResult> {
        let task_id = uuid::Uuid::parse_str(task_external_id).map_err(|error| {
            crate::error::Error::InputValidation(vec![format!("invalid task external ID: {error}")])
        })?;
        let url = format!("{}/stable/tasks/{task_id}/restore", self.base_url);
        self.post_json(&url, &serde_json::json!({})).await
    }

    pub async fn list_runs_filtered(
        &self,
        workflow_name: Option<&str>,
        status: Option<&str>,
    ) -> Result<Vec<RunRecord>> {
        let statuses = status
            .map(|status| match status {
                "QUEUED" => RunStatus::Queued,
                "RUNNING" => RunStatus::Running,
                "FAILED" => RunStatus::Failed,
                "CANCELLED" => RunStatus::Cancelled,
                "COMPLETED" => RunStatus::Completed,
                _ => RunStatus::Unknown,
            })
            .into_iter()
            .collect();
        Ok(self
            .list_run_page(&crate::RunListOptions {
                filter: crate::RunQuery {
                    workflow_names: workflow_name.map(str::to_owned).into_iter().collect(),
                    statuses,
                    ..Default::default()
                },
                ..Default::default()
            })
            .await?
            .rows)
    }

    // --- Workflows ---

    pub async fn list_workflows(&self) -> Result<Vec<WorkflowDefinition>> {
        let url = self.tenant_url("/workflows?limit=200");
        let resp: V1WorkflowList = self.get_json(&url).await?;
        Ok(resp
            .rows
            .into_iter()
            .map(workflow_definition_from_v1)
            .collect())
    }

    pub async fn get_workflow(&self, name: &str) -> Result<WorkflowDefinition> {
        let row = self.workflow_row(name).await?;
        let dto = serde_json::from_value(row)
            .map_err(|error| crate::Error::Transport(error.to_string()))?;
        Ok(workflow_definition_from_v1(dto))
    }

    pub(crate) async fn delete_workflow(&self, name: &str) -> Result<()> {
        let id = self.workflow_id(name).await?;
        self.delete_empty(&format!(
            "{}/workflows/{}",
            self.base_url,
            urlencoding_simple(&id)
        ))
        .await
    }

    pub(crate) async fn pause_workflow(
        &self,
        name: &str,
        options: Option<&crate::WorkflowPauseOptions>,
    ) -> Result<()> {
        let row = self.workflow_row(name).await?;
        let id = row["metadata"]["id"]
            .as_str()
            .ok_or_else(|| crate::Error::Transport("workflow response omitted ID".to_owned()))?;
        let mut pause = match options {
            Some(options) => {
                serde_json::to_value(options).map_err(|e| crate::Error::Transport(e.to_string()))?
            }
            None => serde_json::json!({}),
        };
        pause["action"] = serde_json::json!(if options.is_some() {
            "pause"
        } else {
            "unpause"
        });
        let response: Value = self
            .patch_json(
                &format!("{}/workflows/{}", self.base_url, urlencoding_simple(id)),
                &serde_json::json!({"pause":pause}),
            )
            .await?;
        if response["isPaused"].as_bool() != Some(options.is_some()) {
            return Err(crate::Error::Transport(
                "engine did not confirm requested workflow pause state".to_owned(),
            ));
        }
        Ok(())
    }

    // --- Workers ---

    pub async fn list_workers(&self) -> Result<Vec<WorkerInfo>> {
        let url = self.tenant_url("/worker?limit=200");
        let resp: V1WorkerList = self.get_json(&url).await?;
        Ok(resp.rows.into_iter().map(worker_info_from_v1).collect())
    }

    pub async fn get_worker(&self, worker_id: &str) -> Result<WorkerInfo> {
        let url = format!("{}/workers/{worker_id}", self.base_url);
        let row: V1Worker = self.get_json(&url).await?;
        Ok(worker_info_from_v1(row))
    }

    pub async fn pause_worker(&self, worker_id: &str) -> Result<()> {
        let url = format!("{}/workers/{worker_id}", self.base_url);
        let body = serde_json::json!({ "isPaused": true });
        let resp = self
            .client
            .patch(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| crate::error::Error::Transport(format!("REST PATCH failed: {e}")))?;
        if !resp.status().is_success() {
            return Err(crate::error::Error::Transport(format!(
                "REST PATCH {url}: HTTP {}",
                resp.status()
            )));
        }
        Ok(())
    }

    pub async fn unpause_worker(&self, worker_id: &str) -> Result<()> {
        let url = format!("{}/workers/{worker_id}", self.base_url);
        let body = serde_json::json!({ "isPaused": false });
        let resp = self
            .client
            .patch(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| crate::error::Error::Transport(format!("REST PATCH failed: {e}")))?;
        if !resp.status().is_success() {
            return Err(crate::error::Error::Transport(format!(
                "REST PATCH {url}: HTTP {}",
                resp.status()
            )));
        }
        Ok(())
    }

    // --- Crons ---

    pub async fn list_crons(&self) -> Result<Vec<Cron>> {
        let url = self.tenant_url("/workflows/crons?limit=200");
        let resp: V1CronList = self.get_json(&url).await?;
        Ok(resp.rows.into_iter().map(cron_from_v1).collect())
    }

    pub async fn get_cron(&self, cron_id: &str) -> Result<Cron> {
        let url = self.tenant_url(&format!("/workflows/crons/{cron_id}"));
        let row: V1Cron = self.get_json(&url).await?;
        Ok(cron_from_v1(row))
    }

    pub async fn delete_cron(&self, cron_id: &str) -> Result<()> {
        let url = self.tenant_url(&format!("/workflows/crons/{cron_id}"));
        self.delete_empty(&url).await
    }

    // --- Schedules ---

    pub async fn list_schedules(&self) -> Result<Vec<Schedule>> {
        let url = self.tenant_url("/workflows/scheduled?limit=200");
        let resp: V1ScheduleList = self.get_json(&url).await?;
        Ok(resp.rows.into_iter().map(schedule_from_v1).collect())
    }

    pub async fn get_schedule(&self, schedule_id: &str) -> Result<Schedule> {
        let url = self.tenant_url(&format!("/workflows/scheduled/{schedule_id}"));
        let row: V1Schedule = self.get_json(&url).await?;
        Ok(schedule_from_v1(row))
    }

    pub async fn delete_schedule(&self, schedule_id: &str) -> Result<()> {
        let url = self.tenant_url(&format!("/workflows/scheduled/{schedule_id}"));
        self.delete_empty(&url).await
    }

    // --- Filters ---

    pub async fn list_filters(&self) -> Result<Vec<FilterDefinition>> {
        let url = self.stable_tenant_url("/filters?limit=200");
        let resp: V1FilterList = self.get_json(&url).await?;
        Ok(resp.rows.into_iter().map(filter_from_v1).collect())
    }

    pub async fn delete_filter(&self, filter_id: &str) -> Result<()> {
        let url = self.stable_tenant_url(&format!("/filters/{filter_id}"));
        self.delete_empty(&url).await
    }

    pub(crate) async fn get_filter(&self, id: &str) -> Result<crate::FilterRecord> {
        validate_uuid(id)?;
        self.get_json(&self.stable_tenant_url(&format!("/filters/{id}")))
            .await
    }

    pub(crate) async fn update_filter(
        &self,
        id: &str,
        update: &crate::FilterUpdate,
    ) -> Result<crate::FilterRecord> {
        validate_uuid(id)?;
        self.patch_json(
            &self.stable_tenant_url(&format!("/filters/{id}")),
            &serde_json::to_value(update).map_err(|e| crate::Error::Transport(e.to_string()))?,
        )
        .await
    }

    pub(crate) async fn debug_cel(
        &self,
        options: &crate::CelDebugOptions,
    ) -> Result<crate::CelEvaluation> {
        self.post_json(
            &self.stable_tenant_url("/cel/debug"),
            &serde_json::to_value(options).map_err(|e| crate::Error::Transport(e.to_string()))?,
        )
        .await
    }

    pub(crate) async fn get_tenant(&self) -> Result<crate::TenantDetails> {
        self.get_json(&self.tenant_url("")).await
    }

    pub(crate) async fn reschedule(&self, id: &str, trigger_at: &str) -> Result<Schedule> {
        validate_uuid(id)?;
        let response: V1Schedule = self
            .patch_json(
                &self.tenant_url(&format!("/workflows/scheduled/{id}")),
                &serde_json::json!({"triggerAt":trigger_at}),
            )
            .await?;
        Ok(schedule_from_v1(response))
    }

    pub(crate) async fn bulk_reschedule(
        &self,
        updates: &[crate::ScheduleUpdate],
    ) -> Result<crate::ScheduleBulkUpdateResult> {
        for update in updates {
            validate_uuid(&update.id)?;
        }
        self.post_json(
            &self.tenant_url("/workflows/scheduled/bulk-update"),
            &serde_json::json!({"updates":updates}),
        )
        .await
    }

    pub(crate) async fn bulk_delete_schedules(
        &self,
        ids: &[String],
    ) -> Result<crate::ScheduleBulkDeleteResult> {
        for id in ids {
            validate_uuid(id)?;
        }
        self.post_json(
            &self.tenant_url("/workflows/scheduled/bulk-delete"),
            &serde_json::json!({"scheduledWorkflowRunIds":ids}),
        )
        .await
    }

    // --- Webhooks ---

    pub async fn list_webhooks(&self) -> Result<Vec<WebhookDefinition>> {
        let url = self.tenant_url("/webhook-workers?limit=200");
        let resp: V1WebhookList = self.get_json(&url).await?;
        Ok(resp.rows.into_iter().map(webhook_from_v1).collect())
    }

    pub async fn delete_webhook(&self, webhook_id: &str) -> Result<()> {
        let url = self.tenant_url(&format!("/webhook-workers/{webhook_id}"));
        self.delete_empty(&url).await
    }

    pub async fn create_webhook(&self, name: &str, url_target: &str) -> Result<WebhookDefinition> {
        let url = self.tenant_url("/webhook-workers");
        let body = serde_json::json!({ "name": name, "url": url_target });
        let resp: V1Webhook = self.post_json(&url, &body).await?;
        Ok(webhook_from_v1(resp))
    }

    pub(crate) async fn create_inbound_webhook(
        &self,
        options: &crate::WebhookCreate,
    ) -> Result<crate::InboundWebhook> {
        if options.options.event_key_expression.is_none() {
            return Err(crate::Error::InputValidation(vec![
                "event_key_expression is required".to_owned(),
            ]));
        }
        self.post_json(
            &self.stable_tenant_url("/webhooks"),
            &serde_json::to_value(options).map_err(|e| crate::Error::Transport(e.to_string()))?,
        )
        .await
    }
    pub(crate) async fn get_inbound_webhook(&self, name: &str) -> Result<crate::InboundWebhook> {
        self.get_json(&self.stable_tenant_url(&format!("/webhooks/{}", urlencoding_simple(name))))
            .await
    }
    pub(crate) async fn update_inbound_webhook(
        &self,
        name: &str,
        update: &crate::WebhookUpdate,
    ) -> Result<crate::InboundWebhook> {
        self.patch_json(
            &self.stable_tenant_url(&format!("/webhooks/{}", urlencoding_simple(name))),
            &serde_json::to_value(update).map_err(|e| crate::Error::Transport(e.to_string()))?,
        )
        .await
    }
    pub(crate) async fn list_inbound_webhooks(&self) -> Result<Vec<crate::InboundWebhook>> {
        #[derive(Deserialize)]
        struct Rows {
            rows: Vec<crate::InboundWebhook>,
        }
        let response: Rows = self
            .get_json(&self.stable_tenant_url("/webhooks?limit=200"))
            .await?;
        Ok(response.rows)
    }
    pub(crate) async fn delete_inbound_webhook(&self, name: &str) -> Result<()> {
        self.delete_empty(
            &self.stable_tenant_url(&format!("/webhooks/{}", urlencoding_simple(name))),
        )
        .await
    }

    // --- Crons (create) ---

    pub async fn create_cron(
        &self,
        workflow_name: &str,
        cron_name: &str,
        expression: &str,
        input: &Value,
    ) -> Result<Cron> {
        let url = self.workflow_crons_url(workflow_name);
        let body = serde_json::json!({
            "cronName": cron_name,
            "cronExpression": expression,
            "input": input,
            "additionalMetadata": {},
        });
        let resp: V1Cron = self.post_json(&url, &body).await?;
        Ok(cron_from_v1(resp))
    }

    // --- Schedules (create) ---

    pub async fn create_schedule(
        &self,
        workflow_name: &str,
        schedule_name: &str,
        trigger_at: &str,
        input: &Value,
    ) -> Result<Schedule> {
        if !input.is_object() {
            return Err(crate::error::Error::InputValidation(vec![
                "Hatchet schedule input must be a JSON object".to_owned(),
            ]));
        }
        let url = self.workflow_schedules_url(workflow_name);
        let body = serde_json::json!({
            "triggerAt": trigger_at,
            "input": input,
            "additionalMetadata": { "scheduleName": schedule_name },
        });
        let resp: V1Schedule = self.post_json(&url, &body).await?;
        Ok(schedule_from_v1(resp))
    }

    // --- Filters (create) ---

    pub async fn create_filter(
        &self,
        workflow_name: &str,
        filter_name: &str,
        expression: &str,
    ) -> Result<FilterDefinition> {
        let url = self.stable_tenant_url("/filters");
        let body = serde_json::json!({
            "workflowId": self.workflow_id(workflow_name).await?,
            "scope": filter_name,
            "expression": expression,
        });
        let resp: V1Filter = self.post_json(&url, &body).await?;
        Ok(filter_from_v1(resp))
    }

    // --- Metrics ---

    pub(crate) async fn task_status_metrics(
        &self,
        options: &crate::RunListOptions,
    ) -> Result<Vec<crate::TaskStatusMetric>> {
        if !options.filter.statuses.is_empty()
            || options.worker_id.is_some()
            || options.only_tasks
            || options.include_payloads.is_some()
            || options.offset != 0
            || options.limit.is_some()
        {
            return Err(crate::Error::InputValidation(vec!["task status metrics accept time, workflow, metadata, parent and event filters only".to_owned()]));
        }
        let filter = self.run_query(&options.filter).await?;
        let mut pairs = vec![(
            "since",
            filter["since"].as_str().unwrap_or_default().to_owned(),
        )];
        for (key, value) in [
            ("until", options.filter.until.as_ref()),
            (
                "parent_task_external_id",
                options.parent_task_external_id.as_ref(),
            ),
            (
                "triggering_event_external_id",
                options.triggering_event_external_id.as_ref(),
            ),
        ] {
            if let Some(value) = value {
                if key != "until" {
                    validate_uuid(value)?;
                }
                pairs.push((key, value.clone()));
            }
        }
        for (key, json_key) in [
            ("workflow_ids", "workflowIds"),
            ("additional_metadata", "additionalMetadata"),
        ] {
            if let Some(values) = filter[json_key].as_array() {
                for value in values.iter().filter_map(Value::as_str) {
                    pairs.push((key, value.to_owned()));
                }
            }
        }
        let qs = pairs
            .into_iter()
            .map(|(k, v)| format!("{k}={}", urlencoding_simple(&v)))
            .collect::<Vec<_>>()
            .join("&");
        self.get_json(&self.stable_tenant_url(&format!("/task-metrics?{qs}")))
            .await
    }

    pub(crate) async fn task_queue_metrics(&self) -> Result<crate::TaskQueueMetrics> {
        self.get_json(&self.tenant_url("/step-run-queue-metrics"))
            .await
    }

    pub(crate) async fn task_stats(&self) -> Result<HashMap<String, crate::TaskStat>> {
        self.get_json(&self.tenant_url("/task-stats")).await
    }

    pub(crate) async fn scrape_prometheus(&self) -> Result<String> {
        let response = self
            .client
            .get(self.tenant_url("/prometheus-metrics"))
            .send()
            .await
            .map_err(|e| crate::Error::Transport(format!("REST metrics failed: {e}")))?;
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(crate::Error::Transport(format!(
                "REST metrics HTTP {status}: {body}"
            )));
        }
        response
            .text()
            .await
            .map_err(|e| crate::Error::Transport(format!("REST metrics body failed: {e}")))
    }

    // --- Rate limits ---

    pub async fn list_rate_limits(&self) -> Result<HashMap<String, u32>> {
        let url = self.tenant_url("/rate-limits?limit=200");
        let resp: V1RateLimitList = self.get_json(&url).await?;
        Ok(resp
            .rows
            .into_iter()
            .filter_map(|row| {
                let key = row.key?;
                let limit = row.limit_value.unwrap_or_default();
                Some((key, u32::try_from(limit).unwrap_or_default()))
            })
            .collect())
    }

    pub async fn delete_rate_limit(&self, key: &str) -> Result<()> {
        let url = self.tenant_url(&format!("/rate-limits?key={}", urlencoding_simple(key)));
        self.delete_empty(&url).await
    }

    fn default_since(&self) -> String {
        let hour_ago = SystemTime::now() - Duration::from_secs(3600);
        system_time_to_iso(&hour_ago)
    }
}

// --- REST response types ---

#[derive(Debug, Deserialize)]
struct V1WorkflowRunList {
    pagination: Option<crate::Pagination>,
    #[serde(default)]
    rows: Vec<V1WorkflowRun>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct V1WorkflowRun {
    #[serde(default)]
    metadata: Option<V1Metadata>,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    input: Option<Value>,
    #[serde(flatten)]
    summary: crate::RunSummary,
}

#[derive(Debug, Deserialize)]
struct V1WorkflowRunDetails {
    run: V1WorkflowRun,
    shape: Vec<V1WorkflowRunShape>,
    #[serde(default, rename = "workflowConfig")]
    workflow_config: Option<Value>,
    #[serde(default)]
    tasks: Vec<V1RunTask>,
    #[serde(default, rename = "taskEvents")]
    task_events: Vec<V1RunTaskEvent>,
}

impl V1WorkflowRunDetails {
    fn into_record(self) -> RunRecord {
        let mut record = run_record_from_v1(self.run);
        record.external_task_ids = self
            .shape
            .into_iter()
            .filter(|task| task.has_run())
            .map(|task| (task.task_name, task.task_external_id))
            .collect();
        record
    }

    fn output_tasks(&self) -> Vec<String> {
        let on_failure = self.on_failure_task();
        let mut leaves: Vec<_> = self
            .shape
            .iter()
            .filter(|task| {
                task.children_step_ids.is_empty() && Some(task.task_name.as_str()) != on_failure
            })
            .map(|task| task.task_name.clone())
            .collect();
        leaves.sort();
        leaves.dedup();
        leaves
    }

    fn on_failure_task(&self) -> Option<&str> {
        let config = self.workflow_config.as_ref()?;
        let task = config
            .get("onFailureJob")
            .or_else(|| config.get("on_failure_task"))
            .or_else(|| config.get("onFailureTask"))?;
        task.get("ReadableId")
            .or_else(|| task.get("readable_id"))
            .or_else(|| task.get("readableId"))?
            .as_str()
    }

    fn task_names(&self) -> Vec<String> {
        let failure = self.on_failure_task();
        self.shape
            .iter()
            .filter(|task| Some(task.task_name.as_str()) != failure)
            .map(|task| task.task_name.clone())
            .collect()
    }

    fn main_tasks_completed(&self) -> bool {
        let names = self.task_names();
        !names.is_empty()
            && names.iter().all(|name| {
                self.shape
                    .iter()
                    .find(|task| &task.task_name == name)
                    .is_some_and(|shape| {
                        self.tasks.iter().any(|task| {
                            task.task_external_id == shape.task_external_id
                                && task.status == "COMPLETED"
                        })
                    })
            })
    }

    fn completed_output(&self) -> Result<Option<Value>> {
        if !self.main_tasks_completed() {
            return Ok(None);
        }
        let mut outputs = BTreeMap::new();
        for name in self.output_tasks() {
            let Some(shape) = self.shape.iter().find(|task| task.task_name == name) else {
                return Ok(None);
            };
            // Task summaries coerce scalar outputs to objects in v0.94.14.
            // Completion events retain the original JSON, including scalars.
            let Some(payload) = self
                .task_events
                .iter()
                .filter(|event| {
                    event.task_id == shape.task_external_id && event.event_type == "FINISHED"
                })
                .max_by_key(|event| event.retry_count)
                .and_then(|event| event.output.as_deref())
            else {
                return Ok(None);
            };
            let value = serde_json::from_str(payload).map_err(|error| {
                crate::error::Error::Transport(format!("invalid output for task '{name}': {error}"))
            })?;
            outputs.insert(name, value);
        }
        Ok(match outputs.len() {
            0 => None,
            1 => outputs.into_values().next(),
            _ => Some(Value::Object(outputs.into_iter().collect())),
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct V1RunTask {
    task_external_id: String,
    status: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct V1RunTaskEvent {
    task_id: String,
    event_type: String,
    retry_count: Option<u32>,
    output: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct V1WorkflowRunShape {
    task_name: String,
    task_external_id: String,
    children_step_ids: Vec<String>,
}

impl V1WorkflowRunShape {
    fn has_run(&self) -> bool {
        !self.task_external_id.is_empty()
            && self.task_external_id != "00000000-0000-0000-0000-000000000000"
    }
}

#[derive(Debug, Deserialize)]
struct V1WorkflowList {
    #[serde(default)]
    rows: Vec<V1Workflow>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct V1Workflow {
    #[serde(default)]
    metadata: Option<V1Metadata>,
    #[serde(default)]
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct V1WorkerList {
    #[serde(default)]
    rows: Vec<V1Worker>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct V1Worker {
    #[serde(default)]
    metadata: Option<V1Metadata>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    is_paused: Option<bool>,
    #[serde(default)]
    labels: Option<Vec<V1WorkerLabel>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct V1WorkerLabel {
    key: String,
    #[serde(default)]
    str_value: Option<String>,
    #[serde(default)]
    int_value: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct V1CronList {
    #[serde(default)]
    rows: Vec<V1Cron>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct V1Cron {
    #[serde(default)]
    metadata: Option<V1Metadata>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    workflow_name: Option<String>,
    #[serde(default)]
    cron_expression: Option<String>,
}

#[derive(Debug, Deserialize)]
struct V1ScheduleList {
    #[serde(default)]
    rows: Vec<V1Schedule>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct V1Schedule {
    #[serde(default)]
    metadata: Option<V1Metadata>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    workflow_name: Option<String>,
    #[serde(default)]
    trigger_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct V1FilterList {
    #[serde(default)]
    rows: Vec<V1Filter>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct V1Filter {
    #[serde(default)]
    metadata: Option<V1Metadata>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    expression: Option<String>,
}

#[derive(Debug, Deserialize)]
struct V1WebhookList {
    #[serde(default)]
    rows: Vec<V1Webhook>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct V1Webhook {
    #[serde(default)]
    metadata: Option<V1Metadata>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    is_active: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct V1RateLimitList {
    #[serde(default)]
    rows: Vec<V1RateLimit>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct V1RateLimit {
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    limit_value: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct V1Metadata {
    id: String,
}

// --- Conversion helpers ---

fn run_record_from_v1(row: V1WorkflowRun) -> RunRecord {
    use another_hatchet_core::{RunOpts, WorkflowRunId};
    use std::collections::HashMap;
    let id = row
        .metadata
        .map(|m| m.id)
        .unwrap_or_else(|| "unknown".to_owned());
    let status = match row.status.as_deref() {
        Some("PENDING" | "QUEUED") => RunStatus::Queued,
        Some("RUNNING") => RunStatus::Running,
        Some("FAILED") => RunStatus::Failed,
        Some("CANCELLED" | "CANCELED") => RunStatus::Cancelled,
        Some("SUCCEEDED") | Some("COMPLETED") => RunStatus::Completed,
        _ => RunStatus::Unknown,
    };
    RunRecord {
        run_id: WorkflowRunId(id),
        workflow_name: row.display_name.unwrap_or_default(),
        input: row.input.unwrap_or(Value::Null),
        opts: RunOpts {
            additional_metadata: row
                .summary
                .additional_metadata
                .as_ref()
                .map(|m| {
                    m.iter()
                        .filter_map(|(k, v)| v.as_str().map(|v| (k.clone(), v.to_owned())))
                        .collect()
                })
                .unwrap_or_default(),
            ..RunOpts::default()
        },
        status,
        external_task_ids: HashMap::new(),
        summary: Some(row.summary),
    }
}

fn workflow_definition_from_v1(row: V1Workflow) -> WorkflowDefinition {
    WorkflowDefinition {
        name: row.name.unwrap_or_else(|| {
            row.metadata
                .map(|m| m.id)
                .unwrap_or_else(|| "unknown".to_owned())
        }),
        version: 1,
        definition: Value::Null,
    }
}

fn worker_info_from_v1(row: V1Worker) -> WorkerInfo {
    let labels = row
        .labels
        .unwrap_or_default()
        .into_iter()
        .map(|l| {
            let value = l
                .str_value
                .unwrap_or_else(|| l.int_value.map(|i| i.to_string()).unwrap_or_default());
            (l.key, value)
        })
        .collect::<BTreeMap<_, _>>();
    WorkerInfo {
        name: row.name.unwrap_or_else(|| {
            row.metadata
                .map(|m| m.id)
                .unwrap_or_else(|| "unknown".to_owned())
        }),
        paused: row.is_paused.unwrap_or(false),
        labels,
    }
}

fn cron_from_v1(row: V1Cron) -> Cron {
    Cron {
        name: row.name.unwrap_or_else(|| {
            row.metadata
                .map(|m| m.id)
                .unwrap_or_else(|| "unknown".to_owned())
        }),
        workflow_name: row.workflow_name.unwrap_or_default(),
        expression: row.cron_expression.unwrap_or_default(),
    }
}

fn schedule_from_v1(row: V1Schedule) -> Schedule {
    Schedule {
        name: row.name.unwrap_or_else(|| {
            row.metadata
                .map(|m| m.id)
                .unwrap_or_else(|| "unknown".to_owned())
        }),
        workflow_name: row.workflow_name.unwrap_or_default(),
        expression: row.trigger_at.unwrap_or_default(),
        input: Value::Null,
    }
}

fn filter_from_v1(row: V1Filter) -> FilterDefinition {
    FilterDefinition {
        name: row.name.unwrap_or_else(|| {
            row.metadata
                .map(|m| m.id)
                .unwrap_or_else(|| "unknown".to_owned())
        }),
        expression: row.expression.unwrap_or_default(),
    }
}

fn webhook_from_v1(row: V1Webhook) -> WebhookDefinition {
    WebhookDefinition {
        name: row.name.unwrap_or_else(|| {
            row.metadata
                .map(|m| m.id)
                .unwrap_or_else(|| "unknown".to_owned())
        }),
        url: row.url.unwrap_or_default(),
        enabled: row.is_active.unwrap_or(false),
    }
}

fn urlencoding_simple(s: &str) -> String {
    s.bytes()
        .flat_map(|byte| {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
                vec![char::from(byte)]
            } else {
                format!("%{byte:02X}").chars().collect()
            }
        })
        .collect()
}

/// Converts a `SystemTime` to an ISO 8601 timestamp string.
pub(crate) fn system_time_to_iso(time: &SystemTime) -> String {
    let dur = time
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = dur.as_secs();
    let nanos = dur.subsec_nanos();

    // Simple ISO 8601 formatting without external deps
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Calculate date from days since epoch (simplified — correct for 1970-2099)
    let (year, month, day) = days_to_ymd(days_since_epoch);

    format!(
        "{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}.{millis:03}Z",
        millis = nanos / 1_000_000,
    )
}

/// Converts days since Unix epoch to (year, month, day).
fn days_to_ymd(days: u64) -> (u64, u64, u64) {
    // Algorithm from Howard Hinnant's `chrono`-compatible date calculation
    let z = days + 719468;
    let era = z / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

fn validate_uuid(id: &str) -> Result<()> {
    uuid::Uuid::parse_str(id).map(|_| ()).map_err(|error| {
        crate::Error::InputValidation(vec![format!("invalid external ID: {error}")])
    })
}

fn status_name(status: &RunStatus) -> Result<&'static str> {
    match status {
        RunStatus::Queued => Ok("QUEUED"),
        RunStatus::Running => Ok("RUNNING"),
        RunStatus::Failed => Ok("FAILED"),
        RunStatus::Cancelled => Ok("CANCELLED"),
        RunStatus::Completed => Ok("COMPLETED"),
        RunStatus::Unknown => Err(crate::Error::InputValidation(vec![
            "cannot select an unknown engine status".to_owned(),
        ])),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn engine_statuses_preserve_running_failed_and_unknown_states() {
        for (status, expected) in [
            ("QUEUED", RunStatus::Queued),
            ("PENDING", RunStatus::Queued),
            ("RUNNING", RunStatus::Running),
            ("FAILED", RunStatus::Failed),
            ("CANCELLED", RunStatus::Cancelled),
            ("COMPLETED", RunStatus::Completed),
            ("SUCCEEDED", RunStatus::Completed),
            ("NEW_ENGINE_STATE", RunStatus::Unknown),
        ] {
            let row: V1WorkflowRun = serde_json::from_value(serde_json::json!({
                "metadata": { "id": "run-1" }, "status": status
            }))
            .unwrap();
            assert_eq!(run_record_from_v1(row).status, expected);
        }
    }

    #[test]
    fn stable_run_details_decode_the_nested_run_and_graph_shape() {
        let details: V1WorkflowRunDetails = serde_json::from_value(serde_json::json!({
            "run": { "metadata": { "id": "run-1" }, "status": "FAILED" },
            "workflowConfig": {"onFailureJob":{"ReadableId":"on-failure"}},
            "shape": [
                {"taskName":"first", "taskExternalId":"task-1", "childrenStepIds":["step-2"]},
                {"taskName":"last", "taskExternalId":"task-2", "childrenStepIds":[]},
                {"taskName":"on-failure", "taskExternalId":"failure-task", "childrenStepIds":[]},
                {"taskName":"not-started", "taskExternalId":"00000000-0000-0000-0000-000000000000", "childrenStepIds":[]}
            ]
        }))
        .unwrap();
        assert_eq!(details.output_tasks(), ["last", "not-started"]);
        let record = details.into_record();
        assert_eq!(record.status, RunStatus::Failed);
        assert_eq!(record.run_id.0, "run-1");
        assert_eq!(record.external_task_ids.len(), 3);
        assert_eq!(record.external_task_ids["last"], "task-2");
    }

    #[test]
    fn completed_graph_with_unused_failure_handler_preserves_scalar_output() {
        let fixture = serde_json::json!({
            "run": {"metadata":{"id":"run-1"}, "status":"RUNNING"},
            "workflowConfig":{"onFailureJob":{"ReadableId":"failure"}},
            "shape":[
                {"taskName":"first", "taskExternalId":"task-1", "childrenStepIds":["step-2"]},
                {"taskName":"last", "taskExternalId":"task-2", "childrenStepIds":[]},
                {"taskName":"failure", "taskExternalId":"00000000-0000-0000-0000-000000000000", "childrenStepIds":[]}
            ],
            "tasks":[
                {"taskExternalId":"task-1", "status":"COMPLETED"},
                {"taskExternalId":"task-2", "status":"COMPLETED", "output":{}}
            ],
            "taskEvents":[
                {"taskId":"task-2", "eventType":"FINISHED", "retryCount":0, "output":"\"finished\""}
            ]
        });
        let mut details: V1WorkflowRunDetails = serde_json::from_value(fixture.clone()).unwrap();
        assert_eq!(
            details.completed_output().unwrap(),
            Some(serde_json::json!("finished"))
        );
        details.tasks.pop();
        assert_eq!(details.completed_output().unwrap(), None);
        assert_eq!(details.into_record().status, RunStatus::Running);
        let details: V1WorkflowRunDetails = serde_json::from_value(fixture).unwrap();
        // Status inspection must still reflect the engine's reported state.
        assert_eq!(details.into_record().status, RunStatus::Running);
    }

    #[test]
    fn workflow_creation_routes_include_the_workflow_path_segment() {
        let api = RestApiClient {
            client: reqwest::Client::new(),
            base_url: "https://hatchet.example/api/v1".to_owned(),
            tenant_id: "tenant-1".to_owned(),
            namespace: None,
        };

        assert_eq!(
            api.workflow_crons_url("billing/nightly"),
            "https://hatchet.example/api/v1/tenants/tenant-1/workflows/billing%2Fnightly/crons"
        );
        assert_eq!(
            api.workflow_schedules_url("café"),
            "https://hatchet.example/api/v1/tenants/tenant-1/workflows/caf%C3%A9/scheduled"
        );
    }
}
