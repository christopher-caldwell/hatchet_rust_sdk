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
    Cron, FilterDefinition, RunRecord, RunStatus, Schedule, WebhookDefinition, WorkerInfo,
    WorkflowDefinition,
};
use crate::error::Result;

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
        Arc::new(Self {
            client,
            base_url,
            tenant_id,
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
            urlencoding_simple(workflow_name)
        ))
    }

    fn workflow_schedules_url(&self, workflow_name: &str) -> String {
        self.tenant_url(&format!(
            "/workflows/{}/scheduled",
            urlencoding_simple(workflow_name)
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
        resp.json::<T>()
            .await
            .map_err(|e| crate::error::Error::Transport(format!("REST decode failed: {e}")))
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
        resp.json::<T>()
            .await
            .map_err(|e| crate::error::Error::Transport(format!("REST decode failed: {e}")))
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
        let since = self.default_since();
        let url = self.stable_tenant_url(&format!(
            "/workflow-runs?limit=200&only_tasks=false&since={since}"
        ));
        let resp: V1WorkflowRunList = self.get_json(&url).await?;
        Ok(resp.rows.into_iter().map(run_record_from_v1).collect())
    }

    pub async fn get_run(&self, run_id: &str) -> Result<RunRecord> {
        let url = format!("{}/stable/workflow-runs/{run_id}", self.base_url);
        let row: V1WorkflowRun = self.get_json(&url).await?;
        Ok(run_record_from_v1(row))
    }

    pub async fn cancel_run(&self, run_id: &str) -> Result<()> {
        let url = self.stable_tenant_url("/tasks/cancel");
        let body = serde_json::json!({ "externalIds": [run_id] });
        let _resp: Value = self.post_json(&url, &body).await?;
        Ok(())
    }

    pub async fn replay_run(&self, run_id: &str) -> Result<String> {
        let url = self.stable_tenant_url("/tasks/replay");
        let body = serde_json::json!({ "externalIds": [run_id] });
        let resp: V1WorkflowRunReplayResponse = self.post_json(&url, &body).await?;
        Ok(resp.workflow_run_ids.first().cloned().unwrap_or_default())
    }

    pub async fn list_runs_filtered(
        &self,
        workflow_name: Option<&str>,
        status: Option<&str>,
    ) -> Result<Vec<RunRecord>> {
        let since = self.default_since();
        let mut qs = format!("limit=200&only_tasks=false&since={since}");
        if let Some(name) = workflow_name {
            qs.push_str(&format!("&workflowName={}", urlencoding_simple(name)));
        }
        if let Some(s) = status {
            qs.push_str(&format!("&statuses={s}"));
        }
        let url = self.stable_tenant_url(&format!("/workflow-runs?{qs}"));
        let resp: V1WorkflowRunList = self.get_json(&url).await?;
        Ok(resp.rows.into_iter().map(run_record_from_v1).collect())
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
        self.list_workflows()
            .await?
            .into_iter()
            .find(|workflow| workflow.name == name)
            .ok_or_else(|| crate::error::Error::Worker(format!("workflow not found: {name}")))
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
            "workflowName": workflow_name,
            "name": filter_name,
            "expression": expression,
        });
        let resp: V1Filter = self.post_json(&url, &body).await?;
        Ok(filter_from_v1(resp))
    }

    // --- Metrics ---

    pub async fn get_workflow_metrics(&self, workflow_name: &str) -> Result<V1WorkflowMetrics> {
        let url = self.tenant_url(&format!(
            "/workflows/metrics?workflowName={}",
            urlencoding_simple(workflow_name)
        ));
        self.get_json(&url).await
    }

    pub async fn get_queue_metrics(&self) -> Result<V1QueueMetrics> {
        let url = self.tenant_url("/queue-metrics");
        self.get_json(&url).await
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
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct V1WorkflowRunReplayResponse {
    #[serde(default)]
    workflow_run_ids: Vec<String>,
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
        Some("CANCELLED") => RunStatus::Cancelled,
        Some("SUCCEEDED") | Some("COMPLETED") => RunStatus::Completed,
        _ => RunStatus::Queued,
    };
    RunRecord {
        run_id: WorkflowRunId(id),
        workflow_name: row.display_name.unwrap_or_default(),
        input: row.input.unwrap_or(Value::Null),
        opts: RunOpts::default(),
        status,
        external_task_ids: HashMap::new(),
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct V1WorkflowMetrics {
    #[serde(default)]
    pub total_runs: Option<i64>,
    #[serde(default)]
    pub queued_runs: Option<i64>,
    #[serde(default)]
    pub failed_runs: Option<i64>,
    #[serde(default)]
    pub succeeded_runs: Option<i64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct V1QueueMetrics {
    #[serde(default)]
    pub total_runs: Option<i64>,
    #[serde(default)]
    pub queued_runs: Option<i64>,
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
fn system_time_to_iso(time: &SystemTime) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_creation_routes_include_the_workflow_path_segment() {
        let api = RestApiClient {
            client: reqwest::Client::new(),
            base_url: "https://hatchet.example/api/v1".to_owned(),
            tenant_id: "tenant-1".to_owned(),
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
