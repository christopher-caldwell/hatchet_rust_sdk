//! Typed inputs and outputs for engine management operations.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, Deserialize)]
pub struct ResourceMetadata {
    pub id: String,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TenantDetails {
    pub metadata: ResourceMetadata,
    pub name: String,
    pub slug: String,
    #[serde(flatten)]
    pub settings: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CelDebugOptions {
    pub expression: String,
    pub input: serde_json::Map<String, Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub additional_metadata: Option<serde_json::Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter_payload: Option<serde_json::Map<String, Value>>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CelEvaluation {
    pub status: String,
    pub output: Option<bool>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FilterRecord {
    pub metadata: ResourceMetadata,
    pub expression: String,
    pub scope: String,
    pub payload: Value,
    pub workflow_id: String,
    pub is_declarative: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct FilterUpdate {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Map<String, Value>>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WebhookSource {
    Generic,
    Github,
    Linear,
    Slack,
    Stripe,
    Svix,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum HmacAlgorithm {
    Md5,
    Sha1,
    Sha256,
    Sha512,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum HmacEncoding {
    Base64,
    Base64url,
    Hex,
}

/// Authentication secrets serialize only in create requests and are redacted in Debug.
#[derive(Clone, Serialize)]
#[serde(tag = "authType", content = "auth")]
pub enum WebhookAuth {
    #[serde(rename = "BASIC")]
    Basic { username: String, password: String },
    #[serde(rename = "API_KEY")]
    ApiKey {
        #[serde(rename = "headerName")]
        header_name: String,
        #[serde(rename = "apiKey")]
        api_key: String,
    },
    #[serde(rename = "HMAC")]
    Hmac {
        #[serde(rename = "signingSecret")]
        signing_secret: String,
        #[serde(rename = "signatureHeaderName")]
        signature_header_name: String,
        algorithm: HmacAlgorithm,
        encoding: HmacEncoding,
    },
}
impl std::fmt::Debug for WebhookAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("WebhookAuth([redacted])")
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WebhookCreate {
    pub name: String,
    pub source_name: WebhookSource,
    #[serde(flatten)]
    pub auth: WebhookAuth,
    #[serde(flatten)]
    pub options: WebhookUpdate,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WebhookUpdate {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_key_expression: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope_expression: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub static_payload: Option<serde_json::Map<String, Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub return_event_as_response_payload: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InboundWebhook {
    pub metadata: ResourceMetadata,
    pub name: String,
    pub source_name: WebhookSource,
    pub auth_type: String,
    #[serde(flatten)]
    pub configuration: WebhookUpdate,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleUpdate {
    pub id: String,
    pub trigger_at: String,
}
#[derive(Debug, Clone, Deserialize)]
pub struct ScheduleBulkError {
    pub id: Option<String>,
    pub error: String,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleBulkUpdateResult {
    pub updated_ids: Vec<String>,
    pub errors: Vec<ScheduleBulkError>,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScheduleBulkDeleteResult {
    pub deleted_ids: Vec<String>,
    pub errors: Vec<ScheduleBulkError>,
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PausedRunBehavior {
    Queue,
    Drop,
}
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowPauseOptions {
    #[serde(rename = "pausedWorkflowQueueTTL")]
    pub paused_workflow_queue_ttl: String,
    pub paused_workflow_cron_run_queue_behavior: PausedRunBehavior,
    pub paused_workflow_scheduled_run_queue_behavior: PausedRunBehavior,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TaskStatusMetric {
    pub status: String,
    pub count: u64,
    #[serde(rename = "runningDetailCount")]
    pub running_detail_count: Option<Value>,
}
#[derive(Debug, Clone, Deserialize)]
pub struct TaskQueueMetrics {
    pub queues: Option<HashMap<String, Value>>,
}
#[derive(Debug, Clone, Deserialize)]
pub struct TaskStat {
    pub queued: Option<TaskStatusStat>,
    pub running: Option<TaskStatusStat>,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskStatusStat {
    pub total: Option<u64>,
    pub oldest: Option<String>,
    pub oldest_excluding_retries: Option<String>,
    pub queues: Option<HashMap<String, u64>>,
    pub concurrency: Option<Vec<Value>>,
}

/// Engine run payloads and execution timestamps retained by list and get operations.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunSummary {
    pub output: Option<Value>,
    pub additional_metadata: Option<BTreeMap<String, Value>>,
    pub created_at: Option<String>,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub error_message: Option<String>,
    pub duration: Option<i64>,
    pub workflow_id: Option<String>,
    pub workflow_version_id: Option<String>,
    pub parent_task_external_id: Option<String>,
}
