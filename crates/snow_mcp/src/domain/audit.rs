use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuditEvent {
    pub audit_id: String,
    pub parent_audit_id: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub environment: String,
    pub actor: ActorIdentity,
    pub requester: ActorIdentity,
    pub client: ClientIdentity,
    pub session_id: Option<String>,
    pub tool: String,
    pub tool_schema_version: String,
    pub raw_arguments_redacted: Value,
    pub normalized_arguments_redacted: Value,
    pub kb_evidence: Vec<EvidenceRef>,
    pub policy_decisions: Vec<PolicyDecisionRow>,
    pub confirmation_token_id: Option<String>,
    pub idempotency_key: Option<String>,
    pub concurrency_token: Option<String>,
    pub planned_changes: Option<Value>,
    pub applied_changes: Option<Vec<AppliedChange>>,
    pub result_status: ResultStatus,
    pub service_now_metadata: Option<ServiceNowMetadata>,
    pub error: Option<ErrorRow>,
    pub duration_ms: u64,
    pub service_now_api_calls: u32,
    pub schema_version: u32,
    pub prev_hash: String,
    pub row_hash: String,
    pub redaction_rules_version: String,
}

impl AuditEvent {
    pub fn new_plan(
        audit_id: impl Into<String>,
        environment: impl Into<String>,
        actor: ActorIdentity,
        requester: ActorIdentity,
        client: ClientIdentity,
        tool: impl Into<String>,
    ) -> Self {
        Self {
            audit_id: audit_id.into(),
            parent_audit_id: None,
            timestamp: Utc::now(),
            environment: environment.into(),
            actor,
            requester,
            client,
            session_id: None,
            tool: tool.into(),
            tool_schema_version: "1.0".to_string(),
            raw_arguments_redacted: Value::Null,
            normalized_arguments_redacted: Value::Null,
            kb_evidence: Vec::new(),
            policy_decisions: Vec::new(),
            confirmation_token_id: None,
            idempotency_key: None,
            concurrency_token: None,
            planned_changes: None,
            applied_changes: None,
            result_status: ResultStatus::Plan,
            service_now_metadata: None,
            error: None,
            duration_ms: 0,
            service_now_api_calls: 0,
            schema_version: 1,
            prev_hash: "GENESIS".to_string(),
            row_hash: String::new(),
            redaction_rules_version: "1.0.0".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActorIdentity {
    pub subject: String,
    pub display_name: String,
    pub source_claim: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClientIdentity {
    pub client_id: Option<String>,
    pub user_agent: Option<String>,
    pub transport: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EvidenceRef {
    pub article_number: String,
    pub content_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PolicyDecisionRow {
    pub gate: String,
    pub verdict: String,
    pub reason: Option<String>,
    pub remediation: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AppliedChange {
    pub field: String,
    pub old_hash: String,
    pub new_hash: String,
    pub redacted_preview: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResultStatus {
    Plan,
    AppliedSuccess,
    AppliedPartial,
    Denied,
    Error,
    Replay,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceNowMetadata {
    pub sys_id: Option<String>,
    pub number: Option<String>,
    pub transaction_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ErrorRow {
    pub code: String,
    pub reason: String,
    pub retryable: bool,
    pub transient: bool,
}
