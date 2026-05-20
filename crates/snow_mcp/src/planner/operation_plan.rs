use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::domain::primitives::{Citation, RecordRef};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OperationPlan {
    pub plan_id: String,
    pub tool: String,
    pub target: Option<RecordRef>,
    pub planned_changes: Value,
    pub citations: Vec<Citation>,
    pub op_hash: String,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct OperationReceipt {
    pub plan_id: String,
    pub audit_id: String,
    pub parent_audit_id: String,
    pub tool: String,
    pub status: ReceiptStatus,
    pub applied_changes_summary: Option<Value>,
    pub service_now_metadata: Option<crate::domain::audit::ServiceNowMetadata>,
    pub idempotency_replay: bool,
    pub completed_at: DateTime<Utc>,
    #[serde(default)]
    pub op_hash: String,
    #[serde(default)]
    pub record_url: Option<String>,
    #[serde(default)]
    pub record_snapshot: Option<Value>,
    #[serde(default)]
    pub changed_fields: Vec<FieldChange>,
    #[serde(default)]
    pub concurrency_token_observed: Option<ConcurrencyToken>,
    #[serde(default)]
    pub apply_started_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub error_code: Option<String>,
    #[serde(default)]
    pub warnings: Vec<ReceiptWarning>,
}

impl OperationReceipt {
    pub fn applied_at(&self) -> DateTime<Utc> {
        self.completed_at
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReceiptWarning {
    pub code: String,
    pub field: Option<String>,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FieldChange {
    pub field: String,
    pub before: Option<Value>,
    pub after: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConcurrencyToken {
    pub sys_updated_on: String,
    pub sys_mod_count: Option<i64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReceiptStatus {
    Success,
    Partial,
    Failed,
}

pub struct OperationPlanBuilder {
    tool: String,
    target: Option<RecordRef>,
    planned_changes: Value,
    citations: Vec<Citation>,
    expires_at: DateTime<Utc>,
}

impl OperationPlanBuilder {
    pub fn new(tool: impl Into<String>) -> Self {
        Self {
            tool: tool.into(),
            target: None,
            planned_changes: Value::Null,
            citations: Vec::new(),
            expires_at: Utc::now() + chrono::Duration::minutes(10),
        }
    }

    pub fn target(mut self, target: RecordRef) -> Self {
        self.target = Some(target);
        self
    }

    pub fn planned_changes(mut self, changes: Value) -> Self {
        self.planned_changes = changes;
        self
    }

    pub fn citations(mut self, citations: Vec<Citation>) -> Self {
        self.citations = citations;
        self
    }

    pub fn build(self) -> OperationPlan {
        let plan_id = Uuid::new_v4().to_string();
        let op_hash = compute_op_hash(&self.tool, self.target.as_ref(), &self.planned_changes);
        OperationPlan {
            plan_id,
            tool: self.tool,
            target: self.target,
            planned_changes: self.planned_changes,
            citations: self.citations,
            op_hash,
            expires_at: self.expires_at,
        }
    }
}

pub fn compute_op_hash(tool: &str, target: Option<&RecordRef>, changes: &Value) -> String {
    let mut hasher = Sha256::new();
    hasher.update(tool.as_bytes());
    if let Some(target) = target {
        hasher.update(target.number.as_bytes());
        hasher.update(target.table.as_bytes());
        hasher.update(target.sys_id.as_bytes());
    }
    hasher.update(serde_json::to_vec(changes).unwrap_or_default());
    hex::encode(hasher.finalize())
}
