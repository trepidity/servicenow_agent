use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PipelineOutcome {
    Allow,
    Deny,
    NeedsInput,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PolicyPipeline {
    pub gates: Vec<String>,
}

impl PolicyPipeline {
    pub fn default_read_pipeline() -> Self {
        Self {
            gates: vec![
                "transport_authn".to_string(),
                "environment_binding".to_string(),
                "tool_enabled".to_string(),
                "role_entitlement".to_string(),
                "rate_limit".to_string(),
            ],
        }
    }

    pub fn default_write_pipeline() -> Self {
        Self {
            gates: vec![
                "transport_authn".to_string(),
                "environment_binding".to_string(),
                "tool_enabled".to_string(),
                "role_entitlement".to_string(),
                "kb_evidence".to_string(),
                "unique_resolution".to_string(),
                "field_allowlist".to_string(),
                "phi_redaction".to_string(),
                "confirmation".to_string(),
                "idempotency".to_string(),
                "concurrency_token".to_string(),
                "audit_precommit".to_string(),
            ],
        }
    }
}
