use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[non_exhaustive]
pub enum McpErrorCode {
    InvalidArgs,
    PolicyDenied,
    NeedsInput,
    NotFound,
    ServiceFailure,
    KbEvidenceRequired,
    KbEvidenceStale,
    AuditTamperDetected,
    ConfirmationRequired,
    ConfirmationExpired,
    ConfirmationAlreadyUsed,
    IdempotencyConflict,
    PartialFailure,
    NotImplemented,
}

pub fn invalid_params(
    id: Option<Value>,
    err: impl ToString,
) -> crate::protocol::schema::JsonRpcResponse {
    crate::protocol::schema::JsonRpcResponse::error(
        id,
        McpErrorCode::InvalidArgs.json_rpc_code(),
        "invalid params",
        Some(json!({ "details": err.to_string() })),
    )
}

pub fn method_not_found(
    id: Option<Value>,
    message: &str,
) -> crate::protocol::schema::JsonRpcResponse {
    crate::protocol::schema::JsonRpcResponse::error(id, -32601, message, None)
}

pub fn not_found(id: Option<Value>, message: &str) -> crate::protocol::schema::JsonRpcResponse {
    crate::protocol::schema::JsonRpcResponse::error(
        id,
        McpErrorCode::NotFound.json_rpc_code(),
        message,
        None,
    )
}

pub fn service_failure(
    id: Option<Value>,
    err: impl ToString,
) -> crate::protocol::schema::JsonRpcResponse {
    crate::protocol::schema::JsonRpcResponse::error(
        id,
        McpErrorCode::ServiceFailure.json_rpc_code(),
        "service failure",
        Some(json!({ "details": err.to_string() })),
    )
}

pub fn policy_denied(
    id: Option<Value>,
    reason: impl ToString,
) -> crate::protocol::schema::JsonRpcResponse {
    crate::protocol::schema::JsonRpcResponse::error(
        id,
        McpErrorCode::PolicyDenied.json_rpc_code(),
        "policy denied",
        Some(json!({ "reason": reason.to_string(), "service_now_write_attempted": false })),
    )
}

pub fn not_implemented(
    id: Option<Value>,
    details: impl ToString,
) -> crate::protocol::schema::JsonRpcResponse {
    crate::protocol::schema::JsonRpcResponse::error(
        id,
        McpErrorCode::NotImplemented.json_rpc_code(),
        "not implemented",
        Some(json!({ "details": details.to_string(), "service_now_write_attempted": false })),
    )
}

pub fn kb_evidence_required(
    id: Option<Value>,
    details: impl ToString,
) -> crate::protocol::schema::JsonRpcResponse {
    crate::protocol::schema::JsonRpcResponse::error(
        id,
        McpErrorCode::KbEvidenceRequired.json_rpc_code(),
        "kb evidence required",
        Some(json!({ "details": details.to_string(), "service_now_write_attempted": false })),
    )
}

impl McpErrorCode {
    pub fn json_rpc_code(&self) -> i64 {
        match self {
            Self::InvalidArgs => -32602,
            Self::NotFound => -32004,
            Self::PolicyDenied => -32040,
            Self::NeedsInput => -32041,
            Self::KbEvidenceRequired | Self::KbEvidenceStale => -32042,
            Self::AuditTamperDetected => -32043,
            Self::ConfirmationRequired
            | Self::ConfirmationExpired
            | Self::ConfirmationAlreadyUsed => -32044,
            Self::IdempotencyConflict => -32045,
            Self::PartialFailure => -32046,
            Self::ServiceFailure | Self::NotImplemented => -32000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct McpError {
    pub code: McpErrorCode,
    pub message: String,
    pub details: Option<Value>,
}

impl McpError {
    pub fn new(code: McpErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            details: None,
        }
    }

    pub fn with_details(mut self, details: Value) -> Self {
        self.details = Some(details);
        self
    }

    pub fn data(&self) -> Value {
        json!({
            "code": self.code,
            "details": self.details,
        })
    }
}
