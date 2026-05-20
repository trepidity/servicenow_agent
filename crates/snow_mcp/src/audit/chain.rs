use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::domain::audit::AuditEvent;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditChainBreak {
    pub audit_id: String,
    pub reason: String,
    pub expected: String,
    pub observed: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuditChainRange {
    pub from_audit_id: Option<String>,
    pub to_audit_id: Option<String>,
}

pub fn compute_row_hash(prev_hash: &str, event_json: &Value) -> String {
    let mut normalized = event_json.clone();
    if let Some(object) = normalized.as_object_mut() {
        object.insert(
            "prev_hash".to_string(),
            Value::String(prev_hash.to_string()),
        );
        object.remove("row_hash");
    }
    let bytes = serde_json::to_vec(&normalized).unwrap_or_default();
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

pub fn verify_chain_range(events: &[AuditEvent]) -> std::result::Result<(), AuditChainBreak> {
    let mut prev = "GENESIS".to_string();
    for event in events {
        let value = serde_json::to_value(event).unwrap_or(Value::Null);
        let expected = compute_row_hash(&prev, &value);
        if expected != event.row_hash {
            return Err(AuditChainBreak {
                audit_id: event.audit_id.clone(),
                reason: "row_hash mismatch".to_string(),
                expected,
                observed: event.row_hash.clone(),
            });
        }
        prev = event.row_hash.clone();
    }
    Ok(())
}
