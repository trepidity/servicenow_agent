use chrono::Utc;
use serde_json::json;
use snow_mcp::{
    audit::{AuditChainRange, AuditSink, SqliteAuditSink},
    domain::audit::{ActorIdentity, AuditEvent, ClientIdentity},
};

fn actor(subject: &str) -> ActorIdentity {
    ActorIdentity {
        subject: subject.to_string(),
        display_name: subject.to_string(),
        source_claim: "preferred_username".to_string(),
    }
}

fn event(id: &str, payload: serde_json::Value) -> AuditEvent {
    let mut event = AuditEvent::new_plan(
        id,
        "test",
        actor("actor"),
        actor("requester"),
        ClientIdentity {
            client_id: Some("test-client".to_string()),
            user_agent: None,
            transport: "stdio".to_string(),
        },
        "record_get",
    );
    event.timestamp = Utc::now();
    event.raw_arguments_redacted = payload.clone();
    event.normalized_arguments_redacted = payload;
    event
}

#[tokio::test]
async fn audit_chain_detects_tampering() {
    let sink = SqliteAuditSink::in_memory().expect("audit sink");
    sink.append(event("audit-1", json!({"number": "INC1"})))
        .await
        .expect("append first");
    sink.append(event("audit-2", json!({"number": "INC2"})))
        .await
        .expect("append second");

    assert!(
        sink.verify_chain(AuditChainRange::default())
            .await
            .expect("verify clean")
            .is_empty()
    );

    sink.tamper_raw_arguments_for_test("audit-1", &json!({"number": "CHANGED"}))
        .expect("tamper");
    let breaks = sink
        .verify_chain(AuditChainRange::default())
        .await
        .expect("verify tampered");

    assert_eq!(breaks.len(), 1);
    assert_eq!(breaks[0].audit_id, "audit-1");
    assert_eq!(breaks[0].reason, "row_hash mismatch");
}
