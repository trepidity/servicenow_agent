use chrono::Utc;
use snow_mcp::{
    domain::primitives::IdempotencyKey,
    planner::{
        idempotency::{IdempotencyOutcome, IdempotencyStore, SqliteIdempotencyStore},
        operation_plan::{OperationReceipt, ReceiptStatus},
    },
};

fn receipt() -> OperationReceipt {
    OperationReceipt {
        plan_id: "plan-1".to_string(),
        audit_id: "audit-1".to_string(),
        parent_audit_id: "parent-1".to_string(),
        tool: "work_note_apply_add".to_string(),
        status: ReceiptStatus::Success,
        applied_changes_summary: None,
        service_now_metadata: None,
        idempotency_replay: false,
        completed_at: Utc::now(),
        op_hash: "hash-1".to_string(),
        record_url: None,
        record_snapshot: None,
        changed_fields: Vec::new(),
        concurrency_token_observed: None,
        apply_started_at: None,
        error_code: None,
        warnings: Vec::new(),
    }
}

fn receipt_for(tool: &str, audit_id: &str, op_hash: &str) -> OperationReceipt {
    OperationReceipt {
        tool: tool.to_string(),
        audit_id: audit_id.to_string(),
        op_hash: op_hash.to_string(),
        ..receipt()
    }
}

#[tokio::test]
async fn idempotency_replay_matrix() {
    let store = SqliteIdempotencyStore::in_memory().expect("store");
    let key = IdempotencyKey::client("idem-1");

    let first = store
        .check_and_record(&key, "work_note_apply_add", "hash-1", 86_400)
        .await
        .expect("first check");
    assert_eq!(first, IdempotencyOutcome::NewKey);

    let pending = store
        .check_and_record(&key, "work_note_apply_add", "hash-1", 86_400)
        .await
        .expect("pending check");
    assert_eq!(
        pending,
        IdempotencyOutcome::Pending {
            retryable: true,
            transient: true
        }
    );
    let pending_record = store
        .lookup_record(&key, "work_note_apply_add")
        .expect("pending record")
        .expect("pending record should exist");
    assert!(pending_record.apply_started_at.is_none());

    store
        .mark_apply_started(&key, "work_note_apply_add")
        .await
        .expect("mark apply started");
    let started_record = store
        .lookup_record(&key, "work_note_apply_add")
        .expect("started record")
        .expect("started record should exist");
    assert!(started_record.apply_started_at.is_some());

    let conflict = store
        .check_and_record(&key, "work_note_apply_add", "hash-2", 86_400)
        .await
        .expect("conflict check");
    assert_eq!(
        conflict,
        IdempotencyOutcome::Conflict {
            reason: "idempotency key reused with different op_hash".to_string()
        }
    );

    store
        .save_receipt(&key, &receipt())
        .await
        .expect("save receipt");
    let replay = store
        .check_and_record(&key, "work_note_apply_add", "hash-1", 86_400)
        .await
        .expect("replay check");

    let IdempotencyOutcome::Replay(replayed) = replay else {
        panic!("expected replay");
    };
    assert_eq!(replayed.audit_id, "audit-1");
    assert!(replayed.idempotency_replay);
}

#[tokio::test]
async fn lookup_receipt_is_scoped_by_tool() {
    let store = SqliteIdempotencyStore::in_memory().expect("store");
    let key = IdempotencyKey::client("idem-shared");

    store
        .check_and_record(&key, "story_apply_create", "hash-story", 86_400)
        .await
        .expect("story check");
    store
        .save_receipt(
            &key,
            &receipt_for("story_apply_create", "audit-story", "hash-story"),
        )
        .await
        .expect("save story receipt");

    store
        .check_and_record(&key, "story_task_apply_create", "hash-task", 86_400)
        .await
        .expect("task check");
    store
        .save_receipt(
            &key,
            &receipt_for("story_task_apply_create", "audit-task", "hash-task"),
        )
        .await
        .expect("save task receipt");

    let story_receipt = store
        .lookup_receipt(&key, "story_apply_create", "hash-story")
        .await
        .expect("lookup story")
        .expect("story receipt");
    assert_eq!(story_receipt.audit_id, "audit-story");

    let task_receipt = store
        .lookup_receipt(&key, "story_task_apply_create", "hash-task")
        .await
        .expect("lookup task")
        .expect("task receipt");
    assert_eq!(task_receipt.audit_id, "audit-task");

    let missing = store
        .lookup_receipt(&key, "work_note_apply_add", "hash-story")
        .await
        .expect("lookup missing");
    assert!(missing.is_none());

    let wrong_hash = store
        .lookup_receipt(&key, "story_apply_create", "hash-task")
        .await
        .expect("lookup wrong hash");
    assert!(wrong_hash.is_none());
}
