use snow_mcp::planner::confirmation::{
    ConfirmationBinding, ConfirmationConsumeError, ConfirmationStore, SqliteConfirmationStore,
};

fn binding() -> ConfirmationBinding {
    ConfirmationBinding {
        actor: "actor".to_string(),
        requester: "requester".to_string(),
        tool: "work_note_apply_add".to_string(),
        op_hash: "op-hash".to_string(),
        environment: "test".to_string(),
    }
}

#[tokio::test]
async fn confirmation_token_is_single_use_and_bound() {
    let store = SqliteConfirmationStore::in_memory().expect("store");
    let token = store.issue("plan-1", binding(), 600).await.expect("issue");

    let mut wrong = binding();
    wrong.actor = "other-actor".to_string();
    let err = store
        .consume(&token.token_id, &wrong)
        .await
        .expect_err("wrong actor should fail");
    assert_eq!(
        err,
        ConfirmationConsumeError::BindingMismatch { field: "actor" }
    );

    let consumed = store
        .consume(&token.token_id, &binding())
        .await
        .expect("consume");
    assert!(consumed.consumed);

    let err = store
        .consume(&token.token_id, &binding())
        .await
        .expect_err("second consume should fail");
    assert_eq!(err, ConfirmationConsumeError::AlreadyConsumed);
}

#[tokio::test]
async fn confirmation_token_expires() {
    let store = SqliteConfirmationStore::in_memory().expect("store");
    let token = store.issue("plan-1", binding(), 0).await.expect("issue");

    let err = store
        .consume(&token.token_id, &binding())
        .await
        .expect_err("expired token should fail");
    assert_eq!(err, ConfirmationConsumeError::Expired);
}
