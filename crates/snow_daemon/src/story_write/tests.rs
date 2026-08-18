use super::*;
use chrono::TimeZone;
use snow_core::{CacheSource, FieldValue, ResourceType};
use std::collections::HashMap;

const SPRINT_SYS_ID: &str = "11112222333344445555666677778888";
const OTHER_SPRINT_SYS_ID: &str = "99990000aaaabbbbccccddddeeeeffff";

fn plan_record_created_at(created_at: chrono::DateTime<Utc>) -> PlanStoreRecord {
    PlanStoreRecord {
        plan_id: "plan-1".to_string(),
        tool: "story_plan_create".to_string(),
        actor: "actor-1".to_string(),
        op_hash: "hash-1".to_string(),
        plan_json: json!({}),
        concurrency_token: None,
        created_at,
        expires_at: created_at + chrono::Duration::seconds(PLAN_TTL_SECONDS),
        state: PlanLifecycleState::Pending,
    }
}

fn story_record(fields: &[(&str, &str)]) -> SnowRecord {
    let mut field_map = HashMap::new();
    for (field, value) in fields {
        field_map.insert(
            (*field).to_string(),
            FieldValue {
                value: (*value).to_string(),
                display_value: None,
            },
        );
    }

    SnowRecord {
        sys_id: "story-sys".to_string(),
        number: "STRY001".to_string(),
        table: "rm_story".to_string(),
        resource_type: ResourceType::Story,
        state: String::new(),
        short_description: String::new(),
        description: String::new(),
        fields: field_map,
        work_notes: Vec::new(),
        comments: Vec::new(),
        parent: None,
        children: Vec::new(),
        references: HashMap::new(),
        synced_at: Utc::now(),
        source: CacheSource::Api,
    }
}

fn task_record(fields: &[(&str, &str)]) -> SnowRecord {
    let mut record = story_record(fields);
    record.sys_id = "task-sys".to_string();
    record.number = "STSK001".to_string();
    record.table = "rm_scrum_task".to_string();
    record.resource_type = ResourceType::ScrumTask;
    record
}

fn test_board_binding() -> BoardBinding {
    BoardBinding {
        name: "training-board".to_string(),
        instance_host: "https://example.service-now.com".to_string(),
        board_sys_id: "board-sys".to_string(),
        story_table: "rm_story".to_string(),
        task_table: "rm_scrum_task".to_string(),
        column_field: "sprint".to_string(),
        swim_lane_field: "epic".to_string(),
        assignment_group: "group-sys".to_string(),
        allowed_task_assignment_groups: Vec::new(),
        allowed_sprints: vec![SPRINT_SYS_ID.to_string()],
        allow_production: false,
        allowed_story_states: vec!["1".to_string()],
        allowed_task_states: vec!["1".to_string()],
        allowed_priorities: Vec::new(),
    }
}

#[test]
fn update_number_is_selector_not_blocked_field() {
    let args = Map::from_iter([("number".to_string(), json!("STRY001"))]);
    assert!(reject_blocked_fields(None, "story_plan_update", &args).is_none());
    assert!(reject_blocked_fields(None, "story_task_plan_update", &args).is_none());

    let args = Map::from_iter([("assignment_group".to_string(), json!("group-sys"))]);
    assert!(reject_blocked_fields(None, "story_plan_update", &args).is_none());
    let error = reject_blocked_fields(None, "story_task_plan_update", &args)
        .expect("task assignment_group should remain blocked")
        .error
        .expect("error");
    assert_eq!(error.message, "FIELD_REJECTED");
    assert_eq!(error.code, -32051);

    let args = Map::from_iter([("parent".to_string(), json!("parent-sys"))]);
    assert!(reject_blocked_fields(None, "story_plan_update", &args).is_none());
    assert!(reject_blocked_fields(None, "story_task_plan_update", &args).is_some());
}

#[test]
fn update_number_is_allowed_only_as_plan_selector() {
    let policy = snow_mcp::domain::policy::PolicyConfig::from_toml_str(
        r#"
[mcp.tools.story_plan_update]
enabled = true
requires_confirmation = false
story_board_id = "board-sys"
"#,
    )
    .expect("policy");
    let args = Map::from_iter([
        ("number".to_string(), json!("STRY001")),
        ("short_description".to_string(), json!("Updated")),
        ("u_custom".to_string(), json!("not governed")),
    ]);

    let plan_rejections = field_governance_rejections(
        "story_plan_update",
        &args,
        &policy,
        FieldGovernanceMode::PlanInput,
    );
    assert_eq!(
        plan_rejections,
        vec![json!({"field": "u_custom", "reason": "not_in_allowlist"})]
    );

    let write_rejections = field_governance_rejections(
        "story_apply_update",
        &args,
        &policy,
        FieldGovernanceMode::WritePayload,
    );
    assert_eq!(
        write_rejections,
        vec![
            json!({"field": "number", "reason": "blocked_deny_list"}),
            json!({"field": "u_custom", "reason": "not_in_allowlist"}),
        ]
    );
}

#[test]
fn configured_empty_apply_allowlist_rejects_writable_fields() {
    let policy = snow_mcp::domain::policy::PolicyConfig::from_toml_str(
        r#"
[mcp.tools.story_apply_update]
enabled = true
requires_confirmation = true
story_board_id = "board-sys"
"#,
    )
    .expect("policy");
    let args = Map::from_iter([
        ("number".to_string(), json!("STRY001")),
        ("short_description".to_string(), json!("Updated")),
    ]);

    let rejections = field_governance_rejections(
        "story_plan_update",
        &args,
        &policy,
        FieldGovernanceMode::PlanInput,
    );
    assert_eq!(
        rejections,
        vec![json!({"field": "short_description", "reason": "not_in_allowlist"})]
    );
}

#[test]
fn selector_and_metadata_fields_are_not_sent_to_table_api() {
    let mut story_update = json!({
        "number": "STRY001",
        "short_description": "Updated",
        "actor": {"subject": "casey"}
    });
    strip_non_writable_selector_fields("story_plan_update", &mut story_update);
    assert_eq!(story_update["short_description"], json!("Updated"));
    assert!(story_update.get("number").is_none());
    assert!(story_update.get("actor").is_none());

    let mut task_create = json!({
        "parent_story_number": "STRY001",
        "short_description": "Task",
        "requester": "casey"
    });
    strip_non_writable_selector_fields("story_task_plan_create", &mut task_create);
    assert_eq!(task_create["short_description"], json!("Task"));
    assert!(task_create.get("parent_story_number").is_none());
    assert!(task_create.get("requester").is_none());
}

#[test]
fn story_create_accepts_full_form_fields_when_policy_allows() {
    let policy = snow_mcp::domain::policy::PolicyConfig::from_toml_str(
        r#"
[mcp.tools.story_apply_create]
enabled = true
requires_confirmation = true
story_board_id = "board-sys"
field_allowlist = [
  "short_description",
  "description",
  "acceptance_criteria",
  "cmdb_ci",
  "u_story_owner",
  "backlog_type",
  "sprint",
  "assignment_group",
  "parent",
  "vendor",
  "team",
  "release_scrum",
  "u_impacted_users",
  "u_release_notes",
  "u_lead_dev",
  "u_division",
  "u_region",
  "u_location",
  "u_type",
  "u_moscow",
  "classification",
  "due_date",
  "u_desired_delivery_date",
  "product",
  "release",
  "project",
  "theme",
  "priority",
  "epic",
  "story_points",
  "u_points_est",
  "assigned_to",
]
"#,
    )
    .expect("policy");
    let args = Map::from_iter([
        ("short_description".to_string(), json!("Story")),
        ("description".to_string(), json!("Description")),
        ("acceptance_criteria".to_string(), json!("Done")),
        ("cmdb_ci".to_string(), json!("ci-sys")),
        ("u_story_owner".to_string(), json!("owner-sys")),
        ("backlog_type".to_string(), json!("product")),
        ("assignment_group".to_string(), json!("group-sys")),
        ("sprint".to_string(), json!(SPRINT_SYS_ID)),
        ("parent".to_string(), json!("parent-sys")),
        ("vendor".to_string(), json!("vendor-sys")),
        ("team".to_string(), json!("team-sys")),
        ("release_scrum".to_string(), json!("release-sys")),
        ("u_impacted_users".to_string(), json!("operators")),
        ("u_release_notes".to_string(), json!("notes")),
        ("u_lead_dev".to_string(), json!("lead-sys")),
        ("u_division".to_string(), json!("division")),
        ("u_region".to_string(), json!("region")),
        ("u_location".to_string(), json!("location")),
        ("u_type".to_string(), json!("Standard")),
        ("u_moscow".to_string(), json!("Must")),
        ("classification".to_string(), json!("Feature")),
        ("due_date".to_string(), json!("2026-06-01")),
        ("u_desired_delivery_date".to_string(), json!("2026-06-02")),
        ("product".to_string(), json!("product-sys")),
        ("release".to_string(), json!("release-sys")),
        ("project".to_string(), json!("project-sys")),
        ("theme".to_string(), json!("theme-sys")),
        ("priority".to_string(), json!("3")),
        ("epic".to_string(), json!("epic-sys")),
        ("story_points".to_string(), json!("5")),
        ("u_points_est".to_string(), json!("5")),
        ("assigned_to".to_string(), json!("assignee-sys")),
    ]);

    let rejections = field_governance_rejections(
        "story_plan_create",
        &args,
        &policy,
        FieldGovernanceMode::PlanInput,
    );
    assert!(rejections.is_empty(), "{rejections:?}");

    let binding = test_board_binding();
    let mut payload = Value::Object(args);
    default_story_create_reference_fields(&mut payload, &binding).expect("defaults");

    assert_eq!(payload["assignment_group"], json!("group-sys"));
    assert_eq!(payload["sprint"], json!(SPRINT_SYS_ID));
}

#[tokio::test(flavor = "current_thread")]
async fn story_create_defaults_story_owner_from_actor_sys_id() {
    let fixture = crate::test_support::build_fixture_state()
        .await
        .expect("fixture");
    let actor_sys_id = "0123456789abcdef0123456789abcdef";
    let actor = StoryActor {
        subject: actor_sys_id.to_string(),
        email: None,
        display_name: None,
    };
    let mut payload = json!({
        "short_description": "Story",
        "description": "Description",
    });

    let warnings = default_story_owner_from_actor(&mut payload, &actor, &fixture.state)
        .await
        .expect("owner default");

    assert_eq!(payload["u_story_owner"], json!(actor_sys_id));
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0].code, WARNING_STORY_OWNER_DEFAULTED_FROM_CALLER);
    assert_eq!(warnings[0].field.as_deref(), Some("u_story_owner"));
}

#[test]
fn story_create_warns_when_cmdb_ci_omitted() {
    let payload = json!({
        "short_description": "Story",
        "description": "Description",
    });

    let warnings = warn_missing_story_create_optional_required_fields(&payload);

    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0].code, WARNING_MISSING_OPTIONAL_REQUIRED_FIELD);
    assert_eq!(warnings[0].field.as_deref(), Some("cmdb_ci"));
}

#[test]
fn story_create_omitted_sprint_builds_product_backlog_payload() {
    let binding = test_board_binding();
    let mut payload = json!({
        "short_description": "Story",
        "description": "Description",
        "assignment_group": "group-sys",
    });

    default_story_create_reference_fields(&mut payload, &binding).expect("defaults");
    default_story_create_backlog_type(
        &mut payload,
        &snow_mcp::domain::policy::PolicyConfig::default(),
    );
    inject(&mut payload, "active", true);

    enforce_story_payload_scope(&payload, &binding).expect("backlog create scope");

    assert!(payload.get("sprint").is_none());
    assert!(payload.get("epic").is_none());
    assert_eq!(payload["assignment_group"], json!("group-sys"));
    assert_eq!(payload[STORY_BACKLOG_TYPE_FIELD], json!("product"));
}

#[test]
fn story_create_empty_sprint_builds_product_backlog_payload() {
    let binding = test_board_binding();
    let mut payload = json!({
        "short_description": "Story",
        "description": "Description",
        "assignment_group": "group-sys",
        "sprint": "  ",
    });

    default_story_create_reference_fields(&mut payload, &binding).expect("defaults");
    default_story_create_backlog_type(
        &mut payload,
        &snow_mcp::domain::policy::PolicyConfig::default(),
    );
    inject(&mut payload, "active", true);

    enforce_story_payload_scope(&payload, &binding).expect("backlog create scope");

    assert!(payload.get("sprint").is_none());
    assert_eq!(payload[STORY_BACKLOG_TYPE_FIELD], json!("product"));
}

#[test]
fn story_create_explicit_sprint_is_retained_and_scoped() {
    let binding = test_board_binding();
    let mut payload = json!({
        "short_description": "Story",
        "description": "Description",
        "assignment_group": "group-sys",
        "sprint": SPRINT_SYS_ID,
    });

    default_story_create_reference_fields(&mut payload, &binding).expect("defaults");
    default_story_create_backlog_type(
        &mut payload,
        &snow_mcp::domain::policy::PolicyConfig::default(),
    );
    inject(&mut payload, "active", true);

    enforce_story_payload_scope(&payload, &binding).expect("sprint create scope");

    assert_eq!(payload["sprint"], json!(SPRINT_SYS_ID));
    assert!(payload.get(STORY_BACKLOG_TYPE_FIELD).is_none());
}

#[test]
fn story_create_rejects_non_sys_id_sprint() {
    let binding = test_board_binding();
    let mut payload = json!({
        "short_description": "Story",
        "description": "Description",
        "assignment_group": "group-sys",
        "sprint": "not-a-sys-id",
    });

    let err = default_story_create_reference_fields(&mut payload, &binding)
        .expect_err("sprint must be a sys_id");

    match err {
        PlanBuildError::FieldRejected(fields) => assert_eq!(
            fields,
            vec![json!({
                "field": "sprint",
                "reason": "value_not_in_enum",
            })]
        ),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn story_create_rejects_unallowed_explicit_sprint() {
    let binding = test_board_binding();
    let mut payload = json!({
        "short_description": "Story",
        "description": "Description",
        "assignment_group": "group-sys",
        "sprint": OTHER_SPRINT_SYS_ID,
        "active": true,
    });

    default_story_create_reference_fields(&mut payload, &binding).expect("defaults");
    let err = enforce_story_payload_scope(&payload, &binding)
        .expect_err("sprint must stay in board scope");

    match err {
        PlanBuildError::FieldRejected(fields) => assert_eq!(
            fields,
            vec![json!({
                "field": "sprint",
                "reason": "not_in_allowlist",
                "allowed": [SPRINT_SYS_ID],
                "observed": OTHER_SPRINT_SYS_ID,
            })]
        ),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn story_create_rejects_unallowed_assignment_group_as_field_error() {
    let payload = json!({
        "short_description": "Story",
        "description": "Description",
        "assignment_group": "other-group",
        "sprint": SPRINT_SYS_ID,
        "active": true,
    });

    let err = enforce_story_payload_scope(&payload, &test_board_binding())
        .expect_err("assignment_group must stay in board scope");

    match err {
        PlanBuildError::FieldRejected(fields) => assert_eq!(
            fields,
            vec![json!({
                "field": "assignment_group",
                "reason": "not_in_allowlist",
                "expected": "group-sys",
                "observed": "other-group",
            })]
        ),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn story_task_create_assignment_group_is_governed_not_allowlist_blocked() {
    let policy = snow_mcp::domain::policy::PolicyConfig::from_toml_str(
        r#"
[mcp.tools.story_task_apply_create]
enabled = true
requires_confirmation = true
story_board_id = "board-sys"
field_allowlist = ["short_description"]
"#,
    )
    .expect("policy");
    let plan_args = Map::from_iter([
        ("parent_story_number".to_string(), json!("STRY001")),
        ("short_description".to_string(), json!("Task")),
        ("assignment_group".to_string(), json!("task-group")),
    ]);

    let plan_rejections = field_governance_rejections(
        "story_task_plan_create",
        &plan_args,
        &policy,
        FieldGovernanceMode::PlanInput,
    );
    assert!(plan_rejections.is_empty(), "{plan_rejections:?}");

    let write_payload = Map::from_iter([
        ("story".to_string(), json!("story-sys")),
        ("short_description".to_string(), json!("Task")),
        ("assignment_group".to_string(), json!("task-group")),
    ]);
    let write_rejections = field_governance_rejections(
        "story_task_apply_create",
        &write_payload,
        &policy,
        FieldGovernanceMode::WritePayload,
    );
    assert!(write_rejections.is_empty(), "{write_rejections:?}");
}

#[test]
fn story_task_create_accepts_allowed_assignment_group_override() {
    let mut binding = test_board_binding();
    binding.allowed_task_assignment_groups = vec!["task-group".to_string()];
    let parent = story_record(&[("assignment_group", "group-sys"), ("sprint", SPRINT_SYS_ID)]);
    let args = Map::from_iter([("assignment_group".to_string(), json!("task-group"))]);

    let assignment_group = resolve_story_task_create_assignment_group(&args, &parent, &binding)
        .expect("assignment group");

    assert_eq!(assignment_group, "task-group");
}

#[test]
fn story_task_create_inherits_parent_assignment_group_without_override() {
    let parent = story_record(&[("assignment_group", "group-sys"), ("sprint", SPRINT_SYS_ID)]);
    let args = Map::new();

    let assignment_group =
        resolve_story_task_create_assignment_group(&args, &parent, &test_board_binding())
            .expect("assignment group");

    assert_eq!(assignment_group, "group-sys");
}

#[test]
fn story_task_create_rejects_unallowed_assignment_group_override() {
    let parent = story_record(&[("assignment_group", "group-sys"), ("sprint", SPRINT_SYS_ID)]);
    let args = Map::from_iter([("assignment_group".to_string(), json!("other-group"))]);

    let err = resolve_story_task_create_assignment_group(&args, &parent, &test_board_binding())
        .expect_err("assignment group must be scoped");

    assert!(matches!(
        err,
        PlanBuildError::GuardFailed(InScopeFailure::WrongAssignmentGroup { .. })
    ));
}

#[test]
fn story_task_apply_create_rechecks_assignment_group_scope() {
    let payload = json!({
        "story": "story-sys",
        "short_description": "Task",
        "assignment_group": "other-group"
    });

    let failure = enforce_task_assignment_group_payload_scope(&payload, &test_board_binding())
        .expect_err("apply must re-check task assignment group");

    assert!(matches!(
        failure,
        InScopeFailure::WrongAssignmentGroup { .. }
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn apply_field_governance_rejects_tampered_number_in_update_plan() {
    let fixture = crate::test_support::build_fixture_state()
        .await
        .expect("fixture");
    let plan = OperationPlanBuilder::new("story_plan_update")
        .target(RecordRef {
            sys_id: "story-sys".to_string(),
            number: "STRY001".to_string(),
            table: "rm_story".to_string(),
        })
        .planned_changes(json!({
            "number": "STRY999",
            "short_description": "Updated",
        }))
        .build();

    let err = enforce_apply_field_governance(
        "story_apply_update",
        &plan,
        &test_board_binding(),
        &fixture.state,
        None,
    )
    .await
    .expect_err("number must not be writable");

    match err {
        PlanBuildError::FieldRejected(fields) => {
            assert_eq!(
                fields,
                vec![json!({"field": "number", "reason": "blocked_deny_list"})]
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn apply_field_governance_enforces_state_constraints() {
    let fixture = crate::test_support::build_fixture_state()
        .await
        .expect("fixture");
    let plan = OperationPlanBuilder::new("story_plan_update")
        .target(RecordRef {
            sys_id: "story-sys".to_string(),
            number: "STRY001".to_string(),
            table: "rm_story".to_string(),
        })
        .planned_changes(json!({
            "state": "cancelled",
        }))
        .build();

    let err = enforce_apply_field_governance(
        "story_apply_update",
        &plan,
        &test_board_binding(),
        &fixture.state,
        None,
    )
    .await
    .expect_err("state outside the board choices must be rejected");

    match err {
        PlanBuildError::FieldRejected(fields) => {
            assert_eq!(
                fields,
                vec![json!({"field": "state", "reason": "value_not_in_enum"})]
            );
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[tokio::test(flavor = "current_thread")]
async fn update_plan_does_not_default_assignee() {
    let fixture = crate::test_support::build_fixture_state()
        .await
        .expect("fixture");
    let actor = StoryActor {
        subject: "actor-1".to_string(),
        email: None,
        display_name: None,
    };
    let mut payload = json!({
        "state": "3",
    });

    let warnings = resolve_assignee_in_payload(
        "story_task_plan_update",
        &mut payload,
        &actor,
        &fixture.state,
    )
    .await
    .expect("assignee resolution");

    assert!(warnings.is_empty());
    assert!(payload.get("assigned_to").is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn apply_guard_rechecks_story_board_scope() {
    let fixture = crate::test_support::build_fixture_state()
        .await
        .expect("fixture");
    let plan = OperationPlanBuilder::new("story_plan_create")
        .planned_changes(json!({
            "short_description": "Out of scope",
            "description": "Wrong board",
            "assignment_group": "other-group",
            "sprint": SPRINT_SYS_ID,
            "active": true,
        }))
        .build();

    let failure = enforce_apply_guard(
        "story_apply_create",
        &plan,
        &test_board_binding(),
        &fixture.state,
    )
    .await
    .expect_err("apply must re-check board scope");

    assert!(matches!(
        failure,
        InScopeFailure::WrongAssignmentGroup { .. }
    ));
}

#[test]
fn task_update_scope_uses_task_record_not_parent_story_state() {
    let task = task_record(&[
        ("assignment_group", "group-sys"),
        ("active", "false"),
        ("state", "3"),
    ]);

    task_record_in_scope(&task, &test_board_binding())
        .expect("task update scope should not require an active parent Story");
}

#[test]
fn task_update_scope_rejects_wrong_assignment_group() {
    let task = task_record(&[("assignment_group", "other-group")]);
    let failure = task_record_in_scope(&task, &test_board_binding())
        .expect_err("task update scope must remain board-bound");

    assert!(matches!(
        failure,
        InScopeFailure::WrongAssignmentGroup { .. }
    ));
}

#[test]
fn task_update_scope_allows_configured_task_assignment_group() {
    let task = task_record(&[("assignment_group", "task-group")]);
    let mut binding = test_board_binding();
    binding.allowed_task_assignment_groups = vec!["task-group".to_string()];

    task_record_in_scope(&task, &binding)
        .expect("task update scope should allow configured task groups");
}

#[test]
fn task_create_scope_still_requires_parent_story_in_scope() {
    let parent = story_record(&[
        ("assignment_group", "group-sys"),
        ("sprint", SPRINT_SYS_ID),
        ("active", "false"),
    ]);
    let payload = json!({
        "story": "story-sys",
        "short_description": "Task"
    });

    let failure = enforce_task_parent_payload_scope(&payload, &parent, &test_board_binding())
        .expect_err("task create must still be scoped by the parent Story");

    assert!(matches!(
        failure,
        PlanBuildError::GuardFailed(InScopeFailure::TaskParentOutOfScope { .. })
    ));
}

#[test]
fn priority_choice_with_cancel_label_is_constrained_even_when_raw_value_matches() {
    let choices = vec![FieldChoice {
        label: "Cancelled".to_string(),
        value: "4".to_string(),
        terminal: true,
    }];

    assert_eq!(
        match_choice_validation("4", choices, true),
        ChoiceValidation::Constrained
    );
}

#[test]
fn create_recovery_window_uses_spec_clock_skew() {
    let created_at = Utc.with_ymd_and_hms(2026, 5, 20, 12, 0, 0).unwrap();
    let record = plan_record_created_at(created_at);

    assert_eq!(
        create_recovery_created_after(&record),
        "2026-05-20 11:55:00"
    );
}

#[test]
fn pending_create_no_match_retries_and_ambiguous_requires_operator() {
    assert_eq!(
        classify_create_recovery_lookup(&CreateRecoveryLookup::NoMatch),
        CreatePendingDecision::Proceed
    );
    assert_eq!(
        classify_create_recovery_lookup(&CreateRecoveryLookup::Ambiguous),
        CreatePendingDecision::NeedsOperator
    );
}

#[test]
fn pending_update_distinguishes_applied_from_unchanged_token() {
    let record = story_record(&[
        ("priority", "2"),
        ("sys_updated_on", "2026-05-20 12:00:00"),
        ("sys_mod_count", "7"),
    ]);
    let expected_token = ConcurrencyToken {
        sys_updated_on: "2026-05-20 12:00:00".to_string(),
        sys_mod_count: Some(7),
    };

    assert_eq!(
        classify_update_recovery_record(
            &record,
            &json!({ "priority": "2" }),
            Some(&expected_token)
        ),
        UpdatePendingDecision::AlreadyApplied
    );
    assert_eq!(
        classify_update_recovery_record(
            &record,
            &json!({ "priority": "3" }),
            Some(&expected_token)
        ),
        UpdatePendingDecision::ProceedUnchangedToken
    );

    let advanced_token = ConcurrencyToken {
        sys_updated_on: "2026-05-20 12:01:00".to_string(),
        sys_mod_count: Some(8),
    };
    assert_eq!(
        classify_update_recovery_record(
            &record,
            &json!({ "priority": "3" }),
            Some(&advanced_token)
        ),
        UpdatePendingDecision::NeedsOperator
    );
}

#[test]
fn audit_warnings_keep_only_full_email_hash_for_d5_warning_data() {
    let warnings = vec![ReceiptWarning {
        code: WARNING_ASSIGNEE_DEFAULTED_FROM_CALLER.to_string(),
        field: Some("assigned_to".to_string()),
        message: "Substituted caller identity (first.last@<sha256:a379a6f6>) for omitted assignee."
            .to_string(),
        data: Some(json!({
            "email_local_part": "first.last",
            "domain_hash": "a379a6f6",
        })),
    }];

    let audit_values = audit_warnings(&warnings, &["full-email-hash".to_string()]);
    assert_eq!(
        audit_values,
        vec![json!({
            "code": WARNING_ASSIGNEE_DEFAULTED_FROM_CALLER,
            "field": "assigned_to",
            "data": { "email_sha256": "full-email-hash" },
        })]
    );
    let serialized = serde_json::to_string(&audit_values).expect("serialize audit warnings");
    assert!(!serialized.contains("email_local_part"));
    assert!(!serialized.contains("domain_hash"));
    assert!(!serialized.contains("first.last"));
    assert!(!serialized.contains("a379a6f6"));
}
