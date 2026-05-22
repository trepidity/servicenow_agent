use std::collections::BTreeSet;

use snow_mcp::config::McpEnvironment;
use snow_mcp::domain::policy::{
    BoardPolicyError, PolicyConfig, PolicyGate, STORY_APPLY_TOOL_NAMES, STORY_PLAN_TOOL_NAMES,
    TIMECARD_APPLY_TOOL_NAMES, TIMECARD_PLAN_TOOL_NAMES, is_write_tool, resolve_role_allowlist,
};

#[test]
fn policy_describe_exposes_read_only_defaults() {
    let cfg = PolicyConfig::default();
    let report = cfg.policy_describe("1.0.0");

    assert_eq!(report.environment, "test");
    assert_eq!(report.default_mode, "read_only");
    assert_eq!(report.kb_freshness_days, 365);
    assert_eq!(report.idempotency_window_seconds, 86_400);
    assert_eq!(report.phi_in_work_notes, "warn");
    assert_eq!(
        report.evaluation_order,
        PolicyGate::evaluation_order()
            .into_iter()
            .map(|gate| gate.as_name().to_string())
            .collect::<Vec<_>>()
    );

    let tools = resolve_role_allowlist(&cfg, ["governance_reviewer"]);
    assert!(tools.contains("audit_chain_verify"));
    assert!(cfg.tool_enabled_in_environment("work_note_apply_add", "test"));
    assert!(!cfg.tool_enabled_in_environment("work_note_apply_add", "prod"));
}

#[test]
fn boards_section_parses() {
    let cfg = PolicyConfig::from_toml_str(
        r#"
[mcp.boards."0123456789abcdef0123456789abcdef"]
name = "training-board"
instance_host = "https://example.service-now.com"
story_table = "rm_story"
task_table = "rm_scrum_task"
column_field = "sprint"
swim_lane_field = "epic"
assignment_group = "fedcba9876543210fedcba9876543210"
allowed_sprints = ["11112222333344445555666677778888"]

[mcp.tools.story_apply_create]
enabled = true
requires_confirmation = true
story_board_id = "0123456789abcdef0123456789abcdef"
"#,
    )
    .expect("policy TOML should parse");

    let binding = cfg
        .boards
        .get("0123456789abcdef0123456789abcdef")
        .expect("board binding should be present");
    assert_eq!(binding.name, "training-board");
    assert_eq!(binding.board_sys_id, "0123456789abcdef0123456789abcdef");
    assert!(!binding.allow_production);
    assert!(binding.allowed_story_states.is_empty());
    assert!(binding.allowed_task_states.is_empty());
    assert!(binding.allowed_priorities.is_empty());
    assert_eq!(
        cfg.tools
            .get("story_apply_create")
            .and_then(|policy| policy.story_board_id.as_deref()),
        Some("0123456789abcdef0123456789abcdef")
    );
}

#[test]
fn production_label_refuses_without_override() {
    let cfg = PolicyConfig::from_toml_str(
        r#"
[mcp.boards."0123456789abcdef0123456789abcdef"]
name = "training-board"
instance_host = "https://example.service-now.com"
story_table = "rm_story"
task_table = "rm_scrum_task"
column_field = "sprint"
swim_lane_field = "epic"
assignment_group = "fedcba9876543210fedcba9876543210"
allowed_sprints = ["11112222333344445555666677778888"]

[mcp.tools.story_apply_create]
enabled = true
requires_confirmation = true
environments = ["test", "training", "production"]
story_board_id = "0123456789abcdef0123456789abcdef"
"#,
    )
    .expect("policy TOML should parse");

    let environment = McpEnvironment::explicit_config("production", "America/Chicago");
    let error = cfg
        .validate_story_board_policy("story_apply_create", &environment)
        .expect_err("production should require per-board override");

    assert_eq!(
        error,
        BoardPolicyError::ProductionBoardNotAllowed {
            tool: "story_apply_create".to_string(),
            board_id: "0123456789abcdef0123456789abcdef".to_string(),
        }
    );
}

#[test]
fn configured_production_host_refuses_without_override() {
    let cfg = PolicyConfig::from_toml_str(
        r#"
[mcp]
production_hosts = ["EXAMPLE.service-now.com"]

[mcp.boards."0123456789abcdef0123456789abcdef"]
name = "training-board"
instance_host = "https://example.service-now.com/"
story_table = "rm_story"
task_table = "rm_scrum_task"
column_field = "sprint"
swim_lane_field = "epic"
assignment_group = "fedcba9876543210fedcba9876543210"
allowed_sprints = ["11112222333344445555666677778888"]

[mcp.tools.story_apply_create]
enabled = true
requires_confirmation = true
environments = ["test", "training"]
story_board_id = "0123456789abcdef0123456789abcdef"
"#,
    )
    .expect("policy TOML should parse");

    let environment = McpEnvironment::explicit_config("sandbox", "America/Chicago");
    let error = cfg
        .validate_story_board_policy("story_apply_create", &environment)
        .expect_err("configured production host should require per-board override");

    assert_eq!(
        error,
        BoardPolicyError::ProductionBoardNotAllowed {
            tool: "story_apply_create".to_string(),
            board_id: "0123456789abcdef0123456789abcdef".to_string(),
        }
    );
}

#[test]
fn empty_production_hosts_means_label_only_detection() {
    let cfg = PolicyConfig::from_toml_str(
        r#"
[mcp.boards."0123456789abcdef0123456789abcdef"]
name = "training-board"
instance_host = "https://example.service-now.com"
story_table = "rm_story"
task_table = "rm_scrum_task"
column_field = "sprint"
swim_lane_field = "epic"
assignment_group = "fedcba9876543210fedcba9876543210"
allowed_sprints = ["11112222333344445555666677778888"]

[mcp.tools.story_apply_create]
enabled = true
requires_confirmation = true
environments = ["test", "training"]
story_board_id = "0123456789abcdef0123456789abcdef"
"#,
    )
    .expect("policy TOML should parse");

    let environment = McpEnvironment::explicit_config("sandbox", "America/Chicago");
    let binding = cfg
        .validate_story_board_policy("story_apply_create", &environment)
        .expect("unconfigured host should not be treated as production")
        .expect("binding");

    assert_eq!(binding.instance_host, "https://example.service-now.com");
}

#[test]
fn substring_only_production_host_does_not_match() {
    let cfg = PolicyConfig::from_toml_str(
        r#"
[mcp]
production_hosts = ["example.service-now.com"]

[mcp.boards."0123456789abcdef0123456789abcdef"]
name = "training-board"
instance_host = "https://example.service-now.com.attacker.invalid"
story_table = "rm_story"
task_table = "rm_scrum_task"
column_field = "sprint"
swim_lane_field = "epic"
assignment_group = "fedcba9876543210fedcba9876543210"
allowed_sprints = ["11112222333344445555666677778888"]

[mcp.tools.story_apply_create]
enabled = true
requires_confirmation = true
environments = ["test", "training"]
story_board_id = "0123456789abcdef0123456789abcdef"
"#,
    )
    .expect("policy TOML should parse");

    let environment = McpEnvironment::explicit_config("sandbox", "America/Chicago");
    cfg.validate_story_board_policy("story_apply_create", &environment)
        .expect("substring host must not trigger production gate");
}

#[test]
fn is_write_tool_classifies_story_apply() {
    for tool in STORY_APPLY_TOOL_NAMES {
        assert!(is_write_tool(tool), "{tool} should be classified as write");
    }

    for tool in STORY_PLAN_TOOL_NAMES {
        assert!(
            !is_write_tool(tool),
            "{tool} should not be classified as write"
        );
    }
}

#[test]
fn default_roles_include_story_writer_and_developer_plan_grants() {
    let cfg = PolicyConfig::default();

    let story_writer = cfg
        .roles
        .get("story_writer")
        .expect("story_writer role should exist");
    for tool in ["story_get", "story_tasks_list"] {
        assert!(story_writer.read_tools.contains(tool));
    }
    for tool in STORY_PLAN_TOOL_NAMES {
        assert!(story_writer.read_tools.contains(*tool));
    }
    for tool in STORY_APPLY_TOOL_NAMES {
        assert!(story_writer.write_tools.contains(*tool));
    }

    let developer = cfg
        .roles
        .get("developer")
        .expect("developer role should exist");
    for tool in STORY_PLAN_TOOL_NAMES {
        assert!(developer.read_tools.contains(*tool));
    }
}

#[test]
fn default_story_tool_policies_match_policy_defaults() {
    let cfg = PolicyConfig::default();

    for tool in STORY_PLAN_TOOL_NAMES {
        let policy = cfg.tools.get(*tool).expect("plan policy should exist");
        assert!(policy.enabled);
        assert!(!policy.requires_confirmation);
        assert!(!policy.requires_kb_evidence);
        assert_eq!(
            policy.environments,
            vec!["test".to_string(), "training".to_string()]
        );
        assert!(policy.field_allowlist.is_empty());
    }

    for tool in STORY_APPLY_TOOL_NAMES {
        let policy = cfg.tools.get(*tool).expect("apply policy should exist");
        assert!(!policy.enabled);
        assert!(policy.requires_confirmation);
        assert!(!policy.requires_kb_evidence);
        assert_eq!(
            policy.environments,
            vec!["test".to_string(), "training".to_string()]
        );
    }

    let story_fields = BTreeSet::from([
        "acceptance_criteria".to_string(),
        "assigned_to".to_string(),
        "description".to_string(),
        "epic".to_string(),
        "priority".to_string(),
        "short_description".to_string(),
        "story_points".to_string(),
    ]);
    assert_eq!(
        cfg.tools
            .get("story_apply_create")
            .expect("story create policy")
            .field_allowlist,
        story_fields
    );

    let story_update_fields = BTreeSet::from([
        "acceptance_criteria".to_string(),
        "assigned_to".to_string(),
        "description".to_string(),
        "epic".to_string(),
        "priority".to_string(),
        "short_description".to_string(),
        "state".to_string(),
        "story_points".to_string(),
    ]);
    assert_eq!(
        cfg.tools
            .get("story_apply_update")
            .expect("story update policy")
            .field_allowlist,
        story_update_fields
    );

    let task_fields = BTreeSet::from([
        "actual_hours".to_string(),
        "assigned_to".to_string(),
        "description".to_string(),
        "due_date".to_string(),
        "planned_hours".to_string(),
        "priority".to_string(),
        "short_description".to_string(),
        "state".to_string(),
    ]);
    assert_eq!(
        cfg.tools
            .get("story_task_apply_create")
            .expect("task create policy")
            .field_allowlist,
        task_fields
    );
    assert_eq!(
        cfg.tools
            .get("story_task_apply_update")
            .expect("task update policy")
            .field_allowlist,
        task_fields
    );
}

#[test]
fn default_timecard_tool_policies_match_spec_posture() {
    let cfg = PolicyConfig::default();

    for tool in TIMECARD_PLAN_TOOL_NAMES {
        let policy = cfg.tools.get(*tool).expect("timecard plan policy");
        assert!(policy.enabled);
        assert!(!policy.requires_confirmation);
        assert!(!policy.requires_kb_evidence);
    }

    for tool in TIMECARD_APPLY_TOOL_NAMES {
        let policy = cfg.tools.get(*tool).expect("timecard apply policy");
        assert!(!policy.enabled);
        assert!(policy.requires_confirmation);
        assert!(!policy.requires_kb_evidence);
        assert_eq!(
            policy.field_allowlist,
            BTreeSet::from([
                "friday".to_string(),
                "monday".to_string(),
                "saturday".to_string(),
                "sunday".to_string(),
                "thursday".to_string(),
                "tuesday".to_string(),
                "wednesday".to_string(),
            ])
        );
        assert!(is_write_tool(tool), "{tool}");
    }
}

#[test]
fn timecard_apply_requires_explicit_environment_label() {
    let cfg = PolicyConfig::default();
    let environment = McpEnvironment::default();
    let error = cfg
        .validate_timecard_policy("timecard_apply_set_hours", &environment)
        .expect_err("implicit default label must not permit writes");

    assert_eq!(
        error,
        BoardPolicyError::ImplicitEnvironmentLabel {
            tool: "timecard_apply_set_hours".to_string(),
            environment: "test".to_string(),
        }
    );
}

#[test]
fn timecard_apply_rejects_environment_outside_tool_allowlist() {
    let mut cfg = PolicyConfig::default();
    cfg.tools
        .get_mut("timecard_apply_set_hours")
        .expect("timecard apply policy")
        .enabled = true;
    let environment = McpEnvironment::explicit_config("dev", "America/Chicago");
    let error = cfg
        .validate_timecard_policy("timecard_apply_set_hours", &environment)
        .expect_err("timecard apply should be limited to configured environments");

    assert_eq!(
        error,
        BoardPolicyError::ToolEnvironmentNotAllowed {
            tool: "timecard_apply_set_hours".to_string(),
            environment: "dev".to_string(),
        }
    );
}
