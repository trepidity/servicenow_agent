use std::collections::{BTreeMap, BTreeSet, HashSet};

use crate::config::McpEnvironment;
use crate::protocol::schema::PolicyDescribeReport;
use serde::{Deserialize, Serialize};
use thiserror::Error;

const STORY_APPLY_CREATE_FIELDS: &[&str] = &[
    "short_description",
    "description",
    "acceptance_criteria",
    "cmdb_ci",
    "u_story_owner",
    "sprint",
    "assignment_group",
    "parent",
    "vendor",
    "team",
    "release_scrum",
    "state",
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
];

const STORY_APPLY_UPDATE_FIELDS: &[&str] = &[
    "short_description",
    "description",
    "acceptance_criteria",
    "cmdb_ci",
    "u_story_owner",
    "sprint",
    "assignment_group",
    "parent",
    "vendor",
    "team",
    "release_scrum",
    "state",
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
    "percent_complete",
];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PolicyConfig {
    #[serde(default = "default_environment")]
    pub environment: String,
    #[serde(default = "default_timezone")]
    pub instance_timezone: String,
    #[serde(default = "default_mode")]
    pub default_mode: String,
    #[serde(default = "default_kb_freshness_days")]
    pub kb_freshness_days: u32,
    #[serde(default = "default_idempotency_window_seconds")]
    pub idempotency_window_seconds: u64,
    #[serde(default = "default_phi_in_work_notes")]
    pub phi_in_work_notes: String,
    #[serde(default = "default_confirmation_ttl_seconds")]
    pub confirmation_ttl_seconds_default: u64,
    #[serde(default)]
    pub production_hosts: Vec<String>,
    #[serde(default)]
    pub roles: BTreeMap<String, RoleAllowList>,
    #[serde(default)]
    pub boards: BTreeMap<String, BoardBinding>,
    #[serde(default)]
    pub tools: BTreeMap<String, ToolPolicy>,
}

impl PolicyConfig {
    pub fn read_only_default() -> Self {
        Self {
            environment: default_environment(),
            instance_timezone: default_timezone(),
            default_mode: default_mode(),
            kb_freshness_days: default_kb_freshness_days(),
            idempotency_window_seconds: default_idempotency_window_seconds(),
            phi_in_work_notes: default_phi_in_work_notes(),
            confirmation_ttl_seconds_default: default_confirmation_ttl_seconds(),
            production_hosts: Vec::new(),
            roles: default_roles(),
            boards: BTreeMap::new(),
            tools: default_tools(),
        }
    }

    pub fn from_toml_str(input: &str) -> Result<Self, toml::de::Error> {
        #[derive(Deserialize)]
        struct Wrapper {
            mcp: PolicyConfig,
        }

        toml::from_str::<Wrapper>(input).map(|wrapper| {
            let mut config = wrapper.mcp;
            config.apply_missing_tool_defaults();
            config.apply_board_key_defaults();
            config.normalize_production_hosts();
            config
        })
    }

    pub fn is_tool_enabled(&self, tool: &str) -> bool {
        self.tools
            .get(tool)
            .map(|policy| policy.enabled)
            .unwrap_or_else(|| !is_write_tool(tool))
    }

    pub fn tool_enabled_in_environment(&self, tool: &str, environment: &str) -> bool {
        self.tools
            .get(tool)
            .map(|policy| {
                policy.enabled
                    && (policy.environments.is_empty()
                        || policy.environments.iter().any(|env| env == environment))
            })
            .unwrap_or_else(|| !is_write_tool(tool))
    }

    pub fn policy_describe(
        &self,
        _redaction_rules_version: impl Into<String>,
    ) -> PolicyDescribeReport {
        PolicyDescribeReport {
            environment: self.environment.clone(),
            default_mode: self.default_mode.clone(),
            roles: self.roles.keys().cloned().collect(),
            write_tools_enabled: self
                .tools
                .iter()
                .filter(|(_, policy)| policy.enabled)
                .map(|(name, _)| name.clone())
                .collect(),
            kb_freshness_days: self.kb_freshness_days,
            idempotency_window_seconds: self.idempotency_window_seconds,
            phi_in_work_notes: self.phi_in_work_notes.clone(),
            evaluation_order: PolicyGate::evaluation_order()
                .into_iter()
                .map(|gate| gate.as_name().to_string())
                .collect(),
        }
    }

    pub fn apply_board_key_defaults(&mut self) {
        for (board_id, binding) in &mut self.boards {
            if binding.board_sys_id.is_empty() {
                binding.board_sys_id = board_id.clone();
            }
        }
    }

    pub fn apply_missing_tool_defaults(&mut self) {
        for (tool, policy) in default_tools() {
            self.tools.entry(tool).or_insert(policy);
        }
    }

    pub fn normalize_production_hosts(&mut self) {
        let mut hosts = self
            .production_hosts
            .iter()
            .filter_map(|host| normalize_instance_hostname(host))
            .collect::<Vec<_>>();
        hosts.sort();
        hosts.dedup();
        self.production_hosts = hosts;
    }

    pub fn resolve_tool_board_binding(
        &self,
        tool: &str,
    ) -> Result<Option<&BoardBinding>, BoardPolicyError> {
        if !is_story_board_tool(tool) {
            return Ok(None);
        }

        let policy = self
            .tools
            .get(tool)
            .ok_or_else(|| BoardPolicyError::MissingToolPolicy {
                tool: tool.to_string(),
            })?;
        let board_id = policy.story_board_id.as_deref().ok_or_else(|| {
            BoardPolicyError::MissingStoryBoardId {
                tool: tool.to_string(),
            }
        })?;
        self.boards
            .get(board_id)
            .ok_or_else(|| BoardPolicyError::UnknownBoard {
                tool: tool.to_string(),
                board_id: board_id.to_string(),
            })
            .map(Some)
    }

    pub fn validate_story_board_policy(
        &self,
        tool: &str,
        environment: &McpEnvironment,
    ) -> Result<Option<&BoardBinding>, BoardPolicyError> {
        let Some(binding) = self.resolve_tool_board_binding(tool)? else {
            return Ok(None);
        };

        let label = environment.label.as_str();
        if is_story_apply_tool(tool) && !environment.has_explicit_label_source() {
            return Err(BoardPolicyError::ImplicitEnvironmentLabel {
                tool: tool.to_string(),
                environment: label.to_string(),
            });
        }

        if self.is_story_production_context(environment, binding) {
            self.validate_story_production_gate(tool, binding)?;
        }

        Ok(Some(binding))
    }

    pub fn validate_timecard_policy(
        &self,
        tool: &str,
        environment: &McpEnvironment,
    ) -> Result<(), BoardPolicyError> {
        if !is_timecard_apply_tool(tool) {
            return Ok(());
        }

        let label = environment.label.as_str();
        if !environment.has_explicit_label_source() {
            return Err(BoardPolicyError::ImplicitEnvironmentLabel {
                tool: tool.to_string(),
                environment: label.to_string(),
            });
        }

        let policy = self
            .tools
            .get(tool)
            .ok_or_else(|| BoardPolicyError::MissingToolPolicy {
                tool: tool.to_string(),
            })?;
        if !policy.environments.is_empty() && !policy.environments.iter().any(|env| env == label) {
            return Err(BoardPolicyError::ToolEnvironmentNotAllowed {
                tool: tool.to_string(),
                environment: label.to_string(),
            });
        }

        if is_production_environment(label)
            && !policy
                .environments
                .iter()
                .any(|env| is_production_environment(env))
        {
            return Err(BoardPolicyError::ProductionToolEnvironmentNotAllowed {
                tool: tool.to_string(),
                board_id: "timecard".to_string(),
            });
        }

        Ok(())
    }

    pub fn validate_story_production_gate(
        &self,
        tool: &str,
        binding: &BoardBinding,
    ) -> Result<(), BoardPolicyError> {
        if !is_story_apply_tool(tool) {
            return Ok(());
        }

        if !binding.allow_production {
            return Err(BoardPolicyError::ProductionBoardNotAllowed {
                tool: tool.to_string(),
                board_id: binding.board_sys_id.clone(),
            });
        }

        let policy = self
            .tools
            .get(tool)
            .ok_or_else(|| BoardPolicyError::MissingToolPolicy {
                tool: tool.to_string(),
            })?;
        if !policy
            .environments
            .iter()
            .any(|env| is_production_environment(env))
        {
            return Err(BoardPolicyError::ProductionToolEnvironmentNotAllowed {
                tool: tool.to_string(),
                board_id: binding.board_sys_id.clone(),
            });
        }

        Ok(())
    }

    fn is_story_production_context(
        &self,
        environment: &McpEnvironment,
        binding: &BoardBinding,
    ) -> bool {
        if is_production_environment(&environment.label) {
            return true;
        }

        let Some(host) = normalize_instance_hostname(&binding.instance_host) else {
            return false;
        };
        self.production_hosts
            .iter()
            .any(|candidate| candidate == &host)
    }
}

impl Default for PolicyConfig {
    fn default() -> Self {
        Self::read_only_default()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct RoleAllowList {
    #[serde(default)]
    pub read_tools: BTreeSet<String>,
    #[serde(default)]
    pub write_tools: BTreeSet<String>,
    #[serde(default)]
    pub allowed_tools: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BoardBinding {
    pub name: String,
    pub instance_host: String,
    #[serde(default)]
    pub board_sys_id: String,
    pub story_table: String,
    pub task_table: String,
    pub column_field: String,
    pub swim_lane_field: String,
    pub assignment_group: String,
    #[serde(default)]
    pub allowed_task_assignment_groups: Vec<String>,
    #[serde(default)]
    pub allowed_sprints: Vec<String>,
    #[serde(default)]
    pub allow_production: bool,
    #[serde(default)]
    pub allowed_story_states: Vec<String>,
    #[serde(default)]
    pub allowed_task_states: Vec<String>,
    #[serde(default)]
    pub allowed_priorities: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolPolicy {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub requires_confirmation: bool,
    #[serde(default)]
    pub requires_kb_evidence: bool,
    #[serde(default)]
    pub field_allowlist: BTreeSet<String>,
    #[serde(default)]
    pub environments: Vec<String>,
    #[serde(default)]
    pub confirmation_ttl_seconds: Option<u64>,
    #[serde(default)]
    pub max_records: Option<u64>,
    #[serde(default)]
    pub skip_terminal_records: bool,
    #[serde(default)]
    pub story_board_id: Option<String>,
}

impl Default for ToolPolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            requires_confirmation: true,
            requires_kb_evidence: true,
            field_allowlist: BTreeSet::new(),
            environments: vec!["test".to_string(), "training".to_string()],
            confirmation_ttl_seconds: None,
            max_records: None,
            skip_terminal_records: false,
            story_board_id: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum BoardPolicyError {
    #[error("tool policy is missing for governed Story board tool {tool}")]
    MissingToolPolicy { tool: String },
    #[error("tool policy for governed Story board tool {tool} is missing story_board_id")]
    MissingStoryBoardId { tool: String },
    #[error("tool policy for {tool} references unknown board {board_id}")]
    UnknownBoard { tool: String, board_id: String },
    #[error("Story apply tool {tool} requires an explicit environment label source")]
    ImplicitEnvironmentLabel { tool: String, environment: String },
    #[error("board {board_id} does not allow production for Story apply tool {tool}")]
    ProductionBoardNotAllowed { tool: String, board_id: String },
    #[error("tool policy for {tool} does not allow production environment on board {board_id}")]
    ProductionToolEnvironmentNotAllowed { tool: String, board_id: String },
    #[error("tool policy for {tool} does not allow environment {environment}")]
    ToolEnvironmentNotAllowed { tool: String, environment: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PolicyDecision {
    Allow,
    Deny { reason: String },
    NeedsInput { reason: String },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PolicyGate {
    Authentication,
    Entitlement,
    SchemaValidation,
    EnvironmentGate,
    Resolution,
    KbEvidence,
    FieldAllowlist,
    BulkLimit,
    Idempotency,
    ConcurrencyToken,
    Confirmation,
    BusinessHours,
    RateLimit,
    ElevatedConfirmation,
    PhiRedaction,
    DisplayValueMatch,
}

impl PolicyGate {
    pub fn evaluation_order() -> Vec<Self> {
        vec![
            Self::Authentication,
            Self::Entitlement,
            Self::SchemaValidation,
            Self::EnvironmentGate,
            Self::Resolution,
            Self::KbEvidence,
            Self::FieldAllowlist,
            Self::BulkLimit,
            Self::Idempotency,
            Self::ConcurrencyToken,
            Self::Confirmation,
            Self::BusinessHours,
            Self::RateLimit,
        ]
    }

    pub fn as_name(self) -> &'static str {
        match self {
            Self::Authentication => "Authentication",
            Self::Entitlement => "Entitlement",
            Self::SchemaValidation => "SchemaValidation",
            Self::EnvironmentGate => "EnvironmentGate",
            Self::Resolution => "Resolution",
            Self::KbEvidence => "KbEvidence",
            Self::FieldAllowlist => "FieldAllowlist",
            Self::BulkLimit => "BulkLimit",
            Self::Idempotency => "Idempotency",
            Self::ConcurrencyToken => "ConcurrencyToken",
            Self::Confirmation => "Confirmation",
            Self::BusinessHours => "BusinessHours",
            Self::RateLimit => "RateLimit",
            Self::ElevatedConfirmation => "ElevatedConfirmation",
            Self::PhiRedaction => "PhiRedaction",
            Self::DisplayValueMatch => "DisplayValueMatch",
        }
    }
}

pub const STORY_PLAN_TOOL_NAMES: &[&str] = &[
    "story_plan_create",
    "story_plan_update",
    "story_task_plan_create",
    "story_task_plan_update",
];

pub const STORY_APPLY_TOOL_NAMES: &[&str] = &[
    "story_apply_create",
    "story_apply_update",
    "story_task_apply_create",
    "story_task_apply_update",
];

pub const TIMECARD_PLAN_TOOL_NAMES: &[&str] = &["timecard_plan_set_hours"];

pub const TIMECARD_APPLY_TOOL_NAMES: &[&str] = &["timecard_apply_set_hours"];

pub fn is_story_plan_tool(tool: &str) -> bool {
    STORY_PLAN_TOOL_NAMES.contains(&tool)
}

pub fn is_story_apply_tool(tool: &str) -> bool {
    STORY_APPLY_TOOL_NAMES.contains(&tool)
}

pub fn is_story_board_tool(tool: &str) -> bool {
    is_story_plan_tool(tool) || is_story_apply_tool(tool)
}

pub fn is_timecard_plan_tool(tool: &str) -> bool {
    TIMECARD_PLAN_TOOL_NAMES.contains(&tool)
}

pub fn is_timecard_apply_tool(tool: &str) -> bool {
    TIMECARD_APPLY_TOOL_NAMES.contains(&tool)
}

pub fn is_write_tool(tool: &str) -> bool {
    tool.contains("_apply_")
        || tool.contains("_submit_")
        || matches!(
            tool,
            "catalog_submit_request"
                | "catalog_cancel_request"
                | "work_note_apply_add"
                | "attachment_upload"
                | "plan_cancel"
        )
}

fn is_production_environment(environment: &str) -> bool {
    ["prod", "prd", "production"]
        .iter()
        .any(|candidate| environment.eq_ignore_ascii_case(candidate))
}

fn normalize_instance_hostname(input: &str) -> Option<String> {
    let mut value = input.trim().to_ascii_lowercase();
    if value.is_empty() {
        return None;
    }

    if let Some(rest) = value.strip_prefix("https://") {
        value = rest.to_string();
    } else if let Some(rest) = value.strip_prefix("http://") {
        value = rest.to_string();
    }

    let host = value
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default()
        .trim()
        .trim_end_matches('.');
    if host.is_empty() || host.contains('@') {
        return None;
    }

    let host = host.split(':').next().unwrap_or_default().trim();
    if host.is_empty() {
        None
    } else {
        Some(host.to_string())
    }
}

pub fn resolve_role_allowlist<I, S>(cfg: &PolicyConfig, roles: I) -> HashSet<&str>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut tools = HashSet::new();
    for role in roles {
        if let Some(list) = cfg.roles.get(role.as_ref()) {
            tools.extend(list.read_tools.iter().map(String::as_str));
            tools.extend(list.write_tools.iter().map(String::as_str));
            tools.extend(list.allowed_tools.iter().map(String::as_str));
        }
    }
    tools
}

fn default_environment() -> String {
    "test".to_string()
}

fn default_timezone() -> String {
    "America/Chicago".to_string()
}

fn default_mode() -> String {
    "read_only".to_string()
}

fn default_kb_freshness_days() -> u32 {
    365
}

fn default_idempotency_window_seconds() -> u64 {
    86_400
}

fn default_phi_in_work_notes() -> String {
    "warn".to_string()
}

fn default_confirmation_ttl_seconds() -> u64 {
    600
}

fn role(read_tools: &[&str], write_tools: &[&str]) -> RoleAllowList {
    let mut allowed_tools = read_tools
        .iter()
        .chain(write_tools.iter())
        .map(|tool| (*tool).to_string())
        .collect::<Vec<_>>();
    allowed_tools.sort();
    allowed_tools.dedup();
    RoleAllowList {
        read_tools: read_tools.iter().map(|tool| (*tool).to_string()).collect(),
        write_tools: write_tools.iter().map(|tool| (*tool).to_string()).collect(),
        allowed_tools,
    }
}

fn default_roles() -> BTreeMap<String, RoleAllowList> {
    BTreeMap::from([
        (
            "developer".to_string(),
            role(
                &[
                    "knowledge_search",
                    "knowledge_fetch",
                    "record_get",
                    "record_search",
                    "user_lookup",
                    "user_search",
                    "attachment_list",
                    "get_work_notes",
                    "work_note_plan_add",
                    "resource_plan_get",
                    "story_get",
                    "story_tasks_list",
                    "user_lookup",
                    "user_search",
                    "story_plan_create",
                    "story_plan_update",
                    "story_task_plan_create",
                    "story_task_plan_update",
                    "timecard_list",
                    "timecard_plan_set_hours",
                    "policy_describe",
                    "tool_capabilities",
                ],
                &[],
            ),
        ),
        (
            "story_writer".to_string(),
            role(
                &[
                    "story_get",
                    "story_tasks_list",
                    "story_plan_create",
                    "story_plan_update",
                    "story_task_plan_create",
                    "story_task_plan_update",
                    "work_note_plan_add",
                ],
                &[
                    "story_apply_create",
                    "story_apply_update",
                    "story_task_apply_create",
                    "story_task_apply_update",
                    "work_note_apply_add",
                ],
            ),
        ),
        (
            "timecard_writer".to_string(),
            role(
                &[
                    "timecard_list",
                    "timecard_plan_set_hours",
                    "user_lookup",
                    "user_search",
                ],
                &["timecard_apply_set_hours"],
            ),
        ),
        (
            "change_writer".to_string(),
            role(
                &[
                    "get_record",
                    "get_children",
                    "get_work_notes",
                    "user_lookup",
                    "user_search",
                    "attachment_list",
                    "change_request_plan_create",
                    "change_request_plan_update",
                    "change_task_plan_create",
                    "change_task_plan_update",
                    "plan_get",
                    "policy_describe",
                    "tool_capabilities",
                ],
                &[
                    "change_request_apply_create",
                    "change_request_apply_update",
                    "change_task_apply_create",
                    "change_task_apply_update",
                    "attachment_upload",
                    "work_note_apply_add",
                ],
            ),
        ),
        (
            "governance_reviewer".to_string(),
            role(
                &[
                    "audit_event_get",
                    "audit_events_search",
                    "audit_chain_verify",
                    "policy_describe",
                    "tool_capabilities",
                    "redaction_rules_describe",
                ],
                &[],
            ),
        ),
        (
            "mcp_administrator".to_string(),
            role(
                &[
                    "audit_event_get",
                    "audit_events_search",
                    "audit_chain_verify",
                    "policy_describe",
                    "tool_capabilities",
                    "redaction_rules_describe",
                ],
                &[],
            ),
        ),
    ])
}

fn default_tools() -> BTreeMap<String, ToolPolicy> {
    BTreeMap::from([
        ("story_plan_create".to_string(), story_plan_policy()),
        ("story_plan_update".to_string(), story_plan_policy()),
        ("story_task_plan_create".to_string(), story_plan_policy()),
        ("story_task_plan_update".to_string(), story_plan_policy()),
        (
            "story_apply_create".to_string(),
            story_apply_policy(STORY_APPLY_CREATE_FIELDS),
        ),
        (
            "story_apply_update".to_string(),
            story_apply_policy(STORY_APPLY_UPDATE_FIELDS),
        ),
        (
            "story_task_apply_create".to_string(),
            story_apply_policy(&[
                "short_description",
                "description",
                "assigned_to",
                "priority",
                "due_date",
                "state",
                "planned_hours",
                "actual_hours",
            ]),
        ),
        (
            "story_task_apply_update".to_string(),
            story_apply_policy(&[
                "short_description",
                "description",
                "assigned_to",
                "priority",
                "due_date",
                "state",
                "planned_hours",
                "actual_hours",
                "remaining_hours",
                "percent_complete",
            ]),
        ),
        (
            "catalog_submit_request".to_string(),
            ToolPolicy {
                enabled: true,
                requires_confirmation: true,
                requires_kb_evidence: true,
                environments: vec!["test".to_string(), "training".to_string()],
                confirmation_ttl_seconds: Some(600),
                ..ToolPolicy::default()
            },
        ),
        (
            "attachment_list".to_string(),
            ToolPolicy {
                enabled: true,
                requires_confirmation: false,
                requires_kb_evidence: false,
                environments: default_story_environments(),
                ..ToolPolicy::default()
            },
        ),
        (
            "attachment_upload".to_string(),
            ToolPolicy {
                enabled: false,
                requires_confirmation: true,
                requires_kb_evidence: false,
                environments: default_story_environments(),
                confirmation_ttl_seconds: Some(default_confirmation_ttl_seconds()),
                ..ToolPolicy::default()
            },
        ),
        (
            "change_request_plan_create".to_string(),
            change_plan_policy(),
        ),
        (
            "change_request_plan_update".to_string(),
            change_plan_policy(),
        ),
        ("change_task_plan_create".to_string(), change_plan_policy()),
        ("change_task_plan_update".to_string(), change_plan_policy()),
        (
            "change_request_apply_create".to_string(),
            change_apply_policy(&[
                "short_description",
                "description",
                "type",
                "category",
                "assignment_group",
                "assigned_to",
                "cmdb_ci",
                "start_date",
                "end_date",
                "implementation_plan",
                "change_plan",
                "backout_plan",
                "test_plan",
                "risk",
                "impact",
                "justification",
                "requested_by",
                "u_subcategory",
                "u_division",
                "u_does_this_change_need_cmdb_update",
                "work_notes",
            ]),
        ),
        (
            "change_request_apply_update".to_string(),
            change_apply_policy(&[
                "short_description",
                "description",
                "type",
                "category",
                "assignment_group",
                "assigned_to",
                "cmdb_ci",
                "start_date",
                "end_date",
                "implementation_plan",
                "change_plan",
                "backout_plan",
                "test_plan",
                "risk",
                "impact",
                "justification",
                "requested_by",
                "u_subcategory",
                "u_division",
                "u_does_this_change_need_cmdb_update",
                "state",
                "work_notes",
            ]),
        ),
        (
            "change_task_apply_create".to_string(),
            change_apply_policy(&[
                "change_request",
                "short_description",
                "description",
                "assignment_group",
                "assigned_to",
                "start_date",
                "end_date",
                "state",
                "work_notes",
            ]),
        ),
        (
            "change_task_apply_update".to_string(),
            change_apply_policy(&[
                "short_description",
                "description",
                "assignment_group",
                "assigned_to",
                "start_date",
                "end_date",
                "state",
                "work_notes",
            ]),
        ),
        (
            "work_note_apply_add".to_string(),
            ToolPolicy {
                enabled: true,
                requires_confirmation: true,
                requires_kb_evidence: false,
                field_allowlist: BTreeSet::from(["work_notes".to_string()]),
                environments: vec!["test".to_string(), "training".to_string()],
                confirmation_ttl_seconds: Some(300),
                ..ToolPolicy::default()
            },
        ),
        (
            "timecard_plan_set_hours".to_string(),
            timecard_plan_policy(),
        ),
        (
            "timecard_apply_set_hours".to_string(),
            timecard_apply_policy(),
        ),
    ])
}

fn story_plan_policy() -> ToolPolicy {
    ToolPolicy {
        enabled: true,
        requires_confirmation: false,
        requires_kb_evidence: false,
        environments: default_story_environments(),
        ..ToolPolicy::default()
    }
}

fn story_apply_policy(field_allowlist: &[&str]) -> ToolPolicy {
    ToolPolicy {
        enabled: false,
        requires_confirmation: true,
        requires_kb_evidence: false,
        field_allowlist: field_allowlist
            .iter()
            .map(|field| (*field).to_string())
            .collect(),
        environments: default_story_environments(),
        confirmation_ttl_seconds: Some(default_confirmation_ttl_seconds()),
        ..ToolPolicy::default()
    }
}

fn default_story_environments() -> Vec<String> {
    vec!["test".to_string(), "training".to_string()]
}

fn change_plan_policy() -> ToolPolicy {
    ToolPolicy {
        enabled: true,
        requires_confirmation: false,
        requires_kb_evidence: false,
        environments: default_story_environments(),
        ..ToolPolicy::default()
    }
}

fn change_apply_policy(field_allowlist: &[&str]) -> ToolPolicy {
    ToolPolicy {
        enabled: false,
        requires_confirmation: true,
        requires_kb_evidence: false,
        field_allowlist: field_allowlist
            .iter()
            .map(|field| (*field).to_string())
            .collect(),
        environments: default_story_environments(),
        confirmation_ttl_seconds: Some(default_confirmation_ttl_seconds()),
        skip_terminal_records: true,
        ..ToolPolicy::default()
    }
}

fn timecard_plan_policy() -> ToolPolicy {
    ToolPolicy {
        enabled: true,
        requires_confirmation: false,
        requires_kb_evidence: false,
        environments: default_story_environments(),
        ..ToolPolicy::default()
    }
}

fn timecard_apply_policy() -> ToolPolicy {
    ToolPolicy {
        enabled: false,
        requires_confirmation: true,
        requires_kb_evidence: false,
        field_allowlist: BTreeSet::from([
            "sunday".to_string(),
            "monday".to_string(),
            "tuesday".to_string(),
            "wednesday".to_string(),
            "thursday".to_string(),
            "friday".to_string(),
            "saturday".to_string(),
        ]),
        environments: default_story_environments(),
        confirmation_ttl_seconds: Some(default_confirmation_ttl_seconds()),
        ..ToolPolicy::default()
    }
}
