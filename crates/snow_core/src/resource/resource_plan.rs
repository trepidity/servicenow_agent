use anyhow::{Result, anyhow};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use servicenow_rs::prelude::Record;

use crate::{SnowRecord, parse_i64};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourcePlanState {
    Planning,
    Allocated,
    Other(u16),
}

impl ResourcePlanState {
    pub const KNOWN: [ResourcePlanState; 2] =
        [ResourcePlanState::Planning, ResourcePlanState::Allocated];

    pub const fn raw(self) -> u16 {
        match self {
            ResourcePlanState::Planning => 1,
            ResourcePlanState::Allocated => 3,
            ResourcePlanState::Other(value) => value,
        }
    }

    pub const fn label(self) -> Option<&'static str> {
        match self {
            ResourcePlanState::Planning => Some("Planning"),
            ResourcePlanState::Allocated => Some("Allocated"),
            ResourcePlanState::Other(_) => None,
        }
    }

    pub fn parse(input: &str) -> Option<Self> {
        let input = input.trim();
        if input.is_empty() {
            return None;
        }
        if input.eq_ignore_ascii_case("planning") {
            return Some(Self::Planning);
        }
        if input.eq_ignore_ascii_case("allocated") {
            return Some(Self::Allocated);
        }
        let raw = input.parse::<u16>().ok()?;
        Some(match raw {
            1 => Self::Planning,
            3 => Self::Allocated,
            other => Self::Other(other),
        })
    }
}

impl Serialize for ResourcePlanState {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u16(self.raw())
    }
}

impl<'de> Deserialize<'de> for ResourcePlanState {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct Visitor;

        impl<'de> de::Visitor<'de> for Visitor {
            type Value = ResourcePlanState;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a resource_plan state raw integer or known label")
            }

            fn visit_u64<E>(self, value: u64) -> std::result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                let value = u16::try_from(value)
                    .map_err(|_| E::custom("resource_plan state exceeds u16 range"))?;
                Ok(ResourcePlanState::parse(&value.to_string()).expect("u16 parses"))
            }

            fn visit_i64<E>(self, value: i64) -> std::result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                let value = u16::try_from(value)
                    .map_err(|_| E::custom("resource_plan state must be non-negative u16"))?;
                Ok(ResourcePlanState::parse(&value.to_string()).expect("u16 parses"))
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                ResourcePlanState::parse(value)
                    .ok_or_else(|| E::custom("unknown resource_plan state label"))
            }
        }

        deserializer.deserialize_any(Visitor)
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResourcePlanParentType {
    Demand,
    Project,
}

impl ResourcePlanParentType {
    pub const fn table_name(self) -> &'static str {
        match self {
            Self::Demand => "dmn_demand",
            Self::Project => "pm_project",
        }
    }

    pub fn parse(input: &str) -> Option<Self> {
        let input = input.trim();
        if input.eq_ignore_ascii_case("demand") || input.eq_ignore_ascii_case("dmn_demand") {
            Some(Self::Demand)
        } else if input.eq_ignore_ascii_case("project") || input.eq_ignore_ascii_case("pm_project")
        {
            Some(Self::Project)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResourcePlanResourceType {
    Group,
    User,
}

impl ResourcePlanResourceType {
    pub const fn table_name(self) -> &'static str {
        match self {
            Self::Group => "sys_user_group",
            Self::User => "sys_user",
        }
    }

    pub const fn as_snow_str(self) -> &'static str {
        match self {
            Self::Group => "group",
            Self::User => "user",
        }
    }

    pub fn parse(input: &str) -> Option<Self> {
        let input = input.trim();
        if input.eq_ignore_ascii_case("group") || input.eq_ignore_ascii_case("sys_user_group") {
            Some(Self::Group)
        } else if input.eq_ignore_ascii_case("user") || input.eq_ignore_ascii_case("sys_user") {
            Some(Self::User)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ResourcePlanListInput {
    #[serde(default)]
    pub parent_number: Option<String>,
    #[serde(default)]
    pub task_sys_id: Option<String>,
    #[serde(default)]
    pub resource_sys_id: Option<String>,
    #[serde(default)]
    pub resource_type: Option<ResourcePlanResourceType>,
    #[serde(default)]
    pub state: Option<ResourcePlanStateFilter>,
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum ResourcePlanStateFilter {
    Single(i64),
    Multiple(Vec<i64>),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResourcePlanParentRef {
    pub sys_id: String,
    pub number: Option<String>,
    pub table: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResourcePlanResourceRef {
    pub sys_id: String,
    pub display_name: Option<String>,
    pub table: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResourcePlanRecord {
    pub sys_id: String,
    pub number: String,
    pub browser_url: Option<String>,
    pub vault_relative_path: Option<String>,
    pub parent: Option<ResourcePlanParentRef>,
    pub resource: Option<ResourcePlanResourceRef>,
    pub state: Option<String>,
    pub state_label: Option<String>,
    pub planned_hours: Option<f64>,
    pub notes: Option<String>,
    pub context: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResourcePlanListWarning {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResourcePlanQuerySummary {
    pub filters_applied: Vec<String>,
    pub total_returned: usize,
    pub limit: usize,
    pub truncated: bool,
    #[serde(default)]
    pub warnings: Vec<ResourcePlanListWarning>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResourcePlanListResponse {
    pub records: Vec<ResourcePlanRecord>,
    pub query_summary: ResourcePlanQuerySummary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourcePlanListError {
    InvalidParams(String),
}

impl std::fmt::Display for ResourcePlanListError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidParams(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for ResourcePlanListError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskSelector {
    Number(String),
    SysId(String),
    None,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedResourceFilter {
    Group(String),
    User(String),
    TypeOnly(ResourcePlanResourceType),
    None,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedListQuery {
    pub task_selector: TaskSelector,
    pub resource: ResolvedResourceFilter,
    pub states: Vec<String>,
    pub effective_limit: usize,
    pub warnings: Vec<ResourcePlanListWarning>,
    pub filters_applied: Vec<String>,
}

pub const RESOURCE_PLAN_LIST_DEFAULT_LIMIT: usize = 50;
pub const RESOURCE_PLAN_LIST_MAX_LIMIT: usize = 200;

pub fn validate_list_input(
    input: ResourcePlanListInput,
) -> std::result::Result<ValidatedListQuery, ResourcePlanListError> {
    if input.parent_number.is_some() && input.task_sys_id.is_some() {
        return Err(ResourcePlanListError::InvalidParams(
            "parent_number and task_sys_id are mutually exclusive".to_string(),
        ));
    }
    if let Some(parent_number) = input.parent_number.as_deref()
        && !supported_parent_number_prefix(parent_number)
    {
        return Err(ResourcePlanListError::InvalidParams(
            "parent_number must start with DMND or PRJ".to_string(),
        ));
    }
    if input.resource_sys_id.is_some() && input.resource_type.is_none() {
        return Err(ResourcePlanListError::InvalidParams(
            "resource_sys_id requires resource_type".to_string(),
        ));
    }

    let mut filters_applied = Vec::new();
    let task_selector = if let Some(parent_number) = input.parent_number {
        filters_applied.push("parent_number".to_string());
        TaskSelector::Number(parent_number)
    } else if let Some(task_sys_id) = input.task_sys_id {
        filters_applied.push("task_sys_id".to_string());
        TaskSelector::SysId(task_sys_id)
    } else {
        TaskSelector::None
    };

    let resource = match (input.resource_sys_id, input.resource_type) {
        (Some(sys_id), Some(ResourcePlanResourceType::Group)) => {
            filters_applied.push("resource_sys_id".to_string());
            filters_applied.push("resource_type".to_string());
            ResolvedResourceFilter::Group(sys_id)
        }
        (Some(sys_id), Some(ResourcePlanResourceType::User)) => {
            filters_applied.push("resource_sys_id".to_string());
            filters_applied.push("resource_type".to_string());
            ResolvedResourceFilter::User(sys_id)
        }
        (None, Some(resource_type)) => {
            filters_applied.push("resource_type".to_string());
            ResolvedResourceFilter::TypeOnly(resource_type)
        }
        (None, None) => ResolvedResourceFilter::None,
        (Some(_), None) => unreachable!("resource_sys_id without resource_type is rejected above"),
    };

    let states = match input.state {
        Some(ResourcePlanStateFilter::Single(value)) => {
            let value = validate_state_filter_value(value)?;
            filters_applied.push("state".to_string());
            vec![value]
        }
        Some(ResourcePlanStateFilter::Multiple(values)) => {
            if values.is_empty() {
                return Err(ResourcePlanListError::InvalidParams(
                    "state filter must not be empty".to_string(),
                ));
            }
            let values = values
                .into_iter()
                .map(validate_state_filter_value)
                .collect::<std::result::Result<Vec<_>, _>>()?;
            filters_applied.push("state".to_string());
            values
        }
        None => Vec::new(),
    };

    let mut warnings = Vec::new();
    let effective_limit = match input.limit {
        Some(0) => {
            return Err(ResourcePlanListError::InvalidParams(
                "limit must be greater than zero".to_string(),
            ));
        }
        Some(limit) if limit > RESOURCE_PLAN_LIST_MAX_LIMIT => {
            warnings.push(ResourcePlanListWarning {
                code: "LIMIT_CLAMPED".to_string(),
                message: format!(
                    "limit was clamped to {RESOURCE_PLAN_LIST_MAX_LIMIT} resource plans"
                ),
            });
            RESOURCE_PLAN_LIST_MAX_LIMIT
        }
        Some(limit) => limit,
        None => RESOURCE_PLAN_LIST_DEFAULT_LIMIT,
    };

    Ok(ValidatedListQuery {
        task_selector,
        resource,
        states,
        effective_limit,
        warnings,
        filters_applied,
    })
}

fn supported_parent_number_prefix(number: &str) -> bool {
    let prefix = number
        .trim()
        .chars()
        .take_while(|ch| ch.is_ascii_alphabetic())
        .collect::<String>()
        .to_ascii_uppercase();
    matches!(prefix.as_str(), "DMND" | "PRJ")
}

fn validate_state_filter_value(value: i64) -> std::result::Result<String, ResourcePlanListError> {
    let value = u16::try_from(value).map_err(|_| {
        ResourcePlanListError::InvalidParams(
            "state filter values must be non-negative u16 integers".to_string(),
        )
    })?;
    Ok(value.to_string())
}

pub fn resource_plan_record_from_row(
    record: &Record,
    resource_type_hint: Option<ResourcePlanResourceType>,
) -> ResourcePlanRecord {
    let row_resource_type = || {
        non_empty(record.get_raw("resource_type"))
            .or_else(|| non_empty(record.get_str("resource_type")))
            .and_then(ResourcePlanResourceType::parse)
    };
    let resource = resource_type_hint
        .and_then(|resource_type| resource_ref_from_row(record, resource_type))
        .or_else(|| {
            row_resource_type()
                .and_then(|resource_type| resource_ref_from_row(record, resource_type))
        });

    ResourcePlanRecord {
        sys_id: non_empty(record.get_raw("sys_id"))
            .or_else(|| non_empty(record.get_str("sys_id")))
            .unwrap_or(&record.sys_id)
            .to_string(),
        number: non_empty(record.get_raw("number"))
            .or_else(|| non_empty(record.get_str("number")))
            .unwrap_or_default()
            .to_string(),
        browser_url: None,
        vault_relative_path: None,
        parent: parent_ref_from_row(record),
        resource,
        state: non_empty(record.get_raw("state"))
            .or_else(|| non_empty(record.get_str("state")))
            .map(ToOwned::to_owned),
        state_label: non_empty(record.get_display("state")).map(ToOwned::to_owned),
        planned_hours: non_empty(record.get_raw("planned_hours"))
            .or_else(|| non_empty(record.get_str("planned_hours")))
            .and_then(|value| value.parse::<f64>().ok()),
        notes: non_empty(record.get_raw("notes"))
            .or_else(|| non_empty(record.get_str("notes")))
            .map(ToOwned::to_owned),
        context: non_empty(record.get_raw("u_description"))
            .or_else(|| non_empty(record.get_str("u_description")))
            .map(ToOwned::to_owned),
    }
}

fn parent_ref_from_row(record: &Record) -> Option<ResourcePlanParentRef> {
    let sys_id = non_empty(record.get_raw("task")).or_else(|| non_empty(record.get_str("task")))?;
    Some(ResourcePlanParentRef {
        sys_id: sys_id.to_string(),
        number: non_empty(record.get_raw("task.number"))
            .or_else(|| non_empty(record.get_str("task.number")))
            .map(ToOwned::to_owned),
        table: non_empty(record.get_raw("task.sys_class_name"))
            .or_else(|| non_empty(record.get_str("task.sys_class_name")))
            .map(ToOwned::to_owned),
    })
}

fn resource_ref_from_row(
    record: &Record,
    resource_type: ResourcePlanResourceType,
) -> Option<ResourcePlanResourceRef> {
    let column = match resource_type {
        ResourcePlanResourceType::Group => "group_resource",
        ResourcePlanResourceType::User => "user_resource",
    };
    let sys_id = non_empty(record.get_raw(column)).or_else(|| non_empty(record.get_str(column)))?;
    Some(ResourcePlanResourceRef {
        sys_id: sys_id.to_string(),
        display_name: non_empty(record.get_display(column)).map(ToOwned::to_owned),
        table: resource_type.table_name().to_string(),
    })
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.and_then(|value| {
        let value = value.trim();
        (!value.is_empty()).then_some(value)
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResourcePlanWriteConcurrency {
    pub sys_updated_on: String,
    pub sys_mod_count: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResourcePlanWriteResult {
    pub record: SnowRecord,
    pub concurrency: ResourcePlanWriteConcurrency,
}

#[derive(Debug, Clone, Default)]
pub struct ResourcePlanResource;

impl ResourcePlanResource {
    pub const TABLE: &str = "resource_plan";

    pub fn write_result_from_fresh_row(
        record: SnowRecord,
        fresh_row: &Record,
    ) -> Result<ResourcePlanWriteResult> {
        Ok(ResourcePlanWriteResult {
            record,
            concurrency: ResourcePlanWriteConcurrency::from_fresh_row(fresh_row)?,
        })
    }
}

impl ResourcePlanWriteConcurrency {
    pub fn from_fresh_row(record: &Record) -> Result<Self> {
        let sys_updated_on = record
            .get_raw("sys_updated_on")
            .or_else(|| record.get_str("sys_updated_on"))
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                anyhow!(
                    "fresh {} row {} did not include sys_updated_on",
                    record.table,
                    record.sys_id
                )
            })?
            .to_string();

        let sys_mod_count = record
            .get_raw("sys_mod_count")
            .or_else(|| record.get_str("sys_mod_count"));
        let sys_mod_count = parse_i64(sys_mod_count);

        Ok(Self {
            sys_updated_on,
            sys_mod_count,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use servicenow_rs::prelude::DisplayValue;

    #[test]
    fn state_parses_known_and_passes_through_unknown() {
        assert_eq!(
            ResourcePlanState::parse("1"),
            Some(ResourcePlanState::Planning)
        );
        assert_eq!(
            ResourcePlanState::parse("3"),
            Some(ResourcePlanState::Allocated)
        );
        assert_eq!(
            ResourcePlanState::parse("planning"),
            Some(ResourcePlanState::Planning)
        );
        assert_eq!(
            ResourcePlanState::parse("PLANNING"),
            Some(ResourcePlanState::Planning)
        );
        assert_eq!(
            ResourcePlanState::parse("allocated"),
            Some(ResourcePlanState::Allocated)
        );
        assert_eq!(
            ResourcePlanState::parse("99"),
            Some(ResourcePlanState::Other(99))
        );
        assert_eq!(ResourcePlanState::parse("bogus"), None);
    }

    #[test]
    fn state_label_none_for_other() {
        assert_eq!(ResourcePlanState::Other(99).label(), None);
        assert_eq!(ResourcePlanState::Planning.label(), Some("Planning"));
    }

    #[test]
    fn state_never_panics_on_large_int() {
        assert_eq!(
            ResourcePlanState::parse("65535"),
            Some(ResourcePlanState::Other(65535))
        );
        assert_eq!(ResourcePlanState::parse("65536"), None);
    }

    #[test]
    fn state_serializes_as_raw_integer() {
        assert_eq!(
            serde_json::to_value(ResourcePlanState::Allocated).expect("serialize"),
            serde_json::json!(3)
        );
        assert_eq!(
            serde_json::from_value::<ResourcePlanState>(serde_json::json!("99"))
                .expect("deserialize"),
            ResourcePlanState::Other(99)
        );
    }

    #[test]
    fn parent_type_table_name() {
        assert_eq!(ResourcePlanParentType::Demand.table_name(), "dmn_demand");
        assert_eq!(ResourcePlanParentType::Project.table_name(), "pm_project");
        assert_eq!(
            ResourcePlanParentType::parse("demand"),
            Some(ResourcePlanParentType::Demand)
        );
        assert_eq!(ResourcePlanParentType::parse("demand_task"), None);
    }

    #[test]
    fn resource_type_table_name() {
        assert_eq!(
            ResourcePlanResourceType::Group.table_name(),
            "sys_user_group"
        );
        assert_eq!(ResourcePlanResourceType::User.table_name(), "sys_user");
        assert_eq!(ResourcePlanResourceType::Group.as_snow_str(), "group");
        assert_eq!(ResourcePlanResourceType::User.as_snow_str(), "user");
        assert_eq!(
            ResourcePlanResourceType::parse("group"),
            Some(ResourcePlanResourceType::Group)
        );
        assert_eq!(
            ResourcePlanResourceType::parse("user"),
            Some(ResourcePlanResourceType::User)
        );
    }

    fn input() -> ResourcePlanListInput {
        ResourcePlanListInput::default()
    }

    fn resource_plan_record_from_json(value: serde_json::Value) -> Record {
        Record::from_json("resource_plan", &value, DisplayValue::Both).expect("valid record")
    }

    #[test]
    fn list_input_accepts_single_and_array_state() {
        let single: ResourcePlanListInput =
            serde_json::from_value(serde_json::json!({ "state": 3 })).unwrap();
        assert!(matches!(
            single.state,
            Some(ResourcePlanStateFilter::Single(3))
        ));

        let multi: ResourcePlanListInput =
            serde_json::from_value(serde_json::json!({ "state": [1, 3] })).unwrap();
        assert!(matches!(
            multi.state,
            Some(ResourcePlanStateFilter::Multiple(_))
        ));
    }

    #[test]
    fn list_validation_rejects_negative_and_out_of_range_state() {
        let mut negative = input();
        negative.state = Some(ResourcePlanStateFilter::Single(-5));
        assert!(validate_list_input(negative).is_err());

        let mut too_large = input();
        too_large.state = Some(ResourcePlanStateFilter::Multiple(vec![1, 65_536]));
        assert!(validate_list_input(too_large).is_err());
    }

    #[test]
    fn list_input_rejects_unknown_field() {
        let err = serde_json::from_value::<ResourcePlanListInput>(
            serde_json::json!({ "parent_table": "dmn_demand" }),
        );
        assert!(
            err.is_err(),
            "unknown field must be rejected by deny_unknown_fields"
        );
    }

    #[test]
    fn list_record_serializes_optional_fields_as_null() {
        let rec = ResourcePlanRecord {
            sys_id: "00000000000000000000000000000001".into(),
            number: "<RPLN_NUMBER>".into(),
            browser_url: None,
            vault_relative_path: None,
            parent: None,
            resource: None,
            state: Some("3".into()),
            state_label: Some("Allocated".into()),
            planned_hours: Some(32.0),
            notes: None,
            context: None,
        };
        let value = serde_json::to_value(&rec).unwrap();
        assert!(value.get("vault_relative_path").unwrap().is_null());
        assert!(value.get("context").unwrap().is_null());
        assert_eq!(value.get("state_label").unwrap(), "Allocated");
    }

    #[test]
    fn list_validation_rejects_invalid_combinations() {
        let mut i = input();
        i.parent_number = Some("<PRJ_NUMBER>".into());
        i.task_sys_id = Some("00000000000000000000000000000001".into());
        assert!(matches!(
            validate_list_input(i),
            Err(ResourcePlanListError::InvalidParams(_))
        ));

        let mut i = input();
        i.resource_sys_id = Some("00000000000000000000000000000002".into());
        assert!(matches!(
            validate_list_input(i),
            Err(ResourcePlanListError::InvalidParams(_))
        ));

        let mut i = input();
        i.state = Some(ResourcePlanStateFilter::Multiple(vec![]));
        assert!(matches!(
            validate_list_input(i),
            Err(ResourcePlanListError::InvalidParams(_))
        ));

        let mut i = input();
        i.limit = Some(0);
        assert!(matches!(
            validate_list_input(i),
            Err(ResourcePlanListError::InvalidParams(_))
        ));
    }

    #[test]
    fn list_validation_rejects_unknown_parent_number_prefix() {
        let mut i = input();
        i.parent_number = Some("UNKNOWN-PARENT".to_string());

        let err = validate_list_input(i).expect_err("invalid parent prefix");

        assert_eq!(err.to_string(), "parent_number must start with DMND or PRJ");
    }

    #[test]
    fn list_validation_clamps_and_normalizes_filters() {
        let mut i = input();
        i.resource_sys_id = Some("00000000000000000000000000000003".into());
        i.resource_type = Some(ResourcePlanResourceType::Group);
        i.state = Some(ResourcePlanStateFilter::Multiple(vec![1, 3]));
        i.limit = Some(201);

        let v = validate_list_input(i).unwrap();

        assert_eq!(v.effective_limit, 200);
        assert_eq!(v.states, vec!["1".to_string(), "3".to_string()]);
        assert!(matches!(v.resource, ResolvedResourceFilter::Group(_)));
        assert!(v.warnings.iter().any(|w| w.code == "LIMIT_CLAMPED"));
        assert!(v.filters_applied.contains(&"resource_sys_id".to_string()));
        assert!(v.filters_applied.contains(&"resource_type".to_string()));
        assert!(v.filters_applied.contains(&"state".to_string()));
    }

    #[test]
    fn list_validation_defaults_and_allows_resource_type_only() {
        assert_eq!(validate_list_input(input()).unwrap().effective_limit, 50);

        let mut i = input();
        i.resource_type = Some(ResourcePlanResourceType::Group);
        let v = validate_list_input(i).unwrap();
        assert!(matches!(
            v.resource,
            ResolvedResourceFilter::TypeOnly(ResourcePlanResourceType::Group)
        ));
        assert!(v.filters_applied.contains(&"resource_type".to_string()));
    }

    #[test]
    fn hydrates_group_plan_with_parent_dotwalk() {
        let rec = resource_plan_record_from_json(serde_json::json!({
            "sys_id": { "value": "00000000000000000000000000000001", "display_value": "00000000000000000000000000000001" },
            "number": { "value": "<RPLN_NUMBER>", "display_value": "<RPLN_NUMBER>" },
            "state": { "value": "3", "display_value": "Allocated" },
            "task": { "value": "00000000000000000000000000000010", "display_value": "<PARENT_DISPLAY>" },
            "task.number": { "value": "<PRJ_NUMBER>", "display_value": "<PRJ_NUMBER>" },
            "task.sys_class_name": { "value": "pm_project", "display_value": "Project" },
            "resource_type": { "value": "group", "display_value": "Group" },
            "group_resource": { "value": "00000000000000000000000000000020", "display_value": "<GROUP_DISPLAY>" },
            "user_resource": { "value": "", "display_value": "" },
            "planned_hours": { "value": "32", "display_value": "32" },
            "notes": { "value": "<NOTES>", "display_value": "<NOTES>" },
            "description": { "value": "", "display_value": "" },
            "u_description": { "value": "<CONTEXT>", "display_value": "<CONTEXT>" }
        }));
        let out = resource_plan_record_from_row(&rec, Some(ResourcePlanResourceType::Group));

        assert_eq!(out.state.as_deref(), Some("3"));
        assert_eq!(out.state_label.as_deref(), Some("Allocated"));
        assert_eq!(out.planned_hours, Some(32.0));
        assert_eq!(out.notes.as_deref(), Some("<NOTES>"));
        assert_eq!(out.context.as_deref(), Some("<CONTEXT>"));
        let parent = out.parent.unwrap();
        assert_eq!(parent.sys_id, "00000000000000000000000000000010");
        assert_eq!(parent.number.as_deref(), Some("<PRJ_NUMBER>"));
        assert_eq!(parent.table.as_deref(), Some("pm_project"));
        let resource = out.resource.unwrap();
        assert_eq!(resource.sys_id, "00000000000000000000000000000020");
        assert_eq!(resource.display_name.as_deref(), Some("<GROUP_DISPLAY>"));
        assert_eq!(resource.table, "sys_user_group");
    }

    #[test]
    fn hydrates_demand_parent_class_from_dotwalk() {
        let rec = resource_plan_record_from_json(serde_json::json!({
            "sys_id": { "value": "00000000000000000000000000000001" },
            "number": { "value": "<RPLN_NUMBER>" },
            "task": { "value": "00000000000000000000000000000011" },
            "task.number": { "value": "<DMND_NUMBER>" },
            "task.sys_class_name": { "value": "dmn_demand" }
        }));
        let out = resource_plan_record_from_row(&rec, None);
        let parent = out.parent.unwrap();
        assert_eq!(parent.table.as_deref(), Some("dmn_demand"));
        assert_eq!(parent.number.as_deref(), Some("<DMND_NUMBER>"));
    }

    #[test]
    fn state_label_null_when_display_value_absent() {
        let rec = resource_plan_record_from_json(serde_json::json!({
            "sys_id": { "value": "00000000000000000000000000000001" },
            "number": { "value": "<RPLN_NUMBER>" },
            "state": { "value": "3" }
        }));
        let out = resource_plan_record_from_row(&rec, None);
        assert_eq!(out.state.as_deref(), Some("3"));
        assert_eq!(out.state_label, None);
    }

    #[test]
    fn user_plan_uses_user_resource_column() {
        let rec = resource_plan_record_from_json(serde_json::json!({
            "sys_id": { "value": "00000000000000000000000000000001" },
            "number": { "value": "<RPLN_NUMBER>" },
            "resource_type": { "value": "user" },
            "user_resource": { "value": "00000000000000000000000000000030", "display_value": "<USER_DISPLAY>" }
        }));
        let out = resource_plan_record_from_row(&rec, Some(ResourcePlanResourceType::User));
        let resource = out.resource.unwrap();
        assert_eq!(resource.sys_id, "00000000000000000000000000000030");
        assert_eq!(resource.display_name.as_deref(), Some("<USER_DISPLAY>"));
        assert_eq!(resource.table, "sys_user");
    }

    #[test]
    fn resource_hint_falls_back_to_row_resource_type_when_hint_column_empty() {
        let rec = resource_plan_record_from_json(serde_json::json!({
            "sys_id": { "value": "00000000000000000000000000000001" },
            "number": { "value": "<RPLN_NUMBER>" },
            "resource_type": { "value": "user" },
            "group_resource": { "value": "", "display_value": "" },
            "user_resource": { "value": "00000000000000000000000000000030", "display_value": "<USER_DISPLAY>" }
        }));
        let out = resource_plan_record_from_row(&rec, Some(ResourcePlanResourceType::Group));
        let resource = out.resource.unwrap();
        assert_eq!(resource.sys_id, "00000000000000000000000000000030");
        assert_eq!(resource.table, "sys_user");
    }

    #[test]
    fn captures_concurrency_from_fresh_row() {
        let record = Record::from_json(
            "resource_plan",
            &serde_json::json!({
                "sys_id": "rpln-sys",
                "number": "<RPLN_NUMBER>",
                "sys_updated_on": "2026-06-24 10:11:12",
                "sys_mod_count": "24"
            }),
            DisplayValue::Raw,
        )
        .unwrap();

        let concurrency = ResourcePlanWriteConcurrency::from_fresh_row(&record).unwrap();

        assert_eq!(concurrency.sys_updated_on, "2026-06-24 10:11:12");
        assert_eq!(concurrency.sys_mod_count, Some(24));
    }

    #[test]
    fn concurrency_falls_back_when_mod_count_absent() {
        let record = Record::from_json(
            "resource_plan",
            &serde_json::json!({
                "sys_id": "rpln-sys",
                "number": "<RPLN_NUMBER>",
                "sys_updated_on": "2026-06-24 10:11:12"
            }),
            DisplayValue::Raw,
        )
        .unwrap();

        let concurrency = ResourcePlanWriteConcurrency::from_fresh_row(&record).unwrap();

        assert_eq!(concurrency.sys_updated_on, "2026-06-24 10:11:12");
        assert_eq!(concurrency.sys_mod_count, None);
    }
}
