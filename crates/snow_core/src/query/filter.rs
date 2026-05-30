use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ResourceType;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SortDirection {
    #[default]
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SortField {
    pub field: String,
    #[serde(default)]
    pub direction: SortDirection,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BusinessApplicationQuery {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default)]
    pub filters: Vec<FieldFilter>,
    #[serde(default)]
    pub include_tombstoned: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub offset: Option<usize>,
    #[serde(default)]
    pub sort: Vec<SortField>,
    #[serde(default)]
    pub allow_unknown_fields: bool,
}

impl Default for BusinessApplicationQuery {
    fn default() -> Self {
        Self {
            text: None,
            filters: Vec::new(),
            include_tombstoned: false,
            limit: Some(20),
            offset: None,
            sort: Vec::new(),
            allow_unknown_fields: false,
        }
    }
}

impl BusinessApplicationQuery {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn text(mut self, text: impl Into<String>) -> Self {
        self.text = Some(text.into());
        self
    }

    pub fn filter(mut self, field: impl Into<String>, op: FieldOperator, value: Value) -> Self {
        self.filters.push(FieldFilter {
            field: field.into(),
            op,
            value,
        });
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = Some(offset);
        self
    }

    pub fn sort_by(mut self, field: impl Into<String>, direction: SortDirection) -> Self {
        self.sort.push(SortField {
            field: field.into(),
            direction,
        });
        self
    }

    pub fn allow_unknown_fields(mut self, allow: bool) -> Self {
        self.allow_unknown_fields = allow;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldFilter {
    pub field: String,
    pub op: FieldOperator,
    pub value: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldOperator {
    Eq,
    Ne,
    Contains,
    StartsWith,
    In,
    IsEmpty,
    IsNotEmpty,
    Gt,
    Gte,
    Lt,
    Lte,
}

impl SortField {
    pub fn asc(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            direction: SortDirection::Asc,
        }
    }

    pub fn desc(field: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            direction: SortDirection::Desc,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ListQuery {
    pub resource_type: Option<ResourceType>,
    pub number: Option<String>,
    pub numbers: Vec<String>,
    pub states: Vec<String>,
    pub assigned_to: Option<String>,
    pub parent_sys_id: Option<String>,
    pub include_tombstoned: bool,
    pub limit: Option<usize>,
    pub sort: Vec<SortField>,
}

impl ListQuery {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn active_tasks() -> Self {
        Self {
            resource_type: None,
            number: None,
            numbers: Vec::new(),
            states: Vec::new(),
            assigned_to: None,
            parent_sys_id: None,
            include_tombstoned: false,
            limit: None,
            sort: vec![SortField::asc("number")],
        }
    }

    pub fn resource_type(mut self, resource_type: ResourceType) -> Self {
        self.resource_type = Some(resource_type);
        self
    }

    pub fn number(mut self, number: impl Into<String>) -> Self {
        self.number = Some(number.into());
        self
    }

    pub fn numbers(mut self, numbers: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.numbers = numbers.into_iter().map(Into::into).collect();
        self
    }

    pub fn state(mut self, state: impl Into<String>) -> Self {
        self.states.push(state.into());
        self
    }

    pub fn states(mut self, states: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.states = states.into_iter().map(Into::into).collect();
        self
    }

    pub fn assigned_to(mut self, sys_id: impl Into<String>) -> Self {
        self.assigned_to = Some(sys_id.into());
        self
    }

    pub fn parent_sys_id(mut self, sys_id: impl Into<String>) -> Self {
        self.parent_sys_id = Some(sys_id.into());
        self
    }

    pub fn include_tombstoned(mut self, include: bool) -> Self {
        self.include_tombstoned = include;
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn sort_by(mut self, field: impl Into<String>, direction: SortDirection) -> Self {
        self.sort.push(SortField {
            field: field.into(),
            direction,
        });
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ApprovalQuery {
    pub approver_sys_id: Option<String>,
    pub target_sys_id: Option<String>,
    pub states: Vec<String>,
    pub include_tombstoned: bool,
    pub limit: Option<usize>,
}

impl ApprovalQuery {
    pub fn pending() -> Self {
        Self {
            approver_sys_id: None,
            target_sys_id: None,
            states: vec!["requested".to_string()],
            include_tombstoned: false,
            limit: None,
        }
    }

    pub fn approver_sys_id(mut self, sys_id: impl Into<String>) -> Self {
        self.approver_sys_id = Some(sys_id.into());
        self
    }

    pub fn target_sys_id(mut self, sys_id: impl Into<String>) -> Self {
        self.target_sys_id = Some(sys_id.into());
        self
    }

    pub fn state(mut self, state: impl Into<String>) -> Self {
        self.states.push(state.into());
        self
    }

    pub fn include_tombstoned(mut self, include: bool) -> Self {
        self.include_tombstoned = include;
        self
    }

    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }
}
