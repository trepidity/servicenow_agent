use serde::{Deserialize, Serialize};

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
