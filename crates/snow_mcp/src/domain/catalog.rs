use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::primitives::{Citation, RecordRef, UserRef};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct CatalogItemRef {
    pub sys_id: String,
    pub name: String,
    pub short_description: String,
    pub table: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogVariable {
    pub name: String,
    pub label: String,
    pub variable_type: String,
    pub mandatory: bool,
    pub default_value: Option<String>,
    pub choice_list: Option<Vec<CatalogChoice>>,
    pub regex: Option<String>,
    pub max_length: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct CatalogChoice {
    pub value: String,
    pub label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogRequestPlan {
    pub item: CatalogItemRef,
    pub requested_for: UserRef,
    pub variables: Vec<CatalogVariableValue>,
    pub citations: Vec<Citation>,
    pub justification: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogVariableValue {
    pub name: String,
    pub value: String,
    pub resolved_from: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogReceipt {
    pub req: RecordRef,
    pub items: Vec<RequestedItem>,
    pub submitted_at: DateTime<Utc>,
    pub submitted_variables: Vec<CatalogVariableValue>,
    pub audit_id: String,
    pub browser_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RequestedItem {
    pub record: RecordRef,
    pub catalog_item: CatalogItemRef,
    pub stage: String,
    pub state: String,
}
