use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogItem {
    pub sys_id: String,
    pub name: String,
    pub short_description: String,
    pub table: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub variables: Vec<CatalogVariable>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogVariable {
    pub sys_id: String,
    pub name: String,
    pub label: String,
    pub variable_type: String,
    pub mandatory: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reference_table: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lookup_table: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_length: Option<u32>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub choices: Vec<CatalogChoice>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogChoice {
    pub value: String,
    pub label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CatalogSubmitResult {
    pub item_sys_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sys_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub number: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_table: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_sys_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_number: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_item_sys_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_item_number: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub browser_url: Option<String>,
    pub raw_result: Value,
}
