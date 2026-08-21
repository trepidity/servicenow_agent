use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use servicenow_rs::prelude::Record;

use crate::{OperationEnvelope, SnowCore};

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

/// Native payload for the named `catalog_item_get` operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogItemGetData {
    pub item: CatalogItem,
}

/// Native payload for the named `catalog_items_search` operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CatalogItemsSearchData {
    pub items: Vec<CatalogItem>,
}

pub(crate) fn catalog_item_from_record(
    record: &Record,
    variables: Vec<CatalogVariable>,
) -> CatalogItem {
    let name = record
        .get_str("name")
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(&record.sys_id)
        .to_string();
    let short_description = record
        .get_str("short_description")
        .unwrap_or_default()
        .to_string();
    let table = record
        .get_raw("sys_class_name")
        .or_else(|| record.get_display("sys_class_name"))
        .unwrap_or("sc_cat_item")
        .to_string();
    CatalogItem {
        sys_id: record.sys_id.clone(),
        name,
        short_description,
        table,
        variables,
    }
}

impl SnowCore {
    /// Read one complete catalog product under the daemon-owned cache policy.
    pub async fn catalog_item_get_envelope(
        &self,
        sys_id: &str,
    ) -> Result<OperationEnvelope<CatalogItemGetData>> {
        self.writes.catalog_item_get_envelope(sys_id).await
    }

    /// Search the intentionally narrowed catalog-product projection.
    pub async fn catalog_items_search_envelope(
        &self,
        query: &str,
        limit: u32,
    ) -> Result<OperationEnvelope<CatalogItemsSearchData>> {
        self.writes
            .catalog_items_search_envelope(query, limit)
            .await
    }
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
