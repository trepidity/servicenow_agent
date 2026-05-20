use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct RecordRef {
    pub sys_id: String,
    pub number: String,
    pub table: String,
}

impl From<snow_core::RecordRef> for RecordRef {
    fn from(value: snow_core::RecordRef) -> Self {
        Self {
            sys_id: value.sys_id,
            number: value.number,
            table: value.table,
        }
    }
}

impl From<RecordRef> for snow_core::RecordRef {
    fn from(value: RecordRef) -> Self {
        Self {
            sys_id: value.sys_id,
            number: value.number,
            table: value.table,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct UserRef {
    pub sys_id: String,
    pub user_name: Option<String>,
    pub email: Option<String>,
    pub display_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct GroupRef {
    pub sys_id: String,
    pub name: String,
    pub source: GroupSource,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum GroupSource {
    ServiceNow,
    AdRegistry,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct EnvironmentRef {
    pub label: String,
    pub instance_timezone: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct IdempotencyKey {
    pub value: String,
    pub source: IdempotencyKeySource,
}

impl IdempotencyKey {
    pub fn client(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            source: IdempotencyKeySource::Client,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum IdempotencyKeySource {
    Client,
    ServerDerived,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Citation {
    pub article_number: String,
    pub sys_id: String,
    pub title: String,
    pub knowledge_base: String,
    pub updated: DateTime<Utc>,
    pub section_heading: Option<String>,
    pub url_fragment: Option<String>,
    pub content_hash: String,
}
