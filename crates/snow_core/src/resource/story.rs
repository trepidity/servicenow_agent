use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};

use crate::{RecordRef, ResourceType, SnowRecord};
use servicenow_rs::prelude::Record;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoryRelationSpec {
    pub parent_table: &'static str,
    pub child_table: &'static str,
    pub child_link_field: &'static str,
    pub parent_resource_type: ResourceType,
    pub child_resource_type: ResourceType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoryChildRefreshPlan {
    pub parent: RecordRef,
    pub relation: StoryRelationSpec,
    pub query: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoryWriteConcurrency {
    pub sys_updated_on: String,
    pub sys_mod_count: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoryWriteResult {
    pub record: SnowRecord,
    pub concurrency: StoryWriteConcurrency,
}

#[derive(Debug, Clone, Default)]
pub struct StoryResource;

impl StoryResource {
    pub const PARENT_TABLE: &str = "rm_story";
    pub const CHILD_TABLE: &str = "rm_scrum_task";
    pub const CHILD_LINK_FIELD: &str = "story";

    pub fn relation() -> StoryRelationSpec {
        StoryRelationSpec {
            parent_table: Self::PARENT_TABLE,
            child_table: Self::CHILD_TABLE,
            child_link_field: Self::CHILD_LINK_FIELD,
            parent_resource_type: ResourceType::Story,
            child_resource_type: ResourceType::ScrumTask,
        }
    }

    pub fn is_parent_table(table: &str) -> bool {
        table == Self::PARENT_TABLE
    }

    pub fn is_child_table(table: &str) -> bool {
        table == Self::CHILD_TABLE
    }

    pub fn parent_record(record: &Record) -> Option<SnowRecord> {
        let resource = SnowRecord::from_servicenow(record);
        matches!(resource.resource_type, ResourceType::Story).then_some(resource)
    }

    pub fn child_record(record: &Record) -> Option<SnowRecord> {
        let resource = SnowRecord::from_servicenow(record);
        matches!(resource.resource_type, ResourceType::ScrumTask).then_some(resource)
    }

    pub fn parent_ref(record: &SnowRecord) -> Option<RecordRef> {
        if record.resource_type == ResourceType::Story {
            Some(RecordRef {
                sys_id: record.sys_id.clone(),
                number: record.number.clone(),
                table: record.table.clone(),
            })
        } else {
            None
        }
    }

    pub fn child_refresh_plan(parent: &SnowRecord) -> Option<StoryChildRefreshPlan> {
        let parent_ref = Self::parent_ref(parent)?;
        Some(StoryChildRefreshPlan {
            parent: parent_ref.clone(),
            relation: Self::relation(),
            query: format!("{}={}", Self::CHILD_LINK_FIELD, parent_ref.sys_id),
        })
    }

    pub fn write_result_from_fresh_row(
        record: SnowRecord,
        fresh_row: &Record,
    ) -> Result<StoryWriteResult> {
        Ok(StoryWriteResult {
            record,
            concurrency: StoryWriteConcurrency::from_fresh_row(fresh_row)?,
        })
    }
}

impl StoryChildRefreshPlan {
    pub fn child_link_field(&self) -> &'static str {
        self.relation.child_link_field
    }

    pub fn child_table(&self) -> &'static str {
        self.relation.child_table
    }
}

impl StoryWriteConcurrency {
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
            .or_else(|| record.get_str("sys_mod_count"))
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .and_then(|value| value.parse::<i64>().ok());

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
    fn identifies_tables() {
        assert!(StoryResource::is_parent_table("rm_story"));
        assert!(StoryResource::is_child_table("rm_scrum_task"));
        assert!(!StoryResource::is_parent_table("sc_req_item"));
    }

    #[test]
    fn builds_parent_refresh_plan() {
        let record = Record::from_json(
            "rm_story",
            &serde_json::json!({
                "sys_id": "story-sys",
                "number": "STRY0001234",
                "short_description": "Story",
                "state": "Draft"
            }),
            DisplayValue::Raw,
        )
        .unwrap();
        let core_record = SnowRecord::from_servicenow(&record);
        let plan = StoryResource::child_refresh_plan(&core_record).unwrap();
        assert_eq!(plan.child_table(), "rm_scrum_task");
        assert_eq!(plan.child_link_field(), "story");
        assert_eq!(plan.parent.number, "STRY0001234");
        assert!(plan.query.contains("story-sys"));
    }

    #[test]
    fn captures_concurrency_from_fresh_row() {
        let record = Record::from_json(
            "rm_story",
            &serde_json::json!({
                "sys_id": "story-sys",
                "number": "STRY0001234",
                "sys_updated_on": "2026-05-19 10:11:12",
                "sys_mod_count": "3"
            }),
            DisplayValue::Raw,
        )
        .unwrap();

        let concurrency = StoryWriteConcurrency::from_fresh_row(&record).unwrap();

        assert_eq!(concurrency.sys_updated_on, "2026-05-19 10:11:12");
        assert_eq!(concurrency.sys_mod_count, Some(3));
    }

    #[test]
    fn missing_mod_count_is_allowed() {
        let record = Record::from_json(
            "rm_scrum_task",
            &serde_json::json!({
                "sys_id": "task-sys",
                "number": "STSK0001234",
                "sys_updated_on": "2026-05-19 10:11:12"
            }),
            DisplayValue::Raw,
        )
        .unwrap();

        let concurrency = StoryWriteConcurrency::from_fresh_row(&record).unwrap();

        assert_eq!(concurrency.sys_updated_on, "2026-05-19 10:11:12");
        assert_eq!(concurrency.sys_mod_count, None);
    }
}
