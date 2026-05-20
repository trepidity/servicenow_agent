use crate::{RecordRef, ResourceType, SnowRecord};
use servicenow_rs::prelude::Record;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeRelationSpec {
    pub parent_table: &'static str,
    pub child_table: &'static str,
    pub child_link_field: &'static str,
    pub parent_resource_type: ResourceType,
    pub child_resource_type: ResourceType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeChildRefreshPlan {
    pub parent: RecordRef,
    pub relation: ChangeRelationSpec,
    pub query: String,
}

#[derive(Debug, Clone, Default)]
pub struct ChangeResource;

impl ChangeResource {
    pub const PARENT_TABLE: &str = "change_request";
    pub const CHILD_TABLE: &str = "change_task";
    pub const CHILD_LINK_FIELD: &str = "change_request";

    pub fn relation() -> ChangeRelationSpec {
        ChangeRelationSpec {
            parent_table: Self::PARENT_TABLE,
            child_table: Self::CHILD_TABLE,
            child_link_field: Self::CHILD_LINK_FIELD,
            parent_resource_type: ResourceType::Change,
            child_resource_type: ResourceType::ChangeTask,
        }
    }

    pub fn is_parent_table(table: &str) -> bool {
        crate::canonical_record_table(table) == Self::PARENT_TABLE
    }

    pub fn is_child_table(table: &str) -> bool {
        table == Self::CHILD_TABLE
    }

    pub fn parent_record(record: &Record) -> Option<SnowRecord> {
        let resource = SnowRecord::from_servicenow(record);
        matches!(resource.resource_type, ResourceType::Change).then_some(resource)
    }

    pub fn child_record(record: &Record) -> Option<SnowRecord> {
        let resource = SnowRecord::from_servicenow(record);
        matches!(resource.resource_type, ResourceType::ChangeTask).then_some(resource)
    }

    pub fn parent_ref(record: &SnowRecord) -> Option<RecordRef> {
        if record.resource_type == ResourceType::Change {
            Some(RecordRef {
                sys_id: record.sys_id.clone(),
                number: record.number.clone(),
                table: record.table.clone(),
            })
        } else {
            None
        }
    }

    pub fn child_refresh_plan(parent: &SnowRecord) -> Option<ChangeChildRefreshPlan> {
        let parent_ref = Self::parent_ref(parent)?;
        Some(ChangeChildRefreshPlan {
            parent: parent_ref.clone(),
            relation: Self::relation(),
            query: format!("{}={}", Self::CHILD_LINK_FIELD, parent_ref.sys_id),
        })
    }
}

impl ChangeChildRefreshPlan {
    pub fn child_link_field(&self) -> &'static str {
        self.relation.child_link_field
    }

    pub fn child_table(&self) -> &'static str {
        self.relation.child_table
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use servicenow_rs::prelude::DisplayValue;

    #[test]
    fn identifies_tables() {
        assert!(ChangeResource::is_parent_table("change_request"));
        assert!(ChangeResource::is_child_table("change_task"));
        assert!(!ChangeResource::is_parent_table("incident"));
    }

    #[test]
    fn builds_parent_refresh_plan() {
        let record = Record::from_json(
            "change_request",
            &serde_json::json!({
                "sys_id": "chg-sys",
                "number": "CHG0001234",
                "short_description": "Change",
                "state": "Open"
            }),
            DisplayValue::Raw,
        )
        .unwrap();
        let core_record = SnowRecord::from_servicenow(&record);
        let plan = ChangeResource::child_refresh_plan(&core_record).unwrap();
        assert_eq!(plan.child_table(), "change_task");
        assert_eq!(plan.child_link_field(), "change_request");
        assert_eq!(plan.parent.number, "CHG0001234");
        assert!(plan.query.contains("chg-sys"));
    }
}
