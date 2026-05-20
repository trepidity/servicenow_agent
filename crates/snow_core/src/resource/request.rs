use crate::{RecordRef, ResourceType, SnowRecord};
use servicenow_rs::prelude::Record;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestRelationSpec {
    pub parent_table: &'static str,
    pub child_table: &'static str,
    pub child_link_field: &'static str,
    pub parent_resource_type: ResourceType,
    pub child_resource_type: ResourceType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestChildRefreshPlan {
    pub parent: RecordRef,
    pub relation: RequestRelationSpec,
    pub query: String,
}

#[derive(Debug, Clone, Default)]
pub struct RequestResource;

impl RequestResource {
    pub const PARENT_TABLE: &str = "sc_req_item";
    pub const CHILD_TABLE: &str = "sc_task";
    pub const CHILD_LINK_FIELD: &str = "request_item";

    pub fn relation() -> RequestRelationSpec {
        RequestRelationSpec {
            parent_table: Self::PARENT_TABLE,
            child_table: Self::CHILD_TABLE,
            child_link_field: Self::CHILD_LINK_FIELD,
            parent_resource_type: ResourceType::Request,
            child_resource_type: ResourceType::RequestTask,
        }
    }

    pub fn is_parent_table(table: &str) -> bool {
        matches!(table, "sc_req_item" | "request_item")
    }

    pub fn is_child_table(table: &str) -> bool {
        table == Self::CHILD_TABLE
    }

    pub fn parent_record(record: &Record) -> Option<SnowRecord> {
        let resource = SnowRecord::from_servicenow(record);
        matches!(resource.resource_type, ResourceType::Request).then_some(resource)
    }

    pub fn child_record(record: &Record) -> Option<SnowRecord> {
        let resource = SnowRecord::from_servicenow(record);
        matches!(resource.resource_type, ResourceType::RequestTask).then_some(resource)
    }

    pub fn parent_ref(record: &SnowRecord) -> Option<RecordRef> {
        if record.resource_type == ResourceType::Request {
            Some(RecordRef {
                sys_id: record.sys_id.clone(),
                number: record.number.clone(),
                table: record.table.clone(),
            })
        } else {
            None
        }
    }

    pub fn child_refresh_plan(parent: &SnowRecord) -> Option<RequestChildRefreshPlan> {
        let parent_ref = Self::parent_ref(parent)?;
        Some(RequestChildRefreshPlan {
            parent: parent_ref.clone(),
            relation: Self::relation(),
            query: format!("{}={}", Self::CHILD_LINK_FIELD, parent_ref.sys_id),
        })
    }
}

impl RequestChildRefreshPlan {
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
        assert!(RequestResource::is_parent_table("sc_req_item"));
        assert!(RequestResource::is_parent_table("request_item"));
        assert!(RequestResource::is_child_table("sc_task"));
        assert!(!RequestResource::is_parent_table("change_request"));
    }

    #[test]
    fn builds_parent_refresh_plan() {
        let record = Record::from_json(
            "sc_req_item",
            &serde_json::json!({
                "sys_id": "ritm-sys",
                "number": "RITM0012345",
                "short_description": "Request",
                "state": "Open"
            }),
            DisplayValue::Raw,
        )
        .unwrap();
        let core_record = SnowRecord::from_servicenow(&record);
        let plan = RequestResource::child_refresh_plan(&core_record).unwrap();
        assert_eq!(plan.child_table(), "sc_task");
        assert_eq!(plan.child_link_field(), "request_item");
        assert_eq!(plan.parent.number, "RITM0012345");
        assert!(plan.query.contains("ritm-sys"));
    }
}
