use crate::tools::records::{RESOURCE_PLAN_LOOKUP_TABLES, record_lookup_arg_schema};
use crate::tools::registry::{ToolMetadata, ToolRegistry, object_schema};

pub fn register(registry: &mut ToolRegistry) {
    registry.add(ToolMetadata {
        name: "resource_plan_get".to_string(),
        description: "Retrieve a resource plan by number or resource_plan sys_id".to_string(),
        input_schema: record_lookup_arg_schema(RESOURCE_PLAN_LOOKUP_TABLES),
        output_schema: object_schema(),
        default_enabled: true,
        requires_confirmation: false,
    });
}
