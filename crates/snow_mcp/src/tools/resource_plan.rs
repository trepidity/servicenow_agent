use crate::tools::registry::{ToolMetadata, ToolRegistry, number_arg_schema, object_schema};

pub fn register(registry: &mut ToolRegistry) {
    registry.add(ToolMetadata {
        name: "resource_plan_get".to_string(),
        description: "Retrieve a resource plan by number".to_string(),
        input_schema: number_arg_schema(),
        output_schema: object_schema(),
        default_enabled: true,
        requires_confirmation: false,
    });
}
