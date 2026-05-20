use crate::tools::registry::{ToolMetadata, ToolRegistry, object_schema};

pub fn register(registry: &mut ToolRegistry) {
    for (name, write) in [("plan_get", false), ("plan_cancel", true)] {
        registry.add(ToolMetadata {
            name: name.to_string(),
            description: "Inspect or cancel a pending MCP operation plan".to_string(),
            input_schema: object_schema(),
            output_schema: object_schema(),
            default_enabled: !write,
            requires_confirmation: write,
        });
    }
}
