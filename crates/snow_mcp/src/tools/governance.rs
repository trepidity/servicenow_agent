use crate::tools::registry::{ToolMetadata, ToolRegistry, object_schema};

pub fn register(registry: &mut ToolRegistry) {
    for name in [
        "policy_describe",
        "tool_capabilities",
        "redaction_rules_describe",
    ] {
        registry.add(ToolMetadata {
            name: name.to_string(),
            description: "MCP governance metadata".to_string(),
            input_schema: object_schema(),
            output_schema: object_schema(),
            default_enabled: true,
            requires_confirmation: false,
        });
    }
}
