use crate::tools::registry::{ToolMetadata, ToolRegistry, object_schema};

pub fn register(registry: &mut ToolRegistry) {
    for name in [
        "audit_event_get",
        "audit_events_search",
        "audit_chain_verify",
    ] {
        registry.add(ToolMetadata {
            name: name.to_string(),
            description: "Governance audit read surface".to_string(),
            input_schema: object_schema(),
            output_schema: object_schema(),
            default_enabled: true,
            requires_confirmation: false,
        });
    }
}
