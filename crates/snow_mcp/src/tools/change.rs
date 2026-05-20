use crate::tools::registry::{ToolMetadata, ToolRegistry, object_schema};

pub fn register(registry: &mut ToolRegistry) {
    for name in [
        "change_plan_request",
        "change_submit_request",
        "change_task_plan_assignment",
        "change_task_apply_assignment",
    ] {
        registry.add(ToolMetadata {
            name: name.to_string(),
            description: "Change and CTASK operation scaffold; disabled for writes by default"
                .to_string(),
            input_schema: object_schema(),
            output_schema: object_schema(),
            default_enabled: !name.contains("submit") && !name.contains("apply"),
            requires_confirmation: name.contains("submit") || name.contains("apply"),
        });
    }
}
