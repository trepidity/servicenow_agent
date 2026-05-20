use crate::tools::registry::{ToolMetadata, ToolRegistry, object_schema};

pub fn register(registry: &mut ToolRegistry) {
    registry.add(ToolMetadata {
        name: "work_note_plan_add".to_string(),
        description: "Preview a work-note addition using the record-field journal path".to_string(),
        input_schema: object_schema(),
        output_schema: object_schema(),
        default_enabled: true,
        requires_confirmation: false,
    });
    registry.add(ToolMetadata {
        name: "work_note_apply_add".to_string(),
        description: "Apply a confirmed work-note addition".to_string(),
        input_schema: object_schema(),
        output_schema: object_schema(),
        default_enabled: false,
        requires_confirmation: true,
    });
}
