use crate::tools::registry::{ToolMetadata, ToolRegistry, object_schema};

pub fn register(registry: &mut ToolRegistry) {
    for (name, description, write) in [
        (
            "catalog_items_search",
            "Search Service Catalog items",
            false,
        ),
        ("catalog_item_get", "Get a Service Catalog item", false),
        (
            "catalog_plan_request",
            "Plan a Service Catalog request",
            false,
        ),
        (
            "catalog_submit_request",
            "Submit a confirmed Service Catalog request",
            true,
        ),
        (
            "catalog_cancel_request",
            "Cancel a cancellable catalog request",
            true,
        ),
    ] {
        registry.add(ToolMetadata {
            name: name.to_string(),
            description: description.to_string(),
            input_schema: object_schema(),
            output_schema: object_schema(),
            default_enabled: !write,
            requires_confirmation: write,
        });
    }
}
