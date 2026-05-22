pub mod confirmation;
pub mod idempotency;
pub mod knowledge_grounding;
pub mod operation_plan;
pub mod plan_store;

pub use confirmation::*;
pub use idempotency::*;
pub use operation_plan::*;
pub use plan_store::*;

pub const GOVERNED_STORY_TOOL_NAMES: &[&str] = &[
    "story_plan_create",
    "story_apply_create",
    "story_plan_update",
    "story_apply_update",
    "story_task_plan_create",
    "story_task_apply_create",
    "story_task_plan_update",
    "story_task_apply_update",
];

pub const GOVERNED_TIMECARD_TOOL_NAMES: &[&str] =
    &["timecard_plan_set_hours", "timecard_apply_set_hours"];

pub fn is_governed_story_tool(tool: &str) -> bool {
    GOVERNED_STORY_TOOL_NAMES.contains(&tool)
}

pub fn is_governed_timecard_tool(tool: &str) -> bool {
    GOVERNED_TIMECARD_TOOL_NAMES.contains(&tool)
}

pub fn is_governed_write_tool(tool: &str) -> bool {
    is_governed_story_tool(tool) || is_governed_timecard_tool(tool)
}
