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

pub const GOVERNED_WORK_NOTE_TOOL_NAMES: &[&str] = &["work_note_plan_add", "work_note_apply_add"];

pub const GOVERNED_ATTACHMENT_TOOL_NAMES: &[&str] = &["attachment_upload"];

pub const GOVERNED_CATALOG_TOOL_NAMES: &[&str] =
    &["catalog_plan_request", "catalog_submit_request"];

pub const GOVERNED_APPROVAL_TOOL_NAMES: &[&str] = &["approval_approve", "approval_reject"];

pub const GOVERNED_CHANGE_TOOL_NAMES: &[&str] = &[
    "change_request_plan_create",
    "change_request_apply_create",
    "change_request_plan_update",
    "change_request_apply_update",
    "change_task_plan_create",
    "change_task_apply_create",
    "change_task_plan_update",
    "change_task_apply_update",
];

pub fn is_governed_story_tool(tool: &str) -> bool {
    GOVERNED_STORY_TOOL_NAMES.contains(&tool)
}

pub fn is_governed_timecard_tool(tool: &str) -> bool {
    GOVERNED_TIMECARD_TOOL_NAMES.contains(&tool)
}

pub fn is_governed_work_note_tool(tool: &str) -> bool {
    GOVERNED_WORK_NOTE_TOOL_NAMES.contains(&tool)
}

pub fn is_governed_attachment_tool(tool: &str) -> bool {
    GOVERNED_ATTACHMENT_TOOL_NAMES.contains(&tool)
}

pub fn is_governed_catalog_tool(tool: &str) -> bool {
    GOVERNED_CATALOG_TOOL_NAMES.contains(&tool)
}

pub fn is_governed_approval_tool(tool: &str) -> bool {
    GOVERNED_APPROVAL_TOOL_NAMES.contains(&tool)
}

pub fn is_governed_change_tool(tool: &str) -> bool {
    GOVERNED_CHANGE_TOOL_NAMES.contains(&tool)
}

pub fn is_governed_write_tool(tool: &str) -> bool {
    is_governed_story_tool(tool)
        || is_governed_timecard_tool(tool)
        || is_governed_work_note_tool(tool)
        || is_governed_attachment_tool(tool)
        || is_governed_catalog_tool(tool)
        || is_governed_approval_tool(tool)
        || is_governed_change_tool(tool)
}
