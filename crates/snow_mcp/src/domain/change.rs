use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::primitives::{Citation, RecordRef, UserRef};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChangeRequest {
    pub record: RecordRef,
    pub short_description: String,
    pub change_type: String,
    pub assignment_group: Option<String>,
    pub assigned_to: Option<UserRef>,
    pub window: Option<ChangeWindow>,
    pub implementation_plan: Option<String>,
    pub validation_plan: Option<String>,
    pub backout_plan: Option<String>,
    pub risk: Option<String>,
    pub impact: Option<String>,
    pub citations: Vec<Citation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChangeWindow {
    pub start_utc: DateTime<Utc>,
    pub end_utc: DateTime<Utc>,
    pub display_start_local: String,
    pub display_end_local: String,
    pub instance_timezone: String,
    pub actor_timezone: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChangeTask {
    pub record: RecordRef,
    pub parent_change: RecordRef,
    pub assigned_to: Option<UserRef>,
    pub start_date: Option<DateTime<Utc>>,
    pub end_date: Option<DateTime<Utc>>,
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScheduleConflict {
    pub change: RecordRef,
    pub overlap_window: ChangeWindow,
    pub configuration_item: Option<String>,
    pub reason: String,
}
