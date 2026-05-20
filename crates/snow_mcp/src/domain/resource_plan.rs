use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

use super::primitives::{RecordRef, UserRef};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ResourcePlan {
    pub record: RecordRef,
    pub task: Option<RecordRef>,
    pub user_resource: Option<UserRef>,
    pub group_resource: Option<String>,
    pub planned_hours: Option<f64>,
    pub allocated_hours: Option<f64>,
    pub confirmed_hours: Option<f64>,
    pub start_date: Option<NaiveDate>,
    pub end_date: Option<NaiveDate>,
    pub state: String,
    pub state_display: String,
    pub hours_unit: HoursUnit,
    pub source: PlanSource,
    pub browser_url: String,
    pub synced_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HoursUnit {
    Hours,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PlanSource {
    Cache,
    Live,
}
