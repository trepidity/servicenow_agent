use std::fmt;
use std::str::FromStr;

use anyhow::{Result, anyhow};
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use servicenow_rs::prelude::Record;

use crate::helpers::{non_empty_owned, parse_i64};
use crate::{RecordRef, build_reference_json};

const DAY_FIELDS: [&str; 7] = [
    "sunday",
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
];

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Weekday {
    Sun,
    Mon,
    Tue,
    Wed,
    Thu,
    Fri,
    Sat,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WeekSelector {
    Current,
    Date(NaiveDate),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SetMode {
    Set,
    Add,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TimeValue(String);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SimpleRef {
    pub sys_id: String,
    pub table: String,
    pub display: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserRef {
    pub sys_id: String,
    pub user_name: Option<String>,
    pub email: Option<String>,
    pub display: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TimeCard {
    pub sys_id: String,
    pub time_sheet: Option<SimpleRef>,
    pub week_starts_on: String,
    pub user: UserRef,
    pub task: Option<RecordRef>,
    pub category: String,
    pub category_label: String,
    pub project_time_category: Option<String>,
    pub hours: [String; 7],
    pub total: String,
    pub state: String,
    pub sys_updated_on: String,
    pub sys_mod_count: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TimecardSheet {
    pub sheet: Option<SimpleRef>,
    pub week_starts_on: String,
    pub state: String,
    pub cards: Vec<TimeCard>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CardSelector {
    SysId(String),
    TaskNumber {
        number: String,
        category: Option<String>,
    },
    Index(usize),
}

#[derive(Debug, Clone, Default)]
pub struct TimecardResource;

impl Weekday {
    pub fn field_name(self) -> &'static str {
        match self {
            Self::Sun => "sunday",
            Self::Mon => "monday",
            Self::Tue => "tuesday",
            Self::Wed => "wednesday",
            Self::Thu => "thursday",
            Self::Fri => "friday",
            Self::Sat => "saturday",
        }
    }

    pub fn index(self) -> usize {
        match self {
            Self::Sun => 0,
            Self::Mon => 1,
            Self::Tue => 2,
            Self::Wed => 3,
            Self::Thu => 4,
            Self::Fri => 5,
            Self::Sat => 6,
        }
    }

    pub fn valid_values() -> &'static str {
        "sun, sunday, mon, monday, tue, tuesday, wed, wednesday, thu, thursday, fri, friday, sat, saturday"
    }
}

impl FromStr for Weekday {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "sun" | "sunday" => Ok(Self::Sun),
            "mon" | "monday" => Ok(Self::Mon),
            "tue" | "tues" | "tuesday" => Ok(Self::Tue),
            "wed" | "wednesday" => Ok(Self::Wed),
            "thu" | "thur" | "thurs" | "thursday" => Ok(Self::Thu),
            "fri" | "friday" => Ok(Self::Fri),
            "sat" | "saturday" => Ok(Self::Sat),
            other => Err(anyhow!(
                "unknown weekday '{other}'; valid values: {}",
                Self::valid_values()
            )),
        }
    }
}

impl fmt::Display for Weekday {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.field_name())
    }
}

impl TimeValue {
    pub fn parse(input: impl AsRef<str>) -> Result<Self> {
        let input = input.as_ref().trim();
        if input.is_empty() {
            return Err(anyhow!("hours cannot be empty"));
        }
        let value = input
            .parse::<f64>()
            .map_err(|_| anyhow!("hours must be a decimal number"))?;
        Self::from_hours(value)
    }

    pub fn from_hours(value: f64) -> Result<Self> {
        if !value.is_finite() {
            return Err(anyhow!("hours must be a finite decimal number"));
        }
        if value < 0.0 {
            return Err(anyhow!("hours cannot be negative"));
        }
        if value > 24.0 {
            return Err(anyhow!("hours cannot exceed 24 per day"));
        }
        Ok(Self(normalize_hours(value)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }

    pub fn to_f64(&self) -> Result<f64> {
        self.0
            .parse::<f64>()
            .map_err(|_| anyhow!("stored hours value is not numeric: {}", self.0))
    }
}

impl FromStr for TimeValue {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::parse(value)
    }
}

impl TryFrom<&str> for TimeValue {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl fmt::Display for TimeValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TimeCard {
    pub fn day_hours(&self, day: Weekday) -> &str {
        &self.hours[day.index()]
    }

    pub fn is_owned_by(&self, user_sys_id: &str) -> bool {
        !self.user.sys_id.trim().is_empty() && self.user.sys_id == user_sys_id.trim()
    }

    pub fn is_editable(&self) -> bool {
        is_editable_timecard_state(&self.state)
    }
}

impl TimecardResource {
    pub const TABLE: &str = "time_card";
    pub const SHEET_TABLE: &str = "time_sheet";

    pub fn from_servicenow(record: &Record) -> Result<TimeCard> {
        let user = user_ref_from_record_field(record, "user")?;
        let sys_updated_on = raw_or_display(record, "sys_updated_on")
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                anyhow!(
                    "fresh time_card row {} did not include sys_updated_on",
                    record.sys_id
                )
            })?
            .to_string();

        Ok(TimeCard {
            sys_id: record.sys_id.clone(),
            time_sheet: simple_ref_from_record_field(record, "time_sheet", "time_sheet"),
            week_starts_on: raw_or_display(record, "week_starts_on")
                .unwrap_or_default()
                .to_string(),
            user,
            task: task_ref_from_record_field(record, "task"),
            category: record.get_raw("category").unwrap_or_default().to_string(),
            category_label: record
                .get_display("category")
                .or_else(|| record.get_str("category"))
                .unwrap_or_default()
                .to_string(),
            project_time_category: non_empty_owned(record.get_str("project_time_category")),
            hours: DAY_FIELDS.map(|field| raw_or_display(record, field).unwrap_or("0").to_string()),
            total: raw_or_display(record, "total")
                .unwrap_or_default()
                .to_string(),
            state: record
                .get_display("state")
                .or_else(|| record.get_raw("state"))
                .or_else(|| record.get_str("state"))
                .unwrap_or_default()
                .to_string(),
            sys_updated_on,
            sys_mod_count: parse_i64(raw_or_display(record, "sys_mod_count")),
        })
    }
}

impl CardSelector {
    pub fn parse_shape(value: &str) -> Result<Self> {
        let value = value.trim();
        if value.is_empty() {
            return Err(anyhow!("card selector cannot be empty"));
        }
        if value.len() == 32 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Ok(Self::SysId(value.to_string()));
        }
        if value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Ok(Self::Index(value.parse::<usize>()?));
        }
        Ok(Self::TaskNumber {
            number: value.to_string(),
            category: None,
        })
    }
}

pub fn is_timecard_day_field(field: &str) -> bool {
    DAY_FIELDS.contains(&field)
}

pub fn is_editable_timecard_state(state: &str) -> bool {
    let normalized = state.trim().to_ascii_lowercase();
    matches!(normalized.as_str(), "pending" | "rejected" | "recalled")
}

pub fn parse_existing_hours(value: &str) -> Result<f64> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(0.0);
    }
    value
        .parse::<f64>()
        .map_err(|_| anyhow!("existing time card hours value is not numeric: {value}"))
}

fn normalize_hours(value: f64) -> String {
    let rounded = (value * 100.0).round() / 100.0;
    let mut text = format!("{rounded:.2}");
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
    if text == "-0" { "0".to_string() } else { text }
}

fn simple_ref_from_record_field(
    record: &Record,
    field_name: &str,
    default_table: &str,
) -> Option<SimpleRef> {
    let value = build_reference_json(record, field_name, default_table)?;
    let map = value.as_object()?;
    let sys_id = map.get("sys_id").and_then(|value| value.as_str())?;
    if sys_id.trim().is_empty() {
        return None;
    }
    Some(SimpleRef {
        sys_id: sys_id.to_string(),
        table: map
            .get("table")
            .and_then(|value| value.as_str())
            .unwrap_or(default_table)
            .to_string(),
        display: map
            .get("display_value")
            .and_then(|value| value.as_str())
            .or_else(|| map.get("name").and_then(|value| value.as_str()))
            .or_else(|| map.get("number").and_then(|value| value.as_str()))
            .unwrap_or(sys_id)
            .to_string(),
    })
}

pub(crate) fn user_ref_from_record_field(record: &Record, field_name: &str) -> Result<UserRef> {
    let reference =
        simple_ref_from_record_field(record, field_name, "sys_user").ok_or_else(|| {
            anyhow!(
                "time_card row {} did not include user sys_id",
                record.sys_id
            )
        })?;
    Ok(UserRef {
        sys_id: reference.sys_id.clone(),
        user_name: non_empty_owned(record.get_str(&format!("{field_name}.user_name"))),
        email: non_empty_owned(record.get_str(&format!("{field_name}.email"))),
        display: first_non_empty([
            Some(reference.display.as_str()),
            record.get_str(&format!("{field_name}.name")),
            record.get_str(&format!("{field_name}.user_name")),
            record.get_str(&format!("{field_name}.email")),
            Some(reference.sys_id.as_str()),
        ])
        .unwrap_or_default()
        .to_string(),
    })
}

fn task_ref_from_record_field(record: &Record, field_name: &str) -> Option<RecordRef> {
    let reference = simple_ref_from_record_field(record, field_name, "task")?;
    let number = first_non_empty([
        record.get_str(&format!("{field_name}.number")),
        Some(reference.display.as_str()),
    ])?;
    Some(RecordRef {
        sys_id: reference.sys_id,
        number: number.to_string(),
        table: record
            .get_str(&format!("{field_name}.sys_class_name"))
            .unwrap_or(reference.table.as_str())
            .to_string(),
    })
}

fn raw_or_display<'a>(record: &'a Record, field: &str) -> Option<&'a str> {
    record
        .get_raw(field)
        .or_else(|| record.get_display(field))
        .or_else(|| record.get_str(field))
}

fn first_non_empty<'a>(values: impl IntoIterator<Item = Option<&'a str>>) -> Option<&'a str> {
    values
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use servicenow_rs::prelude::DisplayValue;

    fn sample_timecard_record() -> Record {
        Record::from_json(
            "time_card",
            &serde_json::json!({
                "sys_id": "card-sys",
                "time_sheet": {
                    "value": "sheet-sys",
                    "display_value": "2026-05-17"
                },
                "week_starts_on": {
                    "value": "2026-05-17",
                    "display_value": "2026-05-17"
                },
                "user": {
                    "value": "user-sys",
                    "display_value": "Casey User"
                },
                "user.user_name": "casey.user",
                "user.email": "casey@example.com",
                "task": {
                    "value": "task-sys",
                    "display_value": "PRJ0161219"
                },
                "task.number": "PRJ0161219",
                "task.sys_class_name": "pm_project_task",
                "category": {
                    "value": "project_work",
                    "display_value": "Project/Project Task"
                },
                "project_time_category": "Development",
                "sunday": "0",
                "monday": "2.50",
                "tuesday": "0",
                "wednesday": "0",
                "thursday": "0",
                "friday": "0",
                "saturday": "0",
                "total": "2.5",
                "state": {
                    "value": "Pending",
                    "display_value": "Pending"
                },
                "sys_updated_on": "2026-05-21 10:11:12",
                "sys_mod_count": "7"
            }),
            DisplayValue::Both,
        )
        .expect("record")
    }

    #[test]
    fn maps_raw_timecard_without_number() {
        let card = TimecardResource::from_servicenow(&sample_timecard_record()).expect("card");

        assert_eq!(card.sys_id, "card-sys");
        assert_eq!(card.user.sys_id, "user-sys");
        assert_eq!(card.user.user_name.as_deref(), Some("casey.user"));
        assert_eq!(
            card.time_sheet.as_ref().map(|sheet| sheet.sys_id.as_str()),
            Some("sheet-sys")
        );
        assert_eq!(
            card.task.as_ref().map(|task| task.number.as_str()),
            Some("PRJ0161219")
        );
        assert_eq!(card.category, "project_work");
        assert_eq!(card.category_label, "Project/Project Task");
        assert_eq!(card.hours[Weekday::Mon.index()], "2.50");
        assert_eq!(card.total, "2.5");
        assert_eq!(card.sys_updated_on, "2026-05-21 10:11:12");
        assert_eq!(card.sys_mod_count, Some(7));
    }

    #[test]
    fn parses_weekdays_case_insensitively() {
        assert_eq!("Sun".parse::<Weekday>().unwrap(), Weekday::Sun);
        assert_eq!("monday".parse::<Weekday>().unwrap(), Weekday::Mon);
        assert_eq!("THURS".parse::<Weekday>().unwrap(), Weekday::Thu);
        assert!("noday".parse::<Weekday>().is_err());
    }

    #[test]
    fn validates_and_normalizes_hours() {
        assert_eq!(TimeValue::parse("8.00").unwrap().as_str(), "8");
        assert_eq!(TimeValue::parse("1.236").unwrap().as_str(), "1.24");
        assert_eq!(TimeValue::parse("0").unwrap().as_str(), "0");
        assert!(TimeValue::parse("-1").is_err());
        assert!(TimeValue::parse("24.01").is_err());
        assert!(TimeValue::parse("abc").is_err());
    }

    #[test]
    fn checks_owner_and_editable_state() {
        let mut card = TimecardResource::from_servicenow(&sample_timecard_record()).expect("card");

        assert!(card.is_owned_by("user-sys"));
        assert!(!card.is_owned_by("other-user"));
        assert!(card.is_editable());

        card.state = "Submitted".to_string();
        assert!(!card.is_editable());
    }

    #[test]
    fn parses_card_selector_shape() {
        assert_eq!(
            CardSelector::parse_shape("0123456789abcdef0123456789abcdef").unwrap(),
            CardSelector::SysId("0123456789abcdef0123456789abcdef".to_string())
        );
        assert_eq!(
            CardSelector::parse_shape("3").unwrap(),
            CardSelector::Index(3)
        );
        assert_eq!(
            CardSelector::parse_shape("PRJ0161219").unwrap(),
            CardSelector::TaskNumber {
                number: "PRJ0161219".to_string(),
                category: None,
            }
        );
    }
}
