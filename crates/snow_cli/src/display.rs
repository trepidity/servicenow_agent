use colored::Colorize;
use servicenow_rs::prelude::{CatalogVariable, JournalEntry, Record};
use snow_core::resource::timecard::{TimeCard, TimecardSheet};
use std::io::Write;
use std::process::Command;

use crate::auth::strip_secret_env;

pub fn print_change_summary(record: &Record) {
    let number = record.get_str("number").unwrap_or("-");
    let title = record
        .get_str("short_description")
        .unwrap_or("(no description)");
    println!("{} — {}", number.bold(), title);
    print_field("State", record.get_str("state"));
    print_field("Category", record.get_str("category"));
    print_field("Start", record.get_str("start_date"));
    print_field("End", record.get_str("end_date"));
    print_field("Assigned to", record.get_str("assigned_to"));
    if let Some(desc) = record.get_str("description") {
        let clean = strip_html(desc);
        let truncated = if clean.chars().count() > 200 {
            format!("{}...", clean.chars().take(200).collect::<String>())
        } else {
            clean
        };
        // Collapse to single line for inline display
        let oneline = truncated.lines().collect::<Vec<_>>().join(" ");
        println!("{:>13} {}", "Description:".dimmed(), oneline);
    }
    print_multiline_field("Change Plan", record.get_str("change_plan"));
    print_multiline_field("Implementation", record.get_str("implementation_plan"));
    print_multiline_field("Backout Plan", record.get_str("backout_plan"));
}

pub fn print_project_summary(record: &Record) {
    let number = record.get_str("number").unwrap_or("-");
    let title = record
        .get_str("short_description")
        .unwrap_or("(no description)");
    println!("{} — {}", number.bold(), title);
    print_field("Name", record.get_str("name"));
    print_field("State", record.get_str("state"));
    print_field("Demand", record.get_str("demand"));
    print_field("Manager", record.get_str("project_manager"));
    print_field("Start", record.get_str("start_date"));
    print_field("End", record.get_str("end_date"));
    print_field("Complete", record.get_str("percent_complete"));
    print_multiline_field("Description", record.get_str("description"));
    print_multiline_field("Goals", record.get_str("goals"));
    print_multiline_field("Business Case", record.get_str("business_case"));
}

pub fn print_demand_summary(record: &Record) {
    let number = record.get_str("number").unwrap_or("-");
    let title = record
        .get_str("short_description")
        .unwrap_or("(no description)");
    println!("{} — {}", number.bold(), title);
    print_field("State", record.get_str("state"));
    print_field("Priority", record.get_str("priority"));
    print_field("Requested by", record.get_str("requested_by"));
    print_field("Start", record.get_str("start_date"));
    print_field("End", record.get_str("end_date"));
    print_multiline_field("Description", record.get_str("description"));
    print_multiline_field("Business Case", record.get_str("business_case"));
}

pub fn print_incident_summary(record: &Record) {
    let number = record.get_str("number").unwrap_or("-");
    let title = record
        .get_str("short_description")
        .unwrap_or("(no description)");
    println!("{} — {}", number.bold(), title);
    print_field("State", record.get_str("state"));
    print_field("Priority", record.get_str("priority"));
    print_field("Severity", record.get_str("severity"));
    print_field("Category", record.get_str("category"));
    print_field("Subcategory", record.get_str("subcategory"));
    print_field("Assigned to", record.get_str("assigned_to"));
    print_field("Group", record.get_str("assignment_group"));
    print_field("Caller", record.get_str("caller_id"));
    print_field("Opened", record.get_str("opened_at"));
    print_field("Resolved", record.get_str("resolved_at"));
    print_field("Close code", record.get_str("close_code"));
    print_multiline_field("Description", record.get_str("description"));
}

pub fn print_request_item_summary(record: &Record) {
    let number = record.get_str("number").unwrap_or("-");
    let title = record
        .get_str("short_description")
        .unwrap_or("(no description)");
    println!("{} — {}", number.bold(), title);
    print_field("State", record.get_str("state"));
    print_field("Priority", record.get_str("priority"));
    print_field("Catalog Item", record.get_str("cat_item"));
    print_field("Assigned to", record.get_str("assigned_to"));
    print_field("Group", record.get_str("assignment_group"));
    print_field("Request", record.get_str("request"));
    print_field("Requested for", record.get_str("requested_for"));
    print_field("Opened", record.get_str("opened_at"));
    print_field("Due", record.get_str("due_date"));
    print_field("Stage", record.get_str("stage"));
    print_field("Approval", record.get_str("approval"));
    print_multiline_field("Description", record.get_str("description"));
}

pub fn print_story_summary(record: &Record) {
    let number = record.get_str("number").unwrap_or("-");
    let title = record
        .get_str("short_description")
        .unwrap_or("(no description)");
    println!("{} — {}", number.bold(), title);
    print_field("State", record.get_str("state"));
    print_field("Priority", record.get_str("priority"));
    print_field("Assigned to", record.get_str("assigned_to"));
    print_field("Story Points", record.get_str("story_points"));
    print_field("Blocked", record.get_str("blocked"));
    print_field("Sprint", record.get_str("sprint"));
    print_field("Product", record.get_str("product"));
    print_field("Epic", record.get_str("epic"));
    print_multiline_field("Acceptance Criteria", record.get_str("acceptance_criteria"));
    print_multiline_field("Description", record.get_str("description"));
}

pub fn print_story_tasks(tasks: &[(Record, Vec<JournalEntry>)]) {
    println!("\n{}", "Story Tasks:".bold().underline());
    for (task, notes) in tasks {
        let number = task.get_str("number").unwrap_or("-");
        let title = task
            .get_str("short_description")
            .unwrap_or("(no description)");
        let state = task.get_str("state").unwrap_or("-");
        let assigned = task.get_str("assigned_to").unwrap_or("Unassigned");
        println!("  {} — {}", number.bold(), title);
        println!(
            "    {:>10} {}  {:>8} {}",
            "State:".dimmed(),
            state,
            "Assigned:".dimmed(),
            assigned
        );
        if !notes.is_empty() {
            println!("    {}", "Work Notes:".dimmed());
            for entry in notes {
                println!(
                    "      {} — {}",
                    entry.timestamp.green(),
                    entry.author.cyan().bold()
                );
                for line in entry.body.lines() {
                    println!("        {line}");
                }
            }
        }
    }
}

pub fn print_story_task_summary(record: &Record) {
    let number = record.get_str("number").unwrap_or("-");
    let title = record
        .get_str("short_description")
        .unwrap_or("(no description)");
    println!("{} — {}", number.bold(), title);
    print_field("State", record.get_str("state"));
    print_field("Priority", record.get_str("priority"));
    print_field("Assigned to", record.get_str("assigned_to"));
    print_field("Group", record.get_str("assignment_group"));
    print_field("Story", record.get_str("story"));
    print_field("Due", record.get_str("due_date"));
    print_multiline_field("Description", record.get_str("description"));
}

pub fn print_task_summary(record: &Record) {
    let number = record.get_str("number").unwrap_or("-");
    let title = record
        .get_str("short_description")
        .unwrap_or("(no description)");
    println!("{} — {}", number.bold(), title);
    print_field("State", record.get_str("state"));
    print_field("Priority", record.get_str("priority"));
    print_field("Assigned to", record.get_str("assigned_to"));
    print_field("Group", record.get_str("assignment_group"));
    print_field("Request Item", record.get_str("request_item"));
    print_field("Opened", record.get_str("opened_at"));
    print_field("Due", record.get_str("due_date"));
    print_multiline_field("Description", record.get_str("description"));
}

pub fn print_variables(variables: &[CatalogVariable]) {
    if variables.is_empty() {
        return;
    }
    println!("\n{}", "Variables:".bold().underline());
    for var in variables {
        if var.value.is_empty() {
            continue;
        }
        if var.value.contains('\n') || var.value.len() > 80 {
            println!("  {}:", var.name.dimmed());
            for line in var.value.lines() {
                println!("    {line}");
            }
        } else {
            println!("  {:>30}  {}", format!("{}:", var.name).dimmed(), var.value);
        }
    }
}

pub fn print_activity(entries: &[JournalEntry]) {
    if entries.is_empty() {
        return;
    }
    println!("\n{}", "Recent Activity:".bold().underline());
    for entry in entries {
        println!(
            "  {} — {}",
            entry.timestamp.green(),
            entry.author.cyan().bold()
        );
        for line in entry.body.lines() {
            println!("    {line}");
        }
    }
}

pub fn print_tasks(records: &[Record]) {
    if records.is_empty() {
        println!("No tasks found.");
        return;
    }
    println!(
        "{:<4} {:<16} {:<14} {:<20} {}",
        "#".bold(),
        "Task Number".bold(),
        "State".bold(),
        "Assigned To".bold(),
        "Short Description".bold()
    );
    for (i, record) in records.iter().enumerate() {
        println!(
            "{:<4} {:<16} {:<14} {:<20} {}",
            i + 1,
            record.get_str("number").unwrap_or("-"),
            record.get_str("state").unwrap_or("-"),
            record.get_str("assigned_to").unwrap_or("-"),
            record.get_str("short_description").unwrap_or("-"),
        );
    }
}

pub fn print_timecard_sheet(sheet: &TimecardSheet) {
    print!("{}", format_timecard_sheet(sheet));
}

pub fn format_timecard_sheet(sheet: &TimecardSheet) -> String {
    let mut out = String::new();
    let sheet_state = if sheet.state.trim().is_empty() {
        "-"
    } else {
        sheet.state.as_str()
    };
    out.push_str(&format!(
        "Time sheet - week of {} - {}\n",
        blank_as_dash(&sheet.week_starts_on),
        sheet_state
    ));

    if sheet.cards.is_empty() {
        out.push_str("No time cards found for this week.\n");
        return out;
    }

    let task_width = sheet
        .cards
        .iter()
        .map(timecard_task_display)
        .map(|value| value.len())
        .max()
        .unwrap_or(4)
        .clamp(4, 22);
    let category_width = sheet
        .cards
        .iter()
        .map(|card| display_category(card).len())
        .max()
        .unwrap_or(8)
        .clamp(8, 28);
    let project_width = sheet
        .cards
        .iter()
        .map(|card| {
            card.project_time_category
                .as_deref()
                .unwrap_or("-")
                .trim()
                .len()
        })
        .max()
        .unwrap_or(13)
        .clamp(13, 24);

    out.push_str(&format!(
        " {:>3}  {:<task_width$}  {:<category_width$}  {:<project_width$}  {:>5} {:>5} {:>5} {:>5} {:>5} {:>5} {:>5}  {:>5}\n",
        "Idx", "Task", "Category", "Proj-time-cat", "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Total"
    ));

    let mut totals = [0.0_f64; 7];
    let mut grand_total = 0.0_f64;
    for (index, card) in sheet.cards.iter().enumerate() {
        for (day_index, value) in card.hours.iter().enumerate() {
            totals[day_index] += parse_hours(value).unwrap_or(0.0);
        }
        grand_total += parse_hours(&card.total).unwrap_or_else(|| {
            card.hours
                .iter()
                .filter_map(|value| parse_hours(value))
                .sum::<f64>()
        });
        out.push_str(&format!(
            " {:>3}  {:<task_width$}  {:<category_width$}  {:<project_width$}  {:>5} {:>5} {:>5} {:>5} {:>5} {:>5} {:>5}  {:>5}\n",
            index + 1,
            truncate_cell(&timecard_task_display(card), task_width),
            truncate_cell(&display_category(card), category_width),
            truncate_cell(
                card.project_time_category
                    .as_deref()
                    .filter(|value| !value.trim().is_empty())
                    .unwrap_or("-"),
                project_width
            ),
            format_hour_cell(&card.hours[0]),
            format_hour_cell(&card.hours[1]),
            format_hour_cell(&card.hours[2]),
            format_hour_cell(&card.hours[3]),
            format_hour_cell(&card.hours[4]),
            format_hour_cell(&card.hours[5]),
            format_hour_cell(&card.hours[6]),
            format_hour_cell(&card.total)
        ));
    }

    out.push_str(&format!(
        " {:>3}  {:<task_width$}  {:<category_width$}  {:>project_width$}  {:>5} {:>5} {:>5} {:>5} {:>5} {:>5} {:>5}  {:>5}\n",
        "",
        "",
        "",
        "Totals",
        format_decimal(totals[0]),
        format_decimal(totals[1]),
        format_decimal(totals[2]),
        format_decimal(totals[3]),
        format_decimal(totals[4]),
        format_decimal(totals[5]),
        format_decimal(totals[6]),
        format_decimal(grand_total)
    ));

    out
}

pub fn timecard_task_display(card: &TimeCard) -> String {
    card.task
        .as_ref()
        .map(|task| {
            if task.number.trim().is_empty() {
                task.sys_id.clone()
            } else {
                task.number.clone()
            }
        })
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "-".to_string())
}

pub fn print_approval_records(records: &[Record]) {
    if records.is_empty() {
        return;
    }
    println!("\n{}", "Your Approvals:".bold().underline());
    for record in records {
        let state = record.get_str("state").unwrap_or("unknown");
        let colored_state = match state {
            "approved" => state.green().to_string(),
            "rejected" => state.red().to_string(),
            "requested" => state.yellow().to_string(),
            _ => state.to_string(),
        };
        println!("  {} ({})", record.sys_id, colored_state);
    }
}

pub fn record_to_json(record: &Record) -> serde_json::Value {
    let mut map = serde_json::Map::new();
    map.insert("sys_id".to_string(), serde_json::json!(record.sys_id));
    for (name, fv) in record.fields() {
        let val = if let Some(s) = fv.as_str() {
            serde_json::json!(s)
        } else if let Some(v) = &fv.value {
            v.clone()
        } else {
            serde_json::Value::Null
        };
        map.insert(name.clone(), val);
    }
    serde_json::Value::Object(map)
}

fn display_category(card: &TimeCard) -> String {
    if !card.category_label.trim().is_empty() {
        card.category_label.clone()
    } else if !card.category.trim().is_empty() {
        card.category.clone()
    } else {
        "-".to_string()
    }
}

fn blank_as_dash(value: &str) -> &str {
    if value.trim().is_empty() { "-" } else { value }
}

fn truncate_cell(value: &str, width: usize) -> String {
    if value.len() <= width {
        return value.to_string();
    }
    if width <= 1 {
        return value.chars().take(width).collect();
    }
    let mut truncated = value
        .chars()
        .take(width.saturating_sub(1))
        .collect::<String>();
    truncated.push('~');
    truncated
}

fn format_hour_cell(value: &str) -> String {
    parse_hours(value)
        .map(format_decimal)
        .unwrap_or_else(|| blank_as_dash(value).to_string())
}

fn parse_hours(value: &str) -> Option<f64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Some(0.0);
    }
    trimmed.parse::<f64>().ok()
}

fn format_decimal(value: f64) -> String {
    let rounded = (value * 100.0).round() / 100.0;
    if (rounded.fract()).abs() < f64::EPSILON {
        format!("{}", rounded as i64)
    } else {
        let mut value = format!("{rounded:.2}");
        while value.contains('.') && value.ends_with('0') {
            value.pop();
        }
        if value.ends_with('.') {
            value.pop();
        }
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use snow_core::RecordRef;
    use snow_core::resource::timecard::{SimpleRef, UserRef};

    #[test]
    fn timecard_sheet_grid_includes_day_and_footer_totals() {
        let sheet = TimecardSheet {
            sheet: Some(SimpleRef {
                sys_id: "sheet-1".to_string(),
                table: "time_sheet".to_string(),
                display: "2026-05-17".to_string(),
            }),
            week_starts_on: "2026-05-17".to_string(),
            state: "Pending".to_string(),
            cards: vec![
                timecard(
                    "card-1",
                    "PRJ0161219",
                    "project_work",
                    "Development",
                    ["0", "2", "0", "0", "0", "0", "0"],
                ),
                timecard(
                    "card-2",
                    "PRJ0161126",
                    "project_work",
                    "Meeting",
                    ["0", "0", "0", "0", "2", "0", "0"],
                ),
            ],
        };

        let rendered = format_timecard_sheet(&sheet);

        assert!(rendered.contains("Time sheet - week of 2026-05-17 - Pending"));
        assert!(rendered.contains("Idx"));
        assert!(rendered.contains("Sun"));
        assert!(rendered.contains("Sat"));
        assert!(rendered.contains("PRJ0161219"));
        assert!(rendered.contains("Development"));
        assert!(rendered.contains("Totals"));
        assert!(rendered.contains(" 4"));
    }

    fn timecard(
        sys_id: &str,
        task_number: &str,
        category: &str,
        project_time_category: &str,
        hours: [&str; 7],
    ) -> TimeCard {
        TimeCard {
            sys_id: sys_id.to_string(),
            time_sheet: Some(SimpleRef {
                sys_id: "sheet-1".to_string(),
                table: "time_sheet".to_string(),
                display: "2026-05-17".to_string(),
            }),
            week_starts_on: "2026-05-17".to_string(),
            user: UserRef {
                sys_id: "user-1".to_string(),
                user_name: Some("worker".to_string()),
                email: Some("worker@example.com".to_string()),
                display: "Worker".to_string(),
            },
            task: Some(RecordRef {
                sys_id: format!("{task_number}-sys-id"),
                number: task_number.to_string(),
                table: "pm_project".to_string(),
            }),
            category: category.to_string(),
            category_label: "Project/Project Task".to_string(),
            project_time_category: Some(project_time_category.to_string()),
            hours: hours.map(str::to_string),
            total: hours
                .iter()
                .filter_map(|value| value.parse::<f64>().ok())
                .sum::<f64>()
                .to_string(),
            state: "Pending".to_string(),
            sys_updated_on: "2026-05-17 00:00:00".to_string(),
            sys_mod_count: Some(1),
        }
    }
}

pub fn print_multiline_field_pub(label: &str, value: Option<&str>) {
    print_multiline_field(label, value);
}

pub fn print_full_dump(value: &serde_json::Value) {
    let pretty = serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string());

    // Try to pipe through pager
    let pager = std::env::var("PAGER").unwrap_or_else(|_| "less".to_string());
    let mut command = Command::new(&pager);
    strip_secret_env(command.stdin(std::process::Stdio::piped()));
    if let Ok(mut child) = command.spawn() {
        if let Some(ref mut stdin) = child.stdin {
            let _ = stdin.write_all(pretty.as_bytes());
        }
        let _ = child.wait();
    } else {
        // Fallback: just print
        println!("{pretty}");
    }
}

fn print_field(label: &str, value: Option<&str>) {
    if let Some(v) = value {
        println!("{:>13} {}", format!("{label}:").dimmed(), v);
    }
}

fn print_multiline_field(label: &str, value: Option<&str>) {
    if let Some(v) = value {
        let v = v.trim();
        if v.is_empty() {
            return;
        }
        println!("\n{}", format!("{label}:").bold().underline());
        println!("{}", strip_html(v));
    }
}

/// Convert simple HTML (as returned by ServiceNow rich-text fields) to plain text.
pub fn strip_html(html: &str) -> String {
    let mut out = String::with_capacity(html.len());
    let mut in_tag = false;
    let mut tag_name = String::new();
    let mut collecting_tag = false;

    for ch in html.chars() {
        if ch == '<' {
            in_tag = true;
            tag_name.clear();
            collecting_tag = true;
            continue;
        }
        if in_tag {
            if collecting_tag {
                if ch.is_ascii_alphanumeric() || ch == '/' {
                    tag_name.push(ch.to_ascii_lowercase());
                } else {
                    collecting_tag = false;
                }
            }
            if ch == '>' {
                in_tag = false;
                let name = tag_name.trim_start_matches('/');
                match name {
                    "br" => out.push('\n'),
                    "p" | "div" if !tag_name.starts_with('/') && !out.ends_with('\n') => {
                        // Add blank line before block elements (but avoid double blanks)
                        out.push('\n');
                    }
                    "li" if !tag_name.starts_with('/') => {
                        if !out.ends_with('\n') {
                            out.push('\n');
                        }
                        out.push_str("  • ");
                    }
                    _ => {}
                }
            }
            continue;
        }
        // Decode common HTML entities
        if ch == '&' {
            // Peek-ahead not available char-by-char, so we handle entities in a post-pass below
        }
        out.push(ch);
    }

    // Decode HTML entities
    let out = out
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&nbsp;", " ");

    // Collapse runs of blank lines into at most one
    let mut result = String::with_capacity(out.len());
    let mut blank_count = 0;
    for line in out.lines() {
        if line.trim().is_empty() {
            blank_count += 1;
            if blank_count <= 1 {
                result.push('\n');
            }
        } else {
            blank_count = 0;
            if !result.is_empty() {
                result.push('\n');
            }
            result.push_str(line);
        }
    }

    result.trim().to_string()
}

use crate::tui_client::{
    BusinessApplicationDiagnostic, BusinessApplicationFieldValue, BusinessApplicationRef,
    BusinessApplicationView, ServerFieldValue, ServerRef, ServerView,
};

/// Render a reference as `display name (sys_id)`, or `-` when absent.
fn format_ba_ref(reference: Option<&BusinessApplicationRef>) -> String {
    match reference {
        Some(r) if !r.display_name.trim().is_empty() => {
            format!("{} ({})", r.display_name, r.sys_id)
        }
        Some(r) => r.sys_id.clone(),
        None => "-".to_string(),
    }
}

/// Render a field value, preferring the display value when present.
fn format_ba_field_value(value: &BusinessApplicationFieldValue) -> String {
    match value.display_value.as_deref() {
        Some(display) if !display.trim().is_empty() && display != value.value => {
            format!("{} [{}]", display, value.value)
        }
        Some(display) if !display.trim().is_empty() => display.to_string(),
        _ => value.value.clone(),
    }
}

/// Format the human-readable Business Application detail.
///
/// Per spec §13 the default view shows name, sys_id, owners, groups, portfolio,
/// operational status, attested date, vault path, and unresolved-reference
/// count. `full` appends the all-fields table.
pub fn format_business_application(app: &BusinessApplicationView, full: bool) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    let _ = writeln!(out, "{} ({})", app.name.bold(), app.sys_id);
    if !app.number.trim().is_empty() {
        let _ = writeln!(out, "{:>22} {}", "number:".dimmed(), app.number);
    }
    let _ = writeln!(
        out,
        "{:>22} {}",
        "business owner:".dimmed(),
        format_ba_ref(app.business_owner.as_ref())
    );
    let _ = writeln!(
        out,
        "{:>22} {}",
        "information owner:".dimmed(),
        format_ba_ref(app.is_owner.as_ref())
    );
    let _ = writeln!(
        out,
        "{:>22} {}",
        "ci owner group:".dimmed(),
        format_ba_ref(app.ci_owner_group.as_ref())
    );
    let _ = writeln!(
        out,
        "{:>22} {}",
        "support group:".dimmed(),
        format_ba_ref(app.primary_support_group.as_ref())
    );
    let _ = writeln!(
        out,
        "{:>22} {}",
        "portfolio:".dimmed(),
        format_ba_ref(app.primary_portfolio.as_ref())
    );
    let operational = app
        .operational_state
        .as_ref()
        .map(format_ba_field_value)
        .unwrap_or_else(|| "-".to_string());
    let _ = writeln!(
        out,
        "{:>22} {}",
        "operational status:".dimmed(),
        operational
    );
    let _ = writeln!(
        out,
        "{:>22} {}",
        "attested date:".dimmed(),
        app.attested_date.as_deref().unwrap_or("-")
    );
    let _ = writeln!(
        out,
        "{:>22} {}",
        "vault path:".dimmed(),
        app.vault_relative_path.as_deref().unwrap_or("-")
    );
    let _ = writeln!(
        out,
        "{:>22} {}",
        "unresolved refs:".dimmed(),
        app.unresolved_references.len()
    );
    if let Some(url) = &app.browser_url {
        let _ = writeln!(out, "{:>22} {}", "url:".dimmed(), url);
    }

    // List each unresolved reference so the count is actionable.
    if !app.unresolved_references.is_empty() {
        let _ = writeln!(out, "unresolved references:");
        for diag in &app.unresolved_references {
            let _ = writeln!(out, "  - {}", format_ba_diagnostic(diag));
        }
    }

    if full {
        let _ = writeln!(out, "fields:");
        if app.fields.is_empty() {
            let _ = writeln!(out, "  (none)");
        }
        for (key, value) in &app.fields {
            let _ = writeln!(out, "  {}: {}", key, format_ba_field_value(value));
        }
    }

    out
}

/// One-line description of an unresolved reference diagnostic.
fn format_ba_diagnostic(diag: &BusinessApplicationDiagnostic) -> String {
    let mut parts = vec![diag.field.clone()];
    if let Some(table) = &diag.table {
        parts.push(format!("table={table}"));
    }
    if let Some(sys_id) = &diag.sys_id {
        parts.push(format!("sys_id={sys_id}"));
    }
    parts.push(diag.diagnostic.clone());
    parts.join(" ")
}

/// Compact one-line summary used in `business-app search` listings.
pub fn format_business_application_summary(app: &BusinessApplicationView) -> String {
    let operational = app
        .operational_state
        .as_ref()
        .map(format_ba_field_value)
        .unwrap_or_else(|| "-".to_string());
    format!(
        "{} ({}) — owner: {} — state: {} — unresolved: {}",
        app.name.bold(),
        app.sys_id,
        format_ba_ref(app.business_owner.as_ref()),
        operational,
        app.unresolved_references.len()
    )
}

/// Render a server reference as `display name (sys_id)`, or `-` when absent.
fn format_server_ref(reference: Option<&ServerRef>) -> String {
    match reference {
        Some(r) if !r.display_name.trim().is_empty() => {
            format!("{} ({})", r.display_name, r.sys_id)
        }
        Some(r) => r.sys_id.clone(),
        None => "-".to_string(),
    }
}

/// Render a server field value, preferring the display value when present.
fn format_server_field_value(value: &ServerFieldValue) -> String {
    match value.display_value.as_deref() {
        Some(display) if !display.trim().is_empty() && display != value.value => {
            format!("{} [{}]", display, value.value)
        }
        Some(display) if !display.trim().is_empty() => display.to_string(),
        _ => value.value.clone(),
    }
}

/// Format a human-readable Server detail.
pub fn format_server(server: &ServerView, full: bool) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    let _ = writeln!(out, "{} ({})", server.name.bold(), server.sys_id);
    if !server.number.trim().is_empty() {
        let _ = writeln!(out, "{:>18} {}", "number:".dimmed(), server.number);
    }
    let _ = writeln!(
        out,
        "{:>18} {}",
        "class:".dimmed(),
        server.class_name.as_deref().unwrap_or("-")
    );
    let _ = writeln!(
        out,
        "{:>18} {}",
        "ip address:".dimmed(),
        server.ip_address.as_deref().unwrap_or("-")
    );
    let _ = writeln!(
        out,
        "{:>18} {}",
        "ci owner group:".dimmed(),
        format_server_ref(server.ci_owner_group.as_ref())
    );
    let _ = writeln!(
        out,
        "{:>18} {}",
        "support group:".dimmed(),
        format_server_ref(server.support_group.as_ref())
    );
    let status = server
        .operational_status
        .as_ref()
        .map(format_server_field_value)
        .unwrap_or_else(|| "-".to_string());
    let _ = writeln!(out, "{:>18} {}", "status:".dimmed(), status);
    let _ = writeln!(
        out,
        "{:>18} {}",
        "vault path:".dimmed(),
        server.vault_relative_path.as_deref().unwrap_or("-")
    );
    if let Some(url) = &server.browser_url {
        let _ = writeln!(out, "{:>18} {}", "url:".dimmed(), url);
    }

    if full {
        let _ = writeln!(out, "fields:");
        if server.fields.is_empty() {
            let _ = writeln!(out, "  (none)");
        }
        for (key, value) in &server.fields {
            let _ = writeln!(out, "  {}: {}", key, format_server_field_value(value));
        }
    }

    out
}

/// Compact one-line summary used in `server search` and `server query` listings.
pub fn format_server_summary(server: &ServerView) -> String {
    let status = server
        .operational_status
        .as_ref()
        .map(format_server_field_value)
        .unwrap_or_else(|| "-".to_string());
    format!(
        "{} ({}) - ip: {} - class: {} - ci owner group: {} - status: {}",
        server.name.bold(),
        server.sys_id,
        server.ip_address.as_deref().unwrap_or("-"),
        server.class_name.as_deref().unwrap_or("-"),
        format_server_ref(server.ci_owner_group.as_ref()),
        status
    )
}

/// Generically render the dictionary `fields` array returned by the daemon.
///
/// The backend owns and may enrich the per-field object shape, so this renders
/// each entry by surfacing a name-ish key (`field`/`name`/`element`/`column`)
/// followed by all remaining key/value pairs — without hard-binding to a fixed
/// schema beyond the name key.
pub fn format_business_application_fields(fields: &serde_json::Value) -> String {
    format_observed_fields(fields, "No Business Application fields found.\n")
}

/// Generically render the Server dictionary/observed fields array.
pub fn format_server_fields(fields: &serde_json::Value) -> String {
    format_observed_fields(fields, "No Server fields found.\n")
}

fn format_observed_fields(fields: &serde_json::Value, empty_message: &str) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    let Some(entries) = fields.as_array() else {
        // Not an array; fall back to pretty JSON so nothing is lost.
        return serde_json::to_string_pretty(fields).unwrap_or_else(|_| fields.to_string());
    };
    if entries.is_empty() {
        return empty_message.to_string();
    }
    for entry in entries {
        match entry {
            serde_json::Value::Object(map) => {
                // Pick the first present name-ish key as the heading.
                let name = ["field", "name", "element", "column", "column_name"]
                    .iter()
                    .find_map(|k| map.get(*k).and_then(|v| v.as_str()))
                    .unwrap_or("(field)");
                let _ = writeln!(out, "{}", name.bold());
                for (key, value) in map {
                    if value.as_str() == Some(name) {
                        continue;
                    }
                    let _ = writeln!(out, "  {}: {}", key, render_json_scalar(value));
                }
            }
            other => {
                let _ = writeln!(out, "{}", render_json_scalar(other));
            }
        }
    }
    out
}

/// Generically render a single summary object (e.g. `business-app sync`).
///
/// The backend owns this shape, so each top-level key/value pair is printed as a
/// line rather than binding to specific summary field names.
pub fn format_business_application_summary_object(summary: &serde_json::Value) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    match summary {
        serde_json::Value::Object(map) => {
            if map.is_empty() {
                return "(empty summary)\n".to_string();
            }
            for (key, value) in map {
                let _ = writeln!(out, "{}: {}", key, render_json_scalar(value));
            }
        }
        other => {
            return serde_json::to_string_pretty(other).unwrap_or_else(|_| other.to_string());
        }
    }
    out
}

/// Render a JSON value as a compact one-line string for key/value output.
fn render_json_scalar(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => "-".to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod business_app_tests {
    use super::*;
    use crate::tui_client::{BusinessApplicationRef, BusinessApplicationView};

    fn sample_view() -> BusinessApplicationView {
        BusinessApplicationView {
            sys_id: "sys123".to_string(),
            number: "BAPP0001".to_string(),
            name: "Epic".to_string(),
            business_owner: Some(BusinessApplicationRef {
                sys_id: "owner1".to_string(),
                table: "sys_user".to_string(),
                display_name: "Jane Doe".to_string(),
            }),
            is_owner: None,
            ci_owner_group: None,
            primary_support_group: None,
            operational_state: Some(BusinessApplicationFieldValue {
                value: "1".to_string(),
                display_value: Some("Operational".to_string()),
            }),
            primary_portfolio: None,
            attested_date: Some("2026-01-01".to_string()),
            fields: vec![(
                "u_custom".to_string(),
                BusinessApplicationFieldValue {
                    value: "v".to_string(),
                    display_value: None,
                },
            )],
            unresolved_references: vec![],
            browser_url: None,
            vault_relative_path: Some("business_applications/epic-sys123.md".to_string()),
        }
    }

    #[test]
    fn detail_shows_spec_required_fields() {
        let out = format_business_application(&sample_view(), false);
        assert!(out.contains("Epic"));
        assert!(out.contains("sys123"));
        assert!(out.contains("Jane Doe"));
        assert!(out.contains("Operational"));
        assert!(out.contains("2026-01-01"));
        assert!(out.contains("business_applications/epic-sys123.md"));
        assert!(out.contains("unresolved refs:"));
        // Without --full, the all-fields table is omitted.
        assert!(!out.contains("u_custom"));
    }

    #[test]
    fn full_detail_includes_field_table() {
        let out = format_business_application(&sample_view(), true);
        assert!(out.contains("u_custom"));
    }

    #[test]
    fn summary_is_one_line_with_owner_and_state() {
        let out = format_business_application_summary(&sample_view());
        assert!(out.contains("Epic"));
        assert!(out.contains("Jane Doe"));
        assert!(out.contains("Operational"));
    }

    #[test]
    fn fields_render_generically_by_name_key() {
        let json = serde_json::json!([
            { "field": "business_owner", "type": "reference", "reference": "sys_user" },
            { "name": "operational_state", "type": "integer" }
        ]);
        let out = format_business_application_fields(&json);
        assert!(out.contains("business_owner"));
        assert!(out.contains("reference"));
        assert!(out.contains("operational_state"));
    }

    #[test]
    fn summary_object_renders_key_value_lines() {
        let json = serde_json::json!({ "persisted": 3, "resolved_references": 5 });
        let out = format_business_application_summary_object(&json);
        assert!(out.contains("persisted: 3"));
        assert!(out.contains("resolved_references: 5"));
    }
}

#[cfg(test)]
mod server_tests {
    use super::*;
    use crate::tui_client::{ServerFieldValue, ServerRef, ServerView};

    fn sample_server() -> ServerView {
        ServerView {
            sys_id: "server123".to_string(),
            number: "SERVER:server123".to_string(),
            name: "app01.example.internal".to_string(),
            ip_address: Some("192.0.2.10".to_string()),
            class_name: Some("cmdb_ci_linux_server".to_string()),
            ci_owner_group: Some(ServerRef {
                sys_id: "group1".to_string(),
                table: "sys_user_group".to_string(),
                display_name: "Platform Operations".to_string(),
            }),
            support_group: None,
            operational_status: Some(ServerFieldValue {
                value: "1".to_string(),
                display_value: Some("Operational".to_string()),
            }),
            fields: vec![(
                "u_custom".to_string(),
                ServerFieldValue {
                    value: "v".to_string(),
                    display_value: None,
                },
            )],
            browser_url: None,
            vault_relative_path: Some(
                "servers/server_server123_app01-example-internal.md".to_string(),
            ),
        }
    }

    #[test]
    fn server_detail_renders_core_fields() {
        let out = format_server(&sample_server(), false);
        assert!(out.contains("app01.example.internal"));
        assert!(out.contains("192.0.2.10"));
        assert!(out.contains("cmdb_ci_linux_server"));
        assert!(out.contains("Platform Operations"));
        assert!(out.contains("Operational"));
        assert!(!out.contains("u_custom"));
    }

    #[test]
    fn full_server_detail_includes_field_table() {
        let out = format_server(&sample_server(), true);
        assert!(out.contains("u_custom"));
    }

    #[test]
    fn server_fields_render_generically_by_name_key() {
        let json = serde_json::json!([
            { "field": "managed_by_group", "type": "reference", "reference": "sys_user_group" },
            { "name": "ip_address", "type": "string" }
        ]);
        let out = format_server_fields(&json);
        assert!(out.contains("managed_by_group"));
        assert!(out.contains("ip_address"));
    }
}
