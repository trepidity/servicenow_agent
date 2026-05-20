use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState, Wrap};
use snow_core::display as core_display;
use snow_core::{TaskSlaReadability, TaskSlaStatus, TaskSlaView};

use super::{
    App, BackgroundJobKey, BrowserSelection, DetailChildColumn, DetailChildSummaryModel,
    DetailChildValue, DetailColumnWidth, EntityKind, PrefetchTask, RecordRef, TaskSlaDetail,
};

const TASK_SLA_ROW_LIMIT: usize = 3;

pub(super) fn render_browser(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let Some(_browser) = app.browser.as_ref() else {
        let empty = Paragraph::new("No browser state loaded.")
            .block(Block::default().title("Browser").borders(Borders::ALL));
        frame.render_widget(empty, area);
        return;
    };

    let sections =
        Layout::horizontal([Constraint::Percentage(35), Constraint::Percentage(65)]).split(area);
    render_left(frame, sections[0], app);
    render_right(frame, sections[1], app);
}

fn render_left(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let browser = app.browser.as_ref().expect("browser state");
    let sections = Layout::vertical([Constraint::Length(8), Constraint::Min(5)]).split(area);

    let mut lines = vec![Line::from(vec![
        Span::styled(
            browser.parent.number.clone(),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!("  {}", browser.parent.short_description)),
    ])];
    if let Some(state) = &browser.parent.state_label {
        lines.push(Line::from(format!("State: {state}")));
    }
    if let Some(parent_number) = &browser.parent.parent_number {
        lines.push(Line::from(format!("Parent: {parent_number}")));
    }
    if browser.approval.is_some() {
        lines.push(Line::from("Approval: requested"));
    }

    let summary = Paragraph::new(lines)
        .block(Block::default().title("Parent").borders(Borders::ALL))
        .wrap(Wrap { trim: false });
    frame.render_widget(summary, sections[0]);

    if browser.children.is_empty() {
        let empty = Paragraph::new("No child records")
            .block(Block::default().title("Children").borders(Borders::ALL));
        frame.render_widget(empty, sections[1]);
        return;
    }

    let visible_indices = app.visible_browser_child_indices();
    if visible_indices.is_empty() {
        let empty = Paragraph::new("No child records match the current filter")
            .block(Block::default().title("Children").borders(Borders::ALL));
        frame.render_widget(empty, sections[1]);
        return;
    }

    let is_change_request = browser.parent.kind == Some(EntityKind::ChangeRequest);
    let is_resource_plan_list = browser.relation.child_kind == Some(EntityKind::ResourcePlan)
        || browser
            .children
            .iter()
            .any(|child| child.kind == Some(EntityKind::ResourcePlan));
    let rows: Vec<Row<'static>> = visible_indices
        .iter()
        .filter_map(|index| browser.children.get(*index))
        .map(|child| {
            if is_change_request {
                Row::new(vec![
                    Cell::from(child.number.clone()),
                    Cell::from(child.assigned_to.clone().unwrap_or_else(|| "-".to_string())),
                    Cell::from(
                        child
                            .planned_start
                            .clone()
                            .unwrap_or_else(|| "-".to_string()),
                    ),
                    Cell::from(child.state_label.clone().unwrap_or_else(|| "-".to_string())),
                ])
            } else if is_resource_plan_list {
                Row::new(vec![
                    Cell::from(child.number.clone()),
                    Cell::from(resource_plan_resource_label(child)),
                    Cell::from(
                        child
                            .planned_start
                            .clone()
                            .unwrap_or_else(|| "-".to_string()),
                    ),
                    Cell::from(child.state_label.clone().unwrap_or_else(|| "-".to_string())),
                ])
            } else {
                Row::new(vec![
                    Cell::from(child.number.clone()),
                    Cell::from(child.short_description.clone()),
                    Cell::from(child.state_label.clone().unwrap_or_else(|| "-".to_string())),
                ])
            }
        })
        .collect();

    let (widths, headers, title) = if is_change_request {
        (
            vec![
                Constraint::Length(14),
                Constraint::Length(18),
                Constraint::Length(20),
                Constraint::Length(14),
            ],
            vec!["CTASK", "Assigned To", "Planned Start", "State"],
            "Change Tasks".to_string(),
        )
    } else if is_resource_plan_list {
        (
            vec![
                Constraint::Length(14),
                Constraint::Percentage(40),
                Constraint::Length(16),
                Constraint::Length(16),
            ],
            vec!["RPLN", "Resource", "Start", "State"],
            resource_plan_title(browser),
        )
    } else {
        (
            vec![
                Constraint::Length(14),
                Constraint::Percentage(55),
                Constraint::Length(16),
            ],
            vec!["Number", "Short Description", "State"],
            "Children".to_string(),
        )
    };

    let table = Table::new(rows, widths)
        .header(
            Row::new(headers).style(
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
        )
        .block(Block::default().title(title).borders(Borders::ALL))
        .row_highlight_style(Style::default().bg(Color::Blue).fg(Color::Black));

    let mut state = TableState::default();
    if let BrowserSelection::Child(index) = browser.selected {
        let selected = visible_indices
            .iter()
            .position(|visible| *visible == index)
            .unwrap_or(0);
        state.select(Some(selected));
    }
    frame.render_stateful_widget(table, sections[1], &mut state);
}

fn resource_plan_resource_label(child: &super::RecordRef) -> String {
    child
        .assigned_to
        .clone()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| {
            if child.short_description.trim().is_empty() {
                "-".to_string()
            } else {
                child.short_description.clone()
            }
        })
}

fn resource_plan_title(browser: &super::BrowserState) -> String {
    let state = browser
        .child_state_filter
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("All");
    format!("Resource Plans (state: {state})")
}

fn render_right(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let browser = app.browser.as_ref().expect("browser state");
    let Some(detail) = browser.selected_detail() else {
        let loading = Paragraph::new("Loading record detail...")
            .block(Block::default().title("Detail").borders(Borders::ALL));
        frame.render_widget(loading, area);
        return;
    };

    let child_summary_model = selected_child_summary_model(browser, detail);
    let mut constraints = vec![Constraint::Length(3), Constraint::Length(8)];
    if !detail.extra_sections.is_empty() {
        constraints.push(Constraint::Length(10));
    }
    if child_summary_model.is_some() {
        constraints.push(Constraint::Length(8));
    }
    constraints.extend([
        Constraint::Length(task_sla_section_height(browser.task_sla_expanded)),
        Constraint::Min(6),
        Constraint::Length(8),
    ]);
    let sections = Layout::vertical(constraints).split(area);

    let header = Paragraph::new(Line::from(vec![
        Span::styled(
            detail.record.get_str("number").unwrap_or("-").to_string(),
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!(
            "  {}",
            detail
                .record
                .get_str("short_description")
                .unwrap_or("(no description)")
        )),
    ]))
    .block(
        Block::default()
            .title(detail.page.title)
            .borders(Borders::ALL),
    );
    frame.render_widget(header, sections[0]);

    let field_lines: Vec<Line<'static>> = detail
        .field_lines
        .iter()
        .map(|(label, value)| Line::from(format!("{label}: {value}")))
        .collect();
    let fields = Paragraph::new(field_lines)
        .block(Block::default().title("Fields").borders(Borders::ALL))
        .wrap(Wrap { trim: false });
    frame.render_widget(fields, sections[1]);

    let mut next_section = 2;
    if !detail.extra_sections.is_empty() {
        let extra_lines: Vec<Line<'static>> = detail
            .extra_sections
            .iter()
            .flat_map(|(label, value)| {
                let mut lines = vec![Line::from(format!("{label}:"))];
                lines.extend(value.lines().map(|line| Line::from(format!("  {line}"))));
                lines.push(Line::from(""));
                lines
            })
            .collect();
        let extra = Paragraph::new(extra_lines)
            .block(
                Block::default()
                    .title(detail.page.sections_title)
                    .borders(Borders::ALL),
            )
            .wrap(Wrap { trim: false });
        frame.render_widget(extra, sections[next_section]);
        next_section += 1;
    }

    if let Some(model) = child_summary_model {
        render_child_summary(frame, sections[next_section], app, browser, model);
        next_section += 1;
    }

    render_task_sla(
        frame,
        sections[next_section],
        &detail.task_sla,
        browser.task_sla_expanded,
    );
    next_section += 1;

    let description = Paragraph::new(
        detail
            .description
            .clone()
            .unwrap_or_else(|| "-".to_string()),
    )
    .block(Block::default().title("Description").borders(Borders::ALL))
    .wrap(Wrap { trim: false });
    frame.render_widget(description, sections[next_section]);

    let note_lines: Vec<Line<'static>> = if !detail.notes_loaded {
        vec![Line::from("Loading work notes...")]
    } else if detail.work_notes.is_empty() {
        vec![Line::from("No work notes")]
    } else {
        detail
            .work_notes
            .iter()
            .flat_map(|entry| {
                let mut lines = vec![Line::from(format!(
                    "{} - {}",
                    entry.timestamp, entry.author
                ))];
                for body in entry.body.lines() {
                    lines.push(Line::from(format!("  {body}")));
                }
                lines
            })
            .collect()
    };
    let notes = Paragraph::new(note_lines)
        .block(Block::default().title("Work Notes").borders(Borders::ALL))
        .wrap(Wrap { trim: false });
    frame.render_widget(notes, sections[next_section + 1]);
}

fn selected_child_summary_model(
    browser: &super::BrowserState,
    detail: &super::RecordDetail,
) -> Option<DetailChildSummaryModel> {
    matches!(browser.selected, BrowserSelection::Parent)
        .then_some(detail.page.child_summary)
        .flatten()
}

fn render_child_summary(
    frame: &mut Frame<'_>,
    area: Rect,
    app: &App,
    browser: &super::BrowserState,
    model: DetailChildSummaryModel,
) {
    let visible_indices = app.visible_browser_child_indices();
    let total = browser.children.len();
    let visible_children = visible_indices
        .iter()
        .filter_map(|index| browser.children.get(*index))
        .collect::<Vec<_>>();

    if visible_children.is_empty() {
        let message = if total > 0 {
            model.filtered_empty_text
        } else if child_records_loading(app, browser) {
            model.loading_text
        } else {
            model.empty_text
        };
        let empty = Paragraph::new(message)
            .block(Block::default().title(model.title).borders(Borders::ALL))
            .wrap(Wrap { trim: false });
        frame.render_widget(empty, area);
        return;
    }

    let row_limit = usize::from(area.height.saturating_sub(3))
        .max(1)
        .min(model.row_limit);
    let rows = visible_children
        .iter()
        .take(row_limit)
        .map(|plan| {
            Row::new(
                model
                    .columns
                    .iter()
                    .map(|column| Cell::from(child_column_value(plan, column.value)))
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();

    let table = Table::new(
        rows,
        model
            .columns
            .iter()
            .map(detail_column_constraint)
            .collect::<Vec<_>>(),
    )
    .header(
        Row::new(
            model
                .columns
                .iter()
                .map(|column| column.header)
                .collect::<Vec<_>>(),
        )
        .style(
            Style::default()
                .fg(Color::Yellow)
                .add_modifier(Modifier::BOLD),
        ),
    )
    .block(
        Block::default()
            .title(child_summary_title(
                browser,
                model,
                visible_children.len(),
                total,
            ))
            .borders(Borders::ALL),
    );
    frame.render_widget(table, area);
}

fn child_summary_title(
    browser: &super::BrowserState,
    model: DetailChildSummaryModel,
    visible_count: usize,
    total_count: usize,
) -> String {
    let state = browser
        .child_state_filter
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("All");
    format!(
        "{} ({visible_count}/{total_count}, state: {state})",
        model.title
    )
}

fn child_records_loading(app: &App, browser: &super::BrowserState) -> bool {
    let children_key = format!(
        "{}:{}:children",
        browser.parent.table, browser.parent.sys_id
    );
    app.inflight_jobs
        .contains(&BackgroundJobKey::Children(children_key))
        || app.prefetch_queue.iter().any(|task| {
            matches!(task, PrefetchTask::Children(record) if record.sys_id == browser.parent.sys_id)
        })
}

fn detail_column_constraint(column: &DetailChildColumn) -> Constraint {
    match column.width {
        DetailColumnWidth::Length(width) => Constraint::Length(width),
        DetailColumnWidth::Percentage(width) => Constraint::Percentage(width),
    }
}

fn child_column_value(record: &RecordRef, value: DetailChildValue) -> String {
    match value {
        DetailChildValue::Number => record.number.clone(),
        DetailChildValue::ShortDescription => {
            display_optional_owned(Some(&record.short_description))
        }
        DetailChildValue::AssignedTo => display_optional_owned(record.assigned_to.as_deref()),
        DetailChildValue::Resource => resource_plan_resource_label(record),
        DetailChildValue::PlannedStart => display_optional_owned(record.planned_start.as_deref()),
        DetailChildValue::PlannedEnd => display_optional_owned(record.planned_end.as_deref()),
        DetailChildValue::Hours => resource_plan_hours_label(record),
        DetailChildValue::State => display_optional_owned(record.state_label.as_deref()),
    }
}

fn resource_plan_hours_label(plan: &RecordRef) -> String {
    let planned = display_optional_owned(plan.planned_hours.as_deref());
    let allocated = display_optional_owned(plan.allocated_hours.as_deref());
    let confirmed = display_optional_owned(plan.confirmed_hours.as_deref());
    if planned == "-" && allocated == "-" && confirmed == "-" {
        "-".to_string()
    } else {
        format!("P:{planned} A:{allocated} C:{confirmed}")
    }
}

fn display_optional_owned(value: Option<&str>) -> String {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("-")
        .to_string()
}

fn task_sla_section_height(expanded: bool) -> u16 {
    if expanded { 7 } else { 3 }
}

fn render_task_sla(frame: &mut Frame<'_>, area: Rect, task_sla: &TaskSlaDetail, expanded: bool) {
    let paragraph = Paragraph::new(task_sla_lines(task_sla, expanded))
        .block(Block::default().title("Task SLA").borders(Borders::ALL))
        .wrap(Wrap { trim: false });
    frame.render_widget(paragraph, area);
}

fn task_sla_lines(task_sla: &TaskSlaDetail, expanded: bool) -> Vec<Line<'static>> {
    if !expanded {
        return vec![Line::from(task_sla_heading_line(task_sla))];
    }

    match task_sla {
        TaskSlaDetail::Loading => vec![Line::from("Loading Task SLA...")],
        TaskSlaDetail::Error(err) => vec![Line::from(format!("Task SLA error: {err}"))],
        TaskSlaDetail::Status(status) => task_sla_status_lines(status),
    }
}

fn task_sla_heading_line(task_sla: &TaskSlaDetail) -> String {
    match task_sla {
        TaskSlaDetail::Loading => "Loading Task SLA...".to_string(),
        TaskSlaDetail::Error(err) => format!("Task SLA error: {err}"),
        TaskSlaDetail::Status(status) => task_sla_status_heading(status),
    }
}

fn task_sla_status_heading(status: &TaskSlaStatus) -> String {
    match status.readable {
        TaskSlaReadability::ReadableRows => task_sla_summary_line(status),
        TaskSlaReadability::EmptyOrAclRestricted => {
            "No readable Task SLA rows or none attached; ACLs can also hide rows.".to_string()
        }
        TaskSlaReadability::ParentNotFound => format!(
            "Task SLA parent not found for {}.",
            display_or_unknown(&status.record_number)
        ),
        TaskSlaReadability::NotApplicable => format!(
            "Task SLAs do not apply to {} records.",
            display_or_unknown(&status.record_table)
        ),
    }
}

fn task_sla_status_lines(status: &TaskSlaStatus) -> Vec<Line<'static>> {
    let mut lines = vec![Line::from(task_sla_summary_line(status))];

    match status.readable {
        TaskSlaReadability::ReadableRows => {
            for (index, row) in status.rows.iter().take(TASK_SLA_ROW_LIMIT).enumerate() {
                lines.push(Line::from(format_task_sla_row(index + 1, row)));
            }
            if status.rows.len() > TASK_SLA_ROW_LIMIT {
                lines.push(Line::from(format!(
                    "Showing {TASK_SLA_ROW_LIMIT} of {} rows.",
                    status.rows.len()
                )));
            }
        }
        TaskSlaReadability::EmptyOrAclRestricted => {
            lines.push(Line::from(
                "No readable Task SLA rows or none attached; ACLs can also hide rows.",
            ));
        }
        TaskSlaReadability::ParentNotFound => {
            lines.push(Line::from(format!(
                "Task SLA parent not found for {}.",
                display_or_unknown(&status.record_number)
            )));
        }
        TaskSlaReadability::NotApplicable => {
            lines.push(Line::from(format!(
                "Task SLAs do not apply to {} records.",
                display_or_unknown(&status.record_table)
            )));
        }
    }

    lines
}

fn task_sla_summary_line(status: &TaskSlaStatus) -> String {
    format!(
        "total {} | active {} | breached {} | next {} | highest {}",
        status.summary.total,
        status.summary.active,
        status.summary.breached,
        format_task_sla_next_breach(status.summary.next_breach.as_ref()),
        core_display::format_business_elapsed(status.summary.highest_business_elapsed)
    )
}

fn format_task_sla_row(index: usize, row: &TaskSlaView) -> String {
    format!(
        "{index}. {} | {} | active {} | breached {} | elapsed {} | left {}",
        display_optional(row.name.as_deref()),
        display_optional(row.stage.as_deref()),
        display_bool(row.active),
        display_bool(row.breached),
        core_display::format_business_elapsed(row.business_elapsed_percentage),
        core_display::format_time_left(row.time_left.as_deref())
    )
}

fn format_task_sla_next_breach(row: Option<&TaskSlaView>) -> String {
    let Some(row) = row else {
        return "-".to_string();
    };

    format!(
        "{} ({})",
        display_optional(row.planned_end_time.as_deref()),
        display_optional(row.name.as_deref())
    )
}

fn display_bool(value: Option<bool>) -> &'static str {
    match value {
        Some(true) => "yes",
        Some(false) => "no",
        None => "-",
    }
}

fn display_optional(value: Option<&str>) -> &str {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("-")
}

fn display_or_unknown(value: &str) -> &str {
    let trimmed = value.trim();
    if trimmed.is_empty() { "-" } else { trimmed }
}

#[cfg(test)]
mod tests {
    use super::*;
    use snow_core::TaskSlaSummaryView;
    use std::collections::HashMap;

    fn sample_record(kind: EntityKind, sys_id: &str, number: &str) -> RecordRef {
        RecordRef {
            kind: Some(kind),
            table: match kind {
                EntityKind::Task => "task",
                EntityKind::ChangeRequest => "change_request",
                EntityKind::ChangeTask => "change_task",
                EntityKind::Demand => "dmn_demand",
                EntityKind::Project => "pm_project",
                EntityKind::ResourcePlan => "resource_plan",
                EntityKind::Story => "rm_story",
                EntityKind::ScrumTask => "rm_scrum_task",
                EntityKind::RequestItem => "sc_req_item",
                EntityKind::CatalogTask => "sc_task",
                EntityKind::Incident => "incident",
            }
            .to_string(),
            sys_id: sys_id.to_string(),
            number: number.to_string(),
            short_description: "Sample".to_string(),
            assigned_to: None,
            planned_start: None,
            planned_end: None,
            planned_hours: None,
            allocated_hours: None,
            confirmed_hours: None,
            state_label: Some("Open".to_string()),
            state_value: Some("1".to_string()),
            record_class: None,
            parent_table: None,
            parent_sys_id: None,
            parent_number: None,
            approval: None,
        }
    }

    fn sample_browser(parent: RecordRef, selected: BrowserSelection) -> super::super::BrowserState {
        super::super::BrowserState {
            parent: parent.clone(),
            parent_kind: parent.kind.unwrap_or(EntityKind::Incident),
            relation: super::super::ChildRelation {
                parent_kind: parent.kind.unwrap_or(EntityKind::Incident),
                child_kind: Some(EntityKind::ResourcePlan),
                child_table: Some("resource_plan"),
                child_link_field: Some("task"),
            },
            parent_detail: super::super::fetch::placeholder_detail(&parent),
            children: Vec::new(),
            selected,
            task_sla_expanded: false,
            detail_cache: HashMap::new(),
            approval: None,
            search: super::super::QuickSearch::new(),
            child_state_filter: None,
        }
    }

    #[test]
    fn project_parent_detail_exposes_resource_plan_summary_model() {
        let project = sample_record(EntityKind::Project, "project-sys", "PRJ0161206");
        let mut browser = sample_browser(project, BrowserSelection::Parent);

        let model = selected_child_summary_model(&browser, &browser.parent_detail)
            .expect("project parent should expose resource plans");
        assert_eq!(model.title, "Resource Plans");

        browser.selected = BrowserSelection::Child(0);
        assert!(selected_child_summary_model(&browser, &browser.parent_detail).is_none());

        let incident = sample_record(EntityKind::Incident, "inc-sys", "INC0000001");
        let incident_browser = sample_browser(incident, BrowserSelection::Parent);
        assert!(
            selected_child_summary_model(&incident_browser, &incident_browser.parent_detail)
                .is_none()
        );
    }

    #[test]
    fn child_summary_title_includes_filter_and_counts() {
        let project = sample_record(EntityKind::Project, "project-sys", "PRJ0161206");
        let mut browser = sample_browser(project, BrowserSelection::Parent);
        browser.child_state_filter = Some("Allocated".to_string());
        let model = browser
            .parent_detail
            .page
            .child_summary
            .expect("project model should have child summary");

        assert_eq!(
            child_summary_title(&browser, model, 2, 3),
            "Resource Plans (2/3, state: Allocated)"
        );
    }

    #[test]
    fn resource_plan_hours_label_uses_available_hour_fields() {
        let mut plan = sample_record(EntityKind::ResourcePlan, "rpln-sys", "RPLN0089255");
        plan.planned_hours = Some("80".to_string());
        plan.allocated_hours = Some("40".to_string());

        assert_eq!(resource_plan_hours_label(&plan), "P:80 A:40 C:-");

        plan.planned_hours = None;
        plan.allocated_hours = None;
        assert_eq!(resource_plan_hours_label(&plan), "-");
    }

    #[test]
    fn task_sla_lines_use_distinct_readability_text() {
        let readable = plain_lines(task_sla_lines(
            &TaskSlaDetail::Status(Box::new(status(
                TaskSlaReadability::ReadableRows,
                vec![row("Response", "In progress")],
            ))),
            true,
        ));
        let empty = plain_lines(task_sla_lines(
            &TaskSlaDetail::Status(Box::new(status(
                TaskSlaReadability::EmptyOrAclRestricted,
                Vec::new(),
            ))),
            true,
        ));
        let missing = plain_lines(task_sla_lines(
            &TaskSlaDetail::Status(Box::new(status(
                TaskSlaReadability::ParentNotFound,
                Vec::new(),
            ))),
            true,
        ));
        let not_applicable = plain_lines(task_sla_lines(
            &TaskSlaDetail::Status(Box::new(status(
                TaskSlaReadability::NotApplicable,
                Vec::new(),
            ))),
            true,
        ));

        assert!(readable.iter().any(|line| line.contains("1. Response")));
        assert!(
            empty
                .iter()
                .any(|line| line.contains("No readable Task SLA rows or none attached"))
        );
        assert!(
            missing
                .iter()
                .any(|line| line.contains("Task SLA parent not found"))
        );
        assert!(
            not_applicable
                .iter()
                .any(|line| line.contains("Task SLAs do not apply"))
        );
    }

    #[test]
    fn task_sla_lines_limit_rows() {
        let rendered = plain_lines(task_sla_lines(
            &TaskSlaDetail::Status(Box::new(status(
                TaskSlaReadability::ReadableRows,
                vec![
                    row("First", "Queued"),
                    row("Second", "In progress"),
                    row("Third", "Paused"),
                    row("Fourth", "Pending"),
                ],
            ))),
            true,
        ));

        assert!(rendered.iter().any(|line| line.contains("1. First")));
        assert!(rendered.iter().any(|line| line.contains("3. Third")));
        assert!(!rendered.iter().any(|line| line.contains("Fourth")));
        assert!(rendered.iter().any(|line| line == "Showing 3 of 4 rows."));
    }

    #[test]
    fn task_sla_lines_collapse_to_single_heading() {
        let rendered = plain_lines(task_sla_lines(
            &TaskSlaDetail::Status(Box::new(status(
                TaskSlaReadability::ReadableRows,
                vec![row("Response", "In progress")],
            ))),
            false,
        ));

        assert_eq!(rendered.len(), 1);
        assert!(rendered[0].contains("total 1 | active 1 | breached 0"));
        assert!(!rendered[0].contains("1. Response"));
    }

    #[test]
    fn task_sla_error_renders_inline() {
        let rendered = plain_lines(task_sla_lines(
            &TaskSlaDetail::Error("sample failure".to_string()),
            true,
        ));

        assert_eq!(rendered, vec!["Task SLA error: sample failure"]);
    }

    fn status(readable: TaskSlaReadability, rows: Vec<TaskSlaView>) -> TaskSlaStatus {
        TaskSlaStatus {
            record_number: "sample-record".to_string(),
            record_table: "incident".to_string(),
            record_sys_id: "sample-parent".to_string(),
            summary: TaskSlaSummaryView {
                total: rows.len(),
                active: rows.iter().filter(|row| row.active == Some(true)).count(),
                breached: rows.iter().filter(|row| row.breached == Some(true)).count(),
                next_breach: rows.first().cloned(),
                highest_business_elapsed: rows
                    .iter()
                    .filter_map(|row| row.business_elapsed_percentage)
                    .max_by(f64::total_cmp),
            },
            rows,
            readable,
        }
    }

    fn row(name: &str, stage: &str) -> TaskSlaView {
        TaskSlaView {
            name: Some(name.to_string()),
            stage: Some(stage.to_string()),
            active: Some(true),
            breached: Some(false),
            planned_end_time: Some("2030-01-01 00:00:00".to_string()),
            business_elapsed_percentage: Some(25.0),
            time_left: Some("1 day".to_string()),
        }
    }

    fn plain_lines(lines: Vec<Line<'static>>) -> Vec<String> {
        lines
            .into_iter()
            .map(|line| {
                line.spans
                    .into_iter()
                    .map(|span| span.content.into_owned())
                    .collect()
            })
            .collect()
    }
}
