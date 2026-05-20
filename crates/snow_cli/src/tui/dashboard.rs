use ratatui::Frame;
use ratatui::layout::{Constraint, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState, Tabs};

use super::{App, DashboardTab};

pub(super) fn render_dashboard(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let sections = Layout::vertical([Constraint::Length(3), Constraint::Min(6)]).split(area);
    render_tabs(frame, sections[0], app);
    render_active_table(frame, sections[1], app);
}

fn render_tabs(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let titles: Vec<Line<'static>> = DashboardTab::all()
        .into_iter()
        .map(|tab| {
            let count = app.dashboard_tab_visible_count(tab);
            Line::from(format!("{} ({count})", tab.title()))
        })
        .collect();

    let tabs = Tabs::new(titles)
        .select(app.dashboard.active_tab.index())
        .block(Block::default().title("Queues").borders(Borders::ALL))
        .highlight_style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        );
    frame.render_widget(tabs, area);
}

fn render_active_table(frame: &mut Frame<'_>, area: Rect, app: &App) {
    let tab = app.dashboard.active_tab;
    let data = app.dashboard.active_tab();
    if let Some(error) = &data.error {
        let message = Paragraph::new(error.clone())
            .style(Style::default().fg(Color::Red))
            .block(Block::default().title(tab.title()).borders(Borders::ALL));
        frame.render_widget(message, area);
        return;
    }

    let headers = headers_for(tab);
    let widths = widths_for(tab);
    let visible_indices = app.visible_dashboard_indices();
    let rows: Vec<Row<'static>> = visible_indices
        .iter()
        .filter_map(|index| data.rows.get(*index))
        .map(|row| {
            Row::new(
                row.cells
                    .iter()
                    .cloned()
                    .map(Cell::from)
                    .collect::<Vec<_>>(),
            )
        })
        .collect();

    let table = Table::new(rows, widths)
        .header(
            Row::new(headers.into_iter().map(Cell::from).collect::<Vec<_>>()).style(
                Style::default()
                    .fg(Color::Yellow)
                    .add_modifier(Modifier::BOLD),
            ),
        )
        .block(Block::default().title(tab.title()).borders(Borders::ALL))
        .row_highlight_style(Style::default().bg(Color::Blue).fg(Color::Black));

    let mut state = TableState::default();
    if !visible_indices.is_empty() {
        let selected = visible_indices
            .iter()
            .position(|index| *index == data.selected)
            .unwrap_or(0);
        state.select(Some(selected));
    }
    frame.render_stateful_widget(table, area, &mut state);
}

fn headers_for(tab: DashboardTab) -> Vec<&'static str> {
    match tab {
        DashboardTab::PendingApprovals => vec!["Number", "Short Description", "State", "Requested"],
        DashboardTab::MyTasks => vec![
            "Number",
            "Short Description",
            "State",
            "Assigned To",
            "Parent",
        ],
        DashboardTab::MyStories => vec!["Number", "Short Description", "State", "Points", "Sprint"],
        DashboardTab::MyIncidents => {
            vec![
                "Number",
                "Short Description",
                "State",
                "Priority",
                "SLA",
                "Opened",
            ]
        }
    }
}

fn widths_for(tab: DashboardTab) -> Vec<Constraint> {
    match tab {
        DashboardTab::PendingApprovals => vec![
            Constraint::Length(14),
            Constraint::Percentage(55),
            Constraint::Length(18),
            Constraint::Length(12),
        ],
        DashboardTab::MyTasks => vec![
            Constraint::Length(14),
            Constraint::Percentage(45),
            Constraint::Length(18),
            Constraint::Length(18),
            Constraint::Length(14),
        ],
        DashboardTab::MyStories => vec![
            Constraint::Length(14),
            Constraint::Percentage(50),
            Constraint::Length(18),
            Constraint::Length(8),
            Constraint::Length(18),
        ],
        DashboardTab::MyIncidents => vec![
            Constraint::Length(14),
            Constraint::Percentage(40),
            Constraint::Length(18),
            Constraint::Length(10),
            Constraint::Length(26),
            Constraint::Length(20),
        ],
    }
}
