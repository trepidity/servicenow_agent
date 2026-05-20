//! Two-tier confirmation modal for destructive admin actions.
//!
//! The modal is opaque-rendered (`Clear`) over the current frame and gates
//! all keypresses while present: `y`/`Y` confirms; anything else cancels.

use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Clear, Paragraph};

/// A pending confirmation populated by a tab when the user requests a
/// destructive operation. Cleared by `AdminApp::handle_key` once the user
/// has either confirmed or cancelled.
#[derive(Debug, Clone)]
pub struct PendingConfirm {
    /// Window title (e.g. "Confirm Prune").
    pub title: String,
    /// One-line description of the action ("Prune orphan rows").
    pub action_label: String,
    /// One-line impact summary ("47 rows across 3 tables").
    pub impact: String,
    /// What to actually do if the user confirms.
    pub confirm: ConfirmAction,
}

/// Possible follow-through actions for a [`PendingConfirm`].
#[derive(Debug, Clone)]
pub enum ConfirmAction {
    /// Start a daemon job by kind + params (`start_job` RPC).
    StartJob {
        kind: crate::admin::rpc_client::JobKindWire,
        params: serde_json::Value,
    },
    /// Spawn `snow daemon stop` to terminate the daemon.
    DaemonStop,
    /// Spawn `snow daemon restart`.
    DaemonRestart,
    /// Toggle the active environment (Task 16 fills this in).
    ToggleEnv,
}

/// Render the confirmation popup centered inside `area`.
pub fn render(f: &mut Frame, area: Rect, c: &PendingConfirm) {
    let popup = centered_rect(60, 50, area);
    f.render_widget(Clear, popup);
    let block = Block::default()
        .title(c.title.clone())
        .borders(Borders::ALL)
        .style(Style::default().fg(Color::Yellow));
    let body = Paragraph::new(vec![
        Line::from(c.action_label.clone()),
        Line::from(""),
        Line::from(c.impact.clone()),
        Line::from(""),
        Line::from("[y] Confirm    [n] Cancel"),
    ])
    .alignment(Alignment::Left)
    .block(block);
    f.render_widget(body, popup);
}

/// Compute a centered rectangle of `percent_x` × `percent_y` of `r`.
fn centered_rect(percent_x: u16, percent_y: u16, r: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    #[test]
    fn modal_renders_action_and_impact() {
        let backend = TestBackend::new(80, 20);
        let mut terminal = Terminal::new(backend).unwrap();
        let pending = PendingConfirm {
            title: "Confirm Prune".into(),
            action_label: "Prune orphan rows".into(),
            impact: "47 rows across 3 tables".into(),
            confirm: ConfirmAction::StartJob {
                kind: crate::admin::rpc_client::JobKindWire::PruneOrphans,
                params: serde_json::json!({}),
            },
        };
        terminal.draw(|f| render(f, f.area(), &pending)).unwrap();
        let buf = terminal.backend().buffer();
        let dump: String = (0..buf.area.height)
            .map(|y| {
                (0..buf.area.width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(dump.contains("Confirm Prune"), "missing title:\n{dump}");
        assert!(
            dump.contains("Prune orphan rows"),
            "missing action label:\n{dump}"
        );
        assert!(dump.contains("47 rows"), "missing impact:\n{dump}");
        assert!(
            dump.contains("[y] Confirm"),
            "missing confirm hint:\n{dump}"
        );
    }
}
