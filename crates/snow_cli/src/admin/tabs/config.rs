//! Config tab — env / paths / credential provider readouts and toggle actions.

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Paragraph};

use crate::admin::app::AdminApp;
use crate::daemon_cmd::paths::DaemonPaths;

pub fn render(f: &mut Frame, area: Rect, app: &mut AdminApp) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(12), Constraint::Min(1)])
        .split(area);

    render_config(f, layout[0], app);
    render_actions(f, layout[1]);
}

fn render_config(f: &mut Frame, area: Rect, _app: &AdminApp) {
    let env = crate::daemon_cmd::paths::selected_daemon_start_env(None);
    let paths = DaemonPaths::resolve().ok();
    let provider = credential_provider_summary();
    let lines = vec![
        Line::raw(format!("Env: {env}")),
        Line::raw(format!(
            "Config dir: {}",
            paths
                .as_ref()
                .map(|p| p.config_dir.display().to_string())
                .unwrap_or_default()
        )),
        Line::raw(format!(
            "Socket: {}",
            paths
                .as_ref()
                .map(|p| p.socket.display().to_string())
                .unwrap_or_default()
        )),
        Line::raw(format!("Credential provider: {provider}")),
        Line::raw(format!(
            "Password env: {}",
            configured_flag(password_env_set())
        )),
        Line::raw(format!(
            "1Password item: {}",
            configured_flag(one_password_item_set())
        )),
    ];
    let block = Block::default().title("Config").borders(Borders::ALL);
    f.render_widget(Paragraph::new(lines).block(block), area);
}

fn render_actions(f: &mut Frame, area: Rect) {
    let lines = vec![
        Line::raw("Actions:"),
        Line::raw("  [e] toggle env (test ↔ prd) (DESTRUCTIVE — restarts daemon)"),
        Line::raw("  [o] re-check active credential provider"),
    ];
    let block = Block::default().title("Actions").borders(Borders::ALL);
    f.render_widget(Paragraph::new(lines).block(block), area);
}

fn password_env_set() -> bool {
    std::env::var("SERVICENOW_PASSWORD")
        .or_else(|_| std::env::var("SNOW_PASSWORD"))
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
}

fn one_password_item_set() -> bool {
    std::env::var("OP_ITEM_ID")
        .or_else(|_| std::env::var("SNOW_OP_ITEM"))
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
}

fn configured_flag(configured: bool) -> &'static str {
    if configured { "configured" } else { "unset" }
}

fn credential_provider_summary() -> &'static str {
    if password_env_set() {
        "env"
    } else if one_password_item_set() {
        "1Password"
    } else {
        "env (missing password)"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    #[test]
    fn renders_config_readout_and_actions() {
        let backend = TestBackend::new(120, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        let mut app = crate::admin::app::AdminApp::test_default();
        terminal.draw(|f| render(f, f.area(), &mut app)).unwrap();
        let buf = terminal.backend().buffer();
        let dump: String = (0..buf.area.height)
            .map(|y| {
                (0..buf.area.width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(dump.contains("Env:"), "missing Env::\n{dump}");
        assert!(dump.contains("Config dir:"), "missing Config dir:\n{dump}");
        assert!(dump.contains("toggle env"), "missing toggle env:\n{dump}");
    }
}
