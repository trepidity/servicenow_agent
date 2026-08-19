//! Cache/Vault tab — status block, actions, last verify result.

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Paragraph};

use crate::admin::app::AdminApp;

pub fn render(f: &mut Frame, area: Rect, app: &mut AdminApp) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(10),
            Constraint::Length(8),
            Constraint::Min(1),
        ])
        .split(area);

    render_status(f, layout[0], app);
    render_actions(f, layout[1]);
    render_last_verify(f, layout[2], app);
}

fn render_status(f: &mut Frame, area: Rect, app: &AdminApp) {
    let lines = match &app.cache_info {
        Some(c) => vec![
            Line::raw(format!("Vault: {}", c.vault_path)),
            Line::raw(format!("SQLite: {}", c.sqlite_path)),
            Line::raw(format!("Schema version: {}", c.schema_version)),
            Line::raw(format!("DB size: {} MB", c.db_size_mb)),
            Line::raw(format!("Total rows: {}", c.total_rows)),
        ],
        None => vec![Line::raw("(loading…)")],
    };
    let block = Block::default()
        .title("Cache / Vault")
        .borders(Borders::ALL);
    f.render_widget(Paragraph::new(lines).block(block), area);
}

fn render_actions(f: &mut Frame, area: Rect) {
    let lines = vec![
        Line::raw("Actions:"),
        Line::raw("  [v] verify vault"),
        Line::raw("  cache replacement: stop daemon and use snow rebuild-cache"),
        Line::raw("  [p] prune orphans (DESTRUCTIVE)"),
        Line::raw("  [f] repair vault (DESTRUCTIVE)"),
    ];
    let block = Block::default().title("Actions").borders(Borders::ALL);
    f.render_widget(Paragraph::new(lines).block(block), area);
}

fn render_last_verify(f: &mut Frame, area: Rect, app: &AdminApp) {
    let block = Block::default()
        .title("Last verify result")
        .borders(Borders::ALL);
    let body = match &app.last_verify_summary {
        Some(s) => Paragraph::new(s.clone()),
        None => Paragraph::new("(no verify run this session)"),
    };
    f.render_widget(body.block(block), area);
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    #[test]
    fn renders_cache_vault_actions() {
        let backend = TestBackend::new(100, 30);
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
        assert!(dump.contains("Cache / Vault"), "missing title:\n{dump}");
        assert!(dump.contains("verify vault"), "missing verify:\n{dump}");
        assert!(
            dump.contains("stop daemon"),
            "missing offline lifecycle guidance:\n{dump}"
        );
    }
}
