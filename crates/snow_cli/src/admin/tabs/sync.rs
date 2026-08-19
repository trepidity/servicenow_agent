//! Sync tab — KB status block and sync/rebuild/refresh actions.

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Paragraph};

use crate::admin::app::AdminApp;

pub fn render(f: &mut Frame, area: Rect, app: &mut AdminApp) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(12), Constraint::Min(1)])
        .split(area);

    render_kb_status(f, layout[0], app);
    render_actions(f, layout[1], app);
}

fn render_kb_status(f: &mut Frame, area: Rect, app: &AdminApp) {
    let lines = match &app.kb_status {
        Some(s) => vec![
            Line::raw(format!(
                "Last full sync: {}",
                s.last_full_at.as_deref().unwrap_or("-")
            )),
            Line::raw(format!(
                "Last incremental sync: {}",
                s.last_incremental_at.as_deref().unwrap_or("-")
            )),
            Line::raw(format!(
                "Watermark: ({}, {})",
                s.watermark_updated_at.as_deref().unwrap_or("-"),
                s.watermark_sys_id.as_deref().unwrap_or("-")
            )),
            Line::raw(format!(
                "Articles: {}    Bodies cached: {}",
                s.article_count, s.body_cached_count
            )),
            Line::raw(format!(
                "Knowledge bases: {}    Categories: {}",
                s.knowledge_base_count, s.category_count
            )),
            Line::raw(format!("Sync lock held: {}", s.lock_held)),
        ],
        None => vec![Line::raw("(loading…)")],
    };
    let block = Block::default().title("KB status").borders(Borders::ALL);
    f.render_widget(Paragraph::new(lines).block(block), area);
}

fn render_actions(f: &mut Frame, area: Rect, _app: &AdminApp) {
    let lines = vec![
        Line::raw("Actions:"),
        Line::raw("  [s] incremental sync"),
        Line::raw("  [S] full sync (DESTRUCTIVE)"),
        Line::raw("  [b] full sync with bodies (DESTRUCTIVE)"),
        Line::raw("  [R] rebuild semantic index (DESTRUCTIVE)"),
        Line::raw("  cache rebuild is offline-only"),
    ];
    let block = Block::default().title("Actions").borders(Borders::ALL);
    f.render_widget(Paragraph::new(lines).block(block), area);
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    #[test]
    fn renders_loading_when_no_status() {
        let backend = TestBackend::new(100, 24);
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
        assert!(dump.contains("KB status"), "missing KB status:\n{dump}");
        assert!(dump.contains("loading"), "missing loading:\n{dump}");
        assert!(
            dump.contains("incremental sync"),
            "missing incremental sync:\n{dump}"
        );
    }
}
