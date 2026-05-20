//! Daemon tab — status block, log tail toggle, lifecycle actions.

use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};

use crate::admin::app::AdminApp;
use crate::daemon_cmd::paths::DaemonPaths;

pub fn render(f: &mut Frame, area: Rect, app: &mut AdminApp) {
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(10), Constraint::Min(1)])
        .split(area);

    render_status(f, layout[0], app);
    render_log_tail(f, layout[1], app);
}

fn render_status(f: &mut Frame, area: Rect, app: &AdminApp) {
    let paths = DaemonPaths::resolve().ok();
    let pid = paths
        .as_ref()
        .and_then(|p| std::fs::read_to_string(&p.pidfile).ok())
        .and_then(|s| s.trim().parse::<i32>().ok());

    let lines = vec![
        Line::from(vec![
            Span::raw("Status: "),
            if app.daemon_reachable {
                Span::styled("running", Style::default().fg(Color::Green))
            } else if pid.is_some() {
                Span::styled("unreachable", Style::default().fg(Color::Yellow))
            } else {
                Span::styled("stopped", Style::default().fg(Color::Red))
            },
        ]),
        Line::from(format!(
            "PID: {}",
            pid.map(|p| p.to_string()).unwrap_or_else(|| "-".into())
        )),
        Line::from(format!(
            "Socket: {}",
            paths
                .as_ref()
                .map(|p| p.socket.display().to_string())
                .unwrap_or_default()
        )),
        Line::from(format!(
            "Log: {}",
            paths
                .as_ref()
                .map(|p| p.logfile.display().to_string())
                .unwrap_or_default()
        )),
        Line::from(""),
        Line::from("Actions:  [s] start    [x] stop    [R] restart    [l] toggle log tail"),
    ];

    let block = Block::default().title("Daemon").borders(Borders::ALL);
    f.render_widget(
        Paragraph::new(lines)
            .block(block)
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn render_log_tail(f: &mut Frame, area: Rect, app: &AdminApp) {
    let block = Block::default().title("Log tail").borders(Borders::ALL);
    if !app.log_tail_open {
        let body = Paragraph::new("(press l to toggle)").block(block);
        f.render_widget(body, area);
        return;
    }
    let paths = match DaemonPaths::resolve() {
        Ok(p) => p,
        Err(_) => return,
    };
    let content = std::fs::read_to_string(&paths.logfile).unwrap_or_default();
    let tail: Vec<&str> = content
        .lines()
        .rev()
        .take(area.height.saturating_sub(2) as usize)
        .collect();
    let lines: Vec<Line> = tail.into_iter().rev().map(Line::raw).collect();
    f.render_widget(Paragraph::new(lines).block(block), area);
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    #[test]
    fn renders_stopped_when_no_pidfile() {
        let backend = TestBackend::new(100, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        let mut app = crate::admin::app::AdminApp::test_default();
        app.daemon_reachable = false;
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
        assert!(dump.contains("Status:"), "missing Status:\n{dump}");
        assert!(
            dump.contains("stopped") || dump.contains("unreachable"),
            "missing state:\n{dump}"
        );
    }
}
