//! Job tray (footer) and jobs overlay rendering for the admin TUI.
//!
//! The tray surfaces the most recent active job in a single line at the
//! bottom of the screen; the overlay (toggled with `j`) shows the full
//! job list with status colouring and supports selection + cancel.

use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, Paragraph};
use serde::Deserialize;

/// One job entry as surfaced by the daemon's `list_jobs` RPC.
///
/// Mirrors the daemon-side `Job` struct with snake_case status values:
/// `pending`, `running`, `succeeded`, `failed`, `cancelled`.
#[derive(Debug, Clone, Deserialize)]
pub struct JobSummary {
    /// UUID (hyphenated) — pass directly to `cancel_job` as `{"job_id": id}`.
    pub id: String,
    /// Job kind, e.g. `kb_sync`, `kb_sync_full`, `verify_vault`.
    pub kind: String,
    /// One of `pending`, `running`, `succeeded`, `failed`, `cancelled`.
    pub status: String,
    /// ISO-8601 timestamp when the job started.
    pub started_at: String,
    /// ISO-8601 timestamp when the job finished, if any. Absence means
    /// the job is still active.
    pub finished_at: Option<String>,
    /// Optional progress snapshot for long-running jobs.
    pub progress: Option<JobProgressSummary>,
}

/// Optional progress snapshot for a job.
#[derive(Debug, Clone, Deserialize)]
pub struct JobProgressSummary {
    /// Items processed so far.
    pub current: u64,
    /// Total items to process, if known.
    pub total: Option<u64>,
    /// Free-form stage label, e.g. `articles`, `embedding`.
    pub stage: String,
}

/// Render the one-line job tray.
///
/// If there are no active jobs (i.e. all jobs have a `finished_at`), the
/// tray reads "no active jobs". Otherwise the first active job's kind,
/// status, and progress are shown, followed by `(+N more)` if multiple
/// active jobs are present.
pub fn render_tray(f: &mut Frame, area: Rect, jobs: &[JobSummary]) {
    let active: Vec<&JobSummary> = jobs.iter().filter(|j| j.finished_at.is_none()).collect();
    let line = if active.is_empty() {
        "no active jobs".to_string()
    } else {
        let j = active[0];
        match &j.progress {
            Some(p) if p.total.is_some() => format!(
                "{} · {} · {}/{} · {} {}",
                j.kind,
                j.status,
                p.current,
                p.total.unwrap(),
                p.stage,
                if active.len() > 1 {
                    format!("(+{} more)", active.len() - 1)
                } else {
                    String::new()
                }
            ),
            Some(p) => format!("{} · {} · {} · {}", j.kind, j.status, p.current, p.stage),
            None => format!("{} · {}", j.kind, j.status),
        }
    };
    let block = Block::default().borders(Borders::TOP).title("jobs (j)");
    f.render_widget(Paragraph::new(line).block(block), area);
}

/// Render the centered jobs overlay popup.
///
/// `selected` is the highlighted index in `jobs`. Status is colour-coded:
/// running=yellow, succeeded=green, failed=red, cancelled=magenta.
pub fn render_overlay(f: &mut Frame, area: Rect, jobs: &[JobSummary], selected: usize) {
    let popup = Rect {
        x: area.x + area.width / 8,
        y: area.y + area.height / 8,
        width: area.width * 3 / 4,
        height: area.height * 3 / 4,
    };
    f.render_widget(Clear, popup);
    let items: Vec<ListItem> = jobs
        .iter()
        .map(|j| {
            let status_color = match j.status.as_str() {
                "running" => Color::Yellow,
                "succeeded" => Color::Green,
                "failed" => Color::Red,
                "cancelled" => Color::Magenta,
                _ => Color::White,
            };
            ListItem::new(Line::from(vec![
                Span::styled(
                    format!("{:14}", j.kind),
                    Style::default().add_modifier(Modifier::BOLD),
                ),
                Span::raw("  "),
                Span::styled(j.status.clone(), Style::default().fg(status_color)),
                Span::raw("  "),
                Span::raw(j.started_at.clone()),
            ]))
        })
        .collect();
    let block = Block::default()
        .title("Jobs (↑/↓ select  c cancel  esc close)")
        .borders(Borders::ALL);
    let mut state = ratatui::widgets::ListState::default();
    state.select(Some(selected));
    f.render_stateful_widget(
        List::new(items)
            .block(block)
            .highlight_style(Style::default().bg(Color::DarkGray)),
        popup,
        &mut state,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    #[test]
    fn tray_shows_no_active_when_empty() {
        let backend = TestBackend::new(80, 3);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| render_tray(f, f.area(), &[])).unwrap();
        let buf = terminal.backend().buffer();
        let dump: String = (0..buf.area.height)
            .map(|y| {
                (0..buf.area.width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(dump.contains("no active jobs"));
    }

    #[test]
    fn tray_shows_progress_when_active() {
        let backend = TestBackend::new(80, 3);
        let mut terminal = Terminal::new(backend).unwrap();
        let jobs = vec![JobSummary {
            id: "abc".into(),
            kind: "kb_sync_full".into(),
            status: "running".into(),
            started_at: "now".into(),
            finished_at: None,
            progress: Some(JobProgressSummary {
                current: 1842,
                total: Some(4215),
                stage: "articles".into(),
            }),
        }];
        terminal.draw(|f| render_tray(f, f.area(), &jobs)).unwrap();
        let buf = terminal.backend().buffer();
        let dump: String = (0..buf.area.height)
            .map(|y| {
                (0..buf.area.width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(dump.contains("kb_sync_full"));
        assert!(dump.contains("1842/4215"));
    }
}
