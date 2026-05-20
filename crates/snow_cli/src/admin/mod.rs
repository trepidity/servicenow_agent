//! Admin TUI — operator surface for the snow daemon.
//!
//! Entry point: [`run`]. Sets up raw mode + the alternate screen, drives
//! the event loop, and always restores the terminal on exit.

pub mod app;
pub mod confirm;
pub mod jobs;
pub mod rpc_client;
pub mod tabs;

use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;

use app::{AdminApp, Tick};

/// Launch the admin TUI. Returns when the user presses `q` (or on error).
pub async fn run() -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let result = run_loop(&mut terminal).await;

    // Always restore the terminal, even on error.
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    result
}

async fn run_loop(terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>) -> Result<()> {
    let mut app = AdminApp::new().await?;
    let mut last_tick = std::time::Instant::now();
    let tick_rate = std::time::Duration::from_millis(500);

    loop {
        terminal.draw(|f| app.render(f))?;

        let timeout = tick_rate.saturating_sub(last_tick.elapsed());
        if event::poll(timeout)?
            && let Event::Key(KeyEvent {
                code,
                kind: KeyEventKind::Press,
                ..
            }) = event::read()?
        {
            if matches!(code, KeyCode::Char('q')) {
                return Ok(());
            }
            app.handle_key(code).await?;
        }

        if last_tick.elapsed() >= tick_rate {
            app.tick(Tick).await?;
            last_tick = std::time::Instant::now();
        }
    }
}
