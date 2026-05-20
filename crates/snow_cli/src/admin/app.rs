//! Admin TUI application state, render dispatch, and key handling.

use anyhow::Result;
use crossterm::event::KeyCode;
use ratatui::Frame;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Paragraph, Tabs};
use std::process::Command;

use super::confirm::{ConfirmAction, PendingConfirm};
use super::rpc_client::AdminRpc;
use super::tabs::{Tab, cache_vault, config, daemon, sync};
use crate::auth::{CredentialProvider, strip_secret_env};

/// Marker passed into [`AdminApp::tick`] each interval.
pub struct Tick;

/// Top-level admin TUI state.
pub struct AdminApp {
    /// Currently focused tab.
    pub current: Tab,
    /// Daemon RPC handle (heartbeat + job dispatch).
    pub rpc: AdminRpc,
    /// Last-known reachability of the daemon socket.
    pub daemon_reachable: bool,
    /// Whether the jobs overlay is currently open.
    pub jobs_overlay_open: bool,
    /// If `Some`, a confirmation modal is currently displayed and gates
    /// all other keypresses.
    pub pending_confirm: Option<PendingConfirm>,
    /// Whether the daemon log tail panel is expanded on the Daemon tab.
    pub log_tail_open: bool,
    /// Most recent KB status snapshot (Sync tab).
    pub kb_status: Option<KbStatusSnapshot>,
    /// Most recent cache/vault info snapshot (Cache/Vault tab).
    pub cache_info: Option<CacheInfoSnapshot>,
    /// Last verify-vault summary surfaced this session, if any.
    pub last_verify_summary: Option<String>,
    /// Most recent job list snapshot from the daemon.
    pub jobs: Vec<super::jobs::JobSummary>,
    /// Selected index in the jobs overlay.
    pub jobs_overlay_selected: usize,
}

/// Cache/Vault info surfaced on the Cache/Vault tab.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct CacheInfoSnapshot {
    pub vault_path: String,
    pub sqlite_path: String,
    pub schema_version: i64,
    pub db_size_mb: u64,
    pub total_rows: u64,
}

/// KB sync status surfaced on the Sync tab.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct KbStatusSnapshot {
    pub article_count: usize,
    pub body_cached_count: usize,
    pub knowledge_base_count: usize,
    pub category_count: usize,
    pub last_full_at: Option<String>,
    pub last_incremental_at: Option<String>,
    pub watermark_updated_at: Option<String>,
    pub watermark_sys_id: Option<String>,
    pub lock_held: bool,
}

impl AdminApp {
    /// Construct the app, including a single best-effort daemon ping
    /// to seed `daemon_reachable`.
    pub async fn new() -> Result<Self> {
        let rpc = AdminRpc::resolve()?;
        let daemon_reachable = rpc.ping().await.is_ok();
        Ok(Self {
            current: Tab::Daemon,
            rpc,
            daemon_reachable,
            jobs_overlay_open: false,
            pending_confirm: None,
            log_tail_open: false,
            kb_status: None,
            cache_info: None,
            last_verify_summary: None,
            jobs: Vec::new(),
            jobs_overlay_selected: 0,
        })
    }

    /// Render the full frame: tab bar, body, footer.
    pub fn render(&mut self, f: &mut Frame) {
        let area = f.area();
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3), // tab bar
                Constraint::Min(1),    // body
                Constraint::Length(3), // job tray + help footer
            ])
            .split(area);

        self.render_tab_bar(f, layout[0]);
        self.render_body(f, layout[1]);
        self.render_footer(f, layout[2]);

        // Jobs overlay sits above the body but below any modal confirm.
        if self.jobs_overlay_open {
            super::jobs::render_overlay(f, f.area(), &self.jobs, self.jobs_overlay_selected);
        }

        // Modal overlay last so it sits on top of the rest of the frame.
        if let Some(c) = &self.pending_confirm {
            super::confirm::render(f, f.area(), c);
        }
    }

    fn render_tab_bar(&self, f: &mut Frame, area: Rect) {
        let titles: Vec<Line> = ["1 Daemon", "2 Sync", "3 Cache/Vault", "4 Config"]
            .iter()
            .map(|t| Line::from(Span::raw(*t)))
            .collect();
        let tabs = Tabs::new(titles)
            .block(Block::default().borders(Borders::ALL).title("snow admin"))
            .select(self.current as usize)
            .style(Style::default().fg(Color::White))
            .highlight_style(
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            );
        f.render_widget(tabs, area);
    }

    fn render_body(&mut self, f: &mut Frame, area: Rect) {
        match self.current {
            Tab::Daemon => daemon::render(f, area, self),
            Tab::Sync => sync::render(f, area, self),
            Tab::CacheVault => cache_vault::render(f, area, self),
            Tab::Config => config::render(f, area, self),
        }
    }

    fn render_footer(&self, f: &mut Frame, area: Rect) {
        // Split the footer into the job tray (2 lines including the top
        // border drawn by `render_tray`) and a single-line help row.
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(2), Constraint::Length(1)])
            .split(area);
        super::jobs::render_tray(f, layout[0], &self.jobs);
        let help = "[1-4] tabs  [j] jobs  [r] refresh  [?] help  [q] quit";
        f.render_widget(Paragraph::new(help), layout[1]);
    }

    /// Handle a single keypress. The outer event loop already filters out
    /// `q` (which exits the loop), so this routes everything else.
    pub async fn handle_key(&mut self, key: KeyCode) -> Result<()> {
        // If a confirmation modal is open, intercept all keys: `y`/`Y`
        // confirms; anything else cancels. Either way the modal is
        // dismissed (we always `take()` it).
        if let Some(pending) = self.pending_confirm.take() {
            match key {
                KeyCode::Char('y') | KeyCode::Char('Y') => {
                    self.execute_confirmed(pending).await?;
                }
                _ => {}
            }
            return Ok(());
        }

        // If the jobs overlay is open, intercept navigation + cancel
        // before any tab routing. Esc / `j` close it.
        if self.jobs_overlay_open {
            match key {
                KeyCode::Esc | KeyCode::Char('j') => self.jobs_overlay_open = false,
                KeyCode::Up => {
                    self.jobs_overlay_selected = self.jobs_overlay_selected.saturating_sub(1);
                }
                KeyCode::Down if self.jobs_overlay_selected + 1 < self.jobs.len() => {
                    self.jobs_overlay_selected += 1;
                }
                KeyCode::Char('c') => {
                    if let Some(job) = self.jobs.get(self.jobs_overlay_selected) {
                        let _ = self
                            .rpc
                            .call("cancel_job", serde_json::json!({"job_id": job.id}))
                            .await;
                    }
                }
                _ => {}
            }
            return Ok(());
        }

        match key {
            KeyCode::Char('1') => self.current = Tab::Daemon,
            KeyCode::Char('2') => self.current = Tab::Sync,
            KeyCode::Char('3') => self.current = Tab::CacheVault,
            KeyCode::Char('4') => self.current = Tab::Config,
            KeyCode::Tab => self.current = self.current.next(),
            KeyCode::BackTab => self.current = self.current.prev(),
            KeyCode::Char('j') => self.jobs_overlay_open = !self.jobs_overlay_open,
            other => self.handle_tab_key(other).await?,
        }
        Ok(())
    }

    async fn handle_tab_key(&mut self, key: KeyCode) -> Result<()> {
        match self.current {
            Tab::Daemon => self.handle_daemon_key(key).await?,
            Tab::Sync => self.handle_sync_key(key).await?,
            Tab::CacheVault => self.handle_cache_vault_key(key).await?,
            Tab::Config => self.handle_config_key(key).await?,
        }
        Ok(())
    }

    async fn handle_daemon_key(&mut self, key: KeyCode) -> Result<()> {
        match key {
            KeyCode::Char('s') => {
                std::process::Command::new(std::env::current_exe()?)
                    .args(["daemon", "start"])
                    .spawn()?;
            }
            KeyCode::Char('x') => {
                self.pending_confirm = Some(PendingConfirm {
                    title: "Confirm: stop daemon".into(),
                    action_label: "Send SIGTERM to daemon".into(),
                    impact: "All in-flight RPC calls will fail. Running jobs will be cancelled."
                        .into(),
                    confirm: ConfirmAction::DaemonStop,
                });
            }
            KeyCode::Char('R') => {
                self.pending_confirm = Some(PendingConfirm {
                    title: "Confirm: restart daemon".into(),
                    action_label: "Stop then start".into(),
                    impact: "Running jobs will be cancelled. ~1s downtime.".into(),
                    confirm: ConfirmAction::DaemonRestart,
                });
            }
            KeyCode::Char('l') => self.log_tail_open = !self.log_tail_open,
            _ => {}
        }
        Ok(())
    }

    async fn handle_sync_key(&mut self, key: KeyCode) -> Result<()> {
        use crate::admin::rpc_client::JobKindWire;
        match key {
            KeyCode::Char('s') => {
                self.rpc
                    .call(
                        "start_job",
                        serde_json::json!({"kind": JobKindWire::KbSync, "params": {}}),
                    )
                    .await?;
            }
            KeyCode::Char('S') => {
                self.pending_confirm = Some(PendingConfirm {
                    title: "Confirm: full KB sync".into(),
                    action_label: "Run full metadata sweep".into(),
                    impact:
                        "Reconciles ~4000 articles, may take 1–3 minutes. Tombstones missing rows."
                            .into(),
                    confirm: ConfirmAction::StartJob {
                        kind: JobKindWire::KbSyncFull,
                        params: serde_json::json!({"with_bodies": false}),
                    },
                });
            }
            KeyCode::Char('b') => {
                self.pending_confirm = Some(PendingConfirm {
                    title: "Confirm: full KB sync with bodies".into(),
                    action_label: "Full sweep + fetch all article bodies".into(),
                    impact: "Network-heavy; can take 10+ minutes against a large KB.".into(),
                    confirm: ConfirmAction::StartJob {
                        kind: JobKindWire::KbSyncFull,
                        params: serde_json::json!({"with_bodies": true}),
                    },
                });
            }
            KeyCode::Char('R') => {
                self.pending_confirm = Some(PendingConfirm {
                    title: "Confirm: rebuild semantic index".into(),
                    action_label: "Recompute embeddings for all KB articles".into(),
                    impact: "Replaces existing index. Search remains available during rebuild."
                        .into(),
                    confirm: ConfirmAction::StartJob {
                        kind: JobKindWire::SemanticIndexRebuild,
                        params: serde_json::json!({}),
                    },
                });
            }
            KeyCode::Char('a') => {
                self.rpc
                    .call(
                        "start_job",
                        serde_json::json!({"kind": JobKindWire::RefreshAll, "params": {}}),
                    )
                    .await?;
            }
            _ => {}
        }
        Ok(())
    }
    async fn handle_cache_vault_key(&mut self, key: KeyCode) -> Result<()> {
        use crate::admin::rpc_client::JobKindWire;
        let confirm = |title: &str, label: &str, impact: &str, kind: JobKindWire| PendingConfirm {
            title: title.into(),
            action_label: label.into(),
            impact: impact.into(),
            confirm: ConfirmAction::StartJob {
                kind,
                params: serde_json::json!({}),
            },
        };
        match key {
            KeyCode::Char('v') => {
                self.rpc
                    .call(
                        "start_job",
                        serde_json::json!({"kind": JobKindWire::VerifyVault, "params": {}}),
                    )
                    .await?;
            }
            KeyCode::Char('r') => {
                self.pending_confirm = Some(confirm(
                    "Confirm: rebuild cache",
                    "Rebuild SQLite from vault markdown",
                    "Drops and re-projects all cache rows. Read paths fall back to vault during rebuild.",
                    JobKindWire::RebuildCache,
                ));
            }
            KeyCode::Char('p') => {
                self.pending_confirm = Some(confirm(
                    "Confirm: prune orphans",
                    "Delete orphan cache rows",
                    "Removes SQLite rows with no corresponding vault file. Irreversible.",
                    JobKindWire::PruneOrphans,
                ));
            }
            KeyCode::Char('f') => {
                self.pending_confirm = Some(confirm(
                    "Confirm: repair vault",
                    "Re-write missing vault files from cache",
                    "Writes vault files from SQLite cache for any cache rows missing on disk.",
                    JobKindWire::RepairVault,
                ));
            }
            _ => {}
        }
        Ok(())
    }
    async fn handle_config_key(&mut self, key: KeyCode) -> Result<()> {
        match key {
            KeyCode::Char('e') => {
                let current = crate::daemon_cmd::paths::selected_env(None);
                self.pending_confirm = Some(PendingConfirm {
                    title: "Confirm: toggle env".into(),
                    action_label: format!("Switch SNOW_ENV (currently {current})"),
                    impact:
                        "Restarts the daemon. Active jobs cancelled. Subsequent calls hit the new env."
                            .into(),
                    confirm: ConfirmAction::ToggleEnv,
                });
            }
            KeyCode::Char('o') => match CredentialProvider::from_runtime_env() {
                CredentialProvider::Env => {
                    let _ = CredentialProvider::Env.resolve();
                }
                CredentialProvider::OnePassword { .. } => {
                    let mut command = Command::new("op");
                    command.arg("whoami");
                    let _ = strip_secret_env(&mut command).status();
                }
            },
            _ => {}
        }
        Ok(())
    }

    /// Periodic tick: refresh the daemon-reachable indicator and per-tab data.
    pub async fn tick(&mut self, _tick: Tick) -> Result<()> {
        self.daemon_reachable = self.rpc.ping().await.is_ok();
        if self.daemon_reachable {
            // Refresh job list for the tray + overlay. Errors are
            // ignored so the previous snapshot remains visible.
            if let Ok(v) = self
                .rpc
                .call(
                    "list_jobs",
                    serde_json::json!({"include_finished": true, "limit": 50}),
                )
                .await
                && let Ok(list) = serde_json::from_value::<Vec<super::jobs::JobSummary>>(v)
            {
                self.jobs = list;
            }
            match self.current {
                Tab::Sync => {
                    if let Ok(v) = self.rpc.call("kb_status", serde_json::json!({})).await {
                        let payload = v.get("status").cloned().unwrap_or(v);
                        if let Ok(snap) = serde_json::from_value(payload) {
                            self.kb_status = Some(snap);
                        }
                    }
                }
                Tab::CacheVault => {
                    if let Ok(v) = self.rpc.call("cache_info", serde_json::json!({})).await
                        && let Ok(snap) = serde_json::from_value(v)
                    {
                        self.cache_info = Some(snap);
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Execute a confirmed action. Called only after the user has pressed
    /// `y`/`Y` in the modal.
    async fn execute_confirmed(&mut self, c: PendingConfirm) -> Result<()> {
        match c.confirm {
            ConfirmAction::StartJob { kind, params } => {
                self.rpc
                    .call(
                        "start_job",
                        serde_json::json!({ "kind": kind, "params": params }),
                    )
                    .await?;
            }
            ConfirmAction::DaemonStop => {
                // Spawn `snow daemon stop` so the TUI itself doesn't have to
                // manage signals or fork bookkeeping.
                std::process::Command::new(std::env::current_exe()?)
                    .args(["daemon", "stop"])
                    .spawn()?;
            }
            ConfirmAction::DaemonRestart => {
                std::process::Command::new(std::env::current_exe()?)
                    .args(["daemon", "restart"])
                    .spawn()?;
            }
            ConfirmAction::ToggleEnv => {
                let next = match crate::daemon_cmd::paths::selected_env(None).as_str() {
                    "prd" => "test",
                    _ => "prd",
                };
                let paths = crate::daemon_cmd::paths::DaemonPaths::resolve()?;
                std::fs::write(paths.config_dir.join("env"), next)?;
                std::process::Command::new(std::env::current_exe()?)
                    .args(["daemon", "restart"])
                    .spawn()?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
impl AdminApp {
    pub fn test_default() -> Self {
        Self {
            current: Tab::Daemon,
            rpc: super::rpc_client::AdminRpc::resolve().expect("paths"),
            daemon_reachable: false,
            jobs_overlay_open: false,
            pending_confirm: None,
            log_tail_open: false,
            kb_status: None,
            cache_info: None,
            last_verify_summary: None,
            jobs: Vec::new(),
            jobs_overlay_selected: 0,
        }
    }
}
