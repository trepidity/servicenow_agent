use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

use snow_core::KnowledgeSearchMode;

#[derive(Parser)]
#[command(name = "snow", about = "ServiceNow CLI for change management")]
pub struct Cli {
    /// Environment: test (default) or prd. Falls back to SNOW_ENV env var.
    #[arg(long)]
    pub env: Option<String>,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Launch the interactive terminal UI
    Tui {
        /// Enable auto-refresh. If no value is provided, defaults to 60 seconds.
        #[arg(long, num_args = 0..=1, default_missing_value = "60", value_name = "SECONDS")]
        refresh: Option<u64>,
        /// Include closed/resolved/cancelled records in the TUI.
        #[arg(long)]
        show_closed: bool,
        /// Use the JSON-RPC daemon instead of embedding snow_core locally
        #[arg(long)]
        daemon: bool,
        /// Override the daemon filesystem socket path. Implies --daemon.
        #[arg(long, value_name = "PATH")]
        socket_path: Option<PathBuf>,
    },
    /// Show record details (CHG, PRJ, DMND, INC, RITM, STRY, RPLN)
    Show {
        /// Record number (e.g., CHG0327604, PRJ0160979, DMND0416237, INC0012345, RITM0067890, STRY0423888, RPLN0089255)
        number: String,
        /// Extra sections or field names to display (e.g., activity, notes, worknotes, or any field name)
        extras: Vec<String>,
        /// Filter associated project resource plans by state label or value.
        #[arg(long = "resource-plan-state", value_name = "STATE")]
        resource_plan_state: Option<String>,
        /// Show smart view (your approvals + tasks)
        #[arg(long)]
        smart: bool,
        /// Show full dump in pager
        #[arg(long)]
        full: bool,
    },
    /// Show Task SLA status for a record
    Sla {
        /// Record number
        number: String,
    },
    /// List change tasks
    Tasks {
        /// Change number
        number: String,
    },
    /// Approve a change request
    Approve {
        /// Change number
        number: String,
        /// Skip confirmation prompt
        #[arg(short, long)]
        yes: bool,
    },
    /// Reject a change request
    Reject {
        /// Change number
        number: String,
        /// Reason for rejection
        #[arg(long)]
        reason: Option<String>,
        /// Skip confirmation prompt
        #[arg(short, long)]
        yes: bool,
    },
    /// Add a work note to a record (CHG, INC, RITM, etc.)
    Note {
        /// Record number (e.g., CHG0327604, INC0012345)
        number: String,
        /// Note message
        message: String,
        /// Show what would be sent without making changes
        #[arg(long)]
        dry_run: bool,
    },
    /// Repair missing vault files from cached runtime data
    RepairVault,
    /// Rebuild SQLite cache projection from the markdown vault
    RebuildCache,
    /// Verify vault and cache parity
    VerifyVault,
    /// Prune orphaned cache rows discovered during verification
    PruneOrphans {
        /// Show what would be pruned without making changes
        #[arg(long)]
        dry_run: bool,
    },
    /// Show local cache and vault paths plus the SQLite schema version
    CacheInfo,
    /// Knowledge article detail and browse commands
    Knowledge {
        /// Knowledge browse/action subcommands.
        #[command(subcommand)]
        action: Option<KnowledgeCommand>,
        /// Knowledge article number. Omit when using a knowledge subcommand.
        number: Option<String>,
        /// Fetch a fresh knowledge article before displaying it.
        #[arg(long)]
        fresh: bool,
    },
    /// Show an approval through the typed runtime path
    Approval {
        /// Approval number
        number: String,
    },
    /// Manage the snow daemon process
    Daemon {
        #[command(subcommand)]
        action: DaemonCommand,
    },
    /// Launch the operator admin TUI
    Admin,
}

#[derive(Debug, Subcommand)]
pub enum DaemonCommand {
    /// Start the daemon as a background process
    Start,
    /// Stop the running daemon
    Stop,
    /// Restart the daemon
    Restart,
    /// Show daemon status
    Status,
    /// Tail the daemon log
    Logs {
        /// Follow the log (default true)
        #[arg(long, default_value_t = true)]
        follow: bool,
        /// Print only the last N lines and exit
        #[arg(long)]
        lines: Option<usize>,
    },
    /// Internal daemon entrypoint used by `snow daemon start`.
    #[command(name = "__serve", hide = true)]
    Serve {
        /// Environment label passed by the launcher.
        #[arg(long)]
        env: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
pub enum KnowledgeCommand {
    /// Search knowledge articles
    Search {
        /// Full-text search query
        query: String,
        /// Search mode for KB discovery
        #[arg(long, value_enum, default_value_t = KnowledgeSearchModeArg::Lexical)]
        mode: KnowledgeSearchModeArg,
        /// Filter by knowledge base sys_id
        #[arg(long = "knowledge-base")]
        knowledge_base: Option<String>,
        /// Filter by category sys_id
        #[arg(long)]
        category: Option<String>,
        /// Maximum number of results to return
        #[arg(long)]
        limit: Option<usize>,
        /// Minimum semantic score in millis (0-1000)
        #[arg(long = "min-score-millis")]
        min_score_millis: Option<u32>,
    },
    /// List knowledge bases
    Bases,
    /// List categories for a knowledge base
    Categories {
        /// Knowledge base sys_id
        #[arg(long = "knowledge-base")]
        knowledge_base: String,
    },
    /// Trigger a KB sync surface request
    Sync {
        /// Force a full metadata sweep
        #[arg(long)]
        full: bool,
        /// Request article bodies during the sweep
        #[arg(long = "with-bodies")]
        with_bodies: bool,
    },
    /// Aggregate KB tags from the local cache
    Tags {
        /// Restrict results to one tag layer
        #[arg(long, value_enum, default_value_t = KnowledgeTagLayer::All)]
        layer: KnowledgeTagLayer,
        /// Minimum number of articles that must contain the tag
        #[arg(long = "min-count", default_value_t = 1)]
        min_count: usize,
    },
    /// Show KB sync and cache status
    Status,
    /// Semantic KB index commands
    Semantic {
        /// Semantic KB actions
        #[command(subcommand)]
        action: KnowledgeSemanticCommand,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum KnowledgeTagLayer {
    All,
    Sn,
    Auto,
    User,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum KnowledgeSearchModeArg {
    Lexical,
    Semantic,
    Hybrid,
}

impl From<KnowledgeSearchModeArg> for KnowledgeSearchMode {
    fn from(value: KnowledgeSearchModeArg) -> Self {
        match value {
            KnowledgeSearchModeArg::Lexical => Self::Lexical,
            KnowledgeSearchModeArg::Semantic => Self::Semantic,
            KnowledgeSearchModeArg::Hybrid => Self::Hybrid,
        }
    }
}

#[derive(Debug, Subcommand)]
pub enum KnowledgeSemanticCommand {
    /// Show semantic KB index status
    Status,
    /// Rebuild the semantic KB index
    Rebuild {
        /// Rebuild every active KB article embedding
        #[arg(long)]
        full: bool,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn parses_runtime_maintenance_commands() {
        let cli = Cli::parse_from(["snow", "repair-vault"]);
        assert!(matches!(cli.command, Command::RepairVault));

        let cli = Cli::parse_from(["snow", "rebuild-cache"]);
        assert!(matches!(cli.command, Command::RebuildCache));

        let cli = Cli::parse_from(["snow", "verify-vault"]);
        assert!(matches!(cli.command, Command::VerifyVault));

        let cli = Cli::parse_from(["snow", "prune-orphans", "--dry-run"]);
        assert!(matches!(
            cli.command,
            Command::PruneOrphans { dry_run: true }
        ));

        let cli = Cli::parse_from(["snow", "cache-info"]);
        assert!(matches!(cli.command, Command::CacheInfo));
    }

    #[test]
    fn parses_typed_runtime_lookup_commands() {
        let cli = Cli::parse_from(["snow", "knowledge", "KB001"]);
        assert!(matches!(
            cli.command,
            Command::Knowledge {
                action: None,
                number: Some(number),
                fresh: false,
            } if number == "KB001"
        ));

        let cli = Cli::parse_from(["snow", "knowledge", "KB001", "--fresh"]);
        assert!(matches!(
            cli.command,
            Command::Knowledge {
                action: None,
                number: Some(number),
                fresh: true,
            } if number == "KB001"
        ));

        let cli = Cli::parse_from(["snow", "knowledge", "search", "windows admin"]);
        assert!(matches!(
            cli.command,
            Command::Knowledge {
                action: Some(KnowledgeCommand::Search { query, mode, .. }),
                number: None,
                fresh: false,
            } if query == "windows admin" && mode == KnowledgeSearchModeArg::Lexical
        ));

        let cli = Cli::parse_from([
            "snow",
            "knowledge",
            "search",
            "windows admin",
            "--mode",
            "hybrid",
            "--min-score-millis",
            "250",
        ]);
        assert!(matches!(
            cli.command,
            Command::Knowledge {
                action: Some(KnowledgeCommand::Search {
                    query,
                    mode: KnowledgeSearchModeArg::Hybrid,
                    min_score_millis: Some(250),
                    ..
                }),
                number: None,
                fresh: false,
            } if query == "windows admin"
        ));

        let cli = Cli::parse_from(["snow", "knowledge", "bases"]);
        assert!(matches!(
            cli.command,
            Command::Knowledge {
                action: Some(KnowledgeCommand::Bases),
                number: None,
                fresh: false,
            }
        ));

        let cli = Cli::parse_from([
            "snow",
            "knowledge",
            "categories",
            "--knowledge-base",
            "kb-123",
        ]);
        assert!(matches!(
            cli.command,
            Command::Knowledge {
                action: Some(KnowledgeCommand::Categories { knowledge_base }),
                number: None,
                fresh: false,
            } if knowledge_base == "kb-123"
        ));

        let cli = Cli::parse_from(["snow", "knowledge", "sync", "--full", "--with-bodies"]);
        assert!(matches!(
            cli.command,
            Command::Knowledge {
                action: Some(KnowledgeCommand::Sync {
                    full: true,
                    with_bodies: true
                }),
                number: None,
                fresh: false,
            }
        ));

        let cli = Cli::parse_from([
            "snow",
            "knowledge",
            "tags",
            "--layer",
            "user",
            "--min-count",
            "3",
        ]);
        assert!(matches!(
            cli.command,
            Command::Knowledge {
                action: Some(KnowledgeCommand::Tags {
                    layer: KnowledgeTagLayer::User,
                    min_count: 3
                }),
                number: None,
                fresh: false,
            }
        ));

        let cli = Cli::parse_from(["snow", "knowledge", "status"]);
        assert!(matches!(
            cli.command,
            Command::Knowledge {
                action: Some(KnowledgeCommand::Status),
                number: None,
                fresh: false,
            }
        ));

        let cli = Cli::parse_from(["snow", "knowledge", "semantic", "status"]);
        assert!(matches!(
            cli.command,
            Command::Knowledge {
                action: Some(KnowledgeCommand::Semantic {
                    action: KnowledgeSemanticCommand::Status
                }),
                number: None,
                fresh: false,
            }
        ));

        let cli = Cli::parse_from(["snow", "knowledge", "semantic", "rebuild", "--full"]);
        assert!(matches!(
            cli.command,
            Command::Knowledge {
                action: Some(KnowledgeCommand::Semantic {
                    action: KnowledgeSemanticCommand::Rebuild { full: true }
                }),
                number: None,
                fresh: false,
            }
        ));

        let cli = Cli::parse_from(["snow", "approval", "APR001"]);
        assert!(matches!(cli.command, Command::Approval { number } if number == "APR001"));
    }

    #[test]
    fn parses_task_sla_commands() {
        let cli = Cli::parse_from(["snow", "sla", "TASK000001"]);
        assert!(matches!(cli.command, Command::Sla { number } if number == "TASK000001"));

        let cli = Cli::parse_from(["snow", "show", "TASK000001", "sla"]);
        assert!(matches!(
            cli.command,
            Command::Show {
                number,
                extras,
                resource_plan_state: None,
                smart: false,
                full: false,
            } if number == "TASK000001" && extras == ["sla"]
        ));

        let cli = Cli::parse_from([
            "snow",
            "show",
            "PRJ0161206",
            "--resource-plan-state",
            "Allocated",
        ]);
        assert!(matches!(
            cli.command,
            Command::Show {
                number,
                resource_plan_state: Some(state),
                ..
            } if number == "PRJ0161206" && state == "Allocated"
        ));
    }

    #[test]
    fn parses_daemon_start() {
        let cli = Cli::parse_from(["snow", "daemon", "start"]);
        assert!(matches!(
            cli.command,
            Command::Daemon {
                action: DaemonCommand::Start
            }
        ));
    }

    #[test]
    fn parses_hidden_daemon_serve() {
        let cli = Cli::parse_from(["snow", "daemon", "__serve", "--env", "prd"]);
        assert!(matches!(
            cli.command,
            Command::Daemon {
                action: DaemonCommand::Serve { env: Some(env) }
            } if env == "prd"
        ));
    }

    #[test]
    fn parses_tui_daemon_flags() {
        let cli = Cli::parse_from(["snow", "tui", "--daemon"]);
        assert!(matches!(
            cli.command,
            Command::Tui {
                daemon: true,
                socket_path: None,
                ..
            }
        ));

        let cli = Cli::parse_from(["snow", "tui", "--socket-path", "/tmp/snow.sock"]);
        assert!(matches!(
            cli.command,
            Command::Tui {
                daemon: false,
                socket_path: Some(_),
                ..
            }
        ));

        let cli = Cli::parse_from(["snow", "tui", "--show-closed"]);
        assert!(matches!(
            cli.command,
            Command::Tui {
                show_closed: true,
                ..
            }
        ));
    }
}
