use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

use snow_core::KnowledgeSearchMode;

#[derive(Parser)]
#[command(name = "snow", about = "ServiceNow CLI for change management")]
pub struct Cli {
    /// Environment override, such as test or prd. Falls back to SNOW_ENV env var.
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
    /// Show record details (CHG, PRJ, DMND, INC, RITM, STRY, STSK, RPLN)
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
    /// List and update your existing weekly time cards
    Timecard {
        #[command(subcommand)]
        action: TimecardCommand,
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
    /// List or upload record attachments
    Attachment {
        #[command(subcommand)]
        action: AttachmentCommand,
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
    /// Business Application lookup, search, query, dictionary, and sync commands
    BusinessApp {
        /// Business Application subcommand.
        #[command(subcommand)]
        action: BusinessAppCommand,
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
    Start {
        /// Keep the daemon running indefinitely; disable idle self-shutdown.
        #[arg(long)]
        no_idle_timeout: bool,
    },
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
        /// Disable idle self-shutdown (propagated from `start --no-idle-timeout`).
        #[arg(long)]
        no_idle_timeout: bool,
    },
}

#[derive(Debug, Subcommand)]
pub enum TimecardCommand {
    /// List your existing time cards for a week
    List {
        /// Any date within the target week, formatted as YYYY-MM-DD
        #[arg(long, value_name = "DATE")]
        week: Option<String>,
    },
    /// Set or add hours on one existing time card
    Set {
        /// Card selector: sys_id, exact task display/number, or cached list index
        card: String,
        /// Day to update when using positional single-day form
        day: Option<String>,
        /// Hours to set or add when using positional single-day form
        hours: Option<String>,
        /// Add the supplied hours to the current value instead of replacing it
        #[arg(long)]
        add: bool,
        /// Any date within the target week, formatted as YYYY-MM-DD
        #[arg(long, value_name = "DATE")]
        week: Option<String>,
        /// Show the resolved card and current-to-new preview without writing
        #[arg(long)]
        dry_run: bool,
        /// Skip confirmation prompt
        #[arg(long, short)]
        yes: bool,
        /// Disambiguate cards that share the same task display/number
        #[arg(long)]
        category: Option<String>,
        /// Set/add Sunday hours
        #[arg(long)]
        sun: Option<String>,
        /// Set/add Monday hours
        #[arg(long)]
        mon: Option<String>,
        /// Set/add Tuesday hours
        #[arg(long)]
        tue: Option<String>,
        /// Set/add Wednesday hours
        #[arg(long)]
        wed: Option<String>,
        /// Set/add Thursday hours
        #[arg(long)]
        thu: Option<String>,
        /// Set/add Friday hours
        #[arg(long)]
        fri: Option<String>,
        /// Set/add Saturday hours
        #[arg(long)]
        sat: Option<String>,
    },
    /// Open the timecard editing experience
    Edit {
        /// Any date within the target week, formatted as YYYY-MM-DD
        #[arg(long, value_name = "DATE")]
        week: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
pub enum AttachmentCommand {
    /// List attachments on a record
    List {
        /// Record number (e.g., CHG0327604, INC0012345)
        number: String,
    },
    /// Upload a local file as an attachment on a record
    Upload {
        /// Record number (e.g., CHG0327604, INC0012345)
        number: String,
        /// Local file path to upload
        path: PathBuf,
        /// Attachment file name. Defaults to the local file name.
        #[arg(long = "file-name")]
        file_name: Option<String>,
        /// Attachment MIME type. Defaults from common file extensions.
        #[arg(long = "content-type")]
        content_type: Option<String>,
        /// Show the resolved target and file metadata without uploading.
        #[arg(long)]
        dry_run: bool,
        /// Skip confirmation prompt
        #[arg(short, long)]
        yes: bool,
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

#[derive(Debug, Subcommand)]
pub enum BusinessAppCommand {
    /// Get a single Business Application by sys_id or exact name
    Get {
        /// Business Application sys_id
        #[arg(long = "sys-id")]
        sys_id: Option<String>,
        /// Business Application name (exact match)
        #[arg(long)]
        name: Option<String>,
        /// Fetch a fresh copy from ServiceNow instead of the local cache
        #[arg(long)]
        fresh: bool,
        /// Emit the raw daemon JSON payload
        #[arg(long)]
        json: bool,
        /// Include the full all-fields table in human output
        #[arg(long)]
        full: bool,
    },
    /// Search Business Applications by name and operational state
    Search {
        /// Filter by name (contains match)
        #[arg(long)]
        name: Option<String>,
        /// Exclude rows whose operational_state equals this value
        #[arg(long = "operational-state-not")]
        operational_state_not: Option<String>,
        /// Maximum number of results to return
        #[arg(long)]
        limit: Option<usize>,
        /// Emit the raw daemon JSON payload
        #[arg(long)]
        json: bool,
        /// Include the full all-fields table for each result
        #[arg(long)]
        full: bool,
    },
    /// Run a local all-field query using repeatable field/operator/value filters
    Query {
        /// Field name to filter on. Repeatable; paired by position with operator values.
        #[arg(long = "field")]
        field: Vec<String>,
        /// "contains" operator value. Repeatable.
        #[arg(long = "contains")]
        contains: Vec<String>,
        /// "equals" operator value. Repeatable.
        #[arg(long = "eq")]
        eq: Vec<String>,
        /// Maximum number of results to return
        #[arg(long)]
        limit: Option<usize>,
        /// Emit the raw daemon JSON payload
        #[arg(long)]
        json: bool,
    },
    /// List the dictionary-backed Business Application fields
    Fields {
        /// Refresh the dictionary from ServiceNow before listing
        #[arg(long)]
        refresh: bool,
        /// Emit the raw daemon JSON payload
        #[arg(long)]
        json: bool,
    },
    /// Sync Business Applications into the local vault/cache with optional hydration
    Sync {
        /// Filter by name (contains match)
        #[arg(long)]
        name: Option<String>,
        /// Exclude rows whose operational_state equals this value
        #[arg(long = "operational-state-not")]
        operational_state_not: Option<String>,
        /// Persist results to the local vault/cache
        #[arg(long, default_value_t = true)]
        persist: bool,
        /// Resolve direct references during hydration
        #[arg(long = "resolve-references")]
        resolve_references: bool,
        /// Maximum reference resolution depth
        #[arg(long = "reference-depth")]
        reference_depth: Option<u32>,
        /// Refresh the dictionary before syncing
        #[arg(long = "refresh-dictionary")]
        refresh_dictionary: bool,
        /// Emit the raw daemon JSON payload
        #[arg(long)]
        json: bool,
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
    fn parses_business_app_commands() {
        let cli = Cli::parse_from(["snow", "business-app", "get", "--name", "Epic", "--full"]);
        assert!(matches!(
            cli.command,
            Command::BusinessApp {
                action: BusinessAppCommand::Get {
                    sys_id: None,
                    name: Some(name),
                    fresh: false,
                    json: false,
                    full: true,
                },
            } if name == "Epic"
        ));

        let cli = Cli::parse_from([
            "snow",
            "business-app",
            "get",
            "--sys-id",
            "abc123",
            "--fresh",
            "--json",
        ]);
        assert!(matches!(
            cli.command,
            Command::BusinessApp {
                action: BusinessAppCommand::Get {
                    sys_id: Some(sys_id),
                    name: None,
                    fresh: true,
                    json: true,
                    full: false,
                },
            } if sys_id == "abc123"
        ));

        let cli = Cli::parse_from([
            "snow",
            "business-app",
            "search",
            "--name",
            "Epic",
            "--operational-state-not",
            "2",
        ]);
        assert!(matches!(
            cli.command,
            Command::BusinessApp {
                action: BusinessAppCommand::Search {
                    name: Some(name),
                    operational_state_not: Some(state),
                    ..
                },
            } if name == "Epic" && state == "2"
        ));

        let cli = Cli::parse_from([
            "snow",
            "business-app",
            "query",
            "--field",
            "business_owner",
            "--contains",
            "Jane",
        ]);
        assert!(matches!(
            cli.command,
            Command::BusinessApp {
                action: BusinessAppCommand::Query {
                    field,
                    contains,
                    eq,
                    ..
                },
            } if field == vec!["business_owner".to_string()]
                && contains == vec!["Jane".to_string()]
                && eq.is_empty()
        ));

        let cli = Cli::parse_from([
            "snow",
            "business-app",
            "query",
            "--field",
            "u_custom_field",
            "--eq",
            "value",
        ]);
        assert!(matches!(
            cli.command,
            Command::BusinessApp {
                action: BusinessAppCommand::Query { field, eq, .. },
            } if field == vec!["u_custom_field".to_string()]
                && eq == vec!["value".to_string()]
        ));

        let cli = Cli::parse_from(["snow", "business-app", "fields", "--refresh"]);
        assert!(matches!(
            cli.command,
            Command::BusinessApp {
                action: BusinessAppCommand::Fields {
                    refresh: true,
                    json: false,
                },
            }
        ));

        let cli = Cli::parse_from([
            "snow",
            "business-app",
            "sync",
            "--name",
            "Epic",
            "--resolve-references",
            "--reference-depth",
            "1",
        ]);
        assert!(matches!(
            cli.command,
            Command::BusinessApp {
                action: BusinessAppCommand::Sync {
                    name: Some(name),
                    resolve_references: true,
                    reference_depth: Some(1),
                    ..
                },
            } if name == "Epic"
        ));
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
    fn parses_timecard_commands() {
        let cli = Cli::parse_from(["snow", "timecard", "list", "--week", "2026-05-17"]);
        assert!(matches!(
            cli.command,
            Command::Timecard {
                action: TimecardCommand::List { week: Some(week) }
            } if week == "2026-05-17"
        ));

        let cli = Cli::parse_from([
            "snow",
            "timecard",
            "set",
            "1",
            "mon",
            "8",
            "--add",
            "--yes",
            "--category",
            "project_work",
        ]);
        assert!(matches!(
            cli.command,
            Command::Timecard {
                action: TimecardCommand::Set {
                    card,
                    day: Some(day),
                    hours: Some(hours),
                    add: true,
                    yes: true,
                    category: Some(category),
                    ..
                }
            } if card == "1" && day == "mon" && hours == "8" && category == "project_work"
        ));

        let cli = Cli::parse_from([
            "snow",
            "timecard",
            "set",
            "PRJ0161219",
            "--mon",
            "8",
            "--tue",
            "4",
            "--dry-run",
        ]);
        assert!(matches!(
            cli.command,
            Command::Timecard {
                action: TimecardCommand::Set {
                    card,
                    day: None,
                    hours: None,
                    mon: Some(mon),
                    tue: Some(tue),
                    dry_run: true,
                    ..
                }
            } if card == "PRJ0161219" && mon == "8" && tue == "4"
        ));

        let cli = Cli::parse_from(["snow", "timecard", "edit"]);
        assert!(matches!(
            cli.command,
            Command::Timecard {
                action: TimecardCommand::Edit { week: None }
            }
        ));
    }

    #[test]
    fn parses_daemon_start() {
        let cli = Cli::parse_from(["snow", "daemon", "start"]);
        assert!(matches!(
            cli.command,
            Command::Daemon {
                action: DaemonCommand::Start {
                    no_idle_timeout: false
                }
            }
        ));
    }

    #[test]
    fn parses_hidden_daemon_serve() {
        let cli = Cli::parse_from(["snow", "daemon", "__serve", "--env", "prd"]);
        assert!(matches!(
            cli.command,
            Command::Daemon {
                action: DaemonCommand::Serve {
                    env: Some(env),
                    no_idle_timeout: false
                }
            } if env == "prd"
        ));
    }

    #[test]
    fn parses_daemon_start_no_idle_timeout() {
        let cli = Cli::parse_from(["snow", "daemon", "start", "--no-idle-timeout"]);
        assert!(matches!(
            cli.command,
            Command::Daemon {
                action: DaemonCommand::Start {
                    no_idle_timeout: true
                }
            }
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
