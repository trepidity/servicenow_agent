use clap::{Parser, Subcommand, ValueEnum};
use std::path::PathBuf;

use snow_core::KnowledgeSearchMode;

pub const DEFAULT_CACHE_REBUILD_PAGE_LIMIT: u32 = 500;
pub const DEFAULT_CACHE_REBUILD_TIMEOUT_SECONDS: u64 = 30;

fn parse_sys_id(value: &str) -> Result<String, String> {
    snow_core::cache::policy::normalize_sys_id("sys_id", value).map_err(|error| error.to_string())
}

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
    /// Validate or atomically reload the daemon-owned cache policy
    CachePolicy {
        #[command(subcommand)]
        action: CachePolicyCommand,
    },
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
    /// Mark existing no-vault cache rows as an intentional cache-only projection
    AdoptCacheOnlyProjection {
        /// Confirm the non-destructive provenance update
        #[arg(long)]
        yes: bool,
    },
    /// Rebuild the SQLite cache from terminal live ServiceNow reads
    RebuildCache {
        /// Maximum ServiceNow records requested per rebuild page
        #[arg(
            long,
            default_value_t = DEFAULT_CACHE_REBUILD_PAGE_LIMIT,
            value_parser = clap::value_parser!(u32).range(1..=1_000)
        )]
        page_limit: u32,
        /// Per-request ServiceNow timeout in seconds
        #[arg(
            long,
            default_value_t = DEFAULT_CACHE_REBUILD_TIMEOUT_SECONDS,
            value_parser = clap::value_parser!(u64).range(1..)
        )]
        timeout_seconds: u64,
        /// Knowledge base sys_id to rebuild instead of the cache-policy base
        #[arg(long, value_parser = parse_sys_id)]
        knowledge_base: Option<String>,
        /// Knowledge category sys_id to narrow the selected knowledge base
        #[arg(long, requires = "knowledge_base", value_parser = parse_sys_id)]
        knowledge_category: Option<String>,
    },
    /// Import the SQLite cache projection from markdown vault documents
    ImportCacheFromVault,
    /// Replace the SQLite cache with an empty current-format database
    ResetCache,
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
    /// Server lookup, live search, local query, and field commands
    Server {
        /// Server subcommand.
        #[command(subcommand)]
        action: ServerCommand,
    },
    /// Incident typed-resource commands
    Incident {
        /// Incident subcommand.
        #[command(subcommand)]
        action: IncidentCommand,
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
pub enum CachePolicyCommand {
    /// Validate the fixed cache-policy.toml without changing daemon state
    Validate {
        /// Emit the exact JSON-RPC result object
        #[arg(long)]
        json: bool,
    },
    /// Validate and atomically replace the active cache-policy snapshot
    Reload {
        /// Emit the exact JSON-RPC result object
        #[arg(long)]
        json: bool,
    },
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
    /// Print the bounded daemon JSON-RPC contract report
    ContractInfo,
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
#[allow(clippy::large_enum_variant)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
pub enum BusinessAppExportFormat {
    Json,
    Jsonl,
    Csv,
}

/// Fallback strategy for `business-app servers` when the CMDB traversal finds 0
/// servers. Mirrors `snow_core::FallbackStrategy`; default is `None`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Default)]
pub enum BusinessAppFallbackStrategy {
    /// No fallback (exact current behavior; no new response fields).
    #[default]
    None,
    /// Query servers by the BA's CI owner group when traversal returns empty.
    CiOwnerGroup,
}

impl BusinessAppFallbackStrategy {
    /// Wire value sent to the daemon `fallback_strategy` param.
    pub fn as_wire(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::CiOwnerGroup => "ci_owner_group",
        }
    }
}

/// One Business Application query filter parsed from a single `--filter` token.
///
/// The CLI surface intentionally couples the field, operator, and value into a
/// single repeatable argument (`--filter <field>:<op>:<value>`). Because clap
/// preserves the order of repeated occurrences of one argument, this keeps the
/// field/operator/value pairing intact end to end without re-reading the raw
/// process argv after clap has already parsed it. The wire shape sent to the
/// daemon (`field` + `operator` + `value`) is unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BusinessAppFilter {
    /// Field name to filter on.
    pub field: String,
    /// Operator token understood by the daemon query layer (`contains` or `eq`).
    pub operator: String,
    /// Value compared against the field using the operator.
    pub value: String,
}

/// Operator tokens accepted in the `--filter <field>:<op>:<value>` form.
///
/// Kept deliberately small and explicit so a typo (e.g. `:equals:`) is rejected
/// at parse time rather than silently forwarded to the daemon as an unknown
/// operator.
const BUSINESS_APP_FILTER_OPERATORS: [&str; 2] = ["contains", "eq"];

/// Parse a single `--filter` token of the form `field:op:value`.
///
/// Splitting is done on the first two colons only, so the value may itself
/// contain colons (e.g. a URL or timestamp). The field and operator must not be
/// empty and the operator must be one of [`BUSINESS_APP_FILTER_OPERATORS`].
fn parse_business_app_filter(value: &str) -> Result<BusinessAppFilter, String> {
    // Split into at most three parts so the value keeps any embedded colons.
    let mut parts = value.splitn(3, ':');
    let field = parts.next().unwrap_or("").trim();
    let operator = parts.next().map(str::trim);
    let raw_value = parts.next();

    let (operator, raw_value) = match (operator, raw_value) {
        (Some(operator), Some(raw_value)) => (operator, raw_value),
        _ => {
            return Err(format!(
                "--filter expects <field>:<op>:<value>, got '{value}'"
            ));
        }
    };

    if field.is_empty() {
        return Err(format!("--filter field must not be empty in '{value}'"));
    }
    if !BUSINESS_APP_FILTER_OPERATORS.contains(&operator) {
        return Err(format!(
            "--filter operator must be one of {}, got '{operator}' in '{value}'",
            BUSINESS_APP_FILTER_OPERATORS.join(", ")
        ));
    }
    if raw_value.is_empty() {
        return Err(format!("--filter value must not be empty in '{value}'"));
    }

    Ok(BusinessAppFilter {
        field: field.to_string(),
        operator: operator.to_string(),
        value: raw_value.to_string(),
    })
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
        /// Filter as `<field>:<op>:<value>` where op is `contains` or `eq`.
        /// Repeatable; clap preserves the order in which filters are supplied.
        #[arg(long = "filter", value_name = "FIELD:OP:VALUE", value_parser = parse_business_app_filter)]
        filter: Vec<BusinessAppFilter>,
        /// Maximum number of results to return
        #[arg(long)]
        limit: Option<usize>,
        /// Emit the raw daemon JSON payload
        #[arg(long)]
        json: bool,
    },
    /// List server CIs associated with a Business Application
    Servers {
        /// Business Application number. This is the real APM number field, not a synthetic BA:<sys_id> value.
        #[arg(
            long,
            value_name = "APM_NUMBER",
            value_parser = parse_business_app_number,
            conflicts_with_all = ["sys_id", "for_server"]
        )]
        number: Option<String>,
        /// Business Application sys_id.
        #[arg(
            long = "sys-id",
            value_name = "BUSINESS_APP_SYS_ID",
            conflicts_with_all = ["number", "for_server"]
        )]
        sys_id: Option<String>,
        /// Read cached Business Application-to-Server relationships instead of running live traversal.
        #[arg(
            long,
            conflicts_with_all = [
                "for_server",
                "max_depth",
                "max_cis",
                "max_edges",
                "max_service_membership_associations",
                "max_service_membership_pages",
                "relationship_type",
                "include_paths",
                "no_persist",
                "prune_stale"
            ]
        )]
        cached: bool,
        /// Read cached Business Applications associated with this Server sys_id.
        #[arg(
            long = "for-server",
            value_name = "SERVER_SYS_ID",
            conflicts_with_all = [
                "number",
                "sys_id",
                "cached",
                "max_depth",
                "max_cis",
                "max_edges",
                "max_service_membership_associations",
                "max_service_membership_pages",
                "relationship_type",
                "include_paths",
                "no_persist",
                "prune_stale"
            ]
        )]
        for_server: Option<String>,
        /// Maximum relationship traversal depth.
        #[arg(long = "max-depth", value_name = "N")]
        max_depth: Option<usize>,
        /// Maximum CIs to examine beyond the root Business Application (the root is not counted against this budget).
        #[arg(long = "max-cis", value_name = "N")]
        max_cis: Option<usize>,
        /// Maximum relationship edges to examine during traversal.
        #[arg(long = "max-edges", value_name = "N")]
        max_edges: Option<usize>,
        /// Maximum svc_ci_assoc service-membership associations to examine during traversal.
        #[arg(long = "max-service-membership-associations", value_name = "N")]
        max_service_membership_associations: Option<usize>,
        /// Maximum svc_ci_assoc service-membership pages to examine during traversal.
        #[arg(long = "max-service-membership-pages", value_name = "N")]
        max_service_membership_pages: Option<usize>,
        /// Relationship type to include during traversal. Repeat to include multiple types.
        #[arg(long = "relationship-type", value_name = "TYPE")]
        relationship_type: Vec<String>,
        /// Include relationship path metadata for each server when supported by the daemon.
        #[arg(long)]
        include_paths: bool,
        /// Fallback used only when the live CMDB traversal finds 0 servers. `none` (default)
        /// preserves current behavior; `ci-owner-group` queries servers by the BA's CI owner
        /// group and returns them tagged and live-only, surfacing the CMDB relationship gap.
        #[arg(
            long = "fallback-strategy",
            value_enum,
            default_value_t = BusinessAppFallbackStrategy::None,
            conflicts_with_all = ["cached", "for_server"]
        )]
        fallback_strategy: BusinessAppFallbackStrategy,
        /// Do not persist live traversal results into the local cache.
        #[arg(long = "no-persist", conflicts_with = "prune_stale")]
        no_persist: bool,
        /// Prune stale cached Business Application server relationships after a persisting live traversal.
        #[arg(long = "prune-stale", conflicts_with_all = ["cached", "for_server", "no_persist"])]
        prune_stale: bool,
        /// Include tombstoned cached relationship rows and endpoint records when supported.
        #[arg(long = "include-tombstoned")]
        include_tombstoned: bool,
        /// Emit the raw daemon JSON payload.
        #[arg(long)]
        json: bool,
    },
    /// Export Business Applications from the daemon query surface
    Export {
        /// Export the complete local Business Application projection
        #[arg(long, conflicts_with_all = ["text", "filter", "limit"])]
        all: bool,
        /// Export file format
        #[arg(long, value_enum)]
        format: BusinessAppExportFormat,
        /// Output file path
        #[arg(long, value_name = "PATH")]
        output: PathBuf,
        /// Free-text query passed to the daemon query surface
        #[arg(long)]
        text: Option<String>,
        /// Filter as `<field>:<op>:<value>` where op is `contains` or `eq`.
        /// Repeatable; clap preserves the order in which filters are supplied.
        #[arg(long = "filter", value_name = "FIELD:OP:VALUE", value_parser = parse_business_app_filter)]
        filter: Vec<BusinessAppFilter>,
        /// Maximum number of results to return
        #[arg(long)]
        limit: Option<usize>,
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
        /// Sync all active Business Applications from ServiceNow
        #[arg(long, conflicts_with_all = ["name", "operational_state_not"])]
        all: bool,
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

fn parse_business_app_number(value: &str) -> Result<String, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("Business Application number must not be empty".to_string());
    }
    if value
        .get(..3)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("BA:"))
    {
        return Err(
            "--number accepts a real Business Application number, not BA:<sys_id>".to_string(),
        );
    }
    Ok(value.to_string())
}

#[derive(Debug, Subcommand)]
#[allow(clippy::large_enum_variant)] // Clap owns this short-lived parsed command value.
pub enum IncidentCommand {
    /// Plan or apply one governed Incident update
    Update {
        /// Strict update request JSON path, or - for stdin
        #[arg(long, value_name = "PATH", conflicts_with = "plan")]
        request: Option<PathBuf>,
        /// Saved plan bundle JSON path, or - for stdin
        #[arg(long, value_name = "PATH", conflicts_with = "request")]
        plan: Option<PathBuf>,
        /// Apply the saved plan bundle after confirmation
        #[arg(long, requires = "plan")]
        apply: bool,
        /// Bypass only the interactive confirmation prompt
        #[arg(long, requires = "apply")]
        yes: bool,
        /// Emit the exact JSON-RPC result object
        #[arg(long)]
        json: bool,
    },
    /// Plan or apply a separately governed 3..=25 target Incident bulk update
    BulkUpdate {
        /// Strict bulk-plan request JSON path, or - for stdin
        #[arg(long, value_name = "PATH", conflicts_with = "plan")]
        request: Option<PathBuf>,
        /// Saved plan bundle JSON path, or - for stdin
        #[arg(long, value_name = "PATH", conflicts_with = "request")]
        plan: Option<PathBuf>,
        /// Apply the saved plan bundle after confirmation
        #[arg(long, requires = "plan")]
        apply: bool,
        /// Bypass only the interactive confirmation prompt
        #[arg(long, requires = "apply")]
        yes: bool,
        /// Emit the exact JSON-RPC result object
        #[arg(long)]
        json: bool,
    },
    /// Get one Incident live by exact number or sys_id
    Get {
        #[arg(long)]
        number: Option<String>,
        #[arg(long = "sys-id")]
        sys_id: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Query one bounded live page of ACL-visible Incidents
    Query {
        #[arg(long = "number")]
        numbers: Vec<String>,
        #[arg(long = "assignment-group")]
        assignment_group: Option<String>,
        #[arg(long = "assigned-to")]
        assigned_to: Option<String>,
        #[arg(long = "caller-id")]
        caller_id: Option<String>,
        #[arg(long = "cmdb-ci")]
        cmdb_ci: Option<String>,
        #[arg(long = "state")]
        states: Vec<String>,
        #[arg(long = "priority")]
        priorities: Vec<u8>,
        #[arg(long)]
        active: Option<bool>,
        #[arg(long = "opened-after")]
        opened_after: Option<String>,
        #[arg(long = "opened-before")]
        opened_before: Option<String>,
        #[arg(long = "updated-after")]
        updated_after: Option<String>,
        #[arg(long = "updated-before")]
        updated_before: Option<String>,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long)]
        cursor: Option<String>,
        #[arg(long)]
        json: bool,
    },
    /// Discover readable and writable Incident fields from ServiceNow
    Fields {
        /// Emit the raw daemon JSON payload
        #[arg(long)]
        json: bool,
    },
}

#[derive(Debug, Subcommand)]
pub enum ServerCommand {
    /// Get one cached Server by sys_id, exact name, or IP address
    Get {
        /// Server sys_id
        #[arg(long = "sys-id")]
        sys_id: Option<String>,
        /// Server name (exact match)
        #[arg(long)]
        name: Option<String>,
        /// Server IP address (exact match)
        #[arg(long = "ip-address")]
        ip_address: Option<String>,
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
    /// Live-search Windows and Linux Servers
    Search {
        /// Filter by name (contains match)
        #[arg(long)]
        name: Option<String>,
        /// Filter by IP address (exact match)
        #[arg(long = "ip-address")]
        ip_address: Option<String>,
        /// Filter by CI owner group display-name substring or sys_id
        #[arg(long = "ci-owner-group")]
        ci_owner_group: Option<String>,
        /// Filter by class: linux, windows, cmdb_ci_linux_server, or cmdb_ci_win_server
        #[arg(long)]
        class: Option<String>,
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
    /// Query cached Windows and Linux Servers
    Query {
        /// Text search across cached server name, IP, class, and groups
        #[arg(long)]
        text: Option<String>,
        /// Filter by name (contains match)
        #[arg(long)]
        name: Option<String>,
        /// Filter by IP address (exact match)
        #[arg(long = "ip-address")]
        ip_address: Option<String>,
        /// Filter by CI owner group display-name substring or sys_id
        #[arg(long = "ci-owner-group")]
        ci_owner_group: Option<String>,
        /// Filter by class: linux, windows, cmdb_ci_linux_server, or cmdb_ci_win_server
        #[arg(long)]
        class: Option<String>,
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
    /// List observed Server fields
    Fields {
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
    fn parses_incident_fields_command() {
        let cli = Cli::parse_from(["snow", "incident", "fields"]);
        assert!(matches!(
            cli.command,
            Command::Incident {
                action: IncidentCommand::Fields { json: false }
            }
        ));

        let cli = Cli::parse_from(["snow", "incident", "fields", "--json"]);
        assert!(matches!(
            cli.command,
            Command::Incident {
                action: IncidentCommand::Fields { json: true }
            }
        ));
    }

    #[test]
    fn incident_fields_rejects_a_caller_supplied_table() {
        // Metadata discovery is bound to the `incident` table by the operation.
        // Accepting a table argument would turn it into the generic table
        // browser the capability contract forbids.
        assert!(
            Cli::try_parse_from(["snow", "incident", "fields", "--table", "sys_user"]).is_err()
        );
    }

    #[test]
    fn parses_runtime_maintenance_commands() {
        let cli = Cli::parse_from(["snow", "repair-vault"]);
        assert!(matches!(cli.command, Command::RepairVault));

        let cli = Cli::parse_from(["snow", "adopt-cache-only-projection", "--yes"]);
        assert!(matches!(
            cli.command,
            Command::AdoptCacheOnlyProjection { yes: true }
        ));

        let cli = Cli::parse_from(["snow", "rebuild-cache"]);
        assert!(matches!(
            cli.command,
            Command::RebuildCache {
                page_limit: DEFAULT_CACHE_REBUILD_PAGE_LIMIT,
                timeout_seconds: DEFAULT_CACHE_REBUILD_TIMEOUT_SECONDS,
                knowledge_base: None,
                knowledge_category: None,
            }
        ));

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
            "--filter",
            "business_owner:contains:Jane",
        ]);
        assert!(matches!(
            cli.command,
            Command::BusinessApp {
                action: BusinessAppCommand::Query { filter, .. },
            } if filter == vec![BusinessAppFilter {
                field: "business_owner".to_string(),
                operator: "contains".to_string(),
                value: "Jane".to_string(),
            }]
        ));

        let cli = Cli::parse_from([
            "snow",
            "business-app",
            "query",
            "--filter",
            "u_custom_field:eq:value",
        ]);
        assert!(matches!(
            cli.command,
            Command::BusinessApp {
                action: BusinessAppCommand::Query { filter, .. },
            } if filter == vec![BusinessAppFilter {
                field: "u_custom_field".to_string(),
                operator: "eq".to_string(),
                value: "value".to_string(),
            }]
        ));

        let cli = Cli::parse_from([
            "snow",
            "business-app",
            "servers",
            "--number",
            "<APM_NUMBER>",
            "--max-depth",
            "2",
            "--max-cis",
            "500",
            "--max-edges",
            "2000",
            "--max-service-membership-associations",
            "3000",
            "--max-service-membership-pages",
            "30",
            "--relationship-type",
            "<RELATIONSHIP_TYPE>",
            "--relationship-type",
            "<SECOND_RELATIONSHIP_TYPE>",
            "--include-paths",
            "--json",
        ]);
        assert!(matches!(
            cli.command,
            Command::BusinessApp {
                action: BusinessAppCommand::Servers {
                    number: Some(number),
                    sys_id: None,
                    cached: false,
                    for_server: None,
                    max_depth: Some(2),
                    max_cis: Some(500),
                    max_edges: Some(2000),
                    max_service_membership_associations: Some(3000),
                    max_service_membership_pages: Some(30),
                    relationship_type,
                    include_paths: true,
                    fallback_strategy: BusinessAppFallbackStrategy::None,
                    no_persist: false,
                    prune_stale: false,
                    include_tombstoned: false,
                    json: true,
                },
            } if number == "<APM_NUMBER>"
                && relationship_type == vec![
                    "<RELATIONSHIP_TYPE>".to_string(),
                    "<SECOND_RELATIONSHIP_TYPE>".to_string()
                ]
        ));

        // --fallback-strategy parses to the typed enum; default is None.
        let cli = Cli::parse_from([
            "snow",
            "business-app",
            "servers",
            "--number",
            "<APM_NUMBER>",
            "--fallback-strategy",
            "ci-owner-group",
        ]);
        assert!(matches!(
            cli.command,
            Command::BusinessApp {
                action: BusinessAppCommand::Servers {
                    fallback_strategy: BusinessAppFallbackStrategy::CiOwnerGroup,
                    ..
                },
            }
        ));

        let cli = Cli::parse_from([
            "snow",
            "business-app",
            "servers",
            "--number",
            "<APM_NUMBER>",
        ]);
        assert!(matches!(
            cli.command,
            Command::BusinessApp {
                action: BusinessAppCommand::Servers {
                    fallback_strategy: BusinessAppFallbackStrategy::None,
                    ..
                },
            }
        ));

        // --fallback-strategy conflicts with --cached.
        assert!(
            Cli::try_parse_from([
                "snow",
                "business-app",
                "servers",
                "--number",
                "<APM_NUMBER>",
                "--cached",
                "--fallback-strategy",
                "ci-owner-group",
            ])
            .is_err()
        );

        let cli = Cli::parse_from([
            "snow",
            "business-app",
            "servers",
            "--sys-id",
            "<BUSINESS_APP_SYS_ID>",
        ]);
        assert!(matches!(
            cli.command,
            Command::BusinessApp {
                action: BusinessAppCommand::Servers {
                    number: None,
                    sys_id: Some(sys_id),
                    cached: false,
                    for_server: None,
                    include_paths: false,
                    no_persist: false,
                    prune_stale: false,
                    include_tombstoned: false,
                    json: false,
                    ..
                },
            } if sys_id == "<BUSINESS_APP_SYS_ID>"
        ));

        let cli = Cli::parse_from([
            "snow",
            "business-app",
            "servers",
            "--number",
            "<APM_NUMBER>",
            "--cached",
            "--include-tombstoned",
            "--json",
        ]);
        assert!(matches!(
            cli.command,
            Command::BusinessApp {
                action: BusinessAppCommand::Servers {
                    number: Some(number),
                    sys_id: None,
                    cached: true,
                    for_server: None,
                    include_tombstoned: true,
                    json: true,
                    ..
                },
            } if number == "<APM_NUMBER>"
        ));

        let cli = Cli::parse_from([
            "snow",
            "business-app",
            "servers",
            "--for-server",
            "<SERVER_SYS_ID>",
            "--include-tombstoned",
        ]);
        assert!(matches!(
            cli.command,
            Command::BusinessApp {
                action: BusinessAppCommand::Servers {
                    number: None,
                    sys_id: None,
                    cached: false,
                    for_server: Some(server_sys_id),
                    include_tombstoned: true,
                    json: false,
                    ..
                },
            } if server_sys_id == "<SERVER_SYS_ID>"
        ));

        let cli = Cli::parse_from([
            "snow",
            "business-app",
            "export",
            "--format",
            "jsonl",
            "--output",
            "business-apps.jsonl",
            "--text",
            "portfolio",
            "--filter",
            "business_owner:contains:User",
            "--filter",
            "operational_state:eq:1",
            "--limit",
            "50",
        ]);
        assert!(matches!(
            cli.command,
            Command::BusinessApp {
                action: BusinessAppCommand::Export {
                    all: false,
                    format: BusinessAppExportFormat::Jsonl,
                    output,
                    text: Some(text),
                    filter,
                    limit: Some(50),
                },
            } if output.as_path() == std::path::Path::new("business-apps.jsonl")
                && text == "portfolio"
                && filter == vec![
                    BusinessAppFilter {
                        field: "business_owner".to_string(),
                        operator: "contains".to_string(),
                        value: "User".to_string(),
                    },
                    BusinessAppFilter {
                        field: "operational_state".to_string(),
                        operator: "eq".to_string(),
                        value: "1".to_string(),
                    },
                ]
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
                    all: false,
                    name: Some(name),
                    resolve_references: true,
                    reference_depth: Some(1),
                    ..
                },
            } if name == "Epic"
        ));

        let cli = Cli::parse_from([
            "snow",
            "business-app",
            "sync",
            "--all",
            "--persist",
            "--resolve-references",
            "--reference-depth",
            "1",
            "--refresh-dictionary",
            "--json",
        ]);
        assert!(matches!(
            cli.command,
            Command::BusinessApp {
                action: BusinessAppCommand::Sync {
                    all: true,
                    name: None,
                    operational_state_not: None,
                    persist: true,
                    resolve_references: true,
                    reference_depth: Some(1),
                    refresh_dictionary: true,
                    json: true,
                },
            }
        ));

        let cli = Cli::parse_from([
            "snow",
            "business-app",
            "export",
            "--all",
            "--format",
            "csv",
            "--output",
            "business-apps.csv",
        ]);
        assert!(matches!(
            cli.command,
            Command::BusinessApp {
                action: BusinessAppCommand::Export {
                    all: true,
                    format: BusinessAppExportFormat::Csv,
                    output,
                    text: None,
                    filter,
                    limit: None,
                },
            } if output.as_path() == std::path::Path::new("business-apps.csv")
                && filter.is_empty()
        ));
    }

    #[test]
    fn business_app_query_filter_preserves_interleaved_operator_order() {
        // Headline regression: interleave eq before contains. The previous
        // implementation re-read raw argv and, when that scan failed, fell back
        // to a homogeneous ordering (all --contains first, then all --eq) which
        // would have mis-paired these two filters. With a single repeatable
        // --filter argument, clap itself preserves CLI order, so the pairing is
        // exact regardless of how the binary was invoked.
        let cli = Cli::parse_from([
            "snow",
            "business-app",
            "query",
            "--filter",
            "number:eq:EXAMPLE-APP-001",
            "--filter",
            "name:contains:Example",
        ]);
        let filter = match cli.command {
            Command::BusinessApp {
                action: BusinessAppCommand::Query { filter, .. },
            } => filter,
            other => panic!("expected business-app query, got {other:?}"),
        };
        assert_eq!(
            filter,
            vec![
                BusinessAppFilter {
                    field: "number".to_string(),
                    operator: "eq".to_string(),
                    value: "EXAMPLE-APP-001".to_string(),
                },
                BusinessAppFilter {
                    field: "name".to_string(),
                    operator: "contains".to_string(),
                    value: "Example".to_string(),
                },
            ]
        );
    }

    #[test]
    fn business_app_filter_value_may_contain_colons() {
        // Only the first two colons delimit field/op/value, so values such as
        // URLs or timestamps survive intact.
        let parsed = parse_business_app_filter("browser_url:eq:https://example.com:8443/app")
            .expect("parse");
        assert_eq!(parsed.field, "browser_url");
        assert_eq!(parsed.operator, "eq");
        assert_eq!(parsed.value, "https://example.com:8443/app");
    }

    #[test]
    fn business_app_filter_rejects_malformed_tokens() {
        // Missing the value segment entirely.
        assert!(parse_business_app_filter("name:contains").is_err());
        // Empty field.
        assert!(parse_business_app_filter(":contains:Example").is_err());
        // Empty value.
        assert!(parse_business_app_filter("name:contains:").is_err());
        // Unknown operator is rejected at parse time, not forwarded to the daemon.
        assert!(parse_business_app_filter("name:equals:Example").is_err());
    }

    #[test]
    fn business_app_query_surfaces_filter_parse_errors() {
        let err = match Cli::try_parse_from([
            "snow",
            "business-app",
            "query",
            "--filter",
            "name:equals:Example",
        ]) {
            Ok(_) => panic!("invalid operator should fail to parse"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), clap::error::ErrorKind::ValueValidation);
    }

    #[test]
    fn business_app_servers_rejects_multiple_ba_selectors() {
        let err = match Cli::try_parse_from([
            "snow",
            "business-app",
            "servers",
            "--number",
            "<APM_NUMBER>",
            "--sys-id",
            "<BUSINESS_APP_SYS_ID>",
        ]) {
            Ok(_) => panic!("servers with both selectors should fail"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn business_app_servers_rejects_incompatible_cached_and_reverse_flags() {
        let err = match Cli::try_parse_from([
            "snow",
            "business-app",
            "servers",
            "--number",
            "<APM_NUMBER>",
            "--cached",
            "--max-depth",
            "2",
        ]) {
            Ok(_) => panic!("cached read with live traversal flag should fail"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);

        let err = match Cli::try_parse_from([
            "snow",
            "business-app",
            "servers",
            "--number",
            "<APM_NUMBER>",
            "--cached",
            "--max-service-membership-associations",
            "10",
        ]) {
            Ok(_) => panic!("cached read with service-membership budget should fail"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);

        let err = match Cli::try_parse_from([
            "snow",
            "business-app",
            "servers",
            "--for-server",
            "<SERVER_SYS_ID>",
            "--include-paths",
        ]) {
            Ok(_) => panic!("reverse cached read with live traversal flag should fail"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);

        let err = match Cli::try_parse_from([
            "snow",
            "business-app",
            "servers",
            "--number",
            "<APM_NUMBER>",
            "--prune-stale",
            "--no-persist",
        ]) {
            Ok(_) => panic!("prune-stale without persistence should fail"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn business_app_servers_rejects_synthetic_ba_number() {
        let err = match Cli::try_parse_from([
            "snow",
            "business-app",
            "servers",
            "--number",
            "BA:<BUSINESS_APP_SYS_ID>",
        ]) {
            Ok(_) => panic!("synthetic BA number should fail"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), clap::error::ErrorKind::ValueValidation);
    }

    #[test]
    fn business_app_sync_all_rejects_bounded_filters() {
        for args in [
            [
                "snow",
                "business-app",
                "sync",
                "--all",
                "--name",
                "Example Application",
            ],
            [
                "snow",
                "business-app",
                "sync",
                "--all",
                "--operational-state-not",
                "retired",
            ],
        ] {
            let err = match Cli::try_parse_from(args) {
                Ok(_) => panic!("sync --all conflict should fail"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
        }
    }

    #[test]
    fn business_app_export_all_rejects_search_filter_and_limit_options() {
        for args in [
            [
                "snow",
                "business-app",
                "export",
                "--all",
                "--format",
                "json",
                "--output",
                "business-apps.json",
                "--text",
                "portfolio",
            ],
            [
                "snow",
                "business-app",
                "export",
                "--all",
                "--format",
                "json",
                "--output",
                "business-apps.json",
                "--filter",
                "name:contains:Example",
            ],
            [
                "snow",
                "business-app",
                "export",
                "--all",
                "--format",
                "json",
                "--output",
                "business-apps.json",
                "--limit",
                "50",
            ],
        ] {
            let err = match Cli::try_parse_from(args) {
                Ok(_) => panic!("export --all conflict should fail"),
                Err(err) => err,
            };
            assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
        }
    }

    #[test]
    fn business_app_export_requires_format_and_output() {
        let err = match Cli::try_parse_from([
            "snow",
            "business-app",
            "export",
            "--output",
            "business-apps.json",
        ]) {
            Ok(_) => panic!("missing format should fail"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);

        let missing_output_args = ["snow", "business-app", "export", "--format", "json"];
        let err = match Cli::try_parse_from(missing_output_args) {
            Ok(_) => panic!("missing output should fail"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn business_app_export_rejects_invalid_format() {
        let err = match Cli::try_parse_from([
            "snow",
            "business-app",
            "export",
            "--format",
            "xml",
            "--output",
            "business-apps.xml",
        ]) {
            Ok(_) => panic!("invalid format should fail"),
            Err(err) => err,
        };
        assert_eq!(err.kind(), clap::error::ErrorKind::InvalidValue);
    }

    #[test]
    fn parses_server_commands() {
        let cli = Cli::parse_from(["snow", "server", "get", "--name", "app01.example.internal"]);
        assert!(matches!(
            cli.command,
            Command::Server {
                action: ServerCommand::Get {
                    sys_id: None,
                    name: Some(name),
                    ip_address: None,
                    fresh: false,
                    json: false,
                    full: false,
                },
            } if name == "app01.example.internal"
        ));

        let cli = Cli::parse_from([
            "snow",
            "server",
            "get",
            "--ip-address",
            "192.0.2.10",
            "--fresh",
            "--json",
        ]);
        assert!(matches!(
            cli.command,
            Command::Server {
                action: ServerCommand::Get {
                    sys_id: None,
                    name: None,
                    ip_address: Some(ip_address),
                    fresh: true,
                    json: true,
                    full: false,
                },
            } if ip_address == "192.0.2.10"
        ));

        let cli = Cli::parse_from([
            "snow",
            "server",
            "search",
            "--ci-owner-group",
            "Platform Operations",
            "--class",
            "linux",
            "--limit",
            "10",
        ]);
        assert!(matches!(
            cli.command,
            Command::Server {
                action: ServerCommand::Search {
                    ci_owner_group: Some(ci_owner_group),
                    class: Some(class),
                    limit: Some(10),
                    ..
                },
            } if ci_owner_group == "Platform Operations" && class == "linux"
        ));

        let cli = Cli::parse_from([
            "snow",
            "server",
            "query",
            "--text",
            "db",
            "--ci-owner-group",
            "Platform Operations",
        ]);
        assert!(matches!(
            cli.command,
            Command::Server {
                action: ServerCommand::Query {
                    text: Some(text),
                    ci_owner_group: Some(ci_owner_group),
                    ..
                },
            } if text == "db" && ci_owner_group == "Platform Operations"
        ));

        let cli = Cli::parse_from(["snow", "server", "fields"]);
        assert!(matches!(
            cli.command,
            Command::Server {
                action: ServerCommand::Fields { json: false },
            }
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
