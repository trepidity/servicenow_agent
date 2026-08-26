# snow

A Rust CLI for ServiceNow change management. View change requests, list tasks, approve/reject changes, and add work notes — all from your terminal.

## Prerequisites

- [Rust](https://rustup.rs/) (for building)
- Optional: [1Password CLI (`op`)](https://developer.1password.com/docs/cli/) if you want `snow` to resolve the ServiceNow password from 1Password

## Setup

1. Clone and build:
   ```bash
   cargo build --release
   ```

2. Create `.env.test` from the public example:
   ```
   cp .env.test.example .env.test
   ```

3. Configure the instance, user, and credential provider:
   ```
   SERVICENOW_INSTANCE=https://your-instance.service-now.com
   SERVICENOW_USERNAME=your_username

   # Default: inject a resolved password from your shell or vault.
   SERVICENOW_PASSWORD=replace_with_runtime_secret

   # Optional 1Password compatibility path. Leave SERVICENOW_PASSWORD unset when using it.
   # Plain item IDs use `op item get`; op://vault/item references use `op read`.
   # Use OP_VAULT with plain item IDs when `op` is authenticated by service account.
   # Set OP_SERVICE_ACCOUNT_TOKEN in the parent environment, or point
   # OP_SERVICE_ACCOUNT_TOKEN_FILE at an ignored local file containing it.
   # If SERVICENOW_USERNAME is unset, the username field is read from the same item.
   # OP_ITEM_ID=op://vault/item
   # OP_VAULT=vault
   # OP_SERVICE_ACCOUNT_TOKEN_FILE=/path/to/ignored/op-service-account-token
   ```

4. Copy the env file next to the binary when running the release binary directly:
   ```bash
   cp .env.test target/release/.env.test
   ```

## Build cache maintenance

Cargo retains hashed build artifacts indefinitely across compiler, feature,
profile, and branch changes. Check this checkout's debug cache before a long
build or when Cargo appears to pause between test binaries:

```bash
scripts/build_cache_guard.sh --check
```

If the guard reports a stale cache, remove only the generated debug artifacts:

```bash
scripts/build_cache_guard.sh --clean
```

The next debug build will be cold. Release artifacts are not removed. Override
the default 20,000-entry limit with `SNOW_BUILD_CACHE_MAX_ENTRIES` when needed.

## Usage

By default, regular `snow` commands use the `test` environment. Daemon startup defaults to `prd`. Use `--env` or `SNOW_ENV` to override either path explicitly.

### Show a change request

```bash
snow show CHG0327604              # summary
snow show CHG0327604 --smart      # summary + your tasks & approvals
snow show CHG0327604 --full       # full JSON dump in pager
snow show INC0000001 sla          # Task SLA status alias
```

Summary includes: state, category, dates, assigned to, description, change plan, implementation steps, and backout plan.

### Task SLA status

```bash
snow sla <NUMBER>
```

Task SLA output uses the read-only ServiceNow Task SLA API. It reports aggregate counts, the next active unbreached breach target, and bounded row detail. Empty results are reported as "none readable or no rows" because ACLs and an actually empty SLA set cannot be distinguished from the client side.

Readability states:

- `ReadableRows`: rows were returned and are summarized.
- `EmptyOrAclRestricted`: no rows were readable, or no rows are attached.
- `ParentNotFound`: the supplied record number could not be resolved.
- `NotApplicable`: the record type does not support Task SLA.

### Record browser TUI

```bash
snow tui
snow tui --daemon
```

The record detail pane includes a collapsed `Task SLA` heading by default. Press `S` in the browser view to expand or collapse bounded SLA row detail. SLA loading failures render inside that section and do not block the rest of the detail view.

### Business Applications

Business Applications (`cmdb_ci_business_app`) are a first-class local primitive. The canonical local resource type is `business_application`; `cmdb_ci_business_app` is accepted as an input alias for list/query filters.

The `snow business-app` subcommand family is a thin CLI over the daemon JSON-RPC methods (`business_application_get`, `business_application_search`, `business_application_query`, `business_application_fields`, `business_application_sync`); it drives a running daemon and auto-spawns it as needed. There is no Business Application write/create/update surface — every subcommand is a read (`sync`/`search`/`servers` perform a live ServiceNow read plus a local vault/SQLite write).

> **Breaking changes:** the `query`/`export` filter grammar changed (the old `--field`/`--contains`/`--eq` flags are gone — use `--filter <field>:<op>:<value>` instead), and `server get` is now a read-through cache rather than cache-only. See [CHANGELOG.md](CHANGELOG.md) for migration detail.

```bash
snow business-app get --sys-id <BUSINESS_APP_SYS_ID> | --name "Epic" [--fresh] [--json] [--full]
snow business-app search --name Epic --operational-state-not 2 [--limit N] [--json] [--full]
snow business-app query --filter business_owner:contains:"Jane" [--limit N] [--json]
snow business-app query --filter u_custom_field:eq:"value"
snow business-app servers --number <APM_NUMBER> | --sys-id <BUSINESS_APP_SYS_ID> [--cached] [--json]
snow business-app servers --for-server <SERVER_SYS_ID> [--cached] [--json]
snow business-app export --all [--format json|jsonl|csv] [--output <PATH>]
snow business-app export --text Epic --filter business_owner:contains:"Jane" [--limit N] [--format csv] [--output <PATH>]
snow business-app fields [--refresh] [--json]
snow business-app sync --name Epic [--persist] [--resolve-references] [--reference-depth N] [--refresh-dictionary] [--json]
snow business-app sync --all [--json]
```

- `get` reads from the local cache/vault (after hydration); `--fresh` re-fetches the live row and updates the projection. Supply exactly one of `--sys-id` or `--name`.
- `search` runs a live `cmdb_ci_business_app` query and persists by default.
- `query` filters/sorts entirely against the local SQLite projection — no API call. Use the repeatable `--filter <field>:<op>:<value>` flag, where `op` is `contains` or `eq`. (The old `--field` + `--contains`/`--eq` flags were removed.)
- `servers` traverses the BA→server CMDB relationship graph. Select the BA with `--number <APM_NUMBER>` or `--sys-id <BUSINESS_APP_SYS_ID>`, or reverse the lookup with `--for-server <SERVER_SYS_ID>`. `--cached` reads the persisted BA↔server projection without a live fetch. Traversal bounds are tunable via `--max-depth`, `--max-cis`, `--max-edges`, `--max-service-membership-associations`, and `--max-service-membership-pages`; constrain edges with the repeatable `--relationship-type`. Other flags: `--include-paths`, `--fallback-strategy none|ci-owner-group`, `--no-persist`, `--prune-stale`, `--include-tombstoned`, and `--json`.
- `export` dumps Business Applications from the local projection. Pass `--all` for everything, or filter with `--text`/`--filter`/`--limit` (same `--filter` grammar as `query`). `--format` is `json` (default), `jsonl`, or `csv`; `--output <PATH>` writes to a file instead of stdout.
- `fields` lists dictionary-backed field metadata; `--refresh` triggers a live `sys_dictionary` fetch first (see below).
- `sync` runs a live search+persist and returns a roll-up summary (`total_applications`, `persisted`, `references_resolved`, `references_unresolved`, `dictionary_degraded`, `dictionary_refreshed`, `degraded_reasons`). `--persist` defaults on; `--refresh-dictionary` refreshes the dictionary once before syncing. `sync --all` drains the full live inventory and conflicts with `--name`/`--operational-state-not`.

Default human output shows name, sys_id, owners, groups, portfolio, operational status, attested date, vault path, and unresolved-reference count. `--json` emits the daemon view DTO; `--full` appends the all-fields table.

What a search, sync, or fresh fetch does:

- Fetches the complete readable ServiceNow row — no hand-picked `sysparm_fields`, with `display_value=all` — and maps it to a typed `BusinessApplication`.
- Persists by default. Each returned Business Application is written to the vault as canonical markdown at the stable path `business_applications/business_application_<sys_id>_<slug>.md` (for example `business_applications/business_application_<sys_id>_<slug>.md`). `persist=false` exists only for explicit debug/preview paths.
- Projects every returned field into the current disposable SQLite cache format so `business_application_query` filters and sorts on any observed field locally, without another API call. An incompatible cache is never upgraded in place: inspect it with `snow cache-info`, rebuild the configured ACL-readable projection from ServiceNow with `snow rebuild-cache`, explicitly discard it with `snow reset-cache`, or use `snow import-cache-from-vault` for an intentional offline vault restore. The current projection includes `business_application_servers` (BA↔server membership and path projection, provenance `relationship`/`service_membership`/`both`) and `business_application_server_inventory_health` tables backing the cached BA↔server reads (`business-app servers --cached` and `--for-server`).
- Resolves reference-valued sys_ids (owners, groups, portfolio) into local primitive objects when the target table is supported, or stores them as unresolved/blocked/unknown reference stubs otherwise. Reference-resolution failures are degraded reads — they do not fail the Business Application read.
- Returns `browser_url` and `vault_relative_path` in the daemon DTO when the instance URL and file path are available.

`business_application_fields` returns dictionary-enriched metadata. With `--refresh` (or `refresh_dictionary=true`), the daemon fetches live `sys_dictionary` rows for `cmdb_ci_business_app` and its inherited tables, caches them in `business_application_field_dictionary`, and merges label, field type, reference table, mandatory/read-only/choice flags, max length, and `dictionary_verified=true` with the observed per-field counts. When the dictionary is unreachable it falls back to observed-only entries plus a degraded diagnostic. Field aliases (`ci_owner_group`, `primary_portfolio` and its reference target table, `attested_date`) are dictionary-verified when metadata is available and use the hardcoded baseline otherwise.

Business Applications are also exposed through read-only MCP tools documented in `crates/snow_mcp/CAPABILITIES.md`. `business_application_sync` and `business_application_get_fresh` are JSON-RPC + CLI only; they are deliberately not MCP tools.

In the record browser TUI, `cmdb_ci_business_app` records route to a first-class Business Application detail view with typed ownership/operational sections (operational status, business owner, information owner, CI owner group, support group, portfolio, attested date) plus the all-fields table.

### Servers

Servers are a first-class read-only CMDB primitive for Windows and Linux CIs. The canonical local resource type is `server`; `cmdb_ci_server`, `cmdb_ci_linux_server`, `cmdb_ci_win_server`, `linux`, and `windows` are accepted as input aliases where a table/class selector is supported.

The `snow server` subcommand family is a thin CLI over daemon JSON-RPC methods (`server_get`, `server_get_fresh`, `server_search`, `server_query`, `server_fields`) and auto-spawns the daemon as needed. There is no Server write/create/update surface.

```bash
snow server get --sys-id <SERVER_SYS_ID> | --name "app01.example.internal" | --ip-address 192.0.2.10 [--fresh] [--json] [--full]
snow server search --name app01 --ip-address 192.0.2.10 --ci-owner-group "Platform Operations" --class linux [--limit N] [--json] [--full]
snow server query --ci-owner-group "Platform Operations" [--text app] [--class windows] [--limit N] [--json] [--full]
snow server fields [--json]
```

- `get` is a read-through cache keyed by `sys_id`, exact `name`, or exact `ip_address`: a cache hit returns the cached row; a cache miss triggers a live exact fetch, and on the CLI/daemon path that hit is persisted into the projection. Only a ServiceNow-confirmed 404 returns not-found (`-32004`); other failure modes return distinct structured errors — `-32003` (ACL-restricted), `-32001` (network/timeout), and `-32005` (multiple-match disambiguation). `--fresh` still forces a live refresh regardless of cache state.
- `search` runs a live bounded `cmdb_ci_server` query restricted to Linux/Windows subclasses, supports `name` contains, exact `ip_address`, `ci_owner_group` display-name/sys_id, and class filters, then persists returned rows.
- `query` filters entirely against the local SQLite projection with the same primitive filters, so CI owner group inventory queries do not need another API call after hydration.
- `fields` lists observed local Server fields.

Default human output shows name, sys_id, class, IP address, CI owner group, support group, operational status, vault path, and URL when available. `--json` emits the daemon view DTO; `--full` appends the all-fields table.

### List change tasks

```bash
snow tasks CHG0327604
```

### Approve a change

```bash
snow approve CHG0327604           # prompts for confirmation
snow approve CHG0327604 -y        # skip confirmation
```

### Reject a change

```bash
snow reject CHG0327604 --reason "Missing test plan"
snow reject CHG0327604            # prompts for reason interactively
```

### Add a work note

```bash
snow note CHG0327604 "Reviewed and looks good"
```

### Knowledge articles

```bash
snow knowledge KB0105015
snow knowledge KB0105015 --fresh
snow knowledge search "windows admin" --mode lexical
snow knowledge search "reset my laptop admin rights" --mode semantic
snow knowledge search "vpn access on mac" --mode hybrid --min-score-millis 250
snow knowledge bases
snow knowledge categories --knowledge-base "IT"
snow knowledge semantic status
snow knowledge semantic rebuild --full
```

Notes:

- `snow knowledge search` defaults to `--mode lexical`
- semantic and hybrid modes use the local KB semantic index and require semantic search to be enabled in config
- exact KB number lookups like `KB0105015` remain direct article reads; they do not require embeddings

### Daemon lifecycle

The `snow daemon` subcommands manage a long-running background daemon that hosts the job registry consumed by the admin TUI and other operator surfaces.

```bash
snow daemon start                 # start the daemon against production
snow --env test daemon start      # start the daemon against test
snow daemon stop                  # graceful JSON-RPC shutdown, with PID fallback
snow daemon restart               # stop, then start against production
snow --env test daemon restart    # restart against test
snow daemon status                # running / unreachable / stopped
snow daemon logs --lines 50       # tail the daemon log
snow daemon logs --follow         # stream new log lines as they appear
```

The pidfile lives at `~/.config/snow/daemon.pid` and the log at `~/.config/snow/daemon.log`. Daemon start, status, and logs include the selected environment.

Use `snow tui --daemon` to launch the record browser against the daemon endpoint; if it needs to auto-start the daemon, it uses the same production default. Pass `--env test` when you want daemon-mode TUI startup against test. The older `scripts/start_daemon.sh` and `scripts/start_tui.sh` wrappers have been removed because these flows are now handled by the CLI.

### Supervised daemon (macOS launchd)

`scripts/build_release.sh` installs the daemon as a user LaunchAgent
(`com.servicenow-agent.snow-daemon`) via `scripts/manage_daemon_launchagent.sh install`,
rather than starting it with `snow daemon start`. launchd runs
`snow daemon __serve --no-idle-timeout` in the foreground and becomes the **sole**
owner of the daemon process: it restarts the daemon on non-zero exit and keeps it
alive across idle periods, and logs land in `~/Library/Logs/snow/`.

Because launchd owns the process, do not use `snow daemon start` or
`snow daemon restart` against a supervised daemon — both detach a child that
outlives launchd's lifecycle bookkeeping, leaving two competing owners on one
socket. Use launchd instead, and reserve the `snow daemon` lifecycle commands for
unsupervised local runs:

```bash
launchctl kickstart -k gui/$(id -u)/com.servicenow-agent.snow-daemon   # restart
launchctl bootout gui/$(id -u)/com.servicenow-agent.snow-daemon        # stop
snow daemon status                                                     # still the health check
```

The installer drains any pre-existing `snow daemon __serve` owner before
bootstrapping, and fails if the service never reaches a running state or its
endpoint never becomes reachable. For the same reason, `snow_mcp_bridge` accepts
`--no-auto-spawn`, which suppresses its `snow daemon start` fallback so an MCP
client cannot spawn a competing daemon behind launchd's back.

### Admin TUI

```bash
snow admin
```

`snow admin` opens an operator TUI with four tabs — Daemon, Sync, Cache/Vault, and Config — backed by the running daemon. It includes a persistent job tray for long-running operations (with an overlay to inspect and cancel jobs) and a two-tier confirmation modal for destructive actions such as full KB resync or vault rebuild.

### Guarded Task SLA smoke

```bash
SNOW_SMOKE_ALLOWED_INSTANCE=https://example.service-now.com \
SNOW_TASK_SLA_NUMBER=<record-number> \
python3 scripts/task_sla_training_smoke.py

SNOW_SMOKE_ALLOWED_INSTANCE=https://example.service-now.com \
python3 scripts/task_sla_training_smoke.py --check
```

The smoke harness loads `.env.test` and refuses to continue unless the normalized `SERVICENOW_INSTANCE` exactly matches the separate `SNOW_SMOKE_ALLOWED_INSTANCE` value or `--allowed-instance`. Live mode runs an already-built `snow` binary; build first with `cargo build -p snow_cli` or pass `--snow-bin`. `--check` validates the guard without resolving credentials, invoking the CLI, or contacting ServiceNow.

## Environment Selection

1. `--env` flag (e.g., `--env prd` or `--env test`)
2. `SNOW_ENV` environment variable
3. Saved daemon selection in `~/.config/snow/env`
4. Defaults to `test` for regular commands and `prd` for daemon startup

## Authentication

By default, provide ServiceNow connection settings through `SERVICENOW_INSTANCE`, `SERVICENOW_USERNAME`, and `SERVICENOW_PASSWORD`. This supports any external vault that can inject environment variables at runtime. 1Password remains optional through `OP_ITEM_ID` for environments that use the `op` CLI. Plain item IDs use `op item get`; `op://vault/item` references use `op read op://vault/item/password`. When authenticating `op` with a service account, export `OP_SERVICE_ACCOUNT_TOKEN` in the parent environment or set `OP_SERVICE_ACCOUNT_TOKEN_FILE` to an ignored local token file, and set `OP_VAULT` for plain item IDs. If `SERVICENOW_USERNAME` is unset, `snow` reads the `username` field from the same 1Password item. Do not commit real `.env.test`, `.env.prd`, passwords, vault item IDs, cookies, service account tokens, or session tokens.

At runtime, `snow` resolves the password into zeroizing memory, clears password environment variables after resolution, builds Basic auth clients with session-cookie reuse disabled, and drops the resolved password before awaiting client construction. This reduces retained secret material; it does not eliminate transient copies from env/config loading, 1Password subprocess output, request headers, reqwest internals, or OS/process memory.
