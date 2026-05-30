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
   SNOW_INSTANCE=https://your-instance.service-now.com
   SNOW_USER=your_username

   # Default: inject a resolved password from your shell or vault.
   SNOW_PASSWORD=replace_with_runtime_secret

   # Optional 1Password compatibility path. Leave SNOW_PASSWORD unset when using it.
   # OP_ITEM_ID=your_1password_item_id
   ```

4. Copy the env file next to the binary when running the release binary directly:
   ```bash
   cp .env.test target/release/.env.test
   ```

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

The `snow business-app` subcommand family is a thin CLI over the daemon JSON-RPC methods (`business_application_get`, `business_application_search`, `business_application_query`, `business_application_fields`, `business_application_sync`); it drives a running daemon and auto-spawns it as needed. There is no Business Application write/create/update surface — every subcommand is a read (`sync`/`search` perform a live ServiceNow read plus a local vault/SQLite write).

```bash
snow business-app get --sys-id <sys_id> | --name "Epic" [--fresh] [--json] [--full]
snow business-app search --name Epic --operational-state-not 2 [--limit N] [--json] [--full]
snow business-app query --field business_owner --contains "Jane" [--limit N] [--json]
snow business-app query --field u_custom_field --eq "value"
snow business-app fields [--refresh] [--json]
snow business-app sync --name Epic [--persist] [--resolve-references] [--reference-depth N] [--refresh-dictionary] [--json]
```

- `get` reads from the local cache/vault (after hydration); `--fresh` re-fetches the live row and updates the projection. Supply exactly one of `--sys-id` or `--name`.
- `search` runs a live `cmdb_ci_business_app` query and persists by default.
- `query` filters/sorts entirely against the local SQLite projection — no API call. `--field` is repeatable and pairs by position with `--contains`/`--eq` (one operator value per field).
- `fields` lists dictionary-backed field metadata; `--refresh` triggers a live `sys_dictionary` fetch first (see below).
- `sync` runs a live search+persist and returns a roll-up summary (`total_applications`, `persisted`, `references_resolved`, `references_unresolved`, `dictionary_degraded`, `dictionary_refreshed`, `degraded_reasons`). `--persist` defaults on; `--refresh-dictionary` refreshes the dictionary once before syncing.

Default human output shows name, sys_id, owners, groups, portfolio, operational status, attested date, vault path, and unresolved-reference count. `--json` emits the daemon view DTO; `--full` appends the all-fields table.

What a search, sync, or fresh fetch does:

- Fetches the complete readable ServiceNow row — no hand-picked `sysparm_fields`, with `display_value=all` — and maps it to a typed `BusinessApplication`.
- Persists by default. Each returned Business Application is written to the vault as canonical markdown at the stable path `business_applications/business_application_<sys_id>_<slug>.md` (for example `business_applications/business_application_54a4b61b6fe845000ed852a03f3ee4d0_epic.md`). `persist=false` exists only for explicit debug/preview paths.
- Projects every returned field into local SQLite (schema v8) so `business_application_query` filters and sorts on any observed field locally, without another API call.
- Resolves reference-valued sys_ids (owners, groups, portfolio) into local primitive objects when the target table is supported, or stores them as unresolved/blocked/unknown reference stubs otherwise. Reference-resolution failures are degraded reads — they do not fail the Business Application read.
- Returns `browser_url` and `vault_relative_path` in the daemon DTO when the instance URL and file path are available.

`business_application_fields` returns dictionary-enriched metadata. With `--refresh` (or `refresh_dictionary=true`), the daemon fetches live `sys_dictionary` rows for `cmdb_ci_business_app` and its inherited tables, caches them in `business_application_field_dictionary`, and merges label, field type, reference table, mandatory/read-only/choice flags, max length, and `dictionary_verified=true` with the observed per-field counts. When the dictionary is unreachable it falls back to observed-only entries plus a degraded diagnostic. Field aliases (`ci_owner_group`, `primary_portfolio` and its reference target table, `attested_date`) are dictionary-verified when metadata is available and use the hardcoded baseline otherwise.

Business Applications are also exposed as four read-only MCP tools (`get`/`search`/`query`/`fields`) — see `crates/snow_mcp/CAPABILITIES.md`. `business_application_sync` and `business_application_get_fresh` are JSON-RPC + CLI only; they are deliberately not MCP tools.

In the record browser TUI, `cmdb_ci_business_app` records route to a first-class Business Application detail view with typed ownership/operational sections (operational status, business owner, information owner, CI owner group, support group, portfolio, attested date) plus the all-fields table.

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

The smoke harness loads `.env.test` and refuses to continue unless the normalized `SNOW_INSTANCE` exactly matches the separate `SNOW_SMOKE_ALLOWED_INSTANCE` value or `--allowed-instance`. Live mode runs an already-built `snow` binary; build first with `cargo build -p snow_cli` or pass `--snow-bin`. `--check` validates the guard without resolving credentials, invoking the CLI, or contacting ServiceNow.

## Environment Selection

1. `--env` flag (e.g., `--env prd` or `--env test`)
2. `SNOW_ENV` environment variable
3. Saved daemon selection in `~/.config/snow/env`
4. Defaults to `test` for regular commands and `prd` for daemon startup

## Authentication

By default, provide the ServiceNow password through `SNOW_PASSWORD` or `SERVICENOW_PASSWORD`. This supports any external vault that can inject an environment variable at runtime. 1Password remains optional through `OP_ITEM_ID` for environments that use the `op` CLI. Do not commit real `.env.test`, `.env.prd`, passwords, vault item IDs, cookies, or session tokens.

At runtime, `snow` resolves the password into zeroizing memory, clears password environment variables after resolution, builds Basic auth clients with session-cookie reuse disabled, and drops the resolved password before awaiting client construction. This reduces retained secret material; it does not eliminate transient copies from env/config loading, 1Password subprocess output, request headers, reqwest internals, or OS/process memory.
