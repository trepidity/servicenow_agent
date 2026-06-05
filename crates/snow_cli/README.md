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

## Usage

By default, `snow` uses the `test` environment. Use `--env prd` or set `SNOW_ENV=prd` for production.

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

Task SLA output reports aggregate counts, the next active unbreached breach target, and bounded row detail. Empty results are intentionally ambiguous as "none readable or no rows" because ACLs and an actually empty SLA set cannot be distinguished from the client side.

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

### Business Applications

Business Applications (`cmdb_ci_business_app`) are a first-class local primitive; the canonical local resource type is `business_application` (`cmdb_ci_business_app` is an accepted filter alias). The read-only surface is the daemon JSON-RPC methods (`business_application_search`, `business_application_get`, `business_application_query`, `business_application_fields`, `business_application_sync`) and the matching MCP tools, driven through a running daemon (`snow daemon start`). There is no write/create/update surface.

The `snow business-app` subcommand family is a thin CLI over those daemon methods (it auto-spawns the daemon as needed):

```bash
snow business-app get --sys-id <sys_id> | --name "Epic" [--fresh] [--json] [--full]
snow business-app search --name Epic --operational-state-not 2 [--limit N] [--json] [--full]
snow business-app query --field business_owner --contains "Jane" [--limit N] [--json]
snow business-app query --field u_custom_field --eq "value"
snow business-app export --format <json|jsonl|csv> --output ./output/business-apps.csv [--text "Example Application"] [--field name --contains "Example"] [--limit N]
snow business-app export --all --format <json|jsonl|csv> --output ./output/business-apps.csv
snow business-app fields [--refresh] [--json]
snow business-app sync --name Epic [--persist] [--resolve-references] [--reference-depth N] [--refresh-dictionary] [--json]
snow business-app sync --all --persist [--resolve-references] [--reference-depth N] [--refresh-dictionary] [--json]
```

Default human output shows name, sys_id, owners, groups, portfolio, operational status, attested date, vault path, and unresolved-reference count; `--json` emits the raw daemon payload and `--full` adds the all-fields table. `query`'s `--field` is repeatable and pairs by position with `--contains`/`--eq` (one operator value per field). `export` writes local `business_application_query` results to JSON, JSONL, or CSV after validating `--limit` and the output parent locally; `export --all` drains the cached local Business Application projection page-by-page and rejects search/filter/limit options. `sync` runs a live search+persist and prints a roll-up summary (`total_applications`, `persisted`, `references_resolved`, `references_unresolved`, `dictionary_degraded`, `dictionary_refreshed`, `degraded_reasons`); `sync --all --persist` pages the live Business Application table before local export, and rejects bounded sync filters. `--persist` defaults on. The TUI routes `cmdb_ci_business_app` records to a first-class Business Application detail view (typed ownership/operational sections, then the all-fields table).

Search/sync/fresh-fetch persists by default: each Business Application is fetched as a full row (no `sysparm_fields`, `display_value=all`), written to the vault as canonical markdown at the stable path `business_applications/business_application_<sys_id>_<slug>.md`, and projected into local SQLite (schema v8) so `business_application_query` filters/sorts on any field locally. Reference sys_ids (owners, groups, portfolio) hydrate into local primitive objects when supported, or persist as unresolved stubs otherwise; reference failures degrade rather than fail the read. The daemon DTO returns `browser_url` and `vault_relative_path` when available.

`snow business-app fields` returns dictionary-enriched metadata. `--refresh` triggers a live `sys_dictionary` fetch for `cmdb_ci_business_app` and its inherited tables (cached in `business_application_field_dictionary`); each entry then merges label, field type, reference table, mandatory/read-only/choice flags, max length, and `dictionary_verified=true` with observed per-field counts. When the dictionary is unreachable, entries fall back to observed-only plus a degraded diagnostic. See `crates/snow_mcp/CAPABILITIES.md` for the MCP tools — `sync` is JSON-RPC + CLI only and is deliberately not an MCP tool.

### Servers

Servers are a first-class read-only CMDB primitive for Linux and Windows CIs. The canonical local resource type is `server`; `cmdb_ci_server`, `cmdb_ci_linux_server`, `cmdb_ci_win_server`, `linux`, and `windows` are accepted aliases where a table/class selector is supported.

The `snow server` subcommand family is a thin CLI over daemon JSON-RPC methods and auto-spawns the daemon as needed:

```bash
snow server get --sys-id <sys_id> | --name "app01.example.internal" | --ip-address 192.0.2.10 [--fresh] [--json] [--full]
snow server search --name app01 --ip-address 192.0.2.10 --ci-owner-group "Platform Operations" --class linux [--limit N] [--json] [--full]
snow server query --ci-owner-group "Platform Operations" [--text app] [--class windows] [--limit N] [--json] [--full]
snow server fields [--json]
```

`get` accepts exactly one selector (`--sys-id`, `--name`, or `--ip-address`). `search` is live, bounded, Linux/Windows-only, and persists returned rows to `servers/server_<sys_id>_<slug>.md`; `query` reads the local SQLite projection and supports the same name/IP/class/CI-owner-group filters. Human output shows name, sys_id, class, IP address, CI owner group, support group, operational status, vault path, and URL when available; `--full` adds the all-fields table.

## Daemon and TUI

```bash
snow daemon start
snow daemon status
snow tui --daemon
snow admin
```

Use the native `snow daemon`, `snow tui --daemon`, and `snow admin` commands. The older launch-wrapper scripts have been removed because these flows are handled by the CLI.

## Environment Selection

1. `--env` flag (e.g., `--env prd`)
2. `SNOW_ENV` environment variable
3. Defaults to `test`

## Authentication

By default, provide ServiceNow connection settings through `SERVICENOW_INSTANCE`, `SERVICENOW_USERNAME`, and `SERVICENOW_PASSWORD`. This supports any external vault that can inject environment variables at runtime. 1Password remains optional through `OP_ITEM_ID` for environments that use the `op` CLI. Plain item IDs use `op item get`; `op://vault/item` references use `op read op://vault/item/password`. When authenticating `op` with a service account, export `OP_SERVICE_ACCOUNT_TOKEN` in the parent environment or set `OP_SERVICE_ACCOUNT_TOKEN_FILE` to an ignored local token file, and set `OP_VAULT` for plain item IDs. If `SERVICENOW_USERNAME` is unset, `snow` reads the `username` field from the same 1Password item. Do not commit real `.env.test`, `.env.prd`, passwords, vault item IDs, cookies, service account tokens, or session tokens.

## Guarded Task SLA smoke

```bash
SNOW_SMOKE_ALLOWED_INSTANCE=https://example.service-now.com \
SNOW_TASK_SLA_NUMBER=<record-number> \
python3 scripts/task_sla_training_smoke.py

SNOW_SMOKE_ALLOWED_INSTANCE=https://example.service-now.com \
python3 scripts/task_sla_training_smoke.py --check
```

The smoke harness loads `.env.test` and refuses to continue unless the normalized `SERVICENOW_INSTANCE` exactly matches the separate `SNOW_SMOKE_ALLOWED_INSTANCE` value or `--allowed-instance`. Live mode runs an already-built `snow` binary; build first with `cargo build -p snow_cli` or pass `--snow-bin`. `--check` validates the guard without resolving credentials, invoking the CLI, or contacting ServiceNow.
