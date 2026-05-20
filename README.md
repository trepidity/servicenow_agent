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
snow daemon start                 # fork the daemon as a background process
snow --env prd daemon start       # fork the daemon against production
snow daemon stop                  # SIGTERM, then SIGKILL after 10s
snow daemon restart               # stop, then start
snow --env prd daemon restart     # restart against production
snow daemon status                # running / unreachable / stopped
snow daemon logs --lines 50       # tail the daemon log
snow daemon logs --follow         # stream new log lines as they appear
```

The pidfile lives at `~/.config/snow/daemon.pid` and the log at `~/.config/snow/daemon.log`. Daemon start, status, and logs include the selected environment.

Use `snow tui --daemon` to launch the record browser against the daemon socket, or `snow --env prd tui --daemon` when you want the TUI header to reflect production mode. The older `scripts/start_daemon.sh` and `scripts/start_tui.sh` wrappers have been removed because these flows are now handled by the CLI.

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

1. `--env` flag (e.g., `--env prd`)
2. `SNOW_ENV` environment variable
3. Defaults to `test`

## Authentication

By default, provide the ServiceNow password through `SNOW_PASSWORD` or `SERVICENOW_PASSWORD`. This supports any external vault that can inject an environment variable at runtime. 1Password remains optional through `OP_ITEM_ID` for environments that use the `op` CLI. Do not commit real `.env.test`, `.env.prd`, passwords, vault item IDs, cookies, or session tokens.
