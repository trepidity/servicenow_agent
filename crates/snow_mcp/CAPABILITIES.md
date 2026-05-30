# snow_mcp — Tool Capabilities & Transaction Policy

Canonical reference for the ServiceNow MCP server's tools, which ones perform
**write transactions**, and how a deployer enables/disables them per environment.

> **For consuming agents:** this document describes the *static* model. The
> **authoritative, live answer** for a specific deployment comes from the
> runtime tools [`tool_capabilities`](#runtime-introspection) and
> `policy_describe` — call them at startup. A static doc cannot know which
> tools a given policy file has enabled.

There are two layers to "what is allowed":

| Layer | Fixed at | Question it answers | Source of truth |
|-------|----------|---------------------|-----------------|
| **Capability** | compile time | What tools exist? Which mutate ServiceNow? | `is_write_tool()` — `src/domain/policy.rs:546` |
| **Policy** | deploy time | Which tools are *enabled*, where, with what limits? | TOML at `SNOW_MCP_POLICY_PATH` (see [below](#deploy-time-policy)) |

---

## Write transactions (mutate ServiceNow / plan state)

A tool is a "write tool" if its name contains `_apply_` or `_submit_`, or it is
explicitly listed in `is_write_tool()`. Everything else is read-only.

| ServiceNow entity | Tool | Action | Enabled by default | Confirm? | Field allowlist / limits |
|---|---|---|---|---|---|
| Catalog request (`sc_req_item`) | `catalog_submit_request` | **Create** | ✅ (test/training) | yes | requires KB evidence |
| Catalog request (`sc_req_item`) | `catalog_cancel_request` | **Delete** (cancel) | ❌ | yes | |
| Work note / journal | `work_note_apply_add` | **Create** | ✅ (test/training) | yes | `work_notes` only |
| Story (`rm_story`) | `story_apply_create` | **Create** | ❌ | yes | governed; daemon required |
| Story (`rm_story`) | `story_apply_update` | **Update** | ❌ | yes | governed; daemon required; includes `state`,`percent_complete` |
| Story task (`rm_scrum_task`) | `story_task_apply_create` | **Create** | ❌ | yes | governed; daemon required |
| Story task (`rm_scrum_task`) | `story_task_apply_update` | **Update** | ❌ | yes | governed; daemon required; includes `state`,`remaining_hours`,`percent_complete` |
| Attachment (`sys_attachment`) | `attachment_upload` | **Create** | ❌ | yes | local-file upload; disabled unless policy allows it |
| Time card (`time_card`) | `timecard_apply_set_hours` | **Update** | ❌ | yes | governed; daemon required; day fields only |
| Change request (`change_request`) | `change_request_apply_create` | **Create** | ❌ | yes | governed; daemon required; no delete/cancel |
| Change request (`change_request`) | `change_request_apply_update` | **Update** | ❌ | yes | governed; daemon required; field allowlist |
| Change task (`change_task`) | `change_task_apply_create` | **Create** | ❌ | yes | governed; daemon required |
| Change task (`change_task`) | `change_task_apply_update` | **Update** | ❌ | yes | governed; daemon required; terminal records skipped by policy |
| MCP operation plan | `plan_cancel` | **Delete** (cancel) | ❌ | yes | cancels a pending plan, not a SN record |

`*_plan_*` tools (`story_plan_create`, `story_plan_update`, `story_task_plan_create`,
`story_task_plan_update`, `change_request_plan_create`, `change_request_plan_update`,
`change_task_plan_create`, `change_task_plan_update`,
`timecard_plan_set_hours`, `work_note_plan_add`, `catalog_plan_request`) are **not** transactions — they
build/preview a plan and never mutate ServiceNow. The matching `*_apply_*` /
`*_submit_*` tool executes the plan.

**Enforcement at runtime:**
- Default posture is `read_only` (`default_mode`, `policy.rs:495`).
- The daemon bridge rejects all non-governed write tools — `-32040 policy denied` (`daemon_bridge.rs`).
- Governed Story, Change, and time-card writes need an attached daemon, else `-32044 DAEMON_REQUIRED_FOR_WRITE` (`server.rs`).
- A disabled tool returns `-32040 policy denied`.

---

## Read-only tools

Enabled by default (unless a policy entry disables them). Operate against ServiceNow
or the local cache; none mutate ServiceNow records.

- **Records:** `get_record`, `search_records`, `user_lookup`, `business_application_get`,
  `business_application_search`, `business_application_query`, `business_application_fields`,
  `list_records`, `list_my_tasks`, `list_my_approvals`, `list_my_projects`, `get_approval`,
  `get_children`, `get_work_notes`, `attachment_list`
- **Knowledge:** `search_knowledge`, `knowledge_search`, `kb_semantic_search`, `get_article`,
  `knowledge_fetch`, `knowledge_answer`, `knowledge_grounded_plan`, `list_knowledge_bases`,
  `list_categories`, `list_knowledge_articles`, `vault_path`, `kb_status`,
  `kb_semantic_status`, `kb_list_tags`, `verify_vault`
- **Catalog / plans:** `catalog_items_search`, `catalog_item_get`, `catalog_plan_request`,
  `resource_plan_get`, `story_get`, `story_tasks_list`, `timecard_list`,
  `timecard_plan_set_hours`, `change_request_plan_create`, `change_request_plan_update`,
  `change_task_plan_create`, `change_task_plan_update`, `plan_get`
- **Governance / audit:** `policy_describe`, `tool_capabilities`, `redaction_rules_describe`,
  `audit_event_get`, `audit_events_search`, `audit_chain_verify`

> Local-cache writers (`kb_sync`, `kb_semantic_rebuild`, `rebuild_cache`, `repair_vault`)
> write only to the local KB vault/cache — **never** to ServiceNow.

---

## Business Applications (read-only primitive)

Business Applications (`cmdb_ci_business_app`) are a first-class local primitive
with four **read-only** MCP tools. None mutate ServiceNow — there is **no
create/update/delete/retire surface**. All four are **enabled by default** (they
are read tools) and are included in the `read_only_agent` role allow-list in
[`policy.example.toml`](./policy.example.toml).

| Tool | What it does | Live API call? | Persists to vault? |
|---|---|---|---|
| `business_application_get` | Fetch one Business Application by `sys_id` or exact `name` | No (serves the local cache/vault) | n/a — reads local |
| `business_application_search` | Live query `cmdb_ci_business_app` by name/owner/group/portfolio/state | Yes | Yes, by default |
| `business_application_query` | Local SQLite query/filter/sort across **all** projected BA fields | No | n/a — reads local |
| `business_application_fields` | List dictionary-enriched BA field metadata merged with per-field observed counts (`refresh_dictionary` triggers a live `sys_dictionary` fetch) | Only when `refresh_dictionary=true` | n/a — reads local |

Hydration behavior (search and the daemon `*_get_fresh` path): full-row fetch (no
`sysparm_fields`, `sysparm_display_value=all`), persist to
`business_applications/business_application_<sys_id>_<slug>.md`, project all
fields into SQLite (schema v8), and hydrate referenced sys_ids — owners, groups,
portfolio — into local primitive objects (or unresolved/blocked/unknown stubs).
Reference-resolution failures are **degraded reads**: the BA read still succeeds
and surfaces diagnostics rather than failing.

### `business_application_get`

Reads a single Business Application from the local cache/vault. Schema is a strict
union: supply **exactly one** of `sys_id` or `name`.

- **Params:** `sys_id` (32-hex) **xor** `name` (exact match). Hydration knobs
  `persist` (default `true`), `resolve_references` (default `true`),
  `reference_depth` (`0`–`2`, default `1`), `refresh_dictionary` (default
  `false`) are accepted on the foreground schema; the live re-fetch they drive is
  the daemon `business_application_get_fresh` path.
- **Returns:** `{ "business_application": <BA>, "markdown": "<rendered markdown>" }`.
  Not found → JSON-RPC error `-32004`.

### `business_application_search`

Live query against `cmdb_ci_business_app`; **persists every returned BA by
default** (`persist = true`). Reference filters accept either a sys_id or a
display-name substring.

- **Filter params (all optional):** `name`, `business_owner`, `is_owner`,
  `ci_owner_group`, `primary_support_group`, `operational_state`,
  `operational_state_not`, `primary_portfolio`, `attested_date` (and
  `attested_date_on_or_after` / `attested_date_on_or_before`, all
  `YYYY-MM-DD`), `limit` (`1`–`100`, default `20`).
- **Hydration params:** `persist` (default `true`), `resolve_references` (default
  `true`), `reference_depth` (`0`–`2`, default `1`), `refresh_dictionary`
  (default `false`). Empty params produce the default bounded search ordered by
  `name`.
- **Returns:** `{ "business_applications": [<BA>...], "records": [<record>...] }`
  — the `records` array mirrors each BA's `record` field for callers that consume
  the existing `SnowRecord` shape.

### `business_application_query`

LOCAL SQLite query/filter/sort across all projected BA fields. No live API call;
materializes from vault first with `raw_json` fallback. Unknown field names are
allowed (the server sets `allow_unknown_fields`), so newly observed `u_*` /
custom fields are queryable.

- **Params:** `text` (free-text search), `filters[]` (each
  `{ field, op, value }`, `op` ∈ `eq, ne, contains, starts_with, in, is_empty,
  is_not_empty, gt, gte, lt, lte`), `include_tombstoned` (default `false`),
  `limit` (`1`–`500`, default `20`), `offset` (default `0`), `sort[]` (each
  `{ field, direction }`, `direction` ∈ `asc, desc`). Filter `field` accepts
  ServiceNow field names or convenience aliases (`business_owner`, `is_owner`,
  `ci_owner_group`, `primary_support_group`, `operational_state`,
  `primary_portfolio`).
- **Returns:** `{ "business_applications": [<BA>...] }`.

### `business_application_fields`

Reports dictionary-backed field metadata for the BA table and its ancestors,
merged with the fields observed across the local cache. With
`refresh_dictionary=true`, the daemon first fetches live `sys_dictionary` rows
for `cmdb_ci_business_app` and its inherited tables and caches them in
`business_application_field_dictionary`; entries then carry
`dictionary_verified=true`. When the dictionary is unreachable, entries fall
back to observed-only (`dictionary_verified=false`) plus a degraded
`diagnostic`.

- **Params:** `refresh_dictionary` (default `false`).
- **Returns:** `{ "fields": [{ "field": "<name>", "observed_count": <n>,
  "dictionary_verified": <bool>, "label"?, "field_type"?, "reference_table"?,
  "mandatory"?, "read_only"?, "choice"?, "max_length"?, "sample_value"?,
  "sample_display_value"?, "diagnostic"? }...] }`. Dictionary-only fields appear
  with `observed_count = 0`; observed-only fields appear with
  `dictionary_verified = false`.

### `<BA>` return shape

Each `business_application` object contains:

- `record` — the underlying `SnowRecord` (also carries `browser_url` and
  `vault_relative_path`).
- `name`.
- Typed reference fields when present: `business_owner` (`business_owner` →
  `sys_user`), `is_owner` (`it_application_owner` → `sys_user`), `ci_owner_group`
  (`managed_by_group` → `sys_user_group`), `primary_support_group`
  (`support_group` → `sys_user_group`), `primary_portfolio` (`portfolio`).
- `operational_state` (from `operational_status`), `attested_date` when present.
- `fields` — every readable returned field (value + display value).
- `unresolved_references` — reference-hydration diagnostics. On the foreground
  server this is an empty array; the daemon-backed path
  (`business_application_get_fresh`, `business_application_search`) populates it
  with `DaemonBusinessApplicationDiagnostic` entries
  (`reference_not_found`, `reference_acl_restricted`, `unknown_reference_table`,
  `dictionary_unavailable`, `reference_resolution_failed`, etc.).
- `browser_url`, `vault_relative_path` — when the instance URL and file path are
  available.

### Policy and bridge notes

- These are **read tools**: no confirmation token, no write-policy enablement,
  no `requires_kb_evidence`. They still respect normal MCP allow/deny filtering
  and per-role allow-lists.
- The daemon bridge forwards all four to the matching daemon JSON-RPC methods and
  gates them on daemon `contract_info.supported_methods` — they are only
  advertised/callable when the attached daemon reports support. Search forwards
  the hydration options (`persist`, `resolve_references`, `reference_depth`,
  `refresh_dictionary`) to the daemon. The daemon-only
  `business_application_get_fresh` and `business_application_sync` methods are
  **not** exposed as foreground MCP tools.

---

## Deploy-time policy

Override compiled defaults without recompiling. The daemon reads a TOML file
named by the `SNOW_MCP_POLICY_PATH` env var
(`snow_daemon/src/lib.rs::mcp_config_from_env` → `PolicyConfig::from_toml_str`):

```bash
export SNOW_MCP_POLICY_PATH=/etc/snow/policy.toml
export SNOW_ENV=test
snow_daemon
```

Copy [`policy.example.toml`](./policy.example.toml) as a starting point. The whole
document is namespaced under `[mcp]`. Key rules:

- **Enable a write tool:** add `[mcp.tools.<name>]` with `enabled = true`.
- **Disable any tool:** `[mcp.tools.<name>]` with `enabled = false`.
- **No entry?** Read tools default enabled; write tools default disabled.
- **Environment gate:** `environments = ["test", "training"]` — the tool is
  callable only when `SNOW_ENV` is in this list (empty = all environments).
- Per-tool knobs: `requires_confirmation`, `requires_kb_evidence`,
  `field_allowlist`, `confirmation_ttl_seconds`, `max_records`,
  `skip_terminal_records`, `story_board_id`.
- Optional `[mcp.roles.<role>]` allow-lists further restrict a caller, intersected
  with the per-tool policy.

Minimal example — turn on one write tool in non-prod only:

```toml
[mcp]
default_mode = "read_only"

[mcp.tools.story_apply_create]
enabled = true
requires_confirmation = true
environments = ["test", "training"]
field_allowlist = ["short_description", "description", "priority", "assigned_to"]
```

---

## Runtime introspection

Consuming agents should discover the *live* policy rather than trust this doc:

- **`tool_capabilities`** → `{ environment, default_mode, tools: [{ name, enabled, mode, read_only, requires_confirmation }] }`
- **`policy_describe`** → `{ environment, default_mode, roles, write_tools_enabled, kb_freshness_days, idempotency_window_seconds, phi_in_work_notes, evaluation_order }`

Example agent contract: *call `tool_capabilities` once at session start; only
attempt a write tool if its `enabled` is true; if `requires_confirmation`, obtain
a confirm token before the apply call.*

```jsonc
// shape returned by tool_capabilities (per-deployment values vary).
{
  "environment": "test",
  "default_mode": "read_only",
  "tools": [
    { "name": "get_record",         "enabled": true,  "mode": "read",  "read_only": true, "requires_confirmation": false },
    { "name": "story_apply_create", "enabled": false, "mode": "write", "read_only": false, "requires_confirmation": true }
  ]
}
```

---

## Keeping this in sync

The tables above are hand-maintained against `src/domain/policy.rs`. When you add
a tool, change `is_write_tool()`, or change a default in `default_tools()`, update
the [write](#write-transactions-mutate-servicenow--plan-state) /
[read-only](#read-only-tools) tables here. The runtime `tool_capabilities` output
is always authoritative for a running deployment.
