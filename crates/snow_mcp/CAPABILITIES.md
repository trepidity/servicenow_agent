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
| **Capability** | compile time | What tools exist? Which mutate ServiceNow? | `is_write_tool()` — `src/domain/policy.rs:421` |
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
| Story (`rm_story`) | `story_apply_update` | **Update** | ❌ | yes | governed; daemon required |
| Story task (`rm_scrum_task`) | `story_task_apply_create` | **Create** | ❌ | yes | governed; daemon required |
| Story task (`rm_scrum_task`) | `story_task_apply_update` | **Update** | ❌ | yes | governed; daemon required |
| Time card (`time_card`) | `timecard_apply_set_hours` | **Update** | ❌ | yes | governed; daemon required; day fields only |
| Change request | `change_submit_request` | **Create** | ❌ | yes | ⚠️ scaffold — not implemented |
| Change task | `change_task_apply_assignment` | **Update** | ❌ | yes | `assigned_to`,`start_date`,`end_date`; `max_records=20` |
| MCP operation plan | `plan_cancel` | **Delete** (cancel) | ❌ | yes | cancels a pending plan, not a SN record |

`*_plan_*` tools (`story_plan_create`, `story_plan_update`, `story_task_plan_create`,
`story_task_plan_update`, `change_plan_request`, `change_task_plan_assignment`,
`timecard_plan_set_hours`, `work_note_plan_add`, `catalog_plan_request`) are **not** transactions — they
build/preview a plan and never mutate ServiceNow. The matching `*_apply_*` /
`*_submit_*` tool executes the plan.

**Enforcement at runtime:**
- Default posture is `read_only` (`default_mode`, `policy.rs:495`).
- The daemon bridge rejects all non-governed write tools — `-32040 policy denied` (`daemon_bridge.rs`).
- Governed Story/time-card writes need an attached daemon, else `-32044 DAEMON_REQUIRED_FOR_WRITE` (`server.rs`).
- A disabled tool returns `-32040 policy denied`.

---

## Read-only tools

Enabled by default (unless a policy entry disables them). Operate against ServiceNow
or the local cache; none mutate ServiceNow records.

- **Records:** `get_record`, `search_records`, `list_records`, `list_my_tasks`,
  `list_my_approvals`, `list_my_projects`, `get_approval`, `get_children`, `get_work_notes`
- **Knowledge:** `search_knowledge`, `knowledge_search`, `kb_semantic_search`, `get_article`,
  `knowledge_fetch`, `knowledge_answer`, `knowledge_grounded_plan`, `list_knowledge_bases`,
  `list_categories`, `list_knowledge_articles`, `vault_path`, `kb_status`,
  `kb_semantic_status`, `kb_list_tags`, `verify_vault`
- **Catalog / plans:** `catalog_items_search`, `catalog_item_get`, `catalog_plan_request`,
  `resource_plan_get`, `story_get`, `story_tasks_list`, `timecard_list`,
  `timecard_plan_set_hours`, `plan_get`
- **Governance / audit:** `policy_describe`, `tool_capabilities`, `redaction_rules_describe`,
  `audit_event_get`, `audit_events_search`, `audit_chain_verify`

> Local-cache writers (`kb_sync`, `kb_semantic_rebuild`, `rebuild_cache`, `repair_vault`)
> write only to the local KB vault/cache — **never** to ServiceNow.

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
// NOTE: `read_only` is currently hard-coded true for every entry
// (registry.rs / daemon_bridge.rs) — use `mode` ("read" | "write") and
// `enabled` to decide what an agent may call, not `read_only`.
{
  "environment": "test",
  "default_mode": "read_only",
  "tools": [
    { "name": "get_record",         "enabled": true,  "mode": "read",  "read_only": true, "requires_confirmation": false },
    { "name": "story_apply_create", "enabled": false, "mode": "write", "read_only": true, "requires_confirmation": true }
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
