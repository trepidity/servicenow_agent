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
| Approval (`sysapproval_approver`) | `approval_approve` | **Update** | ❌ | yes | target record number; daemon required |
| Approval (`sysapproval_approver`) | `approval_reject` | **Update** | ❌ | yes | target record number plus reason; daemon required |
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

Read cache policy is data-class based:

- Stable reference primitives, including `sys_user` lookup/search results, use
  a seven-day TTL before the daemon refreshes them from ServiceNow.
- Work records returned by `get_record` use a sixty-minute TTL before refresh.
- Explicit live/fresh paths still bypass the read cache where the tool contract
  says they do.

The defaults can be overridden in the core ServiceNow config TOML, not in the
MCP authorization policy TOML:

```toml
[cache.policy]
stable_reference_ttl = "7d" # minimum 7d; longer values are allowed
work_record_ttl = "60m"
```

- **Records:** `get_record`, `search_records`, `user_lookup`, `user_search`, `business_application_get`,
  `business_application_search`, `business_application_query`, `business_application_servers`,
  `business_application_servers_cached`, `business_applications_for_server`,
  `business_application_fields`,
  `server_get`, `server_search`, `server_query`, `server_fields`, `list_records`,
  `list_my_tasks`, `list_my_approvals`, `list_my_projects`, `get_approval`, `get_children`,
  `get_work_notes`, `attachment_list`
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

## Users (read-only primitive)

User reads are cache-backed ServiceNow `sys_user` queries with a seven-day TTL.
They are **read-only** and enabled by default.

| Tool | What it does | Live API call? | Returns |
|---|---|---|---|
| `user_lookup` | Resolve one user by exact login, email, employee number, sys_id, or inferred query | On cache miss/stale | One lookup result or `-32004 user not found` |
| `user_search` | Search users by first name, last name, name substring, or email substring | On cache miss/stale | `{ "users": [<user>...] }` |

### `user_search`

- **Params:** at least one of `first_name`, `last_name`, `name_contains`, or
  `email_contains`; optional `limit` (`1`-`100`, default `20`) and `active`
  (default `true`).
- **Returns:** `{ "users": [<user>...] }`, where each `<user>` is the typed
  `UserRecord` shape returned by `snow_core`.
- **Daemon bridge:** MCP forwards this tool to daemon JSON-RPC method
  `user_search`; the bridge only advertises it when `contract_info` reports that
  method as supported.

---

## Business Applications (read-only primitive)

Business Applications (`cmdb_ci_business_app`) are a first-class local primitive
with seven **read-only** MCP tools. None mutate ServiceNow — there is **no
create/update/delete/retire surface**. All seven are **enabled by default** (they
are read tools) and are included in the `read_only_agent` role allow-list in
[`policy.example.toml`](./policy.example.toml).

| Tool | What it does | Live API call? | Persists to vault? |
|---|---|---|---|
| `business_application_get` | Fetch one Business Application by `sys_id` or exact `name` | No (serves the local cache/vault) | n/a — reads local |
| `business_application_search` | Live query `cmdb_ci_business_app` by name/owner/group/portfolio/state | Yes | Yes, by default |
| `business_application_query` | Local SQLite query/filter/sort across **all** projected BA fields, including APM `number` values | No | n/a — reads local |
| `business_application_servers` | Bounded, class-hierarchy-aware CMDB relationship traversal from one Business Application to associated Server CIs (any class extending `cmdb_ci_server`) | Yes | No — traversal-only; no persist/prune args |
| `business_application_servers_cached` | Read locally cached Server relationships for one Business Application | No | n/a — reads local |
| `business_applications_for_server` | Read locally cached Business Application relationships for one Server | No | n/a — reads local |
| `business_application_fields` | List dictionary-enriched BA field metadata merged with per-field observed counts (`refresh_dictionary` triggers a live `sys_dictionary` fetch) | Only when `refresh_dictionary=true` | n/a — reads local |

Hydration behavior (search and the daemon `*_get_fresh` path): full-row fetch (no
`sysparm_fields`, `sysparm_display_value=all`), persist to
`business_applications/business_application_<sys_id>_<slug>.md`, project all
fields into SQLite (current cache schema v11), and hydrate referenced sys_ids — owners, groups,
portfolio — into local primitive objects (or unresolved/blocked/unknown stubs).
Reference-resolution failures are **degraded reads**: the BA read still succeeds
and surfaces diagnostics rather than failing.

Routing note for agents: APM identifiers such as `APM0002456` are Business
Application numbers. Do **not** route those requests through generic
`get_record` or `search_records`. For exact APM-number lookup, call
`business_application_query` with a `number` equality filter, then read
owner-related fields from the returned BA. Use `business_application_fields` if
the owner field mapping is unclear.

### `business_application_get`

Reads a single Business Application from the local cache/vault. Schema is a strict
runtime lookup: supply **exactly one** of `sys_id` or `name`. It does not accept
APM `number` values such as `APM0002456`; use `business_application_query` for
those identifiers.

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

### `business_application_servers`

Reads Server CIs associated with one Business Application through bounded CMDB
relationship traversal. This is a live traversal read, not an inventory
persistence operation: its MCP schema intentionally exposes no `persist`,
`prune_stale`, or vault-write arguments. The MCP schema stays flat; runtime
validation enforces the selector XOR because top-level schema composition is not
allowed.

**Server detection is class-hierarchy aware.** The traversal does not rely on a
narrow three-table allowlist. Any CMDB class that extends the base `cmdb_ci_server`
table is collected as a server — the canonical `cmdb_ci_server`,
`cmdb_ci_linux_server`, and `cmdb_ci_win_server` tables, the known baseline
subclasses (`cmdb_ci_esx_server`, `cmdb_ci_aix_server`, `cmdb_ci_solaris_server`,
…), and the long tail of instance-specific subclasses recognized by the
structural `cmdb_ci_*_server` naming convention. (See
`snow_core::resource::server::is_server_class`.) Hydration then reads the matched
CIs against the base `cmdb_ci_server` table by `sys_id`, so a non-server CI that
slips through the name heuristic is simply not returned (recorded as a missing
CI) rather than corrupting results.

- **Params:** `number` (Business Application number such as `<APM_NUMBER>`) **xor**
  `sys_id` (32-hex `cmdb_ci_business_app` sys_id); optional `max_depth`
  (`1`-`4`, default `2`), `max_cis` (`1`-`5000`, default `500`), `max_edges`
  (`1`-`20000`, default `2000`),
  `max_service_membership_associations` (`1`-`20000`, default `2000`),
  `max_service_membership_pages` (`1`-`200`, default `20`),
  `relationship_type[]`, `include_paths` (default `false`), and
  `fallback_strategy` (`none` | `ci_owner_group`, default `none`). `number` must
  be a real Business Application number, not a local `BA:<sys_id>` fallback. The
  documented bounds and defaults match the `snow_core` constants
  `BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_*` /
  `BUSINESS_APPLICATION_SERVERS_MAX_*`.
- **`fallback_strategy` (CMDB-gap fallback):** opt-in, only-on-empty heuristic
  used **only** when the `cmdb_rel_ci` traversal finds **0** servers. `none`
  (default) preserves current behavior exactly — no fallback, no new response
  fields. `ci_owner_group` queries `cmdb_ci_server` by the BA's **raw
  `u_ci_owner_group`** field (NOT the empty `managed_by_group` alias) with an
  exact group-sys_id filter, bounded by the same `max_cis` budget, and returns
  the matches tagged `source: "ci_owner_group_fallback"`. The fallback never
  fires when the traversal finds one or more servers. Fallback results are
  **live-only**: they are never persisted to the durable BA↔server membership or
  inventory-health tables. The fallback surfaces the underlying CMDB
  data-quality gap via
  `relationship_summary.degraded_reasons.cmdb_relationships_unmapped` and the
  `fallback_used` / `cmdb_servers_found` / `fallback_group_*` summary fields.
- **`max_cis` semantics:** bounds the number of CIs examined **beyond** the root
  Business Application. The root BA is **excluded** from the budget, so a caller
  passing `max_cis = N` may examine up to `N` non-root CIs before traversal
  truncates (and sets `ci_limit_reached`).
- **`max_edges` semantics:** bounds the total number of `cmdb_rel_ci` relationship
  edges examined across the whole traversal. Edge reads are **paginated**
  (page size 1000) and continue across pages until the `max_edges` budget is
  consumed or the result set is exhausted, so large graphs are not silently
  undercounted by a single server-capped request. Hitting the budget with pages
  remaining sets `edge_limit_reached`.
- **Service-membership budget semantics:** `max_service_membership_associations`
  and `max_service_membership_pages` bound the `svc_ci_assoc` reader separately
  from `cmdb_rel_ci` relationship traversal. Hitting either budget is reported in
  `relationship_summary` as `service_membership_association_limit_reached` or
  `service_membership_page_limit_reached`, and does not set `edge_limit_reached`
  unless the `cmdb_rel_ci` edge budget was also exhausted.
- **`relationship_type[]`:** allowlist of CMDB relationship types that gate which
  edges are traversed. Each entry may be a `cmdb_rel_type` name or sys_id.
  - **Omitted / empty list (the default through this tool):** the default set
    (`Depends on::Used by`, `Runs on::Runs`, `Contains::Contained by`,
    `Hosted on::Hosts`, `Instantiates::Instantiated by`, and
    `Members::Member of`) is used and resolved once to **stable `cmdb_rel_type`
    sys_id identities** before traversal, so edges are matched by identity rather
    than by mutable/localizable display label. If `cmdb_rel_type` resolution
    fails or is ACL-restricted, matching falls back to the default label strings
    (no worse than prior label-only matching).
  - **Explicit non-empty list:** used verbatim; each value is matched against both
    the edge's raw relationship value and its display label.
- **`include_paths`:** when `true`, the result adds `server_paths`, a map of
  server `sys_id` → the edge chains (routes) from the root Business Application to
  that server. A server reachable via different parents (diamond topology) reports
  **multiple alternate paths**, while the `servers` array still returns **one
  result per server** regardless of how many routes reach it.
- **Returns:** `{ "business_application": <root>, "servers": [<Server>...],
  "relationship_summary": <summary>, "diagnostics": [...] }`, plus
  `server_paths` when `include_paths` is set. Server entries reuse the existing
  Server transport shape. `relationship_summary` carries the traversal accounting
  (`cis_examined`, `relationships_examined`,
  `service_membership_associations_examined`,
  `service_membership_pages_examined`, `servers_found`, and the
  `depth_limit_reached` / `ci_limit_reached` / `edge_limit_reached` /
  `service_membership_association_limit_reached` /
  `service_membership_page_limit_reached` / `truncated` flags). When
  `fallback_strategy != none`, each fallback server additionally carries a
  `source: "ci_owner_group_fallback"` field, and `relationship_summary` adds
  `cmdb_servers_found` (pre-fallback traversal count), `fallback_used`,
  `fallback_strategy`, `fallback_group_sys_id`, `fallback_group_display_name`,
  and `degraded_reasons.cmdb_relationships_unmapped`.
- **Daemon bridge:** MCP forwards this tool to daemon JSON-RPC method
  `business_application_servers`; the bridge only advertises it when
  `contract_info` reports that method as supported. Because the daemon JSON-RPC
  method defaults local persistence on for CLI workflows, the MCP bridge always
  injects `persist=false` and strips `prune_stale` for this tool.

### `business_application_servers_cached`

Reads locally cached Server relationships for one Business Application. This is
a cache-only local read: no ServiceNow API call, no live relationship traversal,
no persistence, no prune, and no vault writes.

- **Params:** exactly one of `number` (Business Application number such as
  `<APM_NUMBER>`), `sys_id` (32-hex `cmdb_ci_business_app` sys_id), or `name`
  (exact Business Application name); optional `include_tombstoned` (default
  `false`).
- **Returns:** `{ "business_application": <BA>, "servers": [...] }`, where each
  server relationship includes the cached Server, source table, `provenance`
  (`relationship` | `service_membership` | `both`), `min_depth`, path evidence,
  and optional `tombstoned_at`. The response includes `endpoint_status`
  (`cache_hit`) and `relationship_status` (`known_relationships`,
  `no_cached_relationships`, `unknown_not_synced`, or `degraded`), plus
  `inventory_health` when a persisted inventory-health marker exists. A local
  miss returns not-found with
  `endpoint_status=live_confirmation_not_attempted`.
- **Daemon bridge:** MCP forwards this tool to daemon JSON-RPC method
  `business_application_servers_cached`; the bridge only advertises it when
  `contract_info` reports that method as supported.

### `business_applications_for_server`

Reads locally cached Business Application relationships for one Server. This is
a cache-only local read: no ServiceNow API call, no live relationship traversal,
no persistence, no prune, and no vault writes.

- **Params:** exactly one of `sys_id` (32-hex Server sys_id), `name` (exact
  Server name), or `ip_address` (exact Server IP address); optional
  `include_tombstoned` (default `false`). Exact duplicate cached server names
  may return multiple matched servers.
- **Returns:** `{ "servers": [{ "server": <Server>, "business_applications": [...] }] }`.
  Each cached relationship includes the Business Application, `provenance`
  (`relationship` | `service_membership` | `both`), `min_depth`, path evidence,
  per-BA `inventory_health` when available, and optional `tombstoned_at`. The
  top-level response and each matched server include `endpoint_status` and
  `relationship_status`; a cached Server with no BA membership rows is returned
  with `relationship_status=no_cached_relationships`. A local miss returns
  not-found with `endpoint_status=live_confirmation_not_attempted`.
- **Daemon bridge:** MCP forwards this tool to daemon JSON-RPC method
  `business_applications_for_server`; the bridge only advertises it when
  `contract_info` reports that method as supported.

### Routing example

User asks: `owner for APM0002456`.

Expected route:

1. Call `business_application_query`:

   ```json
   {
     "filters": [
       { "field": "number", "op": "eq", "value": "APM0002456" }
     ],
     "limit": 1
   }
   ```

2. Read owner-related fields from the returned Business Application:
   `business_owner`, `is_owner`, `ci_owner_group`, or `primary_support_group`
   depending on the requested owner type and available fields.
3. If the owner field mapping is unclear, call `business_application_fields`
   and inspect owner/reference metadata.

Do not use `get_record` or `search_records` for this APM Business Application
number route.

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
- The daemon bridge forwards all seven to the matching daemon JSON-RPC methods and
  gates them on daemon `contract_info.supported_methods` — they are only
  advertised/callable when the attached daemon reports support. Search forwards
  the hydration options (`persist`, `resolve_references`, `reference_depth`,
  `refresh_dictionary`) to the daemon. The cached relationship tools are
  cache-only reads and do not forward persistence, prune, or vault-write
  controls. The daemon-only `business_application_get_fresh` and
  `business_application_sync` methods are **not** exposed as foreground MCP
  tools.

---

## Servers (read-only primitive)

Servers are first-class local CMDB primitives for Linux and Windows CIs. The
canonical local resource type is `server`; the dedicated server tools
(`server_get` / `server_search`) restrict to the canonical ServiceNow classes
`cmdb_ci_server`, `cmdb_ci_linux_server`, and `cmdb_ci_win_server`.

> Note: the `business_application_servers` traversal is broader — it is
> class-hierarchy aware and returns servers of **any** class extending
> `cmdb_ci_server` (including subclasses such as `cmdb_ci_esx_server`,
> `cmdb_ci_aix_server`, `cmdb_ci_solaris_server`). See
> [`business_application_servers`](#business_application_servers).

| Tool | What it does | Live API call? | Persists to vault? |
|---|---|---|---|
| `server_get` | Fetch one Server by `sys_id`, exact `name`, or exact `ip_address` (read-through cache: cache hit → cached; cache miss → live exact fetch) | On cache miss | No on MCP (forces `persist=false`); the CLI/daemon path persists |
| `server_search` | Live query Linux/Windows servers by name, IP, CI owner group, and class | Yes | Yes |
| `server_query` | Local SQLite query across projected Server records | No | n/a — reads local |
| `server_fields` | List observed Server field metadata from the local projection | No | n/a — reads local |

Hydration behavior (`server_search` and daemon `server_get_fresh`): full-row
fetch from `cmdb_ci_server` with `sys_class_name` restricted to Linux/Windows
subclasses, `sysparm_display_value=all`, no hand-picked `sysparm_fields`, then
persist to `servers/server_<sys_id>_<slug>.md` and project every readable field
into SQLite.

### `server_get`

Reads a single Server as a **read-through cache**: a cache hit returns the
cached record without any live call; a cache miss triggers a **live exact
fetch** against ServiceNow. Schema is a strict union: supply **exactly one** of
`sys_id`, `name`, or `ip_address`.

**MCP boundary — mutation-free.** On the MCP surface, `server_get` **forces
`persist = false`**: a cache-miss live fetch returns the live record but
**never** writes it to the local cache/vault (the daemon bridge injects
`persist=false`; see `daemon_bridge.rs`). The CLI/daemon `server_get` path is
different — it **does** persist the live record into the local cache. This
distinction is intentional: the MCP boundary is read/cache-write free, while the
CLI/daemon path is allowed to hydrate the cache.

- **Params:** `sys_id` (32-hex) **xor** `name` (exact match) **xor**
  `ip_address` (exact match).
- **Returns:** `{ "server": <Server>, "markdown": "<rendered markdown>" }` on a
  cache hit, and the same shape from the live record on a cache miss.
- **Error codes (consumers must treat ONLY `-32004` as not-found):**
  - `-32004` — ServiceNow-confirmed 404 (the record does not exist). This is the
    **only** not-found signal.
  - `-32003` — the record exists but is ACL-restricted to the caller.
  - `-32001` — network / timeout error reaching ServiceNow (record state
    unknown; retryable).
  - `-32005` — multiple servers matched the selector; the error payload carries
    `selector` and `matched` for disambiguation.
- **`server_get_fresh` is NOT an MCP tool.** The forced-live re-fetch path is
  reachable only via the daemon JSON-RPC method `server_get_fresh` and the CLI
  `--fresh` flag; it is never advertised or callable on the foreground MCP
  surface.

### `server_search`

Live query against `cmdb_ci_server`, restricted to Linux and Windows subclasses.
Reference filters accept either a sys_id or a display-name substring.

- **Filter params (all optional):** `name` (contains), `ip_address` (exact),
  `ci_owner_group` (display-name substring or sys_id), `class` (`linux`,
  `windows`, `cmdb_ci_linux_server`, `cmdb_ci_win_server`, or `cmdb_ci_server`),
  `limit` (`1`-`100`, default `20`).
- **Returns:** `{ "servers": [<Server>...], "records": [<record>...] }`.

### `server_query`

LOCAL SQLite query against projected Server records. Use this for cached
inventory questions such as all servers owned by a CI owner group after the
relevant records have been hydrated.

- **Params:** `text`, `name`, `ip_address`, `ci_owner_group`, `class`, `limit`
  (`1`-`500`, default `20`), `offset` (default `0`).
- **Returns:** `{ "servers": [<Server>...] }`.

### `server_fields`

- **Params:** none.
- **Returns:** `{ "fields": [{ "field": "<name>", "observed_count": <n>,
  "sample_value"?, "sample_display_value"? }...] }`.

Each `server` object contains the underlying `record`, `name`, optional
`ip_address`, `class_name`, `ci_owner_group`, `support_group`,
`operational_status`, every observed `fields` value, and `browser_url` /
`vault_relative_path` when available.

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
