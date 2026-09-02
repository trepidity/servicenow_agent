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
| Catalog request (`sc_req_item`) | `catalog_submit_request` | **Create** | ❌ | yes | requires KB evidence; explicit environment policy required |
| Approval (`sysapproval_approver`) | `approval_approve` | **Update** | ❌ | yes | prefer `approval_sys_id` from `list_my_approvals`; target record number still accepted; daemon required |
| Approval (`sysapproval_approver`) | `approval_reject` | **Update** | ❌ | yes | prefer `approval_sys_id` from `list_my_approvals` plus reason; target record number still accepted; daemon required |
| Work note / journal | `work_note_apply_add` | **Create** | ❌ | yes | `work_notes` only; explicit environment policy required |
| Knowledge article (`kb_knowledge`) | `knowledge_apply_create_draft` | **Create draft** | ❌ | yes | governed; daemon required; accepts title, HTML body, required knowledge-base sys_id, optional category sys_id; fresh refetch must prove `workflow_state=draft`; no publish tool exists |
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
| Incident (`incident`) | `incident_apply_update` | **Update** | ❌ | yes | compatible single-target operation; daemon required; `assigned_to`,`assignment_group`,`state`,`work_notes`,`comments`; concurrency checked |
| Incident (`incident`) | `incident_bulk_apply_update` | **Update 3..=25** | ❌ | yes | separately governed daemon operation; explicit `max_targets`; ordered, stop-first, no rollback/retry |
| Resource plan (`resource_plan`) | `resource_plan_apply_create` | **Create** | ❌ | yes | governed; daemon required; writes `task`,`group_resource` or `user_resource`,`resource_type`,`state`,`planned_hours`,`notes`,`start_date`,`end_date` |
| Resource plan (`resource_plan`) | `resource_plan_apply_update` | **Update** | ❌ | yes | governed; daemon required; concurrency checked; updates `planned_hours`,`notes`,`start_date`,`end_date` only |
| Resource plan (`resource_plan`) | `resource_plan_apply_decision` | **Confirm / Confirm and Allocate** | ❌ | yes | governed; daemon required; Requested precondition; verifies resulting state and allocation booking type |
| MCP operation plan | `plan_cancel` | **Delete** (cancel) | ❌ | yes | cancels a pending plan, not a SN record |

`*_plan_*` tools (`story_plan_create`, `story_plan_update`, `story_task_plan_create`,
`story_task_plan_update`, `change_request_plan_create`, `change_request_plan_update`,
`change_task_plan_create`, `change_task_plan_update`,
`incident_plan_update`, `incident_bulk_plan_update`,
`resource_plan_plan_create`, `resource_plan_plan_update`, `resource_plan_plan_decision`,
`timecard_plan_set_hours`, `work_note_plan_add`, `knowledge_plan_create_draft`,
`catalog_plan_request`) are **not** transactions — they
build/preview a plan and never mutate ServiceNow. The matching `*_apply_*` /
`*_submit_*` tool executes the plan.

**Enforcement at runtime:**
- Default posture is `read_only` (`default_mode`, `policy.rs:495`).
- The daemon bridge rejects all non-governed write tools — `-32040 policy denied` (`daemon_bridge.rs`).
- Governed Story, Change, Resource Plan, and time-card writes need an attached daemon, else `-32044 DAEMON_REQUIRED_FOR_WRITE` (`server.rs`).
- A disabled tool returns `-32040 policy denied`.

### Knowledge draft creation

`knowledge_plan_create_draft` and `knowledge_apply_create_draft` are the
draft-only Knowledge write surface. The planner accepts exactly
`short_description`, `text`, `knowledge_base_sys_id`, and an optional
`category_sys_id`; it rejects `workflow_state`, publication fields, and every
other field. The apply tool requires the plan-issued confirmation token and
idempotency key, writes `workflow_state=draft`, then performs a fresh
`kb_knowledge` read. If ServiceNow reports any workflow state other than
`draft`, the operation fails rather than claiming a draft was created.

The preview, audit summary, and receipt expose a SHA-256 of the article body,
not the body itself. This capability deliberately does **not** provide edit,
publish, retire, delete, or generic Knowledge-record mutation.

### Resource Plan Writes

`resource_plan_plan_create`, `resource_plan_apply_create`,
`resource_plan_plan_update`, `resource_plan_apply_update`,
`resource_plan_plan_decision`, and `resource_plan_apply_decision` are governed
daemon-backed writes for the `resource_plan` table.

- **Parent model:** create sets the single polymorphic `task` field from
  `parent_sys_id` after the daemon reads the parent and verifies
  `parent_type = demand` maps to `dmn_demand` or `parent_type = project` maps
  to `pm_project`. There is no writable `parent` or `parent_table` column.
- **Resource model:** create accepts `resource_type = group|user` and
  `resource_sys_id`, then writes `group_resource` or `user_resource` plus
  `resource_type`. Update does not allow changing `task`, resource assignment,
  or `resource_type`.
- **State model:** generic update no longer accepts `state`; lifecycle decisions
  use `resource_plan_plan_decision` with `decision = confirm|confirm_and_allocate`.
  The daemon requires Requested (`2`), maps Confirm to Confirmed (`11`) with
  Soft allocations and Confirm and Allocate to Allocated (`3`) with Hard
  allocations, then verifies both the refreshed record and allocation rows.
- **Notes:** caller `notes` maps to the ServiceNow `notes` column. `work_notes`
  is intentionally absent from schemas and allowlists because Resource Plan
  journal writes no-op in ServiceNow.
- **Period and hours:** `start_date` and `end_date` are date strings. Generic
  writes treat `planned_hours` as a direct scalar; the named decision operation
  verifies allocation child rows created by ServiceNow's lifecycle processing.
- **Concurrency:** update apply requires the plan-issued concurrency token
  (`sys_updated_on` plus optional `sys_mod_count`). A missing/mismatched caller
  token returns `CONCURRENCY_TOKEN_INVALID` (`-32061`); a changed record returns
  `CONCURRENCY_CONFLICT` (`-32053`).
- **Kill switch:** `SNOW_RESOURCE_PLAN_WRITE_KILL_SWITCH=1|true|yes` denies
  every Resource Plan apply with `KILL_SWITCH` (`-32057`).
- **ACLs:** the daemon service account needs create/update ACLs on
  `resource_plan`, read ACLs on `dmn_demand` and `pm_project` for parent
  validation, and read access to `sys_user_group` / `sys_user` if deployment
  policy validates resources outside the write payload. A 401/403 write failure
  is surfaced as `UPSTREAM_PERMISSION_DENIED` (`-32063`).

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
  `server_get`, `server_search`, `server_query`, `server_fields`, `list_records`, `record_query`,
  `list_my_tasks`, `list_my_approvals`, `list_my_projects`, `get_approval`, `get_children`,
  `get_work_notes`, `attachment_list`, `resource_plan_list`,
  `incident_list_by_assignment_group`, `incident_assignment_groups`,
  `incident_assignment_group_queue`, `incident_get`, `incident_query`, `incident_fields`

`list_my_approvals` is read-only and returns pending direct approvals plus
pending approvals routed to direct `sys_user_group` memberships for the
daemon-authenticated ServiceNow user. It reads `sys_user_grmember` to resolve
those memberships, accepts no caller-supplied user or approver override, keeps
the approval collection under `records`, and includes a top-level
`query_summary` on successful responses.

### `record_query`

`record_query` is the strict live query primitive for Change Requests and
Stories. It accepts only `resource_type: change_request|story`, typed
resource-specific filters, an optional 32-hex exclusive cursor, and a page
limit from 1 through 200 (default 50). Unknown fields, raw encoded queries,
cross-resource filters, malformed ranges, and unsupported Story description
searches fail with `-32602` before record I/O.

- Change filters: assignment group, assignee, exact live-resolved state, and
  strict start-date bounds.
- Story filters: assignment group, assignee, story owner, lead developer,
  exact live-resolved states, sprint, project, CI, blocked flag, strict due or
  update bounds, Story numbers, and `short_description` text only.
- Pages are ordered by `sys_id` ascending and return
  `{ records, next_cursor, complete, source: "live", limit, rows_inspected }`.
  A full page is incomplete; an exact-multiple scan requires a final empty page.
- The direct and daemon-backed MCP transports expose the same schema/payload.
  The bridge advertises the tool only when `contract_info.supported_methods`
  contains `record_query`.
- The operation is ephemeral and never writes the cache or vault. Legacy
  `list_records` rejects unknown properties, including the obsolete `filter`
  argument, instead of silently returning an unfiltered list.

### `resource_plan_list`

`resource_plan_list` is a read-only, default-enabled live query against
`resource_plan`. It issues one Table API query to `resource_plan`; when
`parent_number` is supplied, it may first resolve the parent number to a sys_id
on `dmn_demand` or `pm_project`, then the resource-plan query filters
`task=<sys_id>`.

- **Params:** all optional. `parent_number` xor `task_sys_id`; `resource_sys_id`
  requires `resource_type` (`group` or `user`); `state` is one raw integer or an
  array of raw integers; `limit` defaults to `50` and clamps at `200` with a
  `LIMIT_CLAMPED` warning.
- **Filters:** parent filters use the `task` field. Group resources filter
  `group_resource`; user resources filter `user_resource`; `resource_type`
  alone filters the raw `resource_type` choice. Multiple states use a
  builder-generated `stateIN...` query.
- **Response:** `{ records, query_summary }`. Each record includes optional
  `browser_url`, optional `vault_relative_path`, `parent` from `task.number` and
  `task.sys_class_name` dot-walk fields, optional `resource`, raw `state`,
  `state_label` from ServiceNow's `state.display_value`, `planned_hours`,
  `notes` from the `notes` field, and `context` from `u_description`.
- **Truncation:** `query_summary.truncated` is `true` when returned rows equal
  the effective limit.
- **ACL:** requires read access to `resource_plan`; parent number/table labels
  depend on read access to the `task` dot-walk fields.
### `incident_list_by_assignment_group`

`incident_list_by_assignment_group` is a read-only, default-enabled live query
against `incident`. It issues exactly one Table API query per call and returns
one cursor page of *active* Incidents for a single assignment group.

- **Params:** `assignment_group_sys_id` is required and must be a 32-hex
  `sys_user_group` sys_id — group names are not accepted. `state` is optional
  and exact: either a raw ServiceNow value (`3`) or an exact case-insensitive
  choice label (`Pending`); substring and fuzzy matching are not supported.
  `limit` defaults to `50` and is rejected above `200` — it is not clamped.
  `cursor` is the previous page's `next_cursor`.
- **Filters:** `assignment_group=<sys_id>` plus `active=true`, ordered by
  `sys_id` ascending. A resolved `state` adds `state=<raw value>`. A cursor adds
  `sys_id><cursor>`, so paging is exclusive.
- **Paging:** `limit` bounds the ServiceNow rows *requested*, reported back as
  `rows_inspected`. Returned `records` may be fewer, because rows that are
  terminal or `active=false` are rejected locally. `next_cursor` is the last row
  ServiceNow returned — not the last surviving record — so a page rejected in
  full still advances. `complete` is `true` only when ServiceNow returned fewer
  rows than `limit`.
- **Response:** `{ records, next_cursor, complete, limit, rows_inspected, state }`,
  where `state` echoes the resolved `{ value, label }` when a selector was given.
- **State correction:** an unknown or ambiguous `state` fails with `-32602` and
  `data` carrying `field`, `requested`, `ambiguous`, and the live `choices`, so a
  caller can correct the selector without a second round trip.
- **Persistence:** none. Unlike `list_my_tasks`-style reads, this operation
  writes nothing to the cache, vault, or search index; the projection is
  ephemeral.
- **Consistency:** the scan is ordered but not transactional. An Incident
  reassigned or re-stated mid-scan may be missed or repeated. The response is a
  page, never a point-in-time inventory.
- **ACL:** authorization is ServiceNow's alone. The tool applies no
  assignment-group allowlist or other scope narrowing, so it returns exactly
  what the daemon credential is permitted to read.

### Incident assignment-group operations

- `incident_assignment_groups` discovers the authenticated user's active,
  direct memberships and returns exact names plus sys_ids.
- `incident_assignment_group_queue` requires one of those exact names or
  sys_ids. It provides unassigned/assignee, priority, state, age/update,
  staleness, and SLA filters; deterministic priority/opened/updated/assignee/SLA
  sorting; caller, CI, service, impact, urgency, hold, latest-activity, and SLA
  context; and state/priority/assignee/SLA, unassigned, and stale counts.
- Queue scans are bounded (`scan_limit` defaults to 2,000 and is capped at
  5,000). `scan_complete`, response `complete`, and aggregate `complete` remain
  false when that bound prevents exhaustive results.
- Delta polling passes the prior `watermark` as `updated_since` and the prior
  row ids as `known_sys_ids`. Reassigned, inactive, terminal, deleted, or
  unreadable baseline records appear in `departed_sys_ids`.
- `incident_plan_update` previews one exact Incident update for `assigned_to`,
  `assignment_group`, `state`, `work_notes`, or `comments`.
  `incident_apply_update` requires its plan's confirmation, idempotency, and
  concurrency token; it is disabled by default and requires the daemon.
- `incident_bulk_plan_update` accepts 3 through the narrower configured
  `max_targets` (never above 25), resolves exact `number` or `sys_id` selectors
  live, and returns canonical `sys_id` order with one concurrency token per
  target. `incident_bulk_apply_update` requires the saved plan's exact
  confirmation, idempotency key, and canonical token array. It preflights every
  target before the first PATCH, applies in order without mutation retries,
  stops on the first failure, and returns or durably replays the exact public-safe
  receipt/error. A partial failure is not rolled back or automatically retried.
- Successful Incident PATCHes strictly remove any legacy local projection;
  Incident reads remain live-only. ServiceNow ACLs remain the record/field
  authority; Snow adds no membership or target-scope allowlist.
- `SNOW_INCIDENT_WRITE_KILL_SWITCH=1|true|yes` or the global
  `SNOW_MCP_WRITE_KILL_SWITCH` denies Incident apply operations.

- **Knowledge:** `search_knowledge`, `knowledge_search`, `kb_semantic_search`, `get_article`,
  `knowledge_fetch`, `knowledge_answer`, `knowledge_grounded_plan`, `list_knowledge_bases`,
  `list_categories`, `list_knowledge_articles`, `vault_path`, `kb_status`,
  `kb_semantic_status`, `kb_list_tags`, `verify_vault`

For product-facing MCP calls, prefer `knowledge_search` and `knowledge_fetch`.
`search_knowledge` and `get_article` remain compatibility aliases for daemon-era
callers and map to the same daemon methods.
- **Catalog / plans:** `catalog_items_search`, `catalog_item_get`, `catalog_plan_request`,
  `resource_plan_get`, `story_get`, `story_tasks_list`, `timecard_list`,
  `timecard_plan_set_hours`, `change_request_plan_create`, `change_request_plan_update`,
  `change_task_plan_create`, `change_task_plan_update`, `plan_get`
- **Governance / audit:** `policy_describe`, `tool_capabilities`, `redaction_rules_describe`,
  `audit_event_get`, `audit_events_search`, `audit_chain_verify`

> Local-cache writers (`kb_sync`, `kb_semantic_rebuild`, `repair_vault`)
> write only to the local KB vault/cache — **never** to ServiceNow.

Cache replacement is offline-only and is not exposed as an MCP tool. Stop the
daemon and use the CLI `rebuild-cache`, `reset-cache`, or explicit
`import-cache-from-vault` operation.

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
fields into the current disposable SQLite cache format, and hydrate referenced sys_ids — owners, groups,
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
The name predates the `_cached` suffix used by `business_application_servers_cached`;
treat this reverse lookup as cache-only despite the asymmetric name.

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
| `incident_fields` | Discover Incident field candidates, dictionary-declared writable candidates, choices, references, and paging from ServiceNow `sys_dictionary` / `sys_choice` | Yes | No — metadata is not cache-eligible |

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

### `incident_fields`

- **Params:** none. The `incident` table is fixed by the operation; there is no
  caller-supplied table, because that would make this a generic table browser.
- **Returns:** an `OperationEnvelope` —
  `{ "operation": "incident_fields", "source": { "kind": "live" },
  "completeness": { "kind": "complete" }, "data": <ResourceDescriptor> }`.

`data` carries `resource_type`, `table`, `readable_fields`, `writable_fields`,
and `paging`. Each field category is either
`{ "status": "available", "value": [<FieldDescriptor>...] }` or
`{ "status": "unavailable", "reason": "not_returned_by_instance" | "acl_denied"
| "not_supported_by_operation" }`.

`available` with an empty list and `unavailable` are **different facts** and are
never interchangeable: the first means the instance reported no fields, the
second means Snow could not find out. An ACL denial on `sys_dictionary` is
reported as `acl_denied` rather than as an empty descriptor or an error.

These lists are structural metadata, not an authorization oracle.
`readable_fields` contains fields visible to live dictionary discovery and
`writable_fields` is its subset not marked `read_only` by that dictionary.
Record-level read/write ACLs and Snow's governed-write policy are separate
runtime decisions; consumers must not infer authorization from either list.
Choice discovery follows the table hierarchy so choices defined on an ancestor
such as `task` are not lost when the child table has no local `sys_choice` row.

Each `FieldDescriptor` carries the native ServiceNow `name`, optional `label`,
native `kind` (the dictionary `internal_type`), optional `reference_table`, and
`choices` in the same `FieldSupport` shape. Choices are fetched only for fields
the dictionary flags as choice fields; every other field reports
`not_supported_by_operation`.

### `incident_get` / `incident_query`

`incident_get` performs a live-only exact lookup by one `number` or `sys_id`
selector. It returns a complete `OperationEnvelope` containing every native
field ServiceNow exposed, with raw and optional display values preserved.

`incident_query` performs a live-only bounded query with typed Incident
filters, a fixed non-journal projection, ascending `sys_id` order, and an
exclusive `sys_id` cursor. The page limit defaults to 50 and is capped at 200.
Exactly a full page reports `page_limit_reached`; callers page until a shorter,
possibly empty, complete page. Neither operation reads or writes Snow's cache,
vault, or index.

`paging` reports native support only:
`{ "mode": "cursor", "default_limit": 50, "max_limit": 200 }` for Incidents.
Snow never fabricates pagination an operation does not have.

Nothing here is read from a bundled schema or inferred from a display name — a
field the instance does not return is omitted rather than guessed.

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
  `skip_terminal_records`, `story_board_id`, and (for Incident bulk plan/apply)
  `max_targets` in the inclusive range 3 through 25.
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
