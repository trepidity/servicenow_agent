# ServiceNow MCP — Capabilities & Transaction Policy

The canonical reference lives next to the implementation:
**[`crates/snow_mcp/CAPABILITIES.md`](../crates/snow_mcp/CAPABILITIES.md)**.

It documents every MCP tool, which ones perform write transactions
(create/update/delete against ServiceNow), and how to enable/disable them.

## TL;DR

- **Default posture is `read_only`.** Write tools (`*_apply_*`, `*_submit_*`,
  and explicit approval/attachment/work-note actions) ship disabled or restricted
  to `test`/`training`.
- **Enable a specific method without recompiling** via a TOML policy file:

  ```bash
  export SNOW_MCP_POLICY_PATH=/etc/snow/policy.toml
  export SNOW_ENV=test
  snow_daemon
  ```

  Add a `[mcp.tools.<name>]` block with `enabled = true` and an `environments`
  gate. Copy [`crates/snow_mcp/policy.example.toml`](../crates/snow_mcp/policy.example.toml)
  to start.

- **Consuming agents** should call the runtime tools `tool_capabilities` and
  `policy_describe` at startup to learn which methods *this* deployment has
  enabled — that is the authoritative answer, not any static doc.

- **Users** are exposed through `user_lookup` for single-user exact/inferred
  resolution and `user_search` for live read-only multi-user search by
  first/last/name/email filters. `user_search` returns `{ "users": [...] }` and
  is daemon-bridge gated on daemon method `user_search`.

- **Approvals** are exposed through `list_my_approvals` as read-only pending
  approval discovery for the daemon-authenticated ServiceNow user. The tool
  returns direct `sysapproval_approver.approver == caller` rows and group-routed
  rows through direct `sys_user_grmember` memberships, keeps results under
  `records`, and adds a top-level `query_summary`. It accepts no caller-supplied
  user, approver, or impersonation override.

- **Resource Plans** are exposed through `resource_plan_list` as a read-only,
  default-enabled live query against `resource_plan`. It filters on the real
  `task` parent reference, `group_resource`/`user_resource`, and raw integer
  `state` values; response labels come from ServiceNow display values, `notes`
  comes from the `notes` field, and `query_summary.truncated` means rows equal
  the effective limit. See the canonical reference for the full schema.

- **Resource Plan writes** are governed daemon-backed plan/apply tools:
  `resource_plan_plan_create`, `resource_plan_apply_create`,
  `resource_plan_plan_update`, `resource_plan_apply_update`,
  `resource_plan_plan_decision`, and `resource_plan_apply_decision`. Create writes
  the polymorphic `task` parent field after validating `parent_type`
  (`demand`→`dmn_demand`, `project`→`pm_project`), writes caller `notes` to the
  `notes` column, omits `work_notes`, and uses `start_date`/`end_date` instead
  of `year`/`quarter`. Generic update cannot change lifecycle state; the named
  decision path maps Confirm to Confirmed plus Soft allocations and verifies
  both postconditions. Apply tools ship disabled by default, require
  confirmation, enforce update concurrency, and obey
  `SNOW_RESOURCE_PLAN_WRITE_KILL_SWITCH`.

- **Business Applications** (`cmdb_ci_business_app`) are a first-class local
  primitive exposed as seven **read-only**, **default-enabled** tools:
  `business_application_get`, `business_application_search`,
  `business_application_query`, `business_application_servers`,
  `business_application_servers_cached`, `business_applications_for_server`,
  and `business_application_fields`. None mutate ServiceNow (no
  create/update/delete surface). `search` runs a live full-row query and
  persists each result to the vault by default
  (`business_applications/business_application_<sys_id>_<slug>.md`), projecting
  all fields into the current disposable SQLite cache format and hydrating referenced
  owners/groups/portfolio into local primitives. `query` runs entirely against
  the local SQLite projection (no live call). `servers` forwards to daemon method
  `business_application_servers` for bounded read-only CMDB relationship
  traversal from one Business Application to associated Server CIs (the MCP bridge
  forces `persist=false` and strips `prune_stale`, so the traversal is live-only
  and never writes the durable membership/inventory-health tables). It accepts an
  opt-in, only-on-empty `fallback_strategy` (`none` | `ci_owner_group`, default
  `none`). `business_application_servers_cached` and
  `business_applications_for_server` are cache-only reads (no live call) that
  return cached relationships with provenance, depth, paths, and health/status
  fields.
  `fields` returns dictionary-enriched field metadata merged with observed
  counts; `refresh_dictionary=true` triggers a live `sys_dictionary` fetch
  (table + ancestors, cached in `business_application_field_dictionary`),
  falling back to observed-only plus a degraded diagnostic when the dictionary is
  unreachable. The daemon-only `business_application_sync` method (live
  search+persist with a roll-up summary) and `business_application_get_fresh`
  are JSON-RPC + CLI only and are **deliberately not exposed as MCP tools**. See
  the canonical reference's *Business Applications (read-only primitive)*
  section for exact params, the `<BA>` return shape (`record`, typed
  references, `fields`, `unresolved_references`, `browser_url`,
  `vault_relative_path`), and the daemon-bridge contract gating.

See the canonical reference for the full tool tables, the policy TOML schema, and
the runtime introspection response shapes.

---

## `server_get` (read-through cache)

`server_get` is a read-only Server lookup that is now a **read-through cache**,
not a cache-only read:

- **Cache hit:** returns the locally cached/vault Server record without any live
  query.
- **Cache miss:** performs a **live exact fetch** from `cmdb_ci_server` for the
  supplied selector. On MCP this fetch **forces `persist=false`** — the live
  record is returned to the caller but is **never written to the local cache**.
  (The daemon bridge also injects `persist=false` for this tool, mirroring the
  in-process behavior.)
- **Selector:** supply **exactly one** of `sys_id` (32-hex), exact `name`, or
  exact `ip_address`. Supplying zero or more than one is an invalid-params error.

### Error codes (live fallback)

The live fallback maps structured failures onto distinct JSON-RPC codes; do not
treat every miss as not-found:

| Code | Meaning |
|---|---|
| `-32004` | **Not found** — returned **only** on a confirmed ServiceNow 404 |
| `-32003` | Server is **ACL-restricted** |
| `-32001` | **Network / timeout** reaching ServiceNow |
| `-32005` | **Disambiguation** — the selector matched multiple Server CIs |

### `server_get_fresh` is not an MCP tool

The forced-live variant `server_get_fresh` is exposed via **daemon JSON-RPC and
the CLI `--fresh` flag only**. It is **not** advertised or callable as an MCP
tool — MCP callers use `server_get` (read-through, `persist=false`) instead.

---

## `business_application_servers` (live traversal + CMDB-gap fallback)

Bounded, class-hierarchy-aware CMDB relationship traversal from one Business
Application to associated Server CIs. On MCP it is **live-only**: the bridge
**rejects** `persist` / `prune_stale` arguments and **forces `persist=false`**,
so the traversal never writes the durable BA↔server membership or
inventory-health tables.

### `fallback_strategy`

Opt-in, only-on-empty heuristic, used **only** when the `cmdb_rel_ci` traversal
finds **0** servers:

- **`none`** (default) — no fallback; preserves prior behavior exactly and emits
  no new response fields.
- **`ci_owner_group`** — queries `cmdb_ci_server` by the BA's raw
  `u_ci_owner_group` group sys_id (bounded by the same `max_cis` budget) and
  returns the matches. Fallback results are **live-only** and are **never
  persisted** to the durable membership or inventory-health tables. Each fallback
  server carries `source: "ci_owner_group_fallback"`.

### `relationship_summary` additions (when `fallback_strategy != none`)

- `cmdb_servers_found` — pre-fallback traversal server count.
- `fallback_used` — whether the fallback fired.
- `fallback_strategy` — the strategy that ran.
- `fallback_group_sys_id` / `fallback_group_display_name` — the CI owner group
  used for the fallback query.
- `degraded_reasons.cmdb_relationships_unmapped` — surfaces the underlying CMDB
  data-quality gap (relationships unmapped, fell back to owner group).

---

## Cached BA↔Server relationship tools

`business_application_servers_cached` and `business_applications_for_server` are
**cache-only local reads**: no ServiceNow API call, no live traversal, no
persistence, no prune, no vault writes.

- **`business_application_servers_cached`** — cached Server relationships for one
  Business Application. Selector: exactly one of `number`, `sys_id`, or exact
  `name`; optional `include_tombstoned` (default `false`).
- **`business_applications_for_server`** — cached Business Application
  relationships for one Server. Selector: exactly one of `sys_id`, exact `name`,
  or exact `ip_address`; optional `include_tombstoned`.

Each cached relationship carries:

- **Provenance** — `relationship`, `service_membership`, or `both`.
- **`min_depth`** — minimum traversal depth at which the pair was observed.
- **Path evidence** — the cached edge chains backing the relationship.
- **`tombstoned_at`** — timestamp when a previously-cached pair was tombstoned
  (present only for tombstoned rows; surfaced when `include_tombstoned=true`).

Status / health fields:

- **`endpoint_status`** — `cache_hit` on a local hit; a local miss returns
  not-found with `endpoint_status=live_confirmation_not_attempted`.
- **`relationship_status`** — `known_relationships`, `no_cached_relationships`,
  `unknown_not_synced`, or `degraded`.
- **`inventory_health`** — per-BA inventory-health marker, included when a
  persisted marker exists.

---

## Local cache projection

The local SQLite projection is a disposable, exact current-format cache. It is
never upgraded in place. Cache replacement is deliberately absent from the MCP
and daemon JSON-RPC surfaces because those processes hold the database open.
With the daemon stopped, use `snow rebuild-cache` to reconstruct the configured
ACL-readable projection from terminal live ServiceNow pages, `snow reset-cache`
to create an empty projection, or `snow import-cache-from-vault` for an explicit
markdown restore. Live rebuild and reset do not read or modify the vault. The
current format includes two tables backing the cached relationship tools:

- **`business_application_servers`** — durable BA↔Server membership rows.
- **`business_application_server_inventory_health`** — per-BA inventory-health
  markers.

These tables back the cached relationship tools above. Note that the
`server_get` live fallback and the `business_application_servers` traversal do
**not** write these tables when invoked through MCP (`persist=false` is forced);
they are populated by the daemon/CLI persistence paths.
