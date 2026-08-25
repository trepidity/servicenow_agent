# Changelog

All notable changes to `snow` are documented here. This changelog tracks the
consumer-facing contract across the daemon JSON-RPC methods, the MCP tools, and
the `snow` CLI going forward. The format follows
[Keep a Changelog](https://keepachangelog.com/).

## [Unreleased]

### Added

- CLI `business-app servers` for BA→server CMDB traversal. Select the Business
  Application with `--number <APM_NUMBER>` or `--sys-id <BUSINESS_APP_SYS_ID>`,
  or reverse the lookup with `--for-server <SERVER_SYS_ID>`. Read the persisted
  projection without a live fetch via `--cached`. Traversal bounds are tunable
  with `--max-depth`, `--max-cis`, `--max-edges`,
  `--max-service-membership-associations`, and `--max-service-membership-pages`;
  constrain edges with the repeatable `--relationship-type`. Other flags:
  `--include-paths`, `--fallback-strategy none|ci-owner-group`, `--no-persist`,
  `--prune-stale`, `--include-tombstoned`, and `--json`.
- CLI `business-app export` (`--all`, or `--text`/`--filter`/`--limit`;
  `--format json|jsonl|csv`; `--output <PATH>`) and `business-app sync --all`
  to drain the full live inventory.
- Server primitive surfaces: CLI/RPC/MCP `server get|search|query|fields`, plus
  the daemon/CLI-only `server_get_fresh` (exposed as `--fresh`; deliberately not
  an MCP tool).
- New daemon JSON-RPC methods and MCP tools: `business_application_servers`,
  `business_application_servers_cached`, `business_applications_for_server`, and
  `server_get`.
- Approval actions now support `approval_sys_id` from `list_my_approvals` on
  `approval_approve` and `approval_reject`, allowing callers to approve or
  reject the selected `sysapproval_approver` row directly after a caller-scoped
  approval listing.
- Durable BA↔server inventory: a membership + path projection carrying
  provenance (`relationship`, `service_membership`, or `both`). A
  `ci_owner_group` fallback (`source: ci_owner_group_fallback`, live-only and
  never persisted) is used when a traversal finds 0 servers. The
  `business_application_servers` response gains a `relationship_summary` with
  `cmdb_servers_found`, `fallback_used`, `fallback_strategy`,
  `fallback_group_sys_id`, `fallback_group_display_name`, and
  `degraded_reasons.cmdb_relationships_unmapped`.
- Fixed operation/object cache policy. `cache-policy.toml` (version 1) sits in
  the daemon configuration directory and assigns each named operation a `live`,
  `read_through`, or `cache_only` rule with an optional TTL; an absent file
  means built-in defaults. It governs `business_application_get|search|query`,
  `get_article`, `search_knowledge`, `list_knowledge_articles`,
  `server_get|search|query`, `catalog_item_get`, and `catalog_items_search`.
  Wildcards and unknown operation/object names are rejected. Incident is
  deliberately not a policy object, so Incident reads cannot be made cached. Lifecycle is exposed as daemon JSON-RPC
  `cache_policy_validate` / `cache_policy_reload` and CLI
  `snow cache-policy validate|reload [--json]`. `reload` swaps the active
  snapshot atomically and reports whether the fingerprint changed. New error
  codes: `-32070` `CACHE_POLICY_INVALID`, `-32071` `CACHE_POLICY_IO`, and
  `-32072` `CACHE_MISS` (a `cache_only` rule with nothing cached).
- Live Incident read operations `incident_get` and `incident_query`, on daemon
  JSON-RPC, MCP, and CLI (`snow incident get|query`). Both are live-only: they
  never read or write the cache, vault, or index. `incident_get` takes exactly
  one `number` or `sys_id` selector and returns every native field ServiceNow
  exposed. `incident_query` takes typed filters, a fixed non-journal projection,
  ascending `sys_id` order, and an exclusive `sys_id` cursor; the limit defaults
  to 50 and caps at 200, and an exactly-full page reports `page_limit_reached`.
  Structured errors: `-32602` `INVALID_PARAMS` / `INCIDENT_STATE_UNRESOLVED`,
  `-32004` `INCIDENT_NOT_FOUND`, `-32005` `INCIDENT_NUMBER_AMBIGUOUS`, `-32007`
  `INCIDENT_LOOKUP_UNAVAILABLE`, `-32003` `ACL_DENIED`, `-32001`
  `SERVICENOW_UNAVAILABLE`, `-32000` `SERVICENOW_ERROR`.
- Separately governed finite bulk Incident update:
  `incident_bulk_plan_update` / `incident_bulk_apply_update` on daemon JSON-RPC
  and MCP, plus CLI `snow incident bulk-update`. It accepts 3 through 25
  targets (narrowed further by the new per-tool `max_targets` policy knob),
  resolves exact `number` or `sys_id` selectors live, and returns canonical
  `sys_id` order with one concurrency token per target. Apply preflights every
  target before the first PATCH, applies in order without mutation retries,
  stops on the first failure, and durably replays the exact receipt or error. A
  partial failure is neither rolled back nor automatically retried. Disabled by
  default and daemon-only.
- CLI `snow incident update` for the single-target governed Incident update,
  and `snow daemon contract-info` for the bounded daemon JSON-RPC contract
  report.

### Changed

- Local cache schema migrated to **v11** (auto-migrated forward-only on open):
  v10 added `business_application_servers`; v11 added
  `business_application_server_inventory_health`.
- `list_my_approvals` now returns pending direct approvals plus approvals routed
  to the daemon-authenticated user's direct `sys_user_group` memberships, with a
  `query_summary` describing direct/group counts and deduplication.
- Legacy daemon JSON-RPC `approve` and `reject` methods remain parseable but are
  deprecated aliases for `approval_approve` and `approval_reject`; they are no
  longer advertised as canonical `supported_methods` and share the same approval
  write policy gate.
- Knowledge MCP naming now documents `knowledge_search` and `knowledge_fetch` as
  product-facing canonical names, while `search_knowledge` and `get_article`
  remain compatibility aliases.
- Business Application, Knowledge, Server, and Service Catalog Product reads now
  resolve their cache behavior through the cache policy. The built-in defaults
  are `read_through` with a 30d TTL for Business Applications and catalog
  products, 7d for Knowledge, and 24h for Servers; anything not covered by a
  rule resolves `live`.
- Governed Incident PATCHes now go through a dedicated mutation client built
  with retries disabled, so an accepted mutation is issued at most once. A
  successful PATCH strictly removes any legacy local Incident projection —
  Incident reads stay live-only.
- `incident_apply_update` and `incident_plan_update` accept `comments` in
  addition to `assigned_to`, `assignment_group`, `state`, and `work_notes`.
- Confirmation consumption is now a single conditional `UPDATE ... WHERE
  consumed = 0 AND revoked = 0`, so a concurrent double-apply loses the race
  with `AlreadyConsumed` instead of both callers proceeding.
- Cache rebuild now derives its table scope from the cache policy rather than
  `[refresh.<resource>]` config, covering `incident`, `change_request`,
  `kb_knowledge`, `cmdb_ci_business_app`, `sc_cat_item`, and `cmdb_ci_server`,
  and pages at 1000 rows instead of 100.
- `[refresh.<resource>]` no longer has built-in defaults; entries are operator
  overrides only. The Knowledge sync still reads `filter` and
  `full_sync_interval` from `[refresh.knowledge]` when present, and otherwise
  falls back to `workflow_state=published` with no full-sync interval.

### Breaking

- **`server_get` is now a read-through cache (previously cache-only).** It used
  to return `-32004` on any cache miss. Now a cache hit returns the cached
  record, and a cache miss triggers a live exact fetch. `-32004` is returned
  ONLY on a ServiceNow-confirmed 404. New structured error codes accompany this
  change: `-32003` (ACL-restricted), `-32001` (network/timeout), and `-32005`
  (multiple-match disambiguation). On the MCP surface `server_get` forces
  `persist=false` (returns the live record, never writes the cache); the
  CLI/daemon path persists the live hit.
- **CLI filter flags removed.** `business-app query` and `business-app export`
  no longer accept `--field` plus `--contains`/`--eq`. Use a single repeatable
  `--filter <field>:<op>:<value>` flag, where `op` is `contains` or `eq`.
- **Cache format marker `snow-cache-v1` → `snow-cache-v2`.** `Store::open` now
  rejects any pre-existing v1 cache, and also rejects a v2 cache that is
  missing the new typed catalog projection tables. Every existing local cache
  must be rebuilt once.
- **MCP `catalog_items_search` and `catalog_item_get` now return an
  `OperationEnvelope`** — `{ "operation", "source", "completeness", "data" }` —
  instead of a bare `{ "items": [...] }` / `{ "item": {...} }`. Under a
  `read_through` or `cache_only` catalog rule, `catalog_items_search` serves
  the intentionally narrowed projection and reports
  `completeness.partial.reason = "narrowed_projection"`.

### Migration

- For the `server_get` change: treat ONLY `-32004` as not-found, and handle
  `-32003`/`-32001`/`-32005` distinctly. Expect previously-uncached-but-existing
  servers to now resolve via the live fallback. On the MCP surface, `server_get`
  no longer needs a manual `server_search` + filter on a miss.
- For the removed filter flags:
  - `--field X --contains Y` → `--filter X:contains:Y`
  - `--field X --eq Y` → `--filter X:eq:Y`
- For the cache format bump: run `snow rebuild-cache` once after upgrading (or
  `snow reset-cache` followed by a rebuild). Until then `snow cache-info`
  reports `incompatible (...)` and cache-backed commands refuse to open the
  database.
- For the catalog envelope: read `data.items` / `data.item` instead of the
  former top-level `items` / `item`, check `source` before treating a result as
  live, and handle `-32072` `CACHE_MISS` if the catalog object is configured
  `cache_only`.
