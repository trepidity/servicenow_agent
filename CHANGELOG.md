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
- Durable BA↔server inventory: a membership + path projection carrying
  provenance (`relationship`, `service_membership`, or `both`). A
  `ci_owner_group` fallback (`source: ci_owner_group_fallback`, live-only and
  never persisted) is used when a traversal finds 0 servers. The
  `business_application_servers` response gains a `relationship_summary` with
  `cmdb_servers_found`, `fallback_used`, `fallback_strategy`,
  `fallback_group_sys_id`, `fallback_group_display_name`, and
  `degraded_reasons.cmdb_relationships_unmapped`.

### Changed

- Local cache schema migrated to **v11** (auto-migrated forward-only on open):
  v10 added `business_application_servers`; v11 added
  `business_application_server_inventory_health`.

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

### Migration

- For the `server_get` change: treat ONLY `-32004` as not-found, and handle
  `-32003`/`-32001`/`-32005` distinctly. Expect previously-uncached-but-existing
  servers to now resolve via the live fallback. On the MCP surface, `server_get`
  no longer needs a manual `server_search` + filter on a miss.
- For the removed filter flags:
  - `--field X --contains Y` → `--filter X:contains:Y`
  - `--field X --eq Y` → `--filter X:eq:Y`
