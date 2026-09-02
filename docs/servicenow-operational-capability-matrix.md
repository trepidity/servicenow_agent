# ServiceNow operational capability matrix

## Purpose

This is the decision record for Snow's consumer-neutral ServiceNow operations.
It does not authorize ServiceNow access, encode a downstream consumer, or
replace ServiceNow ACLs. The canonical contract is
`docs/spec-servicenow-operational-capabilities.md#approved-goal`.

`scripts/mcp_schema_smoke.py --attest-matrix` may consume a separate
machine-readable transport inventory. That inventory attests exposure only; it
does not choose capability, policy, authorization, or installed readiness.

## Status vocabulary

| Status | Meaning |
|---|---|
| Selected | Approved direction; requires its dependency and L0 gates before implementation or readiness claims. |
| Shipped | Verified in current source; not necessarily enabled, authorized, transport-complete, or installed. |
| Retained compatibility | Existing bounded surface may remain but may not expand under this program. |
| Retained legacy | Existing transport-specific capability may remain unchanged but is not selected for new implementation. |
| Remove advertisement | Shipped registration/doc claim has no callable implementation and must be removed, not implemented. |
| Deferred | No implementation authority. |

## Universal decisions

| Decision | Contract |
|---|---|
| Resource boundary | Separate typed operations per selected family; no new generic table, field, or raw-query surface. |
| Transport | Every selected operation targets CLI, daemon JSON-RPC, direct MCP, and daemon-backed MCP unless a row explicitly names a transport exception. Each path is independently proven. |
| Access | The configured ServiceNow identity and ServiceNow ACLs decide record, group, target, field, and update access. Snow adds no authorization narrowing. |
| Cache projection | Projection may be intentionally narrower for offline use, but it never narrows live ACL access or supports a complete-live claim. |
| Record body | Snow owns source/completeness/paging/cache/receipt envelopes; ServiceNow field names and raw/display values remain native. Missing fields are omitted. |
| Discovery | Expose only metadata the configured ServiceNow API actually returns; unsupported metadata is unavailable/omitted, never guessed. |
| Error | Pass through ServiceNow diagnostics after secret redaction. |
| Pagination | Use ServiceNow pagination only where provided; never fabricate it. |
| Policy evidence | Exposure, daemon policy, bridge policy, ServiceNow ACL, and installed evidence are separate claims. |
| Knowledge rebuild scope | Knowledge is rebuilt only when `[rebuild.knowledge] knowledge_base_sys_id` names one exact 32-hex base. Omission performs zero `kb_knowledge` I/O; no display-name, wildcard, or raw-query scope is accepted. |

## Current shipped collision inventory

| Surface | Current source state | Decision |
|---|---|---|
| `attachment_list` | Shipped CLI, daemon RPC, direct MCP, and daemon-backed MCP read | Selected attachment read; add to read catalog. |
| `attachment_upload` | Shipped CLI and daemon RPC; direct MCP declares daemon-required; daemon-backed MCP maps to daemon; disabled by default | Retained legacy, transport-specific. No new transport or behavior. |
| `catalog_cancel_request` | Registered/documented/classified as write but has no direct handler, daemon method, or bridge mapping | Remove advertisement and policy/doc entries. Cancellation remains deferred. |
| `get_record` | Shipped allowlisted number or table/sys_id compatibility read | Retained compatibility; no table/prefix expansion. |
| `list_records` | Shipped local projection read with typed resource aliases; omitted resource can span cached records | Retained compatibility; source/completeness required; no new generic filters. |
| `search_records` | Shipped local free-text search with exact-number live hydration fallback | Retained compatibility; cache policy controls persistence; no raw query. |
| `contract_info` | Shipped daemon method inventory | Reuse as exposure baseline; it does not prove metadata, policy, or transport parity. |

## Read capability catalog

| Family | Status | Source contract |
|---|---|---|
| Work queues and details: personal work, approvals, group queues, Incidents, Incident Tasks, Changes, Change Tasks, Stories, Story Tasks, Problems, Problem Tasks, Catalog Requests, Requested Items, and Catalog Tasks | Selected | Live-only unless named cache policy explicitly enables the object; omission performs no cache I/O. |
| Delivery: Projects, Demands, Demand Tasks, Resource Plans, and Time Cards | Selected | Live-only unless named cache policy explicitly enables the object. |
| Record attachments | Selected | `attachment_list`; live read, no upload authority. |
| Knowledge | Selected | Cache-eligible, seven-day default freshness. |
| Business Applications | Selected | Cache-eligible, thirty-day default freshness. |
| Servers | Selected | Cache-eligible, twenty-four-hour default freshness. |
| Service Catalog product records | Selected | Cache-eligible, thirty-day default freshness; distinct from request/item/task records. |
| Configuration Items, CI relationships, Hardware Assets, Software Assets, Customer Service Cases, HR Cases, Security Incidents, Events, Alerts, Users, and Groups | Selected | Live-only unless named cache policy explicitly enables the object. |
| `get_record`, `list_records`, `search_records` | Retained compatibility | Existing allowlists/semantics only; source/completeness mandatory; no expansion. |
| Custom or unimplemented tables | Deferred | Metadata-only until a typed operation is separately approved. |
| Aggregate/report queries and ServiceNow record history | Deferred | No server-side aggregate/report or history surface. |

## Cache policy decisions

| Rule | Contract |
|---|---|
| Owner | Daemon policy keyed by named operation/object type; callers do not select source. |
| Omission | Live-only with zero cache/vault/index read or persistence for that operation. |
| Default eligible objects | Knowledge, Business Applications, Servers, and Service Catalog products only. |
| Fixed source | `<resolved Snow config directory>/cache-policy.toml`; missing uses built-in defaults, while an existing unreadable or invalid file fails closed. No command accepts a path. |
| Schema | Strict version-1 TOML with typed object defaults and exact operation overrides; unknown fields, wildcards, duplicates, unknown keys, and operation/object mismatches are invalid. |
| Modes | `live` performs zero local I/O; `read_through` refreshes miss/stale and never silently falls back stale; `cache_only` performs no ServiceNow I/O and may return stale data with mandatory refresh age. |
| TTL | Cached modes require `1m` through `365d` using `s`, `m`, `h`, or `d`; `live` forbids TTL. |
| Work-record migration | Current `refresh.resources` work defaults, offline rebuild population, live persistence, TTL reads, and search/list cache paths must be reconciled together. |
| Stale | Age since last successful live refresh exceeds the object policy TTL. |
| Miss or stale | Live query and refresh unless explicit cache-only policy applies. |
| Response truth | Every response states source and completeness; cached responses include last refresh; narrowed projection is partial. |
| Catalog projection | Complete cached catalog get uses a typed current-format `CatalogItem` projection including variables and choices. Generic `sc_cat_item` rows are never complete; narrowed rebuild/search results are `Partial/NarrowedProjection`. Existing caches reset/rebuild with no migration. |
| Catalog transport parity | Direct and daemon-backed MCP use the same policy-aware envelope/error path and return byte-identical structured payloads for the same fixture; no Mullet dependency is introduced. |
| Mutation | Invalidate after success unless ServiceNow supplies the complete replacement record; bulk invalidates known targets or the object segment. |
| Policy lifecycle | `snow cache-policy validate/reload` call `cache_policy_validate/reload` with `{}`; validate makes no state change, reload atomically swaps, and failure retains the prior snapshot. |
| Invalidation | Complete returned records replace exact projections; otherwise known targets are removed across memory/SQLite/vault/index, and an incompletely identified bulk mutation invalidates only its exact object segment. |
| Background work | Offline rebuild obeys the same policy; daemon `refresh_all` remains unavailable unless separately approved. |

## Governed mutation catalog

| Family | Status | Contract |
|---|---|---|
| Assignment and state changes for common work records | Selected | Typed plan/apply, explicit enablement, confirmation, receipt/audit, ServiceNow-permitted fields. |
| Work notes and comments | Selected | Governed write with shared denial, idempotency, and receipt/audit behavior. |
| Approve/reject | Selected | Governed write with explicit environment enablement. |
| Create common work records | Selected | Typed per-resource create; no generic create. |
| Service Catalog request planning/submission | Selected | `catalog_plan_request` / `catalog_submit_request`; distinct from generic work-record creation. |
| Knowledge article lifecycle | In progress | Governed typed draft creation is shipped (`knowledge_plan_create_draft` / `knowledge_apply_create_draft`); edit, publish, and retire remain deferred. |
| Resource Plans and Time Cards | Selected | Typed create/update operations. |
| Attachment upload | Retained legacy | Existing CLI/daemon/daemon-backed MCP behavior only; disabled by default; no expansion. |
| Catalog cancellation | Remove advertisement | Remove `catalog_cancel_request` registration/policy/docs; do not implement. |
| Other cancellation and deletion | Deferred | No ServiceNow cancel/delete surface. |
| CI and asset mutation | Deferred | No create/update lifecycle or ownership surface. |
| User, Group, membership, or role mutation | Deferred | No identity-administration surface. |

## Mutation and bulk decisions

| Rule | Contract |
|---|---|
| Default posture | Every mutation is disabled in every environment until explicit named-tool/environment policy enables it. |
| Default target limit | Every transport allows at most two mutation targets for the ordinary operation. |
| Named bulk | More than two requires a separate bulk operation, explicit environment enablement, and a finite row-approved `max_targets`; omission denies bulk. |
| CLI interaction | Preview and confirmation are default. `--apply --yes` bypasses interaction only; it never bypasses policy, operation classification, or `max_targets`. |
| Audit/receipt | Enabled by default for an enabled mutation; retention is declared by the row-specific write handoff. |
| Incident bulk | `incident_bulk_plan_update` / `incident_bulk_apply_update` accept 3..=25 canonical targets, shared plus non-overlapping per-target patches, and exact per-target `sys_updated_on` tokens. |
| Bulk failure | Preflight is all-or-none. PATCH execution is non-atomic, ordered by `sys_id`, and stops on first failure; applied/failed/not-attempted outcomes are durable and no rollback or automatic write retry is claimed. |
| Local coherence | A successful upstream write followed by invalidation failure is visible `PARTIAL_FAILURE` / `LOCAL_COHERENCE_FAILED` with `upstream_applied: true`, never ordinary success. |
| Daemon background work | Cannot bypass source, cache, mutation, target, or policy contracts. |

## Dispatch rule

T-OPS-05 inventory closure is complete. T-OPS-01 closed on 2026-08-20: the
shared descriptor/envelope shapes ship and `incident_fields` proves transport
parity across compiled CLI, daemon JSON-RPC, direct MCP, and a real
daemon-backed bridge call. Its field lists are dictionary-derived structural
candidates, not record ACL authorization, and inherited choices follow the
ServiceNow table hierarchy.
The upstream Unicode parser gate in
`docs/spec-cache-rebuild-progress.md#verification-and-closure` passed on
2026-08-20. B-OPS-06 also closed on 2026-08-20 with the fixed-source,
strict-schema cache-policy contract. T-OPS-02 source closure passed on
2026-08-20 through compiled CLI and daemon L0 seams for policy lifecycle,
defaults, modes, cache age/completeness, rebuild, and invalidation, plus real
local-socket direct/daemon-backed MCP process parity for the complete and
narrowed typed catalog projections and exact error paths. The 2026-08-21
installed-scale amendment requires one exact Knowledge-base sys_id for rebuild,
performs zero Knowledge I/O when omitted, and passes compiled-CLI durable-cache,
daemon policy, and mutation evidence. The post-T-OPS-04
aggregate source gates passed on 2026-08-21: 816 workspace tests passed with
zero failures and six intentional ignores, strict Clippy and formatting passed,
and the local MCP schema smoke reported 83 tools and three resources. Installed
daemon/live ServiceNow attestation remains separately operator-owned and is not
an implementation or Mullet dependency. B-OPS-07 closed on 2026-08-20
with exact live-only Incident get/query selectors, filters, projection,
exclusive cursor, result, error, transport-parity, and L0 contracts. T-OPS-03
source/fake closure passed on 2026-08-20 through compiled CLI, daemon JSON-RPC,
direct MCP, and real-socket daemon-backed MCP evidence; Incident cache-policy
entries are rejected. B-OPS-08
closed on 2026-08-20 with
the compatible single-target, separately governed 3..=25 target bulk,
concurrency, confirmation, durable partial-failure, local-coherence,
receipt/audit, transport, and L0 contracts. T-OPS-04 source/local-fake closure
passed on 2026-08-20 for the compatible single-target and separately named bulk
families across compiled CLI, daemon JSON-RPC, direct MCP, and real-socket
daemon-backed MCP. Aggregate workspace/Snow-owned E2E passed on 2026-08-21;
installed/live attestation remains separate. Every selected family additionally requires a B-OPS-01 row
amendment that names exact ServiceNow mapping, source anchors, operation names,
transports, cache status,
applicable finite bulk maximum, and L0 evidence. Until that row is approved,
Selected is direction only and grants no coder or runtime authority.

The first two family rows were approved on 2026-08-20: Incidents as the T-OPS-03
read row, and Incident assignment/state update as the T-OPS-04 write row. Both
are specified in
`docs/spec-servicenow-operational-capabilities.md#approved-b-ops-01-rows`.
The Incident read and write rows are source-complete. Every other family is
still row-gated.
