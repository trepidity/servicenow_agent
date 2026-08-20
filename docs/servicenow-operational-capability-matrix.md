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
| Work-record migration | Current `refresh.resources` work defaults, offline rebuild population, live persistence, TTL reads, and search/list cache paths must be reconciled together. |
| Stale | Age since last successful live refresh exceeds the object policy TTL. |
| Miss or stale | Live query and refresh unless explicit cache-only policy applies. |
| Response truth | Every response states source and completeness; cached responses include last refresh; narrowed projection is partial. |
| Mutation | Invalidate after success unless ServiceNow supplies the complete replacement record; bulk invalidates known targets or the object segment. |
| Policy source | Daemon-owned source path captured at startup; validate/reload accept no caller path. |
| Policy lifecycle | Validate makes no state change; reload fully parses then atomically swaps; failure retains the prior snapshot. |
| Background work | Offline rebuild obeys the same policy; daemon `refresh_all` remains unavailable unless separately approved. |

## Governed mutation catalog

| Family | Status | Contract |
|---|---|---|
| Assignment and state changes for common work records | Selected | Typed plan/apply, explicit enablement, confirmation, receipt/audit, ServiceNow-permitted fields. |
| Work notes and comments | Selected | Governed write with shared denial, idempotency, and receipt/audit behavior. |
| Approve/reject | Selected | Governed write with explicit environment enablement. |
| Create common work records | Selected | Typed per-resource create; no generic create. |
| Service Catalog request planning/submission | Selected | `catalog_plan_request` / `catalog_submit_request`; distinct from generic work-record creation. |
| Knowledge article lifecycle | Selected | Typed create, edit, publish, and retire. |
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
2026-08-20, but T-OPS-02 remains decision-blocked because its policy wire and
lifecycle contract is not defined. T-OPS-03 remains blocked because the
approved Incident row does not define get/query request contracts. T-OPS-04
also lacks the required bulk-operation request and outcome contract. The exact
blockers are B-OPS-06 through B-OPS-08 in the implementation spec. Every
selected family additionally requires a B-OPS-01 row amendment that names exact
ServiceNow mapping, source anchors, operation names, transports, cache status,
applicable finite bulk maximum, and L0 evidence. Until that row is approved,
Selected is direction only and grants no coder or runtime authority.

The first two family rows were approved on 2026-08-20: Incidents as the T-OPS-03
read row, and Incident assignment/state update as the T-OPS-04 write row. Both
are specified in
`docs/spec-servicenow-operational-capabilities.md#approved-b-ops-01-rows` and
remain blocked until their missing public contracts and named prerequisites are
approved. Every other family is still row-gated.
