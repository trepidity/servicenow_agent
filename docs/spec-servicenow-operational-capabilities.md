# Implementation Spec: ServiceNow operational capability contract

## Authority and scope

### Approved goal

Define Snow as a consumer-neutral ServiceNow operations agent. It exposes
separate typed capabilities for common ServiceNow resource families, with
equivalent consumer semantics through CLI, daemon JSON-RPC, direct MCP, and
daemon-backed MCP unless this contract explicitly classifies a retained surface
as transport-specific.

This contract distinguishes four states: **Selected** is approved future
behavior, **Shipped** is source-verified current behavior, **Retained legacy** is
an existing compatibility surface that may not expand, and **Deferred** is not
authorized for implementation. Capability exposure, operator policy, ServiceNow
ACL access, and installed-runtime proof remain separate claims.

### Implementation readiness

| Task | Current state | Dispatch decision |
|---|---|---|
| T-OPS-05 runtime inventory reconciliation | Closed: ghost advertisement removed and CLI/RPC/direct-MCP/bridge inventory probes pass | COMPLETE |
| T-OPS-01 typed-operation foundation | Closed: shared descriptor/envelope shipped and `incident_fields` proves parity across all four transports | COMPLETE |
| T-OPS-02 cache-policy foundation | Goal and defaults are approved, but the policy wire/lifecycle contract is absent | BLOCKED by B-OPS-06 |
| T-OPS-03 resource reads | Incident family/table are approved, but get/query request contracts are absent | BLOCKED by B-OPS-07 |
| T-OPS-04 governed writes | Single-target precedent exists, but the required bulk request/outcome contract is absent | BLOCKED by T-OPS-03 and B-OPS-08 |
| FEAT-OPS-CONTRACT production closure | Not installed-runtime proven | BLOCKED until selected row gates and operator evidence pass |

“Selected” never means implemented, enabled, authorized, or production-ready.
No blocked task above is a coder handoff. They authorize no inferred API shape
and no live ServiceNow operation.

### Governing authority

- `AGENTS.md#behavioral-test-seams` — consumer seams and evidence altitude.
- `AGENTS.md#public-safe-content` — public-safe tracked artifacts.
- `docs/spec-servicenow-operational-capabilities.md#approved-goal` — direct user
  decisions recorded by this contract.
- `docs/servicenow-operational-capability-matrix.md#universal-decisions` —
  canonical classification, cache, mutation, and compatibility decisions.
- `docs/spec-incident-list-by-assignment-group.md#scope` — current live, typed,
  ephemeral group-Incident operation and its parity precedent.
- `crates/snow_mcp/CAPABILITIES.md#read-only-tools` — shipped MCP documentation;
  implementation evidence only, not authority to widen this contract.

### Scope

- Provide separate typed read operations for common supported ServiceNow
  families: work records and queues, Change Requests, Change Tasks, Stories,
  Story Tasks, Projects, Demands, Demand Tasks, Resource Plans, Time Cards,
  Incidents, Incident Tasks, Problems, Problem Tasks, Catalog Requests,
  Requested Items, Catalog Tasks, Service Catalog product records, Knowledge,
  Business Applications, Servers, Configuration Items, CI relationships,
  Hardware Assets, Software Assets, Customer Service Cases, HR Cases, Security
  Incidents, Events, Alerts, Users, Groups, and record attachments.
- Include the configured ServiceNow identity's work, approvals, memberships,
  and any group the identity may read. ServiceNow ACLs are the access boundary;
  Snow adds no membership, group, or target-access authorization restriction.
- Treat cache projection scope separately from access scope. An operator may
  intentionally project a subset for offline use, but a narrowed projection
  must never cause a live operation to omit an ACL-visible record or claim a
  complete live result.
- Discover ServiceNow-supported fields, filters, sorting, native pagination,
  field metadata, choices, references, and permitted updates per typed resource.
  Unsupported metadata is reported as unavailable or omitted; Snow never
  guesses support. Snow adds no new generic table, field, or raw encoded-query
  endpoint.
- Use a hybrid response: Snow owns the operation envelope, paging, source,
  completeness, cache freshness, and governed-write receipt; records retain
  ServiceNow field names and raw/display values. Missing ServiceNow fields are
  omitted rather than synthesized as empty.
- Expose source metadata on every response. Cached responses include their last
  successful live-refresh time; incomplete cached projections are explicitly
  partial.
- Cache only Knowledge, Business Applications, Servers, and Service Catalog
  product records by default. Cache policy is keyed by named operation/object
  type, never caller-selected. Policy omission is live-only and performs no
  cache read or persistence. A configured cache miss or stale entry refreshes
  live unless the explicit policy is cache-only.
- Define default freshness as Knowledge seven days, Business Applications
  thirty days, Service Catalog product records thirty days, and Servers
  twenty-four hours. Successful mutation invalidates known entries or the
  affected object segment unless ServiceNow returns the complete current record.
- Replace the current broad `refresh.resources` work-record defaults and
  work-record TTL read path. Offline `snow rebuild-cache` and live read
  persistence obey the same operation/object cache policy; daemon
  `refresh_all` remains unavailable unless separately approved.
- Support selected governed writes: assignment/state changes, work
  notes/comments, approval/rejection, typed creation of common work records,
  Service Catalog request planning/submission, Knowledge article lifecycle,
  and Resource Plan/Time Card creation or update.
- Governed mutation tools are disabled by default in every environment,
  including production. A deployer must explicitly enable a named tool for a
  named environment. Policy may restrict or disable an approved typed contract
  but may never create or widen one.
- Every mutation transport has a default maximum of two targets. More than two
  requires a separately named bulk operation, explicit environment policy, and
  a finite per-family `max_targets` declared in the row-specific handoff. A
  missing finite maximum denies bulk. CLI bulk additionally requires preview
  and confirmation; `--apply --yes` may bypass interaction but never policy,
  the named bulk boundary, or `max_targets`.
- Cache policy uses a daemon-owned source path captured at startup. Fixed
  validate and reload commands accept no caller-supplied path: validate parses
  the configured source without state change; reload parses fully and atomically
  swaps only the cache-policy snapshot. Failure preserves the prior snapshot.
- Pass through ServiceNow failures after redacting credentials, tokens, and
  other secrets. Do not replace upstream diagnostics with generic Snow errors.

### Retained compatibility surfaces

- `get_record`, `list_records`, and `search_records` are retained compatibility
  reads. Their current allowlisted resource/prefix model may not expand.
  `get_record` must not accept a table outside
  `RECORD_LOOKUP_ALLOWED_TABLES`; `list_records` remains a local projection read;
  `search_records` remains local free-text search with its existing exact-number
  live fallback. Every response must state source/completeness, and cache policy
  must prevent unapproved work-record persistence.
- `attachment_upload` is a retained legacy, transport-specific mutation through
  CLI, daemon JSON-RPC, and daemon-backed MCP. It remains disabled by default,
  requires confirmation and idempotency, and may not be added to direct MCP or
  expanded under this program. Retention is not approval for new attachment
  behavior.
- `attachment_list` is a selected read family and is not deferred.
- `catalog_cancel_request` is an advertised ghost: it has registration/policy
  documentation but no callable daemon or bridge implementation. Because
  cancellation is deferred, T-OPS-05 removes the advertisement and policy/doc
  entries; implementing cancellation is forbidden.

### Non-goals

- No new generic ServiceNow table browser, raw encoded-query endpoint,
  arbitrary field update endpoint, custom-table read/write operation, or
  expansion of retained compatibility table/prefix allowlists.
- No Snow authorization narrower than the configured ServiceNow identity's
  ACLs. Operation shape, cache projection, confirmation, bulk limits, and policy
  are safety/completeness controls, not substitute authorization.
- No default cache for ordinary work records or any non-approved object type.
- No server-side aggregate/report operation, ServiceNow record-history lookup,
  ServiceNow cancellation/deletion, new attachment-upload capability,
  User/Group/membership/role mutation, or CI/asset mutation.
- No downstream-consumer coupling, named-user assumptions, deployment-specific
  configuration, live payloads, credentials, identifiers, URLs, or policy
  values in tracked artifacts.

## System progression and dependency map

| Node ID | Node type | Authority refs | Phase | Wave | Provides | Consumes | Hard prerequisites | Closure gate |
|---|---|---|---|---|---|---|---|---|
| FND-OPS-000 | Foundation | `docs/spec-servicenow-operational-capabilities.md#retained-compatibility-surfaces`; `AGENTS.md#behavioral-test-seams` | Phase 1 | Wave 1 | Truthful shipped/legacy/deferred transport inventory and ghost-surface removal | Compiled CLI, daemon contract, direct MCP and bridge inventories | Foundation root | Independent L0 inventory rejects advertised-unreachable tools and proves every classified surface's exact transport state |
| FND-OPS-001 | Foundation | `docs/spec-servicenow-operational-capabilities.md#scope`; `AGENTS.md#behavioral-test-seams` | Phase 1 | Wave 2 | Shared typed operation descriptor, metadata discovery, hybrid envelope, and transport parity seam | Existing `contract_info`, daemon dispatcher, MCP server/bridge, CLI composition | FND-OPS-000 closure | L0 CLI, JSON-RPC, direct MCP, and bridged MCP expectations prove identical selected semantics and native raw/display fields |
| FND-OPS-002 | Foundation | `docs/spec-servicenow-operational-capabilities.md#scope`; `crates/snow_core/src/config.rs#cacheconfig`; `docs/spec-cache-rebuild-progress.md#verification-and-closure` | Phase 1 | Wave 2 after dependency closure | Per-operation cache policy, source/completeness envelope, invalidation, validate-only, and atomic reload | Existing cache/vault/query stores, offline rebuild, daemon configuration | FND-OPS-000 closure; B-OPS-05 closure | L0 cache hit/miss/stale/mutation and reload tests prove no work-record cache I/O by default and prior-policy retention after invalid reload |
| CAP-OPS-READ | Capability | `docs/spec-servicenow-operational-capabilities.md#scope`; `docs/spec-incident-list-by-assignment-group.md#scope` | Phase 2 | Row-specific wave | Separate typed live read/query operations and field metadata | FND-OPS-001; FND-OPS-002 for cache-eligible rows | Foundation closures plus approved B-OPS-01 row | Each selected row has independent transport L0 proof, native pagination where available, and truthful source/completeness |
| CAP-OPS-WRITE | Capability | `docs/spec-servicenow-operational-capabilities.md#scope`; `AGENTS.md#behavioral-test-seams` | Phase 3 | Row-specific wave | Fail-closed governed writes, receipt/audit, two-target default, and finite named bulk | Foundations; target CAP-OPS-READ row | Foundation closures plus approved B-OPS-01 write row and target read closure | L0 plan/apply proves explicit enablement, confirmation, receipt/audit, target limits, replay/concurrency, and no-I/O denial |
| FEAT-OPS-CONTRACT | Feature | `docs/spec-servicenow-operational-capabilities.md#approved-goal`; `AGENTS.md#public-safe-content` | Phase 4 | Final wave | Public-safe discoverable operational contract and bounded readiness statement | CAP-OPS-READ; CAP-OPS-WRITE; operator evidence | Selected capability closures and installed-runtime evidence | Source, policy, transport, ACL, and installed claims are separately evidenced; no selected row is overstated |

Hard `requires` edges override desired delivery order and parallelism. Direct
MCP does not prove daemon-backed MCP, source does not prove installed runtime,
and registry presence does not prove a callable operation.

## Traceability matrix

| Authority ref | Behavior / decision | Task ID | Implementation seam | Acceptance evidence | Owner |
|---|---|---|---|---|---|
| `docs/spec-servicenow-operational-capabilities.md#retained-compatibility-surfaces`; `AGENTS.md#behavioral-test-seams` | Reconcile shipped transports, retain bounded legacy attachment upload, select attachment list, and remove the unimplemented catalog-cancel advertisement | T-OPS-05 | CLI help/commands; daemon `contract_info` and dispatcher; MCP registries/dispatchers/bridge; capability docs | Red-first L0 inventory fails on current ghost/mismatch state, then proves exact classified inventory and no unknown-route result for any advertised operation | Rust coder + QA |
| `docs/spec-servicenow-operational-capabilities.md#scope`; `crates/snow_daemon/src/rpc/handlers/system.rs#contract_info` | Discover selected capability metadata without hardcoded field guesses | T-OPS-01 | `snow_core` resource descriptor; daemon metadata dispatch; CLI and MCP exposure | Independent CLI, JSON-RPC, direct MCP, and bridge fixtures assert discovered fields, choices, references, paging support, and raw/display representation | Rust coder + QA |
| `docs/spec-servicenow-operational-capabilities.md#scope`; `crates/snow_core/src/config.rs#cacheconfig` | Replace broad work-record population/read behavior with named cache policy and atomic policy lifecycle | T-OPS-02 | Config/policy, cache rebuild, live persistence, record reads/search, daemon policy holder and fixed CLI commands | Compiled CLI/daemon with local ServiceNow fake proves approved defaults, no work-record cache I/O, source/completeness, miss/stale behavior, invalidation, and failed reload retention | Rust coder + QA |
| `docs/spec-servicenow-operational-capabilities.md#scope`; approved B-OPS-01 row | Deliver one typed read family without local access narrowing | T-OPS-03 | Row-named resource/core/daemon/CLI/MCP/bridge modules | L0 fake proves ACL-visible records, absent missing fields, native paging, truthful completeness, and no unapproved cache I/O | Rust coder + QA |
| `docs/spec-servicenow-operational-capabilities.md#scope`; approved B-OPS-01 write row | Deliver one fail-closed governed write family with finite target limits | T-OPS-04 | Row-named planner/applier, daemon, CLI, MCP policy/bridge, receipt/audit modules | Red-first L0 fake proves default denial, explicit enablement, confirmation, finite target denial, replay/concurrency, receipt/audit, redaction, and zero I/O on every guard failure | Rust coder + QA |

## Implementation boundary

### Handoff manifest

| Field | Value |
|---|---|
| Immutable base | `622a9258d959afea035ce75e4c52ba888d7d8db0` |
| Ready first | T-OPS-05 (closed) |
| Source complete | T-OPS-05 and T-OPS-01 |
| Decision-blocked | T-OPS-02 by B-OPS-06; T-OPS-03 by B-OPS-07; T-OPS-04 by T-OPS-03 and B-OPS-08 |
| Row-gated | Every family beyond the two approved Incident rows remains row-gated. |
| Operator-owned | Live/installed evidence and deployment policy enablement |
| Scope rule | A changed production path outside the active task scope blocks handoff until this manifest is amended |

### Allowed changes

- T-OPS-05: `crates/snow_cli/src/cli.rs` and compiled CLI inventory tests;
  `crates/snow_daemon/src/rpc/{method.rs,handlers/system.rs}` and daemon inventory
  tests; `crates/snow_mcp/src/{tools/catalog.rs,tools/attachment.rs,domain/policy.rs,planner/mod.rs,server.rs,daemon_bridge.rs}`;
  `crates/snow_mcp/{CAPABILITIES.md,policy.example.toml}`; focused direct/bridge
  MCP process tests; this spec and the capability matrix. Only ghost removal,
  exact legacy classification, and inventory proof are permitted.
- T-OPS-01: `crates/snow_core/src/{types.rs,resource/,service/}`;
  `crates/snow_daemon/src/rpc/{method.rs,handlers/}`;
  `crates/snow_mcp/src/{tools/,server.rs,daemon_bridge.rs}`;
  `crates/snow_cli/src/{cli.rs,daemon_cmd/,app/}`; and focused L0 transport tests.
- T-OPS-02: `crates/snow_core/src/{cache/,config.rs,context.rs,facade.rs,query/,service/cache_rebuild.rs,service/record.rs,service/knowledge.rs,service/business_application/,service/server.rs}`;
  daemon cache-policy state/handlers; fixed CLI validate/reload commands; public-safe
  config examples; and focused L0 cache/policy tests.
- T-OPS-03: only the resource/core/daemon/CLI/MCP/bridge paths and L0 tests named
  in the approved B-OPS-01 row amendment.
- T-OPS-04: only the planner/applier, daemon/CLI/MCP policy/bridge, receipt/audit,
  and L0 paths named in the approved B-OPS-01 write-row amendment.

### Forbidden changes / non-goals

- Implementing catalog cancellation or any deferred write while removing its
  false advertisement.
- Expanding retained compatibility table/prefix allowlists or attachment-upload
  transports/behavior.
- Caller-selectable cache mode, work-record caching by omission, hidden stale
  fallback, or a narrowed cache projection represented as complete live data.
- Default-enabled mutation tools, unbounded bulk, or `--apply --yes` bypassing
  operation policy or target limits.
- A policy reload accepting a caller path, partially activating configuration,
  or replacing a valid active snapshot after validation failure.

**No-invention declaration:** Implement only the cited behavior inside this
boundary. A missing execution path, architecture, API, persistence flow,
provider behavior, or product solution is a blocker requiring an approved
authority update, not a coder decision.

## Decision gaps and blockers

- **B-OPS-01 — row-specific resource contract: FIRST ROWS APPROVED 2026-08-20.**
  The read row is Incidents and the write row is Incident assignment/state
  update, both specified in `#approved-b-ops-01-rows`. B-OPS-01 remains open for
  every other family: no additional row is authorized, and the coder may not
  infer a table or maximum from a display name.
- **B-OPS-03 — live operator evidence:** agents use local fakes. Installed/live
  execution is operator-owned and may prove runtime availability, never source
  completion or authorization by itself.
- **B-OPS-05 — upstream Unicode cache dependency: CLOSED 2026-08-20.** The
  `servicenow_rs` journal parser now uses checked UTF-8 slicing, the fix is
  published as immutable revision `ad9a52acebcc5d68270e4d9124fb5ccf103bb838` on
  `origin main`, and this workspace pins that revision in `Cargo.toml` and
  `Cargo.lock`. The compiled
  `rebuild_cache_projects_unicode_work_notes_without_panicking` regression
  passes and `CARGO_INCREMENTAL=0 cargo test --workspace --all-features
  --no-fail-fast` reports 733 passed / 0 failed / 6 ignored. The parser was
  corrected upstream rather than caught, skipped, vendored, or relabelled.

B-OPS-02 is resolved by fail-closed discovery: expose only metadata the configured
ServiceNow API actually returns and report unsupported categories truthfully.
B-OPS-04 is resolved by the fixed-path daemon-owned cache-policy lifecycle in
`#scope`; audit/receipt retention belongs to each T-OPS-04 write row, not cache
policy. Neither resolution authorizes a live call.

- **B-OPS-06 — cache-policy wire and lifecycle contract:** approval is still
  required for the fixed source filename/location, serialized policy schema,
  operation/object key namespace, live/read-through/cache-only modes, TTL
  validation, validate/reload CLI and daemon method names, result/error shapes,
  and object-segment invalidation rules. The approved defaults and atomicity
  prose do not determine those public contracts. T-OPS-02 is not dispatchable
  until this decision is recorded.
- **B-OPS-07 — Incident get/query contract:** approval is still required for
  `incident_get` selectors, `incident_query` filter and sort allowlists, exact
  cursor/request/result shapes, field projection behavior, and transport error
  mapping. Naming the table and operations does not determine those behaviors.
  T-OPS-03 is not dispatchable until this decision is recorded.
- **B-OPS-08 — Incident bulk-update contract:** approval is still required for
  the `incident_bulk_apply_update` target and shared/per-target patch shape,
  plan/apply relationship, atomic versus partial-failure semantics, concurrency
  tokens, confirmation binding, and receipt/audit result. The approved limit of
  25 does not determine those behaviors. T-OPS-04 is not dispatchable until
  B-OPS-08 and its read prerequisite close.

### Approved B-OPS-01 rows

Approved 2026-08-20. These two rows, and only these two, select the T-OPS-03 and
T-OPS-04 families. They do not unblock coding until B-OPS-07/B-OPS-08 define
their missing public contracts. Every other selected family stays row-gated.

#### Read row — Incidents (T-OPS-03)

| Field | Value |
|---|---|
| Family | Incidents |
| ServiceNow table | `incident` |
| Method | Table API GET |
| Source anchor | `crates/snow_core/src/resource/incident.rs`; `crates/snow_daemon/src/rpc/handlers/incidents.rs`; `crates/snow_mcp/src/tools/records.rs` |
| Operation names | `incident_get`, `incident_query`, `incident_fields` |
| Shipped precedent, unchanged | `incident_list_by_assignment_group`, `incident_assignment_groups`, `incident_assignment_group_queue` |
| Transports | CLI, daemon JSON-RPC, direct MCP, daemon-backed MCP — no exception |
| Cache status | Live-only. Not a default-eligible object. Zero cache/vault/index read or persistence. |
| Pagination | `Cursor { default_limit: 50, max_limit: 200 }`, matching `docs/spec-incident-list-by-assignment-group.md#scope`. The cursor is the `sys_id` of the last row ServiceNow returned, exclusive on the next request. |
| Bulk maximum | Not applicable to a read |

L0 evidence: ACL-visible records returned without Snow-side narrowing; a field
absent upstream is absent from `data` rather than present-and-empty; native
cursor paging only; `Partial { PageLimitReached }` when the limit truncates; and
a negative proof that `incident_query` against a populated local cache performs
zero cache/vault/index I/O.

This row does not authorize expanding `RECORD_LOOKUP_ALLOWED_TABLES`, adding an
`incident` cache-policy entry, or any incident write.

#### Write row — Incident assignment/state update (T-OPS-04)

| Field | Value |
|---|---|
| Family | Incident assignment and state update |
| ServiceNow table | `incident` |
| Method | Table API PATCH |
| Source anchor | `crates/snow_daemon/src/change_write.rs`; `crates/snow_mcp/src/domain/policy.rs#toolpolicy` |
| Operation names | `incident_plan_update`, `incident_apply_update` |
| Target read row | The Incidents read row above; this row does not open until that row closes |
| Transports | CLI, daemon JSON-RPC, direct MCP, daemon-backed MCP |
| Permitted fields | `assigned_to`, `assignment_group`, `state`, `work_notes`, `comments`. All others denied. |
| Default posture | Disabled in every environment including production |
| Enablement | Explicit named tool plus named environment, through `tool_enabled_in_environment` |
| Default target limit | 2 |
| Named bulk | `incident_bulk_apply_update`, `max_targets: 25`, separately enabled. Omission denies bulk. |
| Cancellation | Excluded; `reject_cancel_state` stands |
| Concurrency | `sys_updated_on` token; a stale token denies the apply |

`max_targets` is net-new. The shipped `ToolPolicy::max_records: Option<u64>` is a
different control and must not be reused as the target bound: its `Option` shape
makes a missing maximum permissive, which is the opposite of the required deny.

L0 evidence, red-first, every denial proving zero ServiceNow write I/O: default
denial; denial when enabled without the named environment; denial of a field
outside the permitted list; denial at three targets without named bulk and at
twenty-six with it; `--apply --yes` bypassing only interaction; idempotent replay
returning the prior receipt without re-writing; stale concurrency token denying
and preserving prior state; and redacted pass-through of upstream failures.

Receipts are retained locally, public-safe, and carry no record bodies.

### T-OPS-01 metadata, envelope, and operation contract

Approved 2026-08-20. This closes the shape gap that `#no-invention-declaration`
otherwise makes undispatchable.

Metadata is exposed through per-family named operations — `incident_fields`,
`business_application_fields`, `server_fields` — over one shared descriptor type.
There is no generic `resource_metadata(table)` entry point and no caller-supplied
table parameter.

`ResourceDescriptor` carries `resource_type`, the fixed `table`, `readable_fields`,
`writable_fields`, and `paging`. Each `FieldDescriptor` carries the native
ServiceNow `name`, an optional `label`, the native `kind`, an optional
`reference_table`, and `choices`.

The field-list names describe metadata candidates, not effective record ACLs.
`readable_fields` contains fields visible through the configured identity's
live `sys_dictionary` discovery. `writable_fields` is the subset the live
dictionary does not mark `read_only`. Neither list attests that ServiceNow will
authorize a read or write on a particular record; record ACLs and Snow's
governed-operation policy remain separate runtime facts. Choice discovery walks
the table hierarchy from the selected table through its ancestors.

Field categories use `FieldSupport<T>`, which is either `Available(T)` or
`Unavailable { reason }` where the reason is `NotReturnedByInstance`,
`AclDenied`, or `NotSupportedByOperation`. `Available(vec![])` and `Unavailable`
are distinct states and are never interchangeable; a field the instance does not
return is omitted from its vector rather than emitted empty.

`PagingSupport` is either `Cursor { default_limit, max_limit }` or `None`.

Every selected operation returns `OperationEnvelope<T>` carrying the named
`operation`, a `source`, a `completeness`, and native `data`. `Source` is `Live`
or `Cache { last_refreshed_at }` — the timestamp is mandatory, so a cached
response cannot be constructed without it. `Completeness` is `Complete` or
`Partial { reason }` where the reason is `NarrowedProjection`, `PageLimitReached`,
or `UpstreamTruncated`. The envelope is Snow-owned; no transport may rename,
flatten, case-convert, or default anything inside `data`.

The parity proof resource is Incidents via `incident_fields`, chosen because it
is live-only and therefore independent of FND-OPS-002.

## Scaffold inventory

| Seam ID | Authority refs | Crate / file / symbol | Signature / contract | Safe unresolved state | Owner | Completion evidence |
|---|---|---|---|---|---|---|
| SC-OPS-INVENTORY | `docs/spec-servicenow-operational-capabilities.md#retained-compatibility-surfaces`; `AGENTS.md#behavioral-test-seams` | Compiled CLI, daemon contract/dispatcher, direct MCP, bridge MCP | Independently authored expected inventory names each operation, transport, classification, and callable-route expectation | Mismatch is blocked; never infer callable from registration or docs | Rust QA | L0 inventory and route probes fail on the original ghost and pass after reconciliation |
| SC-OPS-METADATA | `docs/spec-servicenow-operational-capabilities.md#scope`; `crates/snow_core/src/resource/record_query.rs` | Typed resource descriptor | Returns only dictionary-visible field candidates, dictionary-declared non-read-only candidates, hierarchy-aware choices, references, and native paging support; never represents either list as record ACL authorization | Typed unavailable/blocked field category; never guessed success | Rust core coder | L0 CLI/RPC/MCP expectations against independent local metadata fixtures |
| SC-OPS-ENVELOPE | `docs/spec-servicenow-operational-capabilities.md#scope`; `crates/snow_daemon/src/rpc/handlers/records.rs` | Shared transport response adapter | Stable source/completeness/cache-age envelope preserving ServiceNow-native record values | Transport error or explicit partial/unavailable; never synthetic empty data | Rust daemon/MCP coder | Independent consumer-visible transport expectations |
| SC-OPS-CACHE-POLICY | `docs/spec-servicenow-operational-capabilities.md#scope`; `crates/snow_core/src/cache/policy.rs` | Named operation/object policy plus daemon atomic snapshot | `enabled`, TTL, and miss behavior; omitted means live-only/no cache I/O; fixed-source validate and reload | Validation error and prior active snapshot retained | Rust core/daemon coder | Compiled CLI/daemon L0 cache and invalid-reload tests |
| SC-OPS-WRITE-GUARD | `docs/spec-servicenow-operational-capabilities.md#scope`; `crates/snow_mcp/src/domain/policy.rs#toolpolicy` | Shared governed-write enablement and target counter | Disabled by default; two targets maximum unless named bulk policy supplies row-approved finite maximum; CLI flags cannot bypass | Policy refusal before ServiceNow I/O and no synthetic receipt | Rust write/MCP coder | Red-first L0 fake asserts exact denial, no I/O, and receipt/audit outcomes |

## Task breakdown and coder handoffs

### T-OPS-05: Reconcile shipped inventory before adding capability

- System node: FND-OPS-000 runtime inventory foundation.
- Phase / Wave: Phase 1 / Wave 1.
- Hard prerequisites: Foundation root.
- Provides / consumes: consumes current CLI/RPC/MCP registration and dispatch;
  provides truthful selected/legacy/deferred inventory and removes the catalog
  cancellation ghost without implementing cancellation.
- Closure gate: independent L0 inventory proves exact transport classification;
  every advertised operation reaches a declared route outcome rather than an
  unknown-tool/method result; static documentation checks remain supplemental.
- Authority refs: `docs/spec-servicenow-operational-capabilities.md#retained-compatibility-surfaces`;
  `AGENTS.md#behavioral-test-seams`.
- Allowed write scope: exactly the T-OPS-05 paths in `#allowed-changes`.
- Acceptance evidence: red-first consumer inventory fails because
  `catalog_cancel_request` is advertised without a route; after removal it proves
  `attachment_list` selected, attachment upload retained only on its declared
  transports, compatibility reads retained/bounded, and deferred surfaces absent.
- Coder rule: Implement only the cited behavior. Surface every uncited path or solution as a blocker requiring an approved authority update.

### T-OPS-01: Establish the transport-neutral typed-operation foundation

- System node: FND-OPS-001 typed-operation foundation.
- Phase / Wave: Phase 1 / Wave 2.
- Hard prerequisites: FND-OPS-000 closure.
- Provides / consumes: provides metadata discovery, hybrid envelopes, and parity
  adapters; consumes existing `contract_info`, typed resource modules, and all
  four declared consumer transports.
- Closure gate: one existing selected resource proves identical discovered
  metadata, native record body, source, and completeness through CLI, RPC,
  direct MCP, and bridged MCP.
- Authority refs: `docs/spec-servicenow-operational-capabilities.md#scope`;
  `AGENTS.md#behavioral-test-seams`.
- Allowed write scope: exactly the T-OPS-01 paths in `#allowed-changes`; no new
  resource family.
- Acceptance evidence: red-first L0 expectations fail when one transport omits
  a discovered field, changes raw/display values, fabricates metadata, or
  reports a different envelope; schema smoke remains green.
- Coder rule: Implement only the cited behavior. Surface every uncited path or solution as a blocker requiring an approved authority update.

### T-OPS-02: Replace broad cache behavior with named operation policy

- System node: FND-OPS-002 cache-policy foundation.
- Phase / Wave: Phase 1 / Wave 2.
- Hard prerequisites: FND-OPS-000 closure and B-OPS-05 upstream cache-parser
  closure.
- Provides / consumes: provides operation/object cache selection, truthful
  source/completeness, approved TTLs, invalidation, and fixed-source atomic
  lifecycle; consumes current cache/vault/query stores and offline rebuild.
- Closure gate: compiled CLI/daemon L0 proves four approved defaults, no
  work-record cache read or persistence by omission, miss/stale behavior,
  invalidation, partial projection truth, and prior policy retained after invalid
  validate/reload.
- Authority refs: `docs/spec-servicenow-operational-capabilities.md#scope`;
  `crates/snow_core/src/config.rs#cacheconfig`.
- Allowed write scope: exactly the T-OPS-02 paths in `#allowed-changes`.
- Acceptance evidence: red-first offline rebuild fixture exposes current default
  work-record projection; red-first read fixture exposes current work-record
  cache hit/persistence; denied cache paths prove zero cache/vault/index I/O;
  validate proves zero daemon state change.
- Coder rule: Implement only the cited behavior. Surface every uncited path or solution as a blocker requiring an approved authority update.

### T-OPS-03: Add one approved typed read family

- System node: CAP-OPS-READ typed resource row.
- Phase / Wave: Phase 2 / row-specific wave.
- Hard prerequisites: FND-OPS-001 closure; FND-OPS-002 closure when cache-eligible;
  approved B-OPS-01 row.
- Provides / consumes: provides one named typed read/query family; consumes
  discovered metadata and the shared envelope/cache contracts.
- Closure gate: the approved operation works through every selected transport,
  preserves native values, uses only native paging, and reports source and
  completeness truthfully.
- Authority refs: `docs/spec-servicenow-operational-capabilities.md#scope`;
  approved B-OPS-01 row amendment.
- Allowed write scope: only row-named modules/tests; no wildcard table registry.
- Acceptance evidence: red-first local-ServiceNow L0 expectations, negative
  no-cache-I/O proof for live-only rows, and cache miss/stale proof where eligible.
- Coder rule: Implement only the cited behavior. Surface every uncited path or solution as a blocker requiring an approved authority update.

### T-OPS-04: Add one approved fail-closed governed write family

- System node: CAP-OPS-WRITE governed write row.
- Phase / Wave: Phase 3 / row-specific wave.
- Hard prerequisites: Foundation closures; target CAP-OPS-READ row closure;
  approved B-OPS-01 write row including finite bulk maximum when applicable.
- Provides / consumes: provides one disabled-by-default plan/apply family and
  policy enablement; consumes resource metadata, envelopes, receipt/audit, and
  target-count guards.
- Closure gate: L0 proves permitted fields, default denial, explicit environment
  enablement, confirmation, redacted upstream failure, receipt/audit, two-target
  default, finite named bulk, replay/concurrency, and CLI flag non-bypass.
- Authority refs: `docs/spec-servicenow-operational-capabilities.md#scope`;
  approved B-OPS-01 write row.
- Allowed write scope: only row-named modules/tests.
- Acceptance evidence: red-first local ServiceNow/state-store fake; every
  invalid policy, confirmation, idempotency, concurrency, and target-count path
  proves zero ServiceNow write I/O and preserves prior valid state.
- Coder rule: Implement only the cited behavior. Surface every uncited path or solution as a blocker requiring an approved authority update.

## Verification and closure

- Spec structure: `python3 /Users/jared/.openclaw/workspace/foreman/scripts/validate_spec_contract.py docs/spec-servicenow-operational-capabilities.md`.
- Scope topology: pin the handoff base; inspect `git status --short`; reject a
  production path outside the active task's allowed scope.
- T-OPS-05 inventory: compiled CLI help/subcommand inventory, daemon framed
  `contract_info` plus dispatcher route probes, direct MCP process `tools/list`
  plus call probes, and daemon-backed MCP process `tools/list` plus call probes.
  Expectations are independently authored from this contract. An advertised
  route returning unknown tool/method fails; policy-denied or declared
  daemon-required is valid only when the matrix says so.
- Existing `capabilities_doc_sync.rs` remains a supplemental documentation gate;
  it cannot close runtime inventory because string presence does not prove a
  callable route.
- T-OPS-01 read parity: independently assert CLI, RPC, direct MCP, and bridged
  MCP envelopes and native records; bridge forwarding mocks alone are not proof.
- T-OPS-02 cache: assert source, completeness, refresh timestamp, four default
  TTLs, omission/no-I/O, miss/stale behavior, invalidation, offline rebuild
  scope, partial projection truth, and validate/reload atomicity.
- T-OPS-04 writes: assert explicit enablement, permitted fields, confirmation,
  idempotency, replay, applicable concurrency, finite target bounds, audit and
  receipts, upstream failure, and zero I/O for every denial.
- Changed behavior begins with a red L0 test that fails for the intended old
  behavior. DTO, schema, derive, constructor, mock-call, or self-round-trip
  assertions do not count.
- Static gates: `cargo fmt --all --check`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, `python3 scripts/check_rust_source_graph.py`, `git diff --check`, and `bash scripts/sensitive_scan.sh HEAD` plus a working-tree public-safe scan.
- Workspace gate before implementation closure: `CARGO_INCREMENTAL=0 cargo test --workspace --all-features`. External dependency failure is reported as blocked, never silently green.
- Live execution is operator-owned. Raw results stay outside git; source/fake
  proof is never represented as installed-runtime evidence.
- Stub closure: every scaffold seam is implemented with cited proof or reported
  as typed blocked/unavailable; compilation alone does not close it.

## Implementation readiness decision

- **T-OPS-05: COMPLETE** from immutable base
  `622a9258d959afea035ce75e4c52ba888d7d8db0`; focused inventory evidence passes.
- **T-OPS-01: COMPLETE** on 2026-08-20. `snow_core::ResourceDescriptor`,
  `FieldSupport`, and `OperationEnvelope` ship as the shared shapes, and
  `incident_fields` proves exact independently-authored envelopes through the
  compiled CLI, core API, daemon JSON-RPC, direct MCP, and a real local-socket
  daemon-backed MCP bridge call. Discovery reads live `sys_dictionary` and
  hierarchy-aware `sys_choice` only; no bundled schema is consulted. Field
  candidates are explicitly structural metadata, never an ACL authorization
  claim, and every empty-schema transport rejects undeclared arguments.
- **T-OPS-02: BLOCKED by B-OPS-06.** B-OPS-05 and its offline-rebuild baseline
  are green, but the public cache-policy wire/lifecycle contract is not defined.
- **T-OPS-03: BLOCKED by B-OPS-07.** The Incident row selects the family and
  table but does not define implementable get/query request and result contracts.
- **T-OPS-04: BLOCKED by T-OPS-03 and B-OPS-08.** The row approves fields and
  target bounds but does not define the required named bulk operation's wire,
  failure, concurrency, confirmation, or receipt semantics.
- **Production feature closure: BLOCKED** until selected row evidence and
  installed operator proof pass.

This is a bounded readiness decision. It authorizes no blocked source handoff,
live ServiceNow access, or deployment policy change.
