# Implementation Spec: List Incidents by Assignment Group

## Authority and scope

### Approved goal

Expose a read-only API and MCP capability named
`incident_list_by_assignment_group`. It lists active ServiceNow `incident`
records for a supplied assignment-group `sys_id`, optionally constrained to a
resolved exact Incident state such as `Pending`. The caller must be able to
page until the result set is exhausted; a single unbounded MCP response is not
the contract.

The direct user directive and the agreed tool name/state behavior in this
conversation are the product authority. This document records that authority
at `docs/spec-incident-list-by-assignment-group.md#approved-goal` because this
repository has no accepted requirement-ID scheme for this feature.

### Governing authority

- `docs/spec-incident-list-by-assignment-group.md#approved-goal` — requested
  product behavior and agreed tool name.
- `crates/snow_core/src/service/record.rs` — existing fresh Incident behavior:
  filters `active=true`, rejects terminal/inactive records locally, and is
  explicitly assignee-scoped rather than group-scoped.
- `crates/snow_daemon/src/rpc.rs` — daemon RPC method registration,
  dispatch, supported-method contract, and the existing `field_choices`
  service.
- `crates/snow_mcp/src/tools/records.rs` and
  `crates/snow_mcp/src/daemon_bridge.rs` — registered MCP tools and
  daemon-backed capability discovery/routing.
- `crates/snow_mcp/CAPABILITIES.md#runtime-introspection` — consuming agents
  discover enabled tools through live `tool_capabilities`.
- `AGENTS.md#public-safe-content` — tracked docs, tests, fixtures, and
  snapshots remain public-safe.

### Scope

- Add the typed core API `SnowCore::incident_list_by_assignment_group` and
  its input/output types.
- Query only the `incident` table, requiring an assignment-group `sys_id` and
  `active=true`.
- Support an optional exact Incident `state` selector. The API accepts either
  an exact raw ServiceNow value or an exact case-insensitive label resolved
  from `field_choices("incident", "state")`; unknown or ambiguous selectors
  fail with structured invalid parameters and choices for correction.
- Return a cursor-paged, stable `sys_id`-ordered result. `next_cursor` is the
  `sys_id` of the last row **ServiceNow returned** for the page — not the last
  row that survived local filtering — and is exclusive on the next request.
  Anchoring the cursor to the surviving record would stall the scan whenever a
  whole page is rejected locally, which the requested-rows limit semantics make
  reachable. `complete` means the scan reached the end, not that the ServiceNow
  table was a transactional snapshot.
- Bound the response: default page size `50`, maximum `200`, where `limit`
  counts ServiceNow rows requested per page. See
  `docs/spec-incident-list-by-assignment-group.md#decision-gaps-and-blockers`.
- Return an ephemeral typed projection only. The operation performs no local
  persistence and no cache mutation.
- Expose the same typed operation through daemon JSON-RPC, direct MCP, and
  the daemon-backed MCP bridge. It is a read tool, requires no confirmation,
  and participates in `tool_capabilities`.
- Update static MCP capability documentation and agent-facing tool
  descriptions with the paging and state-correction path.

### Non-goals

- No arbitrary table, encoded-query, assignment-group-name, free-text-state,
  inactive, write, or delete capability.
- No CLI/TUI command, background sync job, cache-only query, assignment-group
  membership lookup, or changes to `list_my_incidents`.
- No local persistence, vault write, search-index write, or cache mutation.
- No assignment-group allowlist or policy-layer scope narrowing; ServiceNow
  ACLs are the authorization boundary.
- No claim of a point-in-time inventory while Incident assignment/state changes
  during a multi-page scan.
- No organization-specific ServiceNow identifiers, state values, hosts, or
  result data in tracked artifacts.

## System progression and dependency map

| Node ID | Node type | Authority refs | Phase | Wave | Provides | Consumes | Hard prerequisites | Closure gate |
|---|---|---|---|---|---|---|---|---|
| FND-INC-GRP-001 | Foundation | `docs/spec-incident-list-by-assignment-group.md#approved-goal`; `crates/snow_core/src/service/record.rs` | Phase 1 | Wave 1 | Typed group-and-state page contract with safe validation semantics | Existing Incident projection and ServiceNow Table API | Foundation root | Independent core fake proves exact group, active, state, cursor, and correction behavior |
| CAP-INC-GRP-002 | Capability | `docs/spec-incident-list-by-assignment-group.md#scope`; `crates/snow_daemon/src/rpc.rs` | Phase 2 | Wave 2 | Daemon JSON-RPC method and supported-method advertisement | FND-INC-GRP-001 | FND-INC-GRP-001 closure gate | Daemon consumer receives typed records, cursor, completion, and invalid-parameter errors |
| CAP-INC-GRP-003 | Capability | `docs/spec-incident-list-by-assignment-group.md#scope`; `crates/snow_mcp/src/tools/records.rs`; `crates/snow_mcp/src/daemon_bridge.rs` | Phase 3 | Wave 3 | Direct MCP and daemon-backed MCP parity | FND-INC-GRP-001 and CAP-INC-GRP-002 | FND-INC-GRP-001 and CAP-INC-GRP-002 closure gates | Both MCP transports advertise and execute the same tool contract |
| FEAT-INC-GRP-004 | Feature | `docs/spec-incident-list-by-assignment-group.md#approved-goal`; `crates/snow_mcp/CAPABILITIES.md#runtime-introspection` | Phase 4 | Wave 4 | Discoverable, documented read capability for agents | CAP-INC-GRP-003 | CAP-INC-GRP-003 closure gate | Documentation, schemas, focused tests, MCP schema smoke, and approved live proof all pass |

Hard `requires` edges override desired delivery order and parallelism. A
successful direct-MCP test does not close the daemon-backed feature.

## Traceability matrix

| Authority ref | Behavior / decision | Task ID | Implementation seam | Acceptance evidence | Owner |
|---|---|---|---|---|---|
| `docs/spec-incident-list-by-assignment-group.md#approved-goal` | Tool name is `incident_list_by_assignment_group`; group identifier is a required `sys_id` | T1 | `crates/snow_core/src/service/record.rs`; `crates/snow_core/src/facade.rs` | Core consumer test rejects missing/malformed group identifiers and sends only a group-scoped Incident query | Rust core coder |
| `docs/spec-incident-list-by-assignment-group.md#scope`; `crates/snow_core/src/service/record.rs` | Active Incident semantics are ServiceNow `active=true` plus local terminal/inactive rejection | T1 | `RecordService::incident_list_by_assignment_group` | Mock response with active Pending, inactive Pending, and terminal rows returns only active non-terminal matches | Rust core coder |
| `docs/spec-incident-list-by-assignment-group.md#scope`; `crates/snow_daemon/src/rpc/` | Exact raw or label state resolves through Incident field choices and has a correction path | T1 | Typed input validation and `SnowCore::field_choices("incident", "state")` consumption | Raw value and exact label select the same state; unknown/ambiguous label returns structured invalid parameters and candidates | Rust core coder |
| `docs/spec-incident-list-by-assignment-group.md#scope` | Paging is stable, exclusive, and does not fabricate a snapshot guarantee | T1 | Core page result and ServiceNow Table API query builder | Multi-page fake proves ascending `sys_id`, no duplicate boundary record, `next_cursor`, and final `complete=true` | Rust core coder |
| `docs/spec-incident-list-by-assignment-group.md#decision-gaps-and-blockers` | Page size defaults to 50, rejects above 200, and counts requested ServiceNow rows | T1 | Typed input validation and `sysparm_limit` construction | Omitted `limit` requests 50 rows; `limit=201` returns invalid parameters without a query; a page whose rows are partly filtered locally still reports `complete=false` and issues exactly one request | Rust core coder |
| `docs/spec-incident-list-by-assignment-group.md#scope` | Cursor anchors to the last ServiceNow row, so a fully-filtered page still advances | T1 | Core page result construction | Test where every row on a full page is locally rejected returns zero records, `complete=false`, and a `next_cursor` that advances the next request | Rust core coder |
| `docs/spec-incident-list-by-assignment-group.md#decision-gaps-and-blockers` | Group reads are ephemeral and never persisted | T1 | `RecordService::incident_list_by_assignment_group` | Test drives a full multi-page scan against an in-memory store and asserts the store, vault, and cache remain empty afterward | Rust core coder |
| `docs/spec-incident-list-by-assignment-group.md#scope`; `crates/snow_daemon/src/rpc/` | Daemon method is routable and declared in `contract_info` | T2 | `RpcMethod`, dispatcher, `SUPPORTED_RPC_METHODS` | JSON-RPC consumer test executes the method and bridge contract lists it as supported | Daemon coder |
| `docs/spec-incident-list-by-assignment-group.md#scope`; `crates/snow_mcp/src/tools/records.rs`; `scripts/mcp_schema_smoke.py` | MCP schema is an object, validates supported arguments, and has no top-level composition | T3 | Records tool registry and direct server handler | `contract_tools_list` asserts schema/tool metadata; schema smoke passes | MCP coder |
| `docs/spec-incident-list-by-assignment-group.md#scope`; `crates/snow_mcp/src/daemon_bridge.rs`; `crates/snow_mcp/CAPABILITIES.md#runtime-introspection` | Daemon-backed bridge advertises only a daemon-supported tool and forwards all page arguments unchanged | T3 | `BRIDGE_TOOL_METHODS`, bridge parameter forwarding, capabilities documentation | Mock-daemon bridge test observes method/arguments; `tool_capabilities` reports read-only capability | MCP coder |
| `AGENTS.md#public-safe-content` | Artifacts remain public-safe | T4 | Changed docs, tests, fixtures, snapshots | Diff review and public-safe scan contain only generic values | QA owner |

## Implementation boundary

### Allowed changes

- `crates/snow_core/src/service/record.rs`, `crates/snow_core/src/facade.rs`,
  and the smallest existing core type module needed for the public typed input
  and output.
- `crates/snow_daemon/src/rpc/` for the daemon JSON-RPC operation. The legacy
  `crates/snow_daemon/src/mcp.rs` orphan was removed. The daemon hosts
  `snow_mcp::McpServer` (`crates/snow_daemon/src/lib.rs`), so the daemon-hosted
  MCP path is served by the `snow_mcp` changes below and needs no separate
  daemon-side dispatcher.
- `crates/snow_mcp/src/tools/records.rs`, `crates/snow_mcp/src/server.rs`,
  `crates/snow_mcp/src/daemon_bridge.rs`, and focused contract tests.
- `crates/snow_mcp/CAPABILITIES.md` and this specification for discoverability
  and closure evidence.

### Forbidden changes / non-goals

- Do not add raw `sysparm_query`, a generic table-list API, display-name group
  resolution, fuzzy state matching, or client-provided ServiceNow identity.
- Do not change write policy, confirmation behavior, `list_my_incidents`,
  existing cache/tombstone semantics, or unrelated resource contracts.
- Do not persist, cache, index, or vault any record this operation returns, and
  do not clamp an over-maximum `limit` instead of rejecting it.

**No-invention declaration:** Implement only the cited behavior inside this
boundary. A missing execution path, architecture, API, persistence flow,
provider behavior, or product solution is a blocker requiring an approved
authority update, not a coder decision.

## Decision gaps and blockers

All three prior blockers were resolved by direct user decision on 2026-08-16.
Those decisions are product authority at
`docs/spec-incident-list-by-assignment-group.md#decision-gaps-and-blockers`
and are binding on implementation.

- **RESOLVED — paging limit and response budget.** Default page size is `50`;
  maximum accepted `limit` is `200`, matching the closest existing cursor-style
  precedent (`crates/snow_core/src/resource/resource_plan.rs`
  `RESOURCE_PLAN_LIST_DEFAULT_LIMIT` / `RESOURCE_PLAN_LIST_MAX_LIMIT`). `limit`
  counts **ServiceNow rows requested per page**, not rows returned. A page may
  therefore return fewer than `limit` records after local terminal/inactive
  rejection, and that is not an error. `complete` is `true` only when ServiceNow
  returned fewer than `limit` rows for the page. The implementation must issue
  exactly one Table API request per page — no internal over-fetch loop to
  "fill" a page. A `limit` above the maximum is a structured invalid-parameter
  error, not a silent clamp.
- **RESOLVED — persistence/retention.** The operation returns an ephemeral
  typed projection and performs **no** local persistence: it must not call
  `persist_record`/`persist_records`, must not write the vault or search index,
  and must not populate or invalidate the work-record cache. This deliberately
  diverges from the assignee-scoped hydrate path at
  `crates/snow_core/src/service/record.rs` (`persist_records`), because group
  scope has no tombstoning story for records that later leave the group.
  Callers that need a durable copy of one Incident use the existing fresh
  single-record path.
- **RESOLVED — group-read authorization.** ServiceNow ACLs on the runtime
  credential are the sole authorization boundary, consistent with every other
  read path in this repository. No assignment-group allowlist is added. Agent
  documentation must state plainly that the tool returns whatever the runtime
  credential is permitted to read, and that the tool applies no scope narrowing
  of its own.
- **Risk, not a blocker — moving result set.** The cursor scan is ordered but
  not transactional; an Incident reassigned or re-stated between pages may be
  omitted or repeated by ServiceNow. The response must never claim a snapshot.

## Scaffold inventory

| Seam ID | Authority refs | Crate / file / symbol | Signature / contract | Safe unresolved state | Owner | Completion evidence |
|---|---|---|---|---|---|---|
| SC-INC-GRP-CORE | `docs/spec-incident-list-by-assignment-group.md#scope` | `snow_core::RecordService` and `SnowCore` facade | `incident_list_by_assignment_group(input) -> Result<IncidentAssignmentGroupPage>`; validates `sys_id`, exact state, and cursor before I/O | Typed invalid-parameter error; no query, no partial synthetic success | Rust core coder | Core fake and pagination/state tests |
| SC-INC-GRP-DAEMON | `docs/spec-incident-list-by-assignment-group.md#scope`; `crates/snow_daemon/src/rpc.rs` | Daemon RPC method/dispatcher | JSON-RPC arguments map exactly to core input; result includes records and page metadata | Method unavailable or invalid-parameter error; never map to `list_my_incidents` | Daemon coder | RPC dispatch and contract-info tests |
| SC-INC-GRP-MCP | `docs/spec-incident-list-by-assignment-group.md#scope`; `crates/snow_mcp/src/daemon_bridge.rs` | Tool registry, direct server, daemon bridge | MCP tool schema and direct/bridged execution use the same arguments and structured result | Tool unavailable when daemon contract lacks method; invalid arguments fail-closed | MCP coder | Direct tools/list and bridge-forwarding tests plus schema smoke |

## Task breakdown and coder handoffs

### T1: Core Incident group-page API

- System node: FND-INC-GRP-001.
- Phase / Wave: Phase 1 / Wave 1.
- Hard prerequisites: Foundation root. All three prior blocked decisions are
  resolved in
  `docs/spec-incident-list-by-assignment-group.md#decision-gaps-and-blockers`.
- Provides / consumes: Provides a typed page result; consumes the existing
  Incident projection, `field_choices`, and Table API paginator.
- Closure gate: The focused core tests prove group, active, state-resolution,
  cursor, and correction behavior independently of the implementation.
- Authority refs: `docs/spec-incident-list-by-assignment-group.md#scope`;
  `crates/snow_core/src/service/record.rs`.
- Allowed write scope: Core files listed under implementation boundary and
  focused core tests only.
- Acceptance evidence: A multi-page mock Table API test; a Pending label/raw
  equivalence test; active/terminal negative cases; malformed sys_id/cursor and
  ambiguous/unknown state correction tests; default/over-maximum `limit` tests;
  a no-persistence assertion after a full scan.
- Coder rule: Implement only cited behavior; every uncited path or solution is a blocker requiring an approved authority update.

### T2: Daemon API capability

- System node: CAP-INC-GRP-002.
- Phase / Wave: Phase 2 / Wave 2.
- Hard prerequisites: FND-INC-GRP-001 closure gate.
- Provides / consumes: Provides `incident_list_by_assignment_group` daemon
  JSON-RPC; consumes the core typed input/output.
- Closure gate: The daemon accepts/rejects the same shape as the core and
  declares the method in `contract_info`.
- Authority refs: `docs/spec-incident-list-by-assignment-group.md#scope`;
  `crates/snow_daemon/src/rpc.rs`.
- Allowed write scope: Daemon RPC/MCP dispatch files and their focused tests.
- Acceptance evidence: RPC consumer tests for success, invalid parameters, and
  supported-method advertisement.
- Coder rule: Implement only cited behavior; every uncited path or solution is a blocker requiring an approved authority update.

### T3: Direct and daemon-backed MCP parity

- System node: CAP-INC-GRP-003.
- Phase / Wave: Phase 3 / Wave 3.
- Hard prerequisites: FND-INC-GRP-001 and CAP-INC-GRP-002 closure gates.
- Provides / consumes: Provides discoverable MCP execution over direct core and
  daemon-backed transports; consumes the core contract and daemon method.
- Closure gate: Both paths advertise the tool with the same object schema and
  route the same request/response contract.
- Authority refs: `docs/spec-incident-list-by-assignment-group.md#scope`;
  `crates/snow_mcp/src/tools/records.rs`; `crates/snow_mcp/src/daemon_bridge.rs`.
- Allowed write scope: MCP tool registry, direct server, bridge, focused MCP
  tests, and capability documentation.
- Acceptance evidence: Direct tools/list/execute test; bridge supported-method
  gating and argument-forwarding test; `tool_capabilities` read-only assertion;
  `scripts/mcp_schema_smoke.py` pass.
- Coder rule: Implement only cited behavior; every uncited path or solution is a blocker requiring an approved authority update.

### T4: Feature closure review

- System node: FEAT-INC-GRP-004.
- Phase / Wave: Phase 4 / Wave 4.
- Hard prerequisites: CAP-INC-GRP-003 closure gate.
- Provides / consumes: Provides evidence-backed agent documentation and release
  verdict; consumes all prior node evidence.
- Closure gate: Focused and workspace gates pass; live verification is either
  performed against an approved safe target or explicitly reported as untested.
- Authority refs: `docs/spec-incident-list-by-assignment-group.md#approved-goal`;
  `AGENTS.md#public-safe-content`.
- Allowed write scope: Documentation and test-evidence corrections only.
- Acceptance evidence: Diff review, public-safe scan, Cargo gates, MCP schema
  smoke, and a documented live-read verdict.
- Coder rule: Implement only cited behavior; every uncited path or solution is a blocker requiring an approved authority update.

## Verification and closure

- Spec structure: `python3 /Users/jared/.openclaw/workspace/foreman/scripts/validate_spec_contract.py docs/spec-incident-list-by-assignment-group.md` must pass.
- Core behavior: run the focused `snow_core` tests that drive the mock
  ServiceNow Table API through the public `SnowCore` facade; the test must be
  written and observed failing before the implementation.
- Daemon and MCP behavior: run focused `snow_daemon` RPC/MCP and `snow_mcp`
  contract/bridge tests, including both direct and daemon-backed consumers.
- Workspace gates: run `cargo fmt --check`, `cargo test --workspace --all-targets`,
  `cargo clippy --workspace --all-targets -- -D warnings`, and `git diff --check`.
- Runtime proof: after the daemon is available, run `python3 -B scripts/mcp_schema_smoke.py -- <installed snow_mcp_bridge command>` and verify
  `tool_capabilities` reports `incident_list_by_assignment_group` as read-only.
- Live proof: execute a bounded request against an approved environment under
  the runtime credential's own ACLs; record only generic count,
  paging, and state-resolution evidence. A local mock is not proof of target
  ACLs or actual `sys_choice` values.
- Stub closure: every scaffold seam is implemented with its cited proof, or is
  explicitly reported as a typed blocked/untrusted state; none is called
  complete merely because it compiles.
