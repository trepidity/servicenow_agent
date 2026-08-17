# Implementation Spec: Incident assignment-group operations

## Authority and scope

### Approved goal

Close the assignment-group Incident implementation as a usable normal-operations
team queue by implementing all six user-approved capabilities: group discovery
and membership validation; triage filters and sorting; a governed action loop;
operational context; handoff aggregates; and delta/watch support that can report
Incidents leaving the group.

This specification supersedes only the conflicting non-goals in
`docs/spec-incident-list-by-assignment-group.md#non-goals`. The existing
`incident_list_by_assignment_group` compatibility contract remains supported.

### Governing authority

- `docs/spec-incident-assignment-group-operations.md#approved-goal`
- `docs/spec-incident-list-by-assignment-group.md#scope`
- `crates/snow_mcp/CAPABILITIES.md#write-transactions-mutate-servicenow--plan-state`
- `AGENTS.md#public-safe-content`

### Scope

- Add `incident_assignment_groups`, a live read of the authenticated user's
  active direct `sys_user_grmember` memberships, with optional exact
  case-insensitive name or sys_id resolution.
- Add `incident_assignment_group_queue`, a membership-validated, bounded live
  queue. It accepts an exact group name or sys_id and supports filters for
  state, assignee (`me`, `unassigned`, exact user selector), priority, opened
  time, updated time/staleness, and SLA risk.
- Support deterministic sorting by priority, opened time, updated time,
  assignee, or SLA risk in ascending or descending order. The operation scans
  at most `scan_limit` matching Incidents (default 2,000; maximum 5,000), sorts
  that bounded set, and reports whether the scan was complete.
- Return operational fields for caller, assignment, CI/service, priority,
  impact, urgency, hold reason, timestamps, description, latest journal
  activity, and Task SLA status. Unreadable SLA data remains explicitly
  unavailable; it is never represented as healthy or zero risk.
- Return handoff counts by state, priority, assignee, and SLA bucket plus
  unassigned and stale counts. Aggregate completeness follows scan
  completeness.
- Accept `updated_since` for changed-current-member reads. Accept a bounded
  `known_sys_ids` baseline and compare those records without an assignment
  filter so reassigned, inactive, missing, or terminal Incidents are returned
  in `departed_sys_ids`. Return a server-start watermark for the next poll.
- Add `incident_plan_update` / `incident_apply_update` for one Incident. The
  allowlist is `assigned_to`, `assignment_group`, `state`, and `work_notes`.
  `assigned_to` supports `me`, `unassigned`, or an exact user selector;
  `assignment_group` must resolve to one of the authenticated user's active
  groups; and `state` must resolve through live Incident choices.
- Apply requires the plan-issued confirmation token, idempotency key, and
  concurrency token (`sys_updated_on` plus optional `sys_mod_count`).
- Expose identical contracts through core, daemon JSON-RPC, direct read MCP,
  and daemon-backed MCP. Governed writes require the daemon.

### Non-goals

- No arbitrary encoded query, arbitrary table, arbitrary writable field, bulk
  update, delete, or caller-supplied ServiceNow identity.
- No fuzzy group, state, or user matching. Ambiguous or unknown selectors fail
  with correction candidates where available.
- No claim that a bounded scan is complete when `scan_limit` is reached.
- No durable server-side subscription or background polling job. Delta state is
  explicit caller input/output.
- No CLI/TUI surface in this closure slice.
- No organization-specific identifiers, names, hosts, values, or fixtures.

## System progression and dependency map

| Node ID | Node type | Authority refs | Phase | Wave | Provides | Consumes | Hard prerequisites | Closure gate |
|---|---|---|---|---|---|---|---|---|
| FND-INC-OPS-001 | Foundation | `docs/spec-incident-assignment-group-operations.md#scope` | Phase 1 | Wave 1 | Typed group, queue, context, aggregate, delta, and action contracts | Existing Incident and SLA models | Foundation root | Core consumer tests prove bounded fail-closed behavior |
| CAP-INC-OPS-002 | Capability | `docs/spec-incident-assignment-group-operations.md#scope` | Phase 2 | Wave 2 | Membership-scoped live discovery and queue reads | FND-INC-OPS-001 | FND-INC-OPS-001 closure | Fake ServiceNow seam proves filters, sorting, context, aggregates, and departures |
| CAP-INC-OPS-003 | Capability | `docs/spec-incident-assignment-group-operations.md#scope` | Phase 2 | Wave 2 | Governed single-Incident update | FND-INC-OPS-001 and existing planner stores | FND-INC-OPS-001 closure | Plan/apply seam proves allowlist, confirmation, idempotency, and concurrency conflict |
| FEAT-INC-OPS-004 | Feature | `docs/spec-incident-assignment-group-operations.md#approved-goal` | Phase 3 | Wave 3 | MCP/daemon operational queue and action loop | CAP-INC-OPS-002 and CAP-INC-OPS-003 | Both capability gates | Tool-list, direct read, daemon bridge, schema smoke, and workspace gates pass |

Hard `requires` edges override desired delivery order and parallelism. Preserve
legacy registered IDs as opaque IDs and give them an explicit node/gate noun.

## Traceability matrix

| Authority ref | Behavior / decision | Task ID | Implementation seam | Acceptance evidence | Owner |
|---|---|---|---|---|---|
| `docs/spec-incident-assignment-group-operations.md#scope` | Discover and exactly resolve authenticated-user groups | T1 | `snow_core::service::record`; MCP records tools | Consumer fake proves active membership list, name resolution, ambiguity, and non-member denial | Rust coder |
| `docs/spec-incident-assignment-group-operations.md#scope` | Filter and sort a bounded group queue | T2 | `IncidentAssignmentGroupQueueInput`; `RecordService` | Fake proves query filters plus independent sorted page expectation | Rust coder |
| `docs/spec-incident-assignment-group-operations.md#scope` | Preserve unavailable SLA and return operational context | T2 | queue item projection and `task_sla_status_for_tasks` | Fake proves caller/CI/service/journal projection and unavailable SLA state | Rust coder |
| `docs/spec-incident-assignment-group-operations.md#scope` | Return handoff aggregates with completeness | T2 | queue aggregate projection | Fake proves literal bucket counts and truncated status | Rust coder |
| `docs/spec-incident-assignment-group-operations.md#scope` | Return watermark and detect departures from caller baseline | T2 | queue delta comparison | Fake proves reassigned, inactive, missing, and terminal baseline records depart | Rust coder |
| `docs/spec-incident-assignment-group-operations.md#scope` | Plan/apply one allowlisted Incident update | T3 | daemon write handler and core write adapter | Daemon consumer test proves preview, confirmation, idempotency, and applied payload | Rust coder |
| `docs/spec-incident-assignment-group-operations.md#scope` | Reject stale/missing concurrency and non-member transfer | T3 | daemon write guard | Negative seam tests return typed conflict/field rejection before write | Rust coder |
| `docs/spec-incident-assignment-group-operations.md#scope` | Register public MCP/daemon contracts | T4 | registry, server, bridge, RPC, policy, capability docs | Tool-list and bridge tests plus schema smoke | MCP coder |
| `AGENTS.md#public-safe-content` | Keep artifacts generic | T5 | all changed tracked files | Sensitive-pattern scan over added lines | QA |

## Implementation boundary

### Allowed changes

- `crates/snow_core/src/resource/incident.rs`, `service/record.rs`,
  `service/write.rs`, `facade.rs`, `lib.rs`, and focused tests for typed Incident
  operations.
- `crates/snow_daemon/src/rpc.rs`, the existing governed write implementation
  seam or a focused Incident write module, and focused daemon tests.
- `crates/snow_mcp/src/tools`, registry, direct server, daemon bridge, policy,
  focused MCP tests, and capability documentation.
- This specification and existing Incident capability documentation.

### Forbidden changes / non-goals

- Do not weaken existing write confirmation, idempotency, audit, policy, or
  concurrency behavior.
- Do not expose raw encoded-query or arbitrary-field inputs.
- Do not persist queue results or turn incomplete/unreadable data into a
  complete or healthy result.
- Preserve the existing `incident_list_by_assignment_group` tool and response.

**No-invention declaration:** Implement only the cited behavior inside this
boundary. A missing execution path, architecture, API, persistence flow,
provider behavior, or product solution is a blocker requiring an approved
authority update, not a coder decision.

## Decision gaps and blockers

- None. The approved goal authorizes the six capabilities; the safe bounded
  defaults, exact selector rules, explicit completeness, and governed-write
  requirements above resolve their implementation semantics.

## Scaffold inventory

None.

## Task breakdown and coder handoffs

### T1: Typed group discovery and selector foundation

- System node: FND-INC-OPS-001 typed operations foundation
- Phase / Wave: Phase 1 / Wave 1
- Hard prerequisites: Foundation root
- Provides / consumes: typed group identity and fail-closed membership selector
- Closure gate: focused fake proves discovery and resolution behavior
- Authority refs: `docs/spec-incident-assignment-group-operations.md#scope`.
- Allowed write scope: core Incident resource/service/facade and focused tests
- Acceptance evidence: focused `snow_core` consumer tests
- Coder rule: Implement only cited behavior; every uncited path or solution is a blocker requiring an approved authority update.

### T2: Bounded operational queue

- System node: CAP-INC-OPS-002 queue read capability
- Phase / Wave: Phase 2 / Wave 2
- Hard prerequisites: FND-INC-OPS-001 closure
- Provides / consumes: triage, context, SLA, aggregates, and delta response
- Closure gate: independent fake response proves every queue behavior
- Authority refs: `docs/spec-incident-assignment-group-operations.md#scope`.
- Allowed write scope: core Incident resource/service/facade and focused tests
- Acceptance evidence: focused core and direct MCP tests
- Coder rule: Implement only cited behavior; every uncited path or solution is a blocker requiring an approved authority update.

### T3: Governed Incident update

- System node: CAP-INC-OPS-003 governed action capability
- Phase / Wave: Phase 2 / Wave 2
- Hard prerequisites: FND-INC-OPS-001 closure and existing planner-store gates
- Provides / consumes: concurrency-safe plan/apply for four allowlisted fields
- Closure gate: daemon consumer tests prove success and every fail-closed guard
- Authority refs: `docs/spec-incident-assignment-group-operations.md#scope`;
  `crates/snow_mcp/CAPABILITIES.md#write-transactions-mutate-servicenow--plan-state`.
- Allowed write scope: core write adapter, daemon write/RPC, policy, focused tests
- Acceptance evidence: daemon plan/apply tests
- Coder rule: Implement only cited behavior; every uncited path or solution is a blocker requiring an approved authority update.

### T4: MCP and daemon exposure

- System node: FEAT-INC-OPS-004 operational feature
- Phase / Wave: Phase 3 / Wave 3
- Hard prerequisites: CAP-INC-OPS-002 and CAP-INC-OPS-003 closure
- Provides / consumes: registered tools, RPC methods, bridge mapping, live policy
- Closure gate: tool-list, direct server, daemon bridge, and schema smoke pass
- Authority refs: `docs/spec-incident-assignment-group-operations.md#approved-goal`;
  `docs/spec-incident-assignment-group-operations.md#scope`.
- Allowed write scope: MCP tools/server/bridge, daemon RPC, capability docs/tests
- Acceptance evidence: focused MCP/daemon contracts and schema smoke
- Coder rule: Implement only cited behavior; every uncited path or solution is a blocker requiring an approved authority update.

### T5: Closure verification

- System node: FEAT-INC-OPS-004 closure gate
- Phase / Wave: Phase 3 / Wave 3
- Hard prerequisites: T1 through T4 evidence
- Provides / consumes: falsified production-closure claim
- Closure gate: all commands below pass and no remaining scaffold/stub exists
- Authority refs: `#approved-goal`; `AGENTS.md#public-safe-content`
- Allowed write scope: documentation/test-evidence corrections only
- Acceptance evidence: validator, format, focused tests, workspace tests,
  clippy, schema smoke, diff check, public-safe scan
- Coder rule: Implement only cited behavior; every uncited path or solution is a blocker requiring an approved authority update.

## Verification and closure

- Spec structure: `python3 /Users/jared/.openclaw/workspace/foreman/scripts/validate_spec_contract.py docs/spec-incident-assignment-group-operations.md`
- Focused core: `cargo test -p snow_core --lib incident_assignment_group_operations`
- Focused MCP: `cargo test -p snow_mcp --test incident_assignment_group_operations`
- Focused daemon: `cargo test -p snow_daemon incident_assignment_group_operations`
- MCP contracts: `cargo test -p snow_mcp --test contract_tools_list` and
  `cargo test -p snow_mcp --test daemon_bridge`
- Workspace: `cargo fmt --check`, `cargo test --workspace --all-targets`,
  `cargo clippy --workspace --all-targets -- -D warnings`, and `git diff --check`
- Runtime schema: `python3 -B scripts/mcp_schema_smoke.py -- <bridge command>`
- Public safety: scan added tracked lines for organization/deployment-specific
  names, identifiers, URLs, paths, credentials, and environment values.
- Stub closure: every scaffold seam is implemented with its cited proof, or is
  explicitly reported as a typed blocked/untrusted state; none is called complete
  merely because it compiles.
