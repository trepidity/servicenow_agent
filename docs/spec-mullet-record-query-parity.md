# Implementation Spec: Snow record-query parity for Mullet

## Authority and scope

### Approved goal

Extend Snow so Mullet can execute its supported Change Request and Story list/search workflows through a strict, daemon-discoverable contract. Every accepted filter must be enforced by Snow; unsupported or unknown input must fail with invalid parameters rather than be ignored.

The direct user request is recorded by this section at
`docs/spec-mullet-record-query-parity.md#approved-goal`. The immediate defect
is that Mullet sends `filter` to `list_records`, while the current daemon does
not deserialize or apply that field. The outcome must be a typed contract that
supports Mullet's concrete Change and Story requirements without accepting raw
encoded queries.

### Governing authority

- `docs/spec-mullet-record-query-parity.md#approved-goal` — approved product
  goal and false-filter remediation.
- `docs/spec-incident-list-by-assignment-group.md#scope` — established Snow
  pattern for bounded live reads, strict input, cursor pagination, exact choice
  correction, and an explicit non-snapshot completeness claim.
- `crates/snow_daemon/src/rpc/mod.rs` and
  `crates/snow_daemon/src/rpc/wire.rs` — current `list_records`
  parser/dispatcher and `contract_info` method advertisement at Snow source
  review base `b73ec0872929045ebdefa9b0892df89c6d53f400`.
- `crates/snow_core/src/query/filter.rs` — existing cache-only `ListQuery`
  scope; it is insufficient for the requested fields and freshness semantics.
- `crates/snow_mcp/src/tools/records.rs` and
  `crates/snow_mcp/src/daemon_bridge.rs` — direct and daemon-backed MCP
  registration/parity surfaces.
- `AGENTS.md#public-safe-content` and `AGENTS.md#behavioral-test-seams` —
  public-safe artifacts and required L0/L1 evidence.
- Mullet repository `65d3949f5a85960001130cb24d8f4ddb61dfcea6`,
  `docs/spec-snow-via-mullet.md#DEC-SNOW-005` and `#DEC-SNOW-007` — the
  downstream completeness envelope and bounded generic-evidence boundary.
- Direct user decision on 2026-08-18, recorded under
  `docs/spec-mullet-record-query-parity.md#resolved-product-decisions` — exact
  record-query wire types, text scope, compact projections, legacy rejection,
  and generic-lookup disposition.

### Scope

- Add a new read-only `record_query` capability for exactly two resource kinds:
  `change_request` and `story`.
- Use typed, allowlisted query inputs; reject unknown properties and reject raw
  `filter`, encoded-query, arbitrary table, arbitrary field, and arbitrary
  sort inputs.
- Execute a bounded live ServiceNow Table API query, ordered by `sys_id`, with
  an exclusive cursor and no cache/vault persistence.
- Return records, `next_cursor`, `complete`, `source: "live"`, and the
  effective limit. `complete` means this bounded query reached the end of the
  current scan; it is not a transactional snapshot guarantee.
- Support Mullet's current Change Request filters and the typed Story
  list/search filters described below.
- Expose the same contract through `snow_core`, daemon JSON-RPC,
  direct MCP, daemon-backed MCP, and `contract_info`/tool discovery.
- Make legacy `list_records` reject unknown fields, including `filter`, once
  `record_query` is available. A clear invalid-parameter failure is required
  until a consumer migrates; silently unfiltered success is prohibited.

### Non-goals

- No raw ServiceNow encoded-query input, arbitrary table query, arbitrary
  column filter, free-form sort, write, cache refresh, cache persistence, or
  background synchronization.
- No generic full-text search expansion. `search_records` remains its existing
  generic scope; Story text search belongs only to `record_query` with
  `resource_type: "story"`.
- No Mullet implementation in this repository. Mullet adoption is an external
  hard dependency before the user-facing Mullet operations can be closed.
- No claim that direct source support proves the installed daemon or Mullet
  adapter has been deployed.
- No organization-specific table data, hosts, identifiers, users, or fixtures.

## Canonical operation contract

### Request envelope

`record_query` accepts one top-level object. The MCP schema must remain a
top-level object with no top-level `oneOf`/`anyOf`/`allOf`; it sets
`additionalProperties: false`. Rust uses a tagged enum internally, while the
MCP schema exposes the union of allowlisted filter properties and runtime
validation rejects every property that is invalid for the selected
`resource_type`.

```json
{
  "resource_type": "change_request | story",
  "filters": {},
  "include_description": false,
  "limit": 50,
  "cursor": "32-character lowercase hexadecimal sys_id or null"
}
```

- `resource_type` is required and accepts exactly `change_request` or `story`.
- `filters` defaults to `{}` and rejects unknown properties.
- `include_description` defaults to `false`, is valid only for `story`, and is
  rejected for `change_request`.
- `limit` defaults to `50`, accepts integers `1..=200`, counts ServiceNow rows
  requested for the page, and is rejected rather than clamped outside bounds.
- `cursor` is omitted/null on the first page. A supplied cursor must be exactly
  32 hexadecimal characters, is normalized to lowercase, and is used as the
  exclusive `sys_id > cursor` predicate.
- Empty strings, whitespace-only strings, duplicate array elements after
  normalization, inverted date ranges, and cross-resource filter properties
  are invalid parameters before the record Table API request.

### Change Request filters

| Property | Type and bounds | ServiceNow predicate |
|---|---|---|
| `assignment_group` | 32-hex sys_id | `assignment_group=<value>` |
| `assigned_to` | 32-hex sys_id | `assigned_to=<value>` |
| `state` | non-empty string, max 80 characters | exact raw value or unique case-insensitive exact label resolved from `field_choices("change_request", "state")`, then `state=<raw>` |
| `start_date_after` | calendar date `YYYY-MM-DD` | encoded query `start_date>{date}` |
| `start_date_before` | calendar date `YYYY-MM-DD` | encoded query `start_date<{date}` |

When both start-date bounds are supplied, `start_date_after` must be strictly
earlier than `start_date_before`.

### Story filters

| Property | Type and bounds | ServiceNow predicate |
|---|---|---|
| `assignment_group` | 32-hex sys_id | `assignment_group=<value>` |
| `assigned_to` | 32-hex sys_id | `assigned_to=<value>` |
| `story_owner` | 32-hex sys_id | `u_story_owner=<value>` |
| `lead_developer` | 32-hex sys_id | `u_lead_dev=<value>` |
| `states` | 1..=20 unique non-empty strings, each max 80 characters | each selector resolves through `field_choices("rm_story", "state")`; one raw value uses `state=<raw>`, multiple use `stateIN<raw,...>` |
| `sprint` | 32-hex sys_id | `sprint=<value>` |
| `project` | 32-hex sys_id | `project=<value>` |
| `cmdb_ci` | 32-hex sys_id | `cmdb_ci=<value>` |
| `blocked` | boolean | `blocked=true|false` |
| `due_date_after` | calendar date `YYYY-MM-DD` | encoded query `due_date>{date}` |
| `due_date_before` | calendar date `YYYY-MM-DD` | encoded query `due_date<{date}` |
| `updated_after` | ServiceNow UTC timestamp `YYYY-MM-DD HH:MM:SS` | encoded query `sys_updated_on>{timestamp}` |
| `numbers` | 1..=20 unique strings matching `^STRY[0-9]+$`, normalized uppercase | one value uses `number=<value>`, multiple use `numberIN<values>` |
| `text` | trimmed string, 1..=200 characters | `short_descriptionLIKE<value>` only |

When both due-date bounds are supplied, `due_date_after` must be strictly
earlier than `due_date_before`. Story text does not search `description`,
`acceptance_criteria`, journals, or arbitrary fields.

### State-choice contract

Raw state values and labels are resolved before the record query. Raw values
match case-sensitively; labels match case-insensitively and exactly. Unknown,
ambiguous, duplicate-after-resolution, or empty-choice results return invalid
parameters with `{ requested, table, field, ambiguous, choices }` correction
data and issue no Change/Story record request. The implementation must not
accept an unverified raw state when the live choice set is empty.

### Projection and page response

Change Request pages request exactly `sys_id`, `number`, `short_description`,
`state`, `start_date`, `end_date`, `assigned_to`, `assignment_group`, and
`cmdb_ci` with display values preserved by the established `SnowRecord`
projection.

Story pages request exactly `sys_id`, `number`, `short_description`, `state`,
`sprint`, `project`, `cmdb_ci`, `assigned_to`, `assignment_group`,
`u_story_owner`, `u_lead_dev`, `u_points_est`, `due_date`,
`desired_delivery_date`, `blocked`, `blocked_reason`, `status`,
`sys_updated_on`, and `sys_updated_by`. When `include_description=true`, add
exactly `description` and `acceptance_criteria`.

The canonical response is:

```json
{
  "records": [],
  "next_cursor": null,
  "complete": true,
  "source": "live",
  "limit": 50,
  "rows_inspected": 0
}
```

Exactly one Table API record request is issued per page. Results are ordered by
ascending `sys_id`; `next_cursor` is the last ServiceNow row's `sys_id` when a
full page is returned. `complete` is true only when ServiceNow returns fewer
than the effective limit. Therefore an exact-multiple scan requires a final
empty page to prove completion. This is deterministic cursor progression, not
a transactional snapshot guarantee.

## Dispatch bases and ownership

- Snow source review base is
  `b73ec0872929045ebdefa9b0892df89c6d53f400`; the checkout was clean when this
  base was recorded. Before T1 dispatch, record the commit containing this
  ratified specification as the immutable Snow handoff base. T1-T3 must run in
  a clean checkout/worktree from that base and reject paths outside their
  declared scopes.
- Mullet source review base is
  `65d3949f5a85960001130cb24d8f4ddb61dfcea6`. The review checkout contains
  pre-existing uncommitted Story-read and shared-registration work. T4 may not
  dispatch from that dirty tree: its hard prerequisite is a reconciled clean
  Mullet commit recorded in the T4 handoff manifest. Existing work must be
  preserved, reviewed, and either incorporated as the T4 baseline or kept out
  of its isolated worktree.
- Snow and Mullet are separate write lanes. No agent owns both repositories in
  one task, and no current working-tree mutation is treated as authority.

## System progression and dependency map

| Node ID | Node type | Authority refs | Phase | Wave | Provides | Consumes | Hard prerequisites | Closure gate |
|---|---|---|---|---|---|---|---|---|
| FND-RQ-000 | Foundation | `docs/spec-mullet-record-query-parity.md#state-choice-contract`; `crates/snow_core/src/context.rs`; `crates/snow_core/src/service/record.rs` | Phase 0 | Wave 0 | Provider evidence that exact Change and Story state choices are readable | Installed daemon `field_choices` and approved target environment | Foundation root | Read-only installed-daemon probe returns non-empty typed choice sets for both `change_request.state` and `rm_story.state`, recording only counts and pass/fail |
| FND-RQ-001 | Foundation | `docs/spec-mullet-record-query-parity.md#canonical-operation-contract`; `docs/spec-incident-list-by-assignment-group.md#scope` | Phase 1 | Wave 1 | Typed query/page DTOs, exact selector validation, and safe field-to-query compilation | Existing Snow Table API client and record projection | FND-RQ-000 closure gate and immutable Snow handoff base | L0 core fake proves unknown/raw inputs fail before I/O and each accepted filter compiles/enforces |
| CAP-RQ-002 | Capability | `docs/spec-mullet-record-query-parity.md#scope`; `crates/snow_daemon/src/rpc/mod.rs`; `crates/snow_daemon/src/rpc/wire.rs` | Phase 2 | Wave 2 | Discoverable daemon `record_query` and fail-closed daemon `list_records` parser | FND-RQ-001 | FND-RQ-001 closure gate | JSON-RPC consumer proves records/page metadata, method advertisement, and rejected ignored-filter input |
| CAP-RQ-003 | Capability | `docs/spec-mullet-record-query-parity.md#scope`; `crates/snow_mcp/src/tools/records.rs`; `crates/snow_mcp/src/daemon_bridge.rs` | Phase 3 | Wave 3 | Direct and daemon-backed MCP parity for `record_query` plus strict legacy rejection | FND-RQ-001; CAP-RQ-002 | FND-RQ-001 and CAP-RQ-002 closure gates | Both MCP transports advertise, validate, and return identical consumer-visible page payloads; direct and bridged legacy `filter` calls both fail invalid parameters before record I/O |
| FEAT-RQ-004 | Feature | `docs/spec-mullet-record-query-parity.md#resolved-product-decisions`; Mullet `docs/spec-snow-via-mullet.md#DEC-SNOW-005`; `#DEC-SNOW-007` | Phase 4 | Wave 4 | Safe Mullet adoption, removal of the false generic-list form, and installed-runtime proof | CAP-RQ-002; CAP-RQ-003; Mullet adapter/ops | CAP-RQ-002 and CAP-RQ-003 closure gates, released daemon, and clean immutable Mullet handoff base | Mullet rejects unavailable capability, uses typed inputs without fallback, preserves completeness/continuation, and rejects every remaining `list_records.filter` route |

Hard `requires` edges override desired delivery order. In particular, do not
ship a Mullet fallback to legacy `list_records`: the old request shape is the
defect being removed.

## Traceability matrix

| Authority ref | Behavior / decision | Task ID | Implementation seam | Acceptance evidence | Owner |
|---|---|---|---|---|---|
| `docs/spec-mullet-record-query-parity.md#state-choice-contract` | Change and Story state choices must be non-empty and typed before state-filter implementation starts | T0 | Installed daemon `field_choices` for the two exact table/field pairs | Public-safe read-only probe records non-zero counts and typed `{ value, label }` entries without recording values | Operator + QA owner |
| `docs/spec-mullet-record-query-parity.md#request-envelope` | Unknown, raw, cross-resource, malformed, and out-of-bound inputs fail before record I/O | T1 | `RecordQueryInput`, resource-specific filters, and pre-I/O validation | L0 requests covering unknown `filter`, bad cursor, limit 0/201, inverted ranges, excessive/duplicate arrays, and cross-kind properties return invalid parameters and make no record request | Rust core coder |
| `docs/spec-mullet-record-query-parity.md#change-request-filters` | Every Change Request filter compiles to its exact allowlisted predicate | T1 | `ChangeRequestQueryFilters` and live query compiler | Independent fake asserts the literal expected query and a near-match row cannot appear when any filter is omitted | Rust core coder |
| `docs/spec-mullet-record-query-parity.md#story-filters` | Every Story filter compiles to its exact allowlisted predicate; text is short-description-only | T1 | `StoryQueryFilters` and live query compiler | Independent fake asserts each literal clause; description-only and acceptance-criteria-only matches do not satisfy `text` | Rust core coder |
| `docs/spec-mullet-record-query-parity.md#state-choice-contract`; `docs/spec-incident-list-by-assignment-group.md#scope` | State accepts verified raw values or unique exact labels with correction data | T1 | Shared exact-choice resolver using `field_choices` | Raw/label equivalence, multi-state resolution, and empty/unknown/ambiguous/duplicate correction L0 tests | Rust core coder |
| `docs/spec-mullet-record-query-parity.md#projection-and-page-response` | Every page is compact, bounded, exclusive, sys_id-ordered, live, and explicitly non-snapshot | T1 | Page DTO and Table API cursor/projection compiler | Full page plus terminal empty page proves exact-multiple completion, cursor exclusivity, field lists, `rows_inspected`, and `source: live` | Rust core coder |
| `docs/spec-mullet-record-query-parity.md#approved-goal`; `crates/snow_daemon/src/rpc/mod.rs` | Daemon exposes the canonical method and daemon legacy false-filter path rejects | T2 | `RpcMethod`, strict params, dispatcher, supported method list | JSON-RPC consumer test covers success, `-32602`, `contract_info`, and `{ filter: ... }` rejection before core/Table API I/O | Daemon coder |
| `docs/spec-mullet-record-query-parity.md#approved-goal`; `crates/snow_mcp/src/server.rs`; `crates/snow_mcp/src/tools/records.rs` | Direct MCP legacy `list_records` rejects unknown fields at schema and runtime | T3 | `ListRecordsArguments`, legacy schema, and direct handler | Tool schema has `additionalProperties:false`; direct `{ filter: ... }` call returns invalid parameters before core I/O | MCP coder |
| `docs/spec-mullet-record-query-parity.md#scope`; `crates/snow_mcp/src/daemon_bridge.rs` | Direct and bridged `record_query` contracts are consumer-equivalent | T3 | Tool schema, direct handler, daemon bridge mapping | Independent expected JSON equals both parsed structured results; bridge unavailable path and schema smoke pass | MCP coder |
| `docs/spec-mullet-record-query-parity.md#resolved-product-decisions`; Mullet `docs/spec-snow-via-mullet.md#DEC-SNOW-005`; `#DEC-SNOW-007` | Mullet capability-gates adoption, maps completeness exactly, and removes the false generic-list form | T4 | Downstream allowlist, daemon adapter, Change/Story ops, lookup routing, envelopes | Registered-operation tests prove capability refusal, exact request mapping, continuation/status mapping, and generic-list rejection with no legacy fallback | Mullet coder + operator |

## Implementation boundary

### Allowed changes

- `crates/snow_core/src/resource/` — a focused typed record-query contract,
  exact-choice resolution reuse, and public re-exports.
- `crates/snow_core/src/service/record.rs`, `facade.rs`, and focused core
  consumer tests — bounded live query execution and projection.
- `crates/snow_daemon/src/rpc/mod.rs`, `rpc/wire.rs`, `rpc/tests.rs`, and focused
  daemon consumer tests — method, strict parsing, dispatch, response mapping,
  and `contract_info` inventory.
- `crates/snow_mcp/src/tools/records.rs`, `server.rs`, `daemon_bridge.rs`,
  capability documentation, and focused MCP contract tests. This includes
  strict legacy `ListRecordsArguments` deserialization and the legacy schema's
  `additionalProperties: false` rule.
- In the Mullet repository only during T4:
  `src/integrations/servicenow/{adapter,daemon-rpc,runtime,tools}.ts`,
  `src/ops/servicenow/{change-request-list,story-read,lookup,types}.ts`, the
  existing registry/help/worklog-policy integration surfaces, their focused
  and registered-operation tests, and `docs/spec-snow-via-mullet.md` for the
  approved DEC-SNOW-007 amendment. No other Mullet paths are in scope.
- This specification and migration notes that use only public-safe placeholders.

### Forbidden changes / non-goals

- Do not add a pass-through `filter`/`sysparm_query`, an arbitrary table or
  arbitrary-field API, an unbounded result set, a cache-only result presented
  as live, or a synthetic `complete: true` response.
- Do not modify governed-write policy, legacy record storage format, cache
  migrations, credentials, ServiceNow ACL behavior, or unrelated MCP tools.
- Do not make `search_records` claim a Story-only scope.

**No-invention declaration:** Implement only the cited behavior inside this
boundary. A missing execution path, architecture, API, persistence flow,
provider behavior, or product solution is a blocker requiring an approved
authority update, not a coder decision.

## Decision gaps and blockers

### Resolved product decisions

- **RESOLVED — Story text scope:** `text` searches only
  `rm_story.short_description`. It does not search descriptions, acceptance
  criteria, journals, or an administrator-selected field set.
- **RESOLVED — compact projections:** the exact default Change and Story field
  lists and the Story-only `include_description` expansion are binding under
  `docs/spec-mullet-record-query-parity.md#projection-and-page-response`.
- **RESOLVED — choice algorithm:** both tables use the existing live
  `field_choices(table, "state")` path and the Incident exact raw/label
  algorithm. Empty choices fail closed; raw values are not trusted merely
  because they look numeric.
- **RESOLVED — generic lookup:** T4 amends Mullet DEC-SNOW-007 narrowly. The
  bounded generic evidence operation remains, but no `servicenow.lookup` path
  may emit `list_records.filter`. A Story `record_kind + query` maps to
  `record_query` with the typed `text` filter. Demand, Project, Resource Plan,
  or explicit `tool: "list_records"` query-list forms are rejected before I/O
  with a correction to an existing purpose-built operation or exact-number
  lookup. Story Task parent enumeration and Knowledge search keep their
  existing typed routes. Broad generic record-query support remains out of
  scope.
- **RESOLVED — Mullet response mapping:** Change and Story list operations use
  the existing `ServiceNowReadEnvelope`. Snow `next_cursor` maps unchanged to
  opaque `continuation`; `complete=true` maps to `status="complete"`, otherwise
  `status="partial"`; `rows_inspected` is retained in the single upstream
  evidence item. Change's legacy `total` is retained only as a deprecated
  page-local alias of `returned` and is never presented as an exhaustive count.

### Open evidence blocker

- **B-RQ-001 — installed state choices are empty.** A read-only probe on
  2026-08-18 confirmed installed daemon contract `daemon-json-rpc-v1`, daemon
  version `0.2.0`, and advertised `field_choices`, but both
  `change_request.state` and `rm_story.state` returned successful empty choice
  arrays. No values were recorded. FND-RQ-000 is therefore open and T1 must not
  dispatch. The provider/credential/choice-read path must return non-empty
  typed choice sets for both tables; retrying unchanged empty results is not
  evidence.

### Ratification state

**NOT RATIFIED while B-RQ-001 is open.** The specification is structurally and
architecturally ready for ratification, but implementation is not ready to
dispatch until the FND-RQ-000 live preflight passes and the ratified spec is
recorded in an immutable Snow handoff commit.

## Scaffold inventory

| Seam ID | Authority refs | Crate / file / symbol | Signature / contract | Safe unresolved state | Owner | Completion evidence |
|---|---|---|---|---|---|---|
| SC-RQ-PREFLIGHT | `docs/spec-mullet-record-query-parity.md#state-choice-contract` | Installed daemon `field_choices` | Both target table/field pairs return non-empty typed choices without recording values | Typed blocked preflight `B-RQ-001`; no core implementation dispatch | Operator + QA owner | Public-safe count/type probe |
| SC-RQ-CORE | `docs/spec-mullet-record-query-parity.md#canonical-operation-contract` | `snow_core::resource::record_query` | `RecordQueryInput -> RecordQueryPage`, tagged only for Change Request or Story | Typed invalid-parameter or unresolved-choice error; never partial fake success or unverified state acceptance | Rust core coder | Core L0 fake tests |
| SC-RQ-RPC | `docs/spec-mullet-record-query-parity.md#scope`; `crates/snow_daemon/src/rpc/mod.rs`; `rpc/wire.rs` | `RpcMethod::RecordQuery` | JSON-RPC params map one-to-one to core input; page response preserves cursor/completeness | `-32602` invalid-parameter error for malformed/unknown input; method absent until core closure | Daemon coder | RPC dispatch/contract-info tests |
| SC-RQ-MCP | `docs/spec-mullet-record-query-parity.md#approved-goal`; `crates/snow_mcp/src/server.rs`; `tools/records.rs`; `daemon_bridge.rs` | records tool registry/direct server/bridge plus strict legacy parser | Same top-level object schema and page payload on direct/daemon-backed paths; both legacy paths reject `filter` | Typed tool-unavailable or invalid-parameter error; never ignored input | MCP coder | Direct/bridge parity, legacy rejection, and schema smoke |

## Task breakdown and coder handoffs

### T0: State-choice provider preflight

- System node: FND-RQ-000 provider evidence foundation
- Phase / Wave: Phase 0 / Wave 0
- Hard prerequisites: Foundation root
- Provides / consumes: Provides non-empty typed Change/Story state choices; consumes the installed daemon and approved target environment
- Closure gate: Both target table/field queries return at least one `{ value, label }` item and no error, with only counts/type checks retained
- Authority refs: `docs/spec-mullet-record-query-parity.md#state-choice-contract`; `crates/snow_core/src/context.rs`; `crates/snow_core/src/service/record.rs`
- Allowed write scope: None; this is a read-only preflight. Provider/ACL remediation requires a separate authorized operational task.
- Acceptance evidence: Installed-daemon `contract_info` advertises `field_choices`; `field_choices(change_request,state)` and `field_choices(rm_story,state)` return non-empty typed arrays. Current evidence fails this gate under B-RQ-001.
- Coder rule: Implement only the cited behavior. Surface every uncited path or solution as a blocker requiring an approved authority update.

### T1: Core live typed record-query foundation

- System node: FND-RQ-001 typed query and page foundation
- Phase / Wave: Phase 1 / Wave 1
- Hard prerequisites: FND-RQ-000 closure gate and immutable clean Snow handoff base containing this specification
- Provides / consumes: Provides typed Change/Story query and page contracts; consumes existing live Table API, record normalization, and Incident exact-choice/paging patterns
- Closure gate: L0 fake proves every accepted filter is enforced, every rejected input avoids I/O, and paging/completeness are consumer-visible
- Authority refs: `docs/spec-mullet-record-query-parity.md#scope`; `docs/spec-incident-list-by-assignment-group.md#scope`
- Allowed write scope: `crates/snow_core/src/resource/`, `crates/snow_core/src/service/record.rs`, `facade.rs`, public exports, and focused core consumer tests
- Acceptance evidence: Red-first Table API consumer tests for raw/unknown/cross-kind refusal, every exact Change and Story predicate, state correction including empty choices, full-page plus terminal-empty-page cursor behavior, default/maximum limit, exact compact/expanded projections, and no cache/vault mutation. Record each red failure reason before production edits.
- Coder rule: Implement only the cited behavior. Surface every uncited path or solution as a blocker requiring an approved authority update.

### T2: Daemon method and legacy false-filter refusal

- System node: CAP-RQ-002 daemon capability
- Phase / Wave: Phase 2 / Wave 2
- Hard prerequisites: FND-RQ-001 closure gate
- Provides / consumes: Provides canonical `record_query` and fail-closed legacy list parsing; consumes core typed request/page
- Closure gate: JSON-RPC consumer receives correct page/error payloads and `contract_info` advertises `record_query`
- Authority refs: `docs/spec-mullet-record-query-parity.md#scope`; `crates/snow_daemon/src/rpc/mod.rs`; `crates/snow_daemon/src/rpc/wire.rs`
- Allowed write scope: `crates/snow_daemon/src/rpc/{mod,wire,tests}.rs`, focused daemon integration tests, and daemon contract documentation
- Acceptance evidence: Red-first RPC tests for query success, malformed/unknown params, state correction, cursor, `contract_info`, and an old `{ filter: ... }` `list_records` request returning `-32602` before Table API I/O
- Coder rule: Implement only the cited behavior. Surface every uncited path or solution as a blocker requiring an approved authority update.

### T3: Direct and daemon-backed MCP parity

- System node: CAP-RQ-003 MCP parity capability
- Phase / Wave: Phase 3 / Wave 3
- Hard prerequisites: FND-RQ-001 and CAP-RQ-002 closure gates
- Provides / consumes: Provides `record_query` tool discovery/schema/execution on both transports and strict legacy `list_records` rejection; consumes the daemon/core page contract unchanged
- Closure gate: Direct and bridge consumer-visible parsed payloads equal one independent expected JSON value; both legacy MCP paths reject `filter`; schema smoke passes
- Authority refs: `docs/spec-mullet-record-query-parity.md#scope`; `crates/snow_mcp/src/tools/records.rs`; `crates/snow_mcp/src/daemon_bridge.rs`
- Allowed write scope: records tool schema, direct server, daemon bridge, capability documentation, and focused MCP tests
- Acceptance evidence: Red-first direct and bridge tests for tool list, valid page, invalid parameters, unsupported daemon method, direct legacy `{ filter: ... }` rejection before core I/O, bridged legacy rejection, and semantic JSON equality against an independent literal expectation; `scripts/mcp_schema_smoke.py` pass. Do not use serializer self-round-trips or raw JSON byte ordering as parity evidence.
- Coder rule: Implement only the cited behavior. Surface every uncited path or solution as a blocker requiring an approved authority update.

### T4: Coordinated Mullet adoption and runtime rollout

- System node: FEAT-RQ-004 Mullet consumer feature
- Phase / Wave: Phase 4 / Wave 4
- Hard prerequisites: CAP-RQ-002 and CAP-RQ-003 closure gates, a released/installed Snow daemon advertising `record_query`, and a clean immutable Mullet handoff base after reconciling the pre-existing dirty Story/shared-registration slice
- Provides / consumes: Provides truthful Mullet Change/Story reads; consumes Snow daemon discovery, typed request/page, and continuations
- Closure gate: Mullet uses only `record_query` for supported filtered workflows, rejects unavailable capability without fallback, and preserves Snow's partial/continuation semantics
- Authority refs: `docs/spec-mullet-record-query-parity.md#resolved-product-decisions`; Mullet repository `docs/spec-snow-via-mullet.md#DEC-SNOW-005`; `#DEC-SNOW-007`
- Allowed write scope: Exact Mullet paths listed under `docs/spec-mullet-record-query-parity.md#allowed-changes`; no Snow production code changes in this task
- Acceptance evidence: Red-first registered-operation tests for unavailable capability, exact Change/Story request mapping, partial/complete continuation mapping, compact/expanded projection, Story lookup migration, and rejection of every remaining `list_records.filter` route before adapter I/O; installed-daemon smoke pages to `status="complete"` using public-safe evidence
- Coder rule: Implement only the cited behavior. Surface every uncited path or solution as a blocker requiring an approved authority update.

## Verification and closure

- Spec structure: `python3 /Users/jared/.openclaw/workspace/foreman/scripts/validate_spec_contract.py docs/spec-mullet-record-query-parity.md` must pass.
- Red-first evidence: each new behavior begins with its named L0 consumer test failing for the current missing/false-filter behavior; tests named only for derives, DTO construction, or mock call counts are not acceptable.
- Core/daemon/MCP focused gates: run the focused tests created for T1–T3, including direct/bridge payload parity and no-I/O invalid-parameter cases.
- Workspace gates: `cargo fmt --check`, `cargo test --workspace --all-targets`, `cargo clippy --workspace --all-targets -- -D warnings`, and `git diff --check`.
- MCP runtime proof: after daemon availability, run `python3 -B scripts/mcp_schema_smoke.py -- <installed snow_mcp_bridge command>` and verify `record_query` is exposed with an object schema.
- Installed/runtime proof: query an approved environment through the installed daemon, page until `complete: true`, and record only generic count/cursor/completeness evidence. Fixtures, source tests, and cache contents are not live proof.
- Mullet closure: requires the downstream Mullet task and active daemon capability; Snow repository completion alone does not close user-facing Mullet parity.
- Stub closure: every scaffold seam is implemented with its cited proof, or explicitly remains a typed blocked/untrusted state. Nothing is complete merely because it compiles.
