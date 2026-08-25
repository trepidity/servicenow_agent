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
| T-OPS-02 cache-policy foundation | Closed after installed-scale amendment: Knowledge rebuild requires one explicit typed base scope and omission performs zero Knowledge I/O | COMPLETE |
| T-OPS-03 resource reads | Closed: live-only Incident get/query, strict filters, native projection, cursor paging, exact errors, and four-transport parity pass source/fake gates | COMPLETE |
| T-OPS-04 governed writes | Closed: compatible single and finite bulk governed Incident writes pass source/local-fake gates | COMPLETE |
| FEAT-OPS-CONTRACT source implementation closure | Closed: approved rows, the Knowledge rebuild-scope amendment, and aggregate source/local-fake gates pass | COMPLETE |
| OPS-ATTEST-01 installed/live attestation | Not claimed; operator-owned after installation and policy enablement | SEPARATE — not an implementation or Mullet gate |

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
| FEAT-OPS-CONTRACT | Feature | `docs/spec-servicenow-operational-capabilities.md#approved-goal`; `AGENTS.md#public-safe-content` | Phase 4 | Final source wave | Public-safe discoverable operational contract and bounded source-readiness statement | CAP-OPS-READ; CAP-OPS-WRITE | Approved selected-row closures and aggregate source gates | Every approved source row is behaviorally proven; installed and live claims remain explicitly separate |

Hard `requires` edges override desired delivery order and parallelism. Direct
MCP does not prove daemon-backed MCP, source does not prove installed runtime,
and registry presence does not prove a callable operation.

## Traceability matrix

| Authority ref | Behavior / decision | Task ID | Implementation seam | Acceptance evidence | Owner |
|---|---|---|---|---|---|
| `docs/spec-servicenow-operational-capabilities.md#retained-compatibility-surfaces`; `AGENTS.md#behavioral-test-seams` | Reconcile shipped transports, retain bounded legacy attachment upload, select attachment list, and remove the unimplemented catalog-cancel advertisement | T-OPS-05 | CLI help/commands; daemon `contract_info` and dispatcher; MCP registries/dispatchers/bridge; capability docs | Red-first L0 inventory fails on current ghost/mismatch state, then proves exact classified inventory and no unknown-route result for any advertised operation | Rust coder + QA |
| `docs/spec-servicenow-operational-capabilities.md#scope`; `crates/snow_daemon/src/rpc/handlers/system.rs#contract_info` | Discover selected capability metadata without hardcoded field guesses | T-OPS-01 | `snow_core` resource descriptor; daemon metadata dispatch; CLI and MCP exposure | Independent CLI, JSON-RPC, direct MCP, and bridge fixtures assert discovered fields, choices, references, paging support, and raw/display representation | Rust coder + QA |
| `docs/spec-servicenow-operational-capabilities.md#scope`; `crates/snow_core/src/config.rs#cacheconfig` | Replace broad work-record population/read behavior with named cache policy and atomic policy lifecycle | T-OPS-02 | Config/policy, cache rebuild, live persistence, record reads/search, daemon policy holder and fixed CLI commands | Compiled CLI/daemon with local ServiceNow fake proves approved defaults, no work-record cache I/O, source/completeness, miss/stale behavior, invalidation, and failed reload retention | Rust coder + QA |
| `docs/spec-servicenow-operational-capabilities.md#scope`; approved B-OPS-01 row; approved B-OPS-07 contract | Deliver one typed read family without local access narrowing | T-OPS-03 | Row-named resource/core/daemon/CLI/MCP/bridge modules | L0 fake proves ACL-visible records, absent missing fields, native paging, truthful completeness, exact errors, state correction, and no cache I/O | Rust coder + QA |
| `docs/spec-servicenow-operational-capabilities.md#scope`; approved B-OPS-01 write row; approved B-OPS-08 contract | Deliver one fail-closed governed write family with compatible single-target and finite bulk operations | T-OPS-04 | Row-named planner/applier, daemon, CLI, MCP policy/bridge, receipt/audit modules | Red-first governed-write L0 proves default denial, explicit enablement, confirmation, target bounds, replay/concurrency, durable partial receipts, invalidation failure, redaction, and zero write I/O on every preflight denial | Rust coder + QA |

## Implementation boundary

### Handoff manifest

| Field | Value |
|---|---|
| Immutable base | `622a9258d959afea035ce75e4c52ba888d7d8db0` |
| Ready for implementation | None |
| Source complete | T-OPS-05, T-OPS-01, T-OPS-02, T-OPS-03, and T-OPS-04 |
| Decision-blocked | None for the two approved Incident rows |
| Dependency-blocked | None for the two approved Incident rows |
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
- T-OPS-02: `crates/snow_core/src/{cache/,config.rs,context.rs,facade.rs,query/,resource/catalog.rs,service/cache_rebuild.rs,service/record.rs,service/knowledge.rs,service/business_application/,service/server.rs,service/write.rs}`;
  the required typed catalog-product projection under
  `crates/snow_core/src/cache/store/`; `crates/snow_daemon/src/catalog_write.rs`;
  `crates/snow_mcp/src/server.rs` and focused direct/daemon-backed MCP process
  parity tests for the two catalog read operations;
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

- **B-OPS-06 — cache-policy wire and lifecycle contract: CLOSED 2026-08-20.**
  The approved contract is recorded in `#approved-b-ops-06-cache-policy-contract`.
  T-OPS-02 source closure passed on 2026-08-20; installed/live attestation is
  separately operator-owned.
- **B-OPS-07 — Incident get/query contract: CLOSED 2026-08-20.** The approved
  contract is recorded in `#approved-b-ops-07-incident-read-contract`.
  T-OPS-03 source/fake closure passed on 2026-08-20; installed/live attestation
  remains separately operator-owned.
- **B-OPS-08 — Incident bulk-update contract: CLOSED 2026-08-20.** The approved
  contract is recorded in `#approved-b-ops-08-incident-write-contract`.
  T-OPS-04 has no remaining design or dependency blocker and is source-complete
  against local ServiceNow and state-store fakes.

### Approved B-OPS-06 cache-policy contract

Approved 2026-08-20. This contract closes the cache-policy design gap; current
source implementation status is recorded by the readiness decision below.

Installed-scale amendment approved 2026-08-21: Knowledge cache rebuild is
bounded by an explicit typed Knowledge-base scope in the same fixed policy.
An absent Knowledge rebuild scope performs no `kb_knowledge` I/O; it never
means "all ACL-visible articles." This amendment changes rebuild breadth only,
not live/read-through lookup authorization or ServiceNow ACL behavior.

#### Source and effective-policy construction

- The source is the fixed file `<resolved Snow config directory>/cache-policy.toml`.
  The daemon and offline cache commands resolve and capture that path at process
  startup. No CLI or JSON-RPC request accepts a path.
- An absent file activates the built-in defaults. An existing but unreadable or
  invalid file fails daemon startup; a running daemon retains its prior active
  snapshot when validation or reload fails.
- Built-in object defaults are `knowledge = 7d`,
  `business_application = 30d`, and `service_catalog_product = 30d`, all in
  `read_through` mode. Server reads are live-only unless an explicit policy
  entry enables caching.
- File entries override the exact built-in object or operation entry. A
  `live` override explicitly disables caching. After composing defaults and
  overrides, an operation/object pair with no rule is live-only.
- An object name is a canonical snake-case typed resource key registered by
  Snow, never a table name supplied by a caller. An operation name is a
  canonical daemon operation. An operation override must name the operation's
  registered object; unknown objects, unknown operations, mismatched pairs,
  duplicate entries, and wildcards are invalid. Policy may restrict an
  operation but cannot register or widen one.

The strict TOML schema is versioned and rejects unknown fields:

```toml
version = 1

[objects.server]
mode = "read_through"
ttl = "24h"

[operations.server_get]
object = "server"
mode = "cache_only"
ttl = "24h"
```

`objects.<object>` supplies the default for registered operations bound to that
object. `operations.<operation>` is an exact override and must repeat its
registered object. New operations receive no implicit operation override; they
may consume an approved object default only when their typed registration binds
them to that object.

#### Modes, freshness, and rebuild

| Mode | Contract |
|---|---|
| `live` | Performs zero cache, vault, or derived-index reads and zero persistence. `ttl` is forbidden. |
| `read_through` | Returns a fresh cache hit. A miss or stale entry reads ServiceNow; a successful complete live projection is persisted. A live failure remains an upstream error and never silently falls back to stale data. |
| `cache_only` | Never contacts ServiceNow. Any existing projection, including one older than TTL, is returned with mandatory `last_refreshed_at`; absence returns `CACHE_MISS`. |

Cached-mode TTLs are required positive integer durations using `s`, `m`, `h`,
or `d`, with an inclusive range of `1m` through `365d`. Offline
`snow rebuild-cache` projects only effective `read_through` or `cache_only`
rules and uses the same fixed source and validation. SQLite remains disposable
derived state; this contract adds no migration path. `refresh_all` remains
unavailable.

Knowledge rebuild uses this optional strict section:

```toml
[rebuild.knowledge]
knowledge_base_sys_id = "0123456789abcdef0123456789abcdef"
```

- `knowledge_base_sys_id` is exactly one 32-hex ServiceNow reference,
  normalized to lowercase. Empty, malformed, unknown, wildcard, display-name,
  table-name, and raw encoded-query inputs are invalid or unavailable; this
  contract creates no generic query surface.
- When the section is absent, offline and daemon-triggered rebuilds omit
  Knowledge and perform zero `kb_knowledge` I/O. Other cache-policy objects and
  ordinary Knowledge reads are unchanged.
- When it is present and Knowledge's effective mode is cached, rebuild applies
  exact `kb_knowledge_base=<sys_id>` equality on the ServiceNow `kb_knowledge`
  table before fixed `sys_id` ordering and pagination. Rows from every other
  Knowledge base are outside the rebuilt projection.
- The normalized rebuild scope participates in the policy fingerprint and
  atomic validate/reload snapshot. Validation and public summaries never emit
  the configured reference value.
- Deployment-specific Knowledge-base references belong only in the ignored
  local `cache-policy.toml`; committed examples and tests use generic values.

#### Lifecycle wire contract

- CLI commands are `snow cache-policy validate [--json]` and
  `snow cache-policy reload [--json]`.
- Daemon JSON-RPC methods are `cache_policy_validate` and
  `cache_policy_reload`. Each accepts exactly `{}`; unknown arguments are
  JSON-RPC `-32602`.
- Validate parses and materializes the effective snapshot without changing
  daemon state. Reload parses completely and atomically swaps only the
  cache-policy snapshot.
- Validate returns exactly `version`, `source`, `rule_count`, and `fingerprint`.
  Reload returns exactly `version`, `source`, `rule_count`,
  `previous_fingerprint`, `fingerprint`, and `changed`.
- `source` is `built_in_defaults` or `built_in_plus_file`. `fingerprint` is the
  lowercase SHA-256 of the canonical, key-sorted effective rules, so formatting
  or TOML key order cannot manufacture a change.
- JSON-RPC `-32070` carries
  `{ "code": "CACHE_POLICY_INVALID", "field"?, "rule"?, "reason" }`.
  JSON-RPC `-32071` carries
  `{ "code": "CACHE_POLICY_IO", "kind", "reason" }` without the path or file
  contents. A cache-only read miss uses JSON-RPC `-32072` with
  `{ "code": "CACHE_MISS", "operation", "object" }`.

#### Mutation invalidation

- A successful mutation that returns the complete current record replaces that
  exact cached projection and its successful live-refresh timestamp.
- Otherwise, every known target is removed by canonical object key and
  `sys_id` from memory cache, SQLite projection, vault projection, and derived
  indexes before Snow reports local cache coherence.
- If a successful bulk mutation cannot identify every affected target, Snow
  invalidates only the exact object segment across those same local layers,
  never the entire cache.
- B-OPS-08 owns the externally visible write outcome when ServiceNow succeeds
  but subsequent local invalidation fails. No automatic write retry is allowed.

#### Approved T-OPS-02 catalog projection amendment

Approved 2026-08-20. T-OPS-02 may modify
`crates/snow_core/src/service/write.rs`,
`crates/snow_core/src/resource/catalog.rs`, the required catalog-product
projection files under `crates/snow_core/src/cache/store/`, and
`crates/snow_daemon/src/catalog_write.rs`.

- Catalog products use a typed current-format projection containing the complete
  `CatalogItem`, including variables and choices.
- A cache lacking that typed projection is incompatible with this source and
  fails closed. Recovery is `reset-cache` or ServiceNow-authoritative
  `rebuild-cache`; no migration, legacy backfill, or generic-row promotion is
  added.
- `catalog_item_get` may return a cached result only from that complete typed
  projection. A generic `sc_cat_item` row is never represented as complete.
- Narrowed rebuild or `catalog_items_search` projections report
  `Partial/NarrowedProjection`, including the mandatory cached refresh time,
  and never masquerade as complete catalog items.

#### Approved T-OPS-02 direct-MCP parity amendment

Approved 2026-08-20. T-OPS-02 may modify
`crates/snow_mcp/src/server.rs` and focused direct/daemon-backed MCP process
parity tests for `catalog_items_search` and `catalog_item_get`.

- Direct MCP uses the same policy-aware catalog envelope entry points and exact
  public error mapping as daemon JSON-RPC and daemon-backed MCP.
- Independently authored process-level expectations prove byte-identical
  structured payloads across direct and daemon-backed MCP for `live`,
  `read_through`, and `cache_only`.
- Parity proof includes complete typed `CatalogItem` results, narrowed search
  with mandatory cached refresh time, generic-row rejection/`CACHE_MISS`, and a
  stale live-refresh failure with no stale fallback.
- This amendment changes no operation inventory, policy enablement, write
  behavior, or Mullet consumer path.

T-OPS-02 closure requires red-first L0 proof through compiled CLI and real
daemon JSON-RPC seams for built-in defaults, strict schema rejection, each mode,
fresh/miss/stale behavior, zero I/O for `live`, cache-only no-network behavior,
offline rebuild scope, exact and segment invalidation, validate no-state-change,
atomic reload, and prior-snapshot retention after every invalid reload.

### Approved B-OPS-07 Incident read contract

Approved 2026-08-20. This contract closes the Incident get/query design gap; it
is implemented in current source as recorded by the readiness decision below.
Installed-runtime and live ServiceNow behavior remain separate claims.

#### `incident_get` request and lookup behavior

`incident_get` accepts exactly one selector and rejects unknown properties:

```json
{ "number": "INC0012345" }
```

or:

```json
{ "sys_id": "0123456789abcdef0123456789abcdef" }
```

- `number` is normalized to uppercase and must match `^INC[0-9]+$`.
- `sys_id` must contain exactly 32 hexadecimal characters and is normalized to
  lowercase.
- Supplying neither selector or both selectors is invalid before ServiceNow
  record I/O.
- A number lookup requests at most two rows. Two rows are
  `INCIDENT_NUMBER_AMBIGUOUS`; Snow never picks one. An empty number result is
  `INCIDENT_LOOKUP_UNAVAILABLE`, because an empty Table API query does not prove
  whether the record is absent or hidden by a row ACL. A direct `sys_id` lookup
  that ServiceNow explicitly reports absent is `INCIDENT_NOT_FOUND`.
- The operation is always live and performs zero cache, vault, or index I/O.
  It requests display values for every field ServiceNow exposes and applies no
  caller field projection. Missing upstream fields remain absent.

The successful data shape is exactly `{ "record": <IncidentRecord> }` inside
`OperationEnvelope`, with operation `incident_get`, source `{ "kind": "live" }`,
and completeness `{ "kind": "complete" }`. `IncidentRecord` is a map keyed by
native ServiceNow field name. Each present field retains raw `value` and
optional `display_value`; Snow does not synthesize absent fields or flatten
references.

#### `incident_query` request contract

The request accepts only `filters`, `limit`, and `cursor`; every property is
optional, so `{}` is the bounded query for all ACL-visible Incidents. `filters`
defaults to `{}` and rejects unknown properties. There is no caller-selected
table, raw encoded query, field list, cache/source mode, or sort.

| Filter | Type and bound | ServiceNow predicate |
|---|---|---|
| `numbers` | 1..=20 unique values matching `^INC[0-9]+$`, normalized uppercase | exact `number` equality or `IN` |
| `assignment_group` | 32-hex `sys_id` | exact reference equality |
| `assigned_to` | 32-hex `sys_id` | exact reference equality |
| `caller_id` | 32-hex `sys_id` | exact reference equality |
| `cmdb_ci` | 32-hex `sys_id` | exact reference equality |
| `states` | 1..=20 unique raw values or case-insensitive exact labels | live `incident.state` choice resolution, then exact equality or `IN` |
| `priorities` | 1..=5 unique integers, each in `1..=5` | exact equality or `IN` |
| `active` | boolean | exact boolean equality |
| `opened_after` / `opened_before` | ServiceNow UTC timestamp `YYYY-MM-DD HH:MM:SS` | strict `opened_at >` / `<` bounds |
| `updated_after` / `updated_before` | ServiceNow UTC timestamp `YYYY-MM-DD HH:MM:SS` | strict `sys_updated_on >` / `<` bounds |

Array values must remain unique after normalization or state resolution. Empty
strings, invalid calendar timestamps, inverted or equal time ranges, invalid
references, and unknown properties are invalid before the Incident record
request. Raw state values match case-sensitively; labels match
case-insensitively and exactly. Unknown, ambiguous, duplicate-after-resolution,
or unavailable choices return correction data containing the requested value,
table, field, ambiguity status, and current choices. Snow never accepts an
unverified raw state when the live choice set is unavailable or empty.

`limit` defaults to 50 and accepts integers `1..=200`; values outside the range
are rejected rather than clamped. `cursor` is omitted/null on the first page or
is the previous page's `next_cursor`: exactly 32 hexadecimal characters,
normalized to lowercase, and applied as exclusive `sys_id > cursor`.
Every query is ordered only by ascending `sys_id`. Caller-selected sorting is
forbidden because it would make the approved `sys_id`-only cursor untruthful.

#### Query projection and result

`incident_query` requests exactly these fields, with ServiceNow raw and display
values preserved:

```text
sys_id, number, short_description, state, active, priority, impact, urgency,
opened_at, resolved_at, closed_at, caller_id, assigned_to, assignment_group,
cmdb_ci, business_service, category, subcategory, sys_created_on,
sys_updated_on, sys_updated_by
```

Descriptions, work notes, comments, and arbitrary caller projection are not
query fields; callers use `incident_get` for the complete record returned by
their instance.

The successful data shape is exactly:

```json
{
  "records": [],
  "next_cursor": null,
  "limit": 50,
  "rows_inspected": 0
}
```

inside an `OperationEnvelope` whose operation is `incident_query` and source is
`{ "kind": "live" }`. `rows_inspected` is the number of rows ServiceNow
returned. Fewer rows than `limit` yields `{ "kind": "complete" }` and a null
cursor. Exactly `limit` rows yields
`{ "kind": "partial", "reason": "page_limit_reached" }`; `next_cursor` is
the last ServiceNow row's `sys_id`. An exact-multiple scan therefore requires a
terminal empty page to establish completeness. This is deterministic cursor
progression, not a transactional snapshot guarantee.

#### Errors and transport parity

| JSON-RPC code | Public code | Meaning |
|---|---|---|
| `-32602` | `INVALID_PARAMS` or `INCIDENT_STATE_UNRESOLVED` | Unknown/invalid input or state correction; no Incident record I/O |
| `-32001` | `SERVICENOW_UNAVAILABLE` | Network or timeout failure |
| `-32003` | `ACL_DENIED` | ServiceNow explicitly denied access |
| `-32004` | `INCIDENT_NOT_FOUND` | Direct `sys_id` lookup explicitly absent |
| `-32005` | `INCIDENT_NUMBER_AMBIGUOUS` | More than one row matched an exact number |
| `-32007` | `INCIDENT_LOOKUP_UNAVAILABLE` | Empty exact-number result cannot distinguish absence from row ACL filtering |
| `-32000` | `SERVICENOW_ERROR` | Other redacted upstream/service failure |

Upstream diagnostics pass through only after credential, token, URL, and other
secret redaction. An empty `incident_query` page is a successful complete page,
not a not-found error.

CLI forms are `snow incident get --number <INC...> --json`,
`snow incident get --sys-id <sys_id> --json`, and `snow incident query ...
--json`. Query flags map one-to-one to the approved filters; array flags are
repeatable. Daemon JSON-RPC, direct MCP, and daemon-backed MCP expose the same
operation names and exact request properties. JSON surfaces return
byte-identical envelope JSON for the same upstream fixture; CLI failures are
non-zero and preserve the same public code/correction data without secrets.

T-OPS-03 closure requires red-first L0 proof through the compiled CLI connected
over a real local socket to the daemon and a local ServiceNow fake, plus direct
and daemon-backed MCP parity. The proof covers both selectors, number
ambiguity/ACL uncertainty, fixed projection, missing-field omission, empty and
multi-page results including terminal exact-multiple paging, state correction,
unknown-input rejection before record I/O, explicit ACL distinction, and zero
cache/vault/index I/O. Independently authored wire expectations are L1
supplemental evidence; DTO, derive, constructor, mock-call, or self-round-trip
tests do not close the behavior.

### Approved B-OPS-08 Incident write contract

Approved 2026-08-20. This closes the Incident governed-write design gap. It
did not by itself claim that T-OPS-04 source or runtime behavior was implemented, and it
does not relax the hard dependency on T-OPS-03 closure.

#### Operations, compatibility, and policy

- The shipped `incident_plan_update` / `incident_apply_update` pair remains the
  compatible single-target operation. Its existing flat request stays accepted;
  T-OPS-04 adds `comments` to the exact permitted-field set but does not turn the
  ordinary operation into a bulk request. One target is within the universal
  ordinary-operation ceiling of two.
- The new separately named bulk pair is `incident_bulk_plan_update` /
  `incident_bulk_apply_update`. It accepts 3..=25 targets. Three targets sent to
  the ordinary operation are rejected before ServiceNow write I/O; 26 targets
  are rejected by bulk before target resolution or write I/O.
- Both bulk operations are disabled by default in every environment. Planning
  is refused unless the matching bulk apply tool is explicitly enabled for the
  named environment.
- `ToolPolicy` gains a distinct `max_targets`. A bulk request requires an
  explicitly configured integer in `3..=25`; omission denies bulk, an operator
  may lower the maximum, and a value above the row-approved ceiling is invalid.
  `max_records` is not a target-count control and must not be reused.
- Policy may narrow the five approved fields or lower the target count. It may
  not add a field, increase the maximum, bypass confirmation, or authorize a
  new operation.

#### Bulk plan request

`incident_bulk_plan_update` accepts exactly:

```json
{
  "shared_patch": {
    "assignment_group": "0123456789abcdef0123456789abcdef"
  },
  "targets": [
    {
      "number": "INC0012345",
      "patch": { "state": "In Progress" }
    },
    {
      "sys_id": "fedcba9876543210fedcba9876543210",
      "patch": { "work_notes": "Generic operator note" }
    },
    {
      "number": "INC0012347"
    }
  ]
}
```

`shared_patch` is optional. Each target accepts exactly one normalized
`number` or `sys_id` selector and an optional `patch`; unknown properties are
rejected. Every target must have at least one effective field after combining
the shared and target patch. A field may not appear in both patches for the
same target, so there is no hidden override order. Targets must be unique after
live resolution to canonical lowercase `sys_id`.

The only patch fields are `assigned_to`, `assignment_group`, `state`,
`work_notes`, and `comments`. Assignment references in the bulk form are exact
32-hex `sys_id` values; Snow does not narrow them by configured identity
membership or guess from a name. State accepts an exact raw value or an exact
case-insensitive live choice label, but cancellation values remain forbidden.
`work_notes` and `comments` are non-empty strings under the existing 16,000
character bound and retain ServiceNow append semantics. Empty patches, unknown
fields, malformed selectors, unavailable/ambiguous targets, unresolved states,
duplicates, overlap, target-count failure, or policy denial fail the complete
plan with no ServiceNow write I/O.

Planning performs live target reads through the T-OPS-03 contract, normalizes
the effective patches, captures each target's required `sys_updated_on`, sorts
the normalized targets by `sys_id`, and creates no mutation. There is no partial
plan.

The successful plan result is exactly:

```json
{
  "plan_id": "<opaque>",
  "op_hash": "<lowercase-sha256>",
  "apply_tool": "incident_bulk_apply_update",
  "preview": {
    "targets": [
      {
        "target": { "number": "INC0012345", "sys_id": "00000000000000000000000000000001" },
        "patch": { "state": "2" },
        "concurrency_token": { "sys_updated_on": "2026-08-20 12:00:00" }
      },
      {
        "target": { "number": "INC0012346", "sys_id": "00000000000000000000000000000002" },
        "patch": { "assignment_group": "0123456789abcdef0123456789abcdef", "work_notes": "Generic operator note" },
        "concurrency_token": { "sys_updated_on": "2026-08-20 12:01:00" }
      },
      {
        "target": { "number": "INC0012347", "sys_id": "00000000000000000000000000000003" },
        "patch": { "assignment_group": "0123456789abcdef0123456789abcdef" },
        "concurrency_token": { "sys_updated_on": "2026-08-20 12:02:00" }
      }
    ]
  },
  "expires_at": "<rfc3339>",
  "confirmation_token": "<opaque>",
  "idempotency_key": "<opaque>"
}
```

The plan uses the existing ten-minute lifetime. `op_hash` covers the apply tool,
actor/requester-independent canonical target order, each resolved target, each
effective normalized patch, and every concurrency token.

#### Apply, confirmation, and concurrency

`incident_bulk_apply_update` accepts exactly:

```json
{
  "plan_id": "<opaque>",
  "confirmation_token": "<opaque>",
  "idempotency_key": "<opaque>",
  "concurrency_tokens": [
    { "sys_id": "00000000000000000000000000000001", "sys_updated_on": "2026-08-20 12:00:00" },
    { "sys_id": "00000000000000000000000000000002", "sys_updated_on": "2026-08-20 12:01:00" },
    { "sys_id": "00000000000000000000000000000003", "sys_updated_on": "2026-08-20 12:02:00" }
  ]
}
```

The token array must contain every planned target exactly once and byte-match
the plan after canonical ordering. Apply accepts no selectors or patches. The
confirmation is bound to actor, requester, environment, apply tool, and
`op_hash`; the hash in turn binds the complete normalized targets, patches, and
concurrency tokens. A token or confirmation cannot be reused for a different
target, patch, actor, requester, environment, or operation.

Before the first PATCH, apply rechecks policy, kill switch, plan lifetime,
confirmation, idempotency, target count, and every target's current
`sys_updated_on`. Any preflight failure denies the whole apply and proves zero
ServiceNow write I/O. A matching idempotency replay returns the prior receipt
without another PATCH; a key bound to another hash is a conflict.

ServiceNow Table API PATCH does not provide a cross-record transaction. After
preflight, Snow processes targets in canonical ascending `sys_id` order,
rechecks the target token immediately before its PATCH, and stops at the first
write, concurrency, durable-receipt, or invalidation failure. It performs no
compensating rollback and never automatically retries a ServiceNow write.

#### Success, partial failure, and local coherence

A complete success returns one durable receipt with `status: "success"`. If at
least one target was applied and a later target fails, JSON-RPC returns
`-32046` with public code `PARTIAL_FAILURE` and the durable receipt in error
data. Each target result is exactly `applied`, `failed`, or `not_attempted`, so
an empty or superficially successful response cannot conceal a partial write.

After each successful PATCH, Snow applies the B-OPS-06 exact-target replacement
or invalidation contract before continuing. If ServiceNow succeeded but local
cache/vault/index coherence fails, Snow stops and returns the same `-32046`
partial path with `failure_code: "LOCAL_COHERENCE_FAILED"` and
`upstream_applied: true`. The receipt identifies the applied target; Snow does
not retry the write. If no target was applied, existing typed policy, field,
lookup, confirmation, idempotency, plan, concurrency, upstream, and pending-
resolution errors remain in force rather than being mislabeled partial.

The idempotency pending marker is durable before the first PATCH, and each
per-target outcome is durably appended before Snow attempts the next target.
Existing `PENDING_RESOLUTION_REQUIRED` semantics remain for a crash or local
store failure where Snow cannot prove a final receipt; callers must inspect the
target and create a fresh plan rather than blindly retry.

The bulk receipt contains exactly `plan_id`, `audit_id`, `parent_audit_id`,
`tool`, `status`, `op_hash`, `idempotency_replay`, `target_results`,
`applied_count`, `failed_count`, `not_attempted_count`, `cache_coherent`,
`apply_started_at`, and `completed_at`. Each target result contains its
`number`, `sys_id`, outcome status, changed field names and before/after hashes,
and observed `sys_updated_on`; a failed result also carries its typed redacted
error code. Receipts and audit rows contain no record snapshot, instance URL,
work-note/comment text, or other field body. Receipt replay follows the existing
idempotency window; audit retention follows the existing configured
`McpAuditConfig.retention_days`. This contract adds no second retention store.

#### CLI, MCP, and proof

- CLI planning is `snow incident bulk-update --request <path|-> [--json]`.
  `-` reads JSON from stdin so journal text need not enter process arguments.
- Applying a saved plan bundle is
  `snow incident bulk-update --plan <path|-> --apply [--yes] [--json]`.
  `--request` and `--plan` are mutually exclusive; `--yes` is valid only with
  `--apply` and bypasses interaction only after printing the exact preview.
- Daemon JSON-RPC, direct MCP, and daemon-backed MCP expose
  `incident_bulk_plan_update` and `incident_bulk_apply_update` with the exact
  request/result contracts above. Direct and bridged MCP return byte-identical
  structured results for the same fixture.

T-OPS-04 closure requires red-first governed-write L0 tests through the
compiled CLI over a real daemon socket with local ServiceNow and state-store
fakes, plus daemon JSON-RPC and direct/bridge MCP parity. Required mutations
include removing default denial, accepting three ordinary or 26 bulk targets,
ignoring a patch overlap, skipping all-target preflight, applying with a stale
token, reusing confirmation/idempotency authority for another hash, continuing
after a failed target, collapsing partial into success, retaining journal text
in a receipt, skipping invalidation, or retrying after local-coherence failure.
Every preflight denial asserts zero ServiceNow write I/O and unchanged prior
state; partial paths assert the exact applied/failed/not-attempted state and
durable replay without another write. DTO, schema, derive, constructor,
mock-call, or self-round-trip assertions do not close the governed-write seam.

### Approved B-OPS-01 rows

Approved 2026-08-20. These two rows, and only these two, select the T-OPS-03 and
T-OPS-04 families. B-OPS-07 and B-OPS-08 are closed; T-OPS-03 source/fake
closure passes and T-OPS-04 source/local-fake closure now passes. Every other selected family stays
row-gated.

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
| Named bulk | `incident_bulk_plan_update` / `incident_bulk_apply_update`, 3..=25 targets, separately enabled with `max_targets: 25`. Omission denies bulk. |
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
| SC-OPS-INCIDENT-READ | `docs/spec-servicenow-operational-capabilities.md#approved-b-ops-07-incident-read-contract` | Incident resource/core/daemon/CLI/MCP/bridge seams | Exclusive get selector; strict typed live query; fixed projection; exclusive `sys_id` paging; exact errors and envelope | Typed unavailable/error for invalid, unresolved, or ACL-uncertain lookup; never cache fallback or synthetic empty get | Rust read coder | Red-first compiled CLI/real-daemon L0 plus direct/bridge MCP parity and terminal paging |
| SC-OPS-WRITE-GUARD | `docs/spec-servicenow-operational-capabilities.md#scope`; `crates/snow_mcp/src/domain/policy.rs#toolpolicy` | Shared governed-write enablement and target counter | Disabled by default; two targets maximum unless named bulk policy supplies row-approved finite maximum; CLI flags cannot bypass | Policy refusal before ServiceNow I/O and no synthetic receipt | Rust write/MCP coder | Red-first L0 fake asserts exact denial, no I/O, and receipt/audit outcomes |
| SC-OPS-INCIDENT-BULK | `docs/spec-servicenow-operational-capabilities.md#approved-b-ops-08-incident-write-contract` | Incident bulk planner/applier, policy, durable receipt/audit, invalidation, CLI/RPC/MCP/bridge | Strict shared/per-target patch plan; canonical targets/tokens; stop-on-first non-atomic apply; durable success/partial receipt | Typed fail-closed denied, pending-resolution, or partial state; never synthetic success, rollback claim, or automatic write retry | Rust write coder | Red-first governed-write L0 with exact partial state, replay, local-coherence failure, zero-I/O denials, and transport parity |

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
  approved B-OPS-01 row; B-OPS-07 closure.
- Provides / consumes: provides one named typed read/query family; consumes
  discovered metadata and the shared envelope/cache contracts.
- Closure gate: the approved operation works through every selected transport,
  preserves native values, uses only native paging, and reports source and
  completeness truthfully.
- Authority refs: `docs/spec-servicenow-operational-capabilities.md#scope`;
  approved B-OPS-01 row amendment;
  `docs/spec-servicenow-operational-capabilities.md#approved-b-ops-07-incident-read-contract`.
- Allowed write scope: only row-named modules/tests; no wildcard table registry.
- Acceptance evidence: red-first compiled CLI through a real daemon socket and
  local ServiceNow fake; both get selectors and ambiguity/ACL uncertainty;
  fixed projection and missing fields; state correction; invalid input before
  record I/O; terminal cursor paging; exact parity through direct and bridged
  MCP; and negative no-cache/vault/index-I/O proof.
- Coder rule: Implement only the cited behavior. Surface every uncited path or solution as a blocker requiring an approved authority update.

### T-OPS-04: Add one approved fail-closed governed write family

- System node: CAP-OPS-WRITE governed write row.
- Phase / Wave: Phase 3 / row-specific wave.
- Hard prerequisites: Foundation closures; target CAP-OPS-READ row closure;
  approved B-OPS-01 write row; B-OPS-08 closure.
- Provides / consumes: provides one disabled-by-default plan/apply family and
  policy enablement; consumes resource metadata, envelopes, receipt/audit, and
  target-count guards.
- Closure gate: L0 proves permitted fields, default denial, explicit environment
  enablement, confirmation, redacted upstream failure, receipt/audit, two-target
  default, finite named bulk, replay/concurrency, and CLI flag non-bypass.
- Authority refs: `docs/spec-servicenow-operational-capabilities.md#scope`;
  approved B-OPS-01 write row;
  `docs/spec-servicenow-operational-capabilities.md#approved-b-ops-08-incident-write-contract`.
- Allowed write scope: only row-named modules/tests.
- Acceptance evidence: red-first compiled CLI/real-daemon governed-write seam
  with local ServiceNow/state-store fakes; exact single-target compatibility,
  bulk plan/apply, policy/target bounds, confirmation/idempotency/concurrency,
  durable success/partial replay, stop-on-first behavior, local-coherence
  failure, redaction, direct/bridge MCP parity, and zero write I/O for every
  preflight denial.
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
- T-OPS-03 Incident reads: assert exclusive selectors, number ambiguity versus
  ACL uncertainty, fixed typed filters/projection, state correction, exact
  errors, missing-field omission, exclusive terminal paging, transport parity,
  and zero cache/vault/index I/O.
- T-OPS-04 writes: assert explicit enablement, permitted fields, confirmation,
  idempotency, replay, applicable concurrency, finite target bounds, all-target
  preflight, deterministic stop-on-first behavior, durable success/partial
  audit and receipts, local-coherence failure, upstream failure, redaction, and
  zero write I/O for every preflight denial.
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
- **T-OPS-02: COMPLETE** on 2026-08-20 for current source and local-fake
  evidence. Fixed-source strict policy validate/reload, atomic prior-snapshot
  retention, daemon startup failure, the four default TTLs, `live`,
  `read_through`, and `cache_only`, truthful source/completeness/cache age,
  Server/Knowledge/Business Application/Service Catalog reads, rebuild, and
  exact/segment invalidation are proven through compiled CLI and daemon L0
  seams. The current-format typed catalog projection preserves variables and
  choices, rejects generic rows as `CACHE_MISS`, and direct/daemon-backed MCP
  process tests prove byte-identical catalog envelopes and public errors,
  including narrowed timestamps and no stale fallback. The post-T-OPS-04
  aggregate workspace/Snow-owned E2E gates passed on 2026-08-21; installed/live
  attestation remains separately operator-owned. Installed-scale evidence on
  2026-08-21 exposed an unbounded Knowledge rebuild; the approved amendment now
  requires one exact policy-selected Knowledge base, omits Knowledge with zero
  I/O when unscoped, and fingerprints the redacted scope. Compiled CLI durable-
  cache tests and daemon lifecycle tests pass, and mutations removing the
  filter, the omission guard, or sys_id validation are killed.
- **T-OPS-03: COMPLETE** on 2026-08-20 for current source and local-fake
  evidence. `incident_get` and `incident_query` are live-only, reject unknown
  or ambiguous input before Incident record I/O, preserve native raw/display
  fields, use the fixed query projection and exclusive `sys_id` cursor, and
  report exact errors and truthful completeness. Compiled CLI, daemon JSON-RPC,
  direct MCP, and real-socket daemon-backed MCP pass parity evidence. Incident
  cache-policy entries are rejected, so cached modes cannot widen the row.
  Aggregate workspace/Snow E2E passed on 2026-08-21; installed/live attestation
  remains separate.
- **T-OPS-04: COMPLETE** on 2026-08-20 for current source and local-fake
  evidence. The compatible single-target path and separately named 3..=25
  target bulk family are fail-closed, use no-retry mutation transport, enforce
  exact confirmation/idempotency/concurrency bindings, stop on first failure,
  durably retain public-safe receipts/audits, and strictly invalidate legacy
  Incident projections after successful PATCHes. Compiled CLI, daemon JSON-RPC,
  direct MCP, and real-socket daemon-backed MCP behavior pass focused evidence.
  Aggregate workspace/Snow-owned E2E passed on 2026-08-21 with 816 tests passed,
  zero failed, and six intentionally ignored; strict Clippy, formatting, source
  graph, spec, diff, public-safety, and the local 83-tool MCP schema smoke also
  passed. Installed/live attestation remains separate and is not claimed by
  this source closure.
- **FEAT-OPS-CONTRACT source implementation: COMPLETE.** Every approved source
  row and aggregate implementation gate is closed. Mullet is a downstream
  consumer and is neither a dependency nor a closure gate.
- **OPS-ATTEST-01 installed/live attestation: NOT CLAIMED.** Rebuilding and
  restarting the installed daemon, enabling operator policy, and live
  ServiceNow proof are deployment evidence owned separately by the operator;
  their absence does not reopen source implementation.

This is a bounded readiness decision. It authorizes consumer handoff from the
current source, but no live ServiceNow access or deployment policy change.
