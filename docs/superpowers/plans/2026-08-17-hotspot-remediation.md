# Implementation Spec: Rust Hotspot Remediation

Status: implementation-ready.

Spec validation: passed on 2026-08-17 with the command recorded under
`#verification-and-closure`.

Immutable planning base: `742af261f6a6fe837bcd478a572977938dd5c9c0`.

## Authority and scope

### Approved goal

Remediate the five confirmed Rust source hotspots without rewriting the
product or changing its consumer contracts:

- `crates/snow_daemon/src/rpc.rs`
- `crates/snow_core/src/cache/store.rs`
- `crates/snow_core/src/service/business_application.rs`
- `crates/snow_cli/src/main.rs`
- `crates/snow_daemon/src/story_write.rs`

The target is a feature-oriented, ports-and-adapters structure with thin
entrypoints, typed boundaries, narrow visibility, and behavior verified at
consumer seams. A smaller file is not sufficient evidence; each resulting
module must have one coherent reason to change.

The direct user decisions recorded by this document are binding:

1. The project must remediate the identified god-file hotspots according to
   the repository's Rust engineering standards.
2. The local SQLite projection has **no migration capability**. There are no
   forward migrations, backward migrations, compatibility shims, conditional
   schema upgrades, or legacy-schema fixtures. An incompatible cache is
   discarded and rebuilt in the current format.
3. SQLite is disposable derived state. It is not the authority for records,
   governed write plans, confirmations, idempotency, audit evidence, or other
   durable control state.

This section is the canonical repository record of the user-approved goal and
the no-migration decision because the repository has no accepted requirement
ID scheme for this architectural remediation.

### Governing authority

- `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#approved-goal` —
  approved hotspot scope and the binding no-migration decision.
- `Cargo.toml` — existing four-crate workspace and dependency direction.
- `crates/snow_cli/src/main.rs:83-306` — CLI entry, runtime construction, and
  top-level command dispatch currently share one file with feature handlers.
- `crates/snow_daemon/src/rpc.rs:31-3394` — JSON-RPC wire types, server
  lifecycle, method registry, dispatch, feature handlers, parameter parsing,
  cache inspection, rendering, and error mapping share one production module.
- `crates/snow_core/src/cache/store.rs:1-5203` — SQLite schema management,
  legacy migrations, row types, and repositories for several domains share one
  production module.
- `crates/snow_core/src/service/business_application.rs:108-3250` — Business
  Application search, graph traversal, fallback, persistence, dictionary,
  reference, and rendering concerns share one production module.
- `crates/snow_daemon/src/story_write.rs:46-3402` — story planning, applying,
  scope, field governance, identity resolution, recovery, confirmation, rate
  limiting, and audit behavior share one production module.
- `crates/snow_core/src/service/vault.rs:84-111` — the current rebuild path
  upserts vault documents into an already-open cache and therefore is not yet
  an incompatible-cache recovery path.
- `crates/snow_core/src/facade.rs:680-716` — normal `SnowCore` construction
  opens the SQLite projection before commands can execute.
- `README.md#business-applications` and
  `docs/MCP_CAPABILITIES.md#local-cache-schema-v11` — current public
  documentation incorrectly promises forward migration after the approved
  no-migration decision.
- `AGENTS.md#public-safe-content` — every changed source, test, fixture,
  snapshot, and document must remain public-safe.

### Scope

- Preserve the existing four Cargo crates and their public binary names.
- Decompose each named hotspot by responsibility behind stable facades and
  deliberate re-exports.
- Remove all SQLite migration code, migration tests, and forward-migration
  documentation.
- Define one current cache format and explicit incompatible-cache behavior.
- Make `snow rebuild-cache` usable without opening the existing cache or
  constructing a normal `SnowCore` first.
- Keep the daemon's current-format rebuild path safe while its SQLite
  connection is open; do not replace an in-use database file.
- Declare the project's L0 consumer seams before restructuring tests.
- Preserve CLI flags and output contracts, daemon JSON-RPC method names and
  payloads, direct and daemon-backed MCP payload parity, ServiceNow query
  semantics, vault paths, redaction, write governance, and public Rust APIs.
- Move existing tests with their owning behavior and add tests only where a
  changed behavior or an uncovered consumer contract earns one.
- Update cache documentation to describe disposable current-format state and
  rebuild requirements truthfully.

### Non-goals

- No new Cargo crate, framework, dependency-injection container, code
  generator, procedural macro, or generic RPC framework.
- No ServiceNow API behavior change, new CLI/MCP/RPC capability, policy schema
  change, live write, deployment change, credential change, or environment
  change.
- No migration capability of any kind, including a one-time v11 conversion.
- No automatic deletion of an incompatible cache during ordinary startup.
- No migration or preservation of the separate daemon stores used for plans,
  confirmations, idempotency, or audit evidence.
- No broad cleanup of other large files such as `service/record.rs`,
  `service/knowledge.rs`, `query/mod.rs`, `tui/fetch.rs`, `tui_client.rs`, or
  `snow_mcp/src/server.rs`; those require separate authority and evidence.
- No behavior change hidden inside a move-only refactor.
- No paper modules created solely to reduce a line count, and no test-only
  extraction presented as production architecture remediation.

## Architectural target

The dependency direction within the existing workspace is:

```text
CLI / daemon JSON-RPC / MCP inbound adapters
                    |
                    v
          snow_core application services
                    |
                    v
       typed domain rules and capability ports
                    |
                    v
   ServiceNow / SQLite / vault concrete adapters
```

Rules for every remediated seam:

- Parse untrusted strings and JSON into typed parameters at the inbound
  boundary. Do not pass unvalidated `serde_json::Value` into domain logic.
- Keep domain rules independent of JSON-RPC, CLI formatting, SQLite, filesystem
  layout, environment variables, and ServiceNow response rendering.
- Use typed library errors with preserved sources. Convert them into CLI or
  JSON-RPC presentation errors once at the outer boundary.
- Keep internal APIs `pub(crate)` or `pub(super)` unless an existing public
  contract requires a re-export.
- Do not hold a synchronous lock across `.await`, block a Tokio executor with
  unbounded work, silently detach tasks, or create unbounded channels.
- Keep sensitive values out of `Debug`, tracing fields, errors, tests, and
  fixtures. Redaction belongs at the type or transport boundary.
- Treat a source file above 1,000 production lines as an architectural review
  trigger, not an automatic failure. Cohesion and dependency direction remain
  the closure criteria.

## System progression and dependency map

| Node ID | Node type | Authority refs | Phase | Wave | Provides | Consumes | Hard prerequisites | Closure gate |
|---|---|---|---|---|---|---|---|---|
| FND-HSR-001 | Foundation | `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#approved-goal`; `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#architectural-target`; `AGENTS.md#public-safe-content` | Phase 0 | Wave 0 | Immutable base, declared consumer seams, baseline behavior evidence, and path ownership | Existing workspace and current tests | Foundation root | Baseline gates are recorded; L0/L1/L2 seams and write scopes are explicit |
| CAP-HSR-CACHE-002 | Capability | `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#approved-goal`; `crates/snow_core/src/cache/store.rs:1-5203`; `crates/snow_core/src/service/vault.rs:84-111` | Phase 1 | Wave 1 | Current-format-only SQLite adapter and usable fresh rebuild path | FND-HSR-001; vault documents; existing projection contracts | FND-HSR-001 closure | No migration symbols or legacy fixtures remain; fresh create, mismatch failure, and rebuild replacement behavior pass |
| CAP-HSR-CLI-003 | Capability | `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#approved-goal`; `crates/snow_cli/src/main.rs:83-306` | Phase 2 | Wave 2 | Thin CLI entrypoint and feature command modules | FND-HSR-001; CAP-HSR-CACHE-002 rebuild/status interfaces | CAP-HSR-CACHE-002 closure | Existing CLI contracts pass and `main.rs` only parses, delegates, reports, and exits |
| CAP-HSR-RPC-004 | Capability | `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#approved-goal`; `crates/snow_daemon/src/rpc.rs:31-3394` | Phase 3 | Wave 3 | Stable JSON-RPC wire/server/router facade with feature handlers | FND-HSR-001; CAP-HSR-CACHE-002 status interface; existing application services | CAP-HSR-CACHE-002 and CAP-HSR-CLI-003 closure gates | Existing method names, payloads, error codes, lifecycle, and contract info pass through the decomposed router |
| CAP-HSR-BA-005 | Capability | `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#approved-goal`; `crates/snow_core/src/service/business_application.rs:108-3250` | Phase 4 | Wave 4 | Isolated Business Application domain algorithm and application orchestration | CAP-HSR-CACHE-002 repositories; existing ServiceNow and vault adapters | CAP-HSR-CACHE-002 and CAP-HSR-RPC-004 closure gates | Direct core, daemon, CLI, and MCP Business Application behavior remains unchanged; pure topology tests retain mutation sensitivity |
| CAP-HSR-STORY-006 | Capability | `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#approved-goal`; `crates/snow_daemon/src/story_write.rs:46-3402` | Phase 5 | Wave 5 | Decomposed story plan/apply pipeline with explicit fail-closed governance stages | CAP-HSR-RPC-004 router; existing MCP policy, plan, confirmation, idempotency, and audit contracts | CAP-HSR-RPC-004 closure | Plan/apply, replay, concurrency, scope, field policy, recovery, rate limit, and audit behavior remain consumer-identical |
| FEAT-HSR-007 | Feature | `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#approved-goal`; `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#verification-and-closure` | Phase 6 | Wave 6 | Evidence-backed hotspot closure and truthful public documentation | All five capability nodes | CAP-HSR-CLI-003, CAP-HSR-RPC-004, CAP-HSR-BA-005, CAP-HSR-STORY-006 closure gates | Workspace gates, architecture scans, public-safe review, and consumer parity evidence pass |

Hard `requires` edges override desired delivery order. These waves are
sequential because later work consumes earlier facades and several tasks move
tests or imports that would otherwise overlap. Do not run concurrent writers
against this checkout.

## Traceability matrix

| Authority ref | Behavior / decision | Task ID | Implementation seam | Acceptance evidence | Owner |
|---|---|---|---|---|---|
| `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#approved-goal` | SQLite has no migration capability | T-HSR-01 | `snow_core::cache::store` schema/open path | Source scan finds no migration functions, conditional upgrades, `ALTER TABLE`, legacy fixtures, or auto-migration documentation | Rust core coder |
| `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#approved-goal`; `crates/snow_core/src/service/vault.rs:84-111`; `crates/snow_core/src/facade.rs:680-716` | An incompatible cache can be rebuilt without opening it through normal `SnowCore` construction | T-HSR-01 | Offline cache rebuilder and pre-bootstrap CLI command route | L0 CLI test starts with an incompatible cache and produces a fresh current-format projection from a synthetic vault |
| `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#scope` | Ordinary startup never silently deletes an incompatible cache | T-HSR-01 | `Store::open` and typed `StoreError` | L0 startup test returns the incompatible-cache error and leaves the file unchanged |
| `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#scope` | Failed offline rebuild preserves the prior cache file | T-HSR-01 | Same-directory temporary build and replacement boundary | Negative-path test injects invalid vault content or replacement failure and proves the prior file remains |
| `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#scope` | Live-only derived projections are unavailable after rebuild until refreshed | T-HSR-01 | Rebuild report, Business Application cached reads, dictionary and inventory health status | Rebuild evidence reports what was and was not reconstructed; cached consumers never report missing live-only data as a complete empty inventory |
| `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#architectural-target`; `crates/snow_cli/src/main.rs:83-306` | CLI entrypoint is composition only | T-HSR-02 | `snow_cli::main`, `app`, `bootstrap`, `commands`, `output` | CLI consumer tests preserve flags, stdout/stderr semantics, and exit status; dependency scan removes database/domain work from `main.rs` |
| `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#architectural-target`; `crates/snow_daemon/src/rpc.rs:31-3394` | JSON-RPC transport, routing, feature behavior, and error conversion are separate | T-HSR-03 | `snow_daemon::rpc::{wire,method,server,router,handlers}` | Raw JSON-RPC consumer cases preserve success and error payloads; supported-method contract remains exact |
| `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#scope` | Direct and daemon-backed MCP consumers remain equivalent | T-HSR-03 | Daemon router, `snow_mcp` direct server and daemon bridge | Existing parity tests and schema contract tests pass; a representative read and governed write have identical consumer-visible structured payloads |
| `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#architectural-target`; `crates/snow_core/src/service/business_application.rs:108-3250` | Graph traversal and domain validation are independent of I/O adapters | T-HSR-04 | `service/business_application/topology.rs` and typed model modules | L2 algorithm tests catch cycle, direction, depth, edge-budget, alternate-path, and truncation mutations without network or SQLite |
| `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#scope` | Business Application search, sync, fallback, persistence, dictionary, and rendering behavior remain unchanged | T-HSR-04 | Business Application application modules and existing facades | Existing core/daemon/CLI/MCP behavior tests pass against generic local fixtures |
| `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#architectural-target`; `crates/snow_daemon/src/story_write.rs:46-3402` | Story write stages have explicit, fail-closed ownership | T-HSR-05 | `story_write::{plan,apply,scope,fields,assignment,recovery,confirmation,audit}` | Existing plan/apply tests plus negative replay, tamper, scope, terminal, rate-limit, recovery, and audit cases remain green |
| `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#non-goals` | No speculative shared write framework is introduced | T-HSR-05 | Story modules and other write families | Diff and dependency review show other write families unchanged except import-compatible router movement |
| `AGENTS.md#public-safe-content` | Changed artifacts remain public-safe | T-HSR-06 | Source, tests, fixtures, docs, generated artifacts | Manual diff review and configured sensitive scan find only generic values |
| `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#scope`; `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#verification-and-closure` | File movement does not masquerade as behavioral proof | T-HSR-06 | Declared L0/L1/L2 seams and moved tests | Baseline and final consumer evidence are recorded; no language, tautological, or module-shape test is counted as closure |

## Implementation boundary

### Allowed changes

- `AGENTS.md`: add the L0 consumer seam declaration and hotspot architecture
  rules without weakening public-safe requirements.
- `crates/snow_core/src/cache/store.rs` and a replacement
  `crates/snow_core/src/cache/store/` module tree.
- `crates/snow_core/src/cache/mod.rs`, `query/mod.rs`, `context.rs`,
  `service/vault.rs`, and `facade.rs` only where required to consume the
  current-format store and offline rebuild contracts.
- `crates/snow_cli/src/main.rs` and new feature-oriented CLI modules under
  `crates/snow_cli/src/`; existing `cli.rs`, `auth.rs`, `daemon_cmd/`,
  `display.rs`, `tui/`, and `tui_client.rs` only for import/path adjustments or
  consumer tests required by the split.
- `crates/snow_daemon/src/rpc.rs` and a replacement
  `crates/snow_daemon/src/rpc/` module tree; `lib.rs`, `transport.rs`, and
  focused daemon tests only for facade imports and behavior evidence.
- `crates/snow_core/src/service/business_application.rs` and a replacement
  `crates/snow_core/src/service/business_application/` tree;
  `resource/business_application.rs`, cache repository modules, and vault
  rendering modules only where the governing responsibility is moved rather
  than duplicated.
- `crates/snow_daemon/src/story_write.rs` and a replacement
  `crates/snow_daemon/src/story_write/` tree; existing story tests move with
  their behavior.
- `README.md`, `docs/MCP_CAPABILITIES.md`, and focused existing docs that claim
  schema migration or describe the affected public cache/rebuild contract.
- Focused tests under the owning crate and integration-test directories.

### Forbidden changes / non-goals

- Do not add a migration module, migration trait, ordered migration table,
  version-to-version conversion, schema diff engine, legacy reader, `ALTER
  TABLE` upgrade, or test fixture representing an older cache.
- Do not call removal, truncation, replacement, or rebuild from ordinary
  `Store::open`. It may create an absent current-format cache or validate an
  existing one; incompatibility fails closed.
- Do not delete or modify the separate SQLite files backing MCP plans,
  confirmations, idempotency, or governed audit state.
- Do not rename CLI commands, RPC methods, MCP tools, serialized fields, error
  codes, policy keys, or public domain types.
- Do not change authentication/session construction, ServiceNow query
  cardinality, pagination, redaction, persistence defaults, vault path
  derivation, daemon idle/shutdown behavior, or write confirmation semantics.
- Do not introduce a generic handler trait, macro registry, repository
  framework, or shared write-governance abstraction unless a separately
  approved design demonstrates identical semantics across at least two
  consumers.
- Do not stage or modify unrelated dirty work. Stop if the immutable base no
  longer describes the implementation seams and rebase the spec before coding.

**No-invention declaration:** Implement only the cited behavior inside this
boundary. A missing execution path, architecture, API, persistence flow,
provider behavior, or product solution is a blocker requiring an approved
authority update, not a coder decision.

## Decision gaps and blockers

- **Resolved — migration policy.** There is no migration capability. The
  current cache carries an exact format marker only so startup can distinguish
  current, absent, and incompatible state. The marker does not authorize an
  upgrade path.
- **Resolved — incompatible startup.** Normal startup returns a typed
  incompatible-cache error with the rebuild command. It does not delete,
  mutate, partially bootstrap, or reinterpret the old file.
- **Resolved — offline rebuild.** `snow rebuild-cache` is routed before normal
  `SnowCore` construction. It builds a fresh cache beside the configured cache,
  validates the result, and replaces the target only after success. It requires
  the daemon using that cache to be stopped and fails closed when the endpoint
  is active.
- **Resolved — online daemon rebuild.** The existing daemon RPC/job may rebuild
  only an already-current cache. It clears and rehydrates rebuildable projection
  tables inside a transaction on the existing connection; it never swaps an
  open database file. Failure rolls back.
- **Resolved — rebuild completeness.** Vault-backed records and knowledge are
  reconstructed from the vault. Live-only dictionaries, relationship
  inventories, semantic embeddings, cached users, query caches, and other
  projections that cannot be derived from the vault start unavailable and
  require their existing refresh/sync paths. Responses and rebuild reports must
  distinguish unavailable/not-refreshed from a known empty result.
- **Resolved — crate boundaries.** The first remediation remains within the
  four existing crates. A future crate extraction is permitted only after the
  module dependency graph proves a stable seam and receives separate authority.
- **No current blocker.** Any newly discovered cache authority, shared-store
  coupling, consumer-visible incompatibility, or missing recovery route becomes
  a blocker and must update this section before implementation continues.

## Scaffold inventory

Scaffolds are introduced and consumed sequentially. No placeholder may return
synthetic success; until a new seam is complete, the existing implementation
remains the active path.

| Seam ID | Authority refs | Crate / file / symbol | Signature / contract | Safe unresolved state | Owner | Completion evidence |
|---|---|---|---|---|---|---|
| SC-HSR-SEAMS | `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#scope`; `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#verification-and-closure` | `AGENTS.md#behavioral-test-seams` | Declares CLI, daemon JSON-RPC, MCP direct/bridge, on-disk projection, and governed write consumer seams | Fail-closed: test work is blocked as `Undeclared Seam`; no helper-level substitute | architecture owner | Seam declaration reviewed before test movement |
| SC-HSR-CACHE-FORMAT | `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#approved-goal`; `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#decision-gaps-and-blockers` | `snow_core::cache::store` | `CACHE_FORMAT_ID = "snow-cache-v1"`; `Store::open(path)` creates only when absent and otherwise validates exact format; incompatible state returns `StoreError::IncompatibleCacheFormat { found, expected }` | Typed error; existing file unchanged | Rust core coder | Fresh-create and incompatible-open behavior tests plus no-migration scan |
| SC-HSR-CACHE-REBUILD | `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#decision-gaps-and-blockers` | `snow_core` cache rebuild application seam | `rebuild_cache_from_vault(vault_path: &Path, database_path: &Path) -> Result<RebuildReport>` builds current format without opening the target as a `Store`; report names reconstructed and unavailable projections | Error with prior target preserved; no partial success report | Rust core coder | CLI incompatible-cache rebuild and injected-failure tests |
| SC-HSR-CLI | `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#architectural-target` | `snow_cli::{main,app,bootstrap,commands,output}` | `main` parses and delegates; `app::run(Cli)` selects pre-bootstrap/offline, daemon, or local-core execution; feature modules own command behavior and rendering | Fail-closed: existing command path remains active until the complete feature module is wired | CLI coder | CLI integration tests and dependency scan |
| SC-HSR-RPC-WIRE | `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#scope`; `crates/snow_daemon/src/rpc.rs:31-305` | `snow_daemon::rpc::{wire,method}` | Preserve `JsonRpcRequest`, `JsonRpcResponse`, `JsonRpcError`, `RpcMethod::from_method`, serialization names, and public re-exports | Fail-closed: existing `rpc.rs` definitions remain authoritative until moved intact | daemon coder | Golden consumer request/response expectations and existing contract tests |
| SC-HSR-RPC-ROUTER | `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#architectural-target`; `crates/snow_daemon/src/rpc.rs:307-3394` | `snow_daemon::rpc::{server,router,handlers}` | Preserve `JsonRpcServer` and `dispatch(JsonRpcRequest, &Arc<DaemonState>) -> JsonRpcResponse`; router selects one feature handler and centralizes error conversion | Fail-closed: unknown or unwired method returns method-not-found; never synthetic success | daemon coder | Daemon RPC behavior and lifecycle tests |
| SC-HSR-BA | `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#architectural-target`; `crates/snow_core/src/service/business_application.rs:1062-3250` | `snow_core::service::business_application` | Existing `BusinessApplicationService` and `SnowCore` facade signatures remain stable; topology functions accept typed graph inputs and perform no I/O | Fail-closed: existing service remains wired until each complete responsibility move lands | Rust core coder | Existing facade behavior plus L2 topology mutation cases |
| SC-HSR-STORY | `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#architectural-target`; `crates/snow_daemon/src/story_write.rs:46-3402` | `snow_daemon::story_write` | Preserve `handle_plan_get`, `handle_story_plan`, and `handle_story_apply`; internal modules return existing typed/JSON-RPC outcomes and fail closed | Fail-closed: existing monolithic handler remains active; no partial handler registration | daemon write coder | Governed plan/apply and negative-path regression set |

## Target module inventory

The names below are required ownership seams. A coder may combine two adjacent
modules only when the implementation demonstrates they have the same reason to
change and records that adjustment in this spec before merge.

```text
crates/snow_core/src/cache/store/
  mod.rs                    Store facade, connection ownership, re-exports
  error.rs                  typed store/cache-format errors
  schema.rs                 one current schema; no migration code
  records.rs                record lifecycle and record repository
  relationships.rs          references, relationships, enrichment rows
  knowledge.rs              knowledge projection, terms, tags, embeddings
  users.rs                  cached users and bounded query-cache projection
  business_applications.rs  BA fields, dictionary, server membership/health
  sync.rs                   generic sync metadata only

crates/snow_cli/src/
  main.rs                   parse, delegate, render fatal error, exit
  app.rs                    top-level execution-mode selection
  bootstrap.rs              config, credentials, ServiceNow/core composition
  cache_command.rs          pre-bootstrap cache status and offline rebuild
  commands/
    mod.rs
    attachments.rs
    business_applications.rs
    knowledge.rs
    records.rs
    servers.rs
    timecards.rs
    vault.rs
  output/                   human/JSON renderers grouped by feature

crates/snow_daemon/src/rpc/
  mod.rs                    stable facade and deliberate re-exports
  wire.rs                   JSON-RPC request/response/error DTOs
  method.rs                 method parsing and supported-method inventory
  server.rs                 listener, connections, idle/shutdown/drain
  router.rs                 thin method-to-handler dispatch
  handlers/
    mod.rs
    approvals.rs
    business_applications.rs
    cache_vault.rs
    incidents.rs
    jobs.rs
    knowledge.rs
    records.rs
    servers.rs
    system.rs

crates/snow_core/src/service/business_application/
  mod.rs                    service facade and stable methods
  model.rs                  service-private typed inputs/outcomes
  topology.rs               pure graph/path/budget algorithms
  inventory.rs              traversal orchestration
  fallback.rs               CI owner-group fallback policy
  dictionary.rs             metadata and alias resolution
  references.rs             primitive-reference resolution orchestration
  persistence.rs            repository/vault coordination
  sync.rs                   bounded and all-inventory synchronization

crates/snow_daemon/src/story_write/
  mod.rs                    stable public handlers
  plan.rs                   plan construction and preview
  apply.rs                  apply orchestration and guards
  scope.rs                  board/story/task scope rules
  fields.rs                 selector, allowlist, and constrained choices
  assignment.rs             actor/assignee/default resolution
  recovery.rs               pending create/update recovery state machines
  confirmation.rs           confirmation, replay, kill switch, rate limit
  audit.rs                  redacted audit identities, reasons, and warnings
```

Implementation adjustment: the CLI ownership tree is nested under
`crates/snow_cli/src/app/` (`bootstrap.rs`, `cache_command.rs`, `commands/`,
and `output/`). These modules share the binary-private application context and
therefore have the same reason to change; `main.rs` remains the composition
entrypoint and `app/mod.rs` remains the execution-mode facade.

Tests live with the owning behavior or in an integration-test target that
drives a declared seam. Do not create a mirrored test file for every production
module.

## Task breakdown and coder handoffs

### T-HSR-00: Pin baseline and declare behavioral seams

- System node: FND-HSR-001.
- Phase / Wave: Phase 0 / Wave 0.
- Hard prerequisites: immutable base
  `742af261f6a6fe837bcd478a572977938dd5c9c0` and a clean or explicitly
  reconciled worktree.
- Provides / consumes: provides baseline behavior, L0 seam ownership, and
  bounded task scopes; consumes the existing workspace and tests.
- Closure gate: baseline formatter, workspace tests, strict Clippy, MCP
  contract tests, and diff hygiene produce explicit results; `AGENTS.md`
  declares the consumer seams listed under Verification and closure.
- Authority refs: `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#approved-goal`; `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#scope`; `AGENTS.md#public-safe-content`.
- Allowed write scope: `AGENTS.md` and this specification only.
- Acceptance evidence: command outputs recorded against the immutable base. A
  failing baseline is not normalized away; affected work stops until the
  failure is classified as pre-existing or remediated under updated authority.
- Coder rule: Implement only cited authority; every uncited path or solution is a blocker requiring an approved authority update.

### T-HSR-01: Replace migration-aware Store with current-format repositories

- System node: CAP-HSR-CACHE-002.
- Phase / Wave: Phase 1 / Wave 1.
- Hard prerequisites: FND-HSR-001 closure gate.
- Provides / consumes: provides `SC-HSR-CACHE-FORMAT` and
  `SC-HSR-CACHE-REBUILD`; consumes vault documents and the existing public
  `Store` behavior.
- Closure gate: exact current-format open/create, incompatible failure, offline
  replacement rebuild, and online transactional rebuild pass; all migration
  code/tests/docs are removed.
- Authority refs: `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#approved-goal`; `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#decision-gaps-and-blockers`; `crates/snow_core/src/cache/store.rs:1-5203`; `crates/snow_core/src/service/vault.rs:84-111`.
- Allowed write scope: cache/core/query/vault/facade files and cache docs listed
  under Implementation boundary; focused core and CLI cache tests.
- Acceptance evidence: focused `snow_core` cache/vault tests; CLI cache command integration test; source scan under Verification and closure; updated README and MCP cache documentation.
- Coder rule: Implement only cited authority; every uncited path or solution is a blocker requiring an approved authority update.
- Implementation requirements:
  - Write the changed-behavior consumer tests first and observe the intended
    failure before production edits: incompatible startup currently migrates or
    mutates rather than failing, and rebuild currently requires an open core and
    upserts rather than replacing.
  - Move row types and repository implementations by feature while preserving
    `Store` facade signatures needed by existing callers.
  - Replace `SCHEMA_VERSION`, `schema_version`, `set_schema_version`, every
    `migrate_to_v*`, every `needs_v*_migration`, schema-introspection helper
    used only for migration, post-migration index creation, and legacy-schema
    tests with the exact-format scaffold.
  - `Store::open` may create an absent database using the one current schema.
    When a file exists, inspect the format before any schema mutation.
  - The offline rebuilder writes a same-directory temporary database, fully
    closes it, verifies current format and reconstructed counts, then replaces
    only the configured cache path. Failure leaves the target unchanged and
    removes only its own temporary artifact.
  - The online daemon rebuild runs only against a current-format `Store`,
    clears rebuildable projection state and rehydrates it in one transaction,
    and rolls back on failure. It does not replace the open file.
  - Rebuild reports enumerate unavailable live-only projections. Cached reads
    preserve `unknown_not_synced`, degraded, or unavailable semantics rather
    than returning a superficially complete empty inventory.
  - `cache-info` inspects without bootstrapping or mutating and reports current,
    absent, incompatible, or unreadable format.

### T-HSR-02: Reduce the CLI entrypoint to composition

- System node: CAP-HSR-CLI-003.
- Phase / Wave: Phase 2 / Wave 2.
- Hard prerequisites: CAP-HSR-CACHE-002 closure gate.
- Provides / consumes: provides `SC-HSR-CLI`; consumes the offline cache
  interface, daemon client, local core facade, existing CLI grammar, and output
  contracts.
- Closure gate: `main.rs` contains only module declarations, parsing,
  delegation, fatal-error presentation, and exit status; feature modules own
  execution and rendering without changing consumer behavior.
- Authority refs: `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#architectural-target`; `crates/snow_cli/src/main.rs:83-306`.
- Allowed write scope: CLI files listed under Implementation boundary and
  focused CLI tests.
- Acceptance evidence: existing CLI parser/integration tests, daemon status tests, representative human/JSON output cases, dependency scan, and focused crate tests.
- Coder rule: Implement only cited authority; every uncited path or solution is a blocker requiring an approved authority update.
- Implementation requirements:
  - Route offline `cache-info` and `rebuild-cache` before credential loading,
    Tokio/core startup, or daemon auto-spawn.
  - Move configuration and authentication composition to `bootstrap.rs`; do
    not expose credential values through errors or `Debug`.
  - Group commands by product feature. Keep Clap grammar in `cli.rs`; do not
    mix parser definitions back into execution modules.
  - Separate human/JSON rendering from application invocation where output has
    a stable consumer contract.
  - Remove direct `rusqlite`, `Store`, raw SQL, and feature-domain
    implementation from `main.rs`. Bootstrap may construct concrete adapters;
    command modules consume the CLI application context or daemon client.
  - Move-only work uses the green baseline before and after. Do not manufacture
    red tests for a pure refactor. Add a characterization test only when an
    existing consumer contract lacks coverage, and observe it green against the
    old path before moving code.

### T-HSR-03: Decompose daemon JSON-RPC into wire, server, router, and handlers

- System node: CAP-HSR-RPC-004.
- Phase / Wave: Phase 3 / Wave 3.
- Hard prerequisites: CAP-HSR-CACHE-002 and CAP-HSR-CLI-003 closure gates.
- Provides / consumes: provides `SC-HSR-RPC-WIRE` and
  `SC-HSR-RPC-ROUTER`; consumes `DaemonState`, core application services,
  current cache status, jobs, and existing write handlers.
- Closure gate: the monolithic `rpc.rs` is replaced by the target module tree;
  raw requests observe the same results, errors, method inventory, connection
  lifecycle, idle behavior, and shutdown behavior.
- Authority refs: `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#architectural-target`; `crates/snow_daemon/src/rpc.rs:31-3394`.
- Allowed write scope: daemon RPC module tree, `lib.rs` facade imports,
  transport conversions needed to remove handler-local rendering, and focused
  daemon/MCP tests.
- Acceptance evidence: focused RPC handler tests, raw frame tests, daemon shutdown/idle tests, MCP bridge contract tests, and crate tests.
- Coder rule: Implement only cited authority; every uncited path or solution is a blocker requiring an approved authority update.
- Implementation requirements:
  - Move wire DTOs and `RpcMethod` intact first and re-export their existing
    public paths.
  - Move listener/connection/idle/shutdown logic without mixing handler
    behavior into `server.rs`.
  - Keep `router.rs` as method selection. Each arm delegates to one feature
    handler or existing governed write module; it does not contain full use-case
    implementations.
  - Each feature handler owns its typed parameters, validation, one application
    invocation, and wire response mapping. Centralize common invalid-parameter,
    not-found, and internal-error conversion without branching on message text.
  - Remove direct `Store::open`, raw SQLite inspection, and vault Markdown
    rendering from RPC routing. Consume typed core status and transport
    presentation interfaces.
  - Preserve `contract_info`, supported-method advertisement, JSON-RPC error
    codes/data, direct/daemon MCP parity, connection caps, frame limits,
    deadlines, idle shutdown, cancellation, and task-drain behavior.
  - Move tests to the owning handler or integration seam; do not duplicate the
    old monolithic test module.

### T-HSR-04: Isolate Business Application domain and orchestration concerns

- System node: CAP-HSR-BA-005.
- Phase / Wave: Phase 4 / Wave 4.
- Hard prerequisites: CAP-HSR-CACHE-002 and CAP-HSR-RPC-004 closure gates.
- Provides / consumes: provides `SC-HSR-BA`; consumes typed cache repositories,
  existing ServiceNow client behavior, vault persistence, and core facade.
- Closure gate: topology/validation is pure; I/O orchestration, fallback,
  dictionary, references, persistence, and sync have distinct owners; all
  existing consumer behavior remains stable.
- Authority refs: `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#architectural-target`; `crates/snow_core/src/service/business_application.rs:108-3250`.
- Allowed write scope: Business Application service/resource/cache/vault files
  listed under Implementation boundary and focused tests.
- Acceptance evidence: focused topology tests, core Business Application tests, daemon RPC tests, CLI output tests, MCP contract/parity tests, and no-I/O dependency scan for `topology.rs`.
- Coder rule: Implement only cited authority; every uncited path or solution is a blocker requiring an approved authority update.
- Implementation requirements:
  - Move public/resource DTOs only when ownership becomes clearer; preserve
    existing re-export paths.
  - Extract path construction, cycle prevention, alternate-path handling,
    direction, depth, CI/edge budgets, and truncation into synchronous pure
    topology code over typed inputs.
  - Keep ServiceNow paging and lookup in application/adapter orchestration, not
    in topology.
  - Keep fallback policy separate from primary traversal and preserve degraded
    diagnostics.
  - Route SQLite writes through the Business Application repository module and
    vault Markdown through the vault boundary; do not duplicate SQL or renderers.
  - Preserve persistence defaults, `persist=false`, cached/live behavior,
    relationship and service-membership provenance, dictionary fallback,
    reference status, and partial-result semantics.
  - Retain L2 tests only for the non-obvious graph algorithm. Service and
    adapter behavior stays at the public core/daemon/CLI/MCP seams.

### T-HSR-05: Decompose the governed story write pipeline

- System node: CAP-HSR-STORY-006.
- Phase / Wave: Phase 5 / Wave 5.
- Hard prerequisites: CAP-HSR-RPC-004 closure gate.
- Provides / consumes: provides `SC-HSR-STORY`; consumes the thin RPC router,
  existing MCP policy, ServiceNow adapter, plan/confirmation/idempotency stores,
  and audit sink.
- Closure gate: the original public handlers remain stable while each internal
  governance stage has one owner and all fail-closed behavior remains
  consumer-identical.
- Authority refs: `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#architectural-target`; `crates/snow_daemon/src/story_write.rs:46-3402`.
- Allowed write scope: story write module tree and its existing focused tests.
  Other write families may receive import-only adjustments from RPC movement;
  their behavior is out of scope.
- Acceptance evidence: story plan/apply focused tests, tamper and replay negatives, recovery state-machine tests, audit redaction evidence, daemon crate tests, and diff review of untouched write families.
- Coder rule: Implement only cited authority; every uncited path or solution is a blocker requiring an approved authority update.
- Implementation requirements:
  - Keep plan construction separate from apply execution.
  - Keep board/story/task scope, assignment-group scope, selector fields,
    blocked fields, configured allowlists, and constrained-choice validation in
    explicit modules with typed outcomes.
  - Keep actor/assignee defaults and identity lookup separate from policy.
  - Model pending create/update recovery decisions as explicit enums and retain
    replay, idempotency, and concurrency semantics.
  - Keep confirmation binding, replay protection, kill switch, and rate limit
    fail closed. Preserve the order in which these guards execute where callers
    observe the error.
  - Keep audit redaction and hashing centralized; never include free text,
    credentials, or full identity values in new diagnostics.
  - Do not extract a shared governance framework during this task. Repetition
    may be documented for a later design, but existing change/resource-plan/
    timecard/catalog/work-note semantics must not be generalized speculatively.
  - Move-only work relies on green before/after evidence. Retain L2 tests for
    recovery/replay state machines and boundary conditions; keep plan/apply
    behavior at the daemon/MCP consumer seam.

### T-HSR-06: Falsify architecture closure and update public documentation

- System node: FEAT-HSR-007.
- Phase / Wave: Phase 6 / Wave 6.
- Hard prerequisites: CAP-HSR-CLI-003, CAP-HSR-RPC-004, CAP-HSR-BA-005, and
  CAP-HSR-STORY-006 closure gates.
- Provides / consumes: provides the closure verdict; consumes all focused
  evidence, the final diff, public docs, and workspace gates.
- Closure gate: every command and architectural scan under Verification and
  closure passes; any unavailable live proof or residual hotspot is reported
  rather than normalized into success.
- Authority refs: `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#approved-goal`; `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#verification-and-closure`; `AGENTS.md#public-safe-content`.
- Allowed write scope: directly affected documentation and narrowly related
  defects exposed by closure gates. New behavior requires a spec update.
- Acceptance evidence: final source inventory, consumer parity, full workspace
  gates, sensitive scan of the candidate commit, and a clean or explicitly
  scoped worktree.
- Coder rule: Implement only cited authority; every uncited path or solution is a blocker requiring an approved authority update.

## Verification and closure

### Declared behavioral test seams

T-HSR-00 adds these to `AGENTS.md` before implementation work:

- **L0 CLI:** execute the compiled `snow` binary with temporary generic config,
  vault, and cache paths; assert exit status and consumer-visible stdout,
  stderr, files, or cache state.
- **L0 daemon JSON-RPC:** send a request through the daemon's framed local
  transport or its established request dispatcher and assert the exact
  consumer-visible result/error contract.
- **L0 MCP:** list or invoke tools through both direct and daemon-backed MCP
  paths and assert identical structured payloads where both transports expose
  the capability.
- **L0 durable artifact:** drive a public core/CLI operation and inspect the
  resulting generic vault or current-format cache projection. SQLite layout is
  not a compatibility contract and earns no legacy-format test.
- **L0 governed write:** drive plan/apply through daemon JSON-RPC or MCP using
  local ServiceNow and state-store fakes; assert confirmation, policy,
  idempotency, replay, concurrency, receipt, and audit outcomes.
- **L1 wire contract:** committed independent JSON-RPC/MCP request and response
  expectations where the encoding crosses the process boundary.
- **L2 hard logic:** Business Application graph traversal; story pending
  recovery/replay state machines; parsers and boundary arithmetic only when the
  test names the algorithmic, state-machine, parser, concurrency, or boundary
  earn.

Tests of derives, constructors, getters, module shapes, mock call counts, and
self-generated serialization round trips do not count as evidence. Pure
refactors use green baseline-before and green-after evidence. Changed cache
behavior uses red-first consumer tests that fail for the intended old behavior.

### Baseline and focused gates

- Worktree/base:
  `git status --short --branch`, `git rev-parse HEAD`, and
  `git diff --check`.
- Baseline workspace behavior:
  `CARGO_INCREMENTAL=0 cargo test --workspace --all-targets`.
- Cache capability:
  `CARGO_INCREMENTAL=0 cargo test -p snow_core cache -- --test-threads=1` and
  the focused CLI incompatible-cache rebuild integration test added by
  T-HSR-01.
- CLI capability:
  `CARGO_INCREMENTAL=0 cargo test -p snow_cli --all-targets`.
- Daemon RPC capability:
  `CARGO_INCREMENTAL=0 cargo test -p snow_daemon --lib -- --test-threads=1`
  plus existing daemon integration targets.
- Business Application capability: focused `snow_core`, `snow_daemon`,
  `snow_cli`, and `snow_mcp` tests selected by the existing
  `business_application` test names.
- Story write capability:
  `CARGO_INCREMENTAL=0 cargo test -p snow_daemon story --lib -- --test-threads=1`.
- MCP contracts:
  `cargo test -p snow_mcp --test contract_tools_list` and
  `cargo test -p snow_mcp --test daemon_bridge bridge_filters_tools_against_daemon_contract`.

### Architecture and no-migration scans

- No migration capability:

  ```bash
  rg -n 'migrate_to_v|needs_v[0-9]+_migration|ALTER TABLE|auto-migrat|legacy schema' \
    crates/snow_core/src/cache README.md docs/MCP_CAPABILITIES.md
  ```

  Expected result: no matches. Current-format creation SQL is allowed only in
  `crates/snow_core/src/cache/store/schema.rs`.

- CLI boundary:

  ```bash
  rg -n 'rusqlite|cache::store::Store|Store::open|execute_batch|prepare\(' \
    crates/snow_cli/src/main.rs crates/snow_cli/src/app/commands crates/snow_cli/src/app/output
  ```

  Expected result: no matches. Concrete construction belongs in bootstrap or
  the bounded offline cache command adapter.

- RPC boundary:

  ```bash
  rg -n 'rusqlite|cache::store::Store|Store::open' \
    crates/snow_daemon/src/rpc/router.rs crates/snow_daemon/src/rpc/handlers
  ```

  Expected result: no matches.

- Pure Business Application topology:

  ```bash
  rg -n 'rusqlite|servicenow_rs|tokio|reqwest|std::fs' \
    crates/snow_core/src/service/business_application/topology.rs
  ```

  Expected result: no matches.

- Hotspot inventory:

  ```bash
  rg --files -g '*.rs' -g '!target/**' | xargs wc -l | sort -nr | head -40
  ```

  Closure requires the five original production hotspots to be removed or
  reduced to thin facades and no replacement file to recreate their mixed
  responsibilities. Line count is review evidence, not the sole gate.

### Final workspace and safety gates

- `cargo fmt --all --check`.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`.
- `CARGO_INCREMENTAL=0 cargo test --workspace --all-targets`.
- `git diff --check`.
- Run `python3 -B scripts/mcp_schema_smoke.py -- <local bridge command>` after
  the daemon/bridge is available; a local deterministic bridge check does not
  claim production availability.
- Inspect changed and untracked files for organization-specific values. After
  the coherent candidate commit exists, run
  `bash scripts/sensitive_scan.sh <candidate-commit>` with the operator's
  external pattern file configured.
- Review `git diff --stat`, `git diff`, and final `git status --short --branch`
  for unrelated changes.
- No live ServiceNow read or write is required for this structural remediation.
  If one is separately authorized, report it independently from deterministic
  closure.

### Spec and stub closure

- Spec structure:
  `python3 ~/.openclaw/workspace/foreman/scripts/validate_spec_contract.py docs/superpowers/plans/2026-08-17-hotspot-remediation.md`.
- Every scaffold seam is implemented with its cited proof or explicitly
  reported as blocked. None is complete merely because it compiles.
- The final verdict is `PASS`, `FAIL`, `BLOCKED`, or `PASS WITH RISKS`, with
  exact command evidence. Partial focused success is not workspace closure.

## Coder handoff

Execute one task at a time in dependency order T-HSR-00 through T-HSR-06. Keep
each task independently reviewable and commit behavior changes separately from
move-only refactors. Stage only the coherent task slice and preserve unrelated
work.

Implement only the cited behavior inside the task's allowed write scope. A
missing execution path, architecture, API, persistence flow, provider behavior,
or product solution is a blocker requiring an approved authority update, not a
coder decision.
