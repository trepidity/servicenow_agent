# Implementation Spec: ServiceNow-authoritative cache recovery

## Authority and scope

### Approved goal

Correct cache recovery so `snow rebuild-cache` creates a fresh current-format
SQLite projection from terminal live ServiceNow reads, while `snow reset-cache`
creates an empty current-format projection. The markdown vault is not an
authoritative rebuild input. Preserve vault import as a separately named
offline operation.

### Governing authority

- User directive 2026-08-19: "Why isn't rebuild or reset rebuilding from
  ServiceNow? That's the authorative place" and "Plan and implement these
  corrections".
- `AGENTS.md#behavioral-test-seams` requires changed CLI behavior to be proved
  through the compiled `snow` binary and consumer-visible state.
- `AGENTS.md#public-safe-content` prohibits deployment-specific content in
  committed artifacts.
- `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#scope` establishes
  SQLite as disposable derived state and requires failed reconstruction to
  preserve the prior cache.

### Scope

- Make `snow rebuild-cache` an authenticated, offline, ServiceNow-backed
  operation that drains every page for its declared projection scope.
- Rebuild the enabled configured record resources plus the Business
  Application projection; report the tables, pages, records, source, and
  completeness.
- Build into a fresh sibling staging cache and replace `snow.db` only after all
  live reads and cache validation succeed.
- Make live rebuild independent of markdown parsing and leave the vault
  unchanged.
- Preserve the former vault behavior only as `snow import-cache-from-vault`.
- Refuse `rebuild-cache`, `import-cache-from-vault`, and `reset-cache` while the
  daemon endpoint is reachable.
- Remove or fail closed every daemon, job, admin-TUI, and direct-MCP path that
  attempts an online cache rebuild.
- Update CLI help and public-safe documentation to state the authority and
  lifecycle boundary.

### Non-goals

- No SQLite migration path or compatibility preservation for legacy layouts.
- No full-instance export beyond the configured readable projection scope.
- No live production rebuild, daemon restart, deployment, or credential
  mutation as part of source implementation.
- No vault repair, rewrite, deletion, or parser remediation.
- No daemon-owned background warming system.

## System progression and dependency map

| Node ID | Node type | Authority refs | Phase | Wave | Provides | Consumes | Hard prerequisites | Closure gate |
|---|---|---|---|---|---|---|---|---|
| FND-CACHE-AUTHORITY | Foundation | `docs/spec-servicenow-authoritative-cache-rebuild.md#approved-goal`; `AGENTS.md#behavioral-test-seams` | Phase 1 | Wave 1 | Explicit ServiceNow/vault authority split and offline lifecycle guard | Existing runtime path and daemon endpoint contracts | Foundation root | Compiled CLI tests distinguish live rebuild, vault import, reset, and running-daemon rejection |
| CAP-LIVE-PROJECTION | Capability | `docs/spec-servicenow-authoritative-cache-rebuild.md#approved-goal`; `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#scope` | Phase 2 | Wave 2 | Terminal live page drain into a fresh validated cache, with structured completeness report | FND-CACHE-AUTHORITY; configured resource scopes; ServiceNow Table API paginator | FND-CACHE-AUTHORITY closure | Local ServiceNow fake proves multi-page success and failure preserves the prior cache |
| FEAT-CACHE-RECOVERY | Feature | `docs/spec-servicenow-authoritative-cache-rebuild.md#approved-goal` | Phase 3 | Wave 3 | Correct CLI behavior and truthful public surfaces | FND-CACHE-AUTHORITY; CAP-LIVE-PROJECTION | FND-CACHE-AUTHORITY and CAP-LIVE-PROJECTION closure | Focused CLI suite, workspace tests, strict Clippy, format, diff, public-safe scan, and source-graph guard pass |

Hard `requires` edges override desired delivery order and parallelism. Preserve
legacy registered IDs as opaque IDs and give them an explicit node/gate noun.

## Traceability matrix

| Authority ref | Behavior / decision | Task ID | Implementation seam | Acceptance evidence | Owner |
|---|---|---|---|---|---|
| `docs/spec-servicenow-authoritative-cache-rebuild.md#approved-goal` | ServiceNow is the rebuild authority | T-CACHE-01 | `snow_core` live rebuild service and `snow_cli` rebuild dispatch | L0 compiled CLI fake returns `source: ServiceNow`, terminal page counts, and live records in the replaced cache | Codex |
| `docs/spec-servicenow-authoritative-cache-rebuild.md#approved-goal` | Reset is an empty fresh start | T-CACHE-01 | `snow_core::reset_cache`; `snow reset-cache` | Existing L0 reset test proves zero records, current format, no vault read, and stale sidecar removal | Codex |
| `docs/spec-servicenow-authoritative-cache-rebuild.md#approved-goal` | Vault import is not rebuild | T-CACHE-02 | CLI command grammar and offline vault-import function | L0 test proves malformed vault blocks only `import-cache-from-vault`, not reset or live rebuild | Codex |
| `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#scope` | Failed reconstruction preserves prior cache | T-CACHE-01 | staging-cache replacement boundary | Negative L0 fake fails a later page and prior cache row remains | Codex |
| `AGENTS.md#behavioral-test-seams` | Destructive cache replacement is offline | T-CACHE-02 | CLI daemon endpoint guard; online RPC/MCP removal | L0 tests reject reachable daemon without cache mutation; contract lists omit online rebuild | Codex |
| `AGENTS.md#public-safe-content` | Artifacts contain generic data only | T-CACHE-03 | Tests, docs, fixtures, output | Sensitive scan and diff review | Codex |

## Implementation boundary

### Allowed changes

- `crates/snow_core/src/service/` and `crates/snow_core/src/facade.rs` for a
  typed live rebuild report, declared table scope, cache-only record
  projection, terminal pagination, validation, and atomic promotion.
- `crates/snow_cli/src/` for command grammar, authenticated rebuild dispatch,
  offline guards, reports, and vault-import naming.
- `crates/snow_cli/tests/cache_format_contract.rs` and focused daemon/MCP
  contract tests for red-first consumer evidence.
- `crates/snow_daemon/src/`, `crates/snow_mcp/src/`, and the admin TUI only to
  remove or fail closed the unsafe online rebuild surface.
- Public-safe README and capability documentation describing the corrected
  contract.

### Forbidden changes / non-goals

- Do not read the existing vault from the live rebuild path.
- Do not replace the active cache before every declared live table reaches a
  terminal page and the staging cache validates.
- Do not describe ACL-readable configured scope as a full ServiceNow instance
  export.
- Do not silently skip an enabled resource, unsupported filter, page error, or
  projection error.
- Do not change write governance, daemon startup, MCP record-query semantics,
  or unrelated cache schema.

**No-invention declaration:** Implement only the cited behavior inside this
boundary. A missing execution path, architecture, API, persistence flow,
provider behavior, or product solution is a blocker requiring an approved
authority update, not a coder decision.

## Decision gaps and blockers

- None. "Full rebuild" is bounded to the configured readable projection scope;
  CLI output must name that boundary instead of claiming an instance-wide
  export.

## Scaffold inventory

None.

## Task breakdown and coder handoffs

### T-CACHE-01: atomic ServiceNow-backed rebuild

- System node: CAP-LIVE-PROJECTION capability
- Phase / Wave: Phase 2 / Wave 2
- Hard prerequisites: FND-CACHE-AUTHORITY closure
- Provides / consumes: provides a complete typed rebuild report and promoted
  current-format cache; consumes configured resource scopes, Table API
  pagination, and cache projection
- Closure gate: multi-page positive L0 test plus page-failure preservation L0
  test pass against a local ServiceNow fake
- Authority refs: `docs/spec-servicenow-authoritative-cache-rebuild.md#approved-goal`; `docs/superpowers/plans/2026-08-17-hotspot-remediation.md#scope`;
  `AGENTS.md#behavioral-test-seams`
- Allowed write scope: `crates/snow_core/src/service/`,
  `crates/snow_core/src/facade.rs`, `crates/snow_core/src/lib.rs`,
  `crates/snow_cli/src/app/`, `crates/snow_cli/tests/cache_format_contract.rs`
- Acceptance evidence: compiled `snow` binary, independent HTTP fixtures,
  exact stdout/stderr assertions, and durable cache inspection
- Coder rule: Implement only the cited behavior inside the stated boundary. New execution paths, architectures, APIs, persistence flows, provider semantics, or product solutions require an approved authority update before code changes. An unclear or missing decision is a blocker.

### T-CACHE-02: authority naming and online fail-closed cleanup

- System node: FND-CACHE-AUTHORITY foundation
- Phase / Wave: Phase 1 / Wave 1
- Hard prerequisites: Foundation root
- Provides / consumes: provides distinct reset, live rebuild, and vault-import
  commands; consumes daemon liveness and existing vault-import implementation
- Closure gate: CLI help and negative consumer tests prove the three meanings;
  daemon/direct-MCP contracts no longer advertise executable online rebuild
- Authority refs: `docs/spec-servicenow-authoritative-cache-rebuild.md#approved-goal`; `AGENTS.md#behavioral-test-seams`
- Allowed write scope: `crates/snow_cli/src/`, `crates/snow_daemon/src/`,
  `crates/snow_mcp/src/`, `crates/snow_cli/tests/`
- Acceptance evidence: compiled CLI tests and focused RPC/MCP contract tests
- Coder rule: Implement only the cited behavior inside the stated boundary. New execution paths, architectures, APIs, persistence flows, provider semantics, or product solutions require an approved authority update before code changes. An unclear or missing decision is a blocker.

### T-CACHE-03: truthful documentation and release gates

- System node: FEAT-CACHE-RECOVERY feature
- Phase / Wave: Phase 3 / Wave 3
- Hard prerequisites: FND-CACHE-AUTHORITY and CAP-LIVE-PROJECTION closure
- Provides / consumes: provides public-safe operator guidance; consumes the
  final CLI and runtime contracts
- Closure gate: documentation matches `snow --help`; workspace gates and
  sensitive scan pass
- Authority refs: `docs/spec-servicenow-authoritative-cache-rebuild.md#approved-goal`;
  `AGENTS.md#public-safe-content`
- Allowed write scope: `README.md`, `crates/snow_cli/README.md`,
  `docs/MCP_CAPABILITIES.md`, this specification
- Acceptance evidence: diff review, help smoke, sensitive scan, formatter,
  strict Clippy, workspace tests, and source-graph guard
- Coder rule: Implement only the cited behavior inside the stated boundary. New execution paths, architectures, APIs, persistence flows, provider semantics, or product solutions require an approved authority update before code changes. An unclear or missing decision is a blocker.

## Verification and closure

- Spec structure: `python3 /Users/jared/.openclaw/workspace/foreman/scripts/validate_spec_contract.py docs/spec-servicenow-authoritative-cache-rebuild.md`
- Red-first evidence: run each new compiled CLI behavior test before production
  edits and retain the failure reason in the implementation record.
- Focused evidence: `CARGO_INCREMENTAL=0 cargo test -p snow_cli --test cache_format_contract`
- Shared behavior: `CARGO_INCREMENTAL=0 cargo test --workspace --all-features`
- Static gates: `cargo fmt --all --check` and `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- Repository gates: `git diff --check`, `python3 scripts/check_rust_source_graph.py`,
  and `bash scripts/sensitive_scan.sh HEAD`
- Runtime boundary: source and local-fake closure do not prove the installed
  daemon or a live production rebuild; installation and live cutover remain
  separately reported.
- Stub closure: every scaffold seam is implemented with its cited proof, or is
  explicitly reported as a typed blocked/untrusted state; none is called complete
  merely because it compiles.
