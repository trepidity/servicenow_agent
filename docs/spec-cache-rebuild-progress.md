# Implementation Spec: Observable and panic-safe cache rebuild progress

## Authority and scope

### Approved goal

Make `snow rebuild-cache` continuously explain what it is doing while it builds
the staging cache, using truthful table, page, and record counts that let an
operator distinguish forward progress from a stalled ServiceNow request. Also
close the observed UTF-8 journal-parser panic so Unicode journal content cannot
terminate a rebuild without a controlled outcome.

### Governing authority

- `USER-2026-08-19-CACHE-PROGRESS`: direct user request to "provide updates as
  the cache is built that will give meaningful information", following an
  observed rebuild that remained silent for several minutes.
- `USER-2026-08-19-UTF8-PANIC`: direct user report of a rebuild panic caused by
  slicing an em-dash journal line at a non-character boundary.
- `USER-2026-08-19-CACHE-PROGRESS-PLAN-REVIEW`: direct review identifying the
  paginator terminal-state contract, both reachable UTF-8 slice sites, the
  upstream publication gate, deterministic streaming evidence, diagnostic
  state ownership, enabled-table precomputation, and the unproved promotion
  failure claim.
- `docs/spec-servicenow-authoritative-cache-rebuild.md#approved-goal` defines
  ServiceNow as the rebuild authority and requires complete staging-cache
  construction before atomic promotion.
- `AGENTS.md#behavioral-test-seams` requires changed CLI behavior to be proved
  through the compiled `snow` binary and consumer-visible output and cache
  state.
- `AGENTS.md#public-safe-content` prohibits deployment-specific values in
  committed output examples, tests, fixtures, and documentation.

### Scope

- Emit a flushed progress update before the first potentially long live rebuild
  phase and at every table start, page request, successfully projected page,
  table completion, finalization start, success, and controlled failure.
- Identify the current resource, ServiceNow table, table position, requested
  page, records returned by the page, per-table cumulative records, and rebuild
  cumulative records where those values are known.
- Send progress to `stderr` and retain the final successful report on `stdout`
  so redirected summary output remains separable from operational diagnostics.
- Use line-oriented, ANSI-free output for both terminals and redirected logs;
  flush every progress line immediately.
- Retain the existing final aggregate report and add its already-collected
  per-table page and record counts.
- Fix both reachable journal parsers to use checked UTF-8 string slicing: the
  upstream `servicenow_rs` parser used while projecting live rebuild records
  and the local cached record JSON parser used by `get_record` reads.
- Publish the public-safe upstream parser regression and fix, then pin this
  workspace to the new immutable `servicenow_rs` revision.
- Add an RAII staging-cache guard so ordinary failures and unwinding panics
  remove staging SQLite files and sidecars unless promotion succeeds.
- Preserve the current cache byte-for-byte when the rebuild fails before
  promotion begins; never describe a promotion failure as proof that the
  current cache is unchanged.

### Non-goals

- No fabricated percentage, total-record count, ETA, or throughput claim;
  ServiceNow does not currently provide a reliable rebuild-wide record total.
- No daemon, MCP, admin-TUI, or background rebuild path. Rebuild remains an
  offline CLI operation.
- No raw configured filters, usernames, record contents, sys_ids, instance
  URLs, credential data, local paths, or journal bodies in progress output.
- No cancellation protocol, resumable checkpoint, parallel table fetching, or
  change to rebuild scope, filters, page size, persistence schema, or atomic
  promotion semantics.
- No local duplicate of the dependency journal parser and no `catch_unwind`
  wrapper presented as a parser fix. If the upstream dependency cannot be
  corrected and pinned, that work is blocked pending an approved dependency
  decision.
- No animated progress bar or new terminal-rendering dependency in this change.
- No redesign of `promote_rebuilt_cache` or new promotion-failure guarantee.
  Existing promotion semantics remain governed by
  `docs/spec-servicenow-authoritative-cache-rebuild.md`; this progress change
  distinguishes promotion failures but does not claim the current cache stayed
  unchanged after promotion began.

## Progress output contract

Progress is an append-only event stream on `stderr`. Each completed-page line
means that the returned records were successfully projected into the staging
cache; a page that was only received but failed projection must never be
reported as completed.

```text
rebuild-cache: preparing ServiceNow staging cache
rebuild-cache: tables=12 page_size=100
rebuild-cache: resolving configured user scope
rebuild-cache: configured user scope resolved
[1/12] incident (incident): start
[1/12] incident (incident): requesting page=1
[1/12] incident (incident): page=1 page_records=9 table_records=9 total_records=9
[1/12] incident (incident): complete pages=1 records=9
[2/12] change (change_request): start
...
rebuild-cache: finalizing validated staging cache
rebuild-cache: complete tables=12 pages=31 records=2976
```

Before draining, precompute the enabled mapped resources in existing
`REBUILD_SCOPE` order and append the always-included Business Application
entry. Use this single list for `tables=N`, every `[i/N]` prefix, user-scope
resolution, and iteration. An empty table emits `start`, `requesting page=1`,
and `complete pages=0 records=0`. Page numbering is one-based for operators.

The requesting rule mirrors the real paginator exactly: before each
`next_page()` call, first inspect public `Paginator::is_done()`. If it is
already `true`, end the table without emitting another request. Otherwise emit
`requesting page=N`, where `N` is the number of successfully projected pages
plus one, and then call `next_page()`. Therefore an initial empty response emits
`requesting page=1`, while a short terminal page emits its completed `page=1`
line and no trailing `requesting page=2`. This rule changes neither page size
nor pagination order.

On a controlled failure, `stderr` must retain all earlier progress and end with
a safe-state diagnostic before the existing error chain:

```text
[8/12] knowledge (kb_knowledge): requesting page=8
rebuild-cache: failed during knowledge (kb_knowledge) page=8
rebuild-cache: staging cache removed; current cache unchanged
Error: <existing contextual error chain>
```

The final successful `stdout` report remains stable and gains one line per
processed table before the totals:

```text
rebuild-cache
source: ServiceNow
scope: configured ACL-readable projection
table: resource=incident servicenow_table=incident pages=1 records=9
table: resource=knowledge servicenow_table=kb_knowledge pages=30 records=2967
tables: 12
pages: 31
records: 2976
complete: true
```

The examples define field names and ordering, not specific counts. Progress
rendering must not inspect or print record payloads or configured filter values.
The instance-URL prohibition applies to progress and safe-state diagnostic
lines introduced by this change. The adjacent pre-existing error chain can
contain a request URL from `reqwest`; redacting that existing error contract is
not part of this task.

The CLI progress renderer and failure renderer share one
`Arc<Mutex<CacheRebuildProgressState>>`. The renderer updates the last-seen
table position, resource, ServiceNow table, and requested page in that shared
state before it writes and flushes each event. The CLI retains a clone of the
state handle after `drop(core)` and uses only that snapshot to render the
failure location. It must not parse rendered text or create a second page
counter. A failure before promotion begins ends with the safe-state line shown
above. A failure returned by `promote_rebuilt_cache` instead ends with:

```text
rebuild-cache: failed during staging cache promotion
rebuild-cache: staging cache removed; current cache state not claimed
Error: <existing contextual error chain>
```

The promotion-failure path must not print `current cache unchanged`.

## System progression and dependency map

| Node ID | Node type | Authority refs | Phase | Wave | Provides | Consumes | Hard prerequisites | Closure gate |
|---|---|---|---|---|---|---|---|---|
| FND-JOURNAL-UTF8-SAFETY | Foundation | `USER-2026-08-19-UTF8-PANIC`; `USER-2026-08-19-CACHE-PROGRESS-PLAN-REVIEW`; `AGENTS.md#behavioral-test-seams` | Phase 1 | Wave 1 | Panic-free parsing of untrusted Unicode journal lines on rebuild projection and cached record reads | Existing upstream `servicenow_rs` journal parser, local record JSON parser, published immutable dependency revision, and existing direct-MCP `get_record` seam | Foundation root | Upstream L2 parser regression, compiled-CLI Unicode rebuild regression, and direct-MCP `get_record` Unicode regression fail before their respective fixes and pass after them; the fixed upstream commit is public and pinned |
| CAP-REBUILD-PROGRESS-EVENTS | Capability | `USER-2026-08-19-CACHE-PROGRESS`; `docs/spec-cache-rebuild-progress.md#progress-output-contract` | Phase 2 | Wave 2 | Typed, ordered rebuild lifecycle events carrying only public-safe metadata | Existing sequential terminal paginator and atomic staging rebuild | FND-JOURNAL-UTF8-SAFETY; existing CAP-LIVE-PROJECTION closure | Streaming L0 test observes a projected-page event while the compiled CLI process is still running |
| FEAT-OBSERVABLE-CACHE-REBUILD | Feature | `USER-2026-08-19-CACHE-PROGRESS`; `USER-2026-08-19-UTF8-PANIC`; `docs/spec-servicenow-authoritative-cache-rebuild.md#approved-goal` | Phase 3 | Wave 3 | Meaningful flushed CLI progress, per-table summary, safe failure output, and staging cleanup | CAP-REBUILD-PROGRESS-EVENTS; atomic cache promotion; final typed rebuild report | FND-JOURNAL-UTF8-SAFETY and CAP-REBUILD-PROGRESS-EVENTS | Positive, delayed-page, empty-table, Unicode, and later-page-failure L0 contracts pass with workspace release gates |

Hard `requires` edges override desired delivery order and parallelism. Preserve
legacy registered IDs as opaque IDs and give them an explicit node/gate noun.

## Traceability matrix

| Authority ref | Behavior / decision | Task ID | Implementation seam | Acceptance evidence | Owner |
|---|---|---|---|---|---|
| `docs/spec-cache-rebuild-progress.md#approved-goal` | An operator sees output while the rebuild is still running | T-CRP-03 | Compiled `snow` process with piped `stderr`; CLI progress renderer | Red-first L0 test reads a completed-page line while a deliberately delayed final-table response is pending and proves the child is still running | Rust coder |
| `docs/spec-cache-rebuild-progress.md#progress-output-contract` | Progress identifies precomputed table position, table name, real requested page, projected page counts, and cumulative records without a synthetic terminal request | T-CRP-03 | Enabled rebuild list; `CacheRebuildProgressEvent`; `CacheRebuildProgressSink`; `CacheRebuildService::drain_table`; `Paginator::is_done` | Independent literal stderr expectations for short-terminal-page, multi-page, and empty-table fixtures, including absence of trailing `requesting page=2` after a short page | Rust coder |
| `docs/spec-cache-rebuild-progress.md#progress-output-contract` | Page completion is emitted only after durable staging projection | T-CRP-03 | Event emission immediately after `project_live_records_without_vault` succeeds | Later-page/projection failure test contains no false completion for the failed page and prior cache remains unchanged | Rust coder |
| `AGENTS.md#public-safe-content` | Progress contains metadata only and never leaks filter or record values | T-CRP-03 | Typed event fields and CLI renderer | L0 fixture uses a unique configured-filter marker and asserts it is absent from stdout and stderr | Rust coder |
| `docs/spec-cache-rebuild-progress.md#approved-goal` | Unicode non-header journal lines cannot panic rebuild projection or subsequent cached record reads | T-CRP-02 | Upstream `servicenow_rs::model::journal::parse_header`; local `crates/snow_core/src/query/mod.rs::parse_journal_header`; workspace dependency pin | Upstream L2 regression, compiled-CLI rebuild fixture with `work_notes`, and direct-MCP `get_record` fixture with the second em-dash counterexample | Dependency maintainer and Rust coder |
| `docs/spec-cache-rebuild-progress.md#governing-authority` | The fixed dependency commit is reviewed, published, fetchable, and pinned before downstream closure | T-CRP-02 | Upstream `servicenow_rs` checkout; public `origin`; root `Cargo.toml` and `Cargo.lock` | Upstream fmt/Clippy/test, public-safe sensitive diff review, approved push, remote SHA verification, and downstream lockfile resolution | Dependency maintainer and release owner |
| `docs/spec-servicenow-authoritative-cache-rebuild.md#approved-goal` | A page/projection failure before promotion leaves the current cache usable and removes staging artifacts | T-CRP-03 | RAII staging-cache guard around rebuild; pre-promotion failure renderer | Negative L0 page-failure test checks current cache bytes and absence of rebuild `.tmp`, `-wal`, `-shm`, and `-journal` artifacts | Rust coder |
| `docs/spec-cache-rebuild-progress.md#progress-output-contract` | Failure location comes from retained structured event state; promotion failure makes no unchanged-cache claim | T-CRP-03 | CLI-owned shared `CacheRebuildProgressState`; progress renderer; promotion error branch | Later-page L0 failure names the last requested table/page; promotion-path review forbids the unchanged-cache line | Rust coder |
| `docs/spec-cache-rebuild-progress.md#approved-goal` | Successful output retains a useful after-action per-table breakdown | T-CRP-03 | `print_servicenow_rebuild_report` | L0 stdout assertions for literal resource/table/page/record rows and aggregate totals | Rust coder |
| `AGENTS.md#behavioral-test-seams` | Tests prove consumer-visible streaming rather than callback invocation | T-CRP-01 | `crates/snow_cli/tests/cache_format_contract.rs` | Compiled-binary process with piped stderr, a 30-second delayed final-table response, pre-exit line read, `try_wait`, termination, and mutation record | QA owner |

## Implementation boundary

### Allowed changes

- Upstream `servicenow_rs/src/model/journal.rs` and its
  parser tests only for checked UTF-8 boundary handling, plus the upstream
  commit/publication step and the root `Cargo.toml` and `Cargo.lock` immutable
  revision update.
- `crates/snow_core/src/query/mod.rs::parse_journal_header` only to replace
  both unchecked byte slices with checked `str::get` boundaries and validate
  the literal `" - "` separator before parsing the remainder.
- `crates/snow_core/src/service/cache_rebuild.rs`,
  `crates/snow_core/src/types.rs`, `crates/snow_core/src/facade.rs`, and
  `crates/snow_core/src/lib.rs` for the typed progress event/sink and event
  emission around the existing sequential paginator.
- `crates/snow_cli/src/app/cache_command.rs`,
  `crates/snow_cli/src/app/output/mod.rs`, and only if required
  `crates/snow_cli/src/app/mod.rs` for immediate line-oriented rendering,
  finalization/failure diagnostics, per-table summary, and RAII staging cleanup.
- `crates/snow_cli/tests/cache_format_contract.rs` for red-first L0 consumer
  evidence and public-safe local ServiceNow fixtures.
- `crates/snow_mcp/tests/record_query.rs` and its existing support fixture only
  for direct-MCP L0 proof that the local parser treats the public-safe em-dash
  counterexample as journal body content without panicking.
- Cache-recovery README sections and this specification for truthful operator
  documentation.

### Forbidden changes / non-goals

- Do not change table selection, filters, pagination size/order, projection
  contents, schema, authentication, daemon liveness guard, or promotion order.
- Do not emit a page-complete event before staging persistence succeeds.
- Do not put progress on `stdout`, buffer it until process exit, or require a
  TTY to receive it.
- Do not expose filter expressions, identities, record data, sys_ids, URLs,
  credentials, local paths, or raw ServiceNow errors in progress event fields.
- Do not add helper-level tests for enum construction, callback invocation,
  formatting functions, derives, or serialization round trips.
- Do not vendor or duplicate `servicenow_rs` parser code in this repository
  without a separately approved dependency decision.
- Do not change daemon or MCP production behavior. The direct-MCP file is an
  existing L0 consumer seam used only as regression evidence for the local
  parser correction.
- Do not alter `promote_rebuilt_cache` in this task or infer cache preservation
  after promotion begins from the pre-promotion page-failure test.

**No-invention declaration:** Implement only the cited behavior inside this
boundary. A missing execution path, architecture, API, persistence flow,
provider behavior, or product solution is a blocker requiring an approved
authority update, not a coder decision.

## Decision gaps and blockers

- None. The inspected upstream `servicenow_rs` checkout is currently at the workspace's
  pinned revision `4d67e2bec0c5be21686a74ce51985f899d766321` with public `origin`
  configured. T-CRP-02 includes the previously omitted commit, public-safe
  review, approval-before-push, publication, remote verification, and immutable
  downstream pin. If the remote branch moves from the verified base before the
  push, stop for re-review; do not merge, rebase, force-push, vendor, or create
  a local parser fork by invention.

## Scaffold inventory

None.

## Task breakdown and coder handoffs

### T-CRP-01: red-first compiled-CLI progress and panic contracts

- System node: FND-JOURNAL-UTF8-SAFETY foundation and
  FEAT-OBSERVABLE-CACHE-REBUILD feature evidence
- Phase / Wave: Phase 1 / Wave 1
- Hard prerequisites: Foundation root
- Provides / consumes: provides independent consumer expectations for live
  streaming, Unicode safety, failure cleanup, output separation, and redaction;
  consumes the compiled `snow` binary and local ServiceNow fake
- Closure gate: each new behavior test is observed failing for the intended
  missing behavior or panic before production edits
- Authority refs: `docs/spec-cache-rebuild-progress.md#approved-goal`; `docs/spec-cache-rebuild-progress.md#progress-output-contract`; `AGENTS.md#behavioral-test-seams`
- Allowed write scope: `crates/snow_cli/tests/cache_format_contract.rs` and its
  existing public-safe fixture helpers
- Acceptance evidence: (1) add a new spawn-based helper because existing
  `run_cache_command` uses `.output()` and cannot observe streaming; give the
  last table's response a `ResponseTemplate::set_delay` of about 30 seconds,
  spawn the compiled CLI with `Stdio::piped()`, read stderr until the first
  `page=1 page_records=...` line, assert `child.try_wait()?.is_none()`, then
  kill and reap the child; do not assert staging cleanup after this deliberate
  kill; (2) add `work_notes` containing
  `Escalation note x — generic public-safe follow-up for the queue` to the
  rebuild incident fixture and assert successful cache projection; (3) fail a
  later page under normal process control and assert the old cache plus zero
  staging artifacts; (4) assert a unique filter marker is absent from both
  streams; (5) assert an initial empty table requests page 1, while a short
  terminal page has no trailing request for page 2
- Coder rule: Implement only the cited behavior inside the stated boundary; every uncited or unclear execution path, architecture, API, persistence flow, provider semantic, or product solution is a blocker requiring an approved authority update before code changes.

### T-CRP-02: both Unicode journal parser corrections and dependency publication

- System node: FND-JOURNAL-UTF8-SAFETY foundation
- Phase / Wave: Phase 1 / Wave 1 after T-CRP-01 red evidence
- Hard prerequisites: T-CRP-01 Unicode panic reproduction
- Provides / consumes: provides panic-free checked header slicing in both
  reachable parsers and a public immutable upstream dependency revision;
  consumes untrusted UTF-8 ServiceNow journal strings and the existing
  compiled-CLI/direct-MCP seams
- Closure gate: upstream parser regression, compiled-CLI Unicode rebuild
  regression, and direct-MCP `get_record` Unicode regression pass; the
  reviewed upstream commit is pushed, remotely visible, and resolved by the
  downstream lockfile
- Authority refs: `docs/spec-cache-rebuild-progress.md#approved-goal`;
  `USER-2026-08-19-CACHE-PROGRESS-PLAN-REVIEW`
- Allowed write scope: upstream
  `servicenow_rs/src/model/journal.rs` and its parser test
  module; local `crates/snow_core/src/query/mod.rs::parse_journal_header`;
  `crates/snow_mcp/tests/record_query.rs` and existing support fixture; root
  `Cargo.toml` and `Cargo.lock`
- Acceptance evidence: (1) upstream L2 regression feeds
  `Escalation note x — generic public-safe follow-up for the queue` and proves
  it is rejected as a header without panic; change `let ts = &line[..19]` to
  checked `line.get(..19)?`; (2) compiled-CLI rebuild regression supplies that
  value in `work_notes`; (3) direct-MCP `get_record` regression seeds a current-format
  cache with
  `Escalation note x — vendor - update pending`, asserts a successful
  structured response containing the full headerless body, and fails before
  the local fix from the parser panic rather than a fixture or compile error;
  local parsing must use checked `line.get(..19)?`, require
  `line.get(19..22)? == " - "`, and use checked `line.get(22..)?`; (4) in the
  upstream checkout, confirm the base and remote have not moved, run
  `cargo fmt --all --check`, `cargo clippy --all-targets -- -D warnings`,
  `cargo clippy --no-default-features --features native-tls --all-targets -- -D warnings`,
  `cargo test --lib`, `cargo test --no-default-features --features native-tls --lib`, and
  `git diff --check`, inspect the staged diff
  for credentials and organization/deployment-specific content, commit only
  the parser/test correction, obtain approval for the public side effect, push
  that fast-forward commit to `origin main`, and verify the remote branch
  reports the new SHA; (5) pin that exact SHA in root `Cargo.toml`, update
  `Cargo.lock`, and prove Cargo resolves the published revision
- Coder rule: Implement only the cited behavior inside the stated boundary; every uncited or unclear execution path, architecture, API, persistence flow, provider semantic, or product solution is a blocker requiring an approved authority update before code changes.

### T-CRP-03: typed progress events, CLI renderer, and staging guard

- System node: CAP-REBUILD-PROGRESS-EVENTS capability and
  FEAT-OBSERVABLE-CACHE-REBUILD feature
- Phase / Wave: Phase 2 / Wave 2
- Hard prerequisites: FND-JOURNAL-UTF8-SAFETY closure and T-CRP-01 red evidence
- Provides / consumes: provides ordered metadata-only progress events, flushed
  stderr rendering, final per-table stdout rows, and automatic staging cleanup;
  consumes the existing sequential paginator and atomic promotion function
- Closure gate: all focused positive and negative compiled-CLI contracts pass,
  including observation while the child process is still running
- Authority refs: `docs/spec-cache-rebuild-progress.md#progress-output-contract`; `docs/spec-servicenow-authoritative-cache-rebuild.md#approved-goal`
- Allowed write scope: `crates/snow_core/src/service/cache_rebuild.rs`,
  `crates/snow_core/src/types.rs`, `crates/snow_core/src/facade.rs`,
  `crates/snow_core/src/lib.rs`, `crates/snow_cli/src/app/cache_command.rs`,
  `crates/snow_cli/src/app/output/mod.rs`, only if required
  `crates/snow_cli/src/app/mod.rs`, and
  `crates/snow_cli/tests/cache_format_contract.rs`; T-CRP-02 exclusively owns
  the parser, direct-MCP test, dependency, and pin files
- Acceptance evidence: precompute the enabled ordered table list once and use
  its length/positions everywhere; before every `next_page()` call emit the
  request only when `Paginator::is_done()` is false; retain one CLI-owned
  `Arc<Mutex<CacheRebuildProgressState>>` shared with the renderer so the
  failure path reads the last structured table/page after `drop(core)`; focused
  L0 suite plus mutation checks that (a) buffering events until exit, (b)
  suppressing page completion, (c) routing progress to stdout, (d) emitting
  completion before projection, (e) printing the configured filter, (f)
  emitting a trailing request after a short terminal page, or (g) reporting
  `current cache unchanged` after promotion begins each cause the intended
  contract check to fail
- Coder rule: Implement only the cited behavior inside the stated boundary; every uncited or unclear execution path, architecture, API, persistence flow, provider semantic, or product solution is a blocker requiring an approved authority update before code changes.

### T-CRP-04: operator documentation and release closure

- System node: FEAT-OBSERVABLE-CACHE-REBUILD feature
- Phase / Wave: Phase 3 / Wave 3
- Hard prerequisites: T-CRP-02 and T-CRP-03 closure
- Provides / consumes: provides public-safe documentation of progress streams,
  counts, failure safety, and lack of percentage/ETA; consumes the final CLI
  contract
- Closure gate: docs match compiled output and all repository gates pass
- Authority refs: `docs/spec-cache-rebuild-progress.md#approved-goal`; `AGENTS.md#public-safe-content`
- Allowed write scope: `README.md`, `crates/snow_cli/README.md`, and this
  specification
- Acceptance evidence: compiled help/output smoke, diff review, sensitive scan,
  formatter, strict Clippy, workspace tests, and source-graph guard
- Coder rule: Implement only the cited behavior inside the stated boundary; every uncited or unclear execution path, architecture, API, persistence flow, provider semantic, or product solution is a blocker requiring an approved authority update before code changes.

## Verification and closure

- Spec structure:
  `python3 "${OPENCLAW_WORKSPACE}/foreman/scripts/validate_spec_contract.py" docs/spec-cache-rebuild-progress.md`,
  where `OPENCLAW_WORKSPACE` points to the operator's external OpenClaw
  workspace.
- Red-first evidence: run each new compiled-CLI behavior test before production
  edits, retain its missing-progress or Unicode-panic failure reason, and do not
  accept a compile error or fixture failure as red evidence. Run the direct-MCP
  `get_record` Unicode regression before the local parser edit and retain the
  parser-panic failure reason.
- Focused L0 evidence:
  `CARGO_INCREMENTAL=0 cargo test -p snow_cli --test cache_format_contract -- --nocapture`
- Direct-MCP read evidence:
  `CARGO_INCREMENTAL=0 cargo test -p snow_mcp --test record_query -- --nocapture`
- Deterministic streaming evidence: the streaming test must use a roughly
  30-second `ResponseTemplate::set_delay`, piped stderr, a blocking line read,
  and `try_wait`; it must kill/reap the child after the pre-exit assertion and
  must not claim staging cleanup for that SIGKILLed process.
- Dependency evidence: in the upstream `servicenow_rs` checkout, run
  `cargo fmt --all --check`,
  `cargo clippy --all-targets -- -D warnings`,
  `cargo clippy --no-default-features --features native-tls --all-targets -- -D warnings`,
  `cargo test --lib`, `cargo test --no-default-features --features native-tls --lib`, and
  `git diff --check`; review the complete
  public diff for sensitive or deployment-specific content; obtain approval,
  push the fast-forward commit to `origin main`, verify the remote SHA, and
  confirm root `Cargo.lock` resolves that published immutable revision.
- Shared behavior:
  `CARGO_INCREMENTAL=0 cargo test --workspace --all-features`
- Static gates: `cargo fmt --all --check` and
  `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- Repository gates: `git diff --check`,
  `python3 scripts/check_rust_source_graph.py`, and
  `bash scripts/sensitive_scan.sh HEAD` plus a working-tree scan covering the
  new untracked specification and fixtures.
- Runtime smoke: run the rebuilt release binary against a public-safe local
  ServiceNow fake and prove at least one page-complete line arrives before
  process exit. A real installed/live ServiceNow rebuild remains a separately
  reported operator cutover gate.
- Failure-contract evidence: the normal later-page failure test alone owns the
  staging-cleanup and byte-identical-current-cache assertion. The streaming
  SIGKILL case owns neither. Review or a focused promotion-error seam must prove
  that a promotion failure does not emit `current cache unchanged`; this task
  makes no stronger post-promotion preservation claim. Assert failure message
  text without depending on the colored literal `Error:` prefix from
  `crates/snow_cli/src/main.rs`.
- Mutation evidence: temporarily suppress or misroute each material event,
  emit a completion before projection, emit a synthetic request after
  `Paginator::is_done()`, report mismatched table totals, discard the shared
  failure state, restore either unsafe byte slice, or claim an unchanged cache
  on promotion failure; the corresponding contract test/review gate must fail
  for the intended reason.
- Test-integrity disposition: no derive, callback-call-count, enum-shape,
  self-generated serialization, or formatting-helper tests are accepted as
  closure evidence. The suite remains `design-only` until red-first executions
  are captured and cannot be rated `HEALTHY` before executed or mutation-backed
  evidence exists.
- Stub closure: every scaffold seam is implemented with its cited proof, or is
  explicitly reported as a typed blocked/untrusted state; none is called
  complete merely because it compiles.
