# Implementation Spec: Governed ServiceNow Story Work-Note Support

## Authority and scope

### Approved goal

Make the existing governed `work_note_plan_add` / `work_note_apply_add` flow
usable for ServiceNow Story records resolved as `rm_story`, without weakening
the live proof that a resolved record supports `work_notes`. The direct user
directive on 2026-08-29 is the product authority; this document records it at
`docs/spec-rm-story-work-note-support.md#approved-goal` because this repository
has no accepted requirement-ID scheme for this behavior.

### Governing authority

- `docs/spec-rm-story-work-note-support.md#approved-goal` — requested Story
  work-note outcome.
- `crates/snow_daemon/src/work_note_write.rs#work_note_field_support` — the
  existing plan/apply gate and its distinct supported, unsupported, and
  unavailable outcomes.
- `crates/snow_core/src/service/descriptor.rs#supports_field` and
  `crates/snow_core/src/context.rs#table_parent` — record-derived, live
  dictionary and hierarchy discovery boundary.
- `crates/snow_daemon/src/rpc/work_note_support_tests.rs` — declared L0 daemon
  JSON-RPC seam for governed work-note behavior.
- `AGENTS.md#public-safe-content` and `AGENTS.md#behavioral-test-seams` —
  public-safe artifacts and consumer-seam proof requirements.

### Scope

- Preserve the existing two-step work-note operation, confirmation,
  idempotency, audit, and apply-time recheck.
- Let the core support probe succeed without a table-hierarchy request when
  live `sys_dictionary` metadata on the already-resolved target table directly
  proves that `work_notes` exists.
- Preserve live ancestor discovery when the target table does not directly
  define the field; its result remains required before an inherited field can
  be treated as supported.
- Return a safe typed discovery reason (`acl_denied`,
  `not_returned_by_instance`, `not_supported_by_operation`, or
  `upstream_error`) with `WORK_NOTES_DISCOVERY_UNAVAILABLE`; do not expose
  upstream error text.
- Establish an operator-owned preflight that distinguishes a deployed source
  defect from missing metadata/read/write authorization on the target
  ServiceNow instance.
- Verify that Mullet's existing named `servicenow.work_note.plan_add` and
  `servicenow.work_note.apply_add` consumer route continues to consume the
  daemon contract without a generic-write fallback.

### Non-goals

- No caller-selected table or field, generic `notes` fallback, static schema,
  dictionary cache, raw ServiceNow query, or direct provider write from
  Mullet.
- No weakening of the rule that an unproven field produces neither a plan,
  confirmation token, idempotency key, nor write.
- No ServiceNow table extension, form/UI customization, role grant, credential
  change, or live write from a code task. Those remain operator-owned actions.
- No change to Story state/percent-complete updates, closeout requirements, or
  unrelated daemon/MCP capabilities.
- No organization-specific record IDs, people, hostnames, credential labels,
  or note content in committed material.

## System progression and dependency map

| Node ID | Node type | Authority refs | Phase | Wave | Provides | Consumes | Hard prerequisites | Closure gate |
|---|---|---|---|---|---|---|---|---|
| FND-RM-WN-001 | Foundation | `docs/spec-rm-story-work-note-support.md#scope`; `crates/snow_core/src/service/descriptor.rs#supports_field` | Phase 1 | Wave 1 | Direct-table, live metadata proof that short-circuits hierarchy only after `work_notes` is observed | Existing record-derived table, Table API client, `FieldSupport` | Foundation root and clean Snow handoff base | Red-first L0 daemon fake resolves a `STRY` record on `rm_story`, sees a direct active `work_notes` dictionary row, denies any hierarchy request, and returns a plan |
| FND-RM-WN-002 | Foundation | `docs/spec-rm-story-work-note-support.md#scope`; `crates/snow_daemon/src/work_note_write.rs#work_note_field_support` | Phase 1 | Wave 1 | Classified installed-state evidence for the target Story path | Released daemon, approved target environment, existing named work-note plan operation | Foundation root and a reachable installed daemon | Read-only plan preflight records only result class and safe reason: planned, unsupported, unavailable reason, or transport/runtime blocked |
| CAP-RM-WN-003 | Capability | `docs/spec-rm-story-work-note-support.md#scope`; `crates/snow_daemon/src/work_note_write.rs#handle_work_note_apply_add` | Phase 2 | Wave 2 | Released, governed Story work-note plan/apply path that remains fail-closed | FND-RM-WN-001; FND-RM-WN-002; existing policy, confirmation, audit, and write ACLs | FND-RM-WN-001 and FND-RM-WN-002 closure gates | Installed daemon plans a Story note; after explicit approval, apply returns a receipt proving the planned `work_notes` field landed |
| FEAT-RM-WN-004 | Feature | `docs/spec-rm-story-work-note-support.md#scope`; `Mullet/src/ops/servicenow/work-note.ts` | Phase 3 | Wave 3 | Mullet can complete the existing named governed Story-note flow with no bypass | CAP-RM-WN-003 and Mullet typed ServiceNow port | CAP-RM-WN-003 closure gate, released daemon advertised by `contract_info`, and clean Mullet handoff base | Mullet public dispatch receives the daemon plan and, after explicit confirmation, its apply result contains landed-field evidence; unavailable states remain typed refusals |

Hard `requires` edges override delivery order. A green local fake does not
close installed authorization, and a manual UI entry does not prove the daemon
path.

## Traceability matrix

| Authority ref | Behavior / decision | Task ID | Implementation seam | Acceptance evidence | Owner |
|---|---|---|---|---|---|
| `docs/spec-rm-story-work-note-support.md#scope`; `crates/snow_core/src/service/descriptor.rs#supports_field` | A direct active dictionary definition on the resolved Story table is sufficient proof and must avoid unnecessary ancestry I/O | T1 | `DescriptorService::supports_field` | Red-first L0 daemon test with generic `STRY0000001` / `rm_story` fixture returns a plan while a forbidden `sys_db_object` fake observes zero requests | Rust daemon coder |
| `docs/spec-rm-story-work-note-support.md#scope`; `crates/snow_daemon/src/work_note_write.rs#work_note_field_support` | Missing proof remains distinct from confirmed absence and does not issue plan authority | T1 | `work_note_field_support`, plan response, and apply recheck | L0 tests retain `-32053` for confirmed absence and `-32054` with a safe reason for metadata/ACL unavailability; neither response carries plan tokens | Rust daemon coder |
| `AGENTS.md#behavioral-test-seams`; `crates/snow_daemon/src/rpc/work_note_support_tests.rs` | Tests exercise governed behavior through daemon JSON-RPC instead of descriptor helpers | T1 | Existing request dispatcher plus local ServiceNow/state-store fakes | Focused test fails before the direct-table support implementation, then passes; an apply regression proves discovery is repeated before write I/O | QA owner |
| `docs/spec-rm-story-work-note-support.md#scope`; `crates/snow_core/src/context.rs#table_parent` | Inherited fields still require verified hierarchy traversal; an inaccessible hierarchy is not converted into success | T1 | `CoreContext::table_parent` consumed by `DescriptorService::supports_field` | L0 fake with no direct field and denied hierarchy returns `WORK_NOTES_DISCOVERY_UNAVAILABLE`, makes no plan, and makes no write request | Rust daemon coder |
| `docs/spec-rm-story-work-note-support.md#scope`; `crates/snow_daemon/src/work_note_write.rs#handle_work_note_plan_add` | Installed failure is classified before authorization is changed | T2 | Installed daemon JSON-RPC `work_note_plan_add` | Public-safe operator record captures the result class and safe reason only; no Story mutation occurs during this preflight | ServiceNow operator + QA owner |
| `docs/spec-rm-story-work-note-support.md#scope`; `AGENTS.md#public-safe-content` | Any ServiceNow remediation is least-privilege and does not alter source semantics | T2 | ServiceNow authorization configuration outside the repository | Operator verifies metadata reads needed for the resolved table/ancestors and existing record-field write authorization; follow-up plan succeeds | ServiceNow operator |
| `docs/spec-rm-story-work-note-support.md#scope`; `Mullet/src/ops/servicenow/work-note.ts` | Mullet consumes the existing named plan/apply contract and never falls back to a generic write | T3 | Mullet public `dispatch()` plus typed ServiceNow port/daemon runtime | Registered-operation test preserves opaque plan tokens and typed daemon refusal; installed Mullet plan/apply proof is separately recorded | Mullet coder + QA owner |

## Implementation boundary

### Allowed changes

- `crates/snow_core/src/service/descriptor.rs` and, only if needed to retain
  typed hierarchy-error classification, `crates/snow_core/src/context.rs`.
- `crates/snow_daemon/src/work_note_write.rs` for safe propagation of existing
  `FieldSupport` unavailability reasons.
- `crates/snow_daemon/src/rpc/work_note_support_tests.rs` for L0 daemon
  JSON-RPC regressions using public-safe Story fixtures.
- This specification and a narrowly scoped public-safe operator runbook,
  should the deployed preflight establish an authorization requirement.
- In Mullet, existing work-note operation tests only, from a clean isolated
  handoff base. No Mullet source change is assumed or authorized by this plan.

### Forbidden changes / non-goals

- Do not add a public metadata endpoint, caller-supplied table, arbitrary
  journal field, cache-based support answer, daemon bypass, direct Mullet
  ServiceNow call, or unplanned write.
- Do not mark `rm_story` supported from its name, a UI observation, a static
  allowlist, or a previous successful Story.
- Do not suppress `WORK_NOTES_UNSUPPORTED` or
  `WORK_NOTES_DISCOVERY_UNAVAILABLE`, and do not alter confirmation,
  idempotency, concurrency, audit, or apply-time recheck semantics.
- Do not fold unrelated dirty Mullet Story-update work or unrelated dirty Snow
  changes into this slice.

**No-invention declaration:** Implement only the cited behavior inside this
boundary. A missing execution path, architecture, API, persistence flow,
provider behavior, or product solution is a blocker requiring an approved
authority update, not a coder decision.

## Decision gaps and blockers

- **B-RM-WN-001 — installed daemon unavailable.** The local socket was not
  reachable during investigation, so the exact live result for the target
  Story is unverified. T2 must first establish daemon reachability and run the
  non-mutating plan preflight; retrying an unavailable socket is not evidence.
- **B-RM-WN-002 — actual metadata source remains unproven.** If the preflight
  returns `WORK_NOTES_DISCOVERY_UNAVAILABLE`, the safe reason determines the
  operator path: `acl_denied` requires least-privilege metadata access;
  `not_returned_by_instance` requires instance-side investigation; and
  `upstream_error` requires transport/provider diagnosis. No code path may
  convert any of these into support.
- **B-RM-WN-003 — confirmed absence is a product decision, not a bug fix.** If
  the installed preflight returns `WORK_NOTES_UNSUPPORTED`, `rm_story` has not
  been proven to carry the requested field. Do not use another field or table
  as a substitute. An approved separate requirement must identify the
  canonical journal destination before implementation can continue.
- **B-RM-WN-004 — live mutation needs explicit confirmation.** The installed
  plan is non-mutating to ServiceNow, but apply changes an external record.
  T3 may run apply only with an approved note body and the exact daemon-issued
  confirmation, idempotency, and concurrency values.
- **B-RM-WN-005 — dirty worktrees are not a handoff base.** Current Snow and
  Mullet working trees contain unrelated edits. T1 and T3 must use isolated
  clean worktrees or a reviewed coherent commit; no current diff is evidence
  of release readiness merely because its focused test is green.

## Scaffold inventory

| Seam ID | Authority refs | Crate / file / symbol | Signature / contract | Safe unresolved state | Owner | Completion evidence |
|---|---|---|---|---|---|---|
| SC-RM-WN-DIRECT | `crates/snow_core/src/service/descriptor.rs#supports_field`; `docs/spec-rm-story-work-note-support.md#scope` | `DescriptorService::supports_field` | Record-derived `table, field -> FieldSupport<bool>` only; a direct live dictionary match may return available true before hierarchy lookup | Typed fail-closed `FieldSupport::Unavailable` or available false; never inferred true | Rust daemon coder | L0 Story plan test and retained negative tests |
| SC-RM-WN-LIVE | `crates/snow_daemon/src/work_note_write.rs#handle_work_note_plan_add`; `docs/spec-rm-story-work-note-support.md#decision-gaps-and-blockers` | Installed daemon JSON-RPC `work_note_plan_add` | Existing named plan request produces a plan or one typed refusal class without external record mutation | Blocked runtime preflight; no apply or manual substitution | ServiceNow operator + QA owner | Sanitized result-class record |
| SC-RM-WN-MULLET | `Mullet/src/ops/servicenow/work-note.ts`; `docs/spec-rm-story-work-note-support.md#scope` | Mullet `dispatch()` operation and typed port | Existing named plan/apply consumes opaque daemon authority unchanged | Typed unavailable/unsupported refusal; no generic-write fallback | Mullet coder | Public dispatch regression plus installed handoff evidence |

## Task breakdown and coder handoffs

### T1: Direct-table Story metadata proof

- System node: FND-RM-WN-001 direct-table metadata foundation.
- Phase / Wave: Phase 1 / Wave 1.
- Hard prerequisites: Foundation root and a clean Snow handoff base; this specification must be included in that base.
- Provides / consumes: Provides a direct `rm_story.work_notes` proof path; consumes the existing record-derived core descriptor and existing daemon plan/apply contract.
- Closure gate: A red-first L0 daemon JSON-RPC test for a generic Story record passes only after a direct active `work_notes` metadata row avoids hierarchy I/O; confirmed absence, denied discovery, and apply-time recheck tests remain green.
- Authority refs: `docs/spec-rm-story-work-note-support.md#scope`; `crates/snow_core/src/service/descriptor.rs#supports_field`; `crates/snow_daemon/src/work_note_write.rs#work_note_field_support`.
- Allowed write scope: `crates/snow_core/src/service/descriptor.rs`, `crates/snow_core/src/context.rs` only if required for typed hierarchy classification, `crates/snow_daemon/src/work_note_write.rs`, and `crates/snow_daemon/src/rpc/work_note_support_tests.rs`.
- Acceptance evidence: Record the red failure for the new Story-specific direct-field test, then its green result. Run `cargo test -p snow_daemon work_note_ -- --nocapture`, `cargo fmt --check`, and `git diff --check`. The test must drive `work_note_plan_add` through the daemon dispatcher and assert a plan result plus zero hierarchy requests; it must not test `supports_field` directly.
- Coder rule: Implement only the cited behavior. Surface every uncited path or solution as a blocker requiring an approved authority update.

### T2: Installed metadata and authorization preflight

- System node: FND-RM-WN-002 installed Story evidence foundation.
- Phase / Wave: Phase 1 / Wave 1.
- Hard prerequisites: A reachable installed daemon; T1 is not a prerequisite for collecting the current failure class.
- Provides / consumes: Provides a safe diagnosis of installed Story work-note availability; consumes the released daemon, the existing named plan operation, and the approved target environment.
- Closure gate: A non-mutating Story plan request returns either a valid plan, `WORK_NOTES_UNSUPPORTED`, or `WORK_NOTES_DISCOVERY_UNAVAILABLE` with a safe reason. Socket/cache/runtime failure remains blocked and is repaired before the field claim is made.
- Authority refs: `docs/spec-rm-story-work-note-support.md#decision-gaps-and-blockers`; `crates/snow_daemon/src/work_note_write.rs#handle_work_note_plan_add`.
- Allowed write scope: No repository code. Operator may repair daemon lifecycle/cache state and narrowly grant the runtime identity the metadata reads actually required, subject to existing ServiceNow authorization change control.
- Acceptance evidence: Capture the daemon contract version, method advertisement, and plan outcome category without recording real identifiers or note text. If authorization is changed, repeat the same plan and retain before/after result categories. Do not run apply in this task.
- Coder rule: Implement only the cited behavior. Surface every uncited path or solution as a blocker requiring an approved authority update.

### T3: Governed deployment and Mullet consumption

- System node: CAP-RM-WN-003 and FEAT-RM-WN-004 governed Story-note capability.
- Phase / Wave: Phase 2 / Wave 2, then Phase 3 / Wave 3.
- Hard prerequisites: FND-RM-WN-001 and FND-RM-WN-002 closure gates; released Snow daemon; clean Mullet handoff base; explicit approval for the external apply.
- Provides / consumes: Provides a released Story note path in Mullet; consumes the unchanged named daemon plan/apply interface and opaque authority tokens.
- Closure gate: Mullet public dispatch receives a valid plan, and the explicitly approved apply returns daemon receipt evidence that the intended `work_notes` field landed. Any unavailable/unsupported result remains a typed user-visible refusal.
- Authority refs: `docs/spec-rm-story-work-note-support.md#scope`; `Mullet/src/ops/servicenow/work-note.ts`; `Mullet/src/integrations/servicenow/typed-consumer-port.ts`.
- Allowed write scope: Release/install artifacts and operator-owned runtime configuration. In Mullet, only focused existing work-note tests if compatibility evidence exposes a defect; do not modify the unrelated dirty Story-update files.
- Acceptance evidence: Daemon contract inspection reports the existing work-note methods; Mullet registered-operation/public-dispatch test proves token forwarding and typed refusal behavior; the approved live plan/apply records a receipt. Run applicable Snow release gates, Mullet focused tests, and `git diff --check` in each clean handoff. Keep source/test, installed runtime, and live external-write evidence distinct.
- Coder rule: Implement only the cited behavior. Surface every uncited path or solution as a blocker requiring an approved authority update.

## Verification and closure

- Spec structure: `python3 /Users/jared/.openclaw/workspace/foreman/scripts/validate_spec_contract.py docs/spec-rm-story-work-note-support.md`.
- T1 local evidence: red-first daemon JSON-RPC Story regression, then
  `cargo test -p snow_daemon work_note_ -- --nocapture`, `cargo fmt --check`,
  and `git diff --check`.
- T2 installed evidence: daemon reachability, `contract_info` method
  advertisement, and the non-mutating Story plan result class. This does not
  prove an external write.
- T3 external evidence: only the explicitly approved apply plus its receipt
  proves the provider mutation; Mullet public-dispatch evidence alone proves
  consumer routing, not installed authorization.
- Public-safety evidence: inspect the staged diff and fixture/doc additions for
  real records, identities, hostnames, note bodies, paths, or credentials.
- Stub closure: every scaffold seam is implemented with its cited proof, or is
  explicitly reported as a typed blocked/untrusted state; none is called
  complete merely because it compiles.
