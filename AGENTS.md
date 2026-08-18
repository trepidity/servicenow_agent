# Agent Rules

## Public-Safe Content

This repository must stay public-safe. Do not add organization-specific names,
vault paths, 1Password item names, hostnames, tenant identifiers, internal URLs,
usernames, record IDs, credential labels, environment values, or other
deployment-specific details to committed code, docs, tests, fixtures, snapshots,
or generated artifacts.

Use generic placeholders such as `example.service-now.com`, `op://vault/item`,
`shared-vault`, `service-account-item`, and `user@example.com`. Keep real values
only in ignored local files or the operator's external secret manager.

## Behavioral Test Seams

Tests are owed to behaviors observed at a consumer seam, not to functions or
modules. Every test must name the seam it exercises. Work that cannot name one
is blocked as `Undeclared Seam` rather than pushed down to a helper-level
substitute.

### L0 — consumer seams

- **CLI:** execute the compiled `snow` binary against temporary generic config,
  vault, and cache paths; assert exit status and consumer-visible stdout,
  stderr, files, or cache state.
- **Daemon JSON-RPC:** send a request through the daemon's framed local
  transport or its established request dispatcher and assert the exact
  consumer-visible result/error contract.
- **MCP:** list or invoke tools through both the direct and daemon-backed MCP
  paths and assert identical structured payloads wherever both transports
  expose the capability.
- **Durable artifact:** drive a public core/CLI operation and inspect the
  resulting generic vault or current-format cache projection. The SQLite layout
  is disposable derived state, not a compatibility contract, and earns no
  legacy-format test.
- **Governed write:** drive plan/apply through daemon JSON-RPC or MCP using
  local ServiceNow and state-store fakes; assert confirmation, policy,
  idempotency, replay, concurrency, receipt, and audit outcomes.

### L1 — wire contract

Committed, independently authored JSON-RPC and MCP request/response
expectations, used where the encoding crosses a process boundary. An
expectation generated from the same code it verifies is not evidence.

### L2 — hard logic

Reserved for an explicitly named algorithmic, state-machine, parser,
concurrency, or boundary-arithmetic earn. Current qualifying areas: Business
Application graph traversal, and story pending recovery/replay state machines.

### Not evidence

Tests of derives, constructors, getters, module shapes, or mock call counts,
and self-generated serialization round trips, do not count as evidence of
behavior.

- A pure refactor is evidenced by a green baseline before and a green run
  after.
- A changed behavior is evidenced by a red-first consumer test that fails for
  the intended old behavior before the production edit lands.
