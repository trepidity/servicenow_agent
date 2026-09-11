# Read-only ServiceNow identifier resolution

`record_resolve` is available on daemon JSON-RPC and direct/daemon-backed MCP.
It accepts `{ "sys_id": "abcdef0123456789abcdef0123456789", "cursor": "optional opaque cursor" }`.
Omit `cursor` for a new lookup. No table, number, query, or write field is accepted.

The shared core probes common base tables first, then tables discovered from the
authenticated `sys_db_object` catalog. Custom tables are eligible without adding
an enum variant or allowlist entry. Each call probes at most 32 tables, eight at
a time, with per-request deadlines. No record cache or vault projection is written.

- `resolved`: `record` contains the exact `sys_id`, provider-derived `table`,
  descriptive `resource_type`, complete field JSON values/display values/reference
  links, and `data_model`. Known work types are classified; others are `dynamic`
  and retain their actual table and model.
- `searching`: continue with the same sys_id and returned opaque `cursor`.
  Cursors bind to one identifier and core runtime, expire after five idle minutes,
  and are bounded to 32 retained sessions.
- `document`: the resolved record/model JSON exceeds the inline transport budget.
  Append `content` in `offset_bytes` order and follow the returned cursor until
  null; the result reconstructs the same `record` object as the inline form.
  Pages include the full-document SHA-256 hash and byte length. Cursors are
  replay-safe, including the discovery-to-first-document-page handoff. A separate
  pool retains up to 32 immutable document snapshots (16 MiB each maximum), with
  five-minute idle expiry; capacity and oversize failures never truncate data.
- `not_found_or_inaccessible`: no readable match in the scanned catalog. This is
  not proof of absence or deletion; record/catalog ACLs and failed reads can hide
  data. `unreadable_tables` reports failed probes separately from ordinary misses.

A provider-returned subclass is validated and refetched for its full fields.
Wrong identity, unsafe class paths, conflicting observed classes, or changing
classes fail closed. A hidden class on a polymorphic parent is not type proof.
Metadata access failure preserves readable fields and returns explicit model
unavailability; readable metadata does not grant field-write permission.

Mullet exposes this through `mullet_servicenow_lookup` with `sys_id` or one sys_id
in `ids`, without a resource type. It supplies exact discovery continuations and
pages oversized record/model JSON through its existing document mechanism.
Existing number-based reads and all governed-write boundaries are unchanged.

Evidence: `rpc::record_resolution_tests` exercises real daemon dispatch/HTTP and
direct/bridge parity. The ignored `daemon_identifier_end_to_end_via_mullet_stdio`
test is mandatory in Mullet's explicit cross-checkout release gate and proves
Story, numberless user, and paged custom-record resolution through the full path.
