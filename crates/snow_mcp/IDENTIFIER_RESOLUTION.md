# Read-only ServiceNow identifier resolution

`record_resolve` is available on daemon JSON-RPC and direct/daemon-backed MCP.
It accepts `{ "sys_id": "abcdef0123456789abcdef0123456789", "cursor": "optional opaque cursor" }`.
Omit `cursor` for a new lookup. Optional `table` scopes a sys_id to its canonical class.

The shared core probes common base tables first, then tables discovered from the
authenticated `sys_db_object` catalog. Custom tables are eligible without adding
an enum variant or allowlist entry. Each call probes at most 32 tables, eight at
a time, with per-request deadlines. No record cache or vault projection is written.

- `resolved`: `record` contains the exact `sys_id`, provider-derived `table`,
  descriptive `resource_type`, complete field JSON values/display values/reference
  links, and `data_model`. `data_type`, when readable, supplies the provider table
  label. Existing domain aliases remain compatible; other resource types use the
  canonical table name instead of a generic `dynamic` discriminator.
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
Number-based reads share the core resolver described below. Governed-write
boundaries are unchanged.

## Exact names

The same `record_resolve` tool and daemon method also accept exactly one `name`
instead of `sys_id`, with an optional `table` and opaque `cursor`:

```json
{ "name": "Example Sprint", "table": "rm_sprint" }
{ "name": "Example Release", "table": "rm_release_scrum" }
{ "name": "Example Support", "table": "sys_user_group" }
{ "name": "Example Developer", "table": "sys_user" }
{ "name": "Example Widget", "table": "x_example_widget" }
{ "name": "Example Name" }
```

Exact name resolution is available across MCP, Mullet, and the daemon.
A table only narrows exact
name discovery. Arbitrary queries, caller-selected fields, and writes remain
unavailable through this resolver. Existing `get_record` also recognizes SPNT
numbers and accepts any valid canonical table for table/sys_id reads.

The live UI model proves which standard name, title, number, login, and email
fields exist. The system dictionary identifies custom display fields, walking
parent definitions when necessary. The query contains exact equality predicates
only; names retain internal whitespace. Every returned record must independently
match at least one proven field. Unknown models and ACL failures never become
unfiltered table reads or proof of absence.

`name_matches` returns a page of candidate references: `sys_id`, provider-derived
`table`, `resource_type`, matched `name`, optional `number`, and `matched_fields`.
Use `record_resolve` with the selected sys_id for full fields and model metadata.
The response also includes `match_count`, `complete`, `scanned_tables`,
`unreadable_tables`, and `cursor`. Return every duplicate candidate; do not select
one automatically. `complete` means all requested scopes were inspected with
readable metadata, subject to record ACLs. A null cursor with `complete=false`
means discovery exhausted its accessible scopes but cannot be exhaustive.

Without a table, prioritize common reference/work tables, then page the live
catalog. Each call inspects at most two tables and returns at most 20 candidates.
Table work has a ten-second deadline. Name cursors bind to the exact name and
table, retain replayable positions for five minutes, and have bounded storage
(128 positions; 10,000 distinct matches per search). Capacity failures are
explicit; no result truncation or record persistence occurs.

Provider semantics follow ServiceNow's
[display field rules](https://www.servicenow.com/docs/r/platform-administration/t_SelectTheDisplayValue.html).
Behavioral evidence is in `rpc::name_resolution_tests` and
`rpc::sprint_lookup_tests`, including direct/bridge parity, custom fields,
duplicate paging, rejected provider mismatches, and ACL uncertainty. Mullet's
cross-repository gate requires `daemon_names_end_to_end_via_mullet_stdio`.

Evidence: `rpc::record_resolution_tests` exercises real daemon dispatch/HTTP and
direct/bridge parity. The ignored `daemon_identifier_end_to_end_via_mullet_stdio`
test is mandatory in Mullet's explicit cross-checkout release gate and proves
Story, numberless user, and paged custom-record resolution through the full path.

## Shared table and type translation

`record_resolve` accepts `resource_type` as a prefix, table name, table label,
reference field name, or reference field label. With no record selector it
returns `type_matches` containing candidate `{table, label, resource_type,
source}` definitions. The same selector can scope a name, number, or sys_id.
Labels and prefixes can be ambiguous; a record read requires one resolved type.

```json
{"resource_type":"DMND"}
{"resource_type":"Demand"}
{"resource_type":"dmn_demand"}
{"resource_type":"x_example_planning_note","name":"Example plan"}
{"number":"XPN0000001"}
```

MCP and Mullet pass these selectors to the shared core, without maintaining
name/prefix maps. `sys_db_object` provides canonical table names and labels;
`sys_dictionary` supplies reference targets; `sys_number` provides prefix-to-table
mappings. Existing core compatibility names are aliases, not an admission list.
When numbering metadata is denied or omits a prefix, the core can use its
configured/standard registry and reports `source: core_prefix_fallback`.
Unknown prefixes are not invented; an exact number can also be resolved from a
readable task's actual class, or using an explicit canonical table scope.

Generic `get_record` and fresh reads use the same number resolver. Generic
`table`/`sys_id` reads accept any valid table identifier; domain-specific tools
such as `resource_plan_get` retain their operation-specific contracts. Type
resolution does not grant write capability. No new surface entry is needed for
a custom readable table. Provider metadata remains subject to ACLs.

ServiceNow documents numbering metadata in [Record numbering](https://www.servicenow.com/docs/r/platform-administration/c_ManagingRecordNumbering.html).
Evidence includes `rpc::record_type_tests` and the cross-repository
`daemon_types_end_to_end_via_mullet_stdio` gate.
