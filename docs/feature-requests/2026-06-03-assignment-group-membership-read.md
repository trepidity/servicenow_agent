# Feature Request: Read capability for ServiceNow assignment-group / user-group membership

**One-line summary:** Add read-only MCP tools to reach `sys_user_grmember` from **both** directions — `user_group_get` (group → roster) and `user_group_search` (group name → sys_id), plus `user_group_membership` (user → their groups / membership check) — with **`sys_id` *or* name accepted as the selector on both the group and the user**.

- **Date:** 2026-06-03
- **Requester:** Internal identity engineering requester
- **Target project:** `servicenow_agent` (`snow_daemon` / `snow_mcp`)
- **Posture:** read-only, no confirmation, explicit read-role rollout — mirrors the existing `user_lookup` / `user_search` / `business_application_*` / `server_*` read primitives while recognizing roster-read privacy risk.

---

## Problem / motivation

There is currently **no way to pull the members (roster) of a `sys_user_group`** through the snow_daemon MCP surface. The relation table that holds group membership — `sys_user_grmember` (`user` → `sys_user`, `group` → `sys_user_group`) — is not reachable by any registered tool:

- **`get_record`** only accepts the tables in `RECORD_LOOKUP_ALLOWED_TABLES` (`crates/snow_core/src/lib.rs:1190`): `dmn_demand`, `dmn_demand_task`, `resource_plan`, `pm_project`, the `business_application` variants, and the `cmdb_ci_server` variants. **Neither `sys_user_group` nor `sys_user_grmember` is in that list**, and the schema enum is asserted verbatim in `crates/snow_mcp/tests/contract_tools_list.rs:177` (`get_record_schema_advertises_number_or_allowed_table_sys_id_lookup`), so it cannot be widened casually.
- **`user_lookup` / `user_search`** resolve **individual** `sys_user` records only — by `user_name` / `email` / `employee_number` / `sys_id` / name substrings (`crates/snow_core/src/lib.rs:1808` and `:1871`). They have no group filter and no membership concept.
- **`get_children`** is task-hierarchy only — it takes a record `number` and walks the task tree (`crates/snow_core/src/query/mod.rs:128`). It has nothing to do with group membership.

So group membership is unreachable end to end.

### Concrete blocked use case

This blocked a real task: pulling the members of an assignment group such as **"Example Assignment Group"** — `sys_user_group` sys_id `0123456789abcdef0123456789abcdef`. There was no tool to return who is on that group; the only fallback is the ServiceNow UI.

### Recurring need

This is not a one-off. The roster read is needed regularly to:

- **Enumerate a team roster** (e.g., an identity engineering assignment group) for planning and load balancing.
- **Validate a change's `assignment_group`** before creating a CHG/CTASK — confirm the group exists and that the intended owner is actually a member.
- **Answer "who is on this group?"** before assigning work, routing an approval, or attesting ownership.

The daemon already understands `sys_user_group` as a reference primitive in several places (`crates/snow_core/src/enrich/rules.rs:355`, `crates/snow_core/src/resource/business_application.rs:606-611,630`, `crates/snow_daemon/src/transport.rs:109-143`, `crates/snow_core/src/lib.rs:1294`), so groups are first-class as *references* — what's missing is the **membership read**.

---

## Proposed solution

Add a new **read-only** primitive exposed as three MCP tools, modeled exactly on the existing `user_search` / `user_lookup` read pattern (live ServiceNow REST query, cache-backed when practical, read-only, role-gated, no confirmation).

### Product definition

For v1, "assignment group" and "user group" both mean direct ServiceNow `sys_user_group` membership through `sys_user_grmember`. The feature does **not** read AD/security-group sources, does **not** expand nested/transitive group membership, and does **not** create/update/delete groups or memberships.

### Tool 1 — `user_group_get` (primary)

Resolve one assignment/user group and return the group plus its member roster.

**Input schema** (mirror `user_lookup_arg_schema` / `server_get_arg_schema` conventions — `additionalProperties: false`, no top-level `oneOf`/`anyOf`/`allOf`, no `required` block; runtime enforces the exactly-one selector rule):

```json
{
  "type": "object",
  "additionalProperties": false,
  "description": "Provide exactly one of sys_id or exact group name. Returns the sys_user_group plus its sys_user_grmember roster joined to sys_user. Read-only.",
  "properties": {
    "sys_id": {
      "type": "string",
      "pattern": "^[0-9a-fA-F]{32}$",
      "description": "32-character sys_user_group sys_id, e.g. 0123456789abcdef0123456789abcdef"
    },
    "name": {
      "type": "string",
      "description": "Exact sys_user_group.name, e.g. \"Example Assignment Group\""
    },
    "active_members_only": {
      "type": "boolean",
      "default": true,
      "description": "Filter the roster to active sys_user records (member.user.active=true). Omitted means true."
    },
    "limit": {
      "type": "integer",
      "minimum": 1,
      "maximum": 500,
      "default": 200,
      "description": "Maximum members returned"
    }
  }
}
```

**Output schema shape** (`object_schema()` at the descriptor level, like every other read tool; the concrete payload):

```json
{
  "group": {
    "sys_id": "0123456789abcdef0123456789abcdef",
    "name": "Example Assignment Group",
    "description": "...",
    "active": true,
    "manager": { "sys_id": "fedcba9876543210fedcba9876543210", "name": "Example Manager" },
    "email": "group@example.com"
  },
  "members": [
    {
      "sys_id": "<sys_user.sys_id>",
      "user_name": "example.user",
      "name": "Example User",
      "email": "user@example.com",
      "title": "Engineer",
      "active": true
    }
  ],
  "member_count": 12,
  "truncated": false
}
```

Reuse the existing `USER_LOOKUP_FIELDS` projection (`crates/snow_core/src/lib.rs:142`: `sys_id, user_name, name, first_name, last_name, email, employee_number, active, department, location, title`) for each member so the member shape matches `user_search` output exactly.

**Behavior:**

1. Resolve the group. If `sys_id` given, use it directly. If `name` given, query `sys_user_group` for an exact `name` match; bail if zero or >1 match (same one-row discipline as `lookup_user`, `crates/snow_core/src/lib.rs:1849-1855`).
2. Query `sys_user_grmember` filtered by `group=<group sys_id>` to get member `user` references.
3. Join to `sys_user` (resolve each `user` reference) projecting `USER_LOOKUP_FIELDS`.
4. When `active_members_only` (default `true`), drop members whose `sys_user.active != true`.
5. Apply `limit` (default `200`, max `500`); set `truncated: true` when more rows exist than `limit`.

### Tool 2 — `user_group_search` (companion)

Callers usually have a group **name**, not a sys_id. Provide a substring search that resolves group name → candidate groups (no roster), so an agent can disambiguate before calling `user_group_get`.

**Input schema:**

```json
{
  "type": "object",
  "additionalProperties": false,
  "description": "Live-search sys_user_group by name substring. Returns candidate groups (no roster). Use user_group_get for the member list.",
  "properties": {
    "name_contains": { "type": "string", "description": "Substring match against sys_user_group.name" },
    "active": { "type": "boolean", "default": true, "description": "Filter by sys_user_group.active. Omitted means true." },
    "limit": { "type": "integer", "minimum": 1, "maximum": 100, "default": 20 }
  }
}
```

**Output shape:** `{ "groups": [ { "sys_id", "name", "description", "active", "manager", "email" } ] }` — same envelope style as `user_search`'s `{ "users": [...] }` (see daemon dispatch `crates/snow_daemon/src/rpc.rs:702`).

### Tool 3 — `user_group_membership` (user-side: membership check + reverse lookup)

Answer membership from the **user** side, with the user selectable by **`sys_id` *or* name**. Accepts a **user** (exactly one of `user_sys_id` / `user_name` / `email` / `name`) and an **optional group** (`group_sys_id` *or* exact `group_name`).

- **User + group** → membership check: `{ user, group, is_member, membership: { sys_id, ... } | null }`.
- **User only** → reverse lookup: `{ user, groups: [ { sys_id, name, active, manager } ] }` — all `sys_user_grmember` rows for the user, joined to `sys_user_group`.

**Input schema:**

```json
{
  "type": "object",
  "additionalProperties": false,
  "description": "Resolve group membership from the user side. Provide a user (exactly one of user_sys_id / user_name / email / name) and OPTIONALLY a group (group_sys_id or exact group_name). With a group: membership check. Without: the user's groups. Read-only; direct sys_user_grmember rows only (no nesting).",
  "properties": {
    "user_sys_id": { "type": "string", "pattern": "^[0-9a-fA-F]{32}$" },
    "user_name": { "type": "string", "description": "sys_user.user_name, e.g. example.user" },
    "email": { "type": "string", "description": "exact sys_user.email" },
    "name": { "type": "string", "description": "exact sys_user.name" },
    "group_sys_id": { "type": "string", "pattern": "^[0-9a-fA-F]{32}$" },
    "group_name": { "type": "string", "description": "exact sys_user_group.name" },
    "active_only": { "type": "boolean", "default": true },
    "limit": { "type": "integer", "minimum": 1, "maximum": 500, "default": 200 }
  }
}
```

**Behavior:** resolve the user via the one-of selector (reuse `lookup_user`, `crates/snow_core/src/lib.rs:1808`); query `sys_user_grmember` by `user=<sys_id>` (and `group=<sys_id>` when a group is supplied — resolve `group_name`→sys_id first); join to `sys_user_group` projecting `USER_GROUP_FIELDS`. Wiring threads the **same chain** as Tools 1/2 (snow_core method → `rpc.rs` `RpcMethod` + supported-methods list → `records.rs` registration → `daemon_bridge.rs` map → `CAPABILITIES.md` + contract tests).

**Net effect:** the **group** is addressable by sys_id *or* name (`user_group_get` / `user_group_search`), and the **user** by sys_id / user_name / email / exact name (`user_group_membership`) — so any membership question is answerable from either side, by either identifier form.

### Pagination / limit, active-only default, edge cases

- **Pagination/counts:** `limit` applies to group search (`1`-`100`, default `20`), group roster (`1`-`500`, default `200`), and user reverse lookup (`1`-`500`, default `200`). `member_count` is the number of member rows returned after filtering and limit application (`members.length`). `truncated=true` means at least one additional matching row exists beyond the returned page. v1 has no `offset`; callers can raise `limit` up to the max, and full pagination can be added later if needed.
- **Active-only defaults:** `active_members_only` on `user_group_get` defaults to `true` and filters only returned `sys_user` member rows (`sys_user.active=true`). `user_group_search.active` defaults to `true` and filters `sys_user_group.active=true`. `user_group_membership.active_only` defaults to `true`; it resolves only active users and active groups, omits inactive groups in reverse lookup, and treats inactive selected groups as not eligible unless `active_only=false`.
- **Membership rows:** v1 treats `sys_user_grmember` as a direct relation table and does not define membership-row active filtering because there is no portable `active` field on that table. If an instance-specific active/status field is needed, add it in a later explicit requirement.
- **Group not found:** return the same `-32004` "not found" convention `user_lookup` uses (`crates/snow_daemon/src/rpc.rs:695`) — e.g. `-32004 "group not found"`.
- **Empty group:** return the resolved `group` with `members: []`, `member_count: 0`. Not an error.
- **Inactive members/groups:** inactive members are excluded from rosters by default and included when `active_members_only=false`; inactive groups are excluded from search and user-side membership by default and included when the relevant active flag is `false`. `active` is always present on returned member and group rows so callers can see status.
- **Ambiguous name:** `user_group_get` by exact `name` that matches >1 group → `invalid_params` / bail, telling the caller to use `sys_id` or `user_group_search` to disambiguate (mirror `crates/snow_core/src/lib.rs:1849-1855`).
- **Nested groups (OUT OF SCOPE for v1):** ServiceNow group nesting (`sys_user_group.parent` / contained-group membership) is **explicitly out of scope** for the first version. v1 returns only **direct** `sys_user_grmember` rows for the named group. State this in the tool description so callers don't assume transitive membership. A future `include_nested` flag can layer on later.

---

## Implementation guidance

The end-to-end wiring mirrors the existing `user_search` primitive and the most recent daemon extensions. The full chain is: **MCP tool schema → MCP server dispatch → daemon-bridge method map → daemon RpcMethod enum + dispatch + supported-methods list → `snow_core` live query → policy/capabilities/tests**. Touch these files:

### 1. `snow_core` — the query method (`crates/snow_core/src/lib.rs`)

- Add request structs `UserGroupGet`, `UserGroupSearch`, and `UserGroupMembership` (alongside `UserLookup`/`UserSearch` at `:237`-`:267`), with `#[serde(deny_unknown_fields)]` and a `validate_selector()` / `validate()` method as `UserLookup`/`UserSearch` have.
- Add `pub async fn get_user_group_members(&self, params: UserGroupGet) -> Result<UserGroupRoster>`, `pub async fn search_user_groups(&self, params: UserGroupSearch) -> Result<Vec<UserGroupRecord>>`, and `pub async fn user_group_membership(&self, params: UserGroupMembership) -> Result<UserGroupMembershipResult>`, modeled on `lookup_user` (`:1808`) and `search_users` (`:1871`).
- Use the **existing live query builder**: `self.client.table("sys_user_group")` / `self.client.table("sys_user_grmember")` with `.equals(...)`, `.contains(...)`, `.fields(...)`, `.display_value(DisplayValue::Both)`, `.limit(...)`, `.order_by(...)`, `.execute().await?` — exactly as `search_users` does at `:1878-1907`. The membership join is two live queries:
  1. `table("sys_user_grmember").equals("group", &group_sys_id).fields(&["user"]).limit(...)` → collect member `user` sys_ids,
  2. resolve those to `sys_user` rows projecting `USER_LOOKUP_FIELDS` (`:142`) — either a batched `user IN (...)` query via the query builder or per-user resolution through the existing reference resolver.
- Reuse `USER_LOOKUP_FIELDS` (`:142`) for member projection so the member shape equals `user_search`. Define a `USER_GROUP_FIELDS` const (`sys_id, name, description, active, manager, email`) for the group projection.
- Optional but consistent: cache-back the roster like `cached_users_for_query_key` / `persist_user_query_cache` (`:1815`, `:1908`) with the same TTL the user reads use. If caching is deferred, the live path is still correct.

### 2. MCP tool registration (`crates/snow_mcp/src/tools/records.rs`)

- In `register()` (`:15`), add `("user_group_get", "<desc>", user_group_get_arg_schema())`, `("user_group_search", "<desc>", user_group_search_arg_schema())`, and `("user_group_membership", "<desc>", user_group_membership_arg_schema())` to the read-tool loop (`:16`-`:112`) — that loop already sets `default_enabled: true`, `requires_confirmation: false`, `output_schema: object_schema()`, which is the correct read posture.
- Add the three `*_arg_schema()` builder fns next to `user_lookup_arg_schema` (`:204`) / `user_search_arg_schema` (`:240`): `type: "object"`, `additionalProperties: false`, **no** top-level `oneOf`/`anyOf`/`allOf`, no `required` block (the exactly-one selector is enforced at runtime), schema descriptions that note read-only behavior and the **nested-membership-out-of-scope** caveat.
- If parsing the exactly-one selector centrally, add a small parser like `parse_record_lookup` (`:536`) or rely on `snow_core`'s `validate_selector()`.

### 3. MCP server dispatch (`crates/snow_mcp/src/server.rs`)

- Add `"user_group_get" => self.call_user_group_get(...)`, `"user_group_search" => ...`, and `"user_group_membership" => ...` to the method match (next to `:157`-`:158`), and `call_*` handlers modeled on `call_user_search` (`:471`) / `call_user_lookup` (`:451`) that deserialize into the new `snow_core` structs and call `self.core.get_user_group_members(...)` / `self.core.search_user_groups(...)` / `self.core.user_group_membership(...)`.

### 4. Daemon bridge method map (`crates/snow_mcp/src/daemon_bridge.rs`)

- Add `("user_group_get", "user_group_get")`, `("user_group_search", "user_group_search")`, and `("user_group_membership", "user_group_membership")` to `BRIDGE_TOOL_METHODS` (`:33`, next to the `user_search` entry at `:44`). The bridge only advertises a tool when the daemon's `contract_info` reports the method supported — so the daemon side (next) must list them.

### 5. Daemon JSON-RPC (`crates/snow_daemon/src/rpc.rs`)

- Add `UserGroupGet`, `UserGroupSearch`, and `UserGroupMembership` variants to the `RpcMethod` enum (next to `UserLookup`/`UserSearch` at `:77`-`:78`) and their string mappings in `from_method` (`:185`-`:186`).
- Add dispatch arms modeled on `RpcMethod::UserSearch` (`:700`-`:706`): on success return `json!({ "groups": ... })` for search, the roster object for get, and the membership result object for user-side checks/reverse lookup; map "not found" to `-32004` like `UserLookup` (`:695`).
- Add `"user_group_get"`, `"user_group_search"`, and `"user_group_membership"` to the **supported-methods list** (`:1545`-`:1574`, the array that begins with `"contract_info"`). This is what `contract_info` advertises and what gates the MCP bridge.
- Add `extract_user_group_*_params` helpers like `extract_user_search_params` (`:2163`).
- The daemon's own MCP surface (`crates/snow_daemon/src/mcp.rs`, e.g. `:160`) should register the tools too if it advertises a tool list independently (it lists `user_lookup` at `:1242`).

### 6. Governance — `crates/snow_mcp/src/domain/policy.rs`

- These are **read** tools, so `is_write_tool` (`:560`) correctly returns `false` for all three (no `_apply_` / `_submit_` / explicit-list match) — **do not** add them to `is_write_tool`. The registry's `capability()` (`crates/snow_mcp/src/tools/registry.rs:27-39`) will then report `mode: "read"`, `read_only: true`, `requires_confirmation: false` automatically.
- **Role allow-lists:** add `"user_group_get"`, `"user_group_search"`, and `"user_group_membership"` to the relevant read roles in `default_roles()` — at minimum `developer` (`:673`) and `change_writer` (`:736`), alongside the existing `user_lookup` / `user_search` entries, so role-scoped deployments can call them.
- **No `default_tools()` entry, no `field_allowlist`** — those are write-tool concerns; read tools need neither.

### 7. Table allowlist note

- `RECORD_LOOKUP_ALLOWED_TABLES` (`crates/snow_core/src/lib.rs:1190`) is the allowlist for the **generic `get_record`** path. The new tools query `sys_user_group` / `sys_user_grmember` through their **own dedicated methods** (like `search_users` queries `sys_user` without `sys_user` being in that list), so **do not** widen `RECORD_LOOKUP_ALLOWED_TABLES` — that would also force a change to the asserted enum in `contract_tools_list.rs:177` and broaden the generic surface unnecessarily.

### 8. Contract tests

- **`crates/snow_mcp/tests/contract_tools_list.rs`:** add `"user_group_get"`, `"user_group_search"`, and `"user_group_membership"` to the expected-tools list in `tools_list_contains_daemon_read_parity_tools_with_schema_shape` (`:78`-`:138`), and add per-tool schema assertions modeled on `user_search_schema_advertises_multi_user_filters` (`:336`): `inputSchema.type == "object"`, `additionalProperties == false`, `assert_no_top_level_schema_composition`, `default_enabled == true`, `!requires_confirmation`, and the `limit` min/max/default and active-filter default checks.
- **`crates/snow_mcp/tests/contract_tools_call.rs`** and **`tests/daemon_bridge.rs`:** add forwarding tests modeled on `user_search_forwards_to_daemon_method` (`daemon_bridge.rs:1202`) proving the new tools forward to daemon methods `user_group_get` / `user_group_search` / `user_group_membership`.
- **`crates/snow_mcp/tests/capabilities_doc_sync.rs`:** `capabilities_doc_lists_every_registered_tool` (`:67`) asserts **every** registered tool name appears in `CAPABILITIES.md`. So `crates/snow_mcp/CAPABILITIES.md` **must** gain entries for all three tools (add a "User groups (read-only primitive)" section next to "Users (read-only primitive)" at `:103`, and list them in the read-tool inventory at `:82`). This test fails CI otherwise.
- Daemon-side: add `from_method` and dispatch tests like `direct_rpc_user_search_*` (`crates/snow_daemon/src/rpc.rs:4527`) and the `RpcMethod::from_method("user_search")` assertion (`:2874`).

### 9. Deploy step

- Build, install, restart the daemon, and run the schema smoke via the standard helper:
  `bash scripts/build_release.sh` — which runs `cargo build --release -p snow_cli -p snow_daemon -p snow_mcp`, installs the binaries, runs `snow daemon restart` + `snow daemon status`, and executes `scripts/mcp_schema_smoke.py ... --require-contract daemon-json-rpc-v1` (`scripts/build_release.sh:19-27`).
- In the consuming client (Claude Code), `/mcp reload` to pick up the new tools after the daemon restart.

---

## Governance & safety

- **Read-only, no confirmation.** All three tools are pure reads of `sys_user_group` / `sys_user_grmember` / `sys_user`. They must **not** match `is_write_tool` (`crates/snow_mcp/src/domain/policy.rs:560`) and therefore report `mode: read`, `read_only: true`, `requires_confirmation: false`.
- **Role and environment rollout.** Because group rosters expose workforce directory data, v1 should be enabled only for explicit read roles that need it (`developer`, `change_writer`, and any future group-read role), not blindly granted to every role-scoped deployment. `policy_describe` / `tool_capabilities` should make the enabled role posture visible before rollout.
- **Audit/logging.** Log tool name, caller identity, selector type, resolved group/user sys_id, result count, and `truncated` status for roster and reverse-lookup reads. Do not log full member names or email addresses beyond the normal redacted response path.
- **No mutation surface.** There is no create/update/delete of group membership in scope. This is roster *read* only.
- **PHI / redaction.** Member rows expose work identity (`name`, `user_name`, `email`, `title`) — the same fields `user_search` already returns. No patient/PHI data is touched (`sys_user` is workforce directory data, not clinical). Member `email` must flow through the **existing redaction pipeline** (the `PhiRedaction` / `DisplayValueMatch` gates in `PolicyGate`, `crates/snow_mcp/src/domain/policy.rs:460-519`); do not bypass redaction for these tools. Since `user_search` already returns `email` under current rules, no new redaction rule is required, but the new tools must respect whatever rules `redaction_rules_describe` reports.
- **Least surface.** Querying via dedicated `sys_user_group` / `sys_user_grmember` methods (not by widening the generic `get_record` table allowlist) keeps the readable-table surface tightly scoped.

---

## Acceptance criteria

1. Given a configured test fixture group **"Example Assignment Group"** with two active direct members, one inactive direct member, one inactive group variant, and no nested expansion, `user_group_get` with `sys_id = 0123456789abcdef0123456789abcdef` returns the group plus the active member roster, each member carrying `sys_id, user_name, name, email, title, active`.
2. `user_group_get` with `name = "Example Assignment Group"` resolves the same group and roster (exact-name resolution); an ambiguous name errors with guidance to use `sys_id` / `user_group_search`.
3. `user_group_search` with `name_contains = "Example Assignment"` returns candidate groups (sys_id + name), enabling name → sys_id disambiguation.
4. `active_members_only` defaults to `true` for `user_group_get` (inactive users excluded); passing `false` includes inactive users, and every member row carries `active`. `user_group_search.active` defaults to `true` for groups. `user_group_membership.active_only` defaults to `true` for active user and group results.
5. A group with no members returns `{ group, members: [], member_count: 0, truncated: false }` (not an error); an unknown group returns `-32004 group not found`.
6. **Nested membership is explicitly excluded** in v1 — only direct `sys_user_grmember` rows are returned, and the tool description says so.
7. All three tools appear in `tool_capabilities` output with `mode: read`, `read_only: true`, `requires_confirmation: false`, and role/environment enablement visible.
8. Contract tests pass: all three tools appear in `tools/list` with `schema_version "1.0"`, `inputSchema.type "object"`, `additionalProperties: false`, and no top-level `oneOf`/`anyOf`/`allOf`; the daemon-bridge forwarding tests pass; `capabilities_doc_sync` passes (all three tools documented in `CAPABILITIES.md`).
9. `bash scripts/build_release.sh` completes, including the `mcp_schema_smoke.py --require-contract daemon-json-rpc-v1` step, and `contract_info` advertises `user_group_get` / `user_group_search` / `user_group_membership` as supported daemon methods.
10. `user_group_membership` resolves a user by `sys_id` **or** `user_name` **or** `email` **or** exact `name`; with a `group` selector (sys_id or exact name) it returns a boolean membership check, and without a group it returns the user's groups. Direct membership only (no nesting). Together with criteria 1–3 this proves **both** the group and the user are addressable by sys_id or name.

---

## References — precedent (template for extending the daemon)

The **2026-06-03 `requested_by_date` schema extension** and the most recent **`Add MCP approval action tools`** commit (`bcd22c4`) are the working templates for how the daemon gets extended end to end. The `requested_by_date` precedent threaded a single new field through exactly the layers this request needs:

- **Tool schema:** added to the change input schema in `crates/snow_mcp/src/tools/change.rs` (`requested_by_date` in `change_request_properties`, `:171`).
- **Allowlists:** added to both `change_request_apply_create` and `change_request_apply_update` field allowlists in `crates/snow_mcp/src/domain/policy.rs` (`:900`, `:927`) **and** to the shipped `crates/snow_mcp/policy.example.toml` / deployed policy template.
- **Contract test:** `crates/snow_mcp/tests/capabilities_doc_sync.rs::example_policy_parses` (`:112`-`:121`) asserts the `requested_by_date` allowlist entry is present — i.e. the docs/policy/test drift guards were updated in lockstep.
- **Deploy:** shipped via `scripts/build_release.sh` (build → install → `snow daemon restart` → schema smoke).

The `bcd22c4` commit ("Add MCP approval action tools") is the cleaner shape match for *adding a tool* (vs. a field) and shows the exact file set a new tool touches: `crates/snow_daemon/src/rpc.rs`, `crates/snow_mcp/CAPABILITIES.md`, `crates/snow_mcp/policy.example.toml`, `crates/snow_mcp/src/daemon_bridge.rs`, `crates/snow_mcp/src/domain/policy.rs`, `crates/snow_mcp/src/tools/records.rs`, `crates/snow_mcp/tests/capabilities_doc_sync.rs`, `crates/snow_mcp/tests/contract_tools_call.rs`, `crates/snow_mcp/tests/contract_tools_list.rs`, `crates/snow_mcp/tests/daemon_bridge.rs`. Use that file set as the checklist for this request (minus the write-only governance pieces, since these are read tools).
