# Snow Daemon JSON-RPC Consumer Handoff

## Purpose

Use this document to integrate an AI agent directly with Snow's local daemon
JSON-RPC API. The daemon is the consumer boundary. Mullet and MCP are not
dependencies for this integration.

This document describes the current source contract. An agent must still call
`contract_info` and verify the installed daemon before treating any operation
as available.

## Non-negotiable consumer rules

1. Call `contract_info` when establishing a daemon session.
2. Require `contract_version` to equal `daemon-json-rpc-v1`.
3. Require every operation the agent intends to call to appear in
   `supported_methods`.
4. Treat method advertisement, operator policy, ServiceNow ACL authorization,
   upstream availability, and successful live execution as separate facts.
5. Preserve Snow's response envelopes and native ServiceNow field values. Do
   not flatten, rename, default, or synthesize fields.
6. Branch on `error.data.code` when present, while retaining the numeric RPC
   error code and message for diagnostics.
7. Never bypass a governed plan/apply workflow or reconstruct its returned
   tokens.
8. If a required method is absent, report an incompatible or stale installed
   daemon. Do not silently fall back to a generic record operation or another
   transport.

## Connection and framing

Resolve the local daemon endpoint in this order:

1. `SNOW_DAEMON_ENDPOINT`
2. `SNOW_DAEMON_SOCKET`
3. Snow's platform default endpoint

On Unix, the default endpoint is normally:

```text
~/.config/snow/daemon.sock
```

Windows uses a config-directory-scoped local named pipe. Use a local-socket
library that supports the platform instead of assuming a TCP endpoint.

The protocol is newline-delimited JSON. Send one compact JSON-RPC request per
line and read one newline-terminated response. A connection may remain open
for sequential requests.

Request:

```json
{"jsonrpc":"2.0","method":"contract_info","params":{},"id":1}
```

Success response:

```json
{"jsonrpc":"2.0","result":{},"id":1}
```

Error response:

```json
{
  "jsonrpc": "2.0",
  "error": {
    "code": -32004,
    "message": "incident not found",
    "data": {
      "code": "INCIDENT_NOT_FOUND"
    }
  },
  "id": 1
}
```

Correlate every response with the request `id`. Treat a mismatched or missing
response ID as a protocol failure.

## Session discovery

Call:

```json
{"jsonrpc":"2.0","method":"contract_info","params":{},"id":1}
```

The result includes:

- `contract_version`
- `daemon_version`
- `instance_host`
- `supported_methods`
- `deprecated_aliases`
- `environment`
- `warming_model`
- `mcp_availability`

Minimum startup check:

```python
contract = rpc.call("contract_info", {})

if contract["contract_version"] != "daemon-json-rpc-v1":
    raise RuntimeError("unsupported Snow daemon contract")

required = {"incident_get", "incident_query", "incident_fields"}
missing = required - set(contract["supported_methods"])

if missing:
    raise RuntimeError(f"installed Snow daemon lacks: {sorted(missing)}")
```

Use canonical method names. `deprecated_aliases` is migration information, not
permission to build a new consumer on an obsolete name.

`contract_info` proves daemon exposure only. It does not prove that an
operation is enabled by policy, authorized by ServiceNow ACLs, or able to
reach the configured instance.

## Shared operation envelope

The new resource operations return a common envelope:

```json
{
  "operation": "incident_query",
  "source": {
    "kind": "live"
  },
  "completeness": {
    "kind": "complete"
  },
  "data": {}
}
```

A cached result must state its age:

```json
{
  "source": {
    "kind": "cache",
    "last_refreshed_at": "2026-01-01T12:00:00Z"
  }
}
```

A partial result must state why it is partial:

```json
{
  "completeness": {
    "kind": "partial",
    "reason": "page_limit_reached"
  }
}
```

Defined partial reasons are:

- `narrowed_projection`
- `page_limit_reached`
- `upstream_truncated`

Native ServiceNow fields normally retain both raw and display values:

```json
{
  "state": {
    "value": "2",
    "display_value": "In Progress"
  }
}
```

An absent field is unknown or not returned. It is not equivalent to an empty
string or `null`, and the consumer must not invent a value for it.

## Incident capability discovery

### `incident_fields`

`incident_fields` accepts exactly an empty object:

```json
{"jsonrpc":"2.0","method":"incident_fields","params":{},"id":2}
```

Metadata categories distinguish available data from unavailable discovery:

```json
{"status":"available","value":[]}
```

```json
{"status":"unavailable","reason":"acl_denied"}
```

Unavailable reasons are:

- `not_returned_by_instance`
- `acl_denied`
- `not_supported_by_operation`

An available empty list means the instance successfully reported an empty
category. An unavailable category means Snow could not establish its value.
Do not collapse those states.

The returned readable and writable fields are structural candidates derived
from live dictionary metadata. They do not override operation policy or
record-level ServiceNow ACLs.

## Incident reads

`incident_get` and `incident_query` are always live-only. They do not read or
populate Snow's cache, vault, or derived indexes.

### `incident_get`

Supply exactly one selector.

By Incident number:

```json
{
  "jsonrpc": "2.0",
  "method": "incident_get",
  "params": {
    "number": "INC0000001"
  },
  "id": 3
}
```

By `sys_id`:

```json
{
  "jsonrpc": "2.0",
  "method": "incident_get",
  "params": {
    "sys_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  },
  "id": 3
}
```

The result is a complete live operation envelope whose `data.record` contains
every native field returned by the instance. Fields the instance omits remain
absent.

Important errors:

| Numeric code | `error.data.code` | Meaning |
| --- | --- | --- |
| `-32602` | `INVALID_PARAMS` | Invalid selector or request shape |
| `-32003` | `ACL_DENIED` | ServiceNow explicitly denied access |
| `-32004` | `INCIDENT_NOT_FOUND` | A direct `sys_id` was explicitly absent |
| `-32005` | `INCIDENT_NUMBER_AMBIGUOUS` | More than one row matched the number |
| `-32007` | `INCIDENT_LOOKUP_UNAVAILABLE` | An empty number lookup could not distinguish absence from row ACL filtering |
| `-32001` | `SERVICENOW_UNAVAILABLE` | The configured instance was unavailable |
| `-32000` | `SERVICENOW_ERROR` | Another ServiceNow request failure occurred |

### `incident_query`

Example:

```json
{
  "jsonrpc": "2.0",
  "method": "incident_query",
  "params": {
    "filters": {
      "numbers": ["INC0000001", "INC0000002"],
      "states": ["In Progress"],
      "priorities": [1, 2],
      "active": true
    },
    "limit": 50
  },
  "id": 4
}
```

Supported filters are:

- `numbers`
- `assignment_group`
- `assigned_to`
- `caller_id`
- `cmdb_ci`
- `states`
- `priorities`
- `active`
- `opened_after`
- `opened_before`
- `updated_after`
- `updated_before`

All filters are optional. `{}` is a valid bounded query over all incidents
visible to the configured identity.

`limit` defaults to 50, must be between 1 and 200, and is rejected rather than
clamped when invalid. Results are ordered by ascending `sys_id`.

The response data has this shape:

```json
{
  "records": [],
  "next_cursor": null,
  "limit": 50,
  "rows_inspected": 0
}
```

When a page contains exactly `limit` records, the envelope is partial with
reason `page_limit_reached`. Continue with the returned `next_cursor`:

```json
{
  "filters": {
    "active": true
  },
  "limit": 50,
  "cursor": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
}
```

Continue until Snow returns a shorter or empty complete page. An exact
multiple of the page size requires the final empty request to establish
completion. An empty query page is success, not `INCIDENT_NOT_FOUND`.

Query returns a fixed bounded projection and excludes descriptions and journal
content. Call `incident_get` for the complete visible record.

If a requested state cannot be resolved against the instance's native choices,
Snow returns `INCIDENT_STATE_UNRESOLVED` with the requested value and available
choices in `error.data`.

## Cache-governed resource reads

The operator's fixed daemon policy chooses `live`, `read_through`, or
`cache_only`. A consumer does not send a cache mode with individual requests.

Policy-aware operations include:

- `business_application_get`
- `business_application_search`
- `business_application_query`
- `get_article`
- `search_knowledge`
- `list_knowledge_articles`
- `server_get`
- `server_search`
- `server_query`
- `catalog_items_search`
- `catalog_item_get`

Mode semantics:

| Mode | Behavior |
| --- | --- |
| `live` | Read ServiceNow directly with no local cache read or persistence |
| `read_through` | Return a fresh cached result, or refresh and persist a miss/stale result from ServiceNow |
| `cache_only` | Never contact ServiceNow; stale cached data is allowed and carries `last_refreshed_at` |

A read-through refresh failure never silently falls back to stale data.

A missing cache-only result returns:

```json
{
  "code": -32072,
  "message": "cache miss",
  "data": {
    "code": "CACHE_MISS",
    "operation": "server_get",
    "object": "server"
  }
}
```

Existing operation request shapes remain in force. Do not add `mode` or a
caller-selected persistence flag.

## Cache policy lifecycle

Validate the configured policy:

```json
{"jsonrpc":"2.0","method":"cache_policy_validate","params":{},"id":10}
```

Reload it atomically:

```json
{"jsonrpc":"2.0","method":"cache_policy_reload","params":{},"id":11}
```

Both methods accept exactly `{}`. The policy path is daemon-owned; a caller
cannot supply another path.

Validation returns:

- `version`
- `source`
- `rule_count`
- `fingerprint`

Reload returns those fields plus:

- `previous_fingerprint`
- `changed`

An invalid reload leaves the previously active policy snapshot in place.
Policy failures use:

- `-32070` / `CACHE_POLICY_INVALID`
- `-32071` / `CACHE_POLICY_IO`

## Governed Incident writes

Write operations are disabled by default and constrained by the daemon's
environment and operation policy. Advertising a write method does not mean
that the current environment permits it. Treat `POLICY_DENIED` as a normal,
typed outcome.

Never issue an ungoverned PATCH. Plan first, present or evaluate the preview,
and apply only by returning the exact plan artifacts.

### Single-record update

Plan:

```json
{
  "jsonrpc": "2.0",
  "method": "incident_plan_update",
  "params": {
    "number": "INC0000001",
    "state": "In Progress",
    "work_notes": "Generic operator note"
  },
  "id": 20
}
```

The plan result includes:

- `plan_id`
- `op_hash`
- `preview`
- `expires_at`
- `confirmation_token`
- `idempotency_key`
- `concurrency_token`

The concurrency token contains `sys_updated_on` and may contain
`sys_mod_count`. Copy the entire object unchanged.

Apply:

```json
{
  "jsonrpc": "2.0",
  "method": "incident_apply_update",
  "params": {
    "plan_id": "returned-plan-id",
    "confirmation_token": "returned-confirmation-token",
    "idempotency_key": "returned-idempotency-key",
    "concurrency_token": {
      "sys_updated_on": "returned-sys-updated-on",
      "sys_mod_count": 1
    }
  },
  "id": 21
}
```

If `sys_mod_count` was absent in the plan, keep it absent in the apply request.
Do not modify or normalize any token.

### Bulk update

A bulk plan requires at least three targets and no more than the configured
limit, which cannot exceed 25.

```json
{
  "jsonrpc": "2.0",
  "method": "incident_bulk_plan_update",
  "params": {
    "shared_patch": {
      "assignment_group": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    },
    "targets": [
      {
        "number": "INC0000001",
        "patch": {
          "state": "In Progress"
        }
      },
      {
        "sys_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "patch": {
          "work_notes": "Generic operator note"
        }
      },
      {
        "number": "INC0000003"
      }
    ]
  },
  "id": 30
}
```

Allowed bulk patch fields are:

- `assigned_to`
- `assignment_group`
- `state`
- `work_notes`
- `comments`

Every target must specify exactly one of `number` or `sys_id`. A target patch
may be omitted when `shared_patch` provides its effective change. The same
field cannot appear in both the shared and per-target patch. Journal values
must be non-empty and no longer than 16,000 characters.

The bulk plan returns:

- `plan_id`
- `op_hash`
- `apply_tool`
- `preview.targets`
- `expires_at`
- `confirmation_token`
- `idempotency_key`

Each preview target contains its canonical `number`, `sys_id`, effective
patch, and concurrency token. Preserve the returned target order and copy all
tokens exactly into the apply request:

```json
{
  "jsonrpc": "2.0",
  "method": "incident_bulk_apply_update",
  "params": {
    "plan_id": "returned-plan-id",
    "confirmation_token": "returned-confirmation-token",
    "idempotency_key": "returned-idempotency-key",
    "concurrency_tokens": [
      {
        "sys_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "sys_updated_on": "returned-sys-updated-on"
      }
    ]
  },
  "id": 31
}
```

Snow preflights every target before the first PATCH. It then applies targets in
ascending `sys_id` order, without automatic PATCH retries or rollback, and
stops at the first execution failure.

If a later target fails after at least one earlier target was applied, Snow
returns `-32046` / `PARTIAL_FAILURE`. Its `error.data` includes the typed
failure, whether ServiceNow applied a write, a public-safe diagnostic when
available, and the durable partial receipt. Preserve and surface that receipt.

Important governed-write outcomes include:

| `error.data.code` | Consumer action |
| --- | --- |
| `POLICY_DENIED` | Report that daemon policy or environment denied the operation |
| `CONFIRMATION_INVALID` | Do not apply; acquire a new plan/confirmation if the operation remains desired |
| `CONCURRENCY_CONFLICT` | Re-read the target and create a new plan |
| `PENDING_RESOLUTION_REQUIRED` | Do not blindly retry; inspect the target and reconcile before creating a new plan |
| `PARTIAL_FAILURE` | Preserve the receipt and report applied, failed, and unattempted targets separately |

An exact replay with the same completed idempotency material returns the
durable receipt without another PATCH. A different operation under the same
idempotency key is a conflict.

Receipts and audits are intentionally redacted. Consumers must not expect
journal bodies, full record snapshots, or instance URLs in them.

## Minimal Unix client

```python
import json
import socket


class SnowRpcError(RuntimeError):
    def __init__(self, error):
        self.rpc_code = error["code"]
        self.rpc_message = error["message"]
        self.data = error.get("data") or {}
        self.application_code = self.data.get("code")
        super().__init__(
            f"{self.application_code or self.rpc_code}: {self.rpc_message}"
        )


class SnowRpc:
    def __init__(self, socket_path):
        self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.socket.connect(socket_path)
        self.reader = self.socket.makefile("rb")
        self.next_id = 1

    def call(self, method, params=None):
        request_id = self.next_id
        self.next_id += 1

        request = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params if params is not None else {},
            "id": request_id,
        }
        payload = json.dumps(request, separators=(",", ":")).encode() + b"\n"
        self.socket.sendall(payload)

        line = self.reader.readline()
        if not line:
            raise RuntimeError("Snow daemon closed the RPC connection")

        response = json.loads(line)
        if response.get("id") != request_id:
            raise RuntimeError("JSON-RPC response ID mismatch")
        if "error" in response:
            raise SnowRpcError(response["error"])
        if "result" not in response:
            raise RuntimeError("JSON-RPC response has neither result nor error")

        return response["result"]
```

Use the platform's local-socket equivalent on Windows.

## Readiness boundary

This source-level handoff is not an installed-runtime attestation. Before an
agent declares itself ready, it must establish all of the following:

1. The daemon endpoint is reachable.
2. `contract_info.contract_version` is supported.
3. Every required method is advertised.
4. The daemon reports the intended environment.
5. The specific operation is permitted by current policy.
6. ServiceNow authorizes and successfully executes the requested live action.

Do not combine these into one generic "Snow is available" claim.

## Canonical source anchors

- Endpoint resolution: `crates/snow_core/src/ipc.rs`
- JSON-RPC wire structs: `crates/snow_daemon/src/rpc/wire.rs`
- Newline framing: `crates/snow_daemon/src/rpc/server.rs`
- Contract inventory: `crates/snow_daemon/src/rpc/handlers/system.rs`
- Shared envelopes: `crates/snow_core/src/resource/descriptor.rs`
- Incident read types: `crates/snow_core/src/resource/incident.rs`
- Incident RPC errors/routes: `crates/snow_daemon/src/rpc/handlers/incidents.rs`
- Single governed writes: `crates/snow_daemon/src/change_write.rs`
- Bulk governed writes: `crates/snow_daemon/src/incident_bulk_write.rs`
