#!/usr/bin/env python3
"""Protocol-level MCP tools/list and resources/list schema smoke.

This script launches an MCP server command, requests tools/list, and fails if
any advertised tool uses top-level JSON Schema composition keywords that model
clients reject during tool registration. It also requests resources/list and
fails if advertised resources omit client-required identity fields.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from typing import Any


COMPOSITION_KEYWORDS = ("oneOf", "anyOf", "allOf")
TOOLS_LIST_ID = 2
RESOURCES_LIST_ID = 3
CAPABILITIES_ID = 3
POLICY_DESCRIBE_ID = 4
MATRIX_COLUMNS = (
    "Decision ID",
    "Operational intent",
    "Logical/MCP tool",
    "Canonical daemon method",
    "Request/result contract",
    "Decision",
    "Classification",
    "Data source",
    "Direct transport",
    "Bridge transport",
    "Daemon policy evidence",
    "Bridge policy evidence",
    "Confirmation",
    "Idempotency",
    "Concurrency",
    "Audit/receipt",
    "Direct L0 evidence",
    "Bridge L0 evidence",
    "Installed evidence",
    "Owner",
)
PUBLIC_TOOL_NAME = re.compile(r"^[a-z][a-z0-9_]*$")


class SmokeError(Exception):
    def __init__(self, message: str, code: int = 1) -> None:
        super().__init__(message)
        self.code = code


def request_payload() -> str:
    messages = [
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "snow-mcp-schema-smoke", "version": "0"},
            },
        },
        {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
        {"jsonrpc": "2.0", "id": TOOLS_LIST_ID, "method": "tools/list", "params": {}},
        {"jsonrpc": "2.0", "id": RESOURCES_LIST_ID, "method": "resources/list", "params": {}},
    ]
    return "".join(f"{json.dumps(message, separators=(',', ':'))}\n" for message in messages)


def attestation_request_payload() -> str:
    messages = [
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "snow-mcp-attestation", "version": "0"},
            },
        },
        {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
        {"jsonrpc": "2.0", "id": TOOLS_LIST_ID, "method": "tools/list", "params": {}},
        {"jsonrpc": "2.0", "id": CAPABILITIES_ID, "method": "tool_capabilities", "params": {}},
        {"jsonrpc": "2.0", "id": POLICY_DESCRIBE_ID, "method": "policy_describe", "params": {}},
    ]
    return "".join(f"{json.dumps(message, separators=(',', ':'))}\n" for message in messages)


def parse_json_lines(output: str) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    for line in output.splitlines():
        stripped = line.strip()
        if not stripped.startswith("{"):
            continue
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            messages.append(parsed)
    return messages


def response_message(messages: list[dict[str, Any]], response_id: int, label: str) -> dict[str, Any]:
    for message in messages:
        if message.get("id") == response_id:
            return message
    raise SmokeError(f"MCP {label} response was not returned")


def validate_tools_list(message: dict[str, Any]) -> int:
    if "error" in message:
        raise SmokeError(f"MCP tools/list returned error: {json.dumps(message['error'])}")

    result = message.get("result")
    if not isinstance(result, dict):
        raise SmokeError("MCP tools/list result is not an object")

    tools = result.get("tools")
    if not isinstance(tools, list):
        raise SmokeError("MCP tools/list result.tools is not an array")

    offenders: list[str] = []
    missing_object_schema: list[str] = []
    for index, tool in enumerate(tools):
        if not isinstance(tool, dict):
            raise SmokeError(f"MCP tools[{index}] is not an object")
        name = tool.get("name")
        tool_name = name if isinstance(name, str) and name else f"<tools[{index}]>"
        input_schema = tool.get("inputSchema")
        if not isinstance(input_schema, dict) or input_schema.get("type") != "object":
            missing_object_schema.append(tool_name)
            continue
        for keyword in COMPOSITION_KEYWORDS:
            if keyword in input_schema:
                offenders.append(f"{tool_name}:{keyword}")

    if missing_object_schema:
        raise SmokeError(
            "MCP tools with missing/non-object inputSchema: " + ", ".join(missing_object_schema)
        )
    if offenders:
        raise SmokeError(
            "MCP tools with top-level schema composition: " + ", ".join(offenders)
        )
    return len(tools)


def validate_resources_list(message: dict[str, Any]) -> int:
    if "error" in message:
        raise SmokeError(f"MCP resources/list returned error: {json.dumps(message['error'])}")

    result = message.get("result")
    if not isinstance(result, dict):
        raise SmokeError("MCP resources/list result is not an object")

    resources = result.get("resources")
    if not isinstance(resources, list):
        raise SmokeError("MCP resources/list result.resources is not an array")

    offenders: list[str] = []
    for index, resource in enumerate(resources):
        if not isinstance(resource, dict):
            raise SmokeError(f"MCP resources[{index}] is not an object")
        for field in ("name", "uri"):
            value = resource.get(field)
            if not isinstance(value, str) or not value:
                offenders.append(f"resources[{index}].{field}")

    if offenders:
        raise SmokeError("MCP resources with missing/non-string fields: " + ", ".join(offenders))
    return len(resources)


def run_smoke(command: list[str], timeout: float) -> int:
    if not command:
        raise SmokeError("missing MCP command after --", code=2)

    try:
        completed = subprocess.run(
            command,
            input=request_payload(),
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except FileNotFoundError as exc:
        raise SmokeError(f"MCP command not found: {command[0]}", code=2) from exc
    except subprocess.TimeoutExpired as exc:
        raise SmokeError(f"MCP schema smoke timed out after {timeout:g}s", code=2) from exc

    messages = parse_json_lines(completed.stdout)
    if not messages and completed.stderr:
        raise SmokeError(f"MCP command produced no JSON responses; stderr: {completed.stderr.strip()}")

    tool_count = validate_tools_list(response_message(messages, TOOLS_LIST_ID, "tools/list"))
    resource_count = validate_resources_list(
        response_message(messages, RESOURCES_LIST_ID, "resources/list")
    )
    if completed.returncode not in (0, -15):
        raise SmokeError(
            "MCP command exited after protocol smoke with nonzero status "
            f"{completed.returncode}; stderr: {completed.stderr.strip()}"
        )
    return tool_count, resource_count


def matrix_rows(matrix_path: str) -> list[dict[str, str]]:
    try:
        with open(matrix_path, encoding="utf-8") as matrix_file:
            lines = matrix_file.read().splitlines()
    except OSError as exc:
        raise SmokeError("MCP attestation matrix is unavailable", code=2) from exc

    header_index = next(
        (index for index, line in enumerate(lines) if split_markdown_row(line) == list(MATRIX_COLUMNS)),
        None,
    )
    if header_index is None or header_index + 1 >= len(lines):
        raise SmokeError("MCP attestation matrix is missing its required schema")
    if not is_separator_row(lines[header_index + 1]):
        raise SmokeError("MCP attestation matrix has an invalid header separator")

    rows: list[dict[str, str]] = []
    decision_ids: set[str] = set()
    for line in lines[header_index + 2 :]:
        if not line.lstrip().startswith("|"):
            break
        values = split_markdown_row(line)
        if len(values) != len(MATRIX_COLUMNS):
            raise SmokeError("MCP attestation matrix has an ambiguous row")
        row = dict(zip(MATRIX_COLUMNS, values, strict=True))
        if any(not value for value in row.values()):
            raise SmokeError("MCP attestation matrix has an incomplete row")
        if not re.fullmatch(r"DOPS-OP-[A-Z0-9-]+", row["Decision ID"]):
            raise SmokeError("MCP attestation matrix has an invalid decision identifier")
        if row["Decision ID"] in decision_ids:
            raise SmokeError("MCP attestation matrix has a duplicate decision identifier")
        decision_ids.add(row["Decision ID"])
        rows.append(row)
    if not rows:
        raise SmokeError("MCP attestation matrix has no operation rows")
    return rows


def split_markdown_row(line: str) -> list[str]:
    stripped = line.strip()
    if not stripped.startswith("|") or not stripped.endswith("|"):
        return []
    return [value.strip() for value in stripped[1:-1].split("|")]


def is_separator_row(line: str) -> bool:
    values = split_markdown_row(line)
    return len(values) == len(MATRIX_COLUMNS) and all(re.fullmatch(r":?-{3,}:?", value) for value in values)


def selected_bridge_tools(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    selected: list[dict[str, str]] = []
    for row in rows:
        tool = row["Logical/MCP tool"]
        if tool == "N/A":
            continue
        if not PUBLIC_TOOL_NAME.fullmatch(tool):
            raise SmokeError("MCP attestation matrix names a non-public tool")
        if row["Decision"] != "approved" or not row["Bridge transport"].startswith("approved"):
            selected.append({"Logical/MCP tool": tool, "blocked": "true"})
            continue
        if row["Classification"] not in {"read", "plan", "write"}:
            selected.append({"Logical/MCP tool": tool, "blocked": "true"})
            continue
        if "deprecated" in row["Canonical daemon method"].lower():
            selected.append({"Logical/MCP tool": tool, "blocked": "true"})
            continue
        bridge_evidence = row["Bridge policy evidence"].lower()
        daemon_evidence = row["Daemon policy evidence"].lower()
        is_write = row["Classification"] == "write"
        valid_evidence = (
            "bridge" in bridge_evidence
            and "daemon" in bridge_evidence
            and ((not is_write and daemon_evidence == "n/a") or (is_write and daemon_evidence != "n/a"))
        )
        selected.append({"Logical/MCP tool": tool, "blocked": "true" if not valid_evidence else "false", "write": str(is_write).lower()})
    if not selected:
        raise SmokeError("MCP attestation matrix selects no MCP tools")
    return selected


def result_object(message: dict[str, Any], label: str) -> dict[str, Any]:
    if "error" in message or not isinstance(message.get("result"), dict):
        raise SmokeError(f"MCP attestation {label} response is invalid")
    return message["result"]


def run_attestation(matrix_path: str, command: list[str], timeout: float) -> list[tuple[str, str]]:
    if not command:
        raise SmokeError("missing MCP command after --", code=2)
    selected = selected_bridge_tools(matrix_rows(matrix_path))
    try:
        completed = subprocess.run(
            command, input=attestation_request_payload(), text=True, capture_output=True,
            timeout=timeout, check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return [(row["Logical/MCP tool"], "blocked") for row in selected]
    messages = parse_json_lines(completed.stdout)
    try:
        initialize = result_object(response_message(messages, 1, "initialize"), "initialize")
        if not isinstance(initialize.get("protocolVersion"), str):
            raise SmokeError("MCP attestation initialize response is malformed")
        tools = validate_tools_list(response_message(messages, TOOLS_LIST_ID, "tools/list"))
        del tools
        listed_tools = {
            tool["name"] for tool in result_object(response_message(messages, TOOLS_LIST_ID, "tools/list"), "tools/list")["tools"]
            if isinstance(tool, dict) and isinstance(tool.get("name"), str)
        }
        capabilities = result_object(response_message(messages, CAPABILITIES_ID, "tool_capabilities"), "tool_capabilities").get("tools")
        policy_writes = result_object(response_message(messages, POLICY_DESCRIBE_ID, "policy_describe"), "policy_describe").get("write_tools_enabled")
        if not isinstance(capabilities, list) or not isinstance(policy_writes, list) or not all(isinstance(name, str) for name in policy_writes):
            raise SmokeError("MCP attestation introspection response is malformed")
        enabled = {
            item.get("name") for item in capabilities
            if isinstance(item, dict) and item.get("enabled") is True and isinstance(item.get("name"), str)
        }
    except SmokeError:
        return [(row["Logical/MCP tool"], "blocked") for row in selected]
    if completed.returncode not in (0, -15):
        return [(row["Logical/MCP tool"], "blocked") for row in selected]
    dispositions: list[tuple[str, str]] = []
    for row in selected:
        tool = row["Logical/MCP tool"]
        passed = row.get("blocked") != "true" and tool in listed_tools and tool in enabled
        if row.get("write") == "true":
            passed = passed and tool in policy_writes
        dispositions.append((tool, "pass" if passed else "blocked"))
    return dispositions


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="Seconds to wait for the MCP command to return tools/list.",
    )
    parser.add_argument(
        "--attest-matrix",
        metavar="PATH",
        help="Fail-closed operation matrix to compare against MCP bridge introspection.",
    )
    parser.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        help="MCP command to launch; prefix with -- before command arguments.",
    )
    args = parser.parse_args(argv)
    if args.command and args.command[0] == "--":
        args.command = args.command[1:]
    return args


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        if args.attest_matrix:
            dispositions = run_attestation(args.attest_matrix, args.command, args.timeout)
            for tool, disposition in dispositions:
                print(f"{tool}: {disposition}")
            if any(disposition != "pass" for _, disposition in dispositions):
                return 1
            return 0
        tool_count, resource_count = run_smoke(args.command, args.timeout)
    except SmokeError as exc:
        label = "mcp attestation blocked" if args.attest_matrix else "mcp schema smoke failed"
        print(f"{label}: {exc}", file=sys.stderr)
        return exc.code
    print(
        "mcp schema smoke passed: "
        f"{tool_count} tools, {resource_count} resources, no top-level schema composition"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
