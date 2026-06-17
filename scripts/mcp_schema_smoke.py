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
import subprocess
import sys
from typing import Any


COMPOSITION_KEYWORDS = ("oneOf", "anyOf", "allOf")
TOOLS_LIST_ID = 2
RESOURCES_LIST_ID = 3


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


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="Seconds to wait for the MCP command to return tools/list.",
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
        tool_count, resource_count = run_smoke(args.command, args.timeout)
    except SmokeError as exc:
        print(f"mcp schema smoke failed: {exc}", file=sys.stderr)
        return exc.code
    print(
        "mcp schema smoke passed: "
        f"{tool_count} tools, {resource_count} resources, no top-level schema composition"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
