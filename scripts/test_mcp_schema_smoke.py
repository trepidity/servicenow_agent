#!/usr/bin/env python3
"""Process-level evidence for MCP attestation mode.

The mutation caught here is an attester that reports an advertised bridge tool
as ready without requiring its capability and bridge-policy reports.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path


REPO = Path(__file__).resolve().parents[1]
SMOKE = REPO / "scripts" / "mcp_schema_smoke.py"
HEADERS = [
    "Decision ID", "Operational intent", "Logical/MCP tool", "Canonical daemon method",
    "Request/result contract", "Decision", "Classification", "Data source",
    "Direct transport", "Bridge transport", "Daemon policy evidence", "Bridge policy evidence",
    "Confirmation", "Idempotency", "Concurrency", "Audit/receipt", "Direct L0 evidence",
    "Bridge L0 evidence", "Installed evidence", "Owner",
]


def matrix_row(tool: str, bridge_transport: str = "approved") -> str:
    cells = [
        "DOPS-OP-EXAMPLE", "Example read", tool, "get_record", "source", "approved", "read",
        "live", "approved: cli", bridge_transport + ": MCP", "N/A", "bridge report + daemon evidence",
        "not applicable", "not applicable", "not applicable", "receipt", "direct-test", "bridge-test",
        "local-artifact", "owner",
    ]
    return "| " + " | ".join(cells) + " |\n"


def matrix(tool: str, bridge_transport: str = "approved") -> str:
    return "| " + " | ".join(HEADERS) + " |\n|" + "|".join(["---"] * len(HEADERS)) + "|\n" + matrix_row(tool, bridge_transport)


def bridge_program(
    capability_enabled: bool = True,
    policy_writes: list[str] | None = None,
    listed_tool: str = "get_record",
    malformed_capabilities: bool = False,
) -> str:
    policy_writes = policy_writes or []
    capability_tools: dict[str, object] | list[dict[str, object]] = (
        {} if malformed_capabilities else [{"name": "get_record", "enabled": capability_enabled, "mode": "read", "read_only": True, "requires_confirmation": False}]
    )
    return textwrap.dedent(
        f'''\
        #!/usr/bin/env python3
        import json, sys
        for line in sys.stdin:
            request = json.loads(line)
            method = request.get("method")
            response = {{"jsonrpc": "2.0", "id": request.get("id")}}
            if method == "initialize":
                response["result"] = {{"protocolVersion": "2024-11-05"}}
            elif method == "tools/list":
                response["result"] = {{"tools": [{{"name": {listed_tool!r}, "inputSchema": {{"type": "object"}}}}]}}
            elif method == "resources/list":
                response["result"] = {{"resources": []}}
            elif method == "tool_capabilities":
                response["result"] = {{"tools": {capability_tools!r}}}
            elif method == "policy_describe":
                response["result"] = {{"write_tools_enabled": {json.dumps(policy_writes)}}}
            else:
                continue
            print(json.dumps(response), flush=True)
        '''
    )


class McpAttestationProcessTests(unittest.TestCase):
    def run_attestation(self, matrix_text: str, bridge_text: str) -> subprocess.CompletedProcess[str]:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            matrix_path = root / "matrix.md"
            bridge_path = root / "bridge.py"
            matrix_path.write_text(matrix_text)
            bridge_path.write_text(bridge_text)
            bridge_path.chmod(0o755)
            return subprocess.run(
                [sys.executable, str(SMOKE), "--attest-matrix", str(matrix_path), "--", str(bridge_path)],
                text=True,
                capture_output=True,
                check=False,
            )

    def test_attestation_reports_only_public_tool_and_passes_complete_bridge_evidence(self) -> None:
        result = self.run_attestation(matrix("get_record"), bridge_program())
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "get_record: pass")
        self.assertNotIn("daemon policy", result.stdout.lower())

    def test_attestation_blocks_advertised_tool_when_capability_is_disabled(self) -> None:
        result = self.run_attestation(matrix("get_record"), bridge_program(capability_enabled=False))
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), "get_record: blocked")
        self.assertNotIn("environment", result.stderr.lower())

    def test_attestation_blocks_malformed_introspection_reply(self) -> None:
        result = self.run_attestation(matrix("get_record"), bridge_program(malformed_capabilities=True))
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), "get_record: blocked")

    def test_attestation_blocks_unavailable_bridge_mapping(self) -> None:
        result = self.run_attestation(matrix("get_record"), bridge_program(listed_tool="other_public_tool"))
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), "get_record: blocked")

    def test_attestation_blocks_contradictory_matrix_transport(self) -> None:
        result = self.run_attestation(matrix("get_record", bridge_transport="blocked"), bridge_program())
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), "get_record: blocked")

    def test_attestation_blocks_bridge_only_policy_label(self) -> None:
        contradictory = matrix("get_record").replace(
            "bridge report + daemon evidence", "bridge report only"
        )
        result = self.run_attestation(contradictory, bridge_program())
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout.strip(), "get_record: blocked")

    def test_schema_smoke_mode_remains_available(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            bridge_path = Path(temp_dir) / "bridge.py"
            bridge_path.write_text(bridge_program())
            bridge_path.chmod(0o755)
            result = subprocess.run(
                [sys.executable, str(SMOKE), "--", str(bridge_path)],
                text=True,
                capture_output=True,
                check=False,
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            result.stdout.strip(),
            "mcp schema smoke passed: 1 tools, 0 resources, no top-level schema composition",
        )


if __name__ == "__main__":
    unittest.main()
