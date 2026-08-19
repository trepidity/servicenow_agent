#!/usr/bin/env python3
"""Reject tracked production Rust sources that Cargo never compiles."""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
RUST_ARTIFACT = re.compile(r"^lib(?P<stem>.+)\.(?:rlib|rmeta)$")


def run(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=REPO_ROOT,
        check=True,
        text=True,
        stdout=subprocess.PIPE,
    )


def workspace_packages() -> tuple[set[str], list[Path]]:
    metadata = json.loads(
        run("cargo", "metadata", "--no-deps", "--format-version", "1").stdout
    )
    members = set(metadata["workspace_members"])
    source_roots = [
        Path(package["manifest_path"]).resolve().parent / "src"
        for package in metadata["packages"]
        if package["id"] in members
    ]
    return members, source_roots


def is_below(path: Path, directory: Path) -> bool:
    try:
        path.relative_to(directory)
    except ValueError:
        return False
    return True


def dependency_file(artifact_filename: str) -> Path | None:
    artifact = Path(artifact_filename)
    match = RUST_ARTIFACT.match(artifact.name)
    if match is None:
        return None
    return artifact.with_name(f"{match.group('stem')}.d")


def compiled_sources(
    workspace_members: set[str], source_roots: list[Path]
) -> set[Path]:
    cargo = run(
        "cargo",
        "check",
        "--workspace",
        "--all-targets",
        "--all-features",
        "--message-format=json",
    )
    dependency_files: set[Path] = set()
    expected_targets: set[Path] = set()
    covered_targets: set[Path] = set()

    for line in cargo.stdout.splitlines():
        message = json.loads(line)
        if message.get("reason") != "compiler-artifact":
            continue
        if message.get("package_id") not in workspace_members:
            continue

        target_source = Path(message["target"]["src_path"]).resolve()
        if not any(is_below(target_source, root) for root in source_roots):
            continue
        expected_targets.add(target_source)

        for filename in message["filenames"]:
            dep_file = dependency_file(filename)
            if dep_file is not None and dep_file.exists():
                dependency_files.add(dep_file)
                covered_targets.add(target_source)

    missing_targets = sorted(expected_targets - covered_targets)
    if missing_targets:
        missing = ", ".join(str(path.relative_to(REPO_ROOT)) for path in missing_targets)
        raise RuntimeError(f"Cargo produced no dependency file for: {missing}")
    if not expected_targets:
        raise RuntimeError("Cargo reported no workspace production targets")

    sources: set[Path] = set()
    for dep_file in dependency_files:
        contents = dep_file.read_text(encoding="utf-8").replace("\\\n", "")
        for token in contents.split():
            candidate = token.removesuffix(":")
            if not candidate.endswith(".rs"):
                continue
            path = Path(candidate)
            if not path.is_absolute():
                path = REPO_ROOT / path
            path = path.resolve()
            if any(is_below(path, root) for root in source_roots):
                sources.add(path)
    return sources


def tracked_production_sources(source_roots: list[Path]) -> set[Path]:
    tracked = run("git", "ls-files", "-z", "--", "*.rs").stdout.split("\0")
    sources: set[Path] = set()
    for filename in tracked:
        if not filename:
            continue
        path = (REPO_ROOT / filename).resolve()
        if path.exists() and any(is_below(path, root) for root in source_roots):
            sources.add(path)
    return sources


def main() -> int:
    try:
        workspace_members, source_roots = workspace_packages()
        reachable = compiled_sources(workspace_members, source_roots)
        orphaned = sorted(tracked_production_sources(source_roots) - reachable)
    except (
        json.JSONDecodeError,
        OSError,
        subprocess.CalledProcessError,
        RuntimeError,
    ) as error:
        print(f"Rust source graph check could not run: {error}", file=sys.stderr)
        return 2

    if orphaned:
        print(
            "Tracked production Rust sources outside Cargo's compiled module graph:",
            file=sys.stderr,
        )
        for path in orphaned:
            print(f"  - {path.relative_to(REPO_ROOT)}", file=sys.stderr)
        print("Declare each file as a Cargo target/module or remove it.", file=sys.stderr)
        return 1

    print(
        "Rust source graph check passed: every tracked production source is compiled."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
