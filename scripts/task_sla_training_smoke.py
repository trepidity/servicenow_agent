#!/usr/bin/env python3
"""Guarded live smoke harness for the `snow sla` command.

The guard in this script intentionally runs before credential resolution. It is
safe to use `--check` when validating local configuration because that mode does
not fetch secrets, invoke the CLI, or contact ServiceNow.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from urllib.parse import urlsplit


ALLOWED_INSTANCE_ENV = "SNOW_SMOKE_ALLOWED_INSTANCE"
ENV_FILENAME = ".env.test"
PASSWORD_ENV_KEYS = ("SERVICENOW_PASSWORD", "SNOW_PASSWORD")
USER_ENV_KEYS = ("SERVICENOW_USERNAME", "SERVICENOW_USER", "SNOW_USER")
INSTANCE_ENV_KEYS = ("SERVICENOW_INSTANCE", "SNOW_INSTANCE")
OP_SERVICE_ACCOUNT_TOKEN_FILE_ENV = "OP_SERVICE_ACCOUNT_TOKEN_FILE"
SECRET_ENV_KEYS = (
    "SERVICENOW_PASSWORD",
    "SNOW_PASSWORD",
    "SNOW_CLIENT_SECRET",
    "SERVICENOW_CLIENT_SECRET",
    "SNOW_OAUTH_TOKEN",
    "SERVICENOW_OAUTH_TOKEN",
    "OP_ITEM_ID",
    "OP_VAULT",
    "OP_SERVICE_ACCOUNT_TOKEN",
    OP_SERVICE_ACCOUNT_TOKEN_FILE_ENV,
    "OP_CONNECT_TOKEN",
    "OP_CONNECT_HOST",
    "ONEPASSWORD_CONNECT_TOKEN",
)
ONEPASSWORD_CLI_AUTH_ENV_KEYS = (
    "OP_SERVICE_ACCOUNT_TOKEN",
    "OP_CONNECT_TOKEN",
    "OP_CONNECT_HOST",
    "ONEPASSWORD_CONNECT_TOKEN",
)
SAFE_CHILD_ENV_KEYS = (
    "HOME",
    "LOGNAME",
    "NO_COLOR",
    "PATH",
    "RUST_BACKTRACE",
    "RUST_LOG",
    "SHELL",
    "SSL_CERT_DIR",
    "SSL_CERT_FILE",
    "TERM",
    "USER",
)
UNAVAILABLE_COMMAND_PATTERNS = (
    "unrecognized subcommand",
    "invalid subcommand",
    "unexpected argument 'sla'",
)


class SmokeError(Exception):
    """User-facing failure with an explicit process exit code."""

    def __init__(self, message: str, code: int = 1) -> None:
        super().__init__(message)
        self.code = code


def dedupe_paths(paths: list[Path]) -> list[Path]:
    seen: set[str] = set()
    unique: list[Path] = []
    for path in paths:
        key = str(path.expanduser())
        if key not in seen:
            seen.add(key)
            unique.append(path)
    return unique


def env_search_paths(
    *,
    cwd: Path | None = None,
    script_path: Path | None = None,
    home: Path | None = None,
    executable_dirs: list[Path] | None = None,
) -> list[Path]:
    cwd = cwd or Path.cwd()
    script_path = script_path or Path(__file__)
    home = home or Path.home()
    repo_root = script_path.resolve().parent.parent

    # Match snow's config_path(".env.test") precedence for this smoke path:
    # executable-adjacent, ~/.config/snow, then current directory. Live mode
    # executes an already-built snow binary under target/ or from PATH.
    executable_dirs = executable_dirs or [
        repo_root / "target" / "debug",
        repo_root / "target" / "release",
    ]

    candidates = [path / ENV_FILENAME for path in executable_dirs]
    candidates.extend(
        [
            home / ".config" / "snow" / ENV_FILENAME,
            cwd / ENV_FILENAME,
        ]
    )
    return dedupe_paths(candidates)


def parse_dotenv_line(line: str) -> tuple[str, str] | None:
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None
    if stripped.startswith("export "):
        stripped = stripped[len("export ") :].strip()
    if "=" not in stripped:
        return None

    key, value = stripped.split("=", 1)
    key = key.strip()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
        return None

    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        value = value[1:-1]
    return key, value


def load_env_file(path: Path, environ: dict[str, str] | None = None) -> None:
    environ = environ if environ is not None else os.environ
    for line in path.read_text(encoding="utf-8").splitlines():
        parsed = parse_dotenv_line(line)
        if parsed is None:
            continue
        key, value = parsed
        environ.setdefault(key, value)


def load_first_env_test(
    search_paths: list[Path] | None = None,
    environ: dict[str, str] | None = None,
) -> Path:
    paths = search_paths or env_search_paths()
    for path in paths:
        if path.exists():
            load_env_file(path, environ)
            return path
    searched = ", ".join(str(path) for path in paths)
    raise SmokeError(f"{ENV_FILENAME} not found. Searched: {searched}", code=2)


def normalize_instance_url(raw: str) -> str:
    value = raw.strip()
    if not value:
        raise ValueError("instance URL is empty")

    candidate = value if "://" in value else f"https://{value}"
    parsed = urlsplit(candidate)
    scheme = parsed.scheme.lower()
    if scheme != "https":
        raise ValueError("instance URL must use https")
    if parsed.username or parsed.password:
        raise ValueError("instance URL must not include credentials")
    if not parsed.hostname:
        raise ValueError("instance URL must include a host")
    try:
        if parsed.port is not None:
            raise ValueError("instance URL must not include a port")
    except ValueError as exc:
        raise ValueError(str(exc)) from exc
    if parsed.query:
        raise ValueError("instance URL must not include a query string")
    if parsed.fragment:
        raise ValueError("instance URL must not include a fragment")
    if parsed.path not in ("", "/"):
        raise ValueError("instance URL must not include a path")

    return f"https://{parsed.hostname.lower()}"


def verify_allowed_instance(
    args: argparse.Namespace,
    environ: dict[str, str] | None = None,
) -> str:
    environ = environ if environ is not None else os.environ
    raw = next((environ.get(key) for key in INSTANCE_ENV_KEYS if environ.get(key)), None)
    if raw is None:
        raise SmokeError("SERVICENOW_INSTANCE is not set after loading .env.test", code=2)
    allowed_raw = args.allowed_instance or environ.get(ALLOWED_INSTANCE_ENV)
    if allowed_raw is None:
        raise SmokeError(
            f"{ALLOWED_INSTANCE_ENV} or --allowed-instance is required for live smoke",
            code=2,
        )

    try:
        normalized = normalize_instance_url(raw)
        allowed = normalize_instance_url(allowed_raw)
    except ValueError as exc:
        raise SmokeError(f"live smoke instance guard failed closed: {exc}", code=2) from exc

    if normalized != allowed:
        raise SmokeError(
            "refusing live smoke: normalized SERVICENOW_INSTANCE "
            f"{normalized!r} does not match explicit allow target {allowed!r}",
            code=2,
        )
    return normalized


def record_number_from(args: argparse.Namespace, environ: dict[str, str] | None = None) -> str:
    environ = environ if environ is not None else os.environ
    number = args.number or environ.get("SNOW_TASK_SLA_NUMBER")
    if not number or not number.strip():
        raise SmokeError(
            "record number required via --number or SNOW_TASK_SLA_NUMBER",
            code=2,
        )
    return number.strip()


def non_service_now_child_env(environ: dict[str, str] | None = None) -> dict[str, str]:
    source = environ if environ is not None else os.environ
    child_env = source.copy()
    for key in SECRET_ENV_KEYS:
        child_env.pop(key, None)
    for key in list(child_env):
        if key == "OP_SESSION" or key.startswith("OP_SESSION_"):
            child_env.pop(key, None)
    return child_env


def one_password_child_env(environ: dict[str, str] | None = None) -> dict[str, str]:
    source = environ if environ is not None else os.environ
    child_env = non_service_now_child_env(source)
    for key in ONEPASSWORD_CLI_AUTH_ENV_KEYS:
        value = source.get(key)
        if value:
            child_env[key] = value
    token = one_password_service_account_token(source)
    if token:
        child_env["OP_SERVICE_ACCOUNT_TOKEN"] = token
    for key, value in source.items():
        if value and (key == "OP_SESSION" or key.startswith("OP_SESSION_")):
            child_env[key] = value
    return child_env


def service_now_child_env(
    *,
    password: str,
    username: str,
    normalized_instance: str,
    environ: dict[str, str] | None = None,
) -> dict[str, str]:
    source = environ if environ is not None else os.environ
    child_env = {
        key: source[key]
        for key in SAFE_CHILD_ENV_KEYS
        if key in source and source[key]
    }
    for key in ("SNOW_ENV",):
        if source.get(key):
            child_env[key] = source[key]
    child_env["SERVICENOW_USERNAME"] = username
    child_env["SERVICENOW_USER"] = username
    child_env["SNOW_USER"] = username
    child_env["SERVICENOW_INSTANCE"] = normalized_instance
    child_env["SNOW_INSTANCE"] = normalized_instance
    child_env["SERVICENOW_PASSWORD"] = password
    child_env["SNOW_PASSWORD"] = password
    return child_env


def pop_password_env(environ: dict[str, str] | None = None) -> None:
    target = environ if environ is not None else os.environ
    for key in PASSWORD_ENV_KEYS:
        target.pop(key, None)
        if target is not os.environ:
            os.environ.pop(key, None)


def resolve_password(environ: dict[str, str] | None = None) -> str:
    environ = environ if environ is not None else os.environ
    for key in PASSWORD_ENV_KEYS:
        value = environ.get(key)
        if value and value.strip():
            password = value.strip()
            pop_password_env(environ)
            return password
    return fetch_one_password_field(
        environ.get("OP_ITEM_ID", ""), "password", environ=environ
    )


def fetch_password(op_item_id: str, environ: dict[str, str] | None = None) -> str:
    return fetch_one_password_field(op_item_id, "password", environ=environ)


def resolve_username(environ: dict[str, str] | None = None) -> str:
    environ = environ if environ is not None else os.environ
    for key in USER_ENV_KEYS:
        value = environ.get(key)
        if value and value.strip():
            return value.strip()
    return fetch_one_password_field(
        environ.get("OP_ITEM_ID", ""), "username", environ=environ
    )


def fetch_one_password_field(
    op_item_id: str, field: str, environ: dict[str, str] | None = None
) -> str:
    if not op_item_id.strip():
        raise SmokeError(
            "set SERVICENOW_PASSWORD or OP_ITEM_ID after loading .env.test",
            code=2,
        )

    source = environ if environ is not None else os.environ
    command = op_password_command(op_item_id, field=field, vault=source.get("OP_VAULT"))
    try:
        result = subprocess.run(
            command,
            text=True,
            capture_output=True,
            env=one_password_child_env(environ),
            check=False,
        )
    except FileNotFoundError as exc:
        raise SmokeError("1Password CLI `op` is not available on PATH", code=1) from exc

    if result.returncode != 0:
        stderr = result.stderr.strip()
        detail = f": {stderr}" if stderr else ""
        raise SmokeError(
            f"failed to fetch {field} from 1Password{detail}", code=1
        )

    value = result.stdout.strip()
    if not value:
        raise SmokeError(f"1Password returned an empty {field}", code=1)
    return value


def one_password_service_account_token(source: dict[str, str]) -> str | None:
    token = source.get("OP_SERVICE_ACCOUNT_TOKEN")
    if token and token.strip():
        return token.strip()
    token_file = source.get(OP_SERVICE_ACCOUNT_TOKEN_FILE_ENV)
    if not token_file or not token_file.strip():
        return None
    path = Path(token_file).expanduser()
    try:
        token = path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise SmokeError(
            f"failed to read {OP_SERVICE_ACCOUNT_TOKEN_FILE_ENV}: {exc}", code=1
        ) from exc
    if not token:
        raise SmokeError(f"{OP_SERVICE_ACCOUNT_TOKEN_FILE_ENV} is empty", code=1)
    return token


def op_password_command(
    op_item_id: str, field: str = "password", vault: str | None = None
) -> list[str]:
    item = op_item_id.strip()
    field = field.strip()
    if item.startswith("op://"):
        return ["op", "read", f"{item.rstrip('/')}/{field}"]
    command = ["op", "item", "get", item]
    if vault and vault.strip():
        command.extend(["--vault", vault.strip()])
    command.extend(["--fields", f"label={field}", "--reveal"])
    return command


def repo_root(script_path: Path | None = None) -> Path:
    script_path = script_path or Path(__file__)
    return script_path.resolve().parent.parent


def resolve_snow_binary(
    args: argparse.Namespace,
    *,
    script_path: Path | None = None,
    environ: dict[str, str] | None = None,
) -> str:
    if args.snow_bin:
        return args.snow_bin

    root = repo_root(script_path)
    candidates = [
        root / "target" / "debug" / "snow",
        root / "target" / "release" / "snow",
    ]
    for candidate in candidates:
        if candidate.exists() and os.access(candidate, os.X_OK):
            return str(candidate)

    source = environ if environ is not None else os.environ
    path_binary = shutil.which("snow", path=source.get("PATH"))
    if path_binary:
        return path_binary

    raise SmokeError(
        "snow binary not found; run `cargo build -p snow_cli` first or pass --snow-bin",
        code=2,
    )


def snow_sla_command(snow_bin: str, number: str) -> list[str]:
    return [snow_bin, "--env", "test", "sla", number]


def is_unavailable_command_error(output: str) -> bool:
    lowered = output.lower()
    return any(pattern in lowered for pattern in UNAVAILABLE_COMMAND_PATTERNS)


def run_sla_command(
    number: str,
    password: str,
    username: str,
    normalized_instance: str,
    snow_bin: str,
    environ: dict[str, str] | None = None,
) -> int:
    command = snow_sla_command(snow_bin, number)
    env = service_now_child_env(
        password=password,
        username=username,
        normalized_instance=normalized_instance,
        environ=environ,
    )

    result = subprocess.run(command, text=True, capture_output=True, env=env, check=False)
    if result.stdout:
        sys.stdout.write(result.stdout)
    if result.stderr:
        sys.stderr.write(result.stderr)

    combined = f"{result.stdout}\n{result.stderr}"
    if result.returncode != 0 and is_unavailable_command_error(combined):
        print(
            "snow sla command is unavailable in this checkout. "
            "The live smoke guard passed, but the live smoke cannot run.",
            file=sys.stderr,
        )
        return 3
    return result.returncode


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Guarded live smoke harness for `snow sla <NUMBER>`."
    )
    parser.add_argument(
        "--number",
        help="Training record number. Defaults to SNOW_TASK_SLA_NUMBER.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Validate env loading and the live smoke guard only; no secrets, CLI, or network.",
    )
    parser.add_argument(
        "--allowed-instance",
        help=f"Explicit live instance allow target. Defaults to {ALLOWED_INSTANCE_ENV}.",
    )
    parser.add_argument(
        "--snow-bin",
        help="Path to an already-built snow binary. Defaults to target/debug, target/release, then PATH.",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run the script's local helper tests.",
    )
    return parser.parse_args(argv)


def run_self_tests() -> None:
    example_instance = "https://example.service-now.com"
    assert normalize_instance_url("https://EXAMPLE.service-now.com/") == example_instance
    assert normalize_instance_url("example.service-now.com") == example_instance
    for raw in [
        "http://example.service-now.com",
        "https://example.service-now.com/path",
        "https://example.service-now.com?x=1",
        "https://example.service-now.com#frag",
        "https://example.service-now.com:443",
        "https://user@example.service-now.com",
    ]:
        try:
            normalize_instance_url(raw)
        except ValueError:
            pass
        else:
            raise AssertionError(f"expected normalization failure for {raw!r}")

    env: dict[str, str] = {}
    parsed = parse_dotenv_line("export SERVICENOW_INSTANCE='example.service-now.com'")
    assert parsed == ("SERVICENOW_INSTANCE", "example.service-now.com")
    env["SERVICENOW_INSTANCE"] = "https://example.service-now.com"
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        executable_dir = root / "target" / "debug"
        home = root / "home"
        cwd = root / "cwd"
        executable_dir.mkdir(parents=True)
        (home / ".config" / "snow").mkdir(parents=True)
        cwd.mkdir()

        paths = env_search_paths(
            cwd=cwd,
            script_path=root / "scripts" / "task_sla_training_smoke.py",
            home=home,
            executable_dirs=[executable_dir],
        )
        assert paths == [
            executable_dir / ENV_FILENAME,
            home / ".config" / "snow" / ENV_FILENAME,
            cwd / ENV_FILENAME,
        ]

        dotenv = root / ENV_FILENAME
        dotenv.write_text("SERVICENOW_INSTANCE=example.service-now.com\n", encoding="utf-8")
        load_env_file(dotenv, env)
    assert env["SERVICENOW_INSTANCE"] == "https://example.service-now.com"
    guard_args = argparse.Namespace(allowed_instance=None, snow_bin=None)
    assert verify_allowed_instance(
        guard_args,
        {
            "SERVICENOW_INSTANCE": "https://example.service-now.com",
            ALLOWED_INSTANCE_ENV: "EXAMPLE.service-now.com",
        },
    ) == example_instance
    try:
        verify_allowed_instance(guard_args, {"SERVICENOW_INSTANCE": example_instance})
    except SmokeError:
        pass
    else:
        raise AssertionError("expected missing explicit allow target to fail closed")
    try:
        verify_allowed_instance(
            guard_args,
            {
                "SERVICENOW_INSTANCE": example_instance,
                ALLOWED_INSTANCE_ENV: "other.example.com",
            },
        )
    except SmokeError:
        pass
    else:
        raise AssertionError("expected mismatched allow target to fail closed")
    password_env = {
        "PATH": "/bin",
        "SERVICENOW_PASSWORD": "redacted-password",
        "SERVICENOW_USERNAME": "redacted-user",
        "SNOW_PASSWORD": "redacted-password",
        "SNOW_CLIENT_SECRET": "redacted-secret",
        "OP_SERVICE_ACCOUNT_TOKEN": "redacted-op-token",
        OP_SERVICE_ACCOUNT_TOKEN_FILE_ENV: "/tmp/redacted-op-token",
        "OP_SESSION_test": "redacted-session",
    }
    non_service_env = non_service_now_child_env(password_env)
    for key in SECRET_ENV_KEYS:
        assert key not in non_service_env
    assert "OP_SESSION_test" not in non_service_env
    op_env = one_password_child_env(password_env)
    assert "SERVICENOW_PASSWORD" not in op_env
    assert "SNOW_CLIENT_SECRET" not in op_env
    assert op_env["OP_SERVICE_ACCOUNT_TOKEN"] == "redacted-op-token"
    assert op_env["OP_SESSION_test"] == "redacted-session"
    assert op_password_command("op://vault/item") == [
        "op",
        "read",
        "op://vault/item/password",
    ]
    assert op_password_command("item-123", field="api-password", vault="shared") == [
        "op",
        "item",
        "get",
        "item-123",
        "--vault",
        "shared",
        "--fields",
        "label=api-password",
        "--reveal",
    ]
    service_env = service_now_child_env(
        password="redacted-password",
        username="redacted-user",
        normalized_instance=example_instance,
        environ=password_env,
    )
    assert service_env["SERVICENOW_PASSWORD"] == "redacted-password"
    assert service_env["SNOW_PASSWORD"] == "redacted-password"
    assert service_env["SERVICENOW_USERNAME"] == "redacted-user"
    assert is_unavailable_command_error("error: unrecognized subcommand 'sla'")
    assert not is_unavailable_command_error("error: request timed out")


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    if args.self_test:
        run_self_tests()
        print("self-test passed")
        return 0

    env_path = load_first_env_test()
    normalized_instance = verify_allowed_instance(args)

    number: str | None = None
    try:
        number = record_number_from(args)
    except SmokeError:
        if not args.check:
            raise

    print(f"loaded env: {env_path}")
    print(f"live smoke guard: {normalized_instance}")
    if number:
        print(f"record number: {number}")

    if args.check:
        print("check mode: no credential resolution, CLI, or ServiceNow call was attempted")
        return 0

    assert number is not None
    username = resolve_username()
    password = resolve_password()
    snow_bin = resolve_snow_binary(args)
    return run_sla_command(number, password, username, normalized_instance, snow_bin)


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except SmokeError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(exc.code)
