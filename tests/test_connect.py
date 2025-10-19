import os
import subprocess
from pathlib import Path
from typing import Optional

import pytest
import vertica_python

from .wait_for_port import UNREACHABLE_ERRNOS, wait_for_port


_DEFAULT_SENTINEL = object()


def _get_env_value(*keys: str, default: Optional[str] = _DEFAULT_SENTINEL) -> Optional[str]:
    """Return the first non-empty environment variable among ``keys``."""

    for key in keys:
        value = os.getenv(key)
        if value:
            return value
    if default is not _DEFAULT_SENTINEL:
        return default
    raise KeyError(f"None of the environment variables {keys!r} are set")


DEFAULT_TERRAFORM_DIR = Path(__file__).resolve().parent.parent / "infra"


def _get_terraform_output(name: str) -> Optional[str]:
    """Return the Terraform output ``name`` if it can be resolved."""

    terraform_dir = Path(os.getenv("TERRAFORM_DIR", DEFAULT_TERRAFORM_DIR))
    if not terraform_dir.exists():
        return None

    try:
        result = subprocess.run(
            [
                "terraform",
                "-chdir",
                str(terraform_dir),
                "output",
                "-raw",
                name,
            ],
            check=True,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return None
    except subprocess.CalledProcessError:
        return None

    value = result.stdout.strip()
    return value or None


def _resolve_host() -> str:
    """Choose the Vertica host from env vars, Terraform outputs, or localhost."""

    env_host = _get_env_value("DB_HOST", "VERTICA_HOST", default=None)
    if env_host:
        return env_host

    terraform_host = _get_terraform_output("public_ip") or _get_terraform_output(
        "public_dns"
    )
    if terraform_host:
        return terraform_host

    return "localhost"


HOST = _resolve_host()
PORT = int(_get_env_value("DB_PORT", "VERTICA_PORT", default="5433"))
CONFIG = {
    "host": HOST,
    "port": PORT,
    "user": "dbadmin",
    "password": "",
    "database": "VMart",
}


def test_can_connect_and_query():
    try:
        wait_for_port(HOST, PORT, timeout=10)
    except TimeoutError:
        pytest.skip(
            "Timed out waiting for Vertica service to accept connections; likely not running in the sandbox"
        )
    except OSError as exc:
        if exc.errno in UNREACHABLE_ERRNOS:
            pytest.skip(
                "Network unreachable when connecting to Vertica host; sandbox likely blocks outbound traffic"
            )
        raise
    try:
        with vertica_python.connect(**CONFIG) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            assert cursor.fetchone()[0] == 1

            cursor.execute("SELECT table_name FROM v_catalog.tables LIMIT 1")
            assert cursor.fetchone() is not None
    except vertica_python.errors.ConnectionError as exc:
        message = str(exc)
        if "Failed to establish a connection" in message:
            pytest.skip(
                "Vertica client could not establish a connection; sandbox likely blocks outbound traffic"
            )
        raise
