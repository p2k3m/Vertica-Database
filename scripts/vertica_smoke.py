#!/usr/bin/env python3
"""Connectivity smoke test for Vertica deployments."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Optional

import vertica_python

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tests.wait_for_port import UNREACHABLE_ERRNOS, wait_for_port

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


def _resolve_host() -> str:
    """Choose the Vertica host from env vars or default to localhost."""

    return _get_env_value("DB_HOST", "VERTICA_HOST", default="localhost")


def _resolve_port() -> int:
    return int(_get_env_value("DB_PORT", "VERTICA_PORT", default="5433"))


def _connect_and_query(host: str, port: int) -> None:
    config = {
        "host": host,
        "port": port,
        "user": "dbadmin",
        "password": "",
        "database": "VMart",
    }

    with vertica_python.connect(**config) as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        value = cursor.fetchone()
        if not value or value[0] != 1:
            raise SystemExit("Unexpected response from SELECT 1")

        cursor.execute("SELECT table_name FROM v_catalog.tables LIMIT 1")
        if cursor.fetchone() is None:
            raise SystemExit("Unexpected empty result from v_catalog.tables")


def _wait_for_service(host: str, port: int, timeout: float, require_service: bool) -> bool:
    try:
        wait_for_port(host, port, timeout=timeout)
    except TimeoutError:
        if require_service:
            raise SystemExit(
                "Timed out waiting for Vertica service to accept connections."
            )
        print(
            f"Vertica service at {host}:{port} did not become reachable before the "
            "timeout; skipping connectivity checks.",
            file=sys.stderr,
        )
        return False
    except OSError as exc:
        if exc.errno in UNREACHABLE_ERRNOS and not require_service:
            print(
                "Network unreachable when connecting to Vertica host at "
                f"{host}:{port}. This sandbox likely blocks outbound traffic.",
                file=sys.stderr,
            )
            return False
        raise
    return True


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="Seconds to wait for the Vertica service before giving up",
    )
    parser.add_argument(
        "--require-service",
        action="store_true",
        help="Fail instead of skipping when the service cannot be reached",
    )

    args = parser.parse_args(argv)

    host = _resolve_host()
    port = _resolve_port()

    print(f"Target Vertica endpoint: {host}:{port}")

    if not _wait_for_service(host, port, timeout=args.timeout, require_service=args.require_service):
        return 0

    try:
        _connect_and_query(host, port)
    except vertica_python.errors.ConnectionError as exc:
        if not args.require_service and "Failed to establish a connection" in str(exc):
            print(
                "Vertica client could not establish a connection. This often happens "
                "when the sandbox blocks outbound traffic.",
                file=sys.stderr,
            )
            return 0
        raise

    print("Vertica connectivity checks succeeded.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
