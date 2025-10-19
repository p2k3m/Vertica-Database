import os
import socket
import time
from typing import Optional

import vertica_python


def _get_env_value(*keys: str, default: Optional[str] = None) -> str:
    """Return the first non-empty environment variable among ``keys``."""

    for key in keys:
        value = os.getenv(key)
        if value:
            return value
    if default is not None:
        return default
    raise KeyError(f"None of the environment variables {keys!r} are set")


HOST = _get_env_value("DB_HOST", "VERTICA_HOST", default="localhost")
PORT = int(_get_env_value("DB_PORT", "VERTICA_PORT", default="5433"))
CONFIG = {
    "host": HOST,
    "port": PORT,
    "user": "dbadmin",
    "password": "",
    "database": "VMart",
}


def wait_for_port(host: str, port: int, timeout: int = 120) -> None:
    """Poll the given host/port until it opens or timeout expires."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                return
        except OSError:
            time.sleep(2)
    raise TimeoutError(f"Timed out waiting for {host}:{port}")


def test_can_connect_and_query():
    wait_for_port(HOST, PORT)
    with vertica_python.connect(**CONFIG) as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        assert cursor.fetchone()[0] == 1

        cursor.execute("SELECT table_name FROM v_catalog.tables LIMIT 1")
        assert cursor.fetchone() is not None
