import os
from typing import Optional

import pytest
import vertica_python

from .wait_for_port import UNREACHABLE_ERRNOS, wait_for_port


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


def test_can_connect_and_query():
    try:
        wait_for_port(HOST, PORT)
    except OSError as exc:
        if exc.errno in UNREACHABLE_ERRNOS:
            pytest.skip(
                "Network unreachable when connecting to Vertica host; sandbox likely blocks outbound traffic"
            )
        raise
    with vertica_python.connect(**CONFIG) as connection:
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        assert cursor.fetchone()[0] == 1

        cursor.execute("SELECT table_name FROM v_catalog.tables LIMIT 1")
        assert cursor.fetchone() is not None
