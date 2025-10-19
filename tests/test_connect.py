import os
import socket
import time

import vertica_python


HOST = os.getenv("DB_HOST", "localhost")
PORT = int(os.getenv("DB_PORT", "5433"))
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
