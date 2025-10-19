#!/usr/bin/env python3
"""Display Vertica connection details from Terraform outputs or environment variables."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Optional

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_TERRAFORM_DIR = REPO_ROOT / "infra"

_ENV_KEYS = {
    "host": ("DB_HOST", "VERTICA_HOST"),
    "port": ("DB_PORT", "VERTICA_PORT"),
    "username": ("DB_USER", "VERTICA_USER"),
    "password": ("DB_PASSWORD", "VERTICA_PASSWORD"),
    "database": ("DB_NAME", "VERTICA_DATABASE"),
}


def _first_env(*keys: str) -> Optional[str]:
    for key in keys:
        value = os.getenv(key)
        if value:
            return value
    return None


def _terraform_output(terraform_dir: Path) -> Dict[str, Any]:
    if not terraform_dir.exists():
        return {}

    command = ["terraform", "-chdir", str(terraform_dir), "output", "-json"]
    try:
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return {}
    except subprocess.CalledProcessError:
        return {}

    try:
        parsed = json.loads(result.stdout)
    except json.JSONDecodeError:
        return {}

    if not isinstance(parsed, dict):
        return {}
    return parsed


def _extract_connection_details(outputs: Dict[str, Any]) -> Dict[str, str]:
    connection_block = outputs.get("connection_details")
    if isinstance(connection_block, dict):
        value = connection_block.get("value", connection_block)
        if isinstance(value, dict):
            return {key: str(val) if val is not None else "" for key, val in value.items()}
    return {}


def _build_connection_url(details: Dict[str, str]) -> str:
    host = details.get("host") or details.get("public_dns") or details.get("public_ip")
    if not host:
        return ""

    username = details.get("username", "dbadmin")
    password = details.get("password", "")
    port = details.get("port", "5433")
    database = details.get("database", "VMart")

    auth = username if not password else f"{username}:{password}"
    return f"vertica://{auth}@{host}:{port}/{database}"


def _collect_details() -> Dict[str, str]:
    terraform_dir = Path(os.getenv("TERRAFORM_DIR", DEFAULT_TERRAFORM_DIR))
    outputs = _terraform_output(terraform_dir)
    details = _extract_connection_details(outputs)

    # Merge in environment overrides, falling back to Terraform values.
    for key, env_keys in _ENV_KEYS.items():
        env_value = _first_env(*env_keys)
        if env_value:
            details[key] = env_value

    # Ensure sensible defaults when Terraform output is missing.
    details.setdefault("username", "dbadmin")
    details.setdefault("password", "")
    details.setdefault("database", "VMart")
    details.setdefault("port", "5433")

    if "host" not in details:
        host = details.get("public_ip") or details.get("public_dns") or _first_env(*_ENV_KEYS["host"])
        if host:
            details["host"] = host

    if "connection_url" not in details:
        connection_url = _build_connection_url(details)
        if connection_url:
            details["connection_url"] = connection_url

    return details


def _format_value(label: str, value: str) -> str:
    if label.lower() == "password" and value == "":
        return f"{label}: (empty)"
    return f"{label}: {value}"


def main(argv: Optional[list[str]] = None) -> int:
    details = _collect_details()
    if not details:
        print(
            "No connection details could be determined. Ensure Terraform has been applied "
            "or set the DB_HOST/DB_PORT environment variables.",
            file=sys.stderr,
        )
        return 1

    ordered_labels = [
        ("Connection URL", details.get("connection_url", "")),
        ("Public IP", details.get("public_ip", "")),
        ("Public DNS", details.get("public_dns", "")),
        ("Host", details.get("host", "")),
        ("Port", details.get("port", "")),
        ("Database Username", details.get("username", "")),
        ("Database Password", details.get("password", "")),
        ("Database", details.get("database", "")),
    ]

    print("Vertica connection details:")
    for label, value in ordered_labels:
        if value:
            print(f"  {_format_value(label, value)}")

    missing = [label for label, value in ordered_labels if not value]
    if missing:
        print(
            "\nWarning: The following fields are unavailable: " + ", ".join(missing),
            file=sys.stderr,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
