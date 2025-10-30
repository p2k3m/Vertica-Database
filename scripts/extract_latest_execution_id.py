#!/usr/bin/env python3
"""Extract the latest SSM association execution id from JSON."""
from __future__ import annotations

import json
import sys


def main() -> int:
    try:
        payload = json.loads(sys.stdin.read())
    except json.JSONDecodeError:
        return 0

    executions = payload.get("AssociationExecutions")
    if isinstance(executions, list) and executions:
        first = executions[0]
        if isinstance(first, dict):
            execution_id = first.get("ExecutionId")
            if isinstance(execution_id, str):
                print(execution_id, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
