#!/usr/bin/env python3
"""Parse SSM describe-instance-information output."""
from __future__ import annotations

import json
import shlex
import sys


def main() -> int:
    try:
        data = json.load(sys.stdin)
    except json.JSONDecodeError:
        data = []

    status = ""
    counts: dict[str, int] = {}

    if isinstance(data, list) and data:
        record = data[0]
        if isinstance(record, dict):
            value = record.get("AssociationStatus")
            if isinstance(value, str):
                status = value.strip()
            overview = record.get("AssociationOverview")
            if isinstance(overview, dict):
                aggregated = overview.get("InstanceAssociationStatusAggregatedCount")
                if isinstance(aggregated, dict):
                    counts = {
                        str(key): int(value)
                        for key, value in aggregated.items()
                        if isinstance(value, (int, float))
                    }

    counts_json = json.dumps(counts, sort_keys=True)

    print(f"association_status={shlex.quote(status)}")
    print(f"association_counts={shlex.quote(counts_json)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
