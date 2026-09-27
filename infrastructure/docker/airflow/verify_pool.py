"""Fail unless an Airflow pool readback has the required writer capacity."""

from __future__ import annotations

import json
import sys


def main() -> int:
    payload = json.load(sys.stdin)
    if not isinstance(payload, list) or len(payload) != 1:
        return 1
    pool = payload[0]
    if not isinstance(pool, dict):
        return 1
    return 0 if pool.get("pool") == "lakehouse_writes" and pool.get("slots") == "8" else 1


if __name__ == "__main__":
    raise SystemExit(main())
