#!/usr/bin/env python3
"""Utility script to wait for a TCP port to open."""

import argparse
import socket
import sys
import time


def wait_for_port(host: str, port: int, timeout: int) -> int:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                return 0
        except OSError:
            time.sleep(2)
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("host")
    parser.add_argument("port", type=int)
    parser.add_argument("--timeout", type=int, default=120)
    args = parser.parse_args()
    return wait_for_port(args.host, args.port, args.timeout)


if __name__ == "__main__":
    sys.exit(main())
