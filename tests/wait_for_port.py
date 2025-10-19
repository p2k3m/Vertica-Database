#!/usr/bin/env python3
"""Utility script to wait for a TCP port to open."""

import argparse
import errno
import socket
import sys
import time


UNREACHABLE_ERRNOS = {errno.ENETUNREACH, errno.EHOSTUNREACH}


def wait_for_port(host: str, port: int, timeout: int = 120) -> None:
    deadline = time.time() + timeout
    last_error: OSError | None = None
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                return
        except OSError as exc:
            last_error = exc
            if exc.errno in UNREACHABLE_ERRNOS:
                raise
            time.sleep(2)
    if last_error is not None:
        raise TimeoutError(f"Timed out waiting for {host}:{port}") from last_error
    raise TimeoutError(f"Timed out waiting for {host}:{port}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("host")
    parser.add_argument("port", type=int)
    parser.add_argument("--timeout", type=int, default=120)
    args = parser.parse_args()
    try:
        wait_for_port(args.host, args.port, args.timeout)
        return 0
    except TimeoutError as exc:
        print(exc, file=sys.stderr)
        return 1
    except OSError as exc:
        if exc.errno in UNREACHABLE_ERRNOS:
            print(
                f"Network unreachable while connecting to {args.host}:{args.port}",
                file=sys.stderr,
            )
            return 2
        raise


if __name__ == "__main__":
    sys.exit(main())
