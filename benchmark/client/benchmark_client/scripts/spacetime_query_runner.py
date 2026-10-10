#!/usr/bin/env python3
"""
Run one SQL statement against SpaceTime (MSQL) via pymsql and report success/row-count on stdout.
Used as a subprocess by SpaceTimeExecutor.java -- no native JDBC driver exists for this
partner-hosted engine (see docs/spacetime-cluster-access.md), and pymsql itself is Python-only.

Read queries use query_arrow (the exact path docs/sample-queries.md already verified against real
data: `rides` -> 50,000,000 rows). Non-SELECT statements produce no Arrow result -- query_arrow's
ArrowResult still runs them to completion server-side before raising on the missing result shape,
so retrying via the plain query() path afterward would execute a write twice. To avoid that, the
statement's leading keyword decides the path up front instead of trying Arrow first and falling
back on failure.

Deploy alongside pymsql's venv on whichever machine runs benchmark_client (nst-inserter, per
docs/spacetime-cluster-access.md -- it's the only client box with a route to the private
10.1.0.x network the SpaceTime nodes live on):

    cp -r <bench directory provided by the SpaceTime team> ~/bench   # brings the .venv with pymsql already installed
    cp spacetime_query_runner.py ~/bench/

    echo 'SELECT COUNT(*) FROM rides' | ~/bench/.venv/bin/python spacetime_query_runner.py \
        --host 10.1.0.11 --port 31340

With --serve (2026-10-07) the process stays up and keeps one pymsql connection open, so the
benchmark no longer pays for a Python start, the pymsql import and a new connection on every query
(~105 ms). SpaceTimeExecutor.java keeps one such worker per benchmark thread. Protocol, one request
at a time over stdin/stdout:

    request:  "<byte length>\n<SQL, UTF-8>"
    reply:    "OK <rows>\n"  or  "ERR <message on one line>\n"

After an error the worker reconnects before replying, so the next query starts from a clean session.
"""
from __future__ import annotations

import argparse
import asyncio
import sys

import pymsql
from pymsql.events import QueryStatus

_READ_KEYWORDS = {"SELECT", "WITH", "EXPLAIN", "SHOW", "TABLE"}


def _is_read(sql: str) -> bool:
    stripped = sql.strip()
    if not stripped:
        return False
    first_word = stripped.split(None, 1)[0].upper()
    return first_word in _READ_KEYWORDS


async def run_arrow(conn: pymsql.Connection, sql: str) -> int:
    row_count = 0
    async with conn.query_arrow(sql) as result:
        async for batch in result:
            row_count += batch.num_rows
    return row_count


async def run_native(conn: pymsql.Connection, sql: str) -> int:
    stream = await conn.query(sql)
    affected = 0
    async for event in stream:
        if event.is_error:
            raise RuntimeError(f"{event.error.code}: {event.error.message}")
        if event.is_statement_complete and event.statement_complete.affected_rows is not None:
            affected = event.statement_complete.affected_rows
        if event.is_query_done and event.query_done.status == QueryStatus.FAILED:
            raise RuntimeError("query failed")
    return affected


async def main_async(host: str, port: int, sql: str) -> int:
    conn = pymsql.Connection(host, port)
    try:
        if _is_read(sql):
            return await run_arrow(conn, sql)
        return await run_native(conn, sql)
    finally:
        await conn.close()


async def serve(host: str, port: int) -> int:
    loop = asyncio.get_running_loop()
    stdin = sys.stdin.buffer
    conn = pymsql.Connection(host, port)
    try:
        while True:
            header = await loop.run_in_executor(None, stdin.readline)
            if not header:
                return 0
            body = await loop.run_in_executor(None, stdin.read, int(header))
            sql = body.decode("utf-8").strip().rstrip(";")
            try:
                rows = await (run_arrow(conn, sql) if _is_read(sql) else run_native(conn, sql))
                reply = f"OK {rows}"
            except Exception as exc:  # noqa: BLE001 -- reported to the caller, worker keeps serving
                reply = "ERR " + " ".join(str(exc).split())
                try:
                    await conn.close()
                except Exception:  # noqa: BLE001
                    pass
                conn = pymsql.Connection(host, port)
            sys.stdout.write(reply + "\n")
            sys.stdout.flush()
    finally:
        await conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, default=31340)
    parser.add_argument("--serve", action="store_true",
                        help="stay up and answer framed requests on stdin (see module docstring)")
    args = parser.parse_args()

    if args.serve:
        return asyncio.run(serve(args.host, args.port))

    sql = sys.stdin.read().strip().rstrip(";")
    if not sql:
        print("no statement given", file=sys.stderr)
        return 2
    try:
        count = asyncio.run(main_async(args.host, args.port, sql))
    except Exception as exc:  # noqa: BLE001 -- reported to the caller via stderr + exit code
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(f"ROWS:{count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
