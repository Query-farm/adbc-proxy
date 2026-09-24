#!/usr/bin/env python3
# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Concurrent end-to-end load and soak test for the exported proxy driver."""

from __future__ import annotations

import argparse
import concurrent.futures
import contextlib
import json
import math
import os
import platform
import statistics
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import adbc_driver_manager
import pyarrow


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} must be set")
    return value


def database_options() -> dict[str, str]:
    options = {
        "driver": str(Path(require_env("GRAINLIFT_DRIVER")).resolve(strict=True)),
        "entrypoint": "AdbcDriverGrainliftInit",
        "grainlift.uri": require_env("GRAINLIFT_ENDPOINT"),
        "grainlift.target": require_env("GRAINLIFT_TARGET"),
    }
    optional = {
        "GRAINLIFT_TOKEN": "grainlift.auth.bearer_token",
        "GRAINLIFT_IROH_DIRECT_ADDRESS": "grainlift.iroh.direct_address",
        "GRAINLIFT_IROH_SECRET_KEY": "grainlift.iroh.secret_key",
        "GRAINLIFT_TLS_CA": "grainlift.tls.ca",
        "GRAINLIFT_TLS_CERT": "grainlift.tls.cert",
        "GRAINLIFT_TLS_KEY": "grainlift.tls.key",
        "GRAINLIFT_TLS_SERVER_NAME": "grainlift.tls.server_name",
    }
    for environment, option in optional.items():
        if value := os.environ.get(environment):
            options[option] = value
    if value := os.environ.get("GRAINLIFT_DOWNSTREAM_URI"):
        options["uri"] = value
    return options


def execute_update(connection: adbc_driver_manager.AdbcConnection, sql: str) -> None:
    statement = adbc_driver_manager.AdbcStatement(connection)
    try:
        statement.set_sql_query(sql)
        statement.execute_update()
    finally:
        statement.close()


def prepare_dataset(backend: str, rows: int, payload_bytes: int) -> None:
    database = adbc_driver_manager.AdbcDatabase(**database_options())
    try:
        connection = adbc_driver_manager.AdbcConnection(database)
        try:
            execute_update(connection, "DROP TABLE IF EXISTS grainlift_load")
            execute_update(
                connection,
                "CREATE TABLE grainlift_load (id BIGINT NOT NULL, payload TEXT NOT NULL)",
            )
            if backend == "postgresql":
                source = f"generate_series(0, {rows - 1}) AS id"
                payload = f"repeat('x', {payload_bytes})"
            elif backend == "duckdb":
                source = f"range({rows}) AS values_table(id)"
                payload = f"repeat('x', {payload_bytes})"
            elif backend == "sqlite":
                source = (
                    "(WITH RECURSIVE values_table(id) AS "
                    f"(VALUES(0) UNION ALL SELECT id + 1 FROM values_table WHERE id + 1 < {rows}) "
                    "SELECT id FROM values_table)"
                )
                payload = "'" + ("x" * payload_bytes) + "'"
            else:
                raise RuntimeError(f"unsupported backend: {backend}")
            execute_update(
                connection,
                f"INSERT INTO grainlift_load SELECT id, {payload} FROM {source}",
            )
        finally:
            connection.close()
    finally:
        database.close()


@dataclass
class WorkerResult:
    worker: int
    connect_seconds: float
    latencies_seconds: list[float]
    queries: int
    rows: int
    batches: int
    bytes: int
    error: str | None = None


def consume_query(
    statement: adbc_driver_manager.AdbcStatement,
    offset: int,
    query_rows: int,
) -> tuple[int, int, int]:
    statement.set_sql_query(
        "SELECT id, payload FROM grainlift_load "
        f"WHERE id >= {offset} AND id < {offset + query_rows} ORDER BY id"
    )
    stream, _ = statement.execute_query()
    row_count = 0
    batch_count = 0
    byte_count = 0
    with pyarrow.RecordBatchReader._import_from_c(stream.address) as reader:
        for batch in reader:
            row_count += batch.num_rows
            batch_count += 1
            byte_count += batch.nbytes
    if row_count != query_rows:
        raise AssertionError(f"expected {query_rows} rows, received {row_count}")
    return row_count, batch_count, byte_count


def run_worker(
    worker: int,
    args: argparse.Namespace,
    barrier: threading.Barrier,
) -> WorkerResult:
    database: adbc_driver_manager.AdbcDatabase | None = None
    connection: adbc_driver_manager.AdbcConnection | None = None
    statement: adbc_driver_manager.AdbcStatement | None = None
    started = time.perf_counter()
    try:
        database = adbc_driver_manager.AdbcDatabase(**database_options())
        connection = adbc_driver_manager.AdbcConnection(database)
        connect_seconds = time.perf_counter() - started
        statement = adbc_driver_manager.AdbcStatement(connection)
        barrier.wait(timeout=args.startup_timeout)

        for iteration in range(args.warmup):
            offset = ((worker * args.query_rows) + iteration) % (
                args.rows - args.query_rows + 1
            )
            consume_query(statement, offset, args.query_rows)
        barrier.wait(timeout=args.startup_timeout)

        latencies: list[float] = []
        total_rows = 0
        total_batches = 0
        total_bytes = 0
        deadline = (
            time.monotonic() + args.duration_seconds
            if args.duration_seconds > 0
            else None
        )
        iteration = 0
        while iteration < args.iterations or deadline is not None:
            if deadline is not None and time.monotonic() >= deadline:
                break
            offset = ((worker * args.query_rows) + iteration * 7919) % (
                args.rows - args.query_rows + 1
            )
            query_started = time.perf_counter()
            rows, batches, byte_count = consume_query(
                statement, offset, args.query_rows
            )
            latencies.append(time.perf_counter() - query_started)
            total_rows += rows
            total_batches += batches
            total_bytes += byte_count
            iteration += 1
        return WorkerResult(
            worker=worker,
            connect_seconds=connect_seconds,
            latencies_seconds=latencies,
            queries=len(latencies),
            rows=total_rows,
            batches=total_batches,
            bytes=total_bytes,
        )
    except Exception as error:
        barrier.abort()
        return WorkerResult(
            worker=worker,
            connect_seconds=time.perf_counter() - started,
            latencies_seconds=[],
            queries=0,
            rows=0,
            batches=0,
            bytes=0,
            error=f"{type(error).__name__}: {error}",
        )
    finally:
        if statement is not None:
            with contextlib.suppress(Exception):
                statement.close()
        if connection is not None:
            with contextlib.suppress(Exception):
                connection.close()
        if database is not None:
            with contextlib.suppress(Exception):
                database.close()


class RssSampler:
    def __init__(self, pid: int | None) -> None:
        self.pid = pid
        self.samples_kib: list[int] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self.pid is None:
            return
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)

    def _run(self) -> None:
        while not self._stop.wait(0.1):
            try:
                output = subprocess.check_output(
                    ["ps", "-o", "rss=", "-p", str(self.pid)],
                    text=True,
                    stderr=subprocess.DEVNULL,
                ).strip()
                if output:
                    self.samples_kib.append(int(output.split()[0]))
            except (OSError, ValueError, subprocess.SubprocessError):
                return


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, math.ceil(fraction * len(ordered)) - 1))
    return ordered[index]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--iterations", type=int, default=50)
    parser.add_argument("--duration-seconds", type=float, default=0)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--rows", type=int, default=100_000)
    parser.add_argument("--query-rows", type=int, default=2_048)
    parser.add_argument("--payload-bytes", type=int, default=128)
    parser.add_argument("--startup-timeout", type=float, default=60)
    parser.add_argument("--json-output", type=Path)
    args = parser.parse_args()
    for name in ("workers", "iterations", "rows", "query_rows", "payload_bytes"):
        if getattr(args, name) <= 0:
            parser.error(f"--{name.replace('_', '-')} must be positive")
    if args.query_rows > args.rows:
        parser.error("--query-rows cannot exceed --rows")
    if args.warmup < 0 or args.duration_seconds < 0 or args.startup_timeout <= 0:
        parser.error("warmup/duration/startup-timeout values are invalid")
    return args


def main() -> None:
    args = parse_args()
    backend = require_env("GRAINLIFT_BACKEND")
    transport = require_env("GRAINLIFT_TRANSPORT")
    server_pid = (
        int(value) if (value := os.environ.get("GRAINLIFT_SERVER_PID")) else None
    )

    sampler = RssSampler(server_pid)
    sampler.start()
    prepare_started = time.perf_counter()
    prepare_dataset(backend, args.rows, args.payload_bytes)
    prepare_seconds = time.perf_counter() - prepare_started

    barrier = threading.Barrier(args.workers + 1)
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(run_worker, worker, args, barrier)
            for worker in range(args.workers)
        ]
        try:
            barrier.wait(timeout=args.startup_timeout)
            barrier.wait(timeout=args.startup_timeout)
            measured_started = time.perf_counter()
        except threading.BrokenBarrierError:
            measured_started = time.perf_counter()
        results = [future.result() for future in futures]
    measured_seconds = time.perf_counter() - measured_started
    sampler.stop()

    errors = [result.error for result in results if result.error]
    latencies = [latency for result in results for latency in result.latencies_seconds]
    connect_latencies = [
        result.connect_seconds for result in results if not result.error
    ]
    queries = sum(result.queries for result in results)
    rows = sum(result.rows for result in results)
    batches = sum(result.batches for result in results)
    byte_count = sum(result.bytes for result in results)
    report: dict[str, Any] = {
        "backend": backend,
        "transport": transport,
        "platform": platform.platform(),
        "workers": args.workers,
        "iterations_per_worker": args.iterations
        if args.duration_seconds == 0
        else None,
        "duration_target_seconds": args.duration_seconds or None,
        "dataset_rows": args.rows,
        "query_rows": args.query_rows,
        "payload_bytes": args.payload_bytes,
        "prepare_seconds": prepare_seconds,
        "measured_seconds": measured_seconds,
        "queries": queries,
        "rows": rows,
        "batches": batches,
        "arrow_bytes": byte_count,
        "queries_per_second": queries / measured_seconds if measured_seconds else 0,
        "rows_per_second": rows / measured_seconds if measured_seconds else 0,
        "mib_per_second": byte_count / measured_seconds / (1024 * 1024)
        if measured_seconds
        else 0,
        "latency_ms": {
            "min": min(latencies) * 1000 if latencies else 0,
            "mean": statistics.fmean(latencies) * 1000 if latencies else 0,
            "p50": percentile(latencies, 0.50) * 1000,
            "p95": percentile(latencies, 0.95) * 1000,
            "p99": percentile(latencies, 0.99) * 1000,
            "max": max(latencies) * 1000 if latencies else 0,
        },
        "connect_latency_ms": {
            "p50": percentile(connect_latencies, 0.50) * 1000,
            "p95": percentile(connect_latencies, 0.95) * 1000,
            "max": max(connect_latencies) * 1000 if connect_latencies else 0,
        },
        "server_rss_mib": {
            "min": min(sampler.samples_kib) / 1024 if sampler.samples_kib else None,
            "max": max(sampler.samples_kib) / 1024 if sampler.samples_kib else None,
            "last": sampler.samples_kib[-1] / 1024 if sampler.samples_kib else None,
        },
        "errors": errors,
        "worker_results": [
            {
                "worker": result.worker,
                "connect_seconds": result.connect_seconds,
                "queries": result.queries,
                "rows": result.rows,
                "batches": result.batches,
                "bytes": result.bytes,
                "error": result.error,
            }
            for result in results
        ],
    }
    rendered = json.dumps(report, indent=2, sort_keys=True)
    if args.json_output:
        args.json_output.write_text(rendered + "\n")
    print(rendered)
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
