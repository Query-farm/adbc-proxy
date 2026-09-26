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

"""Run a bounded native C-ABI workload against a separate Waitress service process."""

from __future__ import annotations

import argparse
import hashlib
import importlib
import importlib.metadata
import json
import math
import multiprocessing
import platform
import secrets
import sys
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from contextlib import suppress
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from multiprocessing.connection import Connection as Pipe
from pathlib import Path
from typing import Any, cast

import adbc_driver_manager as manager
import adbc_driver_manager.dbapi as adbc
import psutil
from grainlift import IsolatedWorker, Limits, Service


@dataclass
class Histogram:
    """Keep all-query latency quantiles in a fixed-size logarithmic histogram.

    Attributes:
        buckets: Counts in approximately one-percent-wide latency buckets.
        count: Total observations.
        maximum: Largest observed duration in seconds.
    """

    buckets: list[int] = field(default_factory=lambda: [0] * 2048)
    count: int = 0
    maximum: float = 0

    def add(self, seconds: float) -> None:
        """Record one duration without retaining a per-query sample.

        Args:
            seconds: Nonnegative query duration.
        """
        index = min(2047, max(0, int(math.log1p(seconds * 1_000_000) / math.log(1.01))))
        self.buckets[index] += 1
        self.count += 1
        self.maximum = max(self.maximum, seconds)

    def percentile(self, fraction: float) -> float:
        """Return the upper edge of a percentile bucket in milliseconds.

        Args:
            fraction: Quantile between zero and one.

        Returns:
            Approximate latency, zero for an empty histogram.
        """
        if not self.count:
            return 0
        target = math.ceil(self.count * fraction)
        total = 0
        for index, count in enumerate(self.buckets):
            total += count
            if total >= target:
                return math.expm1((index + 1) * math.log(1.01)) / 1000
        return self.maximum * 1000


@dataclass
class Outcome:
    """Aggregate progress for one independent ADBC client.

    Attributes:
        queries: Successfully verified queries.
        rows: Verified rows.
        connections: Completed connection lifecycles.
        expected_errors: Intentional structured failures observed.
        errors: Unexpected errors counted by class name, without messages.
        latency: All-query fixed-memory histogram.
        maximum_gap_seconds: Longest interval between query completions.
    """

    queries: int = 0
    rows: int = 0
    connections: int = 0
    expected_errors: int = 0
    errors: dict[str, int] = field(default_factory=dict)
    latency: Histogram = field(default_factory=Histogram)
    maximum_gap_seconds: float = 0


def _host(
    control: Pipe,
    token: str,
    clients: int,
    rows: int,
    batch_rows: int,
    payload: int,
    after_close: Callable[[], None] | None = None,
) -> None:
    try:
        _serve(control, token, clients, rows, batch_rows, payload)
        if after_close is not None:
            after_close()
        control.send("closed")
    finally:
        control.close()


def _serve(control: Pipe, token: str, clients: int, rows: int, batch_rows: int, payload: int) -> None:
    worker = IsolatedWorker(
        "soak.worker:LoadWorker",
        timeout_seconds=5,
        startup_timeout_seconds=15,
        worker_options={"rows": rows, "batch_rows": batch_rows, "payload_bytes": payload},
    )
    # Waitress is an untyped external WSGI host. Its live listener is deliberately
    # test-owned; control messages are local, trusted multiprocessing traffic.
    waitress = importlib.import_module("waitress.server")
    asyncore = importlib.import_module("waitress.wasyncore")
    with Service(worker, limits=Limits(sessions=clients + 2, idle_seconds=10)) as service:
        server = waitress.create_server(
            service.app(tokens={token: "load-principal"}),
            host="127.0.0.1",
            port=0,
            threads=max(8, clients * 2),
            connection_limit=clients * 4 + 16,
            # Waitress rejects >= while the SDK's decoded-body limit is inclusive.
            max_request_body_size=2 * 1024 * 1024 + 1,
            channel_timeout=10,
            asyncore_loop_timeout=0.1,
            inbuf_overflow=1024 * 1024,
            outbuf_overflow=1024 * 1024,
        )
        listener = threading.Thread(target=server.run)
        listener.start()
        try:
            control.send(f"http://127.0.0.1:{server.effective_port}")
            control.recv()
        finally:
            server.close()
            server.task_dispatcher.shutdown(timeout=10)
            asyncore.close_all(map=server._map)
            listener.join(timeout=5)
            if listener.is_alive():
                raise RuntimeError("Waitress listener did not stop")


def _client(
    endpoint: str,
    token: str,
    driver: Path,
    deadline: float,
    barrier: threading.Barrier,
    rows: int,
    churn: int,
    payload: int,
) -> Outcome:
    outcome = Outcome()
    barrier.wait(timeout=30)
    last_completed = time.monotonic()
    while time.monotonic() < deadline:
        try:
            with (
                adbc.connect(
                    driver=driver,
                    entrypoint="AdbcDriverGrainliftInit",
                    db_kwargs={
                        "grainlift.uri": endpoint,
                        "grainlift.target": "default",
                        "grainlift.auth.bearer_token": token,
                    },
                    autocommit=True,
                ) as connection,
                connection.cursor() as cursor,
            ):
                for iteration in range(churn):
                    if time.monotonic() >= deadline:
                        break
                    if iteration % 10 == 0:
                        try:
                            cursor.execute("FAIL")
                        except manager.DataError as error:
                            if error.sqlstate != "22000":
                                raise AssertionError("Unexpected failure SQLSTATE") from None
                            outcome.expected_errors += 1
                        else:
                            raise AssertionError("Injected failure was not observed")
                    started = time.monotonic()
                    cursor.execute("QUERY")
                    count = 0
                    with cursor.fetch_record_batch() as reader:
                        for batch in reader:
                            values = batch.column(0).to_pylist()
                            if values != list(range(count, count + batch.num_rows)):
                                raise AssertionError("Invalid row sequence")
                            if batch.column(1).to_pylist() != [b"x" * payload] * batch.num_rows:
                                raise AssertionError("Invalid payload values")
                            count += batch.num_rows
                    if count != rows:
                        raise AssertionError("Invalid row count")
                    completed = time.monotonic()
                    outcome.latency.add(completed - started)
                    outcome.maximum_gap_seconds = max(outcome.maximum_gap_seconds, completed - last_completed)
                    last_completed = completed
                    outcome.queries += 1
                    outcome.rows += count
            outcome.connections += 1
        except Exception as error:
            # Reports intentionally contain only exception class names.
            kind = type(error).__name__
            outcome.errors[kind] = outcome.errors.get(kind, 0) + 1
            break
    return outcome


def _sample(process: psutil.Process, elapsed: float) -> dict[str, int | float]:
    children = process.children(recursive=True)
    child_rss = 0
    child_fds = 0
    unreadable_children = 0
    for child in children:
        try:
            rss = child.memory_info().rss
            descriptors = _descriptors(child)
        except psutil.NoSuchProcess:
            continue
        except psutil.AccessDenied:
            # Linux procfs can deny access while a worker exits. Keep the
            # workload running, but mark this sample's totals as incomplete.
            unreadable_children += 1
            continue
        child_rss += rss
        child_fds += descriptors
    return {
        "seconds": round(elapsed, 3),
        "server_rss_bytes": process.memory_info().rss,
        "descendant_rss_bytes": child_rss,
        "descendants": len(children),
        "server_descriptors": _descriptors(process),
        "descendant_descriptors": child_fds,
        "unreadable_descendants": unreadable_children,
        "client_rss_bytes": psutil.Process().memory_info().rss,
    }


def _descriptors(process: psutil.Process) -> int:
    attribute = "num_handles" if sys.platform == "win32" else "num_fds"
    return int(getattr(process, attribute)())


def main() -> None:
    """Run a bounded local soak and write machine-readable evidence."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--driver", required=True, type=Path)
    parser.add_argument("--seconds", type=int, default=180)
    parser.add_argument("--clients", type=int, default=8)
    parser.add_argument("--rows", type=int, default=4096)
    parser.add_argument("--batch-rows", type=int, default=512)
    parser.add_argument("--payload-bytes", type=int, default=64)
    parser.add_argument("--churn", type=int, default=25)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if not 1 <= args.seconds <= 3600 or not 1 <= args.clients <= 32 or not 1 <= args.churn <= 10000:
        parser.error("seconds must be 1..3600, clients 1..32, churn 1..10000")
    from .worker import LoadWorker

    LoadWorker(args.rows, args.batch_rows, args.payload_bytes)
    driver = args.driver.resolve(strict=True)
    sdk_file = importlib.import_module("grainlift").__file__
    assert sdk_file is not None
    sdk = Path(sdk_file).parent
    sdk_hashes = {
        str(source.relative_to(sdk)): hashlib.sha256(source.read_bytes()).hexdigest() for source in sdk.glob("*.py")
    }
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    token = secrets.token_urlsafe(32)
    host = context.Process(
        target=_host,
        args=(child, token, args.clients, args.rows, args.batch_rows, args.payload_bytes),
    )
    host.start()
    child.close()
    report: dict[str, Any] = {}
    try:
        if not parent.poll(30):
            raise RuntimeError("Load host failed to start")
        endpoint = cast(str, parent.recv())
        assert host.pid is not None
        process = psutil.Process(host.pid)
        baseline = _sample(process, 0)
        samples: list[dict[str, int | float]] = [baseline]
        started = time.monotonic()
        deadline = started + args.seconds
        barrier = threading.Barrier(args.clients)
        with ThreadPoolExecutor(args.clients) as pool:
            futures = [
                pool.submit(
                    _client, endpoint, token, driver, deadline, barrier, args.rows, args.churn, args.payload_bytes
                )
                for _ in range(args.clients)
            ]
            while not all(future.done() for future in futures):
                samples.append(_sample(process, time.monotonic() - started))
                time.sleep(1)
            outcomes = [future.result() for future in futures]
        elapsed = time.monotonic() - started
        # Allow idle HTTP channels to expire before comparing parent descriptors.
        time.sleep(12)
        recovery = _sample(process, time.monotonic() - started)
        combined = Histogram()
        for outcome in outcomes:
            combined.count += outcome.latency.count
            combined.maximum = max(combined.maximum, outcome.latency.maximum)
            for index, count in enumerate(outcome.latency.buckets):
                combined.buckets[index] += count
        report = {
            "recorded_utc": datetime.now(UTC).isoformat(),
            "environment": {
                "platform": platform.platform(),
                "python": platform.python_version(),
                "driver_sha256": hashlib.sha256(driver.read_bytes()).hexdigest(),
                "sdk_source_sha256": sdk_hashes,
                "packages": {
                    name: importlib.metadata.version(name)
                    for name in ("grainlift-python", "vgi-rpc", "pyarrow", "adbc-driver-manager", "waitress", "psutil")
                },
            },
            "workload": {
                "transport": "authenticated loopback HTTP, native ADBC C ABI, Waitress, isolated worker processes",
                "clients": args.clients,
                "requested_seconds": args.seconds,
                "rows_per_query": args.rows,
                "batch_rows": args.batch_rows,
                "payload_bytes_per_row": args.payload_bytes,
                "queries_per_connection": args.churn,
                "expected_error_every_n_queries_per_connection": 10,
                "elapsed_seconds": elapsed,
            },
            "queries": combined.count,
            "queries_per_second": combined.count / elapsed,
            "rows_per_second": sum(outcome.rows for outcome in outcomes) / elapsed,
            "latency_ms": {
                "p50": combined.percentile(0.5),
                "p95": combined.percentile(0.95),
                "p99": combined.percentile(0.99),
                "maximum": combined.maximum * 1000,
                "quantile_method": "fixed logarithmic histogram, upper bucket bound, about 1% resolution",
            },
            "clients": [
                {key: value for key, value in asdict(outcome).items() if key != "latency"} for outcome in outcomes
            ],
            "baseline": baseline,
            "samples": samples,
            "recovery": recovery,
            "peak_server_rss_bytes": max(sample["server_rss_bytes"] for sample in samples),
            "peak_descendant_rss_bytes": max(sample["descendant_rss_bytes"] for sample in samples),
            "unexpected_errors": sum(sum(outcome.errors.values()) for outcome in outcomes),
            "fairness_jain_index": (
                combined.count**2 / (args.clients * sum(outcome.queries**2 for outcome in outcomes))
                if combined.count
                else 0
            ),
            "peak_client_rss_bytes": max(sample["client_rss_bytes"] for sample in samples),
            "incomplete_descendant_samples": sum(sample["unreadable_descendants"] > 0 for sample in samples),
        }
        steady = [sample for sample in samples if sample["seconds"] >= min(20, args.seconds / 3)]
        if steady:
            report["steady_server_rss_growth_bytes"] = steady[-1]["server_rss_bytes"] - steady[0]["server_rss_bytes"]
            report["steady_client_rss_growth_bytes"] = steady[-1]["client_rss_bytes"] - steady[0]["client_rss_bytes"]
        report["sdk_sources_unchanged"] = all(
            hashlib.sha256((sdk / name).read_bytes()).hexdigest() == fingerprint
            for name, fingerprint in sdk_hashes.items()
        )
        parent.send("stop")
        if not parent.poll(20) or parent.recv() != "closed":
            raise RuntimeError("Load host failed graceful shutdown")
        host.join(timeout=5)
        report["host_exit_code"] = host.exitcode
        report["passed"] = (
            report["unexpected_errors"] == 0
            and all(outcome.queries > 0 for outcome in outcomes)
            and host.exitcode == 0
            and recovery["descendants"] == 0
            and recovery["server_descriptors"] <= baseline["server_descriptors"] + 4
            and report["fairness_jain_index"] >= 0.8
            and report["sdk_sources_unchanged"]
        )
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")
        print(
            json.dumps({key: report[key] for key in ("passed", "queries", "unexpected_errors", "queries_per_second")})
        )
        if not report["passed"]:
            raise SystemExit(1)
    finally:
        parent.close()
        descendants = []
        if host.is_alive() and host.pid is not None:
            with suppress(psutil.NoSuchProcess):
                descendants = psutil.Process(host.pid).children(recursive=True)
        if host.is_alive():
            host.terminate()
            host.join(timeout=5)
        if host.is_alive():
            host.kill()
            host.join(timeout=5)
        for descendant in descendants:
            with suppress(psutil.NoSuchProcess):
                descendant.kill()
        psutil.wait_procs(descendants, timeout=5)
        host.close()


if __name__ == "__main__":
    main()
