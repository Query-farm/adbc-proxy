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

"""Measure the synthetic worker and isolated pipe without HTTP or the C ABI.

This is a single-client microbenchmark, not an equivalent service load test.
It preserves bounded batch generation, deadlines, cleanup and value checks.
"""

import argparse
import json
import platform
import time
from contextlib import closing
from pathlib import Path

import psutil
from grainlift import IsolatedWorker, Worker

from .runner import Histogram
from .worker import LoadWorker


def main() -> None:
    """Measure warmed statement execution and verified batch consumption."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--isolated", action="store_true")
    parser.add_argument("--queries", type=int, default=500)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.queries <= 10000:
        parser.error("queries must be 1..10000")
    worker: Worker = (
        IsolatedWorker("soak.worker:LoadWorker", timeout_seconds=5, startup_timeout_seconds=15)
        if args.isolated
        else LoadWorker()
    )
    histogram = Histogram()
    total = 0.0
    with closing(worker.connect("load-principal")) as connection, closing(connection.new_statement()) as statement:
        for iteration in range(args.queries + 10):
            started = time.perf_counter()
            statement.set_sql_query("QUERY")
            with closing(statement.execute()) as result:
                count = 0
                for batch in result.batches:
                    if batch.column(0).to_pylist() != list(range(count, count + batch.num_rows)):
                        raise AssertionError("Invalid row sequence")
                    if batch.column(1).to_pylist() != [b"x" * 64] * batch.num_rows:
                        raise AssertionError("Invalid payload")
                    count += batch.num_rows
                if count != 4096:
                    raise AssertionError("Invalid row count")
            elapsed = time.perf_counter() - started
            if iteration >= 10:
                histogram.add(elapsed)
                total += elapsed
    report = {
        "python": platform.python_version(),
        "isolated": args.isolated,
        "queries": histogram.count,
        "warmup_queries": 10,
        "rows_per_query": 4096,
        "batch_rows": 512,
        "payload_bytes": 64,
        "mean_ms": 1000 * total / histogram.count,
        "p50_ms": histogram.percentile(0.5),
        "p99_ms": histogram.percentile(0.99),
        "remaining_children": len(psutil.Process().children()),
        "resource_tracker_children": sum(
            any("multiprocessing.resource_tracker" in argument for argument in child.cmdline())
            for child in psutil.Process().children()
        ),
    }
    args.output.write_text(json.dumps(report, indent=2) + "\n")


if __name__ == "__main__":
    main()
