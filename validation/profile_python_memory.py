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

"""Add bounded GC/tracemalloc diagnostics to the native Python-worker soak host.

Run with validation/regression on PYTHONPATH and GRAINLIFT_MEMORY_PROFILE set to
an output JSON path. Remaining arguments are the ordinary soak CLI arguments.
Set GRAINLIFT_TRACE_ALLOCATIONS=1 to add expensive allocation tracing. Profiling
changes latency and allocator behavior; this is not a capacity run.
Only allocation locations and aggregate counts are retained, never object values.
"""

from __future__ import annotations

import gc
import json
import os
import threading
import time
import tracemalloc
from collections import Counter
from collections.abc import Callable
from multiprocessing.connection import Connection as Pipe
from pathlib import Path

import psutil
import pyarrow as pa

from soak import runner

_ordinary_host = runner._host


def _profile_host(
    control: Pipe,
    token: str,
    clients: int,
    rows: int,
    batch_rows: int,
    payload: int,
    after_close: Callable[[], None] | None = None,
) -> None:
    trace_allocations = os.environ.get("GRAINLIFT_TRACE_ALLOCATIONS") == "1"
    if trace_allocations:
        tracemalloc.start(1)
    stopped = threading.Event()
    reports: list[dict[str, object]] = []
    started = time.monotonic()
    baseline = tracemalloc.take_snapshot() if trace_allocations else None

    def sample() -> None:
        collected = gc.collect()
        counts = Counter(f"{type(value).__module__}.{type(value).__qualname__}" for value in gc.get_objects())
        current, peak = tracemalloc.get_traced_memory() if trace_allocations else (0, 0)
        growth = tracemalloc.take_snapshot().compare_to(baseline, "filename")[:20] if baseline is not None else []
        reports.append(
            {
                "seconds": time.monotonic() - started,
                "rss_bytes": psutil.Process().memory_info().rss,
                "arrow_allocated_bytes": pa.total_allocated_bytes(),
                "python_traced_bytes": current,
                "python_peak_traced_bytes": peak,
                "tracemalloc_overhead_bytes": tracemalloc.get_tracemalloc_memory(),
                "gc_collected": collected,
                "tracked_objects": sum(counts.values()),
                "top_tracked_types": dict(counts.most_common(20)),
                "live_threads": len(threading.enumerate()),
                "allocation_growth": [
                    {
                        "file": stat.traceback[0].filename,
                        "bytes": stat.size_diff,
                        "allocations": stat.count_diff,
                    }
                    for stat in growth
                ],
            }
        )

    def monitor() -> None:
        sample()
        while not stopped.wait(30) and len(reports) < 122:
            sample()

    thread = threading.Thread(target=monitor, daemon=True)
    thread.start()
    finalized = False

    def finish() -> None:
        nonlocal finalized
        if finalized:
            return
        finalized = True
        stopped.set()
        thread.join(timeout=15)
        if thread.is_alive():
            raise RuntimeError("Memory diagnostic monitor failed to stop")
        sample()
        destination = Path(os.environ["GRAINLIFT_MEMORY_PROFILE"])
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(
            json.dumps({"profiling": True, "tracemalloc_enabled": trace_allocations, "samples": reports}, indent=2)
            + "\n"
        )
        if after_close is not None:
            after_close()

    try:
        _ordinary_host(control, token, clients, rows, batch_rows, payload, after_close=finish)
    finally:
        finish()


def main() -> None:
    """Run the bounded soak with an instrumented host and normal native clients."""
    if "GRAINLIFT_MEMORY_PROFILE" not in os.environ:
        raise SystemExit("Set GRAINLIFT_MEMORY_PROFILE to the diagnostic report path")
    runner._host = _profile_host
    runner.main()


if __name__ == "__main__":
    main()
