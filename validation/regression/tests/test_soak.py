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

"""Check load evidence calculations and synthetic workload resource boundaries."""

import multiprocessing
import threading

import pytest

from soak.runner import Histogram, _host
from soak.worker import LoadWorker


def test_histogram_accounts_for_every_query() -> None:
    """Quantiles use every observation and retain approximately one-percent precision."""
    histogram = Histogram()
    for milliseconds in range(1, 101):
        histogram.add(milliseconds / 1000)
    assert histogram.count == 100
    assert 50 <= histogram.percentile(0.5) <= 50.51
    assert 95 <= histogram.percentile(0.95) <= 95.96
    assert 99 <= histogram.percentile(0.99) <= 100
    assert histogram.maximum == 0.1


@pytest.mark.parametrize("rows", [1, 511, 512, 513])
def test_workload_rows_and_payload(rows: int) -> None:
    """Generate exact ordered results across the workload batch boundary."""
    worker = LoadWorker(rows=rows, batch_rows=512, payload_bytes=3)
    with_result = worker.connect("principal").execute("QUERY")
    batches = list(with_result.batches)
    assert all(batch.num_rows <= 512 for batch in batches)
    assert [value for batch in batches for value in batch.column(0).to_pylist()] == list(range(rows))
    assert [value for batch in batches for value in batch.column(1).to_pylist()] == [b"xxx"] * rows
    with_result.close()


def test_workload_rejects_oversized_batches() -> None:
    """Prevent the load generator itself from allocating an over-budget batch."""
    with pytest.raises(ValueError, match="byte limit"):
        LoadWorker(batch_rows=4096, payload_bytes=1024)


def test_host_waits_for_diagnostics_before_shutdown_acknowledgement() -> None:
    """Keep the parent from terminating a host still writing its diagnostic report."""
    parent, child = multiprocessing.Pipe()
    finalizing = threading.Event()
    release = threading.Event()
    failures: list[BaseException] = []

    def finish() -> None:
        finalizing.set()
        assert release.wait(5)

    def run() -> None:
        try:
            _host(child, "test-token", 1, 1, 1, 1, after_close=finish)
        except BaseException as error:
            failures.append(error)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    try:
        assert parent.poll(5)
        assert parent.recv().startswith("http://127.0.0.1:")
        parent.send("stop")
        assert finalizing.wait(5)
        assert not parent.poll(0.05)
        release.set()
        assert parent.poll(5)
        assert parent.recv() == "closed"
        thread.join(5)
        assert not thread.is_alive()
        assert not failures
        assert child.closed
    finally:
        release.set()
        parent.close()
        thread.join(5)


def test_host_cleans_up_when_controller_disconnects_before_startup() -> None:
    """Close the live listener and control pipe if sending its address fails."""
    parent, child = multiprocessing.Pipe()
    parent.close()
    threads_before = set(threading.enumerate())
    with pytest.raises((BrokenPipeError, ConnectionResetError)):
        _host(child, "test-token", 1, 1, 1, 1)
    assert child.closed
    assert set(threading.enumerate()) <= threads_before
