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

import importlib
import multiprocessing
import threading
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import psutil
import pytest

from soak.runner import Histogram, _host, _sample
from soak.worker import LoadWorker


@pytest.mark.parametrize("host_kind", ["soak", "tls_edge"])
def test_hosts_keep_waitress_poll_timeout_positive(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, host_kind: str
) -> None:
    """Check real Waitress coercion and shutdown for both configured test hosts."""
    waitress = importlib.import_module("waitress.server")
    create_server = waitress.create_server
    parsed_timeouts: list[int] = []

    def capture_server(*args: Any, **kwargs: Any) -> Any:
        server = create_server(*args, **kwargs)
        parsed_timeouts.append(server.adj.asyncore_loop_timeout)
        return server

    monkeypatch.setattr(waitress, "create_server", capture_server)
    parent, child = multiprocessing.Pipe()
    failures: list[BaseException] = []

    def run() -> None:
        try:
            if host_kind == "soak":
                _host(child, "test-token", 1, 1, 1, 1)
            else:
                from deployment.tls_edge import _host as edge_host

                edge_host(child, str(tmp_path), "test-token")
        except BaseException as error:
            failures.append(error)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    try:
        assert parent.poll(5), failures
        parent.recv()
        parent.send("stop")
        assert parent.poll(5), failures
        parent.recv()
        thread.join(5)
        assert not thread.is_alive()
        assert not failures
        assert len(parsed_timeouts) == 1
        assert parsed_timeouts[0] >= 1
    finally:
        parent.close()
        thread.join(5)


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


@pytest.mark.parametrize("failure", [psutil.NoSuchProcess(123), psutil.AccessDenied(123)])
def test_sampling_survives_worker_exit_without_partial_totals(failure: psutil.Error) -> None:
    """Retain usable child totals and flag denied measurements during worker churn."""
    host = Mock(spec=psutil.Process)
    gone = Mock(spec=psutil.Process)
    live = Mock(spec=psutil.Process)
    host.children.return_value = [gone, live]
    host.memory_info.return_value.rss = 1000
    host.num_fds.return_value = 12
    gone.memory_info.return_value.rss = 9999
    gone.num_fds.side_effect = failure
    live.memory_info.return_value.rss = 100
    live.num_fds.return_value = 3

    sample = _sample(host, 1.5)

    assert sample["server_rss_bytes"] == 1000
    assert sample["descendant_rss_bytes"] == 100
    assert sample["descendant_descriptors"] == 3
    assert sample["unreadable_descendants"] == int(isinstance(failure, psutil.AccessDenied))


def test_sampling_does_not_hide_host_permission_failure() -> None:
    """A missing host measurement is not treated as normal child retirement."""
    host = Mock(spec=psutil.Process)
    host.children.return_value = []
    host.memory_info.side_effect = psutil.AccessDenied(123)
    with pytest.raises(psutil.AccessDenied):
        _sample(host, 0)


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
