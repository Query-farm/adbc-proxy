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

"""Run an optional Granian WSGI comparison with ordinary process supervision.

One serving process owns all sessions. The parent is Granian's supervisor;
the soak samples the serving process and its isolated backend children.
The SDK enforces decoded request, response and batch limits. This loopback
diagnostic is not an Internet-facing deployment or an HTTP hardening test.
"""

from __future__ import annotations

import hashlib
import importlib
import importlib.metadata
import json
import os
import signal
import socket
import threading
import time
import urllib.request
from functools import partial
from multiprocessing.connection import Connection as Pipe
from multiprocessing.util import Finalize
from pathlib import Path
from typing import Any

import psutil
from grainlift import IsolatedWorker, Limits, Service

from .worker import LoadWorker
from .wsgi_compat import prime


def _load(
    control: Pipe, token: str, clients: int, rows: int, batch_rows: int, payload: int, port: int, supervisor: int
) -> Any:
    worker = (
        LoadWorker(rows, batch_rows, payload)
        if os.environ.get("GRAINLIFT_DIAGNOSTIC_WORKER", "isolated") == "direct"
        else IsolatedWorker(
            "soak.worker:LoadWorker",
            timeout_seconds=5,
            startup_timeout_seconds=15,
            worker_options={"rows": rows, "batch_rows": batch_rows, "payload_bytes": payload},
        )
    )
    service = Service(worker, limits=Limits(sessions=clients + 2, idle_seconds=10))
    started, cpu = time.perf_counter(), time.process_time()

    def finish() -> None:
        service.close()
        report = {
            "http_host": "granian",
            "granian_version": importlib.metadata.version("granian"),
            "worker": os.environ.get("GRAINLIFT_DIAGNOSTIC_WORKER", "isolated"),
            "host_timings": False,
            "primed_wsgi_response": True,
            "workers": 1,
            "blocking_threads": max(8, clients * 2),
            "runtime_threads": 1,
            "backpressure": clients * 4 + 16,
            "host_elapsed_seconds": time.perf_counter() - started,
            "host_cpu_seconds": time.process_time() - cpu,
            "supervisor_rss_bytes_at_shutdown": psutil.Process(supervisor).memory_info().rss,
            "remaining_children": len(psutil.Process().children(recursive=True)),
            "diagnostic_source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        }
        Path(os.environ["GRAINLIFT_DIAGNOSTIC_OUTPUT"]).write_text(json.dumps(report, indent=2) + "\n")

    # Multiprocessing workers do not execute ordinary atexit handlers.
    Finalize(None, finish, exitpriority=10)

    def controller() -> None:
        endpoint = f"http://127.0.0.1:{port}"
        try:
            # A bounded startup probe ensures the Rust listener is serving before
            # the baseline measurement. This is not part of the query workload.
            deadline = time.monotonic() + 15
            while True:
                try:
                    request = urllib.request.Request(endpoint, method="OPTIONS")
                    with urllib.request.urlopen(request, timeout=1):
                        break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("Granian listener did not start") from None
                    time.sleep(0.05)
            control.send(
                {
                    "endpoint": endpoint,
                    "sample_pid": os.getpid(),
                    "transport": "authenticated loopback HTTP, native ADBC C ABI, Granian WSGI, "
                    + os.environ.get("GRAINLIFT_DIAGNOSTIC_WORKER", "isolated")
                    + " worker processes",
                }
            )
            control.recv()
        except (EOFError, BrokenPipeError, ConnectionResetError):
            pass
        finally:
            os.kill(supervisor, signal.SIGTERM)

    threading.Thread(target=controller, daemon=True).start()
    return partial(prime, service.app(tokens={token: "load-principal"}))


def serve(control: Pipe, token: str, clients: int, rows: int, batch_rows: int, payload: int) -> None:
    """Serve the benchmark application through Granian's public server API.

    Args:
        control: Trusted local readiness and shutdown pipe.
        token: Ephemeral authentication token, never logged.
        clients: Number of independent benchmark clients.
        rows: Result rows per query.
        batch_rows: Maximum rows per result batch.
        payload: Payload bytes per row.
    """
    granian = importlib.import_module("granian")
    constants = importlib.import_module("granian.constants")
    http = importlib.import_module("granian.http")
    # Granian's public API does not expose an ephemeral bound port. Reserve a
    # loopback port briefly, then release it for its supervisor to bind. Any
    # intervening collision fails startup; it never selects another destination.
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    server = granian.Granian(
        "soak.granian_host",
        address="127.0.0.1",
        port=port,
        interface=constants.Interfaces.WSGI,
        workers=1,
        blocking_threads=max(8, clients * 2),
        runtime_threads=1,
        backpressure=clients * 4 + 16,
        http=constants.HTTPModes.http1,
        http1_settings=http.HTTP1Settings(header_read_timeout=10000, max_buffer_size=1024 * 1024),
        log_enabled=False,
        log_access=False,
        workers_kill_timeout=10,
    )
    server.serve(
        target_loader=partial(_load, control, token, clients, rows, batch_rows, payload, port, os.getpid()),
        wrap_loader=False,
    )
