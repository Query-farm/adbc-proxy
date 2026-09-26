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

"""Native-driver fixtures with isolated listeners and deterministic cleanup."""

from __future__ import annotations

import os
import sys
import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from socketserver import ThreadingMixIn
from typing import cast
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server

import adbc_driver_manager.dbapi as adbc
import pytest
from grainlift import Limits, Service

from .worker import ProbeWorker


class QuietHandler(WSGIRequestHandler):
    """Avoid writing request details to the test process's logs."""

    def log_message(self, format: str, *args: object) -> None:
        """Suppress HTTP access messages."""


class ThreadedServer(ThreadingMixIn, WSGIServer):
    """Handle independent HTTP connections concurrently with joined shutdown."""

    daemon_threads = False
    block_on_close = True


@dataclass
class Harness:
    """Describe an authenticated test server and its observations.

    Attributes:
        endpoint: Loopback HTTP endpoint allocated by the operating system.
        driver: Native Grainlift driver shared library.
        worker: Test-owned results and lifecycle probes.
    """

    endpoint: str
    driver: Path
    worker: ProbeWorker

    @contextmanager
    def connect(
        self,
        *,
        token: str = "alice-token",
        target: str = "regression",
        options: Mapping[str, str | int] | None = None,
    ) -> Iterator[adbc.Connection]:
        """Open an ordinary ADBC connection, always closing it on exit.

        Args:
            token: Test credential identifying the caller.
            target: Server-configured target to request.
            options: Additional native client or downstream database options.

        Yields:
            An ADBC DB-API connection with autocommit enabled.
        """
        db_options: dict[str, str | int] = {
            "grainlift.uri": self.endpoint,
            "grainlift.target": target,
            "grainlift.auth.bearer_token": token,
        }
        db_options.update(options or {})
        with adbc.connect(
            driver=self.driver,
            entrypoint="AdbcDriverGrainliftInit",
            # The pinned manager forwards values to AdbcDatabase, which accepts
            # integer options; its DB-API annotation is narrower than that API.
            db_kwargs=cast(Mapping[str, str | Path], db_options),
            autocommit=True,
        ) as connection:
            yield connection


@pytest.fixture(scope="session")
def driver_path() -> Path:
    """Find a built native driver; a missing library is a failure, not a skip."""
    root = Path(__file__).resolve().parents[3]
    filename = {
        "darwin": "libadbc_driver_grainlift.dylib",
        "win32": "adbc_driver_grainlift.dll",
    }.get(sys.platform, "libadbc_driver_grainlift.so")
    configured = os.environ.get("GRAINLIFT_DRIVER")
    path = Path(configured).expanduser() if configured else root / "target" / "debug" / filename
    if not path.is_file():
        pytest.fail("Build cargo build -p adbc-driver-grainlift or set GRAINLIFT_DRIVER")
    return path.resolve()


@pytest.fixture
def worker() -> ProbeWorker:
    """Create an isolated fixture worker for each test."""
    return ProbeWorker()


@pytest.fixture
def limits(request: pytest.FixtureRequest) -> Limits:
    """Use finite limits, accepting indirect parameters from boundary tests.

    Args:
        request: Pytest request carrying an optional Limits parameter.

    Returns:
        Selected resource limits.
    """
    return cast(Limits, getattr(request, "param", Limits()))


@pytest.fixture
def harness(driver_path: Path, worker: ProbeWorker, limits: Limits) -> Iterator[Harness]:
    """Run a toolkit service through an actual loopback HTTP listener.

    Args:
        driver_path: Compiled Grainlift client shared library.
        worker: Isolated plans and lifecycle observations.
        limits: Toolkit resource limits for this test.

    Yields:
        Server endpoint and standard ADBC connection factory.
    """
    with Service(worker, limits=limits) as service:
        server = make_server(
            "127.0.0.1",
            0,
            service.app(tokens={"alice-token": "alice", "bob-token": "bob"}),
            server_class=ThreadedServer,
            handler_class=QuietHandler,
        )
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
        thread.start()
        try:
            yield Harness(f"http://127.0.0.1:{server.server_port}", driver_path, worker)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
            assert not thread.is_alive(), "Test HTTP server failed to stop"
    assert all(connection.closed for connection in worker.connections)
    assert all(reader.closed for connection in worker.connections for reader in connection.readers)
