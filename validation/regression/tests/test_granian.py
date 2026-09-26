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

"""Check Granian's optional diagnostic host and lazy WSGI compatibility."""

import json
import multiprocessing
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar
from http.client import HTTPConnection
from pathlib import Path
from typing import Any
from unittest.mock import Mock
from urllib.parse import urlsplit
from wsgiref.types import StartResponse

import pytest

from soak.diagnose import _host
from soak.wsgi_compat import PrimedResponse


@pytest.mark.parametrize("consume", [False, True])
def test_priming_bounds_prefetch_and_preserves_context(consume: bool) -> None:
    """Preserve headers, bytes and cleanup even if another thread closes early."""
    private = ContextVar("test_private", default=False)
    pulled: list[int] = []
    closed: list[bool] = []

    def app(environ: dict[str, Any], start: StartResponse) -> Iterator[bytes]:
        token = private.set(True)
        try:
            start("401 Unauthorized", [("Content-Type", "text/plain")])
            for index in range(3):
                assert private.get()
                pulled.append(index)
                yield bytes([index])
        finally:
            private.reset(token)
            closed.append(True)

    start = Mock()
    response = PrimedResponse(app, {}, start)
    start.assert_called_once_with("401 Unauthorized", [("Content-Type", "text/plain")])
    assert pulled == [0]
    assert not private.get()
    with ThreadPoolExecutor(1) as pool:
        if consume:
            assert pool.submit(list, response).result() == [b"\x00", b"\x01", b"\x02"]
        pool.submit(response.close).result()
    response.close()
    assert closed == [True]
    assert pulled == ([0, 1, 2] if consume else [0])
    assert not private.get()


def test_priming_failure_closes_response() -> None:
    """A failure before the first chunk must close the original iterable."""
    source = Mock()
    source.__iter__ = Mock(return_value=source)
    source.__next__ = Mock(side_effect=RuntimeError("failed first chunk"))
    with pytest.raises(RuntimeError, match="failed first chunk"):
        PrimedResponse(Mock(return_value=source), {}, Mock())
    source.close.assert_called_once()


def test_granian_authentication_limits_and_shutdown(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Exercise a real listener, rejection boundaries and supervised cleanup."""
    pytest.importorskip("granian")
    output = tmp_path / "host.json"
    monkeypatch.setenv("GRAINLIFT_DIAGNOSTIC_HTTP", "granian")
    monkeypatch.setenv("GRAINLIFT_DIAGNOSTIC_TIMINGS", "off")
    monkeypatch.setenv("GRAINLIFT_DIAGNOSTIC_OUTPUT", str(output))
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    host = context.Process(target=_host, args=(child, "test-token", 1, 1, 1, 1))
    host.start()
    child.close()
    try:
        assert parent.poll(20)
        ready = parent.recv()
        assert ready["sample_pid"] != host.pid
        client = HTTPConnection(urlsplit(ready["endpoint"]).netloc, timeout=5)
        try:
            client.request("POST", "/", body=b"invalid")
            response = client.getresponse()
            assert response.status == 401
            response.read()
            headers = {"Authorization": "Bearer test-token", "Content-Type": "application/vnd.apache.arrow.stream"}
            for size in (2 * 1024 * 1024 - 1, 2 * 1024 * 1024, 2 * 1024 * 1024 + 1):
                client.request("POST", "/org.queryfarm.Grainlift.v1/open_connection", body=b"x" * size, headers=headers)
                response = client.getresponse()
                assert response.status == (413 if size > 2 * 1024 * 1024 else 400)
                response.read()
            client.request("OPTIONS", "/")
            response = client.getresponse()
            assert response.status == 200
            response.read()
        finally:
            client.close()
        parent.send("stop")
        assert parent.poll(20)
        assert parent.recv() == "closed"
        host.join(5)
        assert host.exitcode == 0
        report = json.loads(output.read_text())
        assert report["remaining_children"] == 0
        assert report["primed_wsgi_response"]
    finally:
        parent.close()
        if host.is_alive():
            host.terminate()
            host.join(15)
        if host.is_alive():
            host.kill()
            host.join(5)
        host.close()
