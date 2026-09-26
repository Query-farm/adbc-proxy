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

"""Check the fixture's Grainlift wire contract in-process without a TCP listener."""

import json
from collections.abc import Iterator, Mapping
from dataclasses import dataclass

import falcon.testing
import pyarrow as pa
import pytest
from grainlift import Service

from .worker import ProbeWorker

PROTOCOL = "org.queryfarm.Grainlift.v1"


@dataclass
class Wire:
    """Send independent Arrow requests to the toolkit's WSGI app.

    Attributes:
        client: In-process Falcon client.
    """

    client: falcon.testing.TestClient

    def call(self, method: str, values: Mapping[str, str], *, token: str = "alice-token") -> falcon.testing.Result:
        """Send a unary request encoded according to the Grainlift wire contract.

        Args:
            method: Grainlift method name.
            values: Non-null string fields in wire order.
            token: Authenticated test principal credential.

        Returns:
            HTTP response including the raw Arrow body.
        """
        schema = pa.schema([pa.field(name, pa.string(), nullable=False) for name in values])
        batch = pa.record_batch([[value] for value in values.values()], schema=schema)
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, schema) as writer:
            writer.write_batch(
                batch,
                custom_metadata={
                    "vgi_rpc.method": method,
                    "vgi_rpc.protocol": PROTOCOL,
                    "vgi_rpc.protocol_version": "0.2.0",
                    "vgi_rpc.request_version": "1",
                },
            )
        return self.client.simulate_post(
            f"/{PROTOCOL}/{method}",
            body=sink.getvalue().to_pybytes(),
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/vnd.apache.arrow.stream"},
        )

    def open(self) -> str:
        """Open a connection and verify the native client's expected response schema."""
        response = self.call(
            "open_connection", {"target": "regression", "database_options_json": "[]", "connection_options_json": "[]"}
        )
        assert response.status_code == 200
        reader = pa.ipc.open_stream(response.content)
        assert reader.schema.equals(pa.schema([pa.field("session_id", pa.string(), nullable=False)]))
        return str(reader.read_next_batch().column(0)[0].as_py())


@pytest.fixture
def wire(worker: ProbeWorker) -> Iterator[Wire]:
    """Create an authenticated WSGI app without binding a socket.

    Args:
        worker: Fixture worker with observable connections.

    Yields:
        Independently encoded Grainlift requests.
    """
    with Service(worker) as service:
        yield Wire(falcon.testing.TestClient(service.app(tokens={"alice-token": "alice", "bob-token": "bob"})))
    assert all(connection.closed for connection in worker.connections)


def test_wire_session_close(wire: Wire, worker: ProbeWorker) -> None:
    """Honor the Grainlift open/close wire schema and release the worker connection."""
    sid = wire.open()
    assert len(worker.connections) == 1
    assert not worker.connections[0].closed
    response = wire.call("close_connection", {"session_id": sid})
    assert response.status_code == 200
    assert worker.connections[0].closed


@pytest.mark.parametrize("method", ["new_statement", "close_connection"])
def test_wire_principal_isolation(wire: Wire, worker: ProbeWorker, method: str) -> None:
    """Refuse another principal's session before it can create or close a child handle."""
    sid = wire.open()
    response = wire.call(method, {"session_id": sid}, token="bob-token")
    # VGI-RPC carries application errors inside a successful Arrow response.
    assert response.status_code == 200
    _, metadata = pa.ipc.open_stream(response.content).read_next_batch_with_custom_metadata()
    assert metadata[b"vgi_rpc.log_level"] == b"EXCEPTION"
    assert json.loads(metadata[b"vgi_rpc.log_message"])["status"] == "not_found"
    assert not worker.connections[0].closed


def test_wire_error_payload_is_raw_json(wire: Wire) -> None:
    """Preserve machine-readable ADBC errors without a prefixed exception name."""
    sid = wire.open()
    response = wire.call("commit", {"session_id": sid})
    assert response.status_code == 200
    _, metadata = pa.ipc.open_stream(response.content).read_next_batch_with_custom_metadata()
    assert metadata[b"vgi_rpc.log_level"] == b"EXCEPTION"
    error = json.loads(metadata[b"vgi_rpc.log_message"])
    assert error["status"] == "not_implemented"
    assert error["sqlstate"] == [48, 48, 48, 48, 48]
    assert json.loads(metadata[b"vgi_rpc.log_extra"])["exception_type"] == "AdbcError"


def test_wire_unknown_target_does_not_allocate(wire: Wire, worker: ProbeWorker) -> None:
    """Reject an unauthorized target before a worker connection is constructed."""
    response = wire.call(
        "open_connection", {"target": "missing", "database_options_json": "[]", "connection_options_json": "[]"}
    )
    assert response.status_code == 200
    _, metadata = pa.ipc.open_stream(response.content).read_next_batch_with_custom_metadata()
    assert metadata[b"vgi_rpc.log_level"] == b"EXCEPTION"
    assert json.loads(metadata[b"vgi_rpc.log_message"])["status"] == "not_found"
    assert not worker.connections
