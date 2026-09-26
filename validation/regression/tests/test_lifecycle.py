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

"""Independent connections, statement reuse, resource limits, and request timeout."""

import time
from concurrent.futures import ThreadPoolExecutor

import adbc_driver_manager as manager
import pyarrow as pa
import pytest
from grainlift import Limits

from .conftest import Harness
from .worker import Plan

pytestmark = pytest.mark.native


def test_independent_concurrent_connections(harness: Harness) -> None:
    """Use separate ADBC handles per thread and release every worker connection."""
    schema = pa.schema([("n", pa.int64())])
    harness.worker.plans["SELECT concurrent"] = Plan(schema, (pa.record_batch([[42]], schema=schema),))

    def query(index: int) -> list[int]:
        with (
            harness.connect(token="alice-token" if index % 2 else "bob-token") as connection,
            connection.cursor() as cursor,
        ):
            cursor.execute("SELECT concurrent")
            return [int(value) for value in cursor.fetch_arrow_table().column(0).to_pylist()]

    with ThreadPoolExecutor(max_workers=4) as pool:
        assert list(pool.map(query, range(8))) == [[42]] * 8
    assert len(harness.worker.connections) == 8
    assert {connection.principal for connection in harness.worker.connections} == {"alice", "bob"}
    assert all(connection.closed for connection in harness.worker.connections)


def test_statement_reuse_releases_previous_result(harness: Harness) -> None:
    """Reexecution closes the prior server cursor even when it was not drained."""
    schema = pa.schema([("n", pa.int64())])
    harness.worker.plans["SELECT reuse"] = Plan(
        schema,
        tuple(pa.record_batch([[index]], schema=schema) for index in range(3)),
    )
    with harness.connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT reuse")
        # Import the Arrow stream before DB-API releases it. The manager's raw
        # handle.release() holds the GIL, starving this in-process Python server.
        previous = cursor.fetch_record_batch()
        first = harness.worker.connections[0].readers[0]
        cursor.execute("SELECT reuse")
        assert first.closed
        previous.close()
        assert cursor.fetch_arrow_table().column(0).to_pylist() == [0, 1, 2]


@pytest.mark.parametrize("limits", [Limits(batch_bytes=2048)], indirect=True)
@pytest.mark.parametrize("size", [2047, 2048, 2049])
def test_result_batch_limit(harness: Harness, size: int) -> None:
    """Exercise immediately below, at, and above the toolkit's Arrow buffer limit."""
    schema = pa.schema([("bytes", pa.binary(1))])
    batch = pa.record_batch([[b"x"] * size], schema=schema)
    assert batch.get_total_buffer_size() == size
    harness.worker.plans["SELECT boundary"] = Plan(schema, (batch,))
    with harness.connect() as connection, connection.cursor() as cursor:
        if size > 2048:
            with pytest.raises(manager.Error):
                cursor.execute("SELECT boundary")
            assert harness.worker.connections[0].readers[0].closed
        else:
            cursor.execute("SELECT boundary")
            assert cursor.fetch_arrow_table().num_rows == size


@pytest.mark.parametrize("limits", [Limits(sql_bytes=32)], indirect=True)
@pytest.mark.parametrize("size", [31, 32, 33])
def test_query_size_limit(harness: Harness, size: int) -> None:
    """Reject oversized query text without creating a result cursor."""
    schema = pa.schema([("n", pa.int64())])
    query = "SELECT " + "x" * (size - 7)
    harness.worker.plans[query] = Plan(schema)
    with harness.connect() as connection, connection.cursor() as cursor:
        if size > 32:
            with pytest.raises(manager.ProgrammingError):
                cursor.execute(query)
            assert harness.worker.connections[0].readers == []
        else:
            cursor.execute(query)
            assert cursor.fetch_arrow_table().num_rows == 0


def test_request_timeout_recovery(harness: Harness) -> None:
    """Distinguish an HTTP request timeout from unsupported downstream cancellation."""
    schema = pa.schema([("n", pa.int64())])
    batch = pa.record_batch([[42]], schema=schema)
    harness.worker.plans["SELECT slow"] = Plan(schema, (batch,), delay_seconds=0.3)
    harness.worker.plans["SELECT fast"] = Plan(schema, (batch,))
    with (
        harness.connect(options={"grainlift.request_timeout_ms": 200}) as connection,
        connection.cursor() as cursor,
        pytest.raises((manager.Error, pa.ArrowException)),
    ):
        cursor.execute("SELECT slow")
        # If execution succeeds unexpectedly, still import/close the stream
        # so a failing assertion cannot block the server via raw release.
        with cursor.fetch_record_batch() as reader:
            reader.read_next_batch()
    # The callback is cooperative and finishes on its own; the timeout does not
    # terminate Python code. Wait only for that bounded fixture operation.
    deadline = time.monotonic() + 2
    while any(not reader.closed for item in harness.worker.connections for reader in item.readers):
        assert time.monotonic() < deadline, "Timed-out cursor did not clean up"
        time.sleep(0.01)
    with harness.connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT fast")
        assert cursor.fetch_arrow_table().column(0).to_pylist() == [42]
