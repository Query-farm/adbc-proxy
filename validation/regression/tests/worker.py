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

"""Deterministic Arrow results and faults without a downstream database."""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass

import pyarrow as pa
from grainlift import AdbcError, Connection, QueryResult, Worker


@dataclass(frozen=True)
class Plan:
    """Describe one small, bounded fixture result.

    Attributes:
        schema: Declared result schema, including empty results.
        batches: Predetermined test batches; never production data.
        fail_after: Raise an ADBC error after this many successful batch reads.
        delay_seconds: Delay each batch read for request-timeout tests.
        read_started: Signal entry into a deliberately blocked batch callback.
        read_release: Hold the callback until the test permits bounded cleanup.
    """

    schema: pa.Schema
    batches: tuple[pa.RecordBatch, ...] = ()
    fail_after: int | None = None
    delay_seconds: float = 0
    read_started: threading.Event | None = None
    read_release: threading.Event | None = None


class ProbeIterator(Iterator[pa.RecordBatch]):
    """Observe result consumption and cleanup without inspecting toolkit internals."""

    def __init__(self, plan: Plan) -> None:
        """Initialize a cursor without consuming any batches.

        Args:
            plan: Bounded result or fault to serve.
        """
        self.plan = plan
        self.pulls = 0
        self.closed = False
        self._position = 0

    def __next__(self) -> pa.RecordBatch:
        """Produce a batch, EOF, or a deterministic error."""
        if self.closed:
            raise StopIteration
        self.pulls += 1
        if self.plan.read_started is not None:
            self.plan.read_started.set()
        if self.plan.read_release is not None and not self.plan.read_release.wait(timeout=30):
            raise AdbcError("Fixture read gate watchdog expired", "timeout")
        if self.plan.delay_seconds:
            time.sleep(self.plan.delay_seconds)
        if self.plan.fail_after == self._position:
            raise AdbcError("Injected read error", "invalid_data", sqlstate="22000")
        if self._position == len(self.plan.batches):
            raise StopIteration
        result = self.plan.batches[self._position]
        self._position += 1
        return result

    def close(self) -> None:
        """Release the test cursor; repeated close is harmless."""
        self.closed = True


class ProbeConnection(Connection):
    """Implement fixture statements while recording only lifecycle events."""

    def __init__(self, worker: ProbeWorker, principal: str) -> None:
        """Record the owner and allocate an empty cursor list.

        Args:
            worker: Owner of the fixture plans and observations.
            principal: Authenticated owner of this connection.
        """
        self.worker = worker
        self.principal = principal
        self.closed = False
        self.readers: list[ProbeIterator] = []

    def _plan(self, sql: str) -> Plan:
        if sql == "SELECT structured_error":
            raise AdbcError(
                "Injected query error",
                "invalid_data",
                sqlstate="22000",
                vendor_code=42,
                details={"fixture": b"\x00\xff"},
            )
        if sql == "SELECT unexpected_error":
            raise RuntimeError("private downstream diagnostic")
        plan = self.worker.plans.get(sql)
        if plan is None:
            raise AdbcError("Unknown fixture query", "invalid_arguments", sqlstate="42000")
        return plan

    def execute(self, sql: str) -> QueryResult:
        """Create a lazy cursor for a fixture statement.

        Args:
            sql: Exact statement registered by the test.

        Returns:
            Schema and observed iterator.
        """
        plan = self._plan(sql)
        reader = ProbeIterator(plan)
        self.readers.append(reader)
        return QueryResult(plan.schema, reader)

    def execute_schema(self, sql: str) -> pa.Schema:
        """Describe a result without creating or consuming a cursor.

        Args:
            sql: Exact statement registered by the test.

        Returns:
            Declared Arrow schema.
        """
        return self._plan(sql).schema

    def close(self) -> None:
        """Record connection release."""
        self.closed = True


class ProbeWorker(Worker):
    """Provide a single target with bounded, test-owned plans."""

    target = "regression"

    def __init__(self) -> None:
        """Create isolated plans and connection observations."""
        self.plans: dict[str, Plan] = {}
        self.connections: list[ProbeConnection] = []
        self._lock = threading.Lock()

    def connect(self, principal: str) -> ProbeConnection:
        """Create an independent connection for each ADBC handle.

        Args:
            principal: Authenticated caller identity.

        Returns:
            A newly allocated test connection.
        """
        connection = ProbeConnection(self, principal)
        with self._lock:
            self.connections.append(connection)
        return connection
