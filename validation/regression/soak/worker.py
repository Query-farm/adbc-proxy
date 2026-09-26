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

"""Importable synthetic worker that generates bounded batches for load testing."""

from collections.abc import Iterator

import pyarrow as pa
from grainlift import AdbcError, Connection, QueryResult, Worker


class LoadConnection(Connection):
    """Generate a fixed, finite result for the load harness."""

    def __init__(self, rows: int, batch_rows: int, payload_bytes: int) -> None:
        """Store the workload dimensions.

        Args:
            rows: Total rows in each query.
            batch_rows: Maximum rows generated per pull.
            payload_bytes: Fixed binary payload size per row.
        """
        self.rows = rows
        self.batch_rows = batch_rows
        self.payload_bytes = payload_bytes
        self.schema = pa.schema([("number", pa.int64()), ("payload", pa.binary())])

    def execute(self, sql: str) -> QueryResult:
        """Return lazy bounded batches or an intentional recoverable query error.

        Args:
            sql: A harness command, never logged or recorded in reports.

        Returns:
            Finite Arrow result.
        """
        if sql == "FAIL":
            raise AdbcError("Injected workload error", "invalid_data", sqlstate="22000")
        if sql != "QUERY":
            raise AdbcError("Unknown workload command", "invalid_arguments")

        def batches() -> Iterator[pa.RecordBatch]:
            for start in range(0, self.rows, self.batch_rows):
                end = min(start + self.batch_rows, self.rows)
                yield pa.record_batch(
                    [range(start, end), [b"x" * self.payload_bytes] * (end - start)], schema=self.schema
                )

        return QueryResult(self.schema, batches())


class LoadWorker(Worker):
    """Create independent synthetic connections for each authenticated client."""

    def __init__(self, rows: int = 4096, batch_rows: int = 512, payload_bytes: int = 64) -> None:
        """Validate and retain bounded workload dimensions.

        Args:
            rows: Number of generated rows per result, at most one million.
            batch_rows: Rows per batch, at most 4096.
            payload_bytes: Binary bytes per row, at most 1024.
        """
        if not 1 <= rows <= 1_000_000 or not 1 <= batch_rows <= 4096 or not 0 <= payload_bytes <= 1024:
            raise ValueError("Invalid workload dimensions")
        if batch_rows * (payload_bytes + 16) > 1024 * 1024:
            raise ValueError("Synthetic batches must fit the service byte limit")
        self.rows = rows
        self.batch_rows = batch_rows
        self.payload_bytes = payload_bytes

    def connect(self, principal: str) -> Connection:
        """Create a connection whose result state is independent of other clients.

        Args:
            principal: Authenticated owner supplied by the service.

        Returns:
            Bounded synthetic connection.
        """
        return LoadConnection(self.rows, self.batch_rows, self.payload_bytes)
