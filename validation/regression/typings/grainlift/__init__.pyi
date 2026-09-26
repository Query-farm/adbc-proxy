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

"""Public API declarations for the initially untyped grainlift-python package."""

from collections.abc import Iterator, Mapping
from types import TracebackType
from typing import Self
from wsgiref.types import WSGIApplication

import pyarrow as pa

class AdbcError(Exception):
    status: str
    def __init__(
        self,
        message: str,
        status: str = ...,
        *,
        sqlstate: str = ...,
        vendor_code: int = ...,
        details: Mapping[str, bytes] | None = ...,
    ) -> None: ...

class Limits:
    def __init__(
        self,
        sessions: int = ...,
        statements_per_session: int = ...,
        batch_bytes: int = ...,
        request_bytes: int = ...,
        sql_bytes: int = ...,
        idle_seconds: float = ...,
        lock_timeout_seconds: float = ...,
        shutdown_seconds: float = ...,
    ) -> None: ...

class QueryResult:
    schema: pa.Schema
    batches: Iterator[pa.RecordBatch]
    rows_affected: int | None
    def __init__(
        self,
        schema: pa.Schema,
        batches: Iterator[pa.RecordBatch],
        rows_affected: int | None = ...,
    ) -> None: ...
    def close(self) -> None: ...

class Connection:
    def execute(self, sql: str) -> QueryResult: ...
    def execute_schema(self, sql: str) -> pa.Schema: ...
    def close(self) -> None: ...

class Worker:
    target: str
    def connect(self, principal: str) -> Connection: ...

class IsolatedWorker(Worker):
    def __init__(
        self,
        factory: str,
        *,
        target: str = ...,
        timeout_seconds: float = ...,
        startup_timeout_seconds: float = ...,
        max_message_bytes: int = ...,
        max_results: int = ...,
        worker_options: dict[str, object] | None = ...,
    ) -> None: ...

class Service:
    def __init__(self, worker: Worker, *, limits: Limits | None = ...) -> None: ...
    def __enter__(self) -> Self: ...
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...
    def close(self) -> None: ...
    def app(self, *, tokens: dict[str, str] | TokenStore) -> WSGIApplication: ...

class TokenStore:
    def __init__(self, tokens: Mapping[str, str]) -> None: ...
    def replace(self, tokens: Mapping[str, str]) -> None: ...
    def authenticate(self, authorization: str) -> str | None: ...
