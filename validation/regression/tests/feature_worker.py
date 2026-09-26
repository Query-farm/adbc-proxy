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

"""Small real SQLite fixture; synthetic partition and plan hooks only test transport."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator, Mapping
from contextlib import closing

import pyarrow as pa
from grainlift import AdbcError, Connection, PartitionedResult, QueryResult, Statement, Worker

from .feature_schemas import (
    INFO_SCHEMA,
    INFO_VALUE,
    OBJECTS_SCHEMA,
    STATISTIC,
    STATISTIC_DB,
    STATISTIC_NAMES_SCHEMA,
    STATISTIC_VALUE,
    STATISTICS_SCHEMA,
    TABLE_TYPES_SCHEMA,
)

type Value = str | bytes | int | float
MAX_ROWS = 256


def _identifier(value: str) -> str:
    if not value or len(value.encode()) > 256:
        raise AdbcError("Invalid fixture identifier", "invalid_arguments")
    return '"' + value.replace('"', '""') + '"'


def _result(batch: pa.RecordBatch) -> QueryResult:
    return QueryResult(batch.schema, iter([batch]))


class SQLiteFeatureWorker(Worker):
    """Open independent SQLite connections to a test-owned temporary database."""

    target = "features"

    def __init__(self, database_path: str) -> None:
        """Create the one bounded test table if this is the first connection."""
        self.database_path = database_path
        with closing(sqlite3.connect(database_path)) as connection:
            connection.execute("CREATE TABLE IF NOT EXISTS items (id INTEGER, label TEXT)")
            connection.commit()

    def connect(self, principal: str) -> SQLiteFeatureConnection:
        """Use a genuine independent SQLite transaction for every ADBC connection."""
        return SQLiteFeatureConnection(self.database_path, principal)

    def open_connection(
        self,
        principal: str,
        database_options: Mapping[str, Value],
        connection_options: Mapping[str, Value],
    ) -> SQLiteFeatureConnection:
        """Accept only explicit fixture initialization options; never replace the configured database path."""
        if any(not key.startswith("fixture.database.") for key in database_options):
            raise AdbcError("Unsupported fixture database option", "not_implemented")
        connection = self.connect(principal)
        try:
            for key, value in {**database_options, **connection_options}.items():
                connection.set_option(key, value)
        except BaseException:
            connection.close()
            raise
        return connection


class SQLiteFeatureConnection(Connection):
    """Support actual SQL, metadata, transactions, and typed fixture options."""

    def __init__(self, database_path: str, principal: str) -> None:
        """Start in autocommit mode and retain only bounded test state."""
        self.database = sqlite3.connect(database_path, isolation_level=None, check_same_thread=False)
        self.principal = principal
        self.autocommit = True
        self.options: dict[str, Value] = {}

    def new_statement(self) -> SQLiteFeatureStatement:
        """Allocate a statement with independent binding and option state."""
        return SQLiteFeatureStatement(self)

    def set_option(self, key: str, value: Value) -> None:
        """Apply autocommit to SQLite, or retain a typed fixture option."""
        if key == "adbc.connection.autocommit":
            if value not in ("true", "false"):
                raise AdbcError("Invalid autocommit option", "invalid_arguments")
            enabled = value == "true"
            if enabled and not self.autocommit:
                self.database.commit()
            elif not enabled and self.autocommit:
                self.database.execute("BEGIN")
            self.autocommit = enabled
        elif key.startswith("fixture."):
            self.options[key] = value
        else:
            raise AdbcError("Unsupported fixture connection option", "not_implemented")

    def get_option(self, key: str, value_type: str) -> Value:
        """Return a value without stringifying or losing its declared type."""
        if key == "adbc.connection.autocommit":
            return "true" if self.autocommit else "false"
        if key not in self.options:
            raise AdbcError("Unknown fixture option", "not_found")
        return self.options[key]

    def commit(self) -> None:
        """Commit the real transaction and keep manual transaction mode active."""
        if self.autocommit:
            raise AdbcError("No active transaction", "invalid_state")
        self.database.commit()
        self.database.execute("BEGIN")

    def rollback(self) -> None:
        """Rollback real writes and start the next manual transaction."""
        if self.autocommit:
            raise AdbcError("No active transaction", "invalid_state")
        self.database.rollback()
        self.database.execute("BEGIN")

    def get_info(self, codes: list[int] | None) -> QueryResult:
        """Return standard dense-union metadata, including the real engine version."""
        values = {0: "SQLite feature fixture", 1: sqlite3.sqlite_version, 100: "fixture backend"}
        selected = [code for code in values if codes is None or code in codes]
        children = [pa.array([values[code] for code in selected], type=pa.string())]
        children.extend(pa.array([], type=field.type) for field in list(INFO_VALUE)[1:])
        union = pa.UnionArray.from_dense(
            pa.array([0] * len(selected), type=pa.int8()),
            pa.array(range(len(selected)), type=pa.int32()),
            children,
            field_names=[field.name for field in INFO_VALUE],
            type_codes=list(range(6)),
        )
        return _result(pa.record_batch([pa.array(selected, type=pa.uint32()), union], schema=INFO_SCHEMA))

    def get_table_types(self) -> QueryResult:
        """Describe the table kind implemented by this fixture."""
        return _result(pa.record_batch([["TABLE"]], schema=TABLE_TYPES_SCHEMA))

    def get_table_schema(self, catalog: str | None, db_schema: str | None, table_name: str) -> pa.Schema:
        """Read actual column declarations from SQLite, including empty tables."""
        if catalog not in (None, "main") or db_schema not in (None, ""):
            raise AdbcError("Unknown fixture namespace", "not_found")
        rows = self.database.execute(f"PRAGMA table_info({_identifier(table_name)})").fetchall()
        if not rows:
            raise AdbcError("Unknown fixture table", "not_found")
        types = {"INTEGER": pa.int64(), "TEXT": pa.string(), "REAL": pa.float64(), "BLOB": pa.binary()}
        return pa.schema([pa.field(row[1], types[row[2]], not bool(row[3])) for row in rows])

    def get_objects(
        self,
        depth: int,
        catalog: str | None,
        db_schema: str | None,
        table_name: str | None,
        table_types: list[str] | None,
        column_name: str | None,
    ) -> QueryResult:
        """Describe real tables using the complete standard hierarchical schema."""
        if catalog not in (None, "main") or db_schema not in (None, ""):
            return QueryResult(OBJECTS_SCHEMA, iter([]))
        tables = []
        names = self.database.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE ?", (table_name or "%",)
        )
        for (name,) in names:
            if table_types is not None and "TABLE" not in table_types:
                continue
            columns = [
                {"column_name": field.name, "ordinal_position": index + 1, "xdbc_type_name": str(field.type)}
                for index, field in enumerate(self.get_table_schema(None, None, name))
                if column_name is None or field.name == column_name
            ]
            tables.append(
                {
                    "table_name": name,
                    "table_type": "TABLE",
                    "table_columns": columns if depth == 0 else None,
                    "table_constraints": [] if depth == 0 else None,
                }
            )
        databases = [{"db_schema_name": "", "db_schema_tables": None if depth == 2 else tables}]
        table = pa.Table.from_pylist(
            [{"catalog_name": "main", "catalog_db_schemas": None if depth == 1 else databases}], schema=OBJECTS_SCHEMA
        )
        return QueryResult(table.schema, iter(table.to_batches()))

    def get_statistic_names(self) -> QueryResult:
        """Advertise the fixture's deterministic custom statistic name."""
        return _result(pa.record_batch([["fixture.row_count"], [1024]], schema=STATISTIC_NAMES_SCHEMA))

    def get_statistics(
        self,
        catalog: str | None,
        db_schema: str | None,
        table_name: str | None,
        approximate: bool,
    ) -> QueryResult:
        """Compute an actual SQLite row count in the standard nested union schema."""
        name = table_name or "items"
        self.get_table_schema(catalog, db_schema, name)
        count = self.database.execute(f"SELECT COUNT(*) FROM {_identifier(name)}").fetchone()[0]
        union = pa.UnionArray.from_dense(
            pa.array([0], type=pa.int8()),
            pa.array([0], type=pa.int32()),
            [
                pa.array([count], type=pa.int64()),
                pa.array([], type=pa.uint64()),
                pa.array([], type=pa.float64()),
                pa.array([], type=pa.binary()),
            ],
            field_names=[field.name for field in STATISTIC_VALUE],
            type_codes=list(range(4)),
        )
        stats = pa.StructArray.from_arrays(
            [
                pa.array([name]),
                pa.array([None], type=pa.string()),
                pa.array([6], type=pa.int16()),
                union,
                pa.array([False]),
            ],
            fields=list(STATISTIC),
        )
        databases = pa.StructArray.from_arrays(
            [pa.array([""]), pa.ListArray.from_arrays(pa.array([0, 1], type=pa.int32()), stats)],
            fields=list(STATISTIC_DB),
        )
        return _result(
            pa.record_batch(
                [pa.array(["main"]), pa.ListArray.from_arrays(pa.array([0, 1], type=pa.int32()), databases)],
                schema=STATISTICS_SCHEMA,
            )
        )

    def read_partition(self, partition: bytes) -> QueryResult:
        """Decode a synthetic bounded descriptor after toolkit ownership validation."""
        if not partition.startswith(b"fixture:"):
            raise AdbcError("Unknown fixture partition", "not_found")
        reader = pa.ipc.open_stream(partition[len(b"fixture:") :])
        return QueryResult(reader.schema, reader)

    def close(self) -> None:
        """Closing a manual transaction rolls back pending SQLite writes."""
        self.database.close()


class SQLiteFeatureStatement(Statement):
    """Use actual SQLite SQL except explicitly named Arrow echo/opaque-plan hooks."""

    def __init__(self, connection: SQLiteFeatureConnection) -> None:
        """Allocate independent SQL, binding, and typed option state."""
        self.connection = connection
        self.sql = ""
        self.plan: bytes | None = None
        self.options: dict[str, Value] = {}
        self.bound_schema: pa.Schema | None = None
        self.bound: list[pa.RecordBatch] = []

    def set_sql_query(self, query: str) -> None:
        """Replace the active query and invalidate the previous binding and plan."""
        self.sql = query
        self.plan = None
        self.bound_schema = None
        self.bound = []

    def set_substrait_plan(self, plan: bytes) -> None:
        """Retain opaque bytes; this fixture deliberately does not interpret Substrait."""
        self.plan = plan
        self.sql = ""
        self.bound = []

    def set_option(self, key: str, value: Value) -> None:
        """Retain standard ingestion settings and typed fixture options."""
        if not key.startswith(("fixture.", "adbc.ingest.", "adbc.statement.")):
            raise AdbcError("Unsupported fixture statement option", "not_implemented")
        self.options[key] = value

    def get_option(self, key: str, value_type: str) -> Value:
        """Return the retained typed value without coercion."""
        if key not in self.options:
            raise AdbcError("Unknown fixture statement option", "not_found")
        return self.options[key]

    def prepare(self) -> None:
        """The fixture uses SQLite's real parameter binding when executed."""
        if not self.sql and self.plan is None:
            raise AdbcError("No query configured", "invalid_state")

    def get_parameter_schema(self) -> pa.Schema:
        """Describe the fixture's integer positional parameter contract."""
        return pa.schema([pa.field(str(index), pa.int64()) for index in range(self.sql.count("?"))])

    def bind(self, batch: pa.RecordBatch) -> None:
        """Replace a previous binding and retain a bounded batch, including its schema."""
        if batch.num_rows > MAX_ROWS:
            raise AdbcError("Fixture binding row limit", "invalid_data")
        self.bound_schema = batch.schema
        self.bound = [batch]

    def bind_stream(self, reader: pa.RecordBatchReader) -> None:
        """Retain bounded stream batches, including an empty stream's declared schema."""
        schema = reader.schema
        batches: list[pa.RecordBatch] = []
        rows = 0
        try:
            for batch in reader:
                rows += batch.num_rows
                if rows > MAX_ROWS or len(batches) >= MAX_ROWS:
                    raise AdbcError("Fixture binding row limit", "invalid_data")
                batches.append(batch)
        finally:
            reader.close()
        self.bound_schema = schema
        self.bound = batches

    def _parameters(self) -> Iterator[tuple[object, ...]]:
        if self.bound_schema is None:
            yield ()
        else:
            for batch in self.bound:
                for row in batch.to_pylist():
                    yield tuple(row[name] for name in batch.schema.names)

    def execute(self) -> QueryResult:
        """Run real parameterized SQL, or faithfully return explicit transport probes."""
        if self.plan is not None:
            return _result(pa.record_batch([pa.array([self.plan], type=pa.binary())], names=["plan_bytes"]))
        if self.sql == "SELECT echo_bound":
            if self.bound_schema is None:
                raise AdbcError("No binding", "invalid_state")
            return QueryResult(self.bound_schema, iter(self.bound))
        rows: list[tuple[object, ...]] = []
        names: list[str] = []
        for parameters in self._parameters():
            with closing(self.connection.database.execute(self.sql, parameters)) as cursor:
                if cursor.description is None:
                    return QueryResult(pa.schema([]), iter([]), max(cursor.rowcount, 0))
                names = [column[0] for column in cursor.description]
                rows.extend(cursor.fetchmany(MAX_ROWS + 1))
                if len(rows) > MAX_ROWS:
                    raise AdbcError("Fixture result row limit", "invalid_data")
        if not names:
            names = ["value"]
        fields = [pa.field(name, pa.string() if name == "label" else pa.int64()) for name in names]
        schema = pa.schema(fields)
        batches = [
            pa.record_batch(
                [
                    pa.array([row[i] for row in rows[offset : offset + 2]], type=field.type)
                    for i, field in enumerate(schema)
                ],
                schema=schema,
            )
            for offset in range(0, len(rows), 2)
        ]
        return QueryResult(schema, iter(batches))

    def execute_schema(self) -> pa.Schema:
        """Describe this fixture's fixed SQL or bound Arrow schema without executing writes."""
        if self.sql == "SELECT echo_bound" and self.bound_schema is not None:
            return self.bound_schema
        if self.plan is not None:
            return pa.schema([pa.field("plan_bytes", pa.binary())])
        if "FROM items" in self.sql:
            return self.connection.get_table_schema(None, None, "items")
        return pa.schema([pa.field("value", pa.int64())])

    def execute_update(self) -> int | None:
        """Execute real writes or ingest bound Arrow values using ordinary ADBC options."""
        if "adbc.ingest.target_table" in self.options:
            return self._ingest()
        affected = 0
        for parameters in self._parameters():
            with closing(self.connection.database.execute(self.sql, parameters)) as cursor:
                affected += max(cursor.rowcount, 0)
        return None if self.options.get("fixture.unknown_rows") == "true" else affected

    def _ingest(self) -> int:
        if self.bound_schema is None:
            raise AdbcError("Ingestion requires an Arrow schema", "invalid_state")
        name = _identifier(str(self.options["adbc.ingest.target_table"]))
        mode = self.options.get("adbc.ingest.mode", "adbc.ingest.mode.create")
        temporary = self.options.get("adbc.ingest.temporary", "false") == "true"
        if mode == "adbc.ingest.mode.replace":
            self.connection.database.execute(f"DROP TABLE IF EXISTS {name}")
        if mode != "adbc.ingest.mode.append":
            columns = []
            for field in self.bound_schema:
                dtype = field.type.value_type if pa.types.is_dictionary(field.type) else field.type
                sql_type = "TEXT" if pa.types.is_string(dtype) else "BLOB" if pa.types.is_binary(dtype) else "INTEGER"
                columns.append(f"{_identifier(field.name)} {sql_type}")
            exists = "IF NOT EXISTS " if mode == "adbc.ingest.mode.create_append" else ""
            self.connection.database.execute(
                f"CREATE {'TEMP ' if temporary else ''}TABLE {exists}{name} ({','.join(columns)})"
            )
        parameters = list(self._parameters())
        placeholders = ",".join("?" for _ in self.bound_schema)
        self.connection.database.executemany(f"INSERT INTO {name} VALUES ({placeholders})", parameters)
        return len(parameters)

    def execute_partitions(self) -> PartitionedResult:
        """Return bounded self-contained fixture descriptors, not a distributed database promise."""
        result = self.execute()
        partitions = []
        rows = 0
        try:
            for batch in result.batches:
                rows += batch.num_rows
                sink = pa.BufferOutputStream()
                with pa.ipc.new_stream(sink, result.schema) as writer:
                    writer.write_batch(batch)
                partitions.append(b"fixture:" + sink.getvalue().to_pybytes())
        finally:
            result.close()
        return PartitionedResult(result.schema, partitions, rows)

    def close(self) -> None:
        """Release retained fixture bindings."""
        self.bound = []
        self.bound_schema = None
