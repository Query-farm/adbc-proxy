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

"""Preserve Arrow values, schemas, and pull-based consumption through the C ABI."""

from datetime import UTC, datetime
from decimal import Decimal

import pyarrow as pa
import pytest

from .conftest import Harness
from .worker import Plan

pytestmark = pytest.mark.native


def arrow_cases() -> list[tuple[str, pa.Array]]:
    """Construct representative arrays, including nulls and dictionary encoding."""
    return [
        ("integer", pa.array([-(2**63), None, 2**63 - 1], type=pa.int64())),
        ("boolean", pa.array([True, None, False], type=pa.bool_())),
        ("float", pa.array([1.25, None, -2.5], type=pa.float64())),
        ("unicode", pa.array(["Grain 🌾", None, "日本語"], type=pa.string())),
        ("binary", pa.array([b"\x00\xff", None, b""], type=pa.binary())),
        ("decimal", pa.array([Decimal("123.450"), None, Decimal("-0.001")], type=pa.decimal128(12, 3))),
        ("timestamp", pa.array([datetime(2026, 1, 1, tzinfo=UTC), None], type=pa.timestamp("us", tz="UTC"))),
        ("list", pa.array([[1, None], None, []], type=pa.list_(pa.int32()))),
        ("struct", pa.array([{"x": 1}, None], type=pa.struct([("x", pa.int32())]))),
        ("dictionary", pa.array(["grain", None, "grain", "lift"]).dictionary_encode()),
    ]


@pytest.mark.parametrize(("name", "array"), arrow_cases(), ids=lambda value: value if isinstance(value, str) else None)
def test_arrow_values_and_metadata(harness: Harness, name: str, array: pa.Array) -> None:
    """Preserve values, field metadata, schema metadata, and dictionary types."""
    schema = pa.schema(
        [pa.field(name, array.type, metadata={b"unit": b"fixture"})],
        metadata={b"fixture": b"arrow-types"},
    )
    batch = pa.record_batch([array], schema=schema)
    harness.worker.plans["SELECT types"] = Plan(schema, (batch,))
    with harness.connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT types")
        table = cursor.fetch_arrow_table()
        assert table.equals(pa.Table.from_batches([batch]), check_metadata=True)


@pytest.mark.parametrize("rows", [0, 1, 1023, 1024, 1025, 2500])
def test_batch_boundaries_and_empty_result(harness: Harness, rows: int) -> None:
    """Return all batches without dropping, duplicating, or losing empty schemas."""
    schema = pa.schema([pa.field("number", pa.int64(), nullable=False)])
    batches = tuple(
        pa.record_batch([list(range(start, min(start + 1024, rows)))], schema=schema) for start in range(0, rows, 1024)
    )
    harness.worker.plans["SELECT numbers"] = Plan(schema, batches)
    with harness.connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT numbers")
        with cursor.fetch_record_batch() as reader:
            assert reader.schema.equals(schema)
            received = list(reader)
        assert [batch.num_rows for batch in received] == [batch.num_rows for batch in batches]
        assert pa.Table.from_batches(received, schema=schema).column(0).to_pylist() == list(range(rows))


def test_schema_inference_does_not_execute(harness: Harness) -> None:
    """Describe a result through ADBC without creating an iterator."""
    schema = pa.schema([("value", pa.string())])
    harness.worker.plans["SELECT schema_only"] = Plan(schema)
    with harness.connect() as connection, connection.cursor() as cursor:
        assert cursor.adbc_execute_schema("SELECT schema_only").equals(schema)
        assert harness.worker.connections[0].readers == []


def test_pull_backpressure_and_early_release(harness: Harness) -> None:
    """Fetch at most one batch ahead and release an abandoned result."""
    schema = pa.schema([("n", pa.int64())])
    batches = tuple(pa.record_batch([[value]], schema=schema) for value in range(20))
    harness.worker.plans["SELECT lazy"] = Plan(schema, batches)
    with harness.connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT lazy")
            probe = harness.worker.connections[0].readers[0]
            assert probe.pulls == 1
            with cursor.fetch_record_batch() as reader:
                assert reader.read_next_batch().column(0).to_pylist() == [0]
                assert probe.pulls == 1
        assert probe.closed
        assert probe.pulls < len(batches)


def test_empty_batch_between_nonempty_batches(harness: Harness) -> None:
    """Distinguish a zero-row Arrow batch from end of stream."""
    schema = pa.schema([("n", pa.int64())])
    batches = tuple(pa.record_batch([values], schema=schema) for values in ([1], [], [2]))
    harness.worker.plans["SELECT with_empty_batch"] = Plan(schema, batches)
    with harness.connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT with_empty_batch")
        assert cursor.fetch_arrow_table().column(0).to_pylist() == [1, 2]
