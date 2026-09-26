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

"""Check deterministic fixture behavior without binding a network socket."""

import pyarrow as pa
import pytest
from grainlift import AdbcError

from .worker import Plan, ProbeIterator, ProbeWorker


def test_fixture_is_lazy_and_closes() -> None:
    """Make fixture observations trustworthy before testing native backpressure."""
    worker = ProbeWorker()
    schema = pa.schema([("n", pa.int64())])
    worker.plans["SELECT lazy"] = Plan(schema, (pa.record_batch([[42]], schema=schema),))
    connection = worker.connect("alice")
    result = connection.execute("SELECT lazy")
    probe = connection.readers[0]
    assert probe.pulls == 0
    assert next(result.batches).column(0).to_pylist() == [42]
    assert probe.pulls == 1
    result.close()
    result.close()
    assert probe.closed
    with pytest.raises(StopIteration):
        next(result.batches)
    connection.close()
    assert connection.closed


@pytest.mark.parametrize("after", [0, 1, 2])
def test_fixture_failure_position(after: int) -> None:
    """Inject the read error at exactly the selected batch boundary."""
    schema = pa.schema([("n", pa.int64())])
    batch = pa.record_batch([[1]], schema=schema)
    probe = ProbeIterator(Plan(schema, (batch, batch), fail_after=after))
    for _ in range(after):
        assert next(probe).equals(batch)
    with pytest.raises(AdbcError) as error:
        next(probe)
    assert error.value.status == "invalid_data"
    probe.close()


def test_empty_fixture_retains_schema() -> None:
    """Describe and execute an empty result without fabricating a data batch."""
    worker = ProbeWorker()
    schema = pa.schema([("n", pa.int64())])
    worker.plans["SELECT empty"] = Plan(schema)
    connection = worker.connect("alice")
    assert connection.execute_schema("SELECT empty").equals(schema)
    assert connection.readers == []
    result = connection.execute("SELECT empty")
    assert result.schema.equals(schema)
    assert list(result.batches) == []
    result.close()
    connection.close()
