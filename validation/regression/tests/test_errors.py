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

"""Authentication, structured ADBC failures, streaming failures, and recovery."""

from typing import cast

import adbc_driver_manager as manager
import pyarrow as pa
import pytest

from .conftest import Harness
from .worker import Plan

pytestmark = pytest.mark.native


@pytest.mark.parametrize("token", ["", "wrong-token"])
def test_bad_credentials_rejected(harness: Harness, token: str) -> None:
    """Reject invalid credentials before opening any worker connection."""
    with pytest.raises(manager.Error), harness.connect(token=token):
        pytest.fail("Invalid credentials were accepted")
    assert harness.worker.connections == []


def test_unknown_target_rejected(harness: Harness) -> None:
    """Reject a nonexistent target without invoking the worker."""
    with pytest.raises(manager.Error), harness.connect(target="missing"):
        pytest.fail("Unknown target was accepted")
    assert harness.worker.connections == []


def test_caller_destination_rejected(harness: Harness) -> None:
    """Keep server configuration authoritative over caller connection strings."""
    with pytest.raises(manager.NotSupportedError), harness.connect(options={"uri": "forbidden-destination"}):
        pytest.fail("Caller-supplied destination was accepted")
    assert harness.worker.connections == []


def test_structured_error_and_recovery(harness: Harness) -> None:
    """Preserve status, SQLSTATE, and binary details, then reuse the connection."""
    schema = pa.schema([("n", pa.int64())])
    harness.worker.plans["SELECT recovery"] = Plan(schema, (pa.record_batch([[42]], schema=schema),))
    with harness.connect() as connection, connection.cursor() as cursor:
        with pytest.raises(manager.DataError) as error:
            cursor.execute("SELECT structured_error")
        assert error.value.status_code == manager.AdbcStatusCode.INVALID_DATA
        assert error.value.sqlstate == "22000"
        # The pinned manager's stubs say str; its C extension returns bytes keys.
        details = cast(list[tuple[str | bytes, bytes]], error.value.details)
        normalized = {key.decode() if isinstance(key, bytes) else key: value for key, value in details}
        assert normalized["fixture"] == b"\x00\xff"
        # Known adbc_ffi 1.1 sentinel limitation; do not claim vendor-code fidelity.
        assert error.value.vendor_code in (None, 42)
        cursor.execute("SELECT recovery")
        assert cursor.fetch_arrow_table().column(0).to_pylist() == [42]


def test_unexpected_error_is_sanitized(harness: Harness, caplog: pytest.LogCaptureFixture) -> None:
    """Keep unexpected backend diagnostics out of client errors and logs."""
    with harness.connect() as connection, connection.cursor() as cursor:
        with pytest.raises(manager.InternalError) as error:
            cursor.execute("SELECT unexpected_error")
        assert "private downstream diagnostic" not in str(error.value)
    assert "private downstream diagnostic" not in caplog.text


def test_midstream_failure_closes_iterator(harness: Harness) -> None:
    """Fail Arrow consumption after one batch and clean up the server cursor."""
    schema = pa.schema([("n", pa.int64())])
    harness.worker.plans["SELECT fail_read"] = Plan(
        schema,
        (pa.record_batch([[1]], schema=schema),),
        fail_after=1,
    )
    with harness.connect() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT fail_read")
        with cursor.fetch_record_batch() as reader:
            assert reader.read_next_batch().num_rows == 1
            with pytest.raises(pa.ArrowException):
                reader.read_next_batch()
        assert harness.worker.connections[0].readers[0].closed


@pytest.mark.parametrize("method", ["prepare", "cancel"])
def test_unsupported_statement_operations(harness: Harness, method: str) -> None:
    """Return ADBC NOT_IMPLEMENTED for capabilities the worker cannot provide."""
    with (
        manager.AdbcDatabase(
            driver=str(harness.driver),
            entrypoint="AdbcDriverGrainliftInit",
            **{
                "grainlift.uri": harness.endpoint,
                "grainlift.target": "regression",
                "grainlift.auth.bearer_token": "alice-token",
            },
        ) as database,
        manager.AdbcConnection(database) as connection,
        manager.AdbcStatement(connection) as statement,
    ):
        statement.set_sql_query("SELECT unsupported")
        with pytest.raises(manager.NotSupportedError):
            if method == "prepare":
                statement.prepare()
            else:
                statement.cancel()
