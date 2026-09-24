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

"""Official Driver Foundry bulk-ingest cases."""

import adbc_drivers_validation.tests.ingest
import pytest
from adbc_drivers_validation.tests.ingest import TestIngest as BaseTestIngest

from .grainlift import get_quirks


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    adbc_drivers_validation.tests.ingest.generate_tests(
        [get_quirks()],
        metafunc,
        ingest_mode_queries={"ingest/int64"},
    )


class TestIngest(BaseTestIngest):
    @staticmethod
    def _xfail_status(driver, affected: set[str]) -> None:
        if driver.name not in affected:
            return
        statuses = {
            "grainlift-sqlite": "INTERNAL",
            "grainlift-duckdb": "INTERNAL",
            "grainlift-postgresql": "INVALID_ARGUMENT",
        }
        pytest.xfail(
            f"the downstream {driver.vendor_name} driver reports "
            f"{statuses[driver.name]} instead of ALREADY_EXISTS"
        )

    def test_append_schema_mismatch(self, driver, conn, query) -> None:
        self._xfail_status(
            driver,
            {"grainlift-sqlite", "grainlift-duckdb", "grainlift-postgresql"},
        )
        return super().test_append_schema_mismatch(driver, conn, query)

    def test_create_conflict(self, driver, conn, query) -> None:
        self._xfail_status(driver, {"grainlift-sqlite", "grainlift-postgresql"})
        return super().test_create_conflict(driver, conn, query)

    def test_createappend_schema_mismatch(self, driver, conn, query) -> None:
        self._xfail_status(
            driver,
            {"grainlift-sqlite", "grainlift-duckdb", "grainlift-postgresql"},
        )
        return super().test_createappend_schema_mismatch(driver, conn, query)

    def test_ingest_no_parameters(self, driver, conn) -> None:
        if driver.name == "grainlift-duckdb":
            pytest.xfail(
                "the downstream DuckDB driver treats an unbound ingest as a no-op"
            )
        return super().test_ingest_no_parameters(driver, conn)
