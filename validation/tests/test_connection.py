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

"""Official Driver Foundry connection and metadata cases."""

import adbc_drivers_validation.tests.connection
import pytest
from adbc_drivers_validation.tests.connection import (
    TestConnection as BaseTestConnection,
)

from .grainlift import get_quirks


class TestConnection(BaseTestConnection):
    def test_current_db_schema(self, driver, conn) -> None:
        if driver.name == "grainlift-sqlite":
            pytest.skip("ASF SQLite does not expose the current schema option")
        return super().test_current_db_schema(driver, conn)

    def test_get_table_schema_not_found(self, driver, conn) -> None:
        if driver.name != "grainlift-duckdb":
            return super().test_get_table_schema_not_found(driver, conn)
        with pytest.raises(conn.Error):
            conn.adbc_get_table_schema("test_get_table_schema_not_found")

    def test_get_statistics(self, driver, conn, get_statistics_table) -> None:
        if driver.name == "grainlift-sqlite":
            pytest.skip("ASF SQLite does not implement GetStatistics")
        if driver.name == "grainlift-postgresql":
            table_name = driver.quote_identifier(get_statistics_table[-1])
            with conn.cursor() as cursor:
                cursor.execute(f"ANALYZE {table_name}")
        return super().test_get_statistics(driver, conn, get_statistics_table)

    def test_unknown_option(self, subtests, driver, conn) -> None:
        # The proxy database intentionally accepts arbitrary downstream options
        # before connection initialization. The ASF SQLite driver also returns
        # NOT_IMPLEMENTED, rather than NOT_FOUND, for unknown connection gets.
        pytest.skip("proxy/downstream option pass-through differs from Foundry policy")


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    adbc_drivers_validation.tests.connection.generate_tests([get_quirks()], metafunc)
