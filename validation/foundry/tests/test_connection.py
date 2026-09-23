"""Official Driver Foundry connection and metadata cases."""

import adbc_drivers_validation.tests.connection
import pytest
from adbc_drivers_validation.tests.connection import (
    TestConnection as BaseTestConnection,
)

from .proxy_sqlite import get_quirks


class TestConnection(BaseTestConnection):
    def test_current_db_schema(self, driver, conn) -> None:
        if driver.name == "proxy-sqlite":
            pytest.skip("ASF SQLite does not expose the current schema option")
        return super().test_current_db_schema(driver, conn)

    def test_get_info(self, driver, conn, record_property) -> None:
        """Accept the version strings emitted by the ASF SQLite 1.12 driver."""
        if driver.name != "proxy-sqlite":
            return super().test_get_info(driver, conn, record_property)
        info = conn.adbc_get_info()
        assert info.get("driver_name") == driver.driver_name
        assert info.get("driver_version") == "(unknown)"
        assert info.get("vendor_name") == driver.vendor_name
        assert driver.vendor_version.match(info.get("vendor_version", ""))
        record_property("driver_version", info["driver_version"])
        record_property("vendor_version", info["vendor_version"])

    def test_get_info_arrow_version(self, driver, conn) -> None:
        if driver.name not in {"proxy-sqlite", "proxy-duckdb"}:
            return super().test_get_info_arrow_version(driver, conn)
        info = conn.adbc_get_info()
        expected = {
            "proxy-sqlite": "0.9.0-SNAPSHOT",
            "proxy-duckdb": "(unknown)",
        }
        assert info.get("driver_arrow_version") == expected[driver.name]

    def test_get_table_schema_not_found(self, driver, conn) -> None:
        if driver.name != "proxy-duckdb":
            return super().test_get_table_schema_not_found(driver, conn)
        with pytest.raises(conn.Error):
            conn.adbc_get_table_schema("test_get_table_schema_not_found")

    def test_get_statistics(self, driver, conn, get_statistics_table) -> None:
        if driver.name == "proxy-sqlite":
            pytest.skip("ASF SQLite does not implement GetStatistics")
        if driver.name == "proxy-postgresql":
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
