"""Declared proxy/backend capabilities for Driver Foundry validation."""

from __future__ import annotations

import copy
import os
import re
from pathlib import Path

import adbc_driver_manager
from adbc_drivers_validation import model, quirks


class ProxySqliteQuirks(model.DriverQuirks):
    name = "proxy-sqlite"
    driver = "adbc_driver_proxy"
    # Connection metadata is intentionally forwarded from the downstream
    # driver, so these are the ASF SQLite values rather than proxy branding.
    driver_name = "ADBC SQLite Driver"
    vendor_name = "SQLite"
    vendor_version = re.compile(r"3\..*")
    short_version = "0.2.0"
    features = model.DriverFeatures(
        connection_get_table_schema=True,
        connection_transactions=True,
        get_objects=True,
        statement_bind=True,
        # The ASF SQLite driver returns NOT_IMPLEMENTED; the proxy preserves it.
        statement_execute_schema=False,
        statement_get_parameter_schema=True,
        statement_prepare=True,
        statement_rows_affected=True,
        select_fixture_setup=True,
        quirk_foundry=False,
        current_catalog="main",
        current_schema="",
    )
    _database_options = {
        "adbc.proxy.uri": model.FromEnv("ADBC_PROXY_ENDPOINT"),
        "adbc.proxy.target": model.FromEnv("ADBC_PROXY_TARGET"),
    }
    if os.environ.get("ADBC_PROXY_TOKEN"):
        _database_options["adbc.proxy.auth.bearer_token"] = model.FromEnv("ADBC_PROXY_TOKEN")
    if os.environ.get("ADBC_PROXY_DOWNSTREAM_URI"):
        _database_options["uri"] = model.FromEnv("ADBC_PROXY_DOWNSTREAM_URI")
    if os.environ.get("ADBC_PROXY_IROH_DIRECT_ADDRESS"):
        _database_options["adbc.proxy.iroh.direct_address"] = model.FromEnv(
            "ADBC_PROXY_IROH_DIRECT_ADDRESS"
        )
    for environment, option in {
        "ADBC_PROXY_TLS_CA": "adbc.proxy.tls.ca",
        "ADBC_PROXY_TLS_CERT": "adbc.proxy.tls.cert",
        "ADBC_PROXY_TLS_KEY": "adbc.proxy.tls.key",
        "ADBC_PROXY_TLS_SERVER_NAME": "adbc.proxy.tls.server_name",
    }.items():
        if os.environ.get(environment):
            _database_options[option] = model.FromEnv(environment)
    setup = model.DriverSetup(database=_database_options)

    @property
    def queries_paths(self) -> tuple[Path]:
        return (model.ROOT / "queries",)

    @property
    def query_set(self) -> model.QuerySet:
        # query_set() is cached globally by Foundry; clone it so these SQLite
        # overrides cannot leak into another driver adapter in the same run.
        queries = copy.deepcopy(model.query_set(self.queries_paths))
        # Foundry's base expectations assume that the backend preserves narrow
        # integer/float and temporal types. SQLite intentionally normalizes
        # these types. Until this adapter has SQLite-specific expected schemas,
        # run only cases whose generic expected schema is valid for SQLite.
        compatible = {
            "type/bind/binary",
            "type/bind/float64",
            "type/bind/int64",
            "type/bind/string",
            "type/literal/float64",
            "type/literal/int64",
            "type/literal/string",
            "type/select/binary",
            "type/select/float64",
            "type/select/int64",
            "type/select/string",
        }
        for name, query in queries.queries.items():
            if name not in compatible:
                query.metadata_paths.insert(
                    0,
                    {
                        "skip": (
                            "generic Foundry schema/dialect does not model SQLite "
                            "type normalization"
                        )
                    },
                )
        return queries

    def is_table_not_found(self, table_name: str | None, error: Exception) -> bool:
        return (
            isinstance(error, adbc_driver_manager.NotFoundError)
            or "no such table" in str(error).lower()
        )

    def split_statement(self, statement: str) -> list[str]:
        return quirks.split_statement(statement, dialect="sqlite")


class ProxyDuckdbQuirks(model.DriverQuirks):
    name = "proxy-duckdb"
    driver = "adbc_driver_proxy"
    driver_name = "ADBC DuckDB Driver"
    vendor_name = "duckdb"
    vendor_version = re.compile(r"v1\.5\..*")
    short_version = "0.2.0"
    features = model.DriverFeatures(
        connection_get_table_schema=True,
        connection_get_statistics=True,
        connection_transactions=True,
        # DuckDB 1.5.5 metadata differs from Foundry's generic hierarchy.
        get_objects=False,
        statement_bind=True,
        statement_execute_schema=True,
        statement_get_parameter_schema=True,
        statement_prepare=True,
        statement_rows_affected=True,
        select_fixture_setup=True,
        quirk_foundry=False,
        current_catalog="validation",
        current_schema="main",
    )
    setup = ProxySqliteQuirks.setup

    @property
    def queries_paths(self) -> tuple[Path]:
        return (model.ROOT / "queries",)

    @property
    def query_set(self) -> model.QuerySet:
        queries = copy.deepcopy(model.query_set(self.queries_paths))
        compatible = {
            "type/literal/float64",
            "type/literal/int64",
            "type/literal/string",
            "type/select/float64",
            "type/select/int64",
            "type/select/string",
        }
        for name, query in queries.queries.items():
            if name not in compatible:
                query.metadata_paths.insert(
                    0,
                    {
                        "skip": (
                            "generic Foundry SQL/batch expectations do not model "
                            "DuckDB 1.5.5 behavior"
                        )
                    },
                )
        return queries

    def is_table_not_found(self, table_name: str | None, error: Exception) -> bool:
        return "does not exist" in str(error).lower()

    def split_statement(self, statement: str) -> list[str]:
        return quirks.split_statement(statement, dialect="duckdb")


class ProxyPostgresqlQuirks(model.DriverQuirks):
    name = "proxy-postgresql"
    driver = "adbc_driver_proxy"
    driver_name = "ADBC PostgreSQL Driver"
    vendor_name = "PostgreSQL"
    vendor_version = re.compile(r"14.*")
    short_version = "0.2.0"
    features = model.DriverFeatures(
        connection_get_table_schema=True,
        connection_get_statistics=True,
        connection_transactions=True,
        get_objects=True,
        statement_bind=True,
        statement_execute_schema=True,
        statement_get_parameter_schema=True,
        statement_prepare=True,
        statement_rows_affected=True,
        statement_rows_affected_ddl=False,
        select_fixture_setup=True,
        quirk_foundry=False,
        current_catalog="postgres",
        current_schema="public",
    )
    setup = ProxySqliteQuirks.setup

    @property
    def queries_paths(self) -> tuple[Path]:
        return (model.ROOT / "queries",)

    @property
    def query_set(self) -> model.QuerySet:
        queries = copy.deepcopy(model.query_set(self.queries_paths))
        compatible = {
            "type/bind/boolean",
            "type/bind/date",
            "type/bind/float16",
            "type/bind/float32",
            "type/bind/float64",
            "type/bind/int16",
            "type/bind/int32",
            "type/bind/int64",
            "type/bind/large_string",
            "type/bind/string",
            "type/bind/string_view",
            "type/bind/time_us",
            "type/bind/timestamp_us",
            "type/bind/timestamptz_us",
            "type/literal/boolean",
            "type/literal/date",
            "type/literal/float32",
            "type/literal/float64",
            "type/literal/int16",
            "type/literal/int32",
            "type/literal/int64",
            "type/literal/string",
            "type/literal/time",
            "type/literal/timestamp",
            "type/select/boolean",
            "type/select/date",
            "type/select/float32",
            "type/select/float64",
            "type/select/int16",
            "type/select/int32",
            "type/select/int64",
            "type/select/string",
            "type/select/time",
            "type/select/timestamp4",
            "type/select/timestamp4tz",
            "type/select/timestamp5",
            "type/select/timestamp5tz",
            "type/select/timestamp6",
            "type/select/timestamp6tz",
        }
        for name, query in queries.queries.items():
            if name not in compatible:
                query.metadata_paths.insert(
                    0,
                    {
                        "skip": (
                            "generic Foundry SQL/schema does not model PostgreSQL "
                            "binary, decimal, or temporal conventions"
                        )
                    },
                )
        return queries

    def bind_parameter(self, index: int) -> str:
        return f"${index}"

    def is_table_not_found(self, table_name: str | None, error: Exception) -> bool:
        return "does not exist" in str(error).lower()

    def split_statement(self, statement: str) -> list[str]:
        return quirks.split_statement(statement, dialect="postgres")


def get_quirks() -> model.DriverQuirks:
    backend = os.environ.get("ADBC_PROXY_BACKEND", "sqlite")
    classes = {
        "sqlite": ProxySqliteQuirks,
        "duckdb": ProxyDuckdbQuirks,
        "postgresql": ProxyPostgresqlQuirks,
    }
    try:
        return classes[backend]()
    except KeyError as error:
        raise RuntimeError(f"unsupported ADBC_PROXY_BACKEND: {backend}") from error
