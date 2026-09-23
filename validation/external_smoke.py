#!/usr/bin/env python3
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

"""C-ABI smoke test using the external Python ADBC driver manager."""

from __future__ import annotations

import os
from pathlib import Path

import adbc_driver_manager
import pyarrow


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} must be set")
    return value


def read_all(statement: adbc_driver_manager.AdbcStatement) -> pyarrow.Table:
    handle, rows_affected = statement.execute_query()
    assert rows_affected in (-1, None), rows_affected
    with pyarrow.RecordBatchReader._import_from_c(handle.address) as reader:
        return reader.read_all()


def main() -> None:
    proxy_driver = Path(require_env("ADBC_PROXY_DRIVER")).resolve(strict=True)
    endpoint = require_env("ADBC_PROXY_ENDPOINT")
    token = os.environ.get("ADBC_PROXY_TOKEN", "")
    target = os.environ.get("ADBC_PROXY_TARGET", "sqlite")
    backend = os.environ.get("ADBC_PROXY_BACKEND", target)
    if backend not in {
        "sqlite",
        "duckdb",
        "postgresql",
        "mysql",
        "flightsql",
        "datafusion",
        "trino",
        "mssql",
    }:
        raise RuntimeError(f"unsupported validation backend: {backend}")

    payload_type = "BYTEA" if backend == "postgresql" else "BLOB"
    placeholders = "$1, $2, $3" if backend == "postgresql" else "?, ?, ?"

    if token:
        rejected_options = {
            "driver": str(proxy_driver),
            "entrypoint": "AdbcDriverProxyInit",
            "proxy.uri": endpoint,
            "proxy.target": target,
            "proxy.auth.bearer_token": f"{token}-invalid",
        }
        if downstream_uri := os.environ.get("ADBC_PROXY_DOWNSTREAM_URI"):
            rejected_options["uri"] = downstream_uri
        rejected = adbc_driver_manager.AdbcDatabase(**rejected_options)
        try:
            try:
                rejected_connection = adbc_driver_manager.AdbcConnection(rejected)
            except adbc_driver_manager.Error:
                pass
            else:
                rejected_connection.close()
                raise AssertionError("the service accepted an invalid bearer token")
        finally:
            rejected.close()

    options = {
        "driver": str(proxy_driver),
        "entrypoint": "AdbcDriverProxyInit",
        "proxy.uri": endpoint,
        "proxy.target": target,
    }
    if token:
        options["proxy.auth.bearer_token"] = token
    if downstream_uri := os.environ.get("ADBC_PROXY_DOWNSTREAM_URI"):
        options["uri"] = downstream_uri
    if direct := os.environ.get("ADBC_PROXY_IROH_DIRECT_ADDRESS"):
        options["proxy.iroh.direct_address"] = direct
    tls_options = {
        "ADBC_PROXY_TLS_CA": "proxy.tls.ca",
        "ADBC_PROXY_TLS_CERT": "proxy.tls.cert",
        "ADBC_PROXY_TLS_KEY": "proxy.tls.key",
        "ADBC_PROXY_TLS_SERVER_NAME": "proxy.tls.server_name",
    }
    for environment, option in tls_options.items():
        if value := os.environ.get(environment):
            options[option] = value

    denied_options = dict(options)
    denied_options["proxy.validation.disallowed"] = "must-not-reach-driver"
    denied_database = adbc_driver_manager.AdbcDatabase(**denied_options)
    try:
        try:
            denied_connection = adbc_driver_manager.AdbcConnection(denied_database)
        except adbc_driver_manager.Error as error:
            assert (
                error.status_code == adbc_driver_manager.AdbcStatusCode.INVALID_ARGUMENT
            )
            assert "proxy.validation.disallowed" in str(error)
            assert "must-not-reach-driver" not in str(error)
        else:
            denied_connection.close()
            raise AssertionError("the service accepted a disallowed database option")
    finally:
        denied_database.close()

    database = adbc_driver_manager.AdbcDatabase(**options)
    try:
        connection = adbc_driver_manager.AdbcConnection(database)
        try:
            try:
                connection.set_options(
                    **{"proxy.validation.disallowed": "must-not-reach-driver"}
                )
            except adbc_driver_manager.Error as error:
                assert (
                    error.status_code
                    == adbc_driver_manager.AdbcStatusCode.INVALID_ARGUMENT
                )
                assert "proxy.validation.disallowed" in str(error)
                assert "must-not-reach-driver" not in str(error)
            else:
                raise AssertionError(
                    "the service accepted a disallowed connection option"
                )

            if backend in {"mysql", "flightsql", "datafusion", "trino", "mssql"}:
                statement = adbc_driver_manager.AdbcStatement(connection)
                try:
                    statement.set_sql_query("SELECT 42 AS answer")
                    table = read_all(statement)
                    assert table.num_rows == 1
                    assert table.column("answer").to_pylist() == [42]

                    try:
                        statement.set_sql_query(
                            "SELECT * FROM adbc_proxy_table_that_does_not_exist"
                        )
                        read_all(statement)
                    except adbc_driver_manager.Error:
                        pass
                    else:
                        raise AssertionError(
                            "a downstream query error was not propagated"
                        )
                finally:
                    statement.close()
                print(
                    "PASS external ADBC driver-manager -> proxy dylib -> "
                    f"VGI/{os.environ.get('ADBC_PROXY_TRANSPORT', 'http')} -> "
                    f"proxy service -> {backend} ADBC"
                )
                return

            statement = adbc_driver_manager.AdbcStatement(connection)
            try:
                statement.set_sql_query("DROP TABLE IF EXISTS proxy_validation")
                statement.execute_update()

                statement.set_sql_query(
                    "CREATE TABLE proxy_validation "
                    f"(id INTEGER NOT NULL, value TEXT, payload {payload_type})"
                )
                statement.execute_update()

                statement.set_sql_query(
                    "INSERT INTO proxy_validation VALUES "
                    "(1, 'alpha', NULL), (2, NULL, NULL), (3, '世界 🚀', NULL)"
                )
                assert statement.execute_update() == 3

                statement.set_sql_query(
                    "SELECT id, value, payload FROM proxy_validation ORDER BY id"
                )
                statement.prepare()
                table = read_all(statement)
                assert table.num_rows == 3
                assert table.column("id").to_pylist() == [1, 2, 3]
                assert table.column("value").to_pylist() == ["alpha", None, "世界 🚀"]
                assert table.column("payload").to_pylist() == [
                    None,
                    None,
                    None,
                ]

                statement.set_sql_query(
                    "SELECT COUNT(*) AS count FROM proxy_validation"
                )
                count = read_all(statement)
                assert count.column("count").to_pylist() == [3]

                statement.set_sql_query(
                    "INSERT INTO proxy_validation (id, value, payload) "
                    f"VALUES ({placeholders})"
                )
                parameters = pyarrow.record_batch(
                    [
                        pyarrow.array([4], type=pyarrow.int64()),
                        pyarrow.array(["bound"], type=pyarrow.string()),
                        pyarrow.array([b"bound-bytes"], type=pyarrow.binary()),
                    ],
                    names=["id", "value", "payload"],
                )
                schema_capsule, array_capsule = parameters.__arrow_c_array__()
                statement.bind(array_capsule, schema_capsule)
                assert statement.execute_update() == 1

                connection.set_autocommit(False)
                statement.set_sql_query(
                    "INSERT INTO proxy_validation VALUES (90, 'rollback', NULL)"
                )
                assert statement.execute_update() == 1
                connection.rollback()
                statement.set_sql_query(
                    "SELECT COUNT(*) AS count FROM proxy_validation WHERE id = 90"
                )
                assert read_all(statement).column("count").to_pylist() == [0]

                statement.set_sql_query(
                    "INSERT INTO proxy_validation VALUES (91, 'commit', NULL)"
                )
                assert statement.execute_update() == 1
                connection.commit()
                statement.set_sql_query(
                    "SELECT COUNT(*) AS count FROM proxy_validation WHERE id = 91"
                )
                assert read_all(statement).column("count").to_pylist() == [1]
                # End the read transaction before toggling autocommit.
                connection.rollback()
                connection.set_autocommit(True)

                try:
                    statement.set_sql_query("SELECT * FROM table_that_does_not_exist")
                    read_all(statement)
                except adbc_driver_manager.Error as error:
                    assert "table_that_does_not_exist" in str(error)
                    if backend == "sqlite":
                        assert isinstance(error, adbc_driver_manager.ProgrammingError)
                        assert (
                            error.status_code
                            == adbc_driver_manager.AdbcStatusCode.INVALID_ARGUMENT
                        )
                else:
                    raise AssertionError("a downstream SQLite error was not propagated")
            finally:
                statement.close()
        finally:
            connection.close()
    finally:
        database.close()

    print(
        f"PASS external ADBC driver-manager -> proxy dylib -> VGI/{os.environ.get('ADBC_PROXY_TRANSPORT', 'http')} -> "
        f"proxy service -> {backend} ADBC"
    )


if __name__ == "__main__":
    main()
