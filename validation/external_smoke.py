#!/usr/bin/env python3
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
    if backend not in {"sqlite", "duckdb", "postgresql"}:
        raise RuntimeError(f"unsupported validation backend: {backend}")

    payload_type = "BYTEA" if backend == "postgresql" else "BLOB"
    placeholders = "$1, $2, $3" if backend == "postgresql" else "?, ?, ?"

    if token:
        rejected = adbc_driver_manager.AdbcDatabase(
            **{
                "driver": str(proxy_driver),
                "entrypoint": "AdbcDriverProxyInit",
                "uri": endpoint,
                "adbc.proxy.target": target,
                "adbc.proxy.auth.bearer_token": f"{token}-invalid",
            }
        )
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
        "uri": endpoint,
        "adbc.proxy.target": target,
    }
    if token:
        options["adbc.proxy.auth.bearer_token"] = token
    if direct := os.environ.get("ADBC_PROXY_IROH_DIRECT_ADDRESS"):
        options["adbc.proxy.iroh.direct_address"] = direct
    tls_options = {
        "ADBC_PROXY_TLS_CA": "adbc.proxy.tls.ca",
        "ADBC_PROXY_TLS_CERT": "adbc.proxy.tls.cert",
        "ADBC_PROXY_TLS_KEY": "adbc.proxy.tls.key",
        "ADBC_PROXY_TLS_SERVER_NAME": "adbc.proxy.tls.server_name",
    }
    for environment, option in tls_options.items():
        if value := os.environ.get(environment):
            options[option] = value
    database = adbc_driver_manager.AdbcDatabase(**options)
    try:
        connection = adbc_driver_manager.AdbcConnection(database)
        try:
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
