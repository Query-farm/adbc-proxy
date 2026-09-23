#!/usr/bin/env python3
"""Query an ADBC Proxy target through the standard Python ADBC DBAPI."""

from __future__ import annotations

import os
from pathlib import Path

import adbc_driver_manager
import adbc_driver_manager.dbapi as adbc


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} must be set")
    return value


def main() -> None:
    driver = Path(required_env("ADBC_PROXY_DRIVER")).expanduser().resolve(strict=True)
    endpoint = required_env("ADBC_PROXY_ENDPOINT")
    target = required_env("ADBC_PROXY_TARGET")
    options = {
        "uri": endpoint,
        "adbc.proxy.target": target,
    }
    optional_options = {
        "ADBC_PROXY_TOKEN": "adbc.proxy.auth.bearer_token",
        "ADBC_PROXY_IROH_DIRECT_ADDRESS": "adbc.proxy.iroh.direct_address",
        "ADBC_PROXY_IROH_SECRET_KEY": "adbc.proxy.iroh.secret_key",
        "ADBC_PROXY_TLS_CA": "adbc.proxy.tls.ca",
        "ADBC_PROXY_TLS_CERT": "adbc.proxy.tls.cert",
        "ADBC_PROXY_TLS_KEY": "adbc.proxy.tls.key",
        "ADBC_PROXY_TLS_SERVER_NAME": "adbc.proxy.tls.server_name",
    }
    for environment, option in optional_options.items():
        if value := os.environ.get(environment):
            options[option] = value

    with adbc.connect(
        driver=driver,
        entrypoint="AdbcDriverProxyInit",
        db_kwargs=options,
        autocommit=True,
    ) as connection:
        print("server-selected downstream driver:", connection.adbc_get_info())

        placeholder = "$1" if target == "postgresql" else "?"
        query = f"SELECT 1 + {placeholder} AS answer"

        with connection.cursor() as cursor:
            try:
                schema = cursor.adbc_execute_schema(
                    "SELECT CAST(42 AS BIGINT) AS answer"
                )
                print("schema before execution:", schema)
            except adbc_driver_manager.NotSupportedError:
                print("execute-schema is not supported by this downstream driver")

            cursor.execute(query, [41])
            table = cursor.fetch_arrow_table()
            print(table)
            assert table.column("answer").to_pylist() == [42]


if __name__ == "__main__":
    main()
