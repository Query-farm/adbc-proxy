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

"""Query an Grainlift target through the standard Python ADBC DBAPI."""

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
    driver = Path(required_env("GRAINLIFT_DRIVER")).expanduser().resolve(strict=True)
    endpoint = required_env("GRAINLIFT_ENDPOINT")
    target = required_env("GRAINLIFT_TARGET")
    options = {
        "grainlift.uri": endpoint,
        "grainlift.target": target,
    }
    optional_options = {
        "GRAINLIFT_TOKEN": "grainlift.auth.bearer_token",
        "GRAINLIFT_IROH_DIRECT_ADDRESS": "grainlift.iroh.direct_address",
        "GRAINLIFT_IROH_SECRET_KEY": "grainlift.iroh.secret_key",
        "GRAINLIFT_TLS_CA": "grainlift.tls.ca",
        "GRAINLIFT_TLS_CERT": "grainlift.tls.cert",
        "GRAINLIFT_TLS_KEY": "grainlift.tls.key",
        "GRAINLIFT_TLS_SERVER_NAME": "grainlift.tls.server_name",
    }
    for environment, option in optional_options.items():
        if value := os.environ.get(environment):
            options[option] = value

    with adbc.connect(
        driver=driver,
        entrypoint="AdbcDriverGrainliftInit",
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
