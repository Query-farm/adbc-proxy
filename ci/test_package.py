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

"""Verify that a packaged proxy driver is discoverable and initializes."""

import adbc_driver_manager


def test_package() -> None:
    database = adbc_driver_manager.AdbcDatabase(
        driver="proxy",
        entrypoint="AdbcDriverProxyInit",
        **{
            "proxy.uri": "http://127.0.0.1:1",
            "proxy.target": "package-load-test",
        },
    )
    database.close()
