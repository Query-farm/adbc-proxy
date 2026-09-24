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

"""Fixtures adapting the official ADBC Driver Foundry suite to the proxy."""

from __future__ import annotations

import os

import adbc_drivers_validation.tests.conftest
import pytest
from adbc_drivers_validation.tests.conftest import (  # noqa: F401
    conn,
    conn_factory,
    db_kwargs,
    manual_test,
    noci,
    pytest_collection_modifyitems,
)

from .proxy import get_quirks


def pytest_addoption(parser: pytest.Parser) -> None:
    adbc_drivers_validation.tests.conftest.pytest_addoption(parser)
    parser.addoption("--vendor-version", action="store", default=None)


@pytest.fixture(scope="session")
def driver(request: pytest.FixtureRequest, pytestconfig: pytest.Config):
    quirks = get_quirks(pytestconfig.getoption("vendor_version"))
    assert request.param.startswith(f"{quirks.name}:")
    return quirks


@pytest.fixture(scope="session")
def driver_path() -> str:
    return os.environ["ADBC_PROXY_DRIVER"]
