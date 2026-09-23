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

from .proxy_sqlite import get_quirks


def pytest_addoption(parser: pytest.Parser) -> None:
    adbc_drivers_validation.tests.conftest.pytest_addoption(parser)


@pytest.fixture(scope="session")
def driver(request: pytest.FixtureRequest):
    quirks = get_quirks()
    assert request.param.startswith(f"{quirks.name}:")
    return quirks


@pytest.fixture(scope="session")
def driver_path() -> str:
    return os.environ["ADBC_PROXY_DRIVER"]
