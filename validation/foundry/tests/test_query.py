"""Official Driver Foundry query cases."""

import adbc_drivers_validation.tests.query
import pytest
from adbc_drivers_validation.tests.query import TestQuery  # noqa: F401

from .proxy_sqlite import get_quirks


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    adbc_drivers_validation.tests.query.generate_tests([get_quirks()], metafunc)
