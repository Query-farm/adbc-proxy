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

"""Ensure optional diagnostic instrumentation preserves failures and cleanup."""

import sys
from unittest.mock import Mock

import pytest

from soak.diagnose import _TimedApplication
from soak.latency import _Reader


def test_wsgi_failure_disables_optional_profiler(monkeypatch: pytest.MonkeyPatch) -> None:
    """An application failure must not leave profiling enabled in its host thread."""
    monkeypatch.setenv("GRAINLIFT_DIAGNOSTIC_CPU_PROFILE", "unused-profile-path")
    assert sys.getprofile() is None
    app = _TimedApplication(Mock(side_effect=RuntimeError("diagnostic failure")))
    with pytest.raises(RuntimeError, match="diagnostic failure"):
        next(app({}, Mock()))
    assert sys.getprofile() is None


def test_reader_failure_preserves_context_cleanup() -> None:
    """Reader instrumentation propagates pull errors and delegates cleanup once."""
    failure = OSError("diagnostic pull failure")
    source = Mock()
    source.__enter__ = Mock(return_value=source)
    source.__exit__ = Mock(return_value=False)
    source.__next__ = Mock(side_effect=failure)
    with pytest.raises(OSError, match="diagnostic pull failure"), _Reader(source) as reader:
        next(reader)
    source.__enter__.assert_called_once()
    source.__exit__.assert_called_once()
    assert source.__exit__.call_args.args[1] is failure
