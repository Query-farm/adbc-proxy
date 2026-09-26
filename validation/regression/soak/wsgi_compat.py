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

"""Adapt this bounded WSGI application to Granian 2.8.x's eager header capture.

Retain at most one existing response chunk, never collect a whole result. The
SDK bounds individual responses/batches. This adapter is diagnostic-only and
assumes this application's headers are set before its first yielded chunk.
"""

from collections.abc import Iterable, Iterator
from contextvars import copy_context
from typing import Any, Self
from wsgiref.types import StartResponse, WSGIApplication


class PrimedResponse:
    """Keep one prefetched chunk and its request context across host threads."""

    def __init__(self, app: WSGIApplication, environ: dict[str, Any], start_response: StartResponse) -> None:
        """Start a response before returning it to the host.

        Args:
            app: Bounded WSGI application.
            environ: Request environment.
            start_response: Host status and headers callback.
        """
        self._context = copy_context()
        self._response: Iterable[bytes] = self._context.run(app, environ, start_response)
        self._iterator: Iterator[bytes] = iter(self._response)
        self._closed = False
        self._first: bytes | None = None
        try:
            self._first = self._context.run(next, self._iterator, None)
        except BaseException:
            self.close()
            raise

    def __iter__(self) -> Self:
        """Return this iterator.

        Returns:
            This response iterator.
        """
        return self

    def __next__(self) -> bytes:
        """Return the saved chunk or advance once in the original context.

        Returns:
            One unchanged response chunk.
        """
        if self._closed:
            raise StopIteration
        if self._first is not None:
            chunk, self._first = self._first, None
            return chunk
        return self._context.run(next, self._iterator)

    def close(self) -> None:
        """Release the underlying response once, including an unconsumed chunk."""
        if not self._closed:
            self._closed = True
            self._first = None
            if close := getattr(self._response, "close", None):
                self._context.run(close)


def prime(app: WSGIApplication, environ: dict[str, Any], start_response: StartResponse) -> PrimedResponse:
    """Expose response headers before Granian captures them.

    Args:
        app: Bounded WSGI application.
        environ: Request environment.
        start_response: Host status and headers callback.

    Returns:
        A bounded, context-preserving response iterator.
    """
    return PrimedResponse(app, environ, start_response)
