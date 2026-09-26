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

"""Exercise selected public C ABI calls without Python argument normalization."""

from __future__ import annotations

import ctypes
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import adbc_driver_manager as manager
import adbc_driver_manager._lib as native


class Database(ctypes.Structure):
    """Own the two opaque pointers defined by the public AdbcDatabase C struct."""

    _fields_ = [("private_data", ctypes.c_void_p), ("private_driver", ctypes.c_void_p)]


class Connection(ctypes.Structure):
    """Own the two opaque pointers defined by the public AdbcConnection C struct."""

    _fields_ = [("private_data", ctypes.c_void_p), ("private_driver", ctypes.c_void_p)]


class MetadataConnection:
    """Call public driver-manager symbols using test-owned native handles."""

    def __init__(self, library: ctypes.CDLL, connection: Connection) -> None:
        """Retain the loaded public API and the connection for this scoped probe."""
        self.library = library
        self.connection = connection

    def get_info(self, codes: list[int] | None) -> manager.ArrowArrayStreamHandle:
        """Preserve a non-null zero-length selection independently of its length."""
        values = None if codes is None else (ctypes.c_uint32 * max(1, len(codes)))(*codes)
        stream = manager.ArrowArrayStreamHandle()
        status = self.library.AdbcConnectionGetInfo(
            ctypes.byref(self.connection), values, 0 if codes is None else len(codes), stream.address, None
        )
        assert status == 0, f"AdbcConnectionGetInfo failed with status {status}"
        return stream

    def get_objects(self, table_types: list[str] | None) -> manager.ArrowArrayStreamHandle:
        """Preserve null, empty and nonempty null-terminated table-type lists."""
        values = (
            None
            if table_types is None
            else (ctypes.c_char_p * (len(table_types) + 1))(*(value.encode() for value in table_types), None)
        )
        stream = manager.ArrowArrayStreamHandle()
        status = self.library.AdbcConnectionGetObjects(
            ctypes.byref(self.connection), 0, None, None, None, values, None, stream.address, None
        )
        assert status == 0, f"AdbcConnectionGetObjects failed with status {status}"
        return stream


@contextmanager
def metadata_connection(driver: Path, endpoint: str, target: str) -> Iterator[MetadataConnection]:
    """Allocate and release native handles through public driver-manager functions."""
    library = ctypes.CDLL(native.__file__)
    for name in ("AdbcDatabaseNew", "AdbcDatabaseInit", "AdbcDatabaseRelease"):
        function = getattr(library, name)
        function.argtypes = [ctypes.POINTER(Database), ctypes.c_void_p]
        function.restype = ctypes.c_uint8
    for name in ("AdbcConnectionNew", "AdbcConnectionRelease"):
        function = getattr(library, name)
        function.argtypes = [ctypes.POINTER(Connection), ctypes.c_void_p]
        function.restype = ctypes.c_uint8
    library.AdbcDatabaseSetOption.argtypes = [
        ctypes.POINTER(Database),
        ctypes.c_char_p,
        ctypes.c_char_p,
        ctypes.c_void_p,
    ]
    library.AdbcConnectionInit.argtypes = [ctypes.POINTER(Connection), ctypes.POINTER(Database), ctypes.c_void_p]
    library.AdbcConnectionGetInfo.argtypes = [
        ctypes.POINTER(Connection),
        ctypes.POINTER(ctypes.c_uint32),
        ctypes.c_size_t,
        ctypes.c_void_p,
        ctypes.c_void_p,
    ]
    library.AdbcConnectionGetObjects.argtypes = [
        ctypes.POINTER(Connection),
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_char_p,
        ctypes.c_char_p,
        ctypes.POINTER(ctypes.c_char_p),
        ctypes.c_char_p,
        ctypes.c_void_p,
        ctypes.c_void_p,
    ]
    for name in ("AdbcDatabaseSetOption", "AdbcConnectionInit", "AdbcConnectionGetInfo", "AdbcConnectionGetObjects"):
        getattr(library, name).restype = ctypes.c_uint8
    database = Database()
    connection = Connection()
    try:
        assert library.AdbcDatabaseNew(ctypes.byref(database), None) == 0
        for key, value in {
            "driver": str(driver),
            "entrypoint": "AdbcDriverGrainliftInit",
            "grainlift.uri": endpoint,
            "grainlift.target": target,
            "grainlift.auth.bearer_token": "alice-token",
        }.items():
            assert library.AdbcDatabaseSetOption(ctypes.byref(database), key.encode(), value.encode(), None) == 0
        assert library.AdbcDatabaseInit(ctypes.byref(database), None) == 0
        assert library.AdbcConnectionNew(ctypes.byref(connection), None) == 0
        assert library.AdbcConnectionInit(ctypes.byref(connection), ctypes.byref(database), None) == 0
        yield MetadataConnection(library, connection)
    finally:
        try:
            if connection.private_data:
                assert library.AdbcConnectionRelease(ctypes.byref(connection), None) == 0
        finally:
            if database.private_data:
                assert library.AdbcDatabaseRelease(ctypes.byref(database), None) == 0
