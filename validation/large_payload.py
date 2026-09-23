#!/usr/bin/env python3
"""Bounded-memory probes for proxy bind requests and result responses.

The default run uses a small HTTP response budget so it is suitable for CI.
Pass ``--heavy`` to additionally exercise the real 64 MiB bind ceiling, and
``--response-budget-mib 256`` to exercise the native HTTP client's default
response ceiling rather than an equivalent small budget.
"""

from __future__ import annotations

import argparse
import contextlib
import gc
import json
import os
import socket
import subprocess
import time
import urllib.parse
from pathlib import Path
from typing import Any, Callable

import adbc_driver_manager
import pyarrow

MIB = 1024 * 1024
BIND_CAP = 64 * MIB


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"{name} must be set")
    return value


def options(response_budget: int) -> dict[str, Any]:
    result: dict[str, Any] = {
        "driver": str(Path(require_env("ADBC_PROXY_DRIVER")).resolve(strict=True)),
        "entrypoint": "AdbcDriverProxyInit",
        "uri": require_env("ADBC_PROXY_ENDPOINT"),
        "adbc.proxy.target": require_env("ADBC_PROXY_TARGET"),
        "adbc.proxy.max_response_bytes": response_budget,
    }
    optional = {
        "ADBC_PROXY_TOKEN": "adbc.proxy.auth.bearer_token",
        "ADBC_PROXY_IROH_DIRECT_ADDRESS": "adbc.proxy.iroh.direct_address",
        "ADBC_PROXY_IROH_SECRET_KEY": "adbc.proxy.iroh.secret_key",
        "ADBC_PROXY_TLS_CA": "adbc.proxy.tls.ca",
        "ADBC_PROXY_TLS_CERT": "adbc.proxy.tls.cert",
        "ADBC_PROXY_TLS_KEY": "adbc.proxy.tls.key",
        "ADBC_PROXY_TLS_SERVER_NAME": "adbc.proxy.tls.server_name",
    }
    for environment, option in optional.items():
        if value := os.environ.get(environment):
            result[option] = value
    if value := os.environ.get("ADBC_PROXY_MAX_BIND_BYTES"):
        result["adbc.proxy.max_bind_bytes"] = int(value)
    return result


def rss_kib(pid: int | None) -> int | None:
    if pid is None:
        return None
    try:
        value = subprocess.check_output(
            ["ps", "-o", "rss=", "-p", str(pid)],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        return int(value.split()[0]) if value else None
    except (OSError, ValueError, subprocess.SubprocessError):
        return None


def statement_update(connection: Any, sql: str) -> None:
    statement = adbc_driver_manager.AdbcStatement(connection)
    try:
        statement.set_sql_query(sql)
        statement.execute_update()
    finally:
        statement.close()


def query_blob(connection: Any, backend: str, size: int) -> int:
    expressions = {
        "sqlite": f"zeroblob({size})",
        "duckdb": f"repeat('x', {size})::BLOB",
        "postgresql": f"decode(repeat('00', {size}), 'hex')",
    }
    statement = adbc_driver_manager.AdbcStatement(connection)
    try:
        statement.set_sql_query(f"SELECT {expressions[backend]} AS payload")
        stream, _ = statement.execute_query()
        with pyarrow.RecordBatchReader._import_from_c(stream.address) as reader:
            table = reader.read_all()
        return len(table.column("payload")[0].as_py())
    finally:
        statement.close()


def capture(action: Callable[[], Any]) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        value = action()
        return {"outcome": "accepted", "value": value, "seconds": time.perf_counter() - started}
    except Exception as error:  # Validation deliberately records boundary errors.
        return {
            "outcome": "rejected",
            "error_type": type(error).__name__,
            "error": str(error),
            "seconds": time.perf_counter() - started,
        }


def incomplete_http_request(mode: str, wait_seconds: float) -> str:
    endpoint = urllib.parse.urlparse(require_env("ADBC_PROXY_ENDPOINT"))
    host = endpoint.hostname or "127.0.0.1"
    port = endpoint.port or 80
    path = "/org.queryfarm.AdbcProxy.v1/open_connection"
    token = os.environ.get("ADBC_PROXY_TOKEN", "")
    content_length = 70 * MIB if mode == "oversized_content_length" else MIB
    headers = (
        f"POST {path} HTTP/1.1\r\n"
        f"Host: {host}:{port}\r\n"
        "Content-Type: application/vnd.apache.arrow.stream\r\n"
        f"Content-Length: {content_length}\r\n"
        f"Authorization: Bearer {token}\r\n"
        "Connection: close\r\n\r\n"
    ).encode()
    with socket.create_connection((host, port), timeout=5) as sock:
        sock.sendall(headers + b"truncated")
        if mode == "disconnect":
            return "client_closed"
        if mode == "timeout":
            time.sleep(wait_seconds)
        else:
            sock.shutdown(socket.SHUT_WR)
        sock.settimeout(5)
        response = sock.recv(4096)
        return response.split(b"\r\n", 1)[0].decode("ascii", "replace")


def bind_once(connection: Any, backend: str, payload_size: int, stream: bool) -> int:
    placeholder = "$1" if backend == "postgresql" else "?"
    statement = adbc_driver_manager.AdbcStatement(connection)
    try:
        statement.set_sql_query(
            f"INSERT INTO proxy_large_payload (payload) VALUES ({placeholder})"
        )
        batch = pyarrow.record_batch(
            [pyarrow.array([b"x" * payload_size], type=pyarrow.binary())],
            names=["payload"],
        )
        if stream:
            reader = pyarrow.RecordBatchReader.from_batches(batch.schema, [batch])
            statement.bind_stream(reader.__arrow_c_stream__())
        else:
            schema_capsule, array_capsule = batch.__arrow_c_array__()
            statement.bind(array_capsule, schema_capsule)
        return statement.execute_update()
    finally:
        statement.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--heavy", action="store_true", help="run real 64 MiB bind cases")
    parser.add_argument("--response-budget-mib", type=int, default=2)
    parser.add_argument("--settle-seconds", type=float, default=1.0)
    parser.add_argument(
        "--wire-faults",
        action="store_true",
        help="HTTP-only truncated transfer, disconnect, and request-timeout probes",
    )
    parser.add_argument("--timeout-wait-seconds", type=float, default=3.0)
    parser.add_argument("--json-output", type=Path)
    args = parser.parse_args()
    if args.response_budget_mib <= 0 or args.settle_seconds < 0:
        parser.error("budgets must be positive and settle time non-negative")
    return args


def main() -> None:
    args = parse_args()
    backend = require_env("ADBC_PROXY_BACKEND")
    transport = require_env("ADBC_PROXY_TRANSPORT")
    server_pid = int(value) if (value := os.environ.get("ADBC_PROXY_SERVER_PID")) else None
    budget = args.response_budget_mib * MIB
    configured_bind_cap = int(os.environ.get("ADBC_PROXY_MAX_BIND_BYTES", BIND_CAP))
    server_bind_cap = int(
        os.environ.get("ADBC_PROXY_SERVER_MAX_BIND_BYTES", configured_bind_cap)
    )
    report: dict[str, Any] = {
        "backend": backend,
        "transport": transport,
        "limits": {
            "client_cumulative_bind_bytes": configured_bind_cap,
            "server_cumulative_bind_bytes": server_bind_cap,
            "http_client_response_bytes": budget,
            "vgi_rust_impl_message_bytes": 2**32 - 1,
        },
        "rss_kib": {"baseline": rss_kib(server_pid)},
        "response_cases": {},
        "bind_cases": {},
        "wire_faults": {},
    }

    database = adbc_driver_manager.AdbcDatabase(**options(budget))
    connection = adbc_driver_manager.AdbcConnection(database)
    try:
        # Payload sizes bracket the configured response budget. The HTTP VGI
        # envelope means a payload exactly at the budget is expected to be too
        # large; TCP/mTLS/Iroh do not apply this HTTP-client ceiling.
        margin = min(64 * 1024, budget // 4)
        for label, size in (
            ("below", budget - margin),
            ("at_payload_bytes", budget),
            ("above", budget + margin),
        ):
            report["response_cases"][label] = {
                "payload_bytes": size,
                **capture(lambda size=size: query_blob(connection, backend, size)),
            }
        recovery = query_blob(connection, backend, 1)
        if recovery != 1:
            raise AssertionError("connection did not recover after response boundary probes")

        if args.heavy or "ADBC_PROXY_MAX_BIND_BYTES" in os.environ:
            statement_update(connection, "DROP TABLE IF EXISTS proxy_large_payload")
            blob_type = "BYTEA" if backend == "postgresql" else "BLOB"
            statement_update(
                connection,
                f"CREATE TABLE proxy_large_payload (payload {blob_type} NOT NULL)",
            )
            # Four KiB leaves room for Arrow array and IPC bookkeeping while
            # bracketing the configured cumulative native-stream budget.
            for stream in (False, True):
                binding = "bind_stream" if stream else "bind"
                margin = min(4096, configured_bind_cap // 4)
                cases = [
                    ("below", configured_bind_cap - margin),
                    ("at_payload_bytes", configured_bind_cap),
                    ("above", configured_bind_cap + margin),
                ]
                if args.heavy and configured_bind_cap == BIND_CAP:
                    cases.insert(
                        1, ("below_arrow_cap_near_envelope", BIND_CAP - 512)
                    )
                for label, size in cases:
                    report["bind_cases"][f"{binding}_{label}"] = {
                        "payload_bytes": size,
                        **capture(
                            lambda size=size, stream=stream: bind_once(
                                connection, backend, size, stream
                            )
                        ),
                    }
                if server_bind_cap < configured_bind_cap:
                    report["bind_cases"][f"{binding}_server_limit"] = {
                        "payload_bytes": server_bind_cap,
                        **capture(
                            lambda stream=stream: bind_once(
                                connection, backend, server_bind_cap, stream
                            )
                        ),
                    }
                if query_blob(connection, backend, 1) != 1:
                    raise AssertionError(f"connection unusable after {binding} rejection")
                gc.collect()
                report["rss_kib"][f"after_{binding}"] = rss_kib(server_pid)

        if args.wire_faults:
            if transport != "http":
                raise RuntimeError("--wire-faults currently targets HTTP framing only")
            for fault in (
                "oversized_content_length",
                "truncated",
                "disconnect",
                "timeout",
            ):
                report["wire_faults"][fault] = capture(
                    lambda fault=fault: incomplete_http_request(
                        fault, args.timeout_wait_seconds
                    )
                )
            if query_blob(connection, backend, 1) != 1:
                raise AssertionError("connection unusable after HTTP wire fault probes")
            report["rss_kib"]["after_wire_faults"] = rss_kib(server_pid)
    finally:
        with contextlib.suppress(Exception):
            connection.close()
        with contextlib.suppress(Exception):
            database.close()

    gc.collect()
    time.sleep(args.settle_seconds)
    report["rss_kib"]["settled"] = rss_kib(server_pid)

    http = transport == "http"
    below = report["response_cases"]["below"]["outcome"]
    above = report["response_cases"]["above"]["outcome"]
    if below != "accepted":
        raise AssertionError(f"below-budget response was unexpectedly {below}")
    if http and above != "rejected":
        raise AssertionError("HTTP response above accepted_max_response_bytes was not rejected")
    if not http and above != "accepted":
        raise AssertionError(f"{transport} incorrectly applied the HTTP response budget")
    if args.heavy and configured_bind_cap == BIND_CAP:
        for binding in ("bind", "bind_stream"):
            near = report["bind_cases"][
                f"{binding}_below_arrow_cap_near_envelope"
            ]["outcome"]
            expected = "accepted"
            if near != expected:
                raise AssertionError(
                    f"{binding} near-cap envelope was {near}, expected {expected} on {transport}"
                )
    if args.heavy or "ADBC_PROXY_MAX_BIND_BYTES" in os.environ:
        for binding in ("bind", "bind_stream"):
            if (
                server_bind_cap >= configured_bind_cap
                and report["bind_cases"][f"{binding}_below"]["outcome"] != "accepted"
            ):
                raise AssertionError(f"{binding} below configured cap was rejected")
            for label in ("at_payload_bytes", "above"):
                if report["bind_cases"][f"{binding}_{label}"]["outcome"] != "rejected":
                    raise AssertionError(f"{binding} {label} was not rejected")
            if server_bind_cap < configured_bind_cap:
                if report["bind_cases"][f"{binding}_server_limit"]["outcome"] != "rejected":
                    raise AssertionError(f"server did not independently enforce {binding} cap")

    rendered = json.dumps(report, indent=2, sort_keys=True)
    if args.json_output:
        args.json_output.write_text(rendered + "\n")
    print(rendered)


if __name__ == "__main__":
    main()
