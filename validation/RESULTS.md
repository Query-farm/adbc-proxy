# Validation results

Last run: 2026-09-23 on macOS 15.6.1 arm64.

## Versions

| Component | Version/revision |
|---|---|
| ASF SQLite ADBC driver | 1.12.0, user-level `dbc` install |
| DuckDB ADBC driver | 1.5.5, user-level `dbc` install |
| ASF PostgreSQL ADBC driver | 1.12.0, user-level `dbc` install |
| MySQL ADBC driver | 0.4.0, user-level `dbc` install |
| Flight SQL ADBC driver | 1.9.0, user-level `dbc` install |
| DataFusion ADBC driver | 0.25.0, user-level `dbc` install |
| Trino ADBC driver | 0.4.0, user-level `dbc` install |
| PostgreSQL server | 14.21, disposable local cluster |
| Python ADBC driver manager | 1.12.0 |
| PyArrow | 25.0.1 |
| pytest | 9.1.1 |
| Python | 3.13.12 |
| `uv` | 0.11.7 |
| ADBC Driver Foundry validation | `3c67c7b9ea9a3e4ab714bad828e373b614c41910` |
| Apache Arrow ADBC source reviewed | `616acfdfcea9b66956fdb3d11437b6cf24edbc39` |

The drivers were installed or verified through the default `dbc` path. SQLite
was upgraded from 1.11.0 to 1.12.0 and DuckDB from 1.4.0 to 1.5.5.

```text
$ dbc install sqlite --level user --json
{"schema_version":1,"kind":"install.status","payload":{"status":"installed","driver":"sqlite","version":"1.12.0","location":"/Users/rusty/Library/Application Support/ADBC/Drivers","conflict":"sqlite (version: 1.11.0)","checksum":"f6189b9cf49f86b64df51c53490d4241de49a1c3848a6e4d7547149a213f3ac1"}}
```

## External C-ABI smoke tests

Command:

```sh
ADBC_PROXY_SKIP_BUILD=1 ./validation/run_external.sh smoke sqlite
ADBC_PROXY_SKIP_BUILD=1 ./validation/run_external.sh smoke duckdb
ADBC_PROXY_SKIP_BUILD=1 ./validation/run_external.sh smoke postgresql
```

Result:

```text
PASS ... -> proxy service -> sqlite ADBC
PASS ... -> proxy service -> duckdb ADBC
PASS ... -> proxy service -> postgresql ADBC
```

After the option-policy and downstream-URI changes, HTTP compatibility smokes
also passed locally against MySQL 0.4.0 with MySQL 8.4.11, Flight SQL 1.9.0
with SQLFlite v1.5.5, DataFusion 0.25.0, and Trino 0.4.0 with Trino 483. The
same pass reran SQLite, DuckDB, and PostgreSQL. Each smoke additionally proved
that disallowed database and runtime connection options return
`INVALID_ARGUMENT` without reflecting their values; SQLite supplied its real
database `uri` from the client alongside an independent `adbc.proxy.uri`.
Microsoft SQL Server remains assigned to the x86 Linux CI service because the
local host is arm64.

This passed through an independently installed Python driver manager and the
exported proxy dynamic library. It covered authentication rejection and
acceptance, named target routing, DDL, DML row counts, prepare, query results,
Unicode, binary and null values, Arrow record-batch binding, rollback and
commit visibility, downstream error status preservation, and resource close.

The smoke suite passed for all three downstream drivers over every persistent
byte-stream transport. The full SQLite Foundry suite was also run over each:

```text
                         SQLite smoke  DuckDB smoke  PostgreSQL smoke  SQLite Foundry
ADBC_PROXY_TRANSPORT=tcp:      passed        passed             passed  148 passed, 135 skipped
ADBC_PROXY_TRANSPORT=mtls:     passed        passed             passed  148 passed, 135 skipped
ADBC_PROXY_TRANSPORT=iroh:     passed        passed             passed  148 passed, 135 skipped
```

These runs used the exported proxy dynamic library. The TCP clients retained
one socket for each ADBC connection. The mTLS run used a generated CA and a
strict client X.509-SVID, exercising server-name validation, certificate-chain
verification, SPIFFE identity, and target authorization. The Iroh client used
a direct-address discovery hint and raw VGI Arrow framing over an authenticated
Iroh QUIC stream; it did not use `httpi://`.

## ADBC Driver Foundry

Command:

```sh
ADBC_PROXY_SKIP_BUILD=1 ./validation/run_external.sh foundry sqlite -q
ADBC_PROXY_SKIP_BUILD=1 ./validation/run_external.sh foundry duckdb -q
ADBC_PROXY_SKIP_BUILD=1 ./validation/run_external.sh foundry postgresql -q
```

Result:

```text
SQLite:     148 passed, 135 skipped, 0 failed
DuckDB:     127 passed, 156 skipped, 0 failed
PostgreSQL: 209 passed, 74 skipped, 0 failed
```

Focused `test_get_statistics or test_execute_schema` runs produced 5 passes on
DuckDB and 17 passes on PostgreSQL, with zero failures. This confirms these
operations cross the exported proxy C ABI and VGI/HTTP boundary; they are not
merely direct-driver checks.

That is **849 selected test invocations** across the three backends, with 484
passes and 365 explicit skips. Each backend selects 283 invocations.
`pytest --collect-only` reports 386 per backend
before Foundry's `pytest_collection_modifyitems` hook removes its 102
`test_show_queries` development cases and one interactive `test_repl` case;
the emitted node-id list and the actual run both contain 283. An earlier
collection reported 354 because it covered only query and statement modules,
before the 32 raw connection cases were added. It is a pre-filter count, not a
different executed suite.

The adapter imports the official Foundry connection, query, and statement test
classes. Passing cases include connection metadata/object discovery and
filters, table schema, SQL queries, parameter batch and stream binding,
prepare, parameter schema, transaction toggling, DML row counts, Unicode, and
Arrow stream results.

The skips are explicit capability/backend declarations:

- 119 invocations are generic query cases whose expected schemas or SQL syntax
  do not model SQLite type normalization. Examples include narrow integers
  becoming `int64`, `float32` becoming `double`, and SQLite temporal handling.
  Compatible `binary`, `float64`, `int64`, and `string` query/bind cases run.
- The ASF SQLite driver returns `NOT_IMPLEMENTED` for execute-schema and
  statistics.
- SQLite does not expose current/secondary schema options through this driver.
- Constraint subfeatures and catalog/schema mutation are not declared.
- The generic unknown-option policy is skipped: a proxy database must accept
  arbitrary downstream database options before it knows the selected driver's
  option namespace, and ASF SQLite returns `NOT_IMPLEMENTED` rather than
  Foundry's expected `NOT_FOUND` for some unknown getters.
- DuckDB runs statistics and execute-schema. Its skips primarily cover generic
  SQL/type cases and multi-row parameter binding that DuckDB 1.5.5 does not
  support, plus GetObjects expectations that do not match its metadata layout.
- PostgreSQL runs GetObjects, approximate statistics, execute-schema, parameter
  schemas, batch/stream binding, and transactions. Its query skips are generic
  binary, decimal, or temporal cases whose SQL syntax/schema expectations do
  not describe PostgreSQL. Statistics are preceded by `ANALYZE`.

The first unadapted query/statement audit produced `83 passed, 131 skipped, 38
failed`; the failures were SQLite dialect/schema assumptions. Enabling all new
proxy statement capabilities without SQLite query overrides produced `132
passed, 31 skipped, 89 failed`; those additional failures were the same type
normalization issue. The final adapter skips those cases instead of claiming
the generic expectations passed.

## Concurrent load validation

The exported C-ABI driver was exercised with one independent database,
connection, and statement per worker. Each query pulled and validated an Arrow
result before the next query on that session. The fixed workload used 200,000
source rows, 2,048 result rows per query, and a 128-byte payload per row.

| Backend | Transport | Sessions | Queries | Arrow data | Errors | Queries/s | p95 | p99 | Peak proxy RSS |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| DuckDB | mTLS | 32 | 1,600 | 437.5 MiB | 0 | 2,987 | 18 ms | 117 ms | 252 MiB |
| PostgreSQL | mTLS | 32 | 1,600 | 437.5 MiB | 0 | 382 | 283 ms | 396 ms | 72 MiB |
| DuckDB | Iroh | 32 | 1,600 | 437.5 MiB | 0 | 138 | 247 ms | 440 ms | 209 MiB |
| PostgreSQL | Iroh | 32 | 1,600 | 437.5 MiB | 0 | 136 | 590 ms | 929 ms | 61 MiB |
| DuckDB | mTLS | 64 | 3,200 | 875.0 MiB | 0 | 2,929 | 131 ms | 299 ms | 400 MiB |
| PostgreSQL | mTLS | 64 | 3,200 | 875.0 MiB | 0 | 434 | 389 ms | 529 ms | 99 MiB |

Two 30-second sustained profiles used 32 sessions, 1,024 rows per query, and a
64-byte payload. DuckDB over Iroh completed 12,608 queries and 935.8 MiB with
zero errors at 419 queries/s; PostgreSQL over mTLS completed 13,446 queries and
998.0 MiB with zero errors at 447 queries/s. Across the primary fixed and
sustained runs, 38,854 queries and approximately 5.3 GiB of Arrow batches were
validated without an application error.

These are local macOS measurements, not deployment capacity claims. Client,
proxy, and database shared one machine; DuckDB also executes inside the proxy
process, while PostgreSQL executes separately. The raw reports are in
[`load-results`](load-results/). Multi-hour soak, remote-network runs, and
hundreds-of-session testing remain outstanding.

After the protocol 0.2 native-stream/session-actor migration, short regression
runs completed with zero errors: DuckDB/HTTP ran 160 queries across 16 sessions
(p95 9.3 ms, peak RSS 120.3 MiB), PostgreSQL/HTTP ran 80 across 8 sessions
(p95 13.3 ms, peak RSS 35.5 MiB), and SQLite/Iroh ran 80 across 8 sessions
(p95 131.7 ms, peak RSS 42.1 MiB). These small runs are regression evidence,
not replacements for the larger capacity profiles above.

## Payload boundaries and fault injection

On 2026-09-23 the exported driver and SQLite 1.12 were exercised over HTTP,
TCP, mTLS, and Iroh with a 2 MiB client response budget. HTTP accepted the
below-budget result and rejected payloads at and above the nominal budget once
VGI envelope bytes were included. TCP, mTLS, and Iroh accepted all three,
confirming the HTTP response option does not constrain byte transports.

An HTTP run using the normal 256 MiB response budget accepted a payload 64 KiB
below the limit and rejected payloads at and 64 KiB above it; the exactly
256 MiB value encoded to 268,436,872 bytes. Those response results remain
applicable because query results were already native VGI producer batches.

After the native VGI exchange migration, overridden 256 KiB client/server bind
budgets again passed below/at/above boundary tests for both bind APIs over
HTTP, TCP, and Iroh. Below-limit native batches were accepted, at/above-limit
batches were rejected by the independent client guard, and every connection
remained usable. Multi-batch bind-stream was separately exercised over HTTP,
TCP, mTLS, and Iroh in the Rust transport integration suite.
Raw HTTP probes returned 413 for an oversized/truncated body and 408 at the
configured two-second request timeout; disconnect and every rejection were
followed by a successful query.

The prior nested-IPC heavy run reached a 288.7 MiB high-water mark and is now a
historical baseline, not the current wire architecture. Native bind exchanges
stage batches incrementally to anonymous files with one-turn backpressure; the
old monolithic Arrow `Binary` ceiling no longer applies to a complete bind
stream. Individual VGI messages remain subject to the VGI implementation and
transport request limits. A new multi-hour native-stream memory soak remains
useful follow-up work.

Nine deterministic server fault tests cover producer cancellation, ADBC
statement/connection cancellation, downstream `NOT_IMPLEMENTED`, principal
isolation, caller timeout, abandoned-stream lease cleanup, malformed and
replayed requests, terminal reader errors, structured error fields, in-flight
lease protection, shutdown cleanup, and quota reuse. They found and fixed
non-replay-stable terminal reader errors and idle reaping of active sessions.
Per-session bounded actors now keep blocking callbacks off RPC dispatch
threads, discard timed-out queued work before execution, and expose ADBC
`TIMEOUT` independently from explicit cancellation. Cancellation handles bypass
the actor, and shutdown tests verify that timed-out native work is detached
without joining it. Hard termination remains the process supervisor's role.

## Apache C++ validation library

The Apache `c/validation` source and its `DriverQuirks` contract were reviewed.
It is not a standalone executable: a driver-specific GoogleTest fixture must
be compiled and linked with the C driver manager, nanoarrow, and the validation
sources. That fixture was not built or run in this pass. Driver Foundry was
chosen as the immediately executable official external-driver-manager suite.

Adding the C++ fixture remains useful, especially for lifecycle/error cases
that Foundry does not cover. It should link the driver manager and initialize
the proxy through `driver`, `entrypoint`, `uri`, target, and bearer database
options; it must also declare SQLite's type and optional-feature quirks rather
than inheriting the permissive defaults.
