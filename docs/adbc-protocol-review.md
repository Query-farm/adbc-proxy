<!--
  Copyright (c) 2026 ADBC Drivers Contributors
  Copyright (c) 2026 Query Farm LLC

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# ADBC 1.1 surface and Grainlift 0.4 protocol review

This review distinguishes an implemented ADBC entry point from a capability
implemented by a particular worker. A typed transport does not make every
backend support transactions, ingestion, metadata, cancellation, partitioned
execution, or Substrait. Unsupported worker hooks return an ADBC error; the
toolkit must not fabricate successful operations.

The reference is the pinned upstream [ADBC C header](https://github.com/apache/arrow-adbc/blob/616acfdfcea9b66956fdb3d11437b6cf24edbc39/c/include/arrow-adbc/adbc.h),
limited here to API revision 1.1, together with the checked-in Grainlift client,
protocol and server and the `adbc_core`/`adbc_ffi` 0.25.0 interfaces selected by
`Cargo.lock` at Git revision `616acfdfcea9b66956fdb3d11437b6cf24edbc39`.
This is a source and regression review, not an ADBC certification.

## Entry-point mapping

| ADBC entry points | Grainlift behavior | Worker capability boundary |
| --- | --- | --- |
| DatabaseNew, DatabaseInit, DatabaseRelease | Native database configuration and lifetime; remote session opens at ConnectionInit. | Python workers have no separate shared database-handle object. |
| DatabaseSetOption and Bytes/Int/Double variants | Native configuration stores typed options; backend options enter `OpenConnectionRequest.database_options`. | Authoritative server options cannot be overridden. |
| DatabaseGetOption and Bytes/Int/Double variants | Read native configuration values. | These getters do not discover live backend database defaults. |
| ConnectionNew, ConnectionInit, ConnectionRelease | `open_connection`, `close_connection`; principal-bound session and child cleanup. | `Worker.open_connection`, `Connection.close`. |
| ConnectionSetOption and Bytes/Int/Double variants | `SetConnectionOptionRequest` with a tagged typed value. | `Connection.set_option`. |
| ConnectionGetOption and Bytes/Int/Double variants | `get_connection_option` and a typed value response. | `Connection.get_option`; requested representation is checked. |
| ConnectionCommit, ConnectionRollback | `commit`, `rollback`. | Real backend transaction hooks; no transaction emulation. |
| ConnectionCancel | `cancel_connection`. | Cooperative backend cancellation, or isolated-process termination where configured. |
| ConnectionGetInfo, GetObjects, GetTableSchema, GetStatistics | Named request records preserve each filter and its nullability. | Corresponding metadata hooks return standard Arrow schemas. |
| ConnectionGetTableTypes, GetStatisticNames | Session request and lazy result handle. | Corresponding metadata hooks. |
| ConnectionReadPartition | Opaque descriptor request and lazy result handle. | Backend partition reader plus service ownership verification. |
| StatementNew, StatementRelease | `new_statement`, `close_statement`. | Independent backend statement and binding lifetime. |
| StatementSetSqlQuery, StatementSetSubstraitPlan | `set_sql_query`, `set_substrait_plan`. | SQL execution or a real Substrait implementation must be provided by the worker. |
| StatementPrepare, StatementGetParameterSchema | `prepare`, `get_parameter_schema`. | Backend preparation and parameter type discovery. |
| StatementBind, StatementBindStream | Bounded fixed-envelope exchange containing Arrow parameter batches. | `Statement.bind` or `bind_stream`; toolkit owns staged data until safe release. |
| StatementExecuteQuery | `execute`; the no-result form maps to `execute_update`. | Lazy Arrow query result or affected-row count. |
| StatementExecuteSchema | `execute_schema`. | Backend schema inference; prior results must be invalidated. |
| StatementExecutePartitions | `execute_partitions`. | Actual backend partition descriptors; wrapper signing does not create partition support. |
| StatementSetOption and Bytes/Int/Double variants | `SetStatementOptionRequest` with a tagged typed value. | Includes standard ingestion, incremental-result and progress option names when supported. |
| StatementGetOption and Bytes/Int/Double variants | `get_statement_option` and a typed response. | Backend getter and its declared representation. |
| StatementCancel | `cancel_statement`. | Backend cancellation support and ownership checks. |
| Driver initialization, error-detail and Arrow stream callbacks | Exported by the native ADBC adapter; not separate worker RPC methods. | Preserve structured errors and release callbacks across that adapter boundary. |

The native database configuration lifetime is intentionally different from
the backend connection lifetime. A backend needing a shared database pool,
database-scoped mutation, or discovery of backend database defaults needs an
explicit worker implementation; connection hooks alone do not promise those
semantics. See upstream [connection establishment](https://arrow.apache.org/adbc/current/cpp/api/group__adbc-connection.html).

## Typed contract and extension rules

Protocol name remains `org.queryfarm.Grainlift.v1`; version `0.4.0` changes
seven request signatures. Session initialization, connection/statement option
setters, and the four filtered metadata methods each take one named dataclass
request. Stock VGI serializes that request as `request: binary`, containing
one Arrow IPC record. Other simple method signatures remain unchanged.

Options carry a key and a value with a kind discriminator and nullable string,
bytes, int64 and float64 slots. Exactly the selected slot is populated. Binary
values are binary; integer values do not pass through floating point. Filters
are nullable strings or lists rather than JSON strings. Responses likewise use
the stock `result: binary` envelope and named typed records. SQL, plan bytes,
partition descriptors and Arrow result batches keep their distinct roles.

Binding uses one row of `batch_ipc: binary, finish: bool` per exchange. This
keeps the VGI input schema nonempty even when parameter data has zero columns.
The embedded stream carries one uncompressed parameter batch, including its
dictionaries. Transport compression is separate. Requests, decoded batches,
staged uploads, results and partitions remain subject to documented quotas.

Vendor option keys and valid unknown metadata codes are extension points.
Changing a record's required fields or types is a protocol change, requiring a
new version and paired client/server validation. A named dataclass does not
make arbitrary field additions backward compatible. The protocol must reject
incompatible versions rather than silently reinterpret their bytes.

## Semantics that need independent checks

Metadata null and empty values are distinct. Unset catalog/schema filters do
not restrict selection; empty names select the unnamed namespace. Empty table
or column names must not become wildcard patterns. A null table-type list is
unrestricted; an empty list matches no types. Search patterns retain `%` and
`_`. GetInfo accepts the entire uint32 domain and omits unknown codes. A null
code selection requests all information; an empty selection requests none.
See the upstream [metadata contract](https://arrow.apache.org/adbc/current/cpp/api/group__adbc-connection-metadata.html).

Statement execution and schema execution invalidate earlier results. Bound
arrays/readers transfer release responsibility to the driver. Unknown affected
row counts use `-1` at the C ABI. Early result release must perform cancellation
and resource cleanup appropriate to the backend; merely advertising a cancel
hook is insufficient. Cancellation and option getters require the applicable
thread-safety guarantees even though ordinary statement operations need not
be concurrent. See upstream [statement management](https://arrow.apache.org/adbc/current/cpp/api/group__adbc-statement.html).

## Findings and validation scope

- The review found that Python `execute_schema` retained an earlier result.
  The 0.4 implementation now closes that result before invoking the schema
  hook. SDK lifecycle regressions cover successful and failed schema inference.
- The SQLite test fixture previously treated an empty table-name filter as
  `%`. The fixture now preserves the distinction and handles metadata search
  patterns explicitly. This was a fixture bug, not evidence of an ADBC
  requirement to make empty strings act as wildcards.
- The pinned `adbc_core` 0.25.0 represents arbitrary information codes with
  `InfoCode::Other(u32)`. The wire must preserve that range and reject negative
  or larger integer values before calling the worker.
- Python driver manager 1.12.0 normalizes `get_info([])` to the null/all form
  and does not forward `get_objects(table_types=...)`. The conformance probes
  therefore call the public driver-manager C symbols with test-owned database
  and connection structs for these cases. They pass an explicit non-null
  zero-length info selection and null-terminated table-type lists. Arrow
  streams retain the normal driver-manager/PyArrow release ownership; no
  private Python object memory is inspected or reinterpreted.
- Driver-specific options can pass through all four standard value types.
  Implementing an option transport does not implement each standard option:
  read-only mode, isolation levels, incremental partitions, progress and
  ingestion policies depend on the backend.
- Metadata hooks must supply the canonical ADBC schemas and meanings. The
  generic result path checks batches against their declared schema; that is
  not a validator for every metadata field or backend interpretation.
- Native ADBC error details use the `adbc_ffi` 1.1 error adapter. Its private
  data sentinel occupies the legacy vendor-code slot; current Python driver
  manager observations may omit that numeric code when exposing rich details.
  Wire preservation alone must not be presented as complete C-ABI fidelity.
- Live sessions and cursors remain process-local. Signed partitions enforce
  ownership; they do not establish arbitrary cross-replica readability or
  backend-independent descriptor portability.

The independent regression oracles declare their own Arrow schemas and VGI
dataclasses rather than importing the SDK protocol. Coverage includes actual
SQLite transactions and ingestion, all four initialization/mutation option
types, null/empty metadata filters, unknown uint32 information codes, native
handle lifetimes, and direct and isolated workers. The Substrait fixture only
checks opaque plan delivery; it is not a Substrait engine. Results for the 0.4
revision are recorded only after its paired native and Python tests complete;
earlier candidate evidence does not validate the new contract.
