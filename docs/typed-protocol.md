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

# Typed Grainlift responses

Protocol 0.3.0 uses VGI-RPC's existing typed-dataclass unary encoding. The Python
service returns `ArrowSerializableDataclass` response objects; the Rust client
and server use corresponding typed structs. Response fields have one declared
shape per type, and serialization derives their Arrow schema. Applications
continue to use the ordinary ADBC driver and its existing API.

This is a breaking wire change from protocol 0.2.0. Upgrade the native driver,
Rust server and Python SDK together. The protocol name remains
`org.queryfarm.Grainlift.v1`; Grainlift checks the protocol version before handle
access, so mismatched 0.2/0.3 peers fail explicitly. Existing process-local handles do not
survive a server upgrade; clients must reconnect. There is no automatic fallback
to the old response layout.

## Unary responses

Each unary reply has the standard VGI-RPC outer schema: one non-null `result`
binary column and one row. Its value is an Arrow IPC stream containing the
single-row response record. This is the same envelope used by typed responses
in `vgi-python`. Query rows remain separate pull-based Arrow streams.

| Response type | Record fields |
| --- | --- |
| `OkResponse` | `ok: bool` |
| `SessionResponse` | `session_id: str` |
| `StatementResponse` | `session_id: str`, `statement_id: str` |
| `ExecuteResponse` | `result_id: str`, `rows_affected: int or null`, `schema_ipc: bytes` |
| `SchemaResponse` | `schema_ipc: bytes` |
| `ValueResponse` | `value: WireOptionValue` |
| `UpdateResponse` | `rows_affected: int or null` |
| `PartitionsResponse` | `rows_affected: int`, `schema_ipc: bytes`, `partitions: list[bytes]` |

Integers use Arrow int64. `schema_ipc` retains the explicit Arrow schema message
encoding required for ADBC result schemas. These bytes describe the dynamic
database result, independently of the fixed RPC response type.

`WireOptionValue` is a nested record with a non-null `kind` string and nullable
`string_value`, `bytes_value`, `int_value`, and `double_value` fields. Exactly the
field selected by the kind is populated; unknown kinds, multiple populated fields,
nonfinite doubles and out-of-range integers are rejected. Binary options and
partition descriptors are carried as Arrow binary values rather than JSON/base64
response strings. Request options and metadata filters retain their existing
validated JSON arguments in this version.

Typed responses do not relax response limits. The serialized record and its
outer envelope add overhead that must fit the applicable transport budget.
Malformed schemas, null required fields, unexpected row counts, extra nested
batches and oversized payloads are errors. The service must release any cursor
it allocated if response construction fails.

## Parameter upload turns

Binding uses a fixed nonempty input schema with `batch_ipc: binary` and
`finish: bool`, both non-null. Each turn contains exactly one row. A data turn
contains one Arrow IPC parameter batch, including its schema and any dictionary
messages; a finish turn has empty `batch_ipc` bytes and `finish=True`.

Nested IPC streams are uncompressed. Compression and decompression belong to
the transport; the binding decoder consumes the resulting raw IPC bytes through
the Arrow library, without a separate compression layer or a handwritten
FlatBuffer parser.

The declared parameter schema is still negotiated at bind initialization.
Decoded batches must match it, including metadata. Zero-row and zero-column
parameter batches remain valid data and are distinct from the finish signal.
The envelope keeps exchange direction unambiguous with published VGI-RPC 0.47.1.
Acknowledgements remain the exchange's one-row `ok: bool` batches.

Input messages, decoded batches and cumulative uploads remain bounded. Replay,
principal ownership, pending replacement, cancellation and spool cleanup retain
their existing contracts. Encapsulation does not make a disconnected upload
successful or change downstream binding semantics.

## Error and dependency compatibility

Grainlift interprets structured ADBC error JSON within the `AdbcError` error type.
The native client accepts the Rust server's raw JSON and the published Python
transport's `AdbcError: ` prefix. Parsing is confined to that error type and
preserves status, SQLSTATE, vendor code and details. Other VGI-RPC consumers keep
their existing error formatting.

The Python SDK uses published VGI-RPC without a Git or sibling-source override.
The explicit raw-batch unary return extension and global error-format change are
unnecessary. The new bind envelope also avoids depending on the zero-column
exchange patch. Release candidates build the two Grainlift Python packages and
resolve the transport from the package index with a reviewed dependency hash.
