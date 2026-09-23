# Native VGI data plane

Bulk ADBC record batches travel as native VGI stream turns. The protocol does
not encode an Arrow IPC stream into an Arrow `Binary` value.

## Query results

`read_result` is a VGI producer whose runtime output schema is the downstream
ADBC reader schema. Each tick pulls and emits exactly one downstream
`RecordBatch`.

- HTTP uses signed continuation tokens and can resume each batch on another
  HTTP connection to the same process-local session owner.
- TCP and mTLS allocate a dedicated VGI connection for each active result.
- Iroh opens a dedicated QUIC bidirectional stream on the process-wide pooled
  authenticated connection.

The dedicated byte-stream client is owned by a small reader worker, avoiding a
self-referential `StreamSession<'_>` and preventing a live result from
monopolizing the connection used for unary ADBC calls. Dropping the ADBC reader
sends VGI cancellation and closes the server-side result handle.

## Parameter binding

`bind` and `bind_stream` are runtime-schema VGI exchanges. Exchange init carries
only the session ID, statement ID, and small encoded schema descriptor. Every
subsequent turn carries the caller's native `RecordBatch`. A metadata-marked,
zero-row final turn commits the upload and returns the downstream driver's
structured final result.

Each upload has a one-slot channel to an isolated staging worker. The worker
writes accepted batches incrementally to an anonymous Arrow IPC file and
acknowledges only after the write completes. This provides per-turn
backpressure and bounded heap use while still producing the owned
`RecordBatchReader` required by drivers that retain `bind_stream` input. The
local file is an implementation detail, not an Arrow-in-Arrow wire envelope.
Client and server cumulative upload budgets are independent; HTTP request size
is a separate per-turn limit.

HTTP continuation state contains only authenticated cursor identifiers. Live
upload state remains in the same principal-bound server session, so HTTP still
requires affinity for stateful ADBC sessions. TCP, mTLS, and Iroh retain the
exchange state on their persistent stream. Explicit cancellation removes the
upload and stops its worker.

## Values that remain binary

Genuinely opaque or small control values remain `Binary`: partition
descriptors, Substrait plans, byte-valued options, and encoded schema
descriptors. Bulk record-batch inputs and outputs do not.
