# Native VGI data-plane migration

The initial proxy protocol carries bulk bind data, and byte-transport result
batches, as nested Arrow IPC bytes in an Arrow `Binary` field. That made the
first implementation uniformly unary and replayable, but it is not the target
architecture: it encodes Arrow inside Arrow, adds copies and buffering, and
inherits the signed-32-bit offset ceiling of a single `Binary` value.

VGI-RPC already provides the right primitives. The next protocol version will
use them directly.

## Query results

`READ_RESULT` is already a VGI producer over HTTP. Its runtime output schema is
the downstream ADBC reader schema and each producer turn emits the next native
`RecordBatch`. TCP, mTLS, and Iroh should use the same producer instead of the
legacy unary `READ_RESULT_BATCH` method.

The Rust VGI client's `StreamSession<'_>` currently borrows its `RpcClient`.
The proxy cannot store that borrowed session in an ADBC `RecordBatchReader`
whose lifetime is independent of the call that opened it. A persistent byte
stream is also lockstep, so a live producer would monopolize the connection.
The migration therefore needs an owned stream/transport lease or a dedicated
VGI client per active result:

- Iroh reuses the process-wide endpoint and physical QUIC connection, opening
  a dedicated bidirectional QUIC stream.
- TCP and mTLS use a bounded connection pool or one dedicated connection per
  active result unless VGI gains multiplexing.
- HTTP keeps its continuation-token producer path.

Dropping the ADBC reader cancels the producer and releases its server-side
result handle. HTTP continuation state remains replayable. A disconnected
persistent stream fails that reader; it must never advance a different result.

## Parameter binding

`bind` and `bind_stream` will be runtime-schema VGI exchanges. Exchange init
identifies the authenticated session and statement; `StreamResult::exchange`
declares the actual ADBC input schema. Every exchange turn carries the caller's
native `RecordBatch`, without nested IPC.

The server needs a bounded channel into the worker that owns the downstream
statement. That worker supplies a channel-backed `RecordBatchReader` to the
ADBC driver. Each accepted batch receives an acknowledgement, providing
backpressure. An explicit finish control turn closes the channel, waits for the
driver's final bind result, and returns any structured ADBC error. Cancellation
closes the channel and invokes the independently stored statement cancel
handle.

VGI's current `ExchangeState` has `exchange` and `on_cancel`, but no successful
`on_finish` callback. Plain stream EOF cannot distinguish a committed finish
from a disconnect or return a final driver error, so the protocol needs the
explicit finish turn (or a future VGI finish hook).

HTTP exchanges serialize continuation state between requests. A live channel,
worker, or native statement cannot be serialized into that token. Until VGI
offers a single-request streaming upload, HTTP must use a sticky, leased
server-side exchange registry or bounded external staging. TCP, mTLS, and Iroh
can retain the exchange state on their persistent stream.

## What remains binary

Small or genuinely opaque ADBC values can remain `Binary`: partition
descriptors, Substrait plans, byte-valued options, and encoded schemas. Bulk
record-batch inputs and outputs must not be nested inside one `Binary` cell.

The version-0.1 configurable bind limits remain defence-in-depth while the
migration is underway. They are compatibility controls, not the intended
large-data API.
