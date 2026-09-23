# Feasibility assessment

## Conclusion

An ADBC-to-ADBC proxy is feasible, and VGI-RPC is a better fit than making
Flight SQL the proxy's public contract. The client can remain an ordinary ADBC
client: it loads one proxy driver, while the service loads the selected native
ADBC driver and owns the real connection, transaction, statements, and result
cursors.

Flight SQL is a useful database protocol, but it defines its own service model.
Using it would require translating ADBC operations and driver-specific options
into the subset represented by Flight SQL. VGI-RPC can directly express the
ADBC object lifecycle as typed Arrow RPC methods, while retaining Arrow IPC for
data and supporting HTTP, TCP, Unix sockets, subprocess, Iroh, and shared
memory.

## State and result flow

HTTP does not make the design stateless. It makes the transport connections
interchangeable. The service returns opaque identifiers for a logical session,
statement, and result; every operation carries those identifiers and is bound
again to the authenticated principal. A session is pinned to one worker for
its lifetime.

ADBC query output is a pull-based `ArrowArrayStream`. It does not continuously
emit batches. The proxy stores that stream on the worker and advances it one
batch for each client pull. VGI implements that as continuation requests over
HTTP and lockstep ticks over a persistent byte-stream transport. A sequence
number plus a one-batch replay cache prevents an HTTP retry from advancing a
non-rewindable database cursor twice.

## Transport implications

- HTTP(S) is viable for the public service because logical state is explicit,
  authentication is naturally per request, and intermediaries understand it.
  Deployment routing must keep a live session on its owning worker.
- Raw TCP can reduce per-batch request overhead, but plain VGI TCP provides no
  encryption or authentication. The service restricts it to loopback unless
  explicitly acknowledged; production TCP uses strict mutual TLS and SPIFFE
  workload identities.
- Raw Iroh carries the same stateful VGI framing on long-lived authenticated
  QUIC streams. The cryptographic endpoint key proves peer possession; an
  explicit endpoint-to-principal allowlist supplies deployment authorization.
- Unix, subprocess, and shared-memory transports are valuable for a local
  gateway or for isolating incompatible native drivers. They use the same
  application protocol.
- The protocol must not use HTTP cookies or HTTP-only sticky-session state as
  its source of correctness. Transport affinity is a deployment optimization;
  the explicit session capability remains authoritative.

## Security and credential policy

Callers select a configured target name, never a native library path. A target
decides independently whether client database and connection options are
accepted. Administrator-configured options are merged last, so injected
credentials and database URIs cannot be replaced by the caller. Session and
child handles are random and principal-bound.

The implementation supports static bearer authentication for development,
JWT/JWKS verification for HTTP deployments, strict mTLS/SPIFFE identity for
TCP, and endpoint-key authorization for Iroh. It enforces per-principal target
ACLs, principal-bound handles, request/session/statement/result limits, and
lease expiry. HTTP TLS may terminate at a reverse proxy, sidecar, or service
mesh; plain TCP is loopback-only unless the operator explicitly acknowledges
an insecure remote bind. Secret-manager integration and protected
worker-routing tokens remain deployment work.

## Operational constraints

- A live transaction cannot transparently fail over. Loss of the owning
  worker invalidates the session.
- Native drivers may have incompatible dependencies or poor crash isolation;
  separate worker pools or subprocess workers are appropriate boundaries.
- Long-lived idle sessions and abandoned results are bounded by quotas and a
  background lease reaper.
- Bind streams currently use bounded Arrow IPC buffering rather than a fully
  streaming upload path.
- Cancellation must use an independent transport request and a downstream
  ADBC cancel handle so it is not blocked behind the executing statement.

## Research basis

The implementation was checked against:

- [Query-farm/vgi-rpc-rust at `03632bc`](https://github.com/Query-farm/vgi-rpc-rust/tree/03632bc0eb457ee72e17e1249f8aee73e2989fe4), including its HTTP, TCP, Unix, subprocess, Iroh, shared-memory, authentication, continuation, and OpenTelemetry implementations.
- [Apache Arrow ADBC at `616acfdf`](https://github.com/apache/arrow-adbc/tree/616acfdfcea9b66956fdb3d11437b6cf24edbc39), including the Rust ADBC 1.1 traits, FFI exporter, driver-manager discovery, structured errors, and cancellation handles.
- [OpenTelemetry Rust OTLP exporter guidance](https://github.com/open-telemetry/opentelemetry-rust/tree/main/opentelemetry-otlp), used for the optional OTLP/HTTP trace pipeline.

The workspace pins the ADBC Git revision and VGI-RPC 0.27.0 so the protocol
implementation is reproducible against the interfaces that were reviewed.
