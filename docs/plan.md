# Implementation plan

## Product boundary

ADBC Proxy is a long-running, multi-tenant service. A client installs one
`adbc_driver_proxy` library. Downstream native drivers and their dependencies
are installed only on proxy workers.

```text
ADBC application
  -> adbc_driver_proxy
  -> VGI transport
  -> authenticated proxy service
  -> server-side ADBC driver manager
  -> target database
```

The protocol is transport-neutral. HTTP(S), TCP/mTLS, and raw Iroh are public
service bindings. TCP and Iroh are especially useful for direct worker access;
Unix, subprocess, and shared memory remain candidates for local gateways and
isolation.

## Invariants

1. Callers select an administrator-defined target. They never provide a native
   library path.
2. Every handle is random, scoped to one authenticated principal and session,
   and checked on every operation.
3. A session has one owning worker. Live connections and transactions do not
   fail over to another worker.
4. A result cursor is sequence numbered. Replaying a request cannot advance the
   downstream `ArrowArrayStream` twice.
5. Secrets, complete connection strings, and SQL text are not emitted to logs
   or traces by default.
6. Session expiration or worker drain cancels results, rolls back active work
   where possible, and releases native objects.

## Milestones

### M1: executable vertical slice

Status: complete.

- Versioned VGI protocol and reflection.
- Configured target catalog and dynamic ADBC driver loading.
- Authenticated session creation and principal-bound handles.
- Create statement, set SQL, prepare, execute query/update.
- Pull Arrow result batches with replay-safe sequence numbers.
- Commit, rollback, cancellation, and explicit close operations.
- Exported ADBC 1.1 client driver over HTTP(S), persistent TCP, strict mTLS
  TCP, and raw stateful Iroh.
- Structured per-action spans and optional OTLP/HTTP trace export.
- In-memory backend tests plus optional SQLite end-to-end test.

### M2: complete ADBC 1.1 surface

Status: complete for the ADBC 1.1 Rust traits and validated through the
exported C ABI. Database cancellation still depends on downstream support.

- Typed database, connection, and statement option get/set.
- Native VGI bind and bind-stream exchanges with independently enforced
  configurable client/server budgets (64 MiB default).
- Metadata: info, objects, table schema/types, statistics.
- Execute schema, execute partitions, and read partition.
- Substrait plans and rich error details.
- External smoke and ADBC Driver Foundry connection/query/statement suites.
- Remaining: Apache C++ `c/validation` fixture.

### M3: service hardening

Status: complete for a bounded single-process deployment; distributed worker
routing and trace-parent propagation remain.

- JWT/JWKS, per-principal target policy, strict SPIFFE workload mTLS, and Iroh
  endpoint-identity allowlists.
- Sliding session leases, quotas, admission control, and bounded cleanup.
- Remaining: worker pools by driver/tenant and protected worker-routing tokens.
- Graceful drain and explicit connection-loss semantics.
- Remaining: W3C OpenTelemetry parent propagation and downstream database
  spans.
- Replay, concurrent load, payload-boundary, and deterministic fault-injection
  tests are present; multi-hour soak and credential-redaction tests remain.

### M4: additional transports

Status: TCP/mTLS and Iroh complete.

- Raw VGI TCP for trusted local networks and mandatory mTLS for production.
- Raw stateful Iroh direct-worker transport with endpoint authorization.
- Unix/subprocess/shared-memory profiles for local use.
- Optional HTTP bootstrap followed by a negotiated direct data endpoint.

### M5: native VGI data plane and worker isolation

Status: complete for native streaming and thread isolation.

- TCP/mTLS/Iroh VGI producers
  emit downstream `RecordBatch` values directly.
- Runtime-schema VGI bind/bind-stream exchanges,
  bounded channel backpressure, explicit finish, and structured final errors.
- Owned/dedicated VGI stream leases: pooled QUIC streams for Iroh and a
  bounded dedicated-connection strategy for TCP/mTLS.
- Each downstream session is behind a bounded worker/actor so transport
  deadlines can respond independently and cancellation does not queue behind
  a blocking driver call.
- Use a worker process boundary when hard termination of a hung or hostile
  native driver is required.

See [the native streaming design](native-streaming.md).

## Acceptance criteria for M1

An unmodified ADBC application can load `adbc_driver_proxy`, connect to a named
server target, execute a multi-batch query through a server-installed driver,
consume it through `ArrowArrayStream.get_next`, commit or roll back a
transaction, and receive the original ADBC status, vendor code, SQLSTATE, and
details on failure. The downstream driver is absent from the client machine.
