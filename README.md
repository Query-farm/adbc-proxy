# ADBC Proxy

ADBC Proxy is a network service and an ADBC driver that make server-installed
ADBC drivers available to ordinary ADBC applications. Applications load only
the proxy driver. The service authenticates the caller, authorizes a configured
target, opens the downstream driver, and preserves ADBC connection, statement,
transaction, and result-stream state.

The wire protocol is defined as typed VGI-RPC methods over Arrow IPC. The
driver and service support HTTP(S), persistent TCP, mutual-TLS TCP, and raw
stateful Iroh transports.

The current build is a single-worker production candidate. It implements the
ADBC 1.1 connection and statement surface, explicit remote handle lifecycle,
authentication and authorization, resource limits, and external validation.
The implementation status and remaining distributed-deployment work are in
[the implementation plan](docs/plan.md). The architectural tradeoffs and
research findings are in the
[feasibility assessment](docs/feasibility.md).

Database results are not treated as an endless push stream. A downstream ADBC
`execute` returns a pull-based Arrow stream. The proxy keeps that cursor on its
owning worker and advances it only when the client asks for the next batch.
Over HTTP, VGI continuation tokens turn each pull into a new request. TCP and
Iroh keep one VGI byte stream associated with the ADBC connection and use a
sequence-numbered unary pull for each downstream batch.

## Workspace

- `adbc-proxy-protocol`: stable method names, Arrow schemas, typed option and
  structured ADBC error representations.
- `adbc-proxy-server`: authenticated VGI service, target policy, session and
  handle lifecycle, and dynamic downstream ADBC driver loading.
- `adbc-driver-proxy`: exported ADBC 1.1 client driver.

## Development

```console
cargo fmt --all --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
./validation/run_external.sh smoke
./validation/run_external.sh foundry -q
ADBC_PROXY_TRANSPORT=mtls ./validation/run_external.sh load duckdb \
  --workers 32 --iterations 50
```

The external tests load the exported C ABI with the Python ADBC driver manager,
cross VGI over the selected transport, and execute against the independently installed
SQLite, DuckDB, and PostgreSQL ADBC drivers. Pass a backend after the mode, for
example `./validation/run_external.sh foundry postgresql -q`. See
[the latest validation results](validation/RESULTS.md).

The server configuration format is shown in
[`adbc-proxy.example.toml`](adbc-proxy.example.toml).

## Run the service

Install the downstream ADBC driver only on the server and make it discoverable
by the ADBC driver manager. Then:

```console
cp adbc-proxy.example.toml adbc-proxy.toml
cargo run -p adbc-proxy-server -- --config adbc-proxy.toml
```

Build the client-side ADBC shared library with:

```console
cargo build -p adbc-driver-proxy --release
```

### Python client

Python uses the standard ADBC driver manager and loads the proxy shared
library. It does not install the selected downstream driver:

```console
python -m pip install adbc-driver-manager pyarrow
export ADBC_PROXY_DRIVER="$PWD/target/release/libadbc_driver_proxy.dylib"
export ADBC_PROXY_ENDPOINT="https://adbc.example.com"
export ADBC_PROXY_TARGET="postgresql"
export ADBC_PROXY_TOKEN="..."
python examples/python_client.py
```

Use `libadbc_driver_proxy.so` on Linux. The complete example is
[`examples/python_client.py`](examples/python_client.py).

An ADBC application loads `adbc_driver_proxy` and supplies these database
options:

- `uri` or `adbc.proxy.uri`: an `http://`, `https://`, `tcp://`,
  `tls+tcp://`, or `iroh://<endpoint-id>` endpoint
- `adbc.proxy.target`: administrator-defined target name, such as `sqlite`
- `adbc.proxy.auth.bearer_token`: HTTP(S) bearer token
- `adbc.proxy.tls.ca`, `.cert`, `.key`, and `.server_name`: mTLS files and
  verified server name for `tls+tcp://`
- `adbc.proxy.iroh.secret_key`: optional stable client endpoint secret
- `adbc.proxy.iroh.direct_address`: optional `host:port` discovery hint

Other database options, plus connection options supplied during connection
creation, may be forwarded when the selected target permits them. Configured
server options are applied last, so a caller cannot replace an injected URI or
credential.

Each RPC action emits a structured span with method, authenticated principal,
status, duration, and Arrow batch/row counts. Span-close records are written to
the normal tracing output. Set `OTEL_EXPORTER_OTLP_ENDPOINT` (or the
trace-specific equivalent) to enable batched OTLP/HTTP export; standard OTLP
headers and timeout environment variables are honored by the exporter. SQL,
connection strings, bearer tokens, and full exception messages are not added
to these spans.

The HTTP listener is always available for health checks and HTTP RPC. Optional
TCP and Iroh listeners share the same session manager. Plain TCP is restricted
to loopback by default. Production TCP uses mandatory client-certificate mTLS
and strict SPIFFE workload identities. Raw Iroh authenticates the remote
endpoint key and maps configured endpoint IDs to application principals.

## Current scope

Implemented now: dynamic server-side driver loading; named target policy;
principal-bound sessions; typed options; all connection metadata calls;
SQL/Substrait statements; prepare; batch and stream binding; query, update,
schema, and partition execution; commit/rollback; connection and statement
cancellation; multi-batch result reads; replay-safe sequence numbers;
structured ADBC errors; static bearer or JWT/JWKS authentication; target ACLs;
quotas and lease reaping; graceful shutdown; health probes; and optional OTLP
trace export. Unsupported downstream capabilities remain downstream
`NOT_IMPLEMENTED` errors instead of being emulated.

This is suitable for a bounded single-process deployment. It is not
yet a transparent multi-replica service: live sessions are process-local and
need ingress affinity, native drivers share the service process, and worker
loss invalidates transactions. Parameter streams are buffered with a 64 MiB
cap, and database-level cancellation is still the ADBC default no-op. See
[security and resource controls](docs/security.md) before deployment.
