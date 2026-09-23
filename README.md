<p align="center">
  <a href="https://query.farm">
    <img src="https://query.farm/media-kit/logo/wordmark-adaptive.svg" alt="Query.Farm" width="280">
  </a>
</p>

<h1 align="center">ADBC Proxy</h1>

<p align="center">
  <a href="https://github.com/Query-farm/adbc-proxy/actions/workflows/ci.yml"><img src="https://github.com/Query-farm/adbc-proxy/actions/workflows/ci.yml/badge.svg" alt="CI status"></a>
  <a href="https://arrow.apache.org/adbc/current/"><img src="https://img.shields.io/badge/Apache%20Arrow-ADBC-00A4E4?logo=apachearrow&amp;logoColor=white" alt="Apache Arrow ADBC"></a>
  <a href="https://www.rust-lang.org/"><img src="https://img.shields.io/badge/Rust-1.97%2B-000000?logo=rust&amp;logoColor=white" alt="Rust 1.97 or newer"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="Apache 2.0 license"></a>
</p>

ADBC Proxy makes server-installed [ADBC](https://arrow.apache.org/adbc/current/)
drivers available to ordinary ADBC applications over a network. Applications
load the proxy's ADBC driver and continue to use the standard ADBC API; the
service owns the downstream database connection, statements, transactions,
and [Apache Arrow](https://arrow.apache.org/) result streams. The wire protocol
runs on [VGI-RPC](https://vgi-rpc.query.farm/).

```text
ADBC application
    -> libadbc_driver_proxy
    -> VGI-RPC over HTTP(S), TCP, mTLS, or Iroh
    -> adbc-proxy-server
    -> server-installed ADBC driver
    -> database
```

The project is pre-release. Build the client driver and server from source;
published binary packages are not available yet.

## Features

- Standard ADBC 1.1 client interface and C entrypoint
  `AdbcDriverProxyInit`.
- Server-side loading of [SQLite](https://www.sqlite.org/),
  [DuckDB](https://duckdb.org/), [PostgreSQL](https://www.postgresql.org/), and
  other ADBC drivers.
- SQL and [Substrait](https://substrait.io/) statements, prepared statements,
  parameter binding, transactions, metadata, statistics, partitioned results,
  and cancellation.
- Pull-based native Arrow record-batch streaming without nesting Arrow IPC
  inside Arrow values.
- HTTP(S), persistent TCP, mutual-TLS TCP, and authenticated
  [Iroh](https://www.iroh.computer/) transports.
- Static bearer-token or JWT/JWKS authentication for HTTP,
  [SPIFFE](https://spiffe.io/) identities for mTLS, and endpoint-key identities
  for Iroh.
- Per-principal target authorization, resource quotas, deadlines, session
  expiry, graceful shutdown, structured ADBC errors, and
  [OpenTelemetry](https://opentelemetry.io/) traces.

Capabilities still depend on the selected downstream driver. Unsupported
operations are returned as ADBC `NOT_IMPLEMENTED` errors.

## Quick start

The example below runs the proxy against SQLite on the same machine.

### 1. Install prerequisites

Install [Rust 1.97 or newer](https://www.rust-lang.org/tools/install) and
[`dbc`](https://docs.columnar.tech/dbc/), then install the downstream SQLite
driver:

```console
dbc install sqlite --level user
```

Only the proxy server needs the downstream driver. Client machines need the
proxy shared library instead.

### 2. Build the proxy

```console
git clone https://github.com/Query-farm/adbc-proxy.git
cd adbc-proxy
cargo build --release --workspace
```

The build produces:

- `target/release/adbc-proxy-server`
- `target/release/libadbc_driver_proxy.so` on Linux
- `target/release/libadbc_driver_proxy.dylib` on macOS

Windows builds are not yet covered by the project's CI matrix.

### 3. Start the server

The included development configuration defines a SQLite target, listens on
loopback, and maps the bearer token `development-token` to the principal
`developer@example.com`.

```console
cp adbc-proxy.example.toml adbc-proxy.toml
./target/release/adbc-proxy-server --config adbc-proxy.toml
```

The configuration can also be selected with `ADBC_PROXY_CONFIG`. Use
`ADBC_PROXY_SERVER_ID` to assign a stable server identifier for telemetry.

Check readiness from another terminal:

```console
curl --fail http://127.0.0.1:8080/readyz
```

### 4. Connect from Python

Install the standard
[Python ADBC driver manager](https://arrow.apache.org/adbc/current/python/api/adbc_driver_manager.html)
and [PyArrow](https://arrow.apache.org/docs/python/):

```console
python3 -m pip install adbc-driver-manager pyarrow
```

Then load the proxy driver just like any other ADBC driver:

```python
from pathlib import Path

import adbc_driver_manager.dbapi as adbc

proxy_driver = Path("target/release/libadbc_driver_proxy.dylib").resolve()

with adbc.connect(
    driver=proxy_driver,
    entrypoint="AdbcDriverProxyInit",
    db_kwargs={
        "uri": "http://127.0.0.1:8080",
        "adbc.proxy.target": "sqlite",
        "adbc.proxy.auth.bearer_token": "development-token",
    },
    autocommit=True,
) as connection:
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1 + ? AS answer", [41])
        table = cursor.fetch_arrow_table()
        print(table)
```

Use `libadbc_driver_proxy.so` on Linux. The repository also includes a
complete [Python example](examples/python_client.py), which can be run with:

```console
export ADBC_PROXY_DRIVER="$PWD/target/release/libadbc_driver_proxy.dylib"
export ADBC_PROXY_ENDPOINT="http://127.0.0.1:8080"
export ADBC_PROXY_TARGET="sqlite"
export ADBC_PROXY_TOKEN="development-token"
python3 examples/python_client.py
```

## Server configuration

The server reads a TOML configuration file. See
[`adbc-proxy.example.toml`](adbc-proxy.example.toml) for all resource limits
and transport sections.

Each target names an ADBC driver known to the server's ADBC driver manager and
may inject database or connection options. Server-configured options are
applied after caller-provided options, so callers cannot replace an injected
database URI or credential.

```toml
[targets.postgresql]
driver = "postgresql"
entrypoint = "AdbcDriverPostgresqlInit"
allow_client_database_options = false
allow_client_connection_options = false

[[targets.postgresql.database_options]]
key = "uri"
type = "string"
value = "postgresql://proxy_user:secret@database.internal:5432/app"
```

Supported option value types are `string`, `bytes` (base64 encoded), `int`,
and `double`. Do not commit credentials to source control; supply the runtime
configuration through your deployment's secret-management mechanism.

Set `allow_client_database_options` or `allow_client_connection_options` only
when callers are permitted to provide their own downstream options. Proxy
transport options are never forwarded. Configured server values still win on
duplicate keys.

### Authentication and authorization

For a local or controlled HTTP deployment, static tokens map bearer tokens to
principals:

```toml
[auth.static_bearer_tokens]
development-token = "developer@example.com"

[auth.target_permissions]
"developer@example.com" = ["sqlite", "postgresql"]
```

Production HTTP deployments can replace static tokens with a JWT issuer:

```toml
[auth.jwt]
issuer = "https://identity.example.com/"
audience = "adbc-proxy"
jwks_url = "https://identity.example.com/.well-known/jwks.json"
principal_claim = "sub"
```

Static-token and JWT modes are mutually exclusive. Once
`auth.target_permissions` contains an entry, unlisted principals are denied
all targets. See [Security and resource controls](docs/security.md) before
deploying the service.

## Client options

Pass these as ADBC database options when opening the proxy driver:

| Option | Purpose | Default |
| --- | --- | --- |
| `uri` or `adbc.proxy.uri` | Proxy endpoint using `http://`, `https://`, `tcp://`, `tls+tcp://`, or `iroh://` | required |
| `adbc.proxy.target` | Server-configured target name | required |
| `adbc.proxy.auth.bearer_token` | HTTP(S) bearer token | none |
| `adbc.proxy.request_timeout_ms` | Timeout for each RPC | `30000` |
| `adbc.proxy.max_response_bytes` | Maximum accepted HTTP response size | `268435456` |
| `adbc.proxy.max_bind_bytes` | Cumulative parameter-bind budget | `67108864` |
| `adbc.proxy.tls.ca` | CA bundle for `tls+tcp://` | required for mTLS |
| `adbc.proxy.tls.cert` | Client certificate chain for `tls+tcp://` | required for mTLS |
| `adbc.proxy.tls.key` | Client private key for `tls+tcp://` | required for mTLS |
| `adbc.proxy.tls.server_name` | TLS server name | endpoint host |
| `adbc.proxy.iroh.secret_key` | Stable Iroh client secret key | generated per process |
| `adbc.proxy.iroh.direct_address` | Direct Iroh `host:port` discovery hint | relay/discovery |

## Transports

| Endpoint | Authentication | Intended use |
| --- | --- | --- |
| `http://` / `https://` | Static bearer token or JWT | HTTP ingress, reverse proxies, and service meshes |
| `tcp://host:port` | None | Loopback-only development and trusted local routing |
| `tls+tcp://host:port` | Mutual TLS with a verified SPIFFE identity | Direct production TCP |
| `iroh://<endpoint-id>` | Cryptographic Iroh endpoint identity | Direct authenticated QUIC connectivity |

The server's HTTP listener is plaintext. Terminate TLS in a reverse proxy,
sidecar, or service mesh and keep the server listener on loopback. Setting
`server.allow_insecure_remote = true` acknowledges a plaintext remote bind; it
does not add TLS.

Plain TCP cannot be used when authentication is required. Production TCP uses
the `[tcp.tls]` server configuration and the four `adbc.proxy.tls.*` client
options. Iroh servers map allowed client endpoint IDs to principals in
`iroh.principals`; persist the server secret-key file so its endpoint ID stays
stable.

## Deployment model

ADBC connections are stateful. A server process owns each connection,
transaction, statement, upload, and result cursor for its lifetime. HTTP
requests belonging to a session must therefore reach the same server process.
Use connection/session affinity at ingress, or route clients directly over
mTLS TCP or Iroh.

A worker restart invalidates its live sessions. Do not automatically replay
commits, updates, DDL, or other non-idempotent operations after a connection
loss. Native drivers also share the server process; isolate drivers or tenants
into separate workers when crash containment or hard execution deadlines are
required. See the [process-isolation profile](docs/process-isolation.md).

## Observability and health

Every RPC action emits a structured tracing span with the method, authenticated
principal, status, duration, and Arrow batch/row counts. SQL text, credentials,
tokens, connection strings, Arrow values, and raw downstream error messages
are not recorded.

Set `OTEL_EXPORTER_OTLP_ENDPOINT` or
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` to enable OTLP/HTTP trace export. Standard
OpenTelemetry headers and timeout environment variables are honored. Set
`OTEL_SDK_DISABLED=true` to disable export explicitly.

The HTTP listener exposes unauthenticated liveness and readiness probes:

- `GET /healthz`
- `GET /readyz`
- `GET /health` (VGI health endpoint)

## Development and validation

Run the Rust quality gates with:

```console
cargo fmt --all --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

The external harness loads the compiled C ABI through the Python ADBC driver
manager and tests real SQLite, DuckDB, and PostgreSQL drivers:

```console
dbc install "sqlite=1.12.0" --level user
./validation/run_external.sh smoke sqlite
./validation/run_external.sh foundry sqlite -q
ADBC_PROXY_TRANSPORT=mtls ./validation/run_external.sh foundry sqlite -q
ADBC_PROXY_TRANSPORT=iroh ./validation/run_external.sh load sqlite \
  --workers 32 --iterations 50
```

See the [validation guide](validation/README.md) for prerequisites, transport
selection, payload-boundary testing, fault injection, load testing, and the
[ADBC Driver Foundry](https://adbc-drivers.org/) suite.

## Repository layout

- `crates/adbc-driver-proxy`: client-side ADBC shared library.
- `crates/adbc-proxy-server`: proxy service and downstream driver manager.
- `crates/adbc-proxy-protocol`: typed ADBC-over-VGI wire contract.
- `examples`: client examples.
- `validation`: external conformance, fault, payload, and load tests.
- `docs`: security and process-isolation guidance.

## License

Copyright 2026 [Query Farm LLC](https://query.farm).

[![Built with Query.Farm](https://query.farm/media-kit/shields/built-with-query-farm.svg)](https://query.farm)

Licensed under the [Apache License, Version 2.0](LICENSE).
