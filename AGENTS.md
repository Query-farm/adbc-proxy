# AGENTS.md

## Project purpose

This repository implements an ADBC proxy as two cooperating products:

- `adbc-driver-proxy` is an ordinary ADBC 1.1 driver loaded by client
  applications.
- `adbc-proxy-server` owns the real downstream ADBC drivers and preserves
  database, connection, transaction, statement, and Arrow result state.

The wire contract is in `adbc-proxy-protocol` and is carried by VGI-RPC over
HTTP(S), TCP, mutual-TLS TCP, or raw Iroh QUIC streams.

## Non-negotiable invariants

- The public client surface must remain ADBC. Do not require applications to
  use a proxy-specific query API.
- Treat database results as pull-based Arrow streams, not continuous pushes.
  Keep the downstream cursor server-side and fetch one batch at a time.
- Bind every session and child handle to its authenticated principal. Never
  permit a caller to access another principal's handles.
- Preserve ADBC status codes, SQLSTATE, vendor codes, and error details across
  the wire whenever the downstream driver supplies them.
- Do not log SQL text, credentials, bearer tokens, connection strings, Arrow
  values, TLS private keys, or raw downstream error messages.
- Server-configured connection options are authoritative and are applied after
  caller options. A caller must not be able to override injected credentials
  or destinations.
- Plain TCP remains loopback-only by default. Authenticated non-loopback TCP
  requires mTLS with verified identities. Iroh authorization is based on the
  authenticated endpoint ID.
- Protocol changes must be backward-aware: keep method names, Arrow schemas,
  handle lifetimes, sequence/replay behavior, and resource limits explicit.
- Avoid unbounded buffering. New request or response paths need a documented
  limit, streaming/externalization strategy, cleanup behavior, and tests at and
  beyond the boundary.
- Do not claim multi-replica transparency while sessions are process-local.
  Transactions and live result cursors require affinity to their owning
  server process.

## Repository map

- `crates/adbc-proxy-protocol`: method constants, Arrow wire schemas, typed
  options, and structured errors.
- `crates/adbc-proxy-server`: configuration, authentication/authorization,
  session ownership, downstream driver loading, transport listeners, and
  telemetry.
- `crates/adbc-driver-proxy`: exported ADBC C ABI and transport clients.
- `validation`: Python C-ABI smoke, Driver Foundry conformance, load harness,
  and reproducible results.
- `docs`: operator-facing security and process-isolation guidance.

The local sibling `../vgi-rpc-rust` is the upstream transport implementation.
Keep generally reusable transport features there; keep ADBC lifecycle and
policy in this repository. Do not silently replace the crates.io VGI
dependencies with a local path dependency in committed manifests.

## Development workflow

Use Rust 1.97 or newer. Before handing off a Rust change, run:

```console
cargo fmt --all --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

For shell or Python changes, also run the relevant inexpensive checks:

```console
bash -n validation/run_external.sh
shellcheck validation/run_external.sh
python -m py_compile validation/*.py examples/*.py
```

External validation exercises the compiled C ABI through a real ADBC driver
manager. It requires installed downstream drivers and may require PostgreSQL:

```console
./validation/run_external.sh smoke sqlite
ADBC_PROXY_TRANSPORT=mtls ./validation/run_external.sh foundry sqlite -q
ADBC_PROXY_TRANSPORT=iroh ./validation/run_external.sh load duckdb \
  --workers 32 --iterations 50
```

Do not rewrite recorded validation claims unless you ran the named workload.
Store new machine-readable load evidence under `validation/load-results/` and
record environment, concurrency, workload shape, errors, latency, throughput,
and peak RSS in `validation/RESULTS.md`.

## Testing expectations

- Add a focused unit or integration regression test with every behavior fix.
- Exercise lifecycle cleanup on success, protocol error, timeout,
  cancellation, peer disconnect, and server shutdown where relevant.
- Transport work must cover HTTP, TCP, mTLS, and Iroh semantics where they
  differ. Do not infer persistent-stream behavior from HTTP tests.
- Test resource boundaries immediately below, at, and above configured limits.
- Concurrency tests must use independent ADBC handles unless the test is
  specifically checking invalid cross-thread or cross-owner behavior.
- For cancellation, distinguish request cancellation, ADBC statement or
  connection cancellation, and downstream-driver cancellation support.
- Treat zero application errors as necessary but insufficient for load tests;
  also inspect tail latency, fairness, memory growth, and cleanup after load.

## Change discipline

- Preserve unrelated work in this shared workspace. Inspect diffs before
  editing and keep changes scoped.
- Use `apply_patch` for source edits. Do not commit generated `target/`, Python
  virtual environments, credentials, certificates, or local service configs.
- Never weaken certificate validation, authentication, authorization, quotas,
  or message-size limits merely to make a test pass.
- Update the README and relevant files in `docs/` when behavior, configuration,
  security assumptions, deployment constraints, or validation status changes.
- Call out downstream limitations honestly. A downstream `NOT_IMPLEMENTED`
  response is preferable to incorrect emulation.
