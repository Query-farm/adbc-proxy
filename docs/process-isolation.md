# Hard-termination worker profile

The in-process session actor isolates latency and cancellation, but Rust cannot
safely terminate a thread executing a stuck or crashing native driver. Deploy
drivers that require hard deadlines or crash containment in a process failure
domain.

## Profile

- Run one `adbc-proxy-server` process per driver/tenant failure domain. Do not
  mix an untrusted or historically unstable driver with unrelated tenants in
  the same process.
- Route a stateful session to its owning process for its lifetime. A process
  restart intentionally invalidates its connections, transactions,
  statements, uploads, and result cursors; clients must reconnect and decide
  whether application-level retry is safe.
- Set `server.driver_operation_timeout_seconds` to the soft actor deadline,
  keep `server.request_timeout_seconds` above it, and set
  `server.shutdown_grace_seconds` to the maximum graceful drain. The process
  supervisor must send SIGTERM first and SIGKILL after its own deadline. Its
  kill deadline should be slightly larger than the proxy drain deadline.
- Bound sessions, statements, results, HTTP bodies, and bind bytes per worker.
  The session actor queue is bounded and timed-out queued jobs are abandoned
  before execution.
- Treat abnormal exit as a failed worker, not as a retryable individual RPC.
  Never automatically replay commit, update, DDL, or other non-idempotent
  calls after worker loss.
- Export worker identity, target name, RPC status, and duration through the
  existing OpenTelemetry hooks. Keep SQL text and credentials out of process
  arguments, logs, and span attributes.

For Kubernetes, set `terminationGracePeriodSeconds` above
`shutdown_grace_seconds`, use one target/failure domain per Deployment, and let
the kubelet's final SIGKILL provide the hard boundary. For systemd, use
`TimeoutStopSec` and its default final `SIGKILL`. An HTTP gateway may route and
authenticate ahead of these workers, but the worker remains the owner of all
ADBC state.

This profile deliberately kills the whole worker rather than attempting to
recover a process after undefined behavior or a wedged FFI library. Finer
isolation would require a separate child-worker protocol and supervisor; it is
not equivalent to killing a Rust thread.
