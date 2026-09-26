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

# Python query latency breakdown on EC2

The earlier 160–200 ms query latency is not a fixed cost of generating the
result. The final unchanged-driver control averaged about **24.3 ms with one
client**, versus **209.0 ms with eight clients**. The service's throughput
increased little with concurrency, so requests spent much longer completing
the same sequence of RPCs. This supports a contention/queueing explanation;
these measurements do not separately quantify GIL wait, scheduler wait and
CPU work in each server function.

The strongest demonstrated improvement is reducing batch RPC count. Returning
the same 4,096 rows as one batch instead of eight increased throughput from
43.86 to 98.36 queries/s. Reusing native HTTP clients removed most redundant
capability probes and reduced single-client latency, but did not reliably
remove the eight-client throughput ceiling.

## Conditions

All loads, builds and checks ran on the same EC2 machine as the
[preceding investigation](../ec2-python-investigation-20260926/README.md):
48 ARM Neoverse-N1 cores, Amazon Linux 2023, Python 3.14.7 with the GIL,
Waitress 3.0.2, PyArrow 25.0.1, Python VGI-RPC 0.47.1 and Rust VGI-RPC 0.27.1.
SDK source remained `9ca8f3621320bd19c93afe243fbf3f523872c763`.
The remote checkout's HEAD is `1bce178`; its native source is identical to
`3d99412` before the experimental patches. `environment.json` records these
revisions, build command and the four measured binary hashes.

The first `isolated-*`, `direct-c8` and `batch4096-c8` cases used the previous
campaign's native binary. Subsequent control/reuse binaries were rebuilt with
the same `cargo +1.97.1 build --release -p adbc-driver-grainlift` command so
build configuration was not an intended experimental variable.

Normal HTTP cases ran sequentially for 20 seconds, with independent ADBC
handles, 4,096 rows/query, 64-byte payloads and 512-row batches unless noted.
Connections were held for up to 10,000 queries; intentional structured errors
were exercised every ten queries. Results were fully verified. Auth, limits,
timeouts, isolation and cleanup remained enabled. Every HTTP case used the
previous investigation's **experimental Waitress output-lock workaround** and
a valid integer poll timeout. The native driver and SDK release paths remain
unchanged. No builds or profilers overlapped normal measured cases.

These are short mechanism probes with visible run-to-run variation, not
confidence intervals or production capacity guarantees. Throughput includes
startup and expected-error checks. Query timers exclude those operations.

## Where query time goes

The client wrapper measures execution, batch reads, value verification and
reader close separately. These are disjoint client intervals, unlike the
nested server timers. Values below are mean milliseconds per completed query
from `control-final-c1` and `control-final-c8`.

| Client stage | One client | Eight clients |
| --- | ---: | ---: |
| Execute, including initial stream open and first-batch prefetch | 4.92 | 51.97 |
| Remaining batch pulls, including native/client decode | 15.22 | 122.03 |
| Detect end-of-stream | 1.69 | 13.05 |
| Close reader and release remote result | 1.60 | 20.70 |
| Verify all returned values | 0.90 | 1.22 |
| Obtain reader wrapper | 0.02 | 0.05 |
| Sum of measured intervals | **24.35** | **209.02** |

The eight-client control in this final pair was slower than earlier controls
(p50 206.7 ms versus 171.1–187.1 ms). It is retained rather than selecting only
the faster runs. Earlier client counters did not time reader close; their
interval sums must not be presented as complete query latency. The final
wrapper includes that missing network cleanup operation.

For this workload the native driver sets SQL, executes, opens the pull stream
and receives its first batch, resumes for remaining batches and exhaustion,
then releases the result. The initial batch is already buffered when the
Python iteration starts. Thus an eight-batch result performs seven additional
data-fetch round trips, not eight, during the timed batch iteration. Repeated
client construction also adds capability discovery before these requests.
The stream protocol and server cursor remain pull-based throughout.

## Lower layers

`soak.layers` directly calls the same worker statement API, verifies every
value, and closes every result. It excludes the HTTP/C ABI, service-level
authentication and quotas, connection startup and intentional error injection.
Both single-client microbenchmarks used ten warmups and 500 measured queries.

| Path | Mean ms/query | p50 ms | p99 ms |
| --- | ---: | ---: | ---: |
| Direct synthetic worker | 1.52 | 1.53 | 1.59 |
| Same worker through isolated process pipe | 5.19 | 5.20 | 5.41 |

This establishes that row generation and verification are a small fraction of
HTTP query latency. The pipe adds about 3.66 ms in this single-client comparison;
subtracting microbenchmarks from loaded HTTP latency would not produce a valid
exclusive CPU breakdown. The isolated microbenchmark reported one remaining
child: Python's resource tracker. The final cleanup smoke explicitly identifies
it, with no worker left over; the helper exits with the benchmark parent.

Removing process isolation from the HTTP case improved eight-client throughput
from 43.86 to 48.90 queries/s. Isolation has measurable cost, but does not account
for most of the end-to-end delay.

## Client reuse and batch-size counterfactuals

See [the diagnostic patches and runner](../../diagnostics/README.md).
Unary reuse retains at most one idle RPC client per connection without holding
its mutex during network operations. Full reuse additionally retains a client
for each result's continuation requests. Each client still performs capability
discovery initially and enforces its configured limits. Neither patch is
installed in the production native source.

| Case | Clients | Queries/s | p50 ms | p99 ms |
| --- | ---: | ---: | ---: | ---: |
| Original binary, isolated worker, eight batches | 8 | 43.86 | 169.4 | 221.6 |
| Original binary, direct worker, eight batches | 8 | 48.90 | 154.9 | 202.6 |
| Original binary, isolated worker, one batch | 8 | 98.36 | 71.3 | 113.8 |
| Rebuilt control, first run | 8 | 39.13 | 187.1 | 265.0 |
| Rebuilt control, repeat | 8 | 43.53 | 171.1 | 221.6 |
| Unary reuse, first run | 8 | 45.34 | 164.4 | 210.8 |
| Unary reuse, repeat | 8 | 44.91 | 166.0 | 210.8 |
| Unary reuse, one batch | 8 | 105.20 | 66.5 | 107.2 |
| Final control | 1 | 38.42 | 24.1 | 30.0 |
| Unary and result-client reuse | 1 | 47.91 | 19.2 | 24.1 |
| Final control | 8 | 35.84 | 206.7 | 273.1 |
| Unary and result-client reuse | 8 | 45.86 | 156.4 | 239.9 |
| Unary and result-client reuse, repeat | 8 | 41.15 | 178.0 | 247.2 |

All normal cases passed without unexpected errors and cleaned up their workers.
Single-client full reuse reduced the sum of client stages from 24.35 to 19.41 ms,
about 20%. It also reduced capability requests to approximately one per result
plus one per connection: the eight-client full-reuse run recorded 937 OPTIONS
requests and 10,731 POSTs, versus equal counts in the controls. Despite this,
the repeated eight-client rates overlap the control range. Removing discovery
and construction overhead therefore does not by itself resolve the host's
contention under load.

The larger-batch comparison keeps the data volume and validation identical,
and remains within the same one-MiB synthetic batch limit. It reduces the number
of RPC envelopes, server dispatches and stream continuations. It does not
disable pull semantics, authentication, replay protection, compression or quotas.

## Profiling limitations and next work

`control-profile` enabled cProfile with a thread-CPU timer at eight clients.
It produced seven client errors and internally inconsistent function totals
(one self-time exceeded the entire reported profile duration). It is retained
as a rejected diagnostic. The subsequent one-client wall-clock profile passed
187 queries without errors, but a built-in lock entry still had inconsistent
self/cumulative timing. Its elapsed times also include I/O and scheduling.
Neither profile is used to claim a percentage of CPU time or GIL wait for an
individual function. They show calls through HTTP middleware, Arrow IPC,
identity validation, compression and the isolated worker's timer/pipe path.

The decisive evidence comes from client-stage timing and controlled workload
changes. Priorities supported by these measurements are appropriately sized
Arrow batches, reducing work per HTTP/VGI batch turn, and evaluating process
scaling with explicit session affinity. The result-client reuse prototype is
useful for single-client latency but needs cancellation, restart, partial-read
and error-reuse coverage before promotion. No protocol or safety checks should
be removed to improve the figures.

## Evidence and validation

Raw workload/host/client JSON, the failed profiles, stage exit codes, source
snapshots and checksums are retained. `diagnose-thread-clock.txt` reproduces the
first profile implementation; `diagnose-wall-clock.txt` records the final
profile implementation. `latency-before-close-timer.txt` identifies the earlier
client timer that omitted close. The measured stream-reuse patch is preserved
in `native-full-measured.patch.txt`; the maintained patch differs only in Rust
formatting. Shared libraries stay on EC2 and are identified by SHA-256.

The formatted combined native counterfactual passed `cargo fmt --all --check`,
all 66 Rust workspace tests and `cargo clippy --workspace --all-targets -- -D
warnings` on EC2 with Rust 1.97.1. The remote native source and default library
were restored afterward. These tests supplement the experiments; they do not
close the prototype's untested lifecycle gates.

Fifteen focused Python tests passed, including regression coverage for profiler
cleanup on application failure and transparent reader-error propagation. Ruff,
formatting, strict mypy and pydoclint passed for the diagnostic modules. The
isolated cleanup smoke verified that the remaining helper is the resource
tracker. No SDK, upstream VGI implementation or normal native source was changed.
