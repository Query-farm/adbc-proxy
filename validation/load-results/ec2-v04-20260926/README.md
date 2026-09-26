<!-- Copyright (c) 2026 ADBC Drivers Contributors; Query Farm LLC. SPDX-License-Identifier: Apache-2.0 -->

# Protocol 0.4 benchmarks and profiling on EC2

All builds, tests, load and profiling ran remotely on 2026-09-26. The host has
48 ARM Neoverse-N1 cores, 92.6 GiB RAM, Amazon Linux 2023 and kernel 6.18.41.
Clients, server and downstream drivers share this instance and use loopback.
Measured runs were sequential. Existing unrelated services remained running
with low observed activity; this is not an isolated capacity laboratory.

Grainlift native source: `915d9eb0c2242bbc2fbbfd84348e91471b239e1c`, built with
Rust 1.97.1 in release mode. SDK source:
`9ca8f3621320bd19c93afe243fbf3f523872c763`, with registry VGI-RPC 0.47.1.
The follow-up runs use harness-only commit `5c34118`; the SDK, native binary,
workload and deadlines remain unchanged. Native tests use Python 3.13.15;
Python-worker tests use 3.14.7. See [environment.json](environment.json) for
binary/driver hashes, downstream libraries and the PostgreSQL image ID.

## Native downstream-driver load

Each worker owns independent ADBC handles. Dataset: 200,000 rows; each query
returns and verifies 2,048 rows with 128-byte payloads. Two warmup queries per
worker precede 50 measured iterations. The following 11,200 measured queries
all completed without errors.

| Backend | Transport | Sessions | Queries/s | p95 ms | p99 ms | Peak server RSS MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| SQLite 1.12.0 | HTTP | 32 | 1,064.4 | 31.6 | 32.2 | 182.0 |
| DuckDB 1.5.5 | HTTP | 32 | 3,349.6 | 9.6 | 17.5 | 1,041.5 |
| PostgreSQL driver 1.12.0 / PostgreSQL 14 | HTTP | 32 | 1,130.0 | 31.9 | 35.3 | 175.2 |
| DuckDB 1.5.5 | TCP | 32 | 602.6 | 53.0 | 53.7 | 1,071.2 |
| DuckDB 1.5.5 | mTLS | 32 | 605.4 | 52.9 | 53.6 | 1,131.0 |
| DuckDB 1.5.5 | Iroh | 64 | 576.7 | 151.8 | 186.6 | 1,708.1 |

Throughput ranges from -5.5% to +4.1% of the historical EC2 runs with the same
workload shapes. These are short, single repetitions with different source
revisions, not a statistically controlled optimization comparison. The load
sampler records observed peaks, not guaranteed instantaneous maximum RSS.
Each native server exited through the harness cleanup; the dedicated PostgreSQL
container was stopped and removed. Unrelated containers were left unchanged.

## Python-worker load

The native ADBC driver calls an authenticated Waitress host with one isolated
worker process per connection. Eight clients verify 4,096 rows per query, in
512-row batches with 64-byte payloads. Connections turn over every 25 queries;
an expected structured error is injected every ten queries within each
connection. Worker startup and operation deadlines remain 15 and five seconds.
Throughput includes connection churn; query latency excludes startup and the
intentional errors. A separate 20-second warmup preceded the measured runs.

| Run | Queries | Queries/s | p50 ms | p95 ms | p99 ms | Peak host / children RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 180 seconds | 767 | 4.24 | 398.6 | 8,799.3 | 9,817.1 | 126.2 / 613.5 |
| 300 seconds, retry | 700 | 2.30 | 3,253.2 | 3,969.5 | 4,213.7 | 126.0 / 613.2 |

Both completed with zero unexpected errors, fairness above 0.9998, zero workers
after recovery, 12 host descriptors before and after, and normal host shutdown.
The retry had zero incomplete descendant samples. Host RSS grew by about 5.2
and 3.9 MiB respectively after the initial 20 seconds. Variation between runs
and multi-second tails warrant investigation; passing the harness does not
establish acceptable performance.

The original five-minute attempt aborted in `psutil.num_fds()` while a child
was retiring, before it wrote its workload report. Its
[failure record](python-300s-initial-failure.json) is retained; no throughput or
latency is inferred for that attempt. Commit `5c34118` records child access
denials as incomplete samples instead of aborting, avoids partial child totals,
and keeps host-access failures fatal. Eleven focused tests, Ruff, formatting,
strict mypy and pydoclint passed on EC2 before the retry. No limits were relaxed.

The prior Python figures were measured on macOS using an older protocol and
modified transport. They are not an apples-to-apples baseline for these Linux
results. This campaign does not isolate the cost of typed records or compare
equivalent direct-ADBC and proxied calls.

## Memory and CPU profiles

The separate eight-client, 300-second GC diagnostic passed with 251 verified
queries and zero unexpected errors. Periodic collection reduces comparability
with uninstrumented runs. Tracked objects rose from 58,883 before startup to
about 63,000 during load and fell to 61,239 after shutdown. Final Arrow allocated
bytes were 768 and one host thread remained. RSS rose from 88.4 to 124.7 MiB.
This narrows the investigation but does not establish an hours-long plateau or
exclude native allocation growth and allocator retention.

The four-client, 180-second tracemalloc diagnostic passed with 64 queries and
zero unexpected errors. It ended with 1,359,764 live traced Python bytes, 768
Arrow allocated bytes and one thread. The largest recorded retained Python
allocation increases were import loading, `mimetypes`, and `linecache`; the
profiling code itself also contributes. Tracemalloc does not account for all
native allocations. Its 0.34 queries/s is instrumented behavior, not capacity.

The first CPU profile sampled all processes at 49 Hz. It fell up to 20.61 seconds
behind, and the workload recorded six unexpected errors. Its failed workload
and 3,346-sample profile are retained; do not use it for precise cost estimates.

A second profile sampled only the host at 9 Hz for 45 seconds using
`--nonblocking`, within a 60-second workload. That workload passed with 158
queries, zero unexpected errors and complete cleanup. The profiler collected
231 stacks but reported 68 failed stack reads and up to 1.72 seconds of lag.
These limitations prevent precise CPU accounting or a definitive root cause.
Repeated sampled locations include Arrow IPC reader construction, VGI Arrow
serialization, Waitress socket send/readiness, and producer-turn handling.
Investigate those paths and thread scheduling rather than attributing the
slowdown to the type-system changes without a controlled comparison.

See [cpu-summary.json](cpu-summary.json), the two `*.speedscope.json` profiles,
and the `python-*-diagnostic.json` allocation records. Profiles contain stack
locations and aggregate measurements, not local variables or database values.

## Reproduction and provenance

`run-benchmarks.sh` records the original sequence. That controller was paused
during the GC stage and terminated after its successful report, so `stages.tsv`
does not contain a GC exit-status line. `run-followup.sh` records the broad CPU,
allocation and five-minute retry sequence after the sampler fix. The final
host-only profile ran separately after that sequence:

```sh
# Start the same 60-second, eight-client soak; identify its spawned host PID.
py-spy record --pid "$host_pid" --nonblocking --rate 9 --duration 45 \
  --format speedscope --output python-cpu-light.speedscope.json
```

The scripts expect the isolated checkout layout and prepared Python environments
described in the environment record. The native sequence uses an ephemeral
loopback PostgreSQL 14 container and the installed user-level ADBC drivers.
The first remote build encountered a stale shared Cargo cache; a separate
task-owned cache produced the successful build without changing shared files.
The remote checkout and evidence remain under
`~/Development/grainlift-benchmark-20260926-v04`; no benchmark ran locally.

`SHA256SUMS` identifies the retained evidence. Historical reports are unchanged.
