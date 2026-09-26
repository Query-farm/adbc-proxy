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

# Python HTTP performance investigation on EC2

The low throughput is substantially explained by HTTP busy polling, rather
than the cost of generating the synthetic result. Two interacting conditions
were found in Waitress 3.0.2:

1. Our load and TLS-edge hosts supplied `asyncore_loop_timeout=0.1`.
   Waitress's `Adjustments` converts this option with `int`, producing zero.
   Its select loop therefore polls continuously even without ready sockets.
   Both test hosts now use the valid integer value `1`.
2. While a WSGI thread holds a channel's output lock during a socket write,
   Waitress can advertise that socket as writable. The event-loop thread
   cannot acquire the lock to flush it, but immediately polls it again.
   Suppressing this readiness while the lock is held, together with a positive
   poll timeout, removes the observed polling storm and improves throughput.

The second change is an **experimental diagnostic counterfactual**, implemented
only by `soak.diagnose`. It is not installed in the SDK or supported as a
production Waitress patch. The evidence supports a scheduling/starvation
explanation, but does not measure exclusive GIL wait time. Slow readers,
disconnects during writes, concurrent close/cancel, wakeups and shutdown need
dedicated coverage before adopting a production solution.

## Environment and workload

All measurements ran sequentially on the same EC2 instance as the
[original campaign](../ec2-v04-20260926/README.md): Amazon Linux 2023,
48 ARM Neoverse-N1 cores, 92.6 GiB RAM, Python 3.14.7, GIL enabled,
Waitress 3.0.2, PyArrow 25.0.1, registry VGI-RPC 0.47.1 and ADBC manager 1.12.0.
SDK source remains `9ca8f3621320bd19c93afe243fbf3f523872c763` and the native
release binary remains from `915d9eb0c2242bbc2fbbfd84348e91471b239e1c`.
Each workload JSON records the native binary and SDK source hashes.
The investigation began from harness commit `1bce178`.

The worker does not query a database or run the hello-world example. It
generates two Arrow columns: ordered integers and a repeated 64-byte binary
payload. Normally each query returns 4,096 rows in eight 512-row batches,
followed by end-of-stream. The native ADBC client verifies every returned value.
Each client owns independent handles, and each isolated connection has its own
worker process. Structured errors are intentionally exercised every ten queries.
Unless stated otherwise the probes used 15–30 seconds and churn every 10,000
queries, keeping connections open. Throughput includes startup and intentional
error checks; query latency excludes them. These short probes are mechanism
tests, not capacity estimates or statistically replicated benchmark claims.

## Measurements

| Probe | Clients | Queries/s | p50 ms | p99 ms | Unexpected errors |
| --- | ---: | ---: | ---: | ---: | ---: |
| Original host, 512-row batches, no timers | 1 | 3.16 | 267.7 | 623.7 | 0 |
| Original host, 4,096-row batches, no timers | 1 | 10.63 | 75.7 | 270.4 | 0 |
| Original host, 4,096-row batches, no timers | 8 | 1.14 | 4,009.2 | 6,028.9 | 0 |
| Original host, only one row/query, no timers | 8 | 1.58 | 3,776.9 | 4,384.8 | 0 |
| Timed Waitress, isolated workers | 8 | 0.36 | 12,589.7 | 13,632.9 | 0 |
| Timed Waitress, direct worker | 8 | 5.44 | 716.9 | 2,562.1 | 0 |
| Timed threaded wsgiref, isolated workers | 8 | 25.88 | 257.3 | 1,341.9 | 0 |
| Timed threaded wsgiref, direct worker | 8 | 29.19 | 219.4 | 1,302.4 | 0 |
| Correct integer timeout only, no timers, churn 25 | 8 | 10.41 | 449.1 | 3,629.5 | 0 |
| Correct timeout + skip locked outputs, timed and poll-counted | 8 | 42.83 | 174.5 | 226.0 | 0 |
| Both changes, 180 seconds, timed, churn 25 | 8 | 43.13 | 162.8 | 215.1 | 0 |

The three-minute combined diagnostic completed **7,807 verified queries**,
with p95 198.6 ms, maximum 237.6 ms and fairness 0.99994. All workers exited,
host descriptors returned to 12, and the host exited normally. Peak sampled
RSS was 138.2 MiB in the host and 729.9 MiB across worker descendants.
One descendant sample was incomplete during worker churn; it is flagged in
the raw report, so the sampled child-memory peak has that additional limitation.
Host RSS grew about 9.6 MiB after the initial 20 seconds: this does not establish
a memory plateau. This diagnostic result is approximately ten times the earlier
180-second 4.24/s baseline, with the same workload/churn but different
instrumentation. A supported fix still needs its own uninstrumented validation.

Wsgiref is solely a diagnostic host comparison, not a deployment recommendation.
The measurements show that isolation adds cost, but does not account for the
collapse to 2–4 queries/s. Reducing the result to one row also did not remove
the problem. Instrumentation amplifies the contention: do not equate the
0.36/s timed probe with uninstrumented production throughput.

The socket counters distinguish the two polling conditions. In `poll-skip`,
990,933 writable decisions were suppressed and **no writable events** reached
the event loop, yet it still made 14.4 million channel-readiness checks with
the zero timeout. In `timeout-one`, the positive timeout was insufficient:
1.70 million writable events occurred while requests were active. With both
changes, `timeout-one-skip` completed 1,300 verified queries, and readiness
checks fell to 331,922. Repeated reads after disconnect were zero, ruling out
that proposed explanation in these runs.

The original counting-only probe also illustrates waiting: its 380 POST
responses accumulated 166.2 seconds of wall time across threads but only
5.9 seconds of thread CPU. These overlapping timings include response
iteration and host writes between yields; they are not exclusive function
costs and must not be summed as a CPU breakdown.

## Additional overhead and negative results

The native HTTP transport creates a new VGI `HttpClient` for each unary RPC.
The VGI client caches capabilities for its own lifetime, so this construction
discards that cache. Counters observed **380 OPTIONS requests and 380 POSTs**
in `readiness-observe`. The installed Rust VGI-RPC 0.27.1 source confirms that
`post()` invokes cached capability discovery before sending the request.
Reusing the correctly scoped client/capability state is a separate optimization;
this investigation did not measure its potential speedup. Discovery and
response-budget checks must remain enforced.

Skipping locked outputs alone and reducing the Python thread-switch interval
to 100 microseconds did not solve the problem. All these negative results are
retained. The heavily instrumented `poll-events` run failed with eight client
errors and zero complete queries. `trace-observe`, including a py-spy snapshot
and three seconds of strace syscall summaries, failed with six errors.
Their timings are diagnostic only. The trace recorded substantial futex
activity and frequent thread creation; it does not establish how much time
was exclusively spent on the GIL, isolation deadlines or other locks.

The raw soak JSON's historical `workload.transport` string always names
Waitress and isolated workers. For `timed-wsgiref-*` and `*-direct-*`, the
companion `*-timings.json` and filenames identify the actual overrides.
No raw evidence has been rewritten to hide this harness-label limitation.
`diagnose-measured.txt` preserves the diagnostic source used for the final
measurements. Early probes preceded additions of read/poll counters; absent
sidecar fields identify those stages. The maintained diagnostic module adds
documentation, input validation, effective-timeout/source-hash reporting and
uses `1` as its default timeout; reproducing the old bug now requires the
explicit `0.1` override.

## Reproduce

Use the regression environment with the sibling SDK and a release native
driver. From `validation/regression`, the diagnostic command accepts the
ordinary soak arguments:

```console
GRAINLIFT_DIAGNOSTIC_OUTPUT=timings.json \
GRAINLIFT_DIAGNOSTIC_LOOP_TIMEOUT=1 \
GRAINLIFT_DIAGNOSTIC_READINESS=skip_locked \
  .venv/bin/python -m soak.diagnose \
  --driver ../../target/release/libadbc_driver_grainlift.so \
  --seconds 180 --clients 8 --output workload.json
```

Set the timeout to `0.1` to reproduce integer truncation. Set readiness to
`observe` to count without suppressing it. `GRAINLIFT_DIAGNOSTIC_POLL=1`
adds intrusive event counters. `GRAINLIFT_DIAGNOSTIC_HTTP=wsgiref` and
`GRAINLIFT_DIAGNOSTIC_WORKER=direct` select the alternate host/worker.
Authentication, resource limits, process deadlines and result verification
remain enabled. No production SDK, VGI runtime or native driver source was
modified for these comparisons.

## Change validation

All checks ran on EC2. Thirteen focused pytest cases passed, including a new
parameterized regression that constructs the real soak/TLS-edge hosts, checks
the parsed Waitress timeout remains positive, and exercises shutdown. Ruff,
formatting, strict mypy, pydoclint and all applicable pre-commit hooks passed.
Shell syntax, ShellCheck and Python compilation checks also passed, and all
recorded evidence checksums verified. No Rust source changed.

The final maintained diagnostic module completed a separate five-second
functional smoke: 200 verified queries, zero unexpected errors, effective
timeout `1`, zero remaining workers and normal shutdown. Its recorded source
hash matches the checked-in diagnostic module. This smoke validates the final
harness changes; its startup-heavy throughput is not a capacity claim.
