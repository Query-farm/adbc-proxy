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

# Native Python-worker soak

Run from `validation/regression` after building the native driver:

```sh
uv run python -m soak \
  --driver ../../target/debug/libadbc_driver_grainlift.dylib \
  --seconds 300 --clients 8 \
  --output ../load-results/python-isolated-http-8clients-300s.json
```

Use `.so` on Linux. This starts an independent Waitress host and process-isolated
workers. Every client owns independent ADBC handles, verifies all Arrow rows and
payload bytes, injects a recoverable structured error every ten queries, and
opens a fresh connection every 25 queries. It pulls 4096 rows in 512-row batches
with 64 payload bytes per row. Startup and worker callbacks have finite deadlines.

Reports include package versions and source/driver hashes; query throughput;
all-query p50/p95/p99/max latency; each client's progress, error counts and maximum
completion gap; parent/child/client RSS; process/descriptor samples; post-load
recovery; and graceful host exit. Errors include class names only. Quantiles use
a bounded logarithmic histogram with about one-percent resolution, not a list
that grows with query count. Samples are capped by the maximum one-hour run and
one-second sampling interval. Client count, batch size, payload and result rows
are bounded and validated before starting the host.

The pass flag requires zero unexpected errors, progress by every client, Jain
fairness at least 0.8, unchanged SDK sources during the run, graceful host exit,
zero remaining descendants after recovery, and parent descriptor return within
four of baseline. **Memory and tail-latency trends still require inspection**;
the pass flag is not a universal capacity or leak certificate.
Fresh-process startup and intentional errors are outside query latency, but are
included in throughput and each client's maximum completion gap.

The benchmark generates synthetic values and does not exercise a real downstream
database. Running other builds/tests on the same machine can perturb latency.
Record the actual environment and avoid treating local numbers as deployment
capacity. For longer campaigns use `--seconds 3600`; repeat after warmup and
compare windows before asserting a stable memory plateau.

For aggregate GC/object/Arrow memory diagnostics, use
[`validation/profile_python_memory.py`](../../profile_python_memory.py) with
this directory on `PYTHONPATH` and `GRAINLIFT_MEMORY_PROFILE` set to an output
JSON path. It accepts the same workload arguments and writes its final sample
before acknowledging host shutdown. Allocation tracing is optional via
`GRAINLIFT_TRACE_ALLOCATIONS=1`; it can substantially perturb latency and cause
normal deadlines to expire. Keep instrumented evidence separate from capacity
runs. [Recorded diagnostics](../../RESULTS.md) do not yet establish hours-long
memory stability.
