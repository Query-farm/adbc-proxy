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

# HTTP latency counterfactuals

These patches are experimental inputs to the EC2 latency investigation. They
are not installed in the normal native driver or Python SDK. Apply them only
to an isolated checkout. Both patches are relative to the unchanged native
source at `3d99412`; the stream patch includes the unary patch.

- `http-client-reuse.patch` retains at most one idle unary HTTP RPC client per
  connection. It releases the pool mutex before network I/O; simultaneous
  operations can allocate independent clients, including cancellation calls.
- `http-client-and-stream-reuse.patch` additionally gives each live HTTP result
  its own client, retaining its capability cache across continuation requests.
  It preserves one-batch pulls, continuation tokens and ordinary result cleanup.

Authentication, protocol validation, capability discovery, response limits,
timeouts and replay behavior remain enabled. The optimization preserves cache
state between requests; it does not bypass capability discovery. These probes
do not establish production behavior for cancellation races, partial reads,
server restarts or failed client reuse. The experimental mutex `unwrap` calls
also need ordinary driver error handling before production adoption.

On the remote test machine, build each variant with the same Rust toolchain
and command, copying its shared library outside the tracked checkout:

```console
cargo +1.97.1 build --release -p adbc-driver-grainlift
```

Build an unchanged control as well, rather than attributing differences between
different build configurations to the patch. Place the resulting Linux
libraries in the evidence directory as `native-control.so`, `native-reuse.so`
and `native-full-reuse.so`. They are deliberately not committed. Apply one
patch to a clean native source file at a time, and reverse it after testing.

With the regression environment and sibling Python SDK installed, run the
sequential comparisons on EC2:

```console
bash validation/diagnostics/run_latency.sh /absolute/grainlift /absolute/evidence unary
bash validation/diagnostics/run_latency.sh /absolute/grainlift /absolute/evidence streams
```

The unary phase includes an intentionally retained rejected thread-clock
profiling probe; inspect its status and do not treat its numbers as capacity
evidence. The stream phase uses a single-client wall-clock call profile, whose
timings include I/O and scheduling waits. Every normal load case validates all
returned values, intentional structured errors and final cleanup. Profilers,
compilers and measured load cases must not overlap.

`python -m soak.layers` from `validation/regression` measures the synthetic
worker without HTTP; `--isolated` includes the worker pipe and its deadlines.
It uses ten warmup queries, bounded batch sizes, a bounded iteration count,
normal statement/result cleanup and full value verification. It excludes
connection startup and service-level authentication/quotas, so it is a lower
layer comparison rather than an end-to-end service benchmark.
