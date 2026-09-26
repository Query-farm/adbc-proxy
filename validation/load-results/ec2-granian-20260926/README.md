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

# Granian WSGI comparison on EC2

Granian 2.8.3 achieved **52.97 verified queries/s** at eight clients, confirming
the 53.46 and 53.87 queries/s measured with 2.8.1,
versus 45.09 for the diagnostic Waitress output-lock workaround and 7.79 for
ordinary Waitress with the corrected one-second timeout. Its p99 latency was
182 ms, versus 215 ms and 1,453 ms respectively. The host change removes
the Waitress-specific scheduling problem; it does not eliminate the HTTP
request per batch, serialization, Python execution or process-isolation costs.

## Recorded results

| Host | Clients | Seconds / connection lifetime | Verified queries | Queries/s | p50 / p95 / p99 ms | Peak serving / backend RSS MiB |
| --- | ---: | --- | ---: | ---: | --- | --- |
| Waitress 3.0.2, corrected timeout | 1 | 30 / 10,000 queries | 1,131 | 37.30 | 25.1 / 30.3 / 31.5 | 104.9 / 91.4 |
| Granian 2.8.1 | 1 | 30 / 10,000 queries | 1,317 | 43.44 | 21.4 / 25.8 / 26.9 | 107.1 / 91.4 |
| Granian 2.8.1 | 8 | 30 / 10,000 queries | 1,622 | 53.46 | 140.2 / 172.8 / 183.4 | 136.2 / 731.3 |
| Waitress 3.0.2, corrected timeout | 8 | 30 / 10,000 queries | 244 | 7.79 | 928.6 / 1,355.3 / 1,453.0 | 124.5 / 731.5 |
| Waitress, diagnostic output-lock workaround | 8 | 30 / 10,000 queries | 1,368 | 45.09 | 166.0 / 202.6 / 215.1 | 127.7 / 730.9 |
| Granian 2.8.1, repeat | 8 | 30 / 10,000 queries | 1,634 | 53.87 | 138.8 / 172.8 / 185.2 | 140.4 / 731.2 |
| Granian 2.8.1, churn | 8 | 180 / 25 queries | 9,501 | 52.74 | 136.1 / 167.7 / 179.8 | 139.9 / 731.9 |
| Granian 2.8.3 | 1 | 30 / 10,000 queries | 1,313 | 43.34 | 21.4 / 26.1 / 27.1 | 107.2 / 91.4 |
| Granian 2.8.3 | 8 | 30 / 10,000 queries | 1,607 | 52.97 | 140.2 / 172.8 / 181.6 | 144.0 / 731.3 |
| Granian 2.8.3, churn | 8 | 60 / 25 queries | 3,151 | 51.92 | 136.1 / 167.7 / 179.8 | 147.7 / 732.0 |

All ten measured cases (22,888 verified queries) passed with zero unexpected
application errors, zero remaining
backend workers, normal supervisor exit and serving-process descriptor counts
returning to baseline (18 for Granian, 12 for Waitress). The 180-second Granian
run had fairness 0.99979 and maximum query latency 193.8 ms. Two child-resource
samples were incomplete while processes retired, so child RSS peaks are
observations rather than a guaranteed upper bound. Serving RSS grew 9.65 MiB
after the first 20 seconds; this does not establish a memory plateau.
Granian's additional supervisor used approximately 94.3–94.4 MiB at shutdown.
The 60-second 2.8.3 churn check likewise recovered all workers/descriptors and
had fairness 0.99923. Its serving RSS grew 5.32 MiB after the first 20 seconds;
one child-resource sample was incomplete. These runs provide no claim that
Granian reduces total deployment memory.

## Workload and controls

All runs used the same EC2 instance as the preceding investigations: 48 ARM
Neoverse-N1 cores, Amazon Linux 2023, Python 3.14.7 with the GIL, PyArrow 25.0.1,
Python VGI-RPC 0.47.1, ADBC driver manager 1.12.0 and Waitress 3.0.2. The
unchanged SDK revision is `9ca8f3621320bd19c93afe243fbf3f523872c763`; the
native driver SHA-256 is
`f441c8545ec3c43c13d25f026984c4f67c684e5f62a311b833f8cc90fbedbc2b`.
Neither native client-reuse patch was applied. Environment manifests record
versions, source hashes and the isolated checkout's older Git HEAD separately.

Each query generated and checked 4,096 rows with a 64-byte payload in eight
512-row Arrow batches. Independent ADBC handles owned independent isolated
backend processes. Authentication, SDK limits, structured error checks every
ten queries, normal result cleanup and connection cleanup remained enabled.
Queries are synthetic generation, not SQL engine execution. Short comparisons
used 30 seconds and a 10,000-query connection lifetime. Throughput includes
connection startup and expected error calls; query latency includes result
cleanup and value checking, but excludes those startup/error calls.

All measured cases ran sequentially, without task-owned builds or
profilers running concurrently. Focused pytest suites ran outside measurement
windows. Host method timers were disabled on every
host; client stage timers remained enabled. The Waitress patched comparison
retained its readiness counters because they are part of that diagnostic
wrapper. This is a comparison of complete host variants, not an isolated CPU
cost measurement for a single function. Other user-owned DuckDB unit tests
were observed on the shared machine and were not stopped. These are short
single-machine observations, not confidence intervals or capacity promises.

Granian uses the public server API with HTTP/1, one serving process, one Rust
runtime thread, a Python thread ceiling of `max(8, clients * 2)`, and admission
backpressure of `clients * 4 + 16`. The Python thread ceiling matches Waitress.
Sessions stay in their owning process. The supervisor is an additional
process: resource samples measure the serving process and its backend children,
and record supervisor RSS separately at shutdown. The two memory columns are
not a measurement of total deployment peak RSS.

## Compatibility finding

The initial unadapted 2.8.1 smoke attempt completed zero queries and reported
one `OperationalError`, then cleaned up normally. Its report is retained.
Grainlift's privacy WSGI wrapper calls `start_response` during generator
iteration. Granian captures status/headers before beginning iteration, so
the native client does not receive the expected capability headers. Source
inspection found the same eager header capture in 2.8.3. The retained
`compatibility-2.8.3.json` probe confirms that its unadapted wrapper captures
200 with no headers for a lazy synthetic 401 response; the adapter preserves
401 and the original content type. Response bytes are unchanged.

The diagnostic `soak.wsgi_compat` adapter advances only the first chunk before
returning the iterable. It retains at most one existing SDK-bounded response
chunk and delegates cleanup, including early close. A copied context follows
iteration across threads, preserving the privacy wrapper's context variable
and ensuring its token is reset in the context where it was created. No
complete query result is buffered. The adapter assumes this application's
headers are set before its first yielded chunk; it is not a general WSGI
compatibility implementation.

This adapter and Granian hosting remain optional diagnostics. Neither the SDK's
default host nor upstream VGI was changed. Real HTTP regression tests cover
authentication, malformed requests below/at the 2 MiB body limit, rejection
above it, subsequent server usability and graceful shutdown. Unit tests cover
headers, unchanged bytes, one-chunk prefetch, cross-thread context, early close
and failure before the first chunk. Full production qualification still needs
slow/abandoned clients, in-flight cancellation and shutdown under load, TLS
deployment, and sustained measurements with the actual database worker.

## Checks

On EC2 with Granian 2.8.3, all 19 focused pytest tests passed. Ruff, Ruff format,
strict mypy, isolated pydoclint, Bash syntax, ShellCheck and Python compilation
checks passed for the changed Python/shell files and repository-required
validation entry points. The optional Granian listener test skips when the
diagnostic dependency is not installed; adapter unit tests still run. No Rust
source or SDK source was changed, and no local load or build was performed.

## Reproduction and artifacts

See [the diagnostic instructions](../../diagnostics/README.md#granian-hosting-comparison).
The `v281-` artifacts retain the initial comparison; the `v283-` artifacts
record current patch-release verification. Host sidecars identify actual host
settings and version; client sidecars aggregate execute, fetch, verification
and cleanup times. Earlier smoke reports retain the old static transport label;
their companion host report identifies Granian correctly. `SHA256SUMS` covers
raw evidence files, excluding this narrative and the checksum file itself.
