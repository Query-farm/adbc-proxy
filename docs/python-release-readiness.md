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

<!-- Copyright (c) 2026 Query Farm LLC -->
<!-- SPDX-License-Identifier: Apache-2.0 -->

# Python service release readiness

This record separates locally completed engineering gates from publication,
remote CI, and deployment decisions. The scope is an authenticated HTTP service
with bounded pull-based Arrow results and optional process-isolated callbacks.
The SDK exposes every Grainlift protocol 0.3.0 operation; each backend implements
the capabilities it supports. This does not imply transparent multi-replica use.

The [typed-response migration](typed-protocol.md) changes the wire contract and
restores compatibility with published VGI-RPC 0.47.1. The candidate v4 results
below describe protocol 0.2 and do not validate these new source changes. A new
installed-package candidate and runtime matrix are required before release.

Current protocol 0.3 source validation: 344 SDK tests, 138 native Python
regression tests and 13 hello-world tests pass using registry VGI-RPC 0.47.1.
Ruff, formatting, strict mypy and isolated pydoclint pass. The SDK's
[installed-wheel matrix](https://github.com/Query-farm/grainlift-python/actions/runs/36244653629)
passes Linux/macOS on Python 3.13/3.14. Restoring the transport's stock semantics
passes 4,970 VGI-RPC tests; the `vgi-python` consumer retains its baseline result
of 2,704 passes, 104 skips and one pre-existing directory-parity test failure.

The original candidate v2 validated the query-only SDK. The new operation surface
adds transactions, preparation, binding, ingestion, metadata/statistics, partitions,
Substrait and typed options. Its source and installed-package evidence are tracked
separately in the [verification record](../validation/regression/VALIDATION.md).
Earlier load and TLS-edge measurements have not been rerun for these new paths.

| Gate | Current evidence | Status |
|---|---|---|
| SDK code quality | Ruff, format, strict mypy and isolated pydoclint across SDK source/tests; `py.typed` included | Passed locally |
| ADBC operation surface | All 31 wire methods routed; 283 SDK tests and 130 native regression tests; direct/isolated transaction, binding, ingestion and metadata coverage | Passed locally; SDK wheel CI passed all four platform/interpreter jobs |
| Native failure behavior | Deadlines, crashes, statement/connection cancellation, raw release, client death, shutdown and recovery in independent processes | Passed locally |
| Credential rotation | Atomic replacement, overlap/revocation, principal ownership and signed continuations | Passed locally |
| Reproducible packaging | Exact hashed wheels and dependency closure; all three wheels rebuild byte-for-byte from sdists; forbidden payload checks | Passed locally |
| Fresh installation | Candidate v4: 440 tests without failures or skips on each of Python 3.13.12 and 3.14.7, with installed-package imports verified | Passed locally on macOS arm64 |
| Load and cleanup | Eight clients, 180/300-second native Waitress runs, injected errors and connection churn; no unexpected errors, child or descriptor leak observed | Measured; memory/long-duration gate remains open |
| TLS edge | Real Caddy/Waitress HTTPS, certificate/hostname failures, verified Python RPC, authentication, limits, logs and draining | Passed locally; native HTTPS success remains unverified |
| Runtime CI | SDK, hello-world and combined candidate v4 matrices passed Linux/macOS × Python 3.13/3.14; v4 includes the synchronized timeout regression | Passed remotely |
| Publication | Public source repositories and candidate v4 prerelease available; package-index releases remain separate | PyPI release versions/dependency floors pending |
| Native ARM64 packaging | Separate Custom Test image pull returned registry `denied`; fallback Docker driver rejected a multi-platform build before compilation | Packaging infrastructure gate remains open |
| Target operations | Resource quotas, affinity, credential rotation and supervisor/shutdown contract documented | Real deployment/cgroup, certificate renewal and signal checks pending |

## Defects fixed during the gates

- VGI-RPC mistook zero-column bidirectional parameter exchanges for output-only
  producers. Candidate v4 patched explicit exchange direction; protocol 0.3 uses
  a fixed nonempty binding envelope and works with the published transport.
- Isolated result cleanup could replace a primary structured ADBC error if the
  backend also failed while closing its cursor. Cleanup now preserves that error.
- Statement close removes the cancellation target before invoking backend cleanup,
  preventing cancellation from entering a statement already being closed.
- Grainlift's native HTTP client ignored the configured timeout when using a
  shared Reqwest client. It now configures the timeout on that client.
- Isolated worker startup errors could leave parent pipe handles open.
- Cancellation could race with close and reach a connection already retired.
- Dynamically created transport loggers or newly attached handlers could bypass
  request-scoped log filtering.
- Waitress's exclusive request-size threshold rejected the SDK's exact inclusive
  boundary. The host setting is translated while preserving the SDK quota.
- Source distributions included ignored agent worktrees and test caches. Package
  allowlists and archive/wheel payload validation prevent that release leakage.

## Reviewable release artifact

[Candidate v4](../validation/release-results/candidate-v4/README.md) is the current
[GitHub prerelease](https://github.com/Query-farm/grainlift/releases/tag/python-candidate-v4),
archive SHA-256 `3fe5170b7fcd44eead68d995b4a2ee04900d6c580438d69aa2231b30f4d9f4ee`.
Its wheels and dependency locks are byte-identical to v3; only the native timeout
test and its fixture changed. The test now holds its callback until the client
observes a timeout, then verifies cleanup and recovery. Deliberately extending
the timeout past its watchdog makes it fail. Its
[combined runtime matrix](https://github.com/Query-farm/grainlift/actions/runs/36221096021)
passed all four platform/interpreter jobs against the exact reviewed archive.

[Candidate v3](../validation/release-results/candidate-v3/README.md) contains the
complete operation surface and its installed-package evidence. It is available as
a [GitHub prerelease](https://github.com/Query-farm/grainlift/releases/tag/python-candidate-v3),
archive SHA-256 `ad414e90374b83542a97ac544901ed62c54fc70c8f61a7726fe5623194a9c520`.
Its [combined runtime matrix](https://github.com/Query-farm/grainlift/actions/runs/36220548858)
exposed the timing-sensitive timeout test in one job and is separate from the passing
[SDK matrix](https://github.com/Query-farm/grainlift-python/actions/runs/36220380809).

[Candidate v2](../validation/release-results/candidate-v2/README.md) is locally
available at `target/python-release-candidate-v2/grainlift-python-candidate.tar.gz`,
SHA-256 `56c8ac49a858bdf199385338c7ebe08054625b7ae4c3631b2b1a87fe6d3f9c2b`.
It is available as a [GitHub prerelease](https://github.com/Query-farm/grainlift/releases/tag/python-candidate-v2)
and passed the [remote runtime matrix](https://github.com/Query-farm/grainlift/actions/runs/36218130106).
It contains the earlier query-only implementation. Full provenance,
hash-locked runtime/build requirements, JUnit results and summaries are retained
with that evidence. Those historical candidates include modified VGI-RPC code
under its old development version. Protocol 0.3 removes that dependency: new
candidates resolve the unmodified registry transport and build only the SDK and
example packages. Historical bundles must retain their original dependencies.

The [release instructions](../validation/RELEASE.md) define candidate creation,
configuration, matrix execution and publication. Linux evidence comes from remote
CI, separately from local macOS tests. Passing CI does not establish required
branch-protection checks or actual deployment behavior.

The independent [native ARM64 packaging job](https://github.com/Query-farm/grainlift/actions/runs/36220348135/job/108344790915)
failed before compiling Grainlift. Its container image access and fallback builder
configuration need repair; the successful Python/runtime checks do not establish
native package-release readiness.

## Operational limits to retain

Use [the deployment contract](python-deployment.md). A worker timeout or crash
invalidates its entire connection; clients reconnect explicitly. In-process
callbacks remain cooperative. Sessions need process affinity, worker memory
requires OS/container quotas, and the supervisor needs a final process-group
termination budget. Native clients adopt new bearer credentials by opening new
connections. Backend-specific unsupported capabilities and the native vendor-code limitation
remain explicit. None of these constraints should be hidden by an unrestricted
"production ready" label.

The [load record](../validation/RESULTS.md) includes latency, fairness, memory and
cleanup observations. Multi-minute local runs are useful regression evidence;
they are not hours-long stability tests or capacity planning for a real database
worker. Separate GC diagnostics found roughly stable tracked-object counts and
released Arrow buffers, but did not establish an RSS plateau. Before rollout,
run the candidate on the actual deployment resources and
worker with its real certificate chain and a measured, sustained workload.
