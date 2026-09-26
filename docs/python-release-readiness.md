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
The SDK exposes every Grainlift protocol 0.2.0 operation; each backend implements
the capabilities it supports. This does not imply transparent multi-replica use.

The original candidate v2 validated the query-only SDK. The new operation surface
adds transactions, preparation, binding, ingestion, metadata/statistics, partitions,
Substrait and typed options. Its source and installed-package evidence are tracked
separately in the [verification record](../validation/regression/VALIDATION.md).
Earlier load and TLS-edge measurements have not been rerun for these new paths.

| Gate | Current evidence | Status |
|---|---|---|
| SDK code quality | Ruff, format, strict mypy and isolated pydoclint across SDK source/tests; `py.typed` included | Passed locally |
| Native failure behavior | Deadlines, crashes, statement/connection cancellation, raw release, client death, shutdown and recovery in independent processes | Passed locally |
| Credential rotation | Atomic replacement, overlap/revocation, principal ownership and signed continuations | Passed locally |
| Reproducible packaging | Exact hashed wheels and dependency closure; all three wheels rebuild byte-for-byte from sdists; forbidden payload checks | Passed locally |
| Fresh installation | Historical candidate v2: 240 tests without skips on Python 3.13.12 and 3.14.7; new operation coverage requires its own candidate | v2 passed locally on macOS arm64 |
| Load and cleanup | Eight clients, 180/300-second native Waitress runs, injected errors and connection churn; no unexpected errors, child or descriptor leak observed | Measured; memory/long-duration gate remains open |
| TLS edge | Real Caddy/Waitress HTTPS, certificate/hostname failures, verified Python RPC, authentication, limits, logs and draining | Passed locally; native HTTPS success remains unverified |
| Runtime CI | Historical candidate v2 passed Linux/macOS × Python 3.13/3.14 with an explicit HTTPS URL and reviewed SHA-256 | v2 passed; each new candidate requires a fresh run |
| Publication | Public source repositories and candidate v2 prerelease available; package-index releases remain separate | PyPI release versions/dependency floors pending |
| Target operations | Resource quotas, affinity, credential rotation and supervisor/shutdown contract documented | Real deployment/cgroup, certificate renewal and signal checks pending |

## Defects fixed during the gates

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

[Candidate v2](../validation/release-results/candidate-v2/README.md) is locally
available at `target/python-release-candidate-v2/grainlift-python-candidate.tar.gz`,
SHA-256 `56c8ac49a858bdf199385338c7ebe08054625b7ae4c3631b2b1a87fe6d3f9c2b`.
It is available as a [GitHub prerelease](https://github.com/Query-farm/grainlift/releases/tag/python-candidate-v2)
and passed the [remote runtime matrix](https://github.com/Query-farm/grainlift/actions/runs/36218130106).
It contains the earlier query-only implementation. Full provenance,
hash-locked runtime/build requirements, JUnit results and summaries are retained
with that evidence. The candidate includes modified VGI-RPC code under its old
development version; publish a new VGI version and update the SDK dependency floor
before creating public index releases. Do not overwrite an existing registry
version or rely on registry VGI 0.47.1 for the unpublished compatibility changes.

The [release instructions](../validation/RELEASE.md) define candidate creation,
configuration, matrix execution and publication. Linux evidence comes from remote
CI, separately from local macOS tests. Passing CI does not establish required
branch-protection checks or actual deployment behavior.

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
