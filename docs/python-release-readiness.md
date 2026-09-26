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
It is not a claim of full ADBC feature support or transparent multi-replica use.

| Gate | Current evidence | Status |
|---|---|---|
| SDK code quality | Ruff, format, strict mypy and isolated pydoclint across SDK source/tests; `py.typed` included | Passed locally |
| Native failure behavior | Deadlines, crashes, statement/connection cancellation, raw release, client death, shutdown and recovery in independent processes | Passed locally |
| Credential rotation | Atomic replacement, overlap/revocation, principal ownership and signed continuations | Passed locally |
| Reproducible packaging | Exact hashed wheels and dependency closure; all three wheels rebuild byte-for-byte from sdists; forbidden payload checks | Passed locally |
| Fresh installation | 240 tests without skips on each of Python 3.13.12 and 3.14.7; installed `site-packages` imports verified | Passed locally on macOS arm64 |
| Load and cleanup | Eight clients, 180/300-second native Waitress runs, injected errors and connection churn; no unexpected errors, child or descriptor leak observed | Measured; memory/long-duration gate remains open |
| TLS edge | Real Caddy/Waitress HTTPS, certificate/hostname failures, verified Python RPC, authentication, limits, logs and draining | Passed locally; native HTTPS success remains unverified |
| Runtime CI | Linux/macOS × Python 3.13/3.14 matrix consumes an explicit HTTPS candidate URL and reviewed SHA-256 | Prepared; artifact hosting/configuration and remote execution pending |
| Publication | Reviewed archive and hashes available; source provenance records uncommitted inputs honestly | Target/index and release versions pending |
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
It has not been uploaded, published, or used by remote CI. Full provenance,
hash-locked runtime/build requirements, JUnit results and summaries are retained
with that evidence. The candidate includes modified VGI-RPC code under its old
development version; publish a new VGI version and update the SDK dependency floor
before creating public index releases. Do not overwrite an existing registry
version or rely on registry VGI 0.47.1 for the unpublished compatibility changes.

The [release instructions](../validation/RELEASE.md) make the remaining upload,
configuration, matrix execution and publication steps concrete. Docker's CLI is
installed locally but its engine was not running; Linux runtime was not inferred
from the macOS tests. A configured CI YAML file is not evidence of a successful
Linux job or of required branch-protection checks.

## Operational limits to retain

Use [the deployment contract](python-deployment.md). A worker timeout or crash
invalidates its entire connection; clients reconnect explicitly. In-process
callbacks remain cooperative. Sessions need process affinity, worker memory
requires OS/container quotas, and the supervisor needs a final process-group
termination budget. Native clients adopt new bearer credentials by opening new
connections. Unsupported ADBC features and the native vendor-code limitation
remain explicit. None of these constraints should be hidden by an unrestricted
"production ready" label.

The [load record](../validation/RESULTS.md) includes latency, fairness, memory and
cleanup observations. Multi-minute local runs are useful regression evidence;
they are not hours-long stability tests or capacity planning for a real database
worker. Separate GC diagnostics found roughly stable tracked-object counts and
released Arrow buffers, but did not establish an RSS plateau. Before rollout,
run the candidate on the actual deployment resources and
worker with its real certificate chain and a measured, sustained workload.
