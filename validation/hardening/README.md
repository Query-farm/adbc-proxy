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

# Python toolkit hardening — applied

The tested patch has been applied to the sibling `grainlift-python` checkout.
It was initially staged at `target/grainlift-hardening/grainlift-python` because
the session restricted sibling writes. After those restrictions were lifted,
all nine files were checked against their original hashes and updated. VGI-RPC
source files were not changed by this hardening pass.

`grainlift-python.patch` contains the implementation, regression tests, and SDK
documentation. `manifest.json` records SHA-256 hashes of each original and staged
file, plus the patch. New files have a null original hash. The patch is relative
to the existing development checkout, including its earlier uncommitted toolkit
implementation; it is not a patch against a published release.

## Changes

- Strict positive integer and finite duration configuration validation; malformed
  option JSON returns INVALID_ARGUMENTS without reflecting its contents.
- Per-session execution locks, bounded lock waits, concurrent connection opening,
  and session quota reservations covering factories that are still running.
- Authenticated cancellation hooks that can reach an active worker callback.
  Shutdown rejects new work and cleans up connections that finish opening late.
- Optional process-per-connection `IsolatedWorker`, using anonymous pipes and
  capped JSON/Arrow IPC. Callback deadlines and cancellation terminate the child;
  clients must reconnect. The child caps live result handles and releases failed
  results, including invalid affected-row counts.
- Request-scoped VGI-RPC log filtering and status/duration-only access events,
  replacing process-wide logger disabling. Other applications retain their logs.

In-process callbacks remain cooperative. Worker factories must now be thread-safe
because independent sessions can open and execute concurrently. Process isolation
is opt-in and is not a security sandbox. Child count and transferred data are
bounded, but arbitrary worker allocations require OS/container memory limits.
Per-operation deadlines are not an aggregate service-shutdown deadline. The SDK
README in the patch describes cancellation, cleanup, and deployment semantics.

## Validating the applied changes

From the Grainlift repository root:

```sh
(cd ../grainlift-python && uv run pytest)
./validation/run_regression.sh
```

The retained patch and manifest document the initial SDK hardening changes;
subsequent typing, credential and lifecycle fixes extend that snapshot. Do not apply the
patch again to this checkout. The hello-world example now uses the updated SDK.
VGI-RPC's locally modified unary RecordBatch/error support is still required.

## Current evidence — 2026-09-26

The [release candidate](../release-results/candidate-v2/README.md) now passes
**240 tests on each of Python 3.13.12 and 3.14.7** from fresh wheel installations:
141 SDK, 74 regression, 13 hello-world and 12 VGI compatibility cases. SDK source
and tests pass strict typing, Ruff and isolated pydoclint. Independent native
failure/cancellation tests, credential rotation, bounded load measurements and a
local TLS edge have been added. Review also fixed startup pipe cleanup, a stale
cancellation race, dynamic logging-handler filtering, Waitress's request-size
boundary, and source distributions containing local worktrees/caches.

See [current validation](../regression/VALIDATION.md),
[load/edge evidence](../RESULTS.md), and [release preparation](../RELEASE.md).
Artifact publication, remote runtime CI and target-environment rollout validation
remain external gates. Memory trends require longer observation before a leak-
free or production-capacity claim.

## Initial applied-patch evidence — 2026-09-25

After applying the changes, the live SDK passed **122 tests in 6.81 seconds**.
The hello-world project passed **all 13 tests**, including its four native cases,
in 2.50 seconds against the rebuilt driver. The Grainlift regression suite passed **all 47 cases in 2.58
seconds**, including the previously blocked 37 native cases. Its standard uv
bootstrap and Ruff, strict mypy, and isolated pydoclint gates all passed.

The first full native run exposed a Grainlift HTTP timeout bug. The native client
now configures its shared Reqwest client with the selected request timeout; the
VGI builder setting alone did not affect that supplied client. The existing
timeout regression verifies the fix. Two test paths also import their Arrow
readers before release to avoid a same-process Python GIL stall. See
[the complete regression evidence](../regression/VALIDATION.md).

For the native-client change, `cargo fmt --all --check`, `cargo test --workspace`
(45 tests), and `cargo clippy --workspace --all-targets -- -D warnings` all passed.
The workspace tests include HTTP, TCP, mTLS, and Iroh roundtrips against the Rust
server; they do not add those transports to the Python SDK.

## Original staged evidence — 2026-09-25

Environment: macOS 15.6.1 arm64, Python 3.14.7, PyArrow 25.0.1, pytest 9.1.1,
and the locally modified VGI-RPC 0.47.1 checkout. Existing environments supplied
dependencies; no fresh installation or published-wheel validation was performed.

- **122 toolkit tests passed** in 6.74 seconds. This includes 31 original cases,
  with the logging expectation updated, and 91 added cases.
- **10 Grainlift regression cases passed; 37 native cases deselected**, using the
  staged toolkit. Native listener setup is prohibited by this session's sandbox;
  see [the earlier native attempt](../regression/VALIDATION.md).
- Actual spawned processes exercised execute/fetch/startup/close hangs, crashes,
  cancellation, error metadata, result quotas, repeated process cleanup, and
  complete messages immediately below/at/above the IPC cap.
- An in-process WSGI/VGI client exercised a spawned worker, pull-based results,
  cancellation, and connection release. This did not use a TCP listener or the
  native ADBC C ABI.
- Independent-session progress, expiry while another session is busy, bounded
  lock waiting, opening quota reservations, shutdown during opening, and shutdown
  of an active isolated worker passed.
- SDK Ruff lint/format and Python compilation passed. The Grainlift regression
  suite's Ruff, strict mypy, and isolated pydoclint 0.9.1 gates passed. Strict
  typing/docstring gates cover the regression suite, not the entire SDK.
- Both validation shell runners passed syntax checks and Shellcheck. No Rust
  source changed, and the Rust test suite was not rerun for this candidate.

Commands used for the staged runtime tests, from the Grainlift root:

```sh
PYTHONDONTWRITEBYTECODE=1 \
PYTHONPATH=target/grainlift-hardening/grainlift-python/src \
../grainlift-python/.venv/bin/python -m pytest -p no:cacheprovider \
  target/grainlift-hardening/grainlift-python/tests -q

PYTHONDONTWRITEBYTECODE=1 \
PYTHONPATH=target/grainlift-hardening/grainlift-python/src \
../grainlift-python/.venv/bin/python -m pytest -p no:cacheprovider \
  validation/regression/tests -m 'not native' -q
```

The successful strict type check used the SDK's Python environment for package
discovery (`mypy --python-executable ../grainlift-python/.venv/bin/python`). A
first attempt using the unrelated vgi-python environment encountered its different
Arrow stubs and missing ADBC manager; that environment is not the test runtime.

## Remaining release gates

Review the applied changes and extend the measured workloads to an hours-long
campaign on the intended worker and deployment resources. The local three- and
five-minute workloads are evidence of bounded progress and cleanup, not a
deployment-capacity or memory-stability certificate.

Publish the required VGI-RPC changes under a new version, update the SDK dependency
floor, rebuild the reproducible candidate and enable/run remote runtime CI.
Python 3.13/3.14 wheel validation and SDK typing/docstring gates are now complete
locally. Linux and the intended production resource/supervisor setup remain to
be exercised; positive native HTTPS needs the real trusted certificate chain.
HTTP remains the only Python service transport; unsupported ADBC functionality
and the native vendor-code limitation remain as documented in the SDK. These
changes do not establish production readiness or validate Rust server behavior.
