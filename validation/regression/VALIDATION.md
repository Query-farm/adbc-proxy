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

# Local verification — 2026-09-26

## ADBC operation coverage

The expanded source suite passed **130 tests in 29.92 seconds** against the native
C ABI on macOS. Its 56 new cases cover direct and isolated workers: real SQLite
commit/rollback, prepared parameters, batch/stream binding and rejected replacement
recovery, ingestion modes and temporary tables, independent statements, typed
options, discovery/statistics filters, partition ownership and connection transfer,
unknown row counts, zero-row/zero-column binding and Substrait plan delegation. Independent wire oracles verify
standard response and metadata schemas. Ruff/format, strict mypy and isolated
pydoclint passed for the expanded suite.

The [new candidate v3](../release-results/candidate-v3/README.md) separately passed
**440 tests on each of Python 3.13.12 and 3.14.7**, installed from exact wheels in
fresh macOS environments: 14 transport, 283 toolkit, 13 hello-world and 130 native
regression cases, with no failures or skips. Runtime imports resolved inside
`site-packages`; SDK Ruff/format, strict mypy and isolated pydoclint also passed.
All three wheels rebuilt byte-for-byte from their source distributions. The
[SDK Linux/macOS CI matrix](https://github.com/Query-farm/grainlift-python/actions/runs/36220380809)
passed all four interpreter/platform jobs; the
[candidate v3 combined matrix](https://github.com/Query-farm/grainlift/actions/runs/36220548858)
is tracked separately from the historical query-only v2 below.
The SDK exposes backend hooks, not generic database behavior: SQLite is the real
transaction/ingestion backend here, while metadata, partitions and Substrait use
controlled fixture capabilities.

Feature testing found two further defects now covered by regressions: VGI-RPC
misclassified zero-column bidirectional exchanges as producers, and isolated
result cleanup could mask a primary ADBC error when a backend's close method
also failed. A statement-close/cancel race was also fixed so cancellation cannot
enter a backend statement already being closed.

## Historical query-only candidate v2

The [exact wheel candidate](../release-results/candidate-v2/README.md) passed
**74 regression cases on both Python 3.13.12 and 3.14.7**, installed into fresh
environments with no sibling source overrides. Each environment also passed
141 toolkit tests, 13 hello-world tests and 12 VGI compatibility tests: **240
tests per interpreter, with zero failures or skips**. SDK source and tests pass
Ruff, formatting, strict mypy and isolated pydoclint. The release utility has
19 passing artifact-integrity tests.

The regression suite adds 14 independent-process native isolation/failure tests,
five live HTTP credential-rotation tests, and eight load-harness checks to the
original 47 cases. Isolation cases cover raw C-ABI stream release, execute/fetch/
startup/close deadlines, worker crashes, active statement/connection cancellation,
client death, idle reaping and active shutdown. Rotation tests combine native
ADBC requests with an independent Arrow wire-schema oracle for signed
continuations. A revoked credential cannot advance a cursor or transfer ownership.

The [Waitress soak](soak/README.md) and [Caddy TLS-edge validation](deployment/README.md)
have separate machine-readable evidence under `validation/load-results/` and are
summarized in [RESULTS.md](../RESULTS.md). The configured Linux/macOS runtime CI
matrix subsequently [passed remotely](https://github.com/Query-farm/grainlift/actions/runs/36218130106)
using the [published GitHub candidate v2](https://github.com/Query-farm/grainlift/releases/tag/python-candidate-v2).
No package-index release was published. These results do not establish native
HTTPS success with a custom CA or production capacity.

## Original integration run and defects found

Environment: macOS, Python 3.14.7, PyArrow 25.0.1, adbc-driver-manager 1.12.0,
the hardened sibling grainlift-python 0.1.0 checkout, and the locally modified VGI-RPC
0.47.1 checkout from the toolkit implementation.

- 47 tests collected: 37 native integration cases and 10 fixture/in-process wire
  cases.
- **All 47 cases passed in 2.58 seconds**, including the 37 native integration
  cases, after filesystem/network restrictions were lifted.
- Ruff lint/format, strict mypy, isolated pydoclint 0.9.1, Python compilation,
  bash syntax, and Shellcheck passed.
- Earlier, a native schema-inference test was attempted with the existing compiled driver.
  Setup failed at socket.bind(('127.0.0.1', 0)) with PermissionError (operation not
  permitted). That session initially prohibited loopback listeners; the later
  permission change allowed the named native runs above.

The first full native run found a real HTTP timeout defect: the shared Reqwest
client bypassed VGI-RPC's timeout builder setting. Grainlift now configures
`grainlift.request_timeout_ms` on that shared client. The timeout/recovery test
fails before the fix and passes afterward, checking cleanup and a fresh query.

The same run exposed a test-harness limitation: releasing an unimported Arrow
handle through the Python ADBC manager holds the GIL while waiting for the Python
server in the same process. The reuse and timeout tests now import and close an
Arrow reader, avoiding that artificial stall. The new native-isolation suite now
uses a separate server process and verifies raw handle release directly.

The standard `uv sync` bootstrap completed and generated `uv.lock`. The native
driver was rebuilt with `cargo build -p adbc-driver-grainlift`. The successful
full quality/runtime command was:

```sh
GRAINLIFT_DRIVER="$PWD/target/debug/libadbc_driver_grainlift.dylib" \
  ./validation/run_regression.sh all -q
```

The sibling SDK passed 122 tests (6.81 seconds), and the hello-world project
passed all 13 tests including its four native cases (2.50 seconds against the rebuilt driver).
CI's quality job is configured but has not run here. These short local runs do
not establish sustained-load performance or memory stability.

The native-client fix also passed Rust formatting, all 45 workspace tests, and
Clippy across all workspace targets with warnings denied.
