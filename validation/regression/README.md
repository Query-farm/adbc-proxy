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

# Toolkit-backed Grainlift regression tests

This suite loads **the real native Grainlift ADBC driver** through
adbc-driver-manager and connects it to an authenticated grainlift-python worker.
It supplies deterministic Arrow schemas, batches, and failures without installing
a downstream database. This checks the native client and its interoperability
with an independently implemented server.

It complements the Rust server tests and the existing Foundry/downstream tests;
it does not validate the Rust proxy server by substituting a Python worker.
Only HTTP is covered here. TCP, mTLS, Iroh, transactions, binding, and downstream
cancellation still need the existing validation paths.

## Run

Requires Python 3.13+, uv, Rust 1.97+, and sibling grainlift-python and vgi-rpc
checkouts from the initial toolkit work. VGI-RPC must include the explicit
RecordBatch return-schema and raw structured-error fixes. These dependencies are
unpublished; the path overrides in pyproject.toml are deliberate.

From the Grainlift repository:

    ./validation/run_regression.sh
    ./validation/run_regression.sh quality
    ./validation/run_regression.sh test -q
    ./validation/run_regression.sh unit

The default runs lint, formatting, strict types, docstring consistency, builds
the native driver, and executes all tests. Set GRAINLIFT_DRIVER to an **absolute**
path to use an already-built shared library. Missing dependencies, missing driver,
unavailable listeners, and unavailable check tools fail visibly rather than
turning into skipped tests. The unit mode selects fixture and in-process wire tests;
it is not evidence of native-driver correctness.

Each native test owns its worker and ephemeral loopback port. Authentication uses
test-only principals and tokens; no external services or credentials are needed.
Concurrent cases use independent database, connection, and statement handles.
Results and delays are small and bounded. Server threads, worker connections, and
iterators must close at teardown.

## Coverage

- Integer extremes, floating point, booleans, Unicode, binary values, decimals,
  timestamps, lists, structs, dictionaries, nulls, and Arrow metadata.
- Empty results, empty intermediate batches, batch boundaries, schema inference,
  pull-based consumption, early release, and statement reuse.
- Authentication, target policy, injected destination rejection, and independent
  concurrent connections belonging to different principals.
- Structured ADBC errors, unexpected-error sanitization, midstream failure,
  unsupported statement operations, and connection recovery.
- Batch and SQL limits immediately below, at, and above the boundary.
- HTTP request timeout followed by cleanup and recovery; this does not imply
  downstream cancellation or preemptive interruption of Python.

The current Rust ADBC 1.1 FFI uses the vendor-code slot for a private-data sentinel.
Tests require status, SQLSTATE, and binary details, and accept the known absent
vendor code or the original code when that native limitation is fixed.

## Python standards

The configuration adopts vgi-python's Ruff E/F/I/UP/B/SIM/D rules, 120-column
format, Python 3.13 target, Google docstrings, strict mypy, and pydoclint family
settings. There is no baseline or blanket exclusion for test files.
Checks apply to this new suite; legacy Foundry adapters retain their existing
configuration.

The toolkit now includes `py.typed` and passes strict mypy and pydoclint across
its source and tests. `typings/grainlift` retains small public API declarations
for the quality-only CI job, which does not install unpublished SDK packages.
These declarations are not an SDK implementation; the release gate separately
checks the actual source extracted from each SDK wheel. Remove the declarations
when that job can install a published typed SDK. PyArrow has no bundled type stubs.

Pydoclint runs separately through uvx because docstring-parser-fork and VGI-RPC's
docstring-parser share an import namespace. It is never installed into the
regression runtime, and tool failures do not silently skip the gate.

CI checks lint, types and docstrings without requiring unpublished repositories.
A Linux/macOS Python 3.13/3.14 runtime matrix can consume an explicitly hashed
wheel candidate; see [release configuration](../RELEASE.md). Artifact hosting
and repository-variable configuration remain external steps, so configured jobs
are not evidence of a completed remote run.

See [local verification](VALIDATION.md) for 74 passing cases, including isolated
worker failures and credential rotation. The separate [soak](soak/README.md) and
[TLS-edge gate](deployment/README.md) cover load and deployment behavior.
