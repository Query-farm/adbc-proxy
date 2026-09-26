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

# Regression testing with Python workers

The grainlift-python toolkit provides a controllable peer for testing the native
Grainlift ADBC client. A regression fixture can return an exact Arrow schema,
control batch boundaries, record cursor consumption and release, or inject a
query/read error. Applications still use the standard ADBC driver manager.

The suite lives in validation/regression. See its
[running instructions and coverage](../validation/regression/README.md).

When fixing a client interoperability issue, add a native test that reaches the
bug through the exported C ABI. Keep synthetic fixture behavior in worker.py and
make its observations independent of private toolkit dictionaries. Use
independent ADBC handles for concurrent tests, finite data sizes and delays, and
assert cleanup as well as returned data. Preserve status, SQLSTATE and binary
details when asserting errors; document native limitations rather than hiding
them behind broad expected failures.

`feature_worker.py` adds real SQLite transaction, preparation, parameter and
ingestion behavior. Its metadata uses independent ADBC standard Arrow schemas in
`feature_schemas.py`. Direct and isolated native cases cover all operation hooks,
including typed options, statistics, partition ownership and Substrait delegation.
Wire oracle cases compare responses against independently declared schemas rather
than importing the implementation's schema constants. Substrait fixtures verify
opaque plan transport; they do not constitute a general Substrait engine.

The Python server is an independent implementation of the Grainlift wire
contract. Passing its tests does not prove Rust server authorization, downstream
driver correctness, TCP/mTLS/Iroh behavior, load performance, or multi-replica
transparency. Keep Rust and external downstream validation as separate gates.

Ruff, strict mypy, and isolated pydoclint gate the new Python suite using the
vgi-python standards. The toolkit source and its tests now pass the same gates.
CI runs regression quality checks without unpublished runtime inputs.
Full native tests can use the sibling development checkouts or an exact
[hashed wheel candidate](../validation/RELEASE.md), and require permission to
bind a loopback HTTP listener. The candidate workflow adds Python 3.13/3.14
runtime jobs on Linux/macOS; enabling them requires a reviewed archive URL and
hash. Local passing tests do not establish that this CI matrix has passed.

A [now-applied SDK hardening patch](../validation/hardening/README.md) adds independent
session execution, process-isolated workers, deadline/cancellation handling, and
request-scoped diagnostics. Its evidence distinguishes spawned-process and WSGI
tests from the native C-ABI cases subsequently verified over loopback HTTP.
That native run found and now guards a shared HTTP-client timeout configuration
bug. The Python fixture's timeout test checks cleanup and recovery, separately
from worker-process cancellation and deadlines.

The suite now includes independent-process native tests for worker timeouts,
crashes, active cancellation, abrupt client exit, shutdown, and credential
rotation. Separate load and TLS-edge harnesses use Waitress; those results do not
claim the native client's custom-CA HTTPS success path or a production rollout.
See the [current verification record](../validation/regression/VALIDATION.md).
