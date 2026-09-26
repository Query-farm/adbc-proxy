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

# Candidate v3: verified ADBC capability wheels

The immutable candidate archive is
`target/python-release-candidate-v3/grainlift-python-candidate.tar.gz`.
Its SHA-256 is
`ad414e90374b83542a97ac544901ed62c54fc70c8f61a7726fe5623194a9c520`.
The manifest SHA-256 is
`bfd52840b9b0c42c21146142e0c63b2739159c99763a5284cb04874c1ad3ba4c`.
Candidate v2 remains unchanged.

The exact archive and checksum are published in the
[candidate v3 prerelease](https://github.com/Query-farm/grainlift/releases/tag/python-candidate-v3).
The [combined remote matrix](https://github.com/Query-farm/grainlift/actions/runs/36220548858)
used that archive hash. Quality and three runtime jobs passed. The macOS/Python
3.14 job passed all transport, SDK and example tests plus 129 of 130 regressions;
the existing request-timeout test failed because its 300 ms callback completed
without raising the expected 200 ms timeout. That timing-sensitive test requires
an event-controlled callback in the next candidate. These remote results do not
change the exact passing local records below.

Both fresh environments installed the hash-locked wheels and ran the copied
tests on macOS 15.6.1 arm64. Runtime imports resolved inside the environments,
with inherited source import overrides cleared.

| Suite | Python 3.13.12 | Python 3.14.7 |
| --- | ---: | ---: |
| VGI-RPC unary and zero-column exchange compatibility | 14 passed | 14 passed |
| Python toolkit | 283 passed | 283 passed |
| Hello-world, including native ADBC | 13 passed | 13 passed |
| Grainlift regression | 130 passed | 130 passed |
| Total | 440 passed | 440 passed |

All suites had zero failures, errors, and skips. Both environments passed
Ruff, formatting, strict mypy, and isolated pydoclint against SDK source
extracted from the installed wheel and its corresponding tests.

Native tests cover statement preparation and repeated binding, real SQLite
transactions and ingestion, typed options, independent ADBC metadata schemas,
partition ownership, opaque Substrait hooks, and direct and isolated workers.
Binding cases include dictionary replacement, schema-only streams, zero-row
batches, and zero-column batches with nonzero row counts. The fixture's
Substrait hook verifies opaque plan delivery; it does not interpret Substrait.

All package inputs were clean at these Git revisions:

| Package | Commit |
| --- | --- |
| vgi-rpc | `d0ee383502fcdf57200c24181dbeda13ac11807e` |
| grainlift-python | `056ea119b795d49d13ce52773d8f95af0af0925c` |
| grainlift-hello-world-python | `dd7972105f81e57c272c97ea667be60c2b46094c` |

All three wheels reproduced byte-for-byte when rebuilt from their source
distributions. Artifact checks rejected forbidden local workspace, cache,
and credential paths. The archive contains 51 regular files and is 2,432,964
bytes. `manifest.json` records every bundled file hash and package source
provenance; both requirements files retain the exact hash-locked resolutions.

The native driver SHA-256 is
`abdb59c9fa9006d0b4b0578f74985f624d1da2f70285e4508b4719fee70fbea1`.
The JSON summaries and JUnit reports describe these exact local runs. Full
logs are under `target/python-release-candidate-v3/evidence313` and
`evidence314`. Reproduce the gate using `validation/release_bundle.py check`
with this archive's bundle, the requested Python version, and a built native
driver.

These local records do not establish Linux or GitHub matrix success, package
registry publication, target-deployment behavior, or long-term load stability.
The VGI wheel retains version `0.47.1`; its artifact hash identifies the
modified transport source rather than the registry release with that version.
See [release preparation](../../RELEASE.md) for publication and CI status.
