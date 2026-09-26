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

# Candidate v4: synchronized timeout regression

The exact archive is published as the
[candidate v4 prerelease](https://github.com/Query-farm/grainlift/releases/tag/python-candidate-v4).
The [combined runtime matrix](https://github.com/Query-farm/grainlift/actions/runs/36221096021)
passed quality and all four Linux/macOS Python 3.13/3.14 runtime jobs, verifying
its reviewed hash separately from the local records below.

The immutable candidate archive is
`target/python-release-candidate-v4/grainlift-python-candidate.tar.gz`.
Its SHA-256 is
`3fe5170b7fcd44eead68d995b4a2ee04900d6c580438d69aa2231b30f4d9f4ee`.
The manifest SHA-256 is
`4a7b5c5ca4018788e39057e344752043db4dba47480b97dea7732309a52119f3`.

All three package wheels and dependency locks are byte-identical to
[candidate v3](../candidate-v3/README.md). Only the copied regression files
`tests/test_lifecycle.py` and `tests/worker.py` changed. The timeout regression
holds the worker callback behind an event, observes the native request timeout
before releasing it, then requires cleanup and a successful fresh query. This
replaces the scheduling-sensitive sleep that failed one v3 CI matrix job.
An independent runtime mutation setting the request timeout to 60 seconds
caused the replacement test to fail its 10-second watchdog and exit after
releasing the callback, confirming that callback completion cannot satisfy
the timeout assertion. The test normally retains the 200 ms request timeout.

Both fresh local environments installed hash-locked wheels and ran copied
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
extracted from the wheel and its corresponding tests. These records describe
local validation; remote matrix results are recorded separately when complete.

All package inputs were clean at these Git revisions:

| Package | Commit |
| --- | --- |
| vgi-rpc | `d0ee383502fcdf57200c24181dbeda13ac11807e` |
| grainlift-python | `056ea119b795d49d13ce52773d8f95af0af0925c` |
| grainlift-hello-world-python | `dd7972105f81e57c272c97ea667be60c2b46094c` |

The copied regression changes are from Grainlift revision
`0789d0ce233826816109ebcdf0701d81581bd771`. All three wheels reproduced
byte-for-byte when rebuilt from their source distributions. Artifact checks
rejected forbidden local workspace, cache, and credential paths. The archive
contains 51 regular files and is 2,433,349 bytes. `manifest.json` records every
bundled file hash and package source provenance; both requirements files retain
the exact hash-locked resolutions.

The native driver SHA-256 is
`abdb59c9fa9006d0b4b0578f74985f624d1da2f70285e4508b4719fee70fbea1`.
The JSON summaries and JUnit reports describe these exact runs. Full logs are
under `target/python-release-candidate-v4/evidence313` and `evidence314`.
Reproduce using `validation/release_bundle.py check` with this archive's bundle,
the requested Python version, and a built native driver.

These local records do not establish package registry publication,
target-deployment behavior, or long-term load stability. The VGI wheel retains
version `0.47.1`; its hash identifies the modified transport source rather than
the registry release with that version. See [release preparation](../../RELEASE.md)
for publication and CI status. Earlier candidate archives and machine-readable
evidence remain unchanged.
