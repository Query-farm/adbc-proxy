# Candidate v1: verified wheel evidence

The exact candidate is available locally at
`target/python-release-candidate-v1/grainlift-python-candidate.tar.gz`.
Its SHA-256 is
`f3fd93aeb836ab80287f4c0d0b08e94757e917f66da88c427644adef0533577f`.
The archive has not been published or uploaded to CI.

Both runs used fresh environments on macOS 15.6.1 arm64 and installed the
hash-locked wheel candidate. Imports were checked to resolve inside each new
environment's `site-packages`. Source overrides were cleared. All three wheels
reproduced byte-for-byte when rebuilt from their source distributions, and
artifact path checks rejected local workspace/cache/credential content.

| Suite | Python 3.13.12 | Python 3.14.7 |
| --- | ---: | ---: |
| VGI-RPC fixed-schema/error compatibility | 12 passed | 12 passed |
| Python toolkit | 141 passed | 141 passed |
| Hello-world, including native ADBC | 13 passed | 13 passed |
| Grainlift regression | 72 passed | 72 passed |
| Total | 238 passed | 238 passed |

Every suite had zero failures, errors, and skips. Both environments also passed
SDK Ruff, formatting, strict mypy, and isolated pydoclint against the source
extracted from the built SDK wheel and the corresponding tests.

The JSON summaries and JUnit reports record these runs. `manifest.json` is the
exact archived manifest, containing actual source hashes and dirty/unborn Git
provenance; `requirements.txt` and `build-requirements.txt` preserve the reviewed
dependency resolutions. The manifest SHA-256 is
`37dea7e143fe395ce880d214936b867d3c6d752753da0d49586a1d76eb76c2fe`.
The native-driver SHA-256 appears in each summary. Full command logs remain
under `target/python-release-candidate-v1/evidence313` and `evidence314`.

The release utility separately passed 19 artifact-integrity tests. The exact
isolated CI mypy command passed all 21 regression, soak, deployment, and release
utility source files. Ruff, formatting, pydoclint, Python compilation, shell
syntax/shellcheck, and actionlint checks passed for the release tooling/workflow.

This evidence does not establish Linux runtime support, GitHub matrix success,
package publication, target-deployment behavior, or long-term load stability.
The VGI wheel includes unpublished changes while retaining version `0.47.1`;
its hash distinguishes it from the registry release. See
[release preparation and remaining publication work](../../RELEASE.md).
