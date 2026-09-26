# Candidate v2: verified wheel evidence

The final candidate is available locally at
`target/python-release-candidate-v2/grainlift-python-candidate.tar.gz`.
Its SHA-256 is
`56c8ac49a858bdf199385338c7ebe08054625b7ae4c3631b2b1a87fe6d3f9c2b`.
The archive has not been published or uploaded to CI.

This candidate includes the final soak-host cleanup changes, two additional
regression tests, and consistent Ruff import grouping for the soak package.
All three package wheels remain byte-identical to [candidate v1](../candidate-v1/README.md).
The final copied harness, tests, and configuration were validated together
in fresh environments on macOS 15.6.1 arm64.

| Suite | Python 3.13.12 | Python 3.14.7 |
| --- | ---: | ---: |
| VGI-RPC fixed-schema/error compatibility | 12 passed | 12 passed |
| Python toolkit | 141 passed | 141 passed |
| Hello-world, including native ADBC | 13 passed | 13 passed |
| Grainlift regression | 74 passed | 74 passed |
| Total | 240 passed | 240 passed |

Every suite had zero failures, errors, and skips. Both environments also passed
SDK Ruff, formatting, strict mypy, and isolated pydoclint against source
extracted from the built SDK wheel and the corresponding tests. Runtime imports
were checked to resolve inside each fresh environment's `site-packages`, with
inherited source overrides cleared.

All three wheels reproduced byte-for-byte when rebuilt from their source
distributions. Artifact checks found no forbidden local workspace, cache, or
credential paths in the wheels, source archives, or copied candidate files.

The JSON summaries and JUnit reports record these exact runs. `manifest.json`
is the archived manifest, containing actual source hashes and dirty/unborn Git
provenance. `requirements.txt` and `build-requirements.txt` preserve the reviewed
dependency resolutions. The manifest SHA-256 is
`980adf6b40875ecb60f7ebd4134247c63c050ec4021b42c2729778e2c1dd1a7d`.
The native-driver SHA-256 appears in each summary. Full command logs remain
under `target/python-release-candidate-v2/evidence313` and `evidence314`.

This evidence does not establish Linux runtime support, GitHub matrix success,
package publication, target-deployment behavior, or long-term load stability.
The VGI wheel includes unpublished changes while retaining version `0.47.1`;
its hash distinguishes it from the registry release. See
[release preparation and remaining publication work](../../RELEASE.md).
