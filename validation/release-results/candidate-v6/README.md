<!-- Copyright (c) 2026 ADBC Drivers Contributors; Query Farm LLC. SPDX-License-Identifier: Apache-2.0 -->

# Candidate v6: typed ADBC protocol 0.4

This candidate pairs protocol 0.4 named requests and responses with typed signed
partition claims. It replaces JSON control arguments, preserves ADBC option
types and metadata distinctions, and uses stock registry VGI-RPC 0.47.1.
Nested Arrow IPC payloads are uncompressed; compression belongs to transport.

Archive SHA-256:
`63692d5a6fb81208fdf468dd9e225e04646b33bf2dd5fee4978ccac3da8c3f3e`.
Manifest SHA-256:
`c83453ee6113d6d949fa1d8e84a889c57f531641cf69f9b55e54e052f23aacbd`.

The clean package sources are SDK `9ca8f3621320bd19c93afe243fbf3f523872c763`
and hello-world `c3a27e1113f0c79d3cc3eb9e187dc1e9a19bbba8`. Hello's subsequent
`f23582437848e7ad18b81f8c75d18da65311b473` changes only its native CI pin.
The matching native protocol and regression source is Grainlift
`c94fc38435dc0604e9ddadf77ef6c207da0abb7d`.

Both local wheels rebuild byte-for-byte from their source distributions.
Fresh macOS 15.6.1 arm64 environments install wheels with mandatory dependency
hashes and run copied tests without source import overrides. Ruff, formatting,
strict mypy and isolated pydoclint pass against the installed SDK contents.

| Suite | Python 3.13.12 | Python 3.14.7 |
| --- | ---: | ---: |
| Toolkit | 446 passed | 446 passed |
| Hello-world | 13 passed | 13 passed |
| Native regression | 150 passed | 150 passed |
| Total | 609 passed | 609 passed |

Both runs passed without retries, failures or skips. Summaries record the exact
native driver hash; JUnit and installed versions are retained alongside them.

Separate native gates pass 66 Rust tests, formatting, strict Clippy and a release
workspace build. SQLite C-ABI smoke tests pass over HTTP, TCP, mTLS and Iroh.
HTTP SQLite Foundry reports 164 passed, 167 skipped and three expected failures
for downstream limitations. The SDK's
[installed-wheel matrix](https://github.com/Query-farm/grainlift-python/actions/runs/36246383408)
passes Linux/macOS on Python 3.13/3.14. The combined candidate runtime matrix is
tracked separately and is not implied by these local results.

The [ADBC review](../../../docs/adbc-protocol-review.md) maps every ADBC 1.1
entry point and records backend and adapter limitations. This is not external
ADBC certification. The [readiness record](../../../docs/python-release-readiness.md)
retains deployment, long-duration load and native ARM64 packaging gates.
Protocol 0.2/0.3 candidates are incompatible with this driver: upgrade client
and server together. This candidate does not publish packages to PyPI.
