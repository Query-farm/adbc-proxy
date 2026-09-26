<!-- Copyright (c) 2026 ADBC Drivers Contributors; Query Farm LLC. SPDX-License-Identifier: Apache-2.0 -->

# Candidate v5: typed protocol responses (unpublished)

This intermediate protocol 0.3 candidate was tested locally but not published.
Protocol review identified additional request-typing work, now implemented by
protocol 0.4. Retain this evidence as historical; do not pair this archive with
the current native driver.

This candidate uses Grainlift protocol 0.3.0 with frozen Python response classes
and corresponding Rust structs. Unary responses use the standard VGI-RPC
dataclass envelope. Binding uses a fixed input envelope with raw, uncompressed
Arrow IPC bytes; compression belongs to the transport. The SDK depends on
unmodified registry VGI-RPC 0.47.1. Only the SDK and hello-world wheels are built
locally, and both rebuild byte-for-byte from their source distributions.

Archive SHA-256:
`bb625aa2be5b52ab1702fa392aacdc0b74f35e00e3049e6525dee4d804a5986c`.
Manifest SHA-256:
`eb41949db7ed858c9ff38df26fca0c67bea8ce98e2eeb0223602ad63441afde9`.

Package sources are SDK `7a4a273866382cd0589d654e2015030372844a2f` and hello-world
`1c675cc3dd6b3f6e77c99ac9e1bdaa14de478dcd`. The subsequent hello-world commit only
pins its CI native checkout and does not change package inputs. Native protocol
and validation source is Grainlift `24216c2b720bcb66c29d8e2dcb64cca9d0daa431`.

Fresh local environments use macOS 15.6.1 arm64, mandatory dependency hashes,
installed wheels, copied tests and no source import overrides. Ruff, formatting,
strict mypy and isolated pydoclint pass against the installed SDK contents.

| Suite | Python 3.13.12 retry | Python 3.14.7 |
| --- | ---: | ---: |
| Toolkit | 344 passed | 344 passed |
| Hello-world | 13 passed | 13 passed |
| Native regression | 138 passed | 138 passed |
| Total | 495 passed | 495 passed |

Neither successful run had failures or skips. The Python 3.13 retry used the
unchanged archive after the concurrent build and other interpreter run finished.

The initial Python 3.13 run passed all SDK and hello-world tests, but one of 138
native regression tests failed during worker startup, before its cancellation
operation began. This run overlapped the Python 3.14 suite and a release Rust
build. The worker startup limit remained two seconds; the test and candidate
were not changed. Its JUnit evidence is retained in `failed-initial313/`.

Separate source gates passed 56 Rust tests, formatting, strict Clippy, and the
release workspace build. External SQLite C-ABI smoke tests passed over HTTP,
TCP, mTLS and Iroh. VGI-RPC's restored runtime passed 4,970 tests. The unchanged
`vgi-python` consumer retained its baseline: 2,704 passed, 104 skipped and one
pre-existing directory-parity failure.

Protocol 0.2 candidates remain historical and cannot be paired with the current
native driver. This candidate does not establish long-duration load behavior or
close the deployment and native ARM64 packaging gates listed in the
[readiness record](../../../docs/python-release-readiness.md).
