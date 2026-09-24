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

# Contributing

Thank you for helping improve Grainlift. All contributors are expected to
follow the repository's [Code of Conduct](https://github.com/Query-farm/grainlift?tab=coc-ov-file#readme).

Use the [GitHub issue tracker](https://github.com/Query-farm/grainlift/issues)
for bugs and feature requests. Report potential vulnerabilities through the
repository's [private security-advisory form](https://github.com/Query-farm/grainlift/security/advisories/new),
not a public issue.

## Build and test

Install Rust 1.97 or newer, then run:

```console
cargo build --workspace
cargo fmt --all --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

The Foundry-compatible package build requires
[Pixi](https://pixi.sh/latest/):

```console
pixi run adbc-make run build VERBOSE=true
```

External validation also requires `dbc` and a downstream driver. See the
[validation guide](validation/README.md) for local commands and supported
backends.

## Pull requests

Keep changes focused, add regression coverage, and update user or operator
documentation when behavior changes. Before opening a pull request, run the
quality gates above and `pre-commit run --all-files`. Pull request titles use
[Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) format,
for example `feat: add transport` or `fix: preserve error details`.
