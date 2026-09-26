#!/usr/bin/env bash
# Copyright (c) 2026 ADBC Drivers Contributors
# Copyright (c) 2026 Query Farm LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Run from any directory. The Python SDK and VGI-RPC are explicit local inputs.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
project="$repo_root/validation/regression"
mode="${1:-all}"
if [[ $# -gt 0 ]]; then
  shift
fi
case "$mode" in
  all|quality|test|unit) ;;
  *) echo "Usage: $0 [all|quality|test|unit] [pytest arguments]" >&2; exit 2 ;;
esac

for dependency in grainlift-python vgi-rpc; do
  if [[ ! -f "$repo_root/../$dependency/pyproject.toml" ]]; then
    echo "Missing sibling checkout: $repo_root/../$dependency" >&2
    exit 1
  fi
done

uv sync --project "$project"
cd "$project"

if [[ "$mode" == all || "$mode" == quality ]]; then
  uv run --no-sync ruff check .
  uv run --no-sync ruff format --check .
  uv run --no-sync mypy tests soak deployment
  regression_python="$(uv run --no-sync python -c 'import sys; print(sys.executable)')"
  # Isolation is mandatory: pydoclint's parser fork conflicts with VGI-RPC.
  uvx --python "$regression_python" --from pydoclint==0.9.1 pydoclint --config pyproject.toml tests soak deployment
fi

if [[ "$mode" == all || "$mode" == test ]]; then
  if [[ -z "${GRAINLIFT_DRIVER:-}" ]]; then
    (cd "$repo_root" && cargo build -p adbc-driver-grainlift)
  fi
  uv run --no-sync pytest "$@"
elif [[ "$mode" == unit ]]; then
  uv run --no-sync pytest -m 'not native' "$@"
fi
