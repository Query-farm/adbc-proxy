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

set -euo pipefail
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
bench_root=${1:?Pass the isolated benchmark directory}
repo="$bench_root/grainlift"
evidence="$bench_root/evidence"
python="$repo/validation/regression/.venv/bin/python"
driver="$repo/target/release/libadbc_driver_grainlift.so"
run_stage() {
  local label=$1
  shift
  date -u '+%Y-%m-%dT%H:%M:%SZ' > "$evidence/current-start.txt"
  printf '%s\n' "$label" > "$evidence/current-stage.txt"
  local result=0
  timeout --signal=TERM --kill-after=30 900 "$@" > "$evidence/$label.log" 2>&1 || result=$?
  printf '%s\t%s\t%s\n' "$label" "$result" "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >> "$evidence/stages.tsv"
}
cd "$repo/validation/regression"
run_stage python-cpu-60s py-spy record --subprocesses --rate 49 --format speedscope \
  --output "$evidence/python-cpu.speedscope.json" -- \
  "$python" -m soak --driver "$driver" --seconds 60 --clients 8 --output "$evidence/python-cpu-workload.json"
run_stage python-tracemalloc-180s env PYTHONPATH="$repo/validation/regression" \
  GRAINLIFT_MEMORY_PROFILE="$evidence/python-tracemalloc-diagnostic.json" GRAINLIFT_TRACE_ALLOCATIONS=1 \
  "$python" "$repo/validation/profile_python_memory.py" --driver "$driver" \
  --seconds 180 --clients 4 --output "$evidence/python-tracemalloc-workload.json"
run_stage python-300s-retry "$python" -m soak --driver "$driver" --seconds 300 --clients 8 --output "$evidence/python-300s-retry.json"
printf 'complete\n' > "$evidence/current-stage.txt"
date -u '+%Y-%m-%dT%H:%M:%SZ' > "$evidence/completed.txt"
