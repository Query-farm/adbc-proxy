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
repo=${1:?Pass the absolute Grainlift checkout path}
evidence=${2:?Pass an empty evidence directory}
driver=${3:?Pass the absolute unchanged native driver path}
phase=${4:-all}
case "$phase" in
  all|current) ;;
  *) exit 2 ;;
esac
mkdir -p "$evidence"
test ! -e "$evidence/stages.tsv"
cd "$repo/validation/regression"

run_case() {
  local label=$1 host=$2 clients=$3 seconds=$4 churn=$5
  local status=0
  local controls=(
    "GRAINLIFT_DIAGNOSTIC_OUTPUT=$evidence/$label-host.json"
    "GRAINLIFT_DIAGNOSTIC_HTTP=$host"
    "GRAINLIFT_DIAGNOSTIC_TIMINGS=off"
    "GRAINLIFT_DIAGNOSTIC_LOOP_TIMEOUT=1"
  )
  if [[ $label == waitress-patched-* ]]; then
    controls+=("GRAINLIFT_DIAGNOSTIC_READINESS=skip_locked")
  fi
  printf '%s\n' "$label" > "$evidence/current-stage.txt"
  timeout --kill-after=30 300 env -u GRAINLIFT_DIAGNOSTIC_READINESS \
    -u GRAINLIFT_DIAGNOSTIC_CPU_PROFILE -u GRAINLIFT_DIAGNOSTIC_POLL \
    "${controls[@]}" .venv/bin/python -m soak.latency \
    --driver "$driver" --seconds "$seconds" --clients "$clients" \
    --batch-rows 512 --churn "$churn" --output "$evidence/$label.json" \
    > "$evidence/$label.log" 2>&1 || status=$?
  printf '%s\t%s\n' "$label" "$status" >> "$evidence/stages.tsv"
}

if [[ $phase == all ]]; then
  run_case waitress-c1 waitress 1 30 10000
  run_case granian-c1 granian 1 30 10000
  run_case granian-c8 granian 8 30 10000
  run_case waitress-c8 waitress 8 30 10000
  run_case waitress-patched-c8 waitress 8 30 10000
  run_case granian-repeat-c8 granian 8 30 10000
  run_case granian-churn-c8 granian 8 180 25
else
  run_case granian-c1 granian 1 30 10000
  run_case granian-c8 granian 8 30 10000
  run_case granian-churn-c8 granian 8 60 25
fi
printf '%s\n' complete > "$evidence/current-stage.txt"
awk '$2 != 0 { failed=1 } END { exit failed }' "$evidence/stages.tsv"
