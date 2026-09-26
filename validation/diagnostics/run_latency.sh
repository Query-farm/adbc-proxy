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
evidence=${2:?Pass the absolute evidence directory containing native-control.so and native-reuse.so}
phase=${3:-unary}
case "$phase" in
  unary|streams) ;;
  *) exit 2 ;;
esac
stages="$evidence/stages-$phase.tsv"
cd "$repo/validation/regression"
test ! -e "$stages"

run_case() {
  local label=$1 binary=$2 clients=$3 batch=$4 seconds=$5 profile=$6
  local status=0
  local controls=(
    "GRAINLIFT_DIAGNOSTIC_OUTPUT=$evidence/$label-timings.json"
    "GRAINLIFT_DIAGNOSTIC_READINESS=skip_locked"
    "GRAINLIFT_DIAGNOSTIC_LOOP_TIMEOUT=1"
  )
  if [[ $profile == yes ]]; then
    controls+=("GRAINLIFT_DIAGNOSTIC_CPU_PROFILE=$evidence/$label-cpu.txt")
    if [[ $label == control-profile ]]; then
      controls+=("GRAINLIFT_DIAGNOSTIC_PROFILE_CLOCK=thread_cpu")
    fi
  fi
  printf '%s\n' "$label" > "$evidence/current-stage.txt"
  timeout --kill-after=30 180 env "${controls[@]}" .venv/bin/python -m soak.latency \
    --driver "$evidence/native-$binary.so" --seconds "$seconds" --clients "$clients" \
    --batch-rows "$batch" --churn 10000 --output "$evidence/$label.json" \
    > "$evidence/$label.log" 2>&1 || status=$?
  printf '%s\t%s\n' "$label" "$status" >> "$stages"
}

if [[ $phase == unary ]]; then
  run_case control-c8 control 8 512 20 no
  run_case reuse-c8 reuse 8 512 20 no
  run_case reuse-c1 reuse 1 512 20 no
  run_case reuse-batch4096-c8 reuse 8 4096 20 no
  run_case control-profile control 8 512 10 yes
  run_case control-repeat-c8 control 8 512 20 no
  run_case reuse-repeat-c8 reuse 8 512 20 no
else
  run_case control-final-c1 control 1 512 20 no
  run_case full-reuse-c1 full-reuse 1 512 20 no
  run_case control-final-c8 control 8 512 20 no
  run_case full-reuse-c8 full-reuse 8 512 20 no
  run_case full-reuse-repeat-c8 full-reuse 8 512 20 no
  run_case wall-profile control 1 512 10 yes
fi
printf '%s\n' complete > "$evidence/current-stage.txt"
awk '$2 != 0 { failed=1 } END { exit failed }' "$stages"
