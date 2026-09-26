#!/usr/bin/env bash
set -euo pipefail
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
bench_root=${1:?Pass the isolated benchmark directory}
repo="$bench_root/grainlift"
evidence="$bench_root/evidence"
python="$repo/validation/regression/.venv/bin/python"
driver="$repo/target/release/libadbc_driver_grainlift.so"
test "$(cat "$evidence/build.exit")" = 0
test ! -e "$evidence/stages.tsv"
trap 'docker stop grainlift-bench-v04-postgres > "$evidence/postgres-stop.log" 2>&1 || true' EXIT

run_stage() {
  local label=$1
  shift
  date -u '+%Y-%m-%dT%H:%M:%SZ' > "$evidence/current-start.txt"
  printf '%s\n' "$label" > "$evidence/current-stage.txt"
  local result=0
  timeout --signal=TERM --kill-after=30 900 "$@" > "$evidence/$label.log" 2>&1 || result=$?
  printf '%s\t%s\t%s\n' "$label" "$result" "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" >> "$evidence/stages.tsv"
}

export GRAINLIFT_SKIP_BUILD=1
export GRAINLIFT_VALIDATION_PYTHON="$repo/validation/.venv/bin/python"
export GRAINLIFT_IROH_MAX_ACTIVE_STREAMS_PER_CONNECTION=256
postgres_port=$(docker port grainlift-bench-v04-postgres 5432/tcp | cut -d: -f2)
export ADBC_POSTGRESQL_URI="postgresql://postgres@127.0.0.1:$postgres_port/postgres"
cd "$repo"
for specification in sqlite:http:32 duckdb:http:32 postgresql:http:32 duckdb:tcp:32 duckdb:mtls:32 duckdb:iroh:64; do
  IFS=: read -r backend transport workers <<< "$specification"
  label="native-$backend-$transport-${workers}x50"
  run_stage "$label" env GRAINLIFT_TRANSPORT="$transport" ./validation/run_external.sh load "$backend" \
    --workers "$workers" --iterations 50 --rows 200000 --query-rows 2048 --payload-bytes 128 \
    --json-output "$evidence/$label.json"
done

cd "$repo/validation/regression"
run_stage python-warmup "$python" -m soak --driver "$driver" --seconds 20 --clients 8 --output "$evidence/python-warmup.json"
run_stage python-180s "$python" -m soak --driver "$driver" --seconds 180 --clients 8 --output "$evidence/python-180s.json"
run_stage python-300s "$python" -m soak --driver "$driver" --seconds 300 --clients 8 --output "$evidence/python-300s.json"
run_stage python-gc-300s env PYTHONPATH="$repo/validation/regression" \
  GRAINLIFT_MEMORY_PROFILE="$evidence/python-gc-diagnostic.json" \
  "$python" "$repo/validation/profile_python_memory.py" --driver "$driver" \
  --seconds 300 --clients 8 --output "$evidence/python-gc-workload.json"
run_stage python-tracemalloc-180s env PYTHONPATH="$repo/validation/regression" \
  GRAINLIFT_MEMORY_PROFILE="$evidence/python-tracemalloc-diagnostic.json" GRAINLIFT_TRACE_ALLOCATIONS=1 \
  "$python" "$repo/validation/profile_python_memory.py" --driver "$driver" \
  --seconds 180 --clients 4 --output "$evidence/python-tracemalloc-workload.json"
run_stage python-cpu-60s py-spy record --subprocesses --rate 49 --format speedscope \
  --output "$evidence/python-cpu.speedscope.json" -- \
  "$python" -m soak --driver "$driver" --seconds 60 --clients 8 --output "$evidence/python-cpu-workload.json"
printf 'complete\n' > "$evidence/current-stage.txt"
date -u '+%Y-%m-%dT%H:%M:%SZ' > "$evidence/completed.txt"
