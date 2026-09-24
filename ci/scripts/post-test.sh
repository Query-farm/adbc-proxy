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

if [[ -f .grainlift-validation.pid ]]; then
  runner_pid=$(<.grainlift-validation.pid)
  kill "$runner_pid" 2>/dev/null || true
  wait "$runner_pid" 2>/dev/null || true
fi
if [[ -f .grainlift-validation-container ]]; then
  container_id=$(<.grainlift-validation-container)
  docker stop "$container_id" >/dev/null 2>&1 || true
fi
rm -f .grainlift-validation.pid .env.override
rm -f .grainlift-validation-container
