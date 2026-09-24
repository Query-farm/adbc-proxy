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

args=("$@")
output=""
for index in "${!args[@]}"; do
  if [[ "${args[$index]}" == -o ]] && ((index + 1 < ${#args[@]})); then
    output="${args[$((index + 1))]}"
    break
  fi
done

# The AArch64 blake3 implementation contributes public C symbols to a cdylib.
# Filter those only from our exported ADBC driver. Rewriting version scripts
# for dependency dylibs can leave an empty `global` block, which GNU ld rejects.
if [[ "$output" == */libadbc_driver_grainlift.so ]]; then
  for index in "${!args[@]}"; do
    if [[ "${args[$index]}" == -Wl,--version-script=* ]]; then
      version_script="${args[$index]#*=}"
      scratch=$(mktemp)
      grep -v 'blake3_' "$version_script" >"$scratch" || true
      mv "$scratch" "$version_script"
    fi
  done
fi

exec cc "$@"
