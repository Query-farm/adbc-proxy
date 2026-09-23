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

configuration=${1:?expected test or release}
platform=${2:?expected target platform}
architecture=${3:?expected target architecture}

case "$configuration" in
  test)
    cargo_args=(build --locked -p adbc-driver-proxy)
    profile=debug
    ;;
  release)
    cargo_args=(build --locked --release -p adbc-driver-proxy)
    profile=release
    ;;
  *)
    echo "Unsupported build configuration: $configuration" >&2
    exit 2
    ;;
esac

case "$platform/$architecture" in
  linux/amd64|linux/arm64)
    source_name=libadbc_driver_proxy.so
    output_name=libadbc_driver_proxy.so
    ;;
  macos/amd64|macos/arm64)
    source_name=libadbc_driver_proxy.dylib
    output_name=libadbc_driver_proxy.dylib
    ;;
  windows/amd64)
    source_name=adbc_driver_proxy.dll
    output_name=libadbc_driver_proxy.dll
    ;;
  *)
    echo "Unsupported build target: $platform/$architecture" >&2
    exit 2
    ;;
esac

cargo "${cargo_args[@]}"
mkdir -p build
cp "target/$profile/$source_name" "build/$output_name"
