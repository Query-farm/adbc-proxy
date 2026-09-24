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

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
validation_root="$repo_root/validation"
python_cmd=${GRAINLIFT_VALIDATION_PYTHON:-}
if [[ -z "$python_cmd" ]]; then
  if command -v python3.13 >/dev/null 2>&1; then
    python_cmd=python3.13
  else
    python_cmd=python3
  fi
fi
if ! "$python_cmd" -c 'import tomllib' >/dev/null 2>&1; then
  echo "Validation setup requires Python 3.11+ (set GRAINLIFT_VALIDATION_PYTHON)" >&2
  exit 2
fi
token="grainlift-validation-token"
transport=${GRAINLIFT_TRANSPORT:-http}
case "$transport" in
  http|tcp|mtls|iroh) ;;
  *)
    echo "Unsupported validation transport: $transport" >&2
    exit 2
    ;;
esac

mode=${1:-smoke}
if [[ $# -gt 0 ]]; then
  shift
fi
backend=${GRAINLIFT_BACKEND:-sqlite}
if [[ $# -gt 0 && "$1" =~ ^(sqlite|duckdb|postgresql|mysql|flightsql|datafusion|trino|mssql)$ ]]; then
  backend=$1
  shift
fi
case "$backend" in
  sqlite|duckdb|postgresql|mysql|flightsql|datafusion|trino|mssql) ;;
  *)
    echo "Unsupported validation backend: $backend" >&2
    exit 2
    ;;
esac
target="$backend"

if [[ "${GRAINLIFT_SKIP_BUILD:-0}" != "1" ]]; then
  cargo build --release --workspace --manifest-path "$repo_root/Cargo.toml"
fi

case "$(uname -s)" in
  Darwin)
    grainlift_driver="$repo_root/target/release/libadbc_driver_grainlift.dylib"
    ;;
  Linux)
    grainlift_driver="$repo_root/target/release/libadbc_driver_grainlift.so"
    ;;
  *)
    echo "Unsupported platform: $(uname -s)" >&2
    exit 2
    ;;
esac

if [[ ! -f "$grainlift_driver" ]]; then
  echo "Grainlift driver was not built at $grainlift_driver" >&2
  exit 2
fi

case "$backend" in
  sqlite) downstream_driver=${ADBC_SQLITE_DRIVER:-} ;;
  duckdb) downstream_driver=${ADBC_DUCKDB_DRIVER:-} ;;
  postgresql) downstream_driver=${ADBC_POSTGRESQL_DRIVER:-} ;;
  mysql) downstream_driver=${ADBC_MYSQL_DRIVER:-} ;;
  flightsql) downstream_driver=${ADBC_FLIGHTSQL_DRIVER:-} ;;
  datafusion) downstream_driver=${ADBC_DATAFUSION_DRIVER:-} ;;
  trino) downstream_driver=${ADBC_TRINO_DRIVER:-} ;;
  mssql) downstream_driver=${ADBC_MSSQL_DRIVER:-} ;;
esac
if [[ -z "$downstream_driver" ]]; then
  downstream_driver=$(VALIDATION_BACKEND="$backend" "$python_cmd" - <<'PY'
import os
import platform
import tomllib
from pathlib import Path

backend = os.environ["VALIDATION_BACKEND"]
locations = []
if platform.system() == "Darwin":
    locations.append(Path.home() / f"Library/Application Support/ADBC/Drivers/{backend}.toml")
else:
    locations.extend(
        [
            Path.home() / f".config/adbc/drivers/{backend}.toml",
            Path.home() / f".local/share/adbc/drivers/{backend}.toml",
        ]
    )

for manifest in locations:
    if not manifest.is_file():
        continue
    with manifest.open("rb") as source:
        data = tomllib.load(source)
    shared = data.get("Driver", {}).get("shared", {})
    key = f"{platform.system().lower()}_{platform.machine().lower()}"
    aliases = {
        "darwin_arm64": "macos_arm64",
        "darwin_aarch64": "macos_arm64",
        "darwin_x86_64": "macos_amd64",
        "linux_aarch64": "linux_arm64",
        "linux_x86_64": "linux_amd64",
    }
    driver = shared.get(aliases.get(key, key))
    if driver:
        print(driver)
        raise SystemExit
raise SystemExit(
    f"Could not discover {backend}.toml; run `dbc install {backend} --level user` "
    f"or set ADBC_{backend.upper()}_DRIVER"
)
PY
  )
fi

if [[ ! -f "$downstream_driver" ]]; then
  echo "$backend ADBC driver does not exist: $downstream_driver" >&2
  exit 2
fi

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/grainlift-validation.XXXXXX")
server_pid=""
postgres_data=""
cleanup() {
  status=$?
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  if [[ -n "$postgres_data" ]]; then
    pg_ctl -D "$postgres_data" -m immediate -w stop >/dev/null 2>&1 || true
  fi
  if [[ "$status" -ne 0 && -f "$work_dir/server.log" ]]; then
    echo "Grainlift server log (last 240 lines):" >&2
    tail -n 240 "$work_dir/server.log" >&2
  fi
  rm -rf "$work_dir"
  return "$status"
}
trap cleanup EXIT INT TERM

port=$("$python_cmd" - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
)
tcp_port=$("$python_cmd" - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
)
health_endpoint="http://127.0.0.1:$port"
endpoint="$health_endpoint"
config="$work_dir/grainlift.toml"
iroh_secret="$work_dir/iroh-secret"
iroh_info="$work_dir/iroh-endpoint.json"
tls_ca="$work_dir/tls-ca.pem"
tls_ca_key="$work_dir/tls-ca-key.pem"
tls_server_cert="$work_dir/tls-server.pem"
tls_server_key="$work_dir/tls-server-key.pem"
tls_client_cert="$work_dir/tls-client.pem"
tls_client_key="$work_dir/tls-client-key.pem"
"$python_cmd" - <<'PY' >"$iroh_secret"
import secrets
print(secrets.token_hex(32))
PY
if [[ "$transport" == "mtls" ]]; then
  if ! command -v openssl >/dev/null; then
    echo "mTLS validation requires openssl" >&2
    exit 2
  fi
  openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
    -subj "/CN=Grainlift Validation CA" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -keyout "$tls_ca_key" -out "$tls_ca" >/dev/null 2>&1
  openssl req -new -newkey rsa:2048 -nodes \
    -subj "/CN=localhost" \
    -addext "subjectAltName=DNS:localhost" \
    -addext "basicConstraints=critical,CA:FALSE" \
    -addext "keyUsage=critical,digitalSignature,keyEncipherment" \
    -addext "extendedKeyUsage=serverAuth" \
    -keyout "$tls_server_key" -out "$work_dir/tls-server.csr" >/dev/null 2>&1
  openssl x509 -req -days 1 -sha256 -copy_extensions copy \
    -in "$work_dir/tls-server.csr" -CA "$tls_ca" -CAkey "$tls_ca_key" \
    -CAcreateserial -out "$tls_server_cert" >/dev/null 2>&1
  openssl req -new -newkey rsa:2048 -nodes \
    -subj "/CN=validation-client" \
    -addext "subjectAltName=URI:spiffe://validation.test/client" \
    -addext "basicConstraints=critical,CA:FALSE" \
    -addext "keyUsage=critical,digitalSignature" \
    -addext "extendedKeyUsage=clientAuth,serverAuth" \
    -keyout "$tls_client_key" -out "$work_dir/tls-client.csr" >/dev/null 2>&1
  openssl x509 -req -days 1 -sha256 -copy_extensions copy \
    -in "$work_dir/tls-client.csr" -CA "$tls_ca" -CAkey "$tls_ca_key" \
    -CAcreateserial -out "$tls_client_cert" >/dev/null 2>&1
fi
case "$backend" in
  sqlite)
    entrypoint="AdbcDriverSqliteInit"
    database_option_key="uri"
    database_option_value="$work_dir/validation.sqlite"
    ;;
  duckdb)
    entrypoint="duckdb_adbc_init"
    database_option_key="path"
    database_option_value="$work_dir/validation.duckdb"
    ;;
  postgresql)
    entrypoint="AdbcDriverPostgresqlInit"
    database_option_key="uri"
    database_option_value=${ADBC_POSTGRESQL_URI:-}
    if [[ -z "$database_option_value" ]]; then
      if ! command -v initdb >/dev/null || ! command -v pg_ctl >/dev/null; then
        echo "PostgreSQL validation requires ADBC_POSTGRESQL_URI or local initdb/pg_ctl" >&2
        exit 2
      fi
      postgres_data="$work_dir/postgres"
      postgres_port=$("$python_cmd" - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
)
      initdb -D "$postgres_data" --auth=trust --username=postgres \
        --no-locale --encoding=UTF8 >/dev/null
      pg_ctl -D "$postgres_data" \
        -o "-h 127.0.0.1 -p $postgres_port -F" -w start >/dev/null
      database_option_value="postgresql://postgres@127.0.0.1:$postgres_port/postgres"
    fi
    ;;
  mysql)
    entrypoint=""
    database_option_key="uri"
    database_option_value=${ADBC_MYSQL_URI:-mysql://root:root@127.0.0.1:3306/testdb}
    ;;
  flightsql)
    entrypoint=""
    database_option_key="uri"
    database_option_value=${ADBC_FLIGHTSQL_URI:-grpc://127.0.0.1:31337}
    ;;
  datafusion)
    entrypoint=""
    database_option_key=""
    database_option_value=""
    ;;
  trino)
    entrypoint=""
    database_option_key="uri"
    database_option_value=${ADBC_TRINO_URI:-http://127.0.0.1:18080}
    ;;
  mssql)
    entrypoint=""
    database_option_key="uri"
    database_option_value=${ADBC_MSSQL_URI:-sqlserver://sa:Adbc_Test_Pass123@127.0.0.1:1433}
    ;;
esac

VALIDATION_CONFIG="$config" \
VALIDATION_PORT="$port" \
VALIDATION_TCP_PORT="$tcp_port" \
VALIDATION_TOKEN="$token" \
VALIDATION_TRANSPORT="$transport" \
VALIDATION_IROH_SECRET="$iroh_secret" \
VALIDATION_IROH_INFO="$iroh_info" \
VALIDATION_IROH_MAX_ACTIVE_STREAMS="${GRAINLIFT_IROH_MAX_ACTIVE_STREAMS:-1024}" \
VALIDATION_IROH_MAX_ACTIVE_STREAMS_PER_CONNECTION="${GRAINLIFT_IROH_MAX_ACTIVE_STREAMS_PER_CONNECTION:-64}" \
VALIDATION_TLS_CA="$tls_ca" \
VALIDATION_TLS_SERVER_CERT="$tls_server_cert" \
VALIDATION_TLS_SERVER_KEY="$tls_server_key" \
VALIDATION_BACKEND="$backend" \
VALIDATION_DRIVER="$downstream_driver" \
VALIDATION_ENTRYPOINT="$entrypoint" \
VALIDATION_DATABASE_OPTION_KEY="$database_option_key" \
VALIDATION_DATABASE_OPTION_VALUE="$database_option_value" \
VALIDATION_FLIGHTSQL_USERNAME="${ADBC_FLIGHTSQL_USERNAME:-sqlflite_username}" \
VALIDATION_FLIGHTSQL_PASSWORD="${ADBC_FLIGHTSQL_PASSWORD:-flight_password}" \
VALIDATION_TRINO_USERNAME="${ADBC_TRINO_USERNAME:-trino}" \
VALIDATION_MODE="$mode" \
"$python_cmd" - <<'PY'
import json
import os
from pathlib import Path

def quoted(value: str) -> str:
    return json.dumps(value)

authenticated = os.environ["VALIDATION_TRANSPORT"] in {"http", "mtls"}
contents = f'''[server]
listen = "127.0.0.1:{os.environ["VALIDATION_PORT"]}"
session_ttl_seconds = 300
session_reap_interval_seconds = 5
require_authentication = {str(authenticated).lower()}
request_timeout_seconds = {os.environ.get("GRAINLIFT_VALIDATION_REQUEST_TIMEOUT_SECONDS", "300")}
'''
if value := os.environ.get("GRAINLIFT_SERVER_MAX_BIND_BYTES", os.environ.get("GRAINLIFT_MAX_BIND_BYTES")):
    contents += f'max_bind_bytes = {int(value)}\n'
if value := os.environ.get("GRAINLIFT_VALIDATION_MAX_REQUEST_BODY_BYTES"):
    contents += f'max_request_body_bytes = {int(value)}\n'
if os.environ["VALIDATION_MODE"] == "load":
    contents += '''max_sessions = 2048
max_sessions_per_principal = 2048
max_statements_per_session = 8
max_results_per_session = 8
'''

if authenticated:
    contents += f'''

[auth.static_bearer_tokens]
{quoted(os.environ["VALIDATION_TOKEN"])} = "validation-runner"

[auth.target_permissions]
"validation-runner" = [{quoted(os.environ["VALIDATION_BACKEND"])}]
'''
    if os.environ["VALIDATION_TRANSPORT"] == "mtls":
        contents += f'''"peer/spiffe/spiffe%3A%2F%2Fvalidation.test/spiffe%3A%2F%2Fvalidation.test%2Fclient" = [{quoted(os.environ["VALIDATION_BACKEND"])}]
'''

if os.environ["VALIDATION_TRANSPORT"] == "tcp":
    contents += f'''

[tcp]
listen = "127.0.0.1:{os.environ["VALIDATION_TCP_PORT"]}"
allow_insecure = false
'''
elif os.environ["VALIDATION_TRANSPORT"] == "mtls":
    contents += f'''

[tcp]
listen = "127.0.0.1:{os.environ["VALIDATION_TCP_PORT"]}"

[tcp.tls]
server_certificate_chain = {quoted(os.environ["VALIDATION_TLS_SERVER_CERT"])}
server_private_key = {quoted(os.environ["VALIDATION_TLS_SERVER_KEY"])}
client_ca = {quoted(os.environ["VALIDATION_TLS_CA"])}
trust_domains = ["validation.test"]
'''
elif os.environ["VALIDATION_TRANSPORT"] == "iroh":
    contents += f'''

[iroh]
issuer = "validation"
secret_key_file = {quoted(os.environ["VALIDATION_IROH_SECRET"])}
endpoint_info_file = {quoted(os.environ["VALIDATION_IROH_INFO"])}
disable_relays = true
max_active_streams = {int(os.environ["VALIDATION_IROH_MAX_ACTIVE_STREAMS"])}
max_active_streams_per_connection = {int(os.environ["VALIDATION_IROH_MAX_ACTIVE_STREAMS_PER_CONNECTION"])}
'''

entrypoint = os.environ["VALIDATION_ENTRYPOINT"]
contents += f'''

[targets.{os.environ["VALIDATION_BACKEND"]}]
driver = {quoted(os.environ["VALIDATION_DRIVER"])}
allow_client_database_options = false
allow_client_connection_options = false
allowed_client_connection_options = [
  "adbc.connection.autocommit",
  "adbc.connection.readonly",
  "adbc.connection.catalog",
  "adbc.connection.db_schema",
  "adbc.connection.transaction.isolation_level",
]
'''
if entrypoint:
    contents += f'entrypoint = {quoted(entrypoint)}\n'

database_options = []
client_database_option = (
    os.environ["VALIDATION_DATABASE_OPTION_KEY"]
    if os.environ["VALIDATION_BACKEND"] == "sqlite"
    else ""
)
if client_database_option:
    contents += f'allowed_client_database_options = [{quoted(client_database_option)}]\n'
elif os.environ["VALIDATION_DATABASE_OPTION_KEY"]:
    database_options.append(
        (os.environ["VALIDATION_DATABASE_OPTION_KEY"], os.environ["VALIDATION_DATABASE_OPTION_VALUE"])
    )
if os.environ["VALIDATION_BACKEND"] == "flightsql":
    database_options.extend(
        [
            ("username", os.environ["VALIDATION_FLIGHTSQL_USERNAME"]),
            ("password", os.environ["VALIDATION_FLIGHTSQL_PASSWORD"]),
        ]
    )
elif os.environ["VALIDATION_BACKEND"] == "trino":
    database_options.append(("username", os.environ["VALIDATION_TRINO_USERNAME"]))

for key, value in database_options:
    contents += f'''

[[targets.{os.environ["VALIDATION_BACKEND"]}.database_options]]
key = {quoted(key)}
type = "string"
value = {quoted(value)}
'''
Path(os.environ["VALIDATION_CONFIG"]).write_text(contents)
PY

OTEL_SDK_DISABLED=true \
RUST_LOG="${RUST_LOG:-grainlift_server=info}" \
"$repo_root/target/release/grainlift-server" --config "$config" \
  >"$work_dir/server.log" 2>&1 &
server_pid=$!

ready=0
for _ in {1..100}; do
  if ! kill -0 "$server_pid" 2>/dev/null; then
    break
  fi
  if curl --fail --silent --output /dev/null "$health_endpoint/readyz"; then
    ready=1
    break
  fi
  sleep 0.1
done
if [[ "$ready" != "1" ]]; then
  echo "Grainlift server did not become ready" >&2
  sed -n '1,240p' "$work_dir/server.log" >&2
  exit 1
fi

case "$transport" in
  http)
    endpoint="$health_endpoint"
    ;;
  tcp)
    endpoint="tcp://127.0.0.1:$tcp_port"
    token=""
    ;;
  mtls)
    endpoint="tls+tcp://127.0.0.1:$tcp_port"
    token=""
    export GRAINLIFT_TLS_CA="$tls_ca"
    export GRAINLIFT_TLS_CERT="$tls_client_cert"
    export GRAINLIFT_TLS_KEY="$tls_client_key"
    export GRAINLIFT_TLS_SERVER_NAME="localhost"
    ;;
  iroh)
    for _ in {1..100}; do
      [[ -s "$iroh_info" ]] && break
      sleep 0.1
    done
    if [[ ! -s "$iroh_info" ]]; then
      echo "Grainlift did not publish Iroh endpoint information" >&2
      sed -n '1,240p' "$work_dir/server.log" >&2
      exit 1
    fi
    endpoint=$("$python_cmd" - "$iroh_info" <<'PY'
import json
import sys
record = json.load(open(sys.argv[1]))
print(f'iroh://{record["endpoint_id"]}')
PY
)
    iroh_direct_address=$("$python_cmd" - "$iroh_info" <<'PY'
import json
import sys
record = json.load(open(sys.argv[1]))
if not record["direct_addresses"]:
    raise SystemExit("Iroh endpoint published no direct addresses")
print(record["direct_addresses"][0])
PY
)
    export GRAINLIFT_IROH_DIRECT_ADDRESS="$iroh_direct_address"
    token=""
    ;;
esac

export GRAINLIFT_DRIVER="$grainlift_driver"
export GRAINLIFT_ENDPOINT="$endpoint"
export GRAINLIFT_TOKEN="$token"
export GRAINLIFT_TARGET="$target"
export GRAINLIFT_BACKEND="$backend"
export GRAINLIFT_TRANSPORT="$transport"
export GRAINLIFT_SERVER_PID="$server_pid"
if [[ "$backend" == "sqlite" ]]; then
  export GRAINLIFT_DOWNSTREAM_URI="$database_option_value"
else
  unset GRAINLIFT_DOWNSTREAM_URI || true
fi

if [[ "$mode" == "serve" ]]; then
  env_file=${GRAINLIFT_ENV_FILE:?GRAINLIFT_ENV_FILE is required in serve mode}
  env_file_tmp="$env_file.tmp"
  {
    printf 'export GRAINLIFT_DRIVER=%q\n' "$GRAINLIFT_DRIVER"
    printf 'export GRAINLIFT_ENDPOINT=%q\n' "$GRAINLIFT_ENDPOINT"
    printf 'export GRAINLIFT_TOKEN=%q\n' "$GRAINLIFT_TOKEN"
    printf 'export GRAINLIFT_TARGET=%q\n' "$GRAINLIFT_TARGET"
    printf 'export GRAINLIFT_BACKEND=%q\n' "$GRAINLIFT_BACKEND"
    printf 'export GRAINLIFT_TRANSPORT=%q\n' "$GRAINLIFT_TRANSPORT"
    if [[ -n "${GRAINLIFT_DOWNSTREAM_URI:-}" ]]; then
      printf 'export GRAINLIFT_DOWNSTREAM_URI=%q\n' "$GRAINLIFT_DOWNSTREAM_URI"
    fi
  } >"$env_file_tmp"
  mv "$env_file_tmp" "$env_file"
  wait "$server_pid"
  exit $?
fi

case "$mode" in
  example)
    uv run --project "$validation_root" --python 3.13 \
      python "$repo_root/examples/python_client.py"
    ;;
  smoke)
    uv run --project "$validation_root" --python 3.13 \
      python "$validation_root/external_smoke.py"
    ;;
  foundry)
    uv run --project "$validation_root" --python 3.13 --extra foundry \
      pytest "$validation_root/tests" "$@"
    ;;
  load)
    uv run --project "$validation_root" --python 3.13 \
      python "$validation_root/load_test.py" "$@"
    ;;
  large-payload)
    uv run --project "$validation_root" --python 3.13 \
      python "$validation_root/large_payload.py" "$@"
    ;;
  *)
    echo "Usage: $0 [example|smoke|foundry|load|large-payload|serve] [sqlite|duckdb|postgresql|mysql|flightsql|datafusion|trino|mssql] [arguments...]" >&2
    exit 2
    ;;
esac
