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

# Local TLS-edge validation

This opt-in harness runs a supplied Caddy binary in front of an independent
Waitress process and the Grainlift Python service. It does not download tools,
install certificates into system trust, or change certificate verification.
OpenSSL generates a temporary CA and a one-day localhost certificate. Temporary
keys, credentials, configurations, and raw logs are deleted when the run ends.

From `validation/regression`, after installing the regression environment and
building the native driver:

```console
uv run --no-sync python -m deployment \
  --caddy ../../target/deployment-validation/caddy \
  --driver ../../target/debug/libadbc_driver_grainlift.dylib \
  --output ../load-results/tls-edge-local.json
```

Use the platform's shared-library suffix and an independently verified Caddy
executable. The command fails on any check failure; only a successful run writes
the sanitized JSON report. It exercises verified HTTPS Arrow queries, missing
and invalid bearer tokens, untrusted CAs, hostname mismatch, request and result
boundaries, private-error sanitization, log redaction, and graceful shutdown
while an HTTP request is active.

The 4096-byte request quota is inclusive at Caddy and the SDK. Waitress rejects
bodies greater than **or equal to** its `max_request_body_size`, so the harness
configures its threshold to 4097. Caddy and the SDK still enforce the 4096-byte
quota. Below-limit and exact-limit boundary bodies intentionally contain invalid
Arrow and return HTTP 400; the body above the quota returns HTTP 413.

Positive HTTPS validation uses the Python VGI client with the ephemeral CA
explicitly trusted. Native ADBC HTTPS verifies rejection of the untrusted CA.
The native HTTP client currently has no custom-CA option; its
`grainlift.tls.ca` option applies to mTLS TCP. This run does not establish native
HTTPS success against a publicly trusted certificate, certificate renewal,
external network policy, or multi-host deployment behavior.
