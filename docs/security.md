# Security and resource controls

The service authenticates every VGI HTTP request and binds persistent-transport
identity once at its cryptographic connection handshake. Session handles are
opaque capabilities but are also bound to the authentication domain and
principal that created them.

Two authentication modes are available and are mutually exclusive:

- Static bearer tokens are intended for local development and controlled
  service-to-service deployments. They have no expiry or rotation protocol.
- JWT bearer tokens are signature-checked against a configured HTTPS JWKS URL.
  Issuer, audience, expiry, not-before, and a nonblank principal claim are
  validated by VGI-RPC. Unknown key IDs cause a guarded JWKS refresh.

The current server executable serves plaintext HTTP. Its default configuration
therefore refuses to bind outside loopback. Production deployments must put a
TLS-terminating reverse proxy, sidecar, or private authenticated service mesh in
front of the loopback listener. `allow_insecure_remote = true` only disables
that startup check; it does not enable encryption and must not be used on an
untrusted network.

Plain `tcp://` carries neither encryption nor authentication. It is refused on
non-loopback interfaces unless explicitly acknowledged and cannot be enabled
when authentication is required. `tls+tcp://` uses mandatory client
certificates: the server verifies the certificate chain and a strict
X.509-SVID URI SAN, then binds the SPIFFE identity to every call on that
connection. Client certificate, key, CA, and verified server name are explicit
driver options; there is no certificate-verification bypass.

VGI namespaces a verified SPIFFE workload into a collision-resistant
application principal. For example, `spiffe://prod.example.org/worker` in the
`prod.example.org` trust domain becomes
`peer/spiffe/spiffe%3A%2F%2Fprod.example.org/spiffe%3A%2F%2Fprod.example.org%2Fworker`.
Use that canonical value in `auth.target_permissions`; the original SPIFFE ID
is also retained as an authenticated claim for policy and telemetry.

Raw `iroh://` uses Iroh's cryptographic endpoint identity and long-lived QUIC
streams, not HTTP. A production configuration must keep its Iroh secret key in
a secret store so the endpoint ID remains stable, and must map allowed client
endpoint IDs to principals in `iroh.principals`. Endpoint-key possession is
not, by itself, organizational membership. The optional endpoint information
file contains only public discovery information.

`auth.target_permissions` is a principal-to-target allowlist. If the map is
empty, every authenticated principal can use every configured target. Once it
contains an entry, unlisted principals are denied all targets. A literal `*`
target grants all targets. Session ownership checks continue to prevent one
authorized principal from using another principal's connection handles.

The server enforces limits on decoded HTTP request size, request duration,
global sessions, sessions per principal, and statements and results per
session. Opening calls reserve quota before loading a downstream connection,
so concurrent opens cannot exceed the configured bounds. A background reaper
removes expired leases, and graceful SIGTERM/Ctrl-C shutdown detaches all
sessions and drops idle driver resources. An operation already in flight keeps
its reference and is allowed to finish during Axum's graceful drain.

Unauthenticated `GET /healthz` and `GET /readyz` probes return 204 after the
process and complete RPC/authentication configuration have initialized. VGI's
equivalent `GET /health` endpoint remains enabled. Readiness is process-level;
it deliberately does not open every downstream database on each probe.

Credentials configured on a target override client-provided options. Client
database and connection options are discarded unless the corresponding target
allow flag is explicitly enabled. Do not place bearer tokens or downstream
passwords in a committed TOML file; inject the runtime configuration through a
secret-managed deployment mechanism.
