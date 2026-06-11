# kroxy

A multi-tenant Kafka proxy written in Go.

`kroxy` sits in front of a single Apache Kafka cluster and turns it into a
multi-tenant service. It terminates SASL (PLAIN, SCRAM-SHA-256,
SCRAM-SHA-512) at the edge, uses the client's username to look up a
tenant, and rewrites every topic, consumer group and transactional ID
with a per-tenant prefix on the way to the upstream broker (and back).
Each tenant sees a flat namespace that looks like its own dedicated
cluster; the broker sees fully-qualified, prefixed names.

The proxy is a single static binary with no external dependencies beyond
Kafka itself.

---

## Table of contents

- [Why](#why)
- [How it works](#how-it-works)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [Authentication model](#authentication-model)
- [Admin RPC](#admin-rpc)
- [Observability](#observability)
- [Building & testing](#building--testing)
- [Project layout](#project-layout)
- [Limitations](#limitations)
- [License](#license)

---

## Why

Running one Kafka cluster per tenant is expensive and operationally
painful. Sharing a single cluster naively gives every tenant the keys to
every topic. `kroxy` is a thin, transparent shim that gives you the
cost profile of a shared cluster with the namespace isolation of a
dedicated one:

- **One cluster, many tenants** — the upstream broker is shared.
- **Hard topic-name isolation** — a tenant cannot name, list, produce
  to, fetch from, or commit offsets against a topic outside its own
  prefix.
- **No client changes** — clients speak vanilla Kafka with vanilla
  SASL/PLAIN. They just point at the proxy.
- **No secrets in the proxy** — kroxy forwards SASL credentials to the
  upstream cluster verbatim. The broker remains the only thing that
  knows passwords.

## How it works

```
  ┌──────────┐  SASL/PLAIN   ┌────────┐   SASL/PLAIN   ┌─────────┐
  │  client  │──────────────▶│ kroxy  │───────────────▶│  Kafka  │
  └──────────┘   topic=foo   └────────┘ topic=tenantA. └─────────┘
                                              foo
```

For each accepted client connection kroxy:

1. Reads the SASL/PLAIN handshake. The username **is** the tenant ID;
   the password is held in memory for the life of the connection and
   forwarded to the upstream broker on the first dial.
2. Looks up the tenant in the resolver to get the topic prefix and
   upstream address. Unknown tenant IDs are rejected before any upstream
   round-trip.
3. Lazily opens a single TCP connection to the tenant's upstream Kafka
   broker, performs SASL/PLAIN against it using the client-supplied
   password, and re-uses that connection for the lifetime of the client.
4. For every Kafka request, decodes the typed request struct
   (via `franz-go/kmsg`), rewrites topic / group / transactional IDs by
   prepending the tenant prefix, forwards it, and reverses the rewrite
   on the response. `Metadata` and `ListGroups` responses are filtered
   so the tenant only sees its own names with the prefix stripped.
5. Collapses the upstream broker list to a single virtual broker
   (NodeID 0) advertised as the proxy itself, so clients always come
   back through kroxy.

Requests are pipelined per-client but serialised one-in-flight-at-a-time
to the upstream connection, so request/response ordering is trivially
preserved without correlation-ID juggling.

## Quick start

The fastest way to see kroxy in action is the bundled compose stack,
which runs a single-node Apache Kafka in KRaft mode plus kroxy with two
demo tenants (`tenantA`, `tenantB`).

```bash
make compose-up      # bring up kafka + kroxy
make compose-logs    # follow logs (Ctrl-C to detach)
make compose-down    # tear it all down
```

Ports published on the host:

| Port    | Service              |
| ------- | -------------------- |
| `19092` | kroxy (Kafka)        |
| `19090` | kroxy (`/metrics`)   |
| `19095` | kroxy (admin JSON-RPC) |

Produce and consume through the proxy with any Kafka client. Using the
`kafka-console-*` scripts that ship in the official image:

```bash
# Produce as tenantA
docker run --rm -i --network host apache/kafka:3.8.0 \
  /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:19092 \
    --producer-property security.protocol=SASL_PLAINTEXT \
    --producer-property sasl.mechanism=PLAIN \
    --producer-property 'sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="tenantA" password="tenantApw";' \
    --topic orders

# Consume as tenantA — sees "orders", not "tenantA.orders"
docker run --rm -i --network host apache/kafka:3.8.0 \
  /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:19092 \
    --consumer-property security.protocol=SASL_PLAINTEXT \
    --consumer-property sasl.mechanism=PLAIN \
    --consumer-property 'sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="tenantA" password="tenantApw";' \
    --topic orders --from-beginning
```

If you talk to the broker directly on port `9093` (with broker-admin
credentials) you will see the topic as `tenantA.orders`.

### Run from source

```bash
make build
./bin/kroxy -config dockerfiles/kroxy.yaml
```

## Configuration

kroxy reads a single YAML file. Minimal example:

```yaml
listen: ":9092"               # client-facing Kafka listener
advertised: "kroxy:9092"      # what kroxy advertises as broker 0

tls:                          # optional; omit for a plaintext listener
  enabled: true
  cert_file: /etc/kroxy/certs/server.crt
  key_file:  /etc/kroxy/certs/server.key

upstream:
  bootstrap: "kafka:9093"     # default upstream for tenants that omit it
  sasl:                       # optional; omit for PLAIN pass-through
    mechanism: SCRAM-SHA-256  # authenticate PLAIN clients to the broker via SCRAM

resolver:
  type: memory                # only "memory" is supported in v1
  memory:
    tenants:
      - id: tenantA
        topic_prefix: "tenantA."
        upstream: "kafka:9093"  # optional; falls back to upstream.bootstrap

log:
  level: info                 # debug | info | warn | error
  format: json                # json | text

metrics:
  enabled: true
  listen: ":9090"             # /metrics + /healthz

admin:
  enabled: false              # JSON-RPC tenant management; off by default
  listen: "127.0.0.1:9095"
```

Notes:

- `advertised` is mandatory. It is what kroxy returns in `Metadata`
  responses, so clients must be able to reach the proxy at this address.
- `upstream.bootstrap` is mandatory and is used as the default upstream
  for any tenant that doesn't specify its own `upstream`.
- `resolver.type` defaults to `"memory"` (the only backend in v1; the
  field exists as an extension point for future backends).
- The `resolver.memory.tenants` list may be empty **only if** the admin
  RPC is enabled — otherwise the proxy has nothing to authorise against.
- A tenant's `id` and `topic_prefix` are both required.
- `tls` is optional. When `enabled`, kroxy terminates TLS on the client
  listener using `cert_file`/`key_file` (both required); the upstream broker
  connection is unaffected. Omit the block for a plaintext listener. Server-side
  TLS only — clients are not asked for a certificate, and the keypair is loaded
  once at startup (restart to rotate).
- `upstream.sasl.mechanism` is optional. Leave it empty (the default) to
  forward PLAIN clients' credentials to the broker verbatim. Set it to
  `SCRAM-SHA-256` or `SCRAM-SHA-512` to make kroxy authenticate PLAIN clients
  to the upstream broker with that SCRAM mechanism instead — see
  [PLAIN→SCRAM translation](#plainscram-translation).

## Authentication model

kroxy is a SASL **pass-through**. It supports three mechanisms:

- **PLAIN** — single-shot. Username == tenant ID, password forwarded
  verbatim to the upstream broker.
- **SCRAM-SHA-256** — challenge/response. kroxy peeks only at the SASLname
  in the SCRAM `client-first-message` (== tenant ID) for routing, then
  relays every `SaslAuthenticate` frame between client and upstream
  unchanged.
- **SCRAM-SHA-512** — same model as SCRAM-SHA-256.

The upstream Kafka cluster is the sole authentication authority for all
three mechanisms. **kroxy stores no client secrets** and does not validate
passwords or SCRAM proofs itself.

Consequences:

- Every tenant ID must be a real principal in the upstream broker
  (declared in its JAAS file for PLAIN, or registered as SCRAM credentials
  via `kafka-configs.sh --add-config 'SCRAM-SHA-256=[password=...]'` for
  SCRAM).
- Unknown tenant IDs are rejected at the proxy before any upstream dial,
  returning a SASL authentication failure.
- For PLAIN, the password is held in memory for the duration of the client
  connection (to support upstream reconnection) and never written to logs.
  For SCRAM, kroxy never observes the password at all.
- SASL channel binding (`y`, `p=...`) is not supported — kroxy is not the
  TLS terminator for the SCRAM exchange.

The only thing kroxy needs to know about a tenant is the mapping
`id → (topic_prefix, upstream)`.

### PLAIN→SCRAM translation

Some clients can only speak SASL/PLAIN, while the broker provisions
per-tenant **SCRAM** credentials and no static PLAIN principals. Setting
`upstream.sasl.mechanism` to a SCRAM mechanism bridges the two: a client
authenticates to kroxy with PLAIN, and kroxy authenticates to the upstream
broker with SCRAM, computing the proof itself from the tenant ID and the
client-supplied password. The tenant's SCRAM password must therefore equal
the PLAIN password the client sends.

This is distinct from the pass-through SCRAM mechanism above. It applies
**only** to clients that connect with PLAIN — kroxy needs the plaintext
password to compute a SCRAM proof, and only PLAIN reveals it. Clients that
already speak SCRAM are relayed verbatim regardless of this setting, and when
`upstream.sasl.mechanism` is empty kroxy holds no secret and changes nothing.

## Admin RPC

kroxy exposes a JSON-RPC 2.0 admin API for managing tenants at runtime. See [admin/RPC.md](admin/RPC.md) for methods, request/response shapes, error codes, and curl examples.

## Observability

### Logs

Structured logs via the standard library `log/slog`. Configure level
(`debug`/`info`/`warn`/`error`) and format (`json`/`text`) under `log:`.
Passwords and other sensitive material are never logged.

### Metrics

When `metrics.enabled` is true, kroxy serves Prometheus metrics on
`metrics.listen` at `/metrics`, plus a liveness check at `/healthz`.

| Metric                              | Type      | Labels             | Description                                     |
| ----------------------------------- | --------- | ------------------ | ----------------------------------------------- |
| `kroxy_connections_active`          | gauge     | —                  | Currently open client connections.              |
| `kroxy_connections_total`           | counter   | —                  | Client connections accepted since startup.      |
| `kroxy_requests_total`              | counter   | `api_key`, `tenant`| Kafka requests handled.                         |
| `kroxy_request_duration_seconds`    | histogram | `api_key`, `tenant`| Wall-clock time per request.                    |
| `kroxy_upstream_errors_total`       | counter   | `kind`             | Upstream connection / round-trip errors.        |
| `kroxy_resolver_calls_total`        | counter   | `result`           | Resolver lookups (`hit`/`miss`).                |

## Building & testing

The `Makefile` is the source of truth for development tasks. `make help`
lists everything; the most useful targets:

```bash
make build              # compile ./bin/kroxy
make run                # build and run with dockerfiles/kroxy.yaml
make fmt vet tidy       # gofmt, go vet (incl. integration tag), go mod tidy
make lint               # golangci-lint run
make test               # unit tests
make test-race          # unit tests with -race
make test-integration   # full SASL end-to-end suite (requires Docker)
make docker-build       # build the kroxy image
make compose-up         # demo stack (kafka + kroxy)
make compose-down
make compose-logs
```

The integration suite spins up a real Apache Kafka container with
SASL/PLAIN configured and exercises produce/consume/admin paths through
the proxy. It needs a working Docker socket; on macOS that usually
means setting `DOCKER_HOST=unix:///$HOME/.docker/run/docker.sock`.

CI runs `go fmt`, `go vet` (with and without the `integration` tag),
`go mod tidy`, `golangci-lint`, `go test ./... -race`, and the
integration suite on every push.

## Project layout

```
cmd/kroxy/         # main package — wires proxy + metrics + admin RPC
config/            # YAML loader and validation
resolver/          # tenant lookup interface + in-memory implementation
auth/              # SASL/PLAIN parsing + log redaction
protocol/          # Kafka wire-format primitives (framing, headers, …)
proxy/             # listener, per-conn state machine, request rewriters
rewrite/           # pure functions for prefixing/stripping names
upstream/          # upstream broker connection + SASL/PLAIN handshake
admin/             # JSON-RPC admin service, types, server and client
observability/     # slog logger + Prometheus metrics
integration/       # testcontainers-backed end-to-end suite (build tag)
dockerfiles/       # Dockerfile, demo compose stack, sample configs
examples/          # admin-curl.sh helper
```

## Limitations

v1 is deliberately small. The following are explicitly out of scope and
deferred:

- **Client TLS termination** is supported via the `tls` config block
  (server-side only, no mTLS, no hot reload). **Upstream TLS is not** — the
  connection to the broker is always plaintext, so run kroxy on a trusted
  network relative to the broker.
- **SASL/PLAIN, SCRAM-SHA-256, SCRAM-SHA-512.** No OAUTHBEARER, no
  mTLS, no Kerberos. No SASL channel binding.
- **Single shared upstream cluster.** Per-tenant `upstream` is plumbed
  through but every tenant in the demo points at the same broker.
- **No hot config reload.** Restart to pick up YAML changes; use the
  admin RPC for live tenant changes.
- **No rate limiting or per-tenant quotas.**
- **No authentication on the admin RPC.** Bind to loopback or gate it
  externally.
- **Topic-ID-keyed admin variants** (newer `DeleteTopics`,
  `OffsetForLeaderEpoch` flavours) are capped at versions that use
  topic names so the rewriter can do its job.
- **No share groups (KIP-932).**

## License

[MIT](LICENSE) © Bubunyo Nyavor
