# kroxy

A multi-tenant Kafka proxy written in Go.

`kroxy` sits in front of a single Apache Kafka cluster and turns it into a
multi-tenant service. It terminates SASL/PLAIN at the edge, uses the
client's username to look up a tenant, and rewrites every topic, consumer
group and transactional ID with a per-tenant prefix on the way to the
upstream broker (and back). Each tenant sees a flat namespace that looks
like its own dedicated cluster; the broker sees fully-qualified,
prefixed names.

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

upstream:
  bootstrap: "kafka:9093"     # default upstream for tenants that omit it

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

## Authentication model

kroxy is a SASL/PLAIN **pass-through**. The SASL username on the wire
is the tenant ID; the password is forwarded verbatim to the tenant's
upstream Kafka cluster, which is the sole auth authority. **kroxy
stores no client secrets** and does not validate passwords itself.

Consequences:

- Every tenant ID must be a real principal in the upstream broker
  (declared in its JAAS file or auth backend, e.g. `kafka_jaas.conf`).
- Unknown tenant IDs are rejected at the proxy before any upstream
  dial, returning a SASL authentication failure.
- Passwords are held in memory for the duration of the client
  connection (to be able to reconnect to upstream on failure) and never
  written to logs.

The only thing kroxy needs to know about a tenant is the mapping
`id → (topic_prefix, upstream)`.

## Admin RPC

kroxy exposes a JSON-RPC 2.0 endpoint for managing the in-memory tenant
table at runtime. It runs in the same binary as the proxy on its own
port.

> **Warning** — the admin RPC has **no authentication** in v1. It binds
> to `127.0.0.1:9095` by default. If you expose it on any other
> interface, gate it at the network layer (firewall, mesh, reverse
> proxy with auth, …).

Enable it in `kroxy.yaml`:

```yaml
admin:
  enabled: true
  listen: "127.0.0.1:9095"
```

### Methods

All methods live under the `Tenants` service.

| Method           | Params                                  | Result                 |
| ---------------- | --------------------------------------- | ---------------------- |
| `Tenants.Set`    | `id`, `topic_prefix`, `upstream`        | `{ "ok": true }`       |
| `Tenants.Delete` | `id`                                    | `{ "ok": true }`       |
| `Tenants.List`   | _(none)_                                | `{ "tenants": [...] }` |

`Set` is create-only — to mutate an existing tenant, `Delete` then
`Set` again.

### Domain error codes

| Code     | Meaning                                  |
| -------- | ---------------------------------------- |
| `-32011` | tenant already exists                    |
| `-32012` | tenant not found                         |
| `-32013` | invalid request payload / missing field  |

(Codes `-32001` … `-32004` are reserved by the JSON-RPC framework.)

### curl

```bash
curl -sS -X POST http://127.0.0.1:9095/rpc \
  -H 'Content-Type: application/json' \
  -d '{
        "jsonrpc": "2.0",
        "id": 1,
        "method": "Tenants.Set",
        "params": {
          "id":           "tenantA",
          "topic_prefix": "tenantA.",
          "upstream":     "kafka:9093"
        }
      }'
```

A small helper script lives at `examples/admin-curl.sh`:

```bash
ADMIN=http://localhost:19095/rpc ./examples/admin-curl.sh list
ADMIN=http://localhost:19095/rpc ./examples/admin-curl.sh \
  set tenantA "tenantA." kafka:9093
ADMIN=http://localhost:19095/rpc ./examples/admin-curl.sh delete tenantA
```

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

- **No TLS** on either the client or upstream side. Run kroxy on a
  trusted network or behind a TLS-terminating sidecar.
- **SASL/PLAIN only.** No SCRAM, no OAUTHBEARER, no mTLS, no Kerberos.
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
