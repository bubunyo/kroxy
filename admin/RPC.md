# Admin RPC

kroxy exposes a JSON-RPC 2.0 service for managing the in-memory tenant
table at runtime. It runs in the same binary as the proxy, on its own
port.

> **Warning** — the admin RPC has **no authentication** in v1. It binds
> to `127.0.0.1:9095` by default. If you expose it on any other
> interface, gate access at the network layer (firewall, mesh, reverse
> proxy with auth, …). The data-plane SASL/PLAIN password is never
> handled by this API; that is a pass-through concern of the Kafka
> listener.

---

## Overview

- Single endpoint: `POST /rpc`
- Liveness probe: `GET /healthz` returns `200 ok`
- Wire format: JSON-RPC 2.0 over HTTP
- Default bind: `127.0.0.1:9095` (configurable)
- All methods are dispatched as `Tenants.<Method>`

## Enabling the endpoint

```yaml
admin:
  enabled: true
  listen: "127.0.0.1:9095"
```

## Transport conventions

Every request is a JSON object:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "Tenants.<Method>",
  "params": { ... }
}
```

Every response is a JSON object with either `result` or `error`:

```json
{ "jsonrpc": "2.0", "id": 1, "result": { ... } }
{ "jsonrpc": "2.0", "id": 1, "error": { "code": -32011, "message": "..." } }
```

`Content-Type: application/json` is required on requests.

---

## Methods

### `Tenants.Set`

Creates a brand-new tenant. **Create-only** — to change an existing
tenant, call `Tenants.Delete` first and then `Tenants.Set` again.

**Params**

| Field          | Type   | Required | Description                                                       |
| -------------- | ------ | -------- | ----------------------------------------------------------------- |
| `id`           | string | yes      | Tenant identifier; the SASL/PLAIN username clients will present.  |
| `topic_prefix` | string | yes      | Prepended to every topic, group and transactional ID upstream.    |
| `upstream`     | string | yes      | `host:port` of the Kafka broker this tenant should be routed to.  |

**Result**

```json
{ "ok": true }
```

**Errors**

| Code     | Meaning                                            |
| -------- | -------------------------------------------------- |
| `-32011` | a tenant with this `id` already exists             |
| `-32013` | invalid payload (missing/empty required field)     |

**Example**

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

```json
{ "jsonrpc": "2.0", "id": 1, "result": { "ok": true } }
```

---

### `Tenants.Delete`

Removes a tenant. Existing client connections authenticated as that
tenant are not forcibly closed; only future authentications fail.

**Params**

| Field | Type   | Required | Description                |
| ----- | ------ | -------- | -------------------------- |
| `id`  | string | yes      | Tenant identifier to drop. |

**Result**

```json
{ "ok": true }
```

**Errors**

| Code     | Meaning                                  |
| -------- | ---------------------------------------- |
| `-32012` | no tenant with this `id` exists          |
| `-32013` | invalid payload (e.g. empty `id`)        |

**Example**

```bash
curl -sS -X POST http://127.0.0.1:9095/rpc \
  -H 'Content-Type: application/json' \
  -d '{
        "jsonrpc": "2.0",
        "id": 2,
        "method": "Tenants.Delete",
        "params": { "id": "tenantA" }
      }'
```

```json
{ "jsonrpc": "2.0", "id": 2, "result": { "ok": true } }
```

---

### `Tenants.List`

Returns a snapshot of every configured tenant.

**Params**

None. Send `{}` (or omit `params` entirely).

**Result**

```json
{
  "tenants": [
    { "id": "tenantA", "topic_prefix": "tenantA.", "upstream": "kafka:9093" },
    { "id": "tenantB", "topic_prefix": "tenantB.", "upstream": "kafka:9093" }
  ]
}
```

Order is unspecified.

**Example**

```bash
curl -sS -X POST http://127.0.0.1:9095/rpc \
  -H 'Content-Type: application/json' \
  -d '{
        "jsonrpc": "2.0",
        "id": 3,
        "method": "Tenants.List",
        "params": {}
      }'
```

```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "result": {
    "tenants": [
      { "id": "tenantA", "topic_prefix": "tenantA.", "upstream": "kafka:9093" },
      { "id": "tenantB", "topic_prefix": "tenantB.", "upstream": "kafka:9093" }
    ]
  }
}
```

---

## Error codes

Standard JSON-RPC 2.0 framework errors (`-32700` … `-32600`: parse
error, invalid request, method not found, invalid params, internal
error) apply as defined by the spec. Codes `-32001` … `-32004` are
reserved by the underlying [`go-rpc`](https://github.com/bubunyo/go-rpc)
library for transport-level conditions.

Domain-specific codes used by kroxy:

| Code     | Meaning                                           |
| -------- | ------------------------------------------------- |
| `-32011` | tenant already exists                             |
| `-32012` | tenant not found                                  |
| `-32013` | invalid request payload / missing required field  |

Any other resolver-level error surfaces as a generic JSON-RPC server
error with the original message.

## Helper script

A small bash wrapper lives at [`../examples/admin-curl.sh`](../examples/admin-curl.sh):

```bash
ADMIN=http://localhost:19095/rpc ./examples/admin-curl.sh list
ADMIN=http://localhost:19095/rpc ./examples/admin-curl.sh \
  set tenantA "tenantA." kafka:9093
ADMIN=http://localhost:19095/rpc ./examples/admin-curl.sh delete tenantA
```

`$ADMIN` defaults to `http://127.0.0.1:9095/rpc`.
