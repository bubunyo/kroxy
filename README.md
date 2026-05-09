# kroxy

Multi-tenant Kafka proxy. Terminates SASL/PLAIN at the edge, resolves a
tenant from the username, and rewrites topic / consumer-group /
transactional IDs with a per-tenant prefix against an upstream Kafka
cluster.

See `dockerfiles/docker-compose.yml` for a runnable demo stack.

## Authentication

kroxy is a SASL/PLAIN **pass-through**. The username selects the tenant;
the password is forwarded verbatim to the tenant's upstream Kafka cluster,
which is the sole auth authority. kroxy stores no secrets.

Each tenant must therefore be a real principal in the upstream broker
(declared in its JAAS file or auth backend). Unknown usernames are rejected
at the proxy before any upstream dial.

## Admin RPC

kroxy exposes an unauthenticated JSON-RPC 2.0 endpoint for managing tenants
at runtime. It is **disabled by default** and binds to `127.0.0.1:9095`
when enabled. **Do not expose it on a public interface without putting auth
in front of it** — there is no authentication in v1.

Enable it in `kroxy.yaml`:

```yaml
admin:
  enabled: true
  listen: "127.0.0.1:9095"   # default
```

Methods (service name `Tenants`):

| Method           | Params                                              | Result                       |
| ---------------- | --------------------------------------------------- | ---------------------------- |
| `Tenants.Set`    | `username`, `tenant_id`, `topic_prefix`, `upstream` | `{ "ok": true }`             |
| `Tenants.Delete` | `username`                                          | `{ "ok": true }`             |
| `Tenants.List`   | none                                                | `{ "tenants": [...] }`       |

Error codes:

| Code     | Meaning                                |
| -------- | -------------------------------------- |
| `-32011` | user already exists                    |
| `-32012` | user not found                         |
| `-32013` | invalid request payload / missing field |

### curl

```bash
curl -sS -X POST http://127.0.0.1:9095/rpc \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"Tenants.Set","params":{
        "username":"alice",
        "tenant_id":"tenantA","topic_prefix":"tenantA.",
        "upstream":"kafka:9093"}}'
```

A small helper script lives in `examples/admin-curl.sh`:

```bash
ADMIN=http://localhost:19095/rpc ./examples/admin-curl.sh list
ADMIN=http://localhost:19095/rpc ./examples/admin-curl.sh \
  set alice tenantA "tenantA." kafka:9093
ADMIN=http://localhost:19095/rpc ./examples/admin-curl.sh delete alice
```
