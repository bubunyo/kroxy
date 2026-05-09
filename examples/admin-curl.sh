#!/usr/bin/env bash
# Examples for talking to the kroxy admin RPC.
#
# The admin RPC is JSON-RPC 2.0 over HTTP. By default kroxy binds it to
# 127.0.0.1:9095. The compose stack publishes it on host port 19095.
#
# Override the endpoint with $ADMIN to point at a different host/port:
#   ADMIN=http://localhost:19095/rpc ./examples/admin-curl.sh list
#
# kroxy is a SASL/PLAIN pass-through: the username selects the tenant and
# the password is forwarded verbatim to the upstream Kafka cluster, which
# is the auth authority. kroxy itself stores no secrets.

set -euo pipefail

ADMIN="${ADMIN:-http://127.0.0.1:9095/rpc}"

usage() {
  cat <<EOF
Usage: $0 <command> [args]

Commands:
  list
  set    <username> <tenant_id> <topic_prefix> <upstream>
  delete <username>
EOF
  exit 2
}

call() {
  local method="$1" params="$2"
  curl -sS -X POST "$ADMIN" \
    -H 'Content-Type: application/json' \
    --data "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"$method\",\"params\":$params}" \
    | jq .
}

cmd="${1:-}"; shift || true
case "$cmd" in
  list)
    call "Tenants.List" '{}'
    ;;
  set)
    [ "$#" -eq 4 ] || usage
    user="$1" tid="$2" prefix="$3" upstream="$4"
    call "Tenants.Set" \
      "{\"username\":\"$user\",\"tenant_id\":\"$tid\",\"topic_prefix\":\"$prefix\",\"upstream\":\"$upstream\"}"
    ;;
  delete)
    [ "$#" -eq 1 ] || usage
    user="$1"
    call "Tenants.Delete" "{\"username\":\"$user\"}"
    ;;
  *)
    usage
    ;;
esac
