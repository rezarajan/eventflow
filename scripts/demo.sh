#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export GOCACHE="${GOCACHE:-/tmp/eventflow-go-build-cache}"

CONFIG="examples/openlineage-redpanda-marquez/ingress.yaml"
VALID="examples/openlineage-redpanda-marquez/valid-run-event.json"
UNAUTHORIZED="examples/openlineage-redpanda-marquez/unauthorized-job-namespace.json"
MALFORMED="examples/openlineage-redpanda-marquez/invalid-run-event.json"
PRINCIPAL="${EVENTFLOW_DEMO_PRINCIPAL:-spiffe://example/dagster}"
TOPIC="${EVENTFLOW_OPENLINEAGE_TOPIC:-openlineage.events.v1}"
LOG="var/eventflow/demo-eventflow.log"

pass() { printf 'PASS %s\n' "$1"; }
fail() { printf 'FAIL %s\n' "$1"; exit 1; }

rm -rf var/eventflow
mkdir -p var/eventflow

docker compose up -d redpanda >/dev/null
for _ in {1..60}; do
  if docker compose exec redpanda rpk cluster health -X brokers=localhost:9092 | grep -q 'Healthy:.*true'; then
    break
  fi
  sleep 1
done
docker compose exec redpanda rpk topic create "$TOPIC" -X brokers=localhost:9092 --partitions 1 --replicas 1 >/dev/null 2>&1 || true
pass "redpanda-ready"

go run ./cmd/eventflow run --config "$CONFIG" >"$LOG" 2>&1 &
pid=$!
cleanup() {
  kill "$pid" >/dev/null 2>&1 || true
  wait "$pid" >/dev/null 2>&1 || true
  docker compose down --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT
for _ in {1..60}; do
  if curl -fsS http://localhost:18081/readyz >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl -fsS http://localhost:18081/readyz >/dev/null || fail "eventflow-ready"
pass "eventflow-ready"

curl -fsS -X POST http://localhost:18081/events \
  -H 'content-type: application/json' \
  -H "X-Eventflow-Principal: $PRINCIPAL" \
  --data @"$VALID" >/dev/null
pass "valid-openlineage-accepted"

if go run ./cmd/eventflow policy test --config "$CONFIG" --event "$UNAUTHORIZED" --principal "$PRINCIPAL" --expect-outcome REJECT --expect-reason EF1202_JOB_NAMESPACE_NOT_ALLOWED >/dev/null; then
  pass "unauthorized-namespace-policy-test"
else
  fail "unauthorized-namespace-policy-test"
fi

if go run ./cmd/eventflow policy test --config "$CONFIG" --event "$MALFORMED" --principal "$PRINCIPAL" --expect-outcome REJECT --expect-reason EF1101_OPENLINEAGE_SCHEMA_INVALID >/dev/null; then
  pass "malformed-openlineage-policy-test"
else
  fail "malformed-openlineage-policy-test"
fi

curl -sS -o /tmp/eventflow-demo-unauthorized.out -w '%{http_code}' -X POST http://localhost:18081/events \
  -H 'content-type: application/json' \
  -H "X-Eventflow-Principal: $PRINCIPAL" \
  --data @"$UNAUTHORIZED" | grep -q '^400$' || fail "unauthorized-rejected"
pass "unauthorized-rejected"

curl -sS -o /tmp/eventflow-demo-malformed.out -w '%{http_code}' -X POST http://localhost:18081/events \
  -H 'content-type: application/json' \
  -H "X-Eventflow-Principal: $PRINCIPAL" \
  --data @"$MALFORMED" | grep -q '^400$' || fail "malformed-rejected"
pass "malformed-rejected"

records="$(go run ./cmd/eventflow quarantine list --config "$CONFIG")"
printf '%s\n' "$records" | grep -q 'EF1202_JOB_NAMESPACE_NOT_ALLOWED' || fail "quarantine-list-unauthorized"
printf '%s\n' "$records" | grep -q 'EF1101_OPENLINEAGE_SCHEMA_INVALID' || fail "quarantine-list-malformed"
pass "quarantine-list"

record_id="$(printf '%s\n' "$records" | sed -n 's/.*"id":\([0-9][0-9]*\).*/\1/p' | head -n1)"
test -n "$record_id" || fail "quarantine-record-id"
go run ./cmd/eventflow quarantine validate --config "$CONFIG" "$record_id" >/dev/null
pass "quarantine-validate"

go run ./cmd/eventflow quarantine replay --config "$CONFIG" --principal "$PRINCIPAL" --event "$VALID" "$record_id" >/dev/null
pass "quarantine-replay-corrected"

printf 'Demo complete: accepted, rejected, quarantined, validated, replayed.\n'
