#!/usr/bin/env bash
set -euo pipefail

topic="${DATASCAPE_REDPANDA_TOPIC:-datascape.events.v1}"
brokers="${DATASCAPE_REDPANDA_BROKERS:-localhost:19092}"
generator="${DATASCAPE_GENERATOR:-demo.school.v1}"
handlers="${DATASCAPE_CONSUME_HANDLERS:-jsonl,objects}"
max_events="${DATASCAPE_CONSUME_MAX_EVENTS:-102}"
jsonl_dir="${DATASCAPE_JSONL_DIR:-var/datascape/materialized}"
object_dir="${DATASCAPE_OBJECT_DIR:-var/datascape/objects}"
lineage_file="${DATASCAPE_LINEAGE_FILE:-var/datascape/lineage/openlineage.ndjson}"
lineage_namespace="${DATASCAPE_LINEAGE_NAMESPACE:-datascape}"
group_id="${DATASCAPE_REDPANDA_CONSUMER_GROUP:-datascape-lineage-demo}"

rm -rf "${jsonl_dir}" "${object_dir}" "${lineage_file}"

docker compose up -d redpanda redpanda-console
docker compose exec redpanda rpk cluster health -X brokers=localhost:9092 | grep -E 'Healthy:.+true' || exit 1
docker compose exec redpanda rpk topic describe "${topic}" -X brokers=localhost:9092 >/dev/null 2>&1 || docker compose exec redpanda rpk topic create "${topic}" -X brokers=localhost:9092 --partitions 3 --replicas 1

DATASCAPE_LINEAGE_OUTPUT=file \
DATASCAPE_LINEAGE_FILE="${lineage_file}" \
DATASCAPE_LINEAGE_NAMESPACE="${lineage_namespace}" \
DATASCAPE_REDPANDA_BROKERS="${brokers}" \
DATASCAPE_REDPANDA_TOPIC="${topic}" \
go run ./cmd/datascape-generate --generator "${generator}" \
  | DATASCAPE_LINEAGE_OUTPUT=file \
    DATASCAPE_LINEAGE_FILE="${lineage_file}" \
    DATASCAPE_LINEAGE_NAMESPACE="${lineage_namespace}" \
    DATASCAPE_REDPANDA_BROKERS="${brokers}" \
    DATASCAPE_REDPANDA_TOPIC="${topic}" \
    DATASCAPE_OUTPUTS=redpanda,log \
    go run ./cmd/datascape-fanout --outputs redpanda,log

DATASCAPE_LINEAGE_OUTPUT=file \
DATASCAPE_LINEAGE_FILE="${lineage_file}" \
DATASCAPE_LINEAGE_NAMESPACE="${lineage_namespace}" \
DATASCAPE_REDPANDA_BROKERS="${brokers}" \
DATASCAPE_REDPANDA_TOPIC="${topic}" \
DATASCAPE_REDPANDA_CONSUMER_GROUP="${group_id}" \
DATASCAPE_REDPANDA_CONSUMER_START_OFFSET=first \
DATASCAPE_CONSUME_HANDLERS="${handlers}" \
DATASCAPE_CONSUME_MAX_EVENTS="${max_events}" \
DATASCAPE_JSONL_DIR="${jsonl_dir}" \
DATASCAPE_OBJECT_DIR="${object_dir}" \
go run ./cmd/datascape-consume --source redpanda --handlers "${handlers}" --max-events "${max_events}"

printf 'Materialized JSONL tables in %s\n' "${jsonl_dir}"
printf 'Materialized text object artifacts in %s\n' "${object_dir}"
printf 'OpenLineage NDJSON written to %s\n' "${lineage_file}"
