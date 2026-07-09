#!/usr/bin/env bash
set -euo pipefail

marquez_url="${DATASCAPE_MARQUEZ_URL:-http://localhost:5000}"
lineage_file="${DATASCAPE_LINEAGE_FILE:-var/datascape/lineage/openlineage.ndjson}"

docker compose up -d marquez-db marquez marquez-web
docker compose exec marquez bash -c ': >/dev/tcp/127.0.0.1/5000'

bash scripts/run-lineage-demo.sh

DATASCAPE_LINEAGE_OUTPUT=marquez \
DATASCAPE_LINEAGE_FILE="${lineage_file}" \
DATASCAPE_MARQUEZ_URL="${marquez_url}" \
go run ./cmd/datascape-lineage-replay --file "${lineage_file}"

printf 'Replayed OpenLineage events from %s to %s\n' "${lineage_file}" "${marquez_url}"
printf 'Open Marquez UI at http://localhost:3000\n'
