set shell := ["bash", "-cu"]

default:
    @just --list

# Run unit tests.
test:
    GOCACHE=${GOCACHE:-/tmp/eventflow-go-build-cache} go test ./...

# Refresh module metadata.
tidy:
    go mod tidy

# Start local Redpanda infrastructure.
up:
    docker compose up -d redpanda redpanda-console

# Start local Marquez infrastructure.
up-marquez:
    docker compose up -d marquez marquez-web

# Start all local infrastructure.
up-all:
    docker compose up -d redpanda redpanda-console marquez marquez-web

# Stop local infrastructure.
down:
    docker compose down

# Write AsyncAPI for the configured registry.
asyncapi registry="examples/school/eventflow.yaml" output="-":
    go run ./cmd/eventflow-registry asyncapi --registry {{registry}} --output {{output}}

# Replay OpenLineage NDJSON into local Marquez.
lineage-marquez file="var/eventflow/lineage/openlineage.ndjson":
    EVENTFLOW_LINEAGE_OUTPUT=marquez EVENTFLOW_MARQUEZ_URL=${EVENTFLOW_MARQUEZ_URL:-http://localhost:5000} go run ./cmd/eventflow-lineage-replay --file {{file}}

# Validate the school example registry and show the quickstart command path.
quickstart:
    go run ./cmd/eventflow-registry validate --registry examples/school/eventflow.yaml
    @printf 'Start services with: just up-all\n'
    @printf 'Start ingress with: EVENTFLOW_REGISTRY=examples/school/eventflow.yaml EVENTFLOW_REDPANDA_TOPIC_MODE=registry EVENTFLOW_LINEAGE_OUTPUT=file go run ./cmd/eventflow-ingress-http\n'
    @printf 'Replay lineage to Marquez with: just lineage-marquez\n'
