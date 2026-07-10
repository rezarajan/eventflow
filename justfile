set shell := ["bash", "-cu"]

default:
    @just --list

# Run unit tests. Tests use in-memory fakes only and do not require Redpanda.
test:
    go test ./...

# Download modules declared in go.mod and refresh go.sum.
deps:
    go mod download
    go mod tidy

# Start Redpanda and Redpanda Console.
up:
    docker compose up -d redpanda redpanda-console


# Create the default Redpanda topic after Redpanda is healthy.
create-redpanda-topic:
    docker compose exec redpanda rpk cluster health -X brokers=localhost:9092 | grep -E 'Healthy:.+true' || exit 1
    docker compose exec redpanda rpk topic create ${DATASCAPE_REDPANDA_TOPIC:-datascape.events.v1} -X brokers=localhost:9092 --partitions 3 --replicas 1

# Create canonical catalog topics used by HTTP ingress.
create-catalog-topics:
    docker compose exec redpanda rpk cluster health -X brokers=localhost:9092 | grep -E 'Healthy:.+true' || exit 1
    for topic in school.events.v1 student.events.v1 attendance.events.v1 assessment.events.v1 document.events.v1 audit.events.v1; do docker compose exec redpanda rpk topic create "$topic" -X brokers=localhost:9092 --partitions 3 --replicas 1 || true; done

# Stop local infrastructure.
down:
    docker compose down

# Generate CloudEvents JSONL only.
generate:
    go run ./cmd/datascape-generate --generator ${DATASCAPE_GENERATOR:-demo.school.v1}

# Fan out CloudEvents JSONL from stdin to structured logs.
fanout-log:
    go run ./cmd/datascape-fanout --outputs log

# Fan out CloudEvents JSONL from stdin to Redpanda.
fanout-redpanda:
    DATASCAPE_OUTPUTS=redpanda go run ./cmd/datascape-fanout --outputs redpanda

# Serve the producer-friendly HTTP ingress API.
ingress-http:
    DATASCAPE_REDPANDA_TOPIC_MODE=catalog go run ./cmd/datascape-ingress-http

# Run the local log-only demo pipeline.
run-demo:
    go run ./cmd/datascape-generate --generator ${DATASCAPE_GENERATOR:-demo.school.v1} | go run ./cmd/datascape-fanout --outputs log

# Run the Redpanda demo pipeline.
run-redpanda-demo:
    go run ./cmd/datascape-generate --generator ${DATASCAPE_GENERATOR:-demo.school.v1} | DATASCAPE_OUTPUTS=redpanda go run ./cmd/datascape-fanout --outputs redpanda

# Run the full local materialization demo.
run-materialize-demo:
    bash scripts/run-materialize-demo.sh

# Run the full local lineage and materialization demo.
run-lineage-demo:
    bash scripts/run-lineage-demo.sh

# Consume a bounded Redpanda stream into JSONL, object files, and DuckDB.
run-ingress-duckdb-demo:
    DATASCAPE_CONSUME_HANDLERS=jsonl,objects,duckdb DATASCAPE_CONSUME_MAX_EVENTS=${DATASCAPE_CONSUME_MAX_EVENTS:-10} go run ./cmd/datascape-consume

# Run the local lineage demo and replay OpenLineage events to Marquez.
run-marquez-demo:
    bash scripts/run-marquez-demo.sh

# Consume the default Redpanda topic from the beginning.
consume-redpanda:
    docker compose exec redpanda rpk topic consume ${DATASCAPE_REDPANDA_TOPIC:-datascape.events.v1} -X brokers=localhost:9092 --offset start
