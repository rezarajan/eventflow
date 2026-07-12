set shell := ["bash", "-cu"]

config := env_var_or_default("EVENTFLOW_CONFIG", "examples/openlineage-redpanda-marquez/ingress.yaml")
topic := env_var_or_default("EVENTFLOW_OPENLINEAGE_TOPIC", "openlineage.events.v1")
principal := env_var_or_default("EVENTFLOW_DEMO_PRINCIPAL", "spiffe://example/dagster")

default:
    @just --list

test:
    GOCACHE=${GOCACHE:-/tmp/eventflow-go-build-cache} go test ./...

race:
    GOCACHE=${GOCACHE:-/tmp/eventflow-go-build-cache} go test -race ./...

vet:
    GOCACHE=${GOCACHE:-/tmp/eventflow-go-build-cache} go vet ./...

tidy:
    go mod tidy

validate config=config:
    GOCACHE=${GOCACHE:-/tmp/eventflow-go-build-cache} go run ./cmd/eventflow validate --config {{config}}

inspect config=config:
    GOCACHE=${GOCACHE:-/tmp/eventflow-go-build-cache} go run ./cmd/eventflow inspect --config {{config}}

status config=config:
    GOCACHE=${GOCACHE:-/tmp/eventflow-go-build-cache} go run ./cmd/eventflow status --config {{config}}

up:
    docker compose up -d redpanda redpanda-console

down:
    docker compose down --remove-orphans

topic topic=topic:
    docker compose exec redpanda rpk topic create "{{topic}}" -X brokers=localhost:9092 --partitions 1 --replicas 1 || true

demo:
    ./scripts/demo.sh
