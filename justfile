set shell := ["bash", "-cu"]

config := env_var_or_default("EVENTFLOW_CONFIG", "examples/school/eventflow.yaml")
lineage_file := env_var_or_default("EVENTFLOW_LINEAGE_FILE", "var/eventflow/lineage/openlineage.ndjson")
marquez_url := env_var_or_default("EVENTFLOW_MARQUEZ_URL", "http://localhost:5000")
redpanda_topic := env_var_or_default("EVENTFLOW_REDPANDA_TOPIC", "school.events.v1")

default:
    @just --list

# Run unit tests.
test:
    GOCACHE=${GOCACHE:-/tmp/eventflow-go-build-cache} go test ./...

# Run race-enabled tests.
race:
    GOCACHE=${GOCACHE:-/tmp/eventflow-go-build-cache} go test -race ./...

# Run go vet.
vet:
    GOCACHE=${GOCACHE:-/tmp/eventflow-go-build-cache} go vet ./...

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

# Remove local runtime outputs produced by demo/test recipes.
clean-runtime:
    rm -rf var/eventflow examples/school/var

# Wait until Redpanda accepts broker commands.
wait-redpanda:
    docker compose exec redpanda rpk cluster health -X brokers=localhost:9092 | grep -E 'Healthy:.+true'

# Wait until Marquez accepts HTTP requests.
wait-marquez:
    curl -fsS {{marquez_url}}/api/v1/namespaces >/dev/null

# Create one example topic in Redpanda.
topic topic=redpanda_topic:
    just wait-redpanda
    docker compose exec redpanda rpk topic create "{{topic}}" -X brokers=localhost:9092 --partitions 3 --replicas 1 || true

# Validate a resource config.
validate config=config:
    go run ./cmd/eventflow validate --config {{config}}

# Inspect a resource config.
inspect config=config:
    go run ./cmd/eventflow inspect --config {{config}}

# Run a resource config.
run config=config:
    go run ./cmd/eventflow run --config {{config}}

# Replay OpenLineage NDJSON into local Marquez.
lineage-marquez file=lineage_file:
    EVENTFLOW_LINEAGE_OUTPUT=marquez EVENTFLOW_MARQUEZ_URL={{marquez_url}} go run ./cmd/eventflow-lineage-replay --file {{file}}

# Demo filesystem emit/receive with structured CloudEvents JSON.
demo-filesystem:
    mkdir -p var/eventflow/demo
    printf '{"specversion":"1.0","id":"demo-filesystem-1","source":"urn:eventflow:demo","type":"demo.filesystem.v1","datacontenttype":"application/json","data":{"ok":true}}\n' | GOCACHE=${GOCACHE:-/tmp/eventflow-go-build-cache} go run ./cmd/eventflow-emit --adapter filesystem --path var/eventflow/demo/events.ndjson
    GOCACHE=${GOCACHE:-/tmp/eventflow-go-build-cache} go run ./cmd/eventflow-receive --adapter filesystem --path var/eventflow/demo/events.ndjson --max-events 1

# Demo HTTP emission command wiring.
demo-http:
    @printf 'Start an embedding app using httpflow.NewReceiver, then run:\n'
    @printf 'go run ./cmd/eventflow-emit --adapter http --url http://localhost:8080/events < event.json\n'

# Demo resource validation and graph inspection.
quickstart:
    just validate {{config}}
    just inspect {{config}}
