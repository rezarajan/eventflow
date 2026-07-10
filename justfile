set shell := ["bash", "-cu"]

registry := env_var_or_default("EVENTFLOW_REGISTRY", "examples/school/eventflow.yaml")
lineage_file := env_var_or_default("EVENTFLOW_LINEAGE_FILE", "var/eventflow/lineage/openlineage.ndjson")
duckdb_path := env_var_or_default("EVENTFLOW_DUCKDB_PATH", "var/eventflow/eventflow.duckdb")
marquez_url := env_var_or_default("EVENTFLOW_MARQUEZ_URL", "http://localhost:5000")
ingress_url := env_var_or_default("EVENTFLOW_INGRESS_URL", "http://localhost:8080")
redpanda_topic := env_var_or_default("EVENTFLOW_REDPANDA_TOPIC", "attendance.events.v1")
consumer_group := env_var_or_default("EVENTFLOW_REDPANDA_CONSUMER_GROUP", "eventflow-consume")
school_topics := "school.events.v1 student.events.v1 attendance.events.v1 assessment.events.v1 document.events.v1 audit.events.v1"

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

# Remove local runtime outputs produced by demo/test recipes.
clean-runtime:
    rm -rf var/eventflow

# Wait until Redpanda accepts broker commands.
wait-redpanda:
    docker compose exec redpanda rpk cluster health -X brokers=localhost:9092 | grep -E 'Healthy:.+true'

# Wait until Marquez accepts HTTP requests.
wait-marquez:
    curl -fsS {{marquez_url}}/api/v1/namespaces >/dev/null

# Create the example registry topics in Redpanda.
topics:
    just wait-redpanda
    for topic in {{school_topics}}; do docker compose exec redpanda rpk topic create "$topic" -X brokers=localhost:9092 --partitions 3 --replicas 1 || true; done

# Write AsyncAPI for the configured registry.
asyncapi registry=registry output="-":
    go run ./cmd/eventflow-registry asyncapi --registry {{registry}} --output {{output}}

# Start HTTP ingress in the foreground with file lineage enabled.
ingress registry=registry:
    EVENTFLOW_REGISTRY={{registry}} EVENTFLOW_REDPANDA_TOPIC_MODE=registry EVENTFLOW_LINEAGE_OUTPUT=file EVENTFLOW_LINEAGE_FILE={{lineage_file}} go run ./cmd/eventflow-ingress-http

# Publish one valid attendance event through HTTP ingress using the current timestamp.
publish-event url=ingress_url:
    now="$(date -u +%Y-%m-%dT%H:%M:%SZ)"; today="$(date -u +%Y-%m-%d)"; curl -fsS -X POST "{{url}}/v1/events/attendance.submitted.v1" -H 'content-type: application/json' -H 'X-Eventflow-Subject: student-22222222' -H "X-Correlation-ID: just-${now}" -d "{\"attendance_id\":\"11111111-1111-1111-1111-111111111111\",\"student_id\":\"22222222-2222-2222-2222-222222222222\",\"class_id\":\"33333333-3333-3333-3333-333333333333\",\"school_id\":\"44444444-4444-4444-4444-444444444444\",\"attendance_date\":\"${today}\",\"status_code\":\"PRESENT\",\"submitted_at\":\"${now}\"}"

# Consume one event from Redpanda into DuckDB with file lineage enabled.
consume-event registry=registry topic=redpanda_topic:
    EVENTFLOW_REGISTRY={{registry}} EVENTFLOW_REDPANDA_TOPIC={{topic}} EVENTFLOW_REDPANDA_CONSUMER_GROUP={{consumer_group}} EVENTFLOW_CONSUME_HANDLERS=duckdb EVENTFLOW_CONSUME_MAX_EVENTS=1 EVENTFLOW_DUCKDB_PATH={{duckdb_path}} EVENTFLOW_LINEAGE_OUTPUT=file EVENTFLOW_LINEAGE_FILE={{lineage_file}} go run ./cmd/eventflow-consume

# Replay OpenLineage NDJSON into local Marquez.
lineage-marquez file=lineage_file:
    EVENTFLOW_LINEAGE_OUTPUT=marquez EVENTFLOW_MARQUEZ_URL={{marquez_url}} go run ./cmd/eventflow-lineage-replay --file {{file}}

# Run the short test flow after ingress is already running in another terminal.
smoke-flow:
    just up-all
    just topics
    just wait-marquez
    just publish-event
    just consume-event
    just lineage-marquez

# Validate the school example registry and show the quickstart command path.
quickstart:
    go run ./cmd/eventflow-registry validate --registry {{registry}}
    @printf 'Start services with: just up-all\n'
    @printf 'Create topics with: just topics\n'
    @printf 'Start ingress with: just ingress\n'
    @printf 'Publish an event with: just publish-event\n'
    @printf 'Consume one event with: just consume-event\n'
    @printf 'Replay lineage to Marquez with: just lineage-marquez\n'

# Demo filesystem emit/receive with structured CloudEvents JSON.
demo-filesystem:
    mkdir -p var/eventflow/demo
    printf '{"specversion":"1.0","id":"demo-filesystem-1","source":"urn:eventflow:demo","type":"demo.filesystem.v1","datacontenttype":"application/json","data":{"ok":true}}\n' | GOCACHE=${GOCACHE:-/tmp/eventflow-go-build-cache} go run ./cmd/eventflow-emit --adapter filesystem --path var/eventflow/demo/events.ndjson
    GOCACHE=${GOCACHE:-/tmp/eventflow-go-build-cache} go run ./cmd/eventflow-receive --adapter filesystem --path var/eventflow/demo/events.ndjson --max-events 1

# Demo HTTP command wiring by printing the command path.
demo-http:
    @printf 'Start a receiver with an embedding app using httpflow.NewReceiver, then run:\n'
    @printf 'go run ./cmd/eventflow-emit --adapter http --url http://localhost:8080/events < event.json\n'

# Demo Redpanda using local compose services and existing runtime commands.
demo-redpanda:
    just up
    just topics
    @printf 'Run ingress with: just ingress\n'
    @printf 'Publish with: just publish-event\n'

# Demo S3-compatible storage integration recipe.
demo-s3:
    @printf 'Use package github.com/rezarajan/project-datascape/s3 with an injected S3-compatible client; no buckets or credentials are provisioned by Eventflow.\n'

# Demo DuckDB projection using local consume command.
demo-duckdb:
    EVENTFLOW_REGISTRY={{registry}} EVENTFLOW_DUCKDB_PATH={{duckdb_path}} GOCACHE=${GOCACHE:-/tmp/eventflow-go-build-cache} go test ./internal/adapters/consume/duckdb

# Run all local demos that do not require external credentials.
demo-all:
    just demo-filesystem
    just demo-http
    just demo-redpanda
    just demo-s3
    just demo-duckdb
