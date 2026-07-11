# Contributing

Keep Eventflow split into a public SDK and thin runtime commands.

- Put reusable behavior in public packages when callers need to embed it.
- Keep commands focused on config, startup validation, signal handling, and streaming.
- Use `EVENTFLOW_*` variables for configuration.
- Default validation to explicit `eventflow.dev/v1alpha1` resources.
- Do not add control-plane, provisioning, credential issuance, identity, governance, or ownership workflows.
- Add adapter conformance coverage with `adaptertest` for new adapters.

Verification:

```bash
GOCACHE=/tmp/eventflow-go-build-cache go test ./...
GOCACHE=/tmp/eventflow-go-build-cache go test -race ./...
GOCACHE=/tmp/eventflow-go-build-cache go vet ./...
```

Run `staticcheck ./...` and `govulncheck ./...` when those tools are installed.
