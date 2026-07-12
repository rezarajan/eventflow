package admission

import (
	"testing"
	"time"

	eventflow "github.com/rezarajan/eventflow"
)

func TestEvaluateReasonCodes(t *testing.T) {
	contract := Contract{Type: CloudEventType, AllowedSchemaVersions: []string{"2-0-2"}, MaxEventBytes: 10000}
	policy := Policy{
		Name:                       "policy",
		Version:                    "v1",
		AllowedSchemaVersions:      []string{"2-0-2"},
		AllowedProducers:           []string{"producer"},
		PrincipalJobNamespaces:     map[string][]string{"principal": {"jobs"}},
		PrincipalDatasetNamespaces: map[string][]string{"principal": {"warehouse"}},
		AllowedDatasetURISchemes:   []string{"s3"},
		AllowCustomFacets:          true,
	}
	tests := []struct {
		name   string
		body   string
		mutate func(*eventflow.Event, *Contract, *Policy)
		want   string
	}{
		{
			name: "accept",
			body: validBody(),
			want: "",
		},
		{
			name: "unsupported type",
			body: validBody(),
			mutate: func(event *eventflow.Event, _ *Contract, _ *Policy) {
				event.SetType("other")
			},
			want: eventflow.ReasonEventTypeUnsupported,
		},
		{
			name: "schema invalid",
			body: `{"eventType":"COMPLETE","run":{},"job":{"namespace":"jobs"},"producer":"producer","schemaURL":"https://openlineage.io/spec/2-0-2/OpenLineage.json"}`,
			want: eventflow.ReasonOpenLineageSchemaInvalid,
		},
		{
			name: "version unsupported",
			body: replace(validBody(), "2-0-2", "9-9-9"),
			want: eventflow.ReasonOpenLineageVersionUnsupported,
		},
		{
			name: "producer not allowed",
			body: replace(validBody(), `"producer":"producer"`, `"producer":"other"`),
			want: eventflow.ReasonProducerNotAllowed,
		},
		{
			name: "job namespace not allowed",
			body: replace(validBody(), `"namespace":"jobs","name":"job"`, `"namespace":"other","name":"job"`),
			want: eventflow.ReasonJobNamespaceNotAllowed,
		},
		{
			name: "dataset namespace not allowed",
			body: replace(validBody(), `"namespace":"warehouse","name":"s3://warehouse/in"`, `"namespace":"other","name":"s3://warehouse/in"`),
			want: eventflow.ReasonDatasetNamespaceNotAllowed,
		},
		{
			name: "required facet missing",
			body: validBody(),
			mutate: func(_ *eventflow.Event, _ *Contract, policy *Policy) {
				policy.RequiredFacets = []string{"run.nominalTime"}
			},
			want: eventflow.ReasonRequiredFacetMissing,
		},
		{
			name: "facet prohibited",
			body: replace(validBody(), `"runId":"run-1"`, `"runId":"run-1","facets":{"debug":{}}`),
			mutate: func(_ *eventflow.Event, _ *Contract, policy *Policy) {
				policy.ProhibitedFacets = []string{"run.debug"}
			},
			want: eventflow.ReasonFacetNotAllowed,
		},
		{
			name: "event too large",
			body: validBody(),
			mutate: func(_ *eventflow.Event, _ *Contract, contract *Policy) {
				contract.MaxEventBytes = 10
			},
			want: eventflow.ReasonEventTooLarge,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := testEvent(t, []byte(tt.body))
			c := contract
			p := policy
			if tt.mutate != nil {
				tt.mutate(&event, &c, &p)
			}
			decision, _ := Evaluate(event, []byte(tt.body), "principal", c, p)
			if tt.want == "" && decision.Outcome != eventflow.OutcomeAccept {
				t.Fatalf("decision = %+v, want accept", decision)
			}
			if tt.want != "" && decision.ReasonCode != tt.want {
				t.Fatalf("reason = %s, want %s (%+v)", decision.ReasonCode, tt.want, decision)
			}
		})
	}
}

func TestRateLimiterReasonCode(t *testing.T) {
	limiter := NewRateLimiter()
	policy := Policy{Name: "policy", Version: "v1"}
	now := time.Date(2026, 7, 12, 0, 0, 0, 0, time.UTC)
	if _, limited := limiter.Check("principal", 1, now, policy); limited {
		t.Fatal("first event should be accepted")
	}
	decision, limited := limiter.Check("principal", 1, now, policy)
	if !limited || decision.ReasonCode != eventflow.ReasonRateLimitExceeded {
		t.Fatalf("decision = %+v, limited=%v", decision, limited)
	}
}

func testEvent(t *testing.T, raw []byte) eventflow.Event {
	t.Helper()
	event := eventflow.Event{}
	event.SetSpecVersion("1.0")
	event.SetID("event-1")
	event.SetSource("urn:test")
	event.SetType(CloudEventType)
	event.SetDataContentType("application/json")
	if err := event.SetData("application/json", raw); err != nil {
		t.Fatal(err)
	}
	return event
}

func validBody() string {
	return `{"eventType":"COMPLETE","eventTime":"2026-07-12T00:00:00Z","run":{"runId":"run-1"},"job":{"namespace":"jobs","name":"job"},"inputs":[{"namespace":"warehouse","name":"s3://warehouse/in"}],"producer":"producer","schemaURL":"https://openlineage.io/spec/2-0-2/OpenLineage.json"}`
}

func replace(s string, old string, new string) string {
	return stringsReplace(s, old, new)
}

func stringsReplace(s string, old string, new string) string {
	for i := 0; i+len(old) <= len(s); i++ {
		if s[i:i+len(old)] == old {
			return s[:i] + new + s[i+len(old):]
		}
	}
	return s
}
