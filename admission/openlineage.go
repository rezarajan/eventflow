// Package admission implements OpenLineage admission and quarantine decisions for shared data-platform infrastructure.
package admission

import (
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	eventflow "github.com/rezarajan/eventflow"
)

const CloudEventType = "io.openlineage.run-event.v1"

// OpenLineageEvent is the supported OpenLineage RunEvent subset.
type OpenLineageEvent struct {
	EventType string          `json:"eventType"`
	EventTime string          `json:"eventTime"`
	Run       Run             `json:"run"`
	Job       Job             `json:"job"`
	Inputs    []Dataset       `json:"inputs,omitempty"`
	Outputs   []Dataset       `json:"outputs,omitempty"`
	Producer  string          `json:"producer"`
	SchemaURL string          `json:"schemaURL"`
	Facets    json.RawMessage `json:"facets,omitempty"`
}

type Run struct {
	RunID  string                     `json:"runId"`
	Facets map[string]json.RawMessage `json:"facets,omitempty"`
}

type Job struct {
	Namespace string                     `json:"namespace"`
	Name      string                     `json:"name"`
	Facets    map[string]json.RawMessage `json:"facets,omitempty"`
}

type Dataset struct {
	Namespace string                     `json:"namespace"`
	Name      string                     `json:"name"`
	Facets    map[string]json.RawMessage `json:"facets,omitempty"`
}

// Contract defines the CloudEvents and OpenLineage contract.
type Contract struct {
	Name                  string
	Version               string
	Type                  string
	AllowedSchemaVersions []string
	RequiredExtensions    []string
	MaxEventBytes         int64
}

// Policy defines deterministic typed OpenLineage policy.
type Policy struct {
	Name                          string
	Version                       string
	AllowedSchemaVersions         []string
	AllowedProducers              []string
	PrincipalJobNamespaces        map[string][]string
	PrincipalDatasetNamespaces    map[string][]string
	RequiredFacets                []string
	ProhibitedFacets              []string
	AllowCustomFacets             bool
	AllowedCustomFacetPrefixes    []string
	AllowedDatasetURISchemes      []string
	MaxEventBytes                 int64
	MaxFacetBytes                 int64
	RequiredCloudEventsExtensions []string
	AllowedEnvironments           []string
	AllowedTenants                []string
	JobNamespacePattern           string
	JobNamePattern                string
	DatasetNamespacePattern       string
	DatasetNamePattern            string
	RateLimitPerMinute            int
}

// Evaluate validates one CloudEvent/OpenLineage payload against a contract and policy.
func Evaluate(event eventflow.Event, raw []byte, principal string, contract Contract, policy Policy) (eventflow.Decision, OpenLineageEvent) {
	policyName := policy.Name
	policyVersion := policy.Version
	if err := event.Validate(); err != nil {
		return eventflow.Rejected(eventflow.ReasonCloudEventInvalid, err.Error(), "cloudevents", principal, policyName, policyVersion), OpenLineageEvent{}
	}
	if contract.Type != "" && event.Type() != contract.Type {
		return eventflow.Rejected(eventflow.ReasonEventTypeUnsupported, fmt.Sprintf("event type %q is unsupported", event.Type()), "type", principal, policyName, policyVersion), OpenLineageEvent{}
	}
	if contract.MaxEventBytes > 0 && int64(len(raw)) > contract.MaxEventBytes {
		return eventflow.Rejected(eventflow.ReasonEventTooLarge, "event exceeds contract size limit", "data", principal, policyName, policyVersion), OpenLineageEvent{}
	}
	if policy.MaxEventBytes > 0 && int64(len(raw)) > policy.MaxEventBytes {
		return eventflow.Rejected(eventflow.ReasonEventTooLarge, "event exceeds policy size limit", "data", principal, policyName, policyVersion), OpenLineageEvent{}
	}
	payload := event.Data()
	if len(payload) == 0 && len(raw) > 0 {
		payload = raw
	}
	var ol OpenLineageEvent
	if err := json.Unmarshal(payload, &ol); err != nil {
		return eventflow.Rejected(eventflow.ReasonOpenLineageSchemaInvalid, err.Error(), "data", principal, policyName, policyVersion), OpenLineageEvent{}
	}
	if err := validateShape(ol); err != nil {
		return eventflow.Rejected(eventflow.ReasonOpenLineageSchemaInvalid, err.Error(), "data", principal, policyName, policyVersion), ol
	}
	if !allowedVersion(ol.SchemaURL, contract.AllowedSchemaVersions, policy.AllowedSchemaVersions) {
		return eventflow.Rejected(eventflow.ReasonOpenLineageVersionUnsupported, "OpenLineage schema version is not allowed", "schemaURL", principal, policyName, policyVersion), ol
	}
	if !containsOrEmpty(policy.AllowedProducers, ol.Producer) {
		return eventflow.Rejected(eventflow.ReasonProducerNotAllowed, "producer is not allowed", "producer", principal, policyName, policyVersion), ol
	}
	if !principalAllowed(policy.PrincipalJobNamespaces, principal, ol.Job.Namespace) {
		return eventflow.Rejected(eventflow.ReasonJobNamespaceNotAllowed, "principal is not allowed for job namespace", "job.namespace", principal, policyName, policyVersion), ol
	}
	for _, ds := range append(append([]Dataset{}, ol.Inputs...), ol.Outputs...) {
		if !principalAllowed(policy.PrincipalDatasetNamespaces, principal, ds.Namespace) {
			return eventflow.Rejected(eventflow.ReasonDatasetNamespaceNotAllowed, "principal is not allowed for dataset namespace", "datasets.namespace", principal, policyName, policyVersion), ol
		}
		if !datasetSchemeAllowed(policy.AllowedDatasetURISchemes, ds.Name) {
			return eventflow.Rejected(eventflow.ReasonFacetNotAllowed, "dataset URI scheme is not allowed", "datasets.name", principal, policyName, policyVersion), ol
		}
	}
	if decision, ok := checkPatterns(ol, principal, policy); ok {
		return decision, ol
	}
	if decision, ok := checkExtensions(event, principal, policy); ok {
		return decision, ol
	}
	if decision, ok := checkFacets(ol, principal, policy); ok {
		return decision, ol
	}
	return eventflow.Accepted(principal, policyName, policyVersion), ol
}

func validateShape(ol OpenLineageEvent) error {
	switch ol.EventType {
	case "START", "RUNNING", "COMPLETE", "FAIL", "ABORT", "OTHER":
	default:
		return fmt.Errorf("unsupported eventType %q", ol.EventType)
	}
	if strings.TrimSpace(ol.Run.RunID) == "" {
		return fmt.Errorf("run.runId is required")
	}
	if strings.TrimSpace(ol.Job.Namespace) == "" {
		return fmt.Errorf("job.namespace is required")
	}
	if strings.TrimSpace(ol.Job.Name) == "" {
		return fmt.Errorf("job.name is required")
	}
	if strings.TrimSpace(ol.Producer) == "" {
		return fmt.Errorf("producer is required")
	}
	return nil
}

func allowedVersion(schemaURL string, lists ...[]string) bool {
	var allowed []string
	for _, list := range lists {
		allowed = append(allowed, list...)
	}
	if len(allowed) == 0 {
		return true
	}
	for _, value := range allowed {
		if value != "" && strings.Contains(schemaURL, value) {
			return true
		}
	}
	return false
}

func containsOrEmpty(list []string, value string) bool {
	if len(list) == 0 {
		return true
	}
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}

func principalAllowed(rules map[string][]string, principal string, namespace string) bool {
	if len(rules) == 0 {
		return true
	}
	for _, allowed := range rules[principal] {
		if allowed == namespace || allowed == "*" {
			return true
		}
	}
	return false
}

func datasetSchemeAllowed(allowed []string, name string) bool {
	if len(allowed) == 0 {
		return true
	}
	parsed, err := url.Parse(name)
	if err != nil || parsed.Scheme == "" {
		return false
	}
	return containsOrEmpty(allowed, parsed.Scheme)
}

func checkPatterns(ol OpenLineageEvent, principal string, policy Policy) (eventflow.Decision, bool) {
	for field, pair := range map[string][2]string{
		"job.namespace":      {policy.JobNamespacePattern, ol.Job.Namespace},
		"job.name":           {policy.JobNamePattern, ol.Job.Name},
		"datasets.namespace": {policy.DatasetNamespacePattern, datasetNamespaces(ol)},
		"datasets.name":      {policy.DatasetNamePattern, datasetNames(ol)},
	} {
		pattern := pair[0]
		if pattern == "" {
			continue
		}
		ok, _ := regexp.MatchString(pattern, pair[1])
		if !ok {
			return eventflow.Rejected(eventflow.ReasonFacetNotAllowed, "name does not match required pattern", field, principal, policy.Name, policy.Version), true
		}
	}
	return eventflow.Decision{}, false
}

func checkExtensions(event eventflow.Event, principal string, policy Policy) (eventflow.Decision, bool) {
	for _, extension := range policy.RequiredCloudEventsExtensions {
		if _, ok := event.Extensions()[strings.ToLower(extension)]; !ok {
			return eventflow.Rejected(eventflow.ReasonCloudEventInvalid, "required CloudEvents extension is missing", "extensions."+extension, principal, policy.Name, policy.Version), true
		}
	}
	return eventflow.Decision{}, false
}

func checkFacets(ol OpenLineageEvent, principal string, policy Policy) (eventflow.Decision, bool) {
	facets := map[string]json.RawMessage{}
	for k, v := range ol.Run.Facets {
		facets["run."+k] = v
	}
	for k, v := range ol.Job.Facets {
		facets["job."+k] = v
	}
	for _, ds := range append(append([]Dataset{}, ol.Inputs...), ol.Outputs...) {
		for k, v := range ds.Facets {
			facets["dataset."+k] = v
		}
	}
	for _, required := range policy.RequiredFacets {
		if _, ok := facets[required]; !ok {
			return eventflow.Rejected(eventflow.ReasonRequiredFacetMissing, "required facet is missing", required, principal, policy.Name, policy.Version), true
		}
	}
	for _, prohibited := range policy.ProhibitedFacets {
		if _, ok := facets[prohibited]; ok {
			return eventflow.Rejected(eventflow.ReasonFacetNotAllowed, "facet is prohibited", prohibited, principal, policy.Name, policy.Version), true
		}
	}
	for name, raw := range facets {
		if policy.MaxFacetBytes > 0 && int64(len(raw)) > policy.MaxFacetBytes {
			return eventflow.Rejected(eventflow.ReasonEventTooLarge, "facet exceeds size limit", name, principal, policy.Name, policy.Version), true
		}
		if !policy.AllowCustomFacets && !allowedCustomFacet(policy.AllowedCustomFacetPrefixes, name) {
			return eventflow.Rejected(eventflow.ReasonFacetNotAllowed, "custom facet is not allowed", name, principal, policy.Name, policy.Version), true
		}
	}
	return eventflow.Decision{}, false
}

func allowedCustomFacet(prefixes []string, name string) bool {
	if len(prefixes) == 0 {
		return false
	}
	for _, prefix := range prefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

func datasetNamespaces(ol OpenLineageEvent) string {
	var values []string
	for _, ds := range append(append([]Dataset{}, ol.Inputs...), ol.Outputs...) {
		values = append(values, ds.Namespace)
	}
	return strings.Join(values, ",")
}

func datasetNames(ol OpenLineageEvent) string {
	var values []string
	for _, ds := range append(append([]Dataset{}, ol.Inputs...), ol.Outputs...) {
		values = append(values, ds.Name)
	}
	return strings.Join(values, ",")
}
