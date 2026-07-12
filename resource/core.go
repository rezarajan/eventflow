package resource

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/rezarajan/eventflow/admission"
)

type OpenLineageContractSpec struct {
	Type                  string   `yaml:"type" json:"type"`
	Version               string   `yaml:"version,omitempty" json:"version,omitempty"`
	AllowedSchemaVersions []string `yaml:"allowedSchemaVersions,omitempty" json:"allowedSchemaVersions,omitempty"`
	RequiredExtensions    []string `yaml:"requiredCloudEventsExtensions,omitempty" json:"requiredCloudEventsExtensions,omitempty"`
	MaxEventBytes         int64    `yaml:"maxEventBytes,omitempty" json:"maxEventBytes,omitempty"`
}

type OpenLineagePolicySpec struct {
	Version                       string              `yaml:"version,omitempty" json:"version,omitempty"`
	AllowedSchemaVersions         []string            `yaml:"allowedSchemaVersions,omitempty" json:"allowedSchemaVersions,omitempty"`
	AllowedProducers              []string            `yaml:"allowedProducers,omitempty" json:"allowedProducers,omitempty"`
	PrincipalJobNamespaces        map[string][]string `yaml:"principalJobNamespaces,omitempty" json:"principalJobNamespaces,omitempty"`
	PrincipalDatasetNamespaces    map[string][]string `yaml:"principalDatasetNamespaces,omitempty" json:"principalDatasetNamespaces,omitempty"`
	RequiredFacets                []string            `yaml:"requiredFacets,omitempty" json:"requiredFacets,omitempty"`
	ProhibitedFacets              []string            `yaml:"prohibitedFacets,omitempty" json:"prohibitedFacets,omitempty"`
	AllowCustomFacets             bool                `yaml:"allowCustomFacets,omitempty" json:"allowCustomFacets,omitempty"`
	AllowedCustomFacetPrefixes    []string            `yaml:"allowedCustomFacetPrefixes,omitempty" json:"allowedCustomFacetPrefixes,omitempty"`
	AllowedDatasetURISchemes      []string            `yaml:"allowedDatasetUriSchemes,omitempty" json:"allowedDatasetUriSchemes,omitempty"`
	MaxEventBytes                 int64               `yaml:"maxEventBytes,omitempty" json:"maxEventBytes,omitempty"`
	MaxFacetBytes                 int64               `yaml:"maxFacetBytes,omitempty" json:"maxFacetBytes,omitempty"`
	RequiredCloudEventsExtensions []string            `yaml:"requiredCloudEventsExtensions,omitempty" json:"requiredCloudEventsExtensions,omitempty"`
	AllowedEnvironments           []string            `yaml:"allowedEnvironments,omitempty" json:"allowedEnvironments,omitempty"`
	AllowedTenants                []string            `yaml:"allowedTenants,omitempty" json:"allowedTenants,omitempty"`
	JobNamespacePattern           string              `yaml:"jobNamespacePattern,omitempty" json:"jobNamespacePattern,omitempty"`
	JobNamePattern                string              `yaml:"jobNamePattern,omitempty" json:"jobNamePattern,omitempty"`
	DatasetNamespacePattern       string              `yaml:"datasetNamespacePattern,omitempty" json:"datasetNamespacePattern,omitempty"`
	DatasetNamePattern            string              `yaml:"datasetNamePattern,omitempty" json:"datasetNamePattern,omitempty"`
	RateLimitPerMinute            int                 `yaml:"rateLimitPerMinute,omitempty" json:"rateLimitPerMinute,omitempty"`
}

type EventFlowSpec struct {
	Mode                    string     `yaml:"mode,omitempty" json:"mode,omitempty"`
	ReceiverRef             *Reference `yaml:"receiverRef,omitempty" json:"receiverRef,omitempty"`
	ContractRef             Reference  `yaml:"contractRef" json:"contractRef"`
	PolicyRef               Reference  `yaml:"policyRef" json:"policyRef"`
	EmitterRef              *Reference `yaml:"emitterRef,omitempty" json:"emitterRef,omitempty"`
	QuarantineRef           Reference  `yaml:"quarantineRef" json:"quarantineRef"`
	SpoolRef                *Reference `yaml:"spoolRef,omitempty" json:"spoolRef,omitempty"`
	BrokerUnavailablePolicy string     `yaml:"brokerUnavailablePolicy,omitempty" json:"brokerUnavailablePolicy,omitempty"`
}

type Flow struct {
	Name                    string
	Mode                    string
	Receiver                any
	Contract                admission.Contract
	Policy                  admission.Policy
	RateLimitPerMinute      int
	Emitter                 any
	Quarantine              any
	Spool                   any
	BrokerUnavailablePolicy string
}

func RegisterCore(catalog *Catalog) {
	_ = Register(catalog, Definition[OpenLineageContractSpec]{
		GVK: GVK("OpenLineageContract"),
		Default: func(spec *OpenLineageContractSpec) error {
			if spec.Type == "" {
				spec.Type = admission.CloudEventType
			}
			return nil
		},
		Validate: func(_ context.Context, spec OpenLineageContractSpec) error {
			if spec.Type != admission.CloudEventType {
				return fmt.Errorf("type must be %s", admission.CloudEventType)
			}
			return nil
		},
		Build: func(_ context.Context, bctx BuildContext, spec OpenLineageContractSpec) (any, error) {
			return admission.Contract{Name: bctx.ResourceName(), Version: spec.Version, Type: spec.Type, AllowedSchemaVersions: spec.AllowedSchemaVersions, RequiredExtensions: spec.RequiredExtensions, MaxEventBytes: spec.MaxEventBytes}, nil
		},
		Capabilities: []Capability{CapabilityComponent, CapabilityOpenLineageContract},
	})
	_ = Register(catalog, Definition[OpenLineagePolicySpec]{
		GVK: GVK("OpenLineagePolicy"),
		Validate: func(_ context.Context, spec OpenLineagePolicySpec) error {
			for field, pattern := range map[string]string{
				"jobNamespacePattern":     spec.JobNamespacePattern,
				"jobNamePattern":          spec.JobNamePattern,
				"datasetNamespacePattern": spec.DatasetNamespacePattern,
				"datasetNamePattern":      spec.DatasetNamePattern,
			} {
				if pattern != "" {
					if _, err := regexp.Compile(pattern); err != nil {
						return fmt.Errorf("%s: %w", field, err)
					}
				}
			}
			return nil
		},
		Build: func(_ context.Context, bctx BuildContext, spec OpenLineagePolicySpec) (any, error) {
			return admission.Policy{
				Name: bctx.ResourceName(), Version: spec.Version, AllowedSchemaVersions: spec.AllowedSchemaVersions,
				AllowedProducers: spec.AllowedProducers, PrincipalJobNamespaces: spec.PrincipalJobNamespaces,
				PrincipalDatasetNamespaces: spec.PrincipalDatasetNamespaces, RequiredFacets: spec.RequiredFacets,
				ProhibitedFacets: spec.ProhibitedFacets, AllowCustomFacets: spec.AllowCustomFacets,
				AllowedCustomFacetPrefixes: spec.AllowedCustomFacetPrefixes, AllowedDatasetURISchemes: spec.AllowedDatasetURISchemes,
				MaxEventBytes: spec.MaxEventBytes, MaxFacetBytes: spec.MaxFacetBytes,
				RequiredCloudEventsExtensions: spec.RequiredCloudEventsExtensions, AllowedEnvironments: spec.AllowedEnvironments,
				AllowedTenants: spec.AllowedTenants, JobNamespacePattern: spec.JobNamespacePattern, JobNamePattern: spec.JobNamePattern,
				DatasetNamespacePattern: spec.DatasetNamespacePattern, DatasetNamePattern: spec.DatasetNamePattern,
				RateLimitPerMinute: spec.RateLimitPerMinute,
			}, nil
		},
		Capabilities: []Capability{CapabilityComponent, CapabilityOpenLineagePolicy},
	})
	_ = Register(catalog, Definition[EventFlowSpec]{
		GVK: GVK("EventFlow"),
		Default: func(spec *EventFlowSpec) error {
			if spec.Mode == "" {
				spec.Mode = "http-to-kafka"
			}
			if spec.BrokerUnavailablePolicy == "" {
				spec.BrokerUnavailablePolicy = "reject"
			}
			return nil
		},
		Validate: func(_ context.Context, spec EventFlowSpec) error {
			switch spec.Mode {
			case "http-to-kafka", "kafka-to-marquez", "standalone":
			default:
				return fmt.Errorf("unsupported mode %q", spec.Mode)
			}
			if spec.ContractRef.Kind == "" || spec.PolicyRef.Kind == "" || spec.QuarantineRef.Kind == "" {
				return fmt.Errorf("contractRef, policyRef, and quarantineRef are required")
			}
			if spec.ReceiverRef == nil {
				return fmt.Errorf("receiverRef is required")
			}
			if spec.EmitterRef == nil && spec.Mode != "standalone" {
				return fmt.Errorf("emitterRef is required")
			}
			if spec.BrokerUnavailablePolicy != "reject" && spec.BrokerUnavailablePolicy != "spool" {
				return fmt.Errorf("brokerUnavailablePolicy must be reject or spool")
			}
			if spec.BrokerUnavailablePolicy == "spool" && spec.SpoolRef == nil {
				return fmt.Errorf("spoolRef is required when brokerUnavailablePolicy is spool")
			}
			return nil
		},
		References: func(spec EventFlowSpec) []Reference {
			refs := []Reference{}
			receiver := *spec.ReceiverRef
			if spec.Mode == "http-to-kafka" || spec.Mode == "standalone" {
				receiver.Capability = CapabilityHTTPReceiver
			} else {
				receiver.Capability = CapabilityRedpandaReceiver
			}
			refs = append(refs, receiver)
			contract := spec.ContractRef
			contract.Capability = CapabilityOpenLineageContract
			policy := spec.PolicyRef
			policy.Capability = CapabilityOpenLineagePolicy
			quarantine := spec.QuarantineRef
			quarantine.Capability = CapabilityQuarantineStore
			refs = append(refs, contract, policy, quarantine)
			if spec.EmitterRef != nil {
				emitter := *spec.EmitterRef
				if spec.Mode == "kafka-to-marquez" || spec.Mode == "standalone" {
					emitter.Capability = CapabilityHTTPEmitter
				} else {
					emitter.Capability = CapabilityRedpandaEmitter
				}
				refs = append(refs, emitter)
			}
			if spec.SpoolRef != nil {
				spool := *spec.SpoolRef
				spool.Capability = CapabilitySQLiteSpool
				refs = append(refs, spool)
			}
			return refs
		},
		Build: func(_ context.Context, bctx BuildContext, spec EventFlowSpec) (any, error) {
			contract, err := bctx.Get(spec.ContractRef)
			if err != nil {
				return Flow{}, err
			}
			policy, err := bctx.Get(spec.PolicyRef)
			if err != nil {
				return Flow{}, err
			}
			receiver, err := bctx.Get(*spec.ReceiverRef)
			if err != nil {
				return Flow{}, err
			}
			quarantine, err := bctx.Get(spec.QuarantineRef)
			if err != nil {
				return Flow{}, err
			}
			var emitter any
			if spec.EmitterRef != nil {
				emitter, err = bctx.Get(*spec.EmitterRef)
				if err != nil {
					return Flow{}, err
				}
			}
			var spool any
			if spec.SpoolRef != nil {
				spool, err = bctx.Get(*spec.SpoolRef)
				if err != nil {
					return Flow{}, err
				}
			}
			compiledPolicy := policy.(admission.Policy)
			return Flow{Name: bctx.ResourceName(), Mode: spec.Mode, Receiver: receiver, Contract: contract.(admission.Contract), Policy: compiledPolicy, RateLimitPerMinute: compiledPolicy.RateLimitPerMinute, Emitter: emitter, Quarantine: quarantine, Spool: spool, BrokerUnavailablePolicy: spec.BrokerUnavailablePolicy}, nil
		},
		Capabilities: []Capability{CapabilityComponent, CapabilityEventFlow},
	})
}

func ResourceKindAllowed(kind string) bool {
	switch strings.TrimSpace(kind) {
	case "OpenLineageContract", "OpenLineagePolicy", "HTTPReceiver", "RedpandaEmitter", "RedpandaReceiver", "HTTPEmitter", "QuarantineStore", "SQLiteSpool", "EventFlow":
		return true
	default:
		return false
	}
}
