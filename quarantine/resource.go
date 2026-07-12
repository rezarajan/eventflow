package quarantine

import (
	"context"
	"fmt"
	"strings"

	"github.com/rezarajan/eventflow/resource"
)

type ResourceSpec struct {
	Path string `yaml:"path" json:"path"`
}

func Register(catalog *resource.Catalog) error {
	return resource.Register(catalog, resource.Definition[ResourceSpec]{
		GVK: resource.GVK("QuarantineStore"),
		Default: func(spec *ResourceSpec) error {
			if spec.Path == "" {
				spec.Path = "var/eventflow/quarantine.sqlite"
			}
			return nil
		},
		Validate: func(_ context.Context, spec ResourceSpec) error {
			if strings.TrimSpace(spec.Path) == "" {
				return fmt.Errorf("path is required")
			}
			return nil
		},
		Build: func(_ context.Context, _ resource.BuildContext, spec ResourceSpec) (any, error) {
			return New(spec.Path), nil
		},
		Capabilities: []resource.Capability{resource.CapabilityComponent, resource.CapabilityQuarantineStore},
	})
}
