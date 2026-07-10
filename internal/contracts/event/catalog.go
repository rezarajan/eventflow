package event

import (
	"fmt"
	"sort"
	"strings"
)

// Spec describes one supported domain event contract.
type Spec struct {
	Type       string
	SchemaPath string
	Topic      string
	Table      string
}

// Catalog indexes the supported domain event contracts by CloudEvents type.
type Catalog struct {
	byType map[string]Spec
}

// DefaultCatalog returns the supported event contracts for the sample school domain.
func DefaultCatalog() Catalog {
	return NewCatalog([]Spec{
		{Type: "school.registered.v1", SchemaPath: "contracts/internal/events/payloads/school-registered.v1.schema.json", Topic: "school.events.v1", Table: "schools"},
		{Type: "class.created.v1", SchemaPath: "contracts/internal/events/payloads/class-created.v1.schema.json", Topic: "school.events.v1", Table: "classes"},
		{Type: "student.enrolled.v1", SchemaPath: "contracts/internal/events/payloads/student-enrolled.v1.schema.json", Topic: "student.events.v1", Table: "students"},
		{Type: "attendance.submitted.v1", SchemaPath: "contracts/internal/events/payloads/attendance-submitted.v1.schema.json", Topic: "attendance.events.v1", Table: "attendance"},
		{Type: "attendance.corrected.v1", SchemaPath: "contracts/internal/events/payloads/attendance-corrected.v1.schema.json", Topic: "attendance.events.v1", Table: "attendance"},
		{Type: "exam-paper.uploaded.v1", SchemaPath: "contracts/internal/events/payloads/exam-paper-uploaded.v1.schema.json", Topic: "assessment.events.v1", Table: "exam_papers"},
		{Type: "grade.recorded.v1", SchemaPath: "contracts/internal/events/payloads/grade-recorded.v1.schema.json", Topic: "assessment.events.v1", Table: "grades"},
		{Type: "document.uploaded.v1", SchemaPath: "contracts/internal/events/payloads/document-uploaded.v1.schema.json", Topic: "document.events.v1", Table: "documents"},
		{Type: "document.checksummed.v1", SchemaPath: "contracts/internal/events/payloads/document-checksummed.v1.schema.json", Topic: "document.events.v1", Table: "documents"},
		{Type: "audit.event.recorded.v1", SchemaPath: "contracts/internal/events/payloads/audit-event-recorded.v1.schema.json", Topic: "audit.events.v1", Table: "audit_events"},
	})
}

// NewCatalog constructs a catalog from event specs.
func NewCatalog(specs []Spec) Catalog {
	byType := make(map[string]Spec, len(specs))
	for _, spec := range specs {
		spec.Type = strings.TrimSpace(spec.Type)
		if spec.Type == "" {
			continue
		}
		byType[spec.Type] = spec
	}
	return Catalog{byType: byType}
}

// Lookup returns the event spec for a CloudEvents type.
func (c Catalog) Lookup(eventType string) (Spec, bool) {
	spec, ok := c.byType[strings.TrimSpace(eventType)]
	return spec, ok
}

// MustLookup returns the event spec for a CloudEvents type or an error.
func (c Catalog) MustLookup(eventType string) (Spec, error) {
	spec, ok := c.Lookup(eventType)
	if !ok {
		return Spec{}, fmt.Errorf("unknown event type %q; available event types: %v", eventType, c.Types())
	}
	return spec, nil
}

// Specs returns catalog entries sorted by event type.
func (c Catalog) Specs() []Spec {
	types := c.Types()
	specs := make([]Spec, 0, len(types))
	for _, eventType := range types {
		specs = append(specs, c.byType[eventType])
	}
	return specs
}

// Types returns supported CloudEvents types in stable order.
func (c Catalog) Types() []string {
	types := make([]string, 0, len(c.byType))
	for eventType := range c.byType {
		types = append(types, eventType)
	}
	sort.Strings(types)
	return types
}
