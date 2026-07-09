// Package schoolfaker provides one example generator adapter for school-domain synthetic data.
package schoolfaker

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/datascape/lakehouse-poc/internal/contracts/event"
	"github.com/datascape/lakehouse-poc/internal/ports/generator"
)

// Name is the registry name for the school demo generator.
const Name = "demo.school.v1"

// Generator emits generic facts for a synthetic school domain.
type Generator struct{}

// New constructs a school-domain generator adapter.
func New() *Generator {
	return &Generator{}
}

// Name returns the registry name for this generator.
func (g *Generator) Name() string {
	return Name
}

// Generate streams school-domain facts to the provided output channel.
func (g *Generator) Generate(ctx context.Context, config generator.Config, out chan<- event.Fact) error {
	defer close(out)
	settings := settingsFrom(config)
	if err := settings.validate(); err != nil {
		return err
	}
	rng := rand.New(rand.NewSource(config.Seed))
	for schoolIndex := 1; schoolIndex <= settings.Schools; schoolIndex++ {
		schoolID := fmt.Sprintf("SCH-%03d", schoolIndex)
		if err := emit(ctx, out, schoolFact(schoolID, schoolIndex, settings, rng)); err != nil {
			return err
		}
		for classIndex := 1; classIndex <= settings.ClassesPerSchool; classIndex++ {
			classID := fmt.Sprintf("%s-C%02d", schoolID, classIndex)
			if err := emit(ctx, out, classFact(schoolID, classID, classIndex)); err != nil {
				return err
			}
			for studentIndex := 1; studentIndex <= settings.StudentsPerClass; studentIndex++ {
				studentID := fmt.Sprintf("%s-S%03d", classID, studentIndex)
				if err := emit(ctx, out, studentFact(schoolID, classID, studentID, studentIndex, rng)); err != nil {
					return err
				}
				for day := 1; day <= settings.AttendanceDays; day++ {
					if err := emit(ctx, out, attendanceFact(schoolID, classID, studentID, day, rng)); err != nil {
						return err
					}
				}
				if err := emit(ctx, out, gradeFact(schoolID, classID, studentID, rng)); err != nil {
					return err
				}
				if settings.Documents {
					if err := emit(ctx, out, documentFact(schoolID, classID, studentID, rng)); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

// settings defines school generator parameters with safe defaults.
type settings struct {
	Country          string
	Schools          int
	ClassesPerSchool int
	StudentsPerClass int
	AttendanceDays   int
	Documents        bool
}

// settingsFrom parses generic generator parameters into school generator settings.
func settingsFrom(config generator.Config) settings {
	return settings{
		Country:          stringParam(config.Parameters, "country", "Demo Country"),
		Schools:          intParam(config.Parameters, "schools", 2),
		ClassesPerSchool: intParam(config.Parameters, "classes_per_school", 2),
		StudentsPerClass: intParam(config.Parameters, "students_per_class", 3),
		AttendanceDays:   intParam(config.Parameters, "attendance_days", 5),
		Documents:        boolParam(config.Parameters, "documents", true),
	}
}

// validate checks whether settings can produce a bounded demo dataset.
func (s settings) validate() error {
	if s.Schools < 1 || s.ClassesPerSchool < 1 || s.StudentsPerClass < 1 || s.AttendanceDays < 1 {
		return fmt.Errorf("schools, classes_per_school, students_per_class, and attendance_days must be positive")
	}
	return nil
}

// emit sends a fact to the output channel while respecting context cancellation.
func emit(ctx context.Context, out chan<- event.Fact, fact event.Fact) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case out <- fact:
		return nil
	}
}

// schoolFact creates a school registration fact.
func schoolFact(id string, index int, settings settings, rng *rand.Rand) event.Fact {
	return event.Fact{Kind: "school.registered.v1", Subject: id, Data: map[string]any{
		"school_id": id,
		"name":      fmt.Sprintf("%s School %03d", settings.Country, index),
		"district":  fmt.Sprintf("District %d", rng.Intn(5)+1),
		"type":      []string{"primary", "secondary"}[rng.Intn(2)],
	}}
}

// classFact creates a class creation fact.
func classFact(schoolID string, classID string, index int) event.Fact {
	return event.Fact{Kind: "class.created.v1", Subject: classID, Data: map[string]any{
		"school_id": schoolID,
		"class_id":  classID,
		"grade":     fmt.Sprintf("Grade %d", index),
	}}
}

// studentFact creates a student enrolment fact.
func studentFact(schoolID string, classID string, studentID string, index int, rng *rand.Rand) event.Fact {
	return event.Fact{Kind: "student.enrolled.v1", Subject: studentID, Data: map[string]any{
		"school_id":  schoolID,
		"class_id":   classID,
		"student_id": studentID,
		"ordinal":    index,
		"cohort":     2020 + rng.Intn(5),
	}}
}

// attendanceFact creates an attendance submission fact.
func attendanceFact(schoolID string, classID string, studentID string, day int, rng *rand.Rand) event.Fact {
	status := "present"
	if rng.Intn(100) < 12 {
		status = "absent"
	}
	return event.Fact{Kind: "attendance.submitted.v1", Subject: studentID, Data: map[string]any{
		"school_id":  schoolID,
		"class_id":   classID,
		"student_id": studentID,
		"day":        day,
		"status":     status,
	}}
}

// gradeFact creates a grade recording fact.
func gradeFact(schoolID string, classID string, studentID string, rng *rand.Rand) event.Fact {
	return event.Fact{Kind: "grade.recorded.v1", Subject: studentID, Data: map[string]any{
		"school_id":  schoolID,
		"class_id":   classID,
		"student_id": studentID,
		"subject":    []string{"mathematics", "english", "science"}[rng.Intn(3)],
		"score":      40 + rng.Intn(61),
	}}
}

// documentFact creates a synthetic text-document upload fact without writing document bytes.
func documentFact(schoolID string, classID string, studentID string, rng *rand.Rand) event.Fact {
	checksum := fmt.Sprintf("sha256:%064x", rng.Uint64())
	filename := fmt.Sprintf("%s-homework.txt", studentID)
	content := fmt.Sprintf("Synthetic homework submission for %s in %s. Score seed marker %d.", studentID, classID, rng.Intn(1000000))
	return event.Fact{Kind: "document.uploaded.v1", Subject: studentID, Data: map[string]any{
		"school_id":       schoolID,
		"class_id":        classID,
		"student_id":      studentID,
		"filename":        filename,
		"media_type":      "text/plain",
		"content_preview": content,
		"checksum":        checksum,
	}}
}

// intParam reads an integer setting from generic parameters.
func intParam(params map[string]any, key string, fallback int) int {
	value, ok := params[key]
	if !ok {
		return fallback
	}
	switch v := value.(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		return fallback
	}
}

// stringParam reads a string setting from generic parameters.
func stringParam(params map[string]any, key string, fallback string) string {
	value, ok := params[key].(string)
	if !ok || value == "" {
		return fallback
	}
	return value
}

// boolParam reads a boolean setting from generic parameters.
func boolParam(params map[string]any, key string, fallback bool) bool {
	value, ok := params[key].(bool)
	if !ok {
		return fallback
	}
	return value
}
