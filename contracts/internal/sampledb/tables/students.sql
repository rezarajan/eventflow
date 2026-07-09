CREATE TABLE IF NOT EXISTS sampledb.students (
  student_id UUID PRIMARY KEY,
  school_id UUID NOT NULL REFERENCES sampledb.schools(school_id),
  class_id UUID NOT NULL REFERENCES sampledb.classes(class_id),
  synthetic_student_number TEXT NOT NULL UNIQUE,
  enrollment_status TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);
