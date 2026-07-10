CREATE TABLE IF NOT EXISTS sampledb.classes (
  class_id UUID PRIMARY KEY,
  school_id UUID NOT NULL REFERENCES sampledb.schools(school_id),
  academic_year TEXT NOT NULL,
  grade_level TEXT NOT NULL,
  stream TEXT NOT NULL,
  teacher_ref TEXT,
  created_at TIMESTAMPTZ NOT NULL
);
