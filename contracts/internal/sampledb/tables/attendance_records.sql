CREATE TABLE IF NOT EXISTS sampledb.attendance_records (
  attendance_id UUID PRIMARY KEY,
  student_id UUID NOT NULL REFERENCES sampledb.students(student_id),
  school_id UUID NOT NULL REFERENCES sampledb.schools(school_id),
  class_id UUID NOT NULL REFERENCES sampledb.classes(class_id),
  attendance_date DATE NOT NULL,
  status_code TEXT NOT NULL CHECK (status_code IN ('PRESENT', 'ABSENT', 'LATE', 'EXCUSED')),
  reason_code TEXT,
  submitted_at TIMESTAMPTZ NOT NULL
);
