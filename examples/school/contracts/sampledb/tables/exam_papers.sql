CREATE TABLE IF NOT EXISTS sampledb.exam_papers (
  exam_paper_id UUID PRIMARY KEY,
  student_id UUID NOT NULL REFERENCES sampledb.students(student_id),
  school_id UUID NOT NULL REFERENCES sampledb.schools(school_id),
  class_id UUID NOT NULL REFERENCES sampledb.classes(class_id),
  subject_id UUID NOT NULL REFERENCES sampledb.subjects(subject_id),
  term TEXT NOT NULL,
  assessment_type TEXT NOT NULL,
  document_upload_id UUID,
  created_at TIMESTAMPTZ NOT NULL
);
