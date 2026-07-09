CREATE TABLE IF NOT EXISTS sampledb.grades (
  grade_id UUID PRIMARY KEY,
  exam_paper_id UUID NOT NULL REFERENCES sampledb.exam_papers(exam_paper_id),
  student_id UUID NOT NULL REFERENCES sampledb.students(student_id),
  subject_id UUID NOT NULL REFERENCES sampledb.subjects(subject_id),
  score NUMERIC(5,2) NOT NULL CHECK (score >= 0 AND score <= 100),
  recorded_at TIMESTAMPTZ NOT NULL
);
