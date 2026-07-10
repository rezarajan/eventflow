CREATE TABLE IF NOT EXISTS sampledb.subjects (
  subject_id UUID PRIMARY KEY,
  subject_code TEXT NOT NULL UNIQUE,
  subject_name TEXT NOT NULL
);
