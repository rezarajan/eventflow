CREATE TABLE IF NOT EXISTS sampledb.schools (
  school_id UUID PRIMARY KEY,
  district_id UUID NOT NULL,
  school_code TEXT NOT NULL UNIQUE,
  school_name TEXT NOT NULL,
  school_type TEXT NOT NULL,
  governance_type TEXT NOT NULL,
  latitude DOUBLE PRECISION,
  longitude DOUBLE PRECISION,
  created_at TIMESTAMPTZ NOT NULL
);
