CREATE TABLE IF NOT EXISTS sampledb.document_uploads (
  document_upload_id UUID PRIMARY KEY,
  object_uri TEXT NOT NULL,
  content_type TEXT NOT NULL,
  checksum_sha256 TEXT NOT NULL,
  byte_size BIGINT NOT NULL CHECK (byte_size >= 0),
  uploaded_by_role TEXT NOT NULL,
  uploaded_at TIMESTAMPTZ NOT NULL
);
