CREATE TABLE IF NOT EXISTS sampledb.audit_events (
  audit_event_id UUID PRIMARY KEY,
  run_id UUID NOT NULL,
  event_type TEXT NOT NULL,
  actor_type TEXT,
  resource_type TEXT NOT NULL,
  resource_id TEXT NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL,
  metadata JSONB
);
