-- Queue for low-volume Step4to5 audit fixes (populate missing step_4_actual_time).
-- Safe to run multiple times.

CREATE TABLE IF NOT EXISTS tbl_step4to5_audit_queue (
  id bigserial PRIMARY KEY,
  job_id text NOT NULL UNIQUE,
  customer text NULL,
  template text NULL,
  delivery_winery text NULL,
  vineyard_group text NULL,
  trailermode text NULL,
  reason text NULL,
  status text NOT NULL DEFAULT 'pending', -- pending | processing | done | skipped | error
  attempts int NOT NULL DEFAULT 0,
  last_error text NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  processed_at timestamptz NULL
);

CREATE INDEX IF NOT EXISTS idx_step4to5_audit_queue_status ON tbl_step4to5_audit_queue(status);

