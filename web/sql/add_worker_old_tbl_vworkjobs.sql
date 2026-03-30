-- Add worker_old to tbl_vworkjobs so "Run Fixes" (driver name fixes) can store the previous value before updating worker.
-- Run once. Safe to run if column already exists.

ALTER TABLE tbl_vworkjobs
  ADD COLUMN IF NOT EXISTS worker_old text;

COMMENT ON COLUMN tbl_vworkjobs.worker_old IS 'Previous worker before last Run Fixes (driver name fix) application.';
