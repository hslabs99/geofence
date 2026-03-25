-- Add vineyard_name_old to tbl_vworkjobs so "Run Fixes" (vineyard name fixes) can store the previous value before updating vineyard_name.
-- Run once. Safe to run if column already exists.

ALTER TABLE tbl_vworkjobs
  ADD COLUMN IF NOT EXISTS vineyard_name_old text;

COMMENT ON COLUMN tbl_vworkjobs.vineyard_name_old IS 'Previous vineyard_name before last Run Fixes (vineyard name fix) application.';
