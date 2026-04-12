-- Allow failed/skipped harvest attempts; nullable metrics when not successful.
-- Run once after create_tbl_distances_gps_samples.sql

ALTER TABLE tbl_distances_gps_samples
  ALTER COLUMN winery_tracking_id DROP NOT NULL,
  ALTER COLUMN vineyard_tracking_id DROP NOT NULL,
  ALTER COLUMN meters DROP NOT NULL,
  ALTER COLUMN minutes DROP NOT NULL;

ALTER TABLE tbl_distances_gps_samples
  ADD COLUMN IF NOT EXISTS outcome text NOT NULL DEFAULT 'success',
  ADD COLUMN IF NOT EXISTS failure_reason text,
  ADD COLUMN IF NOT EXISTS run_index smallint;

UPDATE tbl_distances_gps_samples SET outcome = 'success' WHERE outcome IS NULL OR outcome = '';

ALTER TABLE tbl_distances_gps_samples DROP CONSTRAINT IF EXISTS chk_tbl_distances_gps_samples_outcome;
ALTER TABLE tbl_distances_gps_samples
  ADD CONSTRAINT chk_tbl_distances_gps_samples_outcome
  CHECK (outcome IN ('success', 'failed', 'skipped'));

COMMENT ON COLUMN tbl_distances_gps_samples.outcome IS 'success: path computed; failed: anchors/path error; skipped: e.g. duplicate policy (unused if we upsert).';
COMMENT ON COLUMN tbl_distances_gps_samples.failure_reason IS 'Human-readable when outcome is failed or skipped.';
COMMENT ON COLUMN tbl_distances_gps_samples.run_index IS '1-based order within a harvest run (optional).';
