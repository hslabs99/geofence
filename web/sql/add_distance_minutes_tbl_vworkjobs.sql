-- Copy driving distance (m) and duration (min) from tbl_distances onto each job by winery+vineyard pair.
-- Run once; then use Admin → Distances → "Populate vWork" to refresh from tbl_distances.

ALTER TABLE tbl_vworkjobs
  ADD COLUMN IF NOT EXISTS distance numeric(12, 2),
  ADD COLUMN IF NOT EXISTS minutes numeric(12, 2);

COMMENT ON COLUMN tbl_vworkjobs.distance IS 'Metres from tbl_distances.distance_m for this job delivery_winery + vineyard_name pair (Populate vWork).';
COMMENT ON COLUMN tbl_vworkjobs.minutes IS 'Minutes from tbl_distances.duration_min for this job pair (Populate vWork).';
