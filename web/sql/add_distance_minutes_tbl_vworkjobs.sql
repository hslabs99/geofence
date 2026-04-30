-- Populate vWork copies duration (min) from tbl_distances and stores distance as round-trip km:
-- (pair distance_m / 1000) * 2 (to vineyard and back). Run once; then Admin → Distances → "Populate vWork".

ALTER TABLE tbl_vworkjobs
  ADD COLUMN IF NOT EXISTS distance numeric(12, 2),
  ADD COLUMN IF NOT EXISTS minutes numeric(12, 2);

COMMENT ON COLUMN tbl_vworkjobs.distance IS 'Round-trip km from pair distance: (tbl_distances effective_m / 1000) * 2 (Populate vWork).';
COMMENT ON COLUMN tbl_vworkjobs.minutes IS 'One-way drive minutes from tbl_distances.duration_min for this job pair (Populate vWork).';
