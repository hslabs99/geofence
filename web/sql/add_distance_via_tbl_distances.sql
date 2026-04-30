-- How distance_m / duration_min were chosen after GPS harvest rollup (run once).

ALTER TABLE tbl_distances
  ADD COLUMN IF NOT EXISTS distance_via text;

COMMENT ON COLUMN tbl_distances.distance_via IS
  'GPSTAGS: at least one primary GPS path sample (fence ENTER path). GPS+: no primary OK samples but GPS+ OK. FAIL: neither.';
