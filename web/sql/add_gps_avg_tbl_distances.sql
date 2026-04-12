-- GPS harvest roll-up columns for tbl_distances (run once).

ALTER TABLE tbl_distances
  ADD COLUMN IF NOT EXISTS gps_avg_distance_m integer,
  ADD COLUMN IF NOT EXISTS gps_avg_duration_min numeric(10, 2),
  ADD COLUMN IF NOT EXISTS gps_sample_count smallint NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS gps_averaged_at timestamptz;

COMMENT ON COLUMN tbl_distances.gps_avg_distance_m IS 'Mean of GPS path samples (m), up to 5 jobs.';
COMMENT ON COLUMN tbl_distances.gps_avg_duration_min IS 'Mean minutes (vineyard enter − winery exit).';
COMMENT ON COLUMN tbl_distances.gps_sample_count IS 'Rows in tbl_distances_gps_samples for this pair.';
COMMENT ON COLUMN tbl_distances.gps_averaged_at IS 'When gps_avg_* and distance/duration were last updated from samples.';
