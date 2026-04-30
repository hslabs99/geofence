-- GPS+ harvest: winery anchor (EXIT tag, else first track outside winery fence after inside, else step1)
-- then first track within N m of vineyard fence union; path metres/minutes stored alongside primary GPS harvest.

ALTER TABLE tbl_distances
  ADD COLUMN IF NOT EXISTS gps_plus_avg_distance_m integer,
  ADD COLUMN IF NOT EXISTS gps_plus_avg_duration_min numeric(10, 2),
  ADD COLUMN IF NOT EXISTS gps_plus_sample_count smallint NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS gps_plus_averaged_at timestamptz;

COMMENT ON COLUMN tbl_distances.gps_plus_avg_distance_m IS 'Mean GPS+ path metres (vineyard target within buffer), successful samples only.';
COMMENT ON COLUMN tbl_distances.gps_plus_avg_duration_min IS 'Mean minutes winery anchor → first point in vineyard buffer.';
COMMENT ON COLUMN tbl_distances.gps_plus_sample_count IS 'Samples with gps_plus_outcome = success.';
COMMENT ON COLUMN tbl_distances.gps_plus_averaged_at IS 'When gps_plus_* rollups were last updated from samples.';

ALTER TABLE tbl_distances_gps_samples
  ADD COLUMN IF NOT EXISTS gps_plus_meters numeric(14, 2),
  ADD COLUMN IF NOT EXISTS gps_plus_minutes numeric(14, 4),
  ADD COLUMN IF NOT EXISTS gps_plus_outcome text,
  ADD COLUMN IF NOT EXISTS gps_plus_vineyard_tracking_id bigint;

COMMENT ON COLUMN tbl_distances_gps_samples.gps_plus_meters IS 'GPS+ path: winery anchor → first point within vineyard buffer (haversine sum).';
COMMENT ON COLUMN tbl_distances_gps_samples.gps_plus_minutes IS 'Minutes from winery anchor time to first buffer hit.';
COMMENT ON COLUMN tbl_distances_gps_samples.gps_plus_outcome IS 'success or failed; NULL if not computed (legacy rows).';
COMMENT ON COLUMN tbl_distances_gps_samples.gps_plus_vineyard_tracking_id IS 'tbl_tracking.id of first point inside buffered vineyard union.';
