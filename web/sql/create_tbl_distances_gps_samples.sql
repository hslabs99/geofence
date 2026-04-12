-- Per-job GPS harvest: winery EXIT → vineyard ENTER path (same window & mappings as derived steps).
-- Includes failed attempts (outcome) for debugging.

CREATE TABLE IF NOT EXISTS tbl_distances_gps_samples (
  id                     bigserial PRIMARY KEY,
  distance_id            integer NOT NULL
                           REFERENCES tbl_distances (id) ON DELETE CASCADE,
  job_id                 varchar(20) NOT NULL,
  outcome                text NOT NULL DEFAULT 'success',
  failure_reason         text,
  winery_tracking_id     bigint,
  vineyard_tracking_id   bigint,
  meters                 numeric(14, 2),
  minutes                numeric(14, 4),
  segment_point_count    integer,
  debug_json             jsonb,
  run_index              smallint,
  created_at             timestamptz NOT NULL DEFAULT now(),
  updated_at             timestamptz NOT NULL DEFAULT now(),
  UNIQUE (distance_id, job_id),
  CONSTRAINT chk_tbl_distances_gps_samples_outcome CHECK (outcome IN ('success', 'failed', 'skipped'))
);

CREATE INDEX IF NOT EXISTS idx_tbl_distances_gps_samples_distance_id
  ON tbl_distances_gps_samples (distance_id);

CREATE INDEX IF NOT EXISTS idx_tbl_distances_gps_samples_distance_outcome
  ON tbl_distances_gps_samples (distance_id, outcome);

COMMENT ON TABLE tbl_distances_gps_samples IS 'GPS harvest per job: success metrics or failed/skipped with debug_json.';
COMMENT ON COLUMN tbl_distances_gps_samples.debug_json IS 'Window, SQL hints, derived-steps debug, tracking counts.';
