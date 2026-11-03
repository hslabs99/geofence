-- Driver visibility jobs (TBL_driver_jobs). Unquoted name folds to tbl_driver_jobs.
-- planned_start_time is stored verbatim (timestamp without time zone), same as tbl_vworkjobs.
-- Run in your SQL client against the same DB as the app. The Driver Visibility API also creates this if missing.

CREATE TABLE IF NOT EXISTS tbl_driver_jobs (
  id                  serial PRIMARY KEY,
  job_id              text NOT NULL,
  truck_id            text,
  driver              text,
  planned_start_time  timestamp without time zone,
  winery_name         text,
  vineyard_name       text,
  winery_geo_fence    text,
  vineyard_geo_fence  text,
  imei                text,
  step_1_click        timestamp without time zone,
  step_2_click        timestamp without time zone,
  step_3_click        timestamp without time zone,
  step_4_click        timestamp without time zone,
  step_5_click        timestamp without time zone,
  step_2_manual       boolean,
  step_3_manual       boolean,
  step_4_manual       boolean,
  log_cleared_at      timestamp without time zone
);

CREATE INDEX IF NOT EXISTS idx_tbl_driver_jobs_job_id
  ON tbl_driver_jobs (job_id);

CREATE INDEX IF NOT EXISTS idx_tbl_driver_jobs_driver
  ON tbl_driver_jobs (driver);

CREATE INDEX IF NOT EXISTS idx_tbl_driver_jobs_winery_name
  ON tbl_driver_jobs (winery_name);

CREATE INDEX IF NOT EXISTS idx_tbl_driver_jobs_vineyard_name
  ON tbl_driver_jobs (vineyard_name);

CREATE INDEX IF NOT EXISTS idx_tbl_driver_jobs_planned_start_time
  ON tbl_driver_jobs (planned_start_time);

COMMENT ON TABLE tbl_driver_jobs IS 'Driver Visibility jobs: job, truck, driver, planned start, winery, and vineyard.';
