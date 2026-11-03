-- Driver job log: cautionary declines, cautionary accepts, and automatic accepts.
-- Fence enter/exit lines are read from tbl_current_locations, not stored here.
-- `at` is a verbatim clock string (timestamp without time zone).

CREATE TABLE IF NOT EXISTS tbl_driver_job_log (
  id             serial PRIMARY KEY,
  driver_job_id  integer NOT NULL,
  at             timestamp without time zone NOT NULL,
  kind           text NOT NULL,
  step           integer,
  outcome        text,
  detail         text
);

CREATE INDEX IF NOT EXISTS idx_tbl_driver_job_log_job
  ON tbl_driver_job_log (driver_job_id, id);
