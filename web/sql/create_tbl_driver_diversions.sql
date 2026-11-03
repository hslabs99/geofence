-- Diversion reasons for the driver app, and one clock per start/stop on a job.
-- started_at and ended_at are verbatim phone clock strings (timestamp without time zone).

CREATE TABLE IF NOT EXISTS tbl_driver_diversion_types (
  id          serial PRIMARY KEY,
  name        text NOT NULL,
  sort_order  integer NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_tbl_driver_diversion_types_name
  ON tbl_driver_diversion_types (lower(btrim(name)));

CREATE TABLE IF NOT EXISTS tbl_driver_job_diversions (
  id             serial PRIMARY KEY,
  driver_job_id  integer NOT NULL,
  driver         text,
  type_name         text NOT NULL,
  note              text,
  expected_duration text,
  duration_seconds  integer,
  started_at        timestamp without time zone NOT NULL,
  ended_at          timestamp without time zone,
  ended_by_step     integer
);

CREATE INDEX IF NOT EXISTS idx_tbl_driver_job_diversions_job
  ON tbl_driver_job_diversions (driver_job_id, id);
