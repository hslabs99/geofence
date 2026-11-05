import { execute } from '@/lib/db';

let driverJobsEnsured = false;

/** Creates tbl_driver_jobs if missing (same as web/sql/create_tbl_driver_jobs.sql). Safe to call repeatedly. */
export async function ensureTblDriverJobsSchema(): Promise<void> {
  if (driverJobsEnsured) return;

  await execute(`
    CREATE TABLE IF NOT EXISTS tbl_driver_jobs (
      id                  serial PRIMARY KEY,
      job_id              text NOT NULL,
      truck_id            text,
      driver              text,
      planned_start_time  timestamp without time zone,
      winery_name         text,
      vineyard_name       text
    )`);

  await execute(`
    CREATE INDEX IF NOT EXISTS idx_tbl_driver_jobs_job_id
      ON tbl_driver_jobs (job_id)`);

  await execute(`
    CREATE INDEX IF NOT EXISTS idx_tbl_driver_jobs_driver
      ON tbl_driver_jobs (driver)`);

  await execute(`
    CREATE INDEX IF NOT EXISTS idx_tbl_driver_jobs_winery_name
      ON tbl_driver_jobs (winery_name)`);

  await execute(`
    CREATE INDEX IF NOT EXISTS idx_tbl_driver_jobs_vineyard_name
      ON tbl_driver_jobs (vineyard_name)`);

  await execute(`
    CREATE INDEX IF NOT EXISTS idx_tbl_driver_jobs_planned_start_time
      ON tbl_driver_jobs (planned_start_time)`);

  await execute(`
    DO $$
    BEGIN
      IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'tbl_driver_jobs' AND column_name = 'step_1_at'
      ) AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'tbl_driver_jobs' AND column_name = 'step_1_click'
      ) THEN
        ALTER TABLE tbl_driver_jobs RENAME COLUMN step_1_at TO step_1_click;
        ALTER TABLE tbl_driver_jobs RENAME COLUMN step_2_at TO step_2_click;
        ALTER TABLE tbl_driver_jobs RENAME COLUMN step_3_at TO step_3_click;
        ALTER TABLE tbl_driver_jobs RENAME COLUMN step_4_at TO step_4_click;
        ALTER TABLE tbl_driver_jobs RENAME COLUMN step_5_at TO step_5_click;
      END IF;
    END $$`);

  await execute(`ALTER TABLE tbl_driver_jobs ADD COLUMN IF NOT EXISTS step_0_click timestamp without time zone`);
  await execute(`ALTER TABLE tbl_driver_jobs ADD COLUMN IF NOT EXISTS step_1_click timestamp without time zone`);
  await execute(`ALTER TABLE tbl_driver_jobs ADD COLUMN IF NOT EXISTS step_2_click timestamp without time zone`);
  await execute(`ALTER TABLE tbl_driver_jobs ADD COLUMN IF NOT EXISTS step_3_click timestamp without time zone`);
  await execute(`ALTER TABLE tbl_driver_jobs ADD COLUMN IF NOT EXISTS step_4_click timestamp without time zone`);
  await execute(`ALTER TABLE tbl_driver_jobs ADD COLUMN IF NOT EXISTS step_5_click timestamp without time zone`);
  await execute(`ALTER TABLE tbl_driver_jobs ADD COLUMN IF NOT EXISTS winery_geo_fence text`);
  await execute(`ALTER TABLE tbl_driver_jobs ADD COLUMN IF NOT EXISTS vineyard_geo_fence text`);
  await execute(`ALTER TABLE tbl_driver_jobs ADD COLUMN IF NOT EXISTS imei text`);
  await execute(`ALTER TABLE tbl_driver_jobs ADD COLUMN IF NOT EXISTS step_2_manual boolean`);
  await execute(`ALTER TABLE tbl_driver_jobs ADD COLUMN IF NOT EXISTS step_3_manual boolean`);
  await execute(`ALTER TABLE tbl_driver_jobs ADD COLUMN IF NOT EXISTS step_4_manual boolean`);
  await execute(`ALTER TABLE tbl_driver_jobs ADD COLUMN IF NOT EXISTS log_cleared_at timestamp without time zone`);
  await execute(`ALTER TABLE tbl_driver_jobs ADD COLUMN IF NOT EXISTS vwork_account text`);
  await execute(`ALTER TABLE tbl_driver_jobs ADD COLUMN IF NOT EXISTS vwork_job_id text`);
  await execute(`ALTER TABLE tbl_driver_jobs ADD COLUMN IF NOT EXISTS vwork_step_1_name text`);
  await execute(`ALTER TABLE tbl_driver_jobs ADD COLUMN IF NOT EXISTS vwork_step_2_name text`);
  await execute(`ALTER TABLE tbl_driver_jobs ADD COLUMN IF NOT EXISTS vwork_step_3_name text`);
  await execute(`ALTER TABLE tbl_driver_jobs ADD COLUMN IF NOT EXISTS vwork_step_4_name text`);
  await execute(`ALTER TABLE tbl_driver_jobs ADD COLUMN IF NOT EXISTS vwork_step_5_name text`);
  await execute(`ALTER TABLE tbl_driver_jobs ADD COLUMN IF NOT EXISTS truck_rego text`);
  await execute(`ALTER TABLE tbl_driver_jobs ADD COLUMN IF NOT EXISTS trailer_rego text`);
  await execute(`ALTER TABLE tbl_driver_jobs ADD COLUMN IF NOT EXISTS confirmed_driver text`);
  await execute(`
    CREATE TABLE IF NOT EXISTS tbl_truck_plates (
      truck_id      text PRIMARY KEY,
      truck_rego    text,
      trailer_rego  text
    )`);

  driverJobsEnsured = true;
}
