import { execute } from '@/lib/db';

let driverMappEnsured = false;

/** Creates tbl_driver_mapp and worker_old if missing (same as web/sql/*.sql). Safe to call repeatedly. */
export async function ensureDriverMappSchema(): Promise<void> {
  if (driverMappEnsured) return;

  await execute(`
    CREATE TABLE IF NOT EXISTS tbl_driver_mapp (
      id            serial PRIMARY KEY,
      oldvworkname  text NOT NULL,
      newvworkname  text NOT NULL,
      created_at    timestamptz NOT NULL DEFAULT now()
    )`);

  await execute(`
    CREATE INDEX IF NOT EXISTS idx_tbl_driver_mapp_oldvworkname
      ON tbl_driver_mapp (oldvworkname)`);

  await execute(`
    ALTER TABLE tbl_vworkjobs
      ADD COLUMN IF NOT EXISTS worker_old text`);

  driverMappEnsured = true;
}
