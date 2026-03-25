import { execute } from '@/lib/db';

let vineMappEnsured = false;

/** Creates tbl_vine_mapp and vineyard_name_old if missing (same as web/sql/*.sql). Safe to call repeatedly. */
export async function ensureVineMappSchema(): Promise<void> {
  if (vineMappEnsured) return;

  await execute(`
    CREATE TABLE IF NOT EXISTS tbl_vine_mapp (
      id            serial PRIMARY KEY,
      oldvworkname  text NOT NULL,
      newvworkname  text NOT NULL,
      created_at    timestamptz NOT NULL DEFAULT now()
    )`);

  await execute(`
    CREATE INDEX IF NOT EXISTS idx_tbl_vine_mapp_oldvworkname
      ON tbl_vine_mapp (oldvworkname)`);

  await execute(`
    ALTER TABLE tbl_vworkjobs
      ADD COLUMN IF NOT EXISTS vineyard_name_old text`);

  vineMappEnsured = true;
}
