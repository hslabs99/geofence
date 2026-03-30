import { execute } from '@/lib/db';

let tblDevicesEnsured = false;

/** Ensures tbl_devices has imei for TrackSolid merge. Safe to call repeatedly. */
export async function ensureTblDevicesSchema(): Promise<void> {
  if (tblDevicesEnsured) return;

  await execute(`ALTER TABLE tbl_devices ADD COLUMN IF NOT EXISTS imei text`);

  tblDevicesEnsured = true;
}
