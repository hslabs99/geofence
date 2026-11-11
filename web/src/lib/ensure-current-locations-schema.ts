import { execute } from '@/lib/db';

let ensured = false;
let imeiBackfilled = false;

/** Creates tbl_current_locations if missing. Safe to call repeatedly. */
export async function ensureCurrentLocationsSchema(): Promise<void> {
  if (ensured && imeiBackfilled) return;
  await execute(`
    CREATE TABLE IF NOT EXISTS tbl_current_locations (
      id           serial PRIMARY KEY,
      truck_id     text,
      location     text,
      gpstime      text,
      polltime     text,
      coordinates  text,
      fencename    text,
      softfencename text
    )`);
  await execute(`ALTER TABLE tbl_current_locations ADD COLUMN IF NOT EXISTS softfencename text`);
  await execute(`ALTER TABLE tbl_current_locations ADD COLUMN IF NOT EXISTS imei text`);
  await execute(`ALTER TABLE tbl_current_locations ADD COLUMN IF NOT EXISTS gpstime_nz timestamp`);
  try {
    await execute(`
      UPDATE tbl_current_locations c
      SET imei = nullif(trim(d.imei), '')
      FROM tbl_devices d
      WHERE c.imei IS NULL
        AND lower(trim(d.device_name)) = lower(trim(c.truck_id))
        AND nullif(trim(d.imei), '') IS NOT NULL`);
  } catch {
    // tbl_devices may not exist yet; new polls still store the API imei.
  }
  await execute(`
    UPDATE tbl_current_locations c
    SET imei = src.imei
    FROM (
      SELECT DISTINCT ON (lower(trim(truck_id))) lower(trim(truck_id)) AS truck_key, nullif(trim(imei), '') AS imei
      FROM tbl_current_locations
      WHERE nullif(trim(imei), '') IS NOT NULL
      ORDER BY lower(trim(truck_id)), id DESC
    ) src
    WHERE c.imei IS NULL
      AND lower(trim(c.truck_id)) = src.truck_key`);
  imeiBackfilled = true;
  await execute(`
    CREATE INDEX IF NOT EXISTS idx_tbl_current_locations_polltime
      ON tbl_current_locations (polltime DESC)`);
  await execute(`
    CREATE INDEX IF NOT EXISTS idx_tbl_current_locations_truck_lower
      ON tbl_current_locations (lower(truck_id), id DESC)`);
  await execute(`
    CREATE INDEX IF NOT EXISTS idx_tbl_current_locations_imei
      ON tbl_current_locations (btrim(imei), gpstime_nz DESC NULLS LAST, id DESC)`);
  ensured = true;
  await fillCurrentLocationGpsTimeNz();
}

/**
 * Display-only NZ clock for current-location GPS times.
 * gpstime stays the raw API string. This is the same derivation as position_time_nz:
 * the stored UTC-naive time, shifted to Pacific/Auckland in SQL only, and only where still null.
 */
export async function fillCurrentLocationGpsTimeNz(): Promise<number> {
  return execute(`
    UPDATE tbl_current_locations
    SET gpstime_nz = (left(gpstime, 19)::timestamp AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific/Auckland')
    WHERE gpstime_nz IS NULL
      AND gpstime ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}'
  `);
}
