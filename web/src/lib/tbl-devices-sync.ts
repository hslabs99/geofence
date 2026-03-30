import { execute, query } from '@/lib/db';
import { ensureTblDevicesSchema } from '@/lib/ensure-tbl-devices-schema';
import type { DeviceItem } from '@/lib/tracksolid';

export type MergeVworkjobsResult = {
  inserted: number;
  totalDevices: number;
  totalWorkers: number;
};

/**
 * Insert distinct tbl_vworkjobs.worker values into tbl_devices when missing (same as legacy sync-from-vworkjobs).
 */
export async function mergeVworkjobsIntoTblDevices(): Promise<MergeVworkjobsResult> {
  await ensureTblDevicesSchema();

  const withGroup = `
    INSERT INTO tbl_devices (device_name, "!group")
    SELECT DISTINCT trim(w.worker), 'Newt'
    FROM tbl_vworkjobs w
    WHERE w.worker IS NOT NULL AND trim(w.worker) <> ''
    AND NOT EXISTS (SELECT 1 FROM tbl_devices d WHERE trim(d.device_name) = trim(w.worker))
  `;
  const withoutGroup = `
    INSERT INTO tbl_devices (device_name)
    SELECT DISTINCT trim(w.worker)
    FROM tbl_vworkjobs w
    WHERE w.worker IS NOT NULL AND trim(w.worker) <> ''
    AND NOT EXISTS (SELECT 1 FROM tbl_devices d WHERE trim(d.device_name) = trim(w.worker))
  `;
  let inserted = 0;
  try {
    inserted = await execute(withGroup);
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    if (msg.includes('"!group"') || msg.includes('!group') || msg.includes('column')) {
      inserted = await execute(withoutGroup);
    } else {
      throw e;
    }
  }

  const countResult = await query<{ count: string }>('SELECT COUNT(*)::text AS count FROM tbl_devices');
  const totalDevices = Number(countResult[0]?.count ?? 0);
  const workersResult = await query<{ count: string }>(
    "SELECT COUNT(DISTINCT trim(worker))::text AS count FROM tbl_vworkjobs WHERE worker IS NOT NULL AND trim(worker) <> ''"
  );
  const totalWorkers = Number(workersResult[0]?.count ?? 0);

  return { inserted, totalDevices, totalWorkers };
}

export type MergeTracksolidResult = {
  inserted: number;
  imeiUpdates: number;
};

/**
 * Upsert TrackSolid device names and IMEIs into tbl_devices so polling can use stored IMEI.
 */
export async function mergeTracksolidDevicesIntoTblDevices(devices: DeviceItem[]): Promise<MergeTracksolidResult> {
  await ensureTblDevicesSchema();

  let inserted = 0;
  let imeiUpdates = 0;

  for (const d of devices) {
    const name = (d.deviceName ?? '').trim();
    const imei = (d.imei ?? '').trim();
    if (!name || !imei) continue;

    const ins = await execute(
      `INSERT INTO tbl_devices (device_name, imei)
       SELECT $1::text, $2::text
       WHERE NOT EXISTS (
         SELECT 1 FROM tbl_devices t WHERE trim(t.device_name) = trim($1::text)
       )`,
      [name, imei]
    );
    inserted += ins;

    const upd = await execute(
      `UPDATE tbl_devices SET imei = $2::text WHERE trim(device_name) = trim($1::text) AND (imei IS DISTINCT FROM $2::text)`,
      [name, imei]
    );
    imeiUpdates += upd;
  }

  return { inserted, imeiUpdates };
}
