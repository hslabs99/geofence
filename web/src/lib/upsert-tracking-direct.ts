import { getClient } from '@/lib/db';
import { positionTimeForStorage } from '@/lib/verbatim-time';

const BATCH_SIZE = 500;
let trackingConflictKeyEnsured = false;

async function ensureTrackingConflictKey(): Promise<void> {
  if (trackingConflictKeyEnsured) return;
  const client = await getClient();
  try {
    await client.query(
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_tbl_tracking_device_name_position_time
       ON tbl_tracking (device_name, position_time)`
    );
    trackingConflictKeyEnsured = true;
  } finally {
    client.release();
  }
}

export type DirectTrackPoint = {
  lat: number;
  lng: number;
  gpsTime: string;
};

export async function upsertTracksolidPointsToTracking(
  deviceName: string,
  imei: string | null,
  points: DirectTrackPoint[]
): Promise<{ inserted: number; total: number; skipped: number }> {
  await ensureTrackingConflictKey();

  const rows: Array<{ lat: number; lng: number; positionTimeRaw: string }> = [];
  let skipped = 0;

  for (const p of points) {
    const raw = positionTimeForStorage(p.gpsTime ?? '');
    if (!raw) {
      skipped += 1;
      continue;
    }
    const lat = Number(p.lat);
    const lng = Number(p.lng);
    if (Number.isNaN(lat) || Number.isNaN(lng)) {
      skipped += 1;
      continue;
    }
    rows.push({ lat, lng, positionTimeRaw: raw });
  }

  if (rows.length === 0) {
    return { inserted: 0, total: points.length, skipped };
  }

  const client = await getClient();
  let inserted = 0;
  const imeiVal = imei?.trim() || null;

  try {
    await client.query('BEGIN');
    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const batch = rows.slice(i, i + BATCH_SIZE);
      const values: unknown[] = [];
      const placeholders: string[] = [];
      let idx = 1;
      for (const r of batch) {
        placeholders.push(
          `(
            NULL, $${idx}, $${idx + 1}, NULL, NULL, $${idx + 2},
            NULL, NULL, NULL, NULL, NULL, NULL, $${idx + 3}, $${idx + 4},
            ST_SetSRID(ST_MakePoint($${idx + 5}::double precision, $${idx + 6}::double precision), 4326),
            NULL, now(), NULL, false
          )`
        );
        values.push(deviceName, imeiVal, r.positionTimeRaw, r.lat, r.lng, r.lng, r.lat);
        idx += 7;
      }

      const insRes = await client.query(
        `INSERT INTO tbl_tracking (
          device_no, device_name, imei, model, ignition, position_time,
          speed_raw, speed_kmh, azimuth, position_type, satellites, data_type,
          lat, lon, geom, address, created_at, apirow, geofence_mapped
        )
        VALUES ${placeholders.join(', ')}
        ON CONFLICT (device_name, position_time) DO NOTHING`,
        values
      );
      inserted += insRes.rowCount ?? 0;
    }
    await client.query('COMMIT');
  } catch (e) {
    await client.query('ROLLBACK').catch(() => {});
    throw e;
  } finally {
    client.release();
  }

  return { inserted, total: points.length, skipped };
}
