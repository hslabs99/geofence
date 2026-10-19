import { query } from '@/lib/db';
import { ensureTblDevicesSchema } from '@/lib/ensure-tbl-devices-schema';

export type DriverJobWrite = {
  jobId: string;
  truckId: string | null;
  imei: string | null;
  driver: string | null;
  plannedRaw: string | null;
  winery: string | null;
  vineyard: string | null;
  wineryFence: string | null;
  vineyardFence: string | null;
};

function blank(value: unknown): string | null {
  if (typeof value !== 'string') return null;
  const s = value.trim();
  return s || null;
}

/** Keep the spelling stored on tbl_geofences. Blank stays blank. Unknown names are rejected. */
async function canonicalFenceNames(
  fields: { label: string; value: string | null }[],
): Promise<{ ok: true; values: (string | null)[] } | { ok: false; error: string }> {
  const wanted = [...new Set(fields.map((field) => field.value?.toLowerCase()).filter((value): value is string => !!value))];
  const byKey = new Map<string, string>();
  if (wanted.length > 0) {
    const rows = await query<{ key: string; fence_name: string }>(
      `SELECT lower(btrim(fence_name)) AS key, btrim(fence_name) AS fence_name
       FROM tbl_geofences
       WHERE lower(btrim(fence_name)) = ANY($1::text[])`,
      [wanted],
    );
    for (const row of rows) {
      const key = row.key?.trim();
      const name = row.fence_name?.trim();
      if (key && name && !byKey.has(key)) byKey.set(key, name);
    }
  }
  const values: (string | null)[] = [];
  for (const field of fields) {
    if (!field.value) {
      values.push(null);
      continue;
    }
    const canonical = byKey.get(field.value.toLowerCase());
    if (!canonical) {
      return { ok: false, error: `${field.label} must be an existing GPS fence name` };
    }
    values.push(canonical);
  }
  return { ok: true, values };
}

/** IMEI must be a device we already store. The job keeps that device's name as truck_id. */
async function resolveDevice(imeiRaw: string | null): Promise<{ ok: true; imei: string | null; truckId: string | null } | { ok: false; error: string }> {
  if (!imeiRaw) return { ok: true, imei: null, truckId: null };
  await ensureTblDevicesSchema();
  const rows = await query<{ device_name: string | null; imei: string | null }>(
    `SELECT btrim(d.device_name) AS device_name, btrim(d.imei) AS imei
     FROM tbl_devices d
     WHERE btrim(d.imei) = $1
       AND d.device_name IS NOT NULL
       AND btrim(d.device_name) <> ''
     ORDER BY
       CASE WHEN EXISTS (
         SELECT 1 FROM tbl_current_locations c
         WHERE btrim(c.imei) = btrim(d.imei)
           AND lower(btrim(c.truck_id)) = lower(btrim(d.device_name))
       ) THEN 0 ELSE 1 END,
       btrim(d.device_name)
     LIMIT 1`,
    [imeiRaw],
  );
  const row = rows[0];
  const imei = row?.imei?.trim() || '';
  if (!imei) return { ok: false, error: 'Device must be an existing IMEI' };
  const truckId = row?.device_name?.trim() || imei;
  return { ok: true, imei, truckId };
}

export async function driverJobWriteFromBody(
  body: Record<string, unknown>,
): Promise<{ ok: true; job: DriverJobWrite } | { ok: false; error: string }> {
  const jobId = blank(body.job_id ?? body.jobId);
  if (!jobId) return { ok: false, error: 'Job ID is required' };
  const device = await resolveDevice(blank(body.imei));
  if (!device.ok) return device;
  const fences = await canonicalFenceNames([
    { label: 'Winery', value: blank(body.winery_name ?? body.wineryName ?? body.winery) },
    { label: 'Vineyard', value: blank(body.vineyard_name ?? body.vineyardName ?? body.vineyard) },
    { label: 'Winery geofence', value: blank(body.winery_geo_fence ?? body.wineryGeoFence) },
    { label: 'Vineyard geofence', value: blank(body.vineyard_geo_fence ?? body.vineyardGeoFence) },
  ]);
  if (!fences.ok) return fences;
  const [winery, vineyard, wineryFence, vineyardFence] = fences.values;
  return {
    ok: true,
    job: {
      jobId,
      truckId: device.truckId,
      imei: device.imei,
      driver: blank(body.driver),
      plannedRaw: blank(body.planned_start_time ?? body.plannedStartTime),
      winery,
      vineyard,
      wineryFence,
      vineyardFence,
    },
  };
}
