import { query } from '@/lib/db';

function parseLatLng(coordinates: string | null): { lat: number; lng: number } | null {
  if (!coordinates) return null;
  const [latRaw, lngRaw] = coordinates.split(',');
  const lat = Number(latRaw);
  const lng = Number(lngRaw);
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;
  if (Math.abs(lat) > 90 || Math.abs(lng) > 180) return null;
  return { lat, lng };
}

/** Latest stored fix for this job's device. IMEI wins over the truck name. */
export async function latestDevicePoint(imei: string, truck: string): Promise<{ lat: number; lng: number } | null> {
  const rows = await query<{ coordinates: string | null }>(
    `SELECT coordinates
     FROM tbl_current_locations
     WHERE coordinates IS NOT NULL
       AND btrim(coordinates) <> ''
       AND (
         ($1 <> '' AND btrim(imei) = $1)
         OR ($2 <> '' AND lower(btrim(truck_id)) = lower(btrim($2)))
       )
     ORDER BY CASE WHEN $1 <> '' AND btrim(imei) = $1 THEN 0 ELSE 1 END, id DESC
     LIMIT 1`,
    [imei, truck],
  );
  return parseLatLng(rows[0]?.coordinates ?? null);
}

/** Metres from the point to the fence edge. Zero means on or inside the fence. */
export async function metersToFenceEdge(fenceName: string, lat: number, lng: number): Promise<number | null> {
  const rows = await query<{ meters: number | null }>(
    `SELECT MIN(
       ST_Distance(
         ST_Transform(ST_Force2D(g.geom), 4326)::geography,
         ST_SetSRID(ST_MakePoint($2, $1), 4326)::geography
       )
     )::float8 AS meters
     FROM tbl_geofences g
     WHERE g.geom IS NOT NULL
       AND lower(btrim(g.fence_name)) = lower(btrim($3))`,
    [lat, lng, fenceName],
  );
  const meters = rows[0]?.meters;
  if (meters == null || !Number.isFinite(Number(meters))) return null;
  return Math.round(Number(meters));
}
