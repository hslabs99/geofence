import { query } from '@/lib/db';

/**
 * Single centroid (WGS84) for all geofences whose fence_name matches any of the given strings (trim, case-insensitive).
 * Used for Google Maps driving directions between winery and vineyard fence areas.
 */
export async function fenceCentroidLatLngForFenceNames(names: string[]): Promise<{ lat: number; lng: number } | null> {
  const list = [...new Set(names.map((n) => n.trim()).filter(Boolean))];
  if (list.length === 0) return null;
  try {
    const rows = await query<{ lat: unknown; lng: unknown }>(
      `SELECT
         ST_Y(ST_Centroid(ST_Collect(ST_Force2D(g.geom))))::float8 AS lat,
         ST_X(ST_Centroid(ST_Collect(ST_Force2D(g.geom))))::float8 AS lng
       FROM tbl_geofences g
       WHERE g.geom IS NOT NULL
         AND EXISTS (
           SELECT 1 FROM unnest($1::text[]) AS n(nm)
           WHERE nm IS NOT NULL AND trim(nm) <> ''
             AND lower(trim(g.fence_name)) = lower(trim(nm))
         )`,
      [list]
    );
    const lat = Number(rows[0]?.lat);
    const lng = Number(rows[0]?.lng);
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;
    return { lat, lng };
  } catch {
    return null;
  }
}

export function googleMapsDrivingDirectionsUrl(
  origin: { lat: number; lng: number },
  dest: { lat: number; lng: number }
): string {
  const o = `${origin.lat},${origin.lng}`;
  const d = `${dest.lat},${dest.lng}`;
  return `https://www.google.com/maps/dir/?api=1&origin=${encodeURIComponent(o)}&destination=${encodeURIComponent(d)}&travelmode=driving`;
}
