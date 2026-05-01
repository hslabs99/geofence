import { query } from '@/lib/db';

/** Reject null island / bogus DB defaults; both endpoints must be real coordinates for a directions link. */
export function isValidDirectionsEndpoint(lat: number, lng: number): boolean {
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return false;
  if (lat === 0 && lng === 0) return false;
  if (Math.abs(lat) > 90 || Math.abs(lng) > 180) return false;
  return true;
}

/**
 * Single centroid (WGS84) for all geofences whose fence_name matches any of the given strings (trim, case-insensitive).
 * Used for Google Maps driving directions between winery and vineyard fence areas.
 */
export async function fenceCentroidLatLngForFenceNames(names: string[]): Promise<{ lat: number; lng: number } | null> {
  // Preserve input priority order (vWork name first, then mapping aliases), but de-dupe.
  const seen = new Set<string>();
  const list: string[] = [];
  for (const raw of names) {
    const t = raw.trim();
    if (!t) continue;
    const k = t.toLowerCase();
    if (seen.has(k)) continue;
    seen.add(k);
    list.push(t);
  }
  if (list.length === 0) return null;
  try {
    /**
     * IMPORTANT: Do NOT average/collect across multiple fence_name values.
     * The expanded name list can include aliases; if any alias matches an unrelated fence elsewhere,
     * the centroid shifts wildly (\"half a country away\"). Instead, pick the first name (by input
     * order) that resolves to at least one geofence.
     */
    const rows = await query<{ lat: unknown; lng: unknown }>(
      `WITH names AS (
         SELECT nm, ord
         FROM unnest($1::text[]) WITH ORDINALITY AS u(nm, ord)
         WHERE nm IS NOT NULL AND btrim(nm) <> ''
       ),
       matched AS (
         SELECT
           n.ord,
           ST_Y(ST_Centroid(ST_Collect(ST_Force2D(g.geom))))::float8 AS lat,
           ST_X(ST_Centroid(ST_Collect(ST_Force2D(g.geom))))::float8 AS lng
         FROM names n
         INNER JOIN tbl_geofences g
           ON g.geom IS NOT NULL
          AND lower(btrim(g.fence_name)) = lower(btrim(n.nm))
         GROUP BY n.ord
         ORDER BY n.ord
         LIMIT 1
       )
       SELECT lat, lng FROM matched`,
      [list],
    );
    const lat = Number(rows[0]?.lat);
    const lng = Number(rows[0]?.lng);
    if (!isValidDirectionsEndpoint(lat, lng)) return null;
    return { lat, lng };
  } catch {
    return null;
  }
}

export function googleMapsDrivingDirectionsUrl(
  origin: { lat: number; lng: number },
  dest: { lat: number; lng: number }
): string | null {
  if (
    !isValidDirectionsEndpoint(origin.lat, origin.lng) ||
    !isValidDirectionsEndpoint(dest.lat, dest.lng)
  ) {
    return null;
  }
  /**
   * Keep coords as literal `lat,lng` (no encoding) so Google reliably treats them as coordinates
   * rather than geocoding artifacts, and the initial URL stays short/stable.
   */
  const o = `${origin.lat},${origin.lng}`;
  const d = `${dest.lat},${dest.lng}`;
  return `https://www.google.com/maps/dir/?api=1&origin=${o}&destination=${d}&travelmode=driving`;
}
