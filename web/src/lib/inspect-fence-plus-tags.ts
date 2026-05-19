/**
 * Inspect-only: per tbl_tracking row, which job-vineyard Steps+ buffer(s) contain the point (lat/lon).
 * Same geometry as Steps+ (ST_Buffer in Web Mercator); no duration / stay logic.
 */

import { query } from '@/lib/db';

export type FencePlusTagRow = { id: string; fence_plus: string | null };

const FENCE_PLUS_BY_IDS_SQL = `
  WITH fence_buffered AS (
    SELECT g.fence_name, ST_Buffer(ST_Transform(ST_Force2D(g.geom), 3857), $2::numeric) AS buf
    FROM tbl_geofences g
    WHERE g.geom IS NOT NULL
      AND EXISTS (
        SELECT 1 FROM unnest($1::text[]) AS n(nm)
        WHERE nm IS NOT NULL AND TRIM(nm) <> ''
          AND LOWER(TRIM(COALESCE(g.fence_name,''))) = LOWER(TRIM(nm))
      )
  )
  SELECT t.id::text AS id,
    (
      SELECT string_agg(sub.fence_name, ', ' ORDER BY sub.fence_name)
      FROM (
        SELECT DISTINCT f.fence_name
        FROM fence_buffered f
        WHERE ST_Within(
          ST_Transform(ST_SetSRID(ST_MakePoint(t.lon, t.lat), 4326), 3857),
          f.buf
        )
      ) sub
    ) AS fence_plus
  FROM tbl_tracking t
  WHERE t.id = ANY($3::bigint[])
    AND t.device_name = $4
    AND t.lon IS NOT NULL
    AND t.lat IS NOT NULL
`;

/**
 * For each tracking id, comma-separated fence_name list whose buffered polygon contains the point, or null.
 */
export async function fencePlusTagsForTrackingIds(
  fenceNames: string[],
  bufferMeters: number,
  deviceName: string,
  trackingIds: number[]
): Promise<FencePlusTagRow[]> {
  if (fenceNames.length === 0 || trackingIds.length === 0 || !deviceName.trim()) return [];
  const uniqIds = [...new Set(trackingIds.filter((n) => Number.isFinite(n) && n > 0))];
  if (uniqIds.length === 0) return [];
  return query<FencePlusTagRow>(FENCE_PLUS_BY_IDS_SQL, [fenceNames, bufferMeters, uniqIds, deviceName.trim()]);
}
