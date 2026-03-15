/**
 * Shared Steps2 (buffered-fence stays) query. Used by /api/inspect/steps2 and by derived-steps route
 * when step 2/3 are missing (VineFence+ fallback). Read-only.
 */

import { query } from '@/lib/db';

export type Steps2SegmentRow = {
  fence_name: string;
  enter_time: string;
  exit_time: string;
  duration_seconds: string | number;
};

const STEPS2_SQL = `
  WITH pts AS (
    SELECT t.position_time_nz AS t, t.lon, t.lat
    FROM tbl_tracking t
    WHERE t.device_name = $1
      AND t.position_time_nz > $2::timestamptz
      AND t.position_time_nz < $3::timestamptz
      AND t.lon IS NOT NULL
      AND t.lat IS NOT NULL
  ),
  fence_buffered AS (
    SELECT g.fence_name, ST_Buffer(ST_Transform(ST_Force2D(g.geom), 3857), $5::numeric) AS buf
    FROM tbl_geofences g
    WHERE g.fence_name = ANY($4::text[])
      AND g.geom IS NOT NULL
  ),
  points_with_inside AS (
    SELECT p.t, f.fence_name,
      ST_Within(
        ST_Transform(ST_SetSRID(ST_MakePoint(p.lon, p.lat), 4326), 3857),
        f.buf
      ) AS inside
    FROM pts p
    CROSS JOIN fence_buffered f
  ),
  with_grp AS (
    SELECT t, fence_name, inside,
      sum(CASE WHEN NOT inside THEN 1 ELSE 0 END) OVER (PARTITION BY fence_name ORDER BY t) AS grp
    FROM points_with_inside
  ),
  segments AS (
    SELECT
      fence_name,
      min(t) AS enter_time,
      max(t) AS exit_time,
      EXTRACT(EPOCH FROM (max(t) - min(t)))::numeric AS duration_seconds
    FROM with_grp
    WHERE inside
    GROUP BY fence_name, grp
  )
  SELECT
    fence_name,
    to_char(enter_time, 'YYYY-MM-DD HH24:MI:SS') AS enter_time,
    to_char(exit_time, 'YYYY-MM-DD HH24:MI:SS') AS exit_time,
    duration_seconds
  FROM segments
`;

/** Run buffered-fence stays query. Returns all segments (caller filters >= 300s or single row). */
export async function runSteps2Query(
  device: string,
  startTime: string,
  endTime: string,
  fenceNames: string[],
  bufferMeters: number = 10
): Promise<Steps2SegmentRow[]> {
  if (!device || !startTime || !endTime || fenceNames.length === 0) return [];
  const rows = await query<Steps2SegmentRow>(STEPS2_SQL, [
    device,
    startTime,
    endTime,
    fenceNames,
    bufferMeters,
  ]);
  return rows;
}
