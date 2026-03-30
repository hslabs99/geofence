import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

export type GeofenceGapsRow = {
  device_name: string;
  day: string;
  /** Total tbl_tracking rows that day (position_time in range) */
  point_count: number;
  /** Rows still needing geofence pass (geofence_attempted IS NOT TRUE) */
  unattempted_count: number;
  attempted_count: number;
};

/** One row per calendar day — which days to rerun mapping/tagging/steps for the whole day. */
export type GeofenceGapsDayRow = {
  day: string;
  point_count: number;
  unattempted_count: number;
  attempted_count: number;
  device_count: number;
};

/**
 * GET: Days (or device-days) where geofence_attempted is still false/null on some rows.
 *
 * Day boundary: position_time::date (verbatim API time — data checks use position_time only).
 * Query: dateFrom, dateTo (YYYY-MM-DD) required; minPoints (default 1); limit (default 500, max 2000).
 * view: `by-device-day` (default) | `by-day` — rollup to calendar days only for a simple rerun list.
 */
export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const dateFrom = searchParams.get('dateFrom')?.trim() || '';
    const dateTo = searchParams.get('dateTo')?.trim() || '';
    const re = /^\d{4}-\d{2}-\d{2}$/;
    if (!dateFrom || !dateTo || !re.test(dateFrom) || !re.test(dateTo)) {
      return NextResponse.json(
        { error: 'dateFrom and dateTo (YYYY-MM-DD) are required' },
        { status: 400 }
      );
    }
    if (new Date(dateFrom + 'T00:00:00Z').getTime() > new Date(dateTo + 'T00:00:00Z').getTime()) {
      return NextResponse.json({ error: 'dateFrom must be <= dateTo' }, { status: 400 });
    }

    let minPoints = parseInt(searchParams.get('minPoints') ?? '1', 10);
    if (Number.isNaN(minPoints) || minPoints < 1) minPoints = 1;
    if (minPoints > 50000) minPoints = 50000;

    let limit = parseInt(searchParams.get('limit') ?? '500', 10);
    if (Number.isNaN(limit) || limit < 1) limit = 500;
    if (limit > 2000) limit = 2000;

    const viewRaw = searchParams.get('view')?.trim().toLowerCase() || '';
    const viewByDay = viewRaw === 'by-day' || viewRaw === 'days';

    if (viewByDay) {
      const sql = `
        SELECT
          to_char(t.position_time::date, 'YYYY-MM-DD') AS day,
          count(*)::int AS point_count,
          count(*) FILTER (WHERE t.geofence_attempted IS NOT TRUE)::int AS unattempted_count,
          count(*) FILTER (WHERE t.geofence_attempted IS TRUE)::int AS attempted_count,
          count(DISTINCT t.device_name)::int AS device_count
        FROM tbl_tracking t
        WHERE t.position_time IS NOT NULL
          AND t.position_time::date >= $1::date
          AND t.position_time::date <= $2::date
        GROUP BY t.position_time::date
        HAVING count(*) FILTER (WHERE t.geofence_attempted IS NOT TRUE) > 0
          AND count(*) >= $3::int
        ORDER BY day DESC
        LIMIT $4::int
      `;
      const rows = await query<GeofenceGapsDayRow>(sql, [dateFrom, dateTo, minPoints, limit]);
      return NextResponse.json({
        dateFrom,
        dateTo,
        minPoints,
        view: 'by-day' as const,
        count: rows?.length ?? 0,
        rows: rows ?? [],
      });
    }

    const sql = `
      SELECT
        t.device_name,
        to_char(t.position_time::date, 'YYYY-MM-DD') AS day,
        count(*)::int AS point_count,
        count(*) FILTER (WHERE t.geofence_attempted IS NOT TRUE)::int AS unattempted_count,
        count(*) FILTER (WHERE t.geofence_attempted IS TRUE)::int AS attempted_count
      FROM tbl_tracking t
      WHERE t.position_time IS NOT NULL
        AND t.position_time::date >= $1::date
        AND t.position_time::date <= $2::date
      GROUP BY t.device_name, t.position_time::date
      HAVING count(*) FILTER (WHERE t.geofence_attempted IS NOT TRUE) > 0
        AND count(*) >= $3::int
      ORDER BY day DESC, t.device_name
      LIMIT $4::int
    `;

    const rows = await query<GeofenceGapsRow>(sql, [dateFrom, dateTo, minPoints, limit]);

    return NextResponse.json({
      dateFrom,
      dateTo,
      minPoints,
      view: 'by-device-day' as const,
      count: rows?.length ?? 0,
      rows: rows ?? [],
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
