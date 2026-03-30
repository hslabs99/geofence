import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

export type GeofenceEnterExitGapsRow = {
  device_name: string;
  day: string;
  point_count: number;
  /** Always 0 in this report (no ENTER/EXIT rows that device-day). */
  enter_exit_count: number;
};

/**
 * GET: Device-days with tracking but zero ENTER/EXIT tags — likely entry/exit tagging (and downstream steps) not run.
 *
 * Day boundary: position_time::date (verbatim API time — data checks use position_time only).
 * enter_exit_count counts rows with geofence_type ENTER or EXIT (trim/case-insensitive match).
 *
 * Query: dateFrom, dateTo (YYYY-MM-DD) required; minPoints (default 25); limit (default 500, max 2000).
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

    let minPoints = parseInt(searchParams.get('minPoints') ?? '25', 10);
    if (Number.isNaN(minPoints) || minPoints < 1) minPoints = 25;
    if (minPoints > 50000) minPoints = 50000;

    let limit = parseInt(searchParams.get('limit') ?? '500', 10);
    if (Number.isNaN(limit) || limit < 1) limit = 500;
    if (limit > 2000) limit = 2000;

    const sql = `
      SELECT
        t.device_name,
        to_char(t.position_time::date, 'YYYY-MM-DD') AS day,
        count(*)::int AS point_count,
        count(*) FILTER (
          WHERE upper(trim(both from coalesce(t.geofence_type::text, ''))) IN ('ENTER', 'EXIT')
        )::int AS enter_exit_count
      FROM tbl_tracking t
      WHERE t.position_time IS NOT NULL
        AND t.position_time::date >= $1::date
        AND t.position_time::date <= $2::date
      GROUP BY t.device_name, t.position_time::date
      HAVING count(*) >= $3::int
        AND count(*) FILTER (
          WHERE upper(trim(both from coalesce(t.geofence_type::text, ''))) IN ('ENTER', 'EXIT')
        ) = 0
      ORDER BY day DESC, t.device_name
      LIMIT $4::int
    `;

    const rows = await query<GeofenceEnterExitGapsRow>(sql, [dateFrom, dateTo, minPoints, limit]);

    return NextResponse.json({
      dateFrom,
      dateTo,
      minPoints,
      count: rows?.length ?? 0,
      rows: rows ?? [],
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
