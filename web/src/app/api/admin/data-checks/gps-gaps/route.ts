import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

/**
 * GET: Scan tbl_tracking for gaps in position_time_nz per device (ordered by device_name, position_time_nz).
 * Returns gaps > minGapMinutes. Report by date, by device with largest gap in minutes.
 *
 * Query params: minGapMinutes (required), dateFrom (optional YYYY-MM-DD), dateTo (optional YYYY-MM-DD).
 */
export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const minGapMinutesParam = searchParams.get('minGapMinutes');
    const minGapMinutes = minGapMinutesParam != null ? parseFloat(minGapMinutesParam) : NaN;
    if (Number.isNaN(minGapMinutes) || minGapMinutes < 0) {
      return NextResponse.json(
        { error: 'minGapMinutes is required and must be a non-negative number' },
        { status: 400 }
      );
    }

    const dateFrom = searchParams.get('dateFrom')?.trim() || null;
    const dateTo = searchParams.get('dateTo')?.trim() || null;

    const params: unknown[] = [minGapMinutes];
    let paramIdx = 2;
    let dateCondition = '';
    if (dateFrom) {
      params.push(dateFrom);
      dateCondition += ` AND position_time::date >= $${paramIdx}::date`;
      paramIdx++;
    }
    if (dateTo) {
      params.push(dateTo);
      dateCondition += ` AND position_time::date <= $${paramIdx}::date`;
    }

    // Gaps: for each row, previous position_time_nz in same device; gap_minutes = (current - prev) in minutes.
    // Report date (column 1) and date filter use position_time (not position_time_nz).
    // From/To = position_time (raw); From_NZ/To_NZ = position_time_nz.
    // DayCount = total tbl_tracking rows for that device on that (position_time) date.
    const summarySql = `
WITH daily_counts AS (
  SELECT device_name, position_time::date AS d, COUNT(*)::int AS day_count
  FROM tbl_tracking
  WHERE position_time_nz IS NOT NULL AND position_time IS NOT NULL
  ${dateCondition}
  GROUP BY device_name, position_time::date
),
ordered AS (
  SELECT device_name, position_time_nz, position_time, lat, lon,
    LAG(position_time_nz) OVER (PARTITION BY device_name ORDER BY position_time_nz) AS prev_nz,
    LAG(position_time) OVER (PARTITION BY device_name ORDER BY position_time_nz) AS prev_position_time,
    LAG(lat) OVER (PARTITION BY device_name ORDER BY position_time_nz) AS prev_lat,
    LAG(lon) OVER (PARTITION BY device_name ORDER BY position_time_nz) AS prev_lon
  FROM tbl_tracking
  WHERE position_time_nz IS NOT NULL AND position_time IS NOT NULL
  ${dateCondition}
),
gaps AS (
  SELECT device_name,
    prev_nz AS gap_start_nz,
    position_time_nz AS gap_end_nz,
    prev_position_time AS gap_start,
    position_time AS gap_end,
    prev_lat AS gap_start_lat,
    prev_lon AS gap_start_lon,
    lat AS gap_end_lat,
    lon AS gap_end_lon,
    EXTRACT(EPOCH FROM (position_time_nz - prev_nz))/60 AS gap_minutes
  FROM ordered
  WHERE prev_nz IS NOT NULL
  AND EXTRACT(EPOCH FROM (position_time_nz - prev_nz))/60 > $1
),
ranked AS (
  SELECT
    gap_start::date AS report_date,
    device_name,
    gap_start_nz,
    gap_end_nz,
    gap_start,
    gap_end,
    gap_start_lat,
    gap_start_lon,
    gap_end_lat,
    gap_end_lon,
    ROUND(gap_minutes::numeric, 2) AS gap_minutes_rounded,
    COUNT(*) OVER (PARTITION BY gap_start::date, device_name) AS gap_count,
    ROW_NUMBER() OVER (PARTITION BY gap_start::date, device_name ORDER BY gap_minutes DESC) AS rn
  FROM gaps
)
SELECT
  to_char(r.report_date, 'YYYY-MM-DD') AS report_date,
  r.device_name,
  r.gap_minutes_rounded AS max_gap_minutes,
  r.gap_count::int AS gap_count,
  COALESCE(dc.day_count, 0)::int AS day_count,
  to_char(r.gap_start, 'YYYY-MM-DD HH24:MI:SS') AS gap_from,
  to_char(r.gap_end, 'YYYY-MM-DD HH24:MI:SS') AS gap_to,
  to_char(r.gap_start_nz, 'YYYY-MM-DD HH24:MI:SS') AS gap_from_nz,
  to_char(r.gap_end_nz, 'YYYY-MM-DD HH24:MI:SS') AS gap_to_nz,
  r.gap_start_lat,
  r.gap_start_lon,
  r.gap_end_lat,
  r.gap_end_lon
FROM ranked r
LEFT JOIN daily_counts dc ON dc.device_name = r.device_name AND dc.d = r.report_date
WHERE r.rn = 1
ORDER BY r.report_date DESC, r.device_name
`;

    const summaryRows = await query<{
      report_date: string;
      device_name: string;
      max_gap_minutes: string;
      gap_count: number;
      day_count: number;
      gap_from: string | null;
      gap_to: string | null;
      gap_from_nz: string;
      gap_to_nz: string;
      gap_start_lat: number | null;
      gap_start_lon: number | null;
      gap_end_lat: number | null;
      gap_end_lon: number | null;
    }>(summarySql, params);

    // Detail: all gaps > minGapMinutes for optional display (limit 500); date filter by position_time
    const detailSql = `
WITH ordered AS (
  SELECT device_name, position_time_nz, position_time,
    LAG(position_time_nz) OVER (PARTITION BY device_name ORDER BY position_time_nz) AS prev_nz
  FROM tbl_tracking
  WHERE position_time_nz IS NOT NULL AND position_time IS NOT NULL
  ${dateCondition}
),
gaps AS (
  SELECT device_name, prev_nz AS gap_start, position_time_nz AS gap_end,
    ROUND((EXTRACT(EPOCH FROM (position_time_nz - prev_nz))/60)::numeric, 2) AS gap_minutes
  FROM ordered
  WHERE prev_nz IS NOT NULL
  AND EXTRACT(EPOCH FROM (position_time_nz - prev_nz))/60 > $1
)
SELECT device_name,
  to_char(gap_start, 'YYYY-MM-DD HH24:MI:SS') AS gap_start,
  to_char(gap_end, 'YYYY-MM-DD HH24:MI:SS') AS gap_end,
  gap_minutes::text
FROM gaps
ORDER BY gap_start DESC, device_name
LIMIT 500
`;

    const detailRows = await query<{
      device_name: string;
      gap_start: string;
      gap_end: string;
      gap_minutes: string;
    }>(detailSql, params);

    return NextResponse.json({
      summary: summaryRows,
      detail: detailRows,
      minGapMinutes,
      dateFrom: dateFrom ?? null,
      dateTo: dateTo ?? null,
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
