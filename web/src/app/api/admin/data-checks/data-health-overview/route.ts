import { NextResponse } from 'next/server';
import { query } from '@/lib/db';

function defaultRange(): { dateFrom: string; dateTo: string } {
  const to = new Date();
  const from = new Date(to);
  from.setUTCDate(from.getUTCDate() - 13);
  const fmt = (d: Date) => d.toISOString().slice(0, 10);
  return { dateFrom: fmt(from), dateTo: fmt(to) };
}

/**
 * GET: Single JSON snapshot for Data Health Overview (DB-only, no external GPS API).
 * Query: dateFrom, dateTo (YYYY-MM-DD) optional — default last 14 UTC days for window metrics.
 *        minPointsEnterExit (default 25) — same idea as Geofence Enter/Exit gaps tab.
 */
export async function GET(request: Request) {
  try {
    const { searchParams } = new URL(request.url);
    const re = /^\d{4}-\d{2}-\d{2}$/;
    const def = defaultRange();
    let dateFrom = searchParams.get('dateFrom')?.trim() || def.dateFrom;
    let dateTo = searchParams.get('dateTo')?.trim() || def.dateTo;
    if (!re.test(dateFrom) || !re.test(dateTo)) {
      return NextResponse.json({ error: 'dateFrom and dateTo must be YYYY-MM-DD' }, { status: 400 });
    }
    if (new Date(dateFrom + 'T00:00:00Z').getTime() > new Date(dateTo + 'T00:00:00Z').getTime()) {
      return NextResponse.json({ error: 'dateFrom must be <= dateTo' }, { status: 400 });
    }

    let minPointsEnterExit = parseInt(searchParams.get('minPointsEnterExit') ?? '25', 10);
    if (Number.isNaN(minPointsEnterExit) || minPointsEnterExit < 1) minPointsEnterExit = 25;
    if (minPointsEnterExit > 50000) minPointsEnterExit = 50000;

    const [
      trackingGlobal,
      trackingFenceAttempted,
      vworkGlobal,
      fenceDeviceDays,
      enterExitDeviceDays,
    ] = await Promise.all([
      query<{
        total_rows: string;
        max_position_time: string | null;
        max_position_time_nz: string | null;
        rows_missing_nz: string;
        rows_geofence_unattempted: string;
      }>(
        `SELECT
           COUNT(*)::text AS total_rows,
           to_char(max(t.position_time), 'YYYY-MM-DD HH24:MI:SS') AS max_position_time,
           to_char(max(t.position_time_nz), 'YYYY-MM-DD HH24:MI:SS') AS max_position_time_nz,
           COUNT(*) FILTER (WHERE t.position_time IS NOT NULL AND t.position_time_nz IS NULL)::text AS rows_missing_nz,
           COUNT(*) FILTER (WHERE t.geofence_attempted IS NOT TRUE)::text AS rows_geofence_unattempted
         FROM tbl_tracking t`
      ),
      query<{ max_pt_fence: string | null; max_pt_nz_fence: string | null }>(
        `SELECT
           to_char(max(t.position_time) FILTER (WHERE t.geofence_attempted IS TRUE), 'YYYY-MM-DD HH24:MI:SS') AS max_pt_fence,
           to_char(max(t.position_time_nz) FILTER (WHERE t.geofence_attempted IS TRUE), 'YYYY-MM-DD HH24:MI:SS') AS max_pt_nz_fence
         FROM tbl_tracking t`
      ),
      query<{
        jobs_with_actual_start: string;
        max_actual_start: string | null;
        max_actual_start_with_steps: string | null;
        jobs_start_not_fetched: string;
      }>(
        `SELECT
           COUNT(*) FILTER (WHERE v.actual_start_time IS NOT NULL)::text AS jobs_with_actual_start,
           to_char(max(v.actual_start_time), 'YYYY-MM-DD HH24:MI:SS') AS max_actual_start,
           to_char(
             max(v.actual_start_time) FILTER (
               WHERE v.step_1_actual_time IS NOT NULL
                  OR v.step_2_actual_time IS NOT NULL
                  OR v.step_3_actual_time IS NOT NULL
                  OR v.step_4_actual_time IS NOT NULL
                  OR v.step_5_actual_time IS NOT NULL
             ),
             'YYYY-MM-DD HH24:MI:SS'
           ) AS max_actual_start_with_steps,
           COUNT(*) FILTER (
             WHERE v.actual_start_time IS NOT NULL
               AND v.steps_fetched IS NOT TRUE
           )::text AS jobs_start_not_fetched
         FROM tbl_vworkjobs v`
      ),
      query<{ n: string }>(
        `SELECT COUNT(*)::text AS n FROM (
           SELECT 1
           FROM tbl_tracking t
           WHERE t.position_time IS NOT NULL
             AND t.position_time::date >= $1::date
             AND t.position_time::date <= $2::date
           GROUP BY t.device_name, t.position_time::date
           HAVING count(*) FILTER (WHERE t.geofence_attempted IS NOT TRUE) > 0
             AND count(*) >= 1
         ) sub`,
        [dateFrom, dateTo]
      ),
      query<{ n: string }>(
        `SELECT COUNT(*)::text AS n FROM (
           SELECT 1
           FROM tbl_tracking t
           WHERE t.position_time IS NOT NULL
             AND t.position_time::date >= $1::date
             AND t.position_time::date <= $2::date
           GROUP BY t.device_name, t.position_time::date
           HAVING count(*) >= $3::int
             AND count(*) FILTER (
               WHERE upper(trim(both from coalesce(t.geofence_type::text, ''))) IN ('ENTER', 'EXIT')
             ) = 0
         ) sub`,
        [dateFrom, dateTo, minPointsEnterExit]
      ),
    ]);

    const tg = trackingGlobal[0];
    const tfa = trackingFenceAttempted[0];
    const vg = vworkGlobal[0];

    return NextResponse.json({
      ok: true,
      generatedAt: new Date().toISOString(),
      window: { dateFrom, dateTo, minPointsEnterExit },
      tracking: {
        totalRows: parseInt(tg?.total_rows ?? '0', 10) || 0,
        maxPositionTime: tg?.max_position_time ?? null,
        maxPositionTimeNz: tg?.max_position_time_nz ?? null,
        rowsMissingPositionTimeNz: parseInt(tg?.rows_missing_nz ?? '0', 10) || 0,
        rowsGeofenceNotAttempted: parseInt(tg?.rows_geofence_unattempted ?? '0', 10) || 0,
        maxPositionTimeFenceAttempted: tfa?.max_pt_fence ?? null,
        maxPositionTimeNzFenceAttempted: tfa?.max_pt_nz_fence ?? null,
      },
      vworkjobs: {
        jobsWithActualStart: parseInt(vg?.jobs_with_actual_start ?? '0', 10) || 0,
        maxActualStartTime: vg?.max_actual_start ?? null,
        maxActualStartWithStepData: vg?.max_actual_start_with_steps ?? null,
        jobsWithStartStepsNotFetched: parseInt(vg?.jobs_start_not_fetched ?? '0', 10) || 0,
      },
      windowMetrics: {
        /** Device-days in range with some points still needing geofence_attempted */
        deviceDaysWithGeofenceGap: parseInt(fenceDeviceDays[0]?.n ?? '0', 10) || 0,
        /** Device-days with enough points but zero ENTER/EXIT (position_time::date) */
        deviceDaysWithEnterExitGap: parseInt(enterExitDeviceDays[0]?.n ?? '0', 10) || 0,
      },
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.error('[data-health-overview]', message);
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}
